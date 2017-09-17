package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/gouthamve/agni/pkg/errgroup"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/labels"
)

// querier aggregates querying results from time blocks within
// a single partition.
type querier struct {
	blocks []tsdb.Querier
}

// Querier returns a new querier over the data partition for the given time range.
// A goroutine must not handle more than one open Querier.
func (s *DB) Querier(mint, maxt int64) tsdb.Querier {
	s.mtx.RLock()
	sq := &querier{
		blocks: make([]tsdb.Querier, 0, len(s.blocks)),
	}
	for _, b := range s.blocks {
		// Check interval overlap.
		if b.Meta().MinTime <= maxt && mint <= b.Meta().MaxTime {
			sq.blocks = append(sq.blocks, tsdb.NewBlockQuerier(
				b.Index(),
				b.Chunks(),
				b.Tombstones(),
				mint,
				maxt,
			))
		}
	}
	s.mtx.RUnlock()
	return sq
}

func (q *querier) LabelValues(n string) ([]string, error) {
	return q.lvals(q.blocks, n)
}

func (q *querier) lvals(qs []tsdb.Querier, n string) ([]string, error) {
	if len(qs) == 0 {
		return nil, nil
	}
	if len(qs) == 1 {
		return qs[0].LabelValues(n)
	}
	l := len(qs) / 2
	s1, err := q.lvals(qs[:l], n)
	if err != nil {
		return nil, err
	}
	s2, err := q.lvals(qs[l:], n)
	if err != nil {
		return nil, err
	}
	return mergeStrings(s1, s2), nil
}

func (q *querier) LabelValuesFor(string, labels.Label) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

func (q *querier) Select(ms ...labels.Matcher) tsdb.SeriesSet {
	return q.sel(q.blocks, ms)

}

func (q *querier) sel(qs []tsdb.Querier, ms []labels.Matcher) tsdb.SeriesSet {
	if len(qs) == 0 {
		return nopSeriesSet{}
	}
	if len(qs) == 1 {
		return qs[0].Select(ms...)
	}
	l := len(qs) / 2
	return tsdb.NewMergedSeriesSet(q.sel(qs[:l], ms), q.sel(qs[l:], ms))
}

func (q *querier) Close() error {
	cs := make([]io.Closer, 0, len(q.blocks))
	for _, b := range q.blocks {
		cs = append(cs, b)
	}
	return closeAll(cs...)
}

// NewBlockQuerier returns a querier against the readers.
func NewBlockQuerier(ir tsdb.IndexReader, cr ChunkReader, tr tsdb.TombstoneReader, mint, maxt int64) tsdb.Querier {
	return &blockQuerier{
		index:      ir,
		chunks:     cr,
		tombstones: tr,

		mint: mint,
		maxt: maxt,
	}
}

// blockQuerier provides querying access to a single block database.
type blockQuerier struct {
	index      tsdb.IndexReader
	chunks     ChunkReader
	tombstones tsdb.TombstoneReader

	mint, maxt int64
}

func (q *blockQuerier) Select(ms ...labels.Matcher) tsdb.SeriesSet {
	pr := newPostingsReader(q.index)

	p, absent := pr.Select(ms...)

	pcs, err := newPopulatedChunkSeries(
		&baseChunkSeries{
			p:      p,
			index:  q.index,
			absent: absent,

			tombstones: q.tombstones,
		},
		q.chunks, q.mint, q.maxt,
	)

	if err != nil {
		return errSeriesSet{err: err}
	}

	return &blockSeriesSet{
		set: pcs,

		mint: q.mint,
		maxt: q.maxt,
	}
}

func (q *blockQuerier) LabelValues(name string) ([]string, error) {
	tpls, err := q.index.LabelValues(name)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, tpls.Len())

	for i := 0; i < tpls.Len(); i++ {
		vals, err := tpls.At(i)
		if err != nil {
			return nil, err
		}
		res = append(res, vals[0])
	}
	return res, nil
}

func (q *blockQuerier) LabelValuesFor(string, labels.Label) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

func (q *blockQuerier) Close() error {
	return nil
}

// postingsReader is used to select matching postings from an IndexReader.
type postingsReader struct {
	index tsdb.IndexReader
}

func newPostingsReader(i tsdb.IndexReader) *postingsReader {
	return &postingsReader{index: i}
}

func (r *postingsReader) Select(ms ...labels.Matcher) (tsdb.Postings, []string) {
	var (
		its    []tsdb.Postings
		absent []string
	)
	for _, m := range ms {
		// If the matcher checks absence of a label, don't select them
		// but propagate the check into the series set.
		if _, ok := m.(*labels.EqualMatcher); ok && m.Matches("") {
			absent = append(absent, m.Name())
			continue
		}
		its = append(its, r.selectSingle(m))
	}

	p := tsdb.Intersect(its...)

	return r.index.SortedPostings(p), absent
}

// tuplesByPrefix uses binary search to find prefix matches within ts.
func tuplesByPrefix(m *labels.PrefixMatcher, ts tsdb.StringTuples) ([]string, error) {
	var outErr error
	tslen := ts.Len()
	i := sort.Search(tslen, func(i int) bool {
		vs, err := ts.At(i)
		if err != nil {
			outErr = fmt.Errorf("Failed to read tuple %d/%d: %v", i, tslen, err)
			return true
		}
		val := vs[0]
		l := len(m.Prefix())
		if l > len(vs) {
			l = len(val)
		}
		return val[:l] >= m.Prefix()
	})
	if outErr != nil {
		return nil, outErr
	}
	var matches []string
	for ; i < tslen; i++ {
		vs, err := ts.At(i)
		if err != nil || !m.Matches(vs[0]) {
			return matches, err
		}
		matches = append(matches, vs[0])
	}
	return matches, nil
}

func (r *postingsReader) selectSingle(m labels.Matcher) tsdb.Postings {
	// Fast-path for equal matching.
	if em, ok := m.(*labels.EqualMatcher); ok {
		it, err := r.index.Postings(em.Name(), em.Value())
		if err != nil {
			return errPostings{err: err}
		}
		return it
	}

	tpls, err := r.index.LabelValues(m.Name())
	if err != nil {
		return errPostings{err: err}
	}

	var res []string
	if pm, ok := m.(*labels.PrefixMatcher); ok {
		res, err = tuplesByPrefix(pm, tpls)
		if err != nil {
			return errPostings{err: err}
		}

	} else {
		for i := 0; i < tpls.Len(); i++ {
			vals, err := tpls.At(i)
			if err != nil {
				return errPostings{err: err}
			}
			if m.Matches(vals[0]) {
				res = append(res, vals[0])
			}
		}
	}

	if len(res) == 0 {
		return emptyPostings
	}

	var rit []tsdb.Postings

	for _, v := range res {
		it, err := r.index.Postings(m.Name(), v)
		if err != nil {
			return errPostings{err: err}
		}
		rit = append(rit, it)
	}

	return tsdb.Merge(rit...)
}

func mergeStrings(a, b []string) []string {
	maxl := len(a)
	if len(b) > len(a) {
		maxl = len(b)
	}
	res := make([]string, 0, maxl*10/9)

	for len(a) > 0 && len(b) > 0 {
		d := strings.Compare(a[0], b[0])

		if d == 0 {
			res = append(res, a[0])
			a, b = a[1:], b[1:]
		} else if d < 0 {
			res = append(res, a[0])
			a = a[1:]
		} else if d > 0 {
			res = append(res, b[0])
			b = b[1:]
		}
	}

	// Append all remaining elements.
	res = append(res, a...)
	res = append(res, b...)
	return res
}

type nopSeriesSet struct{}

func (nopSeriesSet) Next() bool      { return false }
func (nopSeriesSet) At() tsdb.Series { return nil }
func (nopSeriesSet) Err() error      { return nil }

// mergedSeriesSet takes two series sets as a single series set. The input series sets
// must be sorted and sequential in time, i.e. if they have the same label set,
// the datapoints of a must be before the datapoints of b.
type mergedSeriesSet struct {
	a, b tsdb.SeriesSet

	cur          tsdb.Series
	adone, bdone bool
}

// NewMergedSeriesSet takes two series sets as a single series set. The input series sets
// must be sorted and sequential in time, i.e. if they have the same label set,
// the datapoints of a must be before the datapoints of b.
func NewMergedSeriesSet(a, b tsdb.SeriesSet) tsdb.SeriesSet {
	return newMergedSeriesSet(a, b)
}

func newMergedSeriesSet(a, b tsdb.SeriesSet) *mergedSeriesSet {
	s := &mergedSeriesSet{a: a, b: b}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()

	return s
}

func (s *mergedSeriesSet) At() tsdb.Series {
	return s.cur
}

func (s *mergedSeriesSet) Err() error {
	if s.a.Err() != nil {
		return s.a.Err()
	}
	return s.b.Err()
}

func (s *mergedSeriesSet) compare() int {
	if s.adone {
		return 1
	}
	if s.bdone {
		return -1
	}
	return labels.Compare(s.a.At().Labels(), s.b.At().Labels())
}

func (s *mergedSeriesSet) Next() bool {
	if s.adone && s.bdone || s.Err() != nil {
		return false
	}

	d := s.compare()

	// Both sets contain the current series. Chain them into a single one.
	if d > 0 {
		s.cur = s.b.At()
		s.bdone = !s.b.Next()
	} else if d < 0 {
		s.cur = s.a.At()
		s.adone = !s.a.Next()
	} else {
		s.cur = &chainedSeries{series: []tsdb.Series{s.a.At(), s.b.At()}}
		s.adone = !s.a.Next()
		s.bdone = !s.b.Next()
	}
	return true
}

type chunkSeriesSet interface {
	Next() bool
	At() (labels.Labels, []tsdb.ChunkMeta, tsdb.Intervals)
	Err() error
}

type listChunkSeries struct {
	list []chunkSeries

	cur chunkSeries
}

func newListChunkSeries(l []chunkSeries) *listChunkSeries {
	return &listChunkSeries{list: l}
}

func (l *listChunkSeries) At() (labels.Labels, []tsdb.ChunkMeta, tsdb.Intervals) {
	return l.cur.Labels(), l.cur.chunks, l.cur.intervals
}

func (l *listChunkSeries) Next() bool {
	if len(l.list) > 0 {
		l.cur = l.list[0]
		l.list = l.list[1:]
		return true
	}

	return false
}

func (l *listChunkSeries) Err() error {
	return nil
}

// baseChunkSeries loads the label set and chunk references for a postings
// list from an index. It filters out series that have labels set that should be unset.
type baseChunkSeries struct {
	p          tsdb.Postings
	index      tsdb.IndexReader
	tombstones tsdb.TombstoneReader
	absent     []string // labels that must be unset in results.

	lset      labels.Labels
	chks      []tsdb.ChunkMeta
	intervals tsdb.Intervals
	err       error
}

func (s *baseChunkSeries) At() (labels.Labels, []tsdb.ChunkMeta, tsdb.Intervals) {
	return s.lset, s.chks, s.intervals
}

func (s *baseChunkSeries) Err() error { return s.err }

func (s *baseChunkSeries) Next() bool {
	var (
		lset   labels.Labels
		chunks []tsdb.ChunkMeta
	)
Outer:
	for s.p.Next() {
		ref := s.p.At()
		if err := s.index.Series(ref, &lset, &chunks); err != nil {
			s.err = err
			return false
		}

		// If a series contains a label that must be absent, it is skipped as well.
		for _, abs := range s.absent {
			if lset.Get(abs) != "" {
				continue Outer
			}
		}

		s.lset = lset
		s.chks = chunks
		s.intervals = s.tombstones.Get(s.p.At())

		if len(s.intervals) > 0 {
			// Only those chunks that are not entirely deleted.
			chks := make([]tsdb.ChunkMeta, 0, len(s.chks))
			for _, chk := range s.chks {
				if !isSubrange(s.intervals, tsdb.Interval{chk.MinTime, chk.MaxTime}) {
					chks = append(chks, chk)
				}
			}

			s.chks = chks
		}

		return true
	}
	if err := s.p.Err(); err != nil {
		s.err = err
	}
	return false
}

// populatedChunkSeries loads chunk data from a store for a set of series
// with known chunk references. It filters out chunks that do not fit the
// given time range.
type populatedChunkSeries struct {
	set        chunkSeriesSet
	chunks     ChunkReader
	mint, maxt int64

	list *listChunkSeries
}

func newPopulatedChunkSeries(set chunkSeriesSet, cr ChunkReader, mint, maxt int64) (*populatedChunkSeries, error) {
	p := &populatedChunkSeries{
		set:    set,
		chunks: cr,
		mint:   mint,
		maxt:   maxt,
	}

	return p, p.populate()
}

func (s *populatedChunkSeries) populate() error {
	l := make([]chunkSeries, 0, 2<<11)
	for s.set.Next() {
		lset, chks, dranges := s.set.At()

		for len(chks) > 0 {
			if chks[0].MaxTime >= s.mint {
				break
			}
			chks = chks[1:]
		}

		for i := range chks {
			// Break out at the first chunk that has no overlap with mint, maxt.
			if chks[i].MinTime > s.maxt {
				chks = chks[:i]
				break
			}
		}
		if len(chks) == 0 {
			continue
		}

		l = append(l, chunkSeries{
			labels:    lset,
			chunks:    chks,
			intervals: dranges,
			mint:      s.mint,
			maxt:      s.maxt,
		})
	}

	if err := populateSeriesInParallel(l, s.chunks, 100); err != nil {
		return err
	}

	s.list = newListChunkSeries(l)
	return nil
}

func (s *populatedChunkSeries) At() (labels.Labels, []tsdb.ChunkMeta, tsdb.Intervals) {
	return s.list.At()
}
func (s *populatedChunkSeries) Err() error { return s.list.Err() }

func (s *populatedChunkSeries) Next() bool {
	return s.list.Next()
}

func populateSeriesInParallel(l []chunkSeries, cr ChunkReader, workers int) error {
	g, _ := errgroup.New(context.Background(), workers)

	for i := range l {
		i := i
		g.Add(func() error {
			c := l[i]
			fmt.Println(c.Labels(), ".")
			if err := cr.Populate(c.chunks); err != nil {
				return err
			}
			return nil
		})
	}

	return g.Run()
}

// blockSeriesSet is a set of series from an inverted index query.
type blockSeriesSet struct {
	set chunkSeriesSet
	err error
	cur tsdb.Series

	mint, maxt int64
}

func (s *blockSeriesSet) Next() bool {
	for s.set.Next() {
		lset, chunks, dranges := s.set.At()
		s.cur = &chunkSeries{
			labels: lset,
			chunks: chunks,
			mint:   s.mint,
			maxt:   s.maxt,

			intervals: dranges,
		}
		return true
	}
	if s.set.Err() != nil {
		s.err = s.set.Err()
	}
	return false
}

func (s *blockSeriesSet) At() tsdb.Series { return s.cur }
func (s *blockSeriesSet) Err() error      { return s.err }

// chunkSeries is a series that is backed by a sequence of chunks holding
// time series data.
type chunkSeries struct {
	labels labels.Labels
	chunks []tsdb.ChunkMeta // in-order chunk refs

	mint, maxt int64

	intervals tsdb.Intervals
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.labels
}

func (s *chunkSeries) Iterator() tsdb.SeriesIterator {
	return newChunkSeriesIterator(s.chunks, s.intervals, s.mint, s.maxt)
}

// chainedSeries implements a series for a list of time-sorted series.
// They all must have the same labels.
type chainedSeries struct {
	series []tsdb.Series
}

func (s *chainedSeries) Labels() labels.Labels {
	return s.series[0].Labels()
}

func (s *chainedSeries) Iterator() tsdb.SeriesIterator {
	return newChainedSeriesIterator(s.series...)
}

// chainedSeriesIterator implements a series iterater over a list
// of time-sorted, non-overlapping iterators.
type chainedSeriesIterator struct {
	series []tsdb.Series // series in time order

	i   int
	cur tsdb.SeriesIterator
}

func newChainedSeriesIterator(s ...tsdb.Series) *chainedSeriesIterator {
	return &chainedSeriesIterator{
		series: s,
		i:      0,
		cur:    s[0].Iterator(),
	}
}

func (it *chainedSeriesIterator) Seek(t int64) bool {
	// We just scan the chained series sequentially as they are already
	// pre-selected by relevant time and should be accessed sequentially anyway.
	for i, s := range it.series[it.i:] {
		cur := s.Iterator()
		if !cur.Seek(t) {
			continue
		}
		it.cur = cur
		it.i += i
		return true
	}
	return false
}

func (it *chainedSeriesIterator) Next() bool {
	if it.cur.Next() {
		return true
	}
	if err := it.cur.Err(); err != nil {
		return false
	}
	if it.i == len(it.series)-1 {
		return false
	}

	it.i++
	it.cur = it.series[it.i].Iterator()

	return it.Next()
}

func (it *chainedSeriesIterator) At() (t int64, v float64) {
	return it.cur.At()
}

func (it *chainedSeriesIterator) Err() error {
	return it.cur.Err()
}

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks []tsdb.ChunkMeta

	i   int
	cur chunks.Iterator

	maxt, mint int64

	intervals tsdb.Intervals
}

func newChunkSeriesIterator(cs []tsdb.ChunkMeta, dranges tsdb.Intervals, mint, maxt int64) *chunkSeriesIterator {
	it := cs[0].Chunk.Iterator()

	if len(dranges) > 0 {
		it = &deletedIterator{it: it, intervals: dranges}
	}
	return &chunkSeriesIterator{
		chunks: cs,
		i:      0,
		cur:    it,

		mint: mint,
		maxt: maxt,

		intervals: dranges,
	}
}

func (it *chunkSeriesIterator) Seek(t int64) (ok bool) {
	if t > it.maxt {
		return false
	}

	// Seek to the first valid value after t.
	if t < it.mint {
		t = it.mint
	}

	for ; it.chunks[it.i].MaxTime < t; it.i++ {
		if it.i == len(it.chunks)-1 {
			return false
		}
	}

	it.cur = it.chunks[it.i].Chunk.Iterator()
	if len(it.intervals) > 0 {
		it.cur = &deletedIterator{it: it.cur, intervals: it.intervals}
	}

	for it.cur.Next() {
		t0, _ := it.cur.At()
		if t0 >= t {
			return true
		}
	}
	return false
}

func (it *chunkSeriesIterator) At() (t int64, v float64) {
	return it.cur.At()
}

func (it *chunkSeriesIterator) Next() bool {
	if it.cur.Next() {
		t, _ := it.cur.At()

		if t < it.mint {
			if !it.Seek(it.mint) {
				return false
			}
			t, _ = it.At()

			return t <= it.maxt
		}
		if t > it.maxt {
			return false
		}
		return true
	}
	if err := it.cur.Err(); err != nil {
		return false
	}
	if it.i == len(it.chunks)-1 {
		return false
	}

	it.i++
	it.cur = it.chunks[it.i].Chunk.Iterator()
	if len(it.intervals) > 0 {
		it.cur = &deletedIterator{it: it.cur, intervals: it.intervals}
	}

	return it.Next()
}

func (it *chunkSeriesIterator) Err() error {
	return it.cur.Err()
}

type mockSeriesSet struct {
	next   func() bool
	series func() tsdb.Series
	err    func() error
}

func (m *mockSeriesSet) Next() bool      { return m.next() }
func (m *mockSeriesSet) At() tsdb.Series { return m.series() }
func (m *mockSeriesSet) Err() error      { return m.err() }

func newListSeriesSet(list []tsdb.Series) *mockSeriesSet {
	i := -1
	return &mockSeriesSet{
		next: func() bool {
			i++
			return i < len(list)
		},
		series: func() tsdb.Series {
			return list[i]
		},
		err: func() error { return nil },
	}
}

type errSeriesSet struct {
	err error
}

func (s errSeriesSet) Next() bool      { return false }
func (s errSeriesSet) At() tsdb.Series { return nil }
func (s errSeriesSet) Err() error      { return s.err }

func inBounds(tr tsdb.Interval, t int64) bool {
	return t >= tr.Mint && t <= tr.Maxt
}

func isSubrange(dranges tsdb.Intervals, tr tsdb.Interval) bool {
	for _, r := range dranges {
		if inBounds(r, tr.Mint) && inBounds(r, tr.Maxt) {
			return true
		}
	}

	return false
}

// deletedIterator wraps an Iterator and makes sure any deleted metrics are not
// returned.
type deletedIterator struct {
	it chunks.Iterator

	intervals tsdb.Intervals
}

func (it *deletedIterator) At() (int64, float64) {
	return it.it.At()
}

func (it *deletedIterator) Next() bool {
Outer:
	for it.it.Next() {
		ts, _ := it.it.At()

		for _, tr := range it.intervals {
			if inBounds(tr, ts) {
				continue Outer
			}

			if ts > tr.Maxt {
				it.intervals = it.intervals[1:]
				continue
			}

			return true
		}

		return true
	}

	return false
}

func (it *deletedIterator) Err() error {
	return it.it.Err()
}

type nopTombstones struct{}

func (nopTombstones) Get(ref uint64) tsdb.Intervals {
	return nil
}

// errPostings is an empty iterator that always errors.
type errPostings struct {
	err error
}

func (e errPostings) Next() bool       { return false }
func (e errPostings) Seek(uint64) bool { return false }
func (e errPostings) At() uint64       { return 0 }
func (e errPostings) Err() error       { return e.err }

var emptyPostings = errPostings{}
