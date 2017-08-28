package main

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

const indexSuffix = "/index"

// TODO Fix copying somehow.

type indexTOC struct {
	symbols           uint64
	series            uint64
	labelIndices      uint64
	labelIndicesTable uint64
	postings          uint64
	postingsTable     uint64
}

const indexTOCLen = 6*8 + 4

type indexReader struct {
	// The underlying byte slice holding the encoded series data.
	b   []byte
	toc indexTOC

	// Cached hashmaps of section offsets.
	labels   map[string]uint32
	postings map[string]uint32
}

var (
	errInvalidSize = fmt.Errorf("invalid size")
	errInvalidFlag = fmt.Errorf("invalid flag")
)

// newIndexReader returns a new indexReader on the given directory.
func newIndexReader(buf []byte) (*indexReader, error) {
	r := &indexReader{b: buf}

	// Verify magic number.
	if len(buf) < 4 {
		return nil, errors.Wrap(errInvalidSize, "index header")
	}
	if m := binary.BigEndian.Uint32(r.b[:4]); m != tsdb.MagicIndex {
		return nil, errors.Errorf("invalid magic number %x", m)
	}

	if err := r.readTOC(); err != nil {
		return nil, errors.Wrap(err, "read TOC")
	}

	var err error
	r.labels, err = r.readOffsetTable(r.toc.labelIndicesTable)
	if err != nil {
		return nil, errors.Wrap(err, "read label index table")
	}
	r.postings, err = r.readOffsetTable(r.toc.postingsTable)
	return r, errors.Wrap(err, "read postings table")
}

func (r *indexReader) readTOC() error {
	d := r.decbufAt(len(r.b) - indexTOCLen)

	r.toc.symbols = d.be64()
	r.toc.series = d.be64()
	r.toc.labelIndices = d.be64()
	r.toc.labelIndicesTable = d.be64()
	r.toc.postings = d.be64()
	r.toc.postingsTable = d.be64()

	// TODO(fabxc): validate checksum.

	return nil
}

func (r *indexReader) decbufAt(off int) decbuf {
	if len(r.b) < off {
		return decbuf{e: errInvalidSize}
	}
	return decbuf{b: r.b[off:]}
}

// readOffsetTable reads an offset table at the given position and returns a map
// with the key strings concatenated by the 0xff unicode non-character.
func (r *indexReader) readOffsetTable(off uint64) (map[string]uint32, error) {
	// A table might not have been written at all, in which case the position
	// is zeroed out.
	if off == 0 {
		return nil, nil
	}

	const sep = "\xff"

	var (
		d1  = r.decbufAt(int(off))
		cnt = d1.be32()
		d2  = d1.decbuf(d1.be32int())
	)

	res := make(map[string]uint32, 512)

	for d2.err() == nil && d2.len() > 0 && cnt > 0 {
		keyCount := int(d2.uvarint())
		keys := make([]string, 0, keyCount)

		for i := 0; i < keyCount; i++ {
			keys = append(keys, d2.uvarintStr())
		}
		res[strings.Join(keys, sep)] = uint32(d2.uvarint())

		cnt--
	}

	// TODO(fabxc): verify checksum from remainer of d1.
	return res, d2.err()
}

func (r *indexReader) Close() error {
	return nil
}

func (r *indexReader) section(o uint32) (byte, []byte, error) {
	b := r.b[o:]

	if len(b) < 5 {
		return 0, nil, errors.Wrap(errInvalidSize, "read header")
	}

	flag := b[0]
	l := binary.BigEndian.Uint32(b[1:5])

	b = b[5:]

	// b must have the given length plus 4 bytes for the CRC32 checksum.
	if len(b) < int(l)+4 {
		return 0, nil, errors.Wrap(errInvalidSize, "section content")
	}
	return flag, b[:l], nil
}

func (r *indexReader) lookupSymbol(o uint32) (string, error) {
	d := r.decbufAt(int(o))

	s := d.uvarintStr()
	if d.err() != nil {
		return "", errors.Wrapf(d.err(), "read symbol at %d", o)
	}
	return s, nil
}

func (r *indexReader) Symbols() (map[string]struct{}, error) {
	d1 := r.decbufAt(int(r.toc.symbols))
	d2 := d1.decbuf(d1.be32int())

	count := d2.be32int()
	sym := make(map[string]struct{}, count)

	for ; count > 0; count-- {
		s := d2.uvarintStr()
		sym[s] = struct{}{}
	}

	return sym, d2.err()
}

func (r *indexReader) LabelValues(names ...string) (tsdb.StringTuples, error) {
	const sep = "\xff"

	key := strings.Join(names, sep)
	off, ok := r.labels[key]
	if !ok {
		// XXX(fabxc): hot fix. Should return a partial data error and handle cases
		// where the entire block has no data gracefully.
		return emptyStringTuples{}, nil
		//return nil, fmt.Errorf("label index doesn't exist")
	}

	d1 := r.decbufAt(int(off))
	d2 := d1.decbuf(d1.be32int())

	nc := d2.be32int()
	d2.be32() // consume unused value entry count.

	if d2.err() != nil {
		return nil, errors.Wrap(d2.err(), "read label value index")
	}

	// TODO(fabxc): verify checksum in 4 remaining bytes of d1.

	st := &serializedStringTuples{
		l:      nc,
		b:      d2.get(),
		lookup: r.lookupSymbol,
	}
	return st, nil
}

type emptyStringTuples struct{}

func (emptyStringTuples) At(i int) ([]string, error) { return nil, nil }
func (emptyStringTuples) Len() int                   { return 0 }

func (r *indexReader) LabelIndices() ([][]string, error) {
	const sep = "\xff"

	res := [][]string{}

	for s := range r.labels {
		res = append(res, strings.Split(s, sep))
	}
	return res, nil
}

func (r *indexReader) Series(ref uint32, lbls *labels.Labels, chks *[]tsdb.ChunkMeta) error {
	d1 := r.decbufAt(int(ref))
	d2 := d1.decbuf(int(d1.uvarint()))

	*lbls = (*lbls)[:0]
	*chks = (*chks)[:0]

	k := int(d2.uvarint())

	for i := 0; i < k; i++ {
		lno := uint32(d2.uvarint())
		lvo := uint32(d2.uvarint())

		if d2.err() != nil {
			return errors.Wrap(d2.err(), "read series label offsets")
		}

		ln, err := r.lookupSymbol(lno)
		if err != nil {
			return errors.Wrap(err, "lookup label name")
		}
		lv, err := r.lookupSymbol(lvo)
		if err != nil {
			return errors.Wrap(err, "lookup label value")
		}

		*lbls = append(*lbls, labels.Label{Name: ln, Value: lv})
	}

	// Read the chunks meta data.
	k = int(d2.uvarint())

	for i := 0; i < k; i++ {
		mint := d2.varint64()
		maxt := d2.varint64()
		off := d2.uvarint64()

		if d2.err() != nil {
			return errors.Wrapf(d2.err(), "read meta for chunk %d", i)
		}

		*chks = append(*chks, tsdb.ChunkMeta{
			Ref:     off,
			MinTime: mint,
			MaxTime: maxt,
		})
	}

	// TODO(fabxc): verify CRC32.

	return nil
}

func (r *indexReader) Postings(name, value string) (tsdb.Postings, error) {
	const sep = "\xff"
	key := strings.Join([]string{name, value}, sep)

	off, ok := r.postings[key]
	if !ok {
		return emptyPostings, nil
	}

	d1 := r.decbufAt(int(off))
	d2 := d1.decbuf(d1.be32int())

	d2.be32() // consume unused postings list length.

	if d2.err() != nil {
		return nil, errors.Wrap(d2.err(), "get postings bytes")
	}

	// TODO(fabxc): read checksum from 4 remainer bytes of d1 and verify.

	return newBigEndianPostings(d2.get()), nil
}

func (r *indexReader) SortedPostings(p tsdb.Postings) tsdb.Postings {
	return p
}

type stringTuples struct {
	l int      // tuple length
	s []string // flattened tuple entries
}

func newStringTuples(s []string, l int) (*stringTuples, error) {
	if len(s)%l != 0 {
		return nil, errors.Wrap(errInvalidSize, "string tuple list")
	}
	return &stringTuples{s: s, l: l}, nil
}

func (t *stringTuples) Len() int                   { return len(t.s) / t.l }
func (t *stringTuples) At(i int) ([]string, error) { return t.s[i : i+t.l], nil }

func (t *stringTuples) Swap(i, j int) {
	c := make([]string, t.l)
	copy(c, t.s[i:i+t.l])

	for k := 0; k < t.l; k++ {
		t.s[i+k] = t.s[j+k]
		t.s[j+k] = c[k]
	}
}

func (t *stringTuples) Less(i, j int) bool {
	for k := 0; k < t.l; k++ {
		d := strings.Compare(t.s[i+k], t.s[j+k])

		if d < 0 {
			return true
		}
		if d > 0 {
			return false
		}
	}
	return false
}

type serializedStringTuples struct {
	l      int
	b      []byte
	lookup func(uint32) (string, error)
}

func (t *serializedStringTuples) Len() int {
	return len(t.b) / (4 * t.l)
}

func (t *serializedStringTuples) At(i int) ([]string, error) {
	if len(t.b) < (i+t.l)*4 {
		return nil, errInvalidSize
	}
	res := make([]string, 0, t.l)

	for k := 0; k < t.l; k++ {
		offset := binary.BigEndian.Uint32(t.b[(i+k)*4:])

		s, err := t.lookup(offset)
		if err != nil {
			return nil, errors.Wrap(err, "symbol lookup")
		}
		res = append(res, s)
	}

	return res, nil
}

// errPostings is an empty iterator that always errors.
type errPostings struct {
	err error
}

func (e errPostings) Next() bool       { return false }
func (e errPostings) Seek(uint32) bool { return false }
func (e errPostings) At() uint32       { return 0 }
func (e errPostings) Err() error       { return e.err }

var emptyPostings = errPostings{}

// bigEndianPostings implements the Postings interface over a byte stream of
// big endian numbers.
type bigEndianPostings struct {
	list []byte
	cur  uint32
}

func newBigEndianPostings(list []byte) *bigEndianPostings {
	return &bigEndianPostings{list: list}
}

func (it *bigEndianPostings) At() uint32 {
	return it.cur
}

func (it *bigEndianPostings) Next() bool {
	if len(it.list) >= 4 {
		it.cur = binary.BigEndian.Uint32(it.list)
		it.list = it.list[4:]
		return true
	}
	return false
}

func (it *bigEndianPostings) Seek(x uint32) bool {
	if it.cur >= x {
		return true
	}

	num := len(it.list) / 4
	// Do binary search between current position and end.
	i := sort.Search(num, func(i int) bool {
		return binary.BigEndian.Uint32(it.list[i*4:]) >= x
	})
	if i < num {
		j := i * 4
		it.cur = binary.BigEndian.Uint32(it.list[j:])
		it.list = it.list[j+4:]
		return true
	}
	it.list = nil
	return false
}

func (it *bigEndianPostings) Err() error {
	return nil
}
