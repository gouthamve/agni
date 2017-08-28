package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strings"

	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

const metaSuffix = "/meta.json"

// DB is the TODO struct.
type DB struct {
	blocks []*s3Block

	client *minio.Client
}

// NewDB is TODO stuff.
func NewDB(rcfg remoteConfig, mc *minio.Client) (*DB, error) {
	blocks, err := getStorageBlocks(mc, rcfg.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "get storage blocks")
	}

	db := &DB{
		blocks: make([]*s3Block, 0, len(blocks)),
		client: mc,
	}
	for _, block := range blocks {
		sb, err := newS3Block(mc, rcfg.Bucket, block)
		if err != nil {
			return nil, errors.Wrapf(err, "new s3 block: %q", block)
		}

		db.blocks = append(db.blocks, sb)
	}

	sort.Slice(db.blocks, func(i, j int) bool {
		return db.blocks[i].meta.MinTime < db.blocks[j].meta.MinTime
	})

	return db, nil
}

type s3Block struct {
	key  string
	meta tsdb.BlockMeta

	indexr *indexReader
	chunkr *chunkReader
}

func newS3Block(mc *minio.Client, bucket, key string) (*s3Block, error) {
	sb := &s3Block{
		key: key,
	}

	// Read the metafile.
	obj, err := mc.GetObject(bucket, key+metaSuffix)
	if err != nil {
		return nil, errors.Wrap(err, "get meta")
	}
	defer obj.Close()

	d := json.NewDecoder(obj)
	if err := d.Decode(&sb.meta); err != nil {
		return nil, errors.Wrap(err, "decode meta file")
	}

	// Read the index file.
	obj, err = mc.GetObject(bucket, key+indexSuffix)
	if err != nil {
		return nil, errors.Wrap(err, "get index")
	}
	defer obj.Close()
	buf, err := ioutil.ReadAll(obj)
	if err != nil {
		return nil, errors.Wrap(err, "read index file")
	}
	sb.indexr, err = newIndexReader(buf)
	if err != nil {
		return nil, errors.Wrap(err, "new index reader")
	}

	sb.chunkr, err = newChunkReader(mc, bucket, key, nil)
	if err != nil {
		return nil, errors.Wrap(err, "new chunk reader")
	}

	return sb, nil
}

func (b *s3Block) Querier(mint, maxt int64) tsdb.Querier {
	return tsdb.NewBlockQuerier(b.indexr, b.chunkr, nopTombstones{}, mint, maxt)
}

type nopTombstones struct{}

func (nopTombstones) Get(ref uint32) tsdb.Intervals {
	return nil
}

// querier aggregates querying results from time blocks within
// a single partition.
type querier struct {
	blocks []tsdb.Querier
}

// Querier returns a new querier over the data partition for the given time range.
// A goroutine must not handle more than one open Querier.
func (s *DB) Querier(mint, maxt int64) tsdb.Querier {
	sq := &querier{
		blocks: make([]tsdb.Querier, 0, len(s.blocks)),
	}
	for _, b := range s.blocks {
		sq.blocks = append(sq.blocks, b.Querier(mint, maxt))
	}

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
