package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

const metaSuffix = "/meta.json"

// DB is the TODO struct.
type DB struct {
	blocks []*s3Block

	mtx sync.RWMutex

	client *minio.Client
	bucket string

	logger log.Logger
}

// NewDB is TODO stuff.
func NewDB(rcfg remoteConfig, mc *minio.Client, logger log.Logger) (*DB, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	blocks, err := getStorageBlocks(mc, rcfg.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "get storage blocks")
	}

	db := &DB{
		blocks: make([]*s3Block, 0, len(blocks)),
		client: mc,
		bucket: rcfg.Bucket,
		logger: logger,
	}
	for _, block := range blocks {
		sb, err := newS3Block(mc, rcfg.Bucket, block)
		if err != nil {
			return nil, errors.Wrapf(err, "new s3 block: %q", block)
		}

		logger.Log("debug", "adding block "+sb.key)
		db.blocks = append(db.blocks, sb)
	}

	sort.Slice(db.blocks, func(i, j int) bool {
		return db.blocks[i].meta.MinTime < db.blocks[j].meta.MinTime
	})

	go db.run(1 * time.Minute)
	return db, nil
}

func (s *DB) run(d time.Duration) {
	logger := log.With(s.logger, "caller", "s.run()")

	t := time.Tick(d)
	for _ = range t {
		m := make(map[string]struct{})
		s.mtx.RLock()
		for _, b := range s.blocks {
			m[b.key] = struct{}{}
		}
		s.mtx.RUnlock()

		blocks, err := getStorageBlocks(s.client, s.bucket)
		if err != nil {
			logger.Log("error", err.Error())
			continue
		}
		logger.Log("debug", "checking for new blocks.", "localBlocks", len(m), "remoteBlocks", len(blocks))

		newBlocks := make([]*s3Block, 0)
		for _, b := range blocks {
			if _, ok := m[b]; ok {
				continue
			}

			sb, err := newS3Block(s.client, s.bucket, b)
			if err != nil {
				logger.Log("error", err.Error())
				continue
			}
			newBlocks = append(newBlocks, sb)
			logger.Log("adding block", sb.key)
		}

		if len(newBlocks) == 0 {
			continue
		}

		s.mtx.Lock()
		s.blocks = append(s.blocks, newBlocks...)
		sort.Slice(s.blocks, func(i, j int) bool {
			return s.blocks[i].meta.MinTime < s.blocks[j].meta.MinTime
		})
		s.mtx.Unlock()
	}
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
	return NewBlockQuerier(b.indexr, b.chunkr, nopTombstones{}, mint, maxt)
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
	s.mtx.RLock()
	sq := &querier{
		blocks: make([]tsdb.Querier, 0, len(s.blocks)),
	}
	for _, b := range s.blocks {
		// Check interval overlap.
		if b.meta.MinTime <= maxt && mint <= b.meta.MaxTime {
			sq.blocks = append(sq.blocks, b.Querier(mint, maxt))
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
