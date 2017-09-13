package main

import (
	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

type s3Block struct {
	key  string
	dir  string
	meta tsdb.BlockMeta

	indexr tsdb.IndexReader
	chunkr *chunkReader
}

func newS3Block(mc *minio.Client, bucket, key, dir string) (*s3Block, error) {
	sb := &s3Block{
		key: key,
	}

	meta, err := readMetaFile(dir)
	if err != nil {
		return nil, err
	}
	sb.meta = *meta

	sb.indexr, err = tsdb.NewIndexReader(dir)
	if err != nil {
		return nil, err
	}

	sb.chunkr, err = newChunkReader(mc, bucket, key, nil)
	if err != nil {
		return nil, errors.Wrap(err, "new chunk reader")
	}

	return sb, nil
}

func (b *s3Block) Meta() tsdb.BlockMeta {
	return b.meta
}

func (b *s3Block) Dir() string {
	return b.dir
}

func (b *s3Block) Chunks() tsdb.ChunkReader {
	return b.chunkr
}

func (b *s3Block) Index() tsdb.IndexReader {
	return b.indexr
}

func (b *s3Block) Close() error {
	return b.chunkr.Close()
}

func (b *s3Block) Delete(int64, int64, ...labels.Matcher) error {
	return nil
}

func (b *s3Block) Snapshot(string) error {
	return nil
}

func (b *s3Block) Tombstones() tsdb.TombstoneReader {
	return nopTombstones{}
}
