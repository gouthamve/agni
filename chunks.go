package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/gouthamve/agni/pkg/errgroup"
	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunks"
)

const chunkSuffix = "/chunks/"

var (
	errInvalidSize = fmt.Errorf("invalid size")
	errInvalidFlag = fmt.Errorf("invalid flag")
)

// ChunkReader provides reading access of serialized time series data.
type ChunkReader interface {
	// Chunk returns the series data chunk with the given reference.
	Chunk(ref uint64) (chunks.Chunk, error)

	Populate(cs []tsdb.ChunkMeta) error

	// Close releases all underlying resources of the reader.
	Close() error
}

// chunkReader implements a SeriesReader for a serialized byte stream
// of series data.
type chunkReader struct {
	// The underlying bytes holding the encoded series data.
	bs  []*minio.Object
	ois []minio.ObjectInfo

	pool chunks.Pool
}

// newChunkReader returns a new chunkReader based on mmaped files found in dir.
func newChunkReader(mc *minio.Client, bucket, block string, pool chunks.Pool) (*chunkReader, error) {
	doneCh := make(chan struct{})
	keys := make([]string, 0)
	objCh := mc.ListObjectsV2(bucket, block+chunkSuffix, false, doneCh)
	for object := range objCh {
		if object.Err != nil {
			close(doneCh)
			return nil, errors.Wrapf(object.Err, "listing objects with prefix: %q", block+chunkSuffix)
		}

		keys = append(keys, object.Key)
	}
	close(doneCh)
	sort.Strings(keys)

	if pool == nil {
		pool = chunks.NewPool()
	}
	cr := chunkReader{bs: make([]*minio.Object, 0, len(keys)), pool: pool}

	for _, key := range keys {
		obj, err := mc.GetObject(bucket, key)
		if err != nil {
			return nil, errors.Wrapf(err, "read chunks: %q", key)
		}

		cr.bs = append(cr.bs, obj)
	}

	cr.ois = make([]minio.ObjectInfo, 0, len(cr.bs))
	for i, b := range cr.bs {
		oi, err := b.Stat()
		if err != nil {
			return nil, errors.Wrap(err, "read object stats")
		}

		if oi.Size < 4 {
			return nil, errors.Wrapf(errInvalidSize, "validate magic in segment %d", i)
		}
		// Verify magic number.
		buf := make([]byte, 4)
		_, err = b.Read(buf)
		if err != nil {
			return nil, err
		}

		if m := binary.BigEndian.Uint32(buf); m != tsdb.MagicChunks {
			return nil, fmt.Errorf("invalid magic number %x", m)
		}

		cr.ois = append(cr.ois, oi)
	}
	return &cr, nil
}

func (s *chunkReader) Close() error {
	cs := make([]io.Closer, 0, len(s.bs))
	for _, b := range s.bs {
		cs = append(cs, b)
	}

	return closeAll(cs...)
}

func (s *chunkReader) Chunk(ref uint64) (chunks.Chunk, error) {
	var (
		seq = int(ref >> 32)
		off = int((ref << 32) >> 32)
	)
	if seq >= len(s.bs) {
		return nil, errors.Errorf("reference sequence %d out of range", seq)
	}
	b := s.bs[seq]
	oi := s.ois[seq]

	if int64(off) >= oi.Size {
		return nil, errors.Errorf("offset %d beyond data size %d", off, oi.Size)
	}

	chunkLen := 2 << 11
	for {
		buf := make([]byte, chunkLen)
		n, err := b.ReadAt(buf, int64(off))
		if err != nil {
			if err != io.EOF {
				return nil, err
			}

			buf = buf[:n]
		}

		if len(buf) == 0 {
			// TODO: FIXME
			continue
		}

		l, n := binary.Uvarint(buf)
		if n < 0 {
			return nil, fmt.Errorf("reading chunk length failed")
		}

		buf = buf[n:]
		if int(l) > len(buf)+2 {
			chunkLen *= 2
			continue
		}

		return s.pool.Get(chunks.Encoding(buf[0]), buf[1:1+l])
	}

	return nil, nil
}

func (s *chunkReader) Populate(cs []tsdb.ChunkMeta) error {
	if len(cs) == 0 {
		return nil
	}

	gps := [][]*tsdb.ChunkMeta{}
	gp := []*tsdb.ChunkMeta{}

	seq := int(cs[0].Ref >> 32)
	if seq >= len(s.bs) {
		return errors.Errorf("reference sequence %d out of range", seq)
	}

	for _, c := range cs {
		if int(c.Ref>>32) == seq {
			gp = append(gp, &c)
			continue
		}

		gps = append(gps, gp)
		seq = int(c.Ref >> 32)
		gp = []*tsdb.ChunkMeta{&c}
	}

	g, _ := errgroup.New(context.Background(), len(gps))

	for i := range gps {
		i := i
		g.Add(func() error {
			return s.populate(gps[i])
		})
	}

	return g.Run()
}

var maxChunkLen = 1 << 12

func (s *chunkReader) populate(cs []*tsdb.ChunkMeta) error {
	if len(cs) == 0 {
		return nil
	}

	seq := int(cs[0].Ref >> 32)
	startRef := int((cs[0].Ref << 32) >> 32)
	endRef := int((cs[len(cs)-1].Ref<<32)>>32) + maxChunkLen

	b := s.bs[seq]
	oi := s.ois[seq]
	if int64(endRef) >= oi.Size {
		// TODO: start and endRef == int64??
		endRef = int(oi.Size)
	}

	buf := make([]byte, endRef-startRef)
	n, err := b.ReadAt(buf, int64(startRef))
	if err != nil {
		if err != io.EOF {
			return err
		}

		buf = buf[:n]
	}

	if len(buf) == 0 {
		fmt.Println("Error buf=0", s.ois[seq], cs[0].Ref)
		// TODO: FIXME
	}

	for _, c := range cs {
		l, n := binary.Uvarint(buf)
		if n < 0 {
			return fmt.Errorf("reading chunk length failed")
		}

		buf = buf[n:]
		c.Chunk, err = s.pool.Get(chunks.Encoding(buf[0]), buf[1:1+l])
		if err != nil {
			return err
		}
	}

	return nil
}

func closeAll(cs ...io.Closer) error {
	var merr tsdb.MultiError

	for _, c := range cs {
		merr.Add(c.Close())
	}
	return merr.Err()
}
