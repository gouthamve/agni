package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/gouthamve/agni/pkg/chunkr"
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
	bs  []chunkr.Reader
	ois []minio.ObjectInfo

	pool chunks.Pool
}

// newChunkReader returns a new chunkReader based on mmaped files found in dir.
func newChunkReader(mc *minio.Client, bucket, block string, pool chunks.Pool, rp chunkr.ReaderProvider) (*chunkReader, error) {
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
	cr := chunkReader{bs: make([]chunkr.Reader, 0, len(keys)), pool: pool}

	for i, key := range keys {
		obj, err := mc.GetObject(bucket, key)
		if err != nil {
			return nil, errors.Wrapf(err, "read chunks: %q", key)
		}
		defer obj.Close()

		oi, err := obj.Stat()
		if err != nil {
			return nil, errors.Wrap(err, "read object stats")
		}

		r, err := rp.Reader(obj)
		if err != nil {
			return nil, errors.Wrap(err, "get chunkr.Reader from minio.Object")
		}
		cr.bs = append(cr.bs, r)

		if oi.Size < 4 {
			return nil, errors.Wrapf(errInvalidSize, "validate magic in segment %d", i)
		}
		// Verify magic number.
		buf := make([]byte, 4)
		_, err = obj.Read(buf)
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
		return nil, errors.Errorf("reference sequence %d out of range, object: %s", seq, s.ois)
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
			fmt.Println("Error buf=0", s.ois[seq], ref)
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

	for i := range cs {
		if int(cs[i].Ref>>32) == seq {
			gp = append(gp, &cs[i])
			continue
		}

		gps = append(gps, gp)
		seq = int(cs[i].Ref >> 32)
		gp = []*tsdb.ChunkMeta{&cs[i]}
	}
	gps = append(gps, gp)

	g, _ := errgroup.New(context.Background(), len(gps))

	for i := range gps {
		i := i
		g.Add(func() error {
			return s.populate(gps[i])
		})
	}

	return g.Run()
}

var maxChunkLen int64 = 1 << 11

// TODO: Make a note that all chunks should be in the same segment.
func (s *chunkReader) populate(cs []*tsdb.ChunkMeta) error {
	if len(cs) == 0 {
		return nil
	}

	seq := int(cs[0].Ref >> 32)
	startOff := int64((cs[0].Ref << 32) >> 32)
	endOff := int64((cs[len(cs)-1].Ref<<32)>>32) + maxChunkLen

	b := s.bs[seq]

	buf := make([]byte, endOff-startOff)
	n, err := b.ReadAt(buf, startOff)

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
		// TODO: Inplace shorten buf.
		ref := int64((c.Ref << 32) >> 32)
		buf2 := buf[ref-startOff:]
		l, n := binary.Uvarint(buf2)
		if n < 0 {
			return fmt.Errorf("reading chunk length failed")
		}

		buf2 = buf2[n:]

		c.Chunk, err = s.pool.Get(chunks.Encoding(buf2[0]), buf2[1:1+l])
		if err != nil {
			return errors.Wrapf(err, "get chunk from pool, enc: %d, ref: %d, object: %s", buf2[0], c.Ref, s.ois[seq].Key)
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
