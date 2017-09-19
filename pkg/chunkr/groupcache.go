package chunkr

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/golang/groupcache"
	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
)

type GCProvider struct {
	g *groupcache.Group

	chunkSize int64
}

func NewGCProvider(mc *minio.Client, bucket string, chunkSize int64) ReaderProvider {
	g := groupcache.GetGroup("chunks")

	if g == nil {
		g = groupcache.NewGroup("chunks", 8<<30, groupcache.GetterFunc(
			func(ctx groupcache.Context, key string, dest groupcache.Sink) error {
				ss := strings.Split(key, "-")
				if len(ss) != 2 {
					return fmt.Errorf("invalid key: %s", key)
				}

				off, err := strconv.ParseInt(ss[1], 10, 64)
				if err != nil {
					return errors.Wrap(err, "converting offset to string")
				}

				obj, err := mc.GetObject(bucket, ss[0])
				if err != nil {
					return errors.Wrapf(err, "get object: %s", ss[0])
				}

				b := make([]byte, chunkSize)
				n, err := obj.ReadAt(b, off)
				if err != nil {
					if err != io.EOF {
						return errors.Wrapf(err, "read the chunk from object storage, key: %s, off: %d", ss[0], off)
					}
				}
				b = b[:n]

				return errors.Wrap(dest.SetBytes(b), "put into dest")
			},
		))
	}
	return GCProvider{g: g, chunkSize: chunkSize}
}

func (gp GCProvider) Reader(obj *minio.Object) (Reader, error) {
	oi, err := obj.Stat()
	if err != nil {
		return nil, err
	}

	return &GCReader{key: oi.Key, g: gp.g, chunkSize: gp.chunkSize}, nil
}

type GCReader struct {
	key    string
	offset int64
	obj    *minio.Object

	chunkSize int64
	g         *groupcache.Group
}

func (gcr *GCReader) Read(b []byte) (int, error) {
	n, err := gcr.ReadAt(b, gcr.offset)
	gcr.offset += int64(n)
	return n, err
}

func (gcr *GCReader) ReadAt(b []byte, offset int64) (int, error) {
	twoChunks := false

	chunkStart := (offset / gcr.chunkSize) * gcr.chunkSize
	if chunkStart+gcr.chunkSize < offset+int64(len(b)) {
		twoChunks = true
	}

	key := fmt.Sprintf("%s-%d", gcr.key, chunkStart)
	var byt []byte // TODO: Can we preallocate?
	if err := gcr.g.Get(nil, key, groupcache.AllocatingByteSliceSink(&byt)); err != nil {
		return 0, errors.Wrapf(err, "read from groupcache, key: %s", key)
	}

	if twoChunks {
		key = fmt.Sprintf("%s-%d", gcr.key, chunkStart+gcr.chunkSize)
		var b2 []byte
		if err := gcr.g.Get(nil, key, groupcache.AllocatingByteSliceSink(&b2)); err != nil {
			return 0, errors.Wrapf(err, "read from groupcache, key: %s", key)
		}

		byt = append(byt, b2...)
	}

	return copy(b, byt[offset-chunkStart:]), nil
}

func (gcr *GCReader) Close() error { return gcr.obj.Close() }
