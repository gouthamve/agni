package chunkr

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/gouthamve/agni/pkg/fscache"
	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
)

func init() {
	//prometheus.MustRegister(s3Reqs)
}

type FSCacheProvider struct {
	cache *fscache.Cache

	chunkSize int64
	cacheDir  string
}

func NewFSCProvider(mc *minio.Client, bucket, cacheDir string, chunkSize, cacheSize int64) (ReaderProvider, error) {
	cache, err := fscache.NewCache(
		int(cacheSize/chunkSize),
		cacheDir,
		func(key string) ([]byte, error) {
			ss := strings.Split(key, "-")
			if len(ss) != 2 {
				return nil, fmt.Errorf("invalid key: %q", key)
			}

			off, err := strconv.ParseInt(ss[1], 10, 64)
			if err != nil {
				return nil, errors.Wrap(err, "converting offset from string")
			}

			obj, err := mc.GetObject(bucket, ss[0])
			if err != nil {
				return nil, errors.Wrapf(err, "getting object: %q", ss[0])
			}
			defer obj.Close()

			b := make([]byte, chunkSize)
			n, err := obj.ReadAt(b, off)
			if err != nil {
				if err != io.EOF {
					return nil, errors.Wrapf(err, "reading chunk from object storage, key: %q, off: %d", ss[0], off)
				}
			}
			b = b[:n]

			return b, nil
		},
	)
	if err != nil {
		return nil, errors.Wrap(err, "creating cache")
	}

	return FSCacheProvider{
		cache:     cache,
		chunkSize: chunkSize,
		cacheDir:  cacheDir,
	}, nil
}

func (fsp FSCacheProvider) Reader(obj *minio.Object) (Reader, error) {
	oi, err := obj.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat() object")
	}

	return &FSCacheReader{key: oi.Key, chunkSize: fsp.chunkSize, cache: fsp.cache}, nil
}

type FSCacheReader struct {
	key       string
	chunkSize int64
	cache     *fscache.Cache

	offset int64
}

func (fsr *FSCacheReader) ReadAt(b []byte, offset int64) (int, error) {
	twoChunks := false
	l := len(b)

	chunkStart := (offset / fsr.chunkSize) * fsr.chunkSize
	if chunkStart+fsr.chunkSize < offset+int64(len(b)) {
		twoChunks = true
		// TODO: It could be more than 2 chunks!
	}

	key := fmt.Sprintf("%s-%d", fsr.key, chunkStart)
	val, err := fsr.cache.Get(key)
	if err != nil {
		return 0, errors.Wrapf(err, "reading cache key: %q", key)
	}

	n := copy(b, val.B[offset-chunkStart:])
	b = b[:n]

	if twoChunks {
		key := fmt.Sprintf("%s-%d", fsr.key, chunkStart+fsr.chunkSize)
		val, err = fsr.cache.Get(key)
		if err != nil {
			return 0, errors.Wrapf(err, "reading cache key: %q", key)
		}

		b = append(b, val.B[:l-len(b)]...)
	}

	return len(b), nil
}

func (fsr *FSCacheReader) Read(b []byte) (int, error) {
	n, err := fsr.ReadAt(b, fsr.offset)
	fsr.offset += int64(n)
	return n, err
}

func (fsr *FSCacheReader) Close() error {
	return nil
}

func mmap(f *os.File) ([]byte, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "stat() file: %q", f.Name())
	}

	return syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
}
