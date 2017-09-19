package chunkr

import (
	"io"

	minio "github.com/minio/minio-go"
)

type Reader interface {
	io.ReadCloser
	io.ReaderAt
}

type ReaderProvider interface {
	Reader(*minio.Object) (Reader, error)
}

type SimpleRP struct{}

func (SimpleRP) Reader(o *minio.Object) (Reader, error) { return o, nil }
