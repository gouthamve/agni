package chunkr

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/golang/groupcache"
	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// TODO: Move away from globals.
var (
	s3Reqs = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agni",
		Name:      "s3_reqs_total",
		Help:      "The total number of requests to S3",
	})
)

func init() {
	prometheus.MustRegister(s3Reqs)
}

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

				s3Reqs.Inc()
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

		// TODO: Is this the right place to put it?
		if err := registerMetrics(g); err != nil {
			panic(err)
		}
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

func registerMetrics(g *groupcache.Group) error {
	// TODO: Make this a collector.
	if err := prometheus.Register(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "groupcache",
			Subsystem: "main",
			Name:      "bytes_stored",
			Help:      "The bytes stored in the main cache.",
		},
		func() float64 { return float64(g.CacheStats(groupcache.MainCache).Bytes) },
	)); err != nil {
		return err
	}

	if err := prometheus.Register(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "groupcache",
			Subsystem: "main",
			Name:      "items_stored",
			Help:      "The number of items stored in the main cache.",
		},
		func() float64 { return float64(g.CacheStats(groupcache.MainCache).Items) },
	)); err != nil {
		return err
	}

	if err := prometheus.Register(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: "groupcache",
			Subsystem: "main",
			Name:      "gets_total",
			Help:      "The total number of get reqs served.",
		},
		func() float64 { return float64(g.CacheStats(groupcache.MainCache).Gets) },
	)); err != nil {
		return err
	}

	if err := prometheus.Register(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: "groupcache",
			Subsystem: "main",
			Name:      "hits_total",
			Help:      "The total number of requests where the key was in cache.",
		},
		func() float64 { return float64(g.CacheStats(groupcache.MainCache).Hits) },
	)); err != nil {
		return err
	}

	if err := prometheus.Register(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: "groupcache",
			Subsystem: "main",
			Name:      "evictions_total",
			Help:      "The total number of keys evicted.",
		},
		func() float64 { return float64(g.CacheStats(groupcache.MainCache).Evictions) },
	)); err != nil {
		return err
	}

	if err := prometheus.Register(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "groupcache",
			Subsystem: "hot",
			Name:      "bytes_stored",
			Help:      "The bytes stored in the hot cache.",
		},
		func() float64 { return float64(g.CacheStats(groupcache.HotCache).Bytes) },
	)); err != nil {
		return err
	}

	if err := prometheus.Register(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "groupcache",
			Subsystem: "hot",
			Name:      "items_stored",
			Help:      "The number of items stored in the hot cache.",
		},
		func() float64 { return float64(g.CacheStats(groupcache.HotCache).Items) },
	)); err != nil {
		return err
	}

	if err := prometheus.Register(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: "groupcache",
			Subsystem: "hot",
			Name:      "gets_total",
			Help:      "The total number of get reqs served.",
		},
		func() float64 { return float64(g.CacheStats(groupcache.HotCache).Gets) },
	)); err != nil {
		return err
	}

	if err := prometheus.Register(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: "groupcache",
			Subsystem: "hot",
			Name:      "hits_total",
			Help:      "The total number of requests where the key was in cache.",
		},
		func() float64 { return float64(g.CacheStats(groupcache.HotCache).Hits) },
	)); err != nil {
		return err
	}

	if err := prometheus.Register(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: "groupcache",
			Subsystem: "hot",
			Name:      "evictions_total",
			Help:      "The total number of keys evicted.",
		},
		func() float64 { return float64(g.CacheStats(groupcache.HotCache).Evictions) },
	)); err != nil {
		return err
	}

	return nil
}
