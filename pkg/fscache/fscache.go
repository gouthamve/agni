package fscache

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
)

type Cache struct {
	lru         *simplelru.LRU
	cacheFolder string
	stats       *cacheStats
	getter      GetterFunc

	loadGroup *singleflight.Group

	mtx sync.RWMutex
}

type GetterFunc func(key string) ([]byte, error)

func NewCache(size int, cacheDir string, f GetterFunc, r prometheus.Registerer) (*Cache, error) {
	if err := os.RemoveAll(cacheDir); err != nil {
		return nil, errors.Wrapf(err, "clearing cache dir: %q", cacheDir)
	}
	if err := os.MkdirAll(cacheDir, 0777); err != nil {
		return nil, errors.Wrapf(err, "creating cache dir: %q", cacheDir)
	}

	stats := &cacheStats{}
	ecb := func(key, value interface{}) {
		evictCB(key, value)

		atomic.AddInt64(&stats.Evictions, 1)
		atomic.AddInt64(&stats.Items, -1)
	}

	lru, err := simplelru.NewLRU(size, ecb)
	if err != nil {
		return nil, errors.Wrap(err, "creating a new simplelru")
	}

	if r != nil {
		registerMetrics(r, stats)
	}

	return &Cache{
		lru:         lru,
		getter:      f,
		cacheFolder: cacheDir,

		stats: stats,

		loadGroup: &singleflight.Group{},
	}, nil
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *Cache) Add(key, file string) (bool, error) {
	atomic.AddInt64(&c.stats.Items, 1)
	// Add a file.
	f, err := os.Open(file)
	if err != nil {
		return false, errors.Wrapf(err, "opening the file: %q", file)
	}

	fi, err := f.Stat()
	if err != nil {
		return false, errors.Wrapf(err, "getting the stats for file: %q", file)
	}

	b, err := mmap(f, int(fi.Size()))
	if err != nil {
		return false, errors.Wrapf(err, "mmapping file: %q", file)
	}

	return c.lru.Add(key, Val{b, f}), nil
}

func (c *Cache) Get(key string) (Val, error) {
	atomic.AddInt64(&c.stats.Gets, 1)

	v, ok := c.lru.Get(key)
	if ok {
		atomic.AddInt64(&c.stats.Hits, 1)
		return v.(Val), nil
	}

	v, err, _ := c.loadGroup.Do(key, func() (interface{}, error) {
		// Check the cache again because singleflight can only dedup calls
		// that overlap concurrently.  It's possible for 2 concurrent
		// requests to miss the cache, resulting in 2 load() calls.  An
		// unfortunate goroutine scheduling would result in this callback
		// being run twice, serially.  If we don't check the cache again,
		// cache.nbytes would be incremented below even though there will
		// be only one entry for this key.
		//
		// Consider the following serialized event ordering for two
		// goroutines in which this callback gets called twice for hte
		// same key:
		// 1: Get("key")
		// 2: Get("key")
		// 1: loadGroup.Do("key", fn)
		// 1: fn()
		// 2: loadGroup.Do("key", fn)
		// 2: fn()

		if v, ok := c.lru.Get(key); ok {
			atomic.AddInt64(&c.stats.Hits, 1)
			return v.(Val), nil
		}

		atomic.AddInt64(&c.stats.LoadsDeduped, 1)

		b, err := c.getter(key)
		if err != nil {
			atomic.AddInt64(&c.stats.Errors, 1)
			return Val{}, errors.Wrapf(err, "getting data for key: %q", key)
		}

		path := filepath.Join(c.cacheFolder, key)
		if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
			atomic.AddInt64(&c.stats.Errors, 1)
			return Val{}, errors.Wrapf(err, "creating dir for key: %q", key)
		}

		if err := ioutil.WriteFile(path, b, 0777); err != nil {
			atomic.AddInt64(&c.stats.Errors, 1)
			return Val{}, errors.Wrapf(err, "writing bytes to file: %q", path)
		}

		_, err = c.Add(key, path)
		if err != nil {
			atomic.AddInt64(&c.stats.Errors, 1)
			return Val{}, errors.Wrapf(err, "adding cached file to cache, key: %q", key)
		}

		v, _ = c.lru.Get(key)
		return v, nil
	})

	return v.(Val), err
}

type cacheStats struct {
	Items     int64
	Gets      int64
	Hits      int64
	Evictions int64

	Errors       int64
	LoadsDeduped int64
}

type Val struct {
	B []byte   // The mmap-ed bytes.
	f *os.File // The file itself.
}

func registerMetrics(r prometheus.Registerer, cs *cacheStats) {
	r.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "fscache",
		Name:      "items",
		Help:      "The number of items currently in the cache.",
	}, func() float64 { return float64(atomic.LoadInt64(&cs.Items)) }))

	r.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "fscache",
		Name:      "gets_total",
		Help:      "The number of gets processed.",
	}, func() float64 { return float64(atomic.LoadInt64(&cs.Gets)) }))

	r.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "fscache",
		Name:      "hits_total",
		Help:      "The number of times the key was locally available.",
	}, func() float64 { return float64(atomic.LoadInt64(&cs.Hits)) }))

	r.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "fscache",
		Name:      "evictions_total",
		Help:      "The number of items evicted from cache.",
	}, func() float64 { return float64(atomic.LoadInt64(&cs.Evictions)) }))

	r.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "fscache",
		Name:      "errors_total",
		Help:      "The number of errors that occurred while cache-filling.",
	}, func() float64 { return float64(atomic.LoadInt64(&cs.Errors)) }))

	r.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "fscache",
		Name:      "loads_deduped_total",
		Help:      "The number of loads that were de-duped via singleflight.",
	}, func() float64 { return float64(atomic.LoadInt64(&cs.Errors)) }))
}

func evictCB(key, value interface{}) {
	v := value.(Val)

	// TODO: Handle errors.
	munmap(v.B)
	v.f.Close()
	os.RemoveAll(v.f.Name())
	return
}

func mmap(f *os.File, length int) ([]byte, error) {
	return syscall.Mmap(int(f.Fd()), 0, length, syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmap(b []byte) (err error) {
	return syscall.Munmap(b)
}
