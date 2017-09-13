package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/go-kit/kit/log"
	minio "github.com/minio/minio-go"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
)

const (
	metaSuffix  = "/meta.json"
	indexSuffix = "/index"
)

// DB is the TODO struct.
type DB struct {
	blocks []tsdb.DiskBlock

	mtx sync.RWMutex

	client *minio.Client
	bucket string
	dir    string

	logger log.Logger
}

// NewDB is TODO stuff.
func NewDB(rcfg remoteConfig, mc *minio.Client, logger log.Logger) (*DB, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	db := &DB{
		blocks: make([]tsdb.DiskBlock, 0),
		client: mc,
		bucket: rcfg.Bucket,
		logger: logger,
		dir:    "data", // TODO: Make it a parameter.
	}

	if err := db.reload(); err != nil {
		return nil, err
	}

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
			m[reverse(b.Meta().ULID.String())] = struct{}{}
		}
		s.mtx.RUnlock()

		blocks, err := getStorageBlocks(s.client, s.bucket)
		if err != nil {
			logger.Log("error", err.Error())
			continue
		}
		logger.Log("debug", "checking for new blocks.", "localBlocks", len(m), "remoteBlocks", len(blocks))

		newBlocks := 0
		for _, b := range blocks {
			if _, ok := m[b]; ok {
				continue
			}

			folder := filepath.Join(s.dir, reverse(b))
			tmpFolder := folder + ".tmp"

			if err := os.RemoveAll(tmpFolder); err != nil {
				logger.Log("msg", "removing any existing tmpFolder", "error", err.Error())
				continue
			}

			// Download the meta and index files.
			if err := s.client.FGetObject(s.bucket, b+indexSuffix, filepath.Join(tmpFolder, indexSuffix)); err != nil {
				logger.Log("msg", "download index file", "key", b, "error", err.Error())
				continue
			}

			if err := s.client.FGetObject(s.bucket, b+metaSuffix, filepath.Join(tmpFolder, metaSuffix)); err != nil {
				logger.Log("msg", "download meta file", "error", err.Error())
				continue
			}

			if err := renameFile(tmpFolder, folder); err != nil {
				logger.Log("msg", "rename temp folder", "error", err.Error())
				continue
			}

			newBlocks++
			if err := s.reload(); err != nil {
				logger.Log("msg", "reloading blocks", "error", err.Error())
			}
		}

		if newBlocks == 0 {
			continue
		}

		// Check if we need to reload here.
		if err := s.reload(); err != nil {
			logger.Log("msg", "reloading blocks", "error", err.Error())
		}
	}
}

func (db *DB) getBlock(id ulid.ULID) (tsdb.DiskBlock, bool) {
	for _, b := range db.blocks {
		if b.Meta().ULID == id {
			return b, true
		}
	}
	return nil, false
}

func (db *DB) reload() error {
	var cs []io.Closer
	defer func() { closeAll(cs...) }()

	dirs, err := blockDirs(db.dir)
	if err != nil {
		return errors.Wrap(err, "find blocks")
	}
	var (
		blocks []tsdb.DiskBlock
		exist  = map[ulid.ULID]struct{}{}
	)

	for _, dir := range dirs {
		meta, err := readMetaFile(dir)
		if err != nil {
			return errors.Wrapf(err, "read meta information %s", dir)
		}

		b, ok := db.getBlock(meta.ULID)
		if !ok {
			b, err = newS3Block(db.client, db.bucket, reverse(meta.ULID.String()), dir)
			if err != nil {
				return errors.Wrapf(err, "open block %s", dir)
			}
		}

		blocks = append(blocks, b)
		exist[meta.ULID] = struct{}{}
	}

	if err := validateBlockSequence(blocks); err != nil {
		return errors.Wrap(err, "invalid block sequence")
	}

	// Close all opened blocks that no longer exist after we returned all locks.
	// TODO(fabxc: probably races with querier still reading from them. Can
	// we just abandon them and have the open FDs be GC'd automatically eventually?
	for _, b := range db.blocks {
		if _, ok := exist[b.Meta().ULID]; !ok {
			cs = append(cs, b)
		}
	}

	db.mtx.Lock()
	db.blocks = blocks
	db.mtx.Unlock()
	return nil
}

func blockDirs(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var dirs []string

	for _, fi := range files {
		if isBlockDir(fi) {
			dirs = append(dirs, filepath.Join(dir, fi.Name()))
		}
	}
	return dirs, nil
}

func isBlockDir(fi os.FileInfo) bool {
	if !fi.IsDir() {
		return false
	}
	_, err := ulid.Parse(fi.Name())
	return err == nil
}

func validateBlockSequence(bs []tsdb.DiskBlock) error {
	if len(bs) == 0 {
		return nil
	}
	sort.Slice(bs, func(i, j int) bool {
		return bs[i].Meta().MinTime < bs[j].Meta().MinTime
	})
	prev := bs[0]
	for _, b := range bs[1:] {
		if b.Meta().MinTime < prev.Meta().MaxTime {
			return errors.Errorf("block time ranges overlap (%d, %d)", b.Meta().MinTime, prev.Meta().MaxTime)
		}
	}
	return nil
}

type blockMeta struct {
	Version int `json:"version"`

	*tsdb.BlockMeta
}

func readMetaFile(dir string) (*tsdb.BlockMeta, error) {
	b, err := ioutil.ReadFile(filepath.Join(dir, metaSuffix))
	if err != nil {
		return nil, err
	}
	var m blockMeta

	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m.Version != 1 {
		return nil, errors.Errorf("unexpected meta file version %d", m.Version)
	}

	return m.BlockMeta, nil
}

func renameFile(from, to string) error {
	if err := os.RemoveAll(to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := fileutil.OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}
	defer pdir.Close()

	if err = fileutil.Fsync(pdir); err != nil {
		return err
	}
	if err = pdir.Close(); err != nil {
		return err
	}
	return nil
}
