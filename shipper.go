package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"gopkg.in/fsnotify.v1"
)

var prefix = "promblock-"

func isValidBlockDir(dirName string, bd os.FileInfo, logger log.Logger) bool {
	if !isBlockDir(bd) {
		return false
	}

	byt, err := ioutil.ReadFile(filepath.Join(dirName, bd.Name(), "meta.json"))
	if err != nil {
		level.Error(logger).Log("msg", "check block dir", "error", err.Error())
		return false
	}

	var bm tsdb.BlockMeta
	if err := json.Unmarshal(byt, &bm); err != nil {
		level.Error(logger).Log("msg", "check block dir", "error", err.Error())
		return false
	}

	if bm.Compaction.Level == 1 {
		return true
	}

	return false
}

func refresh(dirName string, logger log.Logger) []string {

	blockDirs, err := ioutil.ReadDir(dirName)
	if err != nil {
		level.Error(logger).Log("error", err.Error())
		os.Exit(1)
	}

	blocks := make([]string, 0, len(blockDirs))
	for _, bd := range blockDirs {
		if !isValidBlockDir(dirName, bd, logger) {
			continue
		}

		blocks = append(blocks, filepath.Join(dirName, bd.Name()))

	}
	return blocks
}

func hardlink(blockDir string, lazyDataDir string, logger log.Logger) {
	fnameSplit := strings.Split(blockDir, "/")
	bName := fnameSplit[len(fnameSplit)-1]

	lazyBlockDir := filepath.Join(lazyDataDir, bName)
	if _, err := os.Stat(lazyBlockDir); !os.IsNotExist(err) {
		return
	}

	if err := os.MkdirAll(lazyBlockDir, 0777); err != nil {
		level.Error(logger).Log("msg", "creation of lazy block dir", "error", err.Error())
	}

	lazyChunksDir := filepath.Join(lazyBlockDir, "chunks")
	if err := os.MkdirAll(lazyChunksDir, 0777); err != nil {
		level.Error(logger).Log("msg", "creation of lazy chunks dir", "error", err.Error())
	}

	// Hardlink meta, index and tombstones
	for _, fname := range []string{
		metaSuffix,
		indexSuffix,
		//tombstoneFilename,
	} {
		if err := os.Link(filepath.Join(blockDir, fname), filepath.Join(lazyBlockDir, fname)); err != nil {
			level.Error(logger).Log("msg", "hardlink", "error", err.Error())
		}
	}

	// Hardlink chunks
	ChunkDir := filepath.Join(blockDir, "chunks")
	files, err := ioutil.ReadDir(ChunkDir)
	if err != nil {
		level.Error(logger).Log("msg", "read chunksDir", "error", err.Error())
	}

	for _, f := range files {
		err := os.Link(filepath.Join(ChunkDir, f.Name()), filepath.Join(lazyChunksDir, f.Name()))
		if err != nil {
			level.Error(logger).Log("msg", "hardlink chunks", "error", err.Error())
		}
	}

}

func stopLazyShip(done chan bool) {
	done <- true
}

func closeShipper(tempDir string, logger log.Logger) {
	os.RemoveAll(tempDir)
	level.Error(logger).Log("msg", "closing the shipper ...")
}

func startShipper(configFile string, logger log.Logger, dataDir string, tempDir string) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	rcfg, err := loadConfig(configFile)
	if err != nil {
		level.Error(logger).Log("msg", "load config", "error", err.Error())
		os.Exit(1)
	}

	mc, err := minio.New(rcfg.Endpoint, rcfg.AccessKey, rcfg.SecretKey, rcfg.UseSSL)
	if err != nil {
		level.Error(logger).Log("msg", "initialise minio client", "error", err.Error())
		os.Exit(1)
	}

	s, err := newShipper(mc, rcfg.Bucket, log.With(logger, "component", "shipper"))
	if err != nil {
		level.Error(logger).Log("msg", "initialise shipper", "error", err.Error())
		os.Exit(1)
	}

	endLazyShipper := make(chan bool)

	go s.lazyShipper(dataDir, tempDir, logger, endLazyShipper)
	defer stopLazyShip(endLazyShipper)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		blocks := refresh(tempDir, logger)

		if err := s.shipBlocks(blocks); err != nil {
			level.Error(logger).Log("msg", "shipping blocks", "error", err.Error())
		}
	}

}

type shipper struct {
	client *minio.Client
	bucket string
	logger log.Logger

	blocks map[string]struct{}
}

func newShipper(mc *minio.Client, bucket string, logger log.Logger) (*shipper, error) {
	blocks, err := getStorageBlocks(mc, bucket)
	if err != nil {
		return nil, err
	}

	existing := make(map[string]struct{})
	for _, block := range blocks {
		// The blocks returned are inverted ones.
		existing[reverse(block)] = struct{}{}
	}

	return &shipper{
		client: mc,
		bucket: bucket,
		blocks: existing,
		logger: logger,
	}, nil
}

func (s *shipper) lazyShipper(dataDir string, lazyDataDir string, logger log.Logger, done <-chan bool) {
	watcher, err := fsnotify.NewWatcher()
	defer watcher.Close()
	if err != nil {
		level.Error(logger).Log("msg", "initialise watcher on data dir", "error", err.Error())
	}
	watcher.Add(dataDir)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case event := <-watcher.Events:

			if event.Op&fsnotify.Create != fsnotify.Create {
				break
			}

			blockDirs := refresh(dataDir, logger)

			for _, blockDir := range blockDirs {

				fnameSplit := strings.Split(blockDir, "/")
				bName := fnameSplit[len(fnameSplit)-1]

				if _, ok := s.blocks[bName]; ok {
					continue
				}

				hardlink(blockDir, lazyDataDir, logger)
			}

			ticker.Stop()
			ticker = time.NewTicker(5 * time.Second)

		case <-ticker.C:
			blockDirs := refresh(dataDir, logger)

			for _, blockDir := range blockDirs {

				fnameSplit := strings.Split(blockDir, "/")
				bName := fnameSplit[len(fnameSplit)-1]

				if _, ok := s.blocks[bName]; ok {
					continue
				}

				hardlink(blockDir, lazyDataDir, logger)
			}

		case err := <-watcher.Errors:
			if err != nil {
				level.Error(logger).Log("msg", "watcher on dataDir", "error", err.Error())
			}

		case <-done:
			return

		}
	}
}

func (s *shipper) shipBlocks(blocks []string) error {
	for _, block := range blocks {
		fnameSplit := strings.Split(block, "/")
		bName := fnameSplit[len(fnameSplit)-1]

		if _, ok := s.blocks[bName]; ok {
			continue
		}

		blockKey := reverse(bName)

		// Put chunks.
		chunksPath := filepath.Join(block, "chunks")
		chunksPathKey := blockKey + chunkSuffix
		files, err := ioutil.ReadDir(chunksPath)
		if err != nil {
			return err
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			reader, err := os.Open(filepath.Join(chunksPath, file.Name()))
			if err != nil {
				return err
			}

			_, err = s.client.PutObject(s.bucket, chunksPathKey+file.Name(), reader, "application/octet-stream")
			if err != nil {
				return err
			}
		}

		// Put index.
		reader, err := os.Open(filepath.Join(block, "index"))
		if err != nil {
			return err
		}

		_, err = s.client.PutObject(s.bucket, blockKey+indexSuffix, reader, "application/octet-stream")
		if err != nil {
			return err
		}

		// Put meta.json.
		reader, err = os.Open(filepath.Join(block, "meta.json"))
		if err != nil {
			return err
		}

		_, err = s.client.PutObject(s.bucket, blockKey+metaSuffix, reader, "application/json")
		if err != nil {
			return err
		}

		// TODO: Put tombstones.

		// Add the lookup key.
		r := strings.NewReader("yolo")
		_, err = s.client.PutObject(s.bucket, prefix+blockKey, r, "application/text")
		if err != nil {
			return err
		}

		level.Info(s.logger).Log("msg", fmt.Sprintf("added block %q\n", blockKey))
		s.blocks[bName] = struct{}{}

		os.RemoveAll(block)
	}

	return nil
}

func getStorageBlocks(mc *minio.Client, bucket string) ([]string, error) {
	doneCh := make(chan struct{})
	defer close(doneCh)

	blocks := make([]string, 0)

	blockCh := mc.ListObjectsV2(bucket, prefix, false, doneCh)
	for block := range blockCh {
		if block.Err != nil {
			return nil, errors.Wrapf(block.Err, "list objects with prefix: %q", prefix)
		}

		blocks = append(blocks, block.Key)
	}

	for i := range blocks {
		blocks[i] = blocks[i][len(prefix):]
	}

	return blocks, nil
}

func reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}
