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
)

var prefix = "promblock-"

func startShipper(configFile string, logger log.Logger) {
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

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		blockDirs, err := ioutil.ReadDir(".")
		if err != nil {
			level.Error(logger).Log("error", err.Error())
			os.Exit(1)
		}
		blocks := make([]string, 0, len(blockDirs))
		for _, bd := range blockDirs {
			if bd.Name() == "wal" {
				continue
			}

			if strings.HasSuffix(bd.Name(), ".tmp") {
				continue
			}
			if !bd.IsDir() {
				continue
			}

			byt, err := ioutil.ReadFile(filepath.Join(bd.Name(), "meta.json"))
			if err != nil {
				level.Error(logger).Log("msg", "load config", "error", err.Error())
				os.Exit(1)
			}

			var bm tsdb.BlockMeta
			if err := json.Unmarshal(byt, &bm); err != nil {
				level.Error(logger).Log("msg", "load config", "error", err.Error())
				os.Exit(1)
			}

			if bm.Compaction.Level == 1 {
				blocks = append(blocks, bd.Name())
			}
		}

		if err := s.shipBlocks(blocks); err != nil {
			level.Error(logger).Log("msg", "load config", "error", err.Error())
			os.Exit(1)
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

func (s *shipper) shipBlocks(blocks []string) error {
	for _, block := range blocks {
		if _, ok := s.blocks[block]; ok {
			continue
		}

		blockKey := reverse(block)

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
		s.blocks[block] = struct{}{}
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
