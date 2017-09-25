package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-kit/kit/log/level"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promlog"
	promlogflag "github.com/prometheus/common/promlog/flag"
	"github.com/uber/jaeger-client-go/config"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	cfg := struct {
		configFile   string
		logLevel     promlog.AllowedLevel
		queryTimeout model.Duration

		tracer string
	}{}

	a := kingpin.New(filepath.Base(os.Args[0]), "Prometheus block based LTS.")
	a.HelpFlag.Short('h')

	a.Flag("config.file", "config file path.").
		Default("agni.yml").StringVar(&cfg.configFile)

	a.Flag("tracer.type", "tracer backend.").
		Default("noop").StringVar(&cfg.tracer)

	a.Flag("query.timeout", "Maximum time a query may take before being aborted.").
		Default("20m").SetValue(&cfg.queryTimeout)

	promlogflag.AddFlags(a, &cfg.logLevel)
	shipperCmd := a.Command("shipper", "Ship the blocks off a S3 based block store.")
	dataDir := shipperCmd.Arg("data-dir", "Path to data directory.").Required().String()
	serverCmd := a.Command("server", "Run a server that reads data off S3.")

	switch kingpin.MustParse(a.Parse(os.Args[1:])) {
	case shipperCmd.FullCommand():
		logger := promlog.New(cfg.logLevel)

		tracer, closer, err := getTracer(cfg.tracer)
		if err != nil {
			level.Error(logger).Log("msg", "init tracer", "error", err.Error())
			os.Exit(1)
		}
		opentracing.SetGlobalTracer(tracer)
		defer closer.Close()

		tempDir, err := ioutil.TempDir("", "tempData")
		if err != nil {
			level.Error(logger).Log("msg", "creation of temporary directory", "error", err.Error())
		}

		go startShipper(cfg.configFile, logger, *dataDir, tempDir)
		term := make(chan os.Signal)
		signal.Notify(term, os.Interrupt, syscall.SIGTERM)
		select {
		case <-term:
			level.Error(logger).Log("msg", "Received SIGTERM, exiting gracefully...")
			closeShipper(tempDir, logger)
			return
		}

	case serverCmd.FullCommand():
		logger := promlog.New(cfg.logLevel)
		startServer(cfg.configFile, cfg.queryTimeout, logger)
	}
}

func getTracer(typ string) (opentracing.Tracer, io.Closer, error) {
	switch typ {
	case "noop":
		return opentracing.NoopTracer{}, ioutil.NopCloser(nil), nil
	case "jaeger":
		cfg := config.Configuration{
			Sampler: &config.SamplerConfig{
				Type:  "const",
				Param: 1,
			},
			Reporter: &config.ReporterConfig{
				LogSpans:            false,
				BufferFlushInterval: time.Second,
			},
		}

		return cfg.New("agni")
	default:
		return nil, nil, fmt.Errorf("unknown tracer: %s", typ)
	}
}
