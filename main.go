package main

import (
	"os"
	"path/filepath"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	cfg := struct {
		configFile string
	}{}

	a := kingpin.New(filepath.Base(os.Args[0]), "Prometheus block based LTS.")
	a.HelpFlag.Short('h')

	a.Flag("config.file", "config file path.").
		Default("agni.yml").StringVar(&cfg.configFile)

	shipperCmd := a.Command("shipper", "Ship the blocks off a S3 based block store.")
	serverCmd := a.Command("server", "Run a server that reads data off S3.")

	switch kingpin.MustParse(a.Parse(os.Args[1:])) {
	case shipperCmd.FullCommand():
		startShipper(cfg.configFile)
	case serverCmd.FullCommand():
		startServer(cfg.configFile)
	}
}
