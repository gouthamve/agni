package main

import (
	"io/ioutil"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
)

type remoteConfig struct {
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"accesskey"`
	SecretKey string `json:"secretkey"`
	Bucket    string `json:"bucket"`
	UseSSL    bool   `json:"usessl"`
}

func loadConfig(file string) (remoteConfig, error) {
	var rcfg remoteConfig

	byt, err := ioutil.ReadFile(file)
	if err != nil {
		return rcfg, errors.Wrap(err, "read config file")
	}

	if err := yaml.Unmarshal(byt, &rcfg); err != nil {
		return rcfg, errors.Wrap(err, "yaml unmarshal")
	}

	return rcfg, nil
}
