package onedrive

import (
	"errors"
	"strings"
)

type Config struct {
	SecretsFilePath string

	Prefix string
}

func NewConfig() Config {
	return Config{}
}

func ParseConfig(s string) (interface{}, error) {
	if !strings.HasPrefix(s, "onedrive:") {
		return nil, errors.New("onedrive: invalid format")
	}

	cfg := NewConfig()
	cfg.Prefix = "test"
	return cfg, nil
}
