package onedrive

import (
	"strings"

	"github.com/restic/restic/internal/errors"
)

type Config struct {
	SecretsFilePath string

	Prefix string
}

func NewConfig() Config {
	return Config{}
}

func ParseConfig(s string) (interface{}, error) {
	data := strings.SplitN(s, ":", 2)
	if len(data) != 2 {
		return nil, errors.New("invalid URL, expected: onedrive:/prefix")
	}

	scheme, prefix := data[0], data[1]
	if scheme != "onedrive" {
		return nil, errors.Errorf("unexpected schema: %s", data[0])
	}

	if len(prefix) == 0 {
		return nil, errors.Errorf("prefix is empty")
	}

	cfg := NewConfig()
	cfg.Prefix = prefix
	return cfg, nil
}
