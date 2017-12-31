package onedrive

import (
	"strings"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/options"
)

// Config contains all configuration necessary to connect to OneDrive
type Config struct {
	SecretsFilePath string

	Prefix string

	Connections uint `option:"connections" help:"set a limit for the number of concurrent connections (default: 5)"`
}

// NewConfig returns a new Config with the default values filled in.
func NewConfig() Config {
	return Config{
		Connections: 5,
	}
}

func init() {
	options.Register("onedrive", Config{})
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
