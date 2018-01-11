package onedrive

import (
	"strings"
	"time"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/options"
)

// Config contains all configuration necessary to connect to OneDrive
type Config struct {
	SecretsFilePath string

	Prefix string

	Connections uint          `option:"connections" help:"set a limit for the number of concurrent connections (default: 5)"`
	Timeout     time.Duration `option:"timeout" help:"set remote request timeout (default: 5 minutes)"`
}

// NewConfig returns a new Config with the default values filled in.
func NewConfig() Config {
	return Config{
		Connections: 5,

		// Back-of-the-envelope calculation
		// with 10MB data file, 1MB/s connection and 5 concurrent connections
		// individual files should take ~50 seconds to transfer
		// 5 minutes should be more than enough to finish any operation
		// note that RetryBackend ExponentialBackOff.MaxElapsedTime is 15 minutes
		Timeout: 5 * time.Minute,
	}
}

func init() {
	options.Register("onedrive", Config{})
}

func ParseConfig(s string) (interface{}, error) {
	data := strings.SplitN(s, ":", 2)
	if len(data) != 2 {
		return nil, errors.New("invalid URL, expected: onedrive:prefix")
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
