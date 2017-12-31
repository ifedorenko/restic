package onedrive_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend/onedrive"
	"github.com/restic/restic/internal/backend/test"
	"github.com/restic/restic/internal/restic"
)

func newOnedriveTestSuite(t testing.TB) *test.Suite {
	return &test.Suite{
		// do not use excessive data
		MinimalData: true,

		// NewConfig returns a config for a new temporary backend that will be used in tests.
		NewConfig: func() (interface{}, error) {
			onedriveCfg, err := onedrive.ParseConfig("onedrive:test")
			if err != nil {
				return nil, err
			}

			cfg := onedriveCfg.(onedrive.Config)
			cfg.Prefix = fmt.Sprintf("test-%d", time.Now().UnixNano())
			return cfg, nil
		},

		// CreateFn is a function that creates a temporary repository for the tests.
		Create: func(config interface{}) (restic.Backend, error) {
			cfg := config.(onedrive.Config)
			return onedrive.Create(cfg, nil)
		},

		// OpenFn is a function that opens a previously created temporary repository.
		Open: func(config interface{}) (restic.Backend, error) {
			cfg := config.(onedrive.Config)
			return onedrive.Open(cfg, nil)
		},

		// CleanupFn removes data created during the tests.
		Cleanup: func(config interface{}) error {
			cfg := config.(onedrive.Config)

			be, err := onedrive.Open(cfg, nil)
			if err != nil {
				if be.IsNotExist(err) {
					return nil
				}
				return err
			}

			return be.Delete(context.TODO())
		},
	}
}

func TestBackendOnedrive(t *testing.T) {
	// defer func() {
	// 	if t.Skipped() {
	// 		rtest.SkipDisallowed(t, "restic/backend/onedrive.TestBackendOnedrive")
	// 	}
	// }()

	// vars := []string{
	// 	"RESTIC_TEST_GS_PROJECT_ID",
	// 	"RESTIC_TEST_GS_APPLICATION_CREDENTIALS",
	// 	"RESTIC_TEST_GS_REPOSITORY",
	// }

	// for _, v := range vars {
	// 	if os.Getenv(v) == "" {
	// 		t.Skipf("environment variable %v not set", v)
	// 		return
	// 	}
	// }

	t.Logf("run tests")
	newOnedriveTestSuite(t).RunTests(t)
}

func BenchmarkOnedrive(t *testing.B) {
	t.Logf("run tests")
	newOnedriveTestSuite(t).RunBenchmarks(t)
}
