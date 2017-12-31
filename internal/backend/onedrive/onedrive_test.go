package onedrive_test

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend/onedrive"
	"github.com/restic/restic/internal/backend/test"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

const (
	OnedriveRootURL = "https://graph.microsoft.com/v1.0/me/drive/root:"
)

func newConfig() (interface{}, error) {
	onedriveCfg, err := onedrive.ParseConfig("onedrive:test")
	if err != nil {
		return nil, err
	}

	cfg := onedriveCfg.(onedrive.Config)
	cfg.Prefix = fmt.Sprintf("test-%d", time.Now().UnixNano())
	return cfg, nil
}

func createTestBackend() (restic.Backend, error) {
	cfg, err := newConfig()
	if err != nil {
		return nil, err
	}
	be, err := onedrive.Create(cfg.(onedrive.Config), nil)
	if err != nil {
		return nil, err
	}
	return be, nil
}

func createTestFile(prefix string, size int64) (*os.File, error) {
	// TODO is there an existing test helper

	tmpfile, err := ioutil.TempFile("", prefix)
	if err != nil {
		return nil, err
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := bufio.NewWriter(tmpfile)
	for i := int64(0); i < size; i++ {
		buf.WriteByte(byte(r.Int()))
	}
	buf.Flush()
	tmpfile.Seek(0, os.SEEK_SET)
	return tmpfile, nil
}

func assertUpload(t *testing.T, be restic.Backend, size int64) {
	fmt.Printf("testing file size=%d...", size) // TODO how do I flush stdout here?
	tmpfile, err := createTestFile(fmt.Sprintf("tmpfile-%d", size), size)
	if err != nil {
		t.Errorf("Failed %v", err)
	}
	defer func() { tmpfile.Close(); os.Remove(tmpfile.Name()) }()

	ctx := context.Background()

	f := restic.Handle{Type: restic.DataFile, Name: tmpfile.Name()}
	err = be.Save(ctx, f, tmpfile)
	if err != nil {
		t.Errorf("Save failed %v", err)
	}

	if fileInfo, err := be.Stat(ctx, f); err != nil || size != fileInfo.Size {
		fmt.Printf("FAILED\n")
		if err != nil {
			t.Errorf("Stat failed %v", err)
		} else {
			t.Errorf("Wrong file size, expect %d but got %d", size, fileInfo.Size)
		}
	} else {
		fmt.Printf("SUCCESS\n")
	}
}

func TestLargeFileUpload(t *testing.T) {
	ctx := context.TODO()
	be, err := createTestBackend()
	if err != nil {
		t.Errorf("Failed %v", err)
	}
	defer be.Delete(ctx)

	assertUpload(t, be, onedrive.LargeUploadFragmentSize-1)
	assertUpload(t, be, onedrive.LargeUploadFragmentSize)
	assertUpload(t, be, onedrive.LargeUploadFragmentSize+1)

	assertUpload(t, be, 3*onedrive.LargeUploadFragmentSize-1)
	assertUpload(t, be, 3*onedrive.LargeUploadFragmentSize)
	assertUpload(t, be, 3*onedrive.LargeUploadFragmentSize+1)
}

func TestLargeFileImmutableUpload(t *testing.T) {
	ctx := context.TODO()
	be, err := createTestBackend()
	if err != nil {
		t.Errorf("Failed %v", err)
	}
	defer be.Delete(ctx)

	tmpfile, err := createTestFile("10M", 10*1024*1024)
	if err != nil {
		t.Errorf("Failed %v", err)
	}
	defer func() { tmpfile.Close(); os.Remove(tmpfile.Name()) }()

	f := restic.Handle{Type: restic.DataFile, Name: "10M"}
	err = be.Save(ctx, f, tmpfile)
	if err != nil {
		t.Errorf("Failed %v", err)
	}
	tmpfile.Seek(0, os.SEEK_SET)
	err = be.Save(ctx, f, tmpfile)
	// TODO assert http status code 412 precondition failed
	if err == nil {
		t.Errorf("Upload existing file didn't fail")
	}
}

func TestListPaging(t *testing.T) {
	ctx := context.TODO()
	be, err := createTestBackend()
	if err != nil {
		t.Errorf("Failed %v", err)
	}
	defer be.Delete(ctx)

	const count = 432
	for i := 0; i < count; i++ {
		f := restic.Handle{Type: restic.DataFile, Name: fmt.Sprintf("temp-%d", i)}
		err = be.Save(ctx, f, strings.NewReader(fmt.Sprintf("temp-%d", i)))
		if err != nil {
			t.Errorf("Failed %v", err)
			return
		}
	}

	// cfg := onedrive.NewConfig()
	// cfg.Prefix = "test-1514509488748254992"
	// be, _ := onedrive.Create(cfg)

	ch := be.List(ctx, restic.DataFile)
	var actual int
	for _ = range ch {
		actual++
	}
	if count != actual {
		t.Errorf("Wrong item count, expected %d got %d", count, actual)
	}
}

func newOnedriveTestSuite(t testing.TB) *test.Suite {
	return &test.Suite{
		// do not use excessive data
		MinimalData: true,

		// NewConfig returns a config for a new temporary backend that will be used in tests.
		NewConfig: func() (interface{}, error) {
			return newConfig()
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
	defer func() {
		if t.Skipped() {
			rtest.SkipDisallowed(t, "restic/backend/onedrive.TestBackendOnedrive")
		}
	}()

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
