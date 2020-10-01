package onedrive

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

func assertNotExist(t *testing.T, client *http.Client, path string) {
	_, err := onedriveItemInfo(context.TODO(), client, path)
	if !isNotExist(err) {
		t.Fatalf("expected item %s to not exist, got %v", path, err)
	}
}

func assertExist(t *testing.T, client *http.Client, path string) {
	_, err := onedriveItemInfo(context.TODO(), client, path)
	if err != nil {
		t.Fatalf("expected item %s to exist, got %v", path, err)
	}
}

func newTestClient(t *testing.T) *http.Client {
	secretsFilePath := os.Getenv("RESTIC_TEST_ONEDRIVE_SECRETS_FILE")
	if secretsFilePath == "" {
		t.Fatalf("Environment variable $RESTIC_TEST_ONEDRIVE_SECRETS_FILE is not set")
	}
	client, err := newClient(http.DefaultClient, secretsFilePath)
	if err != nil {
		t.Fatalf("Could not create http client %v", err)
	}
	return client
}

func TestCreateFolder(t *testing.T) {
	client := newTestClient(t)

	// assert test preconditions
	assertNotExist(t, client, "a")
	assertNotExist(t, client, "a/b")

	// cleanup after ourselves
	defer func() {
		onedriveItemDelete(context.TODO(), client, "a")
	}()

	assertCreateFolder := func(path string) {
		err := onedriveCreateFolder(context.TODO(), client, path)
		if err != nil {
			t.Fatalf("could not create folder %s: %v", path, err)
		}
		assertExist(t, client, path)
	}

	// test create new folder and subfolder
	assertCreateFolder("a")
	assertCreateFolder("a/b")

	// test create existing folders
	assertCreateFolder("a")
	assertCreateFolder("a/b")
}

func assertArrayEquals(t *testing.T, expected []string, actual []string) {
	if reflect.DeepEqual(expected, actual) {
		return
	}
	t.Fatal(fmt.Sprintf("expected %v but got %v", expected, actual))
}

func TestDirectoryNames(t *testing.T) {
	assertArrayEquals(t, []string{}, pathNames(""))
	assertArrayEquals(t, []string{}, pathNames("/"))

	assertArrayEquals(t, []string{"a"}, pathNames("a"))
	assertArrayEquals(t, []string{"a"}, pathNames("a/"))
	assertArrayEquals(t, []string{"a"}, pathNames("/a/"))
	assertArrayEquals(t, []string{"a"}, pathNames("a//"))

	assertArrayEquals(t, []string{"a", "a/b"}, pathNames("a/b"))
	assertArrayEquals(t, []string{"a", "a/b"}, pathNames("a//b"))
}

func createTestBackend(t *testing.T) *onedriveBackend {
	prefix := fmt.Sprintf("test-%d", time.Now().UnixNano())

	cfg := NewConfig()
	cfg.Prefix = prefix
	cfg.SecretsFilePath = os.Getenv("RESTIC_TEST_ONEDRIVE_SECRETS_FILE")

	be, err := open(context.TODO(), cfg, http.DefaultTransport, true)
	if err != nil {
		t.Fatalf("could not create test backend %v ", err)
	}

	return be
}

func createTestFile(t *testing.T, prefix string, size int64) *os.File {
	// TODO is there an existing test helper?

	tmpfile, err := ioutil.TempFile("", prefix)
	if err != nil {
		t.Fatalf("could not create temp file %v", err)
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := bufio.NewWriter(tmpfile)
	for i := int64(0); i < size; i++ {
		buf.WriteByte(byte(r.Int()))
	}
	buf.Flush()
	tmpfile.Seek(0, os.SEEK_SET)
	return tmpfile
}

func skipSlowTest(t *testing.T) {
	if os.Getenv("ONEDRIVE_SLOW_TESTS") == "" {
		t.Skip("skipping test; $ONEDRIVE_SLOW_TESTS is not set")
	}
}

func assertUpload(t *testing.T, be restic.Backend, size int64) {
	fmt.Printf("testing file size=%d...", size) // TODO how do I flush stdout here?
	tmpfile := createTestFile(t, fmt.Sprintf("tmpfile-%d", size), size)
	defer func() { tmpfile.Close(); os.Remove(tmpfile.Name()) }()

	ctx := context.Background()

	f := restic.Handle{Type: restic.PackFile, Name: tmpfile.Name()}
	rd, err := restic.NewFileReader(tmpfile)
	rtest.OK(t, err)
	err = be.Save(ctx, f, rd)
	rtest.OK(t, err)

	if fileInfo, err := be.Stat(ctx, f); err != nil || size != fileInfo.Size {
		fmt.Printf("FAILED\n")
		if err != nil {
			t.Fatalf("Stat failed %v", err)
		} else {
			t.Fatalf("Wrong file size, expect %d but got %d", size, fileInfo.Size)
		}
	} else {
		fmt.Printf("SUCCESS\n")
	}
}

func TestLargeFileUpload(t *testing.T) {
	skipSlowTest(t)

	ctx := context.TODO()
	be := createTestBackend(t)
	defer be.Delete(ctx)

	assertUpload(t, be, uploadFragmentSize-1)
	assertUpload(t, be, uploadFragmentSize)
	assertUpload(t, be, uploadFragmentSize+1)

	assertUpload(t, be, 3*uploadFragmentSize-1)
	assertUpload(t, be, 3*uploadFragmentSize)
	assertUpload(t, be, 3*uploadFragmentSize+1)
}

func TestListPaging(t *testing.T) {
	skipSlowTest(t)

	ctx := context.TODO()
	be := createTestBackend(t)
	defer be.Delete(ctx)

	const count = 432
	for i := 0; i < count; i++ {
		f := restic.Handle{Type: restic.PackFile, Name: fmt.Sprintf("temp-%d", i)}
		rd := restic.NewByteReader([]byte(fmt.Sprintf("temp-%d", i)))
		err := be.Save(ctx, f, rd)
		rtest.OK(t, err)
	}

	// cfg := onedrive.NewConfig()
	// cfg.Prefix = "test-1514509488748254992"
	// be, _ := onedrive.Create(cfg)

	var actual int
	err := be.List(ctx, restic.PackFile, func(item restic.FileInfo) error {
		actual++
		return nil
	})
	rtest.OK(t, err)

	if count != actual {
		t.Fatalf("Wrong item count, expected %d got %d", count, actual)
	}
}

func disabledTestIntermitentInvalidFragmentLength(t *testing.T) {
	// 2018-01-02 observed intermitent failures during PUT
	// response status 400 Bad Request
	// response body {"error":{"code":"invalidRequest","message":"Declared fragment length does not match the provided number of bytes"}}
	// assume server-side issues as most of the requests did succeed

	ctx := context.TODO()
	be := createTestBackend(t)
	defer be.Delete(ctx)

	items := make(chan int)

	upload := func() {
		for {
			i, ok := <-items
			if !ok {
				break
			}
			data := []byte(fmt.Sprintf("random test blob %v", i))
			id := restic.Hash(data)
			h := restic.Handle{Type: restic.PackFile, Name: id.String()}
			err := be.Save(ctx, h, restic.NewByteReader(data))
			rtest.OK(t, err)
		}
	}

	for i := 0; i < 5; i++ {
		go upload()
	}

	for i := 0; i < 2000; i++ {
		items <- i
	}
	close(items)
}

func TestConcurrentDirectoryItemsCreate(t *testing.T) {
	// this test asserts intemediate directories can be created concurrently
	// background:
	// restic used to require backends to fail writing over existing items
	// onedrive implemented that requirement with http hreader If-None-Match=*
	// which resulted in infrequent "412/Precondition failed" errors creating
	// intermediate directories.
	// as a workaround, onedrive backend explicitly created directories and
	// used client-side synchronization to avoid concurrent creating of the
	// same directory.
	// all that complexity was removed after restic no loner requires backends
	// to fail writing over existing files.

	ctx := context.TODO()
	be := createTestBackend(t)
	clients := 5
	defer be.Delete(ctx)

	failures := int32(0)
	for iter := 0; iter < 10; iter++ {
		ch := make(chan bool)
		var wg sync.WaitGroup
		dir := fmt.Sprintf("%s/dir-%d/a/b/c/d", be.basedir, iter)
		for client := 0; client < clients; client++ {
			path := fmt.Sprintf("%s//%d", dir, client)
			go func() {
				<-ch // block until the channel is closed
				err := onedriveItemUpload(ctx, be.client, be.nakedClient, path, restic.NewByteReader([]byte("data")))
				if err != nil {
					atomic.AddInt32(&failures, 1)
					fmt.Printf("%s: %v\n", path, err)
				}
				wg.Done()
			}()
			wg.Add(1)
		}
		close(ch)
		wg.Wait()
		rtest.Equals(t, int32(0), failures)
		children, nextLink, err := onedriveGetChildren(ctx, be.client, onedriveGetChildrenURL(dir))
		rtest.OK(t, err)
		rtest.Equals(t, "", nextLink)
		rtest.Equals(t, clients, len(children))
		fmt.Printf("iter=%d\n", iter)
	}
}
