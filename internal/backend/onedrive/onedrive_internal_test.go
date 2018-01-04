package onedrive

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/restic/restic/internal/restic"
)

func assertNotExist(t *testing.T, client *http.Client, path string) {
	_, err := onedriveItemInfo(client, path)
	if !isNotExist(err) {
		t.Fatalf("expected item %s to not exist, got %v", path, err)
	}
}

func assertExist(t *testing.T, client *http.Client, path string) {
	_, err := onedriveItemInfo(client, path)
	if err != nil {
		t.Fatalf("expected item %s to exist, got %v", path, err)
	}
}

func newTestClient(t *testing.T) *http.Client {
	client, err := newClient(context.TODO(), "")
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
		onedriveItemDelete(client, "a")
	}()

	assertCreateFolder := func(path string) {
		err := onedriveCreateFolder(client, path)
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

	be, err := open(context.TODO(), cfg, http.DefaultTransport, true)
	if err != nil {
		t.Fatalf("could not create test backend %v ", err)
	}

	return be
}

func TestCreateFolders(t *testing.T) {
	be := createTestBackend(t)
	defer be.Delete(context.TODO())

	err := be.createFolders(be.basedir + "/data/aa")
	if err != nil {
		t.Fatalf("failed to create folders: %v", err)
	}
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

	f := restic.Handle{Type: restic.DataFile, Name: tmpfile.Name()}
	err := be.Save(ctx, f, tmpfile)
	if err != nil {
		t.Fatalf("Save failed %v", err)
	}

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

func TestLargeFileImmutableUpload(t *testing.T) {
	skipSlowTest(t)

	ctx := context.TODO()
	be := createTestBackend(t)
	defer be.Delete(ctx)

	tmpfile := createTestFile(t, "10M", 10*1024*1024)
	defer func() { tmpfile.Close(); os.Remove(tmpfile.Name()) }()

	f := restic.Handle{Type: restic.DataFile, Name: "10M"}
	err := be.Save(ctx, f, tmpfile)
	if err != nil {
		t.Fatalf("Failed %v", err)
	}
	tmpfile.Seek(0, os.SEEK_SET)
	err = be.Save(ctx, f, tmpfile)
	if herr, ok := err.(httpError); !ok || herr.statusCode != http.StatusPreconditionFailed {
		t.Fatalf("expected upload to failed with 412/StatusPreconditionFailed, got %v", err)
	}
}

func TestListPaging(t *testing.T) {
	skipSlowTest(t)

	ctx := context.TODO()
	be := createTestBackend(t)
	defer be.Delete(ctx)

	const count = 432
	for i := 0; i < count; i++ {
		f := restic.Handle{Type: restic.DataFile, Name: fmt.Sprintf("temp-%d", i)}
		err := be.Save(ctx, f, strings.NewReader(fmt.Sprintf("temp-%d", i)))
		if err != nil {
			t.Fatalf("Failed %v", err)
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
			h := restic.Handle{Type: restic.DataFile, Name: id.String()}
			err := be.Save(ctx, h, bytes.NewReader(data))
			if err != nil {
				t.Error(err)
			}
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
