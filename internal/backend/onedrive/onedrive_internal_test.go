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
	"strings"
	"testing"
	"time"

	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
)

func assertNotExist(t *testing.T, client *http.Client, path string) bool {
	_, err := onedriveItemInfo(client, path)
	if !isNotExist(err) {
		t.Errorf("expected item %s to not exist, got %v", path, err)
		return false
	}
	return true
}

func assertExist(t *testing.T, client *http.Client, path string) {
	_, err := onedriveItemInfo(client, path)
	if err != nil {
		t.Errorf("expected item %s to exist, got %v", path, err)
	}
}

func TestCreateFolder(t *testing.T) {
	client, err := newClient("")
	if err != nil {
		t.Errorf("failed to create http client %v", err)
		return
	}

	// assert test preconditions
	fail := !assertNotExist(t, client, "a")
	fail = fail || !assertNotExist(t, client, "a/b")
	if fail {
		return
	}

	// cleanup after ourselves
	defer func() {
		onedriveItemDelete(client, "a")
	}()

	assertCreateFolder := func(path string) {
		err := onedriveCreateFolder(client, path)
		if err != nil {
			t.Errorf("could not create folder %s: %v", path, err)
			return
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

func createTestBackend() (*onedriveBackend, error) {
	prefix := fmt.Sprintf("test-%d", time.Now().UnixNano())

	cfg := NewConfig()
	cfg.Prefix = prefix

	be, err := open(cfg, true)
	if err != nil {
		return nil, errors.Wrap(err, "could not create backend ")
	}

	return be, nil
}

func TestCreateFolders(t *testing.T) {
	be, err := createTestBackend()
	if err != nil {
		t.Fatal(fmt.Sprintf("could not create test backend %v ", err))
		return
	}

	err = be.createFolders("data/aa")
	if err != nil {
		t.Errorf("could not create backend %v", err)
		return
	}
}

func createTestFile(prefix string, size int64) (*os.File, error) {
	// TODO is there an existing test helper?

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

	assertUpload(t, be, largeUploadFragmentSize-1)
	assertUpload(t, be, largeUploadFragmentSize)
	assertUpload(t, be, largeUploadFragmentSize+1)

	assertUpload(t, be, 3*largeUploadFragmentSize-1)
	assertUpload(t, be, 3*largeUploadFragmentSize)
	assertUpload(t, be, 3*largeUploadFragmentSize+1)
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
