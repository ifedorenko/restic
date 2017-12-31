package onedrive

import (
	"fmt"
	"net/http"
	"testing"
	"time"

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

	assertCreateFolder := func(parentPath string, name string) {
		err := onedriveCreateFolder(client, parentPath, name)
		path := parentPath + "/" + name
		if err != nil {
			t.Errorf("could not create folder %s: %v", path, err)
			return
		}
		assertExist(t, client, path)
	}

	// test create new folder and subfolder
	assertCreateFolder("", "a")
	assertCreateFolder("a/", "b")

	// test create existing folders
	assertCreateFolder("", "a")
	assertCreateFolder("a/", "b")
}

func TestCreateParentFolders(t *testing.T) {
	client, err := newClient("")
	if err != nil {
		t.Errorf("failed to create http client %v", err)
	}

	prefix := fmt.Sprintf("test-%d", time.Now().UnixNano())

	if !assertNotExist(t, client, prefix) {
		return
	}

	cfg := Config{
		Prefix: prefix,
	}

	be, err := open(cfg)
	if err != nil {
		t.Errorf("could not create backend %v", err)
		return
	}

	f := restic.Handle{
		Type: restic.DataFile,
		Name: "test",
	}

	err = be.createParentFolders(f)
	if err != nil {
		t.Errorf("could not create backend %v", err)
		return
	}
}
