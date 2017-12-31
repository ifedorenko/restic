package onedrive

// TODO skip long-running tests by default
// TODO "proper" personal onedrive endpoint?
// TODO logging
// TODO context cancel
// TODO unexport contants
// TODO limit info returned by itemInfo and itemChildren
// TODO limit number of concurrent http requests
// TODO test-specific secrets file location

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"path"
	"strings"
	"sync"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"

	"time"

	"golang.org/x/oauth2"
)

type onedriveBackend struct {
	basedir string

	client *http.Client

	// see createParentFolders
	folders     map[string]interface{}
	foldersLock sync.Mutex

	backend.Layout
}

// Ensure that *Backend implements restic.Backend.
var _ restic.Backend = &onedriveBackend{}

//
//
//

type httpError struct {
	status     string
	statusCode int
}

func (e httpError) Error() string {
	return e.status
}

func isHTTPSuccess(statusCode int) bool {
	return statusCode >= 200 && statusCode <= 299
}

func newHTTPError(status string, statusCode int) httpError {
	return httpError{statusCode: statusCode}
}

//
//
//

const (
	onedriveBaseURL = "https://graph.microsoft.com/v1.0/me/drive/root"

	// docs says direct PUT can upload "up to 4MB in size"
	// https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_put_content
	SmallUploadLength = 4 * 1000 * 1000

	// From https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_createuploadsession#best-practices
	// Use a byte range size that is a multiple of 320 KiB (327,680 bytes)
	// The recommended fragment size is between 5-10 MiB.
	LargeUploadFragmentSize = 327680 * 30 // little over 9 MiB
)

type driveItem struct {
	CTag string `json:"cTag"`
	ETag string `json:"eTag"`
	ID   string `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
	// File struct {
	// 	MimeType string `json:"mimeType"`
	// } `json:"file"`
	// Folder struct {
	// 	ChildCount int `json:"childCount"`
	// } `json:"folder"`
}

type driveItemChildren struct {
	NextLink string      `json:"@odata.nextLink"`
	Children []driveItem `json:"value"`
}

func onedriveItemInfo(client *http.Client, path string) (driveItem, error) {
	var item driveItem

	req, err := http.NewRequest("GET", onedriveBaseURL+":/"+path, nil)
	if err != nil {
		return item, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return item, err
	}
	defer resp.Body.Close()
	if !isHTTPSuccess(resp.StatusCode) {
		return item, newHTTPError(resp.Status, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return item, err
	}

	return item, nil
}

func onedriveItemChildren(client *http.Client, path string, consumer func(driveItem) bool) error {
	url := onedriveBaseURL + ":/" + path + ":/children"
OUTER:
	for url != "" {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if !isHTTPSuccess(resp.StatusCode) {
			return newHTTPError(resp.Status, resp.StatusCode)
		}

		var item driveItemChildren
		if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
			return err
		}
		for _, child := range item.Children {
			if !consumer(child) {
				break OUTER
			}
		}
		url = item.NextLink
	}
	return nil
}

func onedriveItemDelete(client *http.Client, path string) error {
	req, err := http.NewRequest("DELETE", onedriveBaseURL+":/"+path, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// technicaly, only 204 is valid response here according to the docs
	// https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_delete
	if !isHTTPSuccess(resp.StatusCode) {
		return newHTTPError(resp.Status, resp.StatusCode)
	}

	return nil
}

// creates folder if it does not already exist
func onedriveCreateFolder(client *http.Client, parentPath string, name string) error {
	body := fmt.Sprintf(`{"name":"%s", "folder": {}}`, name)
	// TODO is there a better way to do string manipulations in golang?
	var url string
	if parentPath == "" {
		url = onedriveBaseURL + "/children"
	} else {
		// OneDrive seems to tolerate folder names with trailing '/'
		url = onedriveBaseURL + ":/" + parentPath + ":/children"
	}
	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-None-Match", "*")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// buf := new(bytes.Buffer)
	// buf.ReadFrom(resp.Body)
	// respBody := buf.String()
	// fmt.Println(respBody)

	if !isHTTPSuccess(resp.StatusCode) && resp.StatusCode != 412 {
		return newHTTPError(resp.Status, resp.StatusCode)
	}

	return nil
}

// borrowed from s3.go
func readerSize(rd io.Reader) (int64, error) {
	var size int64 = -1
	type lenner interface {
		Len() int
	}

	// find size for reader
	if f, ok := rd.(*os.File); ok {
		fi, err := f.Stat()
		if err != nil {
			return size, errors.Wrap(err, "Stat")
		}

		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return size, errors.Wrap(err, "Seek")
		}

		size = fi.Size() - pos
	} else if l, ok := rd.(lenner); ok {
		size = int64(l.Len())
	}

	return size, nil
}

func onedriveItemUpload(client *http.Client, path string, rd io.Reader, immutable bool) error {
	length, err := readerSize(rd)
	if err != nil {
		return err
	}
	if length < 0 {
		return errors.Errorf("could not determine reader size")
	}

	// make sure that client.Post() cannot close the reader by wrapping it
	rd = ioutil.NopCloser(rd)

	if length < SmallUploadLength {
		// use single-request PUT for small uploads

		req, err := http.NewRequest("PUT", onedriveBaseURL+":/"+path+":/content", rd)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "binary/octet-stream")
		if immutable {
			req.Header.Set("If-None-Match", "*")
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if !isHTTPSuccess(resp.StatusCode) {
			return newHTTPError(resp.Status, resp.StatusCode)
		}
		return nil
	}

	// for larger uploads use multi-request upload session
	// https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_createuploadsession

	// Create an upload session
	req, err := http.NewRequest("POST", onedriveBaseURL+":/"+path+":/createUploadSession", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "binary/octet-stream")
	if immutable {
		req.Header.Set("If-None-Match", "*")
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if !isHTTPSuccess(resp.StatusCode) {
		return newHTTPError(resp.Status, resp.StatusCode)
	}
	var uploadSession struct {
		UploadURL          string    `json:"uploadUrl"`
		ExpirationDateTime time.Time `json:"expirationDateTime"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&uploadSession); err != nil {
		return err
	}

	for pos := int64(0); pos < length; pos += LargeUploadFragmentSize {
		fragmentSize := length - pos
		if fragmentSize > LargeUploadFragmentSize {
			fragmentSize = LargeUploadFragmentSize
		}
		req, err = http.NewRequest("PUT", uploadSession.UploadURL, io.LimitReader(rd, fragmentSize))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "binary/octet-stream")
		req.Header.Add("Content-Length", fmt.Sprintf("%d", fragmentSize))
		req.Header.Add("Content-Range", fmt.Sprintf("bytes %d-%d/%d", pos, pos+fragmentSize-1, length))
		resp, err = client.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if !isHTTPSuccess(resp.StatusCode) {
			return newHTTPError(resp.Status, resp.StatusCode)
		}
	}

	return nil
}

func onedriveItemContent(client *http.Client, path string, length int, offset int64) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", onedriveBaseURL+":/"+path+":/content", nil)
	if err != nil {
		return nil, err
	}
	if length > 0 || offset > 0 {
		byteRange := fmt.Sprintf("bytes=%d-", offset)
		if length > 0 {
			byteRange = fmt.Sprintf("bytes=%d-%d", offset, offset+int64(length)-1)
		}
		req.Header.Add("Range", byteRange)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if !isHTTPSuccess(resp.StatusCode) {
		resp.Body.Close()
		return nil, newHTTPError(resp.Status, resp.StatusCode)
	}
	return resp.Body, nil
}

//
//
//

type secretsFile struct {
	ClientID     string `json:"ClientID"`
	ClientSecret string `json:"ClientSecret"`
	Token        struct {
		AccessToken  string    `json:"AccessToken"`
		RefreshToken string    `json:"RefreshToken"`
		Expiry       time.Time `json:"Expiry"`
	} `json:"Token"`
}

func newClient(secretsFilePath string) (*http.Client, error) {
	ctx := context.TODO()

	if secretsFilePath == "" {
		me, err := user.Current()
		if err != nil {
			return nil, err
		}
		secretsFilePath = me.HomeDir + "/.config/restic/onedrive-secrets.json"
	}

	var secrets secretsFile
	raw, err := ioutil.ReadFile(secretsFilePath)
	if err != nil {
		return nil, errors.Errorf("Could not read onedrive secrets file %v", err)
	}
	if err := json.Unmarshal(raw, &secrets); err != nil {
		return nil, err
	}

	conf := &oauth2.Config{
		ClientID:     secrets.ClientID,
		ClientSecret: secrets.ClientSecret,
		RedirectURL:  "http://localhost",
		Scopes:       []string{"files.readwrite", "offline_access"},
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
			TokenURL: "https://login.microsoftonline.com/common/oauth2/v2.0/token",
		},
	}

	token := &oauth2.Token{
		TokenType:    "Bearer",
		AccessToken:  secrets.Token.AccessToken,
		RefreshToken: secrets.Token.RefreshToken,
		Expiry:       secrets.Token.Expiry,
	}

	return conf.Client(ctx, token), nil
}

func open(cfg Config) (*onedriveBackend, error) {
	client, err := newClient(cfg.SecretsFilePath)
	if err != nil {
		return nil, err
	}

	return &onedriveBackend{
		Layout:  &backend.DefaultLayout{Path: cfg.Prefix, Join: path.Join},
		basedir: cfg.Prefix,
		client:  client,
		folders: make(map[string]interface{}),
	}, nil
}

//
//
//

// Open opens the onedrive backend.
func Open(cfg Config, rt http.RoundTripper) (restic.Backend, error) {
	return open(cfg)
}

// Create creates and opens the onedrive backend.
func Create(cfg Config, rt http.RoundTripper) (restic.Backend, error) {
	be, err := open(cfg)
	if err != nil {
		return nil, err
	}

	_, err = be.Stat(context.TODO(), restic.Handle{Type: restic.ConfigFile})
	if err == nil {
		return nil, errors.Fatal("config file already exists")
	}

	return be, nil
}

// Location returns a string that describes the type and location of the
// repository.
func (be *onedriveBackend) Location() string {
	return be.basedir
}

// Test a boolean value whether a File with the name and type exists.
func (be *onedriveBackend) Test(ctx context.Context, f restic.Handle) (bool, error) {
	_, err := onedriveItemInfo(be.client, be.Filename(f))
	if err != nil {
		if isNotExist(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// Remove removes a File described  by h.
func (be *onedriveBackend) Remove(ctx context.Context, f restic.Handle) error {
	return onedriveItemDelete(be.client, be.Filename(f))
}

// Close the backend
func (be *onedriveBackend) Close() error {
	return nil
}

func (be *onedriveBackend) createParentFolders(f restic.Handle) error {
	// this is likely overkill, but I wanted to implement the following behaviour
	// * folders known to exist in are skipped without remote request
	//   (the assumption being, local sync is "free" in comparison to remote requests)
	// * folders that are not known to exist are guaranteed to be created only once
	//   (including the case when multiple threads attempt to create the same folder)
	// * different threads can concurrently create different folders
	//
	// onedriveBackend.folders map keeps track of all folders knowns to exist.
	// access to the map is guarded by onedriveBackend.foldersLock mutex.
	// the map key is the folder path string, the map value is one of the following
	// * nil means the folder is not known to exist. first thread to create the folder
	//   will assign the value to sync.Mutex
	// * sync.Mutex means the folder needs to be created or is being created on another thread
	//   a thread that got (or created) folder mutex does the following
	//   - lock the folder mutex
	//   - double-check in #foldersLock that the folder has not been created by another thread
	//   - craete the folder
	//   - set folder's map value to true
	// * true (or any other value) means the folder is known to exist
	//
	// (really not comfortable with this. somebody please review or, better yet, tell me
	// there is much easier solution and/or existing golang library I can use)

	folderLock := func(path string) interface{} {
		be.foldersLock.Lock()
		defer be.foldersLock.Unlock()

		lock := be.folders[path]
		if lock == nil {
			lock = sync.Mutex{}
			be.folders[path] = lock
		}

		return lock
	}

	disableFolderLock := func(path string) {
		be.foldersLock.Lock()
		defer be.foldersLock.Unlock()
		be.folders[path] = true
	}

	createMissingFolder := func(parentPath string, name string) error {
		path := parentPath + "/" + name
		lock := folderLock(path)
		if mutex, ok := lock.(sync.Mutex); ok {
			mutex.Lock()
			defer mutex.Unlock()

			// another thread could have created the folder while we waited on the mutex
			if _, ok = folderLock(path).(sync.Mutex); ok {
				err := onedriveCreateFolder(be.client, parentPath, name)
				if err != nil {
					return err
				}
				disableFolderLock(path)
			}
		}
		return nil
	}

	parentPath := ""
	for _, segment := range strings.Split(be.Dirname(f), "/") {
		if segment == "" {
			continue
		}
		err := createMissingFolder(parentPath, segment)
		if err != nil {
			return err
		}
		parentPath += segment + "/"
	}

	return nil
}

// Save stores the data in the backend under the given handle.
func (be *onedriveBackend) Save(ctx context.Context, f restic.Handle, rd io.Reader) error {
	err := be.createParentFolders(f)
	if err != nil {
		return err
	}

	return onedriveItemUpload(be.client, be.Filename(f), rd, f.Type != restic.ConfigFile)
}

// Load returns a reader that yields the contents of the file at h at the
// given offset. If length is larger than zero, only a portion of the file
// is returned. rd must be closed after use. If an error is returned, the
// ReadCloser must be nil.
func (be *onedriveBackend) Load(ctx context.Context, f restic.Handle, length int, offset int64) (io.ReadCloser, error) {
	// TODO boilerplate from rest.go, see if it's still necessary
	if err := f.Valid(); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, errors.New("offset is negative")
	}
	if length < 0 {
		return nil, errors.Errorf("invalid length %d", length)
	}

	return onedriveItemContent(be.client, be.Filename(f), length, offset)
}

// Stat returns information about the File identified by h.
func (be *onedriveBackend) Stat(ctx context.Context, f restic.Handle) (restic.FileInfo, error) {
	item, err := onedriveItemInfo(be.client, be.Filename(f))
	if err != nil {
		return restic.FileInfo{}, err
	}
	return restic.FileInfo{Size: item.Size}, nil
}

// List returns a channel that yields all names of files of type t in an
// arbitrary order. A goroutine is started for this, which is stopped when
// ctx is cancelled.
func (be *onedriveBackend) List(ctx context.Context, t restic.FileType) <-chan string {
	ch := make(chan string)

	resultForwarder := func(item driveItem) bool {
		select {
		case ch <- item.Name:
			return true
		case <-ctx.Done():
			return false
		}
	}

	go func() {
		defer close(ch)

		prefix, hasSubdirs := be.Basedir(t)

		var err error
		if !hasSubdirs {
			err = onedriveItemChildren(be.client, prefix, resultForwarder)
		} else {
			subdirs := map[string]bool{}
			err = onedriveItemChildren(be.client, prefix, func(item driveItem) bool { subdirs[item.Name] = true; return true })
			if err == nil {
				for subdir := range subdirs {
					err = onedriveItemChildren(be.client, prefix+"/"+subdir, resultForwarder)
					if err != nil {
						break
					}
				}
			}
		}
		if err != nil {
			// TODO: return err to caller once err handling in List() is improved
			// debug.Log("List: %v", err)
		}
	}()

	return ch

}

func isNotExist(err error) bool {
	if herr, ok := err.(httpError); ok {
		return herr.statusCode == 404
	}

	return false
}

// IsNotExist returns true if the error was caused by a non-existing file
// in the backend.
func (be *onedriveBackend) IsNotExist(err error) bool {
	return isNotExist(err)
}

// Delete removes all data in the backend.
func (be *onedriveBackend) Delete(ctx context.Context) error {
	err := onedriveItemDelete(be.client, be.basedir)
	if err != nil && !isNotExist(err) {
		return err
	}
	return nil
}
