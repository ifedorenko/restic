package onedrive

// TODO logging
// TODO context cancel
// TODO test-specific secrets file location
// TODO make small/large/chunked upload threasholds configurable
// TODO skip recycle bin on delete (does not appear to be possible)

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
	"time"

	ncontext "golang.org/x/net/context"
	"golang.org/x/oauth2"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
)

//
// General helpers, likely duplicate code already available elsewhere
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

func isNotExist(err error) bool {
	if herr, ok := err.(httpError); ok {
		return herr.statusCode == http.StatusNotFound
	}

	return false
}

func drainAndCloseBody(body io.ReadCloser) {
	// https://stackoverflow.com/questions/17948827/reusing-http-connections-in-golang
	io.Copy(ioutil.Discard, body)
	body.Close()
}

// returns normalized path names up-to and including provided path
// any leading and trailing '/' are removed, so are any redundant consequent '/'
func pathNames(path string) []string {
	// TODO this seems like a lot of code for what it does

	segments := strings.Split(path, "/")
	segmentIdx := 0

	// skip leading path separatros, if any
	for ; segmentIdx < len(segments) && segments[segmentIdx] == ""; segmentIdx++ {
	}

	if segmentIdx >= len(segments) {
		return []string{} // TODO decide if there is a better empty result
	}

	parentPath := segments[segmentIdx]
	segmentIdx++

	names := make([]string, len(segments))
	names[0] = parentPath
	namesCount := 1

	for ; segmentIdx < len(segments); segmentIdx++ {
		segment := segments[segmentIdx]
		if segment == "" {
			continue
		}
		parentPath += "/" + segment
		names[namesCount] = parentPath
		namesCount++
	}
	return names[:namesCount]
}

//
// Low level OneDrive API calls
// https://docs.microsoft.com/en-us/onedrive/developer/rest-api/
//

const (
	onedriveBaseURL = "https://graph.microsoft.com/v1.0/me/drive/root"

	// docs says direct PUT can upload "up to 4MB in size"
	// https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_put_content
	smallUploadLength = 4 * 1000 * 1000

	// From https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_createuploadsession#best-practices
	// Use a byte range size that is a multiple of 320 KiB (327,680 bytes)
	// The recommended fragment size is between 5-10 MiB.
	largeUploadFragmentSize = 327680 * 30 // little over 9 MiB
)

type driveItem struct {
	// CTag string `json:"cTag"`
	// ETag string `json:"eTag"`
	// ID   string `json:"id"`
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
	url := onedriveBaseURL + ":/" + path + ":/children?select=name"
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
	drainAndCloseBody(resp.Body)

	// technicaly, only 204 is valid response here according to the docs
	// https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_delete
	if !isHTTPSuccess(resp.StatusCode) {
		return newHTTPError(resp.Status, resp.StatusCode)
	}

	return nil
}

// creates folder if it does not already exist
func onedriveCreateFolder(client *http.Client, path string) error {
	var url, name string
	nameIdx := strings.LastIndex(path, "/")
	if nameIdx < 0 {
		name = path
		url = onedriveBaseURL + "/children"
	} else {
		name = path[nameIdx+1:]
		url = onedriveBaseURL + ":/" + path[:nameIdx] + ":/children"
	}

	body := fmt.Sprintf(`{"name":"%s", "folder": {}}`, name)
	// TODO is there a better way to do string manipulations in golang?
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
	drainAndCloseBody(resp.Body)

	// buf := new(bytes.Buffer)
	// buf.ReadFrom(resp.Body)
	// respBody := buf.String()
	// fmt.Println(respBody)

	if !isHTTPSuccess(resp.StatusCode) && resp.StatusCode != http.StatusPreconditionFailed {
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

// fails if overwriteIfExists==false and the item exists
func onedriveItemUpload(client *http.Client, path string, rd io.Reader, overwriteIfExists bool) error {
	length, err := readerSize(rd)
	if err != nil {
		return err
	}
	if length < 0 {
		return errors.Errorf("could not determine reader size")
	}

	// make sure that client.Post() cannot close the reader by wrapping it
	rd = ioutil.NopCloser(rd)

	if length < smallUploadLength {
		// use single-request PUT for small uploads

		req, err := http.NewRequest("PUT", onedriveBaseURL+":/"+path+":/content", rd)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "binary/octet-stream")
		if !overwriteIfExists {
			req.Header.Set("If-None-Match", "*")
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		drainAndCloseBody(resp.Body)
		if !isHTTPSuccess(resp.StatusCode) {
			return newHTTPError(resp.Status, resp.StatusCode)
		}
		return nil
	}

	// for larger uploads use multi-request upload session
	// https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_createuploadsession

	// Create the upload session
	uploadURL, err := func() (string, error) {
		req, err := http.NewRequest("POST", onedriveBaseURL+":/"+path+":/createUploadSession", nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "binary/octet-stream")
		if !overwriteIfExists {
			req.Header.Set("If-None-Match", "*")
		}
		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer drainAndCloseBody(resp.Body)
		if !isHTTPSuccess(resp.StatusCode) {
			return "", newHTTPError(resp.Status, resp.StatusCode)
		}
		var uploadSession struct {
			UploadURL          string    `json:"uploadUrl"`
			ExpirationDateTime time.Time `json:"expirationDateTime"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&uploadSession); err != nil {
			return "", err
		}
		return uploadSession.UploadURL, nil
	}()
	if err != nil {
		return err
	}

	// Use the session to upload individual fragments
	for pos := int64(0); pos < length; pos += largeUploadFragmentSize {
		fragmentSize := length - pos
		if fragmentSize > largeUploadFragmentSize {
			fragmentSize = largeUploadFragmentSize
		}
		req, err := http.NewRequest("PUT", uploadURL, io.LimitReader(rd, fragmentSize))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "binary/octet-stream")
		req.Header.Add("Content-Length", fmt.Sprintf("%d", fragmentSize))
		req.Header.Add("Content-Range", fmt.Sprintf("bytes %d-%d/%d", pos, pos+fragmentSize-1, length))
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		drainAndCloseBody(resp.Body)
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
		drainAndCloseBody(resp.Body)
		return nil, newHTTPError(resp.Status, resp.StatusCode)
	}
	return resp.Body, nil
}

//
// restic.Backend implementation
// actual OneDrive calls are done through low-level methods above
//

type onedriveBackend struct {
	basedir string

	client *http.Client

	// used to limit number of concurrent remote requests
	sem *backend.Semaphore

	// see createParentFolders
	folders     map[string]interface{}
	foldersLock sync.Mutex

	backend.Layout
}

// Ensure that *Backend implements restic.Backend.
var _ restic.Backend = &onedriveBackend{}

type secretsFile struct {
	ClientID     string `json:"ClientID"`
	ClientSecret string `json:"ClientSecret"`
	Token        struct {
		AccessToken  string    `json:"AccessToken"`
		RefreshToken string    `json:"RefreshToken"`
		Expiry       time.Time `json:"Expiry"`
	} `json:"Token"`
}

func newClient(ctx context.Context, secretsFilePath string) (*http.Client, error) {
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

func open(ctx context.Context, cfg Config, createNew bool) (*onedriveBackend, error) {
	hc := &http.Client{
		Transport: &http.Transport{
			// http connection pool size=2 by default
			MaxIdleConnsPerHost: int(cfg.Connections),
		},
	}
	ctx = ncontext.WithValue(ctx, oauth2.HTTPClient, hc)
	client, err := newClient(ctx, cfg.SecretsFilePath)
	if err != nil {
		return nil, err
	}

	layout := &backend.DefaultLayout{Path: cfg.Prefix, Join: path.Join}

	configFile := restic.Handle{Type: restic.ConfigFile}

	_, err = onedriveItemInfo(client, layout.Filename(configFile))
	if err != nil && !isNotExist(err) {
		return nil, err // could not query remote
	}

	if err == nil && createNew {
		return nil, errors.Fatal("config file already exists")
	}

	sem, err := backend.NewSemaphore(cfg.Connections)
	if err != nil {
		return nil, err
	}

	be := &onedriveBackend{
		Layout:  layout,
		basedir: cfg.Prefix,
		client:  client,
		folders: make(map[string]interface{}),
		sem:     sem,
	}

	if createNew {
		err = be.createFolders(cfg.Prefix)
		if err != nil {
			return nil, err
		}
	}

	return be, nil
}

//
//
//

// Open opens the onedrive backend.
func Open(ctx context.Context, cfg Config, rt http.RoundTripper) (restic.Backend, error) {
	return open(ctx, cfg, false)
}

// Create creates and opens the onedrive backend.
func Create(ctx context.Context, cfg Config, rt http.RoundTripper) (restic.Backend, error) {
	return open(ctx, cfg, true)
}

// Location returns a string that describes the type and location of the
// repository.
func (be *onedriveBackend) Location() string {
	return be.basedir
}

// Test a boolean value whether a File with the name and type exists.
func (be *onedriveBackend) Test(ctx context.Context, f restic.Handle) (bool, error) {
	be.sem.GetToken()
	defer be.sem.ReleaseToken()

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
	be.sem.GetToken()
	defer be.sem.ReleaseToken()

	return onedriveItemDelete(be.client, be.Filename(f))
}

// Close the backend
func (be *onedriveBackend) Close() error {
	return nil
}

// creates specified folder and any missing parent folders
func (be *onedriveBackend) createFolders(folderPath string) error {
	// this is likely overkill, but I wanted to implement the following behaviour:
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

	ifCreateFolder := func(path string) error {
		lock := folderLock(path)
		if mutex, ok := lock.(sync.Mutex); ok {
			mutex.Lock()
			defer mutex.Unlock()

			// another thread could have created the folder while we waited on the mutex
			if _, ok = folderLock(path).(sync.Mutex); ok {
				err := onedriveCreateFolder(be.client, path)
				if err != nil {
					return err
				}
				disableFolderLock(path)
			}
		}
		return nil
	}

	for _, path := range pathNames(folderPath) {
		err := ifCreateFolder(path)
		if err != nil {
			return err
		}
	}

	return nil
}

// Save stores the data in the backend under the given handle.
func (be *onedriveBackend) Save(ctx context.Context, f restic.Handle, rd io.Reader) error {
	be.sem.GetToken()
	defer be.sem.ReleaseToken()

	// precreate parent directories to avoid intermittent "412/Precondition failed" errors
	err := be.createFolders(be.Dirname(f))
	if err != nil {
		return err
	}

	return onedriveItemUpload(be.client, be.Filename(f), rd, f.Type == restic.ConfigFile)
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

	be.sem.GetToken()

	rd, err := onedriveItemContent(be.client, be.Filename(f), length, offset)
	if err != nil {
		be.sem.ReleaseToken()
		return nil, err
	}

	return be.sem.ReleaseTokenOnClose(rd, nil), nil
}

// Stat returns information about the File identified by h.
func (be *onedriveBackend) Stat(ctx context.Context, f restic.Handle) (restic.FileInfo, error) {
	be.sem.GetToken()
	defer be.sem.ReleaseToken()

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

	listChildren := func(path string, consumer func(driveItem) bool) error {
		be.sem.GetToken()
		defer be.sem.ReleaseToken()

		return onedriveItemChildren(be.client, path, consumer)
	}

	go func() {
		defer close(ch)

		prefix, hasSubdirs := be.Basedir(t)

		var err error
		if !hasSubdirs {
			err = listChildren(prefix, resultForwarder)
		} else {
			subdirs := map[string]bool{}
			err = listChildren(prefix, func(item driveItem) bool { subdirs[item.Name] = true; return true })
			if err == nil {
				for subdir := range subdirs {
					err = listChildren(prefix+"/"+subdir, resultForwarder)
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

// IsNotExist returns true if the error was caused by a non-existing file
// in the backend.
func (be *onedriveBackend) IsNotExist(err error) bool {
	return isNotExist(err)
}

// Delete removes all data in the backend.
func (be *onedriveBackend) Delete(ctx context.Context) error {
	be.sem.GetToken()
	defer be.sem.ReleaseToken()

	err := onedriveItemDelete(be.client, be.basedir)
	if err != nil && !isNotExist(err) {
		return err
	}
	return nil
}
