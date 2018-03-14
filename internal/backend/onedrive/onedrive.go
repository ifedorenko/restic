package onedrive

// TODO logging and error stack traces
// TODO restic allows backends overwrite existing files now
//      this removes the need for onedriveItemUpload(overwriteIfExists)
//      and, more interestingly, entire createFolders() func
// TODO use rtests in internal test
// TODO CPU utilization (~70% on Intel N3700) appears too high
//      investigate what uses the CPU so much
// TODO make upload fragment size configurable
// TODO skip recycle bin on delete (does not appear to be possible)
//      or empty recycle bin as part of delete (does not appear to be possible either)
// TODO consider adding HTTP METHOD/PATH to httpError

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
)

//
// General helpers, likely duplicate code already available elsewhere
//

type httpError struct {
	statusText string
	statusCode int
}

func (e httpError) Error() string {
	statusText := e.statusText
	if statusText == "" {
		statusText = http.StatusText(e.statusCode)
	}
	return fmt.Sprintf("%d/%s", e.statusCode, statusText)
}

func isHTTPSuccess(statusCode int) bool {
	return statusCode >= 200 && statusCode <= 299
}

func newHTTPError(statusText string, statusCode int) httpError {
	return httpError{statusText: statusText, statusCode: statusCode}
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

	// From https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_createuploadsession#best-practices
	// Use a byte range size that is a multiple of 320 KiB (327,680 bytes)
	// The recommended fragment size is between 5-10 MiB.
	uploadFragmentSize = 327680 * 30 // little over 9 MiB
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

func onedriveItemInfo(ctx context.Context, client *http.Client, path string) (driveItem, error) {
	var item driveItem

	req, err := http.NewRequest("GET", onedriveBaseURL+":/"+path, nil)
	if err != nil {
		return item, err
	}
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return item, err
	}
	defer drainAndCloseBody(resp.Body)
	if !isHTTPSuccess(resp.StatusCode) {
		return item, newHTTPError(resp.Status, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return item, err
	}

	return item, nil
}

func onedriveGetChildren(ctx context.Context, client *http.Client, url string) (children []driveItem, nextLink string, err error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, "", err
	}
	defer drainAndCloseBody(resp.Body)
	if !isHTTPSuccess(resp.StatusCode) {
		return nil, "", newHTTPError(resp.Status, resp.StatusCode)
	}

	var item driveItemChildren
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return nil, "", err
	}
	return item.Children, item.NextLink, nil
}

func onedriveGetChildrenURL(path string) string {
	return onedriveBaseURL + ":/" + path + ":/children?select=name,size"
}

func onedriveItemDelete(ctx context.Context, client *http.Client, path string) error {
	req, err := http.NewRequest("DELETE", onedriveBaseURL+":/"+path, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req.WithContext(ctx))
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
func onedriveCreateFolder(ctx context.Context, client *http.Client, path string) error {
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
	resp, err := client.Do(req.WithContext(ctx))
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

// fails if overwriteIfExists==false and the item exists
func onedriveItemUpload(ctx context.Context, client *http.Client, nakedClient *http.Client, path string, rd restic.RewindReader, overwriteIfExists bool) error {
	length := rd.Length()

	// will always use POST+PUT sequence to upload items
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
		resp, err := client.Do(req.WithContext(ctx))
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
	for pos := int64(0); pos < length; pos += uploadFragmentSize {
		contentLength := length - pos
		if contentLength > uploadFragmentSize {
			contentLength = uploadFragmentSize
		}
		// wrap the reader so that net/http client cannot close the reader
		req, err := http.NewRequest("PUT", uploadURL, io.LimitReader(ioutil.NopCloser(rd), contentLength))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "binary/octet-stream")
		// req.Header.Add("Content-Length", fmt.Sprintf("%d", contentLength))
		req.Header.Add("Content-Range", fmt.Sprintf("bytes %d-%d/%d", pos, pos+contentLength-1, length))

		// https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_createuploadsession#remarks
		// Including the Authorization header when issuing the PUT call may result in a HTTP 401 Unauthorized response.
		// The Authorization header and bearer token should only be sent when issuing the POST during the first step.
		// It should be not be included when issueing the PUT.
		resp, err := nakedClient.Do(req.WithContext(ctx))
		if err != nil {
			return err
		}
		if resp.StatusCode == 400 {
			// this occasionally happens when running tests for no reason I can tell
			// message is "Declared fragment length does not match the provided number of bytes"
			// the debug output is meant to help understand the pattern (if there is one)
			buf, err := ioutil.ReadAll(resp.Body)
			body := ""
			if buf != nil {
				body = string(buf)
			}
			fmt.Printf("onedrive item PUT %s (size=%d offset=%d len=%d): err=%v body=%s\n", path, length, pos, contentLength, err, string(body))
		}
		drainAndCloseBody(resp.Body)
		if !isHTTPSuccess(resp.StatusCode) {
			return newHTTPError(resp.Status, resp.StatusCode)
		}
	}

	// never use single-PUT
	// https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_put_content
	// - creates incomplete files if interrupted
	// - most upload items are over "up to 4MB in size" limit

	return nil
}

func onedriveItemContent(ctx context.Context, client *http.Client, path string, length int, offset int64) (io.ReadCloser, error) {
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
	// disable http compression, it only adds unnecessary CPU overhead for encrypted contents
	req.Header.Add("Accept-Encoding", "identity")
	// note that observed behaviour does not match documentation
	// the docs claim GET item content always return 302/Found redirect response
	// observed (both in golang and postman), 200 or 206 responses
	// https://docs.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_get_content
	resp, err := client.Do(req.WithContext(ctx))
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

	// underlying http.Client
	nakedClient *http.Client

	// oauth2-enabled http.Client wrapper
	client *http.Client

	// used to limit number of concurrent remote requests
	sem         *backend.Semaphore
	connections uint

	// see createFolders
	folders     map[string]*sync.Once
	foldersLock sync.Mutex

	// request timeout
	timeout time.Duration

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

func newClient(client *http.Client, secretsFilePath string) (*http.Client, error) {
	if secretsFilePath == "" {
		return nil, errors.Fatal("OneDrive secrets file path is not specified")
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

	return conf.Client(context.WithValue(context.Background(), oauth2.HTTPClient, client), token), nil
}

func open(ctx context.Context, cfg Config, rt http.RoundTripper, createNew bool) (*onedriveBackend, error) {
	ctx, cancel := timeoutContext(ctx, cfg.Timeout)
	defer cancel()

	nakedClient := &http.Client{Transport: rt}
	client, err := newClient(nakedClient, cfg.SecretsFilePath)
	if err != nil {
		return nil, err
	}

	layout := &backend.DefaultLayout{Path: cfg.Prefix, Join: path.Join}

	configFile := restic.Handle{Type: restic.ConfigFile}

	_, err = onedriveItemInfo(ctx, client, layout.Filename(configFile))
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
		Layout:      layout,
		basedir:     cfg.Prefix,
		nakedClient: nakedClient,
		client:      client,
		folders:     make(map[string]*sync.Once),
		sem:         sem,
		connections: cfg.Connections,
		timeout:     cfg.Timeout,
	}

	if createNew {
		err = be.createFolders(ctx, cfg.Prefix)
		if err != nil {
			return nil, err
		}
	}

	return be, nil
}

//
//
//

func timeoutContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	// TODO this really belongs to RetryBackend
	return context.WithTimeout(ctx, timeout)
}

// Open opens the onedrive backend.
func Open(ctx context.Context, cfg Config, rt http.RoundTripper) (restic.Backend, error) {
	return open(ctx, cfg, rt, false)
}

// Create creates and opens the onedrive backend.
func Create(ctx context.Context, cfg Config, rt http.RoundTripper) (restic.Backend, error) {
	return open(ctx, cfg, rt, true)
}

// Location returns a string that describes the type and location of the
// repository.
func (be *onedriveBackend) Location() string {
	return be.basedir
}

// Test a boolean value whether a File with the name and type exists.
func (be *onedriveBackend) Test(ctx context.Context, f restic.Handle) (bool, error) {
	ctx, cancel := timeoutContext(ctx, be.timeout)
	defer cancel()

	be.sem.GetToken()
	defer be.sem.ReleaseToken()

	_, err := onedriveItemInfo(ctx, be.client, be.Filename(f))
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
	ctx, cancel := timeoutContext(ctx, be.timeout)
	defer cancel()

	be.sem.GetToken()
	defer be.sem.ReleaseToken()

	return onedriveItemDelete(ctx, be.client, be.Filename(f))
}

// Close the backend
func (be *onedriveBackend) Close() error {
	return nil
}

// creates specified folder and any missing parent folders
func (be *onedriveBackend) createFolders(ctx context.Context, folderPath string) error {
	// this is likely overkill, but I wanted to implement the following behaviour:
	// * folders known to exist are skipped without remote requests
	// * folders that are not known to exist are guaranteed to be created only once
	//   (even when multiple threads create the same folder concurrently)
	// * different threads can concurrently create different folders

	// returns per-folde sync.Once
	// uses sync.Mutex to serialize concurrent access
	folderOnce := func(path string) *sync.Once {
		be.foldersLock.Lock()
		defer be.foldersLock.Unlock()

		once := be.folders[path]
		if once == nil {
			once = &sync.Once{}
			be.folders[path] = once
		}

		return once
	}

	// creates the folder, if the folder is not known to exist
	// uses sync.Once to implement only-once behaviour
	ifCreateFolder := func(path string) error {
		once := folderOnce(path)
		var err error
		once.Do(func() {
			err = onedriveCreateFolder(ctx, be.client, path)
		})
		return err
	}

	for _, path := range pathNames(folderPath) {
		err := ifCreateFolder(path)
		if err != nil {
			return err
		}
	}

	return nil
}

// Save stores the data from rd under the given handle.
func (be *onedriveBackend) Save(ctx context.Context, f restic.Handle, rd restic.RewindReader) error {
	ctx, cancel := timeoutContext(ctx, be.timeout)
	defer cancel()

	be.sem.GetToken()
	defer be.sem.ReleaseToken()

	// precreate parent directories to avoid intermittent "412/Precondition failed" errors
	err := be.createFolders(ctx, be.Dirname(f))
	if err != nil {
		return err
	}

	return onedriveItemUpload(ctx, be.client, be.nakedClient, be.Filename(f), rd, f.Type == restic.ConfigFile)
}

// Load runs fn with a reader that yields the contents of the file at h at the
// given offset.
func (be *onedriveBackend) Load(ctx context.Context, h restic.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	return backend.DefaultLoad(ctx, h, length, offset, be.openReader, fn)
}

func (be *onedriveBackend) openReader(ctx context.Context, f restic.Handle, length int, offset int64) (io.ReadCloser, error) {
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

	ctx, cancel := timeoutContext(ctx, be.timeout)

	be.sem.GetToken()

	rd, err := onedriveItemContent(ctx, be.client, be.Filename(f), length, offset)
	if err != nil {
		be.sem.ReleaseToken()
		cancel()
		return nil, err
	}

	return be.sem.ReleaseTokenOnClose(rd, cancel), nil
}

// Stat returns information about the File identified by h.
func (be *onedriveBackend) Stat(ctx context.Context, f restic.Handle) (restic.FileInfo, error) {
	ctx, cancel := timeoutContext(ctx, be.timeout)
	defer cancel()

	be.sem.GetToken()
	defer be.sem.ReleaseToken()

	item, err := onedriveItemInfo(ctx, be.client, be.Filename(f))
	if err != nil {
		return restic.FileInfo{}, err
	}
	return restic.FileInfo{Name: item.Name, Size: item.Size}, nil
}

func (be *onedriveBackend) listChildren(ctx context.Context, path string, fn func(driveItem) error) error {
	// possible dead lock:
	// - (each data/ subdir contains >40 files, which is true for 500GB repo)
	// - checker starts 40 workers, initially blocked on List() result channel
	// - List() starts 5 workers, each acquires a sync token, gets >40 names
	//   from remote system, then pushes names to the result channel;
	//   (!) list workers block pushing results while holding tokens.
	// - all checker workers wake up, get names, attempt to acquire sync tokens
	// - now all list workers wait to push to the results channel and all
	//   checker workers wait to acquire sync tokens.
	// solution: list workers release sync token before pushing results

	url := onedriveGetChildrenURL(path)
	for url != "" {
		var children []driveItem
		var err error
		be.sem.GetToken()
		children, url, err = onedriveGetChildren(ctx, be.client, url)
		be.sem.ReleaseToken()
		if err != nil {
			return err
		}
		for _, child := range children {
			err = fn(child)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// List returns a channel that yields all names of files of type t in an
// arbitrary order. A goroutine is started for this, which is stopped when
// ctx is cancelled.
func (be *onedriveBackend) List(ctx context.Context, t restic.FileType, fn func(restic.FileInfo) error) error {
	ctx, cancel := timeoutContext(ctx, be.timeout)
	defer cancel()

	prefix, hasSubdirs := be.Basedir(t)

	if !hasSubdirs {
		return be.listChildren(ctx, prefix, func(item driveItem) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fn(restic.FileInfo{Name: item.Name, Size: item.Size})
		})
	}

	// list subdirectories concurrently, improves restic-check by ~2 minutes for 500GB repo

	// collect all subdirs first
	subdirs := map[string]bool{}
	err := be.listChildren(ctx, prefix, func(item driveItem) error { subdirs[item.Name] = true; return nil })
	if err != nil {
		return err
	}

	// result channel
	resultChannel := make(chan driveItem)

	// error channel, first error breaks result pump loop and is returned to the caller
	// the channel is guaranteed to accept one error without blocking
	errorChannel := make(chan error, 1)
	reportError := func(err error) {
		select {
		case errorChannel <- err:
			// reported error successfully
		default:
			// error channel is full, drop any secondary errors
		}
	}

	// workers to list individual subdirs
	subdirChannel := make(chan string)
	wg := sync.WaitGroup{}
	for i := uint(0); i < be.connections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for subdir := range subdirChannel {
				err := be.listChildren(ctx, prefix+"/"+subdir, func(item driveItem) error {
					select {
					case resultChannel <- item:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
				if err != nil {
					reportError(err)
					return
				}
			}
		}()
	}

	// workers coordinator
	go func() {
		defer close(resultChannel) // always terminate result pump loop

		// push subdirs to subdirs channel
		for subdir := range subdirs {
			select {
			case subdirChannel <- subdir:
			case <-ctx.Done():
				reportError(ctx.Err())
				break
			}
		}
		close(subdirChannel)

		// wait for workers to finish
		wg.Wait()
	}()

	// result pump loop
	for item := range resultChannel {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errorChannel:
			return err
		default:
			err := fn(restic.FileInfo{Name: item.Name, Size: item.Size})
			if err != nil {
				return err
			}
		}
	}

	select {
	case err := <-errorChannel:
		return err
	default:
	}

	return nil
}

// IsNotExist returns true if the error was caused by a non-existing file
// in the backend.
func (be *onedriveBackend) IsNotExist(err error) bool {
	return isNotExist(err)
}

// Delete removes all data in the backend.
func (be *onedriveBackend) Delete(ctx context.Context) error {
	ctx, cancel := timeoutContext(ctx, be.timeout)
	defer cancel()

	be.sem.GetToken()
	defer be.sem.ReleaseToken()

	err := onedriveItemDelete(ctx, be.client, be.basedir)
	if err != nil && !isNotExist(err) {
		return err
	}
	return nil
}
