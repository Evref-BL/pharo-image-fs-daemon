package mount

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
	nfs "github.com/willscott/go-nfs"
)

const objectPath = "/tonel/Kernel/Object.class.st"

// writeAsNFS writes data at offset to name as go-nfs does for one WRITE
// call, and answers what Written answered.
func writeAsNFS(t *testing.T, fsys *nfsFileSystem, name string, data string, offset int64) (bool, error) {
	t.Helper()
	file, err := fsys.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s for writing: %v", name, err)
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}
	if _, err := file.Write([]byte(data)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return fsys.Written(name, false)
}

func truncateAsNFS(t *testing.T, fsys *nfsFileSystem, name string, size int64) {
	t.Helper()
	file, err := fsys.OpenFile(name, os.O_WRONLY|os.O_EXCL, 0)
	if err != nil {
		t.Fatalf("open %s to truncate: %v", name, err)
	}
	if err := file.Truncate(size); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func readAsNFS(t *testing.T, fsys *nfsFileSystem, name string) string {
	t.Helper()
	file, err := fsys.Open(name)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	contents, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(contents)
}

func TestNFSHeldWritesCompileOnceTheyMakeAWholeFile(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	page := strings.Repeat("a", pageSize)

	truncateAsNFS(t, fsys, objectPath, 0)
	if committed, err := writeAsNFS(t, fsys, objectPath, page, 0); committed || err != nil {
		t.Fatalf("a write ending on a page is not the end of a file: %v, %v", committed, err)
	}
	if written := client.writes(); len(written) != 0 {
		t.Fatalf("a part of a file reached the image: %v", written)
	}
	if info, err := fsys.Stat(objectPath); err != nil || info.Size() != pageSize {
		t.Fatalf("held writes should show their size, got %v, %v", info, err)
	}

	if committed, err := writeAsNFS(t, fsys, objectPath, "end", pageSize); !committed || err != nil {
		t.Fatalf("the write that ends the file should compile it: %v, %v", committed, err)
	}
	if written := client.writes(); len(written) != 1 || written[0] != objectPath+"="+page+"end" {
		t.Fatalf("the whole file should be compiled once, got %d writes", len(written))
	}
	if contents := readAsNFS(t, fsys, objectPath); contents != page+"end" {
		t.Fatalf("read after the compile: %d bytes", len(contents))
	}
}

func TestNFSWritesArrivingOutOfOrderWaitForTheGap(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	page := strings.Repeat("a", pageSize)

	truncateAsNFS(t, fsys, objectPath, 0)
	if committed, _ := writeAsNFS(t, fsys, objectPath, "end", pageSize); committed {
		t.Fatalf("a file with a gap is not whole")
	}
	if committed, err := writeAsNFS(t, fsys, objectPath, page, 0); !committed || err != nil {
		t.Fatalf("the write filling the gap should compile the file: %v, %v", committed, err)
	}
	if contents := client.contents(objectPath); contents != page+"end" {
		t.Fatalf("the image has %d bytes", len(contents))
	}
}

func TestNFSRefusedFileFailsTheWriteThatCompletesIt(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)

	truncateAsNFS(t, fsys, objectPath, 0)
	if _, err := writeAsNFS(t, fsys, objectPath, "syntax error", 0); !errors.Is(err, syscall.EIO) {
		t.Fatalf("a refused compile should fail the write with EIO, got %v", err)
	}
	if contents := readAsNFS(t, fsys, objectPath); contents != "old source" {
		t.Fatalf("the image's version should stand, read %q", contents)
	}
	if latest := readAsNFS(t, fsys, "errors/latest.txt"); !strings.Contains(latest, "Unexpected syntax error") {
		t.Fatalf("the refusal should be in errors/latest.txt, found %q", latest)
	}
}

func TestNFSCommitCompilesWhatIsStillHeld(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	page := strings.Repeat("a", pageSize)

	truncateAsNFS(t, fsys, objectPath, 0)
	_, _ = writeAsNFS(t, fsys, objectPath, page, 0)
	if err := fsys.Commit(objectPath); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if contents := client.contents(objectPath); contents != page {
		t.Fatalf("a file of whole pages should compile at the commit, image has %d bytes", len(contents))
	}

	truncateAsNFS(t, fsys, objectPath, 0)
	_, _ = writeAsNFS(t, fsys, objectPath, "syntax error"+page[12:], 0)
	if err := fsys.Commit(objectPath); err != nil {
		t.Fatalf("a refused commit is recorded, not answered: %v", err)
	}
	if contents := readAsNFS(t, fsys, objectPath); contents != page {
		t.Fatalf("the image's version should stand after a refused commit")
	}
	if latest := readAsNFS(t, fsys, "errors/latest.txt"); !strings.Contains(latest, "Unexpected syntax error") {
		t.Fatalf("the refusal should be in errors/latest.txt, found %q", latest)
	}
}

func TestNFSUnchangedPartsOfAFileSurviveAPartialWrite(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "abcdef"})
	fsys := newNFSFileSystem(client)

	if committed, err := writeAsNFS(t, fsys, objectPath, "XY", 2); !committed || err != nil {
		t.Fatalf("a write into a whole file should compile it: %v, %v", committed, err)
	}
	if contents := client.contents(objectPath); contents != "abXYef" {
		t.Fatalf("expected the write patched into the image's version, got %q", contents)
	}
}

func TestNFSCreatedTonelFileIsListedAndCompiledWhenWritten(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "object"})
	fsys := newNFSFileSystem(client)
	created := "/tonel/Kernel/Fresh.class.st"

	file, err := fsys.Create(created)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = file.Close()
	infos, err := fsys.ReadDir("/tonel/Kernel")
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if !containsInfoNamed(infos, "Fresh.class.st") {
		t.Fatalf("a created file should be listed before it is written: %v", namesOf(infos))
	}

	if committed, err := writeAsNFS(t, fsys, created, "fresh", 0); !committed || err != nil {
		t.Fatalf("write: %v, %v", committed, err)
	}
	if contents := client.contents(created); contents != "fresh" {
		t.Fatalf("the created file should be compiled, image has %q", contents)
	}
	if _, ok := fsys.overlay.Stat(created); ok {
		t.Fatalf("a compiled file should not stay in the overlay")
	}
}

func TestNFSEditorTempFileRenamedOverTonelFileCompiles(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old"})
	fsys := newNFSFileSystem(client)
	temp := "/tonel/Kernel/.Object.class.st.swp"

	if _, err := fsys.Create(temp); err != nil {
		t.Fatalf("create temp: %v", err)
	}
	if committed, err := writeAsNFS(t, fsys, temp, "syntax error", 0); !committed || err != nil {
		t.Fatalf("an editor's file is stored as written: %v, %v", committed, err)
	}
	if err := fsys.Rename(temp, objectPath); !errors.Is(err, syscall.EIO) {
		t.Fatalf("renaming a refused file over a class should fail with EIO, got %v", err)
	}

	truncateAsNFS(t, fsys, temp, 0)
	_, _ = writeAsNFS(t, fsys, temp, "new   ", 0)
	if err := fsys.Rename(temp, objectPath); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if contents := client.contents(objectPath); contents != "new   " {
		t.Fatalf("the rename should compile the temp file, image has %q", contents)
	}
}

func TestNFSReadOnlyFilesRefuseWriting(t *testing.T) {
	client := newMemoryClient(map[string]string{"/critiques/Kernel/Object.md": "critique"})
	fsys := newNFSFileSystem(client)

	_, err := fsys.OpenFile("critiques/Kernel/Object.md", os.O_RDWR, 0)
	if !errors.Is(err, syscall.EROFS) {
		t.Fatalf("expected EROFS, got %v", err)
	}
	info, err := fsys.Stat("critiques/Kernel/Object.md")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm()&0o200 != 0 {
		t.Fatalf("a read-only file should not show an owner write bit: %v", info.Mode())
	}
}

func TestNFSRemovingAnOpenClassKeepsItReadable(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "object source"})
	fsys := newNFSFileSystem(client)
	silly := "/tonel/Kernel/.nfs0000000012345678"

	if err := fsys.Rename(objectPath, silly); err != nil {
		t.Fatalf("silly rename: %v", err)
	}
	if client.has(objectPath) {
		t.Fatalf("removing the file should remove the class")
	}
	if contents := readAsNFS(t, fsys, silly); contents != "object source" {
		t.Fatalf("the open file should stay readable, read %q", contents)
	}
	if err := fsys.Remove(silly); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := fsys.Stat(silly); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("the renamed file should be gone, got %v", err)
	}
}

func TestNFSModificationTimeMovesWithEachChange(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	page := strings.Repeat("a", pageSize)
	before := modifiedAt(t, fsys, objectPath)

	truncateAsNFS(t, fsys, objectPath, 0)
	_, _ = writeAsNFS(t, fsys, objectPath, page, 0)
	held := modifiedAt(t, fsys, objectPath)
	_, _ = writeAsNFS(t, fsys, objectPath, page, pageSize)
	if modifiedAt(t, fsys, objectPath) != held {
		t.Fatalf("held writes should keep one time, as the client's own")
	}
	if _, err := writeAsNFS(t, fsys, objectPath, "end", 2*pageSize); err != nil {
		t.Fatalf("write: %v", err)
	}
	committed := modifiedAt(t, fsys, objectPath)
	if !committed.After(held) || !held.After(before) {
		t.Fatalf("times should move forward: before %v, held %v, committed %v", before, held, committed)
	}

	client.put(objectPath, "changed in the image")
	if !modifiedAt(t, fsys, objectPath).After(committed) {
		t.Fatalf("a change of size in the image should move the time")
	}

	reported := time.Now().Add(time.Hour)
	client.setModified(objectPath, reported.UnixMilli())
	if got := modifiedAt(t, fsys, objectPath); got.UnixMilli() != reported.UnixMilli() {
		t.Fatalf("the image's own time should be used, got %v want %v", got, reported)
	}
}

func TestNFSDirectoryTimeMovesWhenTheImageAddsAClass(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "object"})
	fsys := newNFSFileSystem(client)
	before := modifiedAt(t, fsys, "/tonel/Kernel")
	if again := modifiedAt(t, fsys, "/tonel/Kernel"); again != before {
		t.Fatalf("an unchanged listing should keep its time")
	}

	client.put("/tonel/Kernel/Added.class.st", "added")
	fsys.mu.Lock()
	known := fsys.listings["/tonel/Kernel"]
	known.checked = time.Time{}
	fsys.listings["/tonel/Kernel"] = known
	fsys.mu.Unlock()
	if !modifiedAt(t, fsys, "/tonel/Kernel").After(before) {
		t.Fatalf("a class added in the image should move its package's time")
	}
}

func TestHeldSpansMerge(t *testing.T) {
	spans := withSpan(nil, span{10, 20})
	spans = withSpan(spans, span{0, 5})
	spans = withSpan(spans, span{30, 40})
	spans = withSpan(spans, span{5, 10})
	if len(spans) != 2 || spans[0] != (span{0, 20}) || spans[1] != (span{30, 40}) {
		t.Fatalf("unexpected spans %v", spans)
	}
	spans = withSpan(spans, span{15, 35})
	if len(spans) != 1 || spans[0] != (span{0, 40}) {
		t.Fatalf("unexpected spans %v", spans)
	}
}

func TestNFSRefusesAppleDoubleFiles(t *testing.T) {
	fsys := newNFSFileSystem(newMemoryClient(map[string]string{objectPath: "object"}))

	if _, err := fsys.Create("/tonel/Kernel/._Object.class.st"); !errors.Is(err, syscall.ENOTSUP) {
		t.Fatalf("creating a ._ file should fail with ENOTSUP, got %v", err)
	}
	if _, err := fsys.Create("/tonel/Kernel/.Object.class.st.tmp"); err != nil {
		t.Fatalf("an editor's own file should still be created: %v", err)
	}
	if err := fsys.Rename("/tonel/Kernel/.Object.class.st.tmp", "/tonel/Kernel/._Object.class.st"); !errors.Is(err, syscall.ENOTSUP) {
		t.Fatalf("renaming to a ._ file should fail with ENOTSUP, got %v", err)
	}
}

func TestNFSHandlesNameOnePathForTheDaemonsLife(t *testing.T) {
	handler := newNFSHandler(newNFSFileSystem(newMemoryClient(nil)))
	handle := handler.ToHandle(nil, []string{"tonel", "Kernel"})
	if again := handler.ToHandle(nil, []string{"tonel", "Kernel"}); string(again) != string(handle) {
		t.Fatalf("one path should keep one handle")
	}
	_, components, err := handler.FromHandle(handle)
	if err != nil || strings.Join(components, "/") != "tonel/Kernel" {
		t.Fatalf("handle should name tonel/Kernel, got %v, %v", components, err)
	}
	if _, _, err := handler.FromHandle([]byte{0, 0, 0, 0, 0, 0, 0, 99}); err == nil {
		t.Fatalf("an unknown handle should be stale")
	}
	var _ nfs.Committer = &nfsFileSystem{}
	var _ nfs.CachingHandler = handler
}

func modifiedAt(t *testing.T, fsys *nfsFileSystem, name string) time.Time {
	t.Helper()
	info, err := fsys.Lstat(name)
	if err != nil {
		t.Fatalf("lstat %s: %v", name, err)
	}
	return info.ModTime()
}

func containsInfoNamed(infos []os.FileInfo, name string) bool {
	for _, info := range infos {
		if info.Name() == name {
			return true
		}
	}
	return false
}

func namesOf(infos []os.FileInfo) []string {
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		names = append(names, info.Name())
	}
	return names
}

// memoryClient is an image projecting files from memory, safe to serve from
// several goroutines. It refuses to compile any file containing "syntax
// error", and lets only /tonel be written.
type memoryClient struct {
	mu       sync.Mutex
	files    map[string]string
	modified map[string]int64
	written  []string
}

func newMemoryClient(files map[string]string) *memoryClient {
	client := &memoryClient{files: map[string]string{}, modified: map[string]int64{}}
	for filePath, contents := range files {
		client.files[filePath] = contents
	}
	return client
}

func (c *memoryClient) put(filePath string, contents string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.files[filePath] = contents
}

func (c *memoryClient) setModified(filePath string, millis int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.modified[filePath] = millis
}

func (c *memoryClient) contents(filePath string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.files[filePath]
}

func (c *memoryClient) has(filePath string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.files[filePath]
	return ok
}

func (c *memoryClient) writes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.written...)
}

func (c *memoryClient) isDirectoryLocked(dirPath string) bool {
	if dirPath == "/" {
		return true
	}
	for filePath := range c.files {
		if strings.HasPrefix(filePath, dirPath+"/") {
			return true
		}
	}
	return dirPath == "/tonel"
}

func (c *memoryClient) List(_ context.Context, dirPath string) ([]protocol.Entry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.isDirectoryLocked(dirPath) {
		return nil, &protocol.Error{StatusCode: http.StatusNotFound, Message: "Projection path not found: " + dirPath}
	}
	seen := map[string]protocol.Entry{}
	prefix := strings.TrimSuffix(dirPath, "/") + "/"
	for filePath, contents := range c.files {
		if !strings.HasPrefix(filePath, prefix) {
			continue
		}
		rest := strings.TrimPrefix(filePath, prefix)
		if name, _, nested := strings.Cut(rest, "/"); nested {
			seen[name] = protocol.Entry{Name: name, Kind: protocol.Directory}
		} else {
			seen[name] = protocol.Entry{Name: name, Kind: protocol.File, Size: uint64(len(contents)), Writable: strings.HasPrefix(filePath, "/tonel/")}
		}
	}
	entries := make([]protocol.Entry, 0, len(seen))
	for _, entry := range seen {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, nil
}

func (c *memoryClient) Stat(_ context.Context, filePath string) (protocol.Entry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if contents, ok := c.files[filePath]; ok {
		return protocol.Entry{
			Name:     path.Base(filePath),
			Kind:     protocol.File,
			Size:     uint64(len(contents)),
			Writable: strings.HasPrefix(filePath, "/tonel/"),
			Modified: c.modified[filePath],
		}, nil
	}
	if c.isDirectoryLocked(filePath) {
		return protocol.Entry{Name: path.Base(filePath), Kind: protocol.Directory}, nil
	}
	return protocol.Entry{}, &protocol.Error{StatusCode: http.StatusNotFound, Message: "Projection path not found: " + filePath}
}

func (c *memoryClient) Read(_ context.Context, filePath string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	contents, ok := c.files[filePath]
	if !ok {
		return nil, &protocol.Error{StatusCode: http.StatusNotFound, Message: "Projection path not found: " + filePath}
	}
	return []byte(contents), nil
}

func (c *memoryClient) Write(_ context.Context, filePath string, contents []byte) (protocol.WriteResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !strings.HasPrefix(filePath, "/tonel/") {
		return protocol.WriteResult{}, &protocol.Error{StatusCode: http.StatusForbidden, Message: "Projection path is not writable yet: " + filePath}
	}
	if strings.Contains(string(contents), "syntax error") {
		return protocol.WriteResult{}, &protocol.Error{StatusCode: http.StatusBadRequest, Message: "Unexpected syntax error at line 1"}
	}
	c.files[filePath] = string(contents)
	c.written = append(c.written, filePath+"="+string(contents))
	return protocol.WriteResult{}, nil
}

func (c *memoryClient) Delete(_ context.Context, filePath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.files[filePath]; !ok {
		return &protocol.Error{StatusCode: http.StatusNotFound}
	}
	delete(c.files, filePath)
	return nil
}

func (c *memoryClient) Rename(_ context.Context, sourcePath string, targetPath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	contents, ok := c.files[sourcePath]
	if !ok {
		return &protocol.Error{StatusCode: http.StatusNotFound}
	}
	delete(c.files, sourcePath)
	c.files[targetPath] = contents
	return nil
}

func (c *memoryClient) Mkdir(context.Context, string) error {
	return nil
}

func (c *memoryClient) Rmdir(context.Context, string) error {
	return nil
}
