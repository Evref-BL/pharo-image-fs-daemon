package mount

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
	nfs "github.com/willscott/go-nfs"
	nfsfile "github.com/willscott/go-nfs/file"
)

const objectPath = "/tonel/Kernel/Object.class.st"

// writeAsNFS writes data at offset to name as go-nfs does for one WRITE
// call, and answers whether Written answered it committed.
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
	committed, _, err := fsys.Written(name, false)
	return committed, err
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

// writeNFS is writeAsNFS for a goroutine, which must not fail the test.
func writeNFS(fsys *nfsFileSystem, name string, data string, offset int64) error {
	file, err := fsys.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	if _, err := file.Write([]byte(data)); err != nil {
		return err
	}
	_, _, err = fsys.Written(name, false)
	return err
}

// readNFS is readAsNFS for a goroutine, which must not fail the test.
func readNFS(fsys *nfsFileSystem, name string) (string, error) {
	file, err := fsys.Open(name)
	if err != nil {
		return "", err
	}
	contents, err := io.ReadAll(file)
	return string(contents), err
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

func TestNFSRemovingAnOpenClassKeepsItUntilTheLastClose(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "object source", "/tonel/Kernel/Other.class.st": "other"})
	fsys := newNFSFileSystem(client)
	silly := "/tonel/Kernel/.nfs0000000012345678"

	if err := fsys.Rename(objectPath, silly); err != nil {
		t.Fatalf("silly rename: %v", err)
	}
	if _, err := fsys.Stat(objectPath); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("the removed name should be gone, got %v", err)
	}
	if listed, _ := fsys.ReadDir("/tonel/Kernel"); containsInfoNamed(listed, "Object.class.st") {
		t.Fatalf("the removed name should not be listed")
	}
	if contents := readAsNFS(t, fsys, silly); contents != "object source" {
		t.Fatalf("the open file should stay readable, read %q", contents)
	}
	if !client.has(objectPath) {
		t.Fatalf("the class should stay until the last close")
	}
	if err := fsys.Remove(silly); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if client.has(objectPath) {
		t.Fatalf("the last close should remove the class")
	}
	if _, err := fsys.Stat(silly); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("the renamed file should be gone, got %v", err)
	}
}

func TestNFSASaveOverAnOpenClassCompilesOverIt(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "object source", "/tonel/Kernel/Other.class.st": "other"})
	fsys := newNFSFileSystem(client)
	silly := "/tonel/Kernel/.nfs0000000012345678"
	temp := "/tonel/Kernel/.Object.class.st.swp"

	if err := fsys.Rename(objectPath, silly); err != nil {
		t.Fatalf("silly rename: %v", err)
	}
	if _, err := fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = writeAsNFS(t, fsys, temp, "saved source", 0)
	if err := fsys.Rename(temp, objectPath); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := fsys.Remove(silly); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if contents := client.contents(objectPath); !client.has(objectPath) || contents != "saved source" {
		t.Fatalf("the save should compile over the class and keep it, image has %q", contents)
	}
	if deleted := client.deletes(); len(deleted) != 0 {
		t.Fatalf("the save should compile over the class, not remove it first: removed %v", deleted)
	}
}

func TestNFSARefusedSaveOverAnOpenClassKeepsIt(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "object source"})
	fsys := newNFSFileSystem(client)
	silly := "/tonel/Kernel/.nfs0000000012345678"
	temp := "/tonel/Kernel/.Object.class.st.swp"

	if err := fsys.Rename(objectPath, silly); err != nil {
		t.Fatalf("silly rename: %v", err)
	}
	if _, err := fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = writeAsNFS(t, fsys, temp, "syntax error", 0)
	if err := fsys.Rename(temp, objectPath); !errors.Is(err, syscall.EIO) {
		t.Fatalf("a refused save should fail with EIO, got %v", err)
	}
	if contents := readAsNFS(t, fsys, objectPath); contents != "object source" {
		t.Fatalf("the class should show again under its name, read %q", contents)
	}
	if err := fsys.Remove(silly); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !client.has(objectPath) {
		t.Fatalf("the last close should not remove a class shown again")
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
	expireListings(fsys)
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
	if err != nil {
		t.Fatalf("from handle: %v", err)
	}
	if resolved, _ := handler.fsys.ids.resolve(path.Join(components...)); resolved != "/tonel/Kernel" {
		t.Fatalf("handle should name /tonel/Kernel, got %q", resolved)
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
	deleted  []string
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
	c.deleted = append(c.deleted, filePath)
	return nil
}

func (c *memoryClient) deletes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.deleted...)
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

func TestNFSConcurrentWritesToAnEditorFileLoseNothing(t *testing.T) {
	// Writes that run one after another lose nothing whether they wait for
	// one another or not.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(max(4, runtime.GOMAXPROCS(0))))
	fsys := newNFSFileSystem(newMemoryClient(map[string]string{objectPath: "old"}))
	temp := "/tonel/Kernel/.Object.class.st.swp"
	if _, err := fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}

	const pages = 32
	var group sync.WaitGroup
	for page := range pages {
		group.Add(1)
		go func() {
			defer group.Done()
			file, err := fsys.OpenFile(temp, os.O_RDWR, 0)
			if err != nil {
				t.Errorf("open: %v", err)
				return
			}
			_, _ = file.Seek(int64(page*pageSize), io.SeekStart)
			if _, err := file.Write([]byte(strings.Repeat(string(rune('a'+page%26)), pageSize))); err != nil {
				t.Errorf("write: %v", err)
			}
		}()
	}
	group.Wait()

	contents := readAsNFS(t, fsys, temp)
	if len(contents) != pages*pageSize {
		t.Fatalf("the file has %d bytes, %d were written", len(contents), pages*pageSize)
	}
	for page := range pages {
		if contents[page*pageSize] != byte('a'+page%26) {
			t.Fatalf("page %d was lost to another write", page)
		}
	}
}

func TestNFSHeldFileCompilesOnceWhileItsCompileRuns(t *testing.T) {
	client := newGatedClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	truncateAsNFS(t, fsys, objectPath, 0)
	started, open := client.holdNext("write")
	defer open()

	first := make(chan error, 1)
	go func() { first <- writeNFS(fsys, objectPath, "new source", 0) }()
	waitHeld(t, started)
	second := make(chan error, 1)
	go func() {
		_, _, err := fsys.Written(objectPath, false)
		second <- err
	}()
	select {
	case <-second:
		t.Fatalf("a second commit should wait for the compile running")
	case <-time.After(100 * time.Millisecond):
	}
	open()
	if err := within(t, first, "the first commit"); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := within(t, second, "the second commit"); err != nil {
		t.Fatalf("second: %v", err)
	}
	if written := client.writes(); len(written) != 1 {
		t.Fatalf("the whole file should compile once, got %d compiles", len(written))
	}
}

func TestNFSAHandleOvertakenByARenameNamesItsFile(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	handler := newNFSHandler(fsys)
	temp := "/tonel/Kernel/.Object.class.st.swp"
	if _, err := fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = writeAsNFS(t, fsys, temp, "new source", 0)
	nameFor := func(projectionPath string) string {
		_, components, err := handler.FromHandle(handler.ToHandle(fsys, strings.Split(strings.TrimPrefix(projectionPath, "/"), "/")))
		if err != nil {
			t.Fatalf("handle of %s: %v", projectionPath, err)
		}
		return path.Join(components...)
	}
	className, tempName := nameFor(objectPath), nameFor(temp)
	tempID := fsys.ids.idFor(temp)

	if err := fsys.Rename(temp, objectPath); err != nil {
		t.Fatalf("rename: %v", err)
	}

	if _, err := fsys.Lstat(className); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("a name from the replaced file's handle should be stale, got %v", err)
	}
	if _, err := fsys.OpenFile(className, os.O_RDWR, 0); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("writing through the replaced file's handle should be stale, got %v", err)
	}
	if _, _, err := fsys.Written(className, false); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("committing through the replaced file's handle should be stale, got %v", err)
	}
	info, err := fsys.Lstat(tempName)
	if err != nil {
		t.Fatalf("lstat through the renamed file's handle: %v", err)
	}
	if info.Name() != "Object.class.st" || info.Sys().(*nfsfile.FileInfo).Fileid != tempID {
		t.Fatalf("the renamed file's handle should name it where it is, got %s, id %d", info.Name(), info.Sys().(*nfsfile.FileInfo).Fileid)
	}
	if written := client.writes(); len(written) != 1 {
		t.Fatalf("only the rename should compile, got %d compiles", len(written))
	}
}

func TestNFSACallOnAFileWaitsWhileTheImageRenamesIt(t *testing.T) {
	renamed := "/tonel/Kernel/Renamed.class.st"
	client := newGatedClient(map[string]string{objectPath: "source"})
	fsys := newNFSFileSystem(client)
	handler := newNFSHandler(fsys)
	_, components, _ := handler.FromHandle(handler.ToHandle(fsys, []string{"tonel", "Kernel", "Object.class.st"}))
	objectID := fsys.ids.idFor(objectPath)
	started, open := client.holdNext("rename")
	defer open()

	renaming := make(chan error, 1)
	go func() { renaming <- fsys.Rename(objectPath, renamed) }()
	waitHeld(t, started)
	looked := make(chan os.FileInfo, 1)
	go func() {
		info, _ := fsys.Lstat(path.Join(components...))
		looked <- info
	}()
	select {
	case <-looked:
		t.Fatalf("a call on a file should wait while the image renames it")
	case <-time.After(100 * time.Millisecond):
	}
	open()
	if err := within(t, renaming, "the rename"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	info := within(t, looked, "the call on the file")
	if info == nil || info.Name() != "Renamed.class.st" || info.Sys().(*nfsfile.FileInfo).Fileid != objectID {
		t.Fatalf("the call should answer the file where the rename put it, got %v", info)
	}
}

func TestNFSAReadRacingASaveIsNotKept(t *testing.T) {
	client := newGatedClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	started, open := client.holdNext("read")
	defer open()

	type result struct {
		contents string
		err      error
	}
	read := make(chan result, 1)
	go func() {
		contents, err := readNFS(fsys, objectPath)
		read <- result{contents, err}
	}()
	waitHeld(t, started)
	truncateAsNFS(t, fsys, objectPath, 0)
	if _, err := writeAsNFS(t, fsys, objectPath, "new source", 0); err != nil {
		t.Fatalf("save: %v", err)
	}
	open()
	if got := within(t, read, "the read"); got.err != nil || got.contents != "new source" {
		t.Fatalf("a read the save overtook should read again, got %q, %v", got.contents, got.err)
	}
	if contents := readAsNFS(t, fsys, objectPath); contents != "new source" {
		t.Fatalf("the read before the save should not be kept, read %q", contents)
	}
}

func TestNFSAReadOvertakenByARenameSaveIsNotKept(t *testing.T) {
	client := newGatedClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	temp := "/tonel/Kernel/.Object.class.st.swp"
	if _, err := fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}
	// As long as the old source, so that only the save, not the size, tells
	// the two apart.
	_, _ = writeAsNFS(t, fsys, temp, "new source", 0)

	statted, openStat := client.holdNext("stat")
	defer openStat()
	type result struct {
		contents string
		err      error
	}
	read := make(chan result, 1)
	go func() {
		contents, err := readNFS(fsys, objectPath)
		read <- result{contents, err}
	}()
	waitHeld(t, statted)

	compiling, openCompile := client.holdNext("write")
	defer openCompile()
	renaming := make(chan error, 1)
	go func() { renaming <- fsys.Rename(temp, objectPath) }()
	waitHeld(t, compiling)

	reading, openRead := client.holdNext("read")
	defer openRead()
	openStat()
	waitHeld(t, reading)
	openCompile()
	if err := within(t, renaming, "the rename"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	openRead()
	if got := within(t, read, "the read"); got.err != nil || got.contents != "new source" {
		t.Fatalf("a read the rename save overtook should read again, got %q, %v", got.contents, got.err)
	}
	if contents := readAsNFS(t, fsys, objectPath); contents != "new source" {
		t.Fatalf("what the image rendered during the save should not be kept, read %q", contents)
	}
}

func TestNFSAnEditorFileReadsAsWritten(t *testing.T) {
	fsys := newNFSFileSystem(newMemoryClient(map[string]string{objectPath: "source"}))
	temp := "/tonel/Kernel/.Object.class.st.swp"
	if _, err := fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _ = writeAsNFS(t, fsys, temp, "first", 0)
	if contents := readAsNFS(t, fsys, temp); contents != "first" {
		t.Fatalf("read %q", contents)
	}
	before := modifiedAt(t, fsys, temp)
	_, _ = writeAsNFS(t, fsys, temp, "fresh", 0)
	if contents := readAsNFS(t, fsys, temp); contents != "fresh" {
		t.Fatalf("a read after a write should answer it, read %q", contents)
	}
	if !modifiedAt(t, fsys, temp).After(before) {
		t.Fatalf("a write of the same size should still move the file's time")
	}
}

func TestNFSEachRefusalShowsInLatestError(t *testing.T) {
	fsys := newNFSFileSystem(&echoRefusingClient{memoryClient: newMemoryClient(map[string]string{objectPath: "source"})})
	latest := "/errors/latest.txt"

	truncateAsNFS(t, fsys, objectPath, 0)
	_, _ = writeAsNFS(t, fsys, objectPath, "refusal A", 0)
	if first := readAsNFS(t, fsys, latest); !strings.Contains(first, "refusal A") {
		t.Fatalf("latest.txt should answer the first refusal, read %q", first)
	}
	before := modifiedAt(t, fsys, latest)
	truncateAsNFS(t, fsys, objectPath, 0)
	_, _ = writeAsNFS(t, fsys, objectPath, "refusal B", 0)
	if second := readAsNFS(t, fsys, latest); !strings.Contains(second, "refusal B") {
		t.Fatalf("latest.txt should answer the second refusal, read %q", second)
	}
	if !modifiedAt(t, fsys, latest).After(before) {
		t.Fatalf("a refusal as long as the last should still move latest.txt's time")
	}
}

// echoRefusingClient refuses every compile, saying what it was given.
type echoRefusingClient struct {
	*memoryClient
}

func (c *echoRefusingClient) Write(_ context.Context, _ string, contents []byte) (protocol.WriteResult, error) {
	return protocol.WriteResult{}, &protocol.Error{StatusCode: http.StatusBadRequest, Message: string(contents)}
}

func TestNFSSillyRenamesOwnOnlyTheirOwnRemoval(t *testing.T) {
	for _, hideAgain := range []string{"/tonel/Kernel/.nfs0000000000000002", "/tonel/Kernel/Object.class.st~"} {
		client := newMemoryClient(map[string]string{objectPath: "object source", "/tonel/Kernel/Other.class.st": "other"})
		fsys := newNFSFileSystem(client)
		first := "/tonel/Kernel/.nfs0000000000000001"
		temp := "/tonel/Kernel/.Object.class.st.swp"

		if err := fsys.Rename(objectPath, first); err != nil {
			t.Fatalf("silly rename: %v", err)
		}
		if _, err := fsys.Create(temp); err != nil {
			t.Fatalf("create: %v", err)
		}
		_, _ = writeAsNFS(t, fsys, temp, "saved source", 0)
		if err := fsys.Rename(temp, objectPath); err != nil {
			t.Fatalf("save: %v", err)
		}
		if err := fsys.Rename(objectPath, hideAgain); err != nil {
			t.Fatalf("rename to %s: %v", hideAgain, err)
		}
		if err := fsys.Remove(first); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if !client.has(objectPath) {
			t.Fatalf("closing the version a save replaced should not remove the class (then renamed to %s)", path.Base(hideAgain))
		}
	}
}

func TestNFSARefusedRemovalAtTheLastCloseShowsTheClassAgain(t *testing.T) {
	client := &refusingDeleteClient{memoryClient: newMemoryClient(map[string]string{objectPath: "object source"})}
	fsys := newNFSFileSystem(client)
	silly := "/tonel/Kernel/.nfs0000000012345678"

	if err := fsys.Rename(objectPath, silly); err != nil {
		t.Fatalf("silly rename: %v", err)
	}
	if err := fsys.Remove(silly); err != nil {
		t.Fatalf("the last close's remove should answer what it did to the .nfs file: %v", err)
	}
	if _, err := fsys.Stat(silly); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("the .nfs file should be gone, got %v", err)
	}
	if contents := readAsNFS(t, fsys, objectPath); contents != "object source" {
		t.Fatalf("the class the image kept should show again, read %q", contents)
	}
	if latest := readAsNFS(t, fsys, "/errors/latest.txt"); !strings.Contains(latest, "Object.class.st") {
		t.Fatalf("the refusal should be in latest.txt, read %q", latest)
	}
}

func TestNFSARefusedRenameOntoAnOpenClassShowsItAgain(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "object source", "/tonel/Kernel/Other.class.st": "other"})
	fsys := newNFSFileSystem(client)
	silly := "/tonel/Kernel/.nfs0000000012345678"
	other := "/tonel/Kernel/Other.class.st"

	truncateAsNFS(t, fsys, other, 0)
	_, _ = writeAsNFS(t, fsys, other, strings.Repeat("syntax error ", pageSize/13+1)[:pageSize], 0)
	if err := fsys.Rename(objectPath, silly); err != nil {
		t.Fatalf("silly rename: %v", err)
	}
	if err := fsys.Rename(other, objectPath); err == nil {
		t.Fatalf("renaming a file whose held writes are refused should fail")
	}
	if contents := readAsNFS(t, fsys, objectPath); contents != "object source" {
		t.Fatalf("the class should show again, read %q", contents)
	}
	if err := fsys.Remove(silly); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !client.has(objectPath) {
		t.Fatalf("the last close should not remove a class shown again")
	}
}

// refusingDeleteClient refuses to remove any class, as the image does in a
// package no repository owns.
type refusingDeleteClient struct {
	*memoryClient
}

func (c *refusingDeleteClient) Delete(context.Context, string) error {
	return &protocol.Error{StatusCode: http.StatusBadRequest, Message: "Package is not owned by a repository"}
}

func TestNFSAWriteFollowsItsFileToAnEditorBackup(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	backup := "/tonel/Kernel/Object.class.st~"

	file, err := fsys.OpenFile(objectPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := fsys.Rename(objectPath, backup); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, err := file.Write([]byte("NEW")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if contents, ok := fsys.overlay.Read(backup); !ok || string(contents) != "NEW source" {
		t.Fatalf("the write should reach the backup in the overlay, has %q", contents)
	}
	fsys.mu.Lock()
	_, held := fsys.pending[backup]
	fsys.mu.Unlock()
	if held {
		t.Fatalf("an editor's backup should not hold writes for the image")
	}
}

// expireListings makes the next stat of a directory list it again, as it
// does once its last listing is a second old.
func expireListings(fsys *nfsFileSystem) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	for directory, known := range fsys.listings {
		known.checked = time.Time{}
		fsys.listings[directory] = known
	}
}

func TestNFSToolsTempFilesAreNotClassFiles(t *testing.T) {
	for _, projected := range []string{objectPath, "/tonel/Kernel/Éclair.class.st", "/tonel/Kernel/Object_2.extension.st"} {
		if !isProjectedTonelFilePath(projected) {
			t.Errorf("%s should be a class's file", projected)
		}
	}
	for _, other := range []string{"/tonel/Kernel/.!123!Object.class.st", "/tonel/Kernel/.Object.class.st", "/tonel/Kernel/2Object.class.st", "/tonel/Kernel/.class.st", "/tonel/Kernel/Object.class.st~"} {
		if isProjectedTonelFilePath(other) {
			t.Errorf("%s should be a file of the mount's own", other)
		}
	}

	// macOS sed -i writes its output beside the file, then renames it over.
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	temp := "/tonel/Kernel/.!123!Object.class.st"
	if _, err := fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := writeAsNFS(t, fsys, temp, "new source", 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := fsys.Rename(temp, objectPath); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if writes := client.writes(); len(writes) != 1 || writes[0] != objectPath+"=new source" {
		t.Fatalf("sed's save should compile once, onto the class, got %v", writes)
	}
}

func TestNFSEachRefusalHasItsOwnName(t *testing.T) {
	other := "/tonel/Kernel/Other.class.st"
	fsys := newNFSFileSystem(&echoRefusingClient{newMemoryClient(map[string]string{objectPath: "source", other: "other"})})
	truncateAsNFS(t, fsys, objectPath, 0)
	_, _ = writeAsNFS(t, fsys, objectPath, "refusal A", 0)
	truncateAsNFS(t, fsys, other, 0)
	_, _ = writeAsNFS(t, fsys, other, "refusal B", 0)

	infos, err := fsys.ReadDir("/errors")
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	read := map[string]bool{}
	for _, name := range namesOf(infos) {
		if name == latestErrorName {
			continue
		}
		if read[name] {
			t.Fatalf("/errors lists %s twice", name)
		}
		read[name] = true
		contents := readAsNFS(t, fsys, "/errors/"+name)
		for _, refusal := range []string{"refusal A", "refusal B"} {
			if strings.Contains(contents, refusal) {
				read[refusal] = true
			}
		}
	}
	if !read["refusal A"] || !read["refusal B"] {
		t.Fatalf("each refusal should be read under its own name, read %v", read)
	}
}

func TestNFSARefusedNewClassLeavesItsDirectory(t *testing.T) {
	created := "/tonel/Kernel/New.class.st"
	fsys := newNFSFileSystem(newMemoryClient(map[string]string{objectPath: "source"}))
	if _, err := fsys.Create(created); err != nil {
		t.Fatalf("create: %v", err)
	}
	listed, err := fsys.ReadDir("/tonel/Kernel")
	if err != nil || !containsInfoNamed(listed, "New.class.st") {
		t.Fatalf("a created file should be listed, got %v %v", namesOf(listed), err)
	}
	before := modifiedAt(t, fsys, "/tonel/Kernel")
	if _, err := writeAsNFS(t, fsys, created, "syntax error", 0); err == nil {
		t.Fatalf("the image should refuse the class")
	}
	// The client keeps the listing it has while the directory's time holds.
	if modifiedAt(t, fsys, "/tonel/Kernel") == before {
		t.Fatalf("a refused new class leaves its directory, whose time should move")
	}
}

func TestNFSAListingServedMovesItsDirectorysTime(t *testing.T) {
	added := "/tonel/Kernel/Added.class.st"
	client := newMemoryClient(map[string]string{objectPath: "source"})
	fsys := newNFSFileSystem(client)
	before := modifiedAt(t, fsys, "/tonel/Kernel")

	// A test adds a class and removes it in less than the second a
	// directory's listing is trusted for, while the client lists it.
	client.put(added, "added")
	listed, err := fsys.ReadDir("/tonel/Kernel")
	if err != nil || !containsInfoNamed(listed, "Added.class.st") {
		t.Fatalf("the listing should name the added class, got %v %v", namesOf(listed), err)
	}
	served := modifiedAt(t, fsys, "/tonel/Kernel")
	if served == before {
		t.Fatalf("a listing that differs from the last should move the directory's time")
	}
	_ = client.Delete(context.Background(), added)
	expireListings(fsys)
	if modifiedAt(t, fsys, "/tonel/Kernel") == served {
		t.Fatalf("the client would keep listing the removed class under the time it was served with")
	}
}

func TestNFSOverlappingStatsNeverAnswerAnOlderSize(t *testing.T) {
	client := newGatedClient(map[string]string{objectPath: "version one"})
	fsys := newNFSFileSystem(client)
	if size := statSize(t, fsys); size != len("version one") {
		t.Fatalf("size %d", size)
	}

	started, open := client.holdNext("stat")
	defer open()
	slow := make(chan os.FileInfo, 1)
	go func() {
		info, _ := fsys.Lstat(objectPath)
		slow <- info
	}()
	waitHeld(t, started)
	client.put(objectPath, "version two, longer")
	fast := modifiedAt(t, fsys, objectPath)
	open()

	// The slow stat read the image before it changed.
	if info := within(t, slow, "stat"); info == nil || info.Size() != int64(len("version two, longer")) {
		t.Fatalf("a stat that read the image before another stat saw it change should not answer the older size, got %v", info)
	}
	if size, modified := statSize(t, fsys), modifiedAt(t, fsys, objectPath); size != len("version two, longer") || modified != fast {
		t.Fatalf("the file changed once, but answers size %d at %v after %v", size, modified, fast)
	}
}

func statSize(t *testing.T, fsys *nfsFileSystem) int {
	t.Helper()
	info, err := fsys.Lstat(objectPath)
	if err != nil {
		t.Fatalf("lstat: %v", err)
	}
	return int(info.Size())
}

func TestNFSARefusalMovesLatestErrorsTimePastAnyStat(t *testing.T) {
	latest := "/errors/" + latestErrorName
	fsys := newNFSFileSystem(&echoRefusingClient{newMemoryClient(map[string]string{objectPath: "source"})})
	// The daemon's own times run past the image's millisecond ones, as one
	// taken in the millisecond a refusal is recorded in does.
	fsys.clock = func() time.Time { return time.Now().Add(time.Hour) }
	refuse := func(text string) {
		truncateAsNFS(t, fsys, objectPath, 0)
		_, _ = writeAsNFS(t, fsys, objectPath, text, 0)
	}

	refuse("a long first refusal")
	modifiedAt(t, fsys, latest)
	refuse("refusal A")
	before := modifiedAt(t, fsys, latest)
	refuse("refusal B")
	if modifiedAt(t, fsys, latest) == before {
		t.Fatalf("a refusal as long as the last should move latest.txt's time")
	}
}

func TestNFSTimesNeverGoBackWithTheWallClock(t *testing.T) {
	temp := "/tonel/Kernel/.Object.class.st.swp"
	fsys := newNFSFileSystem(newMemoryClient(map[string]string{objectPath: "source"}))
	behind := time.Now().Round(0).Add(-time.Minute)
	fsys.clock = func() time.Time { return behind }
	if _, err := fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}
	first := modifiedAt(t, fsys, temp)
	if !first.After(fsys.started) {
		t.Fatalf("a change after the wall clock stepped back should still be after the start")
	}
	behind = behind.Add(-time.Minute)
	if err := writeNFS(fsys, temp, "x", 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !modifiedAt(t, fsys, temp).After(first) {
		t.Fatalf("a change after the wall clock stepped back should still be after the last")
	}
}

func TestNFSASizeSetAfterASaveLeavesTheFileTheImages(t *testing.T) {
	client := &renderingClient{newMemoryClient(map[string]string{objectPath: "old source"})}
	fsys := newNFSFileSystem(client)
	// f.write(source); f.truncate(): the client flushes, then sets the size
	// of what it wrote.
	truncateAsNFS(t, fsys, objectPath, 0)
	if _, err := writeAsNFS(t, fsys, objectPath, "new source", 0); err != nil {
		t.Fatalf("save: %v", err)
	}
	truncateAsNFS(t, fsys, objectPath, int64(len("new source")))
	// The same with a file the image renders as written.
	truncateAsNFS(t, fsys, objectPath, int64(len(client.contents(objectPath))))

	if writes := client.writes(); len(writes) != 1 {
		t.Fatalf("the save should compile once, got %v", writes)
	}
	client.put(objectPath, "changed in the image")
	if got := readAsNFS(t, fsys, objectPath); got != "changed in the image" {
		t.Fatalf("the file should follow the image after its size was set, read %q", got)
	}
}

func TestNFSASizeSetAloneCompilesTheFile(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	truncateAsNFS(t, fsys, objectPath, 3)
	if writes := client.writes(); len(writes) != 1 || writes[0] != objectPath+"=old" {
		t.Fatalf("no write follows a size set alone: it should compile the file cut, got %v", writes)
	}

	refusing := newNFSFileSystem(&echoRefusingClient{newMemoryClient(map[string]string{objectPath: "old source"})})
	file, err := refusing.OpenFile(objectPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := file.Truncate(3); err == nil {
		t.Fatalf("a refused size should fail")
	}
	if got := readAsNFS(t, refusing, objectPath); got != "old source" {
		t.Fatalf("a refused size should leave the image's version, read %q", got)
	}
}

func TestNFSACreateOrARefusedSaveGivesTheNameBackFromASillyRename(t *testing.T) {
	other := "/tonel/Kernel/Other.class.st"
	first := "/tonel/Kernel/.nfs0000000000000001"
	second := "/tonel/Kernel/.nfs0000000000000002"
	takeBack := map[string]func(*testing.T, *nfsFileSystem){
		"a create": func(t *testing.T, fsys *nfsFileSystem) {
			if _, err := fsys.Create(objectPath); err != nil {
				t.Fatalf("create: %v", err)
			}
			if _, err := writeAsNFS(t, fsys, objectPath, "saved source", 0); err != nil {
				t.Fatalf("write: %v", err)
			}
		},
		"a refused rename": func(t *testing.T, fsys *nfsFileSystem) {
			truncateAsNFS(t, fsys, other, 0)
			_, _ = writeAsNFS(t, fsys, other, strings.Repeat("syntax error ", pageSize)[:pageSize], 0)
			if err := fsys.Rename(other, objectPath); err == nil {
				t.Fatalf("the rename should be refused")
			}
		},
	}
	for what, take := range takeBack {
		client := newMemoryClient(map[string]string{objectPath: "object source", other: "other"})
		fsys := newNFSFileSystem(client)
		if err := fsys.Rename(objectPath, first); err != nil {
			t.Fatalf("silly rename: %v", err)
		}
		take(t, fsys)
		if err := fsys.Rename(objectPath, second); err != nil {
			t.Fatalf("silly rename again: %v", err)
		}
		if err := fsys.Remove(first); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if !client.has(objectPath) {
			t.Fatalf("after %s took the name back, closing the first .nfs file removed the class still open as the second", what)
		}
	}
}

func TestNFSAChangeOvertakenByARenameIsStale(t *testing.T) {
	client := newGatedClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	handler := newNFSHandler(fsys)
	temp := "/tonel/Kernel/.Object.class.st.swp"
	if _, err := fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := writeNFS(fsys, temp, "new source", 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, components, _ := handler.FromHandle(handler.ToHandle(fsys, []string{"tonel", "Kernel", "Object.class.st"}))
	className := path.Join(components...)

	started, open := client.holdNext("write")
	defer open()
	renaming := make(chan error, 1)
	go func() { renaming <- fsys.Rename(temp, objectPath) }()
	waitHeld(t, started)
	removing := make(chan error, 1)
	go func() { removing <- fsys.Remove(className) }()
	time.Sleep(100 * time.Millisecond)
	open()
	if err := within(t, renaming, "rename"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := within(t, removing, "remove"); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("a remove queued behind a rename save, through the replaced file's handle, should be stale, got %v", err)
	}
	if !client.has(objectPath) {
		t.Fatalf("the remove acted on what the rename put at the old path")
	}
}

func TestNFSCommitKeepsCallsOffItsFile(t *testing.T) {
	client := newGatedClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	truncateAsNFS(t, fsys, objectPath, 0)
	_, _ = writeAsNFS(t, fsys, objectPath, strings.Repeat("a", pageSize), 0)
	started, open := client.holdNext("write")
	defer open()
	committing := make(chan error, 1)
	go func() { committing <- fsys.Commit(objectPath) }()
	waitHeld(t, started)
	looked := make(chan error, 1)
	go func() {
		_, err := fsys.Lstat(objectPath)
		looked <- err
	}()
	select {
	case <-looked:
		t.Fatalf("a stat of a file whose COMMIT compiles it should wait")
	case <-time.After(100 * time.Millisecond):
	}
	open()
	_ = within(t, committing, "commit")
	_ = within(t, looked, "stat")
}

func TestNFSCreateMkdirAndRemoveLeaveTheNameAFreshId(t *testing.T) {
	other := "/tonel/Kernel/Other.class.st"
	client := newMemoryClient(map[string]string{objectPath: "source", other: "other", "/tonel/Kernel/Kept.class.st": "kept"})
	fsys := newNFSFileSystem(client)
	handler := newNFSHandler(fsys)

	handle := handler.ToHandle(fsys, []string{"tonel", "Kernel", "Other.class.st"})
	if err := fsys.Remove(other); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, _, err := handler.FromHandle(handle); err == nil {
		t.Fatalf("a removed file's handle should be stale")
	}

	old := fsys.ids.idFor(objectPath)
	_ = client.Delete(context.Background(), objectPath) // the image removes it on its own
	if _, err := fsys.Create(objectPath); err != nil {
		t.Fatalf("create: %v", err)
	}
	if fsys.ids.idFor(objectPath) == old {
		t.Fatalf("a created file should get a fresh id")
	}

	old = fsys.ids.idFor("/tonel/Fresh")
	if err := fsys.MkdirAll("/tonel/Fresh", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if fsys.ids.idFor("/tonel/Fresh") == old {
		t.Fatalf("a made directory should get a fresh id")
	}
}

func TestNFSATruncateFollowsItsFileToAnEditorBackup(t *testing.T) {
	fsys := newNFSFileSystem(newMemoryClient(map[string]string{objectPath: "old source"}))
	backup := "/tonel/Kernel/Object.class.st~"
	file, err := fsys.OpenFile(objectPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := fsys.Rename(objectPath, backup); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := file.Truncate(3); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if contents, ok := fsys.overlay.Read(backup); !ok || string(contents) != "old" {
		t.Fatalf("the truncate should reach the backup, has %q", contents)
	}
	if err := file.Truncate(maxFileSize + 1); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("a size past the largest file should be refused, got %v", err)
	}
}

func TestNFSReadsDropWhatAChangeMadeStale(t *testing.T) {
	temp := "/tonel/Kernel/.Object.class.st.swp"
	saves := map[string]func(*testing.T, *nfsFileSystem){
		"a save": func(t *testing.T, fsys *nfsFileSystem) {
			truncateAsNFS(t, fsys, objectPath, 0)
			if committed, err := writeAsNFS(t, fsys, objectPath, "new source", 0); !committed || err != nil {
				t.Fatalf("save: %v %v", committed, err)
			}
		},
		"a rename save": func(t *testing.T, fsys *nfsFileSystem) {
			if _, err := fsys.Create(temp); err != nil {
				t.Fatalf("create: %v", err)
			}
			_, _ = writeAsNFS(t, fsys, temp, "new source", 0)
			if err := fsys.Rename(temp, objectPath); err != nil {
				t.Fatalf("rename: %v", err)
			}
		},
	}
	for what, save := range saves {
		fsys := newNFSFileSystem(newMemoryClient(map[string]string{objectPath: "old source"}))
		before := modifiedAt(t, fsys, objectPath)
		if got := readAsNFS(t, fsys, objectPath); got != "old source" {
			t.Fatalf("read %q", got)
		}
		save(t, fsys)
		if got := readAsNFS(t, fsys, objectPath); got != "new source" {
			t.Fatalf("a read after %s of the same size answered %q", what, got)
		}
		if !modifiedAt(t, fsys, objectPath).After(before) {
			t.Fatalf("%s should move the time", what)
		}
	}

	// The image goes back to a size it had, within the second a read serves.
	client := newMemoryClient(map[string]string{objectPath: "version one"})
	fsys := newNFSFileSystem(client)
	modifiedAt(t, fsys, objectPath)
	readAsNFS(t, fsys, objectPath)
	client.put(objectPath, "version two, longer")
	modifiedAt(t, fsys, objectPath)
	client.put(objectPath, "version 3!!")
	modifiedAt(t, fsys, objectPath)
	if got := readAsNFS(t, fsys, objectPath); got != "version 3!!" {
		t.Fatalf("a read after the image changed twice should be fresh, read %q", got)
	}

	// Held writes read as written, however often they change.
	fsys = newNFSFileSystem(newMemoryClient(map[string]string{objectPath: "old source"}))
	truncateAsNFS(t, fsys, objectPath, 0)
	for _, page := range []string{"a", "b"} {
		_, _ = writeAsNFS(t, fsys, objectPath, strings.Repeat(page, pageSize), 0)
		if got := readAsNFS(t, fsys, objectPath); got[:1] != page {
			t.Fatalf("held writes should read as written, read %q", got[:8])
		}
	}
}

func TestNFSARefusedSaveOntoABackedUpClassShowsItAgain(t *testing.T) {
	// JetBrains' safe write: the class goes to a backup name, then a temp file
	// is renamed over it.
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	temp := "/tonel/Kernel/Object.class.st___jb_tmp___"
	if _, err := fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := writeNFS(fsys, temp, "syntax error", 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := fsys.Rename(objectPath, "/tonel/Kernel/Object.class.st___jb_old___"); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := fsys.Rename(temp, objectPath); err == nil {
		t.Fatalf("the image should refuse the save")
	}
	if got := readAsNFS(t, fsys, objectPath); got != "old source" {
		t.Fatalf("the class the image still has should show again, read %q", got)
	}
	listed, err := fsys.ReadDir("/tonel/Kernel")
	if err != nil || !containsInfoNamed(listed, "Object.class.st") {
		t.Fatalf("the class should be listed again, got %v %v", namesOf(listed), err)
	}
}

// refusingChangesClient refuses every change but a compile, as an image does
// in a package no repository owns.
type refusingChangesClient struct {
	*memoryClient
}

func (c *refusingChangesClient) Delete(context.Context, string) error {
	return &protocol.Error{StatusCode: http.StatusBadRequest, Message: "Package is not owned by a repository"}
}

func (c *refusingChangesClient) Rename(context.Context, string, string) error {
	return &protocol.Error{StatusCode: http.StatusBadRequest, Message: "Package is not owned by a repository"}
}

func (c *refusingChangesClient) Mkdir(context.Context, string) error {
	return &protocol.Error{StatusCode: http.StatusBadRequest, Message: "Package is not owned by a repository"}
}

func (c *refusingChangesClient) Rmdir(context.Context, string) error {
	return &protocol.Error{StatusCode: http.StatusBadRequest, Message: "Package is not owned by a repository"}
}

func TestNFSEveryRefusalIsRecorded(t *testing.T) {
	fsys := newNFSFileSystem(&refusingChangesClient{newMemoryClient(map[string]string{objectPath: "source"})})
	changes := map[string]func() error{
		"delete": func() error { return fsys.Remove(objectPath) },
		"rename": func() error { return fsys.Rename(objectPath, "/tonel/Kernel/Renamed.class.st") },
		"mkdir":  func() error { return fsys.MkdirAll("/tonel/Added", 0o755) },
		"rmdir":  func() error { return fsys.Remove("/tonel/Kernel") },
	}
	for operation, change := range changes {
		if err := change(); err == nil {
			t.Fatalf("the image should refuse the %s", operation)
		}
		latest := readAsNFS(t, fsys, "/errors/"+latestErrorName)
		if !strings.Contains(latest, "Operation: "+operation) || !strings.Contains(latest, "not owned by a repository") {
			t.Fatalf("latest.txt should tell the refused %s, read %q", operation, latest)
		}
		listed, err := fsys.ReadDir("/errors")
		if err != nil {
			t.Fatalf("readdir: %v", err)
		}
		named := false
		for _, name := range namesOf(listed) {
			named = named || strings.HasSuffix(name, "-"+operation+".txt")
		}
		if !named {
			t.Fatalf("/errors should name the refused %s, lists %v", operation, namesOf(listed))
		}
	}
}

func TestNohupKeepsTheDaemonPastItsTerminal(t *testing.T) {
	hangUp := os.Signal(syscall.SIGHUP)
	if slices.Contains(stopSignalsUnless(func(each os.Signal) bool { return each == hangUp }), hangUp) {
		t.Fatalf("an ignored SIGHUP should not stop the daemon")
	}
	if !slices.Contains(stopSignalsUnless(func(os.Signal) bool { return false }), hangUp) {
		t.Fatalf("SIGHUP should stop a daemon that does not ignore it")
	}
}
