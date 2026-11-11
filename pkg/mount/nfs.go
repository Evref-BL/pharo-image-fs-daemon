package mount

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"io"
	"io/fs"
	"math"
	"net"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
	billy "github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
	nfsfile "github.com/willscott/go-nfs/file"
)

const (
	// readFreshness is how long one read of a file serves the READ calls that
	// fetch it in pieces.
	readFreshness = time.Second
	// listingFreshness is how long a directory's listing stands for its
	// modification time, which every lookup in it asks for.
	listingFreshness = time.Second
)

// nfsFileSystem adapts the projection to NFSv3, as go-nfs serves it.
//
// NFS has no open or close, and a client does not tell the writer that a
// COMMIT failed; it does tell it that a write failed. So a projected Tonel
// file's writes are held until they make a whole file, and the write that
// completes it compiles it: a refused compile fails that write, and the
// writer's close(2). Editors' and macOS's own files live in the overlay.
type nfsFileSystem struct {
	*projection

	uid     uint32
	gid     uint32
	started time.Time

	mu       sync.Mutex
	pending  map[string]*heldFile
	changed  map[string]time.Time
	sizes    map[string]uint64
	listings map[string]listing
	reads    map[string]cachedRead
	last     time.Time
}

type listing struct {
	signature uint64
	changed   time.Time
	checked   time.Time
}

type cachedRead struct {
	contents []byte
	read     time.Time
}

func newNFSFileSystem(client protocol.Client) *nfsFileSystem {
	return &nfsFileSystem{
		projection: newProjection(client),
		uid:        uint32(os.Getuid()),
		gid:        uint32(os.Getgid()),
		started:    time.Now(),
		pending:    map[string]*heldFile{},
		changed:    map[string]time.Time{},
		sizes:      map[string]uint64{},
		listings:   map[string]listing{},
		reads:      map[string]cachedRead{},
	}
}

// projectionPathFor answers the projection path of a go-nfs file name, which
// is relative and empty for the root.
func projectionPathFor(name string) string {
	return path.Clean("/" + name)
}

func pathError(op string, projectionPath string, errno syscall.Errno) error {
	return &os.PathError{Op: op, Path: projectionPath, Err: errno}
}

// billy.Basic

func (fsys *nfsFileSystem) Create(name string) (billy.File, error) {
	projectionPath := projectionPathFor(name)
	if isIgnoredMetadataPath(projectionPath) {
		if !isWritableProjectionPath(projectionPath) {
			return nil, pathError("create", projectionPath, syscall.EROFS)
		}
		fsys.overlay.Create(projectionPath, nil)
		fsys.namespaceChanged(projectionPath)
		return fsys.newFile(name, projectionPath, overlayFile), nil
	}

	if _, errno := fsys.createFile(projectionPath); errno != 0 {
		return nil, pathError("create", projectionPath, errno)
	}
	kind := overlayFile
	if isProjectedTonelFilePath(projectionPath) {
		fsys.overlay.Delete(projectionPath)
		fsys.mu.Lock()
		fsys.pending[projectionPath] = &heldFile{}
		fsys.mu.Unlock()
		kind = pendingFile
	}
	fsys.namespaceChanged(projectionPath)
	return fsys.newFile(name, projectionPath, kind), nil
}

func (fsys *nfsFileSystem) Open(name string) (billy.File, error) {
	return fsys.OpenFile(name, os.O_RDONLY, 0)
}

func (fsys *nfsFileSystem) OpenFile(name string, flag int, _ os.FileMode) (billy.File, error) {
	projectionPath := projectionPathFor(name)
	entry, errno := fsys.entryFor(projectionPath)
	if errno != 0 {
		return nil, pathError("open", projectionPath, errno)
	}
	if entry.Kind == protocol.Directory {
		return nil, pathError("open", projectionPath, syscall.EISDIR)
	}

	if flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		contents, err := fsys.contentsToRead(projectionPath)
		if err != nil {
			return nil, pathError("open", projectionPath, errnoFor(err))
		}
		file := fsys.newFile(name, projectionPath, snapshotFile)
		file.contents = contents
		return file, nil
	}

	if !entry.Writable {
		return nil, pathError("open", projectionPath, syscall.EROFS)
	}
	if _, ok := fsys.overlay.Stat(projectionPath); ok {
		return fsys.newFile(name, projectionPath, overlayFile), nil
	}
	if isProjectedTonelFilePath(projectionPath) {
		return fsys.newFile(name, projectionPath, pendingFile), nil
	}
	return nil, pathError("open", projectionPath, syscall.EROFS)
}

func (fsys *nfsFileSystem) Stat(name string) (os.FileInfo, error) {
	return fsys.Lstat(name)
}

func (fsys *nfsFileSystem) Rename(oldName string, newName string) error {
	oldPath := projectionPathFor(oldName)
	newPath := projectionPathFor(newName)

	if err := fsys.commitHeld(oldPath); err != nil {
		return err
	}
	fsys.mu.Lock()
	delete(fsys.pending, newPath)
	fsys.mu.Unlock()

	var errno syscall.Errno
	if isProjectedTonelFilePath(oldPath) && isSillyRenameName(path.Base(newPath)) {
		errno = fsys.sillyRename(oldPath, newPath)
	} else {
		errno = fsys.rename(oldPath, newPath)
	}
	fsys.namespaceChanged(oldPath)
	fsys.namespaceChanged(newPath)
	if errno != 0 {
		return pathError("rename", newPath, errno)
	}
	return nil
}

// sillyRename keeps a removed file readable for those who still have it
// open, as the NFS client asks by renaming it to ".nfs…": the class goes, and
// its last contents stay under the new name until the client removes it.
func (fsys *nfsFileSystem) sillyRename(oldPath string, newPath string) syscall.Errno {
	contents, err := fsys.contentsForPath(oldPath)
	if err != nil {
		return errnoFor(err)
	}
	if errno := fsys.unlink(oldPath); errno != 0 {
		return errno
	}
	fsys.overlay.Create(newPath, contents)
	return 0
}

func (fsys *nfsFileSystem) Remove(name string) error {
	projectionPath := projectionPathFor(name)
	entry, errno := fsys.entryFor(projectionPath)
	if errno != 0 {
		return pathError("remove", projectionPath, errno)
	}

	fsys.mu.Lock()
	delete(fsys.pending, projectionPath)
	fsys.mu.Unlock()
	if entry.Kind == protocol.Directory {
		errno = fsys.rmdir(projectionPath)
	} else {
		errno = fsys.unlink(projectionPath)
	}
	fsys.namespaceChanged(projectionPath)
	if errno != 0 {
		return pathError("remove", projectionPath, errno)
	}
	return nil
}

func (fsys *nfsFileSystem) Join(elem ...string) string {
	return path.Join(elem...)
}

// billy.TempFile

func (fsys *nfsFileSystem) TempFile(dir string, _ string) (billy.File, error) {
	return nil, pathError("tempfile", projectionPathFor(dir), syscall.ENOTSUP)
}

// billy.Dir

func (fsys *nfsFileSystem) ReadDir(name string) ([]os.FileInfo, error) {
	projectionPath := projectionPathFor(name)
	entries, errno := fsys.entriesIn(projectionPath)
	if errno != 0 {
		return nil, pathError("readdir", projectionPath, errno)
	}

	fsys.mu.Lock()
	for pendingPath := range fsys.pending {
		if path.Dir(pendingPath) == projectionPath && !containsEntryNamed(entries, path.Base(pendingPath)) {
			entries = append(entries, protocol.Entry{Name: path.Base(pendingPath), Kind: protocol.File, Writable: true})
		}
	}
	fsys.mu.Unlock()

	infos := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		// A listing names its entries; their times and sizes come from the
		// lookups that follow, which is all READDIR asks for.
		infos = append(infos, fsys.infoFor(joinProjectionPath(projectionPath, entry.Name), entry, fsys.started))
	}
	return infos, nil
}

func (fsys *nfsFileSystem) MkdirAll(name string, _ os.FileMode) error {
	projectionPath := projectionPathFor(name)
	errno := fsys.mkdir(projectionPath)
	fsys.namespaceChanged(projectionPath)
	if errno != 0 {
		return pathError("mkdir", projectionPath, errno)
	}
	return nil
}

// billy.Symlink

func (fsys *nfsFileSystem) Lstat(name string) (os.FileInfo, error) {
	projectionPath := projectionPathFor(name)
	entry, errno := fsys.entryFor(projectionPath)
	if errno != 0 {
		return nil, pathError("lstat", projectionPath, errno)
	}
	return fsys.infoFor(projectionPath, entry, fsys.modificationTime(projectionPath, entry)), nil
}

func (fsys *nfsFileSystem) Symlink(_ string, link string) error {
	return pathError("symlink", projectionPathFor(link), syscall.EROFS)
}

func (fsys *nfsFileSystem) Readlink(name string) (string, error) {
	projectionPath := projectionPathFor(name)
	target, errno := fsys.readlink(projectionPath)
	if errno != 0 {
		return "", pathError("readlink", projectionPath, errno)
	}
	return target, nil
}

// billy.Chroot

func (fsys *nfsFileSystem) Chroot(name string) (billy.Filesystem, error) {
	return nil, pathError("chroot", projectionPathFor(name), syscall.ENOTSUP)
}

func (fsys *nfsFileSystem) Root() string {
	return "/"
}

func (fsys *nfsFileSystem) Capabilities() billy.Capability {
	return billy.WriteCapability | billy.ReadCapability | billy.ReadAndWriteCapability | billy.SeekCapability | billy.TruncateCapability
}

// billy.Change: modes, owners and times belong to the image, so changing
// them succeeds and changes nothing.

func (fsys *nfsFileSystem) Chmod(string, os.FileMode) error            { return nil }
func (fsys *nfsFileSystem) Lchown(string, int, int) error              { return nil }
func (fsys *nfsFileSystem) Chown(string, int, int) error               { return nil }
func (fsys *nfsFileSystem) Chtimes(string, time.Time, time.Time) error { return nil }

// Written commits the writes held for the file at name once they make a
// whole file; see nfs.Committer. A client writes whole pages, but for the
// last one of a file: so held writes that cover the file and end inside a
// page are a whole file, and one whose size is a multiple of the page size
// waits for the COMMIT.
func (fsys *nfsFileSystem) Written(name string, _ bool) (bool, error) {
	projectionPath := projectionPathFor(name)
	fsys.mu.Lock()
	held, ok := fsys.pending[projectionPath]
	whole := ok && held.isWhole()
	fsys.mu.Unlock()
	if !ok {
		return true, nil
	}
	if !whole {
		return false, nil
	}
	if err := fsys.commitHeld(projectionPath); err != nil {
		return false, err
	}
	return true, nil
}

// Commit commits the writes still held for the file at name; see
// nfs.Committer. The client would not tell the writer of a refusal: it is
// recorded in /errors, and the image's version stands.
func (fsys *nfsFileSystem) Commit(name string) error {
	_ = fsys.commitHeld(projectionPathFor(name))
	return nil
}

// commitHeld compiles the writes held for the file at projectionPath, and
// drops them either way: the file is the image's from then on.
func (fsys *nfsFileSystem) commitHeld(projectionPath string) error {
	fsys.mu.Lock()
	held, ok := fsys.pending[projectionPath]
	delete(fsys.pending, projectionPath)
	fsys.mu.Unlock()
	if !ok {
		return nil
	}

	err := fsys.writeProjection(context.Background(), projectionPath, held.contents)
	fsys.contentsChanged(projectionPath)
	if err != nil {
		return pathError("write", projectionPath, errnoFor(err))
	}
	return nil
}

// entryFor answers the entry at projectionPath, as the writes held for it
// left it.
func (fsys *nfsFileSystem) entryFor(projectionPath string) (protocol.Entry, syscall.Errno) {
	fsys.mu.Lock()
	held, ok := fsys.pending[projectionPath]
	fsys.mu.Unlock()
	if ok {
		return protocol.Entry{Name: path.Base(projectionPath), Kind: protocol.File, Size: uint64(len(held.contents)), Writable: true}, 0
	}
	return fsys.entryForPath(projectionPath)
}

func (fsys *nfsFileSystem) contentsToRead(projectionPath string) ([]byte, error) {
	fsys.mu.Lock()
	if held, ok := fsys.pending[projectionPath]; ok {
		fsys.mu.Unlock()
		return append([]byte(nil), held.contents...), nil
	}
	if cached, ok := fsys.reads[projectionPath]; ok && time.Since(cached.read) < readFreshness {
		fsys.mu.Unlock()
		return cached.contents, nil
	}
	fsys.mu.Unlock()

	contents, err := fsys.contentsForPath(projectionPath)
	if err != nil {
		return nil, err
	}
	fsys.mu.Lock()
	fsys.reads[projectionPath] = cachedRead{contents: contents, read: time.Now()}
	fsys.mu.Unlock()
	return contents, nil
}

func (fsys *nfsFileSystem) infoFor(projectionPath string, entry protocol.Entry, modified time.Time) os.FileInfo {
	mode := fs.FileMode(permissionsFor(entry))
	links := uint32(1)
	switch entry.Kind {
	case protocol.Directory:
		mode |= fs.ModeDir
		links = 2
	case protocol.Symlink:
		mode |= fs.ModeSymlink
	}

	return &nfsFileInfo{
		name:     path.Base(projectionPath),
		size:     int64(entry.Size),
		mode:     mode,
		modified: modified,
		sys: &nfsfile.FileInfo{
			Nlink:  links,
			UID:    fsys.uid,
			GID:    fsys.gid,
			Fileid: stableInodeFor(projectionPath),
		},
	}
}

// modificationTime answers when the entry at projectionPath last changed, as
// far as the daemon can tell: the NFS client keeps its cached pages and
// listings while this holds still.
func (fsys *nfsFileSystem) modificationTime(projectionPath string, entry protocol.Entry) time.Time {
	if entry.Kind == protocol.Directory {
		return fsys.directoryTime(projectionPath)
	}

	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if _, ok := fsys.pending[projectionPath]; !ok {
		if previous, seen := fsys.sizes[projectionPath]; seen && previous != entry.Size {
			fsys.changed[projectionPath] = fsys.nowLocked()
		}
		fsys.sizes[projectionPath] = entry.Size
	}

	modified := fsys.started
	if changed, ok := fsys.changed[projectionPath]; ok && changed.After(modified) {
		modified = changed
	}
	if entry.Modified > 0 {
		if reported := time.UnixMilli(entry.Modified); reported.After(modified) {
			modified = reported
		}
	}
	return modified
}

// directoryTime answers when the listing of the directory at projectionPath
// last changed, listing it again once the last listing is a second old.
func (fsys *nfsFileSystem) directoryTime(projectionPath string) time.Time {
	fsys.mu.Lock()
	known, ok := fsys.listings[projectionPath]
	fsys.mu.Unlock()
	if ok && time.Since(known.checked) < listingFreshness {
		return known.changed
	}

	entries, errno := fsys.entriesIn(projectionPath)
	if errno != 0 {
		if ok {
			return known.changed
		}
		return fsys.started
	}
	signature := listingSignature(entries)

	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	known, ok = fsys.listings[projectionPath]
	changed := fsys.started
	if ok {
		changed = known.changed
		if known.signature != signature {
			changed = fsys.nowLocked()
		}
	}
	fsys.listings[projectionPath] = listing{signature: signature, changed: changed, checked: time.Now()}
	return changed
}

// namespaceChanged records that an entry was added, removed or renamed at
// projectionPath, through this mount.
func (fsys *nfsFileSystem) namespaceChanged(projectionPath string) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	now := fsys.nowLocked()
	parent := path.Dir(projectionPath)
	fsys.listings[parent] = listing{signature: 0, changed: now}
	fsys.changed[projectionPath] = now
	delete(fsys.reads, projectionPath)
	delete(fsys.sizes, projectionPath)
}

// contentsChanged records that the contents at projectionPath changed
// through this mount.
func (fsys *nfsFileSystem) contentsChanged(projectionPath string) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	fsys.changed[projectionPath] = fsys.nowLocked()
	delete(fsys.reads, projectionPath)
	delete(fsys.sizes, projectionPath)
}

// nowLocked answers the current time, after every time it answered before,
// so that two changes in a row never share a modification time.
func (fsys *nfsFileSystem) nowLocked() time.Time {
	now := time.Now()
	if !now.After(fsys.last) {
		now = fsys.last.Add(time.Microsecond)
	}
	fsys.last = now
	return now
}

func listingSignature(entries []protocol.Entry) uint64 {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name+"\x00"+string(entry.Kind))
	}
	sort.Strings(names)
	hash := fnv.New64a()
	for _, name := range names {
		_, _ = hash.Write([]byte(name))
		_, _ = hash.Write([]byte{0})
	}
	return hash.Sum64() | 1
}

func containsEntryNamed(entries []protocol.Entry, name string) bool {
	for _, entry := range entries {
		if entry.Name == name {
			return true
		}
	}
	return false
}

func isSillyRenameName(name string) bool {
	return strings.HasPrefix(name, ".nfs")
}

// writeAt writes data at offset into the file at projectionPath, held in
// pending or in the overlay.
func (fsys *nfsFileSystem) writeAt(kind nfsFileKind, projectionPath string, data []byte, offset int64) error {
	if kind == overlayFile {
		contents, ok := fsys.overlay.Read(projectionPath)
		if !ok {
			return pathError("write", projectionPath, syscall.ENOENT)
		}
		_ = fsys.overlay.Write(context.Background(), projectionPath, patched(contents, data, offset))
		return nil
	}

	if err := fsys.hold(projectionPath); err != nil {
		return err
	}
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if held, ok := fsys.pending[projectionPath]; ok {
		held.write(data, offset)
	}
	return nil
}

func (fsys *nfsFileSystem) truncate(kind nfsFileKind, projectionPath string, size int64) error {
	if size < 0 {
		return pathError("truncate", projectionPath, syscall.EINVAL)
	}
	if kind == overlayFile {
		contents, ok := fsys.overlay.Read(projectionPath)
		if !ok {
			return pathError("truncate", projectionPath, syscall.ENOENT)
		}
		_ = fsys.overlay.Write(context.Background(), projectionPath, resizedContents(contents, int(size)))
		return nil
	}

	fsys.mu.Lock()
	_, held := fsys.pending[projectionPath]
	if !held && size == 0 {
		// A save starts by emptying the file: what the image has is not needed.
		fsys.pending[projectionPath] = &heldFile{}
		fsys.changed[projectionPath] = fsys.nowLocked()
	}
	fsys.mu.Unlock()
	if err := fsys.hold(projectionPath); err != nil {
		return err
	}
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if held, ok := fsys.pending[projectionPath]; ok {
		held.truncate(int(size))
	}
	return nil
}

// hold makes sure writes to projectionPath are held, starting from what the
// image has when none are yet.
func (fsys *nfsFileSystem) hold(projectionPath string) error {
	fsys.mu.Lock()
	_, ok := fsys.pending[projectionPath]
	fsys.mu.Unlock()
	if ok {
		return nil
	}

	contents, err := fsys.contentsForPath(projectionPath)
	if err != nil {
		return pathError("write", projectionPath, errnoFor(err))
	}
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if _, ok := fsys.pending[projectionPath]; !ok {
		fsys.pending[projectionPath] = &heldFile{
			contents: append([]byte(nil), contents...),
			written:  []span{{0, len(contents)}},
		}
		fsys.changed[projectionPath] = fsys.nowLocked()
	}
	return nil
}

// pageSize is the unit a client writes a file in, but for its end.
const pageSize = 4096

// heldFile is a projected file's contents as written so far, and which of
// its bytes were written rather than left by a truncation.
type heldFile struct {
	contents []byte
	written  []span
}

type span struct {
	start int
	end   int
}

func (h *heldFile) write(data []byte, offset int64) {
	h.contents = patched(h.contents, data, offset)
	h.written = withSpan(h.written, span{int(offset), int(offset) + len(data)})
}

func (h *heldFile) truncate(size int) {
	h.contents = resizedContents(append([]byte(nil), h.contents...), size)
	kept := h.written[:0]
	for _, each := range h.written {
		if each.start >= size {
			continue
		}
		kept = append(kept, span{each.start, min(each.end, size)})
	}
	h.written = kept
}

// isWhole answers whether every byte was written, and the last one is not
// at a page's end, which a client's last write to a file never is.
func (h *heldFile) isWhole() bool {
	size := len(h.contents)
	if size%pageSize == 0 {
		return false
	}
	return len(h.written) == 1 && h.written[0].start == 0 && h.written[0].end == size
}

// withSpan answers spans with added merged in, in order.
func withSpan(spans []span, added span) []span {
	if added.end <= added.start {
		return spans
	}
	merged := make([]span, 0, len(spans)+1)
	inserted := false
	for _, each := range spans {
		switch {
		case each.end < added.start:
			merged = append(merged, each)
		case added.end < each.start:
			if !inserted {
				merged = append(merged, added)
				inserted = true
			}
			merged = append(merged, each)
		default:
			added = span{min(each.start, added.start), max(each.end, added.end)}
		}
	}
	if !inserted {
		merged = append(merged, added)
	}
	return merged
}

func patched(contents []byte, data []byte, offset int64) []byte {
	end := int(offset) + len(data)
	if end > len(contents) {
		contents = resizedContents(append([]byte(nil), contents...), end)
	} else {
		contents = append([]byte(nil), contents...)
	}
	copy(contents[offset:], data)
	return contents
}

type nfsFileKind int

const (
	snapshotFile nfsFileKind = iota
	pendingFile
	overlayFile
)

// nfsFile is what go-nfs opens for one call: a snapshot to read, or a view
// on the writes held for a file.
type nfsFile struct {
	fsys     *nfsFileSystem
	name     string
	path     string
	kind     nfsFileKind
	contents []byte
	offset   int64
}

func (fsys *nfsFileSystem) newFile(name string, projectionPath string, kind nfsFileKind) *nfsFile {
	return &nfsFile{fsys: fsys, name: name, path: projectionPath, kind: kind}
}

func (f *nfsFile) Name() string {
	return f.name
}

func (f *nfsFile) Write(data []byte) (int, error) {
	if f.kind == snapshotFile {
		return 0, pathError("write", f.path, syscall.EBADF)
	}
	if err := f.fsys.writeAt(f.kind, f.path, data, f.offset); err != nil {
		return 0, err
	}
	f.offset += int64(len(data))
	return len(data), nil
}

func (f *nfsFile) Read(data []byte) (int, error) {
	read, err := f.ReadAt(data, f.offset)
	f.offset += int64(read)
	return read, err
}

func (f *nfsFile) ReadAt(data []byte, offset int64) (int, error) {
	contents := f.contents
	if f.kind != snapshotFile {
		var err error
		if contents, err = f.fsys.contentsToRead(f.path); err != nil {
			return 0, err
		}
	}
	if offset >= int64(len(contents)) {
		return 0, io.EOF
	}
	read := copy(data, contents[offset:])
	if read < len(data) {
		return read, io.EOF
	}
	return read, nil
}

func (f *nfsFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	default:
		return 0, pathError("seek", f.path, syscall.EINVAL)
	}
	return f.offset, nil
}

func (f *nfsFile) Close() error {
	return nil
}

func (f *nfsFile) Lock() error {
	return nil
}

func (f *nfsFile) Unlock() error {
	return nil
}

func (f *nfsFile) Truncate(size int64) error {
	if f.kind == snapshotFile {
		return pathError("truncate", f.path, syscall.EBADF)
	}
	return f.fsys.truncate(f.kind, f.path, size)
}

type nfsFileInfo struct {
	name     string
	size     int64
	mode     fs.FileMode
	modified time.Time
	sys      *nfsfile.FileInfo
}

func (info *nfsFileInfo) Name() string       { return info.name }
func (info *nfsFileInfo) Size() int64        { return info.size }
func (info *nfsFileInfo) Mode() fs.FileMode  { return info.mode }
func (info *nfsFileInfo) ModTime() time.Time { return info.modified }
func (info *nfsFileInfo) IsDir() bool        { return info.mode.IsDir() }
func (info *nfsFileInfo) Sys() any           { return info.sys }

// nfsHandler answers go-nfs for the one export: it mounts the projection,
// and names each path by a number that holds for the daemon's life.
type nfsHandler struct {
	fsys *nfsFileSystem

	mu       sync.Mutex
	ids      map[string]uint64
	paths    map[uint64][]string
	listings map[string]cachedListing
	nextID   uint64
}

type cachedListing struct {
	verifier uint64
	contents []fs.FileInfo
}

func newNFSHandler(fsys *nfsFileSystem) *nfsHandler {
	return &nfsHandler{
		fsys:     fsys,
		ids:      map[string]uint64{},
		paths:    map[uint64][]string{},
		listings: map[string]cachedListing{},
		nextID:   1,
	}
}

func (h *nfsHandler) Mount(context.Context, net.Conn, nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	return nfs.MountStatusOk, h.fsys, []nfs.AuthFlavor{nfs.AuthFlavorNull}
}

func (h *nfsHandler) Change(billy.Filesystem) billy.Change {
	return h.fsys
}

func (h *nfsHandler) FSStat(_ context.Context, _ billy.Filesystem, stat *nfs.FSStat) error {
	stat.TotalSize = 1 << 40
	stat.FreeSize = 1 << 39
	stat.AvailableSize = 1 << 39
	stat.TotalFiles = 1 << 20
	stat.FreeFiles = 1 << 19
	stat.AvailableFiles = 1 << 19
	return nil
}

func (h *nfsHandler) ToHandle(_ billy.Filesystem, components []string) []byte {
	key := strings.Join(components, "/")
	h.mu.Lock()
	defer h.mu.Unlock()

	id, ok := h.ids[key]
	if !ok {
		id = h.nextID
		h.nextID++
		h.ids[key] = id
		h.paths[id] = append([]string(nil), components...)
	}
	handle := make([]byte, 8)
	binary.BigEndian.PutUint64(handle, id)
	return handle
}

func (h *nfsHandler) FromHandle(handle []byte) (billy.Filesystem, []string, error) {
	if len(handle) != 8 {
		return nil, nil, errStaleHandle
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	components, ok := h.paths[binary.BigEndian.Uint64(handle)]
	if !ok {
		return nil, nil, errStaleHandle
	}
	return h.fsys, append([]string(nil), components...), nil
}

func (h *nfsHandler) InvalidateHandle(billy.Filesystem, []byte) error {
	return nil
}

func (h *nfsHandler) HandleLimit() int {
	return math.MaxInt32
}

// VerifierFor and DataForVerifier keep the listing a READDIR started, so the
// calls that page through it see the same one.
func (h *nfsHandler) VerifierFor(directoryPath string, contents []fs.FileInfo) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(directoryPath))
	for _, info := range contents {
		_, _ = hash.Write([]byte(info.Name()))
		_, _ = hash.Write([]byte{0})
	}
	verifier := hash.Sum64() | 1

	h.mu.Lock()
	defer h.mu.Unlock()
	h.listings[directoryPath] = cachedListing{verifier: verifier, contents: contents}
	return verifier
}

func (h *nfsHandler) DataForVerifier(directoryPath string, verifier uint64) []fs.FileInfo {
	h.mu.Lock()
	defer h.mu.Unlock()

	cached, ok := h.listings[directoryPath]
	if !ok || cached.verifier != verifier {
		return nil
	}
	return cached.contents
}

var errStaleHandle = errors.New("stale file handle")
