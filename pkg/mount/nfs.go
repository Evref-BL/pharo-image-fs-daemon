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
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
	billy "github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
	nfsfile "github.com/willscott/go-nfs/file"
)

// nfsFileSystem adapts the projection to NFSv3, as go-nfs serves it, on the
// rules every mount keeps (see fileSystem).
//
// NFS has no open or close, and a client does not tell the writer that a
// COMMIT failed; it does tell it that a write failed. So a projected Tonel
// file's writes are held until they make a whole file, and the write that
// completes it compiles it. The mount is synchronous (see
// mountNFSArguments), so a refused compile fails the write(2) that completes
// the file: a writer that checks nothing else sees it. A file whose size is
// a multiple of the page size compiles at the COMMIT, and its refusal
// reaches only /errors. The calls a refused save still makes, the client
// sending the file again and the writer setting its size, answer as that
// save and compile nothing (see refusalLocked). Editors' and macOS's own
// files live in the overlay.
//
// The names go-nfs passes carry the id of the file their handle named (see
// componentsFor), so each call acts on that file wherever it is by then.
type nfsFileSystem struct {
	*fileSystem

	// silly maps each ".nfs…" file the client renamed an open class file to,
	// to that file's path; the class goes when the ".nfs…" file does. Guarded
	// by changing.
	silly map[string]string
}

func newNFSFileSystem(client protocol.Client) *nfsFileSystem {
	fsys := &nfsFileSystem{fileSystem: newFileSystem(client), silly: map[string]string{}}
	// The client keeps a directory's listing while its time holds still.
	fsys.listsDirectoriesOnStat = true
	// The client sends a refused file again as it drops it from its cache.
	fsys.answersRefusedSaves = true
	return fsys
}

// projectionPathFor answers the projection path of a go-nfs file name, which
// is relative and empty for the root.
func projectionPathFor(name string) string {
	return path.Clean("/" + name)
}

// nameError is the error of op on a name that resolve refused.
func nameError(op string, name string, errno syscall.Errno) error {
	return pathError(op, strings.ReplaceAll(name, idMark, "#"), errno)
}

// recovered answers a call whose handler panicked, which the daemon
// survives: the call fails, and the panic is in the log.
func (fsys *nfsFileSystem) recovered(value any) nfs.ResponseCode {
	fsys.logf("panic serving NFS: %v\n%s", value, debug.Stack())
	return nfs.ResponseCodeSystemErr
}

// beginChange starts a change: it waits for the one running, and answers the
// paths names stand for now. endChange ends it.
func (fsys *nfsFileSystem) beginChange(op string, names ...string) ([]string, error) {
	fsys.changing.Lock()
	paths := make([]string, len(names))
	for index, name := range names {
		projectionPath, errno := fsys.ids.resolve(name)
		if errno != 0 {
			fsys.changing.Unlock()
			return nil, nameError(op, name, errno)
		}
		paths[index] = projectionPath
	}
	return paths, nil
}

// observeName observes what a go-nfs name stands for now (see observe).
func (fsys *nfsFileSystem) observeName(op string, name string, read func(projectionPath string) error) error {
	resolve := func() (string, error) {
		projectionPath, errno := fsys.ids.resolve(name)
		if errno != 0 {
			return "", nameError(op, name, errno)
		}
		return projectionPath, nil
	}
	return fsys.observe(resolve, read)
}

// billy.Basic

func (fsys *nfsFileSystem) Create(name string) (billy.File, error) {
	paths, err := fsys.beginChange("create", name)
	if err != nil {
		return nil, err
	}
	defer fsys.endChange()
	projectionPath := paths[0]
	if isAppleDoublePath(projectionPath) {
		return nil, pathError("create", projectionPath, syscall.ENOTSUP)
	}
	fsys.keepOff(projectionPath)

	if isIgnoredMetadataPath(projectionPath) {
		if !isWritableProjectionPath(projectionPath) {
			return nil, pathError("create", projectionPath, syscall.EROFS)
		}
		fsys.overlay.Create(projectionPath, nil)
		fsys.ids.removed(projectionPath)
		fsys.namespaceChanged(projectionPath)
		return fsys.newFile(name, projectionPath, overlayFile), nil
	}

	fsys.noteCreateOverBackup(projectionPath)
	if _, errno := fsys.createFile(projectionPath); errno != 0 {
		return nil, pathError("create", projectionPath, errno)
	}
	fsys.forgetSilly(projectionPath)
	kind := overlayFile
	if isProjectedTonelFilePath(projectionPath) {
		fsys.overlay.Delete(projectionPath)
		fsys.startSaveLocked(projectionPath)
		kind = pendingFile
	}
	fsys.ids.removed(projectionPath)
	fsys.namespaceChanged(projectionPath)
	return fsys.newFile(name, projectionPath, kind), nil
}

func (fsys *nfsFileSystem) Open(name string) (billy.File, error) {
	return fsys.OpenFile(name, os.O_RDONLY, 0)
}

func (fsys *nfsFileSystem) OpenFile(name string, flag int, _ os.FileMode) (billy.File, error) {
	var file *nfsFile
	err := fsys.observeName("open", name, func(projectionPath string) error {
		file = nil
		entry, errno := fsys.entryFor(projectionPath)
		if errno != 0 {
			return pathError("open", projectionPath, errno)
		}
		if entry.Kind == protocol.Directory {
			return pathError("open", projectionPath, syscall.EISDIR)
		}

		if flag&(os.O_WRONLY|os.O_RDWR) == 0 {
			contents, err := fsys.contentsToRead(projectionPath, entry)
			if err != nil {
				return pathError("open", projectionPath, errnoFor(err))
			}
			file = fsys.newFile(name, projectionPath, snapshotFile)
			file.contents = contents
			return nil
		}

		if !entry.Writable {
			return pathError("open", projectionPath, syscall.EROFS)
		}
		if _, ok := fsys.overlay.Stat(projectionPath); ok {
			file = fsys.newFile(name, projectionPath, overlayFile)
			return nil
		}
		if isProjectedTonelFilePath(projectionPath) {
			file = fsys.newFile(name, projectionPath, pendingFile)
			return nil
		}
		return pathError("open", projectionPath, syscall.EROFS)
	})
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (fsys *nfsFileSystem) Stat(name string) (os.FileInfo, error) {
	return fsys.Lstat(name)
}

func (fsys *nfsFileSystem) Rename(oldName string, newName string) error {
	paths, err := fsys.beginChange("rename", oldName, newName)
	if err != nil {
		return err
	}
	defer fsys.endChange()
	oldPath, newPath := paths[0], paths[1]
	if isAppleDoublePath(newPath) {
		return pathError("rename", newPath, syscall.ENOTSUP)
	}
	fsys.keepOff(oldPath, newPath)

	if err := fsys.commitHeld(oldPath); err != nil {
		fsys.showHiddenClass(newPath)
		return err
	}

	var errno syscall.Errno
	if isProjectedTonelFilePath(oldPath) && isSillyRenameName(path.Base(newPath)) {
		errno = fsys.sillyRename(oldPath, newPath)
	} else {
		errno = fsys.rename(oldPath, newPath)
	}
	fsys.namespaceChanged(oldPath)
	fsys.namespaceChanged(newPath)
	if errno != 0 {
		fsys.showHiddenClass(newPath)
		return pathError("rename", newPath, errno)
	}
	fsys.renamedOver(oldPath, newPath)
	if original, ok := fsys.silly[oldPath]; ok {
		delete(fsys.silly, oldPath)
		fsys.silly[newPath] = original
	} else if !isSillyRenameName(path.Base(newPath)) {
		fsys.forgetSilly(newPath)
	}
	return nil
}

// sillyRename keeps a removed file readable for those who still have it
// open, as the NFS client asks by renaming it to ".nfs…": its name goes, and
// its last contents stay under the new one. The class stays in the image
// until the client removes the ".nfs…" file, at the last close, so that a
// rename or create onto the name meanwhile compiles over it rather than
// replacing it.
func (fsys *nfsFileSystem) sillyRename(oldPath string, newPath string) syscall.Errno {
	contents, err := fsys.contentsForPath(oldPath)
	if err != nil {
		return errnoFor(err)
	}
	fsys.overlay.Create(newPath, contents)
	fsys.overlay.Hide(oldPath)
	fsys.silly[newPath] = oldPath
	return 0
}

// showHiddenClass shows the class at projectionPath again when what was to
// take its name failed: an editor's backup rename, or the client's rename of
// an open file to ".nfs…", hid it, and the image still has it, as a refused
// save leaves it.
func (fsys *nfsFileSystem) showHiddenClass(projectionPath string) {
	if fsys.fileSystem.showHiddenClass(projectionPath) {
		fsys.forgetSilly(projectionPath)
	}
}

// forgetSilly drops the ".nfs…" files that stand for the class at
// projectionPath, once something else took the name: the class is no longer
// theirs to remove at their last close.
func (fsys *nfsFileSystem) forgetSilly(projectionPath string) {
	for sillyPath, original := range fsys.silly {
		if original == projectionPath {
			delete(fsys.silly, sillyPath)
		}
	}
}

func (fsys *nfsFileSystem) Remove(name string) error {
	paths, err := fsys.beginChange("remove", name)
	if err != nil {
		return err
	}
	defer fsys.endChange()
	projectionPath := paths[0]
	original, removesClass := fsys.silly[projectionPath]
	removesClass = removesClass && fsys.overlay.IsHidden(original)
	if removesClass {
		fsys.keepOff(projectionPath, original)
	} else {
		fsys.keepOff(projectionPath)
	}

	entry, errno := fsys.entryFor(projectionPath)
	if errno != 0 {
		return pathError("remove", projectionPath, errno)
	}
	if removesClass {
		// The client does not look at what its last close's REMOVE answers: a
		// refusal is recorded in /errors, and the class shows again.
		if err := fsys.client.Delete(context.Background(), original); err != nil && !protocol.NotFound(err) {
			fsys.logf("delete %s failed: %v", original, err)
			fsys.errors.RecordError("delete", original, err)
		}
		fsys.overlay.Delete(original)
		fsys.namespaceChanged(original)
	}
	delete(fsys.silly, projectionPath)

	fsys.mu.Lock()
	delete(fsys.pending, projectionPath)
	fsys.mu.Unlock()
	if entry.Kind == protocol.Directory {
		errno = fsys.rmdir(projectionPath)
	} else {
		errno = fsys.unlinkFile(projectionPath)
	}
	fsys.namespaceChanged(projectionPath)
	if errno != 0 {
		return pathError("remove", projectionPath, errno)
	}
	fsys.ids.removed(projectionPath)
	return nil
}

func (fsys *nfsFileSystem) Join(elem ...string) string {
	return path.Join(elem...)
}

// billy.TempFile

func (fsys *nfsFileSystem) TempFile(dir string, _ string) (billy.File, error) {
	return nil, nameError("tempfile", dir, syscall.ENOTSUP)
}

// billy.Dir

func (fsys *nfsFileSystem) ReadDir(name string) ([]os.FileInfo, error) {
	var infos []os.FileInfo
	err := fsys.observeName("readdir", name, func(projectionPath string) error {
		entries, errno := fsys.listingFor(projectionPath)
		if errno != 0 {
			return pathError("readdir", projectionPath, errno)
		}
		// The time the reply carries, and the client keeps the listing under,
		// is the one this listing records.
		fsys.mu.Lock()
		fsys.recordListingLocked(projectionPath, listingSignature(entries))
		fsys.mu.Unlock()

		infos = make([]os.FileInfo, 0, len(entries))
		for _, entry := range entries {
			// A listing names its entries; their times and sizes come from the
			// lookups that follow, which is all READDIR asks for.
			infos = append(infos, fsys.infoFor(joinProjectionPath(projectionPath, entry.Name), entry, fsys.started))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return infos, nil
}

func (fsys *nfsFileSystem) MkdirAll(name string, _ os.FileMode) error {
	paths, err := fsys.beginChange("mkdir", name)
	if err != nil {
		return err
	}
	defer fsys.endChange()
	projectionPath := paths[0]
	fsys.keepOff(projectionPath)

	errno := fsys.mkdir(projectionPath)
	fsys.namespaceChanged(projectionPath)
	if errno != 0 {
		return pathError("mkdir", projectionPath, errno)
	}
	fsys.ids.removed(projectionPath)
	return nil
}

// billy.Symlink

func (fsys *nfsFileSystem) Lstat(name string) (os.FileInfo, error) {
	var info os.FileInfo
	err := fsys.observeName("lstat", name, func(projectionPath string) error {
		entry, modified, errno := fsys.statAt(projectionPath)
		if errno != 0 {
			return pathError("lstat", projectionPath, errno)
		}
		info = fsys.infoFor(projectionPath, entry, modified)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (fsys *nfsFileSystem) Symlink(_ string, link string) error {
	return nameError("symlink", link, syscall.EROFS)
}

func (fsys *nfsFileSystem) Readlink(name string) (string, error) {
	var target string
	err := fsys.observeName("readlink", name, func(projectionPath string) error {
		var errno syscall.Errno
		target, errno = fsys.readlink(projectionPath)
		if errno != 0 {
			return pathError("readlink", projectionPath, errno)
		}
		return nil
	})
	return target, err
}

// billy.Chroot

func (fsys *nfsFileSystem) Chroot(name string) (billy.Filesystem, error) {
	return nil, nameError("chroot", name, syscall.ENOTSUP)
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
// whole file; see nfs.Committer. The mount is synchronous, so each write(2)
// is sent before it returns, and held writes from 0 that end inside a page
// are taken as the whole file: a writer that writes in chunks not aligned to
// pages has its first chunk compiled alone, and a file whose size is a
// multiple of the page size waits for the COMMIT. A compiled file holds what
// the image renders, which may differ from what was written.
func (fsys *nfsFileSystem) Written(name string, _ bool) (committed bool, compiled bool, err error) {
	paths, err := fsys.beginChange("write", name)
	if err != nil {
		return false, false, err
	}
	defer fsys.endChange()
	projectionPath := paths[0]

	fsys.mu.Lock()
	held, ok := fsys.pending[projectionPath]
	whole := ok && held.isWhole()
	fsys.mu.Unlock()
	if !ok {
		return true, false, nil
	}
	if !whole {
		return false, false, nil
	}
	fsys.keepOff(projectionPath)
	if err := fsys.commitHeld(projectionPath); err != nil {
		return false, false, err
	}
	return true, true, nil
}

// Commit commits the writes still held for the file at name; see
// nfs.Committer. The client would not tell the writer of a refusal: it is
// recorded in /errors, and the image's version stands.
func (fsys *nfsFileSystem) Commit(name string) error {
	paths, err := fsys.beginChange("commit", name)
	if err != nil {
		return err
	}
	defer fsys.endChange()
	projectionPath := paths[0]

	fsys.mu.Lock()
	_, ok := fsys.pending[projectionPath]
	fsys.mu.Unlock()
	if ok {
		fsys.keepOff(projectionPath)
		_ = fsys.commitHeld(projectionPath)
	}
	return nil
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
			Fileid: fsys.ids.idFor(projectionPath),
		},
	}
}

// isAppleDoublePath answers whether projectionPath names a "._" file, where
// the system stores the extended attributes of a volume that cannot hold
// them, through doubleagentd. The mount refuses them, so setting an attribute
// fails as on a volume without any: a Mac running this daemon's tests panicked
// in the NFS client on doubleagentd's path, and mount_nfs's nouserxattr does
// not keep the client from taking it.
func isAppleDoublePath(projectionPath string) bool {
	return strings.HasPrefix(path.Base(projectionPath), "._")
}

func isSillyRenameName(name string) bool {
	return strings.HasPrefix(name, ".nfs")
}

// pageSize is the unit a client writes a file in, but for its end.
const pageSize = 4096

// isWhole answers whether every byte was written, and the last one is not
// at a page's end, which a client's last write to a file never is.
func (h *heldFile) isWhole() bool {
	size := len(h.contents)
	if size%pageSize == 0 {
		return false
	}
	return len(h.written) == 1 && h.written[0].start == 0 && h.written[0].end == size
}

type nfsFileKind int

const (
	snapshotFile nfsFileKind = iota
	pendingFile
	overlayFile
)

// nfsFile is what go-nfs opens for one call: a snapshot to read, or a view
// on the writes held for a file. It acts on the file it was opened on,
// wherever that file is by then.
type nfsFile struct {
	fsys     *nfsFileSystem
	name     string
	id       uint64
	path     string
	kind     nfsFileKind
	contents []byte
	offset   int64
}

func (fsys *nfsFileSystem) newFile(name string, projectionPath string, kind nfsFileKind) *nfsFile {
	return &nfsFile{fsys: fsys, name: name, id: fsys.ids.idFor(projectionPath), path: projectionPath, kind: kind}
}

func (f *nfsFile) Name() string {
	return f.name
}

func (f *nfsFile) Write(data []byte) (int, error) {
	if f.kind == snapshotFile {
		return 0, pathError("write", f.path, syscall.EBADF)
	}
	if err := f.fsys.writeAt(openFile{id: f.id, path: f.path}, data, f.offset); err != nil {
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
	if offset < 0 {
		return 0, pathError("read", f.path, syscall.EINVAL)
	}
	contents := f.contents
	if f.kind != snapshotFile {
		projectionPath, ok := f.fsys.ids.pathFor(f.id)
		if !ok {
			return 0, pathError("read", f.path, syscall.ESTALE)
		}
		entry, errno := f.fsys.entryFor(projectionPath)
		if errno != 0 {
			return 0, pathError("read", projectionPath, errno)
		}
		var err error
		if contents, err = f.fsys.contentsToRead(projectionPath, entry); err != nil {
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
	return f.fsys.truncate(openFile{id: f.id, path: f.path}, size)
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
// and names each file by its id, which follows it through renames.
type nfsHandler struct {
	fsys *nfsFileSystem

	mu       sync.Mutex
	listings map[string]cachedListing
}

type cachedListing struct {
	verifier uint64
	contents []fs.FileInfo
}

func newNFSHandler(fsys *nfsFileSystem) *nfsHandler {
	return &nfsHandler{
		fsys:     fsys,
		listings: map[string]cachedListing{},
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

// ToHandle answers the handle of what components name. Id 0 names nothing,
// so the handle of something gone meanwhile is stale.
func (h *nfsHandler) ToHandle(_ billy.Filesystem, components []string) []byte {
	handle := make([]byte, 8)
	if projectionPath, errno := h.fsys.ids.resolve(path.Join(components...)); errno == 0 {
		binary.BigEndian.PutUint64(handle, h.fsys.ids.idFor(projectionPath))
	}
	return handle
}

// FromHandle answers the components of the file a handle names, which carry
// its id: every name made from them stands for that file wherever it is when
// used. A handle whose file was removed, or replaced by a rename, is stale.
func (h *nfsHandler) FromHandle(handle []byte) (billy.Filesystem, []string, error) {
	if len(handle) != 8 {
		return nil, nil, errStaleHandle
	}
	id := binary.BigEndian.Uint64(handle)
	projectionPath, ok := h.fsys.ids.pathFor(id)
	if !ok {
		return nil, nil, errStaleHandle
	}
	return h.fsys, componentsFor(id, projectionPath), nil
}

// InvalidateHandle does nothing: go-nfs calls it with the handle a rename
// moved, which still names the renamed file, and the file system already
// retired the handles of what it removed or replaced.
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
