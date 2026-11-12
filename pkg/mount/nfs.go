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
//
// go-nfs serves several calls at once. Changes run one at a time, under
// changing, which each holds through its call to the image: the image
// compiles one file at a time, and no change interleaves with another's check
// and act on held writes or editor files. While a change runs, calls on the
// paths it changes, and on everything under them, wait for it, and a call
// that ran while a change of its path began runs again (see observe): no call
// sees a file halfway through a compile or a rename, and calls on other files
// go on.
//
// The names go-nfs passes carry the id of the file their handle named (see
// componentsFor), so each call acts on that file wherever it is by then.
type nfsFileSystem struct {
	*projection

	uid   uint32
	gid   uint32
	clock func() time.Time
	// started is when the daemon started, on the wall clock, as every time it
	// answers is.
	started time.Time
	ids     *identities

	changing sync.Mutex
	// silly maps each ".nfs…" file the client renamed an open class file to,
	// to that file's path; the class goes when the ".nfs…" file does. Guarded
	// by changing.
	silly map[string]string

	mu sync.Mutex
	// busy is what the running change changes, and settled is broadcast when
	// it ends. marks holds, for each path a change changed, the number of the
	// last change that did, and begun the number of the last change begun.
	busy    []string
	settled *sync.Cond
	marks   map[string]uint64
	begun   uint64
	pending map[string]*heldFile
	// compiled holds the length of what this mount last compiled at each
	// path.
	compiled map[string]int
	changed  map[string]time.Time
	// seen holds, for each file, the entry the last stat of it recorded, and
	// stats the number of the last stat that recorded one.
	seen     map[string]seenEntry
	stats    uint64
	listings map[string]listing
	reads    map[string]cachedRead
	last     time.Time
}

type seenEntry struct {
	size     uint64
	modified int64
	stat     uint64
}

type listing struct {
	signature uint64
	changed   time.Time
	checked   time.Time
}

type cachedRead struct {
	contents []byte
	read     time.Time
	size     uint64
	modified int64
}

func newNFSFileSystem(client protocol.Client) *nfsFileSystem {
	started := time.Now().Round(0)
	fsys := &nfsFileSystem{
		projection: newProjection(client),
		uid:        uint32(os.Getuid()),
		gid:        uint32(os.Getgid()),
		clock:      time.Now,
		started:    started,
		ids:        newIdentities(),
		silly:      map[string]string{},
		marks:      map[string]uint64{},
		pending:    map[string]*heldFile{},
		compiled:   map[string]int{},
		changed:    map[string]time.Time{},
		seen:       map[string]seenEntry{},
		listings:   map[string]listing{},
		reads:      map[string]cachedRead{},
		last:       started,
	}
	fsys.settled = sync.NewCond(&fsys.mu)
	return fsys
}

// projectionPathFor answers the projection path of a go-nfs file name, which
// is relative and empty for the root.
func projectionPathFor(name string) string {
	return path.Clean("/" + name)
}

func pathError(op string, projectionPath string, errno syscall.Errno) error {
	return &os.PathError{Op: op, Path: projectionPath, Err: errno}
}

// nameError is the error of op on a name that resolve refused.
func nameError(op string, name string, errno syscall.Errno) error {
	return pathError(op, strings.ReplaceAll(name, idMark, "#"), errno)
}

// maxFileSize bounds what a write or truncation may make a file: no class
// file comes near it, and running out of memory is not a panic that a
// recovery could answer.
const maxFileSize = 64 << 20

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

// keepOff makes calls on paths, and on everything under them, wait until the
// running change ends, and those that began before it run again.
func (fsys *nfsFileSystem) keepOff(paths ...string) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	fsys.begun++
	fsys.busy = append(fsys.busy, paths...)
	for _, projectionPath := range paths {
		fsys.marks[projectionPath] = fsys.begun
	}
}

func (fsys *nfsFileSystem) endChange() {
	fsys.mu.Lock()
	fsys.busy = nil
	fsys.mu.Unlock()
	fsys.settled.Broadcast()
	fsys.changing.Unlock()
}

// isBusyLocked answers whether projectionPath is, or is under, a path the
// running change changes.
func (fsys *nfsFileSystem) isBusyLocked(projectionPath string) bool {
	for _, busy := range fsys.busy {
		if _, ok := pathUnder(projectionPath, busy); ok {
			return true
		}
	}
	return false
}

// markLocked answers the number of the last change of projectionPath or of a
// directory above it.
func (fsys *nfsFileSystem) markLocked(projectionPath string) uint64 {
	var latest uint64
	for current := projectionPath; ; current = path.Dir(current) {
		latest = max(latest, fsys.marks[current])
		if current == "/" {
			return latest
		}
	}
}

// observe runs read on what name stands for, once no change changes it, and
// again if a change of it began meanwhile: so read answers what held between
// two changes, never what one left halfway.
func (fsys *nfsFileSystem) observe(op string, name string, read func(projectionPath string) error) error {
	for {
		fsys.mu.Lock()
		begun := fsys.begun
		fsys.mu.Unlock()
		projectionPath, errno := fsys.ids.resolve(name)
		if errno != 0 {
			return nameError(op, name, errno)
		}

		fsys.mu.Lock()
		if fsys.markLocked(projectionPath) > begun || fsys.isBusyLocked(projectionPath) {
			for fsys.isBusyLocked(projectionPath) {
				fsys.settled.Wait()
			}
			fsys.mu.Unlock()
			continue
		}
		fsys.mu.Unlock()

		err := read(projectionPath)
		fsys.mu.Lock()
		unchanged := fsys.markLocked(projectionPath) <= begun
		fsys.mu.Unlock()
		if unchanged {
			return err
		}
	}
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

	if _, errno := fsys.createFile(projectionPath); errno != 0 {
		return nil, pathError("create", projectionPath, errno)
	}
	fsys.forgetSilly(projectionPath)
	kind := overlayFile
	if isProjectedTonelFilePath(projectionPath) {
		fsys.overlay.Delete(projectionPath)
		fsys.mu.Lock()
		fsys.pending[projectionPath] = &heldFile{}
		fsys.mu.Unlock()
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
	err := fsys.observe("open", name, func(projectionPath string) error {
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
		fsys.showRemovedClass(newPath)
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
		fsys.showRemovedClass(newPath)
		return pathError("rename", newPath, errno)
	}
	fsys.ids.renamed(oldPath, newPath)
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

// showRemovedClass shows the class at projectionPath again, when its file was
// removed while open and what was to take the name failed: the image still
// has it, as a refused rename save leaves it.
func (fsys *nfsFileSystem) showRemovedClass(projectionPath string) {
	for _, original := range fsys.silly {
		if original == projectionPath && fsys.overlay.IsHidden(projectionPath) {
			fsys.overlay.Delete(projectionPath)
			fsys.namespaceChanged(projectionPath)
			fsys.forgetSilly(projectionPath)
			return
		}
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
			fsys.errors.RecordWriteError(original, err)
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
		errno = fsys.unlink(projectionPath)
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
	err := fsys.observe("readdir", name, func(projectionPath string) error {
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
	err := fsys.observe("lstat", name, func(projectionPath string) error {
		for {
			fsys.mu.Lock()
			stat := fsys.stats
			fsys.mu.Unlock()
			entry, errno := fsys.entryFor(projectionPath)
			if errno != 0 {
				return pathError("lstat", projectionPath, errno)
			}
			if modified, ok := fsys.modificationTime(projectionPath, entry, stat); ok {
				info = fsys.infoFor(projectionPath, entry, modified)
				return nil
			}
		}
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
	err := fsys.observe("readlink", name, func(projectionPath string) error {
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
// whole file; see nfs.Committer. A client writes whole pages, but for the
// last one of a file: so held writes that cover the file and end inside a
// page are a whole file, and one whose size is a multiple of the page size
// waits for the COMMIT. A compiled file holds what the image renders, which
// may differ from what was written.
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

// commitHeld compiles the writes held for the file at projectionPath, and
// drops them either way: the file is the image's from then on. The caller
// holds changing, and keeps calls off projectionPath.
func (fsys *nfsFileSystem) commitHeld(projectionPath string) error {
	fsys.mu.Lock()
	held, ok := fsys.pending[projectionPath]
	var contents []byte
	if ok {
		contents = held.contents
	}
	fsys.mu.Unlock()
	if !ok {
		return nil
	}

	err := fsys.writeProjection(context.Background(), projectionPath, contents)
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	delete(fsys.pending, projectionPath)
	if err != nil {
		// A file created through the mount leaves its directory with it.
		fsys.namespaceChangedLocked(projectionPath)
		return pathError("write", projectionPath, errnoFor(err))
	}
	fsys.contentsChangedLocked(projectionPath)
	fsys.compiled[projectionPath] = len(contents)
	return nil
}

// entryFor answers the entry at projectionPath, as the writes held for it
// left it.
func (fsys *nfsFileSystem) entryFor(projectionPath string) (protocol.Entry, syscall.Errno) {
	fsys.mu.Lock()
	held, ok := fsys.pending[projectionPath]
	var size int
	if ok {
		size = len(held.contents)
	}
	fsys.mu.Unlock()
	if ok {
		return protocol.Entry{Name: path.Base(projectionPath), Kind: protocol.File, Size: uint64(size), Writable: true}, 0
	}
	return fsys.entryForPath(projectionPath)
}

// contentsToRead answers the contents at projectionPath, whose entry was
// just taken: the writes held for it, an editor's file or an error as they
// are, or what the image renders. A rendering serves the reads that follow for
// a second, while the entry they take is the same and unless a change of the
// file began while the image rendered it.
func (fsys *nfsFileSystem) contentsToRead(projectionPath string, entry protocol.Entry) ([]byte, error) {
	fsys.mu.Lock()
	held, ok := fsys.pending[projectionPath]
	if ok {
		contents := append([]byte(nil), held.contents...)
		fsys.mu.Unlock()
		return contents, nil
	}
	fsys.mu.Unlock()
	if contents, ok := fsys.overlay.Read(projectionPath); ok {
		return contents, nil
	}
	if contents, ok := fsys.errors.Read(projectionPath); ok {
		return contents, nil
	}

	fsys.mu.Lock()
	cached, ok := fsys.reads[projectionPath]
	if ok && time.Since(cached.read) < readFreshness && cached.size == entry.Size && cached.modified == entry.Modified {
		fsys.mu.Unlock()
		return cached.contents, nil
	}
	mark := fsys.markLocked(projectionPath)
	wasBusy := fsys.isBusyLocked(projectionPath)
	fsys.mu.Unlock()

	contents, err := fsys.contentsForPath(projectionPath)
	if err != nil {
		return nil, err
	}
	fsys.mu.Lock()
	_, heldNow := fsys.pending[projectionPath]
	if !heldNow && !wasBusy && !fsys.isBusyLocked(projectionPath) && fsys.markLocked(projectionPath) == mark {
		fsys.reads[projectionPath] = cachedRead{contents: contents, read: time.Now(), size: entry.Size, modified: entry.Modified}
	}
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
			Fileid: fsys.ids.idFor(projectionPath),
		},
	}
}

// modificationTime answers when the entry at projectionPath, which the stat
// after the one numbered stat took, last changed, as far as the daemon can
// tell: the NFS client keeps its cached pages and listings while this holds
// still. It answers nothing when a stat that overlapped this one saw the
// file otherwise, as nothing says which of the two saw it last: the caller
// takes the entry again, and sees the file at least as late.
func (fsys *nfsFileSystem) modificationTime(projectionPath string, entry protocol.Entry, stat uint64) (time.Time, bool) {
	if entry.Kind == protocol.Directory {
		return fsys.directoryTime(projectionPath), true
	}

	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if _, ok := fsys.pending[projectionPath]; !ok {
		previous, seen := fsys.seen[projectionPath]
		if seen && (previous.size != entry.Size || previous.modified != entry.Modified) {
			if previous.stat > stat {
				return time.Time{}, false
			}
			fsys.changed[projectionPath] = fsys.nowLocked()
			delete(fsys.reads, projectionPath)
		}
		fsys.stats++
		fsys.seen[projectionPath] = seenEntry{size: entry.Size, modified: entry.Modified, stat: fsys.stats}
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
	return modified, true
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

	entries, errno := fsys.listingFor(projectionPath)
	if errno != 0 {
		if ok {
			return known.changed
		}
		return fsys.started
	}

	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	return fsys.recordListingLocked(projectionPath, listingSignature(entries))
}

// listingFor answers the entries of the directory at projectionPath, as a
// READDIR serves them: the image's, with the files created through this
// mount that it does not have yet.
func (fsys *nfsFileSystem) listingFor(projectionPath string) ([]protocol.Entry, syscall.Errno) {
	entries, errno := fsys.entriesIn(projectionPath)
	if errno != 0 {
		return nil, errno
	}

	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	for pendingPath := range fsys.pending {
		if path.Dir(pendingPath) == projectionPath && !containsEntryNamed(entries, path.Base(pendingPath)) {
			entries = append(entries, protocol.Entry{Name: path.Base(pendingPath), Kind: protocol.File, Writable: true})
		}
	}
	return entries, 0
}

// recordListingLocked records a listing of the directory at projectionPath,
// just taken, and answers the directory's time. The time moves whenever the
// listing differs from the one recorded before, whichever of two overlapping
// listings is recorded last: so every listing served stands for a time that
// the directory leaves once its listing is another.
func (fsys *nfsFileSystem) recordListingLocked(projectionPath string, signature uint64) time.Time {
	known, ok := fsys.listings[projectionPath]
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

	fsys.namespaceChangedLocked(projectionPath)
}

func (fsys *nfsFileSystem) namespaceChangedLocked(projectionPath string) {
	now := fsys.nowLocked()
	parent := path.Dir(projectionPath)
	fsys.listings[parent] = listing{signature: 0, changed: now}
	fsys.changed[projectionPath] = now
	delete(fsys.reads, projectionPath)
	delete(fsys.seen, projectionPath)
	delete(fsys.compiled, projectionPath)
}

// contentsChanged records that the contents at projectionPath changed
// through this mount.
func (fsys *nfsFileSystem) contentsChanged(projectionPath string) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	fsys.contentsChangedLocked(projectionPath)
}

func (fsys *nfsFileSystem) contentsChangedLocked(projectionPath string) {
	fsys.changed[projectionPath] = fsys.nowLocked()
	delete(fsys.reads, projectionPath)
	delete(fsys.seen, projectionPath)
}

// nowLocked answers the current time on the wall clock, which is what a
// client is sent, after every time it answered before, even when the wall
// clock steps back: so two changes in a row never share a modification time,
// and none goes back.
func (fsys *nfsFileSystem) nowLocked() time.Time {
	now := fsys.clock().Round(0)
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

// writeAt writes data at offset into file, held in pending or in the
// overlay, wherever file is by now.
func (fsys *nfsFileSystem) writeAt(file *nfsFile, data []byte, offset int64) error {
	if offset < 0 {
		return pathError("write", file.path, syscall.EINVAL)
	}
	if offset > maxFileSize || int64(len(data)) > maxFileSize-offset {
		return pathError("write", file.path, syscall.EFBIG)
	}
	fsys.changing.Lock()
	defer fsys.changing.Unlock()
	projectionPath, ok := fsys.ids.pathFor(file.id)
	if !ok {
		return pathError("write", file.path, syscall.ESTALE)
	}
	kind, errno := fsys.writeKind(projectionPath)
	if errno != 0 {
		return pathError("write", projectionPath, errno)
	}

	if kind == overlayFile {
		contents, ok := fsys.overlay.Read(projectionPath)
		if !ok {
			return pathError("write", projectionPath, syscall.ENOENT)
		}
		_ = fsys.overlay.Write(context.Background(), projectionPath, patched(contents, data, offset))
		fsys.contentsChanged(projectionPath)
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

// truncate resizes file, held in pending or in the overlay, wherever file is
// by now.
//
// A client flushes what it wrote to a file before it sets the file's size,
// so no write follows a size set on a projected file with nothing held, but
// the 0 a save starts with. The file already has that size when it is the
// image's, or that of what the client wrote and the image compiled, which a
// client writing a file in place sets after; otherwise the file is compiled,
// cut to that size, now.
func (fsys *nfsFileSystem) truncate(file *nfsFile, size int64) error {
	if size < 0 {
		return pathError("truncate", file.path, syscall.EINVAL)
	}
	if size > maxFileSize {
		return pathError("truncate", file.path, syscall.EFBIG)
	}
	fsys.changing.Lock()
	defer fsys.endChange()
	projectionPath, ok := fsys.ids.pathFor(file.id)
	if !ok {
		return pathError("truncate", file.path, syscall.ESTALE)
	}
	kind, errno := fsys.writeKind(projectionPath)
	if errno != 0 {
		return pathError("truncate", projectionPath, errno)
	}

	if kind == overlayFile {
		contents, ok := fsys.overlay.Read(projectionPath)
		if !ok {
			return pathError("truncate", projectionPath, syscall.ENOENT)
		}
		_ = fsys.overlay.Write(context.Background(), projectionPath, resizedContents(contents, int(size)))
		fsys.contentsChanged(projectionPath)
		return nil
	}

	fsys.mu.Lock()
	held, ok := fsys.pending[projectionPath]
	switch {
	case ok:
		held.truncate(int(size))
		fsys.mu.Unlock()
		return nil
	case size == 0:
		// A save starts by emptying the file: what the image has is not needed.
		fsys.pending[projectionPath] = &heldFile{}
		fsys.changed[projectionPath] = fsys.nowLocked()
		fsys.mu.Unlock()
		return nil
	}
	compiled, ok := fsys.compiled[projectionPath]
	fsys.mu.Unlock()
	if ok && compiled == int(size) {
		return nil
	}
	entry, errno := fsys.entryForPath(projectionPath)
	if errno != 0 {
		return pathError("truncate", projectionPath, errno)
	}
	if entry.Size == uint64(size) {
		return nil
	}

	fsys.keepOff(projectionPath)
	contents, err := fsys.contentsForPath(projectionPath)
	if err != nil {
		return pathError("truncate", projectionPath, errnoFor(err))
	}
	fsys.mu.Lock()
	fsys.pending[projectionPath] = &heldFile{contents: resizedContents(append([]byte(nil), contents...), int(size))}
	fsys.mu.Unlock()
	return fsys.commitHeld(projectionPath)
}

// writeKind answers where writes to projectionPath are kept now: held for
// the image, or in the overlay. The caller holds changing.
func (fsys *nfsFileSystem) writeKind(projectionPath string) (nfsFileKind, syscall.Errno) {
	fsys.mu.Lock()
	_, held := fsys.pending[projectionPath]
	fsys.mu.Unlock()
	if held {
		return pendingFile, 0
	}
	if _, ok := fsys.overlay.Stat(projectionPath); ok {
		return overlayFile, 0
	}
	if isProjectedTonelFilePath(projectionPath) {
		return pendingFile, 0
	}
	return 0, syscall.EROFS
}

// hold makes sure writes to projectionPath are held, starting from what the
// image has when none are yet. The caller holds changing.
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
	if err := f.fsys.writeAt(f, data, f.offset); err != nil {
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
	return f.fsys.truncate(f, size)
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
