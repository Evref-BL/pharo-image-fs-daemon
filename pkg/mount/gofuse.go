//go:build linux || fusetest

package mount

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// fuseFileSystem adapts the projection to FUSE, as go-fuse serves it on
// Linux, on the rules every mount keeps (see fileSystem).
//
// FUSE has open and close. A projected file's writes are held per file, as
// the kernel's pages are, and compile when the process that opened the file
// closes it, or at an fsync: a refused compile fails that close(2) or
// fsync(2). Linux flushes at every close of every descriptor, including the
// copies children inherit, so a child closing its copy compiles nothing: a
// shell's `{ cat a; cat b; } > F` must not compile what cat a wrote alone.
// What no close compiled compiles when the last descriptor goes, and a
// refusal then reaches only /errors.
//
// A file opened neither truncating nor appending is written in place, as
// Bun's fs.writeFile, which OpenCode saves with, writes: it answers only what
// open(2) and write(2) answered. Its writes compile at the write that
// completes the file, as on NFS, so a refusal fails that write(2) (see
// fuseFile.inPlace). A file created exclusively is not written in place,
// though Linux sends its create without O_TRUNC: archive extractors and a
// shell's > under noclobber create so, and write in pieces that cut the file
// anywhere, so it compiles at the close.
//
// Removing a file removes it at once, from the image too, whether it is open
// or not; its open descriptors answer ESTALE from then on, as for a file
// removed on an NFS server.
type fuseFileSystem struct {
	*fileSystem

	// handles guards what follows and each fuseFile's state. It is taken
	// after changing, never before.
	handles sync.Mutex
	// open counts each id's open files.
	open map[uint64]int
	// writers holds, for each path, the open files whose writes the writes
	// held there carry.
	writers map[string]map[*fuseFile]struct{}
	// threadGroupOf answers the process a calling thread belongs to.
	threadGroupOf func(thread uint32) uint32
}

func newFuseFileSystem(client protocol.Client) *fuseFileSystem {
	return &fuseFileSystem{
		fileSystem:    newFileSystem(client),
		open:          map[uint64]int{},
		writers:       map[string]map[*fuseFile]struct{}{},
		threadGroupOf: threadGroupOf,
	}
}

// fuseOptions answers how go-fuse mounts fsys:
//
//   - The type is fuse.pharo-image-fs, name the source: Ubuntu's AppArmor
//     refuses a type with a slash, and fusermount3 refuses volname=.
//   - The kernel trusts entries and attributes for a second, as macOS's
//     actimeo=1, and asks for a name it did not find every time, so a class
//     added in the image shows at once.
//   - A listing is one READDIR, for names only.
//   - An open passes O_TRUNC, so a save's truncation belongs to its file.
//   - No extended attributes: the kernel answers EOPNOTSUPP from then on.
//   - A panic answers EIO, and is logged.
func fuseOptions(fsys *fuseFileSystem, name string, others []string, debugging bool, logger *log.Logger) *fs.Options {
	second := time.Second
	return &fs.Options{
		MountOptions: fuse.MountOptions{
			Name:               "pharo-image-fs",
			FsName:             name,
			Options:            others,
			DisableXAttrs:      true,
			DisableReadDirPlus: true,
			ExtraCapabilities:  fuse.CAP_ATOMIC_O_TRUNC,
			PanicHandler: func(value any) fuse.Status {
				fsys.logf("panic serving FUSE: %v\n%s", value, debug.Stack())
				return fuse.EIO
			},
			Debug:  debugging,
			Logger: logger,
		},
		EntryTimeout:   &second,
		AttrTimeout:    &second,
		RootStableAttr: &fs.StableAttr{Ino: fsys.ids.idFor("/"), Mode: fuse.S_IFDIR},
		Logger:         logger,
	}
}

// root answers the node of the mount's root.
func (fsys *fuseFileSystem) root() *fuseNode {
	return &fuseNode{fsys: fsys, id: fsys.ids.idFor("/")}
}

// fuseNode is a file or directory of the mount. It names its file by id, so
// it acts on that file wherever renames took it, and never by go-fuse's tree,
// which moves only once a call returns.
type fuseNode struct {
	fs.Inode
	fsys *fuseFileSystem
	id   uint64
}

var (
	_ = (fs.NodeLookuper)((*fuseNode)(nil))
	_ = (fs.NodeGetattrer)((*fuseNode)(nil))
	_ = (fs.NodeSetattrer)((*fuseNode)(nil))
	_ = (fs.NodeReaddirer)((*fuseNode)(nil))
	_ = (fs.NodeOpener)((*fuseNode)(nil))
	_ = (fs.NodeCreater)((*fuseNode)(nil))
	_ = (fs.NodeReader)((*fuseNode)(nil))
	_ = (fs.NodeWriter)((*fuseNode)(nil))
	_ = (fs.NodeFlusher)((*fuseNode)(nil))
	_ = (fs.NodeFsyncer)((*fuseNode)(nil))
	_ = (fs.NodeReleaser)((*fuseNode)(nil))
	_ = (fs.NodeUnlinker)((*fuseNode)(nil))
	_ = (fs.NodeRmdirer)((*fuseNode)(nil))
	_ = (fs.NodeMkdirer)((*fuseNode)(nil))
	_ = (fs.NodeRenamer)((*fuseNode)(nil))
	_ = (fs.NodeReadlinker)((*fuseNode)(nil))
	_ = (fs.NodeStatfser)((*fuseNode)(nil))
	_ = (fs.NodeSymlinker)((*fuseNode)(nil))
	_ = (fs.NodeLinker)((*fuseNode)(nil))
)

// fuseFile is a file a process opened.
type fuseFile struct {
	node     *fuseNode
	writable bool
	appends  bool
	// inPlace says the file writes over what it opened, neither truncated at
	// open nor appended to nor created exclusively, as a writer does that
	// sets the size of what it wrote after its writes. Its write that
	// completes a file, from 0 and ending inside a page (see
	// heldFile.isWhole), compiles it; its write at 0 with nothing held starts
	// the file over, so a shorter file does not compile over the image's
	// longer one; and a size it sets after a refusal, before it writes again,
	// changes nothing: Bun sets the size of what write(2) took, nothing when
	// the write was refused.
	inPlace bool
	// syncs says each write is synced, which the kernel does with an fsync
	// after it.
	syncs bool
	// opener is the process that opened the file, 0 when unknown.
	opener uint32

	// Guarded by the file system's handles.
	//
	// wrote says the file wrote since a compile took its writes, and
	// truncated that it opened truncating since: an open's truncation alone
	// compiles only at an fsync or once the file is closed for good, since a
	// shell opens a file, moves the descriptor and closes the first one
	// before anything is written. writingAt is where its writes are held.
	//
	// base is what the compile that took its writes took, which its next
	// writes go on from while the file has not changed since baseTime;
	// refusal is that compile's error, which each of its closes and fsyncs
	// answers until it writes again. syncDue says the kernel's fsync of a
	// synced write is still to come, which compiles nothing.
	wrote     bool
	truncated bool
	writingAt string
	base      []byte
	baseTime  time.Time
	refusal   error
	syncDue   bool
}

// path answers where the node's file is now.
func (n *fuseNode) path() (string, syscall.Errno) {
	projectionPath, ok := n.fsys.ids.pathFor(n.id)
	if !ok {
		return "", syscall.ESTALE
	}
	return projectionPath, 0
}

// child answers where the node's child name is now.
func (n *fuseNode) child(name string) (string, syscall.Errno) {
	projectionPath, errno := n.path()
	if errno != 0 {
		return "", errno
	}
	return joinProjectionPath(projectionPath, name), 0
}

// observe runs read on the path resolve answers (see fileSystem.observe).
func (n *fuseNode) observe(resolve func() (string, syscall.Errno), read func(projectionPath string) error) syscall.Errno {
	return errnoOf(n.fsys.observe(func() (string, error) {
		projectionPath, errno := resolve()
		if errno != 0 {
			return "", errno
		}
		return projectionPath, nil
	}, read))
}

func (n *fuseNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	var childPath string
	var entry protocol.Entry
	var modified time.Time
	errno := n.observe(func() (string, syscall.Errno) { return n.child(name) }, func(projectionPath string) error {
		var errno syscall.Errno
		childPath = projectionPath
		entry, modified, errno = n.fsys.statAt(projectionPath)
		return errnoError(errno)
	})
	if errno != 0 {
		return nil, errno
	}
	id := n.fsys.ids.idFor(childPath)
	n.fsys.fillAttr(&out.Attr, entry, modified, id)
	return n.NewInode(ctx, &fuseNode{fsys: n.fsys, id: id}, fs.StableAttr{Mode: typeBitsOf(entry), Ino: id}), 0
}

func (n *fuseNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	var entry protocol.Entry
	var modified time.Time
	errno := n.observe(n.path, func(projectionPath string) error {
		var errno syscall.Errno
		entry, modified, errno = n.fsys.statAt(projectionPath)
		return errnoError(errno)
	})
	if errno != 0 {
		return errno
	}
	n.fsys.fillAttr(&out.Attr, entry, modified, n.id)
	return 0
}

// Setattr sets a file's size; modes, owners and times belong to the image,
// so setting them succeeds and changes nothing.
//
// The kernel takes a SETATTR reply's time without dropping its pages, which
// only a GETATTR whose time moved does: so the reply never answers a change
// first. It answers the time without recording what it saw, and after a new
// size, the file's time moves past the reply's.
func (n *fuseNode) Setattr(_ context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	size, sized := in.GetSize()
	if sized {
		file, _ := f.(*fuseFile)
		if errno := n.fsys.setSize(n, file, size); errno != 0 {
			return errno
		}
	}
	projectionPath, errno := n.path()
	if errno != 0 {
		return errno
	}
	entry, modified, errno := n.fsys.peekAt(projectionPath)
	if errno != 0 {
		return errno
	}
	n.fsys.fillAttr(&out.Attr, entry, modified, n.id)
	if sized {
		n.fsys.mu.Lock()
		n.fsys.changed[projectionPath] = n.fsys.nowLocked()
		n.fsys.mu.Unlock()
	}
	return 0
}

func (n *fuseNode) Readdir(context.Context) (fs.DirStream, syscall.Errno) {
	var directory string
	var entries []protocol.Entry
	errno := n.observe(n.path, func(projectionPath string) error {
		listed, errno := n.fsys.listingFor(projectionPath)
		if errno != 0 {
			return errno
		}
		directory, entries = projectionPath, listed
		n.fsys.mu.Lock()
		n.fsys.recordListingLocked(projectionPath, listingSignature(entries))
		n.fsys.mu.Unlock()
		return nil
	})
	if errno != 0 {
		return nil, errno
	}
	list := make([]fuse.DirEntry, 0, len(entries)+2)
	list = append(list, fuse.DirEntry{Name: ".", Mode: fuse.S_IFDIR, Ino: n.id}, fuse.DirEntry{Name: "..", Mode: fuse.S_IFDIR})
	for _, entry := range entries {
		id := n.fsys.ids.idFor(joinProjectionPath(directory, entry.Name))
		list = append(list, fuse.DirEntry{Name: entry.Name, Mode: typeBitsOf(entry), Ino: id})
	}
	return fs.NewListDirStream(list), 0
}

// Open opens the node's file. A file opened only to read never compiles, so
// it asks for no flush at its close. Every open drops the attributes the
// kernel has for the file: so a file the image changed within the second the
// kernel trusts them for reads whole, as a client checks at open on macOS.
func (n *fuseNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	file := &fuseFile{
		node:     n,
		writable: flags&linuxOpenAccessMode != linuxOpenReadOnly || flags&linuxOpenTruncate != 0,
		appends:  flags&linuxOpenAppend != 0,
		inPlace:  flags&(linuxOpenTruncate|linuxOpenAppend) == 0,
		syncs:    flags&linuxOpenDataSync != 0,
	}
	if file.writable {
		// Only a writable file's close compiles, and asks whose it is.
		file.opener = n.fsys.callerProcess(ctx)
	}
	if !file.writable {
		if errno := n.fsys.openForReading(file); errno != 0 {
			return nil, 0, errno
		}
		n.dropCachedAttributes()
		return file, fuse.FOPEN_NOFLUSH, 0
	}
	if errno := n.fsys.openForWriting(file, flags&linuxOpenTruncate != 0); errno != 0 {
		return nil, 0, errno
	}
	n.dropCachedAttributes()
	return file, 0, 0
}

// Create creates a file. Creating is not writing: a class file closed with
// nothing written compiles nothing, and leaves once no one has it open, so
// that it never hides the class the image defines later.
func (n *fuseNode) Create(ctx context.Context, name string, flags uint32, _ uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	node, errno := n.fsys.create(n, name)
	if errno != 0 {
		return nil, nil, 0, errno
	}
	file := &fuseFile{node: node, writable: true, appends: flags&linuxOpenAppend != 0, inPlace: flags&(linuxOpenTruncate|linuxOpenAppend|linuxOpenExclusive) == 0, syncs: flags&linuxOpenDataSync != 0, opener: n.fsys.callerProcess(ctx)}
	n.fsys.opened(file)
	var attributes fuse.AttrOut
	if errno := node.Getattr(ctx, file, &attributes); errno == 0 {
		out.Attr = attributes.Attr
	}
	return n.NewInode(ctx, node, fs.StableAttr{Mode: fuse.S_IFREG, Ino: node.id}), file, 0, 0
}

func (n *fuseNode) Read(_ context.Context, _ fs.FileHandle, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	if offset < 0 {
		return nil, syscall.EINVAL
	}
	var contents []byte
	errno := n.observe(n.path, func(projectionPath string) error {
		entry, errno := n.fsys.entryFor(projectionPath)
		if errno != 0 {
			return errno
		}
		if entry.Kind == protocol.Directory {
			return syscall.EISDIR
		}
		read, err := n.fsys.contentsToRead(projectionPath, entry)
		contents = read
		return err
	})
	if errno != 0 {
		return nil, errno
	}
	return fuse.ReadResultData(sliceAt(contents, offset, len(dest))), 0
}

func (n *fuseNode) Write(_ context.Context, f fs.FileHandle, data []byte, offset int64) (uint32, syscall.Errno) {
	file, ok := f.(*fuseFile)
	if !ok || !file.writable {
		return 0, syscall.EBADF
	}
	if err := checkWrite(openFile{id: n.id}, data, offset); err != nil {
		return 0, errnoOf(err)
	}
	if errno := n.fsys.write(file, data, offset); errno != 0 {
		return 0, errno
	}
	return uint32(len(data)), 0
}

// Flush compiles what the file wrote, when the process that opened it closes
// it (see fuseFileSystem).
func (n *fuseNode) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	file, ok := f.(*fuseFile)
	if !ok || !file.writable {
		return 0
	}
	closing := n.fsys.callerProcess(ctx)
	if file.opener != 0 && closing != 0 && closing != file.opener {
		return 0
	}
	return n.fsys.compile(file, false)
}

// Fsync compiles what the file wrote, and an open's truncation, whoever
// asks; but the fsync the kernel makes after each write of a file opened to
// sync them, which would compile every part of a save.
func (n *fuseNode) Fsync(_ context.Context, f fs.FileHandle, _ uint32) syscall.Errno {
	file, ok := f.(*fuseFile)
	if !ok || !file.writable {
		return 0
	}
	n.fsys.handles.Lock()
	if file.syncDue {
		file.syncDue = false
		refusal := file.refusal
		n.fsys.handles.Unlock()
		return errnoOf(refusal)
	}
	n.fsys.handles.Unlock()
	return n.fsys.compile(file, true)
}

// Release compiles what no close compiled, once the file's last descriptor
// goes. The kernel does not wait for it, nor look at what it answers: a
// refusal then reaches only /errors.
func (n *fuseNode) Release(_ context.Context, f fs.FileHandle) syscall.Errno {
	if file, ok := f.(*fuseFile); ok {
		n.fsys.release(file)
	}
	return 0
}

func (n *fuseNode) Unlink(_ context.Context, name string) syscall.Errno {
	return n.fsys.remove(n, name, false)
}

func (n *fuseNode) Rmdir(_ context.Context, name string) syscall.Errno {
	return n.fsys.remove(n, name, true)
}

func (n *fuseNode) Mkdir(ctx context.Context, name string, _ uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	node, errno := n.fsys.makeDirectory(n, name)
	if errno != 0 {
		return nil, errno
	}
	var attributes fuse.AttrOut
	if errno := node.Getattr(ctx, nil, &attributes); errno != 0 {
		return nil, errno
	}
	out.Attr = attributes.Attr
	return n.NewInode(ctx, node, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: node.id}), 0
}

// The flags of open(2) and renameat2(2) as Linux sends them, whatever system
// the adapter is built on for its tests.
const (
	linuxOpenAccessMode = 0o3
	linuxOpenReadOnly   = 0o0
	linuxOpenExclusive  = 0o200
	linuxOpenTruncate   = 0o1000
	linuxOpenAppend     = 0o2000
	linuxOpenDataSync   = 0o10000

	renameNoReplace = 1
	renameExchange  = 2
	renameWhiteout  = 4
)

func (n *fuseNode) Rename(_ context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags&(renameExchange|renameWhiteout) != 0 {
		return syscall.EINVAL
	}
	target, ok := newParent.(*fuseNode)
	if !ok {
		return syscall.EXDEV
	}
	moved := n.GetChild(name)
	errno := n.fsys.rename(n, name, target, newName, flags&renameNoReplace != 0)
	if errno == 0 && moved != nil {
		// The file may now hold what the image made of it.
		moved.NotifyContent(-1, 0)
	}
	return errno
}

func (n *fuseNode) Readlink(context.Context) ([]byte, syscall.Errno) {
	var target string
	errno := n.observe(n.path, func(projectionPath string) error {
		var errno syscall.Errno
		target, errno = n.fsys.readlink(projectionPath)
		return errnoError(errno)
	})
	return []byte(target), errno
}

// Statfs answers a large, mostly free volume: the image has no size.
func (n *fuseNode) Statfs(_ context.Context, out *fuse.StatfsOut) syscall.Errno {
	const blockSize = 4096
	out.Bsize = blockSize
	out.Frsize = blockSize
	out.Blocks = (1 << 40) / blockSize
	out.Bfree = (1 << 39) / blockSize
	out.Bavail = (1 << 39) / blockSize
	out.Files = 1 << 20
	out.Ffree = 1 << 19
	out.NameLen = 255
	return 0
}

// Symlink and Link answer the errno of a file system without links, which
// Emacs's lock files and perl's -i fall back on.
func (n *fuseNode) Symlink(context.Context, string, string, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EPERM
}

func (n *fuseNode) Link(context.Context, fs.InodeEmbedder, string, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EPERM
}

// dropCachedAttributes makes the kernel ask for the node's attributes again
// before it next trusts them. Only attributes: the kernel drops them under a
// spinlock, while dropping pages or names takes locks a call waiting on a
// change may hold. A changed time or size then drops the pages.
func (n *fuseNode) dropCachedAttributes() {
	n.NotifyContent(-1, 0)
}

func (fsys *fuseFileSystem) fillAttr(out *fuse.Attr, entry protocol.Entry, modified time.Time, id uint64) {
	out.Ino = id
	out.Size = entry.Size
	out.Blocks = (entry.Size + 511) / 512
	out.Blksize = 4096
	out.Mode = typeBitsOf(entry) | permissionsFor(entry)
	// One link, as FUSE file systems that do not count a directory's do.
	out.Nlink = 1
	out.Owner = fuse.Owner{Uid: fsys.uid, Gid: fsys.gid}
	out.SetTimes(&modified, &modified, &modified)
}

func typeBitsOf(entry protocol.Entry) uint32 {
	switch entry.Kind {
	case protocol.Directory:
		return fuse.S_IFDIR
	case protocol.Symlink:
		return fuse.S_IFLNK
	default:
		return fuse.S_IFREG
	}
}

// openForWriting checks that file may write, and empties it when it opens
// truncating, as a save starts.
func (fsys *fuseFileSystem) openForWriting(file *fuseFile, truncates bool) syscall.Errno {
	fsys.changing.Lock()
	defer fsys.endChange()
	projectionPath, errno := file.node.path()
	if errno != 0 {
		return errno
	}
	entry, errno := fsys.entryFor(projectionPath)
	if errno == syscall.ENOENT {
		// The kernel's name outlived the file: ESTALE makes it look the
		// name up again, and create it when the open asks.
		return syscall.ESTALE
	}
	if errno != 0 {
		return errno
	}
	if entry.Kind == protocol.Directory {
		return syscall.EISDIR
	}
	if !entry.Writable {
		return syscall.EROFS
	}
	place, errno := fsys.writeKind(projectionPath)
	if errno != 0 {
		return errno
	}
	if truncates {
		fsys.keepOff(projectionPath)
		resized, err := fsys.resizeLocked(projectionPath, 0)
		if err != nil {
			return errnoOf(err)
		}
		if !resized {
			fsys.startSaveLocked(projectionPath)
		}
		if place == heldForTheImage {
			fsys.handles.Lock()
			fsys.holdsWritesLocked(file, projectionPath)
			file.truncated = true
			fsys.handles.Unlock()
		}
	}
	fsys.opened(file)
	return 0
}

// wroteLocked records that file wrote at projectionPath. The caller holds
// handles.
func (fsys *fuseFileSystem) wroteLocked(file *fuseFile, projectionPath string) {
	fsys.holdsWritesLocked(file, projectionPath)
	file.wrote = true
	file.syncDue = file.syncs
}

// holdsWritesLocked records that the writes held at projectionPath carry
// file's. The caller holds handles.
func (fsys *fuseFileSystem) holdsWritesLocked(file *fuseFile, projectionPath string) {
	file.refusal = nil
	if file.writingAt != "" && file.writingAt != projectionPath {
		delete(fsys.writers[file.writingAt], file)
	}
	file.writingAt = projectionPath
	if fsys.writers[projectionPath] == nil {
		fsys.writers[projectionPath] = map[*fuseFile]struct{}{}
	}
	fsys.writers[projectionPath][file] = struct{}{}
}

// write writes data at offset into file, or at its end when it appends: the
// kernel takes the end from the size it has, which the image may have
// changed since. Written in place, the write that completes a file compiles
// it, and answers the compile's result.
func (fsys *fuseFileSystem) write(file *fuseFile, data []byte, offset int64) syscall.Errno {
	compiled, errno := fsys.writeChanging(file, data, offset)
	if compiled {
		// The writer's cached size is that of what it wrote.
		file.node.dropCachedAttributes()
	}
	return errno
}

// writeChanging is write's change, and answers whether it compiled the file.
func (fsys *fuseFileSystem) writeChanging(file *fuseFile, data []byte, offset int64) (bool, syscall.Errno) {
	fsys.changing.Lock()
	defer fsys.endChange()

	fsys.handles.Lock()
	base, baseTime := file.base, file.baseTime
	fsys.handles.Unlock()

	projectionPath, errno := file.node.path()
	if errno != 0 {
		return false, errno
	}
	base = fsys.baseStill(projectionPath, base, baseTime)
	place, errno := fsys.writeKind(projectionPath)
	if errno != 0 {
		return false, errno
	}
	if file.appends {
		offset = atTheEnd
	}
	inPlace := file.inPlace && place == heldForTheImage
	if held, _ := fsys.heldAt(projectionPath); inPlace && offset == 0 && len(data) > 0 && !held {
		base = []byte{}
	}
	if err := fsys.writeLocked(projectionPath, data, offset, base); err != nil {
		return false, errnoOf(err)
	}
	if place != heldForTheImage {
		return false, 0
	}
	fsys.handles.Lock()
	fsys.wroteLocked(file, projectionPath)
	fsys.handles.Unlock()
	if _, whole := fsys.heldAt(projectionPath); !inPlace || !whole {
		return false, 0
	}
	fsys.keepOff(projectionPath)
	return true, errnoOf(fsys.commitLocked(projectionPath))
}

// heldAt answers whether writes are held for the file at projectionPath, and
// whether they make a whole file (see heldFile.isWhole).
func (fsys *fuseFileSystem) heldAt(projectionPath string) (held bool, whole bool) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	writes, held := fsys.pending[projectionPath]
	return held, held && writes.isWhole()
}

// baseStill answers base, what a file's last compile took, while the file
// at projectionPath has not changed since, at baseTime; nil otherwise, so
// that writes go on from what the file has now.
func (fsys *fuseFileSystem) baseStill(projectionPath string, base []byte, baseTime time.Time) []byte {
	if base == nil {
		return nil
	}
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if !fsys.changed[projectionPath].Equal(baseTime) {
		return nil
	}
	return base
}

// setSize sets the size of node's file. Through file, an ftruncate, the
// size is file's write, which its close compiles; without one, a
// truncate(2), it compiles now, since nothing follows it. The image's size,
// or that of what this mount last compiled, changes nothing either way, nor
// does a size a file written in place sets after a refusal, before it writes
// again (see fuseFile.inPlace).
func (fsys *fuseFileSystem) setSize(node *fuseNode, file *fuseFile, size uint64) syscall.Errno {
	if size > maxFileSize {
		return syscall.EFBIG
	}
	fsys.changing.Lock()
	defer fsys.endChange()

	fsys.handles.Lock()
	var base []byte
	var baseTime time.Time
	refused := false
	if file != nil {
		base, baseTime = file.base, file.baseTime
		refused = file.inPlace && file.refusal != nil && !file.wrote
	}
	fsys.handles.Unlock()

	projectionPath, errno := node.path()
	if errno != 0 {
		return errno
	}
	base = fsys.baseStill(projectionPath, base, baseTime)
	place, errno := fsys.writeKind(projectionPath)
	if errno != 0 {
		return errno
	}
	if held, _ := fsys.heldAt(projectionPath); refused && place == heldForTheImage && !held {
		return 0
	}
	fsys.keepOff(projectionPath)
	resized, err := fsys.resizeLocked(projectionPath, int(size))
	if err != nil {
		return errnoOf(err)
	}
	switch {
	case resized:
	case fsys.hasSize(projectionPath, int(size)):
		return 0
	case file == nil:
		return errnoOf(fsys.compileCutLocked(projectionPath, int(size)))
	default:
		if err := fsys.hold(projectionPath, base); err != nil {
			return errnoOf(err)
		}
		if _, err := fsys.resizeLocked(projectionPath, int(size)); err != nil {
			return errnoOf(err)
		}
	}
	if file != nil && place == heldForTheImage {
		fsys.handles.Lock()
		fsys.wroteLocked(file, projectionPath)
		fsys.handles.Unlock()
	}
	return 0
}

// compile compiles what file wrote, with the writes of every other file held
// at the same path, and answers the result it has: that of the compile that
// took its writes, when another took them. An open's truncation compiles
// only when truncation says so: at an fsync, or once the file is closed for
// good.
func (fsys *fuseFileSystem) compile(file *fuseFile, truncation bool) syscall.Errno {
	fsys.changing.Lock()
	fsys.handles.Lock()
	due, refusal := file.wrote || truncation && file.truncated, file.refusal
	fsys.handles.Unlock()
	if !due {
		fsys.changing.Unlock()
		return errnoOf(refusal)
	}
	projectionPath, errno := file.node.path()
	if errno != 0 {
		fsys.changing.Unlock()
		return errno
	}
	fsys.keepOff(projectionPath)
	err := fsys.commitLocked(projectionPath)
	fsys.endChange()
	// The writer's cached size is that of its held writes.
	file.node.dropCachedAttributes()
	return errnoOf(err)
}

// commitLocked compiles the writes held at projectionPath, if any, and gives
// its result to every file whose writes they carry. The caller holds
// changing, and keeps calls off projectionPath.
func (fsys *fuseFileSystem) commitLocked(projectionPath string) error {
	fsys.mu.Lock()
	held, ok := fsys.pending[projectionPath]
	var contents []byte
	if ok {
		// Not nil when empty: writes go on from it.
		contents = append([]byte{}, held.contents...)
	}
	fsys.mu.Unlock()
	if !ok {
		return nil
	}
	err := fsys.commitHeld(projectionPath)
	if err != nil {
		if _, errno := fsys.entryFor(projectionPath); errno == syscall.ENOENT {
			// A created file the image refused left with its compile: its
			// nodes are stale, and the kernel looks its name up again.
			fsys.ids.removed(projectionPath)
		}
	}
	fsys.mu.Lock()
	compiledAt := fsys.changed[projectionPath]
	fsys.mu.Unlock()
	fsys.handles.Lock()
	defer fsys.handles.Unlock()
	for writer := range fsys.writers[projectionPath] {
		writer.wrote = false
		writer.truncated = false
		writer.refusal = err
		writer.base = contents
		writer.baseTime = compiledAt
		writer.writingAt = ""
	}
	delete(fsys.writers, projectionPath)
	return err
}

// release forgets file, once its last descriptor is gone.
func (fsys *fuseFileSystem) release(file *fuseFile) {
	fsys.handles.Lock()
	due := file.wrote || file.truncated
	fsys.handles.Unlock()
	if due {
		_ = fsys.compile(file, true)
	}

	fsys.handles.Lock()
	if file.writingAt != "" {
		delete(fsys.writers[file.writingAt], file)
		if len(fsys.writers[file.writingAt]) == 0 {
			delete(fsys.writers, file.writingAt)
		}
	}
	fsys.handles.Unlock()
	if fsys.closed(file) {
		fsys.dropUnwrittenCreation(file.node)
	}
}

// opened counts file among its id's open files.
func (fsys *fuseFileSystem) opened(file *fuseFile) {
	fsys.handles.Lock()
	defer fsys.handles.Unlock()
	fsys.open[file.node.id]++
}

// closed forgets file among its id's open files, and answers whether it was
// the last.
func (fsys *fuseFileSystem) closed(file *fuseFile) bool {
	fsys.handles.Lock()
	defer fsys.handles.Unlock()
	id := file.node.id
	fsys.open[id]--
	if fsys.open[id] > 0 {
		return false
	}
	delete(fsys.open, id)
	return true
}

// openForReading counts file among its id's open files once no change of its
// path runs, or answers ESTALE when the file went meanwhile: the kernel then
// looks the name up again. It counts first, so that a created file is not
// dropped under an open that is still on its way.
func (fsys *fuseFileSystem) openForReading(file *fuseFile) syscall.Errno {
	fsys.opened(file)
	if errno := file.node.observe(file.node.path, func(string) error { return nil }); errno != 0 {
		fsys.closed(file)
		return errno
	}
	return 0
}

// dropUnwrittenCreation drops the writes held for node's file when no one
// has it open and no file wrote them: a class file created, and closed with
// nothing written. Nothing would ever compile them, and they would hide the
// class the image defines later.
func (fsys *fuseFileSystem) dropUnwrittenCreation(node *fuseNode) {
	projectionPath, errno := node.path()
	if errno != 0 {
		return
	}
	fsys.mu.Lock()
	_, held := fsys.pending[projectionPath]
	fsys.mu.Unlock()
	if !held {
		return
	}

	fsys.changing.Lock()
	defer fsys.endChange()
	fsys.handles.Lock()
	unwritten := fsys.open[node.id] == 0 && len(fsys.writers[projectionPath]) == 0
	fsys.handles.Unlock()
	if !unwritten {
		return
	}
	fsys.keepOff(projectionPath)
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if _, held := fsys.pending[projectionPath]; held {
		delete(fsys.pending, projectionPath)
		fsys.namespaceChangedLocked(projectionPath)
		if _, errno := fsys.entryForPath(projectionPath); errno == syscall.ENOENT {
			// Its nodes are stale, and the kernel looks its name up again.
			fsys.ids.removed(projectionPath)
		}
	}
}

// create creates name in parent's directory: held for the image when it
// names a class, an editor's file in the overlay otherwise.
func (fsys *fuseFileSystem) create(parent *fuseNode, name string) (*fuseNode, syscall.Errno) {
	fsys.changing.Lock()
	defer fsys.endChange()
	projectionPath, errno := parent.child(name)
	if errno != 0 {
		return nil, errno
	}
	fsys.keepOff(projectionPath)

	if isIgnoredMetadataPath(projectionPath) {
		if !isWritableProjectionPath(projectionPath) {
			return nil, syscall.EROFS
		}
		fsys.overlay.Create(projectionPath, nil)
	} else {
		fsys.noteCreateOverBackup(projectionPath)
		if _, errno := fsys.createFile(projectionPath); errno != 0 {
			return nil, errno
		}
		if isProjectedTonelFilePath(projectionPath) {
			fsys.overlay.Delete(projectionPath)
			fsys.mu.Lock()
			fsys.pending[projectionPath] = &heldFile{}
			fsys.mu.Unlock()
		}
	}
	fsys.ids.removed(projectionPath)
	fsys.namespaceChanged(projectionPath)
	return &fuseNode{fsys: fsys, id: fsys.ids.idFor(projectionPath)}, 0
}

func (fsys *fuseFileSystem) makeDirectory(parent *fuseNode, name string) (*fuseNode, syscall.Errno) {
	fsys.changing.Lock()
	defer fsys.endChange()
	projectionPath, errno := parent.child(name)
	if errno != 0 {
		return nil, errno
	}
	fsys.keepOff(projectionPath)
	errno = fsys.mkdir(projectionPath)
	fsys.namespaceChanged(projectionPath)
	if errno != 0 {
		return nil, errno
	}
	fsys.ids.removed(projectionPath)
	return &fuseNode{fsys: fsys, id: fsys.ids.idFor(projectionPath)}, 0
}

// remove removes name from parent's directory, and from the image. Open
// descriptors of a removed file go on with a copy of it.
func (fsys *fuseFileSystem) remove(parent *fuseNode, name string, directory bool) syscall.Errno {
	fsys.changing.Lock()
	defer fsys.endChange()
	projectionPath, errno := parent.child(name)
	if errno != 0 {
		return errno
	}
	fsys.keepOff(projectionPath)

	if directory {
		errno = fsys.rmdir(projectionPath)
	} else {
		fsys.mu.Lock()
		held, wasHeld := fsys.pending[projectionPath]
		delete(fsys.pending, projectionPath)
		fsys.mu.Unlock()
		if errno = fsys.unlinkFile(projectionPath); errno != 0 {
			if wasHeld {
				fsys.mu.Lock()
				fsys.pending[projectionPath] = held
				fsys.mu.Unlock()
			}
		} else {
			fsys.forgetWriters(projectionPath)
		}
	}
	fsys.namespaceChanged(projectionPath)
	if errno != 0 {
		return errno
	}
	fsys.ids.removed(projectionPath)
	return 0
}

// forgetWriters forgets the writes held at projectionPath, which a removal
// or a rename over it took: their files answer ESTALE from then on. The
// caller holds changing.
func (fsys *fuseFileSystem) forgetWriters(projectionPath string) {
	fsys.handles.Lock()
	defer fsys.handles.Unlock()
	for writer := range fsys.writers[projectionPath] {
		writer.wrote = false
		writer.truncated = false
		writer.writingAt = ""
	}
	delete(fsys.writers, projectionPath)
}

// rename renames oldName in oldParent's directory to newName in newParent's.
// Writes held for the file compile first, and a rename onto a class name
// compiles the editor's file it renames (see projection.rename): a refusal
// fails the rename, and a class an editor's backup rename hid shows again.
func (fsys *fuseFileSystem) rename(oldParent *fuseNode, oldName string, newParent *fuseNode, newName string, noReplace bool) syscall.Errno {
	fsys.changing.Lock()
	defer fsys.endChange()
	oldPath, errno := oldParent.child(oldName)
	if errno != 0 {
		return errno
	}
	newPath, errno := newParent.child(newName)
	if errno != 0 {
		return errno
	}
	if noReplace {
		// The kernel checked a lookup of its own, which the image may have
		// overtaken since.
		if _, errno := fsys.entryFor(newPath); errno == 0 {
			return syscall.EEXIST
		}
	}
	fsys.keepOff(oldPath, newPath)

	if err := fsys.commitLocked(oldPath); err != nil {
		fsys.showHiddenClass(newPath)
		return errnoOf(err)
	}
	errno = fsys.projection.rename(oldPath, newPath)
	fsys.namespaceChanged(oldPath)
	fsys.namespaceChanged(newPath)
	if errno != 0 {
		fsys.showHiddenClass(newPath)
		return errno
	}
	fsys.forgetWriters(newPath)
	fsys.renamedOver(oldPath, newPath)
	return 0
}

// callerProcess answers the process the thread making the call belongs to,
// 0 when the kernel does not say: from another process namespace.
func (fsys *fuseFileSystem) callerProcess(ctx context.Context) uint32 {
	caller, ok := fuse.FromContext(ctx)
	if !ok || caller.Pid == 0 {
		return 0
	}
	return fsys.threadGroupOf(caller.Pid)
}

// threadGroupOf answers the process thread belongs to, from /proc: FUSE
// names the calling thread, and a program may open a file on one thread and
// close it on another. It answers thread itself when /proc does not say.
func threadGroupOf(thread uint32) uint32 {
	status, err := os.Open(fmt.Sprintf("/proc/%d/status", thread))
	if err != nil {
		return thread
	}
	defer status.Close()
	lines := bufio.NewScanner(status)
	for lines.Scan() {
		if value, ok := strings.CutPrefix(lines.Text(), "Tgid:"); ok {
			if group, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32); err == nil {
				return uint32(group)
			}
		}
	}
	return thread
}

func sliceAt(contents []byte, offset int64, size int) []byte {
	if offset >= int64(len(contents)) {
		return nil
	}
	return contents[offset:min(int64(len(contents)), offset+int64(size))]
}

// errnoOf answers the errno err carries, or the one errnoFor gives it.
func errnoOf(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	return errnoFor(err)
}

// errnoError answers errno as an error, nil for 0.
func errnoError(errno syscall.Errno) error {
	if errno == 0 {
		return nil
	}
	return errno
}
