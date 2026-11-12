//go:build windows

package mount

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"syscall"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
	"github.com/winfsp/cgofuse/fuse"
)

// ProjectionFileSystem adapts the projection to FUSE, for the systems that
// have it: libfuse3 on Linux, WinFsp on Windows.
type ProjectionFileSystem struct {
	fuse.FileSystemBase
	*projection

	mu         sync.Mutex
	nextHandle uint64
	handles    map[uint64]*FileHandle
	mounted    bool
}

func NewProjectionFileSystem(client protocol.Client) *ProjectionFileSystem {
	return &ProjectionFileSystem{
		projection: newProjection(client),
		handles:    map[uint64]*FileHandle{},
		nextHandle: 1,
	}
}

func (fsys *ProjectionFileSystem) Init() {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	fsys.mounted = true
}

func (fsys *ProjectionFileSystem) Getattr(projectionPath string, stat *fuse.Stat_t, _ uint64) int {
	entry, errno := fsys.entryForPath(projectionPath)
	if errno != 0 {
		return fuseErrno(errno)
	}

	fillStat(stat, projectionPath, entry)
	return 0
}

func (fsys *ProjectionFileSystem) Readdir(projectionPath string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, _ int64, _ uint64) int {
	entries, errno := fsys.entriesIn(projectionPath)
	if errno != 0 {
		return fuseErrno(errno)
	}

	fill(".", nil, 0)
	fill("..", nil, 0)
	for _, childEntry := range entries {
		stat := fuse.Stat_t{}
		fillStat(&stat, joinProjectionPath(projectionPath, childEntry.Name), childEntry)
		if !fill(childEntry.Name, &stat, 0) {
			break
		}
	}
	return 0
}

func (fsys *ProjectionFileSystem) Open(projectionPath string, flags int) (int, uint64) {
	entry, errno := fsys.entryForPath(projectionPath)
	if errno != 0 {
		return fuseErrno(errno), ^uint64(0)
	}
	if entry.Kind == protocol.Directory {
		return -fuse.EISDIR, ^uint64(0)
	}

	writable := openFlagsAreWritable(flags)
	if writable && !entry.Writable {
		return -fuse.EROFS, ^uint64(0)
	}

	contents := []byte{}
	if !openFlagsTruncate(flags) {
		readContents, err := fsys.contentsForPath(projectionPath)
		if err != nil {
			return fuseErrno(errnoFor(err)), ^uint64(0)
		}
		contents = readContents
	}

	handle := &FileHandle{
		client:   fsys.client,
		path:     projectionPath,
		contents: contents,
		writable: writable,
		dirty:    openFlagsTruncate(flags),
		flush:    fsys.writeProjection,
	}
	return 0, fsys.registerHandle(handle)
}

func (fsys *ProjectionFileSystem) Create(projectionPath string, flags int, _ uint32) (int, uint64) {
	flush, errno := fsys.createFile(projectionPath)
	if errno != 0 {
		return fuseErrno(errno), ^uint64(0)
	}

	handle := &FileHandle{
		path:     projectionPath,
		contents: []byte{},
		writable: openFlagsAreWritable(flags),
		flush:    flush,
	}
	return 0, fsys.registerHandle(handle)
}

func (fsys *ProjectionFileSystem) Read(_ string, buff []byte, ofst int64, fh uint64) int {
	handle, ok := fsys.handle(fh)
	if !ok {
		return -fuse.EBADF
	}

	read, errno := handle.Read(buff, ofst)
	if errno != 0 {
		return fuseErrno(errno)
	}
	return read
}

func (fsys *ProjectionFileSystem) Readlink(projectionPath string) (int, string) {
	target, errno := fsys.readlink(projectionPath)
	if errno != 0 {
		return fuseErrno(errno), ""
	}
	return 0, target
}

func (fsys *ProjectionFileSystem) Write(_ string, buff []byte, ofst int64, fh uint64) int {
	handle, ok := fsys.handle(fh)
	if !ok {
		return -fuse.EBADF
	}

	written, errno := handle.Write(buff, ofst)
	if errno != 0 {
		return fuseErrno(errno)
	}
	return written
}

func (fsys *ProjectionFileSystem) Flush(_ string, fh uint64) int {
	handle, ok := fsys.handle(fh)
	if !ok {
		return -fuse.EBADF
	}

	if errno := handle.Flush(context.Background()); errno != 0 {
		return fuseErrno(errno)
	}
	return 0
}

func (fsys *ProjectionFileSystem) Release(_ string, fh uint64) int {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	delete(fsys.handles, fh)
	return 0
}

func (fsys *ProjectionFileSystem) Truncate(projectionPath string, size int64, fh uint64) int {
	if isIgnoredMetadataPath(projectionPath) {
		return 0
	}

	if handle, ok := fsys.handle(fh); ok {
		if errno := handle.Truncate(size); errno != 0 {
			return fuseErrno(errno)
		}
		return 0
	}

	if handles := fsys.handlesForPath(projectionPath); len(handles) > 0 {
		for _, handle := range handles {
			if errno := handle.Truncate(size); errno != 0 {
				return fuseErrno(errno)
			}
		}
		return 0
	}

	if size < 0 {
		return -fuse.EINVAL
	}
	if !isWritableProjectionPath(projectionPath) {
		return -fuse.EROFS
	}

	contents, err := fsys.contentsForPath(projectionPath)
	if err != nil {
		return fuseErrno(errnoFor(err))
	}
	_ = fsys.overlay.Write(context.Background(), projectionPath, resizedContents(contents, int(size)))
	return 0
}

func (fsys *ProjectionFileSystem) Access(projectionPath string, mask uint32) int {
	entry, errno := fsys.entryForPath(projectionPath)
	if errno != 0 {
		return fuseErrno(errno)
	}
	if mask&2 != 0 && !entry.Writable {
		return -fuse.EROFS
	}
	return 0
}

func (fsys *ProjectionFileSystem) Unlink(projectionPath string) int {
	return fuseErrno(fsys.unlink(projectionPath))
}

func (fsys *ProjectionFileSystem) Mkdir(projectionPath string, _ uint32) int {
	return fuseErrno(fsys.mkdir(projectionPath))
}

func (fsys *ProjectionFileSystem) Rmdir(projectionPath string) int {
	return fuseErrno(fsys.rmdir(projectionPath))
}

func (fsys *ProjectionFileSystem) Rename(oldPath string, newPath string) int {
	return fuseErrno(fsys.rename(oldPath, newPath))
}

func (fsys *ProjectionFileSystem) registerHandle(handle *FileHandle) uint64 {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	fh := fsys.nextHandle
	fsys.nextHandle++
	fsys.handles[fh] = handle
	return fh
}

func (fsys *ProjectionFileSystem) handle(fh uint64) (*FileHandle, bool) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	handle, ok := fsys.handles[fh]
	return handle, ok
}

func (fsys *ProjectionFileSystem) handlesForPath(projectionPath string) []*FileHandle {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	handles := make([]*FileHandle, 0)
	for _, handle := range fsys.handles {
		if handle.path == projectionPath {
			handles = append(handles, handle)
		}
	}
	return handles
}

func (fsys *ProjectionFileSystem) wasMounted() bool {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	return fsys.mounted
}

// Mount mounts the projection at mountPoint through FUSE and serves it until
// it is unmounted.
func Mount(mountPoint string, client protocol.Client, config Config) error {
	if err := ensureMountPoint(mountPoint); err != nil {
		return err
	}

	fsys := NewProjectionFileSystem(client)
	fsys.logger = log.New(os.Stderr, "pharo-image-fs: ", log.LstdFlags)
	host := fuse.NewFileSystemHost(fsys)
	stop := watchImage(client, fsys.logger, func() { host.Unmount() })
	defer stop()
	if !host.Mount(mountPoint, fuseOptions(config)) {
		if fsys.wasMounted() {
			return nil
		}
		return fmt.Errorf("mount failed")
	}
	return nil
}

func fuseOptions(config Config) []string {
	options := make([]string, 0, len(config.MountOptions)*2+1)
	for _, option := range config.MountOptions {
		options = append(options, "-o", option)
	}
	if config.Debug {
		options = append(options, "-d")
	}
	return options
}

func fillStat(stat *fuse.Stat_t, projectionPath string, entry protocol.Entry) {
	stat.Mode = fuseTypeFor(entry.Kind) | permissionsFor(entry)
	stat.Size = int64(entry.Size)
	stat.Ino = stableInodeFor(projectionPath)
	if entry.Kind == protocol.Directory {
		stat.Nlink = 2
		return
	}

	stat.Nlink = 1
}

func fuseTypeFor(kind protocol.EntryKind) uint32 {
	switch kind {
	case protocol.Directory:
		return fuse.S_IFDIR
	case protocol.Symlink:
		return fuse.S_IFLNK
	default:
		return fuse.S_IFREG
	}
}

// fuseErrno answers the negated FUSE error for errno; FUSE numbers errors as
// Linux does, which Windows does not.
func fuseErrno(errno syscall.Errno) int {
	switch errno {
	case 0:
		return 0
	case syscall.ENOENT:
		return -fuse.ENOENT
	case syscall.EROFS:
		return -fuse.EROFS
	case syscall.EEXIST:
		return -fuse.EEXIST
	case syscall.ENOTDIR:
		return -fuse.ENOTDIR
	case syscall.EISDIR:
		return -fuse.EISDIR
	case syscall.EINVAL:
		return -fuse.EINVAL
	case syscall.EBADF:
		return -fuse.EBADF
	case syscall.EINTR:
		return -fuse.EINTR
	case syscall.ENOTEMPTY:
		return -fuse.ENOTEMPTY
	default:
		return -fuse.EIO
	}
}

func openFlagsAreWritable(flags int) bool {
	accessMode := flags & fuse.O_ACCMODE
	return accessMode == fuse.O_WRONLY || accessMode == fuse.O_RDWR
}

func openFlagsTruncate(flags int) bool {
	return flags&fuse.O_TRUNC != 0
}
