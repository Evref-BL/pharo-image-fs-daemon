package mount

import (
	"context"
	"hash/fnv"
	"os"
	"path"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
)

const (
	// readFreshness is how long one read of a file serves the READ calls that
	// fetch it in pieces.
	readFreshness = time.Second
	// listingFreshness is how long a directory's listing stands for its
	// modification time, which every lookup in it asks for.
	listingFreshness = time.Second
)

// maxFileSize bounds what a write or truncation may make a file: no class
// file comes near it, and running out of memory is not a panic that a
// recovery could answer.
const maxFileSize = 64 << 20

// fileSystem holds the rules a mount of the projection keeps, whatever
// protocol serves it: the ids that follow files through renames, the writes
// held for the image, the times a client trusts its caches by, and the order
// changes run in.
//
// Calls are served several at a time. Changes run one at a time, under
// changing, which each holds through its call to the image: the image
// compiles one file at a time, and no change interleaves with another's check
// and act on held writes or editor files. While a change runs, calls on the
// paths it changes, and on everything under them, wait for it, and a call
// that ran while a change of its path began runs again (see observe): no call
// sees a file halfway through a compile or a rename, and calls on other files
// go on.
type fileSystem struct {
	*projection

	uid   uint32
	gid   uint32
	clock func() time.Time
	// started is when the daemon started, on the wall clock, as every time it
	// answers is.
	started time.Time
	ids     *identities
	// listsDirectoriesOnStat makes a stat of a directory list it again once
	// its last listing is a second old, so that its time moves with what the
	// image changes: for a client that caches listings by that time.
	listsDirectoriesOnStat bool

	changing sync.Mutex

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

func newFileSystem(client protocol.Client) *fileSystem {
	started := time.Now().Round(0)
	fsys := &fileSystem{
		projection: newProjection(client),
		uid:        uint32(os.Getuid()),
		gid:        uint32(os.Getgid()),
		clock:      time.Now,
		started:    started,
		ids:        newIdentities(),
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

// openFile is a file a client opened: its id, which follows it through
// renames, and the path it was opened at, which names it in errors once it is
// gone.
type openFile struct {
	id   uint64
	path string
}

// writePlace is where the writes to a file are kept.
type writePlace int

const (
	// heldForTheImage keeps them until the image compiles them.
	heldForTheImage writePlace = iota
	// inTheOverlay keeps an editor's or the system's own file, which the image
	// never sees.
	inTheOverlay
)

// statAt answers the entry at projectionPath and when it last changed. It
// asks the image again when a stat that overlapped this one saw the file
// otherwise (see modificationTime).
func (fsys *fileSystem) statAt(projectionPath string) (protocol.Entry, time.Time, syscall.Errno) {
	for {
		fsys.mu.Lock()
		stat := fsys.stats
		fsys.mu.Unlock()
		entry, errno := fsys.entryFor(projectionPath)
		if errno != 0 {
			return protocol.Entry{}, time.Time{}, errno
		}
		if modified, ok := fsys.modificationTime(projectionPath, entry, stat); ok {
			return entry, modified, 0
		}
	}
}

// keepOff makes calls on paths, and on everything under them, wait until the
// running change ends, and those that began before it run again.
func (fsys *fileSystem) keepOff(paths ...string) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	fsys.begun++
	fsys.busy = append(fsys.busy, paths...)
	for _, projectionPath := range paths {
		fsys.marks[projectionPath] = fsys.begun
	}
}

func (fsys *fileSystem) endChange() {
	fsys.mu.Lock()
	fsys.busy = nil
	fsys.mu.Unlock()
	fsys.settled.Broadcast()
	fsys.changing.Unlock()
}

// isBusyLocked answers whether projectionPath is, or is under, a path the
// running change changes.
func (fsys *fileSystem) isBusyLocked(projectionPath string) bool {
	for _, busy := range fsys.busy {
		if _, ok := pathUnder(projectionPath, busy); ok {
			return true
		}
	}
	return false
}

// markLocked answers the number of the last change of projectionPath or of a
// directory above it.
func (fsys *fileSystem) markLocked(projectionPath string) uint64 {
	var latest uint64
	for current := projectionPath; ; current = path.Dir(current) {
		latest = max(latest, fsys.marks[current])
		if current == "/" {
			return latest
		}
	}
}

// observe runs read on the path resolve answers, once no change changes it,
// and again if a change of it began meanwhile: so read answers what held
// between two changes, never what one left halfway. resolve answers where
// what the call names is now.
func (fsys *fileSystem) observe(resolve func() (string, error), read func(projectionPath string) error) error {
	for {
		fsys.mu.Lock()
		begun := fsys.begun
		fsys.mu.Unlock()
		projectionPath, err := resolve()
		if err != nil {
			return err
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

		err = read(projectionPath)
		fsys.mu.Lock()
		unchanged := fsys.markLocked(projectionPath) <= begun
		fsys.mu.Unlock()
		if unchanged {
			return err
		}
	}
}

// commitHeld compiles the writes held for the file at projectionPath, and
// drops them either way: the file is the image's from then on. The caller
// holds changing, and keeps calls off projectionPath.
func (fsys *fileSystem) commitHeld(projectionPath string) error {
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
func (fsys *fileSystem) entryFor(projectionPath string) (protocol.Entry, syscall.Errno) {
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
func (fsys *fileSystem) contentsToRead(projectionPath string, entry protocol.Entry) ([]byte, error) {
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

// modificationTime answers when the entry at projectionPath, which the stat
// after the one numbered stat took, last changed, as far as the daemon can
// tell: the NFS client keeps its cached pages and listings while this holds
// still. It answers nothing when a stat that overlapped this one saw the
// file otherwise, as nothing says which of the two saw it last: the caller
// takes the entry again, and sees the file at least as late.
func (fsys *fileSystem) modificationTime(projectionPath string, entry protocol.Entry, stat uint64) (time.Time, bool) {
	if entry.Kind == protocol.Directory {
		if !fsys.listsDirectoriesOnStat {
			return fsys.recordedDirectoryTime(projectionPath), true
		}
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
func (fsys *fileSystem) directoryTime(projectionPath string) time.Time {
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

// recordedDirectoryTime answers when the listing of the directory at
// projectionPath last changed, as the listings served and the changes made
// through the mount recorded it.
func (fsys *fileSystem) recordedDirectoryTime(projectionPath string) time.Time {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if known, ok := fsys.listings[projectionPath]; ok {
		return known.changed
	}
	return fsys.started
}

// showHiddenClass shows the class at projectionPath again when what was to
// take its name failed, and answers whether it was hidden: an editor's backup
// rename hid it, and the image still has it, as a refused save leaves it.
func (fsys *fileSystem) showHiddenClass(projectionPath string) bool {
	if !fsys.overlay.IsHidden(projectionPath) {
		return false
	}
	fsys.overlay.Delete(projectionPath)
	fsys.namespaceChanged(projectionPath)
	return true
}

// listingFor answers the entries of the directory at projectionPath, as a
// READDIR serves them: the image's, with the files created through this
// mount that it does not have yet.
func (fsys *fileSystem) listingFor(projectionPath string) ([]protocol.Entry, syscall.Errno) {
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
func (fsys *fileSystem) recordListingLocked(projectionPath string, signature uint64) time.Time {
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
func (fsys *fileSystem) namespaceChanged(projectionPath string) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	fsys.namespaceChangedLocked(projectionPath)
}

func (fsys *fileSystem) namespaceChangedLocked(projectionPath string) {
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
func (fsys *fileSystem) contentsChanged(projectionPath string) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	fsys.contentsChangedLocked(projectionPath)
}

func (fsys *fileSystem) contentsChangedLocked(projectionPath string) {
	fsys.changed[projectionPath] = fsys.nowLocked()
	delete(fsys.reads, projectionPath)
	delete(fsys.seen, projectionPath)
}

// nowLocked answers the current time on the wall clock, which is what a
// client is sent, after every time it answered before, even when the wall
// clock steps back: so two changes in a row never share a modification time,
// and none goes back.
func (fsys *fileSystem) nowLocked() time.Time {
	now := fsys.clock().Round(0)
	if !now.After(fsys.last) {
		now = fsys.last.Add(time.Microsecond)
	}
	fsys.last = now
	return now
}

// writeAt writes data at offset into file, held in pending or in the
// overlay, wherever file is by now.
func (fsys *fileSystem) writeAt(file openFile, data []byte, offset int64) error {
	if err := checkWrite(file, data, offset); err != nil {
		return err
	}
	projectionPath, err := fsys.lockChangeOf("write", file)
	if err != nil {
		return err
	}
	defer fsys.endChange()
	return fsys.writeLocked(projectionPath, data, offset, nil)
}

// checkWrite refuses a write no file reaches, before anything is allocated.
func checkWrite(file openFile, data []byte, offset int64) error {
	if offset < 0 {
		return pathError("write", file.path, syscall.EINVAL)
	}
	if offset > maxFileSize || int64(len(data)) > maxFileSize-offset {
		return pathError("write", file.path, syscall.EFBIG)
	}
	return nil
}

// checkSize refuses a size no file reaches.
func checkSize(file openFile, size int64) error {
	if size < 0 {
		return pathError("truncate", file.path, syscall.EINVAL)
	}
	if size > maxFileSize {
		return pathError("truncate", file.path, syscall.EFBIG)
	}
	return nil
}

// lockChangeOf starts a change of file: it waits for the running change, and
// answers where file is now. endChange ends it.
func (fsys *fileSystem) lockChangeOf(op string, file openFile) (string, error) {
	fsys.changing.Lock()
	projectionPath, ok := fsys.ids.pathFor(file.id)
	if !ok {
		fsys.changing.Unlock()
		return "", pathError(op, file.path, syscall.ESTALE)
	}
	return projectionPath, nil
}

// writeLocked writes data at offset into the file at projectionPath: into
// its held writes, which start from base when it is not nil and from what
// the image has otherwise, or into the editor's file in the overlay. The
// caller holds changing.
func (fsys *fileSystem) writeLocked(projectionPath string, data []byte, offset int64, base []byte) error {
	kind, errno := fsys.writeKind(projectionPath)
	if errno != 0 {
		return pathError("write", projectionPath, errno)
	}

	if kind == inTheOverlay {
		contents, ok := fsys.overlay.Read(projectionPath)
		if !ok {
			return pathError("write", projectionPath, syscall.ENOENT)
		}
		_ = fsys.overlay.Write(context.Background(), projectionPath, patched(contents, data, offset))
		fsys.contentsChanged(projectionPath)
		return nil
	}

	if err := fsys.hold(projectionPath, base); err != nil {
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
func (fsys *fileSystem) truncate(file openFile, size int64) error {
	if err := checkSize(file, size); err != nil {
		return err
	}
	projectionPath, err := fsys.lockChangeOf("truncate", file)
	if err != nil {
		return err
	}
	defer fsys.endChange()
	if resized, err := fsys.resizeLocked(projectionPath, int(size)); resized || err != nil {
		return err
	}
	if size == 0 {
		// A save starts by emptying the file: what the image has is not needed.
		fsys.startSaveLocked(projectionPath)
		return nil
	}
	if fsys.hasSize(projectionPath, int(size)) {
		return nil
	}
	return fsys.compileCutLocked(projectionPath, int(size))
}

// resizeLocked resizes the writes held for the file at projectionPath, or
// the editor's file there, and answers whether there was one. The caller
// holds changing.
func (fsys *fileSystem) resizeLocked(projectionPath string, size int) (bool, error) {
	kind, errno := fsys.writeKind(projectionPath)
	if errno != 0 {
		return false, pathError("truncate", projectionPath, errno)
	}
	if kind == inTheOverlay {
		contents, ok := fsys.overlay.Read(projectionPath)
		if !ok {
			return false, pathError("truncate", projectionPath, syscall.ENOENT)
		}
		_ = fsys.overlay.Write(context.Background(), projectionPath, resizedContents(contents, size))
		fsys.contentsChanged(projectionPath)
		return true, nil
	}

	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	held, ok := fsys.pending[projectionPath]
	if ok {
		held.truncate(size)
	}
	return ok, nil
}

// startSaveLocked holds no bytes for the file at projectionPath, as a save
// that empties the file starts. The caller holds changing.
func (fsys *fileSystem) startSaveLocked(projectionPath string) {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	fsys.pending[projectionPath] = &heldFile{}
	fsys.changed[projectionPath] = fsys.nowLocked()
}

// hasSize answers whether the file at projectionPath, with nothing held for
// it, has size already: the image's size, or that of what this mount last
// compiled there.
func (fsys *fileSystem) hasSize(projectionPath string, size int) bool {
	fsys.mu.Lock()
	compiled, ok := fsys.compiled[projectionPath]
	fsys.mu.Unlock()
	if ok && compiled == size {
		return true
	}
	entry, errno := fsys.entryForPath(projectionPath)
	return errno == 0 && entry.Size == uint64(size)
}

// compileCutLocked compiles what the image has at projectionPath, cut to
// size. The caller holds changing.
func (fsys *fileSystem) compileCutLocked(projectionPath string, size int) error {
	fsys.keepOff(projectionPath)
	contents, err := fsys.contentsForPath(projectionPath)
	if err != nil {
		return pathError("truncate", projectionPath, errnoFor(err))
	}
	fsys.mu.Lock()
	fsys.pending[projectionPath] = &heldFile{contents: resizedContents(append([]byte(nil), contents...), size)}
	fsys.mu.Unlock()
	return fsys.commitHeld(projectionPath)
}

// writeKind answers where writes to projectionPath are kept now: held for
// the image, or in the overlay. The caller holds changing.
func (fsys *fileSystem) writeKind(projectionPath string) (writePlace, syscall.Errno) {
	fsys.mu.Lock()
	_, held := fsys.pending[projectionPath]
	fsys.mu.Unlock()
	if held {
		return heldForTheImage, 0
	}
	if _, ok := fsys.overlay.Stat(projectionPath); ok {
		return inTheOverlay, 0
	}
	if isProjectedTonelFilePath(projectionPath) {
		return heldForTheImage, 0
	}
	return 0, syscall.EROFS
}

// hold makes sure writes to projectionPath are held, starting from base
// when none are yet, or from what the image has when base is nil. The caller
// holds changing.
func (fsys *fileSystem) hold(projectionPath string, base []byte) error {
	fsys.mu.Lock()
	_, ok := fsys.pending[projectionPath]
	fsys.mu.Unlock()
	if ok {
		return nil
	}

	contents := base
	if contents == nil {
		var err error
		if contents, err = fsys.contentsForPath(projectionPath); err != nil {
			return pathError("write", projectionPath, errnoFor(err))
		}
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

func pathError(op string, projectionPath string, errno syscall.Errno) error {
	return &os.PathError{Op: op, Path: projectionPath, Err: errno}
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
