//go:build linux || fusetest

package mount

import (
	"context"
	"encoding/binary"
	"io"
	"log"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// These tests drive go-fuse's bridge as the Linux kernel does, without
// mounting: lookups by node id, OPEN with O_TRUNC, FLUSH at every close of
// every descriptor, RELEASE after the last, which the kernel sends late.
// Run them off Linux with -tags fusetest.

const (
	rootNode = 1
	// writer is the process the tests write as; child is another one.
	writer = 100
	child  = 200
	// The open flags of Linux, which the kernel sends whatever the system
	// the tests run on.
	openReadOnly  = 0
	openWriteOnly = 1
	openReadWrite = 2
	openCreate    = 0o100
	openExclusive = 0o200
	openTruncate  = 0o1000
	openAppend    = 0o2000
)

// testKernel speaks to the adapter the way Linux's FUSE module does.
type testKernel struct {
	t       *testing.T
	fsys    *fuseFileSystem
	raw     fuse.RawFileSystem
	notices *recordedNotices
}

func newTestKernel(t *testing.T, client protocol.Client) *testKernel {
	t.Helper()
	fsys := newFuseFileSystem(client)
	// The tests' callers are processes.
	fsys.threadGroupOf = func(thread uint32) uint32 { return thread }
	options := fuseOptions(fsys, "test", nil, false, log.New(io.Discard, "", 0))
	notices := &recordedNotices{}
	options.ServerCallbacks = notices
	return &testKernel{t: t, fsys: fsys, raw: fs.NewNodeFS(fsys.root(), options), notices: notices}
}

func header(node uint64, process uint32) fuse.InHeader {
	return fuse.InHeader{NodeId: node, Caller: fuse.Caller{Pid: process}}
}

// lookup answers the node at projectionPath, or the status of the lookup
// that failed.
func (k *testKernel) lookup(projectionPath string) (uint64, fuse.Status) {
	node := uint64(rootNode)
	for _, name := range strings.Split(strings.Trim(projectionPath, "/"), "/") {
		if name == "" {
			continue
		}
		var out fuse.EntryOut
		in := header(node, writer)
		if status := k.raw.Lookup(nil, &in, name, &out); status != fuse.OK {
			return 0, status
		}
		node = out.NodeId
	}
	return node, fuse.OK
}

func (k *testKernel) node(projectionPath string) uint64 {
	k.t.Helper()
	node, status := k.lookup(projectionPath)
	if status != fuse.OK {
		k.t.Fatalf("lookup %s: %v", projectionPath, status)
	}
	return node
}

type openedFile struct {
	node  uint64
	fh    uint64
	flags uint32
}

func (k *testKernel) open(projectionPath string, flags uint32, process uint32) (openedFile, fuse.Status) {
	node := k.node(projectionPath)
	in := fuse.OpenIn{InHeader: header(node, process), Flags: flags}
	var out fuse.OpenOut
	status := k.raw.Open(nil, &in, &out)
	return openedFile{node: node, fh: out.Fh, flags: out.OpenFlags}, status
}

func (k *testKernel) mustOpen(projectionPath string, flags uint32, process uint32) openedFile {
	k.t.Helper()
	file, status := k.open(projectionPath, flags, process)
	if status != fuse.OK {
		k.t.Fatalf("open %s: %v", projectionPath, status)
	}
	return file
}

func (k *testKernel) create(projectionPath string, flags uint32, process uint32) (openedFile, fuse.Status) {
	directory, name := splitProjectionPath(projectionPath)
	in := fuse.CreateIn{InHeader: header(k.node(directory), process), Flags: flags | openCreate, Mode: 0o644}
	var out fuse.CreateOut
	status := k.raw.Create(nil, &in, name, &out)
	return openedFile{node: out.NodeId, fh: out.Fh, flags: out.OpenFlags}, status
}

func (k *testKernel) mustCreate(projectionPath string, flags uint32, process uint32) openedFile {
	k.t.Helper()
	file, status := k.create(projectionPath, flags, process)
	if status != fuse.OK {
		k.t.Fatalf("create %s: %v", projectionPath, status)
	}
	return file
}

func (k *testKernel) write(file openedFile, offset uint64, data string) fuse.Status {
	in := fuse.WriteIn{InHeader: header(file.node, writer), Fh: file.fh, Offset: offset, Size: uint32(len(data))}
	_, status := k.raw.Write(nil, &in, []byte(data))
	return status
}

func (k *testKernel) mustWrite(file openedFile, offset uint64, data string) {
	k.t.Helper()
	if status := k.write(file, offset, data); status != fuse.OK {
		k.t.Fatalf("write: %v", status)
	}
}

func (k *testKernel) read(file openedFile) (string, fuse.Status) {
	in := fuse.ReadIn{InHeader: header(file.node, writer), Fh: file.fh, Size: 1 << 20}
	result, status := k.raw.Read(nil, &in, make([]byte, 1<<20))
	if status != fuse.OK {
		return "", status
	}
	data, status := result.Bytes(make([]byte, 1<<20))
	return string(data), status
}

// readFile reads projectionPath as cat does.
func (k *testKernel) readFile(projectionPath string) string {
	k.t.Helper()
	file := k.mustOpen(projectionPath, openReadOnly, writer)
	defer k.release(file)
	contents, status := k.read(file)
	if status != fuse.OK {
		k.t.Fatalf("read %s: %v", projectionPath, status)
	}
	return contents
}

// close is a close(2) of one descriptor by process: a FLUSH, unless the
// open asked for none.
func (k *testKernel) close(file openedFile, process uint32) fuse.Status {
	if file.flags&fuse.FOPEN_NOFLUSH != 0 {
		return fuse.OK
	}
	in := fuse.FlushIn{InHeader: header(file.node, process), Fh: file.fh}
	return k.raw.Flush(nil, &in)
}

func (k *testKernel) fsync(file openedFile) fuse.Status {
	in := fuse.FsyncIn{InHeader: header(file.node, writer), Fh: file.fh}
	return k.raw.Fsync(nil, &in)
}

// release is the RELEASE after a file's last descriptor went.
func (k *testKernel) release(file openedFile) {
	in := fuse.ReleaseIn{InHeader: header(file.node, writer), Fh: file.fh}
	k.raw.Release(nil, &in)
}

// save writes contents to projectionPath as an editor does: open with
// O_TRUNC, write, close; it answers what the close answered.
func (k *testKernel) save(projectionPath string, contents string) fuse.Status {
	k.t.Helper()
	file := k.mustOpen(projectionPath, openWriteOnly|openTruncate, writer)
	defer k.release(file)
	k.mustWrite(file, 0, contents)
	return k.close(file, writer)
}

// setSize sets a size through file, as ftruncate does, or by path when file
// is nil, as truncate(2) does.
func (k *testKernel) setSize(node uint64, file *openedFile, size uint64) fuse.Status {
	in := fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: header(node, writer), Valid: fuse.FATTR_SIZE, Size: size}}
	if file != nil {
		in.Valid |= fuse.FATTR_FH
		in.Fh = file.fh
	}
	var out fuse.AttrOut
	return k.raw.SetAttr(nil, &in, &out)
}

func (k *testKernel) getattr(node uint64) (fuse.Attr, fuse.Status) {
	in := fuse.GetAttrIn{InHeader: header(node, writer)}
	var out fuse.AttrOut
	status := k.raw.GetAttr(nil, &in, &out)
	return out.Attr, status
}

func (k *testKernel) unlink(projectionPath string) fuse.Status {
	directory, name := splitProjectionPath(projectionPath)
	in := header(k.node(directory), writer)
	return k.raw.Unlink(nil, &in, name)
}

func (k *testKernel) rename(oldPath string, newPath string, flags uint32) fuse.Status {
	oldDirectory, oldName := splitProjectionPath(oldPath)
	newDirectory, newName := splitProjectionPath(newPath)
	in := fuse.RenameIn{InHeader: header(k.node(oldDirectory), writer), Newdir: k.node(newDirectory), Flags: flags}
	return k.raw.Rename(nil, &in, oldName, newName)
}

// list answers the names a READDIR of projectionPath answers, without "."
// and "..".
func (k *testKernel) list(projectionPath string) []string {
	k.t.Helper()
	node := k.node(projectionPath)
	in := fuse.OpenIn{InHeader: header(node, writer)}
	var opened fuse.OpenOut
	if status := k.raw.OpenDir(nil, &in, &opened); status != fuse.OK {
		k.t.Fatalf("opendir %s: %v", projectionPath, status)
	}
	defer k.raw.ReleaseDir(&fuse.ReleaseIn{InHeader: header(node, writer), Fh: opened.Fh})
	buffer := make([]byte, 1<<16)
	read := fuse.ReadIn{InHeader: header(node, writer), Fh: opened.Fh, Size: uint32(len(buffer))}
	if status := k.raw.ReadDir(nil, &read, fuse.NewDirEntryList(buffer, 0)); status != fuse.OK {
		k.t.Fatalf("readdir %s: %v", projectionPath, status)
	}
	names := []string{}
	for at := 0; at+24 <= len(buffer); {
		length := int(binary.LittleEndian.Uint32(buffer[at+16:]))
		if length == 0 {
			break
		}
		name := string(buffer[at+24 : at+24+length])
		if name != "." && name != ".." {
			names = append(names, name)
		}
		at += (24 + length + 7) &^ 7
	}
	return names
}

func splitProjectionPath(projectionPath string) (string, string) {
	index := strings.LastIndex(projectionPath, "/")
	if index == 0 {
		return "/", projectionPath[1:]
	}
	return projectionPath[:index], projectionPath[index+1:]
}

// recordedNotices records what the adapter tells the kernel.
type recordedNotices struct {
	mu         sync.Mutex
	attributes []uint64
}

func (r *recordedNotices) InodeNotify(node uint64, offset int64, _ int64) fuse.Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	if offset < 0 {
		r.attributes = append(r.attributes, node)
	}
	return fuse.OK
}

func (r *recordedNotices) droppedAttributesOf(node uint64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, each := range r.attributes {
		if each == node {
			count++
		}
	}
	return count
}

func (r *recordedNotices) DeleteNotify(uint64, uint64, string) fuse.Status { return fuse.ENOSYS }
func (r *recordedNotices) EntryNotify(uint64, string) fuse.Status          { return fuse.ENOSYS }
func (r *recordedNotices) InodeRetrieveCache(uint64, int64, []byte) (int, fuse.Status) {
	return 0, fuse.ENOSYS
}
func (r *recordedNotices) InodeNotifyStoreCache(uint64, int64, []byte) fuse.Status {
	return fuse.ENOSYS
}

func TestFUSEListsAndNumbersFilesAsLookupsDo(t *testing.T) {
	k := newTestKernel(t, newMemoryClient(map[string]string{objectPath: "source"}))
	if names := k.list("/tonel/Kernel"); !slices.Equal(names, []string{"Object.class.st"}) {
		t.Fatalf("listing: %v", names)
	}
	attributes, status := k.getattr(k.node(objectPath))
	if status != fuse.OK || attributes.Size != uint64(len("source")) || attributes.Ino != k.fsys.ids.idFor(objectPath) {
		t.Fatalf("the file's attributes should carry its size and id, got %+v %v", attributes, status)
	}
	if attributes.Mtime == 0 {
		t.Fatalf("a file should have a time")
	}
}

func TestFUSESaveCompilesAtClose(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "an older, longer source"})
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openWriteOnly|openTruncate, writer)
	k.mustWrite(file, 0, "new ")
	k.mustWrite(file, 4, "source")
	if writes := client.writes(); len(writes) != 0 {
		t.Fatalf("nothing should compile before the close, got %v", writes)
	}
	if contents := k.readFile(objectPath); contents != "new source" {
		t.Fatalf("held writes should read as written, read %q", contents)
	}
	beforeClose := k.notices.droppedAttributesOf(file.node)
	if status := k.close(file, writer); status != fuse.OK {
		t.Fatalf("close: %v", status)
	}
	if writes := client.writes(); !slices.Equal(writes, []string{objectPath + "=new source"}) {
		t.Fatalf("the close should compile once, got %v", writes)
	}
	if k.notices.droppedAttributesOf(file.node) == beforeClose {
		t.Fatalf("a compile should drop the attributes the kernel has for the file")
	}
	_ = k.notices.droppedAttributesOf(file.node)
	k.release(file)
	if writes := client.writes(); len(writes) != 1 {
		t.Fatalf("the release should compile nothing more, got %v", writes)
	}
}

func TestFUSERefusedSaveFailsTheCloseAndShowsTheImagesVersion(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openWriteOnly|openTruncate, writer)
	k.mustWrite(file, 0, "syntax error")
	if status := k.fsync(file); status != fuse.EIO {
		t.Fatalf("fsync should report the refusal, got %v", status)
	}
	if status := k.close(file, writer); status != fuse.EIO {
		t.Fatalf("the close should report the refusal again, got %v", status)
	}
	// Before the kernel's late RELEASE.
	if contents := k.readFile(objectPath); contents != "old source" {
		t.Fatalf("a refused save should leave the image's version, read %q", contents)
	}
	if latest := k.readFile("/errors/latest.txt"); !strings.Contains(latest, "syntax error") {
		t.Fatalf("/errors should tell the refusal, read %q", latest)
	}
	k.release(file)
}

func TestFUSEReadOnlyOpenNeverCompiles(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "source"})
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openReadOnly, writer)
	if file.flags&fuse.FOPEN_NOFLUSH == 0 {
		t.Fatalf("a read-only open should ask for no flush")
	}
	if k.notices.droppedAttributesOf(file.node) == 0 {
		t.Fatalf("an open should drop the attributes the kernel has, as a client checks at open")
	}
	if contents, _ := k.read(file); contents != "source" {
		t.Fatalf("read %q", contents)
	}
	k.release(file)
	if writes := client.writes(); len(writes) != 0 {
		t.Fatalf("reading should compile nothing, got %v", writes)
	}
}

func TestFUSEAChildsCloseCompilesNothing(t *testing.T) {
	// { printf a; cat b; } > Object.class.st: the shell opens the file,
	// moves the descriptor to its output and closes the first one, before
	// anything is written; its children write and close their copies; the
	// shell restores its output last.
	client := &emptyRefusingClient{newMemoryClient(map[string]string{objectPath: "an older and longer source"})}
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openWriteOnly|openTruncate, writer)
	if status := k.close(file, writer); status != fuse.OK {
		t.Fatalf("closing the descriptor a shell moved should not fail, got %v", status)
	}
	k.mustWrite(file, 0, "first half, ")
	if status := k.close(file, child); status != fuse.OK {
		t.Fatalf("a child's close should not fail, got %v", status)
	}
	if writes := client.writes(); len(writes) != 0 {
		t.Fatalf("a child's close should compile nothing, got %v", writes)
	}
	k.mustWrite(file, uint64(len("first half, ")), "second half")
	if status := k.close(file, writer); status != fuse.OK {
		t.Fatalf("close: %v", status)
	}
	if writes := client.writes(); !slices.Equal(writes, []string{objectPath + "=first half, second half"}) {
		t.Fatalf("the opener's close should compile the whole file once, got %v", writes)
	}
	k.release(file)
}

func TestFUSEWhatNoCloseCompiledCompilesAtRelease(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openWriteOnly|openTruncate, writer)
	k.mustWrite(file, 0, "written by a child")
	k.close(file, child)
	k.release(file)
	if writes := client.writes(); !slices.Equal(writes, []string{objectPath + "=written by a child"}) {
		t.Fatalf("the release should compile what no close did, got %v", writes)
	}
}

func TestFUSEAWriterGoesOnFromWhatItsLastCompileTook(t *testing.T) {
	client := &prefixingClient{newMemoryClient(map[string]string{objectPath: "old source"})}
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openReadWrite|openTruncate, writer)
	k.mustWrite(file, 0, "first, ")
	if status := k.fsync(file); status != fuse.OK {
		t.Fatalf("fsync: %v", status)
	}
	k.mustWrite(file, uint64(len("first, ")), "second")
	k.close(file, writer)
	k.release(file)
	if contents := client.contents(objectPath); contents != "\"rendered\"\nfirst, second" {
		t.Fatalf("the writer's later writes should go on from its own text, not the image's rendering, image has %q", contents)
	}

	// Once another process saved the file, a writer goes on from that.
	file = k.mustOpen(objectPath, openReadWrite|openTruncate, writer)
	k.mustWrite(file, 0, "mine, ")
	k.fsync(file)
	if status := k.save(objectPath, "theirs"); status != fuse.OK {
		t.Fatalf("save: %v", status)
	}
	k.mustWrite(file, uint64(len("\"rendered\"\ntheirs")), "!")
	k.close(file, writer)
	k.release(file)
	if contents := client.contents(objectPath); contents != "\"rendered\"\n\"rendered\"\ntheirs!" {
		t.Fatalf("a writer should not revert another process's save, image has %q", contents)
	}
}

func TestFUSEAppendsAtTheEndTheFileHas(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	k := newTestKernel(t, client)
	// The image lengthened the file since the kernel learned its size.
	client.put(objectPath, "a longer old source")
	file := k.mustOpen(objectPath, openWriteOnly|openAppend, writer)
	k.mustWrite(file, uint64(len("old source")), "!")
	k.close(file, writer)
	k.release(file)
	if contents := client.contents(objectPath); contents != "a longer old source!" {
		t.Fatalf("an append should land at the file's end, image has %q", contents)
	}
}

// An append past the largest file is refused, and leaves nothing held: the
// image's later changes still show.
func TestFUSEARefusedAppendHoldsNothing(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: strings.Repeat("x", maxFileSize-1)})
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openWriteOnly|openAppend, writer)
	if status := k.write(file, 0, "ab"); status != fuse.Status(syscall.EFBIG) {
		t.Fatalf("an append past the largest file should be refused, got %v", status)
	}
	client.put(objectPath, "changed in the image")
	if contents := k.readFile(objectPath); contents != "changed in the image" {
		t.Fatalf("a refused append should hold nothing, read %d bytes", len(contents))
	}
	k.close(file, writer)
	k.release(file)
}

func TestFUSESizesSetWithAndWithoutAFile(t *testing.T) {
	// f.write(source); f.truncate(); f.close()
	client := newMemoryClient(map[string]string{objectPath: "an old source"})
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openReadWrite, writer)
	k.mustWrite(file, 0, "new source")
	if status := k.setSize(file.node, &file, uint64(len("new source"))); status != fuse.OK {
		t.Fatalf("ftruncate: %v", status)
	}
	k.close(file, writer)
	k.release(file)
	if writes := client.writes(); !slices.Equal(writes, []string{objectPath + "=new source"}) {
		t.Fatalf("a write then a truncation should compile once, got %v", writes)
	}

	// truncate -s at the size the file has: an ftruncate that changes nothing.
	file = k.mustOpen(objectPath, openWriteOnly, writer)
	if status := k.setSize(file.node, &file, uint64(len("new source"))); status != fuse.OK {
		t.Fatalf("ftruncate: %v", status)
	}
	k.close(file, writer)
	k.release(file)
	if writes := client.writes(); len(writes) != 1 {
		t.Fatalf("a size the file has should change nothing, got %v", writes)
	}

	// truncate(2): nothing follows it, so it compiles now.
	if status := k.setSize(k.node(objectPath), nil, 3); status != fuse.OK {
		t.Fatalf("truncate: %v", status)
	}
	if contents := client.contents(objectPath); contents != "new" {
		t.Fatalf("a size set alone should compile the file cut, image has %q", contents)
	}
	refusing := newTestKernel(t, &echoRefusingClient{newMemoryClient(map[string]string{objectPath: "old source"})})
	if status := refusing.setSize(refusing.node(objectPath), nil, 3); status != fuse.EIO {
		t.Fatalf("a refused size should fail truncate(2), got %v", status)
	}
	if status := k.setSize(k.node(objectPath), nil, maxFileSize+1); status != fuse.Status(syscall.EFBIG) {
		t.Fatalf("a size past the largest file should be refused, got %v", status)
	}
}

func TestFUSECreatedFileClosedUnwrittenLeaves(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "source"})
	k := newTestKernel(t, client)
	created := "/tonel/Kernel/New.class.st"
	file := k.mustCreate(created, openWriteOnly, writer)
	if names := k.list("/tonel/Kernel"); !slices.Contains(names, "New.class.st") {
		t.Fatalf("a created file should be listed, got %v", names)
	}
	k.close(file, writer)
	k.release(file)
	if writes := client.writes(); len(writes) != 0 {
		t.Fatalf("creating is not writing, got %v", writes)
	}
	if names := k.list("/tonel/Kernel"); slices.Contains(names, "New.class.st") {
		t.Fatalf("a class file closed unwritten should leave, got %v", names)
	}
}

func TestFUSERefusedNewClassLeavesItsDirectory(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "source"})
	k := newTestKernel(t, client)
	created := "/tonel/Kernel/New.class.st"
	// As a shell's > creates it; a file created in place compiles at its
	// write (see TestFUSEANewClassWrittenInPlaceHearsItsRefusal).
	file := k.mustCreate(created, openWriteOnly|openTruncate, writer)
	k.mustWrite(file, 0, "syntax error")
	if status := k.close(file, writer); status != fuse.EIO {
		t.Fatalf("the close should report the refusal, got %v", status)
	}
	k.release(file)
	if names := k.list("/tonel/Kernel"); slices.Contains(names, "New.class.st") {
		t.Fatalf("a refused new class should leave its directory, got %v", names)
	}
}

func TestFUSEToolsAndEditorsSaveOnce(t *testing.T) {
	saves := map[string]func(*testKernel) fuse.Status{
		// GNU sed -i: a temp file beside the file, renamed over it.
		"sed -i": func(k *testKernel) fuse.Status {
			temp := "/tonel/Kernel/sedAb12Cd"
			file := k.mustCreate(temp, openWriteOnly|openExclusive, writer)
			k.mustWrite(file, 0, "new source")
			k.close(file, writer)
			k.release(file)
			return k.rename(temp, objectPath, 0)
		},
		// vim: the file goes to a backup name, a new one is written.
		"vim": func(k *testKernel) fuse.Status {
			if status := k.rename(objectPath, objectPath+"~", 0); status != fuse.OK {
				return status
			}
			file := k.mustCreate(objectPath, openWriteOnly|openTruncate, writer)
			k.mustWrite(file, 0, "new source")
			status := k.fsync(file)
			if closed := k.close(file, writer); status == fuse.OK {
				status = closed
			}
			k.release(file)
			if status == fuse.OK {
				status = k.unlink(objectPath + "~")
			}
			return status
		},
		// JetBrains' safe write.
		"jetbrains": func(k *testKernel) fuse.Status {
			temp := objectPath + "___jb_tmp___"
			file := k.mustCreate(temp, openWriteOnly, writer)
			k.mustWrite(file, 0, "new source")
			k.close(file, writer)
			k.release(file)
			if status := k.rename(objectPath, objectPath+"___jb_old___", 0); status != fuse.OK {
				return status
			}
			if status := k.rename(temp, objectPath, 0); status != fuse.OK {
				return status
			}
			return k.unlink(objectPath + "___jb_old___")
		},
	}
	for tool, save := range saves {
		client := newMemoryClient(map[string]string{objectPath: "old source"})
		k := newTestKernel(t, client)
		if status := save(k); status != fuse.OK {
			t.Fatalf("%s: %v", tool, status)
		}
		if writes := client.writes(); !slices.Equal(writes, []string{objectPath + "=new source"}) {
			t.Fatalf("%s should compile once, onto the class, got %v", tool, writes)
		}
		if names := k.list("/tonel/Kernel"); !slices.Equal(names, []string{"Object.class.st"}) {
			t.Fatalf("%s should leave only the class, got %v", tool, names)
		}
	}
}

func TestFUSERenameSaveKeepsTheTempFilesNumber(t *testing.T) {
	k := newTestKernel(t, newMemoryClient(map[string]string{objectPath: "old source"}))
	temp := "/tonel/Kernel/sedAb12Cd"
	file := k.mustCreate(temp, openWriteOnly, writer)
	k.mustWrite(file, 0, "new source")
	k.close(file, writer)
	k.release(file)
	saved, _ := k.getattr(file.node)
	if status := k.rename(temp, objectPath, 0); status != fuse.OK {
		t.Fatalf("rename: %v", status)
	}
	renamed, _ := k.getattr(k.node(objectPath))
	if renamed.Ino != saved.Ino {
		t.Fatalf("a file renamed over a class should keep its number, %d became %d", saved.Ino, renamed.Ino)
	}
	if k.node(objectPath) != file.node {
		t.Fatalf("the kernel's node should follow the renamed file")
	}
}

func TestFUSERefusedRenameSaveShowsTheBackedUpClassAgain(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	k := newTestKernel(t, client)
	temp := objectPath + "___jb_tmp___"
	file := k.mustCreate(temp, openWriteOnly, writer)
	k.mustWrite(file, 0, "syntax error")
	k.close(file, writer)
	k.release(file)
	k.rename(objectPath, objectPath+"___jb_old___", 0)
	if status := k.rename(temp, objectPath, 0); status != fuse.EIO {
		t.Fatalf("the refused save should fail the rename, got %v", status)
	}
	if contents := k.readFile(objectPath); contents != "old source" {
		t.Fatalf("the class should show again, read %q", contents)
	}
}

func TestFUSERemovingAnOpenFileRemovesItsClassAndLeavesItsDescriptorsStale(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source", "/tonel/Kernel/Other.class.st": "other"})
	k := newTestKernel(t, client)
	reader := k.mustOpen(objectPath, openReadOnly, writer)
	writable := k.mustOpen(objectPath, openReadWrite|openTruncate, child)
	k.mustWrite(writable, 0, "held")
	if status := k.unlink(objectPath); status != fuse.OK {
		t.Fatalf("unlink: %v", status)
	}
	if client.has(objectPath) {
		t.Fatalf("removing a file should remove its class at once")
	}
	// As for a file removed on an NFS server.
	if _, status := k.read(reader); status != fuse.Status(syscall.ESTALE) {
		t.Fatalf("a removed file's descriptor should read stale, got %v", status)
	}
	if _, status := k.getattr(reader.node); status != fuse.Status(syscall.ESTALE) {
		t.Fatalf("a removed file's descriptor should stat stale, got %v", status)
	}
	if status := k.write(writable, 0, "more"); status != fuse.Status(syscall.ESTALE) {
		t.Fatalf("a removed file's descriptor should write stale, got %v", status)
	}
	if status := k.close(writable, child); status != fuse.OK {
		t.Fatalf("close: %v", status)
	}
	k.release(writable)
	k.release(reader)
	if writes := client.writes(); len(writes) != 0 {
		t.Fatalf("a removed file compiles nothing, got %v", writes)
	}
}

func TestFUSEReplacingAnOpenFileLeavesItsDescriptorsStale(t *testing.T) {
	k := newTestKernel(t, newMemoryClient(map[string]string{objectPath: "old source"}))
	reader := k.mustOpen(objectPath, openReadOnly, writer)
	temp := "/tonel/Kernel/sedAb12Cd"
	file := k.mustCreate(temp, openWriteOnly, writer)
	k.mustWrite(file, 0, "new source")
	k.close(file, writer)
	k.release(file)
	if status := k.rename(temp, objectPath, 0); status != fuse.OK {
		t.Fatalf("rename: %v", status)
	}
	if _, status := k.read(reader); status != fuse.Status(syscall.ESTALE) {
		t.Fatalf("a descriptor of the replaced file should read stale, got %v", status)
	}
	k.release(reader)
	if contents := k.readFile(objectPath); contents != "new source" {
		t.Fatalf("the name should read the new file, got %q", contents)
	}
}

func TestFUSERenameFlags(t *testing.T) {
	other := "/tonel/Kernel/Other.class.st"
	k := newTestKernel(t, newMemoryClient(map[string]string{objectPath: "source", other: "other"}))
	if status := k.rename(other, objectPath, renameNoReplace); status != fuse.Status(syscall.EEXIST) {
		t.Fatalf("a rename that must not replace should be refused, got %v", status)
	}
	if status := k.rename(other, objectPath, renameExchange); status != fuse.EINVAL {
		t.Fatalf("an exchange should be refused, got %v", status)
	}
}

func TestFUSEAnswersOtherFilesWhileACompileRuns(t *testing.T) {
	other := "/tonel/Kernel/Other.class.st"
	client := newGatedClient(map[string]string{objectPath: "old source", other: "other"})
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openWriteOnly|openTruncate, writer)
	k.mustWrite(file, 0, "a longer new source")
	otherNode := k.node(other)
	objectNode := k.node(objectPath)

	started, open := client.holdNext("write")
	defer open()
	closed := make(chan fuse.Status, 1)
	go func() { closed <- k.close(file, writer) }()
	waitHeld(t, started)
	if _, status := k.getattr(otherNode); status != fuse.OK {
		t.Fatalf("another file should answer during the compile, got %v", status)
	}
	stat := make(chan fuse.Attr, 1)
	go func() {
		attributes, _ := k.getattr(objectNode)
		stat <- attributes
	}()
	select {
	case <-stat:
		t.Fatalf("a stat of the compiling file should wait for the compile")
	case <-closed:
		t.Fatalf("the close should wait for the compile")
	case <-time.After(100 * time.Millisecond):
	}
	open()
	if status := within(t, closed, "close"); status != fuse.OK {
		t.Fatalf("close: %v", status)
	}
	if attributes := within(t, stat, "stat"); attributes.Size != uint64(len("a longer new source")) {
		t.Fatalf("the stat should answer the compiled file, got size %d", attributes.Size)
	}
	k.release(file)
}

func TestFUSEWritesAndSizesPastTheLargestFile(t *testing.T) {
	k := newTestKernel(t, newMemoryClient(map[string]string{objectPath: "source"}))
	file := k.mustOpen(objectPath, openWriteOnly, writer)
	if status := k.write(file, maxFileSize, "x"); status != fuse.Status(syscall.EFBIG) {
		t.Fatalf("a write past the largest file should be refused, got %v", status)
	}
	if status := k.setSize(file.node, &file, maxFileSize+1); status != fuse.Status(syscall.EFBIG) {
		t.Fatalf("a size past the largest file should be refused, got %v", status)
	}
	k.release(file)
}

func TestFUSELinksAreRefusedAsUnsupported(t *testing.T) {
	k := newTestKernel(t, newMemoryClient(map[string]string{objectPath: "source"}))
	in := header(k.node("/tonel/Kernel"), writer)
	var out fuse.EntryOut
	if status := k.raw.Symlink(nil, &in, "Object.class.st", ".#Object.class.st", &out); status != fuse.Status(syscall.EPERM) {
		t.Fatalf("a symbolic link should be refused with EPERM, got %v", status)
	}
}

func TestFUSEMountOptions(t *testing.T) {
	fsys := newFuseFileSystem(newMemoryClient(nil))
	name, others := mountNameAndOptions([]string{"volname=my image", "noatime"})
	options := fuseOptions(fsys, name, others, false, log.New(io.Discard, "", 0))
	if options.Name != "pharo-image-fs" || options.FsName != "my-image" {
		t.Fatalf("the type should be fuse.pharo-image-fs and the source the volume name, got %q %q", options.Name, options.FsName)
	}
	if !slices.Equal(options.Options, []string{"noatime"}) {
		t.Fatalf("volname should not reach fusermount3, got %v", options.Options)
	}
	if !options.DisableXAttrs || !options.DisableReadDirPlus || options.ExtraCapabilities&fuse.CAP_ATOMIC_O_TRUNC == 0 {
		t.Fatalf("options: %+v", options.MountOptions)
	}
	if options.NegativeTimeout != nil || options.EntryTimeout == nil || options.AttrTimeout == nil {
		t.Fatalf("names and attributes should last a second, and ENOENT not at all")
	}
	if status := options.PanicHandler("boom"); status != fuse.EIO {
		t.Fatalf("a panic should answer EIO, got %v", status)
	}
}

// emptyRefusingClient refuses to compile an empty class file, as the image
// does.
type emptyRefusingClient struct {
	*memoryClient
}

func (c *emptyRefusingClient) Write(ctx context.Context, filePath string, contents []byte) (protocol.WriteResult, error) {
	if len(contents) == 0 {
		return protocol.WriteResult{}, &protocol.Error{StatusCode: http.StatusBadRequest, Message: "Expected exactly one class definition, got 0"}
	}
	return c.memoryClient.Write(ctx, filePath, contents)
}

// cutMethodRefusingClient refuses a class file whose last method is not
// closed, as the image refuses a file cut in the middle of a method.
type cutMethodRefusingClient struct {
	*memoryClient
}

func (c *cutMethodRefusingClient) Write(ctx context.Context, filePath string, contents []byte) (protocol.WriteResult, error) {
	if !strings.HasSuffix(string(contents), "]\n") {
		return protocol.WriteResult{}, &protocol.Error{StatusCode: http.StatusBadRequest, Message: "Unmatched [ in method body"}
	}
	return c.memoryClient.Write(ctx, filePath, contents)
}

// prefixingClient compiles what it is given and renders it with a comment
// first, as an image that renders a file its own way does.
type prefixingClient struct {
	*memoryClient
}

func (c *prefixingClient) Write(ctx context.Context, filePath string, contents []byte) (protocol.WriteResult, error) {
	return c.memoryClient.Write(ctx, filePath, append([]byte("\"rendered\"\n"), contents...))
}

// listingCountingClient counts the listings the image is asked for.
type listingCountingClient struct {
	*memoryClient
	listings atomic.Int64
}

func (c *listingCountingClient) List(ctx context.Context, directory string) ([]protocol.Entry, error) {
	c.listings.Add(1)
	return c.memoryClient.List(ctx, directory)
}

func TestFUSEAShellRedirectionCompilesOnce(t *testing.T) {
	// cat > Object.class.st <<EOF: the shell opens truncating, moves the
	// descriptor and closes the first one, then cat writes and closes.
	client := &emptyRefusingClient{newMemoryClient(map[string]string{objectPath: "an older and much longer source"})}
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openWriteOnly|openTruncate, writer)
	if status := k.close(file, writer); status != fuse.OK {
		t.Fatalf("closing the moved descriptor should not fail, got %v", status)
	}
	k.mustWrite(file, 0, "new source")
	if status := k.close(file, writer); status != fuse.OK {
		t.Fatalf("close: %v", status)
	}
	k.release(file)
	if writes := client.writes(); !slices.Equal(writes, []string{objectPath + "=new source"}) {
		t.Fatalf("the save should compile once, with nothing of the old text, got %v", writes)
	}

	// : > Object.class.st empties the file, which compiles once it is closed
	// for good.
	file = k.mustOpen(objectPath, openWriteOnly|openTruncate, writer)
	k.close(file, writer)
	if writes := client.writes(); len(writes) != 1 {
		t.Fatalf("a truncation alone should not compile at a close, got %v", writes)
	}
	k.release(file)
	if latest := k.readFile("/errors/latest.txt"); !strings.Contains(latest, "Expected exactly one class definition") {
		t.Fatalf("emptying the file should compile at its release, /errors has %q", latest)
	}
}

func TestFUSEAWriteAfterAnEmptyCompileGoesOnFromEmpty(t *testing.T) {
	client := &emptyRefusingClient{newMemoryClient(map[string]string{objectPath: "an older and much longer source"})}
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openWriteOnly|openTruncate, writer)
	if status := k.fsync(file); status != fuse.EIO {
		t.Fatalf("fsync should report the image's refusal of an empty class, got %v", status)
	}
	k.mustWrite(file, 0, "new source")
	k.close(file, writer)
	k.release(file)
	if contents := client.contents(objectPath); contents != "new source" {
		t.Fatalf("writes after a compile of an empty file should go on from empty, image has %q", contents)
	}
}

func TestFUSEARefusedRenameKeepsTheTargetsWriter(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	k := newTestKernel(t, client)
	target := k.mustOpen(objectPath, openWriteOnly|openTruncate, writer)
	k.mustWrite(target, 0, "the writer's source")
	temp := "/tonel/Kernel/sedAb12Cd"
	file := k.mustCreate(temp, openWriteOnly, child)
	k.mustWrite(file, 0, "syntax error")
	k.close(file, child)
	k.release(file)
	if status := k.rename(temp, objectPath, 0); status != fuse.EIO {
		t.Fatalf("the refused rename should fail, got %v", status)
	}
	if status := k.close(target, writer); status != fuse.OK {
		t.Fatalf("close: %v", status)
	}
	k.release(target)
	if contents := client.contents(objectPath); contents != "the writer's source" {
		t.Fatalf("a refused rename should keep the target's held writes, image has %q", contents)
	}
}

func TestVimsRestoreAfterARefusedSaveKeepsTheClass(t *testing.T) {
	// vim renames the file to its backup, writes a new one, and when the
	// image refuses it, removes the name and renames the backup back.
	t.Run("FUSE", func(t *testing.T) {
		client := newMemoryClient(map[string]string{objectPath: "old source"})
		k := newTestKernel(t, client)
		k.rename(objectPath, objectPath+"~", 0)
		file := k.mustCreate(objectPath, openWriteOnly|openTruncate, writer)
		k.mustWrite(file, 0, "syntax error")
		if status := k.fsync(file); status != fuse.EIO {
			t.Fatalf("fsync should report the refusal, got %v", status)
		}
		k.close(file, writer)
		k.release(file)
		if status := k.unlink(objectPath); status != fuse.OK {
			t.Fatalf("unlink: %v", status)
		}
		if status := k.rename(objectPath+"~", objectPath, 0); status != fuse.OK {
			t.Fatalf("restore: %v", status)
		}
		if deletes := client.deletes(); len(deletes) != 0 {
			t.Fatalf("restoring the backup should not remove the class, the image deleted %v", deletes)
		}
		if contents := client.contents(objectPath); contents != "old source" {
			t.Fatalf("the image has %q", contents)
		}

		// After a save over a backup that compiled, removing the name
		// removes the class.
		k.rename(objectPath, objectPath+"~", 0)
		file = k.mustCreate(objectPath, openWriteOnly|openTruncate, writer)
		k.mustWrite(file, 0, "new source")
		k.close(file, writer)
		k.release(file)
		k.unlink(objectPath)
		if deletes := client.deletes(); !slices.Equal(deletes, []string{objectPath}) {
			t.Fatalf("removing a saved class should remove it from the image, deleted %v", deletes)
		}
	})
	t.Run("NFS", func(t *testing.T) {
		client := newMemoryClient(map[string]string{objectPath: "old source"})
		fsys := newNFSFileSystem(client)
		if err := fsys.Rename(objectPath, objectPath+"~"); err != nil {
			t.Fatalf("backup: %v", err)
		}
		if _, err := fsys.Create(objectPath); err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := writeAsNFS(t, fsys, objectPath, "syntax error", 0); err == nil {
			t.Fatalf("the image should refuse the save")
		}
		if err := fsys.Remove(objectPath); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if err := fsys.Rename(objectPath+"~", objectPath); err != nil {
			t.Fatalf("restore: %v", err)
		}
		if deletes := client.deletes(); len(deletes) != 0 {
			t.Fatalf("restoring the backup should not remove the class, the image deleted %v", deletes)
		}
		if contents := client.contents(objectPath); contents != "old source" {
			t.Fatalf("the image has %q", contents)
		}

		// After a save over a backup that compiled, removing the name
		// removes the class.
		if err := fsys.Rename(objectPath, objectPath+"~"); err != nil {
			t.Fatalf("backup: %v", err)
		}
		if _, err := fsys.Create(objectPath); err != nil {
			t.Fatalf("create: %v", err)
		}
		if _, err := writeAsNFS(t, fsys, objectPath, "new source", 0); err != nil {
			t.Fatalf("save: %v", err)
		}
		if err := fsys.Remove(objectPath); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if deletes := client.deletes(); !slices.Equal(deletes, []string{objectPath}) {
			t.Fatalf("removing a saved class should remove it from the image, deleted %v", deletes)
		}
	})
}

func TestNFSARefusedRenameKeepsTheTargetsHeldWrites(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	fsys := newNFSFileSystem(client)
	truncateAsNFS(t, fsys, objectPath, 0)
	if committed, err := writeAsNFS(t, fsys, objectPath, strings.Repeat("a", pageSize), 0); committed || err != nil {
		t.Fatalf("a page should be held, got %v %v", committed, err)
	}
	temp := "/tonel/Kernel/.Object.class.st.swp"
	if _, err := fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := writeNFS(fsys, temp, "syntax error", 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := fsys.Rename(temp, objectPath); err == nil {
		t.Fatalf("the image should refuse the rename")
	}
	if err := fsys.Commit(objectPath); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if contents := client.contents(objectPath); contents != strings.Repeat("a", pageSize) {
		t.Fatalf("a refused rename should keep the target's held writes, image has %q", contents[:min(len(contents), 16)])
	}
}

func TestFUSEOpeningAClassTheImageRemovedIsStale(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "source", "/tonel/Kernel/Other.class.st": "other"})
	k := newTestKernel(t, client)
	node := k.node(objectPath)
	_ = client.Delete(context.Background(), objectPath)
	// The kernel still trusts the name, and opens its node to save over it.
	in := fuse.OpenIn{InHeader: header(node, writer), Flags: openWriteOnly | openTruncate}
	var out fuse.OpenOut
	if status := k.raw.Open(nil, &in, &out); status != fuse.Status(syscall.ESTALE) {
		t.Fatalf("opening a class the image removed should be stale, so the kernel creates it again, got %v", status)
	}
}

func TestFUSEACreatedFileThatLeftIsStale(t *testing.T) {
	created := "/tonel/Kernel/New.class.st"
	for name, leave := range map[string]func(*testKernel, openedFile){
		"refused": func(k *testKernel, file openedFile) {
			k.mustWrite(file, 0, "syntax error")
			k.close(file, writer)
		},
		"unwritten": func(k *testKernel, file openedFile) {
			k.close(file, writer)
		},
	} {
		k := newTestKernel(t, newMemoryClient(map[string]string{objectPath: "source"}))
		file := k.mustCreate(created, openWriteOnly|openTruncate, writer)
		leave(k, file)
		k.release(file)
		// The kernel still trusts the name, and opens its node.
		for _, flags := range []uint32{openWriteOnly | openTruncate, openReadOnly} {
			in := fuse.OpenIn{InHeader: header(file.node, writer), Flags: flags}
			var out fuse.OpenOut
			if status := k.raw.Open(nil, &in, &out); status != fuse.Status(syscall.ESTALE) {
				t.Fatalf("%s: opening a created file that left should be stale, so the kernel looks the name up again, got %v", name, status)
			}
		}
	}
}

func TestFUSEAnOpenRacingARemovalIsStale(t *testing.T) {
	client := newGatedClient(map[string]string{objectPath: "old source"})
	k := newTestKernel(t, client)
	for _, alreadyOpen := range []bool{false, true} {
		lone := "/tonel/Kernel/Lone.class.st"
		client.put(lone, "lone")
		node := k.node(lone)
		var reader openedFile
		if alreadyOpen {
			reader = k.mustOpen(lone, openReadOnly, writer)
		}
		started, open := client.holdNext("delete")
		removed := make(chan fuse.Status, 1)
		go func() { removed <- k.unlink(lone) }()
		waitHeld(t, started)
		opened := make(chan fuse.Status, 1)
		go func() {
			in := fuse.OpenIn{InHeader: header(node, writer), Flags: openReadOnly}
			var out fuse.OpenOut
			opened <- k.raw.Open(nil, &in, &out)
		}()
		time.Sleep(50 * time.Millisecond)
		open()
		within(t, removed, "unlink")
		// Stale, so the kernel looks the name up again.
		if status := within(t, opened, "open"); status != fuse.Status(syscall.ESTALE) {
			t.Fatalf("open already=%v: an open racing a removal should be stale, got %v", alreadyOpen, status)
		}
		if alreadyOpen {
			k.release(reader)
		}
	}
}

func TestFUSEASetattrNeverAnswersAChangeFirst(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "version one"})
	k := newTestKernel(t, client)
	node := k.node(objectPath)
	k.getattr(node)
	// The image changes the file; then touch -c sets its times.
	client.put(objectPath, "version two, longer")
	in := fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: header(node, writer), Valid: fuse.FATTR_MTIME | fuse.FATTR_MTIME_NOW}}
	var reply fuse.AttrOut
	if status := k.raw.SetAttr(nil, &in, &reply); status != fuse.OK {
		t.Fatalf("setattr: %v", status)
	}
	next, _ := k.getattr(node)
	if next.Mtime == reply.Mtime && next.Mtimensec == reply.Mtimensec {
		t.Fatalf("the GETATTR after a SETATTR should answer the change, so the kernel drops its pages")
	}

	// A size set alone compiles; the next GETATTR's time moves past the reply's.
	in = fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: header(node, writer), Valid: fuse.FATTR_SIZE, Size: 7}}
	if status := k.raw.SetAttr(nil, &in, &reply); status != fuse.OK {
		t.Fatalf("truncate: %v", status)
	}
	next, _ = k.getattr(node)
	if next.Mtime == reply.Mtime && next.Mtimensec == reply.Mtimensec {
		t.Fatalf("the GETATTR after a compiling SETATTR should answer a later time")
	}
}

func TestFUSESyncedWritesCompileOnce(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openWriteOnly|openTruncate|0o10000, writer)
	for offset, part := range []string{"new ", "source"} {
		k.mustWrite(file, uint64(offset*len("new ")), part)
		if status := k.fsync(file); status != fuse.OK {
			t.Fatalf("fsync: %v", status)
		}
	}
	k.close(file, writer)
	k.release(file)
	if writes := client.writes(); !slices.Equal(writes, []string{objectPath + "=new source"}) {
		t.Fatalf("a file opened to sync its writes should compile once, got %v", writes)
	}
}

func TestFUSEAnEditorsFileNeverHoldsWrites(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	k := newTestKernel(t, client)
	temp := "/tonel/Kernel/sedAb12Cd"
	editor := k.mustCreate(temp, openWriteOnly, child)
	k.mustWrite(editor, 0, "the temp's source")
	k.close(editor, child)
	if status := k.rename(temp, objectPath, 0); status != fuse.OK {
		t.Fatalf("rename: %v", status)
	}
	other := k.mustOpen(objectPath, openWriteOnly|openTruncate, writer)
	k.mustWrite(other, 0, "half of a")
	// The editor's late RELEASE, now that its file is the class.
	k.release(editor)
	if writes := client.writes(); len(writes) != 1 {
		t.Fatalf("an editor's file should compile nothing at its release, got %v", writes)
	}
	k.mustWrite(other, uint64(len("half of a")), " save")
	k.close(other, writer)
	k.release(other)
	if contents := client.contents(objectPath); contents != "half of a save" {
		t.Fatalf("the image has %q", contents)
	}
}

func TestFUSEAnFsyncCompilesWhoeverAsks(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openWriteOnly|openTruncate, writer)
	k.mustWrite(file, 0, "new source")
	in := fuse.FsyncIn{InHeader: header(file.node, child), Fh: file.fh}
	if status := k.raw.Fsync(nil, &in); status != fuse.OK {
		t.Fatalf("fsync: %v", status)
	}
	if writes := client.writes(); !slices.Equal(writes, []string{objectPath + "=new source"}) {
		t.Fatalf("an fsync from any process should compile, got %v", writes)
	}
	k.close(file, writer)
	k.release(file)
}

func TestFUSESizesThroughAFileWithNothingHeld(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openWriteOnly, writer)
	if status := k.setSize(file.node, &file, 3); status != fuse.OK {
		t.Fatalf("ftruncate: %v", status)
	}
	if writes := client.writes(); len(writes) != 0 {
		t.Fatalf("a size set through a file is its write, which compiles at its close, got %v", writes)
	}
	k.close(file, writer)
	k.release(file)
	if writes := client.writes(); !slices.Equal(writes, []string{objectPath + "=old"}) {
		t.Fatalf("the close should compile the file cut, got %v", writes)
	}
}

// writeInPlace writes contents through file as Bun's fs.writeFile, which
// OpenCode saves with, does once it opened the file without O_TRUNC: from 0,
// then the size of what write(2) took. It answers what write(2) answered,
// the last call Bun checks.
func (k *testKernel) writeInPlace(file openedFile, contents string) fuse.Status {
	k.t.Helper()
	status := k.write(file, 0, contents)
	written := uint64(len(contents))
	if status != fuse.OK {
		written = 0
	}
	k.setSize(file.node, &file, written)
	return status
}

// refusals answers what each refusal recorded in /errors says.
func (k *testKernel) refusals() []string {
	k.t.Helper()
	refusals := []string{}
	for _, name := range k.list("/errors") {
		if name != latestErrorName {
			refusals = append(refusals, k.readFile("/errors/"+name))
		}
	}
	return refusals
}

func TestFUSEAWriterInPlaceHearsARefusalFromItsWrite(t *testing.T) {
	old := "an older and much longer source"
	client := &emptyRefusingClient{newMemoryClient(map[string]string{objectPath: old})}
	k := newTestKernel(t, client)
	file := k.mustOpen(objectPath, openWriteOnly, writer)
	if status := k.writeInPlace(file, "syntax error"); status != fuse.EIO {
		t.Fatalf("a writer that checks only write(2) should see the refusal there, got %v", status)
	}
	if status := k.close(file, writer); status != fuse.EIO {
		t.Fatalf("the close should answer the refusal again, got %v", status)
	}
	k.release(file)
	if contents := k.readFile(objectPath); contents != old {
		t.Fatalf("the image's version should stand, read %q", contents)
	}
	if refusals := k.refusals(); len(refusals) != 1 || !strings.Contains(refusals[0], "syntax error") {
		t.Fatalf("the size the refused writer set should compile nothing, /errors has %q", refusals)
	}

	// The next save starts the file over, and compiles at its write.
	file = k.mustOpen(objectPath, openWriteOnly, writer)
	if status := k.writeInPlace(file, "new source"); status != fuse.OK {
		t.Fatalf("save: %v", status)
	}
	if writes := client.writes(); !slices.Equal(writes, []string{objectPath + "=new source"}) {
		t.Fatalf("the write should compile the bytes written, without the old file's end, got %v", writes)
	}
	if status := k.close(file, writer); status != fuse.OK {
		t.Fatalf("close: %v", status)
	}
	k.release(file)
	if writes := client.writes(); len(writes) != 1 {
		t.Fatalf("the save should compile once, got %v", writes)
	}
}

func TestFUSEANewClassWrittenInPlaceHearsItsRefusal(t *testing.T) {
	k := newTestKernel(t, newMemoryClient(map[string]string{objectPath: "source"}))
	created := "/tonel/Kernel/New.class.st"
	file := k.mustCreate(created, openWriteOnly, writer)
	if status := k.writeInPlace(file, "syntax error"); status != fuse.EIO {
		t.Fatalf("a writer that checks only write(2) should see the refusal there, got %v", status)
	}
	k.close(file, writer)
	k.release(file)
	if names := k.list("/tonel/Kernel"); slices.Contains(names, "New.class.st") {
		t.Fatalf("a refused new class should leave its directory, got %v", names)
	}
	in := fuse.OpenIn{InHeader: header(file.node, writer), Flags: openWriteOnly}
	var out fuse.OpenOut
	if status := k.raw.Open(nil, &in, &out); status != fuse.Status(syscall.ESTALE) {
		t.Fatalf("opening a refused new class should be stale, so the kernel looks the name up again, got %v", status)
	}
}

// An archive extractor, as GNU tar and bsdtar do, or a shell's > under
// noclobber, creates a file exclusively, which Linux sends without O_TRUNC,
// and writes it in pieces that cut it anywhere: GNU tar's first write is
// what its 10240-byte record holds after the header.
func TestFUSEAFileCreatedExclusivelyCompilesAtItsClose(t *testing.T) {
	client := &cutMethodRefusingClient{newMemoryClient(map[string]string{objectPath: "source"})}
	k := newTestKernel(t, client)
	created := "/tonel/Kernel/New.class.st"
	contents := "Class { #name : 'New' }\n" + strings.Repeat("New >> m [ ^ 1 ]\n", 900)
	record := 9728
	file := k.mustCreate(created, openWriteOnly|openExclusive, writer)
	k.mustWrite(file, 0, contents[:record])
	k.mustWrite(file, uint64(record), contents[record:])
	if writes := client.writes(); len(writes) != 0 {
		t.Fatalf("a file created exclusively should compile at its close, got %d compiles", len(writes))
	}
	if status := k.close(file, writer); status != fuse.OK {
		t.Fatalf("close: %v", status)
	}
	k.release(file)
	if writes := client.writes(); !slices.Equal(writes, []string{created + "=" + contents}) {
		t.Fatalf("the close should compile the whole file, once, got %d compiles", len(writes))
	}
	if refusals := k.refusals(); len(refusals) != 0 {
		t.Fatalf("nothing should be refused, /errors has %q", refusals)
	}
}

func TestFUSEAWriterInPlaceCompilesAtTheWriteThatEndsTheFile(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	k := newTestKernel(t, client)
	page := strings.Repeat("a", pageSize)
	file := k.mustOpen(objectPath, openWriteOnly, writer)
	k.mustWrite(file, 0, page)
	if writes := client.writes(); len(writes) != 0 {
		t.Fatalf("a write ending on a page is not the end of a file, got %d compiles", len(writes))
	}
	k.mustWrite(file, pageSize, "end")
	if writes := client.writes(); !slices.Equal(writes, []string{objectPath + "=" + page + "end"}) {
		t.Fatalf("the write that ends the file should compile it, got %d compiles", len(writes))
	}
	k.close(file, writer)
	k.release(file)
	if writes := client.writes(); len(writes) != 1 {
		t.Fatalf("the save should compile once, got %d compiles", len(writes))
	}

	// A file of whole pages is the start of a longer one, as far as its
	// writes tell: it compiles at the close.
	file = k.mustOpen(objectPath, openWriteOnly, writer)
	k.mustWrite(file, 0, page)
	if writes := client.writes(); len(writes) != 1 {
		t.Fatalf("a write ending on a page is not the end of a file, got %d compiles", len(writes))
	}
	if status := k.close(file, writer); status != fuse.OK {
		t.Fatalf("close: %v", status)
	}
	k.release(file)
	if writes := client.writes(); len(writes) != 2 || writes[1] != objectPath+"="+page {
		t.Fatalf("the close should compile the file of whole pages, got %d compiles", len(writes))
	}
}

func TestFUSERenameCompilesTheSourcesHeldWritesFirst(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "old source"})
	k := newTestKernel(t, client)
	moved := "/tonel/Kernel/Moved.class.st"
	file := k.mustOpen(objectPath, openWriteOnly|openTruncate, writer)
	k.mustWrite(file, 0, "held source")
	if status := k.rename(objectPath, moved, 0); status != fuse.OK {
		t.Fatalf("rename: %v", status)
	}
	if writes := client.writes(); len(writes) == 0 || writes[0] != objectPath+"=held source" {
		t.Fatalf("a rename should compile the source's held writes first, got %v", writes)
	}
	// The file's node follows it: its next writes land at its new name.
	k.mustWrite(file, 0, "moved source")
	k.close(file, writer)
	k.release(file)
	if contents := client.contents(moved); contents != "moved source" {
		t.Fatalf("writes through a renamed file's node should reach its new name, image has %q at %s", contents, moved)
	}
}

func TestFUSEDirectoriesDoNotListThemselvesOnAStat(t *testing.T) {
	client := &listingCountingClient{memoryClient: newMemoryClient(map[string]string{objectPath: "source"})}
	k := newTestKernel(t, client)
	node := k.node("/tonel/Kernel")
	before := client.listings.Load()
	for range 3 {
		// Once the last listing is older than a listing is trusted for.
		k.fsys.mu.Lock()
		for directory, known := range k.fsys.listings {
			known.checked = time.Time{}
			k.fsys.listings[directory] = known
		}
		k.fsys.mu.Unlock()
		k.getattr(node)
	}
	if client.listings.Load() != before {
		t.Fatalf("a stat of a directory should not list it")
	}
}
