package mount

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
)

// These tests speak NFSv3 to the daemon's server over a loopback socket, as
// the client does, without mounting anything.

const (
	nfsProgram = 100003
	nfsVersion = 3

	procGetattr = 1
	procSetattr = 2
	procLookup  = 3
	procRead    = 6
	procWrite   = 7
	procRemove  = 12
	procRmdir   = 13
	procRename  = 14
	procLink    = 15
	procFSInfo  = 19

	acceptSuccess     = 0
	acceptProcUnavail = 3
	acceptSystemErr   = 5

	statusOK      = 0
	statusInval   = 22
	statusNotDir  = 20
	statusIsDir   = 21
	statusFBig    = 27
	statusStale   = 70
	statusNotSupp = 10004

	unstable = 0
	fileSync = 2
)

type testServer struct {
	fsys   *nfsFileSystem
	conn   net.Conn
	logger *recordingLogger
	xid    uint32
}

func startTestServer(t *testing.T, client protocol.Client) *testServer {
	t.Helper()
	logger := &recordingLogger{}
	server, err := startNFSServer(client, logger)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(server.stop)
	conn, err := net.Dial("tcp", server.listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &testServer{fsys: server.fsys, conn: conn, logger: logger}
}

func (s *testServer) handle(projectionPath string) []byte {
	handle := make([]byte, 8)
	binary.BigEndian.PutUint64(handle, s.fsys.ids.idFor(projectionPath))
	return handle
}

// send sends an NFSv3 call, and answers its xid.
func (s *testServer) send(t *testing.T, proc uint32, args *xdrBuffer) uint32 {
	t.Helper()
	s.xid++
	call := &xdrBuffer{}
	call.u32(s.xid, 0, 2, nfsProgram, nfsVersion, proc, 0, 0, 0, 0)
	call.Write(args.Bytes())
	mark := make([]byte, 4)
	binary.BigEndian.PutUint32(mark, uint32(call.Len())|1<<31)
	if _, err := s.conn.Write(append(mark, call.Bytes()...)); err != nil {
		t.Fatalf("send: %v", err)
	}
	return s.xid
}

type rpcReply struct {
	xid    uint32
	accept uint32
	body   []byte
}

// status answers the NFS status a reply starts with.
func (r rpcReply) status() uint32 {
	return binary.BigEndian.Uint32(r.body)
}

// receive reads the next reply, or answers false when none comes in time.
func (s *testServer) receive(t *testing.T, timeout time.Duration) (rpcReply, bool) {
	t.Helper()
	_ = s.conn.SetReadDeadline(time.Now().Add(timeout))
	defer func() { _ = s.conn.SetReadDeadline(time.Time{}) }()
	mark := make([]byte, 4)
	if _, err := io.ReadFull(s.conn, mark); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return rpcReply{}, false
		}
		t.Fatalf("receive: %v", err)
	}
	record := make([]byte, binary.BigEndian.Uint32(mark)&^(1<<31))
	if _, err := io.ReadFull(s.conn, record); err != nil {
		t.Fatalf("receive: %v", err)
	}
	word := func(at int) uint32 { return binary.BigEndian.Uint32(record[at:]) }
	if word(4) != 1 || word(8) != 0 {
		t.Fatalf("expected an accepted reply, got type %d, state %d", word(4), word(8))
	}
	verifier := (int(word(16)) + 3) &^ 3
	return rpcReply{xid: word(0), accept: word(20 + verifier), body: record[24+verifier:]}, true
}

// call sends an NFSv3 call and answers its reply.
func (s *testServer) call(t *testing.T, proc uint32, args *xdrBuffer) rpcReply {
	t.Helper()
	xid := s.send(t, proc, args)
	reply, ok := s.receive(t, 5*time.Second)
	if !ok {
		t.Fatalf("no reply to procedure %d", proc)
	}
	if reply.xid != xid {
		t.Fatalf("reply %d answers another call than %d", reply.xid, xid)
	}
	return reply
}

func (s *testServer) getattr(t *testing.T, handle []byte) rpcReply {
	t.Helper()
	args := &xdrBuffer{}
	args.opaque(handle)
	return s.call(t, procGetattr, args)
}

// fattr3 sizes and file ids sit at fixed places after a GETATTR's status.
func sizeIn(reply rpcReply) uint64   { return binary.BigEndian.Uint64(reply.body[24:]) }
func fileidIn(reply rpcReply) uint64 { return binary.BigEndian.Uint64(reply.body[56:]) }

func TestNFSServerAnswersAnUnknownProcedureAsUnavailable(t *testing.T) {
	server := startTestServer(t, newMemoryClient(nil))

	if reply := server.call(t, 99, &xdrBuffer{}); reply.accept != acceptProcUnavail {
		t.Fatalf("an unknown procedure should answer PROC_UNAVAIL (%d), got %d", acceptProcUnavail, reply.accept)
	}
}

func TestNFSServerSurvivesAPanic(t *testing.T) {
	boom := "/tonel/Kernel/Boom.class.st"
	server := startTestServer(t, &panickingClient{memoryClient: newMemoryClient(map[string]string{objectPath: "source"}), path: boom})

	if reply := server.getattr(t, server.handle(boom)); reply.accept != acceptSystemErr {
		t.Fatalf("a panicking call should answer SYSTEM_ERR, got %d", reply.accept)
	}
	if reply := server.getattr(t, server.handle(objectPath)); reply.accept != acceptSuccess || reply.status() != statusOK {
		t.Fatalf("the server should go on after a panic, got %d, %d", reply.accept, reply.status())
	}
	if !server.logger.contains("panic serving NFS: boom") {
		t.Fatalf("the panic should be logged")
	}
}

func (s *testServer) write(t *testing.T, projectionPath string, data string) uint32 {
	t.Helper()
	args := &xdrBuffer{}
	args.opaque(s.handle(projectionPath))
	args.u64(0)
	args.u32(uint32(len(data)), fileSync)
	args.opaque([]byte(data))
	return s.send(t, procWrite, args)
}

func TestNFSServerAnswersOtherFilesWhileACompileRuns(t *testing.T) {
	other := "/tonel/Kernel/Other.class.st"
	newSource := "a longer new source"
	client := newGatedClient(map[string]string{objectPath: "old source", other: "other"})
	server := startTestServer(t, client)
	started, open := client.holdNext("write")
	defer open()

	compileXID := server.write(t, objectPath, newSource)
	if compiled := waitHeld(t, started); compiled != objectPath {
		t.Fatalf("expected %s to compile, got %s", objectPath, compiled)
	}
	if reply := server.getattr(t, server.handle(other)); reply.status() != statusOK {
		t.Fatalf("a stat of another file should be answered during the compile, got %d", reply.status())
	}

	args := &xdrBuffer{}
	args.opaque(server.handle(objectPath))
	statXID := server.send(t, procGetattr, args)
	changeXID := server.write(t, other, "O")
	if early, ok := server.receive(t, 200*time.Millisecond); ok {
		t.Fatalf("a stat of the compiling file and another change should wait for the compile, but call %d was answered", early.xid)
	}

	open()
	answered := map[uint32]rpcReply{}
	for range 3 {
		reply, ok := server.receive(t, 5*time.Second)
		if !ok {
			t.Fatalf("the calls waiting on the compile were not answered")
		}
		answered[reply.xid] = reply
	}
	if answered[compileXID].status() != statusOK || answered[changeXID].status() != statusOK {
		t.Fatalf("both writes should succeed once the compile ends")
	}
	if stat := answered[statXID]; stat.status() != statusOK || sizeIn(stat) != uint64(len(newSource)) {
		t.Fatalf("the stat should answer the compiled file, got status %d, size %d", stat.status(), sizeIn(stat))
	}
	if contents := client.contents(objectPath); contents != newSource {
		t.Fatalf("the image has %q", contents)
	}
}

func TestNFSServerAnswersReadsBehindQueuedChanges(t *testing.T) {
	// More changes than the macOS client keeps writes in flight.
	queued := 200
	files := map[string]string{objectPath: "old source", "/tonel/Kernel/Read.class.st": "read"}
	for index := range queued {
		files[fmt.Sprintf("/tonel/Kernel/Changed%d.class.st", index)] = "changed"
	}
	client := newGatedClient(files)
	server := startTestServer(t, client)
	started, open := client.holdNext("write")
	defer open()

	server.write(t, objectPath, "new source")
	waitHeld(t, started)
	for index := range queued {
		server.write(t, fmt.Sprintf("/tonel/Kernel/Changed%d.class.st", index), "C")
	}
	args := &xdrBuffer{}
	args.opaque(server.handle("/tonel/Kernel/Read.class.st"))
	statXID := server.send(t, procGetattr, args)
	reply, ok := server.receive(t, time.Second)
	if !ok || reply.xid != statXID || reply.status() != statusOK {
		t.Fatalf("a stat behind %d queued changes should be answered during the compile", queued)
	}
}

func TestNFSServerEndsAConnectionWhoseRepliesCannotBeSent(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: strings.Repeat("a", 256<<10)})
	server, err := startNFSServer(client, &recordingLogger{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer server.stop()
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()

	conn, err := net.Dial("tcp", server.listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.(*net.TCPConn).SetReadBuffer(4096)
	peer := &testServer{fsys: server.fsys, conn: conn}
	// More replies than the connection queues and serves at once, to a peer
	// that reads none of them.
	for range 2*concurrentCalls + 72 {
		read := &xdrBuffer{}
		read.opaque(peer.handle(objectPath))
		read.u64(0)
		read.u32(256 << 10)
		peer.send(t, procRead, read)
	}
	// Every call the connection serves at once waits to queue its reply, so
	// the connection reads no more calls: only the failed send can end it.
	full := time.Now().Add(10 * time.Second)
	for repliesWaitingToQueue() < concurrentCalls {
		if time.Now().After(full) {
			t.Fatalf("every call served should wait to queue its reply, %d do", repliesWaitingToQueue())
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = conn.(*net.TCPConn).SetLinger(0)
	_ = conn.Close()

	// The send blocked on the full window may only fail at the system's next
	// probe of it, seconds later; a connection that leaks never ends.
	deadline := time.Now().Add(30 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("the connection's goroutines outlived it: %d running, %d before", runtime.NumGoroutine(), before)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestNFSServerAcceptsTheWritesItAdvertises(t *testing.T) {
	temp := "/tonel/Kernel/.Object.class.st.swp"
	server := startTestServer(t, newMemoryClient(map[string]string{objectPath: "source"}))
	if _, err := server.fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}

	fsinfo := &xdrBuffer{}
	fsinfo.opaque(server.handle("/"))
	reply := server.call(t, procFSInfo, fsinfo)
	if reply.status() != statusOK {
		t.Fatalf("FSINFO: status %d", reply.status())
	}
	at := 8
	if binary.BigEndian.Uint32(reply.body[4:]) == 1 {
		at += 84
	}
	wtmax := binary.BigEndian.Uint32(reply.body[at+12:])
	if wtmax != 64<<10 {
		t.Fatalf("FSINFO should advertise writes of 64 KiB, which bound what a full queue of calls holds, got %d", wtmax)
	}
	write := server.call(t, procWrite, func() *xdrBuffer {
		args := &xdrBuffer{}
		args.opaque(server.handle(temp))
		args.u64(0)
		args.u32(wtmax, fileSync)
		args.opaque(make([]byte, wtmax))
		return args
	}())
	if write.status() != statusOK {
		t.Fatalf("a write of the advertised size should succeed, got %d", write.status())
	}
}

func TestNFSServerRenameSaveKeepsTheTempFilesHandle(t *testing.T) {
	temp := "/tonel/Kernel/.Object.class.st.swp"
	server := startTestServer(t, newMemoryClient(map[string]string{objectPath: "old"}))
	classHandle := server.handle(objectPath)
	if _, err := server.fsys.Create(temp); err != nil {
		t.Fatalf("create: %v", err)
	}
	tempHandle := server.handle(temp)
	tempID := server.fsys.ids.idFor(temp)

	rename := &xdrBuffer{}
	rename.opaque(server.handle("/tonel/Kernel"))
	rename.str(".Object.class.st.swp")
	rename.opaque(server.handle("/tonel/Kernel"))
	rename.str("Object.class.st")
	if reply := server.call(t, procRename, rename); reply.status() != statusOK {
		t.Fatalf("rename: status %d", reply.status())
	}

	if reply := server.getattr(t, classHandle); reply.status() != statusStale {
		t.Fatalf("the replaced file's handle should be stale, got status %d", reply.status())
	}
	reply := server.getattr(t, tempHandle)
	if reply.status() != statusOK || fileidIn(reply) != tempID {
		t.Fatalf("the renamed file should keep its handle and id %d, got status %d, id %d", tempID, reply.status(), fileidIn(reply))
	}
	lookup := &xdrBuffer{}
	lookup.opaque(server.handle("/tonel/Kernel"))
	lookup.str("Object.class.st")
	if reply := server.call(t, procLookup, lookup); reply.status() != statusOK || !bytes.Equal(reply.body[8:16], tempHandle) {
		t.Fatalf("the name should now answer the renamed file's handle, got status %d", reply.status())
	}
}

func TestNFSServerRemoveAndRmdirCheckWhatTheyRemove(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "source"})
	server := startTestServer(t, client)

	rmdir := &xdrBuffer{}
	rmdir.opaque(server.handle("/tonel/Kernel"))
	rmdir.str("Object.class.st")
	if reply := server.call(t, procRmdir, rmdir); reply.status() != statusNotDir {
		t.Fatalf("RMDIR of a file should answer NOTDIR, got %d", reply.status())
	}
	remove := &xdrBuffer{}
	remove.opaque(server.handle("/tonel"))
	remove.str("Kernel")
	if reply := server.call(t, procRemove, remove); reply.status() != statusIsDir {
		t.Fatalf("REMOVE of a directory should answer ISDIR, got %d", reply.status())
	}
	if !client.has(objectPath) {
		t.Fatalf("nothing should have been removed")
	}
}

func TestNFSServerRefusesHardLinksInLinksOwnShape(t *testing.T) {
	server := startTestServer(t, newMemoryClient(map[string]string{objectPath: "source"}))

	link := &xdrBuffer{}
	link.opaque(server.handle(objectPath))
	link.opaque(server.handle("/tonel/Kernel"))
	link.str("Copy.class.st")
	reply := server.call(t, procLink, link)
	if reply.status() != statusNotSupp || len(reply.body) != 16 {
		t.Fatalf("LINK should answer NOTSUPP with empty attributes, got status %d and %d bytes", reply.status(), len(reply.body))
	}

	fsinfo := &xdrBuffer{}
	fsinfo.opaque(server.handle("/"))
	reply = server.call(t, procFSInfo, fsinfo)
	if reply.accept != acceptSuccess || reply.status() != statusOK {
		t.Fatalf("FSINFO should succeed, got %d, %d", reply.accept, reply.status())
	}
	if properties := binary.BigEndian.Uint32(reply.body[len(reply.body)-4:]); properties&1 != 0 {
		t.Fatalf("FSINFO should not claim hard links: %#x", properties)
	}
}

func TestNFSServerRefusesOffsetsNoFileReaches(t *testing.T) {
	server := startTestServer(t, newMemoryClient(map[string]string{objectPath: "source"}))

	write := &xdrBuffer{}
	write.opaque(server.handle(objectPath))
	write.u64(maxFileSize)
	write.u32(1, fileSync)
	write.opaque([]byte("x"))
	if reply := server.call(t, procWrite, write); reply.status() != statusFBig {
		t.Fatalf("a write past the largest file should answer FBIG, got %d", reply.status())
	}

	read := &xdrBuffer{}
	read.opaque(server.handle(objectPath))
	read.u64(1 << 63)
	read.u32(16)
	if reply := server.call(t, procRead, read); reply.accept != acceptSuccess || reply.status() != statusInval {
		t.Fatalf("a read past 2^63 should answer INVAL, got %d, %d", reply.accept, reply.status())
	}
	if server.logger.contains("panic") {
		t.Fatalf("no call should have panicked")
	}
}

type xdrBuffer struct {
	bytes.Buffer
}

func (b *xdrBuffer) u32(values ...uint32) {
	for _, value := range values {
		_ = binary.Write(b, binary.BigEndian, value)
	}
}

func (b *xdrBuffer) u64(value uint64) {
	_ = binary.Write(b, binary.BigEndian, value)
}

func (b *xdrBuffer) opaque(data []byte) {
	b.u32(uint32(len(data)))
	b.Write(data)
	b.Write(make([]byte, (4-len(data)%4)%4))
}

func (b *xdrBuffer) str(s string) {
	b.opaque([]byte(s))
}

// gatedClient can hold a call to the image, as a debugger or a slow image
// would: a write before it takes effect, a stat or a read after it read, a
// rename or a delete after it took effect.
type gatedClient struct {
	*memoryClient
	gateMu sync.Mutex
	gates  map[string]gate
}

type gate struct {
	open    chan struct{}
	started chan string
}

func newGatedClient(files map[string]string) *gatedClient {
	return &gatedClient{memoryClient: newMemoryClient(files), gates: map[string]gate{}}
}

// holdNext holds the next call of a kind ("write", "stat", "read", "rename",
// "delete") until open is called; started says which file it holds.
func (c *gatedClient) holdNext(kind string) (started <-chan string, open func()) {
	c.gateMu.Lock()
	defer c.gateMu.Unlock()
	next := gate{open: make(chan struct{}), started: make(chan string, 1)}
	c.gates[kind] = next
	var once sync.Once
	return next.started, func() { once.Do(func() { close(next.open) }) }
}

func (c *gatedClient) pass(kind string, filePath string) {
	c.gateMu.Lock()
	held, ok := c.gates[kind]
	delete(c.gates, kind)
	c.gateMu.Unlock()
	if ok {
		held.started <- filePath
		<-held.open
	}
}

func (c *gatedClient) Write(ctx context.Context, filePath string, contents []byte) (protocol.WriteResult, error) {
	c.pass("write", filePath)
	return c.memoryClient.Write(ctx, filePath, contents)
}

func (c *gatedClient) Stat(ctx context.Context, filePath string) (protocol.Entry, error) {
	entry, err := c.memoryClient.Stat(ctx, filePath)
	c.pass("stat", filePath)
	return entry, err
}

func (c *gatedClient) Read(ctx context.Context, filePath string) ([]byte, error) {
	contents, err := c.memoryClient.Read(ctx, filePath)
	c.pass("read", filePath)
	return contents, err
}

func (c *gatedClient) Rename(ctx context.Context, sourcePath string, targetPath string) error {
	err := c.memoryClient.Rename(ctx, sourcePath, targetPath)
	c.pass("rename", sourcePath)
	return err
}

func (c *gatedClient) Delete(ctx context.Context, filePath string) error {
	err := c.memoryClient.Delete(ctx, filePath)
	c.pass("delete", filePath)
	return err
}

// panickingClient panics when asked about one path.
type panickingClient struct {
	*memoryClient
	path string
}

func (c *panickingClient) Stat(ctx context.Context, filePath string) (protocol.Entry, error) {
	if filePath == c.path {
		panic("boom")
	}
	return c.memoryClient.Stat(ctx, filePath)
}

type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) contains(text string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, text) {
			return true
		}
	}
	return false
}

// waitHeld waits for a held call to reach the image, and fails the test if it
// does not.
func waitHeld(t *testing.T, started <-chan string) string {
	t.Helper()
	select {
	case filePath := <-started:
		return filePath
	case <-time.After(5 * time.Second):
		t.Fatalf("the held call never reached the image")
		return ""
	}
}

// within waits for a result, and fails the test if none comes.
func within[T any](t *testing.T, results <-chan T, what string) T {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not end", what)
		var none T
		return none
	}
}

func TestNFSServerReadsAnswerDataAsNewAsTheirAttributes(t *testing.T) {
	client := newMemoryClient(map[string]string{objectPath: "version one"})
	server := startTestServer(t, client)
	read := func() rpcReply {
		args := &xdrBuffer{}
		args.opaque(server.handle(objectPath))
		args.u64(0)
		args.u32(4096)
		return server.call(t, procRead, args)
	}

	read()
	client.put(objectPath, "version two, longer")
	reply := read()
	if reply.status() != statusOK || binary.BigEndian.Uint32(reply.body[4:]) != 1 {
		t.Fatalf("READ should answer with attributes, got status %d", reply.status())
	}
	// READ3resok: status, post_op_attr (follows, fattr3 of 84 bytes), count,
	// eof, then the data as an opaque.
	size := binary.BigEndian.Uint64(reply.body[8+20:])
	count := binary.BigEndian.Uint32(reply.body[92:])
	data := string(reply.body[104 : 104+int(count)])
	if uint64(count) != size || data != "version two, longer" {
		t.Fatalf("a read after the image changed should answer its new data with its new size, got %q for size %d", data, size)
	}

	// A change of the same size, which only the image's time tells.
	client.put(objectPath, "version 3, longer!!")
	client.setModified(objectPath, time.Now().Add(time.Minute).UnixMilli())
	reply = read()
	count = binary.BigEndian.Uint32(reply.body[92:])
	if data := string(reply.body[104 : 104+int(count)]); data != "version 3, longer!!" {
		t.Fatalf("a read after a same-size change in the image should answer the new data, got %q", data)
	}

	// The image changes while the READ renders the file: its attributes,
	// taken first, must not be newer than its data.
	gated := newGatedClient(map[string]string{objectPath: "version one"})
	server = startTestServer(t, gated)
	started, open := gated.holdNext("read")
	defer open()
	args := &xdrBuffer{}
	args.opaque(server.handle(objectPath))
	args.u64(0)
	args.u32(4096)
	xid := server.send(t, procRead, args)
	waitHeld(t, started)
	gated.put(objectPath, "version two, longer")
	open()
	reply, ok := server.receive(t, 5*time.Second)
	if !ok || reply.xid != xid || reply.status() != statusOK {
		t.Fatalf("the READ should be answered")
	}
	size = binary.BigEndian.Uint64(reply.body[8+20:])
	count = binary.BigEndian.Uint32(reply.body[92:])
	if uint64(count) != size || string(reply.body[104:104+int(count)]) != "version one" {
		t.Fatalf("a READ should answer the data its attributes describe, got %q for size %d", reply.body[104:104+int(count)], size)
	}
}

// repliesWaitingToQueue answers how many calls wait to queue their reply.
func repliesWaitingToQueue() int {
	stacks := make([]byte, 64<<20)
	return strings.Count(string(stacks[:runtime.Stack(stacks, true)]), "nfs.(*response).finish(")
}

// renderingClient compiles what it is given and renders it its own way, as
// the image does.
type renderingClient struct {
	*memoryClient
}

func (c *renderingClient) Write(ctx context.Context, filePath string, contents []byte) (protocol.WriteResult, error) {
	return c.memoryClient.Write(ctx, filePath, append(append([]byte(nil), contents...), "\n\"rendered\"\n"...))
}

func (s *testServer) setSize(t *testing.T, projectionPath string, size uint64) rpcReply {
	t.Helper()
	args := &xdrBuffer{}
	args.opaque(s.handle(projectionPath))
	args.u32(0, 0, 0, 1) // mode, uid, gid not set; size set
	args.u64(size)
	args.u32(0, 0, 0) // atime and mtime not set, no guard
	return s.call(t, procSetattr, args)
}

// wcc is the wcc_data a reply's body starts with, after its status.
type wcc struct {
	pre      bool
	postSize uint64
}

func wccIn(reply rpcReply) wcc {
	at := 4
	var data wcc
	if binary.BigEndian.Uint32(reply.body[at:]) == 1 {
		data.pre = true
		at += 24
	}
	at += 4
	if binary.BigEndian.Uint32(reply.body[at:]) == 1 {
		data.postSize = binary.BigEndian.Uint64(reply.body[at+4+20:])
	}
	return data
}

func TestNFSServerACallThatCompilesSendsNoPreOpAttributes(t *testing.T) {
	client := &renderingClient{newMemoryClient(map[string]string{objectPath: "old source"})}
	server := startTestServer(t, client)

	// open(O_TRUNC): the client takes the attributes as its own change.
	if emptied := server.setSize(t, objectPath, 0); emptied.status() != statusOK || !wccIn(emptied).pre {
		t.Fatalf("emptying a file should keep its pre-op attributes")
	}
	page := &xdrBuffer{}
	page.opaque(server.handle(objectPath))
	page.u64(0)
	page.u32(pageSize, unstable)
	page.opaque([]byte(strings.Repeat("a", pageSize)))
	if held := server.call(t, procWrite, page); held.status() != statusOK || !wccIn(held).pre {
		t.Fatalf("a held write should keep its pre-op attributes")
	}

	source := "a new source"
	server.setSize(t, objectPath, 0)
	server.write(t, objectPath, source)
	compiled, ok := server.receive(t, 5*time.Second)
	if !ok || compiled.status() != statusOK {
		t.Fatalf("the save should compile")
	}
	// A client that found its own attributes in the pre-op ones would take
	// the image's rendering for its own bytes, and keep them.
	if data := wccIn(compiled); data.pre || data.postSize != uint64(len(client.contents(objectPath))) {
		t.Fatalf("a write that compiles should send no pre-op attributes, and the rendering's size: %+v", data)
	}

	if cut := server.setSize(t, objectPath, 5); cut.status() != statusOK || wccIn(cut).pre {
		t.Fatalf("a size that compiles the file should send no pre-op attributes")
	}
	if contents := client.contents(objectPath); !strings.HasPrefix(contents, "a new\n") {
		t.Fatalf("the image should have the file cut to its size, has %q", contents)
	}
}
