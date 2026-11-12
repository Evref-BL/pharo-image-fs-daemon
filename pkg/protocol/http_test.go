package protocol

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPClientListPostsProjectionPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/projection/list" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		var body pathRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Path != "/tonel" {
			t.Fatalf("unexpected projection path: %s", body.Path)
		}

		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"entries":[{"name":"PharoImageFS","kind":"directory"}]}`))
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL + "/projection")
	if err != nil {
		t.Fatal(err)
	}

	entries, err := client.List(t.Context(), "/tonel")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name != "PharoImageFS" || entries[0].Kind != Directory {
		t.Fatalf("unexpected entries: %#v", entries)
	}
}

func TestHTTPClientListDecodesSymlinkTarget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"entries":[{"name":"MCP","kind":"symlink","target":"/Users/example/MCP"}]}`))
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL + "/projection")
	if err != nil {
		t.Fatal(err)
	}

	entries, err := client.List(t.Context(), "/repositories")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Kind != Symlink || entries[0].Target != "/Users/example/MCP" {
		t.Fatalf("unexpected entries: %#v", entries)
	}
}

func TestHTTPClientReturnsProtocolErrorMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.WriteHeader(http.StatusBadRequest)
		_, _ = response.Write([]byte(`{"message":"compile failed"}`))
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Read(t.Context(), "/tonel/PharoImageFS/PharoImageFSProjectionBackend.class.st")
	if err == nil {
		t.Fatal("expected protocol error")
	}
	if err.Error() != "compile failed" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHTTPClientDeletePostsProjectionPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/projection/delete" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		var body pathRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Path != "/tonel/PharoImageFS/Old.class.st" {
			t.Fatalf("unexpected projection path: %s", body.Path)
		}

		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{}`))
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL + "/projection")
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Delete(t.Context(), "/tonel/PharoImageFS/Old.class.st"); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPClientRenamePostsSourceAndTargetPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/projection/rename" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		var body renameRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Path != "/tonel/PharoImageFS/Old.class.st" {
			t.Fatalf("unexpected projection path: %s", body.Path)
		}
		if body.TargetPath != "/tonel/PharoImageFS/New.class.st" {
			t.Fatalf("unexpected target projection path: %s", body.TargetPath)
		}

		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{}`))
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL + "/projection")
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Rename(t.Context(), "/tonel/PharoImageFS/Old.class.st", "/tonel/PharoImageFS/New.class.st"); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPClientMkdirPostsProjectionPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/projection/mkdir" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		var body pathRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Path != "/tonel/NewPackage" {
			t.Fatalf("unexpected projection path: %s", body.Path)
		}

		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{}`))
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL + "/projection")
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Mkdir(t.Context(), "/tonel/NewPackage"); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPClientRmdirPostsProjectionPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/projection/rmdir" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		var body pathRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Path != "/tonel/OldPackage" {
			t.Fatalf("unexpected projection path: %s", body.Path)
		}

		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{}`))
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL + "/projection")
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Rmdir(t.Context(), "/tonel/OldPackage"); err != nil {
		t.Fatal(err)
	}
}

// zincListener accepts as Zinc's ZnManagingMultiThreadedServer does: past 32
// open connections it answers 503 and closes, before reading a request.
type zincListener struct {
	net.Listener
	active atomic.Int64
	opened atomic.Int64
}

type zincConnection struct {
	net.Conn
	once     sync.Once
	listener *zincListener
}

func (c *zincConnection) Close() error {
	c.once.Do(func() { c.listener.active.Add(-1) })
	return c.Conn.Close()
}

func (l *zincListener) Accept() (net.Conn, error) {
	for {
		connection, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		l.opened.Add(1)
		if l.active.Load() >= 32 {
			_, _ = connection.Write([]byte("HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"))
			_ = connection.Close()
			continue
		}
		l.active.Add(1)
		return &zincConnection{Conn: connection, listener: l}, nil
	}
}

func TestHTTPClientStaysUnderZincsConnectionCap(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := &zincListener{Listener: inner}
	server := &http.Server{Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		time.Sleep(2 * time.Millisecond)
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"entry":{"name":"Tonel","kind":"directory"}}`))
	})}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()

	client, err := NewHTTPClient("http://" + inner.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	var failures atomic.Int64
	var callers sync.WaitGroup
	for range 64 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			for range 20 {
				if _, err := client.Stat(t.Context(), "/tonel"); err != nil {
					failures.Add(1)
				}
			}
		}()
	}
	callers.Wait()
	if failures.Load() != 0 {
		t.Fatalf("%d of %d calls failed", failures.Load(), 64*20)
	}
	if opened := listener.opened.Load(); opened > maxImageConnections {
		t.Fatalf("opened %d connections, more than %d", opened, maxImageConnections)
	}
}
