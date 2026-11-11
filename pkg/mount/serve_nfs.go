package mount

import (
	"context"
	"net"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
	nfs "github.com/willscott/go-nfs"
)

// nfsServer serves the projection over NFSv3 on a loopback port of its own.
type nfsServer struct {
	fsys     *nfsFileSystem
	listener net.Listener
	cancel   context.CancelFunc
	done     chan error
}

// startNFSServer starts serving client's projection on 127.0.0.1, on a port
// the system picks.
func startNFSServer(client protocol.Client, logger projectionLogger) (*nfsServer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	fsys := newNFSFileSystem(client)
	fsys.logger = logger
	ctx, cancel := context.WithCancel(context.Background())
	server := &nfsServer{fsys: fsys, listener: listener, cancel: cancel, done: make(chan error, 1)}
	nfsServerInstance := &nfs.Server{Handler: newNFSHandler(fsys), Context: ctx}
	go func() {
		server.done <- nfsServerInstance.Serve(listener)
	}()
	return server, nil
}

func (s *nfsServer) port() int {
	return s.listener.Addr().(*net.TCPAddr).Port
}

func (s *nfsServer) stop() {
	s.cancel()
	_ = s.listener.Close()
}

func init() {
	// go-nfs logs every refused call at error level, and a projection refuses
	// by design: read-only files, editors probing for names.
	nfs.Log.SetLevel(nfs.PanicLevel)
}
