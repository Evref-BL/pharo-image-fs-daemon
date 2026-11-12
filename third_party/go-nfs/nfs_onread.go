package nfs

// Changed for pharo-image-fs: an offset past 2^63 answers INVAL, the
// attributes are taken before the data, and an error from the file system
// keeps its errno's meaning (see pifs.go), STALE included.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"

	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type nfsReadArgs struct {
	Handle []byte
	Offset uint64
	Count  uint32
}

type nfsReadResponse struct {
	Count uint32
	EOF   uint32
	Data  []byte
}

// MaxRead is the advertised largest buffer the server is willing to read
const MaxRead = 1 << 24

func onRead(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = opAttrErrorFormatter
	var obj nfsReadArgs
	err := xdr.Read(w.req.Body, &obj)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	if obj.Offset > math.MaxInt64 {
		return &NFSStatusError{NFSStatusInval, os.ErrInvalid}
	}
	fs, path, err := userHandle.FromHandle(obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	// The attributes come before the data: a change between the two then
	// pairs new data with old attributes, which the client sees change and
	// reads again, never old data with new ones, which it would keep.
	fullPath := fs.Join(path...)
	info, err := fs.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{statusFromError(err, NFSStatusAccess), err}
	}

	fh, err := fs.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{statusFromError(err, NFSStatusAccess), err}
	}
	defer fh.Close()

	resp := nfsReadResponse{}
	setEOF := false
	if int64(obj.Offset) >= info.Size() {
		obj.Count = 0
		setEOF = true
	} else if info.Size()-int64(obj.Offset) <= int64(obj.Count) {
		obj.Count = uint32(uint64(info.Size()) - obj.Offset)
		setEOF = true
	}
	if obj.Count > MaxRead {
		obj.Count = MaxRead
	}
	resp.Data = make([]byte, obj.Count)
	// todo: multiple reads if size isn't full
	cnt, err := fh.ReadAt(resp.Data, int64(obj.Offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return &NFSStatusError{statusFromError(err, NFSStatusIO), err}
	}
	resp.Count = uint32(cnt)
	resp.Data = resp.Data[:resp.Count]
	if errors.Is(err, io.EOF) || setEOF {
		resp.EOF = 1
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, ToFileAttribute(info, fullPath)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := xdr.Write(writer, resp); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
