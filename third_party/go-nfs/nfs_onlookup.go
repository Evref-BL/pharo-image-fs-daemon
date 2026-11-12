package nfs

// Changed for pharo-image-fs: an error from the file system keeps its errno's
// meaning (see pifs.go), STALE included, and the attributes of the object
// found are taken through the handle answered.

import (
	"bytes"
	"context"
	"os"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

func lookupSuccessResponse(handle []byte, entPath, dirPath []string, fs billy.Filesystem) ([]byte, error) {
	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return nil, err
	}
	if err := xdr.Write(writer, handle); err != nil {
		return nil, err
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, entPath)); err != nil {
		return nil, err
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, dirPath)); err != nil {
		return nil, err
	}
	return writer.Bytes(), nil
}

func onLookup(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = opAttrErrorFormatter
	obj := DirOpArg{}
	err := xdr.Read(w.req.Body, &obj)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}

	fs, p, err := userHandle.FromHandle(obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	dirInfo, err := fs.Lstat(fs.Join(p...))
	if err != nil {
		return &NFSStatusError{statusFromError(err, NFSStatusNotDir), err}
	}
	if !dirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}

	// Special cases for "." and ".."
	if bytes.Equal(obj.Filename, []byte(".")) {
		resp, err := lookupSuccessResponse(obj.Handle, p, p, fs)
		if err != nil {
			return &NFSStatusError{NFSStatusServerFault, err}
		}
		if err := w.Write(resp); err != nil {
			return &NFSStatusError{NFSStatusServerFault, err}
		}
		return nil
	}
	if bytes.Equal(obj.Filename, []byte("..")) {
		if len(p) == 0 {
			return &NFSStatusError{NFSStatusAccess, os.ErrPermission}
		}
		pPath := p[0 : len(p)-1]
		pHandle := userHandle.ToHandle(fs, pPath)
		resp, err := lookupSuccessResponse(pHandle, pPath, p, fs)
		if err != nil {
			return &NFSStatusError{NFSStatusServerFault, err}
		}
		if err := w.Write(resp); err != nil {
			return &NFSStatusError{NFSStatusServerFault, err}
		}
		return nil
	}

	reqPath := append(p, string(obj.Filename))
	if _, err = fs.Lstat(fs.Join(reqPath...)); err != nil {
		return &NFSStatusError{statusFromError(err, NFSStatusNoEnt), err}
	}

	newHandle := userHandle.ToHandle(fs, reqPath)
	// The object's attributes are taken through the handle answered, so they
	// are its own even if a rename moved the name meanwhile.
	_, objPath, err := userHandle.FromHandle(newHandle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	resp, err := lookupSuccessResponse(newHandle, objPath, p, fs)
	if err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := w.Write(resp); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
