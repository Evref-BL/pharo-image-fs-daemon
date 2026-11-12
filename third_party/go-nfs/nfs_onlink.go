package nfs

// Changed for pharo-image-fs: LINK read SYMLINK's arguments and answered with
// SYMLINK's result. It now reads LINK3args, and refuses in LINK3res's shape.

import (
	"context"
	"os"

	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// linkErrorBody is LINK3resfail with no attributes: the file's post_op_attr,
// then the directory's wcc_data.
var (
	linkErrorBody      = [12]byte{}
	linkErrorFormatter = errFormatterWithBody(linkErrorBody[:])
)

// onLink refuses hard links, which a billy file system cannot make; FSINFO
// does not claim them either.
func onLink(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = linkErrorFormatter
	file, err := xdr.ReadOpaque(w.req.Body)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	link := DirOpArg{}
	if err := xdr.Read(w.req.Body, &link); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	if _, _, err := userHandle.FromHandle(file); err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	if _, _, err := userHandle.FromHandle(link.Handle); err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	return &NFSStatusError{NFSStatusNotSupp, os.ErrInvalid}
}
