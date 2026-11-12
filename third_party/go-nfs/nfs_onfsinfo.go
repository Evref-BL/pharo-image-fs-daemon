package nfs

// Changed for pharo-image-fs: FSINFO does not claim hard links, which LINK
// refuses, and advertises the reads onRead serves and the writes the request
// framing accepts, not 1 GiB.

import (
	"bytes"
	"context"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

const (
	// FSInfoPropertyLink does the FS support hard links?
	FSInfoPropertyLink = 0x0001
	// FSInfoPropertySymlink does the FS support soft links?
	FSInfoPropertySymlink = 0x0002
	// FSInfoPropertyHomogeneous does the FS need PATHCONF calls for each file
	FSInfoPropertyHomogeneous = 0x0008
	// FSInfoPropertyCanSetTime can the FS support setting access/mod times?
	FSInfoPropertyCanSetTime = 0x0010
)

// MaxWrite is the largest write advertised. A WRITE must fit, with its RPC
// header, in maxRequestFragmentBytes.
const MaxWrite = 64 << 10

func onFSInfo(ctx context.Context, w *response, userHandle Handler) error {
	roothandle, err := xdr.ReadOpaque(w.req.Body)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs, path, err := userHandle.FromHandle(roothandle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, path)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	type fsinfores struct {
		Rtmax       uint32
		Rtpref      uint32
		Rtmult      uint32
		Wtmax       uint32
		Wtpref      uint32
		Wtmult      uint32
		Dtpref      uint32
		Maxfilesize uint64
		TimeDelta   uint64
		Properties  uint32
	}

	res := fsinfores{
		Rtmax:       MaxRead,
		Rtpref:      MaxRead,
		Rtmult:      4096,
		Wtmax:       MaxWrite,
		Wtpref:      MaxWrite,
		Wtmult:      4096,
		Dtpref:      8192,
		Maxfilesize: 1 << 62, // wild guess. this seems big.
		TimeDelta:   1,       // nanosecond precision.
		Properties:  0,
	}

	// TODO: these aren't great indications of support, really.
	// LINK always refuses, so hard links are not claimed.
	if _, ok := fs.(billy.Symlink); ok {
		res.Properties |= FSInfoPropertySymlink
	}
	// TODO: if the nfs share spans multiple virtual mounts, may need
	// to support granular PATHINFO responses.
	res.Properties |= FSInfoPropertyHomogeneous
	// TODO: not a perfect indicator
	if billy.CapabilityCheck(fs, billy.WriteCapability) {
		res.Properties |= FSInfoPropertyCanSetTime
	}
	// TODO: this whole struct should be specifiable by the userhandler.

	if err := xdr.Write(writer, res); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
