package nfs

// This copy of go-nfs v0.0.4 is patched for pharo-image-fs, where a write
// compiles the file it writes:
//
//   - A file system implementing Committer can hold writes until they make a
//     whole file, and commit them then: a client does not tell the writer
//     that a COMMIT failed, but it does tell it that a write failed.
//   - ACCESS no longer grants modification of an object whose mode has no
//     owner write bit.
//   - Errors carrying a syscall.Errno keep their meaning (EROFS, ENOENT,
//     EEXIST, ENOTDIR, EISDIR, ENOTEMPTY, EINVAL) instead of becoming IO or
//     ACCESS.

import (
	"errors"
	"syscall"
)

// Committer is implemented by a file system that holds writes until they
// make a whole file.
type Committer interface {
	// Written is called after each write to the file at path, stable when the
	// client asked for it to be durable. It answers whether the writes held
	// for the file are committed now, or why they are refused, which fails
	// the write.
	Written(path string, stable bool) (committed bool, err error)
	// Commit commits the writes still held for the file at path.
	Commit(path string) error
}

func statusFromError(err error, fallback NFSStatus) NFSStatus {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return fallback
	}
	switch errno {
	case syscall.EROFS:
		return NFSStatusROFS
	case syscall.ENOENT:
		return NFSStatusNoEnt
	case syscall.EEXIST:
		return NFSStatusExist
	case syscall.ENOTDIR:
		return NFSStatusNotDir
	case syscall.EISDIR:
		return NFSStatusIsDir
	case syscall.ENOTEMPTY:
		return NFSStatusNotEmpty
	case syscall.EINVAL:
		return NFSStatusInval
	case syscall.EACCES, syscall.EPERM:
		return NFSStatusAccess
	case syscall.EIO:
		return NFSStatusIO
	default:
		return fallback
	}
}
