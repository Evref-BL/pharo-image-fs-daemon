//go:build linux || darwin

package mount

import (
	"os"
	"runtime"
	"strconv"

	"golang.org/x/sys/unix"
)

// closeInheritedDescriptors closes the descriptors above standard error the
// daemon was started with. The image that starts it passes its own down, its
// listening sockets among them, which the daemon would keep open after the
// image is gone, and never notice it went. Every descriptor the daemon opens
// itself is closed on exec, which tells the two apart.
func closeInheritedDescriptors() {
	directory := "/dev/fd"
	if runtime.GOOS == "linux" {
		directory = "/proc/self/fd"
	}
	// Names only: a stat of each would fail on the listing's own
	// descriptor, gone by then.
	listing, err := os.Open(directory)
	if err != nil {
		return
	}
	names, _ := listing.Readdirnames(-1)
	_ = listing.Close()
	for _, name := range names {
		descriptor, err := strconv.Atoi(name)
		if err != nil || descriptor <= 2 {
			continue
		}
		flags, err := unix.FcntlInt(uintptr(descriptor), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC != 0 {
			continue
		}
		_ = unix.Close(descriptor)
	}
}
