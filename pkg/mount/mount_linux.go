//go:build linux

package mount

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

// mountTimeout bounds how long fusermount3 may take to mount: on Ubuntu
// 26.04 its AppArmor profile can hang it (Launchpad bug 2165030).
const mountTimeout = 10 * time.Second

// Mount serves the projection over FUSE at mountPoint, through the setuid
// fusermount3, as the user running the daemon. It returns once the
// projection is unmounted: on SIGINT, SIGTERM or SIGHUP, when the image is
// gone, or when something else unmounted it.
func Mount(mountPoint string, client protocol.Client, config Config) error {
	if err := ensureMountPoint(mountPoint); err != nil {
		if errors.Is(err, syscall.ENOTCONN) {
			return fmt.Errorf("%w: a daemon that is gone left its mount there; run fusermount3 -u %s", err, mountPoint)
		}
		return err
	}
	// The mount table names a mount point by its real path.
	mountPoint, err := filepath.EvalSymlinks(mountPoint)
	if err != nil {
		return err
	}
	if mounted, ok := linuxMountAt(mountPoint); ok {
		return fmt.Errorf("%s is already mounted (%s); unmount it with fusermount3 -u %s", mountPoint, mounted.fsType, mountPoint)
	}
	if err := checkFuse(); err != nil {
		return err
	}
	// From now on, so that a signal while mounting unmounts once mounted
	// rather than ending the daemon with its mount left behind.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, stopSignals()...)
	defer signal.Stop(signals)

	logger := log.New(os.Stderr, "pharo-image-fs: ", log.LstdFlags)
	fsys := newFuseFileSystem(client)
	fsys.logger = logger
	name, others := mountNameAndOptions(config.MountOptions)
	server, err := mountWithin(mountTimeout, mountPoint, fsys.root(), fuseOptions(fsys, name, others, config.Debug, logger))
	if err != nil {
		return err
	}
	served := make(chan struct{})
	go func() {
		server.Wait()
		close(served)
	}()
	own, ok := linuxMountAt(mountPoint)
	if !ok {
		_ = server.Unmount()
		return fmt.Errorf("fusermount3 succeeded, but nothing is mounted at %s", mountPoint)
	}
	if server.KernelSettings().Flags64()&fuse.CAP_ATOMIC_O_TRUNC == 0 {
		unmountFuse(server, mountPoint, own.device, served, logger)
		return errors.New("this kernel's FUSE does not pass O_TRUNC with an open, which the mount needs to tell a save from truncate(2)")
	}

	gone := make(chan struct{})
	stopWatching := watchImage(client, logger, func() { close(gone) })
	defer stopWatching()

	select {
	case <-signals:
	case <-gone:
	case <-served:
		return nil
	}
	unmountFuse(server, mountPoint, own.device, served, logger)
	<-served
	return nil
}

// mountWithin mounts root at mountPoint, or answers why not, within timeout.
func mountWithin(timeout time.Duration, mountPoint string, root *fuseNode, options *gofuse.Options) (*fuse.Server, error) {
	type mounted struct {
		server *fuse.Server
		err    error
	}
	done := make(chan mounted, 1)
	go func() {
		server, err := gofuse.Mount(mountPoint, root, options)
		done <- mounted{server, err}
	}()
	select {
	case result := <-done:
		if result.err != nil {
			return nil, mountFailure(mountPoint, result.err)
		}
		return result.server, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("fusermount3 has not mounted %s after %v; on Ubuntu 26.04, its AppArmor profile is known to hang it (Launchpad bug 2165030)", mountPoint, timeout)
	}
}

// mountFailure says why fusermount3 may have refused mountPoint.
func mountFailure(mountPoint string, err error) error {
	if _, statErr := os.Stat("/etc/apparmor.d/fusermount3"); statErr == nil {
		return fmt.Errorf("mount %s: %w; this system's AppArmor lets fusermount3 mount only under the home directory, /tmp, /mnt, /media and /run/user", mountPoint, err)
	}
	return fmt.Errorf("mount %s: %w", mountPoint, err)
}

// checkFuse says what is missing for a user to mount FUSE, as go-fuse
// mounts it: a setuid fusermount3, or fusermount, and /dev/fuse.
func checkFuse() error {
	helper, err := fusermountPath()
	if err != nil {
		return errors.New("FUSE needs fusermount3, which the fuse3 package installs")
	}
	if os.Geteuid() != 0 {
		info, err := os.Stat(helper)
		if err != nil {
			return fmt.Errorf("FUSE needs fusermount3: %w", err)
		}
		owner, _ := info.Sys().(*syscall.Stat_t)
		if info.Mode()&fs.ModeSetuid == 0 || owner == nil || owner.Uid != 0 {
			return fmt.Errorf("%s is not setuid root, so it cannot mount for this user; a fusermount3 earlier on PATH may hide the system's", helper)
		}
	}
	info, err := os.Stat("/dev/fuse")
	if err != nil || info.Mode()&fs.ModeCharDevice == 0 {
		return errors.New("FUSE needs /dev/fuse, which this system does not have: the kernel module is not loaded, or a container was started without --device /dev/fuse")
	}
	if err := unix.Access("/dev/fuse", unix.R_OK|unix.W_OK); err != nil {
		return fmt.Errorf("this user cannot open /dev/fuse: %w", err)
	}
	return nil
}

// fusermountPath answers the helper go-fuse mounts with: fusermount3, then
// fusermount, on PATH or in /bin.
func fusermountPath() (string, error) {
	for _, name := range []string{"fusermount3", "fusermount"} {
		if found, err := exec.LookPath(name); err == nil {
			return found, nil
		}
		if _, err := os.Stat("/bin/" + name); err == nil {
			return "/bin/" + name, nil
		}
	}
	return "", exec.ErrNotFound
}

// unmountFuse unmounts the projection the daemon mounted at mountPoint,
// waiting while it is busy for as long as it takes, and only while it is the
// one there: a projection another daemon mounted there since is left alone.
// It never forces it nor detaches it lazily, which would leave whoever still
// has it serving from a gone image.
func unmountFuse(server *fuse.Server, mountPoint string, device string, served <-chan struct{}, logger *log.Logger) {
	for attempt := 0; ; attempt++ {
		select {
		case <-served:
			return
		default:
		}
		if mounted, ok := linuxMountAt(mountPoint); !ok || mounted.device != device {
			return
		}
		err := server.Unmount()
		if err == nil {
			return
		}
		if attempt%30 == 0 {
			logger.Printf("fusermount3 -u %s: %v; trying again while it is busy", mountPoint, err)
		}
		select {
		case <-served:
			return
		case <-time.After(time.Second):
		}
	}
}

// linuxMountAt answers what this process's mount table has at mountPoint.
func linuxMountAt(mountPoint string) (mountEntry, bool) {
	table, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return mountEntry{}, false
	}
	defer table.Close()
	return lastMountAt(parseMountInfo(table), mountPoint)
}
