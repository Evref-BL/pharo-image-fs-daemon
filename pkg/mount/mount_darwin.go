//go:build darwin

package mount

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
	nfs "github.com/willscott/go-nfs"
	"golang.org/x/sys/unix"
)

// Mount serves the projection over NFS and mounts it at mountPoint with the
// system's mount_nfs, as the user running the daemon. It returns once the
// projection is unmounted: on SIGINT, SIGTERM or SIGHUP, or when the image is
// gone.
func Mount(mountPoint string, client protocol.Client, config Config) error {
	if err := ensureMountPoint(mountPoint); err != nil {
		return err
	}
	// The mount table names a mount point by its real path.
	mountPoint, err := filepath.EvalSymlinks(mountPoint)
	if err != nil {
		return err
	}

	logger := log.New(os.Stderr, "pharo-image-fs: ", log.LstdFlags)
	server, err := startNFSServer(client, logger)
	if err != nil {
		return err
	}
	defer server.stop()

	arguments := mountNFSArguments(server.port(), mountPoint, config)
	if config.Debug {
		nfs.Log.SetLevel(nfs.TraceLevel)
		logger.Printf("mount_nfs %s", strings.Join(arguments, " "))
	}
	if output, err := exec.Command("/sbin/mount_nfs", arguments...).CombinedOutput(); err != nil {
		return fmt.Errorf("mount_nfs failed: %v: %s", err, strings.TrimSpace(string(output)))
	}
	mounted, ok := mountedAt(mountPoint)
	if !ok {
		return fmt.Errorf("mount_nfs succeeded, but nothing is mounted at %s", mountPoint)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, stopSignals()...)
	gone := make(chan struct{})
	stopWatching := watchImage(client, logger, func() { close(gone) })
	defer stopWatching()

	select {
	case <-signals:
	case <-gone:
	case err := <-server.done:
		logger.Printf("the NFS server stopped: %v", err)
	}
	unmount(mountPoint, mounted.Fsid, logger)
	return nil
}

// mountNFSArguments answers mount_nfs's arguments for a server on port.
//
//   - noresvport lets a user mount it: the client needs no privileged port.
//   - nordirplus keeps a listing to one call, for names only.
//   - actimeo=1 and nonegnamecache show changes made in the image within a
//     second; the client checks a file again whenever it opens it.
//   - soft,intr fail calls to a gone daemon instead of hanging them.
//   - nobrowse keeps Finder and Spotlight off it.
func mountNFSArguments(port int, mountPoint string, config Config) []string {
	options := []string{
		"vers=3", "tcp",
		fmt.Sprintf("port=%d", port), fmt.Sprintf("mountport=%d", port),
		"noresvport", "locallocks", "nordirplus",
		"actimeo=1", "nonegnamecache",
		"soft", "intr", "timeo=100", "retrans=5",
		"nobrowse",
	}
	export := "pharo-image-fs"
	for _, option := range config.MountOptions {
		if name, ok := strings.CutPrefix(option, "volname="); ok {
			if name = exportNameFor(name); name != "" {
				export = name
			}
			continue
		}
		options = append(options, option)
	}
	return []string{"-o", strings.Join(options, ","), "127.0.0.1:/" + export, mountPoint}
}

var unsafeExportCharacters = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// exportNameFor answers name as the export path the mount table shows, so
// that a projection's mount can be told from another's.
func exportNameFor(name string) string {
	return strings.Trim(unsafeExportCharacters.ReplaceAllString(name, "-"), "-")
}

// unmount unmounts the file system with fsid at mountPoint, waiting while it
// is busy for as long as it takes, and only while it is the one there: a
// projection another daemon mounted there since is left alone. It never
// forces it: a forced unmount, or a server gone before its mount, leaves the
// client calls in flight to error paths of the kernel's NFS client, and the
// server must answer until the mount is gone.
func unmount(mountPoint string, fsid unix.Fsid, logger *log.Logger) {
	for attempt := 0; isMountedWith(mountPoint, fsid); attempt++ {
		output, err := exec.Command("/sbin/umount", mountPoint).CombinedOutput()
		if err == nil {
			return
		}
		if attempt%30 == 0 {
			logger.Printf("umount %s: %v: %s; trying again while it is busy", mountPoint, err, strings.TrimSpace(string(output)))
		}
		time.Sleep(time.Second)
	}
}

func isMountedWith(mountPoint string, fsid unix.Fsid) bool {
	mounted, ok := mountedAt(mountPoint)
	return ok && mounted.Fsid == fsid
}

// mountedAt answers the file system mounted at mountPoint, a real path, from
// the kernel's mount table: the last one mounted there, which is the one seen.
// The table answers without asking the file system, which a dead server would
// hold up.
func mountedAt(mountPoint string) (unix.Statfs_t, bool) {
	count, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return unix.Statfs_t{}, false
	}
	// Room for mounts made meanwhile.
	table := make([]unix.Statfs_t, count+8)
	count, err = unix.Getfsstat(table, unix.MNT_NOWAIT)
	if err != nil {
		return unix.Statfs_t{}, false
	}
	for index := count - 1; index >= 0; index-- {
		if unix.ByteSliceToString(table[index].Mntonname[:]) == mountPoint {
			return table[index], true
		}
	}
	return unix.Statfs_t{}, false
}
