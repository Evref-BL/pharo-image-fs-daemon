//go:build linux

package mount

import (
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestTheAdapterReadsLinuxsOpenFlags(t *testing.T) {
	if linuxOpenTruncate != syscall.O_TRUNC || linuxOpenAppend != syscall.O_APPEND || linuxOpenAccessMode != syscall.O_ACCMODE || linuxOpenReadOnly != syscall.O_RDONLY {
		t.Fatalf("the adapter's open flags should be Linux's")
	}
	if renameNoReplace != unix.RENAME_NOREPLACE || renameExchange != unix.RENAME_EXCHANGE || renameWhiteout != unix.RENAME_WHITEOUT {
		t.Fatalf("the adapter's rename flags should be Linux's")
	}
}

func TestAThreadBelongsToItsProcess(t *testing.T) {
	if group := threadGroupOf(uint32(unix.Gettid())); group != uint32(os.Getpid()) {
		t.Fatalf("this thread should belong to process %d, got %d", os.Getpid(), group)
	}
	if group := threadGroupOf(1 << 30); group != 1<<30 {
		t.Fatalf("a thread /proc does not know should stand for itself, got %d", group)
	}
}

// mountTestOnly skips a test that mounts unless PIFS_MOUNT_TEST=1, which CI
// sets: mounts belong on machines that can be thrown away.
func mountTestOnly(t *testing.T) {
	t.Helper()
	if os.Getenv("PIFS_MOUNT_TEST") != "1" {
		t.Skip("mounts a file system; set PIFS_MOUNT_TEST=1 on a disposable machine")
	}
	if err := checkFuse(); err != nil {
		t.Fatalf("this machine cannot mount FUSE: %v", err)
	}
}

// TestMountedProjectionOnLinux mounts a projection through fusermount3, as
// the daemon does, and works on it as a shell and its tools would.
func TestMountedProjectionOnLinux(t *testing.T) {
	mountTestOnly(t)

	other := "/tonel/Kernel/Other.class.st"
	client := newGatedClient(map[string]string{
		objectPath:                    "Class { #name : 'Object' }\n",
		other:                         "Class { #name : 'Other' }\n",
		"/critiques/Kernel/Object.md": "No critique.\n",
	})
	mountPoint := filepath.Join(t.TempDir(), "image")
	mounted := make(chan error, 1)
	go func() {
		mounted <- Mount(mountPoint, client, Config{MountOptions: []string{"volname=test image"}})
	}()
	realMountPoint := waitMounted(t, mountPoint, mounted)
	defer func() {
		if output, err := exec.Command("fusermount3", "-u", realMountPoint).CombinedOutput(); err != nil {
			t.Errorf("fusermount3 -u: %v: %s", err, output)
		}
		if err := within(t, mounted, "the daemon's end after an unmount from outside"); err != nil {
			t.Errorf("the daemon should end without error once unmounted, got %v", err)
		}
		if _, ok := linuxMountAt(realMountPoint); ok {
			t.Errorf("still mounted")
		}
	}()
	path := func(projectionPath string) string { return filepath.Join(realMountPoint, projectionPath) }
	object := path(objectPath)
	compiles := func(do func()) []string {
		before := len(client.writes())
		do()
		return client.writes()[before:]
	}

	t.Run("the mount table names it", func(t *testing.T) {
		entry, _ := linuxMountAt(realMountPoint)
		if entry.fsType != "fuse.pharo-image-fs" || entry.source != "test-image" {
			t.Fatalf("mount table: %+v", entry)
		}
	})

	t.Run("lists and reads", func(t *testing.T) {
		entries, err := os.ReadDir(path("/tonel/Kernel"))
		if err != nil {
			t.Fatalf("readdir: %v", err)
		}
		names := []string{}
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		if !slices.Equal(names, []string{"Object.class.st", "Other.class.st"}) {
			t.Fatalf("listing: %v", names)
		}
		if contents, err := os.ReadFile(object); err != nil || string(contents) != "Class { #name : 'Object' }\n" {
			t.Fatalf("read: %q, %v", contents, err)
		}
	})

	t.Run("a save compiles at close", func(t *testing.T) {
		writes := compiles(func() {
			if err := os.WriteFile(object, []byte("Class { #name : 'Object', #comment : 'saved' }\n"), 0o644); err != nil {
				t.Fatalf("save: %v", err)
			}
		})
		if len(writes) != 1 || !strings.Contains(writes[0], "saved") {
			t.Fatalf("the save should compile once, got %v", writes)
		}
	})

	t.Run("a refused save fails the close, every time", func(t *testing.T) {
		for range 20 {
			if err := os.WriteFile(object, []byte("syntax error"), 0o644); !errors.Is(err, syscall.EIO) {
				t.Fatalf("a refused save should fail with EIO, got %v", err)
			}
			if contents, _ := os.ReadFile(object); strings.Contains(string(contents), "syntax error") {
				t.Fatalf("a refused save should leave the image's version, read %q", contents)
			}
		}
	})

	t.Run("fsync reports a refusal", func(t *testing.T) {
		file, err := os.OpenFile(object, os.O_WRONLY|os.O_TRUNC, 0)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if _, err := file.WriteString("syntax error"); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := file.Sync(); !errors.Is(err, syscall.EIO) {
			t.Fatalf("fsync should report the refusal, got %v", err)
		}
		_ = file.Close()
	})

	t.Run("a safe save keeps the temp file's number", func(t *testing.T) {
		temp := path("/tonel/Kernel/.Object.class.st.tmp")
		if err := os.WriteFile(temp, []byte("Class { #name : 'Object', #comment : 'safe' }\n"), 0o644); err != nil {
			t.Fatalf("write temp: %v", err)
		}
		tempInfo, err := os.Stat(temp)
		if err != nil {
			t.Fatalf("stat temp: %v", err)
		}
		if err := os.Rename(temp, object); err != nil {
			t.Fatalf("rename: %v", err)
		}
		objectInfo, err := os.Stat(object)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if tempInfo.Sys().(*syscall.Stat_t).Ino != objectInfo.Sys().(*syscall.Stat_t).Ino {
			t.Fatalf("a file renamed over a class should keep its number")
		}
		if contents := client.contents(objectPath); !strings.Contains(contents, "safe") {
			t.Fatalf("the rename should compile, the image has %q", contents)
		}
	})

	t.Run("a lookup answers while a compile runs", func(t *testing.T) {
		started, open := client.holdNext("write")
		defer open()
		saved := make(chan error, 1)
		go func() { saved <- os.WriteFile(object, []byte("Class { #name : 'Object' }\n"), 0o644) }()
		waitHeld(t, started)
		looked := make(chan error, 1)
		go func() {
			_, err := os.Stat(path(other))
			looked <- err
		}()
		if err := within(t, looked, "a stat of another file"); err != nil {
			t.Fatalf("stat: %v", err)
		}
		open()
		if err := within(t, saved, "the save"); err != nil {
			t.Fatalf("save: %v", err)
		}
	})

	t.Run("read-only files refuse writing", func(t *testing.T) {
		err := os.WriteFile(path("/critiques/Kernel/Object.md"), []byte("x"), 0o644)
		if !errors.Is(err, syscall.EACCES) && !errors.Is(err, syscall.EROFS) && !errors.Is(err, syscall.EPERM) {
			t.Fatalf("expected a refusal, got %v", err)
		}
	})

	t.Run("an image change shows at the next open", func(t *testing.T) {
		if _, err := os.ReadFile(object); err != nil {
			t.Fatalf("read: %v", err)
		}
		client.put(objectPath, "Class { #name : 'Object', #comment : 'changed in the image, and longer' }\n")
		if contents, err := os.ReadFile(object); err != nil || !strings.Contains(string(contents), "changed in the image, and longer") {
			t.Fatalf("an open should read the image's change whole, read %q, %v", contents, err)
		}
	})

	t.Run("extended attributes are refused", func(t *testing.T) {
		err := unix.Setxattr(object, "user.test", []byte("x"), 0)
		if !errors.Is(err, unix.EOPNOTSUPP) && !errors.Is(err, unix.ENOTSUP) {
			t.Fatalf("setting an attribute should fail as unsupported, got %v", err)
		}
	})

	t.Run("tools save once", func(t *testing.T) {
		shell := func(t *testing.T, script string) {
			t.Helper()
			command := exec.Command("sh", "-c", script)
			command.Dir = t.TempDir()
			command.Env = append(os.Environ(), "F="+object)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("%s: %v: %s", script, err, output)
			}
		}
		for _, tool := range []struct{ name, script, saved string }{
			{"sed -i", `sed -i 's/Object/Object/; s/}$/, #comment : '"'sed'"' }/' "$F"`, "sed"},
			{"perl -i", `perl -pi -e 's/sed/perl/' "$F"`, "perl"},
			{"a shell group", `{ printf "Class { #name : 'Object', "; cat <<'EOF'
#comment : 'group' }
EOF
} > "$F"`, "group"},
			{"a here-document", `cat > "$F" <<'EOF'
Class { #name : 'Object', #comment : 'heredoc' }
EOF`, "heredoc"},
			{"cp", `printf "Class { #name : 'Object', #comment : 'cp' }\n" > source.st && cp source.st "$F"`, "cp"},
			{"python", `python3 -c "
import sys
with open(sys.argv[1], 'r+') as f:
    f.write(\"Class { #name : 'Object', #comment : 'python' }\n\")
    f.truncate()
" "$F"`, "python"},
		} {
			t.Run(tool.name, func(t *testing.T) {
				writes := compiles(func() { shell(t, tool.script) })
				if len(writes) != 1 || !strings.Contains(writes[0], tool.saved) {
					t.Fatalf("%s should compile once, got %v", tool.name, writes)
				}
			})
		}
		t.Run("truncate to the size the file has", func(t *testing.T) {
			info, err := os.Stat(object)
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
			writes := compiles(func() { shell(t, "truncate -s "+strconv.FormatInt(info.Size(), 10)+` "$F"`) })
			if len(writes) != 0 {
				t.Fatalf("a size the file has should change nothing, got %v", writes)
			}
		})
	})

	t.Run("removing an open file removes its class, and leaves its reader stale", func(t *testing.T) {
		reader, err := os.Open(path(other))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer reader.Close()
		if err := os.Remove(path(other)); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if client.has(other) {
			t.Fatalf("removing a file should remove its class")
		}
		// As for a file removed on an NFS server.
		if contents, err := io.ReadAll(reader); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("a removed file's descriptor should read stale, got %q, %v", contents, err)
		}
	})
}

// TestTheDaemonOnLinux runs the daemon, as PIFS starts it, against a
// projection served over HTTP, and ends it as PIFS and a gone image do.
func TestTheDaemonOnLinux(t *testing.T) {
	mountTestOnly(t)

	daemon := filepath.Join(t.TempDir(), "pharo-image-fs")
	build := exec.Command("go", "build", "-o", daemon, "./cmd/pharo-image-fs")
	build.Dir = moduleRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v: %s", err, output)
	}
	start := func(t *testing.T) (*httptest.Server, *exec.Cmd, string, chan error) {
		image := httptest.NewServer(projectionHandler(newMemoryClient(map[string]string{objectPath: "source"})))
		mountPoint := filepath.Join(t.TempDir(), "image")
		command := exec.Command(daemon, "--endpoint", image.URL+"/projection", "--mount-option", "volname=lifecycle", mountPoint)
		command.Stderr = os.Stderr
		if err := command.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		exited := make(chan error, 1)
		go func() { exited <- command.Wait() }()
		return image, command, waitMounted(t, mountPoint, exited), exited
	}

	t.Run("SIGTERM unmounts and ends it", func(t *testing.T) {
		image, command, mountPoint, exited := start(t)
		defer image.Close()
		if err := command.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("signal: %v", err)
		}
		if err := within(t, exited, "the daemon's end"); err != nil {
			t.Fatalf("the daemon should end cleanly, got %v", err)
		}
		if _, ok := linuxMountAt(mountPoint); ok {
			t.Fatalf("SIGTERM should unmount")
		}
	})

	t.Run("a gone image unmounts and ends it", func(t *testing.T) {
		image, _, mountPoint, exited := start(t)
		image.Close()
		select {
		case err := <-exited:
			if err != nil {
				t.Fatalf("the daemon should end cleanly, got %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("the daemon should notice its image is gone")
		}
		if _, ok := linuxMountAt(mountPoint); ok {
			t.Fatalf("a gone image should unmount")
		}
	})
}

// waitMounted waits until something is mounted at mountPoint, and answers
// its real path; ended says the daemon ended first.
func waitMounted(t *testing.T, mountPoint string, ended <-chan error) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-ended:
			t.Fatalf("the daemon ended before mounting: %v", err)
		default:
		}
		if real, err := filepath.EvalSymlinks(mountPoint); err == nil {
			if _, ok := linuxMountAt(real); ok {
				return real
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("nothing was mounted at %s", mountPoint)
	return ""
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	output, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	return filepath.Dir(strings.TrimSpace(string(output)))
}
