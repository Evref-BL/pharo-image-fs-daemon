//go:build darwin

package mount

import (
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestMountedProjection mounts a projection with the system's mount_nfs, as
// the user, and works on it as a shell would. It runs only where
// PIFS_MOUNT_TEST=1, which CI sets: a Mac running it once panicked in the
// kernel's NFS client, so it belongs on machines that can be thrown away.
func TestMountedProjection(t *testing.T) {
	if os.Getenv("PIFS_MOUNT_TEST") != "1" {
		t.Skip("mounts a file system; set PIFS_MOUNT_TEST=1 on a disposable machine")
	}

	client := newGatedClient(map[string]string{
		objectPath:                    "Class { #name : 'Object' }\n",
		"/critiques/Kernel/Object.md": "No critique.\n",
	})
	server, err := startNFSServer(client, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer server.stop()

	mountPoint := filepath.Join(t.TempDir(), "image")
	if err := ensureMountPoint(mountPoint); err != nil {
		t.Fatalf("mount point: %v", err)
	}
	arguments := mountNFSArguments(server.port(), mountPoint, Config{MountOptions: []string{"volname=test image"}})
	if output, err := exec.Command("/sbin/mount_nfs", arguments...).CombinedOutput(); err != nil {
		t.Fatalf("mount_nfs %v: %v: %s", arguments, err, output)
	}
	realMountPoint, err := filepath.EvalSymlinks(mountPoint)
	if err != nil {
		t.Fatalf("mount point: %v", err)
	}
	mounted, ok := mountedAt(realMountPoint)
	if !ok || unix.ByteSliceToString(mounted.Fstypename[:]) != "nfs" {
		t.Fatalf("the mount table should show an NFS mount at %s", realMountPoint)
	}
	defer func() {
		unmount(realMountPoint, mounted.Fsid, log.New(io.Discard, "", 0))
		if isMountedWith(realMountPoint, mounted.Fsid) {
			t.Errorf("still mounted after unmount")
		}
	}()
	object := filepath.Join(mountPoint, "tonel", "Kernel", "Object.class.st")

	t.Run("lists and reads", func(t *testing.T) {
		entries, err := os.ReadDir(filepath.Join(mountPoint, "tonel", "Kernel"))
		if err != nil {
			t.Fatalf("readdir: %v", err)
		}
		names := []string{}
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		if !slices.Contains(names, "Object.class.st") {
			t.Fatalf("listing: %v", names)
		}
		if contents, err := os.ReadFile(object); err != nil || string(contents) != "Class { #name : 'Object' }\n" {
			t.Fatalf("read: %q, %v", contents, err)
		}
	})

	t.Run("a save compiles", func(t *testing.T) {
		if err := os.WriteFile(object, []byte("Class { #name : 'Object', #comment : 'saved' }\n"), 0o644); err != nil {
			t.Fatalf("save: %v", err)
		}
		if contents := client.contents(objectPath); !strings.Contains(contents, "saved") {
			t.Fatalf("the image did not get the save: %q", contents)
		}
	})

	t.Run("a refused compile fails the save", func(t *testing.T) {
		err := os.WriteFile(object, []byte("syntax error\n"), 0o644)
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("expected the save to fail with EIO, got %v", err)
		}
		if contents := client.contents(objectPath); !strings.Contains(contents, "saved") {
			t.Fatalf("the image should keep the last good version: %q", contents)
		}
		latest, err := os.ReadFile(filepath.Join(mountPoint, "errors", "latest.txt"))
		if err != nil || !strings.Contains(string(latest), "Unexpected syntax error") {
			t.Fatalf("errors/latest.txt: %q, %v", latest, err)
		}
		if contents, err := os.ReadFile(object); err != nil || !strings.Contains(string(contents), "saved") {
			t.Fatalf("reading after a refused save should show the image's version: %q, %v", contents, err)
		}
	})

	t.Run("an editor's safe save compiles at the rename", func(t *testing.T) {
		temp := filepath.Join(mountPoint, "tonel", "Kernel", ".Object.class.st.tmp")
		if err := os.WriteFile(temp, []byte("syntax error\n"), 0o644); err != nil {
			t.Fatalf("write temp: %v", err)
		}
		if err := os.Rename(temp, object); !errors.Is(err, syscall.EIO) {
			t.Fatalf("expected the rename to fail with EIO, got %v", err)
		}
		if err := os.WriteFile(temp, []byte("Class { #name : 'Object', #comment : 'renamed' }\n"), 0o644); err != nil {
			t.Fatalf("write temp: %v", err)
		}
		tempInfo, err := os.Stat(temp)
		if err != nil {
			t.Fatalf("stat temp: %v", err)
		}
		if err := os.Rename(temp, object); err != nil {
			t.Fatalf("rename: %v", err)
		}
		if contents := client.contents(objectPath); !strings.Contains(contents, "renamed") {
			t.Fatalf("the image did not get the rename: %q", contents)
		}
		saved := tempInfo.Sys().(*syscall.Stat_t).Ino
		if id := server.fsys.ids.idFor(objectPath); id != saved {
			t.Fatalf("the server gave the saved file id %d, the temp file had %d", id, saved)
		}
		// Past actimeo=1, so that the client asks the server for the renamed
		// file's attributes instead of answering from its cache.
		time.Sleep(1500 * time.Millisecond)
		objectInfo, err := os.Stat(object)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if renamed := objectInfo.Sys().(*syscall.Stat_t).Ino; renamed != saved {
			t.Fatalf("the saved file should keep the temp file's inode %d, has %d", saved, renamed)
		}
	})

	t.Run("a lookup answers while a compile runs", func(t *testing.T) {
		client.put("/tonel/Kernel/Fresh.class.st", "Class { #name : 'Fresh' }\n")
		started, open := client.holdNext("write")
		defer open()
		saved := make(chan error, 1)
		go func() {
			saved <- os.WriteFile(object, []byte("Class { #name : 'Object', #comment : 'held' }\n"), 0o644)
		}()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("the save did not reach the image")
		}
		looked := make(chan error, 1)
		go func() {
			_, err := os.Stat(filepath.Join(mountPoint, "tonel", "Kernel", "Fresh.class.st"))
			looked <- err
		}()
		select {
		case err := <-looked:
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("a lookup waited for the compile")
		}
		open()
		if err := <-saved; err != nil {
			t.Fatalf("save: %v", err)
		}
	})

	t.Run("a writer that checks only write(2) sees a refusal", func(t *testing.T) {
		refused := "/tonel/Kernel/Refused.class.st"
		version := "Class { #name : 'Refused', #comment : 'the image keeps this version' }\n"
		client.put(refused, version)
		err := writeAsBun(filepath.Join(mountPoint, refused), []byte("Class { #name : 'Refused' } syntax error\n"))
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("expected the write to fail with EIO, got %v", err)
		}
		if contents := client.contents(refused); contents != version {
			t.Fatalf("the image should keep its version: %q", contents)
		}
		latest, err := os.ReadFile(filepath.Join(mountPoint, "errors", "latest.txt"))
		if err != nil || !strings.Contains(string(latest), "Refused.class.st") {
			t.Fatalf("errors/latest.txt: %q, %v", latest, err)
		}
		if contents, err := os.ReadFile(filepath.Join(mountPoint, refused)); err != nil || string(contents) != version {
			t.Fatalf("reading after a refused save should show the image's version: %q, %v", contents, err)
		}
	})

	t.Run("a writer that shortens a file in place compiles only its bytes", func(t *testing.T) {
		shortened := "/tonel/Kernel/Shortened.class.st"
		client.put(shortened, "Class { #name : 'Shortened', #comment : 'a longer version' }\n")
		saved := "Class { #name : 'Shortened' }\n"
		before := len(client.writes())
		if err := writeAsBun(filepath.Join(mountPoint, shortened), []byte(saved)); err != nil {
			t.Fatalf("save: %v", err)
		}
		if writes := client.writes()[before:]; !slices.Equal(writes, []string{shortened + "=" + saved}) {
			t.Fatalf("the image should compile the bytes written, once, without the old file's end, got %q", writes)
		}
		if contents, err := os.ReadFile(filepath.Join(mountPoint, shortened)); err != nil || string(contents) != saved {
			t.Fatalf("read after the save: %q, %v", contents, err)
		}
	})

	t.Run("a refused save is recorded once", func(t *testing.T) {
		saves := map[string]func(string, []byte) error{
			"/tonel/Kernel/RefusedOnce.class.st": func(name string, contents []byte) error {
				return os.WriteFile(name, contents, 0o644)
			},
			"/tonel/Kernel/RefusedOnceThroughBun.class.st": writeAsBun,
		}
		for refused, save := range saves {
			client.put(refused, "Class { #name : 'RefusedOnce', #comment : 'the image keeps this version' }\n")
			if err := save(filepath.Join(mountPoint, refused), []byte("syntax error, once\n")); !errors.Is(err, syscall.EIO) {
				t.Fatalf("expected the save of %s to fail with EIO, got %v", refused, err)
			}
		}
		// Past refusedSaveLasts: the client sends a refused file again as it
		// drops it from its cache, and one it sent later than that would
		// compile, and be recorded, again.
		time.Sleep(2 * refusedSaveLasts)
		for refused := range saves {
			recorded := 0
			for _, entry := range server.fsys.errors.EntriesIn(errorsRoot) {
				contents, _ := server.fsys.errors.Read(errorsRoot + "/" + entry.Name)
				if entry.Name != latestErrorName && strings.Contains(string(contents), "Path: "+refused+"\n") {
					recorded++
				}
			}
			if recorded != 1 {
				t.Errorf("the refused save of %s should be recorded once, was %d times", refused, recorded)
			}
		}
	})

	t.Run("read-only files refuse writing", func(t *testing.T) {
		critique := filepath.Join(mountPoint, "critiques", "Kernel", "Object.md")
		err := os.WriteFile(critique, []byte("mine"), 0o644)
		if err == nil {
			t.Fatalf("writing a critique should fail")
		}
		if !errors.Is(err, syscall.EACCES) && !errors.Is(err, syscall.EROFS) && !errors.Is(err, syscall.EPERM) {
			t.Fatalf("expected a permission error, got %v", err)
		}
	})

	t.Run("a change in the image shows within a second", func(t *testing.T) {
		client.put(objectPath, "Class { #name : 'Object', #comment : 'changed in the image' }\n")
		client.put("/tonel/Kernel/Added.class.st", "Class { #name : 'Added' }\n")
		deadline := time.Now().Add(5 * time.Second)
		for {
			contents, _ := os.ReadFile(object)
			_, statErr := os.Stat(filepath.Join(mountPoint, "tonel", "Kernel", "Added.class.st"))
			if strings.Contains(string(contents), "changed in the image") && statErr == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("still reading %q (added class: %v)", contents, statErr)
			}
			time.Sleep(200 * time.Millisecond)
		}
	})

	t.Run("removing a file removes the class", func(t *testing.T) {
		added := filepath.Join(mountPoint, "tonel", "Kernel", "Added.class.st")
		if err := os.Remove(added); err != nil {
			t.Fatalf("remove: %v", err)
		}
		if client.has("/tonel/Kernel/Added.class.st") {
			t.Fatalf("the class is still in the image")
		}
	})

	t.Run("extended attributes stay off the mount", func(t *testing.T) {
		if output, err := exec.Command("/usr/bin/xattr", "-w", "user.pifs", "1", object).CombinedOutput(); err == nil {
			t.Fatalf("setting an extended attribute should fail, it succeeded: %s", output)
		}
		for _, entry := range server.fsys.overlay.EntriesIn("/tonel/Kernel") {
			if strings.HasPrefix(entry.Name, "._") {
				t.Fatalf("the system stored attributes as %s", entry.Name)
			}
		}
	})
}

// writeAsBun writes contents to name as Bun's fs.writeFile does, which
// OpenCode saves with: it opens without O_TRUNC, writes, sets the size of
// what write(2) took and closes, and answers only what open(2) and write(2)
// answered.
func writeAsBun(name string, contents []byte) error {
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0o666)
	if err != nil {
		return err
	}
	written, err := file.Write(contents)
	_ = file.Truncate(int64(written))
	_ = file.Close()
	return err
}

func TestTheMountTableAnswersWhatIsMountedWhere(t *testing.T) {
	root, ok := mountedAt("/")
	if !ok {
		t.Fatalf("the mount table should show the root file system")
	}
	if !isMountedWith("/", root.Fsid) {
		t.Fatalf("the root should be mounted with its own fsid")
	}
	if isMountedWith("/", unix.Fsid{Val: [2]int32{root.Fsid.Val[0] + 1, root.Fsid.Val[1]}}) {
		t.Fatalf("another file system's fsid should not be taken for the one at /")
	}
	if _, ok := mountedAt(t.TempDir()); ok {
		t.Fatalf("a plain directory should not be a mount point")
	}
}
