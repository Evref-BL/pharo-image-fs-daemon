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
)

// TestMountedProjection mounts a projection with the system's mount_nfs, as
// the user, and works on it as a shell would. It runs only where
// PIFS_MOUNT_TEST=1, which CI sets: a Mac running it once panicked in the
// kernel's NFS client, so it belongs on machines that can be thrown away.
func TestMountedProjection(t *testing.T) {
	if os.Getenv("PIFS_MOUNT_TEST") != "1" {
		t.Skip("mounts a file system; set PIFS_MOUNT_TEST=1 on a disposable machine")
	}

	client := newMemoryClient(map[string]string{
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
	defer func() {
		unmount(mountPoint, log.New(io.Discard, "", 0))
		if isMounted(mountPoint) {
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
		if err := os.Rename(temp, object); err != nil {
			t.Fatalf("rename: %v", err)
		}
		if contents := client.contents(objectPath); !strings.Contains(contents, "renamed") {
			t.Fatalf("the image did not get the rename: %q", contents)
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
}
