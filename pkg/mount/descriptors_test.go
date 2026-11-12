//go:build linux || darwin

package mount

import (
	"os"
	"os/exec"
	"strconv"
	"testing"

	"golang.org/x/sys/unix"
)

func TestTheDaemonClosesWhatItInherits(t *testing.T) {
	if os.Getenv("PIFS_INHERITED_DESCRIPTORS") == "1" {
		closeInheritedDescriptors()
		for descriptor := 3; descriptor <= 13; descriptor++ {
			if _, err := unix.FcntlInt(uintptr(descriptor), unix.F_GETFD, 0); err == nil {
				os.Stdout.WriteString("descriptor " + strconv.Itoa(descriptor) + " is still open\n")
				os.Exit(1)
			}
		}
		os.Exit(0)
	}

	// Eleven inherited descriptors, 3 to 13: past what a shell could close.
	var inherited []*os.File
	for range 11 {
		file, err := os.Open(os.DevNull)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer file.Close()
		inherited = append(inherited, file)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestTheDaemonClosesWhatItInherits$")
	child.Env = append(os.Environ(), "PIFS_INHERITED_DESCRIPTORS=1")
	child.ExtraFiles = inherited
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("the daemon should close every descriptor it inherited: %v: %s", err, output)
	}
}
