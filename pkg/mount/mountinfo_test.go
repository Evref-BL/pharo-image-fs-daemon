package mount

import (
	"strings"
	"testing"
)

func TestMountInfoNamesWhatIsMountedWhere(t *testing.T) {
	table := strings.Join([]string{
		`22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw`,
		`36 22 0:45 / /home/me/my\040image rw,nosuid,nodev,relatime shared:2 - fuse.pharo-image-fs my-image rw,user_id=1000`,
		`41 36 0:52 / /home/me/my\040image rw,nosuid,nodev,relatime shared:3 - fuse.pharo-image-fs other rw,user_id=1000`,
		`not a mountinfo line`,
	}, "\n")
	entries := parseMountInfo(strings.NewReader(table))
	if len(entries) != 3 {
		t.Fatalf("entries: %+v", entries)
	}
	mounted, ok := lastMountAt(entries, "/home/me/my image")
	if !ok || mounted.device != "0:52" || mounted.fsType != "fuse.pharo-image-fs" || mounted.source != "other" {
		t.Fatalf("the last mount made at a path should be the one there, got %+v", mounted)
	}
	if _, ok := lastMountAt(entries, "/home/me"); ok {
		t.Fatalf("a path nothing is mounted at should have no mount")
	}
	if got := unescapeMountField(`a\134b\011c\012d\`); got != "a\\b\tc\nd\\" {
		t.Fatalf("escapes: %q", got)
	}
}
