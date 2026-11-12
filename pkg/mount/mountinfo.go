package mount

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// mountEntry is a line of Linux's /proc/self/mountinfo.
type mountEntry struct {
	// device is the mounted file system's major:minor, which no other
	// mount at the same path has.
	device     string
	mountPoint string
	fsType     string
	source     string
}

// parseMountInfo reads a mountinfo table, in the order mounts were made.
func parseMountInfo(table io.Reader) []mountEntry {
	var entries []mountEntry
	lines := bufio.NewScanner(table)
	lines.Buffer(make([]byte, 64<<10), 1<<20)
	for lines.Scan() {
		mounted, filesystem, ok := strings.Cut(lines.Text(), " - ")
		if !ok {
			continue
		}
		fields := strings.Fields(mounted)
		described := strings.Fields(filesystem)
		if len(fields) < 5 || len(described) < 2 {
			continue
		}
		entries = append(entries, mountEntry{
			device:     fields[2],
			mountPoint: unescapeMountField(fields[4]),
			fsType:     described[0],
			source:     unescapeMountField(described[1]),
		})
	}
	return entries
}

// lastMountAt answers what is mounted at mountPoint, a real path: the last
// mount made there, which is the one seen.
func lastMountAt(entries []mountEntry, mountPoint string) (mountEntry, bool) {
	for index := len(entries) - 1; index >= 0; index-- {
		if entries[index].mountPoint == mountPoint {
			return entries[index], true
		}
	}
	return mountEntry{}, false
}

// unescapeMountField undoes the octal escapes the kernel writes for space,
// tab, newline and backslash.
func unescapeMountField(field string) string {
	if !strings.Contains(field, `\`) {
		return field
	}
	var unescaped strings.Builder
	for index := 0; index < len(field); index++ {
		if field[index] == '\\' && index+4 <= len(field) {
			if code, err := strconv.ParseUint(field[index+1:index+4], 8, 8); err == nil {
				unescaped.WriteByte(byte(code))
				index += 3
				continue
			}
		}
		unescaped.WriteByte(field[index])
	}
	return unescaped.String()
}
