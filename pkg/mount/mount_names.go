package mount

import (
	"regexp"
	"strings"
)

var unsafeMountNameCharacters = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// mountNameAndOptions splits the daemon's mount options into the name the
// mount table shows for the projection, from volname=, so that a
// projection's mount can be told from another's, and the options passed on
// to the system.
func mountNameAndOptions(options []string) (string, []string) {
	name := "pharo-image-fs"
	others := []string{}
	for _, option := range options {
		if volume, ok := strings.CutPrefix(option, "volname="); ok {
			if volume = mountNameFor(volume); volume != "" {
				name = volume
			}
			continue
		}
		others = append(others, option)
	}
	return name, others
}

// mountNameFor answers name with only characters every mount table shows
// as they are.
func mountNameFor(name string) string {
	return strings.Trim(unsafeMountNameCharacters.ReplaceAllString(name, "-"), "-")
}
