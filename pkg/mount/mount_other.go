//go:build !linux && !darwin

package mount

import (
	"fmt"
	"runtime"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
)

// Mount answers that this system has no way to mount the projection.
func Mount(string, protocol.Client, Config) error {
	return fmt.Errorf("pharo-image-fs mounts on macOS and Linux, not on %s", runtime.GOOS)
}
