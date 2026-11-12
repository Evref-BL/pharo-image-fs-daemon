//go:build !linux && !darwin

package mount

// closeInheritedDescriptors does nothing where no shell starts the daemon.
func closeInheritedDescriptors() {}
