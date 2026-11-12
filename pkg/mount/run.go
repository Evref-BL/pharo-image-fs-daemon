package mount

import "github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"

// Run starts the pharo-image-fs mount daemon.
func Run(args []string) error {
	closeInheritedDescriptors()
	config, err := ParseConfig(args)
	if err != nil {
		return err
	}

	client, err := protocol.NewHTTPClient(config.Endpoint)
	if err != nil {
		return err
	}

	return Mount(config.MountPoint, client, config)
}
