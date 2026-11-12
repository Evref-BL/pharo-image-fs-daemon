package mount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
)

const (
	imageProbeInterval = 3 * time.Second
	// imageRefusalsBeforeGone is how many probes in a row the image's
	// endpoint must refuse before the daemon unmounts: a refused connection
	// means no one listens on the port, which a busy or stopped-in-a-debugger
	// image never causes.
	imageRefusalsBeforeGone = 3
)

// watchImage calls gone, once, when the image's projection endpoint has
// refused connections imageRefusalsBeforeGone times in a row; it answers a
// function that stops watching.
func watchImage(client protocol.Client, logger projectionLogger, gone func()) func() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(imageProbeInterval)
		defer ticker.Stop()
		refusals := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			probe, cancelProbe := context.WithTimeout(ctx, imageProbeInterval)
			_, err := client.Stat(probe, "/tonel")
			cancelProbe()
			if !errors.Is(err, syscall.ECONNREFUSED) {
				refusals = 0
				continue
			}
			refusals++
			if refusals < imageRefusalsBeforeGone {
				continue
			}
			if logger != nil {
				logger.Printf("the image's projection endpoint is gone; unmounting")
			}
			gone()
			return
		}
	}()
	return cancel
}

func ensureMountPoint(mountPoint string) error {
	info, err := os.Stat(mountPoint)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(mountPoint, 0o755)
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("mountpoint is not a directory: %s", mountPoint)
	}

	return nil
}

// stopSignals answers the signals that stop the daemon: SIGHUP only when it
// is not ignored, as nohup has it ignored to keep the daemon past its
// terminal.
func stopSignals() []os.Signal {
	return stopSignalsUnless(signal.Ignored)
}

func stopSignalsUnless(ignored func(os.Signal) bool) []os.Signal {
	stop := []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	if !ignored(syscall.SIGHUP) {
		stop = append(stop, syscall.SIGHUP)
	}
	return stop
}
