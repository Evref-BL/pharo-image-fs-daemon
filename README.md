# pharo-image-fs-daemon

`pharo-image-fs-daemon` mounts a live Pharo image projection as a local
filesystem.

This repository contains only the Go daemon. The Pharo backend lives in
[`pharo-image-fs`](https://github.com/Evref-BL/pharo-image-fs), which owns Pharo
source projection, parsing, compilation, transactional writes, critiques, and
Tonel synchronization.

## Requirements

- macOS, with nothing to install: the daemon serves NFSv3 on a loopback port
  and mounts it with the system's `mount_nfs`, as the user
- or Linux with FUSE: the `fuse3` package's setuid `fusermount3` and
  `/dev/fuse`, which most desktop and server installs have. Ubuntu 25.04 and
  later let `fusermount3` mount only under the home directory, `/tmp`, `/mnt`,
  `/media` and `/run/user`
- a running `pharo-image-fs` projection endpoint
- Go, for development builds

Users should normally run a prebuilt release binary.

## Build

```sh
go build -o pharo-image-fs ./cmd/pharo-image-fs
```

## Releases

Tagged releases build prebuilt daemon binaries and attach them to the GitHub
Release:

- `pharo-image-fs-daemon-darwin-arm64.zip`
- `pharo-image-fs-daemon-darwin-amd64.zip`
- `pharo-image-fs-daemon-linux-amd64.zip`
- `pharo-image-fs-daemon-linux-arm64.zip`, both static

Create a release by pushing a matching version tag:

```sh
git tag v0.2.1
git push origin v0.2.1
```

The Pharo package uses the same version number to locate the matching daemon
release asset.

## Run

```sh
./pharo-image-fs --endpoint http://127.0.0.1:9023/projection /tmp/pharo-image-fs
```

The mountpoint is created when missing. If it already exists, it must be a
directory.

## Development

```sh
go test ./...
```

The tests that mount a projection run only where `PIFS_MOUNT_TEST=1`, which
CI sets: they drive the kernel's NFS or FUSE client, and a Mac running them
once panicked, so they belong on machines that can be thrown away.

The Linux adapter is built only for Linux. Its tests drive go-fuse as the
kernel does, without mounting, and run elsewhere with a tag:

```sh
go test -tags fusetest ./pkg/mount
```

A Linux mount is unmounted with `fusermount3 -u <path>`.

The daemon does not parse or validate Pharo code. It owns mount lifecycle,
filesystem callbacks, transport, timeouts, and generic filesystem errors.
