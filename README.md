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

The test that mounts a projection runs only where `PIFS_MOUNT_TEST=1`, which
CI sets: it drives the kernel's NFS client, which once panicked a Mac running
it, so it belongs on machines that can be thrown away.

The daemon does not parse or validate Pharo code. It owns mount lifecycle,
filesystem callbacks, transport, timeouts, and generic filesystem errors.
