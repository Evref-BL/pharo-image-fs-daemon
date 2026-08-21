# pharo-image-fs-daemon

`pharo-image-fs-daemon` mounts a live Pharo image projection as a local
filesystem.

This repository contains only the Go daemon. The Pharo backend lives in
[`pharo-image-fs`](https://github.com/Evref-BL/pharo-image-fs), which owns Pharo
source projection, parsing, compilation, transactional writes, critiques, and
Tonel synchronization.

## Requirements

- Go, for development builds
- macOS with [fuse-t](https://www.fuse-t.org/) installed
- a running `pharo-image-fs` projection endpoint

Users should normally run a prebuilt release binary. Go is not required to run a
prebuilt daemon binary, but the platform FUSE dependency is still required.

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
git tag v0.1.0
git push origin v0.1.0
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

The daemon does not parse or validate Pharo code. It owns mount lifecycle,
filesystem callbacks, transport, timeouts, and generic filesystem errors.
