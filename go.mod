module github.com/Evref-BL/pharo-image-fs-daemon

go 1.26

require (
	github.com/go-git/go-billy/v5 v5.6.0
	github.com/willscott/go-nfs v0.0.4
	github.com/winfsp/cgofuse v1.6.0
	golang.org/x/sys v0.24.0
)

require (
	github.com/rasky/go-xdr v0.0.0-20170124162913-1a41d1a06c93 // indirect
	github.com/willscott/go-nfs-client v0.0.0-20240104095149-b44639837b00 // indirect
)

replace github.com/willscott/go-nfs => ./third_party/go-nfs
