#!/bin/sh
# Prints the licence and notice files of every module compiled into the
# daemon for the GOOS and GOARCH in the environment, but its own. A release
# ships them beside the binary, as those licences ask; a module with no
# licence file fails, so CI finds it before a release does.
set -eu
cd "$(dirname "$0")/.."
go list -deps -f '{{with .Module}}{{if not .Main}}{{.Path}}	{{.Dir}}{{end}}{{end}}' ./cmd/pharo-image-fs | sort -u |
  while IFS='	' read -r path dir; do
    ls "$dir" | grep -qiE '^(licen[cs]e|copying)([._-].*)?$' || { echo "$path has no licence file in $dir" >&2; exit 1; }
    for file in $(ls "$dir" | grep -iE '^(licen[cs]e|copying|notice)([._-].*)?$'); do
      title="$path: $file"
      printf '%s\n%s\n\n' "$title" "$(echo "$title" | sed 's/./=/g')"
      cat "$dir/$file"
      printf '\n\n'
    done
  done
