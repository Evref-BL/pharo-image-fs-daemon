package mount

import (
	"context"
	"path"
	"sort"
	"strings"
	"syscall"
	"unicode"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
)

// projection holds the rules every mount shares: what the image projects,
// the editor files kept beside it, and the errors of refused writes.
type projection struct {
	client  protocol.Client
	overlay *Overlay
	errors  *ErrorStore
	logger  projectionLogger
}

type projectionLogger interface {
	Printf(format string, args ...any)
}

func newProjection(client protocol.Client) *projection {
	return &projection{
		client:  client,
		overlay: NewOverlay(),
		errors:  NewErrorStore(),
	}
}

func (p *projection) entryForPath(projectionPath string) (protocol.Entry, syscall.Errno) {
	if projectionPath == "/" {
		return protocol.Entry{Name: "/", Kind: protocol.Directory}, 0
	}
	if p.overlay.IsHidden(projectionPath) {
		return protocol.Entry{}, syscall.ENOENT
	}
	if overlayEntry, ok := p.overlay.Stat(projectionPath); ok {
		return writableEntryForPath(projectionPath, overlayEntry), 0
	}
	if errorEntry, ok := p.errors.Stat(projectionPath); ok {
		return errorEntry, 0
	}

	entry, err := p.client.Stat(context.Background(), projectionPath)
	if err != nil {
		return protocol.Entry{}, errnoFor(err)
	}
	return writableEntryForPath(projectionPath, entry), 0
}

// entriesIn answers the entries listed in the directory at projectionPath,
// without "." and "..".
func (p *projection) entriesIn(projectionPath string) ([]protocol.Entry, syscall.Errno) {
	entry, errno := p.entryForPath(projectionPath)
	if errno != 0 {
		return nil, errno
	}
	if entry.Kind != protocol.Directory {
		return nil, syscall.ENOTDIR
	}

	var entries []protocol.Entry
	if projectionPath == errorsRoot {
		entries = p.errors.EntriesIn(projectionPath)
	} else {
		projectedEntries, err := p.client.List(context.Background(), projectionPath)
		if err != nil {
			return nil, errnoFor(err)
		}
		projectedEntries = p.visibleProjectedEntries(projectionPath, projectedEntries)
		entries = mergeEntries(projectedEntries, p.overlay.EntriesIn(projectionPath), p.errors.EntriesIn(projectionPath))
	}

	visible := make([]protocol.Entry, 0, len(entries))
	for _, childEntry := range entries {
		if isIgnoredMetadataName(childEntry.Name) {
			continue
		}
		visible = append(visible, writableEntryForPath(joinProjectionPath(projectionPath, childEntry.Name), childEntry))
	}
	return visible, 0
}

func (p *projection) readlink(projectionPath string) (string, syscall.Errno) {
	entry, errno := p.entryForPath(projectionPath)
	if errno != 0 {
		return "", errno
	}
	if entry.Kind != protocol.Symlink {
		return "", syscall.EINVAL
	}

	return entry.Target, 0
}

func (p *projection) contentsForPath(projectionPath string) ([]byte, error) {
	if overlayContents, ok := p.overlay.Read(projectionPath); ok {
		return overlayContents, nil
	}
	if errorContents, ok := p.errors.Read(projectionPath); ok {
		return errorContents, nil
	}

	return p.client.Read(context.Background(), projectionPath)
}

// createFile makes a new file at projectionPath and answers how its contents
// are stored once written: in the image for a projected Tonel file, beside it
// for an editor's file, and nowhere for macOS metadata.
func (p *projection) createFile(projectionPath string) (func(context.Context, string, []byte) error, syscall.Errno) {
	if !isWritableProjectionPath(projectionPath) && !isIgnoredMetadataPath(projectionPath) {
		return nil, syscall.EROFS
	}

	parentEntry, errno := p.entryForPath(path.Dir(projectionPath))
	if errno != 0 {
		return nil, errno
	}
	if parentEntry.Kind != protocol.Directory {
		return nil, syscall.ENOTDIR
	}

	if isIgnoredMetadataPath(projectionPath) {
		return func(context.Context, string, []byte) error { return nil }, 0
	}

	if _, ok := p.overlay.Stat(projectionPath); ok {
		return nil, syscall.EEXIST
	}
	if !p.overlay.IsHidden(projectionPath) {
		if _, err := p.client.Stat(context.Background(), projectionPath); err == nil {
			return nil, syscall.EEXIST
		} else if !protocol.NotFound(err) {
			return nil, errnoFor(err)
		}
	}

	p.overlay.Create(projectionPath, nil)
	if isProjectedTonelFilePath(projectionPath) {
		return p.writeProjection, 0
	}
	return p.overlay.Write, 0
}

func (p *projection) unlink(projectionPath string) syscall.Errno {
	if isIgnoredMetadataPath(projectionPath) {
		p.overlay.Delete(projectionPath)
		return 0
	}

	if p.overlay.Delete(projectionPath) {
		return 0
	}
	if !isWritableProjectionPath(projectionPath) {
		return syscall.EROFS
	}
	if err := p.client.Delete(context.Background(), projectionPath); err != nil {
		if protocol.NotFound(err) {
			return 0
		}
		return p.refused("delete", projectionPath, err)
	}
	return 0
}

func (p *projection) mkdir(projectionPath string) syscall.Errno {
	if !isProjectedTonelPackagePath(projectionPath) {
		if isWritableProjectionPath(projectionPath) {
			return syscall.EINVAL
		}
		return syscall.EROFS
	}
	if err := p.client.Mkdir(context.Background(), projectionPath); err != nil {
		return p.refused("mkdir", projectionPath, err)
	}
	return 0
}

func (p *projection) rmdir(projectionPath string) syscall.Errno {
	if !isProjectedTonelPackagePath(projectionPath) {
		if isWritableProjectionPath(projectionPath) {
			return syscall.EINVAL
		}
		return syscall.EROFS
	}
	if err := p.client.Rmdir(context.Background(), projectionPath); err != nil {
		return p.refused("rmdir", projectionPath, err)
	}
	return 0
}

func (p *projection) rename(oldPath string, newPath string) syscall.Errno {
	if isIgnoredMetadataPath(oldPath) || isIgnoredMetadataPath(newPath) {
		p.overlay.Delete(oldPath)
		p.overlay.Delete(newPath)
		return 0
	}

	contents, ok := p.overlay.Read(oldPath)
	if ok && isProjectedTonelFilePath(newPath) {
		if err := p.writeProjection(context.Background(), newPath, contents); err != nil {
			return errnoFor(err)
		}
		p.overlay.Delete(oldPath)
		p.overlay.Delete(newPath)
		return 0
	}

	if ok {
		if !isWritableProjectionPath(newPath) {
			return syscall.EROFS
		}

		p.overlay.Move(oldPath, newPath)
		return 0
	}

	if isProjectedTonelFilePath(oldPath) && isWritableProjectionPath(newPath) && !isProjectedTonelFilePath(newPath) {
		contents, err := p.contentsForPath(oldPath)
		if err != nil {
			return errnoFor(err)
		}
		p.overlay.Create(newPath, contents)
		p.overlay.Hide(oldPath)
		return 0
	}

	if !isWritableProjectionPath(oldPath) || !isWritableProjectionPath(newPath) {
		return syscall.EROFS
	}
	if err := p.client.Rename(context.Background(), oldPath, newPath); err != nil {
		return p.refused("rename", oldPath+" to "+newPath, err)
	}
	return 0
}

// writeProjection hands contents to the image, which compiles them. The
// image's own rendering is the file from then on, so an editor copy of the
// same path no longer shadows it.
func (p *projection) writeProjection(ctx context.Context, projectionPath string, contents []byte) error {
	result, err := p.client.Write(ctx, projectionPath, contents)
	if err != nil {
		p.refused("write", projectionPath, err)
		return err
	}

	p.overlay.Delete(projectionPath)
	p.logDiagnostics(projectionPath, result.Diagnostics)
	return nil
}

func (p *projection) visibleProjectedEntries(parentPath string, entries []protocol.Entry) []protocol.Entry {
	visible := entries[:0]
	for _, entry := range entries {
		if p.overlay.IsHidden(joinProjectionPath(parentPath, entry.Name)) {
			continue
		}
		visible = append(visible, entry)
	}
	return visible
}

func (p *projection) logDiagnostics(projectionPath string, diagnostics []protocol.Diagnostic) {
	if len(diagnostics) == 0 {
		return
	}

	p.logf("write %s returned %d diagnostic(s)", projectionPath, len(diagnostics))
	for _, diagnostic := range diagnostics {
		p.logf("diagnostic %s: %s", diagnostic.Rule, diagnosticLineFor(diagnostic))
	}
}

// refused logs and records in /errors that the image refused operation on
// what, and answers its errno.
func (p *projection) refused(operation string, what string, err error) syscall.Errno {
	p.logf("%s %s failed: %v", operation, what, err)
	p.errors.RecordError(operation, what, err)
	return errnoFor(err)
}

func (p *projection) logf(format string, args ...any) {
	if p.logger == nil {
		return
	}

	p.logger.Printf(format, args...)
}

func resizedContents(contents []byte, size int) []byte {
	if len(contents) == size {
		return contents
	}
	if len(contents) > size {
		return contents[:size]
	}

	resized := make([]byte, size)
	copy(resized, contents)
	return resized
}

func writableEntryForPath(projectionPath string, entry protocol.Entry) protocol.Entry {
	if entry.Kind == protocol.Directory && isWritableProjectionPath(projectionPath) {
		entry.Writable = true
	}

	return entry
}

func stableInodeFor(projectionPath string) uint64 {
	var hash uint64 = 14695981039346656037
	for _, b := range []byte(projectionPath) {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return hash
}

// permissionsFor answers the permission bits of entry: writable entries are
// the owner's to change, the rest read-only.
func permissionsFor(entry protocol.Entry) uint32 {
	switch entry.Kind {
	case protocol.Directory:
		if entry.Writable {
			return 0o755
		}
		return 0o555
	case protocol.Symlink:
		return 0o777
	default:
		if entry.Writable {
			return 0o644
		}
		return 0o444
	}
}

func joinProjectionPath(parentPath string, name string) string {
	if parentPath == "/" {
		return "/" + name
	}

	return strings.TrimRight(parentPath, "/") + "/" + name
}

func mergeEntries(projected []protocol.Entry, overlays ...[]protocol.Entry) []protocol.Entry {
	byName := map[string]protocol.Entry{}
	for _, entry := range projected {
		byName[entry.Name] = entry
	}

	merged := make([]protocol.Entry, 0, len(projected))
	merged = append(merged, projected...)
	for _, overlay := range overlays {
		overlayOnly := make([]protocol.Entry, 0, len(overlay))
		for _, entry := range overlay {
			if _, exists := byName[entry.Name]; exists {
				continue
			}
			byName[entry.Name] = entry
			overlayOnly = append(overlayOnly, entry)
		}
		sort.Slice(overlayOnly, func(i int, j int) bool {
			return overlayOnly[i].Name < overlayOnly[j].Name
		})
		merged = append(merged, overlayOnly...)
	}

	return merged
}

func isWritableProjectionPath(projectionPath string) bool {
	return projectionPath == "/tonel" || strings.HasPrefix(projectionPath, "/tonel/")
}

// isProjectedTonelFilePath answers whether projectionPath names a class's
// Tonel file: a class name, then ".class.st" or ".extension.st". Other names,
// as the temp files of editors and tools such as sed's ".!123!Foo.class.st",
// are files of the mount's own.
func isProjectedTonelFilePath(projectionPath string) bool {
	if isIgnoredMetadataPath(projectionPath) || !strings.HasPrefix(projectionPath, "/tonel/") {
		return false
	}

	base := path.Base(projectionPath)
	for _, suffix := range []string{".class.st", ".extension.st"} {
		if className, ok := strings.CutSuffix(base, suffix); ok {
			return isClassName(className)
		}
	}
	return false
}

// isClassName answers whether name may name a class: a letter, then letters,
// digits and underscores. Whether the image takes it is the image's call.
func isClassName(name string) bool {
	for index, char := range name {
		if !unicode.IsLetter(char) && (index == 0 || !unicode.IsDigit(char) && char != '_') {
			return false
		}
	}
	return name != ""
}

func isProjectedTonelPackagePath(projectionPath string) bool {
	return strings.HasPrefix(projectionPath, "/tonel/") &&
		path.Dir(projectionPath) == "/tonel" &&
		!isIgnoredMetadataPath(projectionPath)
}

func isIgnoredMetadataPath(projectionPath string) bool {
	return isIgnoredMetadataName(path.Base(projectionPath))
}

func isIgnoredMetadataName(name string) bool {
	return strings.HasPrefix(name, "._") || name == ".DS_Store"
}
