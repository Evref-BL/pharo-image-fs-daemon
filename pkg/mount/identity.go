package mount

import (
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// identities names each file and directory of a mount by a number that
// follows it through renames, as an inode does, and that is never given to
// another while the daemon runs. The NFS client takes a file whose id changes
// under a name it holds for a different file: so a temp file renamed over a
// class keeps the temp's id, and the class's old id goes with the file it
// named.
//
// What the image changes on its own keeps a path's id: the daemon cannot tell
// a class removed and added again from one edited.
type identities struct {
	mu    sync.Mutex
	ids   map[string]uint64
	paths map[uint64]string
	next  uint64
}

func newIdentities() *identities {
	return &identities{ids: map[string]uint64{}, paths: map[uint64]string{}, next: 1}
}

// idFor answers the id of what is at projectionPath, giving it one if it has
// none yet.
func (i *identities) idFor(projectionPath string) uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()

	if id, ok := i.ids[projectionPath]; ok {
		return id
	}
	id := i.next
	i.next++
	i.ids[projectionPath] = id
	i.paths[id] = projectionPath
	return id
}

// idAt answers the id of what is at projectionPath, if it has one.
func (i *identities) idAt(projectionPath string) (uint64, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()

	id, ok := i.ids[projectionPath]
	return id, ok
}

// pathFor answers where what id names is now, if it still is.
func (i *identities) pathFor(id uint64) (string, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()

	projectionPath, ok := i.paths[id]
	return projectionPath, ok
}

// renamed records that what was at oldPath, with everything under it, is now
// at newPath, and that what was at newPath is gone.
func (i *identities) renamed(oldPath string, newPath string) {
	if oldPath == newPath {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	i.removeLocked(newPath)
	moved := map[string]uint64{}
	for projectionPath, id := range i.ids {
		if rest, ok := pathUnder(projectionPath, oldPath); ok {
			moved[newPath+rest] = id
			delete(i.ids, projectionPath)
		}
	}
	for projectionPath, id := range moved {
		i.ids[projectionPath] = id
		i.paths[id] = projectionPath
	}
}

// removed records that what was at projectionPath, with everything under it,
// is gone: its ids name nothing from now on, and the path gets a new one.
func (i *identities) removed(projectionPath string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.removeLocked(projectionPath)
}

func (i *identities) removeLocked(removedPath string) {
	for projectionPath, id := range i.ids {
		if _, ok := pathUnder(projectionPath, removedPath); ok {
			delete(i.ids, projectionPath)
			delete(i.paths, id)
		}
	}
}

// pathUnder answers what follows root in projectionPath, when projectionPath
// is root or under it: "" for root itself, "/name…" below it.
func pathUnder(projectionPath string, root string) (string, bool) {
	if projectionPath == root {
		return "", true
	}
	if root == "/" {
		return projectionPath, true
	}
	if strings.HasPrefix(projectionPath, root+"/") {
		return projectionPath[len(root):], true
	}
	return "", false
}

// idMark separates a name from the id that a handle's last component carries.
// A name cannot hold it: NFS names are C strings.
const idMark = "\x00"

// componentsFor answers the components go-nfs is given for the object id
// names at projectionPath: its path, with the id on the last component, so
// that every name go-nfs makes from them stands for that object wherever it
// has moved since. The root's only component carries its id and no name, so
// that ".." of the root is the root.
func componentsFor(id uint64, projectionPath string) []string {
	if projectionPath == "/" {
		return []string{idMark + strconv.FormatUint(id, 10)}
	}
	components := strings.Split(strings.TrimPrefix(projectionPath, "/"), "/")
	last := len(components) - 1
	components[last] += idMark + strconv.FormatUint(id, 10)
	return components
}

// resolve answers the projection path that a name go-nfs made from a
// handle's components stands for now: what follows the component carrying an
// id is under where that object is now. A name with no id is a path as it is.
// A name naming an object that is gone is stale.
func (i *identities) resolve(name string) (string, syscall.Errno) {
	if !strings.Contains(name, idMark) {
		return projectionPathFor(name), 0
	}
	components := strings.Split(name, "/")
	carrying := -1
	for index, component := range components {
		if strings.Contains(component, idMark) {
			if carrying >= 0 {
				return "", syscall.EINVAL
			}
			carrying = index
		}
	}
	_, idText, _ := strings.Cut(components[carrying], idMark)
	id, err := strconv.ParseUint(idText, 10, 64)
	if err != nil {
		return "", syscall.EINVAL
	}
	base, ok := i.pathFor(id)
	if !ok {
		return "", syscall.ESTALE
	}
	return projectionPathFor(path.Join(append([]string{base}, components[carrying+1:]...)...)), 0
}
