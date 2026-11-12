package mount

import "testing"

func TestIdentitiesFollowARename(t *testing.T) {
	ids := newIdentities()
	temp := ids.idFor("/tonel/Kernel/.Object.class.st.swp")
	class := ids.idFor(objectPath)

	ids.renamed("/tonel/Kernel/.Object.class.st.swp", objectPath)

	if got := ids.idFor(objectPath); got != temp {
		t.Fatalf("the renamed file should keep its id %d, got %d", temp, got)
	}
	if _, ok := ids.pathFor(class); ok {
		t.Fatalf("the replaced file's id should name nothing")
	}
	if fresh := ids.idFor("/tonel/Kernel/.Object.class.st.swp"); fresh == temp || fresh == class {
		t.Fatalf("the old name should get a new id, got %d", fresh)
	}
}

func TestIdentitiesMoveEverythingUnderARenamedDirectory(t *testing.T) {
	ids := newIdentities()
	file := ids.idFor("/tonel/Old/A.class.st")
	sibling := ids.idFor("/tonel/Older/B.class.st")

	ids.renamed("/tonel/Old", "/tonel/New")

	if where, ok := ids.pathFor(file); !ok || where != "/tonel/New/A.class.st" {
		t.Fatalf("a file in the renamed directory should move with it, is at %q", where)
	}
	if where, _ := ids.pathFor(sibling); where != "/tonel/Older/B.class.st" {
		t.Fatalf("a directory sharing the name's prefix should stay, is at %q", where)
	}
}

func TestIdentitiesOfRemovedFilesAreNotReused(t *testing.T) {
	ids := newIdentities()
	directory := ids.idFor("/tonel/Kernel")
	file := ids.idFor(objectPath)

	ids.removed("/tonel/Kernel")

	for _, id := range []uint64{directory, file} {
		if _, ok := ids.pathFor(id); ok {
			t.Fatalf("id %d should name nothing once removed", id)
		}
	}
	if again := ids.idFor(objectPath); again == file || again == directory {
		t.Fatalf("a path made again should get a new id, got %d", again)
	}
}
