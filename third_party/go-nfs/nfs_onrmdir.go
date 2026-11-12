package nfs

// Changed for pharo-image-fs: from go-nfs's main branch (3946bf8), RMDIR is
// no longer REMOVE.

import (
	"context"
)

func onRmDir(ctx context.Context, w *response, userHandle Handler) error {
	return onRemoveObj(ctx, w, userHandle, true)
}
