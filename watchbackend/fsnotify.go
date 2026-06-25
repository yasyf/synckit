package watchbackend

import (
	"context"
	"log/slog"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
)

// RunFsnotify watches every directory in dirsByID and calls onEvent with the
// owning id on each filesystem event, until ctx is canceled. It builds an internal
// reverse map from watched directory to id, so an event under a dir is routed back
// to the id that registered it. A directory that cannot be watched is logged and
// skipped rather than failing the whole loop. Returns ctx.Err() on cancellation.
func RunFsnotify(ctx context.Context, dirsByID map[string][]string, onEvent EventFunc) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { _ = watcher.Close() }()

	idByDir := map[string]string{}
	for id, dirs := range dirsByID {
		for _, dir := range dirs {
			if err := watcher.Add(dir); err != nil {
				slog.WarnContext(ctx, "watchbackend: cannot watch dir", "id", id, "dir", dir, "err", err)
				continue
			}
			idByDir[dir] = id
			slog.InfoContext(ctx, "watchbackend: watching dir", "id", id, "dir", dir)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if id, ok := idByDir[filepath.Dir(event.Name)]; ok {
				onEvent(id)
			}
		case watchErr, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			slog.WarnContext(ctx, "watchbackend: fsnotify error", "err", watchErr)
		}
	}
}
