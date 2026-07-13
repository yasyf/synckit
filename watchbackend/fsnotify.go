package watchbackend

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// rewatchInterval is how often Run re-walks each declared root to pick up
// directories that appeared, or were deleted and recreated, since the last sweep.
// It is a package var so tests can shrink it.
var rewatchInterval = 30 * time.Second

// Run recursively watches every directory tree in dirsByID and fans each
// filesystem event out to every id whose declared tree covers it, until ctx is
// canceled — so overlapping declared roots each fire. It holds one kqueue/inotify
// fd per watched directory. Every rewatchInterval it re-walks each declared root,
// so a root missing at startup, deleted and recreated, or grown a nested directory
// is watched again and its id fired on the next sweep; a Create of a subdirectory
// is watched immediately by the fast-path. A queue overflow fires every declared id,
// since the lost events are unknown. Only NewWatcher failure is fatal; an
// unwatchable dir is logged and retried by the sweep. Returns ctx.Err() on
// cancellation.
func Run(ctx context.Context, dirsByID map[string][]string, onEvent EventFunc) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { _ = watcher.Close() }()

	// Clean once so routing keys, watcher.Add args, and filepath.Dir lookups agree
	// for a trailing-slash or "./" declared path.
	declared := make(map[string][]string, len(dirsByID))
	for id, dirs := range dirsByID {
		cleaned := make([]string, len(dirs))
		for i, dir := range dirs {
			cleaned[i] = filepath.Clean(dir)
		}
		declared[id] = cleaned
	}

	// A dir maps to every id whose tree covers it, so an event fans out to all of
	// them; entries are never deleted, so a recreated dir still routes to its ids.
	idsByDir := map[string]map[string]struct{}{}

	// watchedSet snapshots the currently watched dirs. A whole pass shares one
	// snapshot so every owner observes the pre-pass state: a dir deleted and
	// recreated then re-added by the first owner still reads as not-watched for the
	// later owners, so each fires for it (watcher.Add is idempotent).
	watchedSet := func() map[string]struct{} {
		watched := make(map[string]struct{})
		for _, w := range watcher.WatchList() {
			watched[w] = struct{}{}
		}
		return watched
	}

	// addTree watches every not-yet-watched dir under dir, associates id with each,
	// and returns those newly watched (first watch, or recovered after a delete) or
	// newly associated with id (an overlapping root another id already watches). A
	// dir that vanishes mid-walk is skipped and retried on the next sweep. watched is
	// the pass-wide pre-pass snapshot; it is not updated as dirs are added.
	addTree := func(dir, id string, watched map[string]struct{}) []string {
		var fired []string
		walkDirs(dir, func(path string) {
			_, wasWatched := watched[path]
			if !wasWatched {
				if err := watcher.Add(path); err != nil {
					return
				}
			}
			ids := idsByDir[path]
			if ids == nil {
				ids = map[string]struct{}{}
				idsByDir[path] = ids
			}
			_, wasAssociated := ids[id]
			ids[id] = struct{}{}
			if !wasWatched || !wasAssociated {
				fired = append(fired, path)
			}
		})
		return fired
	}

	watched := watchedSet()
	for id, dirs := range declared {
		for _, dir := range dirs {
			if added := addTree(dir, id, watched); len(added) == 0 {
				slog.InfoContext(ctx, "watchbackend: dir not watchable yet", "id", id, "dir", dir)
			} else {
				slog.InfoContext(ctx, "watchbackend: watching dir", "id", id, "dir", dir)
			}
		}
	}

	ticker := time.NewTicker(rewatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			watched := watchedSet()
			for id, dirs := range declared {
				// Dedupe per id: overlapping same-id declarations (e.g. parent and
				// parent/child) both re-add child from the shared stale snapshot, so
				// fire each recovered dir once, not once per covering declaration.
				firedDirs := map[string]struct{}{}
				for _, dir := range dirs {
					for _, added := range addTree(dir, id, watched) {
						if _, seen := firedDirs[added]; seen {
							continue
						}
						firedDirs[added] = struct{}{}
						slog.InfoContext(ctx, "watchbackend: rewatching dir", "id", id, "dir", added)
					}
				}
				for range firedDirs {
					onEvent(id)
				}
			}
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			ids := idsByDir[filepath.Dir(event.Name)]
			if len(ids) == 0 {
				continue
			}
			// Watch a newly created subdir before firing, so an event in it before
			// the next sweep is not missed; the triggering event fans out below.
			// Lstat, not Stat: a newly created symlink-to-dir is not descended, matching
			// the sweep's entry.IsDir() Lstat semantics — persistent watches stay inside
			// the declared tree.
			if event.Op.Has(fsnotify.Create) {
				if info, err := os.Lstat(event.Name); err == nil && info.Mode().IsDir() {
					watched := watchedSet()
					for id := range ids {
						addTree(event.Name, id, watched)
					}
				}
			}
			for id := range ids {
				onEvent(id)
			}
		case watchErr, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			slog.WarnContext(ctx, "watchbackend: fsnotify error", "err", watchErr)
			// Overflow drops unknown events on already-watched dirs that the sweep
			// (fires only for newly added dirs) would miss; treat every id as dirty.
			if errors.Is(watchErr, fsnotify.ErrEventOverflow) {
				for id := range declared {
					onEvent(id)
				}
			}
		}
	}
}

// walkDirs visits dir and every real subdirectory beneath it. os.Stat follows a
// symlinked declared root, so a consumer may declare a symlink (e.g. macOS /tmp),
// while os.ReadDir with entry.IsDir() keeps Lstat semantics on nested entries so a
// symlinked subdirectory is never descended. A path that vanishes or is not a
// directory is skipped.
func walkDirs(dir string, visit func(path string)) {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return
	}
	visit(dir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			walkDirs(filepath.Join(dir, entry.Name()), visit)
		}
	}
}
