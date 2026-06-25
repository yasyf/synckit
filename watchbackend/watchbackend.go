// Package watchbackend maps filesystem changes to watch ids over a choice of
// backends: fsnotify (per-directory inotify/kqueue) and watchman (newline-JSON
// over watchman's unix socket). A backend takes a map of watch id to the dirs to
// watch for it and, whenever any of an id's dirs changes, calls onEvent(id) once
// per event. Debounce and dedupe live in the caller (synckit's watch.Engine);
// this package only translates a raw filesystem event into the id that owns it.
package watchbackend

import (
	"context"
	"fmt"
)

// EventFunc is invoked with the watch id whose directories changed, once per
// filesystem event as the backend observes it (no debounce).
type EventFunc func(id string)

// Run dispatches to the named backend ("fsnotify" or "watchman") and blocks until
// ctx is canceled, returning ctx.Err() on a clean shutdown. An unknown backend is
// an error.
func Run(ctx context.Context, backend string, dirsByID map[string][]string, onEvent EventFunc) error {
	switch backend {
	case "fsnotify":
		return RunFsnotify(ctx, dirsByID, onEvent)
	case "watchman":
		return RunWatchman(ctx, dirsByID, onEvent)
	default:
		return fmt.Errorf("unknown watch backend %q", backend)
	}
}
