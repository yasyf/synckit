// Package watchbackend maps filesystem changes to watch ids over a recursive
// per-directory fsnotify (inotify/kqueue) watch. Run takes a map of watch id to
// the Target to watch for it and, whenever any of an id's dirs changes, calls
// onEvent(id) once per event. Debounce and dedupe live in the caller (synckit's
// watch.Engine); this package only translates a raw filesystem event into the id
// that owns it.
package watchbackend

// EventFunc is invoked with the watch id whose directories changed, once per
// filesystem event as the backend observes it (no debounce).
type EventFunc func(id string)

// Target is what Run watches for one id. Dirs are the declared paths. Names, when
// non-empty, narrows the target: each directory in Dirs is watched without its
// subdirectories, and an event on an entry directly inside it fires the id only
// when the entry's base name is in Names. An event on a declared path itself
// fires regardless of Names.
type Target struct {
	Dirs  []string
	Names []string
}
