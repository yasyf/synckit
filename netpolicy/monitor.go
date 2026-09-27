package netpolicy

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

var manualSweepInterval = time.Second

// Monitor observes this host's network cost State.
type Monitor interface {
	// Current returns the latest State, with the manual override merged in, and
	// a channel closed at the next OS path update or manual setting edit. A
	// manual edit closes the channel with no Current call: the Monitor watches
	// the setting file's directory and re-checks the file every second, so the
	// channel closes within a second of the edit at worst.
	Current() (State, <-chan struct{})
	// Close stops observation and releases every OS resource the Monitor holds.
	Close() error
}

type observed struct {
	manual  *manualSource
	watcher *fsnotify.Watcher
	watched chan struct{}
	mu      sync.Mutex
	path    State
	epoch   uint64
	changed chan struct{}
}

func newObserved(manualPath string) (*observed, error) {
	manualPath = filepath.Clean(manualPath)
	dir := filepath.Dir(manualPath)
	if err := os.MkdirAll(dir, manualDirPerm); err != nil {
		return nil, fmt.Errorf("create manual network setting dir: %w", err)
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("watch manual network setting: %w", err)
	}
	if err := watcher.Add(dir); err != nil {
		_ = watcher.Close()
		return nil, fmt.Errorf("watch manual network setting dir %s: %w", dir, err)
	}
	o := &observed{
		manual:  newManualSource(manualPath),
		watcher: watcher,
		watched: make(chan struct{}),
		path:    State{Status: StatusUnknown},
		changed: make(chan struct{}),
	}
	o.manual.refresh()
	go o.watch(time.NewTicker(manualSweepInterval))
	return o, nil
}

func (o *observed) Current() (State, <-chan struct{}) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.refreshManualLocked()
	return o.stateLocked(), o.changed
}

func (o *observed) publish(path State) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.path = path
	if !o.stateLocked().Unrestricted() {
		o.epoch++
	}
	o.notifyLocked()
}

func (o *observed) watch(sweep *time.Ticker) {
	defer close(o.watched)
	defer sweep.Stop()
	for {
		select {
		// fsnotify's kqueue backend rescans a directory to name its changes and
		// can emit nothing for a rename-over that races the rescan, so the sweep
		// bounds how long a missed SaveManual stays unpublished.
		case <-sweep.C:
			o.refreshManual()
		case event, ok := <-o.watcher.Events:
			if !ok {
				return
			}
			if event.Name == o.manual.path || event.Name == o.manual.markPath {
				o.refreshManual()
			}
		case err, ok := <-o.watcher.Errors:
			if !ok {
				return
			}
			slog.Warn("netpolicy: manual network setting watch failed; rereading the setting", "path", o.manual.path, "err", err)
			o.refreshManual()
		}
	}
}

func (o *observed) refreshManual() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.refreshManualLocked()
}

func (o *observed) refreshManualLocked() {
	wasMetered := o.manual.value
	if !o.manual.refresh() {
		return
	}
	if o.manual.value || !wasMetered {
		o.epoch++
	}
	o.notifyLocked()
}

func (o *observed) stateLocked() State {
	s := o.path
	s.ManualMetered = o.manual.value
	s.RestrictedEpoch = o.epoch
	return s
}

func (o *observed) notifyLocked() {
	close(o.changed)
	o.changed = make(chan struct{})
}

func (o *observed) close() error {
	err := o.watcher.Close()
	<-o.watched
	return err
}
