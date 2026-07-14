// Package debug provides on-demand runtime diagnostics for the resident daemon: a
// SIGUSR1 handler that dumps every goroutine's stack to a file for post-mortem of a
// wedge without restarting the process.
package debug

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"syscall"
	"time"
)

// DumpOnSIGUSR1 arms a listener that writes a full goroutine dump to a timestamped
// file under dir each time the process receives SIGUSR1, so a wedged daemon can be
// inspected without a restart. It registers only SIGUSR1 — the default SIGINT/SIGTERM
// shutdown handling is untouched — and returns once armed; the listener stops when ctx
// is done.
func DumpOnSIGUSR1(ctx context.Context, dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create dump dir %s: %w", dir, err)
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	go func() {
		defer signal.Stop(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				path, err := writeDump(dir)
				if err != nil {
					slog.ErrorContext(ctx, "debug: goroutine dump failed", "err", err)
					continue
				}
				slog.InfoContext(ctx, "debug: wrote goroutine dump", "path", path)
			}
		}
	}()
	return nil
}

// writeDump writes a full goroutine dump to a temp file under dir, then renames it into
// place atomically so a reader never observes a partial dump and a failed write leaves no
// plausible-looking stump. It returns the final timestamped path.
func writeDump(dir string) (string, error) {
	tmp, err := os.CreateTemp(dir, "dump-*.tmp") //nolint:gosec // G304: dir is the daemon's own config dir, not user-supplied.
	if err != nil {
		return "", fmt.Errorf("create dump temp: %w", err)
	}
	tmpName := tmp.Name()
	if err := pprof.Lookup("goroutine").WriteTo(tmp, 2); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("write goroutine profile: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("close dump temp: %w", err)
	}
	path := filepath.Join(dir, "goroutine-"+time.Now().Format("20060102-150405.000000000")+".txt")
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return "", fmt.Errorf("rename dump into place: %w", err)
	}
	return path, nil
}
