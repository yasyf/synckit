package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/watch"
	"github.com/yasyf/synckit/watchbackend"
)

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the resident daemon: own the host mesh, serve the RPC socket, and supervise the watch engines.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return serve(cmd.Context(), hostregistry.NewExecRunner())
		},
	}
}

// serve is the resident process: it migrates any legacy per-tool mesh into the
// shared mesh, binds the rpc socket, registers the status/reconcile/reload
// handlers, and supervises one watch engine per discovered manifest. It blocks
// until ctx is canceled (SIGINT/SIGTERM), rebuilding the supervisor on SIGHUP.
func serve(ctx context.Context, r hostregistry.Runner) error {
	if err := hostregistry.MigrateLegacyMesh(ctx, "reposync", "cookiesync"); err != nil {
		return fmt.Errorf("migrate legacy mesh: %w", err)
	}
	if _, err := ensureManifestsDir(); err != nil {
		return err
	}

	sock, err := hostregistry.Mesh.SockPath()
	if err != nil {
		return err
	}
	ln, err := rpc.Listen(sock)
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()

	sup := newSupervisor(r)
	defer sup.stop()
	if err := sup.reload(ctx); err != nil {
		return err
	}

	d := rpc.NewDispatcher()
	d.Register("status", handleStatus)
	d.Register("reconcile", func(hctx context.Context, _ map[string]any) (any, error) {
		return reconcileAll(hctx, r)
	})
	d.Register("reload", func(hctx context.Context, _ map[string]any) (any, error) {
		if err := sup.reload(hctx); err != nil {
			return nil, err
		}
		return map[string]any{"reloaded": true}, nil
	})

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				if err := sup.reload(ctx); err != nil {
					slog.ErrorContext(ctx, "serve: reload on SIGHUP", "err", err)
				}
			}
		}
	}()

	slog.InfoContext(ctx, "synckitd serving", "socket", sock)
	return rpc.Serve(ctx, ln, d)
}

// supervisor owns the current generation of watch goroutines. reload tears the
// current generation down and starts a fresh one from the manifests on disk, so a
// register/unregister or a SIGHUP rebinds the watchers without restarting the
// process. It is safe for concurrent reload (the rpc reload handler and the SIGHUP
// goroutine both call it).
type supervisor struct {
	runner hostregistry.Runner

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

func newSupervisor(r hostregistry.Runner) *supervisor {
	return &supervisor{runner: r}
}

// reload cancels the running watch generation, waits for it to drain, then starts
// one watch engine per discovered manifest under a fresh child context. The child
// context is derived from parent, so canceling parent (process shutdown) stops the
// current generation too.
func (s *supervisor) reload(parent context.Context) error {
	manifests, err := discoverManifests()
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancel != nil {
		s.cancel()
		s.wg.Wait()
	}

	ctx, cancel := context.WithCancel(parent)
	wg := &sync.WaitGroup{}
	s.cancel = cancel
	s.wg = wg

	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		cancel()
		return fmt.Errorf("load mesh: %w", err)
	}

	for _, m := range manifests {
		s.startEngine(ctx, wg, m, reg)
	}
	slog.InfoContext(ctx, "synckitd watch supervisor reloaded", "manifests", len(manifests))
	return nil
}

// startEngine wires one manifest's watch engine and runs its backend in a
// goroutine accounted for by wg. dirsByID is read once at reload; a change to the
// consumer's watch set is picked up on the next reload.
func (s *supervisor) startEngine(ctx context.Context, wg *sync.WaitGroup, m manifest.Manifest, reg *hostregistry.Registry) {
	items, err := s.listItems(ctx, m)
	if err != nil {
		slog.ErrorContext(ctx, "serve: list watch items", "manifest", m.Name, "err", err)
		return
	}
	dirsByID := make(map[string][]string, len(items))
	for _, it := range items {
		dirsByID[it.ID] = it.WatchDirs
	}

	eng := buildEngine(s.runner, m, reg)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := watchbackend.Run(ctx, m.Watch.Backend, dirsByID, func(id string) {
			eng.OnEvent(ctx, id)
		}); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "serve: watch backend", "manifest", m.Name, "err", err)
		}
	}()
}

// buildEngine wires one manifest's watch engine: the resolver and notifier shell
// the consumer binary through r, the digest is the identity (the id is already the
// stable key), and the host fan-out is self first (local converge) then peers.
func buildEngine(r hostregistry.Runner, m manifest.Manifest, reg *hostregistry.Registry) *watch.Engine[string] {
	hosts := append([]string{reg.Self}, reg.Hosts...)
	return watch.NewEngine[string](
		manifestResolver{runner: r, m: m},
		manifestNotifier{runner: r, m: m, self: reg.Self},
		func(id string) string { return id },
		time.Duration(m.Watch.Debounce),
		hosts,
	)
}

// listItems shells the manifest's binary with its watch list command and decodes
// the JSON array of watch items it prints.
func (s *supervisor) listItems(ctx context.Context, m manifest.Manifest) ([]manifest.WatchItem, error) {
	argv, err := manifest.Render(m.Watch.ListCmd, manifest.ActionVars{})
	if err != nil {
		return nil, fmt.Errorf("render list command for %q: %w", m.Name, err)
	}
	out, err := s.runner.Local(ctx, m.Binary, argv...)
	if err != nil {
		return nil, fmt.Errorf("list watch items for %q: %w", m.Name, err)
	}
	var items []manifest.WatchItem
	if err := json.Unmarshal([]byte(out), &items); err != nil {
		return nil, fmt.Errorf("decode watch items for %q: %w", m.Name, err)
	}
	return items, nil
}

// stop cancels the running generation and waits for it to drain.
func (s *supervisor) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
		s.wg.Wait()
		s.cancel = nil
	}
}

// manifestResolver resolves a watch id's apply-stable fingerprint by re-shelling
// the consumer's watch list command and finding the item by id. Because the
// fingerprint is apply-stable, the engine dedups a consumer's own write without a
// cross-process Seed: after the consumer applies a peer's change, the item's
// fingerprint matches the value the engine already recorded. A missing item
// resolves to "" so the engine treats it as no change.
type manifestResolver struct {
	runner hostregistry.Runner
	m      manifest.Manifest
}

func (r manifestResolver) Resolve(ctx context.Context, id string) (string, error) {
	argv, err := manifest.Render(r.m.Watch.ListCmd, manifest.ActionVars{ID: id})
	if err != nil {
		return "", fmt.Errorf("render list command for %q: %w", r.m.Name, err)
	}
	out, err := r.runner.Local(ctx, r.m.Binary, argv...)
	if err != nil {
		return "", fmt.Errorf("list watch items for %q: %w", r.m.Name, err)
	}
	var items []manifest.WatchItem
	if err := json.Unmarshal([]byte(out), &items); err != nil {
		return "", fmt.Errorf("decode watch items for %q: %w", r.m.Name, err)
	}
	for _, it := range items {
		if it.ID == id {
			return it.Fingerprint, nil
		}
	}
	return "", nil
}

// manifestNotifier renders and runs the manifest's sync action for one peer: the
// self host runs it locally with no origin, a remote peer runs it over ssh with
// Origin set to this host so the peer skips notifying back (anti-echo provenance).
// One unreachable peer never blocks the others — the engine fans out concurrently
// and isolates each error.
type manifestNotifier struct {
	runner hostregistry.Runner
	m      manifest.Manifest
	self   string
}

func (n manifestNotifier) Notify(ctx context.Context, peer, id string) error {
	if peer == n.self {
		argv, err := manifest.Render(n.m.Actions.Sync, manifest.ActionVars{ID: id})
		if err != nil {
			return fmt.Errorf("render sync for %q: %w", n.m.Name, err)
		}
		if _, err := n.runner.Local(ctx, n.m.Binary, argv...); err != nil {
			return fmt.Errorf("local sync for %q: %w", n.m.Name, err)
		}
		return nil
	}
	argv, err := manifest.Render(n.m.Actions.Sync, manifest.ActionVars{Peer: peer, Origin: n.self, ID: id})
	if err != nil {
		return fmt.Errorf("render sync for %q: %w", n.m.Name, err)
	}
	if _, err := n.runner.SSH(ctx, peer, remoteCommand(n.m.Binary, argv)); err != nil {
		return fmt.Errorf("ssh sync for %q on %s: %w", n.m.Name, peer, err)
	}
	return nil
}
