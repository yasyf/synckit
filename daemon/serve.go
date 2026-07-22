package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/daemonrole"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/debug"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
	"github.com/yasyf/synckit/watch"
	"github.com/yasyf/synckit/watchbackend"
)

// listBackoff is the wait between retries when a consumer's typed service is not
// yet reachable at engine startup (the resident helper may not have bound its
// socket at login). It is a package var so tests can shrink it.
var listBackoff = 5 * time.Second

// listRetryBudget caps the total time engine startup retries a transient
// connection failure before giving up on this generation; the periodic reconcile
// and the next reload re-bind. It is a package var so tests can shrink it.
var listRetryBudget = 60 * time.Second

// watchBackoffBase, watchBackoffMax, and watchHealthyRun bound the exponential
// backoff the watch supervisor applies when a backend exits with ctx still live:
// the first restart waits watchBackoffBase, each repeated fast failure doubles up
// to watchBackoffMax, and a run that lasted at least watchHealthyRun resets the
// delay to base. They are package vars so tests can shrink them.
var (
	watchBackoffBase = 1 * time.Second
	watchBackoffMax  = 90 * time.Second
	watchHealthyRun  = 30 * time.Second
)

func newServeCmd(build string) *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the resident daemon: own the host mesh, serve the RPC socket, and supervise the watch engines.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return serve(cmd.Context(), build)
		},
	}
}

// serve is the resident process: it binds the rpc socket, registers the
// status/reconcile/reload handlers, and supervises one watch engine per
// discovered manifest. It blocks until ctx is canceled (SIGINT/SIGTERM),
// rebuilding the supervisor on SIGHUP.
func serve(ctx context.Context, build string) error {
	sock, err := hostregistry.Mesh.SockPath()
	if err != nil {
		return err
	}
	dir, err := hostregistryDir()
	if err != nil {
		return err
	}
	processes, err := newProcessOwner(filepath.Join(dir, "processes.db"))
	if err != nil {
		return err
	}
	sup := newSupervisor(processes.pool)
	d := rpc.NewDispatcher()
	d.Register("status", handleStatus)
	// reconcile and reload mutate the engine generation, so they serialize behind
	// the exclusive mutex — a reload never tears down the clients a reconcile pass
	// is mid-drive on; status is a pure read and stays concurrent.
	d.RegisterExclusive("reconcile", func(hctx context.Context, _ map[string]any) (any, error) {
		return reconcileAll(hctx, processes.pool)
	})
	// The generation reload starts must outlive the request, so it parents to
	// serve's ctx: the request ctx dies as soon as Dispatch returns, which would
	// silently cancel every engine the reload just started.
	d.RegisterExclusive("reload", func(_ context.Context, _ map[string]any) (any, error) {
		if err := sup.reload(ctx); err != nil {
			return nil, err
		}
		return map[string]any{"reloaded": true}, nil
	})
	// consent.request|relay|presence ride plain Register (concurrent), never the
	// exclusive mutex reconcile/reload share: a 10-min Touch ID prompt behind it
	// would wedge the daemon.
	registerConsent(d, processes.pool)

	executable, err := executableAlias(daemonBinary)
	if err != nil {
		return fmt.Errorf("resolve synckitd role path: %w", err)
	}
	rpcServer, err := runtimeRPCServer(d, executable, build)
	if err != nil {
		return err
	}
	peer := &wire.LifecyclePeer{Config: wire.ClientConfig{
		Dial: wire.UnixDialer(sock), Build: rpc.Build, LifecycleBuild: build, MaxFrame: rpc.MaxFrame,
	}}
	runtime, err := dkdaemon.NewRuntime(dkdaemon.RuntimeConfig{
		Socket: sock, Build: build, Protocol: int(wire.ProtocolVersion),
		Peer: peer, Contract: dkdaemon.RequestDaemon, WaitMode: dkdaemon.PIDExit,
		Admission: &drain.Intake{}, Server: rpcServer.Wire,
		Workers: &runtimeWorkers{supervisor: sup, processes: processes}, State: runtimeState{},
		Resources: lifecycleResources{peer: peer},
		Activate: func(activation dkdaemon.Activation) error {
			return activateServe(activation, sup, processes, sock)
		},
	})
	if err != nil {
		_ = peer.Close()
		processes.Close()
		processes.Cancel()
		_ = processes.Wait(context.WithoutCancel(ctx))
		return err
	}
	rpcServer.Wire.RegisterLifecycle(runtime)
	err = runtime.Run(ctx)
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return nil
	}
	return err
}

func runtimeRPCServer(dispatcher *rpc.Dispatcher, executable, build string) (*rpc.Server, error) {
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return nil, fmt.Errorf("synckitd role path %q is not exact and absolute", executable)
	}
	role := daemonrole.Classifier{RoleID: labelPrefix + ".serve", RolePath: executable}
	if err := role.Validate(); err != nil {
		return nil, fmt.Errorf("validate synckitd role: %w", err)
	}
	rpcServer := rpc.NewServer(dispatcher)
	rpcServer.Wire.LifecycleBuild = build
	rpcServer.Wire.ReservedProtectedSessions = 1
	rpcServer.Wire.ProtectedSessionClassifier = role
	return rpcServer, nil
}

func activateServe(activation dkdaemon.Activation, sup *supervisor, processes *processOwner, sock string) error {
	if err := processes.recover(activation.Startup); err != nil {
		return fmt.Errorf("recover daemon processes: %w", err)
	}
	if _, err := ensureManifestsDir(); err != nil {
		return err
	}
	dir, err := hostregistry.Mesh.Dir()
	if err != nil {
		return err
	}
	if err := debug.DumpOnSIGUSR1(activation.Lifetime, dir); err != nil {
		return err
	}
	if err := sup.reload(activation.Lifetime); err != nil {
		return err
	}
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		defer signal.Stop(hup)
		for {
			select {
			case <-activation.Lifetime.Done():
				return
			case <-hup:
				if err := sup.reload(activation.Lifetime); err != nil {
					slog.ErrorContext(activation.Lifetime, "serve: reload on SIGHUP", "err", err)
				}
			}
		}
	}()
	slog.InfoContext(activation.Lifetime, "synckitd activated", "socket", sock)
	return nil
}

type runtimeState struct{}

func (runtimeState) Close() error { return nil }

type lifecycleResources struct{ peer *wire.LifecyclePeer }

func (r lifecycleResources) Close() error { return r.peer.Close() }

type runtimeWorkers struct {
	supervisor *supervisor
	processes  *processOwner
}

func (w *runtimeWorkers) Close() {
	w.supervisor.close()
	w.processes.Close()
}

func (w *runtimeWorkers) Cancel() { w.processes.Cancel() }

func (w *runtimeWorkers) Wait(ctx context.Context) error {
	return errors.Join(w.supervisor.wait(ctx), w.processes.Wait(ctx))
}

// supervisor owns the current generation of watch goroutines and the long-lived
// local clients those goroutines drive. reload tears the current generation down
// and starts a fresh one from the manifests on disk, so a register/unregister or
// a SIGHUP rebinds the watchers without restarting the process. It is safe for
// concurrent reload (the rpc reload handler and the SIGHUP goroutine both call
// it).
type supervisor struct {
	pool    *supervise.Pool
	mu      sync.Mutex
	cancel  context.CancelFunc
	wg      *sync.WaitGroup
	clients []*syncservice.Client
	closed  bool
	settled bool
}

func newSupervisor(pool *supervise.Pool) *supervisor {
	return &supervisor{pool: pool}
}

// reload cancels the running watch generation, waits for it to drain, closes the
// old generation's local clients, then starts one watch engine per discovered
// manifest under a fresh child context. The child context is derived from parent,
// so canceling parent (process shutdown) stops the current generation too. reload
// returns promptly: each engine's first I/O runs asynchronously in its watch
// goroutine, so a consumer that is slow to come up never blocks a reload.
func (s *supervisor) reload(parent context.Context) error {
	manifests, err := discoverManifests()
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("watch supervisor is closed")
	}

	if s.cancel != nil {
		s.cancel()
		s.wg.Wait()
		for _, c := range s.clients {
			_ = c.Close()
		}
		s.clients = nil
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

// startEngine builds one manifest's long-lived local client and watch engine and
// launches its supervised watch goroutine. The client is built without any I/O —
// Socket does not dial and Stdio does not spawn until the first Do — so the first
// round trip happens asynchronously under superviseWatch, keeping reload prompt.
// The caller holds s.mu, so appending to s.clients is safe.
func (s *supervisor) startEngine(ctx context.Context, wg *sync.WaitGroup, m manifest.Manifest, reg *hostregistry.Registry) {
	local := syncservice.NewClient(dialTransport(s.pool, m, reg.Self, reg.Self))
	s.clients = append(s.clients, local)
	eng := buildEngine(ctx, local, m, reg, s.pool)

	// run returns how long it spent inside the backend, so a run that dies in the
	// list phase (never reaching the backend) reports zero and never counts healthy.
	run := func(rctx context.Context) (time.Duration, error) {
		items, err := listForEngine(rctx, local, m.Name)
		if err != nil {
			return 0, err
		}
		dirsByID := make(map[string][]string, len(items))
		for _, it := range items {
			dirsByID[it.ID] = it.WatchDirs
		}
		start := time.Now()
		err = watchbackend.Run(rctx, dirsByID, func(id string) {
			eng.OnEvent(rctx, id)
		})
		return time.Since(start), err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		superviseWatch(ctx, m.Name, run)
	}()
}

// superviseWatch runs the watch backend, restarting it with exponential backoff
// whenever it exits while ctx is live: a backend death or a transient list
// failure must not silently drop a manifest's watches until the next reload. A run
// counts healthy only by the time run reports it spent inside the backend, so a
// slow list that then fails fast never resets the delay. Each restart re-runs run
// from scratch, so the backend rebuilds all state and drops any missed events — the
// periodic reconcile is the floor that covers the gap. It returns once ctx is
// canceled.
func superviseWatch(ctx context.Context, name string, run func(context.Context) (time.Duration, error)) {
	var delay time.Duration
	for {
		if err := sleepCtx(ctx, delay); err != nil {
			return
		}
		backendDur, err := run(ctx)
		if ctx.Err() != nil {
			return
		}
		delay = backoffAfter(delay, backendDur >= watchHealthyRun)
		slog.ErrorContext(ctx, "serve: watch backend exited, restarting", "manifest", name, "backoff", delay, "err", err)
	}
}

// backoffAfter is the delay before the next watch restart given the last delay and
// whether the run that just exited was healthy (lasted at least watchHealthyRun). A
// healthy run resets to base; otherwise the delay doubles, capped at watchBackoffMax.
func backoffAfter(prev time.Duration, healthy bool) time.Duration {
	if healthy || prev == 0 {
		return watchBackoffBase
	}
	return min(2*prev, watchBackoffMax)
}

// listForEngine lists the consumer's items, retrying a transient connection failure
// on a bounded backoff until success, the retry budget is exhausted, or ctx is done.
func listForEngine(ctx context.Context, c *syncservice.Client, name string) ([]syncservice.WatchItem, error) {
	deadline := time.Now().Add(listRetryBudget)
	for {
		items, err := c.List(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("list for %q: %w", name, err)
			}
			slog.WarnContext(ctx, "serve: list not yet reachable, retrying", "manifest", name, "err", err)
			if err := sleepCtx(ctx, listBackoff); err != nil {
				return nil, err
			}
			continue
		}
		return items, nil
	}
}

// sleepCtx waits d or until ctx is done, returning ctx.Err() if ctx is canceled
// first. It never blocks past ctx, so a backoff honors process shutdown.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// buildEngine wires one manifest's watch engine: the resolver and notifier drive
// the consumer's typed sync service, the digest is the identity (the id is already
// the stable key), and the host fan-out is self first (local converge) then peers.
// The notifier is wrapped in a per-peer circuit breaker under ctx (the generation
// context its retry timers outlive single events on), so a repeatedly unreachable
// peer is logged once and probed on a backoff instead of on every event. The gate
// defers a busy item's evaluation at the debounce cadence, firing through after ten
// windows so a persistently busy item can only delay a change, never park it.
func buildEngine(
	ctx context.Context,
	local *syncservice.Client,
	m manifest.Manifest,
	reg *hostregistry.Registry,
	pool *supervise.Pool,
) *watch.Engine[string] {
	hosts := append([]string{reg.Self}, reg.Hosts...)
	debounce := time.Duration(m.Watch.Debounce)
	memo := newFingerprintMemo()
	return watch.NewEngine[string](
		manifestResolver{client: local, name: m.Name, memo: memo},
		newBreakerNotifier(
			ctx,
			manifestNotifier{local: local, m: m, self: reg.Self, pool: pool},
			m.Name,
			reg.Self,
			pool,
		),
		func(id string) string { return id },
		debounce,
		hosts,
		watch.WithGate[string](manifestGate{client: local, name: m.Name, memo: memo}, debounce, 10*debounce),
	)
}

func (s *supervisor) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *supervisor) wait(ctx context.Context) error {
	s.mu.Lock()
	if s.settled {
		s.mu.Unlock()
		return ctx.Err()
	}
	wg := s.wg
	s.mu.Unlock()
	if wg != nil {
		wg.Wait()
	}
	s.mu.Lock()
	for _, client := range s.clients {
		_ = client.Close()
	}
	s.clients = nil
	s.cancel = nil
	s.settled = true
	s.mu.Unlock()
	return ctx.Err()
}

// stop closes and joins the supervisor for non-runtime owners such as tests.
func (s *supervisor) stop() {
	s.close()
	_ = s.wait(context.Background())
}

// manifestResolver resolves a watch id's apply-stable fingerprint by listing the
// consumer's items over its typed service and finding the item by id. Because the
// fingerprint is apply-stable, the engine dedups a consumer's own write without a
// cross-process Seed: after the consumer applies a peer's change, the item's
// fingerprint matches the value the engine already recorded. A missing item
// resolves to "" so the engine treats it as no change. A fingerprint the gate
// stashed from its own List round trip in the same evaluation is consumed instead
// of listing again.
type manifestResolver struct {
	client *syncservice.Client
	name   string
	memo   *fingerprintMemo
}

func (r manifestResolver) Resolve(ctx context.Context, id string) (string, error) {
	if fingerprint, ok := r.memo.take(id); ok {
		return fingerprint, nil
	}
	items, err := r.client.List(ctx)
	if err != nil {
		return "", fmt.Errorf("list watch items for %q: %w", r.name, err)
	}
	for _, it := range items {
		if it.ID == id {
			return it.Fingerprint, nil
		}
	}
	return "", nil
}

// manifestGate reports an item's busy state from the consumer's List, so the
// engine defers acting on an item its consumer says is mid-operation. A missing
// item is not busy. The engine consults the gate immediately before the resolver
// in the same evaluation, so the gate stashes the fingerprint its List round trip
// already carried for the resolver to consume — one List per gated evaluation,
// not two.
type manifestGate struct {
	client *syncservice.Client
	name   string
	memo   *fingerprintMemo
}

func (g manifestGate) Busy(ctx context.Context, id string) (bool, string, error) {
	items, err := g.client.List(ctx)
	if err != nil {
		return false, "", fmt.Errorf("list watch items for %q: %w", g.name, err)
	}
	for _, it := range items {
		if it.ID == id {
			g.memo.put(id, it.Fingerprint)
			return it.Busy, it.BusyReason, nil
		}
	}
	g.memo.put(id, "")
	return false, "", nil
}

// fingerprintMemo hands an id's fingerprint from the gate's List round trip to the
// resolver within a single evaluation. take consumes the entry and every gate
// check overwrites it fresh, so a stashed fingerprint never serves an evaluation
// other than the one that produced it.
type fingerprintMemo struct {
	mu           sync.Mutex
	fingerprints map[string]string
}

func newFingerprintMemo() *fingerprintMemo {
	return &fingerprintMemo{fingerprints: make(map[string]string)}
}

func (m *fingerprintMemo) put(id, fingerprint string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fingerprints[id] = fingerprint
}

func (m *fingerprintMemo) take(id string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fingerprint, ok := m.fingerprints[id]
	if ok {
		delete(m.fingerprints, id)
	}
	return fingerprint, ok
}

// manifestNotifier drives the consumer's typed Sync for one peer: the self host
// runs it locally over the long-lived local client with an empty origin, a remote
// peer runs it over an ssh transport with origin set to this host so the peer
// skips notifying back (anti-echo provenance). The typed Sync converges the whole
// consumer, so the id is unused. One unreachable peer never blocks the others —
// the engine fans out concurrently and isolates each error.
type manifestNotifier struct {
	local *syncservice.Client
	m     manifest.Manifest
	self  string
	pool  *supervise.Pool
}

func (n manifestNotifier) Notify(ctx context.Context, peer, _ string) error {
	if peer == n.self {
		if _, err := n.local.Sync(ctx, ""); err != nil {
			return fmt.Errorf("local sync for %q: %w", n.m.Name, err)
		}
		return nil
	}
	c := syncservice.NewClient(dialTransport(n.pool, n.m, peer, n.self))
	defer func() { _ = c.Close() }()
	if _, err := c.Sync(ctx, n.self); err != nil {
		return fmt.Errorf("ssh sync for %q on %s: %w", n.m.Name, peer, err)
	}
	return nil
}
