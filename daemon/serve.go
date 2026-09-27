package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/debug"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/netpolicy"
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

// runtimeProduct is the daemon daemonkit publishes at readiness: the dispatcher
// answers business, and the watch supervisor spends the shutdown budget.
type runtimeProduct struct {
	supervisor *supervisor
	dispatcher *rpc.Dispatcher
}

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the resident daemon: own the host mesh, serve the RPC socket, and supervise the watch engines.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return serve(cmd.Context())
		},
	}
}

// serve is the resident process: daemonkit owns the socket, the singleton
// flock, the owner record, and the drain ladder; start builds the dispatcher
// and the watch supervisor, and returning the product IS readiness. It blocks
// until ctx is canceled, a drain signal arrives, or the drain verb lands.
func serve(ctx context.Context) error {
	spec, err := stableServeSpec()
	if err != nil {
		return err
	}
	dir, err := hostregistryDir()
	if err != nil {
		return err
	}
	_, err = daemonkit.Serve(ctx, spec, func(c daemonkit.Ctx) (daemonkit.Product, error) {
		return startServe(c, dir)
	})
	return err
}

// startServe wires the daemon against the ownership scope Serve hands over. The
// engine generations and every presentation it starts parent to c.Context — the
// activation's own lifetime, cancelled when the drain begins.
//
//nolint:contextcheck // c.Context is the lifetime Serve mints for the product; the caller's own ctx is deliberately not it.
func startServe(c daemonkit.Ctx, dir string) (daemonkit.Product, error) {
	manualPath, err := netpolicy.ManualPath()
	if err != nil {
		return nil, err
	}
	monitor, err := netpolicy.NewMonitor(manualPath)
	if err != nil {
		return nil, err
	}
	sup := newSupervisor(c, newDeliveryStore(dir), monitor)
	d := rpc.NewDispatcher()
	d.Register("status", handleStatus)
	registerDelivery(d, sup)
	// reconcile and reload mutate the engine generation, so they serialize behind
	// the exclusive mutex — a reload never tears down the clients a reconcile pass
	// is mid-drive on; status is a pure read and stays concurrent.
	d.RegisterExclusive("reconcile", func(hctx context.Context, _ map[string]any) (any, error) {
		return sup.reconcile(hctx, c.Context)
	})
	// The generation reload starts must outlive the request, so it parents to the
	// daemon's own lifetime: the request ctx dies as soon as Dispatch returns,
	// which would silently cancel every engine the reload just started.
	d.RegisterExclusive("reload", func(context.Context, map[string]any) (any, error) {
		//nolint:contextcheck // the engines this starts outlive the request; the daemon's own lifetime is their only honest parent.
		if err := sup.reload(c.Context); err != nil {
			return nil, err
		}
		return map[string]any{"reloaded": true}, nil
	})
	// consent.request|relay|presence ride plain Register (concurrent), never the
	// exclusive mutex reconcile/reload share: a 10-min Touch ID prompt behind it
	// would wedge the daemon.
	registerConsent(d, c)
	if err := activateServe(c.Context, sup); err != nil {
		closeCtx, cancel := context.WithTimeout(c.Context, serveShutdown)
		defer cancel()
		sup.close()
		_ = sup.wait(closeCtx)
		_ = monitor.Close()
		return nil, err
	}
	return &runtimeProduct{supervisor: sup, dispatcher: d}, nil
}

func (p *runtimeProduct) Handle(ctx context.Context, request daemonkit.Request) (daemonkit.Reply, error) {
	return p.dispatcher.Handle(ctx, request)
}

// Drain cancels the running watch generation and joins its goroutines within
// the drain share; Close settles whatever the join left, closing the long-lived
// local clients that generation drove.
func (p *runtimeProduct) Drain(budget daemonkit.Budget) error {
	p.supervisor.close()
	ctx, cancel := budget.Context(context.Background())
	defer cancel()
	return p.supervisor.wait(ctx)
}

func (p *runtimeProduct) Close(budget daemonkit.Budget) error {
	ctx, cancel := budget.Context(context.Background())
	defer cancel()
	return errors.Join(p.supervisor.wait(ctx), p.supervisor.monitor.Close())
}

// activateServe does the presentation work bound to the daemon's activation
// lifetime. SIGHUP is not a reload trigger any more — daemonkit claims it as a
// drain signal — so a manifest change rebinds the watchers through the reload
// rpc verb, which register and unregister already nudge.
func activateServe(lifetime context.Context, sup *supervisor) error {
	if _, err := ensureManifestsDir(); err != nil {
		return err
	}
	dir, err := hostregistry.Mesh.Dir()
	if err != nil {
		return err
	}
	if err := debug.DumpOnSIGUSR1(lifetime, dir); err != nil {
		return err
	}
	if err := sup.reload(lifetime); err != nil {
		return err
	}
	slog.InfoContext(lifetime, "synckitd activated", "label", serveLabel)
	return nil
}

type supervisor struct {
	scope     processScope
	delivery  *deliveryStore
	monitor   netpolicy.Monitor
	mu        sync.Mutex
	cancel    context.CancelFunc
	wg        *sync.WaitGroup
	clients   []*syncservice.Client
	scheduler *deliveryScheduler
	mesh      hostregistry.Registry
	closed    bool
	settled   bool
}

func newSupervisor(scope processScope, delivery *deliveryStore, monitor netpolicy.Monitor) *supervisor {
	return &supervisor{scope: scope, delivery: delivery, monitor: monitor}
}

func (s *supervisor) reconcile(ctx, lifetime context.Context) ([]reconcileResult, error) {
	results, err := reconcileAll(ctx, s.scope)
	if err != nil {
		return nil, err
	}
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return nil, fmt.Errorf("load mesh: %w", err)
	}
	s.mu.Lock()
	current := s.mesh
	s.mu.Unlock()
	if current.Self != reg.Self || !slices.Equal(current.Hosts, reg.Hosts) {
		if err := s.reload(lifetime); err != nil {
			return nil, err
		}
	}
	scheduler, err := s.deliveries()
	if err != nil {
		return nil, err
	}
	return results, scheduler.Kick("", "")
}

func (s *supervisor) deliveries() (*deliveryScheduler, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.scheduler == nil {
		return nil, errors.New("delivery scheduler is not running")
	}
	return s.scheduler, nil
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
		s.scheduler = nil
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

	scheduler := newDeliveryScheduler(ctx, wg, s.scope, s.delivery, s.monitor, reg.Self)
	locals := make([]*syncservice.Client, len(manifests))
	for i, m := range manifests {
		locals[i] = syncservice.NewClient(dialTransport(s.scope, m, reg.Self, reg.Self))
		s.clients = append(s.clients, locals[i])
		scheduler.add(m, locals[i], reg.Hosts)
	}
	for i, m := range manifests {
		s.startEngine(ctx, wg, m, locals[i], reg, scheduler)
	}
	scheduler.start()
	s.scheduler, s.mesh = scheduler, *reg
	slog.InfoContext(ctx, "synckitd watch supervisor reloaded", "manifests", len(manifests))
	return nil
}

func (s *supervisor) startEngine(
	ctx context.Context,
	wg *sync.WaitGroup,
	m manifest.Manifest,
	local *syncservice.Client,
	reg *hostregistry.Registry,
	scheduler *deliveryScheduler,
) {
	eng := buildEngine(ctx, local, m, reg, func(peer string) error { return scheduler.Kick(m.Name, peer) })

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

func buildEngine(
	ctx context.Context,
	local *syncservice.Client,
	m manifest.Manifest,
	reg *hostregistry.Registry,
	kick func(peer string) error,
) *watch.Engine[string] {
	hosts := append([]string{reg.Self}, reg.Hosts...)
	debounce := time.Duration(m.Watch.Debounce)
	memo := newFingerprintMemo()
	return watch.NewEngine[string](
		manifestResolver{client: local, name: m.Name, memo: memo},
		manifestNotifier{
			self:  reg.Self,
			local: newBreakerNotifier(ctx, localReconciler{client: local, name: m.Name}, m.Name),
			kick:  kick,
		},
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
	s.scheduler = nil
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

type manifestNotifier struct {
	self  string
	local watch.Notifier[string]
	kick  func(peer string) error
}

func (n manifestNotifier) Notify(ctx context.Context, peer, id string) error {
	if peer == n.self {
		return n.local.Notify(ctx, peer, id)
	}
	return n.kick(peer)
}

type localReconciler struct {
	client *syncservice.Client
	name   string
}

func (r localReconciler) Notify(ctx context.Context, _, _ string) error {
	if _, err := r.client.Reconcile(ctx, ""); err != nil {
		return fmt.Errorf("local sync for %q: %w", r.name, err)
	}
	return nil
}
