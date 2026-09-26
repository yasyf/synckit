package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/syncservice"
)

var (
	deliveryBackoffBase = 30 * time.Second
	deliveryBackoffMax  = 5 * time.Minute
	pauseRecheck        = 60 * time.Second
	artifactMaxWait     = 10 * time.Second
	peerStateMaxAge     = 2 * time.Minute
	batchObjectLimit    = artifact.MaxBatchObjects
)

const (
	deliveryPinOwnerPrefix = "synckit.delivery/"
	snapshotTimeout        = 15 * time.Second
)

var (
	errIncomplete = errors.New("delivery: peer has not completed every artifact root")
	errSuperseded = errors.New("delivery: a newer kick superseded the transfer")
)

type pauseError struct {
	reason delivery.PauseReason
	cause  error
}

func (e *pauseError) Error() string {
	if e.cause == nil {
		return "delivery paused: " + string(e.reason)
	}
	return fmt.Sprintf("delivery paused: %s: %v", e.reason, e.cause)
}

func (e *pauseError) Unwrap() error { return e.cause }

type laneKey struct {
	service string
	peer    string
}

type laneLive struct {
	state         delivery.State
	pauseReason   delivery.PauseReason
	pauseSince    time.Time
	lastError     string
	lastAttemptAt time.Time
	nextAttemptAt time.Time
	progress      delivery.Progress
	localNetwork  *netpolicy.State
	peerNetwork   *netpolicy.State
	down          bool
	downSince     time.Time
}

type lane struct {
	laneKey
	m         manifest.Manifest
	local     *syncservice.Client
	kick      chan struct{}
	wake      <-chan struct{}
	artifacts atomic.Bool

	mu      sync.Mutex
	dirtyAt time.Time
	live    laneLive
}

func (l *lane) poke(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.dirtyAt.IsZero() {
		l.dirtyAt = now
	}
	select {
	case l.kick <- struct{}{}:
	default:
	}
}

func (l *lane) clean() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dirtyAt = time.Time{}
	select {
	case <-l.kick:
	default:
	}
}

func (l *lane) due() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.dirtyAt.IsZero() {
		return time.Time{}
	}
	if !l.artifacts.Load() {
		return l.dirtyAt
	}
	return l.dirtyAt.Add(artifactMaxWait)
}

func (l *lane) update(fn func(*laneLive)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fn(&l.live)
}

func (l *lane) snapshot() laneLive {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.live
}

type deliveryScheduler struct {
	ctx      context.Context
	wg       *sync.WaitGroup
	scope    processScope
	store    *deliveryStore
	monitor  netpolicy.Monitor
	self     string
	services map[string]bool
	lanes    map[laneKey]*lane
	order    []laneKey
	now      func() time.Time
	snapshot func(ctx context.Context, peer string)
}

func newDeliveryScheduler(
	ctx context.Context,
	wg *sync.WaitGroup,
	scope processScope,
	store *deliveryStore,
	monitor netpolicy.Monitor,
	self string,
) *deliveryScheduler {
	return &deliveryScheduler{
		ctx: ctx, wg: wg, scope: scope, store: store, monitor: monitor, self: self,
		services: make(map[string]bool),
		lanes:    make(map[laneKey]*lane),
		now:      time.Now,
		snapshot: func(ctx context.Context, peer string) { tailscaleSnapshot(ctx, scope, peer) },
	}
}

func (s *deliveryScheduler) add(m manifest.Manifest, local *syncservice.Client, peers []string) {
	s.services[m.Name] = true
	for _, peer := range peers {
		if peer == s.self {
			continue
		}
		key := laneKey{service: m.Name, peer: peer}
		s.lanes[key] = &lane{laneKey: key, m: m, local: local, kick: make(chan struct{}, 1)}
		s.order = append(s.order, key)
	}
	slices.SortFunc(s.order, compareLaneKeys)
}

func (s *deliveryScheduler) start() {
	for _, key := range s.order {
		l := s.lanes[key]
		l.poke(s.now())
		s.wg.Go(func() { s.work(l) })
	}
}

func (s *deliveryScheduler) Kick(serviceID, peer string) error {
	if serviceID != "" && !s.services[serviceID] {
		return fmt.Errorf("delivery: unknown service %q", serviceID)
	}
	matched := false
	for _, key := range s.order {
		if (serviceID == "" || key.service == serviceID) && (peer == "" || key.peer == peer) {
			s.lanes[key].poke(s.now())
			matched = true
		}
	}
	if peer != "" && !matched {
		return fmt.Errorf("delivery: no lane for service %q to peer %q", serviceID, peer)
	}
	return nil
}

func (s *deliveryScheduler) status(ctx context.Context, serviceID string) ([]delivery.PeerStatus, error) {
	records, err := s.store.records(ctx, serviceID)
	if err != nil {
		return nil, err
	}
	out := make([]delivery.PeerStatus, 0, len(records)+len(s.order))
	covered := make(map[laneKey]bool, len(s.order))
	for _, key := range s.order {
		if serviceID != "" && key.service != serviceID {
			continue
		}
		record := deliveryRecord{ServiceID: key.service, Peer: key.peer, Acked: syncservice.NewRevision(0)}
		if index := slices.IndexFunc(records, func(r deliveryRecord) bool { return r.ServiceID == key.service && r.Peer == key.peer }); index >= 0 {
			record = records[index]
		}
		live := s.lanes[key].snapshot()
		out = append(out, peerStatus(record, &live))
		covered[key] = true
	}
	for _, record := range records {
		if !covered[laneKey{service: record.ServiceID, peer: record.Peer}] {
			out = append(out, peerStatus(record, nil))
		}
	}
	slices.SortFunc(out, func(a, b delivery.PeerStatus) int {
		return compareLaneKeys(laneKey{a.ServiceID, a.Peer}, laneKey{b.ServiceID, b.Peer})
	})
	return out, nil
}

func peerStatus(record deliveryRecord, live *laneLive) delivery.PeerStatus {
	status := delivery.PeerStatus{
		ServiceID: record.ServiceID, Peer: record.Peer, Generation: record.Generation,
		Acked: record.Acked, AckedChangeID: record.AckedChangeID, AckedAt: record.AckedAt,
		State: delivery.StateIdle,
	}
	if p := record.Pending; p != nil {
		status.Pending = &delivery.Pending{
			ChangeID: p.ChangeID, Kind: p.Kind, BaseRevision: p.BaseRevision, SourceRevision: p.SourceRevision,
			Roots: len(p.Roots), StagedAt: p.StagedAt, Superseded: p.Superseded,
		}
		status.State = delivery.StateStaged
	}
	if live == nil {
		return status
	}
	if live.state != "" {
		status.State = live.state
	}
	status.PauseReason = live.pauseReason
	status.PauseSince = live.pauseSince
	status.LastError = live.lastError
	status.LastAttemptAt = live.lastAttemptAt
	status.NextAttemptAt = live.nextAttemptAt
	status.Progress = live.progress
	status.LocalNetwork = live.localNetwork
	status.PeerNetwork = live.peerNetwork
	return status
}

func (s *deliveryScheduler) work(l *lane) {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	coalesce := time.NewTimer(time.Hour)
	coalesce.Stop()
	defer coalesce.Stop()
	var retry, due <-chan time.Time
	var wake <-chan struct{}
	var delay time.Duration
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-l.kick:
			switch {
			case retry != nil:
				if _, _, err := s.prepare(s.ctx, l); err != nil && s.ctx.Err() == nil {
					slog.WarnContext(s.ctx, "delivery: stage failed", "manifest", l.service, "peer", l.peer, "err", err)
					l.update(func(v *laneLive) { v.lastError = err.Error() })
				}
			case due == nil:
				coalesce.Reset(l.due().Sub(s.now()))
				due = coalesce.C
			}
			continue
		case <-due:
		case <-retry:
		case <-wake:
		}
		timer.Stop()
		coalesce.Stop()
		retry, wake, due = nil, nil, nil
		attempt := s.now()
		l.update(func(v *laneLive) { v.lastAttemptAt, v.nextAttemptAt = attempt, time.Time{} })
		err := s.deliverOnce(s.ctx, l)
		if s.ctx.Err() != nil {
			return
		}
		var paused *pauseError
		switch {
		case errors.As(err, &paused):
			s.pause(l, paused)
			timer.Reset(pauseRecheck)
			retry, wake = timer.C, l.wake
		case err != nil:
			delay = min(max(2*delay, deliveryBackoffBase), deliveryBackoffMax)
			s.fail(l, err, delay)
			timer.Reset(delay)
			retry = timer.C
		default:
			delay = 0
			s.settle(l)
		}
	}
}

func (s *deliveryScheduler) pause(l *lane, paused *pauseError) {
	now := s.now()
	var entered, down bool
	l.update(func(v *laneLive) {
		entered = v.state != delivery.StatePaused || v.pauseReason != paused.reason
		if entered {
			v.pauseSince = now
		}
		v.state, v.pauseReason, v.nextAttemptAt = delivery.StatePaused, paused.reason, now.Add(pauseRecheck)
		v.lastError = ""
		if paused.cause != nil {
			v.lastError = paused.cause.Error()
		}
		if paused.reason == delivery.PausePeerUnreachable && !v.down {
			v.down, v.downSince, down = true, now, true
		}
	})
	switch {
	case down:
		s.logDown(l, paused, pauseRecheck)
	case entered:
		slog.InfoContext(s.ctx, "delivery: paused", "manifest", l.service, "peer", l.peer, "reason", paused.reason)
	}
}

func (s *deliveryScheduler) fail(l *lane, err error, delay time.Duration) {
	now := s.now()
	var down bool
	l.update(func(v *laneLive) {
		v.state, v.pauseReason, v.pauseSince = delivery.StateBackoff, "", time.Time{}
		v.lastError, v.nextAttemptAt = err.Error(), now.Add(delay)
		if !v.down {
			v.down, v.downSince, down = true, now, true
		}
	})
	if down {
		s.logDown(l, err, delay)
	}
}

func (s *deliveryScheduler) settle(l *lane) {
	var downFor time.Duration
	var recovered bool
	now := s.now()
	l.update(func(v *laneLive) {
		if v.down {
			recovered, downFor = true, now.Sub(v.downSince)
		}
		v.down, v.state, v.pauseReason, v.pauseSince, v.lastError = false, "", "", time.Time{}, ""
	})
	if recovered {
		slog.InfoContext(s.ctx, "delivery: peer recovered", "manifest", l.service, "peer", l.peer, "down_for", downFor)
	}
}

func (s *deliveryScheduler) logDown(l *lane, cause error, retryIn time.Duration) {
	slog.WarnContext(s.ctx, "delivery: peer unreachable; retrying until recovery",
		"manifest", l.service, "peer", l.peer, "err", cause, "retry_in", retryIn)
	s.wg.Go(func() { s.snapshot(s.ctx, l.peer) })
}

func (s *deliveryScheduler) prepare(ctx context.Context, l *lane) (bool, *syncservice.ChangeEnvelope, error) {
	caps, err := l.local.Capabilities(ctx)
	if err != nil {
		return false, nil, fmt.Errorf("local capabilities for %q: %w", l.service, err)
	}
	artifacts := slices.Contains(caps.Methods, syncservice.MethodExportV2)
	l.artifacts.Store(artifacts)
	record, pending, err := s.store.load(ctx, l.service, l.peer)
	if err != nil {
		return false, nil, err
	}
	fresh, err := s.export(ctx, l, artifacts, record.Acked)
	if err != nil {
		return false, nil, err
	}
	source, acked := mustRevision(fresh.SourceRevision), mustRevision(record.Acked)
	floor := acked
	if pending != nil {
		floor = max(floor, mustRevision(pending.SourceRevision))
	}
	if source < floor {
		return false, nil, fmt.Errorf("%w: export %d is behind %d", errSourceRegressed, source, floor)
	}
	switch {
	case pending == nil && source == acked:
		return artifacts, nil, nil
	case pending == nil:
		bound, err := s.replace(ctx, l, artifacts, "", nil, fresh)
		return artifacts, &bound, err
	case source == floor:
		return artifacts, pending, nil
	}
	if fresh.Kind != syncservice.ChangeSnapshot {
		if fresh, err = s.export(ctx, l, artifacts, syncservice.NewRevision(0)); err != nil {
			return false, nil, err
		}
	}
	bound, err := s.replace(ctx, l, artifacts, pending.ChangeID, pending.Artifacts, fresh)
	return artifacts, &bound, err
}

func (s *deliveryScheduler) export(ctx context.Context, l *lane, artifacts bool, since syncservice.Revision) (syncservice.ChangeEnvelope, error) {
	request := syncservice.ExportRequest{
		ServiceID: l.service, SchemaFingerprint: l.m.Service.SchemaFingerprint, SinceRevision: since,
	}
	var change syncservice.ChangeEnvelope
	var err error
	if artifacts {
		change, err = l.local.ExportV2(ctx, request)
	} else {
		change, err = l.local.Export(ctx, request)
	}
	if err != nil {
		return syncservice.ChangeEnvelope{}, fmt.Errorf("export %q since %s: %w", l.service, since, err)
	}
	if since == syncservice.NewRevision(0) && change.Kind != syncservice.ChangeSnapshot {
		return syncservice.ChangeEnvelope{}, fmt.Errorf("export %q: full export returned a %s", l.service, change.Kind)
	}
	return change, nil
}

func (s *deliveryScheduler) replace(
	ctx context.Context,
	l *lane,
	artifacts bool,
	expected string,
	old []artifact.Ref,
	fresh syncservice.ChangeEnvelope,
) (syncservice.ChangeEnvelope, error) {
	bound, err := syncservice.BindDelivery(fresh, s.self)
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	owner := deliveryPinOwnerPrefix + l.peer
	if artifacts {
		if err := l.local.PinsSet(ctx, owner, unionRoots(old, bound.Artifacts)); err != nil {
			return syncservice.ChangeEnvelope{}, fmt.Errorf("pin staged roots: %w", err)
		}
	}
	if err := s.store.stage(ctx, l.peer, expected, bound); err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	if artifacts {
		if err := l.local.PinsSet(ctx, owner, bound.Artifacts); err != nil {
			return syncservice.ChangeEnvelope{}, fmt.Errorf("pin pending roots: %w", err)
		}
	}
	return bound, nil
}

func unionRoots(old, next []artifact.Ref) []artifact.Ref {
	union := slices.Clone(next)
	for _, ref := range old {
		if !slices.ContainsFunc(union, func(r artifact.Ref) bool { return r.Digest == ref.Digest }) {
			union = append(union, ref)
		}
	}
	return union
}

func (s *deliveryScheduler) deliverOnce(ctx context.Context, l *lane) error {
	for {
		l.clean()
		err := s.deliverPending(ctx, l)
		if !errors.Is(err, errSuperseded) {
			return err
		}
		slog.InfoContext(ctx, "delivery: superseded mid-transfer; restaging", "manifest", l.service, "peer", l.peer)
	}
}

func (s *deliveryScheduler) deliverPending(ctx context.Context, l *lane) error {
	artifacts, pending, err := s.prepare(ctx, l)
	if err != nil || pending == nil {
		return err
	}
	peer := syncservice.NewClient(dialTransport(s.scope, l.m, l.peer, s.self))
	defer func() { _ = peer.Close() }()
	run := &deliveryRun{s: s, l: l, peer: peer, artifacts: artifacts, change: *pending}
	if artifacts {
		if err := run.gate(ctx); err != nil {
			return err
		}
		if err := run.transfer(ctx); err != nil {
			return err
		}
	}
	return run.apply(ctx)
}

type deliveryRun struct {
	s          *deliveryScheduler
	l          *lane
	peer       *syncservice.Client
	artifacts  bool
	change     syncservice.ChangeEnvelope
	localState netpolicy.State
	peerState  netpolicy.State
	peerAt     time.Time
}

func (r *deliveryRun) observeLocal() netpolicy.State {
	state, changed := r.s.monitor.Current()
	r.l.wake, r.localState = changed, state
	r.l.update(func(v *laneLive) { v.localNetwork = &state })
	return state
}

func (r *deliveryRun) observePeer(state netpolicy.State) {
	r.peerState, r.peerAt = state, r.s.now()
	r.l.update(func(v *laneLive) { v.peerNetwork = &state })
}

func (r *deliveryRun) gate(ctx context.Context) error {
	local := r.observeLocal()
	caps, err := r.peer.Capabilities(ctx)
	if err != nil {
		return unreachable(local, err)
	}
	if !artifactCapable(caps.Methods) {
		return &pauseError{reason: delivery.PausePeerIncompatible}
	}
	if err := r.probe(ctx, local); err != nil {
		return err
	}
	return r.evaluate(local)
}

func (r *deliveryRun) admit(ctx context.Context) (netpolicy.State, error) {
	local := r.observeLocal()
	if r.s.now().Sub(r.peerAt) >= peerStateMaxAge {
		if err := r.probe(ctx, local); err != nil {
			return local, err
		}
	}
	return local, r.evaluate(local)
}

func (r *deliveryRun) probe(ctx context.Context, local netpolicy.State) error {
	state, err := r.peer.NetStatus(ctx)
	if err != nil {
		return unreachable(local, err)
	}
	r.observePeer(state)
	return nil
}

func (r *deliveryRun) evaluate(local netpolicy.State) error {
	if verdict := netpolicy.Evaluate(local, r.peerState); !verdict.Allowed {
		return &pauseError{reason: delivery.ReasonForVerdict(verdict)}
	}
	return nil
}

func (r *deliveryRun) superseded() bool {
	due := r.l.due()
	return !due.IsZero() && !r.s.now().Before(due)
}

func unreachable(local netpolicy.State, err error) error {
	if !local.Unrestricted() {
		return &pauseError{reason: localReason(local), cause: err}
	}
	return &pauseError{reason: delivery.PausePeerUnreachable, cause: err}
}

func refusal(err error) error {
	var paused *artifact.PausedError
	if errors.As(err, &paused) {
		return &pauseError{reason: delivery.ReasonForRefusal(paused), cause: paused}
	}
	return err
}

func artifactCapable(methods []string) bool {
	return slices.Contains(methods, syncservice.MethodApplyV2) &&
		!slices.ContainsFunc(artifact.Methods, func(m string) bool { return !slices.Contains(methods, m) })
}

func localReason(local netpolicy.State) delivery.PauseReason {
	return delivery.ReasonForVerdict(netpolicy.Evaluate(local, netpolicy.State{}))
}

func (r *deliveryRun) transfer(ctx context.Context) error {
	roots := r.change.Artifacts
	r.l.update(func(v *laneLive) {
		v.state = delivery.StateTransferring
		v.progress = delivery.Progress{RootsTotal: len(roots), InFlightLimit: artifact.PartSize}
	})
	if len(roots) == 0 {
		return nil
	}
	xctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var watcher sync.WaitGroup
	defer watcher.Wait()
	stop := make(chan struct{})
	defer close(stop)
	watcher.Go(func() { r.watchLocal(xctx, cancel, stop) })
	err := r.enumerate(xctx)
	var paused *pauseError
	if err != nil && errors.As(context.Cause(xctx), &paused) {
		return paused
	}
	return err
}

func (r *deliveryRun) watchLocal(ctx context.Context, cancel context.CancelCauseFunc, stop <-chan struct{}) {
	for {
		state, changed := r.s.monitor.Current()
		if !state.Unrestricted() {
			cancel(&pauseError{reason: localReason(state)})
			return
		}
		select {
		case <-changed:
		case <-stop:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (r *deliveryRun) enumerate(ctx context.Context) error {
	roots := r.change.Artifacts
	rootSet := make(map[artifact.Digest]bool, len(roots))
	for _, root := range roots {
		rootSet[root.Digest] = true
	}
	after := 0
	for {
		page, err := r.l.local.ArtifactClosure(ctx, artifact.ClosureParams{Roots: roots, After: after, Limit: artifact.MaxClosurePage})
		if err != nil {
			return fmt.Errorf("closure of %q: %w", r.l.service, err)
		}
		if err := r.page(ctx, page.Objects, page.Done, rootSet); err != nil {
			return err
		}
		if page.Done {
			return nil
		}
		after = page.Next
	}
}

func (r *deliveryRun) page(ctx context.Context, objects []artifact.ObjectEntry, done bool, rootSet map[artifact.Digest]bool) error {
	missing := map[artifact.Digest]bool{}
	if len(objects) > 0 {
		digests := make([]artifact.Digest, len(objects))
		for i, o := range objects {
			digests[i] = o.Digest
		}
		if _, err := r.admit(ctx); err != nil {
			return err
		}
		absent, err := r.peer.ArtifactHave(ctx, digests)
		if err != nil {
			return refusal(fmt.Errorf("peer have: %w", err))
		}
		for _, d := range absent {
			missing[d] = true
		}
	}
	var bytesMissing int64
	for _, o := range objects {
		if missing[o.Digest] {
			bytesMissing += o.Size
		}
	}
	r.l.update(func(v *laneLive) {
		v.progress.ObjectsMissing += int64(len(missing))
		v.progress.BytesMissing += bytesMissing
		v.progress.EnumerationDone = done
	})
	var batch []artifact.ObjectEntry
	var batchBytes int64
	waiting := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if r.superseded() {
			return errSuperseded
		}
		if err := r.send(ctx, batch); err != nil {
			return err
		}
		completed := waiting
		batch, batchBytes, waiting = nil, 0, 0
		var progressive bool
		r.l.update(func(v *laneLive) {
			v.progress.RootsComplete += completed
			progressive = completed > 0 && v.progress.RootsComplete < v.progress.RootsTotal
		})
		if !progressive {
			return nil
		}
		if _, err := r.admit(ctx); err != nil {
			return err
		}
		if _, err := r.peer.ApplyV2(ctx, r.change); err != nil {
			return refusal(fmt.Errorf("progressive apply: %w", err))
		}
		return nil
	}
	for _, o := range objects {
		if missing[o.Digest] {
			if len(batch) == batchObjectLimit || batchBytes+o.Size > artifact.MaxBatchRaw {
				if err := flush(); err != nil {
					return err
				}
			}
			batch = append(batch, o)
			batchBytes += o.Size
		}
		if !rootSet[o.Digest] {
			continue
		}
		if len(batch) > 0 {
			waiting++
			continue
		}
		r.l.update(func(v *laneLive) { v.progress.RootsComplete++ })
	}
	return flush()
}

func (r *deliveryRun) send(ctx context.Context, objects []artifact.ObjectEntry) error {
	var raw int64
	for _, o := range objects {
		raw += o.Size
	}
	batch, err := r.l.local.BatchBuild(ctx, objects)
	if err != nil {
		return fmt.Errorf("build batch: %w", err)
	}
	sender, err := r.admit(ctx)
	if err != nil {
		return err
	}
	begin, err := r.peer.BatchBegin(ctx, batch, sender)
	if err := r.refused(begin.Peer, err); err != nil {
		return err
	}
	have := make(map[int]bool, len(begin.HaveParts))
	for _, index := range begin.HaveParts {
		have[index] = true
	}
	for index := range batch.Parts {
		if have[index] {
			continue
		}
		local, err := r.admit(ctx)
		if err != nil {
			return err
		}
		data, err := r.l.local.BatchRead(ctx, batch.ID, index)
		if err != nil {
			return fmt.Errorf("read part %d of %s: %w", index, batch.ID, err)
		}
		put, err := r.peer.BatchPut(ctx, batch.ID, index, data, local)
		r.l.update(func(v *laneLive) { v.progress.WireBytesSent += int64(len(data)) })
		if err := r.refused(put.Peer, err); err != nil {
			return err
		}
	}
	if _, err := r.admit(ctx); err != nil {
		return err
	}
	if _, err := r.peer.BatchCommit(ctx, batch.ID); err != nil {
		return fmt.Errorf("commit batch %s: %w", batch.ID, err)
	}
	if err := r.l.local.BatchDrop(ctx, batch.ID); err != nil {
		return fmt.Errorf("drop batch %s: %w", batch.ID, err)
	}
	r.l.update(func(v *laneLive) {
		v.progress.ObjectsSent += int64(len(objects))
		v.progress.BytesSent += raw
	})
	return nil
}

func (r *deliveryRun) refused(peer netpolicy.State, err error) error {
	var refusal *artifact.PausedError
	if errors.As(err, &refusal) {
		r.observePeer(peer)
		return &pauseError{reason: delivery.ReasonForRefusal(refusal), cause: refusal}
	}
	if err != nil {
		return err
	}
	r.observePeer(peer)
	return nil
}

func (r *deliveryRun) apply(ctx context.Context) error {
	r.l.update(func(v *laneLive) { v.state = delivery.StateApplying })
	ack, err := r.applyOnce(ctx)
	if err != nil {
		return err
	}
	if ack.NeedSnapshot {
		full, err := r.s.export(ctx, r.l, r.artifacts, syncservice.NewRevision(0))
		if err != nil {
			return err
		}
		if r.change, err = r.s.replace(ctx, r.l, r.artifacts, r.change.ChangeID, r.change.Artifacts, full); err != nil {
			return err
		}
		if r.artifacts {
			if err := r.transfer(ctx); err != nil {
				return err
			}
			r.l.update(func(v *laneLive) { v.state = delivery.StateApplying })
		}
		if ack, err = r.applyOnce(ctx); err != nil {
			return err
		}
		if ack.NeedSnapshot {
			return fmt.Errorf("delivery: %s refused the full snapshot of %q", r.l.peer, r.l.service)
		}
	}
	switch {
	case ack.Stale:
		if ack.AckedRevision != r.change.SourceRevision || ack.HeldDigest != r.change.PayloadDigest {
			return fmt.Errorf("%w: %s holds %s (%s) against pending %s", errSourceRegressed,
				r.l.peer, ack.AckedRevision, ack.HeldDigest, r.change.SourceRevision)
		}
		ack = syncservice.ApplyResult{AckedRevision: ack.AckedRevision}
	case ack.Partial:
		return errIncomplete
	}
	if err := r.s.store.acknowledge(ctx, r.l.peer, r.change, ack); err != nil {
		return err
	}
	if r.artifacts {
		return r.l.local.PinsSet(ctx, deliveryPinOwnerPrefix+r.l.peer, nil)
	}
	return nil
}

func (r *deliveryRun) applyOnce(ctx context.Context) (syncservice.ApplyResult, error) {
	if !r.artifacts {
		ack, err := r.peer.Apply(ctx, r.change)
		if err != nil {
			return syncservice.ApplyResult{}, fmt.Errorf("apply %q on %s: %w", r.l.service, r.l.peer, err)
		}
		return ack, nil
	}
	if _, err := r.admit(ctx); err != nil {
		return syncservice.ApplyResult{}, err
	}
	ack, err := r.peer.ApplyV2(ctx, r.change)
	if err != nil {
		return syncservice.ApplyResult{}, refusal(fmt.Errorf("apply %q on %s: %w", r.l.service, r.l.peer, err))
	}
	return ack, nil
}

func compareLaneKeys(a, b laneKey) int {
	if c := strings.Compare(a.service, b.service); c != 0 {
		return c
	}
	return strings.Compare(a.peer, b.peer)
}

func tailscaleSnapshot(ctx context.Context, runner hostregistry.Commander, peer string) {
	ctx, cancel := context.WithTimeout(ctx, snapshotTimeout)
	defer cancel()
	line, err := hostregistry.TailscalePeerStatus(ctx, hostregistry.NewExecRunner(runner), peer)
	if err != nil {
		slog.InfoContext(ctx, "delivery: tailscale snapshot unavailable", "peer", peer, "err", err)
		return
	}
	slog.WarnContext(ctx, "delivery: tailscale snapshot", "peer", peer, "status", line)
}
