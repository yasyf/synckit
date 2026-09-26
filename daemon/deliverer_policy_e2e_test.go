package daemon

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

func TestE2EMaxWaitIgnoresLaterKicks(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	a := m.hosts["a@node"]
	a.consumer.publish(1, a.put(t, randomBytes(t, 4<<10)))
	d := m.deliver("a@node", "b@node")
	d.await(t, "b@node", "first ack", acked(1))
	artifactMaxWait = 400 * time.Millisecond

	a.consumer.publish(2, a.put(t, randomBytes(t, 4<<10)))
	stop, started := make(chan struct{}), make(chan time.Time, 1)
	var kicker sync.WaitGroup
	kicker.Go(func() {
		ticker := time.NewTicker(40 * time.Millisecond)
		defer ticker.Stop()
		started <- time.Now()
		for {
			if err := d.sched.Kick("stub", "b@node"); err != nil {
				t.Error(err)
				return
			}
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	})
	start := <-started
	d.await(t, "b@node", "ack under continuous kicks", acked(2))
	elapsed := time.Since(start)
	close(stop)
	kicker.Wait()
	if elapsed < artifactMaxWait || elapsed > 3*artifactMaxWait {
		t.Fatalf("ack %s after the first of continuous kicks, want within [%s, %s]", elapsed, artifactMaxWait, 3*artifactMaxWait)
	}
}

func TestE2EEveryPeerCallRechecksPolicy(t *testing.T) {
	expensive := netpolicy.State{Status: netpolicy.StatusConnected, Expensive: true, ObservedAt: time.Now()}
	cellular := netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true, ObservedAt: time.Now()}
	tests := []struct {
		name    string
		trigger string
		host    string
		state   netpolicy.State
		blocked string
		want    delivery.PauseReason
	}{
		{"sender before batch.begin", artifact.MethodHave, "a@node", expensive, artifact.MethodBatchBegin, delivery.PauseLocalExpensive},
		{"sender before batch.put", artifact.MethodBatchBegin, "a@node", expensive, artifact.MethodBatchPut, delivery.PauseLocalExpensive},
		{"sender before batch.commit", artifact.MethodBatchPut, "a@node", expensive, artifact.MethodBatchCommit, delivery.PauseLocalExpensive},
		{"sender before apply.v2", artifact.MethodBatchCommit, "a@node", expensive, syncservice.MethodApplyV2, delivery.PauseLocalExpensive},
		{"receiver have", artifact.MethodHave, "b@node", cellular, artifact.MethodBatchBegin, delivery.PausePeerCellular},
		{"receiver apply.v2", syncservice.MethodApplyV2, "b@node", cellular, "", delivery.PausePeerCellular},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newE2EMesh(t, "a@node", "b@node")
			a, b := m.hosts["a@node"], m.hosts["b@node"]
			a.consumer.publish(1, a.put(t, randomBytes(t, 4<<10)))
			link := m.link("a@node", "b@node")
			var once sync.Once
			link.setHook(func(request *rpc.Request) error {
				if request.Method == tt.trigger {
					once.Do(func() { m.hosts[tt.host].monitor.set(tt.state) })
				}
				return nil
			})
			d := m.deliver("a@node", "b@node")
			d.await(t, "b@node", string(tt.want), pausedWith(tt.want))
			if tt.blocked != "" && link.count(tt.blocked) != 0 {
				t.Fatalf("%s ran %d times after the flip, want 0", tt.blocked, link.count(tt.blocked))
			}
			requireNoApplies(t, b)
		})
	}
}

func TestE2EStalePeerStateIsReprobed(t *testing.T) {
	tests := []struct {
		name      string
		maxAge    time.Duration
		wantProbe int
	}{
		{"fresh peer state is reused", 2 * time.Minute, 1},
		{"stale peer state is reprobed before every peer call", 0, 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newE2EMesh(t, "a@node", "b@node")
			peerStateMaxAge = tt.maxAge
			a := m.hosts["a@node"]
			a.consumer.publish(1, a.put(t, randomBytes(t, 4<<10)))
			d := m.deliver("a@node", "b@node")
			d.await(t, "b@node", "ack", acked(1))
			if got := m.link("a@node", "b@node").count(artifact.MethodNetStatus); got != tt.wantProbe {
				t.Fatalf("net.status ran %d times, want %d", got, tt.wantProbe)
			}
		})
	}

	m := newE2EMesh(t, "a@node", "b@node")
	peerStateMaxAge = 0
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	a.consumer.publish(1, a.put(t, randomBytes(t, 4<<10)))
	link := m.link("a@node", "b@node")
	var probes atomic.Int64
	link.setHook(func(request *rpc.Request) error {
		if request.Method == artifact.MethodNetStatus && probes.Add(1) == 3 {
			b.monitor.set(netpolicy.State{Status: netpolicy.StatusConnected, Cellular: true, ObservedAt: time.Now()})
		}
		return nil
	})
	d := m.deliver("a@node", "b@node")
	d.await(t, "b@node", "peer-cellular from a reprobe", pausedWith(delivery.PausePeerCellular))
	if got := link.count(artifact.MethodBatchBegin); got != 0 {
		t.Fatalf("batch.begin ran %d times after the reprobe saw cellular, want 0", got)
	}
}

func TestE2ESupersedePinsAUnionPastMaxRoots(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	a := m.hosts["a@node"]
	m.link("a@node", "b@node").down.Store(true)
	fabricate := func(prefix string) []artifact.Ref {
		refs := make([]artifact.Ref, artifact.MaxRoots)
		for i := range refs {
			refs[i] = artifact.Ref{Digest: artifact.Sum(fmt.Appendf(nil, "%s %d", prefix, i)), Kind: artifact.KindBlob, Size: 1}
		}
		return refs
	}
	previous, next := fabricate("old"), fabricate("new")
	var widest atomic.Int64
	m.link("a@node", "a@node").setHook(func(request *rpc.Request) error {
		if request.Method != artifact.MethodPinsSet {
			return nil
		}
		roots, _ := request.Params["roots"].([]any)
		for {
			current := widest.Load()
			if int64(len(roots)) <= current || widest.CompareAndSwap(current, int64(len(roots))) {
				return nil
			}
		}
	})
	a.consumer.publish(1, previous...)
	d := m.deliver("a@node", "b@node")
	d.await(t, "b@node", "peer-unreachable pause", pausedWith(delivery.PausePeerUnreachable))
	a.consumer.publish(2, next...)
	if err := d.sched.Kick("stub", "b@node"); err != nil {
		t.Fatal(err)
	}
	status := d.await(t, "b@node", "superseded pending", func(s delivery.PeerStatus) bool {
		return s.Pending != nil && s.Pending.SourceRevision == syncservice.NewRevision(2)
	})
	deadline := time.Now().Add(60 * time.Second)
	for {
		pins, err := a.store.Pins(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(pins) == 1 && pins[0].Owner == "synckit.delivery/b@node" && slices.Equal(pins[0].Roots, next) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("source pins after supersede = %d sets, want only the new roots", len(pins))
		}
		time.Sleep(5 * time.Millisecond)
	}
	if status.Pending.Superseded != 1 || widest.Load() != 2*artifact.MaxRoots {
		t.Fatalf("supersede: %d supersedes, widest pin set %d; want 1 and %d", status.Pending.Superseded, widest.Load(), 2*artifact.MaxRoots)
	}
}

func TestE2ESupersedeMidBacklogDeliversTheNewRootFirst(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	batchObjectLimit = 1
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	backlog := make([]artifact.Ref, 6)
	for i := range backlog {
		backlog[i] = a.put(t, randomBytes(t, 4<<10))
	}
	urgentContent := randomBytes(t, 4<<10)
	urgent := a.put(t, urgentContent)
	a.consumer.publish(1, backlog...)

	link := m.link("a@node", "b@node")
	var mu sync.Mutex
	var begun []artifact.Digest
	running, release := make(chan struct{}), make(chan struct{})
	var commits atomic.Int64
	link.setHook(func(request *rpc.Request) error {
		switch request.Method {
		case artifact.MethodBatchBegin:
			objects := request.Params["batch"].(map[string]any)["objects"].([]any)
			mu.Lock()
			for _, object := range objects {
				begun = append(begun, artifact.Digest(object.(map[string]any)["digest"].(string)))
			}
			mu.Unlock()
		case artifact.MethodBatchCommit:
			if commits.Add(1) == 3 {
				close(running)
				<-release
			}
		}
		return nil
	})
	d := m.deliver("a@node", "b@node")
	<-running
	a.consumer.publish(2, append([]artifact.Ref{urgent}, backlog...)...)
	if err := d.sched.Kick("stub", "b@node"); err != nil {
		t.Fatal(err)
	}
	close(release)
	d.await(t, "b@node", "ack of the superseding change", acked(2))
	b.requireContent(t, urgent, urgentContent)

	mu.Lock()
	defer mu.Unlock()
	seen := map[artifact.Digest]bool{}
	for _, digest := range begun {
		if seen[digest] {
			t.Fatalf("object %s was sent twice; committed objects must be kept", digest)
		}
		seen[digest] = true
	}
	if at, last := slices.Index(begun, urgent.Digest), slices.Index(begun, backlog[len(backlog)-1].Digest); at < 0 || at > last {
		t.Fatalf("urgent root sent at %d, the backlog's last root at %d; want the urgent root first", at, last)
	}
}

func TestE2EApplyReprobeRechecksTheLiveLocalState(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	peerStateMaxAge = 0
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	a.consumer.publish(1, a.put(t, randomBytes(t, 4<<10)))
	link := m.link("a@node", "b@node")
	var committed atomic.Bool
	var once sync.Once
	link.setHook(func(request *rpc.Request) error {
		switch {
		case request.Method == artifact.MethodBatchCommit:
			committed.Store(true)
		case request.Method == artifact.MethodNetStatus && committed.Load():
			once.Do(func() {
				a.monitor.set(netpolicy.State{Status: netpolicy.StatusConnected, Expensive: true, ObservedAt: time.Now()})
			})
		}
		return nil
	})
	d := m.deliver("a@node", "b@node")
	status := d.await(t, "b@node", "pause or ack", func(s delivery.PeerStatus) bool {
		return pausedWith(delivery.PauseLocalExpensive)(s) || acked(1)(s)
	})
	if !pausedWith(delivery.PauseLocalExpensive)(status) {
		t.Fatalf("status = %+v, want a local-expensive pause before apply.v2", status)
	}
	if got := link.count(syncservice.MethodApplyV2); got != 0 {
		t.Fatalf("apply.v2 ran %d times after the reprobe, want 0", got)
	}
	requireNoApplies(t, b)
}

func TestE2EKickSupersedesAtTheNextBatchBoundary(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	batchObjectLimit = 1
	artifactMaxWait = time.Hour
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	backlog := make([]artifact.Ref, 6)
	for i := range backlog {
		backlog[i] = a.put(t, randomBytes(t, 4<<10))
	}
	urgentContent := randomBytes(t, 4<<10)
	urgent := a.put(t, urgentContent)
	urgentClosure, err := a.store.Closure(t.Context(), []artifact.Ref{urgent}, artifact.DefaultClosureBound)
	if err != nil {
		t.Fatal(err)
	}
	a.consumer.publish(1, backlog...)

	link := m.link("a@node", "b@node")
	var mu sync.Mutex
	var begun []artifact.Digest
	running, release := make(chan struct{}), make(chan struct{})
	var commits atomic.Int64
	link.setHook(func(request *rpc.Request) error {
		switch request.Method {
		case artifact.MethodBatchBegin:
			object := request.Params["batch"].(map[string]any)["objects"].([]any)[0]
			mu.Lock()
			begun = append(begun, artifact.Digest(object.(map[string]any)["digest"].(string)))
			mu.Unlock()
		case artifact.MethodBatchCommit:
			if commits.Add(1) == 3 {
				close(running)
				<-release
			}
		}
		return nil
	})
	d := m.deliver("a@node", "b@node")
	<-running
	a.consumer.publish(2, append([]artifact.Ref{urgent}, backlog...)...)
	if err := d.sched.Kick("stub", "b@node"); err != nil {
		t.Fatal(err)
	}
	close(release)
	deadline := time.Now().Add(60 * time.Second)
	var next artifact.Digest
	for next == "" {
		mu.Lock()
		if len(begun) > 3 {
			next = begun[3]
		}
		mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the batch after the kick")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !slices.ContainsFunc(urgentClosure.Objects, func(o artifact.ObjectEntry) bool { return o.Digest == next }) {
		t.Fatalf("batch after the kick carried %s, want an object of the urgent root; the old backlog kept going", next)
	}
	d.await(t, "b@node", "ack of the superseding change", acked(2))
	b.requireContent(t, urgent, urgentContent)
}

func TestE2EDeliversTheChildrenOfADigestReachedAsBothKinds(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	leafContent := randomBytes(t, 4<<10)
	leaf := a.put(t, leafContent)
	inner, err := a.store.PutGroup(t.Context(), "test/group", []artifact.Ref{leaf})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := a.store.Manifest(t.Context(), inner)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := manifest.Encode()
	if err != nil {
		t.Fatal(err)
	}
	file := a.put(t, encoded)
	root, err := a.store.PutGroup(t.Context(), "test/group", []artifact.Ref{file, inner})
	if err != nil {
		t.Fatal(err)
	}
	a.consumer.publish(1, root)
	d := m.deliver("a@node", "b@node")
	d.await(t, "b@node", "ack", acked(1))
	b.requireContent(t, leaf, leafContent)
	b.requireContent(t, file, encoded)
}

func TestE2EKickBeforeEveryFirstBatchStillShipsOne(t *testing.T) {
	m := newE2EMesh(t, "a@node", "b@node")
	artifactMaxWait = time.Hour
	a, b := m.hosts["a@node"], m.hosts["b@node"]
	content := randomBytes(t, 4<<10)
	root := a.put(t, content)
	a.consumer.publish(1, root)
	ready := make(chan struct{})
	var sched atomic.Pointer[deliveryScheduler]
	m.link("a@node", "b@node").setHook(func(request *rpc.Request) error {
		if request.Method != artifact.MethodHave {
			return nil
		}
		<-ready
		return sched.Load().Kick("stub", "b@node")
	})
	d := m.deliver("a@node", "b@node")
	sched.Store(d.sched)
	close(ready)
	d.await(t, "b@node", "ack despite a kick before every first batch", acked(1))
	b.requireContent(t, root, content)
}
