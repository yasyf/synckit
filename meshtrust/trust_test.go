package meshtrust

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// testClock is a fake clock the provider reads; Advance moves it.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type testSources struct {
	mu       sync.Mutex
	reg      registry
	regErr   error
	status   []byte
	statErr  error
	regCalls int
}

func (s *testSources) loadRegistry() (registry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.regCalls++
	return s.reg, s.regErr
}

func (s *testSources) tailscaleStatus(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status, s.statErr
}

func (s *testSources) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.regCalls
}

func (s *testSources) set(fn func(*testSources)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s)
}

func newTestProvider(src *testSources, clock *testClock) *Provider {
	return &Provider{
		loadRegistry: src.loadRegistry,
		status:       src.tailscaleStatus,
		now:          clock.Now,
	}
}

func meshSources() *testSources {
	return &testSources{
		reg: registry{
			Self:  "yasyf@yasyf-home.tail71af5d.ts.net",
			Hosts: []string{"yasyf@yasyf.tail71af5d.ts.net"},
		},
		status: []byte(statusFixture),
	}
}

func TestProviderTTL(t *testing.T) {
	src := meshSources()
	clock := &testClock{now: time.Unix(1000, 0)}
	p := newTestProvider(src, clock)

	if !p.TrustedPeer(addr(t, "100.114.101.73")) {
		t.Fatal("registered peer not trusted on first resolution")
	}
	if !p.TrustedPeer(addr(t, "::ffff:100.114.101.73")) {
		t.Error("v4-mapped input must unmap before the lookup")
	}
	if got, want := src.calls(), 1; got != want {
		t.Fatalf("loader calls = %d, want %d", got, want)
	}
	if !p.TrustedPeer(addr(t, "100.114.101.73")) || src.calls() != 1 {
		t.Error("second verdict within TTL must answer from the snapshot")
	}
	clock.Advance(ttl + time.Second)
	if !p.TrustedPeer(addr(t, "100.114.101.73")) {
		t.Error("peer not trusted after refresh")
	}
	if got, want := src.calls(), 2; got != want {
		t.Errorf("loader calls after TTL = %d, want %d", got, want)
	}
}

func TestProviderFreshness(t *testing.T) {
	src := meshSources()
	clock := &testClock{now: time.Unix(1000, 0)}
	p := newTestProvider(src, clock)

	hermes := addr(t, "100.95.192.38")
	if p.TrustedPeer(hermes) {
		t.Fatal("unregistered peer trusted")
	}
	src.set(func(s *testSources) {
		s.reg.Hosts = append(s.reg.Hosts, "yasyf@hermes.tail71af5d.ts.net")
	})
	clock.Advance(ttl + time.Second)
	if !p.TrustedPeer(hermes) {
		t.Error("host added to the registry not trusted after TTL")
	}
	src.set(func(s *testSources) { s.reg.Hosts = nil })
	clock.Advance(ttl + time.Second)
	if p.TrustedPeer(hermes) || p.TrustedPeer(addr(t, "100.114.101.73")) {
		t.Error("removed hosts still trusted after TTL")
	}
}

func TestProviderFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testSources)
	}{
		{"registry error", func(s *testSources) { s.regErr = errors.New("boom") }},
		{"registry without self", func(s *testSources) { s.reg = registry{Hosts: s.reg.Hosts} }},
		{"status error", func(s *testSources) { s.statErr = errors.New("boom") }},
		{"corrupt status", func(s *testSources) { s.status = []byte("{") }},
		{"backend stopped", func(s *testSources) { s.status = []byte(stoppedStatusFixture) }},
		{"unparseable address", func(s *testSources) { s.status = []byte(mixedAddrStatusFixture) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := meshSources()
			clock := &testClock{now: time.Unix(1000, 0)}
			p := newTestProvider(src, clock)
			if !p.TrustedPeer(addr(t, "100.114.101.73")) {
				t.Fatal("peer not trusted before breakage")
			}
			src.set(tt.mutate)
			clock.Advance(ttl + time.Second)
			if p.TrustedPeer(addr(t, "100.114.101.73")) {
				t.Error("previously-trusted peer must fail closed")
			}
			if p.TrustedOrigin("yasyf-home.tail71af5d.ts.net") {
				t.Error("origin must fail closed")
			}
			calls := src.calls()
			p.TrustedPeer(addr(t, "100.114.101.73"))
			if src.calls() != calls {
				t.Error("failed refresh must not retry before the TTL lapses")
			}
		})
	}
}

func TestProviderCanceledContextNotCached(t *testing.T) {
	src := meshSources()
	clock := &testClock{now: time.Unix(1000, 0)}
	p := newTestProvider(src, clock)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := p.SelfAddrs(ctx); len(got) != 0 {
		t.Fatalf("SelfAddrs(canceled ctx) = %v, want empty (fail-closed)", got)
	}
	if !p.TrustedPeer(addr(t, "100.114.101.73")) {
		t.Error("a canceled caller's failed refresh must not be cached for the TTL")
	}
	if got, want := src.calls(), 2; got != want {
		t.Errorf("loader calls = %d, want %d (canceled refresh uncached, live one cached)", got, want)
	}
}

func TestTrustedOrigin(t *testing.T) {
	src := meshSources()
	clock := &testClock{now: time.Unix(1000, 0)}
	p := newTestProvider(src, clock)

	tests := []struct {
		host string
		want bool
	}{
		{"yasyf-home.tail71af5d.ts.net", true},
		{"YASYF-HOME.tail71af5d.ts.net.", true},
		{"100.88.252.58", true},
		{"fd7a:115c:a1e0::6d33:fc3c", true},
		{"FD7A:115C:A1E0:0:0:0:6D33:FC3C", true},
		{"yasyf.tail71af5d.ts.net", false},
		{"100.114.101.73", false},
		{"evil.example", false},
	}
	for _, tt := range tests {
		if got := p.TrustedOrigin(tt.host); got != tt.want {
			t.Errorf("TrustedOrigin(%q) = %v, want %v", tt.host, got, tt.want)
		}
	}
}

func TestTrustedOriginSelfCollision(t *testing.T) {
	src := meshSources()
	src.set(func(s *testSources) { s.status = []byte(selfCollidingStatusFixture) })
	clock := &testClock{now: time.Unix(1000, 0)}
	p := newTestProvider(src, clock)

	if p.TrustedPeer(addr(t, "100.100.100.3")) {
		t.Error("peer colliding with self's DNS name must not be trusted")
	}
	if p.TrustedOrigin("yasyf-home.tail71af5d.ts.net") {
		t.Error("self DNS-name origin must be quarantined on collision")
	}
	if !p.TrustedOrigin("100.88.252.58") || !p.TrustedOrigin("fd7a:115c:a1e0::6d33:fc3c") {
		t.Error("self IP origins must survive a DNS-name collision")
	}
	if !p.TrustedPeer(addr(t, "100.88.252.58")) {
		t.Error("self addrs must remain trusted peers")
	}
}

func TestProviderConcurrent(t *testing.T) {
	src := meshSources()
	clock := &testClock{now: time.Unix(1000, 0)}
	p := newTestProvider(src, clock)

	peer := addr(t, "100.114.101.73")
	verdicts := make([]bool, 16)
	var wg sync.WaitGroup
	for i := range verdicts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			verdicts[i] = p.TrustedPeer(peer) &&
				p.TrustedOrigin("yasyf-home.tail71af5d.ts.net") &&
				len(p.SelfAddrs(context.Background())) == 2
		}()
	}
	wg.Wait()
	for i, ok := range verdicts {
		if !ok {
			t.Errorf("goroutine %d saw a wrong verdict", i)
		}
	}
	if got, want := src.calls(), 1; got != want {
		t.Errorf("loader calls = %d, want %d (single-flight refresh)", got, want)
	}
}

func TestMesh(t *testing.T) {
	src := meshSources()
	clock := &testClock{now: time.Unix(1000, 0)}
	p := newTestProvider(src, clock)

	m := p.Mesh(context.Background())
	if got, want := m.Self, "yasyf@yasyf-home.tail71af5d.ts.net"; got != want {
		t.Errorf("Self = %q, want %q", got, want)
	}
	if got, want := fmt.Sprint(m.Hosts), "[{yasyf@yasyf.tail71af5d.ts.net [100.114.101.73 fd7a:115c:a1e0::d101:654a]}]"; got != want {
		t.Errorf("Hosts = %s, want %s", got, want)
	}

	m.Hosts[0].Addrs[0] = addr(t, "10.0.0.1")
	if got := p.Mesh(context.Background()).Hosts[0].Addrs[0]; got != addr(t, "100.114.101.73") {
		t.Error("mutating a returned Mesh must not corrupt the snapshot")
	}
	selfs := p.SelfAddrs(context.Background())
	selfs[0] = addr(t, "10.0.0.1")
	if got := p.SelfAddrs(context.Background())[0]; got != addr(t, "100.88.252.58") {
		t.Error("mutating returned SelfAddrs must not corrupt the snapshot")
	}
}
