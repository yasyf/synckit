package meshtrust

import (
	"net/netip"
	"testing"
)

const statusFixture = `{
  "BackendState": "Running",
  "Self": {
    "DNSName": "yasyf-home.tail71af5d.ts.net.",
    "TailscaleIPs": ["100.88.252.58", "fd7a:115c:a1e0::6d33:fc3c"]
  },
  "Peer": {
    "nodekey:48f402ab9b09": {
      "DNSName": "Yasyf.tail71af5d.ts.net.",
      "TailscaleIPs": ["100.114.101.73", "fd7a:115c:a1e0::d101:654a"]
    },
    "nodekey:2dfbd4aed6d5": {
      "DNSName": "hermes.tail71af5d.ts.net.",
      "TailscaleIPs": ["100.95.192.38", "fd7a:115c:a1e0::a033:c027"]
    }
  }
}`

// stoppedStatusFixture retains stale addresses, exactly what a stopped backend
// reports — the trust set must not build on it.
const stoppedStatusFixture = `{
  "BackendState": "Stopped",
  "Self": {
    "DNSName": "yasyf-home.tail71af5d.ts.net.",
    "TailscaleIPs": ["100.88.252.58"]
  },
  "Peer": {
    "nodekey:48f402ab9b09": {
      "DNSName": "yasyf.tail71af5d.ts.net.",
      "TailscaleIPs": ["100.114.101.73"]
    }
  }
}`

const mixedAddrStatusFixture = `{
  "BackendState": "Running",
  "Self": {
    "DNSName": "yasyf-home.tail71af5d.ts.net.",
    "TailscaleIPs": ["100.88.252.58"]
  },
  "Peer": {
    "nodekey:9a01deadbeef": {
      "DNSName": "broken.tail71af5d.ts.net.",
      "TailscaleIPs": ["100.95.192.38", "not-an-ip"]
    }
  }
}`

// collidingStatusFixture has two distinct tailnet nodes that normalize to the
// same DNS name — the case build() must resolve deterministically rather than
// via Go's randomized map iteration order.
const collidingStatusFixture = `{
  "BackendState": "Running",
  "Self": {
    "DNSName": "yasyf-home.tail71af5d.ts.net.",
    "TailscaleIPs": ["100.88.252.58"]
  },
  "Peer": {
    "nodekey:aaaaaaaaaaaa": {
      "DNSName": "dup.tail71af5d.ts.net.",
      "TailscaleIPs": ["100.100.100.1"]
    },
    "nodekey:bbbbbbbbbbbb": {
      "DNSName": "dup.tail71af5d.ts.net.",
      "TailscaleIPs": ["100.100.100.2"]
    }
  }
}`

// selfCollidingStatusFixture has a peer whose DNS name normalizes to Self's —
// the self-vs-peer collision variant build() must quarantine from peer trust
// and from the DNS-name origins set alike.
const selfCollidingStatusFixture = `{
  "BackendState": "Running",
  "Self": {
    "DNSName": "yasyf-home.tail71af5d.ts.net.",
    "TailscaleIPs": ["100.88.252.58", "fd7a:115c:a1e0::6d33:fc3c"]
  },
  "Peer": {
    "nodekey:cccccccccccc": {
      "DNSName": "Yasyf-Home.tail71af5d.ts.net.",
      "TailscaleIPs": ["100.100.100.3"]
    }
  }
}`

const emptyNameStatusFixture = `{
  "BackendState": "Running",
  "Self": {
    "DNSName": "yasyf-home.tail71af5d.ts.net.",
    "TailscaleIPs": ["100.88.252.58"]
  },
  "Peer": {
    "nodekey:0000feedface": {
      "DNSName": "",
      "TailscaleIPs": ["100.95.192.38"]
    }
  }
}`

func addr(t *testing.T, s string) netip.Addr {
	t.Helper()
	a, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatalf("ParseAddr(%q): %v", s, err)
	}
	return a
}

func TestParseStatus(t *testing.T) {
	st, err := parseStatus([]byte(statusFixture))
	if err != nil {
		t.Fatalf("parseStatus() error: %v", err)
	}
	if got, want := st.BackendState, "Running"; got != want {
		t.Errorf("BackendState = %q, want %q", got, want)
	}
	if got, want := st.Self.DNSName, "yasyf-home.tail71af5d.ts.net."; got != want {
		t.Errorf("Self.DNSName = %q, want %q", got, want)
	}
	if got, want := len(st.Peer), 2; got != want {
		t.Errorf("len(Peer) = %d, want %d", got, want)
	}
	if _, err := parseStatus([]byte(`{"Self": [`)); err == nil {
		t.Error("parseStatus(corrupt) = nil error, want error")
	}
}

func TestNormalizeHost(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"Yasyf.tail71af5d.ts.net.", "yasyf.tail71af5d.ts.net"},
		{"yasyf.tail71af5d.ts.net", "yasyf.tail71af5d.ts.net"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := normalizeHost(tt.in); got != tt.want {
			t.Errorf("normalizeHost(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestHostPart(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"yasyf@yasyf.tail71af5d.ts.net", "yasyf.tail71af5d.ts.net"},
		{"bare-host", "bare-host"},
		{"a@b@c.net", "c.net"},
		{"user@", ""},
	}
	for _, tt := range tests {
		if got := hostPart(tt.in); got != tt.want {
			t.Errorf("hostPart(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestBuild(t *testing.T) {
	st, err := parseStatus([]byte(statusFixture))
	if err != nil {
		t.Fatalf("parseStatus() error: %v", err)
	}
	reg := registry{
		Self:  "yasyf@yasyf-home.tail71af5d.ts.net",
		Hosts: []string{"yasyf@yasyf.tail71af5d.ts.net", "yasyf@nas-local"},
	}
	snap, err := build(reg, st)
	if err != nil {
		t.Fatalf("build() error: %v", err)
	}

	trusted := []string{
		"100.88.252.58", "fd7a:115c:a1e0::6d33:fc3c", // self
		"100.114.101.73", "fd7a:115c:a1e0::d101:654a", // registered peer, case+dot normalized
	}
	for _, s := range trusted {
		if _, ok := snap.peers[addr(t, s)]; !ok {
			t.Errorf("peers missing %s", s)
		}
	}
	if got, want := len(snap.peers), len(trusted); got != want {
		t.Errorf("len(peers) = %d, want %d", got, want)
	}
	if _, ok := snap.peers[addr(t, "100.95.192.38")]; ok {
		t.Error("unregistered tailnet peer (hermes) must not be trusted")
	}
	if _, ok := snap.origins["yasyf-home.tail71af5d.ts.net"]; !ok {
		t.Error("origins missing self MagicDNS name")
	}
	if got, want := len(snap.origins), 1; got != want {
		t.Errorf("len(origins) = %d, want %d", got, want)
	}
	if got, want := len(snap.selfAddrs), 2; got != want {
		t.Errorf("len(selfAddrs) = %d, want %d", got, want)
	}
	if got, want := len(snap.hosts), 2; got != want {
		t.Fatalf("len(hosts) = %d, want %d", got, want)
	}
	if got := snap.hosts[1]; got.Target != "yasyf@nas-local" || len(got.Addrs) != 0 {
		t.Errorf("bare-LAN host = %+v, want no addrs", got)
	}
	if got, want := snap.self, reg.Self; got != want {
		t.Errorf("self = %q, want %q", got, want)
	}
}

func TestBuildAllOrNothing(t *testing.T) {
	st, err := parseStatus([]byte(mixedAddrStatusFixture))
	if err != nil {
		t.Fatalf("parseStatus() error: %v", err)
	}
	// The broken peer is not even registered: one bad address anywhere in the
	// status still fails the whole build.
	reg := registry{Self: "yasyf@yasyf-home.tail71af5d.ts.net"}
	if _, err := build(reg, st); err == nil {
		t.Fatal("build() with an unparseable peer address = nil error, want error")
	}
	stSelf := st
	stSelf.Peer = nil
	stSelf.Self.TailscaleIPs = []string{"not-an-ip"}
	if _, err := build(reg, stSelf); err == nil {
		t.Fatal("build() with an unparseable self address = nil error, want error")
	}
}

func TestBuildEmptyNameGuard(t *testing.T) {
	st, err := parseStatus([]byte(emptyNameStatusFixture))
	if err != nil {
		t.Fatalf("parseStatus() error: %v", err)
	}
	reg := registry{
		Self:  "yasyf@yasyf-home.tail71af5d.ts.net",
		Hosts: []string{"user@"},
	}
	snap, err := build(reg, st)
	if err != nil {
		t.Fatalf("build() error: %v", err)
	}
	if _, ok := snap.peers[addr(t, "100.95.192.38")]; ok {
		t.Error("nameless node's address must not be trusted via an empty-host target")
	}
	if got, want := len(snap.peers), 1; got != want {
		t.Errorf("len(peers) = %d, want %d (self only)", got, want)
	}
	if got, want := len(snap.hosts), 1; got != want || len(snap.hosts[0].Addrs) != 0 {
		t.Errorf("hosts = %+v, want the empty-host target with no addrs", snap.hosts)
	}
}

func TestBuildSelfCollisionQuarantine(t *testing.T) {
	st, err := parseStatus([]byte(selfCollidingStatusFixture))
	if err != nil {
		t.Fatalf("parseStatus() error: %v", err)
	}
	reg := registry{
		Self:  "yasyf@yasyf-home.tail71af5d.ts.net",
		Hosts: []string{"yasyf@yasyf-home.tail71af5d.ts.net"},
	}
	snap, err := build(reg, st)
	if err != nil {
		t.Fatalf("build() error: %v", err)
	}
	if _, ok := snap.peers[addr(t, "100.100.100.3")]; ok {
		t.Error("peer colliding with self's DNS name must not be trusted")
	}
	if got, want := len(snap.peers), 2; got != want {
		t.Errorf("len(peers) = %d, want %d (self addrs only)", got, want)
	}
	if got := snap.hosts[0]; len(got.Addrs) != 0 {
		t.Errorf("colliding host = %+v, want no addrs (trust neither)", got)
	}
	if _, ok := snap.origins["yasyf-home.tail71af5d.ts.net"]; ok {
		t.Error("self DNS-name origin must be quarantined on collision")
	}
	if got, want := len(snap.origins), 0; got != want {
		t.Errorf("len(origins) = %d, want %d", got, want)
	}
	if got, want := len(snap.selfAddrs), 2; got != want {
		t.Errorf("len(selfAddrs) = %d, want %d (self IP origins survive)", got, want)
	}
}

func TestBuildCollisionDeterministic(t *testing.T) {
	st, err := parseStatus([]byte(collidingStatusFixture))
	if err != nil {
		t.Fatalf("parseStatus() error: %v", err)
	}
	reg := registry{
		Self:  "yasyf@yasyf-home.tail71af5d.ts.net",
		Hosts: []string{"user@dup.tail71af5d.ts.net"},
	}
	for i := 0; i < 4096; i++ {
		snap, err := build(reg, st)
		if err != nil {
			t.Fatalf("build() iteration %d error: %v", i, err)
		}
		if got, want := len(snap.hosts), 1; got != want {
			t.Fatalf("iteration %d: len(hosts) = %d, want %d", i, got, want)
		}
		if got := snap.hosts[0]; got.Target != "user@dup.tail71af5d.ts.net" || len(got.Addrs) != 0 {
			t.Fatalf("iteration %d: colliding host = %+v, want no addrs (trust neither)", i, got)
		}
		if _, ok := snap.peers[addr(t, "100.100.100.1")]; ok {
			t.Fatalf("iteration %d: colliding peer address 100.100.100.1 must not be trusted", i)
		}
		if _, ok := snap.peers[addr(t, "100.100.100.2")]; ok {
			t.Fatalf("iteration %d: colliding peer address 100.100.100.2 must not be trusted", i)
		}
		if got, want := len(snap.peers), 1; got != want {
			t.Fatalf("iteration %d: len(peers) = %d, want %d (self only)", i, got, want)
		}
	}
}
