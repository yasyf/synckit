// Package meshtrust derives a network-trust set from the shared host mesh
// (hostregistry.Mesh): every machine registered in the mesh is trusted by its
// tailnet addresses, resolved via `tailscale status`. A consuming daemon
// answers per-request trust verdicts through the Provider's TrustedPeer and
// TrustedOrigin methods, whose shapes match cc-interact's daemon.Config hooks.
package meshtrust

import (
	"context"
	"log/slog"
	"net/netip"
	"os"
	"slices"
	"sync"
	"time"
)

const (
	ttl            = 30 * time.Second
	refreshTimeout = 5 * time.Second
)

// snapshot is one immutable resolution of the trust set.
type snapshot struct {
	self       string
	selfDNS    string
	certDomain string
	hosts      []HostTrust
	peers      map[netip.Addr]struct{}
	origins    map[string]struct{}
	selfAddrs  []netip.Addr
	fetched    time.Time
}

// HostTrust is one registered mesh target and the tailnet addresses it resolved
// to — empty when the target names no tailnet node.
type HostTrust struct {
	Target string
	Addrs  []netip.Addr
}

// Mesh is the inspector's view of the current trust set.
type Mesh struct {
	Self  string
	Hosts []HostTrust
}

// Provider answers per-request trust verdicts from a cached snapshot of the
// mesh registry joined with live tailnet addresses. The snapshot refreshes
// at most every 30s, blocking the asking request; any failure to read the
// registry or tailscale yields an empty (fail-closed) set until the next
// refresh window.
type Provider struct {
	mu   sync.Mutex
	snap snapshot

	loadRegistry func() (registry, error)
	status       func(ctx context.Context) ([]byte, error)
	now          func() time.Time
}

// NewProvider returns a Provider backed by the shared mesh state and the
// tailscale CLI.
func NewProvider() *Provider {
	return &Provider{
		loadRegistry: loadRegistry,
		status:       tailscaleStatus,
		now:          time.Now,
	}
}

// Detect returns a Provider when the mesh state exists on this machine, nil
// otherwise — a daemon enables tailnet trust exactly when the mesh is in use,
// with no configuration.
func Detect() *Provider {
	path, err := StatePath()
	if err != nil {
		slog.Warn("meshtrust: cannot resolve mesh state path", "err", err)
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	return NewProvider()
}

// TrustedPeer reports whether ip belongs to a machine in the mesh (including
// this one). It matches cc-interact's TrustedPeer hook; the hook signature
// carries no context, so refreshes run under the Provider's own timeout.
func (p *Provider) TrustedPeer(ip netip.Addr) bool {
	_, ok := p.current(context.Background()).peers[ip.Unmap()]
	return ok
}

// TrustedOrigin reports whether host names this machine's own tailnet identity:
// its MagicDNS name or one of its tailnet addresses. It matches cc-interact's
// TrustedOrigin hook and never approves peer names.
func (p *Provider) TrustedOrigin(host string) bool {
	snap := p.current(context.Background())
	if a, err := netip.ParseAddr(host); err == nil {
		a = a.Unmap()
		for _, self := range snap.selfAddrs {
			if a == self {
				return true
			}
		}
		return false
	}
	_, ok := snap.origins[normalizeHost(host)]
	return ok
}

// SelfAddrs returns this machine's own tailnet addresses, empty when tailscale
// is unavailable.
func (p *Provider) SelfAddrs(ctx context.Context) []netip.Addr {
	return slices.Clone(p.current(ctx).selfAddrs)
}

// SelfDNSName returns this machine's MagicDNS name without a trailing dot,
// empty when tailscale is unavailable.
func (p *Provider) SelfDNSName(ctx context.Context) string {
	return p.current(ctx).selfDNS
}

// SelfHostLabel returns the bare MagicDNS machine label, suitable as the host
// of a plaintext-http URL (a bare label escapes the ts.net HSTS preload); empty
// when tailscale is unavailable or the self name is quarantined by a DNS
// collision.
func (p *Provider) SelfHostLabel(ctx context.Context) string {
	return firstLabel(p.current(ctx).selfDNS)
}

// SelfCertDomain returns the tailnet cert domain naming this machine, empty
// when tailscale is unavailable or the tailnet's HTTPS-certificates feature
// is off.
func (p *Provider) SelfCertDomain(ctx context.Context) string {
	return p.current(ctx).certDomain
}

// Mesh returns the inspector's view of the current trust set.
func (p *Provider) Mesh(ctx context.Context) Mesh {
	snap := p.current(ctx)
	hosts := make([]HostTrust, len(snap.hosts))
	for i, h := range snap.hosts {
		hosts[i] = HostTrust{Target: h.Target, Addrs: slices.Clone(h.Addrs)}
	}
	return Mesh{Self: snap.self, Hosts: hosts}
}

// current returns a fresh snapshot, refreshing under the lock when the TTL has
// lapsed. The mutex makes concurrent refreshes single-flight: every caller
// blocks on the one in-flight resolution and answers from its result. A refresh
// under a dead caller context is never cached — one canceled caller must not
// poison the shared snapshot for a TTL.
func (p *Provider) current(ctx context.Context) snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if !p.snap.fetched.IsZero() && now.Sub(p.snap.fetched) < ttl {
		return p.snap
	}
	snap := p.refresh(ctx)
	if ctx.Err() != nil {
		return snap
	}
	snap.fetched = now
	p.snap = snap
	return p.snap
}

// refresh resolves a new snapshot, failing closed to the empty set on any
// registry or tailscale error, and on a registry naming no self identity (an
// unconfigured mesh authorizes nobody, this machine included).
func (p *Provider) refresh(parent context.Context) snapshot {
	ctx, cancel := context.WithTimeout(parent, refreshTimeout)
	defer cancel()
	reg, err := p.loadRegistry()
	if err != nil {
		slog.Warn("meshtrust: failing closed, cannot load mesh registry", "err", err)
		return snapshot{}
	}
	if reg.Self == "" {
		slog.Warn("meshtrust: failing closed, mesh registry names no self identity")
		return snapshot{}
	}
	raw, err := p.status(ctx)
	if err != nil {
		slog.Warn("meshtrust: failing closed, cannot read tailscale status", "err", err)
		return snapshot{}
	}
	st, err := parseStatus(raw)
	if err != nil {
		slog.Warn("meshtrust: failing closed, cannot parse tailscale status", "err", err)
		return snapshot{}
	}
	if st.BackendState != backendRunning {
		slog.Warn("meshtrust: failing closed, tailscale backend not running", "state", st.BackendState)
		return snapshot{}
	}
	snap, err := build(reg, st)
	if err != nil {
		slog.Warn("meshtrust: failing closed, invalid tailscale status", "err", err)
		return snapshot{}
	}
	return snap
}
