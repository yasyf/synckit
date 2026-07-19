package meshtrust

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/netip"
	"strings"
)

// backendRunning is the tailscale BackendState in which peer addresses are
// live; any other state serves stale data the trust set must not build on.
const backendRunning = "Running"

// registry is the subset of the mesh host registry the trust set is built
// from: the self target and the registered peer targets, each an ssh-style
// "user@host" identity string.
type registry struct {
	Self  string
	Hosts []string
}

// tsStatus is the subset of `tailscale status --json` the trust set is built
// from. Peer is keyed by node public key; only the values matter. CertDomains
// is null when the tailnet's HTTPS-certificates feature is off.
type tsStatus struct {
	BackendState string
	Self         tsNode
	Peer         map[string]tsNode
	CertDomains  []string
}

// tsNode is one tailnet node: its MagicDNS name (with a trailing dot) and its
// tailnet addresses (one IPv4, one IPv6).
type tsNode struct {
	DNSName      string
	TailscaleIPs []string
}

func parseStatus(b []byte) (tsStatus, error) {
	var st tsStatus
	if err := json.Unmarshal(b, &st); err != nil {
		return tsStatus{}, fmt.Errorf("parse tailscale status: %w", err)
	}
	return st, nil
}

// normalizeHost canonicalizes a DNS name for comparison: lowercased, without
// the trailing dot MagicDNS names carry.
func normalizeHost(s string) string {
	return strings.TrimSuffix(strings.ToLower(s), ".")
}

func firstLabel(host string) string {
	label, _, _ := strings.Cut(host, ".")
	return label
}

// hostPart returns the host portion of an ssh-style "user@host" target — the
// whole string when it carries no "@".
func hostPart(target string) string {
	if i := strings.LastIndex(target, "@"); i >= 0 {
		return target[i+1:]
	}
	return target
}

// build joins registry targets to tailnet nodes by MagicDNS name; the set
// stays fail-closed throughout (see meshtrust-dns-collision note).
func build(reg registry, st tsStatus) (snapshot, error) {
	snap := snapshot{
		self:    reg.Self,
		peers:   make(map[netip.Addr]struct{}),
		origins: make(map[string]struct{}),
	}
	selfAddrs, err := parseAddrs(st.Self.TailscaleIPs)
	if err != nil {
		return snapshot{}, err
	}
	snap.selfAddrs = selfAddrs
	for _, a := range selfAddrs {
		snap.peers[a] = struct{}{}
	}
	selfName := normalizeHost(st.Self.DNSName)
	snap.selfDNS = selfName
	if selfName != "" {
		snap.origins[selfName] = struct{}{}
		if label := firstLabel(selfName); label != "" {
			snap.origins[label] = struct{}{}
		}
	}
	if len(st.CertDomains) > 0 {
		snap.certDomain = normalizeHost(st.CertDomains[0])
		if snap.certDomain != "" {
			snap.origins[snap.certDomain] = struct{}{}
		}
	}
	nodes := make(map[string][]netip.Addr, len(st.Peer))
	ambiguous := make(map[string]struct{})
	for _, n := range st.Peer {
		addrs, err := parseAddrs(n.TailscaleIPs)
		if err != nil {
			return snapshot{}, err
		}
		name := normalizeHost(n.DNSName)
		if name == "" {
			continue
		}
		if _, seen := nodes[name]; seen || name == selfName {
			// Name collision, peer-vs-peer or peer-vs-self: trust neither
			// (meshtrust-dns-collision note).
			ambiguous[name] = struct{}{}
			continue
		}
		nodes[name] = addrs
	}
	for name := range ambiguous {
		delete(nodes, name)
		delete(snap.origins, name)
		if name == snap.selfDNS {
			// A quarantined self name must not surface anywhere: a URL
			// printed with it would be rejected by TrustedOrigin.
			delete(snap.origins, firstLabel(name))
			snap.selfDNS = ""
		}
		if name == snap.certDomain {
			snap.certDomain = ""
		}
		// Fail-closed availability tradeoff: a rogue node claiming a
		// legitimate name denies that name trust until the collision clears.
		slog.Warn("meshtrust: failing closed, DNS name collision quarantines name from trust", "name", name)
	}
	for _, target := range reg.Hosts {
		var addrs []netip.Addr
		if host := normalizeHost(hostPart(target)); host != "" {
			addrs = nodes[host]
		}
		for _, a := range addrs {
			snap.peers[a] = struct{}{}
		}
		snap.hosts = append(snap.hosts, HostTrust{Target: target, Addrs: addrs})
	}
	return snap, nil
}

func parseAddrs(raw []string) ([]netip.Addr, error) {
	addrs := make([]netip.Addr, 0, len(raw))
	for _, s := range raw {
		a, err := netip.ParseAddr(s)
		if err != nil {
			return nil, fmt.Errorf("parse tailnet address %q: %w", s, err)
		}
		addrs = append(addrs, a.Unmap())
	}
	return addrs, nil
}
