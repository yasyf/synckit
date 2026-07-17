package meshtrust

import (
	"encoding/json"
	"fmt"
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
// from. Peer is keyed by node public key; only the values matter.
type tsStatus struct {
	BackendState string
	Self         tsNode
	Peer         map[string]tsNode
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

// hostPart returns the host portion of an ssh-style "user@host" target — the
// whole string when it carries no "@".
func hostPart(target string) string {
	if i := strings.LastIndex(target, "@"); i >= 0 {
		return target[i+1:]
	}
	return target
}

// build joins registry targets to tailnet nodes by MagicDNS name: trusted peers
// are self's addresses plus matched nodes' addresses; origins are self's name.
// Any unparseable address fails the whole build, and nameless nodes or
// empty-host targets never join — the set stays fail-closed.
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
	if name := normalizeHost(st.Self.DNSName); name != "" {
		snap.origins[name] = struct{}{}
	}
	nodes := make(map[string][]netip.Addr, len(st.Peer))
	for _, n := range st.Peer {
		addrs, err := parseAddrs(n.TailscaleIPs)
		if err != nil {
			return snapshot{}, err
		}
		if name := normalizeHost(n.DNSName); name != "" {
			nodes[name] = addrs
		}
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
