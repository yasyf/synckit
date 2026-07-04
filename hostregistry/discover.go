package hostregistry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/brutella/dnssd"
)

const bonjourTimeout = 3 * time.Second

// SkipNote records a candidate that a scan skipped, and why. Scans surface
// these instead of aborting so one unreadable entry never hides the rest.
type SkipNote struct {
	Name   string // directory entry, node, or host label that was skipped
	Reason string
}

// HostCandidate is one host discovered on the network.
type HostCandidate struct {
	Node          string // tailscale/bonjour node label
	DefaultTarget string // suggested ssh target: tailscale mints "user@host.<tailnet>.ts.net", bonjour "user@node"
	Source        string // "tailscale" or "bonjour"
	Online        bool   // reported reachable by the discovery source
	Registered    bool   // already present in the registry (matched by node label)
}

// HostResult is the outcome of discovering candidate hosts on the network.
type HostResult struct {
	Candidates []HostCandidate
	Notes      []SkipNote
}

// Hosts enumerates candidate hosts on the network from tailscale and Bonjour,
// dedupes them, and marks which are already named in registered. Discovery is
// best-effort: a missing or failing source degrades to a SkipNote rather than an
// error, so Hosts never returns a non-nil error (the deliberate exception to
// fail-fast — there is nothing to fix when a source is simply absent).
func Hosts(ctx context.Context, r Runner, registered []string) (HostResult, error) {
	var notes []SkipNote

	out, err := r.Local(ctx, "id", "-un")
	localUser := strings.TrimSpace(out)
	if err != nil {
		notes = append(notes, SkipNote{Name: "id", Reason: err.Error()})
	}

	tsCands, tsNotes := discoverTailscale(ctx, r, localUser)
	notes = append(notes, tsNotes...)

	localHost, err := r.Local(ctx, "scutil", "--get", "LocalHostName")
	localHost = strings.TrimSpace(localHost)
	if err != nil {
		notes = append(notes, SkipNote{Name: "scutil", Reason: err.Error()})
	}

	bjCands, bjNotes := discoverBonjour(ctx, localUser, localHost)
	notes = append(notes, bjNotes...)

	cands := make([]HostCandidate, 0, len(tsCands)+len(bjCands))
	cands = append(cands, tsCands...)
	cands = append(cands, bjCands...)

	return HostResult{Candidates: mergeHosts(cands, registered), Notes: notes}, nil
}

// discoverTailscale enumerates online and offline peers from `tailscale status
// --json`. A failure to run or parse tailscale degrades to a single SkipNote.
func discoverTailscale(ctx context.Context, r Runner, localUser string) ([]HostCandidate, []SkipNote) {
	out, err := r.Local(ctx, "tailscale", "status", "--json")
	if err != nil {
		return nil, []SkipNote{{Name: "tailscale", Reason: err.Error()}}
	}
	var status struct {
		Peer map[string]tailscalePeer
	}
	if err := json.Unmarshal([]byte(out), &status); err != nil {
		return nil, []SkipNote{{Name: "tailscale", Reason: err.Error()}}
	}

	var cands []HostCandidate
	for _, p := range status.Peer {
		node := TailscaleNode(p.DNSName)
		if node == "" {
			continue
		}
		if isMullvad(p) {
			continue
		}
		if isLikelyEphemeral(p) {
			continue
		}
		cands = append(cands, HostCandidate{
			Node:          node,
			DefaultTarget: target(localUser, strings.TrimSuffix(p.DNSName, ".")),
			Source:        "tailscale",
			Online:        p.Online,
		})
	}
	return cands, nil
}

// TailscalePeerStatus returns a one-line tailnet view of the peer behind target (a
// "user@node" ssh target): whether it is online, whether the connection is direct
// or DERP-relayed and via which endpoint or region, and when it was last seen. It
// runs `tailscale status --json` through r and matches the peer by short node label,
// erroring when tailscale is unreachable or the node is not in the tailnet.
func TailscalePeerStatus(ctx context.Context, r Runner, target string) (string, error) {
	out, err := r.Local(ctx, "tailscale", "status", "--json")
	if err != nil {
		return "", fmt.Errorf("tailscale status: %w", err)
	}
	var status struct {
		Peer map[string]tailscalePeer
	}
	if err := json.Unmarshal([]byte(out), &status); err != nil {
		return "", fmt.Errorf("parse tailscale status: %w", err)
	}
	node := HostNode(target)
	for _, p := range status.Peer {
		if strings.EqualFold(TailscaleNode(p.DNSName), node) {
			return formatTailscalePeer(p), nil
		}
	}
	return "", fmt.Errorf("no tailscale peer for node %q", node)
}

// formatTailscalePeer renders a peer as a single status line: direct connections
// name their current endpoint, relayed ones their DERP region, and an absent
// last-seen renders as "never".
func formatTailscalePeer(p tailscalePeer) string {
	conn := "derp " + p.Relay
	if p.CurAddr != "" {
		conn = "direct " + p.CurAddr
	}
	lastseen := "never"
	if p.LastSeen != nil {
		lastseen = p.LastSeen.UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf("online=%t conn=%s lastseen=%s", p.Online, conn, lastseen)
}

// tailscalePeer is the subset of a `tailscale status --json` peer that discovery
// and the peer snapshot decode. Location is a pointer so a JSON null (a personal
// node) is distinguishable from a populated object (a Mullvad relay); KeyExpiry is
// a pointer so a null/absent expiry is distinguishable from a real one; LastSeen is
// a pointer so an absent last-seen is distinguishable from the zero time. CurAddr is
// the active direct endpoint when the connection is not relayed, Relay the home DERP
// region otherwise.
type tailscalePeer struct {
	DNSName   string
	HostName  string
	Online    bool
	OS        string
	Tags      []string
	Relay     string
	CurAddr   string
	KeyExpiry *time.Time
	LastSeen  *time.Time
	Location  *struct{}
}

// isMullvad reports whether p is a Mullvad relay rather than a personal node.
// Mullvad peers carry a populated Location and the mullvad-exit-node tag, while
// personal nodes report Location=null and no such tag; either signal alone marks
// a relay, so they are OR'd.
func isMullvad(p tailscalePeer) bool {
	if p.Location != nil {
		return true
	}
	for _, t := range p.Tags {
		if t == "tag:mullvad-exit-node" {
			return true
		}
	}
	return false
}

// isLikelyEphemeral best-effort flags an ephemeral peer. `tailscale status
// --json` carries no Ephemeral field, so a nil KeyExpiry is the only hint, and it
// is not a clean signal (a personal node with key expiry disabled reports it too);
// the guard therefore fires only on non-Mullvad peers.
func isLikelyEphemeral(p tailscalePeer) bool {
	return !isMullvad(p) && p.KeyExpiry == nil
}

// discoverBonjour browses _ssh._tcp services for up to bonjourTimeout, skipping
// this Mac itself (the browse entry whose node matches localHostName). The lookup
// blocks until the context expires, so a deadline or cancellation is normal
// completion; any other error degrades to a SkipNote.
func discoverBonjour(ctx context.Context, localUser, localHostName string) ([]HostCandidate, []SkipNote) {
	ctx2, cancel := context.WithTimeout(ctx, bonjourTimeout)
	defer cancel()

	var nodes []string
	add := func(e dnssd.BrowseEntry) {
		nodes = append(nodes, bonjourNode(e))
	}
	rmv := func(dnssd.BrowseEntry) {}

	err := dnssd.LookupType(ctx2, "_ssh._tcp.local.", add, rmv)
	cands, notes := bonjourCandidates(nodes, localUser, localHostName)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return cands, append(notes, SkipNote{Name: "bonjour", Reason: err.Error()})
	}
	return cands, notes
}

// bonjourCandidates dedupes browsed node labels into candidates, dropping any
// whose node matches localHostName (case-insensitive — bonjour casing is
// inconsistent, e.g. "yBook-Pro" vs "ybook-pro") and recording a SkipNote for the
// self entry. Factored out of discoverBonjour because dnssd.LookupType is not
// mockable, so the self-skip is exercised through this helper.
func bonjourCandidates(nodes []string, localUser, localHostName string) ([]HostCandidate, []SkipNote) {
	var (
		cands    []HostCandidate
		notes    []SkipNote
		seenSelf bool
	)
	seen := map[string]struct{}{}
	for _, node := range nodes {
		if node == "" {
			continue
		}
		if strings.EqualFold(node, localHostName) {
			if !seenSelf {
				notes = append(notes, SkipNote{Name: node, Reason: "self"})
				seenSelf = true
			}
			continue
		}
		if _, dup := seen[node]; dup {
			continue
		}
		seen[node] = struct{}{}
		cands = append(cands, HostCandidate{
			Node:          node,
			DefaultTarget: target(localUser, node),
			Source:        "bonjour",
			Online:        true,
		})
	}
	return cands, notes
}

// mergeHosts dedupes candidates by node — preferring a tailscale entry over a
// bonjour one for the same node — marks each as Registered when registered
// already names that node, and returns them sorted by node.
func mergeHosts(cands []HostCandidate, registered []string) []HostCandidate {
	regNodes := map[string]struct{}{}
	for _, h := range registered {
		regNodes[HostNode(h)] = struct{}{}
	}

	byNode := map[string]HostCandidate{}
	for _, c := range cands {
		existing, ok := byNode[c.Node]
		if ok && (c.Source != "tailscale" || existing.Source != "bonjour") {
			continue
		}
		byNode[c.Node] = c
	}

	merged := make([]HostCandidate, 0, len(byNode))
	for _, c := range byNode {
		_, c.Registered = regNodes[c.Node]
		merged = append(merged, c)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Node < merged[j].Node
	})
	return merged
}

// bonjourNode derives a node label from a browse entry's host, falling back to
// its instance name, stripping a trailing ".local." and any remaining domain.
func bonjourNode(e dnssd.BrowseEntry) string {
	raw := e.Host
	if raw == "" {
		raw = e.Name
	}
	raw = strings.TrimSuffix(raw, ".")
	raw = strings.TrimSuffix(raw, ".local")
	label, _, _ := strings.Cut(raw, ".")
	return label
}

// HostNode returns the short node label of a "user@host" target: the first DNS
// label of the host portion, i.e. the text after the last '@' up to the first '.'.
// So "yasyf@yasyf.tail71af5d.ts.net" yields "yasyf", and a bare short name like
// "yasyf" passes through unchanged.
func HostNode(target string) string {
	host := target
	if i := strings.LastIndex(target, "@"); i >= 0 {
		host = target[i+1:]
	}
	label, _, _ := strings.Cut(host, ".")
	return label
}

// target builds the suggested ssh target, degrading to the bare node when the
// local user is unknown.
func target(localUser, node string) string {
	if localUser == "" {
		return node
	}
	return localUser + "@" + node
}
