package hostregistry

import (
	"context"
	"errors"
	"strings"
	"testing"
)

const tailscaleStatusJSON = `{
  "Self": {"DNSName": "self.tailnet.ts.net.", "HostName": "self", "Online": true},
  "Peer": {
    "key-alpha": {"DNSName": "alpha.tailnet.ts.net.", "HostName": "alpha", "Online": true,  "OS": "linux", "KeyExpiry": "2027-01-01T00:00:00Z"},
    "key-beta":  {"DNSName": "beta.tailnet.ts.net.",  "HostName": "beta",  "Online": false, "OS": "macOS", "KeyExpiry": "2027-01-01T00:00:00Z"},
    "key-mullvad": {"DNSName": "ca-tor-wg-204.mullvad.ts.net.", "HostName": "ca-tor-wg-204", "Online": true, "OS": "linux", "Tags": ["tag:mullvad-exit-node"], "KeyExpiry": null, "Location": {"Country": "Canada", "CountryCode": "ca", "City": "Toronto", "CityCode": "tor"}},
    "key-ephemeral": {"DNSName": "ephemeral.tailnet.ts.net.", "HostName": "ephemeral", "Online": true, "OS": "linux", "KeyExpiry": null}
  }
}`

func candidateByNode(t *testing.T, cands []HostCandidate, node string) HostCandidate {
	t.Helper()
	for _, c := range cands {
		if c.Node == node {
			return c
		}
	}
	t.Fatalf("no candidate for node %q in %+v", node, cands)
	return HostCandidate{}
}

func hasNode(cands []HostCandidate, node string) bool {
	for _, c := range cands {
		if c.Node == node {
			return true
		}
	}
	return false
}

func TestDiscoverTailscale(t *testing.T) {
	r := NewMockRunner().OnLocal("tailscale status --json", tailscaleStatusJSON, nil)

	cands, notes := discoverTailscale(context.Background(), r, "yasyf")
	if len(notes) != 0 {
		t.Fatalf("notes = %+v, want none", notes)
	}
	if len(cands) != 2 {
		t.Fatalf("got %d candidates, want 2: %+v", len(cands), cands)
	}

	alpha := candidateByNode(t, cands, "alpha")
	if alpha.DefaultTarget != "yasyf@alpha.tailnet.ts.net" {
		t.Fatalf("alpha target = %q, want yasyf@alpha.tailnet.ts.net", alpha.DefaultTarget)
	}
	if alpha.Source != "tailscale" {
		t.Fatalf("alpha source = %q, want tailscale", alpha.Source)
	}
	if !alpha.Online {
		t.Fatal("alpha Online = false, want true")
	}

	beta := candidateByNode(t, cands, "beta")
	if beta.DefaultTarget != "yasyf@beta.tailnet.ts.net" {
		t.Fatalf("beta target = %q, want yasyf@beta.tailnet.ts.net", beta.DefaultTarget)
	}
	if beta.Online {
		t.Fatal("beta Online = true, want false")
	}
}

func TestDiscoverTailscaleDropsMullvad(t *testing.T) {
	r := NewMockRunner().OnLocal("tailscale status --json", tailscaleStatusJSON, nil)

	cands, notes := discoverTailscale(context.Background(), r, "yasyf")
	if len(notes) != 0 {
		t.Fatalf("notes = %+v, want none", notes)
	}
	if hasNode(cands, "ca-tor-wg-204") {
		t.Fatalf("mullvad exit node must be dropped, got %+v", cands)
	}
}

func TestDiscoverTailscaleDropsEphemeral(t *testing.T) {
	r := NewMockRunner().OnLocal("tailscale status --json", tailscaleStatusJSON, nil)

	cands, notes := discoverTailscale(context.Background(), r, "yasyf")
	if len(notes) != 0 {
		t.Fatalf("notes = %+v, want none", notes)
	}
	if hasNode(cands, "ephemeral") {
		t.Fatalf("ephemeral node must be dropped, got %+v", cands)
	}
}

func TestDiscoverTailscaleNoUserDegradesTarget(t *testing.T) {
	r := NewMockRunner().OnLocal("tailscale status --json", tailscaleStatusJSON, nil)

	cands, notes := discoverTailscale(context.Background(), r, "")
	if len(notes) != 0 {
		t.Fatalf("notes = %+v, want none", notes)
	}
	alpha := candidateByNode(t, cands, "alpha")
	if alpha.DefaultTarget != "alpha.tailnet.ts.net" {
		t.Fatalf("alpha target = %q, want bare FQDN alpha.tailnet.ts.net when user unknown", alpha.DefaultTarget)
	}
}

func TestDiscoverTailscaleErrorDegrades(t *testing.T) {
	r := NewMockRunner().OnLocal("tailscale status --json", "", errors.New("exec: tailscale: not found"))

	cands, notes := discoverTailscale(context.Background(), r, "yasyf")
	if len(cands) != 0 {
		t.Fatalf("got %d candidates, want 0 when tailscale errors", len(cands))
	}
	if len(notes) != 1 {
		t.Fatalf("got %d notes, want exactly 1: %+v", len(notes), notes)
	}
	if notes[0].Name != "tailscale" {
		t.Fatalf("note name = %q, want tailscale", notes[0].Name)
	}
	if !strings.Contains(notes[0].Reason, "not found") {
		t.Fatalf("note reason = %q, want the runner error text", notes[0].Reason)
	}
}

func TestDiscoverTailscaleBadJSONDegrades(t *testing.T) {
	r := NewMockRunner().OnLocal("tailscale status --json", "{not json", nil)

	cands, notes := discoverTailscale(context.Background(), r, "yasyf")
	if len(cands) != 0 {
		t.Fatalf("got %d candidates, want 0 on parse failure", len(cands))
	}
	if len(notes) != 1 || notes[0].Name != "tailscale" {
		t.Fatalf("want a single tailscale SkipNote, got %+v", notes)
	}
}

func TestMergeHostsRegisteredAndDedupe(t *testing.T) {
	cands := []HostCandidate{
		// alpha discovered by both sources: tailscale must win.
		{Node: "alpha", DefaultTarget: "yasyf@alpha", Source: "bonjour", Online: true},
		{Node: "alpha", DefaultTarget: "yasyf@alpha", Source: "tailscale", Online: true},
		{Node: "beta", DefaultTarget: "yasyf@beta", Source: "tailscale", Online: false},
		{Node: "gamma", DefaultTarget: "yasyf@gamma", Source: "bonjour", Online: true},
	}
	registered := []string{"yasyf@alpha", "beta"}

	merged := mergeHosts(cands, registered)

	if len(merged) != 3 {
		t.Fatalf("got %d merged candidates, want 3 (alpha deduped): %+v", len(merged), merged)
	}
	if got := []string{merged[0].Node, merged[1].Node, merged[2].Node}; strings.Join(got, ",") != "alpha,beta,gamma" {
		t.Fatalf("merge order = %v, want alpha,beta,gamma", got)
	}

	alpha := candidateByNode(t, merged, "alpha")
	if alpha.Source != "tailscale" {
		t.Fatalf("alpha source = %q, want tailscale to win over bonjour", alpha.Source)
	}
	if !alpha.Registered {
		t.Fatal("alpha Registered = false, want true (matched user@node)")
	}

	beta := candidateByNode(t, merged, "beta")
	if !beta.Registered {
		t.Fatal("beta Registered = false, want true (matched bare node)")
	}

	gamma := candidateByNode(t, merged, "gamma")
	if gamma.Registered {
		t.Fatal("gamma Registered = true, want false (undiscovered in state)")
	}
}

func TestMergeHostsNoRegisteredHosts(t *testing.T) {
	cands := []HostCandidate{
		{Node: "alpha", Source: "tailscale"},
		{Node: "beta", Source: "tailscale"},
	}

	merged := mergeHosts(cands, nil)
	for _, c := range merged {
		if c.Registered {
			t.Fatalf("node %q Registered = true with no registered hosts", c.Node)
		}
	}
}

func TestHostNode(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"yasyf@alpha", "alpha"},
		{"alpha", "alpha"},
		{"user@host@weird", "weird"},
		{"yasyf@alpha.tailnet.ts.net", "alpha"},
		{"alpha.tailnet.ts.net", "alpha"},
	}
	for _, tc := range cases {
		if got := HostNode(tc.in); got != tc.want {
			t.Fatalf("HostNode(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestMergeHostsRegisteredByFQDN proves an FQDN registration marks its short-label
// candidate Registered, and a mesh mixing a legacy short name with an FQDN matches
// both — discovery mints short node labels, so matching cuts the registered target
// at its first dot.
func TestMergeHostsRegisteredByFQDN(t *testing.T) {
	cands := []HostCandidate{
		{Node: "alpha", DefaultTarget: "yasyf@alpha.tailnet.ts.net", Source: "tailscale", Online: true},
		{Node: "beta", DefaultTarget: "yasyf@beta.tailnet.ts.net", Source: "tailscale", Online: true},
		{Node: "gamma", DefaultTarget: "yasyf@gamma.tailnet.ts.net", Source: "tailscale", Online: true},
	}
	registered := []string{"yasyf@alpha.tailnet.ts.net", "yasyf@beta"}

	merged := mergeHosts(cands, registered)

	if !candidateByNode(t, merged, "alpha").Registered {
		t.Fatal("alpha Registered = false, want true (matched FQDN registration)")
	}
	if !candidateByNode(t, merged, "beta").Registered {
		t.Fatal("beta Registered = false, want true (matched legacy short registration)")
	}
	if candidateByNode(t, merged, "gamma").Registered {
		t.Fatal("gamma Registered = true, want false (unregistered)")
	}
}

func TestBonjourCandidatesSkipsSelf(t *testing.T) {
	cases := []struct {
		name          string
		nodes         []string
		localHostName string
		wantNodes     []string
		wantSkipNode  string
	}{
		{
			name:          "exact self match dropped",
			nodes:         []string{"alpha", "ybook-pro", "beta"},
			localHostName: "ybook-pro",
			wantNodes:     []string{"alpha", "beta"},
			wantSkipNode:  "ybook-pro",
		},
		{
			name:          "case-mismatched self dropped",
			nodes:         []string{"alpha", "yBook-Pro"},
			localHostName: "ybook-pro",
			wantNodes:     []string{"alpha"},
			wantSkipNode:  "yBook-Pro",
		},
		{
			name:          "no self present passes all through",
			nodes:         []string{"alpha", "beta"},
			localHostName: "ybook-pro",
			wantNodes:     []string{"alpha", "beta"},
			wantSkipNode:  "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cands, notes := bonjourCandidates(tc.nodes, "yasyf", tc.localHostName)

			if len(cands) != len(tc.wantNodes) {
				t.Fatalf("got %d candidates, want %d: %+v", len(cands), len(tc.wantNodes), cands)
			}
			for i, want := range tc.wantNodes {
				if cands[i].Node != want {
					t.Fatalf("candidate[%d].Node = %q, want %q", i, cands[i].Node, want)
				}
				if cands[i].DefaultTarget != "yasyf@"+want {
					t.Fatalf("candidate[%d].DefaultTarget = %q, want %q", i, cands[i].DefaultTarget, "yasyf@"+want)
				}
				if cands[i].Source != "bonjour" {
					t.Fatalf("candidate[%d].Source = %q, want bonjour", i, cands[i].Source)
				}
			}
			if hasNode(cands, tc.localHostName) {
				t.Fatalf("self node %q must not appear in candidates: %+v", tc.localHostName, cands)
			}

			if tc.wantSkipNode == "" {
				if len(notes) != 0 {
					t.Fatalf("notes = %+v, want none", notes)
				}
				return
			}
			if len(notes) != 1 {
				t.Fatalf("got %d notes, want exactly 1: %+v", len(notes), notes)
			}
			if notes[0].Name != tc.wantSkipNode {
				t.Fatalf("skip note name = %q, want %q", notes[0].Name, tc.wantSkipNode)
			}
			if notes[0].Reason != "self" {
				t.Fatalf("skip note reason = %q, want %q", notes[0].Reason, "self")
			}
		})
	}
}

func TestHostsTailscaleErrorStillSucceeds(t *testing.T) {
	r := NewMockRunner().
		OnLocal("id -un", "yasyf\n", nil).
		OnLocal("tailscale status --json", "", errors.New("exec: tailscale: not found")).
		OnLocal("scutil --get LocalHostName", "yBook-Pro\n", nil)

	// An already-cancelled context makes discoverBonjour's LookupType return
	// immediately with context.Canceled (normal completion), so this exercises the
	// tailscale + merge path without firing any live mDNS query.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := Hosts(ctx, r, nil)
	if err != nil {
		t.Fatalf("Hosts must never error on a missing source, got: %v", err)
	}
	if hasNode(res.Candidates, "alpha") {
		t.Fatal("no tailscale candidates expected when tailscale errors")
	}
	var sawTailscale bool
	for _, n := range res.Notes {
		if n.Name == "tailscale" {
			sawTailscale = true
		}
	}
	if !sawTailscale {
		t.Fatalf("expected a tailscale SkipNote, got %+v", res.Notes)
	}
}
