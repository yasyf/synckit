package daemon

import (
	"context"
	"fmt"
	"strings"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
)

// synckitdBrew is the Homebrew formula installed on a peer that lacks synckitd.
const synckitdBrew = "yasyf/tap/synckitd"

// localNodeDiscovery browses bonjour for the .local node labels on the LAN; a var so
// tests inject a deterministic result instead of a live mDNS browse.
var (
	localNodeDiscovery = hostregistry.LocalNodes
	refreshKnownHosts  = hostregistry.Mesh.RefreshKnownHosts
)

// AddHost registers target as a peer in the shared mesh and, unless noRecurse,
// SSH-bootstraps the mesh on it: ensure synckitd is installed, install every
// manifest's consumer binary that declares a Brew formula, register the inverse
// host, then reconcile and install services on the peer. onStep (may be nil) is
// called with each step as it happens. synckit names no consumer — every consumer
// specific comes from the manifests slice.
func AddHost(ctx context.Context, r hostregistry.Runner, manifests []manifest.Manifest, target, self string, noRecurse bool, onStep func(string)) error {
	if err := hostregistry.Mesh.InitializeState(ctx); err != nil {
		return fmt.Errorf("initialize host mesh state: %w", err)
	}
	step := func(msg string) {
		if onStep != nil {
			onStep(msg)
		}
	}

	if self == "" {
		detected, err := hostregistry.DetectSelf(ctx, r)
		if err != nil && !noRecurse {
			return err
		}
		self = detected
	}

	if _, err := hostregistry.Mesh.Update(ctx, func(g *hostregistry.Registry) error {
		if self != "" {
			g.Self = self
		}
		return nil
	}); err != nil {
		return fmt.Errorf("save mesh after registering %s: %w", target, err)
	}
	if self != "" {
		step("self identity: " + self)
	}

	addresses := discoverLANAddresses(ctx, target, self)
	var daemonPath string
	if noRecurse {
		var ok bool
		daemonPath, ok = hostregistry.Mesh.RemoteBinaryPath(ctx, r, target, hostregistry.MeshBinary)
		if !ok {
			return fmt.Errorf("register %s: exact remote synckitd path is unavailable", target)
		}
		step("no-recurse: skipping remote install and reconciliation")
	} else {
		var err error
		daemonPath, err = ensureRemoteDaemon(ctx, r, target, step)
		if err != nil {
			return err
		}
	}
	fact, err := hostregistry.NewSSHHostFact(target, daemonPath, addresses)
	if err != nil {
		return err
	}
	if err := refreshKnownHosts(); err != nil {
		return fmt.Errorf("pin SSH host trust: %w", err)
	}
	if err := hostregistry.Mesh.RegisterHost(ctx, fact); err != nil {
		return fmt.Errorf("register exact SSH fact for %s: %w", target, err)
	}
	step("registered host " + target + " in local mesh")
	for _, address := range fact.Addresses[:len(fact.Addresses)-1] {
		step("recorded LAN dial address " + fact.User + "@" + address)
	}
	if noRecurse {
		return nil
	}

	for _, m := range manifests {
		if err := ensureRemoteConsumer(ctx, r, target, m, step); err != nil {
			return err
		}
	}

	remote := hostregistry.ShellQuote(daemonPath)
	if _, err := r.SSH(ctx, target, remote+" host add "+hostregistry.ShellQuote(self)+" --no-recurse"); err != nil {
		return fmt.Errorf("register inverse host on %s: %w", target, err)
	}
	step("registered inverse host " + self + " on " + target)

	if _, err := r.SSH(ctx, target, remote+" reconcile"); err != nil {
		step(fmt.Sprintf("WARN reconcile on %s: %v", target, err))
	} else {
		step("reconciled " + target)
	}

	if _, err := r.SSH(ctx, target, remote+" install"); err != nil {
		step(fmt.Sprintf("WARN install services on %s: %v", target, err))
	} else {
		step("installed services on " + target)
	}

	return nil
}

// recordLANAddr records target's ".local" mDNS address as a LAN dial candidate when
// bonjour sees target's node on the network, so a later ExecSSH reaches it over the LAN
// (under sshd's TCC identity) before the tailnet. Best-effort: a browse miss or a
// record failure is a note, never fatal.
func discoverLANAddresses(ctx context.Context, target, self string) []string {
	nodes, _ := localNodeDiscovery(ctx, hostregistry.HostNode(self))
	tgtNode := hostregistry.HostNode(target)
	for _, n := range nodes {
		if !strings.EqualFold(n, tgtNode) {
			continue
		}
		return []string{hostregistry.LocalTarget(target)}
	}
	return nil
}

// ensureRemoteDaemon installs synckitd on target over ssh unless it is already on
// the peer's PATH.
func ensureRemoteDaemon(ctx context.Context, r hostregistry.Runner, target string, step func(string)) (string, error) {
	if path, ok := hostregistry.Mesh.RemoteBinaryPath(ctx, r, target, hostregistry.MeshBinary); ok {
		step("synckitd already installed on " + target)
		return path, nil
	}
	if err := remoteBrewInstall(ctx, r, target, synckitdBrew); err != nil {
		return "", err
	}
	step("installed synckitd on " + target + " via brew")
	path, ok := hostregistry.Mesh.RemoteBinaryPath(ctx, r, target, hostregistry.MeshBinary)
	if !ok {
		return "", fmt.Errorf("resolve exact synckitd path on %s after install", target)
	}
	return path, nil
}

// ensureRemoteConsumer installs the manifest's consumer binary on target over ssh
// unless it is already installed; a manifest without a Brew formula is skipped.
func ensureRemoteConsumer(ctx context.Context, r hostregistry.Runner, target string, m manifest.Manifest, step func(string)) error {
	if m.Brew == "" {
		return nil
	}
	if hostregistry.Mesh.RemoteInstalledBinary(ctx, r, target, m.Binary) {
		step(m.Binary + " already installed on " + target)
		return nil
	}
	if err := remoteBrewInstall(ctx, r, target, m.Brew); err != nil {
		return err
	}
	step("installed " + m.Binary + " on " + target + " via brew")
	return nil
}

// remoteBrewInstall taps and installs formula on target over ssh. brew trust is
// required when the remote sets HOMEBREW_REQUIRE_TAP_TRUST, which blocks loading
// formulae from third-party taps; it is idempotent and a no-op otherwise.
func remoteBrewInstall(ctx context.Context, r hostregistry.Runner, target, formula string) error {
	tap, _, _ := strings.Cut(formula, "/")
	out, err := r.SSH(ctx, target, fmt.Sprintf("brew tap %s/tap && brew trust %s/tap && brew install %s", tap, tap, formula))
	if err == nil {
		return nil
	}
	if isNoSuchFormula(out) || isNoSuchFormula(err.Error()) {
		return fmt.Errorf("brew has no %s formula yet on %s: publish a goreleaser release to %s/homebrew-tap first: %w", formula, target, tap, err)
	}
	return fmt.Errorf("brew install %s on %s: %w", formula, target, err)
}

func isNoSuchFormula(msg string) bool {
	m := strings.ToLower(msg)
	return strings.Contains(m, "no available") ||
		strings.Contains(m, "no cask") ||
		strings.Contains(m, "no formulae")
}
