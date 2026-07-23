package hostregistry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
)

const maxConcurrentHosts = 8

// Registry is the host-identity slice of state.json: how peers reach this machine
// (Self) and the peers it reaches (Hosts). Load and Update read-modify-write only
// these keys, leaving every other key in the file untouched.
type Registry struct {
	Self  string   `json:"self"`
	Hosts []string `json:"hosts"`
}

// UpsertHost adds a peer ssh target unless it is already registered.
func (g *Registry) UpsertHost(target string) {
	for _, h := range g.Hosts {
		if h == target {
			return
		}
	}
	g.Hosts = append(g.Hosts, target)
}

// RemoveHost drops a peer ssh target.
func (g *Registry) RemoveHost(target string) {
	kept := make([]string, 0, len(g.Hosts))
	for _, h := range g.Hosts {
		if h != target {
			kept = append(kept, h)
		}
	}
	g.Hosts = kept
}

// Load reads the self/hosts identity from exact schema v1 state.
func (c Config) Load() (*Registry, error) {
	env, err := c.readEnvelope()
	if err != nil {
		return nil, err
	}
	return &Registry{Self: env.Host.Self, Hosts: slices.Clone(env.Host.Hosts)}, nil
}

// Update runs fn against the freshly loaded Registry under the shared reconcile
// lock, then replaces self and hosts while preserving the declared addresses and
// product payload. It serializes read-modify-write across processes.
func (c Config) Update(ctx context.Context, fn func(*Registry) error) (*Registry, error) {
	var out *Registry
	err := c.WithLock(ctx, func() error {
		env, err := c.readEnvelope()
		if err != nil {
			return err
		}
		g := &Registry{Self: env.Host.Self, Hosts: slices.Clone(env.Host.Hosts)}
		if err := fn(g); err != nil {
			return err
		}
		env.Host.Self = g.Self
		env.Host.Hosts = slices.Clone(g.Hosts)
		out = g
		return c.writeEnvelope(env)
	})
	return out, err
}

// DetectSelf returns the ssh target by which a peer reaches this machine,
// derived from the tailscale MagicDNS name (its full FQDN) and the local user, so
// the peer always dials the tailscale path instead of racing LAN DNS.
func DetectSelf(ctx context.Context, r Runner) (string, error) {
	out, err := r.Local(ctx, "tailscale", "status", "--json")
	if err != nil {
		return "", fmt.Errorf("detect self via tailscale (pass --self to override): %w", err)
	}
	var status struct {
		Self struct {
			DNSName string `json:"DNSName"`
		} `json:"Self"`
	}
	if err := json.Unmarshal([]byte(out), &status); err != nil {
		return "", fmt.Errorf("parse tailscale status (pass --self to override): %w", err)
	}
	host := strings.TrimSuffix(status.Self.DNSName, ".")
	if host == "" {
		return "", fmt.Errorf("empty tailscale node name (pass --self to override)")
	}
	user, err := r.Local(ctx, "id", "-un")
	if err != nil {
		return "", fmt.Errorf("detect local user: %w", err)
	}
	return strings.TrimSpace(user) + "@" + host, nil
}

// RemoveHost unregisters target as a peer and persists the change.
func (c Config) RemoveHost(ctx context.Context, target string) error {
	if _, err := c.Update(ctx, func(g *Registry) error {
		g.RemoveHost(target)
		return nil
	}); err != nil {
		return fmt.Errorf("save state after removing %s: %w", target, err)
	}
	return nil
}

// VerifyResult reports a single host's reachability and install state.
type VerifyResult struct {
	Target       string
	Reachable    bool
	Bootstrapped bool
	Version      string
	Err          error
}

// Verify probes target over ssh: whether it is reachable, has the tool installed,
// and its version, shelling the Config's own Binary.
func (c Config) Verify(ctx context.Context, r Runner, target string) VerifyResult {
	return c.VerifyBinary(ctx, r, target, c.Binary)
}

// VerifyBinary probes target over ssh — whether it is reachable, has binary
// installed, and binary's version — using the given binary name instead of the
// Config's Name, for a shared mesh whose Name is not itself an installed tool.
func (c Config) VerifyBinary(ctx context.Context, r Runner, target, binary string) VerifyResult {
	res := VerifyResult{Target: target}
	if c.RemoteInstalledBinary(ctx, r, target, binary) {
		res.Reachable = true
		res.Bootstrapped = true
		if out, err := r.SSH(ctx, target, binary+" --version"); err == nil {
			res.Version = strings.TrimSpace(out)
		}
		return res
	}
	if _, err := r.SSH(ctx, target, "true"); err != nil {
		res.Err = fmt.Errorf("probe %s: %w", target, err)
		return res
	}
	res.Reachable = true
	return res
}

// VerifyAll verifies every host concurrently, returning one result per host in
// input order.
func (c Config) VerifyAll(ctx context.Context, r Runner, hosts []string) []VerifyResult {
	results := make([]VerifyResult, len(hosts))
	sem := make(chan struct{}, maxConcurrentHosts)
	var wg sync.WaitGroup
	for i, target := range hosts {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, target string) {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = c.Verify(ctx, r, target)
		}(i, target)
	}
	wg.Wait()
	return results
}

// RemoteInstalled reports whether the Config's Binary is on target's PATH over ssh.
func (c Config) RemoteInstalled(ctx context.Context, r Runner, target string) bool {
	return c.RemoteInstalledBinary(ctx, r, target, c.Binary)
}

// RemoteInstalledBinary reports whether binary is on target's PATH over ssh,
// probing the given binary name instead of the Config's Name.
func (c Config) RemoteInstalledBinary(ctx context.Context, r Runner, target, binary string) bool {
	out, err := r.SSH(ctx, target, "command -v "+binary)
	if err != nil {
		return false
	}
	return strings.TrimSpace(out) != ""
}

// EachHost runs fn against every host concurrently (bounded), joining per-host
// failures into one error so a single down host never aborts the others.
func EachHost(ctx context.Context, hosts []string, fn func(ctx context.Context, target string) error) error {
	sem := make(chan struct{}, maxConcurrentHosts)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, target := range hosts {
		wg.Add(1)
		sem <- struct{}{}
		go func(target string) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(ctx, target); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", target, err))
				mu.Unlock()
			}
		}(target)
	}
	wg.Wait()
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%d host(s) failed: %w", len(errs), errors.Join(errs...))
}

// TailscaleNode returns the first DNS label of a tailscale DNSName.
func TailscaleNode(dnsName string) string {
	trimmed := strings.TrimSuffix(dnsName, ".")
	label, _, _ := strings.Cut(trimmed, ".")
	return label
}
