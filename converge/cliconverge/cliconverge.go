// Package cliconverge adapts the generic [converge] orchestration to a CLI
// consumer whose registry payload synckit never decodes. The registry value is an
// opaque [encoding/json.RawMessage], so the daemon merges add/remove/value-union
// state across the host mesh without ever knowing the consumer's domain schema.
//
// A consumer ships a CLI that implements the synckit action contract: a fetch
// command that prints its read-only registry JSON to stdout and an apply command
// that reads a merged registry JSON from stdin. [Driver] runs both locally (fetch
// then apply) to load and persist; [Fetcher] runs only the fetch command over ssh,
// because a pull-merge pass is READ-ONLY against a peer — apply is never shelled
// remotely.
//
// Per-item reconcile is not modeled here: the daemon shells the consumer's own
// "reconcile" command once after the merge persists, so [Driver.Reconcile] is a
// no-op returning [OutcomeCLIDeferred].
package cliconverge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"

	"github.com/yasyf/synckit/converge"
	"github.com/yasyf/synckit/cregistry"
)

// OutcomeCLIDeferred is the [converge.Outcome] reported for every item by a CLI
// consumer's [Driver.Reconcile]. Per-item reconcile is deferred to the consumer's
// own "reconcile" command, which the daemon shells once after the merged registry
// persists; the per-item body here does no work.
const OutcomeCLIDeferred converge.Outcome = "cli-deferred"

// Runner runs a consumer's CLI both locally and over ssh, piping stdin and
// returning stdout. It is distinct from hostregistry.Runner, which has no stdin
// channel; the apply command reads the merged registry from stdin, so this seam
// carries it. It is defined here because cliconverge is its only consumer.
type Runner interface {
	// Local runs name with args locally, feeding stdin to the process and returning
	// its stdout. It honors ctx.
	Local(ctx context.Context, name string, stdin []byte, args ...string) (string, error)
	// SSH runs remoteCmd on target over ssh, feeding stdin to the remote process and
	// returning its stdout. It honors ctx.
	SSH(ctx context.Context, target, remoteCmd string, stdin []byte) (string, error)
}

// NewExecRunner returns the default [Runner] backed by os/exec: Local invokes the
// command directly and SSH invokes the local ssh client against target.
func NewExecRunner() Runner {
	return execRunner{}
}

type execRunner struct{}

func (execRunner) Local(ctx context.Context, name string, stdin []byte, args ...string) (string, error) {
	//nolint:gosec // G204: this is a CLI sync tool whose job is to shell registered consumer binaries; name and args come from trusted local manifests, not untrusted input.
	cmd := exec.CommandContext(ctx, name, args...)
	return runCmd(cmd, stdin)
}

func (execRunner) SSH(ctx context.Context, target, remoteCmd string, stdin []byte) (string, error) {
	//nolint:gosec // G204: this is a CLI sync tool whose job is to run ssh; target and remoteCmd come from trusted local mesh/manifest state, not untrusted input.
	cmd := exec.CommandContext(ctx, "ssh", target, remoteCmd)
	return runCmd(cmd, stdin)
}

func runCmd(cmd *exec.Cmd, stdin []byte) (string, error) {
	if len(stdin) > 0 {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("run %q: %w: %s", cmd.Args, err, stderr.String())
	}
	return out.String(), nil
}

// Driver is a [converge.Driver] over an opaque json.RawMessage registry, backed by
// a consumer's CLI. LoadRegistry shells the fetch command locally and SaveRegistry
// pipes the merged registry to the apply command locally; Reconcile is a no-op.
type Driver struct {
	bin       string
	fetchArgs []string
	applyArgs []string
	runner    Runner
}

// NewDriver builds a [Driver] that runs bin with fetchArgs to read the registry and
// bin with applyArgs to write it back, both locally through r.
func NewDriver(bin string, fetchArgs, applyArgs []string, r Runner) *Driver {
	return &Driver{bin: bin, fetchArgs: fetchArgs, applyArgs: applyArgs, runner: r}
}

// LoadRegistry runs the fetch command locally and unmarshals its stdout into the
// opaque registry.
func (d *Driver) LoadRegistry(ctx context.Context) (cregistry.Registry[json.RawMessage], error) {
	out, err := d.runner.Local(ctx, d.bin, nil, d.fetchArgs...)
	if err != nil {
		return nil, fmt.Errorf("cliconverge: fetch registry: %w", err)
	}
	return decodeRegistry(out)
}

// SaveRegistry marshals reg and pipes it to the apply command's stdin locally.
func (d *Driver) SaveRegistry(ctx context.Context, reg cregistry.Registry[json.RawMessage]) error {
	payload, err := json.Marshal(reg)
	if err != nil {
		return fmt.Errorf("cliconverge: marshal registry: %w", err)
	}
	if _, err := d.runner.Local(ctx, d.bin, payload, d.applyArgs...); err != nil {
		return fmt.Errorf("cliconverge: apply registry: %w", err)
	}
	return nil
}

// Reconcile is a no-op for a CLI consumer: per-item reconcile is deferred to the
// consumer's own "reconcile" command, shelled once by the daemon after the merge.
// It always returns [OutcomeCLIDeferred] and a nil error.
func (d *Driver) Reconcile(_ context.Context, _ string, _ cregistry.Entry[json.RawMessage], _ []string, _ string) (converge.Outcome, error) {
	return OutcomeCLIDeferred, nil
}

// Fetcher is a [converge.Fetcher] over an opaque json.RawMessage registry: it runs
// the consumer's fetch command on a peer over ssh and unmarshals the result. It is
// READ-ONLY — apply is never shelled remotely.
type Fetcher struct {
	bin       string
	fetchArgs []string
	runner    Runner
}

// NewFetcher builds a [Fetcher] that runs bin with fetchArgs on a peer over ssh
// through r.
func NewFetcher(bin string, fetchArgs []string, r Runner) Fetcher {
	return Fetcher{bin: bin, fetchArgs: fetchArgs, runner: r}
}

// Fetch runs the fetch command on peer over ssh and unmarshals its stdout into the
// opaque registry. It never writes to the peer.
func (f Fetcher) Fetch(ctx context.Context, peer string) (cregistry.Registry[json.RawMessage], error) {
	out, err := f.runner.SSH(ctx, peer, remoteCmd(f.bin, f.fetchArgs), nil)
	if err != nil {
		return nil, fmt.Errorf("cliconverge: fetch registry from %q: %w", peer, err)
	}
	return decodeRegistry(out)
}

func decodeRegistry(out string) (cregistry.Registry[json.RawMessage], error) {
	var reg cregistry.Registry[json.RawMessage]
	if err := json.Unmarshal([]byte(out), &reg); err != nil {
		return nil, fmt.Errorf("cliconverge: decode registry: %w", err)
	}
	return reg, nil
}

func remoteCmd(bin string, args []string) string {
	parts := append([]string{bin}, args...)
	return shellJoin(parts)
}

func shellJoin(parts []string) string {
	var b bytes.Buffer
	for i, p := range parts {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(p)
	}
	return b.String()
}
