# ![synckit](docs/assets/readme-banner.webp)

**Your file watcher just synced its own write. Again.** synckit, the Go substrate under reposync and cookiesync, ships anti-echo watching, unix-socket RPC, a host mesh, and flock-backed state, each written once.

[![CI](https://github.com/yasyf/synckit/actions/workflows/ci.yml/badge.svg)](https://github.com/yasyf/synckit/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/yasyf/synckit)](https://github.com/yasyf/synckit/releases)
[![License: PolyForm Noncommercial](https://img.shields.io/badge/license-PolyForm--NC--1.0.0-blue)](LICENSE)

## Get started

```bash
go get github.com/yasyf/synckit
```

A persistent RPC suite with exact build admission, same-UID trust, multiplexing,
and bounded 16 MiB frames is already wired. Register product methods, then pass
the server and exact daemonkit process owners to `helperruntime.New`:

```go
package main

import (
	"context"

	"github.com/yasyf/synckit/rpc"
)

func rpcServer() *rpc.Server {
	d := rpc.NewDispatcher()
	d.Register("ping", func(ctx context.Context, p map[string]any) (any, error) {
		return map[string]any{"pong": p["msg"]}, nil
	})
	return rpc.NewServer(d)
}
```

Driving with an agent? Paste this:

```text
Run `go get github.com/yasyf/synckit`, then use its rpc package to stand up a
unix-socket server: register handlers on `rpc.NewDispatcher`, compose
`rpc.NewServer(...).Wire` into daemonkit Runtime, and call it through a persistent
`rpc.NewClient` using `rpc.WireBuild`. Listener ownership, readiness, trust, and
bounded framing stay in daemonkit.
```

---

## Use cases

### Build a keep-X-in-sync daemon without rewriting the plumbing

Every "keep X in sync across my machines" tool re-implements the same daemon: a socket server, host discovery, launchd plists, a reconcile tick, a watch supervisor. Ship a manifest instead:

```bash
brew install yasyf/tap/synckitd
synckitd register manifest.json
```

`synckitd` installs the manifest under `~/.config/synckit/manifests` and drives your tool's typed sync service — list, reconcile, sync — through either a resident Unix socket or a receipt-authenticated local spawned session. Remote calls use Synckit's fixed `rpc-serve-v1` command over strict host-key-pinned SSH. The daemon never imports your code.

Process-backed transports are private to `synckitd` and owned by one daemonkit
process manager. Resident socket helpers use `helperruntime.New`, supplying exact
worker and child owners plus one product preparation callback.

### Watch files without chasing your own writes

Your daemon writes a file, fsnotify fires, the watcher syncs the write it just made, and around it goes. The watch engine breaks the loop:

```go
eng := watch.NewEngine(resolver, notifier, digest, 2*time.Second, peers)
eng.OnEvent(ctx, id)
```

The engine debounces, dedupes on the resolved fingerprint, and records what it applied *before* notifying peers — so the echo of its own write resolves to the recorded fingerprint and dies there instead of fanning out again.

### Merge state from every peer without a write storm

Push-based sync between two daemons is a feedback loop: each write triggers the other's. `converge.Reconcile` is pull-only:

```go
results, err := converge.Reconcile(ctx, lock, driver, fetcher, peers, origin)
```

A pass fetches every peer's registry read-only, folds them in with the CRDT merge (a LWW-element-set join), and performs exactly one write: the local `SaveRegistry`. Merge in any order and every replica lands on the identical registry; an unreachable peer is logged and skipped, never fatal.

## The packages

| Package | What it holds |
|---|---|
| `rpc` | Exact persistent daemonkit sessions carrying typed `{method,params}` calls with same-UID trust and bounded frames |
| `syncservice` | The typed sync contract over `rpc` plus the resident socket transport |
| `watch` | The generic anti-echo watch engine: debounce, fingerprint dedupe, record-before-notify, concurrent peer fan-out, busy gating |
| `watchbackend` | Filesystem events mapped to watch ids over recursive fsnotify (inotify/kqueue) |
| `hostregistry` | The host mesh: reachability detection, Tailscale and Bonjour discovery, an ssh runner, flock-guarded `state.json` |
| `cregistry` | LWW-element-set CRDT registry with per-item payloads; pure and clock-free |
| `converge` | The pull-only convergent-reconcile pass over a `cregistry` registry |
| `manifest` | The JSON manifest a consumer registers, plus discovery and validation |
| `daemon` | The `synckitd` command tree, daemonkit lifecycle runtime, and product-specific typed LaunchAgent policy |
| `codec` | Config-free JSON codecs, e.g. the canonical Go-duration string |
| `tui` | Shared bubbletea terminal UI: a tab router plus the built-in Hosts tab |

reposync and cookiesync import this one substrate, so the wire formats, lock semantics, and fan-out constants two daemons must agree on byte-for-byte are defined once and tested once.

## The synckitd daemon

`synckitd` is the one per-machine daemon behind every consumer: it owns the shared host mesh, the RPC socket, the reconcile tick, and the watch supervisor. `synckitd status` prints the mesh, registered manifests, socket path, and daemon liveness; `synckitd --help` carries the full command surface.

Status: pre-1.0 — the API still moves between minors. Licensed under [PolyForm Noncommercial 1.0.0](LICENSE).
