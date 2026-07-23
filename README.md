# ![synckit](docs/assets/readme-banner.webp)

**Your file watcher just synced its own write. Again.** synckit, the Go substrate under reposync and cookiesync, ships anti-echo watching, unix-socket RPC, a host mesh, and flock-backed state, each written once.

[![CI](https://github.com/yasyf/synckit/actions/workflows/ci.yml/badge.svg)](https://github.com/yasyf/synckit/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/yasyf/synckit)](https://github.com/yasyf/synckit/releases)
[![License: PolyForm Noncommercial](https://img.shields.io/badge/license-PolyForm--NC--1.0.0-blue)](LICENSE)

## Get started

```bash
go get github.com/yasyf/synckit
```

A persistent unix-socket RPC server with exact build admission, same-UID trust,
multiplexing, and bounded 16 MiB frames already wired:

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/synckit/rpc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := rpc.NewDispatcher()
	d.Register("ping", func(ctx context.Context, p map[string]any) (any, error) {
		return map[string]any{"pong": p["msg"]}, nil
	})

	sock := filepath.Join(os.TempDir(), "synckit-rpc-demo.sock")
	_ = os.Remove(sock)
	defer os.Remove(sock)
	ln, _ := rpc.Listen(ctx, sock)
	go rpc.NewServer(d).Serve(ctx, ln)

	client := rpc.NewClient(rpc.ClientConfig{Dial: wire.UnixDialer(sock), WireBuild: rpc.WireBuild})
	defer client.Close()
	resp, _ := client.Call(ctx, &rpc.Request{
		Method: "ping",
		Params: map[string]any{"msg": "hi"},
	})
	var result map[string]string
	_ = json.Unmarshal(resp.Result, &result)
	fmt.Printf("resp.Result = %v\n", result)
}
```

```console
$ go run .
resp.Result = map[pong:hi]
```

(Real output; [`docs/scripts/demo.sh`](docs/scripts/demo.sh) regenerates it.)

Driving with an agent? Paste this:

```text
Run `go get github.com/yasyf/synckit`, then use its rpc package to stand up a
unix-socket server: register a ping handler on rpc.NewDispatcher, bind with
rpc.Listen, serve with rpc.NewServer, and call it through a persistent
rpc.NewClient using rpc.WireBuild. Keep the exact-build handshake, same-UID trust,
and bounded framing as shipped.
```

---

## Use cases

### Build a keep-X-in-sync daemon without rewriting the plumbing

Every "keep X in sync across my machines" tool re-implements the same daemon: a socket server, host discovery, launchd plists, a reconcile tick, a watch supervisor. Ship a manifest instead:

```bash
brew install yasyf/tap/synckitd
synckitd register manifest.json
```

`synckitd` installs the manifest under `~/.config/synckit/manifests` and drives your tool's typed sync service — list, reconcile, sync — over the transport the manifest declares: a unix socket, a spawned child's stdio, or ssh to a peer. The daemon never imports your code.

Consumer CLIs share one crash-recoverable process owner across an entire
concurrent pass with `syncservice.WithTransportRunner`; its opaque runner creates
local `Stdio` and remote `SSHStdio` transports and settles every session when the
callback ends. Resident socket helpers use `helperruntime.New`, supplying only
their product workers, state, resources, activation, and pre-settlement drain.

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
| `syncservice` | The typed sync contract over `rpc` plus socket transport and callback-scoped local/SSH process transports |
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
