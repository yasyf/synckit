# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- The shared TUI no longer renders past the bottom of the terminal. The master-detail split
  double-subtracted each panel's horizontal padding, so a row sized to the full column width wrapped
  onto a second line and pushed the layout off-screen; both panes now render at their budgeted width
  without wrapping, and each fills its full column instead of coming up two cells short.
- The router's help bar now reserves its true height. Expanded help (`?`) prints one line per binding
  and a tab switch can change how many bindings show, but the router reserved a fixed single row and
  let the overflow shove the view past the last terminal line; it now measures the help height live
  and reflows the active screen whenever help toggles or the active tab changes.
- The Hosts tab's removal confirmation no longer pushes the list past the terminal. The confirm box
  stacked three rows below a body that already filled the height budget; opening or closing the
  prompt now recomputes the split so its box is reserved out of the list instead of overflowing.

### Added
- Busy-awareness through the watch layer. `watch.Gate` (with `watch.WithGate`) teaches the engine
  to defer a busy item's evaluation instead of acting on it: nothing is recorded or notified while
  the item is busy, the evaluation retries at a configurable cadence, and an item deferred past a
  max-defer window fires through — so a pending change lands right after the item goes idle
  instead of parking until an external tick. An ungated engine behaves exactly as before.
- Busy on the wire: `syncservice.WatchItem` gains `busy`/`busy_reason`, and `SyncResult`/
  `ReconcileResult` gain `skipped_busy`. All three are additive `omitempty` fields that decode
  compatibly in both directions, so `ProtocolVersion` stays 1 and `SyncConsumer` is unchanged.
- The daemon gates each manifest's watch engine on the busy state its consumer reports via
  `List`, retrying at the manifest's debounce cadence and firing through after ten windows. The
  gate shares its `List` round trip with the fingerprint resolver, so a gated evaluation costs
  one RPC, not two.
- The daemon's watch-notify path now runs a per-peer circuit breaker. The first failed notify to
  a peer logs once, captures a one-line tailscale snapshot of that peer (online, direct or DERP,
  endpoint), and opens the breaker; further notifies are suppressed while a retry probes on a
  doubling cooldown (30s up to 5m). A successful retry is a full-manifest sync, so recovery,
  catch-up, and the single 'recovered after <duration>' line are one act.

### Changed
- `converge.Reconcile` takes a `*converge.PeerStatus` and logs unreachable peers per outage
  instead of per pass: one line when a peer goes down, one with the outage duration when it
  recovers. Skip semantics are unchanged. Consumers hold one tracker for the life of their
  process.
- hostregistry: pin ssh targets to tailscale MagicDNS FQDNs — DetectSelf and tailscale discovery
  now mint `user@host.<tailnet>.ts.net` targets (previously the bare first label), so peer ssh
  always rides the tailscale path instead of racing LAN DNS. Discovery and the hosts TUI
  match/display peers by short node label via the new `hostregistry.HostNode`. Existing
  short-name registrations keep working; `host rm` + `host add` each peer to adopt FQDN dialing.

## [0.4.2] - 2026-06-27

### Fixed
- The shared TUI's Hosts tab no longer blanks to a full "Discovering hosts…" screen on every launch.
  It seeds the known (registered) mesh hosts instantly and revalidates in place — discovered hosts
  and verify results update as the background scan finishes, with a subtle "refreshing…" indicator.

## [0.4.1] - 2026-06-27

### Fixed
- The shared TUI's Hosts tab now sorts registered mesh peers above merely-discovered candidates,
  instead of interleaving them.

## [0.4.0] - 2026-06-27

### Added
- `tui` package: the shared terminal-UI shell (tab router, master-detail, filter bar, theme) and
  the Hosts tab, extracted from reposync so every consumer drives the same UI. `tui.Run(ctx,
  Options{Brand, Screens, Runner})` builds the app from consumer content screens and always
  appends the shared Hosts tab; consumer screens implement the exported `Screen` interface and
  compose the exported primitives (`MasterDetail`, `SplitDims`, `FilterBar`, the style vars).
- `hostregistry.Config.Binary` (and `MeshBinary`): the binary name probed over ssh to decide a
  host is bootstrapped, split from `Config.Name` (the `~/.config/<Name>` config-dir name).

### Fixed
- Host discovery no longer lists the local Mac in its own Hosts list. Bonjour advertised self
  under its `.local` name, which slipped past the tailscale self-skip; `discoverBonjour` now drops
  the candidate whose node matches `scutil --get LocalHostName`.
- A peer that has the daemon installed now reads "installed" instead of "reachable, not installed".
  `Verify`/`RemoteInstalled` for the shared mesh probe `synckitd` (via `Config.Binary`) rather than
  the mesh's config-dir name `synckit`, which was never an installed binary.

## [0.3.0] - 2026-06-26

### Changed
- Replace the stringly-typed CLI action contract between synckitd and its consumers with a
  typed RPC service. The new `syncservice` package defines a `SyncConsumer` interface
  (capabilities/list/reconcile/sync/get_state) served over the existing newline-JSON `rpc`
  as `svc.`-namespaced methods, a typed `Client` over socket/stdio/ssh-stdio transports, and
  a `ProtocolVersion` handshake that fails loud on skew. The registry payload stays an opaque
  `json.RawMessage` so its int64 CRDT stamps round-trip byte-exact. The manifest's `actions{}`
  + `watch.list_cmd` are replaced by a `service{transport,serve_args,sock}` block; the daemon
  drives consumers over the typed client (with async, retrying engine startup) instead of
  rendering and shelling argv templates.

### Added
- `rpc.ServeConn` (a streaming request/response loop over one stdio/ssh pipe), `rpc.CallRaw`
  and `rpc.Proxy` (raw byte-exact forwarding for a consumer's `rpc-serve` bridge), the
  exported `rpc.ReadLine`, and `hostregistry.SSHArgv`.

### Removed
- The action-template machinery (`manifest.ActionSpec`/`ActionVars`/`Render`),
  `manifest.WatchItem` (moved to `syncservice`), and the unused `converge/cliconverge` package.

## [0.2.0] - 2026-06-25

### Added
- `synckitd`, the generic per-machine sync daemon (`cmd/synckitd` + `daemon/`): one shared
  host mesh, rpc socket, reconcile tick, and watch supervisor that drive consumers through
  declarative JSON manifests and a CLI action contract (`list`/`reconcile`/`sync`/`state
  get-json`/`state apply-json`), shelling out to each consumer binary.
- `hostregistry.Mesh` (the single shared `~/.config/synckit` mesh), `MigrateLegacyMesh`, and
  `VerifyBinary`.
- `manifest`, `converge/cliconverge`, and `watchbackend` packages; optional
  `service.AgentSpec.Binary` for per-consumer helper agents.
- goreleaser release of the `synckitd` binary to the Homebrew tap (`brew install
  yasyf/tap/synckitd`).

[Unreleased]: https://github.com/yasyf/synckit/compare/v0.4.2...HEAD
[0.4.2]: https://github.com/yasyf/synckit/releases/tag/v0.4.2
[0.4.1]: https://github.com/yasyf/synckit/releases/tag/v0.4.1
[0.4.0]: https://github.com/yasyf/synckit/releases/tag/v0.4.0
[0.3.0]: https://github.com/yasyf/synckit/releases/tag/v0.3.0
[0.2.0]: https://github.com/yasyf/synckit/releases/tag/v0.2.0
