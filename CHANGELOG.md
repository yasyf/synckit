# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.35.0] - 2026-07-24

### Changed
- Pin daemonkit v0.18.0: the verifier worker lane sizes itself from
  `trust.VerifierWorkerBudgets()`, and `synckitd` dispatches
  `trust.RunVerifierChild` at the top of `Execute` so the serve-time trust
  self-probe passes and signed-role peers verify.

## [0.34.0] - 2026-07-24

### Changed
- Pin daemonkit v0.17.4 so helper runtime drain settles admitted requests and
  terminal transport acknowledgements before socket teardown.

## [0.33.0] - 2026-07-24

### Added
- Exact schema-v1 `rpc-serve-v1` remote sessions with a 32-byte random nonce
  echo before daemonkit framing.
- Immutable registered SSH host facts containing explicit user, HostKeyAlias,
  ordered dial addresses, and an absolute remote `synckitd` path.
- `helperruntime.New` composes resident helpers from exact daemonkit worker and
  child owners plus one product preparation/publication callback.
- Durable full/delta revision delivery with base fencing, idempotent receipts,
  and no-change suppression.

### Changed
- Pin daemonkit v0.17.2 and resolve each RPC dispatcher from the exact
  publication already admitted for its wire request. `helperruntime.Config`
  accepts the dispatcher directly so consumers cannot bypass that resolution.
- The Synckit-owned RPC suite now derives one exact
  `com.yasyf.synckit.rpc/<schema-sha256>/v1` wire build from its canonical schema.
  Daemonkit rejects every other schema before dispatch.
- Local spawned services use daemonkit's sealed spawned-session endpoint and a
  fixed `rpc-serve-v1 <service-id>` command.
- Remote services use absolute `/usr/bin/ssh`, `-F /dev/null`, one mode-0600
  known_hosts file, explicit user and HostKeyAlias, and no PTY, X11, agent,
  proxy, or forwarding surface.
- `synckitd serve` now uses daemonkit's publication runtime and unified worker
  and child ownership. LaunchAgent replacement is owned by service convergence.

### Removed
- Arbitrary `TransportRunner`, `Stdio`, `SSHStdio`, and command-transport APIs.
- Product stop-control roles for unsigned `synckitd`; no TeamID or signing
  identity is invented for local or remote service authentication.
- The EOF-delimited `rpc-once/GetState` protocol and schema.

## [0.30.0] - 2026-07-23

### Added
- `helperruntime.New` composes an embedded consumer helper from product-owned
  workers, state, resources, activation, and drain hooks while Synckit privately
  owns exact wire identity, runtime health, admission, stop authority, and the
  shared service-process store.
- `hostregistry.WithExecRunner` and `syncservice.WithTransportRunner` provide
  callback-scoped, crash-recoverable process ownership for concurrent CLI
  commands and local/SSH sync transports. Escaped runners and transports fail
  deterministically after the scope settles.

### Changed
- Pin daemonkit v0.10.0 as the exact runtime dependency for the fleet hard cut.
- The Synckit-owned RPC suite now derives one exact
  `com.yasyf.synckit.rpc/<schema-sha256>/v1` wire build from its canonical schema.
  Daemonkit rejects every other schema before dispatch.
- `rpc.Server.ServeSession` now serves one spawned-parent stdio session through
  daemonkit's exact framed engine without a synthetic listener or local adapter.
- `synckitd serve` now uses daemonkit's sole composed wire runtime, product runtime-health
  observation, receipt-authenticated stop control, exact release identity, readiness, and drain.
- LaunchAgent installation now stops and settles the exact incumbent generation before
  converging the durable desired set through daemonkit's service controller.

### Removed
- Public `syncservice.Stdio` and `syncservice.SSHStdio` constructors that exposed
  a caller-owned `*supervise.Pool`; process-backed transports now require the
  opaque scoped runner.
- The duplicate `service` package, arbitrary plist keys, manual launchctl retry
  machinery, and the unused manifest `launchd`/helper-label fields.

## [0.23.0] - 2026-07-20

### Added
- `meshtrust`: `Provider.SelfHostLabel` exposes this machine's bare MagicDNS label
  (the name's first label), for composing plaintext-http tailnet URLs — a bare
  label escapes the `ts.net` HSTS preload, so browsers don't force it to https.
  The label joins the `TrustedOrigin` set: skipped when empty (a degenerate
  leading-dot name must not admit the empty origin), and quarantined on name
  collision like the full name.

## [0.22.0] - 2026-07-20

### Added
- `meshtrust`: `MintCert` mints a TLS certificate for a MagicDNS name into a caller
  directory by shelling out to `tailscale cert` (same CLI channel and macOS app-bundle
  fallback as the status source). The host is validated post-normalization (DNS shape
  only — no flags, no path separators) and the minted files are stat-checked before
  success, so a flag-swallowed positional can never return silently empty-handed.
- `meshtrust`: the status snapshot now parses top-level `CertDomains`;
  `Provider.SelfCertDomain` exposes the normalized first entry (empty when the
  tailnet's HTTPS-certificates feature is off), and a non-empty cert domain joins the
  `TrustedOrigin` set — quarantined on name collision like `SelfDNSName`.

## [0.21.0] - 2026-07-19

### Added
- `meshtrust`: `Provider.SelfDNSName` exposes this machine's normalized MagicDNS
  name (lowercase, no trailing dot; empty while tailscale is down), for composing
  user-facing tailnet URLs. A DNS-name collision quarantines the name here too,
  matching the fail-closed `TrustedOrigin` set.

## [0.20.0] - 2026-07-19

### Changed
- The RPC transport now runs on daemonkit sub-primitives (sessions, dispatch), replacing
  synckit's bespoke unix-socket plumbing.

### Fixed
- The resident `synckitd serve` daemon and consumer helper agents no longer set launchd
  `ProcessType` to `Background`. Under sustained host load, the `darwinbg` clamp starved their
  `ioreg` and `git` probe subprocesses past bounded deadlines, killing interactive auth primes
  with `run ioreg: signal: killed`; the periodic reconcile tick remains `Background`.
- `authkit` helper discovery honors an explicit `HOMEBREW_PREFIX` exclusively instead of
  falling through to the `/opt/homebrew` and `/usr/local` defaults behind it — the fallback
  scan resolved bundles the caller's Homebrew does not own and broke temp-prefix test
  isolation on hosts with authkit installed.

## [0.17.0] - 2026-07-17

### Added
- `meshtrust/` package, extracted from cc-present's `internal/trust` + tailnet listener
  factory: a fail-closed network-trust `Provider` over the shared host mesh
  (`hostregistry.Mesh`) joined with live `tailscale status` addresses. `TrustedPeer` /
  `TrustedOrigin` match cc-interact's `daemon.Config` hook shapes (no cc-interact import),
  `SelfAddrs`/`Mesh` expose the resolved set, and `Listeners` prebinds one extra HTTP
  listener per tailnet address (port-hint first, then ephemeral; unspecified addresses
  refused) for a loopback-bound daemon. Snapshots cache for 30s with a single-flight
  refresh; an unreadable registry, a registry naming no self identity, a non-`Running`
  tailscale backend, or any unparseable address trusts nothing, and a refresh under a
  canceled caller context is never cached.

## [0.16.0] - 2026-07-16

### Added
- `consent/` and `authkit/` packages: a domain-agnostic consent engine plus a client for
  the signed authkit helper, generalized from cookiesync's `internal/auth` so cc-sudo and
  cookiesync share one implementation. The engine decides a local prompt versus a routed
  peer approval and enforces the fail-closed routed handshake (a fresh nonce per attempt,
  exact nonce+endpoint echo or terminal failure, a binding mismatch is fatal — never a
  retry). It carries an optional attestation extension (`argv`/`nonce`/`signed_by`) that
  binds a Secure Enclave signature over `nonce ‖ sha256(canonical(argv) ‖ 0x00 ‖ origin_host)`;
  synckitd forwards it opaquely and never verifies it. The `authkit` client maps the helper's
  exit codes, including the new `4` (caller-rejected/usage error — a hard failure, never
  degraded or routed around).
- `synckitd consent request|relay|presence` exposes the engine over the daemon socket; the
  requestor is derived server-side from the peer's socket identity, never client-supplied.

### Removed
- The pre-cutover legacy-mesh migration and per-tool LaunchAgent bootout. `synckitd` no
  longer seeds its shared host mesh from the retired per-tool `reposync`/`cookiesync`
  registries (`hostregistry.MigrateLegacyMesh` is deleted, along with the `serve`/`reconcile`
  calls to it), and `synckitd install` no longer boots out the retired
  `com.github.yasyf.{reposync,cookiesync}.{reconcile,watch}` agents. Both were one-time
  cutover aids from before the single shared daemon; every host has long since converged onto
  the shared mesh.

### Fixed
- `install` no longer fails with launchd's EIO (`Bootstrap failed: 5: Input/output error`) when it
  reloads a running KeepAlive agent. Booting out a live KeepAlive job is asynchronous — the old
  registration can still be draining, or KeepAlive can have respawned the job — so the immediate
  bootstrap raced launchd's catch-all EIO; install now retries the bootout-then-bootstrap pair on
  exit 5 with a doubling backoff. A persistent EIO on a session-limited agent
  (`LimitLoadToSessionType`) now names the likely cause — installing from outside that session type,
  e.g. over ssh — instead of surfacing the bare exit code.
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
  `ReconcileResult` gain `skipped_busy`. `SyncConsumer` is unchanged.
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
  an explicit capabilities surface. The registry payload stays an opaque `json.RawMessage`
  so its int64 CRDT stamps round-trip byte-exact. The manifest's `actions{}`
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

[Unreleased]: https://github.com/yasyf/synckit/compare/v0.19.1...HEAD
[0.19.1]: https://github.com/yasyf/synckit/releases/tag/v0.19.1
[0.4.2]: https://github.com/yasyf/synckit/releases/tag/v0.4.2
[0.4.1]: https://github.com/yasyf/synckit/releases/tag/v0.4.1
[0.4.0]: https://github.com/yasyf/synckit/releases/tag/v0.4.0
[0.3.0]: https://github.com/yasyf/synckit/releases/tag/v0.3.0
[0.2.0]: https://github.com/yasyf/synckit/releases/tag/v0.2.0
