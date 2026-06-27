# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
