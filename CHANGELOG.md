# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/yasyf/synckit/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/yasyf/synckit/releases/tag/v0.3.0
[0.2.0]: https://github.com/yasyf/synckit/releases/tag/v0.2.0
