# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/yasyf/synckit/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/yasyf/synckit/releases/tag/v0.2.0
