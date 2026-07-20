# synckit Development Guide

Shared substrate for cross-host sync tools: host mesh, convergent registry, unix-socket RPC, launchd, and an anti-echo watch engine.

## Repository Structure

synckit ships the `synckitd` daemon (`cmd/synckitd` + `daemon/`) plus library packages — each top-level package is one slice of the substrate, imported by reposync and cookiesync. synckitd is the one per-machine daemon that drives those consumers through declarative manifests and a CLI action contract.

```
synckit/
├── codec/         # canonical Go-duration JSON codec (shared serialization)
├── hostregistry/  # host mesh: self/hosts registry, Tailscale/Bonjour discovery,
│                  #   SSH transport, and the flock-guarded FK-preserving state store (Config{Name})
├── rpc/           # exact persistent daemonkit sessions carrying typed {method,params}
│                  #   calls with same-UID trust and bounded frames
├── daemon/        # daemonkit lifecycle runtime plus synckit-specific service policy
├── watch/         # generic anti-echo watch engine[T] (debounce / dedupe / record-before-notify)
├── .github/       # GitHub Actions workflows
├── AGENTS.md      # This file — shared conventions
└── README.md      # Project overview
```
