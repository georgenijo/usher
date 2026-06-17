# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What usher is

`usher` is a single-binary macOS daemon + control CLI: an **MCP broker** that sits
between AI agents and the MCP tool servers ("backends") they drive. An agent talks
to `usher` instead of wiring each tool itself; `usher` routes the JSON-RPC call to
the right backend and runs every message through an ordered middleware pipeline —
**gate** (block destructive calls), **arbitrate** (per-window write-lock), **trim**
(compact oversized AX digests), **audit** (log everything). The default behavior is
**verbatim forwarding**; each substantive stage falls back to pass-through until
configured, and the MCP handshake (`initialize`, `notifications/initialized`,
`tools/list`) always crosses untouched.

## Commands

```sh
go build -o usher ./cmd/usher     # build the binary
go test ./...                     # run all tests
go vet ./...                      # vet (CI gate)
go test ./internal/broker -run TestGate   # single test / single package
go test -race ./internal/broker   # race detector (the broker is concurrent)
```

Release validation (GoReleaser, `.goreleaser.yaml`):

```sh
goreleaser check                      # lint config against v2 schema
goreleaser build --snapshot --clean   # confirm both arches + universal build
```

Release is `git tag vX.Y.Z && git push origin vX.Y.Z` → `.github/workflows/release.yml`.
Signing/notarization are MANUAL local steps — see `RELEASING.md`. Do not assume CI signs.

## Hard constraints (read before editing)

- **Go stdlib only.** `go.mod` has zero dependencies (go 1.26). No `golang.org/x/*`,
  no third-party libs in the broker. Echo suppression uses `stty`, Keychain uses
  `/usr/bin/security`, peer creds use raw syscalls — all to keep this true.
- **macOS only.** launchd, the Keychain, and the Accessibility tree behind backends
  are assumed. `CGO_ENABLED=0` (pure Go, no framework linking).
- **The MCP stream must never be corrupted.** A stage that touches a message it
  shouldn't, or drops/duplicates a response, breaks the agent. Every stage guards on
  direction + message kind so the handshake and unrelated messages pass through.

## Architecture

### The pipeline (the core abstraction)

`internal/broker/pipeline.go` + `stages.go`. A `Stage` is `Process(ctx, m) → (m, err)`:
return the message to forward, `(nil, nil)` to **drop** it, or an error to abort that
one message (the link stays up). `Broker` holds **two** pipelines, one per direction:

- **inbound** (client→backend): `gate → arbitrate → audit`
- **outbound** (backend→client): `arbitrate(release) → trim → audit`

Stages are nil-safe: an unconfigured policy/registry makes the stage pass-through, which
is how the verbatim-forward default is preserved. **If a stage mutates a message it MUST
set `m.Raw = nil`** so `mcp.Conn.Write` re-marshals from the struct instead of forwarding
the original bytes (`internal/mcp/jsonrpc.go` — `Message.Raw` holds the exact wire bytes).

### Request/response correlation

A single `InflightMap` per connection ties the two pipelines together. The inbound pump
records `id → {method, tool, lock}` when it sees a request; outbound stages consume it:
- `TrimStage` only compacts a result whose request was a `tools/call` (`Consume`).
- `ArbitrateStage` releases the write-lock its inbound pass took (`Peek` then `Release`,
  token-checked so a TTL/death-reclaimed lease is a harmless no-op).
Order matters: arbitrate-release uses `Peek` so the later `TrimStage.Consume` still finds
the entry.

### Single-backend vs multi-backend

`broker.go` is the 1:1 path: `ServeStdio` (agent spawned us) and `ServeSocket` (daemon
accept loop) both funnel into `serveConn` → two pumps (`b.pump`). One backend, names are
bare.

`fanout.go` is the N:1 aggregation path (`ServeMulti`, `--all`/`--backends`). It merges
every backend's `tools/list` into one namespaced list (`<backend>__<tool>`), routes
`tools/call` by prefix, and **remaps each request id to a globally-unique backend-side id**
so two backends can't collide; the original client id is restored just before the wire
(`pumpFanoutOutbound`). Critically, `routeToolCall` strips the namespace and rewrites
`params.name` to the **bare** tool name BEFORE running the inbound pipeline — so GateStage
and ArbitrateStage always match against bare names in both paths.

### Tool-name namespacing — the gotcha

`DefaultBlockedTools` and arbitration classification key on **bare** tool names, never
namespaced. The single-backend pump has no prefix; the fanout strips the prefix before the
pipeline. Never add a namespaced name to a block/allow list.

### Gate policy (#18)

`policyFromConfig` unions built-in `DefaultBlockedTools` + `cfg.BlockedTools` (block-list)
against `cfg.AllowedTools` + `USHER_ALLOW_TOOLS` env (allow-list). **Allow-list always
wins** — it's the operator's accepted-risk escape hatch. A blocked `tools/call` is refused
in-band with JSON-RPC error `-32020`; the backend never sees it. Window-busy is `-32010`.
The dashboard / draft-before-destructive confirmation flow is deferred (the `Policy` struct
is shaped to grow a `RequireConfirmation` set later).

### State, config, secrets

`internal/config` owns `~/.usher/` (override `USHER_STATE_DIR`): `config.json`, `usher.sock`,
`usher.pid`. Config is stdlib JSON. `EnvForBackend` resolves a backend's auth strategy at
**serve time** (never at add time): `none`/`inherit`→ inherit parent env; `env`→ read each
`EnvKeys` name from the Keychain (`internal/keychain`, service `usher.<name>`) — **only the
key NAMES are in config.json, values live in the Keychain**; `oauth`→ not yet supported.
Backend registration (`usher backend add`) is **handshake-validate-before-save**: it probes
`initialize` against the backend and refuses to persist one that doesn't speak MCP.

### Daemon lifecycle (#20)

`cmd/usher/lifecycle.go` + `plist.go`. `usher serve --socket` is the daemon foreground body;
`start`/`stop`/`status` background it via a PID file + signal-0 liveness check; `install`/
`uninstall` hand it to launchd (`com.georgenijo.usher`). The lock registry is **process-wide**
so contention is arbitrated across concurrent socket connections, not just within one.

## Code conventions

- Comments reference GitHub issue numbers (`#14` skeleton, `#15` trim, `#16` arbitrate,
  `#17` fanout, `#18` gate, `#20` daemon, `#32` registration) — they map a stage/feature to
  its tracked work and call out what is deferred. Preserve this when extending a stage.
- `version` in `main.go` is a `var` stamped at release via `-ldflags -X main.version=`.
  Plain `go build` reports `0.0.1-dev`.
- Tests live beside the code (`*_test.go`); platform-specific files use build-tag suffixes
  (`peerpid_darwin.go` / `peerpid_other.go`).
