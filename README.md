<div align="center">

# 🛎️ usher

### The MCP broker — one front desk every agent talks to.

</div>

---

`usher` is a standalone daemon that sits between AI agents and the MCP tool
servers they drive. Instead of each agent wiring every tool itself, it talks to
`usher`, which **routes** the call to the right backend, **trims** oversized
responses, **arbitrates** claims on shared resources (the one Mac screen),
**gates** destructive actions, and **audits** everything — an ordered
middleware pipeline.

It's the front desk for a small fleet of local agents. [GhostHands](https://github.com/georgenijo/ghosthands)
(the macOS "hands") becomes one *backend* behind it; `agent-mesh` is the fleet
bus alongside it.

## Status

**Skeleton (#14) + live pipeline.** Today `usher serve` is a working stdio proxy:
it spawns a backend, forwards JSON-RPC verbatim in both directions, stamps an
identity at connect, and audits every message. The substantive stages —
`trim` (#15 ★), `arbitrate` (#16), and `gate` (#18) — are implemented; each
falls back to pass-through until configured, so the verbatim-forward default
holds. The handshake (`initialize`, `notifications/initialized`, `tools/list`)
always crosses the pipeline untouched.

## Try it

```sh
go build -o usher ./cmd/usher
./usher version

# register a backend (any stdio MCP server); cua is the default if none set
./usher backend add cua -- ~/.local/bin/cua-driver mcp
./usher backend list

# proxy an agent over stdio to that backend
./usher serve --backend cua
```

`usher serve` reads JSON-RPC from stdin and writes to stdout, so any MCP client
that spawns a stdio server can point at `usher serve` instead of the tool
directly. Audit lines go to stderr.

## Design

- **Single binary**, daemon + control CLI. State dir `~/.usher/` (override with
  `USHER_STATE_DIR`).
- **Go, stdlib-only** (no deps in the skeleton).
- **No containers** for the broker — it fronts host-bound hands (macOS
  Accessibility / Screen). Containers only ever sandbox untrusted backends.
- **Arbitration** (#16): per-window write-lock, ungated reads, TTL lease +
  reclaim-on-death. No RW-lock, no global lock, no preemption.
- **Gate** (#18): block destructive/irreversible tool-calls by policy (below).
- **Backend registration** (planned, #32): one path — transport
  (`stdio`/`http`) × auth-strategy (`none`/`env`/`inherit`/`oauth`), Keychain
  secrets, handshake-validated, namespaced.

### Gate — block destructive actions (#18)

The inbound `gate` stage refuses destructive/irreversible tool-calls before the
broker forwards them. A blocked `tools/call` is never sent to the backend; the
client gets a JSON-RPC error (code `-32020`) carrying the original request id, so
the agent gets a clear answer instead of a silent drop. Read-only and benign
calls, and the whole MCP handshake, pass through untouched.

A built-in destructive set is blocked out of the box — `kill_app` (irreversible)
plus the canonical web-DOM mutators `delete`, `remove`, `send`, `submit`,
`purchase`. Tune it from config or the environment (names are **bare** tool
names, never namespaced):

```jsonc
// ~/.usher/config.json
{
  "blockedTools": ["drag"],     // ADDED to the built-in set
  "allowedTools": ["kill_app"]  // OVERRIDE: forwarded even though it is blocked
}
```

```sh
# unblock destructive tools for a single run, without editing config
USHER_ALLOW_TOOLS=kill_app,submit usher serve --backend cua
```

The allow-list always wins over the block-list, so an operator can let a specific
destructive tool through after accepting the risk.

**Deferred (still part of #18):** the *unified dashboard / "absorb monitor"* — a
live view that surfaces gated calls for human approval — is **not** implemented.
Nor is the *draft-before-destructive* confirmation flow (hold the call, ask, then
re-emit on approval); that needs a held-message store and a `-32021`
requires-confirmation code, and the `Policy` struct is shaped to grow a
`RequireConfirmation` set for it later. Today's gate is the simpler
block-and-refuse path only.

## Layout

```
cmd/usher/         CLI entrypoint + subcommand dispatch
internal/mcp/      JSON-RPC 2.0 framing over newline-delimited stdio
internal/backend/  stdio backend (spawn an MCP server, bridge its stdio)
internal/broker/   the front desk: pipeline + stages + serve loop
internal/identity/ identity-at-connect
internal/audit/    append-only message log
internal/config/   backends + state dir
```

## License

MIT.
