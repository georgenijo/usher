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

## Dashboard (control-plane UI)

The always-on daemon (`usher serve --socket`, or backgrounded with
`usher start`) also serves a **loopback-only** web dashboard: see connected
backends and their state (stopped / starting / live / failed), start / stop /
restart them, and watch who is calling which backend — including a backend
**coming live** the first time an agent routes a call to it.

```sh
usher start            # background the daemon (socket + dashboard)
usher status           # prints: running pid=… socket=… ui=http://127.0.0.1:7187
usher ui               # open the dashboard in your browser (macOS `open`)
usher stop             # stop the daemon (also stops the dashboard)
```

The dashboard binds `127.0.0.1` only — never a routable interface — because
usher is a single-user local tool with no auth; loopback is the security
boundary. Live updates ride a Server-Sent-Events stream; management actions are
POSTs. The page is a single self-contained file embedded in the binary (no Node,
no JS deps).

Configure the port and on/off, highest precedence first:

```sh
usher start --ui-port 9000      # bind 127.0.0.1:9000 for this run
usher start --ui-off            # serve MCP only; no dashboard
USHER_UI_ADDR=127.0.0.1:9000 usher serve --socket   # env override (validated loopback)
```

```jsonc
// ~/.usher/config.json — the persistent defaults (used by the launchd daemon)
{
  "uiAddr": "127.0.0.1:7187",   // loopback host:port; empty → built-in default
  "uiOff": false                // true disables the dashboard entirely
}
```

A non-loopback `uiAddr`/`USHER_UI_ADDR` is rejected at bind time, so the API can
never be exposed on a routable host. If the port is taken the daemon logs a
warning and still serves MCP over the socket.

## Install

macOS only (launchd, the Keychain, the Accessibility tree behind its backends).

```sh
# Homebrew (cask)
brew tap georgenijo/usher && brew install --cask usher

# curl (no Homebrew)
curl -fsSL https://raw.githubusercontent.com/georgenijo/usher/main/scripts/install.sh | sh

# from source
go install github.com/georgenijo/usher/cmd/usher@latest

usher install   # register the always-on launchd daemon
```

Releases are cut with [GoReleaser](.goreleaser.yaml) (a single universal binary
+ per-arch tarballs, checksummed). Sign/notarize/publish are manual steps —
see [RELEASING.md](./RELEASING.md).

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
internal/broker/   the front desk: pipeline + stages + serve loop + event bus
internal/control/  loopback HTTP control plane (REST + SSE) + embedded dashboard
internal/identity/ identity-at-connect
internal/audit/    append-only message log
internal/config/   backends + state dir
```

## License

MIT.
