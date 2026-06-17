<div align="center">

# 🛎️ usher

### The MCP broker — one front desk every agent talks to.

*Route. Trim. Arbitrate. Gate. Audit.* — an ordered middleware pipeline between your agents and their tools.

<p>
  <img alt="platform" src="https://img.shields.io/badge/platform-macOS-000000?logo=apple&logoColor=white">
  <img alt="go" src="https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white">
  <img alt="deps" src="https://img.shields.io/badge/deps-stdlib%20only-44cc11">
  <img alt="license" src="https://img.shields.io/badge/license-MIT-blue">
  <img alt="status" src="https://img.shields.io/badge/status-alpha-orange">
</p>

</div>

---

```console
$ usher backend add cua -- ~/.local/bin/cua-driver mcp
registered backend "cua" -> [cua-driver mcp] (transport=stdio, auth=inherit, handshake: ok)

$ usher serve --backend cua          # any MCP client points here instead of the tool
usher: client→backend  tools/call   id=7   click            #  arbitrate: window-locked
usher: backend→client  result       id=7   42b
usher: client→backend  tools/call   id=8   kill_app         #  gate: BLOCKED (-32020)
```

Every local agent wires the same tools — the same screen, the same shell, the same
risk. **usher** is the front desk they all check in at instead. One connection in,
one routed call out, with a middleware pipeline in between that **trims** oversized
responses, **arbitrates** the one Mac screen, **gates** destructive actions, and
**audits** the lot. Verbatim-forward by default; substance only where you ask for it.

> Built for a small fleet of local agents. [GhostHands](https://github.com/georgenijo/ghosthands)
> (the macOS "hands") becomes one *backend* behind it; `agent-mesh` is the fleet bus alongside it.

## How it works

```
                 ┌──────────────────────── usher ────────────────────────┐
                 │                                                        │
  agent  ──────▶ │   gate  ──▶  arbitrate  ──▶  audit   ──────────────▶   │ ──▶  backend
 (MCP client)    │   block      window-lock     log                      │     (cua, fs, …)
                 │                                                        │
  agent  ◀────── │   audit  ◀──   trim    ◀──  arbitrate  ◀───────────    │ ◀──  backend
                 │   log         compact       unlock                     │
                 └────────────────────────────────────────────────────────┘
              JSON-RPC over stdio / unix socket          stdio MCP servers
```

Two pipelines, one per direction. Each stage is pass-through until you configure it,
so the default is a faithful proxy — and the MCP handshake (`initialize`,
`notifications/initialized`, `tools/list`) always crosses untouched.

| Stage | Direction | What it does |
| --- | --- | --- |
| 🚧 **gate** | in | Refuses destructive/irreversible `tools/call` by policy — the backend never sees them. |
| 🔒 **arbitrate** | in / out | Per-window write-lock so two agents can't fight over one window. TTL lease, reclaim-on-death. |
| ✂️ **trim** | out | Compacts an oversized Accessibility-tree digest down to the buttons + values a brain needs. |
| 📒 **audit** | in / out | Append-only line for every message crossing the desk. |
| 🧭 **route** | — | One connection fans out to many backends; tools are namespaced `backend__tool`. |

## Quickstart

```sh
go build -o usher ./cmd/usher
./usher version

# register any stdio MCP server as a backend (handshake-validated before it's saved)
./usher backend add cua -- ~/.local/bin/cua-driver mcp
./usher backend list

# proxy one agent over stdio to that backend …
./usher serve --backend cua

# … or aggregate several behind one connection, tools namespaced by backend
./usher serve --all
```

`usher serve` reads JSON-RPC from stdin and writes to stdout, so any MCP client that
spawns a stdio server can point at `usher serve` instead of the tool directly. Audit
lines go to stderr.

### Run it always-on

```sh
usher install     # register the launchd LaunchAgent (survives logout + crashes)
usher status      # running pid=… socket=~/.usher/usher.sock
usher stop        # … or start / uninstall
```

## Install

macOS only — usher leans on launchd, the Keychain, and the Accessibility tree behind
its backends.

```sh
# Homebrew (cask)
brew tap georgenijo/usher && brew install --cask usher

# curl (no Homebrew)
curl -fsSL https://raw.githubusercontent.com/georgenijo/usher/main/scripts/install.sh | sh

# from source
go install github.com/georgenijo/usher/cmd/usher@latest

usher install
```

Releases are cut with [GoReleaser](.goreleaser.yaml) — one universal binary + per-arch
tarballs, checksummed. See [RELEASING.md](./RELEASING.md).

## Gate — block destructive actions

The inbound `gate` stage refuses destructive `tools/call` before the broker forwards
them. A blocked call is never sent to the backend; the client gets a JSON-RPC error
(`-32020`) carrying the original id, so the agent gets a clear answer instead of a
silent drop. Reads, benign calls, and the whole handshake pass through.

Out of the box it blocks `kill_app` plus the canonical web-DOM mutators `delete`,
`remove`, `send`, `submit`, `purchase`. Tune it (names are **bare**, never namespaced):

```jsonc
// ~/.usher/config.json
{
  "blockedTools": ["drag"],     // ADDED to the built-in set
  "allowedTools": ["kill_app"]  // OVERRIDE: forwarded even though it's blocked
}
```

```sh
# unblock for a single run, no config edit
USHER_ALLOW_TOOLS=kill_app,submit usher serve --backend cua
```

The allow-list always wins, so an operator can let a specific destructive tool through
after accepting the risk.

## Design notes

- **Single binary**, daemon + control CLI. State dir `~/.usher/` (override `USHER_STATE_DIR`).
- **Go, stdlib-only** — zero dependencies.
- **No containers for the broker** — it fronts host-bound hands (Accessibility / Screen).
  Containers only ever sandbox untrusted backends.
- **Backend registration**: transport (`stdio`/`http`) × auth (`none`/`env`/`inherit`/`oauth`),
  secrets in the Keychain (only key *names* hit disk), handshake-validated, namespaced.
- **Arbitration**: per-window write-lock, ungated reads, TTL lease + reclaim-on-death.
  No RW-lock, no global lock, no preemption.

> **Status — alpha.** The proxy, identity, audit, gate, arbitrate, trim, multi-backend
> fanout, and the launchd daemon are live. Deferred: the unified approval dashboard and
> the draft-before-destructive confirmation flow. `http` transport and `oauth` are
> validated-but-stubbed.

## Layout

```
cmd/usher/         CLI entrypoint, subcommands, launchd lifecycle
internal/mcp/      JSON-RPC 2.0 framing over newline-delimited stdio
internal/backend/  stdio backend (spawn an MCP server, bridge its stdio)
internal/broker/   the front desk: pipeline + stages + serve loop + fanout
internal/identity/ identity-at-connect (peer-pid on the socket)
internal/audit/    append-only message log
internal/config/   backends, state dir, auth resolution
internal/keychain/ macOS Keychain secrets (auth=env)
```

## License

[MIT](./LICENSE).
