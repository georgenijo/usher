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

**Skeleton (#14).** Today `usher serve` is a working stdio proxy: it spawns a
backend, forwards JSON-RPC verbatim in both directions, stamps an identity at
connect, and audits every message. The pipeline's substantive stages — `trim`
(#15 ★), `arbitrate` (#16), `gate` (#18) — are wired in execution order but
pass-through; implementing one means filling in its `Process`.

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
- **Arbitration** (planned, #16): per-window write-lock, ungated reads, TTL
  lease + reclaim-on-death. No RW-lock, no global lock, no preemption.
- **Backend registration** (planned, #32): one path — transport
  (`stdio`/`http`) × auth-strategy (`none`/`env`/`inherit`/`oauth`), Keychain
  secrets, handshake-validated, namespaced.

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
