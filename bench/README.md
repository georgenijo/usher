# usher bench — live broker-vs-direct load test

`bench/loadtest` is a real, dep-free integration/load test that proves the
broker's core value: a **shared backend pool** instead of **one backend child per
client**. It measures **PER-PROCESS** resource usage (RSS + CPU attributed by
PID, each tagged `client` / `broker` / `backend`) — never a system total.

## The thesis

| arm | what each client does | total backend RSS |
| --- | --- | --- |
| **broker** | dials the running usher daemon's unix socket, multiplexes onto the **one** shared cua child | `1×cua`, flat as N grows |
| **direct** | spawns its **own** real cua-driver child (the 1:1, no-broker model) | `N×cua`, the spike |

Each synthetic client does a **real MCP session**: `initialize` →
`notifications/initialized` → repeated `tools/call get_screen_size`, held
connected for the whole run so the backend's memory is genuinely real (not a
momentary spike). `get_screen_size` is a read-only, gate-safe tool that exists on
cua-driver, so it round-trips without being blocked.

## Usage

```
go run ./bench/loadtest --arm broker|direct|both [flags]
```

| flag | default | meaning |
| --- | --- | --- |
| `--arm` | `both` | `broker`, `direct`, or `both` (both runs sequentially so the machine is never doubly loaded) |
| `--clients` | `15` | N — the number of synthetic client-agents |
| `--sweep` | off | run N = 1..clients and print the per-client growth curve (direct climbs ~linearly, broker stays flat) |
| `--seconds` | `6` | how long to hold clients connected per run |
| `--call` | `500ms` | `tools/call` cadence per client |
| `--sample` | `1s` | resource-sampling cadence |
| `--socket` | `~/.usher/usher.sock` | usher unix socket (broker arm) |
| `--config` | `~/.usher/config.json` | usher config (direct arm: resolves the backend command) |
| `--cua` | (configured) | override the cua-driver argv, space-separated (direct arm) |

### Broker arm prerequisite

The broker arm reads the daemon's own per-PID sampler via `GET /api/resources`, so
the daemon must be running **with the sampler on** (`USHER_SAMPLE=1`) and the
control plane enabled (no `--ui-off`):

```
USHER_SAMPLE=1 usher start --prewarm
```

The harness reads the dashboard URL the daemon recorded in `~/.usher/usher.ui`.

### Examples

```
# Headline comparison at N=15 (needs the daemon for the broker arm):
USHER_SAMPLE=1 usher start --prewarm
go run ./bench/loadtest --arm both --clients 15

# Direct arm only (spawns real cua children; no daemon needed):
go run ./bench/loadtest --arm direct --clients 8 --seconds 8

# Growth curve 1..12, direct arm:
go run ./bench/loadtest --arm direct --clients 12 --sweep
```

## Output

- A **per-PID table** per run: `ROLE | PID | LABEL | RSS_MB | CPU% | ALIVE`,
  sorted role-then-label; a dead pid is marked `DEAD`.
- **Per-role totals** (a SUM OF PER-PID rows, never a machine reading).
- The **headline**:
  `BACKEND RSS: broker=<1 child, X MB> vs direct=<N children, Y MB>`.
- With `--sweep`, a growth-curve table per arm: `N | BACKEND_RSS_MB |
  BACKEND_CHILDREN | CLIENT_RSS_MB`.

## Cleanup

Every spawned cua-driver child and every client connection is closed/killed on
exit and on Ctrl-C, triple-guarded in the direct arm:

1. `exec.CommandContext(ctx)` — cancelling the run context kills every child;
2. a `defer` reaps (Kill+Wait) every child unconditionally;
3. a post-run liveness sweep asserts zero survivors and **fails loudly** on any
   leak.

The harness never prints a system-total memory number — only per-PID rows summed
by role.

## Notes

- Sampling is dep-free: one batched `ps -o rss=,pid=,%cpu=,comm= -p <csv>` call per
  tick (RSS in KB on macOS), via `internal/procstat`. No third-party process libs,
  no cgo.
- In the **broker arm**, every synthetic client is a goroutine inside the one
  harness process, so the daemon sees N connections from a single peer PID. The
  headline metric (BACKEND RSS = 1×) is PID-accurate; the per-client RSS line is
  the harness process and is labelled honestly. The dashboard copy says "N
  connections", not "N client processes", in this arm.
```
