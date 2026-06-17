package main

import (
	"fmt"
	"io"
	"sort"
	"text/tabwriter"

	"github.com/georgenijo/usher/internal/procstat"
)

// roleOrder ranks the three roles for a stable, readable table ordering:
// broker first (the front desk), then backend children, then clients.
var roleOrder = map[string]int{
	string(procstat.RoleBroker):  0,
	string(procstat.RoleBackend): 1,
	string(procstat.RoleClient):  2,
}

// armResult is one arm's measured outcome: the last sampled per-pid rows, the
// per-role RSS totals (in MB, summed from per-pid rows — NEVER a system total),
// and the child counts the headline compares. clients is the requested client
// count for this run (the N in "N children").
type armResult struct {
	arm     string // "broker" | "direct"
	clients int

	procs []procstat.ProcSample // last tick's per-pid rows, role-tagged

	backendRSSMB float64
	brokerRSSMB  float64
	clientRSSMB  float64
	backendCount int
	clientCount  int
}

// rollup recomputes the per-role totals from the per-pid rows. Dead rows
// (Alive:false — a pid ps no longer saw) contribute nothing, so the totals
// reflect only processes that were genuinely resident this tick. It is a SUM OF
// PER-PID ROWS by construction; there is deliberately no machine-total path.
func (r *armResult) rollup() {
	r.backendRSSMB, r.brokerRSSMB, r.clientRSSMB = 0, 0, 0
	r.backendCount, r.clientCount = 0, 0
	for _, p := range r.procs {
		if !p.Alive {
			continue
		}
		mb := float64(p.RSSKB) / 1024.0
		switch p.Role {
		case string(procstat.RoleBackend):
			r.backendRSSMB += mb
			r.backendCount++
		case string(procstat.RoleBroker):
			r.brokerRSSMB += mb
		case string(procstat.RoleClient):
			r.clientRSSMB += mb
			r.clientCount++
		}
	}
}

// printTable writes the per-PID table (role, pid, label, RSS MB, CPU%, alive)
// plus the per-role totals to w. Rows are sorted role-then-label so the broker,
// its backend children, and the clients group cleanly. RSS is shown in MB
// (KB/1024 once); a dead pid is marked so a leak/early-exit is visible.
func (r *armResult) printTable(w io.Writer) {
	rows := append([]procstat.ProcSample(nil), r.procs...)
	sort.Slice(rows, func(i, j int) bool {
		ri, rj := roleOrder[rows[i].Role], roleOrder[rows[j].Role]
		if ri != rj {
			return ri < rj
		}
		if rows[i].Label != rows[j].Label {
			return rows[i].Label < rows[j].Label
		}
		return rows[i].PID < rows[j].PID
	})

	fmt.Fprintf(w, "\n=== arm=%s clients=%d : per-process resources (per-PID, never system-total) ===\n", r.arm, r.clients)
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "ROLE\tPID\tLABEL\tRSS_MB\tCPU%\tALIVE")
	for _, p := range rows {
		alive := "yes"
		if !p.Alive {
			alive = "DEAD"
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%.1f\t%.1f\t%s\n",
			p.Role, p.PID, p.Label, float64(p.RSSKB)/1024.0, p.CPUPct, alive)
	}
	_ = tw.Flush()

	fmt.Fprintf(w, "totals: broker=%.1f MB  backend=%.1f MB (%d %s)  client=%.1f MB (%d clients)\n",
		r.brokerRSSMB, r.backendRSSMB, r.backendCount, plural(r.backendCount, "child", "children"),
		r.clientRSSMB, r.clientCount)
}

// printHeadline writes the thesis line comparing the two arms' BACKEND RSS: the
// broker arm is one shared cua child no matter how many clients connect (flat),
// the direct arm is N children (the spike). Either arm may be nil if only one was
// run.
func printHeadline(w io.Writer, broker, direct *armResult) {
	fmt.Fprintln(w, "\n=== HEADLINE: shared backend pool vs per-client backends ===")
	switch {
	case broker != nil && direct != nil:
		fmt.Fprintf(w, "BACKEND RSS: broker=<%d %s, %.1f MB> vs direct=<%d %s, %.1f MB>\n",
			broker.backendCount, plural(broker.backendCount, "child", "children"), broker.backendRSSMB,
			direct.backendCount, plural(direct.backendCount, "child", "children"), direct.backendRSSMB)
		if broker.backendRSSMB > 0 {
			fmt.Fprintf(w, "the broker pools %d clients onto %.1f MB of backend; direct spends %.1f MB (%.1fx)\n",
				broker.clientCount, broker.backendRSSMB, direct.backendRSSMB, ratio(direct.backendRSSMB, broker.backendRSSMB))
		}
	case broker != nil:
		fmt.Fprintf(w, "BACKEND RSS: broker=<%d %s, %.1f MB> (direct arm not run)\n",
			broker.backendCount, plural(broker.backendCount, "child", "children"), broker.backendRSSMB)
	case direct != nil:
		fmt.Fprintf(w, "BACKEND RSS: direct=<%d %s, %.1f MB> (broker arm not run)\n",
			direct.backendCount, plural(direct.backendCount, "child", "children"), direct.backendRSSMB)
	}
}

// printSweepCurve writes the per-client growth curve for a sweep: one row per N,
// showing backend and client RSS, so the direct arm's ~linear climb and the
// broker arm's flat backend line are visible side by side.
func printSweepCurve(w io.Writer, arm string, results []*armResult) {
	fmt.Fprintf(w, "\n=== arm=%s : growth curve (1..N) ===\n", arm)
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "N\tBACKEND_RSS_MB\tBACKEND_CHILDREN\tCLIENT_RSS_MB")
	for _, r := range results {
		fmt.Fprintf(tw, "%d\t%.1f\t%d\t%.1f\n", r.clients, r.backendRSSMB, r.backendCount, r.clientRSSMB)
	}
	_ = tw.Flush()
}

// plural picks the singular or plural noun for n.
func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// ratio is direct/broker guarded against a zero denominator (so a 0-MB broker
// reading prints 0 rather than +Inf).
func ratio(direct, broker float64) float64 {
	if broker <= 0 {
		return 0
	}
	return direct / broker
}
