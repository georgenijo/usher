package control

// resources.go is the control plane's RESOURCES surface: it holds the LATEST
// per-process resource tick so GET /api/resources answers a freshly-opened tab
// immediately (without waiting for the next SSE frame), and rolls the per-pid
// rows up by role into the headline totals — total BACKEND RSS vs total CLIENT
// RSS, the broker-vs-direct load-test thesis. It is fed by Watch, one more Hub
// subscriber alongside connRegistry and the SSE stream, off ResourceSampleEvent.
// It never touches the forwarding hot path, and because each tick is a FULL
// snapshot (not a delta), a dropped frame (the Hub is drop-oldest) at worst
// leaves the panel stale-by-one until the next tick — never inconsistent.
//
// The one hard invariant is PER-PROCESS, NEVER SYSTEM-TOTAL: every number here
// is a SUM OF PER-PID rows the sampler explicitly watched, computed server-side
// so the headline is identical in the REST tab, the SSE snapshot, and the live
// panel. There is deliberately no machine-total field anywhere.

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/georgenijo/usher/internal/broker"
)

// resSample is the control-plane view of one sampled process for GET
// /api/resources: pid, role, human name, RSS in MEGABYTES (converted once from
// the sampler's KB so the client never re-divides), and CPU%. Alive surfaces a
// pid that exited between ticks (greyed in the UI) rather than dropping it
// silently. The JSON field names (name, rssMB, cpu) are the REST contract; the
// raw SSE frames still carry the broker's KB-unit ProcStat shape.
type resSample struct {
	PID   int     `json:"pid"`
	Role  string  `json:"role"`
	Name  string  `json:"name"`
	RSSMB float64 `json:"rssMB"`
	CPU   float64 `json:"cpu"`
	Alive bool    `json:"alive"`
}

// resTotals are the per-role rollups: each is a SUM OF PER-PID RSS rows (in MB),
// never a system reading. backendChildCount is the number of LIVE backend pids
// summed into backendRSS_MB — in the broker arm it stays 1 as clients climb (the
// flat line), in the direct arm it tracks N (the spike).
type resTotals struct {
	BackendRSSMB      float64 `json:"backendRSS_MB"`
	BrokerRSSMB       float64 `json:"brokerRSS_MB"`
	ClientRSSMB       float64 `json:"clientRSS_MB"`
	BackendChildCount int     `json:"backendChildCount"`
	ClientCount       int     `json:"clientCount"`
}

// resourcesPayload is what GET /api/resources and the SSE snapshot's resources
// field carry: the per-pid rows PLUS the role rollups, so the headline metric is
// computed once on the server and is identical everywhere it is read. ts is the
// time of the tick these numbers came from.
type resourcesPayload struct {
	TS      time.Time   `json:"ts"`
	Samples []resSample `json:"samples"`
	Totals  resTotals   `json:"totals"`
}

// resourceState holds the latest resource tick. Fed by Watch off
// ResourceSampleEvent (never on the hot path), read by GET /api/resources and the
// SSE snapshot frame. mu guards both fields.
type resourceState struct {
	mu   sync.Mutex
	last []broker.ProcStat // raw per-pid rows from the most recent tick (KB unit)
	ts   time.Time
}

// newResourceState returns an empty state whose snapshot is a well-formed empty
// payload until the first tick arrives.
func newResourceState() *resourceState {
	return &resourceState{}
}

// Watch consumes the Hub and stores the most recent ResourceSampleEvent's rows,
// mirroring connRegistry.Watch. It returns when ctx is cancelled or the hub
// closes the subscription. A nil bus makes it a no-op so a bare test server (no
// daemon behind it) can skip it.
func (rs *resourceState) Watch(ctx context.Context, bus *broker.Hub) {
	if bus == nil {
		return
	}
	ch, cancel := bus.Subscribe(256)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			rs.apply(e)
		}
	}
}

// apply folds one event into the latest tick. Only ResourceSampleEvent moves the
// state; every other event type is ignored here (the SSE stream, the audit
// subscriber, and connRegistry consume those). Each event is a full snapshot, so
// the stored rows are REPLACED, not merged.
func (rs *resourceState) apply(e broker.Event) {
	ev, ok := e.(broker.ResourceSampleEvent)
	if !ok {
		return
	}
	rs.mu.Lock()
	rs.last = ev.Procs
	rs.ts = ev.TS
	rs.mu.Unlock()
}

// snapshot rolls the latest tick's per-pid rows up into the REST/SSE payload:
// KB→MB once per row, role totals summed (backend / broker / client), and the
// rows sorted role-then-name for a stable listing. A tick that never arrived
// yields an empty, well-formed payload (no nil slices) so a fresh tab paints a
// clean panel rather than erroring.
func (rs *resourceState) snapshot() resourcesPayload {
	rs.mu.Lock()
	rows := rs.last
	ts := rs.ts
	rs.mu.Unlock()

	out := resourcesPayload{TS: ts, Samples: make([]resSample, 0, len(rows))}
	for _, p := range rows {
		mb := float64(p.RSSKB) / 1024.0
		out.Samples = append(out.Samples, resSample{
			PID:   p.PID,
			Role:  p.Role,
			Name:  p.Label,
			RSSMB: mb,
			CPU:   p.CPUPct,
			Alive: p.Alive,
		})
		// Roll up by role. A dead pid (Alive:false) contributes 0 RSS and is not
		// counted, so the headline reflects only processes ps still saw this tick.
		if !p.Alive {
			continue
		}
		switch p.Role {
		case roleBackend:
			out.Totals.BackendRSSMB += mb
			out.Totals.BackendChildCount++
		case roleBroker:
			out.Totals.BrokerRSSMB += mb
		case roleClient:
			out.Totals.ClientRSSMB += mb
			out.Totals.ClientCount++
		}
	}
	sort.Slice(out.Samples, func(i, j int) bool {
		if out.Samples[i].Role == out.Samples[j].Role {
			return out.Samples[i].Name < out.Samples[j].Name
		}
		return out.Samples[i].Role < out.Samples[j].Role
	})
	return out
}

// Role string literals, kept local so this package does not depend on procstat's
// Role type (control already imports broker, not procstat). They MUST match
// procstat's RoleBackend/RoleBroker/RoleClient values, which is what the sampler
// stamps onto every ProcStat.Role over the wire.
const (
	roleBackend = "backend"
	roleBroker  = "broker"
	roleClient  = "client"
)
