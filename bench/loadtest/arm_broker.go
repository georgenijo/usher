package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/mcp"
	"github.com/georgenijo/usher/internal/procstat"
)

// resourcesPayload mirrors the control plane's GET /api/resources contract
// (internal/control/resources.go). The harness reads it rather than re-sampling,
// because in the broker arm the DAEMON already attributes broker+backend+client
// pids per role and serves the rollup. We only need the fields the headline uses.
type resourcesPayload struct {
	TS      time.Time `json:"ts"`
	Samples []struct {
		PID   int     `json:"pid"`
		Role  string  `json:"role"`
		Name  string  `json:"name"`
		RSSMB float64 `json:"rssMB"`
		CPU   float64 `json:"cpu"`
		Alive bool    `json:"alive"`
	} `json:"samples"`
	Totals struct {
		BackendRSSMB      float64 `json:"backendRSS_MB"`
		BrokerRSSMB       float64 `json:"brokerRSS_MB"`
		ClientRSSMB       float64 `json:"clientRSS_MB"`
		BackendChildCount int     `json:"backendChildCount"`
		ClientCount       int     `json:"clientCount"`
	} `json:"totals"`
}

// armBroker runs the SHARED-POOL arm: n synthetic agents each DIAL the running
// usher daemon's unix socket and stay connected, so every one multiplexes onto
// the SINGLE shared cua child inside usher — total backend RSS is 1×cua no matter
// how large n is, the flat line that IS the thesis.
//
// Resources are READ from the daemon's own sampler via GET /api/resources (the
// daemon must run with USHER_SAMPLE=1; uiURL is its dashboard base). The harness
// does not sample here: the broker already tags broker/backend/client pids by
// role and serves the rollup, so re-sampling would just duplicate it. The result
// reflects the final tick read at the end of the hold window.
func armBroker(ctx context.Context, opts options, n int) (*armResult, error) {
	if _, err := os.Stat(opts.socket); err != nil {
		return nil, fmt.Errorf("usher socket %s not found: start the daemon first (USHER_SAMPLE=1 usher start --prewarm) — %w", opts.socket, err)
	}
	uiURL, err := readUIURL()
	if err != nil {
		return nil, fmt.Errorf("broker arm needs the daemon's dashboard for /api/resources: %w", err)
	}

	// Bound the run so the harness always tears down even if ctx has no deadline.
	runCtx, cancel := context.WithTimeout(ctx, opts.duration)
	defer cancel()

	var (
		wg    sync.WaitGroup
		conns []net.Conn
		mu    sync.Mutex
	)
	// Dial n clients onto the shared socket; each holds its session for the run.
	for i := 0; i < n; i++ {
		c, derr := net.Dial("unix", opts.socket)
		if derr != nil {
			cancel()
			closeAll(conns)
			wg.Wait()
			return nil, fmt.Errorf("dial usher socket (client %d/%d): %w", i+1, n, derr)
		}
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			_ = runSyntheticClient(runCtx, mcp.NewConn(conn, conn), opts.callEvery)
		}(c)
	}
	// Every dialed conn is closed on return — no leaked sockets.
	defer closeAll(conns)

	// Let the clients connect, handshake, and exercise the backend so its memory is
	// real, then read the daemon's latest resource tick. We poll until we see the
	// expected client count (or the run window ends), so the reading reflects all n
	// connections, not a half-connected snapshot.
	res := pollResources(runCtx, uiURL, n, opts.sampleEvery)

	cancel()  // stop the client call loops
	wg.Wait() // let them exit cleanly before we read the closed conns

	r := &armResult{arm: "broker", clients: n}
	for _, s := range res.Samples {
		r.procs = append(r.procs, procstat.ProcSample{
			PID:    s.PID,
			Role:   s.Role,
			Label:  s.Name,
			RSSKB:  int(s.RSSMB * 1024.0),
			CPUPct: s.CPU,
			Alive:  s.Alive,
		})
	}
	r.rollup()
	return r, nil
}

// pollResources reads GET <uiURL>/api/resources every sampleEvery until the tick
// reports at least wantClients connected clients (so the headline reflects the
// full fan-out) or ctx ends. It returns the last successful read; a transient
// HTTP error is skipped (the next tick retries), matching the sampler's own
// drop-and-retry tolerance.
func pollResources(ctx context.Context, uiURL string, wantClients int, sampleEvery time.Duration) resourcesPayload {
	var last resourcesPayload
	t := time.NewTicker(sampleEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return last
		case <-t.C:
			p, err := fetchResources(ctx, uiURL)
			if err != nil {
				continue // transient: retry next tick
			}
			last = p
			if p.Totals.ClientCount >= wantClients {
				return last
			}
		}
	}
}

// fetchResources GETs and decodes the daemon's /api/resources payload.
func fetchResources(ctx context.Context, uiURL string) (resourcesPayload, error) {
	var p resourcesPayload
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(uiURL, "/")+"/api/resources", nil)
	if err != nil {
		return p, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return p, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return p, fmt.Errorf("GET /api/resources: %s", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return p, err
	}
	return p, nil
}

// readUIURL returns the dashboard base URL the running daemon recorded in the
// state dir (config.UIURLPath), e.g. "http://127.0.0.1:7187". An absent file
// means the daemon never bound the UI (--ui-off), so the broker arm cannot read
// resources and the caller errors with guidance.
func readUIURL() (string, error) {
	b, err := os.ReadFile(config.UIURLPath())
	if err != nil {
		return "", fmt.Errorf("no recorded dashboard URL (%s); the daemon's control plane must be on (do not use --ui-off): %w", config.UIURLPath(), err)
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "", fmt.Errorf("empty dashboard URL file %s", config.UIURLPath())
	}
	return s, nil
}

// closeAll closes every conn, ignoring individual errors (best-effort teardown).
func closeAll(conns []net.Conn) {
	for _, c := range conns {
		_ = c.Close()
	}
}
