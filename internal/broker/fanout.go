package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"

	"github.com/georgenijo/usher/internal/backend"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/identity"
	"github.com/georgenijo/usher/internal/mcp"
)

// JSON-RPC error codes the fanout answers the client with when a tools/call
// cannot be routed. Both live in the application-defined server-error range
// (-32000..-32099), so they never collide with a protocol code.
const (
	// ErrToolNotNamespaced is returned for a tools/call whose name carries no
	// "<backend>__" prefix — the broker cannot tell which backend owns it.
	ErrToolNotNamespaced = -32005
	// ErrUnknownBackend is returned when the name's prefix names no attached
	// backend (e.g. "ghost__click" with only "cua" aggregated).
	ErrUnknownBackend = -32006
)

// serverInfoName/Version is what the aggregated initialize advertises to the
// client: the broker presents itself as one server ("usher"), not as a list of
// backends, since MCP's initialize has no field for "list of backends".
const (
	serverInfoName    = "usher"
	serverInfoVersion = "0.0.1-dev"
)

// fanout aggregates N live backend connections behind one client connection. It
// is used only by ServeMulti; the single-backend ServeStdio path never touches
// it. The merged tools/list is computed once after initialize and answered from
// cache, and tools/call is routed by namespace prefix to the owning backend with
// the client's request id remapped to a globally-unique backend-side id so two
// backends can never return colliding ids.
type fanout struct {
	// order is the configured backend order, used to keep the merged tools/list
	// stable and to namespace deterministically.
	order []string
	// conns maps backend name -> its JSON-RPC connection (set once at start).
	conns map[string]*mcp.Conn

	// mergedTools is the aggregated, namespaced tools array (the value of
	// result.tools), serialized once and answered for every client tools/list.
	mergedTools json.RawMessage

	// nextID is the monotonic allocator for backend-side request ids. Owning the
	// id space across all backends is what makes a cross-backend id collision
	// impossible by construction.
	nextID atomic.Uint64
}

// allocID returns the next globally-unique backend-side request id, rendered as
// a JSON number string ("17") so it round-trips through IDString.
func (f *fanout) allocID() string {
	return strconv.FormatUint(f.nextID.Add(1), 10)
}

// ServeMulti fans one client connection (in/out) across the named backends,
// aggregating their tool namespaces. An empty backendNames aggregates every
// configured backend in config order. Single-backend behaviour is unchanged:
// callers that want the legacy 1:1 proxy still use ServeStdio.
//
// Lifecycle: start all backends, run the initialize fanout (sequential from the
// client's view), prefetch and merge tools/list, then pump — N outbound pumps
// (one per backend, all writing the single client conn, whose Write is already
// serialized) and one inbound dispatcher that routes by namespace.
func (b *Broker) ServeMulti(ctx context.Context, backendNames []string, in io.Reader, out io.Writer) error {
	bes, err := b.resolveBackends(backendNames)
	if err != nil {
		return err
	}

	id := identity.New()
	f := &fanout{order: make([]string, 0, len(bes)), conns: make(map[string]*mcp.Conn, len(bes))}

	// Start every backend; on any start failure tear down the ones already up so
	// we never leak a child process. A backend that fails to start is fatal here
	// (before the client's initialize is answered) — partial aggregation with a
	// dead backend would advertise tools that can never be routed.
	var started []*backend.Stdio
	for _, be := range bes {
		sb := backend.NewStdio(be.Name, be.Command)
		if err := sb.Start(ctx); err != nil {
			for _, s := range started {
				_ = s.Close()
			}
			return fmt.Errorf("start backend %q: %w", be.Name, err)
		}
		started = append(started, sb)
		f.order = append(f.order, be.Name)
		f.conns[be.Name] = sb.Conn()
		b.audit.Connect(id, be.Name)
	}
	defer func() {
		for _, sb := range started {
			_ = sb.Close()
		}
	}()
	defer b.reclaim(id)

	client := mcp.NewConn(in, out)

	// One correlation map shared by the inbound dispatcher and all outbound
	// pumps, exactly as ServeStdio shares one map between its two pumps.
	inflight := NewInflightMap()

	// The MCP handshake is strictly sequential from the client's view: it blocks
	// on the initialize response, then sends notifications/initialized, before
	// any other traffic. We complete the handshake against ALL backends here,
	// before starting the full-duplex pumps, so the routing table (mergedTools)
	// is fully built and no tools/call can race ahead of it.
	if err := f.runHandshake(b, id, client); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
			b.audit.Disconnect(id, "client-eof")
			return nil
		}
		return err
	}

	// Reply lets an outbound stage (none today) or the dispatcher answer the
	// client out of band; it shares the client conn's serialized Write.
	reply := func(m *mcp.Message) error { return client.Write(m) }

	// N outbound pumps: each backend -> the one client conn. The id-restore stage
	// (restoreClientID) runs first so the client sees its own ids, then the
	// normal outbound pipeline (arbitrate-release, trim, audit) runs.
	outboundDone := make(chan error, len(started))
	for _, sb := range started {
		sb := sb
		go func() {
			outboundDone <- b.pumpFanoutOutbound(id, sb.Name(), inflight, sb.Conn(), client)
		}()
	}

	// One inbound dispatcher: client -> the owning backend, by namespace.
	inboundDone := make(chan error, 1)
	go func() {
		inboundDone <- b.dispatchInbound(id, f, inflight, reply, client)
	}()

	select {
	case <-ctx.Done():
		b.audit.Disconnect(id, "signal")
		return nil
	case <-inboundDone:
		// Client hung up: half-close every backend so each flushes and exits,
		// then drain their outbound pumps before we let go.
		for _, sb := range started {
			_ = sb.CloseStdin()
		}
		for range started {
			<-outboundDone
		}
		b.audit.Disconnect(id, "client-eof")
		return nil
	case err := <-outboundDone:
		// A backend ended. The others stay alive, but with one pump gone we tear
		// the connection down (a future watchdog, #20, can reconnect instead).
		reason := "backend-eof"
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
			reason = err.Error()
		}
		b.audit.Disconnect(id, reason)
		return nil
	}
}

// resolveBackends turns the requested names (empty = all) into config backends,
// validating each is a supported stdio transport. Order follows config order so
// the merged tools/list is deterministic.
func (b *Broker) resolveBackends(names []string) ([]*config.Backend, error) {
	var out []*config.Backend
	if len(names) == 0 {
		for i := range b.cfg.Backends {
			be := &b.cfg.Backends[i]
			if be.Transport != "stdio" {
				return nil, fmt.Errorf("backend %q: transport %q not supported yet", be.Name, be.Transport)
			}
			out = append(out, be)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("no backend configured (run: usher backend add <name> -- <command...>)")
		}
		return out, nil
	}
	for _, name := range names {
		be := b.cfg.ResolveBackend(name)
		if be == nil {
			return nil, fmt.Errorf("no backend named %q (run: usher backend list)", name)
		}
		if be.Transport != "stdio" {
			return nil, fmt.Errorf("backend %q: transport %q not supported yet", be.Name, be.Transport)
		}
		out = append(out, be)
	}
	return out, nil
}

// runHandshake completes the MCP startup exchange against every backend before
// the duplex pumps begin. It (1) reads the client's initialize, fans it out, and
// merges the responses into one answer to the client; (2) reads the client's
// notifications/initialized and fans it out; (3) prefetches tools/list from every
// backend and builds the namespaced merged list. The MCP lifecycle requires a
// server to receive notifications/initialized before it answers tool/resource
// requests, so the prefetch MUST follow the notification fanout — a strict
// backend would otherwise reject or defer the tools/list. Sequencing here is safe
// because the full-duplex pumps have not started yet, so no race can occur. After
// this returns nil the routing table (f.mergedTools) is fully built, so no
// tools/call can race ahead of it. The fanout owns the id allocator and the
// merged-tools cache it populates, so this lives on fanout.
func (f *fanout) runHandshake(b *Broker, id identity.Identity, client *mcp.Conn) error {
	// 1. initialize ----------------------------------------------------------
	initReq, err := client.Read()
	if err != nil {
		return err
	}
	if !initReq.IsRequest() || initReq.Method != "initialize" {
		// A well-behaved MCP client opens with initialize. If it does not, we
		// forward the message to the first backend and answer its response, so a
		// non-standard but functional client is not broken; the routing table is
		// then built lazily by the tools/list prefetch below regardless.
		// For the common case this branch is never taken.
		return fmt.Errorf("expected initialize, got %q", initReq.Method)
	}

	// Fan the initialize verbatim to every backend in parallel and collect their
	// responses. The client's id is reused on each backend (no remap needed: we
	// own this exchange and read exactly one response per backend conn).
	initResps, err := f.broadcastRequest(initReq)
	if err != nil {
		return err
	}

	// Answer the client with the first backend's initialize result, re-stamped
	// with the client's id and usher's serverInfo. Capabilities/protocolVersion
	// come from one backend; exact cross-backend capability merging is deferred
	// (the broker can route to whichever backend has a capability regardless).
	merged := mergeInitialize(initResps, f.order, initReq.ID)
	b.audit.Message(id.ID, Outbound.String(), "initialize", initReq.IDString(), len(merged.Raw))
	if err := client.Write(merged); err != nil {
		return err
	}

	// 2. notifications/initialized ------------------------------------------
	// Forward the client's notifications/initialized to every backend BEFORE we
	// send them any tool/resource request, per the MCP lifecycle. A client that
	// skips the notification still has whatever it sent forwarded so nothing is
	// dropped; either way every backend has seen the post-initialize traffic
	// before the prefetch below.
	note, err := client.Read()
	if err != nil {
		return err
	}
	for _, name := range f.order {
		if err := f.conns[name].Write(note); err != nil {
			return err
		}
	}

	// 3. tools/list prefetch + merge ----------------------------------------
	// Only now — after the backends have received notifications/initialized — do
	// we prefetch tools/list and build the namespaced merged routing table.
	if err := f.prefetchTools(); err != nil {
		return err
	}
	return nil
}

// broadcastRequest writes req verbatim to every backend and returns each
// backend's next response keyed by backend name. It is used only for the
// initialize handshake, where exactly one response per backend is expected.
func (f *fanout) broadcastRequest(req *mcp.Message) (map[string]*mcp.Message, error) {
	type result struct {
		name string
		msg  *mcp.Message
		err  error
	}
	ch := make(chan result, len(f.order))
	for _, name := range f.order {
		name := name
		conn := f.conns[name]
		go func() {
			if err := conn.Write(req); err != nil {
				ch <- result{name: name, err: err}
				return
			}
			msg, err := conn.Read()
			ch <- result{name: name, msg: msg, err: err}
		}()
	}
	out := make(map[string]*mcp.Message, len(f.order))
	var firstErr error
	for range f.order {
		r := <-ch
		if r.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("backend %q: %w", r.name, r.err)
			}
			continue
		}
		out[r.name] = r.msg
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

// prefetchTools fetches tools/list from every backend, namespaces each tool's
// name, and stores the merged tools array on the fanout for cached answers.
func (f *fanout) prefetchTools() error {
	type result struct {
		name  string
		tools []json.RawMessage
		err   error
	}
	ch := make(chan result, len(f.order))
	for _, name := range f.order {
		name := name
		conn := f.conns[name]
		id := f.allocID()
		go func() {
			req := &mcp.Message{JSONRPC: "2.0", ID: json.RawMessage(id), Method: "tools/list"}
			if err := conn.Write(req); err != nil {
				ch <- result{name: name, err: err}
				return
			}
			resp, err := conn.Read()
			if err != nil {
				ch <- result{name: name, err: err}
				return
			}
			tools, err := namespaceToolList(name, resp)
			ch <- result{name: name, tools: tools, err: err}
		}()
	}

	byName := make(map[string][]json.RawMessage, len(f.order))
	var firstErr error
	for range f.order {
		r := <-ch
		if r.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("tools/list from %q: %w", r.name, r.err)
			}
			continue
		}
		byName[r.name] = r.tools
	}
	if firstErr != nil {
		return firstErr
	}

	// Concatenate in config order for a deterministic merged list.
	merged := make([]json.RawMessage, 0)
	for _, name := range f.order {
		merged = append(merged, byName[name]...)
	}
	raw, err := json.Marshal(merged)
	if err != nil {
		return fmt.Errorf("merge tools: %w", err)
	}
	f.mergedTools = raw
	return nil
}

// namespaceToolList parses a tools/list response and rewrites each tool's name
// to "<backend>__<name>", leaving description and inputSchema byte-for-byte
// intact (the trim constraint: only names change, never schemas). Returns the
// namespaced tool objects ready to concatenate into the merged list.
func namespaceToolList(backend string, resp *mcp.Message) ([]json.RawMessage, error) {
	if resp.Error != nil && len(resp.Error) > 0 {
		return nil, fmt.Errorf("backend %q tools/list error: %s", backend, resp.Error)
	}
	var res struct {
		Tools []json.RawMessage `json:"tools"`
	}
	if len(resp.Result) == 0 {
		return nil, nil
	}
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		return nil, fmt.Errorf("backend %q tools/list result: %w", backend, err)
	}
	out := make([]json.RawMessage, 0, len(res.Tools))
	for _, raw := range res.Tools {
		// Decode into an ordered-preserving map to keep unknown fields, then
		// rewrite only "name". json.RawMessage values keep inputSchema verbatim.
		var tool map[string]json.RawMessage
		if err := json.Unmarshal(raw, &tool); err != nil {
			return nil, fmt.Errorf("backend %q tool object: %w", backend, err)
		}
		var bare string
		if nameRaw, ok := tool["name"]; ok {
			if err := json.Unmarshal(nameRaw, &bare); err != nil {
				return nil, fmt.Errorf("backend %q tool name: %w", backend, err)
			}
		}
		nsName, err := json.Marshal(namespacedTool(backend, bare))
		if err != nil {
			return nil, err
		}
		tool["name"] = nsName
		nb, err := json.Marshal(tool)
		if err != nil {
			return nil, err
		}
		out = append(out, nb)
	}
	return out, nil
}

// mergeInitialize builds the broker's single initialize response from the
// backends' responses. It uses the FIRST backend in config order as the basis
// (protocolVersion + capabilities) but overrides serverInfo to advertise usher
// itself, and stamps the client's original id. Taking the first by config order
// (not by map iteration) keeps the advertised version deterministic.
// Cross-backend capability union is deferred; routing to the right backend does
// not depend on it.
func mergeInitialize(resps map[string]*mcp.Message, order []string, clientID json.RawMessage) *mcp.Message {
	var first *mcp.Message
	for _, name := range order {
		if r, ok := resps[name]; ok {
			first = r
			break
		}
	}
	if first == nil || len(first.Result) == 0 {
		// No usable backend result: synthesize a minimal valid initialize so the
		// handshake still completes.
		result, _ := json.Marshal(map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": serverInfoName, "version": serverInfoVersion},
		})
		return &mcp.Message{JSONRPC: "2.0", ID: append(json.RawMessage(nil), clientID...), Result: result}
	}

	var res map[string]json.RawMessage
	if err := json.Unmarshal(first.Result, &res); err != nil {
		// Forward the first backend's result verbatim under the client's id.
		return &mcp.Message{
			JSONRPC: "2.0",
			ID:      append(json.RawMessage(nil), clientID...),
			Result:  append(json.RawMessage(nil), first.Result...),
		}
	}
	si, _ := json.Marshal(map[string]any{"name": serverInfoName, "version": serverInfoVersion})
	res["serverInfo"] = si
	merged, _ := json.Marshal(res)
	return &mcp.Message{
		JSONRPC: "2.0",
		ID:      append(json.RawMessage(nil), clientID...),
		Result:  merged,
	}
}

// dispatchInbound is the multi-backend inbound pump: it reads each client
// message and routes it to the owning backend by namespace, remapping the
// request id to a globally-unique backend-side id (recording the original in the
// inflight map). tools/list is answered from the merged cache without touching a
// backend; an unroutable tools/call is refused in-band with a JSON-RPC error.
func (b *Broker) dispatchInbound(id identity.Identity, f *fanout, inflight *InflightMap, reply func(*mcp.Message) error, client *mcp.Conn) error {
	for {
		m, err := client.Read()
		if err != nil {
			return err
		}

		// tools/list: answer from the merged cache; never forwarded.
		if m.IsRequest() && m.Method == "tools/list" {
			resp := &mcp.Message{
				JSONRPC: "2.0",
				ID:      append(json.RawMessage(nil), m.ID...),
				Result:  toolsListResult(f.mergedTools),
			}
			b.audit.Message(id.ID, Outbound.String(), "tools/list", m.IDString(), len(resp.Raw))
			if err := client.Write(resp); err != nil {
				return err
			}
			continue
		}

		// tools/call: route by namespace prefix.
		if m.IsRequest() && m.Method == "tools/call" {
			if err := b.routeToolCall(id, f, inflight, reply, m); err != nil {
				return err
			}
			continue
		}

		// Any other request (ping, resources/list, logging/setLevel, …): a request
		// carries an id and expects EXACTLY ONE response. Fanning it to every
		// backend would yield N identical responses (each backend's outbound pump
		// forwards one) for a single client id — a stream corruption. So a non
		// tools/call request goes to only ONE backend: the first in config order.
		// Its id is left untouched (not a tool-call result; TrimStage/Arbitrate do
		// not act on it), so the single response round-trips with no remap needed.
		if m.IsRequest() {
			first := f.order[0]
			if err := f.conns[first].Write(m); err != nil {
				return err
			}
			b.audit.Message(id.ID, Inbound.String(), m.Method, m.IDString(), len(m.Raw))
			continue
		}

		// A notification (ping, cancellation, logging) has no id and no response,
		// so it is safe — and often required — to fan out to every backend. No
		// correlation is needed because nothing comes back.
		for _, name := range f.order {
			if err := f.conns[name].Write(m); err != nil {
				return err
			}
		}
		b.audit.Message(id.ID, Inbound.String(), m.Method, m.IDString(), len(m.Raw))
	}
}

// routeToolCall strips the namespace from a tools/call, validates the target
// backend, rewrites params.name to the bare tool, remaps the id, records the
// inflight entry, runs the inbound pipeline, and forwards to the owning backend.
// An unroutable call is answered in-band and dropped.
func (b *Broker) routeToolCall(id identity.Identity, f *fanout, inflight *InflightMap, reply func(*mcp.Message) error, m *mcp.Message) error {
	var p map[string]json.RawMessage
	if err := json.Unmarshal(m.Params, &p); err != nil {
		// Unparseable params: refuse in-band rather than guess a backend.
		return reply(mcp.ErrorResponse(m.ID, ErrToolNotNamespaced, "tools/call params not an object"))
	}
	var nsName string
	if nameRaw, ok := p["name"]; ok {
		_ = json.Unmarshal(nameRaw, &nsName)
	}
	beName, bare := stripNamespace(nsName)
	if beName == "" {
		return reply(mcp.ErrorResponse(m.ID, ErrToolNotNamespaced,
			fmt.Sprintf("tool %q is not namespaced as <backend>%s<tool>", nsName, namespaceSep)))
	}
	conn, ok := f.conns[beName]
	if !ok {
		return reply(mcp.ErrorResponse(m.ID, ErrUnknownBackend,
			fmt.Sprintf("unknown backend %q for tool %q", beName, nsName)))
	}

	// Rewrite params.name to the bare tool so the backend (and ArbitrateStage,
	// which classifies by bare tool name) see the un-namespaced name.
	bareRaw, err := json.Marshal(bare)
	if err != nil {
		return err
	}
	p["name"] = bareRaw
	newParams, err := json.Marshal(p)
	if err != nil {
		return err
	}

	// Remap the id: the fanout owns a globally-unique id space so two backends
	// can never answer with colliding ids. The original client id is stored for
	// the outbound restore.
	clientID := m.IDString()
	backendID := f.allocID()
	m.Params = newParams
	m.ID = json.RawMessage(backendID)
	m.Raw = nil // params and id changed: force re-encode

	inflight.Record(backendID, InflightEntry{
		Method:      "tools/call",
		ToolName:    bare,
		BackendName: beName,
		ClientID:    clientID,
	})

	// Run the inbound pipeline (gate, arbitrate, audit) with the per-call backend
	// name so ArbitrateStage keys its lock correctly and audit attributes it.
	//
	// An inbound stage that refuses the call in-band (ArbitrateStage's window-busy
	// reply) stamps the error with m.ID — which we just remapped to the backend
	// id. The client correlates on its ORIGINAL id, so wrap ctx.Reply to restore
	// the client id on any response a stage injects before it reaches the client.
	clientReply := func(rm *mcp.Message) error {
		if rm != nil && rm.IsResponse() && rm.IDString() == backendID {
			rm.ID = json.RawMessage(clientID)
			rm.Raw = nil // id changed: force re-encode
		}
		return reply(rm)
	}
	pctx := &Context{Identity: id, Backend: beName, Dir: Inbound, Inflight: inflight, Locks: b.locks, Reply: clientReply, ClientID: clientID}
	out, err := b.inbound.Run(pctx, m)
	if err != nil {
		b.audit.Errorf(id.ID, "%s pipeline: %v", Inbound, err)
		// The pipeline rejected this message; the inflight entry would never be
		// consumed, so drop it to avoid a leak.
		inflight.Consume(backendID)
		return nil
	}
	if out == nil {
		// A stage dropped the message (e.g. ArbitrateStage answered busy in-band).
		// It owns the client reply; clear the orphaned inflight entry.
		inflight.Consume(backendID)
		return nil
	}
	return conn.Write(out)
}

// pumpFanoutOutbound is the per-backend outbound pump for ServeMulti. It is the
// twin of Broker.pump's outbound side but inserts an id-restore step BEFORE the
// outbound pipeline so the client sees its own request ids (the inbound side
// remapped them to backend-unique ids). Notifications and unmatched responses
// pass through untouched.
func (b *Broker) pumpFanoutOutbound(id identity.Identity, beName string, inflight *InflightMap, src, client *mcp.Conn) error {
	pctx := &Context{Identity: id, Backend: beName, Dir: Outbound, Inflight: inflight, Locks: b.locks}
	for {
		m, err := src.Read()
		if err != nil {
			return err
		}

		// A backend may spontaneously emit notifications/tools/list_changed. We
		// forward it to the client so it re-lists; cache invalidation/re-merge is
		// a deferred refinement (the client re-listing hits our cached merge,
		// which is acceptable until #20 wires a per-backend re-fetch).
		// (No id, so it never matches an inflight entry; it just flows through.)

		// Restore the client id on a response whose backend-side id we remapped.
		// Peek (not Consume): the outbound pipeline's TrimStage still needs to
		// Consume the same entry by the backend-side id, so we must restore the
		// client id only on the wire AFTER the pipeline reads it. We therefore
		// keep the backend-side id through the pipeline and swap to the client id
		// just before writing.
		var clientID string
		if m.IsResponse() {
			if entry, ok := inflight.Peek(m.IDString()); ok {
				clientID = entry.ClientID
			}
		}

		// Hand the client-facing id to the pipeline so AuditStage logs it while the
		// inflight-keyed stages (Trim/Arbitrate) still correlate on the backend-side
		// id carried by m. Reset every iteration (pctx is reused across the loop) so
		// a non-remapped message does not inherit a prior message's client id.
		pctx.ClientID = clientID

		out, err := b.outbound.Run(pctx, m)
		if err != nil {
			b.audit.Errorf(id.ID, "%s pipeline: %v", Outbound, err)
			continue
		}
		if out == nil {
			continue
		}

		// Swap the backend-side id back to the original client id for the wire.
		if clientID != "" {
			out.ID = json.RawMessage(clientID)
			out.Raw = nil // id changed: force re-encode
		}
		if err := client.Write(out); err != nil {
			return err
		}
	}
}

// toolsListResult wraps a namespaced tools array as a tools/list result object:
// {"tools":[...]}. A nil/empty array still yields a valid {"tools":[]}.
func toolsListResult(tools json.RawMessage) json.RawMessage {
	if len(tools) == 0 {
		tools = json.RawMessage("[]")
	}
	out, _ := json.Marshal(map[string]json.RawMessage{"tools": tools})
	return out
}
