// selftest is a built-in end-to-end smoke test of the broker. It drives the full
// MCP lifecycle (initialize, notifications/initialized, tools/list) plus one
// tools/call THROUGH the real broker (broker.New + ServeStdio) and asserts the
// responses round-trip. It is self-contained: it fronts usher's own bundled
// `mcpserver` subcommand (internal/mcpserver — echo/add/now) as the backend, runs
// in a throwaway temp state dir so it never reads or mutates the user's real
// ~/.usher, and exercises the EXISTING serve path verbatim — no pipeline, stage,
// or message handling is touched. PASS/FAIL is printed with detail and a failure
// exits non-zero (the caller in main maps the returned error to exit 1).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/georgenijo/usher/internal/broker"
	"github.com/georgenijo/usher/internal/config"
	"github.com/georgenijo/usher/internal/mcp"
)

// selftestTimeout bounds the whole smoke test so a wedged backend or a broker
// that never answers fails the command instead of hanging it. The bundled
// mcpserver starts instantly and does pure local compute, so this is generous.
const selftestTimeout = 30 * time.Second

// cmdSelftest runs the broker end-to-end against the bundled mcpserver backend in
// an isolated temp state dir and reports PASS/FAIL. A returned error is surfaced
// by main as a non-zero exit; nil means every assertion passed.
func cmdSelftest(args []string) error {
	// No flags today; reject stray args so a typo'd flag is not silently ignored.
	if len(args) > 0 {
		return fmt.Errorf("usage: usher selftest")
	}

	// The backend is THIS binary re-invoked as `usher mcpserver`. Resolving the
	// running executable means the test fronts the exact build under test, with no
	// external backend to install.
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve own executable: %w", err)
	}

	// A throwaway state dir, removed on exit, so the test never touches the user's
	// real ~/.usher. Overriding USHER_STATE_DIR routes config.DefaultPath (and the
	// socket/pid paths, unused here) into it.
	dir, err := os.MkdirTemp("", "usher-selftest-*")
	if err != nil {
		return fmt.Errorf("make temp state dir: %w", err)
	}
	defer os.RemoveAll(dir)
	if err := os.Setenv("USHER_STATE_DIR", dir); err != nil {
		return fmt.Errorf("set temp state dir: %w", err)
	}

	if err := runSelftest([]string{self, "mcpserver"}, dir); err != nil {
		fmt.Printf("usher selftest: FAIL: %v\n", err)
		return fmt.Errorf("selftest failed")
	}
	fmt.Println("usher selftest: PASS — handshake + tools/call round-tripped through the broker")
	return nil
}

// runSelftest builds a broker over the temp config and drives a full MCP session
// through ServeStdio, asserting each response. backendCmd is the argv for the
// bundled mcpserver backend (normally [<this binary> mcpserver]; the test injects
// a re-exec wrapper); dir is the temp state dir the config is written into.
func runSelftest(backendCmd []string, dir string) error {
	// Write a config whose only backend is the bundled mcpserver. config.Save lands
	// it at DefaultPath inside the temp state dir we set above, so broker.New /
	// ServeStdio resolve exactly this one.
	cfg := &config.Config{
		Backends: []config.Backend{{
			Name:      "selftest",
			Transport: "stdio",
			Command:   backendCmd,
			Auth:      "inherit",
			Default:   true,
		}},
	}
	if err := cfg.Save(filepath.Join(dir, "config.json")); err != nil {
		return fmt.Errorf("write temp config: %w", err)
	}

	b, err := broker.New(cfg)
	if err != nil {
		return fmt.Errorf("build broker: %w", err)
	}

	// Two pipes form the agent<->broker link: clientW feeds the broker's stdin
	// (client->broker), brokerW feeds our reader (broker->client). This is exactly
	// the wiring an agent that spawned `usher serve` gets, so ServeStdio runs its
	// real path unmodified.
	clientR, clientW := io.Pipe() // we write requests -> broker reads
	brokerR, brokerW := io.Pipe() // broker writes responses -> we read

	ctx, cancel := context.WithTimeout(context.Background(), selftestTimeout)
	defer cancel()

	serveErr := make(chan error, 1)
	go func() {
		// ServeStdio returns when either side closes; closing clientW (below) gives
		// it a clean client-EOF teardown after our last assertion.
		serveErr <- b.ServeStdio(ctx, "selftest", clientR, brokerW)
	}()

	client := mcp.NewConn(brokerR, clientW)

	// A single read goroutine keeps Read off the main path so a broker that never
	// answers is bounded by ctx rather than blocking forever; expect() selects on
	// it against the deadline.
	type readResult struct {
		m   *mcp.Message
		err error
	}
	reads := make(chan readResult, 1)
	readNext := func() { go func() { m, e := client.Read(); reads <- readResult{m, e} }() }
	expect := func(what string) (*mcp.Message, error) {
		readNext()
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%s: timed out waiting for response: %w", what, ctx.Err())
		case r := <-reads:
			if r.err != nil {
				return nil, fmt.Errorf("%s: read response: %w", what, r.err)
			}
			return r.m, nil
		}
	}

	// --- initialize ---------------------------------------------------------
	if err := client.Write(&mcp.Message{
		JSONRPC: "2.0",
		ID:      json.RawMessage("1"),
		Method:  "initialize",
		Params:  json.RawMessage(`{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"usher-selftest","version":"` + version + `"}}`),
	}); err != nil {
		return fmt.Errorf("write initialize: %w", err)
	}
	initResp, err := expect("initialize")
	if err != nil {
		return err
	}
	if initResp.IDString() != "1" {
		return fmt.Errorf("initialize: response id %q, want \"1\"", initResp.IDString())
	}
	if len(initResp.Error) > 0 {
		return fmt.Errorf("initialize: backend returned error: %s", initResp.Error)
	}
	if len(initResp.Result) == 0 {
		return fmt.Errorf("initialize: empty result (backend did not handshake)")
	}

	// --- notifications/initialized (no response expected) -------------------
	if err := client.Write(&mcp.Message{JSONRPC: "2.0", Method: "notifications/initialized"}); err != nil {
		return fmt.Errorf("write notifications/initialized: %w", err)
	}

	// --- tools/list ---------------------------------------------------------
	if err := client.Write(&mcp.Message{
		JSONRPC: "2.0",
		ID:      json.RawMessage("2"),
		Method:  "tools/list",
	}); err != nil {
		return fmt.Errorf("write tools/list: %w", err)
	}
	listResp, err := expect("tools/list")
	if err != nil {
		return err
	}
	if listResp.IDString() != "2" {
		return fmt.Errorf("tools/list: response id %q, want \"2\"", listResp.IDString())
	}
	if len(listResp.Error) > 0 {
		return fmt.Errorf("tools/list: backend returned error: %s", listResp.Error)
	}
	var list struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(listResp.Result, &list); err != nil {
		return fmt.Errorf("tools/list: decode result: %w", err)
	}
	if !hasTool(list.Tools, "add") {
		return fmt.Errorf("tools/list: expected the bundled \"add\" tool, got %d tool(s)", len(list.Tools))
	}

	// --- tools/call: add(2,3) -> "5" ----------------------------------------
	if err := client.Write(&mcp.Message{
		JSONRPC: "2.0",
		ID:      json.RawMessage("3"),
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"add","arguments":{"a":2,"b":3}}`),
	}); err != nil {
		return fmt.Errorf("write tools/call: %w", err)
	}
	callResp, err := expect("tools/call")
	if err != nil {
		return err
	}
	if callResp.IDString() != "3" {
		return fmt.Errorf("tools/call: response id %q, want \"3\"", callResp.IDString())
	}
	if len(callResp.Error) > 0 {
		return fmt.Errorf("tools/call: backend returned error: %s", callResp.Error)
	}
	got, err := toolResultText(callResp.Result)
	if err != nil {
		return fmt.Errorf("tools/call: %w", err)
	}
	if got != "5" {
		return fmt.Errorf("tools/call add(2,3): got %q, want \"5\"", got)
	}

	// Half-close the client side; ServeStdio drains and returns a clean teardown.
	_ = clientW.Close()
	select {
	case <-ctx.Done():
		return fmt.Errorf("broker did not shut down: %w", ctx.Err())
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("broker exited with error: %w", err)
		}
	}
	return nil
}

// hasTool reports whether the named tool is present in a tools/list result.
func hasTool(tools []struct {
	Name string `json:"name"`
}, name string) bool {
	for _, t := range tools {
		if t.Name == name {
			return true
		}
	}
	return false
}

// toolResultText pulls the first text content item out of an MCP tools/call
// result ({content:[{type:"text",text:...}]}), the shape the bundled mcpserver
// returns. An isError:true result (a tool-level failure) is reported as an error.
func toolResultText(result json.RawMessage) (string, error) {
	var r struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(result, &r); err != nil {
		return "", fmt.Errorf("decode tool result: %w", err)
	}
	if r.IsError {
		return "", fmt.Errorf("tool reported an error result")
	}
	for _, c := range r.Content {
		if c.Type == "text" {
			return c.Text, nil
		}
	}
	return "", fmt.Errorf("tool result had no text content")
}
