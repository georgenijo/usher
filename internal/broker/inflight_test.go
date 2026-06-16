package broker

import (
	"sync"
	"testing"

	"github.com/georgenijo/usher/internal/mcp"
)

func TestInflightMap_RecordConsume(t *testing.T) {
	f := NewInflightMap()

	// Unknown id: miss.
	if _, ok := f.Consume("1"); ok {
		t.Fatal("Consume of an unrecorded id should miss")
	}

	f.Record("7", InflightEntry{Method: "tools/call", ToolName: "get_window_state"})
	got, ok := f.Consume("7")
	if !ok {
		t.Fatal("Consume of recorded id missed")
	}
	if got.Method != "tools/call" || got.ToolName != "get_window_state" {
		t.Errorf("entry = %+v, want tools/call/get_window_state", got)
	}

	// Consume removes: a second Consume misses (prevents unbounded growth).
	if _, ok := f.Consume("7"); ok {
		t.Error("double Consume should miss after the entry is removed")
	}
}

func TestInflightMap_EmptyIDIgnored(t *testing.T) {
	f := NewInflightMap()
	f.Record("", InflightEntry{Method: "notifications/initialized"})
	if _, ok := f.Consume(""); ok {
		t.Error("empty id must never be stored or consumed")
	}
}

func TestInflightMap_StringID(t *testing.T) {
	// IDString renders a string id with its quotes; Record/Consume must use the
	// same rendering. Here we simulate that with the quoted key.
	f := NewInflightMap()
	f.Record(`"abc"`, InflightEntry{Method: "tools/list"})
	if _, ok := f.Consume(`"abc"`); !ok {
		t.Error("quoted string id should round-trip")
	}
}

func TestInflightMap_Concurrent(t *testing.T) {
	// Run with -race: the two pump goroutines hit this map simultaneously.
	f := NewInflightMap()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		id := itoa(i)
		wg.Add(2)
		go func() { defer wg.Done(); f.Record(id, InflightEntry{Method: "tools/call"}) }()
		go func() { defer wg.Done(); f.Consume(id) }()
	}
	wg.Wait()
}

func TestToolNameIf(t *testing.T) {
	call := &mcp.Message{Method: "tools/call", Params: []byte(`{"name":"get_window_state","arguments":{}}`)}
	if got := toolNameIf(call); got != "get_window_state" {
		t.Errorf("toolNameIf(tools/call) = %q, want get_window_state", got)
	}
	list := &mcp.Message{Method: "tools/list"}
	if got := toolNameIf(list); got != "" {
		t.Errorf("toolNameIf(tools/list) = %q, want empty", got)
	}
	bad := &mcp.Message{Method: "tools/call", Params: []byte(`not json`)}
	if got := toolNameIf(bad); got != "" {
		t.Errorf("toolNameIf(bad params) = %q, want empty", got)
	}
}
