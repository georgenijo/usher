package broker

import (
	"encoding/json"
	"testing"
)

// TestClassifyToolCall covers the lock decision for every classification branch:
// read-only tools are ungated, mutating tools lock their (pid, window_id), a
// missing/zero window_id falls back to the whole-process key, a missing pid is
// ungated, and an unknown tool is gated by default.
func TestClassifyToolCall(t *testing.T) {
	cases := []struct {
		name     string
		tool     string
		args     string
		wantLock bool
		wantPid  int64
		wantWin  int64
	}{
		// Read-only: never locked, even when they carry pid/window_id.
		{"get_window_state read-only", "get_window_state", `{"pid":111,"window_id":222}`, false, 0, 0},
		{"list_windows read-only", "list_windows", `{"pid":111}`, false, 0, 0},
		{"zoom read-only despite window_id", "zoom", `{"pid":111,"window_id":222}`, false, 0, 0},
		{"move_cursor read-only", "move_cursor", `{"pid":111,"window_id":222}`, false, 0, 0},
		{"check_permissions windowless read", "check_permissions", `{}`, false, 0, 0},

		// Windowless mutators: state-changing but no window target.
		{"launch_app windowless", "launch_app", `{"app":"Calculator"}`, false, 0, 0},
		{"end_session meta", "end_session", `{"session":"s1"}`, false, 0, 0},
		{"start_recording meta", "start_recording", `{}`, false, 0, 0},
		{"bring_to_front stub", "bring_to_front", `{"pid":111,"window_id":222}`, false, 0, 0},

		// Window-targeted mutators: lock (pid, window_id).
		{"click both ids", "click", `{"pid":111,"window_id":222,"element_index":3}`, true, 111, 222},
		{"set_value both ids", "set_value", `{"pid":7,"window_id":9,"element_index":1,"value":"x"}`, true, 7, 9},
		{"page both ids", "page", `{"pid":7,"window_id":9}`, true, 7, 9},

		// Optional window_id absent: whole-process (pid, wildcard) lock.
		{"type_text no window_id", "type_text", `{"pid":42,"text":"hi"}`, true, 42, wildcardWindow},
		{"press_key no window_id", "press_key", `{"pid":42,"key":"a"}`, true, 42, wildcardWindow},
		{"scroll no window_id", "scroll", `{"pid":42,"direction":"down"}`, true, 42, wildcardWindow},

		// kill_app: pid only, whole-process.
		{"kill_app pid only", "kill_app", `{"pid":99}`, true, 99, wildcardWindow},

		// Malformed window_id of 0 is treated as absent: whole-process.
		{"window_id zero is malformed", "click", `{"pid":5,"window_id":0}`, true, 5, wildcardWindow},

		// No pid: cannot target a window, forwarded ungated.
		{"click without pid", "click", `{"element_index":3}`, false, 0, 0},
		{"empty args", "click", `{}`, false, 0, 0},

		// Unknown tool defaults to mutating (gated when it has a pid).
		{"unknown tool with pid", "some_new_tool", `{"pid":3,"window_id":4}`, true, 3, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dec := classifyToolCall(tc.tool, json.RawMessage(tc.args))
			if dec.needsLock != tc.wantLock {
				t.Fatalf("needsLock = %v, want %v", dec.needsLock, tc.wantLock)
			}
			if !tc.wantLock {
				return
			}
			if dec.key.pid != tc.wantPid || dec.key.windowID != tc.wantWin {
				t.Errorf("key = (%d,%d), want (%d,%d)", dec.key.pid, dec.key.windowID, tc.wantPid, tc.wantWin)
			}
		})
	}
}

// TestClassifyToolCall_EmptyTool guards the empty/absent tool name.
func TestClassifyToolCall_EmptyTool(t *testing.T) {
	if classifyToolCall("", json.RawMessage(`{"pid":1,"window_id":2}`)).needsLock {
		t.Error("empty tool name must not be locked")
	}
}

// TestJSONInt verifies integer coercion (JSON numbers arrive as float64) and the
// absent/non-number cases the lock-key derivation depends on.
func TestJSONInt(t *testing.T) {
	args := json.RawMessage(`{"pid":12345,"window_id":67890.0,"name":"click","flag":true}`)
	if v, ok := jsonInt(args, "pid"); !ok || v != 12345 {
		t.Errorf("pid = (%d,%v), want (12345,true)", v, ok)
	}
	// window_id arriving as a float literal must coerce to the integer value.
	if v, ok := jsonInt(args, "window_id"); !ok || v != 67890 {
		t.Errorf("window_id = (%d,%v), want (67890,true)", v, ok)
	}
	if _, ok := jsonInt(args, "missing"); ok {
		t.Error("absent key must report ok=false")
	}
	if _, ok := jsonInt(args, "name"); ok {
		t.Error("string field must report ok=false (not a number)")
	}
	if _, ok := jsonInt(args, "flag"); ok {
		t.Error("bool field must report ok=false")
	}
	if _, ok := jsonInt(json.RawMessage(``), "pid"); ok {
		t.Error("empty object must report ok=false")
	}
	if _, ok := jsonInt(json.RawMessage(`not json`), "pid"); ok {
		t.Error("malformed JSON must report ok=false")
	}
}
