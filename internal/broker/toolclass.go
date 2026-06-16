package broker

import "encoding/json"

// toolclass.go classifies cua-driver tools into MUTATING (need a per-window
// write-lock) vs READ-ONLY (never gated) and extracts the lock key from a
// tools/call request's arguments. The classification is the ground truth from
// the cua-driver Rust source (ToolDef.read_only): a read-only tool can never
// change a window's state, so ArbitrateStage forwards it untouched; a mutating
// tool acquires the lock for its target window before the broker forwards it.
//
// Window targeting lives one level down, in params.arguments (the MCP tools/call
// envelope wraps the tool's own args), as integer pid and window_id. A tool with
// pid but no window_id (or a malformed window_id of 0) locks the whole process
// via the wildcardWindow sentinel, so an action whose target window we cannot
// name still serialises against everything in that pid rather than slipping
// through ungated.

// readOnlyTools never mutate a window; ArbitrateStage forwards them without ever
// touching the lock registry. Mirrors ToolDef.read_only:true in the cua-driver
// macOS platform crate plus the core read tools. zoom is here despite taking a
// window_id: it only stores a per-pid crop context (read_only:true upstream) and
// changes no UI. check_permissions is windowless (may raise a modal, but targets
// no window), so it is ungated too.
var readOnlyTools = map[string]bool{
	"list_apps":              true,
	"list_windows":           true,
	"get_window_state":       true,
	"get_screen_size":        true,
	"get_cursor_position":    true,
	"move_cursor":            true, // overlay only, no app-state change
	"get_config":             true,
	"get_accessibility_tree": true,
	"zoom":                   true, // stores crop context only; no UI mutation
	"get_agent_cursor_state": true,
	"get_recording_state":    true,
	"check_permissions":      true, // windowless; no lock target
	"check_for_update":       true,
}

// windowlessMutators change state but target no window, so they take no lock:
// session/meta tools, the agent-cursor setters, recording control, and the
// idempotent launch_app (its target window only exists in the RESPONSE).
// bring_to_front is a macOS stub that always errors — skip locking it.
var windowlessMutators = map[string]bool{
	"launch_app":               true,
	"bring_to_front":           true, // macOS stub: always errors
	"start_session":            true,
	"end_session":              true,
	"set_config":               true,
	"set_agent_cursor_enabled": true,
	"set_agent_cursor_motion":  true,
	"set_agent_cursor_style":   true,
	"start_recording":          true,
	"stop_recording":           true,
	"replay_trajectory":        true, // self-serialising replay
	"install_ffmpeg":           true,
}

// lockDecision is the result of classifying a tools/call: whether it needs a
// write-lock and, if so, on which window key.
type lockDecision struct {
	needsLock bool
	key       windowKey
}

// classifyToolCall decides the lock for a tools/call by tool name and arguments.
// tool is params.name; args is params.arguments (may be nil/empty). A read-only
// or windowless tool returns needsLock=false. A mutating tool returns the
// (pid, window_id) key, falling back to the whole-process (pid, wildcard) key
// when window_id is absent or malformed (0). A mutating tool with no parseable
// pid cannot be window-targeted and is left ungated rather than guessed.
func classifyToolCall(tool string, args json.RawMessage) lockDecision {
	if tool == "" || readOnlyTools[tool] || windowlessMutators[tool] {
		return lockDecision{needsLock: false}
	}
	// Everything else is a window-targeted mutator (click, double_click,
	// right_click, type_text, press_key, hotkey, scroll, drag, set_value, page,
	// kill_app, and any future mutating tool that carries a pid). An unknown tool
	// is treated as mutating-by-default: gating an unrecognised call is safe,
	// letting it race is not.
	pid, okPid := jsonInt(args, "pid")
	if !okPid {
		// No pid to target (or no arguments at all). Cannot form a window key;
		// forward ungated rather than invent one.
		return lockDecision{needsLock: false}
	}
	win, okWin := jsonInt(args, "window_id")
	if !okWin || win == 0 {
		// Missing or malformed window_id: lock the whole process so the action
		// still serialises against every window of this pid (kill_app, and the
		// focused-element path of type_text/press_key/hotkey/scroll).
		return lockDecision{needsLock: true, key: windowKey{pid: pid, windowID: wildcardWindow}}
	}
	return lockDecision{needsLock: true, key: windowKey{pid: pid, windowID: win}}
}

// jsonInt reads an integer field from a JSON object by key. It accepts a JSON
// number (Go unmarshals these as float64) and coerces to int64 — the cua-driver
// sends pid (i32) and window_id (u32) as JSON integers. ok is false when the key
// is absent or not a number, so the caller can distinguish "no window_id" from
// "window_id == 0".
func jsonInt(obj json.RawMessage, key string) (int64, bool) {
	if len(obj) == 0 {
		return 0, false
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(obj, &m); err != nil {
		return 0, false
	}
	raw, ok := m[key]
	if !ok {
		return 0, false
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err != nil {
		return 0, false
	}
	return int64(f), true
}
