package broker

import "strings"

// namespaceSep separates a backend name from a tool name in a namespaced tool
// (e.g. "cua__click", "fs__read_file"). Double-underscore is chosen because MCP
// tool names use single underscores internally, so the separator can never be a
// substring of a bare cua-driver or filesystem-server tool name; the split is
// therefore unambiguous. Changing the convention is a single-site edit.
const namespaceSep = "__"

// namespacedTool returns the tool name as the broker exposes it to the client:
// backend "cua", tool "click" -> "cua__click". Multi-backend aggregation
// prefixes every backend's tools this way so two backends that both expose a
// "click" never collide (#17).
func namespacedTool(backend, tool string) string {
	return backend + namespaceSep + tool
}

// stripNamespace splits a client-visible tool name back into its backend and
// bare tool, e.g. "cua__click" -> ("cua", "click"). It splits on the FIRST
// separator so a bare tool name may itself contain "__" (the backend name,
// chosen at registration, never does). A name with no separator returns
// ("", name): the caller treats that as unroutable and answers with a JSON-RPC
// error rather than guessing a backend.
func stripNamespace(name string) (backend, tool string) {
	idx := strings.Index(name, namespaceSep)
	if idx < 0 {
		return "", name
	}
	return name[:idx], name[idx+len(namespaceSep):]
}
