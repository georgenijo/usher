package control

// embed.go bundles the single-page web UI into the usher binary so the daemon
// serves it with zero external assets — no Node build, no JS dependencies, no
// separate file to ship. index.html is a self-contained page (vanilla HTML/CSS/JS)
// that opens an EventSource on /api/events for live updates and POSTs to the
// /api/backends/{name}/{action} routes for management.

import (
	_ "embed"
	"net/http"
)

//go:embed ui/index.html
var indexHTML []byte

// handleIndex serves the embedded single-page UI at GET /. It is the only HTML
// route; everything else is JSON or the SSE stream the page consumes. ServeMux's
// "GET /" pattern is the catch-all for GET, so an unknown path lands here and gets
// the app shell (the page's JS then drives the API) rather than a 404 — fine for a
// single-page app with no deep links.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	// Only the exact root serves the page; any other unmatched GET path is a 404 so
	// a typo (e.g. /api/backend) is a clear miss, not a silently-served shell.
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(indexHTML)
}
