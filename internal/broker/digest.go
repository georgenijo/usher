package broker

import (
	"regexp"
	"strconv"
	"strings"
)

// digest.go is a Go port of GhostHands' Python AX-tree compaction
// (src/ghosthands/ax.py + ownloop.actionable_digest + compaction._digest_text).
// A raw get_window_state snapshot is a multi-KB markdown AX tree, mostly
// menu-bar subtrees and non-actionable chrome; the brain acts on only a few
// lines. This collapses the snapshot to an actionable BUTTONS list plus the
// DISPLAY value nodes, matching the funnel GhostHands runs before every prompt
// so the broker-side trim and the loop agree byte-for-byte on a native tree.

// axElement is one parsed AX node. ax_id distinguishes absent (hasID=false)
// from present-but-empty ("id=" with no value), which Python models as
// None vs "" — only a non-empty id participates in dedup and the id= tag.
type axElement struct {
	role    string
	index   int
	hasIdx  bool // false for non-actionable nodes (no [N] tag)
	title   string
	label   string
	value   string
	hasVal  bool // distinguishes a real "" value from an absent one
	axID    string
	hasID   bool
	depth   int
	subtree int
}

// text is the best human-readable name for matching: title, else label, else
// value, else "". Mirrors Element.text in ax.py.
func (e *axElement) text() string {
	if e.title != "" {
		return e.title
	}
	if e.label != "" {
		return e.label
	}
	if e.value != "" {
		return e.value
	}
	return ""
}

var (
	lineRE  = regexp.MustCompile(`^(\s*)- (?:\[(\d+)\]\s+)?(AX\w+)(.*)$`)
	valueRE = regexp.MustCompile(`^\s*=\s*"(.*?)"`)
	titleRE = regexp.MustCompile(`^\s*"(.*?)"`)
	labelRE = regexp.MustCompile(`^\(([^)]*)\)`)
	// attrsRE: Go's RE2 has no lookahead, so the id=/help=/actions= prefix is
	// kept INSIDE the capture group (Python uses a zero-width lookahead and a
	// separate group). axIDRE then finds id= within that captured block.
	attrsRE = regexp.MustCompile(`(?s)\s*\[((?:id=|help=|actions=).*)\]\s*$`)
	axIDRE  = regexp.MustCompile(`(?s)id=(.*?)(?:\s+help=|\s+actions=|$)`)
)

// menuRoles are dropped from the digest: a backgrounded app's menu items are
// disabled no-ops, and they bloat the tree.
var menuRoles = map[string]bool{
	"AXMenuBar": true, "AXMenuBarItem": true, "AXMenu": true, "AXMenuItem": true,
}

// actionableRoles are the roles a brain can act on; used ONLY in web_scope to
// drop a browser tree's structural nodes (AXWebArea/AXHeading/AXList/...). The
// native path keeps every indexed node, so this filter is web-only.
var actionableRoles = map[string]bool{
	"AXButton": true, "AXLink": true, "AXTextField": true, "AXTextArea": true,
	"AXCheckBox": true, "AXRadioButton": true, "AXPopUpButton": true,
	"AXMenuButton": true, "AXComboBox": true, "AXSlider": true, "AXTab": true,
	"AXDisclosureTriangle": true, "AXSearchField": true, "AXIncrementor": true,
	"AXStepper": true, "AXSwitch": true, "AXToggle": true,
}

// parseTree is the line-by-line port of ax.parse_tree. Lines that do not match
// lineRE (multi-line attribute continuations) are skipped. depth is
// len(indent)/2; the subtree counter starts at -1 and increments on every
// zero-indent line, clamped to 0 for the first element.
func parseTree(markdown string) []axElement {
	var els []axElement
	subtree := -1
	for _, line := range strings.Split(markdown, "\n") {
		m := lineRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		indent, idxStr, role, rest := m[1], m[2], m[3], m[4]
		depth := len(indent) / 2
		if indent == "" {
			subtree++
		}

		el := axElement{role: role, depth: depth, subtree: max(subtree, 0)}
		if idxStr != "" {
			el.index, _ = strconv.Atoi(idxStr)
			el.hasIdx = true
		}

		if am := attrsRE.FindStringSubmatchIndex(rest); am != nil {
			attrs := rest[am[2]:am[3]]
			rest = rest[:am[0]]
			if idm := axIDRE.FindStringSubmatch(attrs); idm != nil {
				if s := strings.TrimSpace(idm[1]); s != "" {
					el.axID, el.hasID = s, true
				}
			}
		}

		if vm := valueRE.FindStringSubmatchIndex(rest); vm != nil {
			el.value, el.hasVal = rest[vm[2]:vm[3]], true
			rest = rest[vm[1]:]
		} else if tm := titleRE.FindStringSubmatchIndex(rest); tm != nil {
			el.title = rest[tm[2]:tm[3]]
			rest = rest[tm[1]:]
		}

		if lm := labelRE.FindStringSubmatch(strings.TrimLeft(rest, " ")); lm != nil {
			el.label = lm[1]
		}

		els = append(els, el)
	}
	return els
}

// webAreaMembers marks every element that is a DESCENDANT of an AXWebArea by
// indentation depth — the page content. Chrome before the web area and the
// menubar after it stay unmarked. Port of ownloop._web_area_members; the
// sentinel for "not in a web area" is untilDepth == -1.
func webAreaMembers(els []axElement) []bool {
	members := make([]bool, len(els))
	untilDepth := -1
	for i := range els {
		if untilDepth != -1 && els[i].depth <= untilDepth {
			untilDepth = -1
		}
		if untilDepth != -1 {
			members[i] = true
		}
		if els[i].role == "AXWebArea" {
			untilDepth = els[i].depth
		}
	}
	return members
}

// actionableDigest is the port of ownloop.actionable_digest. It returns the
// BUTTONS list (deduped, menu-free, capped at maxElements, cut at a second
// AXWindow) and the DISPLAY values (non-indexed value nodes, deduped, capped at
// 10). web_scope additionally drops everything outside the AXWebArea and every
// non-actionable role.
func actionableDigest(markdown string, webScope bool) (buttons, values string) {
	const maxElements = 80
	els := parseTree(markdown)

	var webMember []bool
	hasWebArea := false
	if webScope {
		webMember = webAreaMembers(els)
		for _, in := range webMember {
			if in {
				hasWebArea = true
				break
			}
		}
	}

	type dedupKey struct{ role, text, axID string }
	seen := make(map[dedupKey]bool)
	var lines []string
	windows := 0
	for i := range els {
		el := &els[i]
		if el.role == "AXWindow" {
			windows++
			if windows > 1 {
				// Everything after a second window root is the duplicate
				// window-twin subtree the daemon sometimes appends; its
				// indices go stale instantly and clicking them misfires.
				break
			}
		}
		if !el.hasIdx || menuRoles[el.role] {
			continue
		}
		if webScope {
			if !actionableRoles[el.role] {
				continue // drop AXWebArea/AXHeading/AXList/AXStaticText/chrome groups
			}
			if hasWebArea && !webMember[i] {
				continue // drop browser chrome outside the page's web area
			}
		}
		// Collapse id-pinned duplicates only. Same-named elements WITHOUT ids
		// are normal UI (five 'Learn more' links on one page) and must all stay.
		if el.hasID {
			key := dedupKey{el.role, el.text(), el.axID}
			if seen[key] {
				continue
			}
			seen[key] = true
		}
		name := el.text()
		if name == "" {
			if el.hasID {
				name = el.axID
			} else {
				name = el.role
			}
		}
		idtag := ""
		if el.hasID {
			idtag = " id=" + el.axID
		}
		lines = append(lines, "["+strconv.Itoa(el.index)+"] "+el.role+" "+pyrepr(name)+idtag)
		if len(lines) >= maxElements {
			break
		}
	}

	var vals []string
	seenVals := make(map[string]bool)
	for i := range els {
		el := &els[i]
		if el.hasIdx || !el.hasVal || el.value == "" || menuRoles[el.role] {
			continue
		}
		label := el.label
		if label == "" {
			label = el.title
		}
		if label == "" {
			label = el.role
		}
		line := "- " + label + ": " + pyrepr(el.value)
		if !seenVals[line] {
			seenVals[line] = true
			vals = append(vals, line)
		}
	}
	if len(vals) > 10 {
		vals = vals[:10]
	}

	return strings.Join(lines, "\n"), strings.Join(vals, "\n")
}

// digestText assembles the brain-facing string exactly as
// compaction._digest_text: BUTTONS header, list-or-"(none)", blank, DISPLAY
// header, values-or-"(none)". web_scope fires when the tree has an AXWebArea,
// matching the Python funnel.
func digestText(markdown string) string {
	buttons, values := actionableDigest(markdown, strings.Contains(markdown, "AXWebArea"))
	if buttons == "" {
		buttons = "(none)"
	}
	if values == "" {
		values = "(none)"
	}
	return strings.Join([]string{
		"BUTTONS (act by element_index):",
		buttons,
		"",
		"DISPLAY:",
		values,
	}, "\n")
}

// pyrepr quotes a string the way Python's repr does for the digest line, so the
// names stay single-quoted to match GhostHands' _DIGEST_LINE consumer regex
// (`\[(\d+)\] \S+ '([^']*)'`). Go's %q would emit double quotes and break that
// contract. Python prefers single quotes, switching to double only when the
// value contains a single quote but no double quote; we mirror that, escaping
// the chosen quote and backslashes.
func pyrepr(s string) string {
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, `"`) {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\':
			b.WriteString(`\\`)
		case c == quote:
			b.WriteByte('\\')
			b.WriteByte(c)
		case c == '\n':
			b.WriteString(`\n`)
		case c == '\r':
			b.WriteString(`\r`)
		case c == '\t':
			b.WriteString(`\t`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte(quote)
	return b.String()
}
