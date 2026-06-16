package broker

import (
	"os"
	"strings"
	"testing"
)

const nativeCalc = `- [0] AXWindow "Calculator"
  - AXStaticText = "81" (Edit field)
  - [5] AXButton (7) [id=Seven actions=[press]]
  - [22] AXButton (Show Sidebar) [id= help="Show Sidebar" actions=[press]]
`

func TestDigestText_NativeCalculator(t *testing.T) {
	// Byte-for-byte parity with GhostHands' compaction._digest_text on a native
	// (no AXWebArea) tree: window kept, the id-tagged 7 button, the empty-id
	// Show Sidebar (no id= tag), and the Edit field value in DISPLAY.
	want := strings.Join([]string{
		"BUTTONS (act by element_index):",
		"[0] AXWindow 'Calculator'",
		"[5] AXButton '7' id=Seven",
		"[22] AXButton 'Show Sidebar'",
		"",
		"DISPLAY:",
		"- Edit field: '81'",
	}, "\n")
	if got := digestText(nativeCalc); got != want {
		t.Errorf("digest mismatch\n got:\n%s\nwant:\n%s", got, want)
	}
}

func TestDigestText_Empty(t *testing.T) {
	want := "BUTTONS (act by element_index):\n(none)\n\nDISPLAY:\n(none)"
	if got := digestText(""); got != want {
		t.Errorf("empty digest = %q, want %q", got, want)
	}
}

func TestDigestText_WebScopeFixture(t *testing.T) {
	// The committed Brave fixture has browser chrome (Back/Forward/address bar)
	// OUTSIDE an AXWebArea and the page's own controls inside it. Because the
	// markdown contains "AXWebArea", digestText auto-web-scopes (matching the
	// Python funnel) and drops the chrome.
	md := readFixture(t)
	got := digestText(md)

	// Page controls survive.
	for _, want := range []string{"'Submit'", "'Cancel'", "'Version number'", "'App Store Connect'"} {
		if !strings.Contains(got, want) {
			t.Errorf("web digest missing page control %s\n%s", want, got)
		}
	}
	// Browser chrome (outside the AXWebArea) is dropped.
	for _, bad := range []string{"'Back'", "'Forward'", "Address and search bar"} {
		if strings.Contains(got, bad) {
			t.Errorf("web digest leaked chrome %s\n%s", bad, got)
		}
	}
	// The menubar subtree (AXMenuItem) is always dropped.
	if strings.Contains(got, "AXMenuItem") || strings.Contains(got, "About This Mac") {
		t.Errorf("menu role leaked into digest\n%s", got)
	}
	// Specifically: Back (index 1, chrome) absent; Submit (index 73, in page) present.
	if strings.Contains(got, "[1] ") {
		t.Errorf("Back button [1] (chrome) should be dropped\n%s", got)
	}
	if !strings.Contains(got, "[73] ") {
		t.Errorf("Submit [73] (in web area) should survive\n%s", got)
	}
}

func TestActionableDigest_NativeKeepsChrome(t *testing.T) {
	// Without web_scope (no AXWebArea in the text) every indexed non-menu node
	// is kept — even a node a web tree would call chrome.
	md := "- [0] AXWindow \"X\"\n  - [1] AXButton (Back)\n  - [2] AXButton (Forward)\n"
	buttons, _ := actionableDigest(md, false)
	for _, want := range []string{"'Back'", "'Forward'"} {
		if !strings.Contains(buttons, want) {
			t.Errorf("native digest dropped %s\n%s", want, buttons)
		}
	}
}

func TestActionableDigest_TwoWindowCutoff(t *testing.T) {
	// A second top-level AXWindow root begins the stale window-twin the daemon
	// appends; nothing from it should appear in BUTTONS.
	md := strings.Join([]string{
		`- [0] AXWindow "First"`,
		`  - [1] AXButton (Alpha)`,
		`- [9] AXWindow "Second"`,
		`  - [10] AXButton (Beta)`,
	}, "\n")
	buttons, _ := actionableDigest(md, false)
	if !strings.Contains(buttons, "'Alpha'") {
		t.Errorf("first-window control missing\n%s", buttons)
	}
	if strings.Contains(buttons, "'Beta'") || strings.Contains(buttons, "'Second'") {
		t.Errorf("second window subtree leaked past the cutoff\n%s", buttons)
	}
}

func TestActionableDigest_MenusDropped(t *testing.T) {
	md := strings.Join([]string{
		`- [0] AXWindow "App"`,
		`  - [1] AXButton (Go)`,
		`- [2] AXMenuBar [actions=[cancel]]`,
		`  - [3] AXMenuBarItem "File"`,
		`    - [4] AXMenuItem "Open"`,
	}, "\n")
	buttons, _ := actionableDigest(md, false)
	if !strings.Contains(buttons, "'Go'") {
		t.Errorf("window control dropped\n%s", buttons)
	}
	for _, bad := range []string{"AXMenuBar", "AXMenuBarItem", "AXMenuItem"} {
		if strings.Contains(buttons, bad) {
			t.Errorf("menu role %s leaked\n%s", bad, buttons)
		}
	}
}

func TestActionableDigest_IDPinnedDedup(t *testing.T) {
	// Same role+name+id collapses to the first; same name WITHOUT an id does not.
	md := strings.Join([]string{
		`- [0] AXWindow "W"`,
		`  - [1] AXButton (Save) [id=save actions=[press]]`,
		`  - [2] AXButton (Save) [id=save actions=[press]]`,
		`  - [3] AXLink (Learn more)`,
		`  - [4] AXLink (Learn more)`,
	}, "\n")
	buttons, _ := actionableDigest(md, false)
	if strings.Count(buttons, "'Save'") != 1 {
		t.Errorf("id-pinned dupes not collapsed to one\n%s", buttons)
	}
	if strings.Count(buttons, "'Learn more'") != 2 {
		t.Errorf("id-less same-named links wrongly deduped\n%s", buttons)
	}
}

func TestActionableDigest_ButtonsCap(t *testing.T) {
	var b strings.Builder
	b.WriteString("- [0] AXWindow \"W\"\n")
	for i := 1; i <= 100; i++ {
		b.WriteString("  - [")
		b.WriteString(itoa(i))
		b.WriteString("] AXButton (B")
		b.WriteString(itoa(i))
		b.WriteString(")\n")
	}
	buttons, _ := actionableDigest(b.String(), false)
	if n := strings.Count(buttons, "\n") + 1; n != 80 {
		t.Errorf("BUTTONS cap = %d lines, want 80", n)
	}
}

func TestActionableDigest_DisplayCap(t *testing.T) {
	var b strings.Builder
	b.WriteString("- [0] AXWindow \"W\"\n")
	for i := 0; i < 15; i++ {
		// Distinct labels so dedup keeps all of them; non-indexed value nodes.
		b.WriteString("  - AXStaticText = \"v")
		b.WriteString(itoa(i))
		b.WriteString("\" (L")
		b.WriteString(itoa(i))
		b.WriteString(")\n")
	}
	_, values := actionableDigest(b.String(), false)
	if n := strings.Count(values, "\n") + 1; n != 10 {
		t.Errorf("DISPLAY cap = %d lines, want 10", n)
	}
}

func TestPyrepr(t *testing.T) {
	cases := []struct{ in, want string }{
		{"7", "'7'"},
		{"Show Sidebar", "'Show Sidebar'"},
		{"it's", `"it's"`}, // single quote, no double: switch to double quotes
		{`a"b`, `'a"b'`},   // double quote, no single: stay single
		{`a'b"c`, `'a\'b"c'`},
		{"line\nbreak", `'line\nbreak'`},
	}
	for _, c := range cases {
		if got := pyrepr(c.in); got != c.want {
			t.Errorf("pyrepr(%q) = %s, want %s", c.in, got, c.want)
		}
	}
}

func readFixture(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("testdata/brave_trimmed.md")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return string(b)
}

// itoa is a tiny local helper to keep these table builders dependency-free.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
