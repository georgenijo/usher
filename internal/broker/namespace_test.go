package broker

import "testing"

func TestNamespacedTool(t *testing.T) {
	cases := []struct {
		backend, tool, want string
	}{
		{"cua", "click", "cua__click"},
		{"fs", "read_file", "fs__read_file"},
		{"cua", "get_window_state", "cua__get_window_state"},
	}
	for _, tc := range cases {
		if got := namespacedTool(tc.backend, tc.tool); got != tc.want {
			t.Errorf("namespacedTool(%q,%q) = %q, want %q", tc.backend, tc.tool, got, tc.want)
		}
	}
}

func TestStripNamespace(t *testing.T) {
	cases := []struct {
		name             string
		in               string
		wantBE, wantTool string
	}{
		{"plain", "cua__click", "cua", "click"},
		{"underscore in tool", "fs__read_file", "fs", "read_file"},
		{"tool itself has double underscore", "cua__a__b", "cua", "a__b"},
		{"no separator is unroutable", "click", "", "click"},
		{"empty string", "", "", ""},
		{"separator at start yields empty backend", "__click", "", "click"},
		{"separator at end yields empty tool", "cua__", "cua", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			be, tool := stripNamespace(tc.in)
			if be != tc.wantBE || tool != tc.wantTool {
				t.Errorf("stripNamespace(%q) = (%q,%q), want (%q,%q)",
					tc.in, be, tool, tc.wantBE, tc.wantTool)
			}
		})
	}
}

// TestNamespaceRoundTrip confirms namespacedTool then stripNamespace recovers
// the original (backend, tool) for names whose backend has no separator — the
// invariant routing relies on.
func TestNamespaceRoundTrip(t *testing.T) {
	cases := []struct{ backend, tool string }{
		{"cua", "click"},
		{"fs", "read_file"},
		{"cua", "a__b"}, // tool may contain the separator; backend may not
	}
	for _, tc := range cases {
		ns := namespacedTool(tc.backend, tc.tool)
		be, tool := stripNamespace(ns)
		if be != tc.backend || tool != tc.tool {
			t.Errorf("round-trip %q/%q via %q = (%q,%q)", tc.backend, tc.tool, ns, be, tool)
		}
	}
}
