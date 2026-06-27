package tui

import (
	"testing"

	"github.com/charmbracelet/bubbles/list"
)

func TestFilterItems(t *testing.T) {
	hosts := []list.Item{
		hostItem{target: "yasyf@aneta-web"},
		hostItem{target: "yasyf@aneta-sync"},
		hostItem{target: "yasyf@other-infra"},
	}
	cases := []struct {
		name  string
		query string
		want  int
	}{
		{name: "empty keeps all", query: "", want: 3},
		{name: "case-insensitive prefix", query: "ANETA", want: 2},
		{name: "narrow to one", query: "web", want: 1},
		{name: "no match", query: "zzz", want: 0},
		{name: "whitespace is trimmed", query: "  infra  ", want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := len(FilterItems(hosts, tc.query)); got != tc.want {
				t.Fatalf("FilterItems(%q) kept %d, want %d", tc.query, got, tc.want)
			}
		})
	}
}

func TestFilterItemsByTarget(t *testing.T) {
	// The matcher narrows hosts by target, exercising hostItem.FilterValue.
	hosts := []list.Item{
		hostItem{target: "yasyf@metal"},
		hostItem{target: "yasyf@hermes"},
	}
	if got := len(FilterItems(hosts, "met")); got != 1 {
		t.Fatalf("FilterItems(hosts, met) kept %d, want 1", got)
	}
}

func TestFilterItemsReturnsFreshSlice(t *testing.T) {
	// An empty query must not alias the input, so sorting the result can't reorder
	// the canonical slice.
	hosts := []list.Item{
		hostItem{target: "a"},
		hostItem{target: "b"},
	}
	got := FilterItems(hosts, "")
	got[0], got[1] = got[1], got[0]
	if hosts[0].(hostItem).target != "a" {
		t.Fatal("FilterItems aliased its input; reordering the result mutated the source")
	}
}
