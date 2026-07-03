package tui

import (
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
)

// TestMasterDetailFillsBudget proves each pane fills exactly its budgeted box:
// rows built to the full content width must not wrap into extra lines, and each
// box occupies its content width plus border and padding chrome.
func TestMasterDetailFillsBudget(t *testing.T) {
	cases := []struct {
		name       string
		width      int
		height     int
		wantHeight int
		wantWidth  int
	}{
		{name: "wide with detail", width: 100, height: 40, wantHeight: 40, wantWidth: 100},
		{name: "narrow no detail", width: 50, height: 30, wantHeight: 30, wantWidth: 50},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			listW, detailW, contentH, showDetail := SplitDims(tc.width, tc.height)

			listView := fillBlock("x", listW, contentH)
			detail := ""
			if showDetail {
				detail = fillBlock("y", detailW, contentH)
			}

			out := MasterDetail(listView, detail, listW, detailW, contentH, showDetail)

			if got := lipgloss.Height(out); got != tc.wantHeight {
				t.Fatalf("Height = %d, want %d (flush-width rows must not wrap into extra lines)\n%s", got, tc.wantHeight, out)
			}
			if got := lipgloss.Width(out); got != tc.wantWidth {
				t.Fatalf("Width = %d, want %d (each pane must occupy its full budgeted box width)", got, tc.wantWidth)
			}
		})
	}
}

// fillBlock builds a rows×cols block of the given single-cell glyph, every line
// exactly cols cells wide.
func fillBlock(glyph string, cols, rows int) string {
	line := strings.Repeat(glyph, cols)
	out := make([]string, rows)
	for i := range out {
		out[i] = line
	}
	return strings.Join(out, "\n")
}
