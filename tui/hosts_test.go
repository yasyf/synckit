package tui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// TestHostsConfirmFitsBudget proves an open removal confirmation is reserved out
// of the master-detail split rather than stacked past the height budget: the hosts
// view height is unchanged whether the prompt shows or not.
func TestHostsConfirmFitsBudget(t *testing.T) {
	seedMesh(t, "yasyf@alpha")

	m := newHostsModel(Options{Runner: fakeRunner{}})
	// Narrow enough to drop the detail column so height turns only on the list panel
	// and the reserved rows beneath it.
	s, _ := m.Update(tea.WindowSizeMsg{Width: 50, Height: 24})
	m = s.(hostsModel)

	base := lipgloss.Height(m.View())
	if base != 23 {
		t.Fatalf("baseline hosts View height = %d, want 23 (filter + split + legend inside a 24-row budget)", base)
	}

	s, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	m = s.(hostsModel)
	if m.confirm == nil {
		t.Fatal("remove on a registered host must open the confirm prompt")
	}
	if !strings.Contains(m.View(), "Remove host") {
		t.Fatalf("confirm View missing the prompt:\n%s", m.View())
	}
	if got := lipgloss.Height(m.View()); got != base {
		t.Fatalf("confirm-open View height = %d, want %d (the confirm box must be reserved out of the split)", got, base)
	}
}
