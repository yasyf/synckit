package tui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// TestRouterViewFitsTerminal proves the router never renders past the terminal as
// the help bar grows. The fake screens fill exactly the inner height they are
// handed, so the composed header + body + help totals the terminal height in every
// help state: collapsed, expanded, after a tab switch that changes the binding
// count, and after re-collapse.
func TestRouterViewFitsTerminal(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	opts := Options{
		Brand: "synckit",
		Screens: []Screen{
			heightScreen{title: "A", bindings: fixedBindings(2)},
			heightScreen{title: "B", bindings: fixedBindings(4)},
		},
		Runner: fakeRunner{},
	}
	m := newRootModel(opts)

	const termH = 24
	step := func(name string, msg tea.Msg) {
		next, _ := m.Update(msg)
		m = next.(rootModel)
		if got := lipgloss.Height(m.View()); got != termH {
			t.Fatalf("%s: View height = %d, want %d\n%s", name, got, termH, m.View())
		}
	}

	step("initial collapsed", tea.WindowSizeMsg{Width: 80, Height: termH})
	step("expanded", tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'?'}})
	step("next tab expanded", tea.KeyMsg{Type: tea.KeyTab})
	step("re-collapsed", tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'?'}})
}

// TestRouterReflowsOnScreenHelpChange proves the router re-sizes its screens when
// an internal screen state change — not a WindowSizeMsg and not a global key —
// alters the help bar height. The confirm screen shrinks its Help() while a flag
// toggled through the WantsKey path is set; without a reflow the screens stay
// sized for the prior footer and the composed view drifts off the terminal. The
// sequence reproduces the overflow: expand help, open the confirm (help shrinks),
// take a terminal resize while shrunk, then close the confirm (help regrows).
func TestRouterReflowsOnScreenHelpChange(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	opts := Options{
		Brand: "synckit",
		Screens: []Screen{
			confirmScreen{title: "A", full: fixedBindings(5), confirmed: fixedBindings(1), toggleKey: "x"},
		},
		Runner: fakeRunner{},
	}
	m := newRootModel(opts)

	const termH = 24
	step := func(name string, msg tea.Msg) {
		next, _ := m.Update(msg)
		m = next.(rootModel)
		if got := lipgloss.Height(m.View()); got != termH {
			t.Fatalf("%s: View height = %d, want %d\n%s", name, got, termH, m.View())
		}
	}

	expand := tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'?'}}
	toggle := tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}}

	step("sized", tea.WindowSizeMsg{Width: 80, Height: termH})
	step("help expanded", expand)
	step("confirm opens, help shrinks", toggle)
	step("resize while shrunk", tea.WindowSizeMsg{Width: 80, Height: termH})
	step("confirm closes, help regrows", toggle)
}
