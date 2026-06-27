package tui

import (
	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
)

// Screen is one content tab of the TUI, implemented by a consumer. Update returns
// the concrete Screen so the router replaces it in place, keeping all per-screen
// state on the value.
type Screen interface {
	Init() tea.Cmd
	Update(tea.Msg) (Screen, tea.Cmd)
	View() string
	Title() string
	Help() []key.Binding
	// WantsKey reports whether a modal sub-state (a focused text input or an
	// open confirm dialog) should swallow a key before the router's globals run.
	WantsKey(tea.KeyMsg) bool
}
