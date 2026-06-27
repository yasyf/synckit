package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

// FilterBarLines is the single chrome row each screen reserves for its filter.
const FilterBarLines = 1

// FilterBar is an always-visible filter input owning its own state, independent
// of the bubbles list's built-in filter. '/' focuses it; esc clears and blurs.
type FilterBar struct {
	input   textinput.Model
	focused bool
}

// NewFilterBar returns a blurred filter input ready to mount on a screen.
func NewFilterBar() FilterBar {
	in := textinput.New()
	in.Placeholder = "filter"
	in.Prompt = ""
	in.Width = 24
	return FilterBar{input: in}
}

// Value returns the current filter query.
func (f FilterBar) Value() string { return f.input.Value() }

// Focused reports whether the filter input holds focus.
func (f FilterBar) Focused() bool { return f.focused }

// Focus gives the filter input keyboard focus and returns its blink command.
func (f *FilterBar) Focus() tea.Cmd { f.focused = true; return f.input.Focus() }

// Blur removes focus from the filter input.
func (f *FilterBar) Blur() { f.focused = false; f.input.Blur() }

// Clear resets the filter query to empty.
func (f *FilterBar) Clear() { f.input.SetValue("") }

// Update feeds a message to the filter input, returning the updated bar.
func (f FilterBar) Update(msg tea.Msg) (FilterBar, tea.Cmd) {
	var cmd tea.Cmd
	f.input, cmd = f.input.Update(msg)
	return f, cmd
}

// View renders the prompt, the input, and a live "N/M shown" match count.
func (f FilterBar) View(visible, total int) string {
	count := Dim.Render(fmt.Sprintf("  %d/%d", visible, total))
	return Accent.Render("/ ") + f.input.View() + count
}

// FilterItems narrows items to those whose FilterValue contains query, case-
// insensitively. It always returns a fresh slice so the caller may sort the
// result without disturbing the canonical slice; an empty query keeps every item.
func FilterItems(all []list.Item, query string) []list.Item {
	q := strings.ToLower(strings.TrimSpace(query))
	out := make([]list.Item, 0, len(all))
	for _, it := range all {
		if q == "" || strings.Contains(strings.ToLower(it.FilterValue()), q) {
			out = append(out, it)
		}
	}
	return out
}
