package tui

import "github.com/charmbracelet/bubbles/key"

// globalKeyMap holds the router-level bindings handled by rootModel when no
// screen is in a modal sub-state.
type globalKeyMap struct {
	NextTab key.Binding
	Help    key.Binding
	Quit    key.Binding
}

func newGlobalKeyMap() globalKeyMap {
	return globalKeyMap{
		NextTab: key.NewBinding(key.WithKeys("tab", "n"), key.WithHelp("tab", "switch tab")),
		Help:    key.NewBinding(key.WithKeys("?"), key.WithHelp("?", "help")),
		Quit:    key.NewBinding(key.WithKeys("q", "esc", "ctrl+c"), key.WithHelp("q", "quit")),
	}
}

// hostsKeyMap holds the hosts screen's contextual bindings.
type hostsKeyMap struct {
	Filter  key.Binding
	Add     key.Binding
	Verify  key.Binding
	Select  key.Binding
	Remove  key.Binding
	Confirm key.Binding
	Cancel  key.Binding
}

func newHostsKeyMap() hostsKeyMap {
	return hostsKeyMap{
		Filter:  key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "filter")),
		Add:     key.NewBinding(key.WithKeys("+"), key.WithHelp("+", "add host")),
		Verify:  key.NewBinding(key.WithKeys("v"), key.WithHelp("v", "verify all")),
		Select:  key.NewBinding(key.WithKeys("enter", " "), key.WithHelp("enter", "verify/edit")),
		Remove:  key.NewBinding(key.WithKeys("r", "delete", "backspace"), key.WithHelp("r", "remove")),
		Confirm: key.NewBinding(key.WithKeys("y"), key.WithHelp("y", "confirm")),
		Cancel:  key.NewBinding(key.WithKeys("n", "esc", "ctrl+c"), key.WithHelp("esc", "cancel")),
	}
}
