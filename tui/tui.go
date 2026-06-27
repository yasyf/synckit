// Package tui is the shared interactive terminal UI for synckit consumers: a tab
// router over the consumer's content screens plus a built-in Hosts tab for
// discovering, verifying, and bootstrapping peers in the shared mesh.
package tui

import (
	"context"
	"errors"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/yasyf/synckit/hostregistry"
)

// Options configures a TUI run.
type Options struct {
	// Brand is the mark rendered in the header band, e.g. "reposync".
	Brand string
	// Version is the consumer's release, shown faint after the brand mark; omit to hide.
	Version string
	// Screens are the consumer's content tabs, shown before the shared Hosts tab.
	Screens []Screen
	// Runner executes the local and ssh commands the Hosts tab probes peers with.
	Runner hostregistry.Runner
}

// Run launches the interactive TUI and blocks until the user quits or ctx is
// canceled. It builds the router from the consumer's content Screens and always
// appends the shared Hosts screen. A ctx-driven teardown (ctrl-c, SIGTERM) is a
// clean exit.
func Run(ctx context.Context, opts Options) error {
	p := tea.NewProgram(newRootModel(opts), tea.WithContext(ctx), tea.WithAltScreen())
	_, err := p.Run()
	if errors.Is(err, tea.ErrProgramKilled) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
