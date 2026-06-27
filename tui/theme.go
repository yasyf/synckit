package tui

import "github.com/charmbracelet/lipgloss"

// The synckit palette. 256-color ANSI indices, one semantic role each.
const (
	colActive = lipgloss.Color("212") // pink — active tab
	colAccent = lipgloss.Color("37")  // teal — brand accent
	colOK     = lipgloss.Color("78")  // green — clean / ready
	colWarn   = lipgloss.Color("214") // orange — pending / reachable-not-installed
	colErr    = lipgloss.Color("203") // red — dirty / error / unreachable
	colCheck  = lipgloss.Color("39")  // blue — in-progress / checking
	colBorder = lipgloss.Color("240") // grey — idle panel border
)

// Accent is the teal brand accent a consumer screen tints inline text with.
var Accent = lipgloss.NewStyle().Foreground(colAccent)

var (
	// panel and panelActive box a master or detail column; the active pane
	// borrows the accent border, idle panes stay grey.
	panel = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(colBorder).
		Padding(0, 1)
	panelActive = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colAccent).
			Padding(0, 1)

	// headerTitle is the brand mark in the top band; headerHint is the
	// right-aligned context (host identity, sort order).
	headerTitle = lipgloss.NewStyle().Bold(true).Foreground(colAccent)
	headerHint  = lipgloss.NewStyle().Faint(true)
)

// DetailTitle heads a detail pane; DetailKey labels a field. A consumer screen
// renders its detail column with these so every tab reads the same.
var (
	DetailTitle = lipgloss.NewStyle().Bold(true).Foreground(colAccent)
	DetailKey   = lipgloss.NewStyle().Faint(true)
)

// Row and detail status badges a consumer screen tints state markers with.
var (
	BadgeClean = lipgloss.NewStyle().Foreground(colOK)
	BadgeDirty = lipgloss.NewStyle().Foreground(colWarn)
	BadgeSync  = lipgloss.NewStyle().Foreground(colCheck)
	BadgeKind  = Accent
)
