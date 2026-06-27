package tui

import "github.com/charmbracelet/lipgloss"

var (
	activeTab   = lipgloss.NewStyle().Bold(true).Foreground(colActive).Padding(0, 1)
	inactiveTab = lipgloss.NewStyle().Faint(true).Padding(0, 1)
	tabSep      = lipgloss.NewStyle().Faint(true)

	glyphOK    = lipgloss.NewStyle().Foreground(colOK)
	glyphWarn  = lipgloss.NewStyle().Foreground(colWarn)
	glyphCheck = lipgloss.NewStyle().Foreground(colCheck)
)

// StatusErr, StatusOK, and StatusInfo tint a screen's status line by outcome.
var (
	StatusErr  = lipgloss.NewStyle().Foreground(colErr)
	StatusOK   = lipgloss.NewStyle().Foreground(colOK)
	StatusInfo = lipgloss.NewStyle().Faint(true)
)

// PendingAccent marks a row whose pending selection diverges from its applied
// state; BadgeTracked marks an already-tracked row; Dim renders muted hints.
var (
	PendingAccent = lipgloss.NewStyle().Foreground(colWarn)
	BadgeTracked  = lipgloss.NewStyle().Faint(true)
	Dim           = lipgloss.NewStyle().Faint(true)
)

// GlyphFail tints an error marker in a list row or detail pane.
var GlyphFail = lipgloss.NewStyle().Foreground(colErr)

// ConfirmBox frames an inline yes/no confirmation prompt.
var ConfirmBox = lipgloss.NewStyle().
	Border(lipgloss.RoundedBorder()).
	BorderForeground(colWarn).
	Padding(0, 1)

// logPane frames the streaming bootstrap log on the Hosts screen.
var logPane = lipgloss.NewStyle().
	Border(lipgloss.RoundedBorder()).
	BorderForeground(colBorder).
	Padding(0, 1)
