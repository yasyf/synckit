package tui

import (
	"io"
	"strings"

	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/yasyf/synckit/hostregistry"
)

// verifyState tracks how far a host's reachability probe has progressed.
type verifyState int

const (
	verifyUnknown verifyState = iota
	verifyChecking
	verifyOK
	verifyWarn
	verifyFail
)

// hostItem is one host row: a discovered or registered peer plus the latest
// probe result.
type hostItem struct {
	node       string
	target     string
	source     string
	online     bool
	registered bool
	verify     hostregistry.VerifyResult
	state      verifyState
}

func (i hostItem) FilterValue() string { return i.target }

// hostDelegate renders a hostItem: a verify glyph, the target, a registration
// marker, and a source/online hint.
type hostDelegate struct{}

func (hostDelegate) Height() int                         { return 1 }
func (hostDelegate) Spacing() int                        { return 0 }
func (hostDelegate) Update(tea.Msg, *list.Model) tea.Cmd { return nil }

func (d hostDelegate) Render(w io.Writer, m list.Model, index int, item list.Item) {
	it := item.(hostItem)

	glyph := Dim.Render("·")
	switch it.state {
	case verifyChecking:
		glyph = glyphCheck.Render("…")
	case verifyOK:
		glyph = glyphOK.Render("✓")
	case verifyWarn:
		glyph = glyphWarn.Render("⚠")
	case verifyFail:
		glyph = GlyphFail.Render("✗")
	}

	mark := Dim.Render("[ ]")
	if it.registered {
		mark = BadgeTracked.Render("[reg]")
	}

	hint := it.source
	if it.online {
		hint += " online"
	}
	if it.state == verifyOK && it.verify.Version != "" {
		hint += " " + it.verify.Version
	}
	if it.state == verifyWarn {
		hint += " not-installed"
	}
	if it.state == verifyFail && it.verify.Err != nil {
		hint += " unreachable"
	}

	row := glyph + " " + mark + " " + it.target + "  " + Dim.Render(hint)
	if index == m.Index() {
		row = "> " + row
	} else {
		row = "  " + row
	}
	_, _ = io.WriteString(w, lipgloss.NewStyle().MaxWidth(m.Width()).Render(row))
}

// renderHostDetail describes the selected host for the detail pane: its node,
// discovery source, online and registration state, and the latest probe result.
func renderHostDetail(item list.Item) string {
	it, ok := item.(hostItem)
	if !ok {
		return Dim.Render("No host selected.")
	}

	reg := Dim.Render("unregistered")
	if it.registered {
		reg = BadgeTracked.Render("registered")
	}

	online := Dim.Render("offline")
	if it.online {
		online = BadgeClean.Render("online")
	}

	status := Dim.Render("· not checked")
	switch it.state {
	case verifyChecking:
		status = glyphCheck.Render("… checking")
	case verifyOK:
		status = glyphOK.Render("✓ ready")
	case verifyWarn:
		status = glyphWarn.Render("⚠ reachable, not installed")
	case verifyFail:
		status = GlyphFail.Render("✗ unreachable")
	}

	lines := []string{
		DetailTitle.Render(it.target),
		"",
		DetailKey.Render("node    ") + it.node,
		DetailKey.Render("source  ") + it.source,
		DetailKey.Render("online  ") + online,
		DetailKey.Render("reg     ") + reg,
		DetailKey.Render("status  ") + status,
	}
	if it.state == verifyOK && it.verify.Version != "" {
		lines = append(lines, DetailKey.Render("version ")+it.verify.Version)
	}
	if it.state == verifyFail && it.verify.Err != nil {
		lines = append(lines, "", Dim.Render(it.verify.Err.Error()))
	}
	return strings.Join(lines, "\n")
}

// classifyVerify maps a probe result onto a row state.
func classifyVerify(res hostregistry.VerifyResult) verifyState {
	if res.Reachable && res.Bootstrapped {
		return verifyOK
	}
	if res.Reachable {
		return verifyWarn
	}
	return verifyFail
}
