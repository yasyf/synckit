package tui

import (
	"strings"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/yasyf/synckit/hostregistry"
)

// headerLines is the fixed chrome row the router reserves above the active
// screen; the help bar below is measured live since expanded help grows with the
// active screen's bindings.
const headerLines = 1

// rootModel is the tab router over the consumer's content screens and the shared
// hosts screen.
type rootModel struct {
	opts     Options
	active   int
	screens  []Screen
	inited   []bool
	self     string
	width    int
	height   int
	keys     globalKeyMap
	help     help.Model
	quitting bool
}

// newRootModel builds the router from the consumer's content screens and always
// appends the shared hosts screen so every consumer gets it for free.
func newRootModel(opts Options) rootModel {
	screens := append([]Screen{}, opts.Screens...)
	screens = append(screens, newHostsModel(opts))
	inited := make([]bool, len(screens))
	inited[0] = true
	return rootModel{
		opts:    opts,
		screens: screens,
		inited:  inited,
		self:    detectSelf(),
		keys:    newGlobalKeyMap(),
		help:    help.New(),
	}
}

// detectSelf reads this host's identity for the header band; an unreadable or
// unset identity simply leaves the brand mark standing alone.
func detectSelf() string {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return ""
	}
	return reg.Self
}

func (m rootModel) Init() tea.Cmd {
	return m.screens[0].Init()
}

func (m rootModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, m.resizeScreens()

	case tea.KeyMsg:
		if m.screens[m.active].WantsKey(msg) {
			return m, m.updateActive(msg)
		}
		switch {
		case key.Matches(msg, m.keys.Quit):
			m.quitting = true
			return m, tea.Quit
		case key.Matches(msg, m.keys.NextTab):
			m.active = (m.active + 1) % len(m.screens)
			cmd := m.resizeScreens()
			if !m.inited[m.active] {
				m.inited[m.active] = true
				return m, tea.Batch(cmd, m.screens[m.active].Init())
			}
			return m, cmd
		case key.Matches(msg, m.keys.Help):
			m.help.ShowAll = !m.help.ShowAll
			return m, m.resizeScreens()
		}
		return m, m.updateActive(msg)

	default:
		before := lipgloss.Height(m.helpView())
		var cmds []tea.Cmd
		for i := range m.screens {
			s, cmd := m.screens[i].Update(msg)
			m.screens[i] = s
			cmds = append(cmds, cmd)
		}
		if lipgloss.Height(m.helpView()) != before {
			cmds = append(cmds, m.resizeScreens())
		}
		return m, tea.Batch(cmds...)
	}
}

// updateActive routes msg to the active screen, then rebroadcasts the inner size
// to every screen when the update changed the help bar's height: a screen's Help()
// is state-dependent, so an internal state change can grow or shrink the footer
// with no WindowSizeMsg, leaving the screens sized against the wrong footer.
func (m *rootModel) updateActive(msg tea.Msg) tea.Cmd {
	before := lipgloss.Height(m.helpView())
	s, cmd := m.screens[m.active].Update(msg)
	m.screens[m.active] = s
	if lipgloss.Height(m.helpView()) == before {
		return cmd
	}
	return tea.Batch(cmd, m.resizeScreens())
}

func (m rootModel) View() string {
	if m.quitting {
		return ""
	}
	return lipgloss.JoinVertical(lipgloss.Left, m.header(), m.screens[m.active].View(), m.helpView())
}

func (m rootModel) innerHeight() int {
	inner := m.height - headerLines - lipgloss.Height(m.helpView())
	if inner < 1 {
		return 1
	}
	return inner
}

// resizeScreens broadcasts the current inner content size to every screen and
// batches the commands they return, so a change in the help bar's height reflows
// the active content immediately.
func (m *rootModel) resizeScreens() tea.Cmd {
	inner := tea.WindowSizeMsg{Width: m.width, Height: m.innerHeight()}
	cmds := make([]tea.Cmd, 0, len(m.screens))
	for i := range m.screens {
		s, cmd := m.screens[i].Update(inner)
		m.screens[i] = s
		cmds = append(cmds, cmd)
	}
	return tea.Batch(cmds...)
}

// header renders the brand mark, this host's identity, and the tab strip on a
// single line.
func (m rootModel) header() string {
	tabs := make([]string, len(m.screens))
	for i, s := range m.screens {
		if i == m.active {
			tabs[i] = activeTab.Render(s.Title())
			continue
		}
		tabs[i] = inactiveTab.Render(s.Title())
	}

	brand := headerTitle.Render(m.opts.Brand)
	if m.opts.Version != "" {
		brand += headerHint.Render(" " + m.opts.Version)
	}
	if m.self != "" {
		brand += headerHint.Render(" · " + m.self)
	}
	return brand + "  " + tabSep.Render("[") + strings.Join(tabs, tabSep.Render("|")) + tabSep.Render("]")
}

func (m rootModel) helpView() string {
	bindings := append(m.screens[m.active].Help(), m.keys.NextTab, m.keys.Help, m.keys.Quit)
	if m.help.ShowAll {
		return m.help.FullHelpView([][]key.Binding{bindings})
	}
	return m.help.ShortHelpView(bindings)
}
