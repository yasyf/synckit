package tui

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/yasyf/synckit/hostregistry"
)

const (
	hostModeList = iota
	hostModeAdd
	hostModeBootstrapping
)

// applyTimeout bounds a host mutation so a wedged reconcile holder surfaces as an
// error rather than parking the UI.
const applyTimeout = 10 * time.Minute

const verifyLegend = "✓ ready  ⚠ reachable, not installed  ✗ unreachable  … checking"

type hostsModel struct {
	opts       Options
	list       list.Model
	allItems   []list.Item
	filter     FilterBar
	loading    bool
	refreshing bool
	mode       int
	input      textinput.Model
	spin       spinner.Model
	logVP      viewport.Model
	logLines   []string
	busyTarget string
	cancel     context.CancelFunc
	lines      chan string
	confirm    *hostConfirmState
	status     string
	width      int
	height     int
	keys       hostsKeyMap

	mdListW      int
	mdDetailW    int
	mdHeight     int
	mdShowDetail bool
}

// hostsReserve is the rows the hosts screen keeps below the master-detail split
// for the verify legend and status line.
const hostsReserve = 2

// confirmReserve is the extra rows an open confirmation prompt claims below the
// split: its single prompt line plus the confirm box's top and bottom borders.
const confirmReserve = 3

// hostConfirmState is an open removal confirmation awaiting its target.
type hostConfirmState struct {
	prompt string
	target string
}

func newHostsModel(opts Options) hostsModel {
	sp := spinner.New(spinner.WithSpinner(spinner.Dot))
	in := textinput.New()
	in.Placeholder = "user@node"
	in.Validate = validateTarget
	l := list.New(nil, hostDelegate{}, 0, 0)
	l.SetShowTitle(false)
	l.SetShowStatusBar(false)
	l.SetShowHelp(false)
	l.SetFilteringEnabled(false)
	l.DisableQuitKeybindings()
	m := hostsModel{opts: opts, list: l, filter: NewFilterBar(), loading: true, refreshing: true, input: in, spin: sp, keys: newHostsKeyMap()}
	m.allItems = seedRegisteredItems()
	if m.allItems != nil {
		m.loading = len(m.allItems) == 0
	}
	return m
}

// seedRegisteredItems reads the registered mesh off disk and turns it into
// …checking host rows so the list paints from cache before any network probe
// runs. A registry read error yields nil, leaving the caller on the cold
// full-screen loading path.
func seedRegisteredItems() []list.Item {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return nil
	}
	items := mergeHostItems(nil, reg.Hosts)
	for i := range items {
		items[i].state = verifyChecking
	}
	return toListItems(items)
}

func (m hostsModel) Title() string { return "Hosts" }

func (m hostsModel) Help() []key.Binding {
	switch m.mode {
	case hostModeAdd:
		return []key.Binding{m.keys.Cancel}
	case hostModeBootstrapping:
		return []key.Binding{m.keys.Cancel}
	}
	if m.confirm != nil {
		return []key.Binding{m.keys.Confirm, m.keys.Cancel}
	}
	return []key.Binding{m.keys.Filter, m.keys.Add, m.keys.Select, m.keys.Verify, m.keys.Remove}
}

func (m hostsModel) WantsKey(tea.KeyMsg) bool {
	return m.mode == hostModeAdd || m.mode == hostModeBootstrapping || m.confirm != nil || m.filter.Focused()
}

func (m hostsModel) Init() tea.Cmd {
	return tea.Batch(m.spin.Tick, discoverHostsCmd(m.opts.Runner), verifyAllCmd(m.opts.Runner, m.allHostItems()))
}

// allHostItems is the canonical host slice as concrete rows, the seed for the
// initial verify pass before discovery returns.
func (m hostsModel) allHostItems() []hostItem {
	out := make([]hostItem, len(m.allItems))
	for i, raw := range m.allItems {
		out[i] = raw.(hostItem)
	}
	return out
}

func (m hostsModel) Update(msg tea.Msg) (Screen, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.resizeSplit()
		m.logVP = viewport.New(msg.Width, max(1, msg.Height-2))
		cmd := m.refreshHosts()
		return m, cmd

	case hostsLoadedMsg:
		m.loading = false
		m.refreshing = false
		if msg.err != nil {
			m.status = StatusErr.Render(msg.err.Error())
			return m, nil
		}
		m.allItems = mergeDiscovered(m.allItems, msg.items)
		cmd := m.refreshHosts()
		return m, tea.Batch(cmd, verifyAllCmd(m.opts.Runner, msg.items))

	case hostVerifiedMsg:
		cmd := m.markVerified(msg.target, msg.res)
		return m, cmd

	case hostAddProgressMsg:
		if m.lines == nil {
			return m, nil
		}
		m.logLines = append(m.logLines, msg.line)
		m.logVP.SetContent(strings.Join(m.logLines, "\n"))
		m.logVP.GotoBottom()
		return m, waitForLine(m.lines)

	case hostAddDoneMsg:
		m.mode = hostModeList
		m.cancel = nil
		m.lines = nil
		if msg.err != nil {
			m.status = StatusErr.Render("bootstrap failed: " + msg.err.Error())
		} else {
			m.status = StatusOK.Render("bootstrapped " + msg.target)
		}
		m.refreshing = true
		return m, tea.Batch(m.spin.Tick, discoverHostsCmd(m.opts.Runner))

	case hostRemovedMsg:
		if msg.err != nil {
			m.status = StatusErr.Render("remove failed: " + msg.err.Error())
			return m, nil
		}
		m.status = StatusOK.Render("removed " + msg.target)
		m.refreshing = true
		return m, tea.Batch(m.spin.Tick, discoverHostsCmd(m.opts.Runner))

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spin, cmd = m.spin.Update(msg)
		if m.loading || m.refreshing || m.mode == hostModeBootstrapping {
			return m, cmd
		}
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)
	}

	var cmd tea.Cmd
	m.list, cmd = m.list.Update(msg)
	return m, cmd
}

func (m hostsModel) handleKey(msg tea.KeyMsg) (Screen, tea.Cmd) {
	switch m.mode {
	case hostModeAdd:
		return m.handleAddKey(msg)
	case hostModeBootstrapping:
		if key.Matches(msg, m.keys.Cancel) && m.cancel != nil {
			m.cancel()
		}
		return m, nil
	}

	if m.confirm != nil {
		switch {
		case key.Matches(msg, m.keys.Confirm):
			target := m.confirm.target
			m.confirm = nil
			m.resizeSplit()
			return m, removeHostCmd(target)
		case key.Matches(msg, m.keys.Cancel):
			m.confirm = nil
			m.resizeSplit()
			return m, nil
		}
		return m, nil
	}

	if m.filter.Focused() {
		return m.handleFilterKey(msg)
	}

	switch {
	case key.Matches(msg, m.keys.Filter):
		cmd := m.filter.Focus()
		return m, cmd
	case key.Matches(msg, m.keys.Add):
		return m.startAdd("")
	case key.Matches(msg, m.keys.Verify):
		return m, verifyAllCmd(m.opts.Runner, listItems(m.list))
	case key.Matches(msg, m.keys.Remove):
		return m.startRemove()
	case key.Matches(msg, m.keys.Select):
		return m.selectRow()
	}

	var cmd tea.Cmd
	m.list, cmd = m.list.Update(msg)
	return m, cmd
}

// handleFilterKey routes keys while the filter bar holds focus: esc clears and
// blurs, enter blurs keeping the filter, anything else edits the query and
// re-narrows the list live.
func (m hostsModel) handleFilterKey(msg tea.KeyMsg) (Screen, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEsc:
		m.filter.Blur()
		m.filter.Clear()
		cmd := m.refreshHosts()
		return m, cmd
	case tea.KeyEnter:
		m.filter.Blur()
		return m, nil
	}
	var icmd tea.Cmd
	m.filter, icmd = m.filter.Update(msg)
	rcmd := m.refreshHosts()
	return m, tea.Batch(icmd, rcmd)
}

// resizeSplit recomputes the master-detail dimensions for the stored terminal
// size, widening the reserve while a confirmation is open so its box never pushes
// the layout past the last terminal row.
func (m *hostsModel) resizeSplit() {
	reserve := hostsReserve
	if m.confirm != nil {
		reserve += confirmReserve
	}
	m.mdListW, m.mdDetailW, m.mdHeight, m.mdShowDetail = SplitDims(m.width, m.height-FilterBarLines-reserve)
	m.list.SetSize(m.mdListW, m.mdHeight)
}

// refreshHosts recomputes the visible list from the canonical slice under the
// active filter, keeping the cursor on the same host.
func (m *hostsModel) refreshHosts() tea.Cmd {
	sel := selectedTarget(m.list)
	visible := FilterItems(m.allItems, m.filter.Value())
	cmd := m.list.SetItems(visible)
	selectTarget(&m.list, sel)
	return cmd
}

// selectedTarget reports the target of the cursor row, or "" when the list is
// empty, so a re-render can restore the selection.
func selectedTarget(l list.Model) string {
	if it, ok := l.SelectedItem().(hostItem); ok {
		return it.target
	}
	return ""
}

// selectTarget moves the cursor back onto the row with the given target.
func selectTarget(l *list.Model, target string) {
	if target == "" {
		return
	}
	for i, raw := range l.Items() {
		if it, ok := raw.(hostItem); ok && it.target == target {
			l.Select(i)
			return
		}
	}
}

func (m hostsModel) handleAddKey(msg tea.KeyMsg) (Screen, tea.Cmd) {
	switch {
	case key.Matches(msg, m.keys.Cancel):
		m.mode = hostModeList
		m.input.Blur()
		return m, nil
	case msg.Type == tea.KeyEnter:
		target := strings.TrimSpace(m.input.Value())
		if err := validateTarget(target); err != nil {
			m.status = StatusErr.Render(err.Error())
			return m, nil
		}
		m.input.Blur()
		return m.startBootstrap(target)
	}

	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m hostsModel) selectRow() (Screen, tea.Cmd) {
	it, ok := m.list.SelectedItem().(hostItem)
	if !ok {
		return m, nil
	}
	if it.registered {
		ccmd := m.markChecking(it.target)
		return m, tea.Batch(ccmd, verifyHostCmd(m.opts.Runner, it.target))
	}
	return m.startAdd(it.target)
}

func (m hostsModel) startAdd(prefill string) (Screen, tea.Cmd) {
	m.mode = hostModeAdd
	m.input.SetValue(prefill)
	m.input.CursorEnd()
	cmd := m.input.Focus()
	return m, cmd
}

func (m hostsModel) startRemove() (Screen, tea.Cmd) {
	it, ok := m.list.SelectedItem().(hostItem)
	if !ok || !it.registered {
		return m, nil
	}
	m.confirm = &hostConfirmState{
		prompt: fmt.Sprintf("Remove host %s? (y/N)", it.target),
		target: it.target,
	}
	m.resizeSplit()
	return m, nil
}

func (m hostsModel) startBootstrap(target string) (Screen, tea.Cmd) {
	ctx, cancel := context.WithCancel(context.Background())
	m.mode = hostModeBootstrapping
	m.busyTarget = target
	m.cancel = cancel
	m.lines = make(chan string, 64)
	m.logLines = nil
	m.logVP.SetContent("")
	return m, tea.Batch(m.spin.Tick, addHostCmd(ctx, target, m.lines), waitForLine(m.lines))
}

func (m *hostsModel) markChecking(target string) tea.Cmd {
	for i, raw := range m.allItems {
		it := raw.(hostItem)
		if it.target == target {
			it.state = verifyChecking
			m.allItems[i] = it
			break
		}
	}
	return m.refreshHosts()
}

func (m *hostsModel) markVerified(target string, res hostregistry.VerifyResult) tea.Cmd {
	for i, raw := range m.allItems {
		it := raw.(hostItem)
		if it.target == target {
			it.verify = res
			it.state = classifyVerify(res)
			m.allItems[i] = it
			break
		}
	}
	return m.refreshHosts()
}

func (m hostsModel) View() string {
	switch m.mode {
	case hostModeAdd:
		hint := Dim.Render("enter to bootstrap · esc to cancel")
		return lipgloss.JoinVertical(lipgloss.Left, "Add host:", m.input.View(), hint, m.status)
	case hostModeBootstrapping:
		head := m.spin.View() + " Bootstrapping " + m.busyTarget + Dim.Render(" (esc to cancel)")
		return lipgloss.JoinVertical(lipgloss.Left, head, logPane.Render(m.logVP.View()))
	}

	if m.loading {
		return m.spin.View() + " Discovering hosts…"
	}
	if len(m.list.Items()) == 0 {
		return Dim.Render("No hosts discovered. Press + to add one.")
	}

	legend := Dim.Render(verifyLegend)
	if m.refreshing {
		legend += "  " + m.spin.View() + Dim.Render(" refreshing…")
	}
	split := MasterDetail(m.list.View(), renderHostDetail(m.list.SelectedItem()), m.mdListW, m.mdDetailW, m.mdHeight, m.mdShowDetail)
	body := lipgloss.JoinVertical(lipgloss.Left, m.filter.View(len(m.list.Items()), len(m.allItems)), split, legend)
	if m.confirm != nil {
		body = lipgloss.JoinVertical(lipgloss.Left, body, ConfirmBox.Render(m.confirm.prompt))
	}
	if m.status != "" {
		body = lipgloss.JoinVertical(lipgloss.Left, body, m.status)
	}
	return body
}

func toListItems(items []hostItem) []list.Item {
	out := make([]list.Item, len(items))
	for i, it := range items {
		out[i] = it
	}
	return out
}

// mergeDiscovered overlays the verify result and state of prior rows onto the
// freshly-discovered set, matched by target, so a host that already resolved to
// ✓/⚠/✗ does not flash back to …checking when a discovery pass returns. The
// discovered set is authoritative for membership (it already carries every still-
// registered host) and order (registered-first), so newly-found rows appear and
// nothing still registered drops.
func mergeDiscovered(prior []list.Item, discovered []hostItem) []list.Item {
	resolved := make(map[string]hostItem, len(prior))
	for _, raw := range prior {
		it := raw.(hostItem)
		if it.state != verifyUnknown {
			resolved[it.target] = it
		}
	}
	for i, it := range discovered {
		if was, ok := resolved[it.target]; ok {
			it.verify = was.verify
			it.state = was.state
			discovered[i] = it
		}
	}
	return toListItems(discovered)
}

func listItems(l list.Model) []hostItem {
	raw := l.Items()
	out := make([]hostItem, len(raw))
	for i, r := range raw {
		out[i] = r.(hostItem)
	}
	return out
}

// discoverHostsCmd scans the network for hosts and merges in any registered
// host that discovery did not surface.
func discoverHostsCmd(r hostregistry.Runner) tea.Cmd {
	return func() tea.Msg {
		reg, err := hostregistry.Mesh.Load()
		if err != nil {
			return hostsLoadedMsg{err: fmt.Errorf("load host registry: %w", err)}
		}
		result, err := hostregistry.Hosts(context.Background(), r, reg.Hosts)
		if err != nil {
			return hostsLoadedMsg{err: err}
		}
		return hostsLoadedMsg{items: mergeHostItems(result.Candidates, reg.Hosts)}
	}
}

// mergeHostItems turns discovery candidates into rows and appends every
// registered host that discovery missed as an offline registered row, then
// floats registered hosts above discovered-only candidates. The sort is stable,
// so within each group rows keep their assembly order (candidates sorted by node,
// then registered-only hosts in registry order).
func mergeHostItems(cands []hostregistry.HostCandidate, registered []string) []hostItem {
	items := make([]hostItem, 0, len(cands)+len(registered))
	seen := map[string]struct{}{}
	for _, c := range cands {
		items = append(items, hostItem{
			node:       c.Node,
			target:     c.DefaultTarget,
			source:     c.Source,
			online:     c.Online,
			registered: c.Registered,
		})
		seen[c.Node] = struct{}{}
	}
	for _, h := range registered {
		if _, ok := seen[hostregistry.HostNode(h)]; ok {
			continue
		}
		items = append(items, hostItem{
			node:       hostregistry.HostNode(h),
			target:     h,
			source:     "registered",
			online:     false,
			registered: true,
		})
	}
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].registered && !items[j].registered
	})
	return items
}

func verifyAllCmd(r hostregistry.Runner, items []hostItem) tea.Cmd {
	var cmds []tea.Cmd
	for _, it := range items {
		if it.registered {
			cmds = append(cmds, verifyHostCmd(r, it.target))
		}
	}
	return tea.Batch(cmds...)
}

func verifyHostCmd(r hostregistry.Runner, target string) tea.Cmd {
	return func() tea.Msg {
		return hostVerifiedMsg{target: target, res: hostregistry.Mesh.Verify(context.Background(), r, target)}
	}
}

func removeHostCmd(target string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), applyTimeout)
		defer cancel()
		return hostRemovedMsg{target: target, err: hostregistry.Mesh.RemoveHost(ctx, target)}
	}
}

// addHostCmd bootstraps a peer by shelling synckitd, which owns the shared mesh and
// its ssh bootstrap. Each synckitd output line is streamed onto lines and collected
// into the run log; the channel closes when the run ends so waitForLine unblocks.
func addHostCmd(ctx context.Context, target string, lines chan string) tea.Cmd {
	return func() tea.Msg {
		//nolint:gosec // G204: fixed argv to the synckitd binary on PATH; target is a validated user@node, not a shell string.
		sub := exec.CommandContext(ctx, "synckitd", "host", "add", target)
		stdout, err := sub.StdoutPipe()
		if err != nil {
			close(lines)
			return hostAddDoneMsg{target: target, err: fmt.Errorf("pipe synckitd output: %w", err)}
		}
		sub.Stderr = sub.Stdout
		if err := sub.Start(); err != nil {
			close(lines)
			return hostAddDoneMsg{target: target, err: fmt.Errorf("start synckitd host add: %w", err)}
		}
		var log []string
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			log = append(log, line)
			lines <- line
		}
		err = sub.Wait()
		close(lines)
		if err != nil {
			return hostAddDoneMsg{target: target, log: log, err: fmt.Errorf("synckitd host add %s: %w", target, err)}
		}
		return hostAddDoneMsg{target: target, log: log}
	}
}

// waitForLine blocks on the next bootstrap step; a closed channel yields no
// message, leaving hostAddDoneMsg to end the run.
func waitForLine(lines chan string) tea.Cmd {
	return func() tea.Msg {
		if lines == nil {
			return nil
		}
		line, ok := <-lines
		if !ok {
			return nil
		}
		return hostAddProgressMsg{line: line}
	}
}
