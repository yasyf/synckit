// Package service manages the macOS LaunchAgents that drive a synckit tool: any
// number of launchd jobs, each a binary invoked with a verb on a schedule or as a
// long-lived daemon. It is tool-agnostic — the consuming tool supplies a ToolConfig
// (binary name, label prefix, the set of agents, per-agent plist keys, log naming,
// optional per-agent preflight) and the manager renders deterministic plists and
// drives the launchctl boundary. Plist rendering is a pure function so tests assert
// the exact keys; the launchctl boundary is an injected Launcher so tests never load
// real agents.
//
// The launchd job set, schedule, and any tool-specific plist keys
// (LimitLoadToSessionType=Aqua for a keychain-touching daemon, Nice/LowPriorityIO
// for a background tick) stay in the tool's ToolConfig, not in this package — only
// the launchd/launchctl machinery is generic.
package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

const (
	launchAgentsRelpath = "Library/LaunchAgents"

	plistFileMode = 0o644
	agentsDirMode = 0o755
)

// DefaultDaemonPATH is the PATH a LaunchAgent should run with on Homebrew installs.
// launchd's default PATH omits the Homebrew prefixes where tools like jj and
// watchman live, so a scheduled job fails to find them; EnvironmentVariables
// replaces the process PATH outright, so the system dirs are kept too. Both arches
// are listed so a single plist works on Apple Silicon and Intel.
const DefaultDaemonPATH = "/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/local/sbin:/usr/bin:/bin:/usr/sbin:/sbin"

// AgentSpec describes one launchd job: its label suffix (joined to the tool's
// LabelPrefix), the CLI verb the binary is invoked with, and any per-agent plist
// keys layered on top of the common ones (e.g. StartInterval, KeepAlive,
// LimitLoadToSessionType). ExtraKeys values must be bool, int, or string.
//
// Binary, when set, makes this agent's plist run that binary instead of the tool's
// own resolved executable: it is resolved on PATH the same symlink-preserving way
// exePath resolves the own-exe (no EvalSymlinks), so a Homebrew symlink such as
// /opt/homebrew/bin/cookiesync survives a brew upgrade. Empty Binary leaves the
// current behavior unchanged.
type AgentSpec struct {
	Label     string
	Command   string
	Binary    string
	ExtraKeys map[string]any
}

// ToolConfig is everything this package needs to manage one tool's LaunchAgents.
//
// LogName maps a full agent label to a HOME-relative log path (both StandardOutPath
// and StandardErrorPath point at it). PreflightCheck, when set, runs before an agent
// is loaded and aborts the install if it fails — the seam for a per-agent
// dependency check (e.g. watchman for a file-watch daemon); it is never called for
// agents that supply no check.
type ToolConfig struct {
	BinaryName     string
	LabelPrefix    string
	Agents         []AgentSpec
	DaemonPATH     string
	LogName        func(agentLabel string) string
	PreflightCheck func(ctx context.Context, agent AgentSpec) error
}

// FullLabel returns the launchd label for agent: LabelPrefix joined to the agent's
// label suffix with a dot.
func (c ToolConfig) FullLabel(agent AgentSpec) string {
	return c.LabelPrefix + "." + agent.Label
}

// Launcher bootstraps and boots out launchd jobs in the user's GUI domain; the
// launchctl boundary tests inject. Bootout tolerates a not-loaded agent so a
// reinstall is idempotent; Bootstrap tolerates nothing, since install boots out
// first and a nonzero bootstrap (e.g. a malformed plist) is a real error.
type Launcher interface {
	// Bootstrap registers the job described by the plist at plistPath.
	Bootstrap(ctx context.Context, plistPath string) error
	// Bootout deregisters the launchd job with the given label.
	Bootout(ctx context.Context, label string) error
}

// LaunchdManager renders a tool's plists and drives them through an injected
// Launcher. Construct it with NewLaunchdManager.
type LaunchdManager struct {
	launcher Launcher
}

// NewLaunchdManager returns a manager that drives launchd through launcher.
func NewLaunchdManager(launcher Launcher) *LaunchdManager {
	return &LaunchdManager{launcher: launcher}
}

// Install resolves this executable, then for each agent (tickOnly stops after the
// first) renders its plist, writes it, runs the agent's preflight check, and loads
// it. Each agent is booted out before bootstrap so re-install picks up plist changes.
func (m *LaunchdManager) Install(ctx context.Context, cfg ToolConfig, tickOnly bool) error {
	if runtime.GOOS != "darwin" {
		return fmt.Errorf("install requires macOS launchd, not %s", runtime.GOOS)
	}
	exe, err := exePath()
	if err != nil {
		return err
	}
	agentsDir, err := homeJoin(launchAgentsRelpath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(agentsDir, agentsDirMode); err != nil {
		return fmt.Errorf("create LaunchAgents dir %s: %w", agentsDir, err)
	}

	for _, agent := range cfg.Agents {
		if err := m.installAgent(ctx, cfg, agentsDir, exe, agent); err != nil {
			return err
		}
		if tickOnly {
			return nil
		}
	}
	return nil
}

// Uninstall boots out every agent and removes its plist file; a missing file is not
// an error.
func (m *LaunchdManager) Uninstall(ctx context.Context, cfg ToolConfig) error {
	if runtime.GOOS != "darwin" {
		return fmt.Errorf("uninstall requires macOS launchd, not %s", runtime.GOOS)
	}
	agentsDir, err := homeJoin(launchAgentsRelpath)
	if err != nil {
		return err
	}
	for _, agent := range cfg.Agents {
		label := cfg.FullLabel(agent)
		path := filepath.Join(agentsDir, label+".plist")
		if err := m.launcher.Bootout(ctx, label); err != nil {
			return fmt.Errorf("bootout %s: %w", label, err)
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove plist %s: %w", path, err)
		}
	}
	return nil
}

func (m *LaunchdManager) installAgent(ctx context.Context, cfg ToolConfig, agentsDir, exe string, agent AgentSpec) error {
	if cfg.PreflightCheck != nil {
		if err := cfg.PreflightCheck(ctx, agent); err != nil {
			return err
		}
	}
	xml, err := renderPlist(cfg, exe, agent)
	if err != nil {
		return err
	}
	label := cfg.FullLabel(agent)
	path := filepath.Join(agentsDir, label+".plist")
	if err := os.WriteFile(path, []byte(xml), plistFileMode); err != nil {
		return fmt.Errorf("write plist %s: %w", path, err)
	}
	if err := m.launcher.Bootout(ctx, label); err != nil {
		return fmt.Errorf("bootout %s before reload: %w", label, err)
	}
	if err := m.launcher.Bootstrap(ctx, path); err != nil {
		return fmt.Errorf("bootstrap %s: %w", label, err)
	}
	return nil
}

// exePath returns the path used to invoke this binary, deliberately NOT resolving
// symlinks. On a Homebrew install that keeps the plist pointed at the stable
// /opt/homebrew/bin/<tool> symlink, which brew relinks on every upgrade, so the
// LaunchAgents survive `brew upgrade` untouched. Resolving would bake in a versioned
// Caskroom/uv path that the next upgrade purges, leaving the agents pointing at a
// deleted binary.
func exePath() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	return exe, nil
}

func homeJoin(relpath string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return filepath.Join(home, relpath), nil
}
