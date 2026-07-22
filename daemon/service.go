package daemon

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	dkservice "github.com/yasyf/daemonkit/service"

	"github.com/yasyf/synckit/manifest"
)

const (
	labelPrefix  = "com.github.yasyf.synckit"
	daemonBinary = "synckitd"

	reconcileInterval = 15 * time.Minute
	daemonPATH        = "/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/local/sbin:/usr/bin:/bin:/usr/sbin:/sbin"
)

// serviceAgents builds the exact launchd policy owned by synckitd.
func serviceAgents(manifests []manifest.Manifest) ([]dkservice.Agent, error) {
	executable, err := dkservice.CanonicalExecutable()
	if err != nil {
		return nil, fmt.Errorf("resolve canonical synckitd executable: %w", err)
	}
	logPath := func(label string) (string, error) {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home dir: %w", err)
		}
		return filepath.Join(home, "Library", "Logs", "synckit", label+".log"), nil
	}
	build := func(label string, args []string, program string) (dkservice.Agent, error) {
		log, err := logPath(label)
		if err != nil {
			return dkservice.Agent{}, err
		}
		return dkservice.Agent{
			Label:   label,
			Program: program,
			Args:    args,
			LogPath: log,
			Env:     map[string]string{"PATH": daemonPATH},
		}, nil
	}

	reconcile, err := build(labelPrefix+".reconcile", []string{"reconcile"}, executable)
	if err != nil {
		return nil, err
	}
	reconcile.RestartPolicy = dkservice.NoRestart
	reconcile.StartInterval = reconcileInterval
	reconcile.ProcessType = dkservice.ProcessTypeBackground

	serve, err := build(labelPrefix+".serve", []string{"serve"}, executable)
	if err != nil {
		return nil, err
	}
	serve.RestartPolicy = dkservice.RestartAlways

	agents := []dkservice.Agent{reconcile, serve}
	for _, m := range manifests {
		if m.Helper == nil {
			continue
		}
		program, err := canonicalHelperProgram(m.Binary)
		if err != nil {
			return nil, err
		}
		session, err := serviceSessionType(m.Helper.SessionType)
		if err != nil {
			return nil, fmt.Errorf("manifest %q helper: %w", m.Name, err)
		}
		helper, err := build(labelPrefix+".helper."+m.Name, []string{m.Helper.Command}, program)
		if err != nil {
			return nil, err
		}
		helper.RestartPolicy = dkservice.RestartAlways
		helper.LimitLoadToSessionType = session
		agents = append(agents, helper)
	}
	return agents, nil
}

func executableAlias(binary string) (string, error) {
	alias, err := exec.LookPath(binary)
	if err != nil {
		return "", fmt.Errorf("resolve executable alias %q: %w", binary, err)
	}
	alias, err = filepath.Abs(alias)
	if err != nil {
		return "", fmt.Errorf("resolve absolute executable alias %q: %w", binary, err)
	}
	return filepath.Clean(alias), nil
}

func canonicalHelperProgram(binary string) (string, error) {
	alias, err := executableAlias(binary)
	if err != nil {
		return "", fmt.Errorf("resolve helper binary %q: %w", binary, err)
	}
	program, err := filepath.EvalSymlinks(alias)
	if err != nil {
		return "", fmt.Errorf("resolve helper binary %q target: %w", binary, err)
	}
	if !filepath.IsAbs(program) || filepath.Clean(program) != program {
		return "", fmt.Errorf("helper binary %q resolved to non-exact target %q", binary, program)
	}
	return program, nil
}

func serviceSessionType(value manifest.SessionType) (dkservice.SessionType, error) {
	switch value {
	case "":
		return 0, nil
	case manifest.SessionTypeAqua:
		return dkservice.SessionTypeAqua, nil
	case manifest.SessionTypeBackground:
		return dkservice.SessionTypeBackground, nil
	case manifest.SessionTypeLoginWindow:
		return dkservice.SessionTypeLoginWindow, nil
	case manifest.SessionTypeStandardIO:
		return dkservice.SessionTypeStandardIO, nil
	case manifest.SessionTypeSystem:
		return dkservice.SessionTypeSystem, nil
	default:
		return 0, fmt.Errorf("unsupported launchd session type %q", value)
	}
}
