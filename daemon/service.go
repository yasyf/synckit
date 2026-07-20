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
	labelPrefix = "com.github.yasyf.synckit"

	reconcileInterval = 15 * time.Minute
	daemonPATH        = "/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/local/sbin:/usr/bin:/bin:/usr/sbin:/sbin"
)

// serviceAgents builds the exact launchd policy owned by synckitd.
func serviceAgents(manifests []manifest.Manifest, executable string) ([]dkservice.Agent, error) {
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return nil, fmt.Errorf("synckit service executable %q is not exact and absolute", executable)
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
		program, err := exec.LookPath(m.Binary)
		if err != nil {
			return nil, fmt.Errorf("resolve helper binary %q: %w", m.Binary, err)
		}
		if !filepath.IsAbs(program) || filepath.Clean(program) != program {
			return nil, fmt.Errorf("helper binary %q resolved to non-exact path %q", m.Binary, program)
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
