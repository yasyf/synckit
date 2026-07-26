package daemon

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	dkservice "github.com/yasyf/daemonkit/service"

	"github.com/yasyf/synckit/internal/serviceidentity"
	"github.com/yasyf/synckit/manifest"
)

const (
	labelPrefix  = serviceidentity.LabelPrefix
	daemonBinary = serviceidentity.DaemonBinary

	reconcileInterval = 15 * time.Minute
	daemonPATH        = "/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/local/sbin:/usr/bin:/bin:/usr/sbin:/sbin"
)

// serviceAgents builds the exact launchd policy owned by synckitd.
func serviceAgents(manifests []manifest.Manifest, build string) ([]dkservice.Agent, error) {
	executable, err := dkservice.StableProgram(daemonBinary, build)
	if err != nil {
		return nil, fmt.Errorf("resolve stable synckitd program: %w", err)
	}
	logPath := func(label string) (string, error) {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home dir: %w", err)
		}
		return filepath.Join(home, "Library", "Logs", "synckit", label+".log"), nil
	}
	newAgent := func(label string, args []string, program string) (dkservice.Agent, error) {
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

	reconcile, err := newAgent(labelPrefix+".reconcile", []string{"reconcile"}, executable)
	if err != nil {
		return nil, err
	}
	reconcile.RestartPolicy = dkservice.NoRestart
	reconcile.StartInterval = reconcileInterval
	reconcile.ProcessType = dkservice.ProcessTypeBackground

	serve, err := newAgent(labelPrefix+".serve", []string{"serve"}, executable)
	if err != nil {
		return nil, err
	}
	serve.RestartPolicy = dkservice.RestartAlways

	agents := []dkservice.Agent{reconcile, serve}
	for _, m := range manifests {
		if m.Helper == nil {
			continue
		}
		helperLabel, err := serviceidentity.HelperLabel(m.Name)
		if err != nil {
			return nil, fmt.Errorf("manifest %q helper identity: %w", m.Name, err)
		}
		program, err := helperProgram(helperLabel, m.Binary)
		if err != nil {
			return nil, err
		}
		helper, err := newAgent(helperLabel, []string{m.Helper.Command}, program)
		if err != nil {
			return nil, err
		}
		helper.RestartPolicy = dkservice.RestartAlways
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

var stageHelperProgram = func(label, source string) (string, error) {
	return dkservice.StableProgramFrom(label, source)
}

func helperProgram(label, binary string) (string, error) {
	source, err := canonicalHelperProgram(binary)
	if err != nil {
		return "", err
	}
	if bundledExecutable(source) {
		return source, nil
	}
	program, err := stageHelperProgram(label, source)
	if err != nil {
		return "", fmt.Errorf("stage helper binary %q: %w", binary, err)
	}
	return program, nil
}

func bundledExecutable(program string) bool {
	macos := filepath.Dir(program)
	contents := filepath.Dir(macos)
	return filepath.Base(macos) == "MacOS" &&
		filepath.Base(contents) == "Contents" &&
		filepath.Ext(filepath.Dir(contents)) == ".app"
}
