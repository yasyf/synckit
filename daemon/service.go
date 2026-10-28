package daemon

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/launchd"

	"github.com/yasyf/synckit/internal/serviceidentity"
	"github.com/yasyf/synckit/manifest"
)

const (
	labelPrefix  = serviceidentity.LabelPrefix
	daemonBinary = serviceidentity.DaemonBinary

	// serveAgentLabel is the label the serve daemon carried before daemonkit
	// v0.21 and still carries: Client.Ensure converges exactly its own label, so
	// an unchanged one adopts the install already on disk instead of stranding
	// it beside a second job.
	serveAgentLabel = labelPrefix + ".serve"
	// reconcileAgentLabel names the tick launchd runs on its own schedule. It is
	// not a daemonkit-owned job, so synckit applies it through launchd directly.
	reconcileAgentLabel = labelPrefix + ".reconcile"

	reconcileInterval = 15 * time.Minute
	daemonPATH        = "/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/local/sbin:/usr/bin:/bin:/usr/sbin:/sbin"
)

func reconcileAgent() (launchd.Agent, error) {
	program, err := executableAlias(daemonBinary)
	if err != nil {
		return launchd.Agent{}, err
	}
	reconcile, err := newAgent(reconcileAgentLabel, []string{"reconcile"}, program)
	if err != nil {
		return launchd.Agent{}, err
	}
	reconcile.RestartPolicy = launchd.NoRestart
	reconcile.StartInterval = reconcileInterval
	reconcile.ProcessType = launchd.ProcessTypeBackground
	return reconcile, nil
}

// plannedLabels is every label synckit sweeps: the reconcile tick and the
// helper each manifest declares, derived from the manifests alone, so a helper
// binary that has since been uninstalled still gets its agent removed.
func plannedLabels(manifests []manifest.Manifest) ([]string, error) {
	labels := []string{reconcileAgentLabel}
	for _, m := range manifests {
		if m.Helper == nil {
			continue
		}
		helperLabel, err := serviceidentity.HelperLabel(m.Name)
		if err != nil {
			return nil, fmt.Errorf("manifest %q helper identity: %w", m.Name, err)
		}
		labels = append(labels, helperLabel)
	}
	return labels, nil
}

func newAgent(label string, args []string, program string) (launchd.Agent, error) {
	log, err := agentLogPath(label)
	if err != nil {
		return launchd.Agent{}, err
	}
	return launchd.Agent{
		Label:   label,
		Program: program,
		Args:    args,
		LogPath: log,
		Env:     map[string]string{"PATH": daemonPATH},
	}, nil
}

func agentLogPath(label string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return filepath.Join(home, "Library", "Logs", "synckit", label+".log"), nil
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
