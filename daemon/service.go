package daemon

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/launchd"

	"github.com/yasyf/synckit/hostregistry"
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

	// stagedProgramDirName holds the binaries synckit registers with launchd on
	// its own account, under the mesh directory rather than daemonkit's
	// ~/.daemonkit/bin, whose leaves are daemonkit labels.
	stagedProgramDirName = "bin"
)

// serviceAgents builds the exact launchd policy synckit owns outright: the
// reconcile tick and one helper per manifest that declares one. The serve
// daemon is deliberately absent — daemonkit's Client.Ensure owns that label's
// plist, its stable executable, and its live process.
func serviceAgents(manifests []manifest.Manifest) ([]launchd.Agent, error) {
	program, err := daemonProgram()
	if err != nil {
		return nil, err
	}
	reconcile, err := newAgent(reconcileAgentLabel, []string{"reconcile"}, program)
	if err != nil {
		return nil, err
	}
	reconcile.RestartPolicy = launchd.NoRestart
	reconcile.StartInterval = reconcileInterval
	reconcile.ProcessType = launchd.ProcessTypeBackground

	agents := []launchd.Agent{reconcile}
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
		helper.RestartPolicy = launchd.RestartAlways
		agents = append(agents, helper)
	}
	return agents, nil
}

// plannedLabels is the label set serviceAgents would build, derived from the
// manifests alone. Uninstall reads it without resolving or staging a single
// program, so a helper binary that has since been uninstalled still gets its
// agent removed.
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

// daemonProgram stages this executable at the path the reconcile agent
// registers. daemonkit.Stable stages the serve daemon's own copy under its
// label and nothing else, so the tick — a job daemonkit does not own — gets a
// synckit-owned copy that outlives the versioned original an upgrade deletes.
func daemonProgram() (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve current executable: %w", err)
	}
	source, err := filepath.EvalSymlinks(self)
	if err != nil {
		return "", fmt.Errorf("resolve current executable %q: %w", self, err)
	}
	program, err := stageProgram(reconcileAgentLabel, source)
	if err != nil {
		return "", fmt.Errorf("stage %s program: %w", daemonBinary, err)
	}
	return program, nil
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

// stageProgram publishes source's bytes at <mesh>/bin/<label> and returns the
// exact path launchd registers. daemonkit.Stable copies the invoking executable
// and nothing else, so helper staging stays synckit's policy: identical bytes
// are a no-op, leaving the inode launchd already execs alone.
var stageProgram = func(label, source string) (string, error) {
	data, err := os.ReadFile(source) //nolint:gosec // exact resolved program source
	if err != nil {
		return "", fmt.Errorf("read program source %q: %w", source, err)
	}
	dir, err := stagedProgramDir()
	if err != nil {
		return "", err
	}
	target := filepath.Join(dir, label)
	staged, err := stagedProgramCurrent(target, data)
	if err != nil {
		return "", err
	}
	if !staged {
		if err := durable.WriteFile(target, data, 0o700); err != nil {
			return "", fmt.Errorf("stage program at %q: %w", target, err)
		}
	}
	return canonicalStagedProgram(target)
}

// removeStagedProgram deletes the copy stageProgram published for label. A
// staged program is named for its label exactly and every label that reaches
// here is synckit's own by prefix, so the path can name nothing else; a label
// that was never staged — a bundled helper, or the daemonkit-owned serve job —
// has nothing to delete.
func removeStagedProgram(label string) error {
	mesh, err := hostregistry.Mesh.Dir()
	if err != nil {
		return err
	}
	target := filepath.Join(mesh, stagedProgramDirName, label)
	if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove staged program %q: %w", target, err)
	}
	return nil
}

func stagedProgramDir() (string, error) {
	mesh, err := hostregistry.Mesh.Dir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(mesh, stagedProgramDirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create staging dir %s: %w", dir, err)
	}
	return dir, nil
}

func stagedProgramCurrent(target string, data []byte) (bool, error) {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect staged program %q: %w", target, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 || info.Size() != int64(len(data)) {
		return false, nil
	}
	staged, err := os.ReadFile(target) //nolint:gosec // exact synckit-owned staged program path
	if err != nil {
		return false, fmt.Errorf("read staged program %q: %w", target, err)
	}
	return bytes.Equal(staged, data), nil
}

// canonicalStagedProgram resolves the staging directory but never the final
// component, so a link substituted for the copy can never become the program
// launchd registers — which launchd refuses outright anywhere in a program path.
func canonicalStagedProgram(target string) (string, error) {
	dir, err := filepath.EvalSymlinks(filepath.Dir(target))
	if err != nil {
		return "", fmt.Errorf("resolve staging dir: %w", err)
	}
	resolved := filepath.Join(dir, filepath.Base(target))
	info, err := os.Lstat(resolved)
	if err != nil {
		return "", fmt.Errorf("inspect staged program %q: %w", resolved, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("staged program %q is not a regular file", resolved)
	}
	return resolved, nil
}

func helperProgram(label, binary string) (string, error) {
	source, err := canonicalHelperProgram(binary)
	if err != nil {
		return "", err
	}
	if bundledExecutable(source) {
		return source, nil
	}
	program, err := stageProgram(label, source)
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
