package hostregistry

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

const tailscaleSelfJSON = `{"Self":{"DNSName":"yasyf.tail71af5d.ts.net.","HostName":"yBook Pro"}}`

func TestDetectSelf(t *testing.T) {
	r := NewMockRunner().
		OnLocal("tailscale status --json", tailscaleSelfJSON, nil).
		OnLocal("id -un", "yasyf\n", nil)

	self, err := DetectSelf(context.Background(), r)
	if err != nil {
		t.Fatalf("DetectSelf: %v", err)
	}
	if self != "yasyf@yasyf.tail71af5d.ts.net" {
		t.Fatalf("self = %q, want %q", self, "yasyf@yasyf.tail71af5d.ts.net")
	}
}

func TestDetectSelfTailscaleError(t *testing.T) {
	r := NewMockRunner().
		OnLocal("tailscale status --json", "", errors.New("exec: tailscale: not found"))

	_, err := DetectSelf(context.Background(), r)
	if err == nil {
		t.Fatal("expected error when tailscale is absent")
	}
	if !strings.Contains(err.Error(), "--self") {
		t.Fatalf("error %q should mention --self override", err)
	}
}

func TestVerify(t *testing.T) {
	cases := []struct {
		name             string
		runner           *MockRunner
		wantReachable    bool
		wantBootstrapped bool
		wantVersion      string
		wantErr          bool
	}{
		{
			name: "bootstrapped",
			runner: NewMockRunner().
				OnSSH("command -v synckit", "/opt/homebrew/bin/synckit\n", nil).
				OnSSH("synckit --version", "synckit 1.2.3\n", nil),
			wantReachable:    true,
			wantBootstrapped: true,
			wantVersion:      "synckit 1.2.3",
		},
		{
			name: "reachable but not installed",
			runner: NewMockRunner().
				OnSSH("command -v synckit", "", errors.New("exit status 1")).
				OnSSH("true", "", nil),
			wantReachable:    true,
			wantBootstrapped: false,
		},
		{
			name: "unreachable",
			runner: NewMockRunner().
				OnSSH("command -v synckit", "", errors.New("exit status 1")).
				OnSSH("true", "", errors.New("connection refused")),
			wantReachable:    false,
			wantBootstrapped: false,
			wantErr:          true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := testCfg.Verify(context.Background(), tc.runner, "yasyf@host")
			if res.Target != "yasyf@host" {
				t.Fatalf("Target = %q, want %q", res.Target, "yasyf@host")
			}
			if res.Reachable != tc.wantReachable {
				t.Fatalf("Reachable = %v, want %v", res.Reachable, tc.wantReachable)
			}
			if res.Bootstrapped != tc.wantBootstrapped {
				t.Fatalf("Bootstrapped = %v, want %v", res.Bootstrapped, tc.wantBootstrapped)
			}
			if res.Version != tc.wantVersion {
				t.Fatalf("Version = %q, want %q", res.Version, tc.wantVersion)
			}
			if tc.wantErr && res.Err == nil {
				t.Fatal("expected Err to be set")
			}
			if !tc.wantErr && res.Err != nil {
				t.Fatalf("unexpected Err: %v", res.Err)
			}
		})
	}
}

// TestVerifyProbesConfigBinary pins the binary parameterization: Verify and
// RemoteInstalled must shell the Config's Binary, not its Name. The shared mesh's
// config dir is named "synckit" but the installed binary is "synckitd", so a probe
// keyed on Name would mislabel a bootstrapped host as "reachable, not installed".
func TestVerifyProbesConfigBinary(t *testing.T) {
	cfg := Config{Name: "synckit", Binary: "synckitd"}

	r := NewMockRunner().
		OnSSH("command -v synckitd", "/opt/homebrew/bin/synckitd\n", nil).
		OnSSH("synckitd --version", "synckitd 9.9.9\n", nil)

	res := cfg.Verify(context.Background(), r, "yasyf@host")
	if !res.Bootstrapped || res.Version != "synckitd 9.9.9" {
		t.Fatalf("Verify with Binary=synckitd: %+v", res)
	}
	cmds := r.SSHCmds("yasyf@host")
	want := []string{"command -v synckitd", "synckitd --version"}
	if len(cmds) != len(want) {
		t.Fatalf("ssh cmds = %v, want %v", cmds, want)
	}
	for i := range want {
		if cmds[i] != want[i] {
			t.Fatalf("ssh cmd[%d] = %q, want %q", i, cmds[i], want[i])
		}
	}

	r2 := NewMockRunner().OnSSH("command -v synckitd", "/opt/homebrew/bin/synckitd\n", nil)
	if !cfg.RemoteInstalled(context.Background(), r2, "yasyf@host") {
		t.Fatal("RemoteInstalled with Binary=synckitd should report installed")
	}
	if got := r2.SSHCmds("yasyf@host"); len(got) != 1 || got[0] != "command -v synckitd" {
		t.Fatalf("RemoteInstalled ssh cmds = %v, want [command -v synckitd]", got)
	}
}

func TestVerifyVersionProbeError(t *testing.T) {
	r := NewMockRunner().
		OnSSH("command -v synckit", "/opt/homebrew/bin/synckit\n", nil).
		OnSSH("synckit --version", "", errors.New("exit status 2"))

	res := testCfg.Verify(context.Background(), r, "yasyf@host")
	if !res.Reachable || !res.Bootstrapped {
		t.Fatalf("install succeeded, want reachable+bootstrapped, got %+v", res)
	}
	if res.Version != "" {
		t.Fatalf("Version = %q, want empty on probe error", res.Version)
	}
	if res.Err != nil {
		t.Fatalf("version probe error must not surface: %v", res.Err)
	}
}

func TestVerifyAll(t *testing.T) {
	hosts := []string{"up@one", "down@two", "up@three"}

	r := NewMockRunner().
		OnSSH("command -v synckit", "/opt/homebrew/bin/synckit\n", nil).
		OnSSH("synckit --version", "synckit 1.0.0\n", nil)
	wrapped := &targetFailingRunner{Runner: r, failTarget: "down@two"}

	got := testCfg.VerifyAll(context.Background(), wrapped, hosts)
	if len(got) != len(hosts) {
		t.Fatalf("got %d results, want %d", len(got), len(hosts))
	}
	for i, h := range hosts {
		if got[i].Target != h {
			t.Fatalf("result[%d].Target = %q, want %q (order not preserved)", i, got[i].Target, h)
		}
	}
	if !got[0].Reachable || !got[0].Bootstrapped {
		t.Fatalf("up@one should be green, got %+v", got[0])
	}
	if !got[2].Reachable || !got[2].Bootstrapped {
		t.Fatalf("up@three should be green, got %+v", got[2])
	}
	if got[1].Reachable || got[1].Bootstrapped || got[1].Err == nil {
		t.Fatalf("down@two should be red with Err set, got %+v", got[1])
	}
}

func TestRemoveHost(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	initializeTestState(t, testCfg)

	registerTestHost(t, testCfg, "a@host")
	registerTestHost(t, testCfg, "b@host")

	if err := testCfg.RemoveHost(context.Background(), "a@host"); err != nil {
		t.Fatalf("RemoveHost: %v", err)
	}

	persisted, err := testCfg.Load()
	if err != nil {
		t.Fatalf("load persisted registry: %v", err)
	}
	if contains(persisted.Hosts, "a@host") {
		t.Fatalf("host not removed: %v", persisted.Hosts)
	}
	if !contains(persisted.Hosts, "b@host") {
		t.Fatalf("unrelated host dropped: %v", persisted.Hosts)
	}
}

func TestUpdateRejectsExtraTopLevelKey(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	initializeTestState(t, testCfg)
	path, err := testCfg.Path()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path) //nolint:gosec // temp state path from the fixed test Config
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.TrimSuffix(string(data), "\n"))
	data = append(data[:len(data)-1], []byte(`,"foreign":{}}\n`)...)
	if err := os.WriteFile(path, data, 0o600); err != nil { //nolint:gosec // temp state path from the fixed test Config
		t.Fatal(err)
	}
	if _, err := testCfg.Update(context.Background(), func(*Registry) error { return nil }); !errors.Is(err, ErrStateSchema) {
		t.Fatalf("Update extra state = %v, want ErrStateSchema", err)
	}
}

// targetFailingRunner wraps a Runner and forces SSH to one target to fail.
type targetFailingRunner struct {
	Runner
	failTarget string
}

func (w *targetFailingRunner) SSH(ctx context.Context, target, remoteCmd string) (string, error) {
	out, err := w.Runner.SSH(ctx, target, remoteCmd)
	if target == w.failTarget {
		return out, errors.New("connection refused")
	}
	return out, err
}

func contains(s []string, want string) bool {
	for _, v := range s {
		if v == want {
			return true
		}
	}
	return false
}

// TestVerifyBinary pins the binary parameterization: VerifyBinary must probe the
// given binary rather than the Config's Name, so a mesh Config whose Name is not
// an installed tool can still verify an installed consumer binary.
func TestVerifyBinary(t *testing.T) {
	r := NewMockRunner().
		OnSSH("command -v cookiesync", "/opt/homebrew/bin/cookiesync\n", nil).
		OnSSH("cookiesync --version", "cookiesync 9.9.9\n", nil)

	res := Mesh.VerifyBinary(context.Background(), r, "yasyf@host", "cookiesync")
	if !res.Reachable || !res.Bootstrapped {
		t.Fatalf("VerifyBinary: %+v, want reachable+bootstrapped", res)
	}
	if res.Version != "cookiesync 9.9.9" {
		t.Fatalf("Version = %q, want cookiesync 9.9.9", res.Version)
	}
	cmds := r.SSHCmds("yasyf@host")
	want := []string{"command -v cookiesync", "cookiesync --version"}
	if len(cmds) != len(want) {
		t.Fatalf("ssh cmds = %v, want %v", cmds, want)
	}
	for i := range want {
		if cmds[i] != want[i] {
			t.Fatalf("ssh cmd[%d] = %q, want %q", i, cmds[i], want[i])
		}
	}
}
