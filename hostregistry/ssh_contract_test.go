package hostregistry

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestRemoteSSHArgvIsSealedAndPinsCanonicalHostKey(t *testing.T) {
	knownHosts := privateKnownHosts(t)
	fact, err := NewSSHHostFact(
		"yasyf@home.tail.example", "/Applications/Synckit.app/Contents/MacOS/synckitd",
		[]string{"yasyf@home.local"},
	)
	if err != nil {
		t.Fatal(err)
	}
	argv, err := RemoteSSHArgv(fact, "home.local", knownHosts, "cookie-sync")
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := []string{sshExecutable, "-F", "/dev/null", "-T"}
	if !slices.Equal(argv[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("argv prefix = %q", argv[:len(wantPrefix)])
	}
	for _, required := range []string{
		"StrictHostKeyChecking=yes", "UserKnownHostsFile=" + knownHosts,
		"GlobalKnownHostsFile=/dev/null", "HostKeyAlias=home.tail.example",
		"KnownHostsCommand=none", "UpdateHostKeys=no", "CheckHostIP=no",
		"IdentityAgent=none", "ProxyCommand=none", "ProxyJump=none",
		"CanonicalizeHostname=no", "ForwardAgent=no", "ForwardX11=no",
		"ForwardX11Trusted=no", "ClearAllForwardings=yes", "RequestTTY=no",
		"EscapeChar=none",
	} {
		if !slices.Contains(argv, required) {
			t.Fatalf("argv lacks %q: %q", required, argv)
		}
	}
	wantTail := []string{
		"-l", "yasyf", "home.local",
		"'/Applications/Synckit.app/Contents/MacOS/synckitd' 'rpc-serve-v1' 'cookie-sync'",
	}
	if !slices.Equal(argv[len(argv)-len(wantTail):], wantTail) {
		t.Fatalf("argv tail = %q, want %q", argv[len(argv)-len(wantTail):], wantTail)
	}
}

func TestSSHHostFactRequiresExplicitStableIdentity(t *testing.T) {
	for _, test := range []struct {
		identity string
		path     string
		addrs    []string
	}{
		{identity: "home.example", path: "/bin/synckitd"},
		{identity: "-o@home.example", path: "/bin/synckitd"},
		{identity: "me@home.example", path: "synckitd"},
		{identity: "me@home.example", path: "/bin/../bin/synckitd"},
		{identity: "me@home.example", path: "/bin/synckitd", addrs: []string{"other@home.local"}},
	} {
		if _, err := NewSSHHostFact(test.identity, test.path, test.addrs); err == nil {
			t.Fatalf("NewSSHHostFact(%q, %q, %q) succeeded", test.identity, test.path, test.addrs)
		}
	}
}

func TestKnownHostsRejectsLooseAndSymlinkedFiles(t *testing.T) {
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "known_hosts")
	if err := os.WriteFile(path, []byte("host key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil { //nolint:gosec // exercise fail-closed permissions
		t.Fatal(err)
	}
	if err := ValidateKnownHosts(path); err == nil {
		t.Fatal("mode-0644 known_hosts accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateKnownHosts(path); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "known_hosts-link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if err := ValidateKnownHosts(link); err == nil {
		t.Fatal("symlinked known_hosts accepted")
	}
}

func privateKnownHosts(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "known_hosts")
	if err := os.WriteFile(path, []byte("home.tail.example key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
