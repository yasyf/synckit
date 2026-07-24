package daemon

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRemoteRPCRejectsUnknownServiceBeforeHello(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	var output bytes.Buffer
	err := serveRemoteRPC(t.Context(), bytes.NewReader(nil), &output, "missing")
	if err == nil || !strings.Contains(err.Error(), "not registered") || output.Len() != 0 {
		t.Fatalf("serveRemoteRPC = %v, output=%q", err, output.Bytes())
	}
}

func TestRemoteRPCRejectsSpawnedServiceBeforeHello(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	directory, err := ensureManifestsDir()
	if err != nil {
		t.Fatal(err)
	}
	payload := `{"name":"spawned","binary":"/bin/false","watch":{"debounce":"1s"},` +
		`"service":{"kind":"spawned","schema_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`
	if err := os.WriteFile(filepath.Join(directory, "spawned.json"), []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err = serveRemoteRPC(t.Context(), bytes.NewReader(nil), &output, "spawned")
	if err == nil || !strings.Contains(err.Error(), "not resident") || output.Len() != 0 {
		t.Fatalf("serveRemoteRPC = %v, output=%q", err, output.Bytes())
	}
}
