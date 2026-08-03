package daemon

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/synckit/hostregistry"
)

// TestReconcileAllConvergesPastAManifestThatWillNotLoad covers the daemon's
// serving path, not just discovery: the tick loads manifests every pass, so a
// stale file left by an older synckit would otherwise fail the whole pass and
// wedge the mesh for every consumer. The bad file is skipped with a logged
// error naming it, and the healthy consumer still reconciles.
func TestReconcileAllConvergesPastAManifestThatWillNotLoad(t *testing.T) {
	useMesh(t)
	fake := newFakeConsumer()
	fakeMesh(t, map[string]*fakeConsumer{"me@self": fake})
	if err := hostregistry.Mesh.InitializeState(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := hostregistry.Mesh.Update(t.Context(), func(g *hostregistry.Registry) error {
		g.Self = "me@self"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	directory, err := ensureManifestsDir()
	if err != nil {
		t.Fatal(err)
	}
	good, err := json.Marshal(testManifest())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "stub.json"), good, 0o600); err != nil {
		t.Fatal(err)
	}
	stale := `{"name":"reposync","binary":"reposync","watch":{"backend":"fsnotify","debounce":"15s"},` +
		`"service":{"transport":"stdio","serve_args":["rpc-serve"]}}`
	if err := os.WriteFile(filepath.Join(directory, "reposync.json"), []byte(stale), 0o600); err != nil {
		t.Fatal(err)
	}

	results, err := reconcileAll(t.Context(), testProcessScope(t))
	if err != nil {
		t.Fatalf("reconcileAll error = %v, want the stale manifest skipped", err)
	}
	if len(results) != 1 || results[0].Name != "stub" || results[0].Err != "" {
		t.Fatalf("results = %#v, want stub alone reconciled", results)
	}
	if _, calls := fake.reconcileOrigin(); calls != 1 {
		t.Fatalf("reconcile calls = %d, want 1", calls)
	}
}

func TestReconcileOne(t *testing.T) {
	fake := newFakeConsumer()
	fakeMesh(t, map[string]*fakeConsumer{"me@self": fake})

	ctx := t.Context()
	res := reconcileOne(
		ctx, testProcessScope(t), testManifest(),
		&hostregistry.Registry{Self: "me@self"}, newDeliveryStore(t.TempDir()),
	)
	if res.Err != "" {
		t.Fatalf("reconcileOne err = %q, want none", res.Err)
	}
	if res.Name != "stub" {
		t.Errorf("name = %q, want stub", res.Name)
	}
	origin, calls := fake.reconcileOrigin()
	if calls != 1 {
		t.Fatalf("reconcile calls = %d, want 1", calls)
	}
	// A reconcile is a full pass: origin is empty.
	if origin != "" {
		t.Errorf("reconcile origin = %q, want empty", origin)
	}
}
