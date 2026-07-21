package daemon

import (
	"context"
	"testing"
)

func TestReconcileOne(t *testing.T) {
	fake := newFakeConsumer()
	fakeMesh(t, map[string]*fakeConsumer{"me@self": fake})

	res := reconcileOne(context.Background(), testManifest(), "me@self")
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
