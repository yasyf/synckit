package daemon

import (
	"context"
	"strings"
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

func TestReconcileOneVersionSkew(t *testing.T) {
	fake := newFakeConsumer()
	fake.capsVersion = 999
	fakeMesh(t, map[string]*fakeConsumer{"me@self": fake})

	res := reconcileOne(context.Background(), testManifest(), "me@self")
	if res.Err == "" {
		t.Fatal("reconcileOne err = empty, want a protocol skew error")
	}
	if !strings.Contains(res.Err, "skew") || !strings.Contains(res.Err, "999") {
		t.Errorf("err = %q, want it to name the skew and version 999", res.Err)
	}
	if _, calls := fake.reconcileOrigin(); calls != 0 {
		t.Errorf("reconcile ran %d times on a skew, want 0", calls)
	}
}

func TestReconcileOneCapabilitiesError(t *testing.T) {
	fake := newFakeConsumer()
	fake.capsFails = 1 // the single Capabilities call fails
	fakeMesh(t, map[string]*fakeConsumer{"me@self": fake})

	res := reconcileOne(context.Background(), testManifest(), "me@self")
	if res.Err == "" {
		t.Fatal("reconcileOne err = empty, want a capabilities failure")
	}
	if !strings.Contains(res.Err, "capabilities") {
		t.Errorf("err = %q, want it to mention capabilities", res.Err)
	}
	if _, calls := fake.reconcileOrigin(); calls != 0 {
		t.Errorf("reconcile ran %d times after a capabilities failure, want 0", calls)
	}
}
