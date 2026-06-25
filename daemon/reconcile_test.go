package daemon

import (
	"context"
	"testing"

	"github.com/yasyf/synckit/hostregistry"
)

func TestReconcileOne(t *testing.T) {
	dir := t.TempDir()
	binary, record := stubConsumer(t, dir)

	res := reconcileOne(context.Background(), hostregistry.NewExecRunner(), testManifest(binary))
	if res.Err != "" {
		t.Fatalf("reconcileOne err = %q, want none", res.Err)
	}
	if res.Name != "stub" {
		t.Errorf("name = %q, want stub", res.Name)
	}
	got := readRecord(t, record)
	if len(got) != 1 || got[0] != "reconcile" {
		t.Fatalf("reconcile shelled %v, want one %q", got, "reconcile")
	}
}

func TestReconcileOneError(t *testing.T) {
	res := reconcileOne(context.Background(), hostregistry.NewExecRunner(), testManifest("/nonexistent/consumer-binary"))
	if res.Err == "" {
		t.Fatal("reconcileOne err = empty, want a failure for a missing binary")
	}
	if res.Name != "stub" {
		t.Errorf("name = %q, want stub", res.Name)
	}
}
