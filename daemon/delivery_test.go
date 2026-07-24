package daemon

import (
	"strings"
	"testing"

	"github.com/yasyf/synckit/syncservice"
)

func TestDeliveryStoreRecoversPendingAndExactAck(t *testing.T) {
	store := newDeliveryStore(t.TempDir())
	change, err := syncservice.NewExportedChange(
		"reposync", strings.Repeat("a", 64), syncservice.ChangeDelta,
		syncservice.NewRevision(4), syncservice.NewRevision(5), []byte(`{"repos":{}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	change, err = syncservice.BindDelivery(change, "me@home")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.putPending(t.Context(), "peer@home", change); err != nil {
		t.Fatal(err)
	}

	reopened := newDeliveryStore(t.TempDir())
	reopened.path = store.path
	reopened.lock = store.lock
	acked, pending, err := reopened.load(t.Context(), "reposync", "peer@home")
	if err != nil {
		t.Fatal(err)
	}
	if acked != syncservice.NewRevision(0) || pending == nil || pending.ChangeID != change.ChangeID {
		t.Fatalf("recovered ack=%q pending=%#v", acked, pending)
	}
	if err := reopened.acknowledge(t.Context(), "peer@home", change, syncservice.ApplyResult{
		AckedRevision: change.SourceRevision,
	}); err != nil {
		t.Fatal(err)
	}
	acked, pending, err = store.load(t.Context(), "reposync", "peer@home")
	if err != nil {
		t.Fatal(err)
	}
	if acked != change.SourceRevision || pending != nil {
		t.Fatalf("settled ack=%q pending=%#v", acked, pending)
	}
}
