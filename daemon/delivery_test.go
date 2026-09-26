package daemon

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/syncservice"
)

const (
	testService = "reposync"
	testPeer    = "peer@home"
)

func boundChange(t *testing.T, kind syncservice.ChangeKind, base, source uint64, payload string, roots ...artifact.Ref) syncservice.ChangeEnvelope {
	t.Helper()
	change, err := syncservice.NewExportedArtifactChange(
		testService, strings.Repeat("a", 64), kind,
		syncservice.NewRevision(base), syncservice.NewRevision(source), []byte(payload), roots,
	)
	if err != nil {
		t.Fatal(err)
	}
	change, err = syncservice.BindDelivery(change, "me@home")
	if err != nil {
		t.Fatal(err)
	}
	return change
}

func blobRef(content string) artifact.Ref {
	return artifact.Ref{Digest: artifact.Sum([]byte(content)), Kind: artifact.KindBlob, Size: int64(len(content))}
}

func pendingFiles(t *testing.T, store *deliveryStore) []string {
	t.Helper()
	entries, err := os.ReadDir(store.pending)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestDeliveryStoreStagesAndAcknowledges(t *testing.T) {
	store := newDeliveryStore(t.TempDir())
	change := boundChange(t, syncservice.ChangeDelta, 0, 5, `{"repos":{}}`, blobRef("a"))
	if err := store.stage(t.Context(), testPeer, "", change); err != nil {
		t.Fatal(err)
	}
	reopened := newDeliveryStore(store.dir)
	record, pending, err := reopened.load(t.Context(), testService, testPeer)
	if err != nil {
		t.Fatal(err)
	}
	if record.Acked != syncservice.NewRevision(0) || record.Generation != 1 || pending == nil || pending.ChangeID != change.ChangeID ||
		string(pending.Payload) != `{"repos":{}}` || len(record.Pending.Roots) != 1 || record.Pending.Superseded != 0 {
		t.Fatalf("recovered record=%#v pending=%#v", record, pending)
	}
	if err := reopened.acknowledge(t.Context(), testPeer, change, syncservice.ApplyResult{AckedRevision: change.SourceRevision}); err != nil {
		t.Fatal(err)
	}
	record, pending, err = store.load(t.Context(), testService, testPeer)
	if err != nil {
		t.Fatal(err)
	}
	if record.Acked != syncservice.NewRevision(5) || record.AckedChangeID != change.ChangeID || record.AckedAt.IsZero() ||
		record.Generation != 2 || pending != nil || record.Pending != nil {
		t.Fatalf("settled record=%#v pending=%#v", record, pending)
	}
	if files := pendingFiles(t, store); len(files) != 0 {
		t.Fatalf("pending files after ack = %v, want none", files)
	}
}

func TestDeliveryStoreStageRules(t *testing.T) {
	tests := []struct {
		name       string
		acked      uint64
		held       *syncservice.ChangeEnvelope
		expectHeld bool
		kind       syncservice.ChangeKind
		source     uint64
		want       error
		superseded uint64
	}{
		{name: "fresh delta", kind: syncservice.ChangeDelta, source: 5},
		{name: "expected mismatch", held: &syncservice.ChangeEnvelope{Kind: syncservice.ChangeDelta, SourceRevision: "5"}, kind: syncservice.ChangeSnapshot, source: 6, want: errPendingChanged},
		{name: "not above acked", acked: 5, kind: syncservice.ChangeDelta, source: 5, want: errSourceRegressed},
		{name: "delta replacement", held: &syncservice.ChangeEnvelope{Kind: syncservice.ChangeDelta, SourceRevision: "5"}, expectHeld: true, kind: syncservice.ChangeDelta, source: 6, want: errSupersede},
		{name: "older snapshot", held: &syncservice.ChangeEnvelope{Kind: syncservice.ChangeSnapshot, SourceRevision: "6"}, expectHeld: true, kind: syncservice.ChangeSnapshot, source: 5, want: errSupersede},
		{name: "same snapshot source", held: &syncservice.ChangeEnvelope{Kind: syncservice.ChangeSnapshot, SourceRevision: "6"}, expectHeld: true, kind: syncservice.ChangeSnapshot, source: 6, want: errSupersede},
		{name: "snapshot over delta at same source", held: &syncservice.ChangeEnvelope{Kind: syncservice.ChangeDelta, SourceRevision: "6"}, expectHeld: true, kind: syncservice.ChangeSnapshot, source: 6, superseded: 1},
		{name: "newer snapshot", held: &syncservice.ChangeEnvelope{Kind: syncservice.ChangeSnapshot, SourceRevision: "6"}, expectHeld: true, kind: syncservice.ChangeSnapshot, source: 7, superseded: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newDeliveryStore(t.TempDir())
			if tt.acked > 0 {
				seed := boundChange(t, syncservice.ChangeSnapshot, 0, tt.acked, `{"seed":true}`)
				if err := store.stage(t.Context(), testPeer, "", seed); err != nil {
					t.Fatal(err)
				}
				if err := store.acknowledge(t.Context(), testPeer, seed, syncservice.ApplyResult{AckedRevision: seed.SourceRevision}); err != nil {
					t.Fatal(err)
				}
			}
			expected := ""
			var held syncservice.ChangeEnvelope
			if tt.held != nil {
				source, _ := tt.held.SourceRevision.Uint64()
				var base uint64
				if tt.held.Kind == syncservice.ChangeDelta {
					base = tt.acked
				}
				held = boundChange(t, tt.held.Kind, base, source, `{"held":true}`)
				if err := store.stage(t.Context(), testPeer, "", held); err != nil {
					t.Fatal(err)
				}
				if tt.expectHeld {
					expected = held.ChangeID
				}
			}
			var base uint64
			if tt.kind == syncservice.ChangeDelta {
				base = tt.acked
			}
			next := boundChange(t, tt.kind, base, tt.source, `{"next":true}`, blobRef("next"))
			err := store.stage(t.Context(), testPeer, expected, next)
			if !errors.Is(err, tt.want) {
				t.Fatalf("stage err = %v, want %v", err, tt.want)
			}
			record, pending, err := store.load(t.Context(), testService, testPeer)
			if err != nil {
				t.Fatal(err)
			}
			wantID := next.ChangeID
			if tt.want != nil {
				wantID = held.ChangeID
			}
			if tt.want != nil && tt.held == nil {
				if pending != nil {
					t.Fatalf("refused stage left pending %#v", pending)
				}
				return
			}
			if pending == nil || pending.ChangeID != wantID || record.Pending.Superseded != tt.superseded {
				t.Fatalf("pending=%#v record=%#v, want change %s superseded %d", pending, record.Pending, wantID, tt.superseded)
			}
			if files := pendingFiles(t, store); len(files) != 1 || files[0] != wantID+".json" {
				t.Fatalf("pending files = %v, want [%s.json]", files, wantID)
			}
		})
	}
}

func TestDeliveryStoreStaleAckAfterSupersede(t *testing.T) {
	store := newDeliveryStore(t.TempDir())
	first := boundChange(t, syncservice.ChangeSnapshot, 0, 5, `{"v":5}`)
	second := boundChange(t, syncservice.ChangeSnapshot, 0, 6, `{"v":6}`)
	if err := store.stage(t.Context(), testPeer, "", first); err != nil {
		t.Fatal(err)
	}
	if err := store.stage(t.Context(), testPeer, first.ChangeID, second); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	err = store.acknowledge(t.Context(), testPeer, first, syncservice.ApplyResult{AckedRevision: first.SourceRevision})
	if !errors.Is(err, errPendingChanged) {
		t.Fatalf("stale ack err = %v, want errPendingChanged", err)
	}
	after, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("stale ack mutated state\nbefore %s\nafter  %s", before, after)
	}
	record, pending, err := store.load(t.Context(), testService, testPeer)
	if err != nil {
		t.Fatal(err)
	}
	if record.Acked != syncservice.NewRevision(0) || pending == nil || pending.ChangeID != second.ChangeID || record.Pending.Superseded != 1 {
		t.Fatalf("record=%#v pending=%#v", record, pending)
	}
}

func TestDeliveryStoreMigratesV1Once(t *testing.T) {
	store := newDeliveryStore(t.TempDir())
	legacy := boundChange(t, syncservice.ChangeDelta, 3, 4, `{}`)
	v1 := `{"identity":"synckit-delivery-v1","version":1,"records":[` +
		`{"service_id":"reposync","peer":"peer@home","acked_revision":"3","pending":` + string(mustJSON(t, legacy)) + `},` +
		`{"service_id":"cookiesync","peer":"peer@home","acked_revision":"9"}]}`
	if err := os.WriteFile(store.v1Path, []byte(v1), 0o600); err != nil {
		t.Fatal(err)
	}
	records, err := store.records(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].ServiceID != "cookiesync" || records[0].Acked != "9" ||
		records[1].ServiceID != testService || records[1].Acked != "3" || records[1].Pending != nil {
		t.Fatalf("migrated records = %#v", records)
	}
	if _, err := os.Stat(store.v1Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("v1 state still present: %v", err)
	}
	if err := os.WriteFile(store.v1Path, []byte(v1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.records(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(store.v1Path); err != nil {
		t.Fatalf("a second v1 file was migrated over existing v2 state: %v", err)
	}
}

func TestDeliveryStoreSweepsOrphans(t *testing.T) {
	store := newDeliveryStore(t.TempDir())
	change := boundChange(t, syncservice.ChangeSnapshot, 0, 2, `{}`)
	if err := store.stage(t.Context(), testPeer, "", change); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(store.pending, strings.Repeat("b", 64)+".json")
	if err := os.WriteFile(orphan, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.load(t.Context(), testService, testPeer); err != nil {
		t.Fatal(err)
	}
	if files := pendingFiles(t, store); len(files) != 1 || files[0] != change.ChangeID+".json" {
		t.Fatalf("pending files after sweep = %v", files)
	}
}

func TestDeliveryStoreStrictDecode(t *testing.T) {
	header := `{"identity":"synckit-delivery-v2","version":2,"records":`
	tests := []struct {
		name  string
		state string
	}{
		{"unknown field", header + `[],"extra":1}`},
		{"trailing data", header + `[]} {}`},
		{"wrong identity", `{"identity":"synckit-delivery-v1","version":2,"records":[]}`},
		{"wrong version", `{"identity":"synckit-delivery-v2","version":1,"records":[]}`},
		{"null records", header + `null}`},
		{"duplicate record", header + `[{"service_id":"s","peer":"p","generation":0,"acked_revision":"0","acked_change_id":"","acked_at":"0001-01-01T00:00:00Z"},` +
			`{"service_id":"s","peer":"p","generation":0,"acked_revision":"0","acked_change_id":"","acked_at":"0001-01-01T00:00:00Z"}]}`},
		{"pending not above acked", header + `[{"service_id":"s","peer":"p","generation":1,"acked_revision":"4","acked_change_id":"","acked_at":"0001-01-01T00:00:00Z",` +
			`"pending":{"change_id":"` + strings.Repeat("c", 64) + `","kind":"snapshot","base_revision":"0","source_revision":"4","roots":null,"staged_at":"0001-01-01T00:00:00Z","superseded":0}}]}`},
		{"escaping change id", header + `[{"service_id":"s","peer":"p","generation":1,"acked_revision":"0","acked_change_id":"","acked_at":"0001-01-01T00:00:00Z",` +
			`"pending":{"change_id":"../x","kind":"snapshot","base_revision":"0","source_revision":"4","roots":null,"staged_at":"0001-01-01T00:00:00Z","superseded":0}}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newDeliveryStore(t.TempDir())
			if err := os.WriteFile(store.path, []byte(tt.state), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.load(t.Context(), "s", "p"); err == nil {
				t.Fatal("load accepted invalid state")
			}
		})
	}
}
