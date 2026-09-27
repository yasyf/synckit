package syncservice

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
)

const (
	testSchema                   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	blobDigest   artifact.Digest = "6c87f68371b28954707ebb92afee7ccffb74c6f71ec8fea8a98cf6104289585b"
	manifestRoot artifact.Digest = "05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f"
)

var testRoots = []artifact.Ref{
	{Digest: blobDigest, Kind: artifact.KindBlob, Size: 5},
	{Digest: manifestRoot, Kind: artifact.KindManifest, Size: 10},
}

func boundChange(t *testing.T, roots []artifact.Ref) ChangeEnvelope {
	t.Helper()
	change, err := NewExportedArtifactChange("fake", testSchema, ChangeSnapshot, NewRevision(0), NewRevision(1), []byte(`{}`), roots)
	if err != nil {
		t.Fatal(err)
	}
	change, err = BindDelivery(change, "host-a")
	if err != nil {
		t.Fatal(err)
	}
	return change
}

func TestChangeIDBindsArtifactRoots(t *testing.T) {
	tests := []struct {
		name  string
		roots []artifact.Ref
		want  string
	}{
		{"no roots keeps the v1 id", nil, "47038c4783c7a2eb5de62a09fc6c92dca4a87a44741f8c9d3c2d0c291e49892f"},
		{"empty roots keep the v1 id", []artifact.Ref{}, "47038c4783c7a2eb5de62a09fc6c92dca4a87a44741f8c9d3c2d0c291e49892f"},
		{"roots hash the v2 domain", testRoots, "ab41cbbc99d57025a047a9e9b6f0bbcbfa5658684c7c2526875ded8f6cb08ad1"},
		{"root order changes the id", []artifact.Ref{testRoots[1], testRoots[0]}, "ea217014fcbef6f4ffa8b82f8a78d1ef8bc7a4fc9a2915aecfd3a5655494b2e4"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := boundChange(t, tt.roots).ChangeID; got != tt.want {
				t.Fatalf("ChangeID = %s, want %s", got, tt.want)
			}
		})
	}
	v1, err := NewExportedChange("fake", testSchema, ChangeSnapshot, NewRevision(0), NewRevision(1), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	v1, err = BindDelivery(v1, "host-a")
	if err != nil || v1.ChangeID != tests[0].want {
		t.Fatalf("v1 ChangeID = %s, %v", v1.ChangeID, err)
	}
	resized := []artifact.Ref{testRoots[0], {Digest: manifestRoot, Kind: artifact.KindManifest, Size: 11}}
	if boundChange(t, resized).ChangeID == tests[2].want {
		t.Fatal("root size change kept the ChangeID")
	}
}

func TestArtifactChangeValidation(t *testing.T) {
	tooMany := make([]artifact.Ref, artifact.MaxRoots+1)
	for i := range tooMany {
		tooMany[i] = artifact.Ref{Digest: artifact.Sum([]byte{byte(i), byte(i >> 8)}), Kind: artifact.KindBlob, Size: 1}
	}
	tests := []struct {
		name  string
		roots []artifact.Ref
		valid bool
	}{
		{"distinct roots", testRoots, true},
		{"max roots", tooMany[:artifact.MaxRoots], true},
		{"duplicate root", []artifact.Ref{testRoots[0], testRoots[0]}, false},
		{"too many roots", tooMany, false},
		{"invalid root kind", []artifact.Ref{{Digest: blobDigest, Kind: "tree", Size: 1}}, false},
		{"invalid root digest", []artifact.Ref{{Digest: "ABC", Kind: artifact.KindBlob, Size: 1}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			change, err := NewExportedArtifactChange("fake", testSchema, ChangeSnapshot, NewRevision(0), NewRevision(1), []byte(`{}`), tt.roots)
			if tt.valid != (err == nil) || (err != nil && !errors.Is(err, artifact.ErrInvalid)) {
				t.Fatalf("NewExportedArtifactChange() = %v, valid %v", err, tt.valid)
			}
			if !tt.valid {
				return
			}
			bound, err := BindDelivery(change, "host-a")
			if err != nil {
				t.Fatal(err)
			}
			if err := bound.Validate(true); err != nil {
				t.Fatalf("Validate(true) = %v", err)
			}
		})
	}
	change := boundChange(t, testRoots)
	change.Artifacts = append(change.Artifacts, testRoots[0])
	if err := change.Validate(true); !errors.Is(err, artifact.ErrInvalid) {
		t.Fatalf("Validate(true) with duplicate roots = %v", err)
	}
	roots := append([]artifact.Ref(nil), testRoots...)
	copied, err := NewExportedArtifactChange("fake", testSchema, ChangeSnapshot, NewRevision(0), NewRevision(1), []byte(`{}`), roots)
	if err != nil {
		t.Fatal(err)
	}
	roots[0].Size = 99
	if copied.Artifacts[0].Size != 5 {
		t.Fatal("NewExportedArtifactChange aliases the caller's roots")
	}
}

type artifactExporter struct{ fakeConsumer }

func (*artifactExporter) Export(_ context.Context, request ExportRequest) (ChangeEnvelope, error) {
	return NewExportedArtifactChange(request.ServiceID, request.SchemaFingerprint, ChangeSnapshot, NewRevision(0), NewRevision(1), []byte(`{}`), testRoots)
}

func TestArtifactsRefusedOnV1(t *testing.T) {
	ctx := context.Background()
	change := boundChange(t, testRoots)

	consumer := &artifactExporter{}
	dispatcher := rpc.NewDispatcher()
	RegisterConsumer(dispatcher, consumer)

	if _, err := NewClient(directTransport{dispatcher}).Apply(ctx, change); !errors.Is(err, ErrArtifactsOnV1) {
		t.Fatalf("Client.Apply() = %v, want ErrArtifactsOnV1", err)
	}
	params, err := structParams(change)
	if err != nil {
		t.Fatal(err)
	}
	response := dispatcher.Dispatch(ctx, &rpc.Request{Method: MethodApply, Params: params})
	if response.OK || response.Error != ErrArtifactsOnV1.Error() || consumer.applyOrigin != "" {
		t.Fatalf("apply.v1 handler = %+v, consumer applied from %q", response, consumer.applyOrigin)
	}
	request := ExportRequest{ServiceID: "fake", SchemaFingerprint: testSchema, SinceRevision: NewRevision(0)}
	if _, err := NewClient(directTransport{dispatcher}).Export(ctx, request); err == nil || !strings.Contains(err.Error(), ErrArtifactsOnV1.Error()) {
		t.Fatalf("export.v1 handler = %v, want refusal", err)
	}

	exported, err := json.Marshal(boundChangeExport(t))
	if err != nil {
		t.Fatal(err)
	}
	raw := &callRecordingTransport{response: &Response{OK: true, Result: exported}}
	if _, err := NewClient(raw).Export(ctx, request); !errors.Is(err, ErrArtifactsOnV1) {
		t.Fatalf("Client.Export() of an artifact change = %v, want ErrArtifactsOnV1", err)
	}
}

func boundChangeExport(t *testing.T) ChangeEnvelope {
	t.Helper()
	change, err := NewExportedArtifactChange("fake", testSchema, ChangeSnapshot, NewRevision(0), NewRevision(1), []byte(`{}`), testRoots)
	if err != nil {
		t.Fatal(err)
	}
	return change
}

func TestArtifactCapabilities(t *testing.T) {
	got := ArtifactCapabilities("cc-sync")
	want := []string{
		"svc.capabilities", "svc.list", "svc.reconcile",
		"synckit.syncservice.export.v1", "synckit.syncservice.apply.v1",
		"synckit.syncservice.export.v2", "synckit.syncservice.apply.v2",
		"synckit.net.status.v1", "synckit.artifact.closure.v1", "synckit.artifact.have.v1",
		"synckit.artifact.batch.build.v1", "synckit.artifact.batch.read.v1", "synckit.artifact.batch.drop.v1",
		"synckit.artifact.batch.begin.v1", "synckit.artifact.batch.put.v1", "synckit.artifact.batch.commit.v1",
		"synckit.artifact.pins.set.v1",
	}
	if got.Name != "cc-sync" || !reflect.DeepEqual(got.Methods, want) {
		t.Fatalf("ArtifactCapabilities() = %+v", got)
	}
	got.Methods[0] = "mutated"
	if AllMethods[0] != MethodCapabilities || artifact.Methods[0] != artifact.MethodNetStatus {
		t.Fatal("ArtifactCapabilities aliases a shared method list")
	}
}

func TestClientArtifactPayloadGolden(t *testing.T) {
	rt := &recordingTransport{}
	c := NewClient(rt)
	ctx := context.Background()
	change := boundChange(t, testRoots)
	partID := artifact.Digest(strings.Repeat("b", 64))
	sender := netpolicy.State{Status: netpolicy.StatusConnected}
	refs := `[{"digest":"` + string(blobDigest) + `","kind":"blob","size":5},{"digest":"` + string(manifestRoot) + `","kind":"manifest","size":10}]`
	senderJSON := `{"cellular":false,"constrained":false,"expensive":false,"manual_metered":false,"observed_at":"0001-01-01T00:00:00Z","restricted_epoch":0,"status":"connected"}`

	tests := []struct {
		name string
		call func()
		want string
	}{
		{"export v2", func() {
			_, _ = c.ExportV2(ctx, ExportRequest{ServiceID: "fake", SchemaFingerprint: testSchema, SinceRevision: NewRevision(4)})
		}, `{"method":"synckit.syncservice.export.v2","params":{"schema_fingerprint":"` + testSchema + `","service_id":"fake","since_revision":"4"}}`},
		{"apply v2", func() { _, _ = c.ApplyV2(ctx, change, 7) }, `{"method":"synckit.syncservice.apply.v2","params":{"admitted":7,"artifacts":` + refs + `,"base_revision":"0","change_id":"` + change.ChangeID + `","kind":"snapshot","origin":"host-a","payload":"e30=","payload_digest":"` + change.PayloadDigest + `","schema_fingerprint":"` + testSchema + `","service_id":"fake","source_revision":"1"}}`},
		{"net status", func() { _, _ = c.NetStatus(ctx) }, `{"method":"synckit.net.status.v1","params":null}`},
		{"closure", func() {
			_, _ = c.ArtifactClosure(ctx, artifact.ClosureParams{Roots: testRoots, After: 16384, Limit: 16384})
		}, `{"method":"synckit.artifact.closure.v1","params":{"after":16384,"limit":16384,"roots":` + refs + `}}`},
		{"have", func() { _, _ = c.ArtifactHave(ctx, []artifact.Digest{blobDigest}, 7) }, `{"method":"synckit.artifact.have.v1","params":{"admitted":7,"digests":["` + string(blobDigest) + `"]}}`},
		{"batch build", func() {
			_, _ = c.BatchBuild(ctx, []artifact.ObjectEntry{{Digest: blobDigest, Kind: artifact.KindBlob, Size: 5}})
		}, `{"method":"synckit.artifact.batch.build.v1","params":{"objects":[{"digest":"` + string(blobDigest) + `","kind":"blob","size":5}]}}`},
		{"batch read", func() { _, _ = c.BatchRead(ctx, partID, 2) }, `{"method":"synckit.artifact.batch.read.v1","params":{"id":"` + string(partID) + `","index":2}}`},
		{"batch drop", func() { _ = c.BatchDrop(ctx, partID) }, `{"method":"synckit.artifact.batch.drop.v1","params":{"id":"` + string(partID) + `"}}`},
		{"batch put", func() { _, _ = c.BatchPut(ctx, partID, 1, []byte("hi"), sender, 7) }, `{"method":"synckit.artifact.batch.put.v1","params":{"admitted":7,"data":"aGk=","id":"` + string(partID) + `","index":1,"sender":` + senderJSON + `}}`},
		{"batch commit", func() { _, _ = c.BatchCommit(ctx, partID, 7) }, `{"method":"synckit.artifact.batch.commit.v1","params":{"admitted":7,"id":"` + string(partID) + `"}}`},
		{"pins set", func() { _ = c.PinsSet(ctx, "synckit.delivery/host-b", testRoots[:1]) }, `{"method":"synckit.artifact.pins.set.v1","params":{"owner":"synckit.delivery/host-b","roots":[{"digest":"` + string(blobDigest) + `","kind":"blob","size":5}]}}`},
		{"pins clear", func() { _ = c.PinsSet(ctx, "synckit.delivery/host-b", nil) }, `{"method":"synckit.artifact.pins.set.v1","params":{"owner":"synckit.delivery/host-b","roots":null}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt.last = nil
			tt.call()
			if rt.last == nil {
				t.Fatal("client issued no request")
			}
			payload, err := rpc.EncodeRequest(rt.last)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if got := string(payload); got != tt.want {
				t.Fatalf("client request payload = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClientArtifactRefusesInvalidParams(t *testing.T) {
	rt := &recordingTransport{}
	c := NewClient(rt)
	ctx := context.Background()
	calls := map[string]func() error{
		"closure":     func() error { _, err := c.ArtifactClosure(ctx, artifact.ClosureParams{Roots: testRoots}); return err },
		"have":        func() error { _, err := c.ArtifactHave(ctx, []artifact.Digest{"x"}, 0); return err },
		"batch build": func() error { _, err := c.BatchBuild(ctx, nil); return err },
		"batch read":  func() error { _, err := c.BatchRead(ctx, blobDigest, -1); return err },
		"batch drop":  func() error { return c.BatchDrop(ctx, "") },
		"batch begin": func() error {
			_, err := c.BatchBegin(ctx, artifact.BatchDescriptor{}, netpolicy.State{}, 0)
			return err
		},
		"batch put": func() error {
			_, err := c.BatchPut(ctx, blobDigest, 0, nil, netpolicy.State{}, 0)
			return err
		},
		"batch commit": func() error { _, err := c.BatchCommit(ctx, "", 0); return err },
		"pins set":     func() error { return c.PinsSet(ctx, "", testRoots) },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			rt.last = nil
			if err := call(); !errors.Is(err, artifact.ErrInvalid) || rt.last != nil {
				t.Fatalf("call = %v, sent %v; want ErrInvalid and no request", err, rt.last)
			}
		})
	}
}

func TestClientBatchPaused(t *testing.T) {
	ctx := context.Background()
	descriptor, err := artifact.NewBatchDescriptor(
		4+1+(1+32+1+5),
		[]artifact.ObjectEntry{{Digest: blobDigest, Kind: artifact.KindBlob, Size: 5}},
		[]artifact.PartRef{{Digest: manifestRoot, Size: 20}},
	)
	if err != nil {
		t.Fatal(err)
	}
	peer := `{"status":"connected","expensive":false,"constrained":false,"cellular":true,"manual_metered":false,"observed_at":"0001-01-01T00:00:00Z"}`
	paused := `"paused":{"code":"receiver-cellular","reason":"local: cellular"}`
	tests := []struct {
		name       string
		result     string
		call       func(*Client) (netpolicy.State, error)
		wantCode   artifact.PauseCode
		wantErr    error
		wantPeerOn bool
	}{
		{
			name:   "begin paused",
			result: `{"have_parts":null,"peer":` + peer + `,` + paused + `}`,
			call: func(c *Client) (netpolicy.State, error) {
				out, err := c.BatchBegin(ctx, descriptor, netpolicy.State{Status: netpolicy.StatusConnected}, 0)
				return out.Peer, err
			},
			wantCode:   artifact.PauseReceiverCellular,
			wantPeerOn: true,
		},
		{
			name:   "put paused",
			result: `{"peer":` + peer + `,` + paused + `}`,
			call: func(c *Client) (netpolicy.State, error) {
				out, err := c.BatchPut(ctx, descriptor.ID, 0, []byte{1}, netpolicy.State{Status: netpolicy.StatusConnected}, 0)
				return out.Peer, err
			},
			wantCode:   artifact.PauseReceiverCellular,
			wantPeerOn: true,
		},
		{
			name:   "put accepted",
			result: `{"peer":` + peer + `}`,
			call: func(c *Client) (netpolicy.State, error) {
				out, err := c.BatchPut(ctx, descriptor.ID, 0, []byte{1}, netpolicy.State{Status: netpolicy.StatusConnected}, 0)
				return out.Peer, err
			},
			wantPeerOn: true,
		},
		{
			name:   "unknown pause code",
			result: `{"peer":` + peer + `,"paused":{"code":"receiver-wifi","reason":"?"}}`,
			call: func(c *Client) (netpolicy.State, error) {
				out, err := c.BatchPut(ctx, descriptor.ID, 0, []byte{1}, netpolicy.State{Status: netpolicy.StatusConnected}, 0)
				return out.Peer, err
			},
			wantErr:    artifact.ErrInvalid,
			wantPeerOn: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := &callRecordingTransport{response: &Response{OK: true, Result: json.RawMessage(tt.result)}}
			state, err := tt.call(NewClient(rt))
			var refusal *artifact.PausedError
			switch {
			case tt.wantCode != "":
				if !errors.As(err, &refusal) || refusal.Code != tt.wantCode || refusal.Reason != "local: cellular" {
					t.Fatalf("err = %v, want refusal %s", err, tt.wantCode)
				}
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) || errors.As(err, &refusal) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
			case err != nil:
				t.Fatalf("err = %v", err)
			}
			if state.Cellular != tt.wantPeerOn || state.Status != netpolicy.StatusConnected {
				t.Fatalf("peer state = %+v", state)
			}
		})
	}
}

func TestClientBatchCommitPaused(t *testing.T) {
	tests := []struct {
		name     string
		result   string
		want     artifact.CommitReport
		wantCode artifact.PauseCode
	}{
		{"committed", `{"stored":2,"present":1,"bytes":9}`, artifact.CommitReport{Stored: 2, Present: 1, Bytes: 9}, ""},
		{
			"refused",
			`{"stored":0,"present":0,"bytes":0,"paused":{"code":"receiver-restricted-mid-transfer","reason":"local: restricted mid-transfer"}}`,
			artifact.CommitReport{},
			artifact.PauseReceiverRestrictedMidTransfer,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := &callRecordingTransport{response: &Response{OK: true, Result: json.RawMessage(tt.result)}}
			report, err := NewClient(rt).BatchCommit(context.Background(), blobDigest, 3)
			var refusal *artifact.PausedError
			switch {
			case tt.wantCode != "":
				if !errors.As(err, &refusal) || refusal.Code != tt.wantCode {
					t.Fatalf("err = %v, want refusal %s", err, tt.wantCode)
				}
			case err != nil:
				t.Fatalf("err = %v", err)
			}
			if report != tt.want {
				t.Fatalf("report = %+v, want %+v", report, tt.want)
			}
		})
	}
}
