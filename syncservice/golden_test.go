package syncservice

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/yasyf/synckit/rpc"
)

// recordingTransport captures the *rpc.Request each Client method issues without I/O,
// so a golden can pin the exact daemonkit operation payload. It answers every call with
// a null result so the client's decode is a no-op.
type recordingTransport struct{ last *rpc.Request }

func (rt *recordingTransport) Do(_ context.Context, req *rpc.Request) (*Response, error) {
	rt.last = req
	return &Response{OK: true, Result: json.RawMessage("null")}, nil
}

func (*recordingTransport) Close() error { return nil }

// TestClientRequestPayloadGolden pins the exact request payload each typed Client method
// puts in the daemonkit operation. The load-bearing pin is that an
// empty origin marshals as "params":null, not an omitted key: originParams returns a nil
// map and Request.Params has no omitempty, so the comment at client.go claiming an empty
// origin "omits the key entirely" is false at the byte level. cookiesync and reposync
// speak these exact bytes.
func TestClientRequestPayloadGolden(t *testing.T) {
	rt := &recordingTransport{}
	c := NewClient(rt)
	ctx := context.Background()
	schema := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	change, err := NewExportedChange("fake", schema, ChangeSnapshot, NewRevision(0), NewRevision(1), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	change, err = BindDelivery(change, "host-a")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		call func()
		want string
	}{
		{"capabilities", func() { _, _ = c.Capabilities(ctx) }, `{"method":"svc.capabilities","params":null}`},
		{"list", func() { _, _ = c.List(ctx) }, `{"method":"svc.list","params":null}`},
		{"export", func() {
			_, _ = c.Export(ctx, ExportRequest{ServiceID: "fake", SchemaFingerprint: schema, SinceRevision: NewRevision(0)})
		}, `{"method":"synckit.syncservice.export.v1","params":{"schema_fingerprint":"` + schema + `","service_id":"fake","since_revision":"0"}}`},
		{"apply", func() { _, _ = c.Apply(ctx, change) }, `{"method":"synckit.syncservice.apply.v1","params":{"base_revision":"0","change_id":"` + change.ChangeID + `","kind":"snapshot","origin":"host-a","payload":"e30=","payload_digest":"` + change.PayloadDigest + `","schema_fingerprint":"` + schema + `","service_id":"fake","source_revision":"1"}}`},
		{"reconcile empty origin is params null", func() { _, _ = c.Reconcile(ctx, "") }, `{"method":"svc.reconcile","params":null}`},
		{"reconcile with origin", func() { _, _ = c.Reconcile(ctx, "host-a") }, `{"method":"svc.reconcile","params":{"origin":"host-a"}}`},
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
