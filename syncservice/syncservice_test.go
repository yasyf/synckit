package syncservice

import (
	"context"
	"strings"
	"testing"

	"github.com/yasyf/synckit/rpc"
)

const stateJSON = `{"x":{"added_at":1719273600000000,"removed_at":0,"value":{"k":"v"}}}`

type fakeConsumer struct {
	reconcileOrigin string
	applyOrigin     string
}

func (f *fakeConsumer) Capabilities(context.Context) (Capabilities, error) {
	return DefaultCapabilities("fake"), nil
}

func (f *fakeConsumer) List(context.Context) ([]WatchItem, error) {
	return []WatchItem{{ID: "alpha", WatchDirs: []string{"/a", "/b"}, Fingerprint: "fa"}}, nil
}

func (f *fakeConsumer) Reconcile(_ context.Context, origin string) (ReconcileResult, error) {
	f.reconcileOrigin = origin
	return ReconcileResult{Converged: len(origin)}, nil
}

func (*fakeConsumer) Export(_ context.Context, request ExportRequest) (ChangeEnvelope, error) {
	return NewExportedChange(
		request.ServiceID, request.SchemaFingerprint, ChangeSnapshot,
		NewRevision(0), NewRevision(1), []byte(stateJSON),
	)
}

func (f *fakeConsumer) Apply(_ context.Context, change ChangeEnvelope) (ApplyResult, error) {
	f.applyOrigin = change.Origin
	return ApplyResult{AckedRevision: change.SourceRevision}, nil
}

type directTransport struct{ dispatcher *rpc.Dispatcher }

func (t directTransport) Do(ctx context.Context, request *rpc.Request) (*Response, error) {
	response := t.dispatcher.Dispatch(ctx, request)
	return &Response{OK: response.OK, Result: response.Result, Error: response.Error}, nil
}
func (directTransport) Close() error { return nil }

func testTransport(consumer SyncConsumer) Transport {
	dispatcher := rpc.NewDispatcher()
	RegisterConsumer(dispatcher, consumer)
	return directTransport{dispatcher: dispatcher}
}

func TestClientTypedRoundTrip(t *testing.T) {
	fake := &fakeConsumer{}
	client := NewClient(testTransport(fake))

	caps, err := client.Capabilities(t.Context())
	if err != nil || caps.Name != "fake" {
		t.Fatalf("Capabilities = %#v, %v", caps, err)
	}
	items, err := client.List(t.Context())
	if err != nil || len(items) != 1 || items[0].ID != "alpha" {
		t.Fatalf("List = %#v, %v", items, err)
	}
	if result, err := client.Reconcile(t.Context(), "h1"); err != nil || result.Converged != 2 || fake.reconcileOrigin != "h1" {
		t.Fatalf("Reconcile = %#v, %v; origin=%q", result, err, fake.reconcileOrigin)
	}
	request := ExportRequest{
		ServiceID: "fake", SchemaFingerprint: strings.Repeat("a", 64), SinceRevision: NewRevision(0),
	}
	change, err := client.Export(t.Context(), request)
	if err != nil || string(change.Payload) != stateJSON || !strings.Contains(string(change.Payload), "1719273600000000") {
		t.Fatalf("Export = %#v, %v", change, err)
	}
	change, err = BindDelivery(change, "h2")
	if err != nil {
		t.Fatal(err)
	}
	ack, err := client.Apply(t.Context(), change)
	if err != nil || ack.AckedRevision != change.SourceRevision || fake.applyOrigin != "h2" {
		t.Fatalf("Apply = %#v, %v; origin=%q", ack, err, fake.applyOrigin)
	}
}

type erroringConsumer struct{ fakeConsumer }

func (*erroringConsumer) Reconcile(context.Context, string) (ReconcileResult, error) {
	return ReconcileResult{}, boomError("boom")
}

type boomError string

func (e boomError) Error() string { return string(e) }

func TestHandlerErrorSurfaces(t *testing.T) {
	client := NewClient(testTransport(&erroringConsumer{}))
	if _, err := client.Reconcile(t.Context(), "h1"); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Reconcile error = %v", err)
	}
}

func TestUnknownMethodReturnsErrorResponse(t *testing.T) {
	response, err := testTransport(&fakeConsumer{}).Do(t.Context(), &rpc.Request{Method: "svc.bogus"})
	if err != nil || response.OK || !strings.Contains(response.Error, "unknown method") {
		t.Fatalf("response = %#v, %v", response, err)
	}
}
