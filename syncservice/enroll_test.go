package syncservice

import (
	"context"
	"strings"
	"testing"
)

type fakeEnroller struct {
	fakeConsumer
	identity []byte
	enrolled []EnrollRequest
}

func (f *fakeEnroller) Identity(context.Context) ([]byte, error) { return f.identity, nil }

func (f *fakeEnroller) Enroll(_ context.Context, request EnrollRequest) error {
	f.enrolled = append(f.enrolled, request)
	return nil
}

func TestEnrollerRoundTrip(t *testing.T) {
	fake := &fakeEnroller{identity: []byte(`{"node":"3f9a","n":12345678901234567}`)}
	client := NewClient(testTransport(fake))
	identity, err := client.Identity(t.Context())
	if err != nil || string(identity) != string(fake.identity) {
		t.Fatalf("Identity = %s, %v", identity, err)
	}
	if err := client.Enroll(t.Context(), EnrollRequest{Host: "peer@node", Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if len(fake.enrolled) != 1 || fake.enrolled[0].Host != "peer@node" || string(fake.enrolled[0].Identity) != string(identity) {
		t.Fatalf("enrolled %+v", fake.enrolled)
	}
}

func TestEnrollRefusesInvalidRequests(t *testing.T) {
	tests := []struct {
		name    string
		request EnrollRequest
	}{
		{"empty host", EnrollRequest{Identity: []byte(`{}`)}},
		{"newline host", EnrollRequest{Host: "a\nb", Identity: []byte(`{}`)}},
		{"empty identity", EnrollRequest{Host: "peer@node"}},
		{"invalid identity", EnrollRequest{Host: "peer@node", Identity: []byte(`{`)}},
		{"oversized identity", EnrollRequest{Host: "peer@node", Identity: []byte(`"` + strings.Repeat("a", MaxIdentity) + `"`)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeEnroller{}
			if err := NewClient(testTransport(fake)).Enroll(t.Context(), tt.request); err == nil || len(fake.enrolled) != 0 {
				t.Fatalf("Enroll(%+v) = %v, enrolled %d", tt.request, err, len(fake.enrolled))
			}
		})
	}
}

func TestIdentityRefusesInvalidDocument(t *testing.T) {
	fake := &fakeEnroller{identity: []byte(`not json`)}
	if _, err := NewClient(testTransport(fake)).Identity(t.Context()); err == nil {
		t.Fatal("Identity accepted a non-JSON document")
	}
}

func TestPlainConsumerHasNoEnrollMethods(t *testing.T) {
	if _, err := NewClient(testTransport(&fakeConsumer{})).Identity(t.Context()); err == nil {
		t.Fatal("a plain consumer answered identity")
	}
}
