package daemon

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/synckit/internal/synctransport"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

type enrollFake struct {
	*fakeConsumer
	enroll   bool
	identity string
	enrolled []syncservice.EnrollRequest
}

func newEnrollFake(enroll bool, identity string) *enrollFake {
	return &enrollFake{fakeConsumer: newFakeConsumer(), enroll: enroll, identity: identity}
}

func (f *enrollFake) Capabilities(context.Context) (syncservice.Capabilities, error) {
	caps := syncservice.DefaultCapabilities("stub")
	if f.enroll {
		caps.Methods = append(caps.Methods, syncservice.EnrollMethods...)
	}
	return caps, nil
}

func (f *enrollFake) Identity(context.Context) ([]byte, error) {
	if f.identity == "" {
		return nil, errors.New("no identity")
	}
	return []byte(f.identity), nil
}

func (f *enrollFake) Enroll(_ context.Context, request syncservice.EnrollRequest) error {
	f.enrolled = append(f.enrolled, request)
	return nil
}

func (f *enrollFake) dispatcher() *rpc.Dispatcher {
	d := rpc.NewDispatcher()
	syncservice.RegisterConsumer(d, f)
	return d
}

func withEnrollPeers(t *testing.T, local, remote *enrollFake) *[]string {
	t.Helper()
	var dials []string
	prev := dialTransport
	dialTransport = func(_ processScope, m manifest.Manifest, peer, self string) syncservice.Transport {
		dials = append(dials, m.Name+"->"+peer)
		switch {
		case peer == self:
			return directSyncTransport{dispatcher: local.dispatcher()}
		case peer == "peer@node" && remote != nil:
			return directSyncTransport{dispatcher: remote.dispatcher()}
		}
		return synctransport.Failed(errString("peer unreachable"))
	}
	t.Cleanup(func() { dialTransport = prev })
	return &dials
}

func TestEnrollHostPinsPeerIdentity(t *testing.T) {
	local, remote := newEnrollFake(true, ""), newEnrollFake(true, `{"node":"3f9a"}`)
	withEnrollPeers(t, local, remote)
	var steps []string
	enrollHost(t.Context(), testProcessScope(t), []manifest.Manifest{testManifest()}, "peer@node", "me@self", func(s string) { steps = append(steps, s) })
	if len(local.enrolled) != 1 || local.enrolled[0].Host != "peer@node" || string(local.enrolled[0].Identity) != remote.identity {
		t.Fatalf("enrolled %+v", local.enrolled)
	}
	if len(remote.enrolled) != 0 {
		t.Fatalf("enroll ran on the peer: %+v", remote.enrolled)
	}
	if !slices.Equal(steps, []string{"enrolled stub identity of peer@node"}) {
		t.Fatalf("steps %q", steps)
	}
}

func TestEnrollHostSkipsConsumersWithoutEnroll(t *testing.T) {
	dials := withEnrollPeers(t, newEnrollFake(false, ""), newEnrollFake(true, `{}`))
	var steps []string
	enrollHost(t.Context(), testProcessScope(t), []manifest.Manifest{testManifest()}, "peer@node", "me@self", func(s string) { steps = append(steps, s) })
	if len(steps) != 0 || !slices.Equal(*dials, []string{"stub->me@self"}) {
		t.Fatalf("steps %q dials %q, want a silent local probe only", steps, *dials)
	}
}

func TestEnrollHostWarnsWhenThePeerCannotAnswer(t *testing.T) {
	local := newEnrollFake(true, "")
	withEnrollPeers(t, local, nil)
	var steps []string
	enrollHost(t.Context(), testProcessScope(t), []manifest.Manifest{testManifest()}, "peer@node", "me@self", func(s string) { steps = append(steps, s) })
	if len(local.enrolled) != 0 || len(steps) != 1 || !strings.HasPrefix(steps[0], "WARN enroll stub identity of peer@node: ") {
		t.Fatalf("enrolled %+v steps %q", local.enrolled, steps)
	}
}
