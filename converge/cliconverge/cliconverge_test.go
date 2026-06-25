package cliconverge

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/yasyf/synckit/converge"
	"github.com/yasyf/synckit/cregistry"
)

var (
	_ converge.Driver[json.RawMessage]  = (*Driver)(nil)
	_ converge.Fetcher[json.RawMessage] = (*Fetcher)(nil)
)

type localCall struct {
	name  string
	stdin []byte
	args  []string
}

type sshCall struct {
	target    string
	remoteCmd string
	stdin     []byte
}

type fakeRunner struct {
	localOut string
	localErr error
	sshOut   string
	sshErr   error

	localCalls []localCall
	sshCalls   []sshCall
}

func (f *fakeRunner) Local(_ context.Context, name string, stdin []byte, args ...string) (string, error) {
	f.localCalls = append(f.localCalls, localCall{name: name, stdin: stdin, args: args})
	return f.localOut, f.localErr
}

func (f *fakeRunner) SSH(_ context.Context, target, remoteCmd string, stdin []byte) (string, error) {
	f.sshCalls = append(f.sshCalls, sshCall{target: target, remoteCmd: remoteCmd, stdin: stdin})
	return f.sshOut, f.sshErr
}

func raw(s string) json.RawMessage { return json.RawMessage(s) }

func TestLoadRegistry(t *testing.T) {
	const fetchJSON = `{"site.example":{"added_at":100,"value":{"host":"example.com"}}}`
	fr := &fakeRunner{localOut: fetchJSON}
	d := NewDriver("cookiesync", []string{"state", "get-json"}, []string{"state", "apply-json"}, fr)

	reg, err := d.LoadRegistry(context.Background())
	if err != nil {
		t.Fatalf("LoadRegistry: %v", err)
	}

	if len(fr.localCalls) != 1 {
		t.Fatalf("Local calls = %d, want 1", len(fr.localCalls))
	}
	call := fr.localCalls[0]
	if call.name != "cookiesync" {
		t.Errorf("fetch bin = %q, want cookiesync", call.name)
	}
	if got, want := call.args, []string{"state", "get-json"}; !equalStrs(got, want) {
		t.Errorf("fetch args = %v, want %v", got, want)
	}
	if call.stdin != nil {
		t.Errorf("fetch stdin = %q, want nil", call.stdin)
	}

	entry, ok := reg["site.example"]
	if !ok {
		t.Fatalf("registry missing site.example: %v", reg)
	}
	if entry.Added != 100 {
		t.Errorf("Added = %d, want 100", entry.Added)
	}
	if string(entry.Value) != `{"host":"example.com"}` {
		t.Errorf("Value = %s, want raw passthrough", entry.Value)
	}
}

func TestLoadRegistryFetchError(t *testing.T) {
	sentinel := errors.New("boom")
	fr := &fakeRunner{localErr: sentinel}
	d := NewDriver("cookiesync", []string{"state", "get-json"}, nil, fr)

	_, err := d.LoadRegistry(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want wrapped sentinel", err)
	}
}

func TestSaveRegistryPipesToApplyStdin(t *testing.T) {
	fr := &fakeRunner{localOut: ""}
	d := NewDriver("cookiesync", []string{"state", "get-json"}, []string{"state", "apply-json"}, fr)

	reg := cregistry.New[json.RawMessage]()
	reg.Add("site.example", raw(`{"host":"example.com"}`), 200)

	if err := d.SaveRegistry(context.Background(), reg); err != nil {
		t.Fatalf("SaveRegistry: %v", err)
	}

	if len(fr.localCalls) != 1 {
		t.Fatalf("Local calls = %d, want 1", len(fr.localCalls))
	}
	call := fr.localCalls[0]
	if call.name != "cookiesync" {
		t.Errorf("apply bin = %q, want cookiesync", call.name)
	}
	if got, want := call.args, []string{"state", "apply-json"}; !equalStrs(got, want) {
		t.Errorf("apply args = %v, want %v", got, want)
	}
	if call.stdin == nil {
		t.Fatal("apply stdin = nil, want marshalled registry")
	}

	var roundtrip cregistry.Registry[json.RawMessage]
	if err := json.Unmarshal(call.stdin, &roundtrip); err != nil {
		t.Fatalf("apply stdin not valid registry JSON: %v", err)
	}
	entry, ok := roundtrip["site.example"]
	if !ok {
		t.Fatalf("piped registry missing site.example: %s", call.stdin)
	}
	if entry.Added != 200 {
		t.Errorf("piped Added = %d, want 200", entry.Added)
	}
	if string(entry.Value) != `{"host":"example.com"}` {
		t.Errorf("piped Value = %s, want raw passthrough", entry.Value)
	}
}

func TestReconcileIsDeferredNoOp(t *testing.T) {
	fr := &fakeRunner{}
	d := NewDriver("cookiesync", nil, nil, fr)

	entry := cregistry.Entry[json.RawMessage]{Added: 1, Value: raw(`{}`)}
	outcome, err := d.Reconcile(context.Background(), "id", entry, []string{"peer1"}, "")
	if err != nil {
		t.Fatalf("Reconcile err = %v, want nil", err)
	}
	if outcome != OutcomeCLIDeferred {
		t.Errorf("outcome = %q, want %q", outcome, OutcomeCLIDeferred)
	}
	if len(fr.localCalls)+len(fr.sshCalls) != 0 {
		t.Errorf("Reconcile shelled %d calls, want 0", len(fr.localCalls)+len(fr.sshCalls))
	}
}

func TestFetchOverSSHReadOnly(t *testing.T) {
	const fetchJSON = `{"site.peer":{"added_at":300,"value":{"host":"peer.com"}}}`
	fr := &fakeRunner{sshOut: fetchJSON}
	f := NewFetcher("cookiesync", []string{"state", "get-json"}, fr)

	reg, err := f.Fetch(context.Background(), "alice@host")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(fr.localCalls) != 0 {
		t.Fatalf("Fetch ran %d Local calls, want 0 (ssh-only, read-only)", len(fr.localCalls))
	}
	if len(fr.sshCalls) != 1 {
		t.Fatalf("SSH calls = %d, want 1", len(fr.sshCalls))
	}
	call := fr.sshCalls[0]
	if call.target != "alice@host" {
		t.Errorf("ssh target = %q, want alice@host", call.target)
	}
	if call.remoteCmd != "cookiesync state get-json" {
		t.Errorf("ssh remoteCmd = %q, want %q", call.remoteCmd, "cookiesync state get-json")
	}
	if call.stdin != nil {
		t.Errorf("ssh stdin = %q, want nil (read-only fetch)", call.stdin)
	}

	entry, ok := reg["site.peer"]
	if !ok {
		t.Fatalf("registry missing site.peer: %v", reg)
	}
	if entry.Added != 300 {
		t.Errorf("Added = %d, want 300", entry.Added)
	}
}

func TestFetchSSHError(t *testing.T) {
	sentinel := errors.New("unreachable")
	fr := &fakeRunner{sshErr: sentinel}
	f := NewFetcher("cookiesync", []string{"state", "get-json"}, fr)

	_, err := f.Fetch(context.Background(), "alice@host")
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want wrapped sentinel", err)
	}
}

func TestDecodeRegistryInvalidJSON(t *testing.T) {
	fr := &fakeRunner{localOut: "not json"}
	d := NewDriver("cookiesync", nil, nil, fr)

	_, err := d.LoadRegistry(context.Background())
	if err == nil {
		t.Fatal("LoadRegistry on invalid JSON = nil err, want decode error")
	}
}

func TestMergeRawMessageLWW(t *testing.T) {
	tests := []struct {
		name      string
		a         cregistry.Registry[json.RawMessage]
		b         cregistry.Registry[json.RawMessage]
		wantValue map[string]string
		wantAbel  map[string]bool // present-ness per id
	}{
		{
			name:      "union of disjoint adds",
			a:         regOf(entryOf(10, 0, `{"v":"a"}`), "id.a"),
			b:         regOf(entryOf(20, 0, `{"v":"b"}`), "id.b"),
			wantValue: map[string]string{"id.a": `{"v":"a"}`, "id.b": `{"v":"b"}`},
			wantAbel:  map[string]bool{"id.a": true, "id.b": true},
		},
		{
			name:      "newer add wins value",
			a:         regOf(entryOf(10, 0, `{"v":"old"}`), "id"),
			b:         regOf(entryOf(20, 0, `{"v":"new"}`), "id"),
			wantValue: map[string]string{"id": `{"v":"new"}`},
			wantAbel:  map[string]bool{"id": true},
		},
		{
			name:      "remove newer than add removes item",
			a:         regOf(entryOf(10, 0, `{"v":"a"}`), "id"),
			b:         regOf(entryOf(10, 30, `{"v":"a"}`), "id"),
			wantValue: map[string]string{"id": `{"v":"a"}`},
			wantAbel:  map[string]bool{"id": false},
		},
		{
			name:      "later readd re-admits removed item",
			a:         regOf(entryOf(10, 30, `{"v":"a"}`), "id"),
			b:         regOf(entryOf(40, 30, `{"v":"readd"}`), "id"),
			wantValue: map[string]string{"id": `{"v":"readd"}`},
			wantAbel:  map[string]bool{"id": true},
		},
		{
			name:      "equal-add tiebreak by canonical JSON",
			a:         regOf(entryOf(10, 0, `{"v":"aaa"}`), "id"),
			b:         regOf(entryOf(10, 0, `{"v":"zzz"}`), "id"),
			wantValue: map[string]string{"id": `{"v":"zzz"}`},
			wantAbel:  map[string]bool{"id": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ab := cregistry.Merge(tt.a, tt.b)
			ba := cregistry.Merge(tt.b, tt.a)

			for id, want := range tt.wantValue {
				if got := string(ab[id].Value); got != want {
					t.Errorf("Merge(a,b)[%s].Value = %s, want %s", id, got, want)
				}
				if got := string(ba[id].Value); got != want {
					t.Errorf("Merge(b,a)[%s].Value = %s, want %s (commutative)", id, got, want)
				}
			}
			for id, want := range tt.wantAbel {
				if got := ab[id].Present(); got != want {
					t.Errorf("Merge(a,b)[%s].Present() = %v, want %v", id, got, want)
				}
				if got := ba[id].Present(); got != want {
					t.Errorf("Merge(b,a)[%s].Present() = %v, want %v (commutative)", id, got, want)
				}
			}
		})
	}
}

func entryOf(added, removed cregistry.Micros, value string) cregistry.Entry[json.RawMessage] {
	return cregistry.Entry[json.RawMessage]{Added: added, Removed: removed, Value: raw(value)}
}

func regOf(e cregistry.Entry[json.RawMessage], id string) cregistry.Registry[json.RawMessage] {
	r := cregistry.New[json.RawMessage]()
	r[id] = e
	return r
}

func equalStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
