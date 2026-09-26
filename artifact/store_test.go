package artifact

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/durable"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

func randomBytes(seed, n int) []byte {
	b := make([]byte, n)
	_, _ = rand.NewChaCha8(sha256.Sum256(fmt.Appendf(nil, "%d", seed))).Read(b)
	return b
}

func objectFiles(t *testing.T, s *Store) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(filepath.Join(s.root, objectsDir), func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			files = append(files, path)
		}
		return err
	})
	if err != nil {
		t.Fatalf("walk objects: %v", err)
	}
	return files
}

func ageAll(t *testing.T, s *Store, age time.Duration) {
	t.Helper()
	then := time.Now().Add(-age)
	for _, path := range objectFiles(t, s) {
		if err := os.Chtimes(path, then, then); err != nil {
			t.Fatalf("age %s: %v", path, err)
		}
	}
}

func readAll(t *testing.T, s *Store, ref Ref) []byte {
	t.Helper()
	content, err := s.Open(t.Context(), ref)
	if err != nil {
		t.Fatalf("Open(%s): %v", ref.Digest, err)
	}
	data, err := io.ReadAll(content)
	if err != nil {
		t.Fatalf("read %s: %v", ref.Digest, err)
	}
	if err := content.Close(); err != nil {
		t.Fatalf("close %s: %v", ref.Digest, err)
	}
	return data
}

func TestPutChunksAtFixedOffsetsAndReusesAppendPrefix(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	content := randomBytes(1, 9*ChunkSize+1)
	ref, err := s.Put(ctx, bytes.NewReader(content), "test.bin")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if ref.Kind != KindManifest || ref.Size != int64(len(content)) {
		t.Fatalf("Put ref = %+v, want manifest of %d bytes", ref, len(content))
	}
	m, err := s.Manifest(ctx, ref)
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if len(m.Chunks) != 10 {
		t.Fatalf("chunks = %d, want 10", len(m.Chunks))
	}
	for i, chunk := range m.Chunks {
		end := min((i+1)*ChunkSize, len(content))
		want := ChunkRef{Digest: Sum(content[i*ChunkSize : end]), Size: int64(end - i*ChunkSize)}
		if chunk != want {
			t.Fatalf("chunk %d = %+v, want %+v", i, chunk, want)
		}
	}
	if got := len(objectFiles(t, s)); got != 11 {
		t.Fatalf("objects after Put = %d, want 11", got)
	}

	appended := append(bytes.Clone(content), randomBytes(2, 100)...)
	ref2, err := s.Put(ctx, bytes.NewReader(appended), "test.bin")
	if err != nil {
		t.Fatalf("Put appended: %v", err)
	}
	m2, err := s.Manifest(ctx, ref2)
	if err != nil {
		t.Fatalf("Manifest appended: %v", err)
	}
	if !reflect.DeepEqual(m2.Chunks[:9], m.Chunks[:9]) {
		t.Fatal("appended content rewrote an unchanged prefix chunk")
	}
	if want := (ChunkRef{Digest: Sum(appended[9*ChunkSize:]), Size: 101}); m2.Chunks[9] != want {
		t.Fatalf("last chunk = %+v, want %+v", m2.Chunks[9], want)
	}
	if got := len(objectFiles(t, s)); got != 13 {
		t.Fatalf("objects after append = %d, want 13 (one chunk and one manifest more)", got)
	}
	if !bytes.Equal(readAll(t, s, ref2), appended) {
		t.Fatal("Open(appended) content differs")
	}

	path := filepath.Join(t.TempDir(), "out.bin")
	if err := s.Materialize(ctx, ref, path, 0o640); err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	got, err := os.ReadFile(path) //nolint:gosec // a temp path this test composed
	if err != nil {
		t.Fatalf("read materialized: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat materialized: %v", err)
	}
	if !bytes.Equal(got, content) || info.Mode().Perm() != 0o640 {
		t.Fatalf("materialized %d bytes mode %v, want %d bytes mode 0640", len(got), info.Mode().Perm(), len(content))
	}
}

func TestPutEmptyContentIsChunklessManifest(t *testing.T) {
	s := newStore(t)
	ref, err := s.Put(t.Context(), bytes.NewReader(nil), "test.bin")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	encoded, err := (Manifest{Schema: ManifestSchema, Media: "test.bin"}).Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if want := (Ref{Digest: Sum(encoded), Kind: KindManifest, Size: 0}); ref != want {
		t.Fatalf("Put(empty) = %+v, want %+v", ref, want)
	}
	if got := readAll(t, s, ref); len(got) != 0 {
		t.Fatalf("Open(empty) = %d bytes, want 0", len(got))
	}
}

func TestPutRejectsInvalidInput(t *testing.T) {
	s := newStore(t)
	tests := []struct {
		name string
		put  func() error
	}{
		{"empty blob", func() error { _, err := s.PutBlob(t.Context(), nil); return err }},
		{"oversize blob", func() error { _, err := s.PutBlob(t.Context(), make([]byte, ChunkSize+1)); return err }},
		{"bad media", func() error { _, err := s.Put(t.Context(), strings.NewReader("x"), "Bad Media"); return err }},
		{"duplicate deps", func() error {
			dep := Ref{Digest: Sum([]byte("x")), Kind: KindBlob, Size: 1}
			_, err := s.PutGroup(t.Context(), "test.group", []Ref{dep, dep})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.put(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("err = %v, want ErrInvalid", err)
			}
		})
	}
	if got := len(objectFiles(t, s)); got != 0 {
		t.Fatalf("objects = %d, want 0", got)
	}
}

func TestPutGroupRefusesAbsentDeps(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	blob, err := s.PutBlob(ctx, []byte("present"))
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	absent := Ref{Digest: Sum([]byte("absent")), Kind: KindBlob, Size: 6}
	_, err = s.PutGroup(ctx, "test.group", []Ref{blob, absent})
	var missing *MissingError
	if !errors.As(err, &missing) || missing.Digest != absent.Digest {
		t.Fatalf("PutGroup with absent dep err = %v, want MissingError{%s}", err, absent.Digest)
	}
	if got := len(objectFiles(t, s)); got != 1 {
		t.Fatalf("objects = %d, want 1", got)
	}
	group, err := s.PutGroup(ctx, "test.group", []Ref{blob})
	if err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	m, err := s.Manifest(ctx, group)
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if group.Size != 0 || len(m.Chunks) != 0 || !reflect.DeepEqual(m.Deps, []Ref{blob}) {
		t.Fatalf("group = %+v manifest %+v, want empty chunks and deps [%+v]", group, m, blob)
	}
}

type closureFixture struct {
	roots   []Ref
	objects []ObjectEntry
	bytes   int64
}

func buildClosureFixture(t *testing.T, s *Store) closureFixture {
	t.Helper()
	ctx := t.Context()
	a, err := s.Put(ctx, strings.NewReader("aaa"), "test.bin")
	if err != nil {
		t.Fatalf("Put a: %v", err)
	}
	b, err := s.Put(ctx, strings.NewReader("bbbb"), "test.bin")
	if err != nil {
		t.Fatalf("Put b: %v", err)
	}
	solo, err := s.PutBlob(ctx, []byte("solo!"))
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	g1, err := s.PutGroup(ctx, "test.group", []Ref{a, solo})
	if err != nil {
		t.Fatalf("PutGroup g1: %v", err)
	}
	g2, err := s.PutGroup(ctx, "test.group", []Ref{b, a})
	if err != nil {
		t.Fatalf("PutGroup g2: %v", err)
	}
	stored := func(ref Ref) ObjectEntry {
		info, err := os.Stat(s.objectPath(ref.Digest))
		if err != nil {
			t.Fatalf("stat %s: %v", ref.Digest, err)
		}
		return ObjectEntry{Digest: ref.Digest, Kind: ref.Kind, Size: info.Size()}
	}
	objects := []ObjectEntry{
		{Digest: Sum([]byte("aaa")), Kind: KindBlob, Size: 3},
		stored(a),
		stored(solo),
		stored(g1),
		{Digest: Sum([]byte("bbbb")), Kind: KindBlob, Size: 4},
		stored(b),
		stored(g2),
	}
	var total int64
	for _, object := range objects {
		total += object.Size
	}
	return closureFixture{roots: []Ref{g1, g2}, objects: objects, bytes: total}
}

func TestClosureIsDeterministicChildrenFirst(t *testing.T) {
	s := newStore(t)
	fixture := buildClosureFixture(t, s)
	closure, err := s.Closure(t.Context(), fixture.roots, DefaultClosureBound)
	if err != nil {
		t.Fatalf("Closure: %v", err)
	}
	if !reflect.DeepEqual(closure.Objects, fixture.objects) || closure.Bytes != fixture.bytes {
		t.Fatalf("Closure = %+v (%d bytes), want %+v (%d bytes)", closure.Objects, closure.Bytes, fixture.objects, fixture.bytes)
	}
	reader, err := OpenReadOnly(s.root)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	again, err := reader.Closure(t.Context(), fixture.roots, DefaultClosureBound)
	if err != nil || !reflect.DeepEqual(again, closure) {
		t.Fatalf("read-only Closure = %+v, %v; want %+v", again, err, closure)
	}
}

func TestClosureBounds(t *testing.T) {
	s := newStore(t)
	fixture := buildClosureFixture(t, s)
	tests := []struct {
		name  string
		bound ClosureBound
		want  ClosureError
	}{
		{"objects", ClosureBound{MaxObjects: 3, MaxDepth: 32, MaxBytes: 1 << 30}, ClosureError{Bound: BoundObjects, Limit: 3}},
		{"depth", ClosureBound{MaxObjects: 100, MaxDepth: 1, MaxBytes: 1 << 30}, ClosureError{Bound: BoundDepth, Limit: 1}},
		{"bytes", ClosureBound{MaxObjects: 100, MaxDepth: 32, MaxBytes: fixture.bytes - 1}, ClosureError{Bound: BoundBytes, Limit: fixture.bytes - 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.Closure(t.Context(), fixture.roots, tt.bound)
			var bounded *ClosureError
			if !errors.As(err, &bounded) || *bounded != tt.want {
				t.Fatalf("Closure err = %v, want %+v", err, tt.want)
			}
		})
	}
	absent := Ref{Digest: Sum([]byte("absent")), Kind: KindManifest, Size: 0}
	_, err := s.Closure(t.Context(), []Ref{absent}, DefaultClosureBound)
	var missing *MissingError
	if !errors.As(err, &missing) || missing.Digest != absent.Digest {
		t.Fatalf("Closure(absent) err = %v, want MissingError", err)
	}
}

func TestCompleteAndVerify(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	ref, err := s.Put(ctx, bytes.NewReader(randomBytes(3, 2*ChunkSize+10)), "test.bin")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	group, err := s.PutGroup(ctx, "test.group", []Ref{ref})
	if err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	roots := []Ref{group}
	assertComplete := func(want int) {
		t.Helper()
		if got, err := s.Complete(ctx, roots); err != nil || got != want {
			t.Fatalf("Complete = %d, %v; want %d", got, err, want)
		}
	}
	assertComplete(0)
	if err := s.Verify(ctx, roots); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	m, err := s.Manifest(ctx, ref)
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	chunk := s.objectPath(m.Chunks[1].Digest)
	original, err := os.ReadFile(chunk) //nolint:gosec // an object path under this test's store
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	corrupt := bytes.Clone(original)
	corrupt[7] ^= 1
	if err := os.WriteFile(chunk, corrupt, 0o600); err != nil { //nolint:gosec // an object path under this test's store
		t.Fatalf("corrupt chunk: %v", err)
	}
	assertComplete(0)
	if err := s.Verify(ctx, roots); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Verify(corrupt) = %v, want ErrInvalid", err)
	}
	if _, err := io.ReadAll(must(s.Open(ctx, ref))); !errors.Is(err, ErrInvalid) {
		t.Fatalf("read corrupt content err = %v, want ErrInvalid", err)
	}

	if err := os.Remove(chunk); err != nil {
		t.Fatalf("remove chunk: %v", err)
	}
	assertComplete(1)
	var missing *MissingError
	if err := s.Verify(ctx, roots); !errors.As(err, &missing) || missing.Digest != m.Chunks[1].Digest {
		t.Fatalf("Verify(missing) = %v, want MissingError{%s}", err, m.Chunks[1].Digest)
	}

	if err := os.Remove(s.objectPath(ref.Digest)); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}
	assertComplete(1)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestOpenHoldsStoreLock(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := Open(root); !errors.Is(err, durable.ErrLockBusy) {
		t.Fatalf("second Open err = %v, want ErrLockBusy", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	again, err := Open(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := again.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := OpenReadOnly(t.TempDir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("OpenReadOnly(empty) err = %v, want ErrNotExist", err)
	}
}

func TestHasReportsMissingInQueryOrder(t *testing.T) {
	s := newStore(t)
	present, err := s.PutBlob(t.Context(), []byte("present"))
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	first, second := Sum([]byte("first")), Sum([]byte("second"))
	missing, err := s.Has(t.Context(), []Digest{second, present.Digest, first})
	if err != nil || !reflect.DeepEqual(missing, []Digest{second, first}) {
		t.Fatalf("Has = %v, %v; want [%s %s]", missing, err, second, first)
	}
}

func TestServiceRoot(t *testing.T) {
	config := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", config)
	root, err := ServiceRoot("cc-sync")
	if err != nil {
		t.Fatalf("ServiceRoot: %v", err)
	}
	if want := filepath.Join(config, "synckit", "artifacts", "v1", "cc-sync"); root != want {
		t.Fatalf("ServiceRoot = %q, want %q", root, want)
	}
	if _, err := ServiceRoot("../escape"); err == nil {
		t.Fatal("ServiceRoot(../escape) succeeded")
	}
}
