package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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
	var firstRoot int64
	for _, object := range fixture.objects[:4] {
		firstRoot += object.Size
	}
	tests := []struct {
		name  string
		bound ClosureBound
		want  ClosureError
	}{
		{"objects", ClosureBound{MaxObjects: 3, MaxDepth: 32, MaxBytes: 1 << 30}, ClosureError{Bound: BoundObjects, Limit: 3}},
		{"depth", ClosureBound{MaxObjects: 100, MaxDepth: 1, MaxBytes: 1 << 30}, ClosureError{Bound: BoundDepth, Limit: 1}},
		{"bytes", ClosureBound{MaxObjects: 100, MaxDepth: 32, MaxBytes: firstRoot - 1}, ClosureError{Bound: BoundBytes, Limit: firstRoot - 1}},
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

func TestInFlightPublicationIsAbsent(t *testing.T) {
	s := newStore(t)
	ref, err := s.PutBlob(t.Context(), []byte("published"))
	if err != nil {
		t.Fatal(err)
	}
	readers := ownerAndReadOnly(t, s)
	release := s.claim(ref.Digest)
	if err := s.markUnsynced(ref.Digest); err != nil {
		t.Fatal(err)
	}
	for _, r := range readers {
		if missing, err := r.reader.Has(t.Context(), []Digest{ref.Digest}); err != nil || !reflect.DeepEqual(missing, []Digest{ref.Digest}) {
			t.Fatalf("%s Has(in flight) = %v, %v; want the digest missing", r.name, missing, err)
		}
		if missing, err := r.reader.Complete(t.Context(), []Ref{ref}); err != nil || missing != 1 {
			t.Fatalf("%s Complete(in flight) = %d, %v; want 1 missing", r.name, missing, err)
		}
		var missingErr *MissingError
		if err := r.reader.Verify(t.Context(), []Ref{ref}); !errors.As(err, &missingErr) || missingErr.Digest != ref.Digest {
			t.Fatalf("%s Verify(in flight) = %v, want MissingError", r.name, err)
		}
	}
	wrote := make(chan struct{})
	go func() {
		defer close(wrote)
		_, _ = s.PutBlob(context.Background(), []byte("published"))
	}()
	select {
	case <-wrote:
		t.Fatal("a second writer returned while the object's publication was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	<-wrote
	for _, r := range readers {
		if missing, err := r.reader.Complete(t.Context(), []Ref{ref}); err != nil || missing != 0 {
			t.Fatalf("%s Complete(published) = %d, %v; want 0 missing", r.name, missing, err)
		}
	}
}

func TestClosureExpandsManifestSeenFirstAsBlob(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	child := must(s.PutBlob(ctx, []byte("child")))
	inner := must(s.PutGroup(ctx, "test.inner", []Ref{child}))
	innerBytes := must(s.readObject(inner.Digest))
	alias := must(s.PutBlob(ctx, innerBytes))
	if alias.Digest != inner.Digest {
		t.Fatalf("alias digest %s != manifest digest %s", alias.Digest, inner.Digest)
	}
	wrapper := must(s.PutGroup(ctx, "test.wrap", []Ref{alias}))
	root := must(s.PutGroup(ctx, "test.outer", []Ref{wrapper, inner}))
	size := func(ref Ref) int64 { return int64(len(must(s.readObject(ref.Digest)))) }

	closure, err := s.Closure(ctx, []Ref{root}, DefaultClosureBound)
	if err != nil {
		t.Fatal(err)
	}
	want := []ObjectEntry{
		{Digest: child.Digest, Kind: KindBlob, Size: child.Size},
		{Digest: inner.Digest, Kind: KindManifest, Size: int64(len(innerBytes))},
		{Digest: wrapper.Digest, Kind: KindManifest, Size: size(wrapper)},
		{Digest: root.Digest, Kind: KindManifest, Size: size(root)},
	}
	if !reflect.DeepEqual(closure.Objects, want) {
		t.Fatalf("Closure = %+v, want %+v", closure.Objects, want)
	}

	d, err := s.BuildBatch(ctx, closure.Objects)
	if err != nil {
		t.Fatal(err)
	}
	dst := newStore(t)
	if _, err := dst.BeginBatch(ctx, d); err != nil {
		t.Fatal(err)
	}
	for index := range d.Parts {
		if err := dst.PutPart(ctx, d.ID, index, must(s.ReadPart(ctx, d.ID, index))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dst.CommitBatch(ctx, d.ID); err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}
	if missing, err := dst.Complete(ctx, []Ref{root}); err != nil || missing != 0 {
		t.Fatalf("receiver Complete = %d, %v; want 0", missing, err)
	}

	if err := s.SetPins(ctx, "test", []Ref{root}); err != nil {
		t.Fatal(err)
	}
	ageAll(t, s, 2*GCGrace)
	if _, err := s.GC(ctx); err != nil {
		t.Fatal(err)
	}
	if missing, err := s.Has(ctx, []Digest{child.Digest}); err != nil || len(missing) != 0 {
		t.Fatalf("GC removed the pinned manifest's child: missing %v, %v", missing, err)
	}
	if err := os.Remove(s.objectPath(child.Digest)); err != nil {
		t.Fatal(err)
	}
	if missing, err := s.Complete(ctx, []Ref{root}); err != nil || missing != 1 {
		t.Fatalf("Complete without the child = %d, %v; want 1 missing", missing, err)
	}
}

func TestOpenSyncsObjectDirectoriesBeforeServing(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data := []byte("renamed into place before a crash")
	digest := Sum(data)
	objects := filepath.Join(root, objectsDir)
	prefix := filepath.Join(objects, string(digest[:2]))
	if err := os.WriteFile(filepath.Join(prefix, string(digest)), data, filePerm); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syncObjectDir, flushBarrier = syncDir, fullSyncDir })
	dirFailure := errors.New("directory sync failed")
	tests := []struct {
		name     string
		failDir  string
		wantErr  error
		wantLast string
	}{
		{"unsynced prefix fails open", prefix, dirFailure, "sync " + prefix},
		{"every prefix synced then flushed", "", nil, "barrier " + objects},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var events []string
			synced := map[string]bool{}
			syncObjectDir = func(dir string) error {
				events = append(events, "sync "+dir)
				synced[dir] = true
				if dir == tt.failDir {
					return dirFailure
				}
				return syncDir(dir)
			}
			flushBarrier = func(dir string) error {
				events = append(events, "barrier "+dir)
				return fullSyncDir(dir)
			}
			reopened, err := Open(root)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Open = %v, want %v", err, tt.wantErr)
			}
			if got := events[len(events)-1]; got != tt.wantLast {
				t.Fatalf("last durability event = %q, want %q", got, tt.wantLast)
			}
			if tt.wantErr != nil {
				return
			}
			t.Cleanup(func() { _ = reopened.Close() })
			if len(synced) != 257 || !synced[prefix] || !synced[objects] {
				t.Fatalf("Open synced %d directories (prefix %v, objects %v), want all 256 prefixes and objects/", len(synced), synced[prefix], synced[objects])
			}
			missing, err := reopened.Has(t.Context(), []Digest{digest})
			if err != nil || len(missing) != 0 {
				t.Fatalf("Has after Open = %v, %v; want the synced object present", missing, err)
			}
		})
	}
}

func TestSharedReferencesAreValidatedOnEveryPath(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	blob, err := s.PutBlob(ctx, []byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	right, err := s.PutGroup(ctx, "test/right", []Ref{blob})
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := s.PutGroup(ctx, "test/wrong", []Ref{{Digest: blob.Digest, Kind: KindBlob, Size: 2}})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := s.PutGroup(ctx, "test/outer", []Ref{right, wrong})
	if err != nil {
		t.Fatal(err)
	}
	for _, roots := range [][]Ref{{wrong}, {outer}, {right, wrong}} {
		if n, err := s.Complete(ctx, roots); !errors.Is(err, ErrInvalid) {
			t.Errorf("Complete(%v) = %d, %v; want ErrInvalid", roots, n, err)
		}
		if err := s.Verify(ctx, roots); !errors.Is(err, ErrInvalid) {
			t.Errorf("Verify(%v) = %v; want ErrInvalid", roots, err)
		}
	}
	if _, err := s.Complete(ctx, []Ref{right}); err != nil {
		t.Fatalf("Complete(right) = %v", err)
	}
}

func reversed(refs []Ref) []Ref {
	out := slices.Clone(refs)
	slices.Reverse(out)
	return out
}

func TestSharedDescendantsCountTowardEveryPathDepth(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	blob, err := s.PutBlob(ctx, []byte("leaf"))
	if err != nil {
		t.Fatal(err)
	}
	chain := append(make([]Ref, 0, 1+DefaultClosureBound.MaxDepth), blob)
	for range DefaultClosureBound.MaxDepth {
		next, err := s.PutGroup(ctx, "test/chain", chain[len(chain)-1:])
		if err != nil {
			t.Fatal(err)
		}
		chain = append(chain, next)
	}
	wrap := func(deps []Ref) Ref {
		t.Helper()
		ref, err := s.PutGroup(ctx, "test/wrap", deps)
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	tests := []struct {
		name string
		root Ref
		ok   bool
	}{
		{"at the bound", wrap(chain[1 : len(chain)-1]), true},
		{"shallowest first past the bound", wrap(chain[1:]), false},
		{"deepest first past the bound", wrap(reversed(chain[1:])), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.Closure(ctx, []Ref{tt.root}, DefaultClosureBound)
			var bounded *ClosureError
			if tt.ok != (err == nil) || (!tt.ok && (!errors.As(err, &bounded) || bounded.Bound != BoundDepth)) {
				t.Fatalf("Closure = %v, want ok=%v or a depth bound", err, tt.ok)
			}
			if _, err := s.Complete(ctx, []Ref{tt.root}); tt.ok != (err == nil) {
				t.Fatalf("Complete = %v, want ok=%v", err, tt.ok)
			}
		})
	}
}
