package artifact

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func stageBatch(t *testing.T, src, dst *Store, objects []ObjectEntry) BatchDescriptor {
	t.Helper()
	d, err := src.BuildBatch(t.Context(), objects)
	if err != nil {
		t.Fatalf("BuildBatch: %v", err)
	}
	if _, err := dst.BeginBatch(t.Context(), d); err != nil {
		t.Fatalf("BeginBatch: %v", err)
	}
	for index := range d.Parts {
		data, err := src.ReadPart(t.Context(), d.ID, index)
		if err != nil {
			t.Fatal(err)
		}
		if err := dst.PutPart(t.Context(), d.ID, index, data); err != nil {
			t.Fatalf("PutPart: %v", err)
		}
	}
	return d
}

func TestBatchShipsDeclaredKinds(t *testing.T) {
	src, dst := newStore(t), newStore(t)
	decoy, err := Manifest{
		Schema: ManifestSchema, Media: "test/decoy", Chunks: []ChunkRef{},
		Deps: []Ref{{Digest: Sum([]byte("never stored")), Kind: KindBlob, Size: 12}},
	}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	blob, err := src.PutBlob(t.Context(), decoy)
	if err != nil {
		t.Fatal(err)
	}
	d := stageBatch(t, src, dst, []ObjectEntry{ObjectEntry(blob)})
	if d.Objects[0].Kind != KindBlob {
		t.Fatalf("descriptor kind = %s, want blob", d.Objects[0].Kind)
	}
	report, err := dst.CommitBatch(t.Context(), d.ID)
	if err != nil || report.Stored != 1 {
		t.Fatalf("CommitBatch = %+v, %v; want the blob stored", report, err)
	}
	if missing, err := dst.Complete(t.Context(), []Ref{blob}); err != nil || missing != 0 {
		t.Fatalf("Complete(blob) = %d, %v; want 0 missing", missing, err)
	}
	plain, err := src.PutBlob(t.Context(), []byte("plain bytes"))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		entry ObjectEntry
	}{
		{"manifest kind over plain bytes", ObjectEntry{Digest: plain.Digest, Kind: KindManifest, Size: plain.Size}},
		{"size mismatch", ObjectEntry{Digest: plain.Digest, Kind: KindBlob, Size: plain.Size + 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := src.BuildBatch(t.Context(), []ObjectEntry{tt.entry}); !errors.Is(err, ErrInvalid) {
				t.Fatalf("BuildBatch = %v, want ErrInvalid", err)
			}
		})
	}
}

type dualKindFixture struct {
	leaf, inner, file, root Ref
	objects                 []ObjectEntry
}

func buildDualKindFixture(t *testing.T, s *Store) dualKindFixture {
	t.Helper()
	ctx := t.Context()
	leaf, err := s.PutBlob(ctx, []byte("leaf"))
	if err != nil {
		t.Fatal(err)
	}
	inner, err := s.PutGroup(ctx, "test.group", []Ref{leaf})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.readObject(inner.Digest)
	if err != nil {
		t.Fatal(err)
	}
	file, err := s.Put(ctx, bytes.NewReader(encoded), "test.bin")
	if err != nil {
		t.Fatal(err)
	}
	root, err := s.PutGroup(ctx, "test.group", []Ref{file, inner})
	if err != nil {
		t.Fatal(err)
	}
	stored := func(ref Ref, kind Kind) ObjectEntry {
		data, err := s.readObject(ref.Digest)
		if err != nil {
			t.Fatal(err)
		}
		return ObjectEntry{Digest: ref.Digest, Kind: kind, Size: int64(len(data))}
	}
	return dualKindFixture{
		leaf: leaf, inner: inner, file: file, root: root,
		objects: []ObjectEntry{
			stored(leaf, KindBlob), stored(inner, KindManifest), stored(file, KindManifest), stored(root, KindManifest),
		},
	}
}

func TestDigestReachedAsBothKindsKeepsItsManifestChildren(t *testing.T) {
	src, dst := newStore(t), newStore(t)
	ctx := t.Context()
	fixture := buildDualKindFixture(t, src)
	closure, err := src.Closure(ctx, []Ref{fixture.root}, DefaultClosureBound)
	if err != nil || !reflect.DeepEqual(closure.Objects, fixture.objects) {
		t.Fatalf("Closure = %+v, %v; want %+v", closure.Objects, err, fixture.objects)
	}

	partial := newStore(t)
	for _, object := range fixture.objects[1:] {
		data, err := src.readObject(object.Digest)
		if err != nil {
			t.Fatal(err)
		}
		writeRawBlob(t, partial, data)
	}
	if missing, err := partial.Complete(ctx, []Ref{fixture.root}); err != nil || missing != 1 {
		t.Fatalf("Complete without the leaf = %d, %v; want 1 missing", missing, err)
	}

	d := stageBatch(t, src, dst, closure.Objects)
	if _, err := dst.CommitBatch(ctx, d.ID); err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}
	if missing, err := dst.Complete(ctx, []Ref{fixture.root}); err != nil || missing != 0 {
		t.Fatalf("Complete after the batch = %d, %v; want 0 missing", missing, err)
	}
	encoded, err := src.readObject(fixture.inner.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if missing, err := dst.Has(ctx, []Digest{fixture.leaf.Digest}); err != nil || len(missing) != 0 || !bytes.Equal(readAll(t, dst, fixture.file), encoded) {
		t.Fatalf("receiver lacks %v, %v; want the leaf and the file reading back as the inner manifest's bytes", missing, err)
	}
}

func TestCommitBatchPublishesNothingBeforeTheBarrier(t *testing.T) {
	src, dst := newStore(t), newStore(t)
	root, err := src.Put(t.Context(), bytes.NewReader(randomBytes(7, 3*ChunkSize)), "test/blob")
	if err != nil {
		t.Fatal(err)
	}
	objects := closureEntries(t, src, []Ref{root})
	d := stageBatch(t, src, dst, objects)
	digests := make([]Digest, len(objects))
	for i, object := range objects {
		digests[i] = object.Digest
	}
	crash := errors.New("power lost before the barrier")
	barriers := 0
	flushBarrier = func(string) error { return crash }
	t.Cleanup(func() { flushBarrier = fullSyncDir })
	if _, err := dst.CommitBatch(t.Context(), d.ID); !errors.Is(err, crash) {
		t.Fatalf("CommitBatch = %v, want the barrier failure", err)
	}
	if missing, err := dst.Has(t.Context(), digests); err != nil || len(missing) != len(digests) {
		t.Fatalf("Has after an interrupted commit = %d missing, %v; want all %d", len(missing), err, len(digests))
	}
	flushBarrier = func(dir string) error {
		barriers++
		return fullSyncDir(dir)
	}
	report, err := dst.CommitBatch(t.Context(), d.ID)
	if err != nil || report.Stored != len(objects) {
		t.Fatalf("retried CommitBatch = %+v, %v; want %d stored", report, err, len(objects))
	}
	if barriers != 3 {
		t.Fatalf("commit ran %d barriers, want 3: content, then the chunks, then the manifest", barriers)
	}
	if err := dst.Verify(t.Context(), []Ref{root}); err != nil {
		t.Fatalf("Verify after commit = %v", err)
	}
}
