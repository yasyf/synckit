package artifact

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func closureEntries(t *testing.T, s *Store, roots []Ref) []ObjectEntry {
	t.Helper()
	closure, err := s.Closure(t.Context(), roots, DefaultClosureBound)
	if err != nil {
		t.Fatalf("Closure: %v", err)
	}
	return closure.Objects
}

func putParts(t *testing.T, dst *Store, d BatchDescriptor, parts [][]byte, skip []int) int {
	t.Helper()
	puts := 0
	for index, data := range parts {
		if slices.Contains(skip, index) {
			continue
		}
		if err := dst.PutPart(t.Context(), d.ID, index, data); err != nil {
			t.Fatalf("PutPart(%d): %v", index, err)
		}
		puts++
	}
	return puts
}

func outboxParts(t *testing.T, src *Store, d BatchDescriptor) [][]byte {
	t.Helper()
	parts := make([][]byte, len(d.Parts))
	for index := range d.Parts {
		data, err := src.ReadPart(t.Context(), d.ID, index)
		if err != nil {
			t.Fatalf("ReadPart(%d): %v", index, err)
		}
		parts[index] = data
	}
	return parts
}

func sourceBatch(t *testing.T, seed, size int) (*Store, []Ref, BatchDescriptor) {
	t.Helper()
	src := newStore(t)
	ref, err := src.Put(t.Context(), bytes.NewReader(randomBytes(seed, size)), "test.bin")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	solo, err := src.PutBlob(t.Context(), []byte(fmt.Sprintf("solo %d", seed)))
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	group, err := src.PutGroup(t.Context(), "test.group", []Ref{ref, solo})
	if err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	roots := []Ref{group}
	d, err := src.BuildBatch(t.Context(), closureEntries(t, src, roots))
	if err != nil {
		t.Fatalf("BuildBatch: %v", err)
	}
	return src, roots, d
}

func TestBatchRoundTrip(t *testing.T) {
	src, roots, d := sourceBatch(t, 4, 3*ChunkSize+5)
	ctx := t.Context()
	closure, err := src.Closure(ctx, roots, DefaultClosureBound)
	if err != nil {
		t.Fatalf("Closure: %v", err)
	}
	if !reflect.DeepEqual(d.Objects, closure.Objects) || d.RawSize != packSize(closure.Objects) {
		t.Fatalf("descriptor objects %+v raw %d, want closure %+v raw %d", d.Objects, d.RawSize, closure.Objects, packSize(closure.Objects))
	}
	if len(d.Parts) != 4 {
		t.Fatalf("parts = %d, want 4 for 3 MiB of random content", len(d.Parts))
	}
	again, err := src.BuildBatch(ctx, closureEntries(t, src, roots))
	if err != nil || !reflect.DeepEqual(again, d) {
		t.Fatalf("rebuild = %+v, %v; want the identical descriptor", again.ID, err)
	}

	dst := newStore(t)
	held, err := dst.BeginBatch(ctx, d)
	if err != nil || len(held) != 0 {
		t.Fatalf("BeginBatch = %v, %v; want []", held, err)
	}
	putParts(t, dst, d, outboxParts(t, src, d), nil)
	report, err := dst.CommitBatch(ctx, d.ID)
	if err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}
	if want := (CommitReport{Stored: len(closure.Objects), Bytes: closure.Bytes}); report != want {
		t.Fatalf("CommitBatch = %+v, want %+v", report, want)
	}
	if err := dst.Verify(ctx, roots); err != nil {
		t.Fatalf("Verify after commit: %v", err)
	}
	if entries, err := os.ReadDir(filepath.Join(dst.root, incomingDir)); err != nil || len(entries) != 0 {
		t.Fatalf("incoming after commit = %v, %v; want empty", entries, err)
	}

	if _, err := dst.BeginBatch(ctx, d); err != nil {
		t.Fatalf("BeginBatch again: %v", err)
	}
	putParts(t, dst, d, outboxParts(t, src, d), nil)
	report, err = dst.CommitBatch(ctx, d.ID)
	if want := (CommitReport{Present: len(closure.Objects)}); err != nil || report != want {
		t.Fatalf("recommit = %+v, %v; want %+v", report, err, want)
	}

	if err := src.DropBatch(ctx, d.ID); err != nil {
		t.Fatalf("DropBatch: %v", err)
	}
	if _, err := src.ReadPart(ctx, d.ID, 0); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadPart after drop err = %v, want ErrNotExist", err)
	}
}

func TestBuildBatchRefusesPastBounds(t *testing.T) {
	s := newStore(t)
	tooMany := make([]ObjectEntry, MaxBatchObjects+1)
	for i := range tooMany {
		tooMany[i] = ObjectEntry{Digest: Sum([]byte(fmt.Sprint(i))), Kind: KindBlob, Size: 1}
	}
	if _, err := s.BuildBatch(t.Context(), tooMany); !errors.Is(err, ErrBatchFull) {
		t.Fatalf("BuildBatch(%d objects) err = %v, want ErrBatchFull", len(tooMany), err)
	}
	entries := make([]ObjectEntry, 0, MaxBatchRaw/ChunkSize+1)
	for i := range MaxBatchRaw / ChunkSize {
		ref, err := s.PutBlob(t.Context(), randomBytes(100+i, ChunkSize))
		if err != nil {
			t.Fatalf("PutBlob: %v", err)
		}
		entries = append(entries, ObjectEntry(ref))
	}
	extra, err := s.PutBlob(t.Context(), []byte{1})
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	if _, err := s.BuildBatch(t.Context(), append(entries, ObjectEntry(extra))); !errors.Is(err, ErrBatchFull) {
		t.Fatalf("BuildBatch(32 MiB + 1 B) err = %v, want ErrBatchFull", err)
	}
	var missing *MissingError
	absent := Sum([]byte("absent"))
	if _, err := s.BuildBatch(t.Context(), []ObjectEntry{ObjectEntry(extra), {Digest: absent, Kind: KindBlob, Size: 1}}); !errors.As(err, &missing) || missing.Digest != absent {
		t.Fatalf("BuildBatch(absent) err = %v, want MissingError", err)
	}
}

func TestPutPartRejectsMismatch(t *testing.T) {
	src, _, d := sourceBatch(t, 5, 2*ChunkSize)
	parts := outboxParts(t, src, d)
	dst := newStore(t)
	if _, err := dst.BeginBatch(t.Context(), d); err != nil {
		t.Fatalf("BeginBatch: %v", err)
	}
	flipped := bytes.Clone(parts[0])
	flipped[len(flipped)/2] ^= 0x40
	tests := []struct {
		name  string
		index int
		data  []byte
	}{
		{"offset swap", 0, parts[1]},
		{"flipped byte", 0, flipped},
		{"truncated", 0, parts[0][:len(parts[0])-1]},
		{"index past parts", len(parts), parts[0]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := dst.PutPart(t.Context(), d.ID, tt.index, tt.data); !errors.Is(err, ErrInvalid) {
				t.Fatalf("PutPart err = %v, want ErrInvalid", err)
			}
		})
	}
	held, err := dst.BeginBatch(t.Context(), d)
	if err != nil || len(held) != 0 {
		t.Fatalf("held after rejected puts = %v, %v; want []", held, err)
	}
	other := newStore(t)
	if err := other.PutPart(t.Context(), d.ID, 0, parts[0]); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("PutPart before BeginBatch err = %v, want ErrNotExist", err)
	}
}

func TestInterruptedBatchResumesFromHeldParts(t *testing.T) {
	src, roots, d := sourceBatch(t, 6, 3*ChunkSize)
	parts := outboxParts(t, src, d)
	root := t.TempDir()
	dst, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := dst.BeginBatch(t.Context(), d); err != nil {
		t.Fatalf("BeginBatch: %v", err)
	}
	for index := range 2 {
		if err := dst.PutPart(t.Context(), d.ID, index, parts[index]); err != nil {
			t.Fatalf("PutPart(%d): %v", index, err)
		}
	}
	if err := dst.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dst, err = Open(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := dst.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	held, err := dst.BeginBatch(t.Context(), d)
	if err != nil || !reflect.DeepEqual(held, []int{0, 1}) {
		t.Fatalf("resumed BeginBatch = %v, %v; want [0 1]", held, err)
	}
	if puts := putParts(t, dst, d, parts, held); puts != len(parts)-2 {
		t.Fatalf("resumed puts = %d, want %d", puts, len(parts)-2)
	}
	if _, err := dst.CommitBatch(t.Context(), d.ID); err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}
	if err := dst.Verify(t.Context(), roots); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestMissingPartNeverCommits(t *testing.T) {
	src, _, d := sourceBatch(t, 7, 2*ChunkSize)
	parts := outboxParts(t, src, d)
	dst := newStore(t)
	if _, err := dst.BeginBatch(t.Context(), d); err != nil {
		t.Fatalf("BeginBatch: %v", err)
	}
	last := len(parts) - 1
	putParts(t, dst, d, parts, []int{last})
	if _, err := dst.CommitBatch(t.Context(), d.ID); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("holds %d of %d parts", last, len(parts))) {
		t.Fatalf("CommitBatch with part %d missing err = %v", last, err)
	}
	digests := make([]Digest, 0, len(d.Objects))
	for _, object := range d.Objects {
		digests = append(digests, object.Digest)
	}
	if missing, err := dst.Has(t.Context(), digests); err != nil || len(missing) != len(digests) {
		t.Fatalf("Has after failed commit = %d missing, %v; want %d", len(missing), err, len(digests))
	}
	held, err := dst.BeginBatch(t.Context(), d)
	if err != nil || len(held) != last {
		t.Fatalf("held after failed commit = %v, %v; want %d parts", held, err, last)
	}
}

type craftedObject struct {
	entry ObjectEntry
	data  []byte
}

func blobObject(data []byte) craftedObject {
	return craftedObject{entry: ObjectEntry{Digest: Sum(data), Kind: KindBlob, Size: int64(len(data))}, data: data}
}

func craftBatch(t *testing.T, objects []craftedObject, trailer []byte, window int) (BatchDescriptor, [][]byte) {
	t.Helper()
	var raw bytes.Buffer
	pack := &packWriter{w: &raw}
	entries := make([]ObjectEntry, 0, len(objects))
	if err := pack.begin(); err != nil {
		t.Fatalf("pack begin: %v", err)
	}
	for _, object := range objects {
		if err := pack.object(object.entry, object.data); err != nil {
			t.Fatalf("pack object: %v", err)
		}
		entries = append(entries, object.entry)
	}
	if err := pack.end(); err != nil {
		t.Fatalf("pack end: %v", err)
	}
	raw.Write(trailer)
	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(window), zstd.WithEncoderCRC(true))
	if err != nil {
		t.Fatalf("encoder: %v", err)
	}
	if _, err := encoder.Write(raw.Bytes()); err != nil {
		t.Fatalf("compress: %v", err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("compress close: %v", err)
	}
	var parts [][]byte
	var refs []PartRef
	for stream := compressed.Bytes(); len(stream) > 0; {
		part := stream[:min(PartSize, len(stream))]
		stream = stream[len(part):]
		parts = append(parts, part)
		refs = append(refs, PartRef{Digest: Sum(part), Size: int64(len(part))})
	}
	d, err := NewBatchDescriptor(packSize(entries), entries, refs)
	if err != nil {
		t.Fatalf("NewBatchDescriptor: %v", err)
	}
	return d, parts
}

func commitCrafted(t *testing.T, dst *Store, d BatchDescriptor, parts [][]byte) error {
	t.Helper()
	if _, err := dst.BeginBatch(t.Context(), d); err != nil {
		t.Fatalf("BeginBatch: %v", err)
	}
	putParts(t, dst, d, parts, nil)
	_, err := dst.CommitBatch(t.Context(), d.ID)
	return err
}

func TestCommitRejectsMalformedPackAndStoresNothing(t *testing.T) {
	good := blobObject([]byte("good object"))
	flipped := blobObject([]byte("flipped object"))
	flipped.data = []byte("flipped objecu")
	src := newStore(t)
	ref, err := src.Put(t.Context(), strings.NewReader("chunk content"), "test.bin")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	manifestBytes, err := os.ReadFile(src.objectPath(ref.Digest))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	manifest := craftedObject{entry: ObjectEntry{Digest: ref.Digest, Kind: KindManifest, Size: int64(len(manifestBytes))}, data: manifestBytes}
	chunk := blobObject([]byte("chunk content"))
	wide := make([]craftedObject, 0, 5)
	for i := range 5 {
		wide = append(wide, blobObject(randomBytes(200+i, ChunkSize)))
	}
	tests := []struct {
		name    string
		objects []craftedObject
		trailer []byte
		window  int
	}{
		{"flipped object byte", []craftedObject{good, flipped}, nil, WindowSize},
		{"manifest before chunk", []craftedObject{good, manifest, chunk}, nil, WindowSize},
		{"blob declared manifest", []craftedObject{chunk, {entry: ObjectEntry{Digest: good.entry.Digest, Kind: KindManifest, Size: good.entry.Size}, data: good.data}}, nil, WindowSize},
		{"bytes past terminator", []craftedObject{good}, []byte{0}, WindowSize},
		{"window past bound", wide, nil, 2 * WindowSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := newStore(t)
			d, parts := craftBatch(t, tt.objects, tt.trailer, tt.window)
			if err := commitCrafted(t, dst, d, parts); !errors.Is(err, ErrInvalid) {
				t.Fatalf("CommitBatch err = %v, want ErrInvalid", err)
			}
			if files := objectFiles(t, dst); len(files) != 0 {
				t.Fatalf("stored %d objects from a rejected batch", len(files))
			}
		})
	}
	dst := newStore(t)
	d, parts := craftBatch(t, []craftedObject{good, chunk, manifest}, nil, WindowSize)
	if err := commitCrafted(t, dst, d, parts); err != nil {
		t.Fatalf("CommitBatch(children first): %v", err)
	}
}

func TestCommitRejectsZstdBombWithinMemoryBound(t *testing.T) {
	good := blobObject([]byte("good object"))
	d, parts := craftBatch(t, []craftedObject{good}, make([]byte, 64<<20), WindowSize)
	dst := newStore(t)
	if _, err := dst.BeginBatch(t.Context(), d); err != nil {
		t.Fatalf("BeginBatch: %v", err)
	}
	putParts(t, dst, d, parts, nil)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := dst.CommitBatch(t.Context(), d.ID)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("CommitBatch(bomb) err = %v, want ErrInvalid", err)
	}
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 24<<20 {
		t.Fatalf("CommitBatch(bomb) allocated %d bytes, want under 24 MiB", grown)
	}
	if files := objectFiles(t, dst); len(files) != 0 {
		t.Fatalf("stored %d objects from a bomb", len(files))
	}
}

func TestBeginBatchAdmission(t *testing.T) {
	src := newStore(t)
	batches := make([]BatchDescriptor, 0, MaxIncoming+1)
	for i := range MaxIncoming + 1 {
		ref, err := src.PutBlob(t.Context(), []byte(fmt.Sprintf("object %d", i)))
		if err != nil {
			t.Fatalf("PutBlob: %v", err)
		}
		d, err := src.BuildBatch(t.Context(), []ObjectEntry{ObjectEntry(ref)})
		if err != nil {
			t.Fatalf("BuildBatch: %v", err)
		}
		batches = append(batches, d)
	}
	dst := newStore(t)
	for _, d := range batches[:MaxIncoming] {
		if _, err := dst.BeginBatch(t.Context(), d); err != nil {
			t.Fatalf("BeginBatch: %v", err)
		}
	}
	if _, err := dst.BeginBatch(t.Context(), batches[MaxIncoming]); !errors.Is(err, errIncomingFull) {
		t.Fatalf("BeginBatch past MaxIncoming err = %v, want errIncomingFull", err)
	}
	if held, err := dst.BeginBatch(t.Context(), batches[0]); err != nil || len(held) != 0 {
		t.Fatalf("re-begin staged batch = %v, %v; want []", held, err)
	}
	putParts(t, dst, batches[0], outboxParts(t, src, batches[0]), nil)
	if _, err := dst.CommitBatch(t.Context(), batches[0].ID); err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}
	if _, err := dst.BeginBatch(t.Context(), batches[MaxIncoming]); err != nil {
		t.Fatalf("BeginBatch after a commit freed a slot: %v", err)
	}
}

func TestGCSweepsExpiredStaging(t *testing.T) {
	s := newStore(t)
	batches := make([]BatchDescriptor, 0, 2)
	for i := range 2 {
		ref, err := s.PutBlob(t.Context(), []byte(fmt.Sprintf("staged %d", i)))
		if err != nil {
			t.Fatalf("PutBlob: %v", err)
		}
		d, err := s.BuildBatch(t.Context(), []ObjectEntry{ObjectEntry(ref)})
		if err != nil {
			t.Fatalf("BuildBatch: %v", err)
		}
		if _, err := s.BeginBatch(t.Context(), d); err != nil {
			t.Fatalf("BeginBatch: %v", err)
		}
		batches = append(batches, d)
	}
	expired := time.Now().Add(-StagingTTL - time.Minute)
	for _, dir := range []string{outboxDir, incomingDir} {
		if err := os.Chtimes(filepath.Join(s.root, dir, string(batches[0].ID)), expired, expired); err != nil {
			t.Fatalf("age %s: %v", dir, err)
		}
	}
	report, err := s.GC(t.Context())
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if report.StagingRemoved != 2 {
		t.Fatalf("StagingRemoved = %d, want 2", report.StagingRemoved)
	}
	if _, err := s.ReadPart(t.Context(), batches[0].ID, 0); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired outbox part err = %v, want ErrNotExist", err)
	}
	if err := s.PutPart(t.Context(), batches[0].ID, 0, bytes.Repeat([]byte{1}, int(batches[0].Parts[0].Size))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("put into expired batch err = %v, want ErrNotExist", err)
	}
	if _, err := s.ReadPart(t.Context(), batches[1].ID, 0); err != nil {
		t.Fatalf("fresh outbox part: %v", err)
	}
	if held, err := s.BeginBatch(t.Context(), batches[1]); err != nil || len(held) != 0 {
		t.Fatalf("fresh staged batch = %v, %v", held, err)
	}
}

func TestBlobShapedLikeManifestTransfersAsBlob(t *testing.T) {
	src := newStore(t)
	lookalike := Manifest{
		Schema: ManifestSchema, Media: "test.lookalike", Chunks: []ChunkRef{},
		Deps: []Ref{{Digest: Sum([]byte("never stored")), Kind: KindBlob, Size: 1}},
	}
	encoded, err := lookalike.Encode()
	if err != nil {
		t.Fatal(err)
	}
	ref, err := src.PutBlob(t.Context(), encoded)
	if err != nil {
		t.Fatal(err)
	}
	d, err := src.BuildBatch(t.Context(), closureEntries(t, src, []Ref{ref}))
	if err != nil {
		t.Fatal(err)
	}
	if want := []ObjectEntry{ObjectEntry(ref)}; !reflect.DeepEqual(d.Objects, want) {
		t.Fatalf("batch objects = %+v, want %+v", d.Objects, want)
	}
	dst := newStore(t)
	if _, err := dst.BeginBatch(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	putParts(t, dst, d, outboxParts(t, src, d), nil)
	if report, err := dst.CommitBatch(t.Context(), d.ID); err != nil || report.Stored != 1 {
		t.Fatalf("CommitBatch = %+v, %v; want the blob stored", report, err)
	}
	if missing, err := dst.Complete(t.Context(), []Ref{ref}); err != nil || missing != 0 {
		t.Fatalf("Complete = %d, %v; want 0", missing, err)
	}
}

func TestConcurrentSenderFinishesAfterAnotherCommits(t *testing.T) {
	src, roots, d := sourceBatch(t, 11, 3*ChunkSize)
	parts := outboxParts(t, src, d)
	dst := newStore(t)
	for sender := range 2 {
		if held, err := dst.BeginBatch(t.Context(), d); err != nil || len(held) != 0 {
			t.Fatalf("BeginBatch(sender %d) = %v, %v; want nothing held", sender, held, err)
		}
	}
	putParts(t, dst, d, parts, nil)
	first, err := dst.CommitBatch(t.Context(), d.ID)
	if err != nil || first.Stored != len(d.Objects) {
		t.Fatalf("first CommitBatch = %+v, %v", first, err)
	}
	if err := dst.PutPart(t.Context(), d.ID, 0, parts[0]); err != nil {
		t.Fatalf("second sender's PutPart after the commit: %v", err)
	}
	second, err := dst.CommitBatch(t.Context(), d.ID)
	if want := (CommitReport{Present: len(d.Objects)}); err != nil || second != want {
		t.Fatalf("second CommitBatch = %+v, %v; want %+v", second, err, want)
	}
	held, err := dst.BeginBatch(t.Context(), d)
	if want := []int{0, 1, 2, 3}[:len(d.Parts)]; err != nil || !reflect.DeepEqual(held, want) {
		t.Fatalf("BeginBatch after commit = %v, %v; want %v", held, err, want)
	}
	if missing, err := dst.Complete(t.Context(), roots); err != nil || missing != 0 {
		t.Fatalf("Complete = %d, %v; want 0", missing, err)
	}
	ageStaging(t, dst, committedDir, 2*StagingTTL)
	if _, err := dst.CommitBatch(t.Context(), d.ID); err == nil {
		t.Fatal("CommitBatch succeeded after GC swept the committed record")
	}
}

func ageStaging(t *testing.T, s *Store, dir string, age time.Duration) {
	t.Helper()
	old := time.Now().Add(-age)
	entries, err := os.ReadDir(filepath.Join(s.root, dir))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err := os.Chtimes(filepath.Join(s.root, dir, entry.Name()), old, old); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.GC(t.Context()); err != nil {
		t.Fatal(err)
	}
}
