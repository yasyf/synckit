package artifact

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGCKeepsPinnedAndYoungClosures(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	pinned, err := s.Put(ctx, bytes.NewReader(randomBytes(8, ChunkSize+1)), "test.bin")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.SetPins(ctx, "owner", []Ref{pinned}); err != nil {
		t.Fatalf("SetPins: %v", err)
	}
	old, err := s.PutBlob(ctx, []byte("old unreachable"))
	if err != nil {
		t.Fatalf("PutBlob old: %v", err)
	}
	young, err := s.PutBlob(ctx, []byte("young unreachable"))
	if err != nil {
		t.Fatalf("PutBlob young: %v", err)
	}
	shared, err := s.PutBlob(ctx, []byte("shared chunk"))
	if err != nil {
		t.Fatalf("PutBlob shared: %v", err)
	}
	ageAll(t, s, GCGrace+time.Minute)
	youngManifest, err := s.PutGroup(ctx, "test.group", []Ref{shared})
	if err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	now := time.Now()
	if err := os.Chtimes(s.objectPath(young.Digest), now, now); err != nil {
		t.Fatalf("touch young: %v", err)
	}
	ageShared := now.Add(-GCGrace - time.Minute)
	if err := os.Chtimes(s.objectPath(shared.Digest), ageShared, ageShared); err != nil {
		t.Fatalf("age shared: %v", err)
	}

	report, err := s.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if want := (GCReport{Marked: 3, Removed: 1, FreedBytes: old.Size}); report != want {
		t.Fatalf("GC = %+v, want %+v", report, want)
	}
	missing, err := s.Has(ctx, []Digest{old.Digest, young.Digest, shared.Digest, youngManifest.Digest})
	if err != nil || !reflect.DeepEqual(missing, []Digest{old.Digest}) {
		t.Fatalf("Has after GC = %v, %v; want [%s]", missing, err, old.Digest)
	}
	for _, root := range []Ref{pinned, youngManifest} {
		if n, err := s.Complete(ctx, []Ref{root}); err != nil || n != 0 {
			t.Fatalf("Complete(%s) = %d, %v; want 0", root.Digest, n, err)
		}
	}

	if err := s.SetPins(ctx, "owner", nil); err != nil {
		t.Fatalf("SetPins(nil): %v", err)
	}
	ageAll(t, s, GCGrace+time.Minute)
	report, err = s.GC(ctx)
	if err != nil {
		t.Fatalf("GC after unpin: %v", err)
	}
	if report.Marked != 0 || report.Removed != 6 {
		t.Fatalf("GC after unpin = %+v, want 0 marked and 6 removed", report)
	}
	if files := objectFiles(t, s); len(files) != 0 {
		t.Fatalf("objects after unpinned GC = %v", files)
	}
}

func TestPins(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	r1 := Ref{Digest: Sum([]byte("one")), Kind: KindBlob, Size: 3}
	r2 := Ref{Digest: Sum([]byte("two")), Kind: KindManifest, Size: 9}
	if err := s.SetPins(ctx, "b", []Ref{r1}); err != nil {
		t.Fatalf("SetPins b: %v", err)
	}
	if err := s.SetPins(ctx, "a", []Ref{r1, r2}); err != nil {
		t.Fatalf("SetPins a: %v", err)
	}
	if err := s.SetPins(ctx, "a", []Ref{r2}); err != nil {
		t.Fatalf("replace a: %v", err)
	}
	pins, err := s.Pins(ctx)
	if err != nil {
		t.Fatalf("Pins: %v", err)
	}
	if len(pins) != 2 || pins[0].Owner != "a" || !reflect.DeepEqual(pins[0].Roots, []Ref{r2}) || pins[1].Owner != "b" || !reflect.DeepEqual(pins[1].Roots, []Ref{r1}) {
		t.Fatalf("Pins = %+v, want a:[two] b:[one]", pins)
	}
	if pins[0].UpdatedAt.IsZero() {
		t.Fatal("pin set is unstamped")
	}
	if err := s.SetPins(ctx, "a", nil); err != nil {
		t.Fatalf("remove a: %v", err)
	}
	if err := s.SetPins(ctx, "never", nil); err != nil {
		t.Fatalf("remove absent: %v", err)
	}
	pins, err = s.Pins(ctx)
	if err != nil || len(pins) != 1 || pins[0].Owner != "b" {
		t.Fatalf("Pins after remove = %+v, %v; want [b]", pins, err)
	}
	tests := []struct {
		name  string
		owner string
		roots []Ref
	}{
		{"empty owner", "", []Ref{r1}},
		{"newline owner", "a\nb", []Ref{r1}},
		{"duplicate roots", "c", []Ref{r1, r1}},
		{"invalid root", "c", []Ref{{Digest: "nope", Kind: KindBlob, Size: 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := s.SetPins(ctx, tt.owner, tt.roots); !errors.Is(err, ErrInvalid) {
				t.Fatalf("SetPins err = %v, want ErrInvalid", err)
			}
		})
	}
	pinFile := s.pinPath("b")
	pin, err := os.ReadFile(pinFile) //nolint:gosec // a pin path under this test's store
	if err != nil {
		t.Fatalf("read pin: %v", err)
	}
	if err := os.WriteFile(pinFile, []byte(strings.Replace(string(pin), `"owner":"b"`, `"owner":"z"`, 1)), 0o600); err != nil { //nolint:gosec // a pin path under this test's store
		t.Fatalf("rewrite pin: %v", err)
	}
	if _, err := s.Pins(ctx); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Pins with a misfiled owner err = %v, want ErrInvalid", err)
	}
}

func TestConcurrentWritesSurviveGC(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	const workers = 6
	refs := make([]Ref, workers)
	errs := make(chan error, 2*workers+1)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			content := append(randomBytes(300+worker, ChunkSize), randomBytes(1, 64)...)
			ref, err := s.Put(ctx, bytes.NewReader(content), "test.bin")
			if err != nil {
				errs <- err
				return
			}
			group, err := s.PutGroup(ctx, "test.group", []Ref{ref})
			if err == nil {
				err = s.SetPins(ctx, fmt.Sprint("worker-", worker), []Ref{group})
			}
			var closure Closure
			if err == nil {
				closure, err = s.Closure(ctx, []Ref{group}, DefaultClosureBound)
			}
			if err == nil {
				digests := make([]Digest, 0, len(closure.Objects))
				for _, object := range closure.Objects {
					digests = append(digests, object.Digest)
				}
				_, err = s.BuildBatch(ctx, digests)
			}
			refs[worker] = group
			errs <- err
		})
	}
	stop := make(chan struct{})
	var collector sync.WaitGroup
	collector.Go(func() {
		for {
			select {
			case <-stop:
				errs <- nil
				return
			default:
			}
			if _, err := s.GC(ctx); err != nil {
				errs <- err
				return
			}
		}
	})
	wg.Wait()
	close(stop)
	collector.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent operation: %v", err)
		}
	}
	ageAll(t, s, GCGrace+time.Minute)
	if _, err := s.GC(ctx); err != nil {
		t.Fatalf("GC: %v", err)
	}
	if err := s.Verify(ctx, refs); err != nil {
		t.Fatalf("Verify pinned refs after GC: %v", err)
	}
}
