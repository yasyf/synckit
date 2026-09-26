package artifact

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func writeRawBlob(t *testing.T, s *Store, b []byte) Ref {
	t.Helper()
	digest := Sum(b)
	if err := os.WriteFile(s.objectPath(digest), b, filePerm); err != nil {
		t.Fatal(err)
	}
	return Ref{Digest: digest, Kind: KindBlob, Size: int64(len(b))}
}

func TestPutGroupSplitsPastMaxDeps(t *testing.T) {
	s := newStore(t)
	deps := make([]Ref, 2*MaxDeps+1)
	for i := range deps {
		deps[i] = writeRawBlob(t, s, fmt.Appendf(nil, "dep %d", i))
	}
	root, err := s.PutGroup(t.Context(), "test/group", deps)
	if err != nil {
		t.Fatalf("PutGroup: %v", err)
	}
	if again, err := s.PutGroup(t.Context(), "test/group", deps); err != nil || again != root {
		t.Fatalf("repeated PutGroup = %+v, %v; want %+v", again, err, root)
	}
	top, err := s.Manifest(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	if top.Media != "test/group" || len(top.Deps) != 3 {
		t.Fatalf("root manifest %q holds %d deps, want test/group over 3 subgroups", top.Media, len(top.Deps))
	}
	var flattened []Ref
	for i, want := range []int{MaxDeps, MaxDeps, 1} {
		sub, err := s.Manifest(t.Context(), top.Deps[i])
		if err != nil {
			t.Fatal(err)
		}
		if sub.Media != GroupMedia || len(sub.Deps) != want {
			t.Fatalf("subgroup %d is %q with %d deps, want %q with %d", i, sub.Media, len(sub.Deps), GroupMedia, want)
		}
		flattened = append(flattened, sub.Deps...)
	}
	if !reflect.DeepEqual(flattened, deps) {
		t.Fatal("subgroups do not hold the deps in order")
	}
	if missing, err := s.Complete(t.Context(), []Ref{root}); err != nil || missing != 0 {
		t.Fatalf("Complete(root) = %d, %v; want 0 missing", missing, err)
	}
	closure, err := s.Closure(t.Context(), []Ref{root}, DefaultClosureBound)
	if err != nil || len(closure.Objects) != len(deps)+4 {
		t.Fatalf("Closure(root) = %d objects, %v; want %d", len(closure.Objects), err, len(deps)+4)
	}
	flat, err := s.PutGroup(t.Context(), "test/group", deps[:MaxDeps])
	if err != nil {
		t.Fatal(err)
	}
	if m, err := s.Manifest(t.Context(), flat); err != nil || len(m.Deps) != MaxDeps {
		t.Fatalf("group of exactly MaxDeps = %d deps, %v; want no split", len(m.Deps), err)
	}
}

func TestClosureBoundAppliesPerRoot(t *testing.T) {
	s := newStore(t)
	blobs := make([]Ref, 5)
	for i := range blobs {
		blobs[i] = writeRawBlob(t, s, fmt.Appendf(nil, "blob %d", i))
	}
	group := func(deps ...Ref) Ref {
		t.Helper()
		ref, err := s.PutGroup(t.Context(), "test/group", deps)
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	first, second := group(blobs[0], blobs[1]), group(blobs[2], blobs[3])
	wide := group(blobs[0], blobs[1], blobs[2])
	bound := ClosureBound{MaxObjects: 3, MaxDepth: 32, MaxBytes: 1 << 20}
	tests := []struct {
		name    string
		roots   []Ref
		objects int
		wantErr bool
	}{
		{"each root within the bound", []Ref{first, second}, 6, false},
		{"one root past the bound", []Ref{wide}, 0, true},
		{"shared objects count once in the union", []Ref{first, group(blobs[0], blobs[4])}, 5, false},
		{"shared objects still count toward each root", []Ref{first, group(blobs[0], blobs[1], blobs[4])}, 0, true},
		{"a root past the bound behind a smaller one", []Ref{first, wide}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			closure, err := s.Closure(t.Context(), tt.roots, bound)
			var bounded *ClosureError
			if tt.wantErr {
				if !errors.As(err, &bounded) || bounded.Bound != BoundObjects {
					t.Fatalf("Closure = %v, want an objects ClosureError", err)
				}
				return
			}
			if err != nil || len(closure.Objects) != tt.objects {
				t.Fatalf("Closure = %d objects, %v; want %d", len(closure.Objects), err, tt.objects)
			}
		})
	}
}

func TestPinSetsHoldTwiceMaxRoots(t *testing.T) {
	s := newStore(t)
	roots := make([]Ref, MaxPinRoots+1)
	for i := range roots {
		roots[i] = Ref{Digest: Sum(fmt.Appendf(nil, "root %d", i)), Kind: KindBlob, Size: 1}
	}
	if err := ValidateRoots(roots[:MaxRoots+1]); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ValidateRoots(MaxRoots+1) = %v, want ErrInvalid", err)
	}
	if err := s.SetPins(t.Context(), "union", roots[:MaxPinRoots]); err != nil {
		t.Fatalf("SetPins(2 x MaxRoots) = %v", err)
	}
	if err := s.SetPins(t.Context(), "union", roots); !errors.Is(err, ErrInvalid) {
		t.Fatalf("SetPins(2 x MaxRoots + 1) = %v, want ErrInvalid", err)
	}
	if _, err := s.GC(t.Context()); err != nil {
		t.Fatalf("GC over absent pinned roots = %v", err)
	}
}
