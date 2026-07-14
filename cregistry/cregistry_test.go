package cregistry

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"testing"
	"time"
)

// val is the domain payload used throughout the tests: a small struct so the
// value-LWW tiebreak runs against real (non-scalar) JSON, exercising the canonical
// marshaling path. Its zero value marshals deterministically too.
type val struct {
	Tag  string `json:"tag"`
	Seq  int    `json:"seq"`
	Flag bool   `json:"flag"`
}

func entry(added, removed Micros, v val) Entry[val] {
	return Entry[val]{Added: added, Removed: removed, Value: v}
}

// gen builds a deterministic registry from a seed so the property tests vary their
// inputs without any clock or PRNG. The seed fans out across a fixed id space; each
// id's add stamp, remove stamp, and value are pure functions of the seed and the id
// index, so the same seed always yields the same registry and different seeds
// explore add-wins, remove-wins, tie, and absent cases.
func gen(seed int) Registry[val] {
	const idSpace = 6
	r := New[val]()
	for i := range idSpace {
		mix := seed*7 + i*13
		switch mix % 5 {
		case 0:
			continue // id absent from this registry
		case 1:
			r.Add(id(i), value(seed, i), Micros(mix%9))
		case 2:
			r.Add(id(i), value(seed, i), Micros(mix%9))
			r.Remove(id(i), Micros((mix+3)%9))
		case 3:
			at := Micros(mix % 7)
			r.Add(id(i), value(seed, i), at)
			r.Remove(id(i), at) // deliberate tie => absent
		default:
			r.Add(id(i), value(seed, i), Micros(mix%9))
			r.Remove(id(i), Micros(mix%4))
		}
	}
	return r
}

func id(i int) string { return fmt.Sprintf("item-%d", i) }

func value(seed, i int) val {
	return val{Tag: fmt.Sprintf("v%d-%d", seed, i), Seq: seed + i, Flag: (seed+i)%2 == 0}
}

// seeds is the deterministic corpus the property tests range over. A wide spread of
// small integers is enough to hit every branch of gen across pairs and triples.
var seeds = func() []int {
	out := make([]int, 40)
	for i := range out {
		out[i] = i
	}
	return out
}()

// ---- ALGEBRA: the join-semilattice laws (property tests over gen'd registries) ----

func TestMergeCommutative(t *testing.T) {
	for _, sa := range seeds {
		for _, sb := range seeds {
			a, b := gen(sa), gen(sb)
			if ab, ba := Merge(a, b), Merge(b, a); !reflect.DeepEqual(ab, ba) {
				t.Fatalf("Merge not commutative for seeds (%d,%d):\n ab=%v\n ba=%v", sa, sb, ab, ba)
			}
		}
	}
}

func TestMergeAssociative(t *testing.T) {
	for _, sa := range seeds {
		for sb := sa; sb < sa+7; sb++ {
			for sc := sb; sc < sb+7; sc++ {
				a, b, c := gen(sa), gen(sb%len(seeds)), gen(sc%len(seeds))
				left := Merge(Merge(a, b), c)
				right := Merge(a, Merge(b, c))
				if !reflect.DeepEqual(left, right) {
					t.Fatalf("Merge not associative for seeds (%d,%d,%d):\n (ab)c=%v\n a(bc)=%v",
						sa, sb%len(seeds), sc%len(seeds), left, right)
				}
			}
		}
	}
}

func TestMergeIdempotent(t *testing.T) {
	for _, s := range seeds {
		a := gen(s)
		if aa := Merge(a, a); !reflect.DeepEqual(aa, a) {
			t.Fatalf("Merge(a,a) != a for seed %d:\n got=%v\n want=%v", s, aa, a)
		}
	}
}

// TestMergeIdempotentAbsorbsLeft is the second idempotence form: re-merging a side
// that was already folded in changes nothing — Merge(a, Merge(a,b)) == Merge(a,b).
func TestMergeIdempotentAbsorbsLeft(t *testing.T) {
	for _, sa := range seeds {
		for _, sb := range seeds {
			a, b := gen(sa), gen(sb)
			ab := Merge(a, b)
			if got := Merge(a, ab); !reflect.DeepEqual(got, ab) {
				t.Fatalf("Merge(a, Merge(a,b)) != Merge(a,b) for seeds (%d,%d):\n got=%v\n want=%v",
					sa, sb, got, ab)
			}
		}
	}
}

// TestMergePure proves Merge mutates neither input: snapshots of a and b taken
// before the merge still equal a and b after.
func TestMergePure(t *testing.T) {
	for _, sa := range seeds {
		for _, sb := range seeds {
			a, b := gen(sa), gen(sb)
			aSnap, bSnap := maps.Clone(a), maps.Clone(b)
			_ = Merge(a, b)
			if !reflect.DeepEqual(a, aSnap) {
				t.Fatalf("Merge mutated left input for seeds (%d,%d)", sa, sb)
			}
			if !reflect.DeepEqual(b, bSnap) {
				t.Fatalf("Merge mutated right input for seeds (%d,%d)", sa, sb)
			}
		}
	}
}

// ---- SCENARIO MATRIX: explicit named cases for each LWW rule ----

func TestEntryPresent(t *testing.T) {
	cases := []struct {
		id      string
		added   Micros
		removed Micros
		present bool
	}{
		{"never-removed", 5, 0, true},
		{"added-after-remove", 9, 4, true},
		{"removed-after-add", 4, 9, false},
		{"tie-removes-wins", 7, 7, false},
		{"both-zero-absent", 0, 0, false},
	}
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) {
			if got := entry(c.added, c.removed, val{}).Present(); got != c.present {
				t.Fatalf("Present(added=%d, removed=%d) = %v, want %v", c.added, c.removed, got, c.present)
			}
		})
	}
}

func TestScenarioMatrix(t *testing.T) {
	const key = "k"
	cases := []struct {
		id          string
		build       func(r Registry[val])
		wantPresent bool
		wantValue   val // checked only when wantPresent
	}{
		{
			id:          "add-wins",
			build:       func(r Registry[val]) { r.Add(key, val{Tag: "a", Seq: 1}, 5) },
			wantPresent: true,
			wantValue:   val{Tag: "a", Seq: 1},
		},
		{
			id: "remove-wins-later-remove",
			build: func(r Registry[val]) {
				r.Add(key, val{Tag: "a"}, 1)
				r.Remove(key, 2)
			},
			wantPresent: false,
		},
		{
			id: "concurrent-readd-after-remove",
			build: func(r Registry[val]) {
				r.Add(key, val{Tag: "first"}, 1)
				r.Remove(key, 2)
				r.Add(key, val{Tag: "second", Seq: 2}, 3)
			},
			wantPresent: true,
			wantValue:   val{Tag: "second", Seq: 2},
		},
		{
			id: "re-remove-after-readd",
			build: func(r Registry[val]) {
				r.Add(key, val{Tag: "first"}, 1)
				r.Remove(key, 2)
				r.Add(key, val{Tag: "second"}, 3)
				r.Remove(key, 4)
			},
			wantPresent: false,
		},
		{
			id: "tie-add-equals-remove-absent",
			build: func(r Registry[val]) {
				r.Add(key, val{Tag: "a"}, 7)
				r.Remove(key, 7)
			},
			wantPresent: false,
		},
	}
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) {
			r := New[val]()
			c.build(r)
			got := r[key]
			if got.Present() != c.wantPresent {
				t.Fatalf("%s: Present() = %v, want %v (entry=%+v)", c.id, got.Present(), c.wantPresent, got)
			}
			if c.wantPresent && got.Value != c.wantValue {
				t.Fatalf("%s: Value = %+v, want %+v", c.id, got.Value, c.wantValue)
			}
		})
	}
}

// TestScenarioMatrixOrderIndependent re-runs the remove-wins and re-add scenarios as
// independent replicas merged in BOTH orders, proving the outcome is the same no
// matter which side a replica sees first.
func TestScenarioMatrixOrderIndependent(t *testing.T) {
	const key = "k"
	cases := []struct {
		id          string
		left        func(r Registry[val])
		right       func(r Registry[val])
		wantPresent bool
		wantValue   val
	}{
		{
			id:          "added-here-removed-there",
			left:        func(r Registry[val]) { r.Add(key, val{Tag: "a"}, 1) },
			right:       func(r Registry[val]) { r.Remove(key, 2) },
			wantPresent: false,
		},
		{
			id:          "removed-here-readded-there",
			left:        func(r Registry[val]) { r.Add(key, val{Tag: "old"}, 1); r.Remove(key, 2) },
			right:       func(r Registry[val]) { r.Add(key, val{Tag: "new", Seq: 9}, 3) },
			wantPresent: true,
			wantValue:   val{Tag: "new", Seq: 9},
		},
	}
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) {
			l, r := New[val](), New[val]()
			c.left(l)
			c.right(r)
			for _, m := range []Registry[val]{Merge(l, r), Merge(r, l)} {
				got := m[key]
				if got.Present() != c.wantPresent {
					t.Fatalf("%s: Present() = %v, want %v (entry=%+v)", c.id, got.Present(), c.wantPresent, got)
				}
				if c.wantPresent && got.Value != c.wantValue {
					t.Fatalf("%s: Value = %+v, want %+v", c.id, got.Value, c.wantValue)
				}
			}
		})
	}
}

func TestValueLWWLaterAddWins(t *testing.T) {
	a := Registry[val]{"k": entry(1, 0, val{Tag: "early", Seq: 1})}
	b := Registry[val]{"k": entry(2, 0, val{Tag: "late", Seq: 2})}
	want := val{Tag: "late", Seq: 2}
	if got := Merge(a, b)["k"].Value; got != want {
		t.Fatalf("Merge(a,b) value = %+v, want %+v", got, want)
	}
	if got := Merge(b, a)["k"].Value; got != want {
		t.Fatalf("Merge(b,a) value = %+v, want %+v", got, want)
	}
}

// TestValueLWWEqualAddDeterministic proves the equal-add tiebreak is both
// deterministic and order-independent: two concurrent adds at the SAME stamp with
// different values always resolve to the same winner regardless of merge order, and
// that winner is the lexicographically-larger canonical JSON.
func TestValueLWWEqualAddDeterministic(t *testing.T) {
	left := val{Tag: "alpha", Seq: 1}
	right := val{Tag: "beta", Seq: 2}
	a := Registry[val]{"k": entry(5, 0, left)}
	b := Registry[val]{"k": entry(5, 0, right)}

	ab := Merge(a, b)["k"].Value
	ba := Merge(b, a)["k"].Value
	if ab != ba {
		t.Fatalf("equal-add tiebreak not order-independent: ab=%+v ba=%+v", ab, ba)
	}

	want := left
	if mustJSON(t, left) < mustJSON(t, right) {
		want = right
	}
	if ab != want {
		t.Fatalf("equal-add winner = %+v, want lexicographically-larger JSON winner %+v", ab, want)
	}
}

// TestValueLWWEqualAddCommutativeAcrossManyPairs hammers the equal-add tiebreak over
// many distinct value pairs to be sure no pair resolves order-dependently.
func TestValueLWWEqualAddCommutativeAcrossManyPairs(t *testing.T) {
	vals := []val{
		{Tag: "a", Seq: 0},
		{Tag: "a", Seq: 1},
		{Tag: "b", Seq: 0},
		{Tag: "", Seq: 0, Flag: true},
		{Tag: "z", Seq: -1},
		{Tag: "aa", Seq: 0},
	}
	for i, vi := range vals {
		for j, vj := range vals {
			a := Registry[val]{"k": entry(3, 1, vi)}
			b := Registry[val]{"k": entry(3, 1, vj)}
			ab, ba := Merge(a, b)["k"], Merge(b, a)["k"]
			if !reflect.DeepEqual(ab, ba) {
				t.Fatalf("equal-add pair (%d,%d) order-dependent: ab=%+v ba=%+v", i, j, ab, ba)
			}
		}
	}
}

func TestAddMonotoneNoRegression(t *testing.T) {
	r := New[val]()
	r.Add("k", val{Tag: "new", Seq: 2}, 5)
	r.Add("k", val{Tag: "stale", Seq: 1}, 3) // older stamp: must not regress
	got := r["k"]
	if got.Added != 5 {
		t.Fatalf("stale Add regressed Added to %d, want 5", got.Added)
	}
	if (got.Value != val{Tag: "new", Seq: 2}) {
		t.Fatalf("stale Add regressed Value to %+v, want new", got.Value)
	}
}

// TestAddEqualStampNoOverwrite proves an Add at the SAME stamp does not overwrite
// the value — only a strictly-newer add advances the register.
func TestAddEqualStampNoOverwrite(t *testing.T) {
	r := New[val]()
	r.Add("k", val{Tag: "first"}, 5)
	r.Add("k", val{Tag: "second"}, 5)
	if got := r["k"].Value; (got != val{Tag: "first"}) {
		t.Fatalf("equal-stamp Add overwrote value to %+v, want first", got)
	}
}

func TestRemoveMonotoneTakesMax(t *testing.T) {
	r := New[val]()
	r.Add("k", val{Tag: "a"}, 1)
	r.Remove("k", 8)
	r.Remove("k", 4) // older stamp: must not lower Removed
	if got := r["k"].Removed; got != 8 {
		t.Fatalf("stale Remove lowered Removed to %d, want 8", got)
	}
}

func TestRemoveAbsentRecordsStamp(t *testing.T) {
	r := New[val]()
	r.Remove("k", 6) // remove arrives before any add is seen
	got := r["k"]
	if got.Removed != 6 {
		t.Fatalf("Remove on absent id recorded Removed=%d, want 6", got.Removed)
	}
	if got.Present() {
		t.Fatal("absent id with only a remove must not be Present")
	}
	r.Add("k", val{Tag: "late"}, 9) // concurrent add ordered after the recorded remove
	if !r["k"].Present() {
		t.Fatal("add stamped after the recorded remove must be Present")
	}
}

func TestPresentSubset(t *testing.T) {
	r := New[val]()
	r.Add("present-a", val{Tag: "a"}, 5)
	r.Add("present-b", val{Tag: "b"}, 5)
	r.Add("removed", val{Tag: "c"}, 1)
	r.Remove("removed", 2)
	r.Add("tie", val{Tag: "d"}, 3)
	r.Remove("tie", 3)

	present := r.Present()
	got := slices.Sorted(maps.Keys(present))
	want := []string{"present-a", "present-b"}
	if !slices.Equal(got, want) {
		t.Fatalf("Present() ids = %v, want %v", got, want)
	}
	if len(r) != 4 {
		t.Fatalf("Present() mutated receiver: len = %d, want 4", len(r))
	}
}

// ---- COMPACTION: horizon-gated tombstone drop and its resurrection hazard ----

func TestCompact(t *testing.T) {
	const now, horizon = Micros(1_000), Micros(100)
	const cutoff = now - horizon // 900: a tombstone is expired iff Removed < cutoff

	tests := []struct {
		name  string
		build func(r Registry[val])
		want  []string
	}{
		{
			name:  "present entry always kept",
			build: func(r Registry[val]) { r.Add("live", val{Tag: "a"}, 500) },
			want:  []string{"live"},
		},
		{
			name: "re-added entry kept despite an expired remove stamp",
			build: func(r Registry[val]) {
				r.Add("readd", val{Tag: "a"}, 100)
				r.Remove("readd", 500) // expired, but…
				r.Add("readd", val{Tag: "b"}, 950)
			},
			want: []string{"readd"},
		},
		{
			name: "young tombstone kept",
			build: func(r Registry[val]) {
				r.Add("recent", val{Tag: "b"}, 800)
				r.Remove("recent", 950) // 950 >= 900
			},
			want: []string{"recent"},
		},
		{
			name: "tombstone exactly at the cutoff kept",
			build: func(r Registry[val]) {
				r.Add("edge", val{Tag: "c"}, 400)
				r.Remove("edge", cutoff) // 900 >= 900
			},
			want: []string{"edge"},
		},
		{
			name: "old tombstone dropped",
			build: func(r Registry[val]) {
				r.Add("stale", val{Tag: "d"}, 100)
				r.Remove("stale", 500) // 500 < 900
			},
			want: []string{},
		},
		{
			name:  "zero-value phantom dropped past horizon",
			build: func(r Registry[val]) { r.Remove("phantom", 500) }, // Added==0, Removed 500 < 900
			want:  []string{},
		},
		{
			name:  "phantom within horizon kept",
			build: func(r Registry[val]) { r.Remove("phantom", 950) }, // Added==0, Removed 950 >= 900
			want:  []string{"phantom"},
		},
		{
			name: "mixed set keeps live and young, drops expired",
			build: func(r Registry[val]) {
				r.Add("live", val{Tag: "a"}, 500)
				r.Add("recent", val{Tag: "b"}, 800)
				r.Remove("recent", 950)
				r.Add("stale", val{Tag: "d"}, 100)
				r.Remove("stale", 500)
				r.Remove("phantom", 400)
			},
			want: []string{"live", "recent"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New[val]()
			tt.build(r)
			before := mustJSON(t, r)

			compacted := r.Compact(now, horizon)
			got := slices.Sorted(maps.Keys(compacted))
			if !slices.Equal(got, tt.want) {
				t.Fatalf("Compact ids = %v, want %v", got, tt.want)
			}
			for id, e := range compacted {
				if e != r[id] {
					t.Fatalf("Compact altered surviving entry %q: got %+v, want %+v", id, e, r[id])
				}
			}
			if after := mustJSON(t, r); after != before {
				t.Fatalf("Compact mutated the receiver:\n before=%s\n  after=%s", before, after)
			}
		})
	}
}

// TestCompactResurrectionPastHorizon documents the hazard Compact trades space
// for: dropping an expired tombstone lets a long-offline peer resurrect the id.
// Registry A removes X and compacts the tombstone away past the horizon; peer B
// stayed offline longer than the horizon and still carries X as a live add.
// Merging B back in has no remove stamp left to out-order the add, so X returns to
// the present set — proof that horizon must exceed the longest peer offline time.
func TestCompactResurrectionPastHorizon(t *testing.T) {
	const now, horizon = Micros(1_000), Micros(100)

	a := New[val]()
	a.Add("x", val{Tag: "live"}, 200)
	a.Remove("x", 500) // 500 < now-horizon (900): an expired tombstone
	if a["x"].Present() {
		t.Fatal("precondition: X must be removed in A before compaction")
	}

	compacted := a.Compact(now, horizon)
	if _, ok := compacted["x"]; ok {
		t.Fatal("Compact kept a tombstone older than the horizon; the hazard needs it dropped")
	}

	// B is the offline peer that never saw the remove: it still carries X live.
	b := New[val]()
	b.Add("x", val{Tag: "live"}, 200)

	merged := Merge(compacted, b)
	if !merged["x"].Present() {
		t.Fatal("resurrection hazard not reproduced: X should be present again after merging a stale peer")
	}
}

// ---- JSON: deterministic, exact round-trip ----

func TestRegistryJSONRoundTrip(t *testing.T) {
	for _, s := range seeds {
		r := gen(s)
		out, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("seed %d: Marshal: %v", s, err)
		}
		var back Registry[val]
		if err := json.Unmarshal(out, &back); err != nil {
			t.Fatalf("seed %d: Unmarshal: %v", s, err)
		}
		if !reflect.DeepEqual(back, r) {
			t.Fatalf("seed %d: round-trip mismatch:\n got=%v\n want=%v", s, back, r)
		}
	}
}

// TestRegistryJSONDeterministic proves the encoding is byte-stable: Go sorts object
// keys, so the same registry always marshals to the same bytes across runs.
func TestRegistryJSONDeterministic(t *testing.T) {
	r := New[val]()
	r.Add("zebra", val{Tag: "z"}, 3)
	r.Add("alpha", val{Tag: "a"}, 1)
	r.Add("mike", val{Tag: "m"}, 2)
	r.Remove("mike", 5)

	first, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for range 8 {
		again, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if string(again) != string(first) {
			t.Fatalf("non-deterministic JSON:\n %s\n %s", first, again)
		}
	}
	want := `{` +
		`"alpha":{"added_at":1,"value":{"tag":"a","seq":0,"flag":false}},` +
		`"mike":{"added_at":2,"removed_at":5,"value":{"tag":"m","seq":0,"flag":false}},` +
		`"zebra":{"added_at":3,"value":{"tag":"z","seq":0,"flag":false}}` +
		`}`
	if string(first) != want {
		t.Fatalf("JSON encoding =\n %s\nwant\n %s", first, want)
	}
}

// TestRemovedAtOmittedWhenZero pins the omitempty on removed_at: a never-removed
// item carries no removed_at key, while a removed item does.
func TestRemovedAtOmittedWhenZero(t *testing.T) {
	never, err := json.Marshal(entry(4, 0, val{Tag: "a"}))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if want := `{"added_at":4,"value":{"tag":"a","seq":0,"flag":false}}`; string(never) != want {
		t.Fatalf("never-removed entry JSON = %s, want %s", never, want)
	}
	removed, err := json.Marshal(entry(4, 6, val{Tag: "a"}))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if want := `{"added_at":4,"removed_at":6,"value":{"tag":"a","seq":0,"flag":false}}`; string(removed) != want {
		t.Fatalf("removed entry JSON = %s, want %s", removed, want)
	}
}

// ---- CONVERGENCE: strong eventual consistency across many merge orderings ----

// op is one mutation applied to a replica during the convergence test.
type op struct {
	add bool
	id  string
	v   val
	at  Micros
}

// replicaOps builds a deterministic, replica-specific sequence of adds and removes
// over a shared id space. Every replica draws from the same ids and stamps so their
// mutations genuinely conflict (add vs remove vs re-add on the same id at different
// stamps), which is exactly what convergence must reconcile. The id space is
// partitioned so the merged mesh always lands on a non-trivial MIX of present and
// absent items — otherwise a convergence run where everything ended up removed would
// only ever assert empty-equals-empty. No clock or PRNG is involved — the sequence
// is a pure function of the replica index.
//
//   - item-0 (present-anchor): every replica re-adds it at an ever-rising stamp, so
//     no remove can win — it is always present in the merge.
//   - item-1 (remove-anchor): every replica removes it at an ever-rising stamp and
//     only ever adds it earlier, so a remove always wins — it is always absent.
//   - item-2..4 (churn): genuinely concurrent add/remove/re-add at interleaved
//     stamps, the hard reconciliation cases.
func replicaOps(replica int) []op {
	const steps = 12
	ops := make([]op, 0, steps+2)
	// Present-anchor: a high, replica-rising add stamp nothing removes.
	ops = append(ops, op{add: true, id: id(0), v: value(replica, 0), at: Micros(100 + replica)})
	// Remove-anchor: an early add then a high, replica-rising remove that always wins.
	ops = append(ops, op{add: true, id: id(1), v: value(replica, 1), at: Micros(1)})
	ops = append(ops, op{add: false, id: id(1), at: Micros(100 + replica)})
	for s := range steps {
		mix := replica*31 + s*17
		key := id(2 + mix%3) // churn bucket: item-2..4
		at := Micros(mix%20 + 1)
		if mix%3 == 0 {
			ops = append(ops, op{add: false, id: key, at: at})
			continue
		}
		ops = append(ops, op{add: true, id: key, v: value(replica, s), at: at})
	}
	return ops
}

func buildReplica(replica int) Registry[val] {
	r := New[val]()
	for _, o := range replicaOps(replica) {
		if o.add {
			r.Add(o.id, o.v, o.at)
		} else {
			r.Remove(o.id, o.at)
		}
	}
	return r
}

// TestConvergence is the strong-eventual-consistency proof: build N replicas with
// conflicting op sequences, then fold them together in several different
// orders/groupings — left-to-right, right-to-left, and a balanced pairwise tree —
// and assert every folding yields the byte-identical registry AND the identical
// Present() set. Order and grouping of delivery cannot change where the mesh lands.
func TestConvergence(t *testing.T) {
	for _, n := range []int{2, 3, 4, 5, 7} {
		t.Run(fmt.Sprintf("replicas=%d", n), func(t *testing.T) {
			replicas := make([]Registry[val], n)
			for i := range replicas {
				replicas[i] = buildReplica(i)
			}

			foldings := map[string]Registry[val]{
				"left-to-right": foldLeft(replicas),
				"right-to-left": foldLeft(reversed(replicas)),
				"pairwise-tree": foldTree(replicas),
				"reverse-tree":  foldTree(reversed(replicas)),
			}

			want := foldings["left-to-right"]
			wantJSON := mustJSON(t, want)
			wantPresent := mustJSON(t, want.Present())
			for name, got := range foldings {
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("folding %q diverged:\n got=%v\n want=%v", name, got, want)
				}
				if gotJSON := mustJSON(t, got); gotJSON != wantJSON {
					t.Fatalf("folding %q JSON diverged:\n got=%s\n want=%s", name, gotJSON, wantJSON)
				}
				if gotPresent := mustJSON(t, got.Present()); gotPresent != wantPresent {
					t.Fatalf("folding %q Present() diverged:\n got=%s\n want=%s", name, gotPresent, wantPresent)
				}
			}
		})
	}
}

// TestConvergenceWithDuplicateDelivery proves convergence survives redelivery: a
// replica delivered twice, and a replica re-merged into the running fold, leave the
// result identical to delivering each exactly once. This is idempotence under the
// realistic anti-entropy pattern where the same state arrives more than once.
func TestConvergenceWithDuplicateDelivery(t *testing.T) {
	const n = 5
	replicas := make([]Registry[val], n)
	for i := range replicas {
		replicas[i] = buildReplica(i)
	}
	once := foldLeft(replicas)

	// Deliver every replica twice, interleaved, plus a re-merge of the running state.
	noisy := New[val]()
	for _, r := range replicas {
		noisy = Merge(noisy, r)
		noisy = Merge(noisy, r)           // immediate duplicate
		noisy = Merge(noisy, replicas[0]) // stale redelivery of the first replica
	}
	if !reflect.DeepEqual(noisy, once) {
		t.Fatalf("duplicate/stale delivery diverged from clean fold:\n got=%v\n want=%v", noisy, once)
	}
	if mustJSON(t, noisy.Present()) != mustJSON(t, once.Present()) {
		t.Fatal("duplicate delivery changed the Present() set")
	}
}

func foldLeft(rs []Registry[val]) Registry[val] {
	acc := New[val]()
	for _, r := range rs {
		acc = Merge(acc, r)
	}
	return acc
}

// foldTree merges rs as a balanced binary tree (different association than foldLeft)
// to exercise associativity in the convergence proof.
func foldTree(rs []Registry[val]) Registry[val] {
	if len(rs) == 1 {
		return rs[0]
	}
	mid := len(rs) / 2
	return Merge(foldTree(rs[:mid]), foldTree(rs[mid:]))
}

func reversed(rs []Registry[val]) []Registry[val] {
	out := slices.Clone(rs)
	slices.Reverse(out)
	return out
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return string(out)
}

// TestUnixMicros pins the one bridge from wall time into the package: a known
// instant converts to its Unix-microsecond stamp.
func TestUnixMicros(t *testing.T) {
	instant := time.Unix(1, 234567000) // 1.234567s after the epoch
	if got := UnixMicros(instant); got != 1_234_567 {
		t.Fatalf("UnixMicros = %d, want 1234567", got)
	}
}
