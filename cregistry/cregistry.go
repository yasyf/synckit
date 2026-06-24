// Package cregistry is the convergent item registry: a LWW-Element-Set CRDT (a
// join-semilattice) that converges add/remove/concurrent-readd across replicas and
// carries a per-item domain payload via a last-write-wins register tied to the add
// timestamp.
//
// Each item is an [Entry] keyed by a stable id string. An item is present iff its
// add stamp is strictly newer than its remove stamp, so a remove deterministically
// wins ties and a later add re-admits a previously removed item. [Merge] is the
// lattice join — a pure, per-field maximum with an order-independent value
// tiebreak — making the whole structure idempotent, commutative, and associative,
// which is what gives a mesh of replicas strong eventual consistency: merge the
// same set of mutations in any order or grouping and every replica lands on the
// identical registry.
//
// The package is pure and clock-free: every timestamp is a [Micros] passed in by
// the caller, never read from the wall clock here, so merges are deterministic and
// the proof tests stay reproducible. It assumes an NTP-synced tailnet — replica
// clocks are close enough that the newest add genuinely is the intended winner;
// reconciling real clock skew (vector clocks, hybrid logical clocks) is out of
// scope.
//
// The value type V must capture its full identity in its JSON encoding: the
// equal-add tiebreak orders values by their canonical JSON, so two values that
// marshal to the same bytes are treated as the same value. Don't carry
// merge-significant state in a json:"-" (or otherwise unserialized) field.
package cregistry

import (
	"bytes"
	"encoding/json"
	"time"
)

// Micros is a Unix timestamp in microseconds, the stamp type for registry adds and
// removes. It is a distinct named type so a registry stamp can never be confused
// with a plain count or a nanosecond time; build one from a [time.Time] with
// [UnixMicros].
type Micros int64

// UnixMicros converts a wall-clock time to the microsecond stamp the registry
// orders by. It is the only bridge from real time into the package; the CRDT logic
// itself never reads the clock so that merges stay deterministic.
func UnixMicros(t time.Time) Micros {
	return Micros(t.UnixMicro())
}

// Entry is one item's last-write-wins record: the stamp it was last added at, the
// stamp it was last removed at, and the domain value carried by that add. Added and
// Removed only ever advance (see [Registry.Add] and [Registry.Remove]); Value is
// the payload of whichever add is currently winning.
type Entry[V any] struct {
	Added   Micros `json:"added_at"`
	Removed Micros `json:"removed_at,omitempty"`
	Value   V      `json:"value"`
}

// Present reports whether the item is currently in the set: true iff its add stamp
// is strictly newer than its remove stamp. A tie (Added == Removed) is absent — the
// remove wins, deterministically — so a re-add must carry a strictly later stamp to
// re-admit the item.
func (e Entry[V]) Present() bool {
	return e.Added > e.Removed
}

// Registry is the convergent set itself: a map from each item's stable id to its
// [Entry]. The zero registry is not usable; build one with [New]. It serializes to
// deterministic JSON because Go sorts object keys, so the on-disk form round-trips
// byte-for-byte.
type Registry[V any] map[string]Entry[V]

// New returns an empty registry ready for [Registry.Add] and [Registry.Remove].
func New[V any]() Registry[V] {
	return make(Registry[V])
}

// Add admits id with value, stamping the add at. It is monotone: it advances the
// add stamp (and adopts the new value) only when at is strictly newer than the
// item's current add stamp, so a stale or replayed add never regresses a newer one.
// An id not yet in the registry is created.
func (r Registry[V]) Add(id string, value V, at Micros) {
	entry := r[id]
	if at > entry.Added {
		entry.Added = at
		entry.Value = value
		r[id] = entry
	}
}

// Remove retires id, stamping the removal at. It is monotone: the remove stamp only
// ever advances to the maximum stamp seen, so a stale remove never undoes a newer
// one. Removing an id absent from the registry records the removal so a concurrent
// add can be ordered against it.
func (r Registry[V]) Remove(id string, at Micros) {
	entry := r[id]
	if at > entry.Removed {
		entry.Removed = at
		r[id] = entry
	}
}

// Present returns the subset of items currently in the set — those whose [Entry] is
// Present. The result is a new registry; the receiver is not modified.
func (r Registry[V]) Present() Registry[V] {
	present := make(Registry[V])
	for id, entry := range r {
		if entry.Present() {
			present[id] = entry
		}
	}
	return present
}

// Merge is the lattice join of two registries: a new registry where each id present
// in either input takes the per-field maximum of its stamps — the newest add, the
// newest remove — and the value of whichever side has the strictly-newer add. When
// both sides share the same add stamp, the value is broken deterministically and
// order-independently by canonical JSON bytes (the lexicographically-larger
// marshaling wins), so Merge(a, b) and Merge(b, a) agree on Value as well as on
// every stamp. Merge mutates neither input.
func Merge[V any](a, b Registry[V]) Registry[V] {
	merged := make(Registry[V], max(len(a), len(b)))
	for id, ea := range a {
		merged[id] = joinEntry(ea, b[id])
	}
	for id, eb := range b {
		if _, done := merged[id]; !done {
			merged[id] = joinEntry(a[id], eb)
		}
	}
	return merged
}

// joinEntry joins two entries for the same id by taking the maximum of each stamp
// and the value tied to the winning add. On an add-stamp tie the value is chosen by
// canonical JSON bytes so the join is commutative in the value too.
func joinEntry[V any](a, b Entry[V]) Entry[V] {
	return Entry[V]{
		Added:   max(a.Added, b.Added),
		Removed: max(a.Removed, b.Removed),
		Value:   winningValue(a, b),
	}
}

// winningValue picks the value of the entry with the strictly-newer add, or — when
// the add stamps are equal — the one whose canonical JSON marshaling is
// lexicographically larger, an order-independent tiebreak.
func winningValue[V any](a, b Entry[V]) V {
	switch {
	case a.Added > b.Added:
		return a.Value
	case b.Added > a.Added:
		return b.Value
	case bytes.Compare(canonical(a.Value), canonical(b.Value)) >= 0:
		return a.Value
	default:
		return b.Value
	}
}

// canonical returns the JSON marshaling of v used to order equal-add values. The
// value types carried here are JSON-serializable by construction, so a marshal
// failure is impossible; it panics rather than silently picking a winner.
func canonical[V any](v V) []byte {
	out, err := json.Marshal(v)
	if err != nil {
		panic("cregistry: registry value is not JSON-serializable: " + err.Error())
	}
	return out
}
