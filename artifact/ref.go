// Package artifact is synckit's content-addressed object store and the wire
// types that carry its objects between peers: blobs split at fixed ChunkSize
// offsets, strictly canonical manifests that list chunks and depend on other
// objects, and resumable zstd batches that move a dependency closure to a
// peer.
package artifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	// ChunkSize is the largest blob. Put splits content at fixed ChunkSize
	// offsets, so an append-only file reuses every unchanged prefix chunk.
	ChunkSize = 1 << 20
	// PartSize is the largest compressed transport part: one batch.put call.
	PartSize = 1 << 20
	// MaxManifestBytes bounds one manifest's canonical encoding.
	MaxManifestBytes = 1 << 20
	// MaxDeps bounds one manifest's dependency list.
	MaxDeps = 4096
	// MaxBatchRaw bounds the uncompressed object bytes of one batch.
	MaxBatchRaw = 32 << 20
	// MaxBatchObjects bounds the objects of one batch.
	MaxBatchObjects = 16384
	// MaxHaveDigests bounds the digests of one have query.
	MaxHaveDigests = 16384
	// MaxClosurePage bounds the objects of one closure page.
	MaxClosurePage = 16384
	// MaxRoots bounds the artifact roots of one change or pin set.
	MaxRoots = 4096
	// MaxIncoming bounds the receive batches one store stages at once.
	MaxIncoming = 8
	// WindowSize is the zstd encoder window and the decoder's largest
	// accepted window.
	WindowSize = 4 << 20
	// StagingTTL is how long an outbox or incoming batch survives GC after
	// its last write.
	StagingTTL = 24 * time.Hour
	// GCGrace is how long an unreachable object survives GC after its last
	// write or touch.
	GCGrace = time.Hour
)

const (
	// ManifestSchema identifies the version-1 manifest encoding.
	ManifestSchema = "synckit.artifact.manifest.v1"
	// BatchSchema identifies the version-1 batch descriptor and pack stream.
	BatchSchema = "synckit.artifact.batch.v1"
	// BatchCodec names the compression every batch part stream uses.
	BatchCodec = "zstd"
)

const (
	// BoundObjects names the ClosureBound.MaxObjects limit in a ClosureError.
	BoundObjects = "objects"
	// BoundDepth names the ClosureBound.MaxDepth limit in a ClosureError.
	BoundDepth = "depth"
	// BoundBytes names the ClosureBound.MaxBytes limit in a ClosureError.
	BoundBytes = "bytes"
)

const (
	packMagic     = "SKP1"
	maxPackBytes  = len(packMagic) + MaxBatchObjects*(1+sha256.Size+binary.MaxVarintLen64) + MaxBatchRaw + 1
	maxBatchParts = maxPackBytes/PartSize + 2
	maxWireInt    = 1<<53 - 1
)

// ErrInvalid marks every validation failure of an artifact value.
var ErrInvalid = errors.New("artifact: invalid")

// DefaultClosureBound caps a closure walk: 2^18 objects, 32 manifest levels,
// and 64 GiB of stored bytes.
var DefaultClosureBound = ClosureBound{MaxObjects: 1 << 18, MaxDepth: 32, MaxBytes: 64 << 30}

var mediaPattern = regexp.MustCompile(`^[a-z0-9.+/-]{1,128}$`)

// Digest is the 64-character lowercase hex sha256 of an object's stored bytes.
type Digest string

// Sum returns the Digest of b.
func Sum(b []byte) Digest {
	sum := sha256.Sum256(b)
	return Digest(hex.EncodeToString(sum[:]))
}

// Validate requires exactly 64 lowercase hex characters.
func (d Digest) Validate() error {
	if len(d) != sha256.Size*2 {
		return fmt.Errorf("%w: digest %q is not 64 lowercase hex characters", ErrInvalid, string(d))
	}
	for _, c := range []byte(d) {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf("%w: digest %q is not 64 lowercase hex characters", ErrInvalid, string(d))
		}
	}
	return nil
}

// Kind distinguishes a raw chunk from a manifest.
type Kind string

const (
	// KindBlob is raw content bytes, 1 to ChunkSize long.
	KindBlob Kind = "blob"
	// KindManifest is a canonical Manifest encoding.
	KindManifest Kind = "manifest"
)

// Ref names one object as a consumer holds it. Size is a blob's byte length
// or a manifest's logical content size (Manifest.Size), not its encoding.
type Ref struct {
	Digest Digest `json:"digest"`
	Kind   Kind   `json:"kind"`
	Size   int64  `json:"size"`
}

// Validate checks the digest, the kind, and the size range the kind allows.
func (r Ref) Validate() error {
	if err := r.Digest.Validate(); err != nil {
		return err
	}
	switch r.Kind {
	case KindBlob:
		if r.Size <= 0 || r.Size > ChunkSize {
			return fmt.Errorf("%w: blob %s size %d is outside 1..%d", ErrInvalid, r.Digest, r.Size, ChunkSize)
		}
	case KindManifest:
		if r.Size < 0 || r.Size > maxWireInt {
			return fmt.Errorf("%w: manifest %s size %d is outside 0..%d", ErrInvalid, r.Digest, r.Size, int64(maxWireInt))
		}
	default:
		return fmt.Errorf("%w: ref %s kind %q", ErrInvalid, r.Digest, string(r.Kind))
	}
	return nil
}

// ValidateRoots checks a root list in consumer priority order: at most
// MaxRoots valid refs with unique digests.
func ValidateRoots(roots []Ref) error {
	if len(roots) > MaxRoots {
		return fmt.Errorf("%w: %d roots exceed %d", ErrInvalid, len(roots), MaxRoots)
	}
	return validateRefs(roots, "root")
}

func validateRefs(refs []Ref, label string) error {
	seen := make(map[Digest]struct{}, len(refs))
	for _, ref := range refs {
		if err := ref.Validate(); err != nil {
			return err
		}
		if _, dup := seen[ref.Digest]; dup {
			return fmt.Errorf("%w: duplicate %s %s", ErrInvalid, label, ref.Digest)
		}
		seen[ref.Digest] = struct{}{}
	}
	return nil
}

// ChunkRef is one blob of a manifest's content, in content order.
type ChunkRef struct {
	Digest Digest `json:"digest"`
	Size   int64  `json:"size"`
}

// Manifest groups content chunks at fixed ChunkSize offsets and requires the
// presence of its Deps. Deps are unique, at most MaxDeps, and ordered by
// consumer priority; a manifest without chunks is a pure group.
type Manifest struct {
	Schema string     `json:"schema"`
	Media  string     `json:"media"`
	Size   int64      `json:"size"`
	Chunks []ChunkRef `json:"chunks"`
	Deps   []Ref      `json:"deps,omitempty"`
}

// Validate strictly checks the schema, the media label, every chunk's fixed
// offset size, the content size, and the dependency list.
func (m Manifest) Validate() error {
	if m.Schema != ManifestSchema {
		return fmt.Errorf("%w: manifest schema %q", ErrInvalid, m.Schema)
	}
	if !mediaPattern.MatchString(m.Media) {
		return fmt.Errorf("%w: manifest media %q", ErrInvalid, m.Media)
	}
	var size int64
	for i, chunk := range m.Chunks {
		if err := chunk.Digest.Validate(); err != nil {
			return err
		}
		if chunk.Size <= 0 || chunk.Size > ChunkSize {
			return fmt.Errorf("%w: chunk %d size %d is outside 1..%d", ErrInvalid, i, chunk.Size, ChunkSize)
		}
		if i < len(m.Chunks)-1 && chunk.Size != ChunkSize {
			return fmt.Errorf("%w: non-final chunk %d is %d bytes, want %d", ErrInvalid, i, chunk.Size, ChunkSize)
		}
		size += chunk.Size
	}
	if size != m.Size {
		return fmt.Errorf("%w: manifest size %d, chunks sum to %d", ErrInvalid, m.Size, size)
	}
	if len(m.Deps) > MaxDeps {
		return fmt.Errorf("%w: %d deps exceed %d", ErrInvalid, len(m.Deps), MaxDeps)
	}
	return validateRefs(m.Deps, "dep")
}

// Encode validates m and returns its canonical encoding, whose Sum is the
// manifest's Digest.
func (m Manifest) Encode() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if m.Chunks == nil {
		m.Chunks = []ChunkRef{}
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("artifact: encode manifest: %w", err)
	}
	if len(encoded) > MaxManifestBytes {
		return nil, fmt.Errorf("%w: manifest encodes to %d bytes, over %d", ErrInvalid, len(encoded), MaxManifestBytes)
	}
	return encoded, nil
}

// DecodeManifest strictly decodes b: unknown fields are refused, the result
// must validate, and b must be exactly its canonical encoding.
func DecodeManifest(b []byte) (Manifest, error) {
	if len(b) > MaxManifestBytes {
		return Manifest{}, fmt.Errorf("%w: manifest is %d bytes, over %d", ErrInvalid, len(b), MaxManifestBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	var m Manifest
	if err := decoder.Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("%w: decode manifest: %w", ErrInvalid, err)
	}
	canonical, err := m.Encode()
	if err != nil {
		return Manifest{}, err
	}
	if !bytes.Equal(canonical, b) {
		return Manifest{}, fmt.Errorf("%w: manifest encoding is not canonical", ErrInvalid)
	}
	return m, nil
}

// ObjectEntry names one stored object. Size is its stored byte length: a
// blob's content or a manifest's canonical encoding.
type ObjectEntry struct {
	Digest Digest `json:"digest"`
	Kind   Kind   `json:"kind"`
	Size   int64  `json:"size"`
}

// Validate checks the digest and the stored size range the kind allows.
func (o ObjectEntry) Validate() error {
	if err := o.Digest.Validate(); err != nil {
		return err
	}
	limit := int64(ChunkSize)
	switch o.Kind {
	case KindBlob:
	case KindManifest:
		limit = MaxManifestBytes
	default:
		return fmt.Errorf("%w: object %s kind %q", ErrInvalid, o.Digest, string(o.Kind))
	}
	if o.Size <= 0 || o.Size > limit {
		return fmt.Errorf("%w: %s %s size %d is outside 1..%d", ErrInvalid, o.Kind, o.Digest, o.Size, limit)
	}
	return nil
}

// ClosureBound caps one closure walk.
type ClosureBound struct {
	MaxObjects int
	MaxDepth   int
	MaxBytes   int64
}

// Closure is every object reachable from a root list: a depth-first walk in
// root priority order, children before their manifest, deduplicated and
// deterministic. Bytes sums the stored sizes.
type Closure struct {
	Objects []ObjectEntry
	Bytes   int64
}

// ClosureError reports a closure walk that exceeded one ClosureBound limit;
// Bound is BoundObjects, BoundDepth, or BoundBytes.
type ClosureError struct {
	Bound string
	Limit int64
}

func (e *ClosureError) Error() string {
	return fmt.Sprintf("artifact: closure exceeds the %s bound %d", e.Bound, e.Limit)
}

// MissingError reports an object the local store does not hold.
type MissingError struct {
	Digest Digest
}

func (e *MissingError) Error() string {
	return fmt.Sprintf("artifact: object %s is missing", e.Digest)
}

// PartRef is one compressed part of a batch stream: its sha256 and length.
type PartRef struct {
	Digest Digest `json:"digest"`
	Size   int64  `json:"size"`
}

// BatchDescriptor describes one resumable transfer batch: the objects in pack
// order, the exact pack stream length, and the zstd stream split into parts
// at PartSize offsets. ID is the Digest of the descriptor's JSON encoding
// with an empty ID, so identical inputs always name the same batch.
type BatchDescriptor struct {
	ID      Digest        `json:"id"`
	Schema  string        `json:"schema"`
	Codec   string        `json:"codec"`
	RawSize int64         `json:"raw_size"`
	Objects []ObjectEntry `json:"objects"`
	Parts   []PartRef     `json:"parts"`
}

// NewBatchDescriptor stamps the schema, codec, and ID onto a batch of objects
// whose pack stream is rawSize bytes and compresses into parts, then
// validates it.
func NewBatchDescriptor(rawSize int64, objects []ObjectEntry, parts []PartRef) (BatchDescriptor, error) {
	d := BatchDescriptor{Schema: BatchSchema, Codec: BatchCodec, RawSize: rawSize, Objects: objects, Parts: parts}
	id, err := d.computeID()
	if err != nil {
		return BatchDescriptor{}, err
	}
	d.ID = id
	return d, d.Validate()
}

// Validate checks the schema and codec, the object list and its bounds, that
// RawSize is the exact pack stream length of Objects, the part split, and
// that ID matches the descriptor's content.
func (d BatchDescriptor) Validate() error {
	if d.Schema != BatchSchema || d.Codec != BatchCodec {
		return fmt.Errorf("%w: batch schema %q codec %q", ErrInvalid, d.Schema, d.Codec)
	}
	if len(d.Objects) == 0 || len(d.Objects) > MaxBatchObjects {
		return fmt.Errorf("%w: batch holds %d objects, want 1..%d", ErrInvalid, len(d.Objects), MaxBatchObjects)
	}
	seen := make(map[Digest]struct{}, len(d.Objects))
	var raw int64
	for _, object := range d.Objects {
		if err := object.Validate(); err != nil {
			return err
		}
		if _, dup := seen[object.Digest]; dup {
			return fmt.Errorf("%w: duplicate batch object %s", ErrInvalid, object.Digest)
		}
		seen[object.Digest] = struct{}{}
		raw += object.Size
	}
	if raw > MaxBatchRaw {
		return fmt.Errorf("%w: batch holds %d object bytes, over %d", ErrInvalid, raw, MaxBatchRaw)
	}
	if want := packSize(d.Objects); d.RawSize != want {
		return fmt.Errorf("%w: batch raw size %d, pack stream is %d", ErrInvalid, d.RawSize, want)
	}
	if len(d.Parts) == 0 || len(d.Parts) > maxBatchParts {
		return fmt.Errorf("%w: batch has %d parts, want 1..%d", ErrInvalid, len(d.Parts), maxBatchParts)
	}
	for i, part := range d.Parts {
		if err := part.Digest.Validate(); err != nil {
			return err
		}
		if part.Size <= 0 || part.Size > PartSize {
			return fmt.Errorf("%w: part %d size %d is outside 1..%d", ErrInvalid, i, part.Size, PartSize)
		}
		if i < len(d.Parts)-1 && part.Size != PartSize {
			return fmt.Errorf("%w: non-final part %d is %d bytes, want %d", ErrInvalid, i, part.Size, PartSize)
		}
	}
	id, err := d.computeID()
	if err != nil {
		return err
	}
	if d.ID != id {
		return fmt.Errorf("%w: batch id %s, content hashes to %s", ErrInvalid, d.ID, id)
	}
	return nil
}

func (d BatchDescriptor) computeID() (Digest, error) {
	d.ID = ""
	encoded, err := json.Marshal(d)
	if err != nil {
		return "", fmt.Errorf("artifact: encode batch descriptor: %w", err)
	}
	return Sum(encoded), nil
}

func packSize(objects []ObjectEntry) int64 {
	size := int64(len(packMagic)) + 1
	for _, object := range objects {
		size += 1 + sha256.Size + uvarintLen(object.Size) + object.Size
	}
	return size
}

func uvarintLen(v int64) int64 {
	n := int64(1)
	for ; v >= 0x80; v >>= 7 {
		n++
	}
	return n
}

// CommitReport counts one committed batch: objects newly stored, objects
// already present, and the bytes newly stored.
type CommitReport struct {
	Stored  int   `json:"stored"`
	Present int   `json:"present"`
	Bytes   int64 `json:"bytes"`
}

// PinSet is one owner's pinned roots; GC keeps the closure of every pin set.
type PinSet struct {
	Owner     string    `json:"owner"`
	Roots     []Ref     `json:"roots"`
	UpdatedAt time.Time `json:"updated_at"`
}

// GCReport counts one GC pass: objects marked reachable, unreachable objects
// removed and their bytes, and expired staging batches removed.
type GCReport struct {
	Marked         int   `json:"marked"`
	Removed        int   `json:"removed"`
	FreedBytes     int64 `json:"freed_bytes"`
	StagingRemoved int   `json:"staging_removed"`
}
