package artifact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/yasyf/daemonkit/durable"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/serviceidentity"
)

const (
	lockName       = "store.lock"
	objectsDir     = "objects"
	pinsDir        = "pins"
	outboxDir      = "outbox"
	incomingDir    = "incoming"
	descriptorName = "descriptor.json"
	dirPerm        = 0o700
	filePerm       = 0o600
	openLockWait   = time.Second
	closureMemos   = 2
)

var manifestPrefix = []byte(`{"schema":"` + ManifestSchema + `",`)

// ServiceRoot returns the store directory serviceID's resident consumer
// exclusively owns: <hostregistry.Mesh dir>/artifacts/v1/<serviceID>.
func ServiceRoot(serviceID string) (string, error) {
	if err := serviceidentity.ValidateName(serviceID); err != nil {
		return "", fmt.Errorf("artifact: service root: %w", err)
	}
	dir, err := hostregistry.Mesh.Dir()
	if err != nil {
		return "", fmt.Errorf("artifact: service root: %w", err)
	}
	return filepath.Join(dir, "artifacts", "v1", serviceID), nil
}

// Reader reads a store without writing, locking, or sweeping it. Every read
// re-hashes the objects it returns. Another process of the owning user reads
// through OpenReadOnly after pinning its roots via the owning process, so GC
// cannot remove them mid-read.
type Reader struct {
	root   string
	memoMu sync.Mutex
	memo   []closureMemo
}

type closureMemo struct {
	key     Digest
	closure Closure
}

// OpenReadOnly opens an existing store at root for reading only.
func OpenReadOnly(root string) (*Reader, error) {
	if _, err := os.Stat(filepath.Join(root, objectsDir)); err != nil {
		return nil, fmt.Errorf("artifact: open %s read-only: %w", root, err)
	}
	return &Reader{root: root}, nil
}

// Store is one process's exclusive handle on a content-addressed object
// store. It holds store.lock until Close. An object is present only once its
// bytes are durable, and a manifest is present only once every chunk and dep
// it names is present, so the store is always closed under its manifests.
type Store struct {
	*Reader
	lock       *durable.Lock
	gcMu       sync.RWMutex
	incomingMu sync.Mutex
	encMu      sync.Mutex
	encoder    *zstd.Encoder
}

// Open creates the store layout at root when absent and takes store.lock,
// failing with durable.ErrLockBusy when another handle holds it.
func Open(root string) (*Store, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("artifact: open %s: %w", root, err)
	}
	if err := mkdirAll(root); err != nil {
		return nil, fmt.Errorf("artifact: create %s: %w", root, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), openLockWait)
	defer cancel()
	lock, err := durable.AcquireLock(ctx, filepath.Join(root, lockName))
	if err != nil {
		return nil, fmt.Errorf("artifact: lock %s: %w", root, err)
	}
	encoder, err := newEncoder()
	if err == nil {
		err = createLayout(root)
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("artifact: open %s: %w", root, err), lock.Close())
	}
	return &Store{Reader: &Reader{root: root}, lock: lock, encoder: encoder}, nil
}

// Close releases store.lock.
func (s *Store) Close() error {
	return s.lock.Close()
}

func createLayout(root string) error {
	for _, dir := range []string{objectsDir, pinsDir, outboxDir, incomingDir} {
		if err := mkdirAll(filepath.Join(root, dir)); err != nil {
			return err
		}
	}
	objects := filepath.Join(root, objectsDir)
	created := false
	for prefix := range 256 {
		err := os.Mkdir(filepath.Join(objects, fmt.Sprintf("%02x", prefix)), dirPerm)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return err
		}
		created = true
	}
	if !created {
		return nil
	}
	return durable.SyncDir(objects)
}

func mkdirAll(dir string) error {
	_, err := os.Stat(dir)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := mkdirAll(filepath.Dir(dir)); err != nil {
		return err
	}
	return durable.Mkdir(dir, dirPerm)
}

func readFile(path string) ([]byte, error) {
	return os.ReadFile(path) //nolint:gosec // G304: every path is under the store root and built from a validated digest or index.
}

func (r *Reader) objectPath(digest Digest) string {
	return filepath.Join(r.root, objectsDir, string(digest[:2]), string(digest))
}

func (r *Reader) present(digest Digest) (bool, error) {
	_, err := os.Stat(r.objectPath(digest))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("artifact: stat object %s: %w", digest, err)
	}
	return true, nil
}

func (r *Reader) readObject(digest Digest) ([]byte, error) {
	data, err := readFile(r.objectPath(digest))
	if errors.Is(err, os.ErrNotExist) {
		return nil, &MissingError{Digest: digest}
	}
	if err != nil {
		return nil, fmt.Errorf("artifact: read object %s: %w", digest, err)
	}
	if Sum(data) != digest {
		return nil, fmt.Errorf("%w: object %s is corrupt", ErrInvalid, digest)
	}
	return data, nil
}

func (r *Reader) readManifest(digest Digest, size int64) (Manifest, []byte, error) {
	data, err := r.readObject(digest)
	if err != nil {
		return Manifest{}, nil, err
	}
	m, err := DecodeManifest(data)
	if err != nil {
		return Manifest{}, nil, fmt.Errorf("artifact: manifest %s: %w", digest, err)
	}
	if size >= 0 && m.Size != size {
		return Manifest{}, nil, fmt.Errorf("%w: manifest %s holds %d bytes, ref says %d", ErrInvalid, digest, m.Size, size)
	}
	return m, data, nil
}

func (s *Store) writeObject(digest Digest, data []byte) (bool, error) {
	path := s.objectPath(digest)
	_, err := os.Stat(path)
	if err == nil {
		now := time.Now()
		if err := os.Chtimes(path, now, now); err != nil {
			return false, fmt.Errorf("artifact: touch object %s: %w", digest, err)
		}
		return false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("artifact: stat object %s: %w", digest, err)
	}
	if err := durable.WriteFile(path, data, filePerm); err != nil {
		return false, fmt.Errorf("artifact: write object %s: %w", digest, err)
	}
	return true, nil
}

func (s *Store) putManifest(m Manifest) (Ref, error) {
	encoded, err := m.Encode()
	if err != nil {
		return Ref{}, err
	}
	digest := Sum(encoded)
	if _, err := s.writeObject(digest, encoded); err != nil {
		return Ref{}, err
	}
	return Ref{Digest: digest, Kind: KindManifest, Size: m.Size}, nil
}

// Put stores r's content split at fixed ChunkSize offsets under a manifest
// labeled media and returns the manifest's Ref. Only absent chunks are
// written; present ones are touched so GC keeps them for GCGrace.
func (s *Store) Put(ctx context.Context, r io.Reader, media string) (Ref, error) {
	if !mediaPattern.MatchString(media) {
		return Ref{}, fmt.Errorf("%w: manifest media %q", ErrInvalid, media)
	}
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	m := Manifest{Schema: ManifestSchema, Media: media, Chunks: []ChunkRef{}}
	buf := make([]byte, ChunkSize)
	for {
		if err := ctx.Err(); err != nil {
			return Ref{}, err
		}
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			digest := Sum(buf[:n])
			if _, err := s.writeObject(digest, buf[:n]); err != nil {
				return Ref{}, err
			}
			m.Chunks = append(m.Chunks, ChunkRef{Digest: digest, Size: int64(n)})
			m.Size += int64(n)
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return Ref{}, fmt.Errorf("artifact: read content: %w", readErr)
		}
	}
	return s.putManifest(m)
}

// PutBlob stores b, 1 to ChunkSize bytes, as one blob.
func (s *Store) PutBlob(ctx context.Context, b []byte) (Ref, error) {
	if len(b) == 0 || len(b) > ChunkSize {
		return Ref{}, fmt.Errorf("%w: blob of %d bytes, want 1..%d", ErrInvalid, len(b), ChunkSize)
	}
	if err := ctx.Err(); err != nil {
		return Ref{}, err
	}
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	digest := Sum(b)
	if _, err := s.writeObject(digest, b); err != nil {
		return Ref{}, err
	}
	return Ref{Digest: digest, Kind: KindBlob, Size: int64(len(b))}, nil
}

// PutGroup stores a chunkless manifest labeled media that depends on deps,
// refusing with a MissingError when any dep is absent.
func (s *Store) PutGroup(ctx context.Context, media string, deps []Ref) (Ref, error) {
	m := Manifest{Schema: ManifestSchema, Media: media, Chunks: []ChunkRef{}, Deps: deps}
	if err := m.Validate(); err != nil {
		return Ref{}, err
	}
	if err := ctx.Err(); err != nil {
		return Ref{}, err
	}
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	for _, dep := range deps {
		present, err := s.present(dep.Digest)
		if err != nil {
			return Ref{}, err
		}
		if !present {
			return Ref{}, &MissingError{Digest: dep.Digest}
		}
	}
	return s.putManifest(m)
}

// Manifest reads and strictly decodes the manifest ref names, requiring its
// logical size to match ref.
func (r *Reader) Manifest(ctx context.Context, ref Ref) (Manifest, error) {
	if err := ref.Validate(); err != nil {
		return Manifest{}, err
	}
	if ref.Kind != KindManifest {
		return Manifest{}, fmt.Errorf("%w: ref %s is a %s, not a manifest", ErrInvalid, ref.Digest, ref.Kind)
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	m, _, err := r.readManifest(ref.Digest, ref.Size)
	return m, err
}

// Open streams the content ref names: a blob's bytes or a manifest's chunks
// in order, re-hashing each chunk before any of its bytes are returned.
func (r *Reader) Open(ctx context.Context, ref Ref) (io.ReadCloser, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	if ref.Kind == KindBlob {
		return r.openChunks(ctx, []ChunkRef{{Digest: ref.Digest, Size: ref.Size}}), nil
	}
	m, _, err := r.readManifest(ref.Digest, ref.Size)
	if err != nil {
		return nil, err
	}
	return r.openChunks(ctx, m.Chunks), nil
}

func (r *Reader) openChunks(ctx context.Context, chunks []ChunkRef) *contentReader {
	return &contentReader{ctx: ctx, reader: r, chunks: chunks}
}

type contentReader struct {
	ctx    context.Context
	reader *Reader
	chunks []ChunkRef
	buf    []byte
}

func (c *contentReader) Read(p []byte) (int, error) {
	for len(c.buf) == 0 {
		if len(c.chunks) == 0 {
			return 0, io.EOF
		}
		if err := c.ctx.Err(); err != nil {
			return 0, err
		}
		chunk := c.chunks[0]
		data, err := c.reader.readObject(chunk.Digest)
		if err != nil {
			return 0, err
		}
		if int64(len(data)) != chunk.Size {
			return 0, fmt.Errorf("%w: blob %s holds %d bytes, ref says %d", ErrInvalid, chunk.Digest, len(data), chunk.Size)
		}
		c.chunks, c.buf = c.chunks[1:], data
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

func (c *contentReader) Close() error {
	return nil
}

// Materialize durably writes the verified content ref names to path with
// perm; nothing appears at path unless every chunk verifies.
func (r *Reader) Materialize(ctx context.Context, ref Ref, path string, perm os.FileMode) (err error) {
	content, err := r.Open(ctx, ref)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, content.Close()) }()
	w, err := durable.Create(path, perm)
	if err != nil {
		return fmt.Errorf("artifact: materialize %s: %w", path, err)
	}
	defer func() { err = errors.Join(err, w.Close()) }()
	if _, err := io.Copy(w, content); err != nil {
		return fmt.Errorf("artifact: materialize %s: %w", path, err)
	}
	return w.Commit()
}

// Has returns the digests the store lacks, in query order.
func (r *Reader) Has(ctx context.Context, digests []Digest) ([]Digest, error) {
	missing := []Digest{}
	for _, digest := range digests {
		if err := digest.Validate(); err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		present, err := r.present(digest)
		if err != nil {
			return nil, err
		}
		if !present {
			missing = append(missing, digest)
		}
	}
	return missing, nil
}

// Closure walks roots in priority order, depth first with every child before
// its manifest and each manifest's chunks before its deps, and fails with a
// MissingError on an absent manifest or a ClosureError past bound.
func (r *Reader) Closure(ctx context.Context, roots []Ref, bound ClosureBound) (Closure, error) {
	closure, err := r.closure(ctx, roots, bound)
	if err != nil {
		return Closure{}, err
	}
	return Closure{Objects: slices.Clone(closure.Objects), Bytes: closure.Bytes}, nil
}

func (r *Reader) closure(ctx context.Context, roots []Ref, bound ClosureBound) (Closure, error) {
	if err := ValidateRoots(roots); err != nil {
		return Closure{}, err
	}
	encoded, err := json.Marshal(struct {
		Roots []Ref
		Bound ClosureBound
	}{roots, bound})
	if err != nil {
		return Closure{}, fmt.Errorf("artifact: encode closure key: %w", err)
	}
	key := Sum(encoded)
	r.memoMu.Lock()
	for _, memo := range r.memo {
		if memo.key == key {
			r.memoMu.Unlock()
			return memo.closure, nil
		}
	}
	r.memoMu.Unlock()
	w := newWalker(r, bound, false)
	if err := w.walk(ctx, roots); err != nil {
		return Closure{}, err
	}
	closure := Closure{Objects: w.objects, Bytes: w.bytes}
	r.memoMu.Lock()
	r.memo = append([]closureMemo{{key: key, closure: closure}}, r.memo[:min(len(r.memo), closureMemos-1)]...)
	r.memoMu.Unlock()
	return closure, nil
}

// Complete counts the objects of roots' closure the store lacks, checking
// every present blob's size and every present manifest's content. A missing
// manifest counts once; its children are not walked.
func (r *Reader) Complete(ctx context.Context, roots []Ref) (int, error) {
	if err := ValidateRoots(roots); err != nil {
		return 0, err
	}
	w := newWalker(r, DefaultClosureBound, true)
	if err := w.walk(ctx, roots); err != nil {
		return 0, err
	}
	return w.missing, nil
}

// Verify re-hashes every object of roots' closure, failing with a
// MissingError on an absent object or ErrInvalid on a corrupt one.
func (r *Reader) Verify(ctx context.Context, roots []Ref) error {
	closure, err := r.closure(ctx, roots, DefaultClosureBound)
	if err != nil {
		return err
	}
	for _, object := range closure.Objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := r.readObject(object.Digest)
		if err != nil {
			return err
		}
		if int64(len(data)) != object.Size {
			return fmt.Errorf("%w: %s %s holds %d bytes, want %d", ErrInvalid, object.Kind, object.Digest, len(data), object.Size)
		}
	}
	return nil
}

type walker struct {
	reader  *Reader
	bound   ClosureBound
	audit   bool
	seen    map[Digest]struct{}
	objects []ObjectEntry
	bytes   int64
	missing int
}

func newWalker(r *Reader, bound ClosureBound, audit bool) *walker {
	return &walker{reader: r, bound: bound, audit: audit, seen: map[Digest]struct{}{}, objects: []ObjectEntry{}}
}

func (w *walker) walk(ctx context.Context, roots []Ref) error {
	for _, root := range roots {
		if err := w.visit(ctx, root, 1); err != nil {
			return err
		}
	}
	return nil
}

func (w *walker) visit(ctx context.Context, ref Ref, depth int) error {
	if _, seen := w.seen[ref.Digest]; seen {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if ref.Kind == KindBlob {
		return w.blob(ref)
	}
	if depth > w.bound.MaxDepth {
		return &ClosureError{Bound: BoundDepth, Limit: int64(w.bound.MaxDepth)}
	}
	m, encoded, err := w.reader.readManifest(ref.Digest, ref.Size)
	var missing *MissingError
	if w.audit && errors.As(err, &missing) {
		w.seen[ref.Digest] = struct{}{}
		w.missing++
		return nil
	}
	if err != nil {
		return err
	}
	for _, chunk := range m.Chunks {
		if err := w.visit(ctx, Ref{Digest: chunk.Digest, Kind: KindBlob, Size: chunk.Size}, depth); err != nil {
			return err
		}
	}
	for _, dep := range m.Deps {
		if err := w.visit(ctx, dep, depth+1); err != nil {
			return err
		}
	}
	return w.emit(ObjectEntry{Digest: ref.Digest, Kind: KindManifest, Size: int64(len(encoded))})
}

func (w *walker) blob(ref Ref) error {
	if w.audit {
		info, err := os.Stat(w.reader.objectPath(ref.Digest))
		if errors.Is(err, os.ErrNotExist) {
			w.seen[ref.Digest] = struct{}{}
			w.missing++
			return nil
		}
		if err != nil {
			return fmt.Errorf("artifact: stat object %s: %w", ref.Digest, err)
		}
		if info.Size() != ref.Size {
			return fmt.Errorf("%w: blob %s holds %d bytes, ref says %d", ErrInvalid, ref.Digest, info.Size(), ref.Size)
		}
	}
	return w.emit(ObjectEntry{Digest: ref.Digest, Kind: KindBlob, Size: ref.Size})
}

func (w *walker) emit(entry ObjectEntry) error {
	if len(w.objects) >= w.bound.MaxObjects {
		return &ClosureError{Bound: BoundObjects, Limit: int64(w.bound.MaxObjects)}
	}
	if w.bytes+entry.Size > w.bound.MaxBytes {
		return &ClosureError{Bound: BoundBytes, Limit: w.bound.MaxBytes}
	}
	w.seen[entry.Digest] = struct{}{}
	w.objects = append(w.objects, entry)
	w.bytes += entry.Size
	return nil
}

func sniffKind(data []byte) Kind {
	if bytes.HasPrefix(data, manifestPrefix) {
		if _, err := DecodeManifest(data); err == nil {
			return KindManifest
		}
	}
	return KindBlob
}
