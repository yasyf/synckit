package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	committedDir   = "committed"
	descriptorName = "descriptor.json"
	landingDir     = "objects"
	dirPerm        = 0o700
	filePerm       = 0o600
	openLockWait   = time.Second
	closureMemos   = 2
)

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
	root       string
	memoMu     sync.Mutex
	memo       []closureMemo
	publishMu  sync.Mutex
	publishing map[Digest]chan struct{}
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
	return &Store{Reader: &Reader{root: root, publishing: map[Digest]chan struct{}{}}, lock: lock, encoder: encoder}, nil
}

// Close releases store.lock.
func (s *Store) Close() error {
	return s.lock.Close()
}

func createLayout(root string) error {
	for _, dir := range []string{objectsDir, pinsDir, outboxDir, incomingDir, committedDir} {
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

func (r *Reader) inFlight(digest Digest) bool {
	r.publishMu.Lock()
	defer r.publishMu.Unlock()
	_, publishing := r.publishing[digest]
	return publishing
}

func (r *Reader) stat(digest Digest) (fs.FileInfo, error) {
	path := r.objectPath(digest)
	if r.inFlight(digest) {
		return nil, &fs.PathError{Op: "stat", Path: path, Err: fs.ErrNotExist}
	}
	return os.Stat(path)
}

func (r *Reader) present(digest Digest) (bool, error) {
	_, err := r.stat(digest)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("artifact: stat object %s: %w", digest, err)
	}
	return true, nil
}

func (r *Reader) readObject(digest Digest) ([]byte, error) {
	if r.inFlight(digest) {
		return nil, &MissingError{Digest: digest}
	}
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
	if err := checkManifestSize(digest, m, size); err != nil {
		return Manifest{}, nil, err
	}
	return m, data, nil
}

func checkManifestSize(digest Digest, m Manifest, size int64) error {
	if size >= 0 && m.Size != size {
		return fmt.Errorf("%w: manifest %s holds %d bytes, ref says %d", ErrInvalid, digest, m.Size, size)
	}
	return nil
}

func (s *Store) touch(digest Digest) (bool, error) {
	path := s.objectPath(digest)
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("artifact: stat object %s: %w", digest, err)
	}
	now := time.Now()
	if err := os.Chtimes(path, now, now); err != nil {
		return false, fmt.Errorf("artifact: touch object %s: %w", digest, err)
	}
	return true, nil
}

func (s *Store) claim(digest Digest) func() {
	for {
		s.publishMu.Lock()
		wait, publishing := s.publishing[digest]
		if !publishing {
			done := make(chan struct{})
			s.publishing[digest] = done
			s.publishMu.Unlock()
			return func() {
				s.publishMu.Lock()
				delete(s.publishing, digest)
				s.publishMu.Unlock()
				close(done)
			}
		}
		s.publishMu.Unlock()
		<-wait
	}
}

func (s *Store) writeObject(digest Digest, data []byte) (bool, error) {
	release := s.claim(digest)
	defer release()
	present, err := s.touch(digest)
	if err != nil || present {
		return false, err
	}
	if err := durable.WriteFile(s.objectPath(digest), data, filePerm); err != nil {
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
// refusing with a MissingError when any dep is absent. More than MaxDeps
// deps split deterministically, in order, into a tree of GroupMedia
// manifests of at most MaxDeps deps each beneath the returned root.
func (s *Store) PutGroup(ctx context.Context, media string, deps []Ref) (Ref, error) {
	if !mediaPattern.MatchString(media) {
		return Ref{}, fmt.Errorf("%w: manifest media %q", ErrInvalid, media)
	}
	if err := validateRefs(deps, "dep"); err != nil {
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
	for len(deps) > MaxDeps {
		level := make([]Ref, 0, (len(deps)+MaxDeps-1)/MaxDeps)
		for group := range slices.Chunk(deps, MaxDeps) {
			ref, err := s.putManifest(Manifest{Schema: ManifestSchema, Media: GroupMedia, Chunks: []ChunkRef{}, Deps: group})
			if err != nil {
				return Ref{}, err
			}
			level = append(level, ref)
		}
		deps = level
	}
	return s.putManifest(Manifest{Schema: ManifestSchema, Media: media, Chunks: []ChunkRef{}, Deps: deps})
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
// its manifest and each manifest's chunks before its deps, listing each
// digest once and as a manifest whenever any root reaches it as one. It fails
// with a MissingError on an absent manifest or a ClosureError when any one
// root's complete closure passes bound: the bound applies per root, not to the
// union.
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
	closure := w.order(roots)
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
	return len(w.missing), nil
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

type reach uint8

const (
	reachedBlob reach = 1 << iota
	reachedManifest
)

func reachOf(kind Kind) reach {
	if kind == KindManifest {
		return reachedManifest
	}
	return reachedBlob
}

type walker struct {
	reader       *Reader
	bound        ClosureBound
	audit        bool
	manifests    map[Digest]Manifest
	stored       map[Digest]int64
	missing      map[Digest]struct{}
	scope        map[Digest]reach
	scopeObjects int
	scopeBytes   int64
}

func newWalker(r *Reader, bound ClosureBound, audit bool) *walker {
	return &walker{
		reader: r, bound: bound, audit: audit,
		manifests: map[Digest]Manifest{}, stored: map[Digest]int64{}, missing: map[Digest]struct{}{},
		scope: map[Digest]reach{},
	}
}

func (w *walker) walk(ctx context.Context, roots []Ref) error {
	for _, root := range roots {
		w.scope, w.scopeObjects, w.scopeBytes = map[Digest]reach{}, 0, 0
		if err := w.visit(ctx, root, 1); err != nil {
			return err
		}
	}
	return nil
}

func (w *walker) visit(ctx context.Context, ref Ref, depth int) error {
	reached := w.scope[ref.Digest]
	if reached&reachOf(ref.Kind) != 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w.scope[ref.Digest] = reached | reachOf(ref.Kind)
	if _, absent := w.missing[ref.Digest]; absent {
		return nil
	}
	if ref.Kind == KindBlob {
		return w.blob(ref, reached == 0)
	}
	if depth > w.bound.MaxDepth {
		return &ClosureError{Bound: BoundDepth, Limit: int64(w.bound.MaxDepth)}
	}
	m, err := w.manifest(ref)
	var missing *MissingError
	if w.audit && errors.As(err, &missing) {
		w.missing[ref.Digest] = struct{}{}
		return nil
	}
	if err != nil {
		return err
	}
	if reached == 0 {
		if err := w.count(w.stored[ref.Digest]); err != nil {
			return err
		}
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
	return nil
}

func (w *walker) manifest(ref Ref) (Manifest, error) {
	m, read := w.manifests[ref.Digest]
	if !read {
		decoded, encoded, err := w.reader.readManifest(ref.Digest, -1)
		if err != nil {
			return Manifest{}, err
		}
		m = decoded
		w.manifests[ref.Digest] = m
		w.stored[ref.Digest] = int64(len(encoded))
	}
	return m, checkManifestSize(ref.Digest, m, ref.Size)
}

func (w *walker) blob(ref Ref, first bool) error {
	size, known := w.stored[ref.Digest]
	if !known {
		size = ref.Size
		if w.audit {
			info, err := w.reader.stat(ref.Digest)
			if errors.Is(err, os.ErrNotExist) {
				w.missing[ref.Digest] = struct{}{}
				return nil
			}
			if err != nil {
				return fmt.Errorf("artifact: stat object %s: %w", ref.Digest, err)
			}
			size = info.Size()
		}
		w.stored[ref.Digest] = size
	}
	if w.audit && size != ref.Size {
		return fmt.Errorf("%w: blob %s holds %d bytes, ref says %d", ErrInvalid, ref.Digest, size, ref.Size)
	}
	if !first {
		return nil
	}
	return w.count(size)
}

func (w *walker) count(size int64) error {
	if w.scopeObjects >= w.bound.MaxObjects {
		return &ClosureError{Bound: BoundObjects, Limit: int64(w.bound.MaxObjects)}
	}
	if w.scopeBytes+size > w.bound.MaxBytes {
		return &ClosureError{Bound: BoundBytes, Limit: w.bound.MaxBytes}
	}
	w.scopeObjects++
	w.scopeBytes += size
	return nil
}

func (w *walker) order(roots []Ref) Closure {
	closure := Closure{Objects: []ObjectEntry{}}
	listed := map[Digest]struct{}{}
	var list func(Digest)
	list = func(digest Digest) {
		if _, done := listed[digest]; done {
			return
		}
		listed[digest] = struct{}{}
		kind := KindBlob
		if m, ok := w.manifests[digest]; ok {
			kind = KindManifest
			for _, child := range m.children() {
				list(child)
			}
		}
		size := w.stored[digest]
		closure.Objects = append(closure.Objects, ObjectEntry{Digest: digest, Kind: kind, Size: size})
		closure.Bytes += size
	}
	for _, root := range roots {
		list(root.Digest)
	}
	return closure
}
