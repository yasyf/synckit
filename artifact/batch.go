package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/durable"
)

// ErrBatchFull marks a batch build past MaxBatchObjects objects or
// MaxBatchRaw object bytes.
var ErrBatchFull = errors.New("artifact: batch full")

var errIncomingFull = fmt.Errorf("artifact: %d incoming batches already staged", MaxIncoming)

var (
	flushBarrier  = fullSyncDir
	syncObjectDir = syncDir
)

func partName(index int) string {
	return fmt.Sprintf("%04d.part", index)
}

// BuildBatch packs objects, in order and as their declared kinds, into a
// zstd stream split into outbox parts and returns the batch's descriptor.
// Every object's stored bytes must match its declared size, and a declared
// manifest must decode strictly. Identical inputs name the same batch, and a
// rebuild of a present batch keeps its parts and refreshes its StagingTTL.
func (s *Store) BuildBatch(ctx context.Context, objects []ObjectEntry) (BatchDescriptor, error) {
	if len(objects) > MaxBatchObjects {
		return BatchDescriptor{}, fmt.Errorf("%w: %d objects exceed %d", ErrBatchFull, len(objects), MaxBatchObjects)
	}
	if err := (BatchBuildParams{Objects: objects}).Validate(); err != nil {
		return BatchDescriptor{}, err
	}
	var raw int64
	for _, object := range objects {
		raw += object.Size
	}
	if raw > MaxBatchRaw {
		return BatchDescriptor{}, fmt.Errorf("%w: %d object bytes exceed %d", ErrBatchFull, raw, MaxBatchRaw)
	}
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	s.encMu.Lock()
	defer s.encMu.Unlock()
	outbox := filepath.Join(s.root, outboxDir)
	tmp, err := os.MkdirTemp(outbox, ".build-")
	if err != nil {
		return BatchDescriptor{}, fmt.Errorf("artifact: stage batch build: %w", err)
	}
	d, err := s.pack(ctx, tmp, objects)
	if err == nil {
		err = publishStaged(tmp, filepath.Join(outbox, string(d.ID)), d)
	}
	if err != nil {
		return BatchDescriptor{}, errors.Join(err, durable.RemoveTree(tmp))
	}
	return d, nil
}

func (s *Store) pack(ctx context.Context, dir string, objects []ObjectEntry) (BatchDescriptor, error) {
	parts := &partWriter{dir: dir, buf: make([]byte, 0, PartSize)}
	s.encoder.Reset(parts)
	pack := &packWriter{w: s.encoder}
	if err := pack.begin(); err != nil {
		return BatchDescriptor{}, err
	}
	for _, entry := range objects {
		if err := ctx.Err(); err != nil {
			return BatchDescriptor{}, err
		}
		data, err := s.readObject(entry.Digest)
		if err != nil {
			return BatchDescriptor{}, err
		}
		if err := checkStored(entry, data); err != nil {
			return BatchDescriptor{}, err
		}
		if err := pack.object(entry, data); err != nil {
			return BatchDescriptor{}, err
		}
	}
	if err := pack.end(); err != nil {
		return BatchDescriptor{}, err
	}
	if err := s.encoder.Close(); err != nil {
		return BatchDescriptor{}, fmt.Errorf("artifact: finish batch stream: %w", err)
	}
	if err := parts.flush(); err != nil {
		return BatchDescriptor{}, err
	}
	return NewBatchDescriptor(pack.n, slices.Clone(objects), parts.parts)
}

func checkStored(entry ObjectEntry, data []byte) error {
	if int64(len(data)) != entry.Size {
		return fmt.Errorf("%w: %s %s holds %d bytes, want %d", ErrInvalid, entry.Kind, entry.Digest, len(data), entry.Size)
	}
	if entry.Kind != KindManifest {
		return nil
	}
	if _, err := DecodeManifest(data); err != nil {
		return fmt.Errorf("artifact: manifest %s: %w", entry.Digest, err)
	}
	return nil
}

func publishStaged(tmp, dir string, d BatchDescriptor) error {
	data, err := durable.Marshal(d)
	if err != nil {
		return fmt.Errorf("artifact: encode batch %s: %w", d.ID, err)
	}
	if err := durable.WriteFile(filepath.Join(tmp, descriptorName), data, filePerm); err != nil {
		return fmt.Errorf("artifact: write batch %s: %w", d.ID, err)
	}
	err = durable.Rename(tmp, dir)
	if errors.Is(err, os.ErrExist) {
		now := time.Now()
		return errors.Join(os.Chtimes(dir, now, now), durable.RemoveTree(tmp))
	}
	if err != nil {
		return fmt.Errorf("artifact: publish batch %s: %w", d.ID, err)
	}
	return nil
}

// ReadPart returns part index of outbox batch id.
func (s *Store) ReadPart(ctx context.Context, id Digest, index int) ([]byte, error) {
	if err := (BatchReadParams{ID: id, Index: index}).Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := readFile(filepath.Join(s.root, outboxDir, string(id), partName(index)))
	if err != nil {
		return nil, fmt.Errorf("artifact: read part %d of batch %s: %w", index, id, err)
	}
	return data, nil
}

// DropBatch removes outbox batch id; an absent batch is success.
func (s *Store) DropBatch(ctx context.Context, id Digest) error {
	if err := id.Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	if err := durable.RemoveTree(filepath.Join(s.root, outboxDir, string(id))); err != nil {
		return fmt.Errorf("artifact: drop batch %s: %w", id, err)
	}
	return nil
}

// BeginBatch stages d for receipt and returns the parts already held,
// ascending. Re-beginning a staged batch refreshes its StagingTTL and
// resumes it, re-beginning a batch committed within StagingTTL reports every
// part held, and a new batch is refused while MaxIncoming batches are staged.
func (s *Store) BeginBatch(ctx context.Context, d BatchDescriptor) ([]int, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	s.incomingMu.Lock()
	defer s.incomingMu.Unlock()
	incoming := filepath.Join(s.root, incomingDir)
	dir := filepath.Join(incoming, string(d.ID))
	_, err := os.Stat(dir)
	if err == nil {
		now := time.Now()
		if err := os.Chtimes(dir, now, now); err != nil {
			return nil, fmt.Errorf("artifact: touch batch %s: %w", d.ID, err)
		}
		return heldParts(dir, len(d.Parts))
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("artifact: stat batch %s: %w", d.ID, err)
	}
	if _, err := s.committedBatch(d.ID); err == nil {
		marker := s.committedPath(d.ID)
		now := time.Now()
		if err := os.Chtimes(marker, now, now); err != nil {
			return nil, fmt.Errorf("artifact: touch committed batch %s: %w", d.ID, err)
		}
		held := make([]int, len(d.Parts))
		for index := range held {
			held[index] = index
		}
		return held, nil
	}
	entries, err := os.ReadDir(incoming)
	if err != nil {
		return nil, fmt.Errorf("artifact: list incoming: %w", err)
	}
	staged := 0
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".") {
			staged++
		}
	}
	if staged >= MaxIncoming {
		return nil, errIncomingFull
	}
	tmp, err := os.MkdirTemp(incoming, ".begin-")
	if err != nil {
		return nil, fmt.Errorf("artifact: stage batch %s: %w", d.ID, err)
	}
	if err := publishStaged(tmp, dir, d); err != nil {
		return nil, errors.Join(err, durable.RemoveTree(tmp))
	}
	return []int{}, nil
}

func heldParts(dir string, parts int) ([]int, error) {
	held := []int{}
	for index := range parts {
		_, err := os.Stat(filepath.Join(dir, partName(index)))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("artifact: stat part %d: %w", index, err)
		}
		held = append(held, index)
	}
	return held, nil
}

func (s *Store) committedPath(id Digest) string {
	return filepath.Join(s.root, committedDir, string(id))
}

func (s *Store) committedBatch(id Digest) (BatchDescriptor, error) {
	return durable.ReadFile[BatchDescriptor](s.committedPath(id))
}

func (s *Store) stagedBatch(id Digest) (string, BatchDescriptor, error) {
	dir := filepath.Join(s.root, incomingDir, string(id))
	d, err := durable.ReadFile[BatchDescriptor](filepath.Join(dir, descriptorName))
	if err != nil {
		return "", BatchDescriptor{}, fmt.Errorf("artifact: batch %s is not staged: %w", id, err)
	}
	return dir, d, nil
}

// PutPart durably writes part index of staged batch id after checking its
// length and sha256 against the descriptor. A part of a batch another sender
// already committed is accepted without being written.
func (s *Store) PutPart(ctx context.Context, id Digest, index int, data []byte) error {
	if err := (BatchPutParams{ID: id, Index: index, Data: data}).Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	dir, d, err := s.stagedBatch(id)
	if err != nil {
		if _, committedErr := s.committedBatch(id); committedErr == nil {
			return nil
		}
		return err
	}
	if index >= len(d.Parts) {
		return fmt.Errorf("%w: batch %s has %d parts, not part %d", ErrInvalid, id, len(d.Parts), index)
	}
	if want := d.Parts[index]; int64(len(data)) != want.Size || Sum(data) != want.Digest {
		return fmt.Errorf("%w: part %d of batch %s does not match its descriptor", ErrInvalid, index, id)
	}
	if err := durable.WriteFile(filepath.Join(dir, partName(index)), data, filePerm); err != nil {
		if _, committedErr := s.committedBatch(id); committedErr == nil {
			return nil
		}
		return fmt.Errorf("artifact: write part %d of batch %s: %w", index, id, err)
	}
	return nil
}

// CommitBatch decodes staged batch id through the bounded decoder and
// verifies every object's kind, digest, and size, every manifest strictly,
// and that every manifest's children precede it, before storing anything.
// It then touches each present object and lands each absent one in the
// batch's staging with a plain fsync, flushes them all with one barrier,
// and only then renames them into place, children strictly before their
// manifests; a level whose directory fsync or barrier fails is renamed back
// into staging, so no object is visible before its publication is durable.
// It records the batch as committed for StagingTTL before removing the
// staging, and re-committing a recorded batch touches its objects and reports
// them present, so a concurrent sender of the same batch finishes too.
func (s *Store) CommitBatch(ctx context.Context, id Digest) (CommitReport, error) {
	if err := id.Validate(); err != nil {
		return CommitReport{}, err
	}
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	s.incomingMu.Lock()
	defer s.incomingMu.Unlock()
	dir, d, err := s.stagedBatch(id)
	if errors.Is(err, os.ErrNotExist) {
		return s.recommit(ctx, id, err)
	}
	if err != nil {
		return CommitReport{}, err
	}
	held, err := heldParts(dir, len(d.Parts))
	if err != nil {
		return CommitReport{}, err
	}
	if len(held) != len(d.Parts) {
		return CommitReport{}, fmt.Errorf("artifact: batch %s holds %d of %d parts", id, len(held), len(d.Parts))
	}
	batch := map[Digest]struct{}{}
	err = s.scanStaged(ctx, dir, d, func(entry ObjectEntry, data []byte) error {
		if entry.Kind == KindManifest {
			if err := s.requireChildren(entry.Digest, data, batch); err != nil {
				return err
			}
		}
		batch[entry.Digest] = struct{}{}
		return nil
	})
	if err != nil {
		return CommitReport{}, err
	}
	landing := filepath.Join(dir, landingDir)
	if err := durable.RemoveTree(landing); err != nil {
		return CommitReport{}, fmt.Errorf("artifact: clear landing of batch %s: %w", id, err)
	}
	if err := os.Mkdir(landing, dirPerm); err != nil {
		return CommitReport{}, fmt.Errorf("artifact: create landing of batch %s: %w", id, err)
	}
	var report CommitReport
	levels := map[Digest]int{}
	var landed []Digest
	err = s.scanStaged(ctx, dir, d, func(entry ObjectEntry, data []byte) error {
		present, err := s.touch(entry.Digest)
		if err != nil {
			return err
		}
		if present {
			report.Present++
			return nil
		}
		level, err := landingLevel(entry, data, levels)
		if err != nil {
			return err
		}
		if err := land(filepath.Join(landing, string(entry.Digest)), data); err != nil {
			return err
		}
		levels[entry.Digest] = level
		landed = append(landed, entry.Digest)
		report.Stored++
		report.Bytes += entry.Size
		return nil
	})
	if err != nil {
		return CommitReport{}, err
	}
	if err := s.publishLanded(landing, landed, levels); err != nil {
		return CommitReport{}, err
	}
	marker, err := durable.Marshal(d)
	if err != nil {
		return CommitReport{}, fmt.Errorf("artifact: encode batch %s: %w", id, err)
	}
	if err := durable.WriteFile(s.committedPath(id), marker, filePerm); err != nil {
		return CommitReport{}, fmt.Errorf("artifact: record committed batch %s: %w", id, err)
	}
	if err := durable.RemoveTree(dir); err != nil {
		return CommitReport{}, fmt.Errorf("artifact: remove staged batch %s: %w", id, err)
	}
	return report, nil
}

func (s *Store) recommit(ctx context.Context, id Digest, unstaged error) (CommitReport, error) {
	d, err := s.committedBatch(id)
	if errors.Is(err, os.ErrNotExist) {
		return CommitReport{}, unstaged
	}
	if err != nil {
		return CommitReport{}, fmt.Errorf("artifact: read committed batch %s: %w", id, err)
	}
	now := time.Now()
	for _, object := range d.Objects {
		if err := ctx.Err(); err != nil {
			return CommitReport{}, err
		}
		err := os.Chtimes(s.objectPath(object.Digest), now, now)
		if errors.Is(err, os.ErrNotExist) {
			return CommitReport{}, errors.Join(&MissingError{Digest: object.Digest}, durable.Remove(s.committedPath(id)))
		}
		if err != nil {
			return CommitReport{}, fmt.Errorf("artifact: touch object %s: %w", object.Digest, err)
		}
	}
	return CommitReport{Present: len(d.Objects)}, nil
}

func landingLevel(entry ObjectEntry, data []byte, levels map[Digest]int) (int, error) {
	if entry.Kind != KindManifest {
		return 0, nil
	}
	m, err := DecodeManifest(data)
	if err != nil {
		return 0, fmt.Errorf("artifact: manifest %s: %w", entry.Digest, err)
	}
	level := 0
	for _, child := range m.children() {
		if childLevel, landed := levels[child]; landed {
			level = max(level, childLevel+1)
		}
	}
	return level, nil
}

func land(path string, data []byte) (err error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePerm) //nolint:gosec // G304: path is under the batch staging, named by a validated digest.
	if err != nil {
		return fmt.Errorf("artifact: land %s: %w", filepath.Base(path), err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("artifact: land %s: %w", filepath.Base(path), err)
	}
	if err := syncData(file); err != nil {
		return fmt.Errorf("artifact: fsync landed %s: %w", filepath.Base(path), err)
	}
	return nil
}

func (s *Store) publishLanded(landing string, landed []Digest, levels map[Digest]int) error {
	if len(landed) == 0 {
		return nil
	}
	if err := flushBarrier(landing); err != nil {
		return fmt.Errorf("artifact: flush landed objects: %w", err)
	}
	height := 0
	for _, level := range levels {
		height = max(height, level)
	}
	for level := range height + 1 {
		if err := s.publishLevel(landing, landed, levels, level); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) publishLevel(landing string, landed []Digest, levels map[Digest]int, level int) error {
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	dirs := map[string]struct{}{}
	var published []Digest
	for _, digest := range landed {
		if levels[digest] != level {
			continue
		}
		releases = append(releases, s.claim(digest))
		present, err := s.touch(digest)
		if err != nil {
			return errors.Join(err, s.unpublish(landing, published))
		}
		if present {
			continue
		}
		target := s.objectPath(digest)
		if err := os.Rename(filepath.Join(landing, string(digest)), target); err != nil {
			return errors.Join(fmt.Errorf("artifact: publish object %s: %w", digest, err), s.unpublish(landing, published))
		}
		published = append(published, digest)
		dirs[filepath.Dir(target)] = struct{}{}
	}
	for dir := range dirs {
		if err := syncObjectDir(dir); err != nil {
			return errors.Join(err, s.unpublish(landing, published))
		}
	}
	if err := flushBarrier(landing); err != nil {
		return errors.Join(fmt.Errorf("artifact: flush published objects: %w", err), s.unpublish(landing, published))
	}
	s.markSynced(published...)
	return nil
}

func (s *Store) unpublish(landing string, published []Digest) error {
	var errs []error
	var stranded []Digest
	for _, digest := range published {
		if err := os.Rename(s.objectPath(digest), filepath.Join(landing, string(digest))); err != nil {
			stranded = append(stranded, digest)
			errs = append(errs, fmt.Errorf("artifact: withdraw unsynced object %s: %w", digest, err))
		}
	}
	s.markUnsynced(stranded)
	return errors.Join(errs...)
}

func syncDir(dir string) (err error) {
	d, err := os.Open(dir) //nolint:gosec // G304: dir is an object prefix directory under the store root.
	if err != nil {
		return fmt.Errorf("artifact: open %s: %w", dir, err)
	}
	defer func() { err = errors.Join(err, d.Close()) }()
	if err := syncData(d); err != nil {
		return fmt.Errorf("artifact: fsync %s: %w", dir, err)
	}
	return nil
}

func fullSyncDir(dir string) (err error) {
	d, err := os.Open(dir) //nolint:gosec // G304: dir is a batch landing directory under the store root.
	if err != nil {
		return fmt.Errorf("artifact: open %s: %w", dir, err)
	}
	defer func() { err = errors.Join(err, d.Close()) }()
	return fullSync(d)
}

func (s *Store) requireChildren(digest Digest, data []byte, batch map[Digest]struct{}) error {
	m, err := DecodeManifest(data)
	if err != nil {
		return fmt.Errorf("artifact: manifest %s: %w", digest, err)
	}
	for _, child := range m.children() {
		if _, earlier := batch[child]; earlier {
			continue
		}
		present, err := s.present(child)
		if err != nil {
			return err
		}
		if !present {
			return fmt.Errorf("%w: manifest %s precedes its child %s", ErrInvalid, digest, child)
		}
	}
	return nil
}

func (s *Store) scanStaged(ctx context.Context, dir string, d BatchDescriptor, each func(ObjectEntry, []byte) error) (err error) {
	readers := make([]io.Reader, 0, len(d.Parts))
	for index := range d.Parts {
		file, openErr := os.Open(filepath.Join(dir, partName(index))) //nolint:gosec // G304: part path is under the store root, built from a validated batch id and index.
		if openErr != nil {
			return fmt.Errorf("artifact: open part %d of batch %s: %w", index, d.ID, openErr)
		}
		defer func() { err = errors.Join(err, file.Close()) }()
		readers = append(readers, file)
	}
	return scanPack(ctx, io.MultiReader(readers...), d, each)
}
