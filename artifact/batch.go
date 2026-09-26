package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/durable"
)

// ErrBatchFull marks a batch build past MaxBatchObjects objects or
// MaxBatchRaw object bytes.
var ErrBatchFull = errors.New("artifact: batch full")

var errIncomingFull = fmt.Errorf("artifact: %d incoming batches already staged", MaxIncoming)

func partName(index int) string {
	return fmt.Sprintf("%04d.part", index)
}

// BuildBatch packs objects, in order, into a zstd stream split into outbox
// parts and returns the batch's descriptor. Identical inputs name the same
// batch, and a rebuild of a present batch keeps its parts and refreshes its
// StagingTTL.
func (s *Store) BuildBatch(ctx context.Context, objects []Digest) (BatchDescriptor, error) {
	if len(objects) > MaxBatchObjects {
		return BatchDescriptor{}, fmt.Errorf("%w: %d objects exceed %d", ErrBatchFull, len(objects), MaxBatchObjects)
	}
	if err := (BatchBuildParams{Objects: objects}).Validate(); err != nil {
		return BatchDescriptor{}, err
	}
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	var raw int64
	for _, digest := range objects {
		info, err := os.Stat(s.objectPath(digest))
		if errors.Is(err, os.ErrNotExist) {
			return BatchDescriptor{}, &MissingError{Digest: digest}
		}
		if err != nil {
			return BatchDescriptor{}, fmt.Errorf("artifact: stat object %s: %w", digest, err)
		}
		raw += info.Size()
	}
	if raw > MaxBatchRaw {
		return BatchDescriptor{}, fmt.Errorf("%w: %d object bytes exceed %d", ErrBatchFull, raw, MaxBatchRaw)
	}
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

func (s *Store) pack(ctx context.Context, dir string, objects []Digest) (BatchDescriptor, error) {
	parts := &partWriter{dir: dir, buf: make([]byte, 0, PartSize)}
	s.encoder.Reset(parts)
	pack := &packWriter{w: s.encoder}
	if err := pack.begin(); err != nil {
		return BatchDescriptor{}, err
	}
	entries := make([]ObjectEntry, 0, len(objects))
	for _, digest := range objects {
		if err := ctx.Err(); err != nil {
			return BatchDescriptor{}, err
		}
		data, err := s.readObject(digest)
		if err != nil {
			return BatchDescriptor{}, err
		}
		entry := ObjectEntry{Digest: digest, Kind: sniffKind(data), Size: int64(len(data))}
		if err := pack.object(entry, data); err != nil {
			return BatchDescriptor{}, err
		}
		entries = append(entries, entry)
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
	return NewBatchDescriptor(pack.n, entries, parts.parts)
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
// resumes it; a new batch is refused while MaxIncoming batches are staged.
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

func (s *Store) stagedBatch(id Digest) (string, BatchDescriptor, error) {
	dir := filepath.Join(s.root, incomingDir, string(id))
	d, err := durable.ReadFile[BatchDescriptor](filepath.Join(dir, descriptorName))
	if err != nil {
		return "", BatchDescriptor{}, fmt.Errorf("artifact: batch %s is not staged: %w", id, err)
	}
	return dir, d, nil
}

// PutPart durably writes part index of staged batch id after checking its
// length and sha256 against the descriptor.
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
		return err
	}
	if index >= len(d.Parts) {
		return fmt.Errorf("%w: batch %s has %d parts, not part %d", ErrInvalid, id, len(d.Parts), index)
	}
	if want := d.Parts[index]; int64(len(data)) != want.Size || Sum(data) != want.Digest {
		return fmt.Errorf("%w: part %d of batch %s does not match its descriptor", ErrInvalid, index, id)
	}
	if err := durable.WriteFile(filepath.Join(dir, partName(index)), data, filePerm); err != nil {
		return fmt.Errorf("artifact: write part %d of batch %s: %w", index, id, err)
	}
	return nil
}

// CommitBatch decodes staged batch id through the bounded decoder and
// verifies every object's kind, digest, and size, every manifest strictly,
// and that every manifest's children precede it, before storing anything.
// It then durably creates each absent object in order, touches each present
// one, and removes the staging.
func (s *Store) CommitBatch(ctx context.Context, id Digest) (CommitReport, error) {
	if err := id.Validate(); err != nil {
		return CommitReport{}, err
	}
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	s.incomingMu.Lock()
	defer s.incomingMu.Unlock()
	dir, d, err := s.stagedBatch(id)
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
	var report CommitReport
	err = s.scanStaged(ctx, dir, d, func(entry ObjectEntry, data []byte) error {
		stored, err := s.writeObject(entry.Digest, data)
		if err != nil {
			return err
		}
		if !stored {
			report.Present++
			return nil
		}
		report.Stored++
		report.Bytes += entry.Size
		return nil
	})
	if err != nil {
		return CommitReport{}, err
	}
	if err := durable.RemoveTree(dir); err != nil {
		return CommitReport{}, fmt.Errorf("artifact: remove staged batch %s: %w", id, err)
	}
	return report, nil
}

func (s *Store) requireChildren(digest Digest, data []byte, batch map[Digest]struct{}) error {
	m, err := DecodeManifest(data)
	if err != nil {
		return fmt.Errorf("artifact: manifest %s: %w", digest, err)
	}
	children := make([]Digest, 0, len(m.Chunks)+len(m.Deps))
	for _, chunk := range m.Chunks {
		children = append(children, chunk.Digest)
	}
	for _, dep := range m.Deps {
		children = append(children, dep.Digest)
	}
	for _, child := range children {
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
