package artifact

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/durable"
)

type storedObject struct {
	digest Digest
	size   int64
	young  bool
}

// GC removes every object outside the union of pin closures that has not
// been written or touched within GCGrace, and every outbox or incoming batch
// untouched for StagingTTL. An object younger than GCGrace keeps its whole
// closure, so no surviving manifest ever loses a child.
func (s *Store) GC(ctx context.Context) (GCReport, error) {
	s.gcMu.Lock()
	defer s.gcMu.Unlock()
	now := time.Now()
	var report GCReport
	for _, dir := range []string{outboxDir, incomingDir} {
		removed, err := s.sweepStaging(filepath.Join(s.root, dir), now.Add(-StagingTTL))
		if err != nil {
			return GCReport{}, err
		}
		report.StagingRemoved += removed
	}
	objects, err := s.scanObjects(ctx, now.Add(-GCGrace))
	if err != nil {
		return GCReport{}, err
	}
	pins, err := s.Pins(ctx)
	if err != nil {
		return GCReport{}, err
	}
	w := newWalker(s.Reader, ClosureBound{MaxObjects: math.MaxInt, MaxDepth: DefaultClosureBound.MaxDepth, MaxBytes: math.MaxInt64}, true)
	for _, pin := range pins {
		if err := w.walk(ctx, pin.Roots); err != nil {
			return GCReport{}, fmt.Errorf("artifact: mark pins of %q: %w", pin.Owner, err)
		}
	}
	report.Marked = len(w.objects)
	for _, object := range objects {
		if !object.young {
			continue
		}
		manifest, err := s.isManifest(object.digest)
		if err != nil {
			return GCReport{}, err
		}
		if !manifest {
			continue
		}
		if err := w.visit(ctx, Ref{Digest: object.digest, Kind: KindManifest, Size: -1}, 1); err != nil {
			return GCReport{}, fmt.Errorf("artifact: mark young manifest %s: %w", object.digest, err)
		}
	}
	for _, object := range objects {
		if _, kept := w.seen[object.digest]; kept || object.young {
			continue
		}
		if err := durable.Remove(s.objectPath(object.digest)); err != nil {
			return GCReport{}, fmt.Errorf("artifact: remove object %s: %w", object.digest, err)
		}
		report.Removed++
		report.FreedBytes += object.size
	}
	return report, nil
}

func (s *Store) sweepStaging(dir string, cutoff time.Time) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("artifact: list %s: %w", dir, err)
	}
	removed := 0
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return removed, fmt.Errorf("artifact: stat %s: %w", entry.Name(), err)
		}
		if !info.ModTime().Before(cutoff) {
			continue
		}
		if err := durable.RemoveTree(filepath.Join(dir, entry.Name())); err != nil {
			return removed, fmt.Errorf("artifact: remove staging %s: %w", entry.Name(), err)
		}
		removed++
	}
	return removed, nil
}

func (s *Store) scanObjects(ctx context.Context, cutoff time.Time) ([]storedObject, error) {
	objects := []storedObject{}
	for prefix := range 256 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		dir := filepath.Join(s.root, objectsDir, fmt.Sprintf("%02x", prefix))
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, fmt.Errorf("artifact: list %s: %w", dir, err)
		}
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				return nil, fmt.Errorf("artifact: stat %s: %w", entry.Name(), err)
			}
			young := !info.ModTime().Before(cutoff)
			if strings.HasPrefix(entry.Name(), ".") {
				if !young {
					if err := durable.Remove(filepath.Join(dir, entry.Name())); err != nil {
						return nil, fmt.Errorf("artifact: remove temp %s: %w", entry.Name(), err)
					}
				}
				continue
			}
			digest := Digest(entry.Name())
			if err := digest.Validate(); err != nil {
				return nil, fmt.Errorf("artifact: foreign file in %s: %w", dir, err)
			}
			objects = append(objects, storedObject{digest: digest, size: info.Size(), young: young})
		}
	}
	return objects, nil
}

func (s *Store) isManifest(digest Digest) (bool, error) {
	prefix, err := readPrefix(s.objectPath(digest), len(manifestPrefix))
	if err != nil {
		return false, fmt.Errorf("artifact: read object %s: %w", digest, err)
	}
	if !bytes.Equal(prefix, manifestPrefix) {
		return false, nil
	}
	data, err := s.readObject(digest)
	if err != nil {
		return false, err
	}
	return sniffKind(data) == KindManifest, nil
}

func readPrefix(path string, n int) (prefix []byte, err error) {
	file, err := os.Open(path) //nolint:gosec // G304: path is an object under the store root, named by a validated digest.
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	prefix = make([]byte, n)
	read, err := io.ReadFull(file, prefix)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return prefix[:read], nil
	}
	return prefix, err
}
