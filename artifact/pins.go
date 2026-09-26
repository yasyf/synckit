package artifact

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/durable"
)

// Validate checks the owner, a non-empty root list, and the update stamp.
func (p PinSet) Validate() error {
	if err := (PinsSetParams{Owner: p.Owner, Roots: p.Roots}).Validate(); err != nil {
		return err
	}
	if len(p.Roots) == 0 || p.UpdatedAt.IsZero() {
		return fmt.Errorf("%w: pin set %q is empty or unstamped", ErrInvalid, p.Owner)
	}
	return nil
}

func (s *Store) pinPath(owner string) string {
	return filepath.Join(s.root, pinsDir, string(Sum([]byte(owner)))+".json")
}

// SetPins atomically replaces owner's pinned roots; empty roots removes the
// pin set. Roots need not be present: GC keeps whatever of their closure is.
func (s *Store) SetPins(ctx context.Context, owner string, roots []Ref) error {
	if err := (PinsSetParams{Owner: owner, Roots: roots}).Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	path := s.pinPath(owner)
	if len(roots) == 0 {
		if err := durable.Remove(path); err != nil {
			return fmt.Errorf("artifact: remove pins of %q: %w", owner, err)
		}
		return nil
	}
	data, err := durable.Marshal(PinSet{Owner: owner, Roots: roots, UpdatedAt: time.Now().UTC()})
	if err != nil {
		return fmt.Errorf("artifact: encode pins of %q: %w", owner, err)
	}
	if err := durable.WriteFile(path, data, filePerm); err != nil {
		return fmt.Errorf("artifact: write pins of %q: %w", owner, err)
	}
	return nil
}

// Pins returns every pin set, ordered by owner.
func (s *Store) Pins(ctx context.Context) ([]PinSet, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(s.root, pinsDir))
	if err != nil {
		return nil, fmt.Errorf("artifact: list pins: %w", err)
	}
	pins := []PinSet{}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		path := filepath.Join(s.root, pinsDir, entry.Name())
		pin, err := durable.ReadFile[PinSet](path)
		if err != nil {
			return nil, fmt.Errorf("artifact: read pins: %w", err)
		}
		if s.pinPath(pin.Owner) != path {
			return nil, fmt.Errorf("%w: pin file %s holds owner %q", ErrInvalid, entry.Name(), pin.Owner)
		}
		pins = append(pins, pin)
	}
	slices.SortFunc(pins, func(a, b PinSet) int { return strings.Compare(a.Owner, b.Owner) })
	return pins, nil
}
