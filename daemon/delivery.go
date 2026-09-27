package daemon

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
	"strings"
	"time"

	"github.com/yasyf/daemonkit/durable"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/syncservice"
)

const (
	deliveryStateIdentity   = "synckit-delivery-v2"
	deliveryStateVersion    = 2
	deliveryV1StateIdentity = "synckit-delivery-v1"
	deliveryLockDeadline    = 30 * time.Second
)

var (
	errPendingChanged  = errors.New("delivery: pending change identity changed")
	errSourceRegressed = errors.New("delivery: source revision regressed")
	errSupersede       = errors.New("delivery: replacement must be a newer snapshot")
)

type pendingRef struct {
	ChangeID       string                 `json:"change_id"`
	Kind           syncservice.ChangeKind `json:"kind"`
	BaseRevision   syncservice.Revision   `json:"base_revision"`
	SourceRevision syncservice.Revision   `json:"source_revision"`
	Roots          []artifact.Ref         `json:"roots"`
	StagedAt       time.Time              `json:"staged_at"`
	Superseded     uint64                 `json:"superseded"`
}

type deliveryRecord struct {
	ServiceID     string               `json:"service_id"`
	Peer          string               `json:"peer"`
	Generation    uint64               `json:"generation"`
	Acked         syncservice.Revision `json:"acked_revision"`
	AckedChangeID string               `json:"acked_change_id"`
	AckedAt       time.Time            `json:"acked_at"`
	Pending       *pendingRef          `json:"pending,omitempty"`
}

type deliveryState struct {
	Identity string           `json:"identity"`
	Version  uint64           `json:"version"`
	Records  []deliveryRecord `json:"records"`
}

type deliveryV1Record struct {
	ServiceID string                      `json:"service_id"`
	Peer      string                      `json:"peer"`
	Acked     syncservice.Revision        `json:"acked_revision"`
	Pending   *syncservice.ChangeEnvelope `json:"pending,omitempty"`
}

type deliveryV1State struct {
	Identity string             `json:"identity"`
	Version  uint64             `json:"version"`
	Records  []deliveryV1Record `json:"records"`
}

type deliveryStore struct {
	dir     string
	path    string
	lock    string
	pending string
	v1Path  string
	v1Lock  string
	now     func() time.Time
}

func newDeliveryStore(directory string) *deliveryStore {
	return &deliveryStore{
		dir:     directory,
		path:    filepath.Join(directory, "delivery-v2.json"),
		lock:    filepath.Join(directory, "delivery-v2.lock"),
		pending: filepath.Join(directory, "delivery-v2", "pending"),
		v1Path:  filepath.Join(directory, "delivery-v1.json"),
		v1Lock:  filepath.Join(directory, "delivery-v1.lock"),
		now:     time.Now,
	}
}

func (s *deliveryStore) load(ctx context.Context, serviceID, peer string) (deliveryRecord, *syncservice.ChangeEnvelope, error) {
	var record deliveryRecord
	var pending *syncservice.ChangeEnvelope
	err := s.withState(ctx, false, func(state *deliveryState) error {
		record = state.find(serviceID, peer)
		if record.Pending == nil {
			return nil
		}
		change, err := s.readPending(record.Pending.ChangeID)
		if err != nil {
			return err
		}
		pending = &change
		return nil
	})
	return record, pending, err
}

func (s *deliveryStore) records(ctx context.Context, serviceID string) ([]deliveryRecord, error) {
	var out []deliveryRecord
	err := s.withState(ctx, false, func(state *deliveryState) error {
		for _, record := range state.Records {
			if serviceID == "" || record.ServiceID == serviceID {
				out = append(out, record)
			}
		}
		return nil
	})
	return out, err
}

func (s *deliveryStore) stage(ctx context.Context, peer, expected string, change syncservice.ChangeEnvelope) error {
	if err := change.Validate(true); err != nil {
		return err
	}
	source, err := change.SourceRevision.Uint64()
	if err != nil {
		return err
	}
	return s.withState(ctx, true, func(state *deliveryState) error {
		record := state.upsert(change.ServiceID, peer)
		held := ""
		if record.Pending != nil {
			held = record.Pending.ChangeID
		}
		if held != expected {
			return fmt.Errorf("%w: expected %q, holding %q", errPendingChanged, expected, held)
		}
		acked := mustRevision(record.Acked)
		if source <= acked {
			return fmt.Errorf("%w: source %d does not exceed acked %d", errSourceRegressed, source, acked)
		}
		next := &pendingRef{
			ChangeID: change.ChangeID, Kind: change.Kind,
			BaseRevision: change.BaseRevision, SourceRevision: change.SourceRevision,
			Roots: slices.Clone(change.Artifacts), StagedAt: s.now().UTC(),
		}
		if old := record.Pending; old != nil {
			oldSource := mustRevision(old.SourceRevision)
			replacesDelta := source == oldSource && old.Kind == syncservice.ChangeDelta
			if change.Kind != syncservice.ChangeSnapshot || (source <= oldSource && !replacesDelta) {
				return fmt.Errorf("%w: %s at %d over %s at %d", errSupersede, change.Kind, source, old.Kind, oldSource)
			}
			next.Superseded = old.Superseded + 1
		}
		raw, err := json.Marshal(change)
		if err != nil {
			return err
		}
		if err := durable.WriteFile(s.pendingPath(change.ChangeID), append(raw, '\n'), 0o600); err != nil {
			return err
		}
		record.Pending = next
		record.Generation++
		return nil
	})
}

func (s *deliveryStore) acknowledge(ctx context.Context, peer string, change syncservice.ChangeEnvelope, ack syncservice.ApplyResult) error {
	if ack.NeedSnapshot || ack.Partial || ack.AckedRevision != change.SourceRevision {
		return errors.New("delivery: acknowledgement does not match pending source revision")
	}
	source, err := change.SourceRevision.Uint64()
	if err != nil {
		return err
	}
	return s.withState(ctx, true, func(state *deliveryState) error {
		index := state.index(change.ServiceID, peer)
		if index < 0 || state.Records[index].Pending == nil || state.Records[index].Pending.ChangeID != change.ChangeID {
			return fmt.Errorf("%w: acknowledgement of %q", errPendingChanged, change.ChangeID)
		}
		record := &state.Records[index]
		if source > mustRevision(record.Acked) {
			record.Acked = change.SourceRevision
		}
		record.AckedChangeID = change.ChangeID
		record.AckedAt = s.now().UTC()
		record.Pending = nil
		record.Generation++
		return nil
	})
}

func (s *deliveryStore) withState(ctx context.Context, write bool, apply func(*deliveryState) error) error {
	if s == nil || !filepath.IsAbs(s.path) || !filepath.IsAbs(s.lock) {
		return errors.New("delivery: exact store paths are required")
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	for _, dir := range []string{filepath.Dir(s.pending), s.pending} {
		if err := durable.Mkdir(dir, 0o700); err != nil {
			return err
		}
	}
	lockCtx, cancel := context.WithTimeout(ctx, deliveryLockDeadline)
	defer cancel()
	lock, err := durable.AcquireLock(lockCtx, s.lock)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	state, err := readDeliveryState(s.path)
	if errors.Is(err, os.ErrNotExist) {
		state, err = s.migrate(lockCtx)
	}
	if err != nil {
		return err
	}
	if err := s.sweep(state); err != nil {
		return err
	}
	if err := apply(state); err != nil {
		return err
	}
	if !write {
		return nil
	}
	if err := writeDeliveryState(s.path, state); err != nil {
		return err
	}
	return s.sweep(state)
}

func (s *deliveryStore) migrate(ctx context.Context) (*deliveryState, error) {
	state := &deliveryState{Identity: deliveryStateIdentity, Version: deliveryStateVersion, Records: []deliveryRecord{}}
	if _, err := os.Stat(s.v1Path); errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	lock, err := durable.AcquireLock(ctx, s.v1Lock)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.Close() }()
	var v1 deliveryV1State
	if err := decodeStrict(s.v1Path, &v1); err != nil {
		return nil, fmt.Errorf("delivery: read v1 state: %w", err)
	}
	if v1.Identity != deliveryV1StateIdentity || v1.Version != 1 || v1.Records == nil {
		return nil, errors.New("delivery: v1 state schema mismatch")
	}
	for _, record := range v1.Records {
		if record.ServiceID == "" || record.Peer == "" {
			return nil, errors.New("delivery: v1 record is incomplete")
		}
		if _, err := record.Acked.Uint64(); err != nil {
			return nil, fmt.Errorf("delivery: v1 record acked: %w", err)
		}
		if state.index(record.ServiceID, record.Peer) >= 0 {
			return nil, errors.New("delivery: v1 state has a duplicate record")
		}
		state.Records = append(state.Records, deliveryRecord{ServiceID: record.ServiceID, Peer: record.Peer, Acked: record.Acked})
	}
	if err := writeDeliveryState(s.path, state); err != nil {
		return nil, err
	}
	if err := durable.Remove(s.v1Path); err != nil {
		return nil, err
	}
	return state, nil
}

func (s *deliveryStore) sweep(state *deliveryState) error {
	entries, err := os.ReadDir(s.pending)
	if err != nil {
		return err
	}
	referenced := make(map[string]bool, len(state.Records))
	for _, record := range state.Records {
		if record.Pending != nil {
			referenced[record.Pending.ChangeID+".json"] = true
		}
	}
	for _, entry := range entries {
		if referenced[entry.Name()] {
			continue
		}
		if err := durable.RemoveTree(filepath.Join(s.pending, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (s *deliveryStore) pendingPath(changeID string) string {
	return filepath.Join(s.pending, changeID+".json")
}

func (s *deliveryStore) readPending(changeID string) (syncservice.ChangeEnvelope, error) {
	var change syncservice.ChangeEnvelope
	if err := decodeStrict(s.pendingPath(changeID), &change); err != nil {
		return syncservice.ChangeEnvelope{}, fmt.Errorf("delivery: read pending %s: %w", changeID, err)
	}
	if err := change.Validate(true); err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	if change.ChangeID != changeID {
		return syncservice.ChangeEnvelope{}, fmt.Errorf("delivery: pending file %s holds change %s", changeID, change.ChangeID)
	}
	return change, nil
}

func (state *deliveryState) index(serviceID, peer string) int {
	return slices.IndexFunc(state.Records, func(record deliveryRecord) bool {
		return record.ServiceID == serviceID && record.Peer == peer
	})
}

func (state *deliveryState) find(serviceID, peer string) deliveryRecord {
	if index := state.index(serviceID, peer); index >= 0 {
		return state.Records[index]
	}
	return deliveryRecord{ServiceID: serviceID, Peer: peer, Acked: syncservice.NewRevision(0)}
}

func (state *deliveryState) upsert(serviceID, peer string) *deliveryRecord {
	index := state.index(serviceID, peer)
	if index < 0 {
		state.Records = append(state.Records, state.find(serviceID, peer))
		index = len(state.Records) - 1
	}
	return &state.Records[index]
}

func writeDeliveryState(path string, state *deliveryState) error {
	slices.SortFunc(state.Records, func(a, b deliveryRecord) int {
		if c := strings.Compare(a.ServiceID, b.ServiceID); c != 0 {
			return c
		}
		return strings.Compare(a.Peer, b.Peer)
	})
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return durable.WriteFile(path, append(raw, '\n'), 0o600)
}

func readDeliveryState(path string) (*deliveryState, error) {
	var state deliveryState
	if err := decodeStrict(path, &state); err != nil {
		return nil, err
	}
	if state.Identity != deliveryStateIdentity || state.Version != deliveryStateVersion || state.Records == nil {
		return nil, errors.New("delivery: state schema mismatch")
	}
	for index, record := range state.Records {
		if err := record.validate(); err != nil {
			return nil, fmt.Errorf("delivery: record %d: %w", index, err)
		}
		if slices.IndexFunc(state.Records[:index], func(other deliveryRecord) bool {
			return other.ServiceID == record.ServiceID && other.Peer == record.Peer
		}) >= 0 {
			return nil, errors.New("delivery: duplicate record")
		}
	}
	return &state, nil
}

func (r deliveryRecord) validate() error {
	if r.ServiceID == "" || r.Peer == "" {
		return errors.New("service and peer are required")
	}
	acked, err := r.Acked.Uint64()
	if err != nil {
		return err
	}
	if r.Pending == nil {
		return nil
	}
	p := r.Pending
	if p.ChangeID == "" || filepath.Base(p.ChangeID) != p.ChangeID || strings.HasPrefix(p.ChangeID, ".") {
		return fmt.Errorf("pending change id %q is invalid", p.ChangeID)
	}
	if p.Kind != syncservice.ChangeSnapshot && p.Kind != syncservice.ChangeDelta {
		return fmt.Errorf("pending kind %q is invalid", p.Kind)
	}
	if _, err := p.BaseRevision.Uint64(); err != nil {
		return err
	}
	source, err := p.SourceRevision.Uint64()
	if err != nil {
		return err
	}
	if source <= acked {
		return errors.New("pending source does not exceed acked")
	}
	return artifact.ValidateRoots(p.Roots)
}

func decodeStrict(path string, target any) error {
	raw, err := os.ReadFile(path) //nolint:gosec // fixed Synckit state paths
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("delivery: state has trailing data")
	}
	return nil
}

func mustRevision(revision syncservice.Revision) uint64 {
	value, err := revision.Uint64()
	if err != nil {
		panic(fmt.Sprintf("delivery: validated revision %q does not parse: %v", revision, err))
	}
	return value
}
