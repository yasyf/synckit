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
	"time"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"

	"github.com/yasyf/synckit/syncservice"
)

const deliveryStateIdentity = "synckit-delivery-v1"

type deliveryRecord struct {
	ServiceID string                      `json:"service_id"`
	Peer      string                      `json:"peer"`
	Acked     syncservice.Revision        `json:"acked_revision"`
	Pending   *syncservice.ChangeEnvelope `json:"pending,omitempty"`
}

type deliveryState struct {
	Identity string           `json:"identity"`
	Version  uint64           `json:"version"`
	Records  []deliveryRecord `json:"records"`
}

type deliveryStore struct {
	path string
	lock string
}

func newDeliveryStore(directory string) *deliveryStore {
	return &deliveryStore{
		path: filepath.Join(directory, "delivery-v1.json"),
		lock: filepath.Join(directory, "delivery-v1.lock"),
	}
}

func (s *deliveryStore) load(ctx context.Context, serviceID, peer string) (syncservice.Revision, *syncservice.ChangeEnvelope, error) {
	var record deliveryRecord
	err := s.withState(ctx, false, func(state *deliveryState) error {
		if index := deliveryRecordIndex(state.Records, serviceID, peer); index >= 0 {
			record = state.Records[index]
		}
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	if record.Acked == "" {
		record.Acked = syncservice.NewRevision(0)
	}
	return record.Acked, record.Pending, nil
}

func (s *deliveryStore) putPending(ctx context.Context, peer string, change syncservice.ChangeEnvelope) error {
	if err := change.Validate(true); err != nil {
		return err
	}
	return s.withState(ctx, true, func(state *deliveryState) error {
		index := deliveryRecordIndex(state.Records, change.ServiceID, peer)
		if index < 0 {
			state.Records = append(state.Records, deliveryRecord{
				ServiceID: change.ServiceID, Peer: peer, Acked: syncservice.NewRevision(0),
			})
			index = len(state.Records) - 1
		}
		pending := change
		pending.Payload = bytes.Clone(change.Payload)
		state.Records[index].Pending = &pending
		return nil
	})
}

func (s *deliveryStore) acknowledge(ctx context.Context, peer string, change syncservice.ChangeEnvelope, ack syncservice.ApplyResult) error {
	if ack.NeedSnapshot || ack.AckedRevision != change.SourceRevision {
		return errors.New("delivery: acknowledgement does not match pending source revision")
	}
	return s.withState(ctx, true, func(state *deliveryState) error {
		index := deliveryRecordIndex(state.Records, change.ServiceID, peer)
		if index < 0 || state.Records[index].Pending == nil ||
			state.Records[index].Pending.ChangeID != change.ChangeID {
			return errors.New("delivery: pending change identity changed before acknowledgement")
		}
		state.Records[index].Acked = ack.AckedRevision
		state.Records[index].Pending = nil
		return nil
	})
}

func (s *deliveryStore) withState(ctx context.Context, write bool, apply func(*deliveryState) error) error {
	if s == nil || !filepath.IsAbs(s.path) || !filepath.IsAbs(s.lock) {
		return errors.New("delivery: exact store paths are required")
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	lock, err := (proc.FileLockSpec{Path: s.lock, Mode: proc.FileLockExclusive, Deadline: 30 * time.Second}).Acquire(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	state, err := readDeliveryState(s.path)
	if err != nil {
		return err
	}
	if err := apply(state); err != nil {
		return err
	}
	if !write {
		return nil
	}
	slices.SortFunc(state.Records, func(a, b deliveryRecord) int {
		if a.ServiceID != b.ServiceID {
			return compareString(a.ServiceID, b.ServiceID)
		}
		return compareString(a.Peer, b.Peer)
	})
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return dkdaemon.WriteFileDurable(s.path, append(raw, '\n'), 0o600)
}

func readDeliveryState(path string) (*deliveryState, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // fixed Synckit state path
	if errors.Is(err, os.ErrNotExist) {
		return &deliveryState{Identity: deliveryStateIdentity, Version: 1, Records: []deliveryRecord{}}, nil
	}
	if err != nil {
		return nil, err
	}
	var state deliveryState
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("delivery: state has trailing data")
	}
	if state.Identity != deliveryStateIdentity || state.Version != 1 || state.Records == nil {
		return nil, errors.New("delivery: state schema mismatch")
	}
	for index, record := range state.Records {
		if record.ServiceID == "" || record.Peer == "" || record.Acked == "" {
			return nil, fmt.Errorf("delivery: record %d is incomplete", index)
		}
		if _, err := record.Acked.Uint64(); err != nil {
			return nil, err
		}
		if record.Pending != nil {
			if err := record.Pending.Validate(true); err != nil {
				return nil, err
			}
		}
		if index > 0 && deliveryRecordIndex(state.Records[:index], record.ServiceID, record.Peer) >= 0 {
			return nil, errors.New("delivery: duplicate record")
		}
	}
	return &state, nil
}

func deliveryRecordIndex(records []deliveryRecord, serviceID, peer string) int {
	for index, record := range records {
		if record.ServiceID == serviceID && record.Peer == peer {
			return index
		}
	}
	return -1
}

func compareString(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
