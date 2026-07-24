package syncservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/yasyf/synckit/internal/serviceidentity"
)

// The svc.-namespaced rpc method names that make up the typed sync contract.
const (
	// MethodCapabilities reports the peer's name and methods.
	MethodCapabilities = "svc.capabilities"
	// MethodList enumerates the items this peer tracks for sync.
	MethodList = "svc.list"
	// MethodReconcile converges this peer against an origin host.
	MethodReconcile = "svc.reconcile"
	// MethodExport exports one immutable service-owned full or delta payload.
	MethodExport = "synckit.syncservice.export.v1"
	// MethodApply applies one immutable exported payload and acknowledges it.
	MethodApply = "synckit.syncservice.apply.v1"
)

// MaxTransferPayload is the largest opaque service payload accepted by v1.
const MaxTransferPayload = 8 << 20

// ChangeKind identifies a full snapshot or base-fenced delta.
type ChangeKind string

const (
	// ChangeSnapshot replaces any prior source revision.
	ChangeSnapshot ChangeKind = "snapshot"
	// ChangeDelta applies only to its exact base revision.
	ChangeDelta ChangeKind = "delta"
)

// Revision is one canonical decimal uint64 carried without JSON precision loss.
type Revision string

// NewRevision encodes value as a canonical wire revision.
func NewRevision(value uint64) Revision { return Revision(strconv.FormatUint(value, 10)) }

// Uint64 validates and decodes the canonical revision.
func (r Revision) Uint64() (uint64, error) {
	if r == "" || (len(r) > 1 && r[0] == '0') {
		return 0, errors.New("syncservice: revision is not canonical")
	}
	value, err := strconv.ParseUint(string(r), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("syncservice: revision: %w", err)
	}
	return value, nil
}

// ExportRequest asks a bound service for a change after SinceRevision.
type ExportRequest struct {
	ServiceID         string   `json:"service_id"`
	SchemaFingerprint string   `json:"schema_fingerprint"`
	SinceRevision     Revision `json:"since_revision"`
}

// ChangeEnvelope is one immutable, digest-bound service-owned payload.
type ChangeEnvelope struct {
	ServiceID         string     `json:"service_id"`
	SchemaFingerprint string     `json:"schema_fingerprint"`
	Origin            string     `json:"origin"`
	ChangeID          string     `json:"change_id"`
	Kind              ChangeKind `json:"kind"`
	BaseRevision      Revision   `json:"base_revision"`
	SourceRevision    Revision   `json:"source_revision"`
	PayloadDigest     string     `json:"payload_digest"`
	Payload           []byte     `json:"payload"`
}

// ApplyResult acknowledges one source revision or requests a full snapshot.
type ApplyResult struct {
	AckedRevision Revision `json:"acked_revision"`
	NeedSnapshot  bool     `json:"need_snapshot,omitempty"`
}

// NewExportedChange constructs and validates one source-owned change.
func NewExportedChange(
	serviceID, schemaFingerprint string,
	kind ChangeKind,
	baseRevision, sourceRevision Revision,
	payload []byte,
) (ChangeEnvelope, error) {
	digest := sha256.Sum256(payload)
	change := ChangeEnvelope{
		ServiceID: serviceID, SchemaFingerprint: schemaFingerprint,
		Kind: kind, BaseRevision: baseRevision, SourceRevision: sourceRevision,
		PayloadDigest: hex.EncodeToString(digest[:]), Payload: append([]byte(nil), payload...),
	}
	return change, change.Validate(false)
}

// Validate checks the exact export request identity and revision.
func (r ExportRequest) Validate() error {
	if err := ValidateServiceSchema(r.ServiceID, r.SchemaFingerprint); err != nil {
		return err
	}
	_, err := r.SinceRevision.Uint64()
	return err
}

// Validate checks one exported or delivery-bound change.
func (e ChangeEnvelope) Validate(requireDelivery bool) error {
	if err := ValidateServiceSchema(e.ServiceID, e.SchemaFingerprint); err != nil {
		return err
	}
	base, err := e.BaseRevision.Uint64()
	if err != nil {
		return err
	}
	source, err := e.SourceRevision.Uint64()
	if err != nil {
		return err
	}
	if source == 0 || source <= base {
		return errors.New("syncservice: source revision must exceed base revision")
	}
	if e.Kind != ChangeSnapshot && e.Kind != ChangeDelta {
		return errors.New("syncservice: change kind is invalid")
	}
	if e.Kind == ChangeSnapshot && base != 0 {
		return errors.New("syncservice: snapshot base revision must be zero")
	}
	if len(e.Payload) == 0 || len(e.Payload) > MaxTransferPayload || !json.Valid(e.Payload) {
		return errors.New("syncservice: payload must be bounded valid JSON")
	}
	digest := sha256.Sum256(e.Payload)
	if e.PayloadDigest != hex.EncodeToString(digest[:]) {
		return errors.New("syncservice: payload digest mismatch")
	}
	if requireDelivery {
		if e.Origin == "" || !exactDigest(e.ChangeID) {
			return errors.New("syncservice: delivery origin and change id are required")
		}
	} else if e.Origin != "" || e.ChangeID != "" {
		return errors.New("syncservice: exported change contains delivery identity")
	}
	return nil
}

// BindDelivery adds the authenticated origin and deterministic change identity.
func BindDelivery(change ChangeEnvelope, origin string) (ChangeEnvelope, error) {
	if err := change.Validate(false); err != nil {
		return ChangeEnvelope{}, err
	}
	if origin == "" || strings.ContainsAny(origin, "\x00\r\n") {
		return ChangeEnvelope{}, errors.New("syncservice: delivery origin is invalid")
	}
	change.Origin = origin
	h := sha256.New()
	for _, value := range []string{
		"synckit.syncservice.change.v1", change.ServiceID, change.SchemaFingerprint,
		origin, string(change.Kind), string(change.BaseRevision), string(change.SourceRevision), change.PayloadDigest,
	} {
		_, _ = h.Write([]byte(strconv.Itoa(len(value))))
		_, _ = h.Write([]byte{':'})
		_, _ = h.Write([]byte(value))
	}
	change.ChangeID = hex.EncodeToString(h.Sum(nil))
	return change, change.Validate(true)
}

// ValidateServiceSchema checks an exact service ID and schema fingerprint pair.
func ValidateServiceSchema(serviceID, fingerprint string) error {
	if err := serviceidentity.ValidateName(serviceID); err != nil {
		return fmt.Errorf("syncservice: service id: %w", err)
	}
	if !exactDigest(fingerprint) {
		return errors.New("syncservice: schema fingerprint must be lowercase sha256")
	}
	return nil
}

func exactDigest(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// WatchItem is one tracked unit of sync: a stable id, the directories whose changes
// trigger a sync, and a fingerprint of its current state. A consumer that can tell
// an item is mid-operation reports it busy with a human-readable reason, so a
// watcher defers acting on it until it goes idle.
type WatchItem struct {
	ID          string   `json:"id"`
	WatchDirs   []string `json:"watch_dirs"`
	Fingerprint string   `json:"fingerprint"`
	Busy        bool     `json:"busy,omitempty"`
	BusyReason  string   `json:"busy_reason,omitempty"`
}

// Capabilities is a peer's self-description: its name and the method names it serves.
type Capabilities struct {
	Name    string   `json:"name"`
	Methods []string `json:"methods"`
}

// ReconcileResult reports the outcome of a reconcile: how many items converged and
// how many were skipped because they were busy.
type ReconcileResult struct {
	Converged   int `json:"converged"`
	SkippedBusy int `json:"skipped_busy,omitempty"`
}

// SyncConsumer is the typed sync surface a consumer implements and the daemon serves
// over rpc. [RegisterConsumer] binds each method to the matching rpc handler.
type SyncConsumer interface {
	// Capabilities reports this consumer's name and methods.
	Capabilities(ctx context.Context) (Capabilities, error)
	// List enumerates the items this consumer tracks for sync.
	List(ctx context.Context) ([]WatchItem, error)
	// Reconcile converges this consumer against the named origin host.
	Reconcile(ctx context.Context, origin string) (ReconcileResult, error)
	// Export returns an immutable full or delta change for one acknowledged revision.
	Export(ctx context.Context, request ExportRequest) (ChangeEnvelope, error)
	// Apply merges one immutable source change and returns its exact acknowledgement.
	Apply(ctx context.Context, change ChangeEnvelope) (ApplyResult, error)
}
