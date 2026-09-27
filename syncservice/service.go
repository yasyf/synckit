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

	"github.com/yasyf/synckit/artifact"
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
	// MethodExportV2 exports one change that may carry artifact roots.
	MethodExportV2 = "synckit.syncservice.export.v2"
	// MethodApplyV2 applies one change once the receiver's own store holds
	// every artifact root closure it acknowledges.
	MethodApplyV2 = "synckit.syncservice.apply.v2"
)

const (
	changeDomainV1 = "synckit.syncservice.change.v1"
	changeDomainV2 = "synckit.syncservice.change.v2"
)

// ErrArtifactsOnV1 refuses a change carrying artifact roots on a v1 method,
// so an old peer can never silently strip them.
var ErrArtifactsOnV1 = errors.New("syncservice: artifact change on a v1 method")

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
// Artifacts lists the payload's artifact roots in consumer priority order;
// the exporter derives them deterministically from the payload, and
// BindDelivery binds them into ChangeID.
type ChangeEnvelope struct {
	ServiceID         string         `json:"service_id"`
	SchemaFingerprint string         `json:"schema_fingerprint"`
	Origin            string         `json:"origin"`
	ChangeID          string         `json:"change_id"`
	Kind              ChangeKind     `json:"kind"`
	BaseRevision      Revision       `json:"base_revision"`
	SourceRevision    Revision       `json:"source_revision"`
	PayloadDigest     string         `json:"payload_digest"`
	Payload           []byte         `json:"payload"`
	Artifacts         []artifact.Ref `json:"artifacts,omitempty"`
}

// ApplyResult acknowledges one source revision or requests a full snapshot.
// The v2 fields report a receiver that already holds a receipt at or past
// the source revision (Stale, with that receipt's HeldDigest), or one that
// recorded the change without every artifact root complete (Partial, with
// AckedRevision still the prior receipt). Paused is set, and nothing was
// applied, when the v2 receiver's live network State refused the call.
type ApplyResult struct {
	AckedRevision Revision              `json:"acked_revision"`
	NeedSnapshot  bool                  `json:"need_snapshot,omitempty"`
	Stale         bool                  `json:"stale,omitempty"`
	HeldDigest    string                `json:"held_digest,omitempty"`
	Partial       bool                  `json:"partial,omitempty"`
	Paused        *artifact.PausedError `json:"paused,omitempty"`
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

// NewExportedArtifactChange constructs and validates one source-owned change
// carrying roots, the artifact roots derived from payload in priority order.
func NewExportedArtifactChange(
	serviceID, schemaFingerprint string,
	kind ChangeKind,
	baseRevision, sourceRevision Revision,
	payload []byte,
	roots []artifact.Ref,
) (ChangeEnvelope, error) {
	change, err := NewExportedChange(serviceID, schemaFingerprint, kind, baseRevision, sourceRevision, payload)
	if err != nil {
		return ChangeEnvelope{}, err
	}
	change.Artifacts = append([]artifact.Ref(nil), roots...)
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
	if source == 0 {
		return errors.New("syncservice: source revision must be non-zero")
	}
	if source < base {
		return errors.New("syncservice: source revision must not precede base revision")
	}
	if e.Kind != ChangeSnapshot && e.Kind != ChangeDelta {
		return errors.New("syncservice: change kind is invalid")
	}
	if e.Kind == ChangeSnapshot && base != 0 {
		return errors.New("syncservice: snapshot base revision must be zero")
	}
	if e.Kind == ChangeDelta && len(e.Artifacts) > 0 {
		return errors.New("syncservice: a change carrying artifacts must be a snapshot")
	}
	if len(e.Payload) == 0 || len(e.Payload) > MaxTransferPayload || !json.Valid(e.Payload) {
		return errors.New("syncservice: payload must be bounded valid JSON")
	}
	digest := sha256.Sum256(e.Payload)
	if e.PayloadDigest != hex.EncodeToString(digest[:]) {
		return errors.New("syncservice: payload digest mismatch")
	}
	if err := artifact.ValidateRoots(e.Artifacts); err != nil {
		return fmt.Errorf("syncservice: artifacts: %w", err)
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

// BindDelivery adds the authenticated origin and deterministic change
// identity. A change without artifacts hashes the v1 domain; one with
// artifacts hashes the v2 domain over the same inputs plus the root count and
// each root's kind, digest, and size in order.
func BindDelivery(change ChangeEnvelope, origin string) (ChangeEnvelope, error) {
	if err := change.Validate(false); err != nil {
		return ChangeEnvelope{}, err
	}
	if origin == "" || strings.ContainsAny(origin, "\x00\r\n") {
		return ChangeEnvelope{}, errors.New("syncservice: delivery origin is invalid")
	}
	change.Origin = origin
	values := []string{
		changeDomainV1, change.ServiceID, change.SchemaFingerprint,
		origin, string(change.Kind), string(change.BaseRevision), string(change.SourceRevision), change.PayloadDigest,
	}
	if len(change.Artifacts) > 0 {
		values[0] = changeDomainV2
		values = append(values, strconv.Itoa(len(change.Artifacts)))
		for _, root := range change.Artifacts {
			values = append(values, string(root.Kind)+":"+string(root.Digest)+":"+strconv.FormatInt(root.Size, 10))
		}
	}
	h := sha256.New()
	for _, value := range values {
		_, _ = h.Write([]byte(strconv.Itoa(len(value))))
		_, _ = h.Write([]byte{':'})
		_, _ = h.Write([]byte(value))
	}
	change.ChangeID = hex.EncodeToString(h.Sum(nil))
	return change, change.Validate(true)
}

func (e ChangeEnvelope) refuseArtifacts() error {
	if len(e.Artifacts) > 0 {
		return ErrArtifactsOnV1
	}
	return nil
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

// ArtifactConsumer is a SyncConsumer whose changes carry artifact roots over
// the v2 export and apply methods.
type ArtifactConsumer interface {
	SyncConsumer
	// ExportArtifacts returns an immutable change whose Artifacts are derived
	// deterministically from its payload.
	ExportArtifacts(ctx context.Context, request ExportRequest) (ChangeEnvelope, error)
	// ApplyArtifacts applies change given ready, the roots whose closures the
	// receiver's own store holds. It must re-derive the roots from the payload
	// and refuse a change whose Artifacts differ, and may acknowledge
	// SourceRevision only when every root is ready.
	ApplyArtifacts(ctx context.Context, change ChangeEnvelope, ready []artifact.Ref) (ApplyResult, error)
}
