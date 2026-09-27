package syncservice

import (
	"context"

	"github.com/yasyf/synckit/artifact"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
)

// ExportV2 asks the source service for one immutable change that may carry
// artifact roots.
func (c *Client) ExportV2(ctx context.Context, request ExportRequest) (ChangeEnvelope, error) {
	if err := request.Validate(); err != nil {
		return ChangeEnvelope{}, err
	}
	var out ChangeEnvelope
	if err := c.callStruct(ctx, MethodExportV2, request, &out); err != nil {
		return ChangeEnvelope{}, err
	}
	return out, out.Validate(false)
}

// ApplyV2 delivers one immutable change that may carry artifact roots, for
// a transfer admitted under the receiver's RestrictedEpoch admitted. The
// receiver acknowledges SourceRevision only once its own store holds every
// root closure; otherwise the result is Partial, Stale, or NeedSnapshot. A
// policy refusal returns its *artifact.PausedError as the error.
func (c *Client) ApplyV2(ctx context.Context, change ChangeEnvelope, admitted uint64) (ApplyResult, error) {
	if err := change.Validate(true); err != nil {
		return ApplyResult{}, err
	}
	params, err := structParams(change)
	if err != nil {
		return ApplyResult{}, err
	}
	params[artifact.AdmittedParam] = admitted
	var out ApplyResult
	if err := c.call(ctx, &rpc.Request{Method: MethodApplyV2, Params: params}, &out); err != nil {
		return ApplyResult{}, err
	}
	return out, pausedErr(out.Paused)
}

// NetStatus asks the consumer host for its live network State.
func (c *Client) NetStatus(ctx context.Context) (netpolicy.State, error) {
	var out artifact.NetStatusResult
	err := c.call(ctx, &rpc.Request{Method: artifact.MethodNetStatus}, &out)
	return out.State, err
}

// ArtifactClosure asks for one page of the closure of params.Roots.
func (c *Client) ArtifactClosure(ctx context.Context, params artifact.ClosureParams) (artifact.ClosurePage, error) {
	if err := params.Validate(); err != nil {
		return artifact.ClosurePage{}, err
	}
	var out artifact.ClosurePage
	err := c.callStruct(ctx, artifact.MethodClosure, params, &out)
	return out, err
}

// ArtifactHave returns the digests the store lacks, in query order, for a
// transfer admitted under the receiver's RestrictedEpoch admitted. A policy
// refusal returns its *artifact.PausedError as the error.
func (c *Client) ArtifactHave(ctx context.Context, digests []artifact.Digest, admitted uint64) ([]artifact.Digest, error) {
	params := artifact.HaveParams{Digests: digests, Admitted: admitted}
	if err := params.Validate(); err != nil {
		return nil, err
	}
	var out artifact.HaveResult
	if err := c.callStruct(ctx, artifact.MethodHave, params, &out); err != nil {
		return nil, err
	}
	return out.Missing, pausedErr(out.Paused)
}

// BatchBuild asks the source store to build an outbox batch of objects, each
// packed as its declared kind.
func (c *Client) BatchBuild(ctx context.Context, objects []artifact.ObjectEntry) (artifact.BatchDescriptor, error) {
	params := artifact.BatchBuildParams{Objects: objects}
	if err := params.Validate(); err != nil {
		return artifact.BatchDescriptor{}, err
	}
	var out artifact.BatchDescriptor
	if err := c.callStruct(ctx, artifact.MethodBatchBuild, params, &out); err != nil {
		return artifact.BatchDescriptor{}, err
	}
	return out, out.Validate()
}

// BatchRead reads one outbox part's compressed bytes.
func (c *Client) BatchRead(ctx context.Context, id artifact.Digest, index int) ([]byte, error) {
	params := artifact.BatchReadParams{ID: id, Index: index}
	if err := params.Validate(); err != nil {
		return nil, err
	}
	var out artifact.BatchReadResult
	err := c.callStruct(ctx, artifact.MethodBatchRead, params, &out)
	return out.Data, err
}

// BatchDrop removes one outbox batch.
func (c *Client) BatchDrop(ctx context.Context, id artifact.Digest) error {
	params := artifact.BatchRef{ID: id}
	if err := params.Validate(); err != nil {
		return err
	}
	return c.callStruct(ctx, artifact.MethodBatchDrop, params, nil)
}

// BatchBegin stages batch on the receiver, declaring sender as this host's
// live State and admitted as the receiver's RestrictedEpoch the transfer was
// admitted under. A policy refusal returns the result, whose Peer is the
// receiver's live State, with its *artifact.PausedError as the error.
func (c *Client) BatchBegin(ctx context.Context, batch artifact.BatchDescriptor, sender netpolicy.State, admitted uint64) (artifact.BatchBeginResult, error) {
	params := artifact.BatchBeginParams{Batch: batch, Sender: sender, Admitted: admitted}
	if err := params.Validate(); err != nil {
		return artifact.BatchBeginResult{}, err
	}
	var out artifact.BatchBeginResult
	if err := c.callStruct(ctx, artifact.MethodBatchBegin, params, &out); err != nil {
		return artifact.BatchBeginResult{}, err
	}
	return out, pausedErr(out.Paused)
}

// BatchPut writes part index of batch id on the receiver, declaring sender
// as this host's live State and admitted as the receiver's RestrictedEpoch
// the transfer was admitted under. A policy refusal returns the result, whose
// Peer is the receiver's live State, with its *artifact.PausedError as the
// error.
func (c *Client) BatchPut(ctx context.Context, id artifact.Digest, index int, data []byte, sender netpolicy.State, admitted uint64) (artifact.BatchPutResult, error) {
	params := artifact.BatchPutParams{ID: id, Index: index, Data: data, Sender: sender, Admitted: admitted}
	if err := params.Validate(); err != nil {
		return artifact.BatchPutResult{}, err
	}
	var out artifact.BatchPutResult
	if err := c.callStruct(ctx, artifact.MethodBatchPut, params, &out); err != nil {
		return artifact.BatchPutResult{}, err
	}
	return out, pausedErr(out.Paused)
}

// BatchCommit verifies and stores every object of a fully staged batch, for
// a transfer admitted under the receiver's RestrictedEpoch admitted. A policy
// refusal returns its *artifact.PausedError as the error.
func (c *Client) BatchCommit(ctx context.Context, id artifact.Digest, admitted uint64) (artifact.CommitReport, error) {
	params := artifact.BatchCommitParams{ID: id, Admitted: admitted}
	if err := params.Validate(); err != nil {
		return artifact.CommitReport{}, err
	}
	var out artifact.BatchCommitResult
	if err := c.callStruct(ctx, artifact.MethodBatchCommit, params, &out); err != nil {
		return artifact.CommitReport{}, err
	}
	return out.CommitReport, pausedErr(out.Paused)
}

// PinsSet replaces owner's pinned roots on the consumer store; empty roots
// removes the pin set.
func (c *Client) PinsSet(ctx context.Context, owner string, roots []artifact.Ref) error {
	params := artifact.PinsSetParams{Owner: owner, Roots: roots}
	if err := params.Validate(); err != nil {
		return err
	}
	return c.callStruct(ctx, artifact.MethodPinsSet, params, nil)
}

func (c *Client) callStruct(ctx context.Context, method string, value, out any) error {
	params, err := structParams(value)
	if err != nil {
		return err
	}
	return c.call(ctx, &rpc.Request{Method: method, Params: params}, out)
}

func pausedErr(paused *artifact.PausedError) error {
	if paused == nil {
		return nil
	}
	if err := paused.Code.Validate(); err != nil {
		return err
	}
	return paused
}
