package artifact

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/rpc"
)

// Register binds every artifact method on d to s, each dispatched
// concurrently. have, batch.begin, and batch.put return the typed refusal,
// before decoding their params, while monitor's live State is not
// unrestricted. batch.begin and batch.put then evaluate that State against
// the sender's declared State on every call and, unless both are
// unrestricted, return the typed refusal without writing; both report the
// receiver's live State.
func Register(d *rpc.Dispatcher, s *Store, monitor netpolicy.Monitor) {
	d.Register(MethodNetStatus, func(context.Context, map[string]any) (any, error) {
		state, _ := monitor.Current()
		return NetStatusResult{State: state}, nil
	})
	handle(d, MethodClosure, func(ctx context.Context, p ClosureParams) (any, error) {
		return s.closurePage(ctx, p)
	})
	d.Register(MethodHave, func(ctx context.Context, raw map[string]any) (any, error) {
		if refusal := LiveRefusal(monitor); refusal != nil {
			return HaveResult{Missing: []Digest{}, Paused: refusal}, nil
		}
		var p HaveParams
		if err := decodeParams(raw, &p); err != nil {
			return nil, err
		}
		if err := p.Validate(); err != nil {
			return nil, err
		}
		missing, err := s.Has(ctx, p.Digests)
		if err != nil {
			return nil, err
		}
		return HaveResult{Missing: missing}, nil
	})
	handle(d, MethodBatchBuild, func(ctx context.Context, p BatchBuildParams) (any, error) {
		return s.BuildBatch(ctx, p.Objects)
	})
	handle(d, MethodBatchRead, func(ctx context.Context, p BatchReadParams) (any, error) {
		data, err := s.ReadPart(ctx, p.ID, p.Index)
		if err != nil {
			return nil, err
		}
		return BatchReadResult{Data: data}, nil
	})
	handle(d, MethodBatchDrop, func(ctx context.Context, p BatchRef) (any, error) {
		return struct{}{}, s.DropBatch(ctx, p.ID)
	})
	d.Register(MethodBatchBegin, func(ctx context.Context, raw map[string]any) (any, error) {
		live, _ := monitor.Current()
		if !live.Unrestricted() {
			return BatchBeginResult{HaveParts: []int{}, Peer: live, Paused: PausedFor(netpolicy.Evaluate(live, live))}, nil
		}
		p, err := decode[BatchBeginParams](raw)
		if err != nil {
			return nil, err
		}
		if verdict := netpolicy.Evaluate(live, p.Sender); !verdict.Allowed {
			return BatchBeginResult{HaveParts: []int{}, Peer: live, Paused: PausedFor(verdict)}, nil
		}
		held, err := s.BeginBatch(ctx, p.Batch)
		if err != nil {
			return nil, err
		}
		return BatchBeginResult{HaveParts: held, Peer: live}, nil
	})
	d.Register(MethodBatchPut, func(ctx context.Context, raw map[string]any) (any, error) {
		live, _ := monitor.Current()
		if !live.Unrestricted() {
			return BatchPutResult{Peer: live, Paused: PausedFor(netpolicy.Evaluate(live, live))}, nil
		}
		p, err := decode[BatchPutParams](raw)
		if err != nil {
			return nil, err
		}
		if verdict := netpolicy.Evaluate(live, p.Sender); !verdict.Allowed {
			return BatchPutResult{Peer: live, Paused: PausedFor(verdict)}, nil
		}
		if err := s.PutPart(ctx, p.ID, p.Index, p.Data); err != nil {
			return nil, err
		}
		return BatchPutResult{Peer: live}, nil
	})
	handle(d, MethodBatchCommit, func(ctx context.Context, p BatchRef) (any, error) {
		return s.CommitBatch(ctx, p.ID)
	})
	handle(d, MethodPinsSet, func(ctx context.Context, p PinsSetParams) (any, error) {
		return struct{}{}, s.SetPins(ctx, p.Owner, p.Roots)
	})
}

func handle[P interface{ Validate() error }](d *rpc.Dispatcher, method string, call func(context.Context, P) (any, error)) {
	d.Register(method, func(ctx context.Context, raw map[string]any) (any, error) {
		params, err := decode[P](raw)
		if err != nil {
			return nil, err
		}
		return call(ctx, params)
	})
}

func decode[P interface{ Validate() error }](raw map[string]any) (P, error) {
	var params P
	if err := decodeParams(raw, &params); err != nil {
		return params, err
	}
	return params, params.Validate()
}

func decodeParams(params map[string]any, target any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("artifact: encode params: %w", err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("%w: decode params: %w", ErrInvalid, err)
	}
	return nil
}

func (s *Store) closurePage(ctx context.Context, p ClosureParams) (ClosurePage, error) {
	closure, err := s.closure(ctx, p.Roots, DefaultClosureBound)
	if err != nil {
		return ClosurePage{}, err
	}
	total := len(closure.Objects)
	if p.After > total {
		return ClosurePage{}, fmt.Errorf("%w: closure page after %d past %d objects", ErrInvalid, p.After, total)
	}
	end := min(p.After+p.Limit, total)
	return ClosurePage{
		Objects:      closure.Objects[p.After:end],
		Next:         end,
		Done:         end == total,
		TotalObjects: total,
		TotalBytes:   closure.Bytes,
	}, nil
}
