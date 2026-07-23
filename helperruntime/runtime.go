// Package helperruntime composes Synckit's fixed wire and stop-control
// ownership around one consumer helper's product resources.
package helperruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/internal/runtimehealth"
	"github.com/yasyf/synckit/internal/runtimeowner"
	"github.com/yasyf/synckit/internal/serviceidentity"
	"github.com/yasyf/synckit/rpc"
)

// App identifies the consumer helper generation installed by Synckit.
type App struct {
	Name         string
	RuntimeBuild string
}

// Config supplies only product-owned resources for one embedded helper.
type Config struct {
	App       App
	Socket    string
	Server    *rpc.Server
	Workers   dkdaemon.Workers
	State     io.Closer
	Resources dkdaemon.Resources
	Activate  func(dkdaemon.Activation) error
	Drain     func() error
}

var newRuntime = wire.NewRuntime

// New constructs a helper runtime with Synckit's fixed protocol, health,
// admission, stop authority, and durable service process store.
func New(config Config) (*dkdaemon.Runtime, error) {
	if _, err := serviceidentity.HelperLabel(config.App.Name); err != nil {
		return nil, fmt.Errorf("helperruntime: app name: %w", err)
	}
	if config.App.RuntimeBuild == "" {
		return nil, errors.New("helperruntime: runtime build is required")
	}
	if config.Socket == "" {
		return nil, errors.New("helperruntime: socket is required")
	}
	if config.Server == nil {
		return nil, errors.New("helperruntime: RPC server is required")
	}
	if config.Workers == nil {
		return nil, errors.New("helperruntime: workers are required")
	}
	if config.State == nil {
		return nil, errors.New("helperruntime: state is required")
	}
	if config.Resources == nil {
		return nil, errors.New("helperruntime: resources are required")
	}
	if config.Activate == nil {
		return nil, errors.New("helperruntime: activate is required")
	}
	if config.Drain == nil {
		return nil, errors.New("helperruntime: drain is required")
	}
	classifier, verifier, err := runtimeowner.StopAuthority()
	if err != nil {
		return nil, err
	}
	var runtime *dkdaemon.Runtime
	runtime, err = newRuntime(wire.RuntimeConfig{
		Socket: config.Socket, RuntimeBuild: config.App.RuntimeBuild, RuntimeProtocol: int(rpc.Version),
		Wire: config.Server.Wire, Classifier: classifier, ReservedProtectedSessions: 1,
		StopVerifier: verifier,
		Observations: []wire.ObservationRoute{runtimehealth.Observation(func(ctx context.Context) (dkdaemon.Health, error) {
			return runtime.Health(ctx)
		})},
		Admission: &settlingAdmission{intake: &drain.Intake{}, drain: config.Drain},
		Workers:   config.Workers, State: config.State,
		Resources: config.Resources, Activate: config.Activate,
	})
	return runtime, err
}

type settlingAdmission struct {
	intake *drain.Intake
	drain  func() error
	once   sync.Once
	err    error
}

func (a *settlingAdmission) Admit() (func(), error) { return a.intake.Admit() }

func (a *settlingAdmission) AdmitProtected() (func(), error) { return a.intake.AdmitProtected() }

func (a *settlingAdmission) Close() { a.intake.Close() }

func (a *settlingAdmission) Draining() bool { return a.intake.Draining() }

func (a *settlingAdmission) Settle(ctx context.Context) error {
	a.once.Do(func() { a.err = a.drain() })
	return errors.Join(a.err, a.intake.Settle(ctx))
}
