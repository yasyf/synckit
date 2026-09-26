package daemon

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/yasyf/synckit/delivery"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/syncservice"
)

const harnessPoll = 5 * time.Millisecond

// HarnessHost is one in-process host of a Harness mesh.
type HarnessHost struct {
	// Name is the host's mesh name; the host runs a lane per service to every
	// other host in the mesh.
	Name string
	// StateDir holds the host's delivery-v2 state, the directory synckitd keeps
	// under hostregistry.Mesh.Dir().
	StateDir string
	// Monitor is the host's own network monitor, the one its lanes gate on.
	Monitor netpolicy.Monitor
	// Services maps each service the host runs to a transport reaching its local
	// resident consumer. The Harness owns and closes these transports.
	Services map[string]syncservice.Transport
}

// HarnessConfig describes the hosts and links of a Harness mesh.
type HarnessConfig struct {
	// Hosts lists every host of the mesh.
	Hosts []HarnessHost
	// Manifests declares every service a host names in its Services.
	Manifests []manifest.Manifest
	// Links returns the transport host from dials to reach service's consumer on
	// host to. Every delivery attempt dials a fresh transport and closes it
	// afterwards, so tests inject faults or cut a link here.
	Links func(from, to, service string) syncservice.Transport
	// ArtifactMaxWait is how long a kicked artifact lane coalesces further kicks
	// before it runs; synckitd waits 10 s.
	ArtifactMaxWait time.Duration
	// RetryInterval is how long a paused or failed lane waits before its next
	// attempt; synckitd re-checks a pause after 60 s and backs a failure off
	// from 30 s to 5 min.
	RetryInterval time.Duration
}

// Harness runs synckitd's delivery scheduler for every host of an in-process
// mesh: the real per-(service, peer) workers, deliverOnce, and delivery-v2
// store, with SSH, launchd, and the host registry replaced by the transports
// and monitors of a HarnessConfig. Consumers use it to write multi-host
// delivery acceptance tests.
type Harness struct {
	ctx        context.Context
	cancel     context.CancelFunc
	wg         *sync.WaitGroup
	schedulers map[string]*deliveryScheduler
	clients    []*syncservice.Client
}

// NewHarness starts a delivery scheduler per host of cfg. Like synckitd after a
// reload, every lane runs once at start; ctx bounds the whole harness.
func NewHarness(ctx context.Context, cfg HarnessConfig) (*Harness, error) {
	if cfg.RetryInterval <= 0 {
		return nil, fmt.Errorf("harness: RetryInterval %s is not positive", cfg.RetryInterval)
	}
	manifests := make(map[string]manifest.Manifest, len(cfg.Manifests))
	for _, m := range cfg.Manifests {
		if err := m.Validate(); err != nil {
			return nil, fmt.Errorf("harness manifest %q: %w", m.Name, err)
		}
		manifests[m.Name] = m
	}
	names := make([]string, 0, len(cfg.Hosts))
	for _, host := range cfg.Hosts {
		if host.StateDir == "" {
			return nil, fmt.Errorf("harness host %q has no StateDir", host.Name)
		}
		if slices.Contains(names, host.Name) {
			return nil, fmt.Errorf("harness host %q is listed twice", host.Name)
		}
		for service := range host.Services {
			if _, ok := manifests[service]; !ok {
				return nil, fmt.Errorf("harness host %q runs service %q with no manifest", host.Name, service)
			}
		}
		names = append(names, host.Name)
	}

	hctx, cancel := context.WithCancel(ctx)
	h := &Harness{ctx: hctx, cancel: cancel, wg: &sync.WaitGroup{}, schedulers: make(map[string]*deliveryScheduler, len(cfg.Hosts))}
	timing := deliveryTiming{
		backoffBase: cfg.RetryInterval, backoffMax: cfg.RetryInterval,
		pauseRecheck: cfg.RetryInterval, artifactMaxWait: cfg.ArtifactMaxWait,
	}
	for _, host := range cfg.Hosts {
		s := newDeliveryScheduler(hctx, h.wg, nil, newDeliveryStore(host.StateDir), host.Monitor, host.Name)
		s.timing = timing
		s.dial = func(m manifest.Manifest, peer string) syncservice.Transport {
			return cfg.Links(host.Name, peer, m.Name)
		}
		s.snapshot = func(context.Context, string) {}
		for _, service := range slices.Sorted(maps.Keys(host.Services)) {
			local := syncservice.NewClient(host.Services[service])
			h.clients = append(h.clients, local)
			s.add(manifests[service], local, names)
		}
		h.schedulers[host.Name] = s
	}
	for _, name := range names {
		h.schedulers[name].start()
	}
	return h, nil
}

// Kick marks from's lanes dirty the way synckitd's manifest notifier does after
// a local change: an artifact lane runs once ArtifactMaxWait has passed since
// the first unrun kick, a v1 lane runs at once. A paused or failed lane only
// restages and keeps its RetryInterval. An empty service or to selects every
// service or peer.
func (h *Harness) Kick(service, from, to string) error {
	s, err := h.scheduler(from)
	if err != nil {
		return err
	}
	return s.Kick(service, to)
}

// Status reports host's lanes for service ("" for every service) as synckitd's
// delivery.status does.
func (h *Harness) Status(host, service string) ([]delivery.PeerStatus, error) {
	s, err := h.scheduler(host)
	if err != nil {
		return nil, err
	}
	return s.status(h.ctx, service)
}

// WaitIdle blocks until host's lane for service to peer has no kick queued and
// no attempt running, and rests either idle (its staged change acknowledged, or
// nothing to send) or paused, and returns that status. A failed lane keeps
// retrying every RetryInterval while WaitIdle waits; ctx bounds the wait.
func (h *Harness) WaitIdle(ctx context.Context, host, service, peer string) (delivery.PeerStatus, error) {
	s, err := h.scheduler(host)
	if err != nil {
		return delivery.PeerStatus{}, err
	}
	l := s.lanes[laneKey{service: service, peer: peer}]
	if l == nil {
		return delivery.PeerStatus{}, fmt.Errorf("harness host %q has no lane for service %q to %q", host, service, peer)
	}
	ticker := time.NewTicker(harnessPoll)
	defer ticker.Stop()
	for {
		settled := l.settled()
		statuses, err := s.status(ctx, service)
		if err != nil {
			return delivery.PeerStatus{}, err
		}
		status := statuses[slices.IndexFunc(statuses, func(p delivery.PeerStatus) bool { return p.Peer == peer })]
		if settled && (status.State == delivery.StateIdle || status.State == delivery.StatePaused) {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return status, fmt.Errorf("harness: wait for %s's lane for %q to %q: %w", host, service, peer, ctx.Err())
		case <-ticker.C:
		}
	}
}

// Close stops every host's scheduler, waits for in-flight attempts to unwind,
// and closes the hosts' local service transports.
func (h *Harness) Close() error {
	h.cancel()
	h.wg.Wait()
	errs := make([]error, 0, len(h.clients))
	for _, c := range h.clients {
		errs = append(errs, c.Close())
	}
	return errors.Join(errs...)
}

func (h *Harness) scheduler(host string) (*deliveryScheduler, error) {
	s, ok := h.schedulers[host]
	if !ok {
		return nil, fmt.Errorf("harness has no host %q", host)
	}
	return s, nil
}
