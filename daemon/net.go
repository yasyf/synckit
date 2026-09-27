package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/netpolicy"
	"github.com/yasyf/synckit/syncservice"
)

const (
	netFirstObservationTimeout = time.Second
	netPeerTimeout             = 10 * time.Second
)

var newNetMonitor = netpolicy.NewMonitor

type netReport struct {
	Local netpolicy.State `json:"local"`
	Peers []netPeer       `json:"peers"`
}

type netPeer struct {
	Host    string           `json:"host"`
	Service string           `json:"service,omitempty"`
	State   *netpolicy.State `json:"state,omitempty"`
	Verdict *netVerdict      `json:"verdict,omitempty"`
	Error   string           `json:"error,omitempty"`
}

type netVerdict struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason,omitempty"`
}

func newNetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "net",
		Short: "Inspect or override the network cost policy that gates bulk artifact transfer.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newNetStatusCmd(), newNetMeteredCmd())
	return cmd
}

func newNetStatusCmd() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Print this host's network state and each reachable mesh peer's, with the bulk transfer verdict.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withCLIProcessScope(cmd.Context(), func(owned *daemonkit.Owned) error {
				report, err := netStatus(cmd.Context(), owned)
				if err != nil {
					return err
				}
				if asJSON {
					encoder := json.NewEncoder(cmd.OutOrStdout())
					encoder.SetIndent("", "  ")
					return encoder.Encode(report)
				}
				printNetReport(cmd, report)
				return nil
			})
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the report as JSON")
	return cmd
}

func newNetMeteredCmd() *cobra.Command {
	return &cobra.Command{
		Use:       "metered on|off",
		Short:     "Mark every network this host joins as metered, pausing bulk artifact transfer, or clear the mark.",
		Args:      cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		ValidArgs: []string{"on", "off"},
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := netpolicy.ManualPath()
			if err != nil {
				return err
			}
			if err := netpolicy.SaveManual(path, netpolicy.Manual{Metered: args[0] == "on"}); err != nil {
				return err
			}
			cmd.Println("metered: " + args[0])
			return nil
		},
	}
}

func netStatus(ctx context.Context, scope processScope) (netReport, error) {
	path, err := netpolicy.ManualPath()
	if err != nil {
		return netReport{}, err
	}
	monitor, err := newNetMonitor(path)
	if err != nil {
		return netReport{}, fmt.Errorf("start network monitor: %w", err)
	}
	defer func() { _ = monitor.Close() }()
	local := firstObservation(ctx, monitor)
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return netReport{}, fmt.Errorf("load mesh: %w", err)
	}
	manifests, err := discoverManifests()
	if err != nil {
		return netReport{}, err
	}
	peers := make([]netPeer, len(reg.Hosts))
	var wg sync.WaitGroup
	for i, host := range reg.Hosts {
		wg.Go(func() {
			peers[i] = peerNetwork(ctx, scope, local, reg.Self, host, manifests)
		})
	}
	wg.Wait()
	return netReport{Local: local, Peers: peers}, nil
}

func firstObservation(ctx context.Context, monitor netpolicy.Monitor) netpolicy.State {
	state, changed := monitor.Current()
	if !state.ObservedAt.IsZero() {
		return state
	}
	timer := time.NewTimer(netFirstObservationTimeout)
	defer timer.Stop()
	select {
	case <-changed:
		state, _ = monitor.Current()
	case <-timer.C:
	case <-ctx.Done():
	}
	return state
}

func peerNetwork(
	ctx context.Context,
	scope processScope,
	local netpolicy.State,
	self, peer string,
	manifests []manifest.Manifest,
) netPeer {
	if len(manifests) == 0 {
		return netPeer{Host: peer, Error: "no registered service reaches this peer"}
	}
	errs := make([]error, 0, len(manifests))
	for _, m := range manifests {
		state, err := peerNetStatus(ctx, scope, m, self, peer)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", m.Name, err))
			continue
		}
		verdict := netpolicy.Evaluate(local, state)
		return netPeer{
			Host:    peer,
			Service: m.Name,
			State:   &state,
			Verdict: &netVerdict{Allowed: verdict.Allowed, Reason: verdict.Reason},
		}
	}
	return netPeer{Host: peer, Error: errors.Join(errs...).Error()}
}

func peerNetStatus(ctx context.Context, scope processScope, m manifest.Manifest, self, peer string) (netpolicy.State, error) {
	ctx, cancel := context.WithTimeout(ctx, netPeerTimeout)
	defer cancel()
	client := syncservice.NewClient(dialTransport(scope, m, peer, self))
	defer func() { _ = client.Close() }()
	return client.NetStatus(ctx)
}

func printNetReport(cmd *cobra.Command, report netReport) {
	cmd.Println("local: " + describeNetwork(report.Local))
	for _, peer := range report.Peers {
		if peer.State == nil {
			cmd.Printf("peer %s: unreachable: %s\n", peer.Host, peer.Error)
			continue
		}
		verdict := "allowed"
		if !peer.Verdict.Allowed {
			verdict = "paused (" + peer.Verdict.Reason + ")"
		}
		cmd.Printf("peer %s: %s via %s: %s\n", peer.Host, describeNetwork(*peer.State), peer.Service, verdict)
	}
}

func describeNetwork(state netpolicy.State) string {
	parts := []string{string(state.Status)}
	for _, flag := range []struct {
		set  bool
		name string
	}{
		{state.Cellular, "cellular"},
		{state.Expensive, "expensive"},
		{state.Constrained, "constrained"},
		{state.ManualMetered, "manual-metered"},
	} {
		if flag.set {
			parts = append(parts, flag.name)
		}
	}
	return strings.Join(parts, ", ")
}
