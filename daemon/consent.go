package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/synckit/authkit"
	"github.com/yasyf/synckit/consent"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/presence"
	"github.com/yasyf/synckit/rpc"
)

// consentPromptTimeout is the client read deadline for a consent.request or
// consent.relay round trip. The daemon handler may block a full
// rpc.DispatchTimeout on a Touch ID sheet, so the client must outwait it —
// a shorter deadline would abandon a request the human is still deciding.
const consentPromptTimeout = rpc.DispatchTimeout + time.Minute

// consentProbeTimeout bounds a consent.presence read: a fast console-session
// snapshot that never waits on a human.
const consentProbeTimeout = 10 * time.Second

// execSSHRunner adapts hostregistry.ExecSSH to consent.Runner: the routed relay
// leg and the peer liveness probe both cross the mesh over ssh, with brew's
// shellenv sourced remotely so synckitd resolves on a non-interactive peer.
type execSSHRunner struct{}

func (execSSHRunner) Run(ctx context.Context, target, remoteCmd string, stdin []byte) (string, error) {
	return hostregistry.ExecSSH(ctx, target, remoteCmd, stdin)
}

// buildConsentEngine constructs the consent engine over the live mesh: the
// signed authkit helper prompts locally, presence.Session probes this host's
// console, the router walks the mesh peers over ssh, and selfIdentity +
// resolvePeers read the registry fresh per request. A package var so a test
// swaps in fake collaborators without an installed helper or a real console.
var buildConsentEngine = defaultConsentEngine

func defaultConsentEngine() *consent.Engine {
	router := consent.NewRouter(execSSHRunner{}, consent.PresenceCommand)
	return consent.NewEngine(selfIdentity, authkit.Gate{}, presence.Session, router, resolvePeers)
}

// selfIdentity resolves this host's mesh identity — reloaded each call, like
// resolvePeers, so a daemon that started before the mesh bootstrap stamps and
// forwards the live identity rather than a startup snapshot.
func selfIdentity(context.Context) (string, error) {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return "", fmt.Errorf("resolve consent self identity: %w", err)
	}
	return reg.Self, nil
}

// resolvePeers resolves the routed-consent approver candidates — every mesh peer
// but this host — reloaded each call so a host registered mid-run is a candidate
// on the next routed request.
func resolvePeers(context.Context) ([]string, error) {
	reg, err := hostregistry.Mesh.Load()
	if err != nil {
		return nil, fmt.Errorf("resolve consent approvers: %w", err)
	}
	return reg.Hosts, nil
}

// registerConsent binds consent.request|relay|presence onto d over a freshly
// built engine, via plain consent.Register — NEVER RegisterExclusive, since a
// 10-minute Touch ID prompt behind the exclusive mutex would wedge the reconcile
// and reload handlers that share it.
func registerConsent(d *rpc.Dispatcher) {
	consent.Register(d, buildConsentEngine())
}

// newConsentCmd builds the consent command tree and documents the FROZEN consent
// wire Phase 4 (cookiesync) and Phase 5 (cc-sudo) build against — extend these
// shapes, never repurpose them.
//
// consent.request (local consumer surface; the requestor is server-derived as
// "sid:"+PeerSID, never client-supplied):
//
//	params  {client, reason, subject, argv?, nonce?, ttl_ms?, local_only?}
//	result  {verdict, approved_by, routed, cached, granted_until?, attestation?}
//
// verdict ∈ approved|denied|unavailable; a fatal local prompt is an RPC error
// ({ok:false}), never a routable unavailable. argv/nonce/attestation are the
// cc-sudo attestation extension (optional; cookiesync omits them for verdict-only
// behavior). attestation is {key_id, sig, signed_by}; the Secure Enclave signs
// nonce ‖ subject_bytes, where
// subject_bytes = sha256(canonical(argv) ‖ 0x00 ‖ utf8(origin_host)) and
// origin_host is the requested_from value ("" for a local, non-routed request).
// synckitd forwards argv+nonce+requested_from opaquely and verifies nothing.
// An attestation request ALWAYS prompts and signs fresh — grants are
// verdict-only: ttl_ms records and serves a grant only for a request without
// argv, since a cached verdict carries no signature over a new nonce.
//
// consent.relay (approver leg a routed walk shells as `synckitd consent relay`,
// the request fed as one JSON line on stdin so command text stays off the remote
// argv):
//
//	stdin  {client, reason, subject, nonce, endpoint, origin, argv?, sign_nonce?}
//	reply  {status, nonce, endpoint, key_id?, sig?, signed_by?}
//
// An approval echoes the request's nonce+endpoint verbatim; denied and
// unavailable are the bare {status}. The leg never routes onward and never
// exits 255 (see newConsentRelayCmd).
//
// consent.presence takes no params and answers a presence.SessionSnapshot:
// {on_console, locked, console_user, screen_shared}.
func newConsentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "consent",
		Short: "Gate a privileged action behind mesh consent (request), or answer a peer's routed ask (relay, presence).",
		Args:  cobra.NoArgs,
		RunE:  func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}
	cmd.AddCommand(newConsentRequestCmd(), newConsentRelayCmd(), newConsentPresenceCmd())
	return cmd
}

func newConsentRequestCmd() *cobra.Command {
	var (
		client    string
		reason    string
		subject   string
		nonce     string
		argv      []string
		ttlMs     int
		localOnly bool
	)
	cmd := &cobra.Command{
		Use:   "request",
		Short: "Ask the local daemon to gate a privileged action, prompting here or routing to a live peer.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			params := map[string]any{
				"client":     client,
				"reason":     reason,
				"subject":    subject,
				"ttl_ms":     float64(ttlMs),
				"local_only": localOnly,
			}
			if nonce != "" {
				params["nonce"] = nonce
			}
			if len(argv) > 0 {
				params["argv"] = argv
			}
			resp, err := callDaemon(cmd.Context(), consent.MethodRequest, params, consentPromptTimeout)
			if err != nil {
				return err
			}
			if !resp.OK {
				return fmt.Errorf("consent.request: %s", resp.Error)
			}
			return printJSON(cmd.OutOrStdout(), resp.Result)
		},
	}
	cmd.Flags().StringVar(&client, "client", "", "Consumer name recorded with the request (e.g. cc-sudo).")
	cmd.Flags().StringVar(&reason, "reason", "", "Human-readable reason shown in a verdict-only prompt.")
	cmd.Flags().StringVar(&subject, "subject", "", "Grant subject the approval authorizes (e.g. sha256:...).")
	cmd.Flags().StringVar(&nonce, "nonce", "", "Attestation signing nonce; with --argv it requests a signature.")
	cmd.Flags().StringArrayVar(&argv, "argv", nil, "Exact command the helper hashes, displays, and signs (repeatable).")
	cmd.Flags().IntVar(&ttlMs, "ttl-ms", 0, "Grant lifetime in milliseconds; 0 prompts on every call.")
	cmd.Flags().BoolVar(&localOnly, "local-only", false, "Pin the prompt to this host; never route to a peer.")
	return cmd
}

// newConsentRelayCmd builds the ssh-invoked remote hop of a routed consent. It
// must never exit 255: synckit's ssh runner reads 255 as a connection failure
// and fails over to the next dial address, which would summon a SECOND Touch ID
// sheet for one approval. runRelay maps every failure to an unavailable reply
// and exits 0 — the origin routes around on the reply's status, not this exit.
func newConsentRelayCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "relay",
		Short: "Answer a peer's routed consent on this host, reading the request as JSON on stdin.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRelay(cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout())
		},
	}
}

// runRelay forwards a routed consent from in to the local daemon's consent.relay
// and writes the peer's reply to out. A malformed request, an unreachable
// daemon, or a fatal handler response all resolve to an unavailable reply so the
// origin routes to another peer — the relay leg never surfaces a per-peer failure
// as a non-zero (and never a 255) exit that would trip ssh failover into a double
// prompt. A genuine denial is preserved verbatim, so a human's "no" stays
// terminal rather than routing around.
func runRelay(ctx context.Context, in io.Reader, out io.Writer) error {
	reply, err := relayReply(ctx, in)
	if err != nil {
		reply = map[string]any{"status": "unavailable"}
	}
	return printJSON(out, reply)
}

// relayReply reads the relay-request JSON from in and returns the local daemon's
// consent.relay result. A read/parse failure, a transport failure, or a fatal
// handler response is an error the caller folds into unavailable.
func relayReply(ctx context.Context, in io.Reader) (any, error) {
	raw, err := io.ReadAll(io.LimitReader(in, rpc.MaxLine))
	if err != nil {
		return nil, fmt.Errorf("read relay request: %w", err)
	}
	var params map[string]any
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, fmt.Errorf("parse relay request: %w", err)
	}
	resp, err := callDaemon(ctx, consent.MethodRelay, params, consentPromptTimeout)
	if err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("consent.relay: %s", resp.Error)
	}
	return resp.Result, nil
}

func newConsentPresenceCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "presence",
		Short: "Print this host's console-session snapshot, the liveness a routed walk probe-gates on.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := callDaemon(cmd.Context(), consent.MethodPresence, nil, consentProbeTimeout)
			if err != nil {
				return err
			}
			if !resp.OK {
				return fmt.Errorf("consent.presence: %s", resp.Error)
			}
			return printJSON(cmd.OutOrStdout(), resp.Result)
		},
	}
}

// callDaemon sends one consent RPC to the local daemon socket under a read
// deadline and returns its response.
func callDaemon(ctx context.Context, method string, params map[string]any, timeout time.Duration) (*rpc.Response, error) {
	sock, err := hostregistry.Mesh.SockPath()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return rpc.Call(ctx, sock, &rpc.Request{Method: method, Params: params})
}

// printJSON writes v as one compact JSON line to w.
func printJSON(w io.Writer, v any) error {
	line, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	_, err = fmt.Fprintln(w, string(line))
	return err
}
