// Package phoneapprover is synckitd's phone approver for attesting consent:
// it parks one request under a random id, serves it over a tailnet TLS
// listener only to the phone's tailnet node and transport key, wakes the
// phone with a push that names the Mac and never the command, and waits for
// one answer. It carries messages only; the requester's verifier checks the
// signature the phone returns.
package phoneapprover

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/yasyf/synckit/consent"
)

// PathPrefix is where a parked request is served, matching cc-sudo's
// PendingApproval.path(id:).
const PathPrefix = "/cc-sudo/phone/requests/"

const maxBody = 64 << 10

// Notification is the push a phone gets: the asking Mac's name, the request
// id, and the base URL to fetch it from.
type Notification struct {
	Mac string
	ID  string
	URL string
}

// Approver implements consent.PhoneTransport over injected boundaries, so a
// test drives it without a tailnet, a certificate or Apple.
type Approver struct {
	// Listen opens a TLS listener reachable by the phone and returns it with
	// the base URL the push names.
	Listen func(ctx context.Context) (net.Listener, string, error)
	// WhoIs names the tailnet node behind a connection's remote address.
	WhoIs func(ctx context.Context, remoteAddr string) (string, error)
	Push  func(ctx context.Context, token string, n Notification) error
	// Window bounds how long a parked request waits for the phone.
	Window time.Duration
}

type answer struct {
	ID      string `json:"id"`
	Nonce   string `json:"nonce"`
	Verdict string `json:"verdict"`
	KeyID   string `json:"key_id"`
	Sig     string `json:"sig"`
}

// Ask parks ask for phone and waits for its answer.
func (a *Approver) Ask(ctx context.Context, phone consent.Phone, ask consent.PhoneAsk) (*consent.Reply, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(raw)
	ln, base, err := a.Listen(ctx)
	if err != nil {
		return nil, &consent.AuthRequired{Msg: fmt.Sprintf("phone %s: open the tailnet listener: %v", phone.Name, err)}
	}
	p := &parked{phone: phone, ask: ask, id: id, whois: a.WhoIs, answers: make(chan answer, 1)}
	srv := &http.Server{Handler: p, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	if err := a.Push(ctx, phone.PushToken, Notification{Mac: ask.Origin, ID: id, URL: base}); err != nil {
		return nil, &consent.AuthRequired{Msg: fmt.Sprintf("phone %s: push: %v", phone.Name, err)}
	}
	wctx, cancel := context.WithTimeout(ctx, a.Window)
	defer cancel()
	select {
	case ans := <-p.answers:
		if ans.Verdict == "denied" {
			return nil, &consent.Denied{Peer: phone.Name}
		}
		return &consent.Reply{Status: "approved", Nonce: ans.Nonce, KeyID: ans.KeyID, Sig: ans.Sig, SignedBy: phone.Name}, nil
	case <-wctx.Done():
		return nil, &consent.AuthRequired{Msg: fmt.Sprintf("phone %s did not answer: %v", phone.Name, wctx.Err())}
	}
}

type parked struct {
	phone   consent.Phone
	ask     consent.PhoneAsk
	id      string
	whois   func(ctx context.Context, remoteAddr string) (string, error)
	answers chan answer

	mu       sync.Mutex
	answered bool
}

func (p *parked) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != PathPrefix+p.id {
		http.NotFound(w, r)
		return
	}
	node, err := p.whois(r.Context(), r.RemoteAddr)
	if err != nil || node != p.phone.NodeID {
		http.Error(w, "not the enrolled phone", http.StatusForbidden)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	if !authorized(p.phone.TransportKey, r.Header.Get("Authorization"), r.Method, p.id, body) {
		http.Error(w, "bad transport signature", http.StatusUnauthorized)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.answered {
		http.Error(w, "already answered", http.StatusGone)
		return
	}
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": p.id, "argv": p.ask.Argv, "nonce": p.ask.Nonce})
	case http.MethodPost:
		var ans answer
		if err := json.Unmarshal(body, &ans); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		valid := ans.ID == p.id && ans.Nonce == p.ask.Nonce &&
			(ans.Verdict == "denied" || (ans.Verdict == "approved" && ans.KeyID != "" && ans.Sig != ""))
		if !valid {
			http.Error(w, "answer does not match this request", http.StatusBadRequest)
			return
		}
		p.answered = true
		p.answers <- ans
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
