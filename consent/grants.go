package consent

import (
	"sync"
	"time"
)

type grantKey struct {
	requestor string
	subject   string
}

// Grants is the in-memory requestor+subject grant store: one approval
// authorizes its requestor for every subject it covered until the grant
// expires. Authorization is per requesting principal, never global — a subject
// is served silently only while the requestor holds a live grant for it — and
// grants are process memory only: a restart clears them.
type Grants struct {
	mu     sync.Mutex
	grants map[grantKey]time.Time
}

// NewGrants returns an empty grant store.
func NewGrants() *Grants {
	return &Grants{grants: map[grantKey]time.Time{}}
}

// Granted reports whether requestor holds a live grant for subject and when it
// expires, pruning every expired grant on the way.
func (g *Grants) Granted(requestor, subject string) (time.Time, bool) {
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	for key, expiry := range g.grants {
		if now.After(expiry) {
			delete(g.grants, key)
		}
	}
	expiry, ok := g.grants[grantKey{requestor, subject}]
	return expiry, ok
}

// Grant authorizes requestor for every subject an approval covered, expiring
// after ttl.
func (g *Grants) Grant(requestor string, subjects []string, ttl time.Duration) {
	expiry := time.Now().Add(ttl)
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, subject := range subjects {
		g.grants[grantKey{requestor, subject}] = expiry
	}
}

// Cap shortens requestor's live grant for subject to expire no later than
// now + ttl. It only ever moves an expiry earlier — never creating or
// extending authority — and is a no-op when no grant exists.
func (g *Grants) Cap(requestor, subject string, ttl time.Duration) {
	capped := time.Now().Add(ttl)
	g.mu.Lock()
	defer g.mu.Unlock()
	key := grantKey{requestor, subject}
	if expiry, ok := g.grants[key]; ok && expiry.After(capped) {
		g.grants[key] = capped
	}
}
