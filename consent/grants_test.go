package consent

import (
	"testing"
	"time"
)

func TestGrantCoversEverySubject(t *testing.T) {
	g := NewGrants()
	g.Grant("sid:501", []string{"sha256:aaaa", "sha256:bbbb"}, time.Hour)

	for _, subject := range []string{"sha256:aaaa", "sha256:bbbb"} {
		until, ok := g.Granted("sid:501", subject)
		if !ok {
			t.Fatalf("Granted(sid:501, %s) = false, want the fresh grant", subject)
		}
		if remaining := time.Until(until); remaining < 59*time.Minute || remaining > time.Hour {
			t.Fatalf("grant expiry %v away, want ~1h", remaining)
		}
	}
	if _, ok := g.Granted("sid:502", "sha256:aaaa"); ok {
		t.Fatal("a grant must never cover another requestor")
	}
	if _, ok := g.Granted("sid:501", "sha256:cccc"); ok {
		t.Fatal("a grant must never cover an ungranted subject")
	}
}

func TestGrantedPrunesExpired(t *testing.T) {
	g := NewGrants()
	g.Grant("sid:501", []string{"sha256:aaaa"}, -time.Millisecond)

	if _, ok := g.Granted("sid:501", "sha256:aaaa"); ok {
		t.Fatal("an expired grant must not authorize")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.grants) != 0 {
		t.Fatalf("expired grants left in the store: %v", g.grants)
	}
}

func TestCapOnlyShortens(t *testing.T) {
	g := NewGrants()
	g.Grant("sid:501", []string{"sha256:aaaa"}, time.Hour)

	g.Cap("sid:501", "sha256:aaaa", time.Minute)
	until, ok := g.Granted("sid:501", "sha256:aaaa")
	if !ok {
		t.Fatal("Cap must not revoke the grant")
	}
	if remaining := time.Until(until); remaining > time.Minute || remaining < 59*time.Second {
		t.Fatalf("capped expiry %v away, want ~1m", remaining)
	}

	g.Cap("sid:501", "sha256:aaaa", 2*time.Hour)
	after, ok := g.Granted("sid:501", "sha256:aaaa")
	if !ok {
		t.Fatal("a no-op Cap must not revoke the grant")
	}
	if !after.Equal(until) {
		t.Fatalf("Cap moved the expiry from %v to %v; a longer TTL is a no-op", until, after)
	}

	g.Cap("sid:502", "sha256:aaaa", time.Minute)
	if _, ok := g.Granted("sid:502", "sha256:aaaa"); ok {
		t.Fatal("Cap must never create a grant")
	}
}
