package codec

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDurationMarshalCanonicalBytes(t *testing.T) {
	cases := []struct {
		id   string
		in   time.Duration
		want string
	}{
		{"minutes", 15 * time.Minute, `"15m0s"`},
		{"seconds", 3 * time.Second, `"3s"`},
		{"idle", 5 * time.Minute, `"5m0s"`},
		{"hours", 24 * time.Hour, `"24h0m0s"`},
		{"composite", 2 * time.Minute, `"2m0s"`},
		{"zero", 0, `"0s"`},
	}
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) {
			out, err := json.Marshal(Duration(c.in))
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if string(out) != c.want {
				t.Fatalf("Marshal = %s, want exact %s", out, c.want)
			}
		})
	}
}

func TestDurationRoundTrip(t *testing.T) {
	cases := []struct {
		id string
		in time.Duration
	}{
		{"minutes", 15 * time.Minute},
		{"seconds", 3 * time.Second},
		{"hours", 24 * time.Hour},
		{"sub-second", 250 * time.Millisecond},
	}
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) {
			out, err := json.Marshal(Duration(c.in))
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var back Duration
			if err := json.Unmarshal(out, &back); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if time.Duration(back) != c.in {
				t.Fatalf("round-trip = %s, want %s", time.Duration(back), c.in)
			}
		})
	}
}

func TestDurationUnmarshalInvalid(t *testing.T) {
	cases := []struct {
		id  string
		raw string
	}{
		{"garbage", `"notaduration"`},
		{"number", `15`},
		{"empty", `""`},
		{"object", `{}`},
	}
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) {
			var d Duration
			if err := json.Unmarshal([]byte(c.raw), &d); err == nil {
				t.Fatalf("Unmarshal(%s): want error, got nil", c.raw)
			}
		})
	}
}
