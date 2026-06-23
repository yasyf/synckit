// Package codec holds the config-free JSON codecs shared across synckit tools.
//
// Duration is the canonical Go-duration string codec: it marshals via
// time.Duration.String() so a 15-minute interval is always "15m0s", never "15m"
// or "900s", keeping the on-disk form byte-stable across hosts and tools.
package codec

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration is a time.Duration that marshals to and from a canonical Go duration
// string such as "15m0s".
type Duration time.Duration

// MarshalJSON encodes the duration as a canonical Go duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON decodes a Go duration string, rejecting anything unparseable.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("decode duration: %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}
