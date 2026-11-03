package meshtrust

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
)

// WhoIsNode returns the StableID of the tailnet node behind remoteAddr, a
// host:port as an http.Request carries it, via `tailscale whois`.
func WhoIsNode(ctx context.Context, remoteAddr string) (string, error) {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return "", fmt.Errorf("tailscale whois %s: %w", remoteAddr, err)
	}
	binary, err := tailscaleBinary()
	if err != nil {
		return "", err
	}
	out, err := exec.CommandContext(ctx, binary, "whois", "--json", host).Output() //nolint:gosec // G204: fixed tailscale argv; host is a parsed peer address.
	if err != nil {
		return "", fmt.Errorf("tailscale whois %s: %w", host, err)
	}
	var who struct {
		Node struct {
			StableID string `json:"StableID"`
		} `json:"Node"`
	}
	if err := json.Unmarshal(out, &who); err != nil {
		return "", fmt.Errorf("parse tailscale whois %s: %w", host, err)
	}
	if who.Node.StableID == "" {
		return "", fmt.Errorf("tailscale whois %s: no node", host)
	}
	return who.Node.StableID, nil
}
