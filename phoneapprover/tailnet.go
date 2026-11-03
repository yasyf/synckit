package phoneapprover

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"

	"github.com/yasyf/synckit/meshtrust"
)

// TailnetListener mints this Mac's tailnet certificate into certDir and opens
// a TLS listener on an ephemeral port of its tailnet IPv4 address, so the
// phone fetches over HTTPS from the Mac's own MagicDNS name.
func TailnetListener(provider *meshtrust.Provider, certDir string) func(ctx context.Context) (net.Listener, string, error) {
	return func(ctx context.Context) (net.Listener, string, error) {
		dnsName := provider.SelfDNSName(ctx)
		if dnsName == "" {
			return nil, "", errors.New("tailscale is not running")
		}
		var addr netip.Addr
		for _, candidate := range provider.SelfAddrs(ctx) {
			if candidate.Is4() {
				addr = candidate
				break
			}
		}
		if !addr.IsValid() {
			return nil, "", errors.New("no tailnet IPv4 address")
		}
		certFile, keyFile, err := meshtrust.MintCert(ctx, dnsName, certDir)
		if err != nil {
			return nil, "", err
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, "", fmt.Errorf("load tailnet certificate: %w", err)
		}
		ln, err := net.Listen("tcp", netip.AddrPortFrom(addr, 0).String())
		if err != nil {
			return nil, "", err
		}
		port := ln.Addr().(*net.TCPAddr).Port
		config := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
		return tls.NewListener(ln, config), fmt.Sprintf("https://%s:%d", dnsName, port), nil
	}
}
