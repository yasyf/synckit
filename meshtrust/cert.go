package meshtrust

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// MintCert mints a TLS certificate and key for dnsName into dir by running
// `tailscale cert`, returning the certificate and key file paths. The name is
// normalized first (lowercased, trailing dot stripped), so a raw MagicDNS name
// is accepted as-is.
func MintCert(ctx context.Context, dnsName, dir string) (string, string, error) {
	return mintCert(ctx, tailscaleCert, dnsName, dir)
}

func mintCert(ctx context.Context, run func(ctx context.Context, certFile, keyFile, host string) error, dnsName, dir string) (string, string, error) {
	host := normalizeHost(dnsName)
	if err := validateHost(host); err != nil {
		return "", "", err
	}
	certFile := filepath.Join(dir, host+".crt")
	keyFile := filepath.Join(dir, host+".key")
	if err := run(ctx, certFile, keyFile, host); err != nil {
		return "", "", err
	}
	// A flag-swallowed positional can make the CLI exit 0 without minting;
	// never return success without the artifacts.
	for _, f := range []string{certFile, keyFile} {
		if _, err := os.Stat(f); err != nil {
			return "", "", fmt.Errorf("tailscale cert %s: missing output: %w", host, err)
		}
	}
	return certFile, keyFile, nil
}

// validateHost rejects a normalized name that is empty or not a plausible DNS
// name — anything but [a-z0-9.-], a leading '-' or '.', or an empty label —
// closing option injection and path traversal at the API boundary.
func validateHost(host string) error {
	if host == "" {
		return errors.New("mint cert: empty DNS name")
	}
	if host[0] == '-' || host[0] == '.' || strings.Contains(host, "..") {
		return fmt.Errorf("mint cert: invalid DNS name %q", host)
	}
	for _, r := range host {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '.' && r != '-' {
			return fmt.Errorf("mint cert: invalid DNS name %q", host)
		}
	}
	return nil
}

func tailscaleCert(ctx context.Context, certFile, keyFile, host string) error {
	args := []string{"cert", "--cert-file", certFile, "--key-file", keyFile, host}
	_, err := exec.CommandContext(ctx, "tailscale", args...).Output() //nolint:gosec // G204: fixed tailscale argv; host passed validateHost, paths are the caller's own dir.
	if errors.Is(err, exec.ErrNotFound) {
		_, err = exec.CommandContext(ctx, appBundleTailscale, args...).Output() //nolint:gosec // G204: same argv against the fixed app-bundle path.
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// A ctx kill surfaces as the kill signal's exit error, hiding the
			// cancellation; report ctx.Err() so errors.Is sees it.
			return fmt.Errorf("tailscale cert %s: %w", host, ctxErr)
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && len(exitErr.Stderr) > 0 {
			return fmt.Errorf("tailscale cert %s: %w: %s", host, err, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return fmt.Errorf("tailscale cert %s: %w", host, err)
	}
	return nil
}
