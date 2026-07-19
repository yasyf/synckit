package meshtrust

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	fixtureCertPEM = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
	// Not a real key shape: the tests only assert byte round-trips, and a
	// PRIVATE KEY marker would trip the detect-private-key commit hook.
	fixtureKeyPEM = "-----BEGIN TEST KEY-----\nMIIB\n-----END TEST KEY-----\n"
)

func writeFixturePEMs(certFile, keyFile string) error {
	if err := os.WriteFile(certFile, []byte(fixtureCertPEM), 0o600); err != nil {
		return err
	}
	return os.WriteFile(keyFile, []byte(fixtureKeyPEM), 0o600)
}

func TestMintCert(t *testing.T) {
	tests := []struct {
		name     string
		dnsName  string
		run      func(ctx context.Context, certFile, keyFile, host string) error
		wantHost string
		wantIs   error
		wantMsg  string
	}{
		{
			name:    "success",
			dnsName: "yasyf-home.tail71af5d.ts.net",
			run: func(_ context.Context, certFile, keyFile, _ string) error {
				return writeFixturePEMs(certFile, keyFile)
			},
			wantHost: "yasyf-home.tail71af5d.ts.net",
		},
		{
			name:    "trailing dot normalized",
			dnsName: "Yasyf-Home.tail71af5d.ts.net.",
			run: func(_ context.Context, certFile, keyFile, host string) error {
				if host != "yasyf-home.tail71af5d.ts.net" {
					return fmt.Errorf("unexpected host %q", host)
				}
				return writeFixturePEMs(certFile, keyFile)
			},
			wantHost: "yasyf-home.tail71af5d.ts.net",
		},
		{
			name:    "cli not found",
			dnsName: "yasyf-home.tail71af5d.ts.net",
			run: func(_ context.Context, _, _, host string) error {
				return fmt.Errorf("tailscale cert %s: %w", host, exec.ErrNotFound)
			},
			wantIs:  exec.ErrNotFound,
			wantMsg: "yasyf-home.tail71af5d.ts.net",
		},
		{
			name:    "cli error carries stderr",
			dnsName: "yasyf-home.tail71af5d.ts.net",
			run: func(_ context.Context, _, _, host string) error {
				return fmt.Errorf("tailscale cert %s: exit status 1: %s", host, "certificate access denied: HTTPS certificate support is not enabled")
			},
			wantMsg: "HTTPS certificate support is not enabled",
		},
		{
			name:    "empty name rejected",
			dnsName: ".",
			wantMsg: "empty DNS name",
		},
		{
			name:    "option injection rejected",
			dnsName: "--help",
			wantMsg: "invalid DNS name",
		},
		{
			name:    "path traversal rejected",
			dnsName: "../evil",
			wantMsg: "invalid DNS name",
		},
		{
			name:    "empty label rejected",
			dnsName: "foo..bar.ts.net",
			wantMsg: "invalid DNS name",
		},
		{
			name:    "bad character rejected",
			dnsName: "foo_bar.ts.net",
			wantMsg: "invalid DNS name",
		},
		{
			name:    "cert not written",
			dnsName: "yasyf-home.tail71af5d.ts.net",
			run: func(_ context.Context, _, keyFile, _ string) error {
				return os.WriteFile(keyFile, []byte(fixtureKeyPEM), 0o600)
			},
			wantMsg: ".crt",
		},
		{
			name:    "key not written",
			dnsName: "yasyf-home.tail71af5d.ts.net",
			run: func(_ context.Context, certFile, _, _ string) error {
				return os.WriteFile(certFile, []byte(fixtureCertPEM), 0o600)
			},
			wantMsg: ".key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			certFile, keyFile, err := mintCert(context.Background(), tt.run, tt.dnsName, dir)
			if tt.wantHost == "" {
				if err == nil {
					t.Fatal("mintCert() error = nil, want error")
				}
				if tt.wantIs != nil && !errors.Is(err, tt.wantIs) {
					t.Errorf("mintCert() error = %v, want errors.Is %v", err, tt.wantIs)
				}
				if tt.wantMsg != "" && !strings.Contains(err.Error(), tt.wantMsg) {
					t.Errorf("mintCert() error = %q, want containing %q", err, tt.wantMsg)
				}
				if certFile != "" || keyFile != "" {
					t.Errorf("mintCert() paths = (%q, %q), want empty on error", certFile, keyFile)
				}
				return
			}
			if err != nil {
				t.Fatalf("mintCert() error = %v", err)
			}
			wantCert := filepath.Join(dir, tt.wantHost+".crt")
			wantKey := filepath.Join(dir, tt.wantHost+".key")
			if certFile != wantCert {
				t.Errorf("certFile = %q, want %q", certFile, wantCert)
			}
			if keyFile != wantKey {
				t.Errorf("keyFile = %q, want %q", keyFile, wantKey)
			}
			cert, err := os.ReadFile(certFile) //nolint:gosec // G304: test reads a file it wrote in its own temp dir.
			if err != nil {
				t.Fatalf("read cert: %v", err)
			}
			if string(cert) != fixtureCertPEM {
				t.Errorf("cert contents = %q, want %q", cert, fixtureCertPEM)
			}
			key, err := os.ReadFile(keyFile) //nolint:gosec // G304: test reads a file it wrote in its own temp dir.
			if err != nil {
				t.Fatalf("read key: %v", err)
			}
			if string(key) != fixtureKeyPEM {
				t.Errorf("key contents = %q, want %q", key, fixtureKeyPEM)
			}
		})
	}
}

// TestTailscaleCertRunner exercises the real tailscaleCert runner against a
// stub `tailscale` executable prepended to PATH.
func TestTailscaleCertRunner(t *testing.T) {
	writeStub := func(t *testing.T, script string) {
		t.Helper()
		bin := t.TempDir()
		if err := os.WriteFile(filepath.Join(bin, "tailscale"), []byte(script), 0o700); err != nil { //nolint:gosec // G306: an executable test stub must be +x.
			t.Fatalf("write stub: %v", err)
		}
		t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	}

	t.Run("argv and artifacts", func(t *testing.T) {
		dir := t.TempDir()
		argsFile := filepath.Join(t.TempDir(), "argv")
		writeStub(t, fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$@\" > %q\nprintf 'stub-cert' > \"$3\"\nprintf 'stub-key' > \"$5\"\n", argsFile))

		certFile, keyFile, err := MintCert(context.Background(), "Yasyf-Home.tail71af5d.ts.net.", dir)
		if err != nil {
			t.Fatalf("MintCert() error = %v", err)
		}
		wantCert := filepath.Join(dir, "yasyf-home.tail71af5d.ts.net.crt")
		wantKey := filepath.Join(dir, "yasyf-home.tail71af5d.ts.net.key")
		if certFile != wantCert || keyFile != wantKey {
			t.Errorf("MintCert() = (%q, %q), want (%q, %q)", certFile, keyFile, wantCert, wantKey)
		}
		argv, err := os.ReadFile(argsFile) //nolint:gosec // G304: test reads a file its own stub wrote in a temp dir.
		if err != nil {
			t.Fatalf("read argv: %v", err)
		}
		wantArgv := strings.Join([]string{
			"cert", "--cert-file", wantCert, "--key-file", wantKey, "yasyf-home.tail71af5d.ts.net",
		}, "\n") + "\n"
		if string(argv) != wantArgv {
			t.Errorf("argv = %q, want %q", argv, wantArgv)
		}
		for f, want := range map[string]string{certFile: "stub-cert", keyFile: "stub-key"} {
			got, err := os.ReadFile(f) //nolint:gosec // G304: test reads a file its own stub wrote in a temp dir.
			if err != nil {
				t.Fatalf("read %s: %v", f, err)
			}
			if string(got) != want {
				t.Errorf("%s contents = %q, want %q", f, got, want)
			}
		}
	})

	t.Run("failure carries stderr", func(t *testing.T) {
		writeStub(t, "#!/bin/sh\necho 'certificate access denied: HTTPS certificate support is not enabled' >&2\nexit 1\n")
		_, _, err := MintCert(context.Background(), "yasyf-home.tail71af5d.ts.net", t.TempDir())
		if err == nil {
			t.Fatal("MintCert() error = nil, want error")
		}
		if !strings.Contains(err.Error(), "HTTPS certificate support is not enabled") {
			t.Errorf("MintCert() error = %q, want stub stderr in message", err)
		}
	})

	t.Run("exit zero without artifacts", func(t *testing.T) {
		writeStub(t, "#!/bin/sh\nexit 0\n")
		_, _, err := MintCert(context.Background(), "yasyf-home.tail71af5d.ts.net", t.TempDir())
		if err == nil || !strings.Contains(err.Error(), "missing output") {
			t.Errorf("MintCert() error = %v, want missing-output error", err)
		}
	})

	t.Run("cancellation identity preserved", func(t *testing.T) {
		// exec, so the kill lands on the sleeping process itself rather than
		// leaving a grandchild holding the output pipe open for 5s.
		writeStub(t, "#!/bin/sh\nexec sleep 5\n")
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, _, err := MintCert(ctx, "yasyf-home.tail71af5d.ts.net", t.TempDir())
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("MintCert() error = %v, want errors.Is(context.DeadlineExceeded)", err)
		}
	})
}
