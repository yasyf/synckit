package authkit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// fakeHelper writes an executable shell script standing in for the helper.
func fakeHelper(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "authkit")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil { //nolint:gosec // test stub must be executable
		t.Fatalf("write fake helper: %v", err)
	}
	return path
}

func TestRunReportsContractExitCodes(t *testing.T) {
	tests := []struct {
		name string
		code int
	}{
		{"approved", CodeApproved},
		{"denied", CodeDenied},
		{"unavailable", CodeUnavailable},
		{"screen-locked", CodeScreenLocked},
		{"caller-rejected", CodeCallerRejected},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := Bridge{Binary: fakeHelper(t, fmt.Sprintf("exit %d", tc.code))}
			res, err := b.Run(context.Background(), nil, nil, "consent")
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if res.Code != tc.code {
				t.Fatalf("Code = %d, want %d", res.Code, tc.code)
			}
		})
	}
}

func TestRunCapturesStdoutStderrAndFeedsStdin(t *testing.T) {
	b := Bridge{Binary: fakeHelper(t, `cat; printf 'diag' >&2; exit 1`)}
	res, err := b.Run(context.Background(), []byte("payload"), nil, "consent-sign")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Code != 1 || string(res.Stdout) != "payload" || string(res.Stderr) != "diag" {
		t.Fatalf("Result = code %d stdout %q stderr %q, want 1 / payload / diag", res.Code, res.Stdout, res.Stderr)
	}
}

func TestRunAppendsExtraEnv(t *testing.T) {
	b := Bridge{Binary: fakeHelper(t, `printf '%s' "$AUTHKIT_REASON"`)}
	res, err := b.Run(context.Background(), nil, []string{"AUTHKIT_REASON=tap to approve"}, "consent")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if string(res.Stdout) != "tap to approve" {
		t.Fatalf("stdout = %q, want the reason env passed through", res.Stdout)
	}
}

func TestRunSignalDeathIsAnError(t *testing.T) {
	b := Bridge{Binary: fakeHelper(t, `kill -KILL $$`)}
	_, err := b.Run(context.Background(), nil, nil, "consent")
	if err == nil {
		t.Fatal("a signal-killed helper must be a run error, not a contract exit code")
	}
}

func TestRunFailsClosedWithoutHelper(t *testing.T) {
	t.Setenv(HelperEnvVar, filepath.Join(t.TempDir(), "missing-authkit"))
	_, err := Bridge{}.Run(context.Background(), nil, nil, "consent")
	var helperErr *HelperError
	if !errors.As(err, &helperErr) {
		t.Fatalf("Run without an installed helper = %v, want *HelperError", err)
	}
}
