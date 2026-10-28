package daemon

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRestageUpgradeInstallsOnlyADifferentInstalledBuild(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(self) //nolint:gosec // the test binary itself
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		installed []byte
		want      bool
	}{
		{name: "absent"},
		{name: "current", installed: current},
		{name: "upgraded", installed: []byte("#!/bin/sh\n"), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useHome(t)
			useMesh(t)
			dir := t.TempDir()
			t.Setenv("PATH", dir)
			program := filepath.Join(dir, daemonBinary)
			if tc.installed != nil {
				if err := os.WriteFile(program, tc.installed, 0o700); err != nil { //nolint:gosec // test-owned temp dir
					t.Fatal(err)
				}
			}
			var started []string
			startInstall = func(program string) error {
				started = append(started, program)
				return nil
			}
			t.Cleanup(func() { startInstall = launchInstall })

			restaged, err := restageUpgrade()
			if err != nil {
				t.Fatalf("restageUpgrade: %v", err)
			}
			if restaged != tc.want {
				t.Fatalf("restaged = %v, want %v", restaged, tc.want)
			}
			if !tc.want {
				if len(started) != 0 {
					t.Fatalf("started install of %v, want none", started)
				}
				return
			}
			if len(started) != 1 || started[0] != program {
				t.Fatalf("started install of %v, want [%s]", started, program)
			}
		})
	}
}
