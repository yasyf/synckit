package authkit

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	helperCask       = "authkit"
	helperApp        = "authkit.app"
	helperExecutable = "authkit"
)

// HelperEnvVar overrides helper discovery with an explicit path to the helper
// executable when set.
const HelperEnvVar = "AUTHKIT_HELPER"

// HelperError reports that the signed authkit helper is not installed; callers
// fail closed on it rather than degrading to an unsigned fallback.
type HelperError struct {
	Path string
}

func (e *HelperError) Error() string {
	return fmt.Sprintf("authkit helper not found at %s; run 'brew install --cask authkit' to fetch the signed helper", e.Path)
}

// brewPrefixes returns the Homebrew prefixes whose Caskroom may hold the staged helper
// bundle. An explicit HOMEBREW_PREFIX is authoritative; otherwise both platform defaults
// are searched.
func brewPrefixes() []string {
	if p := os.Getenv("HOMEBREW_PREFIX"); p != "" {
		return []string{p}
	}
	return []string{"/opt/homebrew", "/usr/local"}
}

// HelperAppPath returns the authkit.app bundle. The cask stages it
// (stage_only) so the whole bundle lives intact in the Homebrew Caskroom —
// never /Applications — keeping its bundle-relative provisioning profile
// alongside the binary so the Secure Enclave keychain-access-group stays
// authorized. A *HelperError reports a bundle that is not staged.
func HelperAppPath() (string, error) {
	for _, prefix := range brewPrefixes() {
		matches, _ := filepath.Glob(filepath.Join(prefix, "Caskroom", helperCask, "*", helperApp))
		for _, app := range matches {
			if info, err := os.Stat(filepath.Join(app, "Contents", "MacOS", helperExecutable)); err == nil && info.Mode().IsRegular() {
				return app, nil
			}
		}
	}
	return "", &HelperError{Path: filepath.Join(brewPrefixes()[0], "Caskroom", helperCask)}
}

// HelperBinary returns the signed helper's inner executable, e.g.
// …/authkit.app/Contents/MacOS/authkit. AUTHKIT_HELPER overrides discovery
// with an explicit path.
func HelperBinary() (string, error) {
	if p := os.Getenv(HelperEnvVar); p != "" {
		return p, nil
	}
	app, err := HelperAppPath()
	if err != nil {
		return "", err
	}
	return filepath.Join(app, "Contents", "MacOS", helperExecutable), nil
}

// RequireHelper returns the signed helper executable, or a *HelperError if it
// is not installed. The Secure-Enclave and consent operations run inside a
// Developer-ID-signed, notarized .app; an ad-hoc build is SIGKILLed at exec by
// AMFI and cannot touch the Enclave, so callers fail closed on a missing
// helper.
func RequireHelper() (string, error) {
	binary, err := HelperBinary()
	if err != nil {
		return "", err
	}
	if info, statErr := os.Stat(binary); statErr != nil || !info.Mode().IsRegular() {
		return "", &HelperError{Path: binary}
	}
	return binary, nil
}
