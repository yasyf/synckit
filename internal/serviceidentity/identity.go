// Package serviceidentity defines Synckit's exact service labels.
package serviceidentity

import "fmt"

const (
	// LabelPrefix is the fixed launchd namespace for Synckit services.
	LabelPrefix = "com.github.yasyf.synckit"
	// DaemonBinary is the stable executable alias for Synckit's controller.
	DaemonBinary = "synckitd"
)

// ValidateName enforces the canonical consumer identity used in manifests and
// helper service labels.
func ValidateName(name string) error {
	if len(name) == 0 || len(name) > 63 || name[0] == '-' || name[len(name)-1] == '-' {
		return fmt.Errorf("must be a canonical lowercase service name, got %q", name)
	}
	for _, char := range []byte(name) {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' {
			continue
		}
		return fmt.Errorf("must be a canonical lowercase service name, got %q", name)
	}
	return nil
}

// HelperLabel returns the exact LaunchAgent identity for a consumer helper.
func HelperLabel(name string) (string, error) {
	if err := ValidateName(name); err != nil {
		return "", err
	}
	return LabelPrefix + ".helper." + name, nil
}

// StopRole returns the single Synckit-owned stop-control role identity.
func StopRole() string { return LabelPrefix + ".stop" }
