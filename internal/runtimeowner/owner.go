// Package runtimeowner centralizes Synckit's private runtime stop authority.
package runtimeowner

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"

	"github.com/yasyf/synckit/hostregistry"
)

const serviceProcessFile = "service-processes.db"

// TrustPolicy admits same-UID product RPC without inventing a signed-app role
// for the unsigned synckitd executable. Spawned and remote sessions are
// authenticated by their process receipt and SSH host fact respectively.
func TrustPolicy() (trust.TrustPolicy, error) {
	return trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID:      os.Geteuid(),
		AllowUnprotected: true,
	})
}

// ServiceProcessPath returns the sole durable process authority store shared
// by the service controller and runtime stop verifier.
func ServiceProcessPath() (string, error) {
	directory, err := hostregistry.Mesh.Dir()
	if err != nil {
		return "", fmt.Errorf("resolve synckit state directory: %w", err)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("create synckit state directory: %w", err)
	}
	return filepath.Join(directory, serviceProcessFile), nil
}

// ServiceProcessStore returns the shared controller/runtime stop-authority store.
func ServiceProcessStore() (*proc.FileStore, error) {
	path, err := ServiceProcessPath()
	if err != nil {
		return nil, err
	}
	return &proc.FileStore{Path: path}, nil
}
