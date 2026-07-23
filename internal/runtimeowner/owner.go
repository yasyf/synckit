// Package runtimeowner centralizes Synckit's private runtime stop authority.
package runtimeowner

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/yasyf/daemonkit/daemonrole"
	"github.com/yasyf/daemonkit/proc"
	dkservice "github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/serviceidentity"
	"github.com/yasyf/synckit/rpc"
)

const serviceProcessFile = "service-processes.db"

// StopAuthority builds the exact Synckit-owned classifier and verifier shared
// by synckitd and every embedded helper runtime.
func StopAuthority() (daemonrole.Classifier, wire.StopVerifier, error) {
	executable, err := stopExecutable()
	if err != nil {
		return daemonrole.Classifier{}, wire.StopVerifier{}, err
	}
	classifier := daemonrole.Classifier{RoleID: serviceidentity.StopRole(), RolePath: executable}
	if err := classifier.Validate(); err != nil {
		return daemonrole.Classifier{}, wire.StopVerifier{}, fmt.Errorf("validate synckit stop role: %w", err)
	}
	path, err := ServiceProcessPath()
	if err != nil {
		return daemonrole.Classifier{}, wire.StopVerifier{}, err
	}
	return classifier, wire.StopVerifier{
		Classifier: classifier,
		Role:       serviceidentity.StopRole(),
		Store:      &proc.FileStore{Path: path},
	}, nil
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

// StopControlSpec returns the controller invocation for the private Synckit
// stop role. Callers provide only target identity and product release facts.
func StopControlSpec(sock, runtimeBuild, generation string, intent wire.StopIntent) (dkservice.StopControlSpec, error) {
	executable, err := stopExecutable()
	if err != nil {
		return dkservice.StopControlSpec{}, err
	}
	return dkservice.StopControlSpec{
		Executable:              executable,
		Args:                    []string{"stop-control", sock},
		Role:                    serviceidentity.StopRole(),
		RuntimeBuild:            runtimeBuild,
		RuntimeProtocol:         int(rpc.Version),
		TargetProcessGeneration: generation,
		Intent:                  intent,
	}, nil
}

// StopControlClientConfig returns the fixed wire identity for the hidden stop
// child. The socket is the only product-supplied transport fact.
func StopControlClientConfig(sock string) dkservice.StopControlClientConfig {
	return dkservice.StopControlClientConfig{
		Dial: wire.UnixDialer(sock), WireBuild: rpc.WireBuild, RuntimeProtocol: int(rpc.Version),
	}
}

func stopExecutable() (string, error) {
	alias, err := exec.LookPath(serviceidentity.DaemonBinary)
	if err != nil {
		return "", fmt.Errorf("resolve synckit stop executable: %w", err)
	}
	alias, err = filepath.Abs(alias)
	if err != nil {
		return "", fmt.Errorf("resolve absolute synckit stop executable: %w", err)
	}
	return filepath.Clean(alias), nil
}
