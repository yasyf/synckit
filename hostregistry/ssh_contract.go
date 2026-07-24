package hostregistry

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/yasyf/synckit/rpc"
)

const sshExecutable = "/usr/bin/ssh"

// SSHHostFact is one immutable registered remote SSH identity and daemon location.
type SSHHostFact struct {
	Identity     string   `json:"identity"`
	User         string   `json:"user"`
	HostKeyAlias string   `json:"host_key_alias"`
	Addresses    []string `json:"addresses"`
	SynckitdPath string   `json:"synckitd_path"`
}

// NewSSHHostFact validates and copies one exact registered host fact.
func NewSSHHostFact(identity, synckitdPath string, addresses []string) (SSHHostFact, error) {
	user, host, err := splitSSHIdentity(identity)
	if err != nil {
		return SSHHostFact{}, err
	}
	if !exactRemotePath(synckitdPath) {
		return SSHHostFact{}, errors.New("hostregistry: synckitd path must be exact and absolute")
	}
	result := SSHHostFact{
		Identity: identity, User: user, HostKeyAlias: host, SynckitdPath: synckitdPath,
		Addresses: make([]string, 0, len(addresses)+1),
	}
	seen := make(map[string]struct{}, len(addresses)+1)
	for _, address := range append(addresses, host) {
		addressUser, addressHost, hasUser := strings.Cut(address, "@")
		if hasUser {
			if strings.Contains(addressHost, "@") || addressUser != user {
				return SSHHostFact{}, errors.New("hostregistry: alternate address changes the registered user")
			}
			address = addressHost
		}
		if !validSSHHost(address) {
			return SSHHostFact{}, fmt.Errorf("hostregistry: invalid SSH address %q", address)
		}
		if _, exists := seen[address]; exists {
			continue
		}
		seen[address] = struct{}{}
		result.Addresses = append(result.Addresses, address)
	}
	return result, nil
}

func splitSSHIdentity(identity string) (string, string, error) {
	user, host, ok := strings.Cut(identity, "@")
	if !ok || strings.Contains(host, "@") || !validSSHUser(user) || !validSSHHost(host) {
		return "", "", fmt.Errorf("hostregistry: identity %q must be explicit user@host", identity)
	}
	return user, host, nil
}

func equalSSHHostFact(a, b SSHHostFact) bool {
	return a.Identity == b.Identity && a.User == b.User && a.HostKeyAlias == b.HostKeyAlias &&
		a.SynckitdPath == b.SynckitdPath && slices.Equal(a.Addresses, b.Addresses)
}

// RemoteSSHArgv returns the sealed OpenSSH invocation for one registered service.
func RemoteSSHArgv(fact SSHHostFact, address, knownHostsPath, serviceID string) ([]string, error) {
	validated, err := NewSSHHostFact(fact.Identity, fact.SynckitdPath, fact.Addresses)
	if err != nil {
		return nil, err
	}
	if validated.Identity != fact.Identity || validated.User != fact.User ||
		validated.HostKeyAlias != fact.HostKeyAlias || validated.SynckitdPath != fact.SynckitdPath ||
		!slices.Equal(validated.Addresses, fact.Addresses) || !containsString(validated.Addresses, address) {
		return nil, errors.New("hostregistry: SSH host fact is not canonical")
	}
	if err := ValidateKnownHosts(knownHostsPath); err != nil {
		return nil, err
	}
	command, err := rpc.RemoteServeCommandLine(validated.SynckitdPath, serviceID)
	if err != nil {
		return nil, err
	}
	return []string{
		sshExecutable,
		"-F", "/dev/null", "-T",
		"-o", "BatchMode=yes",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=" + knownHostsPath,
		"-o", "GlobalKnownHostsFile=/dev/null",
		"-o", "KnownHostsCommand=none",
		"-o", "UpdateHostKeys=no",
		"-o", "CheckHostIP=no",
		"-o", "HostKeyAlias=" + validated.HostKeyAlias,
		"-o", "IdentitiesOnly=yes",
		"-o", "IdentityAgent=none",
		"-o", "ProxyCommand=none",
		"-o", "ProxyJump=none",
		"-o", "CanonicalizeHostname=no",
		"-o", "ForwardAgent=no",
		"-o", "ForwardX11=no",
		"-o", "ForwardX11Trusted=no",
		"-o", "ClearAllForwardings=yes",
		"-o", "PermitLocalCommand=no",
		"-o", "RequestTTY=no",
		"-o", "EscapeChar=none",
		"-o", "ConnectTimeout=3",
		"-o", "ServerAliveInterval=5",
		"-o", "ServerAliveCountMax=3",
		"-l", validated.User,
		address, command,
	}, nil
}

// ValidateKnownHosts requires one exact private regular file with no symlink component.
func ValidateKnownHosts(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return errors.New("hostregistry: known_hosts path must be exact and absolute")
	}
	current := string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("hostregistry: inspect known_hosts path: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("hostregistry: known_hosts path contains a symlink")
		}
		if current == path {
			if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
				return errors.New("hostregistry: known_hosts must be a regular mode-0600 file")
			}
		} else if !info.IsDir() {
			return errors.New("hostregistry: known_hosts parent is not a directory")
		}
	}
	return nil
}

func validSSHUser(value string) bool {
	if value == "" || value[0] == '-' {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || strings.ContainsRune("._-", char) {
			continue
		}
		return false
	}
	return true
}

func validSSHHost(value string) bool {
	if value == "" || value[0] == '-' || strings.ContainsAny(value, "\x00\r\n\t /\\") {
		return false
	}
	if net.ParseIP(strings.Trim(value, "[]")) != nil {
		return true
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '.' || char == '-' {
			continue
		}
		return false
	}
	return true
}

func exactRemotePath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && !strings.ContainsAny(path, "\x00\r\n")
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
