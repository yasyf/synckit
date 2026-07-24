package rpc

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/synckit/internal/serviceidentity"
)

const remoteHelloTimeout = 10 * time.Second

// RemoteServeCommandLine returns the sole remote shell command accepted by Synckit v1.
func RemoteServeCommandLine(executable, serviceID string) (string, error) {
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable || strings.ContainsAny(executable, "\x00\r\n") {
		return "", errors.New("rpc: remote synckitd path must be exact and absolute")
	}
	if err := serviceidentity.ValidateName(serviceID); err != nil {
		return "", fmt.Errorf("rpc: remote service id: %w", err)
	}
	return shellWord(executable) + " " + shellWord(RemoteServeCommand) + " " + shellWord(serviceID), nil
}

// NewRemoteNonce returns one cryptographically random exact-width remote-session nonce.
func NewRemoteNonce() ([RemoteHelloNonceBytes]byte, error) {
	var nonce [RemoteHelloNonceBytes]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nonce, fmt.Errorf("rpc: generate remote hello nonce: %w", err)
	}
	return nonce, nil
}

// VerifyRemoteHello sends nonce and requires its exact framed echo before wire traffic.
func VerifyRemoteHello(ctx context.Context, conn net.Conn, nonce [RemoteHelloNonceBytes]byte) error {
	if conn == nil {
		return errors.New("rpc: remote hello connection is required")
	}
	if nonce == ([RemoteHelloNonceBytes]byte{}) {
		return errors.New("rpc: remote hello nonce is zero")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(remoteHelloTimeout)
	if value, ok := ctx.Deadline(); ok && value.Before(deadline) {
		deadline = value
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("rpc: set remote hello deadline: %w", err)
	}
	defer func() { _ = conn.SetDeadline(time.Time{}) }()
	if err := writeRemoteHello(conn, nonce[:]); err != nil {
		return fmt.Errorf("rpc: send remote hello: %w", err)
	}
	echo, err := readRemoteHello(conn)
	if err != nil {
		return fmt.Errorf("rpc: read remote hello: %w", err)
	}
	if !equalNonce(echo, nonce[:]) {
		return errors.New("rpc: remote hello nonce mismatch")
	}
	return nil
}

// ServeRemoteHello reads one exact nonce frame and echoes it before serving wire traffic.
func ServeRemoteHello(reader io.Reader, writer io.Writer) error {
	nonce, err := readRemoteHello(reader)
	if err != nil {
		return fmt.Errorf("rpc: receive remote hello: %w", err)
	}
	if err := writeRemoteHello(writer, nonce); err != nil {
		return fmt.Errorf("rpc: echo remote hello: %w", err)
	}
	return nil
}

func writeRemoteHello(writer io.Writer, nonce []byte) error {
	if len(nonce) != RemoteHelloNonceBytes {
		return errors.New("rpc: remote hello nonce has wrong length")
	}
	var frame [4 + RemoteHelloNonceBytes]byte
	binary.BigEndian.PutUint32(frame[:4], RemoteHelloNonceBytes)
	copy(frame[4:], nonce)
	_, err := io.Copy(writer, strings.NewReader(string(frame[:])))
	return err
}

func readRemoteHello(reader io.Reader) ([]byte, error) {
	var length [4]byte
	if _, err := io.ReadFull(reader, length[:]); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(length[:]) != RemoteHelloNonceBytes {
		return nil, errors.New("rpc: remote hello frame has wrong length")
	}
	nonce := make([]byte, RemoteHelloNonceBytes)
	if _, err := io.ReadFull(reader, nonce); err != nil {
		return nil, err
	}
	return nonce, nil
}

func equalNonce(a, b []byte) bool {
	return subtle.ConstantTimeCompare(a, b) == 1
}

func shellWord(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
