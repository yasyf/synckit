// Package presence reads this host's console GUI session — whether a person is at the
// keyboard, whether the screen is locked, and whether an inbound screen share is
// mirroring the console — by shelling ioreg and netstat under bounded subprocesses.
package presence

import (
	"context"
	"fmt"
	"os/exec"
	"os/user"
	"strings"
	"time"
)

// probeTimeout bounds each probe subprocess so a wedged ioreg or netstat fails fast
// instead of parking the caller.
var probeTimeout = 2 * time.Second

var ioregArgv = []string{"ioreg", "-n", "Root", "-d1", "-a"}

var screenShareArgv = []string{"netstat", "-anv", "-p", "tcp"}

// Console-session plist keys, matching the macOS CoreGraphics session dictionary.
const (
	onConsoleKey    = "kCGSSessionOnConsoleKey"
	userNameKey     = "kCGSSessionUserNameKey"
	screenLockedKey = "CGSSessionScreenIsLocked"
)

// netstat -anv -p tcp vocabulary: Screen Sharing (VNC) serves on TCP 5900, so an
// inbound session shows this host's local address on that port in ESTABLISHED. The
// local address is field 3 and the state field 5 (0-indexed), under a "Proto" header.
const (
	screenSharePort  = ".5900"
	establishedState = "ESTABLISHED"
	netstatHeader    = "Proto"
	localAddrField   = 3
	stateField       = 5
)

// SessionSnapshot is a point-in-time read of this host's console GUI session: whether
// a GUI session owns the physical console, whether its screen is locked, the console
// user's short name (empty when no GUI session is attached), and whether an inbound
// screen share is mirroring the console.
type SessionSnapshot struct {
	OnConsole    bool
	Locked       bool
	ConsoleUser  string
	ScreenShared bool
}

// Console reads this host's console GUI session from ioreg alone: whether a GUI
// session owns the console, whether its screen is locked, and the console user. It
// carries no screen-share signal, so ScreenShared is always false — it is the cheap
// keybag-availability read that stays off the netstat path.
func Console(ctx context.Context) (SessionSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, ioregArgv[0], ioregArgv[1:]...).Output() //nolint:gosec // G204: ioregArgv is a fixed argv, not user-supplied.
	if err != nil {
		return SessionSnapshot{}, fmt.Errorf("run ioreg: %w", err)
	}
	return parseSession(out)
}

// Session reads the full console session: the ioreg console read of Console plus a
// netstat probe for an inbound Screen Sharing session mirroring this host, folded into
// the snapshot's ScreenShared field.
func Session(ctx context.Context) (SessionSnapshot, error) {
	snapshot, err := Console(ctx)
	if err != nil {
		return SessionSnapshot{}, err
	}
	shared, err := screenShared(ctx)
	if err != nil {
		return SessionSnapshot{}, err
	}
	snapshot.ScreenShared = shared
	return snapshot, nil
}

// Attended reports whether a real person is at this host's keyboard: this process's
// user holds the console with an unlocked, unmirrored screen. A live screen share
// returns false — a mirror cannot prove a Touch ID tap reaches the present human.
func Attended(snap SessionSnapshot) (bool, error) {
	me, err := user.Current()
	if err != nil {
		return false, fmt.Errorf("resolve current user: %w", err)
	}
	return snap.OnConsole && !snap.Locked && snap.ConsoleUser == me.Username && !snap.ScreenShared, nil
}

// screenShared shells netstat for an inbound Screen Sharing session mirroring this
// host's console, under a bounded subprocess.
func screenShared(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, screenShareArgv[0], screenShareArgv[1:]...).Output() //nolint:gosec // G204: screenShareArgv is a fixed argv, not user-supplied.
	if err != nil {
		return false, fmt.Errorf("run netstat: %w", err)
	}
	shared, err := parseScreenShare(out)
	if err != nil {
		return false, fmt.Errorf("parse netstat: %w", err)
	}
	return shared, nil
}

// parseSession decodes an ioreg XML plist into a snapshot: the first on-console session
// decides; locked is the root flag OR that session's own screen-locked flag; an absent
// on-console session is headless.
func parseSession(payload []byte) (SessionSnapshot, error) {
	root, err := decodePlist(payload)
	if err != nil {
		return SessionSnapshot{}, err
	}
	dict, ok := root.(map[string]any)
	if !ok {
		return SessionSnapshot{}, fmt.Errorf("ioreg plist root is %T, want a dict", root)
	}
	users, _ := dict["IOConsoleUsers"].([]any)
	for _, raw := range users {
		sess, ok := raw.(map[string]any)
		if !ok || !asBool(sess[onConsoleKey]) {
			continue
		}
		return SessionSnapshot{
			OnConsole:   true,
			Locked:      asBool(dict["IOConsoleLocked"]) || asBool(sess[screenLockedKey]),
			ConsoleUser: asString(sess[userNameKey]),
		}, nil
	}
	return SessionSnapshot{}, nil
}

// parseScreenShare reports whether netstat shows an inbound Screen Sharing session:
// some socket's local address is on the VNC port .5900 in ESTABLISHED. A LISTEN row is
// the idle listener, so only ESTABLISHED counts; a payload with no netstat header is
// malformed and errors, mirroring parseSession's fail-loud on an invalid read.
func parseScreenShare(payload []byte) (bool, error) {
	sawHeader := false
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if fields[0] == netstatHeader {
			sawHeader = true
			continue
		}
		if !sawHeader {
			continue
		}
		if len(fields) <= stateField {
			continue
		}
		if strings.HasSuffix(fields[localAddrField], screenSharePort) && fields[stateField] == establishedState {
			return true, nil
		}
	}
	if !sawHeader {
		return false, fmt.Errorf("netstat output has no %q header: %d bytes", netstatHeader, len(payload))
	}
	return false, nil
}

func asBool(v any) bool {
	b, _ := v.(bool)
	return b
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}
