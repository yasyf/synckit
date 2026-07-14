package presence

import (
	"context"
	"os/user"
	"testing"
	"time"
)

// ioregPlist builds an ioreg-shaped XML plist for one console session with the given
// root-level locked flag, per-session on-console/locked flags, and user name.
func ioregPlist(rootLocked, onConsole, sessionLocked bool, userName string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>IOConsoleLocked</key>
	<` + boolTag(rootLocked) + `/>
	<key>IOConsoleUsers</key>
	<array>
		<dict>
			<key>CGSSessionScreenIsLocked</key>
			<` + boolTag(sessionLocked) + `/>
			<key>kCGSSessionOnConsoleKey</key>
			<` + boolTag(onConsole) + `/>
			<key>kCGSSessionUserIDKey</key>
			<integer>501</integer>
			<key>kCGSSessionUserNameKey</key>
			<string>` + userName + `</string>
		</dict>
	</array>
</dict>
</plist>
`
}

func boolTag(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func currentUser(t *testing.T) string {
	t.Helper()
	me, err := user.Current()
	if err != nil {
		t.Fatalf("user.Current: %v", err)
	}
	return me.Username
}

func liveSession(username string) SessionSnapshot {
	return SessionSnapshot{OnConsole: true, Locked: false, ConsoleUser: username}
}

// TestConsoleBoundsSlowSubprocess proves Console kills a wedged probe subprocess and
// returns an error rather than parking: with ioreg swapped for a sleeping stand-in, a
// short internal probe timeout cancels the child and Console fails fast.
func TestConsoleBoundsSlowSubprocess(t *testing.T) {
	prev := ioregArgv
	ioregArgv = []string{"sleep", "30"}
	t.Cleanup(func() { ioregArgv = prev })

	prevTimeout := probeTimeout
	probeTimeout = 150 * time.Millisecond
	t.Cleanup(func() { probeTimeout = prevTimeout })

	start := time.Now()
	_, err := Console(context.Background())
	if err == nil {
		t.Fatal("Console returned nil, want the bounded subprocess to fail")
	}
	if elapsed := time.Since(start); elapsed > 5*probeTimeout {
		t.Fatalf("Console took %s, want it bounded by the %s internal probe timeout", elapsed, probeTimeout)
	}
}

// TestParseSessionMatchesIoregShape table-drives parseSession over the real ioreg
// plist shape: an unlocked console, a screen-locked one (via both flags), and a
// headless box with no on-console session.
func TestParseSessionMatchesIoregShape(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    SessionSnapshot
	}{
		{
			name:    "live unlocked console",
			payload: ioregPlist(false, true, false, "yasyf"),
			want:    SessionSnapshot{OnConsole: true, Locked: false, ConsoleUser: "yasyf"},
		},
		{
			name:    "locked via root flag",
			payload: ioregPlist(true, true, false, "yasyf"),
			want:    SessionSnapshot{OnConsole: true, Locked: true, ConsoleUser: "yasyf"},
		},
		{
			name:    "locked via per-session flag",
			payload: ioregPlist(false, true, true, "yasyf"),
			want:    SessionSnapshot{OnConsole: true, Locked: true, ConsoleUser: "yasyf"},
		},
		{
			name:    "headless: no on-console session",
			payload: ioregPlist(false, false, false, "yasyf"),
			want:    SessionSnapshot{OnConsole: false, Locked: false, ConsoleUser: ""},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSession([]byte(tc.payload))
			if err != nil {
				t.Fatalf("parseSession: %v", err)
			}
			if got != tc.want {
				t.Fatalf("parseSession = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestParseSessionEmptyConsoleUsers proves an empty IOConsoleUsers array parses as
// headless rather than erroring.
func TestParseSessionEmptyConsoleUsers(t *testing.T) {
	payload := `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
	<key>IOConsoleLocked</key>
	<false/>
	<key>IOConsoleUsers</key>
	<array/>
</dict>
</plist>
`
	got, err := parseSession([]byte(payload))
	if err != nil {
		t.Fatalf("parseSession: %v", err)
	}
	if got != (SessionSnapshot{}) {
		t.Fatalf("empty users = %+v, want zero snapshot", got)
	}
}

// TestParseScreenShare table-drives parseScreenShare over real `netstat -anv -p tcp`
// rows: only an inbound .5900 ESTABLISHED session counts as shared; a header-less
// payload is malformed.
func TestParseScreenShare(t *testing.T) {
	const header = "Active Internet connections (including servers)\n" +
		"Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)      rxbytes  txbytes  rhiwat  shiwat  process:pid  options  gencnt  flags  flags1 usecnt rtncnt fltrs\n"
	const listen5900 = "tcp46      0      0  *.5900                 *.*                    LISTEN            0            0  131072  131072    screensharingd:912   00100 00000006 00000000000037c9 00000001 00000800      1      0 000000\n"
	const established5900 = "tcp4       0      0  192.168.4.145.5900     192.168.4.50.54873     ESTABLISHED   14832         1083  131072  131600    screensharingd:912   00102 00000008 0000000001fec78d 00000081 04000900      2      0 000000\n"
	const established443 = "tcp4       0      0  192.168.4.145.55531    35.190.46.17.443       ESTABLISHED   3010          839  131072  131600          2.1.198:60299  00102 00000008 00000000021a4621 00000081 04000900      2      0 000000\n"

	tests := []struct {
		name    string
		payload string
		want    bool
		wantErr bool
	}{
		{
			name:    "inbound established screen share on .5900",
			payload: header + listen5900 + established443 + established5900,
			want:    true,
		},
		{
			name:    "listen-only on *.5900 is the idle listener",
			payload: header + listen5900 + established443,
			want:    false,
		},
		{
			name:    "established on a non-5900 local port",
			payload: header + established443,
			want:    false,
		},
		{
			name:    "socketless table",
			payload: header,
			want:    false,
		},
		{
			name:    "malformed payload with no netstat header",
			payload: "this is not netstat output\n",
			wantErr: true,
		},
		{
			// A row shaped like an inbound .5900 session must not be accepted before the
			// Proto header confirms this is real netstat output.
			name:    "established .5900 row before any header is rejected",
			payload: established5900,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseScreenShare([]byte(tc.payload))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseScreenShare = %v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseScreenShare: %v", err)
			}
			if got != tc.want {
				t.Fatalf("parseScreenShare = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestDecodePlistData proves a <data> element is base64-decoded to its content — Apple's
// surrounding whitespace ignored — rather than returned as the raw base64 source bytes.
func TestDecodePlistData(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    string
	}{
		{"tight base64", `<plist><data>SGk=</data></plist>`, "Hi"},
		{"whitespace-wrapped base64", "<plist><data>\n\tSGk=\n</data></plist>", "Hi"},
		{"empty data", `<plist><data></data></plist>`, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodePlist([]byte(tc.payload))
			if err != nil {
				t.Fatalf("decodePlist: %v", err)
			}
			b, ok := got.([]byte)
			if !ok {
				t.Fatalf("decoded value is %T, want []byte", got)
			}
			if string(b) != tc.want {
				t.Fatalf("decoded data = %q, want %q", b, tc.want)
			}
		})
	}
}

// TestDecodePlistMalformed proves malformed plist bodies fail loud instead of silently
// storing a nil value: a dict key with no value, an end tag where a value is expected,
// and invalid base64 in a data element.
func TestDecodePlistMalformed(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"dict key with no value", `<plist><dict><key>a</key></dict></plist>`},
		{"end tag where a value is expected", `<plist></plist>`},
		{"invalid base64 data", `<plist><data>not base64!</data></plist>`},
		{"mismatched outer close tag", `<plist><string>hi</string></wrong>`},
		{"missing outer close tag", `<plist><string>hi</string>`},
		{"multiple root values", `<plist><string>a</string><string>b</string></plist>`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodePlist([]byte(tc.payload)); err == nil {
				t.Fatalf("decodePlist(%q) = nil error, want a parse failure", tc.payload)
			}
		})
	}
}

// TestAttendedScreenShareWins proves a live screen share makes Attended return false
// even on an unlocked console owned by this user — a mirror cannot prove the tap
// reaches the present human, so consent must route to a peer.
func TestAttendedScreenShareWins(t *testing.T) {
	me := currentUser(t)

	attended, err := Attended(liveSession(me))
	if err != nil {
		t.Fatalf("Attended: %v", err)
	}
	if !attended {
		t.Fatalf("an unlocked console owned by this user must be attended")
	}

	shared := liveSession(me)
	shared.ScreenShared = true
	attended, err = Attended(shared)
	if err != nil {
		t.Fatalf("Attended: %v", err)
	}
	if attended {
		t.Fatalf("a screen-shared host must NOT be attended (screen-share wins)")
	}
}

// TestAttendedScopesToUnlockedOwnConsole proves Attended is false on a locked screen,
// a console held by another user via fast user switching, and a session-absent box.
func TestAttendedScopesToUnlockedOwnConsole(t *testing.T) {
	me := currentUser(t)
	tests := []struct {
		name string
		snap SessionSnapshot
		want bool
	}{
		{"attended", liveSession(me), true},
		{"locked screen", SessionSnapshot{OnConsole: true, Locked: true, ConsoleUser: me}, false},
		{"another user via fast user switching", SessionSnapshot{OnConsole: true, ConsoleUser: me + "-other"}, false},
		{"session absent", SessionSnapshot{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Attended(tc.snap)
			if err != nil {
				t.Fatalf("Attended: %v", err)
			}
			if got != tc.want {
				t.Fatalf("Attended(%+v) = %v, want %v", tc.snap, got, tc.want)
			}
		})
	}
}
