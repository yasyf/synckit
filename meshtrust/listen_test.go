package meshtrust

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
)

type stubAddr string

func (a stubAddr) Network() string { return "tcp" }
func (a stubAddr) String() string  { return string(a) }

type stubListener struct{ addr string }

func (s stubListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (s stubListener) Close() error              { return nil }
func (s stubListener) Addr() net.Addr            { return stubAddr(s.addr) }

// stubListen records every requested address and fails the ones in reject.
func stubListen(requested *[]string, reject func(string) bool) listenFunc {
	return func(address string) (net.Listener, error) {
		*requested = append(*requested, address)
		if reject != nil && reject(address) {
			return nil, errors.New("stub: rejected")
		}
		return stubListener{addr: address}, nil
	}
}

func TestBindTailnet(t *testing.T) {
	v4 := netip.MustParseAddr("100.88.252.58")
	v6 := netip.MustParseAddr("fd7a:115c:a1e0::6d33:fc3c")
	tests := []struct {
		name      string
		bind      string
		addrs     []netip.Addr
		hint      uint16
		reject    func(string) bool
		wantAddrs []string
	}{
		{"non-loopback bind yields none", "0.0.0.0", []netip.Addr{v4}, 0, nil, nil},
		{"lan bind yields none", "192.168.1.9", []netip.Addr{v4}, 0, nil, nil},
		{"no addrs yields none", "", nil, 0, nil, nil},
		{
			"one listener per addr on ephemeral ports",
			"",
			[]netip.Addr{v4, v6},
			0, nil,
			[]string{"100.88.252.58:0", "[fd7a:115c:a1e0::6d33:fc3c]:0"},
		},
		{
			"hint tried first",
			"",
			[]netip.Addr{v4},
			4321, nil,
			[]string{"100.88.252.58:4321"},
		},
		{
			"hint failure falls back to ephemeral",
			"",
			[]netip.Addr{v4},
			4321,
			func(a string) bool { return strings.HasSuffix(a, ":4321") },
			[]string{"100.88.252.58:0"},
		},
		{
			"unspecified addrs refused, rest survive",
			"",
			[]netip.Addr{
				netip.MustParseAddr("0.0.0.0"),
				netip.MustParseAddr("::"),
				netip.MustParseAddr("::ffff:0.0.0.0"),
				v4,
			},
			0, nil,
			[]string{"100.88.252.58:0"},
		},
		{
			"zoned addrs refused, rest survive",
			"",
			[]netip.Addr{
				netip.MustParseAddr("::%lo0"),
				netip.MustParseAddr("::ffff:0.0.0.0%lo0"),
				netip.IPv6Unspecified().WithZone("lo0"),
				netip.MustParseAddr("fe80::1%en0"),
				v4,
			},
			0, nil,
			[]string{"100.88.252.58:0"},
		},
		{
			"unbindable addr skipped, rest survive",
			"",
			[]netip.Addr{v6, v4},
			0,
			func(a string) bool { return strings.HasPrefix(a, "[fd7a") },
			[]string{"100.88.252.58:0"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var requested []string
			factories := bindTailnet(stubListen(&requested, tt.reject), tt.bind, tt.addrs, tt.hint)
			if got, want := len(factories), len(tt.wantAddrs); got != want {
				t.Fatalf("len(factories) = %d, want %d (requested: %v)", got, want, requested)
			}
			for i, factory := range factories {
				ln, err := factory(context.Background())
				if err != nil {
					t.Fatalf("factory %d: %v", i, err)
				}
				if got := ln.Addr().String(); got != tt.wantAddrs[i] {
					t.Errorf("listener %d bound %q, want %q", i, got, tt.wantAddrs[i])
				}
			}
		})
	}
}

func TestListenersRealBindFallback(t *testing.T) {
	holder, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Close() }()
	occupied := holder.Addr().(*net.TCPAddr).AddrPort().Port()

	factories := Listeners("", []netip.Addr{netip.MustParseAddr("127.0.0.1")}, occupied)
	if len(factories) != 1 {
		t.Fatalf("len(factories) = %d, want 1", len(factories))
	}
	ln, err := factories[0](context.Background())
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	defer func() { _ = ln.Close() }()
	if got := ln.Addr().(*net.TCPAddr).AddrPort().Port(); got == occupied || got == 0 {
		t.Errorf("bound port = %d, want a live ephemeral port != %d", got, occupied)
	}
}

func TestIsLoopbackBind(t *testing.T) {
	tests := []struct {
		bind string
		want bool
	}{
		{"", true},
		{"127.0.0.1", true},
		{"::1", true},
		{"0.0.0.0", false},
		{"192.168.1.5", false},
	}
	for _, tt := range tests {
		if got := isLoopbackBind(tt.bind); got != tt.want {
			t.Errorf("isLoopbackBind(%q) = %v, want %v", tt.bind, got, tt.want)
		}
	}
}
