package meshtrust

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
)

type listenFunc func(address string) (net.Listener, error)

func netListen(address string) (net.Listener, error) {
	return net.Listen("tcp", address)
}

// Listeners eagerly binds one extra listener per tailnet address (a concrete
// unzoned host address, typically Provider.SelfAddrs — an unspecified or
// zoned address is refused, never bound), so a loopback-bound daemon also
// serves the mesh,
// returning them as factories matching cc-interact's ExtraHTTPListeners shape.
// An unbindable address is skipped with a warning (loopback must survive); nil
// for a non-loopback primary bind (it covers the tailnet; a second bind would
// collide) or empty addrs. Binds try portHint first (a consumer's last-known
// port, zero for none), then ephemeral.
func Listeners(bind string, addrs []netip.Addr, portHint uint16) []func(context.Context) (net.Listener, error) {
	return bindTailnet(netListen, bind, addrs, portHint)
}

func bindTailnet(listen listenFunc, bind string, addrs []netip.Addr, hint uint16) []func(context.Context) (net.Listener, error) {
	if !isLoopbackBind(bind) {
		return nil
	}
	if len(addrs) == 0 {
		slog.Warn("meshtrust: no tailnet addresses; serving loopback only")
		return nil
	}
	factories := make([]func(context.Context) (net.Listener, error), 0, len(addrs))
	for _, addr := range addrs {
		if addr.Zone() != "" {
			slog.Warn("meshtrust: refusing to bind zoned address", "addr", addr)
			continue
		}
		if addr.Unmap().IsUnspecified() {
			slog.Warn("meshtrust: refusing to bind unspecified address", "addr", addr)
			continue
		}
		var ln net.Listener
		var err error
		if hint != 0 {
			ln, err = listen(netip.AddrPortFrom(addr, hint).String())
		}
		if ln == nil {
			ln, err = listen(netip.AddrPortFrom(addr, 0).String())
		}
		if err != nil {
			slog.Warn("meshtrust: cannot bind tailnet address; skipping", "addr", addr, "err", err)
			continue
		}
		factories = append(factories, func(context.Context) (net.Listener, error) { return ln, nil })
	}
	return factories
}

// isLoopbackBind reports whether bind keeps the HTTP plane on loopback. An empty
// bind is the loopback default; a loopback IP is loopback; any other address
// (0.0.0.0 or a LAN IP) exposes the plane.
func isLoopbackBind(bind string) bool {
	if bind == "" {
		return true
	}
	ip := net.ParseIP(bind)
	return ip != nil && ip.IsLoopback()
}
