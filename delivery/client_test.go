package delivery

import (
	"errors"
	"os"
	"testing"

	"github.com/yasyf/synckit/internal/rpctest"
	"github.com/yasyf/synckit/internal/servespec"
	"github.com/yasyf/synckit/rpc"
)

func TestCallAgainstADaemonPredatingDeliveryIsUnknownMethod(t *testing.T) {
	base, err := os.MkdirTemp("/tmp", "sk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	t.Setenv("DAEMONKIT_HOME", base)
	server, err := rpctest.Start(t.Context(), servespec.Label, rpc.NewDispatcher())
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close server: %v", err)
		}
	})
	tests := []struct {
		name string
		call func() error
		want string
	}{
		{"status", func() error { _, err := Status(t.Context(), ""); return err }, `delivery.status: unknown method "delivery.status"`},
		{"kick", func() error { return Kick(t.Context(), "", "") }, `delivery.kick: unknown method "delivery.kick"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if !errors.Is(err, rpc.ErrUnknownMethod) || err.Error() != tt.want {
				t.Fatalf("error = %v, want %q wrapping rpc.ErrUnknownMethod", err, tt.want)
			}
		})
	}
}
