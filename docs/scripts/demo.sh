#!/usr/bin/env bash
# Regenerates the README get-started demo: runs the quickstart RPC snippet for
# real against the working copy and prints the output block to paste under it.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cat > "$work/main.go" <<'EOF'
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/synckit/rpc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := rpc.NewDispatcher()
	d.Register("ping", func(ctx context.Context, p map[string]any) (any, error) {
		return map[string]any{"pong": p["msg"]}, nil
	})

	sock := filepath.Join(os.TempDir(), "synckit-rpc-demo.sock")
	_ = os.Remove(sock)
	defer os.Remove(sock)
	ln, _ := rpc.Listen(ctx, sock)
	go rpc.NewServer(d).Serve(ctx, ln)

	client := rpc.NewClient(rpc.ClientConfig{Dial: wire.UnixDialer(sock), WireBuild: rpc.WireBuild})
	defer client.Close()
	resp, _ := client.Call(ctx, &rpc.Request{
		Method: "ping",
		Params: map[string]any{"msg": "hi"},
	})
	var result map[string]string
	_ = json.Unmarshal(resp.Result, &result)
	fmt.Printf("resp.Result = %v\n", result)
}
EOF

cat > "$work/go.mod" <<EOF
module rpcdemo

go 1.26.4

require github.com/yasyf/synckit v0.0.0

replace github.com/yasyf/synckit => $repo_root
EOF

cd "$work"
go mod tidy >/dev/null 2>&1
echo '$ go run .'
go run .
