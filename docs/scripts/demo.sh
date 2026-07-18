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
	"fmt"

	"github.com/yasyf/synckit/rpc"
)

func main() {
	ctx := context.Background()

	d := rpc.NewDispatcher()
	d.Register("ping", func(ctx context.Context, p map[string]any) (any, error) {
		return map[string]any{"pong": p["msg"]}, nil
	})

	ln, _ := rpc.Listen(ctx, "/tmp/app.sock")
	go rpc.Serve(ctx, ln, d)

	resp, _ := rpc.Call(ctx, "/tmp/app.sock", &rpc.Request{
		Method: "ping",
		Params: map[string]any{"msg": "hi"},
	})
	fmt.Printf("resp.Result = %v\n", resp.Result)
}
EOF

cat > "$work/go.mod" <<EOF
module rpcdemo

go 1.26.4

require github.com/yasyf/synckit v0.0.0

replace github.com/yasyf/synckit => $repo_root
EOF

cd "$work"
rm -f /tmp/app.sock
go mod tidy >/dev/null 2>&1
echo '$ go run .'
go run .
