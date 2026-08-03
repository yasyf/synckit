// Command spawnstub is a synckit consumer's sealed local service: it claims the
// handoff descriptor its parent placed at fd 3 and serves exactly one session.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/yasyf/synckit/rpc"
)

func main() {
	dispatcher := rpc.NewDispatcher()
	dispatcher.Register("echo", func(_ context.Context, params map[string]any) (any, error) {
		return params["say"], nil
	})
	if err := rpc.ServeSpawned(context.Background(), dispatcher); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
