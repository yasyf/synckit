// Command ownstub is a prior process-ownership generation that crashes: it
// opens the record its caller names, adopts the process its caller started, and
// exits without settling, exactly as a killed CLI would.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/yasyf/daemonkit"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: ownstub <record-path> <pid>")
		os.Exit(2)
	}
	pid, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	owned, err := daemonkit.OwnProcesses(ctx, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := owned.Adopt(ctx, pid); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
