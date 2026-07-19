// Command hangstub reproduces a peer that receives the start of a daemonkit handshake,
// emits one invalid response byte, then wedges. The transport test proves a context
// timeout is terminal for the operation and cannot trigger replay on another candidate.
package main

import (
	"fmt"
	"io"
	"os"
	"time"
)

func main() {
	_, _ = io.ReadFull(os.Stdin, make([]byte, 1))
	fmt.Fprint(os.Stdout, "{")
	time.Sleep(time.Hour)
}
