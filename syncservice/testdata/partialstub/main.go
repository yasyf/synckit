// Command partialstub reproduces a peer that receives the start of a daemonkit handshake,
// emits an invalid partial response, then exits. The transport test proves the failed
// operation is returned without starting another candidate.
package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	_, _ = io.ReadFull(os.Stdin, make([]byte, 1))
	fmt.Fprint(os.Stdout, `{"ok":true,`)
}
