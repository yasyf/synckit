// Command partialstub reproduces a peer that receives a request and begins answering
// but drops the connection mid-response: it reads one request line, writes a partial
// (newline-less) response chunk, then exits. The syncservice transport failover test
// uses it to prove that once a candidate has emitted its first response byte the tunnel
// pins to it — Do surfaces the read error and never fails over to the next address.
package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	// Consume one request line so the parent's write completes before we answer.
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	// A valid JSON prefix with no terminating newline, then exit: the reader gets a
	// first byte but never a full line.
	fmt.Fprint(os.Stdout, `{"ok":true,`)
}
