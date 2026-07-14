// Command hangstub reproduces a peer that receives a request, emits its first response
// byte, then wedges: it reads one request line, writes one newline-less byte, and sleeps.
// The syncservice transport test uses it to prove that a ctx timeout after the first
// response byte is terminal for the Do — no failover to the next candidate.
package main

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

func main() {
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	fmt.Fprint(os.Stdout, "{")
	time.Sleep(time.Hour)
}
