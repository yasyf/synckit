// Command helperstub serves a resident helper named os.Args[1] at build
// os.Args[2] in its own process, since daemonkit's Drain refuses to pin the
// caller's own PID.
package main

import (
	"context"
	"log"
	"os"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/helperruntime"
	"github.com/yasyf/synckit/rpc"
)

type idleProduct struct{}

func (idleProduct) Drain(context.Context) error { return nil }
func (idleProduct) Close(context.Context) error { return nil }

func main() {
	program, err := daemonkit.Stable(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	runtime, err := helperruntime.New(helperruntime.Config{
		App:        helperruntime.App{Name: os.Args[1]},
		Program:    program,
		Dispatcher: rpc.NewDispatcher(),
		Prepare:    func(daemonkit.Ctx) (helperruntime.Product, error) { return idleProduct{}, nil },
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := runtime.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
