package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Kalen89/kube-node-migrate/cmd"
	"github.com/Kalen89/kube-node-migrate/internal/nodemigrate"
)

var version = "0.1.0 (commit=unknown built=unknown)"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	command := cmd.New(fmt.Sprintf("%s state=%d", version, nodemigrate.StateVersion()))
	err := command.ExecuteContext(ctx)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
