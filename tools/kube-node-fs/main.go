package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Kalen89/kube-node-migrate/internal/filesystem"
)

func run(ctx context.Context, args []string, out, stderr io.Writer) error {
	flags := flag.NewFlagSet("kube-node-fs", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("path", "", "Absolute volume directory")
	mode := flags.String("mode", "digest", "probe, inventory, digest or free")
	limit := flags.Duration("timeout", 12*time.Hour, "Maximum inspection duration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *limit <= 0 {
		return errors.New("unexpected arguments or nonpositive timeout")
	}
	ctx, cancel := context.WithTimeout(ctx, *limit)
	defer cancel()
	result, err := filesystem.Scan(ctx, *path, *mode)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(result)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
