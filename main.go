package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/user"
	"time"
)

const usage = `syncup — topic-based updates between agent sessions

Usage:
  syncup init --brokers <b1,b2,..> [--user <name>]   configure local state
  syncup create <channel> [description]              create a channel
  syncup list                                        list channels (✓ = joined)
  syncup join <channel>                              subscribe (from now on)
  syncup leave <channel>                             unsubscribe
  syncup publish <channel> <message...>             post an update
  syncup inbox [channel] [--quiet]                   read unread updates (all, or one channel)
  syncup watch [--tmux <pane>] [--interval 2s]       daemon: push new updates into a tmux pane
  syncup delete <channel>                            retire a channel

Env: SYNCUP_BROKERS, SYNCUP_CONFIG override config.`

func defaultUser() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return os.Getenv("USER")
}

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	return fs
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println(usage)
		os.Exit(2)
	}
	timeout := 30 * time.Second
	if v := os.Getenv("SYNCUP_TIMEOUT"); v != "" {
		if n, err := time.ParseDuration(v + "s"); err == nil {
			timeout = n
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	args := os.Args[2:]
	var err error
	switch os.Args[1] {
	case "init":
		err = cmdInit(args)
	case "create":
		err = cmdCreate(ctx, args)
	case "list", "ls":
		err = cmdList(ctx, args)
	case "join":
		err = cmdJoin(ctx, args)
	case "leave":
		err = cmdLeave(args)
	case "publish", "pub":
		err = cmdPublish(ctx, args)
	case "inbox":
		err = cmdInbox(ctx, args)
	case "watch":
		err = cmdWatch(args) // long-running daemon; manages its own timeouts
	case "delete", "rm":
		err = cmdDelete(ctx, args)
	case "-h", "--help", "help":
		fmt.Println(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n%s\n", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
