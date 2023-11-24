package main

import (
	"collector/cmd"
	"os/signal"
	"syscall"
)

func main() {
	signal.Ignore(syscall.SIGPIPE) // ignore SIGPIPE
	cmd.Execute()
}
