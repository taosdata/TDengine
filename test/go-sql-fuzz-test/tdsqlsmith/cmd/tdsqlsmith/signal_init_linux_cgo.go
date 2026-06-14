//go:build linux && cgo

package main

// signal_init_linux_cgo.go resets fatal signal handlers (SIGSEGV, SIGABRT,
// SIGFPE, SIGBUS, SIGILL) to their default disposition at startup via cgo, so
// that a crash terminates the worker process and produces a core dump that the
// supervisor can detect, rather than being swallowed by the Go runtime.
//
// signal_init_linux_cgo.go 在启动时通过 cgo 将致命信号处理器(SIGSEGV、SIGABRT、
// SIGFPE、SIGBUS、SIGILL)重置为默认行为,使崩溃能够终止 worker 进程并产生可被
// supervisor 检测到的 core dump,而不会被 Go 运行时吞掉。

/*
#include <stdio.h>
#include <signal.h>
#include <string.h>

void init_signal_handle() {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_DFL;
    sa.sa_flags = SA_ONSTACK;
    sigemptyset(&sa.sa_mask);

    if (sigaction(SIGSEGV, &sa, NULL) == -1) {
        perror("sigaction for SIGSEGV");
    }

    if (sigaction(SIGABRT, &sa, NULL) == -1) {
        perror("sigaction for SIGABRT");
    }

    if (sigaction(SIGFPE, &sa, NULL) == -1) {
        perror("sigaction for SIGFPE");
    }

    if (sigaction(SIGBUS, &sa, NULL) == -1) {
        perror("sigaction for SIGBUS");
    }

    if (sigaction(SIGILL, &sa, NULL) == -1) {
        perror("sigaction for SIGILL");
    }
}
*/
import "C"

// init installs the default signal handlers at package initialization time.
// init 在包初始化时安装默认信号处理器。
func init() {
	C.init_signal_handle()
}
