//go:build linux

package system

/*
#include <stdio.h>
#include <signal.h>
#include <unistd.h>

void init_signal_handle() {
    struct sigaction sa;
	sa.sa_handler = SIG_DFL;
    sa.sa_flags = SA_ONSTACK;

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

func init() {
	C.init_signal_handle()
}
