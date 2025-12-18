// to compile: gcc -o taos_connect_with_example taos_connect_with_example.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include "taos.h"

int main() {
	int code = taos_options(TSDB_OPTION_DRIVER, "websocket");
	if (code != 0) {
		fprintf(stderr, "Failed to set driver option, code: %d\n", code);
		return -1;
	}

	OPTIONS opt = {0};
	taos_set_option(&opt, "ip", "127.0.0.1");
	taos_set_option(&opt, "user", "root");
	taos_set_option(&opt, "pass", "taosdata");
	taos_set_option(&opt, "port", "6041");
	taos_set_option(&opt, "timezone", "UTC");
	taos_set_option(&opt, "userIp", "127.0.0.1");
	taos_set_option(&opt, "userApp", "user_app");
	taos_set_option(&opt, "compression", "0");
	taos_set_option(&opt, "connRetries", "5");
	taos_set_option(&opt, "retryBackoffMs", "200");
	taos_set_option(&opt, "retryBackoffMaxMs", "2000");

	TAOS *taos = taos_connect_with(&opt);
	fprintf(stdout, "Connected to 127.0.0.1:6041 successfully.\n");

	/* put your code here for read and write */

	taos_close(taos);
	taos_cleanup();
}
