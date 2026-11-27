/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to TDengine server: %s\n", taos_errstr(NULL));
    taos_cleanup();
    return -1;
  }
  fprintf(stdout, "Connected to 127.0.0.1:6041 successfully.\n");

  /* put your code here for read and write */

  taos_close(taos);
  taos_cleanup();
}
