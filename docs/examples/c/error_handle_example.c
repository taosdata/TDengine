/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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

// compile with
// gcc error_handle_example.c -o error_handle_example -ltaos
#include <stdio.h>
#include <stdlib.h>
#include "taos.h"

int main() {
  const char *host = "localhost";
  const char *user = "root";
  const char *passwd = "taosdata";
  // if don't want to connect to a default db, set it to NULL or ""
  const char *db = "notexist";
  uint16_t    port = 0;  // 0 means use the default port
  TAOS       *taos = taos_connect(host, user, passwd, db, port);
  if (taos == NULL) {
    int errno = taos_errno(NULL);
    char *msg = taos_errstr(NULL);
    printf("%d, %s\n", errno, msg);
  } else {
    printf("connected\n");
    taos_close(taos);
  }
  taos_cleanup();
}
