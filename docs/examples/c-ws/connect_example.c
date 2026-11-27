/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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
// gcc connect_example.c -o connect_example -ltaos
#include <stdio.h>
#include <stdlib.h>
#include "taosws.h"

int main() {
  ws_enable_log("debug");
  char    *dsn = "ws://localhost:6041";
  WS_TAOS *taos = ws_connect(dsn);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s, ErrCode: 0x%x, ErrMessage: %s.\n", dsn, ws_errno(NULL), ws_errstr(NULL));
    return -1;
  }
  fprintf(stdout, "Connected to %s successfully.\n", dsn);

  /* put your code here for read and write */

  // close & clean
  ws_close(taos);
}
