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
// gcc -o telnet_line_example telnet_line_example.c -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

void executeSQL(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  int       code = taos_errno(res);
  if (code != 0) {
    printf("%s\n", taos_errstr(res));
    taos_free_result(res);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }
  taos_free_result(res);
}

// ANCHOR: main
int main() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", "", 6030);
  if (taos == NULL) {
    printf("failed to connect to server\n");
    exit(EXIT_FAILURE);
  }
  executeSQL(taos, "DROP DATABASE IF EXISTS test");
  executeSQL(taos, "CREATE DATABASE test");
  executeSQL(taos, "USE test");
  char *lines[] = {
      "meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
      "meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
      "meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
      "meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
      "meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
      "meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
      "meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
      "meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
  };
  TAOS_RES *res = taos_schemaless_insert(taos, lines, 8, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  if (taos_errno(res) != 0) {
    printf("failed to insert schema-less data, reason: %s\n", taos_errstr(res));
  } else {
    int affectedRow = taos_affected_rows(res);
    printf("successfully inserted %d rows\n", affectedRow);
  }

  taos_free_result(res);
  taos_close(taos);
  taos_cleanup();
}
// output:
// successfully inserted 8 rows
// ANCHOR_END: main
