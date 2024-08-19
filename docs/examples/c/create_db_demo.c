/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

// TAOS standard API example. The same syntax as MySQL, but only a subset
// to compile: gcc -o create_db_demo create_db_demo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

static int DemoCreateDB() {
  // ANCHOR: create_db_and_table
  const char *host = "localhost";
  const char *user = "root";
  const char *password = "taosdata";
  uint16_t    port = 6030;
  int         code = 0;

  // connect
  TAOS *taos = taos_connect(host, user, password, NULL, port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
           taos_errstr(NULL));
    taos_cleanup();
    return -1;
  }

  // create database
  TAOS_RES *result = taos_query(taos, "CREATE DATABASE IF NOT EXISTS power");
  code = taos_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to create database power, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(result));
    taos_close(taos);
    taos_cleanup();
    return -1;
  }
  taos_free_result(result);
  fprintf(stdout, "Create database power successfully.\n");

  // create table
  const char *sql =
      "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId "
      "INT, location BINARY(24))";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to create stable power.meters, ErrCode: 0x%x, ErrMessage: %s\n.", code, taos_errstr(result));
    taos_close(taos);
    taos_cleanup();
    return -1;
  }
  taos_free_result(result);
  fprintf(stdout, "Create stable power.meters successfully.\n");

  // close & clean
  taos_close(taos);
  taos_cleanup();
  return 0;
  // ANCHOR_END: create_db_and_table
}

int main(int argc, char *argv[]) {
  return DemoCreateDB();
}
