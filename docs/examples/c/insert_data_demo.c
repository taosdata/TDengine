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
// to compile: gcc -o insert_data_demo insert_data_demo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

static int DemoInsertData() {
// ANCHOR: insert_data
const char *ip        = "localhost";
const char *user      = "root";
const char *password  = "taosdata";

// connect
TAOS *taos = taos_connect(ip, user, password, NULL, 0);
if (taos == NULL) {
  printf("failed to connect to server %s, reason: %s\n", ip, taos_errstr(NULL));
  taos_cleanup();
  return -1;
}
printf("success to connect server %s\n", ip);

// use database
TAOS_RES *result = taos_query(taos, "USE power");
taos_free_result(result);

// insert data, please make sure the database and table are already created
const char* sql = "INSERT INTO "                                                  \
          "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') "     \
          "VALUES "                                                               \
          "(NOW + 1a, 10.30000, 219, 0.31000) "                                   \
          "(NOW + 2a, 12.60000, 218, 0.33000) "                                   \
          "(NOW + 3a, 12.30000, 221, 0.31000) "                                   \
          "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') "    \
          "VALUES "                                                               \
          "(NOW + 1a, 10.30000, 218, 0.25000) ";
result = taos_query(taos, sql);
int code = taos_errno(result);
if (code != 0) {
  printf("failed to insert data to power.meters, ip: %s, reason: %s\n", ip, taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}
taos_free_result(result);

// you can check affectedRows here
int rows = taos_affected_rows(result);
printf("success to insert %d rows data to power.meters\n", rows);

// close & clean
taos_close(taos);
taos_cleanup();
return 0;
// ANCHOR_END: insert_data
}

int main(int argc, char *argv[]) {
  return DemoInsertData();
}
