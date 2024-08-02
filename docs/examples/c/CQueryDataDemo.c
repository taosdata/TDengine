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
// to compile: gcc -o CQueryDataDemo CQueryDataDemo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"


static int DemoQueryData() {
// ANCHOR: query_data
const char *ip        = "localhost";
const char *user      = "root";
const char *password  = "taosdata";

// connect
TAOS *taos = taos_connect(ip, user, password, NULL, 0);
if (taos == NULL) {
  printf("failed to connect to server, reason: %s\n", taos_errstr(NULL));
  taos_cleanup();
  return -1;
}

// use database
TAOS_RES *result = taos_query(taos, "USE power");
taos_free_result(result);

// query data, please make sure the database and table are already created
const char* sql = "SELECT * FROM power.meters";
result = taos_query(taos, sql);
int code = taos_errno(result);
if (code != 0) {
  printf("failed to select, reason: %s\n", taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}

TAOS_ROW    row         = NULL;
int         rows        = 0;
int         num_fields  = taos_field_count(result);
TAOS_FIELD *fields      = taos_fetch_fields(result);

printf("fields: %d\n", num_fields);
printf("sql: %s, result:\n", sql);

// fetch the records row by row
while ((row = taos_fetch_row(result))) {
  char temp[1024] = {0};
  rows++;
  taos_print_row(temp, row, fields, num_fields);
  printf("%s\n", temp);
}
printf("total rows: %d\n", rows);
taos_free_result(result);

// close & clean
taos_close(taos);
taos_cleanup();
return 0;
// ANCHOR_END: query_data
}

int main(int argc, char *argv[]) {
  return DemoQueryData();
}
