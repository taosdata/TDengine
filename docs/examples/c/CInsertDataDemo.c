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
// to compile: gcc -o CInsertDataDemo CInsertDataDemo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

static int DemoInsertData() {
// ANCHOR: insert_data
  int         ret_code  = -1;
  const char *ip        = "localhost";
  const char *user      = "root";
  const char *password  = "taosdata";

  // connect
  TAOS *taos = taos_connect(ip, user, password, NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason: %s\n", taos_errstr(NULL));
    goto end;
  }

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
    printf("failed to insert rows, reason: %s\n", taos_errstr(result));
    goto end;
  }
  taos_free_result(result);

  // you can check affectedRows here
  int rows = taos_affected_rows(result);
  printf("success to insert %d rows\n", rows);
  ret_code = 0;

end:
  // close & clean
  taos_close(taos);
  taos_cleanup();
  return ret_code;
// ANCHOR_END: insert_data
}

int main(int argc, char *argv[]) {
  return DemoInsertData();
}
