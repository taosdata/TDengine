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
// to compile: gcc -o query_data_demo query_data_demo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taosws.h"

static int DemoQueryData() {
  // ANCHOR: query_data
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

  // query data, please make sure the database and table are already created
  const char *sql = "SELECT ts, current, location FROM power.meters limit 100";
  TAOS_RES   *result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to query data from power.meters, sql: %s, ErrCode: 0x%x, ErrMessage: %s\n.", sql, code,
            taos_errstr(result));
    taos_close(taos);
    taos_cleanup();
    return -1;
  }

  TAOS_ROW    row = NULL;
  int         rows = 0;
  int         num_fields = taos_field_count(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  fprintf(stdout, "query successfully, got %d fields, the sql is: %s.\n", num_fields, sql);

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    // Add your data processing logic here

    rows++;
  }
  fprintf(stdout, "total rows: %d\n", rows);
  taos_free_result(result);

  // close & clean
  taos_close(taos);
  taos_cleanup();
  return 0;
  // ANCHOR_END: query_data
}

int main(int argc, char *argv[]) { return DemoQueryData(); }
