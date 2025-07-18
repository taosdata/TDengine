// to compile: gcc -o insert_data_demo insert_data_demo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

static int DemoInsertData() {
  // ANCHOR: insert_data
  const char *host = "localhost";
  const char *user = "root";
  const char *password = "taosdata";
  uint16_t    port = 6041;
  int         code = 0;

  // connect
  taos_options(TSDB_OPTION_DRIVER, "websocket");
  TAOS *taos = taos_connect(host, user, password, NULL, port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
            taos_errstr(NULL));
    taos_cleanup();
    return -1;
  }

  // insert data, please make sure the database and table are already created
  const char *sql =
      "INSERT INTO "
      "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') "
      "VALUES "
      "(NOW + 1a, 10.30000, 219, 0.31000) "
      "(NOW + 2a, 12.60000, 218, 0.33000) "
      "(NOW + 3a, 12.30000, 221, 0.31000) "
      "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') "
      "VALUES "
      "(NOW + 1a, 10.30000, 218, 0.25000) ";
  TAOS_RES *result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to insert data to power.meters, sql: %s, ErrCode: 0x%x, ErrMessage: %s\n.", sql, code,
            taos_errstr(result));
    taos_close(taos);
    taos_cleanup();
    return -1;
  }

  // you can check affectedRows here
  int rows = taos_affected_rows(result);
  fprintf(stdout, "Successfully inserted %d rows into power.meters.\n", rows);

  taos_free_result(result);

  taos_close(taos);
  taos_cleanup();
  return 0;
  // ANCHOR_END: insert_data
}

int main() { return DemoInsertData(); }
