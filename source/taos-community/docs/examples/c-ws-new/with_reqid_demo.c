// to compile: gcc -o with_reqid_demo with_reqid_demo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

static int DemoWithReqId() {
  // ANCHOR: with_reqid
  const char *host = "localhost";
  const char *user = "root";
  const char *password = "taosdata";
  uint16_t    port = 6041;
  int         code = 0;

  code = taos_options(TSDB_OPTION_DRIVER, "websocket");
  if (code != 0) {
    fprintf(stderr, "Failed to set driver option, code: %d\n", code);
    return -1;
  }

  // connect
  TAOS *taos = taos_connect(host, user, password, NULL, port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
            taos_errstr(NULL));
    taos_cleanup();
    return -1;
  }

  const char *sql = "SELECT ts, current, location FROM power.meters limit 1";
  // query data with reqid
  long      reqid = 3L;
  TAOS_RES *result = taos_query_with_reqid(taos, sql, reqid);
  code = taos_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to execute sql withQID: %ld, ErrCode: 0x%x, ErrMessage: %s\n.", reqid, code,
            taos_errstr(result));
    taos_close(taos);
    taos_cleanup();
    return -1;
  }

  TAOS_ROW    row = NULL;
  int         rows = 0;
  char        buffer[1024];
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  fprintf(stdout, "query successfully, got %d fields, the sql is: %s.\n", num_fields, sql);

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    // Add your data processing logic here

    rows++;

    // Print the data for easy debugging. You can uncomment them if needed.
    // code = taos_print_row(buffer, row, fields, num_fields);
    // if (code > 0) {
    //   fprintf(stdout, "row %d: %s\n", rows, buffer);
    // } else {
    //   fprintf(stderr, "Failed to print row %d data, ErrCode: 0x%x, ErrMessage: %s\n", rows, taos_errno(NULL),
    //           taos_errstr(NULL));
    // }
  }
  fprintf(stdout, "total rows: %d\n", rows);
  taos_free_result(result);

  // close & clean
  taos_close(taos);
  taos_cleanup();
  return 0;
  // ANCHOR_END: with_reqid
}

int main(int argc, char *argv[]) { return DemoWithReqId(); }
