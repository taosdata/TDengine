// to compile: gcc -o connect_example connect_example.c -ltaosws
#include <stdio.h>
#include <stdlib.h>
#include "taosws.h"

int main() {
  int32_t code = 0;
  code = ws_enable_log("debug");
  if (code != 0) {
    fprintf(stderr, "Failed to enable log, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(NULL));
    return -1;
  }

  char    *dsn = "ws://localhost:6041";
  WS_TAOS *taos = ws_connect(dsn);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s, ErrCode: 0x%x, ErrMessage: %s.\n", dsn, ws_errno(NULL), ws_errstr(NULL));
    return -1;
  }
  fprintf(stdout, "Connected to %s successfully.\n", dsn);

  /* put your code here for read and write */

  // close & clean
  code = ws_close(taos);
  if (code != 0) {
    fprintf(stderr, "Failed to close connection, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(NULL));
    return -1;
  }

  return 0;
}
