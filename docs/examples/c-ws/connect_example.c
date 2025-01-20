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
