// compile with
// gcc connect_example.c -o connect_example -ltaos
#include <stdio.h>
#include <stdlib.h>
#include "taos.h"

int main() {
  const char *host = "localhost";
  const char *user = "root";
  const char *passwd = "taosdata";
  const char *db = NULL;      // if don't want to connect to a default db, set it to NULL or ""
  uint16_t    port = 6030;    // 0 means use the default port
  TAOS       *taos = taos_connect(host, user, passwd, db, port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL), taos_errstr(NULL));
    taos_cleanup();
    return -1;
  }
  fprintf(stdout, "Connected to %s:%hu successfully.\n", host, port);
  
  /* put your code here for read and write */

  // close & clean
  taos_close(taos);
  taos_cleanup();
}
