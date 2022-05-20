// compile with
// gcc error_handle_example.c -o error_handle_example -ltaos
#include <stdio.h>
#include <stdlib.h>
#include "taos.h"

int main() {
  const char *host = "localhost";
  const char *user = "root";
  const char *passwd = "taosdata";
  // if don't want to connect to a default db, set it to NULL or ""
  const char *db = "notexist";
  uint16_t    port = 0;  // 0 means use the default port
  TAOS       *taos = taos_connect(host, user, passwd, db, port);
  if (taos == NULL) {
    int errno = taos_errno(NULL);
    char *msg = taos_errstr(NULL);
    printf("%d, %s\n", errno, msg);
  } else {
    printf("connected\n");
    taos_close(taos);
  }
  taos_cleanup();
}
