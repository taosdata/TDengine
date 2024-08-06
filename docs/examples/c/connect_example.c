// compile with
// gcc connect_example.c -o connect_example -ltaos
#include <stdio.h>
#include <stdlib.h>
#include "taos.h"

int main() {
  const char *host = "localhost";
  const char *user = "root";
  const char *passwd = "taosdata";
  // if don't want to connect to a default db, set it to NULL or ""
  const char *db = NULL;
  uint16_t    port = 0;  // 0 means use the default port
  TAOS       *taos = taos_connect(host, user, passwd, db, port);
  if (taos == NULL) {
    int errno = taos_errno(NULL);
    const char *msg = taos_errstr(NULL);
    printf("%d, %s\n", errno, msg);
    printf("failed to connect to server %s, errno: %d, msg: %s\n", host, errno, msg);
    taos_cleanup();
    return -1;
  }
  printf("success to connect server %s\n", host);
  
  /* put your code here for read and write */

  // close & clean
  taos_close(taos);
  taos_cleanup();
}
