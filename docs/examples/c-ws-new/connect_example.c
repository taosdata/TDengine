// to compile: gcc -o connect_example connect_example.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include "taos.h"

int main() {
  const char *host = "localhost";
  const char *user = "root";
  const char *passwd = "taosdata";
  const char *db = NULL;
  uint16_t    port = 6041;
  taos_options(TSDB_OPTION_DRIVER, "websocket");
  TAOS *taos = taos_connect(host, user, passwd, db, port);
  fprintf(stdout, "Connected to %s:%hu successfully.\n", host, port);

  /* put your code here for read and write */

  taos_close(taos);
  taos_cleanup();
}
