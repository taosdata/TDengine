// compile with
// gcc connect_example.c -o connect_example -I /usr/local/taos/include -L /usr/local/taos/driver -ltaos
#include <stdio.h>
#include "taos.h"
#include "taoserror.h"

int main() {
  // if don't want to connect to a default db, set it to NULL.
  const char *db = NULL;
  TAOS       *taos = taos_connect("localhost", "root", "taosdata", db, 6030);
  printf("Connected\n");
  taos_close(taos);
}
