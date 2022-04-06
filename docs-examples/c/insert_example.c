// compile with
// gcc -o insert_example insert_example.c -ltaos
#include <stdio.h>
#include <stdlib.h>
#include "taos.h"

void execute(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  int       code = taos_errno(res);
  if (code != 0) {
    printf("Error code: %d; Message: %s\n", code, taos_errstr(res));
    taos_free_result(res);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }
  taos_free_result(res);
}

TAOS* connect() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 6030);
  if (taos == NULL) {
    printf("failed to connect to server\n");
    exit(EXIT_FAILURE);
  }
  return taos;
}

int main() {
  TAOS *taos = connect();
  execute(taos, "CREATE DATABASE power");
  execute(taos, "USE power");
  execute(taos, "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");

  execute(taos, "INSERT INTO d1001 USING meters TAGS(Beijing.Chaoyang, 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)"
                "d1002 USING meters TAGS(Beijing.Chaoyang, 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)"
                "d1003 USING meters TAGS(Beijing.Haidian, 2) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)"
                "d1004 USING meters TAGS(Beijing.Haidian, 3) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)");
  
  taos_close(taos);
  taos_cleanup();
}
