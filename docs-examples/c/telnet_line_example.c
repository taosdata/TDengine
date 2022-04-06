// compile with
// gcc -o line_example line_example.c -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

void execute(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  int       code = taos_errno(res);
  if (code != 0) {
    printf("%s\n", taos_errstr(res));
    taos_free_result(res);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }
  taos_free_result(res);
}

TAOS *connect() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", "", 6030);
  if (taos == NULL) {
    printf("failed to connect to server\n");
    exit(EXIT_FAILURE);
  }
  return taos;
}

// ANCHOR: main
int main() {
  TAOS *taos = connect();
  execute(taos, "CREATE DATABASE test");
  execute(taos, "USE test");
  char *lines[] = {
      "meters.current 1648432611249 10.3 location=Beijing.Chaoyang groupid=2",
      "meters.current 1648432611250 12.6 location=Beijing.Chaoyang groupid=2",
      "meters.current 1648432611249 10.8 location=Beijing.Haidian groupid=3",
      "meters.current 1648432611250 11.3 location=Beijing.Haidian groupid=3",
      "meters.voltage 1648432611249 219 location=Beijing.Chaoyang groupid=2",
      "meters.voltage 1648432611250 218 location=Beijing.Chaoyang groupid=2",
      "meters.voltage 1648432611249 221 location=Beijing.Haidian groupid=3",
      "meters.voltage 1648432611250 217 location=Beijing.Haidian groupid=3",
  };
  TAOS_RES *res = taos_schemaless_insert(taos, lines, 8, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  if (taos_errno(res) != 0) {
    printf("failed to insert schema-less data, reason: %s\n", taos_errstr(res));
  }
  taos_free_result(res);
  taos_close(taos);
  taos_cleanup();
}
// runerror: failed to insert schema-less data, reason: Unable to establish connection
// ANCHOR_END: main
