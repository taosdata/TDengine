// compile with
// gcc -o line_example line_example.c -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

void executeSQL(TAOS *taos, const char *sql) {
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

// ANCHOR: main
int main() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", "", 0);
  if (taos == NULL) {
    printf("failed to connect to server\n");
    exit(EXIT_FAILURE);
  }
  executeSQL(taos, "DROP DATABASE IF EXISTS test");
  executeSQL(taos, "CREATE DATABASE test");
  executeSQL(taos, "USE test");
  char     *lines[] = {"meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",
                   "meters,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250",
                   "meters,location=California.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249",
                   "meters,location=California.LosAngeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250"};
  TAOS_RES *res = taos_schemaless_insert(taos, lines, 4, TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  if (taos_errno(res) != 0) {
    printf("failed to insert schema-less data, reason: %s\n", taos_errstr(res));
  } else {
    int affectedRows = taos_affected_rows(res);
    printf("successfully inserted %d rows\n", affectedRows);
  }
  taos_free_result(res);
  taos_close(taos);
  taos_cleanup();
}
// output:
// successfully inserted 4 rows
// ANCHOR_END: main