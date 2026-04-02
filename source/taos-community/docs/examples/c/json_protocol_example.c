// compile with
// gcc -o json_protocol_example json_protocol_example.c -ltaos
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
  TAOS *taos = taos_connect("localhost", "root", "taosdata", "", 6030);
  if (taos == NULL) {
    printf("failed to connect to server\n");
    exit(EXIT_FAILURE);
  }
  executeSQL(taos, "DROP DATABASE IF EXISTS test");
  executeSQL(taos, "CREATE DATABASE test");
  executeSQL(taos, "USE test");
  char *line =
      "[{\"metric\": \"meters.current\", \"timestamp\": 1648432611249, \"value\": 10.3, \"tags\": {\"location\": "
      "\"California.SanFrancisco\", \"groupid\": 2}},{\"metric\": \"meters.voltage\", \"timestamp\": 1648432611249, "
      "\"value\": 219, \"tags\": {\"location\": \"California.LosAngeles\", \"groupid\": 1}},{\"metric\": \"meters.current\", "
      "\"timestamp\": 1648432611250, \"value\": 12.6, \"tags\": {\"location\": \"California.SanFrancisco\", \"groupid\": "
      "2}},{\"metric\": \"meters.voltage\", \"timestamp\": 1648432611250, \"value\": 221, \"tags\": {\"location\": "
      "\"California.LosAngeles\", \"groupid\": 1}}]";

  char     *lines[] = {line};
  TAOS_RES *res = taos_schemaless_insert(taos, lines, 1, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
  if (taos_errno(res) != 0) {
    printf("failed to insert schema-less data, reason: %s\n", taos_errstr(res));
  } else {
    int affectedRow = taos_affected_rows(res);
    printf("successfully inserted %d rows\n", affectedRow);
  }
  taos_free_result(res);
  taos_close(taos);
  taos_cleanup();
}
// output:
// successfully inserted 4 rows
// ANCHOR_END: main
