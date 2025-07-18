// to compile: gcc -o create_db_demo create_db_demo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

static int DemoCreateDB() {
  // ANCHOR: create_db_and_table
  const char *host = "localhost";
  const char *user = "root";
  const char *password = "taosdata";
  uint16_t    port = 6041;
  int         code = 0;

  // connect
  taos_options(TSDB_OPTION_DRIVER, "websocket");
  TAOS *taos = taos_connect(host, user, password, NULL, port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
            taos_errstr(NULL));
    taos_cleanup();
    return -1;
  }

  // create database
  TAOS_RES *result = taos_query(taos, "CREATE DATABASE IF NOT EXISTS power");
  code = taos_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to create database power, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(result));
    taos_close(taos);
    taos_cleanup();
    return -1;
  }
  taos_free_result(result);
  fprintf(stdout, "Create database power successfully.\n");

  // create table
  const char *sql =
      "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId "
      "INT, location BINARY(24))";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to create stable power.meters, ErrCode: 0x%x, ErrMessage: %s\n.", code,
            taos_errstr(result));
    taos_close(taos);
    taos_cleanup();
    return -1;
  }
  taos_free_result(result);
  fprintf(stdout, "Create stable power.meters successfully.\n");

  taos_close(taos);
  taos_cleanup();
  return 0;
  // ANCHOR_END: create_db_and_table
}

int main() { return DemoCreateDB(); }
