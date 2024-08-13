/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

// TAOS standard API example. The same syntax as MySQL, but only a subset
// to compile: gcc -o sml_insert_demo sml_insert_demo.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"


static int DemoSmlInsert() {
// ANCHOR: schemaless
const char *host      = "localhost";
const char *user      = "root";
const char *password  = "taosdata";
uint16_t    port      = 6030;
int code  = 0;

// connect
TAOS *taos = taos_connect(host, user, password, NULL, port);
if (taos == NULL) {
  printf("Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL), taos_errstr(NULL));
  taos_cleanup();
  return -1;
}

// create database
TAOS_RES *result = taos_query(taos, "CREATE DATABASE IF NOT EXISTS power");
code = taos_errno(result);
if (code != 0) {
  printf("Failed to create database power, Server: %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, code, taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}
taos_free_result(result);
printf("Create database power successfully.\n");

// use database
result = taos_query(taos, "USE power");
code = taos_errno(result);
if (code != 0) {
  printf("Failed to execute use power, Server: %s:%hu, ErrCode: 0x%x, ErrMessage: %s\n.", host, port, code, taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}
taos_free_result(result);

// schemaless demo data
char * line_demo = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639";
char * telnet_demo = "metric_telnet 1707095283260 4 host=host0 interface=eth0";
char * json_demo = "{\"metric\": \"metric_json\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}";

// influxdb line protocol
char *lines[] = {line_demo};
result = taos_schemaless_insert(taos, lines, 1, TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_MILLI_SECONDS);
code = taos_errno(result);
if (code != 0) {
  printf("Failed to insert schemaless line data, Server: %s:%hu, ErrCode: 0x%x, ErrMessage: %s\n.", host, port, code, taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}

int rows = taos_affected_rows(result);
printf("Insert %d rows of schemaless line data successfully.\n", rows);
taos_free_result(result);

// opentsdb telnet protocol
char *telnets[] = {telnet_demo};
result = taos_schemaless_insert(taos, telnets, 1, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_MILLI_SECONDS);
code = taos_errno(result);
if (code != 0) {
  printf("Failed to insert schemaless telnet data, Server: %s:%hu, ErrCode: 0x%x, ErrMessage: %s\n.", host, port, code, taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}

rows = taos_affected_rows(result);
printf("Insert %d rows of schemaless telnet data successfully.\n", rows);
taos_free_result(result);

// opentsdb json protocol
char *jsons[1] = {0};
// allocate memory for json data. can not use static memory.
jsons[0] = malloc(1024);
if (jsons[0] == NULL) {
  printf("Failed to allocate memory\n");
  taos_close(taos);
  taos_cleanup();
  return -1;
}
(void)strncpy(jsons[0], json_demo, 1023);
result = taos_schemaless_insert(taos, jsons, 1, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);
code = taos_errno(result);
if (code != 0) {
  free(jsons[0]);
  printf("Failed to insert schemaless json data, Server: %s:%hu, ErrCode: 0x%x, ErrMessage: %s\n.", host, port, code, taos_errstr(result));
  taos_close(taos);
  taos_cleanup();
  return -1;
}
free(jsons[0]);

rows = taos_affected_rows(result);
printf("Insert %d rows of schemaless json data successfully.\n", rows);
taos_free_result(result);

// close & clean
taos_close(taos);
taos_cleanup();
return 0;
// ANCHOR_END: schemaless
}

int main(int argc, char *argv[]) {
  return DemoSmlInsert();
}
