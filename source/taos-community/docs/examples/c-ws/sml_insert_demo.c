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
#include "taosws.h"

static int DemoSmlInsert() {
  // ANCHOR: schemaless
  int   code = 0;
  char *dsn = "ws://localhost:6041";

  // connect
  WS_TAOS *taos = ws_connect(dsn);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s, ErrCode: 0x%x, ErrMessage: %s.\n", dsn, ws_errno(NULL), ws_errstr(NULL));
    return -1;
  }

  // create database
  WS_RES *result = ws_query(taos, "CREATE DATABASE IF NOT EXISTS power");
  code = ws_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to create database power, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(result));
    ws_close(taos);
    return -1;
  }
  ws_free_result(result);

  // use database
  result = ws_query(taos, "USE power");
  code = ws_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to execute use power, ErrCode: 0x%x, ErrMessage: %s\n.", code, ws_errstr(result));
    ws_close(taos);
    return -1;
  }
  ws_free_result(result);

  // schemaless demo data
  char *line_demo =
      "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 "
      "1626006833639";
  char *telnet_demo = "metric_telnet 1707095283260 4 host=host0 interface=eth0";
  char *json_demo =
      "{\"metric\": \"metric_json\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, "
      "\"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}";

  // influxdb line protocol
  char *lines[] = {line_demo};
  int   totalLines = 0;
  result = ws_schemaless_insert_raw(taos, line_demo, strlen(line_demo), &totalLines, WS_TSDB_SML_LINE_PROTOCOL,
                                    WS_TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  code = ws_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to insert schemaless line data, data: %s, ErrCode: 0x%x, ErrMessage: %s\n.", line_demo,
            code, ws_errstr(result));
    ws_close(taos);
    return -1;
  }

  fprintf(stdout, "Insert %d rows of schemaless line data successfully.\n", totalLines);
  ws_free_result(result);

  // opentsdb telnet protocol
  totalLines = 0;
  result = ws_schemaless_insert_raw(taos, telnet_demo, strlen(telnet_demo), &totalLines, WS_TSDB_SML_TELNET_PROTOCOL,
                                    WS_TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  code = ws_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to insert schemaless telnet data, data: %s, ErrCode: 0x%x, ErrMessage: %s\n.", telnet_demo,
            code, ws_errstr(result));
    ws_close(taos);
    return -1;
  }

  fprintf(stdout, "Insert %d rows of schemaless telnet data successfully.\n", totalLines);
  ws_free_result(result);

  // opentsdb json protocol
  char *jsons[1] = {0};
  // allocate memory for json data. can not use static memory.
  totalLines = 0;
  result = ws_schemaless_insert_raw(taos, json_demo, strlen(json_demo), &totalLines, WS_TSDB_SML_JSON_PROTOCOL,
                                    WS_TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  code = ws_errno(result);
  if (code != 0) {
    free(jsons[0]);
    fprintf(stderr, "Failed to insert schemaless json data, Server: %s, ErrCode: 0x%x, ErrMessage: %s\n.", json_demo,
            code, ws_errstr(result));
    ws_close(taos);
    return -1;
  }
  free(jsons[0]);

  fprintf(stdout, "Insert %d rows of schemaless json data successfully.\n", totalLines);
  ws_free_result(result);

  // close & clean
  ws_close(taos);
  return 0;
  // ANCHOR_END: schemaless
}

int main(int argc, char *argv[]) { return DemoSmlInsert(); }
