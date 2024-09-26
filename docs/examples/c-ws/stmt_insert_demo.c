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
// to compile: gcc -o stmt_insert_demo stmt_insert_demo.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "taosws.h"

/**
 * @brief execute sql only.
 *
 * @param taos
 * @param sql
 */
void executeSQL(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  int       code = taos_errno(res);
  if (code != 0) {
    fprintf(stderr, "%s\n", taos_errstr(res));
    taos_free_result(res);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }
  taos_free_result(res);
}

/**
 * @brief check return status and exit program when error occur.
 *
 * @param stmt
 * @param code
 * @param msg
 */
void checkErrorCode(TAOS_STMT *stmt, int code, const char *msg) {
  if (code != 0) {
    fprintf(stderr, "%s. code: %d, error: %s\n", msg,code,taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }
}

typedef struct {
  int64_t ts;
  float   current;
  int     voltage;
  float   phase;
} Row;

int num_of_sub_table = 10;
int num_of_row = 10;
int total_affected = 0;
/**
 * @brief insert data using stmt API
 *
 * @param taos
 */
void insertData(TAOS *taos) {
  // init
  TAOS_STMT *stmt = taos_stmt_init(taos);
  if (stmt == NULL) {
      fprintf(stderr, "Failed to init taos_stmt, error: %s\n", taos_stmt_errstr(NULL));
      exit(EXIT_FAILURE);
  }
  // prepare
  const char *sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)";
  int         code = taos_stmt_prepare(stmt, sql, 0);
  checkErrorCode(stmt, code, "Failed to execute taos_stmt_prepare");
  for (int i = 1; i <= num_of_sub_table; i++) {
    char table_name[20];
    sprintf(table_name, "d_bind_%d", i);
    char location[20];
    sprintf(location, "location_%d", i);

    // set table name and tags
    TAOS_MULTI_BIND tags[2];
    // groupId
    tags[0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[0].buffer_length = sizeof(int);
    tags[0].length = (int32_t *)&tags[0].buffer_length;
    tags[0].buffer = &i;
    tags[0].is_null = NULL;
    tags[0].num = 1;
    // location
    tags[1].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[1].buffer_length = strlen(location);
    tags[1].length =(int32_t *) &tags[1].buffer_length;
    tags[1].buffer = location;
    tags[1].is_null = NULL;
    tags[1].num = 1;
    code = taos_stmt_set_tbname_tags(stmt, table_name, tags);
    checkErrorCode(stmt, code, "Failed to set table name and tags\n");

    // insert rows
    TAOS_MULTI_BIND params[4];
    // ts
    params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[0].buffer_length = sizeof(int64_t);
    params[0].length = (int32_t *)&params[0].buffer_length;
    params[0].is_null = NULL;
    params[0].num = 1;
    // current
    params[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[1].buffer_length = sizeof(float);
    params[1].length = (int32_t *)&params[1].buffer_length;
    params[1].is_null = NULL;
    params[1].num = 1;
    // voltage
    params[2].buffer_type = TSDB_DATA_TYPE_INT;
    params[2].buffer_length = sizeof(int);
    params[2].length = (int32_t *)&params[2].buffer_length;
    params[2].is_null = NULL;
    params[2].num = 1;
    // phase
    params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[3].buffer_length = sizeof(float);
    params[3].length = (int32_t *)&params[3].buffer_length;
    params[3].is_null = NULL;
    params[3].num = 1;

    for (int j = 0; j < num_of_row; j++) {
      struct timeval tv;
      gettimeofday(&tv, NULL);
      long long milliseconds = tv.tv_sec * 1000LL + tv.tv_usec / 1000;  // current timestamp in milliseconds
      int64_t   ts = milliseconds + j;
      float     current = (float)rand() / RAND_MAX * 30;
      int       voltage = rand() % 300;
      float     phase = (float)rand() / RAND_MAX;
      params[0].buffer = &ts;
      params[1].buffer = &current;
      params[2].buffer = &voltage;
      params[3].buffer = &phase;
      // bind param
      code = taos_stmt_bind_param(stmt, params);
      checkErrorCode(stmt, code, "Failed to bind param");
    }
    // add batch
    code = taos_stmt_add_batch(stmt);
    checkErrorCode(stmt, code, "Failed to add batch");
    // execute batch
    code = taos_stmt_execute(stmt);
    checkErrorCode(stmt, code, "Failed to exec stmt");
    // get affected rows
    int affected = taos_stmt_affected_rows_once(stmt);
    total_affected += affected;
  }
  fprintf(stdout, "Successfully inserted %d rows to power.meters.\n", total_affected);
  taos_stmt_close(stmt);
}

int main() {
  const char *host      = "localhost";
  const char *user      = "root";
  const char *password  = "taosdata";
  uint16_t    port      = 6030;
  TAOS *taos = taos_connect(host, user, password, NULL, port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL), taos_errstr(NULL));
    taos_cleanup();
    exit(EXIT_FAILURE);
  }
  // create database and table
  executeSQL(taos, "CREATE DATABASE IF NOT EXISTS power");
  executeSQL(taos, "USE power");
  executeSQL(taos,
             "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
             "(groupId INT, location BINARY(24))");
  insertData(taos);
  taos_close(taos);
  taos_cleanup();
}
