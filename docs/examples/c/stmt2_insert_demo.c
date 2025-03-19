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
// to compile: gcc -o stmt2_insert_demo stmt2_insert_demo.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "taos.h"

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
void checkErrorCode(TAOS_STMT2 *stmt2, int code, const char *msg) {
  if (code != 0) {
    fprintf(stderr, "%s. code: %d, error: %s\n", msg, code, taos_stmt2_error(stmt2));
    taos_stmt2_close(stmt2);
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
 * @brief insert data using stmt2 API
 *
 * @param taos
 */
void insertData(TAOS *taos) {
  // init
  TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};
  TAOS_STMT2       *stmt2 = taos_stmt2_init(taos, &option);
  if (stmt2 == NULL) {
    fprintf(stderr, "Failed to init taos_stmt2, error: %s\n", taos_stmt_errstr(NULL));
    exit(EXIT_FAILURE);
  }
  // prepare
  const char *sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)";
  int         code = taos_stmt2_prepare(stmt2, sql, 0);
  checkErrorCode(stmt2, code, "Failed to execute taos_stmt_prepare");
  // prepare bind params, batch bind recommended malloc memory for each param
  char            **table_name = (char **)malloc(num_of_sub_table * sizeof(char *));
  TAOS_STMT2_BIND **tags = (TAOS_STMT2_BIND **)malloc(num_of_sub_table * sizeof(TAOS_STMT2_BIND *));
  TAOS_STMT2_BIND **params = (TAOS_STMT2_BIND **)malloc(num_of_sub_table * sizeof(TAOS_STMT2_BIND *));
  char            **location = (char **)malloc(num_of_sub_table * sizeof(char *));
  int              *gid = (int *)malloc(num_of_sub_table * sizeof(int *));
  for (int i = 0; i < num_of_sub_table; i++) {
    // tbnaem
    table_name[i] = (char *)malloc(sizeof(char) * 20);
    sprintf(table_name[i], "d_bind_%d", i);

    // tags data and length
    location[i] = (char *)malloc(sizeof(char) * 20);
    int tag1_length = sprintf(location[i], "location_%d", i);
    gid[i] = i;
    int tag0_length = sizeof(int);
    // build tags
    tags[i] = (TAOS_STMT2_BIND *)malloc(2 * sizeof(TAOS_STMT2_BIND));
    // groupId
    tags[i][0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i][0].length = &tag0_length;
    tags[i][0].buffer = &gid[i];
    tags[i][0].is_null = NULL;
    tags[i][0].num = 1;
    // location
    tags[i][1].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i][1].length = &tag1_length;
    tags[i][1].buffer = location[i];
    tags[i][1].is_null = NULL;
    tags[i][1].num = 1;

    // build cols
    params[i] = (TAOS_STMT2_BIND *)malloc(4 * sizeof(TAOS_STMT2_BIND));
    // ts
    params[i][0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i][0].is_null = NULL;
    params[i][0].num = num_of_row;
    // current
    params[i][1].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i][1].is_null = NULL;
    params[i][1].num = num_of_row;
    // voltage
    params[i][2].buffer_type = TSDB_DATA_TYPE_INT;
    params[i][2].is_null = NULL;
    params[i][2].num = num_of_row;
    // phase
    params[i][3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i][3].is_null = NULL;
    params[i][3].num = num_of_row;
    // col data and data length
    int64_t *ts = (int64_t *)malloc(num_of_row * sizeof(int64_t));
    float   *current = (float *)malloc(num_of_row * sizeof(float));
    int     *voltage = (int *)malloc(num_of_row * sizeof(int));
    float   *phase = (float *)malloc(num_of_row * sizeof(float));
    int32_t *ts_len = (int32_t *)malloc(num_of_row * sizeof(int32_t));
    int32_t *current_len = (int32_t *)malloc(num_of_row * sizeof(int32_t));
    int32_t *voltage_len = (int32_t *)malloc(num_of_row * sizeof(int32_t));
    int32_t *phase_len = (int32_t *)malloc(num_of_row * sizeof(int32_t));

    params[i][0].buffer = ts;
    params[i][0].length = ts_len;

    params[i][1].buffer = current;
    params[i][1].length = current_len;

    params[i][2].buffer = voltage;
    params[i][2].length = voltage_len;

    params[i][3].buffer = phase;
    params[i][3].length = phase_len;

    for (int j = 0; j < num_of_row; j++) {
      struct timeval tv;
      gettimeofday(&tv, NULL);
      long long milliseconds = tv.tv_sec * 1000LL + tv.tv_usec / 1000;  // current timestamp in milliseconds
      ts[j] = milliseconds + j;
      current[j] = (float)rand() / RAND_MAX * 30;
      voltage[j] = rand() % 300;
      phase[j] = (float)rand() / RAND_MAX;

      ts_len[j] = sizeof(int64_t);
      current_len[j] = sizeof(float);
      voltage_len[j] = sizeof(int);
      phase_len[j] = sizeof(float);
    }
  }
  // bind batch only once
  TAOS_STMT2_BINDV bindv = {num_of_sub_table, table_name, tags, params};
  code = taos_stmt2_bind_param(stmt2, &bindv, -1);
  checkErrorCode(stmt2, code, "Failed to bind param");
  // execute batch only once
  int affected = 0;
  code = taos_stmt2_exec(stmt2, &affected);
  checkErrorCode(stmt2, code, "Failed to exec stmt");
  // get affected rows
  fprintf(stdout, "Successfully inserted %d rows to power.meters.\n", affected);

  // free bind data
  for (int i = 0; i < num_of_sub_table; i++) {
    for (int j = 0; j < num_of_row; j++) {
      free(params[i][0].buffer);
      free(params[i][1].buffer);
      free(params[i][2].buffer);
      free(params[i][3].buffer);
      free(params[i][0].length);
      free(params[i][1].length);
      free(params[i][2].length);
      free(params[i][3].length);
    }
    free(table_name[i]);
    free(tags[i]);
    free(params[i]);
    free(location[i]);
  }
  free(table_name);
  free(tags);
  free(params);
  free(location);
  free(gid);
  // close and free stmt2
  taos_stmt2_close(stmt2);
}

int main() {
  const char *host = "localhost";
  const char *user = "root";
  const char *password = "taosdata";
  uint16_t    port = 6030;
  TAOS       *taos = taos_connect(host, user, password, NULL, port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
            taos_errstr(NULL));
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