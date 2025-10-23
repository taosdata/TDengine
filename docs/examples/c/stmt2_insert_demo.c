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

#define NUM_OF_SUB_TABLES 10
#define NUM_OF_ROWS       10

/**
 * @brief Executes an SQL query and checks for errors.
 *
 * @param taos Pointer to TAOS connection.
 * @param sql SQL query string.
 */
void executeSQL(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  int       code = taos_errno(res);
  if (code != 0) {
    fprintf(stderr, "Error: %s\n", taos_errstr(res));
    taos_free_result(res);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }
  taos_free_result(res);
}

/**
 * @brief Checks return status and exits if an error occurs.
 *
 * @param stmt2 Pointer to TAOS_STMT2.
 * @param code Error code.
 * @param msg Error message prefix.
 */
void checkErrorCode(TAOS_STMT2 *stmt2, int code, const char *msg) {
  if (code != 0) {
    fprintf(stderr, "%s. Code: %d, Error: %s\n", msg, code, taos_stmt2_error(stmt2));
    (void)taos_stmt2_close(stmt2);
    exit(EXIT_FAILURE);
  }
}

/**
 * @brief Prepares data bindings for batch insertion.
 *
 * @param table_name Pointer to store allocated table names.
 * @param tags Pointer to store allocated tag bindings.
 * @param params Pointer to store allocated parameter bindings.
 */
void prepareBindData(char ***table_name, TAOS_STMT2_BIND ***tags, TAOS_STMT2_BIND ***params) {
  *table_name = (char **)malloc(NUM_OF_SUB_TABLES * sizeof(char *));
  *tags = (TAOS_STMT2_BIND **)malloc(NUM_OF_SUB_TABLES * sizeof(TAOS_STMT2_BIND *));
  *params = (TAOS_STMT2_BIND **)malloc(NUM_OF_SUB_TABLES * sizeof(TAOS_STMT2_BIND *));

  for (int i = 0; i < NUM_OF_SUB_TABLES; i++) {
    // Allocate and assign table name
    (*table_name)[i] = (char *)malloc(20 * sizeof(char));
    sprintf((*table_name)[i], "d_bind_%d", i);

    // Allocate memory for tags data
    int *gid = (int *)malloc(sizeof(int));
    int *gid_len = (int *)malloc(sizeof(int));
    *gid = i;
    *gid_len = sizeof(int);

    char *location = (char *)malloc(20 * sizeof(char));
    int  *location_len = (int *)malloc(sizeof(int));
    *location_len = sprintf(location, "location_%d", i);

    (*tags)[i] = (TAOS_STMT2_BIND *)malloc(2 * sizeof(TAOS_STMT2_BIND));
    (*tags)[i][0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_INT, gid, gid_len, NULL, 1};
    (*tags)[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BINARY, location, location_len, NULL, 1};

    // Allocate memory for columns data
    (*params)[i] = (TAOS_STMT2_BIND *)malloc(4 * sizeof(TAOS_STMT2_BIND));

    int64_t *ts = (int64_t *)malloc(NUM_OF_ROWS * sizeof(int64_t));
    float   *current = (float *)malloc(NUM_OF_ROWS * sizeof(float));
    int     *voltage = (int *)malloc(NUM_OF_ROWS * sizeof(int));
    float   *phase = (float *)malloc(NUM_OF_ROWS * sizeof(float));
    int32_t *ts_len = (int32_t *)malloc(NUM_OF_ROWS * sizeof(int32_t));
    int32_t *current_len = (int32_t *)malloc(NUM_OF_ROWS * sizeof(int32_t));
    int32_t *voltage_len = (int32_t *)malloc(NUM_OF_ROWS * sizeof(int32_t));
    int32_t *phase_len = (int32_t *)malloc(NUM_OF_ROWS * sizeof(int32_t));

    (*params)[i][0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_TIMESTAMP, ts, ts_len, NULL, NUM_OF_ROWS};
    (*params)[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_FLOAT, current, current_len, NULL, NUM_OF_ROWS};
    (*params)[i][2] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_INT, voltage, voltage_len, NULL, NUM_OF_ROWS};
    (*params)[i][3] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_FLOAT, phase, phase_len, NULL, NUM_OF_ROWS};

    for (int j = 0; j < NUM_OF_ROWS; j++) {
      struct timeval tv;
      (void)gettimeofday(&tv, NULL);
      ts[j] = tv.tv_sec * 1000LL + tv.tv_usec / 1000 + j;
      current[j] = (float)rand() / RAND_MAX * 30;
      voltage[j] = rand() % 300;
      phase[j] = (float)rand() / RAND_MAX;

      ts_len[j] = sizeof(int64_t);
      current_len[j] = sizeof(float);
      voltage_len[j] = sizeof(int);
      phase_len[j] = sizeof(float);
    }
  }
}

/**
 * @brief Frees allocated memory for binding data.
 *
 * @param table_name Pointer to allocated table names.
 * @param tags Pointer to allocated tag bindings.
 * @param params Pointer to allocated parameter bindings.
 */
void freeBindData(char ***table_name, TAOS_STMT2_BIND ***tags, TAOS_STMT2_BIND ***params) {
  for (int i = 0; i < NUM_OF_SUB_TABLES; i++) {
    free((*table_name)[i]);
    for (int j = 0; j < 2; j++) {
      free((*tags)[i][j].buffer);
      free((*tags)[i][j].length);
    }
    free((*tags)[i]);

    for (int j = 0; j < 4; j++) {
      free((*params)[i][j].buffer);
      free((*params)[i][j].length);
    }
    free((*params)[i]);
  }
  free(*table_name);
  free(*tags);
  free(*params);
}

/**
 * @brief Inserts data using the TAOS stmt2 API.
 *
 * @param taos Pointer to TAOS connection.
 */
void insertData(TAOS *taos) {
  TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};
  TAOS_STMT2       *stmt2 = taos_stmt2_init(taos, &option);
  if (!stmt2) {
    fprintf(stderr, "Failed to initialize TAOS statement.\n");
    exit(EXIT_FAILURE);
  }
  // stmt2 prepare sql
  checkErrorCode(stmt2, taos_stmt2_prepare(stmt2, "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)", 0),
                 "Statement preparation failed");

  char            **table_name;
  TAOS_STMT2_BIND **tags, **params;
  prepareBindData(&table_name, &tags, &params);
  // stmt2 bind batch
  TAOS_STMT2_BINDV bindv = {NUM_OF_SUB_TABLES, table_name, tags, params};
  checkErrorCode(stmt2, taos_stmt2_bind_param(stmt2, &bindv, -1), "Parameter binding failed");
  // stmt2 exec batch
  int affected;
  checkErrorCode(stmt2, taos_stmt2_exec(stmt2, &affected), "Execution failed");
  printf("Successfully inserted %d rows.\n", affected);
  // free and close
  freeBindData(&table_name, &tags, &params);
  (void)taos_stmt2_close(stmt2);
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
  executeSQL(
      taos,
      "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
      "(groupId INT, location BINARY(24))");
  insertData(taos);
  taos_close(taos);
  taos_cleanup();
}