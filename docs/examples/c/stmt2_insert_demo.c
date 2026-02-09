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
#include "taos.h"

#define NUM_OF_SUB_TABLES 10
#define NUM_OF_ROWS       10
#define TABLE_NAME_BUF_SZ 20
#define LOCATION_BUF_SZ   20

static void freeBindData(char ***table_name, TAOS_STMT2_BIND ***tags, TAOS_STMT2_BIND ***params);

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
    printf("Error: %s\n", taos_errstr(res));
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
    printf("%s. Code: %d, Error: %s\n", msg, code, taos_stmt2_error(stmt2));
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
 * @return 0 on success, -1 on allocation failure (caller should exit).
 */
static int prepareBindData(char ***table_name, TAOS_STMT2_BIND ***tags, TAOS_STMT2_BIND ***params) {
  *table_name = (char **)malloc(NUM_OF_SUB_TABLES * sizeof(char *));
  if (*table_name == NULL) {
    (void)fprintf(stderr, "malloc table_name array failed\n");
    return -1;
  }
  *tags = (TAOS_STMT2_BIND **)malloc(NUM_OF_SUB_TABLES * sizeof(TAOS_STMT2_BIND *));
  if (*tags == NULL) {
    (void)fprintf(stderr, "malloc tags array failed\n");
    free(*table_name);
    *table_name = NULL;
    return -1;
  }
  *params = (TAOS_STMT2_BIND **)malloc(NUM_OF_SUB_TABLES * sizeof(TAOS_STMT2_BIND *));
  if (*params == NULL) {
    (void)fprintf(stderr, "malloc params array failed\n");
    free(*tags);
    free(*table_name);
    *tags = NULL;
    *table_name = NULL;
    return -1;
  }

  for (int i = 0; i < NUM_OF_SUB_TABLES; i++) {
    (*table_name)[i] = NULL;
    (*tags)[i] = NULL;
    (*params)[i] = NULL;
  }

  for (int i = 0; i < NUM_OF_SUB_TABLES; i++) {
    (*table_name)[i] = (char *)malloc((size_t)TABLE_NAME_BUF_SZ * sizeof(char));
    if ((*table_name)[i] == NULL) {
      (void)fprintf(stderr, "malloc table_name[%d] failed\n", i);
      freeBindData(table_name, tags, params);
      return -1;
    }
    (void)snprintf((*table_name)[i], (size_t)TABLE_NAME_BUF_SZ, "d_bind_%d", i);

    int *gid = (int *)malloc(sizeof(int));
    if (gid == NULL) {
      (void)fprintf(stderr, "malloc gid failed (i=%d)\n", i);
      freeBindData(table_name, tags, params);
      return -1;
    }
    *gid = i;

    char *location = (char *)malloc((size_t)LOCATION_BUF_SZ * sizeof(char));
    if (location == NULL) {
      (void)fprintf(stderr, "malloc location failed (i=%d)\n", i);
      free(gid);
      freeBindData(table_name, tags, params);
      return -1;
    }
    int *location_len = (int *)malloc(sizeof(int));
    if (location_len == NULL) {
      (void)fprintf(stderr, "malloc location_len failed (i=%d)\n", i);
      free(location);
      free(gid);
      freeBindData(table_name, tags, params);
      return -1;
    }
    *location_len = snprintf(location, (size_t)LOCATION_BUF_SZ, "location_%d", i);

    (*tags)[i] = (TAOS_STMT2_BIND *)malloc(2 * sizeof(TAOS_STMT2_BIND));
    if ((*tags)[i] == NULL) {
      (void)fprintf(stderr, "malloc tags[%d] failed\n", i);
      free(location_len);
      free(location);
      free(gid);
      freeBindData(table_name, tags, params);
      return -1;
    }
    (*tags)[i][0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_INT, gid, NULL, NULL, 1};
    (*tags)[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BINARY, location, location_len, NULL, 1};

    (*params)[i] = (TAOS_STMT2_BIND *)malloc(4 * sizeof(TAOS_STMT2_BIND));
    if ((*params)[i] == NULL) {
      (void)fprintf(stderr, "malloc params[%d] failed\n", i);
      freeBindData(table_name, tags, params);
      return -1;
    }

    int64_t *ts = (int64_t *)malloc(NUM_OF_ROWS * sizeof(int64_t));
    float   *current = (float *)malloc(NUM_OF_ROWS * sizeof(float));
    int     *voltage = (int *)malloc(NUM_OF_ROWS * sizeof(int));
    float   *phase = (float *)malloc(NUM_OF_ROWS * sizeof(float));
    if (ts == NULL || current == NULL || voltage == NULL || phase == NULL) {
      (void)fprintf(stderr, "malloc column buffers failed (i=%d)\n", i);
      free(phase);
      free(voltage);
      free(current);
      free(ts);
      free((*params)[i]);
      (*params)[i] = NULL;
      freeBindData(table_name, tags, params);
      return -1;
    }

    (*params)[i][0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_TIMESTAMP, ts, NULL, NULL, NUM_OF_ROWS};
    (*params)[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_FLOAT, current, NULL, NULL, NUM_OF_ROWS};
    (*params)[i][2] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_INT, voltage, NULL, NULL, NUM_OF_ROWS};
    (*params)[i][3] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_FLOAT, phase, NULL, NULL, NUM_OF_ROWS};

    for (int j = 0; j < NUM_OF_ROWS; j++) {
      ts[j] = 1609459200000LL + (int64_t)i * NUM_OF_ROWS * 1000LL + (int64_t)j * 1000LL;
      current[j] = 10.0f + (float)j * 0.5f;
      voltage[j] = 100 + j * 10;
      phase[j] = 0.1f + (float)j * 0.05f;
    }
  }
  return 0;
}

/**
 * @brief Frees allocated memory for binding data.
 * Safe to call with partially allocated state (NULL pointers are no-op).
 *
 * @param table_name Pointer to allocated table names.
 * @param tags Pointer to allocated tag bindings.
 * @param params Pointer to allocated parameter bindings.
 */
static void freeBindData(char ***table_name, TAOS_STMT2_BIND ***tags, TAOS_STMT2_BIND ***params) {
  if (table_name == NULL || *table_name == NULL) return;
  if (tags == NULL || *tags == NULL) {
    free(*table_name);
    *table_name = NULL;
    return;
  }
  if (params == NULL || *params == NULL) {
    for (int i = 0; i < NUM_OF_SUB_TABLES; i++) {
      free((*table_name)[i]);
      if ((*tags)[i] != NULL) {
        free((*tags)[i][0].buffer);
        free((*tags)[i][1].buffer);
        free((*tags)[i][1].length);
        free((*tags)[i]);
      }
    }
    free(*table_name);
    free(*tags);
    *table_name = NULL;
    *tags = NULL;
    return;
  }
  for (int i = 0; i < NUM_OF_SUB_TABLES; i++) {
    free((*table_name)[i]);
    if ((*tags)[i] != NULL) {
      free((*tags)[i][0].buffer);
      free((*tags)[i][1].buffer);
      free((*tags)[i][1].length);
      free((*tags)[i]);
    }
    if ((*params)[i] != NULL) {
      for (int j = 0; j < 4; j++) {
        free((*params)[i][j].buffer);
        free((*params)[i][j].length);
      }
      free((*params)[i]);
    }
  }
  free(*table_name);
  free(*tags);
  free(*params);
  *table_name = NULL;
  *tags = NULL;
  *params = NULL;
}

/**
 * @brief Inserts data using the TAOS stmt2 API.
 *
 * @param taos Pointer to TAOS connection.
 */
void insertData(TAOS *taos) {
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2       *stmt2 = taos_stmt2_init(taos, &option);
  if (!stmt2) {
    printf("Failed to initialize TAOS statement.\n");
    exit(EXIT_FAILURE);
  }
  // stmt2 prepare sql
  checkErrorCode(stmt2, taos_stmt2_prepare(stmt2, "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)", 0),
                 "Statement preparation failed");

  char            **table_name = NULL;
  TAOS_STMT2_BIND **tags = NULL, **params = NULL;
  if (prepareBindData(&table_name, &tags, &params) != 0) {
    printf("Failed to prepare bind data (out of memory).\n");
    (void)taos_stmt2_close(stmt2);
    exit(EXIT_FAILURE);
  }
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

int main(void) {
  const char *host = "localhost";
  const char *user = "root";
  const char *password = "taosdata";
  uint16_t    port = 6030;
  TAOS       *taos = taos_connect(host, user, password, NULL, port);
  if (taos == NULL) {
    (void)fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
                  taos_errstr(NULL));
    taos_cleanup();
    exit(EXIT_FAILURE);
  }
  executeSQL(taos, "CREATE DATABASE IF NOT EXISTS power");
  executeSQL(taos, "USE power");
  executeSQL(taos,
             "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
             "TAGS (groupId INT, location BINARY(24))");
  insertData(taos);
  taos_close(taos);
  taos_cleanup();
  return 0;
}