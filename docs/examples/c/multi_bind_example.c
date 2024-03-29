// compile with
// gcc -o multi_bind_example multi_bind_example.c -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

/**
 * @brief execute sql only and ignore result set
 *
 * @param taos
 * @param sql
 */
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

/**
 * @brief exit program when error occur.
 *
 * @param stmt
 * @param code
 * @param msg
 */
void checkErrorCode(TAOS_STMT *stmt, int code, const char *msg) {
  if (code != 0) {
    printf("%s. error: %s\n", msg, taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }
}

/**
 * @brief insert data using stmt API
 *
 * @param taos
 */
void insertData(TAOS *taos) {
  // init
  TAOS_STMT *stmt = taos_stmt_init(taos);
  // prepare
  const char *sql = "INSERT INTO ? USING meters TAGS(?, ?) values(?, ?, ?, ?)";
  int         code = taos_stmt_prepare(stmt, sql, 0);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_prepare");
  // bind table name and tags
  TAOS_MULTI_BIND tags[2];
  char     *location = "California.SanFrancisco";
  int       groupId = 2;
  tags[0].buffer_type = TSDB_DATA_TYPE_BINARY;
  tags[0].buffer_length = strlen(location);
  tags[0].length = &tags[0].buffer_length;
  tags[0].buffer = location;
  tags[0].is_null = NULL;

  tags[1].buffer_type = TSDB_DATA_TYPE_INT;
  tags[1].buffer_length = sizeof(int);
  tags[1].length = &tags[1].buffer_length;
  tags[1].buffer = &groupId;
  tags[1].is_null = NULL;

  code = taos_stmt_set_tbname_tags(stmt, "d1001", tags);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_set_tbname_tags");

  // highlight-start
  // insert two rows with multi binds
  TAOS_MULTI_BIND params[4];
  // values to bind
  int64_t ts[] = {1648432611249, 1648432611749};
  float   current[] = {10.3, 12.6};
  int     voltage[] = {219, 218};
  float   phase[] = {0.31, 0.33};
  // is_null array
  char is_null[2] = {0};
  // length array
  int32_t int64Len[2] = {sizeof(int64_t)};
  int32_t floatLen[2] = {sizeof(float)};
  int32_t intLen[2] = {sizeof(int)};

  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(int64_t);
  params[0].buffer = ts;
  params[0].length = int64Len;
  params[0].is_null = is_null;
  params[0].num = 2;

  params[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[1].buffer_length = sizeof(float);
  params[1].buffer = current;
  params[1].length = floatLen;
  params[1].is_null = is_null;
  params[1].num = 2;

  params[2].buffer_type = TSDB_DATA_TYPE_INT;
  params[2].buffer_length = sizeof(int);
  params[2].buffer = voltage;
  params[2].length = intLen;
  params[2].is_null = is_null;
  params[2].num = 2;

  params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[3].buffer_length = sizeof(float);
  params[3].buffer = phase;
  params[3].length = floatLen;
  params[3].is_null = is_null;
  params[3].num = 2;

  code = taos_stmt_bind_param_batch(stmt, params); // bind batch
  checkErrorCode(stmt, code, "failed to execute taos_stmt_bind_param_batch");
  code = taos_stmt_add_batch(stmt);  // add batch
  checkErrorCode(stmt, code, "failed to execute taos_stmt_add_batch");
  // highlight-end
  // execute
  code = taos_stmt_execute(stmt);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_execute");
  int affectedRows = taos_stmt_affected_rows(stmt);
  printf("successfully inserted %d rows\n", affectedRows);
  // close
  taos_stmt_close(stmt);
}

int main() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 6030);
  if (taos == NULL) {
    printf("failed to connect to server\n");
    exit(EXIT_FAILURE);
  }
  executeSQL(taos, "DROP DATABASE IF EXISTS power");
  executeSQL(taos, "CREATE DATABASE power");
  executeSQL(taos, "USE power");
  executeSQL(taos,
             "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), "
             "groupId INT)");
  insertData(taos);
  taos_close(taos);
  taos_cleanup();
}

// output:
// successfully inserted 2 rows
