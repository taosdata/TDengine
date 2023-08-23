// compile with
// gcc -o stmt_example stmt_example.c -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
    printf("%s\n", taos_errstr(res));
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
void checkErrorCode(TAOS_STMT *stmt, int code, const char* msg) {
  if (code != 0) {
    printf("%s. error: %s\n", msg, taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }
}

typedef struct {
  int64_t ts;
  float current;
  int voltage;
  float phase;
} Row;

/**
 * @brief insert data using stmt API
 * 
 * @param taos 
 */
void insertData(TAOS *taos) {
  // init
  TAOS_STMT *stmt = taos_stmt_init(taos);
  // prepare
  const char *sql = "INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_prepare");
  // bind table name and tags
  TAOS_MULTI_BIND tags[2];
  char* location = "California.SanFrancisco";
  int groupId = 2;
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

  // insert two rows
  Row rows[2] = {
    {1648432611249, 10.3, 219, 0.31},
    {1648432611749, 12.6, 218, 0.33},
  };

  TAOS_MULTI_BIND values[4];
  values[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  values[0].buffer_length = sizeof(int64_t);
  values[0].length = &values[0].buffer_length;
  values[0].is_null = NULL;

  values[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
  values[1].buffer_length = sizeof(float);
  values[1].length = &values[1].buffer_length;
  values[1].is_null = NULL;

  values[2].buffer_type = TSDB_DATA_TYPE_INT;
  values[2].buffer_length = sizeof(int);
  values[2].length = &values[2].buffer_length;
  values[2].is_null = NULL;

  values[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
  values[3].buffer_length = sizeof(float);
  values[3].length = &values[3].buffer_length;
  values[3].is_null = NULL;

  for (int i = 0; i < 2; ++i) {
    values[0].buffer = &rows[i].ts;
    values[1].buffer = &rows[i].current;
    values[2].buffer = &rows[i].voltage;
    values[3].buffer = &rows[i].phase;
    code = taos_stmt_bind_param(stmt, values); // bind param
    checkErrorCode(stmt, code, "failed to execute taos_stmt_bind_param");
    code = taos_stmt_add_batch(stmt); // add batch
    checkErrorCode(stmt, code, "failed to execute taos_stmt_add_batch");
  }
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
  executeSQL(taos, "CREATE DATABASE power");
  executeSQL(taos, "USE power");
  executeSQL(taos, "CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
  insertData(taos);
  taos_close(taos);
  taos_cleanup();
}


// output:
// successfully inserted 2 rows
