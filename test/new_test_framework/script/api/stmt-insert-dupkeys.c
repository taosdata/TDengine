// compile with
// gcc -o stmt-insert-dupkeys stmt-insert-dupkeys.c -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

#define NUMROWS 3

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
    exit(EXIT_FAILURE);
  }
}

void prepareBindTags(TAOS_MULTI_BIND *tags) {
  // bind table name and tags
  char     *location = "California.SanFrancisco";
  int       groupId = 2;
  tags[0].buffer_type = TSDB_DATA_TYPE_BINARY;
  tags[0].buffer_length = strlen(location);
  tags[0].length = (int32_t *)&tags[0].buffer_length;
  tags[0].buffer = location;
  tags[0].is_null = NULL;

  tags[1].buffer_type = TSDB_DATA_TYPE_INT;
  tags[1].buffer_length = sizeof(int);
  tags[1].length = (int32_t *)&tags[1].buffer_length;
  tags[1].buffer = &groupId;
  tags[1].is_null = NULL;
}

void prepareBindParams(TAOS_MULTI_BIND *params, int64_t *ts, float *current, int *voltage, float *phase) {
  // is_null array
  char is_null[NUMROWS] = {0};
  // length array
  int32_t int64Len[NUMROWS] = {sizeof(int64_t)};
  int32_t floatLen[NUMROWS] = {sizeof(float)};
  int32_t intLen[NUMROWS] = {sizeof(int)};

  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(int64_t);
  params[0].buffer = ts;
  params[0].length = int64Len;
  params[0].is_null = is_null;
  params[0].num = NUMROWS;

  params[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[1].buffer_length = sizeof(float);
  params[1].buffer = current;
  params[1].length = floatLen;
  params[1].is_null = is_null;
  params[1].num = NUMROWS;

  params[2].buffer_type = TSDB_DATA_TYPE_INT;
  params[2].buffer_length = sizeof(int);
  params[2].buffer = voltage;
  params[2].length = intLen;
  params[2].is_null = is_null;
  params[2].num = NUMROWS;

  params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[3].buffer_length = sizeof(float);
  params[3].buffer = phase;
  params[3].length = floatLen;
  params[3].is_null = is_null;
  params[3].num = NUMROWS;
}

/**
 * @brief insert data using stmt API
 *
 * @param taos
 */
void insertData(TAOS *taos, int64_t *ts, float *current, int *voltage, float *phase) {
  // init
  TAOS_STMT *stmt = taos_stmt_init(taos);

  // prepare
  const char *sql = "INSERT INTO ? USING meters TAGS(?, ?) values(?, ?, ?, ?)";
  int         code = taos_stmt_prepare(stmt, sql, 0);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_prepare");

  // bind table name and tags
  TAOS_MULTI_BIND tags[2];
  prepareBindTags(tags);
  code = taos_stmt_set_tbname_tags(stmt, "d1001", tags);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_set_tbname_tags");

  TAOS_MULTI_BIND params[4];
  prepareBindParams(params, ts, current, voltage, phase);

  code = taos_stmt_bind_param_batch(stmt, params); // bind batch
  checkErrorCode(stmt, code, "failed to execute taos_stmt_bind_param_batch");

  code = taos_stmt_add_batch(stmt);  // add batch
  checkErrorCode(stmt, code, "failed to execute taos_stmt_add_batch");

  // execute
  code = taos_stmt_execute(stmt);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_execute");

  int affectedRows = taos_stmt_affected_rows(stmt);
  printf("successfully inserted %d rows\n", affectedRows);

  // close
  (void)taos_stmt_close(stmt);
}

void insertDataInterlace(TAOS *taos, int64_t *ts, float *current, int *voltage, float *phase) {
  // init with interlace mode
  TAOS_STMT_OPTIONS op;
  op.reqId = 0;
  op.singleStbInsert = true;
  op.singleTableBindOnce = true;
  TAOS_STMT *stmt = taos_stmt_init_with_options(taos, &op);

  // prepare
  const char *sql = "INSERT INTO ? values(?, ?, ?, ?)";
  int         code = taos_stmt_prepare(stmt, sql, 0);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_prepare");

  // bind table name and tags
  TAOS_MULTI_BIND tags[2];
  prepareBindTags(tags);
  code = taos_stmt_set_tbname_tags(stmt, "d1001", tags);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_set_tbname_tags");

  TAOS_MULTI_BIND params[4];
  prepareBindParams(params, ts, current, voltage, phase);

  code = taos_stmt_bind_param_batch(stmt, params); // bind batch
  checkErrorCode(stmt, code, "failed to execute taos_stmt_bind_param_batch");

  code = taos_stmt_add_batch(stmt);  // add batch
  checkErrorCode(stmt, code, "failed to execute taos_stmt_add_batch");

  // execute
  code = taos_stmt_execute(stmt);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_execute");

  int affectedRows = taos_stmt_affected_rows(stmt);
  printf("successfully inserted %d rows\n", affectedRows);

  // close
  (void)taos_stmt_close(stmt);
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

  // initial insert, expect insert 3 rows
  int64_t ts0[] = {1648432611234, 1648432611345, 1648432611456};
  float   current0[] = {10.1f, 10.2f, 10.3f};
  int     voltage0[] = {216, 217, 218};
  float   phase0[] = {0.31f, 0.32f, 0.33f};
  insertData(taos, ts0, current0, voltage0, phase0);

  // insert with interlace mode, send non-duplicate ts, expect insert 3 overlapped rows
  int64_t ts1[] = {1648432611234, 1648432611345, 1648432611456};
  int     voltage1[] = {219, 220, 221};
  insertDataInterlace(taos, ts1, current0, voltage1, phase0);

  // insert with interlace mode, send duplicate ts, expect insert 2 rows with dups merged
  int64_t ts2[] = {1648432611678, 1648432611678, 1648432611789};
  int     voltage2[] = {222, 223, 224};
  insertDataInterlace(taos, ts2, current0, voltage2, phase0);

  // insert with interlace mode, send disordered rows, expect insert 3 sorted rows
  int64_t ts3[] = {1648432611900, 1648432611890, 1648432611910};
  int     voltage3[] = {225, 226, 227};
  insertDataInterlace(taos, ts3, current0, voltage3, phase0);

  // insert with interlace mode, send disordered and duplicate rows, expect insert 2 sorted and dup-merged rows
  int64_t ts4[] = {1648432611930, 1648432611920, 1648432611930};
  int     voltage4[] = {228, 229, 230};
  insertDataInterlace(taos, ts4, current0, voltage4, phase0);

  taos_close(taos);
  taos_cleanup();

  // final results
  // taos> select * from d1001;
  //            ts            |       current        |   voltage   |        phase         |
  // ======================================================================================
  //  2022-03-28 09:56:51.234 |           10.1000004 |         219 |            0.3100000 |
  //  2022-03-28 09:56:51.345 |           10.1999998 |         220 |            0.3200000 |
  //  2022-03-28 09:56:51.456 |           10.3000002 |         221 |            0.3300000 |
  //  2022-03-28 09:56:51.678 |           10.1999998 |         223 |            0.3200000 |
  //  2022-03-28 09:56:51.789 |           10.3000002 |         224 |            0.3300000 |
  //  2022-03-28 09:56:51.890 |           10.1999998 |         226 |            0.3200000 |
  //  2022-03-28 09:56:51.900 |           10.1000004 |         225 |            0.3100000 |
  //  2022-03-28 09:56:51.910 |           10.3000002 |         227 |            0.3300000 |
  //  2022-03-28 09:56:51.920 |           10.1999998 |         229 |            0.3200000 |
  //  2022-03-28 09:56:51.930 |           10.3000002 |         230 |            0.3300000 |
  // Query OK, 10 row(s) in set (0.005083s)
}

