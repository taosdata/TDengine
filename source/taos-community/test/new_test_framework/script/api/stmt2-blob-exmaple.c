#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

void do_query(TAOS* taos, const char* sql) {
  TAOS_RES* result = taos_query(taos, sql);
  int       code = taos_errno(result);
  if (code) {
    printf("failed to query: %s, reason:%s\n", sql, taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);
}

void do_stmt_row(TAOS* taos) {
  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db");
  do_query(taos, "create table db.stb (ts timestamp, b BLOB) tags(t1 int, t2 binary(10))");

  struct {
    int64_t ts[2];
    char    b[512];
    char    n[108];
  } v;

  int32_t b_len[2], t64_len[2], n_len[2];
  char    is_null[2] = {0};
  char    is_null2[2] = {0, 2};
  //  TAOS_STMT2_OPTION option = {0};
  // TAOS_STMT2_OPTION option = {0, true, true, stmtAsyncQueryCb, NULL};
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  char*           tbs[2] = {"tb", "tb2"};
  int             t1_val[2] = {0, 1};
  int             t2_len[2] = {5, 5};
  int             t3_len[2] = {sizeof(int), sizeof(int)};
  TAOS_STMT2_BIND tags[2][2] = {{{0, &t1_val[0], &t3_len[0], NULL, 0}, {0, "after1", &t2_len[0], NULL, 0}},
                                {{0, &t1_val[1], &t3_len[1], NULL, 0}, {0, "after2", &t2_len[1], NULL, 0}}};
  TAOS_STMT2_BIND params[2][2] = {
      {{TSDB_DATA_TYPE_TIMESTAMP, v.ts, t64_len, is_null, 2}, {TSDB_DATA_TYPE_BLOB, v.b, b_len, NULL, 2}},
      {{TSDB_DATA_TYPE_TIMESTAMP, v.ts, t64_len, is_null, 2}, {TSDB_DATA_TYPE_BLOB, v.b, b_len, NULL, 2}}};
  TAOS_STMT2_BIND* tagv[2] = {&tags[0][0], &tags[1][0]};
  TAOS_STMT2_BIND* paramv[2] = {&params[0][0], &params[1][0]};
  TAOS_STMT2_BINDV bindv = {2, &tbs[0], &tagv[0], &paramv[0]};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);

  // Equivalent to :
  // const char* sql = "insert into db.? using db.stb tags(?, ?) values(?,?)";
  const char* sql = "insert into db.stb(tbname,ts,b,t1,t2) values(?,?,?,?,?)";

  int code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("failed to execute taos_stmt2_prepare. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  if (code != 0) {
    printf("failed get col,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
  } else {
    printf("col nums:%d\n", fieldNum);
    for (int i = 0; i < fieldNum; i++) {
      printf("field[%d]: %s, data_type:%d, field_type:%d\n", i, pFields[i].name, pFields[i].type,
             pFields[i].field_type);
    }
  }

  int64_t ts = 1591060628000;
  for (int i = 0; i < 2; ++i) {
    v.ts[i] = ts++;
    t64_len[i] = sizeof(int64_t);
  }
  strcpy(v.b, "11111111111111111111111111");
  b_len[0] = (int)strlen(v.b);
  strcpy(v.b + b_len[0], "222");
  b_len[1] = 3;

  taos_stmt2_bind_param(stmt, &bindv, -1);

  if (taos_stmt2_exec(stmt, NULL)) {
    printf("failed to execute insert statement.error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }

  taos_stmt2_free_fields(stmt, pFields);
  taos_stmt2_close(stmt);
}
void do_stmt_col(TAOS* taos) {
  do_query(taos, "drop database if exists db1");
  do_query(taos, "create database db1");
  do_query(taos, "create table db1.stb (ts timestamp, b BLOB) tags(t1 int, t2 binary(10))");

  struct {
    int64_t ts[2];
    char    b[512];
    char    n[108];
  } v;

  int32_t b_len[2], t64_len[2], n_len[2];
  char    is_null[2] = {0};
  char    is_null2[2] = {0, 2};
  //  TAOS_STMT2_OPTION option = {0};
  // TAOS_STMT2_OPTION option = {0, true, true, stmtAsyncQueryCb, NULL};
  TAOS_STMT2_OPTION option = {0, true, false, NULL, NULL};

  char*           tbs[2] = {"tb", "tb2"};
  int             t1_val[2] = {0, 1};
  int             t2_len[2] = {5, 5};
  int             t3_len[2] = {sizeof(int), sizeof(int)};
  TAOS_STMT2_BIND tags[2][2] = {{{0, &t1_val[0], &t3_len[0], NULL, 0}, {0, "after1", &t2_len[0], NULL, 0}},
                                {{0, &t1_val[1], &t3_len[1], NULL, 0}, {0, "after2", &t2_len[1], NULL, 0}}};
  TAOS_STMT2_BIND params[2][2] = {
      {{TSDB_DATA_TYPE_TIMESTAMP, v.ts, t64_len, is_null, 2}, {TSDB_DATA_TYPE_BLOB, v.b, b_len, NULL, 2}},
      {{TSDB_DATA_TYPE_TIMESTAMP, v.ts, t64_len, is_null, 2}, {TSDB_DATA_TYPE_BLOB, v.b, b_len, NULL, 2}}};
  TAOS_STMT2_BIND* tagv[2] = {&tags[0][0], &tags[1][0]};
  TAOS_STMT2_BIND* paramv[2] = {&params[0][0], &params[1][0]};
  TAOS_STMT2_BINDV bindv = {2, &tbs[0], &tagv[0], &paramv[0]};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);

  // Equivalent to :
  // const char* sql = "insert into db.? using db.stb tags(?, ?) values(?,?)";
  const char* sql = "insert into db1.stb(tbname,ts,b,t1,t2) values(?,?,?,?,?)";

  int code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("failed to execute taos_stmt2_prepare. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  if (code != 0) {
    printf("failed get col,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
  } else {
    printf("col nums:%d\n", fieldNum);
    for (int i = 0; i < fieldNum; i++) {
      printf("field[%d]: %s, data_type:%d, field_type:%d\n", i, pFields[i].name, pFields[i].type,
             pFields[i].field_type);
    }
  }


  int64_t ts = 1591060628000;
  for (int i = 0; i < 2; ++i) {
    v.ts[i] = ts++;
    t64_len[i] = sizeof(int64_t);
  }
  strcpy(v.b, "11111111111111111111111111");
  b_len[0] = (int)strlen(v.b);
  strcpy(v.b + b_len[0], "222");
  b_len[1] = 3;

  taos_stmt2_bind_param(stmt, &bindv, -1);

  if (taos_stmt2_exec(stmt, NULL)) {
    printf("failed to execute insert statement.error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }

  taos_stmt2_free_fields(stmt, pFields);
  taos_stmt2_close(stmt);
}

int32_t do_stmt_getresult(TAOS* taos) {
  int32_t           code = 0;
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);

  const char* sql = "select * from db.stb where ts < ? or ts < ? limit ?";
  code = taos_stmt2_prepare(stmt, sql, 0);

  int             t64_len[1] = {sizeof(int64_t)};
  int             b_len[1] = {3};
  int             x = 1000;
  int             x_len = sizeof(int);
  int64_t         ts[2] = {1791060627000, 1791060628005};
  TAOS_STMT2_BIND params[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], t64_len, NULL, 1},
                               {TSDB_DATA_TYPE_TIMESTAMP, &ts[1], t64_len, NULL, 1},
                               {TSDB_DATA_TYPE_INT, &x, &x_len, NULL, 1}};

  TAOS_STMT2_BIND* paramv = &params[0];
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  if (code != 0) {
    printf("failed to bind params,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return code;
  }

  code = taos_stmt2_exec(stmt, NULL);
  if (code != 0) {
    printf("failed to execute select statement,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return code;
  }

  TAOS_RES* pRes = taos_stmt2_result(stmt);
  int       getRecordCounts = 0;
  TAOS_ROW  row;
  while ((row = taos_fetch_row(pRes))) {
    getRecordCounts++;
    if (row) {
      int32_t numFields = taos_num_fields(pRes);
      char*   str = (char*)malloc(1024);
      taos_print_row_with_size(str, 1024, row, taos_fetch_fields(pRes), numFields);
      printf("Row %d: %s\n", getRecordCounts, str);
      free(str);
    } else {
      printf("No more rows.\n");
    }
  }

  taos_stmt2_close(stmt);

  return code;
}
int32_t do_stmt_query(TAOS* taos) {
  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);

  int code = taos_stmt2_prepare(stmt, "select * from db.stb", 0);
  // checkError(stmt, code);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, NULL);
  if (code != 0) {
    printf("failed get col,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
  }

  taos_stmt2_free_fields(stmt, NULL);

  taos_stmt2_close(stmt);
  return code;
}

int main() {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  if (!taos) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  do_stmt_row(taos);

  do_stmt_col(taos); 

  do_stmt_query(taos);

  do_stmt_getresult(taos);

  taos_close(taos);
  taos_cleanup();
}
