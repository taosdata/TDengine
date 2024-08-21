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

void do_stmt(TAOS* taos) {
  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db");
  do_query(taos, "create table db.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");

  struct {
    int64_t ts[2];
    char    b[16];
  } v;

  int32_t           b_len[2], t64_len[2];
  char              is_null[2] = {0};
  TAOS_STMT2_OPTION option = {0};
  char*             tbs[2] = {"tb", "tb2"};
  int               t1_val[2] = {0, 1};
  int               t2_len[2] = {3, 3};
  TAOS_STMT2_BIND   tags[2][2] = {{{0, &t1_val[0], NULL, NULL, 0}, {0, "a1", &t2_len[0], NULL, 0}},
                                  {{0, &t1_val[1], NULL, NULL, 0}, {0, "a2", &t2_len[1], NULL, 0}}};
  TAOS_STMT2_BIND   params[2][2] = {
        {{TSDB_DATA_TYPE_TIMESTAMP, v.ts, t64_len, is_null, 2}, {TSDB_DATA_TYPE_BINARY, v.b, b_len, is_null, 2}},
        {{TSDB_DATA_TYPE_TIMESTAMP, v.ts, t64_len, is_null, 2}, {TSDB_DATA_TYPE_BINARY, v.b, b_len, is_null, 2}}};
  TAOS_STMT2_BIND* tagv[2] = {&tags[0][0], &tags[1][0]};
  TAOS_STMT2_BIND* paramv[2] = {&params[0][0], &params[1][0]};
  TAOS_STMT2_BINDV bindv = {2, &tbs[0], &tagv[0], &paramv[0]};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  const char* sql = "insert into db.? using db.stb tags(?, ?) values(?,?)";
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("failed to execute taos_stmt2_prepare. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt_close(stmt);
    return;
  }

  int64_t ts = 1591060628000;
  for (int i = 0; i < 2; ++i) {
    v.ts[i] = ts++;
    t64_len[i] = sizeof(int64_t);
  }
  strcpy(v.b, "abcdefg");
  b_len[0] = (int)strlen(v.b);
  strcpy(v.b + b_len[0], "xyz");
  b_len[1] = 3;

  taos_stmt2_bind_param(stmt, &bindv, -1);

  if (taos_stmt2_exec(stmt, NULL)) {
    printf("failed to execute insert statement.error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }

  taos_stmt2_close(stmt);
}

int main() {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  if (!taos) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  do_stmt(taos);
  taos_close(taos);
  taos_cleanup();
}
