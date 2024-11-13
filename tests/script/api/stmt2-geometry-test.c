#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

void do_query(TAOS* taos, const char* sql) {
  printf("[sql]%s\n", sql);
  TAOS_RES* result = taos_query(taos, sql);
  int       code = taos_errno(result);
  if (code) {
    printf("  failed to query: %s, reason:%s\n", sql, taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);
}

void execute_test(TAOS* taos, const char* tbname, const char* tag2, const char* col2, const char* case_desc) {
  // prepare stmt
  TAOS_STMT2_OPTION option = {0, true, false, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  const char*       sql;
  if (tbname == "tb4") {
    sql = "insert into db.? using db.stb2 tags(?, ?) values(?,?)";
  } else {
    sql = "insert into db.? using db.stb tags(?, ?) values(?,?)";
  }
  int code = taos_stmt2_prepare(stmt, sql, 0);
  printf("\n%s\n  insert into db.? using db.stb tags(?, ?) values(?,?)\n  bind_tag : %s, bind_col : %s\n", case_desc,
         tag2, col2);
  if (code != 0) {
    printf("  failed to execute taos_stmt2_prepare. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }

  // prepare data
  int              t1_val = 0;
  int64_t          ts = 1591060628000;
  int32_t          length[5] = {sizeof(int), 2, sizeof(int64_t), (int32_t)strlen(tag2), (int32_t)strlen(col2)};
  TAOS_STMT2_BIND  tags[2] = {{TSDB_DATA_TYPE_INT, &t1_val, &length[0], NULL, 1},
                              {TSDB_DATA_TYPE_GEOMETRY, (void*)tag2, &length[3], NULL, 1}};
  TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, &length[2], NULL, 1},
                                {TSDB_DATA_TYPE_GEOMETRY, (void*)col2, &length[4], NULL, 1}};
  TAOS_STMT2_BIND* tagv[1] = {&tags[0]};
  TAOS_STMT2_BIND* paramv[1] = {&params[0]};

  TAOS_STMT2_BINDV bindv = {1, &tbname, &tagv[0], &paramv[0]};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  if (code != 0) {
    printf("  failed to bind param. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }

  if (taos_stmt2_exec(stmt, NULL)) {
    printf("  failed to execute insert statement.error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }
  printf("[ok]\n");

  taos_stmt2_close(stmt);
}

void test1(TAOS* taos) {
  execute_test(taos, "tb1", "POINT(1.0 1.0)", "LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", "[normal]case 1");
}

void test2(TAOS* taos) {
  execute_test(taos, "tb2", "hello", "LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)", "[wrong tag]case 2");
}

void test3(TAOS* taos) { execute_test(taos, "tb3", "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))", "1", "[wrong col]case 3"); }

void test4(TAOS* taos) {
  execute_test(taos, "tb4", "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))", "POINT(1.0 1.0)", "[wrong size]case 4");
}

int main() {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  if (!taos) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }
  // init test db & stb table
  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db");
  do_query(taos, "create table db.stb (ts timestamp, b geometry(100)) tags(t1 int, t2 geometry(100))");
  do_query(taos, "create table db.stb2 (ts timestamp, b geometry(100)) tags(t1 int, t2 geometry(10))");

  test1(taos);
  test2(taos);
  test3(taos);
  test4(taos);

  taos_close(taos);
  taos_cleanup();
}
