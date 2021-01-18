#include <gtest/gtest.h>
#include <iostream>

#include "taos.h"
#include "tglobal.h"

namespace {
static int64_t start_ts = 1433955661000;

void stmtInsertTest() {
  TAOS* conn = taos_connect("ubuntu", "root", "taosdata", 0, 0);
  if (conn == NULL) {
    printf("Failed to connect to DB, reason:%s", taos_errstr(conn));
    exit(-1);
  }

  TAOS_RES* res = taos_query(conn, "use test");
  taos_free_result(res);

  const char* sql = "insert into t1 values(?, ?, ?, ?)";
  TAOS_STMT* stmt = taos_stmt_init(conn);

  int32_t ret = taos_stmt_prepare(stmt, sql, 0);
  ASSERT_EQ(ret, 0);

  //ts timestamp, k int, a binary(11), b nchar(4)
  struct {
    int64_t ts;
    int k;
    char* a;
    char* b;
  } v = {0};

  TAOS_BIND params[4];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts);
  params[0].buffer = &v.ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;

  params[1].buffer_type = TSDB_DATA_TYPE_INT;
  params[1].buffer_length = sizeof(v.k);
  params[1].buffer = &v.k;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;

  params[2].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[2].buffer_length = sizeof(v.a);
  params[2].buffer = &v.a;
  params[2].is_null = NULL;

  params[3].buffer_type = TSDB_DATA_TYPE_NCHAR;
  params[3].buffer_length = sizeof(v.b);
  params[3].buffer = &v.b;
  params[3].is_null = NULL;

  v.ts = start_ts + 20;
  v.k = 123;

  char* str = "abc";
  uintptr_t len = strlen(str);

  v.a = str;
  params[2].length = &len;
  params[2].buffer_length = len;
  params[2].buffer = str;

  char* nstr = "999";
  uintptr_t len1 = strlen(nstr);

  v.b = nstr;
  params[3].buffer_length = len1;
  params[3].buffer = nstr;
  params[3].length = &len1;

  taos_stmt_bind_param(stmt, params);
  taos_stmt_add_batch(stmt);

  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute insert statement.\033[0m\n");
    return;
  }

  v.ts = start_ts + 30;
  v.k = 911;

  str = "92";
  len = strlen(str);

  params[2].length = &len;
  params[2].buffer_length = len;
  params[2].buffer = str;

  nstr = "1920";
  len1 = strlen(nstr);

  params[3].buffer_length = len1;
  params[3].buffer = nstr;
  params[3].length = &len1;

  taos_stmt_bind_param(stmt, params);
  taos_stmt_add_batch(stmt);

  ret = taos_stmt_execute(stmt);
  if (ret != 0) {
    printf("%p\n", ret);
    printf("\033[31mfailed to execute insert statement.\033[0m\n");
    return;
  }

  taos_stmt_close(stmt);
  taos_close(conn);
}

void validateResultFields() {
  TAOS* conn = taos_connect("ubuntu", "root", "taosdata", 0, 0);
  if (conn == NULL) {
    printf("Failed to connect to DB, reason:%s", taos_errstr(conn));
    exit(-1);
  }

  TAOS_RES* res = taos_query(conn, "create database if not exists test");
  ASSERT_EQ(taos_errno(res), 0);
  taos_free_result(res);

  res = taos_query(conn, "use test");
  ASSERT_EQ(taos_errno(res), 0);
  taos_free_result(res);

  res = taos_query(conn, "create table if not exists t1(ts timestamp, k int, a binary(11), b nchar(4))");
  ASSERT_EQ(taos_errno(res), 0);
  taos_free_result(res);

  char sql[512] = {0};
  sprintf(sql, "insert into t1 values(%ld, 99, 'abc', 'test')", start_ts);

  res = taos_query(conn, sql);
  ASSERT_EQ(taos_errno(res), 0);
  taos_free_result(res);

  res = taos_query(conn, "select count(*), spread(ts)/(1000 * 3600 * 24), first(a), last(b) from t1");
  ASSERT_EQ(taos_num_fields(res), 4);

  TAOS_FIELD* fields = taos_fetch_fields(res);
  ASSERT_EQ(fields[0].bytes, 8);
  ASSERT_EQ(fields[0].type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_STREQ(fields[0].name, "count(*)");

  ASSERT_EQ(fields[1].bytes, 8);
  ASSERT_EQ(fields[1].type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_STREQ(fields[1].name, "spread(ts)/(1000 * 3600 * 24)");

  ASSERT_EQ(fields[2].bytes, 11);
  ASSERT_EQ(fields[2].type, TSDB_DATA_TYPE_BINARY);
  ASSERT_STREQ(fields[2].name, "first(a)");

  ASSERT_EQ(fields[3].bytes, 4);
  ASSERT_EQ(fields[3].type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_STREQ(fields[3].name, "last(b)");

  taos_free_result(res);

  res = taos_query(conn, "select last_row(*) from t1");
  ASSERT_EQ(taos_num_fields(res), 4);

  fields = taos_fetch_fields(res);
  ASSERT_EQ(fields[0].bytes, 8);
  ASSERT_EQ(fields[0].type, TSDB_DATA_TYPE_TIMESTAMP);
  ASSERT_STREQ(fields[0].name, "last_row(ts)");

  ASSERT_EQ(fields[1].bytes, 4);
  ASSERT_EQ(fields[1].type, TSDB_DATA_TYPE_INT);
  ASSERT_STREQ(fields[1].name, "last_row(k)");

  ASSERT_EQ(fields[2].bytes, 11);
  ASSERT_EQ(fields[2].type, TSDB_DATA_TYPE_BINARY);
  ASSERT_STREQ(fields[2].name, "last_row(a)");

  ASSERT_EQ(fields[3].bytes, 4);
  ASSERT_EQ(fields[3].type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_STREQ(fields[3].name, "last_row(b)");

  taos_free_result(res);
  res = taos_query(conn, "select first(*), last(*) from t1");
  ASSERT_EQ(taos_num_fields(res), 8);

  fields = taos_fetch_fields(res);
  ASSERT_EQ(fields[0].bytes, 8);
  ASSERT_EQ(fields[0].type, TSDB_DATA_TYPE_TIMESTAMP);
  ASSERT_STREQ(fields[0].name, "first(ts)");

  ASSERT_EQ(fields[1].bytes, 4);
  ASSERT_EQ(fields[1].type, TSDB_DATA_TYPE_INT);
  ASSERT_STREQ(fields[1].name, "first(k)");

  ASSERT_EQ(fields[2].bytes, 11);
  ASSERT_EQ(fields[2].type, TSDB_DATA_TYPE_BINARY);
  ASSERT_STREQ(fields[2].name, "first(a)");

  ASSERT_EQ(fields[3].bytes, 4);
  ASSERT_EQ(fields[3].type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_STREQ(fields[3].name, "first(b)");

  taos_free_result(res);

  res = taos_query(conn, "select first(ts, a, k, k, b, b, ts) from t1");
  ASSERT_EQ(taos_num_fields(res), 7);

  fields = taos_fetch_fields(res);
  ASSERT_EQ(fields[0].bytes, 8);
  ASSERT_EQ(fields[0].type, TSDB_DATA_TYPE_TIMESTAMP);
  ASSERT_STREQ(fields[0].name, "first(ts)");

  ASSERT_EQ(fields[1].bytes, 11);
  ASSERT_EQ(fields[1].type, TSDB_DATA_TYPE_BINARY);
  ASSERT_STREQ(fields[1].name, "first(a)");

  ASSERT_EQ(fields[2].bytes, 4);
  ASSERT_EQ(fields[2].type, TSDB_DATA_TYPE_INT);
  ASSERT_STREQ(fields[2].name, "first(k)");

  ASSERT_EQ(fields[3].bytes, 4);
  ASSERT_EQ(fields[3].type, TSDB_DATA_TYPE_INT);
  ASSERT_STREQ(fields[3].name, "first(k)");

  ASSERT_EQ(fields[4].bytes, 4);
  ASSERT_EQ(fields[4].type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_STREQ(fields[4].name, "first(b)");

  ASSERT_EQ(fields[5].bytes, 4);
  ASSERT_EQ(fields[5].type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_STREQ(fields[5].name, "first(b)");

  ASSERT_EQ(fields[6].bytes, 8);
  ASSERT_EQ(fields[6].type, TSDB_DATA_TYPE_TIMESTAMP);
  ASSERT_STREQ(fields[6].name, "first(ts)");

  taos_free_result(res);

  // update the configure parameter, the result field name will be changed
  tsKeepOriginalColumnName = 1;
  res = taos_query(conn, "select first(ts, a, k, k, b, b, ts) from t1");
  ASSERT_EQ(taos_num_fields(res), 7);

  fields = taos_fetch_fields(res);
  ASSERT_EQ(fields[0].bytes, 8);
  ASSERT_EQ(fields[0].type, TSDB_DATA_TYPE_TIMESTAMP);
  ASSERT_STREQ(fields[0].name, "ts");

  ASSERT_EQ(fields[2].bytes, 4);
  ASSERT_EQ(fields[2].type, TSDB_DATA_TYPE_INT);
  ASSERT_STREQ(fields[2].name, "k");

  taos_free_result(res);

  taos_close(conn);
}
}
/* test parse time function */
TEST(testCase, result_field_test) {
  taos_options(TSDB_OPTION_CONFIGDIR, "~/first/cfg");
  taos_init();

  validateResultFields();
  stmtInsertTest();
}
