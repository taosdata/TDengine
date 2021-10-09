// sample code to verify all TDengine API
// to compile: gcc -o apitest apitest.c -ltaos

#include "taoserror.h"
#include "cJSON.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "../../../include/client/taos.h"

static void prepare_data(TAOS* taos) {
  TAOS_RES *result;
  result = taos_query(taos, "drop database if exists test;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database test precision 'us';");
  taos_free_result(result);
  usleep(100000);
  taos_select_db(taos, "test");

  result = taos_query(taos, "create table meters(ts timestamp, a int) tags(area int);");
  taos_free_result(result);

  result = taos_query(taos, "create table t0 using meters tags(0);");
  taos_free_result(result);
  result = taos_query(taos, "create table t1 using meters tags(1);");
  taos_free_result(result);
  result = taos_query(taos, "create table t2 using meters tags(2);");
  taos_free_result(result);
  result = taos_query(taos, "create table t3 using meters tags(3);");
  taos_free_result(result);
  result = taos_query(taos, "create table t4 using meters tags(4);");
  taos_free_result(result);
  result = taos_query(taos, "create table t5 using meters tags(5);");
  taos_free_result(result);
  result = taos_query(taos, "create table t6 using meters tags(6);");
  taos_free_result(result);
  result = taos_query(taos, "create table t7 using meters tags(7);");
  taos_free_result(result);
  result = taos_query(taos, "create table t8 using meters tags(8);");
  taos_free_result(result);
  result = taos_query(taos, "create table t9 using meters tags(9);");
  taos_free_result(result);

  result = taos_query(taos, "insert into t0 values('2020-01-01 00:00:00.000', 0)"
    " ('2020-01-01 00:01:00.000', 0)"
    " ('2020-01-01 00:02:00.000', 0)"
    " t1 values('2020-01-01 00:00:00.000', 0)"
    " ('2020-01-01 00:01:00.000', 0)"
    " ('2020-01-01 00:02:00.000', 0)"
    " ('2020-01-01 00:03:00.000', 0)"
    " t2 values('2020-01-01 00:00:00.000', 0)"
    " ('2020-01-01 00:01:00.000', 0)"
    " ('2020-01-01 00:01:01.000', 0)"
    " ('2020-01-01 00:01:02.000', 0)"
    " t3 values('2020-01-01 00:01:02.000', 0)"
    " t4 values('2020-01-01 00:01:02.000', 0)"
    " t5 values('2020-01-01 00:01:02.000', 0)"
    " t6 values('2020-01-01 00:01:02.000', 0)"
    " t7 values('2020-01-01 00:01:02.000', 0)"
    " t8 values('2020-01-01 00:01:02.000', 0)"
    " t9 values('2020-01-01 00:01:02.000', 0)");
  int affected = taos_affected_rows(result);
  if (affected != 18) {
    printf("\033[31m%d rows affected by last insert statement, but it should be 18\033[0m\n", affected);
  }
  taos_free_result(result);
  // super tables subscription
  usleep(1000000);
}

static int print_result(TAOS_RES* res, int blockFetch) {
  TAOS_ROW    row = NULL;
  int         num_fields = taos_num_fields(res);
  TAOS_FIELD* fields = taos_fetch_fields(res);
  int         nRows = 0;

  if (blockFetch) {
    int rows = 0;
    while ((rows = taos_fetch_block(res, &row))) {
      //for (int i = 0; i < rows; i++) {
      //  char temp[256];
      //  taos_print_row(temp, row + i, fields, num_fields);
      //  puts(temp);
      //}
      nRows += rows;
    }
  } else {
    while ((row = taos_fetch_row(res))) {
      char temp[256] = {0};
      taos_print_row(temp, row, fields, num_fields);
      puts(temp);
      nRows++;
    }
  }

  printf("%d rows consumed.\n", nRows);
  return nRows;
}

static void check_row_count(int line, TAOS_RES* res, int expected) {
  int actual = print_result(res, expected % 2);
  if (actual != expected) {
    printf("\033[31mline %d: row count mismatch, expected: %d, actual: %d\033[0m\n", line, expected, actual);
  } else {
    printf("line %d: %d rows consumed as expected\n", line, actual);
  }
}

static void verify_query(TAOS* taos) {
  prepare_data(taos);

  int code = taos_load_table_info(taos, "t0,t1,t2,t3,t4,t5,t6,t7,t8,t9");
  if (code != 0) {
    printf("\033[31mfailed to load table info: 0x%08x\033[0m\n", code);
  }

  code = taos_validate_sql(taos, "select * from nonexisttable");
  if (code == 0) {
    printf("\033[31mimpossible, the table does not exists\033[0m\n");
  }

  code = taos_validate_sql(taos, "select * from meters");
  if (code != 0) {
    printf("\033[31mimpossible, the table does exists: 0x%08x\033[0m\n", code);
  }

  TAOS_RES* res = taos_query(taos, "select * from meters");
  check_row_count(__LINE__, res, 18);
  printf("result precision is: %d\n",  taos_result_precision(res));
  int c = taos_field_count(res);
  printf("field count is: %d\n",  c);
  int* lengths = taos_fetch_lengths(res);
  for (int i = 0; i < c; i++) {
    printf("length of column %d is %d\n", i, lengths[i]);
  }
  taos_free_result(res);

  res = taos_query(taos, "select * from t0");
  check_row_count(__LINE__, res, 3);
  taos_free_result(res);

  res = taos_query(taos, "select * from nonexisttable");
  code = taos_errno(res);
  printf("code=%d, error msg=%s\n", code, taos_errstr(res));
  taos_free_result(res);

  res = taos_query(taos, "select * from meters");
  taos_stop_query(res);
  taos_free_result(res);
}

void subscribe_callback(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code) {
  int rows = print_result(res, *(int*)param);
  printf("%d rows consumed in subscribe_callback\n", rows);
}

static void verify_subscribe(TAOS* taos) {
  prepare_data(taos);

  TAOS_SUB* tsub = taos_subscribe(taos, 0, "test", "select * from meters;", NULL, NULL, 0);
  TAOS_RES* res = taos_consume(tsub);
  check_row_count(__LINE__, res, 18);

  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 0);

  TAOS_RES *result;
  result = taos_query(taos, "insert into t0 values('2020-01-01 00:02:00.001', 0);");
  taos_free_result(result);
  result = taos_query(taos, "insert into t8 values('2020-01-01 00:01:03.000', 0);");
  taos_free_result(result);
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 2);

  result = taos_query(taos, "insert into t2 values('2020-01-01 00:01:02.001', 0);");
  taos_free_result(result);
  result = taos_query(taos, "insert into t1 values('2020-01-01 00:03:00.001', 0);");
  taos_free_result(result);
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 2);

  result = taos_query(taos, "insert into t1 values('2020-01-01 00:03:00.002', 0);");
  taos_free_result(result);
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 1);

  // keep progress information and restart subscription
  taos_unsubscribe(tsub, 1);
  result = taos_query(taos, "insert into t0 values('2020-01-01 00:04:00.000', 0);");
  taos_free_result(result);
  tsub = taos_subscribe(taos, 1, "test", "select * from meters;", NULL, NULL, 0);
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 24);

  // keep progress information and continue previous subscription
  taos_unsubscribe(tsub, 1);
  tsub = taos_subscribe(taos, 0, "test", "select * from meters;", NULL, NULL, 0);
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 0);

  // don't keep progress information and continue previous subscription
  taos_unsubscribe(tsub, 0);
  tsub = taos_subscribe(taos, 0, "test", "select * from meters;", NULL, NULL, 0);
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 24);

  // single meter subscription

  taos_unsubscribe(tsub, 0);
  tsub = taos_subscribe(taos, 0, "test", "select * from t0;", NULL, NULL, 0);
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 5);

  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 0);

  result = taos_query(taos, "insert into t0 values('2020-01-01 00:04:00.001', 0);");
  taos_free_result(result);
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 1);

  taos_unsubscribe(tsub, 0);

  int blockFetch = 0;
  tsub = taos_subscribe(taos, 1, "test", "select * from meters;", subscribe_callback, &blockFetch, 1000);
  usleep(2000000);
  result = taos_query(taos, "insert into t0 values('2020-01-01 00:05:00.001', 0);");
  taos_free_result(result);
  usleep(2000000);
  taos_unsubscribe(tsub, 0);
}

void verify_prepare(TAOS* taos) {
  TAOS_RES* result = taos_query(taos, "drop database if exists test;");
  taos_free_result(result);

  usleep(100000);
  result = taos_query(taos, "create database test;");

  int code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create database, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }

  taos_free_result(result);

  usleep(100000);
  taos_select_db(taos, "test");

  // create table
  const char* sql = "create table m1 (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin binary(40), blob nchar(10))";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create table, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);

  // insert 10 records
  struct {
      int64_t ts;
      int8_t b;
      int8_t v1;
      int16_t v2;
      int32_t v4;
      int64_t v8;
      float f4;
      double f8;
      char bin[40];
      char blob[80];
  } v = {0};

  TAOS_STMT* stmt = taos_stmt_init(taos);
  TAOS_BIND params[10];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts);
  params[0].buffer = &v.ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b);
  params[1].buffer = &v.b;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[2].buffer_length = sizeof(v.v1);
  params[2].buffer = &v.v1;
  params[2].length = &params[2].buffer_length;
  params[2].is_null = NULL;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[3].buffer_length = sizeof(v.v2);
  params[3].buffer = &v.v2;
  params[3].length = &params[3].buffer_length;
  params[3].is_null = NULL;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer_length = sizeof(v.v4);
  params[4].buffer = &v.v4;
  params[4].length = &params[4].buffer_length;
  params[4].is_null = NULL;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[5].buffer_length = sizeof(v.v8);
  params[5].buffer = &v.v8;
  params[5].length = &params[5].buffer_length;
  params[5].is_null = NULL;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[6].buffer_length = sizeof(v.f4);
  params[6].buffer = &v.f4;
  params[6].length = &params[6].buffer_length;
  params[6].is_null = NULL;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[7].buffer_length = sizeof(v.f8);
  params[7].buffer = &v.f8;
  params[7].length = &params[7].buffer_length;
  params[7].is_null = NULL;

  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[8].buffer_length = sizeof(v.bin);
  params[8].buffer = v.bin;
  params[8].length = &params[8].buffer_length;
  params[8].is_null = NULL;

  strcpy(v.blob, "一二三四五六七八九十");
  params[9].buffer_type = TSDB_DATA_TYPE_NCHAR;
  params[9].buffer_length = strlen(v.blob);
  params[9].buffer = v.blob;
  params[9].length = &params[9].buffer_length;
  params[9].is_null = NULL;

  int is_null = 1;

  sql = "insert into m1 values(?,?,?,?,?,?,?,?,?,?)";
  code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;
  }
  v.ts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts += 1;
    for (int j = 1; j < 10; ++j) {
      params[j].is_null = ((i == j) ? &is_null : 0);
    }
    v.b = (int8_t)i % 2;
    v.v1 = (int8_t)i;
    v.v2 = (int16_t)(i * 2);
    v.v4 = (int32_t)(i * 4);
    v.v8 = (int64_t)(i * 8);
    v.f4 = (float)(i * 40);
    v.f8 = (double)(i * 80);
    for (int j = 0; j < sizeof(v.bin); ++j) {
      v.bin[j] = (char)(i + '0');
    }

    taos_stmt_bind_param(stmt, params);
    taos_stmt_add_batch(stmt);
  }
  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute insert statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;
  }
  taos_stmt_close(stmt);

  // query the records
  stmt = taos_stmt_init(taos);
  taos_stmt_prepare(stmt, "SELECT * FROM m1 WHERE v1 > ? AND v2 < ?", 0);
  v.v1 = 5;
  v.v2 = 15;
  taos_stmt_bind_param(stmt, params + 2);
  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute select statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;
  }

  result = taos_stmt_use_result(stmt);

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    char temp[256] = {0};
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }

  taos_free_result(result);
  taos_stmt_close(stmt);
}

void verify_prepare2(TAOS* taos) {
  TAOS_RES* result = taos_query(taos, "drop database if exists test;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database test;");

  int code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create database, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);

  usleep(100000);
  taos_select_db(taos, "test");

  // create table
  const char* sql = "create table m1 (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin binary(40), blob nchar(10))";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create table, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);

  // insert 10 records
  struct {
      int64_t ts[10];
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
      char blob[10][80];
  } v;

  int32_t *t8_len = malloc(sizeof(int32_t) * 10);
  int32_t *t16_len = malloc(sizeof(int32_t) * 10);
  int32_t *t32_len = malloc(sizeof(int32_t) * 10);
  int32_t *t64_len = malloc(sizeof(int32_t) * 10);
  int32_t *float_len = malloc(sizeof(int32_t) * 10);
  int32_t *double_len = malloc(sizeof(int32_t) * 10);
  int32_t *bin_len = malloc(sizeof(int32_t) * 10);
  int32_t *blob_len = malloc(sizeof(int32_t) * 10);

  TAOS_STMT* stmt = taos_stmt_init(taos);
  TAOS_MULTI_BIND params[10];
  char is_null[10] = {0};
  
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts[0]);
  params[0].buffer = v.ts;
  params[0].length = t64_len;
  params[0].is_null = is_null;
  params[0].num = 10;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b[0]);
  params[1].buffer = v.b;
  params[1].length = t8_len;
  params[1].is_null = is_null;
  params[1].num = 10;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[2].buffer_length = sizeof(v.v1[0]);
  params[2].buffer = v.v1;
  params[2].length = t8_len;
  params[2].is_null = is_null;
  params[2].num = 10;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[3].buffer_length = sizeof(v.v2[0]);
  params[3].buffer = v.v2;
  params[3].length = t16_len;
  params[3].is_null = is_null;
  params[3].num = 10;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer_length = sizeof(v.v4[0]);
  params[4].buffer = v.v4;
  params[4].length = t32_len;
  params[4].is_null = is_null;
  params[4].num = 10;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[5].buffer_length = sizeof(v.v8[0]);
  params[5].buffer = v.v8;
  params[5].length = t64_len;
  params[5].is_null = is_null;
  params[5].num = 10;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[6].buffer_length = sizeof(v.f4[0]);
  params[6].buffer = v.f4;
  params[6].length = float_len;
  params[6].is_null = is_null;
  params[6].num = 10;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[7].buffer_length = sizeof(v.f8[0]);
  params[7].buffer = v.f8;
  params[7].length = double_len;
  params[7].is_null = is_null;
  params[7].num = 10;

  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[8].buffer_length = sizeof(v.bin[0]);
  params[8].buffer = v.bin;
  params[8].length = bin_len;
  params[8].is_null = is_null;
  params[8].num = 10;

  params[9].buffer_type = TSDB_DATA_TYPE_NCHAR;
  params[9].buffer_length = sizeof(v.blob[0]);
  params[9].buffer = v.blob;
  params[9].length = blob_len;
  params[9].is_null = is_null;
  params[9].num = 10;

  sql = "insert into ? (ts, b, v1, v2, v4, v8, f4, f8, bin, blob) values(?,?,?,?,?,?,?,?,?,?)";
  code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;
  }

  code = taos_stmt_set_tbname(stmt, "m1");
  if (code != 0){
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;
  }
  
  int64_t ts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = ts++;
    is_null[i] = 0;

    v.b[i] = (int8_t)i % 2;
    v.v1[i] = (int8_t)i;
    v.v2[i] = (int16_t)(i * 2);
    v.v4[i] = (int32_t)(i * 4);
    v.v8[i] = (int64_t)(i * 8);
    v.f4[i] = (float)(i * 40);
    v.f8[i] = (double)(i * 80);
    for (int j = 0; j < sizeof(v.bin[0]); ++j) {
      v.bin[i][j] = (char)(i + '0');
    }    
    strcpy(v.blob[i], "一二三四五六七八九十");

    t8_len[i] = sizeof(int8_t);
    t16_len[i] = sizeof(int16_t);
    t32_len[i] = sizeof(int32_t);
    t64_len[i] = sizeof(int64_t);
    float_len[i] = sizeof(float);
    double_len[i] = sizeof(double);
    bin_len[i] = sizeof(v.bin[0]);
    blob_len[i] = (int32_t)strlen(v.blob[i]);
  }

  taos_stmt_bind_param_batch(stmt, params);
  taos_stmt_add_batch(stmt);
  
  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute insert statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;
  }

  taos_stmt_close(stmt);

  // query the records
  stmt = taos_stmt_init(taos);
  taos_stmt_prepare(stmt, "SELECT * FROM m1 WHERE v1 > ? AND v2 < ?", 0);
  TAOS_BIND qparams[2];

  int8_t v1 = 5;
  int16_t v2 = 15;
  qparams[0].buffer_type = TSDB_DATA_TYPE_TINYINT;
  qparams[0].buffer_length = sizeof(v1);
  qparams[0].buffer = &v1;
  qparams[0].length = &qparams[0].buffer_length;
  qparams[0].is_null = NULL;

  qparams[1].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  qparams[1].buffer_length = sizeof(v2);
  qparams[1].buffer = &v2;
  qparams[1].length = &qparams[1].buffer_length;
  qparams[1].is_null = NULL;

  taos_stmt_bind_param(stmt, qparams);
  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute select statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);    
    return;
  }

  result = taos_stmt_use_result(stmt);

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    char temp[256] = {0};
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }

  taos_free_result(result);
  taos_stmt_close(stmt);

  free(t8_len);
  free(t16_len);
  free(t32_len);
  free(t64_len);
  free(float_len);
  free(double_len);
  free(bin_len);
  free(blob_len);
}

void verify_prepare3(TAOS* taos) {
  TAOS_RES* result = taos_query(taos, "drop database if exists test;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database test;");

  int code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create database, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);

  usleep(100000);
  taos_select_db(taos, "test");

  // create table
  const char* sql = "create stable st1 (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin binary(40), blob nchar(10)) tags (id1 int, id2 binary(40))";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create table, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);

  TAOS_BIND tags[2];

  int32_t id1 = 1;
  char id2[40] = "abcdefghijklmnopqrstuvwxyz0123456789";
  uintptr_t id2_len = strlen(id2);
  
  tags[0].buffer_type = TSDB_DATA_TYPE_INT;
  tags[0].buffer_length = sizeof(int);
  tags[0].buffer = &id1;
  tags[0].length = NULL;
  tags[0].is_null = NULL;

  tags[1].buffer_type = TSDB_DATA_TYPE_BINARY;
  tags[1].buffer_length = sizeof(id2);
  tags[1].buffer = id2;
  tags[1].length = &id2_len;
  tags[1].is_null = NULL;


  // insert 10 records
  struct {
      int64_t ts[10];
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
      char blob[10][80];
  } v;

  int32_t *t8_len = malloc(sizeof(int32_t) * 10);
  int32_t *t16_len = malloc(sizeof(int32_t) * 10);
  int32_t *t32_len = malloc(sizeof(int32_t) * 10);
  int32_t *t64_len = malloc(sizeof(int32_t) * 10);
  int32_t *float_len = malloc(sizeof(int32_t) * 10);
  int32_t *double_len = malloc(sizeof(int32_t) * 10);
  int32_t *bin_len = malloc(sizeof(int32_t) * 10);
  int32_t *blob_len = malloc(sizeof(int32_t) * 10);

  TAOS_STMT* stmt = taos_stmt_init(taos);
  TAOS_MULTI_BIND params[10];
  char is_null[10] = {0};
  
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts[0]);
  params[0].buffer = v.ts;
  params[0].length = t64_len;
  params[0].is_null = is_null;
  params[0].num = 10;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b[0]);
  params[1].buffer = v.b;
  params[1].length = t8_len;
  params[1].is_null = is_null;
  params[1].num = 10;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[2].buffer_length = sizeof(v.v1[0]);
  params[2].buffer = v.v1;
  params[2].length = t8_len;
  params[2].is_null = is_null;
  params[2].num = 10;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[3].buffer_length = sizeof(v.v2[0]);
  params[3].buffer = v.v2;
  params[3].length = t16_len;
  params[3].is_null = is_null;
  params[3].num = 10;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer_length = sizeof(v.v4[0]);
  params[4].buffer = v.v4;
  params[4].length = t32_len;
  params[4].is_null = is_null;
  params[4].num = 10;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[5].buffer_length = sizeof(v.v8[0]);
  params[5].buffer = v.v8;
  params[5].length = t64_len;
  params[5].is_null = is_null;
  params[5].num = 10;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[6].buffer_length = sizeof(v.f4[0]);
  params[6].buffer = v.f4;
  params[6].length = float_len;
  params[6].is_null = is_null;
  params[6].num = 10;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[7].buffer_length = sizeof(v.f8[0]);
  params[7].buffer = v.f8;
  params[7].length = double_len;
  params[7].is_null = is_null;
  params[7].num = 10;

  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[8].buffer_length = sizeof(v.bin[0]);
  params[8].buffer = v.bin;
  params[8].length = bin_len;
  params[8].is_null = is_null;
  params[8].num = 10;

  params[9].buffer_type = TSDB_DATA_TYPE_NCHAR;
  params[9].buffer_length = sizeof(v.blob[0]);
  params[9].buffer = v.blob;
  params[9].length = blob_len;
  params[9].is_null = is_null;
  params[9].num = 10;


  sql = "insert into ? using st1 tags(?,?) values(?,?,?,?,?,?,?,?,?,?)";
  code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;    
  }

  code = taos_stmt_set_tbname_tags(stmt, "m1", tags);
  if (code != 0){
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);    
    return;
  }
  
  int64_t ts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = ts++;
    is_null[i] = 0;
  
    v.b[i] = (int8_t)i % 2;
    v.v1[i] = (int8_t)i;
    v.v2[i] = (int16_t)(i * 2);
    v.v4[i] = (int32_t)(i * 4);
    v.v8[i] = (int64_t)(i * 8);
    v.f4[i] = (float)(i * 40);
    v.f8[i] = (double)(i * 80);
    for (int j = 0; j < sizeof(v.bin[0]); ++j) {
      v.bin[i][j] = (char)(i + '0');
    }    
    strcpy(v.blob[i], "一二三四五六七八九十");

    t8_len[i] = sizeof(int8_t);
    t16_len[i] = sizeof(int16_t);
    t32_len[i] = sizeof(int32_t);
    t64_len[i] = sizeof(int64_t);
    float_len[i] = sizeof(float);
    double_len[i] = sizeof(double);
    bin_len[i] = sizeof(v.bin[0]);
    blob_len[i] = (int32_t)strlen(v.blob[i]);
  }

  taos_stmt_bind_param_batch(stmt, params);
  taos_stmt_add_batch(stmt);
  
  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute insert statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);    
    return;
  }
  taos_stmt_close(stmt);

  // query the records
  stmt = taos_stmt_init(taos);
  taos_stmt_prepare(stmt, "SELECT * FROM m1 WHERE v1 > ? AND v2 < ?", 0);

  TAOS_BIND qparams[2];

  int8_t v1 = 5;
  int16_t v2 = 15;
  qparams[0].buffer_type = TSDB_DATA_TYPE_TINYINT;
  qparams[0].buffer_length = sizeof(v1);
  qparams[0].buffer = &v1;
  qparams[0].length = &qparams[0].buffer_length;
  qparams[0].is_null = NULL;

  qparams[1].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  qparams[1].buffer_length = sizeof(v2);
  qparams[1].buffer = &v2;
  qparams[1].length = &qparams[1].buffer_length;
  qparams[1].is_null = NULL;

  taos_stmt_bind_param(stmt, qparams);
  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute select statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);    
    return;
  }

  result = taos_stmt_use_result(stmt);

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    char temp[256] = {0};
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }

  taos_free_result(result);
  taos_stmt_close(stmt);

  free(t8_len);
  free(t16_len);
  free(t32_len);
  free(t64_len);
  free(float_len);
  free(double_len);
  free(bin_len);
  free(blob_len);
}

void retrieve_callback(void *param, TAOS_RES *tres, int numOfRows)
{
  if (numOfRows > 0) {
    printf("%d rows async retrieved\n", numOfRows);
    taos_fetch_rows_a(tres, retrieve_callback, param);
  } else {
    if (numOfRows < 0) {
      printf("\033[31masync retrieve failed, code: %d\033[0m\n", numOfRows);
    } else {
      printf("async retrieve completed\n");
    }
    taos_free_result(tres);
  }
}

void select_callback(void *param, TAOS_RES *tres, int code)
{
  if (code == 0 && tres) {
    taos_fetch_rows_a(tres, retrieve_callback, param);
  } else {
    printf("\033[31masync select failed, code: %d\033[0m\n", code);
  }
}

void verify_async(TAOS* taos) {
  prepare_data(taos);
  taos_query_a(taos, "select * from meters", select_callback, NULL);
  usleep(1000000);
}

void stream_callback(void *param, TAOS_RES *res, TAOS_ROW row) {
  if (res == NULL || row == NULL) {
    return;
  }
  
  int         num_fields = taos_num_fields(res);
  TAOS_FIELD* fields = taos_fetch_fields(res);

  printf("got one row from stream_callback\n");
  char temp[256] = {0};
  taos_print_row(temp, row, fields, num_fields);
  puts(temp);
}

void verify_stream(TAOS* taos) {
  prepare_data(taos);
  TAOS_STREAM* strm = taos_open_stream(
    taos,
    "select count(*) from meters interval(1m)",
    stream_callback,
    0,
    NULL,
    NULL);
  printf("waiting for stream data\n"); 
  usleep(100000);
  TAOS_RES* result = taos_query(taos, "insert into t0 values(now, 0)(now+5s,1)(now+10s, 2);");
  taos_free_result(result);
  usleep(200000000);
  taos_close_stream(strm);
}

int32_t verify_schema_less(TAOS* taos) {
  TAOS_RES *result;
  result = taos_query(taos, "drop database if exists test;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database test precision 'us' update 1;");
  taos_free_result(result);
  usleep(100000);

  taos_select_db(taos, "test");
  result = taos_query(taos, "create stable ste(ts timestamp, f int) tags(t1 bigint)");
  taos_free_result(result);
  usleep(100000);

  int code = 0;

  char* lines[] = {
      "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000ns",
      "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000ns",
      "ste,t2=5f64,t3=L\"ste\" c1=true,c2=4i64,c3=\"iam\" 1626056811823316532ns",
      "st,t1=4i64,t2=5f64,t3=\"t4\" c1=3i64,c3=L\"passitagain\",c2=true,c4=5f64 1626006833642000000ns",
      "ste,t2=5f64,t3=L\"ste2\" c3=\"iamszhou\",c4=false 1626056811843316532ns",
      "ste,t2=5f64,t3=L\"ste2\" c3=\"iamszhou\",c4=false,c5=32i8,c6=64i16,c7=32i32,c8=88.88f32 1626056812843316532ns",
      "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000ns",
      "stf,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000ns",
      "stf,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 1626006933641000000ns"
  };

  code = taos_insert_lines(taos, lines , sizeof(lines)/sizeof(char*));

  char* lines2[] = {
      "stg,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000ns",
      "stg,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000ns"
  };
  code = taos_insert_lines(taos, &lines2[0], 1);
  code = taos_insert_lines(taos, &lines2[1], 1);

  char* lines3[] = {
      "sth,t1=4i64,t2=5f64,t4=5f64,ID=\"childtable\" c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 1626006933641ms",
      "sth,t1=4i64,t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 1626006933654ms"
  };
  code = taos_insert_lines(taos, lines3, 2);

  char* lines4[] = {
      "st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000ns",
      "dgtyqodr,t2=5f64,t3=L\"ste\" c1=tRue,c2=4i64,c3=\"iam\" 1626056811823316532ns"
  };
  code = taos_insert_lines(taos, lines4, 2);

  char* lines5[] = {
      "zqlbgs,id=\"zqlbgs_39302_21680\",t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"binaryColValue\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000ns",
      "zqlbgs,t9=f,id=\"zqlbgs_39302_21680\",t0=f,t1=127i8,t11=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\",t10=L\"ncharTagValue\" c10=f,c0=f,c1=127i8,c12=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"binaryColValue\",c8=L\"ncharColValue\",c9=7u64,c11=L\"ncharColValue\" 1626006833639000000ns"
  };
  code = taos_insert_lines(taos, &lines5[0], 1);
  code = taos_insert_lines(taos, &lines5[1], 1);


  char* lines6[] = {
      "st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000ns",
      "dgtyqodr,t2=5f64,t3=L\"ste\" c1=tRue,c2=4i64,c3=\"iam\" 1626056811823316532ns"
  };
  code = taos_insert_lines(taos, lines6, 2);
  return (code);
}

void verify_telnet_insert(TAOS* taos) {
  TAOS_RES *result;

  result = taos_query(taos, "drop database if exists db;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database db precision 'ms';");
  taos_free_result(result);
  usleep(100000);

  (void)taos_select_db(taos, "db");
  int32_t code = 0;

  /* metric */
  char* lines0[] = {
      "stb0_0 1626006833639000000ns 4i8 host=\"host0\",interface=\"eth0\"",
      "stb0_1 1626006833639000000ns 4i8 host=\"host0\",interface=\"eth0\"",
      "stb0_2 1626006833639000000ns 4i8 host=\"host0\",interface=\"eth0\"",
  };
  code = taos_insert_telnet_lines(taos, lines0, 3);
  if (code) {
    printf("lines0 code: %d, %s.\n", code, tstrerror(code));
  }

  /* timestamp */
  char* lines1[] = {
      "stb1 1626006833s 1i8 host=\"host0\"",
      "stb1 1626006833639000000ns 2i8 host=\"host0\"",
      "stb1 1626006833640000us 3i8 host=\"host0\"",
      "stb1 1626006833641123 4i8 host=\"host0\"",
      "stb1 1626006833651ms 5i8 host=\"host0\"",
      "stb1 0 6i8 host=\"host0\"",
  };
  code = taos_insert_telnet_lines(taos, lines1, 6);
  if (code) {
    printf("lines1 code: %d, %s.\n", code, tstrerror(code));
  }

  /* metric value */
  //tinyin
  char* lines2_0[] = {
      "stb2_0 1626006833651ms -127i8 host=\"host0\"",
      "stb2_0 1626006833652ms 127i8 host=\"host0\""
  };
  code = taos_insert_telnet_lines(taos, lines2_0, 2);
  if (code) {
    printf("lines2_0 code: %d, %s.\n", code, tstrerror(code));
  }

  //smallint
  char* lines2_1[] = {
      "stb2_1 1626006833651ms -32767i16 host=\"host0\"",
      "stb2_1 1626006833652ms 32767i16 host=\"host0\""
  };
  code = taos_insert_telnet_lines(taos, lines2_1, 2);
  if (code) {
    printf("lines2_1 code: %d, %s.\n", code, tstrerror(code));
  }

  //int
  char* lines2_2[] = {
      "stb2_2 1626006833651ms -2147483647i32 host=\"host0\"",
      "stb2_2 1626006833652ms 2147483647i32 host=\"host0\""
  };
  code = taos_insert_telnet_lines(taos, lines2_2, 2);
  if (code) {
    printf("lines2_2 code: %d, %s.\n", code, tstrerror(code));
  }

  //bigint
  char* lines2_3[] = {
      "stb2_3 1626006833651ms -9223372036854775807i64 host=\"host0\"",
      "stb2_3 1626006833652ms 9223372036854775807i64 host=\"host0\""
  };
  code = taos_insert_telnet_lines(taos, lines2_3, 2);
  if (code) {
    printf("lines2_3 code: %d, %s.\n", code, tstrerror(code));
  }

  //float
  char* lines2_4[] = {
      "stb2_4 1626006833610ms 3f32 host=\"host0\"",
      "stb2_4 1626006833620ms -3f32 host=\"host0\"",
      "stb2_4 1626006833630ms 3.4f32 host=\"host0\"",
      "stb2_4 1626006833640ms -3.4f32 host=\"host0\"",
      "stb2_4 1626006833650ms 3.4E10f32 host=\"host0\"",
      "stb2_4 1626006833660ms -3.4e10f32 host=\"host0\"",
      "stb2_4 1626006833670ms 3.4E+2f32 host=\"host0\"",
      "stb2_4 1626006833680ms -3.4e-2f32 host=\"host0\"",
      "stb2_4 1626006833690ms 3.15 host=\"host0\"",
      "stb2_4 1626006833700ms 3.4E38f32 host=\"host0\"",
      "stb2_4 1626006833710ms -3.4E38f32 host=\"host0\""
  };
  code = taos_insert_telnet_lines(taos, lines2_4, 11);
  if (code) {
    printf("lines2_4 code: %d, %s.\n", code, tstrerror(code));
  }

  //double
  char* lines2_5[] = {
      "stb2_5 1626006833610ms 3f64 host=\"host0\"",
      "stb2_5 1626006833620ms -3f64 host=\"host0\"",
      "stb2_5 1626006833630ms 3.4f64 host=\"host0\"",
      "stb2_5 1626006833640ms -3.4f64 host=\"host0\"",
      "stb2_5 1626006833650ms 3.4E10f64 host=\"host0\"",
      "stb2_5 1626006833660ms -3.4e10f64 host=\"host0\"",
      "stb2_5 1626006833670ms 3.4E+2f64 host=\"host0\"",
      "stb2_5 1626006833680ms -3.4e-2f64 host=\"host0\"",
      "stb2_5 1626006833690ms 1.7E308f64 host=\"host0\"",
      "stb2_5 1626006833700ms -1.7E308f64 host=\"host0\""
  };
  code = taos_insert_telnet_lines(taos, lines2_5, 10);
  if (code) {
    printf("lines2_5 code: %d, %s.\n", code, tstrerror(code));
  }

  //bool
  char* lines2_6[] = {
      "stb2_6 1626006833610ms t host=\"host0\"",
      "stb2_6 1626006833620ms T host=\"host0\"",
      "stb2_6 1626006833630ms true host=\"host0\"",
      "stb2_6 1626006833640ms True host=\"host0\"",
      "stb2_6 1626006833650ms TRUE host=\"host0\"",
      "stb2_6 1626006833660ms f host=\"host0\"",
      "stb2_6 1626006833670ms F host=\"host0\"",
      "stb2_6 1626006833680ms false host=\"host0\"",
      "stb2_6 1626006833690ms False host=\"host0\"",
      "stb2_6 1626006833700ms FALSE host=\"host0\""
  };
  code = taos_insert_telnet_lines(taos, lines2_6, 10);
  if (code) {
    printf("lines2_6 code: %d, %s.\n", code, tstrerror(code));
  }

  //binary
  char* lines2_7[] = {
      "stb2_7 1626006833610ms \"binary_val.!@#$%^&*\" host=\"host0\"",
      "stb2_7 1626006833620ms \"binary_val.:;,./?|+-=\" host=\"host0\"",
      "stb2_7 1626006833630ms \"binary_val.()[]{}<>\" host=\"host0\""
  };
  code = taos_insert_telnet_lines(taos, lines2_7, 3);
  if (code) {
    printf("lines2_7 code: %d, %s.\n", code, tstrerror(code));
  }

  //nchar
  char* lines2_8[] = {
      "stb2_8 1626006833610ms L\"nchar_val数值一\" host=\"host0\"",
      "stb2_8 1626006833620ms L\"nchar_val数值二\" host=\"host0\"",
  };
  code = taos_insert_telnet_lines(taos, lines2_8, 2);
  if (code) {
    printf("lines2_8 code: %d, %s.\n", code, tstrerror(code));
  }

  /* tags */
  //tag value types
  char* lines3_0[] = {
      "stb3_0 1626006833610ms 1 t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=3.4E38f32,t6=1.7E308f64,t7=true,t8=\"binary_val_1\",t9=L\"标签值1\"",
      "stb3_0 1626006833610ms 2 t1=-127i8,t2=-32767i16,t3=-2147483647i32,t4=-9223372036854775807i64,t5=-3.4E38f32,t6=-1.7E308f64,t7=false,t8=\"binary_val_2\",t9=L\"标签值2\""
  };
  code = taos_insert_telnet_lines(taos, lines3_0, 2);
  if (code) {
    printf("lines3_0 code: %d, %s.\n", code, tstrerror(code));
  }

  //tag ID as child table name
  char* lines3_1[] = {
      "stb3_1 1626006833610ms 1 id=\"child_table1\",host=\"host1\"",
      "stb3_1 1626006833610ms 2 host=\"host2\",iD=\"child_table2\"",
      "stb3_1 1626006833610ms 3 ID=\"child_table3\",host=\"host3\""
  };
  code = taos_insert_telnet_lines(taos, lines3_1, 3);
  if (code) {
    printf("lines3_1 code: %d, %s.\n", code, tstrerror(code));
  }

  return;
}

void verify_json_insert(TAOS* taos) {
  TAOS_RES *result;

  result = taos_query(taos, "drop database if exists db;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database db precision 'ms';");
  taos_free_result(result);
  usleep(100000);

  (void)taos_select_db(taos, "db");
  int32_t code = 0;

  char *message =
  "{                                      \
      \"metric\":\"cpu_load_0\",          \
      \"timestamp\": 1626006833610123,    \
      \"value\": 55.5,                    \
      \"tags\":                           \
          {                               \
              \"host\": \"ubuntu\",       \
              \"interface1\": \"eth0\",   \
              \"Id\": \"tb0\"             \
          }                               \
  }";

  code = taos_insert_json_payload(taos, message);
  if (code) {
    printf("payload_0 code: %d, %s.\n", code, tstrerror(code));
  }

  char *message1 =
  "[                                       \
    {                                      \
       \"metric\":\"cpu_load_1\",          \
       \"timestamp\": 1626006833610123,    \
       \"value\": 55.5,                    \
       \"tags\":                           \
           {                               \
               \"host\": \"ubuntu\",       \
               \"interface\": \"eth1\",    \
               \"Id\": \"tb1\"             \
           }                               \
    },                                     \
    {                                      \
       \"metric\":\"cpu_load_2\",          \
       \"timestamp\": 1626006833610123,    \
       \"value\": 55.5,                    \
       \"tags\":                           \
           {                               \
               \"host\": \"ubuntu\",       \
               \"interface\": \"eth2\",    \
               \"Id\": \"tb2\"             \
           }                               \
    }                                      \
   ]";

  code = taos_insert_json_payload(taos, message1);
  if (code) {
    printf("payload_1 code: %d, %s.\n", code, tstrerror(code));
  }

  char *message2 =
  "[                                       \
    {                                      \
       \"metric\":\"cpu_load_3\",          \
       \"timestamp\":                      \
           {                               \
             \"value\": 1626006833610123,  \
             \"type\": \"us\"              \
           },                              \
       \"value\":                          \
           {                               \
             \"value\": 55,                \
             \"type\": \"int\"             \
           },                              \
       \"tags\":                           \
           {                               \
               \"host\":                   \
                  {                        \
                    \"value\": \"ubuntu\", \
                    \"type\": \"binary\"   \
                  },                       \
               \"interface\":              \
                  {                        \
                    \"value\": \"eth3\",   \
                    \"type\": \"nchar\"    \
                  },                       \
               \"ID\": \"tb3\",            \
               \"port\":                   \
                  {                        \
                    \"value\": 4040,       \
                    \"type\": \"int\"      \
                  }                        \
           }                               \
    },                                     \
    {                                      \
       \"metric\":\"cpu_load_4\",          \
       \"timestamp\": 1626006833610123,    \
       \"value\": 66.6,                    \
       \"tags\":                           \
           {                               \
               \"host\": \"ubuntu\",       \
               \"interface\": \"eth4\",    \
               \"Id\": \"tb4\"             \
           }                               \
    }                                      \
   ]";
  code = taos_insert_json_payload(taos, message2);
  if (code) {
    printf("payload_2 code: %d, %s.\n", code, tstrerror(code));
  }


  cJSON *payload, *tags;
  char *payload_str;

  /* Default format */
  //number
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb0_0");
  cJSON_AddNumberToObject(payload, "timestamp", 1626006833610123);
  cJSON_AddNumberToObject(payload, "value", 10);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload0_0 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //true
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb0_1");
  cJSON_AddNumberToObject(payload, "timestamp", 1626006833610123);
  cJSON_AddTrueToObject(payload, "value");
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload0_1 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //false
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb0_2");
  cJSON_AddNumberToObject(payload, "timestamp", 1626006833610123);
  cJSON_AddFalseToObject(payload, "value");
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload0_2 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //string
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb0_3");
  cJSON_AddNumberToObject(payload, "timestamp", 1626006833610123);
  cJSON_AddStringToObject(payload, "value", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload0_3 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //timestamp 0 -> current time
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb0_4");
  cJSON_AddNumberToObject(payload, "timestamp", 0);
  cJSON_AddNumberToObject(payload, "value", 123);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload0_4 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //ID
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb0_5");
  cJSON_AddNumberToObject(payload, "timestamp", 0);
  cJSON_AddNumberToObject(payload, "value", 123);
  tags = cJSON_CreateObject();
  cJSON_AddStringToObject(tags, "ID", "tb0_5");
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddStringToObject(tags, "iD", "tb000");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddStringToObject(tags, "id", "tb555");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload0_5 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  /* Nested format */
  //timestamp
  cJSON *timestamp;
  //seconds
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb1_0");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  cJSON_AddNumberToObject(payload, "value", 10);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload1_0 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //milleseconds
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb1_1");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833610);
  cJSON_AddStringToObject(timestamp, "type", "ms");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  cJSON_AddNumberToObject(payload, "value", 10);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload1_1 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //microseconds
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb1_2");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833610123);
  cJSON_AddStringToObject(timestamp, "type", "us");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  cJSON_AddNumberToObject(payload, "value", 10);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload1_2 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //nanoseconds
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb1_3");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833610123321);
  cJSON_AddStringToObject(timestamp, "type", "ns");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  cJSON_AddNumberToObject(payload, "value", 10);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload1_3 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //now
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb1_4");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 0);
  cJSON_AddStringToObject(timestamp, "type", "ns");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  cJSON_AddNumberToObject(payload, "value", 10);
  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload1_4 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //metric value
  cJSON *metric_val;
  //bool
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_0");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddTrueToObject(metric_val, "value");
  cJSON_AddStringToObject(metric_val, "type", "bool");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload2_0 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //tinyint
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_1");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddNumberToObject(metric_val, "value", 127);
  cJSON_AddStringToObject(metric_val, "type", "tinyint");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload2_1 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //smallint
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_2");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddNumberToObject(metric_val, "value", 32767);
  cJSON_AddStringToObject(metric_val, "type", "smallint");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload2_2 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //int
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_3");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddNumberToObject(metric_val, "value", 2147483647);
  cJSON_AddStringToObject(metric_val, "type", "int");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload2_3 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //bigint
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_4");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddNumberToObject(metric_val, "value", 9223372036854775807);
  cJSON_AddStringToObject(metric_val, "type", "bigint");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload2_4 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //float
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_5");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddNumberToObject(metric_val, "value", 11.12345);
  cJSON_AddStringToObject(metric_val, "type", "float");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload2_5 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //double
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_6");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddNumberToObject(metric_val, "value", 22.123456789);
  cJSON_AddStringToObject(metric_val, "type", "double");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload2_6 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //binary
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_7");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddStringToObject(metric_val, "value", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddStringToObject(metric_val, "type", "binary");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload2_7 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //nchar
  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb2_8");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddStringToObject(metric_val, "value", "你好");
  cJSON_AddStringToObject(metric_val, "type", "nchar");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();
  cJSON_AddTrueToObject(tags, "t1");
  cJSON_AddFalseToObject(tags, "t2");
  cJSON_AddNumberToObject(tags, "t3", 10);
  cJSON_AddStringToObject(tags, "t4", "123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>");
  cJSON_AddItemToObject(payload, "tags", tags);
  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload2_8 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);

  //tag value
  cJSON *tag;

  payload = cJSON_CreateObject();
  cJSON_AddStringToObject(payload, "metric", "stb3_0");

  timestamp = cJSON_CreateObject();
  cJSON_AddNumberToObject(timestamp, "value", 1626006833);
  cJSON_AddStringToObject(timestamp, "type", "s");
  cJSON_AddItemToObject(payload, "timestamp", timestamp);

  metric_val = cJSON_CreateObject();
  cJSON_AddStringToObject(metric_val, "value", "hello");
  cJSON_AddStringToObject(metric_val, "type", "nchar");
  cJSON_AddItemToObject(payload, "value", metric_val);

  tags = cJSON_CreateObject();

  tag = cJSON_CreateObject();
  cJSON_AddTrueToObject(tag, "value");
  cJSON_AddStringToObject(tag, "type", "bool");
  cJSON_AddItemToObject(tags, "t1", tag);

  tag = cJSON_CreateObject();
  cJSON_AddFalseToObject(tag, "value");
  cJSON_AddStringToObject(tag, "type", "bool");
  cJSON_AddItemToObject(tags, "t2", tag);

  tag = cJSON_CreateObject();
  cJSON_AddNumberToObject(tag, "value", 127);
  cJSON_AddStringToObject(tag, "type", "tinyint");
  cJSON_AddItemToObject(tags, "t3", tag);

  tag = cJSON_CreateObject();
  cJSON_AddNumberToObject(tag, "value", 32767);
  cJSON_AddStringToObject(tag, "type", "smallint");
  cJSON_AddItemToObject(tags, "t4", tag);

  tag = cJSON_CreateObject();
  cJSON_AddNumberToObject(tag, "value", 2147483647);
  cJSON_AddStringToObject(tag, "type", "int");
  cJSON_AddItemToObject(tags, "t5", tag);

  tag = cJSON_CreateObject();
  cJSON_AddNumberToObject(tag, "value", 9223372036854775807);
  cJSON_AddStringToObject(tag, "type", "bigint");
  cJSON_AddItemToObject(tags, "t6", tag);

  tag = cJSON_CreateObject();
  cJSON_AddNumberToObject(tag, "value", 11.12345);
  cJSON_AddStringToObject(tag, "type", "float");
  cJSON_AddItemToObject(tags, "t7", tag);

  tag = cJSON_CreateObject();
  cJSON_AddNumberToObject(tag, "value", 22.1234567890);
  cJSON_AddStringToObject(tag, "type", "double");
  cJSON_AddItemToObject(tags, "t8", tag);

  tag = cJSON_CreateObject();
  cJSON_AddStringToObject(tag, "value", "binary_val");
  cJSON_AddStringToObject(tag, "type", "binary");
  cJSON_AddItemToObject(tags, "t9", tag);

  tag = cJSON_CreateObject();
  cJSON_AddStringToObject(tag, "value", "你好");
  cJSON_AddStringToObject(tag, "type", "nchar");
  cJSON_AddItemToObject(tags, "t10", tag);

  cJSON_AddItemToObject(payload, "tags", tags);

  payload_str = cJSON_Print(payload);
  //printf("%s\n", payload_str);

  code = taos_insert_json_payload(taos, payload_str);
  if (code) {
    printf("payload3_0 code: %d, %s.\n", code, tstrerror(code));
  }
  free(payload_str);
  cJSON_Delete(payload);
}

int main(int argc, char *argv[]) {
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";

  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  TAOS* taos = taos_connect(host, user, passwd, "", 0);
  if (taos == NULL) {
    printf("\033[31mfailed to connect to db, reason:%s\033[0m\n", taos_errstr(taos));
    exit(1);
  }

  char* info = taos_get_server_info(taos);
  printf("server info: %s\n", info);
  info = taos_get_client_info(taos);
  printf("client info: %s\n", info);

  printf("************  verify schema-less  *************\n");
  verify_schema_less(taos);

  printf("************  verify telnet-insert  *************\n");
  verify_telnet_insert(taos);

  printf("************  verify json-insert  *************\n");
  verify_json_insert(taos);

  printf("************  verify query  *************\n");
  verify_query(taos);

  printf("*********  verify async query  **********\n");
  verify_async(taos);

  printf("*********** verify subscribe ************\n");
  verify_subscribe(taos);

  printf("************ verify prepare *************\n");
  verify_prepare(taos);

  printf("************ verify prepare2 *************\n");
  verify_prepare2(taos);
  printf("************ verify prepare3 *************\n");
  verify_prepare3(taos);

  printf("************ verify stream  *************\n");
  verify_stream(taos);
  printf("done\n");
  taos_close(taos);
  taos_cleanup();
}
