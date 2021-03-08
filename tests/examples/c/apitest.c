// sample code to verify all TDengine API
// to compile: gcc -o apitest apitest.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
#include <unistd.h>


static void prepare_data(TAOS* taos) {
  TAOS_RES *result;
  result = taos_query(taos, "drop database if exists test;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database test;");
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
      char temp[256];
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
    printf("\033[31mfailed to execute taos_stmt_prepare. code:0x%x\033[0m\n", code);
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
    for (int j = 0; j < sizeof(v.bin) - 1; ++j) {
      v.bin[j] = (char)(i + '0');
    }

    taos_stmt_bind_param(stmt, params);
    taos_stmt_add_batch(stmt);
  }
  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute insert statement.\033[0m\n");
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
    printf("\033[31mfailed to execute select statement.\033[0m\n");
    return;
  }

  result = taos_stmt_use_result(stmt);

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  char        temp[256];

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }

  taos_free_result(result);
  taos_stmt_close(stmt);
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
  int         num_fields = taos_num_fields(res);
  TAOS_FIELD* fields = taos_fetch_fields(res);

  printf("got one row from stream_callback\n");
  char temp[256];
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

  printf("************  verify query  *************\n");
  verify_query(taos);

  printf("*********  verify async query  **********\n");
  verify_async(taos);

  printf("*********** verify subscribe ************\n");
  verify_subscribe(taos);

  printf("************ verify prepare *************\n");
  verify_prepare(taos);

  printf("************ verify stream  *************\n");
  verify_stream(taos);
  printf("done\n");

  taos_close(taos);
  taos_cleanup();
}
