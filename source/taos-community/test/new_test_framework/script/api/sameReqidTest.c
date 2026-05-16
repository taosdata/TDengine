// sample code to verify multiple queries with the same reqid
// to compile: gcc -o sameReqdiTest sameReqidTest.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "taos.h"

#define NUM_ROUNDS   10
#define CONST_REQ_ID 12345
#define TEST_DB      "test"

#define CHECK_CONDITION(condition, prompt, errstr)                                                     \
  do {                                                                                                 \
    if (!(condition)) {                                                                                \
      printf("\033[31m[%s:%d] failed to " prompt ", reason: %s\033[0m\n", __func__, __LINE__, errstr); \
      exit(EXIT_FAILURE);                                                                              \
    }                                                                                                  \
  } while (0)

#define CHECK_RES(res, prompt)   CHECK_CONDITION(taos_errno(res) == 0, prompt, taos_errstr(res))
#define CHECK_CODE(code, prompt) CHECK_CONDITION(code == 0, prompt, taos_errstr(NULL))

static TAOS* getNewConnection() {
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";
  TAOS*       taos = NULL;

  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  taos = taos_connect(host, user, passwd, "", 0);
  CHECK_CONDITION(taos != NULL, "connect to db", taos_errstr(NULL));
  return taos;
}

static void prepareData(TAOS* taos) {
  TAOS_RES* res = NULL;
  int32_t   code = 0;

  res = taos_query(taos, "create database if not exists " TEST_DB " precision 'ns'");
  CHECK_RES(res, "create database");
  taos_free_result(res);
  usleep(100000);

  code = taos_select_db(taos, TEST_DB);
  CHECK_CODE(code, "switch to database");

  res = taos_query(taos, "create table if not exists meters(ts timestamp, a int) tags(area int)");
  CHECK_RES(res, "create stable meters");
  taos_free_result(res);

  res = taos_query(taos, "create table if not exists t0 using meters tags(0)");
  CHECK_RES(res, "create table t0");
  taos_free_result(res);

  res = taos_query(taos, "create table if not exists t1 using meters tags(1)");
  CHECK_RES(res, "create table t1");
  taos_free_result(res);

  res = taos_query(taos, "create table if not exists t2 using meters tags(2)");
  CHECK_RES(res, "create table t2");
  taos_free_result(res);

  res = taos_query(taos, "create table if not exists t3 using meters tags(3)");
  CHECK_RES(res, "create table t3");
  taos_free_result(res);

  res = taos_query(taos, "create table if not exists t4 using meters tags(4)");
  CHECK_RES(res, "create table t4");
  taos_free_result(res);

  res = taos_query(taos, "create table if not exists t5 using meters tags(5)");
  CHECK_RES(res, "create table t5");
  taos_free_result(res);

  res = taos_query(taos, "create table if not exists t6 using meters tags(6)");
  CHECK_RES(res, "create table t6");
  taos_free_result(res);

  res = taos_query(taos, "create table if not exists t7 using meters tags(7)");
  CHECK_RES(res, "create table t7");
  taos_free_result(res);

  res = taos_query(taos, "create table if not exists t8 using meters tags(8)");
  CHECK_RES(res, "create table t8");
  taos_free_result(res);

  res = taos_query(taos, "create table if not exists t9 using meters tags(9)");
  CHECK_RES(res, "create table t9");
  taos_free_result(res);

  res = taos_query(taos,
                   "insert into t0 values('2020-01-01 00:00:00.000', 0)"
                   " ('2020-01-01 00:01:00.000', 0)"
                   " ('2020-01-01 00:02:00.000', 0)"
                   " t1 values('2020-01-01 00:00:00.000', 1)"
                   " ('2020-01-01 00:01:00.000', 1)"
                   " ('2020-01-01 00:02:00.000', 1)"
                   " ('2020-01-01 00:03:00.000', 1)"
                   " t2 values('2020-01-01 00:00:00.000', 2)"
                   " ('2020-01-01 00:01:00.000', 2)"
                   " ('2020-01-01 00:01:01.000', 2)"
                   " ('2020-01-01 00:01:02.000', 2)"
                   " t3 values('2020-01-01 00:01:02.000', 3)"
                   " t4 values('2020-01-01 00:01:02.000', 4)"
                   " t5 values('2020-01-01 00:01:02.000', 5)"
                   " t6 values('2020-01-01 00:01:02.000', 6)"
                   " t7 values('2020-01-01 00:01:02.000', 7)"
                   " t8 values('2020-01-01 00:01:02.000', 8)"
                   " t9 values('2020-01-01 00:01:02.000', 9)");
  CHECK_RES(res, "insert into meters");
  CHECK_CONDITION(taos_affected_rows(res), "insert into meters", "insufficient count");
  taos_free_result(res);

  res = taos_query(
      taos,
      "create table if not exists m1 (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 "
      "double, bin binary(40), blob nchar(10))");
  CHECK_RES(res, "create table m1");
  taos_free_result(res);

  usleep(1000000);
}

static void verifySchemaLess(TAOS* taos) {
  TAOS_RES* res = NULL;
  char*     lines[] = {
      "st,t1=3i64,t2=4f64,t3=L\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
      "st,t1=4i64,t3=L\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000",
      "st,t2=5f64,t3=L\"ste\" c1=4i64,c2=true,c3=L\"iam\" 1626056811823316532",
      "st,t1=4i64,t2=5f64,t3=L\"t4\" c1=3i64,c3=L\"passitagain\",c2=true,c4=5f64 1626006833642000000",
      "st,t2=5f64,t3=L\"ste2\" c3=L\"iamszhou\",c2=false 1626056811843316532",
      "st,t2=5f64,t3=L\"ste2\" c3=L\"iamszhou\",c2=false,c5=5f64,c6=7u64,c7=32i32,c8=88.88f32 1626056812843316532",
      "st,t1=4i64,t3=L\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 "
          "1626006933640000000",
      "st,t1=4i64,t3=L\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 "
          "1626006933640000000",
      "st,t1=4i64,t3=L\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 "
          "1626006933641000000"};

  res = taos_schemaless_insert_with_reqid(taos, lines, sizeof(lines) / sizeof(char*), TSDB_SML_LINE_PROTOCOL,
                                          TSDB_SML_TIMESTAMP_NANO_SECONDS, CONST_REQ_ID);
  CHECK_RES(res, "insert schema-less data");
  printf("successfully inserted %d rows\n", taos_affected_rows(res));
  taos_free_result(res);
}

static int32_t printResult(TAOS_RES* res, int32_t nlimit) {
  TAOS_ROW    row = NULL;
  TAOS_FIELD* fields = NULL;
  int32_t     numFields = 0;
  int32_t     nRows = 0;

  numFields = taos_num_fields(res);
  fields = taos_fetch_fields(res);
  while ((nlimit-- > 0) && (row = taos_fetch_row(res))) {
    char temp[256] = {0};
    taos_print_row(temp, row, fields, numFields);
    puts(temp);
    nRows++;
  }
  return nRows;
}

static void verifyQuery(TAOS* taos) {
  TAOS_RES* res = NULL;

  res = taos_query_with_reqid(taos, "select * from meters", CONST_REQ_ID);
  CHECK_RES(res, "select from meters");
  printResult(res, INT32_MAX);
  taos_free_result(res);

  res = taos_query_with_reqid(taos, "select * from t0", CONST_REQ_ID);
  CHECK_RES(res, "select from t0");
  printResult(res, INT32_MAX);
  taos_free_result(res);

  res = taos_query_with_reqid(taos, "select * from t1", CONST_REQ_ID);
  CHECK_RES(res, "select from t1");
  printResult(res, INT32_MAX);
  taos_free_result(res);

  res = taos_query_with_reqid(taos, "select * from t2", CONST_REQ_ID);
  CHECK_RES(res, "select from t2");
  printResult(res, INT32_MAX);
  taos_free_result(res);

  res = taos_query_with_reqid(taos, "select * from t3", CONST_REQ_ID);
  CHECK_RES(res, "select from t3");
  printResult(res, INT32_MAX);
  taos_free_result(res);

  printf("succeed to read from meters\n");
}

void retrieveCallback(void* param, TAOS_RES* res, int32_t nrows) {
  if (nrows == 0) {
    taos_free_result(res);
  } else {
    printResult(res, nrows);
    taos_fetch_rows_a(res, retrieveCallback, param);
  }
}

void selectCallback(void* param, TAOS_RES* res, int32_t code) {
  CHECK_CODE(code, "read async from table");
  taos_fetch_rows_a(res, retrieveCallback, param);
}

static void verifyQueryAsync(TAOS* taos) {
  taos_query_a_with_reqid(taos, "select *from meters", selectCallback, NULL, CONST_REQ_ID);
  taos_query_a_with_reqid(taos, "select *from t0", selectCallback, NULL, CONST_REQ_ID);
  taos_query_a_with_reqid(taos, "select *from t1", selectCallback, NULL, CONST_REQ_ID);
  taos_query_a_with_reqid(taos, "select *from t2", selectCallback, NULL, CONST_REQ_ID);
  taos_query_a_with_reqid(taos, "select *from t3", selectCallback, NULL, CONST_REQ_ID);

  sleep(1);
}

void veriryStmt(TAOS* taos) {
  // insert 10 records
  struct {
    int64_t ts[10];
    int8_t  b[10];
    int8_t  v1[10];
    int16_t v2[10];
    int32_t v4[10];
    int64_t v8[10];
    float   f4[10];
    double  f8[10];
    char    bin[10][40];
    char    blob[10][80];
  } v;

  int32_t* t8_len = malloc(sizeof(int32_t) * 10);
  int32_t* t16_len = malloc(sizeof(int32_t) * 10);
  int32_t* t32_len = malloc(sizeof(int32_t) * 10);
  int32_t* t64_len = malloc(sizeof(int32_t) * 10);
  int32_t* float_len = malloc(sizeof(int32_t) * 10);
  int32_t* double_len = malloc(sizeof(int32_t) * 10);
  int32_t* bin_len = malloc(sizeof(int32_t) * 10);
  int32_t* blob_len = malloc(sizeof(int32_t) * 10);

  TAOS_STMT*      stmt = taos_stmt_init_with_reqid(taos, CONST_REQ_ID);
  TAOS_MULTI_BIND params[10];
  char            is_null[10] = {0};

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

  int32_t code = taos_stmt_prepare(
      stmt, "insert into ? (ts, b, v1, v2, v4, v8, f4, f8, bin, blob) values(?,?,?,?,?,?,?,?,?,?)", 0);
  CHECK_CODE(code, "taos_stmt_prepare");

  code = taos_stmt_set_tbname(stmt, "m1");
  CHECK_CODE(code, "taos_stmt_set_tbname");

  int64_t ts = 1591060628000000000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = ts;
    ts += 1000000;
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

  code = taos_stmt_bind_param_batch(stmt, params);
  CHECK_CODE(code, "taos_stmt_bind_param_batch");

  code = taos_stmt_add_batch(stmt);
  CHECK_CODE(code, "taos_stmt_add_batch");

  code = taos_stmt_execute(stmt);
  CHECK_CODE(code, "taos_stmt_execute");

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

int main(int argc, char* argv[]) {
  TAOS*   taos = NULL;
  int32_t code = 0;

  taos = getNewConnection();
  taos_select_db(taos, TEST_DB);
  CHECK_CODE(code, "switch to database");

  printf("************  prepare data  *************\n");
  prepareData(taos);

  for (int32_t i = 0; i < NUM_ROUNDS; ++i) {
    printf("************  verify schema-less  *************\n");
    verifySchemaLess(taos);

    printf("************  verify query  *************\n");
    verifyQuery(taos);

    printf("*********  verify async query  **********\n");
    verifyQueryAsync(taos);

    printf("*********  verify stmt query  **********\n");
    veriryStmt(taos);

    printf("done\n");
  }

  taos_close(taos);
  taos_cleanup();

  return 0;
}
