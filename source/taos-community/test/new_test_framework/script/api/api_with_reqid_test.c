// sample code to verify all TDengine API
// to compile: gcc -o apitest apitest.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "taos.h"
static int64_t count = 10000;

int64_t genReqid() {
  count += 100;
  return count;
}
static void prepare_data(TAOS* taos) {
  TAOS_RES* result;
  result = taos_query_with_reqid(taos, "drop database if exists test;", genReqid());
  taos_free_result(result);
  usleep(100000);
  result = taos_query_with_reqid(taos, "create database test precision 'us';", genReqid());
  taos_free_result(result);
  usleep(100000);
  taos_select_db(taos, "test");

  result = taos_query_with_reqid(taos, "create table meters(ts timestamp, a int) tags(area int);", genReqid());
  taos_free_result(result);

  result = taos_query_with_reqid(taos, "create table t0 using meters tags(0);", genReqid());
  taos_free_result(result);
  result = taos_query_with_reqid(taos, "create table t1 using meters tags(1);", genReqid());
  taos_free_result(result);
  result = taos_query_with_reqid(taos, "create table t2 using meters tags(2);", genReqid());
  taos_free_result(result);
  result = taos_query_with_reqid(taos, "create table t3 using meters tags(3);", genReqid());
  taos_free_result(result);
  result = taos_query_with_reqid(taos, "create table t4 using meters tags(4);", genReqid());
  taos_free_result(result);
  result = taos_query_with_reqid(taos, "create table t5 using meters tags(5);", genReqid());
  taos_free_result(result);
  result = taos_query_with_reqid(taos, "create table t6 using meters tags(6);", genReqid());
  taos_free_result(result);
  result = taos_query_with_reqid(taos, "create table t7 using meters tags(7);", genReqid());
  taos_free_result(result);
  result = taos_query_with_reqid(taos, "create table t8 using meters tags(8);", genReqid());
  taos_free_result(result);
  result = taos_query(taos, "create table t9 using meters tags(9);");
  taos_free_result(result);

  result = taos_query_with_reqid(taos,
                                 "insert into t0 values('2020-01-01 00:00:00.000', 0)"
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
                                 " t9 values('2020-01-01 00:01:02.000', 0)",
                                 genReqid());
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
      // for (int i = 0; i < rows; i++) {
      //   char temp[256];
      //   taos_print_row(temp, row + i, fields, num_fields);
      //   puts(temp);
      // }
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

  TAOS_RES* res = taos_query_with_reqid(taos, "select * from meters", genReqid());
  check_row_count(__LINE__, res, 18);
  printf("result precision is: %d\n", taos_result_precision(res));
  int c = taos_field_count(res);
  printf("field count is: %d\n", c);
  int* lengths = taos_fetch_lengths(res);
  for (int i = 0; i < c; i++) {
    printf("length of column %d is %d\n", i, lengths[i]);
  }
  taos_free_result(res);

  res = taos_query_with_reqid(taos, "select * from t0", genReqid());
  check_row_count(__LINE__, res, 3);
  taos_free_result(res);

  res = taos_query_with_reqid(taos, "select * from nonexisttable", genReqid());
  code = taos_errno(res);
  printf("code=%d, error msg=%s\n", code, taos_errstr(res));
  taos_free_result(res);

  res = taos_query_with_reqid(taos, "select * from meters", genReqid());
  taos_stop_query(res);
  taos_free_result(res);
}

void retrieve_callback(void* param, TAOS_RES* tres, int numOfRows) {
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

void select_callback(void* param, TAOS_RES* tres, int code) {
  if (code == 0 && tres) {
    taos_fetch_rows_a(tres, retrieve_callback, param);
  } else {
    printf("\033[31masync select failed, code: %d\033[0m\n", code);
  }
}

void verify_async(TAOS* taos) {
  prepare_data(taos);
  taos_query_a_with_reqid(taos, "select * from meters", select_callback, NULL, genReqid());
  usleep(1000000);
}

int32_t verify_schema_less(TAOS* taos) {
  TAOS_RES* result;
  result = taos_query_with_reqid(taos, "drop database if exists test;", genReqid());
  taos_free_result(result);
  usleep(100000);
  result = taos_query_with_reqid(taos, "create database test precision 'us' update 1;", genReqid());
  taos_free_result(result);
  usleep(100000);

  taos_select_db(taos, "test");
  result = taos_query_with_reqid(taos, "create stable ste(ts timestamp, f int) tags(t1 bigint)", genReqid());
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
      "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 "
      "1626006933640000000ns",
      "stf,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 "
      "1626006933640000000ns",
      "stf,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 "
      "1626006933641000000ns"};

  taos_select_db(taos, "test");

  TAOS_RES* res = taos_schemaless_insert_with_reqid(taos, lines, sizeof(lines) / sizeof(char*), TSDB_SML_LINE_PROTOCOL,
                                                    TSDB_SML_TIMESTAMP_NOT_CONFIGURED, genReqid());
  if (taos_errno(res) != 0) {
    printf("failed to insert schema-less data, reason: %s\n", taos_errstr(res));
  } else {
    int affectedRow = taos_affected_rows(res);
    printf("successfully inserted %d rows\n", affectedRow);
  }
  taos_free_result(res);

  return (code);
}

void veriry_stmt(TAOS* taos) {
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
  const char* sql =
      "create table m1 (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin "
      "binary(40), blob nchar(10))";
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

  TAOS_STMT*      stmt = taos_stmt_init_with_reqid(taos, genReqid());
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

  sql = "insert into ? (ts, b, v1, v2, v4, v8, f4, f8, bin, blob) values(?,?,?,?,?,?,?,?,?,?)";
  code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;
  }

  code = taos_stmt_set_tbname(stmt, "m1");
  if (code != 0) {
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
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";

  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  TAOS* taos = taos_connect(host, user, passwd, "", 0);
  if (taos == NULL) {
    printf("\033[31mfailed to connect to db, reason:%s\033[0m\n", taos_errstr(taos));
    exit(1);
  }

  printf("************  verify schema-less  *************\n");
  verify_schema_less(taos);

  printf("************  verify query  *************\n");
  verify_query(taos);

  printf("*********  verify async query  **********\n");
  verify_async(taos);

  printf("*********  verify stmt query  **********\n");
  veriry_stmt(taos);

  printf("done\n");
  taos_close(taos);
  taos_cleanup();
}
