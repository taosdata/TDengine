// sample code to verify all TDengine API
// to compile: gcc -o apitest apitest.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
#include <unistd.h>

static void prepare_data(TAOS* taos) {
  TAOS_RES* result;
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

  result = taos_query(taos,
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
      // for (int i = 0; i < rows; i++) {
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
  printf("result precision is: %d\n", taos_result_precision(res));
  int c = taos_field_count(res);
  printf("field count is: %d\n", c);
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

void subscribe_callback(TAOS_SUB* tsub, TAOS_RES* res, void* param, int code) {
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

  TAOS_RES* result;
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
  taos_query_a(taos, "select * from meters", select_callback, NULL);
  usleep(1000000);
}

void stream_callback(void* param, TAOS_RES* res, TAOS_ROW row) {
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
  TAOS_STREAM* strm =
      taos_open_stream(taos, "select count(*) from meters interval(1m)", stream_callback, 0, NULL, NULL);
  printf("waiting for stream data\n");
  usleep(100000);
  TAOS_RES* result = taos_query(taos, "insert into t0 values(now, 0)(now+5s,1)(now+10s, 2);");
  taos_free_result(result);
  usleep(200000000);
  taos_close_stream(strm);
}

int32_t verify_schema_less(TAOS* taos) {
  TAOS_RES* result;
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
      "st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 "
      "1626006933640000000ns",
      "stf,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64,c6=7u64 "
      "1626006933640000000ns",
      "stf,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 "
      "1626006933641000000ns"};

  code = taos_insert_lines(taos, lines, sizeof(lines) / sizeof(char*));

  char* lines2[] = {
      "stg,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000ns",
      "stg,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000ns"};
  code = taos_insert_lines(taos, &lines2[0], 1);
  code = taos_insert_lines(taos, &lines2[1], 1);

  char* lines3[] = {
      "sth,t1=4i64,t2=5f64,t4=5f64,ID=\"childtable\" c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 "
      "1626006933641ms",
      "sth,t1=4i64,t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin_stf\",c2=false,c5=5f64,c6=7u64 1626006933654ms"};
  code = taos_insert_lines(taos, lines3, 2);

  char* lines4[] = {"st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000ns",
                    "dgtyqodr,t2=5f64,t3=L\"ste\" c1=tRue,c2=4i64,c3=\"iam\" 1626056811823316532ns"};
  code = taos_insert_lines(taos, lines4, 2);

  char* lines5[] = {
      "zqlbgs,id=\"zqlbgs_39302_21680\",t0=f,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11."
      "12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" "
      "c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7="
      "\"binaryColValue\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000ns",
      "zqlbgs,t9=f,id=\"zqlbgs_39302_21680\",t0=f,t1=127i8,t11=127i8,t2=32767i16,t3=2147483647i32,t4="
      "9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\",t10="
      "L\"ncharTagValue\" "
      "c10=f,c0=f,c1=127i8,c12=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22."
      "123456789f64,c7=\"binaryColValue\",c8=L\"ncharColValue\",c9=7u64,c11=L\"ncharColValue\" 1626006833639000000ns"};
  code = taos_insert_lines(taos, &lines5[0], 1);
  code = taos_insert_lines(taos, &lines5[1], 1);

  char* lines6[] = {"st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000ns",
                    "dgtyqodr,t2=5f64,t3=L\"ste\" c1=tRue,c2=4i64,c3=\"iam\" 1626056811823316532ns"};
  code = taos_insert_lines(taos, lines6, 2);
  return (code);
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

  char* info = taos_get_server_info(taos);
  printf("server info: %s\n", info);
  info = taos_get_client_info(taos);
  printf("client info: %s\n", info);

  printf("************  verify schema-less  *************\n");
  verify_schema_less(taos);

  printf("************  verify query  *************\n");
  verify_query(taos);

  printf("*********  verify async query  **********\n");
  verify_async(taos);

  printf("*********** verify subscribe ************\n");
  verify_subscribe(taos);

  printf("************ verify stream  *************\n");
  // verify_stream(taos);
  printf("done\n");
  taos_close(taos);
  taos_cleanup();
}
