// sample code for TDengine subscribe/consume API
// to compile: gcc -o subscribe subscribe.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>  // include TDengine header file
#include <unistd.h>

int nTotalRows;

void print_result(TAOS_RES* res, int blockFetch) {
  TAOS_ROW    row = NULL;
  int         num_fields = taos_num_fields(res);
  TAOS_FIELD* fields = taos_fetch_fields(res);
  int         nRows = 0;
  char        buf[4096];

  
  if (blockFetch) {
    nRows = taos_fetch_block(res, &row);
    //for (int i = 0; i < nRows; i++) {
    //  taos_print_row(buf, row + i, fields, num_fields);
    //  puts(buf);
    //}
  } else {
    while ((row = taos_fetch_row(res))) {
      taos_print_row(buf, row, fields, num_fields);
      puts(buf);
      nRows++;
    }
  }

  nTotalRows += nRows;
  printf("%d rows consumed.\n", nRows);
}


void subscribe_callback(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code) {
  print_result(res, *(int*)param);
}


void check_row_count(int line, TAOS_RES* res, int expected) {
  int actual = 0;
  TAOS_ROW    row;
  while ((row = taos_fetch_row(res))) {
    actual++;
  }
  if (actual != expected) {
    printf("line %d: row count mismatch, expected: %d, actual: %d\n", line, expected, actual);
  } else {
    printf("line %d: %d rows consumed as expected\n", line, actual);
  }
}


void do_query(TAOS* taos, const char* sql) {
  TAOS_RES* res = taos_query(taos, sql);
  taos_free_result(res);
}


void run_test(TAOS* taos) {
  do_query(taos, "drop database if exists test;");
  
  usleep(100000);
  do_query(taos, "create database test;");
  usleep(100000);
  do_query(taos, "use test;");

  usleep(100000);
  do_query(taos, "create table meters(ts timestamp, a int) tags(area int);");

  do_query(taos, "create table t0 using meters tags(0);");
  do_query(taos, "create table t1 using meters tags(1);");
  do_query(taos, "create table t2 using meters tags(2);");
  do_query(taos, "create table t3 using meters tags(3);");
  do_query(taos, "create table t4 using meters tags(4);");
  do_query(taos, "create table t5 using meters tags(5);");
  do_query(taos, "create table t6 using meters tags(6);");
  do_query(taos, "create table t7 using meters tags(7);");
  do_query(taos, "create table t8 using meters tags(8);");
  do_query(taos, "create table t9 using meters tags(9);");

  do_query(taos, "insert into t0 values('2020-01-01 00:00:00.000', 0);");
  do_query(taos, "insert into t0 values('2020-01-01 00:01:00.000', 0);");
  do_query(taos, "insert into t0 values('2020-01-01 00:02:00.000', 0);");
  do_query(taos, "insert into t1 values('2020-01-01 00:00:00.000', 0);");
  do_query(taos, "insert into t1 values('2020-01-01 00:01:00.000', 0);");
  do_query(taos, "insert into t1 values('2020-01-01 00:02:00.000', 0);");
  do_query(taos, "insert into t1 values('2020-01-01 00:03:00.000', 0);");
  do_query(taos, "insert into t2 values('2020-01-01 00:00:00.000', 0);");
  do_query(taos, "insert into t2 values('2020-01-01 00:01:00.000', 0);");
  do_query(taos, "insert into t2 values('2020-01-01 00:01:01.000', 0);");
  do_query(taos, "insert into t2 values('2020-01-01 00:01:02.000', 0);");
  do_query(taos, "insert into t3 values('2020-01-01 00:01:02.000', 0);");
  do_query(taos, "insert into t4 values('2020-01-01 00:01:02.000', 0);");
  do_query(taos, "insert into t5 values('2020-01-01 00:01:02.000', 0);");
  do_query(taos, "insert into t6 values('2020-01-01 00:01:02.000', 0);");
  do_query(taos, "insert into t7 values('2020-01-01 00:01:02.000', 0);");
  do_query(taos, "insert into t8 values('2020-01-01 00:01:02.000', 0);");
  do_query(taos, "insert into t9 values('2020-01-01 00:01:02.000', 0);");

  // super tables subscription
  usleep(1000000);

  TAOS_SUB* tsub = taos_subscribe(taos, 0, "test", "select * from meters;", NULL, NULL, 0);
  TAOS_RES* res = taos_consume(tsub);
  check_row_count(__LINE__, res, 18);

  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 0);

  do_query(taos, "insert into t0 values('2020-01-01 00:02:00.001', 0);");
  do_query(taos, "insert into t8 values('2020-01-01 00:01:03.000', 0);");
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 2);

  do_query(taos, "insert into t2 values('2020-01-01 00:01:02.001', 0);");
  do_query(taos, "insert into t1 values('2020-01-01 00:03:00.001', 0);");
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 2);

  do_query(taos, "insert into t1 values('2020-01-01 00:03:00.002', 0);");
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 1);

  // keep progress information and restart subscription
  taos_unsubscribe(tsub, 1);
  do_query(taos, "insert into t0 values('2020-01-01 00:04:00.000', 0);");
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

  do_query(taos, "insert into t0 values('2020-01-01 00:04:00.001', 0);");
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 1);

  taos_unsubscribe(tsub, 0);
}


int main(int argc, char *argv[]) {
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";
  const char* sql = "select * from meters;";
  const char* topic = "test-multiple";
  int async = 1, restart = 0, keep = 1, test = 0, blockFetch = 0;

  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "-h=", 3) == 0) {
      host = argv[i] + 3;
      continue;
    }
    if (strncmp(argv[i], "-u=", 3) == 0) {
      user = argv[i] + 3;
      continue;
    }
    if (strncmp(argv[i], "-p=", 3) == 0) {
      passwd = argv[i] + 3;
      continue;
    }
    if (strcmp(argv[i], "-sync") == 0) {
      async = 0;
      continue;
    }
    if (strcmp(argv[i], "-restart") == 0) {
      restart = 1;
      continue;
    }
    if (strcmp(argv[i], "-single") == 0) {
      sql = "select * from t0;";
      topic = "test-single";
      continue;
    }
    if (strcmp(argv[i], "-nokeep") == 0) {
      keep = 0;
      continue;
    }
    if (strncmp(argv[i], "-sql=", 5) == 0) {
      sql = argv[i] + 5;
      topic = "test-custom";
      continue;
    }
    if (strcmp(argv[i], "-test") == 0) {
      test = 1;
      continue;
    }
    if (strcmp(argv[i], "-block-fetch") == 0) {
      blockFetch = 1;
      continue;
    }
  }

  // init TAOS
  taos_init();

  TAOS* taos = taos_connect(host, user, passwd, "", 0);
  if (taos == NULL) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  if (test) {
    run_test(taos);
    taos_close(taos);
    exit(0);
  }

  taos_select_db(taos, "test");
  TAOS_SUB* tsub = NULL;
  if (async) {
    // create an asynchronized subscription, the callback function will be called every 1s
    tsub = taos_subscribe(taos, restart, topic, sql, subscribe_callback, &blockFetch, 1000);
  } else {
    // create an synchronized subscription, need to call 'taos_consume' manually
    tsub = taos_subscribe(taos, restart, topic, sql, NULL, NULL, 0);
  }

  if (tsub == NULL) {
    printf("failed to create subscription.\n");
    exit(0);
  } 

  if (async) {
    getchar();
  } else while(1) {
    TAOS_RES* res = taos_consume(tsub);
    if (res == NULL) {
      printf("failed to consume data.");
      break;
    } else {
      print_result(res, blockFetch);
      getchar();
    }
  }

  printf("total rows consumed: %d\n", nTotalRows);
  taos_unsubscribe(tsub, keep);
  taos_close(taos);

  return 0;
}
