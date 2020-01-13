// sample code for TDengine subscribe/consume API
// to compile: gcc -o subscribe subscribe.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>  // include TDengine header file
#include <unistd.h>

void print_result(TAOS_RES* res) {
  TAOS_ROW    row = NULL;
  int         num_fields = taos_num_fields(res);
  TAOS_FIELD* fields = taos_fetch_fields(res);

#if 0

  int nRows = taos_fetch_block(res, &row);
  for (int i = 0; i < nRows; i++) {
    char temp[256];
    taos_print_row(temp, row + i, fields, num_fields);
    puts(temp);
  }

#else

  while ((row = taos_fetch_row(res))) {
    char temp[256];
    taos_print_row(temp, row, fields, num_fields);
    puts(temp);
  }

#endif
}

void subscribe_callback(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code) {
  print_result(res);
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

void run_test(TAOS* taos) {
  taos_query(taos, "drop database test;");
  
  usleep(100000);
  taos_query(taos, "create database test;");
  usleep(100000);
  taos_query(taos, "use test;");
  usleep(100000);
  taos_query(taos, "create table meters(ts timestamp, a int, b binary(20)) tags(loc binary(20), area int);");

  taos_query(taos, "insert into t0 using meters tags('beijing', 0) values('2020-01-01 00:00:00.000', 0, 'china');");
  taos_query(taos, "insert into t0 using meters tags('beijing', 0) values('2020-01-01 00:01:00.000', 0, 'china');");
  taos_query(taos, "insert into t0 using meters tags('beijing', 0) values('2020-01-01 00:02:00.000', 0, 'china');");
  taos_query(taos, "insert into t1 using meters tags('shanghai', 0) values('2020-01-01 00:00:00.000', 0, 'china');");
  taos_query(taos, "insert into t1 using meters tags('shanghai', 0) values('2020-01-01 00:01:00.000', 0, 'china');");
  taos_query(taos, "insert into t1 using meters tags('shanghai', 0) values('2020-01-01 00:02:00.000', 0, 'china');");
  taos_query(taos, "insert into t1 using meters tags('shanghai', 0) values('2020-01-01 00:03:00.000', 0, 'china');");
  taos_query(taos, "insert into t2 using meters tags('london', 0) values('2020-01-01 00:00:00.000', 0, 'UK');");
  taos_query(taos, "insert into t2 using meters tags('london', 0) values('2020-01-01 00:01:00.000', 0, 'UK');");
  taos_query(taos, "insert into t2 using meters tags('london', 0) values('2020-01-01 00:01:01.000', 0, 'UK');");
  taos_query(taos, "insert into t2 using meters tags('london', 0) values('2020-01-01 00:01:02.000', 0, 'UK');");

  // super tables subscription

  TAOS_SUB* tsub = taos_subscribe(taos, 0, "test", "select * from meters;", NULL, NULL, 0);
  TAOS_RES* res = taos_consume(tsub);
  check_row_count(__LINE__, res, 11);

  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 0);

  taos_query(taos, "insert into t2 using meters tags('london', 0) values('2020-01-01 00:01:02.001', 0, 'UK');");
  taos_query(taos, "insert into t1 using meters tags('london', 0) values('2020-01-01 00:03:00.001', 0, 'UK');");
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 2);

  taos_query(taos, "insert into t1 using meters tags('shanghai', 0) values('2020-01-01 00:03:00.002', 0, 'china');");
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 1);

  // keep progress information and continue previous subscription
  taos_unsubscribe(tsub, 1);
  taos_query(taos, "insert into t0 using meters tags('beijing', 0) values('2020-01-01 00:03:00.000', 0, 'china');");
  tsub = taos_subscribe(taos, 1, "test", "select * from meters;", NULL, NULL, 0);
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 15);

  // don't keep progress information and continue previous subscription
  taos_unsubscribe(tsub, 0);
  tsub = taos_subscribe(taos, 0, "test", "select * from meters;", NULL, NULL, 0);
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 15);

  // single meter subscription

  taos_unsubscribe(tsub, 0);
  tsub = taos_subscribe(taos, 0, "test", "select * from t0;", NULL, NULL, 0);
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 4);

  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 0);

  taos_query(taos, "insert into t0 using meters tags('beijing', 0) values('2020-01-01 00:03:00.001', 0, 'china');");
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 1);

  taos_query(taos, "insert into t0 using meters tags('beijing', 0) values('2020-01-01 00:03:00.002', 0, 'china');");
  taos_query(taos, "insert into t0 using meters tags('beijing', 0) values('2020-01-01 00:04:00.000', 0, 'china');");
  res = taos_consume(tsub);
  check_row_count(__LINE__, res, 2);

  taos_unsubscribe(tsub, 0);
}


int main(int argc, char *argv[]) {
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";
  const char* sql = "select * from meters;";
  const char* topic = "test-multiple";
  int async = 1, restart = 0, keep = 1, test = 0;
  TAOS_SUB* tsub = NULL;

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
  }

  // init TAOS
  taos_init();

  TAOS* taos = taos_connect(host, user, passwd, "test", 0);
  if (taos == NULL) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  if (test) {
    run_test(taos);
    taos_close(taos);
    exit(0);
  }

  if (async) {
    tsub = taos_subscribe(taos, restart, topic, sql, subscribe_callback, NULL, 1000);
  } else {
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
    print_result(res);
    getchar();
  }

  taos_unsubscribe(tsub, keep);
  taos_close(taos);

  return 0;
}

