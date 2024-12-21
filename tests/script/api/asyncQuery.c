// sample code to verify multiple queries with the same reqid
// to compile: gcc -o sameReqdiTest sameReqidTest.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "taos.h"

#define CONST_REQ_ID 12345
#define TEST_DB      "db1"

#define CHECK_CONDITION(condition, prompt, errstr)                                                     \
  do {                                                                                                 \
    if (!(condition)) {                                                                                \
      printf("\033[31m[%s:%d] failed to " prompt ", reason: %s\033[0m\n", __func__, __LINE__, errstr); \
      exit(EXIT_FAILURE);                                                                              \
    }                                                                                                  \
  } while (0)

#define CHECK_RES(res, prompt)   CHECK_CONDITION(taos_errno(res) == 0, prompt, taos_errstr(res))
#define CHECK_CODE(code, prompt) CHECK_CONDITION(code == 0, prompt, taos_errstr(NULL))

int64_t errTimes = 0, finQueries = 0;

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

static int32_t printResult(TAOS_RES* res, int32_t nlimit) {
  TAOS_ROW    row = NULL;
  TAOS_FIELD* fields = NULL;
  int32_t     numFields = 0;
  int32_t     nRows = 0;

  numFields = taos_num_fields(res);
  fields = taos_fetch_fields(res);
  while ((nlimit-- > 0) && (row = taos_fetch_row(res))) {
    nRows++;
  }
  return nRows;
}

void retrieveCallback(void* param, TAOS_RES* res, int32_t nrows) {
  if (nrows < 0) {
    taos_free_result(res);
    atomic_add_fetch_64(&finQueries, 1);
    atomic_add_fetch_64(&errTimes, 1);    
  } else if (nrows == 0) {
    taos_free_result(res);
    atomic_add_fetch_64(&finQueries, 1);
  } else {
    printResult(res, nrows);
    taos_fetch_rows_a(res, retrieveCallback, param);
  }
}

void selectCallback(void* param, TAOS_RES* res, int32_t code) {
  if (code) {
     atomic_add_fetch_64(&errTimes, 1);
     taos_free_result(res);
     atomic_add_fetch_64(&finQueries, 1);
     return;
  }
  taos_fetch_rows_a(res, retrieveCallback, param);
}


#if 0
static void verifyQueryAsync(TAOS* taos) {
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2023-01-01' and ts <= '2024-12-01' interval(1s) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2023-01-01' and ts <= '2024-12-01' interval(1s) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2023-01-01' and ts <= '2024-12-01' interval(1s) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2023-01-01' and ts <= '2024-12-01' interval(1s) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2023-01-01' and ts <= '2024-12-01' interval(1s) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2023-01-01' and ts <= '2024-12-01' interval(1s) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2023-01-01' and ts <= '2024-12-01' interval(1s) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2023-01-01' and ts <= '2024-12-01' interval(1s) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2023-01-01' and ts <= '2024-12-01' interval(1s) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;;", selectCallback, NULL);
  taos_query_a(taos, "select twa(c1) from stb interval(10s);", selectCallback, NULL);
}
#else
static void verifyQueryAsync(TAOS* taos) {
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2024-11-01' and ts <= '2024-12-01' interval(1m) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2024-11-01' and ts <= '2024-12-01' interval(1m) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2024-11-01' and ts <= '2024-12-01' interval(1m) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2024-11-01' and ts <= '2024-12-01' interval(1m) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2024-11-01' and ts <= '2024-12-01' interval(1m) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2024-11-01' and ts <= '2024-12-01' interval(1m) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2024-11-01' and ts <= '2024-12-01' interval(1m) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2024-11-01' and ts <= '2024-12-01' interval(1m) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;", selectCallback, NULL);
  taos_query_a(taos, "select * from (select cast(count(*) as binary(100)) a, rand() b from tbx where ts >= '2024-11-01' and ts <= '2024-12-01' interval(1m) fill(value, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')) t1 group by t1.a, t1.b;", selectCallback, NULL);
  taos_query_a(taos, "select twa(c1) from tbx interval(10s);", selectCallback, NULL);
}
#endif

int main(int argc, char* argv[]) {
  TAOS*   taos = NULL;
  int32_t code = 0;

  taos = getNewConnection();
  taos_select_db(taos, TEST_DB);
  CHECK_CODE(code, "switch to database");

  printf("************  prepare data  *************\n");

  //create table tbx (ts timestamp, f1 int); insert into tbx values('2024-11-11 00:00:00', 1);

  for (int64_t i = 0; i < 1000000000; ++i) {
    verifyQueryAsync(taos);
    printf("%llu queries launched, errTimes:%lld \n", i * 10, errTimes);
    while ((i * 10 - atomic_load_64(&finQueries)) > 100) {
      printf("left queries:%llu\n", (i * 10 - atomic_load_64(&finQueries)));
      taosMsleep(2000);
    }
    printf("\n");
  }

  taos_close(taos);
  taos_cleanup();

  return 0;
}
