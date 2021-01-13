#include <assert.h>
#include <float.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "testCommon.h"

static int64_t start_ts = 1433955661000;

/*
 * create table m1(ts timestamp, k int, h binary(20), t bigint,
 * s float, f double, x smallint, y tinyint, z bool)
 * tags(a int, b binary(20), c bigint)
 */
static void insertSampleData(TAOS *conn, int32_t count, int32_t tableIdx, int64_t timeDelta) {
  int64_t startTS = start_ts;

  int32_t item = 20;
  int32_t totalRound = count / item;

  char tt[1024 * 64] = {0};
  char first[1024] = {0};
  char second[1024] = {0};

  int32_t delta = 0;

  /*
   * ts timestamp, k int, h binary(400), t bigint, s float, f double, x smallint, y tinyint, z bool
   * k = delta
   * h = delta_delta_123
   * t = delta+10000000
   * s = delta * 1.023
   * f = delta/12 + 1
   * x = delta%30000
   * y = delta%256
   * z = delta%2
   */
  for (int32_t i = 0; i < totalRound; ++i) {
    int32_t len = sprintf(first, "insert into t2m%d values (%ld, %d, '%d_%d_123', %d, %f, %f, %d, %d, %d)", tableIdx,
                          startTS + delta * timeDelta, delta, delta, delta, delta + 10000000, delta * 1.023,
                          ((double)delta) / 12 + 1, delta % 30000, delta % 128, delta % 2);
    strncat(tt, first, len + 1);

    delta++;

    for (int32_t j = 0; j < item - 1; ++j) {
      len = sprintf(second, " (%ld, %d, '%d_%d_123', %d, %f, %f, %d, %d, %d)", startTS + delta * timeDelta, delta,
                    delta, delta, delta + 10000000, delta * 1.023, ((double)delta) / 12 + 1, delta % 30000, delta % 128,
                    delta % 2);
      strncat(tt, second, len + 1);
      delta++;
    }

    NO_VALID_SUCCESS_SQL(conn, tt);

    memset(tt, 0, sizeof(tt) / sizeof(tt[0]));
    memset(first, 0, sizeof(first) / sizeof(first[0]));
    memset(second, 0, sizeof(second) / sizeof(second[0]));
  }
}

void createEnvironment(TAOS *conn, int32_t count, int32_t totalCnt, int32_t pointsPerTable, int64_t timeDelta) {
  sleep(1);
  TAOS_RES* pSql = taos_query(conn,
                    "create table if not exists m2(ts timestamp, k int, h binary(400), t bigint,"
                    "s float, f double, x smallint, y tinyint, z bool) tags(b binary(20), a int, c bigint)");

  if (taos_errno(pSql) != 0) {
    printf("Failed to create super table, reason:%s\n", taos_errstr(conn));
    exit(-1);
  }

  char tt[1024] = {0};
  for (int32_t i = 0; i < count; ++i) {
    sprintf(tt, "create table t2m%d using m2 tags('tm%d', %d, %d)", i, i, i % 20, i);
    pSql = taos_query(conn, tt);
    if (taos_errno(pSql) != 0) {
      printf("%s\n", taos_errstr(conn));
    }

    taos_free_result(pSql);
  }

  for (int32_t i = 0; i < totalCnt; ++i) {
    insertSampleData(conn, pointsPerTable, i, timeDelta);
  }
}

int32_t executeSQL(TAOS *conn, char *sql, ResultInfo *pRes) {
  TAOS_RES* pSql = taos_query(conn, sql);
  if (taos_errno(pSql) != 0) {
    printf("failed to execute %s, reason:%s\n", sql, taos_errstr(pSql));
    taos_free_result(pSql);
    return -1;
  }

  char        temp[4096 + 512 + 1] = {0};
  int         num_fields = taos_field_count(pSql);
  TAOS_FIELD *fields = taos_fetch_fields(pSql);

  displayData(pSql, num_fields, fields, temp, pRes);
  taos_free_result(pSql);
  return 0;
}

void printRow(char *temp, int32_t num_fields, TAOS_FIELD *fields, TAOS_ROW row) {
  for (int i = 0; i < num_fields; i++) {
    size_t len = fields[i].bytes;
    switch (fields[i].type) {
      case TSDB_DATA_TYPE_TINYINT:
        sprintf(temp + strlen(temp), "%d ", *((char *)row[i]));
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        sprintf(temp + strlen(temp), "%d ", *((short *)row[i]));
        break;
      case TSDB_DATA_TYPE_INT:
        sprintf(temp + strlen(temp), "%d ", *((int *)row[i]));
        break;
      case TSDB_DATA_TYPE_BIGINT:
        sprintf(temp + strlen(temp), "%ld ", *((long *)row[i]));
        break;
      case TSDB_DATA_TYPE_FLOAT:
        sprintf(temp + strlen(temp), "%f ", *((float *)row[i]));
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        sprintf(temp + strlen(temp), "%lf ", *((double *)row[i]));
        break;
      case TSDB_DATA_TYPE_BINARY:
        snprintf(temp + strlen(temp), len, "%s ", (char *)row[i]);
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
        sprintf(temp + strlen(temp), "%ld ", *((long *)row[i]));
        break;
      default:
        break;
    }
  }
}

//void validateData(TAOS_FIELD *fields, int32_t num_fields, TAOS_ROW row, ResultInfo *pRes) {
//  for (int32_t i = 0; i < num_fields; ++i) {
//    tVariant *pVal = &pRes->pVal[i];
//    switch (fields[i].type) {
//      case TSDB_DATA_TYPE_INT:
//        assert(pVal->i64 == *(int *)row[i]);
//        break;
//      case TSDB_DATA_TYPE_BIGINT:
//      case TSDB_DATA_TYPE_TIMESTAMP:
//        assert(pVal->i64 == *(int64_t *)row[i]);
//        break;
//      case TSDB_DATA_TYPE_FLOAT:
//        assert(fabs(pVal->dKey - (*(float *)row[i])) < 0.001);
//        break;
//      case TSDB_DATA_TYPE_DOUBLE:
//        assert(fabs(pVal->dKey - (*(double *)row[i])) < 0.001);
//        break;
//      case TSDB_DATA_TYPE_TINYINT:
//        assert(pVal->i64 == *(int8_t *)row[i]);
//        break;
//      case TSDB_DATA_TYPE_SMALLINT:
//        assert(pVal->i64 == *(int16_t *)row[i]);
//        break;
//      case TSDB_DATA_TYPE_BOOL:
//      case TSDB_DATA_TYPE_BINARY:
//        printf("ignore\n");
//        break;
//      default:
//        assert(0);
//    }
//  }
//}

//void setResultInfo(ResultInfo *pRes, int32_t col, int32_t row) {
//  if (pRes == NULL) return;
//
//  pRes->numOfRows = row;
//  pRes->numOfCols = col;
//  pRes->pVal = realloc(pRes->pVal, pRes->numOfCols * pRes->numOfRows * sizeof(tVariant));
//}

void displayData(void *result, int32_t num_fields, TAOS_FIELD *fields, char *temp, ResultInfo *pRes) {
  TAOS_ROW row = NULL;
  int64_t  numOfRows = 0;

  struct timeval st, et;
  gettimeofday(&st, NULL);
  int64_t stt = st.tv_sec*1000*1000 + st.tv_usec;

  char    field[4024] = {0};
  int32_t c = 0;
  for (int32_t i = 0; i < num_fields; ++i) {
    int32_t length = sprintf(field + c, "%s, ", fields[i].name);
    c += length;
  }

  printf("%s\n", field);

  while ((row = taos_fetch_row(result))) {
    temp[0] = 0;
    numOfRows++;

    taos_print_row(temp, row, fields, num_fields);
    printf("%" PRId64 ": %s\n", numOfRows, temp);
  }

  gettimeofday(&et, NULL);

  int64_t ett = et.tv_sec*1000*1000 + et.tv_usec;
  printf("total elapsed time:%"PRId64" ms, %"PRId64" rows\n", (ett - stt)/1000, numOfRows);
}

TAOS *connectdb() {
  taos_init();

  TAOS *conn = taos_connect("ubuntu", "root", "taosdata", 0, 0);
  if (conn == NULL) {
    printf("Failed to connect to DB, reason:%s\n", taos_errstr(conn));
    exit(-1);
  }

  return conn;
}

void sqlParseTestImpl(TAOS *conn, char *sql, bool boolFlag) {
  printf("\nexecution sql: \n%s\n", sql);

  TAOS_RES* pSql = taos_query(conn, sql);

  if (boolFlag) {
    assert(taos_errno(pSql) == 0);
  } else {
    assert(taos_errno(pSql) != 0);
    printf("%s\n", taos_errstr(conn));
  }

  taos_free_result(pSql);
  printf("parse sql test done.\n");
}
