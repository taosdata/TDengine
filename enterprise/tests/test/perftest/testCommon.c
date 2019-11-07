#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <float.h>
#include <math.h>

#include "tutil.h"
#include "ttime.h"
#include "testCommon.h"
#include "ttime.h"

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

    char tt[1024*64] = {0};
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
        int32_t len = sprintf(first, "insert into tm%d values (%ld, %d, '%d_%d_123', %d, %f, %f, %d, %d, %d)", tableIdx, startTS + delta*timeDelta,
                delta, delta, delta, delta + 10000000, delta * 1.023, ((double) delta) / 12 + 1, delta % 30000, delta % 128, delta % 2);
        strncat(tt, first, len+1);

        delta++;

        for(int32_t j = 0; j < item-1; ++j) {
            len = sprintf(second, " (%ld, %d, '%d_%d_123', %d, %f, %f, %d, %d, %d)", startTS + delta*timeDelta,
                          delta, delta, delta, delta + 10000000, delta * 1.023, ((double) delta) / 12 + 1, delta % 30000, delta % 128, delta % 2);
            strncat(tt, second, len + 1);
            delta++;
        }


        NO_VALID_SUCCESS_SQL(conn, tt);

        memset(tt, 0, sizeof(tt) / sizeof(tt[0]));
        memset(first, 0, sizeof(first)/sizeof(first[0]));
        memset(second, 0, sizeof(second)/sizeof(second[0]));
    }
}

void createEnvironment(TAOS *conn, int32_t count, int32_t totalCnt, int32_t pointsPerTable, int64_t timeDelta) {
    taosMsleep(2000);
    taos_query(conn, "drop table m1");
    taosMsleep(1000);
    int32_t ret = taos_query(conn, "create table if not exists m1(ts timestamp, k int, h binary(400), t bigint,"
            "s float, f double, x smallint, y tinyint, z bool) tags(b binary(20), a int, c bigint)");

    if (ret != TSDB_CODE_SUCCESS) {
        printf("Failed to create metric, reason:%s\n", taos_errstr(conn));
        exit(-1);
    }

    char tt[1024] = {0};
    for (int32_t i = 0; i < count; ++i) {
        sprintf(tt, "drop table if exists tm%d", i);
        taos_query(conn, tt);

        sprintf(tt, "create table tm%d using m1 tags('tm%d', %d, %d)\0", i % 40, i, i % 20, i);
        ret = taos_query(conn, tt);
        if (ret != 0) {
            printf("%s\n", taos_errstr(conn));
        }
    }

    int64_t stime = taosGetTimestampMs();
    for (int32_t i = 0; i < totalCnt; ++i) {
        insertSampleData(conn, pointsPerTable, i, timeDelta);
    }

    int64_t etime = taosGetTimestampMs();
    printf("insert data elapsed time:%ld ms\n", etime - stime);
}

int32_t executeSQL(TAOS *conn, char *sql, ResultInfo* pRes) {
    int32_t ret = taos_query(conn, sql);
    if (ret != TSDB_CODE_SUCCESS) {
        printf("failed to execute %s, reason:%s\n", sql, taos_errstr(conn));
        return -1;
    }

    void *result = taos_use_result(conn);
    if (result == NULL) {
        printf("failed to get result, reason:%s\n", taos_errstr(conn));
        return -1;
    }

    char temp[4096+512+1] = {0};
    int num_fields = taos_field_count(conn);
    TAOS_FIELD *fields = taos_fetch_fields(result);

    displayData(result, num_fields, fields, temp, pRes);

    taos_free_result(result);
    return 0;
}

void printRow(char* temp, int32_t num_fields, TAOS_FIELD* fields, TAOS_ROW row) {
    for (int i = 0; i < num_fields; i++) {
        size_t len = fields[i].bytes;
        switch (fields[i].type) {
            case TSDB_DATA_TYPE_TINYINT:
                sprintf(temp + strlen(temp), "%d ", *((char *) row[i]));
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                sprintf(temp + strlen(temp), "%d ", *((short *) row[i]));
                break;
            case TSDB_DATA_TYPE_INT:
                sprintf(temp + strlen(temp), "%d ", *((int *) row[i]));
                break;
            case TSDB_DATA_TYPE_BIGINT:
                sprintf(temp + strlen(temp), "%ld ", *((long *) row[i]));
                break;
            case TSDB_DATA_TYPE_FLOAT:
                sprintf(temp + strlen(temp), "%f ", *((float *) row[i]));
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                sprintf(temp + strlen(temp), "%lf ", *((double *) row[i]));
                break;
            case TSDB_DATA_TYPE_BINARY:
                snprintf(temp + strlen(temp), len, "%s ", (char *) row[i]);
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
                sprintf(temp + strlen(temp), "%ld ", *((long *) row[i]));
                break;
            default:
                break;
        }
    }
}

void validateData(TAOS_FIELD* fields, int32_t num_fields, TAOS_ROW row, ResultInfo* pRes) {
    for(int32_t i=0; i<num_fields; ++i) {
        tVariant* pVal = &pRes->pVal[i];
        switch(fields[i].type) {
            case TSDB_DATA_TYPE_INT:
                assert(pVal->i64Key == *(int*)row[i]);      break;
            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_TIMESTAMP:
                assert(pVal->i64Key == *(int64_t*)row[i]);  break;
            case TSDB_DATA_TYPE_FLOAT:
                assert(fabs(pVal->dKey - (*(float*)row[i])) < 0.001);      break;
            case TSDB_DATA_TYPE_DOUBLE:
                assert(fabs(pVal->dKey - (*(double*)row[i])) < 0.001);     break;
            case TSDB_DATA_TYPE_TINYINT:
                assert(pVal->i64Key == *(int8_t*)row[i]);   break;
            case TSDB_DATA_TYPE_SMALLINT:
                assert(pVal->i64Key == *(int16_t*)row[i]);  break;
            case TSDB_DATA_TYPE_BOOL:
            case TSDB_DATA_TYPE_BINARY:
                printf("ignore\n");break;
            default:
                assert(0);
        }
    }
}

void setResultInfo(ResultInfo* pRes, int32_t col, int32_t row) {
    if (pRes == NULL) return;

    pRes->numOfRows = row;
    pRes->numOfCols = col;
    pRes->pVal = realloc(pRes->pVal, pRes->numOfCols*pRes->numOfRows*sizeof(tVariant));
}

void displayData(void* result, int32_t num_fields, TAOS_FIELD* fields, char* temp, ResultInfo* pRes) {
    TAOS_ROW row = NULL;
    int64_t numOfRows = 0;
    uint64_t start = taosGetTimestampMs();

    char field[4024] = {0};
    int32_t c = 0;
    for(int32_t i=0; i < num_fields; ++i) {
        int32_t length = sprintf(field+c, "%s, ", fields[i].name);
        c += length;
    }

    if (num_fields > 0)
        printf("%s\n", field);

    while ((row = taos_fetch_row(result))) {
        temp[0] = 0;
        numOfRows++;

        taos_print_row(temp, row, fields, num_fields);
        printf("%d: %s, \t len:%d\n", numOfRows, temp, strlen(temp));

        if (pRes != NULL)
            validateData(fields, num_fields, row, pRes);
    }
    printf("total elapsed time:%ld ms, %d rows\n", taosGetTimestampMs() - start, numOfRows);
}

TAOS* connectdb() {
    strcpy(configDir, "/etc/taos");
    taos_init();

    TAOS* conn = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, 0, 0);
    if (conn == NULL) {
        printf("Failed to connect to DB, reason:%s\n", taos_errstr(conn));
        exit(-1);
    }

    return conn;
}

void sqlParseTestImpl(TAOS *conn, char *sql, bool boolFlag) {
    printf("\nexecution sql: \n%s\n", sql);

    int32_t ret = taos_query(conn, sql);

    if (boolFlag) {
        assert(ret == TSDB_CODE_SUCCESS);
    } else {
        assert(ret != TSDB_CODE_SUCCESS);
        printf("%s\n", taos_errstr(conn));
    }

    void* res = taos_use_result(conn);
    taos_free_result(res);
    printf("parse sql test done.\n");
}

