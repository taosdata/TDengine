#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <stdint.h>

#include "taos.h"
#include "tmsg.h"
#include "tutil.h"
#include "testCommon.h"

static void SQLParseTest(TAOS *conn, bool insertData);

static void errorCaseRegressionTest(TAOS *conn, bool insertData);

static int32_t queryTest(TAOS *conn, bool insertData);

static void metricTest(TAOS *conn, bool insertData);

static void largeMetricQuery(TAOS *conn, bool insertData);

static void repeatQueryTest(TAOS *conn, bool insertData);
static void manyRowsInsertTest(TAOS* conn);
static void twoVnodesInsertTest(TAOS* conn);
static void manyVnodesInsertTest(TAOS* conn);
void largeInsertDataPacketTest(TAOS* conn);

static void leastsquareTest() {
    //    double x[10] = {1433955667000, 1433955667001, 1433955667002},
//            y[10] = {10006000, 10006001, 10006002};
//    double x[10] = {1433955667000,1433955667001,1433955667002},
//            y[10] = {1000000,1000001,1000002};
//    double pp[2][3];
//    tInitMatrix(x, y, 3, pp);
//    tCompute(pp);
}

typedef struct {
    int32_t id;
    int32_t nThreads;
    int32_t* finishedThreads;
} multiThreadsParams;

int32_t rid = 0;

void fetchCallBack(void *param, TAOS_RES *tres, int numOfRows) {
    char buf[512] = {0};

    if (numOfRows > 0) {
        printf("fetch data.\n");
        int32_t num_fields = taos_num_fields(tres);
        TAOS_FIELD *pField = taos_fetch_fields(tres);

        for (int i = 0; i < numOfRows; ++i) {
            TAOS_ROW row = taos_fetch_row(tres);
            taos_print_row(buf, row, pField, num_fields);
            printf("%d:%s\n", rid++, buf);
            buf[0] = 0;
        }

        // retrieve next batch of rows
        taos_fetch_rows_a(tres, fetchCallBack, param);
    } else if (numOfRows == 0) {
        printf("%d--------------------all data has been fetched to client.\n",
               ((multiThreadsParams *) param)->id);
        taos_free_result(tres);
    } else {
        printf("fetch data from server failed, code:%d\n", numOfRows);
    }
}

void queryCallBack(void *param, TAOS_RES *tres, int code) {
    printf("start to fetch data\n");
    taos_fetch_rows_a(tres, fetchCallBack, param);
}

void streamCallBack(void *param, TAOS_RES *res, TAOS_ROW row) {
    int32_t fieldCount = taos_field_count(res);
    printf("-----------------------------field count is:%d\n", fieldCount);
    TAOS_FIELD *pFields = taos_fetch_fields(res);
    displayData(res, fieldCount, pFields);
}

void queryImpl(void *param) {
    multiThreadsParams *info = (multiThreadsParams *) param;
//    executeSQL(conn, "select count(*) from tm0");
    TAOS *conn = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
    int32_t count = 0;
    int32_t maxCount = 100000;
    while (count < maxCount) {
//        executeSQL(conn, "insert into ")
    }
//    executeSQL(conn, "select count(*) from m1", queryCallBack, param);
    printf("---------------------\n\n\n");
}

void insertImpl(void *param) {
    TAOS *conn = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
    int32_t ret = taos_query(conn, "create database testins");
    if (ret != 0) {
        printf("Failed to create db, reason:%s\n", taos_errstr(conn));
    }

    ret = taos_query(conn, "use testins");
    if (ret != 0) {
        printf("Failed to use db, reason:%s\n", taos_errstr(conn));
    }

    int32_t count = 100000;

    ret = taos_query(conn, "create table m1(ts timestamp, k int, h binary(20), t bigint,"
            "s float, f double, x smallint, y tinyint, z bool) tags(a int, b binary(20), c bigint)");

    multiThreadsParams* pParam = (multiThreadsParams*) param;
    uint16_t id = pParam->id;


    char tt[1024] = {0};
    sprintf(tt, "drop table tm%d", id);
    taos_query(conn, tt);

    sprintf(tt, "create table tm%d using m1 tags(%d, 'tm%lld', %lld)", id, id*2, id % 20, id);
    printf("%s\n", tt);
    ret = taos_query(conn, tt);
    if (ret != 0) {
        printf("Failed to create table, reason:%s\n", taos_errstr(conn));
    }

    uint64_t startTS = 1433955000000;
    multiThreadsParams* pParams = (multiThreadsParams*) param;

    for (int32_t i = 0; i < count; ++i) {
        char tt[1024] = {0};
        sprintf(tt, "insert into tm%d values (%ld, %d, '%d_%d_123', %d, %f, %f, %d, %d, %d)", id, startTS + i,
                i, i, i, i + 10000000, i * 1.023, ((double) i) / 12 + 1, i % 30000, i % 128, i % 2);

        ret = taos_query(conn, tt);
        if (ret != TSDB_CODE_SUCCESS) {
            printf("failed to insert data:%s", taos_errstr(conn));
        }
    }

    taos_close(conn);

    if (__sync_add_and_fetch(pParams->finishedThreads, 1) == pParams->nThreads) {
        printf("==============finished all threads!=============\n");
    }
}

void multiThreadInsert(int32_t numOfThreads) {
    pthread_attr_t thattr;
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

    assert(numOfThreads <= 50);

    pthread_t threadId[50];
    multiThreadsParams params[50] = {0};
    int32_t totalFinishedThreads = 0;

    for (int i = 0; i < numOfThreads; ++i) {
        params[i].id = i;
        params[i].nThreads = numOfThreads;
        params[i].finishedThreads = &totalFinishedThreads;
        pthread_create(&threadId[i], NULL, insertImpl, &params[i]);
    }

    for (int32_t i = 0; i < numOfThreads; ++i) {
        pthread_join(threadId[i], NULL);
    }

    pthread_attr_destroy(&thattr);
}

bool insertData = true;

int32_t main(int argc, char **argv) {
    if (argc < 3) {
        _error:
        printf("usage: sqltest cfg [noinsert|insert]\n");
        return -1;
    }

    strcpy(configDir, argv[1]);
    if (strcmp(argv[2], "noinsert") == 0) {
        insertData = false;
    } else if (strcmp(argv[2], "insert") == 0) {
        insertData = true;
    } else {
        goto _error;
    }

    taos_init();

    TAOS *conn = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
    if (conn == NULL) {
        printf("Failed to connect to DB, reason:%s\n", taos_errstr(conn));
        exit(-1);
    }

    taos_query(conn, "use test");

    SQLParseTest(conn, insertData);
    errorCaseRegressionTest(conn, insertData);
    queryTest(conn, insertData);
    metricTest(conn, insertData);
    repeatQueryTest(conn, insertData);
////
    multiThreadInsert(50);
//
    largeInsertDataPacketTest(conn);
    manyRowsInsertTest(conn);
    twoVnodesInsertTest(conn);
    manyVnodesInsertTest(conn);
    taos_close(conn);

    // filter on tags

    //arithmetic expression

    //filter operation

    //interpolation test
}

static int32_t queryTest(TAOS *conn, bool insertData) {
    printf("\n\nrunning %s\n", __FUNCTION__);
    printf("the following SQL should be proceeded successfully!\n==========================================\n");

    int32_t totalCount = 100000;
    executeSQL(conn, "drop database if exists querytest", NULL);
    executeSQL(conn, "create database if not exists querytest", NULL);
    executeSQL(conn, "use querytest", NULL);

    if (insertData) {
        createEnvironment(conn, 1, 1, totalCount,1);
    }

    //1.1 describe test
    NO_VALID_SUCCESS_SQL(conn, "describe tm0");

    ResultInfo res = {0};
    ResultInfo* pRes = &res;

    setResultInfo(pRes, 14, 1);

    SET_RES_VAL(res, 0, TSDB_DATA_TYPE_BIGINT, 100000);
    SET_RES_VAL(res, 1, TSDB_DATA_TYPE_INT, 0);
    SET_RES_VAL(res, 2, TSDB_DATA_TYPE_INT, 99999);
    SET_RES_VAL(res, 3, TSDB_DATA_TYPE_FLOAT, 0*1.023);
    SET_RES_VAL(res, 4, TSDB_DATA_TYPE_FLOAT, 99999*1.023);//S = delta*1.023
    SET_RES_VAL(res, 5, TSDB_DATA_TYPE_FLOAT, 15.00);
    SET_RES_VAL(res, 6, TSDB_DATA_TYPE_FLOAT, 4167.625);  //f = delta/12 + 1
    SET_RES_VAL(res, 7, TSDB_DATA_TYPE_FLOAT, 416762500);
    SET_RES_VAL(res, 8, TSDB_DATA_TYPE_BIGINT, 1433955661000);    // ts
    SET_RES_VAL(res, 9, TSDB_DATA_TYPE_DOUBLE, 2405.6261215);    // f = delta/12+1
    SET_RES_VAL(res, 10, TSDB_DATA_TYPE_DOUBLE, 28867.5134624);    // k
    SET_RES_VAL(res, 11, TSDB_DATA_TYPE_BINARY, 0);    // leastsquares
    SET_RES_VAL(res, 12, TSDB_DATA_TYPE_BINARY, 0);    // leastsquares
    SET_RES_VAL(res, 13, TSDB_DATA_TYPE_BIGINT, 1433955661000);    // ts

    //1.2 single output
    SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), percentile(y,12),"
            "avg(f), sum(f), first(ts), stddev(f), stddev(k), leastsquares(t,1,1), leastsquares(x,1,1), first(ts) "
            "from tm0", pRes);

    //2. query with where clause
    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), percentile(y,12),percentile(f, 100),"
            "avg(f), sum(f), first(ts), stddev(f), stddev(k), leastsquares(t,1,1), leastsquares(x,1,1),first(ts) "
            "from tm0 where ts>='2015-6-11 1:1:7' and ts<='2015-6-11 1:1:8.999'");

    NO_VALID_SUCCESS_SQL(conn, "select top(k, 20) from tm0 where ts>='2015-6-11 1:1:7' and ts<='2015-6-11 1:1:8.999'");
    NO_VALID_SUCCESS_SQL(conn, "select bottom(k, 20) from tm0 where ts>='2015-6-11 1:1:7' and ts<='2015-6-11 1:1:8.999'");

    //3. whole output
//    NO_VALID_SUCCESS_SQL(conn, "select k+1.23, k+1.33, k+1.43 from tm0");
//    NO_VALID_SUCCESS_SQL(conn, "select * from tm0 ");
//    NO_VALID_SUCCESS_SQL(conn, "select * from tm0 order by ts asc");
//    NO_VALID_SUCCESS_SQL(conn, "select * from tm0 order by ts desc");

    //5. arithmetic query
 //   NO_VALID_SUCCESS_SQL(conn, "select s+12, k/1.1, f+8, t*1.732 from tm0 order by ts desc");

    //6. query with where & interval
    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), percentile(y,12),percentile(f, 100),"
            "avg(f), sum(f), first(ts), stddev(f), stddev(k), leastsquares(t,1,1), leastsquares(x,1,1),first(ts) "
            "from tm0 where ts>='2015-6-11 1:1:7' and ts<='2015-6-11 1:1:8.999' interval(100a)");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), percentile(y,12),percentile(f, 100),"
            "avg(f), sum(f), first(ts), stddev(f), stddev(k), leastsquares(t,1,1), leastsquares(x,1,1),first(ts) "
            "from tm0 where ts>='2015-6-11 1:1:7' and ts<='2015-6-11 1:1:8.999' interval(10a)");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), percentile(y,12),percentile(f, 100),"
            "avg(f), sum(f), first(ts), stddev(f), stddev(k), leastsquares(t,1,1), leastsquares(x,1,1),first(ts) "
            "from tm0 where ts>='2015-6-11 1:1:7' and ts<='2015-6-11 1:1:8.999' interval(9n)");

    //7.  as test
    NO_VALID_SUCCESS_SQL(conn, "select count(*) as ff, first(k) as kk"
            " from tm0 where ts>='2015-6-11 1:1:7' and ts<='2015-6-11 1:1:8.999' interval(100a)");

    //8. query on metric
    //8.1 aggregate on whole metric
    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), "
            "avg(f), sum(f), first(ts), first(ts) from m1");

    //8.2 query with group by tags
    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), "
            "avg(f), sum(f), first(ts), first(ts) from m1 group by a");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), "
            "avg(f), sum(f), first(ts), first(ts) from m1 group by b");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), "
            "avg(f), sum(f), first(ts), first(ts) from m1 group by c");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), "
            "avg(f), sum(f), first(ts), first(ts) from m1 group by a,c");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), "
            "avg(f), sum(f), first(ts), first(ts) from m1 group by a,b");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), "
            "avg(f), sum(f), first(ts), first(ts) from m1 group by c,b");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), "
            "avg(f), sum(f), first(ts), first(ts) from m1 group by c,b,a");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), "
            "avg(f), sum(f), first(ts), first(ts) from m1 group by a,b,c");

    //8.2 query condition on tags
    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), avg(f), sum(f), first(ts), first(ts) "
            "from m1 where a=0 group by a,b,c");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), avg(f), sum(f), first(ts), first(ts) "
            "from m1 where a=0 group by a");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), avg(f), sum(f), first(ts) "
            "from m1 where a=0 group by b");

    //NULL display for metric query
    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), avg(f), sum(f), first(ts), first(ts) "
            "from m1 where a<>0 group by a,b,c");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s), "
            "avg(f), sum(f), first(ts) from m1 where a<=0 group by b,c");

    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s),"
            "avg(f), sum(f), first(ts), first(ts) "
            "from m1 where a>=0 group by c");

    //8.3 random order on group by clause
    NO_VALID_SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(s), max(s),"
            "avg(f), sum(f), first(ts) from m1 where a='0' group by b,c,a");

    printf("\n\nrunning %s completed\n===========================================\n", __FUNCTION__);
    return 0;
}

/*
 * create table m1(
 * ts timestamp, k int, h binary(20), t bigint,
 * s float, f double, x smallint, y tinyint, z bool
 * ) tags(a int, b binary(20), c bigint)
 *
 */
static void SQLParseTest(TAOS *conn, bool insertData) {
    taos_query(conn, "drop database parsetest");
    taos_query(conn, "create database parsetest");
    taos_query(conn, "use parsetest");

    if (insertData) {
        createEnvironment(conn, 1, 1, 40, 1);
        taos_query(conn, "create table m2 (ts timestamp, k int) tags(a int, b binary(12));");
        taos_query(conn, "create table m2tm0 using m2 tags(0, 'kfk');");
    }

    //[1] on disk data query process
//    SQL_PARSE_CMD_SUCCESS("select count(*),count(* ), count (*), count( * ),count ( * ) from tm0");

    SQL_PARSE_CMD_SUCCESS("select count(*), max(s), max(s), max(f), min(y), min(k), avg(k), sum(y), count(f),"
                                    "stddev(x),first(ts),last(ts),first(x),first(z),last(h),leastsquares(k, 1, 1),leastsquares(x,1,1),"
                                    "percentile(y,12),percentile(y,100) "
                                    "from tm0 ");

    SQL_PARSE_CMD_SUCCESS("select count(*), max(s), max(s), max(f), min(y), min(k), avg(k), sum(y), count(f),"
                                    "stddev(x),first(ts),last(ts),first(x),first(z),last(h),leastsquares(k,1,1),leastsquares(x,1,1),"
                                    "percentile(y,12),percentile(y,100) "
                                    "from tm0 "
                                    "where ts>'2015-6-11 1:1:1.001' and ts<'2015-6-11 1:1:4.999' ");

    SQL_PARSE_CMD_SUCCESS("select count(*), max(s), max(s), max(f), min(y), min(k), avg(k), sum(y), count(f),"
                                    "stddev(x),first(ts),last(ts),first(x),first(z),last(h),leastsquares(k,1,1),leastsquares(x,1,1),"
                                    "percentile(y,12),percentile(y,100) "
                                    "from tm0 "
                                    "where ts>'2015-6-11 1:1:1.001' and ts<'2015-6-11 1:1:4.999' "
                                    "interval(100a)");
    //query on tags conditions
    SQL_PARSE_CMD_SUCCESS("select count(*) from m1 where a=1 and b='tm0' and c<9 and ts>'2015-6-11 1:1:1.001' and "
                            "ts<'2015-6-11 1:1:4.999' ");

    //manipulate the tags
    SQL_PARSE_CMD_SUCCESS("alter table m2 add tag tagcol2 binary(23)");
    SQL_PARSE_CMD_SUCCESS("alter table m2 add tag tagcol3 binary(23)");
    taos_query(conn, "reset query cache");

    SQL_PARSE_CMD_SUCCESS("alter table m2 change tag tagcol2 tag11");
    taos_query(conn, "reset query cache");

    SQL_PARSE_CMD_SUCCESS("alter table m2tm0 set tag tag11='test tstring'");
    taos_query(conn, "reset query cache");

    SQL_PARSE_CMD_SUCCESS("alter table m2 drop tag tag11");

    SQL_PARSE_CMD_SUCCESS("show parsetest.tables");
    SQL_PARSE_CMD_SUCCESS("show parsetest.tables like 'tm%'");
    SQL_PARSE_CMD_SUCCESS("show parsetest.vgroups");
    SQL_PARSE_CMD_SUCCESS("show parsetest.stables");

    SQL_PARSE_CMD_FAILED("select top(k, 1), top(k,20), top(k,11), bottom(t, 1), bottom(y,20) from tm0");

    SQL_PARSE_CMD_SUCCESS("select k+1.23, k+1.33, k+1.43,y+1, x/5, s*3, k*0.001 from tm0");

//    ========================================================================

    SQL_PARSE_CMD_SUCCESS("select count(*), first(k), last(k) from tm0 where ts>=16542317 and ts"
                                    "<=987654321 interval(10n)");

    SQL_PARSE_CMD_SUCCESS("select count(*), first(k), last(k) from tm0 where ts>=16542317 and ts"
                                    "<=987654321 interval(10a)");

    SQL_PARSE_CMD_SUCCESS("select count(*), first(k), last(k) from tm0 where ts>=16542317 and ts"
                                    "<=987654321 interval(100s)");

    SQL_PARSE_CMD_SUCCESS("select count(*), first(k), last(k) from tm0 where ts>=16542317 and ts"
                                    "<=987654321 interval(100m)");

    SQL_PARSE_CMD_SUCCESS("select count(*), first(k), last(k) from tm0 where ts>=16542317 and ts"
                                    "<=987654321 interval(100n)");

    SQL_PARSE_CMD_SUCCESS("select count(*), first(k), last(k) from tm0 where ts>=16542317 and ts"
                                    "<=987654321 interval(100h)");

    SQL_PARSE_CMD_SUCCESS("select count(*), first(k), last(k) from tm0 where ts>=16542317 and ts"
                                    "<=987654321 interval(100d)");

    SQL_PARSE_CMD_SUCCESS("select count(*), first(k), last(k) from tm0 where ts>=16542317 and ts"
                                    "<=987654321 interval(100w)");

    SQL_PARSE_CMD_SUCCESS("select count(*), first(k), last(k) from tm0 where ts>=16542317 and ts"
                                    "<=987654321 interval(100n)");

    SQL_PARSE_CMD_SUCCESS("select count(*), first(k), last(k) from tm0 where ts>=16542317 and ts"
                                    "<=987654321 interval(100y)");

    SQL_PARSE_CMD_SUCCESS("select * from tm0 where ts>=16542317 and ts<=987654321 order by ts asc");

    SQL_PARSE_CMD_SUCCESS("select * from tm0 where ts>=16542317 and ts<=987654321 order by ts desc");

    //2. metric query
    SQL_PARSE_CMD_SUCCESS("select * from m1 where a=1");
    SQL_PARSE_CMD_SUCCESS("select a,b,c from m1");

    SQL_PARSE_CMD_SUCCESS("select k,a,b,c from m1");
    SQL_PARSE_CMD_SUCCESS(
            "select count(*),sum(k),first(y),sum(f),min(x),max(x),last(h) from m1 where a<>1 interval(10a) group by a,b,c");

    SQL_PARSE_CMD_SUCCESS("select b,c from m1 where b=1");

    //3. stream quer
    taos_query(conn, "drop table test_stream");
    SQL_PARSE_CMD_SUCCESS(
            "create table test_stream as select count(*) as ct, avg(k) as vg, sum(f) as ss, first(h) as ff from m1 interval(30s)");
    taos_query(conn, "drop table test_stream");
}

/**
 * error case regression test!!!
 * @param conn
 */
void errorCaseRegressionTest(TAOS *conn, bool insertData) {
    printf("start %s\n", __FUNCTION__);
    int32_t ret = taos_query(conn, "drop database regre");
    ret = taos_query(conn, "create database if not exists regre");

    if (ret != 0) {
        printf("error:%s\n", taos_errstr(conn));
        return;
    }

    taos_query(conn, "use regre");
    if (insertData) {
        createEnvironment(conn, 1, 1, 100000, 1);
    }

    ResultInfo res = {0};
    ResultInfo* pRes = &res;

    setResultInfo(pRes, 9, 1);

    SET_RES_VAL(res, 0, TSDB_DATA_TYPE_BIGINT, 100000);
    SET_RES_VAL(res, 1, TSDB_DATA_TYPE_INT, 0);
    SET_RES_VAL(res, 2, TSDB_DATA_TYPE_INT, 99999);
    SET_RES_VAL(res, 3, TSDB_DATA_TYPE_FLOAT, 1.0);
    SET_RES_VAL(res, 4, TSDB_DATA_TYPE_FLOAT, 8334.25);
    SET_RES_VAL(res, 5, TSDB_DATA_TYPE_BIGINT, 6348464);
    SET_RES_VAL(res, 6, TSDB_DATA_TYPE_FLOAT, 63.48464);
    SET_RES_VAL(res, 7, TSDB_DATA_TYPE_FLOAT, 49999.5);
    SET_RES_VAL(res, 8, TSDB_DATA_TYPE_BIGINT, 0);    // tag value

    SUCCESS_SQL(conn, "select count(*), first(k), last(k), min(f), max(f), sum(y), avg(y), avg(k) "
                      " from m1 group by c", pRes);

    setResultInfo(pRes, 6, 1);

    SET_RES_VAL(res, 0, TSDB_DATA_TYPE_BIGINT, 100000);
    SET_RES_VAL(res, 1, TSDB_DATA_TYPE_DOUBLE, 49999.5);
    SET_RES_VAL(res, 2, TSDB_DATA_TYPE_BIGINT, 6348464);
    SET_RES_VAL(res, 3, TSDB_DATA_TYPE_BIGINT, 100000);
    SET_RES_VAL(res, 4, TSDB_DATA_TYPE_DOUBLE, 63.48464);
    SET_RES_VAL(res, 5, TSDB_DATA_TYPE_INT, 0);

    SUCCESS_SQL(conn, "select count(*), avg(k), sum(y), count(y), avg(y) from m1 group by a", pRes);
/*
    setResultInfo(pRes, 1, 9);
    for(int32_t i=0; i<res.numOfRows; ++i) {
        SET_RES_VAL(res, i, TSDB_DATA_TYPE_BIGINT, 1);
    }

    SUCCESS_SQL(conn, "select diff(k) from tm0 where ts>='2015-6-11 1:1:1.120' and ts<='2015-6-11 1:1:1.129'", pRes);
*/
    setResultInfo(pRes, 8, 1);
    SET_RES_VAL(res, 0, TSDB_DATA_TYPE_DOUBLE, 28867.513458);
    SET_RES_VAL(res, 1, TSDB_DATA_TYPE_DOUBLE, 0);
    SET_RES_VAL(res, 2, TSDB_DATA_TYPE_DOUBLE, 49999.5);
    SET_RES_VAL(res, 3, TSDB_DATA_TYPE_DOUBLE, 99999);
    SET_RES_VAL(res, 4, TSDB_DATA_TYPE_DOUBLE, 24999.75);
    SET_RES_VAL(res, 5, TSDB_DATA_TYPE_DOUBLE, 48999.51);
    SET_RES_VAL(res, 6, TSDB_DATA_TYPE_DOUBLE, 78999.21);
    SET_RES_VAL(res, 7, TSDB_DATA_TYPE_DOUBLE, 66999.33);

    SUCCESS_SQL(conn, "select stddev(k), percentile(k, 0), percentile(k, 50), percentile(k, 100),"
                      " percentile(k, 25), percentile(k, 49), percentile(k, 79), percentile(k, 67) from tm0", pRes);
 }

void metricTest(TAOS *conn, bool insertData) {
    //prepare data
    executeSQL(conn, "drop database if exists metrictes", NULL);
    executeSQL(conn, "create database if not exists metrictes", NULL);
    executeSQL(conn, "use metrictes", NULL);

    taos_query(conn, "create table m1 (ts timestamp, speed int, temp float) tags (model binary(10), type int)");
    taos_query(conn, "create table t1 using m1 tags ('ibm', 1)");
    taos_query(conn, "create table t2 using m1 tags ('ibm', 2)");
    taos_query(conn, "create table t3 using m1 tags ('apple', 3)");
    taos_query(conn, "create table t4 using m1 tags ('apple', 4)");

    taos_query(conn, "insert into t1 values (now-4d, 10, 1.0)");
    taos_query(conn, "insert into t1 values (now-3d, 20, 2.0)");
    taos_query(conn, "insert into t1 values (now-2d, 30, 3.0)");
    taos_query(conn, "insert into t1 values (now-1d, 40, 4.0)");
    taos_query(conn, "insert into t1 values (now, 50, 5.0)");

    taos_query(conn, "insert into t2 values (now-4d, 10, 1.0)");
    taos_query(conn, "insert into t2 values (now-3d, 20, 2.0)");
    taos_query(conn, "insert into t2 values (now-2d, 30, 3.0)");
    taos_query(conn, "insert into t2 values (now-1d, 40, 4.0)");
    taos_query(conn, "insert into t2 values (now, 50, 5.0)");

    taos_query(conn, "insert into t3 values (now-4d, 10, 1.0)");
    taos_query(conn, "insert into t3 values (now-3d, 20, 2.0)");
    taos_query(conn, "insert into t3 values (now-2d, 30, 3.0)");
    taos_query(conn, "insert into t3 values (now-1d, 40, 4.0)");
    taos_query(conn, "insert into t3 values (now, 50, 5.0)");

    taos_query(conn, "insert into t4 values (now-4d, 10, 1.0)");
    taos_query(conn, "insert into t4 values (now-3d, 20, 2.0)");
    taos_query(conn, "insert into t4 values (now-2d, 30, 3.0)");
    taos_query(conn, "insert into t4 values (now-1d, 40, 4.0)");
    taos_query(conn, "insert into t4 values (now, 50, 5.0)");

    taos_query(conn, "insert into t1 values (now+3m, 30, 5.0)");
    taos_query(conn, "insert into t1 values (now+4m, 40, 1.0)");
    taos_query(conn, "insert into t1 values (now+5m, 50, 2.0)");
    taos_query(conn, "insert into t1 values (now+6m, 60, 3.0)");

    taos_query(conn, "insert into t4 values (now+20m, 30, 5.0)");
    taos_query(conn, "insert into t4 values (now+21m, 40, 1.0)");
    taos_query(conn, "insert into t4 values (now+23m, 50, 2.0)");
    taos_query(conn, "insert into t4 values (now+24m, 60, 3.0)");

    SUCCESS_SQL(conn, "select count(*), avg(speed),sum(speed) from m1 interval(1d)", NULL);
    SUCCESS_SQL(conn, "select count(*), avg(speed),sum(speed) from m1 interval(1m)", NULL);
}

void largeMetricQuery(TAOS *conn, bool insertData) {
    printf("=====================================\nrepeat sync query\n");
    taos_query(conn, "drop database test");
    taos_query(conn, "create database test");

    taos_query(conn, "use test");
    if (insertData) {
        createEnvironment(conn, 2000, 500, 500, 1 );
    }

    for (int32_t i = 0; i < 200000; ++i) {
        printf("syn(%d)-------------------------\n", i);
        executeSQL(conn, "select count(*),first(h) from m1", NULL);
        sleep(1);
    }
    printf("query completed\n");
}

/*
 * ts timestamp, k int, h binary(20), t bigint,
 * s float, f double, x smallint, y tinyint, z bool
 */
void repeatQueryTest(TAOS *conn, bool insertData) {
    printf("=====================================\nrepeat sync query\n");
    taos_query(conn, "drop database if exists lgmetric");
    taos_query(conn, "create database lgmetric");
    taos_query(conn, "use lgmetric");

    if (insertData) {
        createEnvironment(conn, 1, 1, 10000, 1);
    }

    int32_t runCount = 1000;

    for (int32_t i = 0; i < runCount; ++i) {
        printf("syn(%d)-------------------------\n", i);
        executeSQL(conn, "select count(*), sum(k), avg(f), first(h), last(h), min(t),"
                "max(s) as sss, stddev(k), percentile(f, 25), leastsquares(k,1,1) "
                " from tm0 interval(10a)", NULL);
        executeSQL(conn, "select top(k, 15) from tm0;", NULL);
    }

    for (int32_t i = 0; i < runCount; ++i) {
        printf("syn(%d)-------------------------\n", i);
        executeSQL(conn, "select count(*), sum(k), avg(f), first(h), last(h), min(t),"
                "max(s) as sss, stddev(k), percentile(f, 25), leastsquares(k,1,1) "
                " from tm0 where ts>'2015-1-1 1:1:1'", NULL);
    }

    for (int32_t i = 0; i < runCount; ++i) {
        printf("syn(%d)-------------------------\n", i);
        executeSQL(conn, "select count(ts),sum(k),avg(f),first(h),last(x),avg(y),min(t),max(s) from m1"
                " where a=0 and ts>'2015-6-11 1:1:9.120' interval(10a)", NULL);
        executeSQL(conn, "select count(ts),first(x),last(x),last(ts),avg(y) from m1"
                " where a=0 and ts>='2015-6-11 1:1:9.120' interval(10a)", NULL);
    }

    for (int32_t i = 0; i < runCount; ++i) {
        printf("syn(%d)-------------------------\n", i);
        executeSQL(conn, "select count(ts),sum(k),avg(f),first(h),last(x),avg(y),min(t),max(s) from m1"
                " where a=0 and ts>'2015-6-11 1:1:1.999' interval(100a) group by b,c", NULL);
    }

    for (int32_t i = 0; i < runCount; ++i) {
        printf("syn(%d)-------------------------\n", i);
        executeSQL(conn, "select count(ts),sum(k),avg(f),first(h),last(x),avg(y),min(t),max(s) from m1", NULL);
    }

    for (int32_t i = 0; i < runCount; ++i) {
        printf("syn(%d)-------------------------\n", i);
        executeSQL(conn, "select count(ts),sum(k),avg(f),first(h),last(x),avg(y),min(t),max(s) from m1"
                " interval(100a)", NULL);
    }

    for (int32_t i = 0; i < runCount; ++i) {
        printf("syn(%d)-------------------------\n", i);
        executeSQL(conn, "select count(ts),sum(k),avg(f),first(h),last(x),avg(y),min(t),max(s) from m1"
                " interval(100a) group by a,b,c", NULL);
    }

    for (int32_t i = 0; i < runCount; ++i) {
        printf("syn(%d)-------------------------\n", i);
        executeSQL(conn, "select count(ts),sum(k),avg(f),first(h),last(x),avg(y),min(t),max(s) from m1"
                " where a=0 interval(100a) group by b,c", NULL);
    }

    for (int32_t i = 0; i < 1; ++i) {
        printf("syn(%d)-------------------------\n", i);
        executeSQL(conn, "select count(ts),sum(k),avg(f),first(h),last(x),avg(y),min(t),max(s) from m1"
                " where a=0 and ts>='2015-6-11 1:1:1.99' and ts<= '2015-6-11 1:1:2.999' interval(100a) group by b,c", NULL);
    }
    printf("query completed\n");
}

/*
 * close vnode during query may cause core dump
 * todo  bug to be fixed, not fixed yet
 */
void errorCaseTest_metric(TAOS* conn) {
    printf("\n\nrunning %s\n", __FUNCTION__);

    createEnvironment(conn, 1, 1, 1, 1);
    executeSQL(conn, "select count(*) from m1", NULL);

    executeSQL(conn, "drop table tm0", NULL);
    executeSQL(conn, "select count(*) from m1", NULL);
    executeSQL(conn, "select count(*) from m1", NULL);
}

void largeInsertDataPacketTest(TAOS* conn) {
    printf("\n\nrunning %s\n", __FUNCTION__);

    taos_query(conn, "drop database lins");
    taos_query(conn, "create database lins");
    taos_query(conn, "use lins");

    executeSQL(conn, "create table gg(ts timestamp, k int, f binary(200), f1 binary(200), f2 binary(90))", NULL);

    char val[65535] = {0};
    char one[512] = {0};
    char sec[512] = {0};

    static int32_t inc = 11;

    for (int32_t j = 0; j < 10000; ++j) {
        sprintf(one, "insert into gg(ts, k, f, f1, f2) values(now+10a, %d, '%s', '%s', '%s') ",
                j, "x", "x", "x");
        strncat(val, one, 512);

        for (int32_t i = 0; i < 125; ++i) {
            sprintf(sec, " (now+%da, %d, '%s', '%s', '%s')", inc++, j * i, "x", "x", "x");
            strncat(val, sec, 512);
        }
        int32_t ret = taos_query(conn, val);
        if (ret != 0) {
            printf("failed to execute sql:%s, reason:%s\n", val, taos_errstr(conn));
        }
        memset(val, 0, 65535);
    }
}

void manyRowsInsertTest(TAOS* conn) {
    printf("\n\nrunning %s\n", __FUNCTION__);

    executeSQL(conn, "create table ttk(ts timestamp, k int)", NULL);

    char val[1<<20] = {0}; //128kb

    char one[512] = {0};
    char sec[512] = {0};

    static int32_t inc = 0;

    int64_t startTime = 1483200000000;
    for (int32_t j = 0; j < 400; ++j) {
        int32_t len = sprintf(one, "insert into ttk values(%ld, %d)", startTime++, inc++);
        strncat(val, one, len+1);

        for (int32_t i = 0; i < (2000-1); ++i) {
            len = sprintf(sec, " (%ld, %d)", startTime++, inc++);
            strncat(val, sec, len + 1);
        }
        len = strlen(val);

        int32_t ret = taos_query(conn, val);
        if (ret != 0) {
            printf("failed:%s\n", taos_errstr(conn));
        }
        memset(val, 0, 65535);
    }

    printf("bulk insert data %d completed\n", 400*2000);
}

static void twoVnodesInsertTest(TAOS* conn) {
    printf("\n\nrunning %s\n", __FUNCTION__);

    executeSQL(conn, "drop database if exists twov", NULL);
    executeSQL(conn, "create database twov tables 1000", NULL);
    executeSQL(conn, "use twov", NULL);

    const size_t numOfTable = 7000;
    char sql[512] = {0};

    for (int32_t i = 0; i < numOfTable; ++i) {
        sprintf(sql, "drop table if exists ttf%d", i);
        executeSQL(conn, sql, NULL);
        memset(sql, 0, sizeof(sql) / sizeof(sql[0]));
    }

    sprintf(sql, "create table if not exists m1(ts timestamp, k int) tags(a int)");
    executeSQL(conn, sql, NULL);

    for (int32_t i = 0; i < numOfTable; ++i) {
        sprintf(sql, "create table if not exists ttf%d using m1 tags(%d)", i, i);
        executeSQL(conn, sql, NULL);
        memset(sql, 0, sizeof(sql) / sizeof(sql[0]));
    }

    printf("create table completed!\n");

    char val[1 << 20] = {0}; //128kb

    char one[512] = {0};
    char sec[512] = {0};

    static int32_t inc1 = 0;
    static int32_t inc2 = 0;

    int64_t startTime1 = 1483200000000;
    int64_t startTime2 = 1483200000000;

    for(int32_t j=0; j<5000; ++j) {
        int32_t len = sprintf(one, "insert into ttf0 values(%ld, %d)", startTime1++, inc1++);
        strncat(val, one, len + 1);

        for (int32_t i = 0; i < (1000 - 1); ++i) {
            len = sprintf(sec, " (%ld, %d)", startTime1++, inc1++);
            strncat(val, sec, len + 1);
        }

        len = strlen(val);

        len = sprintf(one, " ttf3000 values(%ld, %d)", startTime2++, inc2++);
        strncat(val, one, len + 1);

        for (int32_t i = 0; i < (1000 - 1); ++i) {
            len = sprintf(sec, " (%ld, %d)", startTime2++, inc2++);
            strncat(val, sec, len + 1);
        }

        int32_t ret = taos_query(conn, val);
        if (ret != 0) {
            printf("failed:%s\n", taos_errstr(conn));
        }

        memset(val, 0, 1 << 20);
    }

    printf("insert into two vnodes completed\n");
}

static void manyVnodesInsertTest(TAOS* conn) {
    printf("\n\nrunning %s\n", __FUNCTION__);

    executeSQL(conn, "drop database if exists mv", NULL);
    executeSQL(conn, "create database mv tables 150", NULL);
    executeSQL(conn, "use mv", NULL);

    const size_t numOfTable = 3000;
    char sql[512] = {0};

    for (int32_t i = 0; i < numOfTable; ++i) {
        sprintf(sql, "drop table if exists ttn%d", i);
        executeSQL(conn, sql, NULL);
        memset(sql, 0, sizeof(sql) / sizeof(sql[0]));
    }

    for (int32_t i = 0; i < numOfTable; ++i) {
        sprintf(sql, "create table ttn%d(ts timestamp, k int)", i);
        executeSQL(conn, sql, NULL);
        memset(sql, 0, sizeof(sql) / sizeof(sql[0]));
    }

    printf("create table completed!\n");

    char val[1 << 20] = {0}; //128kb

    char one[512] = {0};
    char sec[512] = {0};

    static int32_t inc1 = 0;

    int64_t startTime1 = 1483200000000L;

    for(int32_t j=0; j<100; ++j) {
        int32_t len = sprintf(one, "insert into ttn0 values(%ld, %d)", startTime1, inc1++);
        strncat(val, one, len + 1);

        for (int32_t i = 1; i < (800 - 1); ++i) {
            len = sprintf(sec, " ttn%d values(%ld, %d)", i, startTime1, inc1++);
            strncat(val, sec, len + 1);
        }

        for (int32_t i = 2000; i < (2800 - 1); ++i) {
            len = sprintf(sec, " ttn%d values(%ld, %d)", i, startTime1, inc1++);
            strncat(val, sec, len + 1);
        }

        len = strlen(val);
        int32_t ret = taos_query(conn, val);
        if (ret != 0) {
            printf("failed:%s\n", taos_errstr(conn));
        }

        memset(val, 0, 1 << 20);
        startTime1 += 1;
    }

    printf("insert into many vnodes completed\n");
}
