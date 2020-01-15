#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <float.h>
#include <wordexp.h>
#include <fcntl.h>
#include <unistd.h>
#include <strings.h>
#include <time.h>
#include <locale.h>

#include "taos.h"
#include "testCommon.h"
#include "tutil.h"

static int32_t rid = 1;
void sqlfullTest(TAOS*conn);

void fetchCallBack(void *param, TAOS_RES *tres, int numOfRows) {
    char buf[512] = {0};

    if (numOfRows > 0) {
        printf("fetch data.");
        int32_t num_fields = taos_num_fields(tres);
        TAOS_FIELD *pField = taos_fetch_fields(tres);

        for (int i = 0; i < numOfRows; ++i) {
            TAOS_ROW row = taos_fetch_row(tres);
            taos_print_row(buf, row, pField, num_fields);
            int32_t k = __sync_fetch_and_add(&rid, 1);
            printf("%d:%s", k, buf);
            buf[0] = 0;
        }
//        taos_free_result(tres);
//        return;
//         retrieve next batch of rows
        taos_fetch_rows_a(tres, fetchCallBack, param);
    } else if (numOfRows == 0) {
        printf("--------------------all data has been fetched to client.\n");
        taos_free_result(tres);
    } else {
        printf("fetch data from server failed, code:%d\n", numOfRows);
//        taos_free_result(tres);
    }
}

void fetchCallBack_Single(void *param, TAOS_RES *tres, TAOS_ROW row) {
    char buf[512] = {0};
    if (row) {
        int32_t num_fields = taos_num_fields(tres);
        TAOS_FIELD *pField = taos_fetch_fields(tres);

        //printf("row:%d %lld %lld", pTable->rowsRetrieved, *((int64_t *)row[0]), *((int64_t *)row[1]));
        taos_print_row(buf, row, pField, num_fields);
        int32_t k = __sync_fetch_and_add(&rid, 1);
        printf("%d:%s", k, buf);
        buf[0] = 0;

        taos_fetch_row_a(tres, fetchCallBack_Single, param);
    } else {
        taos_free_result(tres);
        printf("all data has been fixed");
        //printf("index:%d, %d rows data retrieved", pTable->id, pTable->rowsRetrieved);
    }
}

void queryCallback(void *param, TAOS_RES *tres, int code) {
    printf("query completed, code: %d, start to fetch data\n", code);
    if (code < 0) {
        if (param != NULL)
            printf("==================query:%d, error:%d, taos_res:%p=============================",
                   *((int32_t *) param), code, tres);
        else
            printf("==================query:, error:%d, taos_res:%p=============================",
                   code, tres);
        return;
    }

    taosMsleep(2000);
    taos_fetch_rows_a(tres, fetchCallBack, param);
//    taos_fetch_row_a(tres, fetchCallBack_Single, param);
}

void insertCallBack(void *param, TAOS_RES *tres, int code) {
    if (code < 0) {
        if (param != NULL)
            printf("==================query:%d, error:%d, taos_res:%p=============================",
                   *((int32_t *) param), code, tres);
        else {
            printf("==================query:, error:%d, taos_res:%p=============================",
                   code, tres);
            taos_close(param);
        }

        printf("insert error, retry in 2sec.");
        taosMsleep(2000);
    } else if (code == 1) {
        printf("insert into table one row!");
    }

    printf("code:%d", code);
    taosMsleep(5000);

    taos_query_a(param, "insert into test.txu using test.txx tags(1) values(now, 2)", insertCallBack, param);
}

void loadData(char* filePath, char** buf) {
    wordexp_t fullPath;

    wordexp(filePath, &fullPath, 0);

    FILE* src = fopen(fullPath.we_wordv[0], "rb");
    if (src == NULL) {
        perror("open file failed.");
        wordfree(&fullPath);
        return;
    }

    int32_t lineCnt = 0;
    char* st = 0;

    while((st = fgets(buf[lineCnt], 1024, src)) != 0 && !feof(src)){
//        printf("%s", st);
        printf("load line:%d", lineCnt);
        size_t len = strlen(buf[lineCnt]);
        buf[lineCnt][len-1] = '\0';
        lineCnt += 1;
    }

    printf("load line: completed!");

    wordfree(&fullPath);
    fclose(src);
}

void create_table(TAOS* conn, char* dbname, int32_t start, int32_t total) {
    char sql[256] = {0};
    sprintf(sql, "use %s", dbname);

    taos_query(conn, sql);
    memset(sql, 0, sizeof(sql)/sizeof(sql[0]));

    for(int32_t i = start; i < total + start; ++i) {
        sprintf(sql, "create table device%d using dev_mt tags('dev%d', %d)", i, i, i);
        int32_t ret = taos_query(conn, sql);
        if (ret != 0) {
            printf("%s", taos_errstr(conn));
        }
        memset(sql, 0, sizeof(sql)/sizeof(sql[0]));
    }

    printf("create table start:%d, end:%d", start, total+start);
}

int load_one_table(TAOS* conn, char* dbname, int32_t startId, int32_t numOfTable) {
    char oneSql[1024] = {0};
    sprintf(oneSql, "use %s", dbname);

    taos_query(conn, oneSql);
    memset(oneSql, 0 , 1024);

    char sql[65536] = {0};

    int32_t totalCnt = 90*24*3600/5;

    char* buffer[10000] = {0};
    for(int32_t i=0; i<10000; ++i) {
        buffer[i] = malloc(1024);
    }

    loadData("~/0902_excavator_data_f.csv", buffer);

    int64_t start = taosGetTimestampMs();

    uint64_t startTime = 1525104000000L;

    int64_t prevTimestamp = 0;
    int64_t elapsedTime = 0;

    for(int32_t k = startId; k < numOfTable + startId; ++k) {
        struct timeval etv;
        gettimeofday(&etv, NULL);
        int64_t et = etv.tv_sec * 1000 + etv.tv_usec/1000;

        if (k == startId) {
            elapsedTime = 0;
        } else {
            elapsedTime = et - prevTimestamp;
        }
        prevTimestamp = et;

        printf("timestamp:%ld, elapsed time:%ldms, table:%d", et, elapsedTime, k);

        startTime = 1525104000000L;
        for (int32_t i = 0; i < totalCnt;) {
            int32_t totalLen = sprintf(sql, "insert into device%d values(%lld, %s)", k, startTime, buffer[i % 2254]);
            ++i;

            for (int32_t j = 0; j < 80 && i < totalCnt; ++j, ++i) {
                startTime += 5000;

                int32_t len = sprintf(oneSql, " (%lld, %s)", startTime, buffer[i % 2254]);
                memcpy(sql+totalLen, oneSql, len);
                totalLen += len;
            }

            while(1) {
                int32_t ret = taos_query(conn, sql);
                if (ret == 0) {
                    break;
                }

                printf("error:%s\nretry", taos_errstr(conn));
            }

            startTime += 5000;
            memset(sql, 0, sizeof(sql) / sizeof(sql[0]));
        }
    }

    int64_t end = taosGetTimestampMs();
    printf("total elapsed time:%lldms", end - start);

    for(int32_t i=0; i<10000; ++i) {
        free(buffer[i]);
        buffer[i] = NULL;
    }
}

int insert_to_db(TAOS* conn) {
    taos_query(conn, "create database test");
    taos_query(conn, "use test");
    char sql[65536] = {0};
    sprintf(sql, "create table floor_data(updatetime timestamp, dataversion smallint, altitude float,"
                 "coolanttemp float, enginespeed float, fuellevel float, hydraulicoiltemp float,"
                 "pump1pressure float, pump2pressure float, pv1current float, pv2current float,"
                 "totalworktime float, actioncode smallint, alarmcode1 int, alarmcode2 int, autoidling tinyint,"
                 "engineoutputpower int, engineoutputtorque int, gear int, gpsdatavalidity tinyint, lastcycleloadrate smallint,"
                 "lspresure int, negativefeedbackpressure1 int, negativefeedbackpressure2 int, powervalvecurrent int,"
                 "pump1current int, pump1flow int, pump2current int, pump2flow int, pumpabsorbedpower int, pumpabsorbedtorque int,realtimefc smallint,"
                 "realtimeloadrate smallint,sysalarmcode smallint,syserrorcode smallint,torquepreset int,velocity smallint,"
                 "workmode int) tags(deviceid bigint)");
    taos_query(conn, sql);

    uint64_t startTime = 1525104000000L;
    uint64_t tagsId = 204203858L;

    char onesql[1024] = {0};
    memset(sql, 0, sizeof(sql) / sizeof(sql[0]));

    char buffer[10000][128] = {0};
    loadData("~/first/data.txt", buffer);

    for (int32_t i = 0; i < 500; ++i) {
        sprintf(onesql, "create table device%d using floor_data tags(%ld)", i + 1, tagsId++);
        int32_t ret = executeSQL(conn, onesql, NULL);
        if (ret != 0) {
            return -1;
        }
    }

    int32_t len = 0;

    int64_t start = taosGetTimestampMs();
    printf("start time:%ld", start);

#if 1
    //2880*31
    int32_t tid = 1;
//    for (int32_t k = 0; k < 2880*31; ++k) {
    for (int32_t k = 0; k < 5000; ++k) {
        tid = 1;
        for (int32_t i = 0; i < 4; ++i) {
//        for (int32_t i = 0; i < 125; ++i) {
            len = sprintf(onesql, "insert into device%d values(%ld, %s)", tid++, startTime, buffer[k % 10000]);
            strncat(sql, onesql, len + 1);

//            for (int32_t j = 0; j < 399; ++j) {
            for (int32_t j = 0; j < 99; ++j) {
                len = sprintf(onesql, " device%d values(%ld, %s)", tid++, startTime, buffer[k % 10000]);
                strncat(sql, onesql, len + 1);
            }

            int32_t ret = executeSQL(conn, sql, NULL);
            if (ret != 0) {
                printf("failed to insert data:%s", taos_errstr(conn));
                return -1;
            }
            memset(sql, 0, sizeof(sql) / sizeof(sql[0]));
        }
        startTime += 30000;
    }
#else
    for(int32_t tid = 1; tid<=50000; ++tid) {//tables
//    for(int32_t tid = 1; tid<=500; ++tid) {//tables
        int64_t time = startTime;
        for(int32_t k=0; k<31; ++k) {//days
            for(int32_t f=0; f<8; ++f) {
                len = sprintf(sql, "insert into device%d values(%ld, %s)", tid, time, buffer[f % 10000]);

                for (int32_t j = 1; j < 360; ++j) {
                    time += 30000;
                    int32_t newLen = sprintf(sql + len, " (%ld, %s)", time, buffer[j % 10000]);
                    len += newLen;
                }
                time += 30000;
                int32_t ret = executeSQL(conn, sql, NULL);
                if (ret != 0) {
                    printf("failed to insert data:%s", taos_errstr(conn));
                    return -1;
                }

                memset(sql, 0, sizeof(sql)/sizeof(sql[0]));
            }
        }
    }
#endif
    int64_t end = taosGetTimestampMs() - start;
    printf("Elapsed time:%ld ms", end);
}

typedef struct {
    int threadid;
    int start;
    int end;
    int metric_model;
    char* startTime;
    char* endTime;
    bool writeToFile;
} TableRange;

int oneLoader(void* param) {
    TAOS *conn = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);

    executeSQL(conn, "use evidev", NULL);
    int64_t start = taosGetTimestampMs();

    TableRange *range = (TableRange *) param;

    char sql[1024] = {0};

    for (int32_t i = range->start; i < range->end; ++i) {
        sprintf(sql, "select * from device%d where receive_time>= '%s 0:0:0' and receive_time<'%s 0:0:0'",
                i, range->startTime, range->endTime);

        printf("%s", sql);
        executeSQL(conn, sql, NULL);
        memset(sql, 0, sizeof(sql) / sizeof(sql[0]));
    }

    int64_t elapsed = taosGetTimestampMs() - start;
    printf("total elapsed time: %ld ms", elapsed);

    taos_close(conn);

    return 0;
}

int multiThreadLoad(int32_t numOfThreads, int32_t totalMeters, char* startTime, char* endTime) {
    pthread_attr_t thattr;
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

    pthread_t* threadId = malloc(sizeof(pthread_t)*numOfThreads);

    TableRange* params = calloc(1, sizeof(TableRange)*numOfThreads);
//    int32_t numOfTablesPerThread = totalMeters/numOfThreads;

    int32_t startTableIdArray[5] = {0, 3, 9, 6, 12};

    for (int i = 0; i < numOfThreads; ++i) {
        params[i].threadid = i;
        params[i].start = startTableIdArray[i]*10000+1;
        params[i].end = params[i].start + 10000;

        params[i].startTime = startTime;
        params[i].endTime = endTime;

        pthread_create(&threadId[i], NULL, oneLoader, &params[i]);
    }

    for (int32_t i = 0; i < numOfThreads; ++i) {
        pthread_join(threadId[i], NULL);
    }

    pthread_attr_destroy(&thattr);

    return 0;
}

int64_t start_ts = 1433955661000;

static void createEnv(void* conn) {
  executeSQL(conn, "create table sm1(ts timestamp, k int) tags(a int)", NULL);
  char sql[1024] = {0};

  int64_t s = start_ts;
  for(int32_t i = 0; i < 10; ++i) {
    sprintf(sql, "create table stm%d using sm1 tags(%d)", i, i);
    executeSQL(conn, sql, NULL);

    s = start_ts;
    for(int32_t j = 0; j < 5000; ++j) {
      memset(sql, 0, tListLen(sql));
      sprintf(sql, "insert into stm%d values(%lld, %d)", i, s++, j);
      executeSQL(conn, sql, NULL);
    }
  }
}

int main(int argc, char **argv) {
    if (argc < 5) {
        return -1;
    }

    taos_options(TSDB_OPTION_CONFIGDIR, argv[1]);
//    taos_options(TSDB_OPTION_LOCALE,   "zh_cn.cp11936-8");
    taos_options(TSDB_OPTION_CHARSET,   "cp11936");

    taos_init();

    TAOS *conn = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, 0, 0);
    if (conn == NULL) {
        printf("Failed to connect to DB, reason:%s", taos_errstr(conn));
        exit(-1);
    }

/*
    if (argv[2][0] == '0') {
        int nt = atoi(argv[3]);
        int totalMeters = atoi(argv[4]);
        multiThreadLoad(nt, totalMeters);
    } else {
        int64_t start = taosGetTimestampMs();
        executeSQL(conn, "select * from floor_data", NULL);
        int64_t elapsed = taosGetTimestampMs() - start;
        printf("total elapsed time: %ld", elapsed);
    }
    return 0;*/

    int32_t start = 0;
    if (argc >= 4) {
        start = strtol(argv[3], NULL, 10);
    }

    int32_t total = 1;
    if (argc >= 5) {
        total = strtol(argv[4], NULL, 10);
    }
    
    executeSQL(conn, "use intp_db0", NULL);
//    createEnvironment(conn, 5, 5, 100, 30);
    executeSQL(conn, "select interp(ts) from intp_tb0;", NULL);
//    executeSQL(conn, "CREATE database TU1", NULL);
    // selectivity + tags/ts + group by normal columns
//    executeSQL(conn, "(select count(*) from test where ts<'1970-1-1 8:1:40.9') union "
//                     "(select count(*) from test where ts<'1970-1-1 8:1:40.9')", NULL);
//    executeSQL(conn, "select count(*) from test where ts<'1970-1-1 8:1:40.9' union "
//                   "select count(*) from test where ts<'1970-1-1 8:1:40.9'", NULL);

//    createEnvironment(conn, 5000, 5000, 100, 30);
//    executeSQL(conn, "select last_row(ts) from m1 where tbname in ('tm0', 'tm1') group by tbname", NULL);
//    executeSQL(conn, "select top(k, 5) from tm0", NULL);
//    createEnvironment(conn, 50, 50, 100, 30);
//    executeSQL(conn, "select m1.* from m1,m2 where m1.a=m2.a1 and m1.ts=m2.ts;", NULL);
//    executeSQL(conn, "select * from m1,m2 where m1.a=m2.a1 and m1.ts=m2.ts;", NULL);//crash!!!!
//  executeSQL(conn, "select join_tb1.*, join_tb0.* from join_tb1 , join_tb0 where join_tb1.ts = join_tb0.ts and "
//                   "join_tb1.ts >= 100000 and join_tb0.c7 = false limit 10", NULL);

//    executeSQL(conn, "create table t1 (ts timestamp,t2 binary(30),\u2028mrn int,\u2028ptname binary(20)) tags(type int);", NULL);

//    executeSQL(conn, "select count(*) from m1 group by tbname,K", NULL);
//    executeSQL(conn, "select m2.b,m1.a,m1.ts from m1,m2 where m1.ts=m2.ts and m1.a=m2.b;", NULL);
//    executeSQL(conn, "select count(*) from ac_stb group by t1 order by t1 asc slimit 2 soffset 1;", NULL);
//    executeSQL(conn, "select join_mt1.t1,join_mt0.t1,join_mt1.c1,join_mt1.ts from join_mt0, join_mt1 "
//                     "where join_mt0.ts=join_mt1.ts and join_mt0.t1=join_mt1.t1;", NULL);
//    executeSQL(conn, "select count(*) from join_mt0, join_mt1 where join_mt0.ts=join_mt1.ts and join_mt0.t2=join_mt1.t2;", NULL);
//     executeSQL(conn, "select count(join_mt0.c2),last(join_tb0.c2),first(join_mt0.c7) from join_mt0, join_tb0 where join_mt0.t1=join_tb0.t1"
//                      "                     and join_mt0.ts=join_tb0.ts and join_mt0.t1=1 interval(500a) order by join_mt0.ts asc;", NULL);
//    return 0;
//    executeSQL(conn, "select count(join_tb3.*) from join_tb1, join_tb0 where join_tb1.ts = join_tb0.ts and join_tb1.ts <= 100002 and join_tb0.c7 = true;", NULL);
//    executeSQL(conn, "select join_tb1.*, join_tb0.ts from join_tb0 where join_tb1.ts = join_tb0.ts", NULL);
//    executeSQL(conn, "select count(join_mt0.k), sum(join_mt1.k), first(join_mt0.c5)"
//                     " from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1 and join_mt0.ts=join_mt1.ts and join_mt0.t1=1 interval(10a)", NULL);

//    executeSQL(conn, "select count(m1.k), sum(sm1.k), first(m1.h)"
//                     " from m1,sm1 where m1.a=sm1.a and (m1.a=1 or m1.a=2) and m1.ts=sm1.ts interval(10a) group by m1.a", NULL);
//    createEnvironment(conn, 20, 20, 10000, 30);
//    executeSQL(conn, "select count(*) from m1 where m1.a<20", NULL);
//    createEnv(conn);

//    executeSQL(conn, "select count(*) from tm0, tm1 where tm0.ts=tm1.ts ", NULL);
//    executeSQL(conn, "select count(*) from tm0, tm1 where tm0.ts=tm1.ts interval(30s) order by ts asc "
//                     "limit 20 offset 1;", NULL);
//    executeSQL(conn, "select count(tm0.ts), first(tm1.h) from tm0, tm1 where tm0.ts=tm1.ts interval(3s) order by ts desc", NULL);
//    executeSQL(conn, "select t1.k, t2.k from t1,t2 where t1.ts=t2.ts and t1.a=t2.a and t1.ts<now;", NULL);

    //    executeSQL(conn, "select count(t1.k), count(t2.k) from t1,t2 where t1.a=t2.a and t1.ts<now and t1.ts=t2.ts and t1.k=1", NULL);
//   executeSQL(conn, "select count(t1.k), count(t2.k) from t1,t2 where t1.ts=t2.ts and t1.a=t2.a and t1.ts<now;", NULL);
//    executeSQL(conn, "select m2.* from m1, m2 where m1.tbname in ('tm0', 'tm10') and (m1.a=0 or (m1.a=10 and m1.a<20)) "
//                     "and m1.ts<now and m1.a=m2.a and m1.ts=m2.ts", NULL);
//
//    executeSQL(conn, "select m1.ts from m1 where m1.ts<now and (m1.a=9 and m1.a=20) and m1.ts>10000", NULL);
//    for(int32_t i = 0; i < 20000; ++i) {
//        executeSQL(conn, "select count(*) from m1", NULL);
//    }
//    executeSQL(conn, "create database test precision 'us' rows 20000 ablocks 1.9 days 20", NULL);
//    taos_query_a(conn, "select * from tm0", queryCallback, conn);
//    getchar();
//    executeSQL(conn, "select count(m1.*) from m1", NULL);
//    executeSQL(conn, "select top(k, 4) from mx interval(10a) order by ts desc", NULL);
//    executeSQL(conn, "INSERT INTO dev_2(workmode,gear,action_code,auto_idling,alarm_code,sys_error_code_1,sys_error_code_2,sys_error_code_3,alarm_code_1,alarm_code_2,alarm_code_3,alarm_code_4,alarm_code_5,gps_state,lock_level,gps_precision_mode,machine_timestamp,mask_1,mask_2,oil_pressure,atmospheric_pressure,pump1_flow,pump2_flow,fuel_consumption_index,fuel_temperature_index,pressure_index,health_index,pump1_pressure,pump2_pressure,pump1_current,pump2_current,power_valve_current,displacement_direction,displacement_speed,engine_speed,engine_output_torque,engine_output_power,pump_total_absorbed_torque,pump_total_absorbed_power,torque_preset,total_worktime,total_idle_time,worktime,day_fuel_consumption,fuel_temperature,cooling_water_temperature,hydraulic_oil_temperature,ambient_temperature,ls_pressure,maintenance_confirmed_worktime,total_fuel_consumption,total_idle_fuel_consumption,realtime_fuel_consumption,avg_fuel_consumption,last_fuel_utilization_rate,fuel_level,longitude,latitude,altitude,lock_time_left,battery_voltage,intake_temperature,data_version,receive_time)  VALUES (1,11,0,0,0,0,0,13,0,0,0,0,0,10,0,0,0,1837500699,-544335551,250.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,174.0,195.0,535.0,477.0,0.0,161.0,1.0,1901.0,0.0,0.0,0.0,0.0,0.0,9.476253,NaN,1.0095834,0.0,0.0,79.0,52.0,0.0,0.0,0.0,83.0,4.0,0.0,0.0,0.0,100.0,113.975845,33.048813,82.4,100.0,28.2686,0.0,\"17\",1554184943848) ", NULL);

//    FILE* f = fopen("/home/lisa/a.sql", "r");
//    fread(buf, 65535, 1, f);
//    printf("%s", buf);
//    executeSQL(conn, buf, NULL);
//    executeSQL(conn, "create database if not exists test replica 1 days 1 keep 365 rows 255 cache 6400 ablocks 1024 tblocks 128 tables 3100 "
//                     " ctime 360 clog 1 comp 2", NULL);
//    executeSQL(conn, "select max(c1), min(c2), max(c2) from lm_stb0 where ts >= 1537146000000 and ts <= 1537151400000 and t1>1 and t1 < 8 interval(5m) "
//                     "group by t1 order by t1 asc limit 5 offset 0;", NULL);
//    return 0;

//    char sql[64000] = {0};
//    int32_t n = 0;
//    for (int32_t i = 0; i < 1; ++i) {
//        sprintf(sql,"create table test using m1 tags(%d)", i, i);
////        int32_t t = taos_query(conn, sql);
//
////        if (t != 0) {
////            printf("%s\n", taos_errstr(conn));
////        }
//
//        n = sprintf(sql, "insert into test values(100000, 1 , 'abc') ");
//
//        for(int32_t j = 0; j < 1000; ++j) {
//            n += sprintf(sql+n, "(100000+%da, 2, 'abc%d') ", j, j);
//        }
//
//        n += sprintf(sql + n, "test1 values(100000, 1 , 'abc') ");
//        for(int32_t j = 0; j < 1000; ++j) {
//            n += sprintf(sql + n, "(100000+%da, 2, 'abc%d') ", j, j);
//        }
//
//        printf("%s\n", sql);
//
//        int32_t code = taos_query(conn, sql);
//        if (code != TSDB_CODE_SUCCESS) {
//            printf("%s\n", taos_errstr(conn));
//        }
//    }

//    executeSQL(conn, "select count(k),max(k), min(k), sum(k), first(k), last(k), avg(k) from t1.tm0 where ts>1000 and "
//                     "ts< 200000 interval(10s) fill(prev);", NULL);

//    create_table(conn, argv[2], start, total);
//    load_one_table(conn, argv[2], start, total);
//    insert_to_db(conn);
//    getchar();
    taos_close(conn);
    return 0;
}
