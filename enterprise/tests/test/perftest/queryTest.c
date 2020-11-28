#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wordexp.h>

#include <assert.h>
#include <pthread.h>
#include <sys/time.h>
#include <tcache.h>
#include <unistd.h>

#include "taos.h"
#include "testCommon.h"

static int64_t getTime();

static int32_t rid = 1;
void           sqlfullTest(TAOS* conn);
void queryCallback(void* param, TAOS_RES* tres, int code);

struct SInsertParam {
  TAOS* conn;
  int32_t id;
};

void fetchCallBack(void* param, TAOS_RES* tres, int numOfRows) {
  char buf[512] = {0};

  if (numOfRows > 0) {
    printf("fetch data.");
    int32_t     num_fields = taos_num_fields(tres);
    TAOS_FIELD* pField = taos_fetch_fields(tres);

    for (int i = 0; i < numOfRows; ++i) {
      TAOS_ROW row = taos_fetch_row(tres);
      taos_print_row(buf, row, pField, num_fields);
      int32_t k = __sync_fetch_and_add(&rid, 1);
      printf("%d:%s\n", k, buf);
      buf[0] = 0;
    }
    printf("--------------------all data has been fetched to client.\n");
    taos_fetch_rows_a(tres, fetchCallBack, param);
    return;
  } else if (numOfRows == 0) {
    taos_free_result(tres);
  }

  struct SInsertParam* p = (struct SInsertParam*) param;
  taos_query_a(p->conn, "show tables", queryCallback, p);
}

void fetchCallBack_Single(void* param, TAOS_RES* tres, TAOS_ROW row) {
  char buf[512] = {0};
  if (row) {
    int32_t     num_fields = taos_num_fields(tres);
    TAOS_FIELD* pField = taos_fetch_fields(tres);

    // printf("row:%d %"PRId64" %"PRId64"", pTable->rowsRetrieved, *((int64_t *)row[0]), *((int64_t *)row[1]));
    taos_print_row(buf, row, pField, num_fields);
    int32_t k = __sync_fetch_and_add(&rid, 1);
    printf("%d:%s", k, buf);
    buf[0] = 0;

    taos_fetch_row_a(tres, fetchCallBack_Single, param);
  } else {
    taos_free_result(tres);
    printf("all data has been fixed");
    // printf("index:%d, %d rows data retrieved", pTable->id, pTable->rowsRetrieved);
  }
}

void queryCallback(void* param, TAOS_RES* tres, int code) {
  printf("query completed, code: %s, start to fetch data\n", taos_errstr(tres));

  if (code < 0) {
    printf("==================query:, error:%d, taos_res:%p=============================\n", code, tres);
    return;
  }

  taos_fetch_rows_a(tres, fetchCallBack, param);
}

int64_t start_ts = 1433955661000;

void insertCallBack(void* param, TAOS_RES* tres, int code) {
  if (taos_errno(tres) != 0) {
  }

  taos_free_result(tres);
  struct SInsertParam *p = (struct SInsertParam*) param;

  char tt[1024] = {0};
  sprintf(tt, "insert into tm%d values(%"PRId64", 1)", p->id, ++start_ts);
  taos_query_a(p->conn, tt, insertCallBack, param);
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
  char*   st = 0;

  while ((st = fgets(buf[lineCnt], 1024, src)) != 0 && !feof(src)) {
    //        printf("%s", st);
    printf("load line:%d", lineCnt);
    size_t len = strlen(buf[lineCnt]);
    buf[lineCnt][len - 1] = '\0';
    lineCnt += 1;
  }

  printf("load line: completed!");

  wordfree(&fullPath);
  fclose(src);
}

void create_table(TAOS* conn, char* dbname, int32_t start, int32_t total) {
  char sql[256] = {0};
  sprintf(sql, "use %s", dbname);

  TAOS_RES* pSql = taos_query(conn, sql);
  taos_free_result(pSql);

  memset(sql, 0, sizeof(sql) / sizeof(sql[0]));

  for (int32_t i = start; i < total + start; ++i) {
    sprintf(sql, "create table device%d using dev_mt tags('dev%d', %d)", i, i, i);
    pSql = taos_query(conn, sql);

    if (taos_errno(pSql) != 0) {
      printf("%s", taos_errstr(conn));
    }
    memset(sql, 0, sizeof(sql) / sizeof(sql[0]));
  }

  printf("create table start:%d, end:%d", start, total + start);
}

int load_one_table(TAOS* conn, char* dbname, int32_t startId, int32_t numOfTable) {
  char oneSql[1024] = {0};
  sprintf(oneSql, "use %s", dbname);

  TAOS_RES* pSql = taos_query(conn, oneSql);
  taos_free_result(pSql);

  memset(oneSql, 0, 1024);

  char sql[65536] = {0};

  int32_t totalCnt = 90 * 24 * 3600 / 5;

  char* buffer[10000] = {0};
  for (int32_t i = 0; i < 10000; ++i) {
    buffer[i] = malloc(1024);
  }

  loadData("~/0902_excavator_data_f.csv", buffer);

  uint64_t startTime = 1525104000000L;

  int64_t prevTimestamp = 0;
  int64_t elapsedTime = 0;

  for (int32_t k = startId; k < numOfTable + startId; ++k) {
    struct timeval etv;
    gettimeofday(&etv, NULL);
    int64_t et = etv.tv_sec * 1000 + etv.tv_usec / 1000;

    if (k == startId) {
      elapsedTime = 0;
    } else {
      elapsedTime = et - prevTimestamp;
    }
    prevTimestamp = et;

    printf("timestamp:%ld, elapsed time:%ldms, table:%d", et, elapsedTime, k);

    startTime = 1525104000000L;
    for (int32_t i = 0; i < totalCnt;) {
      int32_t totalLen = sprintf(sql, "insert into device%d values(%" PRId64 ", %s)", k, startTime, buffer[i % 2254]);
      ++i;

      for (int32_t j = 0; j < 80 && i < totalCnt; ++j, ++i) {
        startTime += 5000;

        int32_t len = sprintf(oneSql, " (%" PRId64 ", %s)", startTime, buffer[i % 2254]);
        memcpy(sql + totalLen, oneSql, len);
        totalLen += len;
      }

      while (1) {
        pSql = taos_query(conn, sql);
        if (taos_errno(pSql) == 0) {
          break;
        }

        printf("error:%s\nretry", taos_errstr(conn));
      }

      startTime += 5000;
      memset(sql, 0, sizeof(sql) / sizeof(sql[0]));
    }
  }

  for (int32_t i = 0; i < 10000; ++i) {
    free(buffer[i]);
    buffer[i] = NULL;
  }

  return 0;
}

typedef struct {
  int     threadid;
  char*   sql;
} MultiThreadQueryInfo;

void* doQuery(void* param) {
  MultiThreadQueryInfo* range = (MultiThreadQueryInfo*)param;

  for (int32_t i = 0; i < 100000; ++i) {
    int64_t start = getTime();
    printf ("id:%d, time:%ld\n", range->threadid, start);
    TAOS* conn = taos_connect("ubuntu", "root", "taosdata", NULL, 0);
    executeSQL(conn, range->sql, NULL);
    taos_close(conn);

    int64_t end = getTime();
    printf("id:%d, end time:%ld\n", range->threadid, end);
  }


  return 0;
}

int multiThreadQuery(int32_t numOfThreads, char* sql) {
  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  pthread_t* threadId = malloc(sizeof(pthread_t) * numOfThreads);

  MultiThreadQueryInfo* params = calloc(1, sizeof(MultiThreadQueryInfo) * numOfThreads);

  for (int i = 0; i < numOfThreads; ++i) {
    params[i].threadid = i;
    params[i].sql = sql;
    pthread_create(&threadId[i], NULL, doQuery, &params[i]);
  }

  for (int32_t i = 0; i < numOfThreads; ++i) {
    pthread_join(threadId[i], NULL);
  }

  pthread_attr_destroy(&thattr);
  return 0;
}

static int64_t getTime() {
  struct timeval s1;
  gettimeofday(&s1, NULL);
  int64_t es1 = s1.tv_sec * 1000 + s1.tv_usec/1000;

  return es1;
}

void generatedData(TAOS* taos) {
  srand(time(NULL));

  FILE* f = fopen("./devid", "r");
  char* line = NULL;
  size_t length = 0;

  struct timeval tv ;
  gettimeofday(&tv, NULL);
  int64_t start = tv.tv_sec * 1000 + tv.tv_usec/1000;

  int32_t alloc = 10000;
  char** pTableNameList = calloc(alloc, sizeof(void*));

  int32_t i = 0;
  while(getline(&line, &length, f) > 0) {
    pTableNameList[i++] = strdup(line);
  }

  int32_t numOfTables = i;

  free(line);
  line = NULL;

  printf("total devid:%d\n", i);
  fclose(f);

  f = fopen("./sample_data", "r");

  int32_t numOfData = 0;
  char** dd = calloc(1, 1000* sizeof(void*));
  while(getline(&line, &length, f) > 0) {
    dd[numOfData] = strdup(line);
    printf("%s\n", dd[numOfData]);

    numOfData += 1;
  }
  fclose(f);

  // start to insert data to db
  char sql[1024] = {0};

  while(1) {
    int64_t cs1 = getTime();

    for(int32_t j = 0; j < numOfTables; ++j) {
      char* name = pTableNameList[j];
      int32_t index = (rand()%numOfData);

      sprintf(sql, "insert into %s values(%"PRId64", %s)", name, start, dd[index]);

      int64_t begin = getTime();
#if 0
      int32_t code = taos_query(taos, sql);
      if (code != 0) {
        printf("error:%s\n", taos_errstr(taos));
      }
#endif
      int64_t end = getTime();
      if (end - begin > 1200) {
        printf("elapsed time too long:%ldms\n abort\n", end - begin);
        assert(0);
      }
    }

    int64_t es1 = getTime();

    printf("insert completed, total elapsed time:%ldms, ts:%ld\n", es1 - cs1, es1);

    sleep(23);
    start += 23*1000;
  }
}

int main(int argc, char** argv) {
//  taos_options(TSDB_OPTION_CONFIGDIR, "~/first/cfg");
  taos_options(TSDB_OPTION_CONFIGDIR, "/home/lisa/Documents/workspace/TDinternal/sim/tsim/cfg");
//  taos_options(TSDB_OPTION_CONFIGDIR, "/home/lisa/Documents/workspace/TDinternal/community/sim/psim/cfg");
  taos_init();
  TAOS* conn = taos_connect("ubuntu", "root", "taosdata", 0, 0);
  if (conn == NULL) {
    printf("Failed to connect to DB, reason:%s", taos_errstr(conn));
    exit(-1);
  }

#if 0
  executeSQL(conn, "use netmonitortaos", NULL);
  if (atoi(argv[1]) == 1) {
    generatedData(conn);
  } else {
    while(1) {
      int64_t st = getTime();
      executeSQL(conn, "select last_row(*) from warninginfomt group by tbname", NULL);
      int64_t et = getTime();

      if (et - st > 1200) {
        printf("too long, elapsed time:%ld\n", et - st);
        assert(0);
      } else {
        printf("exec completed, elapsed time:%ldms, time:%ld\n", et - st, st);
      }

      sleep(15);
    }
  }
  return 0;
#endif

//  multiThreadQuery(5, "select count(*) from test.m2");
//  return 0;

//  for(int32_t i = 0; i < 500000; ++i) {
//    void* p = taos_query(conn, "select * from db.fs_table");
//    taos_fetch_row(p);
//    taos_free_result(p);
//  }

//  executeSQL(conn,"create database join_db0", NULL);
//  executeSQL(conn, "select count(*) from lr_stb0 where ts>'2018-09-24 00:00:00.000' and ts<'2018-09-25 00:00:00.000' "
//                   "interval(1h) fill(NULL) group by t1 order by ts desc;", NULL);

//  executeSQL(conn, "select count(*) from fl1_stb0 interval(1y)", NULL);
//  executeSQL( conn, "select count(*) from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and
//  join_mt0.t1=join_mt1.t1 and join_mt0.c2=99;;", NULL);
//  return 0;

//  executeSQL(conn, "select sum(join_mt0.c1) from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.c2=99 and join_mt1.ts=100999;;", NULL);
    executeSQL(conn, "use wh_db0", NULL);
    executeSQL(conn, "select last(*) from wh_mt1 where t1 in ('binary')", NULL);

//    executeSQL(conn, "select spread(ts)/(1000 * 3600 * 24) from ca_tb1", NULL);
    taos_close(conn);
    return 0;
//  executeSQL(conn, "select count(*) from test.m1 interval(1s) group by tbname", NULL);
//  executeSQL(conn, "select join_tb1.ts , join_tb0.ts from join_tb1 , join_tb0 where join_tb1.ts = join_tb0.ts;", NULL);
//  createEnvironment(conn, 100, 100, 100000, 30);
      //executeSQL(conn, "select first(ts), last(ts) from lm_tb0", NULL); executeSQL(conn, "CREATE
  //    database TU1", NULL);
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
  //                     " from m1,sm1 where m1.a=sm1.a and (m1.a=1 or m1.a=2) and m1.ts=sm1.ts interval(10a) group by
  //                     m1.a", NULL);
  //    createEnvironment(conn, 20, 20, 10000, 30);
  //    executeSQL(conn, "select count(*) from m1 where m1.a<20", NULL);
  //    createEnv(conn);

  //    executeSQL(conn, "select count(*) from tm0, tm1 where tm0.ts=tm1.ts ", NULL);
  //    executeSQL(conn, "select count(*) from tm0, tm1 where tm0.ts=tm1.ts interval(30s) order by ts asc "
  //                     "limit 20 offset 1;", NULL);
  //    executeSQL(conn, "select count(tm0.ts), first(tm1.h) from tm0, tm1 where tm0.ts=tm1.ts interval(3s) order by ts
  //    desc", NULL); executeSQL(conn, "select t1.k, t2.k from t1,t2 where t1.ts=t2.ts and t1.a=t2.a and t1.ts<now;",
  //    NULL);

  //    executeSQL(conn, "select count(t1.k), count(t2.k) from t1,t2 where t1.a=t2.a and t1.ts<now and t1.ts=t2.ts and
  //    t1.k=1", NULL);
  //   executeSQL(conn, "select count(t1.k), count(t2.k) from t1,t2 where t1.ts=t2.ts and t1.a=t2.a and t1.ts<now;",
  //   NULL);
  //    executeSQL(conn, "select m2.* from m1, m2 where m1.tbname in ('tm0', 'tm10') and (m1.a=0 or (m1.a=10 and
  //    m1.a<20)) "
  //                     "and m1.ts<now and m1.a=m2.a and m1.ts=m2.ts", NULL);
  //
  //    executeSQL(conn, "select m1.ts from m1 where m1.ts<now and (m1.a=9 and m1.a=20) and m1.ts>10000", NULL);

  for(int32_t i = 0; i < 200000; ++i) {
    executeSQL(conn, "select count(*) from tm99", NULL);
    executeSQL(conn, "select * from m1", NULL);
    executeSQL(conn, "select count(*) from tm99 group by k", NULL);
    executeSQL(conn, "select count(*) from m1 where tbname in ('tm99')", NULL);
    executeSQL(conn, "select count(*) from m1 where tbname in ('tm99') interval(1m)", NULL);
    executeSQL(conn, "select count(tm99.ts) from tm99, tm98 where tm99.ts=tm98.ts", NULL);
  }
  return 0;
  //    executeSQL(conn, "create database test precision 'us' rows 20000 ablocks 1.9 days 20", NULL);
//  struct SInsertParam p = {.conn = conn, .id = 0};
//  taos_query_a(conn, "insert into tm0 values(1433955661000, 1)", insertCallBack, &p);
//      taos_query_a(conn, "select * from tm0", insertCallBack, conn);
//  struct SInsertParam p1 = {.conn = conn, .id = 1};
//  taos_query_a(conn, "insert into tm1 values(1433955661000, 1)", insertCallBack, &p1);

//  struct SInsertParam p2 = {.conn = conn, .id = 2};
//  taos_query_a(conn, "insert into tm2 values(1433955661000, 1)", insertCallBack, &p2);
//    taos_query_a(conn, "select count(*) from tm0", queryCallback, &p);
//    taos_query_a(conn, "select count(*) from tm0", queryCallback, &p);
  //    executeSQL(conn, "select count(m1.*) from m1", NULL);
  //    executeSQL(conn, "select top(k, 4) from mx interval(10a) order by ts desc", NULL);
  //    executeSQL(conn, "INSERT INTO
  //    dev_2(workmode,gear,action_code,auto_idling,alarm_code,sys_error_code_1,sys_error_code_2,sys_error_code_3,alarm_code_1,alarm_code_2,alarm_code_3,alarm_code_4,alarm_code_5,gps_state,lock_level,gps_precision_mode,machine_timestamp,mask_1,mask_2,oil_pressure,atmospheric_pressure,pump1_flow,pump2_flow,fuel_consumption_index,fuel_temperature_index,pressure_index,health_index,pump1_pressure,pump2_pressure,pump1_current,pump2_current,power_valve_current,displacement_direction,displacement_speed,engine_speed,engine_output_torque,engine_output_power,pump_total_absorbed_torque,pump_total_absorbed_power,torque_preset,total_worktime,total_idle_time,worktime,day_fuel_consumption,fuel_temperature,cooling_water_temperature,hydraulic_oil_temperature,ambient_temperature,ls_pressure,maintenance_confirmed_worktime,total_fuel_consumption,total_idle_fuel_consumption,realtime_fuel_consumption,avg_fuel_consumption,last_fuel_utilization_rate,fuel_level,longitude,latitude,altitude,lock_time_left,battery_voltage,intake_temperature,data_version,receive_time)
  //    VALUES
  //    (1,11,0,0,0,0,0,13,0,0,0,0,0,10,0,0,0,1837500699,-544335551,250.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,174.0,195.0,535.0,477.0,0.0,161.0,1.0,1901.0,0.0,0.0,0.0,0.0,0.0,9.476253,NaN,1.0095834,0.0,0.0,79.0,52.0,0.0,0.0,0.0,83.0,4.0,0.0,0.0,0.0,100.0,113.975845,33.048813,82.4,100.0,28.2686,0.0,\"17\",1554184943848)
  //    ", NULL);

  //    FILE* f = fopen("/home/lisa/a.sql", "r");
  //    fread(buf, 65535, 1, f);
  //    printf("%s", buf);
  //    executeSQL(conn, buf, NULL);
  //    executeSQL(conn, "create database if not exists test replica 1 days 1 keep 365 rows 255 cache 6400 ablocks 1024
  //    tblocks 128 tables 3100 "
  //                     " ctime 360 clog 1 comp 2", NULL);
  //    executeSQL(conn, "select max(c1), min(c2), max(c2) from lm_stb0 where ts >= 1537146000000 and ts <=
  //    1537151400000 and t1>1 and t1 < 8 interval(5m) "
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

  //    executeSQL(conn, "select count(k),max(k), min(k), sum(k), first(k), last(k), avg(k) from t1.tm0 where ts>1000
  //    and "
  //                     "ts< 200000 interval(10s) fill(prev);", NULL);

  //    create_table(conn, argv[2], start, total);
  //    load_one_table(conn, argv[2], start, total);
  //    insert_to_db(conn);
  //    getchar();
  taos_close(conn);
  return 0;
}
