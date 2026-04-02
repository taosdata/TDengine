// TAOS asynchronous API example
// this example opens multiple tables, insert/retrieve multiple tables
// it is used by TAOS internally for one performance testing
// for a simple async example, check asyncdemo.c
// to compiple: gcc -o masync masync.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>

#include "taos.h"
#include "tsclient.h"
#include <stdint.h>

void taos_error(TAOS *taos);
int64_t get_curr_time(){
  struct timeval tv;
  if (gettimeofday(&tv, NULL)) {
    fprintf(stderr, "Failed to get current time\n");
    exit(1);
  }
  return (int64_t)tv.tv_sec*1000000 + tv.tv_usec;
}

#define FUNC_NUM 8
char *func[] = {
  "count",
  "sum",
  "avg",
  "max",
  "min",
  "stddev",
  "first",
  "last"
};

int main(int argc, char *argv[])
{
  tscPrint("usage: tableNum tableInterval(s) tableColumns streamInterval(s) streamColumns replica createTable(0-only-insert,1-create-insert) \n");

  int tableNum = 1;
  int64_t tableInterval = 1;
  int tableColumns = 64;

  int streamInterval = 0;
  int streamColumns = 64;
  int replica = 1;
  int createTable = 1;
  int rowNum = 1000;

  int code = 0;
  char sql[1024*64] = { 0 };

  // a simple way to parse input parameters
  if (argc >= 2) tableNum = atoi(argv[1]);
  if (argc >= 3) tableInterval = atoi(argv[2]);
  if (argc >= 4) tableColumns = atoi(argv[3]);
  if (argc >= 5) streamInterval = atoi(argv[4]);
  if (argc >= 6) streamColumns = atoi(argv[5]);
  if (argc >= 7) replica = atoi(argv[6]);
  if (argc >= 8) createTable = atoi(argv[7]);
  if (argc >= 9) rowNum =  atoi(argv[8]);

  if (streamColumns > tableColumns) {
    streamColumns = tableColumns;
  }

  tscPrint("tableNum:%d tableInterval:%d tableColumns:%d streamInterval:%d streamColumns:%d replica:%d createTable:%d rowNum:%d"
    , tableNum, tableInterval, tableColumns, streamInterval, streamColumns, replica, createTable, rowNum);

  taos_init();

  void *taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
  if (taos == NULL)
    taos_error(taos);

  tscPrint("connect to taosd success");

  if (createTable > 0) {
    taos_query(taos, "drop database db");

    sprintf(sql, "create database db replica %d", replica);
    code = taos_query(taos, sql);
    if (code != 0) {
      taos_error(taos);
    }

    code = taos_query(taos, "use db");
    if (code != 0) {
      taos_error(taos);
    }
    tscPrint("create database finished");

    int len = sprintf(sql, "create table db.mt (ts timestamp");
    for (int i = 1; i < tableColumns-1; i++) {
      len += sprintf(sql + len, ", c%d double", i);
    }
    sprintf(sql + len, ") tags(tgcol int)");
    code = taos_query(taos, sql);
    if (code != 0) {
      taos_error(taos);
    }
    tscPrint("create metrics finished");

    for (int i = 0; i < tableNum; ++i) {
      sprintf(sql, "create table db.t%d using mt tags(%d)", i, i);
      code = taos_query(taos, sql);
      if (code != 0) {
        taos_error(taos);
      }
    }
    tscPrint("create table finished");

    if (streamInterval != 0) {
      tscPrint("sleep 3 seconds to wait table created");
      taosMsleep(3000);
      tscPrint("sleep finished");

      for (int i = 0; i < tableNum; ++i) {
        int len = sprintf(sql, "create table db.s%d as select count(c1) as s1", i);
        for (int j = 2; j < streamColumns - 1; j++) {
          len += sprintf(sql + len, ",%s(c%d) as s%d", func[(j-1) % FUNC_NUM], j, j);
        }
        sprintf(sql + len, " from t%d interval(%ds)", i, streamInterval);
        code = taos_query(taos, sql);
        if (code != 0) {
          taos_error(taos);
        }
      }
      tscPrint("create stream finished");
    }
  }

  else {
    tscPrint("only insert data");
  }

  int num = 0;
  for (int r = 0; r < rowNum; ++r) {
    int64_t begin = get_curr_time();
    for (int i = 0; i < tableNum; ++i) {

      int len = sprintf(sql, "insert into db.t%d values(now", i);
      for (int j = 1; j < tableColumns - 1; j++) {
        len += sprintf(sql + len, ", %d", num);
      }
      sprintf(sql + len, ")");

      code = taos_query(taos, sql);
      if (code != 0) {
        tscPrint("TDengine error: %s\n", taos_errstr(taos));
        continue;
      }
    }

    int64_t end = get_curr_time();
    int64_t delta = tableInterval * 1000000 - (end - begin);
    if (delta < 0) delta = 0;
	++num;
    if (tableInterval != 0) {
      tscPrint("insert run %d times, sleep:%ld ms", num, delta / 1000);
    }
    else {
      if (num % 1000 == 0)
        tscPrint("insert run %d times, sleep:%ld ms", num, delta / 1000);
    }
      
    taosMsleep((int)(delta / 1000));
  }

  tscPrint("insert finished");
  return 0;
}

void taos_error(TAOS *con)
{
  tscPrint("TDengine error: %s\n", taos_errstr(con));
  //taos_close(con);
  //exit(1);
}
