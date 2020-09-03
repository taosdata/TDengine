/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "os.h"
#include "taos.h"
#include "tulog.h"
#include "ttimer.h"
#include "tutil.h"
#include "tglobal.h"

#define MAX_RANDOM_POINTS 20000
#define GREEN "\033[1;32m"
#define NC "\033[0m"

typedef struct {
  int64_t   startTimeMs;
  int64_t   endTimeMs;
  int       threadIndex;
  pthread_t thread;
} SInfo;

void *syncTest(void *param);
void  shellParseArgument(int argc, char *argv[]);
void  queryData();

int   numOfThreads = 10;
int   useGlobalConn = 1;
int   requestPerThread = 10000;
char  requestSql[10240] = "show dnodes";
TAOS *globalConn;

int main(int argc, char *argv[]) {
  shellParseArgument(argc, argv);
  taos_init();
  queryData();
}

void queryData() {
  struct timeval systemTime;
  int64_t        st, et;
  char           fqdn[TSDB_FQDN_LEN];
  uint16_t       port;

  if (useGlobalConn) {
    taosGetFqdnPortFromEp(tsFirst, fqdn, &port);

    globalConn = taos_connect(fqdn, "root", "taosdata", NULL, port);
    if (globalConn == NULL) {
      pError("failed to connect to DB, reason:%s", taos_errstr(globalConn));
      exit(1);
    }
  }

  pPrint("%d threads are spawned to query", numOfThreads);

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec * 1000000 + systemTime.tv_usec;

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  SInfo *pInfo = (SInfo *)malloc(sizeof(SInfo) * numOfThreads);

  // Start threads to write
  for (int i = 0; i < numOfThreads; ++i) {
    pInfo[i].threadIndex = i;
    pthread_create(&(pInfo[i].thread), &thattr, syncTest, (void *)(pInfo + i));
  }

  taosMsleep(300);
  for (int i = 0; i < numOfThreads; i++) {
    pthread_join(pInfo[i].thread, NULL);
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec * 1000000 + systemTime.tv_usec;
  double totalTimeMs = (et - st) / 1000.0;

  int   totalReq = requestPerThread * numOfThreads;
  float rspTime = totalTimeMs / requestPerThread;
  float qps = totalReq / (totalTimeMs / 1000);

  pPrint("%s threads:%d, totalTime %.1fms totalReq:%d qps:%.1f rspTime:%.3fms %s", GREEN, numOfThreads, totalTimeMs,
         totalReq, qps, rspTime, NC);

  pthread_attr_destroy(&thattr);
  free(pInfo);
}

void *syncTest(void *param) {
  TAOS *   con;
  SInfo *  pInfo = (SInfo *)param;
  char     fqdn[TSDB_FQDN_LEN];
  uint16_t port;

  if (useGlobalConn) {
    pPrint("thread:%d, start to run use global connection", pInfo->threadIndex);
    con = globalConn;
  } else {
    pPrint("thread:%d, start to run, and create new conn", pInfo->threadIndex);
    taosGetFqdnPortFromEp(tsFirst, fqdn, &port);

    con = taos_connect(fqdn, "root", "taosdata", NULL, port);
    if (con == NULL) {
      pError("index:%d, failed to connect to DB, reason:%s", pInfo->threadIndex, taos_errstr(con));
      exit(1);
    }
  }

  for (int i = 0; i < requestPerThread; ++i) {
    void *tres = taos_query(con, requestSql);

    TAOS_ROW row = taos_fetch_row(tres);
    if (row == NULL) {
      taos_free_result(tres);
      exit(0);
    }

    do {
      row = taos_fetch_row(tres);
    } while (row != NULL);

    taos_free_result(tres);
  }
  return NULL;
}

void printHelp() {
  char indent[10] = "        ";
  printf("Used to test the query performance of TDengine\n");

  printf("%s%s\n", indent, "-c");
  printf("%s%s%s%s\n", indent, indent, "Configuration directory, default is ", configDir);
  printf("%s%s\n", indent, "-s");
  printf("%s%s%s%s\n", indent, indent, "The sql to be executed, default is ", requestSql);
  printf("%s%s\n", indent, "-r");
  printf("%s%s%s%d\n", indent, indent, "Request per thread, default is ", requestPerThread);
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s%d\n", indent, indent, "Number of threads to be used, default is ", numOfThreads);
  printf("%s%s\n", indent, "-g");
  printf("%s%s%s%d\n", indent, indent, "Whether to share connections between threads, default is ", useGlobalConn);

  exit(EXIT_SUCCESS);
}

void shellParseArgument(int argc, char *argv[]) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(0);
    } else if (strcmp(argv[i], "-c") == 0) {
      strcpy(configDir, argv[++i]);
    } else if (strcmp(argv[i], "-s") == 0) {
      strcpy(requestSql, argv[++i]);
    } else if (strcmp(argv[i], "-r") == 0) {
      requestPerThread = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-t") == 0) {
      numOfThreads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-g") == 0) {
      useGlobalConn = atoi(argv[++i]);
    } else {
    }
  }

  pPrint("%s sql:%s %s", GREEN, requestSql, NC);
  pPrint("%s requestPerThread:%d %s", GREEN, requestPerThread, NC);
  pPrint("%s numOfThreads:%d %s", GREEN, numOfThreads, NC);
  pPrint("%s useGlobalConn:%d %s", GREEN, useGlobalConn, NC);
  pPrint("%s start to run %s", GREEN, NC);
}
