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
void  insertData();

int64_t numOfThreads = 100;
char sql[10240] = "show dnodes";
int32_t loopTimes = 1000; 

int main(int argc, char *argv[]) {
  shellParseArgument(argc, argv);
  taos_init();
  insertData();
}

void insertData() {
  struct timeval systemTime;
  int64_t        st, et;

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec * 1000000 + systemTime.tv_usec;

  pPrint("%" PRId64 " threads are spawned to query", numOfThreads);

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
  double mseconds = (et - st) / 1000.0;

  int64_t request = loopTimes * numOfThreads;
  float avg = mseconds / request;;
  float qps = 1000 / avg * numOfThreads;
  
  pPrint(
      "%sall threads:%ld finished, use %.1lf ms, qps:%f, avg:%f %s",
      GREEN, numOfThreads, mseconds, qps, avg, NC);

  pPrint("threads exit");

  pthread_attr_destroy(&thattr);
  free(pInfo);
}

void *syncTest(void *param) {
  TAOS *         con;
  SInfo *        pInfo = (SInfo *)param;
  struct timeval systemTime;
  
  pPrint("thread:%d, start to run", pInfo->threadIndex);
  char     fqdn[TSDB_FQDN_LEN];
  uint16_t port;

  taosGetFqdnPortFromEp(tsFirst, fqdn, &port);

  con = taos_connect(fqdn, "root", "taosdata", NULL, port);
  if (con == NULL) {
    pError("index:%d, failed to connect to DB, reason:%s", pInfo->threadIndex, taos_errstr(con));
    exit(1);
  }

  for (int i = 0; i < loopTimes; ++i) {
    void *tres = taos_query(con, sql);

    TAOS_ROW row = taos_fetch_row(tres);
    if (row == NULL) {
      taos_free_result(tres);
      exit(0);
    }

    do {
      row = taos_fetch_row(tres);
    } while( row != NULL);

    taos_free_result(tres);
  }

  gettimeofday(&systemTime, NULL);
  
  return NULL;
}

void printHelp() {
  char indent[10] = "        ";
  printf("Used to test the query performance of TDengine\n");

  printf("%s%s\n", indent, "-c");
  printf("%s%s%s%s\n", indent, indent, "Configuration directory, default is ", configDir);
  printf("%s%s\n", indent, "-s");
  printf("%s%s%s%s\n", indent, indent, "The sql to be executed, default is %s", sql);
  printf("%s%s\n", indent, "-l");
  printf("%s%s%s%d\n", indent, indent, "Loop Times per thread, default is ", loopTimes);
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s%" PRId64 "\n", indent, indent, "Number of threads to be used, default is ", numOfThreads);
  
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
      strcpy(sql, argv[++i]);
    } else if (strcmp(argv[i], "-l") == 0) {
      loopTimes = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-t") == 0) {
      numOfThreads = atoi(argv[++i]);
    } else {
    }
  }

  pPrint("%ssql:%s%s", GREEN, sql, NC);
  pPrint("%sloopTImes:%d%s", GREEN, loopTimes, NC);
  pPrint("%snumOfThreads:%" PRId64 "%s", GREEN, numOfThreads, NC);
   pPrint("%sstart to run%s", GREEN, NC);
}
