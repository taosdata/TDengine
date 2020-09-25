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
#include "osTime.h"

#define MAX_RANDOM_POINTS 20000
#define GREEN "\033[1;32m"
#define NC "\033[0m"

typedef struct {
  int64_t   rowsPerTable;
  int64_t   pointsPerTable;
  int64_t   tableBeginIndex;
  int64_t   tableEndIndex;
  int       threadIndex;
  char      dbName[32];
  char      stableName[64];
  pthread_t thread;
} SInfo;

void *syncTest(void *param);
void  generateRandomPoints();
void  shellParseArgument(int argc, char *argv[]);
void  createDbAndTable();
void  insertData();

int32_t randomData[MAX_RANDOM_POINTS];
int64_t rowsPerTable = 1000000;
int64_t pointsPerTable = 1;
int64_t numOfThreads = 10;
int64_t numOfTablesPerThread = 100;
char    dbName[32] = "db";
char    stableName[64] = "st";
int64_t totalUs = 0;
int64_t reqNum = 0;
int64_t maxUs = 0;
int64_t minUs = 100000000;

int main(int argc, char *argv[]) {
  shellParseArgument(argc, argv);
  generateRandomPoints();
  taos_init();
  createDbAndTable();
  insertData();
  int64_t avgUs = totalUs / reqNum;
  pError("%s totalUs:%ld, avgUs:%ld maxUs:%ld minUs:%ld reqNum:%ld %s\n", GREEN, totalUs, avgUs, maxUs, minUs, reqNum, NC);
}

int32_t query(void *con, char *qstr) {
  int64_t begin = taosGetTimestampUs();

  TAOS_RES *pSql = taos_query(con, qstr);
  int32_t   code = taos_errno(pSql);
  if (code != 0) {
    pError("failed to exec sql:%s, code:%d reason:%s", qstr, taos_errno(con), taos_errstr(con));
    exit(0);
  }
  taos_free_result(pSql);

  int64_t us = taosGetTimestampUs() - begin;
  maxUs = MAX(us, maxUs);
  minUs = MIN(us, minUs);
  atomic_add_fetch_64(&totalUs, us);
  atomic_add_fetch_64(&reqNum, 1);
  if (reqNum > 100000) {
    int64_t avgUs = totalUs / reqNum;
    if (us > avgUs * 100) {
      pError("sql:%s", qstr);
      pError("%s totalUs:%ld, avgUs:%ld maxUs:%ld minUs:%ld reqNum:%ld %s\n", GREEN, totalUs, avgUs, maxUs, minUs,
             reqNum, NC);
      taosMsleep(1000);
      exit(0);
    }
  }

  return code;
}

void createDbAndTable() {
  pPrint("start to create table");

  TAOS *         con;
  struct timeval systemTime;
  int64_t        st, et;
  char           qstr[64000];

  char     fqdn[TSDB_FQDN_LEN];
  uint16_t port;

  taosGetFqdnPortFromEp(tsFirst, fqdn, &port);

  con = taos_connect(fqdn, "root", "taosdata", NULL, port);
  if (con == NULL) {
    pError("failed to connect to DB, reason:%s", taos_errstr(con));
    exit(1);
  }

  sprintf(qstr, "create database if not exists %s", dbName);
  if (query(con, qstr)) {
    pError("failed to create database:%s, code:%d reason:%s", dbName, taos_errno(con), taos_errstr(con));
    exit(0);
  }

  sprintf(qstr, "use %s", dbName);
  if (query(con, qstr)) {
    pError("failed to use db, code:%d reason:%s", taos_errno(con), taos_errstr(con));
    exit(0);
  }

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec * 1000000 + systemTime.tv_usec;
  int64_t totalTables = numOfTablesPerThread * numOfThreads;
    
  if (strcmp(stableName, "no") != 0) {
    int len = sprintf(qstr, "create table if not exists %s(ts timestamp", stableName);
    for (int64_t f = 0; f < pointsPerTable; ++f) {
      len += sprintf(qstr + len, ", f%ld double", f);
    }
    sprintf(qstr + len, ") tags(t int)");

    if (query(con, qstr)) {
      pError("failed to create stable, code:%d reason:%s", taos_errno(con), taos_errstr(con));
      exit(0);
    }
    
    for (int64_t t = 0; t < totalTables; ++t) {
      sprintf(qstr, "create table if not exists %s%ld using %s tags(%ld)", stableName, t, stableName, t);
      if (query(con, qstr)) {
        pError("failed to create table %s%" PRId64 ", reason:%s", stableName, t, taos_errstr(con));
        exit(0);
      }
    }
  } else {
    for (int64_t t = 0; t < totalTables; ++t) {
      int len = sprintf(qstr, "create table if not exists %s%ld(ts timestamp", stableName, t);
      for (int64_t f = 0; f < pointsPerTable; ++f) {
        len += sprintf(qstr + len, ", f%ld double", f);
      }
      sprintf(qstr + len, ")");

      if (query(con, qstr)) {
        pError("failed to create table %s%ld, reason:%s", stableName, t, taos_errstr(con));
        exit(0);
      }
    }
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec * 1000000 + systemTime.tv_usec;
  pPrint("%.1f seconds to create %ld tables", (et - st) / 1000.0 / 1000.0, totalTables);
}

void insertData() {
  struct timeval systemTime;
  int64_t        st, et;

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec * 1000000 + systemTime.tv_usec;

  pPrint("%" PRId64 " threads are spawned to import data", numOfThreads);

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  SInfo *pInfo = (SInfo *)malloc(sizeof(SInfo) * numOfThreads);

  // Start threads to write
  for (int i = 0; i < numOfThreads; ++i) {
    pInfo[i].rowsPerTable = rowsPerTable;
    pInfo[i].pointsPerTable = pointsPerTable;
    pInfo[i].tableBeginIndex = i * numOfTablesPerThread;
    pInfo[i].tableEndIndex = (i + 1) * numOfTablesPerThread;
    pInfo[i].threadIndex = i;
    strcpy(pInfo[i].dbName, dbName);
    strcpy(pInfo[i].stableName, stableName);
    pthread_create(&(pInfo[i].thread), &thattr, syncTest, (void *)(pInfo + i));
  }

  taosMsleep(300);
  for (int i = 0; i < numOfThreads; i++) {
    pthread_join(pInfo[i].thread, NULL);
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec * 1000000 + systemTime.tv_usec;
  double seconds = (et - st) / 1000.0 / 1000.0;

  int64_t totalTables = numOfTablesPerThread * numOfThreads;
  int64_t totalRows = totalTables * rowsPerTable;
  int64_t totalPoints = totalTables * rowsPerTable * pointsPerTable;
  double  speedOfRows = totalRows / seconds;
  double  speedOfPoints = totalPoints / seconds;

  pPrint(
      "%sall threads:%ld finished, use %.1lf seconds, tables:%.ld rows:%ld points:%ld, speed RowsPerSecond:%.1lf "
      "PointsPerSecond:%.1lf%s",
      GREEN, numOfThreads, seconds, totalTables, totalRows, totalPoints, speedOfRows, speedOfPoints, NC);

  pPrint("threads exit");

  pthread_attr_destroy(&thattr);
  free(pInfo);
}

void *syncTest(void *param) {
  TAOS *         con;
  SInfo *        pInfo = (SInfo *)param;
  struct timeval systemTime;
  int64_t        st, et;
  char           qstr[65000];
  int            maxBytes = 60000;

  pPrint("thread:%d, start to run", pInfo->threadIndex);
  char     fqdn[TSDB_FQDN_LEN];
  uint16_t port;

  taosGetFqdnPortFromEp(tsFirst, fqdn, &port);

  con = taos_connect(fqdn, "root", "taosdata", NULL, port);
  if (con == NULL) {
    pError("index:%d, failed to connect to DB, reason:%s", pInfo->threadIndex, taos_errstr(con));
    exit(1);
  }

  sprintf(qstr, "use %s", pInfo->dbName);
  query(con, qstr);

  gettimeofday(&systemTime, NULL);
  st = systemTime.tv_sec * 1000000 + systemTime.tv_usec;

  int64_t start = 1587225600000;
  int64_t interval = 1000;  // 1000 ms

  char *sql = qstr;
  char  inserStr[] = "import into";
  int   len = sprintf(sql, "%s", inserStr);

  for (int64_t table = pInfo->tableBeginIndex; table < pInfo->tableEndIndex; ++table) {
    len += sprintf(sql + len, " %s%ld values", pInfo->stableName, table);
    for (int64_t row = 0; row < pInfo->rowsPerTable; row++) {
      len += sprintf(sql + len, "(%ld", start - row * interval);
      for (int64_t point = 0; point < pInfo->pointsPerTable; ++point) {
        len += sprintf(sql + len, ",%d", randomData[(123 * table + 456 * row + 789 * point) % MAX_RANDOM_POINTS]);
        // len += sprintf(sql + len, ",%ld", row);
      }
      len += sprintf(sql + len, ")");
      if (len > maxBytes) {
        if (query(con, qstr)) {
          pError("thread:%d, failed to import table:%s%ld row:%ld, reason:%s", pInfo->threadIndex, pInfo->stableName,
                 table, row, taos_errstr(con));
        }

        // "insert into"
        len = sprintf(sql, "%s", inserStr);

        // "insert into st1 values"
        if (row != pInfo->rowsPerTable - 1) {
          len += sprintf(sql + len, " %s%ld values", pInfo->stableName, table);
        }
      }
    }
  }

  if (len != strlen(inserStr)) {
    query(con, qstr);
  }

  gettimeofday(&systemTime, NULL);
  et = systemTime.tv_sec * 1000000 + systemTime.tv_usec;
  int64_t totalTables = pInfo->tableEndIndex - pInfo->tableBeginIndex;
  int64_t totalRows = totalTables * pInfo->rowsPerTable;
  int64_t totalPoints = totalRows * pInfo->pointsPerTable;
  pPrint("thread:%d, import finished, use %.2f seconds, tables:%ld rows:%ld points:%ld", pInfo->threadIndex,
         (et - st) / 1000.0 / 1000.0, totalTables, totalRows, totalPoints);

  return NULL;
}

void generateRandomPoints() {
  for (int r = 0; r < MAX_RANDOM_POINTS; ++r) {
    randomData[r] = rand() % 1000;
  }
}

void printHelp() {
  char indent[10] = "        ";
  printf("Used to test the performance of TDengine\n After writing all the data in one table, start the next table\n");

  printf("%s%s\n", indent, "-d");
  printf("%s%s%s%s\n", indent, indent, "The name of the database to be created, default is ", dbName);
  printf("%s%s\n", indent, "-s");
  printf("%s%s%s%s%s\n", indent, indent, "The name of the super table to be created, default is ", stableName, ", if 'no' then create normal table");
  printf("%s%s\n", indent, "-c");
  printf("%s%s%s%s\n", indent, indent, "Configuration directory, default is ", configDir);
  printf("%s%s\n", indent, "-r");
  printf("%s%s%s%ld\n", indent, indent, "Number of records to write to each table, default is ", rowsPerTable);
  printf("%s%s\n", indent, "-p");
  printf("%s%s%s%" PRId64 "\n", indent, indent, "Number of columns per table, default is ", pointsPerTable);
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s%" PRId64 "\n", indent, indent, "Number of threads to be used, default is ", numOfThreads);
  printf("%s%s\n", indent, "-n");
  printf("%s%s%s%" PRId64 "\n", indent, indent, "Number of tables per thread, default is ", numOfTablesPerThread);

  exit(EXIT_SUCCESS);
}

void shellParseArgument(int argc, char *argv[]) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(0);
    } else if (strcmp(argv[i], "-d") == 0) {
      strcpy(dbName, argv[++i]);
    } else if (strcmp(argv[i], "-c") == 0) {
      strcpy(configDir, argv[++i]);
    } else if (strcmp(argv[i], "-s") == 0) {
      strcpy(stableName, argv[++i]);
    } else if (strcmp(argv[i], "-r") == 0) {
      rowsPerTable = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-p") == 0) {
      pointsPerTable = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-t") == 0) {
      numOfThreads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-n") == 0) {
      numOfTablesPerThread = atoi(argv[++i]);
    } else {
    }
  }

  pPrint("%srowsPerTable:%" PRId64 "%s", GREEN, rowsPerTable, NC);
  pPrint("%spointsPerTable:%" PRId64 "%s", GREEN, pointsPerTable, NC);
  pPrint("%snumOfThreads:%" PRId64 "%s", GREEN, numOfThreads, NC);
  pPrint("%snumOfTablesPerThread:%" PRId64 "%s", GREEN, numOfTablesPerThread, NC);
  pPrint("%sdbName:%s%s", GREEN, dbName, NC);
  pPrint("%stableName:%s%s", GREEN, stableName, NC);
  pPrint("%sstart to run%s", GREEN, NC);
}
