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
#include "taoserror.h"
#include "ulog.h"

#define GREEN "\033[1;32m"
#define NC "\033[0m"

char    dbName[32] = "db";
char    stbName[64] = "st";
int32_t numOfThreads = 1;
int32_t numOfTables = 10000;
int32_t createTable = 1;
int32_t insertData = 0;
int32_t batchNum = 1;
int32_t numOfVgroups = 2;

typedef struct {
  int32_t   tableBeginIndex;
  int32_t   tableEndIndex;
  int32_t   threadIndex;
  char      dbName[32];
  char      stbName[64];
  float     createTableSpeed;
  float     insertDataSpeed;
  int64_t   startMs;
  pthread_t thread;
} SThreadInfo;

void  parseArgument(int32_t argc, char *argv[]);
void *threadFunc(void *param);
void  createDbAndStb();

int32_t main(int32_t argc, char *argv[]) {
  parseArgument(argc, argv);
  createDbAndStb();

  pPrint("%d threads are spawned to create %d tables", numOfThreads, numOfThreads);

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  SThreadInfo *pInfo = (SThreadInfo *)calloc(numOfThreads, sizeof(SThreadInfo));

  int32_t numOfTablesPerThread = numOfTables / numOfThreads;
  numOfTables = numOfTablesPerThread * numOfThreads;
  for (int32_t i = 0; i < numOfThreads; ++i) {
    pInfo[i].tableBeginIndex = i * numOfTablesPerThread;
    pInfo[i].tableEndIndex = (i + 1) * numOfTablesPerThread;
    pInfo[i].threadIndex = i;
    strcpy(pInfo[i].dbName, dbName);
    strcpy(pInfo[i].stbName, stbName);
    pthread_create(&(pInfo[i].thread), &thattr, threadFunc, (void *)(pInfo + i));
  }

  taosMsleep(300);
  for (int32_t i = 0; i < numOfThreads; i++) {
    pthread_join(pInfo[i].thread, NULL);
  }

  float createTableSpeed = 0;
  for (int32_t i = 0; i < numOfThreads; ++i) {
    createTableSpeed += pInfo[i].createTableSpeed;
  }

  float insertDataSpeed = 0;
  for (int32_t i = 0; i < numOfThreads; ++i) {
    insertDataSpeed += pInfo[i].insertDataSpeed;
  }

  pPrint("%s total %.1f tables/second, threads:%d %s", GREEN, createTableSpeed, numOfThreads, NC);
  pPrint("%s total %.1f rows/second, threads:%d %s", GREEN, insertDataSpeed, numOfThreads, NC);

  pthread_attr_destroy(&thattr);
  free(pInfo);
}

void createDbAndStb() {
  pPrint("start to create db and stable");
  char qstr[64000];

  TAOS *con = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (con == NULL) {
    pError("failed to connect to DB, reason:%s", taos_errstr(con));
    exit(1);
  }

  sprintf(qstr, "create database if not exists %s vgroups %d", dbName, numOfVgroups);
  TAOS_RES *pSql = taos_query(con, qstr);
  int32_t   code = taos_errno(pSql);
  if (code != 0) {
    pError("failed to create database:%s, sql:%s, code:%d reason:%s", dbName, qstr, taos_errno(con), taos_errstr(con));
    exit(0);
  }
  taos_free_result(pSql);

  sprintf(qstr, "use %s", dbName);
  pSql = taos_query(con, qstr);
  code = taos_errno(pSql);
  if (code != 0) {
    pError("failed to use db, code:%d reason:%s", taos_errno(con), taos_errstr(con));
    exit(0);
  }
  taos_free_result(pSql);

  sprintf(qstr, "create table %s (ts timestamp, i int) tags (j int)", stbName);
  pSql = taos_query(con, qstr);
  code = taos_errno(pSql);
  if (code != 0) {
    pError("failed to use db, code:%d reason:%s", taos_errno(con), taos_errstr(con));
    exit(0);
  }
  taos_free_result(pSql);

  taos_close(con);
}

void printCreateProgress(SThreadInfo *pInfo, int32_t t) {
  int64_t endMs = taosGetTimestampMs();
  int32_t totalTables = t - pInfo->tableBeginIndex;
  float   seconds = (endMs - pInfo->startMs) / 1000.0;
  float   speed = totalTables / seconds;
  pInfo->createTableSpeed = speed;
  pPrint("thread:%d, %d tables created, time:%.2f sec, speed:%.1f tables/second, ", pInfo->threadIndex, totalTables,
         seconds, speed);
}

void printInsertProgress(SThreadInfo *pInfo, int32_t t) {
  int64_t endMs = taosGetTimestampMs();
  int32_t totalTables = t - pInfo->tableBeginIndex;
  float   seconds = (endMs - pInfo->startMs) / 1000.0;
  float   speed = totalTables / seconds;
  pInfo->insertDataSpeed = speed;
  pPrint("thread:%d, %d rows inserted, time:%.2f sec, speed:%.1f rows/second, ", pInfo->threadIndex, totalTables,
         seconds, speed);
}

void *threadFunc(void *param) {
  SThreadInfo *pInfo = (SThreadInfo *)param;
  char        *qstr = malloc(2000 * 1000);
  int32_t      code = 0;

  TAOS *con = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (con == NULL) {
    pError("index:%d, failed to connect to DB, reason:%s", pInfo->threadIndex, taos_errstr(con));
    exit(1);
  }

  sprintf(qstr, "use %s", pInfo->dbName);
  TAOS_RES *pSql = taos_query(con, qstr);
  taos_free_result(pSql);

  if (createTable) {
    pInfo->startMs = taosGetTimestampMs();
    for (int32_t t = pInfo->tableBeginIndex; t < pInfo->tableEndIndex; ++t) {
      int32_t batch = (pInfo->tableEndIndex - t);
      batch = MIN(batch, batchNum);

      int32_t len = sprintf(qstr, "create table");
      for (int32_t i = 0; i < batch; ++i) {
        len += sprintf(qstr + len, " t%d using %s tags(%d)", t + i, stbName, t + i);
      }

      TAOS_RES *pSql = taos_query(con, qstr);
      code = taos_errno(pSql);
      if (code != 0) {
        pError("failed to create table t%d, reason:%s", t, tstrerror(code));
      }
      taos_free_result(pSql);

      if (t % 1000 == 0) {
        printCreateProgress(pInfo, t);
      }
      t += (batch - 1);
    }
    printCreateProgress(pInfo, pInfo->tableEndIndex);
  }

  if (insertData) {
    pInfo->startMs = taosGetTimestampMs();
    for (int32_t t = pInfo->tableBeginIndex; t < pInfo->tableEndIndex; ++t) {
      int32_t batch = (pInfo->tableEndIndex - t);
      batch = MIN(batch, batchNum);

      int32_t len = sprintf(qstr, "insert into");
      for (int32_t i = 0; i < batch; ++i) {
        len += sprintf(qstr + len, " t%d values(now, %d)", t + i, t + i);
      }

      TAOS_RES *pSql = taos_query(con, qstr);
      code = taos_errno(pSql);
      if (code != 0) {
        pError("failed to insert table t%d, reason:%s", t, tstrerror(code));
      }
      taos_free_result(pSql);

      if (t % 100000 == 0) {
        printInsertProgress(pInfo, t);
      }
      t += (batch - 1);
    }
    printInsertProgress(pInfo, pInfo->tableEndIndex);
  }

  taos_close(con);
  free(qstr);
  return 0;
}

void printHelp() {
  char indent[10] = "        ";
  printf("Used to test the performance while create table\n");

  printf("%s%s\n", indent, "-c");
  printf("%s%s%s%s\n", indent, indent, "Configuration directory, default is ", configDir);
  printf("%s%s\n", indent, "-d");
  printf("%s%s%s%s\n", indent, indent, "The name of the database to be created, default is ", dbName);
  printf("%s%s\n", indent, "-s");
  printf("%s%s%s%s\n", indent, indent, "The name of the super table to be created, default is ", stbName);
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s%d\n", indent, indent, "numOfThreads, default is ", numOfThreads);
  printf("%s%s\n", indent, "-n");
  printf("%s%s%s%d\n", indent, indent, "numOfTables, default is ", numOfTables);
  printf("%s%s\n", indent, "-v");
  printf("%s%s%s%d\n", indent, indent, "numOfVgroups, default is ", numOfVgroups);
  printf("%s%s\n", indent, "-a");
  printf("%s%s%s%d\n", indent, indent, "createTable, default is ", createTable);
  printf("%s%s\n", indent, "-i");
  printf("%s%s%s%d\n", indent, indent, "insertData, default is ", insertData);
  printf("%s%s\n", indent, "-b");
  printf("%s%s%s%d\n", indent, indent, "batchNum, default is ", batchNum);

  exit(EXIT_SUCCESS);
}

void parseArgument(int32_t argc, char *argv[]) {
  for (int32_t i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(0);
    } else if (strcmp(argv[i], "-d") == 0) {
      strcpy(dbName, argv[++i]);
    } else if (strcmp(argv[i], "-c") == 0) {
      strcpy(configDir, argv[++i]);
    } else if (strcmp(argv[i], "-s") == 0) {
      strcpy(stbName, argv[++i]);
    } else if (strcmp(argv[i], "-t") == 0) {
      numOfThreads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-n") == 0) {
      numOfTables = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-n") == 0) {
      numOfVgroups = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-a") == 0) {
      createTable = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-i") == 0) {
      insertData = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-b") == 0) {
      batchNum = atoi(argv[++i]);
    } else {
    }
  }

  pPrint("%s dbName:%s %s", GREEN, dbName, NC);
  pPrint("%s stbName:%s %s", GREEN, stbName, NC);
  pPrint("%s configDir:%s %s", GREEN, configDir, NC);
  pPrint("%s numOfTables:%d %s", GREEN, numOfTables, NC);
  pPrint("%s numOfThreads:%d %s", GREEN, numOfThreads, NC);
  pPrint("%s numOfVgroups:%d %s", GREEN, numOfVgroups, NC);
  pPrint("%s createTable:%d %s", GREEN, createTable, NC);
  pPrint("%s insertData:%d %s", GREEN, insertData, NC);
  pPrint("%s batchNum:%d %s", GREEN, batchNum, NC);

  pPrint("%s start create table performace test %s", GREEN, NC);
}
