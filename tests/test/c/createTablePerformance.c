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
#include "taoserror.h"
#include "taos.h"
#include "tulog.h"
#include "tutil.h"
#include "tglobal.h"
#include "hash.h"

#define MAX_RANDOM_POINTS 20000
#define GREEN "\033[1;32m"
#define NC "\033[0m"

char    dbName[32] = "db";
char    stableName[64] = "st";
int32_t numOfThreads = 30;
int32_t numOfTables = 100000;
int32_t replica = 1;
int32_t numOfColumns = 2;
TAOS *  con = NULL;

typedef struct {
  int32_t   tableBeginIndex;
  int32_t   tableEndIndex;
  int32_t   threadIndex;
  char      dbName[32];
  char      stableName[64];
  float     createTableSpeed;
  pthread_t thread;
} SThreadInfo;

void  shellParseArgument(int argc, char *argv[]);
void *threadFunc(void *param);
void  createDbAndSTable();

int main(int argc, char *argv[]) {
  shellParseArgument(argc, argv);
  taos_init();
  createDbAndSTable();

  pPrint("%d threads are spawned to create table", numOfThreads);
  
  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  SThreadInfo *pInfo = (SThreadInfo *)calloc(numOfThreads, sizeof(SThreadInfo));

  int32_t numOfTablesPerThread = numOfTables / numOfThreads;
  numOfTables = numOfTablesPerThread * numOfThreads;
  for (int i = 0; i < numOfThreads; ++i) {
    pInfo[i].tableBeginIndex = i * numOfTablesPerThread;
    pInfo[i].tableEndIndex = (i + 1) * numOfTablesPerThread;
    pInfo[i].threadIndex = i;
    strcpy(pInfo[i].dbName, dbName);
    strcpy(pInfo[i].stableName, stableName);
    pthread_create(&(pInfo[i].thread), &thattr, threadFunc, (void *)(pInfo + i));
  }

  taosMsleep(300);
  for (int i = 0; i < numOfThreads; i++) {
    pthread_join(pInfo[i].thread, NULL);
  }

  float createTableSpeed = 0;
  for (int i = 0; i < numOfThreads; ++i) {
    createTableSpeed += pInfo[i].createTableSpeed;
  }

  pPrint("%s total speed:%.1f tables/second, threads:%d %s", GREEN, createTableSpeed, numOfThreads, NC);

  pthread_attr_destroy(&thattr);
  free(pInfo);
  taos_close(con);
}

void createDbAndSTable() {
  pPrint("start to create db and stable");
  char qstr[64000];
  
  con = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (con == NULL) {
    pError("failed to connect to DB, reason:%s", taos_errstr(con));
    exit(1);
  }

  sprintf(qstr, "create database if not exists %s replica %d", dbName, replica);
  TAOS_RES *pSql = taos_query(con, qstr);
  int32_t code = taos_errno(pSql);
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

  int len = sprintf(qstr, "create table if not exists %s(ts timestamp", stableName);
  for (int32_t f = 0; f < numOfColumns - 1; ++f) {
    len += sprintf(qstr + len, ", f%d double", f);
  }
  sprintf(qstr + len, ") tags(t int)");

  pSql = taos_query(con, qstr);
  code = taos_errno(pSql);
  if (code != 0) {
    pError("failed to create stable, code:%d reason:%s", taos_errno(con), taos_errstr(con));
    exit(0);
  }
  taos_free_result(pSql);
}

void *threadFunc(void *param) {
  SThreadInfo *pInfo = (SThreadInfo *)param;
  char qstr[65000];
  int  code;

  sprintf(qstr, "use %s", pInfo->dbName);
  TAOS_RES *pSql = taos_query(con, qstr);
  taos_free_result(pSql);

  int64_t startMs = taosGetTimestampMs();

  for (int32_t t = pInfo->tableBeginIndex; t < pInfo->tableEndIndex; ++t) {
    sprintf(qstr, "create table if not exists %s%d using %s tags(%d)", stableName, t, stableName, t);
    TAOS_RES *pSql = taos_query(con, qstr);
    code = taos_errno(pSql);
    if (code != 0) {
      pError("failed to create table %s%d, reason:%s", stableName, t, tstrerror(code));
    }
    taos_free_result(pSql);
  }

  float createTableSpeed = 0;
  for (int i = 0; i < numOfThreads; ++i) {
    createTableSpeed += pInfo[i].createTableSpeed;
  }

  int64_t endMs = taosGetTimestampMs();
  int32_t totalTables = pInfo->tableEndIndex - pInfo->tableBeginIndex;
  float   seconds = (endMs - startMs) / 1000.0;
  float   speed = totalTables / seconds;
  pInfo->createTableSpeed = speed;

  pPrint("thread:%d, time:%.2f sec, speed:%.1f tables/second, ", pInfo->threadIndex, seconds, speed);

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
  printf("%s%s%s%s\n", indent, indent, "The name of the super table to be created, default is ", stableName);
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s%d\n", indent, indent, "numOfThreads, default is ", numOfThreads);
  printf("%s%s\n", indent, "-n");
  printf("%s%s%s%d\n", indent, indent, "numOfTables, default is ", numOfTables);
  printf("%s%s\n", indent, "-r");
  printf("%s%s%s%d\n", indent, indent, "replica, default is ", replica);
  printf("%s%s\n", indent, "-columns");
  printf("%s%s%s%d\n", indent, indent, "numOfColumns, default is ", numOfColumns);
  
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
    } else if (strcmp(argv[i], "-t") == 0) {
      numOfThreads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-n") == 0) {
      numOfTables = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-r") == 0) {
      replica = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-columns") == 0) {
      numOfColumns = atoi(argv[++i]);
    } else {
    }
  }

  pPrint("%s dbName:%s %s", GREEN, dbName, NC);
  pPrint("%s stableName:%s %s", GREEN, stableName, NC);
  pPrint("%s configDir:%s %s", GREEN, configDir, NC);
  pPrint("%s numOfTables:%d %s", GREEN, numOfTables, NC);
  pPrint("%s numOfThreads:%d %s", GREEN, numOfThreads, NC);
  pPrint("%s numOfColumns:%d %s", GREEN, numOfColumns, NC);
  pPrint("%s replica:%d %s", GREEN, replica, NC);
  
  pPrint("%s start create table performace test %s", GREEN, NC);
}
