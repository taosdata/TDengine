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
#include "tlog.h"

#define GREEN "\033[1;32m"
#define NC "\033[0m"

char    dbName[32] = "db";
char    stbName[64] = "st";
int32_t numOfThreads = 1;
int64_t numOfTables = 200000;
int64_t startOffset = 0;
int32_t createTable = 1;
int32_t insertData = 0;
int32_t batchNumOfTbl = 100;
int32_t batchNumOfRow = 1;
int32_t totalRowsOfPerTbl = 1;
int32_t numOfVgroups = 2;
int32_t showTablesFlag = 0;
int32_t queryFlag = 0;

int64_t startTimestamp = 1640966400000;  // 2020-01-01 00:00:00.000

typedef struct {
  int64_t   tableBeginIndex;
  int64_t   tableEndIndex;
  int32_t   threadIndex;
  char      dbName[32];
  char      stbName[64];
  float     createTableSpeed;
  float     insertDataSpeed;
  int64_t   startMs;
  int64_t   maxDelay;
  int64_t   minDelay;
  TdThread thread;
} SThreadInfo;

// void  parseArgument(int32_t argc, char *argv[]);
// void *threadFunc(void *param);
// void  createDbAndStb();

void createDbAndStb() {
  pPrint("start to create db and stable");
  char qstr[64000];

  TAOS *con = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (con == NULL) {
    pError("failed to connect to DB, reason:%s", taos_errstr(NULL));
    exit(1);
  }

  sprintf(qstr, "create database if not exists %s vgroups %d", dbName, numOfVgroups);
  TAOS_RES *pRes = taos_query(con, qstr);
  int32_t   code = taos_errno(pRes);
  if (code != 0) {
    pError("failed to create database:%s, sql:%s, code:%d reason:%s", dbName, qstr, taos_errno(pRes),
           taos_errstr(pRes));
    exit(0);
  }
  taos_free_result(pRes);

  sprintf(qstr, "use %s", dbName);
  pRes = taos_query(con, qstr);
  code = taos_errno(pRes);
  if (code != 0) {
    pError("failed to use db, code:%d reason:%s", taos_errno(pRes), taos_errstr(pRes));
    exit(0);
  }
  taos_free_result(pRes);

  sprintf(qstr, "create table if not exists %s (ts timestamp, i int) tags (j bigint)", stbName);
  pRes = taos_query(con, qstr);
  code = taos_errno(pRes);
  if (code != 0) {
    pError("failed to create stable, code:%d reason:%s", taos_errno(pRes), taos_errstr(pRes));
    exit(0);
  }
  taos_free_result(pRes);

  taos_close(con);
}

void printCreateProgress(SThreadInfo *pInfo, int64_t t) {
  int64_t endMs = taosGetTimestampMs();
  int64_t totalTables = t - pInfo->tableBeginIndex;
  float   seconds = (endMs - pInfo->startMs) / 1000.0;
  float   speed = totalTables / seconds;
  pInfo->createTableSpeed = speed;
  pPrint("thread:%d, %" PRId64 " tables created, time:%.2f sec, speed:%.1f tables/second, ", pInfo->threadIndex,
         totalTables, seconds, speed);
}

void printInsertProgress(SThreadInfo *pInfo, int64_t insertTotalRows) {
  int64_t endMs = taosGetTimestampMs();
  //int64_t totalTables = t - pInfo->tableBeginIndex;
  float   seconds = (endMs - pInfo->startMs) / 1000.0;
  float   speed = insertTotalRows / seconds;
  pInfo->insertDataSpeed = speed;
  pPrint("thread:%d, %" PRId64 " rows inserted, time:%.2f sec, speed:%.1f rows/second, ", pInfo->threadIndex,
         insertTotalRows, seconds, speed);
}

static int64_t getResult(TAOS_RES *tres) {
  TAOS_ROW row = taos_fetch_row(tres);
  if (row == NULL) {
    return 0;
  }

  int         num_fields = taos_num_fields(tres);
  TAOS_FIELD *fields = taos_fetch_fields(tres);
  int         precision = taos_result_precision(tres);

  int64_t numOfRows = 0;
  do {
    numOfRows++;
    row = taos_fetch_row(tres);
  } while (row != NULL);

  return numOfRows;
}

void showTables() {
  pPrint("start to show tables");
  char qstr[128];

  TAOS *con = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (con == NULL) {
    pError("failed to connect to DB, reason:%s", taos_errstr(NULL));
    exit(1);
  }

  snprintf(qstr, 128, "use %s", dbName);
  TAOS_RES *pRes = taos_query(con, qstr);
  int       code = taos_errno(pRes);
  if (code != 0) {
    pError("failed to use db, code:%d reason:%s", taos_errno(pRes), taos_errstr(pRes));
    exit(1);
  }
  taos_free_result(pRes);

  sprintf(qstr, "show tables");
  pRes = taos_query(con, qstr);
  code = taos_errno(pRes);
  if (code != 0) {
    pError("failed to show tables, code:%d reason:%s", taos_errno(pRes), taos_errstr(pRes));
    exit(0);
  }

  int64_t totalTableNum = getResult(pRes);
  taos_free_result(pRes);

  pPrint("%s database: %s, total %" PRId64 " tables %s", GREEN, dbName, totalTableNum, NC);

  taos_close(con);
}

void *threadFunc(void *param) {
  SThreadInfo *pInfo = (SThreadInfo *)param;
  char        *qstr = taosMemoryMalloc(batchNumOfTbl * batchNumOfRow * 128);
  int32_t      code = 0;

  TAOS *con = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (con == NULL) {
    pError("index:%d, failed to connect to DB, reason:%s", pInfo->threadIndex, taos_errstr(NULL));
    exit(1);
  }

  //pPrint("====before thread:%d, table range: %" PRId64 " - %" PRId64 "\n", pInfo->threadIndex, pInfo->tableBeginIndex,
  //       pInfo->tableEndIndex);

  pInfo->tableBeginIndex += startOffset;
  pInfo->tableEndIndex += startOffset;

  pPrint("====thread:%d, table range: %" PRId64 " - %" PRId64 "\n", pInfo->threadIndex, pInfo->tableBeginIndex, pInfo->tableEndIndex);

  sprintf(qstr, "use %s", pInfo->dbName);
  TAOS_RES *pRes = taos_query(con, qstr);
  taos_free_result(pRes);

  if (createTable) {
    int64_t curMs = 0;
    int64_t beginMs = taosGetTimestampMs();
    pInfo->startMs = beginMs;
    int64_t t = pInfo->tableBeginIndex;
    for (; t <= pInfo->tableEndIndex;) {
      // int64_t batch = (pInfo->tableEndIndex - t);
      // batch = TMIN(batch, batchNum);

      int32_t len = sprintf(qstr, "create table");
      for (int32_t i = 0; i < batchNumOfTbl;) {
        len += sprintf(qstr + len, " %s_t%" PRId64 " using %s tags(%" PRId64 ")", stbName, t, stbName, t);
        t++;
        i++;
        if (t > pInfo->tableEndIndex) {
          break;
        }
      }

      int64_t   startTs = taosGetTimestampUs();
      TAOS_RES *pRes = taos_query(con, qstr);
      code = taos_errno(pRes);
      if (code != 0) {
        pError("failed to create table reason:%s, sql: %s", tstrerror(code), qstr);
      }
      taos_free_result(pRes);
      int64_t endTs = taosGetTimestampUs();
      int64_t delay = endTs - startTs;
      // printf("==== %"PRId64" -  %"PRId64", %"PRId64"\n", startTs, endTs, delay);
      if (delay > pInfo->maxDelay) pInfo->maxDelay = delay;
      if (delay < pInfo->minDelay) pInfo->minDelay = delay;

      curMs = taosGetTimestampMs();
      if (curMs - beginMs > 10000) {
        beginMs = curMs;
        // printf("==== tableBeginIndex: %"PRId64", t: %"PRId64"\n", pInfo->tableBeginIndex, t);
        printCreateProgress(pInfo, t);
      }
    }
    printCreateProgress(pInfo, t);
  }

  if (insertData) {
  	int64_t insertTotalRows = 0;
    int64_t curMs = 0;
    int64_t beginMs = taosGetTimestampMs();
    pInfo->startMs = beginMs;
    int64_t t = pInfo->tableBeginIndex;
    for (; t <= pInfo->tableEndIndex; t++) {
      //printf("table name: %"PRId64"\n", t);
      int64_t ts = startTimestamp;
      for (int32_t i = 0; i < totalRowsOfPerTbl;) {
        int32_t len = sprintf(qstr, "insert into ");
        len += sprintf(qstr + len, "%s_t%" PRId64 " values ", stbName, t);
        for (int32_t j = 0; j < batchNumOfRow; j++) {
          len += sprintf(qstr + len, "(%" PRId64 ", 6666) ", ts++);
		  i++;
		  insertTotalRows++;
		  if (i >= totalRowsOfPerTbl) {
		  	break;
		  }
        }

        #if 1
        int64_t	startTs = taosGetTimestampUs();
        TAOS_RES *pRes = taos_query(con, qstr);
        code = taos_errno(pRes);
        if (code != 0) {
          pError("failed to insert %s_t%" PRId64 ", reason:%s", stbName, t, tstrerror(code));
        }
        taos_free_result(pRes);
        int64_t endTs = taosGetTimestampUs();
        int64_t delay = endTs - startTs;
        // printf("==== %"PRId64" -  %"PRId64", %"PRId64"\n", startTs, endTs, delay);
        if (delay > pInfo->maxDelay) pInfo->maxDelay = delay;
        if (delay < pInfo->minDelay) pInfo->minDelay = delay;
        
        curMs = taosGetTimestampMs();
        if (curMs - beginMs > 10000) {
          beginMs = curMs;
          // printf("==== tableBeginIndex: %"PRId64", t: %"PRId64"\n", pInfo->tableBeginIndex, t);
          printInsertProgress(pInfo, insertTotalRows);
        }
        #endif		
      }
    }	
    printInsertProgress(pInfo, insertTotalRows);    
  }

  taos_close(con);
  taosMemoryFree(qstr);
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
  printf("%s%s%s%" PRId64 "\n", indent, indent, "numOfTables, default is ", numOfTables);
  printf("%s%s\n", indent, "-g");
  printf("%s%s%s%" PRId64 "\n", indent, indent, "startOffset, default is ", startOffset);
  printf("%s%s\n", indent, "-v");
  printf("%s%s%s%d\n", indent, indent, "numOfVgroups, default is ", numOfVgroups);
  printf("%s%s\n", indent, "-a");
  printf("%s%s%s%d\n", indent, indent, "createTable, default is ", createTable);
  printf("%s%s\n", indent, "-i");
  printf("%s%s%s%d\n", indent, indent, "insertData, default is ", insertData);
  printf("%s%s\n", indent, "-b");
  printf("%s%s%s%d\n", indent, indent, "batchNumOfTbl, default is ", batchNumOfTbl);
  printf("%s%s\n", indent, "-w");
  printf("%s%s%s%d\n", indent, indent, "showTablesFlag, default is ", showTablesFlag);
  printf("%s%s\n", indent, "-q");
  printf("%s%s%s%d\n", indent, indent, "queryFlag, default is ", queryFlag);
  printf("%s%s\n", indent, "-l");
  printf("%s%s%s%d\n", indent, indent, "batchNumOfRow, default is ", batchNumOfRow);
  printf("%s%s\n", indent, "-r");
  printf("%s%s%s%d\n", indent, indent, "totalRowsOfPerTbl, default is ", totalRowsOfPerTbl);

  exit(EXIT_SUCCESS);
}

void parseArgument(int32_t argc, char *argv[]) {
  for (int32_t i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(0);
    } else if (strcmp(argv[i], "-d") == 0) {
      tstrncpy(dbName, argv[++i], sizeof(dbName));
    } else if (strcmp(argv[i], "-c") == 0) {
      tstrncpy(configDir, argv[++i], PATH_MAX);
    } else if (strcmp(argv[i], "-s") == 0) {
      tstrncpy(stbName, argv[++i], sizeof(stbName));
    } else if (strcmp(argv[i], "-t") == 0) {
      numOfThreads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-n") == 0) {
      numOfTables = atoll(argv[++i]);
    } else if (strcmp(argv[i], "-g") == 0) {
      startOffset = atoll(argv[++i]);
    } else if (strcmp(argv[i], "-v") == 0) {
      numOfVgroups = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-a") == 0) {
      createTable = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-i") == 0) {
      insertData = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-b") == 0) {
      batchNumOfTbl = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-l") == 0) {
      batchNumOfRow = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-r") == 0) {
      totalRowsOfPerTbl = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-w") == 0) {
      showTablesFlag = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-q") == 0) {
      queryFlag = atoi(argv[++i]);
    } else {
      pPrint("%s unknow para: %s %s", GREEN, argv[++i], NC);
    }
  }

  pPrint("%s dbName:%s %s", GREEN, dbName, NC);
  pPrint("%s stbName:%s %s", GREEN, stbName, NC);
  pPrint("%s configDir:%s %s", GREEN, configDir, NC);
  pPrint("%s numOfTables:%" PRId64 " %s", GREEN, numOfTables, NC);
  pPrint("%s startOffset:%" PRId64 " %s", GREEN, startOffset, NC);
  pPrint("%s numOfThreads:%d %s", GREEN, numOfThreads, NC);
  pPrint("%s numOfVgroups:%d %s", GREEN, numOfVgroups, NC);
  pPrint("%s createTable:%d %s", GREEN, createTable, NC);
  pPrint("%s insertData:%d %s", GREEN, insertData, NC);
  pPrint("%s batchNumOfTbl:%d %s", GREEN, batchNumOfTbl, NC);
  pPrint("%s batchNumOfRow:%d %s", GREEN, batchNumOfRow, NC);
  pPrint("%s totalRowsOfPerTbl:%d %s", GREEN, totalRowsOfPerTbl, NC);
  pPrint("%s showTablesFlag:%d %s", GREEN, showTablesFlag, NC);
  pPrint("%s queryFlag:%d %s", GREEN, queryFlag, NC);

  pPrint("%s start create table performace test %s", GREEN, NC);
}

int32_t main(int32_t argc, char *argv[]) {
  parseArgument(argc, argv);

  if (showTablesFlag) {
    showTables();
    return 0;
  }

  if (queryFlag) {
    // selectRowsFromTable();
    return 0;
  }

  if (createTable) {
    createDbAndStb();
  }

  pPrint("%d threads are spawned to create %" PRId64 " tables, offset is %" PRId64 " ", numOfThreads, numOfTables,
         startOffset);

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);
  SThreadInfo *pInfo = (SThreadInfo *)taosMemoryCalloc(numOfThreads, sizeof(SThreadInfo));

  // int64_t numOfTablesPerThread = numOfTables / numOfThreads;
  // numOfTables = numOfTablesPerThread * numOfThreads;

  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  int64_t a = numOfTables / numOfThreads;
  if (a < 1) {
    numOfThreads = numOfTables;
    a = 1;
  }

  int64_t b = 0;
  b = numOfTables % numOfThreads;

  int64_t tableFrom = 0;
  for (int32_t i = 0; i < numOfThreads; ++i) {
    pInfo[i].tableBeginIndex = tableFrom;
    pInfo[i].tableEndIndex = (i < b ? tableFrom + a : tableFrom + a - 1);
    tableFrom = pInfo[i].tableEndIndex + 1;
    pInfo[i].threadIndex = i;
    pInfo[i].minDelay = INT64_MAX;
    strcpy(pInfo[i].dbName, dbName);
    strcpy(pInfo[i].stbName, stbName);
    taosThreadCreate(&(pInfo[i].thread), &thattr, threadFunc, (void *)(pInfo + i));
  }

  taosMsleep(300);
  for (int32_t i = 0; i < numOfThreads; i++) {
    taosThreadJoin(pInfo[i].thread, NULL);
    taosThreadClear(&pInfo[i].thread);
  }

  int64_t maxDelay = 0;
  int64_t minDelay = INT64_MAX;

  float createTableSpeed = 0;
  for (int32_t i = 0; i < numOfThreads; ++i) {
    createTableSpeed += pInfo[i].createTableSpeed;

    if (pInfo[i].maxDelay > maxDelay) maxDelay = pInfo[i].maxDelay;
    if (pInfo[i].minDelay < minDelay) minDelay = pInfo[i].minDelay;
  }

  float insertDataSpeed = 0;
  for (int32_t i = 0; i < numOfThreads; ++i) {
    insertDataSpeed += pInfo[i].insertDataSpeed;
  }

  if (createTable) {
    pPrint("%s total %" PRId64 " tables, %.1f tables/second, threads:%d, maxDelay: %" PRId64 "us, minDelay: %" PRId64
           "us %s",
           GREEN, numOfTables, createTableSpeed, numOfThreads, maxDelay, minDelay, NC);
  }

  if (insertData) {
    pPrint("%s total %" PRId64 " tables, %.1f rows/second, threads:%d %s", GREEN, numOfTables, insertDataSpeed,
           numOfThreads, NC);
  }

  taosThreadAttrDestroy(&thattr);
  taosMemoryFree(pInfo);
}
