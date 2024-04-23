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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
// #include <unistd.h>

#include "taos.h"
#include "taoserror.h"
#include "tlog.h"

#define GREEN     "\033[1;32m"
#define NC        "\033[0m"
#define min(a, b) (((a) < (b)) ? (a) : (b))

#define MAX_SQL_STR_LEN (1024 * 1024)
#define MAX_ROW_STR_LEN (16 * 1024)

enum _RUN_MODE {
  TMQ_RUN_INSERT_AND_CONSUME,
  TMQ_RUN_ONLY_INSERT,
  TMQ_RUN_ONLY_CONSUME,
  TMQ_RUN_MODE_BUTT,
};

typedef struct {
  char    dbName[32];
  char    stbName[64];
  char    resultFileName[256];
  char    vnodeWalPath[256];
  int32_t numOfThreads;
  int32_t numOfTables;
  int32_t numOfVgroups;
  int32_t runMode;
  int32_t numOfColumn;
  double  ratio;
  int32_t batchNumOfRow;
  int32_t totalRowsOfPerTbl;
  int64_t startTimestamp;
  int32_t showMsgFlag;
  int32_t simCase;

  int32_t totalRowsOfT2;
} SConfInfo;

static SConfInfo g_stConfInfo = {
    "tmqdb",
    "stb",
    "./tmqResult.txt",  // output_file
    "",                 // /data2/dnode/data/vnode/vnode2/wal",
    1,                  // threads
    1,                  // tables
    1,                  // vgroups
    0,                  // run mode
    1,                  // columns
    1,                  // ratio
    1,                  // batch size
    10000,              // total rows for per table
    0,                  // 2020-01-01 00:00:00.000
    0,                  // show consume msg switch
    0,                  // if run in sim case
    10000,
};

char*     g_pRowValue = NULL;
TdFilePtr g_fp = NULL;

static void printHelp() {
  char indent[10] = "        ";
  printf("Used to test the performance while create table\n");

  printf("%s%s\n", indent, "-c");
  printf("%s%s%s%s\n", indent, indent, "Configuration directory, default is ", configDir);
  printf("%s%s\n", indent, "-d");
  printf("%s%s%s%s\n", indent, indent, "The name of the database to be created, default is ", g_stConfInfo.dbName);
  printf("%s%s\n", indent, "-s");
  printf("%s%s%s%s\n", indent, indent, "The name of the super table to be created, default is ", g_stConfInfo.stbName);
  printf("%s%s\n", indent, "-f");
  printf("%s%s%s%s\n", indent, indent, "The file of result, default is ", g_stConfInfo.resultFileName);
  printf("%s%s\n", indent, "-w");
  printf("%s%s%s%s\n", indent, indent, "The path of vnode of wal, default is ", g_stConfInfo.vnodeWalPath);
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s%d\n", indent, indent, "numOfThreads, default is ", g_stConfInfo.numOfThreads);
  printf("%s%s\n", indent, "-n");
  printf("%s%s%s%d\n", indent, indent, "numOfTables, default is ", g_stConfInfo.numOfTables);
  printf("%s%s\n", indent, "-v");
  printf("%s%s%s%d\n", indent, indent, "numOfVgroups, default is ", g_stConfInfo.numOfVgroups);
  printf("%s%s\n", indent, "-a");
  printf("%s%s%s%d\n", indent, indent, "runMode, default is ", g_stConfInfo.runMode);
  printf("%s%s\n", indent, "-l");
  printf("%s%s%s%d\n", indent, indent, "numOfColumn, default is ", g_stConfInfo.numOfColumn);
  printf("%s%s\n", indent, "-q");
  printf("%s%s%s%f\n", indent, indent, "ratio, default is ", g_stConfInfo.ratio);
  printf("%s%s\n", indent, "-b");
  printf("%s%s%s%d\n", indent, indent, "batchNumOfRow, default is ", g_stConfInfo.batchNumOfRow);
  printf("%s%s\n", indent, "-r");
  printf("%s%s%s%d\n", indent, indent, "totalRowsOfPerTbl, default is ", g_stConfInfo.totalRowsOfPerTbl);
  printf("%s%s\n", indent, "-m");
  printf("%s%s%s%" PRId64 "\n", indent, indent, "startTimestamp, default is ", g_stConfInfo.startTimestamp);
  printf("%s%s\n", indent, "-g");
  printf("%s%s%s%d\n", indent, indent, "showMsgFlag, default is ", g_stConfInfo.showMsgFlag);
  printf("%s%s\n", indent, "-sim");
  printf("%s%s%s%d\n", indent, indent, "simCase, default is ", g_stConfInfo.simCase);

  exit(EXIT_SUCCESS);
}

void parseArgument(int32_t argc, char* argv[]) {
  g_stConfInfo.startTimestamp = 1640966400000;  // 2020-01-01 00:00:00.000

  for (int32_t i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(0);
    } else if (strcmp(argv[i], "-d") == 0) {
      tstrncpy(g_stConfInfo.dbName, argv[++i], sizeof(g_stConfInfo.dbName));
    } else if (strcmp(argv[i], "-c") == 0) {
      tstrncpy(configDir, argv[++i], PATH_MAX);
    } else if (strcmp(argv[i], "-s") == 0) {
      tstrncpy(g_stConfInfo.stbName, argv[++i], sizeof(g_stConfInfo.stbName));
    } else if (strcmp(argv[i], "-w") == 0) {
      tstrncpy(g_stConfInfo.vnodeWalPath, argv[++i], sizeof(g_stConfInfo.vnodeWalPath));
    } else if (strcmp(argv[i], "-f") == 0) {
      tstrncpy(g_stConfInfo.resultFileName, argv[++i], sizeof(g_stConfInfo.resultFileName));
    } else if (strcmp(argv[i], "-t") == 0) {
      g_stConfInfo.numOfThreads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-n") == 0) {
      g_stConfInfo.numOfTables = atoll(argv[++i]);
    } else if (strcmp(argv[i], "-v") == 0) {
      g_stConfInfo.numOfVgroups = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-a") == 0) {
      g_stConfInfo.runMode = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-b") == 0) {
      g_stConfInfo.batchNumOfRow = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-r") == 0) {
      g_stConfInfo.totalRowsOfPerTbl = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-l") == 0) {
      g_stConfInfo.numOfColumn = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-q") == 0) {
      g_stConfInfo.ratio = atof(argv[++i]);
    } else if (strcmp(argv[i], "-m") == 0) {
      g_stConfInfo.startTimestamp = atol(argv[++i]);
    } else if (strcmp(argv[i], "-g") == 0) {
      g_stConfInfo.showMsgFlag = atol(argv[++i]);
    } else if (strcmp(argv[i], "-sim") == 0) {
      g_stConfInfo.simCase = atol(argv[++i]);
    } else {
      printf("%s unknow para: %s %s", GREEN, argv[++i], NC);
      exit(-1);
    }
  }

  g_stConfInfo.totalRowsOfT2 = g_stConfInfo.totalRowsOfPerTbl * g_stConfInfo.ratio;

#if 0
  pPrint("%s configDir:%s %s", GREEN, configDir, NC);
  pPrint("%s dbName:%s %s", GREEN, g_stConfInfo.dbName, NC);
  pPrint("%s stbName:%s %s", GREEN, g_stConfInfo.stbName, NC);
  pPrint("%s resultFileName:%s %s", GREEN, g_stConfInfo.resultFileName, NC);
  pPrint("%s vnodeWalPath:%s %s", GREEN, g_stConfInfo.vnodeWalPath, NC);
  pPrint("%s numOfTables:%d %s", GREEN, g_stConfInfo.numOfTables, NC);
  pPrint("%s numOfThreads:%d %s", GREEN, g_stConfInfo.numOfThreads, NC);
  pPrint("%s numOfVgroups:%d %s", GREEN, g_stConfInfo.numOfVgroups, NC);
  pPrint("%s runMode:%d %s", GREEN, g_stConfInfo.runMode, NC);
  pPrint("%s ratio:%f %s", GREEN, g_stConfInfo.ratio, NC);
  pPrint("%s numOfColumn:%d %s", GREEN, g_stConfInfo.numOfColumn, NC);
  pPrint("%s batchNumOfRow:%d %s", GREEN, g_stConfInfo.batchNumOfRow, NC);
  pPrint("%s totalRowsOfPerTbl:%d %s", GREEN, g_stConfInfo.totalRowsOfPerTbl, NC);
  pPrint("%s totalRowsOfT2:%d %s", GREEN, g_stConfInfo.totalRowsOfT2, NC);
  pPrint("%s startTimestamp:%" PRId64" %s", GREEN, g_stConfInfo.startTimestamp, NC);
  pPrint("%s showMsgFlag:%d %s", GREEN, g_stConfInfo.showMsgFlag, NC);
#endif
}

static int running = 1;
/*static void msg_process(tmq_message_t* message) { tmqShowMsg(message); }*/

// calc dir size (not include itself 4096Byte)
int64_t getDirectorySize(char* dir) {
  TdDirPtr      pDir;
  TdDirEntryPtr pDirEntry;
  int64_t       totalSize = 0;

  if ((pDir = taosOpenDir(dir)) == NULL) {
    fprintf(stderr, "Cannot open dir: %s\n", dir);
    return -1;
  }

  // lstat(dir, &statbuf);
  // totalSize+=statbuf.st_size;

  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char  subdir[1024];
    char* fileName = taosGetDirEntryName(pDirEntry);
    sprintf(subdir, "%s/%s", dir, fileName);

    // printf("===d_name: %s\n", entry->d_name);
    if (taosIsDir(subdir)) {
      if (strcmp(".", fileName) == 0 || strcmp("..", fileName) == 0) {
        continue;
      }

      int64_t subDirSize = getDirectorySize(subdir);
      totalSize += subDirSize;
    } else if (0 == strcmp(strchr(fileName, '.'), ".log")) {  // only calc .log file size, and not include .idx file
      int64_t file_size = 0;
      taosStatFile(subdir, &file_size, NULL, NULL);
      totalSize += file_size;
    }
  }

  taosCloseDir(&pDir);
  return totalSize;
}

int queryDB(TAOS* taos, char* command) {
  TAOS_RES* pRes = taos_query(taos, command);
  int       code = taos_errno(pRes);
  if (code != 0) {
    pError("failed to reason:%s, sql: %s", tstrerror(code), command);
    taos_free_result(pRes);
    return -1;
  }
  taos_free_result(pRes);
  return 0;
}

int32_t init_env() {
  char sqlStr[1024] = {0};

  TAOS* pConn = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  sprintf(sqlStr, "create database if not exists %s vgroups %d", g_stConfInfo.dbName, g_stConfInfo.numOfVgroups);
  TAOS_RES* pRes = taos_query(pConn, sqlStr);
  if (taos_errno(pRes) != 0) {
    printf("error in create db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  sprintf(sqlStr, "use %s", g_stConfInfo.dbName);
  pRes = taos_query(pConn, sqlStr);
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  // create row value
  g_pRowValue = (char*)taosMemoryCalloc(1, g_stConfInfo.numOfColumn * 16 + 128);
  if (NULL == g_pRowValue) {
    return -1;
  }

  int32_t dataLen = 0;
  int32_t sqlLen = 0;
  sqlLen += sprintf(sqlStr + sqlLen, "create stable if not exists %s (ts timestamp, ", g_stConfInfo.stbName);
  for (int32_t i = 0; i < g_stConfInfo.numOfColumn; i++) {
    if (i == g_stConfInfo.numOfColumn - 1) {
      sqlLen += sprintf(sqlStr + sqlLen, "c%d int) ", i);
      memcpy(g_pRowValue + dataLen, "66778899", strlen("66778899"));
      dataLen += strlen("66778899");
    } else {
      sqlLen += sprintf(sqlStr + sqlLen, "c%d int, ", i);
      memcpy(g_pRowValue + dataLen, "66778899, ", strlen("66778899, "));
      dataLen += strlen("66778899, ");
    }
  }
  sqlLen += sprintf(sqlStr + sqlLen, "tags (t0 int)");

  pRes = taos_query(pConn, sqlStr);
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table %s, reason:%s\n", g_stConfInfo.stbName, taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  for (int32_t i = 0; i < g_stConfInfo.numOfTables; i++) {
    sprintf(sqlStr, "create table if not exists %s%d using %s tags(1)", g_stConfInfo.stbName, i, g_stConfInfo.stbName);
    pRes = taos_query(pConn, sqlStr);
    if (taos_errno(pRes) != 0) {
      printf("failed to create child table %s%d, reason:%s\n", g_stConfInfo.stbName, i, taos_errstr(pRes));
      return -1;
    }
    taos_free_result(pRes);
  }

  // const char* sql = "select * from tu1";
  sprintf(sqlStr, "create topic test_stb_topic_1 as select ts,c0 from %s", g_stConfInfo.stbName);
  /*pRes = tmq_create_topic(pConn, "test_stb_topic_1", sqlStr, strlen(sqlStr));*/
  pRes = taos_query(pConn, sqlStr);
  if (taos_errno(pRes) != 0) {
    printf("failed to create topic test_stb_topic_1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
  taos_close(pConn);
  return 0;
}

tmq_t* build_consumer() {
#if 0
  char sqlStr[1024] = {0};
  
  TAOS* pConn = taos_connect(NULL, "root", "taosdata", NULL, 0);
  assert(pConn != NULL);

  sprintf(sqlStr, "use %s", g_stConfInfo.dbName);
  TAOS_RES* pRes = taos_query(pConn, sqlStr);
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);
#endif

  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg2");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "td.connect.db", g_stConfInfo.dbName);
  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);
  return tmq;
}

tmq_list_t* build_topic_list() {
  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "test_stb_topic_1");
  return topic_list;
}

void sync_consume_loop(tmq_t* tmq, tmq_list_t* topics) {
  static const int MIN_COMMIT_COUNT = 1000;

  int     msg_count = 0;
  int32_t err;

  if ((err = tmq_subscribe(tmq, topics))) {
    fprintf(stderr, "%% Failed to start consuming topics: %s\n", tmq_err2str(err));
    return;
  }

  while (running) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 1);
    if (tmqmessage) {
      /*msg_process(tmqmessage);*/
      taos_free_result(tmqmessage);

      if ((++msg_count % MIN_COMMIT_COUNT) == 0) tmq_commit_sync(tmq, NULL);
    }
  }

  err = tmq_consumer_close(tmq);
  if (err)
    fprintf(stderr, "%% Failed to close consumer: %s\n", tmq_err2str(err));
  else
    fprintf(stderr, "%% Consumer closed\n");
}

void perf_loop(tmq_t* tmq, tmq_list_t* topics, int32_t totalMsgs, int64_t walLogSize) {
  int32_t err;

  if ((err = tmq_subscribe(tmq, topics))) {
    fprintf(stderr, "%% Failed to start consuming topics: %s\n", tmq_err2str(err));
    printf("subscribe err\n");
    return;
  }
  /*taosSsleep(3);*/
  int32_t batchCnt = 0;
  int64_t startTime = taosGetTimestampUs();
  while (running) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 3000);
    if (tmqmessage) {
      batchCnt++;
      if (0 != g_stConfInfo.showMsgFlag) {
        /*msg_process(tmqmessage);*/
      }
      taos_free_result(tmqmessage);
    } else {
      break;
    }
  }
  int64_t endTime = taosGetTimestampUs();
  double  consumeTime = (double)(endTime - startTime) / 1000000;

  if (batchCnt != totalMsgs) {
    printf("%s inserted msgs: %d and consume msgs: %d mismatch %s", GREEN, totalMsgs, batchCnt, NC);
    /*exit(-1);*/
  }

  if (0 == g_stConfInfo.simCase) {
    printf("consume result: msgs: %d, time used:%.3f second\n", batchCnt, consumeTime);
  } else {
    printf("{consume success: %d}", totalMsgs);
  }
  taosFprintfFile(g_fp, "|%10d    |   %10.3f    |  %8.2f  |  %10.2f|    %10.2f    |\n", batchCnt, consumeTime,
                  (double)batchCnt / consumeTime, (double)walLogSize / (1024 * 1024.0) / consumeTime,
                  (double)walLogSize / 1024.0 / batchCnt);

  err = tmq_consumer_close(tmq);
  if (err) {
    fprintf(stderr, "%% Failed to close consumer: %s\n", tmq_err2str(err));
    exit(-1);
  }
}

// sync insertion
int32_t syncWriteData() {
  TAOS* pConn = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  char sqlStr[1024] = {0};
  sprintf(sqlStr, "use %s", g_stConfInfo.dbName);
  TAOS_RES* pRes = taos_query(pConn, sqlStr);
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  char* buffer = NULL;
  buffer = (char*)taosMemoryMalloc(MAX_SQL_STR_LEN);
  if (NULL == buffer) {
    return -1;
  }

  int32_t totalMsgs = 0;

  int64_t time_counter = g_stConfInfo.startTimestamp;
  for (int i = 0; i < g_stConfInfo.totalRowsOfPerTbl;) {
    for (int tID = 0; tID <= g_stConfInfo.numOfTables - 1; tID++) {
      int     inserted = i;
      int64_t tmp_time = time_counter;

      int32_t data_len = 0;
      data_len += sprintf(buffer + data_len, "insert into %s%d values", g_stConfInfo.stbName, tID);
      int k;
      for (k = 0; k < g_stConfInfo.batchNumOfRow;) {
        data_len += sprintf(buffer + data_len, "(%" PRId64 ", %s) ", tmp_time++, g_pRowValue);
        inserted++;
        k++;

        if (inserted >= g_stConfInfo.totalRowsOfPerTbl) {
          break;
        }

        if (data_len > MAX_SQL_STR_LEN - MAX_ROW_STR_LEN) {
          break;
        }
      }

      int code = queryDB(pConn, buffer);
      if (0 != code) {
        fprintf(stderr, "insert data error!\n");
        taosMemoryFreeClear(buffer);
        return -1;
      }

      totalMsgs++;

      if (tID == g_stConfInfo.numOfTables - 1) {
        i = inserted;
        time_counter = tmp_time;
      }
    }
  }
  taosMemoryFreeClear(buffer);
  return totalMsgs;
}

// sync insertion
int32_t syncWriteDataByRatio() {
  TAOS* pConn = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  char sqlStr[1024] = {0};
  sprintf(sqlStr, "use %s", g_stConfInfo.dbName);
  TAOS_RES* pRes = taos_query(pConn, sqlStr);
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  char* buffer = NULL;
  buffer = (char*)taosMemoryMalloc(MAX_SQL_STR_LEN);
  if (NULL == buffer) {
    return -1;
  }

  int32_t totalMsgs = 0;

  int32_t insertedOfT1 = 0;
  int32_t insertedOfT2 = 0;

  int64_t tsOfT1 = g_stConfInfo.startTimestamp;
  int64_t tsOfT2 = g_stConfInfo.startTimestamp;
  int64_t tmp_time;

  for (;;) {
    if ((insertedOfT1 >= g_stConfInfo.totalRowsOfPerTbl) && (insertedOfT2 >= g_stConfInfo.totalRowsOfT2)) {
      break;
    }

    for (int tID = 0; tID <= g_stConfInfo.numOfTables - 1; tID++) {
      if (0 == tID) {
        tmp_time = tsOfT1;
        if (insertedOfT1 >= g_stConfInfo.totalRowsOfPerTbl) {
          continue;
        }
      } else if (1 == tID) {
        tmp_time = tsOfT2;
        if (insertedOfT2 >= g_stConfInfo.totalRowsOfT2) {
          continue;
        }
      }

      int32_t data_len = 0;
      data_len += sprintf(buffer + data_len, "insert into %s%d values", g_stConfInfo.stbName, tID);
      int k;
      for (k = 0; k < g_stConfInfo.batchNumOfRow;) {
        data_len += sprintf(buffer + data_len, "(%" PRId64 ", %s) ", tmp_time++, g_pRowValue);
        k++;
        if (0 == tID) {
          insertedOfT1++;
          if (insertedOfT1 >= g_stConfInfo.totalRowsOfPerTbl) {
            break;
          }
        } else if (1 == tID) {
          insertedOfT2++;
          if (insertedOfT2 >= g_stConfInfo.totalRowsOfT2) {
            break;
          }
        }

        if (data_len > MAX_SQL_STR_LEN - MAX_ROW_STR_LEN) {
          break;
        }
      }

      int code = queryDB(pConn, buffer);
      if (0 != code) {
        fprintf(stderr, "insert data error!\n");
        taosMemoryFreeClear(buffer);
        return -1;
      }

      if (0 == tID) {
        tsOfT1 = tmp_time;
      } else if (1 == tID) {
        tsOfT2 = tmp_time;
      }

      totalMsgs++;
    }
  }
  pPrint("expect insert rows: T1[%d] T2[%d], actual insert rows: T1[%d] T2[%d]\n", g_stConfInfo.totalRowsOfPerTbl,
         g_stConfInfo.totalRowsOfT2, insertedOfT1, insertedOfT2);
  taosMemoryFreeClear(buffer);
  return totalMsgs;
}

void printParaIntoFile() {
  // FILE *fp = fopen(g_stConfInfo.resultFileName, "a");
  TdFilePtr pFile =
      taosOpenFile(g_stConfInfo.resultFileName, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND | TD_FILE_STREAM);
  if (NULL == pFile) {
    fprintf(stderr, "Failed to open %s for save result\n", g_stConfInfo.resultFileName);
    exit(-1);
  }
  g_fp = pFile;

  time_t    tTime = taosGetTimestampSec();
  struct tm tm;
  taosLocalTime(&tTime, &tm, NULL);

  taosFprintfFile(pFile, "###################################################################\n");
  taosFprintfFile(pFile, "# configDir:                %s\n", configDir);
  taosFprintfFile(pFile, "# dbName:                   %s\n", g_stConfInfo.dbName);
  taosFprintfFile(pFile, "# stbName:                  %s\n", g_stConfInfo.stbName);
  taosFprintfFile(pFile, "# vnodeWalPath:             %s\n", g_stConfInfo.vnodeWalPath);
  taosFprintfFile(pFile, "# numOfTables:              %d\n", g_stConfInfo.numOfTables);
  taosFprintfFile(pFile, "# numOfThreads:             %d\n", g_stConfInfo.numOfThreads);
  taosFprintfFile(pFile, "# numOfVgroups:             %d\n", g_stConfInfo.numOfVgroups);
  taosFprintfFile(pFile, "# runMode:                  %d\n", g_stConfInfo.runMode);
  taosFprintfFile(pFile, "# ratio:                    %f\n", g_stConfInfo.ratio);
  taosFprintfFile(pFile, "# numOfColumn:              %d\n", g_stConfInfo.numOfColumn);
  taosFprintfFile(pFile, "# batchNumOfRow:            %d\n", g_stConfInfo.batchNumOfRow);
  taosFprintfFile(pFile, "# totalRowsOfPerTbl:        %d\n", g_stConfInfo.totalRowsOfPerTbl);
  taosFprintfFile(pFile, "# totalRowsOfT2:            %d\n", g_stConfInfo.totalRowsOfT2);
  taosFprintfFile(pFile, "# Test time:                %d-%02d-%02d %02d:%02d:%02d\n", tm.tm_year + 1900, tm.tm_mon + 1,
                  tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
  taosFprintfFile(pFile, "###################################################################\n");
  taosFprintfFile(pFile,
                  "|-------------------------------insert "
                  "info-----------------------------|--------------------------------consume "
                  "info---------------------------------|\n");
  taosFprintfFile(pFile,
                  "|batch size| insert msgs | insert time(s) |   msgs/s   | walLogSize(MB) | consume msgs | consume "
                  "time(s) |   msgs/s   |    MB/s    | avg msg size(KB) |\n");
  taosFprintfFile(g_fp, "|%10d", g_stConfInfo.batchNumOfRow);
}

int main(int32_t argc, char* argv[]) {
  parseArgument(argc, argv);
  printParaIntoFile();

  int64_t walLogSize = 0;

  int code;
  code = init_env();
  if (code != 0) {
    fprintf(stderr, "%% init_env error!\n");
    return -1;
  }

  int32_t totalMsgs = 0;

  if (g_stConfInfo.runMode != TMQ_RUN_ONLY_CONSUME) {
    int64_t startTs = taosGetTimestampUs();
    if (1 == g_stConfInfo.ratio) {
      totalMsgs = syncWriteData();
    } else {
      totalMsgs = syncWriteDataByRatio();
    }

    if (totalMsgs <= 0) {
      pError("inset data error!\n");
      return -1;
    }
    int64_t endTs = taosGetTimestampUs();
    int64_t delay = endTs - startTs;

    int32_t totalRows = 0;
    if (1 == g_stConfInfo.ratio) {
      totalRows = g_stConfInfo.totalRowsOfPerTbl * g_stConfInfo.numOfTables;
    } else {
      totalRows = g_stConfInfo.totalRowsOfPerTbl * (1 + g_stConfInfo.ratio);
    }

    float seconds = delay / 1000000.0;
    float rowsSpeed = totalRows / seconds;
    float msgsSpeed = totalMsgs / seconds;

    if ((0 == g_stConfInfo.simCase) && (strlen(g_stConfInfo.vnodeWalPath))) {
      walLogSize = getDirectorySize(g_stConfInfo.vnodeWalPath);
      if (walLogSize <= 0) {
        printf("%s size incorrect!", g_stConfInfo.vnodeWalPath);
        exit(-1);
      } else {
        pPrint(".log file size in vnode2/wal: %.3f MBytes\n", (double)walLogSize / (1024 * 1024.0));
      }
    }

    if (0 == g_stConfInfo.simCase) {
      pPrint("insert result: %d rows, %d msgs, time:%.3f sec, speed:%.1f rows/second, %.1f msgs/second\n", totalRows,
             totalMsgs, seconds, rowsSpeed, msgsSpeed);
    }
    taosFprintfFile(g_fp, "|%10d   |   %10.3f   |  %8.2f  |   %10.3f   ", totalMsgs, seconds, msgsSpeed,
                    (double)walLogSize / (1024 * 1024.0));
  }

  if (g_stConfInfo.runMode == TMQ_RUN_ONLY_INSERT) {
    return 0;
  }

  tmq_t*      tmq = build_consumer();
  tmq_list_t* topic_list = build_topic_list();
  if ((NULL == tmq) || (NULL == topic_list)) {
    return -1;
  }

  perf_loop(tmq, topic_list, totalMsgs, walLogSize);

  taosMemoryFreeClear(g_pRowValue);
  taosFprintfFile(g_fp, "\n");
  taosCloseFile(&g_fp);
  return 0;
}
