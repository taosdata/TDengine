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

#include "taos.h"
#include "taoserror.h"
#include "tlog.h"

#define GREEN     "\033[1;32m"
#define NC        "\033[0m"
#define min(a, b) (((a) < (b)) ? (a) : (b))

#define MAX_SQL_STR_LEN         (1024 * 1024)
#define MAX_ROW_STR_LEN         (16 * 1024)
#define MAX_CONSUMER_THREAD_CNT (16)
#define MAX_VGROUP_CNT          (32)

typedef struct {
  TdThread thread;
  int32_t  consumerId;

  int32_t ifManualCommit;
  // int32_t  autoCommitIntervalMs;  // 1000 ms
  // char     autoCommit[8];         // true, false
  // char     autoOffsetRest[16];    // none, earliest, latest

  int32_t ifCheckData;
  int64_t expectMsgCnt;

  int64_t consumeMsgCnt;
  int64_t consumeRowCnt;
  int32_t checkresult;

  char topicString[1024];
  char keyString[1024];

  int32_t numOfTopic;
  char    topics[32][64];

  int32_t numOfKey;
  char    key[32][64];
  char    value[32][64];

  tmq_t*      tmq;
  tmq_list_t* topicList;

  int32_t numOfVgroups;
  int32_t rowsOfPerVgroups[MAX_VGROUP_CNT][2];  // [i][0]: vgroup id, [i][1]: rows of consume
  int64_t ts;

} SThreadInfo;

typedef struct {
  // input from argvs
  char        cdbName[32];
  char        dbName[32];
  int32_t     showMsgFlag;
  int32_t     showRowFlag;
  int32_t     saveRowFlag;
  int32_t     consumeDelay;  // unit s
  int32_t     numOfThread;
  SThreadInfo stThreads[MAX_CONSUMER_THREAD_CNT];
} SConfInfo;

static SConfInfo g_stConfInfo;
TdFilePtr        g_fp = NULL;
static int       running = 1;

// char* g_pRowValue = NULL;
// TdFilePtr g_fp = NULL;

static void printHelp() {
  char indent[10] = "        ";
  printf("Used to test the tmq feature with sim cases\n");

  printf("%s%s\n", indent, "-c");
  printf("%s%s%s%s\n", indent, indent, "Configuration directory, default is ", configDir);
  printf("%s%s\n", indent, "-d");
  printf("%s%s%s\n", indent, indent, "The name of the database for cosumer, no default ");
  printf("%s%s\n", indent, "-g");
  printf("%s%s%s%d\n", indent, indent, "showMsgFlag, default is ", g_stConfInfo.showMsgFlag);
  printf("%s%s\n", indent, "-r");
  printf("%s%s%s%d\n", indent, indent, "showRowFlag, default is ", g_stConfInfo.showRowFlag);
  printf("%s%s\n", indent, "-s");
  printf("%s%s%s%d\n", indent, indent, "saveRowFlag, default is ", g_stConfInfo.saveRowFlag);
  printf("%s%s\n", indent, "-y");
  printf("%s%s%s%d\n", indent, indent, "consume delay, default is s", g_stConfInfo.consumeDelay);
  exit(EXIT_SUCCESS);
}

char* getCurrentTimeString(char* timeString) {
  time_t    tTime = taosGetTimestampSec();
  struct tm tm = *taosLocalTime(&tTime, NULL);
  sprintf(timeString, "%d-%02d-%02d %02d:%02d:%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
          tm.tm_min, tm.tm_sec);

  return timeString;
}

void initLogFile() {
  char filename[256];
  char tmpString[128];

  sprintf(filename, "%s/../log/tmqlog_%s.txt", configDir, getCurrentTimeString(tmpString));
  // sprintf(filename, "%s/../log/tmqlog.txt", configDir);
#ifdef WINDOWS
  for (int i = 2; i < sizeof(filename); i++) {
    if (filename[i] == ':') filename[i] = '-';
    if (filename[i] == '\0') break;
  }
#endif
  TdFilePtr pFile = taosOpenFile(filename, TD_FILE_TEXT | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_STREAM);
  if (NULL == pFile) {
    fprintf(stderr, "Failed to open %s for save result\n", filename);
    exit(-1);
  }
  g_fp = pFile;
}

void saveConfigToLogFile() {
  taosFprintfFile(g_fp, "###################################################################\n");
  taosFprintfFile(g_fp, "# configDir:           %s\n", configDir);
  taosFprintfFile(g_fp, "# dbName:              %s\n", g_stConfInfo.dbName);
  taosFprintfFile(g_fp, "# cdbName:             %s\n", g_stConfInfo.cdbName);
  taosFprintfFile(g_fp, "# showMsgFlag:         %d\n", g_stConfInfo.showMsgFlag);
  taosFprintfFile(g_fp, "# showRowFlag:         %d\n", g_stConfInfo.showRowFlag);
  taosFprintfFile(g_fp, "# saveRowFlag:         %d\n", g_stConfInfo.saveRowFlag);
  taosFprintfFile(g_fp, "# consumeDelay:        %d\n", g_stConfInfo.consumeDelay);
  taosFprintfFile(g_fp, "# numOfThread:         %d\n", g_stConfInfo.numOfThread);

  for (int32_t i = 0; i < g_stConfInfo.numOfThread; i++) {
    taosFprintfFile(g_fp, "# consumer %d info:\n", g_stConfInfo.stThreads[i].consumerId);
    // taosFprintfFile(g_fp, "  auto commit:              %s\n", g_stConfInfo.stThreads[i].autoCommit);
    // taosFprintfFile(g_fp, "  auto commit interval ms:  %d\n", g_stConfInfo.stThreads[i].autoCommitIntervalMs);
    // taosFprintfFile(g_fp, "  auto offset rest:         %s\n", g_stConfInfo.stThreads[i].autoOffsetRest);
    taosFprintfFile(g_fp, "  Topics: ");
    for (int j = 0; j < g_stConfInfo.stThreads[i].numOfTopic; j++) {
      taosFprintfFile(g_fp, "%s, ", g_stConfInfo.stThreads[i].topics[j]);
    }
    taosFprintfFile(g_fp, "\n");
    taosFprintfFile(g_fp, "  Key: ");
    for (int k = 0; k < g_stConfInfo.stThreads[i].numOfKey; k++) {
      taosFprintfFile(g_fp, "%s:%s, ", g_stConfInfo.stThreads[i].key[k], g_stConfInfo.stThreads[i].value[k]);
    }
    taosFprintfFile(g_fp, "\n");
    taosFprintfFile(g_fp, "  expect rows: %d\n", g_stConfInfo.stThreads[i].expectMsgCnt);
  }

  char tmpString[128];
  taosFprintfFile(g_fp, "# Test time:                %s\n", getCurrentTimeString(tmpString));
  taosFprintfFile(g_fp, "###################################################################\n");
}

void parseArgument(int32_t argc, char* argv[]) {
  memset(&g_stConfInfo, 0, sizeof(SConfInfo));
  g_stConfInfo.showMsgFlag = 0;
  g_stConfInfo.showRowFlag = 0;
  g_stConfInfo.saveRowFlag = 0;
  g_stConfInfo.consumeDelay = 5;

  for (int32_t i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(0);
    } else if (strcmp(argv[i], "-d") == 0) {
      strcpy(g_stConfInfo.dbName, argv[++i]);
    } else if (strcmp(argv[i], "-w") == 0) {
      strcpy(g_stConfInfo.cdbName, argv[++i]);
    } else if (strcmp(argv[i], "-c") == 0) {
      strcpy(configDir, argv[++i]);
    } else if (strcmp(argv[i], "-g") == 0) {
      g_stConfInfo.showMsgFlag = atol(argv[++i]);
    } else if (strcmp(argv[i], "-r") == 0) {
      g_stConfInfo.showRowFlag = atol(argv[++i]);
    } else if (strcmp(argv[i], "-s") == 0) {
      g_stConfInfo.saveRowFlag = atol(argv[++i]);
    } else if (strcmp(argv[i], "-y") == 0) {
      g_stConfInfo.consumeDelay = atol(argv[++i]);
    } else {
      pError("%s unknow para: %s %s", GREEN, argv[++i], NC);
      exit(-1);
    }
  }

  initLogFile();

  taosFprintfFile(g_fp, "====parseArgument() success\n");

#if 1
  pPrint("%s configDir:%s %s", GREEN, configDir, NC);
  pPrint("%s dbName:%s %s", GREEN, g_stConfInfo.dbName, NC);
  pPrint("%s cdbName:%s %s", GREEN, g_stConfInfo.cdbName, NC);
  pPrint("%s consumeDelay:%d %s", GREEN, g_stConfInfo.consumeDelay, NC);
  pPrint("%s showMsgFlag:%d %s", GREEN, g_stConfInfo.showMsgFlag, NC);
  pPrint("%s showRowFlag:%d %s", GREEN, g_stConfInfo.showRowFlag, NC);
  pPrint("%s saveRowFlag:%d %s", GREEN, g_stConfInfo.saveRowFlag, NC);
#endif
}

void splitStr(char** arr, char* str, const char* del) {
  char* s = strtok(str, del);
  while (s != NULL) {
    *arr++ = s;
    s = strtok(NULL, del);
  }
}

void ltrim(char* str) {
  if (str == NULL || *str == '\0') {
    return;
  }
  int   len = 0;
  char* p = str;
  while (*p != '\0' && isspace(*p)) {
    ++p;
    ++len;
  }
  memmove(str, p, strlen(str) - len + 1);
  // return str;
}

void addRowsToVgroupId(SThreadInfo* pInfo, int32_t vgroupId, int32_t rows) {
  int32_t i;
  for (i = 0; i < pInfo->numOfVgroups; i++) {
    if (vgroupId == pInfo->rowsOfPerVgroups[i][0]) {
      pInfo->rowsOfPerVgroups[i][1] += rows;
      return;
    }
  }

  pInfo->rowsOfPerVgroups[pInfo->numOfVgroups][0] = vgroupId;
  pInfo->rowsOfPerVgroups[pInfo->numOfVgroups][1] += rows;
  pInfo->numOfVgroups++;

  taosFprintfFile(g_fp, "consume id %d, add one new vogroup id: %d\n", pInfo->consumerId, vgroupId);
  if (pInfo->numOfVgroups > MAX_VGROUP_CNT) {
    taosFprintfFile(g_fp, "====consume id %d, vgroup num %d over than 32. new vgroupId: %d\n", pInfo->consumerId,
                    pInfo->numOfVgroups, vgroupId);
    taosCloseFile(&g_fp);
    exit(-1);
  }
}

int32_t saveConsumeContentToTbl(SThreadInfo* pInfo, char* buf) {
  char sqlStr[1100] = {0};

  if (strlen(buf) > 1024) {
    taosFprintfFile(g_fp, "The length of one row[%d] is overflow 1024\n", strlen(buf));
    taosCloseFile(&g_fp);
    exit(-1);
  }

  TAOS* pConn = taos_connect(NULL, "root", "taosdata", NULL, 0);
  assert(pConn != NULL);

  sprintf(sqlStr, "insert into %s.content_%d values (%" PRId64 ", \'%s\')", g_stConfInfo.cdbName, pInfo->consumerId,
          pInfo->ts++, buf);
  TAOS_RES* pRes = taos_query(pConn, sqlStr);
  if (taos_errno(pRes) != 0) {
    pError("error in insert consume result, reason:%s\n", taos_errstr(pRes));
    taosFprintfFile(g_fp, "error in insert consume result, reason:%s\n", taos_errstr(pRes));
    taosCloseFile(&g_fp);
    taos_free_result(pRes);
    exit(-1);
  }

  taos_free_result(pRes);

  return 0;
}

static int32_t msg_process(TAOS_RES* msg, SThreadInfo* pInfo, int32_t msgIndex) {
  char    buf[1024];
  int32_t totalRows = 0;

  // printf("topic: %s\n", tmq_get_topic_name(msg));
  int32_t vgroupId = tmq_get_vgroup_id(msg);

  taosFprintfFile(g_fp, "msg index:%" PRId64 ", consumerId: %d\n", msgIndex, pInfo->consumerId);
  // taosFprintfFile(g_fp, "topic: %s, vgroupId: %d, tableName: %s\n", tmq_get_topic_name(msg), vgroupId,
  // tmq_get_table_name(msg));
  taosFprintfFile(g_fp, "topic: %s, vgroupId: %d\n", tmq_get_topic_name(msg), vgroupId);

  while (1) {
    TAOS_ROW row = taos_fetch_row(msg);

    if (row == NULL) break;

    TAOS_FIELD* fields = taos_fetch_fields(msg);
    int32_t     numOfFields = taos_field_count(msg);

    taos_print_row(buf, row, fields, numOfFields);

    const char* tbName = tmq_get_table_name(msg);

    if (0 != g_stConfInfo.showRowFlag) {
      taosFprintfFile(g_fp, "tbname:%s, rows[%d]: %s\n", (tbName != NULL ? tbName : "null table"), totalRows, buf);
      if (0 != g_stConfInfo.saveRowFlag) {
        saveConsumeContentToTbl(pInfo, buf);
      }
    }

    totalRows++;
  }

  addRowsToVgroupId(pInfo, vgroupId, totalRows);

  return totalRows;
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

static void tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  pError("tmq_commit_cb_print() commit %d\n", code);
}

void build_consumer(SThreadInfo* pInfo) {
  tmq_conf_t* conf = tmq_conf_new();

  // tmq_conf_set(conf, "td.connect.ip", "localhost");
  // tmq_conf_set(conf, "td.connect.port", "6030");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");

  // tmq_conf_set(conf, "td.connect.db", g_stConfInfo.dbName);

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);

  // tmq_conf_set(conf, "group.id", "cgrp1");
  for (int32_t i = 0; i < pInfo->numOfKey; i++) {
    tmq_conf_set(conf, pInfo->key[i], pInfo->value[i]);
  }

  tmq_conf_set(conf, "msg.with.table.name", "true");

  // tmq_conf_set(conf, "client.id", "c-001");

  // tmq_conf_set(conf, "enable.auto.commit", "true");
  // tmq_conf_set(conf, "enable.auto.commit", "false");

  // tmq_conf_set(conf, "auto.commit.interval.ms", "1000");

  // tmq_conf_set(conf, "auto.offset.reset", "none");
  // tmq_conf_set(conf, "auto.offset.reset", "earliest");
  // tmq_conf_set(conf, "auto.offset.reset", "latest");

  pInfo->tmq = tmq_consumer_new(conf, NULL, 0);

  tmq_conf_destroy(conf);

  return;
}

void build_topic_list(SThreadInfo* pInfo) {
  pInfo->topicList = tmq_list_new();
  // tmq_list_append(topic_list, "test_stb_topic_1");
  for (int32_t i = 0; i < pInfo->numOfTopic; i++) {
    tmq_list_append(pInfo->topicList, pInfo->topics[i]);
  }
  return;
}

int32_t saveConsumeResult(SThreadInfo* pInfo) {
  char sqlStr[1024] = {0};

  TAOS* pConn = taos_connect(NULL, "root", "taosdata", NULL, 0);
  assert(pConn != NULL);

  int64_t now = taosGetTimestampMs();

  // schema: ts timestamp, consumerid int, consummsgcnt bigint, checkresult int
  sprintf(sqlStr, "insert into %s.consumeresult values (%" PRId64 ", %d, %" PRId64 ", %" PRId64 ", %d)",
          g_stConfInfo.cdbName, now, pInfo->consumerId, pInfo->consumeMsgCnt, pInfo->consumeRowCnt, pInfo->checkresult);

  char tmpString[128];
  taosFprintfFile(g_fp, "%s, consume id %d result: %s\n", getCurrentTimeString(tmpString), pInfo->consumerId, sqlStr);

  TAOS_RES* pRes = taos_query(pConn, sqlStr);
  if (taos_errno(pRes) != 0) {
    pError("error in save consumeinfo, reason:%s\n", taos_errstr(pRes));
    taos_free_result(pRes);
    exit(-1);
  }

  taos_free_result(pRes);

#if 0
  // vgroups
  for (i = 0; i < pInfo->numOfVgroups; i++) {
    // schema: ts timestamp, consumerid int, consummsgcnt bigint, checkresult int
    sprintf(sqlStr, "insert into %s.vgroup_%d values (%"PRId64", %d, %" PRId64 ", %" PRId64 ", %d)", 
                     g_stConfInfo.cdbName,
                     now,
                     pInfo->consumerId, 
                     pInfo->consumeMsgCnt, 
                     pInfo->consumeRowCnt, 
                     pInfo->checkresult);
  
    char tmpString[128];
    taosFprintfFile(g_fp, "%s, consume id %d result: %s\n", getCurrentTimeString(tmpString), pInfo->consumerId ,sqlStr);
  
    TAOS_RES* pRes = taos_query(pConn, sqlStr);
    if (taos_errno(pRes) != 0) {
      pError("error in save consumeinfo, reason:%s\n", taos_errstr(pRes));
      taos_free_result(pRes);
      exit(-1);
    }
  
    taos_free_result(pRes);
  }
#endif

  return 0;
}

void loop_consume(SThreadInfo* pInfo) {
  int32_t code;

  int64_t totalMsgs = 0;
  int64_t totalRows = 0;

  char tmpString[128];
  taosFprintfFile(g_fp, "%s consumer id %d start to loop pull msg\n", getCurrentTimeString(tmpString),
                  pInfo->consumerId);

  pInfo->ts = taosGetTimestampMs();

  while (running) {
    TAOS_RES* tmqMsg = tmq_consumer_poll(pInfo->tmq, g_stConfInfo.consumeDelay * 1000);
    if (tmqMsg) {
      if (0 != g_stConfInfo.showMsgFlag) {
        totalRows += msg_process(tmqMsg, pInfo, totalMsgs);
      }

      taos_free_result(tmqMsg);

      totalMsgs++;

      if (totalRows >= pInfo->expectMsgCnt) {
        char tmpString[128];
        taosFprintfFile(g_fp, "%s over than expect rows, so break consume\n", getCurrentTimeString(tmpString));
        break;
      }
    } else {
      char tmpString[128];
      taosFprintfFile(g_fp, "%s no poll more msg when time over, break consume\n", getCurrentTimeString(tmpString));
      break;
    }
  }

  pInfo->consumeMsgCnt = totalMsgs;
  pInfo->consumeRowCnt = totalRows;

  taosFprintfFile(g_fp, "==== consumerId: %d, consumeMsgCnt: %" PRId64 ", consumeRowCnt: %" PRId64 "\n",
                  pInfo->consumerId, pInfo->consumeMsgCnt, pInfo->consumeRowCnt);
}

void* consumeThreadFunc(void* param) {
  int32_t totalMsgs = 0;

  SThreadInfo* pInfo = (SThreadInfo*)param;

  build_consumer(pInfo);
  build_topic_list(pInfo);
  if ((NULL == pInfo->tmq) || (NULL == pInfo->topicList)) {
    assert(0);
    return NULL;
  }

  int32_t err = tmq_subscribe(pInfo->tmq, pInfo->topicList);
  if (err != 0) {
    pError("tmq_subscribe() fail, reason: %s\n", tmq_err2str(err));
    exit(-1);
  }

  tmq_list_destroy(pInfo->topicList);
  pInfo->topicList = NULL;

  loop_consume(pInfo);

  if (pInfo->ifManualCommit) {
    taosFprintfFile(g_fp, "tmq_commit() manual commit when consume end.\n");
    pPrint("tmq_commit() manual commit when consume end.\n");
    /*tmq_commit(pInfo->tmq, NULL, 0);*/
    tmq_commit_sync(pInfo->tmq, NULL);
    taosFprintfFile(g_fp, "tmq_commit() manual commit over.\n");
    pPrint("tmq_commit() manual commit over.\n");
  }

  err = tmq_unsubscribe(pInfo->tmq);
  if (err != 0) {
    pError("tmq_unsubscribe() fail, reason: %s\n", tmq_err2str(err));
    /*pInfo->consumeMsgCnt = -1;*/
    /*return NULL;*/
  }

  err = tmq_consumer_close(pInfo->tmq);
  if (err != 0) {
    pError("tmq_consumer_close() fail, reason: %s\n", tmq_err2str(err));
    /*exit(-1);*/
  }
  pInfo->tmq = NULL;

  // save consume result into consumeresult table
  saveConsumeResult(pInfo);

  // save rows from per vgroup
  taosFprintfFile(g_fp, "======== consumerId: %d, consume rows from per vgroups ========\n", pInfo->consumerId);
  for (int32_t i = 0; i < pInfo->numOfVgroups; i++) {
    taosFprintfFile(g_fp, "vgroups: %04d, rows: %d\n", pInfo->rowsOfPerVgroups[i][0], pInfo->rowsOfPerVgroups[i][1]);
  }

  return NULL;
}

void parseConsumeInfo() {
  char*      token;
  const char delim[2] = ",";
  const char ch = ':';

  for (int32_t i = 0; i < g_stConfInfo.numOfThread; i++) {
    token = strtok(g_stConfInfo.stThreads[i].topicString, delim);
    while (token != NULL) {
      // printf("%s\n", token );
      strcpy(g_stConfInfo.stThreads[i].topics[g_stConfInfo.stThreads[i].numOfTopic], token);
      ltrim(g_stConfInfo.stThreads[i].topics[g_stConfInfo.stThreads[i].numOfTopic]);
      // printf("%s\n", g_stConfInfo.topics[g_stConfInfo.numOfTopic]);
      g_stConfInfo.stThreads[i].numOfTopic++;

      token = strtok(NULL, delim);
    }

    token = strtok(g_stConfInfo.stThreads[i].keyString, delim);
    while (token != NULL) {
      // printf("%s\n", token );
      {
        char* pstr = token;
        ltrim(pstr);
        char* ret = strchr(pstr, ch);
        memcpy(g_stConfInfo.stThreads[i].key[g_stConfInfo.stThreads[i].numOfKey], pstr, ret - pstr);
        strcpy(g_stConfInfo.stThreads[i].value[g_stConfInfo.stThreads[i].numOfKey], ret + 1);
        // printf("key: %s, value: %s\n", g_stConfInfo.key[g_stConfInfo.numOfKey],
        // g_stConfInfo.value[g_stConfInfo.numOfKey]);
        g_stConfInfo.stThreads[i].numOfKey++;
      }

      token = strtok(NULL, delim);
    }
  }
}

int32_t getConsumeInfo() {
  char sqlStr[1024] = {0};

  TAOS* pConn = taos_connect(NULL, "root", "taosdata", NULL, 0);
  assert(pConn != NULL);

  sprintf(sqlStr, "select * from %s.consumeinfo", g_stConfInfo.cdbName);
  TAOS_RES* pRes = taos_query(pConn, sqlStr);
  if (taos_errno(pRes) != 0) {
    pError("error in get consumeinfo, reason:%s\n", taos_errstr(pRes));
    taosFprintfFile(g_fp, "error in get consumeinfo, reason:%s\n", taos_errstr(pRes));
    taosCloseFile(&g_fp);
    taos_free_result(pRes);
    exit(-1);
  }

  TAOS_ROW    row = NULL;
  int         num_fields = taos_num_fields(pRes);
  TAOS_FIELD* fields = taos_fetch_fields(pRes);

  // schema: ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint,
  // ifcheckdata int

  int32_t numOfThread = 0;
  while ((row = taos_fetch_row(pRes))) {
    int32_t* lengths = taos_fetch_lengths(pRes);

    // set default value
    // g_stConfInfo.stThreads[numOfThread].autoCommitIntervalMs = 5000;
    // memcpy(g_stConfInfo.stThreads[numOfThread].autoCommit, "true", strlen("true"));
    // memcpy(g_stConfInfo.stThreads[numOfThread].autoOffsetRest, "earlieast", strlen("earlieast"));

    for (int i = 0; i < num_fields; ++i) {
      if (row[i] == NULL || 0 == i) {
        continue;
      }

      if ((1 == i) && (fields[i].type == TSDB_DATA_TYPE_INT)) {
        g_stConfInfo.stThreads[numOfThread].consumerId = *((int32_t*)row[i]);
      } else if ((2 == i) && (fields[i].type == TSDB_DATA_TYPE_BINARY)) {
        memcpy(g_stConfInfo.stThreads[numOfThread].topicString, row[i], lengths[i]);
      } else if ((3 == i) && (fields[i].type == TSDB_DATA_TYPE_BINARY)) {
        memcpy(g_stConfInfo.stThreads[numOfThread].keyString, row[i], lengths[i]);
      } else if ((4 == i) && (fields[i].type == TSDB_DATA_TYPE_BIGINT)) {
        g_stConfInfo.stThreads[numOfThread].expectMsgCnt = *((int64_t*)row[i]);
      } else if ((5 == i) && (fields[i].type == TSDB_DATA_TYPE_INT)) {
        g_stConfInfo.stThreads[numOfThread].ifCheckData = *((int32_t*)row[i]);
      } else if ((6 == i) && (fields[i].type == TSDB_DATA_TYPE_INT)) {
        g_stConfInfo.stThreads[numOfThread].ifManualCommit = *((int32_t*)row[i]);
      }
    }
    numOfThread++;
  }
  g_stConfInfo.numOfThread = numOfThread;

  taos_free_result(pRes);

  parseConsumeInfo();

  return 0;
}

int main(int32_t argc, char* argv[]) {
  parseArgument(argc, argv);
  getConsumeInfo();
  saveConfigToLogFile();

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);

  // pthread_create one thread to consume
  taosFprintfFile(g_fp, "==== create %d consume thread ====\n", g_stConfInfo.numOfThread);
  for (int32_t i = 0; i < g_stConfInfo.numOfThread; ++i) {
    taosThreadCreate(&(g_stConfInfo.stThreads[i].thread), &thattr, consumeThreadFunc,
                     (void*)(&(g_stConfInfo.stThreads[i])));
  }

  for (int32_t i = 0; i < g_stConfInfo.numOfThread; i++) {
    taosThreadJoin(g_stConfInfo.stThreads[i].thread, NULL);
    taosThreadClear(&g_stConfInfo.stThreads[i].thread);
  }

  // printf("consumer: %d, cosumer1: %d\n", totalMsgs, pInfo->consumeMsgCnt);

  taosFprintfFile(g_fp, "==== close tmqlog ====\n");
  taosCloseFile(&g_fp);

  return 0;
}

