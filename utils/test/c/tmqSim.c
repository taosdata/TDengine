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
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#include "taos.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tlog.h"
#include "types.h"

#define GREEN     "\033[1;32m"
#define NC        "\033[0m"
#define min(a, b) (((a) < (b)) ? (a) : (b))

#define MAX_SQL_STR_LEN         (1024 * 1024)
#define MAX_ROW_STR_LEN         (16 * 1024)
#define MAX_CONSUMER_THREAD_CNT (16)
#define MAX_VGROUP_CNT          (32)
#define SEND_TIME_UNIT          10  // ms
#define MAX_SQL_LEN             1048576

typedef enum {
  NOTIFY_CMD_START_CONSUM,
  NOTIFY_CMD_START_COMMIT,
  NOTIFY_CMD_ID_BUTT,
} NOTIFY_CMD_ID;

typedef enum enumQUERY_TYPE { NO_INSERT_TYPE, INSERT_TYPE, QUERY_TYPE_BUT } QUERY_TYPE;

typedef struct {
  TdThread thread;
  int32_t  consumerId;

  int32_t ifManualCommit;
  // int32_t  autoCommitIntervalMs;  // 1000 ms
  // char     autoCommit[8];         // true, false
  // char     autoOffsetRest[16];    // none, earliest, latest

  TdFilePtr pConsumeRowsFile;
  TdFilePtr pConsumeMetaFile;
  int32_t   ifCheckData;
  int64_t   expectMsgCnt;

  int64_t consumeMsgCnt;
  int64_t consumeRowCnt;
  int64_t consumeLen;
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

  TAOS* taos;

  // below parameters is used by omb test
  int32_t producerRate;  // unit: msgs/s
  int64_t totalProduceMsgs;
  int64_t totalMsgsLen;

} SThreadInfo;

typedef struct {
  // input from argvs
  char        cdbName[32];
  char        dbName[64];
  int32_t     showMsgFlag;
  int32_t     showRowFlag;
  int32_t     saveRowFlag;
  int32_t     consumeDelay;  // unit s
  int32_t     numOfThread;
  int32_t     useSnapshot;
  int64_t     nowTime;
  SThreadInfo stThreads[MAX_CONSUMER_THREAD_CNT];

  SThreadInfo stProdThreads[MAX_CONSUMER_THREAD_CNT];

  // below parameters is used by omb test
  char    topic[64];
  int32_t producers;
  int32_t producerRate;
  int32_t runDurationMinutes;
  int32_t batchSize;
  int32_t payloadLen;
} SConfInfo;

static SConfInfo g_stConfInfo;
TdFilePtr        g_fp = NULL;
static int       running = 1;
char*            g_payload = NULL;

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
  printf("%s%s%s%ds\n", indent, indent, "consume delay, default is ", g_stConfInfo.consumeDelay);
  printf("%s%s\n", indent, "-e");
  printf("%s%s%s%d\n", indent, indent, "snapshot, default is ", g_stConfInfo.useSnapshot);

  printf("%s%s\n", indent, "-t");
  printf("%s%s%s\n", indent, indent, "topic name, default is null");

  printf("%s%s\n", indent, "-x");
  printf("%s%s%s\n", indent, indent, "consume thread number, default is 1");

  printf("%s%s\n", indent, "-l");
  printf("%s%s%s%d\n", indent, indent, "run duration unit is minutes, default is ", g_stConfInfo.runDurationMinutes);
  printf("%s%s\n", indent, "-p");
  printf("%s%s%s\n", indent, indent, "producer thread number, default is 0");
  printf("%s%s\n", indent, "-b");
  printf("%s%s%s\n", indent, indent, "batch size, default is 1");
  printf("%s%s\n", indent, "-i");
  printf("%s%s%s\n", indent, indent, "produce rate unit is msgs /s, default is 100000");
  printf("%s%s\n", indent, "-n");
  printf("%s%s%s\n", indent, indent, "payload len unit is byte, default is 1000");

  exit(EXIT_SUCCESS);
}

char* getCurrentTimeString(char* timeString) {
  time_t    tTime = taosGetTimestampSec();
  struct tm tm;
  taosLocalTime(&tTime, &tm, NULL);
  sprintf(timeString, "%d-%02d-%02d %02d:%02d:%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
          tm.tm_min, tm.tm_sec);

  return timeString;
}

static void tmqStop(int signum, void* info, void* ctx) {
  running = 0;
  char tmpString[128];
  taosFprintfFile(g_fp, "%s tmqStop() receive stop signal[%d]\n", getCurrentTimeString(tmpString), signum);
}

static void tmqSetSignalHandle() { taosSetSignal(SIGINT, tmqStop); }

void initLogFile() {
  char filename[256];
  char tmpString[128];

  pid_t process_id = getpid();

  if (0 != strlen(g_stConfInfo.topic)) {
    sprintf(filename, "/tmp/tmqlog-%d-%s.txt", process_id, getCurrentTimeString(tmpString));
  } else {
    sprintf(filename, "%s/../log/tmqlog-%d-%s.txt", configDir, process_id, getCurrentTimeString(tmpString));
  }
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
    taosFprintfFile(g_fp, "  expect rows: %" PRId64 "\n", g_stConfInfo.stThreads[i].expectMsgCnt);
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
  g_stConfInfo.numOfThread = 1;
  g_stConfInfo.batchSize = 1;
  g_stConfInfo.producers = 0;

  g_stConfInfo.nowTime = taosGetTimestampMs();

  for (int32_t i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(0);
    } else if (strcmp(argv[i], "-d") == 0) {
      tstrncpy(g_stConfInfo.dbName, argv[++i], sizeof(g_stConfInfo.dbName));
    } else if (strcmp(argv[i], "-w") == 0) {
      tstrncpy(g_stConfInfo.cdbName, argv[++i], sizeof(g_stConfInfo.cdbName));
    } else if (strcmp(argv[i], "-c") == 0) {
      tstrncpy(configDir, argv[++i], PATH_MAX);
    } else if (strcmp(argv[i], "-g") == 0) {
      g_stConfInfo.showMsgFlag = atol(argv[++i]);
    } else if (strcmp(argv[i], "-r") == 0) {
      g_stConfInfo.showRowFlag = atol(argv[++i]);
    } else if (strcmp(argv[i], "-s") == 0) {
      g_stConfInfo.saveRowFlag = atol(argv[++i]);
    } else if (strcmp(argv[i], "-y") == 0) {
      g_stConfInfo.consumeDelay = atol(argv[++i]);
    } else if (strcmp(argv[i], "-e") == 0) {
      g_stConfInfo.useSnapshot = atol(argv[++i]);
    } else if (strcmp(argv[i], "-t") == 0) {
      char tmpBuf[56] = {0};
      tstrncpy(tmpBuf, argv[++i], sizeof(tmpBuf));
      sprintf(g_stConfInfo.topic, "`%s`", tmpBuf);
    } else if (strcmp(argv[i], "-x") == 0) {
      g_stConfInfo.numOfThread = atol(argv[++i]);
    } else if (strcmp(argv[i], "-l") == 0) {
      g_stConfInfo.runDurationMinutes = atol(argv[++i]);
    } else if (strcmp(argv[i], "-p") == 0) {
      g_stConfInfo.producers = atol(argv[++i]);
    } else if (strcmp(argv[i], "-b") == 0) {
      g_stConfInfo.batchSize = atol(argv[++i]);
    } else if (strcmp(argv[i], "-i") == 0) {
      g_stConfInfo.producerRate = atol(argv[++i]);
    } else if (strcmp(argv[i], "-n") == 0) {
      g_stConfInfo.payloadLen = atol(argv[++i]);
      if (g_stConfInfo.payloadLen <= 0 || g_stConfInfo.payloadLen > 1024 * 1024 * 1024) {
        pError("%s calloc size is too large: %s %s", GREEN, argv[++i], NC);
        exit(-1);
      }
    } else {
      pError("%s unknow para: %s %s", GREEN, argv[++i], NC);
      exit(-1);
    }
  }

  g_payload = taosMemoryCalloc(g_stConfInfo.payloadLen + 1, 1);
  if (NULL == g_payload) {
    pPrint("%s failed to malloc for payload %s", GREEN, NC);
    exit(-1);
  }

  for (int32_t i = 0; i < g_stConfInfo.payloadLen; i++) {
    strcpy(&g_payload[i], "a");
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

  pPrint("%s snapshot:%d %s", GREEN, g_stConfInfo.useSnapshot, NC);

  pPrint("%s omb topic:%s %s", GREEN, g_stConfInfo.topic, NC);
  pPrint("%s numOfThread:%d %s", GREEN, g_stConfInfo.numOfThread, NC);
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

int queryDB(TAOS* taos, char* command) {
  int       retryCnt = 10;
  int       code = 0;
  TAOS_RES* pRes = NULL;

  while (retryCnt--) {
    pRes = taos_query(taos, command);
    code = taos_errno(pRes);
    if (code != 0) {
      taosSsleep(1);
      taos_free_result(pRes);
      pRes = NULL;
      continue;
    }
    taos_free_result(pRes);
    return 0;
  }

  pError("failed to reason:%s, sql: %s", tstrerror(code), command);
  taos_free_result(pRes);
  return -1;
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

  taosFprintfFile(g_fp, "consume id %d, add new vgroupId:%d\n", pInfo->consumerId, vgroupId);
  if (pInfo->numOfVgroups > MAX_VGROUP_CNT) {
    taosFprintfFile(g_fp, "====consume id %d, vgroup num %d over than 32. new vgroupId: %d\n", pInfo->consumerId,
                    pInfo->numOfVgroups, vgroupId);
    taosCloseFile(&g_fp);
    exit(-1);
  }
}

TAOS* createNewTaosConnect() {
  TAOS*   taos = NULL;
  int32_t retryCnt = 10;

  while (retryCnt--) {
    taos = taos_connect(NULL, "root", "taosdata", NULL, 0);
    if (NULL != taos) {
      return taos;
    }
    taosSsleep(1);
  }

  taosFprintfFile(g_fp, "taos_connect() fail\n");
  return NULL;
}

int32_t saveConsumeContentToTbl(SThreadInfo* pInfo, char* buf) {
  char sqlStr[1100] = {0};

  if (strlen(buf) > 1024) {
    taosFprintfFile(g_fp, "The length of one row[%d] is overflow 1024\n", (int)strlen(buf));
    taosCloseFile(&g_fp);
    return -1;
  }

  TAOS* pConn = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    taosFprintfFile(g_fp, "taos_connect() fail, can not save consume result to main script\n");
    return -1;
  }

  sprintf(sqlStr, "insert into %s.content_%d values (%" PRId64 ", \'%s\')", g_stConfInfo.cdbName, pInfo->consumerId,
          pInfo->ts++, buf);
  int retCode = queryDB(pConn, sqlStr);
  if (retCode != 0) {
    taosFprintfFile(g_fp, "error in save consume content\n");
    taosCloseFile(&g_fp);
    taos_close(pConn);
    exit(-1);
  }

  taos_close(pConn);

  return 0;
}

static char* shellFormatTimestamp(char* buf, int64_t val, int32_t precision) {
  // if (shell.args.is_raw_time) {
  //   sprintf(buf, "%" PRId64, val);
  //   return buf;
  // }

  time_t  tt;
  int32_t ms = 0;
  if (precision == TSDB_TIME_PRECISION_NANO) {
    tt = (time_t)(val / 1000000000);
    ms = val % 1000000000;
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    tt = (time_t)(val / 1000000);
    ms = val % 1000000;
  } else {
    tt = (time_t)(val / 1000);
    ms = val % 1000;
  }

  if (tt <= 0 && ms < 0) {
    tt--;
    if (precision == TSDB_TIME_PRECISION_NANO) {
      ms += 1000000000;
    } else if (precision == TSDB_TIME_PRECISION_MICRO) {
      ms += 1000000;
    } else {
      ms += 1000;
    }
  }

  struct tm ptm;
  if (taosLocalTime(&tt, &ptm, buf) == NULL) {
    return buf;
  }
  size_t pos = strftime(buf, 35, "%Y-%m-%d %H:%M:%S", &ptm);

  if (precision == TSDB_TIME_PRECISION_NANO) {
    sprintf(buf + pos, ".%09d", ms);
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    sprintf(buf + pos, ".%06d", ms);
  } else {
    sprintf(buf + pos, ".%03d", ms);
  }

  return buf;
}

static void shellDumpFieldToFile(TdFilePtr pFile, const char* val, TAOS_FIELD* field, int32_t length,
                                 int32_t precision) {
  if (val == NULL) {
    taosFprintfFile(pFile, "NULL");
    return;
  }

  char quotationStr[2];
  quotationStr[0] = '\"';
  quotationStr[1] = 0;

  int  n;
  char buf[TSDB_MAX_BYTES_PER_ROW];
  switch (field->type) {
    case TSDB_DATA_TYPE_BOOL:
      taosFprintfFile(pFile, "%d", ((((int32_t)(*((char*)val))) == 1) ? 1 : 0));
      break;
    case TSDB_DATA_TYPE_TINYINT:
      taosFprintfFile(pFile, "%d", *((int8_t*)val));
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      taosFprintfFile(pFile, "%u", *((uint8_t*)val));
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      taosFprintfFile(pFile, "%d", *((int16_t*)val));
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      taosFprintfFile(pFile, "%u", *((uint16_t*)val));
      break;
    case TSDB_DATA_TYPE_INT:
      taosFprintfFile(pFile, "%d", *((int32_t*)val));
      break;
    case TSDB_DATA_TYPE_UINT:
      taosFprintfFile(pFile, "%u", *((uint32_t*)val));
      break;
    case TSDB_DATA_TYPE_BIGINT:
      taosFprintfFile(pFile, "%" PRId64, *((int64_t*)val));
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      taosFprintfFile(pFile, "%" PRIu64, *((uint64_t*)val));
      break;
    case TSDB_DATA_TYPE_FLOAT:
      taosFprintfFile(pFile, "%.5f", GET_FLOAT_VAL(val));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      n = snprintf(buf, TSDB_MAX_BYTES_PER_ROW, "%*.9f", length, GET_DOUBLE_VAL(val));
      if (n > TMAX(25, length)) {
        taosFprintfFile(pFile, "%*.15e", length, GET_DOUBLE_VAL(val));
      } else {
        taosFprintfFile(pFile, "%s", buf);
      }
      break;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_GEOMETRY: {
      int32_t bufIndex = 0;
      for (int32_t i = 0; i < length; i++) {
        buf[bufIndex] = val[i];
        bufIndex++;
        if (val[i] == '\"') {
          buf[bufIndex] = val[i];
          bufIndex++;
        }
      }
      buf[bufIndex] = 0;

      taosFprintfFile(pFile, "%s%s%s", quotationStr, buf, quotationStr);
    } break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      shellFormatTimestamp(buf, *(int64_t*)val, precision);
      taosFprintfFile(pFile, "%s%s%s", quotationStr, buf, quotationStr);
      break;
    default:
      break;
  }
}

static void dumpToFileForCheck(TdFilePtr pFile, TAOS_ROW row, TAOS_FIELD* fields, int32_t* length, int32_t num_fields,
                               int32_t precision) {
  for (int32_t i = 0; i < num_fields; i++) {
    if (i > 0) {
      taosFprintfFile(pFile, ",");
    }
    shellDumpFieldToFile(pFile, (const char*)row[i], fields + i, length[i], precision);
  }
  taosFprintfFile(pFile, "\n");
}

static int32_t data_msg_process(TAOS_RES* msg, SThreadInfo* pInfo, int32_t msgIndex) {
  char    buf[1024];
  int32_t totalRows = 0;

  int32_t     vgroupId = tmq_get_vgroup_id(msg);
  const char* dbName = tmq_get_db_name(msg);

  taosFprintfFile(g_fp, "consumerId: %d, msg index:%d\n", pInfo->consumerId, msgIndex);
  int32_t index = 0;
  for (index = 0; index < pInfo->numOfVgroups; index++) {
    if (vgroupId == pInfo->rowsOfPerVgroups[index][0]) {
      break;
    }
  }

  taosFprintfFile(g_fp, "dbName: %s, topic: %s, vgroupId:%d, currentRows:%d\n", dbName != NULL ? dbName : "invalid table",
                  tmq_get_topic_name(msg), vgroupId, pInfo->rowsOfPerVgroups[index][1]);

  while (1) {
    TAOS_ROW row = taos_fetch_row(msg);
    if (row == NULL) {
      break;
    }

    TAOS_FIELD* fields = taos_fetch_fields(msg);
    int32_t     numOfFields = taos_field_count(msg);
    int32_t*    length = taos_fetch_lengths(msg);
    int32_t     precision = taos_result_precision(msg);
    const char* tbName = tmq_get_table_name(msg);

#if 0
	// get schema
	//============================== stub =================================================//
	for (int32_t i = 0; i < numOfFields; i++) {
	  taosFprintfFile(g_fp, "%02d: name: %s, type: %d, len: %d\n", i, fields[i].name, fields[i].type, fields[i].bytes);
	}
	//============================== stub =================================================//
#endif

    dumpToFileForCheck(pInfo->pConsumeRowsFile, row, fields, length, numOfFields, precision);
    taos_print_row(buf, row, fields, numOfFields);

    if (0 != g_stConfInfo.showRowFlag) {
      taosFprintfFile(g_fp, "time:%" PRId64 " tbname:%s, rows[%d]: %s\n", taosGetTimestampMs(), (tbName != NULL ? tbName : "null table"), totalRows, buf);
      // if (0 != g_stConfInfo.saveRowFlag) {
      //   saveConsumeContentToTbl(pInfo, buf);
      // }
//      taosFsyncFile(g_fp);
    }

    totalRows++;
  }

  addRowsToVgroupId(pInfo, vgroupId, totalRows);
  return totalRows;
}

static int32_t meta_msg_process(TAOS_RES* msg, SThreadInfo* pInfo, int32_t msgIndex) {
  char    buf[1024];
  int32_t totalRows = 0;

  // printf("topic: %s\n", tmq_get_topic_name(msg));
  int32_t     vgroupId = tmq_get_vgroup_id(msg);
  const char* dbName = tmq_get_db_name(msg);

  taosFprintfFile(g_fp, "consumerId: %d, msg index:%d\n", pInfo->consumerId, msgIndex);
  taosFprintfFile(g_fp, "dbName: %s, topic: %s, vgroupId: %d\n", dbName != NULL ? dbName : "invalid table",
                  tmq_get_topic_name(msg), vgroupId);

  {
    tmq_raw_data raw = {0};
    int32_t      code = tmq_get_raw(msg, &raw);

    if (code == TSDB_CODE_SUCCESS) {
      //	  int retCode = queryDB(pInfo->taos, "use metadb");
      //	  if (retCode != 0) {
      //		taosFprintfFile(g_fp, "error when use metadb\n");
      //		taosCloseFile(&g_fp);
      //		exit(-1);
      //	  }
      //	  taosFprintfFile(g_fp, "raw:%p\n", &raw);
      //
      //      tmq_write_raw(pInfo->taos, raw);
    }

    char* result = tmq_get_json_meta(msg);
    if (result && strcmp(result, "") != 0) {
      // printf("meta result: %s\n", result);
      taosFprintfFile(pInfo->pConsumeMetaFile, "%s\n", result);
    }
    tmq_free_json_meta(result);
  }

  totalRows++;

  return totalRows;
}

static void appNothing(void* param, TAOS_RES* res, int32_t numOfRows) {}

int32_t notifyMainScript(SThreadInfo* pInfo, int32_t cmdId) {
  char sqlStr[1024] = {0};

  // schema: ts timestamp, consumerid int, consummsgcnt bigint, checkresult int
  sprintf(sqlStr, "insert into %s.notifyinfo values (%" PRId64 ", %d, %d)", g_stConfInfo.cdbName,
          atomic_fetch_add_64(&g_stConfInfo.nowTime, 1), cmdId, pInfo->consumerId);

  taos_query_a(pInfo->taos, sqlStr, appNothing, NULL);

  taosFprintfFile(g_fp, "notifyMainScript success, sql: %s\n", sqlStr);

  return 0;
}

static int32_t g_once_commit_flag = 0;

static void tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  taosFprintfFile(g_fp, "tmq_commit_cb_print() commit %d\n", code);

  if (0 == g_once_commit_flag && code == 0) {
    g_once_commit_flag = 1;
    notifyMainScript((SThreadInfo*)param, (int32_t)NOTIFY_CMD_START_COMMIT);
  }

  char tmpString[128];
  taosFprintfFile(g_fp, "%s tmq_commit_cb_print() be called\n", getCurrentTimeString(tmpString));
}

void build_consumer(SThreadInfo* pInfo) {
  tmq_conf_t* conf = tmq_conf_new();

  // tmq_conf_set(conf, "td.connect.ip", "localhost");
  // tmq_conf_set(conf, "td.connect.port", "6030");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");

  // tmq_conf_set(conf, "td.connect.db", g_stConfInfo.dbName);

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, pInfo);

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
  //
  if (g_stConfInfo.useSnapshot) {
    tmq_conf_set(conf, "experimental.snapshot.enable", "true");
  }

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
  // schema: ts timestamp, consumerid int, consummsgcnt bigint, checkresult int
  sprintf(sqlStr, "insert into %s.consumeresult values (%" PRId64 ", %d, %" PRId64 ", %" PRId64 ", %d)",
          g_stConfInfo.cdbName, atomic_fetch_add_64(&g_stConfInfo.nowTime, 1), pInfo->consumerId, pInfo->consumeMsgCnt,
          pInfo->consumeRowCnt, pInfo->checkresult);

  char tmpString[128];
  taosFprintfFile(g_fp, "%s, consume id %d result: %s\n", getCurrentTimeString(tmpString), pInfo->consumerId, sqlStr);

  int retCode = queryDB(pInfo->taos, sqlStr);
  if (retCode != 0) {
    taosFprintfFile(g_fp, "consume id %d error in save consume result\n", pInfo->consumerId);
    return -1;
  }

  return 0;
}

void loop_consume(SThreadInfo* pInfo) {
  int32_t code;

  int32_t once_flag = 0;

  int64_t totalMsgs = 0;
  int64_t totalRows = 0;

  char tmpString[128];
  taosFprintfFile(g_fp, "%s consumer id %d start to loop pull msg\n", getCurrentTimeString(tmpString),
                  pInfo->consumerId);

  pInfo->ts = taosGetTimestampMs();

  if (pInfo->ifCheckData) {
    char filename[256] = {0};
    memset(tmpString, 0, tListLen(tmpString));

    // sprintf(filename, "%s/../log/consumerid_%d_%s.txt", configDir, pInfo->consumerId,
    // getCurrentTimeString(tmpString));
    sprintf(filename, "%s/../log/consumerid_%d.txt", configDir, pInfo->consumerId);
    pInfo->pConsumeRowsFile = taosOpenFile(filename, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_STREAM);

    sprintf(filename, "%s/../log/meta_consumerid_%d.txt", configDir, pInfo->consumerId);
    pInfo->pConsumeMetaFile = taosOpenFile(filename, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_STREAM);

    if (pInfo->pConsumeRowsFile == NULL || pInfo->pConsumeMetaFile == NULL) {
      taosFprintfFile(g_fp, "%s create file fail for save rows or save meta\n", getCurrentTimeString(tmpString));
      return;
    }
  }

  int64_t  lastTotalMsgs = 0;
  uint64_t lastPrintTime = taosGetTimestampMs();
  uint64_t startTs = taosGetTimestampMs();

  int32_t consumeDelay = g_stConfInfo.consumeDelay == -1 ? -1 : (g_stConfInfo.consumeDelay * 1000);
  while (running) {
    TAOS_RES* tmqMsg = tmq_consumer_poll(pInfo->tmq, consumeDelay);
    if (tmqMsg) {
      if (0 != g_stConfInfo.showMsgFlag) {
        tmq_res_t msgType = tmq_get_res_type(tmqMsg);
        if (msgType == TMQ_RES_TABLE_META) {
          totalRows += meta_msg_process(tmqMsg, pInfo, totalMsgs);
        } else if (msgType == TMQ_RES_DATA) {
          totalRows += data_msg_process(tmqMsg, pInfo, totalMsgs);
        } else if (msgType == TMQ_RES_METADATA) {
          meta_msg_process(tmqMsg, pInfo, totalMsgs);
          totalRows += data_msg_process(tmqMsg, pInfo, totalMsgs);
        }
      }

      taos_free_result(tmqMsg);
      totalMsgs++;

      int64_t currentPrintTime = taosGetTimestampMs();
      if (currentPrintTime - lastPrintTime > 10 * 1000) {
        taosFprintfFile(
            g_fp, "consumer id %d has currently poll total msgs: %" PRId64 ", period rate: %.3f msgs/second\n",
            pInfo->consumerId, totalMsgs, (totalMsgs - lastTotalMsgs) * 1000.0 / (currentPrintTime - lastPrintTime));
        lastPrintTime = currentPrintTime;
        lastTotalMsgs = totalMsgs;
      }

      if (0 == once_flag) {
        once_flag = 1;
        notifyMainScript(pInfo, NOTIFY_CMD_START_CONSUM);
      }

      if ((totalRows >= pInfo->expectMsgCnt) || (totalMsgs >= pInfo->expectMsgCnt)) {
        memset(tmpString, 0, tListLen(tmpString));
        taosFprintfFile(g_fp, "%s over than expect rows, so break consume\n", getCurrentTimeString(tmpString));
        break;
      }
    } else {
      memset(tmpString, 0, tListLen(tmpString));
      taosFprintfFile(g_fp, "%s no poll more msg when time over, break consume\n", getCurrentTimeString(tmpString));
      break;
    }
  }

  if (0 == running) {
    taosFprintfFile(g_fp, "receive stop signal and not continue consume\n");
  }

  pInfo->consumeMsgCnt = totalMsgs;
  pInfo->consumeRowCnt = totalRows;

  taosFprintfFile(g_fp, "==== consumerId: %d, consumeMsgCnt: %" PRId64 ", consumeRowCnt: %" PRId64 "\n",
                  pInfo->consumerId, pInfo->consumeMsgCnt, pInfo->consumeRowCnt);

  if(taosFsyncFile(pInfo->pConsumeRowsFile) < 0){
    printf("taosFsyncFile error:%s", strerror(errno));
  }
  taosCloseFile(&pInfo->pConsumeRowsFile);
}

void* consumeThreadFunc(void* param) {
  SThreadInfo* pInfo = (SThreadInfo*)param;

  pInfo->taos = createNewTaosConnect();
  if (pInfo->taos == NULL) {
    taosFprintfFile(g_fp, "taos_connect() fail, can not notify and save consume result to main scripte\n");
    return NULL;
  }

  build_consumer(pInfo);
  build_topic_list(pInfo);
  if ((NULL == pInfo->tmq) || (NULL == pInfo->topicList)) {
    taosFprintfFile(g_fp, "create consumer fail! tmq is null or topicList is null\n");
    taos_close(pInfo->taos);
    pInfo->taos = NULL;
    return NULL;
  }

  int32_t err = tmq_subscribe(pInfo->tmq, pInfo->topicList);
  if (err != 0) {
    pError("tmq_subscribe() fail, reason: %s\n", tmq_err2str(err));
    taosFprintfFile(g_fp, "tmq_subscribe() fail! reason: %s\n", tmq_err2str(err));
    taos_close(pInfo->taos);
    pInfo->taos = NULL;
    return NULL;
  }

  tmq_list_destroy(pInfo->topicList);
  pInfo->topicList = NULL;

  loop_consume(pInfo);

  if (pInfo->ifManualCommit) {
    pPrint("tmq_commit() manual commit when consume end.\n");
    /*tmq_commit(pInfo->tmq, NULL, 0);*/
    tmq_commit_sync(pInfo->tmq, NULL);
    tmq_commit_cb_print(pInfo->tmq, 0, pInfo);
    taosFprintfFile(g_fp, "tmq_commit() manual commit over.\n");
    pPrint("tmq_commit() manual commit over.\n");
  }

  err = tmq_unsubscribe(pInfo->tmq);
  if (err != 0) {
    pError("tmq_unsubscribe() fail, reason: %s\n", tmq_err2str(err));
    taosFprintfFile(g_fp, "tmq_unsubscribe()! reason: %s\n", tmq_err2str(err));
  }

  err = tmq_consumer_close(pInfo->tmq);
  if (err != 0) {
    pError("tmq_consumer_close() fail, reason: %s\n", tmq_err2str(err));
    taosFprintfFile(g_fp, "tmq_consumer_close()! reason: %s\n", tmq_err2str(err));
  }
  pInfo->tmq = NULL;

  // save consume result into consumeresult table
  saveConsumeResult(pInfo);

  // save rows from per vgroup
  taosFprintfFile(g_fp, "======== consumerId: %d, consume rows from per vgroups ========\n", pInfo->consumerId);
  for (int32_t i = 0; i < pInfo->numOfVgroups; i++) {
    taosFprintfFile(g_fp, "vgroups: %04d, rows: %d\n", pInfo->rowsOfPerVgroups[i][0], pInfo->rowsOfPerVgroups[i][1]);
  }

  taos_close(pInfo->taos);
  pInfo->taos = NULL;

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
      tstrncpy(g_stConfInfo.stThreads[i].topics[g_stConfInfo.stThreads[i].numOfTopic], token,
               sizeof(g_stConfInfo.stThreads[i].topics[g_stConfInfo.stThreads[i].numOfTopic]));
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
        tstrncpy(g_stConfInfo.stThreads[i].value[g_stConfInfo.stThreads[i].numOfKey], ret + 1,
                 sizeof(g_stConfInfo.stThreads[i].value[g_stConfInfo.stThreads[i].numOfKey]));
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

  TAOS* pConn = createNewTaosConnect();
  if (pConn == NULL) {
    taosFprintfFile(g_fp, "taos_connect() fail, can not get consume info for start consumer\n");
    return -1;
  }

  sprintf(sqlStr, "select * from %s.consumeinfo", g_stConfInfo.cdbName);
  TAOS_RES* pRes = taos_query(pConn, sqlStr);
  if (taos_errno(pRes) != 0) {
    taosFprintfFile(g_fp, "error in get consumeinfo for %s\n", taos_errstr(pRes));
    taosCloseFile(&g_fp);
    taos_free_result(pRes);
    taos_close(pConn);
    return -1;
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
  taos_close(pConn);

  return 0;
}

static int32_t omb_data_msg_process(TAOS_RES* msg, SThreadInfo* pInfo, int32_t msgIndex, int64_t* lenOfRows) {
  char    buf[16 * 1024];
  int32_t totalRows = 0;
  int32_t totalLen = 0;

  // printf("topic: %s\n", tmq_get_topic_name(msg));
  // int32_t     vgroupId = tmq_get_vgroup_id(msg);
  // const char* dbName = tmq_get_db_name(msg);

  // taosFprintfFile(g_fp, "consumerId: %d, msg index:%" PRId64 "\n", pInfo->consumerId, msgIndex);
  // taosFprintfFile(g_fp, "dbName: %s, topic: %s, vgroupId: %d\n", dbName != NULL ? dbName : "invalid table",
  //                 tmq_get_topic_name(msg), vgroupId);

  while (1) {
    TAOS_ROW row = taos_fetch_row(msg);

    if (row == NULL) break;

    TAOS_FIELD* fields = taos_fetch_fields(msg);
    int32_t     numOfFields = taos_field_count(msg);
    // int32_t*    length = taos_fetch_lengths(msg);
    // int32_t     precision = taos_result_precision(msg);
    // const char* tbName = tmq_get_table_name(msg);

    taos_print_row(buf, row, fields, numOfFields);
    totalLen += strlen(buf);
    totalRows++;
  }

  *lenOfRows = totalLen;
  return totalRows;
}

void omb_loop_consume(SThreadInfo* pInfo) {
  int32_t code;

  int32_t once_flag = 0;

  int64_t totalMsgs = 0;
  int64_t totalRows = 0;

  char tmpString[128];
  taosFprintfFile(g_fp, "%s consumer id %d start to loop pull msg\n", getCurrentTimeString(tmpString),
                  pInfo->consumerId);
  printf("%s consumer id %d start to loop pull msg\n", getCurrentTimeString(tmpString), pInfo->consumerId);

  pInfo->ts = taosGetTimestampMs();

  int64_t  lastTotalMsgs = 0;
  uint64_t lastPrintTime = taosGetTimestampMs();
  uint64_t startTs = taosGetTimestampMs();

  int64_t totalLenOfMsg = 0;
  int64_t lastTotalLenOfMsg = 0;
  int32_t consumeDelay = g_stConfInfo.consumeDelay == -1 ? -1 : (g_stConfInfo.consumeDelay * 1000);
  while (running) {
    TAOS_RES* tmqMsg = tmq_consumer_poll(pInfo->tmq, consumeDelay);
    if (tmqMsg) {
      int64_t lenOfMsg = 0;
      totalRows += omb_data_msg_process(tmqMsg, pInfo, totalMsgs, &lenOfMsg);
      totalLenOfMsg += lenOfMsg;
      taos_free_result(tmqMsg);
      totalMsgs++;
      int64_t currentPrintTime = taosGetTimestampMs();
      if (currentPrintTime - lastPrintTime > 10 * 1000) {
        int64_t currentLenOfMsg = totalLenOfMsg - lastTotalLenOfMsg;
        int64_t deltaTime = currentPrintTime - lastPrintTime;
        printf("consumer id %d has currently cons total rows: %" PRId64 ", msgs: %" PRId64
               ", rate: %.3f msgs/s, %.1f MB/s\n",
               pInfo->consumerId, totalRows, totalMsgs, (totalMsgs - lastTotalMsgs) * 1000.0 / deltaTime,
               currentLenOfMsg * 1000.0 / (1024 * 1024) / deltaTime);

        taosFprintfFile(g_fp,
                        "consumer id %d has currently poll total msgs: %" PRId64
                        ", period cons rate: %.3f msgs/s, %.1f MB/s\n",
                        pInfo->consumerId, totalMsgs, (totalMsgs - lastTotalMsgs) * 1000.0 / deltaTime,
                        currentLenOfMsg * 1000.0 / deltaTime);
        lastPrintTime = currentPrintTime;
        lastTotalMsgs = totalMsgs;
        lastTotalLenOfMsg = totalLenOfMsg;
      }
    } else {
      memset(tmpString, 0, tListLen(tmpString));
      taosFprintfFile(g_fp, "%s no poll more msg when time over, break consume\n", getCurrentTimeString(tmpString));
      printf("%s no poll more msg when time over, break consume\n", getCurrentTimeString(tmpString));
      int64_t currentPrintTime = taosGetTimestampMs();
      int64_t currentLenOfMsg = totalLenOfMsg - lastTotalLenOfMsg;
      int64_t deltaTime = currentPrintTime - lastPrintTime;
      printf("consumer id %d has currently cons total rows: %" PRId64 ", msgs: %" PRId64
             ", rate: %.3f msgs/s, %.1f MB/s\n",
             pInfo->consumerId, totalRows, totalMsgs, (totalMsgs - lastTotalMsgs) * 1000.0 / deltaTime,
             currentLenOfMsg * 1000.0 / (1024 * 1024) / deltaTime);
      break;
    }
  }

  pInfo->consumeMsgCnt = totalMsgs;
  pInfo->consumeRowCnt = totalRows;
  pInfo->consumeLen = totalLenOfMsg;
}

void* ombConsumeThreadFunc(void* param) {
  SThreadInfo* pInfo = (SThreadInfo*)param;

  //################### set key ########################
  tmq_conf_t* conf = tmq_conf_new();
  // tmq_conf_set(conf, "td.connect.ip", "localhost");
  // tmq_conf_set(conf, "td.connect.port", "6030");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  // tmq_conf_set(conf, "td.connect.db", g_stConfInfo.dbName);
  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, pInfo);
  tmq_conf_set(conf, "group.id", "ombCgrp");
  // tmq_conf_set(conf, "msg.with.table.name", "true");
  // tmq_conf_set(conf, "client.id", "c-001");
  // tmq_conf_set(conf, "enable.auto.commit", "true");
  tmq_conf_set(conf, "enable.auto.commit", "false");
  // tmq_conf_set(conf, "auto.commit.interval.ms", "1000");
  // tmq_conf_set(conf, "auto.offset.reset", "none");
  // tmq_conf_set(conf, "auto.offset.reset", "earliest");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  //
  if (g_stConfInfo.useSnapshot) {
    tmq_conf_set(conf, "experimental.snapshot.enable", "true");
  }

  pInfo->tmq = tmq_consumer_new(conf, NULL, 0);

  tmq_conf_destroy(conf);

  //################### set topic ##########################
  pInfo->topicList = tmq_list_new();
  tmq_list_append(pInfo->topicList, g_stConfInfo.topic);

  if ((NULL == pInfo->tmq) || (NULL == pInfo->topicList)) {
    taosFprintfFile(g_fp, "create consumer fail! tmq is null or topicList is null\n");
    return NULL;
  }

  int32_t err = tmq_subscribe(pInfo->tmq, pInfo->topicList);
  if (err != 0) {
    pError("tmq_subscribe() fail, reason: %s\n", tmq_err2str(err));
    taosFprintfFile(g_fp, "tmq_subscribe() fail! reason: %s\n", tmq_err2str(err));
    return NULL;
  }

  tmq_list_destroy(pInfo->topicList);
  pInfo->topicList = NULL;

  omb_loop_consume(pInfo);

  err = tmq_unsubscribe(pInfo->tmq);
  if (err != 0) {
    pError("tmq_unsubscribe() fail, reason: %s\n", tmq_err2str(err));
    taosFprintfFile(g_fp, "tmq_unsubscribe()! reason: %s\n", tmq_err2str(err));
  }

  err = tmq_consumer_close(pInfo->tmq);
  if (err != 0) {
    pError("tmq_consumer_close() fail, reason: %s\n", tmq_err2str(err));
    taosFprintfFile(g_fp, "tmq_consumer_close()! reason: %s\n", tmq_err2str(err));
  }
  pInfo->tmq = NULL;

  return NULL;
}

static int queryDbExec(TAOS* taos, char* command, QUERY_TYPE type) {
  TAOS_RES* res = taos_query(taos, command);
  int32_t   code = taos_errno(res);

  if (code != 0) {
    pPrint("%s Failed to execute <%s>, reason: %s %s", GREEN, command, taos_errstr(res), NC);
    taos_free_result(res);
    return -1;
  }

  if (INSERT_TYPE == type) {
    int affectedRows = taos_affected_rows(res);
    taos_free_result(res);
    return affectedRows;
  }

  taos_free_result(res);
  return 0;
}

void* ombProduceThreadFunc(void* param) {
  SThreadInfo* pInfo = (SThreadInfo*)param;

  pInfo->taos = createNewTaosConnect();
  if (pInfo->taos == NULL) {
    taosFprintfFile(g_fp, "taos_connect() fail, can not start producers!\n");
    return NULL;
  }

  int64_t affectedRowsTotal = 0;
  int64_t sendMsgs = 0;

  uint32_t totalSendLoopTimes =
      g_stConfInfo.runDurationMinutes * 60 * 1000 / SEND_TIME_UNIT;  // send some msgs per 10ms
  uint32_t batchPerTblTimes = pInfo->producerRate / 100 / g_stConfInfo.batchSize;
  uint32_t remainder = (pInfo->producerRate / 100) % g_stConfInfo.batchSize;
  if (remainder) {
    batchPerTblTimes += 1;
  }

  char* sqlBuf = taosMemoryMalloc(MAX_SQL_LEN);
  if (NULL == sqlBuf) {
    printf("malloc fail for sqlBuf\n");
    taos_close(pInfo->taos);
    pInfo->taos = NULL;
    return NULL;
  }

  printf("Produce Info: totalSendLoopTimes: %d, batchPerTblTimes: %d, producerRate: %d\n", totalSendLoopTimes,
         batchPerTblTimes, pInfo->producerRate);

  char ctbName[128] = {0};
  sprintf(ctbName, "%s.ctb%d", g_stConfInfo.dbName, pInfo->consumerId);

  int64_t lastPrintTime = taosGetTimestampUs();
  int64_t totalMsgLen = 0;
  // int64_t timeStamp = taosGetTimestampUs();
  while (totalSendLoopTimes) {
    int64_t startTs = taosGetTimestampUs();
    for (int i = 0; i < batchPerTblTimes; ++i) {
      uint32_t msgsOfSql = g_stConfInfo.batchSize;
      if ((i == batchPerTblTimes - 1) && (0 != remainder)) {
        msgsOfSql = remainder;
      }
      int len = 0;
      len += snprintf(sqlBuf + len, MAX_SQL_LEN - len, "insert into %s values ", ctbName);
      for (int j = 0; j < msgsOfSql; j++) {
        int64_t timeStamp = taosGetTimestampNs();
        len += snprintf(sqlBuf + len, MAX_SQL_LEN - len, "(%" PRId64 ", \"%s\")", timeStamp, g_payload);
        sendMsgs++;
        pInfo->totalProduceMsgs++;
      }

      totalMsgLen += len;
      pInfo->totalMsgsLen += len;

      int64_t affectedRows = queryDbExec(pInfo->taos, sqlBuf, INSERT_TYPE);
      if (affectedRows < 0) {
        taos_close(pInfo->taos);
        pInfo->taos = NULL;
        taosMemoryFree(sqlBuf);
        return NULL;
      }

      affectedRowsTotal += affectedRows;

      // printf("Produce Info: affectedRows: %" PRId64 "\n", affectedRows);
    }
    totalSendLoopTimes -= 1;

    // calc spent time
    int64_t currentTs = taosGetTimestampUs();
    int64_t delta = currentTs - startTs;
    if (delta < SEND_TIME_UNIT * 1000) {
      int64_t sleepLen = (int32_t)(SEND_TIME_UNIT * 1000 - delta);
      // printf("sleep %" PRId64 " us, use time: %" PRId64 " us\n", sleepLen, delta);
      taosUsleep((int32_t)sleepLen);
    }

    currentTs = taosGetTimestampUs();
    delta = currentTs - lastPrintTime;
    if (delta > 10 * 1000 * 1000) {
      printf("producer[%d] info: %" PRId64 " msgs, %" PRId64 " Byte, %" PRId64 " us, totalSendLoopTimes: %d\n",
             pInfo->consumerId, sendMsgs, totalMsgLen, delta, totalSendLoopTimes);
      printf("producer[%d] rate: %1.f msgs/s, %1.f KB/s\n", pInfo->consumerId, sendMsgs * 1000.0 * 1000 / delta,
             (totalMsgLen / 1024.0) / (delta / (1000 * 1000)));
      lastPrintTime = currentTs;
      sendMsgs = 0;
      totalMsgLen = 0;
    }
  }

  printf("affectedRowsTotal: %" PRId64 "\n", affectedRowsTotal);
  taos_close(pInfo->taos);
  pInfo->taos = NULL;
  taosMemoryFree(sqlBuf);
  return NULL;
}

void printProduceInfo(int64_t start) {
  int64_t totalMsgs = 0;
  int64_t totalLenOfMsgs = 0;
  for (int i = 0; i < g_stConfInfo.producers; i++) {
    totalMsgs += g_stConfInfo.stProdThreads[i].totalProduceMsgs;
    totalLenOfMsgs += g_stConfInfo.stProdThreads[i].totalMsgsLen;
  }

  int64_t end = taosGetTimestampUs();

  int64_t t = end - start;
  if (0 == t) t = 1;

  double tInMs = (double)t / 1000000.0;
  printf("Spent %.3f seconds to prod %" PRIu64 " msgs, %" PRIu64 " Byte\n\n", tInMs, totalMsgs, totalLenOfMsgs);

  printf("Spent %.3f seconds to prod %" PRIu64 " msgs with %d producer(s), throughput: %.3f msgs/s, %.1f MB/s\n\n",
         tInMs, totalMsgs, g_stConfInfo.producers, (double)totalMsgs / tInMs,
         (double)totalLenOfMsgs / (1024.0 * 1024) / tInMs);
  return;
}

void startOmbConsume() {
  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);

  if (0 != g_stConfInfo.producers) {
    TAOS* taos = createNewTaosConnect();
    if (taos == NULL) {
      taosFprintfFile(g_fp, "taos_connect() fail, can not create db, stbl, ctbl, topic!\n");
      return;
    }

    char stbName[16] = "stb";
    char ctbPrefix[16] = "ctb";

    char sql[256] = {0};
    sprintf(sql, "drop database if exists %s", g_stConfInfo.dbName);
    printf("SQL: %s\n", sql);
    queryDbExec(taos, sql, NO_INSERT_TYPE);

    sprintf(sql, "create database if not exists %s precision 'ns' vgroups %d", g_stConfInfo.dbName,
            g_stConfInfo.producers);
    printf("SQL: %s\n", sql);
    queryDbExec(taos, sql, NO_INSERT_TYPE);

    sprintf(sql, "create stable %s.%s (ts timestamp, payload binary(%d)) tags (t bigint) ", g_stConfInfo.dbName,
            stbName, g_stConfInfo.payloadLen);
    printf("SQL: %s\n", sql);
    queryDbExec(taos, sql, NO_INSERT_TYPE);

    for (int i = 0; i < g_stConfInfo.producers; i++) {
      sprintf(sql, "create table %s.%s%d using %s.stb tags(%d) ", g_stConfInfo.dbName, ctbPrefix, i,
              g_stConfInfo.dbName, i);
      printf("SQL: %s\n", sql);
      queryDbExec(taos, sql, NO_INSERT_TYPE);
    }

    // create topic
    sprintf(sql, "create topic %s as stable %s.%s", g_stConfInfo.topic, g_stConfInfo.dbName, stbName);
    printf("SQL: %s\n", sql);
    queryDbExec(taos, sql, NO_INSERT_TYPE);

    int32_t producerRate = ceil(((double)g_stConfInfo.producerRate) / g_stConfInfo.producers);

    printf("==== create %d produce thread ====\n", g_stConfInfo.producers);
    for (int32_t i = 0; i < g_stConfInfo.producers; ++i) {
      g_stConfInfo.stProdThreads[i].consumerId = i;
      g_stConfInfo.stProdThreads[i].producerRate = producerRate;
      taosThreadCreate(&(g_stConfInfo.stProdThreads[i].thread), &thattr, ombProduceThreadFunc,
                       (void*)(&(g_stConfInfo.stProdThreads[i])));
    }

    if (0 == g_stConfInfo.numOfThread) {
      int64_t start = taosGetTimestampUs();
      for (int32_t i = 0; i < g_stConfInfo.producers; i++) {
        taosThreadJoin(g_stConfInfo.stProdThreads[i].thread, NULL);
        taosThreadClear(&g_stConfInfo.stProdThreads[i].thread);
      }

      printProduceInfo(start);

      taosFprintfFile(g_fp, "==== close tmqlog ====\n");
      taosCloseFile(&g_fp);
      taos_close(taos);
      return;
    }

    taos_close(taos);
  }

  // pthread_create one thread to consume
  taosFprintfFile(g_fp, "==== create %d consume thread ====\n", g_stConfInfo.numOfThread);
  for (int32_t i = 0; i < g_stConfInfo.numOfThread; ++i) {
    g_stConfInfo.stThreads[i].consumerId = i;
    taosThreadCreate(&(g_stConfInfo.stThreads[i].thread), &thattr, ombConsumeThreadFunc,
                     (void*)(&(g_stConfInfo.stThreads[i])));
  }

  int64_t start = taosGetTimestampUs();

  for (int32_t i = 0; i < g_stConfInfo.numOfThread; i++) {
    taosThreadJoin(g_stConfInfo.stThreads[i].thread, NULL);
    taosThreadClear(&g_stConfInfo.stThreads[i].thread);
  }

  int64_t end = taosGetTimestampUs();

  int64_t totalRows = 0;
  int64_t totalMsgs = 0;
  int64_t totalLenOfMsgs = 0;
  for (int32_t i = 0; i < g_stConfInfo.numOfThread; i++) {
    totalMsgs += g_stConfInfo.stThreads[i].consumeMsgCnt;
    totalLenOfMsgs += g_stConfInfo.stThreads[i].consumeLen;
    totalRows += g_stConfInfo.stThreads[i].consumeRowCnt;
  }

  int64_t t = end - start;
  if (0 == t) t = 1;

  double tInMs = (double)t / 1000000.0;
  taosFprintfFile(
      g_fp, "Spent %.3f seconds to poll msgs: %" PRIu64 " with %d thread(s), throughput: %.3f msgs/s, %.1f MB/s\n\n",
      tInMs, totalMsgs, g_stConfInfo.numOfThread, (double)(totalMsgs / tInMs),
      (double)totalLenOfMsgs / (1024 * 1024) / tInMs);

  printf("Spent %.3f seconds to cons rows: %" PRIu64 " msgs: %" PRIu64
         " with %d thread(s), throughput: %.3f msgs/s, %.1f MB/s\n\n",
         tInMs, totalRows, totalMsgs, g_stConfInfo.numOfThread, (double)(totalMsgs / tInMs),
         (double)totalLenOfMsgs / (1024 * 1024) / tInMs);

  taosFprintfFile(g_fp, "==== close tmqlog ====\n");
  taosCloseFile(&g_fp);

  return;
}

int main(int32_t argc, char* argv[]) {
  parseArgument(argc, argv);

  if (0 != strlen(g_stConfInfo.topic)) {
    startOmbConsume();
    return 0;
  }

  int32_t retCode = getConsumeInfo();
  if (0 != retCode) {
    return -1;
  }

  saveConfigToLogFile();

  tmqSetSignalHandle();

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);

  // pthread_create one thread to consume
  taosFprintfFile(g_fp, "==== create %d consume thread ====\n", g_stConfInfo.numOfThread);
  for (int32_t i = 0; i < g_stConfInfo.numOfThread; ++i) {
    taosThreadCreate(&(g_stConfInfo.stThreads[i].thread), &thattr, consumeThreadFunc,
                     (void*)(&(g_stConfInfo.stThreads[i])));
  }

  int64_t start = taosGetTimestampUs();

  for (int32_t i = 0; i < g_stConfInfo.numOfThread; i++) {
    taosThreadJoin(g_stConfInfo.stThreads[i].thread, NULL);
    taosThreadClear(&g_stConfInfo.stThreads[i].thread);
  }

  int64_t end = taosGetTimestampUs();

  int64_t totalMsgs = 0;
  for (int32_t i = 0; i < g_stConfInfo.numOfThread; i++) {
    totalMsgs += g_stConfInfo.stThreads[i].consumeMsgCnt;
  }

  int64_t t = end - start;
  if (0 == t) t = 1;

  double tInMs = (double)t / 1000000.0;
  taosFprintfFile(g_fp,
                  "Spent %.3f seconds to poll msgs: %" PRIu64 " with %d thread(s), throughput: %.3f msgs/second\n\n",
                  tInMs, totalMsgs, g_stConfInfo.numOfThread, (double)(totalMsgs / tInMs));

  taosFprintfFile(g_fp, "==== close tmqlog ====\n");
  taosCloseFile(&g_fp);

  return 0;
}
