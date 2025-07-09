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

#include <gtest/gtest.h>
#include <iostream>
#include "pub.h"
#include "bench.h"
#include "benchLog.h"

#ifdef __cplusplus
extern "C" {
#endif

// benchMain.c global
SArguments*    g_arguments;
SQueryMetaInfo g_queryInfo;
STmqMetaInfo   g_tmqInfo;
bool           g_fail = false;
uint64_t       g_memoryUsage = 0;
tools_cJSON*   root;
extern char    g_configDir[MAX_PATH_LEN];

#define CLIENT_INFO_LEN   20
static char     g_client_info[CLIENT_INFO_LEN] = {0};

int32_t         g_majorVersionOfClient = 0;
// set flag if command passed, see ARG_OPT_ ???
uint64_t        g_argFlag = 0;

// declare fun
int getCodeFromResp(char *responseBuf);
int getServerVersionRest(int16_t rest_port);
void appendResultBufToFile(char *resultBuf, char * filePath);
int32_t replaceChildTblName(char *inSql, char *outSql, int tblIndex);
int32_t calcGroupIndex(char* dbName, char* tbName, int32_t groupCnt);
void prompt(bool nonStopMode);
void printErrCmdCodeStr(char *cmd, int32_t code, TAOS_RES *res);

#ifdef __cplusplus
}
#endif

TEST(jsonTest, strToLowerCopy) {
  // strToLowerCopy
  const char* arr[][2] = {
    {"ABC","abc"},
    {"Http://Localhost:6041","http://localhost:6041"},
    {"DEF","def"}
  };

  int rows = sizeof(arr) / sizeof(arr[0]);
  for (int i = 0; i < rows; i++) {
    char *p1 = (char *)arr[i][1];
    char *p2 = strToLowerCopy((char *)arr[i][0]);
    printf("p1: %s\n", p1);
    printf("p2: %s\n", p2);
    int32_t cmp = strcmp(p1, p2);
    if (p2) {
      free(p2);
    }    
    ASSERT_EQ(cmp, 0);
  }

  // null
  char * p = strToLowerCopy(NULL);
  ASSERT_EQ(p, nullptr);
}

// getCodeFromResp
TEST(benchUtil, getCodeFromResp) {
  int ret;
  
  // "{"
  ret = getCodeFromResp((char *)"http response failed.");
  ASSERT_EQ(ret, -1);

  //  json format
  ret = getCodeFromResp((char *)"{json valid test}");
  ASSERT_EQ(ret, -1);

  // code 
  ret = getCodeFromResp((char *)"{\"code\":\"invalid code type\"}");
  ASSERT_EQ(ret, -1);

  // des
  ret = getCodeFromResp((char *)"{\"code\":100, \"desc\":12}");
  ASSERT_EQ(ret, -1);

  // des
  ret = getCodeFromResp((char *)"{\"code\":100, \"desc\":12}");
  ASSERT_EQ(ret, -1);

  // succ
  ret = getCodeFromResp((char *)"{\"code\":100, \"desc\":\"desc valid\"}");
  ASSERT_EQ(ret, 100);
}

TEST(benchUtil, convertHostToServAddr) {
  struct sockaddr_in  serv_addr;
  ASSERT_EQ(convertHostToServAddr(NULL, 0, &serv_addr), -1);
  ASSERT_EQ(convertHostToServAddr((char *)"invalid.host", 0, &serv_addr), -1);
}

TEST(benchUtil, printErrCmdCodeStr) {
  char msg[600];
  memset(msg, 'a', sizeof(msg));
  msg[sizeof(msg) - 1] = 0;
  printErrCmdCodeStr(msg, 0, NULL);
}

// basic
TEST(benchUtil, Base) {
  int ret;
  // check crash
  engineError((char *)"util", (char *)"taos_connect", 1);

  // append result to file
  appendResultBufToFile((char *)"check null file", NULL);

  // replaceChildTblName
  char szOut[128] = "";
  ret = replaceChildTblName((char *)"select * from xxx;", szOut, 0);
  ASSERT_EQ(ret, -1);

  // toolsGetTimestamp
  int64_t now = 0;
  now = toolsGetTimestamp(TSDB_TIME_PRECISION_MILLI);
  ASSERT_GE(now, 1700000000000);
  now = toolsGetTimestamp(TSDB_TIME_PRECISION_MICRO);
  ASSERT_GE(now, 1700000000000000);
  now = toolsGetTimestamp(TSDB_TIME_PRECISION_NANO);
  ASSERT_GE(now, 1700000000000000000);

  // calc groups
  ret = calcGroupIndex(NULL, NULL, 5);
  ASSERT_EQ(ret, -1);

  // bench
  ASSERT_EQ(benchCalloc(100000000000, 1000000000000, false), nullptr);

  // close
  closeBenchConn(NULL);
}

// main
int main(int argc, char **argv) {
  // init
  initLog();
  g_arguments = (SArguments *)calloc(1, sizeof(SArguments));
  printf("Hello world taosBenchmark unit test for C \n");
  testing::InitGoogleTest(&argc, argv);

  // run
  int ret =  RUN_ALL_TESTS();

  // exit
  exitLog();
  free(g_arguments);
  g_arguments = NULL;
  return ret;
}

