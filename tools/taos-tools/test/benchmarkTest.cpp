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
#include "toolsdef.h"

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
void randomFillCols(uint16_t* cols, uint16_t max, uint16_t cnt);
uint32_t appendRowRuleOld(SSuperTable* stb, char* pstr, uint32_t len, int64_t timestamp);
int32_t parseFunArgs(char* value, uint8_t funType, int64_t* min ,int64_t* max, int32_t* step ,int32_t* period ,int32_t* offset);
uint8_t parseFuns(char* expr, float* multiple, float* addend, float* base, int32_t* random, int64_t* min ,int64_t* max, int32_t* step ,int32_t* period ,int32_t* offset);
int64_t tools_strnatoi(char *num, int32_t len);
char *tools_strnchr(char *haystack, char needle, int32_t len, bool skipquote);
int64_t parseFraction(char* str, char** end, int32_t timePrec);
int32_t toolsParseTimezone(char* str, int64_t* tzOffset);
int32_t parseTimeWithTz(char* timestr, int64_t* time, int32_t timePrec, char delim);

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

TEST(benchInsertMix, randomFillCols) {
  uint16_t max = 5;
  uint16_t cols[max];
  
  randomFillCols(cols, max, 5);
  for (uint16_t i = 0; i < max; i++) {
    assert(cols[i] == i);
  }

  memset(cols, 0, max * sizeof(uint16_t));
  randomFillCols(cols, max, 1);
  for (uint16_t i = 0; i < max; i++) {
    assert(cols[i] == i);
  }

  memset(cols, 0, max * sizeof(uint16_t));
  randomFillCols(cols, max, 3);
  for (uint16_t i = 0; i < max; i++) {
    assert(cols[i] < max);
  } 
  
}

TEST(benchJsonOpt, parseFunArgs) {
    int64_t min = 0, max = 0;
    int32_t step = 0, period = 0, offset = 0;
    
    char* input = "100,200,300,400)";
    uint32_t len = parseFunArgs(input, FUNTYPE_COUNT, &min, &max, &step, &period, &offset);
    assert(len == strlen("100,200,300,400") + 1);

    assert(min == 100);
    assert(max == 200);
    assert(period == 0);
    assert(offset == 400);

    min = 0;
    max = 0;
    step = 0;
    period = 0;
    offset = 0;
    input = "50,150,250,350)";
    len = parseFunArgs(strdup(input), FUNTYPE_CNT, &min, &max, &step, &period, &offset);
    assert(len == strlen("50,150,250,350") + 1);
    assert(min == 50);
    assert(max == 150);
    assert(period == 250);
    assert(offset == 350);

    min = -1;
    max = -1;
    step = -1;
    period = -1;
    offset = -1;
    input = "50,150,250";
    len = parseFunArgs(strdup(input), FUNTYPE_CNT, &min, &max, &step, &period, &offset);
    assert(len == 0);
    assert(min == -1);
    assert(max == -1);
    assert(period == -1);
    assert(offset == -1);


    min = -1;
    max = -1;
    step = -1;
    period = -1;
    offset = -1;
    input = "50";
    len = parseFunArgs(strdup(input), FUNTYPE_CNT, &min, &max, &step, &period, &offset);
    assert(len == 0);
    assert(min == -1);
    assert(max == -1);
    assert(period == -1);
    assert(offset == -1);

}

TEST(benchJsonOpt, parseFun) {
    float multiple = 0, addend = 0, base = 0; 
    int32_t random = -1; 
    int64_t min = -1, max = -1; 
    int32_t step = -1, period = -1, offset = -1;
    char* expr = "2.5*sin(10,20,30,40)+3.14-1.5*random(50)+100";
    uint8_t type = parseFuns(expr, &multiple, &addend, &base, &random, 
                           &min, &max, &step, &period, &offset);
    
    assert(type == FUNTYPE_SIN);
    assert(multiple == 2.5f);
    assert(addend == 3.14f);
    assert(base == 100.0f);
    assert(random == 50);
    assert(min == 10 && max == 20);
    printf("step: %d, period: %d, offset: %d\n", step, period, offset);
    assert(period == 30 && offset == 40);
}

TEST(toolstime, tools_strnatoi) {
    assert(tools_strnatoi("123", 3) == 123);
    assert(tools_strnatoi("0", 1) == 0);
    assert(tools_strnatoi("999999999", 9) == 999999999);
    
    assert(tools_strnatoi("00123", 5) == 123);
    assert(tools_strnatoi("000", 3) == 0);
    
    assert(tools_strnatoi("123", 5) == 123);
    assert(tools_strnatoi("0", 10) == 0);

    assert(tools_strnatoi("-123", 4) == 0);

    assert(tools_strnatoi("0x1A", 4) == 26);
    assert(tools_strnatoi("0X1A", 4) == 26);
    assert(tools_strnatoi("0x0", 3) == 0);
    assert(tools_strnatoi("0xFF", 4) == 255);
    assert(tools_strnatoi("0x12345678", 10) == 0x12345678);

    assert(tools_strnatoi("0xAbCdE", 7) == 0xABCDE);
    assert(tools_strnatoi("0XaBcDe", 7) == 0xABCDE);

    assert(tools_strnatoi("0x00FF", 6) == 255);

    assert(tools_strnatoi("0x123", 5) == 0x123);  

    assert(tools_strnatoi("12a3", 4) == 0);
    assert(tools_strnatoi("abc", 3) == 0);
    
  
    assert(tools_strnatoi("0x1G", 4) == 0);
    assert(tools_strnatoi("0x@#", 4) == 0);


    assert(tools_strnatoi("0x", 2) == 0);         
    assert(tools_strnatoi("x123", 4) == 0);       


    assert(tools_strnatoi("", 0) == 0);
    assert(tools_strnatoi("", 5) == 0);           

    assert(tools_strnatoi("-123", 4) == 0);
    assert(tools_strnatoi("0x-FF", 5) == 0);

    assert(tools_strnatoi("9223372036854775807", 19) == 9223372036854775807LL); // INT64_MAX

    assert(tools_strnatoi("0x7FFFFFFFFFFFFFFF", 18) == 0x7FFFFFFFFFFFFFFFLL); // INT64_MAX 

    assert(tools_strnatoi("123", 0) == 0);

    assert(tools_strnatoi("123", 100) == 123);
    assert(tools_strnatoi("0x123", 100) == 0x123);

    assert(tools_strnatoi(" 123", 4) == 0);
    assert(tools_strnatoi("0x 12", 5) == 0);
}

TEST(toolstime, tools_strnchr) {
  char *str = "abcdefg";
  char *p = tools_strnchr(str, 'd', 7, false);
  assert(p != NULL && *p == 'd'); 

  p = tools_strnchr(str, 'x', 7, false);
  assert(p == NULL);

  str = "test";
  p = tools_strnchr(str, 't', 4, false);
  assert(p != NULL && *p == 't');
  
  str = "a'bc'de\"fg\"h";
  p = tools_strnchr(str, ',', strlen(str), true);
  assert(p == NULL);
  str = "a'bc'de\"fg\"h";
  p = tools_strnchr(str, 'x', strlen(str), true);
  assert(p == NULL); 
  str = "a'bc'de\"fg\"h";
  char *p_single = tools_strnchr(str, '\'', strlen(str), true); 
  char *p_double = tools_strnchr(str, '\"', strlen(str), true); 
  assert(p_single == NULL && p_double == NULL); 

}

TEST(toolstime, parseFraction) {
  char* input = "1000";
  char *end = NULL;
  int32_t timePrec = TSDB_TIME_PRECISION_MILLI;

  // valid
  int64_t fraction = parseFraction(input, &end, timePrec);
  assert(fraction == 100);
  
  input = "1234567";
  end = NULL;
  timePrec = TSDB_TIME_PRECISION_MICRO;
  fraction = parseFraction(input, &end, timePrec);
  assert(fraction == 123456);

  input = "1234567890";
  end = NULL;
  timePrec = TSDB_TIME_PRECISION_NANO;
  fraction = parseFraction(input, &end, timePrec);
  assert(fraction == 123456789);

  input = "abc";
  end = NULL;
  timePrec = TSDB_TIME_PRECISION_NANO;
  fraction = parseFraction(input, &end, timePrec);
  assert(fraction == -1);
}
TEST(toolstime, toolsParseTimezone) {
  int64_t tzOffset = 0;
  char* input = "+08:00";
  
  // valid
  int32_t ret = toolsParseTimezone(input, &tzOffset);
  assert(ret == 0 && tzOffset == -28800); // +8 hours in seconds

  input = "-05:30";
  ret = toolsParseTimezone(input, &tzOffset);
  assert(ret == 0 && tzOffset == 19800); // -5.5 hours in seconds

  input = "-05:";
  ret = toolsParseTimezone(input, &tzOffset);
  assert(ret == -1); // -5.5 hours in seconds

  input = "-05:70";
  ret = toolsParseTimezone(input, &tzOffset);
  assert(ret == -1); // -5.5 hours in seconds

  input = "invalid";
  ret = toolsParseTimezone(input, &tzOffset);
  assert(ret == -1);

  input = "+12:00";
  ret = toolsParseTimezone(input, &tzOffset);
  assert(ret == 0 && tzOffset == -43200); // +12 hours in seconds
}

TEST(toolstime, parseTimeWithTz) {
  int64_t time = 0;
  int32_t timePrec = TSDB_TIME_PRECISION_MILLI;
  char delim = 'T';
  
  // valid
  char* input = "2023-01-01T08:00:00+08:00";
  int32_t ret = parseTimeWithTz(input, &time, timePrec, delim);
  assert(ret == 0 && time == 1672531200000); // expected timestamp in milliseconds

  input = "2023-10-01T12:00:00-05:30";
  ret = parseTimeWithTz(input, &time, timePrec, delim);
  assert(ret == 0 && time == 1706767200000); // expected timestamp in milliseconds

  input = "2023-10-01T12:00:00Z";
  ret = parseTimeWithTz(input, &time, timePrec, delim);
  assert(ret == 0 && time == 1706745600000); // expected timestamp in milliseconds

  input = "invalid-time";
  ret = parseTimeWithTz(input, &time, timePrec, delim);
  assert(ret == -1);

  input = "2023-10-01T12:00:00+25:00"; // invalid timezone
  ret = parseTimeWithTz(input, &time, timePrec, delim);
  assert(ret == -1);
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