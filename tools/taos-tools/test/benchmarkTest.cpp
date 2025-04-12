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
int32_t parseLocaltime(char* timestr, int64_t* time, int32_t timePrec, char delim);
int32_t parseLocaltimeWithDst(char* timestr, int64_t* time, int32_t timePrec, char delim);
int32_t parseTimeWithTz(char* timestr, int64_t* time, int32_t timePrec, char delim);
struct tm *tLocalTime(const time_t *timep, struct tm *result, char *buf);
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

TEST(jsonTest, Strnchr) {
  char haystack[] = "hello 'world'";
  char needle = 'r';
  int32_t len = sizeof(haystack) - 1;
  bool skipquote = false;

  char *result = tools_strnchr(haystack, needle, len, skipquote);
  ASSERT_NE(result, nullptr);

  skipquote = true;
  result = tools_strnchr(haystack, needle, len, skipquote);
  ASSERT_EQ(result, nullptr);
}

TEST(jsonTest, StringConversion) {

  char num[] = "12345";
  int32_t len = sizeof(num) - 1;
  int64_t result = tools_strnatoi(num, len);
  EXPECT_EQ(result, 12345);

  char hexNum1[] = "0X1A";
  len = sizeof(hexNum1) - 1;
  result = tools_strnatoi(hexNum1, len);
  EXPECT_EQ(result, 26);

  char hexNum2[] = "0x1a";
  len = sizeof(hexNum2) - 1;
  result = tools_strnatoi(hexNum2, len);
  EXPECT_EQ(result, 26);
}

TEST(jsonTest, ValidTimezoneWithColon) {
  char str[] = "+08:00";
  int64_t tzOffset;
  int32_t result = toolsParseTimezone(str, &tzOffset);
  EXPECT_EQ(result, 0);
  EXPECT_EQ(tzOffset, -28800);  // 8 小时转换为秒：8 * 3600
}


TEST(jsonTest, ValidTimeStrWithTDelimWithFraction) {
  char timestr[] = "2023-01-01T12:00:00.123";
  int64_t time;
  int32_t timePrec = TSDB_TIME_PRECISION_MILLI;
  char delim = 'T';
  int32_t result = parseLocaltimeWithDst(timestr, &time, timePrec, delim);
  EXPECT_EQ(result, 0);

  char timestr1[] = "2023-01-01 12:00:00.456";
  delim = 0;
  result = parseLocaltimeWithDst(timestr1, &time, timePrec, delim);
  EXPECT_EQ(result, 0);

  delim = 'a';
  result = parseLocaltimeWithDst(timestr1, &time, timePrec, delim);
  EXPECT_EQ(result, -1);

}

TEST(jsonTest, ValidUTCTimeWithTDelimMilliPrecisionNoFraction) {
  char timestr[] = "2023-01-01T12:00:00Z";
  int64_t time;
  int32_t timePrec = TSDB_TIME_PRECISION_MILLI;
  char delim = 'T';
  int32_t result = parseTimeWithTz(timestr, &time, timePrec, delim);
  EXPECT_EQ(result, 0);

  char timestr1[] = "2023-01-01 12:00:00.123456Z";
  int32_t timePrec1 = TSDB_TIME_PRECISION_MICRO;
  delim = 0;
  result = parseTimeWithTz(timestr1, &time, timePrec1, delim);
  EXPECT_EQ(result, 0);
}

TEST(ToolsParseTimeTest, TimeStrWithoutTNoTz) {
  char timestr[] = "2023-01-01 12:00:00";
  int64_t time;
  int32_t len = strlen(timestr);
  int32_t timePrec = 1;
  int8_t day_light = 0;
  int32_t result = toolsParseTime(timestr, &time, len, timePrec, day_light);
  EXPECT_EQ(result, 0);
}

TEST(ToolsParseTimeTest, TimeStrWithTNoTzCoverParseLocaltimeFp) {
  char timestr[] = "2024-05-10T14:30:00";
  int64_t time;
  int32_t len = strlen(timestr);
  int32_t timePrec = 1;  
  int8_t day_light = 0;  
  int32_t result = toolsParseTime(timestr, &time, len, timePrec, day_light);
  EXPECT_EQ(result, 0);  

  int64_t timestamp = toolsGetTimestampNs();
  EXPECT_GE(timestamp, 0);  
}

TEST(TLocalTimeTest, TimepIsNull) {
  time_t *nullTimep = NULL;
  struct tm result;
  char buf[10];
  struct tm *res = tLocalTime(nullTimep, &result, buf);
  EXPECT_EQ(res, nullptr);
}


TEST(TLocalTimeTest, ResultIsNull) {
  time_t currentTime = time(NULL);
  struct tm *nullResult = NULL;
  char buf[10];
  struct tm *res = tLocalTime(&currentTime, nullResult, buf);
  EXPECT_NE(res, nullptr);
  if (res == nullptr) {
      EXPECT_STREQ(buf, "NaN");
  }
}

TEST(TLocalTimeTest, ResultIsNotNull) {
  time_t currentTime = time(NULL);
  struct tm result;
  char buf[10];
  struct tm *res = tLocalTime(&currentTime, &result, buf);
  EXPECT_EQ(res, &result);
  EXPECT_NE(strcmp(buf, "NaN"), 0);
}

TEST(ToolsFormatTimestampTest, NegativeValuesPrecision) {
  char buf[100];
  int64_t val = -1;  
  int32_t precision = TSDB_TIME_PRECISION_NANO;
  char *result = toolsFormatTimestamp(buf, val, precision);
  EXPECT_EQ(result, buf);

  val = -1000;  
  precision = TSDB_TIME_PRECISION_MICRO;
  result = toolsFormatTimestamp(buf, val, precision);
  EXPECT_EQ(result, buf);

  val = -1000000;  
  precision = TSDB_TIME_PRECISION_MILLI;
  result = toolsFormatTimestamp(buf, val, precision);
  EXPECT_EQ(result, buf);
}

TEST(SetConsoleEchoTest, TestEcho) {
  int result = setConsoleEcho(true);
  EXPECT_EQ(result, 0);
  result = setConsoleEcho(false);
  EXPECT_EQ(result, 0);
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