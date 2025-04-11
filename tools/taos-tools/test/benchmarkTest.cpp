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

// declare
int getCodeFromResp(char *responseBuf);
int getServerVersionRest(int16_t rest_port);
void appendResultBufToFile(char *resultBuf, char * filePath);
int32_t replaceChildTblName(char *inSql, char *outSql, int tblIndex);
int32_t calcGroupIndex(char* dbName, char* tbName, int32_t groupCnt);


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
  ret = getCodeFromResp("http response failed.");
  ASSERT_EQ(ret, -1);

  //  json format
  ret = getCodeFromResp("{json valid test}");
  ASSERT_EQ(ret, -1);

  // code 
  ret = getCodeFromResp("{\"code\":\"invalid code type\"}");
  ASSERT_EQ(ret, -1);

  // des
  ret = getCodeFromResp("{\"code\":\100, \"desc\":12}");
  ASSERT_EQ(ret, -1);

  // des
  ret = getCodeFromResp("{\"code\":\100, \"desc\":12}");
  ASSERT_EQ(ret, -1);

  // succ
  ret = getCodeFromResp("{\"code\":\100, \"desc\":\"desc valid\"}");
  ASSERT_EQ(ret, 0);
}

// getServerVersionRest
TEST(benchUtil, getServerVersionRest) {
  int ret;
  
  // "{"
  int16_t invalidPort = 100;
  int32_t ret = getServerVersionRest(invalidPort);
  ASSERT_EQ(ret, -1);
}

// baseic
TEST(BenchUtil, Base) {
  int ret;
  // check crash
  engineError("util", "taos_connect", 1);

  // append result to file
  appendResultBufToFile("check null file", NULL);

  // replaceChildTblName
  char szOut[128] = "";
  ret = replaceChildTblName("select * from xxx;", szOut, 0);
  ASSERT_EQ(ret, -1);

  // toolsGetTimestamp
  int64_t now;
  now = toolsGetTimestamp(TSDB_TIME_PRECISION_MILLI);
  ASSERT_GE(now, 1700000000000)
  now = toolsGetTimestamp(TSDB_TIME_PRECISION_MICRO);
  ASSERT_GE(now, 1700000000000000)
  now = toolsGetTimestamp(TSDB_TIME_PRECISION_NANO);
  ASSERT_GE(now, 1700000000000000000)

  // calc groups
  ret = calcGroupIndex(NULL, NULL, 5);
  ASSERT_EQ(ret, -1);

  ret = calcGroupIndex(NULL, NULL, 5);
  ASSERT_EQ(ret, -1);
}



// main
int main(int argc, char **argv) {
  printf("Hello world taosBenchmark unit test for C \n");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}