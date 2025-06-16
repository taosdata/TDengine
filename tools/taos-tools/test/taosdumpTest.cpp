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
#include <bench.h>
#include "toolsdef.h"
#include "toolsdef.h"
#include "toolscJson.h"

#ifdef __cplusplus
extern "C" {
#include "dump.h"
#endif



SArguments*  g_arguments;
bool           g_fail = false;
uint64_t       g_memoryUsage = 0;
extern char    g_configDir[MAX_PATH_LEN];
SQueryMetaInfo g_queryInfo;
STmqMetaInfo   g_tmqInfo;
tools_cJSON*   root;


char *typeToStr(int type);
bool replaceCopy(char *des, char *src);
uint64_t        g_argFlag = 0;
#ifdef __cplusplus
}
#endif

TEST(taosdump, toolsSys) {
  // errorPrintReqArg3
  errorPrintReqArg3((char *)"taosdump", (char *)"test parameters");
  printf("ut function errorPrintReqArg3 ....................  [Passed]\n");

  // setConsoleEcho
  setConsoleEcho(false);
  setConsoleEcho(true);
  printf("ut function setConsoleEcho .......................  [Passed]\n");
}


TEST(taosdump, typeToStr) {
  assert(strcmp(typeToStr(1), "bool") == 0);
  assert(strcmp(typeToStr(2), "tinyint") == 0);
  assert(strcmp(typeToStr(3), "smallint") == 0);
  assert(strcmp(typeToStr(4), "int") == 0);
  assert(strcmp(typeToStr(5), "bigint") == 0);
  assert(strcmp(typeToStr(6), "float") == 0);
  assert(strcmp(typeToStr(7), "double") == 0);
  assert(strcmp(typeToStr(8), "binary") == 0);
  assert(strcmp(typeToStr(9), "timestamp") == 0);
  assert(strcmp(typeToStr(10), "nchar") == 0);
  assert(strcmp(typeToStr(11), "tinyint unsigned") == 0);
  assert(strcmp(typeToStr(12), "smallint unsigned") == 0);
  assert(strcmp(typeToStr(13), "int unsigned") == 0);
  assert(strcmp(typeToStr(14), "bigint unsigned") == 0);
  assert(strcmp(typeToStr(15), "JSON") == 0);
  assert(strcmp(typeToStr(16), "varbinary") == 0);
  assert(strcmp(typeToStr(20), "geometry") == 0);
  assert(strcmp(typeToStr(22), "unknown") == 0); 
}

TEST(taosdump, replaceCopy) {
  char buf[128];

  // 1. No dot in source string
  strcpy(buf, "");
  assert(replaceCopy(buf, "abc") == false);
  assert(strcmp(buf, "abc") == 0);

  // 2. One dot in source string
  strcpy(buf, "");
  assert(replaceCopy(buf, "a.bc") == true);
  assert(strcmp(buf, "a_bc") == 0);

  // 3. Multiple dots in source string
  strcpy(buf, "");
  assert(replaceCopy(buf, "a.b.c.d") == true);
  assert(strcmp(buf, "a_b_c_d") == 0);

  // 4. Dot at the beginning
  strcpy(buf, "");
  assert(replaceCopy(buf, ".abc") == true);
  assert(strcmp(buf, "_abc") == 0);

  // 5. Dot at the end
  strcpy(buf, "");
  assert(replaceCopy(buf, "abc.") == true);
  assert(strcmp(buf, "abc_") == 0);

  // 6. Empty string
  strcpy(buf, "xxxx");
  assert(replaceCopy(buf, "") == false);
  assert(strcmp(buf, "") == 0);
}

TEST(taosdump, processResultValue) {
    char buf[256];
    int ret;

    // BOOL
    char bool_true = 1, bool_false = 0;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_BOOL, &bool_true, sizeof(bool_true));
    assert(strcmp(buf, "1") == 0);
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_BOOL, &bool_false, sizeof(bool_false));
    assert(strcmp(buf, "0") == 0);

    // TINYINT
    int8_t tiny = -123;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_TINYINT, &tiny, sizeof(tiny));
    assert(strcmp(buf, "-123") == 0);

    // SMALLINT
    int16_t small = -12345;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_SMALLINT, &small, sizeof(small));
    assert(strcmp(buf, "-12345") == 0);

    // INT
    int32_t i32 = 123456;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_INT, &i32, sizeof(i32));
    assert(strcmp(buf, "123456") == 0);

    // BIGINT
    int64_t i64 = -9876543210LL;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_BIGINT, &i64, sizeof(i64));
    assert(strcmp(buf, "-9876543210") == 0);

    // UTINYINT
    uint8_t utiny = 200;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_UTINYINT, &utiny, sizeof(utiny));
    assert(strcmp(buf, "200") == 0);

    // USMALLINT
    uint16_t usmall = 60000;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_USMALLINT, &usmall, sizeof(usmall));
    assert(strcmp(buf, "60000") == 0);

    // UINT
    uint32_t ui32 = 4000000000U;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_UINT, &ui32, sizeof(ui32));
    assert(strcmp(buf, "4000000000") == 0);

    // UBIGINT
    uint64_t ui64 = 1234567890123456789ULL;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_UBIGINT, &ui64, sizeof(ui64));
    assert(strcmp(buf, "1234567890123456789") == 0);

    // FLOAT
    float f = 3.14f;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_FLOAT, &f, sizeof(f));
    assert(strncmp(buf, "3.140", 5) == 0); // Accept rounding

    // DOUBLE
    double d = -2.718281828;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_DOUBLE, &d, sizeof(d));
    assert(strncmp(buf, "-2.718", 6) == 0);

    // BINARY
    char bin[] = "abc";
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_BINARY, bin, 3);
    assert(strstr(buf, "abc") != NULL);

    // NCHAR
    char nchar[] = "xyz";
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_NCHAR, nchar, 3);
    assert(strstr(buf, "xyz") != NULL);

    // TIMESTAMP
    int64_t ts = 1680000000000LL;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_TIMESTAMP, &ts, sizeof(ts));
    assert(strcmp(buf, "1680000000000") == 0);

    // NULL value
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_INT, NULL, 0);
    assert(strcmp(buf, "NULL") == 0);

}

int main(int argc, char **argv) {
  printf("hello world taosdump unit test for C\n");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}