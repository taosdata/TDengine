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
#include <cstring>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

#include "os.h"
#include "tlog.h"

#ifdef WINDOWS
TEST(osStringTests, strsepNormalInput) {
  char       str[] = "This is a test string.";
  char*      ptr = str;
  char*      tok = NULL;
  const char delim[] = " ";

  while ((tok = strsep(&ptr, delim)) != NULL) {
    printf("%s\n", tok);
  }
  EXPECT_STREQ(tok, nullptr);
  EXPECT_EQ(ptr, nullptr);
}

TEST(osStringTests, strsepEmptyInput) {
  char*      str = "";
  char*      ptr = str;
  char*      tok = NULL;
  const char delim[] = " ";

  while ((tok = strsep(&ptr, delim)) != NULL) {
    printf("%s\n", tok);
  }

  EXPECT_STREQ(tok, nullptr);
  EXPECT_EQ(ptr, nullptr);
}

TEST(osStringTests, strsepNullInput) {
  char*      str = NULL;
  char*      ptr = str;
  char*      tok = NULL;
  const char delim[] = " ";

  while ((tok = strsep(&ptr, delim)) != NULL) {
    printf("%s\n", tok);
  }

  EXPECT_STREQ(tok, nullptr);
  EXPECT_EQ(ptr, nullptr);
}

TEST(osStringTests, strndupNormalInput) {
  const char s[] = "This is a test string.";
  int        size = strlen(s) + 1;
  char*      s2 = taosStrndup(s, size);

  EXPECT_STREQ(s, s2);

  free(s2);
}
#endif

TEST(osStringTests, osUcs4Tests1) {
  TdUcs4 f1_ucs4[] = {0x0048, 0x0065, 0x006C, 0x006C, 0x006F, 0x0000};
  TdUcs4 f2_ucs4[] = {0x0048, 0x0065, 0x006C, 0x006C, 0x006F, 0x0000};

  EXPECT_EQ(taosUcs4Compare(f1_ucs4, f2_ucs4, sizeof(f1_ucs4)), 0);

  TdUcs4 f3_ucs4[] = {0x0048, 0x0065, 0x006C, 0x006C, 0x006F, 0x0020, 0x0077,
                      0x006F, 0x0072, 0x006C, 0x0064, 0x0021, 0x0000};
  TdUcs4 f4_ucs4[] = {0x0048, 0x0065, 0x006C, 0x006C, 0x006F, 0x0000};

  EXPECT_GT(taosUcs4Compare(f3_ucs4, f4_ucs4, sizeof(f3_ucs4)), 0);

  TdUcs4 f5_ucs4[] = {0x0048, 0x0065, 0x006C, 0x006C, 0x006F, 0x0000};
  TdUcs4 f6_ucs4[] = {0x0048, 0x0065, 0x006C, 0x006C, 0x006F, 0x0020, 0x0077,
                      0x006F, 0x0072, 0x006C, 0x0064, 0x0021, 0x0000};

  EXPECT_LT(taosUcs4Compare(f5_ucs4, f6_ucs4, sizeof(f5_ucs4)), 0);
}

TEST(osStringTests, osUcs4lenTests2) {
  TdUcs4 ucs4_1[] = {'H', 'e', 'l', 'l', 'o', '\0'};
  EXPECT_EQ(taosUcs4len(ucs4_1), 5);

  TdUcs4 ucs4_2[] = {'\0'};
  EXPECT_EQ(taosUcs4len(ucs4_2), 0);

  TdUcs4 ucs4_3[] = {'C', 'h', 'i', 'n', 'a', 0x4E2D, 0x6587, '\0'};
  EXPECT_EQ(taosUcs4len(ucs4_3), 7);
}

TEST(osStringTests, ostsnprintfTests) {
  char    buffer[50] = {0};
  int64_t ret;

  ret = tsnprintf(buffer, sizeof(buffer), "Hello, %s!", "World");
  EXPECT_EQ(ret, 13);
  EXPECT_STREQ(buffer, "Hello, World!");

  memset(buffer, 0, sizeof(buffer));
  ret = tsnprintf(buffer, 10, "Hello, %s!", "World");
  EXPECT_EQ(ret, 9);
  EXPECT_EQ(strncmp(buffer, "Hello, Wo", 9), 0);

  memset(buffer, 0, sizeof(buffer));
  ret = tsnprintf(buffer, 10, "Hello%s", "World");
  EXPECT_EQ(ret, 9);
  EXPECT_EQ(strncmp(buffer, "HelloWorl", 9), 0);

  memset(buffer, 0, sizeof(buffer));
  ret = tsnprintf(buffer, 0, "Hello, %s!", "World");
  EXPECT_EQ(ret, 0);

  memset(buffer, 0, sizeof(buffer));
  ret = tsnprintf(buffer, SIZE_MAX + 1, "Hello, %s!", "World");
  EXPECT_EQ(ret, 0);

  memset(buffer, 0, sizeof(buffer));
  ret = tsnprintf(buffer, sizeof(buffer), "");
  EXPECT_EQ(ret, 0);
  EXPECT_STREQ(buffer, "");

  memset(buffer, 0, sizeof(buffer));
  ret = tsnprintf(buffer, sizeof(buffer), "Number: %d", 42);
  EXPECT_EQ(ret, 10);
  EXPECT_STREQ(buffer, "Number: 42");

  memset(buffer, 0, sizeof(buffer));
  ret = tsnprintf(buffer, sizeof(buffer), "Float: %.2f", 3.14);
  EXPECT_EQ(ret, 11);
  EXPECT_STREQ(buffer, "Float: 3.14");
}
TEST(osStringTests, osStr2Int64) {
  int64_t val;
  int32_t result;

  // 测试空指针输入
  result = taosStr2int64(NULL, &val);
  TD_ALWAYS_ASSERT(result == TSDB_CODE_INVALID_PARA);

  result = taosStr2int64("123", NULL);
  ASSERT_NE(result, 0);

  // 测试无效输入
  result = taosStr2int64("abc", &val);
  ASSERT_NE(result, 0);

  result = taosStr2int64("", &val);
  ASSERT_NE(result, 0);

  char large_num[50];
  snprintf(large_num, sizeof(large_num), "%lld", LLONG_MAX);
  result = taosStr2int64(large_num, &val);
  TD_ALWAYS_ASSERT(result == 0);
  TD_ALWAYS_ASSERT(val == LLONG_MAX);

  snprintf(large_num, sizeof(large_num), "%lld", LLONG_MIN);
  result = taosStr2int64(large_num, &val);
  TD_ALWAYS_ASSERT(result == 0);
  TD_ALWAYS_ASSERT(val == LLONG_MIN);

  result = taosStr2int64("123abc", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);

  result = taosStr2int64("abc123", &val);
  ASSERT_NE(result, 0);
  // 测试有效的整数字符串
  result = taosStr2int64("12345", &val);
  TD_ALWAYS_ASSERT(result == 0);
  TD_ALWAYS_ASSERT(val == 12345);

  result = taosStr2int64("-12345", &val);
  TD_ALWAYS_ASSERT(result == 0);
  TD_ALWAYS_ASSERT(val == -12345);

  result = taosStr2int64("0", &val);
  TD_ALWAYS_ASSERT(result == 0);
  TD_ALWAYS_ASSERT(val == 0);

  // 测试带空格的字符串
  result = taosStr2int64("  12345", &val);
  TD_ALWAYS_ASSERT(result == 0);
  TD_ALWAYS_ASSERT(val == 12345);

  result = taosStr2int64("12345  ", &val);
  TD_ALWAYS_ASSERT(result == 0);
  TD_ALWAYS_ASSERT(val == 12345);
}
TEST(osStringTests, osStr2int32) {
  int32_t val;
  int32_t result;

  // 测试空指针输入
  result = taosStr2int32(NULL, &val);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  result = taosStr2int32("123", NULL);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  // 测试无效输入
  result = taosStr2int32("abc", &val);
  ASSERT_NE(result, 0);

  result = taosStr2int32("", &val);
  ASSERT_NE(result, 0);

  // 测试超出范围的值
  char large_num[50];
  snprintf(large_num, sizeof(large_num), "%d", INT_MAX);
  result = taosStr2int32(large_num, &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, INT_MAX);

  snprintf(large_num, sizeof(large_num), "%d", INT_MIN);
  result = taosStr2int32(large_num, &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, INT_MIN);

  // 测试大于 INT32 范围的值
  snprintf(large_num, sizeof(large_num), "%lld", (long long)INT_MAX + 1);
  result = taosStr2int32(large_num, &val);
  ASSERT_EQ(result, TAOS_SYSTEM_ERROR(ERANGE));

  snprintf(large_num, sizeof(large_num), "%lld", (long long)INT_MIN - 1);
  result = taosStr2int32(large_num, &val);
  ASSERT_EQ(result, TAOS_SYSTEM_ERROR(ERANGE));

  result = taosStr2int32("123abc", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);

  result = taosStr2int32("abc123", &val);
  ASSERT_NE(result, 0);

  // 测试有效的整数字符串
  result = taosStr2int32("12345", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);

  result = taosStr2int32("-12345", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, -12345);

  result = taosStr2int32("0", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 0);

  // 测试带空格的字符串
  result = taosStr2int32("  12345", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);

  result = taosStr2int32("12345  ", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);
}

TEST(osStringTests, taosStr2int16) {
  int16_t val;
  int32_t result;

  // 测试空指针输入
  result = taosStr2int16(NULL, &val);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  result = taosStr2int16("123", NULL);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  // 测试无效输入
  result = taosStr2int16("abc", &val);
  ASSERT_NE(result, 0);

  result = taosStr2int16("", &val);
  ASSERT_NE(result, 0);

  // 测试超出范围的值
  char large_num[50];
  snprintf(large_num, sizeof(large_num), "%d", INT16_MAX);
  result = taosStr2int16(large_num, &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, INT16_MAX);

  snprintf(large_num, sizeof(large_num), "%d", INT16_MIN);
  result = taosStr2int16(large_num, &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, INT16_MIN);

  // 测试大于 INT16 范围的值
  snprintf(large_num, sizeof(large_num), "%lld", (long long)INT16_MAX + 1);
  result = taosStr2int16(large_num, &val);
  ASSERT_EQ(result, TAOS_SYSTEM_ERROR(ERANGE));

  snprintf(large_num, sizeof(large_num), "%lld", (long long)INT16_MIN - 1);
  result = taosStr2int16(large_num, &val);
  ASSERT_EQ(result, TAOS_SYSTEM_ERROR(ERANGE));

  result = taosStr2int16("123abc", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);

  result = taosStr2int16("abc123", &val);
  ASSERT_NE(result, 0);
  // 测试有效的整数字符串
  result = taosStr2int16("12345", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);

  result = taosStr2int16("-12345", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, -12345);

  result = taosStr2int16("0", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 0);

  // 测试带空格的字符串
  result = taosStr2int16("  12345", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);

  result = taosStr2int16("12345  ", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);
}

TEST(osStringTests, taosStr2int8) {
  int8_t  val;
  int32_t result;

  // 测试空指针输入
  result = taosStr2int8(NULL, &val);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  result = taosStr2int8("123", NULL);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  // 测试无效输入
  result = taosStr2int8("abc", &val);
  ASSERT_NE(result, 0);

  result = taosStr2int8("", &val);
  ASSERT_NE(result, 0);

  // 测试超出范围的值
  char large_num[50];
  snprintf(large_num, sizeof(large_num), "%d", INT8_MAX);
  result = taosStr2int8(large_num, &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, INT8_MAX);

  snprintf(large_num, sizeof(large_num), "%d", INT8_MIN);
  result = taosStr2int8(large_num, &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, INT8_MIN);

  // 测试大于 INT8 范围的值
  snprintf(large_num, sizeof(large_num), "%lld", (long long)INT8_MAX + 1);
  result = taosStr2int8(large_num, &val);
  ASSERT_EQ(result, TAOS_SYSTEM_ERROR(ERANGE));

  snprintf(large_num, sizeof(large_num), "%lld", (long long)INT8_MIN - 1);
  result = taosStr2int8(large_num, &val);
  ASSERT_EQ(result, TAOS_SYSTEM_ERROR(ERANGE));

  result = taosStr2int8("123abc", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);

  result = taosStr2int8("abc123", &val);
  ASSERT_NE(result, 0);

  // 测试有效的整数字符串
  result = taosStr2int8("123", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);

  result = taosStr2int8("-123", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, -123);

  result = taosStr2int8("0", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 0);

  // 测试带空格的字符串
  result = taosStr2int8("  123", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);

  result = taosStr2int8("123  ", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);
}

TEST(osStringTests, osStr2Uint64) {
  uint64_t val;
  int32_t  result;

  // 测试空指针输入
  result = taosStr2Uint64(NULL, &val);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  result = taosStr2Uint64("123", NULL);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  // 测试无效输入
  result = taosStr2Uint64("abc", &val);
  ASSERT_NE(result, 0);

  result = taosStr2Uint64("", &val);
  ASSERT_NE(result, 0);

  char large_num[50];
  snprintf(large_num, sizeof(large_num), "%llu", ULLONG_MAX);
  result = taosStr2Uint64(large_num, &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, ULLONG_MAX);

  result = taosStr2Uint64("123abc", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);

  result = taosStr2Uint64("abc123", &val);
  ASSERT_NE(result, 0);
  // 测试有效的整数字符串
  result = taosStr2Uint64("12345", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);

  result = taosStr2Uint64("0", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 0);

  // 测试带空格的字符串
  result = taosStr2Uint64("  12345", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);

  result = taosStr2Uint64("12345  ", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);
}

TEST(osStringTests, taosStr2Uint32) {
  uint32_t val;
  int32_t  result;

  // 测试空指针输入
  result = taosStr2Uint32(NULL, &val);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  result = taosStr2Uint32("123", NULL);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  // 测试无效输入
  result = taosStr2Uint32("abc", &val);
  ASSERT_NE(result, 0);

  result = taosStr2Uint32("", &val);
  ASSERT_NE(result, 0);

  // 测试超出范围的值
  char large_num[50];
  snprintf(large_num, sizeof(large_num), "%u", UINT32_MAX);
  result = taosStr2Uint32(large_num, &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, UINT32_MAX);

  // 测试大于 UINT32 范围的值
  snprintf(large_num, sizeof(large_num), "%llu", (unsigned long long)UINT32_MAX + 1);
  result = taosStr2Uint32(large_num, &val);
  ASSERT_EQ(result, TAOS_SYSTEM_ERROR(ERANGE));

  result = taosStr2Uint32("123abc", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);

  result = taosStr2Uint32("abc123", &val);
  ASSERT_NE(result, 0);
  // 测试有效的整数字符串
  result = taosStr2Uint32("12345", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);

  result = taosStr2Uint32("0", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 0);

  // 测试带空格的字符串
  result = taosStr2Uint32("  12345", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);

  result = taosStr2Uint32("12345  ", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);
}

TEST(osStringTests, taosStr2Uint16) {
  uint16_t val;
  int32_t  result;

  // 测试空指针输入
  result = taosStr2Uint16(NULL, &val);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  result = taosStr2Uint16("123", NULL);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  // 测试无效输入
  result = taosStr2Uint16("abc", &val);
  ASSERT_NE(result, 0);

  result = taosStr2Uint16("", &val);
  ASSERT_NE(result, 0);

  // 测试超出范围的值
  char large_num[50];
  snprintf(large_num, sizeof(large_num), "%u", UINT16_MAX);
  result = taosStr2Uint16(large_num, &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, UINT16_MAX);

  // 测试大于 UINT16 范围的值
  snprintf(large_num, sizeof(large_num), "%llu", (unsigned long long)UINT16_MAX + 1);
  result = taosStr2Uint16(large_num, &val);
  ASSERT_EQ(result, TAOS_SYSTEM_ERROR(ERANGE));

  result = taosStr2Uint16("123abc", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);

  result = taosStr2Uint16("abc123", &val);
  ASSERT_NE(result, 0);
  // 测试有效的整数字符串
  result = taosStr2Uint16("12345", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);

  result = taosStr2Uint16("0", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 0);

  // 测试带空格的字符串
  result = taosStr2Uint16("  12345", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);

  result = taosStr2Uint16("12345  ", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 12345);
}

TEST(osStringTests, taosStr2Uint8) {
  uint8_t val;
  int32_t result;

  // 测试空指针输入
  result = taosStr2Uint8(NULL, &val);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  result = taosStr2Uint8("123", NULL);
  ASSERT_EQ(result, TSDB_CODE_INVALID_PARA);

  // 测试无效输入
  result = taosStr2Uint8("abc", &val);
  ASSERT_NE(result, 0);

  result = taosStr2Uint8("", &val);
  ASSERT_NE(result, 0);

  // 测试超出范围的值
  char large_num[50];
  snprintf(large_num, sizeof(large_num), "%u", UINT8_MAX);
  result = taosStr2Uint8(large_num, &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, UINT8_MAX);

  // 测试大于 UINT8 范围的值
  snprintf(large_num, sizeof(large_num), "%llu", (unsigned long long)UINT8_MAX + 1);
  result = taosStr2Uint8(large_num, &val);
  ASSERT_EQ(result, TAOS_SYSTEM_ERROR(ERANGE));

  result = taosStr2Uint8("123abc", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);

  result = taosStr2Uint8("abc123", &val);
  ASSERT_NE(result, 0);
  // 测试有效的整数字符串
  result = taosStr2Uint8("123", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);

  result = taosStr2Uint8("0", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 0);

  // 测试带空格的字符串
  result = taosStr2Uint8("  123", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);

  result = taosStr2Uint8("123  ", &val);
  ASSERT_EQ(result, 0);
  ASSERT_EQ(val, 123);
}

TEST(osStringTests, strint2) {
  const char* ret = tstrdup(NULL);
  EXPECT_EQ(ret, nullptr);

  ret = taosStrndupi(NULL, 0);
  EXPECT_EQ(ret, nullptr);

  char buf[12] = "12345";
  ret = tstrndup(buf, 4);
  EXPECT_NE(ret, nullptr);

  int64_t val = 0;
  int32_t ret32 = taosStr2int64(NULL, &val);
  EXPECT_NE(ret32, 0);
  ret32 = taosStr2int64(buf, NULL);
  EXPECT_NE(ret32, 0);

  TdUcs4  p1, p2;
  int32_t val32 = 0;
  ret32 = taosUcs4Compare(&p1, NULL, val32);
  EXPECT_NE(ret32, 0);
  ret32 = taosUcs4Compare(NULL, &p2, val32);
  EXPECT_NE(ret32, 0);

  void* retptr = taosAcquireConv(NULL, M2C, NULL);
  EXPECT_EQ(retptr, nullptr);

  ret32 = taosUcs4ToMbs(NULL, 0, NULL, NULL);
  EXPECT_EQ(ret32, 0);
  ret32 = taosUcs4ToMbs(NULL, -1, NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosUcs4ToMbs(NULL, 1, NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosUcs4ToMbs(&p1, 1, NULL, NULL);
  EXPECT_NE(ret32, 0);

  bool retb = taosValidateEncodec(NULL);
  EXPECT_FALSE(retb);

  ret32 = taosUcs4len(NULL);
  EXPECT_EQ(ret32, 0);

  unsigned char src[24] = "1234";
  char          dst[24] = {0};
  int32_t       len = 5;
  int32_t       bufSize = 24;
  ret32 = taosHexEncode(NULL, NULL, len, -1);
  EXPECT_NE(ret32, 0);
  ret32 = taosHexEncode(src, NULL, len, -1);
  EXPECT_NE(ret32, 0);
  ret32 = taosHexEncode(src, dst, len, -1);
  EXPECT_NE(ret32, 0);
  ret32 = taosHexEncode(src, dst, len, bufSize);
  EXPECT_EQ(ret32, 0);

  char dst2[24] = {0};
  ret32 = taosHexDecode(NULL, NULL, 0);
  EXPECT_NE(ret32, 0);
  ret32 = taosHexDecode(NULL, dst2, 0);
  EXPECT_NE(ret32, 0);
  ret32 = taosHexDecode(dst, NULL, 0);
  EXPECT_NE(ret32, 0);
  ret32 = taosHexDecode(dst, dst2, 24);
  EXPECT_EQ(ret32, 0);
  EXPECT_STREQ((char*)src, dst2);
}

TEST(osStringTests, wchartest) {
  char    src[24] = "1234";
  TdWchar dst[24] = {0};

  int32_t ret32 = taosWcharsWidth(NULL, 0);
  EXPECT_LT(ret32, 0);
  ret32 = taosWcharsWidth(dst, 0);
  EXPECT_LT(ret32, 0);

  ret32 = taosMbToWchar(NULL, NULL, 0);
  EXPECT_LT(ret32, 0);
  ret32 = taosMbToWchar(dst, NULL, 0);
  EXPECT_LT(ret32, 0);
  ret32 = taosMbToWchar(dst, src, 0);
  EXPECT_LT(ret32, 0);
  ret32 = taosMbToWchar(dst, src, 4);
  EXPECT_GT(ret32, 0);
  ret32 = taosWcharsWidth(dst, ret32);
  EXPECT_GT(ret32, 0);

  ret32 = taosMbsToWchars(NULL, NULL, 0);
  EXPECT_LT(ret32, 0);
  ret32 = taosMbsToWchars(dst, NULL, 0);
  EXPECT_LT(ret32, 0);
  ret32 = taosMbsToWchars(dst, src, 0);
  EXPECT_LT(ret32, 0);
  ret32 = taosMbsToWchars(dst, src, 4);
  EXPECT_GT(ret32, 0);
  ret32 = taosWcharsWidth(dst, ret32);
  EXPECT_GT(ret32, 0);

  ret32 = taosWcharsWidth(NULL, dst[0]);
  EXPECT_NE(ret32, 0);
  ret32 = taosWcharToMb(src, dst[0]);
  EXPECT_NE(ret32, 0);
}

TEST(osStringTests, strtransform) {
  char src[12] = "12";

  void* retptr = taosStrCaseStr(NULL, NULL);
  EXPECT_EQ(retptr, nullptr);
  retptr = taosStrCaseStr(src, NULL);
  EXPECT_NE(retptr, nullptr);

  int64_t ret64 = taosStr2Int64(NULL, NULL, 0);
  EXPECT_EQ(ret64, 0);
  uint64_t retu64 = taosStr2UInt64(NULL, NULL, 0);
  EXPECT_EQ(retu64, 0);
  int32_t ret32 = taosStr2Int32(NULL, NULL, 0);
  EXPECT_EQ(ret32, 0);
  uint32_t retu32 = taosStr2UInt32(NULL, NULL, 0);
  EXPECT_EQ(retu32, 0);
  ret32 = taosStr2Int16(NULL, NULL, 0);
  EXPECT_EQ(ret32, 0);
  ret32 = taosStr2UInt16(NULL, NULL, 0);
  EXPECT_EQ(ret32, 0);
  ret32 = taosStr2Int8(NULL, NULL, 0);
  EXPECT_EQ(ret32, 0);
  ret32 = taosStr2UInt8(NULL, NULL, 0);
  EXPECT_EQ(ret32, 0);

  double retd = taosStr2Double(NULL, NULL);
  EXPECT_EQ((int32_t)retd, 0);
  float retf = taosStr2Float(NULL, NULL);
  EXPECT_EQ((int32_t)retf, 0);

  bool retb = isValidateHex(NULL, 0);
  EXPECT_FALSE(retb);

  char z[12] = {0};
  ret32 = taosHex2Ascii(NULL, 0, NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosHex2Ascii(z, 0, NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosHex2Ascii(z, 0, (void**)&z, NULL);
  EXPECT_NE(ret32, 0);

  ret64 = tsnprintf(NULL, 0, NULL);
  EXPECT_EQ(ret64, 0);
  ret64 = tsnprintf(z, 4, NULL);
  EXPECT_EQ(ret64, 0);
  ret64 = tsnprintf(z, 0, "ab");
  EXPECT_EQ(ret64, 0);
  ret64 = tsnprintf(z, 1, "ab");
  EXPECT_EQ(ret64, 0);
}
