#include <gtest/gtest.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "taos.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tmisce.h"
#include "ttime.h"
#include "ttokendef.h"
#include "tvariant.h"

namespace {
//
}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, toUIntegerEx_test) {
  uint64_t val = 0;

  char*   s = "123";
  int32_t ret = toUIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 123);

  s = "1000u";
  ret = toUIntegerEx(s, strlen(s), 0, &val);
  ASSERT_EQ(ret, -1);

  s = "0x1f";
  ret = toUIntegerEx(s, strlen(s), TK_NK_HEX, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 31);

  s = "0b110";
  ret = toUIntegerEx(s, strlen(s), 0, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 6);

  s = "2567.4787";
  ret = toUIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 2567);

  s = "1.869895343e4";
  ret = toUIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 18699);

  s = "-1";
  ret = toUIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, -1);

  s = "-0b10010";
  ret = toUIntegerEx(s, strlen(s), 0, &val);
  ASSERT_EQ(ret, -1);

  s = "-0x40";
  ret = toUIntegerEx(s, strlen(s), TK_NK_HEX, &val);
  ASSERT_EQ(ret, -1);

  s = "-80.9999";
  ret = toUIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, -1);

  s = "-5.2343544534e10";
  ret = toUIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, -1);

  // INT64_MAX
  s = "9223372036854775807";
  ret = toUIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 9223372036854775807);

  // UINT64_MAX
  s = "18446744073709551615";
  ret = toUIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 18446744073709551615u);

  // out of range
  s = "18446744073709551616";
  ret = toUIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, -1);

  s = "5.23e25";
  ret = toUIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, -1);
}

TEST(testCase, toIntegerEx_test) {
  int64_t val = 0;

  char*   s = "123";
  int32_t ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 123);

  s = "-1";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -1);

  s = "1000u";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, -1);

  s = "0x1f";
  ret = toIntegerEx(s, strlen(s), TK_NK_HEX, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 31);

  s = "-0x40";
  ret = toIntegerEx(s, strlen(s), TK_NK_HEX, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -64);

  s = "0b110";
  ret = toIntegerEx(s, strlen(s), TK_NK_BIN, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 6);

  s = "-0b10010";
  ret = toIntegerEx(s, strlen(s), 0, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -18);

  s = "-80.9999";
  ret = toIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -81);

  s = "2567.8787";
  ret = toIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 2568);

  s = "-5.2343544534e10";
  ret = toIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -52343544534);

  s = "1.869895343e4";
  ret = toIntegerEx(s, strlen(s), TK_NK_FLOAT, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 18699);

  // INT64_MAX
  s = "9223372036854775807";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 9223372036854775807LL);

  s = "-9223372036854775808";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, 0);
  // ASSERT_EQ(val, -9223372036854775808);

  // out of range
  s = "9323372036854775807";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, -1);

  s = "-9323372036854775807";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, -1);

  // UINT64_MAX
  s = "18446744073709551615";
  ret = toIntegerEx(s, strlen(s), TK_NK_INTEGER, &val);
  ASSERT_EQ(ret, -1);
}

TEST(testCase, toInteger_test) {
  int64_t val = 0;

  char*   s = "123";
  int32_t ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 123);

  s = "-1";
  ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -1);

  s = "1000u";
  ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, -1);

  s = "0x10";
  ret = toInteger(s, strlen(s), 16, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 16);

  s = "110";
  ret = toInteger(s, strlen(s), 2, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 6);

  s = "110";
  ret = toInteger(s, strlen(s), 8, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 72);

  s = "9223372036854775807";
  ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 9223372036854775807);

  s = "-9223372036854775808";
  ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, 0);
  // ASSERT_EQ(val, -9223372036854775808);

  // out of range
  s = "9323372036854775807";
  ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, -1);

  s = "-9323372036854775807";
  ret = toInteger(s, strlen(s), 10, &val);
  ASSERT_EQ(ret, -1);
}

TEST(testCase, Datablock_test) {
  SSDataBlock* b = createDataBlock();

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 1);
  taosArrayPush(b->pDataBlock, &infoData);
  blockDataAppendColInfo(b, &infoData);

  SColumnInfoData infoData1 = createColumnInfoData(TSDB_DATA_TYPE_BINARY, 40, 2);
  blockDataAppendColInfo(b, &infoData1);
  blockDataEnsureCapacity(b, 40);

  char* str = "the value of: %d";
  char  buf[128] = {0};
  char  varbuf[128] = {0};

  for (int32_t i = 0; i < 40; ++i) {
    SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
    SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);

    if (i & 0x01) {
      int32_t len = sprintf(buf, str, i);
      STR_TO_VARSTR(varbuf, buf)
      colDataSetVal(p0, i, (const char*)&i, false);
      colDataSetVal(p1, i, (const char*)varbuf, false);

      memset(varbuf, 0, sizeof(varbuf));
      memset(buf, 0, sizeof(buf));
    } else {
      colDataSetVal(p0, i, (const char*)&i, true);
      colDataSetVal(p1, i, (const char*)varbuf, true);
    }

    b->info.rows++;
  }

  SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
  SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);
  for (int32_t i = 0; i < 40; ++i) {
    if (i & 0x01) {
      ASSERT_EQ(colDataIsNull_f(p0->nullbitmap, i), false);
      ASSERT_EQ(colDataIsNull(p1, b->info.rows, i, nullptr), false);
    } else {
      ASSERT_EQ(colDataIsNull_f(p0->nullbitmap, i), true);

      ASSERT_EQ(colDataIsNull(p0, b->info.rows, i, nullptr), true);
      ASSERT_EQ(colDataIsNull(p1, b->info.rows, i, nullptr), true);
    }
  }

  printf("binary column length:%d\n", *(int32_t*)p1->pData);

  ASSERT_EQ(blockDataGetNumOfCols(b), 2);
  ASSERT_EQ(blockDataGetNumOfRows(b), 40);

  char* pData = colDataGetData(p1, 3);
  printf("the second row of binary:%s, length:%d\n", (char*)varDataVal(pData), varDataLen(pData));

  SArray*         pOrderInfo = taosArrayInit(3, sizeof(SBlockOrderInfo));
  SBlockOrderInfo order = {true, TSDB_ORDER_ASC, 0, NULL};
  taosArrayPush(pOrderInfo, &order);

  blockDataSort(b, pOrderInfo);
  blockDataDestroy(b);

  taosArrayDestroy(pOrderInfo);
}

#if 0
TEST(testCase, non_var_dataBlock_split_test) {
  SSDataBlock* b = static_cast<SSDataBlock*>(taosMemoryCalloc(1, sizeof(SSDataBlock)));
  b->info.numOfCols = 2;
  b->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {0};
  infoData.info.bytes = 4;
  infoData.info.type = TSDB_DATA_TYPE_INT;
  infoData.info.colId = 1;

  int32_t numOfRows = 1000000;

  infoData.pData = (char*) taosMemoryCalloc(numOfRows, infoData.info.bytes);
  infoData.nullbitmap = (char*) taosMemoryCalloc(1, sizeof(char) * (numOfRows/8));
  taosArrayPush(b->pDataBlock, &infoData);

  SColumnInfoData infoData1 = {0};
  infoData1.info.bytes = 1;
  infoData1.info.type = TSDB_DATA_TYPE_TINYINT;
  infoData1.info.colId = 2;

  infoData1.pData = (char*) taosMemoryCalloc(numOfRows, infoData.info.bytes);
  infoData1.nullbitmap = (char*) taosMemoryCalloc(1, sizeof(char) * (numOfRows/8));
  taosArrayPush(b->pDataBlock, &infoData1);

  for(int32_t i = 0; i < numOfRows; ++i) {
    SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
    SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);

    int8_t v = i;
    colDataSetVal(p0, i, (const char*)&i, false);
    colDataSetVal(p1, i, (const char*)&v, false);
    b->info.rows++;
  }

  int32_t pageSize = 64 * 1024;

  int32_t startIndex= 0;
  int32_t stopIndex = 0;
  int32_t count = 1;
  while(1) {
    blockDataSplitRows(b, false, startIndex, &stopIndex, pageSize);
    printf("the %d split, from: %d to %d\n", count++, startIndex, stopIndex);

    if (stopIndex == numOfRows - 1) {
      break;
    }

    startIndex = stopIndex + 1;
  }

}

#endif

TEST(testCase, var_dataBlock_split_test) {
  int32_t numOfRows = 1000000;

  SSDataBlock* b = createDataBlock();

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 1);
  blockDataAppendColInfo(b, &infoData);

  SColumnInfoData infoData1 = createColumnInfoData(TSDB_DATA_TYPE_BINARY, 40, 2);
  blockDataAppendColInfo(b, &infoData1);

  blockDataEnsureCapacity(b, numOfRows);

  char buf[41] = {0};
  char buf1[100] = {0};

  for (int32_t i = 0; i < numOfRows; ++i) {
    SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
    SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);

    int8_t v = i;
    colDataSetVal(p0, i, (const char*)&i, false);

    sprintf(buf, "the number of row:%d", i);
    int32_t len = sprintf(buf1, buf, i);
    STR_TO_VARSTR(buf1, buf)
    colDataSetVal(p1, i, buf1, false);
    b->info.rows++;

    memset(buf, 0, sizeof(buf));
    memset(buf1, 0, sizeof(buf1));
  }

  int32_t pageSize = 64 * 1024;

  int32_t startIndex = 0;
  int32_t stopIndex = 0;
  int32_t count = 1;
  while (1) {
    blockDataSplitRows(b, true, startIndex, &stopIndex, pageSize);
    printf("the %d split, from: %d to %d\n", count++, startIndex, stopIndex);

    if (stopIndex == numOfRows - 1) {
      break;
    }

    startIndex = stopIndex + 1;
  }
}

void check_tm(const STm* tm, int32_t y, int32_t mon, int32_t d, int32_t h, int32_t m, int32_t s, int64_t fsec) {
  ASSERT_EQ(tm->tm.tm_year, y);
  ASSERT_EQ(tm->tm.tm_mon, mon);
  ASSERT_EQ(tm->tm.tm_mday, d);
  ASSERT_EQ(tm->tm.tm_hour, h);
  ASSERT_EQ(tm->tm.tm_min, m);
  ASSERT_EQ(tm->tm.tm_sec, s);
  ASSERT_EQ(tm->fsec, fsec);
}

void test_timestamp_tm_conversion(int64_t ts, int32_t precision, int32_t y, int32_t mon, int32_t d, int32_t h,
                                  int32_t m, int32_t s, int64_t fsec) {
  int64_t    ts_tmp;
  char       buf[128] = {0};
  struct STm tm;
  taosFormatUtcTime(buf, 128, ts, precision);
  printf("formated ts of %ld, precision: %d is: %s\n", ts, precision, buf);
  taosTs2Tm(ts, precision, &tm);
  check_tm(&tm, y, mon, d, h, m, s, fsec);
  taosTm2Ts(&tm, &ts_tmp, precision);
  ASSERT_EQ(ts, ts_tmp);
}

TEST(timeTest, timestamp2tm) {
  const char* ts_str_ns = "2023-10-12T11:29:00.775726171+0800";
  const char* ts_str_us = "2023-10-12T11:29:00.775726+0800";
  const char* ts_str_ms = "2023-10-12T11:29:00.775+0800";
  int64_t     ts, tmp_ts = 0;
  struct STm  tm;

  ASSERT_EQ(TSDB_CODE_SUCCESS, taosParseTime(ts_str_ns, &ts, strlen(ts_str_ns), TSDB_TIME_PRECISION_NANO, 0));
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_NANO, 2023 - 1900, 9 /* mon start from 0*/, 12, 11, 29, 0,
                               775726171L);

  ASSERT_EQ(TSDB_CODE_SUCCESS, taosParseTime(ts_str_us, &ts, strlen(ts_str_us), TSDB_TIME_PRECISION_MICRO, 0));
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_MICRO, 2023 - 1900, 9 /* mon start from 0*/, 12, 11, 29, 0,
                               775726000L);

  ASSERT_EQ(TSDB_CODE_SUCCESS, taosParseTime(ts_str_ms, &ts, strlen(ts_str_ms), TSDB_TIME_PRECISION_MILLI, 0));
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_MILLI, 2023 - 1900, 9 /* mon start from 0*/, 12, 11, 29, 0,
                               775000000L);

  ts = -5364687943000;  // milliseconds since epoch, Wednesday, January 1, 1800 1:00:00 AM GMT+08:06
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_MILLI, 1800 - 1900, 0 /* mon start from 0*/, 1, 1, 0, 0,
                               000000000L);

  ts = 0;
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_MILLI, 1970 - 1900, 0 /* mon start from 0*/, 1, 8, 0, 0,
                               000000000L);

  ts = -62198784343000;  // milliseconds before epoch, Friday, January 1, -0001 12:00:00 AM GMT+08:06
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_MILLI, -1 - 1900, 0 /* mon start from 0*/, 1,
                               0 /* hour start from 0*/, 0, 0, 000000000L);
}

void test_ts2char(int64_t ts, const char* format, int32_t precison, const char* expected) {
  char buf[256] = {0};
  TEST_ts2char(format, ts, precison, buf, 256);
  printf("ts: %ld format: %s res: [%s], expected: [%s]\n", ts, format, buf, expected);
  ASSERT_STREQ(expected, buf);
}

TEST(timeTest, ts2char) {
  osDefaultInit();
  if (tsTimezone != TdEastZone8) GTEST_SKIP();
  int64_t     ts;
  const char* format = "YYYY-MM-DD";
  ts = 0;
  test_ts2char(ts, format, TSDB_TIME_PRECISION_MILLI, "1970-01-01");
  test_ts2char(ts, format, TSDB_TIME_PRECISION_MICRO, "1970-01-01");
  test_ts2char(ts, format, TSDB_TIME_PRECISION_NANO, "1970-01-01");
  test_ts2char(ts, format, TSDB_TIME_PRECISION_SECONDS, "1970-01-01");

  ts = 1697163517;
  test_ts2char(ts, "YYYY-MM-DD", TSDB_TIME_PRECISION_SECONDS, "2023-10-13");
  ts = 1697163517000;
  test_ts2char(ts, "YYYY-MM-DD-Day-DAY", TSDB_TIME_PRECISION_MILLI, "2023-10-13-Friday   -FRIDAY   ");
#ifndef WINDOWS
  // double quoted: year, month, day are not parsed
  test_ts2char(ts,
               "YYYY-YYY-YY-Y-yyyy-yyy-yy-y-\"年\"-MONTH-MON-Month-Mon-month-mon-\"月\"-DDD-DD-D-ddd-dd-d-DAY-Day-"
               "day-\"日\"",
               TSDB_TIME_PRECISION_MILLI,
               "2023-023-23-3-2023-023-23-3-年-OCTOBER  -OCT-October  -Oct-october  "
               "-oct-月-286-13-6-286-13-6-FRIDAY   -Friday   -friday   -日");
#endif
  ts = 1697182085123L;  // Friday, October 13, 2023 3:28:05.123 PM GMT+08:00
  test_ts2char(ts, "HH24:hh24:HH12:hh12:HH:hh:MI:mi:SS:ss:MS:ms:US:us:NS:ns:PM:AM:pm:am", TSDB_TIME_PRECISION_MILLI,
               "15:15:03:03:03:03:28:28:05:05:123:123:123000:123000:123000000:123000000:PM:PM:pm:pm");

  // double quotes normal output
  test_ts2char(ts, "\\\"HH24:hh24:HH12:hh12:HH:hh:MI:mi:SS:ss:MS:ms:US:us:NS:ns:PM:AM:pm:am\\\"",
               TSDB_TIME_PRECISION_MILLI,
               "\"15:15:03:03:03:03:28:28:05:05:123:123:123000:123000:123000000:123000000:PM:PM:pm:pm\"");
  test_ts2char(ts, "\\\"HH24:hh24:HH12:hh12:HH:hh:MI:mi:SS:ss:MS:ms:US:us:NS:ns:PM:AM:pm:am", TSDB_TIME_PRECISION_MILLI,
               "\"15:15:03:03:03:03:28:28:05:05:123:123:123000:123000:123000000:123000000:PM:PM:pm:pm");
  // double quoted strings recognized as literal string, parsing skipped
  test_ts2char(ts, "\"HH24:hh24:HH12:hh12:HH:hh:MI:mi:SS:ss:MS:ms:US:us:NS:ns:PM:AM:pm:am", TSDB_TIME_PRECISION_MILLI,
               "HH24:hh24:HH12:hh12:HH:hh:MI:mi:SS:ss:MS:ms:US:us:NS:ns:PM:AM:pm:am");
  test_ts2char(ts, "yyyy-mm-dd hh24:mi:ss.nsamaaa", TSDB_TIME_PRECISION_MILLI, "2023-10-13 15:28:05.123000000pmaaa");
  test_ts2char(ts, "aaa--yyyy-mm-dd hh24:mi:ss.nsamaaa", TSDB_TIME_PRECISION_MILLI,
               "aaa--2023-10-13 15:28:05.123000000pmaaa");
  test_ts2char(ts, "add--yyyy-mm-dd hh24:mi:ss.nsamaaa", TSDB_TIME_PRECISION_MILLI,
               "a13--2023-10-13 15:28:05.123000000pmaaa");

  ts = 1693946405000;
  test_ts2char(ts, "Day, Month dd, YYYY hh24:mi:ss AM TZH:tzh", TSDB_TIME_PRECISION_MILLI,
               "Wednesday, September 06, 2023 04:40:05 AM +08:+08");

  ts = -62198784343000;  // milliseconds before epoch, Friday, January 1, -0001 12:00:00 AM GMT+08:06
  test_ts2char(ts, "Day, Month dd, YYYY hh12:mi:ss AM", TSDB_TIME_PRECISION_MILLI,
               "Friday   , January   01, -001 12:00:00 AM");
}

TEST(timeTest, char2ts) {
  osDefaultInit();
  if (tsTimezone != TdEastZone8) GTEST_SKIP();
  int64_t ts;
  int32_t code =
      TEST_char2ts("YYYY-DD-MM HH12:MI:SS:MSPM", &ts, TSDB_TIME_PRECISION_MILLI, "2023-10-10 12:00:00.000AM");
  ASSERT_EQ(code, 0);
  ASSERT_EQ(ts, 1696867200000LL);

  // 2009-1-1 00:00:00
  ASSERT_EQ(0, TEST_char2ts("YYYY-YYY-YY-Y", &ts, TSDB_TIME_PRECISION_MILLI, "2023-123-23-9"));
  ASSERT_EQ(1230739200000LL, ts);
  // 2023-1-1
  ASSERT_EQ(0, TEST_char2ts("YYYY-YYY-YY", &ts, TSDB_TIME_PRECISION_MILLI, "2023-123-23-9"));
  ASSERT_EQ(ts, 1672502400000LL);

  // 2123-1-1, the second year(123) is used, which converted to 2123
  ASSERT_EQ(0, TEST_char2ts("YYYY-YYY", &ts, TSDB_TIME_PRECISION_MILLI, "2023-123-23-9"));
  ASSERT_EQ(ts, 4828176000000LL);
  // 2023-1-1 12:10:10am
  ASSERT_EQ(0, TEST_char2ts("yyyy-mm-dd HH12:MI:SSAM", &ts, TSDB_TIME_PRECISION_MILLI, "2023-1-1 12:10:10am"));
  ASSERT_EQ(ts, 1672503010000LL);

  // 2023-1-1 21:10:10.123
  ASSERT_EQ(0, TEST_char2ts("yy-MM-dd HH12:MI:ss.msa.m.", &ts, TSDB_TIME_PRECISION_MILLI, "23-1-01 9:10:10.123p.m."));
  ASSERT_EQ(ts, 1672578610123LL);

  // 2023-1-1 21:10:10.123456789
  ASSERT_EQ(0, TEST_char2ts("yy-MM-dd HH:MI:ss.ms.us.nsa.m.", &ts, TSDB_TIME_PRECISION_NANO,
                            "23-1-01 9:10:10.123.000456.000000789p.m."));
  ASSERT_EQ(ts, 1672578610123456789LL);

  // 2023-1-1 21:10:10.120450780
  ASSERT_EQ(0, TEST_char2ts("yy-MM-dd HH24:MI:SS.ms.us.ns", &ts, TSDB_TIME_PRECISION_NANO,
                            "   23   - 1 - 01   \t  21:10:10 .  12 .  \t 00045 . 00000078  \t"));
  ASSERT_EQ(ts, 1672578610120450780LL);

#ifndef WINDOWS
  // 2023-1-1 21:10:10.120450780
  ASSERT_EQ(0, TEST_char2ts("yy \"年\"-MM 月-dd  \"日 子\" HH24:MI:ss.ms.us.ns TZH", &ts, TSDB_TIME_PRECISION_NANO,
                            "   23  年 - 1 月 - 01 日 子  \t  21:10:10 .  12 .  \t 00045 . 00000078  \t+08"));
  ASSERT_EQ(ts, 1672578610120450780LL);
#endif

  // 2023-1-1 19:10:10.123456789+06 -> 2023-1-1 21:10:10.123456789+08
  ASSERT_EQ(0, TEST_char2ts("yy-MM-dd HH:MI:ss.ms.us.nsa.m.TZH", &ts, TSDB_TIME_PRECISION_NANO,
                            "23-1-01 7:10:10.123.000456.000000789p.m.6"));
  ASSERT_EQ(ts, 1672578610123456789LL);

  // 2023-1-1 12:10:10.123456789-01 -> 2023-1-1 21:10:10.123456789+08
  ASSERT_EQ(0, TEST_char2ts("yy-MM-dd HH24:MI:ss.ms.us.nsTZH", &ts, TSDB_TIME_PRECISION_NANO,
                            "23-1-01 12:10:10.123.000456.000000789-1"));
  ASSERT_EQ(ts, 1672578610123456789LL);

  // 2100-01-01 11:10:10.124456+08
  ASSERT_EQ(
      0, TEST_char2ts("yyyy-MM-dd HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO, "2100-01-01 11:10:10.124456+08"));
  ASSERT_EQ(ts, 4102456210124456LL);

  // 2100-01-01 11:10:10.124456+08 Firday
  ASSERT_EQ(0, TEST_char2ts("yyyy/MONTH/dd DAY HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "2100/january/01 friday 11:10:10.124456+08"));
  ASSERT_EQ(ts, 4102456210124456LL);

  ASSERT_EQ(0, TEST_char2ts("yyyy/Month/dd Day HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "2100/january/01 FRIDAY 11:10:10.124456+08"));
  ASSERT_EQ(ts, 4102456210124456LL);
  ASSERT_EQ(0, TEST_char2ts("yyyy/Month/dd Dy HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "2100/january/01 Fri 11:10:10.124456+08:00"));
  ASSERT_EQ(ts, 4102456210124456LL);

  ASSERT_EQ(0, TEST_char2ts("yyyy/month/dd day HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "2100/january/01 Friday 11:10:10.124456+08"));
  ASSERT_EQ(ts, 4102456210124456LL);

  // 2100-02-01 11:10:10.124456+08 Firday
  ASSERT_EQ(0, TEST_char2ts("yyyy/mon/dd DY HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "2100/Feb/01 Mon 11:10:10.124456+08"));
  ASSERT_EQ(ts, 4105134610124456LL);

  // 2100-02-01 11:10:10.124456+08 Firday
  ASSERT_EQ(0, TEST_char2ts("yyyy/mon/dd DY DDD-DD-D HH24:MI:ss.usTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "2100/Feb/01 Mon 100-1-01 11:10:10.124456+08"));
  ASSERT_EQ(ts, 4105134610124456LL);

  ASSERT_EQ(0, TEST_char2ts("yyyyMMdd ", &ts, TSDB_TIME_PRECISION_MICRO, "21000101"));

  // What is Fe?
  ASSERT_EQ(-1, TEST_char2ts("yyyy/mon/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "2100/Fe/01"));
  // '/' cannot convert to MM
  ASSERT_EQ(-1, TEST_char2ts("yyyyMMdd ", &ts, TSDB_TIME_PRECISION_MICRO, "2100/2/1"));
  // nothing to be converted to dd
  ASSERT_EQ(0, TEST_char2ts("yyyyMMdd ", &ts, TSDB_TIME_PRECISION_MICRO, "210012"));
  ASSERT_EQ(ts, 4131273600000000LL);  // 2100-12-1
  ASSERT_EQ(-1, TEST_char2ts("yyyyMMdd ", &ts, TSDB_TIME_PRECISION_MICRO, "21001"));
  ASSERT_EQ(-1, TEST_char2ts("yyyyMM-dd ", &ts, TSDB_TIME_PRECISION_MICRO, "23a1-1"));

  // 2100-1-2
  ASSERT_EQ(0, TEST_char2ts("yyyyMM/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "21001/2"));
  ASSERT_EQ(ts, 4102502400000000LL);

  // default to 1970-1-1 00:00:00+08 -> 1969-12-31 16:00:00+00
  ASSERT_EQ(0, TEST_char2ts("YYYY", &ts, TSDB_TIME_PRECISION_SECONDS, "1970"));
  ASSERT_EQ(ts, -1 * tsTimezone * 60 * 60);

  ASSERT_EQ(0, TEST_char2ts("yyyyMM1/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "210001/2"));
  ASSERT_EQ(ts, 4102502400000000LL);

  ASSERT_EQ(-2, TEST_char2ts("yyyyMM/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "210013/2"));
  ASSERT_EQ(-2, TEST_char2ts("yyyyMM/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "210011/32"));
  ASSERT_EQ(-1, TEST_char2ts("HH12:MI:SS", &ts, TSDB_TIME_PRECISION_MICRO, "21:12:12"));
  ASSERT_EQ(-1, TEST_char2ts("yyyy/MM1/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "2100111111111/11/2"));
  ASSERT_EQ(-2, TEST_char2ts("yyyy/MM1/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "23/11/2-13"));
  ASSERT_EQ(0, TEST_char2ts("yyyy年 MM/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "1970年1/1+0"));
  ASSERT_EQ(ts, 0);
  ASSERT_EQ(-1, TEST_char2ts("yyyy年a MM/dd", &ts, TSDB_TIME_PRECISION_MICRO, "2023年1/2"));
  ASSERT_EQ(0, TEST_char2ts("yyyy年 MM/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "1970年   1/1+0"));
  ASSERT_EQ(ts, 0);
  ASSERT_EQ(0, TEST_char2ts("yyyy年 a a a MM/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "1970年 a a a 1/1+0"));
  ASSERT_EQ(0, TEST_char2ts("yyyy年 a a a a a a a a a a a a a a a MM/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO,
                            "1970年 a     "));
  ASSERT_EQ(-3, TEST_char2ts("yyyy-mm-DDD", &ts, TSDB_TIME_PRECISION_MILLI, "1970-01-001"));
}

TEST(timeTest, epSet) {
  {
    SEpSet ep = {0};
    addEpIntoEpSet(&ep, "local", 14);
    addEpIntoEpSet(&ep, "aocal", 13);
    addEpIntoEpSet(&ep, "abcal", 12);
    addEpIntoEpSet(&ep, "abcaleb", 11);
    epsetSort(&ep);
    ASSERT_EQ(strcmp(ep.eps[0].fqdn, "abcal"), 0);
    ASSERT_EQ(ep.eps[0].port, 12);

    ASSERT_EQ(strcmp(ep.eps[1].fqdn, "abcaleb"), 0);
    ASSERT_EQ(ep.eps[1].port, 11);

    ASSERT_EQ(strcmp(ep.eps[2].fqdn, "aocal"), 0);
    ASSERT_EQ(ep.eps[2].port, 13);

    ASSERT_EQ(strcmp(ep.eps[3].fqdn, "local"), 0);
    ASSERT_EQ(ep.eps[3].port, 14);
  }
  {
    SEpSet ep = {0};
    addEpIntoEpSet(&ep, "local", 14);
    addEpIntoEpSet(&ep, "local", 13);
    addEpIntoEpSet(&ep, "local", 12);
    addEpIntoEpSet(&ep, "local", 11);
    epsetSort(&ep);
    ASSERT_EQ(strcmp(ep.eps[0].fqdn, "local"), 0);
    ASSERT_EQ(ep.eps[0].port, 11);

    ASSERT_EQ(strcmp(ep.eps[0].fqdn, "local"), 0);
    ASSERT_EQ(ep.eps[1].port, 12);

    ASSERT_EQ(strcmp(ep.eps[0].fqdn, "local"), 0);
    ASSERT_EQ(ep.eps[2].port, 13);

    ASSERT_EQ(strcmp(ep.eps[0].fqdn, "local"), 0);
    ASSERT_EQ(ep.eps[3].port, 14);
  }
  {
    SEpSet ep = {0};
    addEpIntoEpSet(&ep, "local", 14);
    epsetSort(&ep);
    ASSERT_EQ(ep.numOfEps, 1);
  }
}
#pragma GCC diagnostic pop
