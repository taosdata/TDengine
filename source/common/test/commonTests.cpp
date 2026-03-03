#include <gtest/gtest.h>
#include <iostream>
#include <string>

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
#include "tanalytics.h"
#include "tglobal.h"
#include "tjson.h"
#include "trepair.h"

namespace {
std::string buildRepairTempPath(const char *tag) {
  static int32_t seq = 0;
  return std::string("/tmp/td-repair-") + std::to_string((long long)taosGetTimestampUs()) + "-" +
         std::to_string(seq++) + "-" + tag;
}

class RepairTempDirGuard {
 public:
  explicit RepairTempDirGuard(const std::string &path) : path_(path) {}

  ~RepairTempDirGuard() {
    if (!path_.empty() && taosDirExist(path_.c_str())) {
      taosRemoveDir(path_.c_str());
    }
  }

  const std::string &path() const { return path_; }

 private:
  std::string path_;
};

std::string readRepairFileContent(const char *path) {
  if (path == nullptr || path[0] == '\0') {
    return "";
  }

  int64_t fileSize = 0;
  if (taosStatFile(path, &fileSize, nullptr, nullptr) != 0 || fileSize < 0) {
    return "";
  }

  TdFilePtr pFile = taosOpenFile(path, TD_FILE_READ);
  if (pFile == nullptr) {
    return "";
  }

  std::string content((size_t)fileSize, '\0');
  int64_t     nread = taosReadFile(pFile, &content[0], fileSize);
  (void)taosCloseFile(&pFile);
  if (nread < 0) {
    return "";
  }

  content.resize((size_t)nread);
  return content;
}
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

TEST(testCase, Datablock_test_inc) {
  {
    SColumnInfoData cinfo = {0};
    uint32_t        row = 0;

    bool ret = colDataIsNull_s(&cinfo, row);
    EXPECT_EQ(ret, false);

    cinfo.hasNull = 1;
    cinfo.info.type = TSDB_DATA_TYPE_INT;
    ret = colDataIsNull_s(&cinfo, row);
    EXPECT_EQ(ret, false);
  }

  {
    SColumnInfoData cinfo = {0};
    uint32_t        row = 0;
    bool            isVarType = false;

    bool ret = colDataIsNull_t(&cinfo, row, isVarType);
    EXPECT_EQ(ret, false);

    cinfo.hasNull = 1;
    ret = colDataIsNull_t(&cinfo, row, isVarType);
    EXPECT_EQ(ret, false);
  }

  {
    SColumnInfoData cinfo = {0};
    uint32_t        totalRows = 0;
    uint32_t        row = 0;
    SColumnDataAgg  colAgg = {0};

    bool ret = colDataIsNull(&cinfo, totalRows, row, &colAgg);
    EXPECT_EQ(ret, false);

    cinfo.hasNull = 1;
    ret = colDataIsNull(&cinfo, totalRows, row, &colAgg);
    EXPECT_EQ(ret, true);

    totalRows = 1;
    ret = colDataIsNull(&cinfo, totalRows, row, &colAgg);
    EXPECT_EQ(ret, false);

    colAgg.colId = -1;
    cinfo.info.type = TSDB_DATA_TYPE_INT;
    ret = colDataIsNull(&cinfo, totalRows, row, &colAgg);
    EXPECT_EQ(ret, false);
  }
}

TEST(testCase, Datablock_test) {
  SSDataBlock* b = NULL;
  int32_t      code = createDataBlock(&b);
  ASSERT(code == 0);

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
      ASSERT_EQ(colDataIsNull_f(p0, i), false);
      ASSERT_EQ(colDataIsNull(p1, b->info.rows, i, nullptr), false);
    } else {
      ASSERT_EQ(colDataIsNull_f(p0, i), true);

      ASSERT_EQ(colDataIsNull(p0, b->info.rows, i, nullptr), true);
      ASSERT_EQ(colDataIsNull(p1, b->info.rows, i, nullptr), true);
    }
  }

  printf("binary column length:%d\n", *(int32_t*)p1->pData);

  ASSERT_EQ(blockDataGetNumOfCols(b), 3);
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

  SSDataBlock* b = NULL;
  int32_t      code = createDataBlock(&b);
  ASSERT(code == 0);

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

  blockDataDestroy(b);
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
  taosTs2Tm(ts, precision, &tm, NULL);
  check_tm(&tm, y, mon, d, h, m, s, fsec);
  taosTm2Ts(&tm, &ts_tmp, precision, NULL);
  ASSERT_EQ(ts, ts_tmp);
}

TEST(timeTest, timestamp2tm) {
  const char* ts_str_ns = "2023-10-12T11:29:00.775726171+0800";
  const char* ts_str_us = "2023-10-12T11:29:00.775726+0800";
  const char* ts_str_ms = "2023-10-12T11:29:00.775+0800";
  int64_t     ts, tmp_ts = 0;
  struct STm  tm;

  ASSERT_EQ(TSDB_CODE_SUCCESS, taosParseTime(ts_str_ns, &ts, strlen(ts_str_ns), TSDB_TIME_PRECISION_NANO, NULL));
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_NANO, 2023 - 1900, 9 /* mon start from 0*/, 12, 11, 29, 0,
                               775726171L);

  ASSERT_EQ(TSDB_CODE_SUCCESS, taosParseTime(ts_str_us, &ts, strlen(ts_str_us), TSDB_TIME_PRECISION_MICRO, NULL));
  test_timestamp_tm_conversion(ts, TSDB_TIME_PRECISION_MICRO, 2023 - 1900, 9 /* mon start from 0*/, 12, 11, 29, 0,
                               775726000L);

  ASSERT_EQ(TSDB_CODE_SUCCESS, taosParseTime(ts_str_ms, &ts, strlen(ts_str_ms), TSDB_TIME_PRECISION_MILLI, NULL));
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
  char    buf[256] = {0};
  int32_t code = TEST_ts2char(format, ts, precison, buf, 256);
  ASSERT_EQ(code, 0);
  printf("ts: %ld format: %s res: [%s], expected: [%s]\n", ts, format, buf, expected);
  ASSERT_STREQ(expected, buf);
}

TEST(timeTest, ts2char) {
  osDefaultInit();
  if (taosGetLocalTimezoneOffset() != TdEastZone8) GTEST_SKIP();
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
  if (taosGetLocalTimezoneOffset() != TdEastZone8) GTEST_SKIP();
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
  ASSERT_EQ(ts, -1 * taosGetLocalTimezoneOffset());

  ASSERT_EQ(0, TEST_char2ts("yyyyMM1/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "210001/2"));
  ASSERT_EQ(ts, 4102502400000000LL);

  ASSERT_EQ(-2, TEST_char2ts("yyyyMM/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "210013/2"));
  ASSERT_EQ(-2, TEST_char2ts("yyyyMM/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "210011/32"));
  ASSERT_EQ(-1, TEST_char2ts("HH12:MI:SS", &ts, TSDB_TIME_PRECISION_MICRO, "21:12:12"));
  ASSERT_EQ(-1, TEST_char2ts("yyyy/MM1/dd ", &ts, TSDB_TIME_PRECISION_MICRO, "2100111111111/11/2"));

  TEST_char2ts("yyyy/MM1/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "23/11/2-13");
  // ASSERT_EQ(-2, TEST_char2ts("yyyy/MM1/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "23/11/2-13"));
  ASSERT_EQ(0, TEST_char2ts("yyyy年 MM/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "1970年1/1+0"));
  // ASSERT_EQ(ts, 0);
  ASSERT_EQ(-1, TEST_char2ts("yyyy年a MM/dd", &ts, TSDB_TIME_PRECISION_MICRO, "2023年1/2"));
  ASSERT_EQ(0, TEST_char2ts("yyyy年 MM/ddTZH", &ts, TSDB_TIME_PRECISION_MICRO, "1970年   1/1+0"));
  // ASSERT_EQ(ts, 0);
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

// Define test cases
TEST(AlreadyAddGroupIdTest, GroupIdAdded) {
  // Test case 1: Group ID has been added
  char    ctbName[64] = "abc123";
  int64_t groupId = 123;
  bool    result = alreadyAddGroupId(ctbName, groupId);
  EXPECT_TRUE(result);
}

TEST(AlreadyAddGroupIdTest, GroupIdNotAdded) {
  // Test case 2: Group ID has not been added
  char    ctbName[64] = "abc456";
  int64_t groupId = 123;
  bool    result = alreadyAddGroupId(ctbName, groupId);
  EXPECT_FALSE(result);
}

TEST(AlreadyAddGroupIdTest, GroupIdAddedAtTheEnd) {
  // Test case 3: Group ID has been added at the end
  char    ctbName[64] = "xyz1";
  int64_t groupId = 1;
  bool    result = alreadyAddGroupId(ctbName, groupId);
  EXPECT_TRUE(result);
}

TEST(AlreadyAddGroupIdTest, GroupIdAddedWithDifferentLength) {
  // Test case 4: Group ID has been added with different length
  char    ctbName[64] = "def";
  int64_t groupId = 123456;
  bool    result = alreadyAddGroupId(ctbName, groupId);
  EXPECT_FALSE(result);
}

#define SLOW_LOG_TYPE_NULL   0x0
#define SLOW_LOG_TYPE_QUERY  0x1
#define SLOW_LOG_TYPE_INSERT 0x2
#define SLOW_LOG_TYPE_OTHERS 0x4
#define SLOW_LOG_TYPE_ALL    0x7

static int32_t taosSetSlowLogScope2(char* pScopeStr, int32_t* pScope) {
  if (NULL == pScopeStr || 0 == strlen(pScopeStr)) {
    *pScope = SLOW_LOG_TYPE_QUERY;
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  int32_t slowScope = 0;

  char* scope = NULL;
  char* tmp = NULL;
  while ((scope = strsep(&pScopeStr, "|")) != NULL) {
    taosMemoryFreeClear(tmp);
    tmp = taosStrdup(scope);
    strtrim(tmp);
    if (0 == strcasecmp(tmp, "all")) {
      slowScope |= SLOW_LOG_TYPE_ALL;
      continue;
    }

    if (0 == strcasecmp(tmp, "query")) {
      slowScope |= SLOW_LOG_TYPE_QUERY;
      continue;
    }

    if (0 == strcasecmp(tmp, "insert")) {
      slowScope |= SLOW_LOG_TYPE_INSERT;
      continue;
    }

    if (0 == strcasecmp(tmp, "others")) {
      slowScope |= SLOW_LOG_TYPE_OTHERS;
      continue;
    }

    if (0 == strcasecmp(tmp, "none")) {
      slowScope |= SLOW_LOG_TYPE_NULL;
      continue;
    }

    taosMemoryFreeClear(tmp);
    uError("Invalid slowLog scope value:%s", pScopeStr);
    TAOS_RETURN(TSDB_CODE_INVALID_CFG_VALUE);
  }

  *pScope = slowScope;
  taosMemoryFreeClear(tmp);
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

TEST(TaosSetSlowLogScopeTest, NullPointerInput) {
  char*   pScopeStr = NULL;
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, SLOW_LOG_TYPE_QUERY);
}

TEST(TaosSetSlowLogScopeTest, EmptyStringInput) {
  char    pScopeStr[1] = "";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, SLOW_LOG_TYPE_QUERY);
}

TEST(TaosSetSlowLogScopeTest, AllScopeInput) {
  char    pScopeStr[] = "all";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);

  EXPECT_EQ(scope, SLOW_LOG_TYPE_ALL);
}

TEST(TaosSetSlowLogScopeTest, QueryScopeInput) {
  char    pScopeStr[] = " query";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, SLOW_LOG_TYPE_QUERY);
}

TEST(TaosSetSlowLogScopeTest, InsertScopeInput) {
  char    pScopeStr[] = "insert";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, SLOW_LOG_TYPE_INSERT);
}

TEST(TaosSetSlowLogScopeTest, OthersScopeInput) {
  char    pScopeStr[] = "others";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, SLOW_LOG_TYPE_OTHERS);
}

TEST(TaosSetSlowLogScopeTest, NoneScopeInput) {
  char    pScopeStr[] = "none";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, SLOW_LOG_TYPE_NULL);
}

TEST(TaosSetSlowLogScopeTest, InvalidScopeInput) {
  char    pScopeStr[] = "invalid";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  // EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  // EXPECT_EQ(scope, -1);
}

TEST(TaosSetSlowLogScopeTest, MixedScopesInput) {
  char    pScopeStr[] = "query|insert|others|none";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, (SLOW_LOG_TYPE_QUERY | SLOW_LOG_TYPE_INSERT | SLOW_LOG_TYPE_OTHERS));
}

TEST(TaosSetSlowLogScopeTest, MixedScopesInputWithSpaces) {
  char    pScopeStr[] = "query | insert | others ";
  int32_t scope = 0;
  int32_t result = taosSetSlowLogScope2(pScopeStr, &scope);
  EXPECT_EQ(result, TSDB_CODE_SUCCESS);
  EXPECT_EQ(scope, (SLOW_LOG_TYPE_QUERY | SLOW_LOG_TYPE_INSERT | SLOW_LOG_TYPE_OTHERS));
}

TEST(testCase, function_param_check) {
  char* param = (char*) taosMemoryMalloc(1024);
  strcpy(param, "'algorithm=arima, frows=12'");

  SHashObj* p = NULL;
  int32_t code = taosAnalyGetOpts(param, &p);

  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(taosHashGetSize(p), 2);

    void* pVal = taosHashGet(p, "algo", strlen("algo"));
    EXPECT_TRUE(pVal == NULL);

    pVal = taosHashGet(p, "rows", strlen("rows"));
    EXPECT_TRUE(pVal == NULL);

    pVal = taosHashGet(p, "frows", strlen("frows"));

    char* pStr = taosStrndup((const char*) pVal, taosHashGetValueSize(pVal));
    EXPECT_STREQ(pStr, "12");

    taosMemoryFree(pStr);

    pVal = taosHashGet(p, "algorithm", strlen("algorithm"));
    pStr = taosStrndup((const char*) pVal, taosHashGetValueSize(pVal));
    EXPECT_STREQ(pStr, "arima");

    taosMemoryFree(pStr);
  }

  taosHashCleanup(p);
  p = NULL;

  strcpy(param, " ");
  code = taosAnalyGetOpts(param, &p);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(taosHashGetSize(p), 0);

    void* pVal = taosHashGet(p, "algorithm", strlen("algorithm"));
    EXPECT_TRUE(pVal == NULL);
  }

  taosHashCleanup(p);
  p = NULL;

  strcpy(param, " , , ,");
  code = taosAnalyGetOpts(param, &p);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(taosHashGetSize(p), 0);

    void* pVal = taosHashGet(p, "algorithm", strlen("algorithm"));
    EXPECT_TRUE(pVal == NULL);
  }

  taosHashCleanup(p);
  p = NULL;

  strcpy(param, "a, b, c,");
  code = taosAnalyGetOpts(param, &p);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(taosHashGetSize(p), 0);
  }

  taosHashCleanup(p);
  p = NULL;

  strcpy(param, "\" a, b, c, d = 12 \"");
  code = taosAnalyGetOpts(param, &p);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(taosHashGetSize(p), 1);

    void* pVal = taosHashGet(p, "d", strlen("d"));
    char* pStr = taosStrndup((const char*) pVal, taosHashGetValueSize(pVal));

    EXPECT_STREQ(pStr, "12");
    taosMemoryFree(pStr);
  }

  taosHashCleanup(p);
  p = NULL;

  strcpy(param, "\" a, b, c, d = , c = 911 \"");
  code = taosAnalyGetOpts(param, &p);
  if (code == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(taosHashGetSize(p), 2);

    void* pVal = taosHashGet(p, "c", strlen("c"));
    char* pStr = taosStrndup((const char*) pVal, taosHashGetValueSize(pVal));

    EXPECT_STREQ((char*) pStr, "911");
    taosMemoryFree(pStr);
  }

  taosHashCleanup(p);
  p = NULL;

  taosMemoryFree(param);
}

TEST(testCase, function_fqdn) {
  tsEnableIpv6 = 1;
  {
    SEp ep = {0};
    char *para = "127.0.0.1";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "127.0.0.1"), 0);
    ASSERT_EQ(ep.port, 6030);
  }

  {
    SEp ep = {0};
    char *para = "::1";
   taosGetFqdnPortFromEp (para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "::1"), 0);
    ASSERT_EQ(ep.port, tsServerPort);
  }
   
  {
    SEp ep = {0};
    char *para = "::1:6030";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "::1"), 0);
    ASSERT_EQ(ep.port, 6030);
  }
  {
    SEp ep = {0};
    char *para = "::1:7030";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "::1"), 0);
    ASSERT_EQ(ep.port, 7030);
  }

  {
    SEp ep = {0};
    char *para = "test:7030";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "test"), 0);
    ASSERT_EQ(ep.port, 7030);
  }

  {
    SEp ep = {0};
    char *para = "test";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "test"), 0);
    ASSERT_EQ(ep.port, 6030);
  }

  {
    SEp ep = {0};
    char *para = "[test]";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "test"), 0);
    ASSERT_EQ(ep.port, 6030);
  }
  {
    SEp ep = {0};
    char *para = "[test]:6030";
   taosGetFqdnPortFromEp(para, &ep);
    ASSERT_EQ(strcmp(ep.fqdn, "test"), 0);
    ASSERT_EQ(ep.port, 6030);
  }

}

TEST(testCase, function_taosTimeTruncate) {
  int64_t ts = 1633450000000;
  SInterval interval = {};
  interval.timezone = NULL;
  interval.intervalUnit = 'n';
  interval.slidingUnit = 'n';
  interval.offsetUnit = 0;
  interval.precision = 0;
  interval.interval = 11;
  interval.sliding = 11;
  interval.offset = 24105600000;
  interval.timeRange.skey = INT64_MIN;
  interval.timeRange.ekey = INT64_MAX;
  int64_t res = taosTimeTruncate(ts, &interval);
  ASSERT_LE(res, 1633450000000);
}

TEST(RepairOptionParseTest, ParseNodeType) {
  ERepairNodeType nodeType = REPAIR_NODE_TYPE_INVALID;
  ASSERT_EQ(tRepairParseNodeType("vnode", &nodeType), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodeType, REPAIR_NODE_TYPE_VNODE);

  ASSERT_EQ(tRepairParseNodeType("MNODE", &nodeType), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodeType, REPAIR_NODE_TYPE_MNODE);

  ASSERT_EQ(tRepairParseNodeType("not-a-node", &nodeType), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseNodeType(NULL, &nodeType), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseNodeType("vnode", NULL), TSDB_CODE_INVALID_PARA);
}

TEST(RepairOptionParseTest, ParseFileType) {
  ERepairFileType fileType = REPAIR_FILE_TYPE_INVALID;
  ASSERT_EQ(tRepairParseFileType("wal", &fileType), TSDB_CODE_SUCCESS);
  ASSERT_EQ(fileType, REPAIR_FILE_TYPE_WAL);

  ASSERT_EQ(tRepairParseFileType("TSDB", &fileType), TSDB_CODE_SUCCESS);
  ASSERT_EQ(fileType, REPAIR_FILE_TYPE_TSDB);

  ASSERT_EQ(tRepairParseFileType("meta", &fileType), TSDB_CODE_SUCCESS);
  ASSERT_EQ(fileType, REPAIR_FILE_TYPE_META);

  ASSERT_EQ(tRepairParseFileType("TDB", &fileType), TSDB_CODE_SUCCESS);
  ASSERT_EQ(fileType, REPAIR_FILE_TYPE_META);

  ASSERT_EQ(tRepairParseFileType("bad-file-type", &fileType), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseFileType(NULL, &fileType), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseFileType("wal", NULL), TSDB_CODE_INVALID_PARA);
}

TEST(RepairOptionParseTest, ParseMode) {
  ERepairMode mode = REPAIR_MODE_INVALID;
  ASSERT_EQ(tRepairParseMode("force", &mode), TSDB_CODE_SUCCESS);
  ASSERT_EQ(mode, REPAIR_MODE_FORCE);

  ASSERT_EQ(tRepairParseMode("CoPy", &mode), TSDB_CODE_SUCCESS);
  ASSERT_EQ(mode, REPAIR_MODE_COPY);

  ASSERT_EQ(tRepairParseMode("unknown-mode", &mode), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseMode(NULL, &mode), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseMode("force", NULL), TSDB_CODE_INVALID_PARA);
}

TEST(RepairOptionParseTest, ParseCliOption) {
  SRepairCliArgs cliArgs = {0};

  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(cliArgs.hasNodeType);
  ASSERT_EQ(cliArgs.nodeType, REPAIR_NODE_TYPE_VNODE);

  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "WAL"), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(cliArgs.hasFileType);
  ASSERT_EQ(cliArgs.fileType, REPAIR_FILE_TYPE_WAL);

  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(cliArgs.hasVnodeIdList);
  ASSERT_STREQ(cliArgs.vnodeIdList, "2,3");

  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "backup-path", "/tmp/backup"), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(cliArgs.hasBackupPath);
  ASSERT_STREQ(cliArgs.backupPath, "/tmp/backup");

  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(cliArgs.hasMode);
  ASSERT_EQ(cliArgs.mode, REPAIR_MODE_FORCE);

  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "replica-node", "192.168.1.24:/root/dataDir"), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(cliArgs.hasReplicaNode);
  ASSERT_STREQ(cliArgs.replicaNode, "192.168.1.24:/root/dataDir");
}

TEST(RepairOptionParseTest, ParseCliOptionInvalid) {
  SRepairCliArgs cliArgs = {0};

  ASSERT_EQ(tRepairParseCliOption(NULL, "node-type", "vnode"), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, NULL, "vnode"), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", NULL), TSDB_CODE_INVALID_PARA);

  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "bad-node"), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "unknown-option", "vnode"), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", ""), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "bad-mode"), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "backup-path", ""), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "replica-node", ""), TSDB_CODE_INVALID_PARA);

  std::string tooLongVnodeId(PATH_MAX, '1');
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", tooLongVnodeId.c_str()), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "backup-path", tooLongVnodeId.c_str()), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "replica-node", tooLongVnodeId.c_str()), TSDB_CODE_INVALID_PARA);
}

TEST(RepairOptionParseTest, ValidateCliArgsSuccess) {
  SRepairCliArgs cliArgs = {0};

  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairValidateCliArgs(&cliArgs), TSDB_CODE_SUCCESS);
}

TEST(RepairOptionParseTest, ValidateCliArgsMissingRequired) {
  {
    SRepairCliArgs cliArgs = {0};
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairValidateCliArgs(&cliArgs), TSDB_CODE_INVALID_PARA);
  }
  {
    SRepairCliArgs cliArgs = {0};
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairValidateCliArgs(&cliArgs), TSDB_CODE_INVALID_PARA);
  }
}

TEST(RepairOptionParseTest, ValidateCliArgsNodeFileMismatch) {
  {
    SRepairCliArgs cliArgs = {0};
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "config"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairValidateCliArgs(&cliArgs), TSDB_CODE_INVALID_PARA);
  }
  {
    SRepairCliArgs cliArgs = {0};
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "dnode"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "config"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairValidateCliArgs(&cliArgs), TSDB_CODE_SUCCESS);
  }
}

TEST(RepairOptionParseTest, ValidateCliArgsReplicaNodeRule) {
  {
    SRepairCliArgs cliArgs = {0};
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "copy"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairValidateCliArgs(&cliArgs), TSDB_CODE_INVALID_PARA);

    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "replica-node", "192.168.1.24:/root/dataDir"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairValidateCliArgs(&cliArgs), TSDB_CODE_SUCCESS);
  }
  {
    SRepairCliArgs cliArgs = {0};
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "replica-node", "192.168.1.24:/root/dataDir"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairValidateCliArgs(&cliArgs), TSDB_CODE_INVALID_PARA);
  }
}

TEST(RepairOptionParseTest, ValidateCliArgsVnodeIdRule) {
  {
    SRepairCliArgs cliArgs = {0};
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairValidateCliArgs(&cliArgs), TSDB_CODE_INVALID_PARA);
  }
  {
    SRepairCliArgs cliArgs = {0};
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "mnode"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
    ASSERT_EQ(tRepairValidateCliArgs(&cliArgs), TSDB_CODE_INVALID_PARA);
  }
}

TEST(RepairOptionParseTest, InitRepairCtxSuccess) {
  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "meta"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "backup-path", "/tmp/backup"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);

  const int64_t startTs = 1735689600123LL;
  SRepairCtx    ctx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, startTs, &ctx), TSDB_CODE_SUCCESS);

  ASSERT_TRUE(ctx.enabled);
  ASSERT_EQ(ctx.startTimeMs, startTs);
  ASSERT_STREQ(ctx.sessionId, "repair-1735689600123");
  ASSERT_EQ(ctx.nodeType, REPAIR_NODE_TYPE_VNODE);
  ASSERT_EQ(ctx.fileType, REPAIR_FILE_TYPE_META);
  ASSERT_EQ(ctx.mode, REPAIR_MODE_FORCE);
  ASSERT_TRUE(ctx.hasVnodeIdList);
  ASSERT_STREQ(ctx.vnodeIdList, "2,3");
  ASSERT_EQ(ctx.vnodeIdNum, 2);
  ASSERT_EQ(ctx.vnodeIds[0], 2);
  ASSERT_EQ(ctx.vnodeIds[1], 3);
  ASSERT_TRUE(ctx.hasBackupPath);
  ASSERT_STREQ(ctx.backupPath, "/tmp/backup");
  ASSERT_FALSE(ctx.hasReplicaNode);

  bool shouldRepair = false;
  ASSERT_EQ(tRepairShouldRepairVnode(&ctx, 2, &shouldRepair), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(shouldRepair);
  ASSERT_EQ(tRepairShouldRepairVnode(&ctx, 9, &shouldRepair), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(shouldRepair);
}

TEST(RepairOptionParseTest, InitRepairCtxInvalidArgs) {
  SRepairCtx ctx = {0};
  ASSERT_EQ(tRepairInitCtx(NULL, 1735689600123LL, &ctx), TSDB_CODE_INVALID_PARA);

  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689600123LL, NULL), TSDB_CODE_INVALID_PARA);

  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2,a"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689600123LL, &ctx), TSDB_CODE_INVALID_PARA);

  memset(&cliArgs, 0, sizeof(cliArgs));
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689600123LL, &ctx), TSDB_CODE_INVALID_PARA);

  memset(&cliArgs, 0, sizeof(cliArgs));
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "copy"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "replica-node", "127.0.0.1:/var/lib/taos"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689600999LL, &ctx), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(ctx.hasBackupPath);
  ASSERT_STREQ(ctx.backupPath, "");
}

TEST(RepairOptionParseTest, PrecheckDataDirNotExist) {
  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);

  SRepairCtx ctx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601000LL, &ctx), TSDB_CODE_SUCCESS);

  std::string dataDir = buildRepairTempPath("missing-data");
  ASSERT_FALSE(taosDirExist(dataDir.c_str()));
  ASSERT_NE(tRepairPrecheck(&ctx, dataDir.c_str(), 0), TSDB_CODE_SUCCESS);
}

TEST(RepairOptionParseTest, PrecheckBackupPathNotExist) {
  const std::string dataDirPath = buildRepairTempPath("missing-backup-data");
  RepairTempDirGuard dataDirGuard(dataDirPath);
  const std::string sep(TD_DIRSEP);
  const std::string walDir = dataDirPath + sep + "vnode" + sep + "vnode2" + sep + "wal";
  ASSERT_EQ(taosMulMkDir(walDir.c_str()), 0);

  std::string backupDir = buildRepairTempPath("missing-backup-dir");
  ASSERT_FALSE(taosDirExist(backupDir.c_str()));

  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "backup-path", backupDir.c_str()), TSDB_CODE_SUCCESS);

  SRepairCtx ctx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601001LL, &ctx), TSDB_CODE_SUCCESS);
  ASSERT_NE(tRepairPrecheck(&ctx, dataDirPath.c_str(), 0), TSDB_CODE_SUCCESS);
}

TEST(RepairOptionParseTest, PrecheckDiskSpaceNotEnough) {
  const std::string dataDirPath = buildRepairTempPath("disk-space-data");
  RepairTempDirGuard dataDirGuard(dataDirPath);
  const std::string sep(TD_DIRSEP);
  const std::string walDir = dataDirPath + sep + "vnode" + sep + "vnode2" + sep + "wal";
  ASSERT_EQ(taosMulMkDir(walDir.c_str()), 0);

  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);

  SRepairCtx ctx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601002LL, &ctx), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairPrecheck(&ctx, dataDirPath.c_str(), INT64_MAX), TSDB_CODE_NO_ENOUGH_DISKSPACE);
}

TEST(RepairOptionParseTest, PrecheckTargetPathMissing) {
  const std::string dataDirPath = buildRepairTempPath("missing-target-data");
  RepairTempDirGuard dataDirGuard(dataDirPath);
  const std::string sep(TD_DIRSEP);
  const std::string vnodeDir = dataDirPath + sep + "vnode" + sep + "vnode2";
  ASSERT_EQ(taosMulMkDir(vnodeDir.c_str()), 0);

  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);

  SRepairCtx ctx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601003LL, &ctx), TSDB_CODE_SUCCESS);
  ASSERT_NE(tRepairPrecheck(&ctx, dataDirPath.c_str(), 0), TSDB_CODE_SUCCESS);
}

TEST(RepairOptionParseTest, PrecheckSuccess) {
  const std::string dataDirPath = buildRepairTempPath("precheck-success-data");
  RepairTempDirGuard dataDirGuard(dataDirPath);
  const std::string backupDirPath = buildRepairTempPath("precheck-success-backup");
  RepairTempDirGuard backupDirGuard(backupDirPath);
  const std::string sep(TD_DIRSEP);

  const std::string walDir2 = dataDirPath + sep + "vnode" + sep + "vnode2" + sep + "wal";
  const std::string walDir3 = dataDirPath + sep + "vnode" + sep + "vnode3" + sep + "wal";
  ASSERT_EQ(taosMulMkDir(walDir2.c_str()), 0);
  ASSERT_EQ(taosMulMkDir(walDir3.c_str()), 0);
  ASSERT_EQ(taosMulMkDir(backupDirPath.c_str()), 0);

  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "backup-path", backupDirPath.c_str()), TSDB_CODE_SUCCESS);

  SRepairCtx ctx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601004LL, &ctx), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairPrecheck(&ctx, dataDirPath.c_str(), 0), TSDB_CODE_SUCCESS);
}

TEST(RepairOptionParseTest, PrepareBackupDirWithConfiguredPath) {
  const std::string backupRoot = buildRepairTempPath("backup-root-configured");
  RepairTempDirGuard backupRootGuard(backupRoot);
  ASSERT_EQ(taosMulMkDir(backupRoot.c_str()), 0);

  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "backup-path", backupRoot.c_str()), TSDB_CODE_SUCCESS);

  SRepairCtx ctx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601101LL, &ctx), TSDB_CODE_SUCCESS);

  char backupDir[PATH_MAX] = {0};
  ASSERT_EQ(tRepairPrepareBackupDir(&ctx, "/tmp/unused-data-dir", 2, backupDir, sizeof(backupDir)),
            TSDB_CODE_SUCCESS);

  std::string expected =
      backupRoot + std::string(TD_DIRSEP) + "repair-1735689601101" + TD_DIRSEP + "vnode2" + TD_DIRSEP + "wal";
  ASSERT_STREQ(backupDir, expected.c_str());
  ASSERT_TRUE(taosDirExist(backupDir));
}

TEST(RepairOptionParseTest, PrepareBackupDirWithDefaultPath) {
  const std::string dataDir = buildRepairTempPath("backup-default-data");
  RepairTempDirGuard dataDirGuard(dataDir);
  ASSERT_EQ(taosMulMkDir(dataDir.c_str()), 0);

  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "meta"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "3"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);

  SRepairCtx ctx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601102LL, &ctx), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(ctx.hasBackupPath);

  char backupDir[PATH_MAX] = {0};
  ASSERT_EQ(tRepairPrepareBackupDir(&ctx, dataDir.c_str(), 3, backupDir, sizeof(backupDir)), TSDB_CODE_SUCCESS);

  std::string expected =
      dataDir + std::string(TD_DIRSEP) + "backup" + TD_DIRSEP + "repair-1735689601102" + TD_DIRSEP + "vnode3" +
      TD_DIRSEP + "meta";
  ASSERT_STREQ(backupDir, expected.c_str());
  ASSERT_TRUE(taosDirExist(backupDir));
}

TEST(RepairOptionParseTest, PrepareBackupDirInvalidArgs) {
  SRepairCtx ctx = {0};
  char       backupDir[PATH_MAX] = {0};
  ASSERT_EQ(tRepairPrepareBackupDir(NULL, "/tmp", 2, backupDir, sizeof(backupDir)), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairPrepareBackupDir(&ctx, "/tmp", 2, backupDir, sizeof(backupDir)), TSDB_CODE_INVALID_PARA);

  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601103LL, &ctx), TSDB_CODE_SUCCESS);

  ASSERT_EQ(tRepairPrepareBackupDir(&ctx, NULL, 2, backupDir, sizeof(backupDir)), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairPrepareBackupDir(&ctx, "", 2, backupDir, sizeof(backupDir)), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairPrepareBackupDir(&ctx, "/tmp", -1, backupDir, sizeof(backupDir)), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairPrepareBackupDir(&ctx, "/tmp", 2, NULL, sizeof(backupDir)), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairPrepareBackupDir(&ctx, "/tmp", 2, backupDir, 0), TSDB_CODE_INVALID_PARA);
}

TEST(RepairOptionParseTest, PrepareSessionFilesWithConfiguredPath) {
  const std::string backupRoot = buildRepairTempPath("session-files-configured-root");
  RepairTempDirGuard backupRootGuard(backupRoot);
  ASSERT_EQ(taosMulMkDir(backupRoot.c_str()), 0);

  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "backup-path", backupRoot.c_str()), TSDB_CODE_SUCCESS);

  SRepairCtx ctx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601201LL, &ctx), TSDB_CODE_SUCCESS);

  char sessionDir[PATH_MAX] = {0};
  char logPath[PATH_MAX] = {0};
  char statePath[PATH_MAX] = {0};
  ASSERT_EQ(
      tRepairPrepareSessionFiles(&ctx, "/tmp/unused-data-dir", sessionDir, sizeof(sessionDir), logPath,
                                 sizeof(logPath), statePath, sizeof(statePath)),
      TSDB_CODE_SUCCESS);

  std::string expectedSessionDir = backupRoot + std::string(TD_DIRSEP) + "repair-1735689601201";
  std::string expectedLogPath = expectedSessionDir + TD_DIRSEP + "repair.log";
  std::string expectedStatePath = expectedSessionDir + TD_DIRSEP + "repair.state.json";
  ASSERT_STREQ(sessionDir, expectedSessionDir.c_str());
  ASSERT_STREQ(logPath, expectedLogPath.c_str());
  ASSERT_STREQ(statePath, expectedStatePath.c_str());

  ASSERT_TRUE(taosDirExist(sessionDir));
  ASSERT_TRUE(taosCheckExistFile(logPath));
  ASSERT_TRUE(taosCheckExistFile(statePath));

  std::string stateContent = readRepairFileContent(statePath);
  ASSERT_FALSE(stateContent.empty());
  SJson *pJson = tjsonParse(stateContent.c_str());
  ASSERT_NE(pJson, nullptr);

  char sessionId[REPAIR_SESSION_ID_LEN] = {0};
  ASSERT_EQ(tjsonGetStringValue2(pJson, "sessionId", sessionId, sizeof(sessionId)), TSDB_CODE_SUCCESS);
  ASSERT_STREQ(sessionId, "repair-1735689601201");

  int64_t startTimeMs = 0;
  ASSERT_EQ(tjsonGetBigIntValue(pJson, "startTimeMs", &startTimeMs), TSDB_CODE_SUCCESS);
  ASSERT_EQ(startTimeMs, 1735689601201LL);

  char status[64] = {0};
  ASSERT_EQ(tjsonGetStringValue2(pJson, "status", status, sizeof(status)), TSDB_CODE_SUCCESS);
  ASSERT_STREQ(status, "initialized");

  int32_t totalVnodes = 0;
  ASSERT_EQ(tjsonGetIntValue(pJson, "totalVnodes", &totalVnodes), TSDB_CODE_SUCCESS);
  ASSERT_EQ(totalVnodes, 1);
  tjsonDelete(pJson);
}

TEST(RepairOptionParseTest, AppendSessionLogAndWriteSessionState) {
  const std::string dataDir = buildRepairTempPath("session-files-default-data");
  RepairTempDirGuard dataDirGuard(dataDir);
  ASSERT_EQ(taosMulMkDir(dataDir.c_str()), 0);

  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "meta"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);

  SRepairCtx ctx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601202LL, &ctx), TSDB_CODE_SUCCESS);

  char sessionDir[PATH_MAX] = {0};
  char logPath[PATH_MAX] = {0};
  char statePath[PATH_MAX] = {0};
  ASSERT_EQ(tRepairPrepareSessionFiles(&ctx, dataDir.c_str(), sessionDir, sizeof(sessionDir), logPath,
                                       sizeof(logPath), statePath, sizeof(statePath)),
            TSDB_CODE_SUCCESS);

  ASSERT_EQ(tRepairAppendSessionLog(logPath, "precheck passed"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairAppendSessionLog(logPath, "backup directories prepared"), TSDB_CODE_SUCCESS);
  std::string logContent = readRepairFileContent(logPath);
  ASSERT_NE(logContent.find("precheck passed"), std::string::npos);
  ASSERT_NE(logContent.find("backup directories prepared"), std::string::npos);

  ASSERT_EQ(tRepairWriteSessionState(&ctx, statePath, "precheck", "running", 1, 2), TSDB_CODE_SUCCESS);
  std::string stateContent = readRepairFileContent(statePath);
  ASSERT_FALSE(stateContent.empty());

  SJson *pJson = tjsonParse(stateContent.c_str());
  ASSERT_NE(pJson, nullptr);

  char step[64] = {0};
  ASSERT_EQ(tjsonGetStringValue2(pJson, "step", step, sizeof(step)), TSDB_CODE_SUCCESS);
  ASSERT_STREQ(step, "precheck");

  char status[64] = {0};
  ASSERT_EQ(tjsonGetStringValue2(pJson, "status", status, sizeof(status)), TSDB_CODE_SUCCESS);
  ASSERT_STREQ(status, "running");

  int32_t doneVnodes = 0;
  int32_t totalVnodes = 0;
  ASSERT_EQ(tjsonGetIntValue(pJson, "doneVnodes", &doneVnodes), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tjsonGetIntValue(pJson, "totalVnodes", &totalVnodes), TSDB_CODE_SUCCESS);
  ASSERT_EQ(doneVnodes, 1);
  ASSERT_EQ(totalVnodes, 2);
  tjsonDelete(pJson);
}

TEST(RepairOptionParseTest, SessionFilesInvalidArgs) {
  SRepairCtx ctx = {0};
  char       sessionDir[PATH_MAX] = {0};
  char       logPath[PATH_MAX] = {0};
  char       statePath[PATH_MAX] = {0};

  ASSERT_EQ(tRepairPrepareSessionFiles(NULL, "/tmp", sessionDir, sizeof(sessionDir), logPath, sizeof(logPath),
                                       statePath, sizeof(statePath)),
            TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairPrepareSessionFiles(&ctx, "/tmp", sessionDir, sizeof(sessionDir), logPath, sizeof(logPath),
                                       statePath, sizeof(statePath)),
            TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairPrepareSessionFiles(&ctx, NULL, sessionDir, sizeof(sessionDir), logPath, sizeof(logPath),
                                       statePath, sizeof(statePath)),
            TSDB_CODE_INVALID_PARA);

  ASSERT_EQ(tRepairAppendSessionLog(NULL, "msg"), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairAppendSessionLog("", "msg"), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairAppendSessionLog("/tmp/x.log", NULL), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairAppendSessionLog("/tmp/x.log", ""), TSDB_CODE_INVALID_PARA);

  ASSERT_EQ(tRepairWriteSessionState(NULL, "/tmp/x.state", "step", "status", 1, 1), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairWriteSessionState(&ctx, "/tmp/x.state", "step", "status", 1, 1), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairWriteSessionState(&ctx, NULL, "step", "status", 1, 1), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairWriteSessionState(&ctx, "/tmp/x.state", NULL, "status", 1, 1), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairWriteSessionState(&ctx, "/tmp/x.state", "step", NULL, 1, 1), TSDB_CODE_INVALID_PARA);
}

TEST(RepairOptionParseTest, TryResumeSessionFindsUnfinishedState) {
  const std::string backupRoot = buildRepairTempPath("resume-root");
  RepairTempDirGuard backupRootGuard(backupRoot);
  ASSERT_EQ(taosMulMkDir(backupRoot.c_str()), 0);

  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "backup-path", backupRoot.c_str()), TSDB_CODE_SUCCESS);

  SRepairCtx finishedCtx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601401LL, &finishedCtx), TSDB_CODE_SUCCESS);
  char finishedSessionDir[PATH_MAX] = {0};
  char finishedLogPath[PATH_MAX] = {0};
  char finishedStatePath[PATH_MAX] = {0};
  ASSERT_EQ(tRepairPrepareSessionFiles(&finishedCtx, "/tmp/unused-data-dir", finishedSessionDir,
                                       sizeof(finishedSessionDir), finishedLogPath, sizeof(finishedLogPath),
                                       finishedStatePath, sizeof(finishedStatePath)),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairWriteSessionState(&finishedCtx, finishedStatePath, "preflight", "ready", 2, 2), TSDB_CODE_SUCCESS);

  SRepairCtx runningCtx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601402LL, &runningCtx), TSDB_CODE_SUCCESS);
  char runningSessionDir[PATH_MAX] = {0};
  char runningLogPath[PATH_MAX] = {0};
  char runningStatePath[PATH_MAX] = {0};
  ASSERT_EQ(
      tRepairPrepareSessionFiles(&runningCtx, "/tmp/unused-data-dir", runningSessionDir, sizeof(runningSessionDir),
                                 runningLogPath, sizeof(runningLogPath), runningStatePath, sizeof(runningStatePath)),
      TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairWriteSessionState(&runningCtx, runningStatePath, "backup", "running", 1, 2), TSDB_CODE_SUCCESS);

  SRepairCtx resumeCtx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601999LL, &resumeCtx), TSDB_CODE_SUCCESS);

  char    sessionDir[PATH_MAX] = {0};
  char    logPath[PATH_MAX] = {0};
  char    statePath[PATH_MAX] = {0};
  int32_t doneVnodes = -1;
  int32_t totalVnodes = -1;
  bool    resumed = false;
  ASSERT_EQ(tRepairTryResumeSession(&resumeCtx, "/tmp/unused-data-dir", sessionDir, sizeof(sessionDir), logPath,
                                    sizeof(logPath), statePath, sizeof(statePath), &doneVnodes, &totalVnodes,
                                    &resumed),
            TSDB_CODE_SUCCESS);
  ASSERT_TRUE(resumed);
  ASSERT_EQ(doneVnodes, 1);
  ASSERT_EQ(totalVnodes, 2);
  ASSERT_STREQ(resumeCtx.sessionId, "repair-1735689601402");
  ASSERT_EQ(resumeCtx.startTimeMs, 1735689601402LL);
  ASSERT_STREQ(sessionDir, runningSessionDir);
  ASSERT_STREQ(logPath, runningLogPath);
  ASSERT_STREQ(statePath, runningStatePath);
}

TEST(RepairOptionParseTest, TryResumeSessionSkipMismatchedState) {
  const std::string backupRoot = buildRepairTempPath("resume-mismatch-root");
  RepairTempDirGuard backupRootGuard(backupRoot);
  ASSERT_EQ(taosMulMkDir(backupRoot.c_str()), 0);

  SRepairCliArgs oldCliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&oldCliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&oldCliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&oldCliArgs, "vnode-id", "8,9"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&oldCliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&oldCliArgs, "backup-path", backupRoot.c_str()), TSDB_CODE_SUCCESS);

  SRepairCtx oldCtx = {0};
  ASSERT_EQ(tRepairInitCtx(&oldCliArgs, 1735689601403LL, &oldCtx), TSDB_CODE_SUCCESS);
  char oldSessionDir[PATH_MAX] = {0};
  char oldLogPath[PATH_MAX] = {0};
  char oldStatePath[PATH_MAX] = {0};
  ASSERT_EQ(tRepairPrepareSessionFiles(&oldCtx, "/tmp/unused-data-dir", oldSessionDir, sizeof(oldSessionDir),
                                       oldLogPath, sizeof(oldLogPath), oldStatePath, sizeof(oldStatePath)),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairWriteSessionState(&oldCtx, oldStatePath, "backup", "running", 1, 2), TSDB_CODE_SUCCESS);

  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "backup-path", backupRoot.c_str()), TSDB_CODE_SUCCESS);

  SRepairCtx resumeCtx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689602999LL, &resumeCtx), TSDB_CODE_SUCCESS);

  char    sessionDir[PATH_MAX] = {0};
  char    logPath[PATH_MAX] = {0};
  char    statePath[PATH_MAX] = {0};
  int32_t doneVnodes = -1;
  int32_t totalVnodes = -1;
  bool    resumed = true;
  ASSERT_EQ(tRepairTryResumeSession(&resumeCtx, "/tmp/unused-data-dir", sessionDir, sizeof(sessionDir), logPath,
                                    sizeof(logPath), statePath, sizeof(statePath), &doneVnodes, &totalVnodes,
                                    &resumed),
            TSDB_CODE_SUCCESS);
  ASSERT_FALSE(resumed);
  ASSERT_EQ(doneVnodes, 0);
  ASSERT_EQ(totalVnodes, 2);
  ASSERT_STREQ(resumeCtx.sessionId, "repair-1735689602999");
  ASSERT_EQ(resumeCtx.startTimeMs, 1735689602999LL);
  ASSERT_EQ(sessionDir[0], '\0');
  ASSERT_EQ(logPath[0], '\0');
  ASSERT_EQ(statePath[0], '\0');
}

TEST(RepairOptionParseTest, TryResumeSessionInvalidArgs) {
  SRepairCtx ctx = {0};
  char       path[PATH_MAX] = {0};
  int32_t    doneVnodes = 0;
  int32_t    totalVnodes = 0;
  bool       resumed = false;

  ASSERT_EQ(tRepairTryResumeSession(NULL, "/tmp", path, sizeof(path), path, sizeof(path), path, sizeof(path),
                                    &doneVnodes, &totalVnodes, &resumed),
            TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairTryResumeSession(&ctx, "/tmp", path, sizeof(path), path, sizeof(path), path, sizeof(path),
                                    &doneVnodes, &totalVnodes, &resumed),
            TSDB_CODE_INVALID_PARA);
}

TEST(RepairOptionParseTest, NeedRunWalForceRepair) {
  bool needRun = false;

  SRepairCliArgs walForceCli = {0};
  ASSERT_EQ(tRepairParseCliOption(&walForceCli, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&walForceCli, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&walForceCli, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&walForceCli, "mode", "force"), TSDB_CODE_SUCCESS);
  SRepairCtx walForceCtx = {0};
  ASSERT_EQ(tRepairInitCtx(&walForceCli, 1735689601501LL, &walForceCtx), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairNeedRunWalForceRepair(&walForceCtx, &needRun), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(needRun);

  SRepairCliArgs walReplicaCli = {0};
  ASSERT_EQ(tRepairParseCliOption(&walReplicaCli, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&walReplicaCli, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&walReplicaCli, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&walReplicaCli, "mode", "replica"), TSDB_CODE_SUCCESS);
  SRepairCtx walReplicaCtx = {0};
  ASSERT_EQ(tRepairInitCtx(&walReplicaCli, 1735689601502LL, &walReplicaCtx), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairNeedRunWalForceRepair(&walReplicaCtx, &needRun), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(needRun);

  SRepairCliArgs tsdbForceCli = {0};
  ASSERT_EQ(tRepairParseCliOption(&tsdbForceCli, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&tsdbForceCli, "file-type", "tsdb"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&tsdbForceCli, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&tsdbForceCli, "mode", "force"), TSDB_CODE_SUCCESS);
  SRepairCtx tsdbForceCtx = {0};
  ASSERT_EQ(tRepairInitCtx(&tsdbForceCli, 1735689601503LL, &tsdbForceCtx), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairNeedRunWalForceRepair(&tsdbForceCtx, &needRun), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(needRun);
}

TEST(RepairOptionParseTest, BuildVnodeTargetPath) {
  char targetPath[PATH_MAX] = {0};

  ASSERT_EQ(tRepairBuildVnodeTargetPath("/tmp/repair-data", 11, REPAIR_FILE_TYPE_WAL, targetPath, sizeof(targetPath)),
            TSDB_CODE_SUCCESS);
  std::string expectedWal =
      std::string("/tmp/repair-data") + TD_DIRSEP + "vnode" + TD_DIRSEP + "vnode11" + TD_DIRSEP + "wal";
  ASSERT_STREQ(targetPath, expectedWal.c_str());

  ASSERT_EQ(tRepairBuildVnodeTargetPath("/tmp/repair-data", 11, REPAIR_FILE_TYPE_META, targetPath, sizeof(targetPath)),
            TSDB_CODE_SUCCESS);
  std::string expectedMeta =
      std::string("/tmp/repair-data") + TD_DIRSEP + "vnode" + TD_DIRSEP + "vnode11" + TD_DIRSEP + "meta";
  ASSERT_STREQ(targetPath, expectedMeta.c_str());

  ASSERT_EQ(tRepairBuildVnodeTargetPath(NULL, 11, REPAIR_FILE_TYPE_WAL, targetPath, sizeof(targetPath)),
            TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairBuildVnodeTargetPath("/tmp/repair-data", -1, REPAIR_FILE_TYPE_WAL, targetPath, sizeof(targetPath)),
            TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairBuildVnodeTargetPath("/tmp/repair-data", 11, REPAIR_FILE_TYPE_DATA, targetPath, sizeof(targetPath)),
            TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairBuildVnodeTargetPath("/tmp/repair-data", 11, REPAIR_FILE_TYPE_WAL, NULL, sizeof(targetPath)),
            TSDB_CODE_INVALID_PARA);
}

TEST(RepairOptionParseTest, BuildProgressLineAndSummaryLine) {
  SRepairCliArgs cliArgs = {0};
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "node-type", "vnode"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "file-type", "wal"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "vnode-id", "2,3"), TSDB_CODE_SUCCESS);
  ASSERT_EQ(tRepairParseCliOption(&cliArgs, "mode", "force"), TSDB_CODE_SUCCESS);

  SRepairCtx ctx = {0};
  ASSERT_EQ(tRepairInitCtx(&cliArgs, 1735689601301LL, &ctx), TSDB_CODE_SUCCESS);

  char progressLine[256] = {0};
  ASSERT_EQ(tRepairBuildProgressLine(&ctx, "backup", 1, 2, progressLine, sizeof(progressLine)), TSDB_CODE_SUCCESS);
  ASSERT_STREQ(progressLine, "repair progress: session=repair-1735689601301 step=backup vnode=1/2 progress=50%");

  char summaryLine[256] = {0};
  ASSERT_EQ(tRepairBuildSummaryLine(&ctx, 2, 0, 4567, summaryLine, sizeof(summaryLine)), TSDB_CODE_SUCCESS);
  ASSERT_STREQ(summaryLine,
               "repair summary: session=repair-1735689601301 status=success successVnodes=2 failedVnodes=0 "
               "elapsedMs=4567");
}

TEST(RepairOptionParseTest, NeedReportProgress) {
  int64_t lastReportMs = 0;
  bool    needReport = false;

  ASSERT_EQ(tRepairNeedReportProgress(1000, 3000, &lastReportMs, &needReport), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(needReport);
  ASSERT_EQ(lastReportMs, 1000);

  ASSERT_EQ(tRepairNeedReportProgress(2000, 3000, &lastReportMs, &needReport), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(needReport);
  ASSERT_EQ(lastReportMs, 1000);

  ASSERT_EQ(tRepairNeedReportProgress(4001, 3000, &lastReportMs, &needReport), TSDB_CODE_SUCCESS);
  ASSERT_TRUE(needReport);
  ASSERT_EQ(lastReportMs, 4001);
}

TEST(RepairOptionParseTest, ProgressReporterInvalidArgs) {
  SRepairCtx ctx = {0};
  char       line[32] = {0};
  int64_t    lastReportMs = 0;
  bool       needReport = false;
  ASSERT_EQ(tRepairBuildProgressLine(NULL, "step", 0, 1, line, sizeof(line)), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairBuildSummaryLine(NULL, 1, 0, 1, line, sizeof(line)), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairNeedReportProgress(1, 1000, NULL, &needReport), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairNeedReportProgress(1, 1000, &lastReportMs, NULL), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairNeedReportProgress(-1, 1000, &lastReportMs, &needReport), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairNeedReportProgress(1, 0, &lastReportMs, &needReport), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairBuildProgressLine(&ctx, "step", 0, 1, line, sizeof(line)), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(tRepairBuildSummaryLine(&ctx, 1, 0, 1, line, sizeof(line)), TSDB_CODE_INVALID_PARA);
}

#pragma GCC diagnostic pop
