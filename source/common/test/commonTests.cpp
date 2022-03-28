#include <gtest/gtest.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "tcommon.h"
#include "tdatablock.h"
#include "tcommon.h"
#include "taos.h"
#include "tvariant.h"
#include "tdef.h"

namespace {
//
}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, toInteger_test) {
  char*    s = "123";
  uint32_t type = 0;

  int64_t val = 0;
  bool sign = true;

  int32_t ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 123);
  ASSERT_EQ(sign, true);

  s = "9223372036854775807";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 9223372036854775807);
  ASSERT_EQ(sign, true);

  s = "9323372036854775807";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 9323372036854775807u);
  ASSERT_EQ(sign, false);

  s = "-9323372036854775807";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, -1);

  s = "-1";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -1);
  ASSERT_EQ(sign, true);

  s = "-9223372036854775807";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, -9223372036854775807);
  ASSERT_EQ(sign, true);

  s = "1000u";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, -1);

  s = "0x10";
  ret = toInteger(s, strlen(s), 16, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 16);
  ASSERT_EQ(sign, true);

  s = "110";
  ret = toInteger(s, strlen(s), 2, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 6);
  ASSERT_EQ(sign, true);

  s = "110";
  ret = toInteger(s, strlen(s), 8, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 72);
  ASSERT_EQ(sign, true);

  //18446744073709551615  UINT64_MAX
  s = "18446744073709551615";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(val, 18446744073709551615u);
  ASSERT_EQ(sign, false);

  s = "18446744073709551616";
  ret = toInteger(s, strlen(s), 10, &val, &sign);
  ASSERT_EQ(ret, -1);
}

TEST(testCase, Datablock_test) {
  SSDataBlock* b = static_cast<SSDataBlock*>(taosMemoryCalloc(1, sizeof(SSDataBlock)));
  b->info.numOfCols = 2;
  b->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {0};
  infoData.info.bytes = 4;
  infoData.info.type = TSDB_DATA_TYPE_INT;
  infoData.info.colId = 1;

  infoData.pData = (char*) taosMemoryCalloc(40, infoData.info.bytes);
  infoData.nullbitmap = (char*) taosMemoryCalloc(1, sizeof(char) * (40/8));
  taosArrayPush(b->pDataBlock, &infoData);

  SColumnInfoData infoData1 = {0};
  infoData1.info.bytes = 40;
  infoData1.info.type = TSDB_DATA_TYPE_BINARY;
  infoData1.info.colId = 2;

  infoData1.varmeta.offset = (int32_t*) taosMemoryCalloc(40, sizeof(uint32_t));
  taosArrayPush(b->pDataBlock, &infoData1);

  char* str = "the value of: %d";
  char buf[128] = {0};
  char varbuf[128] = {0};

  for(int32_t i = 0; i < 40; ++i) {
    SColumnInfoData* p0 = (SColumnInfoData *) taosArrayGet(b->pDataBlock, 0);
    SColumnInfoData* p1 = (SColumnInfoData *) taosArrayGet(b->pDataBlock, 1);

    if (i&0x01) {
      int32_t len = sprintf(buf, str, i);
      STR_TO_VARSTR(varbuf, buf)
      colDataAppend(p0, i, (const char*) &i, false);
      colDataAppend(p1, i, (const char*) varbuf, false);

      memset(varbuf, 0, sizeof(varbuf));
      memset(buf, 0, sizeof(buf));
    } else {
      colDataAppend(p0, i, (const char*) &i, true);
      colDataAppend(p1, i, (const char*) varbuf, true);
    }

    b->info.rows++;
  }

  SColumnInfoData* p0 = (SColumnInfoData *) taosArrayGet(b->pDataBlock, 0);
  SColumnInfoData* p1 = (SColumnInfoData *) taosArrayGet(b->pDataBlock, 1);
  for(int32_t i = 0; i < 40; ++i) {
    if (i & 0x01) {
      ASSERT_EQ(colDataIsNull_f(p0->nullbitmap, i), false);
      ASSERT_EQ(colDataIsNull(p1, b->info.rows, i, nullptr), false);
    } else {
      ASSERT_EQ(colDataIsNull_f(p0->nullbitmap, i), true);

      ASSERT_EQ(colDataIsNull(p0, b->info.rows, i, nullptr), true);
      ASSERT_EQ(colDataIsNull(p1, b->info.rows, i, nullptr), true);
    }
  }

  printf("binary column length:%d\n", *(int32_t*) p1->pData);

  ASSERT_EQ(blockDataGetNumOfCols(b), 2);
  ASSERT_EQ(blockDataGetNumOfRows(b), 40);

  char* pData = colDataGetData(p1, 3);
  printf("the second row of binary:%s, length:%d\n", (char*)varDataVal(pData), varDataLen(pData));

  SArray* pOrderInfo = taosArrayInit(3, sizeof(SBlockOrderInfo));
  SBlockOrderInfo order = {.order = TSDB_ORDER_ASC, .colIndex = 0};
  taosArrayPush(pOrderInfo, &order);

  blockDataSort(b, pOrderInfo, true);
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
    colDataAppend(p0, i, (const char*)&i, false);
    colDataAppend(p1, i, (const char*)&v, false);
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
  SSDataBlock* b = static_cast<SSDataBlock*>(taosMemoryCalloc(1, sizeof(SSDataBlock)));
  b->info.numOfCols = 2;
  b->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));

  int32_t numOfRows = 1000000;

  SColumnInfoData infoData = {0};
  infoData.info.bytes = 4;
  infoData.info.type = TSDB_DATA_TYPE_INT;
  infoData.info.colId = 1;

  infoData.pData = (char*) taosMemoryCalloc(numOfRows, infoData.info.bytes);
  infoData.nullbitmap = (char*) taosMemoryCalloc(1, sizeof(char) * (numOfRows/8));
  taosArrayPush(b->pDataBlock, &infoData);

  SColumnInfoData infoData1 = {0};
  infoData1.info.bytes = 40;
  infoData1.info.type = TSDB_DATA_TYPE_BINARY;
  infoData1.info.colId = 2;

  infoData1.varmeta.offset = (int32_t*) taosMemoryCalloc(numOfRows, sizeof(uint32_t));
  taosArrayPush(b->pDataBlock, &infoData1);

  char buf[41] = {0};
  char buf1[100] = {0};

  for(int32_t i = 0; i < numOfRows; ++i) {
    SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
    SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);

    int8_t v = i;
    colDataAppend(p0, i, (const char*)&i, false);

    sprintf(buf, "the number of row:%d", i);
    int32_t len = sprintf(buf1, buf, i);
    STR_TO_VARSTR(buf1, buf)
    colDataAppend(p1, i, buf1, false);
    b->info.rows++;

    memset(buf, 0, sizeof(buf));
    memset(buf1, 0, sizeof(buf1));
  }

  int32_t pageSize = 64 * 1024;

  int32_t startIndex= 0;
  int32_t stopIndex = 0;
  int32_t count = 1;
  while(1) {
    blockDataSplitRows(b, true, startIndex, &stopIndex, pageSize);
    printf("the %d split, from: %d to %d\n", count++, startIndex, stopIndex);

    if (stopIndex == numOfRows - 1) {
      break;
    }

    startIndex = stopIndex + 1;
  }
}

#pragma GCC diagnostic pop