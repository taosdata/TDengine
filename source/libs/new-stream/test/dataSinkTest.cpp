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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

#include <stdint.h>
#include "dataSink.h"
#include "tdatablock.h"

const int64_t baseTestTime = 1745142096000;

SSDataBlock* createTestBlock(int64_t timeOffset) {
  SSDataBlock* b = NULL;
  int32_t      code = createDataBlock(&b);

  int64_t timeStart = baseTestTime + timeOffset;

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, 8, 1);
  blockDataAppendColInfo(b, &infoData);

  SColumnInfoData infoData1 = createColumnInfoData(TSDB_DATA_TYPE_BINARY, 40, 2);
  blockDataAppendColInfo(b, &infoData1);
  blockDataEnsureCapacity(b, 100);

  char* str = "the value of: %d";
  char  buf[128] = {0};
  char  varbuf[128] = {0};

  int64_t ts = timeStart;
  for (int32_t i = 0; i < 100; ++i) {
    SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
    SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);
    ts = timeStart + i;

    if (i & 0x01) {
      int32_t len = sprintf(buf, str, i);
      STR_TO_VARSTR(varbuf, buf)
      colDataSetVal(p0, i, (const char*)&ts, false);
      colDataSetVal(p1, i, (const char*)varbuf, false);

      memset(varbuf, 0, sizeof(varbuf));
      memset(buf, 0, sizeof(buf));
    } else {
      colDataSetVal(p0, i, (const char*)&ts, false);
      colDataSetVal(p1, i, (const char*)varbuf, true);
    }

    b->info.rows++;
  }

  SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
  SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);

  printf("binary column length:%d\n", *(int32_t*)p1->pData);

  char* pData = colDataGetData(p1, 2);
  printf("the second row of binary:%s, length:%d\n", (char*)varDataVal(pData), varDataLen(pData));
  pData = colDataGetData(p1, 3);
  printf("the third row: %s, length:%d\n", (char*)varDataVal(pData), varDataLen(pData));
  return b;
}

bool compareBlock(SSDataBlock* b1, SSDataBlock* b2) {
  if (b1->info.rows != b2->info.rows) {
    return false;
  }

  for (int32_t i = 0; i < b1->info.rows; ++i) {
    SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b1->pDataBlock, 0);
    SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b2->pDataBlock, 0);

    if (*(int32_t*)colDataGetData(p0, i) != *(int32_t*)colDataGetData(p1, i)) {
      return false;
    }
  }
  SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b1->pDataBlock, 1);
  SColumnInfoData* p2 = (SColumnInfoData*)taosArrayGet(b2->pDataBlock, 1);

  char* pData = colDataGetData(p1, 3);
  printf("b1 the third row of binary:%s, length:%d\n", (char*)varDataVal(pData), varDataLen(pData));
  pData = colDataGetData(p1, 3);
  printf("b2 the third row of binary:%s, length:%d\n", (char*)varDataVal(pData), varDataLen(pData));
  return true;
}

TEST(dataSinkTest, fileInit) {
  int32_t code = initStreamDataSinkOnce();
  ASSERT_EQ(code, 0);
  code = initStreamDataSinkOnce();
  ASSERT_EQ(code, 0);
  destroyDataSinkManager2();
}

TEST(dataSinkTest, test_name) {
  SSDataBlock* pBlock = createTestBlock(0);
  ASSERT_NE(pBlock, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = 1;
  TSKEY wstart = 0;
  TSKEY wend = 100;
  void* pCache = NULL;
  int32_t code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 1);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST(dataSinkTest, putStreamDataCacheTest) {
  SSDataBlock* pBlock = createTestBlock(0);
  ASSERT_NE(pBlock, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = 1;
  TSKEY wstart = baseTestTime + 0;
  TSKEY wend = baseTestTime + 100;
  void* pCache = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, cleanMode, &pCache);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 1);
  ASSERT_EQ(code, 0);
  void* pIter = NULL;
  code = getStreamDataCache(pCache, groupID, wstart, wend, &pIter);
  ASSERT_EQ(code, 0);
  SSDataBlock* pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_EQ(pIter, nullptr);
  bool equal = compareBlock(pBlock, pBlock1);
  ASSERT_EQ(equal, true);
  blockDataDestroy(pBlock1);
  blockDataDestroy(pBlock);
  pBlock = createTestBlock(100);
  streamId = 1;
  taskId = 1;
  groupID = 2;
  cleanMode = 1;
  wstart = 100;
  wend = 200;
  pCache = NULL;
  code = initStreamDataCache(streamId, taskId, cleanMode, &pCache);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 1);
  ASSERT_EQ(code, 0);
  pIter = NULL;
  code = getStreamDataCache(pCache, groupID, wstart, wend, &pIter);
  ASSERT_EQ(code, 0);
  pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_EQ(pIter, nullptr);
  equal = compareBlock(pBlock, pBlock1);
  ASSERT_EQ(equal, true);
  blockDataDestroy(pBlock1);
  blockDataDestroy(pBlock);
  pBlock = createTestBlock(0); 
  streamId = 2;
  taskId = 1;
  groupID = 2;
  cleanMode = 1;
  wstart = 0;
  wend = 100;
  pCache = NULL;
  code = initStreamDataCache(streamId, taskId, cleanMode, &pCache);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 1);
  ASSERT_EQ(code, 0);
  pIter = NULL;
  code = getStreamDataCache(pCache, groupID, wstart, wend, &pIter);
  ASSERT_EQ(code, 0);
  pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_EQ(pIter, nullptr);
  equal = compareBlock(pBlock, pBlock1);
  ASSERT_EQ(equal, true);
  blockDataDestroy(pBlock1);
  destroyDataSinkManager2();
  blockDataDestroy(pBlock);
}

int main(int argc, char **argv) {
  taos_init();
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  taos_cleanup();
  return ret;
}
