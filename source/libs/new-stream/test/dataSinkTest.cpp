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
#include <stdint.h>
#include <unistd.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <random>
#include <thread>
#include <vector>

#include "dataSink.h"
#include "osSleep.h"
#include "stub.h"
#include "tdatablock.h"
#include "tglobal.h"

extern "C" int32_t moveMemCacheAllList();

// Macro to initialize DataSink at the beginning of each test
#define INIT_DATA_SINK() do { \
  int32_t initCode = initStreamDataSink(); \
  ASSERT_EQ(initCode, 0); \
} while(0)

// Macro to cleanup DataSink at the end of each test  
#define CLEANUP_DATA_SINK() do { \
  destroyDataSinkMgr(); \
} while(0)

const int64_t baseTestTime1 = 1745142096000;
const int64_t baseTestTime2 = 1745142097000;

int32_t gTestMode = 1;

SSDataBlock* createTestBlock(int64_t basetime, int64_t timeOffset) {
  SSDataBlock* b = NULL;
  int32_t      code = createDataBlock(&b);

  int64_t timeStart = basetime + timeOffset;

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, 8, 1);
  blockDataAppendColInfo(b, &infoData);

  SColumnInfoData infoData1 = createColumnInfoData(TSDB_DATA_TYPE_BINARY, 40, 2);
  blockDataAppendColInfo(b, &infoData1);
  blockDataEnsureCapacity(b, 100);

  const char* str = "the value of: %d";
  char        buf[128] = {0};
  char        varbuf[128] = {0};

  int64_t ts = basetime;
  for (int32_t i = 0; i < 100; ++i) {
    SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 0);
    SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b->pDataBlock, 1);
    ts = timeStart + i;

    if (i & 0x01) {
      int32_t len = sprintf(buf, str, timeOffset + i);
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

  //printf("binary column length:%d\n", *(int32_t*)p1->pData);

  char* pData = colDataGetData(p1, 2);
  //printf("the second row of binary:%s, length:%d\n", (char*)varDataVal(pData), varDataLen(pData));
  pData = colDataGetData(p1, 3);
  //printf("the third row: %s, length:%d\n", (char*)varDataVal(pData), varDataLen(pData));
  return b;
}

bool compareBlock(SSDataBlock* b1, SSDataBlock* b2) {
  if (b1->info.rows != b2->info.rows) {
    printf("compareBlock: rows not equal, b1:%" PRId64 ", b2:%" PRId64 "\n", b1->info.rows, b2->info.rows);
    return false;
  }

  for (int32_t i = 0; i < b1->info.rows; ++i) {
    SColumnInfoData* p0 = (SColumnInfoData*)taosArrayGet(b1->pDataBlock, 0);
    SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b2->pDataBlock, 0);

    if (*(int32_t*)colDataGetData(p0, i) != *(int32_t*)colDataGetData(p1, i)) {
      printf("compareBlock: timestamp not equal at row %d, b1:%" PRId64 ", b2:%" PRId64 "\n", i,
             *(int64_t*)colDataGetData(p0, i), *(int64_t*)colDataGetData(p1, i));
      return false;
    }
  }
  SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b1->pDataBlock, 1);
  SColumnInfoData* p2 = (SColumnInfoData*)taosArrayGet(b2->pDataBlock, 1);

  char* pData = colDataGetData(p1, 3);
  //printf("b1 the third row of binary:%s, length:%d\n", (char*)varDataVal(pData), varDataLen(pData));
  pData = colDataGetData(p1, 3);
  //printf("b2 the third row of binary:%s, length:%d\n", (char*)varDataVal(pData), varDataLen(pData));
  return true;
}

bool compareBlockRow(SSDataBlock* b1, SSDataBlock* b2, int32_t row1, int32_t row2) {
  for (int32_t i = 0; i < b1->pDataBlock->size; ++i) {
    SColumnInfoData* p1 = (SColumnInfoData*)taosArrayGet(b1->pDataBlock, i);
    SColumnInfoData* p2 = (SColumnInfoData*)taosArrayGet(b2->pDataBlock, i);

    if (i == 0) {
      if (*(int64_t*)colDataGetData(p1, row1) != *(int64_t*)colDataGetData(p2, row2)) {
        printf("compareBlockRow: timestamp not equal at row %d, b1:%" PRId64 ", b2:%" PRId64 "\n", row1,
               *(int64_t*)colDataGetData(p1, row1), *(int64_t*)colDataGetData(p2, row2));
        return false;
      }
      continue;
    } else {
      if (colDataIsNull(p1, b1->info.rows, row1, NULL) != colDataIsNull(p2, b2->info.rows, row2, NULL)) {
        printf("compareBlockRow: null status not equal at row %d, b1:%d, b2:%d\n", row1,
               colDataIsNull(p1, b1->info.rows, row1, NULL), colDataIsNull(p2, b2->info.rows, row2, NULL));
        return false;
      }
      if (colDataIsNull(p1, b1->info.rows, row1, NULL) == true) {
        continue;
      }
      if (IS_VAR_DATA_TYPE(p1->info.type)) {
        char* pData = colDataGetData(p1, row1);
        char* pData2 = colDataGetData(p2, row2);
        if (varDataLen(pData) != varDataLen(pData2)) {
          printf("compareBlockRow: var data length not equal at row %d, b1:%d, b2:%d\n", row1, varDataLen(pData),
                 varDataLen(pData2));
          return false;
        }
        if (memcmp(varDataVal(pData), varDataVal(pData2), varDataLen(pData)) != 0) {
          printf("compareBlockRow: var data not equal at row %d, b1:%s, b2:%s\n", row1, (char*)varDataVal(pData),
                 (char*)varDataVal(pData2));
          return false;
        }
      } else {
        if (*(int32_t*)colDataGetData(p1, row1) != *(int32_t*)colDataGetData(p2, row2)) {
          printf("compareBlockRow: data not equal at row %d, b1:%d, b2:%d\n", row1,
                 *(int32_t*)colDataGetData(p1, row1), *(int32_t*)colDataGetData(p2, row2));
          return false;
        }
      }
    }
  }
  return true;
}

TEST(dataSinkTest, fileInit) {
  int32_t code = initStreamDataSink();
  ASSERT_EQ(code, 0);
  destroyDataSinkMgr();
}

TEST(dataSinkTest, test_name) {
  INIT_DATA_SINK();
  SSDataBlock* pBlock = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pBlock, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = 1;
  TSKEY   wstart = 0;
  TSKEY   wend = 100;
  void*   pCache = NULL;
  int32_t code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 1);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  blockDataDestroy(pBlock);
}

TEST(dataSinkTest, putStreamDataCacheTest) {
  INIT_DATA_SINK();
  
  SSDataBlock* pBlock = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pBlock, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = DATA_CLEAN_IMMEDIATE;
  TSKEY   wstart = baseTestTime1 + 0;
  TSKEY   wend = baseTestTime1 + 100;
  void*   pCache = NULL;

  // Test invalid parameters
  int32_t code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 1);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 1, 0);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 1);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  code = moveStreamDataCache(pCache, groupID, wstart, wend, pBlock);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  code = moveStreamDataCache(NULL, groupID, wstart, wend, pBlock);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  code = getStreamDataCache(pCache, groupID, wend, wstart, NULL);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);

  // Test valid parameters
  code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 99);
  ASSERT_EQ(code, 0);
  void* pIter = NULL;

  // Test invalid parameters
  code = getStreamDataCache(pCache, groupID, wend, wstart, &pIter);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  code = getStreamDataCache(NULL, groupID, wstart, wend, &pIter);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  code = getStreamDataCache(pCache, groupID, wstart, wend, NULL);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);

  // Test valid parameters
  code = getStreamDataCache(pCache, groupID, wstart, wend, &pIter);
  ASSERT_EQ(code, 0);
  code = getNextStreamDataCache(&pIter, NULL);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  SSDataBlock* pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_EQ(pIter, nullptr);
  bool equal = compareBlock(pBlock, pBlock1);
  ASSERT_EQ(equal, true);
  blockDataDestroy(pBlock1);
  blockDataDestroy(pBlock);
  pBlock = createTestBlock(baseTestTime1, 100);
  streamId = 1;
  taskId = 1;
  groupID = 2;
  cleanMode = DATA_CLEAN_IMMEDIATE;
  wstart = baseTestTime1 + 100;
  wend = baseTestTime1 + 200;
  pCache = NULL;
  code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 99);
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
  pBlock = createTestBlock(baseTestTime1, 0);
  streamId = 2;
  taskId = 1;
  groupID = 2;
  cleanMode = DATA_CLEAN_IMMEDIATE;
  wstart = baseTestTime1 + 0;
  wend = baseTestTime1 + 100;
  pCache = NULL;
  code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 99);
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
  destroyDataSinkMgr();
  blockDataDestroy(pBlock);
}

TEST(dataSinkTest, getSlidingStreamData) {
  INIT_DATA_SINK(); 
  SSDataBlock* pBlock = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pBlock, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = DATA_CLEAN_EXPIRED;
  TSKEY   wstart = baseTestTime1 + 0;
  TSKEY   wend = baseTestTime1 + 100;
  void*   pCache = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);
  // Test invalid parameters, cleanMode is DATA_CLEAN_EXPIRED, cannot call moveStreamDataCache
  code = moveStreamDataCache(pCache, groupID, wstart, wend, pBlock);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 99);
  ASSERT_EQ(code, 0);
  blockDataDestroy(pBlock);

  pBlock = createTestBlock(baseTestTime1, 100);
  cleanMode = DATA_CLEAN_EXPIRED;
  wstart = baseTestTime1 + 100;
  wend = baseTestTime1 + 200;
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 99);
  ASSERT_EQ(code, 0);
  void* pIter = NULL;
  blockDataDestroy(pBlock);
  pBlock = NULL;
  code = getStreamDataCache(pCache, groupID, baseTestTime1 + 50, baseTestTime1 + 150, &pIter);
  ASSERT_EQ(code, 0);
  SSDataBlock* pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_NE(pIter, nullptr);
  int rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 50);
  blockDataDestroy(pBlock1);
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_EQ(pIter, nullptr);
  rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 51);
  blockDataDestroy(pBlock1);
  pBlock1 = NULL;

  pBlock = createTestBlock(baseTestTime1, 200);
  cleanMode = DATA_CLEAN_EXPIRED;
  wstart = baseTestTime1 + 200;
  wend = baseTestTime1 + 300;
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 99);
  ASSERT_EQ(code, 0);
  pIter = NULL;
  code = getStreamDataCache(pCache, groupID, baseTestTime1 + 150, baseTestTime1 + 249, &pIter);
  ASSERT_EQ(code, 0);
  pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_NE(pIter, nullptr);
  rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 50);
  blockDataDestroy(pBlock1);
  pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_EQ(pIter, nullptr);
  rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 50);
  ASSERT_EQ(compareBlockRow(pBlock, pBlock1, 0, 0), true);
  ASSERT_EQ(compareBlockRow(pBlock, pBlock1, 1, 1), true);

  blockDataDestroy(pBlock);
  blockDataDestroy(pBlock1);

  destroyDataSinkMgr();
}

TEST(dataSinkTest, moveStreamData) {
  INIT_DATA_SINK(); 
  SSDataBlock* pBlock = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pBlock, nullptr);
  int64_t streamId = 3;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = DATA_CLEAN_IMMEDIATE;
  TSKEY   wstart = baseTestTime1 + 0;
  TSKEY   wend = baseTestTime1 + 100;
  void*   pCache = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);
  code = moveStreamDataCache(pCache, groupID, wstart, wend, pBlock);
  ASSERT_EQ(code, 0);

  void* pIter = NULL;
  code = getStreamDataCache(pCache, groupID, baseTestTime1, baseTestTime1 + 100, &pIter);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pIter, nullptr);
  SSDataBlock* pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_EQ(pIter, nullptr);
  ASSERT_EQ(pBlock1, pBlock);

  blockDataDestroy(pBlock);

  code = getStreamDataCache(pCache, groupID, baseTestTime1, baseTestTime1 + 100, &pIter);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pIter, nullptr);
  pBlock1 = NULL;

  destroyDataSinkMgr();
}

TEST(dataSinkTest, cancelStreamDataCacheIterateTest) {
  INIT_DATA_SINK(); 
  int64_t streamId = 3;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = DATA_CLEAN_IMMEDIATE;
  void*   pCache = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);
  SSDataBlock* pBlock1 = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pBlock1, nullptr);
  TSKEY wstart = baseTestTime1 + 0;
  TSKEY wend = baseTestTime1 + 100;
  code = moveStreamDataCache(pCache, groupID, wstart, wend, pBlock1);
  ASSERT_EQ(code, 0);
  SSDataBlock* pBlock2 = createTestBlock(baseTestTime1, 100);
  ASSERT_NE(pBlock2, nullptr);
  wstart = baseTestTime1 + 100;
  wend = baseTestTime1 + 200;
  code = moveStreamDataCache(pCache, groupID, wstart, wend, pBlock2);
  ASSERT_EQ(code, 0);
  SSDataBlock* pBlock3 = createTestBlock(baseTestTime1, 200);
  ASSERT_NE(pBlock2, nullptr);
  wstart = baseTestTime1 + 200;
  wend = baseTestTime1 + 300;
  code = moveStreamDataCache(pCache, groupID, wstart, wend, pBlock2);
  ASSERT_EQ(code, 0);

  void* pIter = NULL;
  code = getStreamDataCache(pCache, groupID, baseTestTime1, baseTestTime1 + 99, &pIter);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pIter, nullptr);
  SSDataBlock* pBlock = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_NE(pIter, nullptr);
  ASSERT_EQ(pBlock1, pBlock);
  blockDataDestroy(pBlock);
  pBlock = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pBlock, nullptr);
  ASSERT_EQ(pIter, nullptr);

  code = getStreamDataCache(pCache, groupID, baseTestTime1 + 100, baseTestTime1 + 199, &pIter);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pIter, nullptr);
  code = getNextStreamDataCache(&pIter, &pBlock);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_NE(pIter, nullptr);
  ASSERT_EQ(pBlock2, pBlock);
  blockDataDestroy(pBlock);
  pBlock = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pBlock, nullptr);
  ASSERT_EQ(pIter, nullptr);

  code = getStreamDataCache(pCache, groupID, baseTestTime1 + 200, baseTestTime1 + 299, &pIter);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pIter, nullptr);

  cancelStreamDataCacheIterate(&pIter);


  blockDataDestroy(pBlock3); // pBlock3 has not moveout, destroyDataSinkMgr should destory it, but now has not finished.

  destroyDataSinkMgr();
}

TEST(dataSinkTest, putStreamDataRows) {
  INIT_DATA_SINK(); 
  SSDataBlock* pBlock = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pBlock, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = DATA_CLEAN_EXPIRED;
  TSKEY   wstart = baseTestTime1 + 0;
  TSKEY   wend = baseTestTime1 + 100;
  void*   pCache = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 29);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 30, 79);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 80, 99);
  ASSERT_EQ(code, 0);
  blockDataDestroy(pBlock);

  pBlock = createTestBlock(baseTestTime1, 100);
  cleanMode = DATA_CLEAN_EXPIRED;
  wstart = baseTestTime1 + 100;
  wend = baseTestTime1 + 200;
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 49);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 50, 99);
  ASSERT_EQ(code, 0);
  void* pIter = NULL;
  blockDataDestroy(pBlock);
  pBlock = NULL;
  code = getStreamDataCache(pCache, groupID, baseTestTime1 + 50, baseTestTime1 + 150, &pIter);
  ASSERT_EQ(code, 0);
  SSDataBlock* pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_NE(pIter, nullptr);
  int rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 30);
  blockDataDestroy(pBlock1);
  pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_NE(pIter, nullptr);
  rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 20);
  blockDataDestroy(pBlock1);
  pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_NE(pIter, nullptr);
  rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 50);
  blockDataDestroy(pBlock1);
  pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_EQ(pIter, nullptr);
  rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 1);
  blockDataDestroy(pBlock1);

  destroyDataSinkMgr();
}

TEST(dataSinkTest, allWriteToFileTest) {
  INIT_DATA_SINK(); 
  setDataSinkMaxMemSize(0);
  SSDataBlock* pBlock11 = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pBlock11, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = DATA_CLEAN_EXPIRED;
  TSKEY   wstart = baseTestTime1 + 0;
  TSKEY   wend = baseTestTime1 + 100;
  void*   pCache1 = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache1);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache1, groupID, wstart, wend, pBlock11, 0, 29);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache1, groupID, wstart, wend, pBlock11, 30, 79);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache1, groupID, wstart, wend, pBlock11, 80, 99);
  ASSERT_EQ(code, 0);

  SSDataBlock* pBlock21 = createTestBlock(baseTestTime2, 0);
  ASSERT_NE(pBlock21, nullptr);
  int64_t streamId2 = 1;
  int64_t taskId2 = 1;
  int64_t groupID2 = 2;
  int32_t cleanMode2 = DATA_CLEAN_EXPIRED;
  TSKEY   wstart2 = baseTestTime2 + 0;
  TSKEY   wend2 = baseTestTime2 + 100;
  void*   pCache2 = NULL;
  code = initStreamDataCache(streamId2, taskId2, 0, cleanMode2, 0, &pCache2);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache2, groupID2, wstart2, wend2, pBlock21, 0, 29);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache2, groupID2, wstart2, wend2, pBlock21, 30, 79);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache2, groupID2, wstart2, wend2, pBlock21, 80, 99);
  ASSERT_EQ(code, 0);

  SSDataBlock* pBlock12 = createTestBlock(baseTestTime1, 100);
  cleanMode = DATA_CLEAN_EXPIRED;
  wstart = baseTestTime1 + 100;
  wend = baseTestTime1 + 200;
  code = putStreamDataCache(pCache1, groupID, wstart, wend, pBlock12, 0, 49);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache1, groupID, wstart, wend, pBlock12, 50, 99);
  ASSERT_EQ(code, 0);

  void*   pIter1 = NULL;
  int64_t notExistGroupID = groupID + 100;
  code = getStreamDataCache(pCache1, notExistGroupID, baseTestTime1 + 50, baseTestTime1 + 150, &pIter1);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pIter1, nullptr);
  code = getStreamDataCache(pCache1, groupID, baseTestTime1 + 50, baseTestTime1 + 149, &pIter1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pIter1, nullptr);
  SSDataBlock* pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter1, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_NE(pIter1, nullptr);
  int rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 30);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 0, 50), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 1, 51), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 2, 52), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 29, 79), true);
  blockDataDestroy(pBlock1);

  void* pIter2 = NULL;
  code = getStreamDataCache(pCache2, groupID2, baseTestTime2 + 50, baseTestTime2 + 150, &pIter2);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pIter2, nullptr);
  SSDataBlock* pBlock2 = NULL;
  code = getNextStreamDataCache(&pIter2, &pBlock2);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock2, nullptr);
  ASSERT_NE(pIter2, nullptr);
  int rows2 = pBlock2->info.rows;
  ASSERT_EQ(rows2, 30);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 0, 50), true);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 1, 51), true);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 2, 52), true);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 29, 79), true);
  blockDataDestroy(pBlock2);

  code = getNextStreamDataCache(&pIter1, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 20);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 0, 80), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 1, 81), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 2, 82), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 19, 99), true);
  ASSERT_NE(pIter1, nullptr);
  blockDataDestroy(pBlock1);

  code = getNextStreamDataCache(&pIter2, &pBlock2);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock2, nullptr);
  ASSERT_EQ(pIter2, nullptr);
  rows = pBlock2->info.rows;
  ASSERT_EQ(rows, 20);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 0, 80), true);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 1, 81), true);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 2, 82), true);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 19, 99), true);
  blockDataDestroy(pBlock2);
  blockDataDestroy(pBlock21);

  code = getNextStreamDataCache(&pIter1, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_NE(pIter1, nullptr);
  rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 50);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock12, 0, 0), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock12, 1, 1), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock12, 2, 2), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock12, 49, 49), true);
  blockDataDestroy(pBlock1);
  pBlock1 = NULL;

  code = getNextStreamDataCache(&pIter1, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pBlock1, nullptr);
  ASSERT_EQ(pIter1, nullptr);

  blockDataDestroy(pBlock11);
  blockDataDestroy(pBlock12);

  destroyDataSinkMgr();
}

TEST(dataSinkTest, allWriteMultiStreamToFileTest) {
  INIT_DATA_SINK(); 
  setDataSinkMaxMemSize(0);
  SSDataBlock* pBlock11 = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pBlock11, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = DATA_CLEAN_EXPIRED;
  TSKEY   wstart = baseTestTime1 + 0;
  TSKEY   wend = baseTestTime1 + 100;
  void*   pCache1 = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache1);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache1, groupID, wstart, wend, pBlock11, 0, 29);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache1, groupID, wstart, wend, pBlock11, 30, 79);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache1, groupID, wstart, wend, pBlock11, 80, 99);
  ASSERT_EQ(code, 0);

  SSDataBlock* pBlock21 = createTestBlock(baseTestTime2, 0);
  ASSERT_NE(pBlock21, nullptr);
  int64_t streamId2 = 2;
  int64_t taskId2 = 1;
  int64_t groupID2 = 2;
  int32_t cleanMode2 = DATA_CLEAN_EXPIRED;
  TSKEY   wstart2 = baseTestTime2 + 0;
  TSKEY   wend2 = baseTestTime2 + 100;
  void*   pCache2 = NULL;
  code = initStreamDataCache(streamId2, taskId2, 0, cleanMode2, 0, &pCache2);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache2, groupID2, wstart2, wend2, pBlock21, 0, 29);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache2, groupID2, wstart2, wend2, pBlock21, 30, 79);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache2, groupID2, wstart2, wend2, pBlock21, 80, 99);
  ASSERT_EQ(code, 0);

  SSDataBlock* pBlock12 = createTestBlock(baseTestTime1, 100);
  cleanMode = DATA_CLEAN_EXPIRED;
  wstart = baseTestTime1 + 100;
  wend = baseTestTime1 + 200;
  code = putStreamDataCache(pCache1, groupID, wstart, wend, pBlock12, 0, 49);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache1, groupID, wstart, wend, pBlock12, 50, 99);
  ASSERT_EQ(code, 0);

  void*   pIter1 = NULL;
  int64_t notExistGroupID = groupID + 100;
  code = getStreamDataCache(pCache1, notExistGroupID, baseTestTime1 + 50, baseTestTime1 + 150, &pIter1);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pIter1, nullptr);
  code = getStreamDataCache(pCache1, groupID, baseTestTime1 + 50, baseTestTime1 + 149, &pIter1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pIter1, nullptr);
  SSDataBlock* pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter1, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_NE(pIter1, nullptr);
  int rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 30);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 0, 50), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 1, 51), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 2, 52), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 29, 79), true);
  blockDataDestroy(pBlock1);

  void* pIter2 = NULL;
  code = getStreamDataCache(pCache2, groupID2, baseTestTime2 + 50, baseTestTime2 + 150, &pIter2);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pIter2, nullptr);
  SSDataBlock* pBlock2 = NULL;
  code = getNextStreamDataCache(&pIter2, &pBlock2);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock2, nullptr);
  ASSERT_NE(pIter2, nullptr);
  int rows2 = pBlock2->info.rows;
  ASSERT_EQ(rows2, 30);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 0, 50), true);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 1, 51), true);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 2, 52), true);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 29, 79), true);
  blockDataDestroy(pBlock2);

  code = getNextStreamDataCache(&pIter1, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 20);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 0, 80), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 1, 81), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 2, 82), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 19, 99), true);
  ASSERT_NE(pIter1, nullptr);
  blockDataDestroy(pBlock1);

  code = getNextStreamDataCache(&pIter2, &pBlock2);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock2, nullptr);
  ASSERT_EQ(pIter2, nullptr);
  rows = pBlock2->info.rows;
  ASSERT_EQ(rows, 20);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 0, 80), true);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 1, 81), true);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 2, 82), true);
  ASSERT_EQ(compareBlockRow(pBlock2, pBlock21, 19, 99), true);
  blockDataDestroy(pBlock2);
  blockDataDestroy(pBlock21);

  destroyStreamDataCache(pCache2);

  code = getNextStreamDataCache(&pIter1, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_NE(pIter1, nullptr);
  rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 50);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock12, 0, 0), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock12, 1, 1), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock12, 2, 2), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock12, 49, 49), true);
  blockDataDestroy(pBlock1);
  pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter1, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pBlock1, nullptr);
  ASSERT_EQ(pIter1, nullptr);

  blockDataDestroy(pBlock11);
  blockDataDestroy(pBlock12);

  destroyStreamDataCache(pCache1);

  destroyDataSinkMgr();
}

TEST(dataSinkTest, testWriteFileSize) {
  INIT_DATA_SINK(); 
  SSDataBlock* pBlock = createTestBlock(baseTestTime1, 0);
  setDataSinkMaxMemSize(DS_MEM_SIZE_RESERVED + 1024 * 1024);
  int64_t streamId = 3;
  void*   pCache = NULL;
  int64_t taskId = 1;
  int32_t cleanMode = DATA_CLEAN_EXPIRED;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_NE(pBlock, nullptr);
  for (int32_t i = 0; i < 100000; i++) {
    int64_t groupID = i;
    TSKEY   wstart = baseTestTime1 + 0;
    TSKEY   wend = baseTestTime1 + 100;
    ASSERT_EQ(code, 0);
    code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 99);
    ASSERT_EQ(code, 0);
  }

  for (int32_t i = 0; i < 100000; i++) {
    int64_t groupID = i;
    TSKEY   wstart = baseTestTime1 + 0;
    TSKEY   wend = baseTestTime1 + 99;
    void*   pIter = NULL;
    code = getStreamDataCache(pCache, groupID, wstart, wend, &pIter);
    ASSERT_EQ(code, 0);
    ASSERT_NE(pIter, nullptr);
    SSDataBlock* pBlock1 = NULL;
    code = getNextStreamDataCache(&pIter, &pBlock1);
    ASSERT_EQ(code, 0);
    ASSERT_NE(pBlock1, nullptr);
    int rows = pBlock1->info.rows;
    ASSERT_EQ(rows, 100);
    bool equal = compareBlock(pBlock, pBlock1);
    ASSERT_EQ(equal, true);
    blockDataDestroy(pBlock1);
    pBlock1 = NULL;
    if (pIter != nullptr) {
      code = getNextStreamDataCache(&pIter, &pBlock1);
      ASSERT_EQ(code, 0);
      ASSERT_EQ(pBlock1, nullptr);
      ASSERT_EQ(pIter, nullptr);
    }
  }

  blockDataDestroy(pBlock);

  destroyStreamDataCache(pCache);
}

TEST(dataSinkTest, readReusedFileBlockOnce) {
  INIT_DATA_SINK();
  setDataSinkMaxMemSize(DS_MEM_SIZE_RESERVED + 1024 * 1024);

  int64_t streamId = 4;
  int64_t taskId = 1;
  void*   pCache = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, 0, DATA_CLEAN_EXPIRED, 0, &pCache);
  ASSERT_EQ(code, 0);

  SSDataBlock* pOldBlock1 = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pOldBlock1, nullptr);
  SSDataBlock* pOldBlock2 = createTestBlock(baseTestTime1 + 100, 0);
  ASSERT_NE(pOldBlock2, nullptr);
  TSKEY   oldStart = baseTestTime1;
  TSKEY   oldEnd = baseTestTime1 + 100;
  int64_t groupId = 1;
  code = putStreamDataCache(pCache, groupId, oldStart, oldEnd, pOldBlock1, 0, 99);
  ASSERT_EQ(code, 0);
  TSKEY oldStart2 = oldEnd;
  TSKEY oldEnd2 = oldStart2 + 100;
  code = putStreamDataCache(pCache, groupId, oldStart2, oldEnd2, pOldBlock2, 0, 99);
  ASSERT_EQ(code, 0);
  SSlidingTaskDSMgr* pTaskMgr = (SSlidingTaskDSMgr*)pCache;
  SSlidingGrpMgr**   ppGrpMgr = (SSlidingGrpMgr**)taosHashGet(pTaskMgr->pSlidingGrpList, &groupId, sizeof(groupId));
  ASSERT_NE(ppGrpMgr, nullptr);
  code = moveSlidingGrpMemCache(pTaskMgr, *ppGrpMgr);
  ASSERT_EQ(code, 0);

  void* pIter = NULL;
  code = getStreamDataCache(pCache, groupId, oldEnd2, oldEnd2 + 99, &pIter);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pIter, nullptr);
  SSDataBlock* pBlock = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pBlock, nullptr);
  ASSERT_EQ(pIter, nullptr);

  TSKEY        newStart = oldEnd2;
  TSKEY        newEnd = newStart + 100;
  SSDataBlock* pNewBlock = createTestBlock(newStart, 0);
  ASSERT_NE(pNewBlock, nullptr);
  code = putStreamDataCache(pCache, groupId, newStart, newEnd, pNewBlock, 0, 99);
  ASSERT_EQ(code, 0);
  code = moveSlidingGrpMemCache(pTaskMgr, *ppGrpMgr);
  ASSERT_EQ(code, 0);

  pIter = NULL;
  code = getStreamDataCache(pCache, groupId, newStart, newEnd - 1, &pIter);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pIter, nullptr);
  pBlock = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 100);
  ASSERT_EQ(compareBlock(pNewBlock, pBlock), true);
  blockDataDestroy(pBlock);

  pBlock = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pBlock, nullptr);
  ASSERT_EQ(pIter, nullptr);

  blockDataDestroy(pOldBlock1);
  blockDataDestroy(pOldBlock2);
  blockDataDestroy(pNewBlock);
  destroyStreamDataCache(pCache);
  destroyDataSinkMgr();
}

TEST(dataSinkTest, multiThreadGet) {
  INIT_DATA_SINK(); 
  const int producerCount = 1;
  const int consumerCount = 16;
  const int taskPerProducer = 10000;

  struct Task {
    int64_t      groupID;
    TSKEY        wstart;
    TSKEY        wend;
    SSDataBlock* pBlock;
  };

  // Each queue has its own mutex and condition_variabl
  std::vector<std::queue<Task>>        taskQueues(consumerCount);
  std::vector<std::mutex>              queueMutexes(consumerCount);
  std::vector<std::condition_variable> queueCVs(consumerCount);
  std::vector<bool>                    doneFlags(consumerCount, false);

  int32_t groups[100] = {0};
  // Initialize data cache
  setDataSinkMaxMemSize(DS_MEM_SIZE_RESERVED + 1024 * 1024);
  int64_t streamId = 100;
  int64_t taskId = 100;
  int32_t cleanMode = DATA_CLEAN_EXPIRED;
  void*   pCache = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);

  std::random_device                     rd;
  std::mt19937                           gen(rd());
  std::uniform_int_distribution<int64_t> dist(0, 99);

  // Producer thread
  auto producer = [&](int tid) {
    for (int i = 0; i < taskPerProducer; ++i) {
      int64_t      groupId = dist(gen);
      TSKEY        wstart = baseTestTime1 + groups[groupId] * 100;
      TSKEY        wend = baseTestTime1 + (++groups[groupId]) * 100;
      SSDataBlock* pBlock = createTestBlock(wstart, 0);
      code = putStreamDataCache(pCache, groupId, wstart, wend, pBlock, 0, 99);
      ASSERT_EQ(code, 0);

      // Assign to different queues according to groupId
      int queueIdx = groupId % consumerCount;
      {
        std::lock_guard<std::mutex> lock(queueMutexes[queueIdx]);
        taskQueues[queueIdx].push(Task{groupId, wstart, wend, pBlock});
      }
       queueCVs[queueIdx].notify_one();
    }
  };

  // Consumer thread
  auto consumer = [&](int idx) {
    while (true) {
      Task task;
      {
        std::unique_lock<std::mutex> lock(queueMutexes[idx]);
        queueCVs[idx].wait(lock, [&] { return !taskQueues[idx].empty() || doneFlags[idx]; });
        if (taskQueues[idx].empty() && doneFlags[idx]) break;
        if (taskQueues[idx].empty()) continue;
        task = taskQueues[idx].front();
        taskQueues[idx].pop();
      }
      //  Consume task: get data and check
      void*   pIter = NULL;
      int32_t code2 = getStreamDataCache(pCache, task.groupID, task.wstart, task.wend - 1, &pIter);
      ASSERT_EQ(code2, 0);
      ASSERT_NE(pIter, nullptr);
      SSDataBlock* pBlock1 = NULL;
      code2 = getNextStreamDataCache(&pIter, &pBlock1);
      ASSERT_EQ(code2, 0);
      ASSERT_NE(pBlock1, nullptr);
      ASSERT_EQ(pBlock1->info.rows, 100);
      bool equal = compareBlock(task.pBlock, pBlock1);
      ASSERT_EQ(equal, true);
      blockDataDestroy(pBlock1);
      pBlock1 = NULL;
      if (pIter != nullptr) {
        code2 = getNextStreamDataCache(&pIter, &pBlock1);
        ASSERT_EQ(code2, 0);
        ASSERT_EQ(pBlock1, nullptr);
        ASSERT_EQ(pIter, nullptr);
      }
      blockDataDestroy(task.pBlock);
      if (gTestMode == 1) {
        taosMsleep(10);  // This is done to create a backlog of data
      }
    }
  };

  // Start producer threads
  std::vector<std::thread> producers;
  for (int i = 0; i < producerCount; ++i) {
    producers.emplace_back(producer, i);
  }

  // Start consumer threads
  std::vector<std::thread> consumers;
  for (int i = 0; i < consumerCount; ++i) {
    consumers.emplace_back(consumer, i);
  }

  // Wait for producers to finish
  for (auto& t : producers) t.join();

  // Notify all consumers that this producer has finished
  for (int i = 0; i < consumerCount; ++i) {
    std::lock_guard<std::mutex> lock(queueMutexes[i]);
    doneFlags[i] = true;
    queueCVs[i].notify_all();
  }

  // Wait for consumers to finish
  for (auto& t : consumers) t.join();

  destroyStreamDataCache(pCache);
  destroyDataSinkMgr();
}

TEST(dataSinkTest, cleanSlidingStreamData) {
  INIT_DATA_SINK(); 
  SSDataBlock* pBlock = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pBlock, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = DATA_CLEAN_EXPIRED;
  TSKEY   wstart = baseTestTime1 + 0;
  TSKEY   wend = baseTestTime1 + 100;
  void*   pCache = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);

  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 99);
  ASSERT_EQ(code, 0);
  blockDataDestroy(pBlock);
  code = cleanStreamDataCache(pCache, groupID);
  ASSERT_EQ(code, 0);

  pBlock = createTestBlock(baseTestTime1, 100);
  cleanMode = DATA_CLEAN_EXPIRED;
  wstart = baseTestTime1 + 100;
  wend = baseTestTime1 + 200;
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 99);
  ASSERT_EQ(code, 0);
  void* pIter = NULL;
  blockDataDestroy(pBlock);
  pBlock = NULL;
  code = getStreamDataCache(pCache, groupID, baseTestTime1 + 50, baseTestTime1 + 150, &pIter);
  ASSERT_EQ(code, 0);
  SSDataBlock* pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_EQ(pIter, nullptr);
  int rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 51);
  blockDataDestroy(pBlock1);
  pBlock1 = NULL;

  destroyDataSinkMgr();
}

namespace {

Stub*   gCreateOneDataBlockStub = nullptr;
int32_t gCreateOneDataBlockCalls = 0;

int32_t countCreateOneDataBlock(const SSDataBlock* source, bool copyData, SSDataBlock** result) {
  ++gCreateOneDataBlockCalls;
  gCreateOneDataBlockStub->reset(createOneDataBlock);
  int32_t code = createOneDataBlock(source, copyData, result);
  gCreateOneDataBlockStub->set(createOneDataBlock, countCreateOneDataBlock);
  return code;
}

SSDataBlock* createScopedBlock(const std::vector<TSKEY>& timestamps, const std::vector<int32_t>& values) {
  if (timestamps.size() != values.size()) return nullptr;
  SSDataBlock* block = nullptr;
  if (createDataBlock(&block) != TSDB_CODE_SUCCESS) return nullptr;
  SColumnInfoData tsInfo = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 1);
  SColumnInfoData valueInfo = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
  if (blockDataAppendColInfo(block, &tsInfo) != TSDB_CODE_SUCCESS ||
      blockDataAppendColInfo(block, &valueInfo) != TSDB_CODE_SUCCESS ||
      blockDataEnsureCapacity(block, timestamps.size()) != TSDB_CODE_SUCCESS) {
    blockDataDestroy(block);
    return nullptr;
  }
  for (size_t i = 0; i < timestamps.size(); ++i) {
    auto* tsCol = static_cast<SColumnInfoData*>(taosArrayGet(block->pDataBlock, 0));
    auto* valueCol = static_cast<SColumnInfoData*>(taosArrayGet(block->pDataBlock, 1));
    if (colDataSetVal(tsCol, i, reinterpret_cast<const char*>(&timestamps[i]), false) != TSDB_CODE_SUCCESS ||
        colDataSetVal(valueCol, i, reinterpret_cast<const char*>(&values[i]), false) != TSDB_CODE_SUCCESS) {
      blockDataDestroy(block);
      return nullptr;
    }
    ++block->info.rows;
  }
  return block;
}

SStreamCacheScope makeScope(int64_t gid, TSKEY openingTs, int64_t discriminator) {
  SStreamCacheScope scope = {};
  scope.gid = gid;
  scope.lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
  const SScopeInstanceId id = {
      .layerIndex = 0,
      .triggerType = WINDOW_TYPE_INTERVAL,
      .openingTs = openingTs,
      .nativeDiscriminator = discriminator,
  };
  EXPECT_NE(nullptr, scope.lineage.pScopes);
  if (scope.lineage.pScopes != nullptr) EXPECT_NE(nullptr, taosArrayPush(scope.lineage.pScopes, &id));
  return scope;
}

void destroyScope(SStreamCacheScope* scope) {
  taosArrayDestroy(scope->lineage.pScopes);
  scope->lineage.pScopes = nullptr;
}

std::vector<int32_t> readScope(void* cache, const SStreamCacheScope& scope, TSKEY start = TSKEY_MIN,
                               TSKEY end = TSKEY_MAX) {
  std::vector<int32_t> values;
  void*                iter = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, getStreamDataCacheScoped(cache, &scope, start, end, &iter));
  while (iter != nullptr) {
    SSDataBlock* block = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, getNextStreamDataCache(&iter, &block));
    if (block == nullptr) continue;
    const auto* valueCol = static_cast<const SColumnInfoData*>(taosArrayGet(block->pDataBlock, 1));
    for (int32_t i = 0; i < block->info.rows; ++i) {
      values.push_back(*reinterpret_cast<const int32_t*>(colDataGetData(valueCol, i)));
    }
    blockDataDestroy(block);
  }
  return values;
}

struct ScopedReadResult {
  std::vector<int32_t> values;
  int32_t              blocks = 0;
};

ScopedReadResult readScopeWithBlockCount(void* cache, const SStreamCacheScope& scope) {
  ScopedReadResult result;
  void*            iter = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, getStreamDataCacheScoped(cache, &scope, TSKEY_MIN, TSKEY_MAX, &iter));
  while (iter != nullptr) {
    SSDataBlock* block = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, getNextStreamDataCache(&iter, &block));
    if (block == nullptr) continue;
    ++result.blocks;
    const auto* valueCol = static_cast<const SColumnInfoData*>(taosArrayGet(block->pDataBlock, 1));
    for (int32_t i = 0; i < block->info.rows; ++i) {
      result.values.push_back(*reinterpret_cast<const int32_t*>(colDataGetData(valueCol, i)));
    }
    blockDataDestroy(block);
  }
  return result;
}

class DataSinkMemoryLimitGuard {
 public:
  DataSinkMemoryLimitGuard() : original_(tsStreamBufferSizeBytes) {}
  ~DataSinkMemoryLimitGuard() { setDataSinkMaxMemSize(original_); }

 private:
  int64_t original_;
};

struct ScopedCacheWriteRecord {
  int64_t offset = 0;
  int64_t size = 0;
};

Stub*                               gScopedCacheWriteStub = nullptr;
std::vector<ScopedCacheWriteRecord> gScopedCacheWrites;

int64_t captureScopedCacheWrite(TdFilePtr pFile, const void* pBuffer, int64_t count, int64_t offset) {
  gScopedCacheWriteStub->reset(taosPWriteFile);
  int64_t written = taosPWriteFile(pFile, pBuffer, count, offset);
  gScopedCacheWriteStub->set(taosPWriteFile, captureScopedCacheWrite);
  ScopedCacheWriteRecord record;
  record.offset = offset;
  record.size = count;
  gScopedCacheWrites.push_back(record);
  return written;
}

int32_t putAndSpillScopedBlock(void* pCache, const SStreamCacheScope* pScope, const std::vector<TSKEY>& timestamps,
                               const std::vector<int32_t>& values, ScopedCacheWriteRecord* pRecord) {
  setDataSinkMaxMemSize(DS_MEM_SIZE_RESERVED + 1024 * 1024);
  SSDataBlock* pBlock = createScopedBlock(timestamps, values);
  if (pBlock == nullptr) return terrno;
  int32_t code = putStreamDataCacheScoped(pCache, pScope, timestamps.front(), timestamps.back(), pBlock, 0,
                                          static_cast<int32_t>(timestamps.size()) - 1);
  blockDataDestroy(pBlock);
  if (code != TSDB_CODE_SUCCESS) return code;

  setDataSinkMaxMemSize(DS_MEM_SIZE_RESERVED + 1);
  Stub writeStub;
  gScopedCacheWriteStub = &writeStub;
  gScopedCacheWrites.clear();
  writeStub.set(taosPWriteFile, captureScopedCacheWrite);
  code = moveMemCacheAllList();
  writeStub.reset(taosPWriteFile);
  gScopedCacheWriteStub = nullptr;
  if (code != TSDB_CODE_SUCCESS) return code;
  if (gScopedCacheWrites.size() != 1) return TSDB_CODE_INTERNAL_ERROR;
  *pRecord = gScopedCacheWrites.front();
  return TSDB_CODE_SUCCESS;
}

class ScopedDataSinkEnvironment {
 public:
  explicit ScopedDataSinkEnvironment(int32_t cleanMode = DATA_CLEAN_IMMEDIATE, int32_t tsSlotId = 0) {
    code = initStreamDataSink();
    streamId = ++nextId_;
    taskId = streamId + 1000;
    if (code == TSDB_CODE_SUCCESS) {
      code = initStreamDataCache(streamId, taskId, sessionId, cleanMode, tsSlotId, &cache);
    }
  }
  ~ScopedDataSinkEnvironment() { destroyDataSinkMgr(); }

  static int64_t nextId_;
  int32_t        code = TSDB_CODE_SUCCESS;
  int64_t        streamId = 0;
  int64_t        taskId = 0;
  int64_t        sessionId = 17;
  void*          cache = nullptr;
};

int64_t ScopedDataSinkEnvironment::nextId_ = 10000;

int32_t failArrayEnsureCapacity(SArray*, size_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return TSDB_CODE_OUT_OF_MEMORY;
}

std::mutex              leaseReleaseMutex;
std::condition_variable leaseReleaseCv;
int32_t volatile*       pausedLeaseCount = nullptr;
int8_t volatile*        observedRetired = nullptr;
bool                    leaseCountDecremented = false;
bool                    retireReturned = false;
std::atomic<bool>       retiredReadAfterRelease{false};

std::mutex              moveManagerMutex;
std::condition_variable moveManagerCv;
SSlidingTaskDSMgr*      movingManager = nullptr;
bool                    moveManagerEntered = false;
bool                    resumeManagerMove = false;

std::mutex              managerPinMutex;
std::condition_variable managerPinCv;
int32_t volatile*       pinnedManagerRefCount = nullptr;
bool                    managerPinEntered = false;
bool                    resumeManagerPin = false;

class ManagerMoveThreadGuard {
 public:
  explicit ManagerMoveThreadGuard(std::thread& thread) : thread_(thread) {}
  ~ManagerMoveThreadGuard() {
    {
      std::lock_guard<std::mutex> lock(managerPinMutex);
      resumeManagerPin = true;
    }
    managerPinCv.notify_all();
    {
      std::lock_guard<std::mutex> lock(moveManagerMutex);
      resumeManagerMove = true;
    }
    moveManagerCv.notify_all();
    if (thread_.joinable()) thread_.join();
  }

  ManagerMoveThreadGuard(const ManagerMoveThreadGuard&) = delete;
  ManagerMoveThreadGuard& operator=(const ManagerMoveThreadGuard&) = delete;

 private:
  std::thread& thread_;
};

class LeaseReleaseThreadGuard {
 public:
  explicit LeaseReleaseThreadGuard(std::thread& thread) : thread_(thread) {}
  ~LeaseReleaseThreadGuard() {
    {
      std::lock_guard<std::mutex> lock(leaseReleaseMutex);
      retireReturned = true;
    }
    leaseReleaseCv.notify_all();
    if (thread_.joinable()) thread_.join();
  }

  LeaseReleaseThreadGuard(const LeaseReleaseThreadGuard&) = delete;
  LeaseReleaseThreadGuard& operator=(const LeaseReleaseThreadGuard&) = delete;

 private:
  std::thread& thread_;
};

int32_t pauseManagerPin(int32_t volatile* ptr, int32_t value) {
  int32_t                      result = __sync_add_and_fetch(ptr, value);
  std::unique_lock<std::mutex> lock(managerPinMutex);
  if (ptr == pinnedManagerRefCount) {
    managerPinEntered = true;
    managerPinCv.notify_all();
    managerPinCv.wait(lock, [] { return resumeManagerPin; });
  }
  return result;
}

int32_t pauseSlidingManagerMove(SSlidingTaskDSMgr* pManager) {
  std::unique_lock<std::mutex> lock(moveManagerMutex);
  movingManager = pManager;
  moveManagerEntered = true;
  moveManagerCv.notify_all();
  moveManagerCv.wait(lock, [] { return resumeManagerMove; });
  return TSDB_CODE_SUCCESS;
}

int32_t pauseLeaseCountDecrement(int32_t volatile* ptr, int32_t value) {
  std::unique_lock<std::mutex> lock(leaseReleaseMutex);
  int32_t                      result = *ptr - value;
  *ptr = result;
  if (ptr == pausedLeaseCount && !leaseCountDecremented) {
    leaseCountDecremented = true;
    leaseReleaseCv.notify_all();
    leaseReleaseCv.wait(lock, [] { return retireReturned; });
  }
  return result;
}

int8_t observeRetiredRead(int8_t volatile* ptr) {
  std::lock_guard<std::mutex> lock(leaseReleaseMutex);
  if (ptr == observedRetired) {
    retiredReadAfterRelease.store(true);
    return false;
  }
  return *ptr;
}

TEST(dataSinkTest, ScopedLineageSeparatesEqualRanges) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamCacheScope scopeA = makeScope(42, 1000, 1);
  SStreamCacheScope scopeB = makeScope(42, 1000, 2);
  SSDataBlock*      blockA = createScopedBlock({1000, 1001}, {10, 11});
  SSDataBlock*      blockB = createScopedBlock({1000, 1001, 1002}, {20, 21, 22});
  ASSERT_NE(nullptr, blockA);
  ASSERT_NE(nullptr, blockB);
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scopeA, 1000, 1999, blockA, 0, 1));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scopeB, 1000, 1999, blockB, 0, 2));
  EXPECT_EQ((std::vector<int32_t>{10, 11}), readScope(env.cache, scopeA));
  EXPECT_EQ((std::vector<int32_t>{20, 21, 22}), readScope(env.cache, scopeB));
  EXPECT_EQ(TSDB_CODE_SUCCESS, cleanStreamDataCacheScope(env.cache, &scopeA));
  EXPECT_EQ((std::vector<int32_t>{20, 21, 22}), readScope(env.cache, scopeB));
  blockDataDestroy(blockA);
  blockDataDestroy(blockB);
  destroyScope(&scopeA);
  destroyScope(&scopeB);
}

TEST(dataSinkTest, ScopedOverlapSharesRowsWithinLineage) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamCacheScope scope = makeScope(42, 1000, 1);
  SSDataBlock*      block = createScopedBlock({1000, 1001}, {10, 11});
  ASSERT_NE(nullptr, block);
  SStreamDataCacheWriteBatch* batch = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, beginStreamDataCacheWriteBatch(env.cache, &batch));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stageStreamDataCacheRowScoped(batch, &scope, block, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stageStreamDataCacheRowScoped(batch, &scope, block, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stageStreamDataCacheRowScoped(batch, &scope, block, 1));
  commitStreamDataCacheWriteBatch(&batch);
  EXPECT_EQ(nullptr, batch);
  EXPECT_EQ((std::vector<int32_t>{10, 11}), readScope(env.cache, scope));
  blockDataDestroy(block);
  destroyScope(&scope);
}

TEST(dataSinkTest, ScopedWriteBatchRewritesExistingTailAtMostOnce) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamCacheScope    scope = makeScope(42, 1000, 1);
  SSDataBlock*         existing = createScopedBlock({999}, {9});
  std::vector<TSKEY>   timestamps;
  std::vector<int32_t> values;
  for (int32_t i = 0; i < 64; ++i) {
    timestamps.push_back(1000 + i);
    values.push_back(i);
  }
  SSDataBlock* batchBlock = createScopedBlock(timestamps, values);
  ASSERT_NE(nullptr, existing);
  ASSERT_NE(nullptr, batchBlock);
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scope, 999, 999, existing, 0, 0));

  SStreamDataCacheWriteBatch* batch = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, beginStreamDataCacheWriteBatch(env.cache, &batch));
  for (int32_t i = 0; i < batchBlock->info.rows; ++i) {
    ASSERT_EQ(TSDB_CODE_SUCCESS, stageStreamDataCacheRowScoped(batch, &scope, batchBlock, i));
  }

  Stub copyCounter;
  gCreateOneDataBlockStub = &copyCounter;
  gCreateOneDataBlockCalls = 0;
  copyCounter.set(createOneDataBlock, countCreateOneDataBlock);
  commitStreamDataCacheWriteBatch(&batch);
  copyCounter.reset(createOneDataBlock);
  gCreateOneDataBlockStub = nullptr;
  EXPECT_LE(gCreateOneDataBlockCalls, 1);
  EXPECT_EQ(65, readScope(env.cache, scope).size());

  blockDataDestroy(existing);
  blockDataDestroy(batchBlock);
  destroyScope(&scope);
}

TEST(dataSinkTest, ScopedIteratorLoadsOneBlockPerNextCall) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamCacheScope scope = makeScope(42, 1000, 1);
  SSDataBlock*      first = createScopedBlock({1000}, {10});
  SSDataBlock*      second = createScopedBlock({2000}, {20});
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  second->info.version = 2;
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scope, 1000, 1000, first, 0, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scope, 2000, 2000, second, 0, 0));

  Stub copyCounter;
  gCreateOneDataBlockStub = &copyCounter;
  gCreateOneDataBlockCalls = 0;
  copyCounter.set(createOneDataBlock, countCreateOneDataBlock);
  void* iter = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, getStreamDataCacheScoped(env.cache, &scope, TSKEY_MIN, TSKEY_MAX, &iter));
  ASSERT_NE(nullptr, iter);
  EXPECT_EQ(0, gCreateOneDataBlockCalls);
  SSDataBlock* result = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, getNextStreamDataCache(&iter, &result));
  ASSERT_NE(nullptr, result);
  EXPECT_EQ(2, gCreateOneDataBlockCalls);
  blockDataDestroy(result);
  result = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, getNextStreamDataCache(&iter, &result));
  ASSERT_NE(nullptr, result);
  EXPECT_EQ(4, gCreateOneDataBlockCalls);
  blockDataDestroy(result);
  copyCounter.reset(createOneDataBlock);
  gCreateOneDataBlockStub = nullptr;

  blockDataDestroy(first);
  blockDataDestroy(second);
  destroyScope(&scope);
}

TEST(dataSinkTest, ScopedProjectedRowsKeepRawIdentityAndTimestamp) {
  ScopedDataSinkEnvironment env(DATA_CLEAN_IMMEDIATE, 2);
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamCacheScope scope = makeScope(42, 3000, 1);

  SSDataBlock* source = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&source));
  SColumnInfoData decoyInfo = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 9);
  SColumnInfoData valueInfo = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  SColumnInfoData tsInfo = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 0);
  ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(source, &decoyInfo));
  ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(source, &valueInfo));
  ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(source, &tsInfo));
  ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataEnsureCapacity(source, 1));
  const TSKEY   decoy = 9999;
  const int32_t value = 17;
  const TSKEY   timestamp = 3000;
  ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(source->pDataBlock, 0)), 0,
                                             reinterpret_cast<const char*>(&decoy), false));
  ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(source->pDataBlock, 1)), 0,
                                             reinterpret_cast<const char*>(&value), false));
  ASSERT_EQ(TSDB_CODE_SUCCESS, colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(source->pDataBlock, 2)), 0,
                                             reinterpret_cast<const char*>(&timestamp), false));
  source->info.rows = 1;
  source->info.id.uid = 7001;

  SSDataBlock* payloadTemplate = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&payloadTemplate));
  ASSERT_EQ(TSDB_CODE_SUCCESS, blockDataAppendColInfo(payloadTemplate, &tsInfo));
  SArray* projection = taosArrayInit(1, sizeof(SStreamDataCacheColumnProjection));
  ASSERT_NE(nullptr, projection);
  const SStreamDataCacheColumnProjection tsProjection = {.sourceSlotId = 2, .targetSlotId = 0};
  ASSERT_NE(nullptr, taosArrayPush(projection, &tsProjection));

  SStreamDataCacheWriteBatch* batch = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, beginStreamDataCacheWriteBatch(env.cache, &batch));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            stageStreamDataCacheProjectedRowScoped(batch, &scope, source, 0, payloadTemplate, projection));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            stageStreamDataCacheProjectedRowScoped(batch, &scope, source, 0, payloadTemplate, projection));
  commitStreamDataCacheWriteBatch(&batch);

  void* iter = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, getStreamDataCacheScoped(env.cache, &scope, timestamp, timestamp, &iter));
  ASSERT_NE(nullptr, iter);
  SSDataBlock* result = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, getNextStreamDataCache(&iter, &result));
  ASSERT_NE(nullptr, result);
  ASSERT_EQ(1, result->info.rows);
  ASSERT_EQ(1, taosArrayGetSize(result->pDataBlock));
  EXPECT_EQ(source->info.id.uid, result->info.id.uid);
  const auto* resultTs = static_cast<const SColumnInfoData*>(taosArrayGet(result->pDataBlock, 0));
  ASSERT_NE(nullptr, resultTs);
  EXPECT_EQ(TSDB_DATA_TYPE_TIMESTAMP, resultTs->info.type);
  EXPECT_EQ(0, resultTs->info.colId);
  EXPECT_EQ(timestamp, *reinterpret_cast<const TSKEY*>(colDataGetData(resultTs, 0)));
  blockDataDestroy(result);
  result = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, getNextStreamDataCache(&iter, &result));
  EXPECT_EQ(nullptr, result);
  EXPECT_EQ(nullptr, iter);

  taosArrayDestroy(projection);
  blockDataDestroy(payloadTemplate);
  blockDataDestroy(source);
  destroyScope(&scope);
}

TEST(dataSinkTest, ScopedAcceptedRowsExistWithoutCandidate) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamCacheScope scope = makeScope(42, 1000, 1);
  SSDataBlock*      block = createScopedBlock({1100, 1200}, {10, 11});
  ASSERT_NE(nullptr, block);
  SStreamDataCacheWriteBatch* batch = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, beginStreamDataCacheWriteBatch(env.cache, &batch));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stageStreamDataCacheRowScoped(batch, &scope, block, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stageStreamDataCacheRowScoped(batch, &scope, block, 1));
  commitStreamDataCacheWriteBatch(&batch);
  EXPECT_EQ((std::vector<int32_t>{10, 11}), readScope(env.cache, scope, 1000, 1999));
  blockDataDestroy(block);
  destroyScope(&scope);
}

TEST(dataSinkTest, ScopedSuccessiveCommitsPreserveEventOrder) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamCacheScope scope = makeScope(42, 1000, 1);
  SSDataBlock*      first = createScopedBlock({10}, {10});
  SSDataBlock*      second = createScopedBlock({20}, {20});
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);

  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scope, 10, 10, first, 0, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scope, 20, 20, second, 0, 0));
  blockDataDestroy(first);
  blockDataDestroy(second);

  EXPECT_EQ((std::vector<int32_t>{10, 20}), readScope(env.cache, scope));
  destroyScope(&scope);
}

TEST(dataSinkTest, ScopedCacheAccountsCompactsAndSpillsSuccessiveRows) {
  DataSinkMemoryLimitGuard limitGuard;
  setDataSinkMaxMemSize(DS_MEM_SIZE_RESERVED + 1024 * 1024);
  ScopedDataSinkEnvironment env(DATA_CLEAN_IMMEDIATE);
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  const int64_t        initialUsed = g_pDataSinkManager.usedMemSize;
  SStreamCacheScope    scope = makeScope(42, 1000, 1);
  std::vector<int32_t> expected;

  for (int32_t i = 0; i < 64; ++i) {
    const TSKEY  ts = 1000 + i;
    SSDataBlock* block = createScopedBlock({ts}, {i});
    ASSERT_NE(nullptr, block);
    ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scope, ts, ts, block, 0, 0));
    blockDataDestroy(block);
    expected.push_back(i);
  }

  const int64_t usedBeforeSpill = g_pDataSinkManager.usedMemSize;
  EXPECT_GT(usedBeforeSpill, initialUsed);
  ScopedReadResult inMemory = readScopeWithBlockCount(env.cache, scope);
  EXPECT_EQ(expected, inMemory.values);
  EXPECT_LT(inMemory.blocks, static_cast<int32_t>(expected.size()));

  setDataSinkMaxMemSize(DS_MEM_SIZE_RESERVED + 1);
  ASSERT_EQ(TSDB_CODE_SUCCESS, moveMemCacheAllList());
  EXPECT_LT(g_pDataSinkManager.usedMemSize, usedBeforeSpill);
  ScopedReadResult spilled = readScopeWithBlockCount(env.cache, scope);
  EXPECT_EQ(expected, spilled.values);

  for (int32_t i = 64; i < 128; ++i) {
    const TSKEY  ts = 1000 + i;
    SSDataBlock* block = createScopedBlock({ts}, {i});
    ASSERT_NE(nullptr, block);
    ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scope, ts, ts, block, 0, 0));
    blockDataDestroy(block);
    expected.push_back(i);
  }
  ScopedReadResult appendedOnDisk = readScopeWithBlockCount(env.cache, scope);
  EXPECT_EQ(expected, appendedOnDisk.values);
  EXPECT_LT(appendedOnDisk.blocks, 4);

  ASSERT_EQ(TSDB_CODE_SUCCESS, cleanStreamDataCacheScope(env.cache, &scope));
  EXPECT_EQ(initialUsed, g_pDataSinkManager.usedMemSize);
  destroyScope(&scope);
}

TEST(dataSinkTest, ScopedSpillCoalescesAdjacentFreeRanges) {
  DataSinkMemoryLimitGuard  limitGuard;
  ScopedDataSinkEnvironment env(DATA_CLEAN_IMMEDIATE);
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);

  SStreamCacheScope scopes[5] = {
      makeScope(42, 1000, 1), makeScope(42, 1000, 2), makeScope(42, 1000, 3),
      makeScope(42, 1000, 4), makeScope(42, 1000, 5),
  };
  ScopedCacheWriteRecord writes[5] = {};
  for (int32_t i = 0; i < 4; ++i) {
    ASSERT_EQ(TSDB_CODE_SUCCESS, putAndSpillScopedBlock(env.cache, &scopes[i], {1000 + i}, {10 + i}, &writes[i]));
  }
  ASSERT_EQ(writes[1].offset + writes[1].size, writes[2].offset);
  ASSERT_EQ(TSDB_CODE_SUCCESS, cleanStreamDataCacheScope(env.cache, &scopes[1]));
  ASSERT_EQ(TSDB_CODE_SUCCESS, cleanStreamDataCacheScope(env.cache, &scopes[2]));

  ASSERT_EQ(TSDB_CODE_SUCCESS, putAndSpillScopedBlock(env.cache, &scopes[4], {2000, 2001}, {20, 21}, &writes[4]));
  ASSERT_GT(writes[4].size, writes[1].size);
  ASSERT_LE(writes[4].size, writes[1].size + writes[2].size);
  EXPECT_EQ(writes[1].offset, writes[4].offset);
  EXPECT_EQ((std::vector<int32_t>{20, 21}), readScope(env.cache, scopes[4]));

  for (SStreamCacheScope& scope : scopes) destroyScope(&scope);
}

TEST(dataSinkTest, ScopedSpillReclaimsTheLogicalFileTail) {
  DataSinkMemoryLimitGuard  limitGuard;
  ScopedDataSinkEnvironment env(DATA_CLEAN_IMMEDIATE);
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);

  SStreamCacheScope scopes[4] = {
      makeScope(42, 1000, 1),
      makeScope(42, 1000, 2),
      makeScope(42, 1000, 3),
      makeScope(42, 1000, 4),
  };
  ScopedCacheWriteRecord writes[4] = {};
  for (int32_t i = 0; i < 3; ++i) {
    ASSERT_EQ(TSDB_CODE_SUCCESS, putAndSpillScopedBlock(env.cache, &scopes[i], {1000 + i}, {10 + i}, &writes[i]));
  }
  ASSERT_EQ(TSDB_CODE_SUCCESS, cleanStreamDataCacheScope(env.cache, &scopes[2]));

  ASSERT_EQ(TSDB_CODE_SUCCESS, putAndSpillScopedBlock(env.cache, &scopes[3], {2000, 2001}, {20, 21}, &writes[3]));
  ASSERT_GT(writes[3].size, writes[2].size);
  EXPECT_EQ(writes[2].offset, writes[3].offset);
  EXPECT_EQ((std::vector<int32_t>{20, 21}), readScope(env.cache, scopes[3]));

  for (SStreamCacheScope& scope : scopes) destroyScope(&scope);
}

TEST(dataSinkTest, ScopedCleanupThenAppendKeepsOtherLineage) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamCacheScope scopeA = makeScope(42, 1000, 1);
  SStreamCacheScope scopeB = makeScope(42, 1000, 2);
  SSDataBlock*      first = createScopedBlock({10}, {10});
  SSDataBlock*      removed = createScopedBlock({20}, {20});
  SSDataBlock*      replacement = createScopedBlock({30}, {30});
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, removed);
  ASSERT_NE(nullptr, replacement);

  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scopeA, 10, 10, first, 0, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scopeB, 20, 20, removed, 0, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, cleanStreamDataCacheScope(env.cache, &scopeB));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scopeB, 30, 30, replacement, 0, 0));

  EXPECT_EQ((std::vector<int32_t>{10}), readScope(env.cache, scopeA));
  EXPECT_EQ((std::vector<int32_t>{30}), readScope(env.cache, scopeB));
  blockDataDestroy(first);
  blockDataDestroy(removed);
  blockDataDestroy(replacement);
  destroyScope(&scopeA);
  destroyScope(&scopeB);
}

TEST(dataSinkTest, ScopedGroupCleanupThenAppendRestoresTail) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamCacheScope scopeA = makeScope(42, 1000, 1);
  SStreamCacheScope scopeB = makeScope(42, 1000, 2);
  SSDataBlock*      removed = createScopedBlock({10}, {10});
  SSDataBlock*      replacement = createScopedBlock({20}, {20});
  ASSERT_NE(nullptr, removed);
  ASSERT_NE(nullptr, replacement);

  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scopeA, 10, 10, removed, 0, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scopeB, 10, 10, removed, 0, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, cleanStreamDataCacheGroup(env.cache, 42));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scopeA, 20, 20, replacement, 0, 0));

  EXPECT_EQ((std::vector<int32_t>{20}), readScope(env.cache, scopeA));
  EXPECT_TRUE(readScope(env.cache, scopeB).empty());
  blockDataDestroy(removed);
  blockDataDestroy(replacement);
  destroyScope(&scopeA);
  destroyScope(&scopeB);
}

TEST(dataSinkTest, ScopedWriteBatchAbortIsInvisible) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamCacheScope scope = makeScope(42, 1000, 1);
  SSDataBlock*      block = createScopedBlock({1000, 1001}, {10, 11});
  ASSERT_NE(nullptr, block);
  SStreamDataCacheWriteBatch* batch = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, beginStreamDataCacheWriteBatch(env.cache, &batch));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stageStreamDataCacheRowScoped(batch, &scope, block, 0));
  {
    Stub allocationFailure;
    allocationFailure.set(taosArrayEnsureCap, failArrayEnsureCapacity);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, stageStreamDataCacheRowScoped(batch, &scope, block, 1));
  }
  abortStreamDataCacheWriteBatch(&batch);
  EXPECT_TRUE(readScope(env.cache, scope).empty());
  ASSERT_EQ(TSDB_CODE_SUCCESS, beginStreamDataCacheWriteBatch(env.cache, &batch));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stageStreamDataCacheRowScoped(batch, &scope, block, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, stageStreamDataCacheRowScoped(batch, &scope, block, 1));
  commitStreamDataCacheWriteBatch(&batch);
  EXPECT_EQ((std::vector<int32_t>{10, 11}), readScope(env.cache, scope));
  blockDataDestroy(block);
  destroyScope(&scope);
}

TEST(dataSinkTest, ScopedGroupCleanupRemovesEveryLineage) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamCacheScope scopeA = makeScope(42, 1000, 1);
  SStreamCacheScope scopeB = makeScope(42, 1000, 2);
  SSDataBlock*      block = createScopedBlock({1000}, {10});
  ASSERT_NE(nullptr, block);
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scopeA, 1000, 1999, block, 0, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scopeB, 1000, 1999, block, 0, 0));
  ASSERT_EQ(TSDB_CODE_SUCCESS, cleanStreamDataCacheGroup(env.cache, 42));
  EXPECT_TRUE(readScope(env.cache, scopeA).empty());
  EXPECT_TRUE(readScope(env.cache, scopeB).empty());
  blockDataDestroy(block);
  destroyScope(&scopeA);
  destroyScope(&scopeB);
}

TEST(dataSinkTest, DetachedManagerDoesNotCollideWithLiveRegistration) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  void* detached = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            createDetachedStreamDataCache(env.streamId, env.taskId, env.sessionId, DATA_CLEAN_IMMEDIATE, 0, &detached));
  EXPECT_NE(nullptr, detached);
  EXPECT_NE(env.cache, detached);
  retireStreamDataCache(&detached);
  EXPECT_EQ(nullptr, detached);
}

TEST(dataSinkTest, RegistrationSwapKeepsOldIteratorAlive) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamCacheScope scope = makeScope(42, 1000, 1);
  SSDataBlock*      oldBlock = createScopedBlock({1000}, {10});
  ASSERT_NE(nullptr, oldBlock);
  ASSERT_EQ(TSDB_CODE_SUCCESS, putStreamDataCacheScoped(env.cache, &scope, 1000, 1000, oldBlock, 0, 0));
  SStreamDataCacheLease* oldLease = nullptr;
  void*                  acquired = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            acquireStreamDataCacheLease(env.streamId, env.taskId, env.sessionId, &oldLease, &acquired));
  ASSERT_EQ(env.cache, acquired);
  void* oldIter = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, getStreamDataCacheScoped(acquired, &scope, 1000, 1000, &oldIter));
  ASSERT_NE(nullptr, oldIter);
  void* replacement = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDetachedStreamDataCache(env.streamId, env.taskId, env.sessionId,
                                                             DATA_CLEAN_IMMEDIATE, 0, &replacement));
  void* retired = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, replaceStreamDataCacheRegistration(env.streamId, env.taskId, env.sessionId, env.cache,
                                                                  replacement, &retired));
  EXPECT_EQ(env.cache, retired);
  retireStreamDataCache(&retired);
  SSDataBlock* result = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, getNextStreamDataCache(&oldIter, &result));
  ASSERT_NE(nullptr, result);
  const auto* values = static_cast<const SColumnInfoData*>(taosArrayGet(result->pDataBlock, 1));
  EXPECT_EQ(10, *reinterpret_cast<const int32_t*>(colDataGetData(values, 0)));
  blockDataDestroy(result);
  releaseStreamDataCacheLease(&oldLease);
  env.cache = replacement;
  blockDataDestroy(oldBlock);
  destroyScope(&scope);
}

TEST(dataSinkTest, RegistrationMovePinsSelectedGeneration) {
  ScopedDataSinkEnvironment env(DATA_CLEAN_EXPIRED);
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  auto* oldManager = static_cast<SSlidingTaskDSMgr*>(env.cache);
  void* replacement = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDetachedStreamDataCache(env.streamId, env.taskId, env.sessionId,
                                                             DATA_CLEAN_EXPIRED, 0, &replacement));

  {
    std::lock_guard<std::mutex> lock(moveManagerMutex);
    movingManager = nullptr;
    moveManagerEntered = false;
    resumeManagerMove = false;
  }
  {
    std::lock_guard<std::mutex> lock(managerPinMutex);
    pinnedManagerRefCount = &oldManager->refCount;
    managerPinEntered = false;
    resumeManagerPin = false;
  }
  Stub movePause;
  movePause.set(atomic_add_fetch_32, pauseManagerPin);
  movePause.set(moveSlidingTaskMemCache, pauseSlidingManagerMove);
  std::atomic<int32_t>   moveCode{TSDB_CODE_INTERNAL_ERROR};
  std::thread            mover([&] { moveCode.store(moveMemCacheAllList()); });
  ManagerMoveThreadGuard moverGuard(mover);
  {
    std::unique_lock<std::mutex> lock(managerPinMutex);
    ASSERT_TRUE(managerPinCv.wait_for(lock, std::chrono::seconds(5), [] { return managerPinEntered; }));
  }

  int32_t tryLockCode = taosThreadMutexTryLock(&g_pDataSinkManager.registrationLock);
  EXPECT_EQ(EBUSY, tryLockCode);
  if (tryLockCode == TSDB_CODE_SUCCESS) taosThreadMutexUnlock(&g_pDataSinkManager.registrationLock);
  {
    std::lock_guard<std::mutex> lock(managerPinMutex);
    resumeManagerPin = true;
  }
  managerPinCv.notify_all();
  {
    std::unique_lock<std::mutex> lock(moveManagerMutex);
    ASSERT_TRUE(moveManagerCv.wait_for(lock, std::chrono::seconds(5), [] { return moveManagerEntered; }));
  }

  void* retired = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, replaceStreamDataCacheRegistration(env.streamId, env.taskId, env.sessionId, env.cache,
                                                                  replacement, &retired));
  EXPECT_EQ(oldManager, movingManager);
  retireStreamDataCache(&retired);
  EXPECT_EQ(nullptr, retired);
  EXPECT_EQ(1, oldManager->refCount);
  {
    std::lock_guard<std::mutex> lock(moveManagerMutex);
    resumeManagerMove = true;
  }
  moveManagerCv.notify_all();
  mover.join();

  EXPECT_EQ(TSDB_CODE_SUCCESS, moveCode.load());
  env.cache = replacement;
}

TEST(dataSinkTest, RetiredManagerFreesAfterLastLease) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamDataCacheLease* lease = nullptr;
  void*                  acquired = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, acquireStreamDataCacheLease(env.streamId, env.taskId, env.sessionId, &lease, &acquired));
  void* replacement = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDetachedStreamDataCache(env.streamId, env.taskId, env.sessionId,
                                                             DATA_CLEAN_IMMEDIATE, 0, &replacement));
  void* retired = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, replaceStreamDataCacheRegistration(env.streamId, env.taskId, env.sessionId, env.cache,
                                                                  replacement, &retired));
  retireStreamDataCache(&retired);
  EXPECT_EQ(nullptr, retired);
  releaseStreamDataCacheLease(&lease);
  EXPECT_EQ(nullptr, lease);
  env.cache = replacement;
}

TEST(dataSinkTest, RetiredManagerReleaseDoesNotReadFreedState) {
  ScopedDataSinkEnvironment env;
  ASSERT_EQ(TSDB_CODE_SUCCESS, env.code);
  SStreamDataCacheLease* lease = nullptr;
  void*                  acquired = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, acquireStreamDataCacheLease(env.streamId, env.taskId, env.sessionId, &lease, &acquired));
  void* replacement = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDetachedStreamDataCache(env.streamId, env.taskId, env.sessionId,
                                                             DATA_CLEAN_IMMEDIATE, 0, &replacement));
  void* retired = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, replaceStreamDataCacheRegistration(env.streamId, env.taskId, env.sessionId, env.cache,
                                                                  replacement, &retired));

  auto* retiredManager = static_cast<SAlignTaskDSMgr*>(retired);
  {
    std::lock_guard<std::mutex> lock(leaseReleaseMutex);
    pausedLeaseCount = &retiredManager->leaseCount;
    observedRetired = reinterpret_cast<int8_t volatile*>(&retiredManager->retired);
    leaseCountDecremented = false;
    retireReturned = false;
    retiredReadAfterRelease.store(false);
  }
  Stub atomicInterleave;
  atomicInterleave.set(atomic_sub_fetch_32, pauseLeaseCountDecrement);
  atomicInterleave.set(atomic_load_8, observeRetiredRead);

  std::thread releaser([&] { releaseStreamDataCacheLease(&lease); });
  LeaseReleaseThreadGuard releaserGuard(releaser);
  {
    std::unique_lock<std::mutex> lock(leaseReleaseMutex);
    ASSERT_TRUE(leaseReleaseCv.wait_for(lock, std::chrono::seconds(5), [] { return leaseCountDecremented; }));
  }
  retireStreamDataCache(&retired);
  {
    std::lock_guard<std::mutex> lock(leaseReleaseMutex);
    retireReturned = true;
  }
  leaseReleaseCv.notify_all();
  releaser.join();

  EXPECT_FALSE(retiredReadAfterRelease.load());
  EXPECT_EQ(nullptr, lease);
  EXPECT_EQ(nullptr, retired);
  env.cache = replacement;
}

}  // namespace

int main(int argc, char** argv) {
  taos_init();
  const bool runAllTests = argc == 1;
  ::testing::InitGoogleTest(&argc, argv);

  int ret = RUN_ALL_TESTS();

  int ret2 = 0;
  if (runAllTests) {
    gTestMode = 0;  // Reset test mode to 0 for the next run
    ::testing::GTEST_FLAG(filter) = "dataSinkTest.multiThreadGet";
    ret2 = RUN_ALL_TESTS();
  }

  taos_cleanup();
  return ret || ret2;
}
