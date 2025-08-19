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
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <random>

#include "dataSink.h"
#include "osSleep.h"
#include "tarray.h"
#include "tdatablock.h"

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
  int32_t cleanMode = DATA_ALLOC_MODE_ALIGN | DATA_CLEAN_IMMEDIATE;
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
  cleanMode = DATA_ALLOC_MODE_ALIGN | DATA_CLEAN_IMMEDIATE;
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
  cleanMode = DATA_ALLOC_MODE_ALIGN | DATA_CLEAN_IMMEDIATE;
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
  int32_t cleanMode = DATA_ALLOC_MODE_SLIDING | DATA_CLEAN_EXPIRED;
  TSKEY   wstart = baseTestTime1 + 0;
  TSKEY   wend = baseTestTime1 + 100;
  void*   pCache = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);
  // Test invalid parameters, cleanMode is DATA_ALLOC_MODE_SLIDING | DATA_CLEAN_EXPIRED, cannot call moveStreamDataCache
  code = moveStreamDataCache(pCache, groupID, wstart, wend, pBlock);
  ASSERT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 99);
  ASSERT_EQ(code, 0);
  blockDataDestroy(pBlock);

  pBlock = createTestBlock(baseTestTime1, 100);
  cleanMode = DATA_ALLOC_MODE_SLIDING | DATA_CLEAN_EXPIRED;
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
  cleanMode = DATA_ALLOC_MODE_SLIDING | DATA_CLEAN_EXPIRED;
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
  int32_t cleanMode = DATA_ALLOC_MODE_ALIGN | DATA_CLEAN_IMMEDIATE;
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
  int32_t cleanMode = DATA_ALLOC_MODE_ALIGN | DATA_CLEAN_IMMEDIATE;
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
  int32_t cleanMode = DATA_ALLOC_MODE_SLIDING | DATA_CLEAN_EXPIRED;
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
  cleanMode = DATA_ALLOC_MODE_SLIDING | DATA_CLEAN_EXPIRED;
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
  int32_t cleanMode = DATA_ALLOC_MODE_REORDER | DATA_CLEAN_PASSIVE;
  TSKEY   wstart = baseTestTime1 + 0;
  TSKEY   wend = baseTestTime1 + 100;
  void*   pCache1 = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache1);
  ASSERT_EQ(code, 0);
  SArray*    pWindws = taosArrayInit(3, sizeof(STimeRange));
  STimeRange pRange = {wstart, wstart + 29};
  taosArrayPush(pWindws, &pRange);
  pRange.startTime = wstart + 30;
  pRange.endTime = wstart + 79;
  taosArrayPush(pWindws, &pRange);
  pRange.startTime = wstart + 80;
  pRange.endTime = wstart + 99;
  taosArrayPush(pWindws, &pRange);
  declareStreamDataWindows(pCache1, groupID, pWindws);
  taosArrayDestroy(pWindws);
  code = putStreamMultiWinDataCache(pCache1, groupID, pBlock11);
  ASSERT_EQ(code, 0);

  SSDataBlock* pBlock21 = createTestBlock(baseTestTime2, 0);
  ASSERT_NE(pBlock21, nullptr);
  int64_t streamId2 = 1;
  int64_t taskId2 = 1;
  int64_t groupID2 = 2;
  int32_t cleanMode2 = DATA_ALLOC_MODE_REORDER | DATA_CLEAN_PASSIVE;
  TSKEY   wstart2 = baseTestTime2 + 0;
  TSKEY   wend2 = baseTestTime2 + 100;
  void*   pCache2 = NULL;
  code = initStreamDataCache(streamId2, taskId2, 0, cleanMode2, 0, &pCache2);
  ASSERT_EQ(code, 0);
  pWindws = taosArrayInit(3, sizeof(STimeRange));
  pRange = {wstart2, wstart2 + 29};
  taosArrayPush(pWindws, &pRange);
  pRange.startTime = wstart2 + 30;
  pRange.endTime = wstart2 + 79;
  taosArrayPush(pWindws, &pRange);
  pRange.startTime = wstart2 + 80;
  pRange.endTime = wstart2 + 99;
  taosArrayPush(pWindws, &pRange);
  declareStreamDataWindows(pCache2, groupID2, pWindws);
  taosArrayDestroy(pWindws);

  code = putStreamMultiWinDataCache(pCache2, groupID2, pBlock21);
  ASSERT_EQ(code, 0);

  SSDataBlock* pBlock12 = createTestBlock(baseTestTime1, 100);
  cleanMode = DATA_ALLOC_MODE_REORDER | DATA_CLEAN_PASSIVE;
  wstart = baseTestTime1 + 100;
  wend = baseTestTime1 + 200;

  pWindws = taosArrayInit(2, sizeof(STimeRange));
  pRange = {wstart, wstart + 49};
  taosArrayPush(pWindws, &pRange);
  pRange.startTime = wstart + 50;
  pRange.endTime = wstart + 99;
  taosArrayPush(pWindws, &pRange);
  declareStreamDataWindows(pCache1, groupID, pWindws);
  taosArrayDestroy(pWindws);
  code = putStreamMultiWinDataCache(pCache1, groupID, pBlock12);
  ASSERT_EQ(code, 0);

  void*   pIter1 = NULL;
  int64_t notExistGroupID = groupID + 100;
  code = getStreamDataCache(pCache1, notExistGroupID, baseTestTime1 + 30, baseTestTime1 + 149, &pIter1);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pIter1, nullptr);
  code = getStreamDataCache(pCache1, groupID, baseTestTime1 + 30, baseTestTime1 + 149, &pIter1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pIter1, nullptr);
  SSDataBlock* pBlock1 = NULL;
  code = getNextStreamDataCache(&pIter1, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_NE(pIter1, nullptr);
  int rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 50);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 0, 30), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 1, 31), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 2, 32), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 29, 59), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 48, 78), true);
  ASSERT_EQ(compareBlockRow(pBlock1, pBlock11, 49, 79), true);
  blockDataDestroy(pBlock1);

  void* pIter2 = NULL;
  code = getStreamDataCache(pCache2, groupID2, baseTestTime2 + 80, baseTestTime2 + 150, &pIter2);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pIter2, nullptr);
  SSDataBlock* pBlock2 = NULL;
  code = getNextStreamDataCache(&pIter2, &pBlock2);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock2, nullptr);
  ASSERT_EQ(pIter2, nullptr);
  int rows2 = pBlock2->info.rows;
  ASSERT_EQ(rows2, 20);
  bool equal = false;
  for (int i = 0; i < 20; ++i) {
    equal = compareBlockRow(pBlock2, pBlock21, i, 80 + i);
    ASSERT_EQ(equal, true);
  }
  blockDataDestroy(pBlock2);

  code = getNextStreamDataCache(&pIter1, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 20);
  equal = false;
  for (int i = 0; i < 20; ++i) {
    equal = compareBlockRow(pBlock1, pBlock11, i, 80 + i);
    ASSERT_EQ(equal, true);
  }
  blockDataDestroy(pBlock1);
  pBlock1 = NULL;

  code = getNextStreamDataCache(&pIter1, &pBlock1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlock1, nullptr);
  ASSERT_EQ(pIter1, nullptr);
  rows = pBlock1->info.rows;
  ASSERT_EQ(rows, 50);
  equal = false;
  for (int i = 0; i < 50; ++i) {
    equal = compareBlockRow(pBlock1, pBlock12, i, i);
    ASSERT_EQ(equal, true);
  }
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

TEST(dataSinkTest, testWriteFileSize) {
  INIT_DATA_SINK(); 
  SSDataBlock* pBlock = createTestBlock(baseTestTime1, 0);
  setDataSinkMaxMemSize(DS_MEM_SIZE_RESERVED + 1024 * 1024);
  int64_t streamId = 3;
  void*   pCache = NULL;
  int64_t taskId = 1;
  int32_t cleanMode = DATA_ALLOC_MODE_REORDER | DATA_CLEAN_PASSIVE;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_NE(pBlock, nullptr);

  SArray*    pWindws = taosArrayInit_s(sizeof(STimeRange), 1);
  STimeRange* pRange = ( STimeRange*)taosArrayGet(pWindws, 0);
  pRange->startTime = baseTestTime1 + 0;
  pRange->endTime = baseTestTime1 + 99;
  
  ASSERT_EQ(code, 0);
  for (int32_t i = 0; i < 100000; i++) {
    int64_t groupID = i;
    TSKEY   wstart = baseTestTime1 + 0;
    TSKEY   wend = baseTestTime1 + 100;
    ASSERT_EQ(code, 0);
    code = declareStreamDataWindows(pCache, groupID, pWindws);
    ASSERT_EQ(code, 0);
    code = putStreamMultiWinDataCache(pCache, groupID, pBlock);
    ASSERT_EQ(code, 0);
  }
  taosArrayDestroy(pWindws);

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
  int32_t cleanMode = DATA_ALLOC_MODE_REORDER | DATA_CLEAN_PASSIVE;
  void*   pCache = NULL;
  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);

  std::random_device                     rd;
  std::mt19937                           gen(rd());
  std::uniform_int_distribution<int64_t> dist(0, 99);

  SArray*     pWindws = taosArrayInit_s(sizeof(STimeRange), 1);
  STimeRange* pRange = (STimeRange*)taosArrayGet(pWindws, 0);

  // Producer thread
  auto producer = [&](int tid) {
    for (int i = 0; i < taskPerProducer; ++i) {
      int64_t      groupId = dist(gen);
      if (groupId % producerCount != tid) continue;
      TSKEY        wstart = baseTestTime1 + groups[groupId] * 100;
      TSKEY        wend = baseTestTime1 + (++groups[groupId]) * 100 - 1;

      pRange->startTime = wstart;
      pRange->endTime = wend;

      SSDataBlock* pBlock = createTestBlock(wstart, 0);

      code = declareStreamDataWindows(pCache, groupId, pWindws);
      ASSERT_EQ(code, 0);
      code = putStreamMultiWinDataCache(pCache, groupId, pBlock);
      ASSERT_EQ(code, 0);

      // Assign to different queues according to groupId
      int queueIdx = groupId % consumerCount;
      {
        std::lock_guard<std::mutex> lock(queueMutexes[queueIdx]);
        taskQueues[queueIdx].push(Task{groupId, wstart, wend, pBlock});
      }
      queueCVs[queueIdx].notify_one();
    }
    printf("Producer %d finished producing tasks.\n", tid);
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
      int32_t code2 = getStreamDataCache(pCache, task.groupID, task.wstart, task.wend, &pIter);
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
}

TEST(dataSinkTest, cleanDataPassiveTest) {
  INIT_DATA_SINK();
  
  SSDataBlock* pBlock = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pBlock, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = DATA_ALLOC_MODE_ALIGN | DATA_CLEAN_PASSIVE;
  TSKEY   wstart = baseTestTime1 + 0;
  TSKEY   wend = baseTestTime1 + 100;
  void*   pCache = NULL;

  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache, groupID, wstart, wend, pBlock, 0, 99);
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

  // get data repeatly, should not clean data
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

  pBlock = createTestBlock(baseTestTime1, 100);
  streamId = 1;
  taskId = 1;
  groupID = 2;
  cleanMode = DATA_ALLOC_MODE_ALIGN | DATA_CLEAN_PASSIVE;
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
  cleanMode = DATA_ALLOC_MODE_ALIGN | DATA_CLEAN_PASSIVE;
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

TEST(dataSinkTest, cleanDataPassiveTest2) {
  INIT_DATA_SINK();
  
  SSDataBlock* pBlockS0 = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pBlockS0, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = DATA_ALLOC_MODE_ALIGN | DATA_CLEAN_PASSIVE;
  TSKEY   wstart0 = baseTestTime1 + 0;
  TSKEY   wend0 = baseTestTime1 + 99;
  void*   pCache = NULL;

  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);
  code = putStreamDataCache(pCache, groupID, wstart0, wend0, pBlockS0, 0, 49);
  ASSERT_EQ(code, 0);
  void* pIter = NULL;

  code = getStreamDataCache(pCache, groupID, wstart0, wend0, &pIter);
  ASSERT_EQ(code, 0);
  SSDataBlock* pBlockR0 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlockR0);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlockR0, nullptr);
  ASSERT_EQ(pIter, nullptr);
  bool equal = false;
  for(int i = 0; i < 40; ++i) {
    equal = compareBlockRow(pBlockR0, pBlockS0, i, i);
    ASSERT_EQ(equal, true);
  }
  ASSERT_EQ(equal, true);
  blockDataDestroy(pBlockR0);

  SSDataBlock* pBlockS1 = createTestBlock(baseTestTime1, 100);
  TSKEY wstart1 = baseTestTime1 + 100;
  TSKEY wend1 = baseTestTime1 + 199;

  code = putStreamDataCache(pCache, groupID, wstart1, wend1, pBlockS1, 0, 99);
  ASSERT_EQ(code, 0);
  pIter = NULL;
  code = getStreamDataCache(pCache, groupID, wstart1, wend1, &pIter);
  ASSERT_EQ(code, 0);
  SSDataBlock* pBlockR1 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlockR1);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlockR1, nullptr);
  ASSERT_EQ(pIter, nullptr);
  equal = compareBlock(pBlockS1, pBlockR1);
  ASSERT_EQ(equal, true);
  blockDataDestroy(pBlockR1);

  code = putStreamDataCache(pCache, groupID, wstart0, wend0, pBlockS0, 50, 99);
  ASSERT_EQ(code, 0);

  code = getStreamDataCache(pCache, groupID, wstart0, wend0, &pIter);
  ASSERT_EQ(code, 0);
  SSDataBlock* pBlockR2 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlockR2);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlockR2, nullptr);
  ASSERT_NE(pIter, nullptr);
  for(int i = 0; i < 40; ++i) {
    equal = compareBlockRow(pBlockR2, pBlockS0, i, i);
    ASSERT_EQ(equal, true);
  }
  blockDataDestroy(pBlockR2);
  pBlockR2 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlockR2);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlockR2, nullptr);
  ASSERT_EQ(pIter, nullptr);
  for (int i = 0; i < 50; ++i) {
    equal = compareBlockRow(pBlockR2, pBlockS0, i, i + 50);
    ASSERT_EQ(equal, true);
  }
  blockDataDestroy(pBlockR2);

  blockDataDestroy(pBlockS0);
  blockDataDestroy(pBlockS1);
  destroyDataSinkMgr();
}


TEST(dataSinkTest, dataUnsortedCacheTest) {
  INIT_DATA_SINK();
  
  SSDataBlock* pBlockS0 = createTestBlock(baseTestTime1, 0);
  ASSERT_NE(pBlockS0, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = DATA_ALLOC_MODE_REORDER | DATA_CLEAN_PASSIVE;
  TSKEY   wstart0 = baseTestTime1 + 0;
  TSKEY   wend0 = baseTestTime1 + 99;
  void*   pCache = NULL;

  int32_t code = initStreamDataCache(streamId, taskId, 0, cleanMode, 0, &pCache);
  ASSERT_EQ(code, 0);

  SArray*    pWindws = taosArrayInit(3, sizeof(STimeRange));
  STimeRange pRange = {wstart0, wstart0 + 29};
  taosArrayPush(pWindws, &pRange);
  pRange.startTime = wstart0 + 30;
  pRange.endTime = wstart0 + 79;
  taosArrayPush(pWindws, &pRange);
  pRange.startTime = wstart0 + 80;
  pRange.endTime = wstart0 + 99;
  taosArrayPush(pWindws, &pRange);

  code = declareStreamDataWindows(pCache, groupID, pWindws);
  taosArrayDestroy(pWindws);
  code = putStreamMultiWinDataCache(pCache, groupID, pBlockS0);
  ASSERT_EQ(code, 0);

  code = putStreamMultiWinDataCache(pCache, groupID, pBlockS0);
  ASSERT_EQ(code, 0);

  void* pIter = NULL;

  code = getStreamDataCache(pCache, groupID, wstart0, wstart0 + 79, &pIter);
  ASSERT_EQ(code, 0);
  SSDataBlock* pBlockR0 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlockR0);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlockR0, nullptr);
  ASSERT_NE(pIter, nullptr);
  bool equal = false;
  for(int i = 0; i < 30; ++i) {
    equal = compareBlockRow(pBlockR0, pBlockS0, i, i);
    ASSERT_EQ(equal, true);
  }
  blockDataDestroy(pBlockR0);

  pBlockR0 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlockR0);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlockR0, nullptr);
  ASSERT_NE(pIter, nullptr);
  for(int i = 0; i < 30; ++i) {
    equal = compareBlockRow(pBlockR0, pBlockS0, i, i);
    ASSERT_EQ(equal, true);
  }
  blockDataDestroy(pBlockR0);

  pBlockR0 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlockR0);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlockR0, nullptr);
  ASSERT_NE(pIter, nullptr);
  for(int i = 0; i < 50; ++i) {
    equal = compareBlockRow(pBlockR0, pBlockS0, i, i + 30);
    ASSERT_EQ(equal, true);
  }
  blockDataDestroy(pBlockR0);

  pBlockR0 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlockR0);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlockR0, nullptr);
  ASSERT_EQ(pIter, nullptr);
  for(int i = 0; i < 50; ++i) {
    equal = compareBlockRow(pBlockR0, pBlockS0, i, i + 30);
    ASSERT_EQ(equal, true);
  }
  blockDataDestroy(pBlockR0);

  clearStreamDataCache(pCache, groupID, wstart0, wstart0 + 29);

  code = getStreamDataCache(pCache, groupID, wstart0, wstart0 + 29, &pIter);
  ASSERT_EQ(code, 0);
  pBlockR0 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlockR0);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(pBlockR0, nullptr);

  code = getStreamDataCache(pCache, groupID, wstart0 + 30, wstart0 + 99, &pIter);
  ASSERT_EQ(code, 0);
  pBlockR0 = NULL;
  code = getNextStreamDataCache(&pIter, &pBlockR0);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlockR0, nullptr);
  ASSERT_NE(pIter, nullptr);
  for (int i = 0; i < 50; ++i) {
    equal = compareBlockRow(pBlockR0, pBlockS0, i, i + 30);
    ASSERT_EQ(equal, true);
  }
  blockDataDestroy(pBlockR0);
  code = getNextStreamDataCache(&pIter, &pBlockR0);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlockR0, nullptr);
  ASSERT_NE(pIter, nullptr);
  for (int i = 0; i < 50; ++i) {
    equal = compareBlockRow(pBlockR0, pBlockS0, i, i + 30);
    ASSERT_EQ(equal, true);
  }
  blockDataDestroy(pBlockR0);
  code = getNextStreamDataCache(&pIter, &pBlockR0);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlockR0, nullptr);
  ASSERT_NE(pIter, nullptr);
  for (int i = 0; i < 20; ++i) {
    equal = compareBlockRow(pBlockR0, pBlockS0, i, i + 80);
    ASSERT_EQ(equal, true);
  }
  blockDataDestroy(pBlockR0);
  code = getNextStreamDataCache(&pIter, &pBlockR0);
  ASSERT_EQ(code, 0);
  ASSERT_NE(pBlockR0, nullptr);
  ASSERT_EQ(pIter, nullptr);
  for (int i = 0; i < 20; ++i) {
    equal = compareBlockRow(pBlockR0, pBlockS0, i, i + 80);
    ASSERT_EQ(equal, true);
  }
  blockDataDestroy(pBlockR0);

  blockDataDestroy(pBlockS0);
  destroyDataSinkMgr();
}


int main(int argc, char** argv) {
  taos_init();
  ::testing::InitGoogleTest(&argc, argv);

  if (argc > 1) {
    ::testing::GTEST_FLAG(filter) = argv[1];
  } else {
    ::testing::GTEST_FLAG(filter) = "*";
  }

  int ret = RUN_ALL_TESTS();

  gTestMode = 0; // Reset test mode to 0 for the next run
  ::testing::GTEST_FLAG(filter) = "dataSinkTest.multiThreadGet";
  int ret2 = RUN_ALL_TESTS();

  taos_cleanup();
  return ret || ret2;
}
