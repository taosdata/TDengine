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

SSDataBlock* createTestBlock() {
  SSDataBlock* b = NULL;
  int32_t      code = createDataBlock(&b);

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 1);
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

  printf("binary column length:%d\n", *(int32_t*)p1->pData);

  char* pData = colDataGetData(p1, 3);
  printf("the second row of binary:%s, length:%d\n", (char*)varDataVal(pData), varDataLen(pData));
  return b;
}

TEST(dataSinkTest, fileInit) {
  int32_t code = initStreamDataSinkOnce();
  ASSERT_EQ(code, 0);
  code = initStreamDataSinkOnce();
  ASSERT_EQ(code, 0);
  destroyDataSinkManager2();
}


TEST(dataSinkTest, putStreamDataCacheTest) {
  SSDataBlock* pBlock = createTestBlock();
  ASSERT_NE(pBlock, nullptr);
  int64_t streamId = 1;
  int64_t taskId = 1;
  int64_t groupID = 1;
  int32_t cleanMode = 1;
  TSKEY wstart = 0;
  TSKEY wend = 100;
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
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
