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
#include <taoserror.h>
#include <tglobal.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "tsdbDef.h"



int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#if 0
TEST(testCase, tSmaInsertTest) {
  STSma     tSma = {0};
  STSmaData* pSmaData = NULL;
  STsdb     tsdb = {0};

  // init
  tSma.intervalUnit = TD_TIME_UNIT_DAY;
  tSma.interval = 1;
  tSma.numOfFuncIds = 5;  // sum/min/max/avg/last

  int32_t blockSize = tSma.numOfFuncIds * sizeof(int64_t);
  int32_t numOfColIds = 3;
  int32_t numOfSmaBlocks = 10;

  int32_t dataLen = numOfColIds * numOfSmaBlocks * blockSize;

  pSmaData = (STSmaData*)malloc(sizeof(STSmaData) + dataLen);
  ASSERT_EQ(pSmaData != NULL, true);
  pSmaData->tableUid = 3232329230;
  pSmaData->numOfColIds = numOfColIds;
  pSmaData->numOfSmaBlocks = numOfSmaBlocks;
  pSmaData->dataLen = dataLen;
  pSmaData->tsWindow.skey = 1640000000;
  pSmaData->tsWindow.ekey = 1645788649;
  pSmaData->colIds = (col_id_t*)malloc(sizeof(col_id_t) * numOfColIds);
  ASSERT_EQ(pSmaData->colIds != NULL, true);

  for (int32_t i = 0; i < numOfColIds; ++i) {
    *(pSmaData->colIds + i) = (i + PRIMARYKEY_TIMESTAMP_COL_ID);
  }

  // execute
  EXPECT_EQ(tsdbInsertTSmaData(&tsdb, &tSma, pSmaData), TSDB_CODE_SUCCESS);

  // release
  tdDestroySmaData(pSmaData);
}
#endif

#pragma GCC diagnostic pop