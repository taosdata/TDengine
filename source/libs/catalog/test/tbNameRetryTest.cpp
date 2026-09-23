/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "catalogInt.h"
#include "stub.h"

namespace {

SCtgTbNamesCtx* activeTbNames;
STableMeta* successfulMeta;

int32_t decodeTbNameRetryResponse(void* out, int32_t reqType, char*, int32_t, int32_t code, char*) {
  EXPECT_EQ(reqType, TDMT_VND_TABLE_NAME);
  EXPECT_NE(activeTbNames->lock, 0);
  if (code == TSDB_CODE_SUCCESS) {
    auto* meta = static_cast<STableMetaOutput*>(out);
    SET_META_TYPE_TABLE(meta->metaType);
    meta->tbMeta = static_cast<STableMeta*>(taosMemoryCalloc(1, sizeof(STableMeta)));
    successfulMeta = meta->tbMeta;
  }
  return code;
}

TEST(catalogTbNameRetry, wholeBatchErrorPreservesSuccessAndErrorPriority) {
  Stub stub;
  stub.set(ctgProcessRspMsg, decodeTbNameRetryResponse);
  for (int32_t firstCode : {TSDB_CODE_SUCCESS, TSDB_CODE_TDB_TABLE_NOT_EXIST, TSDB_CODE_VND_HASH_MISMATCH}) {
    for (bool batchErrorFirst : {false, true}) {
      SCatalog catalog = {};
      SCtgJob job = {};
      SCtgTask task = {};
      SCtgTbNamesCtx context = {};
      SCtgFetch fetch = {};
      STableMetaOutput output = {};
      SMetaRes result = {};
      SName name = {};
      STablesReq names = {};
      names.pTables = taosArrayInit(1, sizeof(SName));
      ASSERT_NE(taosArrayPush(names.pTables, &name), nullptr);
      context.pNames = taosArrayInit(1, sizeof(STablesReq));
      ASSERT_NE(taosArrayPush(context.pNames, &names), nullptr);
      context.pFetchs = taosArrayInit(1, sizeof(SCtgFetch));
      ASSERT_NE(taosArrayPush(context.pFetchs, &fetch), nullptr);
      context.pResList = taosArrayInit(1, sizeof(SMetaRes));
      ASSERT_NE(taosArrayPush(context.pResList, &result), nullptr);
      context.fetchNum = 2;
      activeTbNames = &context;
      successfulMeta = nullptr;

      // Leave another task pending so completion records its code without
      // launching an unrelated asynchronous job callback.
      job.pTasks = taosArrayInit(2, sizeof(SCtgTask));
      ASSERT_NE(taosArrayPush(job.pTasks, &task), nullptr);
      ASSERT_NE(taosArrayPush(job.pTasks, &task), nullptr);
      job.pCtg = &catalog;
      task.pJob = &job;
      task.type = CTG_TASK_GET_TB_NAME;
      task.taskCtx = &context;
      task.msgCtx.reqType = TDMT_VND_TABLE_NAME;
      task.msgCtx.out = &output;
      SCtgTaskReq request = {};
      request.pTask = &task;
      request.msgIdx = 0;
      SDataBuf message = {};
      auto handler = gCtgAsyncFps[CTG_TASK_GET_TB_NAME].handleRspFp;
      const int32_t batchCode = TSDB_CODE_RPC_NETWORK_UNAVAIL;
      for (int32_t index = 0; index < 2; ++index) {
        bool batchError = (index == 0) == batchErrorFirst;
        (void)handler(&request, batchError ? -1 : TDMT_VND_TABLE_NAME, &message,
                       batchError ? batchCode : firstCode);
        EXPECT_EQ(context.lock, 0);
      }

      ASSERT_NE(task.res, nullptr);
      auto* actual = static_cast<SMetaRes*>(taosArrayGet(static_cast<SArray*>(task.res), 0));
      ASSERT_NE(actual, nullptr);
      int32_t expectedCode = firstCode == TSDB_CODE_SUCCESS ? TSDB_CODE_SUCCESS : batchCode;
      EXPECT_EQ(actual->code, expectedCode);
      EXPECT_EQ(actual->pRes, successfulMeta);
      EXPECT_EQ(task.code, expectedCode);
      EXPECT_EQ(context.fetchNum, 0);
      EXPECT_EQ(job.taskDone, 1);
      EXPECT_EQ(output.tbMeta, nullptr);

      taosMemoryFree(successfulMeta);
      taosArrayDestroy(static_cast<SArray*>(task.res));
      taosArrayDestroy(context.pNames);
      taosArrayDestroy(context.pFetchs);
      taosArrayDestroy(names.pTables);
      taosArrayDestroy(job.pTasks);
    }
  }
  activeTbNames = nullptr;
}

}  // namespace
