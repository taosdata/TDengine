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
#include <vnodeInt.h>

#include <taoserror.h>
#include <tglobal.h>
#include <iostream>

#include <tmsg.h>
#include <vnodeInt.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

SDmNotifyHandle dmNotifyHdl = {}; // zero-init (state = 0)

#include "tq.h"
#include "executor.h"
#include "stub.h"
#include "tdatablock.h"
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

void tqWriteOffset() {
  TdFilePtr pFile = taosOpenFile(TQ_OFFSET_NAME, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);

  STqOffset offset = {};
  offset.val.type = TMQ_OFFSET__LOG;
  offset.val.version = 8923;
  strcpy(offset.subKey, "testtest");
  int32_t    bodyLen;
  int32_t    code;
  tEncodeSize(tEncodeSTqOffset, &offset, bodyLen, code);
  int32_t totLen = INT_BYTES + bodyLen;
  void*   buf = taosMemoryCalloc(1, totLen);
  void*   abuf = POINTER_SHIFT(buf, INT_BYTES);

  *(int32_t*)buf = htonl(bodyLen);
  SEncoder encoder;
  tEncoderInit(&encoder, (uint8_t*)abuf, bodyLen);
  tEncodeSTqOffset(&encoder, &offset);
  taosWriteFile(pFile, buf, totLen);

  taosMemoryFree(buf);

  taosCloseFile(&pFile);
}

TEST(testCase, tqOffsetTest) {
  STQ* pTq = (STQ*)taosMemoryCalloc(1, sizeof(STQ));
  pTq->path = taosStrdup("./");

  pTq->pOffset = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_ENTRY_LOCK);
  taosHashSetFreeFp(pTq->pOffset, (FDelete)tDeleteSTqOffset);

  tdbOpen(pTq->path, 16 * 1024, 1, &pTq->pMetaDB, 0, NULL);
  tdbTbOpen("tq.offset.db", -1, -1, NULL, pTq->pMetaDB, &pTq->pOffsetStore, 0);

  tqWriteOffset();
  tqOffsetRestoreFromFile(pTq, TQ_OFFSET_NAME);
  taosRemoveFile(TQ_OFFSET_NAME);
  tqClose(pTq);
}

namespace {

SSDataBlock* snapshotBlock = nullptr;
int32_t snapshotReads = 0;
int32_t snapshotResponses = 0;
STqOffsetVal snapshotResponseOffset = {};
SMqBatchMetaRsp snapshotMeta = {};

int32_t prepareSnapshot(qTaskInfo_t, STqOffsetVal*, int8_t) { return TSDB_CODE_SUCCESS; }
void setSnapshotParams(qTaskInfo_t, int8_t, int32_t, int64_t, int8_t, int64_t) {}

int32_t readSnapshot(qTaskInfo_t, SSDataBlock** block, uint64_t*) {
  ++snapshotReads;
  taosMsleep(20);
  if (snapshotReads > 1) {
    return TSDB_CODE_FAILED;
  }
  *block = snapshotBlock;
  return TSDB_CODE_SUCCESS;
}

SMqBatchMetaRsp* extractSnapshotMeta(qTaskInfo_t) { return &snapshotMeta; }

int32_t extractSnapshotOffset(qTaskInfo_t, STqOffsetVal* offset) {
  offset->type = TMQ_OFFSET__SNAPSHOT_DATA;
  offset->uid = 42;
  offset->ts = 1000;
  return TSDB_CODE_SUCCESS;
}

int32_t sendSnapshotResponse(STqHandle*, const SRpcMsg*, const SMqPollReq*, SMqDataRsp* response,
                             int32_t, int32_t) {
  ++snapshotResponses;
  EXPECT_EQ(response->blockNum, 0);
  snapshotResponseOffset = response->rspOffset;
  return TSDB_CODE_SUCCESS;
}

}

TEST(testCase, filtered_snapshot_returns_progress_on_timeout) {
  snapshotReads = 0;
  snapshotResponses = 0;
  snapshotResponseOffset = {};
  snapshotMeta = {};
  ASSERT_EQ(createDataBlock(&snapshotBlock), TSDB_CODE_SUCCESS);
  SColumnInfoData column = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t),
                                               PRIMARYKEY_TIMESTAMP_COL_ID);
  ASSERT_EQ(blockDataAppendColInfo(snapshotBlock, &column), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(snapshotBlock, 1), TSDB_CODE_SUCCESS);
  auto* timestampColumn = static_cast<SColumnInfoData*>(taosArrayGet(snapshotBlock->pDataBlock, 0));
  int64_t timestamp = 1000;
  ASSERT_EQ(colDataSetVal(timestampColumn, 0, reinterpret_cast<const char*>(&timestamp), false), TSDB_CODE_SUCCESS);
  snapshotBlock->info.rows = 1;

  Stub stub;
  stub.set(qStreamPrepareScan, prepareSnapshot);
  stub.set(qStreamSetParams, setSnapshotParams);
  stub.set(qExecTask, readSnapshot);
  stub.set(qStreamExtractMetaMsg, extractSnapshotMeta);
  stub.set(qStreamExtractOffset, extractSnapshotOffset);
  stub.set(tqSendDataRsp, sendSnapshotResponse);

  SVnode vnode = {};
  vnode.config.tsdbCfg.precision = TSDB_TIME_PRECISION_MILLI;
  STQ queue = {};
  queue.pVnode = &vnode;
  STqHandle handle = {};
  handle.execHandle.subType = TOPIC_SUB_TYPE__DB;
  SMqPollReq request = {};
  request.reqOffset.type = TMQ_OFFSET__SNAPSHOT_DATA;
  request.reqOffset.uid = 42;
  request.startTs = 2000;
  request.timeout = 10;
  request.minPollRows = 4096;
  SRpcMsg message = {};

  EXPECT_EQ(tqExtractDataForMq(&queue, &handle, &request, &message), TSDB_CODE_SUCCESS);
  EXPECT_EQ(snapshotReads, 1);
  EXPECT_EQ(snapshotResponses, 1);
  EXPECT_EQ(snapshotResponseOffset.type, TMQ_OFFSET__SNAPSHOT_DATA);
  EXPECT_EQ(snapshotResponseOffset.uid, 42);
  EXPECT_EQ(snapshotResponseOffset.ts, 1000);
  blockDataDestroy(snapshotBlock);
  snapshotBlock = nullptr;
}

#pragma GCC diagnostic pop
