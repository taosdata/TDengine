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

// C++ standard headers first: os.h turns nanosleep() into a forbidden symbol, which breaks std::this_thread
#include <atomic>
#include <iostream>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <vnodeInt.h>

#include <taoserror.h>
#include <tglobal.h>

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

namespace {

struct TqTestEnv {
  SVnode* pVnode;
  STQ*    pTq;
};

TqTestEnv tqTestEnvCreate() {
  TqTestEnv env = {};
  env.pVnode = (SVnode*)taosMemoryCalloc(1, sizeof(SVnode));
  env.pTq = (STQ*)taosMemoryCalloc(1, sizeof(STQ));
  env.pVnode->restored = 1;
  env.pTq->pVnode = env.pVnode;
  taosInitRWLatch(&env.pTq->lock);
  env.pTq->pHandle = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_ENTRY_LOCK);
  env.pTq->pOffset = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_ENTRY_LOCK);
  taosHashSetFreeFp(env.pTq->pOffset, (FDelete)tDeleteSTqOffset);
  return env;
}

void tqTestEnvDestroy(TqTestEnv* pEnv) {
  taosHashCleanup(pEnv->pTq->pHandle);
  taosHashCleanup(pEnv->pTq->pOffset);
  taosMemoryFree(pEnv->pTq);
  taosMemoryFree(pEnv->pVnode);
}

// puts a snapshot offset with a heap allocated varchar primary key into the offset cache
void tqTestPutVarKeyOffset(STQ* pTq, const char* subKey, const char* pk) {
  STqOffset offset = {};
  offset.val.type = TMQ_OFFSET__SNAPSHOT_DATA;
  offset.val.uid = 7;
  offset.val.ts = 99;
  offset.val.primaryKey.type = TSDB_DATA_TYPE_VARCHAR;
  offset.val.primaryKey.nData = strlen(pk);
  offset.val.primaryKey.pData = (uint8_t*)taosMemoryCalloc(1, strlen(pk));
  memcpy(offset.val.primaryKey.pData, pk, strlen(pk));
  tstrncpy(offset.subKey, subKey, sizeof(offset.subKey));
  ASSERT_EQ(0, taosHashPut(pTq->pOffset, subKey, strlen(subKey), &offset, sizeof(STqOffset)));
}

}  // namespace

TEST(testCase, tqProcessDeleteSubReqRejectsShortMsg) {
  TqTestEnv env = tqTestEnvCreate();

  char msg[8] = {0};
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tqProcessDeleteSubReq(env.pTq, 0, msg, (int32_t)sizeof(msg)));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tqProcessDeleteSubReq(env.pTq, 0, msg, 0));

  std::vector<char> almost(sizeof(SMqVDeleteReq), 0);
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tqProcessDeleteSubReq(env.pTq, 0, almost.data(), (int32_t)almost.size() - 1));

  tqTestEnvDestroy(&env);
}

TEST(testCase, tqProcessDeleteSubReqRejectsNonTerminatedSubKey) {
  TqTestEnv env = tqTestEnvCreate();

  // body is long enough but subKey has no NUL byte: must not be strlen()'d
  std::vector<char> exact(sizeof(SMqVDeleteReq), 'a');
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tqProcessDeleteSubReq(env.pTq, 0, exact.data(), (int32_t)exact.size()));

  std::vector<char> big(1500, 'a');
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tqProcessDeleteSubReq(env.pTq, 0, big.data(), (int32_t)big.size()));

  tqTestEnvDestroy(&env);
}

TEST(testCase, tqMetaGetOffsetCopiesOut) {
  TqTestEnv env = tqTestEnvCreate();
  tqTestPutVarKeyOffset(env.pTq, "g1:topic", "abcde");

  STqOffset copy = {};
  ASSERT_EQ(0, tqMetaGetOffset(env.pTq, "g1:topic", &copy));
  EXPECT_EQ(TMQ_OFFSET__SNAPSHOT_DATA, copy.val.type);
  EXPECT_EQ(7, copy.val.uid);
  EXPECT_EQ(99, copy.val.ts);
  EXPECT_STREQ("g1:topic", copy.subKey);
  ASSERT_EQ(5u, copy.val.primaryKey.nData);
  ASSERT_NE(nullptr, copy.val.primaryKey.pData);

  // the copy must not alias the cached entry
  STqOffset* pCached = (STqOffset*)taosHashGet(env.pTq->pOffset, "g1:topic", strlen("g1:topic"));
  ASSERT_NE(nullptr, pCached);
  EXPECT_NE(pCached, &copy);
  EXPECT_NE(pCached->val.primaryKey.pData, copy.val.primaryKey.pData);

  // a concurrent commit replaces (and frees) the cached entry, the copy has to stay valid
  STqOffset newer = {};
  newer.val.type = TMQ_OFFSET__LOG;
  newer.val.version = 123;
  tstrncpy(newer.subKey, "g1:topic", sizeof(newer.subKey));
  taosWLockLatch(&env.pTq->lock);
  ASSERT_EQ(0, taosHashPut(env.pTq->pOffset, "g1:topic", strlen("g1:topic"), &newer, sizeof(STqOffset)));
  taosWUnLockLatch(&env.pTq->lock);

  EXPECT_EQ(TMQ_OFFSET__SNAPSHOT_DATA, copy.val.type);
  EXPECT_EQ(0, memcmp(copy.val.primaryKey.pData, "abcde", 5));
  tDeleteSTqOffset(&copy);

  STqOffset copy2 = {};
  ASSERT_EQ(0, tqMetaGetOffset(env.pTq, "g1:topic", &copy2));
  EXPECT_EQ(TMQ_OFFSET__LOG, copy2.val.type);
  EXPECT_EQ(123, copy2.val.version);
  tDeleteSTqOffset(&copy2);

  tqTestEnvDestroy(&env);
}

TEST(testCase, tqMetaGetOffsetConcurrentWithReplace) {
  TqTestEnv env = tqTestEnvCreate();
  tqTestPutVarKeyOffset(env.pTq, "g1:topic", "abcde");

  std::atomic<bool> stop{false};
  std::atomic<int>  bad{0};
  std::vector<std::thread> readers;
  for (int i = 0; i < 4; i++) {
    readers.emplace_back([&]() {
      while (!stop.load()) {
        STqOffset copy = {};
        if (tqMetaGetOffset(env.pTq, "g1:topic", &copy) != 0 || copy.val.primaryKey.nData != 5 ||
            copy.val.primaryKey.pData == nullptr || memcmp(copy.val.primaryKey.pData, "abcde", 5) != 0) {
          bad++;
        }
        tDeleteSTqOffset(&copy);
      }
    });
  }

  // replacing a cached entry frees the old one, readers must be excluded by the write latch while it happens
  for (int i = 0; i < 2000; i++) {
    STqOffset next = {};
    next.val.type = TMQ_OFFSET__SNAPSHOT_DATA;
    next.val.uid = 7;
    next.val.ts = i;
    next.val.primaryKey.type = TSDB_DATA_TYPE_VARCHAR;
    next.val.primaryKey.nData = 5;
    next.val.primaryKey.pData = (uint8_t*)taosMemoryCalloc(1, 5);
    memcpy(next.val.primaryKey.pData, "abcde", 5);
    tstrncpy(next.subKey, "g1:topic", sizeof(next.subKey));
    taosWLockLatch(&env.pTq->lock);
    ASSERT_EQ(0, taosHashPut(env.pTq->pOffset, "g1:topic", strlen("g1:topic"), &next, sizeof(STqOffset)));
    taosWUnLockLatch(&env.pTq->lock);
  }

  stop = true;
  for (auto& t : readers) t.join();
  EXPECT_EQ(0, bad.load());
  tqTestEnvDestroy(&env);
}

// tqOffsetCommit (iterates the offset hash) and tqMetaGetOffset (readers) must never wait for each other in opposite
// orders: the iteration keeps the latch of the current hash entry, and saving an offset needs pTq->lock
TEST(testCase, tqOffsetCommitConcurrentWithGetOffset) {
  TqTestEnv env = tqTestEnvCreate();
  const int kKeys = 8;
  char      keys[kKeys][32];
  for (int i = 0; i < kKeys; i++) {
    snprintf(keys[i], sizeof(keys[i]), "g%d:topic", i);
    tqTestPutVarKeyOffset(env.pTq, keys[i], "abcde");
  }

  std::atomic<bool> stop{false};
  std::atomic<bool> finished{false};
  std::atomic<int>  bad{0};

  // a deadlock must fail the test instead of hanging the whole run
  std::thread watchdog([&]() {
    for (int i = 0; i < 600 && !finished.load(); i++) taosMsleep(100);
    if (!finished.load()) {
      fprintf(stderr, "tqOffsetCommit/tqMetaGetOffset deadlock: no progress for 60s\n");
      _exit(1);
    }
  });

  std::vector<std::thread> readers;
  for (int i = 0; i < 4; i++) {
    readers.emplace_back([&, i]() {
      int n = i;
      while (!stop.load()) {
        STqOffset copy = {};
        if (tqMetaGetOffset(env.pTq, keys[n++ % kKeys], &copy) != 0 || copy.val.primaryKey.nData != 5) bad++;
        tDeleteSTqOffset(&copy);
      }
    });
  }

  // the offset store is not set up in this environment, saving fails after taking pTq->lock: that is all the
  // lock order needs
  for (int i = 0; i < 300; i++) (void)tqOffsetCommit(env.pTq);

  stop = true;
  for (auto& t : readers) t.join();
  finished = true;
  watchdog.join();
  EXPECT_EQ(0, bad.load());
  tqTestEnvDestroy(&env);
}

TEST(testCase, tqMetaGetOffsetInvalidParam) {
  TqTestEnv env = tqTestEnvCreate();
  STqOffset offset = {};
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tqMetaGetOffset(NULL, "k", &offset));
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tqMetaGetOffset(env.pTq, NULL, &offset));
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tqMetaGetOffset(env.pTq, "k", NULL));
  tqTestEnvDestroy(&env);
}
