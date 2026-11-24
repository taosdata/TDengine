/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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
#include <chrono>
#include <functional>
#include <future>
#include <utility>
#include <vector>
#include "meta.h"
#include "monitor.h"
#include "qworker.h"
#include "streamInt.h"
#include "streamReader.h"
#include "stub.h"
#include "tcompare.h"
#include "tworker.h"
#include "vnodeInt.h"

SDmNotifyHandle dmNotifyHdl = {};

namespace {
SStreamTriggerReaderInfo* activeReader = nullptr;
SSTriggerTimeAlignDataRsp response = {};
std::vector<char>         responseWire;
int32_t                   responseCode = 0;
ESTriggerPullType         pullType = STRIGGER_PULL_TIME_ALIGN_DATA;
int64_t                   nextCursor = -1;
int   responseCount = 0;
bool  ready(int64_t) { return true; }
void* reader(int64_t, int64_t, void** taskAddr) {
  *taskAddr = nullptr;
  return activeReader;
}
void capture(SRpcMsg* rsp) {
  ++responseCount;
  responseCode = rsp->code;
  responseWire.clear();
  if ((responseCode == 0 || responseCode == TSDB_CODE_STREAM_NO_DATA) && rsp->contLen > 0) {
    responseWire.assign(static_cast<char*>(rsp->pCont), static_cast<char*>(rsp->pCont) + rsp->contLen);
    if (responseCode == TSDB_CODE_STREAM_NO_DATA) {
      memcpy(&nextCursor, rsp->pCont, sizeof(nextCursor));
    } else if (pullType == STRIGGER_PULL_TIME_ALIGN_BOUND || pullType == STRIGGER_PULL_FIRST_TS) {
      SSDataBlock block = {};
      responseCode = tDeserializeSStreamTsResponse(rsp->pCont, rsp->contLen, &block);
      nextCursor = block.info.id.groupId;
      blockDataFreeRes(&block);
    } else if (pullType != STRIGGER_PULL_TSDB_META && pullType != STRIGGER_PULL_TSDB_META_NEXT &&
               pullType != STRIGGER_PULL_TIME_ALIGN_WAL_RANGE && pullType != STRIGGER_PULL_TSDB_DATA) {
      responseCode = tDeserializeSTriggerTimeAlignDataRsp(rsp->pCont, rsp->contLen, &response);
    }
  }
  rpcFreeCont(rsp->pCont);
}

class TimeAlignDataDispatchTest : public ::testing::Test {
 protected:
  Stub                      stub;
  SVnode                    vnode = {};
  SStreamReaderTask         task = {};
  SStreamTriggerReaderInfo  info = {};
  SSTriggerPullRequestUnion req = {};
  void                      SetUp() override {
    vnode.pWal = static_cast<SWal*>(taosMemoryCalloc(1, sizeof(SWal)));
    ASSERT_NE(nullptr, vnode.pWal);
    ASSERT_EQ(0, taosThreadRwlockInit(&vnode.pWal->mutex, nullptr));
    vnode.pWal->pRefHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    ASSERT_NE(nullptr, vnode.pWal->pRefHash);
    vnode.config.vgId = 7;
    vnode.state.applied = 100;
    info.pTask = &task;
    info.timeAlignConsumer = true;
    info.isVtableStream = 1;
    info.vTableReady = 1;
    info.uidHashTrigger = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    SSHashObj* columns = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
    ASSERT_NE(nullptr, columns);
    ASSERT_NE(nullptr, info.uidHashTrigger);
    int64_t key[2] = {0, 7};
    ASSERT_EQ(0, tSimpleHashPut(info.uidHashTrigger, key, sizeof(key), &columns, sizeof(columns)));
    for (col_id_t cid : {2, 3}) ASSERT_EQ(0, tSimpleHashPut(columns, &cid, sizeof(cid), &cid, sizeof(cid)));
    ASSERT_EQ(0, initStreamTableListInfo(&info.tableList, UIDMAP_SINGLE));
    info.streamTaskMap = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    ASSERT_NE(nullptr, info.streamTaskMap);
    taosHashSetFreeFp(info.streamTaskMap, releaseStreamTask);
    activeReader = &info;
    stub.set(syncIsReadyForRead, ready);
    stub.set(qStreamGetReaderInfo, reader);
    stub.set(tmsgSendRsp, capture);
    req.base.type = STRIGGER_PULL_TIME_ALIGN_DATA;
    req.base.streamId = 11;
    req.base.readerTaskId = 22;
    req.base.sessionId = STREAM_TRIGGER_REALTIME_SESSIONID;
    req.timeAlignDataReq.uid = 7;
    req.timeAlignDataReq.ekey = 25;
    req.timeAlignDataReq.ver = 10;
    req.timeAlignDataReq.roundId = 12;
    req.timeAlignDataReq.scanId = 1;
    req.timeAlignDataReq.order = TSDB_ORDER_DESC;
    req.timeAlignDataReq.skey = INT64_MIN;
    req.timeAlignDataReq.cids = taosArrayInit(2, sizeof(col_id_t));
    ASSERT_NE(nullptr, req.timeAlignDataReq.cids);
    for (col_id_t cid : {1, 3, 2}) ASSERT_NE(nullptr, taosArrayPush(req.timeAlignDataReq.cids, &cid));
  }
  void TearDown() override {
    int64_t key[2] = {0, 7};
    auto*   columns = static_cast<SSHashObj**>(tSimpleHashGet(info.uidHashTrigger, key, sizeof(key)));
    if (columns) tSimpleHashCleanup(*columns);
    tSimpleHashCleanup(info.uidHashTrigger);
    taosHashCleanup(info.pTimeAlignScans);
    taosHashCleanup(info.pTimeAlignRounds);
    req.base.type = STRIGGER_PULL_TIME_ALIGN_DATA;
    taosHashCleanup(info.streamTaskMap);
    qStreamDestroyTableInfo(&info.tableList);
    tDestroySTriggerPullRequest(&req);
    tDestroySTriggerTimeAlignDataRsp(&response);
    EXPECT_EQ(0, taosHashGetSize(vnode.pWal->pRefHash));
    taosHashCleanup(vnode.pWal->pRefHash);
    (void)taosThreadRwlockDestroy(&vnode.pWal->mutex);
    taosMemoryFree(vnode.pWal);
    activeReader = nullptr;
  }
  int32_t historyPull(SSTriggerPullRequestUnion request) {
    auto saved = req;
    req = request;
    int32_t code = pull(request.base.type);
    req = saved;
    return code;
  }
  int32_t round(ESTriggerPullType type) {
    auto                           saved = req;
    SSTriggerTimeAlignRoundRequest request = {};
    request.base = req.base;
    request.roundId = req.timeAlignDataReq.roundId;
    request.window = {INT64_MIN, INT64_MAX};
    req.timeAlignRoundReq = request;
    int32_t code = pull(type);
    req = saved;
    return code;
  }
  int32_t pull(ESTriggerPullType type = STRIGGER_PULL_TIME_ALIGN_DATA, int32_t page = 0) {
    tDestroySTriggerTimeAlignDataRsp(&response);
    req.base.type = type;
    pullType = type;
    if (type == STRIGGER_PULL_TIME_ALIGN_DATA || type == STRIGGER_PULL_TIME_ALIGN_DATA_NEXT ||
        type == STRIGGER_PULL_TIME_ALIGN_DATA_CLOSE)
      req.timeAlignDataReq.batchId = page;
    int32_t len = tSerializeSTriggerPullRequest(nullptr, 0, &req.base);
    EXPECT_GT(len, 0);
    if (len <= 0) return len;
    std::vector<char> wire(sizeof(SMsgHead) + len);
    EXPECT_EQ(len, tSerializeSTriggerPullRequest(wire.data() + sizeof(SMsgHead), len, &req.base));
    SRpcMsg msg = {};
    msg.msgType = TDMT_STREAM_TRIGGER_PULL;
    msg.pCont = wire.data();
    msg.contLen = wire.size();
    responseCount = 0;
    int32_t code = vnodeProcessStreamReaderMsg(&vnode, &msg, nullptr);
    EXPECT_EQ(1, responseCount);
    if ((type == STRIGGER_PULL_WAL_META_NEW || type == STRIGGER_PULL_WAL_META_DATA_NEW) &&
        responseCode == TSDB_CODE_STREAM_NO_DATA) {
      EXPECT_EQ(0, code);
    } else {
      EXPECT_EQ(code, responseCode);
    }
    return code;
  }
};

struct Row {
  int64_t ts;
  int32_t a;
  int32_t b;
  bool    nullA;
  bool    nullB;
  int64_t sampled;
};
std::vector<Row> rows;
std::vector<SSchema> columns;
int                  opened = 0, closed = 0, advances = 0;
int32_t          readError = 0, metaError = 0;
int64_t          capturedVersion = -1;
struct Cursor {
  SSDataBlock*          block;
  std::vector<col_id_t> cids;
  std::vector<int32_t>  slots;
  int64_t               lower;
  int64_t               upper;
  bool                  ascending;
  size_t                next;
};
void    initMeta(SMetaReader*, void*, int32_t, SStoreMeta*, int64_t) {}
void    clearMeta(SMetaReader*) {}
int32_t getMeta(SMetaReader* r, tb_uid_t) {
  if (metaError != 0) return metaError;
  r->me.type = TD_NORMAL_TABLE;
  r->me.ntbEntry.schemaRow.nCols = columns.size();
  r->me.ntbEntry.schemaRow.pSchema = columns.data();
  return 0;
}
int32_t openCursor(void*, SQueryTableDataCond* cond, void*, int32_t count, SSDataBlock* block, void** result,
                   const char*, SHashObj**) {
  EXPECT_EQ(1, count);
  EXPECT_TRUE(cond->order == TSDB_ORDER_DESC || cond->order == TSDB_ORDER_ASC);
  capturedVersion = cond->endVersion;
  auto* c = new Cursor{block, {}, {}, cond->twindows.skey, cond->twindows.ekey, cond->order == TSDB_ORDER_ASC, 0};
  for (int i = 0; i < cond->numOfCols; ++i) {
    if (i) EXPECT_LT(cond->colList[i - 1].colId, cond->colList[i].colId);
    c->cids.push_back(cond->colList[i].colId);
    c->slots.push_back(cond->pSlotList[i]);
  }
  *result = c;
  ++opened;
  return 0;
}
struct Boundary {
  STimeWindow window;
  int64_t     uid;
  int32_t     order;
};
int32_t openBoundary(void*, STimeWindow* window, SVersionRange* version, uint64_t, void* tables, int32_t count,
                     int32_t order, void** result, const char*) {
  EXPECT_EQ(1, count);
  capturedVersion = version->maxVer;
  *result = new Boundary{*window, static_cast<int64_t>(static_cast<STableKeyInfo*>(tables)->uid), order};
  return 0;
}
int32_t nextBoundary(void* p, SSDataBlock* block, bool* more) {
  const auto& b = *static_cast<Boundary*>(p);
  *more = false;
  for (size_t i = 0; i < rows.size(); ++i) {
    const auto& r = rows[b.order == TSDB_ORDER_ASC ? rows.size() - i - 1 : i];
    if (r.ts < b.window.skey || r.ts > b.window.ekey) continue;
    auto*   ts = static_cast<SColumnInfoData*>(taosArrayGet(block->pDataBlock, 0));
    auto*   uid = static_cast<SColumnInfoData*>(taosArrayGet(block->pDataBlock, 1));
    int64_t stamp = r.ts, table = b.uid;
    colDataSetInt64(ts, block->info.rows, &stamp);
    colDataSetInt64(uid, block->info.rows, &table);
    ++block->info.rows;
    break;
  }
  return 0;
}
void closeBoundary(void* p) { delete static_cast<Boundary*>(p); }
void closeCursor(void* p) {
  if (p != nullptr) {
    delete static_cast<Cursor*>(p);
    ++closed;
  }
}
void    releaseBlock(void*) {}
int32_t nextBlock(void* p, bool* more) {
  ++advances;
  if (readError != 0) return readError;
  auto* c = static_cast<Cursor*>(p);
  blockDataCleanup(c->block);
  int32_t code = blockDataEnsureCapacity(c->block, STREAM_RETURN_ROWS_NUM);
  if (code != 0) return code;
  int n = 0;
  while (c->next < rows.size() && n < STREAM_RETURN_ROWS_NUM) {
    size_t      index = c->next++;
    const auto& row = rows[c->ascending ? rows.size() - index - 1 : index];
    if (row.ts < c->lower || row.ts > c->upper) continue;
    for (size_t col = 0; col < c->cids.size(); ++col) {
      auto*          dst = static_cast<SColumnInfoData*>(taosArrayGet(c->block->pDataBlock, c->slots[col]));
      const col_id_t cid = c->cids[col];
      const void*    value = cid == 1 ? static_cast<const void*>(&row.ts) : cid == 2 ? &row.a : &row.b;
      if (cid != PRIMARYKEY_TIMESTAMP_COL_ID && dst->info.type == TSDB_DATA_TYPE_TIMESTAMP) value = &row.sampled;
      bool           isNull = cid == 2 ? row.nullA : cid == 3 && row.nullB;
      code = colDataSetVal(dst, n, static_cast<const char*>(value), isNull);
      if (code != 0) return code;
    }
    ++n;
  }
  c->block->info.rows = n;
  *more = n != 0;
  return 0;
}
int32_t retrieve(void* p, SSDataBlock** block) {
  *block = static_cast<Cursor*>(p)->block;
  return 0;
}

class TimeAlignDataReaderTest : public TimeAlignDataDispatchTest {
 protected:
  void SetUp() override {
    TimeAlignDataDispatchTest::SetUp();
    opened = closed = advances = 0;
    readError = metaError = 0;
    rows = {{30, 0, 9, true, false}, {20, 7, 0, false, true}, {10, 1, 2, false, false}};
    columns.assign(3, SSchema{});
    for (int i = 0; i < 3; ++i) {
      columns[i].colId = i + 1;
      columns[i].type = i == 0 ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_INT;
      columns[i].bytes = i == 0 ? 8 : 4;
    }
    info.storageApi.metaReaderFn.initReader = initMeta;
    info.storageApi.metaReaderFn.clearReader = clearMeta;
    info.storageApi.metaReaderFn.getTableEntryByUid = getMeta;
    info.storageApi.tsdReader.tsdReaderOpen = openCursor;
    info.storageApi.tsdReader.tsdCreateFirstLastTsIter = openBoundary;
    info.storageApi.tsdReader.tsdNextFirstLastTsBlock = nextBoundary;
    info.storageApi.tsdReader.tsdDestroyFirstLastTsIter = closeBoundary;
    info.storageApi.tsdReader.tsdReaderClose = closeCursor;
    info.storageApi.tsdReader.tsdNextDataBlock = nextBlock;
    info.storageApi.tsdReader.tsdReaderRetrieveDataBlock = retrieve;
    info.storageApi.tsdReader.tsdReaderReleaseDataBlock = releaseBlock;
    vnode.state.applied = 10;
    ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
    ASSERT_EQ(vnode.state.applied + 1, nextCursor);
    EXPECT_EQ(opened, closed);
    opened = closed = advances = 0;
  }
  void TearDown() override {
    TimeAlignDataDispatchTest::TearDown();
    EXPECT_EQ(opened, closed);
  }
  void longNullRun() {
    rows.clear();
    for (int i = STREAM_RETURN_ROWS_NUM + 1; i > 0; --i) rows.push_back({i, 0, 0, true, true});
    rows.push_back({0, 1, 2, false, false});
    req.timeAlignDataReq.ekey = STREAM_RETURN_ROWS_NUM + 2;
  }
};

class TimeAlignWalReaderTest : public TimeAlignDataReaderTest {
 protected:
  void SetUp() override {
    TimeAlignDataReaderTest::SetUp();
    info.walMetaRspCache = taosMemoryCalloc(2, sizeof(int64_t));
    ASSERT_NE(nullptr, info.walMetaRspCache);
    info.walMetaRspCacheLen = 2 * sizeof(int64_t);
    info.walMetaRspCacheCode = TSDB_CODE_STREAM_NO_DATA;
    info.walMetaRspCacheType = STRIGGER_PULL_WAL_META_NEW;
    info.walMetaRspCacheSessionId = req.base.sessionId;
    info.walMetaRspCacheRound = req.timeAlignDataReq.roundId;
  }

  void TearDown() override {
    TimeAlignDataReaderTest::TearDown();
    taosMemoryFree(info.walMetaRspCache);
    blockDataDestroy(static_cast<SSDataBlock*>(info.metaBlock));
    blockDataDestroy(info.triggerBlock);
  }

  int32_t poll(uint64_t id, int64_t cursor) {
    const auto old = req;
    req.walMetaNewReq = {};
    req.base = old.base;
    req.base.timeAlignWalRound = old.timeAlignDataReq.roundId;
    req.base.timeAlignWalRequest = id;
    req.walMetaNewReq.lastVer = cursor;
    req.walMetaNewReq.withGroupColVals = true;
    req.walMetaNewReq.requestId = id;
    info.walMetaRspCacheRequestId = id;
    int32_t code = pull(STRIGGER_PULL_WAL_META_NEW);
    req = old;
    return code;
  }
};

// Encode a real submit so the range scan pays for the unrelated rows it reads.
void rangeSubmit(int64_t uid, size_t rows, std::vector<char>* body) {
  SRow row = {};
  row.len = sizeof(row);
  row.ts = 20;
  SRow*         ptr = &row;
  SSubmitTbData table = {};
  table.uid = uid;
  table.aRowP = taosArrayInit(rows, sizeof(ptr));
  ASSERT_NE(nullptr, table.aRowP);
  for (size_t i = 0; i < rows; ++i) ASSERT_NE(nullptr, taosArrayPush(table.aRowP, &ptr));
  SSubmitReq2 submit = {};
  submit.aSubmitTbData = taosArrayFromList(&table, 1, sizeof(table));
  ASSERT_NE(nullptr, submit.aSubmitTbData);
  int32_t size = 0, code = 0;
  tEncodeSize(tEncodeSubmitReq, &submit, size, code);
  ASSERT_EQ(0, code);
  body->resize(sizeof(SSubmitReq2Msg) + size);
  SEncoder encoder = {};
  tEncoderInit(&encoder, reinterpret_cast<uint8_t*>(body->data()) + sizeof(SSubmitReq2Msg), size);
  EXPECT_EQ(0, tEncodeSubmitReq(&encoder, &submit));
  tEncoderClear(&encoder);
  taosArrayDestroy(submit.aSubmitTbData);
  taosArrayDestroy(table.aRowP);
}

TEST_F(TimeAlignWalReaderTest, RangeChecksYieldBetweenLargeUnselectedSubmitsAndDiscardRetiredCuts) {
  const uint64_t liveRound = req.timeAlignDataReq.roundId;
  req.base.sessionId = STREAM_TRIGGER_HISTORY_SESSIONID;
  ++req.timeAlignDataReq.roundId;
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  std::vector<char> large, oversized, matched;
  rangeSubmit(8, (1U << 20) / sizeof(SRow), &large);
  rangeSubmit(8, (4U << 20) / sizeof(SRow), &oversized);
  rangeSubmit(7, 1, &matched);
  static std::function<const std::vector<char>&(int64_t)> record;
  static std::function<void()>                            duringRead;
  static int                                              bodies;
  bodies = 0;
  record = [&](int64_t ver) -> const std::vector<char>& { return ver == 7 ? matched : ver == 6 ? oversized : large; };
  stub.set(
      walFetchHead, +[](SWalReader* wal, int64_t ver) -> int32_t {
        memset(&wal->pHead->head, 0, sizeof(SWalCont));
        wal->pHead->head.version = ver;
        wal->pHead->head.msgType = TDMT_VND_SUBMIT;
        wal->pHead->head.bodyLen = record(ver).size();
        return 0;
      });
  stub.set(
      walFetchBody, +[](SWalReader* wal) -> int32_t {
        ++bodies;
        if (duringRead) duringRead();
        const auto& body = record(wal->pHead->head.version);
        auto*       head = static_cast<SWalCkHead*>(taosMemoryRealloc(wal->pHead, sizeof(SWalCkHead) + body.size()));
        if (!head) return terrno;
        wal->pHead = head;
        memcpy(head->head.body, body.data(), body.size());
        return 0;
      });
  SSTriggerPullRequestUnion page = {};
  page.base = req.base;
  page.base.type = STRIGGER_PULL_TIME_ALIGN_WAL_RANGE;
  page.walRangeReq.roundId = req.timeAlignDataReq.roundId;
  page.walRangeReq.throughTs = 20;
  int64_t uid = 7;
  page.walRangeReq.uids = taosArrayFromList(&uid, 1, sizeof(uid));
  ASSERT_NE(nullptr, page.walRangeReq.uids);
  page.walRangeReq.firstVer = 1;
  for (int64_t next : {4, 6, 7, 8}) {
    bodies = 0;
    ASSERT_EQ(0, historyPull(page));
    SSTriggerTimeAlignWalRangeRsp rsp = {};
    ASSERT_EQ(0, tDeserializeSTriggerTimeAlignWalRangeRsp(responseWire.data(), responseWire.size(), &rsp));
    EXPECT_EQ(page.walRangeReq.roundId, rsp.roundId);
    EXPECT_EQ(page.walRangeReq.firstVer, rsp.firstVer);
    EXPECT_EQ(next, rsp.nextVer);
    EXPECT_EQ(next - rsp.firstVer, bodies);
    EXPECT_EQ(next == 8, rsp.changed);
    const auto original = responseWire;
    ASSERT_EQ(0, historyPull(page));
    EXPECT_EQ(original, responseWire);
    page.walRangeReq.firstVer = next;
  }
  page.walRangeReq.firstVer = 6;
  duringRead = [&] {
    auto control = std::async(std::launch::async, [&] {
      ASSERT_EQ(0, taosWTryLockLatch(&info.timeAlignScanLock));
      taosWUnLockLatch(&info.timeAlignScanLock);
      SSTriggerPullRequestUnion check = {};
      check.base = req.base;
      check.base.sessionId = STREAM_TRIGGER_REALTIME_SESSIONID;
      check.base.type = STRIGGER_PULL_TIME_ALIGN_VALIDATE;
      check.timeAlignRoundReq.roundId = liveRound;
      EXPECT_EQ(0, historyPull(check));
      check.base = page.base;
      check.base.type = STRIGGER_PULL_TIME_ALIGN_RELEASE;
      check.timeAlignRoundReq.roundId = page.walRangeReq.roundId;
      EXPECT_EQ(0, historyPull(check));
      EXPECT_EQ(0, taosHashGetSize(vnode.pWal->pRefHash));
    });
    control.get();
    pullType = page.base.type;
    responseCount = 0;
  };
  EXPECT_EQ(TSDB_CODE_STREAM_EXEC_CANCELLED, historyPull(page));
  EXPECT_TRUE(responseWire.empty());
  EXPECT_EQ(0, taosHashGetSize(vnode.pWal->pRefHash));
  taosArrayDestroy(page.walRangeReq.uids);
  record = {};
  duringRead = {};
}

TEST_F(TimeAlignWalReaderTest, BoundaryDoesNotRetainWal) {
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_RELEASE));
  ++req.timeAlignDataReq.roundId;
  vnode.pWal->vers.firstVer = vnode.state.applied + 2;
  EXPECT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  EXPECT_EQ(vnode.state.applied + 1, nextCursor);
  EXPECT_EQ(0, taosHashGetSize(vnode.pWal->pRefHash));
}

TEST_F(TimeAlignWalReaderTest, EmptyAlignedPageKeepsItsExactNextCursor) {
  ASSERT_EQ(0, createDataBlock(&info.triggerBlock));
  SSTriggerPullRequestUnion page = {};
  page.base = req.base;
  page.base.type = STRIGGER_PULL_WAL_META_DATA_NEW;
  page.base.timeAlignWalRound = req.timeAlignDataReq.roundId;
  for (int64_t cursor : {0, 11}) {
    ++page.base.timeAlignWalRequest;
    page.walMetaDataNewReq.lastVer = cursor;
    page.walMetaDataNewReq.endVer = cursor ? cursor : -1;
    ASSERT_EQ(0, historyPull(page));
    EXPECT_EQ(TSDB_CODE_STREAM_NO_DATA, responseCode);
    EXPECT_EQ(cursor, nextCursor);
    EXPECT_EQ(sizeof(cursor), responseWire.size());
    EXPECT_EQ(0, taosHashGetSize(vnode.pWal->pRefHash));
  }
}

TEST_F(TimeAlignWalReaderTest, MetadataRetryRetainsNoWal) {
  ASSERT_EQ(0, poll(1, 21));
  ASSERT_EQ(0, poll(1, 21));
  ASSERT_EQ(0, poll(2, 21));
  EXPECT_EQ(TSDB_CODE_STREAM_EXEC_CANCELLED, poll(1, 21));
  EXPECT_EQ(0, taosHashGetSize(vnode.pWal->pRefHash));
}

// A virtual Reader starts with no SET_TABLE state after redeployment.
TEST_F(TimeAlignDataDispatchTest, HistoryBoundaryRequiresSetTableOnRestart) {
  info.vTableReady = 0;
  req.base.sessionId = STREAM_TRIGGER_HISTORY_SESSIONID;
  EXPECT_EQ(TSDB_CODE_STREAM_TASK_NOT_EXIST, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  EXPECT_EQ(0, taosHashGetSize(info.pTimeAlignRounds));
}

TEST_F(TimeAlignDataReaderTest, ValidationAfterReaderRestartRequiresSetTableInBothSessions) {
  SStreamTriggerReaderInfo restarted = {};
  restarted.pTask = &task;
  restarted.isVtableStream = 1;
  restarted.timeAlignConsumer = true;
  for (int64_t session : {STREAM_TRIGGER_REALTIME_SESSIONID, STREAM_TRIGGER_HISTORY_SESSIONID}) {
    SCOPED_TRACE(session);
    req.base.sessionId = session;
    ++req.timeAlignDataReq.roundId;
    ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
    ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_VALIDATE));
    activeReader = &restarted;
    EXPECT_EQ(TSDB_CODE_STREAM_TASK_NOT_EXIST, round(STRIGGER_PULL_TIME_ALIGN_VALIDATE));
    activeReader = &info;
  }
}

TEST_F(TimeAlignDataReaderTest, SourceAndProjectedColumnsMustBelongToTheTrigger) {
  req.timeAlignDataReq.uid++;
  EXPECT_EQ(TSDB_CODE_STREAM_NO_CONTEXT, pull());
  req.timeAlignDataReq.uid--;
  req.timeAlignDataReq.suid++;
  EXPECT_EQ(TSDB_CODE_STREAM_NO_CONTEXT, pull());
  req.timeAlignDataReq.suid--;
  col_id_t other = 5;
  ASSERT_NE(nullptr, taosArrayPush(req.timeAlignDataReq.cids, &other));
  EXPECT_EQ(TSDB_CODE_STREAM_NO_CONTEXT, pull());
  EXPECT_EQ(0, opened);
}

TEST_F(TimeAlignDataReaderTest, MissingProjectedColumnOrTableRequiresANewBinding) {
  metaError = TSDB_CODE_PAR_TABLE_NOT_EXIST;
  EXPECT_EQ(TSDB_CODE_STREAM_NO_CONTEXT, pull());
  metaError = TSDB_CODE_OUT_OF_MEMORY;
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, pull());
  metaError = 0;
  columns[2].colId = 4;
  EXPECT_EQ(TSDB_CODE_STREAM_NO_CONTEXT, pull());
  EXPECT_EQ(0, opened);
  EXPECT_EQ(0, taosHashGetSize(info.pTimeAlignScans));
  columns[2].colId = 3;
  ASSERT_EQ(0, pull());
}

TEST_F(TimeAlignDataReaderTest, DescendingSeedIncludesSubmillisecondRowsBelowItsExclusiveBodyBoundary) {
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_RELEASE));
  ++req.timeAlignDataReq.roundId;
  info.physicalWalCells = true;
  info.timeAlignPrecision = TSDB_TIME_PRECISION_MILLI + 1;
  vnode.config.tsdbCfg.precision = TSDB_TIME_PRECISION_NANO;
  rows = {{26000001, 99, 0, false, true}, {25999999, 7, 0, false, true}, {24999999, 3, 0, false, true}};
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  auto checkBoundary = [](int64_t expected) {
    SSDataBlock boundary{};
    ASSERT_EQ(0, tDeserializeSStreamTsResponse(responseWire.data(), responseWire.size(), &boundary));
    ASSERT_EQ(1, boundary.info.rows);
    auto* ts = static_cast<SColumnInfoData*>(taosArrayGet(boundary.pDataBlock, 1));
    EXPECT_EQ(expected, *reinterpret_cast<const int64_t*>(colDataGetData(ts, 0)));
    blockDataFreeRes(&boundary);
  };
  checkBoundary(26);
  SSTriggerPullRequestUnion first{};
  first.base = req.base;
  first.base.type = STRIGGER_PULL_FIRST_TS;
  first.firstTsReq.ver = vnode.state.applied;
  first.firstTsReq.roundId = req.timeAlignDataReq.roundId;
  first.firstTsReq.startTime = 25;
  ASSERT_EQ(0, historyPull(first));
  checkBoundary(25);
  req.timeAlignDataReq.skey = req.timeAlignDataReq.ekey = 25;
  ASSERT_EQ(0, pull());
  ASSERT_NE(nullptr, response.pBlock);
  ASSERT_EQ(1, response.pBlock->info.rows);
  const auto* ts = static_cast<const SColumnInfoData*>(taosArrayGet(response.pBlock->pDataBlock, 0));
  const auto* v = static_cast<const SColumnInfoData*>(taosArrayGet(response.pBlock->pDataBlock, 2));
  const auto* native = static_cast<const SColumnInfoData*>(taosArrayGetLast(response.pBlock->pDataBlock));
  ASSERT_EQ(STREAM_WAL_PHYSICAL_TS_COL_ID, native->info.colId);
  EXPECT_EQ(25999999, *reinterpret_cast<const int64_t*>(colDataGetData(native, 0)));
  EXPECT_EQ(25, *reinterpret_cast<const int64_t*>(colDataGetData(ts, 0)));
  EXPECT_EQ(7, *reinterpret_cast<const int32_t*>(colDataGetData(v, 0)));
}

TEST_F(TimeAlignDataReaderTest, TimestampPayloadPreservesNullAndSentinelsWhenRescaled) {
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_RELEASE));
  ++req.timeAlignDataReq.roundId;
  info.timeAlignPrecision = TSDB_TIME_PRECISION_MICRO + 1;
  vnode.config.tsdbCfg.precision = TSDB_TIME_PRECISION_NANO;
  columns[1].type = TSDB_DATA_TYPE_TIMESTAMP;
  columns[1].bytes = sizeof(int64_t);
  rows = {{5000, 0, 0, false, false, TSKEY_MIN},
          {4000, 0, 0, false, false, TSKEY_MAX},
          {3000, 0, 0, true, false, 12345},
          {2000, 0, 0, false, false, -4500}};
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  ASSERT_EQ(0, pull());
  ASSERT_NE(nullptr, response.pBlock);
  ASSERT_EQ(4, response.pBlock->info.rows);
  const auto* sampled = static_cast<const SColumnInfoData*>(taosArrayGet(response.pBlock->pDataBlock, 2));
  EXPECT_EQ(TSKEY_MIN, *reinterpret_cast<const int64_t*>(colDataGetData(sampled, 0)));
  EXPECT_EQ(TSKEY_MAX, *reinterpret_cast<const int64_t*>(colDataGetData(sampled, 1)));
  EXPECT_TRUE(colDataIsNull_s(sampled, 2));
  EXPECT_EQ(-4, *reinterpret_cast<const int64_t*>(colDataGetData(sampled, 3)));
}

TEST_F(TimeAlignDataReaderTest, RawProjectionPreservesAliasOrderForBothReaders) {
  col_id_t alias = 3;
  ASSERT_NE(nullptr, taosArrayPush(req.timeAlignDataReq.cids, &alias));
  auto check = [](const SSDataBlock& block) {
    ASSERT_EQ(2, block.info.rows);
    ASSERT_EQ(4, taosArrayGetSize(block.pDataBlock));
    auto column = [&](int slot) { return static_cast<SColumnInfoData*>(taosArrayGet(block.pDataBlock, slot)); };
    EXPECT_EQ(20, *reinterpret_cast<int64_t*>(colDataGetData(column(0), 0)));
    EXPECT_EQ(7, *reinterpret_cast<int32_t*>(colDataGetData(column(2), 0)));
    for (int slot : {1, 3}) {
      EXPECT_TRUE(colDataIsNull_s(column(slot), 0));
      EXPECT_EQ(2, *reinterpret_cast<int32_t*>(colDataGetData(column(slot), 1)));
    }
  };
  ASSERT_EQ(0, pull());
  ASSERT_NE(nullptr, response.pBlock);
  check(*response.pBlock);
  const auto firstPage = responseWire;
  const int  reads = advances;
  ASSERT_EQ(0, pull());
  EXPECT_EQ(firstPage, responseWire);
  EXPECT_EQ(reads, advances);

  // A replicated supertable can retain cids in a different order. Its child inherits that schema.
  std::swap(columns[1], columns[2]);
  info.storageApi.metaReaderFn.getTableEntryByUid = +[](SMetaReader* reader, tb_uid_t uid) -> int32_t {
    if (uid == 7) {
      reader->me.type = TD_CHILD_TABLE;
      reader->me.ctbEntry.suid = 9;
    } else {
      EXPECT_EQ(9, uid);
      reader->me.type = TD_SUPER_TABLE;
      reader->me.stbEntry.schemaRow.nCols = columns.size();
      reader->me.stbEntry.schemaRow.pSchema = columns.data();
    }
    return 0;
  };
  info.storageApi.tsdReader.tsdReaderOpen = openCursor;
  SSTriggerPullRequestUnion legacy{};
  legacy.base = req.base;
  legacy.base.type = STRIGGER_PULL_TSDB_DATA;
  legacy.tsdbDataReq.suid = 9;
  legacy.tsdbDataReq.uid = req.timeAlignDataReq.uid;
  legacy.tsdbDataReq.cids = req.timeAlignDataReq.cids;
  legacy.tsdbDataReq.skey = req.timeAlignDataReq.skey;
  legacy.tsdbDataReq.ekey = req.timeAlignDataReq.ekey;
  legacy.tsdbDataReq.order = req.timeAlignDataReq.order;
  legacy.tsdbDataReq.ver = req.timeAlignDataReq.ver;
  ASSERT_EQ(0, historyPull(legacy));
  SSDataBlock block{};
  const char* end = nullptr;
  ASSERT_FALSE(responseWire.empty());
  ASSERT_EQ(0, blockDecode(&block, responseWire.data(), &end));
  check(block);
  blockDataFreeRes(&block);
  columns.pop_back();
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, historyPull(legacy));
}

TEST_F(TimeAlignDataReaderTest, WideRawProjectionBoundsColumnComparisons) {
  const int     width = TSDB_MAX_COLUMNS_NON_VIRTUAL;
  const int64_t key[2] = {0, 7};
  auto*         bindings = static_cast<SSHashObj**>(tSimpleHashGet(info.uidHashTrigger, key, sizeof(key)));
  ASSERT_NE(nullptr, bindings);
  columns.resize(width, columns.back());
  taosArrayClear(req.timeAlignDataReq.cids);
  for (int slot = 0; slot < width; ++slot) {
    col_id_t cid = 1 + 2 * slot;
    columns[slot].colId = cid;
    snprintf(columns[slot].name, sizeof(columns[slot].name), "c%d", cid);
    if (slot) {
      col_id_t virtualCid = slot + 1;
      ASSERT_EQ(0, tSimpleHashPut(*bindings, &virtualCid, sizeof(virtualCid), &cid, sizeof(cid)));
    }
    // An odd stride permutes all 4096 column ids and keeps the primary timestamp first.
    col_id_t projected = 1 + 2 * ((slot * 2053) % width);
    ASSERT_NE(nullptr, taosArrayPush(req.timeAlignDataReq.cids, &projected));
  }
  ++req.timeAlignDataReq.roundId;
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  static int comparisons;
  comparisons = 0;
  stub.set(
      compareInt16Val, +[](const void* left, const void* right) -> int32_t {
        ++comparisons;
        return *static_cast<const col_id_t*>(left) - *static_cast<const col_id_t*>(right);
      });
  ASSERT_EQ(0, pull());
  RecordProperty("raw_scan_column_comparisons", comparisons);
  EXPECT_GT(comparisons, width);
  EXPECT_LT(comparisons, 32 * width);
  ASSERT_NE(nullptr, response.pBlock);
  ASSERT_EQ(width, taosArrayGetSize(response.pBlock->pDataBlock));
  for (int slot = 0; slot < width; ++slot) {
    auto* col = static_cast<const SColumnInfoData*>(taosArrayGet(response.pBlock->pDataBlock, slot));
    EXPECT_EQ(*static_cast<const col_id_t*>(taosArrayGet(req.timeAlignDataReq.cids, slot)), col->info.colId);
  }
}

TEST_F(TimeAlignDataReaderTest, UnalignedDeploymentCannotOpenRawCursor) {
  info.timeAlignConsumer = false;
  EXPECT_EQ(TSDB_CODE_STREAM_NO_CONTEXT, pull());
  EXPECT_EQ(0, opened);
}

TEST_F(TimeAlignDataReaderTest, PeriodMetadataAdmissionCannotOpenRawCursor) {
  info.timeAlignConsumer = false;
  info.requireLocalVtableRefs = true;
  EXPECT_EQ(TSDB_CODE_STREAM_NO_CONTEXT, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  EXPECT_EQ(TSDB_CODE_STREAM_NO_CONTEXT, pull());
  EXPECT_EQ(0, opened);
}

TEST_F(TimeAlignDataReaderTest, RetrySameBatchReturnsSameRows) {
  longNullRun();
  ASSERT_EQ(0, pull());
  EXPECT_FALSE(response.done);
  ASSERT_NE(nullptr, response.pBlock);
  auto first = responseWire;
  int  calls = advances;
  ASSERT_EQ(0, pull());
  EXPECT_EQ(calls, advances);
  EXPECT_EQ(first, responseWire);
  ASSERT_EQ(0, pull(STRIGGER_PULL_TIME_ALIGN_DATA_NEXT, 1));
  EXPECT_TRUE(response.done);
  auto last = responseWire;
  calls = advances;
  ASSERT_EQ(0, pull(STRIGGER_PULL_TIME_ALIGN_DATA_NEXT, 1));
  EXPECT_EQ(calls, advances);
  EXPECT_EQ(last, responseWire);
  EXPECT_EQ(1, closed);
  ASSERT_NE(0, pull(STRIGGER_PULL_TIME_ALIGN_DATA_NEXT, 3));
}

TEST_F(TimeAlignDataReaderTest, ClosingAndReopeningScanDuringDataReadDiscardsOnlyTheOldPage) {
  const uint64_t liveRound = req.timeAlignDataReq.roundId;
  req.base.sessionId = STREAM_TRIGGER_HISTORY_SESSIONID;
  ++req.timeAlignDataReq.roundId;
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  static std::function<void()> control;
  bool                         paused = false;
  control = [&] {
    if (paused) return;
    paused = true;
    auto task = std::async(std::launch::async, [&] {
      ASSERT_EQ(0, taosWTryLockLatch(&info.timeAlignScanLock));
      taosWUnLockLatch(&info.timeAlignScanLock);
      SSTriggerPullRequestUnion live{};
      live.base = req.base;
      live.base.type = STRIGGER_PULL_TIME_ALIGN_VALIDATE;
      live.base.sessionId = STREAM_TRIGGER_REALTIME_SESSIONID;
      live.timeAlignRoundReq.roundId = liveRound;
      EXPECT_EQ(0, historyPull(live));
      EXPECT_EQ(0, pull(STRIGGER_PULL_TIME_ALIGN_DATA_CLOSE));
      EXPECT_EQ(0, pull());
    });
    task.get();
    pullType = STRIGGER_PULL_TIME_ALIGN_DATA;
    responseCount = 0;
  };
  info.storageApi.tsdReader.tsdNextDataBlock = +[](void* cursor, bool* more) {
    control();
    return nextBlock(cursor, more);
  };
  EXPECT_EQ(TSDB_CODE_STREAM_EXEC_CANCELLED, pull());
  EXPECT_TRUE(paused);
  EXPECT_TRUE(responseWire.empty());
  int before = advances;
  ASSERT_EQ(0, pull());
  EXPECT_EQ(before, advances);
  EXPECT_NE(nullptr, response.pBlock);
  control = {};
}

TEST_F(TimeAlignDataReaderTest, ConcurrentSameUidScansStayIsolated) {
  longNullRun();
  for (uint64_t id : {1, 2, 3}) {
    req.timeAlignDataReq.scanId = id;
    ASSERT_EQ(0, pull());
    ASSERT_NE(nullptr, response.pBlock);
    EXPECT_EQ(STREAM_RETURN_ROWS_NUM, response.pBlock->info.rows);
    EXPECT_EQ(id, response.scanId);
  }
  EXPECT_EQ(3, taosHashGetSize(info.pTimeAlignScans));
  for (uint64_t id : {2, 3, 1}) {
    req.timeAlignDataReq.scanId = id;
    ASSERT_EQ(0, pull(STRIGGER_PULL_TIME_ALIGN_DATA_NEXT, 1));
    ASSERT_NE(nullptr, response.pBlock);
    EXPECT_EQ(2, response.pBlock->info.rows);
    ASSERT_EQ(0, pull(STRIGGER_PULL_TIME_ALIGN_DATA_CLOSE));
    ASSERT_EQ(0, pull(STRIGGER_PULL_TIME_ALIGN_DATA_CLOSE));
  }
  EXPECT_EQ(0, taosHashGetSize(info.pTimeAlignScans));
}

TEST_F(TimeAlignDataReaderTest, HistoryIndexUsesRoundVersionAndReplaysMetadataPages) {
  req.base.sessionId = STREAM_TRIGGER_HISTORY_SESSIONID;
  ++req.timeAlignDataReq.roundId;
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  ++vnode.state.applied;
  SSTriggerPullRequestUnion pullReq{};
  pullReq.firstTsReq.base = req.base;
  pullReq.base.type = STRIGGER_PULL_FIRST_TS;
  pullReq.firstTsReq.ver = 10;
  pullReq.firstTsReq.roundId = req.timeAlignDataReq.roundId;
  pullReq.firstTsReq.startTime = 15;
  ASSERT_EQ(0, historyPull(pullReq));
  SSDataBlock first{};
  ASSERT_EQ(0, tDeserializeSStreamTsResponse(responseWire.data(), responseWire.size(), &first));
  ASSERT_EQ(1, first.info.rows);
  EXPECT_EQ(20, *reinterpret_cast<int64_t*>(
                    colDataGetData(static_cast<SColumnInfoData*>(taosArrayGet(first.pDataBlock, 1)), 0)));
  blockDataFreeRes(&first);

  pullReq = {};
  pullReq.tsdbMetaReq.base = req.base;
  pullReq.base.type = STRIGGER_PULL_TSDB_META;
  pullReq.tsdbMetaReq.roundId = req.timeAlignDataReq.roundId;
  pullReq.tsdbMetaReq.requestId = 7;
  pullReq.tsdbMetaReq.order = TSDB_ORDER_ASC;
  pullReq.tsdbMetaReq.startTime = 15;
  pullReq.tsdbMetaReq.endTime = 25;
  pullReq.tsdbMetaReq.ver = 10;
  ASSERT_EQ(0, historyPull(pullReq));
  EXPECT_EQ(10, capturedVersion);
  auto        wire = responseWire;
  const char* end = nullptr;
  SSDataBlock meta{};
  ASSERT_EQ(0, blockDecodeBounded(&meta, wire.data(), wire.size(), &end));
  ASSERT_EQ(1, meta.info.rows);
  for (int col = 0; col < 4; ++col) {
    auto*   info = static_cast<SColumnInfoData*>(taosArrayGet(meta.pDataBlock, col));
    int64_t expected[] = {20, 20, 7, 1};
    EXPECT_EQ(expected[col], *reinterpret_cast<int64_t*>(colDataGetData(info, 0)));
  }
  blockDataFreeRes(&meta);
  int count = advances;
  ++pullReq.tsdbMetaReq.ver;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, historyPull(pullReq));
  --pullReq.tsdbMetaReq.ver;
  ASSERT_EQ(0, historyPull(pullReq));
  EXPECT_EQ(wire, responseWire);
  EXPECT_EQ(count, advances);
  pullReq.base.type = STRIGGER_PULL_TSDB_META_NEXT;
  ++pullReq.tsdbMetaReq.requestId;
  ASSERT_EQ(0, historyPull(pullReq));
  EXPECT_EQ(count, advances);
  EXPECT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_RELEASE));
  EXPECT_EQ(TSDB_CODE_STREAM_EXEC_CANCELLED, historyPull(pullReq));
}

TEST_F(TimeAlignDataReaderTest, EmptyHistorySourcesAllowConcurrentValidationAndRelease) {
  constexpr int sourceCount = 128;
  const auto    liveRound = req.timeAlignDataReq.roundId;
  int64_t       key[2] = {0, 7};
  auto*         projection = static_cast<SSHashObj**>(tSimpleHashGet(info.uidHashTrigger, key, sizeof(key)));
  ASSERT_NE(nullptr, projection);
  SSHashObj* columns = *projection;
  for (int i = 1; i < sourceCount; ++i) {
    key[1] = 7 + i;
    ASSERT_EQ(0, tSimpleHashPut(info.uidHashTrigger, key, sizeof(key), &columns, sizeof(columns)));
  }
  req.base.sessionId = STREAM_TRIGGER_HISTORY_SESSIONID;
  ++req.timeAlignDataReq.roundId;
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));

  SSTriggerPullRequestUnion meta = {};
  meta.base = req.base;
  meta.base.type = STRIGGER_PULL_TSDB_META;
  meta.tsdbMetaReq.roundId = req.timeAlignDataReq.roundId;
  meta.tsdbMetaReq.order = TSDB_ORDER_ASC;
  meta.tsdbMetaReq.startTime = 15;
  meta.tsdbMetaReq.endTime = 25;
  meta.tsdbMetaReq.ver = vnode.state.applied;
  static std::function<int32_t(void*, bool*)> readSource;
  info.storageApi.tsdReader.tsdNextDataBlock = +[](void* cursor, bool* more) { return readSource(cursor, more); };
  for (bool retire : {false, true}) {
    int before = opened;
    readSource = [&](void* cursor, bool* more) -> int32_t {
      if (opened - before == sourceCount / 2) {
        auto control = std::async(std::launch::async, [&] {
          // Fail immediately if storage still owns the Reader latch.
          ASSERT_EQ(0, taosWTryLockLatch(&info.timeAlignScanLock));
          taosWUnLockLatch(&info.timeAlignScanLock);
          SSTriggerPullRequestUnion check = {};
          check.base = meta.base;
          check.base.sessionId = STREAM_TRIGGER_REALTIME_SESSIONID;
          check.base.type = STRIGGER_PULL_TIME_ALIGN_VALIDATE;
          check.timeAlignRoundReq.roundId = liveRound;
          EXPECT_EQ(0, historyPull(check));
          if (retire) {
            check.base = meta.base;
            check.base.type = STRIGGER_PULL_TIME_ALIGN_RELEASE;
            check.timeAlignRoundReq.roundId = meta.tsdbMetaReq.roundId;
            EXPECT_EQ(0, historyPull(check));
          }
        });
        control.get();
        pullType = meta.base.type;
        responseCount = 0;
      }
      if (opened - before == sourceCount) return nextBlock(cursor, more);
      *more = false;
      return 0;
    };
    ++meta.tsdbMetaReq.requestId;
    EXPECT_EQ(retire ? TSDB_CODE_STREAM_EXEC_CANCELLED : 0, historyPull(meta));
    EXPECT_EQ(retire ? sourceCount / 2 : sourceCount, opened - before);
    if (retire) {
      EXPECT_TRUE(responseWire.empty());
    } else {
      SSDataBlock block = {};
      const char* end = nullptr;
      ASSERT_EQ(0, blockDecodeBounded(&block, responseWire.data(), responseWire.size(), &end));
      ASSERT_EQ(1, block.info.rows);
      const auto* uid = static_cast<SColumnInfoData*>(taosArrayGet(block.pDataBlock, 2));
      EXPECT_EQ(7 + sourceCount - 1, *reinterpret_cast<const int64_t*>(colDataGetData(uid, 0)));
      blockDataFreeRes(&block);
    }
  }
  readSource = {};
}

TEST_F(TimeAlignDataReaderTest, FailedHistoryPageClosesStorageCursorBeforeRetry) {
  req.base.sessionId = STREAM_TRIGGER_HISTORY_SESSIONID;
  ++req.timeAlignDataReq.roundId;
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  SSTriggerPullRequestUnion meta{};
  meta.tsdbMetaReq.base = req.base;
  meta.base.type = STRIGGER_PULL_TSDB_META;
  meta.tsdbMetaReq.roundId = req.timeAlignDataReq.roundId;
  meta.tsdbMetaReq.requestId = 7;
  meta.tsdbMetaReq.order = TSDB_ORDER_ASC;
  meta.tsdbMetaReq.startTime = 15;
  meta.tsdbMetaReq.endTime = 25;
  meta.tsdbMetaReq.ver = 10;
  info.storageApi.tsdReader.tsdReaderReleaseDataBlock =
      +[](void*) { ADD_FAILURE() << "NEXT failed before a block was acquired"; };
  readError = TSDB_CODE_FAILED;
  EXPECT_EQ(readError, historyPull(meta));
  EXPECT_EQ(1, opened);
  EXPECT_EQ(1, closed);
  const int failed = advances;
  EXPECT_EQ(readError, historyPull(meta));
  EXPECT_EQ(failed, advances);
}

TEST_F(TimeAlignDataReaderTest, NewHistoryRoundReadsUpdatedPredecessor) {
  ASSERT_EQ(0, pull());
  auto readValue = [] {
    return *reinterpret_cast<const int32_t*>(
        colDataGetData(static_cast<SColumnInfoData*>(taosArrayGet(response.pBlock->pDataBlock, 2)), 0));
  };
  ASSERT_NE(nullptr, response.pBlock);
  ASSERT_EQ(7, readValue());
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_RELEASE));
  rows[1].a = 42;
  ++vnode.state.applied;
  ++req.timeAlignDataReq.roundId;
  req.base.sessionId = STREAM_TRIGGER_HISTORY_SESSIONID;
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  EXPECT_EQ(vnode.state.applied + 1, nextCursor);
  req.timeAlignDataReq.ver = vnode.state.applied;
  ASSERT_EQ(0, pull());
  ASSERT_NE(nullptr, response.pBlock);
  EXPECT_EQ(vnode.state.applied, capturedVersion);
  EXPECT_EQ(42, readValue());
}

TEST_F(TimeAlignDataReaderTest, RealtimeAndHistoryBoundsIdentifyTheSameReader) {
  uint64_t    live = 0, history = 0;
  SSDataBlock block{};
  ASSERT_EQ(0, tDeserializeTimeAlignBoundRsp(responseWire.data(), responseWire.size(), &block, &live));
  blockDataFreeRes(&block);
  ASSERT_NE(0u, live);
  ++req.timeAlignDataReq.roundId;
  req.base.sessionId = STREAM_TRIGGER_HISTORY_SESSIONID;
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  block = {};
  ASSERT_EQ(0, tDeserializeTimeAlignBoundRsp(responseWire.data(), responseWire.size(), &block, &history));
  blockDataFreeRes(&block);
  EXPECT_EQ(live, history);
}

TEST_F(TimeAlignDataReaderTest, ReleaseDuringBoundaryReadDiscardsTheReply) {
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_RELEASE));
  for (int session : {STREAM_TRIGGER_REALTIME_SESSIONID, STREAM_TRIGGER_HISTORY_SESSIONID}) {
    req.base.sessionId = session;
    ++req.timeAlignDataReq.roundId;
    if (session == STREAM_TRIGGER_HISTORY_SESSIONID) ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
    SSTriggerPullRequestUnion retire = {};
    retire.base = req.base;
    retire.base.type = STRIGGER_PULL_TIME_ALIGN_RELEASE;
    retire.timeAlignRoundReq.roundId = req.timeAlignDataReq.roundId;
    static std::function<void()> release;
    release = [&] {
      ASSERT_EQ(0, taosWTryLockLatch(&info.timeAlignScanLock));
      taosWUnLockLatch(&info.timeAlignScanLock);
      auto outerType = pullType;
      EXPECT_EQ(0, historyPull(retire));
      pullType = outerType;
      responseCount = 0;
    };
    info.storageApi.tsdReader.tsdNextFirstLastTsBlock = +[](void*, SSDataBlock*, bool* more) -> int32_t {
      release();
      *more = false;
      return 0;
    };
    if (session == STREAM_TRIGGER_REALTIME_SESSIONID) {
      EXPECT_EQ(TSDB_CODE_STREAM_EXEC_CANCELLED, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
    } else {
      SSTriggerPullRequestUnion first = {};
      first.base = req.base;
      first.base.type = STRIGGER_PULL_FIRST_TS;
      first.firstTsReq.roundId = req.timeAlignDataReq.roundId;
      first.firstTsReq.ver = vnode.state.applied;
      EXPECT_EQ(TSDB_CODE_STREAM_EXEC_CANCELLED, historyPull(first));
    }
    EXPECT_EQ(TSDB_CODE_STREAM_EXEC_CANCELLED, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
    info.storageApi.tsdReader.tsdNextFirstLastTsBlock = nextBoundary;
    release = {};
  }
}

TEST_F(TimeAlignDataReaderTest, BoundaryRetryKeepsVersionAndReleaseIsIdempotent) {
  vnode.state.applied++;
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  EXPECT_EQ(11, nextCursor);
  longNullRun();
  ASSERT_EQ(0, pull());
  EXPECT_EQ(1, taosHashGetSize(info.pTimeAlignScans));
  EXPECT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_RELEASE));
  EXPECT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_RELEASE));
  EXPECT_EQ(TSDB_CODE_STREAM_EXEC_CANCELLED, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  EXPECT_EQ(0, taosHashGetSize(info.pTimeAlignScans));
  EXPECT_NE(0, pull(STRIGGER_PULL_TIME_ALIGN_DATA_NEXT, 1));
}

TEST_F(TimeAlignDataReaderTest, EmptySourceDoesNotOpenAnUnboundedReader) {
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_RELEASE));
  ++req.timeAlignDataReq.roundId;
  vnode.state.applied = -1;
  int calls = opened;
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  EXPECT_EQ(0, nextCursor);
  EXPECT_EQ(calls, opened);
  req.timeAlignDataReq.ver = 0;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, pull());
  EXPECT_EQ(calls, opened);
}

TEST_F(TimeAlignDataReaderTest, ZeroVersionIsNotLatest) {
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_RELEASE));
  ++req.timeAlignDataReq.roundId;
  vnode.state.applied = 0;
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  EXPECT_EQ(1, nextCursor);
  opened = closed = 0;
  req.timeAlignDataReq.ver = 0;
  ASSERT_EQ(0, pull());
  EXPECT_EQ(0, capturedVersion);
  req.timeAlignDataReq.scanId++;
  req.timeAlignDataReq.ver = -1;
  ASSERT_NE(0, pull());
  EXPECT_EQ(1, opened);
}
TEST_F(TimeAlignDataReaderTest, FailedFirstBatchRestartsWithAFreshRound) {
  info.storageApi.tsdReader.tsdReaderReleaseDataBlock =
      +[](void*) { ADD_FAILURE() << "NEXT failed before a block was acquired"; };
  readError = TSDB_CODE_FAILED;
  ASSERT_EQ(readError, pull());
  EXPECT_EQ(1, closed);
  const int failedAdvance = advances;
  readError = 0;
  EXPECT_EQ(TSDB_CODE_STREAM_EXEC_CANCELLED, pull());
  EXPECT_EQ(failedAdvance, advances);
  EXPECT_EQ(nullptr, response.pBlock);
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_RELEASE));
  ++req.timeAlignDataReq.roundId;
  ASSERT_EQ(0, round(STRIGGER_PULL_TIME_ALIGN_BOUND));
  ASSERT_EQ(0, pull());
  EXPECT_NE(nullptr, response.pBlock);
}

TEST_F(TimeAlignDataReaderTest, ReaderUndeployClosesUnacknowledgedScans) {
  longNullRun();
  ASSERT_EQ(0, pull());
  ++req.timeAlignDataReq.scanId;
  ASSERT_EQ(0, pull());
  ASSERT_EQ(2, opened - closed);
  ASSERT_EQ(0, taosHashGetSize(vnode.pWal->pRefHash));

  // Hand the two live registries to the same task destructor used on vgroup removal.
  auto* owner = static_cast<SStreamTriggerReaderInfo*>(taosMemoryCalloc(1, sizeof(SStreamTriggerReaderInfo)));
  ASSERT_NE(nullptr, owner);
  std::swap(owner->pTimeAlignScans, info.pTimeAlignScans);
  std::swap(owner->pTimeAlignRounds, info.pTimeAlignRounds);
  task.info = owner;
  task.triggerReader = 1;
  task.task.undeployCb = [](void* address) { *static_cast<SStreamReaderTask**>(address) = nullptr; };
  auto* taskPtr = &task;
  EXPECT_EQ(0, stReaderTaskUndeploy(&taskPtr, false));
  EXPECT_EQ(nullptr, taskPtr);
  EXPECT_EQ(nullptr, task.info);
  EXPECT_EQ(opened, closed);
  EXPECT_EQ(0, taosHashGetSize(vnode.pWal->pRefHash));
}

TEST_F(TimeAlignDataReaderTest, NullAcrossBatchesAndClose) {
  longNullRun();
  ASSERT_EQ(0, pull());
  auto* first = static_cast<SColumnInfoData*>(taosArrayGet(response.pBlock->pDataBlock, 1));
  EXPECT_TRUE(colDataIsNull_s(first, 0));
  EXPECT_TRUE(colDataIsNull_s(first, response.pBlock->info.rows - 1));
  req.timeAlignDataReq.ver++;
  EXPECT_NE(0, pull());
  req.timeAlignDataReq.ver--;
  req.base.sessionId++;
  EXPECT_NE(0, pull(STRIGGER_PULL_TIME_ALIGN_DATA_CLOSE));
  req.base.sessionId--;
  readError = TSDB_CODE_FAILED;
  EXPECT_EQ(readError, pull(STRIGGER_PULL_TIME_ALIGN_DATA_NEXT, 1));
  EXPECT_EQ(nullptr, response.pBlock);
  const int failedAdvance = advances;
  readError = 0;
  EXPECT_EQ(TSDB_CODE_STREAM_EXEC_CANCELLED, pull(STRIGGER_PULL_TIME_ALIGN_DATA_NEXT, 1));
  EXPECT_EQ(failedAdvance, advances);
  EXPECT_EQ(nullptr, response.pBlock);
  EXPECT_EQ(0, pull(STRIGGER_PULL_TIME_ALIGN_DATA_CLOSE));
  EXPECT_EQ(0, pull(STRIGGER_PULL_TIME_ALIGN_DATA_CLOSE));
  EXPECT_EQ(0, taosHashGetSize(info.pTimeAlignScans));
}
}  // namespace

TEST(StreamResultAdmission, RejectsTruncatedSubmitBeforeCheckingSources) {
  SVnode   vnode = {};
  SMsgHead head = {};
  SRpcMsg  msg = {};
  msg.msgType = TDMT_VND_SUBMIT;
  msg.pCont = &head;
  msg.contLen = sizeof(head);
  SQueueInfo info = {};
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, vnodeProcessQueryMsg(&vnode, &msg, &info));
}

namespace {
struct ResultAdmissionTransport {
  SStreamResultAdmission identity = {};
  SRpcMsg*               received = nullptr;
  std::vector<char>      body;
  int32_t                response = 0;
  int32_t                calls = 0;
  int32_t                queued = 0;
  SQueueInfo             queue = {};
  SVnode*                vnode = nullptr;
  SQueryAutoQWorkerPool  pool = {};
  STaosQueue*            jobs = nullptr;
  std::promise<int32_t>  result, ordinary;
  bool                   poolReady = false;

  ~ResultAdmissionTransport() {
    if (poolReady) tQueryAutoQWorkerCleanup(&pool);
    if (jobs) tQueryAutoQWorkerFreeQueue(&pool, jobs);
  }

  static void process(SQueueInfo* queue, void* item) {
    auto* state = static_cast<ResultAdmissionTransport*>(queue->ahandle);
    bool  admission = *static_cast<bool*>(item);
    taosFreeQitem(item);
    if (admission) {
      state->queue = *queue;
      state->result.set_value(vnodeProcessQueryMsg(state->vnode, state->received, queue));
    } else {
      SRpcMsg query = {};
      query.msgType = TDMT_SCH_QUERY_CONTINUE;
      state->ordinary.set_value(vnodeProcessQueryMsg(state->vnode, &query, queue));
    }
  }

  bool enqueue(bool admission) {
    void* item = nullptr;
    if (taosAllocateQitem(sizeof(bool), DEF_QITEM, 0, &item)) return false;
    *static_cast<bool*>(item) = admission;
    if (!taosWriteQitem(jobs, item)) return true;
    taosFreeQitem(item);
    return false;
  }

  static int32_t processQuery(void*, void*, SRpcMsg*, int64_t) { return 0; }

  static void resolve(void*, int32_t id, char*, char* fqdn, uint16_t* port) {
    EXPECT_EQ(5, id);
    tstrncpy(fqdn, "synthetic-source", TSDB_FQDN_LEN);
    *port = 12345;
  }

  static int32_t check(void* ctx, SEpSet* ep, SRpcMsg* req, SRpcMsg* rsp, int8_t*, int32_t) {
    auto* t = static_cast<ResultAdmissionTransport*>(ctx);
    ++t->calls;
    if (t->calls == 1) {
      // With one configured query worker, a normal query must still finish while the check is pending.
      auto done = t->ordinary.get_future();
      EXPECT_TRUE(t->enqueue(false));
      if (done.wait_for(std::chrono::seconds(3)) == std::future_status::ready)
        EXPECT_EQ(0, done.get());
      else
        ADD_FAILURE() << "normal query could not use the released execution slot";
      EXPECT_EQ(TSDB_CODE_NEED_RETRY, vnodeProcessQueryMsg(t->vnode, t->received, &t->queue));
    }
    EXPECT_NE(nullptr, t->received->pCont);
    EXPECT_EQ(TDMT_STREAM_VALIDATE_RESULT, req->msgType);
    EXPECT_EQ(1, ep->numOfEps);
    EXPECT_STREQ("synthetic-source", ep->eps[0].fqdn);
    EXPECT_EQ(12345, ep->eps[0].port);
    auto* head = static_cast<SMsgHead*>(req->pCont);
    EXPECT_EQ(SNODE_HANDLE, static_cast<int32_t>(ntohl(head->vgId)));
    EXPECT_EQ(req->contLen, static_cast<int32_t>(ntohl(head->contLen)));
    SStreamResultAdmission decoded = {};
    EXPECT_EQ(0, tDeserializeStreamResultAdmission(head + 1, req->contLen - sizeof(*head), &decoded));
    EXPECT_EQ(t->identity.calcTokenHi, decoded.calcTokenHi);
    EXPECT_EQ(t->identity.roundId, decoded.roundId);
    rpcFreeCont(req->pCont);
    rsp->msgType = TDMT_STREAM_VALIDATE_RESULT_RSP;
    rsp->code = t->response;
    return 0;
  }

  static int32_t write(void* ctx, EQueueType queue, SRpcMsg* msg) {
    auto* t = static_cast<ResultAdmissionTransport*>(ctx);
    EXPECT_EQ(t->received, msg);
    EXPECT_EQ(ctx, msg->info.handle);
    EXPECT_STREQ("synthetic-submit-user", RPC_MSG_USER(msg));
    EXPECT_EQ(WRITE_QUEUE, queue);
    EXPECT_EQ(TDMT_VND_SUBMIT, msg->msgType);
    auto* submit = static_cast<SSubmitReq2Msg*>(msg->pCont);
    EXPECT_EQ(7, submit->header.vgId);
    EXPECT_EQ(msg->contLen, submit->header.contLen);
    EXPECT_EQ(1, be64toh(submit->version));
    EXPECT_EQ(t->body, std::vector<char>(submit->data, static_cast<char*>(msg->pCont) + msg->contLen));
    ++t->queued;
    rpcFreeCont(msg->pCont);
    msg->pCont = nullptr;
    return 0;
  }
};
}  // namespace

TEST(StreamResultAdmission, RechecksEveryReceivedPayloadAndPreservesTheSubmitter) {
  Stub stub;
  stub.set(rpcSendRecvWithTimeout, ResultAdmissionTransport::check);
  stub.set(qWorkerProcessCQueryMsg, ResultAdmissionTransport::processQuery);
  ResultAdmissionTransport transport;
  transport.identity = {11, 12, 13, STREAM_TRIGGER_REALTIME_SESSIONID, 17, 0, 19, 5};
  SVnode vnode = {};
  vnode.config.vgId = 7;
  transport.pool.min = transport.pool.max = 1;
  transport.pool.name = "result-admission-test";
  ASSERT_EQ(0, tQueryAutoQWorkerInit(&transport.pool));
  transport.poolReady = true;
  transport.jobs = tQueryAutoQWorkerAllocQueue(&transport.pool, &transport, ResultAdmissionTransport::process);
  ASSERT_NE(nullptr, transport.jobs);
  transport.vnode = &vnode;
  vnode.msgCb.data = vnode.msgCb.mgmt = vnode.msgCb.clientRpc = &transport;
  vnode.msgCb.getDnodeEpFp = ResultAdmissionTransport::resolve;
  vnode.msgCb.putToQueueFp = ResultAdmissionTransport::write;
  for (int32_t code : {0, TSDB_CODE_NEED_RETRY, TSDB_CODE_STREAM_EXEC_CANCELLED, 0}) {
    // A real encoded table batch with one synthetic timestamp. Each retry receives its own RPC buffer.
    SRow row = {};
    row.len = sizeof(row);
    row.ts = transport.calls + 100;
    SRow*         rowPtr = &row;
    SSubmitTbData table = {};
    table.uid = 29;
    table.sver = 1;
    table.aRowP = taosArrayInit(1, sizeof(SRow*));
    ASSERT_NE(nullptr, taosArrayPush(table.aRowP, &rowPtr));
    SSubmitReq2 req = {};
    req.aSubmitTbData = taosArrayInit(1, sizeof(table));
    ASSERT_NE(nullptr, taosArrayPush(req.aSubmitTbData, &table));
    SEncoder enc = {};
    tEncoderInit(&enc, nullptr, 0);
    ASSERT_EQ(0, tEncodeSubmitReq(&enc, &req));
    int32_t len = sizeof(SSubmitReq2Msg) + enc.pos;
    tEncoderClear(&enc);
    void* wire = taosMemoryMalloc(len);
    ASSERT_NE(nullptr, wire);
    auto* submit = static_cast<SSubmitReq2Msg*>(wire);
    submit->header.vgId = htonl(7);
    submit->header.contLen = htonl(len);
    submit->version = htobe64(1);
    tEncoderInit(&enc, reinterpret_cast<uint8_t*>(submit->data), len - sizeof(*submit));
    ASSERT_EQ(0, tEncodeSubmitReq(&enc, &req));
    tEncoderClear(&enc);
    taosArrayDestroy(table.aRowP);
    taosArrayDestroy(req.aSubmitTbData);
    transport.body.assign(submit->data, static_cast<char*>(wire) + len);
    ASSERT_EQ(0, tAttachStreamResultAdmission(&wire, &len, &transport.identity));
    SRpcMsg rpc = {};
    rpc.msgType = TDMT_VND_SUBMIT;
    rpc.info.handle = &transport;
    tstrncpy(RPC_MSG_USER(&rpc), "synthetic-submit-user", TSDB_USER_LEN);
    rpc.pCont = rpcMallocCont(len);
    ASSERT_NE(nullptr, rpc.pCont);
    memcpy(rpc.pCont, wire, len);
    taosMemoryFree(wire);
    rpc.contLen = len;
    // vmPutMsgToQueue converts the outer header once on arrival.
    auto* head = static_cast<SMsgHead*>(rpc.pCont);
    head->contLen = ntohl(head->contLen);
    head->vgId = ntohl(head->vgId);
    transport.received = &rpc;
    transport.response = code;
    int32_t before = transport.queued;
    transport.result = std::promise<int32_t>();
    auto done = transport.result.get_future();
    ASSERT_TRUE(transport.enqueue(true));
    EXPECT_EQ(code, done.get());
    EXPECT_EQ(before + (code == 0 ? 1 : 0), transport.queued);
    rpcFreeCont(rpc.pCont);
    if (code == TSDB_CODE_STREAM_EXEC_CANCELLED) {
      ++transport.identity.roundId;
      ++transport.identity.calcTokenHi;
    }
  }
  EXPECT_EQ(4, transport.calls);
  EXPECT_EQ(2, transport.queued);
}
