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

#include "stub.h"

extern "C" {
#include "taoserror.h"
#include "tmsgcb.h"
#include "tsdb.h"
#include "tsdbReadUtil.h"
#include "vnode.h"
#include "vnodeInt.h"
}

SDmNotifyHandle dmNotifyHdl = {};

namespace {

struct SnapshotStubState {
  STsdbReadSnap* snap = nullptr;
  int32_t        takeCalls = 0;
  int32_t        releaseCalls = 0;
};

SnapshotStubState* gSnapshotStubState = nullptr;

struct ReallocStubState {
  Stub* stub = nullptr;
  void* uidList = nullptr;
};

ReallocStubState* gReallocStubState = nullptr;

int32_t getNullSchema(SMeta*, tb_uid_t, int32_t, int, STSchema** ppSchema) {
  *ppSchema = nullptr;
  return TSDB_CODE_SUCCESS;
}

int64_t getEarliestTimestamp(STsdb*) { return 0; }

int32_t takeFixtureSnapshot(STsdbReader*, _query_reseek_func_t, STsdbReadSnap** ppSnap, const char*) {
  ++gSnapshotStubState->takeCalls;
  *ppSnap = gSnapshotStubState->snap;
  return TSDB_CODE_SUCCESS;
}

void releaseFixtureSnapshot(STsdbReader*, STsdbReadSnap*, bool) { ++gSnapshotStubState->releaseCalls; }

int32_t failHashPut(SSHashObj*, const void*, size_t, const void*, size_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return TSDB_CODE_OUT_OF_MEMORY;
}

void* failUidListRealloc(void* ptr, int64_t size) {
  if (ptr == gReallocStubState->uidList) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }

  gReallocStubState->stub->reset(taosMemRealloc);
  void* result = taosMemRealloc(ptr, size);
  gReallocStubState->stub->set(taosMemRealloc, failUidListRealloc);
  return result;
}

class TsdbReaderResetTest : public ::testing::Test {
 protected:
  void SetUp() override {
    vnode_.pTsdb = &tsdb_;
    tsdb_.pVnode = &vnode_;
    vnode_.config.tsdbCfg.maxRows = 128;
    vnode_.state.applied = 1;
    column_.colId = PRIMARYKEY_TIMESTAMP_COL_ID;
    column_.type = TSDB_DATA_TYPE_TIMESTAMP;
    column_.bytes = sizeof(TSKEY);
    slot_ = 0;
    table_.uid = 7;
    TARRAY2_INIT(&fileSets_);
    snap_.pfSetArray = &fileSets_;
    snapshotStubState_.snap = &snap_;
    gSnapshotStubState = &snapshotStubState_;

    stub_.set(metaGetTbTSchemaMaybeNull, getNullSchema);
    stub_.set(tsdbGetEarliestTs, getEarliestTimestamp);
    stub_.set(tsdbTakeReadSnap2, takeFixtureSnapshot);
    stub_.set(tsdbUntakeReadSnap2, releaseFixtureSnapshot);
  }

  void TearDown() override {
    const int32_t expectedReleaseCalls =
        reader_ != nullptr && static_cast<STsdbReader*>(reader_)->pReadSnap != nullptr ? 1 : 0;
    tsdbReaderClose2(reader_);
    reader_ = nullptr;
    EXPECT_EQ(expectedReleaseCalls, snapshotStubState_.releaseCalls);
    stub_.reset(metaGetTbTSchemaMaybeNull);
    stub_.reset(tsdbGetEarliestTs);
    stub_.reset(tsdbTakeReadSnap2);
    stub_.reset(tsdbUntakeReadSnap2);
    gSnapshotStubState = nullptr;
    gReallocStubState = nullptr;
  }

  SQueryTableDataCond makeCond(int64_t skey, int64_t ekey) {
    SQueryTableDataCond cond = {};
    cond.order = TSDB_ORDER_ASC;
    cond.numOfCols = 1;
    cond.colList = &column_;
    cond.pSlotList = &slot_;
    cond.type = TIMEWINDOW_RANGE_CONTAINED;
    cond.twindows.skey = skey;
    cond.twindows.ekey = ekey;
    cond.startVersion = -1;
    cond.endVersion = -1;
    return cond;
  }

  int32_t openReader(SQueryTableDataCond* cond) {
    return tsdbReaderOpen2(&vnode_, cond, &table_, 1, nullptr, &reader_, "tsdb-reader-reset-test", nullptr);
  }

  void expectEmptyTableState(SSHashObj* tableMap, int32_t numPerBucket) {
    auto* reader = static_cast<STsdbReader*>(reader_);
    ASSERT_NE(nullptr, reader);
    ASSERT_NE(nullptr, reader->status.pTableMap);
    EXPECT_EQ(tableMap, reader->status.pTableMap);
    EXPECT_EQ(0, tSimpleHashGetSize(reader->status.pTableMap));
    EXPECT_EQ(nullptr, reader->status.pTableIter);
    EXPECT_EQ(nullptr, reader->status.uidList.tableUidList);
    EXPECT_EQ(0, reader->status.uidList.currentIndex);
    EXPECT_EQ(nullptr, reader->blockInfoBuf.pData);
    EXPECT_EQ(0, reader->blockInfoBuf.numOfTables);
    EXPECT_EQ(numPerBucket, reader->blockInfoBuf.numPerBucket);
  }

  Stub              stub_;
  SVnode            vnode_ = {};
  STsdb             tsdb_ = {};
  SColumnInfo       column_ = {};
  int32_t           slot_ = 0;
  STableKeyInfo     table_ = {};
  TFileSetArray     fileSets_ = {};
  STsdbReadSnap     snap_ = {};
  SnapshotStubState snapshotStubState_ = {};
  void*             reader_ = nullptr;
};

TEST_F(TsdbReaderResetTest, EmptyOpenReturnsCompleteDeferredReader) {
  auto cond = makeCond(10, 9);

  ASSERT_EQ(TSDB_CODE_SUCCESS, openReader(&cond));
  ASSERT_NE(nullptr, reader_);

  auto* reader = static_cast<STsdbReader*>(reader_);
  EXPECT_NE(nullptr, reader->pSchemaMap);
  EXPECT_NE(nullptr, reader->status.pTableMap);
  EXPECT_NE(nullptr, reader->status.pLDataIterArray);
  EXPECT_EQ(READER_STATUS_SUSPEND, reader->flag);
  EXPECT_EQ(nullptr, reader->pReadSnap);
  bool hasNext = true;
  EXPECT_EQ(TSDB_CODE_SUCCESS, tsdbNextDataBlock2(reader, &hasNext));
  EXPECT_FALSE(hasNext);
  EXPECT_EQ(0, snapshotStubState_.takeCalls);
}

TEST_F(TsdbReaderResetTest, ContainedOpenDoesNotPinEndVersion) {
  auto cond = makeCond(10, 9);
  cond.mainFirst = true;

  ASSERT_EQ(TSDB_CODE_SUCCESS, openReader(&cond));

  EXPECT_EQ(-1, cond.endVersion);
}

TEST_F(TsdbReaderResetTest, EmptyReaderAppliesLaterNonemptyCondition) {
  auto empty = makeCond(10, 9);
  ASSERT_EQ(TSDB_CODE_SUCCESS, openReader(&empty));
  ASSERT_NE(nullptr, reader_);
  auto* const originalReader = reader_;
  auto        nonempty = makeCond(40, 50);

  ASSERT_EQ(TSDB_CODE_SUCCESS, tsdbReaderReset2(reader_, &nonempty));

  auto* reader = static_cast<STsdbReader*>(reader_);
  EXPECT_EQ(originalReader, reader_);
  EXPECT_EQ(40, reader->info.window.skey);
  EXPECT_EQ(50, reader->info.window.ekey);
  EXPECT_EQ(&snap_, reader->pReadSnap);
  EXPECT_EQ(READER_STATUS_NORMAL, reader->flag);
  EXPECT_EQ(1, snapshotStubState_.takeCalls);
}

TEST_F(TsdbReaderResetTest, NonemptyEmptyNonemptyKeepsSnapshotIdentity) {
  auto first = makeCond(10, 20);
  ASSERT_EQ(TSDB_CODE_SUCCESS, openReader(&first));
  ASSERT_NE(nullptr, reader_);
  auto* const originalReader = reader_;
  bool        hasNext = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tsdbNextDataBlock2(reader_, &hasNext));
  ASSERT_FALSE(hasNext);
  auto*       reader = static_cast<STsdbReader*>(reader_);
  auto* const originalSnapshot = reader->pReadSnap;
  ASSERT_EQ(&snap_, originalSnapshot);
  ASSERT_EQ(1, snapshotStubState_.takeCalls);

  auto empty = makeCond(30, 29);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tsdbReaderReset2(reader_, &empty));
  EXPECT_EQ(originalReader, reader_);
  EXPECT_EQ(originalSnapshot, reader->pReadSnap);
  EXPECT_EQ(30, reader->info.window.skey);
  EXPECT_EQ(29, reader->info.window.ekey);
  EXPECT_EQ(1, snapshotStubState_.takeCalls);

  auto last = makeCond(40, 50);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tsdbReaderReset2(reader_, &last));
  EXPECT_EQ(originalReader, reader_);
  EXPECT_EQ(originalSnapshot, reader->pReadSnap);
  EXPECT_EQ(40, reader->info.window.skey);
  EXPECT_EQ(50, reader->info.window.ekey);
  EXPECT_EQ(1, snapshotStubState_.takeCalls);
}

TEST_F(TsdbReaderResetTest, SuspendedReaderAcquiresSnapshotForNonemptyReset) {
  auto first = makeCond(10, 20);
  ASSERT_EQ(TSDB_CODE_SUCCESS, openReader(&first));
  ASSERT_NE(nullptr, reader_);
  auto* reader = static_cast<STsdbReader*>(reader_);
  ASSERT_EQ(READER_STATUS_SUSPEND, reader->flag);
  ASSERT_EQ(nullptr, reader->pReadSnap);
  auto next = makeCond(30, 40);

  ASSERT_EQ(TSDB_CODE_SUCCESS, tsdbReaderReset2(reader_, &next));

  EXPECT_EQ(READER_STATUS_NORMAL, reader->flag);
  EXPECT_EQ(&snap_, reader->pReadSnap);
  EXPECT_EQ(30, reader->info.window.skey);
  EXPECT_EQ(40, reader->info.window.ekey);
  EXPECT_EQ(1, snapshotStubState_.takeCalls);
}

TEST_F(TsdbReaderResetTest, NormalReaderWithoutSnapshotFailsClosed) {
  auto first = makeCond(10, 20);
  ASSERT_EQ(TSDB_CODE_SUCCESS, openReader(&first));
  ASSERT_NE(nullptr, reader_);
  auto* reader = static_cast<STsdbReader*>(reader_);
  reader->flag = READER_STATUS_NORMAL;
  reader->pReadSnap = nullptr;
  auto next = makeCond(30, 40);

  EXPECT_NE(TSDB_CODE_SUCCESS, tsdbReaderReset2(reader_, &next));
  EXPECT_EQ(nullptr, reader->pReadSnap);
  EXPECT_EQ(0, snapshotStubState_.takeCalls);
}

TEST_F(TsdbReaderResetTest, NullConditionIsRejectedEvenWhenOldWindowIsEmpty) {
  auto empty = makeCond(10, 9);
  ASSERT_EQ(TSDB_CODE_SUCCESS, openReader(&empty));
  ASSERT_NE(nullptr, reader_);

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tsdbReaderReset2(reader_, nullptr));
  EXPECT_EQ(0, snapshotStubState_.takeCalls);
}

TEST_F(TsdbReaderResetTest, ExternalReaderResetRemainsRejected) {
  auto external = makeCond(10, 20);
  external.type = TIMEWINDOW_RANGE_EXTERNAL;
  external.extTwindows[0] = {1, 9};
  external.extTwindows[1] = {21, 30};
  ASSERT_EQ(TSDB_CODE_SUCCESS, openReader(&external));
  ASSERT_NE(nullptr, reader_);
  auto contained = makeCond(30, 40);

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tsdbReaderReset2(reader_, &contained));
  EXPECT_EQ(TIMEWINDOW_RANGE_EXTERNAL, static_cast<STsdbReader*>(reader_)->type);
  EXPECT_EQ(0, snapshotStubState_.takeCalls);
}

TEST_F(TsdbReaderResetTest, HashInsertFailureClearsVariablePkAndTableState) {
  auto cond = makeCond(10, 20);
  ASSERT_EQ(TSDB_CODE_SUCCESS, openReader(&cond));
  ASSERT_NE(nullptr, reader_);
  auto* reader = static_cast<STsdbReader*>(reader_);
  reader->suppInfo.numOfPks = 1;
  reader->suppInfo.pk.type = TSDB_DATA_TYPE_VARCHAR;
  reader->suppInfo.pk.bytes = 32;
  auto* const   tableMap = reader->status.pTableMap;
  const auto    numPerBucket = reader->blockInfoBuf.numPerBucket;
  STableKeyInfo replacement = {};
  replacement.uid = 8;
  stub_.set(tSimpleHashPut, failHashPut);

  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, tsdbSetTableList2(reader_, &replacement, 1));

  stub_.reset(tSimpleHashPut);
  expectEmptyTableState(tableMap, numPerBucket);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tsdbSetTableList2(reader_, &replacement, 1));
  EXPECT_EQ(1, tSimpleHashGetSize(reader->status.pTableMap));
  ASSERT_NE(nullptr, reader->status.uidList.tableUidList);
  EXPECT_EQ(replacement.uid, reader->status.uidList.tableUidList[0]);
}

TEST_F(TsdbReaderResetTest, UidReallocFailureClearsHalfExpandedBuffer) {
  auto cond = makeCond(10, 20);
  ASSERT_EQ(TSDB_CODE_SUCCESS, openReader(&cond));
  ASSERT_NE(nullptr, reader_);
  auto* reader = static_cast<STsdbReader*>(reader_);
  ASSERT_NE(nullptr, reader->status.uidList.tableUidList);
  ReallocStubState reallocState;
  reallocState.stub = &stub_;
  reallocState.uidList = reader->status.uidList.tableUidList;
  gReallocStubState = &reallocState;
  auto* const   tableMap = reader->status.pTableMap;
  const auto    numPerBucket = reader->blockInfoBuf.numPerBucket;
  STableKeyInfo tables[2] = {};
  tables[0].uid = 9;
  tables[1].uid = 8;
  stub_.set(taosMemRealloc, failUidListRealloc);

  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, tsdbSetTableList2(reader_, tables, 2));

  stub_.reset(taosMemRealloc);
  gReallocStubState = nullptr;
  expectEmptyTableState(tableMap, numPerBucket);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tsdbSetTableList2(reader_, tables, 2));
  EXPECT_EQ(2, tSimpleHashGetSize(reader->status.pTableMap));
  ASSERT_NE(nullptr, reader->status.uidList.tableUidList);
  EXPECT_EQ(8, reader->status.uidList.tableUidList[0]);
  EXPECT_EQ(9, reader->status.uidList.tableUidList[1]);
}

TEST_F(TsdbReaderResetTest, RebindSucceedsAfterPriorTableListFailure) {
  auto empty = makeCond(10, 9);
  ASSERT_EQ(TSDB_CODE_SUCCESS, openReader(&empty));
  ASSERT_NE(nullptr, reader_);
  auto* reader = static_cast<STsdbReader*>(reader_);
  ASSERT_EQ(1, tSimpleHashGetSize(reader->status.pTableMap));
  auto* const originalUidList = reader->status.uidList.tableUidList;
  auto* const originalBlockBuf = reader->blockInfoBuf.pData;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tsdbSetTableList2(reader_, nullptr, -1));
  EXPECT_EQ(1, tSimpleHashGetSize(reader->status.pTableMap));
  EXPECT_EQ(originalUidList, reader->status.uidList.tableUidList);
  EXPECT_EQ(originalBlockBuf, reader->blockInfoBuf.pData);

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, tsdbSetTableList2(reader_, nullptr, 1));
  EXPECT_EQ(1, tSimpleHashGetSize(reader->status.pTableMap));
  EXPECT_EQ(originalUidList, reader->status.uidList.tableUidList);
  EXPECT_EQ(originalBlockBuf, reader->blockInfoBuf.pData);

  STableKeyInfo replacement = {};
  replacement.uid = 9;
  ASSERT_EQ(TSDB_CODE_SUCCESS, tsdbSetTableList2(reader_, &replacement, 1));
  EXPECT_EQ(1, tSimpleHashGetSize(reader->status.pTableMap));
  ASSERT_NE(nullptr, reader->status.uidList.tableUidList);
  EXPECT_EQ(replacement.uid, reader->status.uidList.tableUidList[0]);
}

}  // namespace
