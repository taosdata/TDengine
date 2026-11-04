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

extern "C" {
#include "os.h"
#include "tsdbReadUtil.h"
#include "vnode.h"

// The meta audit-notify path expects this dnode-owned handle. It remains
// disabled in this reader-only unit test.
SDmNotifyHandle dmNotifyHdl = {};
}

namespace {

class TsdbReadPhaseTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(taosThreadMutexInit(&reader.readerMutex, nullptr),
              TSDB_CODE_SUCCESS);
    pTableMap = tSimpleHashInit(
        1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    ASSERT_NE(pTableMap, nullptr);
    reader.status.pTableMap = pTableMap;
    prevReader.status.pTableMap = pTableMap;
    nextReader.status.pTableMap = pTableMap;
    ASSERT_EQ(createDataBlock(&reader.resBlockInfo.pResBlock),
              TSDB_CODE_SUCCESS);
    ASSERT_EQ(createDataBlock(&prevReader.resBlockInfo.pResBlock),
              TSDB_CODE_SUCCESS);
    ASSERT_EQ(createDataBlock(&nextReader.resBlockInfo.pResBlock),
              TSDB_CODE_SUCCESS);
    reader.idStr = id;
    reader.type = TIMEWINDOW_RANGE_EXTERNAL;
    reader.innerReader[0] = &prevReader;
    reader.innerReader[1] = &nextReader;
    reader.info.window = {100, 199};
    prevReader.info.window = {0, 99};
    nextReader.info.window = {200, 299};
  }

  void TearDown() override {
    blockDataDestroy(reader.resBlockInfo.pResBlock);
    blockDataDestroy(prevReader.resBlockInfo.pResBlock);
    blockDataDestroy(nextReader.resBlockInfo.pResBlock);
    clearBlockScanInfo(&scanInfo);
    tSimpleHashCleanup(pTableMap);
    EXPECT_EQ(taosThreadMutexDestroy(&reader.readerMutex),
              TSDB_CODE_SUCCESS);
  }

  void ResetStep(EContentData step, bool mainFirst) {
    reader.step = step;
    reader.mainFirst = mainFirst;
    reader.currentStepDone = false;
  }

  void AddScanInfo() {
    STableBlockScanInfo* pScanInfo = &scanInfo;
    ASSERT_EQ(tSimpleHashPut(pTableMap, &scanInfo.uid,
                             sizeof(scanInfo.uid), &pScanInfo,
                             POINTER_BYTES), TSDB_CODE_SUCCESS);
  }

  char        id[16] = "phase-test";
  SSHashObj*  pTableMap = nullptr;
  STsdbReader reader = {};
  STsdbReader prevReader = {};
  STsdbReader nextReader = {};
  STableBlockScanInfo scanInfo = {};
};

TEST_F(TsdbReadPhaseTest, ResetRebuildsScalarCompositeRowKey) {
  scanInfo.uid = 1;
  reader.suppInfo.numOfPks = 1;
  reader.suppInfo.pk.type = TSDB_DATA_TYPE_INT;
  reader.suppInfo.pk.bytes = sizeof(int32_t);
  ASSERT_EQ(initRowKey(&scanInfo.lastProcKey, 99, 1,
                       TSDB_DATA_TYPE_INT, sizeof(int32_t), false),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(initRowKey(&scanInfo.sttKeyInfo.nextProcKey, 100, 1,
                       TSDB_DATA_TYPE_INT, sizeof(int32_t), false),
            TSDB_CODE_SUCCESS);
  AddScanInfo();

  ASSERT_EQ(resetAllDataBlockScanInfo(pTableMap, 100, 1,
                                      &reader.suppInfo),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(scanInfo.lastProcKey.ts, 100);
  EXPECT_EQ(scanInfo.lastProcKey.numOfPKs, 1);
  EXPECT_EQ(scanInfo.lastProcKey.pks[0].type, TSDB_DATA_TYPE_INT);
  EXPECT_EQ(*static_cast<int32_t*>(VALUE_GET_DATUM(
                &scanInfo.lastProcKey.pks[0], TSDB_DATA_TYPE_INT)),
            INT32_MIN);
  EXPECT_EQ(scanInfo.sttKeyInfo.nextProcKey.ts, 101);
  EXPECT_EQ(*static_cast<int32_t*>(VALUE_GET_DATUM(
                &scanInfo.sttKeyInfo.nextProcKey.pks[0],
                TSDB_DATA_TYPE_INT)), INT32_MIN);
}

TEST_F(TsdbReadPhaseTest, ResetRebuildsVariableCompositeRowKey) {
  scanInfo.uid = 1;
  reader.suppInfo.numOfPks = 1;
  reader.suppInfo.pk.type = TSDB_DATA_TYPE_BINARY;
  reader.suppInfo.pk.bytes = 16;
  ASSERT_EQ(initRowKey(&scanInfo.lastProcKey, 99, 1,
                       TSDB_DATA_TYPE_BINARY, 16, false),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(initRowKey(&scanInfo.sttKeyInfo.nextProcKey, 100, 1,
                       TSDB_DATA_TYPE_BINARY, 16, false),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(scanInfo.lastProcKey.numOfPKs, 2);
  AddScanInfo();

  ASSERT_EQ(resetAllDataBlockScanInfo(pTableMap, 100, 1,
                                      &reader.suppInfo),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(scanInfo.lastProcKey.ts, 100);
  EXPECT_EQ(scanInfo.lastProcKey.numOfPKs, 1);
  EXPECT_EQ(scanInfo.lastProcKey.pks[0].type,
            TSDB_DATA_TYPE_BINARY);
  EXPECT_EQ(scanInfo.lastProcKey.pks[0].nData, 0);
  EXPECT_NE(scanInfo.lastProcKey.pks[0].pData, nullptr);
  EXPECT_EQ(scanInfo.sttKeyInfo.nextProcKey.ts, 101);
  EXPECT_EQ(scanInfo.sttKeyInfo.nextProcKey.numOfPKs, 1);
}

// MAIN_FIRST early-stop advances MAIN and NEXT on the next read, while PREV
// is terminal. Boundary checks keep delayed notifications out of other phases.
TEST_F(TsdbReadPhaseTest, MainFirstEarlyStopFollowsPhaseOrder) {
  ResetStep(EXTERNAL_ROWS_MAIN, true);
  ASSERT_EQ(tsdbReaderStepDone(&reader, 199), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(reader.currentStepDone);
  ASSERT_EQ(tsdbReaderStepDone(&reader, 200), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(reader.currentStepDone);
  EXPECT_EQ(reader.step, EXTERNAL_ROWS_MAIN);

  ResetStep(EXTERNAL_ROWS_NEXT, true);
  ASSERT_EQ(tsdbReaderStepDone(&reader, 199), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(reader.currentStepDone);
  ASSERT_EQ(tsdbReaderStepDone(&reader, 200), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(reader.currentStepDone);
  EXPECT_EQ(reader.step, EXTERNAL_ROWS_NEXT);

  ResetStep(EXTERNAL_ROWS_PREV, true);
  ASSERT_EQ(tsdbReaderStepDone(&reader, 100), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(reader.currentStepDone);
  ASSERT_EQ(tsdbReaderStepDone(&reader, 99), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(reader.currentStepDone);
  EXPECT_EQ(reader.step, EXTERNAL_ROWS_DONE);
}

// LEGACY retains PREV -> MAIN -> NEXT semantics: PREV is nonterminal and
// NEXT early-stop completes the reader immediately.
TEST_F(TsdbReadPhaseTest, LegacyEarlyStopSemanticsRemainUnchanged) {
  ResetStep(EXTERNAL_ROWS_PREV, false);
  ASSERT_EQ(tsdbReaderStepDone(&reader, 99), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(reader.currentStepDone);
  EXPECT_EQ(reader.step, EXTERNAL_ROWS_PREV);

  ResetStep(EXTERNAL_ROWS_NEXT, false);
  ASSERT_EQ(tsdbReaderStepDone(&reader, 200), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(reader.currentStepDone);
  EXPECT_EQ(reader.step, EXTERNAL_ROWS_DONE);
}

// The same transition helper drives reader initialization and natural EOF.
// This table test protects both phase orders without constructing TSDB files.
TEST(TsdbReadPhaseTransitionTest, MainFirstAndLegacyOrdersStayDistinct) {
  EXPECT_EQ(tsdbReaderNextExternalStep(true, EXTERNAL_ROWS_INIT),
            EXTERNAL_ROWS_MAIN);
  EXPECT_EQ(tsdbReaderNextExternalStep(true, EXTERNAL_ROWS_MAIN),
            EXTERNAL_ROWS_NEXT);
  EXPECT_EQ(tsdbReaderNextExternalStep(true, EXTERNAL_ROWS_NEXT),
            EXTERNAL_ROWS_PREV);
  EXPECT_EQ(tsdbReaderNextExternalStep(true, EXTERNAL_ROWS_PREV),
            EXTERNAL_ROWS_DONE);

  EXPECT_EQ(tsdbReaderNextExternalStep(false, EXTERNAL_ROWS_INIT),
            EXTERNAL_ROWS_PREV);
  EXPECT_EQ(tsdbReaderNextExternalStep(false, EXTERNAL_ROWS_PREV),
            EXTERNAL_ROWS_MAIN);
  EXPECT_EQ(tsdbReaderNextExternalStep(false, EXTERNAL_ROWS_MAIN),
            EXTERNAL_ROWS_NEXT);
  EXPECT_EQ(tsdbReaderNextExternalStep(false, EXTERNAL_ROWS_NEXT),
            EXTERNAL_ROWS_DONE);
}

// Invalid states retain the former switch default instead of indexing beyond
// the transition table.
TEST(TsdbReadPhaseTransitionTest, InvalidStepStopsSafely) {
  EXPECT_EQ(tsdbReaderNextExternalStep(
                true, static_cast<EContentData>(-1)),
            EXTERNAL_ROWS_DONE);
  EXPECT_EQ(tsdbReaderNextExternalStep(
                false,
                static_cast<EContentData>(EXTERNAL_ROWS_DONE + 1)),
            EXTERNAL_ROWS_DONE);
}

// MAIN_FIRST publishes the active segment, whereas the compatibility path
// keeps every external-reader block at the legacy MAIN phase.
TEST(TsdbReadPhaseTransitionTest, BlockPhaseMatchesSelectedReaderOrder) {
  EXPECT_EQ(tsdbReaderExternalStepPhase(true, EXTERNAL_ROWS_MAIN),
            SCAN_PHASE_MAIN);
  EXPECT_EQ(tsdbReaderExternalStepPhase(true, EXTERNAL_ROWS_NEXT),
            SCAN_PHASE_NEXT);
  EXPECT_EQ(tsdbReaderExternalStepPhase(true, EXTERNAL_ROWS_PREV),
            SCAN_PHASE_PREV);
  EXPECT_EQ(tsdbReaderExternalStepPhase(false, EXTERNAL_ROWS_PREV),
            SCAN_PHASE_MAIN);
  EXPECT_EQ(tsdbReaderExternalStepPhase(false, EXTERNAL_ROWS_NEXT),
            SCAN_PHASE_MAIN);
}

TEST(TsdbReadPhaseTransitionTest,
     RejectsDescendingMainFirstBeforeVnodeAccess) {
  SQueryTableDataCond cond = {0};
  cond.type = TIMEWINDOW_RANGE_EXTERNAL;
  cond.mainFirst = true;
  cond.order = TSDB_ORDER_DESC;
  void* pReader = reinterpret_cast<void*>(1);

  EXPECT_EQ(tsdbReaderOpen2(nullptr, &cond, nullptr, 0, nullptr,
                            &pReader, "phase-test", nullptr),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(pReader, nullptr);
}

// Side-reader conditions share the pinned snapshot but keep their own window
// and direction. Building them must not mutate the caller's main condition.
TEST(TsdbReadPhaseTransitionTest, ExternalCondsPreserveMainAndVersion) {
  SQueryTableDataCond main = {0};
  main.order = TSDB_ORDER_ASC;
  main.twindows = {100, 199};
  main.extTwindows[0] = {0, 99};
  main.extTwindows[1] = {200, 299};
  main.endVersion = 88;
  SQueryTableDataCond first = {0};
  SQueryTableDataCond last = {0};

  tsdbReaderInitExternalConds(&main, &first, &last);

  EXPECT_EQ(main.order, TSDB_ORDER_ASC);
  EXPECT_EQ(main.twindows.skey, 100);
  EXPECT_EQ(main.twindows.ekey, 199);
  EXPECT_EQ(first.order, TSDB_ORDER_DESC);
  EXPECT_EQ(first.twindows.skey, 0);
  EXPECT_EQ(first.twindows.ekey, 99);
  EXPECT_EQ(last.order, TSDB_ORDER_ASC);
  EXPECT_EQ(last.twindows.skey, 200);
  EXPECT_EQ(last.twindows.ekey, 299);
  EXPECT_EQ(first.endVersion, main.endVersion);
  EXPECT_EQ(last.endVersion, main.endVersion);
}

// Descending LEGACY scans reverse both side-reader placement and direction.
TEST(TsdbReadPhaseTransitionTest, ExternalCondsPreserveDescendingOrder) {
  SQueryTableDataCond main = {0};
  main.order = TSDB_ORDER_DESC;
  main.twindows = {100, 199};
  main.extTwindows[0] = {0, 99};
  main.extTwindows[1] = {200, 299};
  SQueryTableDataCond first = {0};
  SQueryTableDataCond last = {0};

  tsdbReaderInitExternalConds(&main, &first, &last);

  EXPECT_EQ(main.order, TSDB_ORDER_DESC);
  EXPECT_EQ(main.twindows.skey, 100);
  EXPECT_EQ(main.twindows.ekey, 199);
  EXPECT_EQ(first.order, TSDB_ORDER_ASC);
  EXPECT_EQ(first.twindows.skey, 200);
  EXPECT_EQ(first.twindows.ekey, 299);
  EXPECT_EQ(last.order, TSDB_ORDER_DESC);
  EXPECT_EQ(last.twindows.skey, 0);
  EXPECT_EQ(last.twindows.ekey, 99);
}

// Empty MAIN and NEXT phases advance internally through PREV. The caller sees
// only the final end-of-stream and subsequent calls remain at DONE.
TEST_F(TsdbReadPhaseTest, EmptyMainFirstPhasesReturnOneFinalEos) {
  reader.mainFirst = true;
  reader.step = EXTERNAL_ROWS_MAIN;
  reader.flag = READER_STATUS_NORMAL;
  reader.info.window = TSWINDOW_DESC_INITIALIZER;
  prevReader.info.window = TSWINDOW_DESC_INITIALIZER;
  nextReader.info.window = TSWINDOW_DESC_INITIALIZER;

  bool hasNext = true;
  ASSERT_EQ(tsdbNextDataBlock2(&reader, &hasNext), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(hasNext);
  EXPECT_EQ(reader.step, EXTERNAL_ROWS_DONE);

  hasNext = true;
  ASSERT_EQ(tsdbNextDataBlock2(&reader, &hasNext), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(hasNext);
  EXPECT_EQ(reader.step, EXTERNAL_ROWS_DONE);
}

// An empty NEXT phase must not hide a nonempty PREV phase. Even if PREV yields
// no rows, it remains the actual reader that DONE suspend must clean up.
TEST_F(TsdbReadPhaseTest, EmptyNextContinuesToPrevAndTracksIt) {
  reader.mainFirst = true;
  reader.step = EXTERNAL_ROWS_MAIN;
  reader.flag = READER_STATUS_NORMAL;
  reader.info.window = TSWINDOW_DESC_INITIALIZER;
  nextReader.info.window = TSWINDOW_DESC_INITIALIZER;

  bool hasNext = true;
  ASSERT_EQ(tsdbNextDataBlock2(&reader, &hasNext), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(hasNext);
  EXPECT_EQ(reader.step, EXTERNAL_ROWS_DONE);
  EXPECT_EQ(reader.pLastActiveReader, &prevReader);
  EXPECT_EQ(tsdbReaderGetExternalSuspendReader(&reader), &prevReader);
}

// DONE dispatch follows runtime activity instead of the nominal final phase.
TEST_F(TsdbReadPhaseTest, DoneSuspendUsesLastActiveReader) {
  reader.step = EXTERNAL_ROWS_DONE;
  reader.pLastActiveReader = &nextReader;
  EXPECT_EQ(tsdbReaderGetExternalSuspendReader(&reader), &nextReader);

  reader.pLastActiveReader = &reader;
  EXPECT_EQ(tsdbReaderGetExternalSuspendReader(&reader), &reader);
}

// An unspecified or too-new end version is clipped once; a caller-specified
// historical snapshot remains fixed across later phase and group reopens.
TEST(TsdbReadVersionTest, PinEndVersionUsesOneEffectiveUpperBound) {
  SQueryTableDataCond unspecified = {0};
  unspecified.endVersion = -1;
  tsdbReaderPinEndVersion(&unspecified, 100);
  EXPECT_EQ(unspecified.endVersion, 100);

  SQueryTableDataCond tooNew = {0};
  tooNew.endVersion = 200;
  tsdbReaderPinEndVersion(&tooNew, 100);
  EXPECT_EQ(tooNew.endVersion, 100);

  SQueryTableDataCond historical = {0};
  historical.endVersion = 50;
  tsdbReaderPinEndVersion(&historical, 100);
  EXPECT_EQ(historical.endVersion, 50);
  tsdbReaderPinEndVersion(&historical, 200);
  EXPECT_EQ(historical.endVersion, 50);
}

// The file block column type must match the result column type exactly,
// otherwise the bulk memcpy would be sized from the wrong width.
TEST(TsdbReadHardeningTest, CopyNumericColsRejectsTypeMismatch) {
  int32_t src[4] = {1, 2, 3, 4};
  SColData data = {};
  data.type = TSDB_DATA_TYPE_INT;
  data.nVal = 4;
  data.flag = HAS_VALUE;
  data.pData = (uint8_t*)src;

  int64_t dst[4] = {0};
  SColumnInfoData col = {};
  col.info.type = TSDB_DATA_TYPE_BIGINT;
  col.info.bytes = sizeof(int64_t);
  col.pData = (char*)dst;

  SFileBlockDumpInfo dump = {};
  dump.totalRows = 4;
  dump.rowIndex = 0;

  EXPECT_EQ(tsdbCopyNumericCols(&data, &dump, &col, 4, true), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(dst[0], 0);

  // invalid file block type
  data.type = TSDB_DATA_TYPE_MAX;
  EXPECT_EQ(tsdbCopyNumericCols(&data, &dump, &col, 4, true), TSDB_CODE_INVALID_PARA);

  // matching type copies
  int32_t out[4] = {0};
  data.type = TSDB_DATA_TYPE_INT;
  col.info.type = TSDB_DATA_TYPE_INT;
  col.info.bytes = sizeof(int32_t);
  col.pData = (char*)out;
  EXPECT_EQ(tsdbCopyNumericCols(&data, &dump, &col, 4, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(out[3], 4);

  // matching type but different width
  col.info.bytes = sizeof(int64_t);
  EXPECT_EQ(tsdbCopyNumericCols(&data, &dump, &col, 4, true), TSDB_CODE_INVALID_PARA);
}

// A varchar primary key longer than the schema-sized key buffer is rejected.
TEST(TsdbReadHardeningTest, PkDeepCopyRejectsOverflow) {
  EXPECT_EQ(tsdbCheckPkDataLen(0, 10), TSDB_CODE_SUCCESS);
  EXPECT_EQ(tsdbCheckPkDataLen(8, 10), TSDB_CODE_SUCCESS);
  EXPECT_EQ(tsdbCheckPkDataLen(9, 10), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(tsdbCheckPkDataLen(-1, 10), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(tsdbCheckPkDataLen(0, 1), TSDB_CODE_INVALID_PARA);

  const int32_t pkBytes = 10;  // varchar(8)
  uint8_t       raw[32];
  memset(raw, 'a', sizeof(raw));
  int32_t   aOffset[1] = {0};
  SColData  cd = {};
  cd.type = TSDB_DATA_TYPE_VARCHAR;
  cd.flag = HAS_VALUE;
  cd.nVal = 1;
  cd.aOffset = aOffset;
  cd.pData = raw;

  SBlockData block = {};
  block.nColData = 1;
  block.aColData = &cd;

  char    buf[pkBytes];
  SRowKey key = {};
  key.pks[0].pData = (uint8_t*)buf;

  cd.nData = 8;
  EXPECT_EQ(tsdbBlockDataGetPrimaryKeyDeepCopy(&block, 0, 0, &key, pkBytes), TSDB_CODE_SUCCESS);
  EXPECT_EQ(key.pks[0].nData, 8u);

  cd.nData = 32;
  EXPECT_EQ(tsdbBlockDataGetPrimaryKeyDeepCopy(&block, 0, 0, &key, pkBytes), TSDB_CODE_INVALID_PARA);

  // slot id out of range
  cd.nData = 8;
  EXPECT_EQ(tsdbBlockDataGetPrimaryKeyDeepCopy(&block, 0, 1, &key, pkBytes), TSDB_CODE_INVALID_PARA);
}

TEST(TsdbReadHardeningTest, GetBucketIndexClamps) {
  EXPECT_EQ(tsdbGetBucketIndex(10, 5, 5, 4), 0);     // rows below start
  EXPECT_EQ(tsdbGetBucketIndex(10, 5, 10, 4), 0);
  EXPECT_EQ(tsdbGetBucketIndex(10, 5, 17, 4), 1);
  EXPECT_EQ(tsdbGetBucketIndex(10, 5, 30, 4), 3);    // == numOfBucket
  EXPECT_EQ(tsdbGetBucketIndex(10, 5, 1000, 4), 3);  // far beyond
  EXPECT_EQ(tsdbGetBucketIndex(10, 0, 100, 4), 0);   // zero range
  EXPECT_EQ(tsdbGetBucketIndex(10, 5, 100, 0), 0);   // no bucket
  EXPECT_EQ(tsdbGetBucketIndex(INT32_MIN, 1, INT32_MAX, 4), 3);
}

}  // namespace
