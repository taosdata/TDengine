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

#ifndef TDENGINE_GROUPOPERATOR_TEST_H
#define TDENGINE_GROUPOPERATOR_TEST_H

#if defined(BUILD_TEST)

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define PARTITION_SPILL_TEST_MAX_PAGES 8
#define PARTITION_SPILL_TEST_FRAGMENT_BATCHES 2
#define PARTITION_SPILL_TEST_BATCH_GROUPS     10
#define PARTITION_PHASE_TEST_MAX_OUTPUTS      64
#define PARTITION_PHASE_TEST_MAX_ROWS         4096
#define PARTITION_PHASE_TEST_PAGE_SIZE_SMALL  4096
#define PARTITION_PHASE_TEST_PAGE_SIZE_LARGE  8192
// Two pages, one per configured page class, matching the class sizes
// partitionPhaseTestInit constructs.
#define PARTITION_PHASE_TEST_PAGE_BUDGET                            \
  (PARTITION_PHASE_TEST_PAGE_SIZE_SMALL +                           \
   PARTITION_PHASE_TEST_PAGE_SIZE_LARGE)

typedef struct SPartitionSpillTestGroup {
  uint64_t groupId;
  int32_t  growLevel;
  int64_t  residentRows;
  int64_t  numOfRows;
} SPartitionSpillTestGroup;

typedef struct SPartitionSpillTestResult {
  int32_t pageSize;
  int32_t inputPageCount;
  int64_t inputRowCount;

  int32_t fragmentCount;
  int64_t fragmentOffset;
  int32_t fragmentLength;
  int64_t fragmentNumRows;
  int64_t logicalFileSize;
  int64_t physicalFileSize;

  int32_t blockCount;  // blocks found by walking the length prefixes
  int32_t blockSizes[PARTITION_SPILL_TEST_MAX_PAGES];
  int32_t decodedRows[PARTITION_SPILL_TEST_MAX_PAGES];
  int64_t decodedValueSum;

  int32_t remainingGroupPages;
  int64_t remainingGroupRows;
  int32_t remainingResidentPages;

  int32_t spillCount;
  int64_t spillPages;
  int64_t spillRows;
  int64_t spillBytes;
  int32_t loadCount;
  int64_t loadBytes;
  int32_t writeCallCount;
  int32_t readCallCount;
} SPartitionSpillTestResult;

// Outcome of a production 20% spill batch over ten one-page groups. The
// first two groups are selected, allowing tests to verify both cross-group
// write coalescing and independent fragment ownership.
typedef struct SPartitionSpillTestBatchWriteResult {
  int32_t spillCode;
  int32_t writeCallCount;
  int32_t spillCount;
  int32_t spillFailures;
  int32_t remainingResidentPages;
  int64_t logicalFileSize;
  int64_t physicalFileSize;
  int64_t physicalFileSizeAtFailure;
  int32_t groupPages[PARTITION_SPILL_TEST_BATCH_GROUPS];
  int64_t groupRows[PARTITION_SPILL_TEST_BATCH_GROUPS];
  int32_t fragmentCounts[PARTITION_SPILL_TEST_BATCH_GROUPS];
  int64_t fragmentOffsets[PARTITION_SPILL_TEST_BATCH_GROUPS];
  int32_t fragmentLengths[PARTITION_SPILL_TEST_BATCH_GROUPS];
  int64_t fragmentRows[PARTITION_SPILL_TEST_BATCH_GROUPS];
  int64_t decodedRows;
  int64_t decodedValueSum;
} SPartitionSpillTestBatchWriteResult;

// Corruptions applied to a spilled fragment before reading it back through
// the production reader; each must be rejected as corrupt. FRAGMENT_ROWS
// tampers the in-memory fragment record, the others tamper on-disk bytes.
typedef enum EPartitionSpillTestCorruption {
  PARTITION_SPILL_TEST_CORRUPT_FRAGMENT_ROWS = 1,
  PARTITION_SPILL_TEST_CORRUPT_BLOCK_ROWS = 2,
  PARTITION_SPILL_TEST_CORRUPT_COL_LENGTH = 3,
  PARTITION_SPILL_TEST_CORRUPT_COL_LENGTH_ZERO = 4,
} EPartitionSpillTestCorruption;

// Outcome of spilling with an injected page-recycle failure: the fragment
// must stay committed and readable while page bookkeeping is fully cleared.
typedef struct SPartitionSpillTestRecycleResult {
  int32_t spillCode;
  int32_t remainingGroupPages;
  int64_t remainingGroupRows;
  int32_t remainingResidentPages;
  int32_t fragmentCount;
  int64_t readBackRows;
} SPartitionSpillTestRecycleResult;

// Spill-reader cursor snapshot taken exactly at a fragment boundary.
typedef struct SPartitionSpillTestCursorState {
  int32_t fragmentIndex;
  int64_t readOffset;
  int64_t decodedRows;
} SPartitionSpillTestCursorState;

// Outcome of spilling the same group twice and reading both fragments through
// the production cursor. The optional corruption mode proves that the second
// fragment's row total is reconciled independently of the first.
typedef struct SPartitionSpillTestMultiFragmentResult {
  int32_t readCode;
  int32_t fragmentCount;
  int64_t fragmentOffsets[PARTITION_SPILL_TEST_FRAGMENT_BATCHES];
  int32_t fragmentLengths[PARTITION_SPILL_TEST_FRAGMENT_BATCHES];
  int64_t fragmentNumRows[PARTITION_SPILL_TEST_FRAGMENT_BATCHES];
  int64_t logicalFileSize;
  int64_t physicalFileSize;

  int32_t decodedBlockCount;
  int32_t decodedBlockRows[PARTITION_SPILL_TEST_MAX_PAGES];
  int32_t decodedFirstValues[PARTITION_SPILL_TEST_MAX_PAGES];
  int64_t decodedRows;
  int64_t decodedValueSum;
  int32_t loadCount;
  int64_t loadBytes;

  SPartitionSpillTestCursorState afterFirstFragment;
  SPartitionSpillTestCursorState afterRead;
} SPartitionSpillTestMultiFragmentResult;

// Partition-specific encryption integration result. Shared crypt primitives
// have their own tests; this verifies the partition writer/reader wiring.
typedef struct SPartitionSpillTestEncryptionResult {
  bool    cryptContextCreated;
  bool    scopeRestored;
  int32_t fragmentCount;
  int64_t fragmentNumRows;
  int32_t plainBlockSize;
  int32_t encryptedBlockSize;
  int64_t logicalFileSize;
  int64_t physicalFileSize;
  int32_t encryptedSpillCount;
  int64_t readBackRows;
  int64_t decodedValueSum;
  int32_t loadCount;
  int64_t loadBytes;
} SPartitionSpillTestEncryptionResult;

typedef struct SPartitionSpillTestState {
  int64_t logicalFileSize;
  int64_t physicalFileSize;
  int32_t fragmentCount;
  int32_t groupPages;
  int64_t groupRows;
  int32_t residentPages;
  int32_t spillCount;
  int32_t spillFailures;
  int64_t spillPages;
  int64_t spillRows;
  int64_t spillBytes;
  int64_t residentRows;
} SPartitionSpillTestState;

typedef struct SPartitionMaxPageTestResult {
  SPartitionSpillTestState beforeSpill;
  SPartitionSpillTestState afterSpill;
  // Debug/test invariants checked against beforeSpill's still-resident,
  // still-bucketed group: residentBytes/numResidentPages must match its
  // pages exactly (nothing unrecycled yet), and its spill-bucket level
  // must match its last page's (grown) class.
  bool residentBytesInvariantOk;
  bool residentPagesInvariantOk;
  bool spillBucketLevelInvariantOk;
} SPartitionMaxPageTestResult;

// State surrounding a pre-commit write failure after one fragment has already
// been committed. The failed append must be truncated without disturbing the
// committed fragment or the still-resident pages.
typedef struct SPartitionSpillTestRollbackResult {
  int32_t spillCode;
  SPartitionSpillTestState before;
  SPartitionSpillTestState after;
  int64_t fragmentOffsetBefore;
  int32_t fragmentLengthBefore;
  int64_t fragmentOffsetAfter;
  int32_t fragmentLengthAfter;
  int64_t physicalFileSizeAtFailure;
  int64_t residentValueSumBefore;
  int64_t residentValueSumAfter;
  int64_t readBackRows;
  int64_t decodedValueSum;
} SPartitionSpillTestRollbackResult;

// Outcome of sending one metadata-only block through the production
// doHashPartition path. Such a block must belong to a group without consuming
// a resident page or triggering spill.
typedef struct SPartitionNotLoadedTestResult {
  bool     pageListInitialized;
  int32_t  groupPages;
  int32_t  residentPages;
  int32_t  peakResidentPages;
  int32_t  spillCount;
  int32_t  notLoadedBlocks;
  int32_t  outputRows;
  bool     outputDataLoad;
  uint64_t groupId;
  uint64_t outputGroupId;
} SPartitionNotLoadedTestResult;

// NULL rows observed on each of the two read paths a partition group can
// take: straight from a resident page, and after the group has spilled.
// Both must agree, and both must report the NULLs the pages were staged
// with.
typedef struct SPartitionSpillTestNullResult {
  bool    pagedHasNull;    // every page block reported hasNull
  int64_t pagedRows;
  int64_t pagedNullRows;
  int64_t pagedValueSum;   // non-NULL rows only
  bool    spilledHasNull;  // every fragment block reported hasNull
  int64_t spilledRows;
  int64_t spilledNullRows;
  int64_t spilledValueSum;
} SPartitionSpillTestNullResult;

// phase must be a valid EScanPhase value.
typedef struct SPartitionPhaseTestBlock {
  int8_t  phase;
  int32_t groupValue;
  int32_t rows;
  int64_t firstTs;
  int64_t tsStep;
  bool    metadataOnly;
  uint64_t outerEpoch;
  uint64_t nullGroupRowMask;
} SPartitionPhaseTestBlock;

// Zero-initialize this config. A success code disables failure injection.
typedef struct SPartitionPhaseTestConfig {
  int64_t memBudgetBytes;
  bool    encryptSpill;
  bool    reserveColumn;
  int32_t prepareOutputFailureCode;
  int32_t pageAllocationFailureCode;
  int32_t spillReadFailureCode;
  int32_t spillCryptFailureCode;
} SPartitionPhaseTestConfig;

typedef struct SPartitionPhaseTestOutput {
  int8_t   phase;
  int32_t  rows;
  int32_t  pulledBlocks;
  uint64_t groupId;
  uint64_t baseGId;
  int64_t  firstTs;
  int64_t  lastTs;
} SPartitionPhaseTestOutput;

// This result is about 50 KiB; callers must allocate it on the heap.
typedef struct SPartitionPhaseTestResult {
  int32_t outputCount;
  int32_t totalPulledBlocks;
  int32_t rowCount;
  int32_t spillCountBeforeSecondBatch;
  int32_t encryptedSpillCountBeforeSecondBatch;
  bool    spillScratchRetainedBeforeSecondBatch;
  bool    spillCryptReleasedBeforeSecondBatch;
  int32_t spillCountDuringOutput;
  int32_t encryptedSpillCountDuringOutput;
  int32_t hashGroupCountOnExit;
  int32_t sortedGroupCountOnExit;
  int32_t residentPageCountOnExit;
  bool    spillFileOpenOnExit;
  int64_t timestamps[PARTITION_PHASE_TEST_MAX_ROWS];
  int32_t groupValues[PARTITION_PHASE_TEST_MAX_ROWS];
  SPartitionPhaseTestOutput outputs[PARTITION_PHASE_TEST_MAX_OUTPUTS];
} SPartitionPhaseTestResult;

typedef enum EPartitionPhaseTestLifecycleAction {
  PARTITION_PHASE_TEST_RESET = 1,
  PARTITION_PHASE_TEST_DESTROY,
} EPartitionPhaseTestLifecycleAction;

typedef struct SPartitionPhaseTestLifecycleResult {
  bool    heldBeforeAction;
  bool    heldAfterAction;
  bool    destroyed;
  int8_t  phaseAfterAction;
  bool    readingAfterAction;
  bool    inputDoneAfterAction;
} SPartitionPhaseTestLifecycleResult;

int32_t partitionSpillTestSelectGroup(const SPartitionSpillTestGroup* pGroups,
                                      int32_t                         numGroups,
                                      uint64_t* pSelectedGroupId);
int32_t partitionSpillTestSelectSequence(const SPartitionSpillTestGroup* pGroups, int32_t numGroups,
                                         uint64_t* pSelectedGroupIds, int32_t numSelections);
int64_t partitionSpillTestBatchTargetBytes(int64_t residentBytes);
int32_t partitionSpillTestRun(const int32_t* pRowsPerPage, int32_t numPages,
                              SPartitionSpillTestResult* pResult);
int32_t partitionSpillTestRunWithWriteLimit(
    const int32_t* pRowsPerPage, int32_t numPages, int32_t writeBatchLimit,
    SPartitionSpillTestResult* pResult);
int32_t partitionSpillTestBatchWrite(bool failWrite, SPartitionSpillTestBatchWriteResult* pResult);
int32_t partitionSpillTestMultipleFragments(
    const int32_t* pRowsPerPage, int32_t firstNumPages, int32_t secondNumPages,
    bool                                    corruptSecondFragment,
    SPartitionSpillTestMultiFragmentResult* pResult);
int32_t partitionSpillTestEncryption(
    int32_t rows, SPartitionSpillTestEncryptionResult* pResult);
int32_t partitionSpillTestRollback(int32_t committedRows,
                                   int32_t pendingRows,
                                   SPartitionSpillTestRollbackResult* pResult);
int32_t partitionSpillTestCorruptRead(const int32_t* pRowsPerPage,
                                      int32_t numPages, int32_t corruption);
int32_t partitionSpillTestRecycleFailure(
    const int32_t* pRowsPerPage, int32_t numPages, int32_t failIndex,
    SPartitionSpillTestRecycleResult* pResult);
int32_t partitionSpillTestNullRoundTrip(const int32_t* pRowsPerPage,
                                        int32_t numPages,
                                        uint32_t nullRowMask,
                                        SPartitionSpillTestNullResult* pResult);
int32_t partitionSpillTestErrnoPropagation(int32_t* pReportedCode);
int32_t partitionSpillTestRecoveredErrno(int32_t* pReportedCode);
int32_t partitionTestNotLoadedBlock(SPartitionNotLoadedTestResult* pResult);
int32_t partitionTestRetainMaxPages(int32_t numPages,
                                    SPartitionMaxPageTestResult* pResult);
int32_t partitionTestSelectGrowthClass(const int32_t* pRowCapacities,
                                       int32_t numClasses,
                                       int32_t currentClass,
                                       int32_t requiredRows);
bool partitionTestNeedsRunSize(const int32_t* pRowCapacities,
                               int32_t numClasses, int32_t currentClass,
                               int64_t residentRows, bool hasPage);
// These helpers temporarily override process-global spill settings. They
// restore them before returning and must not run concurrently in one process.
int32_t partitionTestRunPhased(
    const SPartitionPhaseTestBlock* pBlocks, int32_t numBlocks,
    SPartitionPhaseTestResult* pResult);
int32_t partitionTestRunPhasedWithConfig(
    const SPartitionPhaseTestBlock* pBlocks, int32_t numBlocks,
    const SPartitionPhaseTestConfig* pConfig,
    SPartitionPhaseTestResult* pResult);
int32_t partitionTestHeldBlockLifecycle(
    EPartitionPhaseTestLifecycleAction action,
    SPartitionPhaseTestLifecycleResult* pResult);

#ifdef __cplusplus
}
#endif

#endif
#endif
