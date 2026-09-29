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

// Internals of the partition operator, shared with the BUILD_TEST
// scaffolding in groupoperatorTest.c. Nothing outside those two files may
// include this header: these types and functions are implementation detail
// and change with the operator.

#ifndef _TD_EXECUTOR_GROUPOPERATOR_INT_H_
#define _TD_EXECUTOR_GROUPOPERATOR_INT_H_

#include "crypt.h"
#include "executorInt.h"
#include "operator.h"
#include "tcommon.h"
#include "thash.h"
#include "tpagedbuf.h"

#ifdef __cplusplus
extern "C" {
#endif

// Definitions the test scaffolding drives directly are visible to it, and
// stay file-static in every other build.
#if defined(BUILD_TEST)
#define STATIC_EXCEPT_TEST
#else
#define STATIC_EXCEPT_TEST static
#endif

// Describes one self-contained region appended to the shared spill file.
// The file itself holds only length-prefixed blocks; all fragment metadata
// lives here, in memory, because the spill file never outlives the operator.
typedef struct SPartitionFragment {
  int64_t offset;   // absolute file offset of the first block
  int32_t length;   // encoded blocks in total, bounded by INT32_MAX
  int64_t numRows;  // rows in the fragment; reconciled when reading back
} SPartitionFragment;

typedef struct SPartitionSpillBucket SPartitionSpillBucket;

enum {
  PARTITION_MAX_PAGE_CLASSES = 6,
  // Blocks a side phase accumulates before the batch is drained.
  PARTITION_SIDE_BATCH_BLOCKS = 16,
};

// Packed (class-index, page-id) reference into pInfo->pageClasses. Opaque
// outside partitionEncodePageRef/partitionPageRefClass/partitionPageRefId:
// never mix it with a raw class index or a raw SDiskbasedBuf page id.
typedef int32_t SPartitionPageRef;

typedef struct SPartitionPageClass {
  SDiskbasedBuf* pBuf;
  int32_t        pageSize;
  int32_t        rowCapacity;
  int32_t*       columnOffset;
} SPartitionPageClass;

typedef struct SPartitionDataInfo {
  uint64_t groupId;
  int64_t  numOfRows;
  SArray*  pPageList;
  SArray*  blockForNotLoaded;   // SSDataBlock that data is not loaded
  int32_t  offsetForNotLoaded;  // read offset into blockForNotLoaded
  int64_t  residentRows;        // in-memory rows; excludes spilled fragments
  SArray*  fragments;           // SPartitionFragment entries in spill order
  SPartitionSpillBucket* pSpillBucket;
  struct SPartitionDataInfo* pSpillPrev;
  struct SPartitionDataInfo* pSpillNext;
} SPartitionDataInfo;

// Resources and cursors used exclusively while partition data is spilled.
typedef struct SPartitionSpillInfo {
  TdFilePtr       pFile;      // lazily opened auto-delete file owned here
  int64_t         fileSize;   // append cursor, rewound when a spill fails
  char*           pBuf;       // grow-only serialize/read scratch buffer
  int32_t         bufSize;    // allocated capacity, not current data length
  char*           pBatchBuf;  // grow-only buffer shared by grouped spill writes
  int32_t         batchBufSize;
  SArray*         pBatchGroups;  // selected groups and pending fragment info
  SSpillCryptCtx* pCryptCtx;     // owns the per-file key and crypt buffers
  SSDataBlock*    pBlock;        // reusable spill staging block

  // Read cursor for the current group's fragments; reset these fields
  // together. fragmentDecodedRows == 0 marks a fresh fragment whose read
  // offset has not been primed from the fragment record yet.
  int32_t fragmentIndex;        // fragment within SPartitionDataInfo.fragments
  int64_t fragmentReadOffset;   // absolute file offset of the next block
  int64_t fragmentDecodedRows;  // rows decoded so far within the fragment
} SPartitionSpillInfo;

typedef struct SPartitionSpillStat {
  int32_t spillBatchCount;
  int32_t spillCount;
  int32_t spillFailures;
  int32_t encryptedSpillCount;
  int32_t loadCount;
  int32_t peakResidentPages;
  int64_t residentRows;
  int64_t peakResidentRows;
  int64_t spillPages;
  int64_t spillRows;
  int64_t spillBytes;
  int64_t loadBytes;
  int64_t peakResidentBytes;  // metric: high-water mark of residentBytes
  int64_t growCount[PARTITION_MAX_PAGE_CLASSES];  // metric: page-grow
                                                  // count per target class
  int32_t forcedCurrentSpills;  // metric: spills forced by OOM on write
} SPartitionSpillStat;

typedef enum EPartitionPhaseState {
  PARTITION_PHASE_READING = 0,
  // A full output batch is ready; continue the same phase after draining it.
  PARTITION_PHASE_BATCH_DRAINED,
  // The phase input is exhausted; advance after draining buffered rows.
  PARTITION_PHASE_DRAINED,
  PARTITION_PHASE_DONE,
} EPartitionPhaseState;

typedef struct SPartitionOperatorInfo {
  SOptrBasicInfo binfo;
  SArray*        pGroupCols;
  SArray*        pGroupColVals;     // group column values, SArray<SGroupKeys>
  char*          keyBuf;            // group by keys for hash
  int32_t        groupKeyLen;       // total group by column width
  SHashObj*      pGroupSet;         // locates the result object per group
  SPartitionDataInfo* pLastGroupInfo;  // group used by previous input row

  // Class 0 is the initial page pool for every group. A full page can grow
  // only into a later class with strictly greater row capacity. Only the
  // first numPageClasses entries are constructed.
  SPartitionPageClass pageClasses[PARTITION_MAX_PAGE_CLASSES];
  int32_t        numPageClasses;
  SArray*        sortedGroupArray;  // SPartitionDataInfo sorted by group id
  int32_t        groupIndex;        // group index
  int32_t        pageIndex;         // page index of current group
  SExprSupp      scalarSup;

  int32_t remainRows;
  int32_t orderedRows;
  SArray* pOrderInfoArr;

  bool                 phasedMode;
  int8_t               curPhase;
  uint64_t             outerEpoch;
  bool                 outerEpochSet;
  EPartitionPhaseState phaseState;
  int32_t              batchBlockCount;
  bool                 inputDone;
  SSDataBlock*         pHeldBlock;

  int32_t      numResidentPages;  // pages held by all groups, any class
  int64_t      residentBytes;     // bytes held by those pages; drives spilling
  int64_t      memBudgetBytes;    // shared limit across all page classes
  SPartitionSpillBucket* pSpillBucketHead;
  SPartitionSpillBucket* pSpillBucketTail;
  SPartitionSpillInfo spill;
  SExecTaskInfo*      pTaskInfo;
  SPartitionSpillStat spillStat;
} SPartitionOperatorInfo;

#if defined(BUILD_TEST)
// Production entry points the scaffolding drives, so the tests exercise the
// real implementations rather than copies of them.
int32_t      blockDataFromPagedBuf(SSDataBlock* pBlock, const char* buf,
                                   size_t capacity);
SSDataBlock* buildPartitionResultForNotLoadBlock(
    SPartitionDataInfo* pGroupInfo);
void         cleanupPartitionGroupInfo(SPartitionDataInfo* pGroup);
void         doHashPartition(SOperatorInfo* pOperator, SSDataBlock* pBlock);
void         freeGroupKey(void* param);
int32_t      initGroupOptrInfo(SArray** pGroupColVals, int32_t* keyLen,
                               char** keyBuf, const SArray* pGroupColList);
void         partitionCleanupGroupSet(SHashObj* pGroupSet);
void         partitionFreeSpillResources(SPartitionSpillInfo* pSpill);
void         clearPartitionOperator(SPartitionOperatorInfo* pInfo);
void         destroyPartitionOperatorInfo(void* param);
int32_t      resetPartitionOperState(SOperatorInfo* pOper);
int32_t      partitionGetOrCreateGroupInfo(SPartitionOperatorInfo* pInfo,
                                           SPartitionDataInfo**    pGroupInfo,
                                           int32_t                 len);
SPartitionPageRef partitionEncodePageRef(int32_t classIndex, int32_t pageId);
int64_t      partitionGetSpillBatchTargetBytes(int64_t residentBytes);
int32_t      partitionSpillGroupBatch(SPartitionOperatorInfo* pInfo);
void*        partitionGetWritablePage(SPartitionOperatorInfo* pInfo,
                                      SPartitionDataInfo*     pGroupInfo);
int32_t      phasedHashPartitionNext(SOperatorInfo* pOperator,
                                     SSDataBlock** ppRes);
int32_t      partitionReadAt(TdFilePtr pFile, int64_t offset, void* pData,
                             int32_t length);
int32_t      partitionReadFragmentBlock(SPartitionOperatorInfo* pInfo,
                                        SPartitionDataInfo*     pGroup);
SPartitionDataInfo* partitionSelectSpillGroup(SPartitionOperatorInfo* pInfo);
int32_t partitionSetSpillGroupLevel(SPartitionOperatorInfo* pInfo,
                                    SPartitionDataInfo* pGroup,
                                    int32_t growLevel);
int32_t partitionSelectGrowthClass(
    const SPartitionOperatorInfo* pInfo, int32_t currentClass,
    int32_t requiredRows);
bool partitionNeedsRunSize(const SPartitionOperatorInfo* pInfo,
                           const SPartitionDataInfo* pGroup);
void partitionRemoveSpillGroup(SPartitionOperatorInfo* pInfo,
                               SPartitionDataInfo* pGroup);
void partitionClearSpillBuckets(SPartitionOperatorInfo* pInfo);
int32_t      partitionSpillGroup(SPartitionOperatorInfo* pInfo,
                                 SPartitionDataInfo*     pGroup);
int32_t      partitionWriteAt(TdFilePtr pFile, int64_t offset,
                              const void* pData, int32_t length);
int32_t*     setupColumnOffset(const SSDataBlock* pBlock, int32_t rowCapacity);

// Debug/test invariants over the page-accounting model: residentBytes/
// numResidentPages against the pages actually reachable from a caller-
// supplied group list, and a group's spill-bucket level against its last
// resident page's class. Read-only; each returns false on mismatch
// instead of asserting.
bool partitionCheckResidentBytesInvariant(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* const* pGroups,
    int32_t numGroups);
bool partitionCheckResidentPagesInvariant(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* const* pGroups,
    int32_t numGroups);
bool partitionCheckSpillBucketLevelInvariant(
    const SPartitionOperatorInfo* pInfo, const SPartitionDataInfo* pGroup);

// One-shot failure injection, armed by the scaffolding and consumed by the
// two production seams below. Both disarm on first use, so a helper only
// affects its own call. Returning true means "fail this operation now".
bool partitionSpillTestConsumeWriteFailure(TdFilePtr pFile, int64_t offset,
                                           const void* pData, int32_t length);
bool partitionSpillTestConsumeRecycleFailure(int32_t pageIndex);
void partitionSpillTestResetIoCalls(void);
void partitionSpillTestGetIoCalls(int32_t* pWriteCalls, int32_t* pReadCalls);
void partitionSpillTestSetWriteBatchLimit(int32_t writeBatchLimit);
void partitionTestSetPrepareOutputFailure(int32_t code);
void partitionTestSetPageAllocationFailure(int32_t code);
void partitionTestSetSpillReadFailure(int32_t code);
void partitionTestSetSpillCryptFailure(int32_t code);
#endif

#ifdef __cplusplus
}
#endif

#endif  // _TD_EXECUTOR_GROUPOPERATOR_INT_H_
