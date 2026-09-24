/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include "gtest/gtest.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <map>
#include <new>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "executil.h"
#include "executor.h"
#include "executorInt.h"
#include "filter.h"
#include "functionMgt.h"
#include "interpResultStore.h"
#include "libs/new-stream/dataSink.h"
#include "nodes.h"
#include "operator.h"
#include "osEnv.h"
#include "osMemory.h"
#include "planner.h"
#include "querynodes.h"
#include "query.h"
#include "querytask.h"
#include "streamMsg.h"
#include "stub.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "ttime.h"
#include "tsort.h"
#include "tmsg.h"
#include "windowfunc.h"

extern "C" SHashObj *gStreamGrpTableHash;
extern "C" char      tsStableTagFilterCache;
extern "C" int32_t getTableList(void *pVnode, SScanPhysiNode *pScanNode, SNode *pTagCond, SNode *pTagIndexCond,
                                STableListInfo *pListInfo, uint8_t *digest, const char *idstr, SStorageAPI *pStorageAPI,
                                void *pStreamInfo, int64_t txnId);
extern "C" void handleRemoteRowRes(SScalarFetchParam* pParam, STaskSubJobCtx* ctx, SRetrieveTableRsp* pRsp,
                                    bool* fetchDone);
extern "C" int32_t   extWinTestGetMergeAlignedAggBufferState(SOperatorInfo *pOperator, int32_t *pPageCount,
                                                             int32_t *pCurrentPageId, int32_t *pCurrentPageUsed);
extern "C" int32_t   extWinTestGetBroadcastStat(SOperatorInfo *pOperator, int64_t *pScanBlocks, int64_t *pScanRows,
                                                int64_t *pReplayBlocks, int64_t *pReplayRows, int64_t *pActualBudget,
                                                int64_t *pPayloadPeak, int64_t *pFlushBytes, int64_t *pLoadBytes,
                                                int64_t *pPredicateEvals);
extern "C" int32_t   extWinTestGetBroadcastRegisteredPageCount(SOperatorInfo *pOperator, int32_t *pRegisteredPages);
extern "C" int32_t   extWinTestReplaceActiveBroadcastState(SOperatorInfo *pOperator, SSDataBlock *pBlock,
                                                           int32_t *pRegisteredPages);
extern "C" int32_t   extWinTestGetBroadcastPageMeta(SOperatorInfo *pOperator, uint64_t triggerGroupId,
                                                    uint64_t calcGroupId, int32_t pageIndex, int32_t *pPageCount,
                                                    SBlockID *pId, STimeWindow *pWindow, int64_t *pVersion,
                                                    int16_t *pDataLoad, uint8_t *pScanFlag);
extern "C" int32_t   extWinTestGetBroadcastReplayObservation(SOperatorInfo *pOperator, int32_t observationIndex,
                                                             int32_t *pObservationCount, int32_t *pPageIndex,
                                                             SBlockID *pId, STimeWindow *pWindow, int64_t *pVersion,
                                                             int16_t *pDataLoad, uint8_t *pScanFlag);
extern "C" int32_t   extWinTestRunBroadcastPayloadTransition(int64_t firstContribution, int64_t secondContribution,
                                                             int64_t *pCurrentAfterFirst, int64_t *pCurrentAfterSecond,
                                                             int64_t *pCurrentAfterSecondDestroy,
                                                             int64_t *pPeakAfterSecondDestroy);
extern "C" int32_t   extWinTestGetWindowConditionEvals(SOperatorInfo *pOperator, int64_t *pEvaluations);
extern "C" int32_t   extWinTestGetWindowDecisionState(SOperatorInfo *pOperator, int32_t *pCalcContexts,
                                                      int32_t *pReadyContexts, int64_t *pTotalWindows,
                                                      int64_t *pSettledWindows);
extern "C" void      exchangeTestSetStreamFetchResetMode(SResFetchReq *pReq, bool *pFetchSent,
                                                         const SStreamRuntimeInfo *pRuntime);
extern "C" void     *exchangeTestCreateMultiGroupOwner(bool staticQualified, bool runtimeMultiGroup, bool runnerSource);
extern "C" void     *exchangeTestCreateMultiSourceOwner(bool staticQualified, bool runtimeMultiGroup, bool runnerSource,
                                                        int32_t sourceCount, const uint64_t *pGroupOrder,
                                                        int32_t groupCount);
extern "C" int32_t   exchangeTestAcceptFetchResponse(void *pOwner, const void *pRsp, int32_t rspLen);
extern "C" int32_t   exchangeTestAcceptSourceFetchResponse(void *pOwner, int32_t sourceIndex, const void *pRsp,
                                                           int32_t rspLen);
extern "C" int32_t   exchangeTestPopMultiGroupResult(void *pOwner, SSDataBlock **ppBlock,
                                                     const SArray **ppPublishedLineage);
extern "C" int32_t   exchangeTestLoadNextMultiGroupResult(void *pOwner, SSDataBlock **ppBlock);
extern "C" int32_t   exchangeTestGetResultQueueSizes(void *pOwner, int32_t *pStrictItems, int32_t *pOrdinaryBlocks);
extern "C" int32_t   exchangeTestEnableFilterLimitGuards(void *pOwner);
extern "C" int32_t   exchangeTestResetMultiGroupOwner(void *pOwner);
extern "C" void      exchangeTestDestroyMultiGroupOwner(void *pOwner);
extern "C" void      exchangeTestSetRuntimeLineageAlias(void *pOwner, SArray *pLineage);
extern "C" const SArray *exchangeTestGetRuntimeLineageAlias(void *pOwner);
extern "C" int32_t       exchangeTestNormalizeFetchHeader(const void *pRsp, int32_t rspLen, int64_t *pNumOfCols);
extern "C" int32_t       exchangeTestGetLocalFetchResponseLen(const void *pRsp, int32_t rspLen, uint32_t *pLogicalLen);
extern "C" bool          exchangeTestIsStrictResultQueueAllocated(void *pOwner);
extern "C" bool          exchangeTestCanPollSource(void *pOwner, int32_t sourceIndex);
extern "C" bool          exchangeTestAllSourcesExhausted(void *pOwner);
extern "C" int32_t       exchangeTestBeginDelayedDestroy(void *pOwner);
extern "C" int32_t       exchangeTestFinishDelayedDestroy(void *pOwner);
extern "C" int32_t       blockDecodeInternalBounded(SSDataBlock *pBlock, const char *pData, size_t dataLen,
                                                    const char **pEndPos);
extern "C" int32_t scanTestOpenOrRebindTableReader(STableScanBase *pBase, STableKeyInfo *pTables, int32_t numOfTables,
                                                   SSDataBlock *pBlock, const char *pTaskId, SHashObj **ppIgnoreTables,
                                                   bool *pOpened);
extern "C" int32_t scanTestDoInitReader(STableScanInfo *pInfo, SExecTaskInfo *pTaskInfo, int32_t *pNum,
                                        STableKeyInfo **pList);
extern "C" void    scanTestResetTableScanReader(STableScanInfo *pInfo, bool reuseReader);
extern "C" void    scanTestResetTableMergeReader(STableMergeScanInfo *pInfo, bool reuseReader);
extern "C" void    scanTestBindFreshReaderStats(STableScanBase *pBase);
extern "C" int32_t scanTestBeginReaderStatsEpoch(STableScanBase *pBase);
extern "C" void    scanTestSnapshotReaderStats(const STableScanBase *pBase, STableScanAnalyzeInfo *pOut);
extern "C" int32_t scanTestSnapshotReaderStatsCode(const STableScanBase *pBase, STableScanAnalyzeInfo *pOut);
extern "C" void    scanTestFlushReaderStats(STableScanBase *pBase);
extern "C" int32_t scanTestCloseTableScanReader(STableScanInfo *pInfo);
extern "C" void    scanTestDestroyTableScanBase(STableScanBase *pBase);
extern "C" int32_t scanTestFlushAndCloseTmsReader(STableScanBase *pBase, STsdbReader **ppReader, bool sharedReader);
extern "C" int32_t scanTestResetTableScanOperatorState(SOperatorInfo *pOper);
extern "C" int32_t scanTestResetTableMergeScanOperatorState(SOperatorInfo *pOper);
extern "C" void    scanTestStartGroupTableMergeScan(SOperatorInfo *pOper);
extern "C" int32_t scanTestSetTableScanBlockGroupId(const STableScanInfo *pInfo, SSDataBlock *pBlock);
extern "C" int32_t createStreamGrpTableListFromCond(SScanPhysiNode *pScanNode, SReadHandle *pHandle,
                                                    SExecTaskInfo *pTaskInfo, SStreamRuntimeFuncInfo *pStream,
                                                    STableListInfo *pTableListInfo, SNode *pTagCond,
                                                    SNode *pTagIndexCond, int32_t grpNum, bool hasCalcGrp);
extern "C" int32_t createStreamGrpTableListFromTrig(SExecTaskInfo *pTaskInfo, SStreamRuntimeFuncInfo *pStream,
                                                    STableListInfo *pTableListInfo, int32_t grpNum, bool hasCalcGrp);
extern "C" int32_t createStreamMultiGrpTableListInfo(SScanPhysiNode *pScanNode, SNodeList *pGroupTags, bool groupSort,
                                                     SReadHandle *pHandle, STableListInfo *pTableListInfo,
                                                     SNode *pTagCond, SNode *pTagIndexCond, SExecTaskInfo *pTaskInfo,
                                                     SHashObj *groupIdMap);
extern "C" int32_t doTableScanNext(SOperatorInfo* pOperator,
                                    SSDataBlock** ppRes);
extern "C" int32_t doTableMergeScanNext(SOperatorInfo* pOperator,
                                         SSDataBlock** ppRes);
extern "C" int32_t openSortMergeOperator(SOperatorInfo* pOperator);

namespace {

int32_t         gStreamGroupTableListExistsCalls = 0;
SScanPhysiNode *gStreamGroupTableListScan = nullptr;
Stub           *gExternalWindowActiveStub = nullptr;

struct ExternalWindowStoreCreateSpyState {
  int32_t calls = 0;
  int32_t pageSize = 0;
  int64_t inMemBufSize = 0;
};

struct ExternalWindowDelegatingFailureState {
  int32_t              calls = 0;
  int32_t              failOnCall = 0;
  std::vector<int32_t> pageIndexes;
};

ExternalWindowStoreCreateSpyState    gExternalWindowCreateSpy;
ExternalWindowDelegatingFailureState gExternalWindowAppendSpy;
ExternalWindowDelegatingFailureState gExternalWindowGetSpy;
int64_t                              gExternalWindowHashIterateCalls = 0;
int32_t                              gExternalWindowGroupProjectionCalls = 0;
int32_t                              gExternalWindowPoolDestroyCalls = 0;
int32_t                              gExternalWindowBufferPageSizeCalls = 0;
int32_t                              gExternalWindowFreePageListAppendCalls = 0;

bool countExistingStreamGroupTable(void *pVnode, tb_uid_t uid) {
  (void)pVnode;
  (void)uid;
  ++gStreamGroupTableListExistsCalls;
  return true;
}

struct TableScanResetSpy {
  int32_t calls = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  bool    observedReuse = false;
};

TableScanResetSpy *gTableScanResetSpy = nullptr;

int32_t observeTableScanReset(SOperatorInfo *pOperator) {
  ++gTableScanResetSpy->calls;
  gTableScanResetSpy->observedReuse = pOperator->pTaskInfo->reuseReaderOnReset;
  return gTableScanResetSpy->code;
}

struct ScanReaderLifecycleSpy {
  int32_t               openCalls = 0;
  int32_t               closeCalls = 0;
  int32_t               setListCalls = 0;
  int32_t               resetStatusCalls = 0;
  int32_t               setExecInfoCalls = 0;
  int32_t               filesetDelimitedCalls = 0;
  int32_t               setNotifyCalls = 0;
  int32_t               eventSequence = 0;
  int32_t               setExecInfoSequence = 0;
  int32_t               closeSequence = 0;
  int64_t               lastSetExecInfoFileLoadBlocks = 0;
  int32_t               setListCode = TSDB_CODE_SUCCESS;
  int32_t               resetStatusCode = TSDB_CODE_SUCCESS;
  void                 *openedReader = reinterpret_cast<void *>(0x7100);
  const void           *openTables = nullptr;
  const void           *setListTables = nullptr;
  const void           *openBlock = nullptr;
  const void           *openIgnoreTables = nullptr;
  SQueryTableDataCond  *openCond = nullptr;
  SQueryTableDataCond  *resetCond = nullptr;
  int32_t               openTableCount = -1;
  int32_t               setListTableCount = -1;
  STableScanAnalyzeInfo cost = {};
  TsdReaderNotifyCbFn   notifyFn = nullptr;
  void                 *notifyParam = nullptr;
};

ScanReaderLifecycleSpy *gScanReaderLifecycleSpy = nullptr;

int32_t scanReaderOpenSpy(void *, SQueryTableDataCond *pCond, void *pTables, int32_t numOfTables, SSDataBlock *pBlock,
                          void **ppReader, const char *, SHashObj **ppIgnoreTables) {
  ++gScanReaderLifecycleSpy->openCalls;
  gScanReaderLifecycleSpy->openCond = pCond;
  gScanReaderLifecycleSpy->openTables = pTables;
  gScanReaderLifecycleSpy->openBlock = pBlock;
  gScanReaderLifecycleSpy->openIgnoreTables = ppIgnoreTables;
  gScanReaderLifecycleSpy->openTableCount = numOfTables;
  *ppReader = gScanReaderLifecycleSpy->openedReader;
  return TSDB_CODE_SUCCESS;
}

void scanReaderCloseSpy(void *) {
  ++gScanReaderLifecycleSpy->closeCalls;
  gScanReaderLifecycleSpy->closeSequence = ++gScanReaderLifecycleSpy->eventSequence;
}

int32_t scanReaderSetListSpy(void *, const void *pTables, int32_t numOfTables) {
  ++gScanReaderLifecycleSpy->setListCalls;
  gScanReaderLifecycleSpy->setListTables = pTables;
  gScanReaderLifecycleSpy->setListTableCount = numOfTables;
  return gScanReaderLifecycleSpy->setListCode;
}

int32_t scanReaderResetStatusSpy(void *, SQueryTableDataCond *pCond) {
  ++gScanReaderLifecycleSpy->resetStatusCalls;
  gScanReaderLifecycleSpy->resetCond = pCond;
  return gScanReaderLifecycleSpy->resetStatusCode;
}

void scanReaderSetFilesetDelimitedSpy(void *) { ++gScanReaderLifecycleSpy->filesetDelimitedCalls; }

void scanReaderSetNotifyCbSpy(void *, TsdReaderNotifyCbFn notifyFn, void *param) {
  ++gScanReaderLifecycleSpy->setNotifyCalls;
  gScanReaderLifecycleSpy->notifyFn = notifyFn;
  gScanReaderLifecycleSpy->notifyParam = param;
}

void scanReaderSetExecInfoSpy(const void *, STableScanAnalyzeInfo *pOut) {
  ++gScanReaderLifecycleSpy->setExecInfoCalls;
  gScanReaderLifecycleSpy->setExecInfoSequence = ++gScanReaderLifecycleSpy->eventSequence;
  const STableScanAnalyzeInfo &cost = gScanReaderLifecycleSpy->cost;
  pOut->fileLoadBlocks += cost.fileLoadBlocks;
  pOut->fileLoadElapsed += cost.fileLoadElapsed;
  pOut->sttLoadBlocks += cost.sttLoadBlocks;
  pOut->sttLoadElapsed += cost.sttLoadElapsed;
  pOut->memLoadBlocks += cost.memLoadBlocks;
  pOut->memLoadElapsed += cost.memLoadElapsed;
  pOut->smaLoadBlocks += cost.smaLoadBlocks;
  pOut->smaLoadElapsed += cost.smaLoadElapsed;
  pOut->composedBlocks += cost.composedBlocks;
  pOut->composedElapsed += cost.composedElapsed;
  gScanReaderLifecycleSpy->lastSetExecInfoFileLoadBlocks = pOut->fileLoadBlocks;
}

bool appendDisjointStreamGroupTable(void *pVnode, tb_uid_t uid) {
  (void)pVnode;
  (void)uid;
  ++gStreamGroupTableListExistsCalls;
  if (gStreamGroupTableListScan != nullptr) {
    gStreamGroupTableListScan->uid = 9000 + gStreamGroupTableListExistsCalls;
  }
  return true;
}

void appendStreamGroupRead(SStreamRuntimeFuncInfo *runtime, int64_t gid, std::initializer_list<uint64_t> uids) {
  if (runtime->curGrpRead == nullptr) {
    runtime->curGrpRead = taosArrayInit(2, sizeof(SSTriggerGroupReadInfo));
    runtime->curGrpReadOwnership = STREAM_GRP_READ_OWNED_DEEP;
  }
  SSTriggerGroupReadInfo read = {};
  read.gid = gid;
  read.pTables = taosArrayInit(static_cast<int32_t>(uids.size()), sizeof(uint64_t));
  for (uint64_t uid : uids) {
    EXPECT_NE(taosArrayPush(read.pTables, &uid), nullptr);
  }
  EXPECT_NE(taosArrayPush(runtime->curGrpRead, &read), nullptr);
}

void appendStreamGroupCalc(SStreamRuntimeFuncInfo *runtime, int64_t gid) {
  if (runtime->pGroupCalcInfos == nullptr) {
    runtime->pGroupCalcInfos = tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  }
  SSTriggerGroupCalcInfo calc = {};
  EXPECT_EQ(tSimpleHashPut(runtime->pGroupCalcInfos, &gid, sizeof(gid), &calc, sizeof(calc)), TSDB_CODE_SUCCESS);
}

SStreamAncestorContext *makeFetchProjectionContext(int64_t gid, int32_t paramIndex) {
  auto *context = static_cast<SStreamAncestorContext *>(taosMemoryCalloc(1, sizeof(SStreamAncestorContext)));
  EXPECT_NE(context, nullptr);
  if (context == nullptr) return nullptr;
  context->pParamContexts = taosArrayInit(1, sizeof(SStreamAncestorParamContext));
  EXPECT_NE(context->pParamContexts, nullptr);
  if (context->pParamContexts == nullptr) return context;

  SStreamAncestorParamContext param = {};
  param.paramIndex = paramIndex;
  param.leafIdentity.gid = gid;
  param.leafIdentity.triggerType = WINDOW_TYPE_COUNT;
  param.leafIdentity.lineage.pScopes = taosArrayInit(1, sizeof(SScopeInstanceId));
  EXPECT_NE(param.leafIdentity.lineage.pScopes, nullptr);
  SScopeInstanceId scope = {};
  scope.layerIndex = 0;
  scope.triggerType = WINDOW_TYPE_SESSION;
  scope.openingTs = 100;
  scope.nativeDiscriminator = 1;
  EXPECT_NE(taosArrayPush(param.leafIdentity.lineage.pScopes, &scope), nullptr);
  param.pSnapshots = taosArrayInit(1, sizeof(SWindowAncestorSnapshot));
  EXPECT_NE(param.pSnapshots, nullptr);
  SWindowAncestorSnapshot snapshot = {};
  snapshot.layerIndex = 0;
  snapshot.triggerType = WINDOW_TYPE_SESSION;
  snapshot.placeholderMask = PLACE_HOLDER_WSTART;
  snapshot.values.window.start = 100;
  EXPECT_NE(taosArrayPush(param.pSnapshots, &snapshot), nullptr);
  EXPECT_NE(taosArrayPush(context->pParamContexts, &param), nullptr);
  return context;
}

SStreamContextPolicy *makeFetchProjectionPolicy(int64_t gid, int32_t paramIndex) {
  auto *policy = static_cast<SStreamContextPolicy *>(taosMemoryCalloc(1, sizeof(SStreamContextPolicy)));
  EXPECT_NE(policy, nullptr);
  if (policy == nullptr) return nullptr;
  policy->pEntries = taosArrayInit(paramIndex + 1, sizeof(SStreamContextPolicyEntry));
  EXPECT_NE(policy->pEntries, nullptr);
  if (policy->pEntries == nullptr) return policy;
  for (int32_t i = 0; i <= paramIndex; ++i) {
    SStreamContextPolicyEntry entry = {};
    entry.gid = gid;
    entry.paramIndex = i;
    entry.contextPolicy = i == paramIndex ? STREAM_CONTEXT_POLICY_ANCESTOR : STREAM_CONTEXT_POLICY_NONE;
    EXPECT_NE(taosArrayPush(policy->pEntries, &entry), nullptr);
  }
  return policy;
}

int32_t execRecordTestNext(SOperatorInfo* pOperator,
                           SSDataBlock** ppBlock) {
  *ppBlock = static_cast<SSDataBlock*>(pOperator->info);
  return TSDB_CODE_SUCCESS;
}

int32_t execRecordTestNextExt(SOperatorInfo* pOperator, SOperatorParam*,
                              SSDataBlock** ppBlock) {
  *ppBlock = static_cast<SSDataBlock*>(pOperator->info);
  return TSDB_CODE_SUCCESS;
}

int32_t execRecordTestLongJmpNext(SOperatorInfo* pOperator,
                                  SSDataBlock**) {
  T_LONG_JMP(pOperator->pTaskInfo->env, TSDB_CODE_FAILED);
  return TSDB_CODE_FAILED;
}

int32_t execRecordTestPullDownstream(SOperatorInfo* pOperator,
                                     SSDataBlock** ppBlock) {
  return pOperator->pDownstream[0]->fpSet.getNextFn(
      pOperator->pDownstream[0], ppBlock);
}

int32_t createInterpStoreSequenceBlock(uint64_t groupId,
                                       int32_t firstValue,
                                       int32_t rows,
                                       SSDataBlock** ppBlock) {
  SSDataBlock* pBlock = nullptr;
  int32_t code = createDataBlock(&pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SColumnInfoData col =
      createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  code = blockDataAppendColInfo(pBlock, &col);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return code;
  }

  code = blockDataEnsureCapacity(pBlock, rows);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return code;
  }

  SColumnInfoData* pCol =
      static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 0));
  for (int32_t i = 0; i < rows; ++i) {
    int32_t value = firstValue + i;
    code = colDataSetVal(pCol, i,
                         reinterpret_cast<const char*>(&value), false);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
      return code;
    }
  }

  pBlock->info.rows = rows;
  pBlock->info.id.groupId = groupId;
  *ppBlock = pBlock;
  return TSDB_CODE_SUCCESS;
}

int32_t createInterpStoreTestBlock(uint64_t groupId, int32_t firstValue,
                                   SSDataBlock** ppBlock) {
  return createInterpStoreSequenceBlock(groupId, firstValue, 2,
                                        ppBlock);
}

int32_t interpStoreFirstValue(const SSDataBlock* pBlock) {
  SColumnInfoData* pCol =
      static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 0));
  return *reinterpret_cast<int32_t*>(colDataGetData(pCol, 0));
}

int32_t createInterpStoreVarBlock(uint64_t groupId, int32_t seed,
                                  int32_t rows,
                                  SSDataBlock** ppBlock) {
  SSDataBlock* pBlock = nullptr;
  int32_t code = createDataBlock(&pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SColumnInfoData intCol =
      createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  SColumnInfoData strCol =
      createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, 64, 2);
  code = blockDataAppendColInfo(pBlock, &intCol);
  if (code == TSDB_CODE_SUCCESS) {
    code = blockDataAppendColInfo(pBlock, &strCol);
  }
  if (code == TSDB_CODE_SUCCESS) {
    code = blockDataEnsureCapacity(pBlock, rows);
  }
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return code;
  }

  SColumnInfoData* pInt = static_cast<SColumnInfoData*>(
      taosArrayGet(pBlock->pDataBlock, 0));
  SColumnInfoData* pStr = static_cast<SColumnInfoData*>(
      taosArrayGet(pBlock->pDataBlock, 1));
  if (pInt == nullptr || pStr == nullptr) {
    blockDataDestroy(pBlock);
    return terrno;
  }

  for (int32_t row = 0; row < rows; ++row) {
    int32_t intValue = seed + row;
    char strValue[64] = {0};
    int32_t strLen = 24 + row % 16;
    varDataSetLen(strValue, strLen);
    memset(varDataVal(strValue), 'a' + (seed + row) % 26, strLen);
    code = colDataSetVal(pInt, row,
                         reinterpret_cast<const char*>(&intValue),
                         row % 4 == 1);
    if (code == TSDB_CODE_SUCCESS) {
      code = colDataSetVal(pStr, row, strValue, row % 3 == 2);
    }
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
      return code;
    }
  }

  pBlock->info.rows = rows;
  pBlock->info.window = {seed, seed + rows - 1};
  pBlock->info.id.uid = groupId - 1;
  pBlock->info.id.groupId = groupId;
  pBlock->info.id.baseGId = groupId + 1;
  pBlock->info.dataLoad = 1;
  pBlock->info.scanFlag = 7;
  pBlock->info.scanPhase = SCAN_PHASE_PREV;
  pBlock->info.blankFill = true;
  pBlock->info.pks[0].type = TSDB_DATA_TYPE_INT;
  pBlock->info.pks[0].val = seed;
  pBlock->info.pks[1].type = TSDB_DATA_TYPE_INT;
  pBlock->info.pks[1].val = seed + rows - 1;
  pBlock->info.version = seed + 1000;
  pBlock->info.childId = seed + 2000;
  pBlock->info.calWin = {seed - 1, seed + rows};
  pBlock->info.watermark = seed + 3000;
  tstrncpy(pBlock->info.parTbName, "interp_store_source",
           sizeof(pBlock->info.parTbName));
  *ppBlock = pBlock;
  return TSDB_CODE_SUCCESS;
}

int32_t setInterpStoreVarPks(SSDataBlock* pBlock,
                             const char* pFirst,
                             const char* pLast) {
  uint32_t firstLen = (uint32_t)strlen(pFirst) + 1;
  uint32_t lastLen = (uint32_t)strlen(pLast) + 1;
  uint8_t* pFirstCopy =
      static_cast<uint8_t*>(taosMemoryMalloc(firstLen));
  if (pFirstCopy == nullptr) {
    return terrno;
  }
  uint8_t* pLastCopy =
      static_cast<uint8_t*>(taosMemoryMalloc(lastLen));
  if (pLastCopy == nullptr) {
    int32_t code = terrno;
    taosMemoryFree(pFirstCopy);
    return code;
  }

  memcpy(pFirstCopy, pFirst, firstLen);
  memcpy(pLastCopy, pLast, lastLen);
  pBlock->info.pks[0].type = TSDB_DATA_TYPE_VARCHAR;
  pBlock->info.pks[0].nData = firstLen;
  pBlock->info.pks[0].pData = pFirstCopy;
  pBlock->info.pks[1].type = TSDB_DATA_TYPE_VARCHAR;
  pBlock->info.pks[1].nData = lastLen;
  pBlock->info.pks[1].pData = pLastCopy;
  return TSDB_CODE_SUCCESS;
}

void expectInterpStoreBlocksEqual(const SSDataBlock* pExpected,
                                  const SSDataBlock* pActual) {
  ASSERT_NE(pExpected, nullptr);
  ASSERT_NE(pActual, nullptr);
  EXPECT_EQ(pActual->info.rows, pExpected->info.rows);
  EXPECT_EQ(pActual->info.window.skey, pExpected->info.window.skey);
  EXPECT_EQ(pActual->info.window.ekey, pExpected->info.window.ekey);
  EXPECT_EQ(pActual->info.id.uid, pExpected->info.id.uid);
  EXPECT_EQ(pActual->info.id.groupId, pExpected->info.id.groupId);
  EXPECT_EQ(pActual->info.id.baseGId, pExpected->info.id.baseGId);
  EXPECT_EQ(pActual->info.rowSize, pExpected->info.rowSize);
  EXPECT_EQ(pActual->info.hasVarCol, pExpected->info.hasVarCol);
  EXPECT_EQ(pActual->info.dataLoad, pExpected->info.dataLoad);
  EXPECT_EQ(pActual->info.scanFlag, pExpected->info.scanFlag);
  EXPECT_EQ(pActual->info.scanPhase, pExpected->info.scanPhase);
  EXPECT_EQ(pActual->info.blankFill, pExpected->info.blankFill);
  EXPECT_EQ(pActual->info.pks[0].type, pExpected->info.pks[0].type);
  EXPECT_EQ(pActual->info.pks[1].type, pExpected->info.pks[1].type);
  if (IS_VAR_DATA_TYPE(pExpected->info.pks[0].type)) {
    ASSERT_EQ(pActual->info.pks[0].nData,
              pExpected->info.pks[0].nData);
    ASSERT_EQ(pActual->info.pks[1].nData,
              pExpected->info.pks[1].nData);
    EXPECT_EQ(memcmp(pActual->info.pks[0].pData,
                     pExpected->info.pks[0].pData,
                     pExpected->info.pks[0].nData), 0);
    EXPECT_EQ(memcmp(pActual->info.pks[1].pData,
                     pExpected->info.pks[1].pData,
                     pExpected->info.pks[1].nData), 0);
  } else {
    EXPECT_EQ(pActual->info.pks[0].val,
              pExpected->info.pks[0].val);
    EXPECT_EQ(pActual->info.pks[1].val,
              pExpected->info.pks[1].val);
  }
  EXPECT_EQ(pActual->info.version, pExpected->info.version);
  EXPECT_EQ(pActual->info.childId, pExpected->info.childId);
  EXPECT_EQ(pActual->info.calWin.skey, pExpected->info.calWin.skey);
  EXPECT_EQ(pActual->info.calWin.ekey, pExpected->info.calWin.ekey);
  EXPECT_EQ(pActual->info.watermark, pExpected->info.watermark);
  EXPECT_STREQ(pActual->info.parTbName, pExpected->info.parTbName);

  int32_t cols = taosArrayGetSize(pExpected->pDataBlock);
  ASSERT_EQ(taosArrayGetSize(pActual->pDataBlock), cols);
  for (int32_t col = 0; col < cols; ++col) {
    SColumnInfoData* pExpectedCol = static_cast<SColumnInfoData*>(
        taosArrayGet(pExpected->pDataBlock, col));
    SColumnInfoData* pActualCol = static_cast<SColumnInfoData*>(
        taosArrayGet(pActual->pDataBlock, col));
    ASSERT_NE(pExpectedCol, nullptr);
    ASSERT_NE(pActualCol, nullptr);
    EXPECT_EQ(pActualCol->info.type, pExpectedCol->info.type);
    EXPECT_EQ(pActualCol->info.bytes, pExpectedCol->info.bytes);
    EXPECT_EQ(pActualCol->info.slotId, pExpectedCol->info.slotId);
    EXPECT_EQ(pActualCol->info.precision,
              pExpectedCol->info.precision);
    EXPECT_EQ(pActualCol->info.scale, pExpectedCol->info.scale);
    EXPECT_EQ(pActualCol->info.pk, pExpectedCol->info.pk);
    EXPECT_EQ(pActualCol->info.noData, pExpectedCol->info.noData);
    for (int32_t row = 0; row < pExpected->info.rows; ++row) {
      bool expectedNull = colDataIsNull_s(pExpectedCol, row);
      ASSERT_EQ(colDataIsNull_s(pActualCol, row), expectedNull);
      if (expectedNull) {
        continue;
      }
      const char* pExpectedValue = colDataGetData(pExpectedCol, row);
      const char* pActualValue = colDataGetData(pActualCol, row);
      if (IS_VAR_DATA_TYPE(pExpectedCol->info.type)) {
        ASSERT_EQ(varDataTLen(pActualValue),
                  varDataTLen(pExpectedValue));
        EXPECT_EQ(memcmp(pActualValue, pExpectedValue,
                         varDataTLen(pExpectedValue)), 0);
      } else {
        EXPECT_EQ(memcmp(pActualValue, pExpectedValue,
                         pExpectedCol->info.bytes), 0);
      }
    }
  }
}

struct InterpMergeTempDirGuard {
  char       savedPath[PATH_MAX] = {0};
  SDiskSpace savedSpace = {};
  int32_t    code = TSDB_CODE_SUCCESS;

  InterpMergeTempDirGuard() {
    tstrncpy(savedPath, tsTempDir, sizeof(savedPath));
    savedSpace = tsTempSpace;
    tstrncpy(tsTempDir, "/tmp", PATH_MAX);
    code = osUpdate();
  }

  ~InterpMergeTempDirGuard() {
    tstrncpy(tsTempDir, savedPath, PATH_MAX);
    tsTempSpace = savedSpace;
  }
};

struct InterpMergeFakeSource {
  std::vector<SSDataBlock*> blocks;
  size_t                    index = 0;
};

struct InterpOutputGroupCounter {
  STableListInfo list = {};
  int32_t        calls = 0;
};

int32_t interpCountOutputGroups(const STableListInfo* pList) {
  auto* pCounter = reinterpret_cast<InterpOutputGroupCounter*>(
      const_cast<STableListInfo*>(pList));
  ++pCounter->calls;
  return pCounter->list.numOfOuputGroups;
}

struct InterpOrdinaryScanFixture {
  InterpOutputGroupCounter groups;
  SExecTaskInfo            task = {};
  STableScanInfo           scanInfo = {};
  STableScanPhysiNode      scanNode = {};
  SOperatorInfo            root = {};
  SSDataBlock*             pResult = nullptr;
  char                     taskId[32] = "ordinary-phase-skip";

  InterpOrdinaryScanFixture() {
    groups.list.numOfOuputGroups = 3;
    task.id.str = taskId;

    scanInfo.base.cond.mainFirst = true;
    scanInfo.base.pTableListInfo = &groups.list;
    scanInfo.scanInfo.numOfAsc = 1;
    scanInfo.scanInfo.numOfDesc = 0;
    scanInfo.currentGroupId = -1;
    scanInfo.currentPhase = SCAN_PHASE_MAIN;
    scanInfo.groupSort = true;
    STimeWindow emptyWindow = {INT64_MAX, INT64_MIN};
    scanInfo.base.cond.twindows = emptyWindow;
    scanInfo.base.cond.extTwindows[0] = emptyWindow;
    scanInfo.base.cond.extTwindows[1] = emptyWindow;

    scanNode.interpMode = INTERP_MODE_MAIN_FIRST;
    scanNode.groupSort = true;

    setOperatorInfo(&root, "TableScan",
                    QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, false,
                    OP_OPENED, &scanInfo, &task);
    root.pPhyNode = reinterpret_cast<SPhysiNode*>(&scanNode);
    root.fpSet._openFn = optrDummyOpenFn;
  }

  int32_t next() {
    Stub stub;
    stub.set(tableListGetOutputGroups, interpCountOutputGroups);
    return doTableScanNext(&root, &pResult);
  }
};

// Minimal remote-task fixture for the component test. The root operator,
// parameter dispatch, sort handle, and task execution all use production
// implementations; only RPC transport and storage fetch are replaced.
struct InterpRemoteScanFixture {
  SExecTaskInfo task = {};
  SOperatorInfo root = {};
  SSubplan subplan = {};
  STableMergeScanInfo scanInfo = {};
  STableScanPhysiNode scanNode = {};
  SOperatorParam* downstreamParam = nullptr;
  SArray* pSortInfo = nullptr;
  SSDataBlock* pScanBlock = nullptr;
  char taskId[64] = {0};
  uint64_t remoteTaskId = 0;
  uint64_t expectedOuterEpoch = 0;
  uint64_t expectedScanGroupId = 0;
  TSKEY expectedNotifyTs = 0;
  int8_t expectedPhase = SCAN_PHASE_MAIN;
  int32_t notifyCount = 0;
  int32_t readCount = 0;
  int32_t execCode = TSDB_CODE_SUCCESS;
};

// Pass the mock registry through Exchange's transport pointer so each test
// owns its remote fixtures without process-global mutable state.
struct InterpRemoteTransport {
  std::vector<InterpRemoteScanFixture*> scans;
};

// Mock only the storage-facing fetch callback used by the real sort handle.
// If early-stop is not consumed first, returning one block increments
// readCount and makes the unwanted scan observable to the test.
int32_t interpRemoteMergeScanFetch(void* param, SSDataBlock** ppBlock) {
  InterpRemoteScanFixture* pScan =
      static_cast<InterpRemoteScanFixture*>(param);
  ++pScan->readCount;
  *ppBlock = pScan->readCount == 1 ? pScan->pScanBlock : nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t interpUnexpectedReaderOpen(void* pVnode, SQueryTableDataCond*,
                                   void*, int32_t, SSDataBlock*, void**,
                                   const char*, SHashObj**) {
  int32_t* pOpenCount = static_cast<int32_t*>(pVnode);
  ++*pOpenCount;
  return TSDB_CODE_FAILED;
}

// Replace asynchronous RPC delivery while retaining the production request
// codec, operator-param update, and one-block task execution path. This keeps
// ownership equivalent to qworker: decoded pOpParam moves into the task, and
// the send request is destroyed after the simulated remote execution.
int32_t interpRemoteSend(void* param, SEpSet*, int64_t* pTransporterId,
                         SMsgSendInfo* pSendInfo) {
  if (pTransporterId != nullptr) {
    *pTransporterId = 0;
  }

  SResFetchReq req = {};
  int32_t code = tDeserializeSResFetchReq(
      pSendInfo->msgInfo.pData, pSendInfo->msgInfo.len, &req);
  if (code < 0) {
    destroySendMsgInfo(pSendInfo);
    return code;
  }

  InterpRemoteScanFixture* pRemote = nullptr;
  InterpRemoteTransport* pTransport =
      static_cast<InterpRemoteTransport*>(param);
  if (pTransport != nullptr) {
    for (InterpRemoteScanFixture* pCandidate : pTransport->scans) {
      if (pCandidate->remoteTaskId == req.taskId) {
        pRemote = pCandidate;
        break;
      }
    }
  }

  if (pRemote != nullptr) {
    if (req.pOpParam != nullptr) {
      SOperatorParam* pParam = req.pOpParam;
      if (pParam->opType == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN &&
          pParam->value != nullptr) {
        STableScanOperatorParam* pNotify =
            static_cast<STableScanOperatorParam*>(pParam->value);
        bool matched =
            pNotify->paramType == NOTIFY_TYPE_SCAN_PARAM &&
            pNotify->notifyToProcess &&
            pNotify->groupid == pRemote->expectedOuterEpoch &&
            pNotify->notifyPhase == pRemote->expectedPhase &&
            pNotify->notifyScanGroupId ==
                pRemote->expectedScanGroupId &&
            pNotify->notifyTs == pRemote->expectedNotifyTs;
        pRemote->notifyCount += matched ? 1 : 0;
      }
      qUpdateOperatorParam(&pRemote->task, req.pOpParam);
      req.pOpParam = nullptr;
    }
    SArray* pResults = taosArrayInit(1, POINTER_BYTES);
    if (pResults == nullptr) {
      pRemote->execCode = terrno;
    } else {
      uint64_t useconds = 0;
      bool hasMore = false;
      // processOneBlock=true matches one qworker fetch dispatch. A delivered
      // notification must complete PREV before the sort fetch callback runs.
      pRemote->execCode = qExecTaskOpt(
          &pRemote->task, pResults, &useconds, &hasMore, nullptr, true);
      taosArrayDestroy(pResults);
    }
    // Dispatched parameters are owned, and may be freed, by the operator.
    if (!pRemote->task.paramSet) {
      qDestroyOperatorParam(pRemote->task.pOpParam);
    }
    pRemote->task.pOpParam = nullptr;
  } else if (req.pOpParam != nullptr) {
    qDestroyOperatorParam(req.pOpParam);
    req.pOpParam = nullptr;
  }

  tDestroySResFetchReq(&req);
  destroySendMsgInfo(pSendInfo);
  return TSDB_CODE_SUCCESS;
}

int32_t interpMergeFakeNext(SOperatorInfo* pOperator,
                            SSDataBlock** ppBlock) {
  InterpMergeFakeSource* pSource =
      static_cast<InterpMergeFakeSource*>(pOperator->info);
  *ppBlock = pSource->index < pSource->blocks.size()
                 ? pSource->blocks[pSource->index++]
                 : nullptr;
  return TSDB_CODE_SUCCESS;
}

int32_t createInterpMergeTestBlock(int64_t ts, int8_t phase,
                                   SSDataBlock** ppBlock,
                                   uint64_t groupId = 77) {
  SSDataBlock* pBlock = nullptr;
  int32_t code = createDataBlock(&pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  SColumnInfoData col = createColumnInfoData(
      TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1);
  code = blockDataAppendColInfo(pBlock, &col);
  if (code == TSDB_CODE_SUCCESS) {
    code = blockDataEnsureCapacity(pBlock, 1);
  }
  if (code == TSDB_CODE_SUCCESS) {
    SColumnInfoData* pCol = static_cast<SColumnInfoData*>(
        taosArrayGet(pBlock->pDataBlock, 0));
    code = colDataSetVal(pCol, 0,
                         reinterpret_cast<const char*>(&ts), false);
  }
  if (code == TSDB_CODE_SUCCESS) {
    pBlock->info.rows = 1;
    pBlock->info.id.groupId = groupId;
    pBlock->info.scanPhase = phase;
    code = blockDataUpdateTsWindow(pBlock, 0);
  }
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return code;
  }
  *ppBlock = pBlock;
  return TSDB_CODE_SUCCESS;
}

SDataBlockDescNode* createInterpMergeTestDesc(int64_t blockId) {
  SDataBlockDescNode* pDesc = nullptr;
  if (nodesMakeNode(QUERY_NODE_DATABLOCK_DESC,
                    reinterpret_cast<SNode**>(&pDesc)) != 0) {
    return nullptr;
  }
  pDesc->dataBlockId = blockId;
  pDesc->totalRowSize = sizeof(int64_t);
  pDesc->outputRowSize = sizeof(int64_t);

  SSlotDescNode* pSlot = nullptr;
  if (nodesMakeNode(QUERY_NODE_SLOT_DESC,
                    reinterpret_cast<SNode**>(&pSlot)) != 0) {
    nodesDestroyNode(reinterpret_cast<SNode*>(pDesc));
    return nullptr;
  }
  pSlot->slotId = 0;
  pSlot->dataType.type = TSDB_DATA_TYPE_TIMESTAMP;
  pSlot->dataType.bytes = sizeof(int64_t);
  pSlot->output = true;
  if (nodesListMakeStrictAppend(
          &pDesc->pSlots, reinterpret_cast<SNode*>(pSlot)) != 0) {
    nodesDestroyNode(reinterpret_cast<SNode*>(pDesc));
    return nullptr;
  }
  return pDesc;
}

SMergePhysiNode* createInterpMergeTestPhysNode(bool groupSort = false) {
  SMergePhysiNode* pMerge = nullptr;
  if (nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_MERGE,
                    reinterpret_cast<SNode**>(&pMerge)) != 0) {
    return nullptr;
  }
  pMerge->type = MERGE_TYPE_SORT;
  pMerge->groupSort = groupSort;
  pMerge->inputWithGroupId = true;
  pMerge->node.inputTsOrder = ORDER_ASC;
  pMerge->node.outputTsOrder = ORDER_ASC;
  pMerge->node.pOutputDataBlockDesc = createInterpMergeTestDesc(1);
  if (pMerge->node.pOutputDataBlockDesc == nullptr) {
    nodesDestroyNode(reinterpret_cast<SNode*>(pMerge));
    return nullptr;
  }

  SPhysiNode* pChild = nullptr;
  int32_t code = nodesMakeNode(
      QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN,
      reinterpret_cast<SNode**>(&pChild));
  if (code == 0) {
    code = nodesCloneNode(
        reinterpret_cast<SNode*>(pMerge->node.pOutputDataBlockDesc),
        reinterpret_cast<SNode**>(&pChild->pOutputDataBlockDesc));
  }
  if (code == 0) {
    code = nodesListMakeStrictAppend(
        &pMerge->node.pChildren, reinterpret_cast<SNode*>(pChild));
    pChild = nullptr;
  }
  if (code != 0) {
    nodesDestroyNode(reinterpret_cast<SNode*>(pChild));
    nodesDestroyNode(reinterpret_cast<SNode*>(pMerge));
    return nullptr;
  }

  SColumnNode* pColumn = nullptr;
  STargetNode* pTarget = nullptr;
  SOrderByExprNode* pOrder = nullptr;
  code = nodesMakeNode(QUERY_NODE_COLUMN,
                       reinterpret_cast<SNode**>(&pColumn));
  if (code == 0) {
    pColumn->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    pColumn->node.resType.bytes = sizeof(int64_t);
    pColumn->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
    pColumn->slotId = 0;
    pColumn->dataBlockId = 1;
    pColumn->isPrimTs = true;
    code = nodesMakeNode(QUERY_NODE_TARGET,
                         reinterpret_cast<SNode**>(&pTarget));
  }
  if (code == 0) {
    pTarget->dataBlockId = 1;
    pTarget->slotId = 0;
    pTarget->pExpr = reinterpret_cast<SNode*>(pColumn);
    pColumn = nullptr;
    code = nodesListMakeStrictAppend(
        &pMerge->pTargets, reinterpret_cast<SNode*>(pTarget));
    pTarget = nullptr;
  }
  if (code == 0) {
    code = nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR,
                         reinterpret_cast<SNode**>(&pOrder));
  }
  if (code == 0) {
    code = nodesCloneNode(
        reinterpret_cast<SNode*>(
            reinterpret_cast<STargetNode*>(
                nodesListGetNode(pMerge->pTargets, 0))->pExpr),
        &pOrder->pExpr);
  }
  if (code == 0) {
    pOrder->order = ORDER_ASC;
    pOrder->nullOrder = NULL_ORDER_FIRST;
    code = nodesListMakeStrictAppend(
        &pMerge->pMergeKeys, reinterpret_cast<SNode*>(pOrder));
    pOrder = nullptr;
  }
  if (code != 0) {
    nodesDestroyNode(reinterpret_cast<SNode*>(pColumn));
    nodesDestroyNode(reinterpret_cast<SNode*>(pTarget));
    nodesDestroyNode(reinterpret_cast<SNode*>(pOrder));
    nodesDestroyNode(reinterpret_cast<SNode*>(pMerge));
    return nullptr;
  }
  return pMerge;
}

struct InterpNodeDestroySpy {
  static InterpNodeDestroySpy* active;
  Stub stub;
  SNode* failed = nullptr;
  int32_t destroys = 0;

  InterpNodeDestroySpy() {
    active = this;
    stub.set(nodesDestroyNode, destroy);
  }

  ~InterpNodeDestroySpy() { active = nullptr; }

  static void destroy(SNode* node) {
    if (node != nullptr && node == active->failed &&
        ++active->destroys > 1) {
      return;
    }
    active->stub.reset(nodesDestroyNode);
    nodesDestroyNode(node);
    active->stub.set(nodesDestroyNode, destroy);
  }
};

InterpNodeDestroySpy* InterpNodeDestroySpy::active = nullptr;

template <ENodeType failType>
int32_t failInterpNodeAppend(SNodeList** ppList, SNode* pNode) {
  // Keep StrictAppend's consume-on-failure contract in the injected failure.
  bool fail = nodeType(pNode) == failType;
  if (fail) {
    InterpNodeDestroySpy::active->failed = pNode;
  }
  int32_t code = fail
                     ? TSDB_CODE_OUT_OF_MEMORY
                     : nodesListMakeAppend(ppList, pNode);
  if (code != TSDB_CODE_SUCCESS) {
    nodesDestroyNode(pNode);
  }
  return code;
}

void* failInterpStoreCalloc(int64_t, int64_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

Stub*   gSortMergeCleanupStub = nullptr;
int32_t gSortMergeDestroyCalls = 0;

int32_t failSortMergeOpen(SSortHandle*) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

void countAndDestroySortMergeHandle(SSortHandle* pHandle) {
  ++gSortMergeDestroyCalls;
  gSortMergeCleanupStub->reset(tsortDestroySortHandle);
  tsortDestroySortHandle(pHandle);
}

struct SortMergeCleanupStubGuard {
  Stub stub;

  SortMergeCleanupStubGuard() {
    gSortMergeCleanupStub = &stub;
    stub.set(tsortOpen, failSortMergeOpen);
    stub.set(tsortDestroySortHandle,
             countAndDestroySortMergeHandle);
  }

  ~SortMergeCleanupStubGuard() {
    gSortMergeCleanupStub = nullptr;
  }
};

int64_t makeInterpTestTimestamp(timezone_t timezone, int32_t year,
                                int32_t month, int32_t day,
                                int32_t hour = 0) {
  struct tm local = {};
  local.tm_year = year - 1900;
  local.tm_mon = month - 1;
  local.tm_mday = day;
  local.tm_hour = hour;
  local.tm_isdst = -1;
  time_t seconds = taosMktime(&local, timezone);
  EXPECT_NE(seconds, static_cast<time_t>(-1));
  return static_cast<int64_t>(seconds) * 1000;
}

struct StableTagFilterWarmupMock {
  int32_t getCacheCalls = 0;
  int32_t warmupCalls = 0;
  int32_t getTableTagsCalls = 0;
  bool    warmed = false;
  bool    cacheHitAfterWarmup = true;
};

int32_t mockGetStableCachedTableList(void *pVnode, tb_uid_t suid, const uint8_t *pTagCondKey, int32_t tagCondKeyLen,
                                     const uint8_t *pKey, int32_t keyLen, SArray *pList, bool *acquired,
                                     bool *needWarmup) {
  (void)suid;
  (void)pTagCondKey;
  (void)tagCondKeyLen;
  (void)pKey;
  (void)keyLen;
  StableTagFilterWarmupMock *pMock = static_cast<StableTagFilterWarmupMock *>(pVnode);
  pMock->getCacheCalls += 1;
  *acquired = false;
  if (needWarmup != nullptr) {
    *needWarmup = !pMock->warmed;
  }

  if (pMock->warmed && pMock->cacheHitAfterWarmup) {
    uint64_t uid = 1001;
    if (taosArrayPush(pList, &uid) == nullptr) {
      return terrno;
    }
    *acquired = true;
    if (needWarmup != nullptr) {
      *needWarmup = false;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mockWarmupStableCachedTableList(void *pVnode, uint64_t suid, const void *pTagCondKey, int32_t tagCondKeyLen,
                                        const uint8_t *pKey, int32_t keyLen, const SArray *pTagColIds, SArray *pList,
                                        bool *acquired) {
  (void)suid;
  (void)pTagCondKey;
  (void)tagCondKeyLen;
  (void)pKey;
  (void)keyLen;
  StableTagFilterWarmupMock *pMock = static_cast<StableTagFilterWarmupMock *>(pVnode);
  pMock->warmupCalls += 1;
  pMock->warmed = true;
  EXPECT_NE(pTagColIds, nullptr);
  EXPECT_EQ(taosArrayGetSize(pTagColIds), 1);
  if (acquired != nullptr) {
    *acquired = false;
  }
  if (pMock->cacheHitAfterWarmup) {
    uint64_t uid = 1001;
    if (taosArrayPush(pList, &uid) == nullptr) {
      return terrno;
    }
    if (acquired != nullptr) {
      *acquired = true;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t mockGetTableTags(void *pVnode, uint64_t suid, SArray *uidList, int64_t txnId) {
  (void)suid;
  (void)uidList;
  (void)txnId;
  StableTagFilterWarmupMock *pMock = static_cast<StableTagFilterWarmupMock *>(pVnode);
  pMock->getTableTagsCalls += 1;
  return TSDB_CODE_SUCCESS;
}

int32_t mockPutStableCachedTableList(void *pVnode, uint64_t suid, const void *pTagCondKey, int32_t tagCondKeyLen,
                                     const void *pKey, int32_t keyLen, SArray *pUidList, SArray **pTagColIds) {
  (void)pVnode;
  (void)suid;
  (void)pTagCondKey;
  (void)tagCondKeyLen;
  (void)pKey;
  (void)keyLen;
  (void)pUidList;
  (void)pTagColIds;
  return TSDB_CODE_SUCCESS;
}

SNode *makeTagColumn(col_id_t colId, int8_t type, int32_t bytes) {
  SColumnNode *pColumn = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&pColumn)), TSDB_CODE_SUCCESS);
  pColumn->node.resType.type = type;
  pColumn->node.resType.bytes = bytes;
  pColumn->colId = colId;
  pColumn->colType = COLUMN_TYPE_TAG;
  return reinterpret_cast<SNode *>(pColumn);
}

SNode *makeIntValue(int32_t value) {
  SValueNode *pValue = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pValue)), TSDB_CODE_SUCCESS);
  pValue->node.resType.type = TSDB_DATA_TYPE_INT;
  pValue->node.resType.bytes = sizeof(value);
  EXPECT_EQ(nodesSetValueNodeValue(pValue, &value), TSDB_CODE_SUCCESS);
  pValue->translate = true;
  return reinterpret_cast<SNode *>(pValue);
}

SNode *makeEqualCond(SNode *pLeft, SNode *pRight) {
  SOperatorNode *pOp = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode **>(&pOp)), TSDB_CODE_SUCCESS);
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = sizeof(bool);
  pOp->opType = OP_TYPE_EQUAL;
  pOp->pLeft = pLeft;
  pOp->pRight = pRight;
  return reinterpret_cast<SNode *>(pOp);
}

}  // namespace

namespace {

class OperatorParamOwnershipTest : public testing::Test {
 protected:
  static OperatorParamOwnershipTest* active;
  Stub stub;
  SExecTaskInfo* task = nullptr;
  SOperatorInfo root = {};
  SOperatorParam* params[2] = {};
  std::array<void*, 4> allocations = {};
  std::array<int, 4> freeCounts = {};

  static void recordFree(void* ptr) {
    for (size_t i = 0; i < active->allocations.size(); ++i) {
      if (ptr != nullptr && ptr == active->allocations[i]) {
        ++active->freeCounts[i];
      }
    }
    active->stub.reset(taosMemFree);
    taosMemFree(ptr);
    active->stub.set(taosMemFree, recordFree);
  }

  void SetUp() override {
    task = static_cast<SExecTaskInfo*>(
        taosMemoryCalloc(1, sizeof(SExecTaskInfo)));
    ASSERT_NE(task, nullptr);
    root.operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
    task->pRoot = &root;
    for (size_t i = 0; i < 2; ++i) {
      params[i] = static_cast<SOperatorParam*>(
          taosMemoryCalloc(1, sizeof(SOperatorParam)));
      allocations[2 * i] = params[i];
      ASSERT_NE(params[i], nullptr);
      params[i]->opType = root.operatorType;
      params[i]->value = taosMemoryCalloc(1, sizeof(STableScanOperatorParam));
      allocations[2 * i + 1] = params[i]->value;
      ASSERT_NE(params[i]->value, nullptr);
    }
    active = this;
    stub.set(taosMemFree, recordFree);
  }

  void destroyTask() {
    task->pRoot = nullptr;
    doDestroyTask(task);
    task = nullptr;
  }

  void expectFrees(int first, int second) {
    EXPECT_EQ(freeCounts[0], first);
    EXPECT_EQ(freeCounts[1], first);
    EXPECT_EQ(freeCounts[2], second);
    EXPECT_EQ(freeCounts[3], second);
  }

  void TearDown() override {
    if (task != nullptr) {
      destroyTask();
    }
    stub.reset(taosMemFree);
    active = nullptr;
    // Release allocations left unowned by a failed assertion or reUse.
    for (size_t i = 0; i < allocations.size(); ++i) {
      if (freeCounts[i] == 0) {
        taosMemoryFree(allocations[i]);
      }
    }
  }
};

OperatorParamOwnershipTest* OperatorParamOwnershipTest::active = nullptr;

}  // namespace

TEST_F(OperatorParamOwnershipTest, ReplacesPendingAndDestroysLatestParam) {
  qUpdateOperatorParam(task, params[0]);
  qUpdateOperatorParam(task, params[1]);

  EXPECT_EQ(task->pOpParam, params[1]);
  EXPECT_FALSE(task->paramSet);
  expectFrees(1, 0);
  destroyTask();
  expectFrees(1, 1);
}

TEST_F(OperatorParamOwnershipTest, LeavesDispatchedParamWithOperator) {
  qUpdateOperatorParam(task, params[0]);
  root.pOperatorGetParam = params[0];
  task->paramSet = true;
  qUpdateOperatorParam(task, params[1]);

  EXPECT_EQ(task->pOpParam, params[1]);
  EXPECT_FALSE(task->paramSet);
  expectFrees(0, 0);
  freeOperatorParam(root.pOperatorGetParam, OP_GET_PARAM);
  root.pOperatorGetParam = nullptr;
  destroyTask();
  expectFrees(1, 1);
}

TEST_F(OperatorParamOwnershipTest, DoesNotFreeDispatchedParamAgain) {
  qUpdateOperatorParam(task, params[0]);
  task->paramSet = true;
  freeOperatorParam(params[0], OP_GET_PARAM);
  qUpdateOperatorParam(task, params[1]);

  expectFrees(1, 0);
  destroyTask();
  expectFrees(1, 1);
}

TEST_F(OperatorParamOwnershipTest, RejectsMismatchWithoutReplacingPending) {
  qUpdateOperatorParam(task, params[0]);
  params[1]->opType = QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN;
  qUpdateOperatorParam(task, params[1]);

  EXPECT_EQ(task->pOpParam, params[0]);
  EXPECT_FALSE(task->paramSet);
  expectFrees(0, 1);
  destroyTask();
  expectFrees(1, 1);
}

TEST_F(OperatorParamOwnershipTest, KeepsRepeatedPendingPointerAlive) {
  qUpdateOperatorParam(task, params[0]);
  qUpdateOperatorParam(task, params[0]);

  EXPECT_EQ(task->pOpParam, params[0]);
  EXPECT_FALSE(task->paramSet);
  expectFrees(0, 0);
  destroyTask();
  expectFrees(1, 0);
}

TEST_F(OperatorParamOwnershipTest, PreservesReusableParamOwnership) {
  params[0]->reUse = true;
  qUpdateOperatorParam(task, params[0]);
  qUpdateOperatorParam(task, params[1]);

  expectFrees(0, 0);
  params[0]->reUse = false;
  freeOperatorParam(params[0], OP_GET_PARAM);
  destroyTask();
  expectFrees(1, 1);
}

static void checkGetNextExtRecordsOutputOnce(__optr_get_ext_fn_t nextExt) {
  SExecTaskInfo task = {};
  task.enableExplain = true;

  SSDataBlock block = {};
  block.info.rows = 3;

  SOperatorInfo op = {};
  op.info = &block;
  op.pTaskInfo = &task;
  op.fpSet = createOperatorFpSet(
      nullptr, execRecordTestNext, nullptr, nullptr, nullptr, nullptr,
      nextExt, nullptr);

  SSDataBlock* pResult = nullptr;
  ASSERT_EQ(op.fpSet.getNextExtFn(&op, nullptr, &pResult),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pResult, &block);
  EXPECT_EQ(op.resultInfo.totalRows, 3);
  EXPECT_EQ(op.cost.execTimes, 1);
}

TEST(execUtilTest, defaultGetNextExtRecordsOutputOnce) {
  checkGetNextExtRecordsOutputOnce(optrDefaultGetNextExtFn);
}

TEST(execUtilTest, customGetNextExtRecordsOutputOnce) {
  checkGetNextExtRecordsOutputOnce(execRecordTestNextExt);
}

TEST(execUtilTest, longJmpClearsNestedOperatorExecDepth) {
  SExecTaskInfo task = {};
  task.id.str = "exec-depth-test";

  SOperatorInfo child = {};
  child.pTaskInfo = &task;
  child.fpSet = createOperatorFpSet(
      nullptr, execRecordTestLongJmpNext, nullptr, nullptr, nullptr,
      nullptr, nullptr, nullptr);

  SOperatorInfo root = {};
  SOperatorInfo* downstream[] = {&child};
  root.pTaskInfo = &task;
  root.numOfDownstream = 1;
  root.pDownstream = downstream;
  root.fpSet = createOperatorFpSet(
      nullptr, execRecordTestPullDownstream, nullptr, nullptr, nullptr,
      nullptr, nullptr, nullptr);
  task.pRoot = &root;

  SSDataBlock* pResult = nullptr;
  uint64_t useconds = 0;
  EXPECT_EQ(qExecTask(&task, &pResult, &useconds), TSDB_CODE_FAILED);
  EXPECT_EQ(root.cost.execDepth, 0);
  EXPECT_EQ(child.cost.execDepth, 0);
}

TEST(execUtilTest, federatedScanInitializesResultBlockCapacity) {
  SFederatedScanPhysiNode scanNode = {};
  SExecTaskInfo           taskInfo = {};
  SOperatorInfo*          pOperator = nullptr;

  ASSERT_EQ(createFederatedScanOperatorInfo(nullptr, &scanNode, &taskInfo,
                                             &pOperator),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(pOperator, nullptr);
  EXPECT_EQ(pOperator->resultInfo.capacity, 4096);
  EXPECT_EQ(pOperator->resultInfo.threshold, 3072);

  destroyOperator(pOperator);
}

class ScanReaderLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gScanReaderLifecycleSpy = &spy_;
    base_.readerAPI.tsdReaderOpen = scanReaderOpenSpy;
    base_.readerAPI.tsdReaderClose = scanReaderCloseSpy;
    base_.readerAPI.tsdSetQueryTableList = scanReaderSetListSpy;
    base_.readerAPI.tsdReaderResetStatus = scanReaderResetStatusSpy;
    base_.readerAPI.tsdReaderSetExecInfo = scanReaderSetExecInfoSpy;
    base_.readerAPI.tsdSetFilesetDelimited = scanReaderSetFilesetDelimitedSpy;
    base_.readerAPI.tsdSetSetNotifyCb = scanReaderSetNotifyCbSpy;
    tables_[0].uid = 7;
    ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&block_));
  }

  void TearDown() override {
    blockDataDestroy(block_);
    gScanReaderLifecycleSpy = nullptr;
  }

  void installFreshReader(STableScanBase *pBase, STsdbReader *pReader) {
    pBase->dataReader = pReader;
    scanTestBindFreshReaderStats(pBase);
  }

  void prepareProductionReset(STableScanBase *pBase, STableScanPhysiNode *pNode, SSubplan *pSubplan,
                              SStreamRuntimeInfo *pRuntime, SExecTaskInfo *pTaskInfo, SOperatorInfo *pOperator,
                              void *pOperatorInfo, bool reuseReader) {
    pBase->cond.type = TIMEWINDOW_RANGE_EXTERNAL;
    pBase->cond.twindows.skey = 1;
    pBase->cond.twindows.ekey = 2;
    pBase->limitInfo.limit.limit = 88;
    pBase->pTableListInfo = tableListCreate();
    ASSERT_NE(nullptr, pBase->pTableListInfo);
    ASSERT_EQ(TSDB_CODE_SUCCESS, tableListAddTableInfo(pBase->pTableListInfo, 7001, 7));

    pNode->scanSeq[0] = 1;
    pNode->scanRange.skey = 100;
    pNode->scanRange.ekey = 200;
    pNode->scan.useTriggerTableList = true;
    pRuntime->funcInfo.isMultiGroupCalc = true;
    appendStreamGroupRead(&pRuntime->funcInfo, 77, {9001});
    appendStreamGroupRead(&pRuntime->funcInfo, 88, {9002});

    pSubplan->pNode = reinterpret_cast<SPhysiNode *>(pNode);
    pTaskInfo->pSubplan = pSubplan;
    pTaskInfo->pStreamRuntimeInfo = pRuntime;
    pTaskInfo->reuseReaderOnReset = reuseReader;
    pTaskInfo->storageAPI.tsdReader = pBase->readerAPI;
    pOperator->info = pOperatorInfo;
    pOperator->pTaskInfo = pTaskInfo;
    pOperator->status = OP_OPENED;
    pOperator->resultInfo.totalRows = 123;
  }

  int32_t initCurrentTableScanGroup(STableScanInfo *pInfo, SExecTaskInfo *pTaskInfo) {
    int32_t        numOfTables = -1;
    STableKeyInfo *pTables = nullptr;
    return scanTestDoInitReader(pInfo, pTaskInfo, &numOfTables, &pTables);
  }

  void cleanupProductionReset(STableScanBase *pBase, SStreamRuntimeInfo *pRuntime) {
    cleanupQueryTableDataCond(&pBase->cond);
    tableListDestroy(pBase->pTableListInfo);
    pBase->pTableListInfo = nullptr;
    tClearStreamCurrentGroupRead(&pRuntime->funcInfo);
  }

  int32_t resetPreservedTableScan(STableScanInfo *pInfo) {
    STableScanPhysiNode node = {};
    node.scan.node.dynamicOp = true;
    node.scanSeq[0] = 1;
    SSubplan subplan = {};
    subplan.pNode = reinterpret_cast<SPhysiNode *>(&node);
    SExecTaskInfo task = {};
    task.pSubplan = &subplan;
    task.reuseReaderOnReset = true;
    SOperatorInfo op = {};
    op.info = pInfo;
    op.pTaskInfo = &task;

    int32_t code = scanTestResetTableScanOperatorState(&op);
    cleanupQueryTableDataCond(&pInfo->base.cond);
    tableListDestroy(pInfo->base.pTableListInfo);
    pInfo->base.pTableListInfo = nullptr;
    return code;
  }

  int32_t resetPreservedTableMerge(STableMergeScanInfo *pInfo) {
    STableScanPhysiNode node = {};
    node.scan.node.dynamicOp = true;
    node.scanSeq[0] = 1;
    SSubplan subplan = {};
    subplan.pNode = reinterpret_cast<SPhysiNode *>(&node);
    SExecTaskInfo task = {};
    task.pSubplan = &subplan;
    task.reuseReaderOnReset = true;
    SOperatorInfo op = {};
    op.info = pInfo;
    op.pTaskInfo = &task;

    int32_t code = scanTestResetTableMergeScanOperatorState(&op);
    cleanupQueryTableDataCond(&pInfo->base.cond);
    tableListDestroy(pInfo->base.pTableListInfo);
    pInfo->base.pTableListInfo = nullptr;
    return code;
  }

  ScanReaderLifecycleSpy spy_ = {};
  STableScanBase         base_ = {};
  STableKeyInfo          tables_[1] = {};
  SSDataBlock           *block_ = nullptr;
  SHashObj              *ignoreTables_ = nullptr;
  bool                   opened_ = false;
};

TEST_F(ScanReaderLifecycleTest, OpensWhenReaderDoesNotExist) {
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            scanTestOpenOrRebindTableReader(&base_, tables_, 1, block_, "scan-reader-test", &ignoreTables_, &opened_));
  EXPECT_EQ(1, spy_.openCalls);
  EXPECT_EQ(0, spy_.setListCalls);
  EXPECT_EQ(0, spy_.resetStatusCalls);
  EXPECT_TRUE(opened_);
  EXPECT_EQ(spy_.openedReader, base_.dataReader);
}

TEST_F(ScanReaderLifecycleTest, RebindsExistingReaderWithoutOpening) {
  base_.dataReader = reinterpret_cast<STsdbReader *>(0x7000);

  EXPECT_EQ(TSDB_CODE_SUCCESS,
            scanTestOpenOrRebindTableReader(&base_, tables_, 1, block_, "scan-reader-test", &ignoreTables_, &opened_));
  EXPECT_EQ(0, spy_.openCalls);
  EXPECT_EQ(1, spy_.setListCalls);
  EXPECT_EQ(1, spy_.resetStatusCalls);
  EXPECT_FALSE(opened_);
  EXPECT_EQ(reinterpret_cast<void *>(0x7000), base_.dataReader);
}

TEST_F(ScanReaderLifecycleTest, SetListFailureLeavesExistingReaderUntouched) {
  STsdbReader *reader = reinterpret_cast<STsdbReader *>(0x7000);
  base_.dataReader = reader;
  spy_.setListCode = TSDB_CODE_OUT_OF_MEMORY;

  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY,
            scanTestOpenOrRebindTableReader(&base_, tables_, 1, block_, "scan-reader-test", &ignoreTables_, &opened_));
  EXPECT_EQ(0, spy_.openCalls);
  EXPECT_EQ(0, spy_.closeCalls);
  EXPECT_EQ(1, spy_.setListCalls);
  EXPECT_EQ(0, spy_.resetStatusCalls);
  EXPECT_FALSE(opened_);
  EXPECT_EQ(reader, base_.dataReader);
}

TEST_F(ScanReaderLifecycleTest, ResetStatusFailureLeavesExistingReaderUntouched) {
  STsdbReader *reader = reinterpret_cast<STsdbReader *>(0x7000);
  base_.dataReader = reader;
  spy_.resetStatusCode = TSDB_CODE_OUT_OF_MEMORY;

  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY,
            scanTestOpenOrRebindTableReader(&base_, tables_, 1, block_, "scan-reader-test", &ignoreTables_, &opened_));
  EXPECT_EQ(0, spy_.openCalls);
  EXPECT_EQ(0, spy_.closeCalls);
  EXPECT_EQ(1, spy_.setListCalls);
  EXPECT_EQ(1, spy_.resetStatusCalls);
  EXPECT_FALSE(opened_);
  EXPECT_EQ(reader, base_.dataReader);
}

TEST_F(ScanReaderLifecycleTest, FreshTableScanResetClosesThenNextInitOpensCurrentGroupOnce) {
  STableScanInfo      tableInfo = {};
  STableScanPhysiNode node = {};
  SSubplan            subplan = {};
  SStreamRuntimeInfo  runtime = {};
  SExecTaskInfo       task = {};
  SOperatorInfo       op = {};
  STsdbReader        *oldReader = reinterpret_cast<STsdbReader *>(0x7000);
  tableInfo.base = base_;
  tableInfo.pResBlock = block_;
  tableInfo.currentGroupId = 42;
  tableInfo.tableEndIndex = 41;
  tableInfo.scanTimes = 9;
  prepareProductionReset(&tableInfo.base, &node, &subplan, &runtime, &task, &op, &tableInfo, false);
  installFreshReader(&tableInfo.base, oldReader);
  spy_.cost.fileLoadBlocks = 5;

  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestResetTableScanOperatorState(&op));
  ASSERT_EQ(-1, tableInfo.currentGroupId);
  EXPECT_EQ(-1, tableInfo.tableEndIndex);
  EXPECT_EQ(0, tableInfo.scanTimes);
  EXPECT_EQ(OP_NOT_OPENED, op.status);
  EXPECT_EQ(0, op.resultInfo.totalRows);
  EXPECT_EQ(-1, tableInfo.base.limitInfo.limit.limit);
  EXPECT_EQ(100, tableInfo.base.cond.twindows.skey);
  EXPECT_EQ(200, tableInfo.base.cond.twindows.ekey);
  tableInfo.currentGroupId = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, initCurrentTableScanGroup(&tableInfo, &task));

  EXPECT_EQ(1, spy_.setExecInfoCalls);
  EXPECT_EQ(1, spy_.closeCalls);
  EXPECT_EQ(1, spy_.openCalls);
  EXPECT_EQ(0, spy_.setListCalls);
  EXPECT_EQ(0, spy_.resetStatusCalls);
  EXPECT_EQ(spy_.openedReader, tableInfo.base.dataReader);
  EXPECT_EQ(&tableInfo.base.cond, spy_.openCond);
  EXPECT_EQ(1, spy_.openTableCount);
  ASSERT_NE(nullptr, spy_.openTables);
  EXPECT_EQ(9001, static_cast<const STableKeyInfo *>(spy_.openTables)->uid);
  EXPECT_EQ(block_, spy_.openBlock);
  EXPECT_EQ(tableInfo.base.dataReader, tableInfo.base.readerCostBaselineReader);
  EXPECT_EQ(5, spy_.lastSetExecInfoFileLoadBlocks);
  EXPECT_LT(spy_.setExecInfoSequence, spy_.closeSequence);
  EXPECT_EQ(0, tableInfo.base.readRecorder.fileLoadBlocks);

  cleanupProductionReset(&tableInfo.base, &runtime);
}

TEST_F(ScanReaderLifecycleTest, ReuseTableScanResetPreservesThenNextInitRebindsCurrentGroupOnce) {
  STableScanInfo      tableInfo = {};
  STableScanPhysiNode node = {};
  SSubplan            subplan = {};
  SStreamRuntimeInfo  runtime = {};
  SExecTaskInfo       task = {};
  SOperatorInfo       op = {};
  STsdbReader        *reader = reinterpret_cast<STsdbReader *>(0x7000);
  tableInfo.base = base_;
  tableInfo.pResBlock = block_;
  tableInfo.currentGroupId = 42;
  tableInfo.tableEndIndex = 41;
  tableInfo.scanTimes = 9;
  prepareProductionReset(&tableInfo.base, &node, &subplan, &runtime, &task, &op, &tableInfo, true);
  installFreshReader(&tableInfo.base, reader);

  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestResetTableScanOperatorState(&op));
  ASSERT_EQ(-1, tableInfo.currentGroupId);
  EXPECT_EQ(-1, tableInfo.tableEndIndex);
  EXPECT_EQ(0, tableInfo.scanTimes);
  EXPECT_EQ(OP_NOT_OPENED, op.status);
  EXPECT_EQ(0, op.resultInfo.totalRows);
  EXPECT_EQ(-1, tableInfo.base.limitInfo.limit.limit);
  EXPECT_EQ(100, tableInfo.base.cond.twindows.skey);
  EXPECT_EQ(200, tableInfo.base.cond.twindows.ekey);
  tableInfo.currentGroupId = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, initCurrentTableScanGroup(&tableInfo, &task));

  EXPECT_EQ(1, spy_.setExecInfoCalls);
  EXPECT_EQ(0, spy_.closeCalls);
  EXPECT_EQ(0, spy_.openCalls);
  EXPECT_EQ(1, spy_.setListCalls);
  EXPECT_EQ(1, spy_.resetStatusCalls);
  EXPECT_EQ(reader, tableInfo.base.dataReader);
  EXPECT_EQ(&tableInfo.base.cond, spy_.resetCond);
  EXPECT_EQ(1, spy_.setListTableCount);
  ASSERT_NE(nullptr, spy_.setListTables);
  EXPECT_EQ(9001, static_cast<const STableKeyInfo *>(spy_.setListTables)->uid);
  EXPECT_EQ(reader, tableInfo.base.readerCostBaselineReader);

  cleanupProductionReset(&tableInfo.base, &runtime);
}

TEST_F(ScanReaderLifecycleTest, FreshSharedTableMergeFlushesAndClosesThenNextInitOpensOnce) {
  STableMergeScanInfo mergeInfo = {};
  STableScanPhysiNode node = {};
  SSubplan            subplan = {};
  SStreamRuntimeInfo  runtime = {};
  SExecTaskInfo       task = {};
  SOperatorInfo       op = {};
  STsdbReader        *oldReader = reinterpret_cast<STsdbReader *>(0x7000);
  mergeInfo.base = base_;
  mergeInfo.pReaderBlock = block_;
  mergeInfo.pSortInputBlock = block_;
  mergeInfo.bufPageSize = 4096;
  mergeInfo.mergeLimit = -1;
  mergeInfo.filesetDelimited = true;
  mergeInfo.tableEndIndex = 41;
  mergeInfo.hasGroupId = true;
  mergeInfo.groupId = 42;
  mergeInfo.bGroupProcessed = true;
  mergeInfo.bNewFilesetEvent = true;
  mergeInfo.limitInfo.limit.limit = 88;
  SBlockOrderInfo order = {};
  order.nullFirst = true;
  order.order = TSDB_ORDER_ASC;
  mergeInfo.pSortInfo = taosArrayInit(1, sizeof(order));
  ASSERT_NE(nullptr, mergeInfo.pSortInfo);
  ASSERT_NE(nullptr, taosArrayPush(mergeInfo.pSortInfo, &order));
  prepareProductionReset(&mergeInfo.base, &node, &subplan, &runtime, &task, &op, &mergeInfo, false);
  installFreshReader(&mergeInfo.base, oldReader);
  spy_.cost.fileLoadBlocks = 5;

  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestResetTableMergeScanOperatorState(&op));
  EXPECT_EQ(0, mergeInfo.tableStartIndex);
  EXPECT_EQ(0, mergeInfo.tableEndIndex);
  EXPECT_FALSE(mergeInfo.hasGroupId);
  EXPECT_FALSE(mergeInfo.bGroupProcessed);
  EXPECT_FALSE(mergeInfo.bNewFilesetEvent);
  EXPECT_EQ(OP_NOT_OPENED, op.status);
  EXPECT_EQ(0, op.resultInfo.totalRows);
  EXPECT_EQ(-1, mergeInfo.limitInfo.limit.limit);
  EXPECT_EQ(100, mergeInfo.base.cond.twindows.skey);
  EXPECT_EQ(200, mergeInfo.base.cond.twindows.ekey);
  STableKeyInfo *first = tableListGetInfo(mergeInfo.base.pTableListInfo, 0);
  ASSERT_NE(nullptr, first);
  mergeInfo.groupId = first->groupId;
  mergeInfo.baseGroupId = first->baseGId;
  scanTestStartGroupTableMergeScan(&op);

  EXPECT_EQ(1, spy_.setExecInfoCalls);
  EXPECT_EQ(1, spy_.closeCalls);
  EXPECT_EQ(1, spy_.openCalls);
  EXPECT_EQ(0, spy_.setListCalls);
  EXPECT_EQ(0, spy_.resetStatusCalls);
  EXPECT_EQ(spy_.openedReader, mergeInfo.base.dataReader);
  EXPECT_EQ(&mergeInfo.base.cond, spy_.openCond);
  EXPECT_EQ(1, spy_.openTableCount);
  ASSERT_NE(nullptr, spy_.openTables);
  EXPECT_EQ(9001, static_cast<const STableKeyInfo *>(spy_.openTables)->uid);
  EXPECT_EQ(mergeInfo.pReaderBlock, spy_.openBlock);
  EXPECT_EQ(&mergeInfo.mSkipTables, spy_.openIgnoreTables);
  EXPECT_EQ(1, spy_.filesetDelimitedCalls);
  EXPECT_EQ(1, spy_.setNotifyCalls);
  EXPECT_NE(nullptr, spy_.notifyFn);
  EXPECT_EQ(&mergeInfo, spy_.notifyParam);
  EXPECT_NE(nullptr, mergeInfo.pSortHandle);
  EXPECT_EQ(5, spy_.lastSetExecInfoFileLoadBlocks);
  EXPECT_LT(spy_.setExecInfoSequence, spy_.closeSequence);
  EXPECT_EQ(0, mergeInfo.base.readRecorder.fileLoadBlocks);

  tsortDestroySortHandle(mergeInfo.pSortHandle);
  taosArrayDestroy(mergeInfo.pSortInfo);
  cleanupProductionReset(&mergeInfo.base, &runtime);
}

TEST_F(ScanReaderLifecycleTest, ReuseSharedTableMergePreservesThenNextInitRebindsSameReaderOnce) {
  STableMergeScanInfo mergeInfo = {};
  STableScanPhysiNode node = {};
  SSubplan            subplan = {};
  SStreamRuntimeInfo  runtime = {};
  SExecTaskInfo       task = {};
  SOperatorInfo       op = {};
  STsdbReader        *reader = reinterpret_cast<STsdbReader *>(0x7000);
  mergeInfo.base = base_;
  mergeInfo.pReaderBlock = block_;
  mergeInfo.pSortInputBlock = block_;
  mergeInfo.bufPageSize = 4096;
  mergeInfo.mergeLimit = -1;
  mergeInfo.filesetDelimited = true;
  mergeInfo.tableEndIndex = 41;
  mergeInfo.hasGroupId = true;
  mergeInfo.groupId = 42;
  mergeInfo.bGroupProcessed = true;
  mergeInfo.bNewFilesetEvent = true;
  mergeInfo.limitInfo.limit.limit = 88;
  SBlockOrderInfo order = {};
  order.nullFirst = true;
  order.order = TSDB_ORDER_ASC;
  mergeInfo.pSortInfo = taosArrayInit(1, sizeof(order));
  ASSERT_NE(nullptr, mergeInfo.pSortInfo);
  ASSERT_NE(nullptr, taosArrayPush(mergeInfo.pSortInfo, &order));
  prepareProductionReset(&mergeInfo.base, &node, &subplan, &runtime, &task, &op, &mergeInfo, true);
  installFreshReader(&mergeInfo.base, reader);

  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestResetTableMergeScanOperatorState(&op));
  EXPECT_EQ(0, mergeInfo.tableStartIndex);
  EXPECT_EQ(0, mergeInfo.tableEndIndex);
  EXPECT_FALSE(mergeInfo.hasGroupId);
  EXPECT_FALSE(mergeInfo.bGroupProcessed);
  EXPECT_FALSE(mergeInfo.bNewFilesetEvent);
  EXPECT_EQ(OP_NOT_OPENED, op.status);
  EXPECT_EQ(0, op.resultInfo.totalRows);
  EXPECT_EQ(-1, mergeInfo.limitInfo.limit.limit);
  EXPECT_EQ(100, mergeInfo.base.cond.twindows.skey);
  EXPECT_EQ(200, mergeInfo.base.cond.twindows.ekey);
  STableKeyInfo *first = tableListGetInfo(mergeInfo.base.pTableListInfo, 0);
  ASSERT_NE(nullptr, first);
  mergeInfo.groupId = first->groupId;
  mergeInfo.baseGroupId = first->baseGId;
  scanTestStartGroupTableMergeScan(&op);

  EXPECT_EQ(1, spy_.setExecInfoCalls);
  EXPECT_EQ(0, spy_.closeCalls);
  EXPECT_EQ(0, spy_.openCalls);
  EXPECT_EQ(1, spy_.setListCalls);
  EXPECT_EQ(1, spy_.resetStatusCalls);
  EXPECT_EQ(reader, mergeInfo.base.dataReader);
  EXPECT_EQ(&mergeInfo.base.cond, spy_.resetCond);
  EXPECT_EQ(1, spy_.setListTableCount);
  ASSERT_NE(nullptr, spy_.setListTables);
  EXPECT_EQ(9001, static_cast<const STableKeyInfo *>(spy_.setListTables)->uid);
  EXPECT_EQ(0, spy_.filesetDelimitedCalls);
  EXPECT_EQ(0, spy_.setNotifyCalls);
  EXPECT_NE(nullptr, mergeInfo.pSortHandle);
  EXPECT_EQ(reader, mergeInfo.base.readerCostBaselineReader);

  tsortDestroySortHandle(mergeInfo.pSortHandle);
  taosArrayDestroy(mergeInfo.pSortInfo);
  cleanupProductionReset(&mergeInfo.base, &runtime);
}

TEST_F(ScanReaderLifecycleTest, TableScanResetPreservesOnlyReuseReader) {
  STableScanInfo tableInfo = {};
  STsdbReader   *reader = reinterpret_cast<STsdbReader *>(0x7000);
  tableInfo.base = base_;
  installFreshReader(&tableInfo.base, reader);
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&tableInfo.base));

  scanTestResetTableScanReader(&tableInfo, true);
  EXPECT_EQ(0, spy_.closeCalls);
  EXPECT_EQ(reader, tableInfo.base.dataReader);

  scanTestResetTableScanReader(&tableInfo, false);
  EXPECT_EQ(1, spy_.closeCalls);
  EXPECT_EQ(nullptr, tableInfo.base.dataReader);
}

TEST_F(ScanReaderLifecycleTest, TableMergeResetPreservesOnlyReuseReader) {
  STableMergeScanInfo mergeInfo = {};
  STsdbReader        *reader = reinterpret_cast<STsdbReader *>(0x7000);
  mergeInfo.base = base_;
  installFreshReader(&mergeInfo.base, reader);
  spy_.cost.fileLoadBlocks = 10;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&mergeInfo.base));
  EXPECT_EQ(reader, mergeInfo.base.readerCostBaselineReader);

  scanTestResetTableMergeReader(&mergeInfo, true);
  EXPECT_EQ(0, spy_.closeCalls);
  EXPECT_EQ(reader, mergeInfo.base.dataReader);

  spy_.cost.fileLoadBlocks = 15;
  scanTestResetTableMergeReader(&mergeInfo, false);
  EXPECT_EQ(2, spy_.setExecInfoCalls);
  EXPECT_EQ(1, spy_.closeCalls);
  EXPECT_EQ(nullptr, mergeInfo.base.dataReader);
  EXPECT_EQ(5, mergeInfo.base.readRecorder.fileLoadBlocks);
  EXPECT_EQ(nullptr, mergeInfo.base.readerCostBaselineReader);
}

TEST_F(ScanReaderLifecycleTest, ReuseEpochReportsOnlyCumulativeReaderDelta) {
  installFreshReader(&base_, reinterpret_cast<STsdbReader *>(0x7000));
  spy_.cost.fileLoadBlocks = 10;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&base_));

  spy_.cost.fileLoadBlocks = 15;
  STableScanAnalyzeInfo firstEpoch = {};
  scanTestSnapshotReaderStats(&base_, &firstEpoch);
  EXPECT_EQ(5, firstEpoch.fileLoadBlocks);

  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&base_));
  spy_.cost.fileLoadBlocks = 18;
  STableScanAnalyzeInfo secondEpoch = {};
  scanTestSnapshotReaderStats(&base_, &secondEpoch);
  EXPECT_EQ(3, secondEpoch.fileLoadBlocks);
}

TEST_F(ScanReaderLifecycleTest, RepeatedStatsSnapshotIsIdempotent) {
  installFreshReader(&base_, reinterpret_cast<STsdbReader *>(0x7000));
  spy_.cost.fileLoadBlocks = 10;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&base_));
  base_.readRecorder.totalBlocks = 4;
  base_.readRecorder.checkRows = 23;
  spy_.cost.fileLoadBlocks = 15;

  const SFileBlockLoadRecorder recorderBefore = base_.readRecorder;
  const SFileBlockLoadRecorder baselineBefore = base_.readerCostBaseline;
  STableScanAnalyzeInfo        first = {};
  STableScanAnalyzeInfo        second = {};
  scanTestSnapshotReaderStats(&base_, &first);
  scanTestSnapshotReaderStats(&base_, &second);

  EXPECT_EQ(0, std::memcmp(&first, &second, sizeof(first)));
  EXPECT_EQ(5, first.fileLoadBlocks);
  EXPECT_EQ(4, first.totalBlocks);
  EXPECT_EQ(23, first.checkRows);
  EXPECT_EQ(0, std::memcmp(&recorderBefore, &base_.readRecorder, sizeof(recorderBefore)));
  EXPECT_EQ(0, std::memcmp(&baselineBefore, &base_.readerCostBaseline, sizeof(baselineBefore)));
}

TEST_F(ScanReaderLifecycleTest, InternalGroupRebindDoesNotStartNewStatsEpoch) {
  installFreshReader(&base_, reinterpret_cast<STsdbReader *>(0x7000));
  spy_.cost.fileLoadBlocks = 10;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&base_));
  const SFileBlockLoadRecorder baseline = base_.readerCostBaseline;

  spy_.cost.fileLoadBlocks = 15;
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            scanTestOpenOrRebindTableReader(&base_, tables_, 1, block_, "scan-reader-test", &ignoreTables_, &opened_));
  EXPECT_EQ(0, std::memcmp(&baseline, &base_.readerCostBaseline, sizeof(baseline)));

  spy_.cost.fileLoadBlocks = 18;
  STableScanAnalyzeInfo epoch = {};
  scanTestSnapshotReaderStats(&base_, &epoch);
  EXPECT_EQ(8, epoch.fileLoadBlocks);
}

TEST_F(ScanReaderLifecycleTest, FailedResetDoesNotAdvanceStatsBaseline) {
  installFreshReader(&base_, reinterpret_cast<STsdbReader *>(0x7000));
  spy_.cost.fileLoadBlocks = 10;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&base_));
  base_.readRecorder.totalBlocks = 6;
  base_.readRecorder.checkRows = 29;
  const SFileBlockLoadRecorder recorder = base_.readRecorder;
  const SFileBlockLoadRecorder baseline = base_.readerCostBaseline;
  STsdbReader *const           baselineReader = base_.readerCostBaselineReader;

  spy_.setListCode = TSDB_CODE_OUT_OF_MEMORY;
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY,
            scanTestOpenOrRebindTableReader(&base_, tables_, 1, block_, "scan-reader-test", &ignoreTables_, &opened_));
  EXPECT_EQ(0, std::memcmp(&recorder, &base_.readRecorder, sizeof(recorder)));
  EXPECT_EQ(0, std::memcmp(&baseline, &base_.readerCostBaseline, sizeof(baseline)));
  EXPECT_EQ(baselineReader, base_.readerCostBaselineReader);

  spy_.setListCode = TSDB_CODE_SUCCESS;
  spy_.resetStatusCode = TSDB_CODE_OUT_OF_MEMORY;
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY,
            scanTestOpenOrRebindTableReader(&base_, tables_, 1, block_, "scan-reader-test", &ignoreTables_, &opened_));
  EXPECT_EQ(0, std::memcmp(&recorder, &base_.readRecorder, sizeof(recorder)));
  EXPECT_EQ(0, std::memcmp(&baseline, &base_.readerCostBaseline, sizeof(baseline)));
  EXPECT_EQ(baselineReader, base_.readerCostBaselineReader);
}

TEST_F(ScanReaderLifecycleTest, FreshReaderStartsWithZeroBaseline) {
  installFreshReader(&base_, reinterpret_cast<STsdbReader *>(0x7000));
  spy_.cost.fileLoadBlocks = 10;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&base_));
  spy_.cost.fileLoadBlocks = 15;
  scanTestFlushReaderStats(&base_);
  EXPECT_EQ(5, base_.readRecorder.fileLoadBlocks);
  EXPECT_EQ(0, base_.readerCostBaseline.fileLoadBlocks);

  base_.dataReader = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&base_));
  const SFileBlockLoadRecorder zero = {};
  EXPECT_EQ(0, std::memcmp(&zero, &base_.readRecorder, sizeof(base_.readRecorder)));
  EXPECT_EQ(0, std::memcmp(&zero, &base_.readerCostBaseline, sizeof(base_.readerCostBaseline)));
}

TEST_F(ScanReaderLifecycleTest, ReaderReplacementFlushesOldDeltaAndBindsFreshIdentity) {
  STableScanInfo tableInfo = {};
  STsdbReader   *readerA = reinterpret_cast<STsdbReader *>(0x7000);
  STsdbReader   *readerB = reinterpret_cast<STsdbReader *>(0x7200);
  tableInfo.base = base_;
  installFreshReader(&tableInfo.base, readerA);
  spy_.cost.fileLoadBlocks = 10;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&tableInfo.base));
  EXPECT_EQ(readerA, tableInfo.base.readerCostBaselineReader);

  spy_.cost.fileLoadBlocks = 15;
  scanTestResetTableScanReader(&tableInfo, false);
  EXPECT_EQ(5, tableInfo.base.readRecorder.fileLoadBlocks);
  EXPECT_EQ(nullptr, tableInfo.base.readerCostBaselineReader);

  spy_.openedReader = readerB;
  spy_.cost.fileLoadBlocks = 0;
  EXPECT_EQ(TSDB_CODE_SUCCESS, scanTestOpenOrRebindTableReader(&tableInfo.base, tables_, 1, block_, "scan-reader-test",
                                                               &ignoreTables_, &opened_));
  EXPECT_EQ(readerB, tableInfo.base.dataReader);
  EXPECT_EQ(readerB, tableInfo.base.readerCostBaselineReader);
  EXPECT_EQ(0, tableInfo.base.readerCostBaseline.fileLoadBlocks);

  spy_.cost.fileLoadBlocks = 3;
  STableScanAnalyzeInfo epoch = {};
  EXPECT_EQ(TSDB_CODE_SUCCESS, scanTestSnapshotReaderStatsCode(&tableInfo.base, &epoch));
  EXPECT_EQ(8, epoch.fileLoadBlocks);
}

TEST_F(ScanReaderLifecycleTest, DestroyFlushesActiveReaderBeforeInvalidatingIdentity) {
  STsdbReader *reader = reinterpret_cast<STsdbReader *>(0x7000);
  installFreshReader(&base_, reader);
  spy_.cost.fileLoadBlocks = 10;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&base_));
  spy_.cost.fileLoadBlocks = 15;

  scanTestDestroyTableScanBase(&base_);

  EXPECT_EQ(1, spy_.closeCalls);
  EXPECT_EQ(nullptr, base_.dataReader);
  EXPECT_EQ(5, base_.readRecorder.fileLoadBlocks);
  EXPECT_EQ(nullptr, base_.readerCostBaselineReader);
}

TEST_F(ScanReaderLifecycleTest, ReaderCostRegressionFailsClosedBeforeSnapshotOrClose) {
  STableScanInfo tableInfo = {};
  STsdbReader   *reader = reinterpret_cast<STsdbReader *>(0x7000);
  tableInfo.base = base_;
  installFreshReader(&tableInfo.base, reader);
  spy_.cost.fileLoadBlocks = 10;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&tableInfo.base));
  const SFileBlockLoadRecorder recorder = tableInfo.base.readRecorder;
  const SFileBlockLoadRecorder baseline = tableInfo.base.readerCostBaseline;
  spy_.cost.fileLoadBlocks = 9;

  STableScanAnalyzeInfo snapshot = {};
  EXPECT_EQ(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR, scanTestSnapshotReaderStatsCode(&tableInfo.base, &snapshot));
  EXPECT_EQ(0, std::memcmp(&recorder, &tableInfo.base.readRecorder, sizeof(recorder)));
  EXPECT_EQ(0, std::memcmp(&baseline, &tableInfo.base.readerCostBaseline, sizeof(baseline)));

  EXPECT_EQ(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR, scanTestCloseTableScanReader(&tableInfo));
  EXPECT_EQ(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR,
            scanTestFlushAndCloseTmsReader(&tableInfo.base, &tableInfo.base.dataReader, true));
  EXPECT_EQ(0, spy_.closeCalls);
  EXPECT_EQ(reader, tableInfo.base.dataReader);
  EXPECT_EQ(reader, tableInfo.base.readerCostBaselineReader);
  EXPECT_EQ(0, std::memcmp(&recorder, &tableInfo.base.readRecorder, sizeof(recorder)));
  EXPECT_EQ(0, std::memcmp(&baseline, &tableInfo.base.readerCostBaseline, sizeof(baseline)));

  scanTestDestroyTableScanBase(&tableInfo.base);
  EXPECT_EQ(1, spy_.closeCalls);
  EXPECT_EQ(nullptr, tableInfo.base.dataReader);
  EXPECT_EQ(nullptr, tableInfo.base.readerCostBaselineReader);
  EXPECT_EQ(0, std::memcmp(&recorder, &tableInfo.base.readRecorder, sizeof(recorder)));
}

TEST_F(ScanReaderLifecycleTest, PerSubtableMergeCloseKeepsDirectReaderAccumulation) {
  STsdbReader *sharedReader = reinterpret_cast<STsdbReader *>(0x7000);
  STsdbReader *subtableReader = reinterpret_cast<STsdbReader *>(0x7300);
  installFreshReader(&base_, sharedReader);
  spy_.cost.fileLoadBlocks = 10;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&base_));
  const SFileBlockLoadRecorder baseline = base_.readerCostBaseline;

  spy_.cost.fileLoadBlocks = 7;
  EXPECT_EQ(TSDB_CODE_SUCCESS, scanTestFlushAndCloseTmsReader(&base_, &subtableReader, false));

  EXPECT_EQ(nullptr, subtableReader);
  EXPECT_EQ(1, spy_.closeCalls);
  EXPECT_EQ(7, base_.readRecorder.fileLoadBlocks);
  EXPECT_EQ(sharedReader, base_.dataReader);
  EXPECT_EQ(sharedReader, base_.readerCostBaselineReader);
  EXPECT_EQ(0, std::memcmp(&baseline, &base_.readerCostBaseline, sizeof(baseline)));
}

TEST_F(ScanReaderLifecycleTest, PreservedTableScanResetRejectsRegressionAndIdentityMismatchAtomically) {
  STsdbReader *readerA = reinterpret_cast<STsdbReader *>(0x7000);
  STsdbReader *readerB = reinterpret_cast<STsdbReader *>(0x7200);

  STableScanInfo regression = {};
  regression.base = base_;
  installFreshReader(&regression.base, readerA);
  spy_.cost.fileLoadBlocks = 10;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&regression.base));
  regression.base.readRecorder.totalBlocks = 6;
  const SFileBlockLoadRecorder regressionRecorder = regression.base.readRecorder;
  const SFileBlockLoadRecorder regressionBaseline = regression.base.readerCostBaseline;
  STsdbReader *const           regressionIdentity = regression.base.readerCostBaselineReader;
  spy_.cost.fileLoadBlocks = 9;

  EXPECT_EQ(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR, resetPreservedTableScan(&regression));
  EXPECT_EQ(readerA, regression.base.dataReader);
  EXPECT_EQ(0, std::memcmp(&regressionRecorder, &regression.base.readRecorder, sizeof(regressionRecorder)));
  EXPECT_EQ(0, std::memcmp(&regressionBaseline, &regression.base.readerCostBaseline, sizeof(regressionBaseline)));
  EXPECT_EQ(regressionIdentity, regression.base.readerCostBaselineReader);

  STableScanInfo mismatch = {};
  mismatch.base = base_;
  mismatch.base.dataReader = readerB;
  mismatch.base.readRecorder = regressionRecorder;
  mismatch.base.readerCostBaseline = regressionBaseline;
  mismatch.base.readerCostBaselineReader = readerA;
  const SFileBlockLoadRecorder mismatchRecorder = mismatch.base.readRecorder;
  const SFileBlockLoadRecorder mismatchBaseline = mismatch.base.readerCostBaseline;

  EXPECT_EQ(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR, resetPreservedTableScan(&mismatch));
  EXPECT_EQ(readerB, mismatch.base.dataReader);
  EXPECT_EQ(0, std::memcmp(&mismatchRecorder, &mismatch.base.readRecorder, sizeof(mismatchRecorder)));
  EXPECT_EQ(0, std::memcmp(&mismatchBaseline, &mismatch.base.readerCostBaseline, sizeof(mismatchBaseline)));
  EXPECT_EQ(readerA, mismatch.base.readerCostBaselineReader);
}

TEST_F(ScanReaderLifecycleTest, PreservedSharedTableMergeResetRejectsRegressionAtomically) {
  STableMergeScanInfo mergeInfo = {};
  STsdbReader        *reader = reinterpret_cast<STsdbReader *>(0x7000);
  mergeInfo.base = base_;
  installFreshReader(&mergeInfo.base, reader);
  spy_.cost.fileLoadBlocks = 10;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestBeginReaderStatsEpoch(&mergeInfo.base));
  mergeInfo.base.readRecorder.totalBlocks = 8;
  const SFileBlockLoadRecorder recorder = mergeInfo.base.readRecorder;
  const SFileBlockLoadRecorder baseline = mergeInfo.base.readerCostBaseline;
  STsdbReader *const           identity = mergeInfo.base.readerCostBaselineReader;
  spy_.cost.fileLoadBlocks = 9;

  EXPECT_EQ(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR, resetPreservedTableMerge(&mergeInfo));
  EXPECT_EQ(reader, mergeInfo.base.dataReader);
  EXPECT_EQ(0, std::memcmp(&recorder, &mergeInfo.base.readRecorder, sizeof(recorder)));
  EXPECT_EQ(0, std::memcmp(&baseline, &mergeInfo.base.readerCostBaseline, sizeof(baseline)));
  EXPECT_EQ(identity, mergeInfo.base.readerCostBaselineReader);
}

class TableScanResetModeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    scanNode_.scan.node.type = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
    scanInfo_.base.cond.type = TIMEWINDOW_RANGE_CONTAINED;
    root_.info = &scanInfo_;
    root_.pPhyNode = reinterpret_cast<SPhysiNode *>(&scanNode_);
    root_.pTaskInfo = &task_;
    root_.fpSet.resetStateFn = observeTableScanReset;
    task_.pRoot = &root_;
    gTableScanResetSpy = &spy_;
  }

  void TearDown() override { gTableScanResetSpy = nullptr; }

  void useTableMergeRoot() {
    mergeNode_.scan.node.type = QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN;
    mergeInfo_.base.cond.type = TIMEWINDOW_RANGE_CONTAINED;
    root_.info = &mergeInfo_;
    root_.pPhyNode = reinterpret_cast<SPhysiNode *>(&mergeNode_);
  }

  int32_t reset(EQueryTableScanResetMode mode) { return qResetTableScan(&task_, &handle_, mode); }

  TableScanResetSpy        spy_ = {};
  SExecTaskInfo            task_ = {};
  SOperatorInfo            root_ = {};
  STableScanInfo           scanInfo_ = {};
  STableMergeScanInfo      mergeInfo_ = {};
  STableScanPhysiNode      scanNode_ = {};
  STableMergeScanPhysiNode mergeNode_ = {};
  SReadHandle              handle_ = {};
};

TEST_F(TableScanResetModeTest, ReuseContainedTableScanScopesModeToCallback) {
  EXPECT_EQ(TSDB_CODE_SUCCESS, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(1, spy_.calls);
  EXPECT_TRUE(spy_.observedReuse);
  EXPECT_FALSE(task_.reuseReaderOnReset);
}

TEST_F(TableScanResetModeTest, ReuseTableScanFailureClearsCallbackMode) {
  spy_.code = TSDB_CODE_OUT_OF_MEMORY;

  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(1, spy_.calls);
  EXPECT_TRUE(spy_.observedReuse);
  EXPECT_FALSE(task_.reuseReaderOnReset);
}

TEST_F(TableScanResetModeTest, ReuseRejectsExternalTableScanBeforeCallback) {
  scanInfo_.base.cond.type = TIMEWINDOW_RANGE_EXTERNAL;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(0, spy_.calls);
}

TEST_F(TableScanResetModeTest, ReuseRejectsUnknownTableScanRangeBeforeCallback) {
  scanInfo_.base.cond.type = 0;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(0, spy_.calls);
}

TEST_F(TableScanResetModeTest, ReuseRejectsDynamicTableScanBeforeCallback) {
  scanNode_.scan.node.dynamicOp = true;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(0, spy_.calls);
}

TEST_F(TableScanResetModeTest, ReuseRejectsVirtualTableScanBeforeCallback) {
  scanNode_.scan.virtualStableScan = true;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(0, spy_.calls);
}

TEST_F(TableScanResetModeTest, ReuseAcceptsSharedReaderTableMerge) {
  useTableMergeRoot();

  EXPECT_EQ(TSDB_CODE_SUCCESS, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(1, spy_.calls);
  EXPECT_TRUE(spy_.observedReuse);
  EXPECT_FALSE(task_.reuseReaderOnReset);
}

TEST_F(TableScanResetModeTest, ReuseRejectsExternalTableMergeBeforeCallback) {
  useTableMergeRoot();
  mergeInfo_.base.cond.type = TIMEWINDOW_RANGE_EXTERNAL;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(0, spy_.calls);
}

TEST_F(TableScanResetModeTest, ReuseRejectsUnknownTableMergeRangeBeforeCallback) {
  useTableMergeRoot();
  mergeInfo_.base.cond.type = 0;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(0, spy_.calls);
}

TEST_F(TableScanResetModeTest, ReuseRejectsDynamicTableMergeBeforeCallback) {
  useTableMergeRoot();
  mergeNode_.scan.node.dynamicOp = true;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(0, spy_.calls);
}

TEST_F(TableScanResetModeTest, ReuseRejectsVirtualTableMergeBeforeCallback) {
  useTableMergeRoot();
  mergeNode_.scan.virtualStableScan = true;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(0, spy_.calls);
}

TEST_F(TableScanResetModeTest, ReuseRejectsParallelTableMergeBeforeCallback) {
  useTableMergeRoot();
  mergeNode_.paraTablesSort = true;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(0, spy_.calls);
}

TEST_F(TableScanResetModeTest, ReuseRejectsPerSubtableTableMergeBeforeCallback) {
  useTableMergeRoot();
  mergeInfo_.pSubTablesMergeInfo = reinterpret_cast<STmsSubTablesMergeInfo *>(static_cast<uintptr_t>(1));

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(0, spy_.calls);
}

TEST_F(TableScanResetModeTest, ReuseRejectsOtherRootTypeBeforeCallback) {
  scanNode_.scan.node.type = QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, reset(QUERY_TABLE_SCAN_RESET_REUSE_READER));
  EXPECT_EQ(0, spy_.calls);
}

TEST_F(TableScanResetModeTest, FreshResetPreservesExistingCallbackBehavior) {
  scanInfo_.base.cond.type = TIMEWINDOW_RANGE_EXTERNAL;
  scanNode_.scan.node.dynamicOp = true;
  scanNode_.scan.virtualStableScan = true;

  EXPECT_EQ(TSDB_CODE_SUCCESS, reset(QUERY_TABLE_SCAN_RESET_FRESH));
  EXPECT_EQ(1, spy_.calls);
  EXPECT_FALSE(spy_.observedReuse);
  EXPECT_FALSE(task_.reuseReaderOnReset);
}

TEST_F(TableScanResetModeTest, InvalidModeFailsBeforeMutationOrCallback) {
  handle_.uid = 42;
  scanInfo_.base.readHandle.uid = 73;

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, reset(static_cast<EQueryTableScanResetMode>(2)));
  EXPECT_EQ(0, spy_.calls);
  EXPECT_FALSE(task_.reuseReaderOnReset);
  EXPECT_EQ(73, scanInfo_.base.readHandle.uid);
}

TEST(MultiGroupJoinQualificationTest, LocalJoinQualifiesItsAncestorButNotAnUnrelatedSibling) {
  SProjectPhysiNode project = {};
  project.node.type = QUERY_NODE_PHYSICAL_PLAN_PROJECT;
  SHashJoinPhysiNode join = {};
  join.node.type = QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN;
  join.node.pParent = &project.node;
  join.multiGroupJoin = true;
  SExchangePhysiNode sibling = {};
  sibling.node.type = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  sibling.node.pParent = &project.node;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&project.node.pChildren, reinterpret_cast<SNode *>(&join)));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&project.node.pChildren, reinterpret_cast<SNode *>(&sibling)));

  SSubplan subplan = {};
  subplan.pNode = &project.node;
  SExecTaskInfo task = {};
  task.pSubplan = &subplan;
  SOperatorInfo projectOperator = {};
  projectOperator.pTaskInfo = &task;
  projectOperator.pPhyNode = &project.node;
  SOperatorInfo siblingOperator = {};
  siblingOperator.pTaskInfo = &task;
  siblingOperator.pPhyNode = &sibling.node;

  EXPECT_TRUE(isStaticMultiGroupJoinQualified(&projectOperator));
  EXPECT_FALSE(isStaticMultiGroupJoinQualified(&siblingOperator));

  subplan.inheritedMultiGroupJoin = true;
  EXPECT_TRUE(isStaticMultiGroupJoinQualified(&projectOperator));
  EXPECT_TRUE(isStaticMultiGroupJoinQualified(&siblingOperator));

  subplan.inheritedMultiGroupJoin = false;
  join.multiGroupJoin = false;
  EXPECT_FALSE(isStaticMultiGroupJoinQualified(&projectOperator));
  EXPECT_FALSE(isStaticMultiGroupJoinQualified(&siblingOperator));

  nodesClearList(project.node.pChildren);
}

TEST(execUtilTest, resRowTest) {
  SDiskbasedBuf *pBuf = nullptr;
  int32_t        pageSize = 32;
  int32_t        numPages = 3;
  int32_t        code = createDiskbasedBuf(&pBuf, pageSize, pageSize * numPages, "test_buf", "/");
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);

  std::vector<void *>  pages(numPages);
  std::vector<int32_t> pageIds(numPages);
  for (int32_t i = 0; i < numPages; ++i) {
    pages[i] = getNewBufPage(pBuf, &pageIds[i]);
    EXPECT_NE(pages[i], nullptr);
    EXPECT_EQ(pageIds[i], i);
  }

  EXPECT_EQ(getNewBufPage(pBuf, nullptr), nullptr);

  SResultRowPosition pos;
  pos.offset = 0;
  for (int32_t i = 0; i < numPages; ++i) {
    pos.pageId = pageIds[i];
    bool        forUpdate = i & 0x1;
    SResultRow *row = getResultRowByPos(pBuf, &pos, forUpdate);
    EXPECT_EQ((void *)row, pages[i]);
  }

  pos.pageId = numPages + 1;
  EXPECT_EQ(getResultRowByPos(pBuf, &pos, true), nullptr);

  destroyDiskbasedBuf(pBuf);
}

TEST(StreamGroupTableListTest, FromCondKeepsSameUidInEachGid) {
  SStreamRuntimeFuncInfo runtime = {};
  appendStreamGroupRead(&runtime, 101, {});
  appendStreamGroupRead(&runtime, 101, {});
  appendStreamGroupRead(&runtime, 202, {});
  appendStreamGroupCalc(&runtime, 101);
  appendStreamGroupCalc(&runtime, 202);

  SScanPhysiNode scan = {};
  scan.uid = 9001;
  scan.tableType = TSDB_CHILD_TABLE;
  SReadHandle   handle = {};
  SExecTaskInfo task = {};
  task.storageAPI.metaFn.isTableExisted = countExistingStreamGroupTable;
  STableListInfo *tableList = tableListCreate();
  ASSERT_NE(tableList, nullptr);

  gStreamGroupTableListExistsCalls = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            createStreamGrpTableListFromCond(&scan, &handle, &task, &runtime, tableList, nullptr, nullptr, 3, true));
  EXPECT_EQ(2, gStreamGroupTableListExistsCalls);
  ASSERT_EQ(2, taosArrayGetSize(tableList->pTableList));
  const auto *first = static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, 0));
  const auto *second = static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, 1));
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  EXPECT_EQ(9001, first->uid);
  EXPECT_EQ(101, first->baseGId);
  EXPECT_EQ(9001, second->uid);
  EXPECT_EQ(202, second->baseGId);

  ASSERT_EQ(TSDB_CODE_SUCCESS, execUtilTestSortStreamTableGroups(tableList));
  ASSERT_EQ(2, tableListGetOutputGroups(tableList));
  for (int32_t group = 0; group < 2; ++group) {
    STableKeyInfo *groupTables = nullptr;
    int32_t        tableCount = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tableListGetGroupList(tableList, group, &groupTables, &tableCount));
    ASSERT_EQ(1, tableCount);
    ASSERT_NE(nullptr, groupTables);
    EXPECT_EQ(9001, groupTables->uid);
    EXPECT_EQ(group == 0 ? 101 : 202, groupTables->baseGId);

    STableScanInfo scanInfo = {};
    SSDataBlock    block = {};
    scanInfo.base.pTableListInfo = tableList;
    scanInfo.tableStartIndex = tableList->groupOffset[group];
    block.info.id.uid = 9001;
    ASSERT_EQ(TSDB_CODE_SUCCESS, scanTestSetTableScanBlockGroupId(&scanInfo, &block));
    EXPECT_EQ(group == 0 ? 101 : 202, block.info.id.baseGId);
  }

  tableListDestroy(tableList);
  tClearStreamCurrentGroupRead(&runtime);
  tSimpleHashCleanup(runtime.pGroupCalcInfos);
}

TEST(StreamGroupTableListTest, FromCondBuildsDisjointMembershipOncePerGid) {
  SStreamRuntimeFuncInfo runtime = {};
  appendStreamGroupRead(&runtime, 101, {});
  appendStreamGroupRead(&runtime, 101, {});
  appendStreamGroupRead(&runtime, 202, {});
  appendStreamGroupCalc(&runtime, 101);
  appendStreamGroupCalc(&runtime, 202);

  SScanPhysiNode scan = {};
  scan.uid = 9000;
  scan.tableType = TSDB_CHILD_TABLE;
  SReadHandle   handle = {};
  SExecTaskInfo task = {};
  task.storageAPI.metaFn.isTableExisted = appendDisjointStreamGroupTable;
  STableListInfo *tableList = tableListCreate();
  ASSERT_NE(tableList, nullptr);

  gStreamGroupTableListExistsCalls = 0;
  gStreamGroupTableListScan = &scan;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            createStreamGrpTableListFromCond(&scan, &handle, &task, &runtime, tableList, nullptr, nullptr, 3, true));
  gStreamGroupTableListScan = nullptr;
  ASSERT_EQ(2, gStreamGroupTableListExistsCalls);
  ASSERT_EQ(2, taosArrayGetSize(tableList->pTableList));
  const auto *first = static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, 0));
  const auto *second = static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, 1));
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  EXPECT_EQ(9001, first->uid);
  EXPECT_EQ(101, first->baseGId);
  EXPECT_EQ(9002, second->uid);
  EXPECT_EQ(202, second->baseGId);

  tableListDestroy(tableList);
  tClearStreamCurrentGroupRead(&runtime);
  tSimpleHashCleanup(runtime.pGroupCalcInfos);
}

TEST(StreamGroupTableListTest, FixedScanIgnoresNonemptyTriggerUidList) {
  SStreamRuntimeInfo runtime = {};
  runtime.funcInfo.isMultiGroupCalc = true;
  appendStreamGroupRead(&runtime.funcInfo, 101, {7001});
  appendStreamGroupRead(&runtime.funcInfo, 202, {7002});
  appendStreamGroupCalc(&runtime.funcInfo, 101);
  appendStreamGroupCalc(&runtime.funcInfo, 202);

  SScanPhysiNode scan = {};
  scan.uid = 9000;
  scan.tableType = TSDB_CHILD_TABLE;
  SReadHandle   handle = {};
  SExecTaskInfo task = {};
  task.pStreamRuntimeInfo = &runtime;
  task.storageAPI.metaFn.isTableExisted = appendDisjointStreamGroupTable;
  STableListInfo *tableList = tableListCreate();
  ASSERT_NE(nullptr, tableList);

  gStreamGroupTableListExistsCalls = 0;
  gStreamGroupTableListScan = &scan;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createStreamMultiGrpTableListInfo(&scan, nullptr, false, &handle, tableList, nullptr,
                                                                 nullptr, &task, nullptr));
  gStreamGroupTableListScan = nullptr;
  EXPECT_EQ(2, gStreamGroupTableListExistsCalls);
  ASSERT_EQ(2, taosArrayGetSize(tableList->pTableList));
  EXPECT_EQ(9001, static_cast<STableKeyInfo *>(taosArrayGet(tableList->pTableList, 0))->uid);
  EXPECT_EQ(9002, static_cast<STableKeyInfo *>(taosArrayGet(tableList->pTableList, 1))->uid);

  tableListDestroy(tableList);
  tClearStreamCurrentGroupRead(&runtime.funcInfo);
  tSimpleHashCleanup(runtime.funcInfo.pGroupCalcInfos);
}

TEST(StreamGroupTableListTest, PlaceholderScanConsumesNonemptyTriggerUidList) {
  SStreamRuntimeInfo runtime = {};
  runtime.funcInfo.isMultiGroupCalc = true;
  appendStreamGroupRead(&runtime.funcInfo, 101, {7001});
  appendStreamGroupRead(&runtime.funcInfo, 202, {7002});

  SScanPhysiNode scan = {};
  scan.uid = 9000;
  scan.tableType = TSDB_CHILD_TABLE;
  scan.useTriggerTableList = true;
  SReadHandle   handle = {};
  SExecTaskInfo task = {};
  task.pStreamRuntimeInfo = &runtime;
  task.storageAPI.metaFn.isTableExisted = appendDisjointStreamGroupTable;
  STableListInfo *tableList = tableListCreate();
  ASSERT_NE(nullptr, tableList);

  gStreamGroupTableListExistsCalls = 0;
  gStreamGroupTableListScan = &scan;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createStreamMultiGrpTableListInfo(&scan, nullptr, false, &handle, tableList, nullptr,
                                                                 nullptr, &task, nullptr));
  gStreamGroupTableListScan = nullptr;
  EXPECT_EQ(0, gStreamGroupTableListExistsCalls);
  ASSERT_EQ(2, taosArrayGetSize(tableList->pTableList));
  EXPECT_EQ(7001, static_cast<STableKeyInfo *>(taosArrayGet(tableList->pTableList, 0))->uid);
  EXPECT_EQ(7002, static_cast<STableKeyInfo *>(taosArrayGet(tableList->pTableList, 1))->uid);

  tableListDestroy(tableList);
  tClearStreamCurrentGroupRead(&runtime.funcInfo);
}

TEST(StreamGroupTableListTest, FromCondAppendsBroadcastMembershipAfterExistingEntries) {
  SStreamRuntimeFuncInfo runtime = {};
  appendStreamGroupRead(&runtime, 101, {});
  appendStreamGroupRead(&runtime, 202, {});
  appendStreamGroupCalc(&runtime, 101);
  appendStreamGroupCalc(&runtime, 202);

  SScanPhysiNode scan = {};
  scan.uid = 9001;
  scan.tableType = TSDB_CHILD_TABLE;
  SReadHandle   handle = {};
  SExecTaskInfo task = {};
  task.storageAPI.metaFn.isTableExisted = countExistingStreamGroupTable;
  STableListInfo *tableList = tableListCreate();
  ASSERT_NE(tableList, nullptr);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tableListAddTableInfo(tableList, 7001, 77));
  const int32_t originalMapSize = taosHashGetSize(tableList->map);

  ASSERT_EQ(TSDB_CODE_SUCCESS,
            createStreamGrpTableListFromCond(&scan, &handle, &task, &runtime, tableList, nullptr, nullptr, 2, true));
  ASSERT_EQ(3, taosArrayGetSize(tableList->pTableList));
  const auto *existing = static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, 0));
  const auto *first = static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, 1));
  const auto *second = static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, 2));
  ASSERT_NE(nullptr, existing);
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  EXPECT_EQ(7001, existing->uid);
  EXPECT_EQ(77, existing->groupId);
  EXPECT_EQ(9001, first->uid);
  EXPECT_EQ(101, first->baseGId);
  EXPECT_EQ(9001, second->uid);
  EXPECT_EQ(202, second->baseGId);
  const uint64_t existingUid = existing->uid;
  const uint64_t broadcastUid = first->uid;
  EXPECT_EQ(originalMapSize, taosHashGetSize(tableList->map));
  EXPECT_NE(nullptr, taosHashGet(tableList->map, &existingUid, sizeof(existingUid)));
  EXPECT_EQ(nullptr, taosHashGet(tableList->map, &broadcastUid, sizeof(broadcastUid)));

  ASSERT_EQ(TSDB_CODE_SUCCESS, execUtilTestSortStreamTableGroups(tableList));
  EXPECT_EQ(originalMapSize + 1, taosHashGetSize(tableList->map));
  EXPECT_NE(nullptr, taosHashGet(tableList->map, &existingUid, sizeof(existingUid)));
  EXPECT_NE(nullptr, taosHashGet(tableList->map, &broadcastUid, sizeof(broadcastUid)));

  tableListDestroy(tableList);
  tClearStreamCurrentGroupRead(&runtime);
  tSimpleHashCleanup(runtime.pGroupCalcInfos);
}

TEST(StreamGroupTableListTest, FromCondRollbackPreservesExistingEntriesWhenCalcIsMissing) {
  SStreamRuntimeFuncInfo runtime = {};
  appendStreamGroupRead(&runtime, 101, {});
  appendStreamGroupRead(&runtime, 202, {});
  appendStreamGroupCalc(&runtime, 101);

  SScanPhysiNode scan = {};
  scan.uid = 9001;
  scan.tableType = TSDB_CHILD_TABLE;
  SReadHandle   handle = {};
  SExecTaskInfo task = {};
  task.storageAPI.metaFn.isTableExisted = countExistingStreamGroupTable;
  STableListInfo *tableList = tableListCreate();
  ASSERT_NE(tableList, nullptr);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tableListAddTableInfo(tableList, 7001, 77));
  const int32_t originalMapSize = taosHashGetSize(tableList->map);

  EXPECT_EQ(TSDB_CODE_INVALID_PARA,
            createStreamGrpTableListFromCond(&scan, &handle, &task, &runtime, tableList, nullptr, nullptr, 2, true));
  ASSERT_EQ(1, taosArrayGetSize(tableList->pTableList));
  const auto *existing = static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, 0));
  ASSERT_NE(nullptr, existing);
  EXPECT_EQ(7001, existing->uid);
  EXPECT_EQ(77, existing->groupId);
  EXPECT_EQ(originalMapSize, taosHashGetSize(tableList->map));
  EXPECT_NE(nullptr, taosHashGet(tableList->map, &existing->uid, sizeof(existing->uid)));

  tableListDestroy(tableList);
  tClearStreamCurrentGroupRead(&runtime);
  tSimpleHashCleanup(runtime.pGroupCalcInfos);
}

TEST(StreamGroupTableListTest, TbnameBuildsEveryNodeGid) {
  SStreamRuntimeFuncInfo runtime = {};
  runtime.stbPartByTbname = true;
  runtime.groupId = 999;
  appendStreamGroupRead(&runtime, 101, {});
  appendStreamGroupRead(&runtime, 101, {});
  appendStreamGroupRead(&runtime, 202, {});
  SExecTaskInfo   task = {};
  STableListInfo *tableList = tableListCreate();
  ASSERT_NE(tableList, nullptr);

  ASSERT_EQ(TSDB_CODE_SUCCESS, createStreamGrpTableListFromTrig(&task, &runtime, tableList, 3, true));
  ASSERT_EQ(2, taosArrayGetSize(tableList->pTableList));
  const auto *first = static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, 0));
  const auto *second = static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, 1));
  ASSERT_NE(nullptr, first);
  ASSERT_NE(nullptr, second);
  EXPECT_EQ(101, first->uid);
  EXPECT_EQ(101, first->baseGId);
  EXPECT_EQ(202, second->uid);
  EXPECT_EQ(202, second->baseGId);

  tableListDestroy(tableList);
  tClearStreamCurrentGroupRead(&runtime);
}

TEST(StreamGroupTableListTest, FromTrigDeduplicatesRepeatedUidSegmentsWithinGid) {
  SStreamRuntimeFuncInfo runtime = {};
  appendStreamGroupRead(&runtime, 101, {9001});
  appendStreamGroupRead(&runtime, 101, {9001});
  appendStreamGroupRead(&runtime, 202, {9002});
  SExecTaskInfo   task = {};
  STableListInfo *tableList = tableListCreate();
  ASSERT_NE(tableList, nullptr);

  ASSERT_EQ(TSDB_CODE_SUCCESS, createStreamGrpTableListFromTrig(&task, &runtime, tableList, 3, true));
  ASSERT_EQ(2, taosArrayGetSize(tableList->pTableList));
  EXPECT_EQ(9001, static_cast<STableKeyInfo *>(taosArrayGet(tableList->pTableList, 0))->uid);
  EXPECT_EQ(101, static_cast<STableKeyInfo *>(taosArrayGet(tableList->pTableList, 0))->baseGId);
  EXPECT_EQ(9002, static_cast<STableKeyInfo *>(taosArrayGet(tableList->pTableList, 1))->uid);
  EXPECT_EQ(202, static_cast<STableKeyInfo *>(taosArrayGet(tableList->pTableList, 1))->baseGId);

  tableListDestroy(tableList);
  tClearStreamCurrentGroupRead(&runtime);
}

TEST(StreamGroupTableListTest, RejectsSameUidBoundToDifferentGids) {
  SStreamRuntimeFuncInfo runtime = {};
  appendStreamGroupRead(&runtime, 101, {9001});
  appendStreamGroupRead(&runtime, 202, {9001});
  SExecTaskInfo   task = {};
  STableListInfo *tableList = tableListCreate();
  ASSERT_NE(tableList, nullptr);

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, createStreamGrpTableListFromTrig(&task, &runtime, tableList, 2, true));
  EXPECT_EQ(0, taosArrayGetSize(tableList->pTableList));

  tableListDestroy(tableList);
  tClearStreamCurrentGroupRead(&runtime);
}

TEST(StreamGroupTableListTest, FromTrigRollbackPreservesExistingMapOnConflict) {
  SStreamRuntimeFuncInfo runtime = {};
  appendStreamGroupRead(&runtime, 101, {9001, 9002, 9003});
  appendStreamGroupRead(&runtime, 202, {9003});
  SExecTaskInfo   task = {};
  STableListInfo *tableList = tableListCreate();
  ASSERT_NE(tableList, nullptr);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tableListAddTableInfo(tableList, 7001, 77));
  const int32_t originalMapSize = taosHashGetSize(tableList->map);

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, createStreamGrpTableListFromTrig(&task, &runtime, tableList, 2, false));
  ASSERT_EQ(1, taosArrayGetSize(tableList->pTableList));
  const auto *existing = static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, 0));
  ASSERT_NE(nullptr, existing);
  EXPECT_EQ(7001, existing->uid);
  EXPECT_EQ(77, existing->groupId);
  EXPECT_EQ(originalMapSize, taosHashGetSize(tableList->map));
  EXPECT_NE(nullptr, taosHashGet(tableList->map, &existing->uid, sizeof(existing->uid)));
  for (const uint64_t addedUid : {9001, 9002, 9003}) {
    EXPECT_EQ(nullptr, taosHashGet(tableList->map, &addedUid, sizeof(addedUid)));
  }

  tableListDestroy(tableList);
  tClearStreamCurrentGroupRead(&runtime);
}

TEST(StreamGroupTableListTest, SortFailurePreservesListMapAndOffsets) {
  STableListInfo *tableList = tableListCreate();
  ASSERT_NE(tableList, nullptr);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tableListAddTableInfo(tableList, 7001, 20));
  ASSERT_EQ(TSDB_CODE_SUCCESS, tableListAddTableInfo(tableList, 7002, 10));
  ASSERT_EQ(TSDB_CODE_SUCCESS, execUtilTestSortStreamTableGroups(tableList));
  ASSERT_EQ(2, tableList->numOfOuputGroups);
  ASSERT_NE(nullptr, tableList->groupOffset);

  ASSERT_EQ(TSDB_CODE_SUCCESS, tableListAddTableInfo(tableList, 7003, 5));
  std::vector<STableKeyInfo> before;
  for (int32_t i = 0; i < taosArrayGetSize(tableList->pTableList); ++i) {
    before.push_back(*static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, i)));
  }
  const int32_t        oldGroupCount = tableList->numOfOuputGroups;
  std::vector<int32_t> oldOffsets(tableList->groupOffset, tableList->groupOffset + oldGroupCount);

  execUtilTestFailStreamGroupSortHashPutAfter(1);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, execUtilTestSortStreamTableGroups(tableList));
  ASSERT_EQ(static_cast<int32_t>(before.size()), taosArrayGetSize(tableList->pTableList));
  for (int32_t i = 0; i < taosArrayGetSize(tableList->pTableList); ++i) {
    const auto *actual = static_cast<const STableKeyInfo *>(taosArrayGet(tableList->pTableList, i));
    ASSERT_NE(nullptr, actual);
    EXPECT_EQ(before[i].uid, actual->uid);
    EXPECT_EQ(before[i].groupId, actual->groupId);
    const auto *pIndex = static_cast<const int32_t *>(taosHashGet(tableList->map, &actual->uid, sizeof(actual->uid)));
    ASSERT_NE(nullptr, pIndex);
    EXPECT_EQ(i, *pIndex);
  }
  EXPECT_EQ(oldGroupCount, tableList->numOfOuputGroups);
  ASSERT_NE(nullptr, tableList->groupOffset);
  for (int32_t i = 0; i < oldGroupCount; ++i) EXPECT_EQ(oldOffsets[i], tableList->groupOffset[i]);

  tableListDestroy(tableList);
}

TEST(streamDataInserterTest, groupTableHashUsesFullStreamGroupKey) {
  ASSERT_EQ(initInserterGrpInfo(), TSDB_CODE_SUCCESS);
  ASSERT_NE(gStreamGrpTableHash, nullptr);

  constexpr int32_t numOfGroups = 512;
  constexpr int64_t streamId = 0x12345678;

  for (int64_t groupId = 0; groupId < numOfGroups; ++groupId) {
    int64_t key[2] = {streamId, groupId};
    auto    pInfo = (SInsertTableInfo *)taosMemoryCalloc(1, sizeof(SInsertTableInfo));
    ASSERT_NE(pInfo, nullptr);
    ASSERT_EQ(taosHashPut(gStreamGrpTableHash, key, sizeof(key), &pInfo, sizeof(SInsertTableInfo *)),
              TSDB_CODE_SUCCESS);
  }

  EXPECT_LT(taosHashGetMaxOverflowLinkLength(gStreamGrpTableHash), 64);

  taosHashCleanup(gStreamGrpTableHash);
  gStreamGrpTableHash = nullptr;
}

TEST(execUtilTest, stableTagFilterCacheWarmsAllEqualTagGroupsOnMiss) {
  char oldStableTagFilterCache = tsStableTagFilterCache;
  tsStableTagFilterCache = 1;

  StableTagFilterWarmupMock mock;
  SStorageAPI               api = {0};
  api.metaFn.getStableCachedTableList = mockGetStableCachedTableList;
  api.metaFn.warmupStableCachedTableList = mockWarmupStableCachedTableList;
  api.metaFn.getTableTags = mockGetTableTags;
  api.metaFn.putStableCachedTableList = mockPutStableCachedTableList;

  SScanPhysiNode scan;
  memset(&scan, 0, sizeof(scan));
  scan.suid = 42;
  scan.uid = 42;
  scan.tableType = TSDB_SUPER_TABLE;

  SStreamRuntimeFuncInfo streamInfo = {0};
  STableListInfo         tableListInfo = {0};
  tableListInfo.pTableList = taosArrayInit(8, sizeof(STableKeyInfo));
  ASSERT_NE(tableListInfo.pTableList, nullptr);

  SNode *pTagCond = makeEqualCond(makeTagColumn(3, TSDB_DATA_TYPE_INT, sizeof(int32_t)), makeIntValue(7));

  ASSERT_EQ(getTableList(&mock, &scan, pTagCond, nullptr, &tableListInfo, nullptr, "stable-cache-warmup", &api,
                         &streamInfo, 0),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(taosArrayGetSize(tableListInfo.pTableList), 1);

  STableKeyInfo *pTable = static_cast<STableKeyInfo *>(taosArrayGet(tableListInfo.pTableList, 0));
  ASSERT_NE(pTable, nullptr);
  EXPECT_EQ(pTable->uid, 1001);
  EXPECT_EQ(mock.getCacheCalls, 1);
  EXPECT_EQ(mock.warmupCalls, 1);
  EXPECT_EQ(mock.getTableTagsCalls, 0);

  nodesDestroyNode(pTagCond);
  taosArrayDestroy(tableListInfo.pTableList);
  tsStableTagFilterCache = oldStableTagFilterCache;
}

TEST(execUtilTest, stableTagFilterCacheFallsBackWhenWarmupMissesDigest) {
  char oldStableTagFilterCache = tsStableTagFilterCache;
  tsStableTagFilterCache = 1;

  StableTagFilterWarmupMock mock;
  mock.cacheHitAfterWarmup = false;

  SStorageAPI api = {0};
  api.metaFn.getStableCachedTableList = mockGetStableCachedTableList;
  api.metaFn.warmupStableCachedTableList = mockWarmupStableCachedTableList;
  api.metaFn.getTableTags = mockGetTableTags;
  api.metaFn.putStableCachedTableList = mockPutStableCachedTableList;

  SScanPhysiNode scan;
  memset(&scan, 0, sizeof(scan));
  scan.suid = 42;
  scan.uid = 42;
  scan.tableType = TSDB_SUPER_TABLE;

  SStreamRuntimeFuncInfo streamInfo = {0};
  STableListInfo         tableListInfo = {0};
  tableListInfo.pTableList = taosArrayInit(8, sizeof(STableKeyInfo));
  ASSERT_NE(tableListInfo.pTableList, nullptr);

  SNode *pTagCond = makeEqualCond(makeTagColumn(3, TSDB_DATA_TYPE_INT, sizeof(int32_t)), makeIntValue(7));

  ASSERT_EQ(getTableList(&mock, &scan, pTagCond, nullptr, &tableListInfo, nullptr, "stable-cache-warmup-miss", &api,
                         &streamInfo, 0),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosArrayGetSize(tableListInfo.pTableList), 0);
  EXPECT_EQ(mock.getCacheCalls, 1);
  EXPECT_EQ(mock.warmupCalls, 1);
  EXPECT_EQ(mock.getTableTagsCalls, 1);

  nodesDestroyNode(pTagCond);
  taosArrayDestroy(tableListInfo.pTableList);
  tsStableTagFilterCache = oldStableTagFilterCache;
}

TEST(execUtilTest, stableTagFilterCacheSkipsWarmupWhenEntryAlreadyPrewarmed) {
  char oldStableTagFilterCache = tsStableTagFilterCache;
  tsStableTagFilterCache = 1;

  StableTagFilterWarmupMock mock;
  mock.warmed = true;
  mock.cacheHitAfterWarmup = false;

  SStorageAPI api = {0};
  api.metaFn.getStableCachedTableList = mockGetStableCachedTableList;
  api.metaFn.warmupStableCachedTableList = mockWarmupStableCachedTableList;
  api.metaFn.getTableTags = mockGetTableTags;
  api.metaFn.putStableCachedTableList = mockPutStableCachedTableList;

  SScanPhysiNode scan;
  memset(&scan, 0, sizeof(scan));
  scan.suid = 42;
  scan.uid = 42;
  scan.tableType = TSDB_SUPER_TABLE;

  SStreamRuntimeFuncInfo streamInfo = {0};
  STableListInfo         tableListInfo = {0};
  tableListInfo.pTableList = taosArrayInit(8, sizeof(STableKeyInfo));
  ASSERT_NE(tableListInfo.pTableList, nullptr);

  SNode *pTagCond = makeEqualCond(makeTagColumn(3, TSDB_DATA_TYPE_INT, sizeof(int32_t)), makeIntValue(7));

  ASSERT_EQ(getTableList(&mock, &scan, pTagCond, nullptr, &tableListInfo, nullptr, "stable-cache-prewarmed-miss", &api,
                         &streamInfo, 0),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosArrayGetSize(tableListInfo.pTableList), 0);
  EXPECT_EQ(mock.getCacheCalls, 1);
  EXPECT_EQ(mock.warmupCalls, 0);
  EXPECT_EQ(mock.getTableTagsCalls, 1);

  nodesDestroyNode(pTagCond);
  taosArrayDestroy(tableListInfo.pTableList);
  tsStableTagFilterCache = oldStableTagFilterCache;
}

TEST(execUtilTest, buildRunnerFetchInfoProjectsCurrentAncestorContext) {
  SStreamRuntimeFuncInfo source = {};
  source.groupId = 101;
  source.curIdx = 1;
  source.addOptions = STREAM_OPTION_NESTED_WINDOW_PLAN;
  source.pGroupCalcOrder = taosArrayInit(1, sizeof(uint64_t));
  ASSERT_NE(source.pGroupCalcOrder, nullptr);
  const uint64_t orderGid = 101;
  ASSERT_NE(taosArrayPush(source.pGroupCalcOrder, &orderGid), nullptr);
  source.pStreamPesudoFuncVals = taosArrayInit(2, sizeof(SSTriggerCalcParam));
  ASSERT_NE(source.pStreamPesudoFuncVals, nullptr);
  SSTriggerCalcParam first = {};
  first.wstart = 100;
  first.wend = 199;
  first.notifyType = BIT_FLAG_MASK(0);
  SSTriggerCalcParam second = {};
  second.wstart = 200;
  second.wend = 299;
  second.notifyType = BIT_FLAG_MASK(0);
  ASSERT_NE(taosArrayPush(source.pStreamPesudoFuncVals, &first), nullptr);
  ASSERT_NE(taosArrayPush(source.pStreamPesudoFuncVals, &second), nullptr);
  source.pContextPolicy = makeFetchProjectionPolicy(source.groupId, 1);
  source.pAncestorContext = makeFetchProjectionContext(source.groupId, 1);

  SStreamRuntimeFuncInfo projected = {};
  ASSERT_EQ(buildStreamRunnerFetchRtInfo(&source, &projected), TSDB_CODE_SUCCESS);
  EXPECT_EQ(projected.pGroupCalcOrder, nullptr);
  ASSERT_NE(projected.pContextPolicy, nullptr);
  EXPECT_NE(source.pContextPolicy, projected.pContextPolicy);
  ASSERT_EQ(taosArrayGetSize(projected.pContextPolicy->pEntries), 1);
  const auto *policyEntry =
      static_cast<const SStreamContextPolicyEntry *>(taosArrayGet(projected.pContextPolicy->pEntries, 0));
  ASSERT_NE(policyEntry, nullptr);
  EXPECT_EQ(policyEntry->paramIndex, 0);
  ASSERT_NE(projected.pAncestorContext, nullptr);
  EXPECT_NE(source.pAncestorContext, projected.pAncestorContext);
  ASSERT_EQ(taosArrayGetSize(projected.pAncestorContext->pParamContexts), 1);
  const auto *param =
      static_cast<const SStreamAncestorParamContext *>(taosArrayGet(projected.pAncestorContext->pParamContexts, 0));
  ASSERT_NE(param, nullptr);
  EXPECT_EQ(param->paramIndex, 0);
  const auto *snapshot = static_cast<const SWindowAncestorSnapshot *>(taosArrayGet(param->pSnapshots, 0));
  ASSERT_NE(snapshot, nullptr);
  EXPECT_EQ(snapshot->values.window.start, 100);

  tDestroyStRtFuncInfo(&source);
  ASSERT_NE(projected.pContextPolicy, nullptr);
  ASSERT_NE(projected.pAncestorContext, nullptr);
  cleanupStreamRunnerFetchRtInfo(&projected);
}

// Test for fix of double-free bug in projectApplyFunction (GHSA-f8pf-77fh-53wv).
//
// When scalarCalculate succeeds but the subsequent colDataMergeCol/colDataAssign
// fails, pBlockList was freed manually and then freed again at _exit because the
// pointer was not nullified after the first taosArrayDestroy call.
//
// The fix: set pBlockList = NULL immediately after taosArrayDestroy so the
// second call at _exit is a safe no-op.
//
// This test exercises the scalar function path of projectApplyFunctions using
// the abs() built-in and verifies:
//   1. Success path (createNewColModel=true): result column is populated correctly.
//   2. Merge path (createNewColModel=false, pResult->info.rows > 0): subsequent
//      rows are appended without memory errors.
//
// Running under AddressSanitizer (-fsanitize=address) will catch any double-free
// regression on either path.
TEST(projectApplyFunctionsTest, scalarFuncNoDoubleFreeOnErrorAndSuccessPath) {
  ASSERT_EQ(fmFuncMgtInit(), TSDB_CODE_SUCCESS);

  // --- resolve abs() funcId ---
  int32_t absFuncId = fmGetFuncId("abs");
  ASSERT_GE(absFuncId, 0);
  ASSERT_TRUE(fmIsScalarFunc(absFuncId));

  // --- build SFunctionNode for abs(col) ---
  SFunctionNode *pFuncNode = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION, reinterpret_cast<SNode **>(&pFuncNode)), TSDB_CODE_SUCCESS);
  pFuncNode->funcId = absFuncId;
  pFuncNode->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pFuncNode->node.resType.bytes = sizeof(int64_t);

  // Build the column parameter node for abs(col0)
  SColumnNode *pColNode = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&pColNode)), TSDB_CODE_SUCCESS);
  pColNode->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pColNode->node.resType.bytes = sizeof(int64_t);
  pColNode->slotId = 0;
  ASSERT_EQ(nodesListMakeAppend(&pFuncNode->pParameterList, reinterpret_cast<SNode *>(pColNode)), TSDB_CODE_SUCCESS);

  // --- build tExprNode wrapper ---
  tExprNode exprNode;
  memset(&exprNode, 0, sizeof(exprNode));
  exprNode.nodeType = QUERY_NODE_FUNCTION;
  exprNode._function.functionId = absFuncId;
  exprNode._function.pFunctNode = pFuncNode;

  // --- build SExprInfo ---
  SExprInfo exprInfo;
  memset(&exprInfo, 0, sizeof(exprInfo));
  exprInfo.pExpr = &exprNode;
  exprInfo.base.resSchema.type = TSDB_DATA_TYPE_BIGINT;
  exprInfo.base.resSchema.bytes = sizeof(int64_t);
  exprInfo.base.resSchema.slotId = 0;

  // --- build SqlFunctionCtx (only the fields accessed on the scalar path) ---
  SExprInfo ctxExprInfo;
  memset(&ctxExprInfo, 0, sizeof(ctxExprInfo));
  ctxExprInfo.base.pParamList = nullptr;  // ensures fmIsPlaceHolderFunc branch is skipped

  SqlFunctionCtx funcCtx;
  memset(&funcCtx, 0, sizeof(funcCtx));
  funcCtx.functionId = static_cast<int16_t>(absFuncId);
  funcCtx.pExpr = &ctxExprInfo;

  // --- build source block with one BIGINT column, 3 rows ---
  SSDataBlock *pSrc = nullptr;
  ASSERT_EQ(createDataBlock(&pSrc), TSDB_CODE_SUCCESS);
  SColumnInfoData srcCol = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 1);
  ASSERT_EQ(blockDataAppendColInfo(pSrc, &srcCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pSrc, 3), TSDB_CODE_SUCCESS);
  pSrc->info.rows = 3;
  const SBlockID sourceId = {701, 801, 101, 101};
  pSrc->info.id = sourceId;
  int64_t vals[3] = {-1LL, 2LL, -3LL};
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(colDataSetVal(static_cast<SColumnInfoData *>(taosArrayGet(pSrc->pDataBlock, 0)), i,
                            reinterpret_cast<const char *>(&vals[i]), false),
              TSDB_CODE_SUCCESS);
  }

  // --- build result block with matching BIGINT output column ---
  SSDataBlock *pResult = nullptr;
  ASSERT_EQ(createDataBlock(&pResult), TSDB_CODE_SUCCESS);
  SColumnInfoData resCol = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 1);
  ASSERT_EQ(blockDataAppendColInfo(pResult, &resCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pResult, 8), TSDB_CODE_SUCCESS);
  const SBlockID resultId = {17, 18, 0, 0};
  pResult->info.id = resultId;

  SArray *pPseudoList = taosArrayInit(1, sizeof(int32_t));
  ASSERT_NE(pPseudoList, nullptr);

  // --- Test 1: createNewColModel path (pResult->info.rows == 0) ---
  // Exercises colDataAssign; pBlockList must be freed exactly once.
  pResult->info.rows = 0;
  int32_t code = projectApplyFunctions(&exprInfo, pResult, pSrc, &funcCtx, 1, pPseudoList, nullptr, nullptr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_EQ(pResult->info.rows, 3);
  EXPECT_EQ(pResult->info.id.uid, resultId.uid);
  EXPECT_EQ(pResult->info.id.blockId, resultId.blockId);
  EXPECT_EQ(pResult->info.id.groupId, sourceId.groupId);
  EXPECT_EQ(pResult->info.id.baseGId, sourceId.baseGId);

  // Verify abs values: |-1|=1, |2|=2, |-3|=3
  SColumnInfoData *pOut = static_cast<SColumnInfoData *>(taosArrayGet(pResult->pDataBlock, 0));
  ASSERT_NE(pOut, nullptr);
  EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(pOut, 0)), 1LL);
  EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(pOut, 1)), 2LL);
  EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(pOut, 2)), 3LL);

  // --- Test 2: merge path (pResult->info.rows > 0, createNewColModel=false) ---
  // Exercises colDataMergeCol; pBlockList must also be freed exactly once.
  // pResult already has 3 rows; append 3 more.
  ASSERT_EQ(blockDataEnsureCapacity(pResult, 6), TSDB_CODE_SUCCESS);
  code = projectApplyFunctions(&exprInfo, pResult, pSrc, &funcCtx, 1, pPseudoList, nullptr, nullptr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_EQ(pResult->info.rows, 6);
  EXPECT_EQ(pResult->info.id.uid, resultId.uid);
  EXPECT_EQ(pResult->info.id.blockId, resultId.blockId);
  EXPECT_EQ(pResult->info.id.groupId, sourceId.groupId);
  EXPECT_EQ(pResult->info.id.baseGId, sourceId.baseGId);

  // Verify the newly appended rows
  EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(pOut, 3)), 1LL);
  EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(pOut, 4)), 2LL);
  EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(pOut, 5)), 3LL);

  // --- cleanup ---
  taosArrayDestroy(pPseudoList);
  blockDataDestroy(pSrc);
  blockDataDestroy(pResult);
  nodesDestroyNode(reinterpret_cast<SNode *>(pFuncNode));
  fmFuncMgtDestroy();
}

struct ExecutorCorrelationMock {
  int32_t calls = 0;
  int32_t failOnCall = 0;
};

static int32_t invokeExecutorCorrelationMock(SCorrelationTaskSession* pSession,
                                             const SCorrelationScalarRequest* pRequest,
                                             SValueNode** ppResult) {
  auto* pMock = reinterpret_cast<ExecutorCorrelationMock*>(pSession);
  auto* pParam = static_cast<const SCorrelationParamValue*>(taosArrayGet(pRequest->pParams, 0));
  if (nullptr == pParam || nullptr == pParam->pData || pParam->dataType.type != TSDB_DATA_TYPE_INT) {
    return TSDB_CODE_QRY_INVALID_CORRELATION_NODE;
  }
  ++pMock->calls;
  if (pMock->calls == pMock->failOnCall) {
    return TSDB_CODE_QRY_SUBQ_EXEC_ERROR;
  }
  SNode* pNode = nullptr;
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE, &pNode);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  auto* pValue = reinterpret_cast<SValueNode*>(pNode);
  pValue->node.resType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)};
  pValue->typeData = *static_cast<const int32_t*>(pParam->pData) * 10;
  *ppResult = pValue;
  return TSDB_CODE_SUCCESS;
}

static int32_t reserveExecutorCorrelationMock(SCorrelationTaskSession*, int64_t) {
  return TSDB_CODE_SUCCESS;
}

static void releaseExecutorCorrelationMock(SCorrelationTaskSession*, int64_t) {}

static SCorrelatedSubqueryNode* makeExecutorCorrelationCall(int64_t blockId) {
  SNode* pNode = nullptr;
  if (TSDB_CODE_SUCCESS != nodesMakeNode(QUERY_NODE_CORRELATED_SUBQUERY, &pNode)) {
    return nullptr;
  }
  auto* pCall = reinterpret_cast<SCorrelatedSubqueryNode*>(pNode);
  pCall->node.resType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)};
  SNode* pBindingNode = nullptr;
  if (TSDB_CODE_SUCCESS != nodesMakeNode(QUERY_NODE_CORRELATION_PARAM_BINDING, &pBindingNode)) {
    nodesDestroyNode(pNode);
    return nullptr;
  }
  auto* pBinding = reinterpret_cast<SCorrelationParamBindingNode*>(pBindingNode);
  pBinding->dataType = pCall->node.resType;
  pBinding->sourceKind = CORRELATION_SOURCE_BLOCK;
  pBinding->sourceBlockId = blockId;
  pBinding->sourceSlotId = 0;
  if (TSDB_CODE_SUCCESS != nodesListMakeStrictAppend(&pCall->pParamBindings, pBindingNode)) {
    nodesDestroyNode(pBindingNode);
    nodesDestroyNode(pNode);
    return nullptr;
  }
  return pCall;
}

TEST(CorrelationExecutorTest, ProjectMaterializesEachInputRow) {
  SSDataBlock* pBlock = nullptr;
  ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
  SColumnInfoData input = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  SColumnInfoData result = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
  ASSERT_EQ(blockDataAppendColInfo(pBlock, &input), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(pBlock, &result), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pBlock, 3), TSDB_CODE_SUCCESS);
  pBlock->info.id.blockId = 71;
  pBlock->info.rows = 3;
  for (int32_t row = 0; row < 3; ++row) {
    int32_t value = row + 1;
    ASSERT_EQ(colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 0)), row,
                            reinterpret_cast<const char*>(&value), false), TSDB_CODE_SUCCESS);
  }

  auto* pCall = makeExecutorCorrelationCall(pBlock->info.id.blockId);
  ASSERT_NE(pCall, nullptr);
  SExprInfo expr = {};
  ASSERT_EQ(createExprFromOneNode(&expr, reinterpret_cast<SNode*>(pCall), 1), TSDB_CODE_SUCCESS);
  ExecutorCorrelationMock mock;
  SCorrelationRuntimeOps ops = {};
  ops.invokeScalar = invokeExecutorCorrelationMock;
  ops.reserveResultMemory = reserveExecutorCorrelationMock;
  ops.releaseResultMemory = releaseExecutorCorrelationMock;
  SExecTaskInfo task = {};
  task.pCorrelationOps = &ops;
  task.pCorrelationSession = reinterpret_cast<SCorrelationTaskSession*>(&mock);
  SqlFunctionCtx func = {};
  ASSERT_EQ(projectApplyFunctions(&expr, pBlock, pBlock, &func, 1, nullptr, nullptr, &task), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mock.calls, 3);
  auto* pOutput = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 1));
  for (int32_t row = 0; row < 3; ++row) {
    EXPECT_EQ(*reinterpret_cast<int32_t*>(colDataGetData(pOutput, row)), (row + 1) * 10);
  }

  destroyExprInfo(&expr, 1);
  nodesDestroyNode(reinterpret_cast<SNode*>(pCall));
  blockDataDestroy(pBlock);
}

TEST(CorrelationExecutorTest, FilterKeepsInputColumnsAndPropagatesCallError) {
  SSDataBlock* pBlock = nullptr;
  ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
  SColumnInfoData parameter = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  SColumnInfoData voltage = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
  ASSERT_EQ(blockDataAppendColInfo(pBlock, &parameter), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataAppendColInfo(pBlock, &voltage), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pBlock, 3), TSDB_CODE_SUCCESS);
  pBlock->info.id.blockId = 72;
  pBlock->info.rows = 3;
  int32_t voltages[] = {15, 15, 35};
  for (int32_t row = 0; row < 3; ++row) {
    int32_t value = row + 1;
    ASSERT_EQ(colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 0)), row,
                            reinterpret_cast<const char*>(&value), false), TSDB_CODE_SUCCESS);
    ASSERT_EQ(colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 1)), row,
                            reinterpret_cast<const char*>(&voltages[row]), false), TSDB_CODE_SUCCESS);
  }

  auto* pCall = makeExecutorCorrelationCall(pBlock->info.id.blockId);
  ASSERT_NE(pCall, nullptr);
  SNode* pColumnNode = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &pColumnNode), TSDB_CODE_SUCCESS);
  auto* pColumn = reinterpret_cast<SColumnNode*>(pColumnNode);
  pColumn->node.resType = {.type = TSDB_DATA_TYPE_INT, .bytes = sizeof(int32_t)};
  pColumn->dataBlockId = pBlock->info.id.blockId;
  pColumn->slotId = 1;
  SNode* pCondition = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_OPERATOR, &pCondition), TSDB_CODE_SUCCESS);
  auto* pOperator = reinterpret_cast<SOperatorNode*>(pCondition);
  pOperator->node.resType = {.type = TSDB_DATA_TYPE_BOOL, .bytes = sizeof(bool)};
  pOperator->opType = OP_TYPE_GREATER_THAN;
  pOperator->pLeft = pColumnNode;
  pOperator->pRight = reinterpret_cast<SNode*>(pCall);
  SFilterInfo* pFilter = nullptr;
  ASSERT_EQ(filterInitFromNode(pCondition, &pFilter, 0, nullptr), TSDB_CODE_SUCCESS);

  ExecutorCorrelationMock mock;
  SCorrelationRuntimeOps ops = {};
  ops.invokeScalar = invokeExecutorCorrelationMock;
  ops.reserveResultMemory = reserveExecutorCorrelationMock;
  ops.releaseResultMemory = releaseExecutorCorrelationMock;
  SExecTaskInfo task = {};
  task.pCorrelationOps = &ops;
  task.pCorrelationSession = reinterpret_cast<SCorrelationTaskSession*>(&mock);
  mock.failOnCall = 2;
  EXPECT_EQ(doFilterWithTaskCorrelation(pBlock, pFilter, nullptr, nullptr, &task),
            TSDB_CODE_QRY_SUBQ_EXEC_ERROR);
  EXPECT_EQ(pBlock->info.rows, 3);
  mock.calls = 0;
  mock.failOnCall = 0;
  ASSERT_EQ(doFilterWithTaskCorrelation(pBlock, pFilter, nullptr, nullptr, &task), TSDB_CODE_SUCCESS);
  EXPECT_EQ(mock.calls, 3);
  ASSERT_EQ(pBlock->info.rows, 2);
  auto* pParams = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t*>(colDataGetData(pParams, 0)), 1);
  EXPECT_EQ(*reinterpret_cast<int32_t*>(colDataGetData(pParams, 1)), 3);

  filterFreeInfo(pFilter);
  nodesDestroyNode(pCondition);
  blockDataDestroy(pBlock);
}

// Test for fix of double-free bug in handleRemoteRowRes / extractSingleRspBlock
// (GHSA-2gr8-qqmv-v247).
//
// extractSingleRspBlock previously destroyed the caller-owned SSDataBlock on
// error, and handleRemoteRowRes then destroyed the same block again in its
// unified cleanup path. The fix keeps cleanup ownership in the caller.
TEST(execUtilTest, handleRemoteRowResNoDoubleFreeOnExtractFailure) {
  SExecTaskInfo     task;
  STaskSubJobCtx    ctx = {0};
  SScalarFetchParam param = {0};
  SRemoteRowNode    remote;
  SRetrieveTableRsp rsp = {0};
  bool              fetchDone = false;
  SNode            *pStored = nullptr;

  memset(&task, 0, sizeof(task));
  memset(&remote, 0, sizeof(remote));
  task.execModel = OPTR_EXEC_MODEL_BATCH;

  ctx.idStr = const_cast<char *>("executil-test");
  ctx.pTaskInfo = &task;
  ctx.subResNodes = taosArrayInit(1, POINTER_BYTES);
  ASSERT_NE(ctx.subResNodes, nullptr);
  ASSERT_NE(taosArrayPush(ctx.subResNodes, &pStored), nullptr);

  remote.val.node.resType.type = TSDB_DATA_TYPE_INT;
  remote.val.node.resType.bytes = sizeof(int32_t);
  remote.val.flag = VALUE_FLAG_VAL_UNSET;

  param.subQIdx = 0;
  param.pRes = reinterpret_cast<SNode *>(&remote);

  rsp.numOfRows = 1;
  rsp.numOfBlocks = 1;
  rsp.numOfCols = 2;
  rsp.completed = true;

  execUtilTestResetHandleRemoteRowResState();
  execUtilTestSetExtractSingleRspBlockFailOnce(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

  handleRemoteRowRes(&param, &ctx, &rsp, &fetchDone);

  EXPECT_EQ(ctx.code, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  EXPECT_TRUE(fetchDone);
  EXPECT_EQ(execUtilTestGetHandleRemoteRowResDestroyCount(), 1);
  EXPECT_FALSE(remote.valSet);
  EXPECT_FALSE(remote.hasValue);

  execUtilTestResetHandleRemoteRowResState();
  taosArrayDestroy(ctx.subResNodes);
}

TEST(execUtilTest, remoteScalarCopyRetainsRemotePlanOwnershipAfterValueRewrite) {
  SValueNode* pCached = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode**>(&pCached)), TSDB_CODE_SUCCESS);
  pCached->node.resType.type = TSDB_DATA_TYPE_INT;
  pCached->node.resType.bytes = sizeof(int32_t);
  pCached->literal = taosStrdup("cached value");
  ASSERT_NE(pCached->literal, nullptr);

  const ENodeType remoteTypes[] = {QUERY_NODE_REMOTE_VALUE, QUERY_NODE_REMOTE_ZERO_ROWS};
  for (ENodeType type : remoteTypes) {
    SRemoteValueNode* pRemote = nullptr;
    ASSERT_EQ(nodesMakeNode(type, reinterpret_cast<SNode**>(&pRemote)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &pRemote->pRemotePlan), TSDB_CODE_SUCCESS);
    pRemote->val.literal = taosStrdup("serialized remote subquery");
    ASSERT_NE(pRemote->val.literal, nullptr);

    ASSERT_EQ(remoteNodeCopy(reinterpret_cast<SNode*>(pCached), reinterpret_cast<SNode*>(pRemote)), TSDB_CODE_SUCCESS);
    EXPECT_EQ(nodeType(reinterpret_cast<SNode*>(pRemote)), QUERY_NODE_VALUE);
    EXPECT_NE(pRemote->val.flag & VALUE_FLAG_REMOTE_PLAN, 0);

    nodesDestroyNode(reinterpret_cast<SNode*>(pRemote));
  }

  nodesDestroyNode(reinterpret_cast<SNode*>(pCached));
}

TEST(execUtilTest, externalWindowReusableBlockGrowsToRequestedRows) {
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);

  SColumnInfoData col = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1);
  ASSERT_EQ(blockDataAppendColInfo(pBlock, &col), TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDataEnsureCapacity(pBlock, 4096), TSDB_CODE_SUCCESS);
  ASSERT_EQ(pBlock->info.capacity, 4096);

  SArray *pIdx = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(pIdx, nullptr);

  SList *pFreeBlocks = tdListNew(POINTER_BYTES * 2);
  SList *pTargetBlocks = tdListNew(POINTER_BYTES * 2);
  ASSERT_NE(pFreeBlocks, nullptr);
  ASSERT_NE(pTargetBlocks, nullptr);

  void *reusable[2] = {pBlock, pIdx};
  ASSERT_EQ(tdListAppend(pFreeBlocks, reusable), TSDB_CODE_SUCCESS);

  SSDataBlock *pReusedBlock = nullptr;
  SArray      *pReusedIdx = nullptr;
  ASSERT_EQ(extWinTestTakeReusableBlock(pFreeBlocks, pTargetBlocks, 4097, &pReusedBlock, &pReusedIdx),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pReusedBlock, pBlock);
  EXPECT_EQ(pReusedIdx, pIdx);
  EXPECT_GE(pReusedBlock->info.capacity, 4097);
  EXPECT_EQ(listNEles(pFreeBlocks), 0);
  EXPECT_EQ(listNEles(pTargetBlocks), 1);

  SListNode *pNode = listTail(pTargetBlocks);
  ASSERT_NE(pNode, nullptr);
  int32_t overlapCol = -1;
  EXPECT_TRUE(extWinTestBlockNodeInvariantHolds(pTargetBlocks, pNode, pNode->dl_prev_, pReusedBlock, pReusedIdx, 0,
                                                &overlapCol));
  EXPECT_EQ(overlapCol, -1);

  SColumnInfoData *pCol = static_cast<SColumnInfoData *>(taosArrayGet(pReusedBlock->pDataBlock, 0));
  ASSERT_NE(pCol, nullptr);
  char *pOriginalData = pCol->pData;
  pCol->pData = reinterpret_cast<char *>(pNode);
  EXPECT_FALSE(extWinTestBlockNodeInvariantHolds(pTargetBlocks, pNode, pNode->dl_prev_, pReusedBlock, pReusedIdx, 0,
                                                 &overlapCol));
  EXPECT_EQ(overlapCol, 0);
  pCol->pData = pOriginalData;

  pNode = tdListPopHead(pTargetBlocks);
  ASSERT_NE(pNode, nullptr);
  blockDataDestroy(*(SSDataBlock **)pNode->data);
  taosArrayDestroy(*(SArray **)((SSDataBlock **)pNode->data + 1));
  taosMemoryFree(pNode);
  tdListFree(pFreeBlocks);
  tdListFree(pTargetBlocks);
}

TEST(execUtilTest, externalWindowReusableBlockRejectsEmptyFreeListAsInternalError) {
  SList *pFreeBlocks = tdListNew(POINTER_BYTES * 2);
  SList *pTargetBlocks = tdListNew(POINTER_BYTES * 2);
  ASSERT_NE(pFreeBlocks, nullptr);
  ASSERT_NE(pTargetBlocks, nullptr);

  SSDataBlock *pBlock = nullptr;
  SArray      *pIdx = nullptr;
  EXPECT_EQ(extWinTestTakeReusableBlock(pFreeBlocks, pTargetBlocks, 1, &pBlock, &pIdx),
            TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  EXPECT_EQ(pBlock, nullptr);
  EXPECT_EQ(pIdx, nullptr);
  EXPECT_EQ(listNEles(pFreeBlocks), 0);
  EXPECT_EQ(listNEles(pTargetBlocks), 0);

  tdListFree(pFreeBlocks);
  tdListFree(pTargetBlocks);
}

static void checkExternalWindowOutputAliasLifecycle(bool reset) {
  SList *pFreeBlocks = tdListNew(POINTER_BYTES * 2);
  SList *pOutputBlocks = tdListNew(POINTER_BYTES * 2);
  ASSERT_NE(pFreeBlocks, nullptr);
  ASSERT_NE(pOutputBlocks, nullptr);

  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
  SArray *pIdx = taosArrayInit(1, sizeof(int64_t));
  ASSERT_NE(pIdx, nullptr);

  void *outputBlock[2] = {pBlock, pIdx};
  ASSERT_EQ(tdListAppend(pOutputBlocks, outputBlock), TSDB_CODE_SUCCESS);

  bool aliasEstablished = false;
  bool aliasCleared = false;
  EXPECT_EQ(extWinTestOutputAliasLifecycle(pOutputBlocks, pFreeBlocks, reset, &aliasEstablished, &aliasCleared),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(aliasEstablished);
  EXPECT_TRUE(aliasCleared);
  EXPECT_EQ(listNEles(pOutputBlocks), 0);
  EXPECT_EQ(listNEles(pFreeBlocks), 1);

  SListNode *pNode = tdListPopHead(pFreeBlocks);
  ASSERT_NE(pNode, nullptr);
  blockDataDestroy(*(SSDataBlock **)pNode->data);
  taosArrayDestroy(*(SArray **)((SSDataBlock **)pNode->data + 1));
  taosMemoryFree(pNode);

  tdListFree(pOutputBlocks);
  tdListFree(pFreeBlocks);
}

TEST(execUtilTest, externalWindowNextClearsBorrowedIndexBeforeRecycle) {
  checkExternalWindowOutputAliasLifecycle(false);
}

TEST(execUtilTest, externalWindowResetClearsBorrowedIndexBeforeRecycle) {
  checkExternalWindowOutputAliasLifecycle(true);
}

TEST(execUtilTest, externalWindowLastClosedRejectsMissingIndexArray) {
  SList *pOutputBlocks = tdListNew(POINTER_BYTES * 2);
  ASSERT_NE(pOutputBlocks, nullptr);

  void *outputBlock[2] = {nullptr, nullptr};
  ASSERT_EQ(tdListAppend(pOutputBlocks, outputBlock), TSDB_CODE_SUCCESS);

  bool closed = true;
  EXPECT_EQ(extWinTestLastWinClosed(pOutputBlocks, &closed), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  EXPECT_FALSE(closed);

  tdListFree(pOutputBlocks);
}

namespace {

constexpr int64_t kExtWinFactoryInputTs = 1000;
constexpr int64_t kExtWinFactoryInputBlockId = 100;
constexpr int64_t kExtWinFactoryOutputBlockId = 101;

enum class ExternalWindowConditionKind {
  kNone,
  kTwstartLowerThan,
  kTwstartGreaterEqual,
  kTwstartNullComparison,
  kAllMetadataNotEqualMiddle,
  kQualifiedAncestorTimeRange,
};

struct ExternalWindowConditionSpec {
  ExternalWindowConditionSpec(ExternalWindowConditionKind kind = ExternalWindowConditionKind::kNone, int64_t bound = 0,
                              int32_t layerIndex = -1)
      : kind(kind), bound(bound), layerIndex(layerIndex) {}

  ExternalWindowConditionKind kind;
  int64_t                     bound;
  int32_t                     layerIndex;
};

SSTriggerCalcParam makeExternalWindowParam(int64_t start, int64_t end, int64_t duration, int64_t rowCount) {
  SSTriggerCalcParam param = {0};
  param.wstart = start;
  param.wend = end;
  param.wduration = duration;
  param.wrownum = rowCount;
  return param;
}

struct ExternalWindowInput {
  SArray               *pBlocks = nullptr;
  int32_t               nextBlock = 0;
  int32_t               getNextCalls = 0;
  int32_t               multiGroupGetNextCalls = 0;
  int32_t               nestedPlanGetNextCalls = 0;
  int32_t               contextGetNextCalls = 0;
  int32_t               resetCalls = 0;
  std::vector<bool>     resetReuseModes;
  std::vector<bool>     getNextReuseModes;
  std::vector<uint64_t> emptyReaderGroupIds;
  std::vector<int32_t>  readerCreateCountsAtReset;
  int32_t               readerCreateCalls = 0;
  int32_t               readerResetCalls = 0;
  bool                  readerCreated = false;
  int32_t               resetCode = TSDB_CODE_SUCCESS;
  int32_t               longJumpAtGetNextCall = 0;
  int32_t               longJumpCode = TSDB_CODE_SUCCESS;
};

struct ExternalWindowInputBlockSpec {
  std::vector<int64_t>     timestamps;
  std::vector<int32_t>     values;
  std::vector<bool>        valueNulls;
  std::vector<std::string> binaryValues;
  std::vector<std::string> ncharValues;
  SBlockID                 id = {0};
  STimeWindow              window = {0};
  int64_t                  version = 0;
  int16_t                  dataLoad = 1;
  uint8_t                  scanFlag = MAIN_SCAN;
  bool                     explicitWindow = false;
};

int32_t externalWindowInputNext(SOperatorInfo *pOperator, SSDataBlock **ppBlock) {
  auto *pInput = static_cast<ExternalWindowInput *>(pOperator->info);
  ++pInput->getNextCalls;
  if (pOperator->pTaskInfo != nullptr && pOperator->pTaskInfo->pStreamRuntimeInfo != nullptr) {
    pInput->getNextReuseModes.push_back(qStreamFetchReuseReader(pOperator->pTaskInfo->pStreamRuntimeInfo));
  }
  SStreamRuntimeFuncInfo *pRuntime = pOperator->pTaskInfo == nullptr ? nullptr : GET_STM_RTINFO(pOperator->pTaskInfo);
  if (pRuntime != nullptr && pRuntime->isMultiGroupCalc) {
    ++pInput->multiGroupGetNextCalls;
  }
  if (pRuntime != nullptr && BIT_FLAG_TEST_MASK(pRuntime->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN)) {
    ++pInput->nestedPlanGetNextCalls;
  }
  if (pRuntime != nullptr && (pRuntime->pContextPolicy != nullptr || pRuntime->pAncestorContext != nullptr)) {
    ++pInput->contextGetNextCalls;
  }
  if (pRuntime != nullptr) {
    const uint64_t groupId = pRuntime->groupId;
    if (std::find(pInput->emptyReaderGroupIds.begin(), pInput->emptyReaderGroupIds.end(), groupId) !=
        pInput->emptyReaderGroupIds.end()) {
      *ppBlock = nullptr;
      return TSDB_CODE_SUCCESS;
    }
  }
  if (pInput->longJumpAtGetNextCall == pInput->getNextCalls) {
    pOperator->pTaskInfo->code = pInput->longJumpCode;
    T_LONG_JMP(pOperator->pTaskInfo->env, pInput->longJumpCode);
  }
  if (pInput->nextBlock >= taosArrayGetSize(pInput->pBlocks)) {
    *ppBlock = nullptr;
    return TSDB_CODE_SUCCESS;
  }

  if (!pInput->readerCreated) {
    pInput->readerCreated = true;
    ++pInput->readerCreateCalls;
  }

  auto **ppNext = static_cast<SSDataBlock **>(taosArrayGet(pInput->pBlocks, pInput->nextBlock++));
  *ppBlock = ppNext == nullptr ? nullptr : *ppNext;
  return TSDB_CODE_SUCCESS;
}

int32_t externalWindowInputReset(SOperatorInfo *pOperator) {
  auto *pInput = static_cast<ExternalWindowInput *>(pOperator->info);
  pInput->nextBlock = 0;
  ++pInput->resetCalls;
  pInput->readerCreateCountsAtReset.push_back(pInput->readerCreateCalls);
  if (pOperator->pTaskInfo != nullptr && pOperator->pTaskInfo->pStreamRuntimeInfo != nullptr) {
    pInput->resetReuseModes.push_back(qStreamFetchReuseReader(pOperator->pTaskInfo->pStreamRuntimeInfo));
  }
  if (pInput->readerCreated) {
    ++pInput->readerResetCalls;
    const bool reuseFetchReader =
        pOperator->pTaskInfo != nullptr && qStreamFetchReuseReader(pOperator->pTaskInfo->pStreamRuntimeInfo);
    const bool reuseRequestReader = pOperator->pTaskInfo != nullptr && pOperator->pTaskInfo->reuseReaderOnReset;
    if (!reuseFetchReader && !reuseRequestReader) {
      pInput->readerCreated = false;
    }
  }
  return pInput->resetCode;
}

void destroyExternalWindowInput(void *pInfo) {
  auto *pInput = static_cast<ExternalWindowInput *>(pInfo);
  if (pInput != nullptr && pInput->pBlocks != nullptr) {
    for (int32_t i = 0; i < taosArrayGetSize(pInput->pBlocks); ++i) {
      auto **ppBlock = static_cast<SSDataBlock **>(taosArrayGet(pInput->pBlocks, i));
      if (ppBlock != nullptr) {
        blockDataDestroy(*ppBlock);
      }
    }
    taosArrayDestroy(pInput->pBlocks);
  }
  if (pInput != nullptr) {
    pInput->~ExternalWindowInput();
  }
  taosMemoryFree(pInput);
}

int32_t makeExternalWindowInputBlock(const ExternalWindowInputBlockSpec &spec, int32_t binaryBytes, int32_t ncharBytes,
                                     SSDataBlock **ppBlock) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SSDataBlock     *pBlock = nullptr;
  SColumnInfoData  tsCol = {0};
  SColumnInfoData  valueCol = {0};
  SColumnInfoData  binaryCol = {0};
  SColumnInfoData  ncharCol = {0};
  SColumnInfoData *pTsCol = nullptr;
  SColumnInfoData *pValueCol = nullptr;
  SColumnInfoData *pBinaryCol = nullptr;
  SColumnInfoData *pNcharCol = nullptr;
  const int32_t    rows = static_cast<int32_t>(spec.values.size());

  if (spec.timestamps.size() != spec.values.size() ||
      (!spec.valueNulls.empty() && spec.valueNulls.size() != spec.values.size()) ||
      (!spec.binaryValues.empty() && spec.binaryValues.size() != spec.values.size()) ||
      (!spec.ncharValues.empty() && spec.ncharValues.size() != spec.values.size()) ||
      (binaryBytes > 0 && spec.binaryValues.size() != spec.values.size()) ||
      (ncharBytes > 0 && spec.ncharValues.size() != spec.values.size())) {
    return TSDB_CODE_INVALID_PARA;
  }

  code = createDataBlock(&pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1);
  code = blockDataAppendColInfo(pBlock, &tsCol);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  valueCol = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
  code = blockDataAppendColInfo(pBlock, &valueCol);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (binaryBytes > 0) {
    binaryCol = createColumnInfoData(TSDB_DATA_TYPE_BINARY, binaryBytes + VARSTR_HEADER_SIZE, 3);
    code = blockDataAppendColInfo(pBlock, &binaryCol);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  if (ncharBytes > 0) {
    ncharCol = createColumnInfoData(TSDB_DATA_TYPE_NCHAR, ncharBytes + VARSTR_HEADER_SIZE, 4);
    code = blockDataAppendColInfo(pBlock, &ncharCol);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  code = blockDataEnsureCapacity(pBlock, rows);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pTsCol = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  pValueCol = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 1));
  pBinaryCol = binaryBytes > 0 ? static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 2)) : nullptr;
  pNcharCol = ncharBytes > 0 ? static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, binaryBytes > 0 ? 3 : 2))
                             : nullptr;
  if (pTsCol == nullptr || pValueCol == nullptr || (binaryBytes > 0 && pBinaryCol == nullptr) ||
      (ncharBytes > 0 && pNcharCol == nullptr)) {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _error;
  }

  for (int32_t row = 0; row < rows; ++row) {
    int64_t timestamp = spec.timestamps[row];
    int32_t value = spec.values[row];
    code = colDataSetVal(pTsCol, row, reinterpret_cast<const char *>(&timestamp), false);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    bool valueIsNull = !spec.valueNulls.empty() && spec.valueNulls[row];
    code = colDataSetVal(pValueCol, row, valueIsNull ? nullptr : reinterpret_cast<const char *>(&value), valueIsNull);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    if (pBinaryCol != nullptr) {
      const std::string &valueBytes = spec.binaryValues[row];
      if (valueBytes.size() > static_cast<size_t>(binaryBytes)) {
        code = TSDB_CODE_INVALID_PARA;
        goto _error;
      }
      std::vector<char> encoded(valueBytes.size() + VARSTR_HEADER_SIZE);
      varDataLen(encoded.data()) = static_cast<VarDataLenT>(valueBytes.size());
      memcpy(varDataVal(encoded.data()), valueBytes.data(), valueBytes.size());
      code = colDataSetVal(pBinaryCol, row, encoded.data(), false);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
    }
    if (pNcharCol != nullptr) {
      const std::string &valueBytes = spec.ncharValues[row];
      if (valueBytes.size() > static_cast<size_t>(ncharBytes)) {
        code = TSDB_CODE_INVALID_PARA;
        goto _error;
      }
      std::vector<char> encoded(valueBytes.size() + VARSTR_HEADER_SIZE);
      varDataLen(encoded.data()) = static_cast<VarDataLenT>(valueBytes.size());
      memcpy(varDataVal(encoded.data()), valueBytes.data(), valueBytes.size());
      code = colDataSetVal(pNcharCol, row, encoded.data(), false);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
    }
  }

  pBlock->info.rows = rows;
  pBlock->info.id = spec.id;
  pBlock->info.version = spec.version;
  pBlock->info.dataLoad = spec.dataLoad;
  pBlock->info.scanFlag = spec.scanFlag;
  code = blockDataUpdateTsWindow(pBlock, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  if (spec.explicitWindow) {
    pBlock->info.window = spec.window;
  }

  *ppBlock = pBlock;
  return TSDB_CODE_SUCCESS;

_error:
  blockDataDestroy(pBlock);
  return code;
}

int32_t makeExternalWindowInput(SExecTaskInfo *pTaskInfo, const std::vector<int32_t> &blockRows,
                                bool sequentialTimestamps, const std::vector<uint64_t> &blockGroupIds,
                                const std::vector<uint64_t> &blockCalcGroupIds, bool nullFirstValue,
                                const std::vector<int32_t> &blockFirstValues,
                                const std::vector<int32_t> &blockScanFlags, int32_t binaryBytes, int32_t ncharBytes,
                                const std::vector<ExternalWindowInputBlockSpec> &explicitBlocks,
                                SOperatorInfo                                  **ppOperator) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t firstValue = 0;
  size_t  blockCount = explicitBlocks.empty() ? blockRows.size() : explicitBlocks.size();
  auto   *pOperator = static_cast<SOperatorInfo *>(taosMemoryCalloc(1, sizeof(SOperatorInfo)));
  auto   *pInput = static_cast<ExternalWindowInput *>(taosMemoryCalloc(1, sizeof(ExternalWindowInput)));
  if (pInput != nullptr) {
    new (pInput) ExternalWindowInput();
  }
  if (pOperator == nullptr || pInput == nullptr) {
    code = terrno;
    goto _error;
  }
  if (!blockGroupIds.empty() && blockGroupIds.size() != blockRows.size()) {
    code = TSDB_CODE_INVALID_PARA;
    goto _error;
  }
  if (!blockCalcGroupIds.empty() && blockCalcGroupIds.size() != blockRows.size()) {
    code = TSDB_CODE_INVALID_PARA;
    goto _error;
  }
  if (!blockFirstValues.empty() && blockFirstValues.size() != blockRows.size()) {
    code = TSDB_CODE_INVALID_PARA;
    goto _error;
  }
  if (!blockScanFlags.empty() && blockScanFlags.size() != blockRows.size()) {
    code = TSDB_CODE_INVALID_PARA;
    goto _error;
  }

  pInput->pBlocks = taosArrayInit(blockCount, POINTER_BYTES);
  if (pInput->pBlocks == nullptr) {
    code = terrno;
    goto _error;
  }

  for (size_t block = 0; block < blockCount; ++block) {
    SSDataBlock                 *pBlock = nullptr;
    int32_t                      rows = explicitBlocks.empty() ? blockRows[block] : explicitBlocks[block].values.size();
    int32_t                      blockFirstValue = blockFirstValues.empty() ? firstValue : blockFirstValues[block];
    ExternalWindowInputBlockSpec legacy;
    const ExternalWindowInputBlockSpec *pSpec = explicitBlocks.empty() ? &legacy : &explicitBlocks[block];
    if (explicitBlocks.empty()) {
      legacy.id.uid = 7000 + block;
      legacy.id.blockId = kExtWinFactoryInputBlockId + block;
      legacy.values.reserve(rows);
      legacy.timestamps.reserve(rows);
      legacy.valueNulls.reserve(rows);
      if (binaryBytes > 0) legacy.binaryValues.reserve(rows);
      for (int32_t row = 0; row < rows; ++row) {
        legacy.values.push_back(blockFirstValue + row);
        legacy.timestamps.push_back(kExtWinFactoryInputTs + (sequentialTimestamps ? blockFirstValue + row : 0));
        legacy.valueNulls.push_back(nullFirstValue && row == 0);
        if (binaryBytes > 0) {
          std::string bytes(binaryBytes, static_cast<char>('a' + (row % 23)));
          int32_t     marker = blockFirstValue + row;
          memcpy(&bytes[0], &marker, TMIN(binaryBytes, static_cast<int32_t>(sizeof(marker))));
          legacy.binaryValues.push_back(std::move(bytes));
        }
      }
      pSpec = &legacy;
    }
    code = makeExternalWindowInputBlock(*pSpec, binaryBytes, ncharBytes, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    if (explicitBlocks.empty() && !blockGroupIds.empty()) {
      pBlock->info.id.baseGId = blockGroupIds[block];
      pBlock->info.id.groupId = blockGroupIds[block];
    }
    if (explicitBlocks.empty() && !blockCalcGroupIds.empty()) {
      pBlock->info.id.groupId = blockCalcGroupIds[block];
    }
    if (explicitBlocks.empty() && !blockScanFlags.empty()) {
      pBlock->info.scanFlag = blockScanFlags[block];
    }
    if (taosArrayPush(pInput->pBlocks, &pBlock) == nullptr) {
      blockDataDestroy(pBlock);
      code = terrno;
      goto _error;
    }
    firstValue += rows;
  }

  pOperator->info = pInput;
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, externalWindowInputNext, nullptr, destroyExternalWindowInput,
                                         optrDefaultBufFn, nullptr, optrDefaultGetNextExtFn, nullptr);
  setOperatorResetStateFn(pOperator, externalWindowInputReset);
  setOperatorInfo(pOperator, "ExternalWindowInput", QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN, false, OP_NOT_OPENED, pInput,
                  pTaskInfo);
  initOperatorCostInfo(pOperator);
  *ppOperator = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  destroyExternalWindowInput(pInput);
  taosMemoryFree(pOperator);
  return code;
}

int32_t addExternalWindowNonStreamMockSubquery(SExternalWindowPhysiNode *pNode) {
  int32_t               code = TSDB_CODE_SUCCESS;
  SRemoteTableNode     *pRemote = nullptr;
  STableScanPhysiNode  *pScan = nullptr;
  constexpr const char *kMockDb = "external_window_test_group_blocks";

  code = nodesMakeNode(QUERY_NODE_REMOTE_TABLE, reinterpret_cast<SNode **>(&pRemote));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, reinterpret_cast<SNode **>(&pScan));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  code = tNameSetDbName(&pScan->scan.tableName, 1, kMockDb, strlen(kMockDb));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  code = nodesListMakeStrictAppend(&pNode->window.node.pChildren, reinterpret_cast<SNode *>(pScan));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pScan = nullptr;
  pNode->pSubquery = reinterpret_cast<SNode *>(pRemote);
  pRemote = nullptr;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pScan));
  nodesDestroyNode(reinterpret_cast<SNode *>(pRemote));
  return code;
}

int32_t appendExternalWindowTarget(SExternalWindowPhysiNode *pNode, const char *pFunctionName, bool scalar,
                                   bool indefinite, int32_t outputSlot = 0) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SColumnNode   *pValue = nullptr;
  SFunctionNode *pFunction = nullptr;
  STargetNode   *pTarget = nullptr;
  SValueNode    *pPercentile = nullptr;

  code = nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&pValue));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pValue->node.resType.type = TSDB_DATA_TYPE_INT;
  pValue->node.resType.bytes = sizeof(int32_t);
  pValue->dataBlockId = kExtWinFactoryInputBlockId;
  pValue->slotId = 1;
  pValue->colId = 2;
  pValue->colType = COLUMN_TYPE_COLUMN;

  code = nodesMakeNode(QUERY_NODE_TARGET, reinterpret_cast<SNode **>(&pTarget));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pTarget->dataBlockId = kExtWinFactoryOutputBlockId;
  pTarget->slotId = outputSlot;

  if (scalar) {
    pTarget->pExpr = reinterpret_cast<SNode *>(pValue);
    pValue = nullptr;
    code = nodesListMakeStrictAppend(&pNode->window.pProjs, reinterpret_cast<SNode *>(pTarget));
  } else {
    code = nodesMakeNode(QUERY_NODE_FUNCTION, reinterpret_cast<SNode **>(&pFunction));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    tstrncpy(pFunction->functionName, pFunctionName, TSDB_FUNC_NAME_LEN);
    code = nodesListMakeStrictAppend(&pFunction->pParameterList, reinterpret_cast<SNode *>(pValue));
    pValue = nullptr;
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    if (strcmp(pFunctionName, "percentile") == 0) {
      int32_t percentile = 50;
      code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pPercentile));
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
      pPercentile->node.resType.type = TSDB_DATA_TYPE_INT;
      pPercentile->node.resType.bytes = sizeof(percentile);
      code = nodesSetValueNodeValue(pPercentile, &percentile);
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
      pPercentile->translate = true;
      code = nodesListMakeStrictAppend(&pFunction->pParameterList, reinterpret_cast<SNode *>(pPercentile));
      pPercentile = nullptr;
      if (code != TSDB_CODE_SUCCESS) {
        goto _error;
      }
    }
    code = fmGetFuncInfo(pFunction, nullptr, 0);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    pTarget->pExpr = reinterpret_cast<SNode *>(pFunction);
    pFunction = nullptr;
    code = nodesListMakeStrictAppend(&pNode->window.pFuncs, reinterpret_cast<SNode *>(pTarget));
    pNode->window.indefRowsFunc = indefinite;
  }
  pTarget = nullptr;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pTarget));
  nodesDestroyNode(reinterpret_cast<SNode *>(pFunction));
  nodesDestroyNode(reinterpret_cast<SNode *>(pPercentile));
  nodesDestroyNode(reinterpret_cast<SNode *>(pValue));
  return code;
}

int32_t appendExternalWindowProjectionColumn(SExternalWindowPhysiNode *pNode, int32_t inputSlot, int8_t type,
                                             int32_t bytes, int32_t outputSlot) {
  SColumnNode *pColumn = nullptr;
  STargetNode *pTarget = nullptr;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&pColumn));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pColumn->node.resType.type = type;
  pColumn->node.resType.bytes = bytes;
  pColumn->dataBlockId = kExtWinFactoryInputBlockId;
  pColumn->slotId = inputSlot;
  pColumn->colId = inputSlot + 1;
  pColumn->colType = COLUMN_TYPE_COLUMN;

  code = nodesMakeNode(QUERY_NODE_TARGET, reinterpret_cast<SNode **>(&pTarget));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pTarget->dataBlockId = kExtWinFactoryOutputBlockId;
  pTarget->slotId = outputSlot;
  pTarget->pExpr = reinterpret_cast<SNode *>(pColumn);
  pColumn = nullptr;
  code = nodesListMakeStrictAppend(&pNode->window.pProjs, reinterpret_cast<SNode *>(pTarget));
  pTarget = nullptr;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pTarget));
  nodesDestroyNode(reinterpret_cast<SNode *>(pColumn));
  return code;
}

int32_t makeExternalWindowPseudoFunction(const char *pFunctionName, int32_t layerIndex, SFunctionNode **ppFunction) {
  SFunctionNode *pFunction = nullptr;
  SValueNode    *pValue = nullptr;
  SValueNode    *pLayer = nullptr;
  int32_t        code = nodesMakeNode(QUERY_NODE_FUNCTION, reinterpret_cast<SNode **>(&pFunction));
  bool           integerResult = strcmp(pFunctionName, "_twduration") == 0 || strcmp(pFunctionName, "_twrownum") == 0;
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  tstrncpy(pFunction->functionName, pFunctionName, TSDB_FUNC_NAME_LEN);

  code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pValue));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pValue->node.resType.type = integerResult ? TSDB_DATA_TYPE_BIGINT : TSDB_DATA_TYPE_TIMESTAMP;
  pValue->node.resType.bytes = sizeof(int64_t);
  pValue->translate = true;
  code = nodesListMakeStrictAppend(&pFunction->pParameterList, reinterpret_cast<SNode *>(pValue));
  pValue = nullptr;
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (layerIndex >= 0) {
    code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pLayer));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    pLayer->node.resType.type = TSDB_DATA_TYPE_INT;
    pLayer->node.resType.bytes = sizeof(layerIndex);
    pLayer->datum.i = layerIndex;
    pLayer->translate = true;
    code = nodesListMakeStrictAppend(&pFunction->pParameterList, reinterpret_cast<SNode *>(pLayer));
    pLayer = nullptr;
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  code = fmGetFuncInfo(pFunction, nullptr, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  *ppFunction = pFunction;
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pLayer));
  nodesDestroyNode(reinterpret_cast<SNode *>(pValue));
  nodesDestroyNode(reinterpret_cast<SNode *>(pFunction));
  return code;
}

int32_t appendExternalWindowPseudoProjection(SExternalWindowPhysiNode *pNode, const char *pFunctionName,
                                             int32_t outputSlot) {
  SFunctionNode *pFunction = nullptr;
  STargetNode   *pTarget = nullptr;
  int32_t        code = makeExternalWindowPseudoFunction(pFunctionName, -1, &pFunction);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  code = nodesMakeNode(QUERY_NODE_TARGET, reinterpret_cast<SNode **>(&pTarget));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pTarget->dataBlockId = kExtWinFactoryOutputBlockId;
  pTarget->slotId = outputSlot;
  pTarget->pExpr = reinterpret_cast<SNode *>(pFunction);
  pFunction = nullptr;
  code = nodesListMakeStrictAppend(&pNode->window.pProjs, reinterpret_cast<SNode *>(pTarget));
  pTarget = nullptr;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pTarget));
  nodesDestroyNode(reinterpret_cast<SNode *>(pFunction));
  return code;
}

int32_t appendExternalWindowOutputSlot(SExternalWindowPhysiNode *pNode, int32_t slotId, int8_t type, int32_t bytes) {
  auto          *pDesc = pNode->window.node.pOutputDataBlockDesc;
  SSlotDescNode *pSlot = nullptr;
  int32_t        code = nodesMakeNode(QUERY_NODE_SLOT_DESC, reinterpret_cast<SNode **>(&pSlot));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pSlot->slotId = slotId;
  pSlot->dataType.type = type;
  pSlot->dataType.bytes = bytes;
  pSlot->output = true;
  code = nodesListMakeStrictAppend(&pDesc->pSlots, reinterpret_cast<SNode *>(pSlot));
  if (code != TSDB_CODE_SUCCESS) {
    nodesDestroyNode(reinterpret_cast<SNode *>(pSlot));
    return code;
  }
  pDesc->totalRowSize += bytes;
  pDesc->outputRowSize += bytes;
  return TSDB_CODE_SUCCESS;
}

int32_t makeExternalWindowComparison(const char *pFunctionName, EOperatorType opType, int64_t bound, bool nullBound,
                                     int32_t layerIndex, SNode **ppComparison) {
  SFunctionNode *pFunction = nullptr;
  SValueNode    *pBound = nullptr;
  SOperatorNode *pCondition = nullptr;
  int32_t        code = makeExternalWindowPseudoFunction(pFunctionName, layerIndex, &pFunction);
  bool           integerResult = strcmp(pFunctionName, "_twduration") == 0 || strcmp(pFunctionName, "_twrownum") == 0;
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pBound));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pBound->node.resType.type = integerResult ? TSDB_DATA_TYPE_BIGINT : TSDB_DATA_TYPE_TIMESTAMP;
  pBound->node.resType.bytes = sizeof(bound);
  pBound->isNull = nullBound;
  if (!nullBound) {
    code = nodesSetValueNodeValue(pBound, &bound);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }
  pBound->translate = true;

  code = nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode **>(&pCondition));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pCondition->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pCondition->node.resType.bytes = sizeof(bool);
  pCondition->opType = opType;
  pCondition->pLeft = reinterpret_cast<SNode *>(pFunction);
  pFunction = nullptr;
  pCondition->pRight = reinterpret_cast<SNode *>(pBound);
  pBound = nullptr;
  *ppComparison = reinterpret_cast<SNode *>(pCondition);
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pCondition));
  nodesDestroyNode(reinterpret_cast<SNode *>(pBound));
  nodesDestroyNode(reinterpret_cast<SNode *>(pFunction));
  return code;
}

int32_t makeExternalWindowRangeBoundary(SExternalWindowPhysiNode *pNode, const char *pFunctionName,
                                        EOperatorType opType, int32_t layerIndex, SNode **ppBoundary) {
  SNode         *pTimestamp = nullptr;
  SFunctionNode *pFunction = nullptr;
  SOperatorNode *pBoundary = nullptr;
  int32_t        code = nodesCloneNode(pNode->window.pTspk, &pTimestamp);
  if (code != TSDB_CODE_SUCCESS) goto _error;
  code = makeExternalWindowPseudoFunction(pFunctionName, layerIndex, &pFunction);
  if (code != TSDB_CODE_SUCCESS) goto _error;
  code = nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode **>(&pBoundary));
  if (code != TSDB_CODE_SUCCESS) goto _error;
  pBoundary->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pBoundary->node.resType.bytes = sizeof(bool);
  pBoundary->opType = opType;
  pBoundary->pLeft = pTimestamp;
  pTimestamp = nullptr;
  pBoundary->pRight = reinterpret_cast<SNode *>(pFunction);
  pFunction = nullptr;
  *ppBoundary = reinterpret_cast<SNode *>(pBoundary);
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pBoundary));
  nodesDestroyNode(reinterpret_cast<SNode *>(pFunction));
  nodesDestroyNode(pTimestamp);
  return code;
}

int32_t addExternalWindowQualifiedAncestorTimeRange(SExternalWindowPhysiNode *pNode, int32_t layerIndex) {
  STimeRangeNode *pRange = nullptr;
  int32_t         code = nodesMakeNode(QUERY_NODE_TIME_RANGE, reinterpret_cast<SNode **>(&pRange));
  if (code != TSDB_CODE_SUCCESS) goto _error;
  code = makeExternalWindowRangeBoundary(pNode, "_twstart", OP_TYPE_GREATER_EQUAL, layerIndex, &pRange->pStart);
  if (code != TSDB_CODE_SUCCESS) goto _error;
  code = makeExternalWindowRangeBoundary(pNode, "_twend", OP_TYPE_LOWER_EQUAL, -1, &pRange->pEnd);
  if (code != TSDB_CODE_SUCCESS) goto _error;
  pRange->needCalc = true;
  pNode->pTimeRange = reinterpret_cast<SNode *>(pRange);
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pRange));
  return code;
}

int32_t addExternalWindowCondition(SExternalWindowPhysiNode *pNode, const ExternalWindowConditionSpec &spec) {
  if (spec.kind == ExternalWindowConditionKind::kNone) {
    return TSDB_CODE_SUCCESS;
  }

  if (spec.kind != ExternalWindowConditionKind::kAllMetadataNotEqualMiddle) {
    EOperatorType opType =
        spec.kind == ExternalWindowConditionKind::kTwstartGreaterEqual ? OP_TYPE_GREATER_EQUAL : OP_TYPE_LOWER_THAN;
    bool nullBound = spec.kind == ExternalWindowConditionKind::kTwstartNullComparison;
    return makeExternalWindowComparison("_twstart", opType, spec.bound, nullBound, spec.layerIndex,
                                        &pNode->pWindowConditions);
  }

  SFunctionNode *pStart = nullptr;
  SFunctionNode *pEnd = nullptr;
  SFunctionNode *pDuration = nullptr;
  SFunctionNode *pRowCount = nullptr;
  SOperatorNode *pStartWithDuration = nullptr;
  SOperatorNode *pEndWithRowCount = nullptr;
  SOperatorNode *pCondition = nullptr;
  int32_t        code = makeExternalWindowPseudoFunction("_twstart", -1, &pStart);
  if (code != TSDB_CODE_SUCCESS) goto _error;
  code = makeExternalWindowPseudoFunction("_twend", -1, &pEnd);
  if (code != TSDB_CODE_SUCCESS) goto _error;
  code = makeExternalWindowPseudoFunction("_twduration", -1, &pDuration);
  if (code != TSDB_CODE_SUCCESS) goto _error;
  code = makeExternalWindowPseudoFunction("_twrownum", -1, &pRowCount);
  if (code != TSDB_CODE_SUCCESS) goto _error;

  code = nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode **>(&pStartWithDuration));
  if (code != TSDB_CODE_SUCCESS) goto _error;
  pStartWithDuration->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  pStartWithDuration->node.resType.bytes = sizeof(int64_t);
  pStartWithDuration->opType = OP_TYPE_ADD;
  pStartWithDuration->pLeft = reinterpret_cast<SNode *>(pStart);
  pStart = nullptr;
  pStartWithDuration->pRight = reinterpret_cast<SNode *>(pDuration);
  pDuration = nullptr;

  code = nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode **>(&pEndWithRowCount));
  if (code != TSDB_CODE_SUCCESS) goto _error;
  pEndWithRowCount->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  pEndWithRowCount->node.resType.bytes = sizeof(int64_t);
  pEndWithRowCount->opType = OP_TYPE_ADD;
  pEndWithRowCount->pLeft = reinterpret_cast<SNode *>(pEnd);
  pEnd = nullptr;
  pEndWithRowCount->pRight = reinterpret_cast<SNode *>(pRowCount);
  pRowCount = nullptr;

  code = nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode **>(&pCondition));
  if (code != TSDB_CODE_SUCCESS) goto _error;
  pCondition->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pCondition->node.resType.bytes = sizeof(bool);
  pCondition->opType = OP_TYPE_LOWER_THAN;
  pCondition->pLeft = reinterpret_cast<SNode *>(pStartWithDuration);
  pStartWithDuration = nullptr;
  pCondition->pRight = reinterpret_cast<SNode *>(pEndWithRowCount);
  pEndWithRowCount = nullptr;
  pNode->pWindowConditions = reinterpret_cast<SNode *>(pCondition);
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pCondition));
  nodesDestroyNode(reinterpret_cast<SNode *>(pEndWithRowCount));
  nodesDestroyNode(reinterpret_cast<SNode *>(pStartWithDuration));
  nodesDestroyNode(reinterpret_cast<SNode *>(pRowCount));
  nodesDestroyNode(reinterpret_cast<SNode *>(pDuration));
  nodesDestroyNode(reinterpret_cast<SNode *>(pEnd));
  nodesDestroyNode(reinterpret_cast<SNode *>(pStart));
  return code;
}

int32_t makeExternalWindowNode(ENodeType nodeType, const char *pFunctionName, bool scalar, bool indefinite,
                               bool addEmptyObserverCount, SExternalWindowPhysiNode **ppNode) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  SExternalWindowPhysiNode *pNode = nullptr;
  SColumnNode              *pTs = nullptr;
  SDataBlockDescNode       *pDesc = nullptr;
  SSlotDescNode            *pSlot = nullptr;
  bool                      intOutput = false;
  bool                      doubleOutput = false;

  code = nodesMakeNode(nodeType, reinterpret_cast<SNode **>(&pNode));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&pTs));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pTs->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  pTs->node.resType.bytes = sizeof(int64_t);
  pTs->dataBlockId = kExtWinFactoryInputBlockId;
  pTs->slotId = 0;
  pTs->colId = 1;
  pTs->colType = COLUMN_TYPE_COLUMN;
  pTs->isPk = true;
  pNode->window.pTspk = reinterpret_cast<SNode *>(pTs);
  pTs = nullptr;

  code = appendExternalWindowTarget(pNode, pFunctionName, scalar, indefinite);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  if (addEmptyObserverCount) {
    code = appendExternalWindowTarget(pNode, "count", false, false, 1);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  code = nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, reinterpret_cast<SNode **>(&pDesc));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pDesc->dataBlockId = kExtWinFactoryOutputBlockId;
  intOutput = scalar || indefinite || (pFunctionName != nullptr && strcmp(pFunctionName, "_group_const_value") == 0);
  doubleOutput = pFunctionName != nullptr && strcmp(pFunctionName, "percentile") == 0;
  pDesc->totalRowSize = (intOutput ? sizeof(int32_t) : (doubleOutput ? sizeof(double) : sizeof(int64_t))) +
                        (addEmptyObserverCount ? sizeof(int64_t) : 0);
  pDesc->outputRowSize = pDesc->totalRowSize;

  code = nodesMakeNode(QUERY_NODE_SLOT_DESC, reinterpret_cast<SNode **>(&pSlot));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pSlot->slotId = 0;
  pSlot->dataType.type =
      intOutput ? TSDB_DATA_TYPE_INT : (doubleOutput ? TSDB_DATA_TYPE_DOUBLE : TSDB_DATA_TYPE_BIGINT);
  pSlot->dataType.bytes = intOutput ? sizeof(int32_t) : (doubleOutput ? sizeof(double) : sizeof(int64_t));
  pSlot->output = true;
  code = nodesListMakeStrictAppend(&pDesc->pSlots, reinterpret_cast<SNode *>(pSlot));
  pSlot = nullptr;
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  if (addEmptyObserverCount) {
    code = nodesMakeNode(QUERY_NODE_SLOT_DESC, reinterpret_cast<SNode **>(&pSlot));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    pSlot->slotId = 1;
    pSlot->dataType.type = TSDB_DATA_TYPE_BIGINT;
    pSlot->dataType.bytes = sizeof(int64_t);
    pSlot->output = true;
    code = nodesListMakeStrictAppend(&pDesc->pSlots, reinterpret_cast<SNode *>(pSlot));
    pSlot = nullptr;
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }
  pNode->window.node.pOutputDataBlockDesc = pDesc;
  pDesc = nullptr;
  pNode->extFill.mode = FILL_MODE_NONE;
  pNode->isSingleTable = true;
  pNode->inputHasOrder = true;

  *ppNode = pNode;
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pSlot));
  nodesDestroyNode(reinterpret_cast<SNode *>(pDesc));
  nodesDestroyNode(reinterpret_cast<SNode *>(pTs));
  nodesDestroyNode(reinterpret_cast<SNode *>(pNode));
  return code;
}

int32_t addExternalWindowPositiveOutputFilter(SExternalWindowPhysiNode *pNode) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SColumnNode   *pOutput = nullptr;
  SValueNode    *pZero = nullptr;
  SOperatorNode *pCondition = nullptr;
  int32_t        zero = 0;

  code = nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&pOutput));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pOutput->node.resType.type = TSDB_DATA_TYPE_INT;
  pOutput->node.resType.bytes = sizeof(int32_t);
  pOutput->dataBlockId = kExtWinFactoryOutputBlockId;
  pOutput->slotId = 0;
  pOutput->colId = 1;
  pOutput->colType = COLUMN_TYPE_COLUMN;

  code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pZero));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pZero->node.resType.type = TSDB_DATA_TYPE_INT;
  pZero->node.resType.bytes = sizeof(int32_t);
  code = nodesSetValueNodeValue(pZero, &zero);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pZero->translate = true;

  code = nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode **>(&pCondition));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pCondition->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pCondition->node.resType.bytes = sizeof(bool);
  pCondition->opType = OP_TYPE_GREATER_THAN;
  pCondition->pLeft = reinterpret_cast<SNode *>(pOutput);
  pOutput = nullptr;
  pCondition->pRight = reinterpret_cast<SNode *>(pZero);
  pZero = nullptr;
  pNode->window.node.pConditions = reinterpret_cast<SNode *>(pCondition);
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pCondition));
  nodesDestroyNode(reinterpret_cast<SNode *>(pZero));
  nodesDestroyNode(reinterpret_cast<SNode *>(pOutput));
  return code;
}

struct ExternalWindowTwoLeafParent {
  int32_t nextLeaf = 0;
  bool    failReset = false;
};

int32_t externalWindowTwoLeafParentNext(SOperatorInfo *pOperator, SSDataBlock **ppBlock) {
  auto *pParent = static_cast<ExternalWindowTwoLeafParent *>(pOperator->info);
  *ppBlock = nullptr;
  while (pParent->nextLeaf < pOperator->numOfDownstream) {
    int32_t code =
        pOperator->pDownstream[pParent->nextLeaf]->fpSet.getNextFn(pOperator->pDownstream[pParent->nextLeaf], ppBlock);
    if (code != TSDB_CODE_SUCCESS || *ppBlock != nullptr) {
      return code;
    }
    ++pParent->nextLeaf;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t externalWindowTwoLeafParentReset(SOperatorInfo *pOperator) {
  auto *pParent = static_cast<ExternalWindowTwoLeafParent *>(pOperator->info);
  if (pParent->failReset) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pParent->nextLeaf = 0;
  return TSDB_CODE_SUCCESS;
}

void destroyExternalWindowTwoLeafParent(void *pInfo) { taosMemoryFree(pInfo); }

struct ExternalWindowFactoryOwner {
  SExecTaskInfo            *pTaskInfo = nullptr;
  SExternalWindowPhysiNode *pNode = nullptr;
  SProjectPhysiNode        *pProjectNode = nullptr;
  SHashJoinPhysiNode        staticMultiGroupJoin = {};
  bool                      functionMgtInitialized = false;
  bool                      tempEnvSaved = false;
  char                      previousTempDir[PATH_MAX] = {0};
  SDiskSpace                previousTempSpace = {0};

  ~ExternalWindowFactoryOwner() {
    qDestroyTask(pTaskInfo);
    nodesDestroyNode(reinterpret_cast<SNode *>(pProjectNode));
    nodesDestroyNode(reinterpret_cast<SNode *>(pNode));
    if (functionMgtInitialized) {
      fmFuncMgtDestroy();
    }
    if (tempEnvSaved) {
      tstrncpy(tsTempDir, previousTempDir, PATH_MAX);
      tsTempSpace = previousTempSpace;
    }
  }

  ExternalWindowInput *input() const {
    return pTaskInfo == nullptr || pTaskInfo->pRoot == nullptr || pTaskInfo->pRoot->numOfDownstream != 1
               ? nullptr
               : static_cast<ExternalWindowInput *>(pTaskInfo->pRoot->pDownstream[0]->info);
  }

  void qualifyForMultiGroupJoin() {
    staticMultiGroupJoin.node.type = QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN;
    staticMultiGroupJoin.multiGroupJoin = true;
    pNode->window.node.pParent = reinterpret_cast<SPhysiNode *>(&staticMultiGroupJoin);
  }

  int32_t init(const std::vector<int32_t> &blockRows, ENodeType nodeType, const char *pFunctionName, bool scalar,
               bool indefinite, int32_t numWindows, bool sequentialTimestamps = false,
               bool positiveOutputFilter = false, bool extWinSplit = false,
               const std::vector<uint64_t> &blockGroupIds = {}, bool nullFirstValue = false,
               const std::vector<uint64_t> &runtimeGroupIds = {}, bool needGroupSort = false,
               bool calcWithPartition = false, EFillMode fillMode = FILL_MODE_NONE,
               const std::vector<uint64_t> &blockCalcGroupIds = {}, EOPTR_EXEC_MODEL execModel = OPTR_EXEC_MODEL_STREAM,
               bool addEmptyObserverCount = false, const std::vector<int32_t> &blockFirstValues = {},
               const std::vector<int32_t> &blockScanFlags = {}, EExtWinInputMode inputMode = EXT_WIN_INPUT_WINDOW_BOUND,
               int32_t binaryBytes = 0, bool projectBinary = false, bool projectWindowMetadata = false,
               ExternalWindowConditionSpec condition = {}, const std::vector<SSTriggerCalcParam> &windowParams = {},
               bool projectAllWindowMetadata = false, int32_t ncharBytes = 0, bool projectNchar = false,
               const std::vector<ExternalWindowInputBlockSpec> &explicitBlocks = {}) {
    int32_t code = fmFuncMgtInit();
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    functionMgtInitialized = true;

    tstrncpy(previousTempDir, tsTempDir, sizeof(previousTempDir));
    previousTempSpace = tsTempSpace;
    tempEnvSaved = true;
    tstrncpy(tsTempDir, "/tmp", PATH_MAX);
    tsTempSpace.size.avail = 1024 * 1024;

    SStorageAPI storageApi = {0};
    code = doCreateTask(1, 1, 0, execModel, &storageApi, &pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pTaskInfo->window.skey = INT64_MIN;
    pTaskInfo->window.ekey = INT64_MAX;
    if (execModel == OPTR_EXEC_MODEL_STREAM) {
      pTaskInfo->pStreamRuntimeInfo =
          static_cast<SStreamRuntimeInfo *>(taosMemoryCalloc(1, sizeof(SStreamRuntimeInfo)));
      if (pTaskInfo->pStreamRuntimeInfo == nullptr) {
        return terrno;
      }
      pTaskInfo->ownStreamRtInfo = true;

      auto *pRuntime = &pTaskInfo->pStreamRuntimeInfo->funcInfo;
      if (runtimeGroupIds.empty()) {
        pRuntime->pStreamPesudoFuncVals = taosArrayInit(numWindows, sizeof(SSTriggerCalcParam));
        if (pRuntime->pStreamPesudoFuncVals == nullptr) {
          return terrno;
        }
        if (!windowParams.empty() && windowParams.size() != static_cast<size_t>(numWindows)) {
          return TSDB_CODE_INVALID_PARA;
        }
        for (int32_t window = 0; window < numWindows; ++window) {
          SSTriggerCalcParam param =
              windowParams.empty()
                  ? makeExternalWindowParam(kExtWinFactoryInputTs + window, kExtWinFactoryInputTs + window, 0, 0)
                  : windowParams[window];
          if (taosArrayPush(pRuntime->pStreamPesudoFuncVals, &param) == nullptr) {
            return terrno;
          }
        }
      } else {
        pRuntime->isMultiGroupCalc = true;
        pRuntime->pGroupCalcInfos =
            tSimpleHashInit(runtimeGroupIds.size(), taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
        if (pRuntime->pGroupCalcInfos == nullptr) {
          return terrno;
        }
        tSimpleHashSetFreeFp(pRuntime->pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
        pRuntime->pGroupCalcOrder = taosArrayInit(runtimeGroupIds.size(), sizeof(uint64_t));
        if (pRuntime->pGroupCalcOrder == nullptr) {
          return terrno;
        }

        for (uint64_t gid : runtimeGroupIds) {
          if (tSimpleHashGet(pRuntime->pGroupCalcInfos, &gid, sizeof(gid)) != nullptr) {
            continue;
          }
          SSTriggerGroupCalcInfo group = {0};
          group.pParams = taosArrayInit(numWindows, sizeof(SSTriggerCalcParam));
          if (group.pParams == nullptr) {
            return terrno;
          }
          if (!windowParams.empty() && windowParams.size() != static_cast<size_t>(numWindows)) {
            tDestroySSTriggerGroupCalcInfo(&group);
            return TSDB_CODE_INVALID_PARA;
          }
          for (int32_t window = 0; window < numWindows; ++window) {
            SSTriggerCalcParam param =
                windowParams.empty()
                    ? makeExternalWindowParam(kExtWinFactoryInputTs + window, kExtWinFactoryInputTs + window, 0, 0)
                    : windowParams[window];
            if (taosArrayPush(group.pParams, &param) == nullptr) {
              tDestroySSTriggerGroupCalcInfo(&group);
              return terrno;
            }
          }
          code = tSimpleHashPut(pRuntime->pGroupCalcInfos, &gid, sizeof(gid), &group, sizeof(group));
          if (code != TSDB_CODE_SUCCESS) {
            tDestroySSTriggerGroupCalcInfo(&group);
            return code;
          }
        }
        std::vector<uint64_t> canonicalOrder = runtimeGroupIds;
        std::sort(canonicalOrder.begin(), canonicalOrder.end());
        canonicalOrder.erase(std::unique(canonicalOrder.begin(), canonicalOrder.end()), canonicalOrder.end());
        for (uint64_t gid : canonicalOrder) {
          if (taosArrayPush(pRuntime->pGroupCalcOrder, &gid) == nullptr) {
            return terrno;
          }
        }
      }
    }

    code = makeExternalWindowNode(nodeType, pFunctionName, scalar, indefinite, addEmptyObserverCount, &pNode);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pNode->extWinSplit = extWinSplit;
    pNode->needGroupSort = needGroupSort;
    pNode->calcWithPartition = calcWithPartition;
    pNode->extFill.mode = fillMode;
    pNode->extWinInputMode = inputMode;
    int32_t nextOutputSlot = 1;
    if (projectBinary) {
      code = appendExternalWindowProjectionColumn(pNode, 2, TSDB_DATA_TYPE_BINARY, binaryBytes + VARSTR_HEADER_SIZE,
                                                  nextOutputSlot);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      code = appendExternalWindowOutputSlot(pNode, nextOutputSlot++, TSDB_DATA_TYPE_BINARY,
                                            binaryBytes + VARSTR_HEADER_SIZE);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    if (projectNchar) {
      int32_t ncharInputSlot = binaryBytes > 0 ? 3 : 2;
      code = appendExternalWindowProjectionColumn(pNode, ncharInputSlot, TSDB_DATA_TYPE_NCHAR,
                                                  ncharBytes + VARSTR_HEADER_SIZE, nextOutputSlot);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      code = appendExternalWindowOutputSlot(pNode, nextOutputSlot++, TSDB_DATA_TYPE_NCHAR,
                                            ncharBytes + VARSTR_HEADER_SIZE);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    if (projectWindowMetadata) {
      for (const char *pFunctionName : {"_twstart", "_twend"}) {
        code = appendExternalWindowPseudoProjection(pNode, pFunctionName, nextOutputSlot);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        code = appendExternalWindowOutputSlot(pNode, nextOutputSlot++, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t));
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
    }
    if (projectAllWindowMetadata) {
      struct MetadataProjection {
        const char *pFunctionName;
        int8_t      type;
      } projections[] = {{"_twstart", TSDB_DATA_TYPE_TIMESTAMP},
                         {"_twend", TSDB_DATA_TYPE_TIMESTAMP},
                         {"_twduration", TSDB_DATA_TYPE_BIGINT},
                         {"_twrownum", TSDB_DATA_TYPE_BIGINT}};
      for (const MetadataProjection &projection : projections) {
        code = appendExternalWindowPseudoProjection(pNode, projection.pFunctionName, nextOutputSlot);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
        code = appendExternalWindowOutputSlot(pNode, nextOutputSlot++, projection.type, sizeof(int64_t));
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
    }
    if (condition.kind == ExternalWindowConditionKind::kQualifiedAncestorTimeRange) {
      code = addExternalWindowQualifiedAncestorTimeRange(pNode, condition.layerIndex);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else if (condition.kind != ExternalWindowConditionKind::kNone) {
      code = addExternalWindowCondition(pNode, condition);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    if (execModel != OPTR_EXEC_MODEL_STREAM) {
      code = addExternalWindowNonStreamMockSubquery(pNode);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      code = extWinPreInitFromSubquery(reinterpret_cast<SPhysiNode *>(pNode), pTaskInfo);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    if (positiveOutputFilter) {
      code = addExternalWindowPositiveOutputFilter(pNode);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    SOperatorInfo *pDownstream = nullptr;
    code = makeExternalWindowInput(pTaskInfo, blockRows, sequentialTimestamps, blockGroupIds, blockCalcGroupIds,
                                   nullFirstValue, blockFirstValues, blockScanFlags, binaryBytes, ncharBytes,
                                   explicitBlocks, &pDownstream);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (nodeType == QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL) {
      code = createMergeAlignedExternalWindowOperator(pDownstream, reinterpret_cast<SPhysiNode *>(pNode), pTaskInfo,
                                                      &pTaskInfo->pRoot);
    } else {
      code = createExternalWindowOperator(pDownstream, reinterpret_cast<SPhysiNode *>(pNode), pTaskInfo,
                                          &pTaskInfo->pRoot);
    }
    return code;
  }

  int32_t initExplicit(const std::vector<ExternalWindowInputBlockSpec> &blocks, const char *pFunctionName, bool scalar,
                       bool indefinite, int32_t numWindows, const std::vector<uint64_t> &runtimeGroupIds = {},
                       bool needGroupSort = false, bool calcWithPartition = false, int32_t binaryBytes = 0,
                       bool projectBinary = false, int32_t ncharBytes = 0, bool projectNchar = false) {
    return init({}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, pFunctionName, scalar, indefinite, numWindows, false, false,
                false, {}, false, runtimeGroupIds, needGroupSort, calcWithPartition, FILL_MODE_NONE, {},
                OPTR_EXEC_MODEL_STREAM, false, {}, {}, EXT_WIN_INPUT_BROADCAST, binaryBytes, projectBinary, false, {},
                {}, false, ncharBytes, projectNchar, blocks);
  }
};

int32_t createRoundTripHashJoinOperator(SOperatorInfo **pDownstream, int32_t numOfDownstream,
                                        SHashJoinPhysiNode *pJoinNode, SExecTaskInfo *pTaskInfo,
                                        SOperatorInfo **ppOperator) {
  auto *pOperator = static_cast<SOperatorInfo *>(taosMemoryCalloc(1, sizeof(SOperatorInfo)));
  auto *pInfo = static_cast<ExternalWindowTwoLeafParent *>(taosMemoryCalloc(1, sizeof(ExternalWindowTwoLeafParent)));
  if (pOperator == nullptr || pInfo == nullptr) {
    taosMemoryFree(pOperator);
    taosMemoryFree(pInfo);
    return terrno;
  }

  pOperator->pPhyNode = reinterpret_cast<SPhysiNode *>(pJoinNode);
  pOperator->fpSet =
      createOperatorFpSet(optrDummyOpenFn, externalWindowTwoLeafParentNext, nullptr, destroyExternalWindowTwoLeafParent,
                          optrDefaultBufFn, nullptr, optrDefaultGetNextExtFn, nullptr);
  setOperatorInfo(pOperator, "RoundTripHashJoin", QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  int32_t code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  if (code != TSDB_CODE_SUCCESS) {
    destroyExternalWindowTwoLeafParent(pInfo);
    taosMemoryFree(pOperator);
    return code;
  }

  *ppOperator = pOperator;
  return TSDB_CODE_SUCCESS;
}

int32_t makeRoundTripRowsetDesc(SDataBlockDescNode **ppDesc) {
  SDataBlockDescNode *pDesc = nullptr;
  SSlotDescNode      *pSlot = nullptr;
  int32_t             code = nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, reinterpret_cast<SNode **>(&pDesc));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pDesc->dataBlockId = kExtWinFactoryInputBlockId;
  pDesc->totalRowSize = sizeof(int64_t) + sizeof(int32_t);
  pDesc->outputRowSize = pDesc->totalRowSize;

  code = nodesMakeNode(QUERY_NODE_SLOT_DESC, reinterpret_cast<SNode **>(&pSlot));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pSlot->slotId = 0;
  pSlot->dataType.type = TSDB_DATA_TYPE_TIMESTAMP;
  pSlot->dataType.bytes = sizeof(int64_t);
  pSlot->output = true;
  code = nodesListMakeStrictAppend(&pDesc->pSlots, reinterpret_cast<SNode *>(pSlot));
  pSlot = nullptr;
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = nodesMakeNode(QUERY_NODE_SLOT_DESC, reinterpret_cast<SNode **>(&pSlot));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pSlot->slotId = 1;
  pSlot->dataType.type = TSDB_DATA_TYPE_INT;
  pSlot->dataType.bytes = sizeof(int32_t);
  pSlot->output = true;
  code = nodesListMakeStrictAppend(&pDesc->pSlots, reinterpret_cast<SNode *>(pSlot));
  pSlot = nullptr;
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *ppDesc = pDesc;
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pSlot));
  nodesDestroyNode(reinterpret_cast<SNode *>(pDesc));
  return code;
}

int32_t makeRoundTripRowsetNode(SRowsetSourcePhysiNode **ppNode) {
  SRowsetSourcePhysiNode *pNode = nullptr;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_ROWSET_SOURCE, reinterpret_cast<SNode **>(&pNode));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = makeRoundTripRowsetDesc(&pNode->node.pOutputDataBlockDesc);
  if (code != TSDB_CODE_SUCCESS) {
    nodesDestroyNode(reinterpret_cast<SNode *>(pNode));
    return code;
  }
  pNode->hasPrimaryTs = true;
  pNode->isSortedByTs = true;
  pNode->primaryTsSlot = 0;
  *ppNode = pNode;
  return TSDB_CODE_SUCCESS;
}

struct ExternalWindowRoundTripOwner {
  SExecTaskInfo     *pTaskInfo = nullptr;
  SStreamRuntimeInfo runtime = {};
  bool               functionMgtInitialized = false;
  bool               tempEnvSaved = false;
  char               previousTempDir[PATH_MAX] = {0};
  SDiskSpace         previousTempSpace = {0};

  ~ExternalWindowRoundTripOwner() {
    qDestroyTask(pTaskInfo);
    tDestroyStRtFuncInfo(&runtime.funcInfo);
    if (functionMgtInitialized) {
      fmFuncMgtDestroy();
    }
    if (tempEnvSaved) {
      tstrncpy(tsTempDir, previousTempDir, PATH_MAX);
      tsTempSpace = previousTempSpace;
    }
  }

  SOperatorInfo *externalWindow() const {
    return pTaskInfo == nullptr || pTaskInfo->pRoot == nullptr || pTaskInfo->pRoot->numOfDownstream < 1
               ? nullptr
               : pTaskInfo->pRoot->pDownstream[0];
  }

  int32_t initRuntime(bool multiGroup) {
    SSTriggerCalcParam param = makeExternalWindowParam(kExtWinFactoryInputTs, kExtWinFactoryInputTs, 0, 0);
    if (!multiGroup) {
      runtime.funcInfo.pStreamPesudoFuncVals = taosArrayInit(1, sizeof(param));
      if (runtime.funcInfo.pStreamPesudoFuncVals == nullptr ||
          taosArrayPush(runtime.funcInfo.pStreamPesudoFuncVals, &param) == nullptr) {
        return terrno;
      }
      return TSDB_CODE_SUCCESS;
    }

    runtime.funcInfo.isMultiGroupCalc = true;
    runtime.funcInfo.pGroupCalcInfos = tSimpleHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
    if (runtime.funcInfo.pGroupCalcInfos == nullptr) {
      return terrno;
    }
    tSimpleHashSetFreeFp(runtime.funcInfo.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
    runtime.funcInfo.pGroupCalcOrder = taosArrayInit(1, sizeof(uint64_t));
    if (runtime.funcInfo.pGroupCalcOrder == nullptr) {
      return terrno;
    }

    constexpr uint64_t     kGroupId = 101;
    SSTriggerGroupCalcInfo group = {0};
    group.pParams = taosArrayInit(1, sizeof(param));
    if (group.pParams == nullptr || taosArrayPush(group.pParams, &param) == nullptr) {
      tDestroySSTriggerGroupCalcInfo(&group);
      return terrno;
    }
    int32_t code = tSimpleHashPut(runtime.funcInfo.pGroupCalcInfos, &kGroupId, sizeof(kGroupId), &group, sizeof(group));
    if (code != TSDB_CODE_SUCCESS) {
      tDestroySSTriggerGroupCalcInfo(&group);
      return code;
    }
    if (taosArrayPush(runtime.funcInfo.pGroupCalcOrder, &kGroupId) == nullptr) {
      return terrno;
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t makePlan(SSubplan **ppPlan) {
    SSubplan                 *pPlan = nullptr;
    SHashJoinPhysiNode       *pJoin = nullptr;
    SExternalWindowPhysiNode *pExternal = nullptr;
    SRowsetSourcePhysiNode   *pExternalInput = nullptr;
    SRowsetSourcePhysiNode   *pOtherInput = nullptr;
    int32_t                   code = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, reinterpret_cast<SNode **>(&pPlan));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, reinterpret_cast<SNode **>(&pJoin));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    code = makeExternalWindowNode(QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, false, &pExternal);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    code = makeRoundTripRowsetNode(&pExternalInput);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    code = makeRoundTripRowsetNode(&pOtherInput);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    code = nodesListMakeStrictAppend(&pExternal->window.node.pChildren, reinterpret_cast<SNode *>(pExternalInput));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    pExternalInput->node.pParent = reinterpret_cast<SPhysiNode *>(pExternal);
    pExternalInput = nullptr;

    code = nodesCloneNode(reinterpret_cast<SNode *>(pExternal->window.node.pOutputDataBlockDesc),
                          reinterpret_cast<SNode **>(&pJoin->node.pOutputDataBlockDesc));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    pJoin->multiGroupJoin = true;
    pJoin->timeRange = {INT64_MIN, INT64_MAX};
    code = nodesListMakeStrictAppend(&pJoin->node.pChildren, reinterpret_cast<SNode *>(pExternal));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    pExternal->window.node.pParent = reinterpret_cast<SPhysiNode *>(pJoin);
    pExternal = nullptr;
    code = nodesListMakeStrictAppend(&pJoin->node.pChildren, reinterpret_cast<SNode *>(pOtherInput));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    pOtherInput->node.pParent = reinterpret_cast<SPhysiNode *>(pJoin);
    pOtherInput = nullptr;

    pPlan->id.queryId = 1;
    pPlan->subplanType = SUBPLAN_TYPE_COMPUTE;
    pPlan->pNode = reinterpret_cast<SPhysiNode *>(pJoin);
    pJoin = nullptr;
    *ppPlan = pPlan;
    return TSDB_CODE_SUCCESS;

  _error:
    nodesDestroyNode(reinterpret_cast<SNode *>(pOtherInput));
    nodesDestroyNode(reinterpret_cast<SNode *>(pExternalInput));
    nodesDestroyNode(reinterpret_cast<SNode *>(pExternal));
    nodesDestroyNode(reinterpret_cast<SNode *>(pJoin));
    nodesDestroyNode(reinterpret_cast<SNode *>(pPlan));
    return code;
  }

  int32_t init(bool multiGroup) {
    int32_t code = fmFuncMgtInit();
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    functionMgtInitialized = true;
    tstrncpy(previousTempDir, tsTempDir, sizeof(previousTempDir));
    previousTempSpace = tsTempSpace;
    tempEnvSaved = true;
    tstrncpy(tsTempDir, "/tmp", PATH_MAX);
    tsTempSpace.size.avail = 1024 * 1024;
    code = initRuntime(multiGroup);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    SSubplan *pPlan = nullptr;
    char     *pSerialized = nullptr;
    int32_t   serializedLen = 0;
    code = makePlan(&pPlan);
    if (code == TSDB_CODE_SUCCESS) {
      code = qSubPlanToString(pPlan, &pSerialized, &serializedLen);
    }
    nodesDestroyNode(reinterpret_cast<SNode *>(pPlan));
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pSerialized);
      return code;
    }

    SReadHandle handle = {};
    handle.streamRtInfo = &runtime;
    code = qCreateStreamExecTaskInfo(reinterpret_cast<qTaskInfo_t *>(&pTaskInfo), pSerialized, &handle, nullptr, 1, 1);
    taosMemoryFree(pSerialized);
    return code;
  }
};

struct ExternalWindowTwoLeafOwner {
  ExternalWindowFactoryOwner owner;
  SOperatorInfo             *pLeafA = nullptr;
  SOperatorInfo             *pLeafB = nullptr;
  SOperatorInfo             *pParent = nullptr;

  int32_t init(int32_t firstValueA, int32_t firstValueB, int32_t numWindows = 1,
               EExtWinInputMode inputMode = EXT_WIN_INPUT_WINDOW_BOUND, bool scalar = true, bool emptyLeafA = false) {
    const std::vector<uint64_t> triggerGroups = {101};
    const std::vector<uint64_t> calcGroups = {1001};
    std::vector<int32_t>        leafABlockRows = emptyLeafA ? std::vector<int32_t>{} : std::vector<int32_t>{1};
    std::vector<int32_t>        leafAValues = emptyLeafA ? std::vector<int32_t>{} : std::vector<int32_t>{firstValueA};
    std::vector<uint64_t>       leafATriggerGroups = emptyLeafA ? std::vector<uint64_t>{} : triggerGroups;
    std::vector<uint64_t>       leafACalcGroups = emptyLeafA ? std::vector<uint64_t>{} : calcGroups;
    int32_t                     code =
        owner.init(leafABlockRows, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, scalar ? nullptr : "sum", scalar, false,
                   numWindows, false, false, true, leafATriggerGroups, false, triggerGroups, true, true, FILL_MODE_NONE,
                   leafACalcGroups, OPTR_EXEC_MODEL_STREAM, false, leafAValues, {}, inputMode);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    pLeafA = owner.pTaskInfo->pRoot;
    SOperatorInfo *pInputB = nullptr;
    code = makeExternalWindowInput(owner.pTaskInfo, {1}, false, triggerGroups, calcGroups, false, {firstValueB}, {}, 0,
                                   0, {}, &pInputB);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = createExternalWindowOperator(pInputB, reinterpret_cast<SPhysiNode *>(owner.pNode), owner.pTaskInfo, &pLeafB);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    pParent = static_cast<SOperatorInfo *>(taosMemoryCalloc(1, sizeof(SOperatorInfo)));
    auto *pParentInfo =
        static_cast<ExternalWindowTwoLeafParent *>(taosMemoryCalloc(1, sizeof(ExternalWindowTwoLeafParent)));
    if (pParent == nullptr || pParentInfo == nullptr) {
      taosMemoryFree(pParent);
      taosMemoryFree(pParentInfo);
      return terrno;
    }
    pParent->fpSet = createOperatorFpSet(optrDummyOpenFn, externalWindowTwoLeafParentNext, nullptr,
                                         destroyExternalWindowTwoLeafParent, optrDefaultBufFn, nullptr,
                                         optrDefaultGetNextExtFn, nullptr);
    setOperatorResetStateFn(pParent, externalWindowTwoLeafParentReset);
    setOperatorInfo(pParent, "ExternalWindowTwoLeafParent", QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, false, OP_NOT_OPENED,
                    pParentInfo, owner.pTaskInfo);
    SOperatorInfo *leaves[] = {pLeafA, pLeafB};
    code = appendDownstream(pParent, leaves, 2);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    owner.pTaskInfo->pRoot = pParent;
    return TSDB_CODE_SUCCESS;
  }

  void setParentResetFailure(bool fail) {
    auto *pInfo = static_cast<ExternalWindowTwoLeafParent *>(pParent->info);
    pInfo->failReset = fail;
  }

  void destroyRuntimeBeforeTask() {
    SExecTaskInfo *pTaskInfo = owner.pTaskInfo;
    if (pTaskInfo == nullptr || pTaskInfo->pStreamRuntimeInfo == nullptr) {
      return;
    }
    SStreamRuntimeInfo *pRuntime = pTaskInfo->pStreamRuntimeInfo;
    pTaskInfo->pStreamRuntimeInfo = nullptr;
    pTaskInfo->ownStreamRtInfo = false;
    pRuntime->funcInfo.pStreamBlkWinIdx = nullptr;
    pRuntime->funcInfo.pStreamPesudoFuncVals = nullptr;
    pRuntime->funcInfo.pStreamPartColVals = nullptr;
    pRuntime->funcInfo.curGrpCalc = nullptr;
    pRuntime->funcInfo.groupId = 0;
    pRuntime->funcInfo.createTable = nullptr;
    pRuntime->funcInfo.rollupTbCount = 0;
    tClearStreamCurrentGroupRead(&pRuntime->funcInfo);
    tDestroyStRtFuncInfo(&pRuntime->funcInfo);
    taosMemoryFree(pRuntime);
  }
};

template <typename T>
int32_t drainExternalWindow(ExternalWindowFactoryOwner *pOwner, std::vector<T> *pValues,
                            std::vector<int64_t>  *pCapacities = nullptr,
                            std::vector<uint64_t> *pOutputGroupIds = nullptr) {
  bool finished = false;
  pValues->clear();
  if (pCapacities != nullptr) {
    pCapacities->clear();
  }
  if (pOutputGroupIds != nullptr) {
    pOutputGroupIds->clear();
  }

  for (int32_t attempt = 0; attempt < 128 && !finished; ++attempt) {
    SSDataBlock *pBlock = nullptr;
    int32_t      code = streamExecuteTask(pOwner->pTaskInfo, &pBlock, &finished);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (pBlock == nullptr) {
      continue;
    }
    if (qIsMultiGroupEogBlock(pBlock)) {
      continue;
    }
    if (finished) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    if (pBlock->pDataBlock == nullptr || pBlock->info.rows <= 0 || pBlock->info.rows > pBlock->info.capacity) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    SArray *pIdx = pOwner->pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx;
    int32_t idxSize = pIdx == nullptr ? 0 : taosArrayGetSize(pIdx);
    if (idxSize <= 0 || idxSize > pBlock->info.rows) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    int32_t previousRow = -1;
    for (int32_t idx = 0; idx < idxSize; ++idx) {
      int64_t value = *static_cast<int64_t *>(taosArrayGet(pIdx, idx));
      auto   *pWinIdx = reinterpret_cast<int32_t *>(&value);
      int32_t row = *(pWinIdx + 1);
      if (*pWinIdx < 0 || row <= previousRow || row >= pBlock->info.rows || (idx == 0 && row != 0)) {
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }
      previousRow = row;
    }
    if (pCapacities != nullptr) {
      pCapacities->push_back(pBlock->info.capacity);
    }
    if (pOutputGroupIds != nullptr) {
      pOutputGroupIds->push_back(pBlock->info.id.groupId);
    }
    auto *pCol = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
    if (pCol == nullptr) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    for (int32_t row = 0; row < pBlock->info.rows; ++row) {
      if (colDataIsNull_s(pCol, row)) {
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }
      pValues->push_back(*reinterpret_cast<T *>(colDataGetData(pCol, row)));
    }
  }

  return finished ? TSDB_CODE_SUCCESS : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
}

using ExternalWindowOutputRow = std::tuple<uint64_t, int32_t, bool, int64_t>;
using ExternalWindowOutputRows = std::vector<ExternalWindowOutputRow>;

struct ExternalWindowPublication {
  uint64_t                 gid = 0;
  uint64_t                 blockGid = 0;
  uint64_t                 blockBaseGid = 0;
  int64_t                  rows = 0;
  int64_t                  nullRows = 0;
  int8_t                   createTable = 0;
  int8_t                   dataLoad = 0;
  int64_t                  version = 0;
  std::vector<int64_t>     values;
  std::vector<int64_t>     observerValues;
  std::vector<int32_t>     windowIndexes;
  ExternalWindowOutputRows rowTuples;
};

struct ExternalWindowRuntimeAliases {
  SArray                   *pWindowIndexes = nullptr;
  SSTriggerGroupCalcInfo   *pCalc = nullptr;
  SArray                   *pParams = nullptr;
  SArray                   *pPartCols = nullptr;
  int8_t                   *pCreateTable = nullptr;
  int64_t                   groupId = 0;
  int32_t                   rollupTbCount = 0;
  SArray                   *pGroupRead = nullptr;
  EStreamGroupReadOwnership groupReadOwnership = STREAM_GRP_READ_BORROWED;
};

ExternalWindowRuntimeAliases captureExternalWindowRuntimeAliases(const SStreamRuntimeFuncInfo *pRuntime) {
  ExternalWindowRuntimeAliases aliases;
  aliases.pWindowIndexes = pRuntime->pStreamBlkWinIdx;
  aliases.pCalc = pRuntime->curGrpCalc;
  aliases.pParams = pRuntime->pStreamPesudoFuncVals;
  aliases.pPartCols = pRuntime->pStreamPartColVals;
  aliases.pCreateTable = pRuntime->createTable;
  aliases.groupId = pRuntime->groupId;
  aliases.rollupTbCount = pRuntime->rollupTbCount;
  aliases.pGroupRead = pRuntime->curGrpRead;
  aliases.groupReadOwnership = pRuntime->curGrpReadOwnership;
  return aliases;
}

struct ExtWinProtocolItem {
  uint64_t groupId;
  uint64_t baseGId;
  int32_t  rows;
  int16_t  dataLoad;
  bool     eog;
};

static std::vector<ExtWinProtocolItem> drainExtWinProtocol(SOperatorInfo *op) {
  std::vector<ExtWinProtocolItem> out;
  for (;;) {
    SSDataBlock *block = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, op->fpSet.getNextFn(op, &block));
    if (block == nullptr) break;
    out.push_back({block->info.id.groupId, block->info.id.baseGId, static_cast<int32_t>(block->info.rows),
                   block->info.dataLoad, qIsMultiGroupEogBlock(block)});
  }
  return out;
}

static void expectExtWinProtocol(const std::vector<ExtWinProtocolItem> &actual,
                                 const std::vector<ExtWinProtocolItem> &expected) {
  ASSERT_EQ(actual.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_EQ(actual[i].groupId, expected[i].groupId);
    EXPECT_EQ(actual[i].baseGId, expected[i].baseGId);
    EXPECT_EQ(actual[i].rows, expected[i].rows);
    EXPECT_EQ(actual[i].dataLoad, expected[i].dataLoad);
    EXPECT_EQ(actual[i].eog, expected[i].eog);
    if (actual[i].eog) {
      EXPECT_EQ(actual[i].rows, 0);
      EXPECT_EQ(actual[i].dataLoad, 1);
      EXPECT_NE(actual[i].baseGId, 0);
      EXPECT_EQ(actual[i].groupId, actual[i].baseGId);
    } else {
      EXPECT_GT(actual[i].rows, 0);
      EXPECT_NE(actual[i].baseGId, 0);
      EXPECT_EQ(actual[i].groupId, actual[i].baseGId);
    }
  }
}

void expectExternalWindowRuntimeAliasesEmpty(const SStreamRuntimeFuncInfo *pRuntime) {
  EXPECT_EQ(pRuntime->pStreamBlkWinIdx, nullptr);
  EXPECT_EQ(pRuntime->curGrpCalc, nullptr);
  EXPECT_EQ(pRuntime->pStreamPesudoFuncVals, nullptr);
  EXPECT_EQ(pRuntime->pStreamPartColVals, nullptr);
  EXPECT_EQ(pRuntime->createTable, nullptr);
  EXPECT_EQ(pRuntime->groupId, 0);
  EXPECT_EQ(pRuntime->rollupTbCount, 0);
  EXPECT_EQ(pRuntime->curGrpRead, nullptr);
  EXPECT_EQ(pRuntime->curGrpReadOwnership, STREAM_GRP_READ_BORROWED);
}

void expectExternalWindowRuntimeAliasesEqual(const SStreamRuntimeFuncInfo       *pRuntime,
                                             const ExternalWindowRuntimeAliases &expected) {
  EXPECT_EQ(pRuntime->pStreamBlkWinIdx, expected.pWindowIndexes);
  EXPECT_EQ(pRuntime->curGrpCalc, expected.pCalc);
  EXPECT_EQ(pRuntime->pStreamPesudoFuncVals, expected.pParams);
  EXPECT_EQ(pRuntime->pStreamPartColVals, expected.pPartCols);
  EXPECT_EQ(pRuntime->createTable, expected.pCreateTable);
  EXPECT_EQ(pRuntime->groupId, expected.groupId);
  EXPECT_EQ(pRuntime->rollupTbCount, expected.rollupTbCount);
  EXPECT_EQ(pRuntime->curGrpRead, expected.pGroupRead);
  EXPECT_EQ(pRuntime->curGrpReadOwnership, expected.groupReadOwnership);
}

void expectRunnerContextsNull(const SSHashObj *pGroupCalcInfos) {
  ASSERT_NE(pGroupCalcInfos, nullptr);
  int32_t iter = 0;
  auto   *pGroup = static_cast<SSTriggerGroupCalcInfo *>(
      tSimpleHashIterate(const_cast<SSHashObj *>(pGroupCalcInfos), nullptr, &iter));
  while (pGroup != nullptr) {
    EXPECT_EQ(pGroup->pRunnerGrpCtx, nullptr);
    pGroup = static_cast<SSTriggerGroupCalcInfo *>(
        tSimpleHashIterate(const_cast<SSHashObj *>(pGroupCalcInfos), pGroup, &iter));
  }
}

std::vector<ExternalWindowPublication> drainLeaf(SOperatorInfo *pLeaf) {
  std::vector<ExternalWindowPublication> publications;
  for (int32_t call = 0; call < 128; ++call) {
    SSDataBlock *pBlock = nullptr;
    int32_t      code = pLeaf->fpSet.getNextFn(pLeaf, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      ADD_FAILURE() << "external-window leaf failed: " << code;
      return publications;
    }
    if (pBlock == nullptr) {
      return publications;
    }
    if (qIsMultiGroupEogBlock(pBlock)) {
      continue;
    }

    ExternalWindowPublication publication;
    publication.gid = pLeaf->pTaskInfo->pStreamRuntimeInfo->funcInfo.groupId;
    publication.blockGid = pBlock->info.id.groupId;
    publication.rows = pBlock->info.rows;
    if (pBlock->info.rows > 0) {
      auto *pCol = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
      if (pCol == nullptr) {
        ADD_FAILURE() << "external-window output has no first column";
        return publications;
      }
      for (int32_t row = 0; row < pBlock->info.rows; ++row) {
        if (colDataIsNull_s(pCol, row)) {
          ++publication.nullRows;
        } else if (pCol->info.type == TSDB_DATA_TYPE_INT) {
          publication.values.push_back(*reinterpret_cast<int32_t *>(colDataGetData(pCol, row)));
        } else if (pCol->info.type == TSDB_DATA_TYPE_BIGINT) {
          publication.values.push_back(*reinterpret_cast<int64_t *>(colDataGetData(pCol, row)));
        } else {
          ADD_FAILURE() << "unexpected output type: " << static_cast<int32_t>(pCol->info.type);
          return publications;
        }
      }
    }
    publications.push_back(std::move(publication));
  }
  ADD_FAILURE() << "external-window leaf did not finish";
  return publications;
}

int32_t drainExternalWindowPublications(ExternalWindowFactoryOwner             *pOwner,
                                        std::vector<ExternalWindowPublication> *pPublications,
                                        int32_t                                 observerSlot = -1) {
  bool finished = false;
  pPublications->clear();

  for (int32_t call = 0; !finished && call < 4096; ++call) {
    SSDataBlock *pBlock = nullptr;
    int32_t      code = streamExecuteTask(pOwner->pTaskInfo, &pBlock, &finished);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (pBlock == nullptr) {
      continue;
    }
    if (finished) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    SStreamRuntimeFuncInfo *pRuntime = &pOwner->pTaskInfo->pStreamRuntimeInfo->funcInfo;
    if (pRuntime->curGrpCalc == nullptr || pRuntime->groupId == 0 || pRuntime->pGroupCalcInfos == nullptr ||
        pRuntime->curGrpCalc !=
            tSimpleHashGet(pRuntime->pGroupCalcInfos, &pRuntime->groupId, sizeof(pRuntime->groupId))) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    const bool isEog = qIsMultiGroupEogBlock(pBlock);
    if (isEog && !pPublications->empty() && pPublications->back().gid == pBlock->info.id.baseGId) {
      continue;
    }

    ExternalWindowPublication publication;
    publication.gid = static_cast<uint64_t>(pRuntime->groupId);
    publication.blockGid = pBlock->info.id.groupId;
    publication.blockBaseGid = pBlock->info.id.baseGId;
    if (pBlock->info.id.baseGId != 0 && pBlock->info.id.baseGId != publication.gid) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    publication.rows = pBlock->info.rows;
    publication.createTable = pRuntime->curGrpCalc->createTable;
    publication.dataLoad = pBlock->info.dataLoad;
    publication.version = pBlock->info.version;
    if (publication.rows > 0) {
      SArray *pWindowIndexes = pRuntime->pStreamBlkWinIdx;
      if (pWindowIndexes == nullptr) {
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }
      std::vector<int32_t> rowWindowIndexes(publication.rows, -1);
      for (int32_t idx = 0; idx < taosArrayGetSize(pWindowIndexes); ++idx) {
        int64_t encoded = *static_cast<int64_t *>(taosArrayGet(pWindowIndexes, idx));
        auto   *pWinIdx = reinterpret_cast<int32_t *>(&encoded);
        int32_t startRow = *(pWinIdx + 1);
        int32_t endRow = idx + 1 < taosArrayGetSize(pWindowIndexes)
                             ? *(reinterpret_cast<int32_t *>(taosArrayGet(pWindowIndexes, idx + 1)) + 1)
                             : publication.rows;
        if (*pWinIdx < 0 || startRow < 0 || startRow >= endRow || endRow > publication.rows) {
          return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        }
        publication.windowIndexes.push_back(*pWinIdx);
        std::fill(rowWindowIndexes.begin() + startRow, rowWindowIndexes.begin() + endRow, *pWinIdx);
      }

      auto *pCol = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
      if (pCol == nullptr) {
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }
      for (int32_t row = 0; row < publication.rows; ++row) {
        bool    isNull = colDataIsNull_s(pCol, row);
        int64_t value = 0;
        if (isNull) {
          ++publication.nullRows;
        } else if (pCol->info.type == TSDB_DATA_TYPE_BIGINT) {
          value = *reinterpret_cast<int64_t *>(colDataGetData(pCol, row));
        } else if (pCol->info.type == TSDB_DATA_TYPE_INT) {
          value = *reinterpret_cast<int32_t *>(colDataGetData(pCol, row));
        } else {
          return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        }
        if (rowWindowIndexes[row] < 0) {
          return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        }
        publication.values.push_back(value);
        publication.rowTuples.emplace_back(publication.blockGid, rowWindowIndexes[row], isNull, value);
      }
      if (observerSlot >= 0) {
        auto *pObserverCol = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, observerSlot));
        if (pObserverCol == nullptr || pObserverCol->info.type != TSDB_DATA_TYPE_BIGINT ||
            pObserverCol->info.bytes != sizeof(int64_t)) {
          return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        }
        for (int32_t row = 0; row < publication.rows; ++row) {
          publication.observerValues.push_back(colDataIsNull_s(pObserverCol, row)
                                                   ? INT64_MIN
                                                   : *reinterpret_cast<int64_t *>(colDataGetData(pObserverCol, row)));
        }
      }
    }
    pPublications->push_back(publication);
  }

  return finished ? TSDB_CODE_SUCCESS : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
}

std::vector<uint64_t> externalWindowPublicationRuns(const std::vector<ExternalWindowPublication> &publications) {
  std::vector<uint64_t> runs;
  for (const ExternalWindowPublication &publication : publications) {
    if (runs.empty() || runs.back() != publication.gid) {
      runs.push_back(publication.gid);
    }
  }
  return runs;
}

void expectExternalWindowGroupsPublishedOnce(const std::vector<ExternalWindowPublication> &publications,
                                             std::vector<uint64_t>                         expected) {
  std::vector<uint64_t> runs = externalWindowPublicationRuns(publications);
  std::sort(runs.begin(), runs.end());
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(runs, expected);

  for (const ExternalWindowPublication &publication : publications) {
    EXPECT_EQ(publication.blockGid, publication.gid);
  }
}

struct ExternalWindowBroadcastRow {
  SBlockID    id = {0};
  int32_t     windowIndex = -1;
  bool        valueIsNull = false;
  int32_t     value = 0;
  std::string binary;
  std::string nchar;
  int64_t     windowStart = 0;
  int64_t     windowEnd = 0;
  int64_t     windowDuration = 0;
  int64_t     windowRowCount = 0;
};

int32_t drainExternalWindowBroadcast(ExternalWindowFactoryOwner *pOwner, int32_t binarySlot, int32_t windowStartSlot,
                                     int32_t windowEndSlot, std::vector<ExternalWindowBroadcastRow> *pRows,
                                     int32_t windowDurationSlot = -1, int32_t windowRowCountSlot = -1,
                                     int32_t ncharSlot = -1) {
  bool finished = false;
  pRows->clear();
  for (int32_t call = 0; !finished && call < 512; ++call) {
    SSDataBlock *pBlock = nullptr;
    int32_t      code = streamExecuteTask(pOwner->pTaskInfo, &pBlock, &finished);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (pBlock == nullptr) {
      continue;
    }
    if (qIsMultiGroupEogBlock(pBlock)) {
      continue;
    }
    if (finished || pBlock->info.rows <= 0) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    SArray *pWindowIndexes = pOwner->pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx;
    if (pWindowIndexes == nullptr || taosArrayGetSize(pWindowIndexes) <= 0) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    std::vector<int32_t> rowWindowIndexes(pBlock->info.rows, -1);
    for (int32_t idx = 0; idx < taosArrayGetSize(pWindowIndexes); ++idx) {
      int64_t encoded = *static_cast<int64_t *>(taosArrayGet(pWindowIndexes, idx));
      auto   *pWinIdx = reinterpret_cast<int32_t *>(&encoded);
      int32_t startRow = *(pWinIdx + 1);
      int32_t endRow = idx + 1 < taosArrayGetSize(pWindowIndexes)
                           ? *(reinterpret_cast<int32_t *>(taosArrayGet(pWindowIndexes, idx + 1)) + 1)
                           : pBlock->info.rows;
      if (*pWinIdx < 0 || startRow < 0 || startRow >= endRow || endRow > pBlock->info.rows) {
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }
      std::fill(rowWindowIndexes.begin() + startRow, rowWindowIndexes.begin() + endRow, *pWinIdx);
    }

    auto *pValueCol = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
    auto *pBinaryCol =
        binarySlot >= 0 ? static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, binarySlot)) : nullptr;
    auto *pNcharCol =
        ncharSlot >= 0 ? static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, ncharSlot)) : nullptr;
    auto *pStartCol = windowStartSlot >= 0
                          ? static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, windowStartSlot))
                          : nullptr;
    auto *pEndCol =
        windowEndSlot >= 0 ? static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, windowEndSlot)) : nullptr;
    auto *pDurationCol = windowDurationSlot >= 0
                             ? static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, windowDurationSlot))
                             : nullptr;
    auto *pRowCountCol = windowRowCountSlot >= 0
                             ? static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, windowRowCountSlot))
                             : nullptr;
    if (pValueCol == nullptr || (binarySlot >= 0 && pBinaryCol == nullptr) ||
        (ncharSlot >= 0 && pNcharCol == nullptr) || (windowStartSlot >= 0 && pStartCol == nullptr) ||
        (windowEndSlot >= 0 && pEndCol == nullptr) || (windowDurationSlot >= 0 && pDurationCol == nullptr) ||
        (windowRowCountSlot >= 0 && pRowCountCol == nullptr)) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    for (int32_t row = 0; row < pBlock->info.rows; ++row) {
      ExternalWindowBroadcastRow output;
      output.id = pBlock->info.id;
      output.windowIndex = rowWindowIndexes[row];
      if (output.windowIndex < 0) {
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }
      output.valueIsNull = colDataIsNull_s(pValueCol, row);
      if (!output.valueIsNull) {
        output.value = *reinterpret_cast<int32_t *>(colDataGetData(pValueCol, row));
      }
      if (pBinaryCol != nullptr) {
        const char *pVar = colDataGetVarData(pBinaryCol, row);
        output.binary.assign(varDataVal(pVar), varDataLen(pVar));
      }
      if (pNcharCol != nullptr) {
        const char *pVar = colDataGetVarData(pNcharCol, row);
        output.nchar.assign(varDataVal(pVar), varDataLen(pVar));
      }
      if (pStartCol != nullptr) {
        output.windowStart = *reinterpret_cast<int64_t *>(colDataGetData(pStartCol, row));
      }
      if (pEndCol != nullptr) {
        output.windowEnd = *reinterpret_cast<int64_t *>(colDataGetData(pEndCol, row));
      }
      if (pDurationCol != nullptr) {
        output.windowDuration = *reinterpret_cast<int64_t *>(colDataGetData(pDurationCol, row));
      }
      if (pRowCountCol != nullptr) {
        output.windowRowCount = *reinterpret_cast<int64_t *>(colDataGetData(pRowCountCol, row));
      }
      pRows->push_back(std::move(output));
    }
  }
  return finished ? TSDB_CODE_SUCCESS : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
}

constexpr int32_t kExternalWindowCreateError = TSDB_CODE_APP_ERROR;
constexpr int32_t kExternalWindowAppendError = TSDB_CODE_INVALID_PARA;
constexpr int32_t kExternalWindowReplayError = TSDB_CODE_OUT_OF_RANGE;

int32_t failExternalWindowPoolCreate(int32_t, int64_t, const char *, SWindowInputPool **) {
  return kExternalWindowCreateError;
}

int32_t failExternalWindowStoreCreate(const SSDataBlock *, SWindowInputPool *, SWindowInputStore **) {
  return kExternalWindowCreateError;
}

int32_t failExternalWindowStoreAppend(SWindowInputStore *, SSDataBlock *) { return kExternalWindowAppendError; }

int32_t failExternalWindowStoreReplay(SWindowInputStore *, int32_t, SSDataBlock **) {
  return kExternalWindowReplayError;
}

int32_t failExternalWindowFreePageListAppend(SList *, const void *) {
  ++gExternalWindowFreePageListAppendCalls;
  return TSDB_CODE_OUT_OF_MEMORY;
}

bool failExternalWindowTempSpace() { return false; }

int32_t spyExternalWindowPoolCreate(int32_t pageSize, int64_t inMemBufSize, const char *pId,
                                    SWindowInputPool **ppPool) {
  ++gExternalWindowCreateSpy.calls;
  gExternalWindowCreateSpy.pageSize = pageSize;
  gExternalWindowCreateSpy.inMemBufSize = inMemBufSize;
  gExternalWindowActiveStub->reset(winInputPoolCreate);
  int32_t code = winInputPoolCreate(pageSize, inMemBufSize, pId, ppPool);
  gExternalWindowActiveStub->set(winInputPoolCreate, spyExternalWindowPoolCreate);
  return code;
}

void spyExternalWindowPoolDestroy(SWindowInputPool *pPool) {
  ++gExternalWindowPoolDestroyCalls;
  gExternalWindowActiveStub->reset(winInputPoolDestroy);
  winInputPoolDestroy(pPool);
  gExternalWindowActiveStub->set(winInputPoolDestroy, spyExternalWindowPoolDestroy);
}

int32_t changeSecondExternalWindowPageSize(int32_t rowSize, uint32_t *pPageSize, int64_t *pBufferSize) {
  ++gExternalWindowBufferPageSizeCalls;
  gExternalWindowActiveStub->reset(getBufferPgSize);
  int32_t code = getBufferPgSize(rowSize, pPageSize, pBufferSize);
  gExternalWindowActiveStub->set(getBufferPgSize, changeSecondExternalWindowPageSize);
  if (code == TSDB_CODE_SUCCESS && gExternalWindowBufferPageSizeCalls == 2) {
    *pPageSize *= 2;
  }
  return code;
}

int32_t failLaterExternalWindowStoreAppend(SWindowInputStore *pStore, SSDataBlock *pBlock) {
  ++gExternalWindowAppendSpy.calls;
  if (gExternalWindowAppendSpy.calls == gExternalWindowAppendSpy.failOnCall) {
    return kExternalWindowAppendError;
  }
  gExternalWindowActiveStub->reset(winInputStoreAppendBlock);
  int32_t code = winInputStoreAppendBlock(pStore, pBlock);
  gExternalWindowActiveStub->set(winInputStoreAppendBlock, failLaterExternalWindowStoreAppend);
  return code;
}

int32_t failLaterExternalWindowStoreGet(SWindowInputStore *pStore, int32_t pageIndex, SSDataBlock **ppBlock) {
  ++gExternalWindowGetSpy.calls;
  gExternalWindowGetSpy.pageIndexes.push_back(pageIndex);
  if (gExternalWindowGetSpy.calls == gExternalWindowGetSpy.failOnCall) {
    return kExternalWindowReplayError;
  }
  gExternalWindowActiveStub->reset(winInputStoreGetBlock);
  int32_t code = winInputStoreGetBlock(pStore, pageIndex, ppBlock);
  gExternalWindowActiveStub->set(winInputStoreGetBlock, failLaterExternalWindowStoreGet);
  return code;
}

void *countExternalWindowHashIterate(const SSHashObj *pHashObj, void *pData, int32_t *pIter) {
  ++gExternalWindowHashIterateCalls;
  gExternalWindowActiveStub->reset(tSimpleHashIterate);
  void *pResult = tSimpleHashIterate(pHashObj, pData, pIter);
  gExternalWindowActiveStub->set(tSimpleHashIterate, countExternalWindowHashIterate);
  return pResult;
}

int32_t countExternalWindowGroupProjection(const SStreamContextPolicy *pPolicy, const SStreamAncestorContext *pContext,
                                           int64_t gid, SStreamContextPolicy **ppPolicy,
                                           SStreamAncestorContext **ppContext) {
  ++gExternalWindowGroupProjectionCalls;
  gExternalWindowActiveStub->reset(tProjectStreamCalcContextForGroup);
  int32_t code = tProjectStreamCalcContextForGroup(pPolicy, pContext, gid, ppPolicy, ppContext);
  gExternalWindowActiveStub->set(tProjectStreamCalcContextForGroup, countExternalWindowGroupProjection);
  return code;
}

int32_t failExternalWindowResetExprSupp(SExprSupp *, SExecTaskInfo *, SNodeList *, SNodeList *, SFunctionStateStore *) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

int32_t failExternalWindowResetAggSup(SExprSupp *, SAggSupporter *, SExecTaskInfo *, SNodeList *, SNodeList *, size_t,
                                      const char *, void *, SFunctionStateStore *) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

ExternalWindowInputBlockSpec makeExternalWindowBlockSpec(std::vector<int32_t> values, SBlockID id,
                                                         int64_t firstTimestamp, STimeWindow window, int64_t version,
                                                         int16_t dataLoad, uint8_t scanFlag,
                                                         std::vector<bool>        nulls = {},
                                                         std::vector<std::string> binaryValues = {},
                                                         std::vector<std::string> ncharValues = {}) {
  ExternalWindowInputBlockSpec spec;
  spec.values = std::move(values);
  spec.valueNulls = std::move(nulls);
  spec.binaryValues = std::move(binaryValues);
  spec.ncharValues = std::move(ncharValues);
  spec.id = id;
  spec.window = window;
  spec.version = version;
  spec.dataLoad = dataLoad;
  spec.scanFlag = scanFlag;
  spec.explicitWindow = true;
  for (size_t row = 0; row < spec.values.size(); ++row) {
    spec.timestamps.push_back(firstTimestamp + row);
  }
  return spec;
}

}  // namespace

class ExternalWindowOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    gExternalWindowActiveStub = &stub;
    gExternalWindowCreateSpy = {};
    gExternalWindowAppendSpy = {};
    gExternalWindowGetSpy = {};
    gExternalWindowHashIterateCalls = 0;
    gExternalWindowGroupProjectionCalls = 0;
    gExternalWindowPoolDestroyCalls = 0;
    gExternalWindowBufferPageSizeCalls = 0;
    gExternalWindowFreePageListAppendCalls = 0;
  }

  void TearDown() override {
    stub.reset(winInputPoolCreate);
    stub.reset(winInputPoolDestroy);
    stub.reset(winInputStoreCreate);
    stub.reset(winInputStoreAppendBlock);
    stub.reset(winInputStoreGetBlock);
    stub.reset(osTempSpaceAvailable);
    stub.reset(resetExprSupp);
    stub.reset(resetAggSup);
    stub.reset(tSimpleHashIterate);
    stub.reset(tProjectStreamCalcContextForGroup);
    stub.reset(getBufferPgSize);
    stub.reset(tdListAppend);
    stub.reset(createHashJoinOperatorInfo);
    gExternalWindowActiveStub = nullptr;
  }

  Stub stub;
};

TEST_F(ExternalWindowOperatorTest, MultiGroupOutputUsesCanonicalOrderAcrossRegistries) {
  const std::vector<uint64_t> registryOrder = {303, 101, 202};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 1, false, false, false,
                       {303, 101}, false, registryOrder),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();

  const auto protocol = drainExtWinProtocol(owner.pTaskInfo->pRoot);
  expectExtWinProtocol(protocol, {{101, 101, 1, 1, false},
                                  {101, 101, 0, 1, true},
                                  {202, 202, 0, 1, true},
                                  {303, 303, 1, 1, false},
                                  {303, 303, 0, 1, true}});
}

TEST_F(ExternalWindowOperatorTest, EmptyFirstAndMiddleGroupsEmitExactlyOneEog) {
  const std::vector<uint64_t> registryOrder = {303, 101, 202};
  for (ENodeType nodeType : {QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL}) {
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({1}, nodeType, "sum", false, false, 1, false, false, false, {303}, false, registryOrder),
              TSDB_CODE_SUCCESS);
    owner.qualifyForMultiGroupJoin();

    const auto protocol = drainExtWinProtocol(owner.pTaskInfo->pRoot);
    expectExtWinProtocol(
        protocol, {{101, 101, 0, 1, true}, {202, 202, 0, 1, true}, {303, 303, 1, 1, false}, {303, 303, 0, 1, true}});
  }
}

TEST_F(ExternalWindowOperatorTest, SingletonGroupEmitsNonzeroIdentityAndEog) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 1, false, false, false, {101},
                       false, {101}),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();

  const auto protocol = drainExtWinProtocol(owner.pTaskInfo->pRoot);
  expectExtWinProtocol(protocol, {{101, 101, 1, 1, false}, {101, 101, 0, 1, true}});
}

TEST_F(ExternalWindowOperatorTest, AggregateModeEmitsEogAfterData) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 1, false, false, false, {101},
                       false, {101}),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  expectExtWinProtocol(drainExtWinProtocol(owner.pTaskInfo->pRoot), {{101, 101, 1, 1, false}, {101, 101, 0, 1, true}});
}

TEST_F(ExternalWindowOperatorTest, NonAggregateModeEmitsEogAfterData) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {101},
                       false, {101}),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  expectExtWinProtocol(drainExtWinProtocol(owner.pTaskInfo->pRoot), {{101, 101, 1, 1, false}, {101, 101, 0, 1, true}});
}

TEST_F(ExternalWindowOperatorTest, WindowBoundScalarSingletonPublishesStableHeadBeforeInputEof) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({4096, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false,
                       {101, 101}, false, {101}),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  owner.pTaskInfo->pRoot->resultInfo.threshold = 4096;
  ASSERT_NE(owner.input(), nullptr);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.rows, 4096);
  EXPECT_EQ(pBlock->info.id.groupId, 101);
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_FALSE(finished);
  EXPECT_EQ(owner.input()->getNextCalls, 2);

  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.rows, 1);
  EXPECT_EQ(pBlock->info.id.groupId, 101);
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_FALSE(finished);
  EXPECT_EQ(owner.input()->getNextCalls, 3);

  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.groupId, 101);
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_FALSE(finished);
  EXPECT_EQ(owner.input()->getNextCalls, 3);

  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pBlock, nullptr);
  EXPECT_TRUE(finished);
  EXPECT_EQ(owner.input()->getNextCalls, 3);
}

TEST_F(ExternalWindowOperatorTest, WindowBoundScalarRebindsCanonicalGroupAfterScanAliasSwitch) {
  const std::vector<SSTriggerCalcParam> windows = {
      makeExternalWindowParam(kExtWinFactoryInputTs, kExtWinFactoryInputTs + 4095, 0, 0),
      makeExternalWindowParam(kExtWinFactoryInputTs + 4096, kExtWinFactoryInputTs + 8191, 0, 0),
      makeExternalWindowParam(kExtWinFactoryInputTs + 8192, kExtWinFactoryInputTs + 8192, 0, 0),
  };
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({8193, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 3, true, false, false,
                       {101, 202}, false, {101, 202}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false,
                       {0, 0}, {}, EXT_WIN_INPUT_WINDOW_BOUND, 0, false, false, {}, windows),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  owner.pTaskInfo->pRoot->resultInfo.threshold = 4096;
  ASSERT_NE(owner.input(), nullptr);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.rows, 4096);
  EXPECT_EQ(pBlock->info.id.groupId, 101);
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_FALSE(finished);
  EXPECT_EQ(owner.input()->getNextCalls, 1);

  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.rows, 4096);
  EXPECT_EQ(pBlock->info.id.groupId, 101);
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_FALSE(finished);
  EXPECT_EQ(owner.input()->getNextCalls, 2);

  expectExtWinProtocol(
      drainExtWinProtocol(owner.pTaskInfo->pRoot),
      {{101, 101, 1, 1, false}, {101, 101, 0, 1, true}, {202, 202, 1, 1, false}, {202, 202, 0, 1, true}});
  EXPECT_EQ(owner.input()->getNextCalls, 3);
}

TEST_F(ExternalWindowOperatorTest, WindowBoundScalarWaitsForCanonicalGroupBeforePublishing) {
  const std::vector<SSTriggerCalcParam> windows = {
      makeExternalWindowParam(kExtWinFactoryInputTs, kExtWinFactoryInputTs + 4095, 0, 0),
      makeExternalWindowParam(kExtWinFactoryInputTs + 4096, kExtWinFactoryInputTs + 8191, 0, 0),
      makeExternalWindowParam(kExtWinFactoryInputTs + 8192, kExtWinFactoryInputTs + 8192, 0, 0),
  };
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({8193, 8193}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 3, true, false,
                       false, {202, 101}, false, {101, 202}, true, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM,
                       false, {0, 0}, {}, EXT_WIN_INPUT_WINDOW_BOUND, 0, false, false, {}, windows),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  owner.pTaskInfo->pRoot->resultInfo.threshold = 4096;
  ASSERT_NE(owner.input(), nullptr);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.rows, 4096);
  EXPECT_EQ(pBlock->info.id.groupId, 101);
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_FALSE(finished);
  EXPECT_EQ(owner.input()->getNextCalls, 2);

  expectExtWinProtocol(drainExtWinProtocol(owner.pTaskInfo->pRoot), {{101, 101, 4096, 1, false},
                                                                     {101, 101, 1, 1, false},
                                                                     {101, 101, 0, 1, true},
                                                                     {202, 202, 4096, 1, false},
                                                                     {202, 202, 4096, 1, false},
                                                                     {202, 202, 1, 1, false},
                                                                     {202, 202, 0, 1, true}});
  EXPECT_EQ(owner.input()->getNextCalls, 3);
}

TEST_F(ExternalWindowOperatorTest, WindowBoundScalarPartialOutputResetRestartsCleanly) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({4096, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false,
                       {101, 101}, false, {101}),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  owner.pTaskInfo->pRoot->resultInfo.threshold = 4096;
  ASSERT_NE(owner.input(), nullptr);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_FALSE(qIsMultiGroupEogBlock(pBlock));
  ASSERT_EQ(pBlock->info.rows, 4096);
  ASSERT_EQ(owner.input()->getNextCalls, 2);

  ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.input()->resetCalls, 1);
  expectExternalWindowRuntimeAliasesEmpty(&owner.pTaskInfo->pStreamRuntimeInfo->funcInfo);
  EXPECT_EQ(owner.pTaskInfo->pRoot->status, OP_NOT_OPENED);

  expectExtWinProtocol(drainExtWinProtocol(owner.pTaskInfo->pRoot),
                       {{101, 101, 4096, 1, false}, {101, 101, 1, 1, false}, {101, 101, 0, 1, true}});
  EXPECT_EQ(owner.input()->getNextCalls, 5);
}

TEST_F(ExternalWindowOperatorTest, WindowBoundScalarPartialOutputFailedResetCleansAliases) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({4096, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false,
                       {101, 101}, false, {101}),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  owner.pTaskInfo->pRoot->resultInfo.threshold = 4096;

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_FALSE(qIsMultiGroupEogBlock(pBlock));

  stub.set(resetExprSupp, failExternalWindowResetExprSupp);
  EXPECT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(owner.pTaskInfo->pRoot->status, OP_NOT_OPENED);
  expectExternalWindowRuntimeAliasesEmpty(&owner.pTaskInfo->pStreamRuntimeInfo->funcInfo);

  stub.reset(resetExprSupp);
  ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.pTaskInfo->pRoot->status, OP_NOT_OPENED);
  expectExternalWindowRuntimeAliasesEmpty(&owner.pTaskInfo->pStreamRuntimeInfo->funcInfo);
  expectExtWinProtocol(drainExtWinProtocol(owner.pTaskInfo->pRoot),
                       {{101, 101, 4096, 1, false}, {101, 101, 1, 1, false}, {101, 101, 0, 1, true}});
}

TEST_F(ExternalWindowOperatorTest, WindowBoundScalarCanonicalMismatchFailsClosed) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({4096, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false,
                       {101, 101}, false, {101}),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  owner.pTaskInfo->pRoot->resultInfo.threshold = 4096;

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_FALSE(qIsMultiGroupEogBlock(pBlock));

  SArray *pOrder = owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcOrder;
  ASSERT_NE(pOrder, nullptr);
  auto *pFirstGid = static_cast<uint64_t *>(taosArrayGet(pOrder, 0));
  ASSERT_NE(pFirstGid, nullptr);
  *pFirstGid = 999;

  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(pBlock, nullptr);
}

TEST_F(ExternalWindowOperatorTest, WindowBoundStrictIndefiniteWaitsForInputEof) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({4096, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "csum", false, true, 1, false, false, false,
                       {101, 101}, false, {101}),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  owner.pTaskInfo->pRoot->resultInfo.threshold = 4096;
  ASSERT_NE(owner.input(), nullptr);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.groupId, 101);
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_FALSE(finished);
  EXPECT_EQ(owner.input()->getNextCalls, 3);
}

TEST_F(ExternalWindowOperatorTest, MergeAlignedModeEmitsEogAfterData) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, "count", false, false, 1, false, false,
                       false, {101}, false, {101}),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  expectExtWinProtocol(drainExtWinProtocol(owner.pTaskInfo->pRoot), {{101, 101, 1, 1, false}, {101, 101, 0, 1, true}});
}

TEST_F(ExternalWindowOperatorTest, BroadcastZeroRowsStillEmitsEog) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {},
                       false, {101}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  expectExtWinProtocol(drainExtWinProtocol(owner.pTaskInfo->pRoot), {{101, 101, 0, 1, true}});
}

TEST_F(ExternalWindowOperatorTest, BroadcastPublishesBeforeReplayingLaterWindows) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 3, false, false, false, {},
                       false, {101}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_FALSE(finished);

  int64_t scanBlocks = 0, scanRows = 0, replayBlocks = 0, replayRows = 0;
  int64_t budget = 0, peak = 0, flush = 0, load = 0, predicates = 0;
  ASSERT_EQ(extWinTestGetBroadcastStat(owner.pTaskInfo->pRoot, &scanBlocks, &scanRows, &replayBlocks, &replayRows,
                                       &budget, &peak, &flush, &load, &predicates),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(replayBlocks, 1);
  EXPECT_EQ(replayRows, 1);
}

TEST_F(ExternalWindowOperatorTest, BroadcastIndefiniteRowsPublishesBeforeReplayingLaterWindows) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "csum", false, true, 3, false, false, false, {},
                       false, {101}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_FALSE(finished);

  int64_t scanBlocks = 0, scanRows = 0, replayBlocks = 0, replayRows = 0;
  int64_t budget = 0, peak = 0, flush = 0, load = 0, predicates = 0;
  ASSERT_EQ(extWinTestGetBroadcastStat(owner.pTaskInfo->pRoot, &scanBlocks, &scanRows, &replayBlocks, &replayRows,
                                       &budget, &peak, &flush, &load, &predicates),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(replayBlocks, 1);
  EXPECT_EQ(replayRows, 1);
}

TEST_F(ExternalWindowOperatorTest, BroadcastReusesPublishedBlockForLaterWindows) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 2, false, false, false, {},
                       false, {101}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();

  bool         finished = false;
  SSDataBlock *pFirst = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pFirst, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pFirst, nullptr);
  ASSERT_FALSE(qIsMultiGroupEogBlock(pFirst));

  SSDataBlock *pSecond = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pSecond, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pSecond, nullptr);
  ASSERT_FALSE(qIsMultiGroupEogBlock(pSecond));
  EXPECT_EQ(pSecond, pFirst);
}

TEST_F(ExternalWindowOperatorTest, BroadcastReusesPublishedBlockForLaterPages) {
  constexpr int32_t          kRows = 11200;
  constexpr int32_t          kBinaryBytes = 1024;
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({kRows}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false,
                       {}, true, {101}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {100}, {},
                       EXT_WIN_INPUT_BROADCAST, kBinaryBytes, true),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();

  bool         finished = false;
  SSDataBlock *pFirst = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pFirst, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pFirst, nullptr);
  ASSERT_FALSE(qIsMultiGroupEogBlock(pFirst));

  int32_t     pageCount = 0;
  SBlockID    id = {0};
  STimeWindow window = {0};
  int64_t     version = 0;
  int16_t     dataLoad = 0;
  uint8_t     scanFlag = 0;
  ASSERT_EQ(extWinTestGetBroadcastPageMeta(owner.pTaskInfo->pRoot, 101, 0, 0, &pageCount, &id, &window, &version,
                                           &dataLoad, &scanFlag),
            TSDB_CODE_SUCCESS);
  ASSERT_GT(pageCount, 1);

  SSDataBlock *pSecond = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pSecond, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pSecond, nullptr);
  ASSERT_FALSE(qIsMultiGroupEogBlock(pSecond));
  EXPECT_EQ(pSecond, pFirst);
}

TEST_F(ExternalWindowOperatorTest, BroadcastLoadsNextGroupAfterPreviousGroupOutput) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {},
                       false, {101, 202}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  ASSERT_NE(owner.input(), nullptr);
  EXPECT_EQ(owner.input()->getNextCalls, 2);
}

TEST_F(ExternalWindowOperatorTest, BroadcastKeepsReaderAcrossGroupEogAndRequestReset) {
  const std::vector<uint64_t> groups = {101, 202};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {},
                       false, groups, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  owner.pTaskInfo->reuseReaderOnReset = true;

  expectExtWinProtocol(
      drainExtWinProtocol(owner.pTaskInfo->pRoot),
      {{101, 101, 1, 1, false}, {101, 101, 0, 1, true}, {202, 202, 1, 1, false}, {202, 202, 0, 1, true}});
  ASSERT_NE(owner.input(), nullptr);
  EXPECT_EQ(owner.input()->readerCreateCalls, 1);
  ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
  expectExtWinProtocol(
      drainExtWinProtocol(owner.pTaskInfo->pRoot),
      {{101, 101, 1, 1, false}, {101, 101, 0, 1, true}, {202, 202, 1, 1, false}, {202, 202, 0, 1, true}});
  EXPECT_EQ(owner.input()->readerCreateCalls, 1);

  int64_t scanBlocks = 0;
  int64_t scanRows = 0;
  int64_t replayBlocks = 0;
  int64_t replayRows = 0;
  int64_t actualBudget = 0;
  int64_t payloadPeak = 0;
  int64_t flushBytes = 0;
  int64_t loadBytes = 0;
  int64_t predicateEvals = 0;
  ASSERT_EQ(extWinTestGetBroadcastStat(owner.pTaskInfo->pRoot, &scanBlocks, &scanRows, &replayBlocks, &replayRows,
                                       &actualBudget, &payloadPeak, &flushBytes, &loadBytes, &predicateEvals),
            TSDB_CODE_SUCCESS);
  EXPECT_GT(actualBudget, 0);
}

TEST_F(ExternalWindowOperatorTest, BroadcastEmptyFirstGroupDefersReaderUntilNonemptyGroup) {
  const std::vector<uint64_t> groups = {101, 202};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {},
                       false, groups, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  ASSERT_NE(owner.input(), nullptr);
  owner.input()->emptyReaderGroupIds = {101};

  expectExtWinProtocol(drainExtWinProtocol(owner.pTaskInfo->pRoot),
                       {{101, 101, 0, 1, true}, {202, 202, 1, 1, false}, {202, 202, 0, 1, true}});
  ASSERT_FALSE(owner.input()->readerCreateCountsAtReset.empty());
  EXPECT_EQ(owner.input()->readerCreateCountsAtReset.front(), 0);
  EXPECT_EQ(owner.input()->readerCreateCalls, 1);

  int64_t scanBlocks = 0;
  int64_t scanRows = 0;
  int64_t replayBlocks = 0;
  int64_t replayRows = 0;
  int64_t actualBudget = 0;
  int64_t payloadPeak = 0;
  int64_t flushBytes = 0;
  int64_t loadBytes = 0;
  int64_t predicateEvals = 0;
  ASSERT_EQ(extWinTestGetBroadcastStat(owner.pTaskInfo->pRoot, &scanBlocks, &scanRows, &replayBlocks, &replayRows,
                                       &actualBudget, &payloadPeak, &flushBytes, &loadBytes, &predicateEvals),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(scanBlocks, 1);
  EXPECT_EQ(scanRows, 1);
  EXPECT_GT(actualBudget, 0);
}

TEST_F(ExternalWindowOperatorTest, NonStreamPartitionedDrainPreservesEveryRemoteGroupWithoutStrictEog) {
  ExternalWindowInputBlockSpec first;
  first.timestamps = {1589335200000};
  first.values = {11};
  first.id.groupId = 1001;
  first.id.baseGId = 1001;

  ExternalWindowInputBlockSpec second;
  second.timestamps = {1589338800000};
  second.values = {22};
  second.id.groupId = 1002;
  second.id.baseGId = 1002;

  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 1, false, false, false, {},
                       false, {}, false, true, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_BATCH, false, {}, {},
                       EXT_WIN_INPUT_WINDOW_BOUND, 0, false, false, {}, {}, false, 0, false, {first, second}),
            TSDB_CODE_SUCCESS);

  const auto protocol = drainExtWinProtocol(owner.pTaskInfo->pRoot);
  ASSERT_EQ(protocol.size(), 2);
  EXPECT_EQ(protocol[0].baseGId, 1001);
  EXPECT_EQ(protocol[1].baseGId, 1002);
  for (const auto &item : protocol) {
    EXPECT_EQ(item.groupId, item.baseGId);
    EXPECT_GT(item.rows, 0);
    EXPECT_EQ(item.dataLoad, 1);
    EXPECT_FALSE(item.eog);
  }
}

TEST_F(ExternalWindowOperatorTest, RuntimeMultiGroupWithoutStaticJoinKeepsLegacyProtocol) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 1, false, false, false, {101},
                       false, {101}),
            TSDB_CODE_SUCCESS);

  expectExtWinProtocol(drainExtWinProtocol(owner.pTaskInfo->pRoot), {{101, 101, 1, 1, false}});
}

TEST_F(ExternalWindowOperatorTest, LegacyEmptySettlementPreservesPublishedGroup) {
  const std::vector<uint64_t> groups = {101, 202, 303, 404, 505};
  for (ENodeType nodeType : {QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL}) {
    SCOPED_TRACE(nodeType);
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({1, 1, 1}, nodeType, "sum", false, false, 1, false, true, false, {101, 202, 303}, false,
                         groups, true, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {0, 5, 7}),
              TSDB_CODE_SUCCESS);

    for (int pass = 0; pass < 2; ++pass) {
      SCOPED_TRACE(pass);
      std::vector<ExternalWindowPublication> publications;
      ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
      expectExternalWindowGroupsPublishedOnce(publications, groups);
      ASSERT_EQ(publications.size(), groups.size());
      for (const auto &publication : publications) {
        bool hasData = publication.gid == 202 || publication.gid == 303;
        EXPECT_EQ(publication.rows, hasData ? 1 : 0);
        EXPECT_EQ(publication.dataLoad, 1);
        if (hasData) {
          EXPECT_EQ(publication.values, std::vector<int64_t>{publication.gid == 202 ? 5 : 7});
        }
      }
      if (pass == 0) {
        ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
      }
    }
  }
}

TEST_F(ExternalWindowOperatorTest, LegacyFilteredGroupsAfterPublishedGroupStillSettle) {
  const std::vector<uint64_t> groups = {101, 202, 303, 404, 505, 606};
  for (ENodeType nodeType : {QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL}) {
    SCOPED_TRACE(nodeType);
    for (bool scalar : {false, true}) {
      // Hash scalar mode does not initialize the output filter used by this fixture.
      if (scalar && nodeType == QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL) {
        continue;
      }
      SCOPED_TRACE(scalar);
      for (int32_t threshold : {1, 3072}) {
        SCOPED_TRACE(threshold);
        ExternalWindowFactoryOwner owner;
        ASSERT_EQ(owner.init({1, 1, 1, 1, 1, 1}, nodeType, scalar ? nullptr : "sum", scalar, false, 1, false, true,
                             false, {101, 101, 202, 303, 404, 505}, false, groups, true, false, FILL_MODE_NONE, {},
                             OPTR_EXEC_MODEL_STREAM, false, {5, 0, 0, 0, 7, 0}),
                  TSDB_CODE_SUCCESS);
        owner.pTaskInfo->pRoot->resultInfo.threshold = threshold;

        for (int pass = 0; pass < 2; ++pass) {
          SCOPED_TRACE(pass);
          std::vector<ExternalWindowPublication> publications;
          ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
          expectExternalWindowGroupsPublishedOnce(publications, groups);
          ASSERT_EQ(publications.size(), groups.size());
          for (const auto &publication : publications) {
            bool hasData = publication.gid == 101 || publication.gid == 404;
            EXPECT_EQ(publication.rows, hasData ? 1 : 0);
            EXPECT_EQ(publication.dataLoad, 1);
            EXPECT_EQ(publication.values,
                      hasData ? std::vector<int64_t>{publication.gid == 101 ? 5 : 7} : std::vector<int64_t>{});
          }
          if (pass == 0) {
            ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
          }
        }
      }
    }
  }
}

TEST_F(ExternalWindowOperatorTest, StaticMultiGroupJoinWithoutRuntimeModeFailsClosed) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 1), TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();

  bool         finished = false;
  SSDataBlock *block = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &block, &finished), TSDB_CODE_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(block, nullptr);
}

TEST_F(ExternalWindowOperatorTest, RoundTripOperatorConstructionRestoresPhysicalParents) {
  stub.set(createHashJoinOperatorInfo, createRoundTripHashJoinOperator);
  ExternalWindowRoundTripOwner owner;
  ASSERT_EQ(owner.init(true), TSDB_CODE_SUCCESS);

  ASSERT_NE(owner.pTaskInfo, nullptr);
  ASSERT_NE(owner.pTaskInfo->pSubplan, nullptr);
  SPhysiNode *pJoin = owner.pTaskInfo->pSubplan->pNode;
  ASSERT_NE(pJoin, nullptr);
  ASSERT_EQ(nodeType(pJoin), QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN);
  auto *pExternal = reinterpret_cast<SPhysiNode *>(nodesListGetNode(pJoin->pChildren, 0));
  ASSERT_NE(pExternal, nullptr);
  EXPECT_EQ(pExternal->pParent, pJoin);
  auto *pRowset = reinterpret_cast<SPhysiNode *>(nodesListGetNode(pExternal->pChildren, 0));
  ASSERT_NE(pRowset, nullptr);
  EXPECT_EQ(pRowset->pParent, pExternal);

  ASSERT_NE(owner.externalWindow(), nullptr);
  EXPECT_EQ(owner.externalWindow()->pPhyNode, pExternal);
}

TEST_F(ExternalWindowOperatorTest, RoundTripStaticAndRuntimeMultiGroupActivatesStrictEog) {
  stub.set(createHashJoinOperatorInfo, createRoundTripHashJoinOperator);
  ExternalWindowRoundTripOwner owner;
  ASSERT_EQ(owner.init(true), TSDB_CODE_SUCCESS);

  expectExtWinProtocol(drainExtWinProtocol(owner.externalWindow()), {{101, 101, 1, 1, false}, {101, 101, 0, 1, true}});
}

TEST_F(ExternalWindowOperatorTest, RoundTripStaticMultiGroupWithoutRuntimeModeFailsClosed) {
  stub.set(createHashJoinOperatorInfo, createRoundTripHashJoinOperator);
  ExternalWindowRoundTripOwner owner;
  ASSERT_EQ(owner.init(false), TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *block = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &block, &finished), TSDB_CODE_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(block, nullptr);
}

TEST_F(ExternalWindowOperatorTest, MergeAlignedLastEmptyGroupEmitsExactProtocol) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, "count", false, false, 1, false, false,
                       false, {101}, false, {101, 202}),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();

  expectExtWinProtocol(drainExtWinProtocol(owner.pTaskInfo->pRoot),
                       {{101, 101, 1, 1, false}, {101, 101, 0, 1, true}, {202, 202, 0, 1, true}});
}

TEST_F(ExternalWindowOperatorTest, MultiGroupPredicateFilteredToZeroEmitsExactEogSequence) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 1, false, true, false, {101, 202},
                 false, {101, 202}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {-1, -1}),
      TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();

  expectExtWinProtocol(drainExtWinProtocol(owner.pTaskInfo->pRoot), {{101, 101, 0, 1, true}, {202, 202, 0, 1, true}});
}

TEST_F(ExternalWindowOperatorTest, TwoLeavesOwnSeparateRegistryForSameTriggerGroup) {
  ExternalWindowTwoLeafOwner owner;
  ASSERT_EQ(owner.init(11, 22), TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowPublication> first = drainLeaf(owner.pLeafA);
  std::vector<ExternalWindowPublication> second = drainLeaf(owner.pLeafB);
  ASSERT_EQ(first.size(), 1);
  ASSERT_EQ(second.size(), 1);
  EXPECT_EQ(first[0].values, (std::vector<int64_t>{11}));
  EXPECT_EQ(second[0].values, (std::vector<int64_t>{22}));
}

TEST_F(ExternalWindowOperatorTest, SameLeafOwnsSeparateContextsForMultipleCalcGroups) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1, 1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 1, false, false, true,
                       {101, 101, 101}, false, {101}, true, true, FILL_MODE_NONE, {1001, 1002, 1001},
                       OPTR_EXEC_MODEL_STREAM, false, {11, 22, 33}),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  ExternalWindowOutputRows rows;
  for (const ExternalWindowPublication &publication : publications) {
    rows.insert(rows.end(), publication.rowTuples.begin(), publication.rowTuples.end());
  }
  std::sort(rows.begin(), rows.end());
  EXPECT_EQ(rows, (ExternalWindowOutputRows{{1001, 0, false, 44}, {1002, 0, false, 22}}));
}

TEST_F(ExternalWindowOperatorTest, RunnerGroupContextStaysNullAcrossExecuteResetAndDestroy) {
  ExternalWindowTwoLeafOwner owner;
  ASSERT_EQ(owner.init(11, 22), TSDB_CODE_SUCCESS);
  SSHashObj *pGroups = owner.owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcInfos;
  expectRunnerContextsNull(pGroups);

  ASSERT_FALSE(drainLeaf(owner.pLeafA).empty());
  expectRunnerContextsNull(pGroups);
  ASSERT_EQ(owner.pLeafA->fpSet.resetStateFn(owner.pLeafA), TSDB_CODE_SUCCESS);
  expectRunnerContextsNull(pGroups);
}

TEST_F(ExternalWindowOperatorTest, SuccessfulResetDropsOnlyTheTargetLeafRegistry) {
  ExternalWindowTwoLeafOwner owner;
  ASSERT_EQ(owner.init(11, 22, 3, EXT_WIN_INPUT_BROADCAST, false), TSDB_CODE_SUCCESS);
  owner.pLeafA->resultInfo.threshold = 1;
  owner.pLeafB->resultInfo.threshold = 1;

  SSDataBlock *pFirstA = nullptr;
  SSDataBlock *pFirstB = nullptr;
  ASSERT_EQ(owner.pLeafB->fpSet.getNextFn(owner.pLeafB, &pFirstB), TSDB_CODE_SUCCESS);
  ASSERT_EQ(owner.pLeafA->fpSet.getNextFn(owner.pLeafA, &pFirstA), TSDB_CODE_SUCCESS);
  ASSERT_NE(pFirstB, nullptr);
  ASSERT_NE(pFirstA, nullptr);
  SStreamRuntimeFuncInfo *pRuntime = &owner.owner.pTaskInfo->pStreamRuntimeInfo->funcInfo;
  ASSERT_NE(pRuntime->pStreamBlkWinIdx, nullptr);
  ASSERT_NE(pRuntime->curGrpCalc, nullptr);
  ASSERT_NE(pRuntime->pStreamPesudoFuncVals, nullptr);
  ASSERT_NE(pRuntime->createTable, nullptr);
  ASSERT_EQ(owner.pLeafA->fpSet.resetStateFn(owner.pLeafA), TSDB_CODE_SUCCESS);
  expectExternalWindowRuntimeAliasesEmpty(pRuntime);

  std::vector<ExternalWindowPublication> remaining = drainLeaf(owner.pLeafB);
  std::vector<int64_t>                   values;
  for (const ExternalWindowPublication &publication : remaining) {
    values.insert(values.end(), publication.values.begin(), publication.values.end());
  }
  EXPECT_EQ(values, (std::vector<int64_t>{22, 22}));
}

TEST_F(ExternalWindowOperatorTest, ResetDoesNotDetachSiblingRuntimePublication) {
  ExternalWindowTwoLeafOwner owner;
  ASSERT_EQ(owner.init(11, 22), TSDB_CODE_SUCCESS);

  SSDataBlock *pFirstA = nullptr;
  SSDataBlock *pFirstB = nullptr;
  ASSERT_EQ(owner.pLeafA->fpSet.getNextFn(owner.pLeafA, &pFirstA), TSDB_CODE_SUCCESS);
  ASSERT_EQ(owner.pLeafB->fpSet.getNextFn(owner.pLeafB, &pFirstB), TSDB_CODE_SUCCESS);
  ASSERT_NE(pFirstA, nullptr);
  ASSERT_NE(pFirstB, nullptr);

  SStreamRuntimeFuncInfo      *pRuntime = &owner.owner.pTaskInfo->pStreamRuntimeInfo->funcInfo;
  ExternalWindowRuntimeAliases aliases = captureExternalWindowRuntimeAliases(pRuntime);
  ASSERT_NE(aliases.pWindowIndexes, nullptr);
  ASSERT_NE(aliases.pCalc, nullptr);
  ASSERT_EQ(owner.pLeafA->fpSet.resetStateFn(owner.pLeafA), TSDB_CODE_SUCCESS);
  expectExternalWindowRuntimeAliasesEqual(pRuntime, aliases);
  ASSERT_EQ(owner.pLeafB->fpSet.resetStateFn(owner.pLeafB), TSDB_CODE_SUCCESS);
  expectExternalWindowRuntimeAliasesEmpty(pRuntime);
}

TEST_F(ExternalWindowOperatorTest, EogClearsSiblingRuntimeIndexPublication) {
  ExternalWindowTwoLeafOwner owner;
  ASSERT_EQ(owner.init(11, 22, 1, EXT_WIN_INPUT_WINDOW_BOUND, false, true), TSDB_CODE_SUCCESS);
  owner.owner.qualifyForMultiGroupJoin();

  SSDataBlock *pSiblingOutput = nullptr;
  ASSERT_EQ(owner.pLeafB->fpSet.getNextFn(owner.pLeafB, &pSiblingOutput), TSDB_CODE_SUCCESS);
  ASSERT_NE(pSiblingOutput, nullptr);
  SStreamRuntimeFuncInfo *pRuntime = &owner.owner.pTaskInfo->pStreamRuntimeInfo->funcInfo;
  SArray                 *pSiblingIndexes = pRuntime->pStreamBlkWinIdx;
  ASSERT_NE(pSiblingIndexes, nullptr);

  SSDataBlock *pSettlement = nullptr;
  ASSERT_EQ(owner.pLeafA->fpSet.getNextFn(owner.pLeafA, &pSettlement), TSDB_CODE_SUCCESS);
  ASSERT_NE(pSettlement, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pSettlement));
  EXPECT_EQ(pSettlement->info.id.baseGId, 101);
  EXPECT_EQ(pRuntime->pStreamBlkWinIdx, nullptr);
}

TEST_F(ExternalWindowOperatorTest, NormalAggregateFailedResetDetachesAliasesAndRetriesSameOperator) {
  ExternalWindowTwoLeafOwner owner;
  ASSERT_EQ(owner.init(11, 22, 3, EXT_WIN_INPUT_BROADCAST, false), TSDB_CODE_SUCCESS);
  owner.pLeafA->resultInfo.threshold = 1;

  SSDataBlock *pFirst = nullptr;
  ASSERT_EQ(owner.pLeafA->fpSet.getNextFn(owner.pLeafA, &pFirst), TSDB_CODE_SUCCESS);
  ASSERT_NE(pFirst, nullptr);
  SStreamRuntimeFuncInfo *pRuntime = &owner.owner.pTaskInfo->pStreamRuntimeInfo->funcInfo;
  ASSERT_NE(pRuntime->pStreamBlkWinIdx, nullptr);
  ASSERT_NE(pRuntime->curGrpCalc, nullptr);

  stub.set(resetAggSup, failExternalWindowResetAggSup);
  EXPECT_EQ(owner.pLeafA->fpSet.resetStateFn(owner.pLeafA), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(owner.pLeafA->status, OP_NOT_OPENED);
  expectExternalWindowRuntimeAliasesEmpty(pRuntime);
  expectRunnerContextsNull(pRuntime->pGroupCalcInfos);

  stub.reset(resetAggSup);
  EXPECT_EQ(owner.pLeafA->fpSet.resetStateFn(owner.pLeafA), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.pLeafA->status, OP_NOT_OPENED);
  expectExternalWindowRuntimeAliasesEmpty(pRuntime);
}

TEST_F(ExternalWindowOperatorTest, FailedResetLeavesOperatorInertAndRetryable) {
  ExternalWindowTwoLeafOwner owner;
  ASSERT_EQ(owner.init(11, 22), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(drainLeaf(owner.pLeafA).empty());
  stub.set(resetExprSupp, failExternalWindowResetExprSupp);
  EXPECT_EQ(owner.pLeafA->fpSet.resetStateFn(owner.pLeafA), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(owner.pLeafA->status, OP_NOT_OPENED);
  expectExternalWindowRuntimeAliasesEmpty(&owner.owner.pTaskInfo->pStreamRuntimeInfo->funcInfo);
  expectRunnerContextsNull(owner.owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcInfos);
  stub.reset(resetExprSupp);
  EXPECT_EQ(owner.pLeafA->fpSet.resetStateFn(owner.pLeafA), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.pLeafA->status, OP_NOT_OPENED);

  ExternalWindowTwoLeafOwner destroyOwner;
  ASSERT_EQ(destroyOwner.init(33, 44), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(drainLeaf(destroyOwner.pLeafA).empty());
  stub.set(resetExprSupp, failExternalWindowResetExprSupp);
  EXPECT_EQ(destroyOwner.pLeafA->fpSet.resetStateFn(destroyOwner.pLeafA), TSDB_CODE_OUT_OF_MEMORY);
  expectExternalWindowRuntimeAliasesEmpty(&destroyOwner.owner.pTaskInfo->pStreamRuntimeInfo->funcInfo);
  destroyOwner.destroyRuntimeBeforeTask();
}

TEST_F(ExternalWindowOperatorTest, ParentResetFailureLeavesUnresetChildSafeAfterRuntimeCleanup) {
  ExternalWindowTwoLeafOwner owner;
  ASSERT_EQ(owner.init(11, 22), TSDB_CODE_SUCCESS);
  ASSERT_FALSE(drainLeaf(owner.pLeafA).empty());
  ASSERT_FALSE(drainLeaf(owner.pLeafB).empty());
  owner.setParentResetFailure(true);
  EXPECT_EQ(streamClearStatesForOperators(owner.owner.pTaskInfo), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_NE(owner.pLeafA->status, OP_NOT_OPENED);
  EXPECT_NE(owner.pLeafB->status, OP_NOT_OPENED);
  owner.destroyRuntimeBeforeTask();
}

TEST_F(ExternalWindowOperatorTest, BroadcastReadsOnceAndReplaysForEveryWindow) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({2, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 3, false, false, false, {},
                       false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7, 9}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
  ASSERT_EQ(rows.size(), 9);
  for (int32_t window = 0; window < 3; ++window) {
    std::vector<int32_t> values;
    for (const auto &row : rows) {
      if (row.windowIndex == window) {
        values.push_back(row.value);
      }
    }
    EXPECT_EQ(values, (std::vector<int32_t>{7, 8, 9}));
  }
  ASSERT_NE(owner.input(), nullptr);
  EXPECT_EQ(owner.input()->getNextCalls, 3);

  ExternalWindowFactoryOwner aggregateOwner;
  ASSERT_EQ(aggregateOwner.init({2, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 3, false, false,
                                false, {}, false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false,
                                {7, 9}, {}, EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  std::vector<int64_t> aggregateValues;
  ASSERT_EQ(drainExternalWindow(&aggregateOwner, &aggregateValues), TSDB_CODE_SUCCESS);
  EXPECT_EQ(aggregateValues, (std::vector<int64_t>{24, 24, 24}));
  ASSERT_NE(aggregateOwner.input(), nullptr);
  EXPECT_EQ(aggregateOwner.input()->getNextCalls, 3);
}

TEST_F(ExternalWindowOperatorTest, BroadcastScanProjectsSingleGroupRuntimeForDownstream) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {101},
                       false, {101}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.pTaskInfo->pStreamRuntimeInfo, nullptr);
  auto *pRuntime = &owner.pTaskInfo->pStreamRuntimeInfo->funcInfo;
  pRuntime->addOptions |= STREAM_OPTION_NESTED_WINDOW_PLAN;
  pRuntime->pContextPolicy = makeFetchProjectionPolicy(101, 0);
  pRuntime->pAncestorContext = makeFetchProjectionContext(101, 0);
  ASSERT_NE(pRuntime->pContextPolicy, nullptr);
  ASSERT_NE(pRuntime->pAncestorContext, nullptr);
  auto *pContextPolicy = pRuntime->pContextPolicy;
  auto *pAncestorContext = pRuntime->pAncestorContext;

  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.input(), nullptr);
  EXPECT_EQ(owner.input()->getNextCalls, 2);
  EXPECT_EQ(owner.input()->multiGroupGetNextCalls, 0);
  EXPECT_EQ(owner.input()->nestedPlanGetNextCalls, 2);
  EXPECT_EQ(owner.input()->contextGetNextCalls, 2);
  EXPECT_TRUE(pRuntime->isMultiGroupCalc);
  EXPECT_TRUE(BIT_FLAG_TEST_MASK(pRuntime->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN));
  EXPECT_EQ(pRuntime->pContextPolicy, pContextPolicy);
  EXPECT_EQ(pRuntime->pAncestorContext, pAncestorContext);
}

TEST_F(ExternalWindowOperatorTest, BroadcastLongJumpPreservesOwningRuntime) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {101},
                       false, {101}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.pTaskInfo->pStreamRuntimeInfo, nullptr);
  auto *pRuntimeInfo = owner.pTaskInfo->pStreamRuntimeInfo;
  auto *pRuntime = &pRuntimeInfo->funcInfo;
  pRuntime->addOptions |= STREAM_OPTION_NESTED_WINDOW_PLAN;
  pRuntime->pContextPolicy = makeFetchProjectionPolicy(101, 0);
  pRuntime->pAncestorContext = makeFetchProjectionContext(101, 0);
  ASSERT_NE(pRuntime->pContextPolicy, nullptr);
  ASSERT_NE(pRuntime->pAncestorContext, nullptr);
  auto *pContextPolicy = pRuntime->pContextPolicy;
  auto *pAncestorContext = pRuntime->pAncestorContext;
  pRuntimeInfo->reuseReaderOnFetchReset = true;
  ASSERT_NE(owner.input(), nullptr);
  owner.input()->longJumpAtGetNextCall = 1;
  owner.input()->longJumpCode = TSDB_CODE_OUT_OF_MEMORY;

  std::vector<ExternalWindowBroadcastRow> rows;
  EXPECT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_TRUE(pRuntime->isMultiGroupCalc);
  EXPECT_EQ(pRuntime->pContextPolicy, pContextPolicy);
  EXPECT_EQ(pRuntime->pAncestorContext, pAncestorContext);
  EXPECT_TRUE(pRuntimeInfo->reuseReaderOnFetchReset);
  EXPECT_FALSE(pRuntimeInfo->fetchInfo.active);
  EXPECT_EQ(GET_STM_RTINFO(owner.pTaskInfo), pRuntime);
  EXPECT_EQ(owner.input()->multiGroupGetNextCalls, 0);
  EXPECT_EQ(owner.input()->nestedPlanGetNextCalls, 1);
  EXPECT_EQ(owner.input()->contextGetNextCalls, 1);
  ASSERT_EQ(owner.input()->getNextReuseModes, (std::vector<bool>{false}));

  ASSERT_EQ(owner.pTaskInfo->pRoot->fpSet.resetStateFn(owner.pTaskInfo->pRoot), TSDB_CODE_SUCCESS);
  owner.input()->longJumpAtGetNextCall = 0;
  owner.pTaskInfo->code = TSDB_CODE_SUCCESS;
  rows.clear();
  EXPECT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
  EXPECT_EQ(rows.size(), 1);
}

TEST_F(ExternalWindowOperatorTest, BroadcastUsesPrebuiltContextViewWithoutPerGroupProjection) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false,
                       {101, 101}, false, {101}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false,
                       {7, 8}, {}, EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.pTaskInfo->pStreamRuntimeInfo, nullptr);
  auto *pRuntime = &owner.pTaskInfo->pStreamRuntimeInfo->funcInfo;
  pRuntime->addOptions |= STREAM_OPTION_NESTED_WINDOW_PLAN;
  pRuntime->pContextPolicy = makeFetchProjectionPolicy(101, 0);
  pRuntime->pAncestorContext = makeFetchProjectionContext(101, 0);
  ASSERT_NE(pRuntime->pContextPolicy, nullptr);
  ASSERT_NE(pRuntime->pAncestorContext, nullptr);
  ASSERT_NE(owner.input(), nullptr);
  auto *pContextPolicy = pRuntime->pContextPolicy;
  auto *pAncestorContext = pRuntime->pAncestorContext;
  stub.set(tProjectStreamCalcContextForGroup, countExternalWindowGroupProjection);

  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.input()->getNextCalls, 3);
  EXPECT_EQ(gExternalWindowGroupProjectionCalls, 0);
  EXPECT_TRUE(pRuntime->isMultiGroupCalc);
  EXPECT_EQ(pRuntime->pContextPolicy, pContextPolicy);
  EXPECT_EQ(pRuntime->pAncestorContext, pAncestorContext);
}

TEST_F(ExternalWindowOperatorTest, BroadcastRescansUnpartitionedRelationForEachTriggerGroup) {
  const std::vector<uint64_t> groups = {101, 202};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {},
                       false, groups, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0].value, 7);
  EXPECT_EQ(rows[1].value, 7);
  EXPECT_NE(rows[0].id.baseGId, rows[1].id.baseGId);
  EXPECT_TRUE(rows[0].id.baseGId == 101 || rows[0].id.baseGId == 202);
  EXPECT_TRUE(rows[1].id.baseGId == 101 || rows[1].id.baseGId == 202);
  ASSERT_NE(owner.input(), nullptr);
  EXPECT_EQ(owner.input()->getNextCalls, 4);
  EXPECT_EQ(owner.input()->resetCalls, 1);
  EXPECT_EQ(owner.input()->multiGroupGetNextCalls, 0);
  ASSERT_EQ(owner.input()->resetReuseModes, (std::vector<bool>{true}));
  ASSERT_FALSE(owner.pTaskInfo->pStreamRuntimeInfo->reuseReaderOnFetchReset);
  ASSERT_FALSE(owner.input()->getNextReuseModes.front());
  EXPECT_TRUE(std::find(owner.input()->getNextReuseModes.begin(), owner.input()->getNextReuseModes.end(), true) !=
              owner.input()->getNextReuseModes.end());
}

TEST_F(ExternalWindowOperatorTest, BroadcastResetFailureClearsReaderReuseMode) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {},
                       false, {101, 202}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  owner.input()->resetCode = TSDB_CODE_OUT_OF_MEMORY;

  std::vector<ExternalWindowBroadcastRow> rows;
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows));
  ASSERT_EQ(owner.input()->resetReuseModes, (std::vector<bool>{true}));
  EXPECT_FALSE(owner.pTaskInfo->pStreamRuntimeInfo->reuseReaderOnFetchReset);
}

TEST(StreamExchangeResetModeTest, ProjectsReuseOnlyOntoFirstResetFetch) {
  SStreamRuntimeInfo runtime = {};
  runtime.reuseReaderOnFetchReset = true;
  bool fetchSent = false;

  SResFetchReq first = {};
  exchangeTestSetStreamFetchResetMode(&first, &fetchSent, &runtime);
  EXPECT_TRUE(first.reset);
  EXPECT_TRUE(first.reuseReader);
  EXPECT_TRUE(fetchSent);

  SResFetchReq continuation = {};
  exchangeTestSetStreamFetchResetMode(&continuation, &fetchSent, &runtime);
  EXPECT_FALSE(continuation.reset);
  EXPECT_FALSE(continuation.reuseReader);

  fetchSent = false;
  runtime.reuseReaderOnFetchReset = false;
  SResFetchReq fresh = {};
  exchangeTestSetStreamFetchResetMode(&fresh, &fetchSent, &runtime);
  EXPECT_TRUE(fresh.reset);
  EXPECT_FALSE(fresh.reuseReader);
}

TEST_F(ExternalWindowOperatorTest, BroadcastKeepsZeroAndDuplicateRows) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1, 1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 2, false, false, false,
                       {}, false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {0, 0, 2}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
  for (int32_t window = 0; window < 2; ++window) {
    std::vector<int32_t> values;
    for (const auto &row : rows) {
      if (row.windowIndex == window) {
        values.push_back(row.value);
      }
    }
    EXPECT_EQ(values, (std::vector<int32_t>{0, 0, 2}));
  }
}

TEST_F(ExternalWindowOperatorTest, BroadcastStoreSpillsAndReplaysInOrder) {
  constexpr int32_t          kRows = 11200;
  constexpr int32_t          kBinaryBytes = 1024;
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({kRows}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false,
                       {}, true, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {100}, {},
                       EXT_WIN_INPUT_BROADCAST, kBinaryBytes, true),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, 1, -1, -1, &rows), TSDB_CODE_SUCCESS);
  ASSERT_EQ(rows.size(), kRows);
  for (int32_t row = 0; row < kRows; ++row) {
    SCOPED_TRACE(row);
    EXPECT_EQ(rows[row].windowIndex, 0);
    EXPECT_EQ(rows[row].valueIsNull, row == 0);
    if (row != 0) {
      EXPECT_EQ(rows[row].value, 100 + row);
    }
    std::string expected(kBinaryBytes, static_cast<char>('a' + (row % 23)));
    int32_t     marker = 100 + row;
    memcpy(&expected[0], &marker, sizeof(marker));
    EXPECT_EQ(rows[row].binary, expected);
    EXPECT_EQ(rows[row].id.uid, 0);
    EXPECT_EQ(rows[row].id.blockId, kExtWinFactoryOutputBlockId);
    EXPECT_EQ(rows[row].id.groupId, 0);
    EXPECT_EQ(rows[row].id.baseGId, 0);
  }

  int64_t scanBlocks = 0;
  int64_t scanRows = 0;
  int64_t replayBlocks = 0;
  int64_t replayRows = 0;
  int64_t actualBudget = 0;
  int64_t payloadPeak = 0;
  int64_t flushBytes = 0;
  int64_t loadBytes = 0;
  int64_t predicateEvals = 0;
  ASSERT_EQ(extWinTestGetBroadcastStat(owner.pTaskInfo->pRoot, &scanBlocks, &scanRows, &replayBlocks, &replayRows,
                                       &actualBudget, &payloadPeak, &flushBytes, &loadBytes, &predicateEvals),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(scanBlocks, 1);
  EXPECT_EQ(scanRows, kRows);
  EXPECT_GT(replayBlocks, 1);
  EXPECT_EQ(replayRows, kRows);
  EXPECT_EQ(actualBudget, 10 * 1024 * 1024);
  EXPECT_EQ(payloadPeak, 10 * 1024 * 1024);
  EXPECT_GT(flushBytes, 0);
  EXPECT_GT(loadBytes, 0);
}

TEST_F(ExternalWindowOperatorTest, BroadcastStoreIsIsolatedByBaseGroup) {
  const std::vector<uint64_t> groups = {101, 202};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 2, false, false, false,
                       groups, false, groups, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {11, 22},
                       {}, EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
  ASSERT_EQ(rows.size(), 8);
  for (const auto &row : rows) {
    ASSERT_TRUE(row.id.baseGId == 101 || row.id.baseGId == 202);
    EXPECT_EQ(row.id.groupId, row.id.baseGId);
  }
  for (uint64_t groupId : groups) {
    for (int32_t window = 0; window < 2; ++window) {
      std::vector<int32_t> values;
      for (const auto &row : rows) {
        if (row.id.baseGId == groupId && row.windowIndex == window) {
          values.push_back(row.value);
        }
      }
      EXPECT_EQ(values, (std::vector<int32_t>{11, 22}));
    }
  }
}

TEST_F(ExternalWindowOperatorTest, BroadcastRecomputesWindowMetadataPerReplay) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 2, false, false, false, {},
                       false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {42}, {},
                       EXT_WIN_INPUT_BROADCAST, 0, false, true),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, 1, 2, &rows), TSDB_CODE_SUCCESS);
  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0].value, 42);
  EXPECT_EQ(rows[0].windowIndex, 0);
  EXPECT_EQ(rows[0].windowStart, kExtWinFactoryInputTs);
  EXPECT_EQ(rows[0].windowEnd, kExtWinFactoryInputTs);
  EXPECT_EQ(rows[1].value, 42);
  EXPECT_EQ(rows[1].windowIndex, 1);
  EXPECT_EQ(rows[1].windowStart, kExtWinFactoryInputTs + 1);
  EXPECT_EQ(rows[1].windowEnd, kExtWinFactoryInputTs + 1);
}

TEST_F(ExternalWindowOperatorTest, BroadcastAppendFailureFailsRequest) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {}, false,
                 {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {}, EXT_WIN_INPUT_BROADCAST),
      TSDB_CODE_SUCCESS);
  stub.set(winInputStoreAppendBlock, failExternalWindowStoreAppend);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), kExternalWindowAppendError);
  EXPECT_EQ(pBlock, nullptr);
  stub.reset(winInputStoreAppendBlock);
  EXPECT_EQ(owner.pTaskInfo->pRoot->fpSet.resetStateFn(owner.pTaskInfo->pRoot), TSDB_CODE_SUCCESS);
}

TEST_F(ExternalWindowOperatorTest, BroadcastReplayFailureFailsRequest) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {}, false,
                 {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {}, EXT_WIN_INPUT_BROADCAST),
      TSDB_CODE_SUCCESS);
  stub.set(winInputStoreGetBlock, failExternalWindowStoreReplay);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), kExternalWindowReplayError);
  EXPECT_EQ(pBlock, nullptr);
  stub.reset(winInputStoreGetBlock);
  EXPECT_EQ(owner.pTaskInfo->pRoot->fpSet.resetStateFn(owner.pTaskInfo->pRoot), TSDB_CODE_SUCCESS);
}

TEST_F(ExternalWindowOperatorTest, BroadcastSecondAppendFailureAllowsDirectDestroy) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {},
                       false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {11, 22}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  gExternalWindowAppendSpy.failOnCall = 2;
  stub.set(winInputStoreAppendBlock, failLaterExternalWindowStoreAppend);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), kExternalWindowAppendError);
  EXPECT_EQ(pBlock, nullptr);
  EXPECT_EQ(gExternalWindowAppendSpy.calls, 2);
  stub.reset(winInputStoreAppendBlock);
  qDestroyTask(owner.pTaskInfo);
  owner.pTaskInfo = nullptr;
}

TEST_F(ExternalWindowOperatorTest, BroadcastSecondPageReplayFailureAllowsRepeatedReset) {
  constexpr int32_t          kRows = 20;
  constexpr int32_t          kBinaryBytes = 1024;
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({kRows}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false,
                       {}, false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {11}, {},
                       EXT_WIN_INPUT_BROADCAST, kBinaryBytes, true),
            TSDB_CODE_SUCCESS);
  gExternalWindowGetSpy.failOnCall = 2;
  stub.set(winInputStoreGetBlock, failLaterExternalWindowStoreGet);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), kExternalWindowReplayError);
  EXPECT_EQ(pBlock, nullptr);
  EXPECT_EQ(gExternalWindowGetSpy.calls, 2);
  EXPECT_EQ(gExternalWindowGetSpy.pageIndexes, (std::vector<int32_t>{0, 1}));
  stub.reset(winInputStoreGetBlock);
  EXPECT_EQ(owner.pTaskInfo->pRoot->fpSet.resetStateFn(owner.pTaskInfo->pRoot), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.pTaskInfo->pRoot->fpSet.resetStateFn(owner.pTaskInfo->pRoot), TSDB_CODE_SUCCESS);
}

TEST_F(ExternalWindowOperatorTest, BroadcastNoTempSpaceFailsRequest) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {}, false,
                 {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {}, EXT_WIN_INPUT_BROADCAST),
      TSDB_CODE_SUCCESS);
  stub.set(osTempSpaceAvailable, failExternalWindowTempSpace);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_NO_DISKSPACE);
  EXPECT_EQ(pBlock, nullptr);
  stub.reset(osTempSpaceAvailable);
  EXPECT_EQ(owner.pTaskInfo->pRoot->fpSet.resetStateFn(owner.pTaskInfo->pRoot), TSDB_CODE_SUCCESS);
}

TEST_F(ExternalWindowOperatorTest, BroadcastZeroRowsMarksLoadedAndNeverRescans) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.initExplicit({}, "count", false, false, 2), TSDB_CODE_SUCCESS);

  std::vector<int64_t> values;
  ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
  EXPECT_EQ(values, (std::vector<int64_t>{0, 0}));
  ASSERT_NE(owner.input(), nullptr);
  EXPECT_EQ(owner.input()->getNextCalls, 1);

  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(owner.pTaskInfo->pRoot->fpSet.getNextFn(owner.pTaskInfo->pRoot, &pBlock), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pBlock, nullptr);
  EXPECT_EQ(owner.input()->getNextCalls, 1);
}

TEST_F(ExternalWindowOperatorTest, BroadcastMultipleInputBlocksReplayInOriginalOrder) {
  SBlockID                                  firstId = {11, 111, 0, 0};
  SBlockID                                  secondId = {22, 222, 0, 0};
  SBlockID                                  thirdId = {33, 333, 0, 0};
  std::vector<ExternalWindowInputBlockSpec> blocks = {
      makeExternalWindowBlockSpec({7, 7}, firstId, 1000, {1000, 1001}, 10, 2, 3, {false, false}, {"dup-a", "dup-b"},
                                  {std::string("A\0\0\0", 4), std::string("B\0\0\0", 4)}),
      makeExternalWindowBlockSpec({9}, secondId, 1010, {1010, 1010}, 20, 4, 5, {true}, {"null"},
                                  {std::string("\x2d\x4e\0\0", 4)}),
      makeExternalWindowBlockSpec({11, 7}, thirdId, 1020, {1020, 1021}, 30, 6, 7, {false, false}, {"tail-a", "tail-b"},
                                  {std::string("C\0\0\0", 4), std::string("D\0\0\0", 4)}),
  };
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.initExplicit(blocks, nullptr, true, false, 2, {}, false, false, 8, true, 8, true), TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, 1, -1, -1, &rows, -1, -1, 2), TSDB_CODE_SUCCESS);
  ASSERT_EQ(rows.size(), 10);
  const std::vector<int32_t>     expectedValues = {7, 7, 0, 11, 7};
  const std::vector<bool>        expectedNulls = {false, false, true, false, false};
  const std::vector<std::string> expectedBinary = {"dup-a", "dup-b", "null", "tail-a", "tail-b"};
  const std::vector<std::string> expectedNchar = {std::string("A\0\0\0", 4), std::string("B\0\0\0", 4),
                                                  std::string("\x2d\x4e\0\0", 4), std::string("C\0\0\0", 4),
                                                  std::string("D\0\0\0", 4)};
  for (int32_t window = 0; window < 2; ++window) {
    for (size_t row = 0; row < expectedValues.size(); ++row) {
      const auto &actual = rows[window * expectedValues.size() + row];
      SCOPED_TRACE(::testing::Message() << "window=" << window << " row=" << row << " value=" << actual.value
                                        << " binary=" << actual.binary);
      EXPECT_EQ(actual.windowIndex, window);
      EXPECT_EQ(actual.valueIsNull, expectedNulls[row]);
      if (!expectedNulls[row]) EXPECT_EQ(actual.value, expectedValues[row]);
      EXPECT_EQ(actual.binary, expectedBinary[row]);
      EXPECT_EQ(actual.nchar, expectedNchar[row]);
    }
  }
  EXPECT_EQ(owner.input()->getNextCalls, 4);
}

TEST_F(ExternalWindowOperatorTest, BroadcastSpillPreservesFullNonzeroBlockIdentity) {
  constexpr int32_t        kRows = 11200;
  constexpr int32_t        kBinaryBytes = 1024;
  SBlockID                 id = {701, 801, 101, 101};
  std::vector<int32_t>     values;
  std::vector<std::string> binaryValues;
  values.reserve(kRows);
  binaryValues.reserve(kRows);
  for (int32_t row = 0; row < kRows; ++row) {
    values.push_back(row);
    std::string bytes(kBinaryBytes, static_cast<char>('a' + row % 23));
    memcpy(&bytes[0], &row, sizeof(row));
    binaryValues.push_back(std::move(bytes));
  }
  auto block =
      makeExternalWindowBlockSpec(std::move(values), id, 1000, {900, 12000}, 71, 9, 13, {}, std::move(binaryValues));
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.initExplicit({std::move(block)}, nullptr, true, false, 1, {101}, false, false, kBinaryBytes, true),
            TSDB_CODE_SUCCESS);
  owner.pTaskInfo->pRoot->resultInfo.threshold = 1;

  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, 1, -1, -1, &rows), TSDB_CODE_SUCCESS);
  ASSERT_EQ(rows.size(), kRows);
  EXPECT_EQ(rows.front().value, 0);
  EXPECT_EQ(rows.back().value, kRows - 1);
  EXPECT_EQ(rows.front().id.uid, 0);
  EXPECT_EQ(rows.front().id.blockId, kExtWinFactoryOutputBlockId);
  EXPECT_EQ(rows.front().id.groupId, 101);
  EXPECT_EQ(rows.front().id.baseGId, 101);
  EXPECT_EQ(memcmp(&rows.front().id, &rows.back().id, sizeof(SBlockID)), 0);

  int32_t     pageCount = 0;
  SBlockID    storedId = {0};
  STimeWindow storedWindow = {0};
  int64_t     storedVersion = 0;
  int16_t     storedDataLoad = 0;
  uint8_t     storedScanFlag = 0;
  ASSERT_EQ(extWinTestGetBroadcastPageMeta(owner.pTaskInfo->pRoot, 101, 0, 0, &pageCount, &storedId, &storedWindow,
                                           &storedVersion, &storedDataLoad, &storedScanFlag),
            TSDB_CODE_SUCCESS);
  ASSERT_GT(pageCount, 1);
  for (int32_t page = 0; page < pageCount; ++page) {
    SCOPED_TRACE(::testing::Message() << "page=" << page);
    ASSERT_EQ(extWinTestGetBroadcastPageMeta(owner.pTaskInfo->pRoot, 101, 0, page, &pageCount, &storedId, &storedWindow,
                                             &storedVersion, &storedDataLoad, &storedScanFlag),
              TSDB_CODE_SUCCESS);
    EXPECT_EQ(memcmp(&storedId, &id, sizeof(id)), 0);
    EXPECT_EQ(storedWindow.skey, 900);
    EXPECT_EQ(storedWindow.ekey, 12000);
    EXPECT_EQ(storedVersion, 71);
    EXPECT_EQ(storedDataLoad, 9);
    EXPECT_EQ(storedScanFlag, 13);

    int32_t     observedCount = 0;
    int32_t     observedPage = -1;
    SBlockID    observedId = {0};
    STimeWindow observedWindow = {0};
    int64_t     observedVersion = 0;
    int16_t     observedDataLoad = 0;
    uint8_t     observedScanFlag = 0;
    ASSERT_EQ(extWinTestGetBroadcastReplayObservation(owner.pTaskInfo->pRoot, page, &observedCount, &observedPage,
                                                      &observedId, &observedWindow, &observedVersion, &observedDataLoad,
                                                      &observedScanFlag),
              TSDB_CODE_SUCCESS);
    EXPECT_EQ(observedCount, pageCount);
    EXPECT_EQ(observedPage, page);
    EXPECT_EQ(memcmp(&observedId, &id, sizeof(id)), 0);
    EXPECT_EQ(observedWindow.skey, 900);
    EXPECT_EQ(observedWindow.ekey, 12000);
    EXPECT_EQ(observedVersion, 71);
    EXPECT_EQ(observedDataLoad, 9);
    EXPECT_EQ(observedScanFlag, 13);
  }
}

TEST_F(ExternalWindowOperatorTest, BroadcastSortsTriggerAndCalcGroups) {
  const std::vector<std::pair<uint64_t, uint64_t>> activations = {{101, 1002}, {101, 1001}, {202, 2002}, {202, 2001}};
  std::vector<ExternalWindowInputBlockSpec>        blocks;
  for (size_t i = 0; i < activations.size(); ++i) {
    SBlockID id = {700 + i, 800 + static_cast<int64_t>(i), activations[i].second, activations[i].first};
    blocks.push_back(
        makeExternalWindowBlockSpec({static_cast<int32_t>(11 + i)}, id, 1000 + i, {1000, 1000}, 10 + i, 1, MAIN_SCAN));
  }
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.initExplicit(blocks, "sum", false, false, 1, {101, 202}, false, true), TSDB_CODE_SUCCESS);
  int64_t expectedPayload = 0;
  ASSERT_NE(owner.input(), nullptr);
  for (int32_t i = 0; i < taosArrayGetSize(owner.input()->pBlocks); ++i) {
    auto **ppInput = static_cast<SSDataBlock **>(taosArrayGet(owner.input()->pBlocks, i));
    ASSERT_NE(ppInput, nullptr);
    ASSERT_NE(*ppInput, nullptr);
    expectedPayload +=
        blockDataGetSize(*ppInput) + sizeof(int32_t) + taosArrayGetSize((*ppInput)->pDataBlock) * sizeof(int32_t);
  }
  std::vector<ExternalWindowPublication> publications;
  stub.set(winInputPoolCreate, spyExternalWindowPoolCreate);
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  stub.reset(winInputPoolCreate);
  EXPECT_EQ(gExternalWindowCreateSpy.calls, 1);
  EXPECT_EQ(externalWindowPublicationRuns(publications), (std::vector<uint64_t>{101, 202}));
  std::vector<std::pair<uint64_t, int64_t>> actual;
  for (const auto &publication : publications) {
    EXPECT_NE(publication.gid, 0);
    for (int64_t value : publication.values) actual.emplace_back(publication.gid, value);
  }
  EXPECT_EQ(actual, (std::vector<std::pair<uint64_t, int64_t>>{{101, 12}, {101, 11}, {202, 14}, {202, 13}}));
  int64_t groupScanBlocks = 0, groupScanRows = 0, groupReplayBlocks = 0, groupReplayRows = 0, groupBudget = 0,
          groupPeak = 0, groupFlush = 0, groupLoad = 0, groupPredicate = 0;
  ASSERT_EQ(
      extWinTestGetBroadcastStat(owner.pTaskInfo->pRoot, &groupScanBlocks, &groupScanRows, &groupReplayBlocks,
                                 &groupReplayRows, &groupBudget, &groupPeak, &groupFlush, &groupLoad, &groupPredicate),
      TSDB_CODE_SUCCESS);
  EXPECT_EQ(groupScanBlocks, 4);
  EXPECT_EQ(groupScanRows, 4);
  EXPECT_EQ(groupReplayBlocks, 4);
  EXPECT_EQ(groupReplayRows, 4);
  EXPECT_EQ(groupBudget, 10LL * 1024 * 1024);
  EXPECT_EQ(groupPeak, expectedPayload);

  ExternalWindowTwoLeafOwner siblings;
  ASSERT_EQ(siblings.init(31, 41, 2, EXT_WIN_INPUT_BROADCAST), TSDB_CODE_SUCCESS);
  int64_t aScanBlocks = 0, aScanRows = 0, aReplayBlocks = 0, aReplayRows = 0, aBudget = 0, aPeak = 0, aFlush = 0,
          aLoad = 0, aPredicate = 0;
  int64_t bScanBlocks = 0, bScanRows = 0, bReplayBlocks = 0, bReplayRows = 0, bBudget = 0, bPeak = 0, bFlush = 0,
          bLoad = 0, bPredicate = 0;
  ASSERT_FALSE(drainLeaf(siblings.pLeafA).empty());
  ASSERT_EQ(extWinTestGetBroadcastStat(siblings.pLeafA, &aScanBlocks, &aScanRows, &aReplayBlocks, &aReplayRows,
                                       &aBudget, &aPeak, &aFlush, &aLoad, &aPredicate),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(extWinTestGetBroadcastStat(siblings.pLeafB, &bScanBlocks, &bScanRows, &bReplayBlocks, &bReplayRows,
                                       &bBudget, &bPeak, &bFlush, &bLoad, &bPredicate),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(aScanBlocks, 1);
  EXPECT_EQ(aScanRows, 1);
  EXPECT_EQ(aReplayBlocks, 2);
  EXPECT_EQ(aReplayRows, 2);
  EXPECT_EQ(aBudget, 10LL * 1024 * 1024);
  EXPECT_GT(aPeak, 0);
  EXPECT_EQ(bScanBlocks, 0);
  EXPECT_EQ(bScanRows, 0);
  EXPECT_EQ(bReplayBlocks, 0);
  EXPECT_EQ(bReplayRows, 0);
  EXPECT_EQ(bBudget, 0);
  EXPECT_EQ(bPeak, 0);
  const int64_t savedABudget = aBudget;
  const int64_t savedAPeak = aPeak;
  ASSERT_FALSE(drainLeaf(siblings.pLeafB).empty());
  ASSERT_EQ(extWinTestGetBroadcastStat(siblings.pLeafA, &aScanBlocks, &aScanRows, &aReplayBlocks, &aReplayRows,
                                       &aBudget, &aPeak, &aFlush, &aLoad, &aPredicate),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(extWinTestGetBroadcastStat(siblings.pLeafB, &bScanBlocks, &bScanRows, &bReplayBlocks, &bReplayRows,
                                       &bBudget, &bPeak, &bFlush, &bLoad, &bPredicate),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(aScanBlocks, 1);
  EXPECT_EQ(aReplayBlocks, 2);
  EXPECT_EQ(aBudget, savedABudget);
  EXPECT_EQ(aPeak, savedAPeak);
  EXPECT_EQ(bScanBlocks, 1);
  EXPECT_EQ(bScanRows, 1);
  EXPECT_EQ(bReplayBlocks, 2);
  EXPECT_EQ(bReplayRows, 2);
  EXPECT_EQ(bBudget, 10LL * 1024 * 1024);
  EXPECT_GT(bPeak, 0);
}

TEST_F(ExternalWindowOperatorTest, BroadcastGroupsShareOneBudgetAndSpillAcrossGroups) {
  constexpr int32_t        kRowsPerGroup = 6000;
  constexpr int32_t        kBinaryBytes = 1024;
  std::vector<int32_t>     firstValues(kRowsPerGroup);
  std::vector<int32_t>     secondValues(kRowsPerGroup);
  std::vector<std::string> firstBinary(kRowsPerGroup);
  std::vector<std::string> secondBinary(kRowsPerGroup);
  for (int32_t row = 0; row < kRowsPerGroup; ++row) {
    firstValues[row] = 1000 + row;
    secondValues[row] = 20000 + row;
    firstBinary[row] = std::string(kBinaryBytes, static_cast<char>('a' + row % 23));
    secondBinary[row] = std::string(kBinaryBytes, static_cast<char>('a' + row % 23));
    memcpy(&firstBinary[row][0], &firstValues[row], sizeof(firstValues[row]));
    memcpy(&secondBinary[row][0], &secondValues[row], sizeof(secondValues[row]));
  }

  SBlockID firstId = {701, 801, 1001, 101};
  SBlockID secondId = {702, 802, 2001, 202};
  auto first = makeExternalWindowBlockSpec(std::move(firstValues), firstId, 1000, {1000, 1000 + kRowsPerGroup - 1}, 1,
                                           1, MAIN_SCAN, {}, std::move(firstBinary));
  auto second =
      makeExternalWindowBlockSpec(std::move(secondValues), secondId, 20000, {20000, 20000 + kRowsPerGroup - 1}, 1, 1,
                                  MAIN_SCAN, {}, std::move(secondBinary));

  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.initExplicit({std::move(first), std::move(second)}, nullptr, true, false, 1, {101, 202}, false, true,
                               kBinaryBytes, true),
            TSDB_CODE_SUCCESS);
  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);

  int64_t scanBlocks = 0, scanRows = 0, replayBlocks = 0, replayRows = 0;
  int64_t budget = 0, peak = 0, flush = 0, load = 0, predicates = 0;
  ASSERT_EQ(extWinTestGetBroadcastStat(owner.pTaskInfo->pRoot, &scanBlocks, &scanRows, &replayBlocks, &replayRows,
                                       &budget, &peak, &flush, &load, &predicates),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(budget, 10LL * 1024 * 1024);
  EXPECT_EQ(peak, budget);
  EXPECT_GT(flush, 0);
  EXPECT_GT(load, 0);
  EXPECT_EQ(externalWindowPublicationRuns(publications), (std::vector<uint64_t>{101, 202}));
}

TEST_F(ExternalWindowOperatorTest, BroadcastWorksForScalarAggregateAndIndefiniteRows) {
  SBlockID                                  id1 = {1, 10, 0, 0};
  SBlockID                                  id2 = {2, 20, 0, 0};
  std::vector<ExternalWindowInputBlockSpec> blocks = {
      makeExternalWindowBlockSpec({3}, id1, 1000, {1000, 1000}, 1, 1, MAIN_SCAN),
      makeExternalWindowBlockSpec({4}, id2, 1001, {1001, 1001}, 2, 1, MAIN_SCAN)};
  {
    ExternalWindowFactoryOwner baseline;
    ASSERT_EQ(baseline.init({1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "csum", false, true, 1, false, false, false,
                            {}, false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {3, 4}),
              TSDB_CODE_SUCCESS);
    std::vector<int32_t> baselineValues;
    ASSERT_EQ(drainExternalWindow(&baseline, &baselineValues), TSDB_CODE_SUCCESS);
    EXPECT_EQ(baselineValues, (std::vector<int32_t>{3, 4}));

    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.initExplicit(blocks, nullptr, true, false, 2), TSDB_CODE_SUCCESS);
    std::vector<ExternalWindowBroadcastRow> rows;
    ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
    std::vector<int32_t> values;
    for (const auto &row : rows) values.push_back(row.value);
    EXPECT_EQ(values, (std::vector<int32_t>{3, 4, 3, 4}));
  }
  {
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.initExplicit(blocks, "sum", false, false, 2), TSDB_CODE_SUCCESS);
    std::vector<int64_t> values;
    ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
    EXPECT_EQ(values, (std::vector<int64_t>{7, 7}));
  }
  {
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.initExplicit(blocks, "csum", false, true, 2), TSDB_CODE_SUCCESS);
    std::vector<ExternalWindowBroadcastRow> rows;
    ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
    std::vector<int32_t> values;
    for (const auto &row : rows) values.push_back(row.value);
    EXPECT_EQ(values, (std::vector<int32_t>{3, 4, 3, 4}));
  }
}

TEST_F(ExternalWindowOperatorTest, BroadcastStoreCreateFailureFailsRequest) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {}, false,
                 {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {}, EXT_WIN_INPUT_BROADCAST),
      TSDB_CODE_SUCCESS);
  stub.set(winInputPoolCreate, failExternalWindowPoolCreate);
  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), kExternalWindowCreateError);
  EXPECT_EQ(pBlock, nullptr);
  stub.reset(winInputPoolCreate);
  EXPECT_EQ(owner.pTaskInfo->pRoot->fpSet.resetStateFn(owner.pTaskInfo->pRoot), TSDB_CODE_SUCCESS);
}

TEST_F(ExternalWindowOperatorTest, BroadcastLogicalStoreCreateFailureDestroysPoolOnReset) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {}, false,
                 {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {}, EXT_WIN_INPUT_BROADCAST),
      TSDB_CODE_SUCCESS);
  stub.set(winInputStoreCreate, failExternalWindowStoreCreate);
  stub.set(winInputPoolDestroy, spyExternalWindowPoolDestroy);
  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), kExternalWindowCreateError);
  EXPECT_EQ(pBlock, nullptr);
  stub.reset(winInputStoreCreate);
  EXPECT_EQ(owner.pTaskInfo->pRoot->fpSet.resetStateFn(owner.pTaskInfo->pRoot), TSDB_CODE_SUCCESS);
  EXPECT_EQ(gExternalWindowPoolDestroyCalls, 1);
  stub.reset(winInputPoolDestroy);
}

TEST_F(ExternalWindowOperatorTest, PartialBroadcastDrainDestroyReleasesEveryPage) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({11200}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 3, false, false, false,
                       {}, true, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {100}, {},
                       EXT_WIN_INPUT_BROADCAST, 1024, true),
            TSDB_CODE_SUCCESS);
  owner.pTaskInfo->pRoot->resultInfo.threshold = 1;
  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  qDestroyTask(owner.pTaskInfo);
  owner.pTaskInfo = nullptr;

  ExternalWindowFactoryOwner resetOwner;
  ASSERT_EQ(resetOwner.init({2}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 2, false, false, false,
                            {}, false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {5}, {},
                            EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(streamExecuteTask(resetOwner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_EQ(resetOwner.pTaskInfo->pRoot->fpSet.resetStateFn(resetOwner.pTaskInfo->pRoot), TSDB_CODE_SUCCESS);
  int64_t scanBlocks = -1, scanRows = -1, replayBlocks = -1, replayRows = -1, actualBudget = -1, payloadPeak = -1,
          flushBytes = -1, loadBytes = -1, predicateEvals = -1;
  ASSERT_EQ(extWinTestGetBroadcastStat(resetOwner.pTaskInfo->pRoot, &scanBlocks, &scanRows, &replayBlocks, &replayRows,
                                       &actualBudget, &payloadPeak, &flushBytes, &loadBytes, &predicateEvals),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(scanBlocks, 0);
  EXPECT_EQ(scanRows, 0);
  EXPECT_EQ(replayBlocks, 0);
  EXPECT_EQ(replayRows, 0);
  EXPECT_EQ(actualBudget, 0);
  EXPECT_EQ(payloadPeak, 0);
  EXPECT_EQ(flushBytes, 0);
  EXPECT_EQ(loadBytes, 0);
  EXPECT_EQ(predicateEvals, 0);
  ASSERT_EQ(resetOwner.pTaskInfo->pRoot->fpSet.resetStateFn(resetOwner.pTaskInfo->pRoot), TSDB_CODE_SUCCESS);
}

TEST_F(ExternalWindowOperatorTest, BroadcastMetricsUseActualStoreBudget) {
  constexpr int32_t        kDeclaredBinaryBytes = 3 * 1024 * 1024;
  constexpr int32_t        kRows = 200;
  std::vector<int32_t>     values(kRows, 7);
  std::vector<std::string> binaryValues(kRows, std::string(60000, 'x'));
  SBlockID                 id = {701, 801, 101, 101};
  auto block = makeExternalWindowBlockSpec(std::move(values), id, 1000, {1000, 1199}, 1, 1, MAIN_SCAN, {},
                                           std::move(binaryValues));
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.initExplicit({std::move(block)}, nullptr, true, false, 1, {101}, false, false, kDeclaredBinaryBytes, false),
      TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.input(), nullptr);
  auto **ppInputBlock = static_cast<SSDataBlock **>(taosArrayGet(owner.input()->pBlocks, 0));
  ASSERT_NE(ppInputBlock, nullptr);
  ASSERT_NE(*ppInputBlock, nullptr);
  uint32_t pageSize = 0;
  int64_t  defaultBudget = 0;
  ASSERT_EQ(getBufferPgSize((*ppInputBlock)->info.rowSize, &pageSize, &defaultBudget), TSDB_CODE_SUCCESS);
  const int64_t actualBudget = TMAX(10LL * 1024 * 1024, static_cast<int64_t>(pageSize) * 4);
  ASSERT_GT(actualBudget, 10LL * 1024 * 1024);
  const int64_t serializedBytes = blockDataGetSize(*ppInputBlock) + sizeof(int32_t) +
                                  taosArrayGetSize((*ppInputBlock)->pDataBlock) * sizeof(int32_t);
  ASSERT_GT(serializedBytes, 10LL * 1024 * 1024);
  ASSERT_LT(serializedBytes, actualBudget);

  stub.set(winInputPoolCreate, spyExternalWindowPoolCreate);
  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
  stub.reset(winInputPoolCreate);
  ASSERT_EQ(gExternalWindowCreateSpy.calls, 1);
  EXPECT_EQ(gExternalWindowCreateSpy.pageSize, pageSize);
  EXPECT_EQ(gExternalWindowCreateSpy.inMemBufSize, actualBudget);
  int64_t scanBlocks = 0, scanRows = 0, replayBlocks = 0, replayRows = 0, reportedBudget = 0, payloadPeak = 0,
          flushBytes = 0, loadBytes = 0, predicateEvals = 0;
  ASSERT_EQ(extWinTestGetBroadcastStat(owner.pTaskInfo->pRoot, &scanBlocks, &scanRows, &replayBlocks, &replayRows,
                                       &reportedBudget, &payloadPeak, &flushBytes, &loadBytes, &predicateEvals),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(reportedBudget, actualBudget);
  EXPECT_EQ(reportedBudget, gExternalWindowCreateSpy.inMemBufSize);
  EXPECT_EQ(payloadPeak, serializedBytes);
  EXPECT_EQ(predicateEvals, 0);
}

TEST_F(ExternalWindowOperatorTest, BroadcastEmptyFirstGroupDefersSharedPoolCreation) {
  auto block = makeExternalWindowBlockSpec({22}, {702, 802, 2001, 202}, 2000, {2000, 2000}, 1, 1, MAIN_SCAN);
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.initExplicit({std::move(block)}, nullptr, true, false, 1, {101, 202}, false, true),
            TSDB_CODE_SUCCESS);

  stub.set(winInputPoolCreate, spyExternalWindowPoolCreate);
  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  stub.reset(winInputPoolCreate);

  EXPECT_EQ(gExternalWindowCreateSpy.calls, 1);
  int64_t scanBlocks = 0, scanRows = 0, replayBlocks = 0, replayRows = 0;
  int64_t reportedBudget = 0, peak = 0, flush = 0, load = 0;
  int64_t predicates = 0;
  ASSERT_EQ(extWinTestGetBroadcastStat(owner.pTaskInfo->pRoot, &scanBlocks, &scanRows, &replayBlocks, &replayRows,
                                       &reportedBudget, &peak, &flush, &load, &predicates),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(reportedBudget, 10LL * 1024 * 1024);
  auto runs = externalWindowPublicationRuns(publications);
  EXPECT_NE(std::find(runs.begin(), runs.end(), 202), runs.end());
}

TEST_F(ExternalWindowOperatorTest, BroadcastRejectsPageSizeChangeAcrossGroups) {
  std::vector<ExternalWindowInputBlockSpec> blocks = {
      makeExternalWindowBlockSpec({11}, {701, 801, 1001, 101}, 1000, {1000, 1000}, 1, 1, MAIN_SCAN),
      makeExternalWindowBlockSpec({22}, {702, 802, 2001, 202}, 2000, {2000, 2000}, 1, 1, MAIN_SCAN)};
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.initExplicit(blocks, nullptr, true, false, 1, {101, 202}, false, true), TSDB_CODE_SUCCESS);

  gExternalWindowBufferPageSizeCalls = 0;
  stub.set(getBufferPgSize, changeSecondExternalWindowPageSize);
  std::vector<ExternalWindowPublication> publications;
  EXPECT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  stub.reset(getBufferPgSize);
  EXPECT_EQ(gExternalWindowBufferPageSizeCalls, 2);
}

TEST_F(ExternalWindowOperatorTest, BroadcastPayloadAccountingDoesNotRescanRegistryPerAppend) {
  constexpr int32_t                         kContexts = 32;
  std::vector<ExternalWindowInputBlockSpec> blocks;
  std::vector<uint64_t>                     runtimeGroups;
  blocks.reserve(kContexts);
  runtimeGroups.reserve(kContexts);
  for (int32_t i = 0; i < kContexts; ++i) {
    uint64_t triggerGroupId = 1000 + i;
    uint64_t calcGroupId = 2000 + i;
    SBlockID id = {3000 + static_cast<uint64_t>(i), 4000 + i, calcGroupId, triggerGroupId};
    blocks.push_back(makeExternalWindowBlockSpec({i}, id, 5000 + i, {5000 + i, 5000 + i}, 1, 1, MAIN_SCAN));
    runtimeGroups.push_back(triggerGroupId);
  }

  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.initExplicit(blocks, "sum", false, false, 1, runtimeGroups, false, true), TSDB_CODE_SUCCESS);
  stub.set(tSimpleHashIterate, countExternalWindowHashIterate);
  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  stub.reset(tSimpleHashIterate);

  EXPECT_EQ(externalWindowPublicationRuns(publications), runtimeGroups);
  EXPECT_LT(gExternalWindowHashIterateCalls, static_cast<int64_t>(kContexts) * kContexts);
}

TEST_F(ExternalWindowOperatorTest, BroadcastPayloadOverflowDestroyRestoresExactCurrent) {
  constexpr int64_t kFirstContribution = INT64_MAX - 4;
  constexpr int64_t kSecondContribution = 10;
  int64_t           currentAfterFirst = -1;
  int64_t           currentAfterSecond = -1;
  int64_t           currentAfterSecondDestroy = -1;
  int64_t           peakAfterSecondDestroy = -1;

  ASSERT_EQ(
      extWinTestRunBroadcastPayloadTransition(kFirstContribution, kSecondContribution, &currentAfterFirst,
                                              &currentAfterSecond, &currentAfterSecondDestroy, &peakAfterSecondDestroy),
      TSDB_CODE_SUCCESS);
  EXPECT_EQ(currentAfterFirst, kFirstContribution);
  EXPECT_EQ(currentAfterSecond, INT64_MAX);
  EXPECT_EQ(currentAfterSecondDestroy, kFirstContribution);
  EXPECT_EQ(peakAfterSecondDestroy, INT64_MAX);
}

TEST_F(ExternalWindowOperatorTest, BroadcastStateReplacementReusesPoolRegisteredPages) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({20}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {},
                       false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {11}, {},
                       EXT_WIN_INPUT_BROADCAST, 1024, true),
            TSDB_CODE_SUCCESS);
  bool         finished = false;
  SSDataBlock *pOutput = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pOutput, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pOutput, nullptr);
  ASSERT_NE(owner.input(), nullptr);
  auto **ppInput = static_cast<SSDataBlock **>(taosArrayGet(owner.input()->pBlocks, 0));
  ASSERT_NE(ppInput, nullptr);
  ASSERT_NE(*ppInput, nullptr);

  int32_t initialRegisteredPages = 0;
  ASSERT_EQ(extWinTestGetBroadcastRegisteredPageCount(owner.pTaskInfo->pRoot, &initialRegisteredPages),
            TSDB_CODE_SUCCESS);
  ASSERT_GT(initialRegisteredPages, 0);
  for (int32_t replacement = 0; replacement < 3; ++replacement) {
    int32_t registeredPages = 0;
    ASSERT_EQ(extWinTestReplaceActiveBroadcastState(owner.pTaskInfo->pRoot, *ppInput, &registeredPages),
              TSDB_CODE_SUCCESS);
    EXPECT_EQ(registeredPages, initialRegisteredPages);
  }
}

TEST_F(ExternalWindowOperatorTest, BroadcastStateRecycleDoesNotAllocateFreeListNodes) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({20}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {},
                       false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {11}, {},
                       EXT_WIN_INPUT_BROADCAST, 1024, true),
            TSDB_CODE_SUCCESS);
  bool         finished = false;
  SSDataBlock *pOutput = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pOutput, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pOutput, nullptr);
  ASSERT_NE(owner.input(), nullptr);
  auto **ppInput = static_cast<SSDataBlock **>(taosArrayGet(owner.input()->pBlocks, 0));
  ASSERT_NE(ppInput, nullptr);
  ASSERT_NE(*ppInput, nullptr);

  int32_t initialRegisteredPages = 0;
  ASSERT_EQ(extWinTestGetBroadcastRegisteredPageCount(owner.pTaskInfo->pRoot, &initialRegisteredPages),
            TSDB_CODE_SUCCESS);
  ASSERT_GT(initialRegisteredPages, 0);

  stub.set(tdListAppend, failExternalWindowFreePageListAppend);
  int32_t registeredAfterInjectedReplacement = 0;
  int32_t code =
      extWinTestReplaceActiveBroadcastState(owner.pTaskInfo->pRoot, *ppInput, &registeredAfterInjectedReplacement);
  stub.reset(tdListAppend);

  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(gExternalWindowFreePageListAppendCalls, 0);
  EXPECT_EQ(registeredAfterInjectedReplacement, initialRegisteredPages);

  int32_t registeredAfterNextReplacement = 0;
  ASSERT_EQ(extWinTestReplaceActiveBroadcastState(owner.pTaskInfo->pRoot, *ppInput, &registeredAfterNextReplacement),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(registeredAfterNextReplacement, initialRegisteredPages);
}

TEST_F(ExternalWindowOperatorTest, FalseWindowConditionKeepsEmptySettlement) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 2, false, false, false, {}, false,
                 {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {}, EXT_WIN_INPUT_BROADCAST,
                 0, false, false, {ExternalWindowConditionKind::kTwstartLowerThan, 0, -1}),
      TSDB_CODE_SUCCESS);

  std::vector<int64_t> values;
  ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
  EXPECT_EQ(values, (std::vector<int64_t>{0, 0}));
}

TEST_F(ExternalWindowOperatorTest, WindowConditionErrorFailsRequest) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {}, false,
                 {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {}, EXT_WIN_INPUT_BROADCAST,
                 0, false, false, {ExternalWindowConditionKind::kTwstartLowerThan, INT64_MAX, 100}),
      TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  EXPECT_EQ(pBlock, nullptr);
}

TEST_F(ExternalWindowOperatorTest, WindowBoundEmptyInputStillEvaluatesPredicateError) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 1, false, false, false, {},
                       false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {},
                       EXT_WIN_INPUT_WINDOW_BOUND, 0, false, false,
                       {ExternalWindowConditionKind::kTwstartLowerThan, INT64_MAX, 100}),
            TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  EXPECT_EQ(pBlock, nullptr);
}

TEST_F(ExternalWindowOperatorTest, WindowBoundEmptyScalarEvaluatesPredicateError) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 2, false, false, false, {},
                       false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {},
                       EXT_WIN_INPUT_WINDOW_BOUND, 0, false, false,
                       {ExternalWindowConditionKind::kTwstartLowerThan, INT64_MAX, 100}),
            TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  EXPECT_EQ(pBlock, nullptr);
}

TEST_F(ExternalWindowOperatorTest, WindowBoundEmptyIndefiniteEvaluatesPredicateError) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "csum", false, true, 2, false, false, false, {}, false, {},
                 false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {}, EXT_WIN_INPUT_WINDOW_BOUND, 0,
                 false, false, {ExternalWindowConditionKind::kTwstartLowerThan, INT64_MAX, 100}),
      TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  EXPECT_EQ(pBlock, nullptr);
}

TEST_F(ExternalWindowOperatorTest, WindowBoundEmptyNoFillAggregateEvaluatesPredicateError) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 2, false, false, false, {}, false, {},
                 false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {}, EXT_WIN_INPUT_WINDOW_BOUND, 0,
                 false, false, {ExternalWindowConditionKind::kTwstartLowerThan, INT64_MAX, 100}),
      TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  EXPECT_EQ(pBlock, nullptr);
}

TEST_F(ExternalWindowOperatorTest, WindowBoundEmptyMultiContextPreparesAndSettlesEveryWindow) {
  const std::vector<uint64_t> groups = {101, 202};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 3, false, false, false, {},
                       false, groups, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {},
                       EXT_WIN_INPUT_WINDOW_BOUND, 0, false, false,
                       {ExternalWindowConditionKind::kTwstartLowerThan, INT64_MAX, -1}),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  EXPECT_EQ(externalWindowPublicationRuns(publications), groups);

  int64_t evaluations = -1;
  ASSERT_EQ(extWinTestGetWindowConditionEvals(owner.pTaskInfo->pRoot, &evaluations), TSDB_CODE_SUCCESS);
  EXPECT_EQ(evaluations, 6);

  int32_t calcContexts = -1;
  int32_t readyContexts = -1;
  int64_t totalWindows = -1;
  int64_t settledWindows = -1;
  ASSERT_EQ(extWinTestGetWindowDecisionState(owner.pTaskInfo->pRoot, &calcContexts, &readyContexts, &totalWindows,
                                             &settledWindows),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(calcContexts, 2);
  EXPECT_EQ(readyContexts, 2);
  EXPECT_EQ(totalWindows, 6);
  EXPECT_EQ(settledWindows, 6);
}

TEST_F(ExternalWindowOperatorTest, WindowBoundNoFillCalcContextsSettleFinalWindows) {
  const std::vector<uint64_t> triggerGroups = {101, 101};
  const std::vector<uint64_t> calcGroups = {1001, 1002};
  const std::vector<uint64_t> runtimeGroups = {101};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 3, true, false, true,
                       triggerGroups, false, runtimeGroups, false, true, FILL_MODE_NONE, calcGroups,
                       OPTR_EXEC_MODEL_STREAM, false, {}, {}, EXT_WIN_INPUT_WINDOW_BOUND, 0, false, false,
                       {ExternalWindowConditionKind::kTwstartLowerThan, INT64_MAX, -1}),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);

  int64_t evaluations = -1;
  ASSERT_EQ(extWinTestGetWindowConditionEvals(owner.pTaskInfo->pRoot, &evaluations), TSDB_CODE_SUCCESS);
  EXPECT_EQ(evaluations, 6);

  int32_t calcContexts = -1;
  int32_t readyContexts = -1;
  int64_t totalWindows = -1;
  int64_t settledWindows = -1;
  ASSERT_EQ(extWinTestGetWindowDecisionState(owner.pTaskInfo->pRoot, &calcContexts, &readyContexts, &totalWindows,
                                             &settledWindows),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(calcContexts, 2);
  EXPECT_EQ(readyContexts, 2);
  EXPECT_EQ(totalWindows, 6);
  EXPECT_EQ(settledWindows, 6);
}

TEST_F(ExternalWindowOperatorTest, BroadcastEmptyInputStillEvaluatesPredicateError) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 1, false, false, false, {}, false,
                 {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {}, {}, EXT_WIN_INPUT_BROADCAST,
                 0, false, false, {ExternalWindowConditionKind::kTwstartLowerThan, INT64_MAX, 100}),
      TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  EXPECT_EQ(pBlock, nullptr);
}

TEST_F(ExternalWindowOperatorTest, WindowConditionNullMeansZeroInput) {
  for (EExtWinInputMode inputMode : {EXT_WIN_INPUT_WINDOW_BOUND, EXT_WIN_INPUT_BROADCAST}) {
    SCOPED_TRACE(inputMode == EXT_WIN_INPUT_WINDOW_BOUND ? "window-bound" : "broadcast");
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 1, false, false, false, {},
                         false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {}, inputMode,
                         0, false, false, {ExternalWindowConditionKind::kTwstartNullComparison, 0, -1}),
              TSDB_CODE_SUCCESS);

    std::vector<int64_t> values;
    ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
    EXPECT_EQ(values, (std::vector<int64_t>{0}));
  }
}

TEST_F(ExternalWindowOperatorTest, WindowConditionEvaluatesEachWindowExactlyOnce) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 3, false, false, false, {},
                       false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7, 9}, {},
                       EXT_WIN_INPUT_BROADCAST, 0, false, false,
                       {ExternalWindowConditionKind::kTwstartLowerThan, INT64_MAX, -1}),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
  EXPECT_EQ(rows.size(), 6);
  int64_t evaluations = -1;
  ASSERT_EQ(extWinTestGetWindowConditionEvals(owner.pTaskInfo->pRoot, &evaluations), TSDB_CODE_SUCCESS);
  EXPECT_EQ(evaluations, 3);
}

TEST_F(ExternalWindowOperatorTest, WindowConditionUsesAllFourWindowMetadataValues) {
  std::vector<SSTriggerCalcParam> params = {
      makeExternalWindowParam(1000, 1010, 10, 1),
      makeExternalWindowParam(1001, 1011, 20, 2),
      makeExternalWindowParam(1002, 1012, 10, 3),
  };
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 3, false, false, false, {}, false,
                 {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {42}, {}, EXT_WIN_INPUT_BROADCAST,
                 0, false, false, {ExternalWindowConditionKind::kAllMetadataNotEqualMiddle, 0, -1}, params, true),
      TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowBroadcastRow> rows;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, 1, 2, &rows, 3, 4), TSDB_CODE_SUCCESS);
  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0].windowIndex, 0);
  EXPECT_EQ(rows[0].windowStart, 1000);
  EXPECT_EQ(rows[0].windowEnd, 1010);
  EXPECT_EQ(rows[0].windowDuration, 10);
  EXPECT_EQ(rows[0].windowRowCount, 1);
  EXPECT_EQ(rows[1].windowIndex, 2);
  EXPECT_EQ(rows[1].windowStart, 1002);
  EXPECT_EQ(rows[1].windowEnd, 1012);
  EXPECT_EQ(rows[1].windowDuration, 10);
  EXPECT_EQ(rows[1].windowRowCount, 3);
}

TEST_F(ExternalWindowOperatorTest, FalseTrueTrueAggregateSettlesEveryWindowInOrder) {
  const std::vector<uint64_t> groups = {101};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 3, false, false, false,
                       groups, false, groups, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_BROADCAST, 0, false, false,
                       {ExternalWindowConditionKind::kTwstartGreaterEqual, 1001, -1}),
            TSDB_CODE_SUCCESS);
  owner.pTaskInfo->pRoot->resultInfo.threshold = 1;

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  ASSERT_EQ(publications.size(), 3);
  for (int32_t window = 0; window < 3; ++window) {
    SCOPED_TRACE(window);
    ASSERT_EQ(publications[window].windowIndexes, (std::vector<int32_t>{window}));
    ASSERT_EQ(publications[window].values.size(), 1);
    EXPECT_EQ(publications[window].values[0], window == 0 ? 0 : 1);
  }
}

TEST_F(ExternalWindowOperatorTest, FalseWindowKeepsEmptyAggregateFillAndForceOutput) {
  const std::vector<uint64_t>       groups = {101};
  const ExternalWindowConditionSpec falseCondition = {ExternalWindowConditionKind::kTwstartLowerThan, 0, -1};

  {
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 1, false, false, false,
                         groups, false, groups, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7},
                         {}, EXT_WIN_INPUT_BROADCAST, 0, false, false, falseCondition),
              TSDB_CODE_SUCCESS);
    std::vector<ExternalWindowPublication> publications;
    ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
    ASSERT_EQ(publications.size(), 1);
    EXPECT_EQ(publications[0].values, (std::vector<int64_t>{0}));
    EXPECT_EQ(publications[0].windowIndexes, (std::vector<int32_t>{0}));
  }

  {
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 1, false, false, false,
                         groups, false, groups, false, false, FILL_MODE_VALUE_F, {}, OPTR_EXEC_MODEL_STREAM, false, {7},
                         {}, EXT_WIN_INPUT_BROADCAST, 0, false, false, falseCondition),
              TSDB_CODE_SUCCESS);
    std::vector<ExternalWindowPublication> publications;
    ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
    ASSERT_EQ(publications.size(), 1);
    EXPECT_EQ(publications[0].rows, 1);
    EXPECT_EQ(publications[0].nullRows, 1);
    EXPECT_EQ(publications[0].windowIndexes, (std::vector<int32_t>{0}));
  }

  {
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 1, false, false, false,
                         groups, false, groups, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, true, {7}, {},
                         EXT_WIN_INPUT_BROADCAST, 0, false, false, falseCondition),
              TSDB_CODE_SUCCESS);
    std::vector<ExternalWindowPublication> publications;
    ASSERT_EQ(drainExternalWindowPublications(&owner, &publications, 1), TSDB_CODE_SUCCESS);
    ASSERT_EQ(publications.size(), 1);
    EXPECT_EQ(publications[0].rows, 1);
    EXPECT_EQ(publications[0].nullRows, 1);
    EXPECT_EQ(publications[0].observerValues, (std::vector<int64_t>{0}));
    EXPECT_EQ(publications[0].windowIndexes, (std::vector<int32_t>{0}));
  }
}

TEST_F(ExternalWindowOperatorTest, WindowConditionWorksForScalarAggregateAndIndefiniteRows) {
  const ExternalWindowConditionSpec firstWindowOnly = {ExternalWindowConditionKind::kTwstartLowerThan, 1001, -1};

  {
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 2, false, false, false, {},
                         false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                         EXT_WIN_INPUT_BROADCAST, 0, false, false, firstWindowOnly),
              TSDB_CODE_SUCCESS);
    std::vector<ExternalWindowBroadcastRow> rows;
    ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0].windowIndex, 0);
    EXPECT_EQ(rows[0].value, 7);
  }

  {
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 2, false, false, false, {},
                         false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                         EXT_WIN_INPUT_BROADCAST, 0, false, false, firstWindowOnly),
              TSDB_CODE_SUCCESS);
    std::vector<int64_t> values;
    ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
    EXPECT_EQ(values, (std::vector<int64_t>{1, 0}));
  }

  {
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "csum", false, true, 2, false, false, false, {},
                         false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                         EXT_WIN_INPUT_BROADCAST, 0, false, false, firstWindowOnly),
              TSDB_CODE_SUCCESS);
    std::vector<ExternalWindowBroadcastRow> rows;
    ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &rows), TSDB_CODE_SUCCESS);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0].windowIndex, 0);
    EXPECT_EQ(rows[0].value, 7);
  }
}

TEST_F(ExternalWindowOperatorTest, ResetClearsBroadcastPagesAndReplayCursor) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(
      owner.init({2}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 2, false, false, false, {}, false,
                 {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {5}, {}, EXT_WIN_INPUT_BROADCAST),
      TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowBroadcastRow> first;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &first), TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.input(), nullptr);
  EXPECT_EQ(owner.input()->getNextCalls, 2);
  ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.input()->resetCalls, 1);

  std::vector<ExternalWindowBroadcastRow> second;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &second), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.input()->getNextCalls, 4);
  ASSERT_EQ(first.size(), 4);
  ASSERT_EQ(first.size(), second.size());
  for (size_t row = 0; row < first.size(); ++row) {
    EXPECT_EQ(first[row].windowIndex, second[row].windowIndex);
    EXPECT_EQ(first[row].value, second[row].value);
    EXPECT_EQ(first[row].valueIsNull, second[row].valueIsNull);
  }
}

TEST_F(ExternalWindowOperatorTest, BroadcastGroupedResetReloadsNextRequest) {
  const std::vector<uint64_t> groups = {101, 202};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {},
                       false, groups, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {5}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowBroadcastRow> first;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &first), TSDB_CODE_SUCCESS);
  ASSERT_EQ(first.size(), groups.size());
  ASSERT_NE(owner.input(), nullptr);
  EXPECT_EQ(owner.input()->getNextCalls, 4);
  ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.input()->resetCalls, 2);

  std::vector<ExternalWindowBroadcastRow> second;
  ASSERT_EQ(drainExternalWindowBroadcast(&owner, -1, -1, -1, &second), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.input()->getNextCalls, 8);
  ASSERT_EQ(second.size(), first.size());
  for (size_t row = 0; row < first.size(); ++row) {
    EXPECT_EQ(second[row].id.groupId, first[row].id.groupId);
    EXPECT_EQ(second[row].id.baseGId, first[row].id.baseGId);
    EXPECT_EQ(second[row].windowIndex, first[row].windowIndex);
    EXPECT_EQ(second[row].value, first[row].value);
    EXPECT_EQ(second[row].valueIsNull, first[row].valueIsNull);
  }
}

class ExternalWindowStreamFactoryTest : public ::testing::Test {};

TEST_F(ExternalWindowStreamFactoryTest, SingleGroupEmptyCountPublishesAggregateRow) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 1, false, false, false, {},
                       false, {}, false, false, static_cast<EFillMode>(0)),
            TSDB_CODE_SUCCESS);

  std::vector<int64_t> values;
  ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
  EXPECT_EQ(values, (std::vector<int64_t>{0}));
}

TEST_F(ExternalWindowStreamFactoryTest, RepeatScanRewindsEveryExternalWindow) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({3, 3}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "percentile", false, false, 3, true, false,
                       false, {}, false, {}, false, false, static_cast<EFillMode>(0), {}, OPTR_EXEC_MODEL_STREAM, false,
                       {0, 0}, {PRE_SCAN, MAIN_SCAN}),
            TSDB_CODE_SUCCESS);

  std::vector<double> values;
  ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
  EXPECT_EQ(values, (std::vector<double>{0, 1, 2}));
}

TEST_F(ExternalWindowStreamFactoryTest, NormalAggregateResetRebuildsSupport) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 1, false, false, false, {},
                       false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}),
            TSDB_CODE_SUCCESS);

  std::vector<int64_t> values;
  ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
  EXPECT_EQ(values, (std::vector<int64_t>{7}));
  ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
  ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
  EXPECT_EQ(values, (std::vector<int64_t>{7}));
}

TEST_F(ExternalWindowStreamFactoryTest, HashPublishesEveryTriggerGroupExactlyOnce) {
  const std::vector<uint64_t> inputGids = {101, 202, 101, 303, 202};
  const std::vector<uint64_t> groups = {101, 202, 303};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1, 1, 1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 5, true, false,
                       false, inputGids, false, groups),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  expectExternalWindowGroupsPublishedOnce(publications, groups);
}

TEST_F(ExternalWindowStreamFactoryTest, ScalarOutputForEachGidIsContiguous) {
  const std::vector<uint64_t> inputGids = {101, 202, 101, 303, 202};
  const std::vector<uint64_t> groups = {101, 202, 303};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1, 1, 1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 5, true, false,
                       false, inputGids, false, groups),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  expectExternalWindowGroupsPublishedOnce(publications, groups);
}

TEST_F(ExternalWindowStreamFactoryTest, ScalarDrainsEveryBlockBeforeFinalizingGroup) {
  const std::vector<uint64_t> inputGids = {101, 202, 101};
  const std::vector<uint64_t> groups = {101, 202};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({4095, 1, 2}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false,
                       false, inputGids, false, groups),
            TSDB_CODE_SUCCESS);
  owner.pTaskInfo->pRoot->resultInfo.threshold = 4096;

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  expectExternalWindowGroupsPublishedOnce(publications, groups);

  int64_t group101Rows = 0;
  int32_t group101Blocks = 0;
  for (const ExternalWindowPublication &publication : publications) {
    if (publication.gid == 101) {
      group101Rows += publication.rows;
      ++group101Blocks;
    }
  }
  EXPECT_EQ(group101Rows, 4097);
  EXPECT_EQ(group101Blocks, 2);
}

TEST_F(ExternalWindowStreamFactoryTest, IndefiniteOutputForEachGidIsContiguous) {
  const std::vector<uint64_t> inputGids = {101, 202, 101, 303, 202};
  const std::vector<uint64_t> groups = {101, 202, 303};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1, 1, 1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "csum", false, true, 5, true, false,
                       false, inputGids, false, groups),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  expectExternalWindowGroupsPublishedOnce(publications, groups);
}

TEST_F(ExternalWindowStreamFactoryTest, IndefiniteDrainsEveryBlockBeforeFinalizingGroup) {
  const std::vector<uint64_t> inputGids = {101, 202, 101};
  const std::vector<uint64_t> groups = {101, 202};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({4095, 1, 2}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "csum", false, true, 1, false, false,
                       false, inputGids, false, groups),
            TSDB_CODE_SUCCESS);
  owner.pTaskInfo->pRoot->resultInfo.threshold = 4096;

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  expectExternalWindowGroupsPublishedOnce(publications, groups);

  int64_t group101Rows = 0;
  int32_t group101Blocks = 0;
  for (const ExternalWindowPublication &publication : publications) {
    if (publication.gid == 101) {
      group101Rows += publication.rows;
      ++group101Blocks;
    }
  }
  EXPECT_EQ(group101Rows, 4097);
  EXPECT_EQ(group101Blocks, 2);
}

TEST_F(ExternalWindowStreamFactoryTest, OrderedTriggerAndCalcGroupsUseOneProgressionContract) {
  const std::vector<uint64_t> triggerGids = {101, 202, 101};
  const std::vector<uint64_t> calcGids = {1001, 2001, 1002};
  const std::vector<uint64_t> groups = {101, 202};

  for (bool needGroupSort : {false, true}) {
    SCOPED_TRACE(needGroupSort ? "ordered TC-group" : "unordered TC-group");
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({1, 1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 3, true, false, false,
                         triggerGids, false, groups, needGroupSort, true, FILL_MODE_NONE, calcGids),
              TSDB_CODE_SUCCESS);

    std::vector<ExternalWindowPublication> publications;
    ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
    expectExternalWindowGroupsPublishedOnce(publications, groups);

    std::vector<int64_t> group101Values;
    std::vector<int64_t> group202Values;
    for (const ExternalWindowPublication &publication : publications) {
      std::vector<int64_t> *pValues = publication.gid == 101 ? &group101Values : &group202Values;
      pValues->insert(pValues->end(), publication.values.begin(), publication.values.end());
    }
    std::sort(group101Values.begin(), group101Values.end());
    std::sort(group202Values.begin(), group202Values.end());
    EXPECT_EQ(group101Values, (std::vector<int64_t>{0, 2}));
    EXPECT_EQ(group202Values, (std::vector<int64_t>{1}));
  }
}

void expectRepeatedCalcGroupIsScopedByTrigger(bool needGroupSort) {
  const std::vector<uint64_t> triggerGids = {101, 202, 101};
  const std::vector<uint64_t> calcGids = {1001, 1001, 1001};
  const std::vector<uint64_t> groups = {101, 202};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 3, true, false, false,
                       triggerGids, false, groups, needGroupSort, true, FILL_MODE_NONE, calcGids),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  expectExternalWindowGroupsPublishedOnce(publications, groups);

  std::vector<int64_t> group101Values;
  std::vector<int64_t> group202Values;
  for (const ExternalWindowPublication &publication : publications) {
    std::vector<int64_t> *pValues = publication.gid == 101 ? &group101Values : &group202Values;
    pValues->insert(pValues->end(), publication.values.begin(), publication.values.end());
  }
  std::sort(group101Values.begin(), group101Values.end());
  std::sort(group202Values.begin(), group202Values.end());
  EXPECT_EQ(group101Values, (std::vector<int64_t>{0, 2}));
  EXPECT_EQ(group202Values, (std::vector<int64_t>{1}));
}

TEST_F(ExternalWindowStreamFactoryTest, UnorderedRepeatedCalcGroupIsScopedByTrigger) {
  expectRepeatedCalcGroupIsScopedByTrigger(false);
}

TEST_F(ExternalWindowStreamFactoryTest, OrderedRepeatedCalcGroupIsScopedByTrigger) {
  expectRepeatedCalcGroupIsScopedByTrigger(true);
}

void expectStreamMultiGroupMissingBaseFailsClosed(ENodeType nodeType) {
  const std::vector<uint64_t> triggerGids = {0};
  const std::vector<uint64_t> calcGids = {101};
  const std::vector<uint64_t> groups = {101};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1}, nodeType, "count", false, false, 1, true, false, false, triggerGids, false, groups, false,
                       true, FILL_MODE_NONE, calcGids),
            TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_NE(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pBlock, nullptr);
}

TEST_F(ExternalWindowStreamFactoryTest, HashRejectsInferableMissingTriggerGroupId) {
  expectStreamMultiGroupMissingBaseFailsClosed(QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL);
}

TEST_F(ExternalWindowStreamFactoryTest, MergeAlignedRejectsInferableMissingTriggerGroupId) {
  expectStreamMultiGroupMissingBaseFailsClosed(QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL);
}

void expectStreamPartitionedMultiGroupMissingCalcFailsClosed(ENodeType nodeType) {
  const std::vector<uint64_t> triggerGids = {101};
  const std::vector<uint64_t> calcGids = {0};
  const std::vector<uint64_t> groups = {101};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1}, nodeType, "count", false, false, 1, true, false, false, triggerGids, false, groups, false,
                       true, FILL_MODE_NONE, calcGids),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowPublication> publications;
  EXPECT_NE(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(publications.empty());
}

TEST_F(ExternalWindowStreamFactoryTest, HashRejectsMissingPartitionCalcGroupId) {
  expectStreamPartitionedMultiGroupMissingCalcFailsClosed(QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL);
}

TEST_F(ExternalWindowStreamFactoryTest, MergeAlignedRejectsMissingPartitionCalcGroupId) {
  expectStreamPartitionedMultiGroupMissingCalcFailsClosed(QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL);
}

TEST_F(ExternalWindowStreamFactoryTest, NonPartitionedMultiGroupKeepsZeroCalcGroupCompatibility) {
  const std::vector<uint64_t> triggerGids = {101};
  const std::vector<uint64_t> calcGids = {0};
  const std::vector<uint64_t> groups = {101};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 1, true, false, false,
                       triggerGids, false, groups, false, false, FILL_MODE_NONE, calcGids),
            TSDB_CODE_SUCCESS);

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  expectExternalWindowGroupsPublishedOnce(publications, groups);
  ASSERT_EQ(publications.size(), 1);
  EXPECT_EQ(publications[0].values, (std::vector<int64_t>{0}));
}

TEST_F(ExternalWindowStreamFactoryTest, MultiGroupQualifiedAncestorRangeBindsTriggerGroupBeforeInit) {
  constexpr uint64_t                groupId = 101;
  const std::vector<uint64_t>       groups = {groupId};
  ExternalWindowFactoryOwner        owner;
  const ExternalWindowConditionSpec qualifiedRange = {ExternalWindowConditionKind::kQualifiedAncestorTimeRange, 0, 0};
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 1, false, false, false,
                       groups, false, groups, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_WINDOW_BOUND, 0, false, false, qualifiedRange),
            TSDB_CODE_SUCCESS);

  auto *pRuntime = &owner.pTaskInfo->pStreamRuntimeInfo->funcInfo;
  pRuntime->pContextPolicy = makeFetchProjectionPolicy(groupId, 0);
  pRuntime->pAncestorContext = makeFetchProjectionContext(groupId, 0);
  ASSERT_NE(pRuntime->pContextPolicy, nullptr);
  ASSERT_NE(pRuntime->pAncestorContext, nullptr);

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  ASSERT_EQ(publications.size(), 1);
  EXPECT_EQ(publications[0].gid, groupId);
  EXPECT_EQ(publications[0].blockGid, groupId);
  EXPECT_EQ(publications[0].blockBaseGid, groupId);
  EXPECT_EQ(publications[0].values, (std::vector<int64_t>{1}));
}

TEST_F(ExternalWindowStreamFactoryTest, NoFillCompletesEveryCalcGroupWindow) {
  const std::vector<uint64_t> triggerGids = {101, 101};
  const std::vector<uint64_t> calcGids = {1001, 1002};
  const std::vector<uint64_t> groups = {101};
  for (bool needGroupSort : {false, true}) {
    SCOPED_TRACE(needGroupSort ? "ordered TC-group" : "unordered TC-group");
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 3, true, false, true,
                         triggerGids, false, groups, needGroupSort, true, FILL_MODE_NONE, calcGids,
                         OPTR_EXEC_MODEL_STREAM, true),
              TSDB_CODE_SUCCESS);
    owner.pTaskInfo->pRoot->resultInfo.threshold = 1;

    std::vector<ExternalWindowPublication> publications;
    ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
    EXPECT_EQ(externalWindowPublicationRuns(publications), groups);

    ExternalWindowOutputRows rows;
    for (const ExternalWindowPublication &publication : publications) {
      EXPECT_EQ(publication.gid, 101);
      EXPECT_EQ(publication.rows, 1);
      rows.insert(rows.end(), publication.rowTuples.begin(), publication.rowTuples.end());
    }
    std::sort(rows.begin(), rows.end());
    EXPECT_EQ(rows, (ExternalWindowOutputRows{
                        {1001, 0, false, 0},
                        {1001, 1, true, 0},
                        {1001, 2, true, 0},
                        {1002, 0, true, 0},
                        {1002, 1, false, 1},
                        {1002, 2, true, 0},
                    }));
  }
}

TEST_F(ExternalWindowStreamFactoryTest, EmptyAndFillGroupsArePublishedOnce) {
  const std::vector<uint64_t> groups = {101, 202, 303};

  for (EFillMode fillMode : {FILL_MODE_NONE, FILL_MODE_NULL_F}) {
    SCOPED_TRACE(fillMode == FILL_MODE_NONE ? "empty" : "fill");
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 1, false, false, false, {},
                         false, groups, false, false, fillMode),
              TSDB_CODE_SUCCESS);

    std::vector<ExternalWindowPublication> publications;
    ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
    expectExternalWindowGroupsPublishedOnce(publications, groups);
    for (uint64_t gid : groups) {
      EXPECT_EQ(std::count_if(publications.begin(), publications.end(),
                              [gid](const ExternalWindowPublication &item) { return item.gid == gid; }),
                1);
    }
    for (const ExternalWindowPublication &publication : publications) {
      if (fillMode == FILL_MODE_NULL_F) {
        EXPECT_EQ(publication.rows, 1);
        EXPECT_EQ(publication.nullRows, 1);
        EXPECT_EQ(publication.values.size(), 1);
      } else {
        EXPECT_EQ(publication.rows, 0);
        EXPECT_EQ(publication.nullRows, 0);
        EXPECT_TRUE(publication.values.empty());
      }
    }
  }
}

TEST_F(ExternalWindowStreamFactoryTest, CreateOnlyGroupPublishesOneBoundary) {
  const std::vector<uint64_t> inputGids = {101, 303};
  const std::vector<uint64_t> groups = {101, 202, 303};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "sum", false, false, 2, true, false, false,
                       inputGids, false, groups),
            TSDB_CODE_SUCCESS);
  auto *pCreateOnly = static_cast<SSTriggerGroupCalcInfo *>(
      tSimpleHashGet(owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcInfos, &groups[1], sizeof(groups[1])));
  ASSERT_NE(pCreateOnly, nullptr);
  pCreateOnly->createTable = 1;
  owner.pTaskInfo->version = 8765;

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  expectExternalWindowGroupsPublishedOnce(publications, groups);
  auto boundary = std::find_if(publications.begin(), publications.end(), [](const ExternalWindowPublication &item) {
    return item.gid == 202 && item.rows == 0 && item.createTable == 1;
  });
  ASSERT_NE(boundary, publications.end());
  EXPECT_EQ(boundary->dataLoad, 1);
  EXPECT_EQ(boundary->version, owner.pTaskInfo->version);
  EXPECT_EQ(std::count_if(publications.begin(), publications.end(),
                          [](const ExternalWindowPublication &item) {
                            return item.gid == 202 && item.rows == 0 && item.createTable == 1;
                          }),
            1);
}

TEST_F(ExternalWindowStreamFactoryTest, MergeAlignedPublishesEveryTriggerGroupExactlyOnce) {
  const std::vector<uint64_t> inputGids = {101, 101, 202, 202, 303};
  const std::vector<uint64_t> groups = {101, 202, 303};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1, 1, 1, 1}, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, "count", false, false, 5, true,
                       false, false, inputGids, false, groups),
            TSDB_CODE_SUCCESS);
  owner.pTaskInfo->pRoot->resultInfo.threshold = 1;

  std::vector<ExternalWindowPublication> publications;
  ASSERT_EQ(drainExternalWindowPublications(&owner, &publications), TSDB_CODE_SUCCESS);
  expectExternalWindowGroupsPublishedOnce(publications, groups);
}

TEST_F(ExternalWindowStreamFactoryTest, SettledGroupIsNeverPublishedAgain) {
  const std::vector<uint64_t> inputGids = {101, 202, 101};
  const std::vector<uint64_t> groups = {101, 202};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1, 1}, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, "count", false, false, 3, true,
                       false, false, inputGids, false, groups),
            TSDB_CODE_SUCCESS);
  owner.qualifyForMultiGroupJoin();
  owner.pTaskInfo->pRoot->resultInfo.threshold = 1;

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.groupId, groups[0]);

  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, groups[0]);

  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.groupId, groups[1]);

  SSDataBlock *pRepeated = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pRepeated, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pRepeated, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pRepeated));
  EXPECT_EQ(pRepeated->info.id.baseGId, groups[1]);

  pRepeated = nullptr;
  EXPECT_NE(streamExecuteTask(owner.pTaskInfo, &pRepeated, &finished), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pRepeated, nullptr);
}

TEST_F(ExternalWindowStreamFactoryTest, ResetDestroysEveryRuntimeGroupContext) {
  const std::vector<uint64_t> inputGids = {101, 202, 101, 303, 202};
  const std::vector<uint64_t> groups = {101, 202, 303};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1, 1, 1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 5, true, false,
                       false, inputGids, false, groups),
            TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);

  for (uint64_t gid : groups) {
    auto *pGroup = static_cast<SSTriggerGroupCalcInfo *>(
        tSimpleHashGet(owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcInfos, &gid, sizeof(gid)));
    ASSERT_NE(pGroup, nullptr);
    EXPECT_EQ(pGroup->pRunnerGrpCtx, nullptr);
  }
}

TEST_F(ExternalWindowStreamFactoryTest, DestroyAfterPartialDrainReleasesEveryGroupContext) {
  const std::vector<uint64_t> inputGids = {101, 202, 101, 303, 202};
  const std::vector<uint64_t> groups = {101, 202, 303};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1, 1, 1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 5, true, false,
                       false, inputGids, false, groups),
            TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);

  SStreamRuntimeInfo *pRuntime = owner.pTaskInfo->pStreamRuntimeInfo;
  owner.pTaskInfo->ownStreamRtInfo = false;
  qDestroyTask(owner.pTaskInfo);
  owner.pTaskInfo = nullptr;

  for (uint64_t gid : groups) {
    auto *pGroup =
        static_cast<SSTriggerGroupCalcInfo *>(tSimpleHashGet(pRuntime->funcInfo.pGroupCalcInfos, &gid, sizeof(gid)));
    ASSERT_NE(pGroup, nullptr);
    EXPECT_EQ(pGroup->pRunnerGrpCtx, nullptr);
  }
  pRuntime->funcInfo.pStreamPesudoFuncVals = nullptr;
  pRuntime->funcInfo.pStreamPartColVals = nullptr;
  pRuntime->funcInfo.curGrpCalc = nullptr;
  tDestroyStRtFuncInfo(&pRuntime->funcInfo);
  taosMemoryFree(pRuntime);
}

TEST_F(ExternalWindowStreamFactoryTest, NonStreamDestroyKeepsRequestGroupContextUnowned) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 1, false, false, false,
                       {1001}, false, {}, false, true, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_BATCH),
            TSDB_CODE_SUCCESS);

  SStreamRuntimeInfo *pRuntime = owner.pTaskInfo->pStreamRuntimeInfo;
  ASSERT_NE(pRuntime, nullptr);
  uint64_t gid = 1001;
  auto    *pGroup =
      static_cast<SSTriggerGroupCalcInfo *>(tSimpleHashGet(pRuntime->funcInfo.pGroupCalcInfos, &gid, sizeof(gid)));
  ASSERT_NE(pGroup, nullptr);

  SSDataBlock *pBlock = nullptr;
  uint64_t     useconds = 0;
  ASSERT_EQ(qExecTask(owner.pTaskInfo, &pBlock, &useconds), TSDB_CODE_SUCCESS);
  ASSERT_EQ(pGroup->pRunnerGrpCtx, nullptr);

  owner.pTaskInfo->ownStreamRtInfo = false;
  qDestroyTask(owner.pTaskInfo);
  owner.pTaskInfo = nullptr;
  EXPECT_EQ(pGroup->pRunnerGrpCtx, nullptr);

  qCleanupExternalWindowGroupCtx(pRuntime->funcInfo.pGroupCalcInfos);
  pRuntime->funcInfo.pStreamPesudoFuncVals = nullptr;
  pRuntime->funcInfo.pStreamPartColVals = nullptr;
  pRuntime->funcInfo.curGrpCalc = nullptr;
  tDestroyStRtFuncInfo(&pRuntime->funcInfo);
  taosMemoryFree(pRuntime);
}

TEST_F(ExternalWindowStreamFactoryTest, CleanupInterfaceAcceptsNullAndIsIdempotent) {
  qCleanupExternalWindowGroupCtx(nullptr);

  const std::vector<uint64_t> inputGids = {101, 202};
  const std::vector<uint64_t> groups = {101, 202};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 2, true, false, false,
                       inputGids, false, groups),
            TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);

  SSHashObj *pGroups = owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcInfos;
  qCleanupExternalWindowGroupCtx(pGroups);
  qCleanupExternalWindowGroupCtx(pGroups);
  for (uint64_t gid : groups) {
    auto *pGroup = static_cast<SSTriggerGroupCalcInfo *>(tSimpleHashGet(pGroups, &gid, sizeof(gid)));
    ASSERT_NE(pGroup, nullptr);
    EXPECT_EQ(pGroup->pRunnerGrpCtx, nullptr);
  }
}

TEST(execUtilTest, externalWindowHashFactoryDrainsScalarMultiBlockAndResets) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({4096, 1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1), TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.pTaskInfo->pRoot, nullptr);
  owner.pTaskInfo->pRoot->resultInfo.threshold = 4096;

  bool         finished = false;
  SSDataBlock *pFirst = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pFirst, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pFirst, nullptr);
  ASSERT_EQ(pFirst->info.rows, 4096);
  ASSERT_GE(pFirst->info.capacity, pFirst->info.rows);
  ASSERT_NE(owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx, nullptr);

  ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx, nullptr);

  std::vector<int32_t> values;
  std::vector<int64_t> capacities;
  ASSERT_EQ(drainExternalWindow(&owner, &values, &capacities), TSDB_CODE_SUCCESS);
  ASSERT_EQ(values.size(), 4097);
  EXPECT_EQ(values.front(), 0);
  EXPECT_EQ(values.back(), 4096);
  ASSERT_EQ(capacities.size(), 2);
  EXPECT_GE(capacities[0], 4096);
  EXPECT_GE(capacities[1], 1);
}

TEST(execUtilTest, externalWindowHashFactoryForceOutputsAndDrainsIndefiniteRows) {
  {
    ExternalWindowFactoryOwner owner;
    ASSERT_EQ(owner.init({}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "count", false, false, 2), TSDB_CODE_SUCCESS);
    ASSERT_NE(owner.pTaskInfo->pRoot, nullptr);

    std::vector<int64_t> values;
    ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
    EXPECT_EQ(values, (std::vector<int64_t>{0, 0}));
  }

  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({4}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, "csum", false, true, 1), TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.pTaskInfo->pRoot, nullptr);

  std::vector<int32_t> values;
  ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
  EXPECT_EQ(values, (std::vector<int32_t>{0, 1, 3, 6}));
}

TEST(execUtilTest, externalWindowMergeAlignedBatchDrainsWithoutStreamRuntime) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({2}, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, "count", false, false, 2, true, false,
                       false, {}, false, {}, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_BATCH),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.pTaskInfo->pRoot, nullptr);
  ASSERT_NE(owner.pTaskInfo->pStreamRuntimeInfo, nullptr);

  auto *pGetParam = static_cast<SOperatorParam *>(taosMemoryCalloc(1, sizeof(SOperatorParam)));
  auto *pExtParam =
      static_cast<SExternalWindowOperatorParam *>(taosMemoryCalloc(1, sizeof(SExternalWindowOperatorParam)));
  ASSERT_NE(pGetParam, nullptr);
  ASSERT_NE(pExtParam, nullptr);
  pExtParam->ExtWins = taosArrayInit(2, sizeof(SExtWinTimeWindow));
  ASSERT_NE(pExtParam->ExtWins, nullptr);
  for (int32_t window = 0; window < 2; ++window) {
    SExtWinTimeWindow extWin = {};
    extWin.tw.skey = kExtWinFactoryInputTs + window;
    extWin.tw.ekey = extWin.tw.skey + 1;
    extWin.resWinIdx = -1;
    ASSERT_NE(taosArrayPush(pExtParam->ExtWins, &extWin), nullptr);
  }
  pGetParam->opType = QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW;
  pGetParam->value = pExtParam;
  owner.pTaskInfo->pRoot->pOperatorGetParam = pGetParam;

  struct StreamRuntimeRestore {
    SExecTaskInfo      *pTaskInfo;
    SStreamRuntimeInfo *pRuntime;
    ~StreamRuntimeRestore() { pTaskInfo->pStreamRuntimeInfo = pRuntime; }
  } restore = {owner.pTaskInfo, owner.pTaskInfo->pStreamRuntimeInfo};
  owner.pTaskInfo->pStreamRuntimeInfo = nullptr;
  ASSERT_EQ(owner.pTaskInfo->pStreamRuntimeInfo, nullptr);

  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(owner.pTaskInfo->pRoot->fpSet.getNextFn(owner.pTaskInfo->pRoot, &pBlock), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 2);

  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  ASSERT_FALSE(colDataIsNull_s(pColumn, 0));
  ASSERT_FALSE(colDataIsNull_s(pColumn, 1));
  EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(pColumn, 0)), 1);
  EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(pColumn, 1)), 1);

  pBlock = nullptr;
  ASSERT_EQ(owner.pTaskInfo->pRoot->fpSet.getNextFn(owner.pTaskInfo->pRoot, &pBlock), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pBlock, nullptr);
}

TEST(execUtilTest, externalWindowMergeAlignedFactoryDrainsAndResets) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({2}, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, "count", false, false, 2, true),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.pTaskInfo->pRoot, nullptr);
  owner.pTaskInfo->pRoot->resultInfo.threshold = 1;

  std::vector<int64_t> values;
  std::vector<int64_t> capacities;
  ASSERT_EQ(drainExternalWindow(&owner, &values, &capacities), TSDB_CODE_SUCCESS);
  EXPECT_EQ(values, (std::vector<int64_t>{1, 1}));
  ASSERT_EQ(capacities.size(), 2);
  EXPECT_GE(capacities[0], 1);
  EXPECT_GE(capacities[1], 1);

  ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx, nullptr);
  owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx = 0;
  ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
  EXPECT_EQ(values, (std::vector<int64_t>{1, 1}));
}

TEST(execUtilTest, externalWindowMergeAlignedResetRecreatesAggBuffer) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({2}, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, "count", false, false, 2, true),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.pTaskInfo->pRoot, nullptr);

  int32_t expectedPageCount = -1;
  int32_t expectedCurrentPageId = -1;
  int32_t expectedCurrentPageUsed = -1;
  for (int32_t iteration = 0; iteration < 3; ++iteration) {
    std::vector<int64_t> values;
    ASSERT_EQ(drainExternalWindow(&owner, &values), TSDB_CODE_SUCCESS);
    ASSERT_EQ(values, (std::vector<int64_t>{1, 1}));

    int32_t pageCount = -1;
    int32_t currentPageId = -1;
    int32_t currentPageUsed = -1;
    ASSERT_EQ(
        extWinTestGetMergeAlignedAggBufferState(owner.pTaskInfo->pRoot, &pageCount, &currentPageId, &currentPageUsed),
        TSDB_CODE_SUCCESS);
    ASSERT_GT(pageCount, 0);
    ASSERT_GE(currentPageId, 0);
    ASSERT_GT(currentPageUsed, 0);
    if (iteration == 0) {
      expectedPageCount = pageCount;
      expectedCurrentPageId = currentPageId;
      expectedCurrentPageUsed = currentPageUsed;
    } else {
      EXPECT_EQ(pageCount, expectedPageCount);
      EXPECT_EQ(currentPageId, expectedCurrentPageId);
      EXPECT_EQ(currentPageUsed, expectedCurrentPageUsed);
    }

    if (iteration + 1 < 3) {
      ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
      owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx = 0;
    }
  }
}

TEST(execUtilTest, externalWindowMergeAlignedScalarResetsAfterPartialMultiBlockOutput) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1, 2}, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, nullptr, true, false, 3, true),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.pTaskInfo->pRoot, nullptr);
  owner.pTaskInfo->pRoot->resultInfo.threshold = 1;

  bool         finished = false;
  SSDataBlock *pFirst = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pFirst, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pFirst, nullptr);
  ASSERT_EQ(pFirst->info.rows, 1);
  auto *pFirstCol = static_cast<SColumnInfoData *>(taosArrayGet(pFirst->pDataBlock, 0));
  ASSERT_NE(pFirstCol, nullptr);
  ASSERT_FALSE(colDataIsNull_s(pFirstCol, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pFirstCol, 0)), 0);

  ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx, nullptr);
  owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx = 0;

  std::vector<int32_t> values;
  std::vector<int64_t> capacities;
  ASSERT_EQ(drainExternalWindow(&owner, &values, &capacities), TSDB_CODE_SUCCESS);
  EXPECT_EQ(values, (std::vector<int32_t>{0, 1, 2}));
  ASSERT_EQ(capacities.size(), 2);
  EXPECT_GE(capacities[0], 1);
  EXPECT_GE(capacities[1], 2);
}

TEST(execUtilTest, externalWindowMergeAlignedFilterRebuildsWindowIndex) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({2}, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, nullptr, true, false, 2, true, true, true),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.pTaskInfo->pRoot, nullptr);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 1);
  auto *pCol = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pCol, nullptr);
  ASSERT_FALSE(colDataIsNull_s(pCol, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pCol, 0)), 1);

  SArray *pIdx = owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx;
  ASSERT_NE(pIdx, nullptr);
  ASSERT_EQ(taosArrayGetSize(pIdx), 1);
  int64_t encoded = *static_cast<int64_t *>(taosArrayGet(pIdx, 0));
  auto   *pWinIdx = reinterpret_cast<int32_t *>(&encoded);
  EXPECT_EQ(*pWinIdx, 1);
  EXPECT_EQ(*(pWinIdx + 1), 0);
}

TEST(execUtilTest, externalWindowMergeAlignedResetDropsPrefetchedGroup) {
  ExternalWindowFactoryOwner  owner;
  const std::vector<uint64_t> inputGroupIds = {101, 202};
  ASSERT_EQ(owner.init({1, 1}, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, "count", false, false, 2, true, false,
                       false, inputGroupIds),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.pTaskInfo->pRoot, nullptr);
  owner.pTaskInfo->pRoot->resultInfo.threshold = 1;

  bool         finished = false;
  SSDataBlock *pFirst = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pFirst, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pFirst, nullptr);
  ASSERT_EQ(pFirst->info.rows, 1);
  EXPECT_EQ(pFirst->info.id.groupId, inputGroupIds[0]);

  ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
  EXPECT_EQ(owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx, nullptr);
  owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx = 0;

  std::vector<int64_t>  values;
  std::vector<uint64_t> outputGroupIds;
  ASSERT_EQ(drainExternalWindow(&owner, &values, nullptr, &outputGroupIds), TSDB_CODE_SUCCESS);
  EXPECT_EQ(values, (std::vector<int64_t>{1, 1}));
  EXPECT_EQ(outputGroupIds, inputGroupIds);
}

TEST(execUtilTest, externalWindowMergeAlignedResetPreservesWindowAggregateRole) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({2}, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, "_group_const_value", false, false, 1,
                       false, false, false, {}, true),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(owner.pTaskInfo->pRoot, nullptr);

  bool         finished = false;
  SSDataBlock *pFirst = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pFirst, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pFirst, nullptr);
  ASSERT_EQ(pFirst->info.rows, 1);
  auto *pFirstCol = static_cast<SColumnInfoData *>(taosArrayGet(pFirst->pDataBlock, 0));
  ASSERT_NE(pFirstCol, nullptr);
  ASSERT_FALSE(colDataIsNull_s(pFirstCol, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pFirstCol, 0)), 1);

  ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
  owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx = 0;

  finished = false;
  SSDataBlock *pSecond = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pSecond, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pSecond, nullptr);
  ASSERT_EQ(pSecond->info.rows, 1);
  auto *pSecondCol = static_cast<SColumnInfoData *>(taosArrayGet(pSecond->pDataBlock, 0));
  ASSERT_NE(pSecondCol, nullptr);
  ASSERT_FALSE(colDataIsNull_s(pSecondCol, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pSecondCol, 0)), 1);
}

namespace {

constexpr int64_t kProjectTestInputBlockId = 9100;
constexpr int64_t kProjectTestOutputBlockId = 9101;

int64_t makeProjectTestLineageSegment(int32_t winOutIdx, int32_t rowStartIdx) {
  int64_t  value = 0;
  int32_t *pair = reinterpret_cast<int32_t *>(&value);
  pair[0] = winOutIdx;
  pair[1] = rowStartIdx;
  return value;
}

std::vector<std::pair<int32_t, int32_t>> decodeProjectTestLineage(const SArray *pLineage) {
  std::vector<std::pair<int32_t, int32_t>> decoded;
  if (pLineage == nullptr) {
    return decoded;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pLineage); ++i) {
    const auto *value = static_cast<const int64_t *>(taosArrayGet(pLineage, i));
    if (value == nullptr) {
      return {};
    }
    const auto *pair = reinterpret_cast<const int32_t *>(value);
    decoded.emplace_back(pair[0], pair[1]);
  }
  return decoded;
}

struct ProjectTestInputItem {
  SSDataBlock *pBlock = nullptr;
  SArray      *pLineage = nullptr;
  bool         publishLineage = true;
};

void destroyProjectTestInputItem(void *pItem) {
  auto *pInput = static_cast<ProjectTestInputItem *>(pItem);
  blockDataDestroy(pInput->pBlock);
  taosArrayDestroy(pInput->pLineage);
}

struct ProjectTestInput {
  SArray *pItems = nullptr;
  SArray *pPublishedLineage = nullptr;
  int32_t nextItem = 0;
};

int32_t projectTestInputNext(SOperatorInfo *pOperator, SSDataBlock **ppBlock) {
  auto *pInput = static_cast<ProjectTestInput *>(pOperator->info);
  *ppBlock = nullptr;
  if (pInput->nextItem >= taosArrayGetSize(pInput->pItems)) {
    return TSDB_CODE_SUCCESS;
  }

  auto *pItem = static_cast<ProjectTestInputItem *>(taosArrayGet(pInput->pItems, pInput->nextItem++));
  if (pItem == nullptr) {
    return terrno;
  }

  if (pItem->publishLineage) {
    taosArrayClear(pInput->pPublishedLineage);
    const int32_t segmentCount = taosArrayGetSize(pItem->pLineage);
    if (segmentCount > 0 &&
        taosArrayAddBatch(pInput->pPublishedLineage, TARRAY_DATA(pItem->pLineage), segmentCount) == nullptr) {
      return terrno;
    }
    pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx = pInput->pPublishedLineage;
  } else {
    pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx = nullptr;
  }
  *ppBlock = pItem->pBlock;
  return TSDB_CODE_SUCCESS;
}

int32_t projectTestInputReset(SOperatorInfo *pOperator) {
  auto *pInput = static_cast<ProjectTestInput *>(pOperator->info);
  pInput->nextItem = 0;
  taosArrayClear(pInput->pPublishedLineage);
  pOperator->status = OP_NOT_OPENED;
  return TSDB_CODE_SUCCESS;
}

void destroyProjectTestInput(void *pInfo) {
  auto *pInput = static_cast<ProjectTestInput *>(pInfo);
  if (pInput == nullptr) {
    return;
  }
  taosArrayDestroyEx(pInput->pItems, destroyProjectTestInputItem);
  taosArrayDestroy(pInput->pPublishedLineage);
  taosMemoryFree(pInput);
}

struct ProjectTestBlockSpec {
  uint64_t                                 gid = 0;
  uint64_t                                 baseGid = 0;
  bool                                     eog = false;
  bool                                     publishLineage = true;
  int32_t                                  dataLoad = 1;
  int8_t                                   dataType = TSDB_DATA_TYPE_INT;
  int32_t                                  dataBytes = sizeof(int32_t);
  std::vector<int32_t>                     values;
  std::vector<std::string>                 stringValues;
  std::vector<bool>                        nulls;
  std::vector<std::pair<int32_t, int32_t>> lineage;
};

int32_t makeProjectTestDataBlock(const ProjectTestBlockSpec &spec, ProjectTestInputItem *pItem) {
  pItem->publishLineage = spec.publishLineage;
  int32_t code = createDataBlock(&pItem->pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (spec.eog) {
    code = qSetMultiGroupEogBlock(pItem->pBlock, spec.gid);
  } else {
    SColumnInfoData column = createColumnInfoData(spec.dataType, spec.dataBytes, 1);
    code = blockDataAppendColInfo(pItem->pBlock, &column);
    const int32_t rows = spec.dataType == TSDB_DATA_TYPE_INT ? static_cast<int32_t>(spec.values.size())
                                                             : static_cast<int32_t>(spec.stringValues.size());
    if (code == TSDB_CODE_SUCCESS) {
      code = blockDataEnsureCapacity(pItem->pBlock, rows);
    }
    auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pItem->pBlock->pDataBlock, 0));
    for (int32_t i = 0; code == TSDB_CODE_SUCCESS && i < rows; ++i) {
      const bool isNull = !spec.nulls.empty() && spec.nulls[i];
      if (isNull) {
        code = colDataSetVal(pColumn, i, nullptr, true);
      } else if (spec.dataType == TSDB_DATA_TYPE_INT) {
        code = colDataSetVal(pColumn, i, reinterpret_cast<const char *>(&spec.values[i]), false);
      } else {
        const std::string &value = spec.stringValues[i];
        std::vector<char>  encoded(value.size() + VARSTR_HEADER_SIZE);
        varDataLen(encoded.data()) = static_cast<VarDataLenT>(value.size());
        memcpy(varDataVal(encoded.data()), value.data(), value.size());
        code = colDataSetVal(pColumn, i, encoded.data(), false);
      }
    }
    if (code == TSDB_CODE_SUCCESS) {
      pItem->pBlock->info.rows = rows;
      pItem->pBlock->info.dataLoad = spec.dataLoad;
      pItem->pBlock->info.id.groupId = spec.gid;
      pItem->pBlock->info.id.baseGId = spec.baseGid == 0 ? spec.gid : spec.baseGid;
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pItem->pLineage = taosArrayInit(static_cast<int32_t>(spec.lineage.size()), sizeof(int64_t));
  if (pItem->pLineage == nullptr) {
    return terrno;
  }
  for (const auto &lineage : spec.lineage) {
    const int64_t segment = makeProjectTestLineageSegment(lineage.first, lineage.second);
    if (taosArrayPush(pItem->pLineage, &segment) == nullptr) {
      return terrno;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t makeProjectTestDesc(const std::vector<std::pair<int8_t, int32_t>> &outputSlots, SDataBlockDescNode **ppDesc) {
  SDataBlockDescNode *pDesc = nullptr;
  SSlotDescNode      *pSlot = nullptr;
  int32_t             code = nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, reinterpret_cast<SNode **>(&pDesc));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pDesc->dataBlockId = kProjectTestOutputBlockId;
  for (const auto &slot : outputSlots) {
    pDesc->totalRowSize += slot.second;
    pDesc->outputRowSize += slot.second;
  }

  for (int32_t slot = 0; slot < static_cast<int32_t>(outputSlots.size()); ++slot) {
    code = nodesMakeNode(QUERY_NODE_SLOT_DESC, reinterpret_cast<SNode **>(&pSlot));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    pSlot->slotId = slot;
    pSlot->dataType.type = outputSlots[slot].first;
    pSlot->dataType.bytes = outputSlots[slot].second;
    pSlot->output = true;
    code = nodesListMakeStrictAppend(&pDesc->pSlots, reinterpret_cast<SNode *>(pSlot));
    pSlot = nullptr;
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  *ppDesc = pDesc;
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pSlot));
  nodesDestroyNode(reinterpret_cast<SNode *>(pDesc));
  return code;
}

int32_t makeProjectTestDesc(int8_t outputType, int32_t outputBytes, SDataBlockDescNode **ppDesc) {
  return makeProjectTestDesc({{outputType, outputBytes}}, ppDesc);
}

int32_t appendProjectTestColumnProjection(SProjectPhysiNode *pNode, int64_t inputBlockId = kProjectTestInputBlockId) {
  SColumnNode *pColumn = nullptr;
  STargetNode *pTarget = nullptr;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&pColumn));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pColumn->node.resType.type = TSDB_DATA_TYPE_INT;
  pColumn->node.resType.bytes = sizeof(int32_t);
  pColumn->dataBlockId = inputBlockId;
  pColumn->slotId = 0;
  pColumn->colId = 1;
  pColumn->colType = COLUMN_TYPE_COLUMN;

  code = nodesMakeNode(QUERY_NODE_TARGET, reinterpret_cast<SNode **>(&pTarget));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pTarget->dataBlockId = kProjectTestOutputBlockId;
  pTarget->slotId = 0;
  pTarget->pExpr = reinterpret_cast<SNode *>(pColumn);
  pColumn = nullptr;
  code = nodesListMakeStrictAppend(&pNode->pProjections, reinterpret_cast<SNode *>(pTarget));
  pTarget = nullptr;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pTarget));
  nodesDestroyNode(reinterpret_cast<SNode *>(pColumn));
  return code;
}

int32_t wrapExternalWindowWithInheritedProject(ExternalWindowFactoryOwner *pOwner, bool inheritedMultiGroupJoin,
                                               bool compareCoalesce = false) {
  SSubplan *pSubplan = nullptr;
  int32_t   code = nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, reinterpret_cast<SNode **>(&pSubplan));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pSubplan->inheritedMultiGroupJoin = inheritedMultiGroupJoin;
  pOwner->pTaskInfo->pSubplan = pSubplan;

  code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PROJECT, reinterpret_cast<SNode **>(&pOwner->pProjectNode));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = makeProjectTestDesc(TSDB_DATA_TYPE_INT, sizeof(int32_t), &pOwner->pProjectNode->node.pOutputDataBlockDesc);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = appendProjectTestColumnProjection(pOwner->pProjectNode, kExtWinFactoryOutputBlockId);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (compareCoalesce) {
    auto          *pTarget = reinterpret_cast<STargetNode *>(pOwner->pProjectNode->pProjections->pHead->pNode);
    SCaseWhenNode *pCase = nullptr;
    code = nodesMakeNode(QUERY_NODE_CASE_WHEN, reinterpret_cast<SNode **>(&pCase));
    if (code != TSDB_CODE_SUCCESS) return code;
    pCase->node.resType = reinterpret_cast<SExprNode *>(pTarget->pExpr)->resType;
    pCase->pElse = makeIntValue(0);
    SWhenThenNode *pWhen = nullptr;
    code = nodesMakeNode(QUERY_NODE_WHEN_THEN, reinterpret_cast<SNode **>(&pWhen));
    if (code != TSDB_CODE_SUCCESS) {
      nodesDestroyNode(reinterpret_cast<SNode *>(pCase));
      return code;
    }
    pWhen->pThen = pTarget->pExpr;
    pTarget->pExpr = reinterpret_cast<SNode *>(pCase);
    code = nodesListMakeStrictAppend(&pCase->pWhenThenList, reinterpret_cast<SNode *>(pWhen));
    if (code != TSDB_CODE_SUCCESS) return code;
    auto *pIsNotNull = reinterpret_cast<SOperatorNode *>(makeEqualCond(nullptr, nullptr));
    pIsNotNull->opType = OP_TYPE_IS_NOT_NULL;
    pWhen->pWhen = reinterpret_cast<SNode *>(pIsNotNull);
    code = nodesCloneNode(pWhen->pThen, &pIsNotNull->pLeft);
    if (code != TSDB_CODE_SUCCESS) return code;
    // COALESCE(value, 0) <= 1, using the CASE form produced by the parser.
    auto *pCompare = reinterpret_cast<SOperatorNode *>(makeEqualCond(pTarget->pExpr, makeIntValue(1)));
    pCompare->opType = OP_TYPE_LOWER_EQUAL;
    pTarget->pExpr = reinterpret_cast<SNode *>(pCompare);
    auto *pDesc = pOwner->pProjectNode->node.pOutputDataBlockDesc;
    auto *pSlot = reinterpret_cast<SSlotDescNode *>(pDesc->pSlots->pHead->pNode);
    pSlot->dataType = pCompare->node.resType;
    pDesc->totalRowSize = pDesc->outputRowSize = sizeof(bool);
  }
  pOwner->pProjectNode->ignoreGroupId = true;
  pOwner->pProjectNode->inputIgnoreGroup = true;
  pOwner->pNode->window.node.pParent = reinterpret_cast<SPhysiNode *>(pOwner->pProjectNode);

  SOperatorInfo *pExternalWindow = pOwner->pTaskInfo->pRoot;
  pOwner->pTaskInfo->pRoot = nullptr;
  code = createProjectOperatorInfo(pExternalWindow, pOwner->pProjectNode, pOwner->pTaskInfo, &pOwner->pTaskInfo->pRoot);
  if (code != TSDB_CODE_SUCCESS) {
    pOwner->pTaskInfo->pRoot = pExternalWindow;
  }
  return code;
}

TEST_F(ExternalWindowOperatorTest, InheritedLeafProjectPreservesStrictExternalWindowProtocol) {
  const std::vector<uint64_t> runtimeGroups = {202, 101};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {},
                       false, runtimeGroups, false, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM, false, {7}, {},
                       EXT_WIN_INPUT_BROADCAST),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(wrapExternalWindowWithInheritedProject(&owner, true), TSDB_CODE_SUCCESS);

  std::vector<ExtWinProtocolItem>                       protocol;
  std::vector<std::vector<std::pair<int32_t, int32_t>>> lineage;
  for (;;) {
    bool         finished = false;
    SSDataBlock *pBlock = nullptr;
    ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
    if (pBlock == nullptr) {
      EXPECT_TRUE(finished);
      break;
    }
    protocol.push_back({pBlock->info.id.groupId, pBlock->info.id.baseGId, static_cast<int32_t>(pBlock->info.rows),
                        pBlock->info.dataLoad, qIsMultiGroupEogBlock(pBlock)});
    lineage.push_back(decodeProjectTestLineage(owner.pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx));
  }

  expectExtWinProtocol(
      protocol, {{101, 101, 1, 1, false}, {101, 101, 0, 1, true}, {202, 202, 1, 1, false}, {202, 202, 0, 1, true}});
  ASSERT_EQ(lineage.size(), protocol.size());
  for (size_t i = 0; i < protocol.size(); ++i) {
    SCOPED_TRACE(i);
    if (protocol[i].eog) {
      EXPECT_TRUE(lineage[i].empty());
    } else {
      ASSERT_FALSE(lineage[i].empty());
      EXPECT_EQ(lineage[i].front().second, 0);
      for (size_t j = 0; j < lineage[i].size(); ++j) {
        EXPECT_GE(lineage[i][j].first, 0);
        EXPECT_GE(lineage[i][j].second, 0);
        EXPECT_LT(lineage[i][j].second, protocol[i].rows);
        if (j > 0) {
          EXPECT_LT(lineage[i][j - 1].second, lineage[i][j].second);
        }
      }
    }
  }
}

TEST_F(ExternalWindowOperatorTest, InheritedLeafWithoutRuntimeMultiGroupFailsClosed) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(wrapExternalWindowWithInheritedProject(&owner, true), TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  EXPECT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(pBlock, nullptr);
}

TEST_F(ExternalWindowOperatorTest, RuntimeMultiGroupWithoutInheritedLeafKeepsLegacyProtocol) {
  ExternalWindowFactoryOwner owner;
  ASSERT_EQ(owner.init({1}, QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL, nullptr, true, false, 1, false, false, false, {101},
                       false, {101}),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(wrapExternalWindowWithInheritedProject(&owner, false), TSDB_CODE_SUCCESS);

  bool         finished = false;
  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.id.groupId, 0);
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_EQ(pBlock->info.rows, 1);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));

  ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pBlock, nullptr);
  EXPECT_TRUE(finished);
}

TEST_F(ExternalWindowOperatorTest, LegacyProjectPreservesEmptySettlementsAcrossReset) {
  const std::vector<uint64_t> groups = {101, 202, 303, 404, 505};
  ExternalWindowFactoryOwner  owner;
  ASSERT_EQ(owner.init({1, 1, 1}, QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL, nullptr, true, false, 1, false, true,
                       false, {101, 202, 303}, false, groups, true, false, FILL_MODE_NONE, {}, OPTR_EXEC_MODEL_STREAM,
                       false, {0, 5, 7}),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(wrapExternalWindowWithInheritedProject(&owner, false, true), TSDB_CODE_SUCCESS);
  auto *pRuntime = &owner.pTaskInfo->pStreamRuntimeInfo->funcInfo;

  for (int pass = 0; pass < 2; ++pass) {
    SCOPED_TRACE(pass);
    std::vector<uint64_t> publishedGroups;
    std::vector<int8_t>   values;
    bool                  finished = false;
    for (size_t call = 0; call <= groups.size() && !finished; ++call) {
      SSDataBlock *pBlock = nullptr;
      ASSERT_EQ(streamExecuteTask(owner.pTaskInfo, &pBlock, &finished), TSDB_CODE_SUCCESS);
      if (pBlock == nullptr) {
        EXPECT_TRUE(finished);
        break;
      }
      ASSERT_FALSE(finished);
      uint64_t gid = pRuntime->groupId;
      publishedGroups.push_back(gid);
      auto *pGroup =
          static_cast<SSTriggerGroupCalcInfo *>(tSimpleHashGet(pRuntime->pGroupCalcInfos, &gid, sizeof(gid)));
      ASSERT_NE(pGroup, nullptr);
      EXPECT_EQ(pRuntime->curGrpCalc, pGroup);
      EXPECT_EQ(pRuntime->groupId, gid);
      EXPECT_EQ(pRuntime->pStreamPesudoFuncVals, pGroup->pParams);
      EXPECT_EQ(pRuntime->pStreamPartColVals, pGroup->pGroupColVals);
      EXPECT_EQ(pRuntime->createTable, &pGroup->createTable);
      bool hasData = gid == 202 || gid == 303;
      ASSERT_EQ(pBlock->info.rows, hasData ? 1 : 0);
      if (hasData) {
        auto *pCol = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
        ASSERT_NE(pCol, nullptr);
        ASSERT_EQ(pCol->info.type, TSDB_DATA_TYPE_BOOL);
        values.push_back(*reinterpret_cast<int8_t *>(colDataGetData(pCol, 0)));
      } else {
        EXPECT_EQ(pRuntime->pStreamBlkWinIdx, nullptr);
        EXPECT_EQ(pRuntime->curGrpRead, nullptr);
      }
    }
    EXPECT_TRUE(finished);
    std::sort(publishedGroups.begin(), publishedGroups.end());
    EXPECT_EQ(publishedGroups, groups);
    EXPECT_EQ(values, (std::vector<int8_t>{0, 0}));
    if (pass == 0) {
      ASSERT_EQ(streamClearStatesForOperators(owner.pTaskInfo), TSDB_CODE_SUCCESS);
    }
  }
}

int32_t appendProjectTestProcessByRowProjection(SProjectPhysiNode *pNode, const char *pFunctionName, int32_t outputSlot,
                                                int8_t inputType, int32_t inputBytes, int64_t offset = -1) {
  SColumnNode   *pColumn = nullptr;
  SFunctionNode *pFunction = nullptr;
  SValueNode    *pOffset = nullptr;
  STargetNode   *pTarget = nullptr;
  int32_t        code = nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&pColumn));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pColumn->node.resType.type = inputType;
  pColumn->node.resType.bytes = inputBytes;
  pColumn->dataBlockId = kProjectTestInputBlockId;
  pColumn->slotId = 0;
  pColumn->colId = 1;
  pColumn->colType = COLUMN_TYPE_COLUMN;

  code = nodesMakeNode(QUERY_NODE_FUNCTION, reinterpret_cast<SNode **>(&pFunction));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  tstrncpy(pFunction->functionName, pFunctionName, TSDB_FUNC_NAME_LEN);
  code = nodesListMakeStrictAppend(&pFunction->pParameterList, reinterpret_cast<SNode *>(pColumn));
  pColumn = nullptr;
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  if (offset >= 0) {
    code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pOffset));
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    pOffset->node.resType.type = TSDB_DATA_TYPE_BIGINT;
    pOffset->node.resType.bytes = sizeof(offset);
    code = nodesSetValueNodeValue(pOffset, &offset);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
    pOffset->translate = true;
    code = nodesListMakeStrictAppend(&pFunction->pParameterList, reinterpret_cast<SNode *>(pOffset));
    pOffset = nullptr;
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }
  code = fmGetFuncInfo(pFunction, nullptr, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = nodesMakeNode(QUERY_NODE_TARGET, reinterpret_cast<SNode **>(&pTarget));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pTarget->dataBlockId = kProjectTestOutputBlockId;
  pTarget->slotId = outputSlot;
  pTarget->pExpr = reinterpret_cast<SNode *>(pFunction);
  pFunction = nullptr;
  code = nodesListMakeStrictAppend(&pNode->pProjections, reinterpret_cast<SNode *>(pTarget));
  pTarget = nullptr;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pTarget));
  nodesDestroyNode(reinterpret_cast<SNode *>(pFunction));
  nodesDestroyNode(reinterpret_cast<SNode *>(pOffset));
  nodesDestroyNode(reinterpret_cast<SNode *>(pColumn));
  return code;
}

int32_t addProjectTestLimit(SProjectPhysiNode *pNode, int64_t limit, int64_t offset) {
  SLimitNode *pLimit = nullptr;
  SValueNode *pLimitValue = nullptr;
  SValueNode *pOffsetValue = nullptr;
  int32_t     code = nodesMakeNode(QUERY_NODE_LIMIT, reinterpret_cast<SNode **>(&pLimit));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pLimitValue));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pLimitValue->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pLimitValue->node.resType.bytes = sizeof(limit);
  code = nodesSetValueNodeValue(pLimitValue, &limit);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pLimitValue->translate = true;
  pLimit->limit = pLimitValue;
  pLimitValue = nullptr;

  code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pOffsetValue));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pOffsetValue->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pOffsetValue->node.resType.bytes = sizeof(offset);
  code = nodesSetValueNodeValue(pOffsetValue, &offset);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pOffsetValue->translate = true;
  pLimit->offset = pOffsetValue;
  pOffsetValue = nullptr;
  pNode->node.pLimit = reinterpret_cast<SNode *>(pLimit);
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pOffsetValue));
  nodesDestroyNode(reinterpret_cast<SNode *>(pLimitValue));
  nodesDestroyNode(reinterpret_cast<SNode *>(pLimit));
  return code;
}

int32_t addProjectTestGroupOffset(SProjectPhysiNode *pNode, int64_t offset) {
  SLimitNode *pLimit = nullptr;
  SValueNode *pOffsetValue = nullptr;
  int32_t     code = nodesMakeNode(QUERY_NODE_LIMIT, reinterpret_cast<SNode **>(&pLimit));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pOffsetValue));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pOffsetValue->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pOffsetValue->node.resType.bytes = sizeof(offset);
  code = nodesSetValueNodeValue(pOffsetValue, &offset);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pOffsetValue->translate = true;
  pLimit->offset = pOffsetValue;
  pOffsetValue = nullptr;
  pNode->node.pSlimit = reinterpret_cast<SNode *>(pLimit);
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pOffsetValue));
  nodesDestroyNode(reinterpret_cast<SNode *>(pLimit));
  return code;
}

int32_t addProjectTestGroupLimit(SProjectPhysiNode *pNode, int64_t limit) {
  SLimitNode *pLimit = nullptr;
  SValueNode *pLimitValue = nullptr;
  int32_t     code = nodesMakeNode(QUERY_NODE_LIMIT, reinterpret_cast<SNode **>(&pLimit));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pLimitValue));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pLimitValue->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pLimitValue->node.resType.bytes = sizeof(limit);
  code = nodesSetValueNodeValue(pLimitValue, &limit);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pLimitValue->translate = true;
  pLimit->limit = pLimitValue;
  pLimitValue = nullptr;
  pNode->node.pSlimit = reinterpret_cast<SNode *>(pLimit);
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pLimitValue));
  nodesDestroyNode(reinterpret_cast<SNode *>(pLimit));
  return code;
}

int32_t projectTestReportTooManyRowsByRow(SArray *pCtxArray) {
  for (int32_t i = 0; i < taosArrayGetSize(pCtxArray); ++i) {
    auto **ppCtx = static_cast<SqlFunctionCtx **>(taosArrayGet(pCtxArray, i));
    if (ppCtx == nullptr || *ppCtx == nullptr || (*ppCtx)->resultInfo == nullptr) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    (*ppCtx)->resultInfo->numOfRes = (*ppCtx)->input.numOfRows + 1;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t failProjectResetAggSup(SExprSupp *, SAggSupporter *, SExecTaskInfo *, SNodeList *, SNodeList *, size_t,
                               const char *, void *, SFunctionStateStore *) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

int32_t failProjectSetFunctionResultOutput(SOperatorInfo *, SOptrBasicInfo *, SAggSupporter *, int32_t, int32_t) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

int32_t addProjectTestFilter(SProjectPhysiNode *pNode, EOperatorType opType, int32_t bound) {
  SColumnNode   *pColumn = nullptr;
  SValueNode    *pValue = nullptr;
  SOperatorNode *pCondition = nullptr;
  int32_t        code = nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode **>(&pColumn));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pColumn->node.resType.type = TSDB_DATA_TYPE_INT;
  pColumn->node.resType.bytes = sizeof(int32_t);
  pColumn->dataBlockId = kProjectTestOutputBlockId;
  pColumn->slotId = 0;
  pColumn->colId = 1;
  pColumn->colType = COLUMN_TYPE_COLUMN;

  code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode **>(&pValue));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pValue->node.resType.type = TSDB_DATA_TYPE_INT;
  pValue->node.resType.bytes = sizeof(bound);
  code = nodesSetValueNodeValue(pValue, &bound);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pValue->translate = true;

  code = nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode **>(&pCondition));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pCondition->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pCondition->node.resType.bytes = sizeof(bool);
  pCondition->opType = opType;
  pCondition->pLeft = reinterpret_cast<SNode *>(pColumn);
  pColumn = nullptr;
  pCondition->pRight = reinterpret_cast<SNode *>(pValue);
  pValue = nullptr;
  pNode->node.pConditions = reinterpret_cast<SNode *>(pCondition);
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode *>(pCondition));
  nodesDestroyNode(reinterpret_cast<SNode *>(pValue));
  nodesDestroyNode(reinterpret_cast<SNode *>(pColumn));
  return code;
}

int32_t createProjectTestInputOperator(SExecTaskInfo *pTaskInfo, const std::vector<ProjectTestBlockSpec> &specs,
                                       SOperatorInfo **ppOperator, ProjectTestInput **ppInput) {
  auto *pOperator = static_cast<SOperatorInfo *>(taosMemoryCalloc(1, sizeof(SOperatorInfo)));
  auto *pInput = static_cast<ProjectTestInput *>(taosMemoryCalloc(1, sizeof(ProjectTestInput)));
  if (pOperator == nullptr || pInput == nullptr) {
    taosMemoryFree(pOperator);
    taosMemoryFree(pInput);
    return terrno;
  }

  pInput->pItems = taosArrayInit(static_cast<int32_t>(specs.size()), sizeof(ProjectTestInputItem));
  pInput->pPublishedLineage = taosArrayInit(2, sizeof(int64_t));
  if (pInput->pItems == nullptr || pInput->pPublishedLineage == nullptr) {
    destroyProjectTestInput(pInput);
    taosMemoryFree(pOperator);
    return terrno;
  }
  for (const auto &spec : specs) {
    ProjectTestInputItem item;
    int32_t              code = makeProjectTestDataBlock(spec, &item);
    if (code != TSDB_CODE_SUCCESS || taosArrayPush(pInput->pItems, &item) == nullptr) {
      destroyProjectTestInputItem(&item);
      destroyProjectTestInput(pInput);
      taosMemoryFree(pOperator);
      return code == TSDB_CODE_SUCCESS ? terrno : code;
    }
  }

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, projectTestInputNext, nullptr, destroyProjectTestInput,
                                         optrDefaultBufFn, nullptr, optrDefaultGetNextExtFn, nullptr);
  setOperatorResetStateFn(pOperator, projectTestInputReset);
  setOperatorInfo(pOperator, "ProjectTestInput", QUERY_NODE_PHYSICAL_PLAN_ROWSET_SOURCE, false, OP_NOT_OPENED, pInput,
                  pTaskInfo);
  *ppOperator = pOperator;
  *ppInput = pInput;
  return TSDB_CODE_SUCCESS;
}

struct ProjectProtocolItem {
  SSDataBlock *pBlock = nullptr;
  uint64_t     gid = 0;
  int32_t      rows = 0;
  bool         eog = false;
};

enum class ProjectProcessByRowKind {
  kNone,
  kInvalidRowCount,
  kRealLag,
  kRealDiff,
  kRealLead,
  kRealVarcharFillForward,
  kRealLagAndFillForward,
  kRealLagAndDiff,
  kRealLeadAndLag,
};

class ProjectMultiGroupOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(fmFuncMgtInit(), TSDB_CODE_SUCCESS);
    functionMgtInitialized_ = true;
    tstrncpy(previousTempDir_, tsTempDir, sizeof(previousTempDir_));
    previousTempSpace_ = tsTempSpace;
    tempEnvSaved_ = true;
    tstrncpy(tsTempDir, "/tmp", PATH_MAX);
    tsTempSpace.size.avail = 1024 * 1024;
    SStorageAPI storage = {};
    ASSERT_EQ(doCreateTask(1, 1, 1, OPTR_EXEC_MODEL_STREAM, &storage, &pTaskInfo_), TSDB_CODE_SUCCESS);
    pTaskInfo_->pStreamRuntimeInfo = &runtime_;
    runtime_.funcInfo.isMultiGroupCalc = true;
    runtime_.funcInfo.withExternalWindow = true;
    runtime_.funcInfo.curGrpCalc = &groupCalc_;
  }

  void TearDown() override {
    stub_.reset(resetAggSup);
    stub_.reset(setFunctionResultOutput);
    doDestroyTask(pTaskInfo_);
    pTaskInfo_ = nullptr;
    nodesDestroyNode(reinterpret_cast<SNode *>(pNode_));
    pNode_ = nullptr;
    if (tempEnvSaved_) {
      tstrncpy(tsTempDir, previousTempDir_, PATH_MAX);
      tsTempSpace = previousTempSpace_;
    }
    if (functionMgtInitialized_) {
      fmFuncMgtDestroy();
    }
  }

  void pushDataBlock(uint64_t gid, std::initializer_list<int32_t> values,
                     std::initializer_list<std::pair<int32_t, int32_t>> lineage) {
    ProjectTestBlockSpec spec;
    spec.gid = gid;
    spec.values.assign(values.begin(), values.end());
    spec.lineage.assign(lineage.begin(), lineage.end());
    specs_.push_back(spec);
  }

  void pushDataBlockWithIdentity(uint64_t baseGid, uint64_t groupId, std::initializer_list<int32_t> values,
                                 std::initializer_list<std::pair<int32_t, int32_t>> lineage, int32_t dataLoad = 1) {
    ProjectTestBlockSpec spec;
    spec.gid = groupId;
    spec.baseGid = baseGid;
    spec.dataLoad = dataLoad;
    spec.values.assign(values.begin(), values.end());
    spec.lineage.assign(lineage.begin(), lineage.end());
    specs_.push_back(spec);
  }

  void pushDataVectorBlock(uint64_t gid, std::vector<int32_t> values,
                           std::initializer_list<std::pair<int32_t, int32_t>> lineage) {
    ProjectTestBlockSpec spec;
    spec.gid = gid;
    spec.values = std::move(values);
    spec.lineage.assign(lineage.begin(), lineage.end());
    specs_.push_back(std::move(spec));
  }

  void pushLegacyDataBlock(uint64_t gid, std::initializer_list<int32_t> values) {
    ProjectTestBlockSpec spec;
    spec.gid = gid;
    spec.publishLineage = false;
    spec.values.assign(values.begin(), values.end());
    specs_.push_back(spec);
  }

  void pushVarcharDataBlock(uint64_t gid, std::vector<std::string> values, std::vector<bool> nulls,
                            std::initializer_list<std::pair<int32_t, int32_t>> lineage) {
    ProjectTestBlockSpec spec;
    spec.gid = gid;
    spec.dataType = TSDB_DATA_TYPE_VARCHAR;
    spec.dataBytes = 64;
    spec.stringValues = std::move(values);
    spec.nulls = std::move(nulls);
    spec.lineage.assign(lineage.begin(), lineage.end());
    specs_.push_back(std::move(spec));
  }

  void pushEogBlock(uint64_t gid) {
    ProjectTestBlockSpec spec;
    spec.gid = gid;
    spec.eog = true;
    specs_.push_back(spec);
  }

  void setFilter(EOperatorType opType, int32_t bound) {
    filterType_ = opType;
    filterBound_ = bound;
    hasFilter_ = true;
  }

  void setLimit(int64_t limit, int64_t offset) {
    limit_ = limit;
    offset_ = offset;
    hasLimit_ = true;
  }

  void setGroupOffset(int64_t offset) { groupOffset_ = offset; }

  void setGroupLimit(int64_t limit) { groupLimit_ = limit; }

  void setMergeDataBlocks() {
    mergeDataBlocks_ = true;
    runtime_.funcInfo.withExternalWindow = false;
  }

  void setInvalidProcessByRowResult() { processByRowKind_ = ProjectProcessByRowKind::kInvalidRowCount; }

  void setRealLagProcessByRow() { processByRowKind_ = ProjectProcessByRowKind::kRealLag; }

  void setRealDiffProcessByRow() { processByRowKind_ = ProjectProcessByRowKind::kRealDiff; }

  void setRealLeadProcessByRow(int64_t offset = 1) {
    processByRowKind_ = ProjectProcessByRowKind::kRealLead;
    lagLeadOffset_ = offset;
  }

  void setRealVarcharFillForwardProcessByRow() { processByRowKind_ = ProjectProcessByRowKind::kRealVarcharFillForward; }

  void setRealLagAndFillForwardProcessByRow() { processByRowKind_ = ProjectProcessByRowKind::kRealLagAndFillForward; }

  void setRealLagAndDiffProcessByRow() { processByRowKind_ = ProjectProcessByRowKind::kRealLagAndDiff; }

  void setRealLeadAndLagProcessByRow(int64_t offset = 1) {
    processByRowKind_ = ProjectProcessByRowKind::kRealLeadAndLag;
    lagLeadOffset_ = offset;
  }

  void setLegacyMode() { staticQualified_ = false; }

  int32_t ensureOperator() {
    if (pTaskInfo_->pRoot != nullptr) {
      return TSDB_CODE_SUCCESS;
    }

    int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PROJECT, reinterpret_cast<SNode **>(&pNode_));
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    std::vector<std::pair<int8_t, int32_t>> outputSlots = {{TSDB_DATA_TYPE_INT, sizeof(int32_t)}};
    if (processByRowKind_ == ProjectProcessByRowKind::kRealDiff) {
      outputSlots = {{TSDB_DATA_TYPE_BIGINT, sizeof(int64_t)}};
    } else if (processByRowKind_ == ProjectProcessByRowKind::kRealVarcharFillForward) {
      outputSlots = {{TSDB_DATA_TYPE_VARCHAR, 64}};
    } else if (processByRowKind_ == ProjectProcessByRowKind::kRealLagAndFillForward) {
      outputSlots = {{TSDB_DATA_TYPE_INT, sizeof(int32_t)}, {TSDB_DATA_TYPE_INT, sizeof(int32_t)}};
    } else if (processByRowKind_ == ProjectProcessByRowKind::kRealLagAndDiff) {
      outputSlots = {{TSDB_DATA_TYPE_INT, sizeof(int32_t)}, {TSDB_DATA_TYPE_BIGINT, sizeof(int64_t)}};
    } else if (processByRowKind_ == ProjectProcessByRowKind::kRealLeadAndLag) {
      outputSlots = {{TSDB_DATA_TYPE_INT, sizeof(int32_t)}, {TSDB_DATA_TYPE_INT, sizeof(int32_t)}};
    }
    code = makeProjectTestDesc(outputSlots, &pNode_->node.pOutputDataBlockDesc);
    if (code == TSDB_CODE_SUCCESS && processByRowKind_ == ProjectProcessByRowKind::kNone) {
      code = appendProjectTestColumnProjection(pNode_);
    } else if (code == TSDB_CODE_SUCCESS) {
      const char *pFirstFunction = "lag";
      int8_t      inputType = TSDB_DATA_TYPE_INT;
      int32_t     inputBytes = sizeof(int32_t);
      if (processByRowKind_ == ProjectProcessByRowKind::kRealDiff) {
        pFirstFunction = "diff";
      } else if (processByRowKind_ == ProjectProcessByRowKind::kRealLead ||
                 processByRowKind_ == ProjectProcessByRowKind::kRealLeadAndLag) {
        pFirstFunction = "lead";
      } else if (processByRowKind_ == ProjectProcessByRowKind::kRealVarcharFillForward) {
        pFirstFunction = "fill_forward";
        inputType = TSDB_DATA_TYPE_VARCHAR;
        inputBytes = 64;
      }
      const bool hasLagLead = processByRowKind_ == ProjectProcessByRowKind::kInvalidRowCount ||
                              processByRowKind_ == ProjectProcessByRowKind::kRealLag ||
                              processByRowKind_ == ProjectProcessByRowKind::kRealLead ||
                              processByRowKind_ == ProjectProcessByRowKind::kRealLagAndFillForward ||
                              processByRowKind_ == ProjectProcessByRowKind::kRealLagAndDiff ||
                              processByRowKind_ == ProjectProcessByRowKind::kRealLeadAndLag;
      code = appendProjectTestProcessByRowProjection(pNode_, pFirstFunction, 0, inputType, inputBytes,
                                                     hasLagLead ? lagLeadOffset_ : -1);
      if (code == TSDB_CODE_SUCCESS && processByRowKind_ == ProjectProcessByRowKind::kRealLagAndFillForward) {
        code = appendProjectTestProcessByRowProjection(pNode_, "fill_forward", 1, TSDB_DATA_TYPE_INT, sizeof(int32_t));
      } else if (code == TSDB_CODE_SUCCESS && processByRowKind_ == ProjectProcessByRowKind::kRealLagAndDiff) {
        code = appendProjectTestProcessByRowProjection(pNode_, "diff", 1, TSDB_DATA_TYPE_INT, sizeof(int32_t));
      } else if (code == TSDB_CODE_SUCCESS && processByRowKind_ == ProjectProcessByRowKind::kRealLeadAndLag) {
        code = appendProjectTestProcessByRowProjection(pNode_, "lag", 1, TSDB_DATA_TYPE_INT, sizeof(int32_t),
                                                       lagLeadOffset_);
      }
    }
    if (code == TSDB_CODE_SUCCESS && hasFilter_) {
      code = addProjectTestFilter(pNode_, filterType_, filterBound_);
    }
    if (code == TSDB_CODE_SUCCESS && hasLimit_) {
      code = addProjectTestLimit(pNode_, limit_, offset_);
    }
    if (code == TSDB_CODE_SUCCESS && groupOffset_ >= 0) {
      code = addProjectTestGroupOffset(pNode_, groupOffset_);
    }
    if (code == TSDB_CODE_SUCCESS && groupLimit_ >= 0) {
      code = addProjectTestGroupLimit(pNode_, groupLimit_);
    }
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (staticQualified_) {
      staticJoin_.node.type = QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN;
      staticJoin_.multiGroupJoin = true;
      pNode_->node.pParent = reinterpret_cast<SPhysiNode *>(&staticJoin_);
    }
    if (mergeDataBlocks_) {
      pNode_->ignoreGroupId = true;
      pNode_->mergeDataBlock = true;
    }

    SOperatorInfo *pDownstream = nullptr;
    code = createProjectTestInputOperator(pTaskInfo_, specs_, &pDownstream, &pInput_);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = createProjectOperatorInfo(pDownstream, pNode_, pTaskInfo_, &pTaskInfo_->pRoot);
    if (code == TSDB_CODE_SUCCESS && processByRowKind_ == ProjectProcessByRowKind::kInvalidRowCount) {
      pTaskInfo_->pRoot->exprSupp.pCtx[0].fpSet.processFuncByRow = projectTestReportTooManyRowsByRow;
    }
    return code;
  }

  SSDataBlock *pullOne(int32_t *pCode = nullptr) {
    const int32_t initCode = ensureOperator();
    if (initCode != TSDB_CODE_SUCCESS) {
      if (pCode != nullptr) {
        *pCode = initCode;
      }
      return nullptr;
    }
    bool         finished = false;
    SSDataBlock *pBlock = nullptr;
    int32_t      code = streamExecuteTask(pTaskInfo_, &pBlock, &finished);
    if (pCode != nullptr) {
      *pCode = code;
    }
    return pBlock;
  }

  std::vector<ProjectProtocolItem> drainProjectProtocol() {
    std::vector<ProjectProtocolItem> protocol;
    for (;;) {
      int32_t      code = TSDB_CODE_SUCCESS;
      SSDataBlock *pBlock = pullOne(&code);
      EXPECT_EQ(code, TSDB_CODE_SUCCESS);
      if (code != TSDB_CODE_SUCCESS || pBlock == nullptr) {
        return protocol;
      }
      ProjectProtocolItem item;
      item.pBlock = pBlock;
      item.gid = pBlock->info.id.baseGId;
      item.rows = static_cast<int32_t>(pBlock->info.rows);
      item.eog = qIsMultiGroupEogBlock(pBlock);
      protocol.push_back(item);
    }
  }

  SStreamRuntimeFuncInfo *runtime() { return &runtime_.funcInfo; }
  int32_t                 inputPullCount() const { return pInput_ == nullptr ? 0 : pInput_->nextItem; }
  SArray                 *childLineage() const { return pInput_ == nullptr ? nullptr : pInput_->pPublishedLineage; }
  SSDataBlock            *childBlock(int32_t index) const {
    if (pInput_ == nullptr) {
      return nullptr;
    }
    auto *pItem = static_cast<ProjectTestInputItem *>(taosArrayGet(pInput_->pItems, index));
    return pItem == nullptr ? nullptr : pItem->pBlock;
  }

  int32_t pullChild(SSDataBlock **ppBlock) {
    if (ensureOperator() != TSDB_CODE_SUCCESS) {
      return pTaskInfo_->code;
    }
    return pTaskInfo_->pRoot->pDownstream[0]->fpSet.getNextFn(pTaskInfo_->pRoot->pDownstream[0], ppBlock);
  }

  int32_t resetRequest() {
    const int32_t code = ensureOperator();
    return code == TSDB_CODE_SUCCESS ? streamClearStatesForOperators(pTaskInfo_) : code;
  }

  void failResetAggSup() { stub_.set(resetAggSup, failProjectResetAggSup); }

  void failSetFunctionResultOutput() { stub_.set(setFunctionResultOutput, failProjectSetFunctionResultOutput); }

  void restoreResetHooks() {
    stub_.reset(resetAggSup);
    stub_.reset(setFunctionResultOutput);
  }

 private:
  Stub                              stub_;
  SExecTaskInfo                    *pTaskInfo_ = nullptr;
  SStreamRuntimeInfo                runtime_ = {};
  SSTriggerGroupCalcInfo            groupCalc_ = {};
  SProjectPhysiNode                *pNode_ = nullptr;
  SHashJoinPhysiNode                staticJoin_ = {};
  ProjectTestInput                 *pInput_ = nullptr;
  std::vector<ProjectTestBlockSpec> specs_;
  EOperatorType                     filterType_ = OP_TYPE_GREATER_THAN;
  int32_t                           filterBound_ = 0;
  int64_t                           limit_ = -1;
  int64_t                           offset_ = -1;
  int64_t                           groupOffset_ = -1;
  int64_t                           groupLimit_ = -1;
  bool                              hasFilter_ = false;
  bool                              hasLimit_ = false;
  bool                              mergeDataBlocks_ = false;
  ProjectProcessByRowKind           processByRowKind_ = ProjectProcessByRowKind::kNone;
  int64_t                           lagLeadOffset_ = 1;
  bool                              staticQualified_ = true;
  bool                              functionMgtInitialized_ = false;
  bool                              tempEnvSaved_ = false;
  char                              previousTempDir_[PATH_MAX] = {0};
  SDiskSpace                        previousTempSpace_ = {};
};

TEST_F(ProjectMultiGroupOperatorTest, ForwardsEogBeforeFilterAndLimit) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushEogBlock(101);
  setFilter(OP_TYPE_GREATER_THAN, 1000);

  const auto protocol = drainProjectProtocol();
  ASSERT_EQ(protocol.size(), 1);
  EXPECT_TRUE(protocol[0].eog);
  EXPECT_EQ(protocol[0].gid, 101);
  EXPECT_NE(protocol[0].pBlock, childBlock(1));
}

TEST_F(ProjectMultiGroupOperatorTest, EogHasEmptyOwnedLineage) {
  pushEogBlock(202);

  SSDataBlock *pBlock = pullOne();
  ASSERT_NE(pBlock, nullptr);
  ASSERT_TRUE(qIsMultiGroupEogBlock(pBlock));
  ASSERT_NE(runtime()->pStreamBlkWinIdx, nullptr);
  EXPECT_EQ(taosArrayGetSize(runtime()->pStreamBlkWinIdx), 0);
  EXPECT_NE(runtime()->pStreamBlkWinIdx, childLineage());
  EXPECT_NE(pBlock, childBlock(0));
}

TEST_F(ProjectMultiGroupOperatorTest, OneToOneProjectionCopiesCompressedLineage) {
  pushDataBlock(101, {10, 20, 30, 40}, {{7, 0}, {8, 2}});

  SSDataBlock *pBlock = pullOne();
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 4);
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx),
            (std::vector<std::pair<int32_t, int32_t>>{{7, 0}, {8, 2}}));
  EXPECT_NE(runtime()->pStreamBlkWinIdx, childLineage());
}

TEST_F(ProjectMultiGroupOperatorTest, StrictProjectionWithoutLagLeadStillReturnsEachChildBlock) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushDataBlock(101, {30, 40}, {{7, 0}});
  pushEogBlock(101);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 2);
  EXPECT_EQ(inputPullCount(), 1);

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 2);
  EXPECT_EQ(inputPullCount(), 2);
}

TEST_F(ProjectMultiGroupOperatorTest, PartialFilterRebuildsCompressedLineage) {
  pushDataBlock(101, {10, 20, 30, 40}, {{7, 0}, {8, 2}});
  setFilter(OP_TYPE_NOT_EQUAL, 20);

  SSDataBlock *pBlock = pullOne();
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 3);
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx),
            (std::vector<std::pair<int32_t, int32_t>>{{7, 0}, {8, 1}}));
}

TEST_F(ProjectMultiGroupOperatorTest, FullFilterDropsDataButKeepsFollowingEog) {
  pushDataBlock(101, {10, 20, 30, 40}, {{7, 0}, {8, 2}});
  pushEogBlock(101);
  setFilter(OP_TYPE_GREATER_THAN, 1000);

  SSDataBlock *pBlock = pullOne();
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  ASSERT_NE(runtime()->pStreamBlkWinIdx, nullptr);
  EXPECT_EQ(taosArrayGetSize(runtime()->pStreamBlkWinIdx), 0);
  EXPECT_NE(runtime()->pStreamBlkWinIdx, childLineage());
  EXPECT_NE(pBlock, childBlock(1));
}

TEST_F(ProjectMultiGroupOperatorTest, FilteredDataFollowedByDifferentGroupEogFailsClosed) {
  pushDataBlock(101, {10}, {{7, 0}});
  pushEogBlock(202);
  setFilter(OP_TYPE_GREATER_THAN, 1000);

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, FilteredDataFollowedByEofFailsClosed) {
  pushDataBlock(101, {10}, {{7, 0}});
  setFilter(OP_TYPE_GREATER_THAN, 1000);

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, DiscardedDataFollowedByDifferentGroupEogFailsClosed) {
  pushDataBlock(101, {10}, {{7, 0}});
  pushEogBlock(202);
  setGroupOffset(1);

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, DiscardedDataFollowedByEofFailsClosed) {
  pushDataBlock(101, {10}, {{7, 0}});
  setGroupOffset(1);

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, DiscardedDataFollowedByMatchingEogIsLegal) {
  pushDataBlock(101, {10}, {{7, 0}});
  pushEogBlock(101);
  setGroupOffset(1);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
}

TEST_F(ProjectMultiGroupOperatorTest, StrictDiscardedMalformedDataIdentityFailsBeforeEog) {
  pushDataBlockWithIdentity(101, 202, {10}, {{7, 0}});
  pushEogBlock(101);
  setGroupOffset(1);

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(inputPullCount(), 1);
}

TEST_F(ProjectMultiGroupOperatorTest, StrictMalformedZeroRowDataIdentityFailsBeforeEog) {
  pushDataBlockWithIdentity(101, 202, {}, {}, 0);
  pushEogBlock(101);

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(inputPullCount(), 1);
}

TEST_F(ProjectMultiGroupOperatorTest, StrictValidZeroRowDataConsumesMatchingEog) {
  pushDataBlockWithIdentity(101, 101, {}, {}, 0);
  pushEogBlock(101);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_EQ(inputPullCount(), 2);
}

TEST_F(ProjectMultiGroupOperatorTest, GroupLimitZeroDrainsMatchingEogBeforeCompletion) {
  pushDataBlock(101, {10}, {{7, 0}});
  pushEogBlock(101);
  setGroupLimit(0);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_EQ(inputPullCount(), 2);
}

TEST_F(ProjectMultiGroupOperatorTest, GroupLimitZeroRejectsWrongEogWhileDrainActive) {
  pushDataBlock(101, {10}, {{7, 0}});
  pushEogBlock(202);
  setGroupLimit(0);

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(inputPullCount(), 2);
}

TEST_F(ProjectMultiGroupOperatorTest, GroupLimitZeroRejectsEofWhileDrainActive) {
  pushDataBlock(101, {10}, {{7, 0}});
  setGroupLimit(0);

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, CompletedStrictDrainReturnsMatchingEogOnNextPull) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushEogBlock(101);
  setLimit(1, 0);
  setGroupLimit(1);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 1);
  EXPECT_EQ(inputPullCount(), 1);

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_EQ(inputPullCount(), 2);
}

TEST_F(ProjectMultiGroupOperatorTest, CompletedStrictDrainRejectsWrongEogOnNextPull) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushEogBlock(202);
  setLimit(1, 0);
  setGroupLimit(1);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 1);

  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(inputPullCount(), 2);
}

TEST_F(ProjectMultiGroupOperatorTest, CompletedStrictDrainRejectsEofOnNextPull) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  setLimit(1, 0);
  setGroupLimit(1);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 1);

  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, MergeFilteredDataFollowedByDifferentGroupEogFailsClosed) {
  pushDataVectorBlock(101, std::vector<int32_t>(2050, 10), {{7, 0}});
  pushEogBlock(202);
  setMergeDataBlocks();
  setFilter(OP_TYPE_GREATER_THAN, 1000);

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, MergeFilteredDataFollowedByEofFailsClosed) {
  pushDataVectorBlock(101, std::vector<int32_t>(2050, 10), {{7, 0}});
  setMergeDataBlocks();
  setFilter(OP_TYPE_GREATER_THAN, 1000);

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, MergeFilteredDataFollowedByMatchingEogIsLegal) {
  pushDataVectorBlock(101, std::vector<int32_t>(2050, 10), {{7, 0}});
  pushEogBlock(101);
  setMergeDataBlocks();
  setFilter(OP_TYPE_GREATER_THAN, 1000);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
}

TEST_F(ProjectMultiGroupOperatorTest, LimitZeroAndFilterKeepFollowingOwnedEog) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushEogBlock(101);
  setLimit(0, 0);
  setFilter(OP_TYPE_GREATER_THAN, 1000);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  ASSERT_NE(runtime()->pStreamBlkWinIdx, nullptr);
  EXPECT_EQ(taosArrayGetSize(runtime()->pStreamBlkWinIdx), 0);
  EXPECT_NE(runtime()->pStreamBlkWinIdx, childLineage());
  EXPECT_NE(pBlock, childBlock(1));
}

TEST_F(ProjectMultiGroupOperatorTest, OffsetConsumesDataButKeepsFollowingOwnedEog) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushEogBlock(101);
  setLimit(2, 2);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  ASSERT_NE(runtime()->pStreamBlkWinIdx, nullptr);
  EXPECT_EQ(taosArrayGetSize(runtime()->pStreamBlkWinIdx), 0);
  EXPECT_NE(runtime()->pStreamBlkWinIdx, childLineage());
  EXPECT_NE(pBlock, childBlock(1));
}

TEST_F(ProjectMultiGroupOperatorTest, LimitOffsetSlicesCompressedLineage) {
  pushDataBlock(101, {10, 20, 30, 40}, {{7, 0}, {8, 2}});
  setLimit(2, 1);

  SSDataBlock *pBlock = pullOne();
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 2);
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx),
            (std::vector<std::pair<int32_t, int32_t>>{{7, 0}, {8, 1}}));
}

TEST_F(ProjectMultiGroupOperatorTest, ProcessByRowResultAboveInputFailsClosed) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  setInvalidProcessByRowResult();

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, StatefulLagContinuesAcrossChildBlocksInSameWindow) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushDataBlock(101, {30, 40}, {{7, 0}});
  pushEogBlock(101);
  setRealLagProcessByRow();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 4);
  EXPECT_EQ(inputPullCount(), 3);
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  EXPECT_TRUE(colDataIsNull_s(pColumn, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 1)), 10);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 2)), 20);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 3)), 30);
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx), (std::vector<std::pair<int32_t, int32_t>>{{7, 0}}));

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
}

TEST_F(ProjectMultiGroupOperatorTest, StatefulDiffProducesNMinusOneAcrossChildBlocks) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushDataBlock(101, {30, 40}, {{7, 0}});
  pushEogBlock(101);
  setRealDiffProcessByRow();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 1);
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  ASSERT_FALSE(colDataIsNull_s(pColumn, 0));
  EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(pColumn, 0)), 10);
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx), (std::vector<std::pair<int32_t, int32_t>>{{7, 0}}));

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 2);
  pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  ASSERT_FALSE(colDataIsNull_s(pColumn, 0));
  ASSERT_FALSE(colDataIsNull_s(pColumn, 1));
  EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(pColumn, 0)), 10);
  EXPECT_EQ(*reinterpret_cast<int64_t *>(colDataGetData(pColumn, 1)), 10);
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx), (std::vector<std::pair<int32_t, int32_t>>{{7, 0}}));

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
}

TEST_F(ProjectMultiGroupOperatorTest, VarcharFillForwardContinuesAcrossChildBlocksInSameWindow) {
  pushVarcharDataBlock(101, {"alpha"}, {false}, {{7, 0}});
  pushVarcharDataBlock(101, {"unused", "beta"}, {true, false}, {{7, 0}});
  pushEogBlock(101);
  setRealVarcharFillForwardProcessByRow();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 1);

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 2);
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  ASSERT_FALSE(colDataIsNull_s(pColumn, 0));
  const char *pFirst = colDataGetData(pColumn, 0);
  EXPECT_EQ(std::string(varDataVal(pFirst), varDataLen(pFirst)), "alpha");
  ASSERT_FALSE(colDataIsNull_s(pColumn, 1));
  const char *pSecond = colDataGetData(pColumn, 1);
  EXPECT_EQ(std::string(varDataVal(pSecond), varDataLen(pSecond)), "beta");

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
}

TEST_F(ProjectMultiGroupOperatorTest, MixedRegisteredFunctionsTrackEveryWindowAcrossBlocksAndEog) {
  pushDataBlock(101, {10, 20, 30, 40}, {{7, 0}, {8, 2}});
  pushDataBlock(101, {50, 60}, {{8, 0}});
  pushEogBlock(101);
  pushDataBlock(202, {100, 200}, {{7, 0}});
  pushEogBlock(202);
  setRealLagAndFillForwardProcessByRow();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 6);
  EXPECT_EQ(inputPullCount(), 3);
  auto *pLag = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  auto *pFill = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 1));
  ASSERT_NE(pLag, nullptr);
  ASSERT_NE(pFill, nullptr);
  EXPECT_TRUE(colDataIsNull_s(pLag, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pLag, 1)), 10);
  EXPECT_TRUE(colDataIsNull_s(pLag, 2));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pLag, 3)), 30);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pLag, 4)), 40);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pLag, 5)), 50);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pFill, 0)), 10);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pFill, 5)), 60);
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx),
            (std::vector<std::pair<int32_t, int32_t>>{{7, 0}, {8, 2}}));

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_TRUE(qIsMultiGroupEogBlock(pBlock));
  ASSERT_EQ(pBlock->info.id.baseGId, 101);

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 2);
  pLag = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pLag, nullptr);
  EXPECT_TRUE(colDataIsNull_s(pLag, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pLag, 1)), 100);
}

TEST_F(ProjectMultiGroupOperatorTest, MixedRegisteredFunctionRowCountsMustMatch) {
  pushDataBlock(101, {10, 20, 30, 40}, {{7, 0}, {8, 2}});
  pushEogBlock(101);
  setRealLagAndDiffProcessByRow();

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, LeadBuffersCompleteGroupUntilEog) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushDataBlock(101, {30, 40}, {{7, 0}});
  pushEogBlock(101);
  setRealLeadProcessByRow();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 4);
  EXPECT_EQ(inputPullCount(), 3);
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 0)), 20);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 1)), 30);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 2)), 40);
  EXPECT_TRUE(colDataIsNull_s(pColumn, 3));
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx), (std::vector<std::pair<int32_t, int32_t>>{{7, 0}}));

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
}

TEST_F(ProjectMultiGroupOperatorTest, LeadOffsetTwoResolvesAcrossChildBlocks) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushDataBlock(101, {30, 40}, {{7, 0}});
  pushDataBlock(101, {50}, {{7, 0}});
  pushEogBlock(101);
  setRealLeadProcessByRow(2);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 5);
  EXPECT_EQ(inputPullCount(), 4);
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 0)), 30);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 1)), 40);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 2)), 50);
  EXPECT_TRUE(colDataIsNull_s(pColumn, 3));
  EXPECT_TRUE(colDataIsNull_s(pColumn, 4));
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx), (std::vector<std::pair<int32_t, int32_t>>{{7, 0}}));
}

TEST_F(ProjectMultiGroupOperatorTest, LeadAndLagMixedColumnsStayAlignedUntilEog) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushDataBlock(101, {30, 40}, {{7, 0}});
  pushEogBlock(101);
  setRealLeadAndLagProcessByRow();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 4);
  EXPECT_EQ(inputPullCount(), 3);
  auto *pLead = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  auto *pLag = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 1));
  ASSERT_NE(pLead, nullptr);
  ASSERT_NE(pLag, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pLead, 0)), 20);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pLead, 1)), 30);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pLead, 2)), 40);
  EXPECT_TRUE(colDataIsNull_s(pLead, 3));
  EXPECT_TRUE(colDataIsNull_s(pLag, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pLag, 1)), 10);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pLag, 2)), 20);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pLag, 3)), 30);
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx), (std::vector<std::pair<int32_t, int32_t>>{{7, 0}}));
}

TEST_F(ProjectMultiGroupOperatorTest, LeadBufferedGroupLimitZeroReturnsOwnedEog) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushDataBlock(101, {30, 40}, {{7, 0}});
  pushEogBlock(101);
  setRealLeadProcessByRow();
  setLimit(0, 0);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_EQ(inputPullCount(), 3);
  ASSERT_NE(runtime()->pStreamBlkWinIdx, nullptr);
  EXPECT_EQ(taosArrayGetSize(runtime()->pStreamBlkWinIdx), 0);
}

TEST_F(ProjectMultiGroupOperatorTest, LeadBufferedGroupFullFilterReturnsOwnedEog) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushDataBlock(101, {30, 40}, {{7, 0}});
  pushEogBlock(101);
  setRealLeadProcessByRow();
  setFilter(OP_TYPE_GREATER_THAN, 1000);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_EQ(inputPullCount(), 3);
  ASSERT_NE(runtime()->pStreamBlkWinIdx, nullptr);
  EXPECT_EQ(taosArrayGetSize(runtime()->pStreamBlkWinIdx), 0);
}

TEST_F(ProjectMultiGroupOperatorTest, LeadBufferedGroupAppliesLimitOnceAfterAccumulation) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushDataBlock(101, {30, 40}, {{7, 0}});
  pushEogBlock(101);
  setRealLeadProcessByRow();
  setLimit(2, 1);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 2);
  EXPECT_EQ(inputPullCount(), 3);
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 0)), 30);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 1)), 40);
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx), (std::vector<std::pair<int32_t, int32_t>>{{7, 0}}));
}

TEST_F(ProjectMultiGroupOperatorTest, LeadBufferedGroupFiltersAfterCrossBlockResolution) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushDataBlock(101, {30, 40}, {{7, 0}});
  pushEogBlock(101);
  setRealLeadProcessByRow();
  setFilter(OP_TYPE_GREATER_THAN, 25);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 2);
  EXPECT_EQ(inputPullCount(), 3);
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 0)), 30);
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 1)), 40);
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx), (std::vector<std::pair<int32_t, int32_t>>{{7, 0}}));
}

TEST_F(ProjectMultiGroupOperatorTest, LeadBufferedGroupRejectsDifferentGidBeforeEog) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushDataBlock(202, {30, 40}, {{7, 0}});
  setRealLeadProcessByRow();

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, LeadBufferedGroupRejectsDifferentGidEog) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushEogBlock(202);
  setRealLeadProcessByRow();

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, LeadBufferedGroupRejectsEofBeforeEog) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  setRealLeadProcessByRow();

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, LeadResultCapacityRetainsHistoricalGroupPeak) {
  pushDataBlock(101, {10, 20, 30, 40}, {{7, 0}});
  pushDataBlock(101, {50, 60, 70, 80}, {{7, 0}});
  pushEogBlock(101);
  pushDataBlock(202, {100}, {{8, 0}});
  pushEogBlock(202);
  setRealLeadProcessByRow();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 8);
  const int64_t peakCapacity = pBlock->info.capacity;

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_TRUE(qIsMultiGroupEogBlock(pBlock));

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 1);
  EXPECT_GE(pBlock->info.capacity, peakCapacity);
}

TEST_F(ProjectMultiGroupOperatorTest, LegacyLeadStillReturnsEachChildBlock) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushDataBlock(101, {30, 40}, {{7, 0}});
  setRealLeadProcessByRow();
  setLegacyMode();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 2);
  EXPECT_EQ(inputPullCount(), 1);
}

TEST_F(ProjectMultiGroupOperatorTest, StatefulLagResetsAtWindowBoundary) {
  pushDataBlock(101, {10, 20, 30, 40}, {{7, 0}, {8, 2}});
  pushEogBlock(101);
  setRealLagProcessByRow();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 4);
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  EXPECT_TRUE(colDataIsNull_s(pColumn, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 1)), 10);
  EXPECT_TRUE(colDataIsNull_s(pColumn, 2));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 3)), 30);
}

TEST_F(ProjectMultiGroupOperatorTest, StatefulLagResetsAtGroupEog) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushEogBlock(101);
  pushDataBlock(202, {100, 200}, {{7, 0}});
  pushEogBlock(202);
  setRealLagProcessByRow();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_TRUE(qIsMultiGroupEogBlock(pBlock));
  ASSERT_EQ(pBlock->info.id.baseGId, 101);

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 2);
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  EXPECT_TRUE(colDataIsNull_s(pColumn, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 1)), 100);
}

TEST_F(ProjectMultiGroupOperatorTest, RequestResetClearsStatefulLagWindow) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushEogBlock(101);
  setRealLagProcessByRow();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(resetRequest(), TSDB_CODE_SUCCESS);

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 2);
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  EXPECT_TRUE(colDataIsNull_s(pColumn, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 1)), 10);
}

TEST_F(ProjectMultiGroupOperatorTest, ResetAggSupFailurePropagatesAndClearsPendingEog) {
  pushDataBlock(101, {10}, {{7, 0}});
  pushDataBlock(101, {20}, {{8, 0}});
  pushEogBlock(101);
  setMergeDataBlocks();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_FALSE(qIsMultiGroupEogBlock(pBlock));
  ASSERT_NE(runtime()->pStreamBlkWinIdx, nullptr);

  failResetAggSup();
  EXPECT_EQ(resetRequest(), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(runtime()->pStreamBlkWinIdx, nullptr);

  restoreResetHooks();
  ASSERT_EQ(resetRequest(), TSDB_CODE_SUCCESS);
  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.rows, 2);
}

TEST_F(ProjectMultiGroupOperatorTest, SetFunctionResultOutputFailurePropagatesAndStateIsRetryable) {
  pushDataBlock(101, {10, 20}, {{7, 0}});
  pushEogBlock(101);
  setRealLagProcessByRow();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_NE(runtime()->pStreamBlkWinIdx, nullptr);

  failSetFunctionResultOutput();
  EXPECT_EQ(resetRequest(), TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(runtime()->pStreamBlkWinIdx, nullptr);

  restoreResetHooks();
  ASSERT_EQ(resetRequest(), TSDB_CODE_SUCCESS);
  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  ASSERT_NE(pColumn, nullptr);
  EXPECT_TRUE(colDataIsNull_s(pColumn, 0));
  EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 1)), 10);
}

TEST_F(ProjectMultiGroupOperatorTest, MergeDoesNotCombineDifferentBaseGroups) {
  pushDataBlock(101, {10}, {{7, 0}});
  pushDataBlock(202, {20}, {{8, 0}});
  setMergeDataBlocks();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
  EXPECT_EQ(pBlock, nullptr);
}

TEST_F(ProjectMultiGroupOperatorTest, MergeReturnsAccumulatedDataBeforeEog) {
  pushDataBlock(101, {10}, {{7, 0}});
  pushDataBlock(101, {20}, {{8, 0}});
  pushEogBlock(101);
  setMergeDataBlocks();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.rows, 2);
  EXPECT_EQ(decodeProjectTestLineage(runtime()->pStreamBlkWinIdx),
            (std::vector<std::pair<int32_t, int32_t>>{{7, 0}, {8, 1}}));

  pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  ASSERT_NE(runtime()->pStreamBlkWinIdx, nullptr);
  EXPECT_EQ(taosArrayGetSize(runtime()->pStreamBlkWinIdx), 0);
}

TEST_F(ProjectMultiGroupOperatorTest, MergeFullFilterDropsDataButKeepsEog) {
  pushDataBlock(101, {10}, {{7, 0}});
  pushDataBlock(101, {20}, {{8, 0}});
  pushEogBlock(101);
  setMergeDataBlocks();
  setFilter(OP_TYPE_GREATER_THAN, 1000);

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  ASSERT_NE(runtime()->pStreamBlkWinIdx, nullptr);
  EXPECT_EQ(taosArrayGetSize(runtime()->pStreamBlkWinIdx), 0);
}

TEST_F(ProjectMultiGroupOperatorTest, ChildNextPullCannotMutateReturnedLineage) {
  pushDataBlock(101, {10, 20, 30, 40}, {{7, 0}, {8, 2}});
  pushDataBlock(101, {50}, {{9, 0}});

  SSDataBlock *pBlock = pullOne();
  ASSERT_NE(pBlock, nullptr);
  SArray *pReturnedLineage = runtime()->pStreamBlkWinIdx;
  ASSERT_NE(pReturnedLineage, nullptr);
  ASSERT_NE(pReturnedLineage, childLineage());

  SSDataBlock *pChildBlock = nullptr;
  ASSERT_EQ(pullChild(&pChildBlock), TSDB_CODE_SUCCESS);
  ASSERT_NE(pChildBlock, nullptr);
  EXPECT_EQ(decodeProjectTestLineage(pReturnedLineage), (std::vector<std::pair<int32_t, int32_t>>{{7, 0}, {8, 2}}));
  EXPECT_EQ(decodeProjectTestLineage(childLineage()), (std::vector<std::pair<int32_t, int32_t>>{{9, 0}}));
}

TEST_F(ProjectMultiGroupOperatorTest, MissingLineageFailsClosed) {
  pushDataBlock(101, {10, 20}, {});

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, NonzeroFirstLineageStartFailsClosed) {
  pushDataBlock(101, {10, 20}, {{7, 1}});

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, DuplicateOrDescendingLineageStartFailsClosed) {
  pushDataBlock(101, {10, 20, 30}, {{7, 0}, {8, 2}, {9, 2}});

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, LineageStartAtRowCountFailsClosed) {
  pushDataBlock(101, {10, 20}, {{7, 0}, {8, 2}});

  int32_t code = TSDB_CODE_SUCCESS;
  EXPECT_EQ(pullOne(&code), nullptr);
  EXPECT_EQ(code, TSDB_CODE_STREAM_INTERNAL_ERROR);
}

TEST_F(ProjectMultiGroupOperatorTest, LegacyProjectDoesNotRequireStreamLineage) {
  pushLegacyDataBlock(101, {10, 20});
  setLegacyMode();

  int32_t      code = TSDB_CODE_SUCCESS;
  SSDataBlock *pBlock = pullOne(&code);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 2);
  EXPECT_EQ(runtime()->pStreamBlkWinIdx, nullptr);
}

}  // namespace

TEST(MultiGroupEogProtocolTest, StrictEncodingAndIdentity) {
  SSDataBlock *block = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&block));
  ASSERT_EQ(TSDB_CODE_SUCCESS, qSetMultiGroupEogBlock(block, 202));
  EXPECT_EQ(0, block->info.rows);
  EXPECT_EQ(1, block->info.dataLoad);
  EXPECT_EQ(202, block->info.id.baseGId);
  EXPECT_EQ(202, block->info.id.groupId);
  EXPECT_TRUE(qIsMultiGroupEogBlock(block));
  blockDataDestroy(block);
}

TEST(MultiGroupEogProtocolTest, RejectsIncompleteEncoding) {
  SSDataBlock *block = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&block));
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, qSetMultiGroupEogBlock(nullptr, 202));
  block->info.dataLoad = 1;
  block->info.id.baseGId = 202;
  EXPECT_FALSE(qIsMultiGroupEogBlock(block));
  block->info.id.groupId = 101;
  EXPECT_FALSE(qIsMultiGroupEogBlock(block));
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, qSetMultiGroupEogBlock(block, 0));
  blockDataDestroy(block);
}

TEST(MultiGroupEogProtocolTest, ClearsZeroCapacityStaleMetadata) {
  SSDataBlock *block = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, createDataBlock(&block));
  ASSERT_EQ(0, block->info.capacity);
  block->info.rows = 9;
  block->info.dataLoad = 7;
  block->info.id = {};
  block->info.id.uid = 101;
  block->info.id.groupId = 102;
  block->info.id.baseGId = 103;

  ASSERT_EQ(TSDB_CODE_SUCCESS, qSetMultiGroupEogBlock(block, 202));
  EXPECT_EQ(0, block->info.rows);
  EXPECT_EQ(0, block->info.id.uid);
  EXPECT_EQ(202, block->info.id.groupId);
  EXPECT_EQ(202, block->info.id.baseGId);
  EXPECT_EQ(1, block->info.dataLoad);
  EXPECT_TRUE(qIsMultiGroupEogBlock(block));
  blockDataDestroy(block);
}

namespace {

struct ExchangeFetchResponseBytes {
  std::vector<uint8_t> bytes;
  int32_t              blockSize = 0;
  int32_t              frameOffset = 0;
};

struct ExchangeMultiGroupOwner {
  explicit ExchangeMultiGroupOwner(bool staticQualified = true, bool runtimeMultiGroup = true, bool runnerSource = true)
      : handle(exchangeTestCreateMultiGroupOwner(staticQualified, runtimeMultiGroup, runnerSource)) {}
  ExchangeMultiGroupOwner(std::initializer_list<uint64_t> groupOrder, int32_t sourceCount)
      : handle(exchangeTestCreateMultiSourceOwner(true, true, true, sourceCount, groupOrder.begin(),
                                                  static_cast<int32_t>(groupOrder.size()))) {}
  ~ExchangeMultiGroupOwner() {
    if (delayedDestroy) {
      (void)finishDelayedDestroy();
    } else {
      exchangeTestDestroyMultiGroupOwner(handle);
    }
  }

  ExchangeMultiGroupOwner(const ExchangeMultiGroupOwner &) = delete;
  ExchangeMultiGroupOwner &operator=(const ExchangeMultiGroupOwner &) = delete;

  int32_t beginDelayedDestroy() {
    int32_t code = exchangeTestBeginDelayedDestroy(handle);
    if (code == TSDB_CODE_SUCCESS) delayedDestroy = true;
    return code;
  }

  int32_t finishDelayedDestroy() {
    void *pOwner = handle;
    handle = nullptr;
    delayedDestroy = false;
    return exchangeTestFinishDelayedDestroy(pOwner);
  }

  void *handle = nullptr;
  bool  delayedDestroy = false;
};

ExchangeFetchResponseBytes makeStrictExchangeResponse(EStreamRunnerFetchType outerType,
                                                      EStreamRunnerFetchType frameType, uint64_t blockGid,
                                                      uint64_t frameGid, std::initializer_list<int32_t> values,
                                                      std::initializer_list<std::pair<int32_t, int32_t>> lineage,
                                                      bool includeFrame = true) {
  ExchangeFetchResponseBytes result;
  ProjectTestInputItem       input;
  ProjectTestBlockSpec       spec;
  spec.gid = blockGid;
  spec.values.assign(values.begin(), values.end());
  spec.lineage.assign(lineage.begin(), lineage.end());

  const bool hasBlock = outerType == STREAM_RUNNER_FETCH_DATA;
  if (hasBlock && makeProjectTestDataBlock(spec, &input) != TSDB_CODE_SUCCESS) {
    destroyProjectTestInputItem(&input);
    return result;
  }

  SStreamRunnerFetchFrame frame = {};
  frame.type = frameType;
  frame.baseGId = frameGid;
  frame.pLineage = frameType == STREAM_RUNNER_FETCH_DATA ? input.pLineage : nullptr;
  result.blockSize = hasBlock ? blockGetInternalEncodeSize(input.pBlock) : 0;
  const int32_t frameSize = includeFrame ? tSerializeSStreamRunnerFetchFrame(nullptr, 0, &frame) : 0;
  if (result.blockSize < 0 || frameSize < 0) {
    destroyProjectTestInputItem(&input);
    return {};
  }

  const int32_t payloadLen = INT_BYTES * 2 + result.blockSize + frameSize;
  result.bytes.resize(sizeof(SRetrieveTableRsp) + payloadLen);
  memset(result.bytes.data(), 0, result.bytes.size());
  auto *pRsp = reinterpret_cast<SRetrieveTableRsp *>(result.bytes.data());
  pRsp->completed = 0;
  pRsp->compressed = 0;
  pRsp->streamBlockType = static_cast<int8_t>(outerType);
  pRsp->payloadLen = htonl(payloadLen);
  pRsp->compLen = htonl(payloadLen);
  pRsp->numOfBlocks = htonl(hasBlock ? 1 : 0);
  pRsp->numOfRows = htobe64(hasBlock ? static_cast<int64_t>(values.size()) : 0);
  pRsp->numOfCols = htobe64(hasBlock ? 1 : 0);
  memcpy(pRsp->data, &result.blockSize, INT_BYTES);
  memcpy(pRsp->data + INT_BYTES, &result.blockSize, INT_BYTES);

  if (hasBlock) {
    const int32_t encoded = blockEncodeInternal(input.pBlock, pRsp->data + INT_BYTES * 2, result.blockSize, 1);
    if (encoded != result.blockSize) {
      destroyProjectTestInputItem(&input);
      return {};
    }
  }
  result.frameOffset = INT_BYTES * 2 + result.blockSize;
  if (includeFrame) {
    const int32_t encoded = tSerializeSStreamRunnerFetchFrame(pRsp->data + result.frameOffset, frameSize, &frame);
    if (encoded != frameSize) {
      destroyProjectTestInputItem(&input);
      return {};
    }
  }

  destroyProjectTestInputItem(&input);
  return result;
}

ExchangeFetchResponseBytes makeStrictExchangeDataResponse(uint64_t gid, std::initializer_list<int32_t> values,
                                                          std::initializer_list<std::pair<int32_t, int32_t>> lineage) {
  return makeStrictExchangeResponse(STREAM_RUNNER_FETCH_DATA, STREAM_RUNNER_FETCH_DATA, gid, gid, values, lineage);
}

ExchangeFetchResponseBytes makeStrictExchangeEogResponse(uint64_t gid) {
  return makeStrictExchangeResponse(STREAM_RUNNER_FETCH_EOG, STREAM_RUNNER_FETCH_EOG, gid, gid, {}, {});
}

ExchangeFetchResponseBytes makeLegacyExchangeResponse(uint64_t gid, std::initializer_list<int32_t> values,
                                                      bool completed = false) {
  ExchangeFetchResponseBytes result;
  ProjectTestInputItem       input;
  ProjectTestBlockSpec       spec;
  spec.gid = gid;
  spec.values.assign(values.begin(), values.end());
  if (values.size() != 0 && makeProjectTestDataBlock(spec, &input) != TSDB_CODE_SUCCESS) {
    destroyProjectTestInputItem(&input);
    return result;
  }

  result.blockSize = values.size() == 0 ? 0 : blockGetInternalEncodeSize(input.pBlock);
  if (result.blockSize < 0) {
    destroyProjectTestInputItem(&input);
    return {};
  }
  const int32_t payloadBytes = INT_BYTES * 2 + result.blockSize;
  result.bytes.resize(sizeof(SRetrieveTableRsp) + payloadBytes);
  memset(result.bytes.data(), 0, result.bytes.size());
  auto *pRsp = reinterpret_cast<SRetrieveTableRsp *>(result.bytes.data());
  pRsp->completed = completed || values.size() == 0;
  pRsp->streamBlockType = STREAM_RUNNER_FETCH_LEGACY;
  pRsp->numOfBlocks = htonl(values.size() == 0 ? 0 : 1);
  pRsp->numOfRows = htobe64(static_cast<int64_t>(values.size()));
  pRsp->numOfCols = htonl(values.size() == 0 ? 0 : 1);
  memcpy(pRsp->data, &result.blockSize, INT_BYTES);
  memcpy(pRsp->data + INT_BYTES, &result.blockSize, INT_BYTES);
  if (values.size() != 0) {
    const int32_t encoded = blockEncodeInternal(input.pBlock, pRsp->data + INT_BYTES * 2, result.blockSize, 1);
    if (encoded != result.blockSize) {
      destroyProjectTestInputItem(&input);
      return {};
    }
  }
  destroyProjectTestInputItem(&input);
  return result;
}

void setExchangeResponsePayloadLen(ExchangeFetchResponseBytes *pResponse, int32_t payloadLen) {
  auto *pRsp = reinterpret_cast<SRetrieveTableRsp *>(pResponse->bytes.data());
  pRsp->payloadLen = htonl(payloadLen);
  pRsp->compLen = htonl(payloadLen);
}

std::vector<uint8_t> getStrictExchangeInternalBlock(const ExchangeFetchResponseBytes &response) {
  const auto *pRsp = reinterpret_cast<const SRetrieveTableRsp *>(response.bytes.data());
  const auto *pBlockStart = reinterpret_cast<const uint8_t *>(pRsp->data + INT_BYTES * 2);
  return std::vector<uint8_t>(pBlockStart, pBlockStart + response.blockSize);
}

void setInternalBlockInt32(std::vector<uint8_t> *pBlock, size_t offset, int32_t value) {
  ASSERT_LE(offset + sizeof(value), pBlock->size());
  memcpy(pBlock->data() + offset, &value, sizeof(value));
}

std::vector<uint8_t> makeInternalSingleColumnBlock(int8_t type, int32_t bytes,
                                                   const std::vector<std::vector<uint8_t>> &values,
                                                   uint8_t precision = 0, uint8_t scale = 0) {
  SSDataBlock *pBlock = nullptr;
  if (values.empty() || values.size() > INT32_MAX || createDataBlock(&pBlock) != TSDB_CODE_SUCCESS) return {};

  SColumnInfoData column = createColumnInfoData(type, bytes, 1);
  column.info.precision = precision;
  column.info.scale = scale;
  if (blockDataAppendColInfo(pBlock, &column) != TSDB_CODE_SUCCESS ||
      blockDataEnsureCapacity(pBlock, static_cast<uint32_t>(values.size())) != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return {};
  }

  SColumnInfoData *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
  if (pColumn == nullptr) {
    blockDataDestroy(pBlock);
    return {};
  }
  for (size_t i = 0; i < values.size(); ++i) {
    const bool  isNull = values[i].empty();
    const char *pValue = isNull ? nullptr : reinterpret_cast<const char *>(values[i].data());
    if (colDataSetVal(pColumn, static_cast<uint32_t>(i), pValue, isNull) != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
      return {};
    }
  }

  pBlock->info.rows = static_cast<int32_t>(values.size());
  pBlock->info.dataLoad = 1;
  pBlock->info.id.groupId = 101;
  pBlock->info.id.baseGId = 101;
  const int32_t encodedLen = blockGetInternalEncodeSize(pBlock);
  if (encodedLen <= 0) {
    blockDataDestroy(pBlock);
    return {};
  }

  std::vector<uint8_t> encoded(encodedLen);
  if (blockEncodeInternal(pBlock, reinterpret_cast<char *>(encoded.data()), encoded.size(), 1) != encodedLen) {
    encoded.clear();
  }
  blockDataDestroy(pBlock);
  return encoded;
}

std::vector<uint8_t> makeVarStringValue(const std::string &value) {
  std::vector<uint8_t> encoded(VARSTR_HEADER_SIZE + value.size());
  const VarDataLenT    valueLen = static_cast<VarDataLenT>(value.size());
  memcpy(encoded.data(), &valueLen, sizeof(valueLen));
  memcpy(encoded.data() + VARSTR_HEADER_SIZE, value.data(), value.size());
  return encoded;
}

std::vector<uint8_t> makeBlobValue(const std::string &value) {
  std::vector<uint8_t> encoded(BLOBSTR_HEADER_SIZE + value.size());
  const BlobDataLenT   valueLen = static_cast<BlobDataLenT>(value.size());
  memcpy(encoded.data(), &valueLen, sizeof(valueLen));
  memcpy(encoded.data() + BLOBSTR_HEADER_SIZE, value.data(), value.size());
  return encoded;
}

std::vector<uint8_t> makeJsonNcharValue(const std::string &value) {
  std::vector<uint8_t> encoded(CHAR_BYTES + VARSTR_HEADER_SIZE + value.size());
  encoded[0] = TSDB_DATA_TYPE_NCHAR;
  const VarDataLenT valueLen = static_cast<VarDataLenT>(value.size());
  memcpy(encoded.data() + CHAR_BYTES, &valueLen, sizeof(valueLen));
  memcpy(encoded.data() + CHAR_BYTES + VARSTR_HEADER_SIZE, value.data(), value.size());
  return encoded;
}

std::vector<uint8_t> makeJsonTagValue() {
  std::vector<uint8_t> encoded(sizeof(STag));
  const int8_t         flags = TD_TAG_JSON;
  const int16_t        len = sizeof(STag);
  const int16_t        nTag = 0;
  const int32_t        ver = 1;
  memcpy(encoded.data() + offsetof(STag, flags), &flags, sizeof(flags));
  memcpy(encoded.data() + offsetof(STag, len), &len, sizeof(len));
  memcpy(encoded.data() + offsetof(STag, nTag), &nTag, sizeof(nTag));
  memcpy(encoded.data() + offsetof(STag, ver), &ver, sizeof(ver));
  return encoded;
}

using JsonTagTestValue = std::tuple<std::string, int8_t, std::vector<uint8_t>>;

std::vector<uint8_t> makeJsonTagValue(const std::vector<JsonTagTestValue> &values) {
  SArray *pValues = taosArrayInit(values.size(), sizeof(STagVal));
  if (pValues == nullptr) return {};

  for (const auto &spec : values) {
    const std::string          &key = std::get<0>(spec);
    const int8_t                type = std::get<1>(spec);
    const std::vector<uint8_t> &data = std::get<2>(spec);
    STagVal                     value = {};
    value.pKey = const_cast<char *>(key.c_str());
    value.type = type;
    if (IS_VAR_DATA_TYPE(type)) {
      value.nData = data.size();
      value.pData = const_cast<uint8_t *>(data.data());
    } else {
      if (IS_INVALID_TYPE(type) || data.size() != static_cast<size_t>(TYPE_BYTES[type])) {
        taosArrayDestroy(pValues);
        return {};
      }
      memcpy(&value.i64, data.data(), data.size());
    }
    if (taosArrayPush(pValues, &value) == nullptr) {
      taosArrayDestroy(pValues);
      return {};
    }
  }

  STag   *pTag = nullptr;
  int32_t code = tTagNew(pValues, 1, true, &pTag);
  taosArrayDestroy(pValues);
  if (code != TSDB_CODE_SUCCESS || pTag == nullptr) {
    tTagFree(pTag);
    return {};
  }

  std::vector<uint8_t> encoded(pTag->len);
  memcpy(encoded.data(), pTag, pTag->len);
  tTagFree(pTag);
  return encoded;
}

std::vector<uint8_t> wrapJsonTagInInternalBlock(const std::vector<uint8_t> &tag) {
  return makeInternalSingleColumnBlock(TSDB_DATA_TYPE_JSON, TYPE_BYTES[TSDB_DATA_TYPE_JSON], {tag});
}

std::vector<uint8_t> makeJsonTagWithRawFixedEntry(const std::string &key, int8_t type, size_t valueLen) {
  const size_t keyLen = key.size() + 1;
  const size_t entryLen = sizeof(uint8_t) + keyLen + sizeof(type) + valueLen;
  const size_t tagLen = sizeof(STag) + sizeof(int8_t) + entryLen;
  if (keyLen > INT8_MAX || entryLen > INT8_MAX || tagLen > INT16_MAX) return {};

  std::vector<uint8_t> encoded(tagLen);
  const int8_t         flags = TD_TAG_JSON;
  const int16_t        len = static_cast<int16_t>(tagLen);
  const int16_t        nTag = 1;
  const int32_t        ver = 1;
  memcpy(encoded.data() + offsetof(STag, flags), &flags, sizeof(flags));
  memcpy(encoded.data() + offsetof(STag, len), &len, sizeof(len));
  memcpy(encoded.data() + offsetof(STag, nTag), &nTag, sizeof(nTag));
  memcpy(encoded.data() + offsetof(STag, ver), &ver, sizeof(ver));

  size_t offset = sizeof(STag) + sizeof(int8_t);
  encoded[offset++] = static_cast<uint8_t>(keyLen);
  memcpy(encoded.data() + offset, key.c_str(), keyLen);
  offset += keyLen;
  encoded[offset] = type;
  return encoded;
}

struct JsonTagHeader {
  int8_t  flags = 0;
  int16_t len = 0;
  int16_t nTag = 0;
  int32_t ver = 0;
};

JsonTagHeader readJsonTagHeader(const std::vector<uint8_t> &tag) {
  JsonTagHeader header;
  EXPECT_GE(tag.size(), sizeof(STag));
  if (tag.size() >= sizeof(STag)) {
    memcpy(&header.flags, tag.data() + offsetof(STag, flags), sizeof(header.flags));
    memcpy(&header.len, tag.data() + offsetof(STag, len), sizeof(header.len));
    memcpy(&header.nTag, tag.data() + offsetof(STag, nTag), sizeof(header.nTag));
    memcpy(&header.ver, tag.data() + offsetof(STag, ver), sizeof(header.ver));
  }
  return header;
}

void setJsonTagLength(std::vector<uint8_t> *pTag) {
  ASSERT_NE(pTag, nullptr);
  ASSERT_GE(pTag->size(), sizeof(STag));
  ASSERT_LE(pTag->size(), static_cast<size_t>(INT16_MAX));
  const int16_t len = static_cast<int16_t>(pTag->size());
  memcpy(pTag->data() + offsetof(STag, len), &len, sizeof(len));
}

size_t jsonTagEntriesOffset(const std::vector<uint8_t> &tag) {
  const JsonTagHeader header = readJsonTagHeader(tag);
  return sizeof(STag) +
         static_cast<size_t>(header.nTag) * ((header.flags & TD_TAG_LARGE) != 0 ? sizeof(int16_t) : sizeof(int8_t));
}

int32_t decodeInternalBlockWithEmptySchema(const std::vector<uint8_t> &encoded) {
  SSDataBlock *pDecoded = nullptr;
  if (createDataBlock(&pDecoded) != TSDB_CODE_SUCCESS) return terrno;
  blockDataFreeRes(pDecoded);
  int32_t code =
      blockDecodeInternalBounded(pDecoded, reinterpret_cast<const char *>(encoded.data()), encoded.size(), nullptr);
  blockDataDestroy(pDecoded);
  return code;
}

constexpr size_t kSingleColumnOffsetsOffset =
    sizeof(int32_t) * 5 + sizeof(uint64_t) + sizeof(int8_t) + sizeof(int32_t) + sizeof(int32_t);

size_t singleColumnPayloadOffset(size_t rows) { return kSingleColumnOffsetsOffset + rows * sizeof(int32_t); }

}  // namespace

TEST(BoundedInternalBlockDecoderTest, PreservesValidInternalDecodeContract) {
  auto response = makeStrictExchangeDataResponse(101, {10, 20}, {{3, 0}});
  auto encoded = getStrictExchangeInternalBlock(response);
  ASSERT_FALSE(encoded.empty());

  SSDataBlock *pBounded = nullptr;
  SSDataBlock *pLegacy = nullptr;
  ASSERT_EQ(createDataBlock(&pBounded), TSDB_CODE_SUCCESS);
  ASSERT_EQ(createDataBlock(&pLegacy), TSDB_CODE_SUCCESS);
  blockDataFreeRes(pBounded);
  blockDataFreeRes(pLegacy);

  const char *pBoundedEnd = nullptr;
  const char *pLegacyEnd = nullptr;
  ASSERT_EQ(blockDecodeInternalBounded(pBounded, reinterpret_cast<const char *>(encoded.data()), encoded.size(),
                                       &pBoundedEnd),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(blockDecodeInternal(pLegacy, reinterpret_cast<const char *>(encoded.data()), &pLegacyEnd),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pBoundedEnd, reinterpret_cast<const char *>(encoded.data() + encoded.size()));
  EXPECT_EQ(pLegacyEnd, reinterpret_cast<const char *>(encoded.data() + encoded.size()));
  EXPECT_EQ(pBounded->info.rows, pLegacy->info.rows);
  EXPECT_EQ(pBounded->info.id.groupId, pLegacy->info.id.groupId);
  EXPECT_EQ(pBounded->info.id.baseGId, pLegacy->info.id.baseGId);

  blockDataDestroy(pBounded);
  blockDataDestroy(pLegacy);
}

TEST(BoundedInternalBlockDecoderTest, RejectsEveryTruncatedPrefixBeforeRead) {
  auto response = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  auto encoded = getStrictExchangeInternalBlock(response);
  ASSERT_FALSE(encoded.empty());

  for (size_t len = 0; len < encoded.size(); ++len) {
    SCOPED_TRACE(len);
    std::vector<uint8_t> truncated(encoded.begin(), encoded.begin() + len);
    SSDataBlock         *pDecoded = nullptr;
    ASSERT_EQ(createDataBlock(&pDecoded), TSDB_CODE_SUCCESS);
    EXPECT_EQ(blockDecodeInternalBounded(pDecoded, reinterpret_cast<const char *>(truncated.data()), truncated.size(),
                                         nullptr),
              TSDB_CODE_INVALID_MSG);
    blockDataDestroy(pDecoded);
  }
}

TEST(BoundedInternalBlockDecoderTest, RejectsMalformedLengthsCountsAndColumnMetadata) {
  constexpr size_t kInternalDataLenOffset = sizeof(int32_t);
  constexpr size_t kInternalRowsOffset = sizeof(int32_t) * 2;
  constexpr size_t kInternalColsOffset = sizeof(int32_t) * 3;
  constexpr size_t kInternalSchemaOffset = sizeof(int32_t) * 5 + sizeof(uint64_t);
  constexpr size_t kInternalColumnBytesOffset = kInternalSchemaOffset + sizeof(int8_t);
  constexpr size_t kInternalColumnLenOffset = kInternalSchemaOffset + sizeof(int8_t) + sizeof(int32_t);

  auto response = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  auto encoded = getStrictExchangeInternalBlock(response);
  ASSERT_FALSE(encoded.empty());

  std::vector<std::vector<uint8_t>> malformed;
  auto                              internalDataLen = encoded;
  setInternalBlockInt32(&internalDataLen, kInternalDataLenOffset, static_cast<int32_t>(encoded.size() + 1));
  malformed.push_back(std::move(internalDataLen));

  auto rows = encoded;
  setInternalBlockInt32(&rows, kInternalRowsOffset, INT32_MAX);
  malformed.push_back(std::move(rows));

  auto cols = encoded;
  setInternalBlockInt32(&cols, kInternalColsOffset, INT32_MAX);
  malformed.push_back(std::move(cols));

  auto invalidType = encoded;
  invalidType[kInternalSchemaOffset] = TSDB_DATA_TYPE_MAX;
  malformed.push_back(std::move(invalidType));

  auto columnBytes = encoded;
  setInternalBlockInt32(&columnBytes, kInternalColumnBytesOffset, INT32_MAX);
  malformed.push_back(std::move(columnBytes));

  auto columnLen = encoded;
  setInternalBlockInt32(&columnLen, kInternalColumnLenOffset, htonl(INT32_MAX));
  malformed.push_back(std::move(columnLen));

  for (const auto &block : malformed) {
    SSDataBlock *pDecoded = nullptr;
    ASSERT_EQ(createDataBlock(&pDecoded), TSDB_CODE_SUCCESS);
    ASSERT_EQ(blockDecodeInternalBounded(pDecoded, reinterpret_cast<const char *>(block.data()), block.size(), nullptr),
              TSDB_CODE_INVALID_MSG);
    blockDataDestroy(pDecoded);
  }
}

TEST(BoundedInternalBlockDecoderTest, AcceptsCanonicalVariableJsonNullAndDecimalEncodings) {
  std::vector<uint8_t> jsonNull = {TSDB_DATA_TYPE_NULL};
  std::vector<uint8_t> jsonDouble(CHAR_BYTES + DOUBLE_BYTES);
  jsonDouble[0] = TSDB_DATA_TYPE_DOUBLE;
  std::vector<uint8_t> jsonBool = {TSDB_DATA_TYPE_BOOL, 1};

  const std::vector<std::vector<uint8_t>> validBlocks = {
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_VARCHAR, TYPE_BYTES[TSDB_DATA_TYPE_VARCHAR],
                                    {makeVarStringValue("a"), makeVarStringValue("bc")}),
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_BLOB, TYPE_BYTES[TSDB_DATA_TYPE_BLOB], {makeBlobValue("blob")}),
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_MEDIUMBLOB, TYPE_BYTES[TSDB_DATA_TYPE_MEDIUMBLOB],
                                    {makeBlobValue("medium")}),
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_JSON, TYPE_BYTES[TSDB_DATA_TYPE_JSON], {jsonNull}),
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_JSON, TYPE_BYTES[TSDB_DATA_TYPE_JSON],
                                    {makeJsonNcharValue("nchar")}),
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_JSON, TYPE_BYTES[TSDB_DATA_TYPE_JSON], {jsonDouble}),
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_JSON, TYPE_BYTES[TSDB_DATA_TYPE_JSON], {jsonBool}),
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_JSON, TYPE_BYTES[TSDB_DATA_TYPE_JSON], {makeJsonTagValue()}),
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_NULL, CHAR_BYTES, {{}}),
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_NULL, TYPE_BYTES[TSDB_DATA_TYPE_NULL], {{}}),
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_DECIMAL64, TYPE_BYTES[TSDB_DATA_TYPE_DECIMAL64],
                                    {std::vector<uint8_t>(DECIMAL64_BYTES)}, TSDB_DECIMAL64_MAX_PRECISION, 2),
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_DECIMAL, TYPE_BYTES[TSDB_DATA_TYPE_DECIMAL],
                                    {std::vector<uint8_t>(DECIMAL128_BYTES)}, TSDB_DECIMAL128_MAX_PRECISION, 2),
  };

  for (const auto &block : validBlocks) {
    ASSERT_FALSE(block.empty());
    EXPECT_EQ(decodeInternalBlockWithEmptySchema(block), TSDB_CODE_SUCCESS);
  }
}

TEST(BoundedInternalBlockDecoderTest, AcceptsProductionEncodedSmallAndLargeJsonTags) {
  const std::vector<JsonTagTestValue> smallValues = {
      {"bool", TSDB_DATA_TYPE_BOOL, {1}},
      {"double", TSDB_DATA_TYPE_DOUBLE, std::vector<uint8_t>(DOUBLE_BYTES)},
      {"nchar", TSDB_DATA_TYPE_NCHAR, {'o', 'k'}},
      {"null", TSDB_DATA_TYPE_NULL, std::vector<uint8_t>(tDataTypes[TSDB_DATA_TYPE_NULL].bytes)},
  };
  const std::vector<JsonTagTestValue> largeValues = {
      {"large", TSDB_DATA_TYPE_NCHAR, std::vector<uint8_t>(INT8_MAX + 1, 'x')},
  };

  const auto emptyTag = makeJsonTagValue();
  const auto smallTag = makeJsonTagValue(smallValues);
  const auto largeTag = makeJsonTagValue(largeValues);
  ASSERT_FALSE(emptyTag.empty());
  ASSERT_FALSE(smallTag.empty());
  ASSERT_FALSE(largeTag.empty());
  EXPECT_EQ(readJsonTagHeader(emptyTag).nTag, 0);
  EXPECT_EQ(readJsonTagHeader(smallTag).flags & TD_TAG_LARGE, 0);
  EXPECT_NE(readJsonTagHeader(largeTag).flags & TD_TAG_LARGE, 0);

  for (const auto &tag : {emptyTag, smallTag, largeTag}) {
    auto block = wrapJsonTagInInternalBlock(tag);
    ASSERT_FALSE(block.empty());
    EXPECT_EQ(decodeInternalBlockWithEmptySchema(block), TSDB_CODE_SUCCESS);
  }
}

TEST(BoundedInternalBlockDecoderTest, RejectsUnsupportedSelfConsistentJsonTagEntryTypes) {
  const std::vector<JsonTagTestValue> intValue = {
      {"int", TSDB_DATA_TYPE_INT, std::vector<uint8_t>(INT_BYTES)},
  };
  const std::vector<std::pair<std::string, std::vector<uint8_t>>> unsupportedTags = {
      {"int", makeJsonTagValue(intValue)},
      {"decimal128", makeJsonTagWithRawFixedEntry("decimal", TSDB_DATA_TYPE_DECIMAL, DECIMAL128_BYTES)},
  };

  for (const auto &item : unsupportedTags) {
    SCOPED_TRACE(item.first);
    const std::vector<uint8_t> &tag = item.second;
    ASSERT_FALSE(tag.empty());
    auto block = wrapJsonTagInInternalBlock(tag);
    ASSERT_FALSE(block.empty());
    EXPECT_EQ(decodeInternalBlockWithEmptySchema(block), TSDB_CODE_INVALID_MSG);
  }
}

TEST(BoundedInternalBlockDecoderTest, RejectsMalformedJsonTagIndexes) {
  const std::vector<JsonTagTestValue> smallValues = {
      {"a", TSDB_DATA_TYPE_BOOL, {1}},
      {"b", TSDB_DATA_TYPE_BOOL, {0}},
  };
  const std::vector<JsonTagTestValue> largeValues = {
      {"large", TSDB_DATA_TYPE_NCHAR, std::vector<uint8_t>(INT8_MAX + 1, 'x')},
  };

  std::vector<std::pair<std::string, std::vector<uint8_t>>> malformed;

  auto smallOutOfRange = makeJsonTagValue(smallValues);
  ASSERT_FALSE(smallOutOfRange.empty());
  const size_t smallEntriesOffset = jsonTagEntriesOffset(smallOutOfRange);
  ASSERT_LT(smallEntriesOffset, smallOutOfRange.size());
  smallOutOfRange[sizeof(STag)] = static_cast<uint8_t>(smallOutOfRange.size() - smallEntriesOffset);
  malformed.emplace_back("small offset outside entry segment", std::move(smallOutOfRange));

  auto largeNegative = makeJsonTagValue(largeValues);
  ASSERT_FALSE(largeNegative.empty());
  const int16_t negativeOffset = -1;
  memcpy(largeNegative.data() + sizeof(STag), &negativeOffset, sizeof(negativeOffset));
  malformed.emplace_back("negative large offset", std::move(largeNegative));

  auto largeOutOfRange = makeJsonTagValue(largeValues);
  ASSERT_FALSE(largeOutOfRange.empty());
  const size_t largeEntriesOffset = jsonTagEntriesOffset(largeOutOfRange);
  ASSERT_LT(largeEntriesOffset, largeOutOfRange.size());
  const int16_t largeEndOffset = static_cast<int16_t>(largeOutOfRange.size() - largeEntriesOffset);
  memcpy(largeOutOfRange.data() + sizeof(STag), &largeEndOffset, sizeof(largeEndOffset));
  malformed.emplace_back("large offset outside entry segment", std::move(largeOutOfRange));

  auto overlapping = makeJsonTagValue(smallValues);
  ASSERT_FALSE(overlapping.empty());
  overlapping[sizeof(STag) + sizeof(int8_t)] = overlapping[sizeof(STag)];
  malformed.emplace_back("overlapping offsets", std::move(overlapping));

  auto reversed = makeJsonTagValue(smallValues);
  ASSERT_FALSE(reversed.empty());
  std::swap(reversed[sizeof(STag)], reversed[sizeof(STag) + sizeof(int8_t)]);
  malformed.emplace_back("reversed offsets", std::move(reversed));

  for (const auto &item : malformed) {
    SCOPED_TRACE(item.first);
    const std::vector<uint8_t> &tag = item.second;
    auto                        block = wrapJsonTagInInternalBlock(tag);
    ASSERT_FALSE(block.empty());
    EXPECT_EQ(decodeInternalBlockWithEmptySchema(block), TSDB_CODE_INVALID_MSG);
  }
}

TEST(BoundedInternalBlockDecoderTest, RejectsMalformedJsonTagEntries) {
  const std::vector<JsonTagTestValue> boolValue = {
      {"x", TSDB_DATA_TYPE_BOOL, {1}},
  };
  const std::vector<JsonTagTestValue> ncharValue = {
      {"x", TSDB_DATA_TYPE_NCHAR, {'a'}},
  };

  std::vector<std::pair<std::string, std::vector<uint8_t>>> malformed;

  auto truncatedKeyLength = makeJsonTagValue(boolValue);
  ASSERT_FALSE(truncatedKeyLength.empty());
  const size_t keyEntryOffset = jsonTagEntriesOffset(truncatedKeyLength);
  truncatedKeyLength.resize(keyEntryOffset + 5, 0x80);
  std::fill(truncatedKeyLength.begin() + keyEntryOffset, truncatedKeyLength.end(), 0x80);
  setJsonTagLength(&truncatedKeyLength);
  malformed.emplace_back("truncated key length varint", std::move(truncatedKeyLength));

  auto keyWithoutNul = makeJsonTagValue(boolValue);
  ASSERT_FALSE(keyWithoutNul.empty());
  keyWithoutNul[jsonTagEntriesOffset(keyWithoutNul) + 2] = 'y';
  malformed.emplace_back("key without trailing nul", std::move(keyWithoutNul));

  auto invalidType = makeJsonTagValue(boolValue);
  ASSERT_FALSE(invalidType.empty());
  invalidType[jsonTagEntriesOffset(invalidType) + 3] = TSDB_DATA_TYPE_MAX;
  malformed.emplace_back("invalid entry type", std::move(invalidType));

  auto truncatedFixedValue = makeJsonTagValue(boolValue);
  ASSERT_FALSE(truncatedFixedValue.empty());
  const size_t fixedEntryOffset = jsonTagEntriesOffset(truncatedFixedValue);
  truncatedFixedValue.resize(fixedEntryOffset + 4);
  truncatedFixedValue[fixedEntryOffset] = 2;
  truncatedFixedValue[fixedEntryOffset + 1] = 'x';
  truncatedFixedValue[fixedEntryOffset + 2] = 0;
  truncatedFixedValue[fixedEntryOffset + 3] = TSDB_DATA_TYPE_DOUBLE;
  setJsonTagLength(&truncatedFixedValue);
  malformed.emplace_back("truncated fixed entry value", std::move(truncatedFixedValue));

  auto oversizedVariableValue = makeJsonTagValue(ncharValue);
  ASSERT_FALSE(oversizedVariableValue.empty());
  oversizedVariableValue[jsonTagEntriesOffset(oversizedVariableValue) + 4] = UINT8_MAX >> 1;
  malformed.emplace_back("variable entry length outside tag", std::move(oversizedVariableValue));

  for (const auto &item : malformed) {
    SCOPED_TRACE(item.first);
    const std::vector<uint8_t> &tag = item.second;
    auto                        block = wrapJsonTagInInternalBlock(tag);
    ASSERT_FALSE(block.empty());
    EXPECT_EQ(decodeInternalBlockWithEmptySchema(block), TSDB_CODE_INVALID_MSG);
  }
}

TEST(BoundedInternalBlockDecoderTest, RejectsMalformedVariableOffsetsAndValueHeaders) {
  std::vector<std::vector<uint8_t>> malformed;

  auto negativeOffset = makeInternalSingleColumnBlock(TSDB_DATA_TYPE_VARCHAR, TYPE_BYTES[TSDB_DATA_TYPE_VARCHAR],
                                                      {makeVarStringValue("a")});
  setInternalBlockInt32(&negativeOffset, kSingleColumnOffsetsOffset, -2);
  malformed.push_back(std::move(negativeOffset));

  auto offsetAtEnd = makeInternalSingleColumnBlock(TSDB_DATA_TYPE_VARCHAR, TYPE_BYTES[TSDB_DATA_TYPE_VARCHAR],
                                                   {makeVarStringValue("a")});
  setInternalBlockInt32(&offsetAtEnd, kSingleColumnOffsetsOffset, static_cast<int32_t>(makeVarStringValue("a").size()));
  malformed.push_back(std::move(offsetAtEnd));

  auto discontinuous = makeInternalSingleColumnBlock(TSDB_DATA_TYPE_VARCHAR, TYPE_BYTES[TSDB_DATA_TYPE_VARCHAR],
                                                     {makeVarStringValue("a"), makeVarStringValue("b")});
  setInternalBlockInt32(&discontinuous, kSingleColumnOffsetsOffset + sizeof(int32_t), 0);
  malformed.push_back(std::move(discontinuous));

  auto varcharLength = makeInternalSingleColumnBlock(TSDB_DATA_TYPE_VARCHAR, TYPE_BYTES[TSDB_DATA_TYPE_VARCHAR],
                                                     {makeVarStringValue("a")});
  const VarDataLenT oversizedVar = UINT16_MAX;
  memcpy(varcharLength.data() + singleColumnPayloadOffset(1), &oversizedVar, sizeof(oversizedVar));
  malformed.push_back(std::move(varcharLength));

  for (const int8_t type : {TSDB_DATA_TYPE_BLOB, TSDB_DATA_TYPE_MEDIUMBLOB}) {
    auto               blobLength = makeInternalSingleColumnBlock(type, TYPE_BYTES[type], {makeBlobValue("a")});
    const BlobDataLenT oversizedBlob = UINT32_MAX;
    memcpy(blobLength.data() + singleColumnPayloadOffset(1), &oversizedBlob, sizeof(oversizedBlob));
    malformed.push_back(std::move(blobLength));
  }

  auto jsonUnknown =
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_JSON, TYPE_BYTES[TSDB_DATA_TYPE_JSON], {{TSDB_DATA_TYPE_NULL}});
  jsonUnknown[singleColumnPayloadOffset(1)] = TSDB_DATA_TYPE_MAX;
  malformed.push_back(std::move(jsonUnknown));

  auto jsonNcharLength =
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_JSON, TYPE_BYTES[TSDB_DATA_TYPE_JSON], {makeJsonNcharValue("a")});
  memcpy(jsonNcharLength.data() + singleColumnPayloadOffset(1) + CHAR_BYTES, &oversizedVar, sizeof(oversizedVar));
  malformed.push_back(std::move(jsonNcharLength));

  auto jsonDoubleHeader =
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_JSON, TYPE_BYTES[TSDB_DATA_TYPE_JSON], {{TSDB_DATA_TYPE_NULL}});
  jsonDoubleHeader[singleColumnPayloadOffset(1)] = TSDB_DATA_TYPE_DOUBLE;
  malformed.push_back(std::move(jsonDoubleHeader));

  auto jsonTagHeader =
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_JSON, TYPE_BYTES[TSDB_DATA_TYPE_JSON], {{TSDB_DATA_TYPE_NULL}});
  jsonTagHeader[singleColumnPayloadOffset(1)] = TD_TAG_JSON;
  malformed.push_back(std::move(jsonTagHeader));

  auto jsonTagLength =
      makeInternalSingleColumnBlock(TSDB_DATA_TYPE_JSON, TYPE_BYTES[TSDB_DATA_TYPE_JSON], {makeJsonTagValue()});
  const int16_t oversizedTag = INT16_MAX;
  memcpy(jsonTagLength.data() + singleColumnPayloadOffset(1) + offsetof(STag, len), &oversizedTag,
         sizeof(oversizedTag));
  malformed.push_back(std::move(jsonTagLength));

  for (const auto &block : malformed) {
    ASSERT_FALSE(block.empty());
    EXPECT_EQ(decodeInternalBlockWithEmptySchema(block), TSDB_CODE_INVALID_MSG);
  }
}

TEST(BoundedInternalBlockDecoderTest, RejectsSelfConsistentNonCanonicalFixedWidthWithoutTemplate) {
  auto wrongIntWidth = makeInternalSingleColumnBlock(TSDB_DATA_TYPE_INT, SHORT_BYTES, {{1, 0}});
  ASSERT_FALSE(wrongIntWidth.empty());
  EXPECT_EQ(decodeInternalBlockWithEmptySchema(wrongIntWidth), TSDB_CODE_INVALID_MSG);
}

TEST(StreamExchangeMultiGroupTest, QueuedDataPublishesOnlyMatchingOwnedLineageOnEachPop) {
  ExchangeMultiGroupOwner owner;
  ASSERT_NE(owner.handle, nullptr);
  auto first = makeStrictExchangeDataResponse(101, {10, 20, 30, 40}, {{3, 0}, {7, 2}});
  auto second = makeStrictExchangeDataResponse(101, {50, 60}, {{9, 0}});
  auto third = makeStrictExchangeDataResponse(101, {70}, {{11, 0}});
  ASSERT_FALSE(first.bytes.empty());
  ASSERT_FALSE(second.bytes.empty());
  ASSERT_FALSE(third.bytes.empty());

  ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, first.bytes.data(), first.bytes.size()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, second.bytes.data(), second.bytes.size()), TSDB_CODE_SUCCESS);
  EXPECT_EQ(exchangeTestGetRuntimeLineageAlias(owner.handle), nullptr);

  int32_t strictItems = -1;
  int32_t ordinaryBlocks = -1;
  ASSERT_EQ(exchangeTestGetResultQueueSizes(owner.handle, &strictItems, &ordinaryBlocks), TSDB_CODE_SUCCESS);
  EXPECT_EQ(strictItems, 2);
  EXPECT_EQ(ordinaryBlocks, 0);

  SSDataBlock  *pBlock = nullptr;
  const SArray *pPublished = nullptr;
  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pPublished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 4);
  EXPECT_EQ(decodeProjectTestLineage(pPublished), (std::vector<std::pair<int32_t, int32_t>>{{3, 0}, {7, 2}}));
  const SArray *pFirstPublished = pPublished;

  ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, third.bytes.data(), third.bytes.size()), TSDB_CODE_SUCCESS);
  EXPECT_EQ(exchangeTestGetRuntimeLineageAlias(owner.handle), pFirstPublished);
  EXPECT_EQ(decodeProjectTestLineage(exchangeTestGetRuntimeLineageAlias(owner.handle)),
            (std::vector<std::pair<int32_t, int32_t>>{{3, 0}, {7, 2}}));

  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pPublished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 2);
  EXPECT_NE(pPublished, pFirstPublished);
  EXPECT_EQ(decodeProjectTestLineage(pPublished), (std::vector<std::pair<int32_t, int32_t>>{{9, 0}}));

  ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, first.bytes.data(), first.bytes.size()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(exchangeTestResetMultiGroupOwner(owner.handle), TSDB_CODE_SUCCESS);
  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pPublished), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pBlock, nullptr);
  EXPECT_EQ(pPublished, nullptr);
}

TEST(StreamExchangeMultiGroupTest, RejectsMalformedRpcBlockFrameAndIdentityBeforeQueueing) {
  std::vector<ExchangeFetchResponseBytes> malformed;

  auto shortHeader = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  shortHeader.bytes.resize(sizeof(SRetrieveTableRsp) - 1);
  malformed.push_back(std::move(shortHeader));

  auto payloadOutsideRpc = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  setExchangeResponsePayloadLen(&payloadOutsideRpc,
                                static_cast<int32_t>(payloadOutsideRpc.bytes.size() - sizeof(SRetrieveTableRsp) + 1));
  malformed.push_back(std::move(payloadOutsideRpc));

  auto          blockOutsidePayload = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  const int32_t overflowingBlock = INT32_MAX;
  memcpy(reinterpret_cast<SRetrieveTableRsp *>(blockOutsidePayload.bytes.data())->data, &overflowingBlock, INT_BYTES);
  malformed.push_back(std::move(blockOutsidePayload));

  auto truncatedFrame = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  truncatedFrame.bytes.resize(truncatedFrame.bytes.size() - 1);
  setExchangeResponsePayloadLen(&truncatedFrame,
                                static_cast<int32_t>(truncatedFrame.bytes.size() - sizeof(SRetrieveTableRsp)));
  malformed.push_back(std::move(truncatedFrame));

  auto outerInnerMismatch = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  reinterpret_cast<SRetrieveTableRsp *>(outerInnerMismatch.bytes.data())->streamBlockType = STREAM_RUNNER_FETCH_EOG;
  malformed.push_back(std::move(outerInnerMismatch));

  auto gidMismatch =
      makeStrictExchangeResponse(STREAM_RUNNER_FETCH_DATA, STREAM_RUNNER_FETCH_DATA, 101, 202, {10}, {{3, 0}});
  malformed.push_back(std::move(gidMismatch));

  auto missingFrame =
      makeStrictExchangeResponse(STREAM_RUNNER_FETCH_DATA, STREAM_RUNNER_FETCH_DATA, 101, 101, {10}, {{3, 0}}, false);
  malformed.push_back(std::move(missingFrame));

  auto columnCountMismatch = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  reinterpret_cast<SRetrieveTableRsp *>(columnCountMismatch.bytes.data())->numOfCols = htobe64(2);
  malformed.push_back(std::move(columnCountMismatch));

  auto           invalidLineage = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  auto          *pInvalidRsp = reinterpret_cast<SRetrieveTableRsp *>(invalidLineage.bytes.data());
  const uint64_t invalidFirstStart = htobe64((UINT64_C(1) << 32) | UINT64_C(3));
  memcpy(pInvalidRsp->data + invalidLineage.frameOffset + 18, &invalidFirstStart, sizeof(invalidFirstStart));
  malformed.push_back(std::move(invalidLineage));

  for (const auto &response : malformed) {
    SCOPED_TRACE(response.bytes.size());
    ExchangeMultiGroupOwner owner;
    ASSERT_NE(owner.handle, nullptr);
    ASSERT_FALSE(response.bytes.empty());
    EXPECT_EQ(exchangeTestAcceptFetchResponse(owner.handle, response.bytes.data(), response.bytes.size()),
              TSDB_CODE_INVALID_MSG);
    int32_t strictItems = -1;
    int32_t ordinaryBlocks = -1;
    ASSERT_EQ(exchangeTestGetResultQueueSizes(owner.handle, &strictItems, &ordinaryBlocks), TSDB_CODE_SUCCESS);
    EXPECT_EQ(strictItems, 0);
    EXPECT_EQ(ordinaryBlocks, 0);
  }
}

TEST(StreamExchangeMultiGroupTest, RejectsNegativeLegacyBlockLengthBeforeDecode) {
  auto legacy = makeLegacyExchangeResponse(101, {10});
  ASSERT_FALSE(legacy.bytes.empty());

  const int32_t negativeBlockSize = -1;
  memcpy(reinterpret_cast<SRetrieveTableRsp *>(legacy.bytes.data())->data, &negativeBlockSize, INT_BYTES);

  ExchangeMultiGroupOwner owner(false, false, false);
  ASSERT_NE(owner.handle, nullptr);
  EXPECT_EQ(exchangeTestAcceptFetchResponse(owner.handle, legacy.bytes.data(), legacy.bytes.size()),
            TSDB_CODE_INVALID_MSG);
}

TEST(StreamExchangeMultiGroupTest, PublishesDataThenEogThenEofWithoutConflation) {
  ExchangeMultiGroupOwner owner;
  ASSERT_NE(owner.handle, nullptr);
  auto data = makeStrictExchangeDataResponse(101, {10, 20}, {{3, 0}});
  auto eog = makeStrictExchangeEogResponse(101);
  auto eof = makeLegacyExchangeResponse(0, {}, true);
  ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, data.bytes.data(), data.bytes.size()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, eog.bytes.data(), eog.bytes.size()), TSDB_CODE_SUCCESS);
  ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, eof.bytes.data(), eof.bytes.size()), TSDB_CODE_SUCCESS);

  SSDataBlock  *pBlock = nullptr;
  const SArray *pPublished = nullptr;
  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pPublished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_EQ(pBlock->info.rows, 2);
  ASSERT_NE(pPublished, nullptr);

  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pPublished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  EXPECT_EQ(pPublished, nullptr);

  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pPublished), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pBlock, nullptr);
  EXPECT_EQ(pPublished, nullptr);
}

TEST(StreamExchangeMultiGroupTest, EogBypassesFilterLimitAndGenericZeroRowSkipping) {
  ExchangeMultiGroupOwner owner({202}, 1);
  ASSERT_NE(owner.handle, nullptr);
  ASSERT_EQ(exchangeTestEnableFilterLimitGuards(owner.handle), TSDB_CODE_SUCCESS);
  auto eog = makeStrictExchangeEogResponse(202);
  ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, eog.bytes.data(), eog.bytes.size()), TSDB_CODE_SUCCESS);

  SSDataBlock *pBlock = nullptr;
  ASSERT_EQ(exchangeTestLoadNextMultiGroupResult(owner.handle, &pBlock), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 202);
}

TEST(StreamExchangeMultiGroupTest, ExactGateRejectsMixedProtocolAndLegacyUsesOrdinaryQueue) {
  auto strict = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  auto legacy = makeLegacyExchangeResponse(101, {10});
  ASSERT_FALSE(strict.bytes.empty());
  ASSERT_FALSE(legacy.bytes.empty());

  for (const auto gate :
       {std::make_tuple(false, true, true), std::make_tuple(true, false, true), std::make_tuple(true, true, false)}) {
    ExchangeMultiGroupOwner owner(std::get<0>(gate), std::get<1>(gate), std::get<2>(gate));
    ASSERT_NE(owner.handle, nullptr);
    EXPECT_EQ(exchangeTestAcceptFetchResponse(owner.handle, strict.bytes.data(), strict.bytes.size()),
              TSDB_CODE_INVALID_MSG);
  }

  auto unknown = strict;
  reinterpret_cast<SRetrieveTableRsp *>(unknown.bytes.data())->streamBlockType = 99;
  ExchangeMultiGroupOwner nonStrictOwner(false, false, false);
  ASSERT_NE(nonStrictOwner.handle, nullptr);
  EXPECT_EQ(exchangeTestAcceptFetchResponse(nonStrictOwner.handle, unknown.bytes.data(), unknown.bytes.size()),
            TSDB_CODE_INVALID_MSG);

  for (const auto gate :
       {std::make_tuple(false, true, true), std::make_tuple(true, false, true), std::make_tuple(false, false, false)}) {
    ExchangeMultiGroupOwner owner(std::get<0>(gate), std::get<1>(gate), std::get<2>(gate));
    ASSERT_NE(owner.handle, nullptr);
    ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, legacy.bytes.data(), legacy.bytes.size()),
              TSDB_CODE_SUCCESS);
    int32_t strictItems = -1;
    int32_t ordinaryBlocks = -1;
    ASSERT_EQ(exchangeTestGetResultQueueSizes(owner.handle, &strictItems, &ordinaryBlocks), TSDB_CODE_SUCCESS);
    EXPECT_EQ(strictItems, 0);
    EXPECT_EQ(ordinaryBlocks, 1);
  }

  ExchangeMultiGroupOwner strictOwner;
  ASSERT_NE(strictOwner.handle, nullptr);
  EXPECT_EQ(exchangeTestAcceptFetchResponse(strictOwner.handle, legacy.bytes.data(), legacy.bytes.size()),
            TSDB_CODE_INVALID_MSG);
}

TEST(StreamExchangeMultiGroupTest, HeaderNormalizationUsesStrict64BitAndLegacy32BitColumnCounts) {
  auto strict = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  auto legacy = makeLegacyExchangeResponse(101, {10});
  ASSERT_FALSE(strict.bytes.empty());
  ASSERT_FALSE(legacy.bytes.empty());

  constexpr int64_t strictColumnCount = (INT64_C(1) << 32) + 3;
  reinterpret_cast<SRetrieveTableRsp *>(strict.bytes.data())->numOfCols = htobe64(strictColumnCount);
  reinterpret_cast<SRetrieveTableRsp *>(legacy.bytes.data())->numOfCols = htonl(3);

  int64_t normalized = 0;
  ASSERT_EQ(exchangeTestNormalizeFetchHeader(strict.bytes.data(), strict.bytes.size(), &normalized), TSDB_CODE_SUCCESS);
  EXPECT_EQ(normalized, strictColumnCount);
  ASSERT_EQ(exchangeTestNormalizeFetchHeader(legacy.bytes.data(), legacy.bytes.size(), &normalized), TSDB_CODE_SUCCESS);
  EXPECT_EQ(normalized, 3);
}

TEST(StreamExchangeMultiGroupTest, LocalFetchDerivesLogicalLengthWithinOwnedAllocation) {
  auto strict = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  auto localEof = makeLegacyExchangeResponse(0, {}, true);
  localEof.bytes.resize(sizeof(SRetrieveTableRsp));
  ASSERT_FALSE(strict.bytes.empty());
  ASSERT_EQ(localEof.bytes.size(), sizeof(SRetrieveTableRsp));

  uint32_t logicalLen = 0;
  ASSERT_EQ(exchangeTestGetLocalFetchResponseLen(strict.bytes.data(), strict.bytes.size(), &logicalLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(logicalLen, strict.bytes.size());
  ASSERT_EQ(exchangeTestGetLocalFetchResponseLen(localEof.bytes.data(), localEof.bytes.size(), &logicalLen),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(logicalLen, localEof.bytes.size());

  auto  outsideAllocation = strict;
  auto *pRsp = reinterpret_cast<SRetrieveTableRsp *>(outsideAllocation.bytes.data());
  pRsp->compLen = htonl(static_cast<int32_t>(outsideAllocation.bytes.size() - sizeof(SRetrieveTableRsp) + 1024));
  EXPECT_EQ(
      exchangeTestGetLocalFetchResponseLen(outsideAllocation.bytes.data(), outsideAllocation.bytes.size(), &logicalLen),
      TSDB_CODE_INVALID_MSG);
}

TEST(StreamExchangeMultiGroupTest, ResetPreservesForeignRuntimeAliasAndDestroyOwnsQueuedResults) {
  auto data = makeStrictExchangeDataResponse(101, {10, 20}, {{3, 0}});
  ASSERT_FALSE(data.bytes.empty());
  {
    ExchangeMultiGroupOwner owner;
    ASSERT_NE(owner.handle, nullptr);
    ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, data.bytes.data(), data.bytes.size()), TSDB_CODE_SUCCESS);
    SSDataBlock  *pBlock = nullptr;
    const SArray *pPublished = nullptr;
    ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pPublished), TSDB_CODE_SUCCESS);
    ASSERT_NE(pPublished, nullptr);

    SArray *pForeign = taosArrayInit(1, sizeof(int64_t));
    ASSERT_NE(pForeign, nullptr);
    exchangeTestSetRuntimeLineageAlias(owner.handle, pForeign);
    ASSERT_EQ(exchangeTestResetMultiGroupOwner(owner.handle), TSDB_CODE_SUCCESS);
    EXPECT_EQ(exchangeTestGetRuntimeLineageAlias(owner.handle), pForeign);
    exchangeTestSetRuntimeLineageAlias(owner.handle, nullptr);
    taosArrayDestroy(pForeign);

    ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, data.bytes.data(), data.bytes.size()), TSDB_CODE_SUCCESS);
  }
}

TEST(StreamExchangeMultiGroupTest, StrictDataUsesBoundedInternalDecoder) {
  constexpr size_t kInternalDataLenOffset = sizeof(int32_t);
  auto             response = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  ASSERT_FALSE(response.bytes.empty());

  auto         *pRsp = reinterpret_cast<SRetrieveTableRsp *>(response.bytes.data());
  const int32_t wrongInternalLen = response.blockSize + 1;
  memcpy(pRsp->data + INT_BYTES * 2 + kInternalDataLenOffset, &wrongInternalLen, sizeof(wrongInternalLen));

  ExchangeMultiGroupOwner owner;
  ASSERT_NE(owner.handle, nullptr);
  EXPECT_EQ(exchangeTestAcceptFetchResponse(owner.handle, response.bytes.data(), response.bytes.size()),
            TSDB_CODE_INVALID_MSG);
}

TEST(StreamExchangeMultiGroupTest, CloseDetachesPublishedLineageBeforeDelayedRefDestructor) {
  auto response = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  ASSERT_FALSE(response.bytes.empty());

  ExchangeMultiGroupOwner owner;
  ASSERT_NE(owner.handle, nullptr);
  ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, response.bytes.data(), response.bytes.size()),
            TSDB_CODE_SUCCESS);
  SSDataBlock  *pBlock = nullptr;
  const SArray *pPublished = nullptr;
  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pPublished), TSDB_CODE_SUCCESS);
  ASSERT_NE(pPublished, nullptr);

  ASSERT_EQ(owner.beginDelayedDestroy(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(exchangeTestGetRuntimeLineageAlias(owner.handle), nullptr);
  EXPECT_EQ(owner.finishDelayedDestroy(), TSDB_CODE_SUCCESS);
}

TEST(StreamExchangeMultiGroupTest, NonStrictOwnersNeverAllocateStrictQueue) {
  auto strict = makeStrictExchangeDataResponse(101, {10}, {{3, 0}});
  auto legacy = makeLegacyExchangeResponse(101, {10});
  ASSERT_FALSE(strict.bytes.empty());
  ASSERT_FALSE(legacy.bytes.empty());

  for (const auto gate : {std::make_tuple(false, true, true), std::make_tuple(true, false, true),
                          std::make_tuple(true, true, false), std::make_tuple(false, false, false)}) {
    ExchangeMultiGroupOwner owner(std::get<0>(gate), std::get<1>(gate), std::get<2>(gate));
    ASSERT_NE(owner.handle, nullptr);
    EXPECT_FALSE(exchangeTestIsStrictResultQueueAllocated(owner.handle));
    ASSERT_EQ(exchangeTestAcceptFetchResponse(owner.handle, legacy.bytes.data(), legacy.bytes.size()),
              TSDB_CODE_SUCCESS);
    EXPECT_FALSE(exchangeTestIsStrictResultQueueAllocated(owner.handle));
  }

  ExchangeMultiGroupOwner strictOwner;
  ASSERT_NE(strictOwner.handle, nullptr);
  EXPECT_FALSE(exchangeTestIsStrictResultQueueAllocated(strictOwner.handle));
  ASSERT_EQ(exchangeTestAcceptFetchResponse(strictOwner.handle, strict.bytes.data(), strict.bytes.size()),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(exchangeTestIsStrictResultQueueAllocated(strictOwner.handle));
}

TEST(StreamExchangeMultiGroupTest, TwoSourcesPauseAtEachCanonicalGroupBarrier) {
  ExchangeMultiGroupOwner owner({101, 202}, 2);
  ASSERT_NE(owner.handle, nullptr);

  auto data101Source0 = makeStrictExchangeDataResponse(101, {10}, {{1, 0}});
  auto data101Source1First = makeStrictExchangeDataResponse(101, {20}, {{2, 0}});
  auto data101Source1Second = makeStrictExchangeDataResponse(101, {21}, {{3, 0}});
  auto data202Source0 = makeStrictExchangeDataResponse(202, {30}, {{4, 0}});
  auto eog101 = makeStrictExchangeEogResponse(101);
  auto eog202 = makeStrictExchangeEogResponse(202);
  auto eof = makeLegacyExchangeResponse(0, {}, true);

  auto accept = [&](int32_t sourceIndex, const ExchangeFetchResponseBytes &response) {
    return exchangeTestAcceptSourceFetchResponse(owner.handle, sourceIndex, response.bytes.data(),
                                                 static_cast<int32_t>(response.bytes.size()));
  };
  auto pop = [&]() {
    SSDataBlock  *pBlock = nullptr;
    const SArray *pLineage = nullptr;
    EXPECT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pLineage), TSDB_CODE_SUCCESS);
    return pBlock;
  };
  auto expectData = [&](SSDataBlock *pBlock, uint64_t gid, int32_t value) {
    ASSERT_NE(pBlock, nullptr);
    ASSERT_FALSE(qIsMultiGroupEogBlock(pBlock));
    ASSERT_EQ(pBlock->info.id.baseGId, gid);
    ASSERT_EQ(pBlock->info.rows, 1);
    auto *pColumn = static_cast<SColumnInfoData *>(taosArrayGet(pBlock->pDataBlock, 0));
    ASSERT_NE(pColumn, nullptr);
    EXPECT_EQ(*reinterpret_cast<int32_t *>(colDataGetData(pColumn, 0)), value);
  };
  auto expectEog = [&](SSDataBlock *pBlock, uint64_t gid) {
    ASSERT_NE(pBlock, nullptr);
    EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
    EXPECT_EQ(pBlock->info.id.baseGId, gid);
  };

  ASSERT_EQ(accept(0, data101Source0), TSDB_CODE_SUCCESS);
  expectData(pop(), 101, 10);
  ASSERT_EQ(accept(1, data101Source1First), TSDB_CODE_SUCCESS);
  expectData(pop(), 101, 20);

  ASSERT_EQ(accept(0, eog101), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(exchangeTestCanPollSource(owner.handle, 0));
  EXPECT_EQ(pop(), nullptr);
  ASSERT_EQ(accept(1, data101Source1Second), TSDB_CODE_SUCCESS);
  expectData(pop(), 101, 21);
  ASSERT_EQ(accept(1, eog101), TSDB_CODE_SUCCESS);
  expectEog(pop(), 101);
  EXPECT_TRUE(exchangeTestCanPollSource(owner.handle, 0));

  ASSERT_EQ(accept(0, data202Source0), TSDB_CODE_SUCCESS);
  expectData(pop(), 202, 30);
  ASSERT_EQ(accept(0, eog202), TSDB_CODE_SUCCESS);
  EXPECT_FALSE(exchangeTestCanPollSource(owner.handle, 0));
  EXPECT_EQ(pop(), nullptr);
  ASSERT_EQ(accept(1, eog202), TSDB_CODE_SUCCESS);
  expectEog(pop(), 202);

  ASSERT_EQ(accept(0, eof), TSDB_CODE_SUCCESS);
  ASSERT_EQ(accept(1, eof), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(exchangeTestAllSourcesExhausted(owner.handle));
  EXPECT_EQ(pop(), nullptr);
}

TEST(StreamExchangeMultiGroupTest, DuplicateSourceEogIsFatalAndClearsResults) {
  ExchangeMultiGroupOwner owner({101}, 2);
  ASSERT_NE(owner.handle, nullptr);
  auto eog = makeStrictExchangeEogResponse(101);
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, eog.bytes.data(), eog.bytes.size()),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, eog.bytes.data(), eog.bytes.size()),
            TSDB_CODE_INVALID_MSG);
  int32_t strictItems = -1;
  int32_t ordinaryBlocks = -1;
  ASSERT_EQ(exchangeTestGetResultQueueSizes(owner.handle, &strictItems, &ordinaryBlocks), TSDB_CODE_SUCCESS);
  EXPECT_EQ(strictItems, 0);
  EXPECT_EQ(exchangeTestGetRuntimeLineageAlias(owner.handle), nullptr);
}

TEST(StreamExchangeMultiGroupTest, WrongCanonicalEogIsFatal) {
  ExchangeMultiGroupOwner owner({101}, 1);
  ASSERT_NE(owner.handle, nullptr);
  auto data = makeStrictExchangeDataResponse(101, {10}, {{1, 0}});
  auto eog = makeStrictExchangeEogResponse(202);
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, data.bytes.data(), data.bytes.size()),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, eog.bytes.data(), eog.bytes.size()),
            TSDB_CODE_INVALID_MSG);
  int32_t strictItems = -1;
  int32_t ordinaryBlocks = -1;
  ASSERT_EQ(exchangeTestGetResultQueueSizes(owner.handle, &strictItems, &ordinaryBlocks), TSDB_CODE_SUCCESS);
  EXPECT_EQ(strictItems, 0);
}

TEST(StreamExchangeMultiGroupTest, NextGroupDataBeforeCurrentBarrierIsFatal) {
  ExchangeMultiGroupOwner owner({101, 202}, 2);
  ASSERT_NE(owner.handle, nullptr);
  auto eog101 = makeStrictExchangeEogResponse(101);
  auto data202 = makeStrictExchangeDataResponse(202, {30}, {{4, 0}});
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, eog101.bytes.data(), eog101.bytes.size()),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 1, data202.bytes.data(), data202.bytes.size()),
            TSDB_CODE_INVALID_MSG);
}

TEST(StreamExchangeMultiGroupTest, NextGroupEogBeforeCurrentBarrierIsFatal) {
  ExchangeMultiGroupOwner owner({101, 202}, 2);
  ASSERT_NE(owner.handle, nullptr);
  auto eog101 = makeStrictExchangeEogResponse(101);
  auto eog202 = makeStrictExchangeEogResponse(202);
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, eog101.bytes.data(), eog101.bytes.size()),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 1, eog202.bytes.data(), eog202.bytes.size()),
            TSDB_CODE_INVALID_MSG);
}

TEST(StreamExchangeMultiGroupTest, AllEmptyGroupStillEmitsOneMergedEog) {
  ExchangeMultiGroupOwner owner({101}, 2);
  ASSERT_NE(owner.handle, nullptr);
  auto eog = makeStrictExchangeEogResponse(101);
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, eog.bytes.data(), eog.bytes.size()),
            TSDB_CODE_SUCCESS);

  SSDataBlock  *pBlock = nullptr;
  const SArray *pLineage = nullptr;
  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pLineage), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pBlock, nullptr);

  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 1, eog.bytes.data(), eog.bytes.size()),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pLineage), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pLineage), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pBlock, nullptr);
}

TEST(StreamExchangeMultiGroupTest, EofBeforeCurrentGroupEogIsFatal) {
  ExchangeMultiGroupOwner owner({101}, 1);
  ASSERT_NE(owner.handle, nullptr);
  auto eof = makeLegacyExchangeResponse(0, {}, true);
  EXPECT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, eof.bytes.data(), eof.bytes.size()),
            TSDB_CODE_INVALID_MSG);
}

TEST(StreamExchangeMultiGroupTest, DataAfterSourceEogIsFatalAndClearsPublishedLineage) {
  ExchangeMultiGroupOwner owner({101}, 2);
  ASSERT_NE(owner.handle, nullptr);
  auto data = makeStrictExchangeDataResponse(101, {10}, {{1, 0}});
  auto eog = makeStrictExchangeEogResponse(101);
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, data.bytes.data(), data.bytes.size()),
            TSDB_CODE_SUCCESS);
  SSDataBlock  *pBlock = nullptr;
  const SArray *pLineage = nullptr;
  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pLineage), TSDB_CODE_SUCCESS);
  ASSERT_NE(pLineage, nullptr);
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, eog.bytes.data(), eog.bytes.size()),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, data.bytes.data(), data.bytes.size()),
            TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(exchangeTestGetRuntimeLineageAlias(owner.handle), nullptr);
}

TEST(StreamExchangeMultiGroupTest, DataOrEogAfterFinalBarrierIsFatal) {
  auto finalEog = makeStrictExchangeEogResponse(101);
  auto lateData = makeStrictExchangeDataResponse(101, {10}, {{1, 0}});

  for (bool sendData : {true, false}) {
    SCOPED_TRACE(sendData);
    ExchangeMultiGroupOwner owner({101}, 1);
    ASSERT_NE(owner.handle, nullptr);
    ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, finalEog.bytes.data(), finalEog.bytes.size()),
              TSDB_CODE_SUCCESS);
    SSDataBlock  *pBlock = nullptr;
    const SArray *pLineage = nullptr;
    ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pLineage), TSDB_CODE_SUCCESS);
    ASSERT_NE(pBlock, nullptr);
    const auto &late = sendData ? lateData : finalEog;
    EXPECT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, late.bytes.data(), late.bytes.size()),
              TSDB_CODE_INVALID_MSG);
  }
}

TEST(StreamExchangeMultiGroupTest, SingletonSourceAndGroupUsesBarrierThenEof) {
  ExchangeMultiGroupOwner owner({101}, 1);
  ASSERT_NE(owner.handle, nullptr);
  auto data = makeStrictExchangeDataResponse(101, {10}, {{1, 0}});
  auto eog = makeStrictExchangeEogResponse(101);
  auto eof = makeLegacyExchangeResponse(0, {}, true);
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, data.bytes.data(), data.bytes.size()),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, eog.bytes.data(), eog.bytes.size()),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, eof.bytes.data(), eof.bytes.size()),
            TSDB_CODE_SUCCESS);

  SSDataBlock  *pBlock = nullptr;
  const SArray *pLineage = nullptr;
  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pLineage), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pLineage), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_TRUE(exchangeTestAllSourcesExhausted(owner.handle));
}

TEST(StreamExchangeMultiGroupTest, ResetMidBarrierRestartsCanonicalCursorAndSourceState) {
  ExchangeMultiGroupOwner owner({101, 202}, 2);
  ASSERT_NE(owner.handle, nullptr);
  auto eog101 = makeStrictExchangeEogResponse(101);
  auto data101 = makeStrictExchangeDataResponse(101, {10}, {{1, 0}});
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, eog101.bytes.data(), eog101.bytes.size()),
            TSDB_CODE_SUCCESS);
  EXPECT_FALSE(exchangeTestCanPollSource(owner.handle, 0));
  ASSERT_EQ(exchangeTestResetMultiGroupOwner(owner.handle), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(exchangeTestCanPollSource(owner.handle, 0));
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, data101.bytes.data(), data101.bytes.size()),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, eog101.bytes.data(), eog101.bytes.size()),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 1, eog101.bytes.data(), eog101.bytes.size()),
            TSDB_CODE_SUCCESS);

  SSDataBlock  *pBlock = nullptr;
  const SArray *pLineage = nullptr;
  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pLineage), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_FALSE(qIsMultiGroupEogBlock(pBlock));
  ASSERT_EQ(exchangeTestPopMultiGroupResult(owner.handle, &pBlock, &pLineage), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  EXPECT_TRUE(qIsMultiGroupEogBlock(pBlock));
  EXPECT_EQ(pBlock->info.id.baseGId, 101);
}

TEST(StreamExchangeMultiGroupTest, StrictFirstEventRequiresNonEmptyCanonicalOrder) {
  ExchangeMultiGroupOwner owner({}, 1);
  ASSERT_NE(owner.handle, nullptr);
  auto data = makeStrictExchangeDataResponse(101, {10}, {{1, 0}});
  EXPECT_EQ(exchangeTestAcceptSourceFetchResponse(owner.handle, 0, data.bytes.data(), data.bytes.size()),
            TSDB_CODE_INVALID_MSG);
}

TEST(execUtilTest, interpMergeNodeBuildersHandleAppendFailure) {
  using AppendFn = int32_t (*)(SNodeList**, SNode*);
  const AppendFn failures[] = {
      failInterpNodeAppend<QUERY_NODE_SLOT_DESC>,
      failInterpNodeAppend<QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN>,
      failInterpNodeAppend<QUERY_NODE_TARGET>,
      failInterpNodeAppend<QUERY_NODE_ORDER_BY_EXPR>,
  };
  for (size_t i = 0; i < sizeof(failures) / sizeof(failures[0]); ++i) {
    SCOPED_TRACE(i);
    InterpNodeDestroySpy spy;
    spy.stub.set(nodesListMakeStrictAppend, failures[i]);
    auto* plan = createInterpMergeTestPhysNode();
    EXPECT_EQ(plan, nullptr);
    nodesDestroyNode(reinterpret_cast<SNode*>(plan));
    EXPECT_NE(spy.failed, nullptr);
    EXPECT_EQ(spy.destroys, 1);
  }
}

// The three phase windows must saturate at the timestamp limits instead of
// wrapping INT64_MIN - 1 or INT64_MAX + 1 into a nonempty side window.
TEST(execUtilTest, interpPhaseWindowsSaturateTimestampLimits) {
  SQueryTableDataCond cond = {0};
  cond.twindows = {INT64_MIN, INT64_MAX};
  STimeWindow range = {INT64_MIN, INT64_MAX};

  ASSERT_EQ(initInterpPhaseWindows(&cond, &range,
                                   INTERP_MODE_MAIN_FIRST,
                                   FILL_MODE_LINEAR),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(cond.twindows.skey, INT64_MIN);
  EXPECT_EQ(cond.twindows.ekey, INT64_MAX);
  EXPECT_GT(cond.extTwindows[0].skey, cond.extTwindows[0].ekey);
  EXPECT_GT(cond.extTwindows[1].skey, cond.extTwindows[1].ekey);

  cond.twindows = range;
  ASSERT_EQ(initInterpPhaseWindows(&cond, &range,
                                   INTERP_MODE_LEGACY,
                                   FILL_MODE_LINEAR),
            TSDB_CODE_SUCCESS);
  EXPECT_GT(cond.extTwindows[0].skey, cond.extTwindows[0].ekey);
  EXPECT_GT(cond.extTwindows[1].skey, cond.extTwindows[1].ekey);
}

// MAIN_FIRST retains side-only groups for NULL fill, while MULTI_GROUP has a
// closed MAIN membership and can statically prune both reference windows.
TEST(execUtilTest, interpPhaseWindowsApplyModeAwarePruning) {
  const STimeWindow whereRange = {0, 99};
  const STimeWindow interpRange = {20, 79};
  SQueryTableDataCond mainFirst = {0};
  mainFirst.twindows = whereRange;

  ASSERT_EQ(initInterpPhaseWindows(&mainFirst, &interpRange,
                                   INTERP_MODE_MAIN_FIRST,
                                   FILL_MODE_NULL),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(mainFirst.twindows.skey, 20);
  EXPECT_EQ(mainFirst.twindows.ekey, 79);
  EXPECT_EQ(mainFirst.extTwindows[0].skey, 0);
  EXPECT_EQ(mainFirst.extTwindows[0].ekey, 19);
  EXPECT_EQ(mainFirst.extTwindows[1].skey, 80);
  EXPECT_EQ(mainFirst.extTwindows[1].ekey, 99);

  SQueryTableDataCond multiGroup = {0};
  multiGroup.twindows = whereRange;
  ASSERT_EQ(initInterpPhaseWindows(&multiGroup, &interpRange,
                                   INTERP_MODE_MULTI_GROUP,
                                   FILL_MODE_NULL),
            TSDB_CODE_SUCCESS);
  EXPECT_GT(multiGroup.extTwindows[0].skey,
            multiGroup.extTwindows[0].ekey);
  EXPECT_GT(multiGroup.extTwindows[1].skey,
            multiGroup.extTwindows[1].ekey);

  multiGroup.twindows = whereRange;
  ASSERT_EQ(initInterpPhaseWindows(&multiGroup, &interpRange,
                                   INTERP_MODE_MULTI_GROUP,
                                   FILL_MODE_VALUE_F),
            TSDB_CODE_SUCCESS);
  EXPECT_GT(multiGroup.extTwindows[0].skey,
            multiGroup.extTwindows[0].ekey);
  EXPECT_GT(multiGroup.extTwindows[1].skey,
            multiGroup.extTwindows[1].ekey);
}

// One-sided fill pruning remains mode-independent: PREV never consumes the
// NEXT window, and NEXT never consumes the PREV window.
TEST(execUtilTest, interpPhaseWindowsPruneUnusedFillSide) {
  const STimeWindow whereRange = {0, 99};
  const STimeWindow interpRange = {20, 79};
  SQueryTableDataCond cond = {0};
  cond.twindows = whereRange;

  ASSERT_EQ(initInterpPhaseWindows(&cond, &interpRange,
                                   INTERP_MODE_MAIN_FIRST,
                                   FILL_MODE_PREV),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(cond.extTwindows[0].skey, 0);
  EXPECT_EQ(cond.extTwindows[0].ekey, 19);
  EXPECT_GT(cond.extTwindows[1].skey, cond.extTwindows[1].ekey);

  cond.twindows = whereRange;
  ASSERT_EQ(initInterpPhaseWindows(&cond, &interpRange,
                                   INTERP_MODE_MAIN_FIRST,
                                   FILL_MODE_NEXT),
            TSDB_CODE_SUCCESS);
  EXPECT_GT(cond.extTwindows[0].skey, cond.extTwindows[0].ekey);
  EXPECT_EQ(cond.extTwindows[1].skey, 80);
  EXPECT_EQ(cond.extTwindows[1].ekey, 99);
}

TEST(execUtilTest, interpMainFirstEdgeReferenceRequirements) {
  EXPECT_FALSE(interpGroupNeedsPrev(TSDB_FILL_PREV, false));
  EXPECT_FALSE(interpGroupNeedsNext(TSDB_FILL_NEXT, false));

  EXPECT_TRUE(interpGroupNeedsPrev(TSDB_FILL_PREV, true));
  EXPECT_FALSE(interpGroupNeedsNext(TSDB_FILL_PREV, true));
  EXPECT_FALSE(interpGroupNeedsPrev(TSDB_FILL_NEXT, true));
  EXPECT_TRUE(interpGroupNeedsNext(TSDB_FILL_NEXT, true));

  EXPECT_TRUE(interpGroupNeedsPrev(TSDB_FILL_LINEAR, true));
  EXPECT_TRUE(interpGroupNeedsNext(TSDB_FILL_LINEAR, true));
  EXPECT_TRUE(interpGroupNeedsPrev(TSDB_FILL_NEAR, true));
  EXPECT_TRUE(interpGroupNeedsNext(TSDB_FILL_NEAR, true));

  EXPECT_FALSE(interpGroupNeedsPrev(TSDB_FILL_NULL, true));
  EXPECT_FALSE(interpGroupNeedsNext(TSDB_FILL_NULL, true));
  EXPECT_FALSE(interpGroupNeedsPrev(TSDB_FILL_NULL_F, true));
  EXPECT_FALSE(interpGroupNeedsNext(TSDB_FILL_NULL_F, true));
  EXPECT_FALSE(interpGroupNeedsPrev(TSDB_FILL_SET_VALUE, true));
  EXPECT_FALSE(interpGroupNeedsNext(TSDB_FILL_SET_VALUE, true));
  EXPECT_FALSE(interpGroupNeedsPrev(TSDB_FILL_SET_VALUE_F, true));
  EXPECT_FALSE(interpGroupNeedsNext(TSDB_FILL_SET_VALUE_F, true));
}

TEST(execUtilTest, interpMainFirstEdgeProductionRequirementsMatchFill) {
  EXPECT_FALSE(interpMainFirstEdgeCanProduce(TSDB_FILL_LINEAR, false,
                                             true, true));
  EXPECT_TRUE(interpMainFirstEdgeCanProduce(TSDB_FILL_LINEAR, true,
                                            false, false));
  EXPECT_FALSE(interpMainFirstEdgeCanProduce(TSDB_FILL_PREV, true,
                                             false, true));
  EXPECT_TRUE(interpMainFirstEdgeCanProduce(TSDB_FILL_PREV, true,
                                            true, false));
  EXPECT_FALSE(interpMainFirstEdgeCanProduce(TSDB_FILL_NEXT, true,
                                             true, false));
  EXPECT_TRUE(interpMainFirstEdgeCanProduce(TSDB_FILL_NEXT, true,
                                            false, true));
  EXPECT_FALSE(interpMainFirstEdgeCanProduce(TSDB_FILL_NEAR, true,
                                             false, false));
  EXPECT_TRUE(interpMainFirstEdgeCanProduce(TSDB_FILL_NEAR, true,
                                            true, false));
  EXPECT_TRUE(interpMainFirstEdgeCanProduce(TSDB_FILL_NEAR, true,
                                            false, true));
  EXPECT_TRUE(interpMainFirstEdgeCanProduce(TSDB_FILL_NULL, false,
                                            false, false));
  EXPECT_TRUE(interpMainFirstEdgeCanProduce(TSDB_FILL_NULL_F, false,
                                            false, false));
  EXPECT_TRUE(interpMainFirstEdgeCanProduce(TSDB_FILL_SET_VALUE, false,
                                            false, false));
  EXPECT_TRUE(interpMainFirstEdgeCanProduce(TSDB_FILL_SET_VALUE_F, false,
                                            false, false));
  EXPECT_FALSE(interpMainFirstEdgeCanProduce(TSDB_FILL_NONE, true,
                                             true, true));
}

TEST(execUtilTest, interpMainFirstLeadHoleLocatesWideFixedGrid) {
  SInterval interval = {};
  interval.interval = 10;

  STimeWindow hole = {};
  int64_t nextTs = 0;
  bool hasHole = false;
  ASSERT_EQ(interpMainFirstGetLeadHole(0, 1000000000005LL,
                                       &interval, &hole, &nextTs,
                                       &hasHole),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(hasHole);
  EXPECT_EQ(hole.skey, 0);
  EXPECT_EQ(hole.ekey, 1000000000000LL);
  EXPECT_EQ(nextTs, 1000000000010LL);

  ASSERT_EQ(interpMainFirstGetLeadHole(0, 100, &interval, &hole,
                                       &nextTs, &hasHole),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(hole.ekey, 90);
  EXPECT_EQ(nextTs, 100);

  ASSERT_EQ(interpMainFirstGetLeadHole(100, 100, &interval, &hole,
                                       &nextTs, &hasHole),
            TSDB_CODE_SUCCESS);
  EXPECT_FALSE(hasHole);
  EXPECT_EQ(nextTs, 100);

  interval.interval = 2;
  ASSERT_EQ(interpMainFirstGetLeadHole(INT64_MAX - 2, INT64_MAX,
                                       &interval, &hole, &nextTs,
                                       &hasHole),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(hole.ekey, INT64_MAX - 2);
  EXPECT_EQ(nextTs, INT64_MAX);

  interval.interval = 3;
  ASSERT_EQ(interpMainFirstGetLeadHole(INT64_MIN, INT64_MAX,
                                       &interval, &hole, &nextTs,
                                       &hasHole),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(hole.ekey, INT64_MAX - 3);
  EXPECT_EQ(nextTs, INT64_MAX);

  interval.interval = 3;
  EXPECT_EQ(interpMainFirstGetLeadHole(INT64_MAX - 2, INT64_MAX,
                                       &interval, &hole, &nextTs,
                                       &hasHole),
            TSDB_CODE_TIME_ERROR);
}

TEST(execUtilTest, interpMainFirstLeadHolePreservesCalendarSequence) {
  timezone_t utc = tzalloc("UTC");
  timezone_t ny = tzalloc("America/New_York");
  ASSERT_NE(utc, nullptr);
  ASSERT_NE(ny, nullptr);

  STimeWindow hole = {};
  int64_t nextTs = 0;
  bool hasHole = false;
  SInterval interval = {};
  interval.interval = 1;
  interval.intervalUnit = TIME_UNIT_MONTH;
  interval.precision = TSDB_TIME_PRECISION_MILLI;
  interval.timezone = utc;
  int64_t start = makeInterpTestTimestamp(utc, 2024, 1, 31);
  int64_t first = makeInterpTestTimestamp(utc, 2024, 4, 15);
  ASSERT_EQ(interpMainFirstGetLeadHole(start, first, &interval,
                                       &hole, &nextTs, &hasHole),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(hasHole);
  EXPECT_EQ(hole.ekey,
            makeInterpTestTimestamp(utc, 2024, 3, 29));
  EXPECT_EQ(nextTs, makeInterpTestTimestamp(utc, 2024, 4, 29));

  interval.intervalUnit = TIME_UNIT_YEAR;
  start = makeInterpTestTimestamp(utc, 2020, 2, 29);
  first = makeInterpTestTimestamp(utc, 2024, 2, 20);
  ASSERT_EQ(interpMainFirstGetLeadHole(start, first, &interval,
                                       &hole, &nextTs, &hasHole),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(hole.ekey,
            makeInterpTestTimestamp(utc, 2023, 2, 28));
  EXPECT_EQ(nextTs, makeInterpTestTimestamp(utc, 2024, 2, 28));

  interval.interval = 86400LL * 1000;
  interval.intervalUnit = TIME_UNIT_DAY;
  interval.timezone = ny;
  start = makeInterpTestTimestamp(ny, 2024, 3, 9);
  first = makeInterpTestTimestamp(ny, 2024, 3, 10, 12);
  ASSERT_EQ(interpMainFirstGetLeadHole(start, first, &interval,
                                       &hole, &nextTs, &hasHole),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(hole.ekey,
            makeInterpTestTimestamp(ny, 2024, 3, 10));
  EXPECT_EQ(nextTs, makeInterpTestTimestamp(ny, 2024, 3, 11));
  EXPECT_EQ(nextTs - hole.ekey, 23LL * 3600 * 1000);

  tzfree(utc);
  tzfree(ny);
}

TEST(execUtilTest, interpMainFirstPendingGroupsUseReadinessFifo) {
  SInterpMainFirstReadiness first = {};
  first.fillType = TSDB_FILL_LINEAR;
  first.firstMainRowSet = true;
  first.groupRowSet = true;
  first.mainFinalized = true;
  first.leadHoleSet = true;
  first.trailHoleSet = true;
  first.extNextRowSet = true;

  SInterpMainFirstReadiness second = first;
  second.extPrevRowSet = true;
  EXPECT_FALSE(interpMainFirstGroupIsReady(&first));
  EXPECT_TRUE(interpMainFirstGroupIsReady(&second));

  SArray* pCompleted = taosArrayInit(2, sizeof(uint64_t));
  ASSERT_NE(pCompleted, nullptr);
  uint64_t secondId = 22;
  ASSERT_NE(taosArrayPush(pCompleted, &secondId), nullptr);
  first.extPrevRowSet = true;
  ASSERT_TRUE(interpMainFirstGroupIsReady(&first));
  uint64_t firstId = 11;
  ASSERT_NE(taosArrayPush(pCompleted, &firstId), nullptr);

  int32_t index = 0;
  uint64_t groupId = 0;
  ASSERT_TRUE(interpMainFirstNextCompletedGroup(pCompleted, &index,
                                                &groupId));
  EXPECT_EQ(groupId, secondId);
  ASSERT_TRUE(interpMainFirstNextCompletedGroup(pCompleted, &index,
                                                &groupId));
  EXPECT_EQ(groupId, firstId);
  EXPECT_FALSE(interpMainFirstNextCompletedGroup(pCompleted, &index,
                                                 &groupId));
  taosArrayDestroy(pCompleted);
}

TEST(execUtilTest, interpMainFirstEmittedGroupsAreNotReentered) {
  SHashObj* pEmittedGroups =
      taosHashInit(4,
                   taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT),
                   false, HASH_NO_LOCK);
  ASSERT_NE(pEmittedGroups, nullptr);

  uint64_t emittedGroupId = 11;
  uint64_t otherGroupId = 22;
  EXPECT_FALSE(interpMainFirstGroupWasEmitted(pEmittedGroups,
                                               emittedGroupId));
  uint8_t emitted = 1;
  ASSERT_EQ(taosHashPut(pEmittedGroups, &emittedGroupId,
                        sizeof(emittedGroupId), &emitted,
                        sizeof(emitted)), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(interpMainFirstGroupWasEmitted(pEmittedGroups,
                                              emittedGroupId));
  EXPECT_FALSE(interpMainFirstGroupWasEmitted(pEmittedGroups,
                                               otherGroupId));

  taosHashCleanup(pEmittedGroups);
}

TEST(execUtilTest, interpMainFirstEmittedGroupNotifiesEachSideOnce) {
  SHashObj* pEmittedGroups = taosHashInit(
      4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT),
      false, HASH_NO_LOCK);
  ASSERT_NE(pEmittedGroups, nullptr);

  uint64_t groupId = 42;
  uint8_t emitted = 1;
  ASSERT_EQ(taosHashPut(pEmittedGroups, &groupId, sizeof(groupId),
                        &emitted, sizeof(emitted)),
            TSDB_CODE_SUCCESS);

  EXPECT_TRUE(interpMainFirstEmittedGroupNeedsNotify(
      pEmittedGroups, groupId, SCAN_PHASE_NEXT));
  EXPECT_FALSE(interpMainFirstEmittedGroupNeedsNotify(
      pEmittedGroups, groupId, SCAN_PHASE_NEXT));
  EXPECT_TRUE(interpMainFirstEmittedGroupNeedsNotify(
      pEmittedGroups, groupId, SCAN_PHASE_PREV));
  EXPECT_FALSE(interpMainFirstEmittedGroupNeedsNotify(
      pEmittedGroups, groupId, SCAN_PHASE_PREV));
  EXPECT_FALSE(interpMainFirstEmittedGroupNeedsNotify(
      pEmittedGroups, groupId, SCAN_PHASE_MAIN));
  EXPECT_FALSE(interpMainFirstEmittedGroupNeedsNotify(
      pEmittedGroups, groupId + 1, SCAN_PHASE_PREV));

  taosHashCleanup(pEmittedGroups);
}

TEST(execUtilTest, interpMultiGroupUsesPhaseScopedEarlyStop) {
  EXPECT_FALSE(interpModeUsesPhasedProtocol(INTERP_MODE_LEGACY));
  EXPECT_TRUE(interpModeUsesPhasedProtocol(INTERP_MODE_MAIN_FIRST));
  EXPECT_TRUE(interpModeUsesPhasedProtocol(INTERP_MODE_MULTI_GROUP));
  EXPECT_TRUE(interpModeUsesPhasedProtocol(INTERP_MODE_GROUP_FIRST));

  EXPECT_TRUE(interpModeCanEarlyStop(INTERP_MODE_LEGACY));
  EXPECT_TRUE(interpModeCanEarlyStop(INTERP_MODE_MAIN_FIRST));
  EXPECT_TRUE(interpModeCanEarlyStop(INTERP_MODE_MULTI_GROUP));
  EXPECT_TRUE(interpModeCanEarlyStop(INTERP_MODE_GROUP_FIRST));
}

TEST(execUtilTest, interpMultiGroupSideBlocksUseClosedMainMembership) {
  EXPECT_EQ(INTERP_SIDE_GROUP_CREATE,
            interpSideGroupAction(INTERP_MODE_MAIN_FIRST, false, false));
  EXPECT_EQ(INTERP_SIDE_GROUP_DROP,
            interpSideGroupAction(INTERP_MODE_MULTI_GROUP, false, false));

  EXPECT_EQ(INTERP_SIDE_GROUP_PROCESS,
            interpSideGroupAction(INTERP_MODE_MAIN_FIRST, true, false));
  EXPECT_EQ(INTERP_SIDE_GROUP_PROCESS,
            interpSideGroupAction(INTERP_MODE_MULTI_GROUP, true, false));

  EXPECT_EQ(INTERP_SIDE_GROUP_NOTIFY,
            interpSideGroupAction(INTERP_MODE_MAIN_FIRST, false, true));
  EXPECT_EQ(INTERP_SIDE_GROUP_DROP,
            interpSideGroupAction(INTERP_MODE_MULTI_GROUP, false, true));
}

TEST(execUtilTest, interpMainFirstSideOnlyGroupWaitsAcrossBlocks) {
  SInterpMainFirstReadiness readiness = {};
  readiness.fillType = TSDB_FILL_NEXT;
  readiness.groupRowSet = true;
  readiness.mainFinalized = true;

  // A side-only NEXT group stays pending until its required reference arrives.
  EXPECT_FALSE(interpMainFirstGroupIsReady(&readiness));
  readiness.extNextRowSet = true;
  EXPECT_TRUE(interpMainFirstGroupIsReady(&readiness));
}

TEST(execUtilTest, interpMainFirstSkippedPhaseReleasesGroup) {
  SInterpMainFirstReadiness readiness = {};
  readiness.fillType = TSDB_FILL_LINEAR;
  readiness.firstMainRowSet = true;
  readiness.groupRowSet = true;
  readiness.mainFinalized = true;
  readiness.leadHoleSet = true;
  readiness.trailHoleSet = true;
  readiness.extPrevRowSet = true;

  EXPECT_FALSE(interpMainFirstGroupIsReady(&readiness));
  readiness.nextPhaseClosed = true;
  EXPECT_TRUE(interpMainFirstGroupIsReady(&readiness));
}

TEST(execUtilTest, interpMainFirstEarlyStopIsScopedPerGroup) {
  SInterpMainFirstReadiness completed = {};
  completed.fillType = TSDB_FILL_NEXT;
  completed.groupRowSet = true;
  completed.mainFinalized = true;
  completed.extNextRowSet = true;

  SInterpMainFirstReadiness pending = completed;
  pending.extNextRowSet = false;
  EXPECT_TRUE(interpMainFirstGroupIsReady(&completed));
  EXPECT_FALSE(interpMainFirstGroupIsReady(&pending));

  completed.queued = true;
  EXPECT_FALSE(interpMainFirstGroupIsReady(&completed));
  EXPECT_FALSE(interpMainFirstGroupIsReady(&pending));

  pending.extNextRowSet = true;
  EXPECT_TRUE(interpMainFirstGroupIsReady(&pending));
}

TEST(execUtilTest, interpMainFirstNaturalEofReleasesPendingGroup) {
  SInterpMainFirstReadiness readiness = {};
  readiness.fillType = TSDB_FILL_LINEAR;
  readiness.groupRowSet = true;
  readiness.mainFinalized = true;

  EXPECT_FALSE(interpMainFirstGroupIsReady(&readiness));
  readiness.nextPhaseClosed = true;
  EXPECT_FALSE(interpMainFirstGroupIsReady(&readiness));
  readiness.prevPhaseClosed = true;
  EXPECT_TRUE(interpMainFirstGroupIsReady(&readiness));

  readiness.queued = true;
  EXPECT_FALSE(interpMainFirstGroupIsReady(&readiness));
}

TEST(execUtilTest, interpMainFirstPhaseTransitionsAreMonotonic) {
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            interpValidatePhaseTransition(SCAN_PHASE_MAIN,
                                          SCAN_PHASE_MAIN));
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            interpValidatePhaseTransition(SCAN_PHASE_MAIN,
                                          SCAN_PHASE_NEXT));
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            interpValidatePhaseTransition(SCAN_PHASE_MAIN,
                                          SCAN_PHASE_PREV));
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            interpValidatePhaseTransition(SCAN_PHASE_NEXT,
                                          SCAN_PHASE_PREV));

  EXPECT_EQ(TSDB_CODE_PLAN_INTERNAL_ERROR,
            interpValidatePhaseTransition(SCAN_PHASE_NEXT,
                                          SCAN_PHASE_MAIN));
  EXPECT_EQ(TSDB_CODE_PLAN_INTERNAL_ERROR,
            interpValidatePhaseTransition(SCAN_PHASE_PREV,
                                          SCAN_PHASE_NEXT));
  EXPECT_EQ(TSDB_CODE_PLAN_INTERNAL_ERROR,
            interpValidatePhaseTransition(-1, SCAN_PHASE_MAIN));
  EXPECT_EQ(TSDB_CODE_PLAN_INTERNAL_ERROR,
            interpValidatePhaseTransition(SCAN_PHASE_MAIN,
                                          SCAN_PHASE_PREV + 1));
}

TEST(execUtilTest, interpMainFirstNotifyMatchesActivePhaseBoundary) {
  STimeWindow mainWin = {};
  mainWin.skey = 10;
  mainWin.ekey = 20;
  STimeWindow nextWin = {};
  nextWin.skey = 21;
  nextWin.ekey = 30;
  STimeWindow prevWin = {};
  prevWin.skey = 0;
  prevWin.ekey = 9;

  // Adjacent phases share boundary timestamp 21. The caller supplies the
  // active phase; this helper validates its window, not notify identity.
  EXPECT_TRUE(interpMainFirstNotifyMatchesPhase(
      SCAN_PHASE_MAIN, &mainWin, 0, 0, 21));
  EXPECT_FALSE(interpMainFirstNotifyMatchesPhase(
      SCAN_PHASE_MAIN, &mainWin, 0, 0, 20));
  EXPECT_TRUE(interpMainFirstNotifyMatchesPhase(
      SCAN_PHASE_NEXT, &nextWin, 0, 0, 21));
  EXPECT_FALSE(interpMainFirstNotifyMatchesPhase(
      SCAN_PHASE_NEXT, &nextWin, 0, 0, 20));
  EXPECT_TRUE(interpMainFirstNotifyMatchesPhase(
      SCAN_PHASE_PREV, &prevWin, 0, 0, 9));
  EXPECT_FALSE(interpMainFirstNotifyMatchesPhase(
      SCAN_PHASE_PREV, &prevWin, 0, 1, 9));
}

TEST(execUtilTest, interpMainFirstNotifyMatchesRemoteSourceIdentity) {
  STimeWindow nextWin = {};
  nextWin.skey = 21;
  nextWin.ekey = 30;
  STableScanOperatorParam notify = {};
  notify.groupid = 42;
  notify.notifyPhase = SCAN_PHASE_NEXT;
  notify.notifyScanGroupId = 77;
  notify.notifyTs = 21;

  EXPECT_TRUE(interpMainFirstNotifyMatchesSource(
      SCAN_PHASE_NEXT, 77, &nextWin, 42, &notify));
  EXPECT_FALSE(interpMainFirstNotifyMatchesSource(
      SCAN_PHASE_MAIN, 77, &nextWin, 42, &notify));
  EXPECT_FALSE(interpMainFirstNotifyMatchesSource(
      SCAN_PHASE_NEXT, 78, &nextWin, 42, &notify));
  EXPECT_FALSE(interpMainFirstNotifyMatchesSource(
      SCAN_PHASE_NEXT, 77, &nextWin, 43, &notify));
}

TEST(execUtilTest, interpMainFirstNotifyAcceptsProjectAndPartition) {
  SOperatorInfo tableScan = {};
  tableScan.operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;

  SOperatorInfo* partitionChildren[] = {&tableScan};
  SOperatorInfo partition = {};
  partition.operatorType = QUERY_NODE_PHYSICAL_PLAN_PARTITION;
  partition.numOfDownstream = 1;
  partition.pDownstream = partitionChildren;

  EXPECT_EQ(interpMainFirstResolveNotifyTarget(&partition), &tableScan);

  SOperatorInfo* projectChildren[] = {&partition};
  SOperatorInfo project = {};
  project.operatorType = QUERY_NODE_PHYSICAL_PLAN_PROJECT;
  project.numOfDownstream = 1;
  project.pDownstream = projectChildren;

  EXPECT_EQ(interpMainFirstResolveNotifyTarget(&project), &tableScan);

  projectChildren[0] = &tableScan;
  EXPECT_EQ(interpMainFirstResolveNotifyTarget(&project), &tableScan);

  SOperatorInfo* sortChildren[] = {&tableScan};
  SOperatorInfo sort = {};
  sort.operatorType = QUERY_NODE_PHYSICAL_PLAN_SORT;
  sort.numOfDownstream = 1;
  sort.pDownstream = sortChildren;
  EXPECT_EQ(interpMainFirstResolveNotifyTarget(&sort), nullptr);

  SOperatorInfo* projectFanout[] = {&tableScan, &tableScan};
  project.numOfDownstream = 2;
  project.pDownstream = projectFanout;
  EXPECT_EQ(interpMainFirstResolveNotifyTarget(&project), nullptr);

  partition.numOfDownstream = 2;
  partition.pDownstream = projectFanout;
  EXPECT_EQ(interpMainFirstResolveNotifyTarget(&partition), nullptr);
}

TEST(execUtilTest, interpMultiGroupNotifyBuildsPartitionScanTree) {
  const int32_t scanTypes[] = {
      QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN,
      QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN,
  };

  for (int32_t scanType : scanTypes) {
    SOperatorInfo scan = {};
    scan.operatorType = scanType;
    SOperatorInfo* partitionChildren[] = {&scan};
    SOperatorInfo partition = {};
    partition.operatorType = QUERY_NODE_PHYSICAL_PLAN_PARTITION;
    partition.numOfDownstream = 1;
    partition.pDownstream = partitionChildren;

    SOperatorParam* pRoot = nullptr;
    ASSERT_EQ(interpMainFirstBuildNotifyParam(
                  &partition, 0, 0, SCAN_PHASE_NEXT, 0, 123, &pRoot),
              TSDB_CODE_SUCCESS);
    ASSERT_NE(pRoot, nullptr);
    EXPECT_EQ(pRoot->opType, QUERY_NODE_PHYSICAL_PLAN_PARTITION);
    EXPECT_TRUE(pRoot->reUse);
    ASSERT_NE(pRoot->pChildren, nullptr);
    ASSERT_EQ(taosArrayGetSize(pRoot->pChildren), 1);

    SOperatorParam* pChild = static_cast<SOperatorParam*>(
        taosArrayGetP(pRoot->pChildren, 0));
    ASSERT_NE(pChild, nullptr);
    EXPECT_EQ(pChild->opType, scanType);
    EXPECT_EQ(pChild->downstreamIdx, 0);
    EXPECT_FALSE(pChild->reUse);
    STableScanOperatorParam* pScan =
        static_cast<STableScanOperatorParam*>(pChild->value);
    ASSERT_NE(pScan, nullptr);
    EXPECT_EQ(pScan->paramType, NOTIFY_TYPE_SCAN_PARAM);
    EXPECT_TRUE(pScan->notifyToProcess);
    EXPECT_EQ(pScan->groupid, 0);
    EXPECT_EQ(pScan->notifyPhase, SCAN_PHASE_NEXT);
    EXPECT_EQ(pScan->notifyScanGroupId, 0);
    EXPECT_EQ(pScan->notifyTs, 123);

    interpPrepareNotifyParamForDispatch(pRoot);
    EXPECT_TRUE(pChild->reUse);
    pRoot->reUse = false;
    pChild->reUse = false;
    freeOperatorParam(pRoot, OP_GET_PARAM);
  }
}

namespace {

struct InterpRoutingParamFixture {
  SOperatorInfo leaf = {};
  SOperatorInfo* downstream[1] = {&leaf};
  SOperatorInfo op = {};
  SOperatorParam* pPending = nullptr;

  explicit InterpRoutingParamFixture(
      int32_t type = QUERY_NODE_PHYSICAL_PLAN_PARTITION) {
    leaf.name = const_cast<char*>("leaf");
    op.name = const_cast<char*>("routing");
    op.operatorType = type;
    op.numOfDownstream = 1;
    op.pDownstream = downstream;
  }

  ~InterpRoutingParamFixture() {
    freeOperatorParam(pPending, OP_GET_PARAM);
    freeResetOperatorParams(&op, OP_GET_PARAM, true);
  }

  void build(int32_t childType = QUERY_NODE_PHYSICAL_PLAN_PROJECT,
             int32_t childCount = 1) {
    pPending = static_cast<SOperatorParam*>(
        taosMemoryCalloc(1, sizeof(SOperatorParam)));
    ASSERT_NE(pPending, nullptr);
    pPending->opType = op.operatorType;
    if (childCount == 0) {
      return;
    }
    pPending->pChildren =
        taosArrayInit(childCount, sizeof(SOperatorParam*));
    ASSERT_NE(pPending->pChildren, nullptr);
    for (int32_t i = 0; i < childCount; ++i) {
      SOperatorParam* pChild = static_cast<SOperatorParam*>(
          taosMemoryCalloc(1, sizeof(SOperatorParam)));
      ASSERT_NE(pChild, nullptr);
      if (childType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
        pChild->value =
            taosMemoryCalloc(1, sizeof(SExchangeOperatorParam));
        if (pChild->value == nullptr) {
          taosMemoryFree(pChild);
          FAIL() << "failed to allocate exchange parameter";
        }
      }
      pChild->opType = childType;
      if (taosArrayPush(pPending->pChildren, &pChild) == nullptr) {
        freeOperatorParam(pChild, OP_GET_PARAM);
        FAIL() << "failed to append routing child";
      }
    }
  }

  SOperatorParam* child(int32_t index = 0) {
    return static_cast<SOperatorParam*>(
        taosArrayGetP(pPending->pChildren, index));
  }

  int32_t dispatch() {
    SOperatorParam* pParam = pPending;
    pPending = nullptr;
    return setOperatorParams(&op, pParam, OP_GET_PARAM);
  }
};

}  // namespace

TEST(execUtilTest, partitionRoutesAndReleasesOperatorParamTree) {
  InterpRoutingParamFixture fixture;
  ASSERT_NO_FATAL_FAILURE(fixture.build());
  SOperatorInfo& partition = fixture.op;
  SOperatorParam* pRoot = fixture.pPending;
  SOperatorParam* pChild = fixture.child();

  ASSERT_EQ(fixture.dispatch(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(partition.pOperatorGetParam, pRoot);
  EXPECT_EQ(pRoot->pChildren, nullptr);
  ASSERT_NE(partition.pDownstreamGetParams, nullptr);
  EXPECT_EQ(partition.pDownstreamGetParams[0], pChild);

  detachOperatorRoutingParam(&partition);
  EXPECT_EQ(partition.pOperatorGetParam, nullptr);
  EXPECT_EQ(partition.pDownstreamGetParams[0], pChild);
  InterpRoutingParamFixture unconsumed;
  ASSERT_NO_FATAL_FAILURE(unconsumed.build());
}

TEST(execUtilTest, partitionRejectsMergeableMultipleChildren) {
  InterpRoutingParamFixture fixture;
  ASSERT_NO_FATAL_FAILURE(
      fixture.build(QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, 2));
  SOperatorInfo& partition = fixture.op;
  SOperatorParam* pRoot = fixture.pPending;
  for (int32_t i = 0; i < 2; ++i) {
    auto* pValue = static_cast<SExchangeOperatorParam*>(
        fixture.child(i)->value);
    ASSERT_NE(pValue, nullptr);
    pValue->basic.paramType = DYN_TYPE_EXCHANGE_PARAM;
    pValue->basic.vgId = i + 1;
  }

  EXPECT_EQ(fixture.dispatch(), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(partition.pOperatorGetParam, pRoot);
  EXPECT_NE(pRoot->pChildren, nullptr);
  ASSERT_NE(partition.pDownstreamGetParams, nullptr);
  EXPECT_EQ(partition.pDownstreamGetParams[0], nullptr);
}

TEST(execUtilTest, partitionHandlesEmptyAndInvalidParamTrees) {
  InterpRoutingParamFixture fixture;
  ASSERT_NO_FATAL_FAILURE(
      fixture.build(QUERY_NODE_PHYSICAL_PLAN_PROJECT, 0));
  SOperatorInfo& partition = fixture.op;
  ASSERT_EQ(fixture.dispatch(), TSDB_CODE_SUCCESS);
  ASSERT_NE(partition.pDownstreamGetParams, nullptr);
  EXPECT_EQ(partition.pDownstreamGetParams[0], nullptr);
  detachOperatorRoutingParam(&partition);
  EXPECT_EQ(partition.pOperatorGetParam, nullptr);
  freeResetOperatorParams(&partition, OP_GET_PARAM, true);

  ASSERT_NO_FATAL_FAILURE(fixture.build());
  SOperatorParam* pRoot = fixture.pPending;
  fixture.child()->downstreamIdx = 1;
  EXPECT_EQ(fixture.dispatch(), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(partition.pOperatorGetParam, pRoot);
  EXPECT_NE(pRoot->pChildren, nullptr);
  ASSERT_NE(partition.pDownstreamGetParams, nullptr);
  EXPECT_EQ(partition.pDownstreamGetParams[0], nullptr);
}

TEST(execUtilTest, routingMergeFailureKeepsParamTreeOwned) {
  InterpRoutingParamFixture fixture(QUERY_NODE_PHYSICAL_PLAN_MERGE);
  ASSERT_NO_FATAL_FAILURE(
      fixture.build(QUERY_NODE_PHYSICAL_PLAN_PROJECT, 2));
  SOperatorInfo& merge = fixture.op;
  SOperatorParam* pRoot = fixture.pPending;

  EXPECT_EQ(fixture.dispatch(), TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(merge.pOperatorGetParam, pRoot);
  EXPECT_NE(pRoot->pChildren, nullptr);
  ASSERT_NE(merge.pDownstreamGetParams, nullptr);
  EXPECT_EQ(merge.pDownstreamGetParams[0], nullptr);
}

TEST(execUtilTest, interpMainFirstNotifyKeepsExistingLeafTargets) {
  SOperatorInfo target = {};
  const int32_t targetTypes[] = {
      QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN,
      QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN,
      QUERY_NODE_PHYSICAL_PLAN_EXCHANGE,
      QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN,
  };

  for (int32_t targetType : targetTypes) {
    target.operatorType = targetType;
    EXPECT_EQ(interpMainFirstResolveNotifyTarget(&target), &target);
  }
  EXPECT_EQ(interpMainFirstResolveNotifyTarget(nullptr), nullptr);
}

TEST(execUtilTest, interpMainFirstNotifyBuildsMergeExchangeTree) {
  SOperatorInfo firstExchange = {};
  firstExchange.operatorType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  SOperatorInfo secondExchange = firstExchange;
  SOperatorInfo* mergeChildren[] = {
      &firstExchange,
      &secondExchange,
  };
  SOperatorInfo merge = {};
  merge.operatorType = QUERY_NODE_PHYSICAL_PLAN_MERGE;
  merge.numOfDownstream = 2;
  merge.pDownstream = mergeChildren;

  EXPECT_EQ(interpMainFirstResolveNotifyTarget(&merge), &merge);
  SOperatorParam* pRoot = nullptr;
  ASSERT_EQ(interpMainFirstBuildNotifyParam(
                &merge, 0, 42, SCAN_PHASE_NEXT, 77, 123, &pRoot),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(pRoot, nullptr);
  EXPECT_EQ(pRoot->opType, QUERY_NODE_PHYSICAL_PLAN_MERGE);
  EXPECT_TRUE(pRoot->reUse);
  ASSERT_NE(pRoot->pChildren, nullptr);
  ASSERT_EQ(taosArrayGetSize(pRoot->pChildren), 2);

  for (int32_t i = 0; i < 2; ++i) {
    SOperatorParam* pChild = static_cast<SOperatorParam*>(
        taosArrayGetP(pRoot->pChildren, i));
    ASSERT_NE(pChild, nullptr);
    EXPECT_EQ(pChild->opType, QUERY_NODE_PHYSICAL_PLAN_EXCHANGE);
    EXPECT_EQ(pChild->downstreamIdx, i);
    SExchangeOperatorParam* pExchange =
        static_cast<SExchangeOperatorParam*>(pChild->value);
    ASSERT_NE(pExchange, nullptr);
    EXPECT_FALSE(pExchange->multiParams);
    EXPECT_EQ(pExchange->basic.paramType,
              NOTIFY_TYPE_EXCHANGE_PARAM);
    EXPECT_EQ(pExchange->basic.srcOpType,
              QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN);
    EXPECT_EQ(pExchange->basic.groupid, 42);
    EXPECT_EQ(pExchange->basic.notifyPhase, SCAN_PHASE_NEXT);
    EXPECT_EQ(pExchange->basic.notifyScanGroupId, 77);
    EXPECT_EQ(pExchange->basic.notifyTs, 123);
    EXPECT_FALSE(pChild->reUse);
  }

  interpPrepareNotifyParamForDispatch(pRoot);
  ASSERT_EQ(setOperatorParams(&merge, pRoot, OP_GET_PARAM),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(merge.pOperatorGetParam, pRoot);
  EXPECT_EQ(pRoot->pChildren, nullptr);
  pRoot->reUse = false;
  detachOperatorRoutingParam(&merge);
  EXPECT_EQ(merge.pOperatorGetParam, nullptr);
  for (int32_t i = 0; i < 2; ++i) {
    SOperatorParam* pChild = merge.pDownstreamGetParams[i];
    ASSERT_NE(pChild, nullptr);
    EXPECT_FALSE(pChild->reUse);
  }
  freeResetOperatorParams(&merge, OP_GET_PARAM, true);
}

TEST(execUtilTest, interpMainFirstRemoteNotifyRequiresMergeScanType) {
  SOperatorParam* pParam = nullptr;
  EXPECT_EQ(buildTableScanOperatorParamNotify(
                &pParam, 0, 42, SCAN_PHASE_NEXT, 77, 123),
            TSDB_CODE_INVALID_PARA);
  if (pParam != nullptr) {
    freeOperatorParam(pParam, OP_GET_PARAM);
    pParam = nullptr;
  }

  ASSERT_EQ(buildTableScanOperatorParamNotify(
                &pParam, QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN,
                42, SCAN_PHASE_NEXT, 77, 123),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(pParam, nullptr);
  EXPECT_EQ(pParam->opType,
            QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN);
  STableScanOperatorParam* pNotify =
      static_cast<STableScanOperatorParam*>(pParam->value);
  ASSERT_NE(pNotify, nullptr);
  EXPECT_EQ(pNotify->groupid, 42);
  EXPECT_EQ(pNotify->notifyPhase, SCAN_PHASE_NEXT);
  EXPECT_EQ(pNotify->notifyScanGroupId, 77);
  EXPECT_EQ(pNotify->notifyTs, 123);
  freeOperatorParam(pParam, OP_GET_PARAM);
}

TEST(execUtilTest, interpMainFirstExchangeNotifyRequiresSingleSource) {
  SExchangeInfo info = {};
  info.pSources = taosArrayInit(2, sizeof(SDownstreamSourceNode));
  ASSERT_NE(info.pSources, nullptr);
  SDownstreamSourceNode source = {};
  ASSERT_NE(taosArrayPush(info.pSources, &source), nullptr);

  SExchangePhysiNode exchangeNode = {};
  exchangeNode.singleSrc = true;
  SExecTaskInfo task = {};
  SExchangeOperatorParam exchangeParam = {};
  exchangeParam.basic.paramType = NOTIFY_TYPE_EXCHANGE_PARAM;
  exchangeParam.basic.srcOpType =
      QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN;
  exchangeParam.basic.groupid = 42;
  exchangeParam.basic.notifyPhase = SCAN_PHASE_NEXT;
  exchangeParam.basic.notifyScanGroupId = 77;
  exchangeParam.basic.notifyTs = 123;
  SOperatorParam getParam = {};
  getParam.opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  getParam.value = &exchangeParam;

  SOperatorInfo op = {};
  op.info = &info;
  op.pTaskInfo = &task;
  op.pPhyNode = reinterpret_cast<SPhysiNode*>(&exchangeNode);
  op.pOperatorGetParam = &getParam;
  ASSERT_EQ(storeExchangeNotifyInfo(&op), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(info.notifyToSend);
  EXPECT_EQ(info.notifySrcOpType,
            QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN);
  EXPECT_EQ(info.notifyGroupId, 42);
  EXPECT_EQ(info.notifyPhase, SCAN_PHASE_NEXT);
  EXPECT_EQ(info.notifyScanGroupId, 77);
  EXPECT_EQ(info.notifyTs, 123);

  ASSERT_NE(taosArrayPush(info.pSources, &source), nullptr);
  info.notifyToSend = false;
  EXPECT_EQ(storeExchangeNotifyInfo(&op), TSDB_CODE_INVALID_PARA);
  EXPECT_FALSE(info.notifyToSend);
  taosArrayDestroy(info.pSources);
}

TEST(execUtilTest, interpLegacyExchangeNotifyAllowsMultipleTableScans) {
  SExchangeInfo info = {};
  info.pSources = taosArrayInit(2, sizeof(SDownstreamSourceNode));
  ASSERT_NE(info.pSources, nullptr);
  SDownstreamSourceNode source = {};
  ASSERT_NE(taosArrayPush(info.pSources, &source), nullptr);
  ASSERT_NE(taosArrayPush(info.pSources, &source), nullptr);

  SExchangePhysiNode exchangeNode = {};
  SExecTaskInfo task = {};
  SExchangeOperatorParam exchangeParam = {};
  exchangeParam.basic.paramType = NOTIFY_TYPE_EXCHANGE_PARAM;
  exchangeParam.basic.srcOpType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
  exchangeParam.basic.notifyPhase = SCAN_PHASE_MAIN;
  exchangeParam.basic.notifyTs = 123;
  SOperatorParam getParam = {};
  getParam.opType = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  getParam.value = &exchangeParam;

  SOperatorInfo op = {};
  op.info = &info;
  op.pTaskInfo = &task;
  op.pPhyNode = reinterpret_cast<SPhysiNode*>(&exchangeNode);
  op.pOperatorGetParam = &getParam;

  ASSERT_EQ(storeExchangeNotifyInfo(&op), TSDB_CODE_SUCCESS);
  EXPECT_TRUE(info.notifyToSend);
  EXPECT_EQ(info.notifySrcOpType,
            QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
  EXPECT_EQ(info.notifyTs, 123);
  taosArrayDestroy(info.pSources);
}

TEST(execUtilTest, interpRemoteSendPropagatesDeserializeFailure) {
  SMsgSendInfo* pSendInfo = static_cast<SMsgSendInfo*>(
      taosMemoryCalloc(1, sizeof(SMsgSendInfo)));
  ASSERT_NE(pSendInfo, nullptr);

  pSendInfo->msgInfo.len = sizeof(SMsgHead);
  pSendInfo->msgInfo.pData =
      taosMemoryCalloc(1, pSendInfo->msgInfo.len);
  if (pSendInfo->msgInfo.pData == nullptr) {
    destroySendMsgInfo(pSendInfo);
    FAIL() << "failed to allocate malformed fetch request";
  }

  EXPECT_LT(interpRemoteSend(nullptr, nullptr, nullptr, pSendInfo), 0);
}

namespace {

class InterpRemoteParamOwnershipTest : public testing::Test {
 protected:
  static InterpRemoteParamOwnershipTest* active;
  Stub stub;
  InterpRemoteScanFixture remote;
  InterpRemoteTransport transport;
  SMsgSendInfo* pSendInfo = nullptr;
  std::array<void*, 2> allocations = {};
  std::array<int, 2> freeCounts = {};
  bool failResults = false;

  static SArray* initArray(size_t size, size_t elemSize) {
    SOperatorParam* pParam = active->remote.task.pOpParam;
    // The first allocation after ownership reaches the task is pResults.
    if (pParam != nullptr && active->allocations[0] == nullptr) {
      active->allocations = {pParam, pParam->value};
      if (active->failResults) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return nullptr;
      }
    }
    active->stub.reset(taosArrayInit);
    SArray* pArray = taosArrayInit(size, elemSize);
    active->stub.set(taosArrayInit, initArray);
    return pArray;
  }

  static void recordFree(void* ptr) {
    for (size_t i = 0; i < active->allocations.size(); ++i) {
      if (ptr != nullptr && ptr == active->allocations[i]) {
        ++active->freeCounts[i];
      }
    }
    active->stub.reset(taosMemFree);
    taosMemFree(ptr);
    active->stub.set(taosMemFree, recordFree);
  }

  void SetUp() override {
    remote.remoteTaskId = 0x101;
    remote.task.id.str = remote.taskId;
    remote.task.pRoot = &remote.root;
    remote.root.operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN;
    transport.scans.push_back(&remote);

    STableScanOperatorParam notify = {};
    notify.paramType = NOTIFY_TYPE_SCAN_PARAM;
    notify.notifyToProcess = true;
    SOperatorParam param = {};
    param.opType = remote.root.operatorType;
    param.value = &notify;
    SResFetchReq req = {};
    req.taskId = remote.remoteTaskId;
    req.pOpParam = &param;

    int32_t len = tSerializeSResFetchReq(nullptr, 0, &req, false, false);
    ASSERT_GT(len, 0);
    pSendInfo = static_cast<SMsgSendInfo*>(
        taosMemoryCalloc(1, sizeof(SMsgSendInfo)));
    ASSERT_NE(pSendInfo, nullptr);
    pSendInfo->msgInfo.len = len;
    pSendInfo->msgInfo.pData = taosMemoryCalloc(1, len);
    ASSERT_NE(pSendInfo->msgInfo.pData, nullptr);
    ASSERT_EQ(tSerializeSResFetchReq(pSendInfo->msgInfo.pData, len, &req,
                                     false, false),
              len);

    active = this;
    stub.set(taosArrayInit, initArray);
    stub.set(taosMemFree, recordFree);
  }

  void sendAndCheck(int32_t expectedCode) {
    SMsgSendInfo* pRequest = pSendInfo;
    pSendInfo = nullptr;
    EXPECT_EQ(interpRemoteSend(&transport, nullptr, nullptr, pRequest),
              TSDB_CODE_SUCCESS);
    EXPECT_EQ(remote.execCode, expectedCode);
    EXPECT_EQ(remote.notifyCount, 1);
    EXPECT_FALSE(remote.task.paramSet);
    EXPECT_EQ(remote.task.pOpParam, nullptr);
    for (size_t i = 0; i < allocations.size(); ++i) {
      EXPECT_NE(allocations[i], nullptr);
      EXPECT_EQ(freeCounts[i], 1);
    }
  }

  void TearDown() override {
    stub.reset(taosArrayInit);
    stub.reset(taosMemFree);
    active = nullptr;
    destroySendMsgInfo(pSendInfo);
    // Keep failing ownership assertions from leaking the decoded payload.
    for (size_t i = 0; i < allocations.size(); ++i) {
      if (freeCounts[i] == 0) {
        taosMemoryFree(allocations[i]);
      }
    }
  }
};

InterpRemoteParamOwnershipTest* InterpRemoteParamOwnershipTest::active =
    nullptr;

}  // namespace

TEST_F(InterpRemoteParamOwnershipTest, FreesParamOnResultArrayAllocationOom) {
  failResults = true;
  sendAndCheck(TSDB_CODE_OUT_OF_MEMORY);
}

TEST_F(InterpRemoteParamOwnershipTest, FreesParamWhenKilledBeforeDispatch) {
  remote.task.code = TSDB_CODE_TSC_QUERY_KILLED;
  sendAndCheck(TSDB_CODE_TSC_QUERY_KILLED);
}

TEST(execUtilTest, interpOrdinaryScanSkipsEmptyPhasesWithoutWalkingGroups) {
  InterpOrdinaryScanFixture fixture;

  EXPECT_EQ(fixture.next(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(fixture.pResult, nullptr);
  EXPECT_EQ(fixture.root.status, OP_EXEC_DONE);
  EXPECT_EQ(fixture.scanInfo.currentGroupId, 0);
  EXPECT_EQ(fixture.scanInfo.currentPhase, SCAN_PHASE_PREV);
  EXPECT_EQ(fixture.groups.calls, 1);
}

TEST(execUtilTest, interpOrdinaryEarlyStopRemainsGroupScoped) {
  InterpOrdinaryScanFixture fixture;
  fixture.scanInfo.currentGroupId = 2;
  fixture.scanInfo.groupDone = true;
  fixture.scanInfo.base.cond.twindows = {0, 1};

  EXPECT_EQ(fixture.next(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(fixture.pResult, nullptr);
  EXPECT_EQ(fixture.root.status, OP_EXEC_DONE);
  EXPECT_EQ(fixture.scanInfo.currentGroupId, 0);
  EXPECT_EQ(fixture.scanInfo.currentPhase, SCAN_PHASE_PREV);
  EXPECT_EQ(fixture.groups.calls, 2);
}

TEST(execUtilTest, interpGroupFirstScanFinishesEveryEmptyGroup) {
  InterpOrdinaryScanFixture fixture;
  fixture.scanNode.interpMode = INTERP_MODE_GROUP_FIRST;
  EXPECT_EQ(fixture.next(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(fixture.pResult, nullptr);
  EXPECT_EQ(fixture.root.status, OP_EXEC_DONE);
  EXPECT_EQ(fixture.scanInfo.currentGroupId, 3);
  EXPECT_EQ(fixture.scanInfo.currentPhase, SCAN_PHASE_MAIN);
}

TEST(execUtilTest, interpMixedMergeScanSkipsAllEmptyGroups) {
  SExecTaskInfo task = {};
  char taskId[] = "mixed-empty-groups";
  task.id.str = taskId;

  int32_t openCount = 0;
  task.storageAPI.tsdReader.tsdReaderOpen = interpUnexpectedReaderOpen;

  STableMergeScanInfo scanInfo = {};
  scanInfo.base.readHandle.vnode = &openCount;
  scanInfo.base.cond.mainFirst = true;
  scanInfo.base.cond.order = TSDB_ORDER_ASC;
  scanInfo.currentPhase = SCAN_PHASE_MAIN;
  scanInfo.scanInfo.numOfAsc = 1;
  scanInfo.scanInfo.numOfDesc = 0;
  STimeWindow emptyWindow = {INT64_MAX, INT64_MIN};
  for (int32_t phase = SCAN_PHASE_MAIN;
       phase <= SCAN_PHASE_PREV; ++phase) {
    scanInfo.phaseWins[phase] = emptyWindow;
  }
  scanInfo.base.cond.twindows = emptyWindow;
  scanInfo.base.pTableListInfo = tableListCreate();
  ASSERT_NE(scanInfo.base.pTableListInfo, nullptr);
  ASSERT_EQ(tableListAddTableInfo(scanInfo.base.pTableListInfo, 1, 10),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(tableListAddTableInfo(scanInfo.base.pTableListInfo, 2, 10),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(tableListAddTableInfo(scanInfo.base.pTableListInfo, 3, 20),
            TSDB_CODE_SUCCESS);

  STableScanPhysiNode scanNode = {};
  scanNode.interpMode = INTERP_MODE_MULTI_GROUP;
  scanNode.groupSort = true;

  SOperatorInfo root = {};
  setOperatorInfo(&root, "TableMergeScan",
                  QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN, false,
                  OP_OPENED, &scanInfo, &task);
  root.pPhyNode = reinterpret_cast<SPhysiNode*>(&scanNode);
  root.fpSet._openFn = optrDummyOpenFn;
  root.resultInfo.capacity = 1;

  SSDataBlock* pResult = nullptr;
  int32_t jmpCode = setjmp(task.env);
  if (jmpCode == TSDB_CODE_SUCCESS) {
    EXPECT_EQ(doTableMergeScanNext(&root, &pResult),
              TSDB_CODE_SUCCESS);
  } else {
    ADD_FAILURE() << "empty groups opened storage, code " << jmpCode;
  }

  EXPECT_EQ(openCount, 0);
  EXPECT_EQ(pResult, nullptr);
  EXPECT_EQ(root.status, OP_EXEC_DONE);

  tableListDestroy(scanInfo.base.pTableListInfo);
  taosArrayDestroy(scanInfo.pSortInfo);
}

// The notify identity a remote merge scan must match. Mixed grouping carries
// a nonzero outer epoch with scanGroupId 0; the other phased modes carry the
// scan group id with epoch 0. Both halves of that pair need coverage.
struct InterpRemoteNotifyScenario {
  int8_t   interpMode;
  bool     groupSort;
  uint64_t outerEpoch;
  uint64_t scanGroupId;
};

// Component path under test:
//   interpMainFirstBuildNotifyParam
//     -> real Exchange stores the notification
//     -> real Exchange attaches it to SResFetchReq
//     -> real request serialization
//     -> mocked asynchronous transport
//     -> real request deserialization
//     -> real qUpdateOperatorParam
//     -> real qExecTaskOpt(processOneBlock=true)
//     -> real TableMergeScan consumes the notification and stops the group
//     -> mocked storage fetch remains uncalled
//
// The scenario directly triggers notification construction, so it does not
// cover the TimeSlice readiness decision. It also replaces the RPC/qworker
// transport shell and physical storage read, while retaining their executor
// boundaries.
static void interpRunRemoteNotifyScenario(
    const InterpRemoteNotifyScenario& scenario) {
  constexpr int32_t kRemoteCount = 3;
  constexpr TSKEY kNotifyTs = 123;
  constexpr int8_t kNotifyPhase = SCAN_PHASE_PREV;
  // The scan only stores its own group id. consumeTableMergeScanNotify reads
  // it back as the outer epoch under mixed grouping, as the scan group id
  // otherwise, so one field carries whichever identity the mode uses.
  const uint64_t kGroupId =
      scenario.groupSort ? scenario.outerEpoch : scenario.scanGroupId;

  ASSERT_EQ(qExecutorInit(), TSDB_CODE_SUCCESS);

  std::array<InterpRemoteScanFixture, kRemoteCount> remotes;
  InterpRemoteTransport transport;
  for (int32_t i = 0; i < kRemoteCount; ++i) {
    InterpRemoteScanFixture& remote = remotes[i];
    remote.remoteTaskId = 0x101 + i;
    remote.expectedOuterEpoch = scenario.outerEpoch;
    remote.expectedScanGroupId = scenario.scanGroupId;
    remote.expectedNotifyTs = kNotifyTs;
    remote.expectedPhase = kNotifyPhase;
    snprintf(remote.taskId, sizeof(remote.taskId),
             "remote-vgroup-%d", i);
    remote.task.id.str = remote.taskId;
    remote.task.id.taskId = remote.remoteTaskId;
    remote.task.pSubplan = &remote.subplan;
    remote.task.pResultBlockList = taosArrayInit(1, POINTER_BYTES);
    ASSERT_NE(remote.task.pResultBlockList, nullptr);
    remote.scanInfo.base.cond.mainFirst = true;
    remote.scanInfo.currentPhase = kNotifyPhase;
    remote.scanInfo.groupId = kGroupId;
    remote.scanInfo.hasGroupId = true;
    remote.scanInfo.tableStartIndex = 0;
    remote.scanInfo.tableEndIndex = 0;
    remote.scanInfo.phaseWins[kNotifyPhase] = {100, kNotifyTs};
    remote.scanInfo.base.cond.twindows =
        remote.scanInfo.phaseWins[kNotifyPhase];
    remote.scanInfo.scanInfo.numOfAsc = 1;
    remote.scanInfo.scanInfo.numOfDesc = 0;
    remote.scanInfo.base.pTableListInfo = tableListCreate();
    ASSERT_NE(remote.scanInfo.base.pTableListInfo, nullptr);
    ASSERT_EQ(tableListAddTableInfo(remote.scanInfo.base.pTableListInfo,
                                    i + 1, kGroupId),
              TSDB_CODE_SUCCESS);
    remote.scanNode.interpMode = scenario.interpMode;
    remote.scanNode.groupSort = scenario.groupSort;
    setOperatorInfo(&remote.root, "TableMergeScan",
                    QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN, false,
                    OP_OPENED, &remote.scanInfo, &remote.task);
    remote.root.pPhyNode =
        reinterpret_cast<SPhysiNode*>(&remote.scanNode);
    // The scan is already positioned in PREV for this scenario's identity.
    // Skip physical reader setup but retain the real scan and transition
    // logic.
    remote.root.fpSet._openFn = optrDummyOpenFn;
    remote.root.fpSet.getNextFn = doTableMergeScanNext;
    remote.root.fpSet.getNextExtFn = optrDefaultGetNextExtFn;
    remote.root.pDownstreamGetParams = &remote.downstreamParam;
    remote.root.resultInfo.capacity = 1;
    remote.task.pRoot = &remote.root;
    ASSERT_EQ(createInterpMergeTestBlock(
                  i, kNotifyPhase, &remote.pScanBlock, kGroupId),
              TSDB_CODE_SUCCESS);
    ASSERT_EQ(createInterpMergeTestBlock(
                  0, kNotifyPhase, &remote.scanInfo.pResBlock, kGroupId),
              TSDB_CODE_SUCCESS);
    SBlockOrderInfo orderInfo = {};
    orderInfo.order = TSDB_ORDER_DESC;
    orderInfo.slotId = 0;
    orderInfo.compFn = reinterpret_cast<void*>(
        getKeyComparFunc(TSDB_DATA_TYPE_TIMESTAMP, TSDB_ORDER_DESC));
    remote.pSortInfo = taosArrayInit(1, sizeof(SBlockOrderInfo));
    ASSERT_NE(remote.pSortInfo, nullptr);
    ASSERT_NE(taosArrayPush(remote.pSortInfo, &orderInfo), nullptr);
    ASSERT_EQ(tsortCreateSortHandle(
                  remote.pSortInfo, SORT_BLOCK_TS_MERGE, 1024, 4,
                  remote.pScanBlock, remote.taskId, 0, 0, 0,
                  &remote.scanInfo.pSortHandle),
              TSDB_CODE_SUCCESS);
    // Inject the only storage fake beneath the production sort handle.
    tsortSetFetchRawDataFp(remote.scanInfo.pSortHandle,
                           interpRemoteMergeScanFetch, nullptr, nullptr);
    SSortSource* pSource = static_cast<SSortSource*>(
        taosMemoryCalloc(1, sizeof(SSortSource)));
    ASSERT_NE(pSource, nullptr);
    pSource->param = &remote;
    pSource->onlyRef = true;
    ASSERT_EQ(tsortAddSource(remote.scanInfo.pSortHandle, pSource),
              TSDB_CODE_SUCCESS);
    tsortSetSingleTableMerge(remote.scanInfo.pSortHandle);
    transport.scans.push_back(&remote);
  }

  SExecTaskInfo exchangeTask = {};
  char exchangeTaskId[] = "coordinator-exchange";
  exchangeTask.id.str = exchangeTaskId;
  exchangeTask.id.queryId = 0xabc;
  exchangeTask.stopInfo.pStopInfo =
      taosArrayInit(kRemoteCount, sizeof(SExchangeOpStopInfo));
  ASSERT_NE(exchangeTask.stopInfo.pStopInfo, nullptr);

  std::array<SOperatorInfo*, kRemoteCount> exchanges = {};
  std::array<SExchangePhysiNode*, kRemoteCount> exchangeNodes = {};
  {
    Stub stub;
    // Intercept only the network entry point. Exchange request construction,
    // notification attachment, and serialization remain production code.
    stub.set(asyncSendMsgToServer, interpRemoteSend);
    for (int32_t i = 0; i < kRemoteCount; ++i) {
      SExchangePhysiNode* pExchangeNode = nullptr;
      ASSERT_EQ(nodesMakeNode(
                    QUERY_NODE_PHYSICAL_PLAN_EXCHANGE,
                    reinterpret_cast<SNode**>(&pExchangeNode)),
                TSDB_CODE_SUCCESS);
      exchangeNodes[i] = pExchangeNode;
      pExchangeNode->singleSrc = true;
      pExchangeNode->node.pOutputDataBlockDesc =
          createInterpMergeTestDesc(i + 10);
      ASSERT_NE(pExchangeNode->node.pOutputDataBlockDesc, nullptr);

      SDownstreamSourceNode* pSource = nullptr;
      ASSERT_EQ(nodesMakeNode(
                    QUERY_NODE_DOWNSTREAM_SOURCE,
                    reinterpret_cast<SNode**>(&pSource)),
                TSDB_CODE_SUCCESS);
      pSource->addr.nodeId = i + 1;
      pSource->taskId = remotes[i].remoteTaskId;
      pSource->fetchMsgType = TDMT_SCH_FETCH;
      ASSERT_EQ(nodesListMakeStrictAppend(
                    &pExchangeNode->pSrcEndPoints,
                    reinterpret_cast<SNode*>(pSource)),
                TSDB_CODE_SUCCESS);

      ASSERT_EQ(createExchangeOperatorInfo(
                    &transport, pExchangeNode, &exchangeTask,
                    &exchanges[i]),
                TSDB_CODE_SUCCESS);
      ASSERT_NE(exchanges[i], nullptr);

      SOperatorParam* pNotify = nullptr;
      ASSERT_EQ(interpMainFirstBuildNotifyParam(
                    exchanges[i], 0, scenario.outerEpoch, kNotifyPhase,
                    scenario.scanGroupId, kNotifyTs, &pNotify),
                TSDB_CODE_SUCCESS);
      ASSERT_NE(pNotify, nullptr);
      exchanges[i]->pOperatorGetParam = pNotify;
      ASSERT_EQ(exchanges[i]->fpSet._openFn(exchanges[i]),
                TSDB_CODE_SUCCESS);
      EXPECT_FALSE(static_cast<SExchangeInfo*>(
                       exchanges[i]->info)->notifyToSend);
      freeOperatorParam(pNotify, OP_GET_PARAM);
    }
  }

  for (const InterpRemoteScanFixture& remote : remotes) {
    EXPECT_EQ(remote.execCode, TSDB_CODE_SUCCESS);
    EXPECT_EQ(remote.notifyCount, 1);
    EXPECT_TRUE(remote.task.paramSet);
    EXPECT_EQ(remote.task.pOpParam, nullptr);
    EXPECT_EQ(remote.root.pOperatorGetParam, nullptr);
    EXPECT_EQ(remote.readCount, 0);
    EXPECT_EQ(remote.root.status, OP_EXEC_DONE);
    EXPECT_EQ(remote.scanInfo.pSortHandle, nullptr);
  }

  for (int32_t i = 0; i < kRemoteCount; ++i) {
    destroyOperator(exchanges[i]);
    nodesDestroyNode(reinterpret_cast<SNode*>(exchangeNodes[i]));
    InterpRemoteScanFixture& remote = remotes[i];
    for (int32_t j = 0;
         j < taosArrayGetSize(remote.task.pResultBlockList); ++j) {
      SSDataBlock** ppBlock = static_cast<SSDataBlock**>(
          taosArrayGet(remote.task.pResultBlockList, j));
      blockDataDestroy(*ppBlock);
    }
    taosArrayDestroy(remote.task.pResultBlockList);
    tsortDestroySortHandle(remote.scanInfo.pSortHandle);
    tableListDestroy(remote.scanInfo.base.pTableListInfo);
    blockDataDestroy(remote.scanInfo.pResBlock);
    taosArrayDestroy(remote.pSortInfo);
    blockDataDestroy(remote.pScanBlock);
  }
  taosArrayDestroy(exchangeTask.stopInfo.pStopInfo);
}

TEST(execUtilTest, interpMainFirstNotifyReachesThreeRemoteVgroups) {
  // Group-within-phase identity: the scan group id carries the notification
  // and the outer epoch stays 0.
  interpRunRemoteNotifyScenario(
      {INTERP_MODE_MAIN_FIRST, false, 0, 77});
}

TEST(execUtilTest, interpMainFirstNotifyReachesMixedRemoteVgroups) {
  // Phase-within-outer-group identity: the outer epoch carries the
  // notification and the scan group id stays 0.
  interpRunRemoteNotifyScenario(
      {INTERP_MODE_MULTI_GROUP, true, 42, 0});
}

namespace {

struct InterpMergeInput {
  int64_t ts;
  uint64_t groupId;
  int8_t phase;
};

struct InterpMergeFixture {
  InterpMergeTempDirGuard tempDir;
  SExecTaskInfo task = {};
  std::vector<InterpMergeFakeSource> sources;
  std::vector<SOperatorInfo*> pendingDownstreams;
  SMergePhysiNode* pMergeNode = nullptr;
  SOperatorInfo* pMergeOp = nullptr;
  std::vector<std::pair<uint64_t, int8_t>> cursors;
  std::vector<std::vector<int64_t>> values;

  ~InterpMergeFixture() {
    destroyOperator(pMergeOp);
    for (SOperatorInfo* pSourceOp : pendingDownstreams) {
      destroyOperator(pSourceOp);
    }
    for (const auto& source : sources) {
      for (SSDataBlock* pBlock : source.blocks) {
        blockDataDestroy(pBlock);
      }
    }
    nodesDestroyNode(reinterpret_cast<SNode*>(pMergeNode));
  }

  void init(const std::vector<std::vector<InterpMergeInput>>& inputs,
            bool groupSort = false, bool outerGroupPhaseMode = false) {
    ASSERT_EQ(tempDir.code, TSDB_CODE_SUCCESS);
    sources.resize(inputs.size());
    pendingDownstreams.resize(inputs.size(), nullptr);
    for (size_t i = 0; i < inputs.size(); ++i) {
      for (const auto& input : inputs[i]) {
        SSDataBlock* pBlock = nullptr;
        ASSERT_EQ(createInterpMergeTestBlock(
                      input.ts, input.phase, &pBlock, input.groupId),
                  TSDB_CODE_SUCCESS);
        sources[i].blocks.push_back(pBlock);
      }
      auto*& pSourceOp = pendingDownstreams[i];
      pSourceOp = static_cast<SOperatorInfo*>(
          taosMemoryCalloc(1, sizeof(SOperatorInfo)));
      ASSERT_NE(pSourceOp, nullptr);
      setOperatorInfo(pSourceOp, "InterpMergeFake",
                      QUERY_NODE_PHYSICAL_PLAN_PROJECT, false,
                      OP_RES_TO_RETURN, &sources[i], &task);
      pSourceOp->fpSet.getNextFn = interpMergeFakeNext;
    }

    pMergeNode = createInterpMergeTestPhysNode(groupSort);
    ASSERT_NE(pMergeNode, nullptr);
    if (outerGroupPhaseMode) {
      pMergeNode->outerGroupPhaseMode = true;
    }
    int32_t code = createMultiwayMergeOperatorInfo(
        pendingDownstreams.data(), pendingDownstreams.size(),
        pMergeNode, &task, &pMergeOp);
    // The constructor consumes downstreams on both success and failure.
    pendingDownstreams.clear();
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    ASSERT_NE(pMergeOp, nullptr);
  }

  void collect() {
    int32_t jmpCode = setjmp(task.env);
    ASSERT_EQ(jmpCode, TSDB_CODE_SUCCESS)
        << "merge aborted with code " << jmpCode;
    while (true) {
      SSDataBlock* pResult = nullptr;
      ASSERT_EQ(pMergeOp->fpSet.getNextFn(pMergeOp, &pResult),
                TSDB_CODE_SUCCESS);
      if (pResult == nullptr) {
        break;
      }
      cursors.emplace_back(pResult->info.id.groupId,
                           pResult->info.scanPhase);
      values.emplace_back();
      SColumnInfoData* pTsCol = static_cast<SColumnInfoData*>(
          taosArrayGet(pResult->pDataBlock, 0));
      ASSERT_NE(pTsCol, nullptr);
      for (int32_t i = 0; i < pResult->info.rows; ++i) {
        values.back().push_back(
            *reinterpret_cast<int64_t*>(colDataGetData(pTsCol, i)));
      }
    }
  }

  void expectSourcesConsumed() const {
    for (const auto& source : sources) {
      EXPECT_EQ(source.index, source.blocks.size());
    }
  }
};

}  // namespace

TEST(execUtilTest, interpMainFirstMergeKeepsOutputPhasePure) {
  InterpMergeFixture fixture;
  ASSERT_NO_FATAL_FAILURE(fixture.init({
      {{10, 1, SCAN_PHASE_MAIN}, {20, 2, SCAN_PHASE_MAIN},
       {30, 1, SCAN_PHASE_NEXT}, {40, 2, SCAN_PHASE_NEXT},
       {9, 1, SCAN_PHASE_PREV}, {19, 2, SCAN_PHASE_PREV}},
      {{11, 1, SCAN_PHASE_MAIN}, {21, 2, SCAN_PHASE_MAIN},
       {8, 1, SCAN_PHASE_PREV}, {18, 2, SCAN_PHASE_PREV}},
  }, true));
  ASSERT_NO_FATAL_FAILURE(fixture.collect());

  const std::vector<std::pair<uint64_t, int8_t>> expected = {
      {1, SCAN_PHASE_MAIN}, {2, SCAN_PHASE_MAIN},
      {1, SCAN_PHASE_NEXT}, {2, SCAN_PHASE_NEXT},
      {1, SCAN_PHASE_PREV}, {2, SCAN_PHASE_PREV}};
  ASSERT_EQ(fixture.cursors.size(), 6);
  EXPECT_EQ(fixture.cursors, expected);
  EXPECT_EQ(fixture.values, (std::vector<std::vector<int64_t>>{
      {10, 11}, {20, 21}, {30}, {40}, {9, 8}, {19, 18}}));
  fixture.expectSourcesConsumed();

  SSDataBlock* pFinal = nullptr;
  EXPECT_EQ(fixture.pMergeOp->fpSet.getNextFn(fixture.pMergeOp, &pFinal),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pFinal, nullptr);
}

TEST(execUtilTest, interpMergeGroupIdsDoNotEnableOuterCursor) {
  InterpMergeFixture fixture;
  ASSERT_NO_FATAL_FAILURE(fixture.init({
      {{10, 10, SCAN_PHASE_MAIN}, {20, 20, SCAN_PHASE_MAIN},
       {30, 10, SCAN_PHASE_NEXT}, {9, 10, SCAN_PHASE_PREV}},
  }));
  EXPECT_FALSE(fixture.pMergeNode->outerGroupPhaseMode);
  ASSERT_NO_FATAL_FAILURE(fixture.collect());

  const std::vector<std::pair<uint64_t, int8_t>> expected = {
      {10, SCAN_PHASE_MAIN}, {20, SCAN_PHASE_MAIN},
      {10, SCAN_PHASE_NEXT}, {10, SCAN_PHASE_PREV}};
  EXPECT_EQ(fixture.cursors, expected);
}

TEST(execUtilTest, interpMixedMergeBarriersOuterGroups) {
  InterpMergeFixture fixture;
  ASSERT_NO_FATAL_FAILURE(fixture.init({
      {{10, 10, SCAN_PHASE_MAIN}, {30, 10, SCAN_PHASE_NEXT},
       {9, 10, SCAN_PHASE_PREV}, {310, 30, SCAN_PHASE_MAIN},
       {309, 30, SCAN_PHASE_PREV}},
      {{11, 10, SCAN_PHASE_MAIN}, {8, 10, SCAN_PHASE_PREV},
       {210, 20, SCAN_PHASE_MAIN}, {230, 20, SCAN_PHASE_NEXT},
       {209, 20, SCAN_PHASE_PREV}, {311, 30, SCAN_PHASE_MAIN},
       {308, 30, SCAN_PHASE_PREV}},
      {{211, 20, SCAN_PHASE_MAIN}, {231, 20, SCAN_PHASE_NEXT},
       {208, 20, SCAN_PHASE_PREV}},
  }, false, true));
  ASSERT_NO_FATAL_FAILURE(fixture.collect());

  const std::vector<std::pair<uint64_t, int8_t>> expected = {
      {10, SCAN_PHASE_MAIN}, {10, SCAN_PHASE_NEXT},
      {10, SCAN_PHASE_PREV}, {20, SCAN_PHASE_MAIN},
      {20, SCAN_PHASE_NEXT}, {20, SCAN_PHASE_PREV},
      {30, SCAN_PHASE_MAIN}, {30, SCAN_PHASE_PREV}};
  EXPECT_EQ(fixture.cursors, expected);
  ASSERT_EQ(fixture.values.size(), expected.size());
  EXPECT_EQ(fixture.values, (std::vector<std::vector<int64_t>>{
      {10, 11}, {30}, {9, 8}, {210, 211}, {230, 231}, {209, 208},
      {310, 311}, {309, 308}}));
  fixture.expectSourcesConsumed();
}

TEST(execUtilTest, sortMergeOpenFailureDestroysPartialHandle) {
  InterpMergeFixture fixture;
  ASSERT_NO_FATAL_FAILURE(fixture.init({{}}));

  gSortMergeDestroyCalls = 0;
  {
    SortMergeCleanupStubGuard cleanupStub;
    EXPECT_EQ(openSortMergeOperator(fixture.pMergeOp),
              TSDB_CODE_OUT_OF_MEMORY);
    EXPECT_EQ(gSortMergeDestroyCalls, 1);
  }
}

TEST(execUtilTest, interpMainFirstTimelineDirectionIsExplicit) {
  EXPECT_TRUE(interpTimestampFollows(TSDB_ORDER_ASC, 10, 11));
  EXPECT_FALSE(interpTimestampFollows(TSDB_ORDER_ASC, 10, 10));
  EXPECT_FALSE(interpTimestampFollows(TSDB_ORDER_ASC, 10, 9));

  EXPECT_TRUE(interpTimestampFollows(TSDB_ORDER_DESC, 10, 9));
  EXPECT_FALSE(interpTimestampFollows(TSDB_ORDER_DESC, 10, 10));
  EXPECT_FALSE(interpTimestampFollows(TSDB_ORDER_DESC, 10, 11));
  EXPECT_FALSE(interpTimestampFollows(0, 10, 11));
}

TEST(execUtilTest, interpLegacyMembershipWaitsForMainTimestamp) {
  STimeWindow win = {100, 200};
  bool mainSeen = false;

  EXPECT_FALSE(interpTimestampInMainRange(&win, 99));
  EXPECT_FALSE(interpLegacyGroupCanEmit(true, mainSeen));

  mainSeen = interpTimestampInMainRange(&win, 150);
  EXPECT_TRUE(mainSeen);
  EXPECT_TRUE(interpLegacyGroupCanEmit(true, mainSeen));
}

TEST(execUtilTest, interpLegacyMembershipDropsSideOnlyGroup) {
  STimeWindow win = {100, 200};
  bool mainSeen = interpTimestampInMainRange(&win, 99) ||
                  interpTimestampInMainRange(&win, 201);

  EXPECT_FALSE(mainSeen);
  EXPECT_FALSE(interpLegacyGroupCanEmit(true, mainSeen));
  EXPECT_TRUE(interpLegacyGroupCanEmit(false, mainSeen));
}

TEST(execUtilTest, interpLegacyMembershipGatesResultBlock) {
  SSDataBlock* pBlock = nullptr;
  ASSERT_EQ(createInterpStoreTestBlock(1, 10, &pBlock),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(pBlock->info.rows, 2);

  EXPECT_FALSE(interpLegacyApplyGroupMembership(true, false, pBlock));
  EXPECT_EQ(pBlock->info.rows, 0);
  blockDataDestroy(pBlock);

  ASSERT_EQ(createInterpStoreTestBlock(2, 20, &pBlock),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(interpLegacyApplyGroupMembership(true, true, pBlock));
  EXPECT_EQ(pBlock->info.rows, 2);
  blockDataDestroy(pBlock);

  ASSERT_EQ(createInterpStoreTestBlock(3, 30, &pBlock),
            TSDB_CODE_SUCCESS);
  EXPECT_TRUE(interpLegacyApplyGroupMembership(false, false, pBlock));
  EXPECT_EQ(pBlock->info.rows, 2);
  blockDataDestroy(pBlock);
}

class InterpTimeSliceTest : public testing::Test {
 protected:
  SExecTaskInfo task = {};
  struct Source : InterpMergeFakeSource {
    std::vector<STableScanOperatorParam> notifications;
    std::vector<size_t> notifiedAt;
  } source;
  SInterpFuncPhysiNode* plan = nullptr;
  SOperatorInfo* op = nullptr;
  char taskId[32] = "interp-streaming";

  virtual EFillMode fillMode() { return FILL_MODE_LINEAR; }

  virtual EInterpMode interpMode() { return INTERP_MODE_GROUP_FIRST; }

  virtual int32_t scanType() { return QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN; }

  virtual bool ignoreNull() { return false; }

  virtual __optr_get_ext_fn_t pullFn() { return optrDefaultGetNextExtFn; }

  virtual void configurePlan() {}

  void SetUp() override {
    task.id.str = taskId;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC,
                            reinterpret_cast<SNode**>(&plan)), 0);
    plan->interpMode = interpMode();
    plan->fillMode = fillMode();
    if (plan->fillMode == FILL_MODE_VALUE) {
      SNodeListNode* values = nullptr;
      ASSERT_EQ(nodesMakeNode(QUERY_NODE_NODE_LIST,
                              reinterpret_cast<SNode**>(&values)), 0);
      plan->pFillValues = reinterpret_cast<SNode*>(values);
      ASSERT_EQ(nodesListMakeStrictAppend(&values->pNodeList,
                                          makeIntValue(99)), 0);
    }
    plan->interval = 1;
    plan->timeRange = {0, 7};
    plan->node.pOutputDataBlockDesc = createInterpMergeTestDesc(2);
    ASSERT_NE(plan->node.pOutputDataBlockDesc, nullptr);
    auto* slot = reinterpret_cast<SSlotDescNode*>(
        nodesListGetNode(plan->node.pOutputDataBlockDesc->pSlots, 0));
    slot->dataType.type = TSDB_DATA_TYPE_DOUBLE;

    SColumnNode* ts = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN,
                            reinterpret_cast<SNode**>(&ts)), 0);
    ts->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
    ts->node.resType.bytes = sizeof(int64_t);
    ts->dataBlockId = 1;
    ts->slotId = 0;
    ts->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
    ts->isPrimTs = true;
    plan->pTimeSeries = reinterpret_cast<SNode*>(ts);

    SFunctionNode* func = nullptr;
    SColumnNode* value = nullptr;
    STargetNode* target = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION,
                            reinterpret_cast<SNode**>(&func)), 0);
    tstrncpy(func->functionName, "interp", TSDB_FUNC_NAME_LEN);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN,
                            reinterpret_cast<SNode**>(&value)), 0);
    value->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
    value->node.resType.bytes = sizeof(double);
    value->dataBlockId = 1;
    value->slotId = 1;
    value->colId = 2;
    ASSERT_EQ(nodesListMakeStrictAppend(&func->pParameterList,
                                        reinterpret_cast<SNode*>(value)),
              0);
    if (ignoreNull()) {
      ASSERT_EQ(nodesListMakeStrictAppend(&func->pParameterList,
                                          makeIntValue(1)), 0);
    }
    ASSERT_EQ(fmGetFuncInfo(func, nullptr, 0), 0);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_TARGET,
                            reinterpret_cast<SNode**>(&target)), 0);
    target->dataBlockId = 2;
    target->slotId = 0;
    target->pExpr = reinterpret_cast<SNode*>(func);
    ASSERT_EQ(nodesListMakeStrictAppend(&plan->pFuncs,
                                        reinterpret_cast<SNode*>(target)), 0);

    ASSERT_NO_FATAL_FAILURE(configurePlan());

    auto* downstream = static_cast<SOperatorInfo*>(
        taosMemoryCalloc(1, sizeof(SOperatorInfo)));
    ASSERT_NE(downstream, nullptr);
    setOperatorInfo(downstream, "TestSource", scanType(), false, OP_OPENED,
                    &source, &task);
    downstream->fpSet =
        createOperatorFpSet(optrDummyOpenFn, interpMergeFakeNext, nullptr,
                            nullptr, optrDefaultBufFn, nullptr, pullFn(),
                            nullptr);
    ASSERT_EQ(createTimeSliceOperatorInfo(downstream,
                                          reinterpret_cast<SPhysiNode*>(plan),
                                          &task, &op), 0);
    op->resultInfo.threshold = 3;
  }

  void TearDown() override {
    destroyOperator(op);
    nodesDestroyNode(reinterpret_cast<SNode*>(plan));
    for (auto* block : source.blocks) {
      blockDataDestroy(block);
    }
  }

  void addBlock(uint64_t group, int8_t phase, int64_t first, int32_t rows,
                bool nullValue = false) {
    SSDataBlock* block = nullptr;
    ASSERT_EQ(createDataBlock(&block), 0);
    SColumnInfoData ts = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP,
                                             sizeof(int64_t), 0);
    SColumnInfoData value = createColumnInfoData(TSDB_DATA_TYPE_DOUBLE,
                                                sizeof(double), 1);
    ASSERT_EQ(blockDataAppendColInfo(block, &ts), 0);
    ASSERT_EQ(blockDataAppendColInfo(block, &value), 0);
    ASSERT_EQ(blockDataEnsureCapacity(block, rows), 0);
    auto* tsCol = static_cast<SColumnInfoData*>(
        taosArrayGet(block->pDataBlock, 0));
    auto* valCol = static_cast<SColumnInfoData*>(
        taosArrayGet(block->pDataBlock, 1));
    for (int32_t i = 0; i < rows; ++i) {
      int64_t time = first + i;
      double val = time;
      ASSERT_EQ(colDataSetVal(tsCol, i,
                              reinterpret_cast<const char*>(&time), false),
                0);
      ASSERT_EQ(colDataSetVal(valCol, i,
                              reinterpret_cast<const char*>(&val),
                              nullValue),
                0);
    }
    block->info.rows = rows;
    block->info.id.blockId = 1;
    block->info.id.groupId = group;
    block->info.scanPhase = phase;
    source.blocks.push_back(block);
  }

  int32_t next(SSDataBlock** result) {
    int32_t code = setjmp(task.env);
    return code == 0 ? op->fpSet.getNextFn(op, result) : code;
  }
};

class InterpLegacyGroupTest : public InterpTimeSliceTest {
 protected:
  EInterpMode interpMode() override { return INTERP_MODE_LEGACY; }
};

TEST_F(InterpLegacyGroupTest, PreservesGroupIdAcrossBatchesAndGroups) {
  addBlock(17, SCAN_PHASE_MAIN, 0, 2);
  addBlock(17, SCAN_PHASE_MAIN, 2, 6);
  addBlock(42, SCAN_PHASE_MAIN, 0, 8);

  std::map<uint64_t, int32_t> groupRows;
  std::vector<uint64_t> groups;
  int32_t blocks = 0;
  while (true) {
    SSDataBlock* result = nullptr;
    ASSERT_EQ(next(&result), 0);
    if (result == nullptr) {
      break;
    }
    ASSERT_LT(++blocks, 20);
    uint64_t group = result->info.id.groupId;
    ASSERT_TRUE(group == 17 || group == 42);
    if (groups.empty() || groups.back() != group) {
      groups.push_back(group);
    }
    auto* value = static_cast<SColumnInfoData*>(
        taosArrayGet(result->pDataBlock, 0));
    for (int32_t i = 0; i < result->info.rows; ++i) {
      ASSERT_FALSE(colDataIsNull_s(value, i));
      EXPECT_DOUBLE_EQ(*reinterpret_cast<double*>(colDataGetData(value, i)),
                       groupRows[group]++);
    }
  }
  EXPECT_EQ(groups, (std::vector<uint64_t>{17, 42}));
  EXPECT_EQ(groupRows[17], 8);
  EXPECT_EQ(groupRows[42], 8);
  EXPECT_GT(blocks, 2);
}

class InterpSideStopTest : public InterpTimeSliceTest,
                           public testing::WithParamInterface<int> {
 protected:
  virtual bool multiGroup() { return GetParam() >= 2; }

  EInterpMode interpMode() override {
    return multiGroup() ? INTERP_MODE_MULTI_GROUP : INTERP_MODE_MAIN_FIRST;
  }

  int32_t scanType() override {
    return GetParam() % 2 == 0 ? QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN
                               : QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN;
  }

  bool ignoreNull() override { return true; }

  __optr_get_ext_fn_t pullFn() override { return pull; }

  uint64_t expectedScanGroup() { return multiGroup() ? 0 : 42; }

  static int32_t pull(SOperatorInfo* scan, SOperatorParam* param,
                     SSDataBlock** result) {
    auto* src = static_cast<Source*>(scan->info);
    if (param != nullptr && param->value != nullptr) {
      EXPECT_EQ(param->opType, scan->operatorType);
      src->notifications.push_back(
          *static_cast<STableScanOperatorParam*>(param->value));
      src->notifiedAt.push_back(src->index);
    }
    return optrDefaultGetNextExtFn(scan, param, result);
  }

  void drain(int32_t expectedRows) {
    int32_t rows = 0;
    std::map<uint64_t, int32_t> groupRows;
    while (true) {
      SSDataBlock* result = nullptr;
      ASSERT_EQ(next(&result), 0);
      if (result == nullptr) {
        break;
      }
      auto* value = static_cast<SColumnInfoData*>(
          taosArrayGet(result->pDataBlock, 0));
      for (int32_t i = 0; i < result->info.rows; ++i) {
        EXPECT_FALSE(colDataIsNull_s(value, i));
        EXPECT_DOUBLE_EQ(*reinterpret_cast<double*>(
                             colDataGetData(value, i)),
                         groupRows[result->info.id.groupId]++);
      }
      rows += result->info.rows;
    }
    EXPECT_EQ(rows, expectedRows);
  }

  void expectNotify(int8_t phase, uint64_t group, size_t at) {
    int32_t count = 0;
    for (size_t i = 0; i < source.notifications.size(); ++i) {
      const auto& notify = source.notifications[i];
      if (notify.notifyPhase == phase &&
          notify.notifyScanGroupId == group) {
        EXPECT_EQ(source.notifiedAt[i], at);
        EXPECT_TRUE(notify.notifyToProcess);
        ++count;
      }
    }
    EXPECT_EQ(count, 1);
  }
};

TEST_P(InterpSideStopTest, stopsNextWhilePrevIsPending) {
  addBlock(42, SCAN_PHASE_MAIN, 1, 7);
  addBlock(42, SCAN_PHASE_NEXT, 8, 1, true);
  addBlock(42, SCAN_PHASE_NEXT, 9, 1, true);
  addBlock(42, SCAN_PHASE_PREV, -1, 1);
  drain(8);
  expectNotify(SCAN_PHASE_NEXT, expectedScanGroup(), 2);
}

TEST_P(InterpSideStopTest, waitsForNeededNextReference) {
  addBlock(42, SCAN_PHASE_MAIN, 1, 6);
  addBlock(42, SCAN_PHASE_NEXT, 8, 1, true);
  addBlock(42, SCAN_PHASE_NEXT, 9, 1);
  addBlock(42, SCAN_PHASE_PREV, -1, 1);
  drain(8);
  expectNotify(SCAN_PHASE_NEXT, expectedScanGroup(), 3);
}

TEST_P(InterpSideStopTest, allNullMainStillNeedsBothReferences) {
  addBlock(42, SCAN_PHASE_MAIN, 0, 8, true);
  addBlock(42, SCAN_PHASE_NEXT, 8, 1, true);
  addBlock(42, SCAN_PHASE_NEXT, 9, 1);
  addBlock(42, SCAN_PHASE_PREV, -1, 1);
  drain(8);
  expectNotify(SCAN_PHASE_NEXT, expectedScanGroup(), 3);
  expectNotify(SCAN_PHASE_PREV, expectedScanGroup(), 4);
}

TEST_P(InterpSideStopTest, otherGroupKeepsNeededSideOpen) {
  addBlock(42, SCAN_PHASE_MAIN, 1, 7);
  addBlock(7, SCAN_PHASE_MAIN, 1, 6);
  addBlock(42, SCAN_PHASE_NEXT, 8, 1, true);
  addBlock(7, SCAN_PHASE_NEXT, 8, 1, true);
  addBlock(7, SCAN_PHASE_NEXT, 9, 1);
  addBlock(42, SCAN_PHASE_PREV, -1, 1);
  addBlock(7, SCAN_PHASE_PREV, -1, 1);
  drain(16);
  if (GetParam() < 2) {
    expectNotify(SCAN_PHASE_NEXT, 42, 3);
    expectNotify(SCAN_PHASE_NEXT, 7, 5);
  } else {
    expectNotify(SCAN_PHASE_NEXT, 0, 5);
  }
}

TEST_P(InterpSideStopTest, stopsPrevWithoutLeadingHole) {
  addBlock(42, SCAN_PHASE_MAIN, 0, 7);
  addBlock(42, SCAN_PHASE_NEXT, 8, 1);
  addBlock(42, SCAN_PHASE_PREV, -1, 1, true);
  addBlock(42, SCAN_PHASE_PREV, -2, 1, true);
  drain(8);
  expectNotify(SCAN_PHASE_PREV, expectedScanGroup(), 3);
}

TEST_P(InterpSideStopTest, emptyMainPreservesGroupMembership) {
  addBlock(42, SCAN_PHASE_NEXT, 8, 1, true);
  addBlock(42, SCAN_PHASE_NEXT, 9, 1);
  addBlock(42, SCAN_PHASE_PREV, -1, 1);
  drain(GetParam() < 2 ? 8 : 0);
  expectNotify(SCAN_PHASE_NEXT, expectedScanGroup(),
               GetParam() < 2 ? 2 : 1);
}

INSTANTIATE_TEST_SUITE_P(ScanTypes, InterpSideStopTest,
                         testing::Values(0, 1, 2, 3));

class InterpConstantSideStopTest : public InterpSideStopTest {
 protected:
  EFillMode fillMode() override {
    return GetParam() < 2 ? FILL_MODE_NULL : FILL_MODE_VALUE;
  }

  bool multiGroup() override { return false; }

  void drainFilledGroups(const std::vector<uint64_t>& groups,
                         bool hasMain = false) {
    std::map<uint64_t, int32_t> groupRows;
    while (true) {
      SSDataBlock* result = nullptr;
      ASSERT_EQ(next(&result), 0);
      if (result == nullptr) {
        break;
      }
      const auto group = result->info.id.groupId;
      ASSERT_NE(std::find(groups.begin(), groups.end(), group), groups.end());
      auto* value = static_cast<SColumnInfoData*>(
          taosArrayGet(result->pDataBlock, 0));
      for (int32_t i = 0; i < result->info.rows; ++i) {
        int32_t row = groupRows[group]++;
        ASSERT_LT(row, 8);
        bool mainRow = hasMain && row > 0 && row < 7;
        if (!mainRow && fillMode() == FILL_MODE_NULL) {
          EXPECT_TRUE(colDataIsNull_s(value, i));
        } else {
          ASSERT_FALSE(colDataIsNull_s(value, i));
          EXPECT_DOUBLE_EQ(*reinterpret_cast<double*>(
                               colDataGetData(value, i)),
                           mainRow ? row : 99);
        }
      }
    }
    for (uint64_t group : groups) {
      EXPECT_EQ(groupRows[group], 8);
    }
  }
};

TEST_P(InterpConstantSideStopTest, discoversNextOnlyGroupsWithoutReferences) {
  addBlock(42, SCAN_PHASE_NEXT, 8, 1, true);
  addBlock(42, SCAN_PHASE_NEXT, 9, 1, true);
  addBlock(7, SCAN_PHASE_NEXT, 8, 1, true);
  addBlock(7, SCAN_PHASE_NEXT, 9, 1, true);
  addBlock(42, SCAN_PHASE_PREV, -1, 1, true);
  addBlock(7, SCAN_PHASE_PREV, -1, 1, true);
  drainFilledGroups({42, 7});
  expectNotify(SCAN_PHASE_NEXT, 42, 1);
  expectNotify(SCAN_PHASE_NEXT, 7, 3);
  expectNotify(SCAN_PHASE_PREV, 42, 5);
  expectNotify(SCAN_PHASE_PREV, 7, 6);
  EXPECT_EQ(source.notifications.size(), 4);
}

TEST_P(InterpConstantSideStopTest, discoversPrevOnlyGroupsWithoutReferences) {
  addBlock(42, SCAN_PHASE_PREV, -1, 1, true);
  addBlock(42, SCAN_PHASE_PREV, -2, 1, true);
  addBlock(7, SCAN_PHASE_PREV, -1, 1, true);
  addBlock(7, SCAN_PHASE_PREV, -2, 1, true);
  drainFilledGroups({42, 7});
  expectNotify(SCAN_PHASE_PREV, 42, 1);
  expectNotify(SCAN_PHASE_PREV, 7, 3);
  EXPECT_EQ(source.notifications.size(), 2);
}

TEST_P(InterpConstantSideStopTest, fillsMainHolesWithoutSideReferences) {
  addBlock(42, SCAN_PHASE_MAIN, 1, 6);
  addBlock(42, SCAN_PHASE_NEXT, 8, 1, true);
  addBlock(42, SCAN_PHASE_NEXT, 9, 1, true);
  addBlock(42, SCAN_PHASE_PREV, -1, 1, true);
  addBlock(42, SCAN_PHASE_PREV, -2, 1, true);
  drainFilledGroups({42}, true);
  expectNotify(SCAN_PHASE_NEXT, 42, 2);
  expectNotify(SCAN_PHASE_PREV, 42, 4);
  EXPECT_EQ(source.notifications.size(), 2);
}

TEST_P(InterpConstantSideStopTest, resetAllowsSameGroupToNotifyAgain) {
  addBlock(42, SCAN_PHASE_NEXT, 8, 1, true);
  addBlock(42, SCAN_PHASE_NEXT, 9, 1, true);
  addBlock(42, SCAN_PHASE_PREV, -1, 1, true);
  for (int32_t run = 0; run < 2; ++run) {
    drainFilledGroups({42});
    expectNotify(SCAN_PHASE_NEXT, 42, 1);
    expectNotify(SCAN_PHASE_PREV, 42, 3);
    EXPECT_EQ(source.notifications.size(), 2);
    if (run == 0) {
      ASSERT_EQ(op->fpSet.resetStateFn(op), 0);
      source.index = 0;
      source.notifications.clear();
      source.notifiedAt.clear();
    }
  }
}

INSTANTIATE_TEST_SUITE_P(ScanTypes, InterpConstantSideStopTest,
                         testing::Values(0, 1, 2, 3));

class InterpGroupFirstTest : public InterpTimeSliceTest {
 protected:
  void checkBoundaries(bool needPrev, bool needNext) {
    const uint64_t groups[] = {42, 7};
    for (uint64_t group : groups) {
      int32_t first = needPrev ? 1 : 0;
      int32_t end = needNext ? 6 : 7;
      for (int32_t ts = first; ts <= end; ts += 2) {
        addBlock(group, SCAN_PHASE_MAIN, ts, TMIN(2, end - ts + 1));
      }
      if (needNext) {
        addBlock(group, SCAN_PHASE_NEXT, 8, 1);
      }
      if (needPrev) {
        addBlock(group, SCAN_PHASE_PREV, -1, 1);
      }
    }
    int32_t row = 0;
    while (true) {
      SSDataBlock* result = nullptr;
      ASSERT_EQ(next(&result), 0);
      if (result == nullptr) {
        break;
      }
      if (row == 0 && !needPrev) {
        EXPECT_EQ(source.index, 2);
      }
      auto* value = static_cast<SColumnInfoData*>(
          taosArrayGet(result->pDataBlock, 0));
      for (int32_t i = 0; i < result->info.rows; ++i, ++row) {
        ASSERT_LT(row, 16);
        EXPECT_EQ(result->info.id.groupId, groups[row / 8]);
        EXPECT_DOUBLE_EQ(*reinterpret_cast<double*>(
                             colDataGetData(value, i)), row % 8);
      }
    }
    EXPECT_EQ(row, 16);
  }
};

class InterpGroupNextTest : public InterpGroupFirstTest {
 protected:
  EFillMode fillMode() override { return FILL_MODE_NEXT; }
};

TEST_F(InterpGroupNextTest, interpStreamsLeadingFillWithoutPrev) {
  for (int32_t first = 1; first < 8; first += 2) {
    addBlock(42, SCAN_PHASE_MAIN, first, TMIN(2, 8 - first));
  }
  SSDataBlock* result = nullptr;
  ASSERT_EQ(next(&result), 0);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->info.rows, 4);
  EXPECT_EQ(source.index, 2);
  auto* value = static_cast<SColumnInfoData*>(
      taosArrayGet(result->pDataBlock, 0));
  const double expected[] = {1, 1, 2, 3};
  for (int32_t i = 0; i < 4; ++i) {
    EXPECT_DOUBLE_EQ(*reinterpret_cast<double*>(colDataGetData(value, i)),
                     expected[i]);
  }
}

TEST_F(InterpGroupFirstTest, interpMainOnlyKeepsGroupsContinuous) {
  checkBoundaries(false, false);
}

TEST_F(InterpGroupFirstTest, interpMainNextStreamsBeforeTailResolution) {
  checkBoundaries(false, true);
}

TEST_F(InterpGroupFirstTest, interpMainPrevWaitsForLeadingReference) {
  checkBoundaries(true, false);
}

TEST_F(InterpGroupFirstTest, interpMainNextPrevKeepsAllThreeStagesOrdered) {
  checkBoundaries(true, true);
}

TEST_F(InterpGroupFirstTest, interpStreamsPartiallyConsumedMainBlock) {
  addBlock(42, SCAN_PHASE_MAIN, 0, 8);
  for (int32_t first : {0, 4}) {
    SSDataBlock* result = nullptr;
    ASSERT_EQ(next(&result), 0);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->info.rows, 4);
    EXPECT_EQ(source.index, 1);
    auto* value = static_cast<SColumnInfoData*>(
        taosArrayGet(result->pDataBlock, 0));
    EXPECT_DOUBLE_EQ(*reinterpret_cast<double*>(colDataGetData(value, 0)),
                     first);
  }
  SSDataBlock* result = nullptr;
  ASSERT_EQ(next(&result), 0);
  EXPECT_EQ(result, nullptr);
}

TEST_F(InterpGroupFirstTest, interpStreamsMainBeforeItsLastInputBlock) {
  for (int32_t first = 0; first < 8; first += 2) {
    addBlock(42, SCAN_PHASE_MAIN, first, 2);
  }
  SSDataBlock* result = nullptr;
  ASSERT_EQ(next(&result), 0);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->info.rows, 4);
  EXPECT_EQ(source.index, 2);
  auto* value = static_cast<SColumnInfoData*>(
      taosArrayGet(result->pDataBlock, 0));
  EXPECT_DOUBLE_EQ(*reinterpret_cast<double*>(colDataGetData(value, 0)), 0);
  EXPECT_DOUBLE_EQ(*reinterpret_cast<double*>(colDataGetData(value, 3)), 3);
}

TEST_F(InterpGroupFirstTest, interpResetDiscardsPartialMainInput) {
  addBlock(42, SCAN_PHASE_MAIN, 0, 8);
  SSDataBlock* result = nullptr;
  ASSERT_EQ(next(&result), 0);
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->info.rows, 4);
  ASSERT_EQ(op->fpSet.resetStateFn(op), 0);
  source.index = 0;
  int32_t rows = 0;
  while (true) {
    ASSERT_EQ(next(&result), 0);
    if (result == nullptr) {
      break;
    }
    auto* value = static_cast<SColumnInfoData*>(
        taosArrayGet(result->pDataBlock, 0));
    for (int32_t i = 0; i < result->info.rows; ++i) {
      EXPECT_DOUBLE_EQ(*reinterpret_cast<double*>(
                           colDataGetData(value, i)), rows++);
    }
  }
  EXPECT_EQ(rows, 8);
}

TEST_F(InterpGroupFirstTest, interpFilteredPrefixDoesNotReturnFalseEof) {
  SValueNode* condition = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_VALUE,
                         reinterpret_cast<SNode**>(&condition)), 0);
  condition->node.resType.type = TSDB_DATA_TYPE_BOOL;
  condition->node.resType.bytes = sizeof(bool);
  bool value = false;
  ASSERT_EQ(nodesSetValueNodeValue(condition, &value), 0);
  condition->translate = true;
  ASSERT_EQ(filterInitFromNode(reinterpret_cast<SNode*>(condition),
                              &op->exprSupp.pFilterInfo, 0, nullptr), 0);
  nodesDestroyNode(reinterpret_cast<SNode*>(condition));
  addBlock(42, SCAN_PHASE_MAIN, 0, 8);
  addBlock(7, SCAN_PHASE_MAIN, 0, 8);
  SSDataBlock* result = nullptr;
  ASSERT_EQ(next(&result), 0);
  EXPECT_EQ(result, nullptr);
  EXPECT_EQ(source.index, 2);
  EXPECT_EQ(op->status, OP_EXEC_DONE);
}

class InterpGroupGapTest : public InterpGroupFirstTest {
 protected:
  static constexpr int64_t anchor = 8192;

  EFillMode fillMode() override { return FILL_MODE_NEXT; }

  virtual int64_t rangeEnd() { return anchor; }

  virtual int64_t timeStep() { return 1; }

  void configurePlan() override {
    plan->timeRange.ekey = rangeEnd();
    plan->interval = timeStep();
    plan->timelineSource = TIME_LINE_SOURCE_DEGRADED_TS;

    SSlotDescNode* slot = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_SLOT_DESC,
                           reinterpret_cast<SNode**>(&slot)), 0);
    slot->slotId = 1;
    slot->dataType.type = TSDB_DATA_TYPE_TIMESTAMP;
    slot->dataType.bytes = sizeof(int64_t);
    slot->output = true;
    auto* desc = plan->node.pOutputDataBlockDesc;
    ASSERT_EQ(nodesListMakeStrictAppend(&desc->pSlots,
                                       reinterpret_cast<SNode*>(slot)), 0);
    desc->totalRowSize += sizeof(int64_t);
    desc->outputRowSize += sizeof(int64_t);

    SFunctionNode* func = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION,
                           reinterpret_cast<SNode**>(&func)), 0);
    tstrncpy(func->functionName, "_irowts", TSDB_FUNC_NAME_LEN);
    ASSERT_EQ(fmGetFuncInfo(func, nullptr, 0), 0);
    STargetNode* target = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_TARGET,
                           reinterpret_cast<SNode**>(&target)), 0);
    target->dataBlockId = 2;
    target->slotId = 1;
    target->pExpr = reinterpret_cast<SNode*>(func);
    ASSERT_EQ(nodesListMakeStrictAppend(&plan->pFuncs,
                                       reinterpret_cast<SNode*>(target)), 0);
  }

  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(InterpGroupFirstTest::SetUp());
    ASSERT_NE(op, nullptr);
    op->resultInfo.threshold = 31;
    ASSERT_GT(anchor, op->resultInfo.capacity);
  }

  void addSparseBlock(uint64_t group) {
    ASSERT_NO_FATAL_FAILURE(addBlock(group, SCAN_PHASE_MAIN, 0, 2));
    auto* block = source.blocks.back();
    auto* ts = static_cast<SColumnInfoData*>(
        taosArrayGet(block->pDataBlock, 0));
    auto* value = static_cast<SColumnInfoData*>(
        taosArrayGet(block->pDataBlock, 1));
    int64_t time = anchor;
    double val = anchor;
    ASSERT_EQ(colDataSetVal(ts, 1, reinterpret_cast<const char*>(&time),
                           false), 0);
    ASSERT_EQ(colDataSetVal(value, 1, reinterpret_cast<const char*>(&val),
                           false), 0);
  }

  virtual double expectedValue(int64_t time, bool realAtStart) {
    if (time == rangeEnd() || (realAtStart && time == 0)) {
      return time;
    }
    return fillMode() == FILL_MODE_VALUE ? 99 : anchor;
  }

  void drain(const std::vector<uint64_t>& groups, bool realAtStart = false,
             int64_t first = 0) {
    for (uint64_t group : groups) {
      int64_t time = first;
      while (time <= rangeEnd()) {
        SSDataBlock* result = nullptr;
        ASSERT_EQ(next(&result), 0);
        ASSERT_NE(result, nullptr);
        ASSERT_GT(result->info.rows, 0);
        ASSERT_LE(result->info.rows, op->resultInfo.threshold + 1);
        ASSERT_EQ(result->info.capacity, op->resultInfo.capacity);
        ASSERT_EQ(result->info.id.groupId, group);
        auto* value = static_cast<SColumnInfoData*>(
            taosArrayGet(result->pDataBlock, 0));
        auto* ts = static_cast<SColumnInfoData*>(
            taosArrayGet(result->pDataBlock, 1));
        for (int32_t i = 0; i < result->info.rows;
             ++i, time += timeStep()) {
          ASSERT_LE(time, rangeEnd());
          ASSERT_FALSE(colDataIsNull_s(ts, i));
          EXPECT_EQ(*reinterpret_cast<int64_t*>(colDataGetData(ts, i)),
                    time);
          bool real = time == rangeEnd() || (realAtStart && time == 0);
          if (fillMode() == FILL_MODE_NULL && !real) {
            EXPECT_TRUE(colDataIsNull_s(value, i));
          } else {
            ASSERT_FALSE(colDataIsNull_s(value, i));
            EXPECT_DOUBLE_EQ(*reinterpret_cast<double*>(
                                 colDataGetData(value, i)),
                             expectedValue(time, realAtStart));
          }
        }
      }
    }
    SSDataBlock* result = nullptr;
    ASSERT_EQ(next(&result), 0);
    EXPECT_EQ(result, nullptr);
  }
};

constexpr int64_t InterpGroupGapTest::anchor;

class InterpGroupLeadingGapTest
    : public InterpGroupGapTest,
      public testing::WithParamInterface<EFillMode> {
 protected:
  EFillMode fillMode() override { return GetParam(); }
};

TEST_P(InterpGroupLeadingGapTest, boundsLeadingFillAcrossGroups) {
  addBlock(42, SCAN_PHASE_MAIN, anchor, 1);
  addBlock(7, SCAN_PHASE_MAIN, anchor, 1);
  drain({42, 7});
}

TEST_P(InterpGroupLeadingGapTest, resumesGapBeforeLastInputRow) {
  addSparseBlock(42);
  drain({42}, true);
}

TEST_P(InterpGroupLeadingGapTest, resetDiscardsPartiallyFilledGap) {
  addBlock(42, SCAN_PHASE_MAIN, anchor, 1);
  SSDataBlock* result = nullptr;
  ASSERT_EQ(next(&result), 0);
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->info.rows, 32);
  ASSERT_EQ(result->info.capacity, op->resultInfo.capacity);
  ASSERT_EQ(source.index, 1);
  ASSERT_EQ(op->fpSet.resetStateFn(op), 0);
  source.index = 0;
  drain({42});
}

TEST_P(InterpGroupLeadingGapTest, filteredGapResumesToRealRows) {
  SColumnNode* ts = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN,
                         reinterpret_cast<SNode**>(&ts)), 0);
  ts->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  ts->node.resType.bytes = sizeof(int64_t);
  ts->dataBlockId = 2;
  ts->slotId = 1;
  SNode* condition = makeEqualCond(reinterpret_cast<SNode*>(ts),
                                 makeIntValue(anchor));
  plan->node.pConditions = condition;
  ASSERT_EQ(filterInitFromNode(condition, &op->exprSupp.pFilterInfo,
                              0, nullptr), 0);
  addBlock(42, SCAN_PHASE_MAIN, anchor, 1);
  addBlock(7, SCAN_PHASE_MAIN, anchor, 1);
  drain({42, 7}, false, anchor);
  EXPECT_EQ(source.index, 2);
  EXPECT_EQ(op->status, OP_EXEC_DONE);
}

INSTANTIATE_TEST_SUITE_P(FillModes, InterpGroupLeadingGapTest,
                         testing::Values(FILL_MODE_NEXT, FILL_MODE_NULL,
                                         FILL_MODE_VALUE));

class InterpGroupLinearGapTest : public InterpGroupGapTest {
 protected:
  EFillMode fillMode() override { return FILL_MODE_LINEAR; }

  int64_t rangeEnd() override { return 2 * anchor; }

  double expectedValue(int64_t time, bool realAtStart) override {
    return time <= anchor ? time : anchor + 2 * (time - anchor);
  }
};

TEST_F(InterpGroupLinearGapTest, preservesAnchorsAcrossRepeatedGapResumes) {
  addSparseBlock(42);
  addBlock(42, SCAN_PHASE_MAIN, 2 * anchor, 1);
  auto* value = static_cast<SColumnInfoData*>(
      taosArrayGet(source.blocks.back()->pDataBlock, 1));
  double val = 3 * anchor;
  ASSERT_EQ(colDataSetVal(value, 0, reinterpret_cast<const char*>(&val),
                         false), 0);
  drain({42}, true);
}

class InterpGroupUnalignedGapTest : public InterpGroupLinearGapTest {
 protected:
  int64_t rangeEnd() override { return 192; }

  int64_t timeStep() override { return 2; }

  double expectedValue(int64_t time, bool realAtStart) override {
    if (time <= 63) {
      return time;
    }
    return time <= 129 ? 63 + 3 * (time - 63) : 261 + 2 * (time - 129);
  }
};

TEST_F(InterpGroupUnalignedGapTest, preservesReferencesPastNonGridAlignedRow) {
  const int64_t times[] = {0, 63, 129, 192};
  const double values[] = {0, 63, 261, 387};
  for (size_t i = 0; i < 4; ++i) {
    addBlock(42, SCAN_PHASE_MAIN, times[i], 1);
    auto* value = static_cast<SColumnInfoData*>(
        taosArrayGet(source.blocks.back()->pDataBlock, 1));
    ASSERT_EQ(colDataSetVal(value, 0,
                           reinterpret_cast<const char*>(&values[i]),
                           false), 0);
  }
  drain({42}, true);
}

TEST_F(InterpGroupGapTest, rejectsDuplicateTimestampAfterResumedRow) {
  addBlock(42, SCAN_PHASE_MAIN, anchor - 1, 1);
  addBlock(42, SCAN_PHASE_MAIN, anchor - 1, 2);
  SSDataBlock* result = nullptr;
  for (int32_t block = 0; block < anchor / 32; ++block) {
    ASSERT_EQ(next(&result), 0);
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->info.rows, 32);
    ASSERT_EQ(result->info.capacity, op->resultInfo.capacity);
  }
  EXPECT_EQ(next(&result), TSDB_CODE_FUNC_DUP_TIMESTAMP);
}

namespace {

struct InterpStoredSequenceOutput {
  SInterpMainFirstOutputTestCtx* context = nullptr;
  std::vector<SSDataBlock*> sources;

  ~InterpStoredSequenceOutput() {
    interpTestDestroyMainFirstOutput(context);
    for (SSDataBlock* source : sources) {
      blockDataDestroy(source);
    }
  }
};

void expectInterpStoredSequenceOutput(
    const std::vector<int32_t>& fragmentRows, int32_t capacity,
    int32_t threshold, const std::vector<int32_t>& expectedRows) {
  constexpr uint64_t groupId = 42;
  constexpr uint64_t baseGroupId = 420;
  constexpr int32_t scanFlag = 7;
  InterpStoredSequenceOutput output;
  output.sources.resize(fragmentRows.size(), nullptr);

  int32_t firstValue = 0;
  for (size_t i = 0; i < output.sources.size(); ++i) {
    ASSERT_EQ(createInterpStoreSequenceBlock(
                  groupId, firstValue, fragmentRows[i],
                  &output.sources[i]),
              TSDB_CODE_SUCCESS);
    output.sources[i]->info.id.baseGId = baseGroupId;
    output.sources[i]->info.scanFlag = scanFlag;
    output.sources[i]->info.scanPhase = SCAN_PHASE_MAIN;
    firstValue += fragmentRows[i];
  }

  ASSERT_EQ(interpTestCreateMainFirstOutput(
                output.sources.data(),
                static_cast<int32_t>(output.sources.size()),
                capacity, threshold, &output.context),
            TSDB_CODE_SUCCESS);
  for (SSDataBlock* pSource : output.sources) {
    blockDataDestroy(pSource);
  }
  output.sources.clear();

  int32_t expectedValue = 0;
  for (int32_t rows : expectedRows) {
    SSDataBlock* pOutput = nullptr;
    ASSERT_EQ(interpTestNextMainFirstOutput(output.context, &pOutput),
              TSDB_CODE_SUCCESS);
    ASSERT_NE(pOutput, nullptr);
    ASSERT_EQ(pOutput->info.rows, rows);
    EXPECT_EQ(pOutput->info.capacity, capacity);
    EXPECT_EQ(pOutput->info.id.groupId, groupId);
    EXPECT_EQ(pOutput->info.id.baseGId, baseGroupId);
    EXPECT_EQ(pOutput->info.scanFlag, scanFlag);
    EXPECT_EQ(pOutput->info.scanPhase, SCAN_PHASE_MAIN);

    SColumnInfoData* pColumn = static_cast<SColumnInfoData*>(
        taosArrayGet(pOutput->pDataBlock, 0));
    ASSERT_NE(pColumn, nullptr);
    for (int32_t row = 0; row < rows; ++row) {
      ASSERT_FALSE(colDataIsNull_s(pColumn, row));
      EXPECT_EQ(*reinterpret_cast<int32_t*>(
                    colDataGetData(pColumn, row)),
                expectedValue++);
    }
  }

  SSDataBlock* pOutput = nullptr;
  ASSERT_EQ(interpTestNextMainFirstOutput(output.context, &pOutput),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pOutput, nullptr);
  EXPECT_EQ(expectedValue, firstValue);
}

void expectInterpStoredGroup(SInterpResultStore* pStore, uint64_t groupId,
                            const std::vector<SSDataBlock*>& expected) {
  ASSERT_EQ(interpResultStoreOpenRead(pStore, groupId), TSDB_CODE_SUCCESS);
  SSDataBlock* pRead = nullptr;
  for (SSDataBlock* pExpected : expected) {
    ASSERT_EQ(interpResultStoreNextBlock(pStore, &pRead),
              TSDB_CODE_SUCCESS);
    ASSERT_NO_FATAL_FAILURE(expectInterpStoreBlocksEqual(pExpected, pRead));
  }
  ASSERT_EQ(interpResultStoreNextBlock(pStore, &pRead), TSDB_CODE_SUCCESS);
  ASSERT_EQ(pRead, nullptr);
  ASSERT_EQ(interpResultStoreDropGroup(pStore, groupId), TSDB_CODE_SUCCESS);
}

}  // namespace

TEST(execUtilTest, interpMainFirstCoalescesStoredFragmentsToOutputLimit) {
  ASSERT_NO_FATAL_FAILURE(
      expectInterpStoredSequenceOutput({4, 5, 4}, 10, 9, {10, 3}));
}

TEST(execUtilTest, interpMainFirstSplitsOneOversizedStoredBlock) {
  ASSERT_NO_FATAL_FAILURE(
      expectInterpStoredSequenceOutput({13}, 10, 9, {10, 3}));
}

TEST(execUtilTest, interpMainFirstTreatsThresholdAsSoftLimit) {
  ASSERT_NO_FATAL_FAILURE(
      expectInterpStoredSequenceOutput({4, 7}, 20, 9, {11}));
}

TEST(execUtilTest, interpMainFirstCoalescesManyStoredFragments) {
  ASSERT_NO_FATAL_FAILURE(expectInterpStoredSequenceOutput(
      std::vector<int32_t>(100, 10), 1024, 1000, {1000}));
}

TEST(execUtilTest, interpMainFirstNormalizesDerivedOutputMetadata) {
  SSDataBlock* pSource = nullptr;
  ASSERT_EQ(createInterpStoreVarBlock(42, 100, 4, &pSource),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(setInterpStoreVarPks(pSource, "first-pk", "last-pk"),
            TSDB_CODE_SUCCESS);

  SInterpMainFirstOutputTestCtx* pCtx = nullptr;
  ASSERT_EQ(interpTestCreateMainFirstOutput(
                &pSource, 1, 10, 9, &pCtx),
            TSDB_CODE_SUCCESS);
  blockDataDestroy(pSource);

  SSDataBlock* pOutput = nullptr;
  ASSERT_EQ(interpTestNextMainFirstOutput(pCtx, &pOutput),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(pOutput, nullptr);
  EXPECT_EQ(pOutput->info.window.skey, 0);
  EXPECT_EQ(pOutput->info.window.ekey, 0);
  EXPECT_EQ(pOutput->info.dataLoad, 0);
  EXPECT_FALSE(pOutput->info.blankFill);
  EXPECT_EQ(pOutput->info.pks[0].type, 0);
  EXPECT_EQ(pOutput->info.pks[1].type, 0);
  EXPECT_EQ(pOutput->info.pks[0].pData, nullptr);
  EXPECT_EQ(pOutput->info.pks[1].pData, nullptr);
  EXPECT_EQ(pOutput->info.pBlockAgg, nullptr);
  EXPECT_EQ(pOutput->pBlockAgg, nullptr);
  EXPECT_STREQ(pOutput->info.parTbName, "");
  EXPECT_EQ(pOutput->info.id.groupId, 42);
  EXPECT_EQ(pOutput->info.id.baseGId, 43);
  EXPECT_EQ(pOutput->info.scanFlag, 7);
  EXPECT_EQ(pOutput->info.scanPhase, SCAN_PHASE_PREV);
  interpTestDestroyMainFirstOutput(pCtx);
}

TEST(execUtilTest, interpMainFirstMergesVariableColumnsAndNulls) {
  const std::array<int32_t, 3> fragmentRows = {4, 5, 4};
  std::array<SSDataBlock*, 3> sources = {};
  std::vector<int32_t> expectedValues;
  std::vector<bool> expectedIntNulls;
  std::vector<bool> expectedStrNulls;
  std::vector<std::string> expectedStrings;

  int32_t seed = 0;
  for (size_t i = 0; i < sources.size(); ++i) {
    ASSERT_EQ(createInterpStoreVarBlock(
                  42, seed, fragmentRows[i], &sources[i]),
              TSDB_CODE_SUCCESS);
    std::string firstPk = "first-" + std::to_string(i);
    std::string lastPk = "last-" + std::to_string(i);
    ASSERT_EQ(setInterpStoreVarPks(
                  sources[i], firstPk.c_str(), lastPk.c_str()),
              TSDB_CODE_SUCCESS);
    for (int32_t row = 0; row < fragmentRows[i]; ++row) {
      int32_t value = seed + row;
      int32_t strLen = 24 + row % 16;
      expectedValues.push_back(value);
      expectedIntNulls.push_back(row % 4 == 1);
      expectedStrNulls.push_back(row % 3 == 2);
      expectedStrings.emplace_back(
          strLen, static_cast<char>('a' + value % 26));
    }
    seed += fragmentRows[i];
  }

  SInterpMainFirstOutputTestCtx* pCtx = nullptr;
  ASSERT_EQ(interpTestCreateMainFirstOutput(
                sources.data(), static_cast<int32_t>(sources.size()),
                10, 9, &pCtx),
            TSDB_CODE_SUCCESS);
  for (SSDataBlock* pSource : sources) {
    blockDataDestroy(pSource);
  }

  const std::array<int32_t, 2> outputRows = {10, 3};
  int32_t expectedIndex = 0;
  for (int32_t rows : outputRows) {
    SSDataBlock* pOutput = nullptr;
    ASSERT_EQ(interpTestNextMainFirstOutput(pCtx, &pOutput),
              TSDB_CODE_SUCCESS);
    ASSERT_NE(pOutput, nullptr);
    ASSERT_EQ(pOutput->info.rows, rows);
    EXPECT_EQ(pOutput->info.pks[0].type, 0);
    EXPECT_EQ(pOutput->info.pks[1].type, 0);

    SColumnInfoData* pInt = static_cast<SColumnInfoData*>(
        taosArrayGet(pOutput->pDataBlock, 0));
    SColumnInfoData* pStr = static_cast<SColumnInfoData*>(
        taosArrayGet(pOutput->pDataBlock, 1));
    ASSERT_NE(pInt, nullptr);
    ASSERT_NE(pStr, nullptr);
    for (int32_t row = 0; row < rows; ++row, ++expectedIndex) {
      EXPECT_EQ(colDataIsNull_s(pInt, row),
                expectedIntNulls[expectedIndex]);
      EXPECT_EQ(colDataIsNull_s(pStr, row),
                expectedStrNulls[expectedIndex]);
      if (!expectedIntNulls[expectedIndex]) {
        EXPECT_EQ(*reinterpret_cast<int32_t*>(
                      colDataGetData(pInt, row)),
                  expectedValues[expectedIndex]);
      }
      if (!expectedStrNulls[expectedIndex]) {
        char* pValue = colDataGetData(pStr, row);
        std::string actual(varDataVal(pValue),
                           varDataLen(pValue));
        EXPECT_EQ(actual, expectedStrings[expectedIndex]);
      }
    }
  }
  EXPECT_EQ(expectedIndex, 13);
  interpTestDestroyMainFirstOutput(pCtx);
}

TEST(execUtilTest, interpMainFirstSealsIncompatibleStoredMetadata) {
  std::array<SSDataBlock*, 2> sources = {};
  ASSERT_EQ(createInterpStoreSequenceBlock(42, 0, 4, &sources[0]),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreSequenceBlock(42, 4, 5, &sources[1]),
            TSDB_CODE_SUCCESS);
  sources[0]->info.id.baseGId = 420;
  sources[1]->info.id.baseGId = 421;

  SInterpMainFirstOutputTestCtx* pCtx = nullptr;
  ASSERT_EQ(interpTestCreateMainFirstOutput(
                sources.data(), static_cast<int32_t>(sources.size()),
                10, 9, &pCtx),
            TSDB_CODE_SUCCESS);
  for (SSDataBlock* pSource : sources) {
    blockDataDestroy(pSource);
  }

  const std::array<int32_t, 2> expectedRows = {4, 5};
  const std::array<uint64_t, 2> expectedBaseGroups = {420, 421};
  for (size_t i = 0; i < expectedRows.size(); ++i) {
    SSDataBlock* pOutput = nullptr;
    ASSERT_EQ(interpTestNextMainFirstOutput(pCtx, &pOutput),
              TSDB_CODE_SUCCESS);
    ASSERT_NE(pOutput, nullptr);
    EXPECT_EQ(pOutput->info.rows, expectedRows[i]);
    EXPECT_EQ(pOutput->info.id.baseGId, expectedBaseGroups[i]);
  }

  SSDataBlock* pOutput = nullptr;
  ASSERT_EQ(interpTestNextMainFirstOutput(pCtx, &pOutput),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pOutput, nullptr);
  interpTestDestroyMainFirstOutput(pCtx);
}

TEST(execUtilTest, interpMainFirstContinuesAfterFilterRemovesBatch) {
  std::array<SSDataBlock*, 2> sources = {};
  ASSERT_EQ(createInterpStoreSequenceBlock(42, 0, 13, &sources[0]),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreSequenceBlock(7, 100, 4, &sources[1]),
            TSDB_CODE_SUCCESS);

  SInterpMainFirstOutputTestCtx* pCtx = nullptr;
  ASSERT_EQ(interpTestCreateMainFirstOutput(
                sources.data(), static_cast<int32_t>(sources.size()),
                10, 9, &pCtx),
            TSDB_CODE_SUCCESS);
  for (SSDataBlock* pSource : sources) {
    blockDataDestroy(pSource);
  }
  ASSERT_EQ(interpTestSetMainFirstOutputFilterFalse(pCtx),
            TSDB_CODE_SUCCESS);
  EXPECT_GT(interpTestMainFirstOutputStoreBytes(pCtx), 0);

  SSDataBlock* pOutput = nullptr;
  ASSERT_EQ(interpTestNextMainFirstOutput(pCtx, &pOutput),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pOutput, nullptr);
  EXPECT_EQ(interpTestMainFirstOutputStoreBytes(pCtx), 0);
  interpTestDestroyMainFirstOutput(pCtx);
}

TEST(execUtilTest, interpMainFirstResetClearsPartialStoredCursor) {
  SSDataBlock* pSource = nullptr;
  ASSERT_EQ(createInterpStoreSequenceBlock(42, 0, 13, &pSource),
            TSDB_CODE_SUCCESS);

  SInterpMainFirstOutputTestCtx* pCtx = nullptr;
  ASSERT_EQ(interpTestCreateMainFirstOutput(
                &pSource, 1, 10, 9, &pCtx),
            TSDB_CODE_SUCCESS);
  blockDataDestroy(pSource);

  SSDataBlock* pOutput = nullptr;
  ASSERT_EQ(interpTestNextMainFirstOutput(pCtx, &pOutput),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(pOutput, nullptr);
  ASSERT_EQ(pOutput->info.rows, 10);
  EXPECT_GT(interpTestMainFirstOutputStoreBytes(pCtx), 0);

  interpTestResetMainFirstOutput(pCtx);
  EXPECT_EQ(interpTestMainFirstOutputStoreBytes(pCtx), 0);
  ASSERT_EQ(interpTestNextMainFirstOutput(pCtx, &pOutput),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pOutput, nullptr);
  interpTestDestroyMainFirstOutput(pCtx);
}

TEST(execUtilTest, interpMainFirstDoesNotCoalesceAcrossGroups) {
  constexpr int64_t outputVersion = 101;
  constexpr int32_t outputChildId = 102;
  constexpr int64_t outputCalStart = 103;
  constexpr int64_t outputCalEnd = 104;
  constexpr int64_t outputWatermark = 105;
  std::array<SSDataBlock*, 4> sources = {};
  const std::array<uint64_t, 4> groupIds = {42, 42, 7, 7};
  const std::array<int32_t, 4> firstValues = {0, 4, 100, 103};
  const std::array<int32_t, 4> rows = {4, 5, 3, 4};
  for (size_t i = 0; i < sources.size(); ++i) {
    ASSERT_EQ(createInterpStoreSequenceBlock(
                  groupIds[i], firstValues[i], rows[i], &sources[i]),
              TSDB_CODE_SUCCESS);
    sources[i]->info.id.baseGId = groupIds[i] * 10;
    sources[i]->info.version = i < 2 ? outputVersion : 201;
    sources[i]->info.childId = i < 2 ? outputChildId : 202;
    sources[i]->info.calWin =
        i < 2 ? STimeWindow{outputCalStart, outputCalEnd}
              : STimeWindow{203, 204};
    sources[i]->info.watermark =
        i < 2 ? outputWatermark : 205;
  }

  SInterpMainFirstOutputTestCtx* pCtx = nullptr;
  ASSERT_EQ(interpTestCreateMainFirstOutput(
                sources.data(), static_cast<int32_t>(sources.size()),
                10, 9, &pCtx),
            TSDB_CODE_SUCCESS);
  for (SSDataBlock* pSource : sources) {
    blockDataDestroy(pSource);
  }

  const std::array<uint64_t, 2> expectedGroups = {42, 7};
  const std::array<int32_t, 2> expectedRows = {9, 7};
  for (size_t i = 0; i < expectedGroups.size(); ++i) {
    SSDataBlock* pOutput = nullptr;
    ASSERT_EQ(interpTestNextMainFirstOutput(pCtx, &pOutput),
              TSDB_CODE_SUCCESS);
    ASSERT_NE(pOutput, nullptr);
    EXPECT_EQ(pOutput->info.id.groupId, expectedGroups[i]);
    EXPECT_EQ(pOutput->info.id.baseGId, expectedGroups[i] * 10);
    EXPECT_EQ(pOutput->info.rows, expectedRows[i]);
    EXPECT_EQ(pOutput->info.version, outputVersion);
    EXPECT_EQ(pOutput->info.childId, outputChildId);
    EXPECT_EQ(pOutput->info.calWin.skey, outputCalStart);
    EXPECT_EQ(pOutput->info.calWin.ekey, outputCalEnd);
    EXPECT_EQ(pOutput->info.watermark, outputWatermark);
  }

  SSDataBlock* pOutput = nullptr;
  ASSERT_EQ(interpTestNextMainFirstOutput(pCtx, &pOutput),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pOutput, nullptr);
  interpTestDestroyMainFirstOutput(pCtx);
}

TEST(execUtilTest, interpResultStoreCopiesAndReadsGroupsSerially) {
  SInterpResultStore* pStore = nullptr;
  ASSERT_EQ(interpResultStoreCreate(&pStore), TSDB_CODE_SUCCESS);

  SSDataBlock* pGroup42First = nullptr;
  SSDataBlock* pGroup42Second = nullptr;
  SSDataBlock* pGroup7First = nullptr;
  SSDataBlock* pGroup7Second = nullptr;
  ASSERT_EQ(createInterpStoreTestBlock(42, 10, &pGroup42First),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreTestBlock(42, 20, &pGroup42Second),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreTestBlock(7, 30, &pGroup7First),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreTestBlock(7, 40, &pGroup7Second),
            TSDB_CODE_SUCCESS);

  int64_t group42Bytes = blockDataGetSize(pGroup42First) +
                         blockDataGetSize(pGroup42Second);
  int64_t group7Bytes = blockDataGetSize(pGroup7First) +
                        blockDataGetSize(pGroup7Second);
  ASSERT_EQ(interpResultStoreAppend(pStore, 42, pGroup42First),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(interpResultStoreAppend(pStore, 42, pGroup42Second),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(interpResultStoreAppend(pStore, 7, pGroup7First),
            TSDB_CODE_SUCCESS);

  SColumnInfoData* pSourceCol = static_cast<SColumnInfoData*>(
      taosArrayGet(pGroup42First->pDataBlock, 0));
  int32_t changed = 999;
  ASSERT_EQ(colDataSetVal(pSourceCol, 0,
                          reinterpret_cast<const char*>(&changed), false),
            TSDB_CODE_SUCCESS);

  ASSERT_EQ(interpResultStoreBytesUsed(pStore),
            group42Bytes + blockDataGetSize(pGroup7First));
  ASSERT_EQ(interpResultStoreOpenRead(pStore, 42), TSDB_CODE_SUCCESS);
  EXPECT_EQ(interpResultStoreOpenRead(pStore, 7),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(interpResultStoreAppend(pStore, 42, pGroup42First),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(interpResultStoreDropGroup(pStore, 42),
            TSDB_CODE_INVALID_PARA);

  SSDataBlock* pRead = nullptr;
  ASSERT_EQ(interpResultStoreNextBlock(pStore, &pRead),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(pRead, nullptr);
  EXPECT_EQ(interpStoreFirstValue(pRead), 10);

  ASSERT_EQ(interpResultStoreAppend(pStore, 7, pGroup7Second),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(interpStoreFirstValue(pRead), 10);

  ASSERT_EQ(interpResultStoreNextBlock(pStore, &pRead),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(pRead, nullptr);
  EXPECT_EQ(interpStoreFirstValue(pRead), 20);
  ASSERT_EQ(interpResultStoreNextBlock(pStore, &pRead),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pRead, nullptr);
  EXPECT_EQ(interpResultStoreDropGroup(pStore, 7),
            TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(interpResultStoreDropGroup(pStore, 42), TSDB_CODE_SUCCESS);
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), group7Bytes);

  ASSERT_EQ(interpResultStoreOpenRead(pStore, 7), TSDB_CODE_SUCCESS);
  for (int32_t expected : {30, 40}) {
    ASSERT_EQ(interpResultStoreNextBlock(pStore, &pRead),
              TSDB_CODE_SUCCESS);
    ASSERT_NE(pRead, nullptr);
    EXPECT_EQ(interpStoreFirstValue(pRead), expected);
  }
  ASSERT_EQ(interpResultStoreNextBlock(pStore, &pRead),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pRead, nullptr);
  ASSERT_EQ(interpResultStoreDropGroup(pStore, 7), TSDB_CODE_SUCCESS);
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), 0);

  blockDataDestroy(pGroup42First);
  blockDataDestroy(pGroup42Second);
  blockDataDestroy(pGroup7First);
  blockDataDestroy(pGroup7Second);
  interpResultStoreDestroy(pStore);
}

TEST(execUtilTest, interpResultStoreResetClearsReaderAndLogicalBytes) {
  SInterpResultStore* pStore = nullptr;
  SSDataBlock* pBlock = nullptr;
  ASSERT_EQ(interpResultStoreCreate(&pStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreTestBlock(UINT64_MAX, 50, &pBlock),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(interpResultStoreAppend(pStore, UINT64_MAX, pBlock),
            TSDB_CODE_SUCCESS);
  ASSERT_GT(interpResultStoreBytesUsed(pStore), 0);
  ASSERT_EQ(interpResultStoreOpenRead(pStore, UINT64_MAX),
            TSDB_CODE_SUCCESS);

  SSDataBlock* pRead = nullptr;
  ASSERT_EQ(interpResultStoreNextBlock(pStore, &pRead),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(pRead, nullptr);
  interpResultStoreReset(pStore);

  EXPECT_EQ(interpResultStoreBytesUsed(pStore), 0);
  EXPECT_EQ(interpResultStoreNextBlock(pStore, &pRead),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(interpResultStoreOpenRead(pStore, UINT64_MAX),
            TSDB_CODE_INVALID_PARA);

  blockDataDestroy(pBlock);
  interpResultStoreDestroy(pStore);
}

TEST(execUtilTest,
     interpResultStoreRejectsInvalidInputWithoutChangingStoredGroups) {
  SInterpResultStore* pStore = nullptr;
  SSDataBlock* pGoodBlock = nullptr;
  SSDataBlock* pMalformedBlock = nullptr;
  EXPECT_EQ(interpResultStoreCreate(nullptr), TSDB_CODE_INVALID_PARA);
  ASSERT_EQ(interpResultStoreCreate(&pStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreTestBlock(1, 10, &pGoodBlock),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreTestBlock(2, 20, &pMalformedBlock),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(interpResultStoreAppend(pStore, 1, pGoodBlock),
            TSDB_CODE_SUCCESS);
  int64_t logicalBytes = interpResultStoreBytesUsed(pStore);
  ASSERT_GT(logicalBytes, 0);

  EXPECT_EQ(interpResultStoreAppend(nullptr, 1, pGoodBlock),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(interpResultStoreAppend(pStore, 1, nullptr),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(interpResultStoreOpenRead(pStore, 3),
            TSDB_CODE_INVALID_PARA);
  SSDataBlock* pRead = nullptr;
  EXPECT_EQ(interpResultStoreNextBlock(pStore, &pRead),
            TSDB_CODE_INVALID_PARA);

  blockDataCleanup(pMalformedBlock);
  EXPECT_EQ(interpResultStoreAppend(pStore, 2, pMalformedBlock),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), logicalBytes);

  ASSERT_EQ(interpResultStoreOpenRead(pStore, 1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(interpResultStoreNextBlock(pStore, &pRead),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(pRead, nullptr);
  EXPECT_EQ(interpStoreFirstValue(pRead), 10);
  ASSERT_EQ(interpResultStoreNextBlock(pStore, &pRead),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(pRead, nullptr);
  ASSERT_EQ(interpResultStoreDropGroup(pStore, 1), TSDB_CODE_SUCCESS);
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), 0);

  blockDataDestroy(pGoodBlock);
  blockDataDestroy(pMalformedBlock);
  interpResultStoreDestroy(pStore);
  interpResultStoreReset(nullptr);
  interpResultStoreDestroy(nullptr);
  EXPECT_EQ(interpResultStoreBytesUsed(nullptr), 0);
}

TEST(execUtilTest, interpResultStoreAllocationFailuresPreserveState) {
  SInterpResultStore* pStore = nullptr;
  {
    Stub stub;
    stub.set(taosMemCalloc, failInterpStoreCalloc);
    EXPECT_EQ(interpResultStoreCreate(&pStore),
              TSDB_CODE_OUT_OF_MEMORY);
  }
  EXPECT_EQ(pStore, nullptr);

  SSDataBlock* pBlock = nullptr;
  ASSERT_EQ(interpResultStoreCreate(&pStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreTestBlock(1, 10, &pBlock),
            TSDB_CODE_SUCCESS);
  {
    Stub stub;
    stub.set(taosMemCalloc, failInterpStoreCalloc);
    EXPECT_EQ(interpResultStoreAppend(pStore, 1, pBlock),
              TSDB_CODE_OUT_OF_MEMORY);
  }
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), 0);
  EXPECT_EQ(interpResultStoreOpenRead(pStore, 1),
            TSDB_CODE_INVALID_PARA);

  ASSERT_EQ(interpResultStoreAppend(pStore, 1, pBlock),
            TSDB_CODE_SUCCESS);
  EXPECT_GT(interpResultStoreBytesUsed(pStore), 0);

  blockDataDestroy(pBlock);
  interpResultStoreDestroy(pStore);
}

TEST(execUtilTest, interpResultStorePagedRoundTripPreservesBlocks) {
  InterpMergeTempDirGuard tempDir;
  ASSERT_EQ(tempDir.code, TSDB_CODE_SUCCESS);

  SInterpResultStore* pStore = nullptr;
  ASSERT_EQ(interpResultStoreTestCreate(&pStore, 128, 256),
            TSDB_CODE_SUCCESS);

  const uint64_t firstGroup = UINT64_MAX - 9;
  const uint64_t secondGroup = 17;
  SSDataBlock* sources[3] = {nullptr, nullptr, nullptr};
  SSDataBlock* expected[3] = {nullptr, nullptr, nullptr};
  ASSERT_EQ(createInterpStoreVarBlock(firstGroup, 100, 12,
                                      &sources[0]),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreVarBlock(secondGroup, 200, 9,
                                      &sources[1]),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreVarBlock(firstGroup, 300, 15,
                                      &sources[2]),
            TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < 3; ++i) {
    ASSERT_EQ(createOneDataBlock(sources[i], true, &expected[i]),
              TSDB_CODE_SUCCESS);
    expected[i]->info.pks[0] = sources[i]->info.pks[0];
    expected[i]->info.pks[1] = sources[i]->info.pks[1];
  }

  ASSERT_EQ(interpResultStoreAppend(pStore, firstGroup, sources[0]),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(interpResultStoreAppend(pStore, secondGroup, sources[1]),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(interpResultStoreAppend(pStore, firstGroup, sources[2]),
            TSDB_CODE_SUCCESS);
  EXPECT_GT(interpResultStoreTestActivePages(pStore), 3);

  SColumnInfoData* pInt = static_cast<SColumnInfoData*>(
      taosArrayGet(sources[0]->pDataBlock, 0));
  SColumnInfoData* pStr = static_cast<SColumnInfoData*>(
      taosArrayGet(sources[0]->pDataBlock, 1));
  ASSERT_NE(pInt, nullptr);
  ASSERT_NE(pStr, nullptr);
  sources[0]->info.version = -1;
  sources[0]->info.pks[0].val = -1;
  pInt->info.slotId = -1;
  int32_t changed = -1;
  char changedStr[16] = {0};
  varDataSetLen(changedStr, 7);
  memcpy(varDataVal(changedStr), "changed", 7);
  ASSERT_EQ(colDataSetVal(pInt, 0,
                          reinterpret_cast<const char*>(&changed), false),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(colDataSetVal(pStr, 0, changedStr, false),
            TSDB_CODE_SUCCESS);

  ASSERT_NO_FATAL_FAILURE(
      expectInterpStoredGroup(pStore, firstGroup, {expected[0], expected[2]}));
  ASSERT_NO_FATAL_FAILURE(
      expectInterpStoredGroup(pStore, secondGroup, {expected[1]}));
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), 0);

  for (int32_t i = 0; i < 3; ++i) {
    blockDataDestroy(sources[i]);
    blockDataDestroy(expected[i]);
  }
  interpResultStoreDestroy(pStore);
}

TEST(execUtilTest,
     interpResultStoreSpillsWithinPagePayloadBudgetAndReusesPages) {
  InterpMergeTempDirGuard tempDir;
  ASSERT_EQ(tempDir.code, TSDB_CODE_SUCCESS);

  SInterpResultStore* pStore = nullptr;
  SSDataBlock* pFirst = nullptr;
  SSDataBlock* pSecond = nullptr;
  ASSERT_EQ(interpResultStoreTestCreate(&pStore, 128, 256),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreVarBlock(1, 400, 20, &pFirst),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreVarBlock(2, 500, 20, &pSecond),
            TSDB_CODE_SUCCESS);

  ASSERT_EQ(interpResultStoreAppend(pStore, 1, pFirst),
            TSDB_CODE_SUCCESS);
  EXPECT_GT(interpResultStoreTestActivePages(pStore), 2);
  EXPECT_GT(interpResultStoreResidentPagePayloadBytes(pStore), 0);
  EXPECT_LE(interpResultStoreResidentPagePayloadBytes(pStore), 256);
  EXPECT_GT(interpResultStoreSpillBytes(pStore), 0);
  int32_t registeredPages =
      interpResultStoreTestRegisteredPages(pStore);

  ASSERT_NO_FATAL_FAILURE(expectInterpStoredGroup(pStore, 1, {pFirst}));
  EXPECT_EQ(interpResultStoreTestActivePages(pStore), 0);
  EXPECT_EQ(interpResultStoreResidentPagePayloadBytes(pStore), 0);

  ASSERT_EQ(interpResultStoreAppend(pStore, 2, pSecond),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(interpResultStoreTestRegisteredPages(pStore),
            registeredPages);
  ASSERT_NO_FATAL_FAILURE(expectInterpStoredGroup(pStore, 2, {pSecond}));

  blockDataDestroy(pFirst);
  blockDataDestroy(pSecond);
  interpResultStoreDestroy(pStore);
}

TEST(execUtilTest, interpResultStoreRollsBackPagedAppendFailures) {
  InterpMergeTempDirGuard tempDir;
  ASSERT_EQ(tempDir.code, TSDB_CODE_SUCCESS);

  SInterpResultStore* pStore = nullptr;
  SSDataBlock* pGood = nullptr;
  SSDataBlock* pFailed = nullptr;
  ASSERT_EQ(interpResultStoreTestCreate(&pStore, 128, 256),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreVarBlock(1, 600, 12, &pGood),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreVarBlock(2, 700, 20, &pFailed),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(interpResultStoreAppend(pStore, 1, pGood),
            TSDB_CODE_SUCCESS);
  int64_t committedBytes = interpResultStoreBytesUsed(pStore);
  int32_t committedPages = interpResultStoreTestActivePages(pStore);

  interpResultStoreTestFailPageAllocAfter(pStore, 1);
  EXPECT_EQ(interpResultStoreAppend(pStore, 2, pFailed),
            TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), committedBytes);
  EXPECT_EQ(interpResultStoreTestActivePages(pStore), committedPages);
  EXPECT_EQ(interpResultStoreOpenRead(pStore, 2),
            TSDB_CODE_INVALID_PARA);

  interpResultStoreTestFailNextDirectoryCommit(pStore);
  EXPECT_EQ(interpResultStoreAppend(pStore, 3, pFailed),
            TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), committedBytes);
  EXPECT_EQ(interpResultStoreTestActivePages(pStore), committedPages);
  EXPECT_EQ(interpResultStoreOpenRead(pStore, 3),
            TSDB_CODE_INVALID_PARA);

  ASSERT_NO_FATAL_FAILURE(expectInterpStoredGroup(pStore, 1, {pGood}));

  interpResultStoreReset(pStore);
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), 0);
  EXPECT_EQ(interpResultStoreResidentPagePayloadBytes(pStore), 0);
  EXPECT_EQ(interpResultStoreSpillBytes(pStore), 0);
  EXPECT_EQ(interpResultStoreTestActivePages(pStore), 0);
  EXPECT_EQ(interpResultStoreTestRegisteredPages(pStore), 0);
  ASSERT_EQ(interpResultStoreAppend(pStore, 2, pFailed),
            TSDB_CODE_SUCCESS);

  blockDataDestroy(pGood);
  blockDataDestroy(pFailed);
  interpResultStoreDestroy(pStore);
}

TEST(execUtilTest, interpResultStoreSpillWriteFailurePreservesGroup) {
  InterpMergeTempDirGuard tempDir;
  ASSERT_EQ(tempDir.code, TSDB_CODE_SUCCESS);

  SInterpResultStore* pStore = nullptr;
  SSDataBlock* pGood = nullptr;
  SSDataBlock* pFailed = nullptr;
  ASSERT_EQ(interpResultStoreTestCreate(&pStore, 128, 256),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreVarBlock(1, 800, 20, &pGood),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreVarBlock(2, 900, 12, &pFailed),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(interpResultStoreAppend(pStore, 1, pGood),
            TSDB_CODE_SUCCESS);

  int64_t committedBytes = interpResultStoreBytesUsed(pStore);
  int64_t committedPayloadBytes =
      interpResultStoreResidentPagePayloadBytes(pStore);
  int64_t committedSpill = interpResultStoreSpillBytes(pStore);
  int32_t committedPages = interpResultStoreTestActivePages(pStore);
  int32_t committedRegisteredPages =
      interpResultStoreTestRegisteredPages(pStore);
  ASSERT_EQ(committedPayloadBytes, 256);

  interpResultStoreTestFailNextSpillWrite(pStore);
  EXPECT_EQ(interpResultStoreAppend(pStore, 2, pFailed),
            TSDB_CODE_APP_ERROR);
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), committedBytes);
  EXPECT_EQ(interpResultStoreResidentPagePayloadBytes(pStore),
            committedPayloadBytes);
  EXPECT_EQ(interpResultStoreSpillBytes(pStore), committedSpill);
  EXPECT_EQ(interpResultStoreTestActivePages(pStore),
            committedPages);
  EXPECT_EQ(interpResultStoreTestRegisteredPages(pStore),
            committedRegisteredPages);
  EXPECT_EQ(interpResultStoreOpenRead(pStore, 2),
            TSDB_CODE_INVALID_PARA);

  ASSERT_NO_FATAL_FAILURE(expectInterpStoredGroup(pStore, 1, {pGood}));
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), 0);
  EXPECT_EQ(interpResultStoreTestActivePages(pStore), 0);

  interpResultStoreReset(pStore);
  EXPECT_EQ(interpResultStoreResidentPagePayloadBytes(pStore), 0);
  EXPECT_EQ(interpResultStoreSpillBytes(pStore), 0);
  ASSERT_EQ(interpResultStoreAppend(pStore, 3, pFailed),
            TSDB_CODE_SUCCESS);

  blockDataDestroy(pGood);
  blockDataDestroy(pFailed);
  interpResultStoreDestroy(pStore);
}

TEST(execUtilTest, interpResultStoreRejectsPageCountOverflow) {
  const int32_t pageSize = 128;
  const int64_t maxBudget = (int64_t)INT32_MAX * pageSize;
  SSDataBlock* pBlock = nullptr;
  ASSERT_EQ(createInterpStoreTestBlock(1, 10, &pBlock),
            TSDB_CODE_SUCCESS);

  SInterpResultStore* pStore = nullptr;
  ASSERT_EQ(interpResultStoreTestCreate(&pStore, pageSize,
                                        maxBudget),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(interpResultStoreAppend(pStore, 1, pBlock),
            TSDB_CODE_SUCCESS);
  interpResultStoreDestroy(pStore);

  pStore = nullptr;
  ASSERT_EQ(interpResultStoreTestCreate(&pStore, pageSize,
                                        maxBudget + pageSize),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(interpResultStoreAppend(pStore, 1, pBlock),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), 0);

  blockDataDestroy(pBlock);
  interpResultStoreDestroy(pStore);
}

TEST(execUtilTest, interpResultStoreDeepCopiesVariablePksAcrossCleanup) {
  SInterpResultStore* pStore = nullptr;
  SSDataBlock* pSource = nullptr;
  SSDataBlock* pExpected = nullptr;
  ASSERT_EQ(interpResultStoreCreate(&pStore), TSDB_CODE_SUCCESS);
  ASSERT_EQ(createInterpStoreVarBlock(1, 1000, 4, &pSource),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(setInterpStoreVarPks(pSource, "first-key", "last-key"),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(createOneDataBlock(pSource, true, &pExpected),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(interpResultStoreAppend(pStore, 1, pSource),
            TSDB_CODE_SUCCESS);
  memset(pSource->info.pks[0].pData, 'x',
         pSource->info.pks[0].nData);
  blockDataDestroy(pSource);
  pSource = nullptr;

  ASSERT_NO_FATAL_FAILURE(expectInterpStoredGroup(pStore, 1, {pExpected}));

  ASSERT_EQ(createInterpStoreVarBlock(2, 1100, 4, &pSource),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(setInterpStoreVarPks(pSource, "reset-first", "reset-last"),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(interpResultStoreAppend(pStore, 2, pSource),
            TSDB_CODE_SUCCESS);
  blockDataDestroy(pSource);
  pSource = nullptr;
  interpResultStoreReset(pStore);
  EXPECT_EQ(interpResultStoreBytesUsed(pStore), 0);

  ASSERT_EQ(createInterpStoreVarBlock(3, 1200, 4, &pSource),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(setInterpStoreVarPks(pSource, "destroy-first",
                                 "destroy-last"),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(interpResultStoreAppend(pStore, 3, pSource),
            TSDB_CODE_SUCCESS);
  blockDataDestroy(pSource);
  blockDataDestroy(pExpected);
  interpResultStoreDestroy(pStore);
}

TEST(CorrelationBindTest, PrivatePhysicalPlanOwnsTypedValuesAndRejectsMissingParam) {
  SSubplan* subplan = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&subplan));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PROJECT, (SNode**)&subplan->pNode));
  auto* project = (SProjectPhysiNode*)subplan->pNode;

  int32_t number = 42;
  uint8_t text[VARSTR_HEADER_SIZE + 3] = {0};
  varDataSetLen(text, 3);
  memcpy(varDataVal(text), "a\0b", 3);
  SCorrelationParamDef defs[3] = {};
  SCorrelationWireValue values[3] = {};
  for (int32_t i = 0; i < 3; ++i) {
    defs[i].paramId = i;
    defs[i].dataType.type = i == 1 ? TSDB_DATA_TYPE_VARCHAR : TSDB_DATA_TYPE_INT;
    defs[i].dataType.bytes = i == 1 ? sizeof(text) : sizeof(number);
    values[i].paramId = i;
    values[i].dataType = defs[i].dataType;
    SCorrelationParamNode* param = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_CORRELATION_PARAM, (SNode**)&param));
    param->paramId = i;
    param->node.resType = defs[i].dataType;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&project->pProjections, (SNode*)param));
  }
  values[0].dataLen = sizeof(number);
  values[0].pData = (uint8_t*)&number;
  values[1].dataLen = sizeof(text);
  values[1].pData = text;
  values[2].isNull = true;
  SArray* pDefs = taosArrayInit(3, sizeof(SCorrelationParamDef));
  SArray* pValues = taosArrayInit(3, sizeof(SCorrelationWireValue));
  ASSERT_NE(nullptr, pDefs);
  ASSERT_NE(nullptr, pValues);
  for (int32_t i = 0; i < 3; ++i) {
    ASSERT_NE(nullptr, taosArrayPush(pDefs, &defs[i]));
    ASSERT_NE(nullptr, taosArrayPush(pValues, &values[i]));
  }

  ASSERT_EQ(TSDB_CODE_SUCCESS, qBindCorrelationSubplan(subplan, pDefs, pValues, nullptr));
  auto* boundInt = (SValueNode*)nodesListGetNode(project->pProjections, 0);
  auto* boundText = (SValueNode*)nodesListGetNode(project->pProjections, 1);
  auto* boundNull = (SValueNode*)nodesListGetNode(project->pProjections, 2);
  ASSERT_EQ(QUERY_NODE_VALUE, nodeType(boundInt));
  ASSERT_EQ(QUERY_NODE_VALUE, nodeType(boundText));
  ASSERT_EQ(QUERY_NODE_VALUE, nodeType(boundNull));
  EXPECT_EQ(42, boundInt->datum.i);
  EXPECT_EQ(0, memcmp(boundText->datum.p, text, sizeof(text)));
  EXPECT_TRUE(boundNull->isNull);
  memset(text, 0, sizeof(text));
  EXPECT_EQ(3, varDataLen(boundText->datum.p));
  EXPECT_EQ('a', varDataVal(boundText->datum.p)[0]);
  varDataSetLen(text, 3);
  memcpy(varDataVal(text), "a\0b", 3);

  SCorrelationParamNode* missing = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_CORRELATION_PARAM, (SNode**)&missing));
  missing->paramId = 99;
  missing->node.resType = defs[0].dataType;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&project->pProjections, (SNode*)missing));
  EXPECT_EQ(TSDB_CODE_QRY_INVALID_CORRELATION_NODE, qBindCorrelationSubplan(subplan, pDefs, pValues, nullptr));

  taosArrayDestroy(pDefs);
  taosArrayDestroy(pValues);
  nodesDestroyNode((SNode*)subplan);
}

TEST(CorrelationBindTest, DecodedParameterOutlivesRpcBufferDuringDelayedBind) {
  SSubplan* subplan = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&subplan));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PROJECT, (SNode**)&subplan->pNode));
  auto* project = (SProjectPhysiNode*)subplan->pNode;
  SCorrelationParamNode* param = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_CORRELATION_PARAM, (SNode**)&param));
  param->paramId = 0;
  param->node.resType.type = TSDB_DATA_TYPE_INT;
  param->node.resType.bytes = sizeof(int32_t);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&project->pProjections, (SNode*)param));

  int32_t original = 101;
  SCorrelationParamDef def = {};
  def.paramId = 0;
  def.dataType = param->node.resType;
  SCorrelationWireValue value = {};
  value.paramId = 0;
  value.dataType = def.dataType;
  value.dataLen = sizeof(original);
  value.pData = (uint8_t*)&original;
  SSubQueryMsg message = {};
  char planByte = 'x';
  message.queryId = 1;
  message.msg = &planByte;
  message.msgLen = 1;
  message.correlation.rootExecutionId = 1;
  message.correlation.rootQueryId = 1;
  message.correlation.executionGeneration = 1;
  strcpy(message.correlation.timezoneName, "UTC");
  strcpy(message.correlation.charsetName, "UTF-8");
  message.pCorrelationParamDefs = taosArrayInit(1, sizeof(def));
  message.pCorrelationParamValues = taosArrayInit(1, sizeof(value));
  ASSERT_NE(nullptr, message.pCorrelationParamDefs);
  ASSERT_NE(nullptr, message.pCorrelationParamValues);
  ASSERT_NE(nullptr, taosArrayPush(message.pCorrelationParamDefs, &def));
  ASSERT_NE(nullptr, taosArrayPush(message.pCorrelationParamValues, &value));
  int32_t length = tSerializeSSubQueryMsg(nullptr, 0, &message);
  ASSERT_GT(length, 0);
  std::vector<uint8_t> wire(length);
  ASSERT_EQ(length, tSerializeSSubQueryMsg(wire.data(), length, &message));

  SSubQueryMsg received = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSSubQueryMsg(wire.data(), length, &received));
  taosArrayDestroy(message.pCorrelationParamDefs);
  taosArrayDestroy(message.pCorrelationParamValues);
  std::vector<uint8_t>().swap(wire);
  original = 0;

  int32_t bindCode = TSDB_CODE_SUCCESS;
  std::thread consumer([&] {
    bindCode = qBindCorrelationSubplan(subplan, received.pCorrelationParamDefs, received.pCorrelationParamValues,
                                      nullptr);
  });
  consumer.join();
  ASSERT_EQ(TSDB_CODE_SUCCESS, bindCode);
  auto* bound = (SValueNode*)nodesListGetNode(project->pProjections, 0);
  ASSERT_EQ(QUERY_NODE_VALUE, nodeType(bound));
  EXPECT_EQ(101, bound->datum.i);
  tFreeSSubQueryMsg(&received);
  nodesDestroyNode((SNode*)subplan);
}
