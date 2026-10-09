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

#include "executorInt.h"
#include "filter.h"
#include "function.h"
#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"
#include "tuuid.h"

typedef struct SFetchRspHandleWrapper {
  int64_t  exchangeId;
  int32_t  sourceIndex;
  int64_t  seqId;
} SFetchRspHandleWrapper;

// vtable two-pass: cap the empty-rsp retry rounds so a wedged phase-2 rescan (remote
// cquery stuck, sink never filled, never completed) fails with a clear error instead of
// retrying forever. At the 50ms backoff cap this bounds the wait to ~30s.
#define EX_TWO_PASS_MAX_RETRY_ROUNDS 600

typedef struct SSourceDataInfo {
  int32_t             index;
  int64_t             seqId;
  SRWLatch            lock;
  SRetrieveTableRsp*  pRsp;
  int32_t             rspLen;
  uint64_t            totalRows;
  int64_t             startTime;
  int32_t             code;
  EX_SOURCE_STATUS    status;
  const char*         taskId;
  SArray*             pSrcUidList;
  int32_t             srcOpType;
  bool                tableSeq;
  char*               decompBuf;
  int32_t             decompBufSize;
  char                extErrMsg[512]; // detail text carried back from the source task's failed fetch response
  SOrgTbInfo*         orgTbInfo;
  SArray*             batchOrgTbInfo; // SArray<SOrgTbInfo>
  SArray*             tagList;
  SArray*             sysScanReqs; // SArray<SSysTableScanVtbRefReq>
  EExchangeSourceType type;
  bool                isNewParam;
  STimeWindow         window;
  uint64_t            groupid;
  uint64_t            readerFetchId;
  bool                fetchSent; // need reset
  bool                currentEogSeen;  // strict Runner fetch current-group barrier state
  uint64_t            currentEogGid;
  bool                runnerEogBatch;
  SArray*             pRunnerEogs;  // owned tail, consumed only after returning the leading DATA
  int32_t             runnerEogIdx;
  SArray*             pReaderGroups;  // owned uint64_t actual-source gids, unsigned sorted
  SArray*             pReaderItems;   // owns decoded DATA until transferred to result queue
  int32_t             readerItemIdx;
  int32_t             readerValidatedIdx;  // next expected EOG in the received stream
  int32_t             readerConsumedIdx;   // next expected EOG in the consumed stream
  int32_t             readerMode;
  bool                readerEof;    // may coexist with unread items
  uint64_t            fetchTimes;   // per-source fetch count
  int64_t             fetchCostUs;  // per-source total RPC round-trip (us)
} SSourceDataInfo;

typedef enum EExchangeFetchResultType {
  EXCHANGE_FETCH_RESULT_NONE = 0,
  EXCHANGE_FETCH_RESULT_DATA,
  EXCHANGE_FETCH_RESULT_EOG,
  EXCHANGE_FETCH_RESULT_EOF,
} EExchangeFetchResultType;

static void destroyExchangeOperatorInfo(void* param);
static void freeBlock(void* pParam);
static void freeSourceDataInfo(void* param);
static void setAllSourcesCompleted(SOperatorInfo* pOperator);
static void clearSourceResponse(SSourceDataInfo* pDataInfo);
static void destroyExchangeMultiGroupResult(void* pParam);

static int32_t loadRemoteDataCallback(void* param, SDataBuf* pMsg, int32_t code);

// Vtable data-source fetch types. EX_SRC_TYPE_VSTB_SYS_SCAN is deliberately
// excluded: it is the ins_vc_cols / requested-column meta read issued by the
// dynamic query control operator, whose failures are resolution-level and are
// already upgraded to the dedicated codes server-side in
// sysTableScanFillRequestedVirtualTableCol.
static bool exIsVtbDataSource(EExchangeSourceType type) {
  switch (type) {
    case EX_SRC_TYPE_VSTB_SCAN:
    case EX_SRC_TYPE_VSTB_WIN_SCAN:
    case EX_SRC_TYPE_VSTB_AGG_SCAN:
    case EX_SRC_TYPE_VSTB_TAG_SCAN:
    case EX_SRC_TYPE_VTB_WIN_SCAN:
    case EX_SRC_TYPE_VSTB_TS_SCAN:
    case EX_SRC_TYPE_VSTB_INTERVAL_SCAN:
    case EX_SRC_TYPE_VSTB_PART_INTERVAL_SCAN:
      return true;
    default:
      return false;
  }
}

// A failed virtual-table source fetch is by construction a dependency failure
// (these sources exist only to serve vtable refs): upgrade the stock
// table/column-not-found codes to the dedicated vtable codes so clients can
// tell a broken vtable dependency from a plain missing table. The failing
// source is also formatted into extErrMsg (unless the source task already
// supplied a more precise detail text) so the client-visible error names the
// missing db.table; SHOW VTABLE VALIDATE gives the full picture.
static int32_t exUpgradeVtbSourceCode(SSourceDataInfo* pDataInfo, int32_t errCode) {
  int32_t code = errCode;
  if (code == TSDB_CODE_SUCCESS || !exIsVtbDataSource(pDataInfo->type)) {
    return code;
  }
  int32_t upgraded = code;
  switch (code) {
    case TSDB_CODE_TDB_TABLE_NOT_EXIST:
    case TSDB_CODE_PAR_TABLE_NOT_EXIST:
      upgraded = TSDB_CODE_VTABLE_SRC_TABLE_NOT_EXIST;
      break;
    case TSDB_CODE_PAR_INVALID_COLUMN:
    case TSDB_CODE_NOT_FOUND:
      upgraded = TSDB_CODE_VTABLE_SRC_COL_NOT_FOUND;
      break;
    default:
      return code;
  }
  const char* pTbName = NULL;
  int32_t     batchSize = 0;
  if (pDataInfo->orgTbInfo != NULL) {
    pTbName = pDataInfo->orgTbInfo->tbName;
  } else if (pDataInfo->batchOrgTbInfo != NULL && taosArrayGetSize(pDataInfo->batchOrgTbInfo) > 0) {
    SOrgTbInfo* pFirst = taosArrayGet(pDataInfo->batchOrgTbInfo, 0);
    pTbName = pFirst->tbName;
    batchSize = (int32_t)taosArrayGetSize(pDataInfo->batchOrgTbInfo);
  }
  if (pTbName != NULL && batchSize <= 1) {
    qError("virtual table source scan failed, source table %s, code:%s -> %s; run SHOW VTABLE VALIDATE for details",
           pTbName, tstrerror(code), tstrerror(upgraded));
  } else if (pTbName != NULL) {
    // one consolidated line: a batch failure hits the whole array, and per-table lines
    // would flood the log with duplicates and drown other messages
    qError("virtual table source scan failed, first failing source table %s of %d batched tables, code:%s -> %s; "
           "run SHOW VTABLE VALIDATE for details",
           pTbName, batchSize, tstrerror(code), tstrerror(upgraded));
  } else {
    qError("virtual table source scan failed, code:%s -> %s; run SHOW VTABLE VALIDATE for details", tstrerror(code),
           tstrerror(upgraded));
  }
  if (pTbName != NULL && pDataInfo->extErrMsg[0] == '\0') {
    // the column name is only known at the fill layer; here name the source table
    if (upgraded == TSDB_CODE_VTABLE_SRC_TABLE_NOT_EXIST) {
      (void)snprintf(pDataInfo->extErrMsg, sizeof(pDataInfo->extErrMsg), "%s: %s", tstrerror(upgraded), pTbName);
    } else {
      (void)snprintf(pDataInfo->extErrMsg, sizeof(pDataInfo->extErrMsg), "%s (source table %s)", tstrerror(upgraded),
                     pTbName);
    }
  }
  return upgraded;
}

// The source-task detail text (when present) is the client-facing error: copy it
// onto the task so qworker can attach it to the query/fetch response.
static void copySourceExtErrMsg(SExecTaskInfo* pTaskInfo, const SSourceDataInfo* pDataInfo) {
  if (pDataInfo->extErrMsg[0] != '\0' && pTaskInfo->extErrMsg[0] == '\0') {
    tstrncpy(pTaskInfo->extErrMsg, pDataInfo->extErrMsg, sizeof(pTaskInfo->extErrMsg));
  }
}
static int32_t doSendFetchDataRequest(SExchangeInfo* pExchangeInfo, SExecTaskInfo* pTaskInfo, int32_t sourceIndex);

static int32_t getCompletedSources(const SArray* pArray, int32_t* pRes);
static int32_t prepareConcurrentlyLoad(SOperatorInfo* pOperator);
int32_t        storeExchangeNotifyInfo(SOperatorInfo* pOperator);
static int32_t seqLoadRemoteData(SOperatorInfo* pOperator);
static int32_t prepareLoadRemoteData(SOperatorInfo* pOperator);
static int32_t handleLimitOffset(SOperatorInfo* pOperator, SLimitInfo* pLimitInfo, SSDataBlock* pBlock,
                                 bool holdDataInBuf);
static int32_t doExtractResultBlocks(SExchangeInfo* pExchangeInfo, SSourceDataInfo* pDataInfo,
                                     SStreamRuntimeInfo* pStreamRuntimeInfo);
static int32_t decodeStrictMultiGroupFetchResponse(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo,
                                                   SSourceDataInfo* pDataInfo, int32_t sourceIndex,
                                                   EExchangeFetchResultType* pResultType, bool* pOutputQueued);

static int32_t exchangeWait(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo);
static int32_t initReaderGroupSources(SOperatorInfo* pOperator);
static int32_t consumeReaderGroups(SOperatorInfo* pOperator);
static int32_t acceptReaderFetchResponse(SOperatorInfo* pOperator, SSourceDataInfo* pDataInfo);
static int32_t loadReaderGroupData(SOperatorInfo* pOperator);
static int32_t adoptReaderBlockDescriptor(SSDataBlock* pBlock, const SSDataBlock* pTemplate);

static bool isVstbScan(SSourceDataInfo* pDataInfo) {return pDataInfo->type == EX_SRC_TYPE_VSTB_SCAN; }
static bool isVstbWinScan(SSourceDataInfo* pDataInfo) { return pDataInfo->type == EX_SRC_TYPE_VSTB_WIN_SCAN; }
static bool isVstbAggScan(SSourceDataInfo* pDataInfo) { return pDataInfo->type == EX_SRC_TYPE_VSTB_AGG_SCAN; }
static bool isVstbTagScan(SSourceDataInfo* pDataInfo) { return pDataInfo->type == EX_SRC_TYPE_VSTB_TAG_SCAN; }
static bool isStbJoinScan(SSourceDataInfo* pDataInfo) { return pDataInfo->type == EX_SRC_TYPE_STB_JOIN_SCAN; }

static bool isStrictRunnerFetchType(int8_t type) {
  return type == STREAM_RUNNER_FETCH_DATA || type == STREAM_RUNNER_FETCH_EOG || type == STREAM_RUNNER_FETCH_BATCH;
}

static bool hasRunnerFetchFrame(int8_t type) { return type != STREAM_RUNNER_FETCH_LEGACY; }

static bool isStrictMultiGroupRunnerSource(const SOperatorInfo* pOperator, int32_t sourceIndex) {
  const SStreamRuntimeFuncInfo* pStream =
      pOperator == NULL || pOperator->pTaskInfo == NULL ? NULL : GET_STM_RTINFO(pOperator->pTaskInfo);
  if (pOperator == NULL || pOperator->info == NULL || pOperator->pTaskInfo == NULL ||
      pOperator->pTaskInfo->execModel != OPTR_EXEC_MODEL_STREAM || pOperator->pTaskInfo->pStreamRuntimeInfo == NULL ||
      pStream == NULL || !pStream->isMultiGroupCalc || !isStaticMultiGroupJoinQualified(pOperator)) {
    return false;
  }

  const SExchangeInfo*         pExchangeInfo = pOperator->info;
  const SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, sourceIndex);
  return pSource != NULL && pSource->fetchMsgType == TDMT_STREAM_FETCH_FROM_RUNNER;
}

static int32_t normalizeFetchResponseHeader(SRetrieveTableRsp* pRsp, int32_t rspLen) {
  if (pRsp == NULL || rspLen < (int32_t)sizeof(SRetrieveTableRsp)) return TSDB_CODE_INVALID_MSG;

  pRsp->numOfRows = be64toh((uint64_t)pRsp->numOfRows);
  pRsp->compLen = ntohl((uint32_t)pRsp->compLen);
  pRsp->payloadLen = ntohl((uint32_t)pRsp->payloadLen);
  pRsp->useconds = be64toh((uint64_t)pRsp->useconds);
  pRsp->numOfBlocks = ntohl((uint32_t)pRsp->numOfBlocks);
  if (isStrictRunnerFetchType(pRsp->streamBlockType)) {
    pRsp->numOfCols = be64toh((uint64_t)pRsp->numOfCols);
  } else {
    pRsp->numOfCols = (int64_t)ntohl((uint32_t)pRsp->numOfCols);
  }

  int32_t bodyLen = rspLen - (int32_t)sizeof(SRetrieveTableRsp);
  if (pRsp->payloadLen < 0 || pRsp->compLen < 0 || pRsp->compLen > bodyLen || pRsp->numOfBlocks < 0 ||
      pRsp->numOfRows < 0 || pRsp->numOfCols < 0 ||
      ((isStrictRunnerFetchType(pRsp->streamBlockType) || pRsp->streamBlockType == STREAM_READER_FETCH_BATCH) &&
       pRsp->payloadLen > bodyLen)) {
    return TSDB_CODE_INVALID_MSG;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getLocalFetchResponseLen(const void* pData, uint32_t* pRspLen) {
  if (pData == NULL || pRspLen == NULL) return TSDB_CODE_INVALID_PARA;

  int64_t allocationSize = taosMemorySize((void*)pData);
  if (allocationSize < (int64_t)sizeof(SRetrieveTableRsp)) return TSDB_CODE_INVALID_MSG;

  const SRetrieveTableRsp* pRsp = pData;
  uint32_t                 bodyLen = ntohl((uint32_t)pRsp->compLen);
  int64_t                  responseLen = (int64_t)sizeof(SRetrieveTableRsp) + bodyLen;
  if (responseLen > allocationSize || responseLen > INT32_MAX) return TSDB_CODE_INVALID_MSG;
  *pRspLen = (uint32_t)responseLen;
  return TSDB_CODE_SUCCESS;
}

static void clearSourceResponse(SSourceDataInfo* pDataInfo) {
  if (pDataInfo == NULL) return;
  taosMemoryFreeClear(pDataInfo->pRsp);
  pDataInfo->rspLen = 0;
}

static void destroyExchangeMultiGroupResult(void* pParam) {
  SExchangeMultiGroupResult* pResult = pParam;
  if (pResult == NULL) return;
  blockDataDestroy(pResult->pBlock);
  taosArrayDestroy(pResult->pLineage);
  memset(pResult, 0, sizeof(*pResult));
}

static void clearPublishedLineage(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo) {
  if (pExchangeInfo->pPublishedLineage == NULL) return;

  SStreamRuntimeFuncInfo* pRuntime = pExchangeInfo->pPublishedStream;
  if (pRuntime == NULL && pOperator != NULL && pOperator->pTaskInfo != NULL &&
      pOperator->pTaskInfo->pStreamRuntimeInfo != NULL) {
    pRuntime = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
  }
  if (pRuntime != NULL && pRuntime->pStreamBlkWinIdx == pExchangeInfo->pPublishedLineage) {
    pRuntime->pStreamBlkWinIdx = NULL;
  }
  taosArrayDestroy(pExchangeInfo->pPublishedLineage);
  pExchangeInfo->pPublishedLineage = NULL;
  pExchangeInfo->pPublishedStream = NULL;
}

static void clearReaderSourceState(SSourceDataInfo* pSource) {
  taosArrayDestroy(pSource->pReaderGroups);
  taosArrayDestroyEx(pSource->pReaderItems, tDestroyStreamReaderFetchItem);
  pSource->pReaderGroups = NULL;
  pSource->pReaderItems = NULL;
  pSource->readerItemIdx = 0;
  pSource->readerValidatedIdx = 0;
  pSource->readerConsumedIdx = 0;
  pSource->readerMode = 0;
  pSource->readerEof = false;
}

static void clearRunnerSourceState(SSourceDataInfo* pSource) {
  taosArrayDestroy(pSource->pRunnerEogs);
  pSource->pRunnerEogs = NULL;
  pSource->runnerEogIdx = 0;
  pSource->runnerEogBatch = false;
}

static void clearExchangeResultState(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo) {
  for (int32_t i = 0; i < taosArrayGetSize(pExchangeInfo->pSourceDataInfo); ++i) {
    SSourceDataInfo* pSource = taosArrayGet(pExchangeInfo->pSourceDataInfo, i);
    clearReaderSourceState(pSource);
    clearRunnerSourceState(pSource);
  }
  pExchangeInfo->readerGroupsInitialized = false;
  pExchangeInfo->readerGroupMode = pExchangeInfo->readerGroupRequested ? 0 : 1;
  pExchangeInfo->readerNextGroupIdx = 0;
  clearPublishedLineage(pOperator, pExchangeInfo);
  taosArrayClearEx(pExchangeInfo->pMultiGroupResultList, destroyExchangeMultiGroupResult);
  taosArrayClearEx(pExchangeInfo->pResultBlockList, freeBlock);
  taosArrayClearEx(pExchangeInfo->pRecycledBlocks, freeBlock);
}

static int32_t acquireExchangeResultBlock(SExchangeInfo* pExchangeInfo, SSDataBlock** ppBlock) {
  *ppBlock = NULL;
  if (taosArrayGetSize(pExchangeInfo->pRecycledBlocks) > 0) {
    *ppBlock = *(SSDataBlock**)taosArrayPop(pExchangeInfo->pRecycledBlocks);
    blockDataCleanup(*ppBlock);
    return TSDB_CODE_SUCCESS;
  }
  return createOneDataBlock(pExchangeInfo->pDummyBlock, false, ppBlock);
}

static int32_t enqueueExchangeMultiGroupResult(SExchangeInfo* pExchangeInfo, SExchangeMultiGroupResult* pResult) {
  if (pExchangeInfo->pMultiGroupResultList == NULL) {
    pExchangeInfo->pMultiGroupResultList = taosArrayInit(16, sizeof(SExchangeMultiGroupResult));
    if (pExchangeInfo->pMultiGroupResultList == NULL) return terrno;
  }
  if (taosArrayPush(pExchangeInfo->pMultiGroupResultList, pResult) == NULL) return terrno;
  memset(pResult, 0, sizeof(*pResult));
  return TSDB_CODE_SUCCESS;
}

static void resetStrictMultiGroupBarrier(SExchangeInfo* pExchangeInfo) {
  pExchangeInfo->nextGroupIdx = 0;
  pExchangeInfo->eogSourceCount = 0;
  pExchangeInfo->strictSourcesValidated = false;
  for (int32_t i = 0; i < taosArrayGetSize(pExchangeInfo->pSourceDataInfo); ++i) {
    SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, i);
    if (pDataInfo == NULL) continue;
    pDataInfo->currentEogSeen = false;
    pDataInfo->currentEogGid = 0;
  }
}

static bool canPollStrictMultiGroupSource(const SExchangeInfo* pExchangeInfo, int32_t sourceIndex) {
  const SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, sourceIndex);
  return pDataInfo != NULL && pDataInfo->status != EX_SOURCE_DATA_EXHAUSTED && !pDataInfo->currentEogSeen &&
         pDataInfo->pRunnerEogs == NULL;
}

static int32_t getStrictCanonicalGroup(const SOperatorInfo* pOperator, const SExchangeInfo* pExchangeInfo,
                                       uint64_t* pGroupId, int32_t* pGroupCount) {
  if (pOperator == NULL || pOperator->pTaskInfo == NULL || pOperator->pTaskInfo->pStreamRuntimeInfo == NULL ||
      pGroupId == NULL || pGroupCount == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }

  const SArray* pGroupOrder = pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcOrder;
  int32_t       groupCount = taosArrayGetSize(pGroupOrder);
  if (pGroupOrder == NULL || groupCount <= 0 || pExchangeInfo->nextGroupIdx < 0 ||
      pExchangeInfo->nextGroupIdx >= groupCount) {
    return TSDB_CODE_INVALID_MSG;
  }

  uint64_t* pExpectedGroup = taosArrayGet(pGroupOrder, pExchangeInfo->nextGroupIdx);
  if (pExpectedGroup == NULL || *pExpectedGroup == 0) return TSDB_CODE_INVALID_MSG;
  *pGroupId = *pExpectedGroup;
  *pGroupCount = groupCount;
  return TSDB_CODE_SUCCESS;
}

static int32_t acceptStrictMultiGroupFetchResult(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo,
                                                 int32_t sourceIndex, EExchangeFetchResultType resultType,
                                                 SExchangeMultiGroupResult* pResult, bool* pOutputQueued) {
  *pOutputQueued = false;
  int32_t totalSources = taosArrayGetSize(pExchangeInfo->pSourceDataInfo);
  if (totalSources <= 0 || taosArrayGetSize(pExchangeInfo->pSources) != totalSources || sourceIndex < 0 ||
      sourceIndex >= totalSources) {
    return TSDB_CODE_INVALID_MSG;
  }
  if (!pExchangeInfo->strictSourcesValidated) {
    for (int32_t i = 0; i < totalSources; ++i) {
      if (!isStrictMultiGroupRunnerSource(pOperator, i)) return TSDB_CODE_INVALID_MSG;
    }
    pExchangeInfo->strictSourcesValidated = true;
  }

  SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, sourceIndex);
  if (pDataInfo == NULL || pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) return TSDB_CODE_INVALID_MSG;

  const SArray* pGroupOrder = pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.pGroupCalcOrder;
  int32_t       groupCount = taosArrayGetSize(pGroupOrder);
  if (pGroupOrder == NULL || groupCount <= 0 || pExchangeInfo->nextGroupIdx < 0 ||
      pExchangeInfo->nextGroupIdx > groupCount || pExchangeInfo->eogSourceCount < 0 ||
      pExchangeInfo->eogSourceCount > totalSources) {
    return TSDB_CODE_INVALID_MSG;
  }

  if (resultType == EXCHANGE_FETCH_RESULT_EOF) {
    if (pExchangeInfo->nextGroupIdx != groupCount || pExchangeInfo->eogSourceCount != 0) {
      return TSDB_CODE_INVALID_MSG;
    }
    pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
    return TSDB_CODE_SUCCESS;
  }

  uint64_t expectedGroup = 0;
  int32_t  checkedGroupCount = 0;
  int32_t  code = getStrictCanonicalGroup(pOperator, pExchangeInfo, &expectedGroup, &checkedGroupCount);
  if (code != TSDB_CODE_SUCCESS || checkedGroupCount != groupCount || pResult == NULL || pDataInfo->currentEogSeen ||
      pResult->baseGId != expectedGroup) {
    return TSDB_CODE_INVALID_MSG;
  }

  if (resultType == EXCHANGE_FETCH_RESULT_DATA) {
    if (pResult->type != STREAM_RUNNER_FETCH_DATA || pResult->pBlock == NULL) return TSDB_CODE_INVALID_MSG;
    code = enqueueExchangeMultiGroupResult(pExchangeInfo, pResult);
    if (code == TSDB_CODE_SUCCESS) *pOutputQueued = true;
    return code;
  }
  if (resultType != EXCHANGE_FETCH_RESULT_EOG || pResult->type != STREAM_RUNNER_FETCH_EOG) {
    return TSDB_CODE_INVALID_MSG;
  }

  pDataInfo->currentEogSeen = true;
  pDataInfo->currentEogGid = expectedGroup;
  ++pExchangeInfo->eogSourceCount;
  if (pExchangeInfo->eogSourceCount < totalSources) return TSDB_CODE_SUCCESS;
  if (pExchangeInfo->eogSourceCount != totalSources) return TSDB_CODE_INVALID_MSG;

  code = acquireExchangeResultBlock(pExchangeInfo, &pResult->pBlock);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = qSetMultiGroupEogBlock(pResult->pBlock, expectedGroup);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = enqueueExchangeMultiGroupResult(pExchangeInfo, pResult);
  if (code != TSDB_CODE_SUCCESS) return code;

  ++pExchangeInfo->nextGroupIdx;
  pExchangeInfo->eogSourceCount = 0;
  for (int32_t i = 0; i < totalSources; ++i) {
    SSourceDataInfo* pSourceInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, i);
    if (pSourceInfo == NULL || !pSourceInfo->currentEogSeen || pSourceInfo->currentEogGid != expectedGroup) {
      return TSDB_CODE_INVALID_MSG;
    }
    pSourceInfo->currentEogSeen = false;
    pSourceInfo->currentEogGid = 0;
  }
  *pOutputQueued = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t consumeStrictMultiGroupEog(SOperatorInfo* pOperator, int32_t sourceIndex, bool* pOutputQueued) {
  SExchangeInfo*   pExchangeInfo = pOperator->info;
  SSourceDataInfo* pSource = taosArrayGet(pExchangeInfo->pSourceDataInfo, sourceIndex);
  const uint64_t*  pGid = taosArrayGet(pSource->pRunnerEogs, pSource->runnerEogIdx);
  if (pGid == NULL) return TSDB_CODE_INVALID_MSG;

  SExchangeMultiGroupResult result = {.type = STREAM_RUNNER_FETCH_EOG, .baseGId = *pGid};
  int32_t code = acceptStrictMultiGroupFetchResult(pOperator, pExchangeInfo, sourceIndex, EXCHANGE_FETCH_RESULT_EOG,
                                                   &result, pOutputQueued);
  destroyExchangeMultiGroupResult(&result);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (++pSource->runnerEogIdx == taosArrayGetSize(pSource->pRunnerEogs)) {
    taosArrayDestroy(pSource->pRunnerEogs);
    pSource->pRunnerEogs = NULL;
    pSource->runnerEogIdx = 0;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t popExchangeMultiGroupResult(SOperatorInfo* pOperator, SSDataBlock** ppBlock, bool* pPopped) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  *ppBlock = NULL;
  *pPopped = false;
  if (taosArrayGetSize(pExchangeInfo->pMultiGroupResultList) == 0) return TSDB_CODE_SUCCESS;

  // The previously returned block expires at the next getNext/pop. Batch DATA owns a fresh decoded block.
  taosArrayClearEx(pExchangeInfo->pRecycledBlocks, freeBlock);
  SExchangeMultiGroupResult* pQueued = taosArrayGet(pExchangeInfo->pMultiGroupResultList, 0);
  if (pQueued == NULL || pQueued->pBlock == NULL) return TSDB_CODE_INVALID_MSG;
  SExchangeMultiGroupResult result = *pQueued;
  memset(pQueued, 0, sizeof(*pQueued));
  taosArrayRemove(pExchangeInfo->pMultiGroupResultList, 0);

  if (taosArrayPush(pExchangeInfo->pRecycledBlocks, &result.pBlock) == NULL) {
    int32_t code = terrno;
    destroyExchangeMultiGroupResult(&result);
    clearExchangeResultState(pOperator, pExchangeInfo);
    resetStrictMultiGroupBarrier(pExchangeInfo);
    return code;
  }
  *ppBlock = result.pBlock;
  result.pBlock = NULL;

  clearPublishedLineage(pOperator, pExchangeInfo);
  if (result.type == STREAM_RUNNER_FETCH_DATA) {
    pExchangeInfo->pPublishedLineage = result.pLineage;
    result.pLineage = NULL;
    pExchangeInfo->pPublishedStream = &pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo;
    pExchangeInfo->pPublishedStream->pStreamBlkWinIdx = pExchangeInfo->pPublishedLineage;
  } else if (result.type != STREAM_RUNNER_FETCH_EOG && result.type != STREAM_READER_FETCH_BATCH) {
    destroyExchangeMultiGroupResult(&result);
    return TSDB_CODE_INVALID_MSG;
  }

  destroyExchangeMultiGroupResult(&result);
  *pPopped = true;
  return TSDB_CODE_SUCCESS;
}

static void streamSequenciallyLoadRemoteData(SOperatorInfo* pOperator,
                                             SExchangeInfo* pExchangeInfo,
                                             SExecTaskInfo* pTaskInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t startTs = taosGetTimestampUs();  
  int32_t  totalSources = (int32_t)taosArrayGetSize(pExchangeInfo->pSourceDataInfo);
  int32_t completed = 0;
  SSourceDataInfo* pDataInfo = NULL;
  code = getCompletedSources(pExchangeInfo->pSourceDataInfo, &completed);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }
  if (completed == totalSources) {
    qDebug("%s no load since all sources completed, completed:%d, totalSources:%d", pTaskInfo->id.str, completed, totalSources);
    setAllSourcesCompleted(pOperator);
    return;
  }

  SStreamRuntimeFuncInfo* pStream = &pTaskInfo->pStreamRuntimeInfo->funcInfo;

  while (1) {
    if (pExchangeInfo->current < 0) {
      qDebug("current %d and all sources completed, totalSources:%d", pExchangeInfo->current, totalSources);
      setAllSourcesCompleted(pOperator);
      return;
    }
    
    if (pExchangeInfo->current >= totalSources) {
      completed = 0;
      code = getCompletedSources(pExchangeInfo->pSourceDataInfo, &completed);
      if (code != TSDB_CODE_SUCCESS) {
        goto _exit;
      }
      if (completed == totalSources) {
        qDebug("stop to load since all sources completed, completed:%d, totalSources:%d", completed, totalSources);
        setAllSourcesCompleted(pOperator);
        return;
      }
      
      pExchangeInfo->current = 0;
    }

    qDebug("%s start stream exchange %p idx:%d fetch", GET_TASKID(pTaskInfo), pExchangeInfo, pExchangeInfo->current);

    pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pExchangeInfo->current);
    if (!pDataInfo) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      code = terrno;
      goto _exit;
    }

    if (pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) {
      pExchangeInfo->current++;
      continue;
    }
    pDataInfo->runnerEogBatch = isStrictMultiGroupRunnerSource(pOperator, pExchangeInfo->current);
    if (pDataInfo->runnerEogBatch) {
      if (pDataInfo->currentEogSeen) {
        pExchangeInfo->current++;
        continue;
      }
      if (pDataInfo->pRunnerEogs != NULL) {
        bool outputQueued = false;
        TAOS_CHECK_EXIT(consumeStrictMultiGroupEog(pOperator, pExchangeInfo->current, &outputQueued));
        pExchangeInfo->current++;
        if (outputQueued) return;
        continue;
      }
    }

    pDataInfo->status = EX_SOURCE_DATA_NOT_READY;

    code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, pExchangeInfo->current);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      goto _exit;
    }
    if (pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) {
      pExchangeInfo->current++;
      continue;
    }

    while (true) {
      recordOpExecBeforeDownstream(pOperator);
      code = exchangeWait(pOperator, pExchangeInfo);
      recordOpExecAfterDownstream(pOperator, 0);

      if (code != TSDB_CODE_SUCCESS || isTaskKilled(pTaskInfo)) {
        if (code == TSDB_CODE_SUCCESS) code = pTaskInfo->code;
        goto _exit;
      }

      int64_t currSeqId = atomic_load_64(&pExchangeInfo->seqId);
      if (pDataInfo->seqId != currSeqId) {
        qDebug("%s seq rsp reqId %" PRId64 " mismatch with exchange %p curr seqId %" PRId64 ", ignore it", 
            GET_TASKID(pTaskInfo), pDataInfo->seqId, pExchangeInfo, currSeqId);
        clearSourceResponse(pDataInfo);
        continue;
      }

      break;
    }

    SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pExchangeInfo->current);
    if (!pSource) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      code = terrno;
      goto _exit;
    }

    if (pDataInfo->code != TSDB_CODE_SUCCESS) {
      qError("%s vgId:%d, clientId:0x%" PRIx64 " taskID:0x%" PRIx64 " execId:%d error happens, code:%s",
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             tstrerror(pDataInfo->code));
      code = pDataInfo->code;
      copySourceExtErrMsg(pTaskInfo, pDataInfo);
      goto _exit;
    }

    SRetrieveTableRsp*   pRsp = pDataInfo->pRsp;
    SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;

    if (isStrictMultiGroupRunnerSource(pOperator, pExchangeInfo->current) ||
        hasRunnerFetchFrame(pRsp->streamBlockType)) {
      EExchangeFetchResultType resultType = EXCHANGE_FETCH_RESULT_NONE;
      bool                     outputQueued = false;
      code = decodeStrictMultiGroupFetchResponse(pOperator, pExchangeInfo, pDataInfo, pExchangeInfo->current,
                                                 &resultType, &outputQueued);
      TAOS_CHECK_EXIT(code);
      if (resultType == EXCHANGE_FETCH_RESULT_EOF) {
        pExchangeInfo->current++;
        clearSourceResponse(pDataInfo);
        continue;
      }
      if (resultType != EXCHANGE_FETCH_RESULT_DATA && resultType != EXCHANGE_FETCH_RESULT_EOG) {
        TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
      }
      updateLoadRemoteInfo(pLoadInfo, pRsp->numOfRows, pRsp->compLen, startTs, pOperator);
      pDataInfo->totalRows += pRsp->numOfRows;
      pExchangeInfo->current++;
      clearSourceResponse(pDataInfo);
      if (outputQueued) return;
      continue;
    }

    if (pRsp->numOfRows == 0) {
      qDebug("exhausted %p,%s vgId:%d, clientId:0x%" PRIx64 " taskID:0x%" PRIx64
             " execId:%d idx %d of total completed, rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 " try next", pDataInfo,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             pExchangeInfo->current + 1, pDataInfo->totalRows, pLoadInfo->totalRows);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      if (isVstbScan(pDataInfo) || isVstbTagScan(pDataInfo)) {
        pExchangeInfo->current = -1;
      } else {
        pExchangeInfo->current += 1;
      }
      clearSourceResponse(pDataInfo);
      continue;
    }

    code = doExtractResultBlocks(pExchangeInfo, pDataInfo, pTaskInfo->pStreamRuntimeInfo);
    TAOS_CHECK_EXIT(code);

    SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;
    if (pRsp->completed == 1) {
      qDebug("exhausted %p,%s fetch msg rsp from vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64 " execId:%d numOfRows:%" PRId64
             ", rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64 " try next %d/%d", pDataInfo,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             pRetrieveRsp->numOfRows, pDataInfo->totalRows, pLoadInfo->totalRows, pLoadInfo->totalSize,
             pExchangeInfo->current + 1, totalSources);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      if (isVstbScan(pDataInfo)) {
        pExchangeInfo->current = -1;
        clearSourceResponse(pDataInfo);
        continue;
      }
    } else {
      qDebug("%s fetch msg rsp from vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64 " execId:%d idx:%d numOfRows:%" PRId64
             ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             pExchangeInfo->current, pRetrieveRsp->numOfRows, pLoadInfo->totalRows, pLoadInfo->totalSize);
    }

    updateLoadRemoteInfo(pLoadInfo, pRetrieveRsp->numOfRows, pRetrieveRsp->compLen, startTs, pOperator);
    pDataInfo->totalRows += pRetrieveRsp->numOfRows;

    pExchangeInfo->current++;

    clearSourceResponse(pDataInfo);
    return;
  }

_exit:

  if (code) {
    if (isStrictMultiGroupRunnerSource(pOperator, 0)) {
      clearSourceResponse(pDataInfo);
      clearExchangeResultState(pOperator, pExchangeInfo);
      resetStrictMultiGroupBarrier(pExchangeInfo);
    }
    pTaskInfo->code = code;
    qError("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
  }
}


static void concurrentlyLoadRemoteDataImpl(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo,
                                           SExecTaskInfo* pTaskInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSourceDataInfo);
  int32_t completed = 0;
  code = getCompletedSources(pExchangeInfo->pSourceDataInfo, &completed);
  if (code != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  if (completed == totalSources) {
    setAllSourcesCompleted(pOperator);
    return;
  }

  SSourceDataInfo* pDataInfo = NULL;

  while (1) {
    qDebug("prepare wait for ready, %p, %s", pExchangeInfo, GET_TASKID(pTaskInfo));
    recordOpExecBeforeDownstream(pOperator);
    code = exchangeWait(pOperator, pExchangeInfo);
    recordOpExecAfterDownstream(pOperator, 0);

    if (code != TSDB_CODE_SUCCESS || isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    for (int32_t i = 0; i < totalSources; ++i) {
      pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, i);
      QUERY_CHECK_NULL(pDataInfo, code, lino, _exit, terrno);
      if (pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) {
        continue;
      }

      if (pDataInfo->status != EX_SOURCE_DATA_READY) {
        continue;
      }

      int64_t currSeqId = atomic_load_64(&pExchangeInfo->seqId);
      if (pDataInfo->seqId != currSeqId) {
        qDebug("concurrent rsp reqId %" PRId64 " mismatch with exchange %p curr seqId %" PRId64 ", ignore it", pDataInfo->seqId, pExchangeInfo, currSeqId);
        clearSourceResponse(pDataInfo);
        break;
      }

      if (pDataInfo->code != TSDB_CODE_SUCCESS) {
        code = pDataInfo->code;
        copySourceExtErrMsg(pTaskInfo, pDataInfo);
        TAOS_CHECK_EXIT(code);
      }

      tmemory_barrier();
      SRetrieveTableRsp*     pRsp = pDataInfo->pRsp;
      SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pDataInfo->index);
      QUERY_CHECK_NULL(pSource, code, lino, _exit, terrno);

      // todo
      SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
      if (hasRunnerFetchFrame(pRsp->streamBlockType)) {
        TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
      }
      if (pRsp->numOfRows == 0) {
        bool twoPassRetry = (pExchangeInfo->twoPassExchange && !pRsp->completed);
        if (twoPassRetry || (NULL != pDataInfo->pSrcUidList && !isVstbScan(pDataInfo))) {
          // two-pass exchange: phase-2 remote re-runs cquery async, the first fetch may return
          // empty before cquery fills the sink -- retry until data or completion. For other
          // exchanges, keep the original semantics (empty rsp exhausts the source).
          if (twoPassRetry) {
            // A killed/cancelled query must not sleep a whole backoff interval first.
            if (isTaskKilled(pTaskInfo)) {
              T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
            }
            // Cap the retry rounds: a phase-2 rescan that never yields data and never
            // completes must fail the query instead of holding it open forever.
            if (++pExchangeInfo->twoPassRetries > EX_TWO_PASS_MAX_RETRY_ROUNDS) {
              qError("two-pass exchange %p phase-2 retry exceeded %d rounds without data, give up, %s",
                     pExchangeInfo, EX_TWO_PASS_MAX_RETRY_ROUNDS, GET_TASKID(pTaskInfo));
              // Distinct errno, not a transport timeout: the remote phase-2 rescan wedged.
              code = TSDB_CODE_QRY_TWO_PASS_RESCAN_STALLED;
              TAOS_CHECK_EXIT(code);
            }
            // Back off exponentially (1ms -> 50ms cap) so a slow phase-2 rescan cannot turn the
            // retry into an RPC storm at round-trip speed; reset when data arrives. Yield the
            // pooled worker thread for the sleep (mirrors exchangeWait) so the backoff neither
            // holds a worker hostage nor delays a query kill/cancel.
            if (pExchangeInfo->twoPassRetryMs > 0) {
              if (pTaskInfo->pWorkerCb) {
                code = pTaskInfo->pWorkerCb->beforeBlocking(pTaskInfo->pWorkerCb->pPool);
                TAOS_CHECK_EXIT(code);
              }
              taosMsleep(pExchangeInfo->twoPassRetryMs);
              if (pTaskInfo->pWorkerCb) {
                code = pTaskInfo->pWorkerCb->afterRecoverFromBlocking(pTaskInfo->pWorkerCb->pPool);
                TAOS_CHECK_EXIT(code);
              }
              if (isTaskKilled(pTaskInfo)) {
                T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
              }
            }
            pExchangeInfo->twoPassRetryMs = (0 == pExchangeInfo->twoPassRetryMs)
                                                ? 1
                                                : TMIN(pExchangeInfo->twoPassRetryMs * 2, 50);
          }
          pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
          // Free the consumed empty rsp before resending: loadRemoteDataCallback overwrites
          // pDataInfo->pRsp without freeing the old one, so every retry round would leak it.
          taosMemoryFreeClear(pDataInfo->pRsp);
          code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
          if (code != TSDB_CODE_SUCCESS) {
            clearSourceResponse(pDataInfo);
            TAOS_CHECK_EXIT(code);
          }
        } else {
          pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
          qDebug("exhausted %p,%s vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64
                 " execId:%d index:%d completed, rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 ", try next %d/%" PRIzu, pDataInfo,
                 GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId, i,
                 pDataInfo->totalRows, pExchangeInfo->loadInfo.totalRows, i + 1, totalSources);
          clearSourceResponse(pDataInfo);
        }
        break;
      }

      TAOS_CHECK_EXIT(doExtractResultBlocks(pExchangeInfo, pDataInfo, pTaskInfo->pStreamRuntimeInfo));
      pExchangeInfo->twoPassRetryMs = 0;  // data arrived -- two-pass empty-rsp backoff restarts
      pExchangeInfo->twoPassRetries = 0;

      SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;
      updateLoadRemoteInfo(pLoadInfo, pRetrieveRsp->numOfRows, pRetrieveRsp->compLen, pDataInfo->startTime, pOperator);
      pDataInfo->totalRows += pRetrieveRsp->numOfRows;

      if (pRsp->completed == 1) {
        pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
        qDebug("exhausted %p,%s fetch msg rsp from vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64
               " execId:%d index:%d completed, blocks:%d, numOfRows:%" PRId64 ", rowsOfSource:%" PRIu64
               ", totalRows:%" PRIu64 ", total:%.2f Kb, try next %d/%" PRIzu, pDataInfo,
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId, i,
               pRsp->numOfBlocks, pRsp->numOfRows, pDataInfo->totalRows, pLoadInfo->totalRows,
               pLoadInfo->totalSize / 1024.0, i + 1, totalSources);
      } else {
        qDebug("%s fetch msg rsp from vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64
               " execId:%d blocks:%d, numOfRows:%" PRId64 ", totalRows:%" PRIu64 ", total:%.2f Kb",
               GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
               pRsp->numOfBlocks, pRsp->numOfRows, pLoadInfo->totalRows, pLoadInfo->totalSize / 1024.0);
      }

      clearSourceResponse(pDataInfo);

      if ((pDataInfo->status != EX_SOURCE_DATA_EXHAUSTED || NULL != pDataInfo->pSrcUidList) && !isVstbScan(pDataInfo) && !isVstbTagScan(pDataInfo)) {
        pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
        code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
        if (code != TSDB_CODE_SUCCESS) {
          clearSourceResponse(pDataInfo);
          TAOS_CHECK_EXIT(code);
        }
      }
      
      return;
    }  // end loop

    int32_t complete1 = 0;
    code = getCompletedSources(pExchangeInfo->pSourceDataInfo, &complete1);
    if (code != TSDB_CODE_SUCCESS) {
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }
    if (complete1 == totalSources) {
      qDebug("all sources are completed, %s", GET_TASKID(pTaskInfo));
      return;
    }
  }

_exit:

  if (code) {
    pTaskInfo->code = code;
    qError("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
  }
}

static SSDataBlock* doLoadRemoteDataImpl(SOperatorInfo* pOperator) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);

  SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
  if (pOperator->status == OP_EXEC_DONE) {
    qDebug("%s all %" PRIzu " source(s) are exhausted, total rows:%" PRIu64 " bytes:%" PRIu64 ", elapsed:%.2f ms",
           GET_TASKID(pTaskInfo), totalSources, pLoadInfo->totalRows, pLoadInfo->totalSize,
           pLoadInfo->totalElapsed / 1000.0);
    return NULL;
  }

  bool         strictPopped = false;
  SSDataBlock* p = NULL;
  code = popExchangeMultiGroupResult(pOperator, &p, &strictPopped);
  if (code != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  if (strictPopped) return p;

  // we have buffered retrieved datablock, return it directly
  if (taosArrayGetSize(pExchangeInfo->pResultBlockList) > 0) {
    p = taosArrayGetP(pExchangeInfo->pResultBlockList, 0);
    taosArrayRemove(pExchangeInfo->pResultBlockList, 0);
  }

  if (p != NULL) {
    void* tmp = taosArrayPush(pExchangeInfo->pRecycledBlocks, &p);
    if (!tmp) {
      code = terrno;
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }
    return p;
  } else {
    if (pExchangeInfo->readerGroupRequested) {
      code = loadReaderGroupData(pOperator);
      if (code != TSDB_CODE_SUCCESS) {
        clearExchangeResultState(pOperator, pExchangeInfo);
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
    } else if (pExchangeInfo->seqLoadData && (IS_NON_STREAM_MODE(pTaskInfo) || IS_STREAM_SINGLE_GRP(pTaskInfo))) {
      code = seqLoadRemoteData(pOperator);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
    } else if (IS_STREAM_MODE(pOperator->pTaskInfo))   {
      streamSequenciallyLoadRemoteData(pOperator, pExchangeInfo, pTaskInfo);
    } else {
      concurrentlyLoadRemoteDataImpl(pOperator, pExchangeInfo, pTaskInfo);
    }
    if (TSDB_CODE_SUCCESS != pOperator->pTaskInfo->code) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(pOperator->pTaskInfo->code));
      T_LONG_JMP(pTaskInfo->env, pOperator->pTaskInfo->code);
    }

    code = popExchangeMultiGroupResult(pOperator, &p, &strictPopped);
    if (code != TSDB_CODE_SUCCESS) {
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }
    if (strictPopped) return p;

    if (taosArrayGetSize(pExchangeInfo->pResultBlockList) == 0) {
      qDebug("empty resultBlockList");
      return NULL;
    } else {
      p = taosArrayGetP(pExchangeInfo->pResultBlockList, 0);
      taosArrayRemove(pExchangeInfo->pResultBlockList, 0);
      void* tmp = taosArrayPush(pExchangeInfo->pRecycledBlocks, &p);
      if (!tmp) {
        code = terrno;
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }

      qDebug("block with rows:%" PRId64 " loaded", p->info.rows);
      return p;
    }
  }
}

static int32_t processExchangeLoadedBlock(SOperatorInfo* pOperator, SSDataBlock* pBlock, SSDataBlock** ppRes,
                                          bool* pContinue) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SExchangeInfo* pExchangeInfo = pOperator->info;
  *ppRes = NULL;
  *pContinue = false;

  if ((isStrictMultiGroupRunnerSource(pOperator, 0) || pExchangeInfo->readerGroupMode == 2) &&
      qIsMultiGroupEogBlock(pBlock)) {
    *ppRes = pBlock;
    return TSDB_CODE_SUCCESS;
  }

  code = doFilter(pBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL);
  if (code != TSDB_CODE_SUCCESS) return code;

  if (blockDataGetNumOfRows(pBlock) == 0) {
    qDebug("rows 0 block got, continue next load");
    *pContinue = true;
    return TSDB_CODE_SUCCESS;
  }

  SLimitInfo* pLimitInfo = &pExchangeInfo->limitInfo;
  if (hasLimitOffsetInfo(pLimitInfo)) {
    int32_t status = handleLimitOffset(pOperator, pLimitInfo, pBlock, false);
    if (status == PROJECT_RETRIEVE_CONTINUE) {
      qDebug("limit retrieve continue");
      *pContinue = true;
    } else if (status == PROJECT_RETRIEVE_DONE) {
      if (pBlock->info.rows == 0) {
        setOperatorCompleted(pOperator);
      } else {
        *ppRes = pBlock;
      }
    }
  } else {
    *ppRes = pBlock;
    qDebug("block with rows %" PRId64 " returned in exechange", pBlock->info.rows);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t loadRemoteDataNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  qDebug("%s start to load from exchange %p", pTaskInfo->id.str, pExchangeInfo);

  if (isStrictMultiGroupRunnerSource(pOperator, 0) && isTaskKilled(pTaskInfo)) {
    code = pTaskInfo->code;
    goto _end;
  }

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  while (1) {
    SSDataBlock* pBlock = doLoadRemoteDataImpl(pOperator);
    if (pBlock == NULL) {
      (*ppRes) = NULL;
      return code;
    }

    bool continueLoad = false;
    code = processExchangeLoadedBlock(pOperator, pBlock, ppRes, &continueLoad);
    QUERY_CHECK_CODE(code, lino, _end);
    if (continueLoad) continue;
    return code;
  }

_end:

  if (code != TSDB_CODE_SUCCESS) {
    if (isStrictMultiGroupRunnerSource(pOperator, 0)) {
      clearExchangeResultState(pOperator, pExchangeInfo);
      resetStrictMultiGroupBarrier(pExchangeInfo);
    }
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  } else {
    qDebug("empty block returned in exchange");
  }
  
  (*ppRes) = NULL;
  return code;
}

static int32_t initDataSource(int32_t numOfSources, SExchangeInfo* pInfo, const char* id) {
  pInfo->pSourceDataInfo = taosArrayInit(numOfSources, sizeof(SSourceDataInfo));
  if (pInfo->pSourceDataInfo == NULL) {
    return terrno;
  }

  if (pInfo->dynamicOp) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t len = strlen(id) + 1;
  pInfo->pTaskId = taosMemoryCalloc(1, len);
  if (!pInfo->pTaskId) {
    return terrno;
  }
  tstrncpy(pInfo->pTaskId, id, len);
  for (int32_t i = 0; i < numOfSources; ++i) {
    SSourceDataInfo dataInfo = {0};
    dataInfo.status = EX_SOURCE_DATA_NOT_READY;
    dataInfo.taskId = pInfo->pTaskId;
    dataInfo.index = i;
    SSourceDataInfo* pDs = taosArrayPush(pInfo->pSourceDataInfo, &dataInfo);
    if (pDs == NULL) {
      taosArrayDestroyEx(pInfo->pSourceDataInfo, freeSourceDataInfo);
      return terrno;
    }
    qDebug("init source data info %d, pDs:%p, status:%d", i, pDs, pDs->status);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initExchangeOperator(SExchangePhysiNode* pExNode, SExchangeInfo* pInfo, const char* id) {
  size_t numOfSources = LIST_LENGTH(pExNode->pSrcEndPoints);

  if (numOfSources == 0) {
    qError("%s invalid number: %d of sources in exchange operator", id, (int32_t)numOfSources);
    return TSDB_CODE_INVALID_PARA;
  }
  pInfo->pFetchRpcHandles = taosArrayInit(numOfSources, sizeof(int64_t));
  if (!pInfo->pFetchRpcHandles) {
    return terrno;
  }
  void* ret = taosArrayReserve(pInfo->pFetchRpcHandles, numOfSources);
  if (!ret) {
    return terrno;
  }

  pInfo->pSources = taosArrayInit(numOfSources, sizeof(SDownstreamSourceNode));
  if (pInfo->pSources == NULL) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return terrno;
  }

  if (pExNode->node.dynamicOp) {
    pInfo->pHashSources = tSimpleHashInit(numOfSources * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (NULL == pInfo->pHashSources) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
  }

  for (int32_t i = 0; i < numOfSources; ++i) {
    SDownstreamSourceNode* pNode = (SDownstreamSourceNode*)nodesListGetNode((SNodeList*)pExNode->pSrcEndPoints, i);
    if (!pNode) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    void* tmp = taosArrayPush(pInfo->pSources, pNode);
    if (!tmp) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    SExchangeSrcIndex idx = {.srcIdx = i, .inUseIdx = -1};
    qDebug("%s initExchange %p register source[%d] nodeId:%d taskId:0x%" PRIx64 " dynamicOp:%d",
           id, pInfo, i, pNode->addr.nodeId, pNode->taskId, pExNode->node.dynamicOp);
    int32_t           code =
        tSimpleHashPut(pInfo->pHashSources, &pNode->addr.nodeId, sizeof(pNode->addr.nodeId), &idx, sizeof(idx));
    if (pInfo->pHashSources && code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      return code;
    }
  }

  initLimitInfo(pExNode->node.pLimit, pExNode->node.pSlimit, &pInfo->limitInfo);
  int64_t refId = taosAddRef(fetchObjRefPool, pInfo);
  if (refId < 0) {
    int32_t code = terrno;
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  } else {
    pInfo->self = refId;
  }

  return initDataSource(numOfSources, pInfo, id);
}

int32_t resetExchangeOperState(SOperatorInfo* pOper) {
  SExchangeInfo* pInfo = pOper->info;
  SExchangePhysiNode* pPhynode = (SExchangePhysiNode*)pOper->pPhyNode;

  qDebug("%s reset exchange op:%p info:%p", pOper->pTaskInfo->id.str, pOper, pInfo);

  (void)atomic_add_fetch_64(&pInfo->seqId, 1);
  pOper->status = OP_NOT_OPENED;
  pInfo->current = 0;
  pInfo->notifyToSend = false;
  pInfo->notifySrcOpType = 0;
  pInfo->notifyGroupId = 0;
  pInfo->notifyPhase = SCAN_PHASE_MAIN;
  pInfo->notifyScanGroupId = 0;
  pInfo->notifyTs = 0;
  pInfo->loadInfo.totalElapsed = 0;
  pInfo->loadInfo.totalRows = 0;
  pInfo->loadInfo.totalSize = 0;
  pInfo->phase2Rescan = false;    // clear stale vtable two-pass state; the coordinator re-arms
  pInfo->twoPassExchange = false; // it after reset when a phase-2 restart is actually wanted
  pInfo->twoPassRetryMs = 0;
  pInfo->twoPassRetries = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSourceDataInfo); ++i) {
    SSourceDataInfo* pDataInfo = taosArrayGet(pInfo->pSourceDataInfo, i);
    taosWLockLatch(&pDataInfo->lock);
    taosMemoryFreeClear(pDataInfo->decompBuf);
    clearSourceResponse(pDataInfo);

    pDataInfo->totalRows = 0;
    pDataInfo->code = 0;
    pDataInfo->extErrMsg[0] = '\0';
    pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
    pDataInfo->fetchSent = false;
    pDataInfo->readerFetchId = 0;
    pDataInfo->fetchTimes = 0;
    pDataInfo->fetchCostUs = 0;
    taosWUnLockLatch(&pDataInfo->lock);
  }
  if (isStrictMultiGroupRunnerSource(pOper, 0)) resetStrictMultiGroupBarrier(pInfo);

  if (pInfo->dynamicOp) {
    taosArrayClearEx(pInfo->pSourceDataInfo, freeSourceDataInfo);
  }

  // A new execution must qualify again in its parent's open, after the recursive reset.
  pInfo->readerGroupRequested = false;
  clearExchangeResultState(pOper, pInfo);

  blockDataCleanup(pInfo->pDummyBlock);

  void   *data = NULL;
  int32_t iter = 0;
  while ((data = tSimpleHashIterate(pInfo->pHashSources, data, &iter))) {
    ((SExchangeSrcIndex *)data)->inUseIdx = -1;
  }
  
  pInfo->limitInfo = (SLimitInfo){0};
  initLimitInfo(pPhynode->node.pLimit, pPhynode->node.pSlimit, &pInfo->limitInfo);

  return 0;
}

static int32_t exchangeGetExplainExecInfo(SOperatorInfo* pOptr,
                                          void** pOptrExplain, uint32_t* len) {
  const SExchangeInfo* pExchangeInfo = pOptr->info;
  int32_t numSources = (int32_t)taosArrayGetSize(pExchangeInfo->pSourceDataInfo);

  SExchangeExplainInfo* pInfo = taosMemoryCalloc(1, sizeof(SExchangeExplainInfo));
  if (!pInfo) {
    return terrno;
  }

  pInfo->mode = pExchangeInfo->seqLoadData ? 1 : 0;
  pInfo->numSources = numSources;

  /* all sources are exhausted, thus no need to lock the sources data info */
  for (int32_t i = 0; i < numSources; ++i) {
    const SSourceDataInfo* pSrc = taosArrayGet(pExchangeInfo->pSourceDataInfo, i);
    pInfo->avgFetchTimes += (double)pSrc->fetchTimes / numSources;
    pInfo->avgFetchRows += (double)pSrc->totalRows / numSources;
    pInfo->avgFetchCost += (double)pSrc->fetchCostUs / numSources;
    pInfo->maxFetchTimes = TMAX(pInfo->maxFetchTimes, pSrc->fetchTimes);
    pInfo->maxFetchRows = TMAX(pInfo->maxFetchRows, pSrc->totalRows);
    pInfo->maxFetchCost = TMAX(pInfo->maxFetchCost, pSrc->fetchCostUs);
  }

  *pOptrExplain = pInfo;
  *len = sizeof(SExchangeExplainInfo);
  return TSDB_CODE_SUCCESS;
}

int32_t createExchangeOperatorInfo(void* pTransporter, SExchangePhysiNode* pExNode, SExecTaskInfo* pTaskInfo,
                                   SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t        code = 0;
  int32_t        lino = 0;
  SExchangeInfo* pInfo = taosMemoryCalloc(1, sizeof(SExchangeInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pInfo->isExchange = true;
  pOperator->pPhyNode = pExNode;
  pInfo->dynamicOp = pExNode->node.dynamicOp;
  code = initExchangeOperator(pExNode, pInfo, GET_TASKID(pTaskInfo));
  QUERY_CHECK_CODE(code, lino, _error);

  code = tsem_init(&pInfo->ready, 0, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->pDummyBlock = createDataBlockFromDescNode(pExNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pDummyBlock, code, lino, _error, terrno);

  pInfo->pResultBlockList = taosArrayInit(64, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->pResultBlockList, code, lino, _error, terrno);
  pInfo->pRecycledBlocks = taosArrayInit(64, POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->pRecycledBlocks, code, lino, _error, terrno);
  SExchangeOpStopInfo stopInfo = {QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, pInfo->self};
  code = qAppendTaskStopInfo(pTaskInfo, &stopInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->seqLoadData = pExNode->seqRecvData;
  pInfo->dynTbname = pExNode->dynTbname;
  pInfo->fastInterval = pExNode->fastInterval;
  if (pInfo->dynTbname) {
    pInfo->seqLoadData = true;
  }
  pInfo->pTransporter = pTransporter;

  setOperatorInfo(pOperator, "ExchangeOperator", QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pDummyBlock->pDataBlock);

  code = filterInitFromNode((SNode*)pExNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);
  filterSetExecContext(pOperator->exprSupp.pFilterInfo, pTaskInfo, isTaskKilled);
  qTrace("%s exchange op:%p", __func__, pOperator);
  pOperator->fpSet = createOperatorFpSet(prepareLoadRemoteData, loadRemoteDataNext, NULL,
                                         destroyExchangeOperatorInfo, optrDefaultBufFn,
                                         exchangeGetExplainExecInfo, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetExchangeOperState);
  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  if (pInfo != NULL) {
    doDestroyExchangeOperatorInfo(pInfo);
  }

  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  return code;
}

void destroyExchangeOperatorInfo(void* param) {
  SExchangeInfo* pExInfo = (SExchangeInfo*)param;
  clearPublishedLineage(NULL, pExInfo);
  int32_t        code = taosRemoveRef(fetchObjRefPool, pExInfo->self);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
}

void freeBlock(void* pParam) {
  SSDataBlock* pBlock = *(SSDataBlock**)pParam;
  blockDataDestroy(pBlock);
}

void freeSourceDataInfo(void* p) {
  SSourceDataInfo* pInfo = (SSourceDataInfo*)p;
  taosMemoryFreeClear(pInfo->decompBuf);
  clearSourceResponse(pInfo);
  clearReaderSourceState(pInfo);
  clearRunnerSourceState(pInfo);
  taosArrayDestroy(pInfo->sysScanReqs);
  pInfo->sysScanReqs = NULL;

  pInfo->decompBufSize = 0;
}

void doDestroyExchangeOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }
  SExchangeInfo* pExInfo = (SExchangeInfo*)param;
  if (pExInfo->pFetchRpcHandles) {
    for (int32_t i = 0; i < pExInfo->pFetchRpcHandles->size; ++i) {
      int64_t* pRpcHandle = taosArrayGet(pExInfo->pFetchRpcHandles, i);
      if (*pRpcHandle > 0) {
        SDownstreamSourceNode* pSource = taosArrayGet(pExInfo->pSources, i);
        (void)asyncFreeConnById(pExInfo->pTransporter, *pRpcHandle);
      }
    }
    taosArrayDestroy(pExInfo->pFetchRpcHandles);
  }

  taosArrayDestroy(pExInfo->pSources);
  taosArrayDestroyEx(pExInfo->pSourceDataInfo, freeSourceDataInfo);

  taosArrayDestroyEx(pExInfo->pResultBlockList, freeBlock);
  taosArrayDestroyEx(pExInfo->pRecycledBlocks, freeBlock);
  taosArrayDestroyEx(pExInfo->pMultiGroupResultList, destroyExchangeMultiGroupResult);
  clearPublishedLineage(NULL, pExInfo);

  blockDataDestroy(pExInfo->pDummyBlock);
  tSimpleHashCleanup(pExInfo->pHashSources);

  int32_t code = tsem_destroy(&pExInfo->ready);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
  taosMemoryFreeClear(pExInfo->pTaskId);

  taosMemoryFreeClear(param);
}

int32_t loadRemoteDataCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SFetchRspHandleWrapper* pWrapper = (SFetchRspHandleWrapper*)param;

  taosMemoryFreeClear(pMsg->pEpSet);
  SExchangeInfo* pExchangeInfo = taosAcquireRef(fetchObjRefPool, pWrapper->exchangeId);
  if (pExchangeInfo == NULL) {
    qWarn("failed to acquire exchange operator, since it may have been released, %p", pExchangeInfo);
    taosMemoryFree(pMsg->pData);
    return TSDB_CODE_SUCCESS;
  }

  int64_t currSeqId = atomic_load_64(&pExchangeInfo->seqId);
  if (pWrapper->seqId != currSeqId) {
    qDebug("rsp reqId %" PRId64 " mismatch with exchange %p curr seqId %" PRId64 ", ignore it", pWrapper->seqId, pExchangeInfo, currSeqId);
    taosMemoryFree(pMsg->pData);
    code = taosReleaseRef(fetchObjRefPool, pWrapper->exchangeId);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t          index = pWrapper->sourceIndex;

  qDebug("%s exchange %p %dth source got rsp, code:%d, rsp:%p", pExchangeInfo->pTaskId, pExchangeInfo, index, code, pMsg->pData);

  size_t fetchHandleNum = pExchangeInfo->pFetchRpcHandles ? taosArrayGetSize(pExchangeInfo->pFetchRpcHandles) : 0;
  size_t sourceInfoNum = pExchangeInfo->pSourceDataInfo ? taosArrayGetSize(pExchangeInfo->pSourceDataInfo) : 0;
  if (index < 0 || (size_t)index >= fetchHandleNum || (size_t)index >= sourceInfoNum) {
    qWarn("%s ignore stale fetch rsp, idx:%d fetchHandles:%zu sourceInfos:%zu",
          pExchangeInfo->pTaskId, index, fetchHandleNum, sourceInfoNum);
    taosMemoryFree(pMsg->pData);
    code = taosReleaseRef(fetchObjRefPool, pWrapper->exchangeId);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    }
    return TSDB_CODE_SUCCESS;
  }

  int64_t* pRpcHandle = taosArrayGet(pExchangeInfo->pFetchRpcHandles, index);
  if (pRpcHandle != NULL) {
    int32_t ret = asyncFreeConnById(pExchangeInfo->pTransporter, *pRpcHandle);
    if (ret != 0) {
      qDebug("failed to free rpc handle, code:%s, %p", tstrerror(ret), pExchangeInfo);
    }
    *pRpcHandle = -1;
  }

  SSourceDataInfo* pSourceDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, index);
  if (!pSourceDataInfo) {
    taosMemoryFree(pMsg->pData);
    code = taosReleaseRef(fetchObjRefPool, pWrapper->exchangeId);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    }
    return TSDB_CODE_SUCCESS;
  }

  if (0 == code && NULL == pMsg->pData) {
    qError("invalid rsp msg, msgType:%d, len:%d", pMsg->msgType, pMsg->len);
    code = TSDB_CODE_QRY_INVALID_MSG;
  } else if (code == TSDB_CODE_SUCCESS) {
    code = normalizeFetchResponseHeader(pMsg->pData, pMsg->len);
  }

  taosWLockLatch(&pSourceDataInfo->lock);
  // An error also completes this request and must carry its execution identity.
  pSourceDataInfo->seqId = pWrapper->seqId;
  if (code == TSDB_CODE_SUCCESS) {
    pSourceDataInfo->fetchCostUs += taosGetTimestampUs() - pSourceDataInfo->startTime;
    pSourceDataInfo->fetchTimes++;

    pSourceDataInfo->pRsp = pMsg->pData;
    pSourceDataInfo->rspLen = pMsg->len;
    pSourceDataInfo->code = TSDB_CODE_SUCCESS;

    SRetrieveTableRsp* pRsp = pSourceDataInfo->pRsp;
    qDebug("%s fetch rsp received, index:%d, blocks:%d, rows:%" PRId64 ", %p", pSourceDataInfo->taskId, index,
           pRsp->numOfBlocks, pRsp->numOfRows, pExchangeInfo);
  } else {
    // qworker appends optional NUL-terminated detail text to failed fetch responses
    if (pMsg->pData != NULL && pMsg->len > (int32_t)sizeof(SRetrieveTableRsp)) {
      const char* pExtErrMsg = ((SRetrieveTableRsp*)pMsg->pData)->data;
      size_t      extErrLen = pMsg->len - sizeof(SRetrieveTableRsp);
      if (memchr(pExtErrMsg, '\0', extErrLen) != NULL) {
        tstrncpy(pSourceDataInfo->extErrMsg, pExtErrMsg, sizeof(pSourceDataInfo->extErrMsg));
      }
    }
    taosMemoryFree(pMsg->pData);
    pSourceDataInfo->rspLen = 0;
    int32_t cvted = rpcCvtErrCode(code);
    // single write: the upgrade takes the rpc-converted code and returns the final one
    pSourceDataInfo->code = exUpgradeVtbSourceCode(pSourceDataInfo, cvted);
    // the "cvted" log branch must key off rpcCvtErrCode's conversion only; the
    // vtable upgrade always changes the code and logs on its own
    if (cvted != code) {
      qError("%s fetch rsp received, index:%d, error:%s, cvted error: %s, %p", pSourceDataInfo->taskId, index,
             tstrerror(code), tstrerror(pSourceDataInfo->code), pExchangeInfo);
    } else {
      qError("%s fetch rsp received, index:%d, error:%s, %p", pSourceDataInfo->taskId, index, tstrerror(code),
             pExchangeInfo);
    }
  }

  tmemory_barrier();
  pSourceDataInfo->status = EX_SOURCE_DATA_READY;
  taosWUnLockLatch(&pSourceDataInfo->lock);
  
  code = tsem_post(&pExchangeInfo->ready);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to invoke post when fetch rsp is ready, code:%s, %p", tstrerror(code), pExchangeInfo);
    return code;
  }

  code = taosReleaseRef(fetchObjRefPool, pWrapper->exchangeId);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
  return code;
}

int32_t buildTableScanOperatorParam(SOperatorParam** ppRes, SArray* pUidList, int32_t srcOpType, bool tableSeq) {
  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  if (NULL == *ppRes) {
    return terrno;
  }

  STableScanOperatorParam* pScan = taosMemoryMalloc(sizeof(STableScanOperatorParam));
  if (NULL == pScan) {
    taosMemoryFreeClear(*ppRes);
    return terrno;
  }

  pScan->paramType = DYN_TYPE_SCAN_PARAM;
  pScan->pUidList = taosArrayDup(pUidList, NULL);
  if (NULL == pScan->pUidList) {
    taosMemoryFree(pScan);
    taosMemoryFreeClear(*ppRes);
    return terrno;
  }
  pScan->dynType = DYN_TYPE_STB_JOIN;
  pScan->tableSeq = tableSeq;
  pScan->pOrgTbInfo = NULL;
  pScan->pBatchTbInfo = NULL;
  pScan->pTagList = NULL;
  pScan->isNewParam = false;
  pScan->window.skey = INT64_MAX;
  pScan->window.ekey = INT64_MIN;
  pScan->notifyToProcess = false;
  pScan->notifyTs = 0;

  (*ppRes)->opType = srcOpType;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = pScan;
  (*ppRes)->pChildren = NULL;
  (*ppRes)->reUse = false;

  return TSDB_CODE_SUCCESS;
}

int32_t buildTableScanOperatorParamEx(SOperatorParam** ppRes, SArray* pUidList, int32_t srcOpType, SOrgTbInfo *pMap, bool tableSeq, STimeWindow *window, bool isNewParam, ETableScanDynType type) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  STableScanOperatorParam* pScan = NULL;

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno);

  pScan = taosMemoryMalloc(sizeof(STableScanOperatorParam));
  QUERY_CHECK_NULL(pScan, code, lino, _return, terrno);

  pScan->paramType = DYN_TYPE_SCAN_PARAM;
  if (pUidList) {
    pScan->pUidList = taosArrayDup(pUidList, NULL);
    QUERY_CHECK_NULL(pScan->pUidList, code, lino, _return, terrno);
  } else {
    pScan->pUidList = NULL;
  }

  if (pMap) {
    pScan->pOrgTbInfo = taosMemoryMalloc(sizeof(SOrgTbInfo));
    QUERY_CHECK_NULL(pScan->pOrgTbInfo, code, lino, _return, terrno);

    pScan->pOrgTbInfo->vgId = pMap->vgId;
    tstrncpy(pScan->pOrgTbInfo->tbName, pMap->tbName, TSDB_TABLE_FNAME_LEN);
    pScan->pOrgTbInfo->srcPrecision = pMap->srcPrecision;
    pScan->pOrgTbInfo->dstPrecision = pMap->dstPrecision;

    pScan->pOrgTbInfo->colMap = taosArrayDup(pMap->colMap, NULL);
    QUERY_CHECK_NULL(pScan->pOrgTbInfo->colMap, code, lino, _return, terrno);
  } else {
    pScan->pOrgTbInfo = NULL;
  }
  pScan->pTagList = NULL;
  pScan->pBatchTbInfo = NULL;


  pScan->dynType = type;
  pScan->tableSeq = tableSeq;
  pScan->window.skey = window->skey;
  pScan->window.ekey = window->ekey;
  pScan->isNewParam = isNewParam;
  pScan->notifyToProcess = false;
  pScan->notifyTs = 0;
  (*ppRes)->opType = srcOpType;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = pScan;
  (*ppRes)->pChildren = NULL;
  (*ppRes)->reUse = false;

  return code;
_return:
  qError("%s failed at %d, failed to build scan operator msg:%s", __FUNCTION__, lino, tstrerror(code));
  taosMemoryFreeClear(*ppRes);
  if (pScan) {
    taosArrayDestroy(pScan->pUidList);
    if (pScan->pOrgTbInfo) {
      taosArrayDestroy(pScan->pOrgTbInfo->colMap);
      taosMemoryFreeClear(pScan->pOrgTbInfo);
    }
    taosMemoryFree(pScan);
  }
  return code;
}

/**
  @brief build the table scan operator param for notify message
*/
int32_t buildTableScanOperatorParamNotify(SOperatorParam** ppRes,
                                          int32_t srcOpType,
                                          uint64_t notifyGroupId,
                                          int8_t notifyPhase,
                                          uint64_t notifyScanGroupId,
                                          TSKEY notifyTs) {
  if (srcOpType != QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN &&
      srcOpType != QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
    qWarn("%s, invalid srcOpType:%d", __func__, srcOpType);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno);

  STableScanOperatorParam* pTsParam =
    taosMemoryCalloc(1, sizeof(STableScanOperatorParam));
  QUERY_CHECK_NULL(pTsParam, code, lino, _return, terrno);

  pTsParam->paramType = NOTIFY_TYPE_SCAN_PARAM;
  pTsParam->notifyToProcess = true;
  pTsParam->notifyTs = notifyTs;
  pTsParam->groupid = notifyGroupId;
  pTsParam->notifyPhase = notifyPhase;
  pTsParam->notifyScanGroupId = notifyScanGroupId;

  (*ppRes)->opType = srcOpType;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = pTsParam;
  (*ppRes)->pChildren = NULL;
  // A message transfers ownership of the parameter to the receiver.
  (*ppRes)->reUse = false;

_return:
  if (TSDB_CODE_SUCCESS != code) {
    qError("%s failed at %d, failed to build scan operator msg:%s",
           __func__, lino, tstrerror(code));
    taosMemoryFreeClear(*ppRes);
    if (pTsParam) {
      taosMemoryFree(pTsParam);
    }
  }
  return code;
}

/*
 * Build one systable-scan get-param for layered virtual-table reference lookup.
 *
 * @param ppRes output operator param
 * @param pReqs layered reference requests owned by one exchange source
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t buildSysTableScanOperatorParamForExchange(SOperatorParam** ppRes, SArray* pReqs) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  SOperatorParam*             pParam = NULL;
  SSysTableScanOperatorParam* pSysScan = NULL;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno);

  pSysScan = taosMemoryCalloc(1, sizeof(SSysTableScanOperatorParam));
  QUERY_CHECK_NULL(pSysScan, code, lino, _return, terrno);

  if (pReqs != NULL) {
    pSysScan->pVtbRefReqs = taosArrayDup(pReqs, NULL);
    QUERY_CHECK_NULL(pSysScan->pVtbRefReqs, code, lino, _return, terrno);
  }

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN;
  pParam->downstreamIdx = 0;
  pParam->value = pSysScan;
  pParam->pChildren = NULL;
  pParam->reUse = false;

  *ppRes = pParam;
  return code;

_return:
  if (pSysScan != NULL) {
    taosArrayDestroy(pSysScan->pVtbRefReqs);
    pSysScan->pVtbRefReqs = NULL;
    taosMemoryFree(pSysScan);
  }
  taosMemoryFree(pParam);
  qError("%s failed at %d, failed to build sys scan operator msg:%s", __func__, lino, tstrerror(code));
  return code;
}

int32_t buildTableScanOperatorParamBatchInfo(SOperatorParam** ppRes, uint64_t groupid, SArray* pUidList, int32_t srcOpType, SArray *pBatchMap, SArray *pTagList, bool tableSeq, STimeWindow *window, bool isNewParam) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  STableScanOperatorParam* pScan = NULL;

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno);

  pScan = taosMemoryMalloc(sizeof(STableScanOperatorParam));
  QUERY_CHECK_NULL(pScan, code, lino, _return, terrno);

  pScan->paramType = DYN_TYPE_SCAN_PARAM;
  pScan->groupid = groupid;
  if (pUidList) {
    pScan->pUidList = taosArrayDup(pUidList, NULL);
    QUERY_CHECK_NULL(pScan->pUidList, code, lino, _return, terrno);
  } else {
    pScan->pUidList = NULL;
  }
  pScan->pOrgTbInfo = NULL;

  if (pBatchMap) {
    pScan->pBatchTbInfo = taosArrayInit(1, sizeof(SOrgTbInfo));
    QUERY_CHECK_NULL(pScan->pBatchTbInfo, code, lino, _return, terrno);
    for (int32_t i = 0; i < taosArrayGetSize(pBatchMap); i++) {
      SOrgTbInfo *pSrcInfo = taosArrayGet(pBatchMap, i);
      SOrgTbInfo batchInfo = {0};
      batchInfo.vgId = pSrcInfo->vgId;
      tstrncpy(batchInfo.tbName, pSrcInfo->tbName, TSDB_TABLE_FNAME_LEN);
      batchInfo.srcPrecision = pSrcInfo->srcPrecision;
      batchInfo.dstPrecision = pSrcInfo->dstPrecision;
      batchInfo.colMap = taosArrayDup(pSrcInfo->colMap, NULL);
      QUERY_CHECK_NULL(batchInfo.colMap, code, lino, _return, terrno);
      SOrgTbInfo *pDstInfo = taosArrayPush(pScan->pBatchTbInfo, &batchInfo);
      QUERY_CHECK_NULL(pDstInfo, code, lino, _return, terrno);
    }
  } else {
    pScan->pBatchTbInfo = NULL;
  }

  if (pTagList) {
    pScan->pTagList = taosArrayInit(1, sizeof(STagVal));
    QUERY_CHECK_NULL(pScan->pTagList, code, lino, _return, terrno);

    for (int32_t i = 0; i < taosArrayGetSize(pTagList); ++i) {
      STagVal *pSrcTag = (STagVal*)taosArrayGet(pTagList, i);
      STagVal  dstTag = {0};
      dstTag.type = pSrcTag->type;
      dstTag.cid = pSrcTag->cid;
      if (IS_VAR_DATA_TYPE(pSrcTag->type)) {
        dstTag.nData = pSrcTag->nData;
        if (pSrcTag->pData != NULL && dstTag.nData > 0 && dstTag.nData != UINT32_MAX) {
          dstTag.pData = taosMemoryMalloc(dstTag.nData);
          QUERY_CHECK_NULL(dstTag.pData, code, lino, _return, terrno);
          memcpy(dstTag.pData, pSrcTag->pData, dstTag.nData);
        }
      } else {
        dstTag.i64 = pSrcTag->i64;
      }

      QUERY_CHECK_NULL(taosArrayPush(pScan->pTagList, &dstTag), code, lino, _return, terrno);
    }
  } else {
    pScan->pTagList = NULL;
  }


  pScan->dynType = DYN_TYPE_VSTB_BATCH_SCAN;
  pScan->tableSeq = tableSeq;
  pScan->window.skey = window->skey;
  pScan->window.ekey = window->ekey;
  pScan->isNewParam = isNewParam;
  pScan->notifyToProcess = false;
  pScan->notifyTs = 0;
  (*ppRes)->opType = srcOpType;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = pScan;
  (*ppRes)->pChildren = NULL;
  (*ppRes)->reUse = false;

  return code;
_return:
  qError("%s failed at %d, failed to build scan operator msg:%s", __FUNCTION__, lino, tstrerror(code));
  taosMemoryFreeClear(*ppRes);
  if (pScan) {
    taosArrayDestroy(pScan->pUidList);
    if (pScan->pBatchTbInfo) {
      taosArrayDestroy(pScan->pBatchTbInfo);
    }
    taosMemoryFree(pScan);
  }
  return code;
}

/*
 * Build hash-agg operator get-param for dynamic virtual-table aggregation.
 *
 * @param ppRes Output operator param.
 * @param groupid Group id bound to this batch.
 * @param pUidList Source table uid list.
 * @param pBatchMap Batch table metadata map.
 * @param pTagList Optional tag value list.
 * @param tableSeq Whether to keep table-sequential mode.
 * @param window Time window for downstream scan.
 * @param isNewParam Whether downstream should treat this as a new param batch.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
int32_t buildAggOperatorParam(SOperatorParam** ppRes, uint64_t groupid, SArray* pUidList, SArray* pBatchMap,
                              SArray* pTagList, bool tableSeq, STimeWindow* window, bool isNewParam) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  SOperatorParam*          pParam = NULL;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno);

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL;
  pParam->downstreamIdx = 0;
  pParam->value = NULL;
  pParam->pChildren = NULL;
  pParam->reUse = false;

  pParam->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL(pParam->pChildren, code, lino, _return, terrno);

  SOperatorParam* pTableScanParam = NULL;
  code = buildTableScanOperatorParamBatchInfo(&pTableScanParam, groupid, pUidList, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN,
                                              pBatchMap, pTagList, tableSeq, window, isNewParam);
  QUERY_CHECK_CODE(code, lino, _return);

  QUERY_CHECK_NULL(taosArrayPush(pParam->pChildren, &pTableScanParam), code, lino, _return, terrno);

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_HASH_AGG;
  pParam->downstreamIdx = 0;
  pParam->value = NULL;
  pParam->reUse = false;

  *ppRes = pParam;
  return code;

_return:
  freeOperatorParam(pParam, OP_GET_PARAM);
  qError("%s failed at %d, failed to build agg scan operator msg:%s", __FUNCTION__, lino, tstrerror(code));
  return code;
}

/*
 * Build hash-interval operator get-param for dynamic virtual-table interval query.
 *
 * @param ppRes Output operator param.
 * @param groupid Group id bound to this batch.
 * @param pUidList Source table uid list.
 * @param pBatchMap Batch table metadata map.
 * @param pTagList Optional tag value list.
 * @param tableSeq Whether to keep table-sequential mode.
 * @param window Time window for downstream scan.
 * @param isNewParam Whether downstream should treat this as a new param batch.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
static int32_t buildIntervalOperatorParam(SOperatorParam** ppRes, uint64_t groupid, SArray* pUidList, SArray* pBatchMap,
                                          SArray* pTagList, bool tableSeq, STimeWindow* window, bool isNewParam) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SOperatorParam* pParam = NULL;

  pParam = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(pParam, code, lino, _return, terrno);

  pParam->opType = QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL;
  pParam->downstreamIdx = 0;
  pParam->value = NULL;
  pParam->reUse = false;
  pParam->pChildren = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL(pParam->pChildren, code, lino, _return, terrno);

  SOperatorParam* pTableScanParam = NULL;
  code = buildTableScanOperatorParamBatchInfo(&pTableScanParam, groupid, pUidList, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN,
                                              pBatchMap, pTagList, tableSeq, window, isNewParam);
  QUERY_CHECK_CODE(code, lino, _return);

  QUERY_CHECK_NULL(taosArrayPush(pParam->pChildren, &pTableScanParam), code, lino, _return, terrno);

  *ppRes = pParam;
  return code;

_return:
  freeOperatorParam(pParam, OP_GET_PARAM);
  qError("%s failed at %d, failed to build interval scan operator msg:%s", __FUNCTION__, lino, tstrerror(code));
  return code;
}

int32_t buildTagScanOperatorParam(SOperatorParam** ppRes, SArray* pUidList, int32_t srcOpType) {
  int32_t                  code = TSDB_CODE_SUCCESS;
  int32_t                  lino = 0;
  STagScanOperatorParam*   pScan = NULL;

  if (pUidList == NULL || taosArrayGetSize(pUidList) <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  *ppRes = taosMemoryCalloc(1, sizeof(SOperatorParam));
  QUERY_CHECK_NULL(*ppRes, code, lino, _return, terrno);

  pScan = taosMemoryMalloc(sizeof(STagScanOperatorParam));
  QUERY_CHECK_NULL(pScan, code, lino, _return, terrno);
  pScan->vcUid = *(tb_uid_t*)taosArrayGet(pUidList, 0);

  (*ppRes)->opType = srcOpType;
  (*ppRes)->downstreamIdx = 0;
  (*ppRes)->value = pScan;
  (*ppRes)->pChildren = NULL;
  (*ppRes)->reUse = false;

  return code;
_return:
  qError("%s failed at %d, failed to build scan operator msg:%s", __FUNCTION__, lino, tstrerror(code));
  taosMemoryFreeClear(*ppRes);
  if (pScan) {
    taosMemoryFree(pScan);
  }
  return code;
}

static int32_t getCurrentWinCalcParam(SStreamRuntimeFuncInfo* pRuntimeInfo, SSTriggerCalcParam** ppParam) {
  if (!pRuntimeInfo || !ppParam) {
    return TSDB_CODE_INTERNAL_ERROR;
  }

  SSTriggerCalcParam* pParam = taosArrayGet(pRuntimeInfo->pStreamPesudoFuncVals, pRuntimeInfo->curIdx);
  if (!pParam) {
    return TSDB_CODE_INTERNAL_ERROR;
  }

  *ppParam = pParam;
  return TSDB_CODE_SUCCESS;
}

static int32_t getCurrentWinCalcTimeRange(SStreamRuntimeFuncInfo* pRuntimeInfo, STimeWindow* pTimeRange) {
  if (!pRuntimeInfo || !pTimeRange) {
    return TSDB_CODE_INTERNAL_ERROR;
  }

  SSTriggerCalcParam* pParam = NULL;
  int32_t             code = getCurrentWinCalcParam(pRuntimeInfo, &pParam);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  switch (pRuntimeInfo->triggerType) {
    case STREAM_TRIGGER_SLIDING:
      // Unable to distinguish whether there is an interval, all use wstart/wend
      // and the results are equal to those of prevTs/currentTs, using the same address of union.
      pTimeRange->skey = pParam->wstart;  // is equal to wstart
      pTimeRange->ekey = pParam->wend;    // is equal to wend
      break;
    case STREAM_TRIGGER_PERIOD:
      pTimeRange->skey = pParam->prevLocalTime;
      pTimeRange->ekey = pParam->triggerTime;
      break;
    default:
      pTimeRange->skey = pParam->wstart;
      pTimeRange->ekey = pParam->wend;
      break;
  }

  return TSDB_CODE_SUCCESS;
}

void clearVtbScanDataInfo(void* pItem) {
  SSourceDataInfo *pInfo = (SSourceDataInfo *)pItem;
  taosMemoryFreeClear(pInfo->decompBuf);
  taosMemoryFreeClear(pInfo->pRsp);
  pInfo->decompBufSize = 0;
  if (pInfo->orgTbInfo) {
    taosArrayDestroy(pInfo->orgTbInfo->colMap);
    taosMemoryFreeClear(pInfo->orgTbInfo);
  }
  if (pInfo->batchOrgTbInfo) {
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->batchOrgTbInfo); ++i) {
      SOrgTbInfo* pColMap = taosArrayGet(pInfo->batchOrgTbInfo, i);
      if (pColMap) {
        taosArrayDestroy(pColMap->colMap);
      }
    }
    taosArrayDestroy(pInfo->batchOrgTbInfo);
    pInfo->batchOrgTbInfo = NULL;
  }
  if (pInfo->tagList) {
    taosArrayDestroyEx(pInfo->tagList, destroyTagVal);
    pInfo->tagList = NULL;
  }
  if (pInfo->pSrcUidList) {
    taosArrayDestroy(pInfo->pSrcUidList);
    pInfo->pSrcUidList = NULL;
  }
  if (pInfo->sysScanReqs) {
    taosArrayDestroy(pInfo->sysScanReqs);
    pInfo->sysScanReqs = NULL;
  }
}

static int32_t projectNodeGroupReadView(SStreamRuntimeFuncInfo* pStream, int32_t actualNodeId) {
  tClearStreamCurrentGroupRead(pStream);
  pStream->curNodeId = actualNodeId;
  SArray** ppNodeReads = tSimpleHashGet(pStream->pGroupReadInfos, &actualNodeId, sizeof(actualNodeId));
  if (ppNodeReads == NULL || *ppNodeReads == NULL) return TSDB_CODE_SUCCESS;

  SArray* pView = taosArrayInit(1, sizeof(SSTriggerGroupReadInfo));
  if (pView == NULL) return terrno;
  for (int32_t i = 0; i < taosArrayGetSize(*ppNodeReads); ++i) {
    const SSTriggerGroupReadInfo* pRead = taosArrayGet(*ppNodeReads, i);
    if (pRead == NULL) {
      taosArrayDestroy(pView);
      return TSDB_CODE_INVALID_PARA;
    }
    if (taosArrayPush(pView, pRead) == NULL) {
      taosArrayDestroy(pView);
      return terrno;
    }
  }
  if (taosArrayGetSize(pView) == 0) {
    taosArrayDestroy(pView);
    return TSDB_CODE_SUCCESS;
  }

  pStream->curGrpRead = pView;
  pStream->curGrpReadOwnership = STREAM_GRP_READ_OWNED_SHALLOW_VIEW;
  return TSDB_CODE_SUCCESS;
}

static void setStreamFetchResetMode(SResFetchReq* pReq, bool* pFetchSent, const SStreamRuntimeInfo* pRuntime) {
  if (*pFetchSent) {
    return;
  }
  pReq->reset = true;
  pReq->reuseReader = qStreamFetchReuseReader(pRuntime);
  *pFetchSent = true;
}

static int32_t serializeStreamFetchReq(void* buf, int32_t bufLen, SResFetchReq* pReq, int32_t msgType,
                                       bool needStreamRtInfo, bool needStreamGrpInfo) {
  const SStreamRuntimeFuncInfo* pInfo = pReq->pStRtFuncInfo;
  if (msgType == TDMT_STREAM_FETCH_FROM_RUNNER && !pReq->reset && pInfo != NULL && pInfo->isMultiGroupCalc) {
    // The runner retains the batch and nested context until EOF. Keep the grouped
    // envelope on continuations without projecting or sending that context again.
    SStreamRuntimeFuncInfo continuation = {0};
    continuation.isMultiGroupCalc = pInfo->isMultiGroupCalc;
    continuation.stbPartByTbname = pInfo->stbPartByTbname;
    continuation.curWindow = pInfo->curWindow;
    continuation.curIdx = pInfo->curIdx;
    continuation.sessionId = pInfo->sessionId;
    continuation.streamGen = pInfo->streamGen;
    continuation.withExternalWindow = pInfo->withExternalWindow;
    continuation.triggerType = pInfo->triggerType;
    continuation.isWindowTrigger = pInfo->isWindowTrigger;
    continuation.precision = pInfo->precision;
    SResFetchReq request = *pReq;
    request.pStRtFuncInfo = &continuation;
    return tSerializeSResFetchReq(buf, bufLen, &request, false, false);
  }
  return tSerializeSResFetchReq(buf, bufLen, pReq, needStreamRtInfo, needStreamGrpInfo);
}

#if defined(BUILD_TEST)
int32_t exchangeTestSerializeStreamFetchReq(void* buf, int32_t bufLen, SResFetchReq* pReq, int32_t msgType,
                                            bool needStreamRtInfo, bool needStreamGrpInfo) {
  return serializeStreamFetchReq(buf, bufLen, pReq, msgType, needStreamRtInfo, needStreamGrpInfo);
}

void exchangeTestSetStreamFetchResetMode(SResFetchReq* pReq, bool* pFetchSent, const SStreamRuntimeInfo* pRuntime) {
  setStreamFetchResetMode(pReq, pFetchSent, pRuntime);
}
#endif

int32_t doSendFetchDataRequest(SExchangeInfo* pExchangeInfo, SExecTaskInfo* pTaskInfo, int32_t sourceIndex) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, sourceIndex);
  QUERY_CHECK_NULL(pDataInfo, code, lino, _end, terrno);

  if (EX_SOURCE_DATA_NOT_READY != pDataInfo->status) {
    return TSDB_CODE_SUCCESS;
  }

  SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pDataInfo->index);
  QUERY_CHECK_NULL(pSource, code, lino, _end, terrno);

  SStreamRuntimeFuncInfo* pStream = GET_STM_RTINFO(pTaskInfo);
  if (pStream != NULL && pStream->isMultiGroupCalc && pStream->pGroupReadInfos != NULL &&
      pSource->fetchMsgType == TDMT_STREAM_FETCH && pDataInfo->readerFetchId == 0) {
    code = projectNodeGroupReadView(pStream, pSource->addr.nodeId);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pStream->curGrpRead == NULL) {
      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      return TSDB_CODE_SUCCESS;
    }
  }

  pDataInfo->status = EX_SOURCE_DATA_STARTED;

  size_t totalSources = taosArrayGetSize(pExchangeInfo->pSources);

  SFetchRspHandleWrapper* pWrapper = taosMemoryCalloc(1, sizeof(SFetchRspHandleWrapper));
  QUERY_CHECK_NULL(pWrapper, code, lino, _end, terrno);
  pWrapper->exchangeId = pExchangeInfo->self;
  pWrapper->sourceIndex = sourceIndex;
  pWrapper->seqId = pExchangeInfo->seqId;
  uint64_t queryId = pTaskInfo->pStreamRuntimeInfo == NULL && pSource->producerQueryId != 0
                         ? pSource->producerQueryId
                         : pTaskInfo->id.queryId;

  if (pSource->localExec) {
    // vtable two-pass phase-2 runs over the RPC fetch path only: the rescan directive
    // (DYN_TYPE_TWO_PASS_RESCAN) rides the fetch request msg, and the local-fetch entry
    // has no such channel -- it would re-read the drained sink and report the source
    // exhausted (silently wrong PERCENTILE). Structurally unreachable today (two-pass
    // origins are SCAN subplans, SCH_IS_LOCAL_EXEC_TASK excludes them), so fail loudly
    // if that invariant is ever broken instead of returning wrong results.
    if (pExchangeInfo->phase2Rescan || pExchangeInfo->twoPassExchange) {
      qError("%s vtable two-pass rescan hit a localExec source, sourceIndex:%d, taskId:0x%" PRIx64,
             GET_TASKID(pTaskInfo), sourceIndex, pSource->taskId);
      code = TSDB_CODE_PAR_NOT_SUPPORT_VTABLE_TWO_PASS;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    SDataBuf pBuf = {0};
    code = (*pTaskInfo->localFetch.fp)(pTaskInfo->localFetch.handle, pSource->sId, queryId,
                                       pSource->clientId, pSource->taskId, 0, pSource->execId, &pBuf.pData,
                                       pTaskInfo->localFetch.explainRes);
    if (code == TSDB_CODE_SUCCESS && pBuf.pData != NULL) {
      code = getLocalFetchResponseLen(pBuf.pData, &pBuf.len);
      if (code != TSDB_CODE_SUCCESS) taosMemoryFreeClear(pBuf.pData);
    }
    if (code != TSDB_CODE_SUCCESS) {
      // The local-exec path aborts before loadRemoteDataCallback, so the
      // vtable-source code upgrade must happen here as well.
      code = exUpgradeVtbSourceCode(pDataInfo, rpcCvtErrCode(code));
      pDataInfo->code = code;
      copySourceExtErrMsg(pTaskInfo, pDataInfo);
    }
    QUERY_CHECK_CODE(code, lino, _end);
    pDataInfo->startTime = taosGetTimestampUs();
    code = loadRemoteDataCallback(pWrapper, &pBuf, code);
    taosMemoryFreeClear(pWrapper);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    bool needStreamRtInfo = true;
    bool needStreamGrpInfo = false;
    SResFetchReq req = {0};
    SStreamRuntimeFuncInfo cacheRtInfo = {0};
    // The declaration is fixed for the Reader execution, including overall legacy fallback.
    req.readerGroupMode = pExchangeInfo->readerGroupRequested && pSource->fetchMsgType == TDMT_STREAM_FETCH;
    req.runnerEogBatch = pDataInfo->runnerEogBatch;
    req.header.vgId = pSource->addr.nodeId;
    req.sId = pSource->sId;
    req.clientId = pSource->clientId;
    req.taskId = pSource->taskId;
    req.queryId = queryId;
    req.execId = pSource->execId;
    if (pTaskInfo->pStreamRuntimeInfo) {
      req.dynTbname = pExchangeInfo->dynTbname;
      req.execId = pTaskInfo->pStreamRuntimeInfo->execId;
      req.pStRtFuncInfo = pStream;

      if (pSource->fetchMsgType == TDMT_STREAM_FETCH_FROM_RUNNER) {
        qDebug("%s stream fetch from runner, execId:%d, %p", GET_TASKID(pTaskInfo), req.execId, pTaskInfo->pStreamRuntimeInfo);
      } else if (pSource->fetchMsgType == TDMT_STREAM_FETCH_FROM_CACHE) {
        SSTriggerCalcParam* pParam = NULL;
        code = getCurrentWinCalcParam(req.pStRtFuncInfo, &pParam);
        QUERY_CHECK_CODE(code, lino, _end);
        code = getCurrentWinCalcTimeRange(req.pStRtFuncInfo, &req.pStRtFuncInfo->curWindow);
        QUERY_CHECK_CODE(code, lino, _end);
        cacheRtInfo = *req.pStRtFuncInfo;
        cacheRtInfo.curEventConditionPath = pParam->eventConditionPath;
        req.pStRtFuncInfo = &cacheRtInfo;
        needStreamRtInfo = false;
        qDebug("%s stream fetch from cache, execId:%d, curWinIdx:%d, time range:[%" PRId64 ", %" PRId64 "]",
               GET_TASKID(pTaskInfo), req.execId, req.pStRtFuncInfo->curIdx, req.pStRtFuncInfo->curWindow.skey,
               req.pStRtFuncInfo->curWindow.ekey);
      } else if (pSource->fetchMsgType == TDMT_STREAM_FETCH_EXT) {
        /* P4-E3: EXT fetch  -  runner requests data from an ETR (external-table
         * reader) task.  Only set a merged time range when the runner has an
         * external window operator (%%trows); otherwise leave curWindow as
         * {0,0} so the ext reader does a full table scan  -  the calc SQL has
         * no time range filter or uses _twstart/_twend which are evaluated
         * on the runner side after fetching. */
        if (req.pStRtFuncInfo->withExternalWindow &&
            req.pStRtFuncInfo->triggerType != STREAM_TRIGGER_PERIOD) {
          SArray *pVals = req.pStRtFuncInfo->pStreamPesudoFuncVals;
          int32_t nWin = (int32_t)taosArrayGetSize(pVals);
          if (nWin > 0) {
            SSTriggerCalcParam *p0 = taosArrayGet(pVals, 0);
            SSTriggerCalcParam *pN = taosArrayGet(pVals, nWin - 1);
            req.pStRtFuncInfo->curWindow.skey = p0->wstart;
            req.pStRtFuncInfo->curWindow.ekey = pN->wend;
          }
        }
        if (req.pStRtFuncInfo->triggerType == STREAM_TRIGGER_PERIOD) {
          req.pStRtFuncInfo->curWindow = (STimeWindow){0};
          qDebug("%s stream fetch ext skips dynamic period bounds", GET_TASKID(pTaskInfo));
        }
        needStreamRtInfo = true;
        needStreamGrpInfo = false;
        qDebug("%s stream fetch ext, execId:%d, withExtWin:%d, merged time range:[%" PRId64 ", %" PRId64 "] nWin:%d",
               GET_TASKID(pTaskInfo), req.execId, req.pStRtFuncInfo->withExternalWindow,
               req.pStRtFuncInfo->curWindow.skey,
               req.pStRtFuncInfo->curWindow.ekey,
               (int32_t)taosArrayGetSize(req.pStRtFuncInfo->pStreamPesudoFuncVals));
      } else {
        needStreamGrpInfo = true;
      }

      setStreamFetchResetMode(&req, &pDataInfo->fetchSent, pTaskInfo->pStreamRuntimeInfo);
    }

    req.phase2Rescan = pExchangeInfo->phase2Rescan;

    switch (pDataInfo->type) {
      case EX_SRC_TYPE_VSTB_SCAN: {
        code = buildTableScanOperatorParamEx(&req.pOpParam, pDataInfo->pSrcUidList, pDataInfo->srcOpType,
                                             pDataInfo->orgTbInfo, pDataInfo->tableSeq, &pDataInfo->window,
                                             pDataInfo->isNewParam, DYN_TYPE_VSTB_SINGLE_SCAN);
        clearVtbScanDataInfo(pDataInfo);
        QUERY_CHECK_CODE(code, lino, _end);
        break;
      }
      case EX_SRC_TYPE_VTB_WIN_SCAN: {
        if (pDataInfo->pSrcUidList) {
          code = buildTableScanOperatorParamEx(&req.pOpParam, pDataInfo->pSrcUidList, pDataInfo->srcOpType, NULL, pDataInfo->tableSeq, &pDataInfo->window, false, DYN_TYPE_VSTB_WIN_SCAN);
          taosArrayDestroy(pDataInfo->pSrcUidList);
          pDataInfo->pSrcUidList = NULL;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      case EX_SRC_TYPE_VSTB_TAG_SCAN: {
        if (pDataInfo->pSrcUidList == NULL || taosArrayGetSize(pDataInfo->pSrcUidList) <= 0) {
          qDebug("%s skip VSTB_TAG_SCAN due to empty uid list", GET_TASKID(pTaskInfo));
          break;
        }
        code = buildTagScanOperatorParam(&req.pOpParam, pDataInfo->pSrcUidList, pDataInfo->srcOpType);
        taosArrayDestroy(pDataInfo->pSrcUidList);
        pDataInfo->pSrcUidList = NULL;
        QUERY_CHECK_CODE(code, lino, _end);
        break;
      }
      case EX_SRC_TYPE_VSTB_WIN_SCAN:
      case EX_SRC_TYPE_VSTB_INTERVAL_SCAN:
      case EX_SRC_TYPE_VSTB_TS_SCAN: {
        if (pDataInfo->batchOrgTbInfo) {
          int32_t srcOpType =
              (pDataInfo->type == EX_SRC_TYPE_VSTB_TS_SCAN)
                  ? QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN
                  : QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
          code = buildTableScanOperatorParamBatchInfo(
              &req.pOpParam, pDataInfo->groupid, pDataInfo->pSrcUidList, srcOpType,
              pDataInfo->batchOrgTbInfo, pDataInfo->tagList, pDataInfo->tableSeq, &pDataInfo->window,
              pDataInfo->isNewParam);
          clearVtbScanDataInfo(pDataInfo);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      case EX_SRC_TYPE_VSTB_PART_INTERVAL_SCAN: {
        if (pDataInfo->batchOrgTbInfo) {
          code = buildIntervalOperatorParam(&req.pOpParam, pDataInfo->groupid, pDataInfo->pSrcUidList,
                                            pDataInfo->batchOrgTbInfo, pDataInfo->tagList, pDataInfo->tableSeq,
                                            &pDataInfo->window, pDataInfo->isNewParam);
          clearVtbScanDataInfo(pDataInfo);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      case EX_SRC_TYPE_VSTB_AGG_SCAN: {
        if (pDataInfo->batchOrgTbInfo) {
          code = buildAggOperatorParam(&req.pOpParam, pDataInfo->groupid, pDataInfo->pSrcUidList,
                                       pDataInfo->batchOrgTbInfo, pDataInfo->tagList,
                                       pDataInfo->tableSeq, &pDataInfo->window, pDataInfo->isNewParam);
          clearVtbScanDataInfo(pDataInfo);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      case EX_SRC_TYPE_VSTB_SYS_SCAN: {
        if (pDataInfo->sysScanReqs) {
          code = buildSysTableScanOperatorParamForExchange(&req.pOpParam, pDataInfo->sysScanReqs);
          clearVtbScanDataInfo(pDataInfo);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        break;
      }
      case EX_SRC_TYPE_STB_JOIN_SCAN:
      default: {
        if (pExchangeInfo->phase2Rescan) {
          // vtable two-pass phase-2 rescan: tell the remote origin TableScan to reuse its
          // reader (tsdReaderResetStatus, version-stable) and rescan from the beginning.
          STimeWindow zeroWin = {.skey = INT64_MAX, .ekey = INT64_MIN};
          code = buildTableScanOperatorParamEx(&req.pOpParam, NULL, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, NULL, false,
                                                &zeroWin, false, DYN_TYPE_TWO_PASS_RESCAN);
          QUERY_CHECK_CODE(code, lino, _end);
          pExchangeInfo->phase2Rescan = false;  // one-shot: only the first fetch after phase-2 restart carries it
          break;
        }
        if (pDataInfo->pSrcUidList) {
          code = buildTableScanOperatorParam(&req.pOpParam,
                                             pDataInfo->pSrcUidList,
                                             pDataInfo->srcOpType,
                                             pDataInfo->tableSeq);
          /* source uid list can be reused in vnode size, so only use once */
          taosArrayDestroy(pDataInfo->pSrcUidList);
          pDataInfo->pSrcUidList = NULL;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        if (pExchangeInfo->notifyToSend) {
          if (NULL == req.pOpParam) {
            code = buildTableScanOperatorParamNotify(
                &req.pOpParam, pExchangeInfo->notifySrcOpType,
                pExchangeInfo->notifyGroupId,
                pExchangeInfo->notifyPhase,
                pExchangeInfo->notifyScanGroupId,
                pExchangeInfo->notifyTs);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            /**
              Reusing the same param multiple times is not supported!
            */
            qError("%s, %s failed, reusing the same param multiple times is not supported",
                   GET_TASKID(pTaskInfo), __func__);
            pTaskInfo->code = TSDB_CODE_INVALID_PARA;
            taosMemoryFree(pWrapper);
            return pTaskInfo->code;
          }
          pExchangeInfo->notifyToSend = false;
          qDebug("%s Exchange attached notify exchange:%p target:%s "
                 "outerEpoch:%" PRIu64 " phase:%d scanGroupId:%" PRIu64
                 " pending:0 source:%d remoteTask:0x%" PRIx64,
                 GET_TASKID(pTaskInfo), pExchangeInfo,
                 interpNotifyTargetOpName(pExchangeInfo->notifySrcOpType),
                 pExchangeInfo->notifyGroupId,
                 pExchangeInfo->notifyPhase,
                 pExchangeInfo->notifyScanGroupId, sourceIndex,
                 pSource->taskId);
        }
        break;
      }
    }

    int32_t msgSize = 0;
    void*   msg = NULL;
    if (pSource->fetchMsgType == TDMT_STREAM_FETCH && req.pStRtFuncInfo != NULL &&
        req.pStRtFuncInfo->isMultiGroupCalc && req.pStRtFuncInfo->pGroupReadInfos != NULL && req.pOpParam == NULL &&
        (req.reset || pDataInfo->readerFetchId != 0)) {
      if (req.reset) pDataInfo->readerFetchId = (uint64_t)tGenIdPI64();
      req.readerFetchId = pDataInfo->readerFetchId;
      SStreamRuntimeInfo* pRuntime = pTaskInfo->pStreamRuntimeInfo;
      if (req.reset && pRuntime != NULL && pRuntime->pPreparedFetchContext != NULL &&
          req.pStRtFuncInfo == &pRuntime->funcInfo) {
        const SStreamFetchContextSelection* pSelection = NULL;
        code = tSelectStreamFetchContextForNode(pRuntime->pPreparedFetchContext, &pRuntime->funcInfo, req.header.vgId,
                                                &pSelection);
        if (code == TSDB_CODE_SUCCESS) {
          code = tBuildStreamPreparedFetchReq(&req, &pRuntime->funcInfo, pSelection, &msg, &msgSize);
        }
      } else {
        code = tBuildStreamReaderFetchReq(&req, &msg, &msgSize);
      }
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        taosMemoryFree(pWrapper);
        freeOperatorParam(req.pOpParam, OP_GET_PARAM);
        return code;
      }
    } else if (pSource->fetchMsgType == TDMT_STREAM_FETCH && req.reset && req.pOpParam == NULL &&
               pTaskInfo->pStreamRuntimeInfo != NULL && pTaskInfo->pStreamRuntimeInfo->fetchInfo.active &&
               pTaskInfo->pStreamRuntimeInfo->pPreparedFetchContext != NULL &&
               pTaskInfo->pStreamRuntimeInfo->fetchInfo.pContextSelection != NULL &&
               req.pStRtFuncInfo == &pTaskInfo->pStreamRuntimeInfo->fetchInfo.funcInfo &&
               !req.pStRtFuncInfo->isMultiGroupCalc) {
      SStreamRuntimeInfo*      pRuntime = pTaskInfo->pStreamRuntimeInfo;
      code =
          tStreamFetchSelectionBelongsToPrepared(pRuntime->pPreparedFetchContext, pRuntime->fetchInfo.pContextSelection)
              ? TSDB_CODE_SUCCESS
              : TSDB_CODE_INVALID_PARA;
      if (code == TSDB_CODE_SUCCESS) {
        code = tBuildStreamPreparedFetchReq(&req, &pRuntime->funcInfo, pRuntime->fetchInfo.pContextSelection, &msg,
                                            &msgSize);
      }
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        taosMemoryFree(pWrapper);
        freeOperatorParam(req.pOpParam, OP_GET_PARAM);
        return code;
      }
    } else if (streamCanForwardRunnerContext(pTaskInfo->pStreamRuntimeInfo, req.pStRtFuncInfo, pSource->fetchMsgType,
                                             req.reset)) {
      code = tBuildStreamRunnerFetchReq(&req, pTaskInfo->pStreamRuntimeInfo->pContextFrames, &msg, &msgSize);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        taosMemoryFree(pWrapper);
        freeOperatorParam(req.pOpParam, OP_GET_PARAM);
        return code;
      }
    } else {
      msgSize = serializeStreamFetchReq(NULL, 0, &req, pSource->fetchMsgType, needStreamRtInfo, needStreamGrpInfo);
      if (msgSize < 0) {
        pTaskInfo->code = msgSize;
        taosMemoryFree(pWrapper);
        freeOperatorParam(req.pOpParam, OP_GET_PARAM);
        return pTaskInfo->code;
      }

      msg = taosMemoryCalloc(1, msgSize);
      if (NULL == msg) {
        pTaskInfo->code = terrno;
        taosMemoryFree(pWrapper);
        freeOperatorParam(req.pOpParam, OP_GET_PARAM);
        return pTaskInfo->code;
      }

      msgSize = serializeStreamFetchReq(msg, msgSize, &req, pSource->fetchMsgType, needStreamRtInfo, needStreamGrpInfo);
      if (msgSize < 0) {
        pTaskInfo->code = msgSize;
        taosMemoryFree(pWrapper);
        taosMemoryFree(msg);
        freeOperatorParam(req.pOpParam, OP_GET_PARAM);
        return pTaskInfo->code;
      }
    }

    freeOperatorParam(req.pOpParam, OP_GET_PARAM);

    qDebug("%s build fetch msg and send to vgId:%d, ep:%s, clientId:0x%" PRIx64 " taskId:0x%" PRIx64
           ", seqId:%" PRId64 ", execId:%d, %p, %d/%" PRIzu,
           GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->addr.epSet.eps[0].fqdn, pSource->clientId,
           pSource->taskId, pExchangeInfo->seqId, pSource->execId, pExchangeInfo, sourceIndex, totalSources);

    // send the fetch remote task result reques
    SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
    if (NULL == pMsgSendInfo) {
      taosMemoryFreeClear(msg);
      taosMemoryFree(pWrapper);
      qError("%s prepare message %d failed", GET_TASKID(pTaskInfo), (int32_t)sizeof(SMsgSendInfo));
      pTaskInfo->code = terrno;
      return pTaskInfo->code;
    }

    pMsgSendInfo->param = pWrapper;
    pMsgSendInfo->paramFreeFp = taosAutoMemoryFree;
    pMsgSendInfo->msgInfo.pData = msg;
    pMsgSendInfo->msgInfo.len = msgSize;
    pMsgSendInfo->msgType = pSource->fetchMsgType;
    pMsgSendInfo->fp = loadRemoteDataCallback;
    pMsgSendInfo->requestId = pTaskInfo->id.queryId;

    int64_t transporterId = 0;
    void* poolHandle = NULL;
    pDataInfo->startTime = taosGetTimestampUs();
    code = asyncSendMsgToServer(pExchangeInfo->pTransporter, &pSource->addr.epSet, &transporterId, pMsgSendInfo);
    QUERY_CHECK_CODE(code, lino, _end);
    int64_t* pRpcHandle = taosArrayGet(pExchangeInfo->pFetchRpcHandles, sourceIndex);
    *pRpcHandle = transporterId;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    if (pWrapper) {
      taosMemoryFree(pWrapper);
    }
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

/**
  @brief record the data loading metrics of the exchange operator, including
  the number of rows, the data length, and the elapsed time of current load operation.
*/
void updateLoadRemoteInfo(SLoadRemoteDataInfo* pInfo, int64_t numOfRows,
                          int32_t dataLen, int64_t startTs, SOperatorInfo* pOperator) {
  pInfo->totalRows += numOfRows;
  pInfo->totalSize += dataLen;
  pInfo->totalElapsed += (taosGetTimestampUs() - startTs);
}

int32_t extractDataBlockFromFetchRsp(SSDataBlock* pRes, char* pData, SArray* pColList, char** pNextStart, bool isVstbScan) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pBlock = NULL;
  if (isVstbScan) {
    blockDataCleanup(pRes);
    code = blockDecodeInternal(pRes, pData, (const char**)pNextStart);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pColList == NULL) {  // data from other sources
    blockDataCleanup(pRes);
    code = blockDecodeInternal(pRes, pData, (const char**)pNextStart);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {  // extract data according to pColList
    char* pStart = pData;

    int32_t numOfCols = htonl(*(int32_t*)pStart);
    pStart += sizeof(int32_t);

    // todo refactor:extract method
    SSysTableSchema* pSchema = (SSysTableSchema*)pStart;
    for (int32_t i = 0; i < numOfCols; ++i) {
      SSysTableSchema* p = (SSysTableSchema*)pStart;

      p->colId = htons(p->colId);
      p->bytes = htonl(p->bytes);
      pStart += sizeof(SSysTableSchema);
    }

    pBlock = NULL;
    code = createDataBlock(&pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData idata = createColumnInfoData(pSchema[i].type, pSchema[i].bytes, pSchema[i].colId);
      code = blockDataAppendColInfo(pBlock, &idata);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    code = blockDecodeInternal(pBlock, pStart, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    code = blockDataEnsureCapacity(pRes, pBlock->info.rows);
    QUERY_CHECK_CODE(code, lino, _end);

    // data from mnode
    pRes->info.dataLoad = 1;
    pRes->info.rows = pBlock->info.rows;
    pRes->info.scanFlag = pBlock->info.scanFlag;
    pRes->info.scanPhase = pBlock->info.scanPhase;
    pRes->info.id.groupId = pBlock->info.id.groupId;
    pRes->info.id.baseGId = pBlock->info.id.baseGId;
    code = relocateColumnData(pRes, pColList, pBlock->pDataBlock, false);
    QUERY_CHECK_CODE(code, lino, _end);

    blockDataDestroy(pBlock);
    pBlock = NULL;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void setAllSourcesCompleted(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;
  size_t               totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  qDebug("%s all %" PRIzu " sources are exhausted, total rows: %" PRIu64 ", %.2f Kb, elapsed:%.2f ms",
         GET_TASKID(pTaskInfo), totalSources, pLoadInfo->totalRows, pLoadInfo->totalSize / 1024.0,
         pLoadInfo->totalElapsed / 1000.0);

  setOperatorCompleted(pOperator);
}

int32_t getCompletedSources(const SArray* pArray, int32_t* pRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  total = taosArrayGetSize(pArray);

  int32_t completed = 0;
  for (int32_t k = 0; k < total; ++k) {
    SSourceDataInfo* p = taosArrayGet(pArray, k);
    QUERY_CHECK_NULL(p, code, lino, _end, terrno);
    if (p->status == EX_SOURCE_DATA_EXHAUSTED) {
      qDebug("source %d is completed, info:%p %p", k, pArray, p);
      completed += 1;
    }
  }

  *pRes = completed;
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t prepareConcurrentlyLoad(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSourceDataInfo);
  int64_t startTs = taosGetTimestampUs();

  // Asynchronously send all fetch requests to all sources.
  for (int32_t i = 0; i < totalSources; ++i) {
    int32_t code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, i);
    if (code != TSDB_CODE_SUCCESS) {
      pTaskInfo->code = code;
      return code;
    }
  }

  int64_t endTs = taosGetTimestampUs();
  qDebug("%s send all fetch requests to %" PRIzu " sources completed, elapsed:%.2fms", GET_TASKID(pTaskInfo),
         totalSources, (endTs - startTs) / 1000.0);

  pOperator->status = OP_RES_TO_RETURN;
  if (isTaskKilled(pTaskInfo)) {
    T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
  }

  return TSDB_CODE_SUCCESS;
}

/**
  @brief store STEP DONE notification info
*/
int32_t storeExchangeNotifyInfo(SOperatorInfo* pOperator) {
  SExchangeInfo*  pExchangeInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SOperatorParam* pGetParam = pOperator->pOperatorGetParam;

  if (pGetParam == NULL ||
      pGetParam->opType != QUERY_NODE_PHYSICAL_PLAN_EXCHANGE ||
      pGetParam->value == NULL ||
      pOperator->pPhyNode == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SExchangeOperatorParam* pParam = (SExchangeOperatorParam*)pGetParam->value;
  if (pParam->multiParams) {
    qWarn("%s, %s found multi params are not supported for notify msg",
           GET_TASKID(pTaskInfo), __func__);
    return TSDB_CODE_INVALID_PARA;
  }

  SExchangeOperatorBasicParam* pBasic = &pParam->basic;
  if (pBasic->paramType != NOTIFY_TYPE_EXCHANGE_PARAM ||
      (pBasic->srcOpType != QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN &&
       pBasic->srcOpType !=
           QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) ||
      pBasic->notifyPhase < SCAN_PHASE_MAIN ||
      pBasic->notifyPhase > SCAN_PHASE_PREV) {
    qWarn("%s, %s found invalid exchange notify param, type:%d "
          "srcOpType:%d phase:%d",
          GET_TASKID(pTaskInfo), __func__, pBasic->paramType,
          pBasic->srcOpType, pBasic->notifyPhase);
    return TSDB_CODE_INVALID_PARA;
  }

  SExchangePhysiNode* pExchangeNode =
      (SExchangePhysiNode*)pOperator->pPhyNode;
  if (pBasic->srcOpType ==
          QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN &&
      (!pExchangeNode->singleSrc ||
       taosArrayGetSize(pExchangeInfo->pSources) != 1)) {
    qWarn("%s, %s requires one exchange source, singleSrc:%d sources:%d",
          GET_TASKID(pTaskInfo), __func__, pExchangeNode->singleSrc,
          (int32_t)taosArrayGetSize(pExchangeInfo->pSources));
    return TSDB_CODE_INVALID_PARA;
  }

  pExchangeInfo->notifyToSend = true;
  pExchangeInfo->notifySrcOpType = pBasic->srcOpType;
  pExchangeInfo->notifyGroupId = pBasic->groupid;
  pExchangeInfo->notifyPhase = pBasic->notifyPhase;
  pExchangeInfo->notifyScanGroupId = pBasic->notifyScanGroupId;
  pExchangeInfo->notifyTs = pBasic->notifyTs;
  qDebug("%s Exchange stored notify exchange:%p target:%s "
         "outerEpoch:%" PRIu64 " phase:%d scanGroupId:%" PRIu64
         " pending:1",
         GET_TASKID(pTaskInfo), pExchangeInfo,
         interpNotifyTargetOpName(pExchangeInfo->notifySrcOpType),
         pExchangeInfo->notifyGroupId,
         pExchangeInfo->notifyPhase,
         pExchangeInfo->notifyScanGroupId);
  return TSDB_CODE_SUCCESS;
}

static int32_t decodeStrictMultiGroupBatch(SOperatorInfo* pOperator, SSourceDataInfo* pSource, int32_t sourceIndex,
                                           EExchangeFetchResultType* pResultType, bool* pOutputQueued) {
  int32_t                   code = TSDB_CODE_SUCCESS, lino = 0;
  SExchangeInfo*            pInfo = pOperator->info;
  SRetrieveTableRsp*        pRsp = pSource->pRsp;
  SStreamRunnerFetchBatch   batch = {0};
  SExchangeMultiGroupResult result = {0};
  uint64_t                  expectedGid = 0;
  int32_t                   groupCount = 0;

  if (pSource->currentEogSeen || pSource->status == EX_SOURCE_DATA_EXHAUSTED || pRsp->completed != 0 ||
      pRsp->compressed != 0 || pRsp->payloadLen <= 0 || pRsp->compLen != pRsp->payloadLen ||
      pRsp->payloadLen != pSource->rspLen - (int32_t)sizeof(*pRsp)) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
  TAOS_CHECK_EXIT(tDeserializeSStreamRunnerFetchBatch(pRsp->data, pRsp->payloadLen, &batch));
  TAOS_CHECK_EXIT(getStrictCanonicalGroup(pOperator, pInfo, &expectedGid, &groupCount));
  const SArray* pOrder = GET_STM_RTINFO(pOperator->pTaskInfo)->pGroupCalcOrder;
  int32_t       eogCount = taosArrayGetSize(batch.pEogGids);
  if (eogCount > groupCount - pInfo->nextGroupIdx) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  for (int32_t i = 0; i < eogCount; ++i) {
    if (*(uint64_t*)taosArrayGet(batch.pEogGids, i) != *(uint64_t*)taosArrayGet(pOrder, pInfo->nextGroupIdx + i)) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
  }

  if (batch.pBlock != NULL) {
    if (batch.dataFrame.baseGId != expectedGid || batch.pBlock->info.id.groupId != expectedGid ||
        pRsp->numOfRows != batch.pBlock->info.rows || pRsp->numOfBlocks != 1 ||
        pRsp->numOfCols != taosArrayGetSize(batch.pBlock->pDataBlock)) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    TAOS_CHECK_EXIT(adoptReaderBlockDescriptor(batch.pBlock, pInfo->pDummyBlock));
    result.type = STREAM_RUNNER_FETCH_DATA;
    result.baseGId = expectedGid;
    result.pBlock = batch.pBlock;
    batch.pBlock = NULL;
    result.pLineage = batch.dataFrame.pLineage;
    batch.dataFrame.pLineage = NULL;
    *pResultType = EXCHANGE_FETCH_RESULT_DATA;
    TAOS_CHECK_EXIT(
        acceptStrictMultiGroupFetchResult(pOperator, pInfo, sourceIndex, *pResultType, &result, pOutputQueued));
  } else {
    if (pRsp->numOfRows != 0 || pRsp->numOfCols != 0 || pRsp->numOfBlocks != 0) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
    *pResultType = EXCHANGE_FETCH_RESULT_EOG;
  }
  // DATA is now queued. Its trailing EOGs are applied only by a later load, after DATA is returned.
  pSource->pRunnerEogs = batch.pEogGids;
  batch.pEogGids = NULL;
  pSource->runnerEogIdx = 0;

_exit:
  destroyExchangeMultiGroupResult(&result);
  tDestroySStreamRunnerFetchBatch(&batch);
  return code;
}

static int32_t decodeStrictMultiGroupFetchResponse(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo,
                                                       SSourceDataInfo* pDataInfo, int32_t sourceIndex,
                                                       EExchangeFetchResultType* pResultType, bool* pOutputQueued) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  SRetrieveTableRsp*        pRsp = pDataInfo->pRsp;
  SStreamRunnerFetchFrame   frame = {0};
  SExchangeMultiGroupResult result = {0};
  *pResultType = EXCHANGE_FETCH_RESULT_NONE;
  *pOutputQueued = false;

  bool strictGate = isStrictMultiGroupRunnerSource(pOperator, sourceIndex);
  bool strictType = pRsp != NULL && isStrictRunnerFetchType(pRsp->streamBlockType);
  if (!strictGate)
    return pRsp != NULL && hasRunnerFetchFrame(pRsp->streamBlockType) ? TSDB_CODE_INVALID_MSG : TSDB_CODE_SUCCESS;
  if (pRsp == NULL || pDataInfo->rspLen < (int32_t)sizeof(SRetrieveTableRsp) || pDataInfo->pRunnerEogs != NULL) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  if (pRsp->streamBlockType == STREAM_RUNNER_FETCH_BATCH) {
    code = decodeStrictMultiGroupBatch(pOperator, pDataInfo, sourceIndex, pResultType, pOutputQueued);
    goto _exit;
  }

  if (pRsp->streamBlockType == STREAM_RUNNER_FETCH_LEGACY) {
    if (pRsp->completed != 1 || pRsp->numOfRows != 0 || pRsp->numOfBlocks != 0 || pRsp->numOfCols != 0 ||
        pRsp->payloadLen != 0 || pRsp->compLen != 0 ||
        pDataInfo->rspLen != (int32_t)sizeof(SRetrieveTableRsp) + INT_BYTES * 2) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
    int32_t compSize = 0;
    int32_t rawSize = 0;
    memcpy(&compSize, pRsp->data, INT_BYTES);
    memcpy(&rawSize, pRsp->data + INT_BYTES, INT_BYTES);
    if (compSize != 0 || rawSize != 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
    *pResultType = EXCHANGE_FETCH_RESULT_EOF;
    code = acceptStrictMultiGroupFetchResult(pOperator, pExchangeInfo, sourceIndex, *pResultType, NULL, pOutputQueued);
    goto _exit;
  }
  if (!strictType || pRsp->completed != 0 || pRsp->compressed != 0 || pRsp->payloadLen < INT_BYTES * 2 ||
      pRsp->compLen != pRsp->payloadLen || pRsp->payloadLen > pDataInfo->rspLen - (int32_t)sizeof(SRetrieveTableRsp) ||
      pDataInfo->rspLen != (int32_t)sizeof(SRetrieveTableRsp) + pRsp->payloadLen) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  int32_t blockSize = 0;
  int32_t rawSize = 0;
  memcpy(&blockSize, pRsp->data, INT_BYTES);
  memcpy(&rawSize, pRsp->data + INT_BYTES, INT_BYTES);
  if (blockSize < 0 || rawSize < 0 || blockSize != rawSize || blockSize > pRsp->payloadLen - INT_BYTES * 2) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  const int32_t frameOffset = INT_BYTES * 2 + blockSize;
  const int32_t frameLen = pRsp->payloadLen - frameOffset;
  if (frameLen <= 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }
  code = tDeserializeSStreamRunnerFetchFrame(pRsp->data + frameOffset, frameLen, &frame);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  if ((int8_t)frame.type != pRsp->streamBlockType) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  result.type = frame.type;
  result.baseGId = frame.baseGId;
  if (frame.type == STREAM_RUNNER_FETCH_DATA) {
    if (pRsp->numOfRows <= 0 || pRsp->numOfBlocks != 1 || blockSize == 0 || pRsp->numOfCols <= 0 ||
        pRsp->numOfCols > INT32_MAX) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
    code = acquireExchangeResultBlock(pExchangeInfo, &result.pBlock);
    if (code != TSDB_CODE_SUCCESS) goto _exit;

    blockDataCleanup(result.pBlock);
    const char* pDecodedEnd = NULL;
    code = blockDecodeInternalBounded(result.pBlock, pRsp->data + INT_BYTES * 2, (size_t)blockSize, &pDecodedEnd);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    if (pDecodedEnd != pRsp->data + frameOffset || result.pBlock->info.rows != pRsp->numOfRows ||
        taosArrayGetSize(result.pBlock->pDataBlock) != pRsp->numOfCols ||
        result.pBlock->info.id.baseGId != frame.baseGId || result.pBlock->info.id.groupId != frame.baseGId) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
    code = tValidateSStreamRunnerFetchFrame(&frame, result.pBlock->info.rows);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    result.pLineage = frame.pLineage;
    frame.pLineage = NULL;
    *pResultType = EXCHANGE_FETCH_RESULT_DATA;
  } else if (frame.type == STREAM_RUNNER_FETCH_EOG) {
    if (pRsp->numOfRows != 0 || pRsp->numOfBlocks != 0 || pRsp->numOfCols != 0 || blockSize != 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
    code = tValidateSStreamRunnerFetchFrame(&frame, 0);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    *pResultType = EXCHANGE_FETCH_RESULT_EOG;
  } else {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  code = acceptStrictMultiGroupFetchResult(pOperator, pExchangeInfo, sourceIndex, *pResultType, &result, pOutputQueued);

_exit:
  if (code == TSDB_CODE_SUCCESS && *pResultType == EXCHANGE_FETCH_RESULT_DATA &&
      pOperator->pTaskInfo->pStreamRuntimeInfo->inputStatsFp != NULL) {
    pOperator->pTaskInfo->pStreamRuntimeInfo->inputStatsFp(pOperator->pTaskInfo->pStreamRuntimeInfo->pInputStatsParam,
                                                           pRsp->numOfRows, 1);
  }

  tDestroySStreamRunnerFetchFrame(&frame);
  destroyExchangeMultiGroupResult(&result);
  if (code != TSDB_CODE_SUCCESS) {
    clearExchangeResultState(pOperator, pExchangeInfo);
    resetStrictMultiGroupBarrier(pExchangeInfo);
    *pResultType = EXCHANGE_FETCH_RESULT_NONE;
    *pOutputQueued = false;
  }
  return code;
}

int32_t doExtractResultBlocks(SExchangeInfo* pExchangeInfo, SSourceDataInfo* pDataInfo,
                              SStreamRuntimeInfo* pStreamRuntimeInfo) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;
  SSDataBlock*       pb = NULL;

  if (pRetrieveRsp == NULL || pDataInfo->rspLen < (int32_t)sizeof(SRetrieveTableRsp)) {
    return TSDB_CODE_INVALID_MSG;
  }
  int32_t availablePayload = pDataInfo->rspLen - (int32_t)sizeof(SRetrieveTableRsp);
  int32_t declaredPayload = pRetrieveRsp->compLen > 0 ? pRetrieveRsp->compLen : availablePayload;
  if (declaredPayload < 0 || declaredPayload > availablePayload) return TSDB_CODE_INVALID_MSG;

  char* pNextStart = pRetrieveRsp->data;
  char* pStart = pNextStart;
  char* pPayloadEnd = pRetrieveRsp->data + declaredPayload;

  int32_t index = 0;

  if (pRetrieveRsp->compressed) {  // decompress the data
    if (pDataInfo->decompBuf == NULL) {
      pDataInfo->decompBuf = taosMemoryMalloc(pRetrieveRsp->payloadLen);
      QUERY_CHECK_NULL(pDataInfo->decompBuf, code, lino, _end, terrno);
      pDataInfo->decompBufSize = pRetrieveRsp->payloadLen;
    } else {
      if (pDataInfo->decompBufSize < pRetrieveRsp->payloadLen) {
        char* p = taosMemoryRealloc(pDataInfo->decompBuf, pRetrieveRsp->payloadLen);
        QUERY_CHECK_NULL(p, code, lino, _end, terrno);
        if (p != NULL) {
          pDataInfo->decompBuf = p;
          pDataInfo->decompBufSize = pRetrieveRsp->payloadLen;
        }
      }
    }
  }

  while (index++ < pRetrieveRsp->numOfBlocks) {
    pStart = pNextStart;

    if (pPayloadEnd - pStart < INT_BYTES * 2) {
      code = TSDB_CODE_INVALID_MSG;
      goto _end;
    }

    if (pDataInfo->type == EX_SRC_TYPE_VSTB_SCAN || pDataInfo->type == EX_SRC_TYPE_VSTB_TAG_SCAN) {
      pb = taosMemoryCalloc(1, sizeof(SSDataBlock));
      QUERY_CHECK_NULL(pb, code, lino, _end, terrno);
    } else if (taosArrayGetSize(pExchangeInfo->pRecycledBlocks) > 0) {
      pb = *(SSDataBlock**)taosArrayPop(pExchangeInfo->pRecycledBlocks);
      blockDataCleanup(pb);
    } else {
      code = createOneDataBlock(pExchangeInfo->pDummyBlock, false, &pb);
      QUERY_CHECK_NULL(pb, code, lino, _end, code);
    }

    int32_t compLen = 0;
    memcpy(&compLen, pStart, INT_BYTES);
    pStart += sizeof(int32_t);

    int32_t rawLen = 0;
    memcpy(&rawLen, pStart, INT_BYTES);
    pStart += sizeof(int32_t);
    QUERY_CHECK_CONDITION((compLen > 0 && compLen <= rawLen && compLen <= pPayloadEnd - pStart), code, lino, _end,
                          TSDB_CODE_INVALID_MSG);

    pNextStart = pStart + compLen;
    if (pRetrieveRsp->compressed && (compLen < rawLen)) {
      int32_t t = tsDecompressString(pStart, compLen, 1, pDataInfo->decompBuf, rawLen, ONE_STAGE_COMP, NULL, 0);
      QUERY_CHECK_CONDITION((t == rawLen), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
      pStart = pDataInfo->decompBuf;
    }

    bool isVstb = (pDataInfo->type == EX_SRC_TYPE_VSTB_SCAN || pDataInfo->type == EX_SRC_TYPE_VSTB_TAG_SCAN);
    code = extractDataBlockFromFetchRsp(pb, pStart, NULL, &pStart, isVstb);
    if (code != 0) {
      clearSourceResponse(pDataInfo);
      goto _end;
    }
    if (pExchangeInfo->fastInterval) {
      if (pDataInfo->index < 0) {
        qError("FAST_INTERVAL invalid exchange source index:%d", pDataInfo->index);
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        goto _end;
      }
      pb->info.id.uid = (uint64_t)pDataInfo->index;
    }

    void* tmp = taosArrayPush(pExchangeInfo->pResultBlockList, &pb);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    qDebug("%dth block added to resultBlockList, rows:%" PRId64, index, pb->info.rows);
    if (pStreamRuntimeInfo != NULL && pStreamRuntimeInfo->inputStatsFp != NULL) {
      pStreamRuntimeInfo->inputStatsFp(pStreamRuntimeInfo->pInputStatsParam, pb->info.rows, 1);
    }
    pb = NULL;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pb);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t qExchangeRequestReaderGroupOutput(SOperatorInfo* pOperator) {
  if (pOperator == NULL || pOperator->info == NULL || pOperator->pPhyNode == NULL ||
      nodeType(pOperator->pPhyNode) != QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
    return TSDB_CODE_INVALID_PARA;
  }
  SExchangeInfo* pInfo = pOperator->info;
  if (pInfo->readerGroupRequested) return TSDB_CODE_SUCCESS;
  pInfo->readerGroupMode = 1;
  SExecTaskInfo* pTask = pOperator->pTaskInfo;
  if (pTask == NULL || pTask->execModel != OPTR_EXEC_MODEL_STREAM || pTask->pStreamRuntimeInfo == NULL ||
      !GET_STM_RTINFO(pTask)->isMultiGroupCalc || GET_STM_RTINFO(pTask)->pGroupReadInfos == NULL || pInfo->dynamicOp ||
      pOperator->pOperatorGetParam != NULL || hasLimitOffsetInfo(&pInfo->limitInfo)) {
    return TSDB_CODE_SUCCESS;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSources); ++i) {
    const SDownstreamSourceNode* pSource = taosArrayGet(pInfo->pSources, i);
    const SSourceDataInfo*       pData = taosArrayGet(pInfo->pSourceDataInfo, i);
    if (pSource == NULL || pData == NULL || pSource->fetchMsgType != TDMT_STREAM_FETCH || pSource->localExec ||
        pData->fetchSent) {
      return TSDB_CODE_SUCCESS;
    }
  }
  pInfo->readerGroupRequested = true;
  pInfo->readerGroupMode = 0;
  return TSDB_CODE_SUCCESS;
}

int32_t qExchangeReaderGroupOutputMode(const SOperatorInfo* pOperator) {
  if (pOperator == NULL || pOperator->info == NULL || pOperator->pPhyNode == NULL ||
      nodeType(pOperator->pPhyNode) != QUERY_NODE_PHYSICAL_PLAN_EXCHANGE)
    return 1;
  return ((const SExchangeInfo*)pOperator->info)->readerGroupMode;
}

static int32_t compareReaderGid(const void* a, const void* b) {
  uint64_t left = *(const uint64_t*)a, right = *(const uint64_t*)b;
  return (left > right) - (left < right);
}

static int32_t initReaderGroupSources(SOperatorInfo* pOperator) {
  SExchangeInfo* pInfo = pOperator->info;
  if (pInfo->readerGroupsInitialized) return TSDB_CODE_SUCCESS;
  SStreamRuntimeFuncInfo* pStream = GET_STM_RTINFO(pOperator->pTaskInfo);
  const SArray*           pOrder = pStream->pGroupCalcOrder;
  if (taosArrayGetSize(pOrder) == 0 || pStream->pGroupReadInfos == NULL ||
      taosArrayGetSize(pInfo->pSources) != taosArrayGetSize(pInfo->pSourceDataInfo))
    return TSDB_CODE_INVALID_MSG;
  uint64_t previous = 0;
  for (int32_t g = 0; g < taosArrayGetSize(pOrder); ++g) {
    uint64_t gid = *(uint64_t*)taosArrayGet(pOrder, g);
    if (gid <= previous) return TSDB_CODE_INVALID_MSG;
    previous = gid;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSources); ++i) {
    const SDownstreamSourceNode* pSource = taosArrayGet(pInfo->pSources, i);
    SSourceDataInfo*             pData = taosArrayGet(pInfo->pSourceDataInfo, i);
    SArray** ppReads = tSimpleHashGet(pStream->pGroupReadInfos, &pSource->addr.nodeId, sizeof(pSource->addr.nodeId));
    pData->pReaderGroups = taosArrayInit(1, sizeof(uint64_t));
    if (pData->pReaderGroups == NULL) return terrno;
    for (int32_t g = 0; ppReads != NULL && g < taosArrayGetSize(*ppReads); ++g) {
      const SSTriggerGroupReadInfo* pRead = taosArrayGet(*ppReads, g);
      uint64_t                      gid = (uint64_t)pRead->gid;
      if (gid == 0) return TSDB_CODE_INVALID_MSG;
      if (taosArrayPush(pData->pReaderGroups, &gid) == NULL) return terrno;
    }
    taosArraySort(pData->pReaderGroups, compareReaderGid);
    taosArrayRemoveDuplicate(pData->pReaderGroups, compareReaderGid, NULL);
    int32_t canonical = 0;
    for (int32_t g = 0; g < taosArrayGetSize(pData->pReaderGroups); ++g) {
      uint64_t gid = *(uint64_t*)taosArrayGet(pData->pReaderGroups, g);
      while (canonical < taosArrayGetSize(pOrder) && *(uint64_t*)taosArrayGet(pOrder, canonical) < gid) ++canonical;
      if (canonical == taosArrayGetSize(pOrder) || *(uint64_t*)taosArrayGet(pOrder, canonical) != gid) {
        return TSDB_CODE_INVALID_MSG;
      }
    }
    if (taosArrayGetSize(pData->pReaderGroups) == 0) {
      pData->readerMode = 2;
      pData->readerEof = true;
      pData->status = EX_SOURCE_DATA_EXHAUSTED;
    }
  }
  pInfo->readerGroupsInitialized = true;
  return TSDB_CODE_SUCCESS;
}

static void finishReaderBatch(SSourceDataInfo* pData) {
  if (pData->readerItemIdx < taosArrayGetSize(pData->pReaderItems)) return;
  taosArrayDestroyEx(pData->pReaderItems, tDestroyStreamReaderFetchItem);
  pData->pReaderItems = NULL;
  pData->readerItemIdx = 0;
  pData->status = pData->readerEof ? EX_SOURCE_DATA_EXHAUSTED : EX_SOURCE_DATA_NOT_READY;
}

static int32_t adoptReaderBlockDescriptor(SSDataBlock* pBlock, const SSDataBlock* pTemplate) {
  int32_t columns = taosArrayGetSize(pBlock->pDataBlock);
  int32_t expectedColumns = taosArrayGetSize(pTemplate->pDataBlock);
  if (columns > expectedColumns) return TSDB_CODE_INVALID_MSG;
  for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
    SColumnInfoData*       pColumn = taosArrayGet(pBlock->pDataBlock, i);
    const SColumnInfoData* pExpected = taosArrayGet(pTemplate->pDataBlock, i);
    if (pColumn->info.type != pExpected->info.type || pColumn->info.bytes != pExpected->info.bytes ||
        (IS_DECIMAL_TYPE(pColumn->info.type) &&
         (pColumn->info.precision != pExpected->info.precision || pColumn->info.scale != pExpected->info.scale))) {
      return TSDB_CODE_INVALID_MSG;
    }
  }
  // Remote output omits trailing slots reserved for expressions evaluated locally.
  // Legacy decoding retains these slots in its template-created destination block.
  for (int32_t i = columns; i < expectedColumns; ++i) {
    const SColumnInfoData* pExpected = taosArrayGet(pTemplate->pDataBlock, i);
    if (!pExpected->info.noData) return TSDB_CODE_INVALID_MSG;
    SColumnInfoData column = {.hasNull = true, .info = pExpected->info};
    int32_t         code = blockDataAppendColInfo(pBlock, &column);
    if (code != TSDB_CODE_SUCCESS) return code;
    SColumnInfoData* pColumn = taosArrayGet(pBlock->pDataBlock, i);
    code = colInfoDataEnsureCapacity(pColumn, pBlock->info.capacity, false);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  // Internal encoding carries group identity and slot ids, but not the local output block id.
  pBlock->info.id.blockId = pTemplate->info.id.blockId;
  pBlock->info.rowSize = pTemplate->info.rowSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t acceptReaderFetchResponse(SOperatorInfo* pOperator, SSourceDataInfo* pData) {
  SExchangeInfo*     pInfo = pOperator->info;
  SRetrieveTableRsp* pRsp = pData->pRsp;
  SArray*            pItems = NULL;
  int32_t            code = TSDB_CODE_SUCCESS, lino = 0;
  TAOS_CHECK_EXIT(initReaderGroupSources(pOperator));
  if (pData->readerEof || pData->pReaderItems != NULL || pRsp == NULL) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  bool    grouped = pRsp->streamBlockType == STREAM_READER_FETCH_BATCH;
  int32_t mode = grouped ? 2 : 1;
  if ((pData->readerMode != 0 && pData->readerMode != mode) ||
      (!grouped && pRsp->streamBlockType != STREAM_RUNNER_FETCH_LEGACY))
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  if (grouped) {
    int32_t bodyLen = pData->rspLen - (int32_t)sizeof(*pRsp);
    if (pRsp->compressed || pRsp->payloadLen != bodyLen || pRsp->compLen != bodyLen || pRsp->numOfCols != 0 ||
        pRsp->completed > 1)
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    TAOS_CHECK_EXIT(tDecodeStreamReaderFetchBatch(pRsp->data, bodyLen, &pItems));
    int64_t rows = 0;
    int32_t blocks = 0, next = pData->readerValidatedIdx;
    for (int32_t i = 0; i < taosArrayGetSize(pItems); ++i) {
      const SStreamReaderFetchItem* pItem = taosArrayGet(pItems, i);
      if (next >= taosArrayGetSize(pData->pReaderGroups) ||
          pItem->baseGId != *(uint64_t*)taosArrayGet(pData->pReaderGroups, next)) {
        TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
      }
      if (pItem->type == STREAM_RUNNER_FETCH_EOG) {
        ++next;
      } else {
        TAOS_CHECK_EXIT(adoptReaderBlockDescriptor(pItem->pBlock, pInfo->pDummyBlock));
        if (pItem->pBlock->info.rows > INT64_MAX - rows) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
        rows += pItem->pBlock->info.rows;
        ++blocks;
      }
    }
    if (rows != pRsp->numOfRows || blocks != pRsp->numOfBlocks ||
        (pRsp->completed && next != taosArrayGetSize(pData->pReaderGroups)))
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    pData->readerValidatedIdx = next;
    SStreamRuntimeInfo* pRuntime = pOperator->pTaskInfo->pStreamRuntimeInfo;
    if (pRuntime->inputStatsFp != NULL) pRuntime->inputStatsFp(pRuntime->pInputStatsParam, rows, blocks);
  } else {
    pItems = taosArrayInit(1, sizeof(SStreamReaderFetchItem));
    if (pItems == NULL) TAOS_CHECK_EXIT(terrno);
    if (pRsp->numOfRows > 0) {
      TAOS_CHECK_EXIT(doExtractResultBlocks(pInfo, pData, pOperator->pTaskInfo->pStreamRuntimeInfo));
      while (taosArrayGetSize(pInfo->pResultBlockList) > 0) {
        SSDataBlock*           pBlock = taosArrayGetP(pInfo->pResultBlockList, 0);
        SStreamReaderFetchItem item = {
            .type = STREAM_RUNNER_FETCH_DATA, .baseGId = pBlock->info.id.baseGId, .pBlock = pBlock};
        if (pBlock->info.rows <= 0) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
        if (taosArrayPush(pItems, &item) == NULL) TAOS_CHECK_EXIT(terrno);
        taosArrayRemove(pInfo->pResultBlockList, 0);
      }
    } else if (pRsp->numOfBlocks != 0) {
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    }
  }
  pData->readerMode = mode;
  pData->readerEof = pRsp->completed;
  pData->pReaderItems = pItems;
  pItems = NULL;
  finishReaderBatch(pData);

_exit:
  if (code != TSDB_CODE_SUCCESS) qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  taosArrayDestroyEx(pItems, tDestroyStreamReaderFetchItem);
  clearSourceResponse(pData);
  if (code != TSDB_CODE_SUCCESS) clearExchangeResultState(pOperator, pInfo);
  return code;
}

// Produces at most one block. Later-group events remain in their source's bounded batch.
static int32_t consumeReaderGroups(SOperatorInfo* pOperator) {
  SExchangeInfo* pInfo = pOperator->info;
  if (taosArrayGetSize(pInfo->pMultiGroupResultList) > 0) return TSDB_CODE_SUCCESS;
  int32_t code = initReaderGroupSources(pOperator);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (pInfo->readerGroupMode == 0) {
    int32_t mode = 2;
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSourceDataInfo); ++i) {
      const SSourceDataInfo* pData = taosArrayGet(pInfo->pSourceDataInfo, i);
      if (pData->readerMode == 0) return TSDB_CODE_SUCCESS;
      if (pData->readerMode == 1) mode = 1;
    }
    pInfo->readerGroupMode = mode;
  }
  const SArray* pOrder = GET_STM_RTINFO(pOperator->pTaskInfo)->pGroupCalcOrder;
  bool          grouped = pInfo->readerGroupMode == 2;
  uint64_t      gid = pInfo->readerNextGroupIdx < taosArrayGetSize(pOrder)
                          ? *(uint64_t*)taosArrayGet(pOrder, pInfo->readerNextGroupIdx)
                          : 0;
  bool          allEnded = true;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSourceDataInfo); ++i) {
    SSourceDataInfo* pData = taosArrayGet(pInfo->pSourceDataInfo, i);
    while (pData->readerItemIdx < taosArrayGetSize(pData->pReaderItems)) {
      SStreamReaderFetchItem* pItem = taosArrayGet(pData->pReaderItems, pData->readerItemIdx);
      if (grouped && (gid == 0 || pItem->baseGId > gid)) break;
      if (grouped && pItem->baseGId != gid) return TSDB_CODE_INVALID_MSG;
      if (pItem->type == STREAM_RUNNER_FETCH_DATA) {
        SExchangeMultiGroupResult result = {
            .type = STREAM_READER_FETCH_BATCH, .baseGId = pItem->baseGId, .pBlock = pItem->pBlock};
        code = enqueueExchangeMultiGroupResult(pInfo, &result);
        if (code != TSDB_CODE_SUCCESS) return code;
        pItem->pBlock = NULL;
        ++pData->readerItemIdx;
        finishReaderBatch(pData);
        return TSDB_CODE_SUCCESS;
      }
      ++pData->readerConsumedIdx;
      ++pData->readerItemIdx;
    }
    finishReaderBatch(pData);
    if (grouped && pData->readerConsumedIdx < taosArrayGetSize(pData->pReaderGroups) &&
        *(uint64_t*)taosArrayGet(pData->pReaderGroups, pData->readerConsumedIdx) == gid)
      allEnded = false;
  }
  if (grouped && gid != 0 && allEnded) {
    SExchangeMultiGroupResult result = {.type = STREAM_RUNNER_FETCH_EOG, .baseGId = gid};
    code = createDataBlock(&result.pBlock);
    if (code == TSDB_CODE_SUCCESS) code = qSetMultiGroupEogBlock(result.pBlock, gid);
    if (code == TSDB_CODE_SUCCESS) code = enqueueExchangeMultiGroupResult(pInfo, &result);
    destroyExchangeMultiGroupResult(&result);
    if (code != TSDB_CODE_SUCCESS) return code;
    ++pInfo->readerNextGroupIdx;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t loadReaderGroupData(SOperatorInfo* pOperator) {
  SExchangeInfo* pInfo = pOperator->info;
  SExecTaskInfo* pTask = pOperator->pTaskInfo;
  int32_t        code = TSDB_CODE_SUCCESS;
  while (true) {
    if (isTaskKilled(pTask)) return pTask->code;
    code = consumeReaderGroups(pOperator);
    if (code != TSDB_CODE_SUCCESS || taosArrayGetSize(pInfo->pMultiGroupResultList) > 0) return code;
    int32_t       source = -1;
    const SArray* pOrder = GET_STM_RTINFO(pTask)->pGroupCalcOrder;
    uint64_t      gid = pInfo->readerNextGroupIdx < taosArrayGetSize(pOrder)
                            ? *(uint64_t*)taosArrayGet(pOrder, pInfo->readerNextGroupIdx)
                            : 0;
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSourceDataInfo); ++i) {
      SSourceDataInfo* pData = taosArrayGet(pInfo->pSourceDataInfo, i);
      if (pData->readerEof || pData->pReaderItems != NULL) continue;
      if (pInfo->readerGroupMode == 0 && pData->readerMode != 0) continue;
      if (pInfo->readerGroupMode == 2 && gid != 0 &&
          (pData->readerConsumedIdx == taosArrayGetSize(pData->pReaderGroups) ||
           *(uint64_t*)taosArrayGet(pData->pReaderGroups, pData->readerConsumedIdx) != gid))
        continue;
      source = i;
      break;
    }
    if (source < 0) {
      int32_t completed = 0;
      code = getCompletedSources(pInfo->pSourceDataInfo, &completed);
      if (code != TSDB_CODE_SUCCESS) return code;
      if (completed != taosArrayGetSize(pInfo->pSourceDataInfo)) return TSDB_CODE_INVALID_MSG;
      setAllSourcesCompleted(pOperator);
      return TSDB_CODE_SUCCESS;
    }
    SSourceDataInfo* pData = taosArrayGet(pInfo->pSourceDataInfo, source);
    pData->status = EX_SOURCE_DATA_NOT_READY;
    int64_t started = taosGetTimestampUs();
    code = doSendFetchDataRequest(pInfo, pTask, source);
    if (code != TSDB_CODE_SUCCESS) return code;
    if (pData->status == EX_SOURCE_DATA_EXHAUSTED) return TSDB_CODE_INVALID_MSG;
    while (true) {
      recordOpExecBeforeDownstream(pOperator);
      code = exchangeWait(pOperator, pInfo);
      recordOpExecAfterDownstream(pOperator, 0);
      if (code != TSDB_CODE_SUCCESS) return code;
      if (isTaskKilled(pTask)) return pTask->code;
      if (pData->seqId == atomic_load_64(&pInfo->seqId)) break;
      clearSourceResponse(pData);
    }
    if (pData->code != TSDB_CODE_SUCCESS) return pData->code;
    if (pData->pRsp == NULL) return TSDB_CODE_INVALID_MSG;
    updateLoadRemoteInfo(&pInfo->loadInfo, pData->pRsp->numOfRows, pData->pRsp->compLen, started, pOperator);
    pData->totalRows += pData->pRsp->numOfRows;
    code = acceptReaderFetchResponse(pOperator, pData);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
}

#if defined(BUILD_TEST)
/* Borrow the execution and exercise the real sender without Reader teardown. */
int32_t exchangeTestSendRunnerFetch(SExecTaskInfo* pTask, SDownstreamSourceNode* pSource, bool fetchSent) {
  SExchangeInfo   exchange = {0};
  SSourceDataInfo data = {
      .index = 0, .type = EX_SRC_TYPE_STB_JOIN_SCAN, .fetchSent = fetchSent, .status = EX_SOURCE_DATA_NOT_READY};
  int64_t rpcHandle = 0;
  int32_t code = TSDB_CODE_OUT_OF_MEMORY;
  exchange.pSources = taosArrayInit(1, sizeof(*pSource));
  exchange.pSourceDataInfo = taosArrayInit(1, sizeof(data));
  exchange.pFetchRpcHandles = taosArrayInit(1, sizeof(rpcHandle));
  if (exchange.pSources != NULL && exchange.pSourceDataInfo != NULL && exchange.pFetchRpcHandles != NULL &&
      taosArrayPush(exchange.pSources, pSource) != NULL && taosArrayPush(exchange.pSourceDataInfo, &data) != NULL &&
      taosArrayPush(exchange.pFetchRpcHandles, &rpcHandle) != NULL) {
    code = doSendFetchDataRequest(&exchange, pTask, 0);
  }
  taosArrayDestroy(exchange.pSources);
  taosArrayDestroy(exchange.pSourceDataInfo);
  taosArrayDestroy(exchange.pFetchRpcHandles);
  return code;
}

typedef struct SExchangeMultiGroupTestOwner {
  SExchangeInfo       exchange;
  SOperatorInfo       operatorInfo;
  SExecTaskInfo       taskInfo;
  SStreamRuntimeInfo* pRuntimeInfo;
  SSubplan            subplan;
  SExchangePhysiNode  exchangeNode;
  bool                delayedRefHeld;
  bool                readerTestReady;
  bool                readerCallbackRegistered;
} SExchangeMultiGroupTestOwner;

void exchangeTestDestroyMultiGroupOwner(void* pParam);

static void* exchangeTestCreateMultiSourceOwnerImpl(bool staticQualified, bool runtimeMultiGroup, bool runnerSource,
                                                    int32_t sourceCount, const uint64_t* pGroupOrder,
                                                    int32_t groupCount) {
  if (sourceCount <= 0 || groupCount < 0 || (groupCount > 0 && pGroupOrder == NULL)) return NULL;

  SExchangeMultiGroupTestOwner* pOwner = taosMemoryCalloc(1, sizeof(*pOwner));
  if (pOwner == NULL) return NULL;
  pOwner->pRuntimeInfo = taosMemoryCalloc(1, sizeof(*pOwner->pRuntimeInfo));
  if (pOwner->pRuntimeInfo == NULL) {
    taosMemoryFree(pOwner);
    return NULL;
  }

  pOwner->exchange.pSources = taosArrayInit(sourceCount, sizeof(SDownstreamSourceNode));
  pOwner->exchange.pSourceDataInfo = taosArrayInit(sourceCount, sizeof(SSourceDataInfo));
  pOwner->exchange.pResultBlockList = taosArrayInit(1, POINTER_BYTES);
  pOwner->exchange.pRecycledBlocks = taosArrayInit(1, POINTER_BYTES);
  if (pOwner->exchange.pSources == NULL || pOwner->exchange.pSourceDataInfo == NULL ||
      pOwner->exchange.pResultBlockList == NULL || pOwner->exchange.pRecycledBlocks == NULL ||
      createDataBlock(&pOwner->exchange.pDummyBlock) != 0) {
    exchangeTestDestroyMultiGroupOwner(pOwner);
    return NULL;
  }

  SColumnInfoData column = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
  if (blockDataAppendColInfo(pOwner->exchange.pDummyBlock, &column) != TSDB_CODE_SUCCESS) {
    exchangeTestDestroyMultiGroupOwner(pOwner);
    return NULL;
  }

  for (int32_t i = 0; i < sourceCount; ++i) {
    SDownstreamSourceNode source = {0};
    source.fetchMsgType = runnerSource ? TDMT_STREAM_FETCH_FROM_RUNNER : TDMT_STREAM_FETCH;
    SSourceDataInfo sourceInfo = {.index = i, .type = EX_SRC_TYPE_STB_JOIN_SCAN};
    if (taosArrayPush(pOwner->exchange.pSources, &source) == NULL ||
        taosArrayPush(pOwner->exchange.pSourceDataInfo, &sourceInfo) == NULL) {
      exchangeTestDestroyMultiGroupOwner(pOwner);
      return NULL;
    }
  }

  pOwner->exchange.limitInfo.limit = (SLimit){.limit = -1, .offset = -1};
  pOwner->exchange.limitInfo.slimit = (SLimit){.limit = -1, .offset = -1};
  pOwner->operatorInfo.info = &pOwner->exchange;
  pOwner->operatorInfo.pTaskInfo = &pOwner->taskInfo;
  pOwner->operatorInfo.pPhyNode = &pOwner->exchangeNode.node;
  pOwner->exchangeNode.node.type = QUERY_NODE_PHYSICAL_PLAN_EXCHANGE;
  pOwner->taskInfo.execModel = OPTR_EXEC_MODEL_STREAM;
  pOwner->taskInfo.pStreamRuntimeInfo = pOwner->pRuntimeInfo;
  pOwner->taskInfo.pSubplan = &pOwner->subplan;
  pOwner->pRuntimeInfo->funcInfo.isMultiGroupCalc = runtimeMultiGroup;
  if (groupCount > 0) {
    if (runtimeMultiGroup) {
      pOwner->pRuntimeInfo->funcInfo.pGroupCalcInfos =
          tSimpleHashInit(groupCount, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    }
    pOwner->pRuntimeInfo->funcInfo.pGroupCalcOrder = taosArrayInit(groupCount, sizeof(uint64_t));
    if ((runtimeMultiGroup && pOwner->pRuntimeInfo->funcInfo.pGroupCalcInfos == NULL) ||
        pOwner->pRuntimeInfo->funcInfo.pGroupCalcOrder == NULL) {
      exchangeTestDestroyMultiGroupOwner(pOwner);
      return NULL;
    }
    if (runtimeMultiGroup) {
      tSimpleHashSetFreeFp(pOwner->pRuntimeInfo->funcInfo.pGroupCalcInfos, tDestroySSTriggerGroupCalcInfo);
    }
    for (int32_t i = 0; i < groupCount; ++i) {
      SSTriggerGroupCalcInfo calc = {0};
      if (runtimeMultiGroup && tSimpleHashGet(pOwner->pRuntimeInfo->funcInfo.pGroupCalcInfos, &pGroupOrder[i],
                                              sizeof(pGroupOrder[i])) == NULL) {
        calc.pParams = taosArrayInit_s(sizeof(SSTriggerCalcParam), 1);
        if (calc.pParams == NULL || tSimpleHashPut(pOwner->pRuntimeInfo->funcInfo.pGroupCalcInfos, &pGroupOrder[i],
                                                   sizeof(pGroupOrder[i]), &calc, sizeof(calc)) != TSDB_CODE_SUCCESS) {
          tDestroySSTriggerGroupCalcInfo(&calc);
          exchangeTestDestroyMultiGroupOwner(pOwner);
          return NULL;
        }
      }
      if (taosArrayPush(pOwner->pRuntimeInfo->funcInfo.pGroupCalcOrder, &pGroupOrder[i]) == NULL) {
        exchangeTestDestroyMultiGroupOwner(pOwner);
        return NULL;
      }
    }
  }
  pOwner->subplan.inheritedMultiGroupJoin = staticQualified;
  return pOwner;
}

void* exchangeTestCreateMultiGroupOwner(bool staticQualified, bool runtimeMultiGroup, bool runnerSource) {
  const uint64_t defaultGroup = 101;
  return exchangeTestCreateMultiSourceOwnerImpl(staticQualified, runtimeMultiGroup, runnerSource, 1, &defaultGroup, 1);
}

void* exchangeTestCreateMultiSourceOwner(bool staticQualified, bool runtimeMultiGroup, bool runnerSource,
                                         int32_t sourceCount, const uint64_t* pGroupOrder, int32_t groupCount) {
  return exchangeTestCreateMultiSourceOwnerImpl(staticQualified, runtimeMultiGroup, runnerSource, sourceCount,
                                                pGroupOrder, groupCount);
}

int32_t exchangeTestSetReaderOutputDescriptor(void* pParam, SDataBlockDescNode* pDesc) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  SSDataBlock*                  pBlock = createDataBlockFromDescNode(pDesc);
  if (pBlock == NULL) return terrno;
  blockDataDestroy(pOwner->exchange.pDummyBlock);
  pOwner->exchange.pDummyBlock = pBlock;
  return TSDB_CODE_SUCCESS;
}

int32_t exchangeTestSetReaderSourceGroups(void* pParam, int32_t sourceIndex, const uint64_t* pGids, int32_t count) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  SStreamRuntimeFuncInfo*       pStream = &pOwner->pRuntimeInfo->funcInfo;
  SDownstreamSourceNode*        pSource = taosArrayGet(pOwner->exchange.pSources, sourceIndex);
  if (pSource == NULL) return TSDB_CODE_INVALID_PARA;
  pSource->addr.nodeId = sourceIndex + 1;
  if (pStream->pGroupReadInfos == NULL) {
    pStream->pGroupReadInfos = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (pStream->pGroupReadInfos == NULL) return terrno;
  }
  SArray* pReads = taosArrayInit(1, sizeof(SSTriggerGroupReadInfo));
  if (pReads == NULL) return terrno;
  for (int32_t i = 0; i < count; ++i) {
    SSTriggerGroupReadInfo read = {.gid = (int64_t)pGids[i]};
    if (taosArrayPush(pReads, &read) == NULL) {
      taosArrayDestroy(pReads);
      return terrno;
    }
  }
  SArray** ppOld = tSimpleHashGet(pStream->pGroupReadInfos, &pSource->addr.nodeId, sizeof(pSource->addr.nodeId));
  SArray*  pOld = ppOld == NULL ? NULL : *ppOld;
  int32_t  code = tSimpleHashPut(pStream->pGroupReadInfos, &pSource->addr.nodeId, sizeof(pSource->addr.nodeId), &pReads,
                                 POINTER_BYTES);
  taosArrayDestroy(code == TSDB_CODE_SUCCESS ? pOld : pReads);
  return code;
}

static int32_t exchangeTestInitReaderTransport(SExchangeMultiGroupTestOwner* pOwner) {
  if (pOwner->readerTestReady) return TSDB_CODE_SUCCESS;
  int32_t code = tsem_init(&pOwner->exchange.ready, 0, 0);
  if (code != TSDB_CODE_SUCCESS) return code;
  pOwner->readerTestReady = true;
  pOwner->operatorInfo.fpSet._openFn = prepareLoadRemoteData;
  pOwner->exchange.pFetchRpcHandles = taosArrayInit_s(sizeof(int64_t), taosArrayGetSize(pOwner->exchange.pSources));
  return pOwner->exchange.pFetchRpcHandles == NULL ? terrno : TSDB_CODE_SUCCESS;
}

int32_t exchangeTestSendReaderFetch(void* owner, int32_t sourceIndex) {
  SExchangeMultiGroupTestOwner* pOwner = owner;
  int32_t                       code = exchangeTestInitReaderTransport(pOwner);
  if (code != TSDB_CODE_SUCCESS) return code;
  return doSendFetchDataRequest(&pOwner->exchange, &pOwner->taskInfo, sourceIndex);
}

SStreamRuntimeInfo* exchangeTestReaderFetchRuntime(void* owner) {
  SExchangeMultiGroupTestOwner* pOwner = owner;
  return pOwner->pRuntimeInfo;
}

int32_t exchangeTestSetReaderFetchSource(void* owner, int32_t sourceIndex, const SDownstreamSourceNode* source) {
  SExchangeMultiGroupTestOwner* pOwner = owner;
  SDownstreamSourceNode*        pSource = taosArrayGet(pOwner->exchange.pSources, sourceIndex);
  if (pSource == NULL || source == NULL) return TSDB_CODE_INVALID_PARA;
  *pSource = *source;
  return TSDB_CODE_SUCCESS;
}

int32_t exchangeTestSetReaderFetchUids(void* owner, int32_t sourceIndex, const SArray* pUids) {
  SExchangeMultiGroupTestOwner* pOwner = owner;
  SSourceDataInfo*              pSource = taosArrayGet(pOwner->exchange.pSourceDataInfo, sourceIndex);
  if (pSource == NULL || pSource->pSrcUidList != NULL || pUids == NULL) return TSDB_CODE_INVALID_PARA;
  pSource->pSrcUidList = taosArrayDup(pUids, NULL);
  pSource->srcOpType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
  return pSource->pSrcUidList == NULL ? terrno : TSDB_CODE_SUCCESS;
}

int32_t exchangeTestEnableReaderCallback(void* pParam, SQueryAutoQWorkerPoolCB* pWorker) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  int32_t                       code = qExecutorInit();
  if (code != TSDB_CODE_SUCCESS) return code;
  code = exchangeTestInitReaderTransport(pOwner);
  if (code != TSDB_CODE_SUCCESS) return code;
  pOwner->exchange.isExchange = true;
  pOwner->exchange.self = taosAddRef(fetchObjRefPool, &pOwner->exchange);
  if (pOwner->exchange.self < 0) return terrno;
  pOwner->readerCallbackRegistered = true;
  pOwner->taskInfo.pWorkerCb = pWorker;
  return TSDB_CODE_SUCCESS;
}

int32_t exchangeTestReaderGetNext(void* pParam, SSDataBlock** ppBlock, bool cancel) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  int32_t                       code = exchangeTestInitReaderTransport(pOwner);
  if (code != TSDB_CODE_SUCCESS) return code;
  *ppBlock = NULL;
  if (cancel) pOwner->taskInfo.code = TSDB_CODE_TSC_QUERY_CANCELLED;
  code = setjmp(pOwner->taskInfo.env);
  if (code != 0) return code;
  return loadRemoteDataNext(&pOwner->operatorInfo, ppBlock);
}

int32_t exchangeTestCallReaderCallback(void* pParam, int64_t seqId, const void* pRsp, int32_t len, int32_t code) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  SFetchRspHandleWrapper        wrapper = {.exchangeId = pOwner->exchange.self, .sourceIndex = 0, .seqId = seqId};
  SDataBuf                      message = {.len = len};
  if (pRsp != NULL) {
    message.pData = taosMemoryMalloc(len);
    if (message.pData == NULL) return terrno;
    memcpy(message.pData, pRsp, len);
  }
  return loadRemoteDataCallback(&wrapper, &message, code);
}

void exchangeTestReaderCallbackState(void* pParam, int64_t* pSeqId, int32_t* pCode, const void** ppRsp,
                                     int32_t* pStatus) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  SSourceDataInfo*              pSource = taosArrayGet(pOwner->exchange.pSourceDataInfo, 0);
  *pSeqId = pSource->seqId;
  *pCode = pSource->code;
  *ppRsp = pSource->pRsp;
  *pStatus = pSource->status;
}

int32_t exchangeTestDeliverReaderResponse(void* pParam, int32_t sourceIndex, const void* pRsp, int32_t len) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  SSourceDataInfo*              pData = taosArrayGet(pOwner->exchange.pSourceDataInfo, sourceIndex);
  if (pData == NULL || pData->pRsp != NULL) return TSDB_CODE_INVALID_PARA;
  pData->pRsp = taosMemoryMalloc(len);
  if (pData->pRsp == NULL) return terrno;
  memcpy(pData->pRsp, pRsp, len);
  pData->rspLen = len;
  pData->code = normalizeFetchResponseHeader(pData->pRsp, len);
  pData->seqId = pOwner->exchange.seqId;
  pData->status = EX_SOURCE_DATA_READY;
  return tsem_post(&pOwner->exchange.ready);
}

int32_t exchangeTestRequestReaderGroups(void* pParam) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  pOwner->exchange.pDummyBlock->info.id.blockId = 31;
  SColumnInfoData* pColumn = taosArrayGet(pOwner->exchange.pDummyBlock->pDataBlock, 0);
  pColumn->info.colId = 42;
  return qExchangeRequestReaderGroupOutput(&pOwner->operatorInfo);
}

int32_t exchangeTestReaderMode(void* pParam) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  return qExchangeReaderGroupOutputMode(&pOwner->operatorInfo);
}

int32_t exchangeTestAcceptSourceFetchResponse(void* pParam, int32_t sourceIndex, const void* pRspData, int32_t rspLen) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  if (pOwner == NULL || sourceIndex < 0 || pRspData == NULL || rspLen <= 0) return TSDB_CODE_INVALID_PARA;

  SSourceDataInfo* pDataInfo = taosArrayGet(pOwner->exchange.pSourceDataInfo, sourceIndex);
  if (pDataInfo == NULL || pDataInfo->pRsp != NULL) return TSDB_CODE_INVALID_PARA;
  pDataInfo->pRsp = taosMemoryMalloc(rspLen);
  if (pDataInfo->pRsp == NULL) return terrno;
  memcpy(pDataInfo->pRsp, pRspData, rspLen);
  pDataInfo->rspLen = rspLen;

  int32_t code = normalizeFetchResponseHeader(pDataInfo->pRsp, rspLen);
  if (code != TSDB_CODE_SUCCESS) {
    clearSourceResponse(pDataInfo);
    if (pOwner->exchange.readerGroupRequested || isStrictMultiGroupRunnerSource(&pOwner->operatorInfo, sourceIndex)) {
      clearExchangeResultState(&pOwner->operatorInfo, &pOwner->exchange);
      resetStrictMultiGroupBarrier(&pOwner->exchange);
    }
    return code;
  }

  SRetrieveTableRsp* pRsp = pDataInfo->pRsp;
  if (pOwner->exchange.readerGroupRequested) {
    code = acceptReaderFetchResponse(&pOwner->operatorInfo, pDataInfo);
    if (code == TSDB_CODE_SUCCESS) code = consumeReaderGroups(&pOwner->operatorInfo);
  } else if (isStrictMultiGroupRunnerSource(&pOwner->operatorInfo, sourceIndex) ||
             hasRunnerFetchFrame(pRsp->streamBlockType)) {
    EExchangeFetchResultType resultType = EXCHANGE_FETCH_RESULT_NONE;
    bool                     outputQueued = false;
    code = decodeStrictMultiGroupFetchResponse(&pOwner->operatorInfo, &pOwner->exchange, pDataInfo, sourceIndex,
                                               &resultType, &outputQueued);
  } else if (pRsp->numOfRows > 0) {
    code = doExtractResultBlocks(&pOwner->exchange, pDataInfo, pOwner->pRuntimeInfo);
  }
  clearSourceResponse(pDataInfo);
  return code;
}

int32_t exchangeTestAcceptFetchResponse(void* pParam, const void* pRspData, int32_t rspLen) {
  return exchangeTestAcceptSourceFetchResponse(pParam, 0, pRspData, rspLen);
}

bool exchangeTestCanPollSource(void* pParam, int32_t sourceIndex) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  return pOwner != NULL && canPollStrictMultiGroupSource(&pOwner->exchange, sourceIndex);
}

bool exchangeTestAllSourcesExhausted(void* pParam) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  if (pOwner == NULL) return false;
  int32_t completed = 0;
  return getCompletedSources(pOwner->exchange.pSourceDataInfo, &completed) == TSDB_CODE_SUCCESS &&
         completed == taosArrayGetSize(pOwner->exchange.pSourceDataInfo);
}

int32_t exchangeTestPopMultiGroupResult(void* pParam, SSDataBlock** ppBlock, const SArray** ppPublishedLineage) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  if (pOwner == NULL || ppBlock == NULL || ppPublishedLineage == NULL) return TSDB_CODE_INVALID_PARA;

  if (pOwner->exchange.readerGroupRequested) {
    int32_t code = consumeReaderGroups(&pOwner->operatorInfo);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  bool    popped = false;
  int32_t code = popExchangeMultiGroupResult(&pOwner->operatorInfo, ppBlock, &popped);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (!popped && taosArrayGetSize(pOwner->exchange.pResultBlockList) > 0) {
    *ppBlock = taosArrayGetP(pOwner->exchange.pResultBlockList, 0);
    taosArrayRemove(pOwner->exchange.pResultBlockList, 0);
    if (taosArrayPush(pOwner->exchange.pRecycledBlocks, ppBlock) == NULL) return terrno;
  }
  *ppPublishedLineage = pOwner->exchange.pPublishedLineage;
  return TSDB_CODE_SUCCESS;
}

int32_t exchangeTestLoadNextMultiGroupResult(void* pParam, SSDataBlock** ppBlock) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  if (pOwner == NULL || ppBlock == NULL) return TSDB_CODE_INVALID_PARA;

  const SArray* pLineage = NULL;
  int32_t       code = exchangeTestPopMultiGroupResult(pOwner, ppBlock, &pLineage);
  if (code != TSDB_CODE_SUCCESS || *ppBlock == NULL) return code;
  bool continueLoad = false;
  return processExchangeLoadedBlock(&pOwner->operatorInfo, *ppBlock, ppBlock, &continueLoad);
}

int32_t exchangeTestGetResultQueueSizes(void* pParam, int32_t* pStrictItems, int32_t* pOrdinaryBlocks) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  if (pOwner == NULL || pStrictItems == NULL || pOrdinaryBlocks == NULL) return TSDB_CODE_INVALID_PARA;
  *pStrictItems = taosArrayGetSize(pOwner->exchange.pMultiGroupResultList);
  *pOrdinaryBlocks = taosArrayGetSize(pOwner->exchange.pResultBlockList);
  return TSDB_CODE_SUCCESS;
}

int32_t exchangeTestEnableFilterLimitGuards(void* pParam) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  if (pOwner == NULL || pOwner->operatorInfo.exprSupp.pFilterInfo != NULL) return TSDB_CODE_INVALID_PARA;

  SValueNode* pFalse = NULL;
  int32_t     code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pFalse);
  if (code != TSDB_CODE_SUCCESS) return code;
  pFalse->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pFalse->node.resType.bytes = sizeof(bool);
  bool falseValue = false;
  code = nodesSetValueNodeValue(pFalse, &falseValue);
  if (code == TSDB_CODE_SUCCESS) {
    pFalse->translate = true;
    code = filterInitFromNode((SNode*)pFalse, &pOwner->operatorInfo.exprSupp.pFilterInfo, 0, NULL);
  }
  nodesDestroyNode((SNode*)pFalse);
  if (code != TSDB_CODE_SUCCESS) return code;

  pOwner->exchange.limitInfo.limit = (SLimit){.limit = 0, .offset = 0};
  pOwner->exchange.limitInfo.slimit = (SLimit){.limit = 0, .offset = 0};
  return TSDB_CODE_SUCCESS;
}

int32_t exchangeTestResetReaderExecution(void* pParam, bool multiGroup, bool request) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  pOwner->taskInfo.code = TSDB_CODE_SUCCESS;
  pOwner->pRuntimeInfo->funcInfo.isMultiGroupCalc = multiGroup;
  int32_t code = resetExchangeOperState(&pOwner->operatorInfo);
  if (code == TSDB_CODE_SUCCESS && request) code = qExchangeRequestReaderGroupOutput(&pOwner->operatorInfo);
  return code;
}

int32_t exchangeTestResetMultiGroupOwner(void* pParam) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  if (pOwner == NULL) return TSDB_CODE_INVALID_PARA;
  return exchangeTestResetReaderExecution(pOwner, pOwner->pRuntimeInfo->funcInfo.isMultiGroupCalc,
                                          pOwner->exchange.readerGroupRequested);
}

void exchangeTestDestroyMultiGroupOwner(void* pParam) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  if (pOwner == NULL) return;
  filterFreeInfo(pOwner->operatorInfo.exprSupp.pFilterInfo);
  pOwner->operatorInfo.exprSupp.pFilterInfo = NULL;
  clearExchangeResultState(&pOwner->operatorInfo, &pOwner->exchange);
  if (pOwner->pRuntimeInfo != NULL) {
    tClearStreamCurrentGroupRead(&pOwner->pRuntimeInfo->funcInfo);
    SSHashObj* pReads = pOwner->pRuntimeInfo->funcInfo.pGroupReadInfos;
    if (pReads != NULL) tSimpleHashSetFreeFp(pReads, tDestroySSTriggerGroupReadInfoArray);
    tSimpleHashCleanup(pReads);
    pOwner->pRuntimeInfo->funcInfo.pGroupReadInfos = NULL;
    tDestroyStRtFuncInfo(&pOwner->pRuntimeInfo->funcInfo);
  }
  taosMemoryFree(pOwner->pRuntimeInfo);
  if (pOwner->readerCallbackRegistered) {
    destroyExchangeOperatorInfo(&pOwner->exchange);
    return;
  }
  if (pOwner->readerTestReady) TAOS_UNUSED(tsem_destroy(&pOwner->exchange.ready));
  taosArrayDestroy(pOwner->exchange.pFetchRpcHandles);
  taosArrayDestroy(pOwner->exchange.pMultiGroupResultList);
  taosArrayDestroy(pOwner->exchange.pResultBlockList);
  taosArrayDestroy(pOwner->exchange.pRecycledBlocks);
  taosArrayDestroy(pOwner->exchange.pSources);
  taosArrayDestroyEx(pOwner->exchange.pSourceDataInfo, freeSourceDataInfo);
  blockDataDestroy(pOwner->exchange.pDummyBlock);
  taosMemoryFree(pOwner);
}

void exchangeTestSetRuntimeLineageAlias(void* pParam, SArray* pLineage) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  if (pOwner != NULL) pOwner->pRuntimeInfo->funcInfo.pStreamBlkWinIdx = pLineage;
}

const SArray* exchangeTestGetRuntimeLineageAlias(void* pParam) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  return pOwner == NULL ? NULL : pOwner->pRuntimeInfo->funcInfo.pStreamBlkWinIdx;
}

bool exchangeTestIsStrictResultQueueAllocated(void* pParam) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  return pOwner != NULL && pOwner->exchange.pMultiGroupResultList != NULL;
}

int32_t exchangeTestBeginDelayedDestroy(void* pParam) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  if (pOwner == NULL || pOwner->delayedRefHeld) return TSDB_CODE_INVALID_PARA;

  int32_t code = qExecutorInit();
  if (code != TSDB_CODE_SUCCESS) return code;
  code = tsem_init(&pOwner->exchange.ready, 0, 0);
  if (code != TSDB_CODE_SUCCESS) return code;

  pOwner->exchange.isExchange = true;
  int64_t refId = taosAddRef(fetchObjRefPool, &pOwner->exchange);
  if (refId < 0) return terrno;
  pOwner->exchange.self = refId;
  if (taosAcquireRef(fetchObjRefPool, refId) == NULL) return terrno;
  pOwner->delayedRefHeld = true;
  destroyExchangeOperatorInfo(&pOwner->exchange);
  return TSDB_CODE_SUCCESS;
}

int32_t exchangeTestFinishDelayedDestroy(void* pParam) {
  SExchangeMultiGroupTestOwner* pOwner = pParam;
  if (pOwner == NULL || !pOwner->delayedRefHeld) return TSDB_CODE_INVALID_PARA;

  int64_t refId = pOwner->exchange.self;
  if (pOwner->pRuntimeInfo->funcInfo.pGroupReadInfos != NULL) {
    tSimpleHashSetFreeFp(pOwner->pRuntimeInfo->funcInfo.pGroupReadInfos, tDestroySSTriggerGroupReadInfoArray);
  }
  tDestroyStRtFuncInfo(&pOwner->pRuntimeInfo->funcInfo);
  taosMemoryFreeClear(pOwner->pRuntimeInfo);
  pOwner->taskInfo.pStreamRuntimeInfo = NULL;
  return taosReleaseRef(fetchObjRefPool, refId);
}

int32_t exchangeTestNormalizeFetchHeader(const void* pRspData, int32_t rspLen, int64_t* pNumOfCols) {
  if (pRspData == NULL || pNumOfCols == NULL || rspLen <= 0) return TSDB_CODE_INVALID_PARA;
  SRetrieveTableRsp* pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) return terrno;
  memcpy(pRsp, pRspData, rspLen);
  int32_t code = normalizeFetchResponseHeader(pRsp, rspLen);
  if (code == TSDB_CODE_SUCCESS) *pNumOfCols = pRsp->numOfCols;
  taosMemoryFree(pRsp);
  return code;
}

int32_t exchangeTestGetLocalFetchResponseLen(const void* pRspData, int32_t rspLen, uint32_t* pLogicalLen) {
  if (pRspData == NULL || pLogicalLen == NULL || rspLen <= 0) return TSDB_CODE_INVALID_PARA;
  void* pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) return terrno;
  memcpy(pRsp, pRspData, rspLen);
  int32_t code = getLocalFetchResponseLen(pRsp, pLogicalLen);
  taosMemoryFree(pRsp);
  return code;
}
#endif

int32_t seqLoadRemoteData(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  int32_t code = 0;
  int32_t lino = 0;
  size_t  totalSources = taosArrayGetSize(pExchangeInfo->pSources);
  int64_t startTs = taosGetTimestampUs();

  int32_t vgId = 0;
  if (pExchangeInfo->dynTbname) {
    SArray* vals = pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamPartColVals;
    for (int32_t i = 0; i < taosArrayGetSize(vals); ++i) {
      SStreamGroupValue* pValue = taosArrayGet(vals, i);
      if (pValue != NULL && pValue->isTbname) {
        vgId = pValue->vgId;
        break;
      }
    }
  }

  while (1) {
    if (pExchangeInfo->current >= totalSources) {
      setAllSourcesCompleted(pOperator);
      return TSDB_CODE_SUCCESS;
    }

    SDownstreamSourceNode* pSource = taosArrayGet(pExchangeInfo->pSources, pExchangeInfo->current);
    if (!pSource) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = terrno;
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    if (vgId != 0 && pSource->addr.nodeId != vgId){
      pExchangeInfo->current += 1;
      continue;
    }

    SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pExchangeInfo->current);
    if (!pDataInfo) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      pTaskInfo->code = terrno;
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }
    pDataInfo->status = EX_SOURCE_DATA_NOT_READY;

    code = doSendFetchDataRequest(pExchangeInfo, pTaskInfo, pExchangeInfo->current);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    while (true) {
      recordOpExecBeforeDownstream(pOperator);
      code = exchangeWait(pOperator, pExchangeInfo);
      recordOpExecAfterDownstream(pOperator, 0);

      if (code != TSDB_CODE_SUCCESS || isTaskKilled(pTaskInfo)) {
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }

      int64_t currSeqId = atomic_load_64(&pExchangeInfo->seqId);
      if (pDataInfo->seqId != currSeqId) {
        qDebug("seq rsp reqId %" PRId64 " mismatch with exchange %p curr seqId %" PRId64 ", ignore it", pDataInfo->seqId, pExchangeInfo, currSeqId);
        clearSourceResponse(pDataInfo);
        continue;
      }

      break;
    }

    if (pDataInfo->code != TSDB_CODE_SUCCESS) {
      qError("%s vgId:%d, clientId:0x%" PRIx64 " taskID:0x%" PRIx64 " execId:%d error happens, code:%s",
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             tstrerror(pDataInfo->code));
      pOperator->pTaskInfo->code = pDataInfo->code;
      copySourceExtErrMsg(pOperator->pTaskInfo, pDataInfo);
      return pOperator->pTaskInfo->code;
    }

    SRetrieveTableRsp*   pRsp = pDataInfo->pRsp;
    SLoadRemoteDataInfo* pLoadInfo = &pExchangeInfo->loadInfo;

    if (hasRunnerFetchFrame(pRsp->streamBlockType)) {
      code = TSDB_CODE_INVALID_MSG;
      QUERY_CHECK_CODE(code, lino, _return);
    }

    if (pRsp->numOfRows == 0) {
      qDebug("exhausted %p,%s vgId:%d, clientId:0x%" PRIx64 " taskID:0x%" PRIx64
             " execId:%d %d of total completed, rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 " try next", pDataInfo,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             pExchangeInfo->current + 1, pDataInfo->totalRows, pLoadInfo->totalRows);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      if (isVstbScan(pDataInfo) || isVstbTagScan(pDataInfo)) {
        pExchangeInfo->current = totalSources;
      } else {
        pExchangeInfo->current += 1;
      }
      clearSourceResponse(pDataInfo);
      continue;
    }

    code = doExtractResultBlocks(pExchangeInfo, pDataInfo, pTaskInfo->pStreamRuntimeInfo);
    QUERY_CHECK_CODE(code, lino, _return);

    SRetrieveTableRsp* pRetrieveRsp = pDataInfo->pRsp;
    if (pRsp->completed == 1) {
      qDebug("exhausted %p,%s fetch msg rsp from vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64 " execId:%d numOfRows:%" PRId64
             ", rowsOfSource:%" PRIu64 ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64 " try next %d/%" PRIzu, pDataInfo,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             pRetrieveRsp->numOfRows, pDataInfo->totalRows, pLoadInfo->totalRows, pLoadInfo->totalSize,
             pExchangeInfo->current + 1, totalSources);

      pDataInfo->status = EX_SOURCE_DATA_EXHAUSTED;
      if (isVstbScan(pDataInfo)) {
        pExchangeInfo->current = totalSources;
      } else {
        pExchangeInfo->current += 1;
      }
    } else {
      qDebug("%s fetch msg rsp from vgId:%d, clientId:0x%" PRIx64 " taskId:0x%" PRIx64 " execId:%d numOfRows:%" PRId64
             ", totalRows:%" PRIu64 ", totalBytes:%" PRIu64,
             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->clientId, pSource->taskId, pSource->execId,
             pRetrieveRsp->numOfRows, pLoadInfo->totalRows, pLoadInfo->totalSize);
    }
    updateLoadRemoteInfo(pLoadInfo, pRetrieveRsp->numOfRows, pRetrieveRsp->compLen, startTs, pOperator);
    pDataInfo->totalRows += pRetrieveRsp->numOfRows;

    clearSourceResponse(pDataInfo);
    if (pExchangeInfo->dynamicOp && pExchangeInfo->seqLoadData) {
      taosArrayClearEx(pExchangeInfo->pSourceDataInfo, clearVtbScanDataInfo);

      void*   data = NULL;
      int32_t iter = 0;
      while ((data = tSimpleHashIterate(pExchangeInfo->pHashSources, data, &iter))) {
        ((SExchangeSrcIndex*)data)->inUseIdx = -1;
      }
    }
    return TSDB_CODE_SUCCESS;
  }

_return:
  pTaskInfo->code = code;
  qError("%s %s failed at line %d since %s", GET_TASKID(pTaskInfo), __func__, lino, tstrerror(code));
  return code;
}

static int32_t loadTagListFromBasicParam(SSourceDataInfo* pDataInfo, SExchangeOperatorBasicParam* pBasicParam) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  STagVal  dstTag;
  bool     needFree = false;

  QUERY_CHECK_NULL(pDataInfo, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pBasicParam, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_CONDITION(DYN_TYPE_EXCHANGE_PARAM == pBasicParam->paramType, code, lino, _return,
                        TSDB_CODE_INVALID_PARA);

  if (pDataInfo->tagList) {
    taosArrayDestroyEx(pDataInfo->tagList, destroyTagVal);
    pDataInfo->tagList = NULL;
  }

  if (pBasicParam->tagList) {
    pDataInfo->tagList = taosArrayInit(1, sizeof(STagVal));
    QUERY_CHECK_NULL(pDataInfo->tagList, code, lino, _return, terrno);

    for (int32_t i = 0; i < taosArrayGetSize(pBasicParam->tagList); ++i) {
      STagVal *pSrcTag = (STagVal*)taosArrayGet(pBasicParam->tagList, i);
      QUERY_CHECK_NULL(pSrcTag, code, lino, _return, terrno);
      dstTag = (STagVal){0};
      dstTag.type = pSrcTag->type;
      dstTag.cid = pSrcTag->cid;
      if (IS_VAR_DATA_TYPE(pSrcTag->type)) {
        dstTag.nData = pSrcTag->nData;
        if (NULL != pSrcTag->pData && pSrcTag->nData > 0 && pSrcTag->nData != UINT32_MAX) {
          dstTag.pData = taosMemoryMalloc(dstTag.nData);
          QUERY_CHECK_NULL(dstTag.pData, code, lino, _return, terrno);
          needFree = true;
          memcpy(dstTag.pData, pSrcTag->pData, dstTag.nData);
        }
      } else {
        dstTag.i64 = pSrcTag->i64;
      }

      QUERY_CHECK_NULL(taosArrayPush(pDataInfo->tagList, &dstTag), code, lino, _return, terrno);
      needFree = false;
    }
  } else {
    pDataInfo->tagList = NULL;
  }

  return code;
_return:
  if (needFree) {
    taosMemoryFreeClear(dstTag.pData);
  }
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

/*
 * Duplicate layered sysscan requests from exchange basic param.
 *
 * @param pDataInfo   exchange source runtime slot
 * @param pBasicParam exchange basic param
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise an error code
 */
static int32_t loadSysScanReqsFromBasicParam(SSourceDataInfo* pDataInfo, SExchangeOperatorBasicParam* pBasicParam) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pBasicParam->paramType != DYN_TYPE_EXCHANGE_PARAM) {
    qError("%s failed since invalid exchange operator param type %d", __func__, pBasicParam->paramType);
    return TSDB_CODE_INVALID_PARA;
  }

  taosArrayDestroy(pDataInfo->sysScanReqs);
  pDataInfo->sysScanReqs = NULL;

  if (pBasicParam->sysScanReqs != NULL) {
    pDataInfo->sysScanReqs = taosArrayDup(pBasicParam->sysScanReqs, NULL);
    QUERY_CHECK_NULL(pDataInfo->sysScanReqs, code, lino, _return, terrno);
  }

  return code;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

int32_t loadBatchColMapFromBasicParam(SSourceDataInfo* pDataInfo, SExchangeOperatorBasicParam* pBasicParam) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SOrgTbInfo  dstOrgTbInfo = {0};
  bool        needFree = false;

  if (pBasicParam->paramType != DYN_TYPE_EXCHANGE_PARAM) {
    qError("%s failed since invalid exchange operator param type %d",
      __func__, pBasicParam->paramType);
    return TSDB_CODE_INVALID_PARA;
  }

  if (pBasicParam->batchOrgTbInfo) {
    pDataInfo->batchOrgTbInfo = taosArrayInit(1, sizeof(SOrgTbInfo));
    QUERY_CHECK_NULL(pDataInfo->batchOrgTbInfo, code, lino, _return, terrno);

    for (int32_t i = 0; i < taosArrayGetSize(pBasicParam->batchOrgTbInfo); ++i) {
      SOrgTbInfo* pSrcOrgTbInfo = taosArrayGet(pBasicParam->batchOrgTbInfo, i);
      QUERY_CHECK_NULL(pSrcOrgTbInfo, code, lino, _return, terrno);

      dstOrgTbInfo = (SOrgTbInfo){0};
      dstOrgTbInfo.vgId = pSrcOrgTbInfo->vgId;
      tstrncpy(dstOrgTbInfo.tbName, pSrcOrgTbInfo->tbName, TSDB_TABLE_FNAME_LEN);
      dstOrgTbInfo.srcPrecision = pSrcOrgTbInfo->srcPrecision;
      dstOrgTbInfo.dstPrecision = pSrcOrgTbInfo->dstPrecision;

      dstOrgTbInfo.colMap = taosArrayDup(pSrcOrgTbInfo->colMap, NULL);
      QUERY_CHECK_NULL(dstOrgTbInfo.colMap, code, lino, _return, terrno);

      needFree = true;
      QUERY_CHECK_NULL(taosArrayPush(pDataInfo->batchOrgTbInfo, &dstOrgTbInfo), code, lino, _return, terrno);
      needFree = false;
    }
  } else {
    pBasicParam->batchOrgTbInfo = NULL;
  }

  return code;
_return:
  if (needFree) {
    taosArrayDestroy(dstOrgTbInfo.colMap);
  }
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

int32_t addSingleExchangeSource(SOperatorInfo* pOperator,
                                SExchangeOperatorBasicParam* pBasicParam) {
  if (pBasicParam->paramType != DYN_TYPE_EXCHANGE_PARAM) {
    qWarn("%s, %s found invalid exchange operator param type %d",
      GET_TASKID(pOperator->pTaskInfo), __func__, pBasicParam->paramType);
    return TSDB_CODE_SUCCESS;
  }

  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExchangeInfo*     pExchangeInfo = pOperator->info;
  SExchangeSrcIndex* pIdx = tSimpleHashGet(pExchangeInfo->pHashSources, &pBasicParam->vgId, sizeof(pBasicParam->vgId));

  if (NULL == pIdx) {
    if (pBasicParam->isNewDeployed) {
      SDownstreamSourceNode *pNode = NULL;
      code = nodesCloneNode((SNode*)&pBasicParam->newDeployedSrc, (SNode**)&pNode);
      QUERY_CHECK_CODE(code, lino, _return);

      SExchangePhysiNode* pExchange = (SExchangePhysiNode*)pOperator->pPhyNode;
      code = nodesListMakeStrictAppend(&pExchange->pSrcEndPoints, (SNode*)pNode);
      QUERY_CHECK_CODE(code, lino, _return);

      void* tmp = taosArrayPush(pExchangeInfo->pSources, pNode);
      QUERY_CHECK_NULL(tmp, code, lino, _return, terrno);

      // keep pFetchRpcHandles parallel with pSources so the new source's index is in range
      // when doSendFetchDataRequest stores its transporter handle.
      if (pExchangeInfo->pFetchRpcHandles) {
        int64_t initRpcHandle = 0;
        QUERY_CHECK_NULL(taosArrayPush(pExchangeInfo->pFetchRpcHandles, &initRpcHandle), code, lino, _return, terrno);
      }

      SExchangeSrcIndex idx = {.srcIdx = taosArrayGetSize(pExchangeInfo->pSources) - 1, .inUseIdx = -1};
      code = tSimpleHashPut(pExchangeInfo->pHashSources, &pNode->addr.nodeId, sizeof(pNode->addr.nodeId), &idx, sizeof(idx));
      if (pExchangeInfo->pHashSources) {
        QUERY_CHECK_CODE(code, lino, _return);
      }
      pIdx = tSimpleHashGet(pExchangeInfo->pHashSources, &pBasicParam->vgId, sizeof(pBasicParam->vgId));
      QUERY_CHECK_NULL(pIdx, code, lino, _return, TSDB_CODE_INVALID_PARA);
    } else if (pBasicParam->type == EX_SRC_TYPE_VSTB_SCAN) {
      qDebug("%s vgroup:%d is missing from the query plan, "
             "request metadata refresh",
             GET_TASKID(pOperator->pTaskInfo), pBasicParam->vgId);
      return TSDB_CODE_VTABLE_DATA_SRC_EP_MISS;
    } else if (pBasicParam->type == EX_SRC_TYPE_VSTB_TS_SCAN || pBasicParam->type == EX_SRC_TYPE_VSTB_PART_INTERVAL_SCAN ||
               pBasicParam->type == EX_SRC_TYPE_VSTB_SYS_SCAN) {
      // Multi-exchange virtual table paths build each exchange param from the full vg map.
      // If this exchange does not own the current vg source, skip it and let the matching exchange consume it.
      qDebug("addSingleExchangeSource found no existing source for vgId: %d, sourceType:%d, skip it",
             pBasicParam->vgId, pBasicParam->type);
      return TSDB_CODE_SUCCESS;
    } else {
      qError("No exchange source for vgId: %d", pBasicParam->vgId);
      return TSDB_CODE_INVALID_PARA;
    }
  }

  qDebug("start to add single exchange source");

  switch (pBasicParam->type) {
    case EX_SRC_TYPE_VSTB_TS_SCAN:
    case EX_SRC_TYPE_VSTB_WIN_SCAN:
    case EX_SRC_TYPE_VSTB_INTERVAL_SCAN:
    case EX_SRC_TYPE_VSTB_PART_INTERVAL_SCAN:
    case EX_SRC_TYPE_VSTB_AGG_SCAN: {
      if (pIdx->inUseIdx >= taosArrayGetSize(pExchangeInfo->pSourceDataInfo)) {
        pIdx->inUseIdx = -1;
      }

      if (pIdx->inUseIdx < 0) {
        SSourceDataInfo dataInfo = {0};
        dataInfo.status = EX_SOURCE_DATA_NOT_READY;
        dataInfo.taskId = pExchangeInfo->pTaskId;
        dataInfo.index = pIdx->srcIdx;
        dataInfo.groupid = pBasicParam->groupid;
        dataInfo.window = pBasicParam->window;
        dataInfo.isNewParam = pBasicParam->isNewParam;
        code = loadTagListFromBasicParam(&dataInfo, pBasicParam);
        QUERY_CHECK_CODE(code, lino, _return);

        code = loadBatchColMapFromBasicParam(&dataInfo, pBasicParam);
        QUERY_CHECK_CODE(code, lino, _return);

        dataInfo.orgTbInfo = NULL;

        dataInfo.pSrcUidList = taosArrayDup(pBasicParam->uidList, NULL);
        QUERY_CHECK_NULL(dataInfo.pSrcUidList, code, lino, _return, terrno);

        dataInfo.type = pBasicParam->type;
        dataInfo.srcOpType = pBasicParam->srcOpType;
        dataInfo.tableSeq = pBasicParam->tableSeq;

        QUERY_CHECK_NULL(taosArrayPush(pExchangeInfo->pSourceDataInfo, &dataInfo), code, lino, _return, terrno);

        pIdx->inUseIdx = taosArrayGetSize(pExchangeInfo->pSourceDataInfo) - 1;
      } else {
        SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pIdx->inUseIdx);
        QUERY_CHECK_NULL(pDataInfo, code, lino, _return, terrno);

        if (pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) {
          pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
        }

        pDataInfo->taskId = pExchangeInfo->pTaskId;
        pDataInfo->index = pIdx->srcIdx;
        pDataInfo->window = pBasicParam->window;
        pDataInfo->groupid = pBasicParam->groupid;
        pDataInfo->isNewParam = pBasicParam->isNewParam;

        code = loadTagListFromBasicParam(pDataInfo, pBasicParam);
        QUERY_CHECK_CODE(code, lino, _return);

        if (pDataInfo->batchOrgTbInfo) {
          for (int32_t i = 0; i < taosArrayGetSize(pDataInfo->batchOrgTbInfo); ++i) {
            SOrgTbInfo* pOrgTbInfo = taosArrayGet(pDataInfo->batchOrgTbInfo, i);
            if (pOrgTbInfo) {
              taosArrayDestroy(pOrgTbInfo->colMap);
            }
          }
          taosArrayDestroy(pDataInfo->batchOrgTbInfo);
          pDataInfo->batchOrgTbInfo = NULL;
        }
        code = loadBatchColMapFromBasicParam(pDataInfo, pBasicParam);
        QUERY_CHECK_CODE(code, lino, _return);

        pDataInfo->orgTbInfo = NULL;

        taosArrayDestroy(pDataInfo->pSrcUidList);
        pDataInfo->pSrcUidList = taosArrayDup(pBasicParam->uidList, NULL);
        QUERY_CHECK_NULL(pDataInfo->pSrcUidList, code, lino, _return, terrno);

        pDataInfo->type = pBasicParam->type;
        pDataInfo->srcOpType = pBasicParam->srcOpType;
        pDataInfo->tableSeq = pBasicParam->tableSeq;
      }
      break;
    }
    case EX_SRC_TYPE_VTB_WIN_SCAN:
    case EX_SRC_TYPE_VSTB_TAG_SCAN: {
      SSourceDataInfo dataInfo = {0};
      dataInfo.status = EX_SOURCE_DATA_NOT_READY;
      dataInfo.taskId = pExchangeInfo->pTaskId;
      dataInfo.index = pIdx->srcIdx;
      dataInfo.window = pBasicParam->window;
      dataInfo.groupid = 0;
      dataInfo.orgTbInfo = NULL;
      dataInfo.tagList = NULL;

      dataInfo.pSrcUidList = taosArrayDup(pBasicParam->uidList, NULL);
      QUERY_CHECK_NULL(dataInfo.pSrcUidList, code, lino, _return, terrno);

      dataInfo.isNewParam = false;
      dataInfo.type = pBasicParam->type;
      dataInfo.srcOpType = pBasicParam->srcOpType;
      dataInfo.tableSeq = pBasicParam->tableSeq;

      taosArrayClearEx(pExchangeInfo->pSourceDataInfo, clearVtbScanDataInfo);
      QUERY_CHECK_NULL(taosArrayPush(pExchangeInfo->pSourceDataInfo, &dataInfo), code, lino, _return, terrno);
      break;
    }
    case EX_SRC_TYPE_VSTB_SCAN: {
      SSourceDataInfo dataInfo = {0};
      dataInfo.status = EX_SOURCE_DATA_NOT_READY;
      dataInfo.taskId = pExchangeInfo->pTaskId;
      dataInfo.index = pIdx->srcIdx;
      dataInfo.window = pBasicParam->window;
      dataInfo.groupid = 0;
      dataInfo.isNewParam = pBasicParam->isNewParam;
      dataInfo.tagList = NULL;
      dataInfo.orgTbInfo = taosMemoryMalloc(sizeof(SOrgTbInfo));
      QUERY_CHECK_NULL(dataInfo.orgTbInfo, code, lino, _return, terrno);
      dataInfo.orgTbInfo->vgId = pBasicParam->orgTbInfo->vgId;
      tstrncpy(dataInfo.orgTbInfo->tbName, pBasicParam->orgTbInfo->tbName, TSDB_TABLE_FNAME_LEN);
      dataInfo.orgTbInfo->srcPrecision = pBasicParam->orgTbInfo->srcPrecision;
      dataInfo.orgTbInfo->dstPrecision = pBasicParam->orgTbInfo->dstPrecision;
      dataInfo.orgTbInfo->colMap = taosArrayDup(pBasicParam->orgTbInfo->colMap, NULL);
      QUERY_CHECK_NULL(dataInfo.orgTbInfo->colMap, code, lino, _return, terrno);

      dataInfo.pSrcUidList = taosArrayDup(pBasicParam->uidList, NULL);
      QUERY_CHECK_NULL(dataInfo.pSrcUidList, code, lino, _return, terrno);

      dataInfo.type = pBasicParam->type;
      dataInfo.srcOpType = pBasicParam->srcOpType;
      dataInfo.tableSeq = pBasicParam->tableSeq;

      taosArrayClearEx(pExchangeInfo->pSourceDataInfo, clearVtbScanDataInfo);
      QUERY_CHECK_NULL( taosArrayPush(pExchangeInfo->pSourceDataInfo, &dataInfo), code, lino, _return, terrno);
      break;
    }
    case EX_SRC_TYPE_VSTB_SYS_SCAN: {
      if (pIdx->inUseIdx < 0) {
        SSourceDataInfo dataInfo = {0};
        dataInfo.status = EX_SOURCE_DATA_NOT_READY;
        dataInfo.taskId = pExchangeInfo->pTaskId;
        dataInfo.index = pIdx->srcIdx;
        dataInfo.window = pBasicParam->window;
        dataInfo.groupid = 0;
        dataInfo.isNewParam = pBasicParam->isNewParam;
        dataInfo.tagList = NULL;
        dataInfo.orgTbInfo = NULL;
        dataInfo.batchOrgTbInfo = NULL;
        dataInfo.pSrcUidList = NULL;

        code = loadSysScanReqsFromBasicParam(&dataInfo, pBasicParam);
        QUERY_CHECK_CODE(code, lino, _return);

        dataInfo.type = pBasicParam->type;
        dataInfo.srcOpType = pBasicParam->srcOpType;
        dataInfo.tableSeq = pBasicParam->tableSeq;

        QUERY_CHECK_NULL(taosArrayPush(pExchangeInfo->pSourceDataInfo, &dataInfo), code, lino, _return, terrno);
        pIdx->inUseIdx = taosArrayGetSize(pExchangeInfo->pSourceDataInfo) - 1;
      } else {
        SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pIdx->inUseIdx);
        QUERY_CHECK_NULL(pDataInfo, code, lino, _return, terrno);

        if (pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) {
          pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
        }

        pDataInfo->taskId = pExchangeInfo->pTaskId;
        pDataInfo->index = pIdx->srcIdx;
        pDataInfo->window = pBasicParam->window;
        pDataInfo->groupid = 0;
        pDataInfo->isNewParam = pBasicParam->isNewParam;
        pDataInfo->tagList = NULL;
        pDataInfo->orgTbInfo = NULL;
        pDataInfo->batchOrgTbInfo = NULL;
        pDataInfo->pSrcUidList = NULL;

        code = loadSysScanReqsFromBasicParam(pDataInfo, pBasicParam);
        QUERY_CHECK_CODE(code, lino, _return);

        pDataInfo->type = pBasicParam->type;
        pDataInfo->srcOpType = pBasicParam->srcOpType;
        pDataInfo->tableSeq = pBasicParam->tableSeq;
      }
      break;
    }
    case EX_SRC_TYPE_STB_JOIN_SCAN:
    default: {
      if (pIdx->inUseIdx < 0) {
        SSourceDataInfo dataInfo = {0};
        dataInfo.status = EX_SOURCE_DATA_NOT_READY;
        dataInfo.taskId = pExchangeInfo->pTaskId;
        dataInfo.index = pIdx->srcIdx;
        dataInfo.groupid = 0;
        dataInfo.tagList = NULL;

        dataInfo.pSrcUidList = taosArrayDup(pBasicParam->uidList, NULL);
        QUERY_CHECK_NULL(dataInfo.pSrcUidList, code, lino, _return, terrno);

        dataInfo.isNewParam = false;
        dataInfo.type = pBasicParam->type;
        dataInfo.srcOpType = pBasicParam->srcOpType;
        dataInfo.tableSeq = pBasicParam->tableSeq;

        QUERY_CHECK_NULL(taosArrayPush(pExchangeInfo->pSourceDataInfo, &dataInfo), code, lino, _return, terrno);

        pIdx->inUseIdx = taosArrayGetSize(pExchangeInfo->pSourceDataInfo) - 1;
      } else {
        SSourceDataInfo* pDataInfo = taosArrayGet(pExchangeInfo->pSourceDataInfo, pIdx->inUseIdx);
        QUERY_CHECK_NULL(pDataInfo, code, lino, _return, terrno);
        if (pDataInfo->status == EX_SOURCE_DATA_EXHAUSTED) {
          pDataInfo->status = EX_SOURCE_DATA_NOT_READY;
        }

        pDataInfo->tagList = NULL;
        pDataInfo->pSrcUidList = taosArrayDup(pBasicParam->uidList, NULL);
        QUERY_CHECK_NULL(pDataInfo->pSrcUidList, code, lino, _return, terrno);

        pDataInfo->groupid = 0;
        pDataInfo->isNewParam = false;
        pDataInfo->type = pBasicParam->type;
        pDataInfo->srcOpType = pBasicParam->srcOpType;
        pDataInfo->tableSeq = pBasicParam->tableSeq;
      }
      break;
    }
  }

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

int32_t addDynamicExchangeSource(SOperatorInfo* pOperator) {
  SExchangeInfo*               pExchangeInfo = pOperator->info;
  int32_t                      code = TSDB_CODE_SUCCESS;
  SExchangeOperatorBasicParam* pBasicParam = NULL;
  SExchangeOperatorParam*      pParam = (SExchangeOperatorParam*)pOperator->pOperatorGetParam->value;
  if (pParam->multiParams) {
    SExchangeOperatorBatchParam* pBatch = (SExchangeOperatorBatchParam*)pOperator->pOperatorGetParam->value;
    int32_t                      iter = 0;
    while (NULL != (pBasicParam = tSimpleHashIterate(pBatch->pBatchs, pBasicParam, &iter))) {
      code = addSingleExchangeSource(pOperator, pBasicParam);
      if (code) {
        return code;
      }
    }
  } else {
    pBasicParam = &pParam->basic;
    code = addSingleExchangeSource(pOperator, pBasicParam);
  }

  freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
  pOperator->pOperatorGetParam = NULL;

  return code;
}

int32_t prepareLoadRemoteData(SOperatorInfo* pOperator) {
  SExchangeInfo* pExchangeInfo = pOperator->info;
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  
  if ((OPTR_IS_OPENED(pOperator) && !pExchangeInfo->dynamicOp &&
       NULL == pOperator->pOperatorGetParam) ||
      (pExchangeInfo->dynamicOp && NULL == pOperator->pOperatorGetParam)) {
    qDebug("%s, skip prepare, opened:%d, dynamicOp:%d, getParam:%p",
      GET_TASKID(pOperator->pTaskInfo), OPTR_IS_OPENED(pOperator),
      pExchangeInfo->dynamicOp, pOperator->pOperatorGetParam);
    return TSDB_CODE_SUCCESS;
  }

  if (pExchangeInfo->dynamicOp) {
    code = addDynamicExchangeSource(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pOperator->status == OP_NOT_OPENED &&
      (pExchangeInfo->dynamicOp && pExchangeInfo->seqLoadData) ||
      IS_STREAM_MODE(pOperator->pTaskInfo)) {
    pExchangeInfo->current = 0;
  }

  if (NULL != pOperator->pOperatorGetParam) {
    SOperatorParam* pGetParam = pOperator->pOperatorGetParam;
    QUERY_CHECK_CONDITION(
        pGetParam->opType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE &&
            pGetParam->value != NULL,
        code, lino, _end, TSDB_CODE_INVALID_PARA);

    SExchangeOperatorParam* pParam = pGetParam->value;
    // Dynamic query control owns single and batched dynamic parameters.
    // Only early-stop parameters should update Exchange notification state.
    if (!pParam->multiParams &&
        pParam->basic.paramType != DYN_TYPE_EXCHANGE_PARAM) {
      code = storeExchangeNotifyInfo(pOperator);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (!pGetParam->reUse) {
      freeOperatorParam(pGetParam, OP_GET_PARAM);
    } else {
      /**
        The param is referenced by getParam, and it will be freed by
        the parent operator after getting next block.
      */
      pGetParam->reUse = false;
    }
    pOperator->pOperatorGetParam = NULL;
  }

  int64_t st = taosGetTimestampUs();

  if (!IS_STREAM_MODE(pOperator->pTaskInfo) && !pExchangeInfo->seqLoadData) {
    code = prepareConcurrentlyLoad(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
    pExchangeInfo->openedTs = taosGetTimestampUs();
  }

  OPTR_SET_OPENED(pOperator);
  qDebug("%s prepare load complete", pOperator->pTaskInfo->id.str);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }
  return code;
}

int32_t handleLimitOffset(SOperatorInfo* pOperator, SLimitInfo* pLimitInfo, SSDataBlock* pBlock, bool holdDataInBuf) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  if (pLimitInfo->remainGroupOffset > 0) {
    if (pLimitInfo->currentGroupId == 0) {  // it is the first group
      pLimitInfo->currentGroupId = pBlock->info.id.groupId;
      blockDataCleanup(pBlock);
      return PROJECT_RETRIEVE_CONTINUE;
    } else if (pLimitInfo->currentGroupId != pBlock->info.id.groupId) {
      // now it is the data from a new group
      pLimitInfo->remainGroupOffset -= 1;

      // ignore data block in current group
      if (pLimitInfo->remainGroupOffset > 0) {
        blockDataCleanup(pBlock);
        return PROJECT_RETRIEVE_CONTINUE;
      }
    }

    // set current group id of the project operator
    pLimitInfo->currentGroupId = pBlock->info.id.groupId;
  }

  // here check for a new group data, we need to handle the data of the previous group.
  if (pLimitInfo->currentGroupId != 0 && pLimitInfo->currentGroupId != pBlock->info.id.groupId) {
    pLimitInfo->numOfOutputGroups += 1;
    if ((pLimitInfo->slimit.limit > 0) && (pLimitInfo->slimit.limit <= pLimitInfo->numOfOutputGroups)) {
      pOperator->status = OP_EXEC_DONE;
      blockDataCleanup(pBlock);

      return PROJECT_RETRIEVE_DONE;
    }

    // reset the value for a new group data
    resetLimitInfoForNextGroup(pLimitInfo);
    // existing rows that belongs to previous group.
    if (pBlock->info.rows > 0) {
      return PROJECT_RETRIEVE_DONE;
    }
  }

  // here we reach the start position, according to the limit/offset requirements.

  // set current group id
  pLimitInfo->currentGroupId = pBlock->info.id.groupId;

  bool limitReached = applyLimitOffset(pLimitInfo, pBlock, pTaskInfo);
  if (pBlock->info.rows == 0) {
    return PROJECT_RETRIEVE_CONTINUE;
  } else {
    if (limitReached && (pLimitInfo->slimit.limit > 0 && pLimitInfo->slimit.limit <= pLimitInfo->numOfOutputGroups)) {
      setOperatorCompleted(pOperator);
      return PROJECT_RETRIEVE_DONE;
    }
  }

  // todo optimize performance
  // If there are slimit/soffset value exists, multi-round result can not be packed into one group, since the
  // they may not belong to the same group the limit/offset value is not valid in this case.
  if ((!holdDataInBuf) || (pBlock->info.rows >= pOperator->resultInfo.threshold) || hasSlimitOffsetInfo(pLimitInfo)) {
    return PROJECT_RETRIEVE_DONE;
  } else {  // not full enough, continue to accumulate the output data in the buffer.
    return PROJECT_RETRIEVE_CONTINUE;
  }
}

static int32_t exchangeWait(SOperatorInfo* pOperator, SExchangeInfo* pExchangeInfo) {
  SExecTaskInfo* pTask = pOperator->pTaskInfo;
  int32_t        code = TSDB_CODE_SUCCESS;
  void*                pBlockingStatsParam = NULL;
  FStreamBlockingStats blockingStatsFp = NULL;
  if (pTask->pWorkerCb) {
    code = pTask->pWorkerCb->beforeBlocking(pTask->pWorkerCb->pPool);
    if (code != TSDB_CODE_SUCCESS) {
      pTask->code = code;
      return pTask->code;
    }
  }

  if (pTask->pStreamRuntimeInfo != NULL) {
    pBlockingStatsParam = pTask->pStreamRuntimeInfo->pBlockingStatsParam;
    blockingStatsFp = pTask->pStreamRuntimeInfo->blockingStatsFp;
  }
  if (blockingStatsFp != NULL) {
    blockingStatsFp(pBlockingStatsParam, true);
  }
  code = tsem_wait(&pExchangeInfo->ready);
  if (blockingStatsFp != NULL) {
    blockingStatsFp(pBlockingStatsParam, false);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    pTask->code = code;
    return pTask->code;
  }

  if (pTask->pWorkerCb) {
    code = pTask->pWorkerCb->afterRecoverFromBlocking(pTask->pWorkerCb->pPool);
    if (code != TSDB_CODE_SUCCESS) {
      pTask->code = code;
      return pTask->code;
    }
  }
  return TSDB_CODE_SUCCESS;
}
