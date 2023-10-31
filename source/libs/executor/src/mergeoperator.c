/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"

typedef struct SSortMergeInfo {
  SArray*        pSortInfo;
  SSortHandle*   pSortHandle;
  STupleHandle*  prefetchedTuple;
} SSortMergeInfo;

typedef struct SNonSortMergeInfo {

} SNonSortMergeInfo;

typedef struct SColumnMergeInfo {

} SColumnMergeInfo;

typedef struct SMultiwayMergeOperatorInfo {
  SOptrBasicInfo binfo;
  union {
    SSortMergeInfo    sortMergeInfo;
    SNonSortMergeInfo nsortMergeInfo;
    SColumnMergeInfo  colMergeInfo;
  };
  int32_t        bufPageSize;
  uint32_t       sortBufSize;  // max buffer size for in-memory sort
  SLimitInfo     limitInfo;
  SColMatchInfo  matchInfo;
  SSDataBlock*   pInputBlock;
  SSDataBlock*   pIntermediateBlock;   // to hold the intermediate result
  int64_t        startTs;  // sort start time
  bool           groupMerge;
  bool           ignoreGroupId;
  uint64_t       groupId;
  bool           inputWithGroupId;
} SMultiwayMergeOperatorInfo;

int32_t openMultiwayMergeOperator(SOperatorInfo* pOperator) {
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  pInfo->startTs = taosGetTimestampUs();
  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;

  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, SORT_MULTISOURCE_MERGE, pInfo->bufPageSize, numOfBufPage,
                                             pInfo->pInputBlock, pTaskInfo->id.str, 0, 0, 0);

  tsortSetFetchRawDataFp(pInfo->pSortHandle, loadNextDataBlock, NULL, NULL);
  tsortSetCompareGroupId(pInfo->pSortHandle, pInfo->groupMerge);

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      pDownstream->fpSet._openFn(pDownstream);
    }

    SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
    ps->param = pDownstream;
    ps->onlyRef = true;

    tsortAddSource(pInfo->pSortHandle, ps);
  }

  int32_t code = tsortOpen(pInfo->pSortHandle);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  pOperator->cost.openCost = (taosGetTimestampUs() - pInfo->startTs) / 1000.0;
  pOperator->status = OP_RES_TO_RETURN;

  OPTR_SET_OPENED(pOperator);
  return TSDB_CODE_SUCCESS;
}

SSDataBlock* doMultiwayMerge(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*              pTaskInfo = pOperator->pTaskInfo;
  SMultiwayMergeOperatorInfo* pInfo = pOperator->info;

  int32_t code = pOperator->fpSet._openFn(pOperator);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  qDebug("start to merge final sorted rows, %s", GET_TASKID(pTaskInfo));
  SSDataBlock* pBlock = getMultiwaySortedBlockData(pInfo->pSortHandle, pInfo->binfo.pRes, pInfo->matchInfo.pList, pOperator);
  if (pBlock != NULL) {
    pOperator->resultInfo.totalRows += pBlock->info.rows;
  } else {
    setOperatorCompleted(pOperator);
  }

  return pBlock;
}

void destroyMultiwayMergeOperatorInfo(void* param) {
  SMultiwayMergeOperatorInfo* pInfo = (SMultiwayMergeOperatorInfo*)param;
  pInfo->binfo.pRes = blockDataDestroy(pInfo->binfo.pRes);
  pInfo->pInputBlock = blockDataDestroy(pInfo->pInputBlock);
  pInfo->pIntermediateBlock = blockDataDestroy(pInfo->pIntermediateBlock);

  tsortDestroySortHandle(pInfo->pSortHandle);
  taosArrayDestroy(pInfo->pSortInfo);
  taosArrayDestroy(pInfo->matchInfo.pList);

  taosMemoryFreeClear(param);
}

int32_t getMultiwayMergeExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  SSortExecInfo* pSortExecInfo = taosMemoryCalloc(1, sizeof(SSortExecInfo));

  SMultiwayMergeOperatorInfo* pInfo = (SMultiwayMergeOperatorInfo*)pOptr->info;

  *pSortExecInfo = tsortGetSortExecInfo(pInfo->pSortHandle);
  *pOptrExplain = pSortExecInfo;

  *len = sizeof(SSortExecInfo);
  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createMultiwayMergeOperatorInfo(SOperatorInfo** downStreams, size_t numStreams,
                                               SMergePhysiNode* pMergePhyNode, SExecTaskInfo* pTaskInfo) {
  SPhysiNode* pPhyNode = (SPhysiNode*)pMergePhyNode;

  SMultiwayMergeOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SMultiwayMergeOperatorInfo));
  SOperatorInfo*              pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SDataBlockDescNode*         pDescNode = pPhyNode->pOutputDataBlockDesc;

  int32_t code = TSDB_CODE_SUCCESS;
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  initLimitInfo(pMergePhyNode->node.pLimit, pMergePhyNode->node.pSlimit, &pInfo->limitInfo);
  pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);

  int32_t rowSize = pInfo->binfo.pRes->info.rowSize;
  int32_t numOfOutputCols = 0;
  code = extractColMatchInfo(pMergePhyNode->pTargets, pDescNode, &numOfOutputCols, COL_MATCH_FROM_SLOT_ID,
                             &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SPhysiNode*  pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, 0);
  SSDataBlock* pInputBlock = createDataBlockFromDescNode(pChildNode->pOutputDataBlockDesc);

  initResultSizeInfo(&pOperator->resultInfo, 1024);
  blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);

  pInfo->groupMerge = pMergePhyNode->groupSort;
  pInfo->ignoreGroupId = pMergePhyNode->ignoreGroupId;
  pInfo->pSortInfo = createSortInfo(pMergePhyNode->pMergeKeys);
  pInfo->pInputBlock = pInputBlock;
  size_t numOfCols = taosArrayGetSize(pInfo->binfo.pRes->pDataBlock);
  pInfo->bufPageSize = getProperSortPageSize(rowSize, numOfCols);
  pInfo->sortBufSize = pInfo->bufPageSize * (numStreams + 1);  // one additional is reserved for merged result.
  pInfo->binfo.inputTsOrder = pMergePhyNode->node.inputTsOrder;
  pInfo->binfo.outputTsOrder = pMergePhyNode->node.outputTsOrder;
  pInfo->inputWithGroupId = pMergePhyNode->inputWithGroupId;

  setOperatorInfo(pOperator, "MultiwayMergeOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE, false, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(openMultiwayMergeOperator, doMultiwayMerge, NULL,
                                         destroyMultiwayMergeOperatorInfo, optrDefaultBufFn, getMultiwayMergeExplainExecInfo, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, downStreams, numStreams);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyMultiwayMergeOperatorInfo(pInfo);
  }

  pTaskInfo->code = code;
  taosMemoryFree(pOperator);
  return NULL;
}
