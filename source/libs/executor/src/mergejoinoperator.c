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
#include "function.h"
#include "operator.h"
#include "os.h"
#include "querynodes.h"
#include "querytask.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "ttypes.h"
#include "mergejoin.h"

SOperatorInfo** mJoinBuildDownstreams(SMJoinOperatorInfo* pInfo, SOperatorInfo** pDownstream) {
  SOperatorInfo** p = taosMemoryMalloc(2 * POINTER_BYTES);
  if (p) {
    p[0] = pDownstream[0];
    p[1] = pDownstream[0];
  }

  return p;
}

int32_t mJoinInitDownstreamInfo(SMJoinOperatorInfo* pInfo, SOperatorInfo** pDownstream, int32_t *numOfDownstream, bool *newDownstreams) {
  if (1 == *numOfDownstream) {
    *newDownstreams = true;
    pDownstream = mJoinBuildDownstreams(pInfo, pDownstream);
    if (NULL == pDownstream) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    *numOfDownstream = 2;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinInitPrimKeyInfo(SMJoinTableCtx* pTable, int32_t slotId) {
  pTable->primCol = taosMemoryMalloc(sizeof(SMJoinColInfo));
  if (NULL == pTable->primCol) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTable->primCol->srcSlot = slotId;

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinInitColsInfo(int32_t* colNum, int64_t* rowSize, SMJoinColInfo** pCols, SNodeList* pList) {
  *colNum = LIST_LENGTH(pList);
  
  *pCols = taosMemoryMalloc((*colNum) * sizeof(SMJoinColInfo));
  if (NULL == *pCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *rowSize = 0;
  
  int32_t i = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SColumnNode* pColNode = (SColumnNode*)pNode;
    (*pCols)[i].srcSlot = pColNode->slotId;
    (*pCols)[i].vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
    (*pCols)[i].bytes = pColNode->node.resType.bytes;
    *rowSize += pColNode->node.resType.bytes;
    ++i;
  }  

  return TSDB_CODE_SUCCESS;
}


static int32_t mJoinInitKeyColsInfo(SMJoinTableCtx* pTable, SNodeList* pList) {
  int64_t rowSize = 0;
  MJ_ERR_RET(mJoinInitColsInfo(&pTable->keyNum, &rowSize, &pTable->keyCols, pList));

  if (pTable->keyNum > 1) {
    pTable->keyBuf = taosMemoryMalloc(rowSize);
    if (NULL == pTable->keyBuf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}


static int32_t mJoinInitColsMap(int32_t* colNum, SMJoinColMap** pCols, int32_t blkId, SNodeList* pList) {
  *pCols = taosMemoryMalloc(LIST_LENGTH(pList) * sizeof(SMJoinColMap));
  if (NULL == *pCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  int32_t i = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    STargetNode* pTarget = (STargetNode*)pNode;
    SColumnNode* pColumn = (SColumnNode*)pTarget->pExpr;
    if (pColumn->dataBlockId == blkId) {
      (*pCols)[i].srcSlot = pColumn->slotId;
      (*pCols)[i].dstSlot = pTarget->slotId;
      ++i;
    }
  }  

  *colNum = i;

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinInitTableInfo(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode, SOperatorInfo** pDownstream, int32_t idx, SQueryStat* pStat) {
  SMJoinTableCtx* pTable = &pJoin->tbs[idx];
  pTable->downStream = pDownstream[idx];
  pTable->blkId = pDownstream[idx]->resultDataBlockId;
  MJ_ERR_RET(mJoinInitPrimKeyInfo(pTable, (0 == idx) ? pJoinNode->leftPrimSlotId : pJoinNode->rightPrimSlotId));

  MJ_ERR_RET(mJoinInitKeyColsInfo(pTable, (0 == idx) ? pJoinNode->pEqLeft : pJoinNode->pEqRight));
  MJ_ERR_RET(mJoinInitColsMap(&pTable->finNum, &pTable->finCols, pTable->blkId, pJoinNode->pTargets));

  memcpy(&pTable->inputStat, pStat, sizeof(*pStat));

  pTable->eqGrps = taosArrayInit(8, sizeof(SMJoinGrpRows));
  taosArrayReserve(pTable->eqGrps, 1);
  
  if (E_JOIN_TB_BUILD == pTable->type) {
    pTable->createdBlks = taosArrayInit(8, POINTER_BYTES);
    pTable->pGrpArrays = taosArrayInit(32, POINTER_BYTES);
    pTable->pGrpHash = tSimpleHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
    if (NULL == pTable->createdBlks || NULL == pTable->pGrpArrays || NULL == pTable->pGrpHash) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  
  return TSDB_CODE_SUCCESS;
}

static void mJoinSetBuildAndProbeTable(SMJoinOperatorInfo* pInfo, SSortMergeJoinPhysiNode* pJoinNode) {
  int32_t buildIdx = 0;
  int32_t probeIdx = 1;

  pInfo->joinType = pJoinNode->joinType;
  pInfo->subType = pJoinNode->subType;
  
  switch (pInfo->joinType) {
    case JOIN_TYPE_INNER:
    case JOIN_TYPE_FULL:
      if (pInfo->tbs[0].inputStat.inputRowNum <= pInfo->tbs[1].inputStat.inputRowNum) {
        buildIdx = 0;
        probeIdx = 1;
      } else {
        buildIdx = 1;
        probeIdx = 0;
      }
      break;
    case JOIN_TYPE_LEFT:
      buildIdx = 1;
      probeIdx = 0;
      break;
    case JOIN_TYPE_RIGHT:
      buildIdx = 0;
      probeIdx = 1;
      break;
    default:
      break;
  } 
  
  pInfo->build = &pInfo->tbs[buildIdx];
  pInfo->probe = &pInfo->tbs[probeIdx];
  
  pInfo->build->downStreamIdx = buildIdx;
  pInfo->probe->downStreamIdx = probeIdx;

  pInfo->build->type = E_JOIN_TB_BUILD;
  pInfo->probe->type = E_JOIN_TB_PROBE;
}

static int32_t mJoinInitCtx(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode) {
#if 0
  pJoin->joinFps = &gMJoinFps[pJoin->joinType][pJoin->subType];

  int32_t code = (*pJoin->joinFps->initJoinCtx)(pOperator, pJoin);
  if (code) {
    return code;
  }
#else
  return mJoinInitMergeCtx(pJoin, pJoinNode);
#endif
}

void mJoinSetDone(SOperatorInfo* pOperator) {
  setOperatorCompleted(pOperator);
  if (pOperator->pDownstreamGetParams) {
    freeOperatorParam(pOperator->pDownstreamGetParams[0], OP_GET_PARAM);
    freeOperatorParam(pOperator->pDownstreamGetParams[1], OP_GET_PARAM);
    pOperator->pDownstreamGetParams[0] = NULL;
    pOperator->pDownstreamGetParams[1] = NULL;
  }
}

bool mJoinRetrieveImpl(SMJoinOperatorInfo* pJoin, int32_t* pIdx, SSDataBlock** ppBlk, SMJoinTableCtx* pTb) {
  if (pTb->dsFetchDone) {
    return (NULL == (*ppBlk) || *pIdx >= (*ppBlk)->info.rows) ? false : true;
  }
  
  if (NULL == (*ppBlk) || *pIdx >= (*ppBlk)->info.rows) {
    (*ppBlk) = getNextBlockFromDownstreamRemain(pJoin->pOperator, pTb->downStreamIdx);
    pTb->dsInitDone = true;

    qDebug("%s merge join %s table got %" PRId64 " rows block", GET_TASKID(pJoin->pOperator->pTaskInfo), MJOIN_TBTYPE(pTb->type), (*ppBlk) ? (*ppBlk)->info.rows : 0);

    *pIdx = 0;
    if (NULL == (*ppBlk)) {
      pTb->dsFetchDone = true;
    } else {
      pTb->newBlk = true;
    }
    
    return ((*ppBlk) == NULL) ? false : true;
  }

  return true;
}

static void mJoinDestroyCreatedBlks(SArray* pCreatedBlks) {
  int32_t blkNum = taosArrayGetSize(pCreatedBlks);
  for (int32_t i = 0; i < blkNum; ++i) {
    blockDataDestroy(*(SSDataBlock**)TARRAY_GET_ELEM(pCreatedBlks, i));
  }
  taosArrayClear(pCreatedBlks);
}

void mJoinBuildEqGroups(SMJoinTableCtx* pTable, int64_t timestamp, bool* wholeBlk, bool restart) {
  SColumnInfoData* pCol = taosArrayGet(pTable->blk->pDataBlock, pTable->primCol->srcSlot);
  SMJoinGrpRows* pGrp = NULL;

  if (*(int64_t*)colDataGetData(pCol, pTable->blkRowIdx) != timestamp) {
    return;
  }

  if (restart) {
    pTable->grpTotalRows = 0;
    pTable->grpIdx = 0;
    mJoinDestroyCreatedBlks(pTable->createdBlks);
    taosArrayClear(pTable->eqGrps);
  }

  pGrp = taosArrayReserve(pTable->eqGrps, 1);

  pGrp->beginIdx = pTable->blkRowIdx++;
  pGrp->readIdx = pGrp->beginIdx;
  pGrp->endIdx = pGrp->beginIdx;
  pGrp->readMatch = false;
  pGrp->blk = pTable->blk;
  
  for (; pTable->blkRowIdx < pTable->blk->info.rows; ++pTable->blkRowIdx) {
    char* pNextVal = colDataGetData(pCol, pTable->blkRowIdx);
    if (timestamp == *(int64_t*)pNextVal) {
      pGrp->endIdx++;
      continue;
    }

    pTable->grpTotalRows += pGrp->endIdx - pGrp->beginIdx + 1;
    return;
  }

  if (wholeBlk) {
    *wholeBlk = true;
    if (0 == pGrp->beginIdx) {
      pGrp->blk = createOneDataBlock(pTable->blk, true);
    } else {
      pGrp->blk = blockDataExtractBlock(pTable->blk, pGrp->beginIdx, pGrp->endIdx - pGrp->beginIdx + 1);
      pGrp->endIdx -= pGrp->beginIdx;
      pGrp->beginIdx = 0;
      pGrp->readIdx = 0;
    }
    taosArrayPush(pTable->createdBlks, &pGrp->blk);
  }

  pTable->grpTotalRows += pGrp->endIdx - pGrp->beginIdx + 1;  
}


int32_t mJoinRetrieveEqGrpRows(SOperatorInfo* pOperator, SMJoinTableCtx* pTable, int64_t timestamp) {
  bool wholeBlk = false;
  
  mJoinBuildEqGroups(pTable, timestamp, &wholeBlk, true);
  
  while (wholeBlk) {
    pTable->blk = getNextBlockFromDownstreamRemain(pOperator, pTable->downStreamIdx);
    qDebug("%s merge join %s table got block for same ts, rows:%" PRId64, GET_TASKID(pOperator->pTaskInfo), MJOIN_TBTYPE(pTable->type), pTable->blk ? pTable->blk->info.rows : 0);

    pTable->blkRowIdx = 0;

    if (NULL == pTable->blk) {
      pTable->dsFetchDone = true;
      break;
    }

    wholeBlk = false;
    mJoinBuildEqGroups(pTable, timestamp, &wholeBlk, false);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mJoinSetKeyColsData(SSDataBlock* pBlock, SMJoinTableCtx* pTable) {
  for (int32_t i = 0; i < pTable->keyNum; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pTable->keyCols[i].srcSlot);
    if (pTable->keyCols[i].vardata != IS_VAR_DATA_TYPE(pCol->info.type))  {
      qError("column type mismatch, idx:%d, slotId:%d, type:%d, vardata:%d", i, pTable->keyCols[i].srcSlot, pCol->info.type, pTable->keyCols[i].vardata);
      return TSDB_CODE_INVALID_PARA;
    }
    if (pTable->keyCols[i].bytes != pCol->info.bytes)  {
      qError("column bytes mismatch, idx:%d, slotId:%d, bytes:%d, %d", i, pTable->keyCols[i].srcSlot, pCol->info.bytes, pTable->keyCols[i].bytes);
      return TSDB_CODE_INVALID_PARA;
    }
    pTable->keyCols[i].data = pCol->pData;
    if (pTable->keyCols[i].vardata) {
      pTable->keyCols[i].offset = pCol->varmeta.offset;
    }
    pTable->keyCols[i].colData = pCol;
  }

  return TSDB_CODE_SUCCESS;
}

bool mJoinCopyKeyColsDataToBuf(SMJoinTableCtx* pTable, int32_t rowIdx, size_t *pBufLen) {
  char *pData = NULL;
  size_t bufLen = 0;
  
  if (1 == pTable->keyNum) {
    if (colDataIsNull_s(pTable->keyCols[0].colData, rowIdx)) {
      return true;
    }
    if (pTable->keyCols[0].vardata) {
      pData = pTable->keyCols[0].data + pTable->keyCols[0].offset[rowIdx];
      bufLen = varDataTLen(pData);
    } else {
      pData = pTable->keyCols[0].data + pTable->keyCols[0].bytes * rowIdx;
      bufLen = pTable->keyCols[0].bytes;
    }
    pTable->keyData = pData;
  } else {
    for (int32_t i = 0; i < pTable->keyNum; ++i) {
      if (colDataIsNull_s(pTable->keyCols[i].colData, rowIdx)) {
        return true;
      }
      if (pTable->keyCols[i].vardata) {
        pData = pTable->keyCols[i].data + pTable->keyCols[i].offset[rowIdx];
        memcpy(pTable->keyBuf + bufLen, pData, varDataTLen(pData));
        bufLen += varDataTLen(pData);
      } else {
        pData = pTable->keyCols[i].data + pTable->keyCols[i].bytes * rowIdx;
        memcpy(pTable->keyBuf + bufLen, pData, pTable->keyCols[i].bytes);
        bufLen += pTable->keyCols[i].bytes;
      }
    }
    pTable->keyData = pTable->keyBuf;
  }

  if (pBufLen) {
    *pBufLen = bufLen;
  }

  return false;
}

static int32_t mJoinGetAvailableGrpArray(SMJoinTableCtx* pTable, SArray** ppRes) {
  do {
    if (pTable->grpArrayIdx < taosArrayGetSize(pTable->pGrpArrays)) {
      *ppRes = taosArrayGetP(pTable->pGrpArrays, pTable->grpArrayIdx++);
      taosArrayClear(*ppRes);
      return TSDB_CODE_SUCCESS;
    }

    SArray* pNew = taosArrayInit(4, sizeof(SMJoinRowPos));
    if (NULL == pNew) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosArrayPush(pTable->pGrpArrays, &pNew);
  } while (true);

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinAddRowToHash(SMJoinOperatorInfo* pJoin, size_t keyLen, SSDataBlock* pBlock, int32_t rowIdx) {
  SMJoinTableCtx* pBuild = pJoin->build;
  SMJoinRowPos pos = {pBlock, rowIdx};
  SArray** pGrpRows = tSimpleHashGet(pBuild->pGrpHash, pBuild->keyData, keyLen);
  if (!pGrpRows) {
    SArray* pNewGrp = NULL;
    MJ_ERR_RET(mJoinGetAvailableGrpArray(pBuild, &pNewGrp));

    taosArrayPush(pNewGrp, &pos);
    tSimpleHashPut(pBuild->pGrpHash, pBuild->keyData, keyLen, &pNewGrp, POINTER_BYTES);
  } else {
    taosArrayPush(*pGrpRows, &pos);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t mJoinMakeBuildTbHash(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pTable) {
  size_t bufLen = 0;

  tSimpleHashClear(pJoin->build->pGrpHash);
  pJoin->build->grpArrayIdx = 0;

  pJoin->build->grpRowIdx = -1;
  
  int32_t grpNum = taosArrayGetSize(pTable->eqGrps);
  for (int32_t g = 0; g < grpNum; ++g) {
    SMJoinGrpRows* pGrp = taosArrayGet(pTable->eqGrps, g);
    MJ_ERR_RET(mJoinSetKeyColsData(pGrp->blk, pTable));

    int32_t grpRows = GRP_REMAIN_ROWS(pGrp);
    for (int32_t r = 0; r < grpRows; ++r) {
      if (mJoinCopyKeyColsDataToBuf(pTable, pGrp->beginIdx + r, &bufLen)) {
        continue;
      }

      MJ_ERR_RET(mJoinAddRowToHash(pJoin, bufLen, pGrp->blk, pGrp->beginIdx + r));
    }
  }

  return TSDB_CODE_SUCCESS;
}

void mJoinResetTableCtx(SMJoinTableCtx* pCtx) {
  pCtx->dsInitDone = false;
  pCtx->dsFetchDone = false;

  mJoinDestroyCreatedBlks(pCtx->createdBlks);
  tSimpleHashClear(pCtx->pGrpHash);
}

void mJoinResetMergeCtx(SMJoinMergeCtx* pCtx) {
  pCtx->grpRemains = false;
  pCtx->midRemains = false;
  pCtx->lastEqGrp = false;

  pCtx->lastEqTs = INT64_MIN;
  pCtx->hashJoin = false;
}

void mJoinResetCtx(SMJoinOperatorInfo* pJoin) {
  mJoinResetMergeCtx(&pJoin->ctx.mergeCtx);
}

void mJoinResetOperator(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;

  mJoinResetTableCtx(pJoin->build);
  mJoinResetTableCtx(pJoin->probe);

  mJoinResetCtx(pJoin);

  pOperator->status = OP_OPENED;
}

SSDataBlock* mJoinMainProcess(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    if (NULL == pOperator->pDownstreamGetParams || NULL == pOperator->pDownstreamGetParams[0] || NULL == pOperator->pDownstreamGetParams[1]) {
      qDebug("%s merge join done", GET_TASKID(pOperator->pTaskInfo));
      return NULL;
    } else {
      mJoinResetOperator(pOperator);
      qDebug("%s start new round merge join", GET_TASKID(pOperator->pTaskInfo));
    }
  }

  int64_t st = 0;
  if (pOperator->cost.openCost == 0) {
    st = taosGetTimestampUs();
  }

  SSDataBlock* pBlock = NULL;
  while (true) {
    //pBlock = (*pJoin->joinFps)(pOperator);
    pBlock = mLeftJoinDo(pOperator);
    if (NULL == pBlock) {
      if (pJoin->errCode) {
        ASSERT(0);
        T_LONG_JMP(pOperator->pTaskInfo->env, pJoin->errCode);
      }
      break;
    }
    if (pJoin->pFinFilter != NULL) {
      doFilter(pBlock, pJoin->pFinFilter, NULL);
    }
    
    if (pBlock->info.rows > 0 || pOperator->status == OP_EXEC_DONE) {
      break;
    }
  }

  if (pOperator->cost.openCost == 0) {
    pOperator->cost.openCost = (taosGetTimestampUs() - st) / 1000.0;
  }
  
  return (pBlock && pBlock->info.rows > 0) ? pBlock : NULL;
}


void destroyMergeJoinOperator(void* param) {
  SOperatorInfo* pOperator = (SOperatorInfo*)param;
  SMJoinOperatorInfo* pJoin = pOperator->info;
  pJoin->ctx.mergeCtx.finBlk = blockDataDestroy(pJoin->ctx.mergeCtx.finBlk);
  pJoin->ctx.mergeCtx.midBlk = blockDataDestroy(pJoin->ctx.mergeCtx.midBlk);

  mJoinDestroyCreatedBlks(pJoin->probe->createdBlks);
  taosArrayDestroy(pJoin->probe->createdBlks);
  tSimpleHashCleanup(pJoin->probe->pGrpHash);

  mJoinDestroyCreatedBlks(pJoin->build->createdBlks);
  taosArrayDestroy(pJoin->build->createdBlks);
  tSimpleHashCleanup(pJoin->build->pGrpHash);

  taosMemoryFreeClear(param);
}


SOperatorInfo* createMergeJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                           SSortMergeJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo) {
  SMJoinOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SMJoinOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  bool newDownstreams = false;
  
  int32_t code = TSDB_CODE_SUCCESS;
  if (pOperator == NULL || pInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }

  pInfo->pOperator = pOperator;
  MJ_ERR_JRET(mJoinInitDownstreamInfo(pInfo, pDownstream, &numOfDownstream, &newDownstreams));

  setOperatorInfo(pOperator, "MergeJoinOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN, false, OP_NOT_OPENED, pInfo, pTaskInfo);

  mJoinSetBuildAndProbeTable(pInfo, pJoinNode);

  mJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 0, &pJoinNode->inputStat[0]);
  mJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 1, &pJoinNode->inputStat[1]);
    
  if (pJoinNode->pFullOnCond != NULL) {
    MJ_ERR_JRET(filterInitFromNode(pJoinNode->pFullOnCond, &pInfo->pFPreFilter, 0));
  }

  if (pJoinNode->pColOnCond != NULL) {
    MJ_ERR_JRET(filterInitFromNode(pJoinNode->pColOnCond, &pInfo->pPreFilter, 0));
  }

  if (pJoinNode->node.pConditions != NULL) {
    MJ_ERR_JRET(filterInitFromNode(pJoinNode->node.pConditions, &pInfo->pFinFilter, 0));
  }

  MJ_ERR_JRET(mJoinInitCtx(pInfo, pJoinNode));

  if (pJoinNode->node.inputTsOrder == ORDER_ASC) {
    pInfo->inputTsOrder = TSDB_ORDER_ASC;
  } else if (pJoinNode->node.inputTsOrder == ORDER_DESC) {
    pInfo->inputTsOrder = TSDB_ORDER_DESC;
  } else {
    pInfo->inputTsOrder = TSDB_ORDER_ASC;
  }

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, mJoinMainProcess, NULL, destroyMergeJoinOperator, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  MJ_ERR_JRET(appendDownstream(pOperator, pDownstream, numOfDownstream));

  if (newDownstreams) {
    taosMemoryFree(pDownstream);
    pOperator->numOfRealDownstream = 1;
  } else {
    pOperator->numOfRealDownstream = 2;
  }
  
  return pOperator;

_return:
  if (pInfo != NULL) {
    destroyMergeJoinOperator(pInfo);
  }
  if (newDownstreams) {
    taosMemoryFree(pDownstream);
  }

  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

