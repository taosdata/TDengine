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
    pInfo->downstreamResBlkId[0] = getOperatorResultBlockId(p[0], 0);
    pInfo->downstreamResBlkId[1] = getOperatorResultBlockId(p[1], 1);
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
  } else {
    pInfo->downstreamResBlkId[0] = getOperatorResultBlockId(pDownstream[0], 0);
    pInfo->downstreamResBlkId[1] = getOperatorResultBlockId(pDownstream[1], 0);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinInitPrimKeyInfo(SMJoinTableInfo* pTable, int32_t slotId) {
  pTable->primCol = taosMemoryMalloc(sizeof(SMJoinColInfo));
  if (NULL == pTable->primCol) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTable->primCol->srcSlot = slotId;

  return TSDB_CODE_SUCCESS;
}

static void mJoinGetValColNum(SNodeList* pList, int32_t blkId, int32_t* colNum) {
  *colNum = 0;
  
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    STargetNode* pTarget = (STargetNode*)pNode;
    SColumnNode* pCol = (SColumnNode*)pTarget->pExpr;
    if (pCol->dataBlockId == blkId) {
      (*colNum)++;
    }
  }
}

static int32_t mJoinInitValColsInfo(SMJoinTableInfo* pTable, SNodeList* pList) {
  mJoinGetValColNum(pList, pTable->blkId, &pTable->valNum);
  if (pTable->valNum == 0) {
    return TSDB_CODE_SUCCESS;
  }
  
  pTable->valCols = taosMemoryMalloc(pTable->valNum * sizeof(SMJoinColInfo));
  if (NULL == pTable->valCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t i = 0;
  int32_t colNum = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    STargetNode* pTarget = (STargetNode*)pNode;
    SColumnNode* pColNode = (SColumnNode*)pTarget->pExpr;
    if (pColNode->dataBlockId == pTable->blkId) {
      if (valColInKeyCols(pColNode->slotId, pTable->keyNum, pTable->keyCols, &pTable->valCols[i].srcSlot)) {
        pTable->valCols[i].keyCol = true;
      } else {
        pTable->valCols[i].keyCol = false;
        pTable->valCols[i].srcSlot = pColNode->slotId;
        pTable->valColExist = true;
        colNum++;
      }
      pTable->valCols[i].dstSlot = pTarget->slotId;
      pTable->valCols[i].vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
      if (pTable->valCols[i].vardata) {
        if (NULL == pTable->valVarCols) {
          pTable->valVarCols = taosArrayInit(pTable->valNum, sizeof(int32_t));
          if (NULL == pTable->valVarCols) {
            return TSDB_CODE_OUT_OF_MEMORY;
          }
        }
        taosArrayPush(pTable->valVarCols, &i);
      }
      pTable->valCols[i].bytes = pColNode->node.resType.bytes;
      if (!pTable->valCols[i].keyCol && !pTable->valCols[i].vardata) {
        pTable->valBufSize += pColNode->node.resType.bytes;
      }
      i++;
    }
  }

  pTable->valBitMapSize = BitmapLen(colNum);
  pTable->valBufSize += pTable->valBitMapSize;

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinInitKeyColsInfo(SMJoinTableInfo* pTable, SNodeList* pList) {
  pTable->keyNum = LIST_LENGTH(pList);
  
  pTable->keyCols = taosMemoryMalloc(pTable->keyNum * sizeof(SMJoinColInfo));
  if (NULL == pTable->keyCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int64_t bufSize = 0;
  int32_t i = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SColumnNode* pColNode = (SColumnNode*)pNode;
    pTable->keyCols[i].srcSlot = pColNode->slotId;
    pTable->keyCols[i].vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
    pTable->keyCols[i].bytes = pColNode->node.resType.bytes;
    bufSize += pColNode->node.resType.bytes;
    ++i;
  }  

  if (pTable->keyNum > 1) {
    pTable->keyBuf = taosMemoryMalloc(bufSize);
    if (NULL == pTable->keyBuf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinInitTableInfo(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode, SOperatorInfo** pDownstream, int32_t idx, SQueryStat* pStat) {
  SMJoinTableInfo* pTable = &pJoin->tbs[idx];
  pTable->downStream = pDownstream[idx];
  pTable->blkId = pDownstream[idx]->resultDataBlockId;
  int32_t code = mJoinInitPrimKeyInfo(pTable, (0 == idx) ? pJoinNode->leftPrimSlotId : pJoinNode->rightPrimSlotId);
  if (code) {
    return code;
  }
  code = mJoinInitKeyColsInfo(pTable, (0 == idx) ? pJoinNode->pEqLeft : pJoinNode->pEqRight);
  if (code) {
    return code;
  }
  code = mJoinInitValColsInfo(pTable, pJoinNode->pTargets);
  if (code) {
    return code;
  }

  memcpy(&pTable->inputStat, pStat, sizeof(*pStat));

  pTable->eqGrps = taosArrayInit(8, sizeof(SMJoinGrpRows));
  if (E_JOIN_TB_BUILD == pTable->type) {
    pTable->createdBlks = taosArrayInit(8, POINTER_BYTES);
    pTable->pGrpArrays = taosArrayInit(32, POINTER_BYTES);
    pTable->pGrpHash = tSimpleHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
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

static int32_t mJoinBuildResColMap(SMJoinOperatorInfo* pInfo, SSortMergeJoinPhysiNode* pJoinNode) {
  pInfo->pResColNum = pJoinNode->pTargets->length;
  pInfo->pResColMap = taosMemoryCalloc(pJoinNode->pTargets->length, sizeof(int8_t));
  if (NULL == pInfo->pResColMap) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  SNode* pNode = NULL;
  int32_t i = 0;
  FOREACH(pNode, pJoinNode->pTargets) {
    STargetNode* pTarget = (STargetNode*)pNode;
    SColumnNode* pCol = (SColumnNode*)pTarget->pExpr;
    if (pCol->dataBlockId == pInfo->build->blkId) {
      pInfo->pResColMap[i] = 1;
    }
    
    i++;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinInitMergeCtx(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode) {
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;

  pCtx->lastEqTs = INT64_MIN;
  pCtx->hashCan = pJoin->probe->keyNum > 0;

  pCtx->finBlk = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);
  blockDataEnsureCapacity(pCtx->finBlk, TMAX(MJOIN_DEFAULT_BLK_ROWS_NUM, MJOIN_BLK_SIZE_LIMIT/pJoinNode->node.pOutputDataBlockDesc.totalRowSize));

  if (pJoin->pFPreFilter) {
    pCtx->midBlk = createOneDataBlock(pCtx->finBlk, false);
    blockDataEnsureCapacity(pCtx->midBlk, pCtx->finBlk->info.capacity);
  }

  pCtx->blkThreshold = pCtx->finBlk->info.capacity * 0.5;
  
  return TSDB_CODE_SUCCESS;
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

static void mJoinSetDone(SOperatorInfo* pOperator) {
  setOperatorCompleted(pOperator);
  if (pOperator->pDownstreamGetParams) {
    freeOperatorParam(pOperator->pDownstreamGetParams[0], OP_GET_PARAM);
    freeOperatorParam(pOperator->pDownstreamGetParams[1], OP_GET_PARAM);
    pOperator->pDownstreamGetParams[0] = NULL;
    pOperator->pDownstreamGetParams[1] = NULL;
  }
}

static int32_t mLeftJoinGrpNonEqCart(SMJoinOperatorInfo* pJoin, SSDataBlock* pRes, bool append, SMJoinGrpRows* pGrp) {
  SMJoinTableInfo* probe = pJoin->probe;
  SMJoinTableInfo* build = pJoin->build;
  int32_t currRows = append ? pRes->info.rows : 0;
  int32_t firstRows = GRP_REMAIN_ROWS(pGrp);
  
  for (int32_t c = 0; c < probe->finNum; ++c) {
    SMJoinColMap* pFirstCol = probe->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pGrp->blk, pFirstCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pRes->pDataBlock, pFirstCol->dstSlot);
    colDataAssignNRows(pOutCol, currRows, pInCol, pGrp->readIdx, firstRows);
  }
  
  for (int32_t c = 0; c < build->finNum; ++c) {
    SMJoinColMap* pSecondCol = build->finCols + c;
    SColumnInfoData* pOutCol = taosArrayGet(pRes->pDataBlock, pSecondCol->dstSlot);
    colDataSetNItemsNull(pOutCol, currRows, firstRows);
  }
  
  pRes->info.rows = append ? (pRes->info.rows + firstRows) : firstRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t mLeftJoinGrpEqCart(SMJoinOperatorInfo* pJoin, SSDataBlock* pRes, bool append, SMJoinGrpRows* pFirst, SMJoinGrpRows* pSecond) {
  SMJoinTableInfo* probe = pJoin->probe;
  SMJoinTableInfo* build = pJoin->build;
  int32_t currRows = append ? pRes->info.rows : 0;
  int32_t firstRows = GRP_REMAIN_ROWS(pFirst);  
  int32_t secondRows = GRP_REMAIN_ROWS(pSecond);

  for (int32_t c = 0; c < probe->finNum; ++c) {
    SMJoinColMap* pFirstCol = probe->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pFirst->blk, pFirstCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pRes->pDataBlock, pFirstCol->dstSlot);
    for (int32_t r = 0; r < firstRows; ++r) {
      if (colDataIsNull_s(pInCol, pFirst->readIdx + r)) {
        colDataSetNItemsNull(pOutCol, currRows + r * secondRows, secondRows);
      } else {
        colDataSetNItems(pOutCol, currRows + r * secondRows, colDataGetData(pInCol, pFirst->beginIdx + r), secondRows, true);
      }
    }
  }

  for (int32_t c = 0; c < build->finNum; ++c) {
    SMJoinColMap* pSecondCol = build->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pSecond->blk, pSecondCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pRes->pDataBlock, pSecondCol->dstSlot);
    for (int32_t r = 0; r < firstRows; ++r) {
      colDataAssignNRows(pOutCol, currRows + r * secondRows, pInCol, pSecond->readIdx, secondRows);
    }
  }

  pRes->info.rows = append ? (pRes->info.rows + firstRows * secondRows) : firstRows * secondRows;
  return TSDB_CODE_SUCCESS;
}


static int32_t mLeftJoinMergeFullCart(SMJoinMergeCtx* pCtx) {
  int32_t rowsLeft = pCtx->finBlk->info.capacity - pCtx->finBlk->info.rows;
  SMJoinTableInfo* probe = pCtx->pJoin->probe;
  SMJoinTableInfo* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, 0);
  int32_t buildGrpNum = taosArrayGetSize(build->eqGrps);
  int32_t probeRows = GRP_REMAIN_ROWS(probeGrp);
  int32_t probeEndIdx = probeGrp->endIdx;

  if (probeRows * build->grpTotalRows <= rowsLeft) {
    for (; build->grpIdx < buildGrpNum; ++build->grpIdx) {
      SMJoinGrpRows* buildGrp = taosArrayGet(build->eqGrps, build->grpIdx);
      MJ_ERR_RET(mLeftJoinGrpEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp));
    }

    pCtx->grpRemains = false;
    return TSDB_CODE_SUCCESS;
  }

  for (; !GRP_DONE(probeGrp); ++probeGrp->readIdx, build->grpIdx = 0) {
    probeGrp->endIdx = probeGrp->readIdx;
    for (; build->grpIdx < buildGrpNum; ++build->grpIdx) {
      SMJoinGrpRows* buildGrp = taosArrayGet(build->eqGrps, build->grpIdx);

      if (rowsLeft >= GRP_REMAIN_ROWS(buildGrp)) {
        MJ_ERR_RET(mLeftJoinGrpEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp));
        rowsLeft -= GRP_REMAIN_ROWS(buildGrp);
        continue;
      }
      
      int32_t buildEndIdx = buildGrp->endIdx;
      buildGrp->endIdx = buildGrp->readIdx + rowsLeft - 1;
      mLeftJoinGrpEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp);
      buildGrp->readIdx += rowsLeft;
      buildGrp->endIdx = buildEndIdx;
      rowsLeft = 0;
      break;
    }

    if (rowsLeft <= 0) {
      break;
    }
  }

  probeGrp->endIdx = probeEndIdx;        

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;
  
  return TSDB_CODE_SUCCESS;  
}

static int32_t mLeftJoinCopyMergeMidBlk(SMJoinMergeCtx* pCtx, SSDataBlock** ppMid, SSDataBlock** ppFin) {
  SSDataBlock* pLess = NULL;
  SSDataBlock* pMore = NULL;
  if ((*ppMid)->info.rows < ppFin->info.rows) {
    pLess = (*ppMid);
    pMore = (*ppFin);
  } else {
    pLess = (*ppFin);
    pMore = (*ppMid);
  }

  int32_t totalRows = pMore->info.rows + pLess->info.rows;
  if (totalRows <= pMore->info.capacity) {
    MJ_ERR_RET(blockDataMerge(pMore, pLess));
    tDataBlkReset(pLess);
    pCtx->midRemains = false;
  } else {
    int32_t copyRows = pMore->info.capacity - pMore->info.rows;
    MJ_ERR_RET(blockDataMergeNRows(pMore, pLess, pLess->info.rows - copyRows, copyRows));
    pCtx->midRemains = true;
  }

  if (pMore != (*ppFin)) {
    TSWAP(*ppMid, *ppFin);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mLeftJoinMergeSeqCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableInfo* probe = pCtx->pJoin->probe;
  SMJoinTableInfo* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);
  int32_t buildGrpNum = taosArrayGetSize(build->eqGrps);
  int32_t probeEndIdx = probeGrp->endIdx;
  int32_t rowsLeft = pCtx->midBlk->info.capacity;  
  bool contLoop = true;

  blockDataReset(pCtx->midBlk);

  do {
    for (; !GRP_DONE(probeGrp) && !BLK_IS_FULL(pCtx->finBlk); ++probeGrp->readIdx, probeGrp->readMatch = false, build->grpIdx = 0) {
      probeGrp->endIdx = probeGrp->readIdx;
      for (; build->grpIdx < buildGrpNum; ++build->grpIdx) {
        SMJoinGrpRows* buildGrp = taosArrayGet(build->eqGrps, build->grpIdx);

        if (rowsLeft >= GRP_REMAIN_ROWS(buildGrp)) {
          MJ_ERR_RET(mLeftJoinGrpEqCart(pCtx->pJoin, pCtx->midBlk, true, probeGrp, buildGrp));
          rowsLeft -= GRP_REMAIN_ROWS(buildGrp);
          continue;
        }
        
        int32_t buildEndIdx = buildGrp->endIdx;
        buildGrp->endIdx = buildGrp->readIdx + rowsLeft - 1;
        MJ_ERR_RET(mLeftJoinGrpEqCart(pCtx->pJoin, pCtx->midBlk, true, probeGrp, buildGrp));
        buildGrp->readIdx += rowsLeft;
        buildGrp->endIdx = buildEndIdx;
        rowsLeft = 0;
        break;
      }

      if (pCtx->midBlk->info.rows > 0) {
        MJ_ERR_RET(doFilter(pCtx->midBlk, pCtx->pJoin->pFPreFilter, NULL));
        if (pCtx->midBlk->info.rows > 0) {
          probeGrp->readMatch = true;
        }
      } 

      if (0 == pCtx->midBlk->info.rows) {
        if (build->grpIdx == buildGrpNum) {
          if (!probeGrp->readMatch) {
            MJ_ERR_RET(mLeftJoinGrpNonEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp));
          }

          continue;
        }
        
        break;
      } else {
        MJ_ERR_RET(mLeftJoinCopyMergeMidBlk(pCtx, &pCtx->midBlk, &pCtx->finBlk));
        
        if (pCtx->midRemains) {
          contLoop = false;
          break;
        }

        if (build->grpIdx == buildGrpNum) {
          continue;
        }

        break;
      }
    }

    if (GRP_DONE(probeGrp->readIdx) || BLK_IS_FULL(pCtx->finBlk)) {
      break;
    }

    rowsLeft = pCtx->midBlk->info.capacity;
  } while (contLoop);

  probeGrp->endIdx = probeEndIdx;        

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}

static int32_t mLeftJoinMergeCart(SMJoinMergeCtx* pCtx) {
  return (NULL == pCtx->pJoin->pFPreFilter) ? mLeftJoinMergeFullCart(pCtx) : mLeftJoinMergeSeqCart(pCtx);
}

static int32_t mLeftJoinNonEqCart(SMJoinMergeCtx* pCtx) {
  int32_t rowsLeft = pCtx->finBlk->info.capacity - pCtx->finBlk->info.rows;
  SMJoinGrpRows* probeGrp = &pCtx->probeNEqGrp;
  int32_t probeRows = GRP_REMAIN_ROWS(probeGrp);

  pCtx->lastEqGrp = false;

  if (probeRows <= rowsLeft) {
    MJ_ERR_RET(mLeftJoinGrpNonEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp));
    probeGrp->readIdx = probeGrp->endIdx + 1;
    pCtx->grpRemains = false;
  } else {
    int32_t probeEndIdx = probeGrp->endIdx;
    probeGrp->endIdx = probeGrp->readIdx + rowsLeft - 1;
    MJ_ERR_RET(mLeftJoinGrpNonEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp));
    probeGrp->readIdx = probeGrp->endIdx + 1;
    probeGrp->endIdx = probeEndIdx;
    pCtx->grpRemains = true;
  }

  return TSDB_CODE_SUCCESS;
}



static bool mJoinRetrieveImpl(SMJoinOperatorInfo* pJoin, int32_t* pIdx, SSDataBlock** ppBlk, SMJoinTableInfo* ppTb) {
  if ((*ppTb)->dsFetchDone) {
    return (NULL == (*ppBlk) || *pIdx >= (*ppBlk)->info.rows) ? false : true;
  }
  
  if (NULL == (*ppBlk) || *pIdx >= (*ppBlk)->info.rows) {
    (*ppBlk) = getNextBlockFromDownstreamRemain(pJoin->pOperator, (*ppTb)->downStreamIdx);
    (*ppTb)->dsInitDone = true;

    qDebug("%s merge join %s table got %" PRId64 " rows block", GET_TASKID(pJoin->pOperator->pTaskInfo), MJOIN_TBTYPE(ppTb->type), (*ppBlk) ? (*ppBlk)->info.rows : 0);

    *pIdx = 0;
    if (NULL == (*ppBlk)) {
      (*ppTb)->dsFetchDone = true;
    }
    
    return ((*ppBlk) == NULL) ? false : true;
  }

  return true;
}


static bool mLeftJoinRetrieve(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin, SMJoinMergeCtx* pCtx) {
  bool probeGot = mJoinRetrieveImpl(pJoin, &pJoin->probe->blkRowIdx, &pJoin->probe->blk, &pJoin->probe);
  bool buildGot = false;

  do {
    if (probeGot || MJOIN_DS_NEED_INIT(pOperator, pJoin->build)) {  
      buildGot = mJoinRetrieveImpl(pJoin, &pJoin->build->blkRowIdx, &pJoin->build->blk, &pJoin->build);
    }
    
    if (!probeGot) {
      mJoinSetDone(pOperator);
      return false;
    }

    if (buildGot) {
      SColumnInfoData* pProbeCol = taosArrayGet(pJoin->probe->blk->pDataBlock, pJoin->probe->primCol->srcSlot);
      SColumnInfoData* pBuildCol = taosArrayGet(pJoin->build->blk->pDataBlock, pJoin->build->primCol->srcSlot);
      if (*((int64_t*)pProbeCol->pData + pJoin->probe->blkRowIdx) > *((int64_t*)pBuildCol->pData + pJoin->build->blk->info.rows - 1)) {
        continue;
      }
    }
    
    break;
  } while (true);

  return true;
}

static int32_t mergeJoinGetBlockRowsEqualTs(SSDataBlock* pBlock, int16_t tsSlotId, int32_t startPos, int64_t timestamp,
                                            int32_t* pEndPos, SArray* rowLocations, SArray* createdBlocks) {
  int32_t numRows = pBlock->info.rows;
  ASSERT(startPos < numRows);
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, tsSlotId);

  int32_t i = startPos;
  for (; i < numRows; ++i) {
    char* pNextVal = colDataGetData(pCol, i);
    if (timestamp != *(int64_t*)pNextVal) {
      break;
    }
  }
  int32_t endPos = i;
  *pEndPos = endPos;

  if (endPos - startPos == 0) {
    return 0;
  }

  SSDataBlock* block = pBlock;
  bool         createdNewBlock = false;
  if (endPos == numRows) {
    block = blockDataExtractBlock(pBlock, startPos, endPos - startPos);
    taosArrayPush(createdBlocks, &block);
    createdNewBlock = true;
  }
  SRowLocation location = {0};
  for (int32_t j = startPos; j < endPos; ++j) {
    location.pDataBlock = block;
    location.pos = (createdNewBlock ? j - startPos : j);
    taosArrayPush(rowLocations, &location);
  }
  return 0;
}


static void mJoinBuildEqGroups(SOperatorInfo* pOperator, SMJoinTableInfo* pTable, int64_t timestamp, bool* wholeBlk, bool restart) {
  SColumnInfoData* pCol = taosArrayGet(pTable->blk->pDataBlock, pTable->primCol->srcSlot);
  SMJoinGrpRows* pGrp = NULL;

  if (restart) {
    pTable->grpTotalRows = 0;
    pTable->grpIdx = 0;
    pGrp = taosArrayGet(pTable->eqGrps, 0);
  } else {
    pGrp = taosArrayReserve(pTable->eqGrps, 1);
  }

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


static int32_t mJoinRetrieveEqGrpRows(SOperatorInfo* pOperator, SMJoinTableInfo* pTable, int64_t timestamp) {
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

static int32_t mJoinSetKeyColsData(SSDataBlock* pBlock, SMJoinTableInfo* pTable) {
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

static FORCE_INLINE true mJoinCopyKeyColsDataToBuf(SMJoinTableInfo* pTable, int32_t rowIdx, size_t *pBufLen) {
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

static int32_t mJoinGetAvailableGrpArray(SMJoinTableInfo* pTable, SArray** ppRes) {
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
  SMJoinTableInfo* pBuild = pJoin->build;
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


static int32_t mJoinMakeBuildTbHash(SMJoinOperatorInfo* pJoin, SMJoinTableInfo* pTable) {
  size_t bufLen = 0;

  tSimpleHashClear(pJoin->build->pGrpHash);
  pJoin->build->grpArrayIdx = 0;
  
  int32_t grpNum = taosArrayGetSize(pTable->eqGrps);
  for (int32_t g = 0; g < grpNum; ++g) {
    SMJoinGrpRows* pGrp = taosArrayGet(pTable->eqGrps, g);
    MJ_ERR_RET(mJoinSetKeyColsData(pGrp->blk, pTable));

    int32_t grpRows = GRP_REMAIN_ROWS(pGrp);
    for (int32_t r = 0; r < grpRows; ++r) {
      if (mJoinCopyKeyColsDataToBuf(pTable, r, &bufLen)) {
        continue;
      }

      MJ_ERR_RET(mJoinAddRowToHash(pJoin, bufLen, pGrp->blk, pGrp->beginIdx + r));
    }
  }

  return TSDB_CODE_SUCCESS;
}

static bool mLeftJoinHashGrpCart(SSDataBlock* pBlk, SMJoinGrpRows* probeGrp, bool append, SMJoinTableInfo* probe, SMJoinTableInfo* build) {
  int32_t rowsLeft = append ? (pBlk->info.capacity - pBlk->info.rows) : pBlk->info.capacity;
  if (rowsLeft <= 0) {
    return false;
  }
  
  int32_t buildGrpRows = taosArrayGetSize(build->pHashCurGrp);
  int32_t grpRows = buildGrpRows - build->grpRowIdx;
  if (grpRows <= 0) {
    return true;
  }
  
  int32_t actRows = TMIN(grpRows, rowsLeft);
  int32_t currRows = append ? pBlk->info.rows : 0;

  for (int32_t c = 0; c < probe->finNum; ++c) {
    SMJoinColMap* pFirstCol = probe->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(probeGrp->blk, pFirstCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pBlk->pDataBlock, pFirstCol->dstSlot);
    if (colDataIsNull_s(pInCol, probeGrp->readIdx)) {
      colDataSetNItemsNull(pOutCol, currRows, actRows);
    } else {
      colDataSetNItems(pOutCol, currRows, colDataGetData(pInCol, probeGrp->readIdx), actRows, true);
    }
  }

  for (int32_t c = 0; c < build->finNum; ++c) {
    SMJoinColMap* pSecondCol = build->finCols + c;
    SColumnInfoData* pOutCol = taosArrayGet(pBlk->pDataBlock, pSecondCol->dstSlot);
    for (int32_t r = 0; r < actRows; ++r) {
      SMJoinRowPos* pRow = taosArrayGet(build->pHashCurGrp, r);
      SColumnInfoData* pInCol = taosArrayGet(pRow->pBlk, pSecondCol->srcSlot);
      colDataAssignNRows(pOutCol, currRows + r, pInCol, pRow->pos, 1);
    }
  }

  pBlk->info.rows += actRows;
  
  if (actRows == grpRows) {
    build->grpRowIdx = -1;
  } else {
    build->grpRowIdx += actRows;
  }
  
  if (actRows == rowsLeft) {
    return false;
  }

  return true;
}

static int32_t mLeftJoinHashFullCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableInfo* probe = pCtx->pJoin->probe;
  SMJoinTableInfo* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);

  if (build->grpRowIdx >= 0) {
    bool contLoop = mLeftJoinHashGrpCart(pCtx->finBlk, probeGrp, true, probe, build);
    if (build->grpRowIdx < 0) {
      probeGrp->readIdx++;
    }
    
    if (!contLoop) {
      goto _return;
    }
  }

  size_t bufLen = 0;
  int32_t probeEndIdx = probeGrp->endIdx;
  for (; !GRP_DONE(probeGrp) && !BLK_IS_FULL(pCtx->finBlk); ++probeGrp->readIdx) {
    if (mJoinCopyKeyColsDataToBuf(probe, probeGrp->readIdx, &bufLen)) {
      continue;
    }

    SArray** pGrp = tSimpleHashGet(build->pGrpHash, probe->keyData, bufLen);
    if (NULL == pGrp) {
      probeGrp->endIdx = probeGrp->readIdx;
      MJ_ERR_RET(mLeftJoinGrpNonEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp));
      probeGrp->endIdx = probeEndIdx;
    } else {
      build->pHashCurGrp = *pGrp;
      build->grpRowIdx = 0;
      if (!mLeftJoinHashGrpCart(pCtx->finBlk, probeGrp, true, probe, build)) {
        break;
      }
    }
  }

_return:

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}

static int32_t mLeftJoinHashGrpCartFilter(SMJoinMergeCtx* pCtx, bool* contLoop) {
  SMJoinTableInfo* probe = pCtx->pJoin->probe;
  SMJoinTableInfo* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);

  blockDataReset(pCtx->midBlk);

  do {
    mLeftJoinHashGrpCart(pCtx->midBlk, probeGrp, true, probe, build);
    if (build->grpRowIdx < 0) {
      probeGrp->readIdx++;
    }

    if (pCtx->midBlk->info.rows > 0) {
      MJ_ERR_RET(doFilter(pCtx->midBlk, pCtx->pJoin->pPreFilter, NULL));
      if (pCtx->midBlk->info.rows > 0) {
        probeGrp->readMatch = true;
      }
    } 

    if (0 == pCtx->midBlk->info.rows) {
      if (build->grpRowIdx < 0) {
        if (!probeGrp->readMatch) {
          MJ_ERR_RET(mLeftJoinGrpNonEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp));
        }

        probeGrp->readMatch = false;
        break;
      }
      
      continue;
    } else {
      MJ_ERR_RET(mLeftJoinCopyMergeMidBlk(pCtx, &pCtx->midBlk, &pCtx->finBlk));
      
      if (pCtx->midRemains) {
        pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;
        *contLoop = false;
        return TSDB_CODE_SUCCESS;
      }

      if (build->grpRowIdx < 0) {
        probeGrp->readMatch = false;
        break;
      }

      continue;
    }
  } while (true);

  *contLoop = true;
  return TSDB_CODE_SUCCESS;
}


static int32_t mLeftJoinHashSeqCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableInfo* probe = pCtx->pJoin->probe;
  SMJoinTableInfo* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, 0);
  bool contLoop = false;

  if (build->grpRowIdx >= 0) {
    MJ_ERR_RET(mLeftJoinHashGrpCartFilter(pCtx, &contLoop));
    if (!contLoop) {
      goto _return;
    }
  }

  size_t bufLen = 0;
  int32_t probeEndIdx = probeGrp->endIdx;
  for (; !GRP_DONE(probeGrp) && !BLK_IS_FULL(pCtx->finBlk);) {
    if (mJoinCopyKeyColsDataToBuf(probe, probeGrp->readIdx, &bufLen)) {
      continue;
    }

    SArray** pGrp = tSimpleHashGet(build->pGrpHash, probe->keyData, bufLen);
    if (NULL == pGrp) {
      probeGrp->endIdx = probeGrp->readIdx;
      MJ_ERR_RET(mLeftJoinGrpNonEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp));
      probeGrp->endIdx = probeEndIdx;
      probeGrp->readIdx++;
      probeGrp->readMatch = false;
    } else {
      build->pHashCurGrp = *pGrp;
      build->grpRowIdx = 0;
      
      MJ_ERR_RET(mLeftJoinHashGrpCartFilter(pCtx, &contLoop));
      if (!contLoop) {
        break;
      }
    }
  }

_return:

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}


static int32_t mLeftJoinHashCart(SMJoinMergeCtx* pCtx) {
  return (NULL == pCtx->pJoin->pPreFilter) ? mLeftJoinHashFullCart(pCtx) : mLeftJoinHashSeqCart(pCtx);
}

static int32_t mLeftJoinProcessEqualGrp(SMJoinMergeCtx* pCtx, int64_t timestamp, bool lastBuildGrp) {
  SMJoinOperatorInfo* pJoin = pCtx->pJoin;

  pCtx->lastEqGrp = true;

  mJoinBuildEqGroups(pJoin->probe, timestamp, NULL, true);
  if (!lastBuildGrp) {
    mJoinRetrieveEqGrpRows(pJoin->pOperator, pJoin->build, timestamp);
  } else {
    pJoin->build->grpIdx = 0;
  }
  
  if (pCtx->hashCan && REACH_HJOIN_THRESHOLD(pJoin->probe, pJoin->build)) {
    if (!lastBuildGrp || NULL == pJoin->build->pGrpHash) {
      MJ_ERR_RET(mJoinMakeBuildTbHash(pJoin, pJoin->build));
      MJ_ERR_RET(mJoinSetKeyColsData(pJoin->probe->blk, pJoin->probe));
    }

    pCtx->hashJoin = true;    

    return mLeftJoinHashCart(pJoin, pCtx);
  }
  
  return mLeftJoinMergeCart(pJoin, pCtx);
}

static bool mLeftJoinHandleMidRemains(SMJoinMergeCtx* pCtx) {
  ASSERT(0 < pCtx->midBlk.info.rows);

  TSWAP(pCtx->midBlk, pCtx->finBlk);

  return (pCtx->finBlk->info.rows >= pCtx->blkThreshold) ? false : true;
}


static int32_t mLeftJoinHandleGrpRemains(SMJoinMergeCtx* pCtx) {
  if (pCtx->lastEqGrp) {
    return (pCtx->hashJoin) ? mLeftJoinHashCart(pCtx) : mLeftJoinMergeCart(pCtx);
  }
  
  return mLeftJoinNonEqCart(pCtx);
}

static SSDataBlock* mLeftJoinDo(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t probeTs = 0;
  int64_t buildTs = 0;
  SColumnInfoData* pBuildCol = NULL;
  SColumnInfoData* pProbeCol = NULL;
  bool asc = (pJoin->inputTsOrder == TSDB_ORDER_ASC) ? true : false;

  blockDataReset(pCtx->finBlk);

  if (pCtx->midRemains) {
    MJ_ERR_JRET(mLeftJoinHandleMidRemains(pCtx));
    if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
      return pCtx->finBlk;
    }
  }

  if (pCtx->grpRemains) {
    MJ_ERR_JRET(mLeftJoinHandleGrpRemains(pCtx));
    if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
      return pCtx->finBlk;
    }
  }

  do {
    if (!mLeftJoinRetrieve(pOperator, pJoin, pCtx)) {
      break;
    }

    MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
    MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
    
    if (probeTs == pCtx->lastEqTs) {
      MJ_ERR_JRET(mLeftJoinProcessEqualGrp(pCtx, probeTs, true));
      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
      }

      if (MJOIN_TB_ROWS_DONE(pJoin->probe)) {
        continue;
      } else {
        MJOIN_GET_TB_CUR_TS(pProbeCol, probeTs, pJoin->probe);
      }
    }

    while (!MJOIN_TB_ROWS_DONE(pJoin->probe) && !MJOIN_TB_ROWS_DONE(pJoin->build)) {
      if (probeTs == buildTs) {
        pCtx->lastEqTs = probeTs;
        MJ_ERR_JRET(mLeftJoinProcessEqualGrp(pCtx, probeTs, false));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }

        MJOIN_GET_TB_CUR_TS(pBuildCol, buildTs, pJoin->build);
        MJOIN_GET_TB_CUR_TS(pProbeCol, probeTs, pJoin->probe);
      } else if (LEFT_JOIN_NO_EQUAL(asc, probeTs, buildTs)) {
        pCtx->probeNEqGrp.beginIdx = pJoin->probe->blkRowIdx;
        pCtx->probeNEqGrp.readIdx = pCtx->probeNEqGrp.beginIdx;
        pCtx->probeNEqGrp.endIdx = pCtx->probeNEqGrp.beginIdx;

        while (++pJoin->probe->blkRowIdx < pJoin->probe->blk->info.rows) {
          MJOIN_GET_TB_CUR_TS(pProbeCol, probeTs, pJoin->probe);
          if (LEFT_JOIN_NO_EQUAL(asc, probeTs, buildTs)) {
            pCtx->probeNEqGrp.endIdx = pJoin->probe->blkRowIdx;
            continue;
          }
          
          break;
        }
        
        MJ_ERR_JRET(mLeftJoinNonEqCart(pCtx));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }
      } else {
        while (++pJoin->build->blkRowIdx < pJoin->build->blk->info.rows) {
          MJOIN_GET_TB_CUR_TS(pBuildCol, buildTs, pJoin->build);
          if (LEFT_JOIN_DISCRAD(asc, probeTs, buildTs)) {
            continue;
          }
          
          break;
        }
      }
    }
  } while (true);

_return:

  if (code) {
    pJoin->errCode = code;
    return NULL;
  }

  return pCtx->finBlk;
}



SSDataBlock* mJoinMainProcess(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    if (NULL == pOperator->pDownstreamGetParams || NULL == pOperator->pDownstreamGetParams[0] || NULL == pOperator->pDownstreamGetParams[1]) {
      qDebug("%s total merge join res rows:%" PRId64, GET_TASKID(pOperator->pTaskInfo), pJoin->resRows);
      return NULL;
    } else {
      resetMergeJoinOperator(pOperator);
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


SOperatorInfo* createMergeJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                           SSortMergeJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo) {
  SMJoinOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SMJoinOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  bool newDownstreams = false;
  
  int32_t code = TSDB_CODE_SUCCESS;
  if (pOperator == NULL || pInfo == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->pOperator = pOperator;
  code = mJoinInitDownstreamInfo(pInfo, pDownstream, numOfDownstream, newDownstreams);
  if (TSDB_CODE_SUCCESS != code) {
    goto _error;
  }

  setOperatorInfo(pOperator, "MergeJoinOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN, false, OP_NOT_OPENED, pInfo, pTaskInfo);

  mJoinSetBuildAndProbeTable(pInfo, pJoinNode);

  mJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 0, &pJoinNode->inputStat[0]);
  mJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 1, &pJoinNode->inputStat[1]);

  
  MJ_ERR_JRET(mJoinInitCtx(pInfo, pJoinNode));
  
  code = mJoinBuildResColMap(pInfo, pJoinNode);
  if (code) {
    goto _error;
  }

  code = initHJoinBufPages(pInfo);
  if (code) {
    goto _error;
  }

  if (pJoinNode->pFullOnCond != NULL) {
    code = filterInitFromNode(pJoinNode->pFullOnCond, &pInfo->pFPreFilter, 0);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  if (pJoinNode->pColOnCond != NULL) {
    code = filterInitFromNode(pJoinNode->pColOnCond, &pInfo->pPreFilter, 0);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  if (pJoinNode->node.pConditions != NULL) {
    code = filterInitFromNode(pJoinNode->node.pConditions, &pInfo->pFinFilter, 0);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }
  }

  if (pJoinNode->node.inputTsOrder == ORDER_ASC) {
    pInfo->inputTsOrder = TSDB_ORDER_ASC;
  } else if (pJoinNode->node.inputTsOrder == ORDER_DESC) {
    pInfo->inputTsOrder = TSDB_ORDER_DESC;
  } else {
    pInfo->inputTsOrder = TSDB_ORDER_ASC;
  }

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, mJoinMainProcess, NULL, destroyMergeJoinOperator, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, pDownstream, numOfDownstream);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  if (newDownstreams) {
    taosMemoryFree(pDownstream);
    pOperator->numOfRealDownstream = 1;
  } else {
    pOperator->numOfRealDownstream = 2;
  }
  
  return pOperator;

_error:
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

void destroyMergeJoinOperator(void* param) {
  SMJoinOperatorInfo* pJoinOperator = (SMJoinOperatorInfo*)param;
  if (pJoinOperator->pColEqualOnConditions != NULL) {
    mergeJoinDestoryBuildTable(pJoinOperator->rightBuildTable);
    taosMemoryFreeClear(pJoinOperator->rightEqOnCondKeyBuf);
    taosArrayDestroy(pJoinOperator->rightEqOnCondCols);

    taosMemoryFreeClear(pJoinOperator->leftEqOnCondKeyBuf);
    taosArrayDestroy(pJoinOperator->leftEqOnCondCols);
  }
  nodesDestroyNode(pJoinOperator->pCondAfterMerge);

  taosArrayDestroy(pJoinOperator->rowCtx.leftCreatedBlocks);
  taosArrayDestroy(pJoinOperator->rowCtx.rightCreatedBlocks);
  taosArrayDestroy(pJoinOperator->rowCtx.leftRowLocations);
  taosArrayDestroy(pJoinOperator->rowCtx.rightRowLocations);

  pJoinOperator->pRes = blockDataDestroy(pJoinOperator->pRes);
  taosMemoryFreeClear(param);
}

