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


static FORCE_INLINE int32_t mJoinAddPageToBufList(SArray* pRowBufs) {
  SBufPageInfo page;
  page.pageSize = HASH_JOIN_DEFAULT_PAGE_SIZE;
  page.offset = 0;
  page.data = taosMemoryMalloc(page.pageSize);
  if (NULL == page.data) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  taosArrayPush(pRowBufs, &page);
  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinInitBufPages(SMJoinOperatorInfo* pInfo) {
  pInfo->pRowBufs = taosArrayInit(32, sizeof(SBufPageInfo));
  if (NULL == pInfo->pRowBufs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return mJoinAddPageToBufList(pInfo->pRowBufs);
}

static int32_t mJoinInitMergeCtx(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;

  pCtx->lastEqTs = INT64_MIN;
  pCtx->hashCan = pJoin->probe->keyNum > 0;

  pCtx->probeEqGrps = taosArrayInit(8, sizeof(SMJoinGrpRows));
  pCtx->probeCreatedBlks = taosArrayInit(8, POINTER_BYTES);
  
  pCtx->buildEqGrps = taosArrayInit(8, sizeof(SMJoinGrpRows));
  pCtx->buildCreatedBlks = taosArrayInit(8, POINTER_BYTES);

  if (pJoin->pFPreFilter) {
    pCtx->midBlk = createOneDataBlock(pJoin->pResBlk, false);
    blockDataEnsureCapacity(pCtx->midBlk, pJoin->pResBlk->info.rows);
  }

  pCtx->finBlk = pJoin->pResBlk;

  pCtx->blksCapacity = pJoin->pResBlk->info.rows * 2;
  
  pCtx->resBlk = NULL;

  return TSDB_CODE_SUCCESS;
}

static int32_t mJoinInitCtx(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  pJoin->joinFps = &gMJoinFps[pJoin->joinType][pJoin->subType];

  int32_t code = (*pJoin->joinFps->initJoinCtx)(pOperator, pJoin);
  if (code) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
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

static int32_t mJoinAddBlkToList(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pCtx, SSDataBlock* pBlock) {
  SMJoinBlkInfo* pNew = taosMemoryCalloc(1, sizeof(SMJoinBlkInfo));
  if (NULL == pNew) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  pNew->pBlk = pBlock;
  pNew->inUse = true;
  
  if (NULL == pCtx->pTailBlk) {
    pCtx->pTailBlk = pCtx->pHeadBlk = pNew;
    pCtx->pCurrBlk = pCtx->pHeadBlk;
    pCtx->blkIdx = 0;
    pCtx->blkRowIdx = 0;
    pCtx->blkNum = 1;
    if (E_JOIN_TB_PROBE == pCtx->type) {
      SColumnInfoData* probeCol = taosArrayGet(pCtx->pCurrBlk->pBlk, pCtx->pTbInfo->primCol->srcSlot);
      pCtx->blkCurTs = *(int64_t*)probeCol->pData;
    }
  } else {
    pCtx->pTailBlk->pNext = pNew;
    pCtx->blkNum++;
    if (E_JOIN_TB_PROBE == pCtx->type) {
      SMJoinTsJoinCtx* pTsCtx = &pJoin->ctx.mergeCtx.tsJoinCtx;
      pCtx->blkCurTs = pTsCtx->probeTs[pCtx->blkRowIdx];
    }
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void mLefeJoinUpdateTsJoinCtx(SMJoinTsJoinCtx* pCtx, SMJoinTableCtx* pProbeCtx, SMJoinTableCtx* pBuildCtx) {
  pCtx->probeRowNum = pProbeCtx->pCurrBlk->pBlk->info.rows;
  pCtx->buildRowNum = pBuildCtx->pCurrBlk->pBlk->info.rows;
  SColumnInfoData* probeCol = taosArrayGet(pProbeCtx->pCurrBlk->pBlk, pProbeCtx->pTbInfo->primCol->srcSlot);
  SColumnInfoData* buildCol = taosArrayGet(pBuildCtx->pHeadBlk->pBlk, pBuildCtx->pTbInfo->primCol->srcSlot);
  pCtx->probeTs = (int64_t*)probeCol->pData; 
  pCtx->probeEndTs = (int64_t*)probeCol->pData + pCtx->probeRowNum - 1;
  pCtx->buildTs = (int64_t*)buildCol->pData; 
  pCtx->buildEndTs = (int64_t*)buildCol->pData + pCtx->buildRowNum - 1;
}

static FORCE_INLINE void mJoinResetTsJoinCtx(SMJoinTsJoinCtx* pCtx) {
  pCtx->inSameTsGrp = false;
  pCtx->inDiffTsGrp = false;
  pCtx->nextProbeRow = false;
  pCtx->pLastGrpPair = NULL;
}


static void mLeftJoinGrpNonEqCart(SMJoinOperatorInfo* pJoin, SSDataBlock* pRes, bool append, SMJoinGrpRows* pFirst) {
  SMJoinTableInfo* probe = pJoin->probe;
  SMJoinTableInfo* build = pJoin->build;
  int32_t currRows = append ? pRes->info.rows : 0;
  int32_t firstRows = GRP_REMAIN_ROWS(pFirst);
  
  for (int32_t c = 0; c < probe->finNum; ++c) {
    SMJoinColMap* pFirstCol = probe->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pFirst->blk, pFirstCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pRes->pDataBlock, pFirstCol->dstSlot);
    colDataAssignNRows(pOutCol, currRows, pInCol, pFirst->readIdx, firstRows);
  }
  
  for (int32_t c = 0; c < build->finNum; ++c) {
    SMJoinColMap* pSecondCol = build->finCols + c;
    SColumnInfoData* pOutCol = taosArrayGet(pRes->pDataBlock, pSecondCol->dstSlot);
    colDataSetNItemsNull(pOutCol, currRows, firstRows);
  }
  
  pRes->info.rows = append ? (pRes->info.rows + firstRows) : firstRows;
}

static void mLeftJoinGrpEqCart(SMJoinOperatorInfo* pJoin, SSDataBlock* pRes, bool append, SMJoinGrpRows* pFirst, SMJoinGrpRows* pSecond) {
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

  pRes->info.rows = append ? (pRes->info.rows + firstRows) : firstRows;
}


static void mLeftJoinMergeFullCart(SMJoinOperatorInfo* pJoin, SMJoinMergeCtx* pCtx) {
  int32_t rowsLeft = pCtx->finBlk->info.capacity - pCtx->finBlk->info.rows;
  SMJoinTableInfo* probe = pJoin->probe;
  SMJoinTableInfo* build = pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);
  int32_t buildGrpNum = taosArrayGetSize(build->eqGrps);
  int32_t probeRows = GRP_REMAIN_ROWS(probeGrp);

  pCtx->eqCart = true;

  if (probeRows * build->grpRemainRows <= rowsLeft) {
    for (; build->grpIdx < buildGrpNum; ++build->grpIdx) {
      SMJoinGrpRows* buildGrp = taosArrayGet(build->eqGrps, build->grpIdx);
      mLeftJoinGrpEqCart(pJoin, pCtx->finBlk, true, probeGrp, buildGrp);
    }
    probe->grpIdx++;
    build->grpRemainRows = 0;
    pCtx->grpRemains = false;
    return true;
  }

  for (; probeGrp->readIdx <= probeGrp->endIdx; ++probeGrp->readIdx) {
    for (; build->grpIdx < buildGrpNum; ++build->grpIdx) {
      SMJoinGrpRows* buildGrp = taosArrayGet(build->eqGrps, build->grpIdx);
      int32_t probeEndIdx = probeGrp->endIdx;
      probeGrp->endIdx = probeGrp->readIdx;

      if (rowsLeft >= GRP_REMAIN_ROWS(buildGrp)) {
        mLeftJoinGrpEqCart(pJoin, pCtx->finBlk, true, probeGrp, buildGrp);
        rowsLeft -= GRP_REMAIN_ROWS(buildGrp);
        probeGrp->endIdx = probeEndIdx;        
        continue;
      }
      
      int32_t buildEndIdx = buildGrp->endIdx;
      buildGrp->endIdx = buildGrp->readIdx + rowsLeft - 1;
      mLeftJoinGrpEqCart(pJoin, pCtx->finBlk, true, probeGrp, buildGrp);
      buildGrp->readIdx += rowsLeft;
      buildGrp->endIdx = buildEndIdx;
      rowsLeft = 0;
      break;
    }

    if (rowsLeft <= 0) {
      break;
    }
  }

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;
}

static void mLeftJoinCopyMergeMidBlk(SSDataBlock* pMid, SSDataBlock* pFin) {
  SSDataBlock* pLess = NULL;
  SSDataBlock* pMore = NULL;
  if (pMid->info.rows < pFin->info.rows) {
    pLess = pMid;
    pMore = pFin;
  } else {
    pLess = pFin;
    pMore = pMid;
  }

  int32_t totalRows = pMid->info.rows + pFin->info.rows;
  if (totalRows <= pMore->info.capacity) {
    blockDataMerge(pMore, pLess);
  } else {
    
  }
}

static void mLeftJoinMergeSeqCart(SMJoinOperatorInfo* pJoin, SMJoinMergeCtx* pCtx) {
  SMJoinTableInfo* probe = pJoin->probe;
  SMJoinTableInfo* build = pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);
  int32_t buildGrpNum = taosArrayGetSize(build->eqGrps);
  int32_t probeEndIdx = probeGrp->endIdx;
  int32_t rowsLeft = pCtx->midBlk->info.capacity;  
  bool contLoop = true;

  pCtx->eqCart = true;

  do {
    for (; !GRP_DONE(probeGrp->readIdx) && !BLK_IS_FULL(pCtx->finBlk); ++probeGrp->readIdx) {
      probeGrp->endIdx = probeGrp->readIdx;
      for (; build->grpIdx < buildGrpNum; ++build->grpIdx) {
        SMJoinGrpRows* buildGrp = taosArrayGet(build->eqGrps, build->grpIdx);

        if (rowsLeft >= GRP_REMAIN_ROWS(buildGrp)) {
          mLeftJoinGrpEqCart(pJoin, pCtx->midBlk, true, probeGrp, buildGrp);
          rowsLeft -= GRP_REMAIN_ROWS(buildGrp);
          continue;
        }
        
        int32_t buildEndIdx = buildGrp->endIdx;
        buildGrp->endIdx = buildGrp->readIdx + rowsLeft - 1;
        mLeftJoinGrpEqCart(pJoin, pCtx->midBlk, true, probeGrp, buildGrp);
        buildGrp->readIdx += rowsLeft;
        buildGrp->endIdx = buildEndIdx;
        rowsLeft = 0;
        break;
      }

      doFilter(pCtx->midBlk, pJoin->pFPreFilter, NULL);
      if (pCtx->midBlk->info.rows > 0) {
        probeGrp->readMatch = true;
      } else if (build->grpIdx == buildGrpNum && !probeGrp->readMatch) {
        mLeftJoinGrpNonEqCart(pJoin, pCtx->finBlk, true, probeGrp);
        continue;
      }

      if (pCtx->midBlk->info.rows >= pJoin->pOperator->resultInfo.threshold) {
        contLoop = false;
        break;
      }

      rowsLeft = pCtx->midBlk->info.capacity - pCtx->midBlk->info.rows;
      
      mLeftJoinCopyMergeMidBlk(&pCtx->midBlk, &pCtx->finBlk);
      break;
    }

    if (GRP_DONE(probeGrp->readIdx) || BLK_IS_FULL(pCtx->finBlk)) {
      break;
    }
  } while (contLoop);

  probeGrp->endIdx = probeEndIdx;        

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;
}

static void mLeftJoinMergeCart(SMJoinOperatorInfo* pJoin, SMJoinMergeCtx* pCtx) {
  if (NULL == pJoin->pFPreFilter) {
    mLeftJoinMergeFullCart(pJoin, pCtx);
  } else {
    mLeftJoinMergeSeqCart(pJoin, pCtx);
  }
}

static void mLeftJoinNonEqCart(SMJoinOperatorInfo* pJoin, SMJoinMergeCtx* pCtx) {
  int32_t rowsLeft = pCtx->finBlk->info.capacity - pCtx->finBlk->info.rows;
  SMJoinGrpRows* probeGrp = &pCtx->probeNEqGrp;
  int32_t probeRows = GRP_REMAIN_ROWS(probeGrp);

  pCtx->eqCart = false;

  if (probeRows <= rowsLeft) {
    mLeftJoinGrpNonEqCart(pJoin, pCtx->finBlk, true, probeGrp, NULL);
    probeGrp->readIdx = probeGrp->endIdx + 1;
    pCtx->grpRemains = false;
  } else {
    int32_t probeEndIdx = probeGrp->endIdx;
    probeGrp->endIdx = probeGrp->readIdx + rowsLeft - 1;
    mLeftJoinGrpNonEqCart(pJoin, pCtx->finBlk, true, probeGrp, NULL);
    probeGrp->readIdx = probeGrp->endIdx + 1;
    probeGrp->endIdx = probeEndIdx;
    pCtx->grpRemains = true;
  }
}



static bool mJoinRetrieveImpl(SMJoinOperatorInfo* pJoin, int32_t* pIdx, SSDataBlock** ppBlk, SMJoinTableInfo* ppTb) {
  if (!(*ppTb)->dsFetchDone && (NULL == (*ppBlk) || *pIdx >= (*ppBlk)->info.rows)) {
    (*ppBlk) = getNextBlockFromDownstreamRemain(pJoin->pOperator, (*ppTb)->downStreamIdx);
    (*ppTb)->dsInitDone = true;

    qDebug("merge join %s table got %" PRId64 " rows block", MJOIN_TBTYPE(ppTb->type), (*ppBlk) ? (*ppBlk)->info.rows : 0);

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
    
    if (NULL == pJoin->probe->blk) {
      mJoinSetDone(pOperator);
      return false;
    } else if (buildGot && probeGot) {
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


static void mJoinBuildEqGroups(SOperatorInfo* pOperator, SMJoinTableInfo* pTable, int64_t timestamp, bool* allBlk, bool restart) {
  SColumnInfoData* pCol = taosArrayGet(pTable->blk->pDataBlock, pTable->primCol->srcSlot);
  SMJoinGrpRows* pGrp = NULL;
  int32_t 

  if (restart) {
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

    pTable->grpRowsNum += pGrp->endIdx - pGrp->beginIdx + 1;
    pTable->grpRemainRows = pTable->grpRowsNum;
    return;
  }

  if (allBlk) {
    *allBlk = true;
    if (0 == pGrp->beginIdx) {
      pGrp->blk = createOneDataBlock(pTable->blk, true);
    } else {
      pGrp->blk = blockDataExtractBlock(pTable->blk, pGrp->beginIdx, pGrp->endIdx - pGrp->beginIdx + 1);
    }
    taosArrayPush(pTable->createdBlks, &pGrp->blk);
    pGrp->beginIdx = 0;
  }

  pTable->grpRowsNum += pGrp->endIdx - pGrp->beginIdx + 1;  
  pTable->grpRemainRows = pTable->grpRowsNum;
}


static int32_t mJoinRetrieveSameTsRows(SOperatorInfo* pOperator, SMJoinTableInfo* pTable, int64_t timestamp) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  bool allBlk = false;
  
  mJoinBuildEqGroups(pOperator, pTable, timestamp, &allBlk, true);
  
  while (allBlk) {
    pTable->blk = getNextBlockFromDownstreamRemain(pOperator, pTable->downStreamIdx);
    qDebug("merge join %s table got block for same ts, rows:%" PRId64, MJOIN_TBTYPE(pTable->type), pTable->blk ? pTable->blk->info.rows : 0);

    pTable->blkRowIdx = 0;

    if (NULL == pTable->blk) {
      pTable->dsFetchDone = true;
      break;
    }

    allBlk = false;
    mJoinBuildEqGroups(pOperator, pTable, timestamp, &allBlk, false);
  }

  return 0;
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
    int32_t code = mJoinGetAvailableGrpArray(pBuild, &pNewGrp);
    if (code) {
      return code;
    }
    taosArrayPush(pNewGrp, &pos);
    tSimpleHashPut(pBuild->pGrpHash, pBuild->keyData, keyLen, &pNewGrp, POINTER_BYTES);
  } else {
    taosArrayPush(*pGrpRows, &pos);
  }

  return TSDB_CODE_SUCCESS;
}


static void mJoinMakeBuildTbHash(SMJoinOperatorInfo* pJoin, SMJoinTableInfo* pTable) {
  int32_t grpNum = taosArrayGetSize(pTable->eqGrps);
  for (int32_t g = 0; g < grpNum; ++g) {
    SMJoinGrpRows* pGrp = taosArrayGet(pTable->eqGrps, g);
    int32_t code = mJoinSetKeyColsData(pGrp->blk, pTable);
    if (code) {
      return code;
    }

    int32_t grpRows = GRP_REMAIN_ROWS(pGrp);
    size_t bufLen = 0;
    for (int32_t r = 0; r < grpRows; ++r) {
      if (mJoinCopyKeyColsDataToBuf(pTable, r, &bufLen)) {
        continue;
      }
      code = mJoinAddRowToHash(pJoin, bufLen, pGrp->blk, pGrp->beginIdx + r);
      if (code) {
        return code;
      }
    }
  }
}

static bool mLeftJoinHashRowCart(SMJoinMergeCtx* pCtx, SMJoinGrpRows* probeGrp, SMJoinTableInfo* probe, SMJoinTableInfo* build) {
  int32_t rowsLeft = pCtx->resBlk->info.capacity - pCtx->resBlk->info.rows;
  int32_t buildGrpRows = taosArrayGetSize(build->pHashCurGrp);
  int32_t grpRows = buildGrpRows - build->grpRowIdx;
  int32_t actRows = TMIN(grpRows, rowsLeft);
  int32_t currRows = pCtx->noColCond ? pCtx->resBlk->info.rows : 0;

  for (int32_t c = 0; c < probe->finNum; ++c) {
    SMJoinColMap* pFirstCol = probe->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(probeGrp->blk, pFirstCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pCtx->resBlk->pDataBlock, pFirstCol->dstSlot);
    if (colDataIsNull_s(pInCol, probeGrp->readIdx)) {
      colDataSetNItemsNull(pOutCol, currRows, actRows);
    } else {
      colDataSetNItems(pOutCol, currRows, colDataGetData(pInCol, probeGrp->beginIdx), actRows, true);
    }
  }

  for (int32_t c = 0; c < build->finNum; ++c) {
    SMJoinColMap* pSecondCol = build->finCols + c;
    SColumnInfoData* pOutCol = taosArrayGet(pCtx->resBlk->pDataBlock, pSecondCol->dstSlot);
    for (int32_t r = 0; r < actRows; ++r) {
      SMJoinRowPos* pRow = taosArrayGet(build->pHashCurGrp, r);
      SColumnInfoData* pInCol = taosArrayGet(pRow->pBlk, pSecondCol->srcSlot);
      colDataAssignNRows(pOutCol, currRows + r, pInCol, pRow->pos, 1);
    }
  }

  pCtx->resBlk->info.rows += actRows;
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

static void mLeftJoinHashCart(SMJoinOperatorInfo* pJoin, SMJoinMergeCtx* pCtx) {
  SMJoinTableInfo* probe = pJoin->probe;
  SMJoinTableInfo* build = pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);

  if (pJoin->build->grpRowIdx >= 0) {
    bool contLoop = mLeftJoinHashRowCart(pCtx, probeGrp, probe, build);
    if (build->grpRowIdx < 0) {
      probeGrp->readIdx++;
    }
    
    if (!contLoop) {
      pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;
      return;
    }
  }

  pCtx->eqCart = true;

  size_t bufLen = 0;
  for (; probeGrp->readIdx < probeGrp->endIdx; ++probeGrp->readIdx) {
    if (mJoinCopyKeyColsDataToBuf(probe, probeGrp->readIdx, &bufLen)) {
      continue;
    }

    SArray** pGrp = tSimpleHashGet(build->pGrpHash, probe->keyData, bufLen);
    if (NULL == pGrp) {
      
    } else {
      build->pHashCurGrp = *pGrp;
      build->grpRowIdx = 0;
      if (NULL == pJoin->pPreFilter) {
        if (!mLeftJoinHashRowCart(pCtx, probeGrp, probe, build)) {
          break;
        }
      } else {

      }
    }
  }

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;
}

static int32_t mLeftJoinProcessEqualGrp(SOperatorInfo* pOperator, int64_t timestamp, SSDataBlock* pRes) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;

  mJoinBuildEqGroups(pOperator, pJoin->probe, timestamp, NULL, true);
  mJoinRetrieveSameTsRows(pOperator, pJoin->build, timestamp);
  if (pCtx->hashCan && REACH_HJOIN_THRESHOLD(pJoin->probe, pJoin->build)) {
    int32_t code = mJoinMakeBuildTbHash(pJoin, pJoin->build);
    if (code) {
      return code;
    }
    code = mJoinSetKeyColsData(pJoin->probe->blk, pJoin->probe);
    if (code) {
      return code;
    }

    pCtx->hashJoin = true;    
    mLeftJoinHashCart(pJoin, pCtx);
  } else {
    mLeftJoinMergeCart(pJoin, pCtx);
  }
  
  return TSDB_CODE_SUCCESS;
}


static bool mLeftJoinHandleRowRemains(struct SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin, SMJoinMergeCtx* pCtx, SSDataBlock* pRes) {
  if (pCtx->eqCart) {
    if (pCtx->hashJoin) {
      mLeftJoinHashCart(pJoin, pCtx);
    } else {
      mLeftJoinMergeCart(pJoin, pCtx);
    }
    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      return false;
    }

    return true;
  }
  
  mLeftJoinNonEqCart(pJoin, pCtx);
  if (pRes->info.rows >= pOperator->resultInfo.threshold) {
    return false;
  }
  
  return true;
}

static void mLeftJoinDo(struct SOperatorInfo* pOperator, SSDataBlock* pRes) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  int64_t probeTs = 0;
  int64_t buildTs = 0;
  SColumnInfoData* pBuildCol = NULL;
  SColumnInfoData* pProbeCol = NULL;
  bool asc = (pJoin->inputTsOrder == TSDB_ORDER_ASC) ? true : false;

  if (pCtx->grpRemains && !mLeftJoinHandleRowRemains(pOperator, pJoin, pCtx, pRes)) {
    return;
  }

  do {
    if (!mLeftJoinRetrieve(pOperator, pJoin, pCtx)) {
      break;
    }

    SET_TABLE_CUR_TS(pBuildCol, buildTs, pJoin->build);
    SET_TABLE_CUR_TS(pProbeCol, probeTs, pJoin->probe);
    
    if (probeTs == pCtx->lastEqTs) {
      mLeftJoinProcessEqualGrp(pOperator, probeTs, pRes);
      if (pRes->info.rows >= pOperator->resultInfo.threshold) {
        return;
      }

      if (pJoin->probe->blkRowIdx < pJoin->probe->blk->info.rows) {
        SET_TABLE_CUR_TS(pProbeCol, probeTs, pJoin->probe);
      } else {
        continue;
      }
    }

    while (pJoin->probe->blkRowIdx < pJoin->probe->blk->info.rows && pJoin->build->blkRowIdx < pJoin->build->blk->info.rows) {
      if (probeTs == buildTs) {
        pCtx->lastEqTs = probeTs;
        mLeftJoinProcessEqualGrp(pOperator, probeTs, pRes);
        if (pRes->info.rows >= pOperator->resultInfo.threshold) {
          return;
        }

        SET_TABLE_CUR_TS(pBuildCol, buildTs, pJoin->build);
        SET_TABLE_CUR_TS(pProbeCol, probeTs, pJoin->probe);
      } else if (LEFT_JOIN_NO_EQUAL(asc, probeTs, buildTs)) {
        pCtx->probeNEqGrp.beginIdx = pJoin->probe->blkRowIdx;

        do {
          pCtx->probeNEqGrp.endIdx = pJoin->probe->blkRowIdx;
          probeTs = *((int64_t*)pProbeCol->pData + (++pJoin->probe->blkRowIdx)); 
        } while (pJoin->probe->blkRowIdx < pJoin->probe->blk->info.rows && LEFT_JOIN_NO_EQUAL(asc, probeTs, buildTs));
        
        mLeftJoinNonEqCart(pJoin, pCtx);
        
        if (pRes->info.rows >= pOperator->resultInfo.threshold) {
          return;
        }
      } else {
        buildTs = *((int64_t*)pBuildCol->pData + (++pJoin->build->blkRowIdx)); 
        while (pJoin->build->blkRowIdx < pJoin->build->blk->info.rows && LEFT_JOIN_DISCRAD(asc, probeTs, buildTs)) {
          buildTs = *((int64_t*)pBuildCol->pData + (++pJoin->build->blkRowIdx)); 
        }
      }
    }
  } while (true);
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
  //blockDataCleanup(pJoin->pRes);

  while (true) {
    pBlock = (*pJoin->joinFp)(pOperator);
    if (NULL == pBlock) {
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
  
  if (pBlock->info.rows > 0) {
    pJoin->resRows += pBlock->info.rows;
    qDebug("%s merge join returns res rows:%" PRId64, GET_TASKID(pOperator->pTaskInfo), pBlock->info.rows);
    return pBlock;
  } else {
    qDebug("%s total merge join res rows:%" PRId64, GET_TASKID(pOperator->pTaskInfo), pJoin->resRows);
    return NULL;
  }
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

  int32_t      numOfCols = 0;
  pInfo->pResBlk = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);
  blockDataEnsureCapacity(pInfo->pResBlk, TMAX(MJOIN_DEFAULT_BLK_ROWS_NUM, MJOIN_BLK_SIZE_LIMIT/pJoinNode->node.pOutputDataBlockDesc.totalRowSize));

  setOperatorInfo(pOperator, "MergeJoinOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN, false, OP_NOT_OPENED, pInfo, pTaskInfo);

  mJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 0, &pJoinNode->inputStat[0]);
  mJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 1, &pJoinNode->inputStat[1]);

  mJoinSetBuildAndProbeTable(pInfo, pJoinNode);
  
  mJoinInitCtx(pOperator, pInfo);
  
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

