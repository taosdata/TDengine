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
  int32_t code = mJoinInitPrimKeyInfo(pTable, (0 == idx) ? pJoinNode->LeftPrimSlotId : pJoinNode->rightPrimSlotId);
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
  
  pInfo->pBuild = &pInfo->tbs[buildIdx];
  pInfo->pProbe = &pInfo->tbs[probeIdx];
  
  pInfo->pBuild->downStreamIdx = buildIdx;
  pInfo->pProbe->downStreamIdx = probeIdx;
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
    if (pCol->dataBlockId == pInfo->pBuild->blkId) {
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

static int32_t mJoinDoRetrieve(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pTbCtx) {
  bool retrieveCont = false;
  int32_t code = TSDB_CODE_SUCCESS;
  
  do {
    SSDataBlock* pBlock = getNextBlockFromDownstreamRemain(pJoin->pOperator, pTbCtx->pTbInfo->downStreamIdx);
    pTbCtx->dsInitDone = true;
    
    if (NULL == pBlock) {
      retrieveCont = false;
      code = (*pJoin->joinFps.handleTbFetchDoneFp)(pJoin, pTbCtx);
    } else {
      code = (*pTbCtx->blkFetchedFp)(pJoin, pTbCtx, pBlock, &retrieveCont);
    }
  } while (retrieveCont || TSDB_CODE_SUCCESS != code);

  return code;
}

static int32_t mJoinInitMergeCtx(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  SMJoinTableCtx* pProbeCtx = &pCtx->probeTbCtx;
  SMJoinTableCtx* pBuildCtx = &pCtx->buildTbCtx;

  pProbeCtx->type = E_JOIN_TB_PROBE;
  pBuildCtx->type = E_JOIN_TB_BUILD;

  pCtx->tsJoinCtx.pProbeCtx = pProbeCtx;
  pCtx->tsJoinCtx.pBuildCtx = pBuildCtx;

  pCtx->joinPhase = E_JOIN_PHASE_RETRIEVE;
  
  pCtx->outputCtx.hashCan = pProbeCtx->pTbInfo->eqNum > 0;
  pCtx->outputCtx.pGrpResList = taosArrayInit(MJOIN_DEFAULT_BLK_ROWS_NUM, sizeof(SGrpPairRes));
  if (NULL == pCtx->outputCtx.pGrpResList) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (pJoin->pFPreFilter) {
    pCtx->outputCtx.cartCtx.pResBlk = createOneDataBlock(pJoin->pRes);
    blockDataEnsureCapacity(pCtx->outputCtx.cartCtx.pResBlk, MJOIN_DEFAULT_BUFF_BLK_ROWS_NUM);
    pCtx->outputCtx.cartCtx.resThreshold = MJOIN_DEFAULT_BUFF_BLK_ROWS_NUM * 0.75;
  } else {
    pCtx->outputCtx.cartCtx.pResBlk = pJoin->pRes;
    pCtx->outputCtx.cartCtx.resThreshold = pOperator->resultInfo.threshold;
  }

  if (!pCtx->outputCtx.hashCan && NULL == pJoin->pFPreFilter) {
    pCtx->outputCtx.cartCtx.appendRes = true;
  }
  
  pCtx->outputCtx.cartCtx.firstColNum = pProbeCtx->pTbInfo->finNum;
  pCtx->outputCtx.cartCtx.pFirstCols = pProbeCtx->pTbInfo->finCols;
  pCtx->outputCtx.cartCtx.secondColNum = pBuildCtx->pTbInfo->finNum;
  pCtx->outputCtx.cartCtx.pSecondCols = pBuildCtx->pTbInfo->finCols;

  pCtx->outputCtx.cartCtx.pCartRowIdx = taosArrayInit(MJOIN_DEFAULT_BUFF_BLK_ROWS_NUM, sizeof(int32_t));
  if (NULL == pCtx->outputCtx.cartCtx.pCartRowIdx) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCtx->outputCtx.cartCtx.pCartGrps = taosArrayInit(MJOIN_DEFAULT_BUFF_BLK_ROWS_NUM, sizeof(SMJoinCartGrp));
  if (NULL == pCtx->outputCtx.cartCtx.pCartGrps) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

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

static int32_t mLeftJoinProbeFetchDone(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pProbeCtx) {
  pProbeCtx->dsFetchDone = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t mLeftJoinBuildFetchDone(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pBuildCtx) {
  pBuildCtx->dsFetchDone = true;
  return TSDB_CODE_SUCCESS;
}


static int32_t mLeftJoinProbeBlkFetched(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pProbeCtx, SSDataBlock* pBlock, bool* retrieveCont) {
  int32_t code = mJoinAddBlkToList(pJoin, pProbeCtx, pBlock);
  if (code) {
    return code;
  }
  
  *retrieveCont = false;
  return TSDB_CODE_SUCCESS;
}

static int32_t mLeftJoinBuildBlkFetched(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pBuildCtx, SSDataBlock* pBlock, bool* retrieveCont) {
  SMJoinTableCtx* pProbeCtx = &pJoin->ctx.mergeCtx.probeTbCtx;
  if (pProbeCtx->blkNum <= 0) {
    *retrieveCont = false;
    return TSDB_CODE_SUCCESS;
  }

  SColumnInfoData* tsCol = taosArrayGet(pBlock, pBuildCtx->pTbInfo->primCol->srcSlot);
  int64_t lastTs = *((int64_t*)tsCol->pData + pBlock->info.rows - 1);
  if (pProbeCtx->blkCurTs > lastTs) {
    *retrieveCont = true;
  } else {
    int32_t code = mJoinAddBlkToList(pJoin, pBuildCtx, pBlock);
    if (code) {
      return code;
    }

    if (pProbeCtx->blkCurTs == lastTs && lastTs == *(int64_t*)tsCol->pData) {
      *retrieveCont = true;
    } else {
      *retrieveCont = false;
    }
  }
  
  return TSDB_CODE_SUCCESS;
}

static bool mLeftJoinRetrieve(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  SMJoinTableCtx* pProbeCtx = &pCtx->probeTbCtx;
  SMJoinTableCtx* pBuildCtx = &pCtx->buildTbCtx;

  if ((!pProbeCtx->dsFetchDone) && MJOIN_TB_LOW_BLK(pProbeCtx)) {
    int32_t code = mJoinDoRetrieve(pOperator, pProbeCtx, pJoin->pProbe);
    if (TSDB_CODE_SUCCESS != code) {
      pOperator->pTaskInfo->code = code;
      T_LONG_JMP(pOperator->pTaskInfo->env, code);
    }
  }

  if ((pProbeCtx->blkNum > 0 || MJOIN_DS_NEED_INIT(pOperator, pBuildCtx)) && (!pBuildCtx->dsFetchDone) && MJOIN_TB_LOW_BLK(pBuildCtx)) {  
    int32_t code = mJoinDoRetrieve(pJoin, pBuildCtx, pJoin->pBuild);
    if (TSDB_CODE_SUCCESS != code) {
      pOperator->pTaskInfo->code = code;
      T_LONG_JMP(pOperator->pTaskInfo->env, code);
    }
  }

  if (pProbeCtx->pHeadBlk) {
    pJoin->ctx.mergeCtx.joinPhase = E_JOIN_PHASE_SPLIT;
    return true;
  }
  
  mJoinSetDone(pOperator);
  return false;
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

static bool mJoinProbeMoveToNextBlk(SMJoinTsJoinCtx* pCtx, SMJoinTableCtx* pProbeCtx) {
  if (NULL == pProbeCtx->pCurrBlk->pNext) {
    pProbeCtx->blkIdx++;
    return false;
  }
  
  pProbeCtx->pCurrBlk = pProbeCtx->pCurrBlk->pNext;
  pProbeCtx->blkIdx++;
  pCtx->probeRowNum = pProbeCtx->pCurrBlk->pBlk->info.rows;
  SColumnInfoData* probeCol = taosArrayGet(pProbeCtx->pCurrBlk->pBlk, pProbeCtx->pTbInfo->primCol->srcSlot);
  pCtx->probeTs = (int64_t*)probeCol->pData;
  pCtx->probeEndTs = (int64_t*)probeCol->pData + pCtx->probeRowNum - 1;
  pProbeCtx->blkRowIdx = 0;

  return true;
}

static void mJoinLeftJoinAddBlkToGrp(SMJoinOperatorInfo* pJoin, SMJoinTsJoinCtx* pCtx, SMJoinTableCtx* pProbeCtx, SMJoinTableCtx* pBuildCtx) {
  FIN_SAME_TS_GRP();

  int32_t rowNum = pProbeCtx->pCurrBlk->pBlk->info.rows - pProbeCtx->blkRowIdx;
  
  if (pCtx->nextProbeRow && pCtx->inDiffTsGrp) {
    pCtx->currGrpPair->probeIn.grpRowNum += rowNum;
  } else {
    pCtx->inDiffTsGrp = true;
    START_NEW_GRP(pCtx);
    pCtx->currGrpPair.probeIn.grpBeginBlk = pProbeCtx->pCurrBlk;
    pCtx->currGrpPair.probeIn.grpRowBeginIdx = pProbeCtx->blkRowIdx;
    pCtx->currGrpPair->probeIn.grpRowNum = rowNum;
  }
  
  pCtx->nextProbeRow = true;
}

static bool mJoinBuildMoveToNextBlk(SMJoinOperatorInfo* pJoin, SMJoinTsJoinCtx* pCtx, SMJoinTableCtx* pBuildCtx, SMJoinTableCtx* pProbeCtx) {
  bool contLoop = false;
  bool res = true;

  pCtx->nextProbeRow = false;
  
  do {
    if (NULL == pBuildCtx->pCurrBlk->pNext) {
      pBuildCtx->blkIdx++;
      return false;
    }
    
    pBuildCtx->pCurrBlk = pBuildCtx->pCurrBlk->pNext;
    pBuildCtx->blkIdx++;
    pCtx->buildRowNum = pBuildCtx->pCurrBlk->pBlk->info.rows;
    SColumnInfoData* buildCol = taosArrayGet(pBuildCtx->pCurrBlk->pBlk, pBuildCtx->pTbInfo->primCol->srcSlot);
    pCtx->buildTs = (int64_t*)buildCol->pData;
    pCtx->buildEndTs = (int64_t*)buildCol->pData + pCtx->buildRowNum - 1;
    pBuildCtx->blkRowIdx = 0;

    do {
      if (*pCtx->buildTs > pCtx->probeTs[pProbeCtx->blkRowIdx]) {
        mJoinLeftJoinAddBlkToGrp(pJoin, pCtx, pProbeCtx, pBuildCtx);
        contLoop = mJoinProbeMoveToNextBlk(pCtx, pProbeCtx);
      } else if (pCtx->probeTs[pProbeCtx->blkRowIdx] > *pCtx->buildEndTs) {
        contLoop = true;
        break;
      } else {
        contLoop = false;
        res = true;
      }
    } while (contLoop);
  } while (contLoop);

  return res;
}

static bool mLeftJoinSplitGrpImpl(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  SMJoinTsJoinCtx* pCtx = &pJoin->ctx.mergeCtx.tsJoinCtx;
  SMJoinOutputCtx* pOutCtx = &pJoin->ctx.mergeCtx.outputCtx;
  SMJoinTableCtx* pProbeCtx = &pJoin->ctx.mergeCtx.probeTbCtx;
  SMJoinTableCtx* pBuildCtx = &pJoin->ctx.mergeCtx.buildTbCtx;

  mLefeJoinUpdateTsJoinCtx(pCtx, pProbeCtx, pBuildCtx);
    
  for (; pProbeCtx->blkIdx < pProbeCtx->blkNum; mJoinProbeMoveToNextBlk(pCtx, pProbeCtx)) {
    if (*pCtx->buildTs > *pCtx->probeEndTs) {
      mJoinLeftJoinAddBlkToGrp(pJoin, pCtx, pProbeCtx, pBuildCtx);
      continue;
    } else if (*pCtx->probeTs > *pCtx->buildEndTs) {
      if (!mJoinBuildMoveToNextBlk(pJoin, pCtx, pBuildCtx, pProbeCtx)) {
        break;
      //retrieve build
      }
    }

    for (; pProbeCtx->blkRowIdx < pCtx->probeRowNum; ++pProbeCtx->blkRowIdx) {
      if (pCtx->pLastGrpPair && pCtx->pLastGrpPair->sameTsGrp 
           && pCtx->probeTs[pProbeCtx->blkRowIdx] == pCtx->pLastGrpPair->probeIn.grpLastTs) {
        pCtx->pLastGrpPair->probeIn.grpRowNum++;
        SET_SAME_TS_GRP_HJOIN(pCtx->pLastGrpPair, pOutCtx);
        continue;
      }
      for (; pBuildCtx->blkIdx < pBuildCtx->blkNum; mJoinBuildMoveToNextBlk(pJoin, pCtx, pBuildCtx, pProbeCtx)) {
        for (; pBuildCtx->blkRowIdx < pCtx->buildRowNum; ++pBuildCtx->blkRowIdx) {
          if (pCtx->probeTs[pProbeCtx->blkRowIdx] > pCtx->buildTs[pBuildCtx->blkRowIdx]) {
            FIN_SAME_TS_GRP(pCtx, pOutCtx, true);
            FIN_DIFF_TS_GRP(pCtx, pOutCtx, false);
            pCtx->nextProbeRow = false;
            continue;
          } else if (pCtx->probeTs[pProbeCtx->blkRowIdx] == pCtx->buildTs[pBuildCtx->blkRowIdx]) {
            FIN_DIFF_TS_GRP(pCtx, pOutCtx, false);
            if (pCtx->inSameTsGrp) {
              pCtx->currGrpPair.buildIn.grpRowNum++;
            } else {
              pCtx->inSameTsGrp = true;              
              START_NEW_GRP(pCtx);
              pCtx->currGrpPair.buildIn.grpBeginBlk = pBuildCtx->pCurrBlk;
              pCtx->currGrpPair.buildIn.grpRowBeginIdx = pBuildCtx->blkRowIdx;
              pCtx->currGrpPair.buildIn.grpRowNum = 1;
              pCtx->currGrpPair.probeIn.grpBeginBlk = pProbeCtx->pCurrBlk;
              pCtx->currGrpPair.probeIn.grpLastTs = pCtx->probeTs[pProbeCtx->blkRowIdx];
              pCtx->currGrpPair.probeIn.grpRowBeginIdx = pProbeCtx->blkRowIdx;
              pCtx->currGrpPair.probeIn.grpRowNum = 1;
            }
            pCtx->nextProbeRow = false;
          } else {
            FIN_SAME_TS_GRP(pCtx, pOutCtx, true);
            if (pCtx->inDiffTsGrp) {
              pCtx->currGrpPair.probeIn.grpRowNum++;
            } else {
              pCtx->inDiffTsGrp = true;
              START_NEW_GRP(pCtx);
              pCtx->currGrpPair.probeIn.grpBeginBlk = pProbeCtx->pCurrBlk;
              pCtx->currGrpPair.probeIn.grpRowBeginIdx = pProbeCtx->blkRowIdx;
              pCtx->currGrpPair.probeIn.grpRowNum = 1;
            }
            pCtx->nextProbeRow = true;
            break;
          }
        }

        // end of single build table
        if (pCtx->nextProbeRow) {
          break;
        }
      }

      // end of all build tables
      if (pCtx->nextProbeRow) {
        continue;
      }
      
      if (pCtx->inSameTsGrp) {
        FIN_SAME_TS_GRP(pCtx, pOutCtx, pBuildCtx->dsFetchDone);
      }
      
      break;
    }

    // end of single probe table
    if (pCtx->nextProbeRow) {
      continue;
    }

    break;
  }

  // end of all probe tables
  FIN_DIFF_TS_GRP(pCtx, pOutCtx, pBuildCtx->dsFetchDone);

  return true;
}

static FORCE_INLINE void mJoinResetTsJoinCtx(SMJoinTsJoinCtx* pCtx) {
  pCtx->inSameTsGrp = false;
  pCtx->inDiffTsGrp = false;
  pCtx->nextProbeRow = false;
  pCtx->pLastGrpPair = NULL;
}

static bool mLeftJoinSplitGrp(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  SMJoinTsJoinCtx* pCtx = &pJoin->ctx.mergeCtx.tsJoinCtx;
  SMJoinTableCtx* pBuildCtx = &pJoin->ctx.mergeCtx.buildTbCtx;

  mJoinResetTsJoinCtx(pCtx);
  
  if (0 == pJoin->ctx.mergeCtx.buildTbCtx.blkNum) {
    ASSERTS(pJoin->ctx.mergeCtx.buildTbCtx.dsFetchDone, "left join empty build table while fetch not done");
    
    FIN_DIFF_TS_GRP(pCtx, &pJoin->ctx.mergeCtx.outputCtx, pJoin->ctx.mergeCtx.buildTbCtx.dsFetchDone);
  } else {
    mLeftJoinSplitGrpImpl(pOperator, pJoin);
  }

  pJoin->ctx.mergeCtx.joinPhase = E_JOIN_PHASE_OUTPUT;

  return true;
}

static void mLeftJoinCart(SMJoinCartCtx* pCtx) {
  int32_t currRows = pCtx->appendRes ? pCtx->pResBlk->info.rows : 0;
  
  for (int32_t c = 0; c < pCtx->firstColNum; ++c) {
    SMJoinColMap* pFirstCol = pCtx->pFirstCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pCtx->pFirstBlk->pDataBlock, pFirstCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pCtx->pResBlk->pDataBlock, pFirstCol->dstSlot);
    for (int32_t r = 0; r < pCtx->firstRowNum; ++r) {
      if (colDataIsNull_s(pInCol, pCtx->firstRowIdx + r)) {
        colDataSetNItemsNull(pOutCol, currRows + r * pCtx->secondRowNum, pCtx->secondRowNum);
      } else {
        colDataSetNItems(pOutCol, currRows + r * pCtx->secondRowNum, colDataGetData(pInCol, pCtx->firstRowIdx + r), pCtx->secondRowNum, true);
      }
    }
  }

  for (int32_t c = 0; c < pCtx->secondColNum; ++c) {
    SMJoinColMap* pSecondCol = pCtx->pSecondCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pCtx->pSecondBlk->pDataBlock, pSecondCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pCtx->pResBlk->pDataBlock, pSecondCol->dstSlot);
    for (int32_t r = 0; r < pCtx->firstRowNum; ++r) {
      colDataAssignNRows(pOutCol, currRows + r * pCtx->secondRowNum, pInCol, pCtx->secondRowIdx, pCtx->secondRowNum);
    }
  }
}

static bool mLeftJoinSameTsOutput(SMJoinOperatorInfo* pJoin, SGrpPairRes* pPair) {
  SProbeGrpResIn* pProbeIn = &pPair->probeIn;
  SBuildGrpResIn* pBuildIn = &pPair->buildIn;
  
  if (!pPair->outBegin) {

  }

  SMJoinBlkInfo* pBInfo = pProbeIn->grpBeginBlk;
  do {
    if (pJoin->prevFilter) {

    } else {
      if (pPair->buildOut.hashJoin) {

      } else {
        for (; pProbeIn->grpRowBeginIdx < pBInfo->pBlk->info.rows && pProbeIn->grpRowNum > 0; pProbeIn->grpRowBeginIdx++, pProbeIn->grpRowNum--) {

        }
      }
    }
  } while (true);
}

static bool mLeftJoinCartOutput(SMJoinOperatorInfo* pJoin, SMJoinOutputCtx* pCtx) {
  bool contLoop = false;
  SMJoinCartCtx* pCart = &pCtx->cartCtx;
  int32_t grpNum = taosArrayGetSize(pCtx->pGrpResList);
  int32_t rowsLeft = pCart->pResBlk pCart->pResBlk->info.rows;

  for (; pCtx->grpReadIdx < grpNum; pCtx->grpReadIdx++) {
    SGrpPairRes* pPair = taosArrayGet(pCtx->pGrpResList, pCtx->grpReadIdx);
    if (!pPair->finishGrp) {
      ASSERTS(pCtx->grpReadIdx == grpNum - 1, "unfinished grp not the last");
      taosArrayRemoveBatch(pCtx->pGrpResList, 0, pCtx->grpReadIdx - 1, NULL);
      pCtx->grpReadIdx = 0;
      break;
    }

    if (pPair->hashJoin) {
      contLoop = mLeftJoinHashOutput(pJoin, pPair);
    } else if (pCtx->cartCtx.appendRes) {
      contLoop = mLeftJoinDirectOutput(pJoin, pPair);
    }

    if (!contLoop) {
      return false;
    }
  }
  
}

static bool mLeftJoinOutput(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  SMJoinOutputCtx* pCtx = &pJoin->ctx.mergeCtx.outputCtx;
  bool contLoop = false;
  int32_t grpNum = taosArrayGetSize(pCtx->pGrpResList);

  if (pCtx->cartCtx.appendRes) {
    return mLeftJoinCartOutput(pJoin, pCtx);
  }
  
  for (; pCtx->grpReadIdx < grpNum; pCtx->grpReadIdx++) {
    SGrpPairRes* pPair = taosArrayGet(pCtx->pGrpResList, pCtx->grpReadIdx);
    if (!pPair->finishGrp) {
      ASSERTS(pCtx->grpReadIdx == grpNum - 1, "unfinished grp not the last");
      taosArrayRemoveBatch(pCtx->pGrpResList, 0, pCtx->grpReadIdx - 1, NULL);
      pCtx->grpReadIdx = 0;
      break;
    }

    if (pPair->hashJoin) {
      contLoop = mLeftJoinHashOutput(pJoin, pPair);
    } else if (pCtx->cartCtx.appendRes) {
      contLoop = mLeftJoinDirectOutput(pJoin, pPair);
    }

    if (!contLoop) {
      return false;
    }
  }

  pJoin->ctx.mergeCtx.joinPhase = E_JOIN_PHASE_RETRIEVE;

  return true;
}

static SSDataBlock* mJoinDoLeftJoin(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  bool contLoop = false;
  
  do {
    switch (pJoin->ctx.mergeCtx.joinPhase) {
      case E_JOIN_PHASE_RETRIEVE:
        contLoop = mLeftJoinRetrieve(pOperator, pJoin);
        break;
      case E_JOIN_PHASE_SPLIT:
        contLoop = mLeftJoinSplitGrp(pOperator, pJoin);
        break;
      case E_JOIN_PHASE_OUTPUT:
        contLoop = mLeftJoinOutput(pOperator, pJoin);
        break;
      case E_JOIN_PHASE_DONE:
        contLoop = false;
        break;
    }
  } while (contLoop);

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
  pInfo->pRes = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);
  initResultSizeInfo(&pOperator->resultInfo, MJOIN_DEFAULT_BLK_ROWS_NUM);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

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

