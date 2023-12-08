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
  
  pInfo->build = &pInfo->tbs[buildIdx];
  pInfo->probe = &pInfo->tbs[probeIdx];
  
  pInfo->build->downStreamIdx = buildIdx;
  pInfo->probe->downStreamIdx = probeIdx;
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

  pProbeCtx->type = E_JOIN_TB_PROBE;
  pBuildCtx->type = E_JOIN_TB_BUILD;

  pCtx->hashCan = pJoin->probe->eqNum > 0;

  pCtx->probeEqGrps = taosArrayInit(8, sizeof(SMJoinGrpRows));
  pCtx->probeCreatedBlks = taosArrayInit(8, POINTER_BYTES);
  
  pCtx->buildEqGrps = taosArrayInit(8, sizeof(SMJoinGrpRows));
  pCtx->buildCreatedBlks = taosArrayInit(8, POINTER_BYTES);

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


static void mLeftJoinCart(SMJoinCartCtx* pCtx) {
  int32_t currRows = pCtx->appendRes ? pCtx->pResBlk->info.rows : 0;
  
  for (int32_t c = 0; c < pCtx->firstColNum; ++c) {
    SMJoinColMap* pFirstCol = pCtx->pFirstCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pCtx->pFirstBlk->pBlk, pFirstCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pCtx->pResBlk->pDataBlock, pFirstCol->dstSlot);
    for (int32_t r = 0; r < pCtx->firstRowNum; ++r) {
      if (colDataIsNull_s(pInCol, pCtx->firstRowIdx + r)) {
        colDataSetNItemsNull(pOutCol, currRows + r * pCtx->secondRowNum, pCtx->secondRowNum);
      } else {
        colDataSetNItems(pOutCol, currRows + r * pCtx->secondRowNum, colDataGetData(pInCol, pCtx->firstRowIdx + r), pCtx->secondRowNum, true);
      }
    }
  }

  if (pCtx->firstOnly) {
    ASSERT(1 == pCtx->secondRowNum);
    for (int32_t c = 0; c < pCtx->secondColNum; ++c) {
      SMJoinColMap* pSecondCol = pCtx->pSecondCols + c;
      SColumnInfoData* pOutCol = taosArrayGet(pCtx->pResBlk->pDataBlock, pSecondCol->dstSlot);
      colDataSetNItemsNull(pOutCol, currRows, pCtx->firstRowNum);
    }
  } else {
    for (int32_t c = 0; c < pCtx->secondColNum; ++c) {
      SMJoinColMap* pSecondCol = pCtx->pSecondCols + c;
      SColumnInfoData* pInCol = taosArrayGet(pCtx->pSecondBlk->pBlk, pSecondCol->srcSlot);
      SColumnInfoData* pOutCol = taosArrayGet(pCtx->pResBlk->pDataBlock, pSecondCol->dstSlot);
      for (int32_t r = 0; r < pCtx->firstRowNum; ++r) {
        colDataAssignNRows(pOutCol, currRows + r * pCtx->secondRowNum, pInCol, pCtx->secondRowIdx, pCtx->secondRowNum);
      }
    }
  }

  pCtx->pResBlk.info.rows += pCtx->firstRowNum * pCtx->secondRowNum;
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
  bool probeGot = mJoinRetrieveImpl(pJoin, &pJoin->probe->rowIdx, &pJoin->probe->blk, &pJoin->probe);
  bool buildGot = false;

  do {
    if (probeGot || MJOIN_DS_NEED_INIT(pOperator, pJoin->build)) {  
      buildGot = mJoinRetrieveImpl(pJoin, &pJoin->build->rowIdx, &pJoin->build->blk, &pJoin->build);
    }
    
    if (NULL == pJoin->probe->blk) {
      mJoinSetDone(pOperator);
      return false;
    } else if (buildGot && probeGot) {
      SColumnInfoData* pProbeCol = taosArrayGet(pJoin->probe->blk->pDataBlock, pJoin->probe->primCol->srcSlot);
      SColumnInfoData* pBuildCol = taosArrayGet(pJoin->build->blk->pDataBlock, pJoin->build->primCol->srcSlot);
      if (*((int64_t*)pProbeCol->pData + pJoin->probe->rowIdx) > *((int64_t*)pBuildCol->pData + pJoin->build->blk->info.rows - 1)) {
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

  pGrp->beginIdx = pTable->rowIdx++;
  pGrp->rowsNum = 1;
  pGrp->blk = pTable->blk;
  
  for (; pTable->rowIdx < pTable->blk->info.rows; ++pTable->rowIdx) {
    char* pNextVal = colDataGetData(pCol, pTable->rowIdx);
    if (timestamp == *(int64_t*)pNextVal) {
      pGrp->rowsNum++;
      continue;
    }
    return;
  }

  if (allBlk) {
    *allBlk = true;
    if (0 == pGrp->beginIdx) {
      pGrp->blk = createOneDataBlock(pTable->blk, true);
    } else {
      pGrp->blk = blockDataExtractBlock(pTable->blk, pGrp->beginIdx, pGrp->rowsNum - pGrp->beginIdx);
    }
    taosArrayPush(pTable->createdBlks, &pGrp->blk);
    pGrp->beginIdx = 0;
  }
}


static int32_t mJoinRetrieveSameTsRows(SOperatorInfo* pOperator, SMJoinTableInfo* pTable, int64_t timestamp) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  int32_t            endPos = -1;
  SSDataBlock*       dataBlock = startDataBlock;
  bool allBlk = false;
  
  mJoinBuildEqGroups(pOperator, pTable, timestamp, &allBlk, true);
  
  while (allBlk) {
    pTable->blk = getNextBlockFromDownstreamRemain(pOperator, pTable->downStreamIdx);
    qDebug("merge join %s table got block for same ts, rows:%" PRId64, MJOIN_TBTYPE(pTable->type), pTable->blk ? pTable->blk->info.rows : 0);

    pTable->rowIdx = 0;

    if (NULL == pTable->blk) {
      pTable->dsFetchDone = true;
      break;
    }

    allBlk = false;
    mJoinBuildEqGroups(pOperator, pTable, timestamp, &allBlk, false);
  }

  return 0;
}

static int32_t mJoinEqualCart(SOperatorInfo* pOperator, int64_t timestamp, SSDataBlock* pRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  SSHashObj* rightTableHash = NULL;
  bool rightUseBuildTable = false;

  if (!pCtx->rowRemains) {
    mJoinBuildEqGroups(pOperator, pJoin->probe, timestamp, NULL, true);
    mJoinRetrieveSameTsRows(pOperator, pJoin->build, timestamp);
    if (pJoinInfo->pColEqualOnConditions != NULL && taosArrayGetSize(rightRowLocations) > 16) {
      mergeJoinFillBuildTable(pJoinInfo, rightRowLocations);
      pCtx->hashJoin = true;
      taosArrayDestroy(rightRowLocations);
      rightRowLocations = NULL;
    }
  }

  bool reachThreshold = false;

  if (code == TSDB_CODE_SUCCESS) {
    mLeftJoinCart(pOperator, pRes, nRows, leftRowLocations, leftRowIdx,
                                                rightRowIdx, pCtx->hashJoin, rightRowLocations, &reachThreshold);
  }

  if (!reachThreshold) {
    mergeJoinDestroyTSRangeCtx(pJoinInfo, leftRowLocations, leftCreatedBlocks, rightCreatedBlocks,
                               pCtx->hashJoin, rightRowLocations);

  } else {
      pJoinInfo->rowCtx.rowRemains = true;
      pJoinInfo->rowCtx.ts = timestamp;
      pJoinInfo->rowCtx.leftRowLocations = leftRowLocations;
      pJoinInfo->rowCtx.leftCreatedBlocks = leftCreatedBlocks;
      pJoinInfo->rowCtx.rightCreatedBlocks = rightCreatedBlocks;
      pJoinInfo->rowCtx.rightRowLocations = rightRowLocations;
  }
  return TSDB_CODE_SUCCESS;
}



static void mLeftJoinDo(struct SOperatorInfo* pOperator, SSDataBlock* pRes) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  int64_t probeTs = INT64_MIN;
  int64_t buildTs = 0;
  SColumnInfoData* pBuildCol = NULL;
  SColumnInfoData* pProbeCol = NULL;

  bool asc = (pJoin->inputOrder == TSDB_ORDER_ASC) ? true : false;

  do {
    if (pCtx->rowRemains) {
      probeTs = buildTs = pCtx->curTs;
    } else {
      if (!mLeftJoinRetrieve(pOperator, pJoin, pCtx, probeTs)) {
        break;
      }

      pBuildCol = taosArrayGet(pJoin->build->blk->pDataBlock, pJoin->build->primCol->srcSlot);
      pProbeCol = taosArrayGet(pJoin->probe->blk->pDataBlock, pJoin->probe->primCol->srcSlot);
      probeTs = *((int64_t*)pProbeCol->pData + pJoin->probe->rowIdx); 
      buildTs = *((int64_t*)pBuildCol->pData + pCtx->buildIdx); 
    }

    while (pCtx->probeIdx < pJoin->probe->blk->info.rows && pCtx->buildIdx < pJoin->build->blk->info.rows) {
      if (probeTs == buildTs) {
        mJoinEqualCart(pOperator, probeTs, pRes);
        if (pRes->info.rows >= pOperator->resultInfo.threshold) {
          return;
        }
        break;
      } else if (LEFT_JOIN_NO_EQUAL(asc, probeTs, buildTs)) {
        pCtx->probeNEqGrps.beginIdx = pCtx->probeIdx;

        do {
          pCtx->probeNEqGrps.rowsNum++;
          probeTs = *((int64_t*)pProbeCol->pData + (++pCtx->probeIdx)); 
        } while (pCtx->probeIdx < pJoin->probe->blk->info.rows && LEFT_JOIN_NO_EQUAL(asc, probeTs, buildTs));
        
        mJoinNonEqualCart(pOperator, &pCtx->probeNEqGrps, pRes);
        
        if (pRes->info.rows >= pOperator->resultInfo.threshold) {
          return;
        }
      } else {
        buildTs = *((int64_t*)pBuildCol->pData + (++pCtx->buildIdx)); 
        while (pCtx->buildIdx < pJoin->build->blk->info.rows && LEFT_JOIN_DISCRAD(asc, probeTs, buildTs)) {
          buildTs = *((int64_t*)pBuildCol->pData + (++pCtx->buildIdx)); 
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

