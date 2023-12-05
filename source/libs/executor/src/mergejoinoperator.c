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

  pInfo->joinFp = (pInfo->subType == JOIN_STYPE_ASOF || pInfo->subType == JOIN_STYPE_WIN) ? mJoinProcessWinJoin: mJoinProcessMergeJoin;
  
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

static int32_t mJoinDoRetrieve(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pTbCtx, SMJoinTableInfo* pTbInfo) {
  bool retrieveCont = false;
  int32_t code = TSDB_CODE_SUCCESS;
  
  do {
    SSDataBlock* pBlock = getNextBlockFromDownstreamRemain(pJoin->pOperator, pTbInfo->downStreamIdx);
    pTbCtx->dsInitDone = true;
    
    if (NULL == pBlock) {
      retrieveCont = false;
      code = (*pJoin->joinFps.handleTbFetchDoneFp)(pJoin, pTbCtx, pTbInfo);
    } else {
      code = (*pJoin->joinFps.handleBlkFetchedFp)(pJoin, pTbCtx, pTbInfo, pBlock, &retrieveCont);
    }
  } while (retrieveCont || TSDB_CODE_SUCCESS != code);

  return code;
}

static FORCE_INLINE bool mJoinBuildTbNeedRetrieve(SMJoinOperatorInfo* pJoin, SMJoinTableCtx* pBuildCtx) {
  if ((!pBuildCtx->grpRetrieved) && (!pBuildCtx->dsFetchDone)) {
    return true;
  }
  return false;
}

static FORCE_INLINE bool mJoinProbeTbNeedRetrieve(SOperatorInfo* pOperator, SMJoinTableCtx* pProbeCtx, SMJoinTableCtx* pBuildCtx) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  if (MJOIN_DOWNSTREAM_NEED_INIT(pOperator) && !pBuildCtx->dsInitDone) {
    return true;
  }
  if (((!pProbeCtx->grpRetrieved) && (!pProbeCtx->dsFetchDone)) && (pBuildCtx->grpRetrieved || pJoin->ctx.flags.retrieveAfterBuildDone))) {
    return true;
  }
  return false;
}

static bool mJoinOpenMergeJoin(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  SMJoinTableCtx* pProbeCtx = &pCtx->probeTbCtx;
  SMJoinTableCtx* pBuildCtx = &pCtx->buildTbCtx;
  int32_t code = mJoinDoRetrieve(pOperator, pBuildCtx, pJoin->pBuild);
  if (TSDB_CODE_SUCCESS != code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }
  if (pBuildCtx->dsFetchDone && !pJoin->ctx.flags.retrieveAfterBuildDone) {
    return false;
  }

  code = mJoinDoRetrieve(pOperator, pProbeCtx, pJoin->pProbe);
  if (TSDB_CODE_SUCCESS != code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }
  return true;
}

static void mJoinInitJoinCtx(SMJoinOperatorInfo* pJoin) {

}

static void mJoinLeftJoinRetrieve(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  SMJoinTableCtx* pProbeCtx = &pCtx->probeTbCtx;
  SMJoinTableCtx* pBuildCtx = &pCtx->buildTbCtx;

  int32_t code = mJoinDoRetrieve(pOperator, pProbeCtx, pJoin->pProbe);
  if (TSDB_CODE_SUCCESS != code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }

  if (NULL == pProbeCtx->pHeadBlk) {
    return;
  }
  
  code = mJoinDoRetrieve(pJoin, pBuildCtx, pJoin->pBuild);
  if (TSDB_CODE_SUCCESS != code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }

  pJoin->ctx.mergeCtx.joinPhase = E_JOIN_PHASE_TS_JOIN;
}

static FORCE_INLINE void mJoinLefeJoinUpdateTsJoinCtx(SMJoinTsJoinCtx* pCtx, SMJoinTableCtx* pProbeCtx, SMJoinTableCtx* pBuildCtx) {
  pCtx->probeRowNum = pProbeCtx->pCurrBlk->pBlk->info.rows;
  pCtx->buildRowNum = pBuildCtx->pCurrBlk->pBlk->info.rows;
  SColumnInfoData* probeCol = taosArrayGet(pProbeCtx->pCurrBlk->pBlk, pProbeCtx->pTbInfo->primCol->srcSlot);
  SColumnInfoData* buildCol = taosArrayGet(pBuildCtx->pHeadBlk->pBlk, pBuildCtx->pTbInfo->primCol->srcSlot);
  pCtx->probeTs = (int64_t*)probeCol->pData; 
  pCtx->probeEndTs = (int64_t*)probeCol->pData + pCtx->probeRowNum - 1;
  pCtx->buildTs = (int64_t*)buildCol->pData; 
  pCtx->buildEndTs = (int64_t*)buildCol->pData + pCtx->buildRowNum - 1;
}

static bool mJoinMoveToNextProbeTable(SMJoinTsJoinCtx* pCtx, SMJoinTableCtx* pProbeCtx) {
  if (NULL == pProbeCtx->pCurrBlk->pNext) {
    pProbeCtx->blkIdx++;
    return false;
  }
  
  pProbeCtx->pCurrBlk = pProbeCtx->pCurrBlk->pNext;
  pCtx->probeRowNum = pProbeCtx->pCurrBlk->pBlk->info.rows;
  SColumnInfoData* probeCol = taosArrayGet(pProbeCtx->pCurrBlk->pBlk, pProbeCtx->pTbInfo->primCol->srcSlot);
  pCtx->probeTs = (int64_t*)probeCol->pData;
  pCtx->probeEndTs = (int64_t*)probeCol->pData + pCtx->probeRowNum - 1;
  pProbeCtx->blkRowIdx = 0;

  return true;
}

static bool mJoinMoveToNextBuildTable(SMJoinOperatorInfo* pJoin, SMJoinTsJoinCtx* pCtx, SMJoinTableCtx* pBuildCtx, SMJoinTableCtx* pProbeCtx) {
  bool contLoop = false;
  bool res = false;
  
  do {
    if (pBuildCtx->pCurrBlk->pNext) {
      pBuildCtx->blkIdx++;
      return false;
    }
    
    pBuildCtx->pCurrBlk = pBuildCtx->pCurrBlk->pNext;
    pCtx->buildRowNum = pBuildCtx->pCurrBlk->pBlk->info.rows;
    SColumnInfoData* buildCol = taosArrayGet(pBuildCtx->pCurrBlk->pBlk, pBuildCtx->pTbInfo->primCol->srcSlot);
    pCtx->buildTs = (int64_t*)buildCol->pData;
    pCtx->buildEndTs = (int64_t*)buildCol->pData + pCtx->buildRowNum - 1;
    pBuildCtx->blkRowIdx = 0;

    do {
      if (*pCtx->buildTs > pCtx->probeTs[pProbeCtx->blkRowIdx]) {
        mJoinLeftJoinAddBlkToGrp(pJoin, pCtx, pProbeCtx, pBuildCtx);
        contLoop = mJoinMoveToNextProbeTable(pCtx, pProbeCtx);
      } else if (pCtx->probeTs[pProbeCtx->blkRowIdx] > *pCtx->buildEndTs) {
        break;
      } else {
        contLoop = false;
        res = true;
      }
    } while (contLoop);
  } while (contLoop);

  return res;
}



static void mJoinLeftJoinSplitGrp(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  SMJoinTsJoinCtx* pCtx = &pJoin->ctx.mergeCtx.tsJoinCtx;
  SMJoinTableCtx* pProbeCtx = &pJoin->ctx.mergeCtx.probeTbCtx;
  SMJoinTableCtx* pBuildCtx = &pJoin->ctx.mergeCtx.buildTbCtx;

  mJoinLefeJoinUpdateTsJoinCtx(pCtx, pProbeCtx, pBuildCtx);
    
  bool nextRow = false;
  for (; pProbeCtx->blkIdx < pProbeCtx->blkNum; mJoinMoveToNextProbeTable(pCtx, pProbeCtx)) {
    if (*pCtx->buildTs > *pCtx->probeEndTs) {
      mJoinLeftJoinAddBlkToGrp(pJoin, pCtx, pProbeCtx, pBuildCtx);
      continue;
    } else if (*pCtx->probeTs > *pCtx->buildEndTs) {
      if (!mJoinMoveToNextBuildTable(pJoin, pCtx, pBuildCtx, pProbeCtx)) {
        break;
      //retrieve build
      }
    }

    for (; pProbeCtx->blkRowIdx < pCtx->probeRowNum; ++pProbeCtx->blkRowIdx) {
      for (; pBuildCtx->blkIdx < pBuildCtx->blkNum; mJoinMoveToNextBuildTable(pJoin, pCtx, pBuildCtx, pProbeCtx)) {
        for (; pBuildCtx->blkRowIdx < pCtx->buildRowNum; ++pBuildCtx->blkRowIdx) {
          if (pCtx->probeTs[pProbeCtx->blkRowIdx] > pCtx->buildTs[pBuildCtx->blkRowIdx]) {
            FIN_SAME_TS_GRP();
            FIN_DIFF_TS_GRP();
            continue;
          } else if (pCtx->probeTs[pProbeCtx->blkRowIdx] == pCtx->buildTs[pBuildCtx->blkRowIdx]) {
            FIN_DIFF_TS_GRP();
            if (pCtx->inSameTsGrp) {
              pCtx->currGrpPairCtx.buildGrp.grpRowNum++;
            } else {
              pCtx->inSameTsGrp = true;
              pCtx->currGrpPairCtx.buildGrp.grpRowBeginIdx = pBuildCtx->blkRowIdx;
            }
          } else {
            FIN_SAME_TS_GRP();
            if (pCtx->inDiffTsGrp) {
              pCtx->currGrpPairCtx.probeGrp.grpRowNum++;
            } else {
              pCtx->inDiffTsGrp = true;
              pCtx->currGrpPairCtx.probeGrp.grpRowBeginIdx = pProbeCtx->blkRowIdx;
            }
            nextRow = true;
            break;
          }
        }

        // end of single build table
        if (nextRow) {
          break;
        }
      }

      // end of all build tables
      if (nextRow) {
        continue;
      }
      
      if (pCtx->inSameTsGrp) {
        PAUSE_SAME_TS_GRP();
      }
      
      break;
    }

    // end of single probe table
    if (nextRow) {
      continue;
    }

    break;
  }

  // end of all probe tables
  FIN_DIFF_TS_GRP();

  pJoin->ctx.mergeCtx.joinPhase = E_JOIN_PHASE_OUTPUT;

  return true;
}

static bool mJoinLeftJoinOutput(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {

}

static SSDataBlock* mJoinDoLeftJoin(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  bool contLoop = false;
  
  do {
    switch (pJoin->ctx.mergeCtx.joinPhase) {
      case E_JOIN_PHASE_RETRIEVE:
        contLoop = mJoinLeftJoinRetrieve(pOperator, pJoin);
        break;
      case E_JOIN_PHASE_SPLIT:
        contLoop = mJoinLeftJoinSplitGrp(pOperator, pJoin);
        break;
      case E_JOIN_PHASE_OUTPUT:
        contLoop = mJoinLeftJoinOutput(pOperator, pJoin);
        break;
    }
  } while (contLoop);

}

static SSDataBlock* mJoinHanleMergeJoin(SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  SMJoinTableCtx* pProbeCtx = &pCtx->probeTbCtx;
  SMJoinTableCtx* pBuildCtx = &pCtx->buildTbCtx;
  int32_t code = TSDB_CODE_SUCCESS;
  SSDataBlock* pBlock = NULL;

  while (true) {
    pBlock = (*pJoin->joinFp)(pOperator, pJoin, pCtx, pBuildCtx, pProbeCtx);
    if (pBlock && pBlock->info.rows > 0) {
      return pBlock;
    }
  }
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
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  setOperatorInfo(pOperator, "MergeJoinOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN, false, OP_NOT_OPENED, pInfo, pTaskInfo);

  mJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 0, &pJoinNode->inputStat[0]);
  mJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 1, &pJoinNode->inputStat[1]);

  mJoinSetBuildAndProbeTable(pInfo, pJoinNode);
  
  mJoinInitJoinCtx(pInfo);
  
  code = mJoinBuildResColMap(pInfo, pJoinNode);
  if (code) {
    goto _error;
  }

  code = initHJoinBufPages(pInfo);
  if (code) {
    goto _error;
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

