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

int32_t mJoinInitMergeCtx(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode) {
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;

  pCtx->pJoin = pJoin;
  pCtx->lastEqTs = INT64_MIN;
  pCtx->hashCan = pJoin->probe->keyNum > 0;

  pCtx->finBlk = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);
  blockDataEnsureCapacity(pCtx->finBlk, TMAX(MJOIN_DEFAULT_BLK_ROWS_NUM, MJOIN_BLK_SIZE_LIMIT/pJoinNode->node.pOutputDataBlockDesc->totalRowSize));

  if (pJoin->pFPreFilter) {
    pCtx->midBlk = createOneDataBlock(pCtx->finBlk, false);
    blockDataEnsureCapacity(pCtx->midBlk, pCtx->finBlk->info.capacity);
  }

  pCtx->blkThreshold = pCtx->finBlk->info.capacity * 0.5;
  
  return TSDB_CODE_SUCCESS;
}

static int32_t mLeftJoinGrpNonEqCart(SMJoinOperatorInfo* pJoin, SSDataBlock* pRes, bool append, SMJoinGrpRows* pGrp) {
  SMJoinTableCtx* probe = pJoin->probe;
  SMJoinTableCtx* build = pJoin->build;
  int32_t currRows = append ? pRes->info.rows : 0;
  int32_t firstRows = GRP_REMAIN_ROWS(pGrp);
  
  for (int32_t c = 0; c < probe->finNum; ++c) {
    SMJoinColMap* pFirstCol = probe->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pGrp->blk->pDataBlock, pFirstCol->srcSlot);
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
  SMJoinTableCtx* probe = pJoin->probe;
  SMJoinTableCtx* build = pJoin->build;
  int32_t currRows = append ? pRes->info.rows : 0;
  int32_t firstRows = GRP_REMAIN_ROWS(pFirst);  
  int32_t secondRows = GRP_REMAIN_ROWS(pSecond);
  ASSERT(secondRows > 0);

  for (int32_t c = 0; c < probe->finNum; ++c) {
    SMJoinColMap* pFirstCol = probe->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pFirst->blk->pDataBlock, pFirstCol->srcSlot);
    SColumnInfoData* pOutCol = taosArrayGet(pRes->pDataBlock, pFirstCol->dstSlot);
    for (int32_t r = 0; r < firstRows; ++r) {
      if (colDataIsNull_s(pInCol, pFirst->readIdx + r)) {
        colDataSetNItemsNull(pOutCol, currRows + r * secondRows, secondRows);
      } else {
        ASSERT(pRes->info.capacity >= (pRes->info.rows + firstRows * secondRows));
        uint32_t startOffset = (IS_VAR_DATA_TYPE(pOutCol->info.type)) ? pOutCol->varmeta.length : ((currRows + r * secondRows) * pOutCol->info.bytes);
        ASSERT((startOffset + 1 * pOutCol->info.bytes) <= pRes->info.capacity * pOutCol->info.bytes);
        colDataSetNItems(pOutCol, currRows + r * secondRows, colDataGetData(pInCol, pFirst->readIdx + r), secondRows, true);
      }
    }
  }

  for (int32_t c = 0; c < build->finNum; ++c) {
    SMJoinColMap* pSecondCol = build->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(pSecond->blk->pDataBlock, pSecondCol->srcSlot);
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
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, 0);
  int32_t buildGrpNum = taosArrayGetSize(build->eqGrps);
  int32_t probeRows = GRP_REMAIN_ROWS(probeGrp);
  int32_t probeEndIdx = probeGrp->endIdx;

  if (0 == build->grpIdx && probeRows * build->grpTotalRows <= rowsLeft) {
    SMJoinGrpRows* pFirstBuild = taosArrayGet(build->eqGrps, 0);
    if (pFirstBuild->readIdx == pFirstBuild->beginIdx) {
      for (; build->grpIdx < buildGrpNum; ++build->grpIdx) {
        SMJoinGrpRows* buildGrp = taosArrayGet(build->eqGrps, build->grpIdx);
        MJ_ERR_RET(mLeftJoinGrpEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp));
        buildGrp->readIdx = buildGrp->beginIdx;
      }

      pCtx->grpRemains = false;
      return TSDB_CODE_SUCCESS;
    }
  }

  for (; !GRP_DONE(probeGrp); ) {
    probeGrp->endIdx = probeGrp->readIdx;
    for (; build->grpIdx < buildGrpNum && rowsLeft > 0; ++build->grpIdx) {
      SMJoinGrpRows* buildGrp = taosArrayGet(build->eqGrps, build->grpIdx);

      if (rowsLeft >= GRP_REMAIN_ROWS(buildGrp)) {
        MJ_ERR_RET(mLeftJoinGrpEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp));
        rowsLeft -= GRP_REMAIN_ROWS(buildGrp);
        buildGrp->readIdx = buildGrp->beginIdx;
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
    probeGrp->endIdx = probeEndIdx;

    if (build->grpIdx >= buildGrpNum) {
      build->grpIdx = 0;
      ++probeGrp->readIdx; 
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
  if ((*ppMid)->info.rows < (*ppFin)->info.rows) {
    pLess = (*ppMid);
    pMore = (*ppFin);
  } else {
    pLess = (*ppFin);
    pMore = (*ppMid);
  }

  int32_t totalRows = pMore->info.rows + pLess->info.rows;
  if (totalRows <= pMore->info.capacity) {
    MJ_ERR_RET(blockDataMerge(pMore, pLess));
    blockDataCleanup(pLess);
    pCtx->midRemains = false;
  } else {
    int32_t copyRows = pMore->info.capacity - pMore->info.rows;
    MJ_ERR_RET(blockDataMergeNRows(pMore, pLess, pLess->info.rows - copyRows, copyRows));
    blockDataShrinkNRows(pLess, copyRows);
    pCtx->midRemains = true;
  }

  if (pMore != (*ppFin)) {
    TSWAP(*ppMid, *ppFin);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mLeftJoinMergeSeqCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);
  int32_t buildGrpNum = taosArrayGetSize(build->eqGrps);
  int32_t probeEndIdx = probeGrp->endIdx;
  int32_t rowsLeft = pCtx->midBlk->info.capacity;  
  bool contLoop = true;

  blockDataCleanup(pCtx->midBlk);

  do {
    for (; !GRP_DONE(probeGrp) && !BLK_IS_FULL(pCtx->finBlk); 
      ++probeGrp->readIdx, probeGrp->readMatch = false, probeGrp->endIdx = probeEndIdx, build->grpIdx = 0) {
      probeGrp->endIdx = probeGrp->readIdx;
      
      rowsLeft = pCtx->midBlk->info.capacity;
      for (; build->grpIdx < buildGrpNum && rowsLeft > 0; ++build->grpIdx) {
        SMJoinGrpRows* buildGrp = taosArrayGet(build->eqGrps, build->grpIdx);

        if (rowsLeft >= GRP_REMAIN_ROWS(buildGrp)) {
          MJ_ERR_RET(mLeftJoinGrpEqCart(pCtx->pJoin, pCtx->midBlk, true, probeGrp, buildGrp));
          rowsLeft -= GRP_REMAIN_ROWS(buildGrp);
          buildGrp->readIdx = buildGrp->beginIdx;
          continue;
        }
        
        int32_t buildEndIdx = buildGrp->endIdx;
        buildGrp->endIdx = buildGrp->readIdx + rowsLeft - 1;
        ASSERT(buildGrp->endIdx >= buildGrp->readIdx);
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
      } else {
        MJ_ERR_RET(mLeftJoinCopyMergeMidBlk(pCtx, &pCtx->midBlk, &pCtx->finBlk));
        
        if (pCtx->midRemains) {
          contLoop = false;
        } else if (build->grpIdx == buildGrpNum) {
          continue;
        }
      }

      //need break

      probeGrp->endIdx = probeEndIdx;
      
      if (build->grpIdx >= buildGrpNum) {
        build->grpIdx = 0;
        ++probeGrp->readIdx;
        probeGrp->readMatch = false;
      }      

      break;
    }

    if (GRP_DONE(probeGrp) || BLK_IS_FULL(pCtx->finBlk)) {
      break;
    }
  } while (contLoop);

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}

static int32_t mLeftJoinMergeCart(SMJoinMergeCtx* pCtx) {
  return (NULL == pCtx->pJoin->pFPreFilter) ? mLeftJoinMergeFullCart(pCtx) : mLeftJoinMergeSeqCart(pCtx);
}

static int32_t mLeftJoinNonEqCart(SMJoinMergeCtx* pCtx) {
  int32_t rowsLeft = pCtx->finBlk->info.capacity - pCtx->finBlk->info.rows;
  SMJoinGrpRows* probeGrp = &pCtx->probeNEqGrp;
  if (rowsLeft <= 0) {
    pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;
    return TSDB_CODE_SUCCESS;
  }
  
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


static bool mLeftJoinRetrieve(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin, SMJoinMergeCtx* pCtx) {
  bool probeGot = mJoinRetrieveImpl(pJoin, &pJoin->probe->blkRowIdx, &pJoin->probe->blk, pJoin->probe);
  bool buildGot = false;

  do {
    if (probeGot || MJOIN_DS_NEED_INIT(pOperator, pJoin->build)) {  
      buildGot = mJoinRetrieveImpl(pJoin, &pJoin->build->blkRowIdx, &pJoin->build->blk, pJoin->build);
    }
    
    if (!probeGot) {
      mJoinSetDone(pOperator);
      return false;
    }

    if (buildGot) {
      SColumnInfoData* pProbeCol = taosArrayGet(pJoin->probe->blk->pDataBlock, pJoin->probe->primCol->srcSlot);
      SColumnInfoData* pBuildCol = taosArrayGet(pJoin->build->blk->pDataBlock, pJoin->build->primCol->srcSlot);
      if (*((int64_t*)pProbeCol->pData + pJoin->probe->blkRowIdx) > *((int64_t*)pBuildCol->pData + pJoin->build->blk->info.rows - 1)) {
        pJoin->build->blkRowIdx = pJoin->build->blk->info.rows;
        continue;
      }
    }
    
    break;
  } while (true);

  return true;
}

static bool mLeftJoinHashGrpCart(SSDataBlock* pBlk, SMJoinGrpRows* probeGrp, bool append, SMJoinTableCtx* probe, SMJoinTableCtx* build) {
  int32_t rowsLeft = append ? (pBlk->info.capacity - pBlk->info.rows) : pBlk->info.capacity;
  if (rowsLeft <= 0) {
    return false;
  }
  
  int32_t buildGrpRows = taosArrayGetSize(build->pHashCurGrp);
  int32_t grpRows = buildGrpRows - build->grpRowIdx;
  if (grpRows <= 0 || build->grpRowIdx < 0) {
    build->grpRowIdx = -1;
    return true;
  }
  
  int32_t actRows = TMIN(grpRows, rowsLeft);
  int32_t currRows = append ? pBlk->info.rows : 0;

  for (int32_t c = 0; c < probe->finNum; ++c) {
    SMJoinColMap* pFirstCol = probe->finCols + c;
    SColumnInfoData* pInCol = taosArrayGet(probeGrp->blk->pDataBlock, pFirstCol->srcSlot);
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
      SMJoinRowPos* pRow = taosArrayGet(build->pHashCurGrp, build->grpRowIdx + r);
      SColumnInfoData* pInCol = taosArrayGet(pRow->pBlk->pDataBlock, pSecondCol->srcSlot);
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
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
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
      probeGrp->endIdx = probeGrp->readIdx;
      MJ_ERR_RET(mLeftJoinGrpNonEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp));
      probeGrp->endIdx = probeEndIdx;
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
      bool contLoop = mLeftJoinHashGrpCart(pCtx->finBlk, probeGrp, true, probe, build);
      if (!contLoop) {
        if (build->grpRowIdx < 0) {
          probeGrp->readIdx++;
        }
        goto _return;
      }  
    }
  }

_return:

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}

static int32_t mLeftJoinHashGrpCartFilter(SMJoinMergeCtx* pCtx, bool* contLoop) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);

  blockDataCleanup(pCtx->midBlk);

  do {
    mLeftJoinHashGrpCart(pCtx->midBlk, probeGrp, true, probe, build);

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
        break;
      }

      continue;
    }
  } while (true);

  *contLoop = true;
  return TSDB_CODE_SUCCESS;
}


static int32_t mLeftJoinHashSeqCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, 0);
  bool contLoop = false;

  if (build->grpRowIdx >= 0) {
    MJ_ERR_RET(mLeftJoinHashGrpCartFilter(pCtx, &contLoop));
    if (build->grpRowIdx < 0) {
      probeGrp->readIdx++;
      probeGrp->readMatch = false;
    }

    if (!contLoop) {
      goto _return;
    }
  }

  size_t bufLen = 0;
  int32_t probeEndIdx = probeGrp->endIdx;
  for (; !GRP_DONE(probeGrp) && !BLK_IS_FULL(pCtx->finBlk);) {
    if (mJoinCopyKeyColsDataToBuf(probe, probeGrp->readIdx, &bufLen)) {
      probeGrp->endIdx = probeGrp->readIdx;
      MJ_ERR_RET(mLeftJoinGrpNonEqCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp));
      probeGrp->endIdx = probeEndIdx;
      probeGrp->readIdx++;
      probeGrp->readMatch = false;
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

      probeGrp->endIdx = probeGrp->readIdx;      
      MJ_ERR_RET(mLeftJoinHashGrpCartFilter(pCtx, &contLoop));
      probeGrp->endIdx = probeEndIdx;
      if (build->grpRowIdx < 0) {
        probeGrp->readIdx++;
        probeGrp->readMatch = false;
      }

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
    if (!lastBuildGrp || !pCtx->hashJoin) {
      MJ_ERR_RET(mJoinMakeBuildTbHash(pJoin, pJoin->build));
    }

    if (pJoin->probe->newBlk) {
      MJ_ERR_RET(mJoinSetKeyColsData(pJoin->probe->blk, pJoin->probe));
      pJoin->probe->newBlk = false;
    }
    
    pCtx->hashJoin = true;    

    return mLeftJoinHashCart(pCtx);
  }

  pCtx->hashJoin = false;    
  
  return mLeftJoinMergeCart(pCtx);
}

static int32_t mLeftJoinHandleMidRemains(SMJoinMergeCtx* pCtx) {
  ASSERT(0 < pCtx->midBlk->info.rows);

  TSWAP(pCtx->midBlk, pCtx->finBlk);

  pCtx->midRemains = false;

  return TSDB_CODE_SUCCESS;
}


static int32_t mLeftJoinHandleGrpRemains(SMJoinMergeCtx* pCtx) {
  if (pCtx->lastEqGrp) {
    return (pCtx->hashJoin) ? mLeftJoinHashCart(pCtx) : mLeftJoinMergeCart(pCtx);
  }
  
  return mLeftJoinNonEqCart(pCtx);
}

SSDataBlock* mLeftJoinDo(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t probeTs = 0;
  int64_t buildTs = 0;
  SColumnInfoData* pBuildCol = NULL;
  SColumnInfoData* pProbeCol = NULL;
  bool asc = (pJoin->inputTsOrder == TSDB_ORDER_ASC) ? true : false;

  blockDataCleanup(pCtx->finBlk);

  if (pCtx->midRemains) {
    MJ_ERR_JRET(mLeftJoinHandleMidRemains(pCtx));
    if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
      return pCtx->finBlk;
    }
    pCtx->midRemains = false;
  }

  if (pCtx->grpRemains) {
    MJ_ERR_JRET(mLeftJoinHandleGrpRemains(pCtx));
    if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
      return pCtx->finBlk;
    }
    pCtx->grpRemains = false;
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

      if (MJOIN_PROBE_TB_ROWS_DONE(pJoin->probe)) {
        continue;
      } else {
        MJOIN_GET_TB_CUR_TS(pProbeCol, probeTs, pJoin->probe);
      }
    }

    while (!MJOIN_PROBE_TB_ROWS_DONE(pJoin->probe) && !MJOIN_BUILD_TB_ROWS_DONE(pJoin->build)) {
      if (probeTs == buildTs) {
        pCtx->lastEqTs = probeTs;
        MJ_ERR_JRET(mLeftJoinProcessEqualGrp(pCtx, probeTs, false));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }

        MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
        MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
      } else if (LEFT_JOIN_NO_EQUAL(asc, probeTs, buildTs)) {
        pCtx->probeNEqGrp.blk = pJoin->probe->blk;
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

    if (!MJOIN_PROBE_TB_ROWS_DONE(pJoin->probe) && pJoin->build->dsFetchDone) {
      pCtx->probeNEqGrp.blk = pJoin->probe->blk;
      pCtx->probeNEqGrp.beginIdx = pJoin->probe->blkRowIdx;
      pCtx->probeNEqGrp.readIdx = pCtx->probeNEqGrp.beginIdx;
      pCtx->probeNEqGrp.endIdx = pJoin->probe->blk->info.rows - 1;
      
      pJoin->probe->blkRowIdx = pJoin->probe->blk->info.rows;
            
      MJ_ERR_JRET(mLeftJoinNonEqCart(pCtx));
      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
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

static bool mInnerJoinRetrieve(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin, SMJoinMergeCtx* pCtx) {
  bool probeGot = mJoinRetrieveImpl(pJoin, &pJoin->probe->blkRowIdx, &pJoin->probe->blk, pJoin->probe);
  bool buildGot = false;

  if (probeGot || MJOIN_DS_NEED_INIT(pOperator, pJoin->build)) {  
    buildGot = mJoinRetrieveImpl(pJoin, &pJoin->build->blkRowIdx, &pJoin->build->blk, pJoin->build);
  }
  
  if (!probeGot || !buildGot) {
    mJoinSetDone(pOperator);
    return false;
  }

  return true;
}


SSDataBlock* mInnerJoinDo(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t probeTs = 0;
  int64_t buildTs = 0;
  SColumnInfoData* pBuildCol = NULL;
  SColumnInfoData* pProbeCol = NULL;
  bool asc = (pJoin->inputTsOrder == TSDB_ORDER_ASC) ? true : false;

  blockDataCleanup(pCtx->finBlk);

  if (pCtx->grpRemains) {
    MJ_ERR_JRET(mLeftJoinHandleGrpRemains(pCtx));
    if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
      return pCtx->finBlk;
    }
    pCtx->grpRemains = false;
  }

  do {
    if (!mInnerJoinRetrieve(pOperator, pJoin, pCtx)) {
      break;
    }

    MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
    MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
    
    if (probeTs == pCtx->lastEqTs) {
      MJ_ERR_JRET(mLeftJoinProcessEqualGrp(pCtx, probeTs, true));
      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
      }

      if (MJOIN_PROBE_TB_ROWS_DONE(pJoin->probe)) {
        continue;
      } else {
        MJOIN_GET_TB_CUR_TS(pProbeCol, probeTs, pJoin->probe);
      }
    }

    while (!MJOIN_PROBE_TB_ROWS_DONE(pJoin->probe) && !MJOIN_BUILD_TB_ROWS_DONE(pJoin->build)) {
      if (probeTs == buildTs) {
        pCtx->lastEqTs = probeTs;
        MJ_ERR_JRET(mLeftJoinProcessEqualGrp(pCtx, probeTs, false));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }

        MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
        MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
      } else if (LEFT_JOIN_NO_EQUAL(asc, probeTs, buildTs)) {
        pCtx->probeNEqGrp.blk = pJoin->probe->blk;
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

    if (!MJOIN_PROBE_TB_ROWS_DONE(pJoin->probe) && pJoin->build->dsFetchDone) {
      pCtx->probeNEqGrp.blk = pJoin->probe->blk;
      pCtx->probeNEqGrp.beginIdx = pJoin->probe->blkRowIdx;
      pCtx->probeNEqGrp.readIdx = pCtx->probeNEqGrp.beginIdx;
      pCtx->probeNEqGrp.endIdx = pJoin->probe->blk->info.rows - 1;
      
      pJoin->probe->blkRowIdx = pJoin->probe->blk->info.rows;
            
      MJ_ERR_JRET(mLeftJoinNonEqCart(pCtx));
      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
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



