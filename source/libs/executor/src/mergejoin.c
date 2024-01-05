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

static int32_t mOuterJoinHashFullCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);

  if (build->grpRowIdx >= 0) {
    bool contLoop = mJoinHashGrpCart(pCtx->finBlk, probeGrp, true, probe, build);
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
      MJ_ERR_RET(mJoinNonEqGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, true));
      probeGrp->endIdx = probeEndIdx;
      continue;
    }

    void* pGrp = tSimpleHashGet(build->pGrpHash, probe->keyData, bufLen);
    if (NULL == pGrp) {
      probeGrp->endIdx = probeGrp->readIdx;
      MJ_ERR_RET(mJoinNonEqGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, true));
      probeGrp->endIdx = probeEndIdx;
      continue;
    }

    if (build->rowBitmapSize > 0) {
      build->pHashCurGrp = ((SMJoinHashGrpRows*)pGrp)->pRows;
      build->pHashGrpRows = pGrp;
      build->pHashGrpRows->allRowsMatch = true;
    } else {
      build->pHashCurGrp = *(SArray**)pGrp;
    }
    
    build->grpRowIdx = 0;
    bool contLoop = mJoinHashGrpCart(pCtx->finBlk, probeGrp, true, probe, build);
    if (!contLoop) {
      if (build->grpRowIdx < 0) {
        probeGrp->readIdx++;
      }
      goto _return;
    }  
  }

_return:

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}


static int32_t mOuterJoinMergeFullCart(SMJoinMergeCtx* pCtx) {
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
        MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp));
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
        MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp));
        rowsLeft -= GRP_REMAIN_ROWS(buildGrp);
        buildGrp->readIdx = buildGrp->beginIdx;
        continue;
      }
      
      int32_t buildEndIdx = buildGrp->endIdx;
      buildGrp->endIdx = buildGrp->readIdx + rowsLeft - 1;
      mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp);
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

static int32_t mOuterJoinMergeSeqCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);
  int32_t buildGrpNum = taosArrayGetSize(build->eqGrps);
  int32_t probeEndIdx = probeGrp->endIdx;
  int32_t rowsLeft = pCtx->midBlk->info.capacity;  
  bool contLoop = true;
  int32_t startGrpIdx = 0;
  int32_t startRowIdx = -1;

  blockDataCleanup(pCtx->midBlk);

  do {
    for (; !GRP_DONE(probeGrp) && !BLK_IS_FULL(pCtx->finBlk); 
      ++probeGrp->readIdx, probeGrp->readMatch = false, probeGrp->endIdx = probeEndIdx, build->grpIdx = 0) {
      probeGrp->endIdx = probeGrp->readIdx;
      
      rowsLeft = pCtx->midBlk->info.capacity;
      startGrpIdx = build->grpIdx;
      startRowIdx = -1;
      
      for (; build->grpIdx < buildGrpNum && rowsLeft > 0; ++build->grpIdx) {
        SMJoinGrpRows* buildGrp = taosArrayGet(build->eqGrps, build->grpIdx);
        if (startRowIdx < 0) {
          startRowIdx = buildGrp->readIdx;
        }

        if (rowsLeft >= GRP_REMAIN_ROWS(buildGrp)) {
          MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->midBlk, true, probeGrp, buildGrp));
          rowsLeft -= GRP_REMAIN_ROWS(buildGrp);
          buildGrp->readIdx = buildGrp->beginIdx;
          continue;
        }
        
        int32_t buildEndIdx = buildGrp->endIdx;
        buildGrp->endIdx = buildGrp->readIdx + rowsLeft - 1;
        ASSERT(buildGrp->endIdx >= buildGrp->readIdx);
        MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->midBlk, true, probeGrp, buildGrp));
        buildGrp->readIdx += rowsLeft;
        buildGrp->endIdx = buildEndIdx;
        rowsLeft = 0;
        break;
      }

      if (pCtx->midBlk->info.rows > 0) {
        if (build->rowBitmapSize > 0) {
          MJ_ERR_RET(mJoinFilterAndMarkRows(pCtx->midBlk, pCtx->pJoin->pFPreFilter, build, startGrpIdx, startRowIdx));
        } else {
          MJ_ERR_RET(doFilter(pCtx->midBlk, pCtx->pJoin->pFPreFilter, NULL));
        }

        if (pCtx->midBlk->info.rows > 0) {
          probeGrp->readMatch = true;
        }
      } 

      if (0 == pCtx->midBlk->info.rows) {
        if (build->grpIdx == buildGrpNum) {
          if (!probeGrp->readMatch) {
            MJ_ERR_RET(mJoinNonEqGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, true));
          }

          continue;
        }
      } else {
        MJ_ERR_RET(mJoinCopyMergeMidBlk(pCtx, &pCtx->midBlk, &pCtx->finBlk));
        
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

static int32_t mOuterJoinHashGrpCartFilter(SMJoinMergeCtx* pCtx, bool* contLoop) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);
  int32_t startRowIdx = 0;
  
  blockDataCleanup(pCtx->midBlk);

  do {
    startRowIdx = build->grpRowIdx;
    mJoinHashGrpCart(pCtx->midBlk, probeGrp, true, probe, build);

    if (pCtx->midBlk->info.rows > 0) {
      if (build->rowBitmapSize > 0) {
        MJ_ERR_RET(mJoinFilterAndMarkHashRows(pCtx->midBlk, pCtx->pJoin->pPreFilter, build, startRowIdx));
      } else {
        MJ_ERR_RET(doFilter(pCtx->midBlk, pCtx->pJoin->pPreFilter, NULL));
      }
      if (pCtx->midBlk->info.rows > 0) {
        probeGrp->readMatch = true;
      }
    } 

    if (0 == pCtx->midBlk->info.rows) {
      if (build->grpRowIdx < 0) {
        if (!probeGrp->readMatch) {
          MJ_ERR_RET(mJoinNonEqGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, true));
        }

        break;
      }
      
      continue;
    } else {
      MJ_ERR_RET(mJoinCopyMergeMidBlk(pCtx, &pCtx->midBlk, &pCtx->finBlk));
      
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


static int32_t mOuterJoinHashSeqCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, 0);
  bool contLoop = false;

  if (build->grpRowIdx >= 0) {
    MJ_ERR_RET(mOuterJoinHashGrpCartFilter(pCtx, &contLoop));
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
      MJ_ERR_RET(mJoinNonEqGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, true));
      probeGrp->endIdx = probeEndIdx;
      probeGrp->readIdx++;
      probeGrp->readMatch = false;
      continue;
    }

    void* pGrp = tSimpleHashGet(build->pGrpHash, probe->keyData, bufLen);
    if (NULL == pGrp) {
      probeGrp->endIdx = probeGrp->readIdx;
      MJ_ERR_RET(mJoinNonEqGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, true));
      probeGrp->endIdx = probeEndIdx;
      probeGrp->readIdx++;
      probeGrp->readMatch = false;
      continue;
    }

    if (build->rowBitmapSize > 0) {
      build->pHashCurGrp = ((SMJoinHashGrpRows*)pGrp)->pRows;
      build->pHashGrpRows = pGrp;
      if (0 == build->pHashGrpRows->rowBitmapOffset) {
        MJ_ERR_RET(mJoinGetRowBitmapOffset(build, taosArrayGetSize(build->pHashCurGrp), &build->pHashGrpRows->rowBitmapOffset));
      }
    } else {
      build->pHashCurGrp = *(SArray**)pGrp;
    }
    
    build->grpRowIdx = 0;

    probeGrp->endIdx = probeGrp->readIdx;      
    MJ_ERR_RET(mOuterJoinHashGrpCartFilter(pCtx, &contLoop));
    probeGrp->endIdx = probeEndIdx;
    if (build->grpRowIdx < 0) {
      probeGrp->readIdx++;
      probeGrp->readMatch = false;
    }

    if (!contLoop) {
      break;
    }
  }

_return:

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}


static int32_t mLeftJoinMergeCart(SMJoinMergeCtx* pCtx) {
  return (NULL == pCtx->pJoin->pFPreFilter) ? mOuterJoinMergeFullCart(pCtx) : mOuterJoinMergeSeqCart(pCtx);
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

static int32_t mLeftJoinHashCart(SMJoinMergeCtx* pCtx) {
  return (NULL == pCtx->pJoin->pPreFilter) ? mOuterJoinHashFullCart(pCtx) : mOuterJoinHashSeqCart(pCtx);
}

static FORCE_INLINE int32_t mLeftJoinHandleGrpRemains(SMJoinMergeCtx* pCtx) {
  if (pCtx->lastEqGrp) {
    return (pCtx->hashJoin) ? (*pCtx->hashCartFp)(pCtx) : (*pCtx->mergeCartFp)(pCtx);
  }
  
  return mJoinNonEqCart(pCtx, &pCtx->probeNEqGrp, true);
}

SSDataBlock* mLeftJoinDo(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t probeTs = 0;
  int64_t buildTs = 0;
  SColumnInfoData* pBuildCol = NULL;
  SColumnInfoData* pProbeCol = NULL;

  blockDataCleanup(pCtx->finBlk);

  if (pCtx->midRemains) {
    MJ_ERR_JRET(mJoinHandleMidRemains(pCtx));
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
      MJ_ERR_JRET(mJoinProcessEqualGrp(pCtx, probeTs, true));
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
        MJ_ERR_JRET(mJoinProcessEqualGrp(pCtx, probeTs, false));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }

        MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
        MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
      } else if (PROBE_TS_UNREACH(pCtx->ascTs, probeTs, buildTs)) {
        MJ_ERR_JRET(mJoinProcessUnreachGrp(pCtx, pJoin->probe, pProbeCol, &probeTs, &buildTs));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }
      } else {
        while (++pJoin->build->blkRowIdx < pJoin->build->blk->info.rows) {
          MJOIN_GET_TB_CUR_TS(pBuildCol, buildTs, pJoin->build);
          if (PROBE_TS_OVER(pCtx->ascTs, probeTs, buildTs)) {
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
            
      MJ_ERR_JRET(mJoinNonEqCart(pCtx, &pCtx->probeNEqGrp, true));
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

static int32_t mInnerJoinMergeCart(SMJoinMergeCtx* pCtx) {
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
        MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp));
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
        MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp));
        rowsLeft -= GRP_REMAIN_ROWS(buildGrp);
        buildGrp->readIdx = buildGrp->beginIdx;
        continue;
      }
      
      int32_t buildEndIdx = buildGrp->endIdx;
      buildGrp->endIdx = buildGrp->readIdx + rowsLeft - 1;
      mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp);
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


static int32_t mInnerJoinHashCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);

  if (build->grpRowIdx >= 0) {
    bool contLoop = mJoinHashGrpCart(pCtx->finBlk, probeGrp, true, probe, build);
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
    if (NULL != pGrp) {
      build->pHashCurGrp = *pGrp;
      build->grpRowIdx = 0;
      bool contLoop = mJoinHashGrpCart(pCtx->finBlk, probeGrp, true, probe, build);
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

static FORCE_INLINE int32_t mInnerJoinHandleGrpRemains(SMJoinMergeCtx* pCtx) {
  return (pCtx->hashJoin) ? (*pCtx->hashCartFp)(pCtx) : (*pCtx->mergeCartFp)(pCtx);
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

  blockDataCleanup(pCtx->finBlk);

  if (pCtx->grpRemains) {
    MJ_ERR_JRET(mInnerJoinHandleGrpRemains(pCtx));
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
      MJ_ERR_JRET(mJoinProcessEqualGrp(pCtx, probeTs, true));
      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
      }

      if (MJOIN_PROBE_TB_ROWS_DONE(pJoin->probe)) {
        continue;
      } else {
        MJOIN_GET_TB_CUR_TS(pProbeCol, probeTs, pJoin->probe);
      }
    }

    do {
      if (probeTs == buildTs) {
        pCtx->lastEqTs = probeTs;
        MJ_ERR_JRET(mJoinProcessEqualGrp(pCtx, probeTs, false));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }

        if (MJOIN_PROBE_TB_ROWS_DONE(pJoin->probe) || MJOIN_BUILD_TB_ROWS_DONE(pJoin->build)) {
          break;
        }
        
        MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
        MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
        continue;
      } else if (PROBE_TS_UNREACH(pCtx->ascTs, probeTs, buildTs)) {
        if (++pJoin->probe->blkRowIdx < pJoin->probe->blk->info.rows) {
          MJOIN_GET_TB_CUR_TS(pProbeCol, probeTs, pJoin->probe);
          continue;
        }
      } else {
        if (++pJoin->build->blkRowIdx < pJoin->build->blk->info.rows) {
          MJOIN_GET_TB_CUR_TS(pBuildCol, buildTs, pJoin->build);
          continue;
        }
      }
      
      break;
    } while (true);
  } while (true);

_return:

  if (code) {
    pJoin->errCode = code;
    return NULL;
  }

  return pCtx->finBlk;
}

static FORCE_INLINE int32_t mFullJoinHandleGrpRemains(SMJoinMergeCtx* pCtx) {
  if (pCtx->lastEqGrp) {
    return (pCtx->hashJoin) ? (*pCtx->hashCartFp)(pCtx) : (*pCtx->mergeCartFp)(pCtx);
  }
  
  return pCtx->lastProbeGrp ? mJoinNonEqCart(pCtx, &pCtx->probeNEqGrp, true) : mJoinNonEqCart(pCtx, &pCtx->buildNEqGrp, false);
}

static bool mFullJoinRetrieve(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin, SMJoinMergeCtx* pCtx) {
  bool probeGot = mJoinRetrieveImpl(pJoin, &pJoin->probe->blkRowIdx, &pJoin->probe->blk, pJoin->probe);
  bool buildGot = mJoinRetrieveImpl(pJoin, &pJoin->build->blkRowIdx, &pJoin->build->blk, pJoin->build);
  
  if (!probeGot && !buildGot) {
    return false;
  }

  return true;
}

static FORCE_INLINE int32_t mFullJoinHashCart(SMJoinMergeCtx* pCtx) {
  return (NULL == pCtx->pJoin->pPreFilter) ? mOuterJoinHashFullCart(pCtx) : mOuterJoinHashSeqCart(pCtx);
}

static int32_t mFullJoinMergeCart(SMJoinMergeCtx* pCtx) {
  return (NULL == pCtx->pJoin->pFPreFilter) ? mOuterJoinMergeFullCart(pCtx) : mOuterJoinMergeSeqCart(pCtx);
}

const uint8_t lowest_bit_bitmap[] = {32, 7, 6, 32, 5, 3, 32, 0, 4, 1, 2};

static FORCE_INLINE int32_t mFullJoinOutputHashRow(SMJoinMergeCtx* pCtx, SMJoinHashGrpRows* pGrpRows, int32_t idx) {
  SMJoinGrpRows grp = {0};
  SMJoinRowPos* pPos = taosArrayGet(pGrpRows->pRows, idx);
  grp.blk = pPos->pBlk;
  grp.readIdx = pPos->pos;
  grp.endIdx = pPos->pos;
  return mJoinNonEqGrpCart(pCtx->pJoin, pCtx->finBlk, true, &grp, false);
}

static int32_t mFullJoinOutputHashGrpRows(SMJoinMergeCtx* pCtx, SMJoinHashGrpRows* pGrpRows, SMJoinNMatchCtx* pNMatch, bool* grpDone) {
  int32_t rowNum = taosArrayGetSize(pGrpRows->pRows);
  for (; pNMatch->rowIdx < rowNum && !BLK_IS_FULL(pCtx->finBlk); ++pNMatch->rowIdx) {
    MJ_ERR_RET(mFullJoinOutputHashRow(pCtx, pGrpRows, pNMatch->rowIdx));
  }

  if (pNMatch->rowIdx >= rowNum) {
    *grpDone = true;
    pNMatch->rowIdx = 0;
  }
  
  return TSDB_CODE_SUCCESS;
}

static int32_t mFullJoinHandleHashGrpRemains(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinNMatchCtx* pNMatch = &build->nMatchCtx;
  if (NULL == pNMatch->pGrp) {
    pNMatch->pGrp = tSimpleHashIterate(build->pGrpHash, pNMatch->pGrp, &pNMatch->iter);
    pNMatch->bitIdx = 0;
  }

  int32_t baseIdx = 0;
  while (NULL != pNMatch->pGrp) {
    SMJoinHashGrpRows* pGrpRows = (SMJoinHashGrpRows*)pNMatch->pGrp;
    if (pGrpRows->allRowsMatch) {
      pNMatch->pGrp = tSimpleHashIterate(build->pGrpHash, pNMatch->pGrp, &pNMatch->iter);
      pNMatch->bitIdx = 0;
      continue;
    }
  
    if (pGrpRows->rowMatchNum <= 0 || pGrpRows->allRowsNMatch) {
      pGrpRows->allRowsNMatch = true;

      bool grpDone = false;      
      MJ_ERR_RET(mFullJoinOutputHashGrpRows(pCtx, pGrpRows, pNMatch, &grpDone));
      if (BLK_IS_FULL(pCtx->finBlk)) {
        if (grpDone) {
          pNMatch->pGrp = tSimpleHashIterate(build->pGrpHash, pNMatch->pGrp, &pNMatch->iter);
          pNMatch->bitIdx = 0;      
        }
        
        pCtx->nmatchRemains = true;
        return TSDB_CODE_SUCCESS;
      }

      pNMatch->pGrp = tSimpleHashIterate(build->pGrpHash, pNMatch->pGrp, &pNMatch->iter);
      pNMatch->bitIdx = 0;      
      continue;
    }

    int32_t grpRowNum = taosArrayGetSize(pGrpRows->pRows);
    int32_t bitBytes = BitmapLen(grpRowNum);
    for (; pNMatch->bitIdx < bitBytes; ++pNMatch->bitIdx) {
      if (0 == build->pRowBitmap[pGrpRows->rowBitmapOffset + pNMatch->bitIdx]) {
        continue;
      }

      baseIdx = 8 * pNMatch->bitIdx;
      char *v = &build->pRowBitmap[pGrpRows->rowBitmapOffset + pNMatch->bitIdx];
      while (*v && !BLK_IS_FULL(pCtx->finBlk)) {
        uint8_t n = lowest_bit_bitmap[((*v & (*v - 1)) ^ *v) % 11];
        if (baseIdx + n >= grpRowNum) {
          MJOIN_SET_ROW_BITMAP(build->pRowBitmap, pGrpRows->rowBitmapOffset + pNMatch->bitIdx, n);
          continue;
        }

        MJ_ERR_RET(mFullJoinOutputHashRow(pCtx, pGrpRows, baseIdx + n));
        MJOIN_SET_ROW_BITMAP(build->pRowBitmap, pGrpRows->rowBitmapOffset + pNMatch->bitIdx, n);
        if (++pGrpRows->rowMatchNum == taosArrayGetSize(pGrpRows->pRows)) {
          pGrpRows->allRowsMatch = true;
          pNMatch->bitIdx = bitBytes;
          break;
        }
      }
  
      if (BLK_IS_FULL(pCtx->finBlk)) {
        if (pNMatch->bitIdx == bitBytes) {
          pNMatch->pGrp = tSimpleHashIterate(build->pGrpHash, pNMatch->pGrp, &pNMatch->iter);
          pNMatch->bitIdx = 0;      
        }

        pCtx->nmatchRemains = true;
        return TSDB_CODE_SUCCESS;
      }
    }

    pNMatch->pGrp = tSimpleHashIterate(build->pGrpHash, pNMatch->pGrp, &pNMatch->iter);
    pNMatch->bitIdx = 0;
  }
  
  pCtx->nmatchRemains = false;
  pCtx->lastEqGrp = false;
  
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t mFullJoinOutputMergeRow(SMJoinMergeCtx* pCtx, SMJoinGrpRows* pGrpRows, int32_t idx) {
  SMJoinGrpRows grp = {0};
  grp.blk = pGrpRows->blk;
  grp.readIdx = idx;
  grp.endIdx = idx;
  return mJoinNonEqGrpCart(pCtx->pJoin, pCtx->finBlk, true, &grp, false);
}


static int32_t mFullJoinOutputMergeGrpRows(SMJoinMergeCtx* pCtx, SMJoinGrpRows* pGrpRows, SMJoinNMatchCtx* pNMatch, bool* grpDone) {
  for (; pNMatch->rowIdx <= pGrpRows->endIdx && !BLK_IS_FULL(pCtx->finBlk); ++pNMatch->rowIdx) {
    MJ_ERR_RET(mFullJoinOutputMergeRow(pCtx, pGrpRows, pNMatch->rowIdx));
  }

  if (pNMatch->rowIdx > pGrpRows->endIdx) {
    *grpDone = true;
    pNMatch->rowIdx = 0;
  }
  
  return TSDB_CODE_SUCCESS;
}


static int32_t mFullJoinHandleMergeGrpRemains(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinNMatchCtx* pNMatch = &build->nMatchCtx;
  bool grpDone = false;
  int32_t baseIdx = 0;
  int32_t rowNum = 0;
  int32_t grpNum = taosArrayGetSize(build->eqGrps);
  for (; pNMatch->grpIdx < grpNum; ++pNMatch->grpIdx, pNMatch->bitIdx = 0) {
    grpDone = false;
    
    SMJoinGrpRows* pGrpRows = taosArrayGet(build->eqGrps, pNMatch->grpIdx);
    if (pGrpRows->allRowsMatch) {
      continue;
    }

    if (pGrpRows->rowMatchNum <= 0 || pGrpRows->allRowsNMatch) {
      if (pGrpRows->rowMatchNum <= 0) {
        pGrpRows->allRowsNMatch = true;
        pNMatch->rowIdx = pGrpRows->beginIdx;
      }
      
      MJ_ERR_RET(mFullJoinOutputMergeGrpRows(pCtx, pGrpRows, pNMatch, &grpDone));

      if (BLK_IS_FULL(pCtx->finBlk)) {
        if (grpDone) {
          ++pNMatch->grpIdx;
          pNMatch->bitIdx = 0;
        }
        
        pCtx->nmatchRemains = true;
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }

    int32_t bitBytes = BitmapLen(pGrpRows->endIdx - pGrpRows->beginIdx + 1);
    baseIdx = 8 * pNMatch->bitIdx;
    rowNum = pGrpRows->endIdx - pGrpRows->beginIdx + 1;
    for (; pNMatch->bitIdx < bitBytes; ++pNMatch->bitIdx) {
      if (0 == build->pRowBitmap[pGrpRows->rowBitmapOffset + pNMatch->bitIdx]) {
        continue;
      }

      char *v = &build->pRowBitmap[pGrpRows->rowBitmapOffset + pNMatch->bitIdx];
      while (*v && !BLK_IS_FULL(pCtx->finBlk)) {
        uint8_t n = lowest_bit_bitmap[((*v & (*v - 1)) ^ *v) % 11];
        if (baseIdx + n > pGrpRows->endIdx) {
          MJOIN_SET_ROW_BITMAP(build->pRowBitmap, pGrpRows->rowBitmapOffset + pNMatch->bitIdx, n);
          continue;
        }
        
        ASSERT(baseIdx + n <= pGrpRows->endIdx);
        MJ_ERR_RET(mFullJoinOutputMergeRow(pCtx, pGrpRows, baseIdx + n));

        MJOIN_SET_ROW_BITMAP(build->pRowBitmap, pGrpRows->rowBitmapOffset + pNMatch->bitIdx, n);
        if (++pGrpRows->rowMatchNum == rowNum) {
          pGrpRows->allRowsMatch = true;
          pNMatch->bitIdx = bitBytes;
          break;
        }
      }
    }

    if (BLK_IS_FULL(pCtx->finBlk)) {
      if (pNMatch->bitIdx == bitBytes) {
        ++pNMatch->grpIdx;
        pNMatch->bitIdx = 0;
      }
      
      pCtx->nmatchRemains = true;
      return TSDB_CODE_SUCCESS;
    }      
  }

  pCtx->nmatchRemains = false;
  pCtx->lastEqGrp = false;  
  
  return TSDB_CODE_SUCCESS;  
}

static int32_t mFullJoinHandleBuildTableRemains(SMJoinMergeCtx* pCtx) {
  return pCtx->hashJoin ? mFullJoinHandleHashGrpRemains(pCtx) : mFullJoinHandleMergeGrpRemains(pCtx);
}

SSDataBlock* mFullJoinDo(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t probeTs = 0;
  int64_t buildTs = 0;
  SColumnInfoData* pBuildCol = NULL;
  SColumnInfoData* pProbeCol = NULL;

  blockDataCleanup(pCtx->finBlk);

  if (pCtx->midRemains) {
    MJ_ERR_JRET(mJoinHandleMidRemains(pCtx));
    if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
      return pCtx->finBlk;
    }
    pCtx->midRemains = false;
  }

  if (pCtx->grpRemains) {
    MJ_ERR_JRET(mFullJoinHandleGrpRemains(pCtx));
    if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
      return pCtx->finBlk;
    }
    pCtx->grpRemains = false;
  }

  if (pCtx->nmatchRemains) {
    MJ_ERR_JRET(mFullJoinHandleBuildTableRemains(pCtx));
    if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
      return pCtx->finBlk;
    }
  }

  do {
    if (!mFullJoinRetrieve(pOperator, pJoin, pCtx)) {
      if (pCtx->lastEqGrp && pJoin->build->rowBitmapSize > 0) {
        MJ_ERR_JRET(mFullJoinHandleBuildTableRemains(pCtx));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }
      }

      mJoinSetDone(pOperator);      
      break;
    }

    MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
    MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
    
    if (probeTs == pCtx->lastEqTs) {
      MJ_ERR_JRET(mJoinProcessEqualGrp(pCtx, probeTs, true));
      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
      }

      if (FJOIN_PROBE_TB_ROWS_DONE(pJoin->probe)) {
        continue;
      } else {
        MJOIN_GET_TB_CUR_TS(pProbeCol, probeTs, pJoin->probe);
      }
    }

    if (pCtx->lastEqGrp && pJoin->build->rowBitmapSize > 0) {
      MJ_ERR_JRET(mFullJoinHandleBuildTableRemains(pCtx));
      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
      }
    }

    while (!FJOIN_PROBE_TB_ROWS_DONE(pJoin->probe) && !MJOIN_BUILD_TB_ROWS_DONE(pJoin->build)) {
      if (probeTs == buildTs) {
        pCtx->lastEqTs = probeTs;
        MJ_ERR_JRET(mJoinProcessEqualGrp(pCtx, probeTs, false));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }

        MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
        MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);

        if (!FJOIN_PROBE_TB_ROWS_DONE(pJoin->probe) && probeTs != pCtx->lastEqTs && pJoin->build->rowBitmapSize > 0) {
          MJ_ERR_JRET(mFullJoinHandleBuildTableRemains(pCtx));
          if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
            return pCtx->finBlk;
          }
        }

        continue;
      }

      if (PROBE_TS_UNREACH(pCtx->ascTs, probeTs, buildTs)) {
        MJ_ERR_JRET(mJoinProcessUnreachGrp(pCtx, pJoin->probe, pProbeCol, &probeTs, &buildTs));
      } else {
        MJ_ERR_JRET(mJoinProcessOverGrp(pCtx, pJoin->build, pBuildCol, &probeTs, &buildTs));
      }

      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
      }
    }

    if (pJoin->build->dsFetchDone && !FJOIN_PROBE_TB_ROWS_DONE(pJoin->probe)) {
      if (pCtx->lastEqGrp && pJoin->build->rowBitmapSize > 0) {
        MJ_ERR_JRET(mFullJoinHandleBuildTableRemains(pCtx));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }
      }
      
      pCtx->probeNEqGrp.blk = pJoin->probe->blk;
      pCtx->probeNEqGrp.beginIdx = pJoin->probe->blkRowIdx;
      pCtx->probeNEqGrp.readIdx = pCtx->probeNEqGrp.beginIdx;
      pCtx->probeNEqGrp.endIdx = pJoin->probe->blk->info.rows - 1;
      
      pJoin->probe->blkRowIdx = pJoin->probe->blk->info.rows;
            
      MJ_ERR_JRET(mJoinNonEqCart(pCtx, &pCtx->probeNEqGrp, true));
      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
      }
    }

    if (pJoin->probe->dsFetchDone && !MJOIN_BUILD_TB_ROWS_DONE(pJoin->build)) {
      if (pCtx->lastEqGrp && pJoin->build->rowBitmapSize > 0) {
        MJ_ERR_JRET(mFullJoinHandleBuildTableRemains(pCtx));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }
      }

      pCtx->buildNEqGrp.blk = pJoin->build->blk;
      pCtx->buildNEqGrp.beginIdx = pJoin->build->blkRowIdx;
      pCtx->buildNEqGrp.readIdx = pCtx->buildNEqGrp.beginIdx;
      pCtx->buildNEqGrp.endIdx = pJoin->build->blk->info.rows - 1;
      
      pJoin->build->blkRowIdx = pJoin->build->blk->info.rows;
            
      MJ_ERR_JRET(mJoinNonEqCart(pCtx, &pCtx->buildNEqGrp, false));
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


int32_t mJoinInitMergeCtx(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode) {
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;

  pCtx->pJoin = pJoin;
  pCtx->lastEqTs = INT64_MIN;
  pCtx->hashCan = pJoin->probe->keyNum > 0;
    
  if (pJoinNode->node.inputTsOrder != ORDER_DESC) {
    pCtx->ascTs = true;
  }

  pCtx->finBlk = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);
  blockDataEnsureCapacity(pCtx->finBlk, TMAX(MJOIN_DEFAULT_BLK_ROWS_NUM, MJOIN_BLK_SIZE_LIMIT/pJoinNode->node.pOutputDataBlockDesc->totalRowSize));

  if (pJoin->pFPreFilter) {
    pCtx->midBlk = createOneDataBlock(pCtx->finBlk, false);
    blockDataEnsureCapacity(pCtx->midBlk, pCtx->finBlk->info.capacity);
  }

  pCtx->blkThreshold = pCtx->finBlk->info.capacity * 0.5;

  switch (pJoin->joinType) {
    case JOIN_TYPE_INNER:
      pCtx->hashCartFp = (joinCartFp)mInnerJoinHashCart;
      pCtx->mergeCartFp = (joinCartFp)mInnerJoinMergeCart;
      break;
    case JOIN_TYPE_LEFT:
    case JOIN_TYPE_RIGHT:
      pCtx->hashCartFp = (joinCartFp)mLeftJoinHashCart;
      pCtx->mergeCartFp = (joinCartFp)mLeftJoinMergeCart;
      break;
    case JOIN_TYPE_FULL:
      pCtx->hashCartFp = (joinCartFp)mFullJoinHashCart;
      pCtx->mergeCartFp = (joinCartFp)mFullJoinMergeCart;
      break;
    default:
      break;
  }
  
  return TSDB_CODE_SUCCESS;
}


