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



static bool mLeftJoinRetrieve(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
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
  
  return mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->probeNEqGrp, true);
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
    if (!mLeftJoinRetrieve(pOperator, pJoin)) {
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
      } else if (PROBE_TS_LOWER(pCtx->ascTs, probeTs, buildTs)) {
        MJ_ERR_JRET(mJoinProcessLowerGrp(pCtx, pJoin->probe, pProbeCol, &probeTs, &buildTs));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }
      } else {
        while (++pJoin->build->blkRowIdx < pJoin->build->blk->info.rows) {
          MJOIN_GET_TB_CUR_TS(pBuildCol, buildTs, pJoin->build);
          if (PROBE_TS_GREATER(pCtx->ascTs, probeTs, buildTs)) {
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
            
      MJ_ERR_JRET(mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->probeNEqGrp, true));
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


static bool mInnerJoinRetrieve(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
  bool probeGot = mJoinRetrieveImpl(pJoin, &pJoin->probe->blkRowIdx, &pJoin->probe->blk, pJoin->probe);
  bool buildGot = false;

  if (probeGot || MJOIN_DS_NEED_INIT(pOperator, pJoin->build)) {  
    buildGot = mJoinRetrieveImpl(pJoin, &pJoin->build->blkRowIdx, &pJoin->build->blk, pJoin->build);
  }
  
  if (!probeGot) {
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
    if (!mInnerJoinRetrieve(pOperator, pJoin)) {
      break;
    }

    MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
    MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
    
    if (probeTs == pCtx->lastEqTs) {
      MJ_ERR_JRET(mJoinProcessEqualGrp(pCtx, probeTs, true));
      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
      }

      if (MJOIN_PROBE_TB_ROWS_DONE(pJoin->probe) || MJOIN_BUILD_TB_ROWS_DONE(pJoin->build)) {
        continue;
      } 

      MJOIN_GET_TB_CUR_TS(pProbeCol, probeTs, pJoin->probe);
    } else if (MJOIN_BUILD_TB_ROWS_DONE(pJoin->build)) {
      mJoinSetDone(pOperator);
      break;
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
      }

      if (PROBE_TS_LOWER(pCtx->ascTs, probeTs, buildTs)) {
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
  
  return pCtx->lastProbeGrp ? mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->probeNEqGrp, true) : mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->buildNEqGrp, false);
}

static bool mFullJoinRetrieve(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin) {
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
  static const uint8_t lowest_bit_bitmap[] = {32, 7, 6, 32, 5, 3, 32, 0, 4, 1, 2};
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
  static const uint8_t lowest_bit_bitmap[] = {32, 7, 6, 32, 5, 3, 32, 0, 4, 1, 2};
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
      if (!pGrpRows->allRowsNMatch) {
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
    rowNum = pGrpRows->endIdx - pGrpRows->beginIdx + 1;
    for (; pNMatch->bitIdx < bitBytes; ++pNMatch->bitIdx) {
      if (0 == build->pRowBitmap[pGrpRows->rowBitmapOffset + pNMatch->bitIdx]) {
        continue;
      }

      baseIdx = 8 * pNMatch->bitIdx;
      char *v = &build->pRowBitmap[pGrpRows->rowBitmapOffset + pNMatch->bitIdx];
      while (*v && !BLK_IS_FULL(pCtx->finBlk)) {
        uint8_t n = lowest_bit_bitmap[((*v & (*v - 1)) ^ *v) % 11];
        if (pGrpRows->beginIdx + baseIdx + n > pGrpRows->endIdx) {
          MJOIN_SET_ROW_BITMAP(build->pRowBitmap, pGrpRows->rowBitmapOffset + pNMatch->bitIdx, n);
          continue;
        }
        
        MJ_ERR_RET(mFullJoinOutputMergeRow(pCtx, pGrpRows, pGrpRows->beginIdx + baseIdx + n));

        MJOIN_SET_ROW_BITMAP(build->pRowBitmap, pGrpRows->rowBitmapOffset + pNMatch->bitIdx, n);
        if (++pGrpRows->rowMatchNum == rowNum) {
          pGrpRows->allRowsMatch = true;
          pNMatch->bitIdx = bitBytes;
          break;
        }
      }

      if (BLK_IS_FULL(pCtx->finBlk)) {
        break;
      }
    }

    if (BLK_IS_FULL(pCtx->finBlk)) {
      if (pNMatch->bitIdx >= bitBytes) {
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
    if (!mFullJoinRetrieve(pOperator, pJoin)) {
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

      if (PROBE_TS_LOWER(pCtx->ascTs, probeTs, buildTs)) {
        MJ_ERR_JRET(mJoinProcessLowerGrp(pCtx, pJoin->probe, pProbeCol, &probeTs, &buildTs));
      } else {
        MJ_ERR_JRET(mJoinProcessGreaterGrp(pCtx, pJoin->build, pBuildCol, &probeTs, &buildTs));
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
            
      MJ_ERR_JRET(mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->probeNEqGrp, true));
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
            
      MJ_ERR_JRET(mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->buildNEqGrp, false));
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


static int32_t mSemiJoinHashGrpCartFilter(SMJoinMergeCtx* pCtx, SMJoinGrpRows* probeGrp) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  
  do {
    blockDataCleanup(pCtx->midBlk);

    mJoinHashGrpCart(pCtx->midBlk, probeGrp, true, probe, build);

    if (pCtx->midBlk->info.rows > 0) {
      MJ_ERR_RET(mJoinFilterAndKeepSingleRow(pCtx->midBlk, pCtx->pJoin->pPreFilter));
    }

    if (pCtx->midBlk->info.rows <= 0) {
      if (build->grpRowIdx < 0) {
        break;
      }
      
      continue;
    }

    ASSERT(1 == pCtx->midBlk->info.rows);
    MJ_ERR_RET(mJoinCopyMergeMidBlk(pCtx, &pCtx->midBlk, &pCtx->finBlk));
    ASSERT(false == pCtx->midRemains);
    
    break;
  } while (true);

  return TSDB_CODE_SUCCESS;
}


static int32_t mSemiJoinHashSeqCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, 0);

  size_t bufLen = 0;
  int32_t probeEndIdx = probeGrp->endIdx;
  for (; !GRP_DONE(probeGrp) && !BLK_IS_FULL(pCtx->finBlk); probeGrp->readIdx++) {
    if (mJoinCopyKeyColsDataToBuf(probe, probeGrp->readIdx, &bufLen)) {
      continue;
    }

    void* pGrp = tSimpleHashGet(build->pGrpHash, probe->keyData, bufLen);
    if (NULL == pGrp) {
      continue;
    }

    build->pHashCurGrp = *(SArray**)pGrp;
    build->grpRowIdx = 0;

    probeGrp->endIdx = probeGrp->readIdx;      
    MJ_ERR_RET(mSemiJoinHashGrpCartFilter(pCtx, probeGrp));
    probeGrp->endIdx = probeEndIdx;
  }

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}


static int32_t mSemiJoinHashFullCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);
  size_t bufLen = 0;

  for (; !GRP_DONE(probeGrp) && !BLK_IS_FULL(pCtx->finBlk); ++probeGrp->readIdx) {
    if (mJoinCopyKeyColsDataToBuf(probe, probeGrp->readIdx, &bufLen)) {
      continue;
    }

    void* pGrp = tSimpleHashGet(build->pGrpHash, probe->keyData, bufLen);
    if (NULL == pGrp) {
      continue;
    }

    build->pHashCurGrp = *(SArray**)pGrp;
    ASSERT(1 == taosArrayGetSize(build->pHashCurGrp));
    build->grpRowIdx = 0;
    mJoinHashGrpCart(pCtx->finBlk, probeGrp, true, probe, build);
    ASSERT(build->grpRowIdx < 0);
  }

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}


static int32_t mSemiJoinMergeSeqCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);
  SMJoinGrpRows* buildGrp = NULL;
  int32_t buildGrpNum = taosArrayGetSize(build->eqGrps);
  int32_t probeEndIdx = probeGrp->endIdx;
  int32_t rowsLeft = pCtx->midBlk->info.capacity;  

  do {
    for (; !GRP_DONE(probeGrp) && !BLK_IS_FULL(pCtx->finBlk); 
      ++probeGrp->readIdx, probeGrp->endIdx = probeEndIdx, build->grpIdx = 0) {
      probeGrp->endIdx = probeGrp->readIdx;
      
      rowsLeft = pCtx->midBlk->info.capacity;

      blockDataCleanup(pCtx->midBlk);      
      for (; build->grpIdx < buildGrpNum && rowsLeft > 0; ++build->grpIdx) {
        buildGrp = taosArrayGet(build->eqGrps, build->grpIdx);

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
        MJ_ERR_RET(mJoinFilterAndKeepSingleRow(pCtx->midBlk, pCtx->pJoin->pFPreFilter));
      } 

      if (0 == pCtx->midBlk->info.rows) {
        if (build->grpIdx == buildGrpNum) {
          continue;
        }
      } else {
        ASSERT(1 == pCtx->midBlk->info.rows);
        MJ_ERR_RET(mJoinCopyMergeMidBlk(pCtx, &pCtx->midBlk, &pCtx->finBlk));
        ASSERT(false == pCtx->midRemains);

        if (build->grpIdx == buildGrpNum) {
          continue;
        }

        buildGrp->readIdx = buildGrp->beginIdx;        
        continue;
      }

      //need break

      probeGrp->endIdx = probeEndIdx;
      break;
    }

    if (GRP_DONE(probeGrp) || BLK_IS_FULL(pCtx->finBlk)) {
      break;
    }
  } while (true);

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}


static int32_t mSemiJoinMergeFullCart(SMJoinMergeCtx* pCtx) {
  int32_t rowsLeft = pCtx->finBlk->info.capacity - pCtx->finBlk->info.rows;
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, 0);
  SMJoinGrpRows* buildGrp = taosArrayGet(build->eqGrps, 0);
  int32_t probeRows = GRP_REMAIN_ROWS(probeGrp);
  int32_t probeEndIdx = probeGrp->endIdx;

  ASSERT(1 == taosArrayGetSize(build->eqGrps));
  ASSERT(buildGrp->beginIdx == buildGrp->endIdx);

  if (probeRows <= rowsLeft) {
    MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp));

    pCtx->grpRemains = false;
    return TSDB_CODE_SUCCESS;
  }

  probeGrp->endIdx = probeGrp->readIdx + rowsLeft - 1;
  MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp));
  probeGrp->readIdx = probeGrp->endIdx + 1; 
  probeGrp->endIdx = probeEndIdx;

  pCtx->grpRemains = true;
  
  return TSDB_CODE_SUCCESS;  
}


static int32_t mSemiJoinHashCart(SMJoinMergeCtx* pCtx) {
  return (NULL == pCtx->pJoin->pPreFilter) ? mSemiJoinHashFullCart(pCtx) : mSemiJoinHashSeqCart(pCtx);
}

static int32_t mSemiJoinMergeCart(SMJoinMergeCtx* pCtx) {
  return (NULL == pCtx->pJoin->pFPreFilter) ? mSemiJoinMergeFullCart(pCtx) : mSemiJoinMergeSeqCart(pCtx);
}

static FORCE_INLINE int32_t mSemiJoinHandleGrpRemains(SMJoinMergeCtx* pCtx) {
  return (pCtx->hashJoin) ? (*pCtx->hashCartFp)(pCtx) : (*pCtx->mergeCartFp)(pCtx);
}


SSDataBlock* mSemiJoinDo(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t probeTs = 0;
  int64_t buildTs = 0;
  SColumnInfoData* pBuildCol = NULL;
  SColumnInfoData* pProbeCol = NULL;

  blockDataCleanup(pCtx->finBlk);

  if (pCtx->grpRemains) {
    MJ_ERR_JRET(mSemiJoinHandleGrpRemains(pCtx));
    if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
      return pCtx->finBlk;
    }
    pCtx->grpRemains = false;
  }

  do {
    if (!mInnerJoinRetrieve(pOperator, pJoin)) {
      break;
    }

    MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
    MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
    
    if (probeTs == pCtx->lastEqTs) {
      MJ_ERR_JRET(mJoinProcessEqualGrp(pCtx, probeTs, true));
      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
      }

      if (MJOIN_PROBE_TB_ROWS_DONE(pJoin->probe) || MJOIN_BUILD_TB_ROWS_DONE(pJoin->build)) {
        continue;
      } 

      MJOIN_GET_TB_CUR_TS(pProbeCol, probeTs, pJoin->probe);
    } else if (MJOIN_BUILD_TB_ROWS_DONE(pJoin->build)) {
      mJoinSetDone(pOperator);
      break;
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
      }

      if (PROBE_TS_LOWER(pCtx->ascTs, probeTs, buildTs)) {
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


static FORCE_INLINE int32_t mAntiJoinHandleGrpRemains(SMJoinMergeCtx* pCtx) {
  if (pCtx->lastEqGrp) {
    return (pCtx->hashJoin) ? (*pCtx->hashCartFp)(pCtx) : (*pCtx->mergeCartFp)(pCtx);
  }
  
  return mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->probeNEqGrp, true);
}

static int32_t mAntiJoinHashFullCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);
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
    }
  }

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}


static int32_t mAntiJoinHashGrpCartFilter(SMJoinMergeCtx* pCtx, SMJoinGrpRows* probeGrp) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  
  do {
    blockDataCleanup(pCtx->midBlk);

    mJoinHashGrpCart(pCtx->midBlk, probeGrp, true, probe, build);

    if (pCtx->midBlk->info.rows > 0) {
      MJ_ERR_RET(mJoinFilterAndNoKeepRows(pCtx->midBlk, pCtx->pJoin->pPreFilter));
    } 

    if (pCtx->midBlk->info.rows) {
      break;
    }
    
    if (build->grpRowIdx < 0) {
      MJ_ERR_RET(mJoinNonEqGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, true));
      break;
    }
    
    continue;
  } while (true);

  return TSDB_CODE_SUCCESS;
}


static int32_t mAntiJoinHashSeqCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, 0);
  size_t bufLen = 0;
  int32_t probeEndIdx = probeGrp->endIdx;

  for (; !GRP_DONE(probeGrp) && !BLK_IS_FULL(pCtx->finBlk); probeGrp->readIdx++) {
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

    build->pHashCurGrp = *(SArray**)pGrp;
    build->grpRowIdx = 0;

    probeGrp->endIdx = probeGrp->readIdx;      
    MJ_ERR_RET(mAntiJoinHashGrpCartFilter(pCtx, probeGrp));
    probeGrp->endIdx = probeEndIdx;
  }

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}

static int32_t mAntiJoinMergeFullCart(SMJoinMergeCtx* pCtx) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mAntiJoinMergeSeqCart(SMJoinMergeCtx* pCtx) {
  SMJoinTableCtx* probe = pCtx->pJoin->probe;
  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinGrpRows* probeGrp = taosArrayGet(probe->eqGrps, probe->grpIdx);
  SMJoinGrpRows* buildGrp = NULL;
  int32_t buildGrpNum = taosArrayGetSize(build->eqGrps);
  int32_t probeEndIdx = probeGrp->endIdx;
  int32_t rowsLeft = pCtx->midBlk->info.capacity;  

  do {
    for (; !GRP_DONE(probeGrp) && !BLK_IS_FULL(pCtx->finBlk); 
      ++probeGrp->readIdx, probeGrp->endIdx = probeEndIdx, build->grpIdx = 0) {
      probeGrp->endIdx = probeGrp->readIdx;
      
      rowsLeft = pCtx->midBlk->info.capacity;

      blockDataCleanup(pCtx->midBlk);      
      for (; build->grpIdx < buildGrpNum && rowsLeft > 0; ++build->grpIdx) {
        buildGrp = taosArrayGet(build->eqGrps, build->grpIdx);
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
        MJ_ERR_RET(mJoinFilterAndNoKeepRows(pCtx->midBlk, pCtx->pJoin->pFPreFilter));
      } 

      if (pCtx->midBlk->info.rows > 0) {
        if (build->grpIdx < buildGrpNum) {
          buildGrp->readIdx = buildGrp->beginIdx;        
        }

        continue;
      }
      
      if (build->grpIdx >= buildGrpNum) {
        MJ_ERR_RET(mJoinNonEqGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, true));
        continue;
      }

      //need break

      probeGrp->endIdx = probeEndIdx;
      break;
    }

    if (GRP_DONE(probeGrp) || BLK_IS_FULL(pCtx->finBlk)) {
      break;
    }
  } while (true);

  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;

  return TSDB_CODE_SUCCESS;
}


static int32_t mAntiJoinHashCart(SMJoinMergeCtx* pCtx) {
  return (NULL == pCtx->pJoin->pPreFilter) ? mAntiJoinHashFullCart(pCtx) : mAntiJoinHashSeqCart(pCtx);
}

static int32_t mAntiJoinMergeCart(SMJoinMergeCtx* pCtx) {
  return (NULL == pCtx->pJoin->pFPreFilter) ? mAntiJoinMergeFullCart(pCtx) : mAntiJoinMergeSeqCart(pCtx);
}

SSDataBlock* mAntiJoinDo(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t probeTs = 0;
  int64_t buildTs = 0;
  SColumnInfoData* pBuildCol = NULL;
  SColumnInfoData* pProbeCol = NULL;

  blockDataCleanup(pCtx->finBlk);

  if (pCtx->grpRemains) {
    MJ_ERR_JRET(mAntiJoinHandleGrpRemains(pCtx));
    if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
      return pCtx->finBlk;
    }
    pCtx->grpRemains = false;
  }

  do {
    if (!mLeftJoinRetrieve(pOperator, pJoin)) {
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
      } else if (PROBE_TS_LOWER(pCtx->ascTs, probeTs, buildTs)) {
        MJ_ERR_JRET(mJoinProcessLowerGrp(pCtx, pJoin->probe, pProbeCol, &probeTs, &buildTs));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }
      } else {
        while (++pJoin->build->blkRowIdx < pJoin->build->blk->info.rows) {
          MJOIN_GET_TB_CUR_TS(pBuildCol, buildTs, pJoin->build);
          if (PROBE_TS_GREATER(pCtx->ascTs, probeTs, buildTs)) {
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
            
      MJ_ERR_JRET(mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->probeNEqGrp, true));
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


static bool mAsofJoinRetrieve(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin, SMJoinWindowCtx* pCtx) {
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
    
    break;
  } while (true);

  pCtx->probeGrp.blk = pJoin->probe->blk;
  pCtx->buildGrp.blk = pJoin->build->blk;

  return true;
}

int32_t mAsofJoinCalcRowNum(SMJoinWinCache* pCache, int64_t jLimit, int32_t newRows, int32_t* evictRows) {
  if (pCache->outBlk->info.rows <= 0) {
    *evictRows = 0;
    return TMIN(jLimit, newRows);
  }

  if ((pCache->outBlk->info.rows + newRows) <= jLimit) {
    *evictRows = 0;
    return newRows;
  }

  if (newRows >= jLimit) {
    *evictRows = pCache->outBlk->info.rows;
    return jLimit;
  }

  *evictRows = pCache->outBlk->info.rows + newRows - jLimit;
  return newRows;
}

int32_t mAsofJoinAddRowsToCache(SMJoinWindowCtx* pCtx, SMJoinGrpRows* pGrp, bool fromBegin) {
  int32_t evictRows = 0;
  SMJoinWinCache* pCache = &pCtx->cache;
  int32_t rows = mAsofJoinCalcRowNum(pCache, pCtx->jLimit, pGrp->endIdx - pGrp->beginIdx + 1, &evictRows);
  if (evictRows > 0) {
    MJ_ERR_RET(blockDataTrimFirstRows(pCache->outBlk, evictRows));
  }

  int32_t startIdx = fromBegin ? pGrp->beginIdx : pGrp->endIdx - rows + 1;
  return blockDataMergeNRows(pCache->outBlk, pGrp->blk, startIdx, rows);
}

int32_t mAsofJoinBuildEqGrp(SMJoinTableCtx* pTable, int64_t timestamp, bool* wholeBlk, SMJoinGrpRows* pGrp) {
  SColumnInfoData* pCol = taosArrayGet(pTable->blk->pDataBlock, pTable->primCol->srcSlot);

  if (*(int64_t*)colDataGetNumData(pCol, pTable->blkRowIdx) != timestamp) {
    return TSDB_CODE_SUCCESS;
  }

  pGrp->beginIdx = pTable->blkRowIdx;
  pGrp->readIdx = pTable->blkRowIdx;
  
  char* pEndVal = colDataGetNumData(pCol, pTable->blk->info.rows - 1);
  if (timestamp != *(int64_t*)pEndVal) {
    for (; pTable->blkRowIdx < pTable->blk->info.rows; ++pTable->blkRowIdx) {
      char* pNextVal = colDataGetNumData(pCol, pTable->blkRowIdx);
      if (timestamp == *(int64_t*)pNextVal) {
        continue;
      }

      return TSDB_CODE_SUCCESS;
    }
  }

  pGrp->endIdx = pTable->blk->info.rows - 1;
  pTable->blkRowIdx = pTable->blk->info.rows;

  if (wholeBlk) {
    *wholeBlk = true;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t mAsofJoinAddEqRowsToCache(struct SOperatorInfo* pOperator, SMJoinWindowCtx* pCtx, SMJoinTableCtx* pTable, int64_t timestamp) {
  int64_t eqRowsNum = 0;
  SMJoinGrpRows grp = {.blk = pTable->blk};

  do {
      SColumnInfoData* pCol = taosArrayGet(pTable->blk->pDataBlock, pTable->primCol->srcSlot);

      if (*(int64_t*)colDataGetNumData(pCol, pTable->blkRowIdx) != timestamp) {
        return TSDB_CODE_SUCCESS;
      }

      grp.beginIdx = pTable->blkRowIdx;
      
      char* pEndVal = colDataGetNumData(pCol, pTable->blk->info.rows - 1);
      if (timestamp != *(int64_t*)pEndVal) {
        for (; pTable->blkRowIdx < pTable->blk->info.rows; ++pTable->blkRowIdx) {
          char* pNextVal = colDataGetNumData(pCol, pTable->blkRowIdx);
          if (timestamp == *(int64_t*)pNextVal) {
            continue;
          }

          break;
        }

        grp.endIdx = pTable->blkRowIdx - 1;
      } else {
        grp.endIdx = pTable->blk->info.rows - 1;
        pTable->blkRowIdx = pTable->blk->info.rows;
      }

      if (eqRowsNum < pCtx->jLimit) {
        MJ_ERR_RET(mAsofJoinAddRowsToCache(pCtx, &grp, false));
      }
      
      eqRowsNum += grp.endIdx - grp.beginIdx + 1;

    if (pTable->blkRowIdx == pTable->blk->info.rows) {
      pTable->blk = getNextBlockFromDownstreamRemain(pOperator, pTable->downStreamIdx);
      qDebug("%s merge join %s table got block for same ts, rows:%" PRId64, GET_TASKID(pOperator->pTaskInfo), MJOIN_TBTYPE(pTable->type), pTable->blk ? pTable->blk->info.rows : 0);

      pTable->blkRowIdx = 0;

      if (NULL == pTable->blk) {
        pTable->dsFetchDone = true;
        break;
      }    
    } else {
      break;
    }
  } while (true);

  return TSDB_CODE_SUCCESS;
}

int32_t mAsofLowerDumpGrpCache(SMJoinWindowCtx* pCtx) {
  if (pCtx->cache.outBlk->info.rows <= 0) {
    return mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->probeGrp, true);
  }

  int32_t rowsLeft = pCtx->finBlk->info.capacity - pCtx->finBlk->info.rows;
  SMJoinGrpRows* probeGrp = &pCtx->probeGrp;
  SMJoinGrpRows buildGrp = {.blk = pCtx->cache.outBlk, .readIdx = pCtx->cache.outRowIdx, .endIdx = pCtx->cache.outBlk->info.rows - 1};
  int32_t probeRows = GRP_REMAIN_ROWS(probeGrp);
  int32_t probeEndIdx = probeGrp->endIdx;
  int64_t totalResRows = (0 == pCtx->cache.outRowIdx) ? (probeRows * pCtx->cache.outBlk->info.rows) : 
    (pCtx->cache.outBlk->info.rows - pCtx->cache.outRowIdx + (probeRows - 1) * pCtx->cache.outBlk->info.rows);

  if (totalResRows <= rowsLeft) {
    if (0 == pCtx->cache.outRowIdx) {
      MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, &buildGrp));

      pCtx->grpRemains = false;
      return TSDB_CODE_SUCCESS;
    }

    probeGrp->endIdx = probeGrp->readIdx;
    MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, &buildGrp));
    if (++probeGrp->readIdx <= probeEndIdx) {
      probeGrp->endIdx = probeEndIdx;
      buildGrp.readIdx = 0;
      MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, &buildGrp));
    }
    
    pCtx->grpRemains = false;
    return TSDB_CODE_SUCCESS;
  }

  for (; !GRP_DONE(probeGrp) && rowsLeft > 0; ) {
    if (0 == pCtx->cache.outRowIdx) {
      int32_t grpNum = rowsLeft / pCtx->cache.outBlk->info.rows;
      if (grpNum > 0) {
        probeGrp->endIdx = probeGrp->readIdx + grpNum - 1;
        buildGrp.readIdx = 0;
        MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, &buildGrp));
        rowsLeft -= grpNum * pCtx->cache.outBlk->info.rows;
        pCtx->cache.outRowIdx = 0;
        probeGrp->readIdx += grpNum;
        continue;
      }
    }
    
    probeGrp->endIdx = probeGrp->readIdx;
    buildGrp.readIdx = pCtx->cache.outRowIdx;
    
    int32_t grpRemainRows = pCtx->cache.outBlk->info.rows - pCtx->cache.outRowIdx;
    if (rowsLeft >= grpRemainRows) {
      MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, &buildGrp));
      rowsLeft -= grpRemainRows;
      pCtx->cache.outRowIdx = 0;
      continue;
    }
    
    buildGrp.endIdx = buildGrp.readIdx + rowsLeft - 1;
    mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, &buildGrp);
    pCtx->cache.outRowIdx += rowsLeft;
    break;
  }

  probeGrp->endIdx = probeEndIdx;
  pCtx->grpRemains = probeGrp->readIdx <= probeGrp->endIdx;
  
  return TSDB_CODE_SUCCESS;  
}

int32_t mAsofLowerDumpUpdateEqRows(SMJoinWindowCtx* pCtx, SMJoinOperatorInfo* pJoin, bool lastBuildGrp) {
  if (!pCtx->asofEqRow) {
    MJ_ERR_RET(mAsofLowerDumpGrpCache(pCtx));
    if (pCtx->grpRemains) {
      return TSDB_CODE_SUCCESS;
    }

    if (!pCtx->eqPostDone && !lastBuildGrp) {
      pCtx->eqPostDone = true;
      return mAsofJoinAddEqRowsToCache(pJoin->pOperator, pCtx, pJoin->build, pCtx->lastTs);
    }

    return TSDB_CODE_SUCCESS;
  }

  if (!pCtx->eqPostDone && !lastBuildGrp) {
    pCtx->eqPostDone = true;
    MJ_ERR_RET(mAsofJoinAddEqRowsToCache(pJoin->pOperator, pCtx, pJoin->build, pCtx->lastTs));
  }

  return mAsofLowerDumpGrpCache(pCtx);
}

int32_t mAsofLowerProcessEqualGrp(SMJoinWindowCtx* pCtx, int64_t timestamp, bool lastBuildGrp) {
  SMJoinOperatorInfo* pJoin = pCtx->pJoin;

  pCtx->lastEqGrp = true;
  pCtx->eqPostDone = false;

  MJ_ERR_RET(mAsofJoinBuildEqGrp(pJoin->probe, timestamp, NULL, &pCtx->probeGrp));

  return mAsofLowerDumpUpdateEqRows(pCtx, pJoin, lastBuildGrp);
}


int32_t mAsofLowerProcessLowerGrp(SMJoinWindowCtx* pCtx, SMJoinOperatorInfo* pJoin, SColumnInfoData* pCol,  int64_t* probeTs, int64_t* buildTs) {
  pCtx->lastEqGrp = false;
  
  pCtx->probeGrp.beginIdx = pJoin->probe->blkRowIdx;
  pCtx->probeGrp.readIdx = pCtx->probeGrp.beginIdx;
  pCtx->probeGrp.endIdx = pCtx->probeGrp.beginIdx;
  
  while (++pJoin->probe->blkRowIdx < pJoin->probe->blk->info.rows) {
    MJOIN_GET_TB_CUR_TS(pCol, *probeTs, pJoin->probe);
    if (PROBE_TS_LOWER(pCtx->ascTs, *probeTs, *buildTs)) {
      pCtx->probeGrp.endIdx = pJoin->probe->blkRowIdx;
      continue;
    }
    
    break;
  }

  return mAsofLowerDumpGrpCache(pCtx);
}

int32_t mAsofLowerProcessGreaterGrp(SMJoinWindowCtx* pCtx, SMJoinOperatorInfo* pJoin, SColumnInfoData* pCol,  int64_t* probeTs, int64_t* buildTs) {
  pCtx->lastEqGrp = false;

  pCtx->buildGrp.beginIdx = pJoin->build->blkRowIdx;
  pCtx->buildGrp.readIdx = pCtx->buildGrp.beginIdx;
  pCtx->buildGrp.endIdx = pCtx->buildGrp.beginIdx;
  
  while (++pJoin->build->blkRowIdx < pJoin->build->blk->info.rows) {
    MJOIN_GET_TB_CUR_TS(pCol, *buildTs, pJoin->build);
    if (PROBE_TS_GREATER(pCtx->ascTs, *probeTs, *buildTs)) {
      pCtx->buildGrp.endIdx = pJoin->build->blkRowIdx;
      continue;
    }
    
    break;
  }

  pCtx->probeGrp.beginIdx = pJoin->probe->blkRowIdx;
  pCtx->probeGrp.readIdx = pCtx->probeGrp.beginIdx;
  pCtx->probeGrp.endIdx = pCtx->probeGrp.beginIdx;

  MJ_ERR_RET(mAsofJoinAddRowsToCache(pCtx, &pCtx->buildGrp, false));

  return mAsofLowerDumpGrpCache(pCtx);
}

int32_t mAsofLowerHandleGrpRemains(SMJoinWindowCtx* pCtx) {
  return (pCtx->lastEqGrp) ? mAsofLowerDumpUpdateEqRows(pCtx, pCtx->pJoin, false) : mAsofLowerDumpGrpCache(pCtx);
}

static bool mAsofLowerRetrieve(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin, SMJoinWindowCtx* pCtx) {
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
    
    break;
  } while (true);

  pCtx->probeGrp.blk = pJoin->probe->blk;
  pCtx->buildGrp.blk = pJoin->build->blk;

  return true;
}


SSDataBlock* mAsofLowerJoinDo(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinWindowCtx* pCtx = &pJoin->ctx.windowCtx;
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t probeTs = 0;
  int64_t buildTs = 0;
  SColumnInfoData* pBuildCol = NULL;
  SColumnInfoData* pProbeCol = NULL;

  blockDataCleanup(pCtx->finBlk);

  if (pCtx->grpRemains) {
    MJ_ERR_JRET(mAsofLowerHandleGrpRemains(pCtx));
    if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
      return pCtx->finBlk;
    }
    pCtx->grpRemains = false;
  }

  do {
    if (!mAsofLowerRetrieve(pOperator, pJoin, pCtx)) {
      break;
    }

    MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
    MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
    
    if (probeTs == pCtx->lastTs) {
      MJ_ERR_JRET(mAsofLowerProcessEqualGrp(pCtx, probeTs, true));
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
        pCtx->lastTs = probeTs;
        MJ_ERR_JRET(mAsofLowerProcessEqualGrp(pCtx, probeTs, false));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }

        MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
        MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
        continue;
      }

      if (PROBE_TS_LOWER(pCtx->ascTs, probeTs, buildTs)) {
        MJ_ERR_JRET(mAsofLowerProcessLowerGrp(pCtx, pJoin, pProbeCol, &probeTs, &buildTs));
      } else {
        MJ_ERR_JRET(mAsofLowerProcessGreaterGrp(pCtx, pJoin, pBuildCol, &probeTs, &buildTs));
      }

      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
      }
    }

    if (!MJOIN_PROBE_TB_ROWS_DONE(pJoin->probe) && pJoin->build->dsFetchDone) {
      pCtx->probeGrp.beginIdx = pJoin->probe->blkRowIdx;
      pCtx->probeGrp.readIdx = pCtx->probeGrp.beginIdx;
      pCtx->probeGrp.endIdx = pJoin->probe->blk->info.rows - 1;
      
      MJ_ERR_JRET(mAsofLowerDumpGrpCache(pCtx));
      
      pJoin->probe->blkRowIdx = pJoin->probe->blk->info.rows;
            
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


int32_t mAsofGreaterDumpGrpCache(SMJoinWindowCtx* pCtx) {
  int64_t rowsLeft = pCtx->finBlk->info.capacity - pCtx->finBlk->info.rows;
  SMJoinWinCache* cache = &pCtx->cache;
  int32_t buildGrpNum = taosArrayGetSize(cache->grps);
  int64_t buildTotalRows = (cache->rowNum > pCtx->jLimit) ? pCtx->jLimit : cache->rowNum;
  if (buildGrpNum <= 0 || buildTotalRows <= 0) {
    return mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->probeGrp, true);
  }
  
  SMJoinGrpRows* probeGrp = &pCtx->probeGrp;
  int32_t probeRows = GRP_REMAIN_ROWS(probeGrp);
  int32_t probeEndIdx = probeGrp->endIdx;

  if (0 == cache->grpIdx && probeRows * buildTotalRows <= rowsLeft) {
    SMJoinGrpRows* pFirstBuild = taosArrayGet(cache->grps, 0);
    if (pFirstBuild->readIdx == pFirstBuild->beginIdx) {
      for (; cache->grpIdx < buildGrpNum; ++cache->grpIdx) {
        SMJoinGrpRows* buildGrp = taosArrayGet(cache->grps, cache->grpIdx);
        MJ_ERR_RET(mJoinMergeGrpCart(pCtx->pJoin, pCtx->finBlk, true, probeGrp, buildGrp));
        buildGrp->readIdx = buildGrp->beginIdx;
      }

      pCtx->grpRemains = false;
      return TSDB_CODE_SUCCESS;
    }
  }

  for (; !GRP_DONE(probeGrp); ) {
    probeGrp->endIdx = probeGrp->readIdx;
    for (; cache->grpIdx < buildGrpNum && rowsLeft > 0; ++cache->grpIdx) {
      SMJoinGrpRows* buildGrp = taosArrayGet(cache->grps, cache->grpIdx);

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

    if (cache->grpIdx >= buildGrpNum) {
      cache->grpIdx = 0;
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



int32_t mAsofGreaterChkFillGrpCache(SMJoinWindowCtx* pCtx) {
  if (pCtx->cache.rowNum >= pCtx->jLimit || pCtx->pJoin->build->dsFetchDone) {
    return TSDB_CODE_SUCCESS;
  }

  SMJoinTableCtx* build = pCtx->pJoin->build;
  SMJoinWinCache* pCache = &pCtx->cache;
  int32_t grpNum = taosArrayGetSize(pCache->grps);
  ASSERT(grpNum >= 1 && grpNum <= 2);

  SSDataBlock* pBlk = NULL;
  SMJoinGrpRows* pGrp = taosArrayGet(pCache->grps, grpNum - 1);
  if (pGrp->blk != pCache->outBlk) {
    pBlk = pGrp->blk;
    taosArrayPop(pCache->grps);
  }
  
  do {
    if (NULL != pBlk) {
      MJ_ERR_RET(blockDataMergeNRows(pCache->outBlk, pBlk, build->blkRowIdx, pBlk->info.rows - build->blkRowIdx));
    }
    
    build->blk = getNextBlockFromDownstreamRemain(pCtx->pJoin->pOperator, build->downStreamIdx);
    qDebug("%s merge join %s table got block to fill grp, rows:%" PRId64, GET_TASKID(pCtx->pJoin->pOperator->pTaskInfo), MJOIN_TBTYPE(build->type), build->blk ? build->blk->info.rows : 0);
    
    build->blkRowIdx = 0;
    
    if (NULL == build->blk) {
      build->dsFetchDone = true;
      break;
    }

    MJOIN_PUSH_BLK_TO_CACHE(pCache, build->blk);
    pBlk = build->blk;
  } while (pCache->rowNum < pCtx->jLimit);

  MJOIN_RESTORE_TB_BLK(pCache, build);

  return TSDB_CODE_SUCCESS;
}

int32_t mAsofGreaterFillDumpGrpCache(SMJoinWindowCtx* pCtx, bool lastBuildGrp) {
  if (!lastBuildGrp) {
    MJ_ERR_RET(mAsofGreaterChkFillGrpCache(pCtx));
  }
  
  return mAsofGreaterDumpGrpCache(pCtx);
}

int32_t mAsofGreaterSkipEqRows(SMJoinTableCtx* pTable, int64_t timestamp, bool* wholeBlk) {
  SColumnInfoData* pCol = taosArrayGet(pTable->blk->pDataBlock, pTable->primCol->srcSlot);
  
  if (*(int64_t*)colDataGetNumData(pCol, pTable->blkRowIdx) != timestamp) {
    *wholeBlk = false;
    return TSDB_CODE_SUCCESS;
  }
  
  char* pEndVal = colDataGetNumData(pCol, pTable->blk->info.rows - 1);
  if (timestamp != *(int64_t*)pEndVal) {
    for (; pTable->blkRowIdx < pTable->blk->info.rows; ++pTable->blkRowIdx) {
      char* pNextVal = colDataGetNumData(pCol, pTable->blkRowIdx);
      if (timestamp == *(int64_t*)pNextVal) {
        continue;
      }
  
      return TSDB_CODE_SUCCESS;
    }
  }

  *wholeBlk = true;
  
  return TSDB_CODE_SUCCESS;
}

int32_t mAsofGreaterSkipAllEqRows(SMJoinWindowCtx* pCtx, int64_t timestamp) {
  SMJoinWinCache* cache = &pCtx->cache;
  int32_t grpNum = taosArrayGetSize(cache->grps);
  SMJoinTableCtx* pTable = pCtx->pJoin->build;
  bool wholeBlk = false;

  do {
    do {
      MJ_ERR_RET(mAsofGreaterSkipEqRows(pTable, timestamp, &wholeBlk));
      if (!wholeBlk) {
        return TSDB_CODE_SUCCESS;
      }

      MJOIN_POP_TB_BLK(cache);
      MJOIN_RESTORE_TB_BLK(cache, pTable);
    } while (!MJOIN_BUILD_TB_ROWS_DONE(pTable));
    
    pTable->blk = getNextBlockFromDownstreamRemain(pCtx->pJoin->pOperator, pTable->downStreamIdx);
    qDebug("%s merge join %s table got block to skip eq ts, rows:%" PRId64, GET_TASKID(pCtx->pJoin->pOperator->pTaskInfo), MJOIN_TBTYPE(pTable->type), pTable->blk ? pTable->blk->info.rows : 0);

    pTable->blkRowIdx = 0;

    if (NULL == pTable->blk) {
      pTable->dsFetchDone = true;
      return TSDB_CODE_SUCCESS;
    }

    MJOIN_PUSH_BLK_TO_CACHE(cache, pTable->blk);
  } while (true);

  return TSDB_CODE_SUCCESS;
}


int32_t mAsofGreaterUpdateDumpEqRows(SMJoinWindowCtx* pCtx, int64_t timestamp, bool lastBuildGrp) {
  if (!pCtx->asofEqRow && !lastBuildGrp) {
    MJ_ERR_RET(mAsofGreaterSkipAllEqRows(pCtx, timestamp));
  }

  return mAsofGreaterFillDumpGrpCache(pCtx, lastBuildGrp);
}


int32_t mAsofGreaterProcessEqualGrp(SMJoinWindowCtx* pCtx, int64_t timestamp, bool lastBuildGrp) {
  SMJoinOperatorInfo* pJoin = pCtx->pJoin;

  pCtx->lastEqGrp = true;
  pCtx->cache.grpIdx = 0;

  MJ_ERR_RET(mAsofJoinBuildEqGrp(pJoin->probe, timestamp, NULL, &pCtx->probeGrp));

  return mAsofGreaterUpdateDumpEqRows(pCtx, timestamp, lastBuildGrp);
}

int32_t mAsofGreaterProcessLowerGrp(SMJoinWindowCtx* pCtx, SMJoinOperatorInfo* pJoin, SColumnInfoData* pCol,  int64_t* probeTs, int64_t* buildTs) {
  pCtx->lastEqGrp = false;
  
  pCtx->probeGrp.beginIdx = pJoin->probe->blkRowIdx;
  pCtx->probeGrp.readIdx = pCtx->probeGrp.beginIdx;
  pCtx->probeGrp.endIdx = pCtx->probeGrp.beginIdx;
  
  while (++pJoin->probe->blkRowIdx < pJoin->probe->blk->info.rows) {
    MJOIN_GET_TB_CUR_TS(pCol, *probeTs, pJoin->probe);
    if (PROBE_TS_LOWER(pCtx->ascTs, *probeTs, *buildTs)) {
      pCtx->probeGrp.endIdx = pJoin->probe->blkRowIdx;
      continue;
    }
    
    break;
  }

  pCtx->cache.grpIdx = 0;

  return mAsofGreaterFillDumpGrpCache(pCtx, false);
}

int32_t mAsofGreaterProcessGreaterGrp(SMJoinWindowCtx* pCtx, SMJoinOperatorInfo* pJoin, SColumnInfoData* pCol,  int64_t* probeTs, int64_t* buildTs) {
  do {
    MJOIN_GET_TB_CUR_TS(pCol, *buildTs, pJoin->build);
    if (!PROBE_TS_GREATER(pCtx->ascTs, *probeTs, *buildTs)) {
      break;
    }

    pCtx->cache.rowNum--;
    while (++pJoin->build->blkRowIdx < pJoin->build->blk->info.rows) {
      MJOIN_GET_TB_CUR_TS(pCol, *buildTs, pJoin->build);
      if (PROBE_TS_GREATER(pCtx->ascTs, *probeTs, *buildTs)) {
        pCtx->cache.rowNum--;
        continue;
      }
      
      return TSDB_CODE_SUCCESS;
    }

    MJOIN_POP_TB_BLK(&pCtx->cache);
    MJOIN_RESTORE_TB_BLK(&pCtx->cache, pJoin->build);
  } while (!MJOIN_BUILD_TB_ROWS_DONE(pJoin->build));

  return TSDB_CODE_SUCCESS;
}

static bool mAsofGreaterRetrieve(SOperatorInfo* pOperator, SMJoinOperatorInfo* pJoin, SMJoinWindowCtx* pCtx) {
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

    if (buildGot && pCtx->asofGreaterRow) {
      SColumnInfoData* pProbeCol = taosArrayGet(pJoin->probe->blk->pDataBlock, pJoin->probe->primCol->srcSlot);
      SColumnInfoData* pBuildCol = taosArrayGet(pJoin->build->blk->pDataBlock, pJoin->build->primCol->srcSlot);
      if (*((int64_t*)pProbeCol->pData + pJoin->probe->blkRowIdx) > *((int64_t*)pBuildCol->pData + pJoin->build->blk->info.rows - 1)) {
        pJoin->build->blkRowIdx = pJoin->build->blk->info.rows;
        continue;
      }
    }
    
    break;
  } while (true);

  if (buildGot) {
    MJOIN_PUSH_BLK_TO_CACHE(&pCtx->cache, pJoin->build->blk);
    MJOIN_RESTORE_TB_BLK(&pCtx->cache, pJoin->build);
  }

  pCtx->probeGrp.blk = pJoin->probe->blk;
  pCtx->buildGrp.blk = pJoin->build->blk;

  return true;
}


int32_t mAsofGreaterHandleGrpRemains(SMJoinWindowCtx* pCtx) {
  return mAsofGreaterDumpGrpCache(pCtx);
}


SSDataBlock* mAsofGreaterJoinDo(struct SOperatorInfo* pOperator) {
  SMJoinOperatorInfo* pJoin = pOperator->info;
  SMJoinWindowCtx* pCtx = &pJoin->ctx.windowCtx;
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t probeTs = 0;
  int64_t buildTs = 0;
  SColumnInfoData* pBuildCol = NULL;
  SColumnInfoData* pProbeCol = NULL;

  blockDataCleanup(pCtx->finBlk);

  if (pCtx->grpRemains) {
    MJ_ERR_JRET(mAsofGreaterHandleGrpRemains(pCtx));
    if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
      return pCtx->finBlk;
    }
    pCtx->grpRemains = false;
  }

  do {
    if (!mAsofGreaterRetrieve(pOperator, pJoin, pCtx)) {
      break;
    }

    MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
    MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
    
    if (probeTs == pCtx->lastTs) {
      MJ_ERR_JRET(mAsofGreaterProcessEqualGrp(pCtx, probeTs, true));
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
        pCtx->lastTs = probeTs;
        MJ_ERR_JRET(mAsofGreaterProcessEqualGrp(pCtx, probeTs, false));
        if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
          return pCtx->finBlk;
        }

        MJOIN_GET_TB_COL_TS(pBuildCol, buildTs, pJoin->build);
        MJOIN_GET_TB_COL_TS(pProbeCol, probeTs, pJoin->probe);
        continue;
      }

      if (PROBE_TS_LOWER(pCtx->ascTs, probeTs, buildTs)) {
        MJ_ERR_JRET(mAsofGreaterProcessLowerGrp(pCtx, pJoin, pProbeCol, &probeTs, &buildTs));
      } else {
        MJ_ERR_JRET(mAsofGreaterProcessGreaterGrp(pCtx, pJoin, pBuildCol, &probeTs, &buildTs));
      }

      if (pCtx->finBlk->info.rows >= pCtx->blkThreshold) {
        return pCtx->finBlk;
      }
    }

    if (!MJOIN_PROBE_TB_ROWS_DONE(pJoin->probe) && pJoin->build->dsFetchDone) {
      pCtx->probeGrp.beginIdx = pJoin->probe->blkRowIdx;
      pCtx->probeGrp.readIdx = pCtx->probeGrp.beginIdx;
      pCtx->probeGrp.endIdx = pJoin->probe->blk->info.rows - 1;
      
      MJ_ERR_JRET(mJoinNonEqCart((SMJoinCommonCtx*)pCtx, &pCtx->probeGrp, true));
      
      pJoin->probe->blkRowIdx = pJoin->probe->blk->info.rows;
            
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


int32_t mJoinInitWinCache(SMJoinWinCache* pCache, SMJoinOperatorInfo* pJoin, SMJoinWindowCtx* pCtx) {
  pCache->pageLimit = MJOIN_BLK_SIZE_LIMIT;

  pCache->colNum = pJoin->build->finNum;
  pCache->outBlk = createOneDataBlock(pCtx->finBlk, false);
  if (NULL == pCache->outBlk) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCache->outBlk->info.capacity = pCtx->jLimit;

  SMJoinTableCtx* build = pJoin->build;  
  for (int32_t i = 0; i < pCache->colNum; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pCache->outBlk->pDataBlock, build->finCols[i].dstSlot);
    doEnsureCapacity(pCol, NULL, pCtx->jLimit, false);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mJoinInitWindowCtx(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode) {
  SMJoinWindowCtx* pCtx = &pJoin->ctx.windowCtx;
  
  pCtx->pJoin = pJoin;
  pCtx->asofOpType = pJoinNode->asofOpType;
  pCtx->asofEqRow = ASOF_EQ_ROW_INCLUDED(pCtx->asofOpType);
  pCtx->asofLowerRow = ASOF_LOWER_ROW_INCLUDED(pCtx->asofOpType);
  pCtx->asofGreaterRow = ASOF_GREATER_ROW_INCLUDED(pCtx->asofOpType);
  pCtx->jLimit = pJoinNode->pJLimit ? ((SLimitNode*)pJoinNode->pJLimit)->limit : 1;

  if (pCtx->asofLowerRow) {
    pJoin->joinFp = mAsofLowerJoinDo;
  } else if (pCtx->asofGreaterRow) {
    pJoin->joinFp = mAsofGreaterJoinDo;
  }

  if (pJoinNode->node.inputTsOrder != ORDER_DESC) {
    pCtx->ascTs = true;
  }

  pCtx->finBlk = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);
  blockDataEnsureCapacity(pCtx->finBlk, TMAX(MJOIN_DEFAULT_BLK_ROWS_NUM, MJOIN_BLK_SIZE_LIMIT/pJoinNode->node.pOutputDataBlockDesc->totalRowSize));

  pCtx->blkThreshold = pCtx->finBlk->info.capacity * 0.9;

  MJ_ERR_RET(mJoinInitWinCache(&pCtx->cache, pJoin, pCtx));
  
  return TSDB_CODE_SUCCESS;
}

int32_t mJoinInitMergeCtx(SMJoinOperatorInfo* pJoin, SSortMergeJoinPhysiNode* pJoinNode) {
  SMJoinMergeCtx* pCtx = &pJoin->ctx.mergeCtx;

  pCtx->pJoin = pJoin;
  pCtx->lastEqTs = INT64_MIN;
  pCtx->hashCan = pJoin->probe->keyNum > 0;
  if (JOIN_STYPE_ASOF == pJoin->subType) {
    pCtx->jLimit = pJoinNode->pJLimit ? ((SLimitNode*)pJoinNode->pJLimit)->limit : 1;
    pJoin->subType = JOIN_STYPE_OUTER;
  } else {
    pCtx->jLimit = -1;
  }
    
  if (pJoinNode->node.inputTsOrder != ORDER_DESC) {
    pCtx->ascTs = true;
  }

  pCtx->finBlk = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);
  blockDataEnsureCapacity(pCtx->finBlk, TMAX(MJOIN_DEFAULT_BLK_ROWS_NUM, MJOIN_BLK_SIZE_LIMIT/pJoinNode->node.pOutputDataBlockDesc->totalRowSize));

  if (pJoin->pFPreFilter) {
    pCtx->midBlk = createOneDataBlock(pCtx->finBlk, false);
    blockDataEnsureCapacity(pCtx->midBlk, pCtx->finBlk->info.capacity);
  }

  pCtx->blkThreshold = pCtx->finBlk->info.capacity * 0.9;

  switch (pJoin->joinType) {
    case JOIN_TYPE_INNER:
      pCtx->hashCartFp = (joinCartFp)mInnerJoinHashCart;
      pCtx->mergeCartFp = (joinCartFp)mInnerJoinMergeCart;
      break;
    case JOIN_TYPE_LEFT:
    case JOIN_TYPE_RIGHT: {
      switch (pJoin->subType) {
        case JOIN_STYPE_OUTER:          
          pCtx->hashCartFp = (joinCartFp)mLeftJoinHashCart;
          pCtx->mergeCartFp = (joinCartFp)mLeftJoinMergeCart;
          break;
        case JOIN_STYPE_SEMI: 
          pCtx->hashCartFp = (joinCartFp)mSemiJoinHashCart;
          pCtx->mergeCartFp = (joinCartFp)mSemiJoinMergeCart;
          break;
        case JOIN_STYPE_ANTI:
          pCtx->hashCartFp = (joinCartFp)mAntiJoinHashCart;
          pCtx->mergeCartFp = (joinCartFp)mAntiJoinMergeCart;
          break;
        default:
          break;
      }
      break;
    }
    case JOIN_TYPE_FULL:
      pCtx->hashCartFp = (joinCartFp)mFullJoinHashCart;
      pCtx->mergeCartFp = (joinCartFp)mFullJoinMergeCart;
      break;
    default:
      break;
  }
  
  return TSDB_CODE_SUCCESS;
}


