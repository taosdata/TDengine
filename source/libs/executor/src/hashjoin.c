/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "hashjoin.h"



int32_t hInnerJoinDo(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  SHJoinCtx* pCtx = &pJoin->ctx;
  SSDataBlock* pRes = pJoin->finBlk;
  size_t bufLen = 0;
  int32_t code = 0;
  bool allFetched = false;

  if (pJoin->ctx.pBuildRow) {
    HJ_ERR_RET(hJoinAppendResToBlock(pOperator, pRes, &allFetched));
    if (pRes->info.rows >= pRes->info.capacity) {
      if (allFetched) {
        ++pCtx->probeStartIdx;
      }
      
      return code;
    } else {
      ++pCtx->probeStartIdx;
    }
  }

  for (; pCtx->probeStartIdx <= pCtx->probeEndIdx; ++pCtx->probeStartIdx) {
    bool nullKey = false;
    HJ_ERR_RET(hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen, &nullKey));
    if (nullKey) {
      continue;
    }
    
    SGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pProbe->keyData, bufLen);
/*
    size_t keySize = 0;
    int32_t* pKey = tSimpleHashGetKey(pGroup, &keySize);
    A S S E R T(keySize == bufLen && 0 == memcmp(pKey, pProbe->keyData, bufLen));
    int64_t rows = getSingleKeyRowsNum(pGroup->rows);
    pJoin->execInfo.expectRows += rows;    
    qTrace("hash_key:%d, rows:%" PRId64, *pKey, rows);
*/
    if (pGroup) {
      pCtx->pBuildRow = pGroup->rows;
      HJ_ERR_RET(hJoinAppendResToBlock(pOperator, pRes, &allFetched));
      if (pRes->info.rows >= pRes->info.capacity) {
        if (allFetched) {
          ++pCtx->probeStartIdx;
        }
        
        return code;
      }
    }
  }

  pCtx->rowRemains = false;

  return code;
}

static int32_t hJoinHasPreFilterMatch(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, SGroupData* pGroup,
                                      bool* pMatched) {
  SHJoinCtx* pCtx = &pJoin->ctx;
  bool       allFetched = false;

  *pMatched = false;
  pCtx->pBuildRow = pGroup->rows;

  while (!allFetched) {
    blockDataCleanup(pJoin->midBlk);
    HJ_ERR_RET(hJoinAppendResToBlock(pOperator, pJoin->midBlk, &allFetched));

    if (pJoin->midBlk->info.rows > 0) {
      HJ_ERR_RET(doFilter(pJoin->midBlk, pJoin->pPreFilter, NULL, NULL));
      if (pJoin->midBlk->info.rows > 0) {
        *pMatched = true;
        break;
      }
    }
  }

  pCtx->pBuildRow = NULL;
  blockDataCleanup(pJoin->midBlk);

  return TSDB_CODE_SUCCESS;
}

int32_t hSemiJoinDo(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinTableCtx*     pProbe = pJoin->pProbe;
  SHJoinCtx*          pCtx = &pJoin->ctx;
  size_t              bufLen = 0;

  // Rows before the time range cannot match, but the current interval can.
  if (E_JOIN_PHASE_PRE == pCtx->probePhase) {
    pCtx->probePhase = E_JOIN_PHASE_CUR;
  }
  if (E_JOIN_PHASE_CUR != pCtx->probePhase) {
    pCtx->rowRemains = false;
    return TSDB_CODE_SUCCESS;
  }

  for (; pCtx->probeStartIdx <= pCtx->probeEndIdx; ++pCtx->probeStartIdx) {
    bool nullKey = false;
    HJ_ERR_RET(hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen, &nullKey));
    if (nullKey) {
      continue;
    }

    SGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pProbe->keyData, bufLen);
    if (NULL == pGroup) {
      continue;
    }

    bool matched = false;
    if (pJoin->pPreFilter == NULL) {
      // SEMI JOIN emits one row per probe row, but projected build columns
      // must come from an actual matching build row. The NMatch helper is
      // only for NULL-padded unmatched rows.
      pCtx->pBuildRow = pGroup->rows;
      bool allFetched = false;
      int32_t rowsBefore = pJoin->finBlk->info.rows;
      HJ_ERR_RET(hJoinAppendResRowsToBlock(pOperator, pJoin->finBlk, 1, NULL, &allFetched));
      matched = pJoin->finBlk->info.rows > rowsBefore;
    } else {
      // Find one actual build row satisfying the residual ON condition.
      pCtx->pBuildRow = pGroup->rows;
      while (pCtx->pBuildRow != NULL && !matched) {
        SBufRowInfo* pCandidate = pCtx->pBuildRow;
        bool allFetched = false;
        blockDataCleanup(pJoin->midBlk);
        HJ_ERR_RET(hJoinAppendResRowsToBlock(pOperator, pJoin->midBlk, 1, NULL, &allFetched));

        if (pJoin->midBlk->info.rows > 0) {
          HJ_ERR_RET(doFilter(pJoin->midBlk, pJoin->pPreFilter, NULL, NULL));
          if (pJoin->midBlk->info.rows > 0) {
            pCtx->pBuildRow = pCandidate;
            HJ_ERR_RET(hJoinAppendResRowsToBlock(pOperator, pJoin->finBlk, 1, NULL, &allFetched));
            matched = true;
          }
        }
      }
      blockDataCleanup(pJoin->midBlk);
    }
    // SEMI JOIN is done with this build group after finding one match.
    pCtx->pBuildRow = NULL;
    if (matched && hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
      ++pCtx->probeStartIdx;
      return TSDB_CODE_SUCCESS;
    }
  }

  pCtx->rowRemains = false;
  return TSDB_CODE_SUCCESS;
}

int32_t hAntiJoinDo(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinTableCtx*     pProbe = pJoin->pProbe;
  SHJoinCtx*          pCtx = &pJoin->ctx;
  size_t              bufLen = 0;

  if (E_JOIN_PHASE_PRE == pCtx->probePhase) {
    int32_t rows = pCtx->probeStartIdx - pCtx->probePreIdx;
    int32_t rowsLeft = pJoin->finBlk->info.capacity - pJoin->finBlk->info.rows;
    int32_t rowsToCopy = TMIN(rows, rowsLeft);
    HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probePreIdx, rowsToCopy));
    pCtx->probePreIdx += rowsToCopy;
    if (rowsToCopy < rows) {
      return TSDB_CODE_SUCCESS;
    }
    pCtx->probePhase = E_JOIN_PHASE_CUR;
  }

  if (E_JOIN_PHASE_CUR == pCtx->probePhase) {
    for (; pCtx->probeStartIdx <= pCtx->probeEndIdx; ++pCtx->probeStartIdx) {
      bool matched = false;
      bool nullKey = false;
      HJ_ERR_RET(hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen, &nullKey));
      if (!nullKey) {
        SGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pProbe->keyData, bufLen);
        if (pGroup != NULL) {
          matched = true;
          if (pJoin->pPreFilter != NULL) {
            HJ_ERR_RET(hJoinHasPreFilterMatch(pOperator, pJoin, pGroup, &matched));
          }
        }
      }

      if (!matched) {
        HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
        if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
          ++pCtx->probeStartIdx;
          return TSDB_CODE_SUCCESS;
        }
      }
    }

    pCtx->probePhase = E_JOIN_PHASE_POST;
  }

  if (E_JOIN_PHASE_POST == pCtx->probePhase) {
    if (pCtx->probeEndIdx < (pCtx->pProbeData->info.rows - 1) && pCtx->probePostIdx <= (pCtx->pProbeData->info.rows - 1)) {
      int32_t rows = pCtx->pProbeData->info.rows - pCtx->probePostIdx;
      int32_t rowsLeft = pJoin->finBlk->info.capacity - pJoin->finBlk->info.rows;
      int32_t rowsToCopy = TMIN(rows, rowsLeft);
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probePostIdx, rowsToCopy));
      pCtx->probePostIdx += rowsToCopy;
      if (rowsToCopy < rows) {
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  pCtx->rowRemains = false;
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void hFullJoinMarkGroupMatched(SGroupData* pGroup) {
  if (pGroup != NULL) {
    pGroup->rowsMatchNum = pGroup->rowsNum;
  }
}

static int32_t hFullJoinMarkBuildRowMatched(SGroupData* pGroup, const SBufRowInfo* pRow) {
  if (NULL == pGroup || NULL == pRow) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pGroup->rowsMatchNum >= pGroup->rowsNum) {
    return TSDB_CODE_SUCCESS;
  }

  if (pRow->grpRowIdx >= pGroup->rowsNum) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  if (pGroup->bitmap == NULL) {
    pGroup->bitmap = taosMemoryCalloc(1, BitmapLen(pGroup->rowsNum));
    if (NULL == pGroup->bitmap) {
      return terrno;
    }
  }

  if (!BMIsNull(pGroup->bitmap, pRow->grpRowIdx)) {
    colDataSetNull_f(pGroup->bitmap, pRow->grpRowIdx);
    ++pGroup->rowsMatchNum;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t hFullJoinHandleSeqRowRemains(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  SHJoinCtx* pCtx = &pJoin->ctx;
  SGroupData* pGroup = (SGroupData*)pCtx->pBuildNMatchGrp;
  SBufRowInfo* pStart = NULL;
  bool allFetched = false;

  HJ_ERR_RET(hJoinAppendResRowsToBlock(pOperator, pJoin->midBlk, 1, &pStart, &allFetched));
  if (pJoin->midBlk->info.rows > 0) {
    HJ_ERR_RET(doFilter(pJoin->midBlk, pJoin->pPreFilter, NULL, NULL));
    if (pJoin->midBlk->info.rows > 0) {
      pCtx->readMatch = true;
      if (pStart != NULL) {
        HJ_ERR_RET(hFullJoinMarkBuildRowMatched(pGroup, pStart));
      }
      HJ_ERR_RET(hJoinCopyMergeMidBlk(pJoin, &pJoin->midBlk, &pJoin->finBlk));

      if (pCtx->midRemains) {
        if (allFetched) {
          ++pCtx->probeStartIdx;
        }

        *loopCont = false;
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  if (allFetched && !pCtx->readMatch) {
    HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
  }

  if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
    if (allFetched) {
      ++pCtx->probeStartIdx;
    }

    *loopCont = false;
    return TSDB_CODE_SUCCESS;
  }

  if (allFetched) {
    ++pCtx->probeStartIdx;
    pCtx->pBuildNMatchGrp = NULL;
  }
  *loopCont = !allFetched;

  return TSDB_CODE_SUCCESS;
}

int32_t hFullJoinHandleSeqProbeRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  SHJoinCtx* pCtx = &pJoin->ctx;
  size_t bufLen = 0;
  bool allFetched = false;

  if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
    return TSDB_CODE_SUCCESS;
  }

  for (; pCtx->probeStartIdx <= pCtx->probeEndIdx; ++pCtx->probeStartIdx) {
    bool nullKey = false;
    HJ_ERR_RET(hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen, &nullKey));
    if (nullKey) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        *loopCont = false;
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }

    SGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pProbe->keyData, bufLen);
    if (NULL == pGroup) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        *loopCont = false;
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }

    pCtx->readMatch = false;
    pCtx->pBuildNMatchGrp = pGroup;
    pCtx->pBuildRow = pGroup->rows;
    allFetched = false;

    while (!allFetched) {
      SBufRowInfo* pStart = NULL;
      blockDataCleanup(pJoin->midBlk);
      HJ_ERR_RET(hJoinAppendResRowsToBlock(pOperator, pJoin->midBlk, 1, &pStart, &allFetched));
      if (pJoin->midBlk->info.rows > 0) {
        HJ_ERR_RET(doFilter(pJoin->midBlk, pJoin->pPreFilter, NULL, NULL));
        if (pJoin->midBlk->info.rows > 0) {
          pCtx->readMatch = true;
          if (pStart != NULL) {
            HJ_ERR_RET(hFullJoinMarkBuildRowMatched(pGroup, pStart));
          }
          HJ_ERR_RET(hJoinCopyMergeMidBlk(pJoin, &pJoin->midBlk, &pJoin->finBlk));

          if (pCtx->midRemains) {
            if (allFetched) {
              ++pCtx->probeStartIdx;
            }

            *loopCont = false;
            return TSDB_CODE_SUCCESS;
          }
        }
      }

      if (allFetched && !pCtx->readMatch) {
        HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      }

      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        if (allFetched) {
          ++pCtx->probeStartIdx;
        }

        *loopCont = false;
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  pCtx->probePhase = E_JOIN_PHASE_POST;
  pCtx->pBuildNMatchGrp = NULL;
  *loopCont = true;

  return TSDB_CODE_SUCCESS;
}

int32_t hFullJoinHandleRowRemains(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  bool allFetched = false;
  SHJoinCtx* pCtx = &pJoin->ctx;

  HJ_ERR_RET(hJoinAppendResToBlock(pOperator, pJoin->finBlk, &allFetched));
  if (allFetched) {
    hFullJoinMarkGroupMatched((SGroupData*)pCtx->pBuildNMatchGrp);
  }

  if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
    if (allFetched) {
      ++pCtx->probeStartIdx;
    }

    *loopCont = false;
    return TSDB_CODE_SUCCESS;
  } else {
    ++pCtx->probeStartIdx;
  }

  pCtx->pBuildNMatchGrp = NULL;
  *loopCont = true;

  return TSDB_CODE_SUCCESS;
}

int32_t hFullJoinHandleProbeRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  SHJoinCtx* pCtx = &pJoin->ctx;
  size_t bufLen = 0;
  bool allFetched = false;

  for (; pCtx->probeStartIdx <= pCtx->probeEndIdx; ++pCtx->probeStartIdx) {
    bool nullKey = false;
    HJ_ERR_RET(hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen, &nullKey));
    if (nullKey) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        *loopCont = false;
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }

    SGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pProbe->keyData, bufLen);
    if (NULL == pGroup) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        *loopCont = false;
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }

    pCtx->pBuildNMatchGrp = pGroup;
    pCtx->pBuildRow = pGroup->rows;
    HJ_ERR_RET(hJoinAppendResToBlock(pOperator, pJoin->finBlk, &allFetched));
    if (allFetched) {
      hFullJoinMarkGroupMatched(pGroup);
    }
    if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
      if (allFetched) {
        ++pCtx->probeStartIdx;
      }
      *loopCont = false;
      return TSDB_CODE_SUCCESS;
    }
  }

  pCtx->probePhase = E_JOIN_PHASE_POST;
  pCtx->pBuildNMatchGrp = NULL;
  *loopCont = true;

  return TSDB_CODE_SUCCESS;
}

int32_t hFullJoinDo(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinCtx* pCtx = &pJoin->ctx;

  while (pCtx->rowRemains) {
    switch (pCtx->probePhase) {
      case E_JOIN_PHASE_PRE: {
        int32_t rows = pCtx->probeStartIdx - pCtx->probePreIdx;
        int32_t rowsLeft = pJoin->finBlk->info.capacity - pJoin->finBlk->info.rows;
        int32_t rowsToCopy = TMIN(rows, rowsLeft);
        HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probePreIdx, rowsToCopy));
        pCtx->probePreIdx += rowsToCopy;
        if (rowsToCopy < rows) {
          return TSDB_CODE_SUCCESS;
        }
        pCtx->probePhase = E_JOIN_PHASE_CUR;
        break;
      }
      case E_JOIN_PHASE_CUR: {
        bool loopCont = false;
        if (NULL == pJoin->ctx.pBuildRow) {
          HJ_ERR_RET(pJoin->pPreFilter ? hFullJoinHandleSeqProbeRows(pOperator, pJoin, &loopCont)
                                       : hFullJoinHandleProbeRows(pOperator, pJoin, &loopCont));
        } else {
          HJ_ERR_RET(pJoin->pPreFilter ? hFullJoinHandleSeqRowRemains(pOperator, pJoin, &loopCont)
                                       : hFullJoinHandleRowRemains(pOperator, pJoin, &loopCont));
        }

        if (!loopCont) {
          return TSDB_CODE_SUCCESS;
        }
        break;
      }
      case E_JOIN_PHASE_POST: {
        if (pCtx->probeEndIdx < (pCtx->pProbeData->info.rows - 1) &&
            pCtx->probePostIdx <= (pCtx->pProbeData->info.rows - 1)) {
          int32_t rows = pCtx->pProbeData->info.rows - pCtx->probePostIdx;
          int32_t rowsLeft = pJoin->finBlk->info.capacity - pJoin->finBlk->info.rows;
          int32_t rowsToCopy = TMIN(rows, rowsLeft);
          HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probePostIdx, rowsToCopy));
          pCtx->probePostIdx += rowsToCopy;
          if (rowsToCopy < rows) {
            return TSDB_CODE_SUCCESS;
          }
        }
        pCtx->rowRemains = false;
        break;
      }
      default:
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t hLeftJoinHandleSeqRowRemains(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  bool allFetched = false;
  SHJoinCtx* pCtx = &pJoin->ctx;
  
  while (!allFetched) {
    HJ_ERR_RET(hJoinAppendResToBlock(pOperator, pJoin->midBlk, &allFetched));
    if (pJoin->midBlk->info.rows > 0) {
      HJ_ERR_RET(doFilterWithTaskCorrelation(pJoin->midBlk, pJoin->pPreFilter, NULL, NULL,
                                             pOperator->pTaskInfo));
      if (pJoin->midBlk->info.rows > 0) {
        pCtx->readMatch = true;
        HJ_ERR_RET(hJoinCopyMergeMidBlk(pJoin, &pJoin->midBlk, &pJoin->finBlk));
        
        if (pCtx->midRemains) {
          if (allFetched) {
            ++pCtx->probeStartIdx;
          }

          *loopCont = false;
          return TSDB_CODE_SUCCESS;
        }
      }
    }
  
    if (allFetched && !pCtx->readMatch) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
    }    
    
    if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
      if (allFetched) {
        ++pCtx->probeStartIdx;
      }

      *loopCont = false;      
      return TSDB_CODE_SUCCESS;
    }
  }
  
  ++pCtx->probeStartIdx;
  *loopCont = true;

  return TSDB_CODE_SUCCESS;
}

int32_t hLeftJoinHandleSeqProbeRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  SHJoinCtx* pCtx = &pJoin->ctx;
  size_t bufLen = 0;
  bool allFetched = false;

  if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
    return TSDB_CODE_SUCCESS;
  }

  for (; pCtx->probeStartIdx <= pCtx->probeEndIdx; ++pCtx->probeStartIdx) {
    bool nullKey = false;
    HJ_ERR_RET(hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen, &nullKey));
    if (nullKey) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        *loopCont = false;

        return TSDB_CODE_SUCCESS;
      }

      continue;
    }
    
    SGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pProbe->keyData, bufLen);
/*
    size_t keySize = 0;
    int32_t* pKey = tSimpleHashGetKey(pGroup, &keySize);
    A S S E R T(keySize == bufLen && 0 == memcmp(pKey, pProbe->keyData, bufLen));
    int64_t rows = getSingleKeyRowsNum(pGroup->rows);
    pJoin->execInfo.expectRows += rows;    
    qTrace("hash_key:%d, rows:%" PRId64, *pKey, rows);
*/

    if (NULL == pGroup) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        *loopCont = false;
        
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }
    
    pCtx->readMatch = false;
    pCtx->pBuildRow = pGroup->rows;
    allFetched = false;

    while (!allFetched) {
      HJ_ERR_RET(hJoinAppendResToBlock(pOperator, pJoin->midBlk, &allFetched));
      if (pJoin->midBlk->info.rows > 0) {
        HJ_ERR_RET(doFilterWithTaskCorrelation(pJoin->midBlk, pJoin->pPreFilter, NULL, NULL,
                                               pOperator->pTaskInfo));
        if (pJoin->midBlk->info.rows > 0) {
          pCtx->readMatch = true;
          HJ_ERR_RET(hJoinCopyMergeMidBlk(pJoin, &pJoin->midBlk, &pJoin->finBlk));
          
          if (pCtx->midRemains) {
            if (allFetched) {
              ++pCtx->probeStartIdx;
            }

            *loopCont = false;
            
            return TSDB_CODE_SUCCESS;
          }
        }
      }
      
      if (allFetched && !pCtx->readMatch) {
        HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      }    
      
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        if (allFetched) {
          ++pCtx->probeStartIdx;
        }

        *loopCont = false;
        
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  pCtx->probePhase = E_JOIN_PHASE_POST;
  *loopCont = true;

  return TSDB_CODE_SUCCESS;
}


int32_t hLeftJoinHandleRowRemains(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  bool allFetched = false;
  SHJoinCtx* pCtx = &pJoin->ctx;

  HJ_ERR_RET(hJoinAppendResToBlock(pOperator, pJoin->finBlk, &allFetched));

  if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
    if (allFetched) {
      ++pCtx->probeStartIdx;
    }

    *loopCont = false;
    return TSDB_CODE_SUCCESS;
  } else {
    ++pCtx->probeStartIdx;
  }

  *loopCont = true;

  return TSDB_CODE_SUCCESS;
}


int32_t hLeftJoinHandleProbeRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  SHJoinCtx* pCtx = &pJoin->ctx;
  size_t bufLen = 0;
  bool allFetched = false;

  for (; pCtx->probeStartIdx <= pCtx->probeEndIdx; ++pCtx->probeStartIdx) {
    bool nullKey = false;
    HJ_ERR_RET(hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen, &nullKey));
    if (nullKey) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        *loopCont = false;

        return TSDB_CODE_SUCCESS;
      }

      continue;
    }
    
    SGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pProbe->keyData, bufLen);
/*
    size_t keySize = 0;
    int32_t* pKey = tSimpleHashGetKey(pGroup, &keySize);
    A S S E R T(keySize == bufLen && 0 == memcmp(pKey, pProbe->keyData, bufLen));
    int64_t rows = getSingleKeyRowsNum(pGroup->rows);
    pJoin->execInfo.expectRows += rows;    
    qTrace("hash_key:%d, rows:%" PRId64, *pKey, rows);
*/

    if (NULL == pGroup) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        *loopCont = false;
        
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }
    
    pCtx->pBuildRow = pGroup->rows;

    HJ_ERR_RET(hJoinAppendResToBlock(pOperator, pJoin->finBlk, &allFetched));
    if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
      if (allFetched) {
        ++pCtx->probeStartIdx;
      }
      *loopCont = false;
      
      return TSDB_CODE_SUCCESS;
    }
  }

  pCtx->probePhase = E_JOIN_PHASE_POST;
  *loopCont = true;

  return TSDB_CODE_SUCCESS;
}



int32_t hLeftJoinDo(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinCtx* pCtx = &pJoin->ctx;

  while (pCtx->rowRemains) {
    switch (pCtx->probePhase) {
      case E_JOIN_PHASE_PRE: {
        int32_t rows = pCtx->probeStartIdx - pCtx->probePreIdx;
        int32_t rowsLeft = pJoin->finBlk->info.capacity - pJoin->finBlk->info.rows;
        if (rows <= rowsLeft) {
          HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, 0, rows));        
          pCtx->probePhase = E_JOIN_PHASE_CUR;
        } else {
          HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, 0, rowsLeft));
          pJoin->ctx.probePreIdx += rowsLeft;
          
          return TSDB_CODE_SUCCESS;
        }
        break;
      }
      case E_JOIN_PHASE_CUR: {
        bool loopCont = false;
        if (NULL == pJoin->ctx.pBuildRow) {
          HJ_ERR_RET(pJoin->pPreFilter ? hLeftJoinHandleSeqProbeRows(pOperator, pJoin, &loopCont) : hLeftJoinHandleProbeRows(pOperator, pJoin, &loopCont));
        } else {
          HJ_ERR_RET(pJoin->pPreFilter ? hLeftJoinHandleSeqRowRemains(pOperator, pJoin, &loopCont) : hLeftJoinHandleRowRemains(pOperator, pJoin, &loopCont));
        }

        if (!loopCont) {
          return TSDB_CODE_SUCCESS;
        }
        break;
      }
      case E_JOIN_PHASE_POST: {
        if (pCtx->probeEndIdx < (pCtx->pProbeData->info.rows - 1) && pCtx->probePostIdx <= (pCtx->pProbeData->info.rows - 1)) {
          int32_t rowsLeft = pJoin->finBlk->info.capacity - pJoin->finBlk->info.rows;
          int32_t rows = pCtx->pProbeData->info.rows - pCtx->probePostIdx;
          if (rows <= rowsLeft) {
            HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pJoin->ctx.probePostIdx, rows));
            pCtx->rowRemains = false;
          } else {
            HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pJoin->ctx.probePostIdx, rowsLeft));
            pCtx->probePostIdx += rowsLeft;
            
            return TSDB_CODE_SUCCESS;
          }
        } else {
          pJoin->ctx.rowRemains = false;
        }
        break;
      }
      default:
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
  }

  return TSDB_CODE_SUCCESS;
}
