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
    hJoinAppendResToBlock(pOperator, pRes, &allFetched);
    if (pRes->info.rows >= pRes->info.capacity) {
      if (allFetched) {
        ++pCtx->probeStartIdx;
      }

      if (pCtx->probeStartIdx <= pCtx->probeEndIdx) {
        pJoin->ctx.rowRemains = true;
      } 
      
      return code;
    } else {
      ++pCtx->probeStartIdx;
    }
  }

  for (; pCtx->probeStartIdx <= pCtx->probeEndIdx; ++pCtx->probeStartIdx) {
    if (hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen)) {
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
      hJoinAppendResToBlock(pOperator, pRes, &allFetched);
      if (pRes->info.rows >= pRes->info.capacity) {
        if (allFetched) {
          ++pCtx->probeStartIdx;
        }

        if (pCtx->probeStartIdx <= pCtx->probeEndIdx) {
          pJoin->ctx.rowRemains = true;
        }
        
        return code;
      }
    }
  }

  pCtx->rowRemains = false;

  return code;
}

int32_t hLeftJoinHandleSeqRemainBuildRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  bool allFetched = false;
  SHJoinCtx* pCtx = &pJoin->ctx;
  
  while (!allFetched) {
    hJoinAppendResToBlock(pOperator, pJoin->midBlk, &allFetched);
    if (pJoin->midBlk->info.rows > 0) {
      HJ_ERR_RET(doFilter(pJoin->midBlk, pJoin->pPreFilter, NULL));
      if (pJoin->midBlk->info.rows > 0) {
        pCtx->readMatch = true;
        HJ_ERR_RET(hJoinCopyMergeMidBlk(pCtx, &pJoin->midBlk, &pJoin->finBlk));
        
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
    if (hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen)) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        
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
        
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }
    
    pCtx->readMatch = false;
    pCtx->pBuildRow = pGroup->rows;
    allFetched = false;

    while (!allFetched) {
      hJoinAppendResToBlock(pOperator, pJoin->midBlk, &allFetched);
      if (pJoin->midBlk->info.rows > 0) {
        HJ_ERR_RET(doFilter(pJoin->midBlk, pJoin->pPreFilter, NULL));
        if (pJoin->midBlk->info.rows > 0) {
          pCtx->readMatch = true;
          HJ_ERR_RET(hJoinCopyMergeMidBlk(pCtx, &pJoin->midBlk, &pJoin->finBlk));
          
          if (pCtx->midRemains) {
            if (allFetched) {
              ++pCtx->probeStartIdx;
            }

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
        
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  pCtx->probePhase = E_JOIN_PHASE_POST;
  *loopCont = true;

  return TSDB_CODE_SUCCESS;
}


int32_t hLeftJoinHandleRemainBuildRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  bool allFetched = false;
  SHJoinCtx* pCtx = &pJoin->ctx;
  
  hJoinAppendResToBlock(pOperator, pJoin->finBlk, &allFetched);
  
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
    if (hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen)) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        
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
        
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }
    
    pCtx->pBuildRow = pGroup->rows;

    hJoinAppendResToBlock(pOperator, pJoin->finBlk, &allFetched);
    if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
      if (allFetched) {
        ++pCtx->probeStartIdx;
      }
      
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
          HJ_ERR_RET(pJoin->pPreFilter ? hLeftJoinHandleSeqRemainBuildRows(pOperator, pJoin, &loopCont) : hLeftJoinHandleRemainBuildRows(pOperator, pJoin, &loopCont));
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

int32_t hSemiJoinHandleSeqProbeRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin) {
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  SHJoinCtx* pCtx = &pJoin->ctx;
  size_t bufLen = 0;
  bool allFetched = false;

  if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
    goto _return;
  }

  for (; pCtx->probeStartIdx <= pCtx->probeEndIdx; ++pCtx->probeStartIdx) {
    if (hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen)) {
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
      continue;
    }
    
    pCtx->pBuildRow = pGroup->rows;
    allFetched = false;

    while (!allFetched) {
      hJoinAppendResToBlock(pOperator, pJoin->midBlk, &allFetched);
      if (pJoin->midBlk->info.rows > 0) {
        HJ_ERR_RET(mJoinFilterAndKeepSingleRow(pJoin->midBlk, pJoin->pPreFilter));
        if (pJoin->midBlk->info.rows > 0) {
          HJ_ERR_RET(hJoinCopyMergeMidBlk(pCtx, &pJoin->midBlk, &pJoin->finBlk));
          ASSERT(!pCtx->midRemains);
          pCtx->pBuildRow = NULL;
          break;
        }
      }
    }

    if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
      ++pCtx->probeStartIdx;
    
      goto _return;
    }
  }

_return:

  pCtx->rowRemains = (pCtx->probeStartIdx <= pCtx->probeEndIdx);

  return TSDB_CODE_SUCCESS;
}

int32_t hSemiJoinHandleProbeRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin) {
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  SHJoinCtx* pCtx = &pJoin->ctx;
  size_t bufLen = 0;
  bool allFetched = false;

  if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
    goto _return;
  }

  for (; pCtx->probeStartIdx <= pCtx->probeEndIdx; ++pCtx->probeStartIdx) {
    if (hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen)) {
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
      continue;
    }
    
    pCtx->pBuildRow = pGroup->rows;
    if (pCtx->pBuildRow->next) {
      qError("semi join got more than one row in group");
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    
    allFetched = false;
    hJoinAppendResToBlock(pOperator, pJoin->finBlk, &allFetched);

    if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
      ++pCtx->probeStartIdx;
    
      goto _return;
    }
  }

_return:

  pCtx->rowRemains = (pCtx->probeStartIdx <= pCtx->probeEndIdx);

  return TSDB_CODE_SUCCESS;
}


int32_t hSemiJoinDo(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinCtx* pCtx = &pJoin->ctx;

  HJ_ERR_RET(pJoin->pPreFilter ? hSemiJoinHandleSeqProbeRows(pOperator, pJoin) : hSemiJoinHandleProbeRows(pOperator, pJoin));

  return TSDB_CODE_SUCCESS;
}

int32_t hAntiJoinHandleSeqProbeRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  SHJoinCtx* pCtx = &pJoin->ctx;
  size_t bufLen = 0;
  bool allFetched = false;

  if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
    return TSDB_CODE_SUCCESS;
  }

  for (; pCtx->probeStartIdx <= pCtx->probeEndIdx; ++pCtx->probeStartIdx) {
    if (hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen)) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        
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
        
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }
    
    pCtx->pBuildRow = pGroup->rows;
    allFetched = false;

    while (!allFetched) {
      hJoinAppendResToBlock(pOperator, pJoin->midBlk, &allFetched);
      if (pJoin->midBlk->info.rows > 0) {
        HJ_ERR_RET(mJoinFilterAndNoKeepRows(pJoin->midBlk, pJoin->pPreFilter));
        if (pJoin->midBlk->info.rows > 0) {
          blockDataCleanup(pJoin->midBlk);
          pCtx->pBuildRow = NULL;
          break;
        }
      }

      if (!allFetched) {
        continue;
      }

      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
    }

    if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
      ++pCtx->probeStartIdx;
    
      return TSDB_CODE_SUCCESS;
    }
  }

_return:

  pCtx->probePhase = E_JOIN_PHASE_POST;
  *loopCont = true;

  return TSDB_CODE_SUCCESS;
}

int32_t hAntiJoinHandleProbeRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  SHJoinCtx* pCtx = &pJoin->ctx;
  size_t bufLen = 0;
  bool allFetched = false;

  if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
    return TSDB_CODE_SUCCESS;
  }

  for (; pCtx->probeStartIdx <= pCtx->probeEndIdx; ++pCtx->probeStartIdx) {
    if (hJoinCopyKeyColsDataToBuf(pProbe, pCtx->probeStartIdx, &bufLen)) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        
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

    if (NULL == pGroup || NULL == pGroup->rows) {
      HJ_ERR_RET(hJoinCopyNMatchRowsToBlock(pJoin, pJoin->finBlk, pCtx->probeStartIdx, 1));
      if (hJoinBlkReachThreshold(pJoin, pJoin->finBlk->info.rows)) {
        ++pCtx->probeStartIdx;
        
        return TSDB_CODE_SUCCESS;
      }
    }
  }

_return:

  pCtx->probePhase = E_JOIN_PHASE_POST;
  *loopCont = true;

  return TSDB_CODE_SUCCESS;
}


int32_t hAntiJoinDo(struct SOperatorInfo* pOperator) {
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
        HJ_ERR_RET(pJoin->pPreFilter ? hAntiJoinHandleSeqProbeRows(pOperator, pJoin, &loopCont) : hAntiJoinHandleProbeRows(pOperator, pJoin, &loopCont));

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


