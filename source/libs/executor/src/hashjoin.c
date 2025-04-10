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

static int32_t hFullJoinCopyBuildNMRowsToBlock(SHJoinOperatorInfo* pJoin, SSDataBlock* pRes, SHJoinCtx* pCtx, bool* returnDirect) {
  SHJoinTableCtx* pBuild = pJoin->pBuild;
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  int32_t code = 0;

  while (true) {
    int32_t rowsLeft = pRes->info.capacity - pRes->info.rows;
    int32_t rows = pCtx->buildNMEndIdx - pCtx->buildNMStartIdx + 1;
    int32_t copyRows = TMIN(rows, rowsLeft);
    int32_t buildIdx = 0;
    int32_t probeIdx = 0;

    for (int32_t i = 0; i < pJoin->pResColNum; ++i) {
      if (pJoin->pResColMap[i]) {
        SColumnInfoData* pSrc = taosArrayGet(pJoin->ctx.pBuildData->pDataBlock, pBuild->valCols[buildIdx].srcSlot);
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pBuild->valCols[buildIdx].dstSlot);

        QRY_ERR_RET(colDataAssignNRows(pDst, pRes->info.rows, pSrc, pCtx->buildNMStartIdx, copyRows));

        buildIdx++;
      } else {
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pProbe->valCols[probeIdx].dstSlot);
        colDataSetNItemsNull(pDst, pRes->info.rows, copyRows);

        probeIdx++;
      }
    }

    pRes->info.rows += copyRows;

    if (rows <= rowsLeft) {
      pCtx->buildNMStartIdx = -1;
      break;
    } else {
      if (pJoin->pFinFilter != NULL) {
        QRY_ERR_RET(doFilter(pRes, pJoin->pFinFilter, NULL));
      }

      if (pRes->info.rows > 0) {
        *returnDirect = true;
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t hFullJoinAddBlockRowsToHashImpl(SHJoinOperatorInfo* pJoin, SHJoinCtx* pCtx, SSDataBlock* pBlock, bool* returnDirect) {
  size_t bufLen = 0;
  int32_t code = TSDB_CODE_SUCCESS;

  for (; pCtx->buildStartIdx <= pCtx->buildEndIdx; ++pCtx->buildStartIdx) {
    if (hJoinCopyKeyColsDataToBuf(pJoin->pBuild, pCtx->buildStartIdx, &bufLen)) {
      if (pCtx->buildNMStartIdx < 0) {
        pCtx->buildNMStartIdx = pCtx->buildStartIdx;
      }
      
      continue;
    }

    if (pCtx->buildNMStartIdx >= 0) {
      pCtx->buildNMEndIdx = pCtx->buildStartIdx - 1;
      HJ_ERR_RET(hFullJoinCopyBuildNMRowsToBlock(pJoin, pJoin->finBlk, pCtx, returnDirect));
      if (*returnDirect) {
        return code;
      }
    }
    
    code = hJoinAddRowToHash(pJoin, pBlock, bufLen, pCtx->buildStartIdx);
    if (code) {
      return code;
    }
  }

  pCtx->buildStartIdx = -1;

  return TSDB_CODE_SUCCESS;
}


static int32_t hFullJoinAddBlockRowsToHash(SSDataBlock* pBlock, SHJoinOperatorInfo* pJoin, bool* returnDirect) {
  SHJoinTableCtx* pBuild = pJoin->pBuild;
  SHJoinCtx* pCtx = &pJoin->ctx;

  pCtx->pBuildData = pBlock;
  pCtx->buildNMStartIdx = -1;
  pCtx->buildStartIdx = -1;

  if (pBuild->hasTimeRange && !hJoinFilterTimeRange(pCtx, pBlock, &pJoin->tblTimeRange, pBuild->primCol->srcSlot, &pCtx->buildStartIdx, &pCtx->buildEndIdx)) {
    pCtx->buildNMStartIdx = 0;
    pCtx->buildNMEndIdx = pBlock->info.rows - 1;
    
    return hFullJoinCopyBuildNMRowsToBlock(pJoin, pJoin->finBlk, pCtx, returnDirect);
  }

  HJ_ERR_RET(hJoinLaunchEqualExpr(pBlock, pBuild, pCtx->buildStartIdx, pCtx->buildEndIdx));

  HJ_ERR_RET(hJoinSetKeyColsData(pBlock, pBuild));

  if (pCtx->buildStartIdx > 0) {
    pCtx->buildNMStartIdx = 0;  
  }

  return hFullJoinAddBlockRowsToHashImpl(pJoin, pCtx, pBlock, returnDirect);
}

int32_t hFullJoinHandleBuildRemains(SHJoinOperatorInfo* pJoin, bool* returnDirect) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  if (pJoin->ctx.pBuildData && pJoin->ctx.buildNMStartIdx >= 0) {
    code = hFullJoinCopyBuildNMRowsToBlock(pJoin, pJoin->finBlk, &pJoin->ctx, returnDirect);
    if (code || *returnDirect) {
      return code;
    }
  }

  if (pJoin->ctx.pBuildData && pJoin->ctx.buildStartIdx >= 0 && pJoin->ctx.buildStartIdx <= pJoin->ctx.buildEndIdx) {
    code = hFullJoinAddBlockRowsToHashImpl(pJoin, &pJoin->ctx, pJoin->ctx.pBuildData, returnDirect);
  }

  return code;
}

int32_t hFullJoinBuildHash(struct SOperatorInfo* pOperator, bool* returnDirect) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SSDataBlock*        pBlock = NULL;
  int32_t             code = hFullJoinHandleBuildRemains(pJoin, returnDirect);
  if (code || *returnDirect) {
    return code;
  }

  while (true) {
    pBlock = getNextBlockFromDownstream(pOperator, pJoin->pBuild->downStreamIdx);
    if (NULL == pBlock) {
      break;
    }

    pJoin->execInfo.buildBlkNum++;
    pJoin->execInfo.buildBlkRows += pBlock->info.rows;

    code = hFullJoinAddBlockRowsToHash(pBlock, pJoin, returnDirect);
    if (code || *returnDirect) {
      return code;
    }
  }

  if (tSimpleHashGetSize(pJoin->pKeyHash) <= 0) {
    tSimpleHashCleanup(pJoin->pKeyHash);
    pJoin->pKeyHash = NULL;
  }

  if (pJoin->finBlk->info.rows > 0 && pJoin->pFinFilter != NULL) {
    QRY_ERR_RET(doFilter(pJoin->finBlk, pJoin->pFinFilter, NULL));
  }
  
  if (pJoin->finBlk->info.rows > 0) {
    *returnDirect = true;
  }

#if 0  
  qTrace("build table rows:%" PRId64, hJoinGetRowsNumOfKeyHash(pJoin->pKeyHash));
#endif

  pJoin->keyHashBuilt = true;

  return TSDB_CODE_SUCCESS;
}


int32_t hFullJoinHandleSeqRemainBuildRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
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

int32_t hFullJoinHandleSeqProbeRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
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
    
    SFGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pProbe->keyData, bufLen);
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


int32_t hFullJoinHandleRemainBuildRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
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


int32_t hFullJoinHandleProbeRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
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
    
    SFGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pProbe->keyData, bufLen);
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
    pGroup->rowsMatchNum = pGroup->rowsNum;

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


int32_t hFullJoinDo(struct SOperatorInfo* pOperator) {
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
          HJ_ERR_RET(pJoin->pPreFilter ? hFullJoinHandleSeqProbeRows(pOperator, pJoin, &loopCont) : hFullJoinHandleProbeRows(pOperator, pJoin, &loopCont));
        } else {
          HJ_ERR_RET(pJoin->pPreFilter ? hFullJoinHandleSeqRemainBuildRows(pOperator, pJoin, &loopCont) : hFullJoinHandleRemainBuildRows(pOperator, pJoin, &loopCont));
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



