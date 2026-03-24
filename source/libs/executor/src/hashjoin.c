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

/*
 * Per-join-type execution logic for the hash join operator.
 *
 * Each h<Type>JoinDo function implements the probe-phase logic for a specific join type.
 * They iterate over probe rows, look up keys in the hash table, and emit result rows.
 *
 * For LEFT/ANTI/FULL joins, probe rows are divided into three phases (PRE/CUR/POST)
 * based on the time window. Rows outside the window are emitted as non-matching.
 *
 * When a pPreFilter exists (non-equi ON conditions), a two-block strategy is used:
 *   - Matched rows are first written to midBlk
 *   - pPreFilter is applied to midBlk
 *   - Surviving rows are merged into finBlk
 *   - If no rows survive and all build rows exhausted, a NULL-padded row is emitted
 *
 * "Seq" variants (e.g. hLeftJoinHandleSeqProbeRows) handle the two-block pPreFilter path.
 * Non-"Seq" variants handle the simpler direct-output path (no pPreFilter).
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


/*
 * INNER JOIN probe execution.
 * For each probe row, serializes the key, looks it up in the hash table,
 * and appends all matching build-side rows paired with the probe row to finBlk.
 * Rows with NULL keys are skipped (no match possible in equi-join).
 *
 * @param pOperator  the hash join operator
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 *
 * Side effect: sets ctx.rowRemains if the output block fills before all probe rows are processed.
 */
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

/*
 * Handles remaining build-side rows for LEFT JOIN when pPreFilter is active.
 * Continues appending build rows from the current linked list position to midBlk,
 * applies pPreFilter, merges survivors into finBlk. If no rows pass the filter after
 * exhausting all build rows, emits a NULL-padded non-match row for the current probe row.
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @param loopCont   output: true if caller should continue to next probe row, false to yield
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hLeftJoinHandleSeqRemainBuildRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  bool allFetched = false;
  SHJoinCtx* pCtx = &pJoin->ctx;
  
  while (!allFetched) {
    hJoinAppendResToBlock(pOperator, pJoin->midBlk, &allFetched);
    if (pJoin->midBlk->info.rows > 0) {
      HJ_ERR_RET(doFilter(pJoin->midBlk, pJoin->pPreFilter, NULL, NULL));
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

/*
 * Processes CUR-phase probe rows for LEFT JOIN when pPreFilter is active (two-block path).
 * For each probe row in [probeStartIdx, probeEndIdx]:
 *   - If key is NULL or has no hash match: emits a NULL-padded non-match row directly to finBlk.
 *   - If hash match found: writes all matching build rows to midBlk, applies pPreFilter,
 *     and merges survivors into finBlk. If no survivors after exhausting all build rows,
 *     emits a NULL-padded non-match row.
 *
 * When finBlk reaches the threshold, sets loopCont=false and returns so the caller can yield.
 * When all CUR-phase rows are processed, transitions probePhase to E_JOIN_PHASE_POST.
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @param loopCont   output: true if caller should continue to next phase, false to yield now
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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
        HJ_ERR_RET(doFilter(pJoin->midBlk, pJoin->pPreFilter, NULL, NULL));
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


/*
 * Continues emitting matched build rows for the current probe row (no pPreFilter path).
 * Called when ctx.pBuildRow is non-NULL, meaning the previous call to hLeftJoinHandleProbeRows
 * ran out of output capacity while traversing the row linked list.
 *
 * Appends more matched rows from pBuildRow into finBlk until the block threshold is reached
 * or the linked list is exhausted. On threshold: sets loopCont=false to yield. On completion:
 * advances probeStartIdx and sets loopCont=true to continue with the next probe row.
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @param loopCont   output: true if caller should continue to next probe row, false to yield
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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


/*
 * Processes CUR-phase probe rows for LEFT JOIN when pPreFilter is absent (direct output path).
 * For each probe row in [probeStartIdx, probeEndIdx]:
 *   - If key is NULL: emits a NULL-padded non-match row directly to finBlk.
 *   - If no hash match: emits a NULL-padded non-match row.
 *   - If hash match found: appends all matching build rows paired with the probe row to finBlk.
 *
 * When finBlk reaches the block threshold, returns immediately (rowRemains state preserved).
 * When all CUR-phase rows are processed, transitions probePhase to E_JOIN_PHASE_POST.
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @param loopCont   output: true if caller should continue to POST phase, false to yield
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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



/*
 * LEFT (OUTER) JOIN probe execution. Implements the three-phase PRE/CUR/POST state machine.
 *
 * PRE phase: rows before the join time window — emitted directly as non-matching (NULL build cols).
 * CUR phase: rows within the time window — dispatched to hLeftJoinHandleProbeRows or
 *            hLeftJoinHandleSeqProbeRows (with pPreFilter).
 * POST phase: rows after the join time window — emitted directly as non-matching.
 *
 * The outer while loop runs until all remaining state (rowRemains) is drained or the output
 * block threshold is reached. The caller is responsible for calling this function again if
 * ctx.rowRemains is still true after return.
 *
 * @param pOperator  the hash join operator
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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

/*
 * Processes probe rows for SEMI JOIN when pPreFilter is active (two-block path).
 * For each probe row with a hash match, writes all matching build rows to midBlk,
 * applies mJoinFilterAndKeepSingleRow to retain at most one matching row, then merges
 * the survivor into finBlk. The probe row is emitted at most once (SEMI semantics).
 * Probe rows with NULL keys or no hash match are silently skipped.
 *
 * When finBlk reaches the threshold, sets rowRemains and returns.
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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

/*
 * Processes probe rows for SEMI JOIN when pPreFilter is absent (grpSingleRow fast path).
 * For each probe row with a hash match, emits at most one result row (SEMI semantics).
 * Relies on the grpSingleRow invariant: when there is no ON condition, the build phase
 * inserts only the first row per key, so pBuildRow->next is guaranteed to be NULL.
 * An error is logged if that invariant is violated.
 * Probe rows with NULL keys or no hash match are skipped (no output).
 *
 * When finBlk reaches the threshold, sets rowRemains and returns.
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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


/*
 * SEMI JOIN probe execution. Dispatches to the appropriate probe handler based on whether
 * pPreFilter is present:
 *   - With pPreFilter:    hSemiJoinHandleSeqProbeRows (two-block pPreFilter path)
 *   - Without pPreFilter: hSemiJoinHandleProbeRows (grpSingleRow fast path)
 *
 * SEMI JOIN does not use the PRE/CUR/POST phase model because unmatched probe rows are
 * simply discarded (not emitted as non-matching).
 *
 * @param pOperator  the hash join operator
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hSemiJoinDo(struct SOperatorInfo* pOperator) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinCtx* pCtx = &pJoin->ctx;

  HJ_ERR_RET(pJoin->pPreFilter ? hSemiJoinHandleSeqProbeRows(pOperator, pJoin) : hSemiJoinHandleProbeRows(pOperator, pJoin));

  return TSDB_CODE_SUCCESS;
}

/*
 * Processes CUR-phase probe rows for ANTI JOIN when pPreFilter is active (two-block path).
 * For each probe row:
 *   - If key is NULL: emits a non-match row (ANTI semantics: NULL keys are never matched).
 *   - If no hash match: emits a non-match row.
 *   - If hash match found: writes build rows to midBlk, applies mJoinFilterAndNoKeepRows.
 *     If any row passes the filter, the probe row IS matched — skip it (don't emit).
 *     If no rows pass the filter after exhausting build rows, emit the probe row as non-matching.
 *
 * When finBlk reaches the threshold, returns. loopCont is set to true when all CUR-phase
 * rows are processed and the caller should transition to POST phase.
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @param loopCont   output: true if caller should continue to POST phase, false to yield
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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

/*
 * Processes CUR-phase probe rows for ANTI JOIN when pPreFilter is absent (grpSingleRow path).
 * For each probe row:
 *   - If key is NULL: emits a non-match row.
 *   - If no hash match (or group has no rows): emits a non-match row.
 *   - If hash match exists: probe row IS matched — skip it (don't emit).
 *
 * This path relies on the grpSingleRow invariant (build phase stores at most one row per key
 * when there is no ON condition). An empty group (pGroup->rows == NULL) is treated as no match.
 *
 * When finBlk reaches the threshold, returns. loopCont=true when transitioning to POST phase.
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @param loopCont   output: true if caller should continue to POST phase, false to yield
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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


/*
 * ANTI JOIN probe execution. Implements the three-phase PRE/CUR/POST state machine.
 *
 * PRE phase: rows before the join time window — emitted as non-matching (no build match possible).
 * CUR phase: dispatched to hAntiJoinHandleSeqProbeRows (with pPreFilter) or
 *            hAntiJoinHandleProbeRows (without pPreFilter).
 * POST phase: rows after the join time window — emitted as non-matching.
 *
 * The outer while loop runs until rowRemains is false or the output block threshold is reached.
 *
 * @param pOperator  the hash join operator
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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

/*
 * Copies build-side rows with NULL keys to the output block as non-matching rows
 * (FULL OUTER JOIN build phase). Rows from [buildNMStartIdx, buildNMEndIdx] are
 * emitted with build columns filled from the build block and probe columns set to NULL.
 *
 * Called during the FULL JOIN build phase to immediately output build rows that cannot
 * be keyed into the hash table (NULL keys have no equi-join match by definition).
 * If pFinFilter is present and the output block fills up, applies the filter and returns
 * with returnDirect=true so the caller can yield the block immediately.
 *
 * @param pJoin          hash join operator info
 * @param pRes           output block to append rows to
 * @param pCtx           join context (provides buildNMStartIdx, buildNMEndIdx, pBuildData)
 * @param returnDirect   output: true if the caller should yield pRes to upstream now
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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
        QRY_ERR_RET(doFilter(pRes, pJoin->pFinFilter, NULL, NULL));
      }

      if (pRes->info.rows > 0) {
        *returnDirect = true;
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

/*
 * Processes a range of build-side rows [buildStartIdx, buildEndIdx] for FULL JOIN hash building.
 * Interleaves NULL-key row emission (hFullJoinCopyBuildNMRowsToBlock) with hash insertion:
 *   - Consecutive NULL-key rows are accumulated and emitted as a batch.
 *   - Rows with valid keys are inserted into the hash table via hJoinAddRowToHash.
 *
 * If finBlk becomes full during NULL-key row emission, returnDirect is set and the function
 * returns early. The caller must resume via hFullJoinHandleBuildRemains.
 *
 * @param pJoin          hash join operator info
 * @param pCtx           join context (buildStartIdx, buildEndIdx, buildNMStartIdx, etc.)
 * @param pBlock         current build-side data block
 * @param returnDirect   output: true if finBlk is full and should be yielded immediately
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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

  if (pCtx->buildNMStartIdx >= 0) {
    pCtx->buildNMEndIdx = pCtx->buildEndIdx;
    HJ_ERR_RET(hFullJoinCopyBuildNMRowsToBlock(pJoin, pJoin->finBlk, pCtx, returnDirect));
    if (*returnDirect) {
      return code;
    }
  }
    
  return TSDB_CODE_SUCCESS;
}


/*
 * Processes a single build-side block for FULL JOIN: evaluates expressions, applies time range
 * filter, sets up key column data pointers, then calls hFullJoinAddBlockRowsToHashImpl.
 *
 * If all rows are outside the time range, all rows are treated as NULL-key non-matching rows
 * and emitted immediately via hFullJoinCopyBuildNMRowsToBlock.
 *
 * Note: hJoinLaunchEqualExpr is called before hJoinFilterTimeRange because the expression
 * may compute the key value that time range filtering depends on.
 *
 * @param pBlock         build-side data block to process
 * @param pJoin          hash join operator info
 * @param returnDirect   output: true if finBlk filled up and should be yielded immediately
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
static int32_t hFullJoinAddBlockRowsToHash(SSDataBlock* pBlock, SHJoinOperatorInfo* pJoin, bool* returnDirect) {
  SHJoinTableCtx* pBuild = pJoin->pBuild;
  SHJoinCtx* pCtx = &pJoin->ctx;

  pCtx->pBuildData = pBlock;
  pCtx->buildNMStartIdx = -1;
  pCtx->buildStartIdx = 0;
  pCtx->buildEndIdx = pBlock->info.rows - 1;

  // 先计算表达式，确保所有列数据正确
  HJ_ERR_RET(hJoinLaunchEqualExpr(pJoin->pOperator, pBlock, pBuild, pCtx->buildStartIdx, pCtx->buildEndIdx));

  // 再检查时间范围过滤
  if (pBuild->hasTimeRange && !hJoinFilterTimeRange(pCtx, pBlock, &pJoin->tblTimeRange, pBuild->primCol->srcSlot, &pCtx->buildStartIdx, &pCtx->buildEndIdx)) {
    pCtx->buildNMStartIdx = 0;
    pCtx->buildNMEndIdx = pBlock->info.rows - 1;
    
    return hFullJoinCopyBuildNMRowsToBlock(pJoin, pJoin->finBlk, pCtx, returnDirect);
  }

  HJ_ERR_RET(hJoinSetKeyColsData(pBlock, pBuild));

  if (pCtx->buildStartIdx > 0) {
    pCtx->buildNMStartIdx = 0;  
  }

  return hFullJoinAddBlockRowsToHashImpl(pJoin, pCtx, pBlock, returnDirect);
}

/*
 * Resumes an interrupted FULL JOIN build phase after a previous call returned early
 * because finBlk was full (returnDirect=true). Two resumption paths:
 *   1. buildNMStartIdx >= 0: there are pending NULL-key rows to emit first.
 *   2. buildStartIdx >= 0: there are rows in the current block still waiting to be hashed.
 *
 * Called at the start of each hFullJoinBuildHash invocation to drain leftover state
 * before pulling the next block from downstream.
 *
 * @param pJoin          hash join operator info
 * @param returnDirect   output: true if finBlk is full again and should be yielded
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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
    QRY_ERR_RET(doFilter(pJoin->finBlk, pJoin->pFinFilter, NULL, NULL));
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


/*
 * Handles remaining pPreFilter-path build rows for the current probe row in FULL JOIN.
 * Functionally identical to hLeftJoinHandleSeqRemainBuildRows but operates on FULL JOIN:
 * appends build rows to midBlk, applies pPreFilter, merges survivors into finBlk.
 * If no survivors after exhausting all build rows, emits a NULL-padded non-match row.
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @param loopCont   output: true to continue to next probe row, false to yield now
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hFullJoinHandleSeqRemainBuildRows(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin, bool* loopCont) {
  bool allFetched = false;
  SHJoinCtx* pCtx = &pJoin->ctx;
  
  while (!allFetched) {
    hJoinAppendResToBlock(pOperator, pJoin->midBlk, &allFetched);
    if (pJoin->midBlk->info.rows > 0) {
      HJ_ERR_RET(doFilter(pJoin->midBlk, pJoin->pPreFilter, NULL, NULL));
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

/*
 * Processes CUR-phase probe rows for FULL JOIN when pPreFilter is active (two-block path).
 * Functionally identical to hLeftJoinHandleSeqProbeRows but uses SFGroupData (which carries
 * a match bitmap) instead of SGroupData for hash table lookups.
 * Unmatched probe rows (NULL key, no hash match, or no survivor after filter) are emitted
 * with NULL-padded build columns.
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @param loopCont   output: true if caller should continue to POST phase, false to yield
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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
        HJ_ERR_RET(doFilter(pJoin->midBlk, pJoin->pPreFilter, NULL, NULL));
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


/*
 * Continues emitting matched build rows for the current probe row in FULL JOIN (no pPreFilter).
 * Functionally identical to hLeftJoinHandleRemainBuildRows but used in the FULL JOIN path.
 * Called when ctx.pBuildRow is non-NULL (linked list traversal was interrupted by block threshold).
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @param loopCont   output: true to continue to next probe row, false to yield
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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


/*
 * Processes CUR-phase probe rows for FULL JOIN when pPreFilter is absent (direct output path).
 * Unlike hLeftJoinHandleProbeRows, this path also sets pGroup->rowsMatchNum to track how many
 * build rows in each group have been matched, enabling the post-probe unmatched build row
 * emission phase (handled externally, not in this function).
 *
 * Unmatched probe rows (NULL key, no hash match) are emitted with NULL-padded build columns.
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @param loopCont   output: true if caller should continue to POST phase, false to yield
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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


/*
 * FULL OUTER JOIN probe execution. Implements the three-phase PRE/CUR/POST state machine.
 *
 * PRE phase: probe rows before the join time window — emitted as non-matching.
 * CUR phase: dispatched to hFullJoinHandleSeqProbeRows (with pPreFilter) or
 *            hFullJoinHandleProbeRows (without). Also handles remaining build row state.
 * POST phase: probe rows after the join time window — emitted as non-matching.
 *
 * Note: FULL JOIN also requires a second pass to emit unmatched build-side rows after all
 * probe blocks are processed. That pass is NOT handled here — it is handled during the
 * build phase (hFullJoinBuildHash) which emits NULL-key build rows directly, and the
 * SFGroupData.bitmap mechanism tracks which build rows were matched during probing.
 *
 * @param pOperator  the hash join operator
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
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



