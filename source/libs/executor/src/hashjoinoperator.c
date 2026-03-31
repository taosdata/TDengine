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
 * Hash join operator framework: initialization, main loop, teardown, and shared utilities.
 *
 * This file implements the operator infrastructure that drives hash join execution:
 *   - createHashJoinOperatorInfo: allocates and initializes all operator state
 *   - hJoinMainProcess: the main per-call entry point (build once, probe repeatedly)
 *   - destroyHashJoinOperator: releases all allocated resources
 *   - Shared utilities used by per-join-type functions in hashjoin.c
 *
 * Separation of concerns:
 *   - hashjoin.c holds per-join-type probe logic (hInnerJoinDo, hLeftJoinDo, etc.)
 *   - This file holds the operator shell, key/value serialization, hash table management,
 *     and result block construction shared across all join types.
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
#include "functionMgt.h"


/*
 * Returns true if the output block has reached the threshold for yielding to the caller.
 * Two criteria depending on whether a LIMIT clause and finFilter are present:
 *   - With pFinFilter or no LIMIT: threshold = blkThreshold (capacity * HJOIN_BLK_THRESHOLD_RATIO)
 *   - With LIMIT and no pFinFilter: threshold = limit - rows already returned
 * The LIMIT shortcut avoids over-producing rows when no post-filter can reduce the count.
 *
 * @param pInfo    hash join operator info
 * @param blkRows  current number of rows in the output block
 * @return true if the block should be yielded to the upstream caller
 */
bool hJoinBlkReachThreshold(SHJoinOperatorInfo* pInfo, int64_t blkRows) {
  if (INT64_MAX == pInfo->ctx.limit || pInfo->pFinFilter != NULL) {
    return blkRows >= pInfo->blkThreshold;
  }
  
  return (pInfo->execInfo.resRows + blkRows) >= pInfo->ctx.limit;
}

/*
 * Resolves the midBlk-overflows-finBlk situation by swapping midBlk and finBlk pointers.
 * Called at the start of the next hJoinMainProcess iteration when ctx.midRemains is true.
 * After the swap, the former midBlk (now finBlk) contains the leftover rows and is
 * returned to the upstream caller. The former finBlk (now midBlk) is the fresh block.
 * Sets ctx.midRemains = false after the swap.
 *
 * @param pJoin  hash join operator info
 * @return TSDB_CODE_SUCCESS
 */
int32_t hJoinHandleMidRemains(SHJoinOperatorInfo* pJoin) {
  TSWAP(pJoin->midBlk, pJoin->finBlk);

  pJoin->ctx.midRemains = false;

  return TSDB_CODE_SUCCESS;
}

/*
 * Resumes join execution when ctx.rowRemains is true (output block ran out of capacity
 * before all probe rows in the current block were processed). Calls joinFp to continue
 * from where it left off, then applies pFinFilter if present.
 *
 * @param pOperator  the hash join operator
 * @param pJoin      hash join operator info
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hJoinHandleRowRemains(struct SOperatorInfo* pOperator, SHJoinOperatorInfo* pJoin) {
  HJ_ERR_RET((*pJoin->joinFp)(pOperator));
  
  if (pJoin->finBlk->info.rows > 0 && pJoin->pFinFilter != NULL) {
    doFilter(pJoin->finBlk, pJoin->pFinFilter, NULL, NULL);
  }

  return TSDB_CODE_SUCCESS;
}


/*
 * Merges rows from midBlk into finBlk, handling the case where combined rows exceed
 * finBlk capacity. If they fit: all rows are moved to finBlk and midBlk is cleared.
 * If they don't fit: copies as many as possible and sets ctx.midRemains = true, leaving
 * the remaining rows in midBlk for the next call (via hJoinHandleMidRemains).
 *
 * The merge always appends midBlk rows after existing finBlk rows (midBlk is "less full").
 * The commented-out block above would swap pointers based on row count but is disabled —
 * the current approach avoids a pointer swap to keep the code simpler and correct.
 *
 * @param pCtx  join context (midRemains is updated here)
 * @param ppMid pointer to midBlk pointer
 * @param ppFin pointer to finBlk pointer
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hJoinCopyMergeMidBlk(SHJoinCtx* pCtx, SSDataBlock** ppMid, SSDataBlock** ppFin) {
  SSDataBlock* pLess = *ppMid;
  SSDataBlock* pMore = *ppFin;

/*
  if ((*ppMid)->info.rows < (*ppFin)->info.rows) {
    pLess = (*ppMid);
    pMore = (*ppFin);
  } else {
    pLess = (*ppFin);
    pMore = (*ppMid);
  }
*/

  int32_t totalRows = pMore->info.rows + pLess->info.rows;
  if (totalRows <= pMore->info.capacity) {
    HJ_ERR_RET(blockDataMerge(pMore, pLess));
    blockDataCleanup(pLess);
    pCtx->midRemains = false;
  } else {
    int32_t copyRows = pMore->info.capacity - pMore->info.rows;
    HJ_ERR_RET(blockDataMergeNRows(pMore, pLess, pLess->info.rows - copyRows, copyRows));
    blockDataShrinkNRows(pLess, copyRows);
    pCtx->midRemains = true;
  }

/*
  if (pMore != (*ppFin)) {
    TSWAP(*ppMid, *ppFin);
  }
*/

  return TSDB_CODE_SUCCESS;
}

/*
 * Binds the joinFp and buildFp function pointers based on join type and subtype.
 * For FULL JOIN, uses hFullJoinBuildHash instead of the standard hJoinBuildHash.
 * For RIGHT joins, the build/probe side swap (done in hJoinSetBuildAndProbeTable) means
 * the LEFT join logic (hLeftJoinDo, hSemiJoinDo, hAntiJoinDo) applies symmetrically.
 * Returns an error for unsupported type/subtype combinations.
 *
 * @param pJoin  hash join operator info (joinType, subType must already be set)
 * @return TSDB_CODE_SUCCESS on success, TSDB_CODE_QRY_INVALID_PLAN for unsupported types
 */
static int32_t hJoinProbeEndFull(struct SHJoinOperatorInfo* pJoin, bool* needBreak, bool* needContinue) {
  SSDataBlock* pRes = pJoin->finBlk;

  *needBreak = false;
  *needContinue = false;

  if (pJoin->ctx.buildNMatchDone) {
    return TSDB_CODE_SUCCESS;
  }

  HJ_ERR_RET(hJoinEmitBuildNMatchRows(pJoin));

  if (pRes->info.rows > 0 && pJoin->pFinFilter != NULL) {
    HJ_ERR_RET(doFilter(pRes, pJoin->pFinFilter, NULL, NULL));
  }

  if (pRes->info.rows > 0) {
    *needBreak = true;
    return TSDB_CODE_SUCCESS;
  }

  if (!pJoin->ctx.buildNMatchDone) {
    *needContinue = true;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t hJoinSetImplFp(SHJoinOperatorInfo* pJoin) {
  int32_t code = TSDB_CODE_SUCCESS;
  pJoin->buildFp = hJoinBuildHash;
  pJoin->probeEndFp = NULL;
  switch (pJoin->joinType) {
    case JOIN_TYPE_INNER:
      pJoin->joinFp = hInnerJoinDo;
      break;
    case JOIN_TYPE_LEFT:
    case JOIN_TYPE_RIGHT: {
      switch (pJoin->subType) {
        case JOIN_STYPE_OUTER:          
          pJoin->joinFp = hLeftJoinDo;
          break;
        case JOIN_STYPE_SEMI:
          pJoin->joinFp = hSemiJoinDo;
          break;
        case JOIN_STYPE_ANTI:
          pJoin->joinFp = hAntiJoinDo;
          break;
        default:
          qError("Not supported join type, type:%d, subType:%d", pJoin->joinType, pJoin->subType);
          code = TSDB_CODE_QRY_INVALID_PLAN;
          break;
      }
      break;
    }      
    case JOIN_TYPE_FULL:
      pJoin->joinFp = hFullJoinDo;
      pJoin->buildFp = hFullJoinBuildHash;
      pJoin->probeEndFp = hJoinProbeEndFull;
      break;
    default:
      qError("Not supported join type, type:%d, subType:%d", pJoin->joinType, pJoin->subType);
      code = TSDB_CODE_QRY_INVALID_PLAN;
      break;
  }

  return code;
}


/*
 * Evaluates the TIMETRUNCATE primary key expression for a range of rows in the block.
 * Computes: truncated_ts = ts - (ts + timezoneUnit) % truncateUnit  (timezone-aware)
 *        or: truncated_ts = ts / truncateUnit * truncateUnit          (no timezone)
 * Writes the result to the target slot in place, so subsequent key serialization reads
 * the truncated value instead of the raw timestamp.
 *
 * @param pBlock     data block (source and destination for the truncated value)
 * @param pTable     table context (provides primCtx for truncate parameters)
 * @param startIdx   first row index to evaluate
 * @param endIdx     last row index to evaluate (inclusive)
 * @return TSDB_CODE_SUCCESS
 */
int32_t hJoinLaunchPrimExpr(SSDataBlock* pBlock, SHJoinTableCtx* pTable, int32_t startIdx, int32_t endIdx) {
  SHJoinPrimExprCtx* pCtx = &pTable->primCtx;
  SColumnInfoData* pPrimIn = taosArrayGet(pBlock->pDataBlock, pTable->primCol->srcSlot);
  SColumnInfoData* pPrimOut = taosArrayGet(pBlock->pDataBlock, pTable->primCtx.targetSlotId);
  if (0 != pCtx->timezoneUnit) {
    for (int32_t i = startIdx; i <= endIdx; ++i) {
      ((int64_t*)pPrimOut->pData)[i] = ((int64_t*)pPrimIn->pData)[i] - (((int64_t*)pPrimIn->pData)[i] + pCtx->timezoneUnit) % pCtx->truncateUnit;
    }
  } else {
    for (int32_t i = startIdx; i <= endIdx; ++i) {
      ((int64_t*)pPrimOut->pData)[i] = ((int64_t*)pPrimIn->pData)[i] / pCtx->truncateUnit * pCtx->truncateUnit;
    }
  }

  return TSDB_CODE_SUCCESS;
}

/*
 * Evaluates all equality key expressions on a row range of the given block.
 * First evaluates the primary key expression (e.g. TIMETRUNCATE) if present,
 * then evaluates any additional scalar expressions via projectApplyFunctions.
 * Must be called before hJoinSetKeyColsData / hJoinCopyKeyColsDataToBuf for each block.
 *
 * @param pOperator  the hash join operator (for task info / stream runtime info)
 * @param pBlock     data block to evaluate expressions on (modified in place)
 * @param pTable     table context with expression metadata
 * @param startIdx   first row index
 * @param endIdx     last row index (inclusive)
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hJoinLaunchEqualExpr(SOperatorInfo*       pOperator, SSDataBlock* pBlock, SHJoinTableCtx* pTable, int32_t startIdx, int32_t endIdx) {
  if (NULL != pTable->primExpr) {
    HJ_ERR_RET(hJoinLaunchPrimExpr(pBlock, pTable, startIdx, endIdx));
  }
  if (pTable->exprSup.numOfExprs > 0) {
    HJ_ERR_RET(projectApplyFunctions(pTable->exprSup.pExprInfo, pBlock, pBlock, pTable->exprSup.pCtx, pTable->exprSup.numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo)));
  }
  
  return TSDB_CODE_SUCCESS;
}


/*
 * Counts the number of rows in a single hash group by traversing the linked list.
 * Used only in debug/trace code (currently disabled with #if 0).
 *
 * @param pRow  head of the SBufRowInfo linked list for a group
 * @return total number of rows in the group
 */
static int64_t hJoinGetSingleKeyRowsNum(SBufRowInfo* pRow) {
  int64_t rows = 0;
  while (pRow) {
    rows++;
    pRow = pRow->next;
  }
  return rows;
}

#if 0
static int64_t hJoinGetRowsNumOfKeyHash(SSHashObj* pHash) {
  SGroupData* pGroup = NULL;
  int32_t iter = 0;
  int64_t rowsNum = 0;
  
  while (NULL != (pGroup = tSimpleHashIterate(pHash, pGroup, &iter))) {
    int32_t* pKey = tSimpleHashGetKey(pGroup, NULL);
    int64_t rows = hJoinGetSingleKeyRowsNum(pGroup->rows);
    //qTrace("build_key:%d, rows:%" PRId64, *pKey, rows);
    rowsNum += rows;
  }

  return rowsNum;
}
#endif

/*
 * Initializes key column metadata for a table by parsing the join equality condition list.
 * Allocates keyCols[] and (conditionally) keyBuf for multi-column composite key serialization.
 *
 * keyBuf is allocated when:
 *   - keyNum > 1: multi-column keys must be concatenated into a single buffer for hashing.
 *   - allocKeyBuf is true: FULL JOIN always allocates keyBuf for build-side NULL key tracking.
 *
 * keyNullSize is the size of a sentinel value used to represent a NULL key in the buffer.
 * It is set to 1 or 2 bytes depending on total key size to ensure it differs from any valid key.
 *
 * @param pTable      table context to initialize
 * @param pList       list of SColumnNode (equality condition columns for this side)
 * @param allocKeyBuf whether to force-allocate keyBuf even for single-column keys
 * @return TSDB_CODE_SUCCESS on success, error code on allocation failure
 */
static int32_t hJoinInitKeyColsInfo(SHJoinTableCtx* pTable, SNodeList* pList, bool allocKeyBuf) {
  pTable->keyNum = LIST_LENGTH(pList);
  if (pTable->keyNum < 1) {
    qError("Invalid keyNum %d for hash join", pTable->keyNum);
    return TSDB_CODE_QRY_INVALID_PLAN;
  }
  
  // calloc is required here: for single-key joins (keyNum==1), hJoinCopyKeyColsDataToBuf skips
  // the multi-key loop that sets bufOffset, but hJoinCopyResRowsToBlock reads bufOffset
  // unconditionally. Zero-initialization ensures bufOffset defaults to 0 in the single-key path.
  pTable->keyCols = taosMemoryCalloc(pTable->keyNum, sizeof(SHJoinColInfo));
  if (NULL == pTable->keyCols) {
    qError("failed to alloc keyCols, keyNum:%d, code:%d", pTable->keyNum, terrno);
    return terrno;
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

  if (pTable->keyNum > 1 || allocKeyBuf) {
    if (bufSize > 1) {
      pTable->keyNullSize = 1;
    } else {
      pTable->keyNullSize = 2;
    }

    pTable->keyBuf = taosMemoryMalloc(TMAX(bufSize, pTable->keyNullSize));
    if (NULL == pTable->keyBuf) {
      qError("failed to alloc keyBuf, bufSize:%" PRId64 ", keyNullSize:%d, code:%d", bufSize, pTable->keyNullSize,
             terrno);
      return terrno;
    }
  }

  return TSDB_CODE_SUCCESS;
}

/*
 * Counts how many columns in the target list belong to the given data block (by blkId).
 * Used to pre-calculate valNum before allocating valCols[].
 *
 * @param pList   list of STargetNode (join output column list)
 * @param blkId   data block ID to match against
 * @param colNum  output: number of matching columns
 */
static void hJoinGetValColsNum(SNodeList* pList, int64_t blkId, int32_t* colNum) {
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

/*
 * Checks if a value column (identified by slotId) is also a key column.
 * If yes, returns the index into keyCols[] via pKeyIdx — used to read the value
 * from the probe-side key buffer instead of storing it redundantly in the row buffer.
 *
 * @param slotId   source slot ID of the value column
 * @param keyNum   number of key columns
 * @param pKeys    key column array
 * @param pKeyIdx  output: index into keyCols[] if this is a key col, -1 otherwise
 * @return true if the column is a key column, false otherwise
 */
static bool hJoinIsValColInKeyCols(int16_t slotId, int32_t keyNum, SHJoinColInfo* pKeys, int32_t* pKeyIdx) {
  for (int32_t i = 0; i < keyNum; ++i) {
    if (pKeys[i].srcSlot == slotId) {
      *pKeyIdx = i;
      return true;
    }
  }

  *pKeyIdx = -1;
  return false;
}

/*
 * Initializes value column metadata for a table by parsing the output target list.
 * Only columns belonging to this table (matching blkId) are included.
 *
 * For each value column:
 *   - Checks if it is also a key column (to avoid redundant storage in the row buffer).
 *   - Tracks variable-length columns in valVarCols[] for per-row buffer size calculation.
 *   - Accumulates fixed-length value buffer size (valBufSize) and null bitmap size (valBitMapSize).
 *
 * valColExist is set to true if any value column is NOT a key column, indicating that
 * non-key value data actually needs to be stored in the row buffer.
 *
 * @param pTable  table context to initialize
 * @param pList   join output target column list
 * @return TSDB_CODE_SUCCESS on success, error code on allocation failure
 */
static int32_t hJoinInitValColsInfo(SHJoinTableCtx* pTable, SNodeList* pList) {
  hJoinGetValColsNum(pList, pTable->blkId, &pTable->valNum);
  if (pTable->valNum < 1) {
    return TSDB_CODE_SUCCESS;
  }
  
  pTable->valCols = taosMemoryMalloc(pTable->valNum * sizeof(SHJoinColInfo));
  if (NULL == pTable->valCols) {
    qError("failed to alloc valCols, valNum:%d, code:%d", pTable->valNum, terrno);
    return terrno;
  }

  int32_t i = 0;
  int32_t colNum = 0;
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    STargetNode* pTarget = (STargetNode*)pNode;
    SColumnNode* pColNode = (SColumnNode*)pTarget->pExpr;
    if (pColNode->dataBlockId == pTable->blkId) {
      if (!hJoinIsValColInKeyCols(pColNode->slotId, pTable->keyNum, pTable->keyCols, &pTable->valCols[i].keyColIdx)) {
        pTable->valColExist = true;
        colNum++;
      }
      pTable->valCols[i].srcSlot = pColNode->slotId;
      pTable->valCols[i].dstSlot = pTarget->slotId;
      pTable->valCols[i].vardata = IS_VAR_DATA_TYPE(pColNode->node.resType.type);
      if (pTable->valCols[i].vardata && !IS_HASH_JOIN_KEY_COL(pTable->valCols[i].keyColIdx)) {
        if (NULL == pTable->valVarCols) {
          pTable->valVarCols = taosArrayInit(pTable->valNum, sizeof(int32_t));
          if (NULL == pTable->valVarCols) {
            qError("failed to init valVarCols, valNum:%d, code:%d", pTable->valNum, terrno);
            return terrno;
          }
        }
        if (NULL == taosArrayPush(pTable->valVarCols, &i)) {
          qError("failed to push valVarCols index:%d, code:%d", i, terrno);
          return terrno;
        }
      }
      pTable->valCols[i].bytes = pColNode->node.resType.bytes;
      if (!IS_HASH_JOIN_KEY_COL(pTable->valCols[i].keyColIdx) && !pTable->valCols[i].vardata) {
        pTable->valBufSize += pColNode->node.resType.bytes;
      }
      i++;
    }
  }

  pTable->valBitMapSize = BitmapLen(colNum);
  pTable->valBufSize += pTable->valBitMapSize;

  return TSDB_CODE_SUCCESS;
}

/*
 * Allocates and initializes the primary key column mapping for a table.
 * The primary key column is the timestamp column used for time range filtering.
 * Only srcSlot is set here; dstSlot is not used for the primary key.
 *
 * @param pTable  table context to initialize
 * @param slotId  source slot ID of the primary key (timestamp) column
 * @return TSDB_CODE_SUCCESS on success, error code on allocation failure
 */
static int32_t hJoinInitPrimKeyInfo(SHJoinTableCtx* pTable, int32_t slotId) {
  pTable->primCol = taosMemoryMalloc(sizeof(SHJoinColMap));
  if (NULL == pTable->primCol) {
    qError("failed to alloc primCol, code:%d", terrno);
    return terrno;
  }

  pTable->primCol->srcSlot = slotId;

  return TSDB_CODE_SUCCESS;
}


/*
 * Initializes the primary key expression context for a TIMETRUNCATE join condition.
 * Parses the STargetNode -> SFunctionNode tree to extract truncation unit, timezone offset,
 * and the output slot. If pNode is NULL (no expression), the target slot is set to the
 * raw primary key slot (identity: no truncation needed).
 *
 * Only TIMETRUNCATE with 4 or 5 parameters is supported. Any other expression type
 * returns TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR.
 *
 * @param pNode   the primary key expression node (STargetNode), or NULL for no expression
 * @param pCtx    output: initialized SHJoinPrimExprCtx
 * @param pTable  table context (provides primCol->srcSlot for the no-expression case)
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
static int32_t hJoinInitPrimExprCtx(SNode* pNode, SHJoinPrimExprCtx* pCtx, SHJoinTableCtx* pTable) {
  if (NULL == pNode) {
    pCtx->targetSlotId = pTable->primCol->srcSlot;
    return TSDB_CODE_SUCCESS;
  }
  
  if (QUERY_NODE_TARGET != nodeType(pNode)) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }  

  STargetNode* pTarget = (STargetNode*)pNode;
  if (QUERY_NODE_FUNCTION != nodeType(pTarget->pExpr)) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SFunctionNode* pFunc = (SFunctionNode*)pTarget->pExpr;
  if (FUNCTION_TYPE_TIMETRUNCATE != pFunc->funcType) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  if (4 != pFunc->pParameterList->length && 5 != pFunc->pParameterList->length) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SValueNode* pUnit = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 1);
  SValueNode* pCurrTz = (5 == pFunc->pParameterList->length) ? (SValueNode*)nodesListGetNode(pFunc->pParameterList, 2) : NULL;
  SValueNode* pTimeZone = (5 == pFunc->pParameterList->length) ? (SValueNode*)nodesListGetNode(pFunc->pParameterList, 4) : (SValueNode*)nodesListGetNode(pFunc->pParameterList, 3);

  pCtx->truncateUnit = pUnit->typeData;
  if ((NULL == pCurrTz || 1 == pCurrTz->typeData) && pCtx->truncateUnit >= (86400 * TSDB_TICK_PER_SECOND(pFunc->node.resType.precision))) {
    pCtx->timezoneUnit = offsetFromTz(varDataVal(pTimeZone->datum.p), TSDB_TICK_PER_SECOND(pFunc->node.resType.precision));
  }

  pCtx->targetSlotId = pTarget->slotId;

  return TSDB_CODE_SUCCESS;
}

/*
 * Initializes the scalar expression support for a table's key equality conditions.
 * Creates SExprInfo objects from pExprList and initializes the expression support context.
 * No-op if pExprList is NULL (no scalar expressions for this table).
 *
 * @param pTable     table context to initialize
 * @param pExprList  list of scalar expression nodes (from plan node), or NULL
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
static int32_t hJoinInitScalarExpr(SHJoinTableCtx* pTable, SNodeList* pExprList) {
  if (NULL == pExprList) {
    return TSDB_CODE_SUCCESS;
  }
  
  int32_t    numOfExpr = 0;
  SExprInfo* pExprInfo = NULL;

  HJ_ERR_RET(createExprInfo(pExprList, NULL, &pExprInfo, &numOfExpr));
  
  HJ_ERR_RET(initExprSupp(&pTable->exprSup, pExprInfo, numOfExpr, NULL));

  return TSDB_CODE_SUCCESS;
}


/*
 * Initializes a single table context (build or probe) for the hash join.
 * Sets up downstream operator link, key/value column metadata, primary key info,
 * scalar expression context, and primary key expression context.
 *
 * idx=0 → left table (pOnLeftCols / leftPrimSlotId / pLeftExpr)
 * idx=1 → right table (pOnRightCols / rightPrimSlotId / pRightExpr)
 *
 * For FULL JOIN, allocKeyBuf is forced to true so the build side always has a keyBuf
 * for NULL-key detection during the build phase.
 *
 * @param pJoin       hash join operator info (partially initialized)
 * @param pJoinNode   physical plan node with column and expression info
 * @param pDownstream array of downstream operators
 * @param idx         which table to initialize (0=left, 1=right)
 * @param pStat       input statistics for hash table sizing hint
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
static int32_t hJoinInitTableInfo(SHJoinOperatorInfo* pJoin, SHashJoinPhysiNode* pJoinNode, SOperatorInfo** pDownstream, int32_t idx, SQueryStat* pStat) {
  SNodeList* pKeyList = NULL;
  SHJoinTableCtx* pTable = &pJoin->tbs[idx];
  pTable->downStream = pDownstream[idx];
  pTable->blkId = pDownstream[idx]->resultDataBlockId;
  if (0 == idx) {
    pKeyList = pJoinNode->pOnLeftCols;
    pTable->hasTimeRange = pJoinNode->timeRangeTarget & 0x1;
    HJ_ERR_RET(hJoinInitScalarExpr(pTable, pJoinNode->pLeftExpr));
  } else {
    pKeyList = pJoinNode->pOnRightCols;
    pTable->hasTimeRange = pJoinNode->timeRangeTarget & 0x2;
    HJ_ERR_RET(hJoinInitScalarExpr(pTable, pJoinNode->pRightExpr));
  }

  HJ_ERR_RET(hJoinInitPrimKeyInfo(pTable, (0 == idx) ? pJoinNode->leftPrimSlotId : pJoinNode->rightPrimSlotId));
  
  int32_t code = hJoinInitKeyColsInfo(pTable, pKeyList, JOIN_TYPE_FULL == pJoin->joinType);
  HJ_ERR_RET(code);
  code = hJoinInitValColsInfo(pTable, pJoinNode->pTargets);
  HJ_ERR_RET(code);

  TAOS_MEMCPY(&pTable->inputStat, pStat, sizeof(*pStat));

  HJ_ERR_RET(hJoinInitPrimExprCtx(pTable->primExpr, &pTable->primCtx, pTable));

  return TSDB_CODE_SUCCESS;
}

/*
 * Assigns pBuild and pProbe pointers and sets downStreamIdx based on join type.
 * For INNER, FULL, and LEFT joins: build=tbs[1] (right), probe=tbs[0] (left).
 * For RIGHT joins: build=tbs[0] (left), probe=tbs[1] (right). This swap means the LEFT
 * join logic (hLeftJoinDo, etc.) applies symmetrically without special-casing RIGHT joins.
 * Also assigns the primary key expression pointers for each side.
 *
 * @param pInfo      hash join operator info (joinType, subType must already be set)
 * @param pJoinNode  physical plan node with left/right primary key expression info
 */
static void hJoinSetBuildAndProbeTable(SHJoinOperatorInfo* pInfo, SHashJoinPhysiNode* pJoinNode) {
  int32_t buildIdx = 0;
  int32_t probeIdx = 1;

  pInfo->joinType = pJoinNode->joinType;
  pInfo->subType = pJoinNode->subType;
  
  switch (pInfo->joinType) {
    case JOIN_TYPE_INNER:
    case JOIN_TYPE_FULL:
      buildIdx = 1;
      probeIdx = 0;
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

  if (0 == buildIdx) {
    pInfo->pBuild->primExpr = pJoinNode->leftPrimExpr;
    pInfo->pProbe->primExpr = pJoinNode->rightPrimExpr;
  } else {
    pInfo->pBuild->primExpr = pJoinNode->rightPrimExpr;
    pInfo->pProbe->primExpr = pJoinNode->leftPrimExpr;
  }

  pInfo->pBuild->type = E_JOIN_TB_BUILD;
  pInfo->pProbe->type = E_JOIN_TB_PROBE;  
}

/*
 * Builds pResColMap[]: a per-output-column flag array where 1 means the column comes from
 * the build side and 0 means it comes from the probe side. Used in result construction
 * (hJoinCopyResRowsToBlock, hJoinCopyNMatchRowsToBlock) to route each column correctly.
 *
 * @param pInfo      hash join operator info
 * @param pJoinNode  physical plan node (provides pTargets with column-to-block mappings)
 * @return TSDB_CODE_SUCCESS on success, error code on allocation failure
 */
static int32_t hJoinBuildResColsMap(SHJoinOperatorInfo* pInfo, SHashJoinPhysiNode* pJoinNode) {
  pInfo->pResColNum = pJoinNode->pTargets->length;
  pInfo->pResColMap = taosMemoryCalloc(pJoinNode->pTargets->length, sizeof(int8_t));
  if (NULL == pInfo->pResColMap) {
    qError("failed to alloc result col map, colNum:%d, code:%d", pJoinNode->pTargets->length, terrno);
    return terrno;
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


/*
 * Allocates a new 10 MB page and appends it to the row buffer pool.
 * Called when the current page has insufficient space for the next row's value data.
 * The page pool grows on demand; no disk spill occurs (memory-only).
 *
 * @param pRowBufs  array of SBufPageInfo (the page pool)
 * @return TSDB_CODE_SUCCESS on success, error code on allocation failure
 */
static FORCE_INLINE int32_t hJoinAddPageToBufs(SArray* pRowBufs) {
  SBufPageInfo page;
  page.pageSize = HASH_JOIN_DEFAULT_PAGE_SIZE;
  page.offset = 0;
  page.data = taosMemoryMalloc(page.pageSize);
  if (NULL == page.data) {
    qError("failed to alloc row page, pageSize:%d, code:%d", page.pageSize, terrno);
    return terrno;
  }

  if (NULL == taosArrayPush(pRowBufs, &page)) {
    qError("failed to append row page into row buffer pool, code:%d", terrno);
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

/*
 * Initializes the row buffer pool by allocating the first 10 MB page.
 * The pool is an array of SBufPageInfo; additional pages are added on demand
 * as the build phase inserts more rows than fit in a single page.
 *
 * @param pInfo  hash join operator info (pRowBufs is set here)
 * @return TSDB_CODE_SUCCESS on success, error code on allocation failure
 */
static int32_t hJoinInitBufPages(SHJoinOperatorInfo* pInfo) {
  pInfo->pRowBufs = taosArrayInit(32, sizeof(SBufPageInfo));
  if (NULL == pInfo->pRowBufs) {
    qError("failed to init row buffer pool, code:%d", terrno);
    return terrno;
  }

  return hJoinAddPageToBufs(pInfo->pRowBufs);
}

/*
 * Frees all memory allocated for a table context (key/value column arrays, key buffer,
 * primary key column mapping, and variable-length column index array).
 * Does NOT free the table context struct itself (it is embedded in SHJoinOperatorInfo).
 *
 * @param pTable  table context to free
 */
static void hJoinFreeTableInfo(SHJoinTableCtx* pTable) {
  taosMemoryFreeClear(pTable->keyCols);
  taosMemoryFreeClear(pTable->keyBuf);
  taosMemoryFreeClear(pTable->valCols);
  taosArrayDestroy(pTable->valVarCols);
  taosMemoryFree(pTable->primCol);
}

/*
 * Callback for taosArrayDestroyEx to free a single page in the row buffer pool.
 * Frees the page data buffer (allocated in hJoinAddPageToBufs).
 *
 * @param param  pointer to SBufPageInfo (cast from void*)
 */
static void hJoinFreeBufPage(void* param) {
  SBufPageInfo* pInfo = (SBufPageInfo*)param;
  taosMemoryFree(pInfo->data);
}

static void hJoinFreeFullGroupBitmap(SSHashObj* pHash, SHJoinCtx* pCtx) {
  if (NULL == pHash) {
    if (pCtx) {
      taosMemoryFreeClear(pCtx->fullGrpBitmapPool);
      pCtx->fullGrpBitmapPoolSize = 0;
      pCtx->fullGrpBitmapPoolOffset = 0;
    }
    return;
  }

  void* pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHash, pIte, &iter)) != NULL) {
    SFGroupData* pGroup = pIte;
    pGroup->bitmap = NULL;
    taosMemoryFreeClear(pGroup->keyOffsets);
  }

  if (pCtx) {
    taosMemoryFreeClear(pCtx->fullGrpBitmapPool);
    pCtx->fullGrpBitmapPoolSize = 0;
    pCtx->fullGrpBitmapPoolOffset = 0;
  }
}

/*
 * Frees the hash table and all SBufRowInfo nodes in its linked lists.
 * Each hash entry (SGroupData) contains a linked list of SBufRowInfo nodes
 * that are individually heap-allocated (via hJoinGetValBufFromPages). These must
 * be freed before calling tSimpleHashCleanup to avoid memory leaks.
 * Sets *ppHash to NULL after cleanup.
 *
 * Note: this function is currently replaced by a direct tSimpleHashCleanup call in
 * most teardown paths. The SBufRowInfo nodes are allocated within page pool memory
 * (not separately malloc'd), so this function is potentially incorrect for the current
 * allocation scheme. It is left here for reference but commented-out at call sites.
 *
 * @param ppHash  pointer to the hash table pointer; set to NULL on return
 */
static void hJoinDestroyKeyHash(SSHashObj** ppHash) {
  if (NULL == ppHash || NULL == (*ppHash)) {
    return;
  }

  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(*ppHash, pIte, &iter)) != NULL) {
    SGroupData* pGroup = pIte;
    SBufRowInfo* pRow = pGroup->rows;
    SBufRowInfo* pNext = NULL;
    while (pRow) {
      pNext = pRow->next;
      taosMemoryFree(pRow);
      pRow = pNext;
    }
  }

  tSimpleHashCleanup(*ppHash);
  *ppHash = NULL;
}

/*
 * Retrieves a pointer to a build-side row's value data from the page pool.
 * Uses pRow->pageId and pRow->offset to locate the data within the page.
 * Returns NULL (via *ppData = NULL) if pageId is UINT16_MAX, which indicates
 * a row that has no value data (all output columns are key columns).
 *
 * @param pRowBufs  page pool array
 * @param pRow      row metadata (pageId and offset)
 * @param ppData    output: pointer to the row's value data, or NULL if no data
 * @return TSDB_CODE_SUCCESS on success, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR if page not found
 */
static FORCE_INLINE int32_t hJoinRetrieveColDataFromRowBufs(SArray* pRowBufs, SBufRowInfo* pRow, char** ppData) {
  *ppData = NULL;
  
  if ((uint16_t)-1 == pRow->pageId) {
    return TSDB_CODE_SUCCESS;
  }
  SBufPageInfo *pPage = taosArrayGet(pRowBufs, pRow->pageId);
  if (NULL == pPage) {
    qError("fail to get %d page, total:%d", pRow->pageId, (int32_t)taosArrayGetSize(pRowBufs));
    QRY_ERR_RET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  
  *ppData = pPage->data + pRow->offset;

  return TSDB_CODE_SUCCESS;
}

/*
 * Copies rowNum matched result rows to the output block pRes. Each result row is constructed
 * by combining:
 *   - Build-side value columns: read from the page pool via pRow->pageId/offset
 *   - Build-side key columns that are also output cols: read from probe-side key buffer
 *     (to avoid redundant storage; keyColIdx >= 0 indicates this case)
 *   - Probe-side columns: batch-copied for all rowNum rows in one colDataCopyNItems call
 *
 * For build-side value columns, a NULL bitmap is checked per column to set null values.
 * Variable-length columns are advanced by varDataTLen. Probe-side columns are only
 * written on r==0 (first row) and use colDataCopyNItems to repeat for all rowNum rows.
 *
 * @param pJoin   hash join operator info
 * @param rowNum  number of consecutive build rows to copy (from pStart linked list)
 * @param pStart  head of the build-side row linked list segment to copy
 * @param pRes    output block to append rows to
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
static int32_t hJoinCopyResRowsToBlock(SHJoinOperatorInfo* pJoin, int32_t rowNum, SBufRowInfo* pStart, SSDataBlock* pRes) {
  SHJoinTableCtx* pBuild = pJoin->pBuild;
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  int32_t buildIdx = 0, buildValIdx = 0;
  int32_t probeIdx = 0;
  SBufRowInfo* pRow = pStart;
  int32_t code = 0;
  char* pData = NULL;

  for (int32_t r = 0; r < rowNum; ++r) {
    HJ_ERR_RET(hJoinRetrieveColDataFromRowBufs(pJoin->pRowBufs, pRow, &pData));
    
    char* pValData = pData + pBuild->valBitMapSize;
    char* pKeyData = pProbe->keyData;
    buildIdx = buildValIdx = probeIdx = 0;
    for (int32_t i = 0; i < pJoin->pResColNum; ++i) {
      if (pJoin->pResColMap[i]) {
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pBuild->valCols[buildIdx].dstSlot);
        if (IS_HASH_JOIN_KEY_COL(pBuild->valCols[buildIdx].keyColIdx)) {
          int32_t bufOffset = pProbe->keyCols[pBuild->valCols[buildIdx].keyColIdx].bufOffset;
          code = colDataSetVal(pDst, pRes->info.rows + r, pKeyData + bufOffset, false);
          HJ_ERR_RET(code);
        } else {
          if (BMIsNull(pData, buildValIdx)) {
            code = colDataSetVal(pDst, pRes->info.rows + r, NULL, true);
            HJ_ERR_RET(code);
          } else {
            code = colDataSetVal(pDst, pRes->info.rows + r, pValData, false);
            HJ_ERR_RET(code);
            pValData += pBuild->valCols[buildIdx].vardata ? varDataTLen(pValData) : pBuild->valCols[buildIdx].bytes;
          }
          buildValIdx++;
        }
        buildIdx++;
      } else if (0 == r) {
        SColumnInfoData* pSrc = taosArrayGet(pJoin->ctx.pProbeData->pDataBlock, pProbe->valCols[probeIdx].srcSlot);
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pProbe->valCols[probeIdx].dstSlot);

        int32_t probeRowIdx = pJoin->ctx.probeStartIdx;
        if (probeRowIdx < 0 || probeRowIdx >= pJoin->ctx.pProbeData->info.rows) {
          qError("invalid probe row index:%d, probe rows:%ld", probeRowIdx, pJoin->ctx.pProbeData->info.rows);
          return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        }

        if (IS_VAR_DATA_TYPE(pSrc->info.type)) {
          int32_t offset = pSrc->varmeta.offset[probeRowIdx];
          if (offset < 0) {
            code = colDataCopyNItems(pDst, pRes->info.rows, NULL, rowNum, true);
          } else {
            if (offset >= pSrc->varmeta.length) {
              qError("invalid var offset:%d, var length:%d, row:%d", offset, pSrc->varmeta.length, probeRowIdx);
              return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
            }
            code = colDataCopyNItems(pDst, pRes->info.rows, pSrc->pData + offset, rowNum, false);
          }
        } else {
          if (colDataIsNull_s(pSrc, probeRowIdx)) {
            code = colDataCopyNItems(pDst, pRes->info.rows, NULL, rowNum, true);
          } else {
            code = colDataCopyNItems(pDst, pRes->info.rows, colDataGetData(pSrc, probeRowIdx), rowNum, false);
          }
        }
        HJ_ERR_RET(code);

        probeIdx++;
      }
    }
    pRow = pRow->next;
  }

  return TSDB_CODE_SUCCESS;
}

static void hJoinBuildKeyOffsets(const SHJoinTableCtx* pBuild, const char* pKeyData, int32_t* pOffsets) {
  int32_t offset = 0;
  for (int32_t i = 0; i < pBuild->keyNum; ++i) {
    pOffsets[i] = offset;
    if (pBuild->keyCols[i].vardata) {
      offset += varDataTLen((char*)(pKeyData + offset));
    } else {
      offset += pBuild->keyCols[i].bytes;
    }
  }
}

static int32_t hJoinEnsureGroupKeyOffsets(const SHJoinTableCtx* pBuild, SFGroupData* pGroup, const char* pKeyData) {
  if (NULL == pBuild || NULL == pGroup || NULL == pKeyData || pBuild->keyNum <= 0) {
    qError("invalid key offset context, pBuild:%p, pGroup:%p, pKeyData:%p, keyNum:%d", (void*)pBuild, (void*)pGroup,
           (void*)pKeyData, pBuild ? pBuild->keyNum : -1);
    return TSDB_CODE_INVALID_PARA;
  }

  if (pGroup->keyOffsets && pGroup->keyOffsetNum == pBuild->keyNum) {
    return TSDB_CODE_SUCCESS;
  }

  if (pGroup->keyOffsets && pGroup->keyOffsetNum != pBuild->keyNum) {
    qError("key offset cache mismatch, cached:%d, expected:%d, rebuilding", pGroup->keyOffsetNum, pBuild->keyNum);
    taosMemoryFreeClear(pGroup->keyOffsets);
  }

  pGroup->keyOffsets = taosMemoryCalloc(pBuild->keyNum, sizeof(int32_t));
  if (NULL == pGroup->keyOffsets) {
    qError("failed to alloc key offset cache, keyNum:%d, code:%d", pBuild->keyNum, terrno);
    return terrno;
  }

  hJoinBuildKeyOffsets(pBuild, pKeyData, pGroup->keyOffsets);
  pGroup->keyOffsetNum = pBuild->keyNum;

  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinCopyBuildNMatchRowsToBlock(SHJoinOperatorInfo* pJoin, SSDataBlock* pRes, SBufRowInfo** pRows, int32_t rowNum,
                                               const char* pKeyData, const int32_t* pKeyOffsets) {
  SHJoinTableCtx* pBuild = pJoin->pBuild;
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  int32_t buildIdx = 0, probeIdx = 0;
  int32_t startRow = pRes->info.rows;

  if (rowNum <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  // Batch fill probe-side NULLs and repeated build-side key columns.
  for (int32_t i = 0; i < pJoin->pResColNum; ++i) {
    if (pJoin->pResColMap[i]) {
      SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pBuild->valCols[buildIdx].dstSlot);
      if (IS_HASH_JOIN_KEY_COL(pBuild->valCols[buildIdx].keyColIdx)) {
        int32_t keyColIdx = pBuild->valCols[buildIdx].keyColIdx;
        HJ_ERR_RET(colDataCopyNItems(pDst, startRow, pKeyData + pKeyOffsets[keyColIdx], rowNum, false));
      }
      buildIdx++;
    } else {
      SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pProbe->valCols[probeIdx].dstSlot);
      colDataSetNItemsNull(pDst, startRow, rowNum);
      probeIdx++;
    }
  }

  // Pre-decode each row's non-key value pointers once, then write by column.
  int32_t nonKeyNum = 0;
  for (int32_t i = 0; i < pBuild->valNum; ++i) {
    if (!IS_HASH_JOIN_KEY_COL(pBuild->valCols[i].keyColIdx)) {
      nonKeyNum++;
    }
  }

  if (nonKeyNum <= 0) {
    pRes->info.rows += rowNum;
    return TSDB_CODE_SUCCESS;
  }

  const char* valPtrs[HJOIN_NMATCH_BATCH_SIZE][nonKeyNum];
  bool colAllNull[nonKeyNum];
  for (int32_t c = 0; c < nonKeyNum; ++c) {
    colAllNull[c] = true;
  }

  for (int32_t r = 0; r < rowNum; ++r) {
    char* pData = NULL;
    int32_t code = hJoinRetrieveColDataFromRowBufs(pJoin->pRowBufs, pRows[r], &pData);
    if (code) {
      qError("failed to retrieve row data from buf pages, pageId:%u, offset:%d, grpRowIdx:%u, rowIdx:%d", pRows[r]->pageId,
             pRows[r]->offset, pRows[r]->grpRowIdx, r);
      return code;
    }

    char* pValData = pData ? (pData + pBuild->valBitMapSize) : NULL;

    int32_t colIdx = 0;
    for (int32_t v = 0; v < pBuild->valNum; ++v) {
      if (IS_HASH_JOIN_KEY_COL(pBuild->valCols[v].keyColIdx)) {
        continue;
      }

      if (NULL == pData || BMIsNull(pData, colIdx)) {
        valPtrs[r][colIdx] = NULL;
      } else {
        valPtrs[r][colIdx] = pValData;
        colAllNull[colIdx] = false;
        pValData += pBuild->valCols[v].vardata ? varDataTLen(pValData) : pBuild->valCols[v].bytes;
      }

      colIdx++;
    }
  }

  int32_t colIdx = 0;
  for (int32_t v = 0; v < pBuild->valNum; ++v) {
    if (IS_HASH_JOIN_KEY_COL(pBuild->valCols[v].keyColIdx)) {
      continue;
    }

    SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pBuild->valCols[v].dstSlot);
    if (colAllNull[colIdx]) {
      colDataSetNItemsNull(pDst, startRow, rowNum);
      colIdx++;
      continue;
    }

    // Use null-run batching; keep per-row set for non-null rows.
    int32_t nullRunStart = -1;
    for (int32_t r = 0; r < rowNum; ++r) {
      if (NULL == valPtrs[r][colIdx]) {
        if (nullRunStart < 0) {
          nullRunStart = r;
        }
        continue;
      }

      if (nullRunStart >= 0) {
        colDataSetNItemsNull(pDst, startRow + nullRunStart, r - nullRunStart);
        nullRunStart = -1;
      }

      HJ_ERR_RET(colDataSetVal(pDst, startRow + r, (void*)valPtrs[r][colIdx], false));
    }

    if (nullRunStart >= 0) {
      colDataSetNItemsNull(pDst, startRow + nullRunStart, rowNum - nullRunStart);
    }

    colIdx++;
  }

  pRes->info.rows += rowNum;
  return TSDB_CODE_SUCCESS;
}

int32_t hJoinCopyBuildNMatchRowToBlock(SHJoinOperatorInfo* pJoin, SSDataBlock* pRes, SBufRowInfo* pRow, const char* pKeyData,
                                       const int32_t* pKeyOffsets) {
  SBufRowInfo* rows[1] = {pRow};
  return hJoinCopyBuildNMatchRowsToBlock(pJoin, pRes, rows, 1, pKeyData, pKeyOffsets);
}

static bool hJoinIsBuildRowMatched(const SFGroupData* pGroup, const SBufRowInfo* pRow) {
  if (pGroup->rowsMatchNum >= pGroup->rowsNum) {
    return true;
  }

  if (NULL == pGroup->bitmap || pRow->grpRowIdx >= pGroup->rowsNum) {
    return false;
  }

  return JOIN_ROW_BITMAP_SET(pGroup->bitmap, 0, pRow->grpRowIdx);
}

int32_t hJoinEmitBuildNMatchRows(SHJoinOperatorInfo* pJoin) {
  if (JOIN_TYPE_FULL != pJoin->joinType || NULL == pJoin->pKeyHash || pJoin->ctx.buildNMatchDone) {
    pJoin->ctx.buildNMatchDone = true;
    pJoin->ctx.rowRemains = false;
    return TSDB_CODE_SUCCESS;
  }

  SHJoinCtx* pCtx = &pJoin->ctx;
  SSDataBlock* pRes = pJoin->finBlk;

  if (!pCtx->buildNMatchInited) {
    pCtx->pBuildNMatchGrp = NULL;
    pCtx->pBuildNMatchRow = NULL;
    pCtx->buildNMatchIter = 0;
    pCtx->buildNMatchInited = true;
  }

  while (!hJoinBlkReachThreshold(pJoin, pRes->info.rows) && pRes->info.rows < pRes->info.capacity) {
    if (NULL == pCtx->pBuildNMatchGrp) {
      pCtx->pBuildNMatchGrp = tSimpleHashIterate(pJoin->pKeyHash, pCtx->pBuildNMatchGrp, &pCtx->buildNMatchIter);
      if (NULL == pCtx->pBuildNMatchGrp) {
        pCtx->buildNMatchDone = true;
        pCtx->rowRemains = false;
        return TSDB_CODE_SUCCESS;
      }
      pCtx->pBuildNMatchRow = ((SFGroupData*)pCtx->pBuildNMatchGrp)->rows;
    }

    SFGroupData* pGroup = (SFGroupData*)pCtx->pBuildNMatchGrp;
    if (pGroup->rowsMatchNum >= pGroup->rowsNum) {
      pCtx->pBuildNMatchRow = NULL;
      pCtx->pBuildNMatchGrp = tSimpleHashIterate(pJoin->pKeyHash, pCtx->pBuildNMatchGrp, &pCtx->buildNMatchIter);
      if (pCtx->pBuildNMatchGrp) {
        pCtx->pBuildNMatchRow = ((SFGroupData*)pCtx->pBuildNMatchGrp)->rows;
        continue;
      }

      pCtx->buildNMatchDone = true;
      pCtx->rowRemains = false;
      return TSDB_CODE_SUCCESS;
    }

    size_t keyLen = 0;
    const char* pKeyData = (const char*)tSimpleHashGetKey(pGroup, &keyLen);
    (void)keyLen;
    if (NULL == pKeyData) {
      qError("failed to get hash group key, rowsNum:%u, rowsMatchNum:%u", pGroup->rowsNum, pGroup->rowsMatchNum);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    HJ_ERR_RET(hJoinEnsureGroupKeyOffsets(pJoin->pBuild, pGroup, pKeyData));

    while (pCtx->pBuildNMatchRow && !hJoinBlkReachThreshold(pJoin, pRes->info.rows) && pRes->info.rows < pRes->info.capacity) {
      int32_t rowsBudget = TMIN(HJOIN_NMATCH_BATCH_SIZE, pRes->info.capacity - pRes->info.rows);
      SBufRowInfo* batchRows[HJOIN_NMATCH_BATCH_SIZE];
      int32_t batchNum = 0;
      while (pCtx->pBuildNMatchRow && batchNum < rowsBudget && !hJoinBlkReachThreshold(pJoin, pRes->info.rows + batchNum)) {
        SBufRowInfo* pRow = pCtx->pBuildNMatchRow;
        pCtx->pBuildNMatchRow = pRow->next;
        if (hJoinIsBuildRowMatched(pGroup, pRow)) {
          continue;
        }

        batchRows[batchNum++] = pRow;
      }

      if (batchNum > 0) {
        HJ_ERR_RET(hJoinCopyBuildNMatchRowsToBlock(pJoin, pRes, batchRows, batchNum, pKeyData, pGroup->keyOffsets));
      }
    }

    if (NULL == pCtx->pBuildNMatchRow) {
      pCtx->pBuildNMatchGrp = tSimpleHashIterate(pJoin->pKeyHash, pCtx->pBuildNMatchGrp, &pCtx->buildNMatchIter);
      if (pCtx->pBuildNMatchGrp) {
        pCtx->pBuildNMatchRow = ((SFGroupData*)pCtx->pBuildNMatchGrp)->rows;
      } else {
        pCtx->buildNMatchDone = true;
        pCtx->rowRemains = false;
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  pCtx->rowRemains = !pCtx->buildNMatchDone;
  return TSDB_CODE_SUCCESS;
}

int32_t hJoinCopyNMatchRowsToBlock(SHJoinOperatorInfo* pJoin, SSDataBlock* pRes, int32_t startIdx, int32_t rows) {
  SHJoinTableCtx* pBuild = pJoin->pBuild;
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  int32_t buildIdx = 0;
  int32_t probeIdx = 0;
  int32_t code = 0;

  for (int32_t i = 0; i < pJoin->pResColNum; ++i) {
    if (pJoin->pResColMap[i]) {
      SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pBuild->valCols[buildIdx].dstSlot);
      colDataSetNItemsNull(pDst, pRes->info.rows, rows);

      buildIdx++;
    } else {
      SColumnInfoData* pSrc = taosArrayGet(pJoin->ctx.pProbeData->pDataBlock, pProbe->valCols[probeIdx].srcSlot);
      SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pProbe->valCols[probeIdx].dstSlot);

      QRY_ERR_RET(colDataAssignNRows(pDst, pRes->info.rows, pSrc, startIdx, rows));

      probeIdx++;
    }
  }

  pRes->info.rows += rows;

  return TSDB_CODE_SUCCESS;
}



void hJoinAppendResToBlock(struct SOperatorInfo* pOperator, SSDataBlock* pRes, bool* allFetched) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinCtx* pCtx = &pJoin->ctx;
  SBufRowInfo* pStart = pCtx->pBuildRow;
  int32_t rowNum = 0;
  int32_t resNum = pRes->info.rows;
  
  while (pCtx->pBuildRow && (resNum < pRes->info.capacity)) {
    rowNum++;
    resNum++;
    pCtx->pBuildRow = pCtx->pBuildRow->next;
  }

  pJoin->execInfo.resRows += rowNum;

  int32_t code = hJoinCopyResRowsToBlock(pJoin, rowNum, pStart, pRes);
  if (code) {
    pOperator->pTaskInfo->code = code;
    T_LONG_JMP(pOperator->pTaskInfo->env, code);
  }

  pRes->info.rows = resNum;
  *allFetched = pCtx->pBuildRow ? false : true;
}


bool hJoinCopyKeyColsDataToBuf(SHJoinTableCtx* pTable, int32_t rowIdx, size_t *pBufLen) {
  char *pData = NULL;
  size_t bufLen = 0;
  int32_t dataLen = 0;
  
  // NOTE: single-key path does NOT set keyCols[0].bufOffset (it uses keyData directly).
  // The multi-key path below DOES set bufOffset for each key column.
  // Any field read downstream must be initialized in both paths (see calloc in hJoinInitKeyColsInfo).
  if (1 == pTable->keyNum) {
    if (colDataIsNull_s(pTable->keyCols[0].colData, rowIdx)) {
      return true;
    }
    if (pTable->keyCols[0].vardata) {
      pData = pTable->keyCols[0].data + pTable->keyCols[0].colData->varmeta.offset[rowIdx];
      bufLen = calcStrBytesByType(pTable->keyCols[0].colData->info.type, pData);
    } else {
      pData = pTable->keyCols[0].data + pTable->keyCols[0].bytes * rowIdx;
      bufLen = pTable->keyCols[0].bytes;
    }
    pTable->keyData = pData;
  } else {
    for (int32_t i = 0; i < pTable->keyNum; ++i) {
      if (colDataIsNull_s(pTable->keyCols[i].colData, rowIdx)) {
        return true;
      }
      
      pTable->keyCols[i].bufOffset = bufLen;
      
      if (pTable->keyCols[i].vardata) {
        pData = pTable->keyCols[i].data + pTable->keyCols[i].colData->varmeta.offset[rowIdx];
        dataLen = calcStrBytesByType(pTable->keyCols[i].colData->info.type, pData);
        TAOS_MEMCPY(pTable->keyBuf + bufLen, pData, dataLen);
        bufLen += dataLen;
      } else {
        pData = pTable->keyCols[i].data + pTable->keyCols[i].bytes * rowIdx;
        TAOS_MEMCPY(pTable->keyBuf + bufLen, pData, pTable->keyCols[i].bytes);
        bufLen += pTable->keyCols[i].bytes;
      }
    }
    pTable->keyData = pTable->keyBuf;
  }

  if (pBufLen) {
    *pBufLen = bufLen;
  }

  return false;
}

int32_t hJoinSetKeyColsData(SSDataBlock* pBlock, SHJoinTableCtx* pTable) {
  for (int32_t i = 0; i < pTable->keyNum; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pTable->keyCols[i].srcSlot);
    if (NULL == pCol) {
      qError("fail to get %d col, total:%d", pTable->keyCols[i].srcSlot, (int32_t)taosArrayGetSize(pBlock->pDataBlock));
      QRY_ERR_RET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    }
    if (pTable->keyCols[i].vardata != IS_VAR_DATA_TYPE(pCol->info.type))  {
      qError("column type mismatch, idx:%d, slotId:%d, type:%d, vardata:%d", i, pTable->keyCols[i].srcSlot, pCol->info.type, pTable->keyCols[i].vardata);
      return TSDB_CODE_INVALID_PARA;
    }
    if (pTable->keyCols[i].bytes != pCol->info.bytes)  {
      qError("column bytes mismatch, idx:%d, slotId:%d, bytes:%d, %d", i, pTable->keyCols[i].srcSlot, pCol->info.bytes, pTable->keyCols[i].bytes);
      return TSDB_CODE_INVALID_PARA;
    }
    pTable->keyCols[i].data = pCol->pData;
    pTable->keyCols[i].colData = pCol;
  }

  return TSDB_CODE_SUCCESS;
}

/*
 * Sets up value column data pointers (data, bitMap, colData) for the build side
 * from the current data block. Only processes columns that are NOT also key columns
 * (key column values are read from the probe key buffer, not stored separately).
 * Also validates column type and byte size consistency between plan-time and runtime info.
 *
 * @param pBlock  build-side data block
 * @param pTable  build-side table context
 * @return TSDB_CODE_SUCCESS on success, TSDB_CODE_INVALID_PARA on type/size mismatch
 */
static int32_t hJoinSetValColsData(SSDataBlock* pBlock, SHJoinTableCtx* pTable) {
  if (!pTable->valColExist) {
    return TSDB_CODE_SUCCESS;
  }
  for (int32_t i = 0; i < pTable->valNum; ++i) {
    if (IS_HASH_JOIN_KEY_COL(pTable->valCols[i].keyColIdx)) {
      continue;
    }
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, pTable->valCols[i].srcSlot);
    if (NULL == pCol) {
      qError("fail to get %d col, total:%d", pTable->valCols[i].srcSlot, (int32_t)taosArrayGetSize(pBlock->pDataBlock));
      QRY_ERR_RET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    }
    if (pTable->valCols[i].vardata != IS_VAR_DATA_TYPE(pCol->info.type))  {
      qError("column type mismatch, idx:%d, slotId:%d, type:%d, vardata:%d", i, pTable->valCols[i].srcSlot, pCol->info.type, pTable->valCols[i].vardata);
      return TSDB_CODE_INVALID_PARA;
    }
    if (pTable->valCols[i].bytes != pCol->info.bytes)  {
      qError("column bytes mismatch, idx:%d, slotId:%d, bytes:%d, %d", i, pTable->valCols[i].srcSlot, pCol->info.bytes, pTable->valCols[i].bytes);
      return TSDB_CODE_INVALID_PARA;
    }
    if (!pTable->valCols[i].vardata) {
      pTable->valCols[i].bitMap = pCol->nullbitmap;
    }
    pTable->valCols[i].data = pCol->pData;
    pTable->valCols[i].colData = pCol;
  }

  return TSDB_CODE_SUCCESS;
}



/*
 * Serializes the value columns of a single build-side row into the table's valData buffer.
 * Layout in valData: [null_bitmap | fixed_col_1 | fixed_col_2 | ... | varlen_col_1 | ...]
 * The null bitmap is written first (zeroed, then individual bits set for NULL values).
 * Fixed-length columns follow the bitmap; variable-length columns are appended after.
 * Key columns (keyColIdx >= 0) are skipped — their values are in the probe key buffer.
 * No-op if valColExist is false (all output columns are key columns).
 *
 * @param pTable  build-side table context (valData must point to a sufficiently large buffer)
 * @param rowIdx  row index in the current block to serialize
 */
static FORCE_INLINE void hJoinCopyValColsDataToBuf(SHJoinTableCtx* pTable, int32_t rowIdx) {
  if (!pTable->valColExist) {
    return;
  }

  char *pData = NULL;
  size_t bufLen = pTable->valBitMapSize;
  TAOS_MEMSET(pTable->valData, 0, pTable->valBitMapSize);
  for (int32_t i = 0, m = 0; i < pTable->valNum; ++i) {
    if (IS_HASH_JOIN_KEY_COL(pTable->valCols[i].keyColIdx)) {
      continue;
    }
    if (pTable->valCols[i].vardata) {
      if (-1 == pTable->valCols[i].colData->varmeta.offset[rowIdx]) {
        colDataSetNull_f(pTable->valData, m);
      } else {
        pData = pTable->valCols[i].data + pTable->valCols[i].colData->varmeta.offset[rowIdx];
        TAOS_MEMCPY(pTable->valData + bufLen, pData, varDataTLen(pData));
        bufLen += varDataTLen(pData);
      }
    } else {
      if (BMIsNull(pTable->valCols[i].bitMap, rowIdx)) {
        colDataSetNull_f(pTable->valData, m);
      } else {
        pData = pTable->valCols[i].data + pTable->valCols[i].bytes * rowIdx;
        TAOS_MEMCPY(pTable->valData + bufLen, pData, pTable->valCols[i].bytes);
        bufLen += pTable->valCols[i].bytes;
      }
    }
    m++;
  }
}


/*
 * Allocates space for a row's value data from the page pool.
 * Finds a contiguous region of size bufSize in the current page (or allocates a new page
 * if the current page has insufficient space). Returns a pointer to the SBufRowInfo header
 * (ppRow) and to the data area immediately following it (pBuf).
 *
 * The SBufRowInfo header and value data are stored contiguously:
 *   [SBufRowInfo header][value data bytes]
 * ppRow->pageId and ppRow->offset are set so the data can be retrieved later.
 *
 * Constraint: bufSize must not exceed HASH_JOIN_DEFAULT_PAGE_SIZE (10 MB).
 *
 * @param pPages   page pool array
 * @param bufSize  total allocation size (sizeof(SBufRowInfo) + value data size)
 * @param pBuf     output: pointer to the value data area (right after the SBufRowInfo header)
 * @param ppRow    output: pointer to the allocated SBufRowInfo header
 * @return TSDB_CODE_SUCCESS on success, TSDB_CODE_INVALID_PARA if bufSize > page size
 */
static FORCE_INLINE int32_t hJoinGetValBufFromPages(SArray* pPages, int32_t bufSize, char** pBuf, SBufRowInfo** ppRow) {
  if (bufSize > HASH_JOIN_DEFAULT_PAGE_SIZE) {
    qError("invalid join value buf size:%d", bufSize);
    return TSDB_CODE_INVALID_PARA;
  }
  
  do {
    SBufPageInfo* page = taosArrayGetLast(pPages);
    if ((page->pageSize - page->offset) >= bufSize) {
      *ppRow = (SBufRowInfo*)(page->data + page->offset);
      *pBuf = (char*)(*ppRow + 1);
      
      (*ppRow)->pageId = taosArrayGetSize(pPages) - 1;
      (*ppRow)->offset = page->offset + sizeof(SBufRowInfo);
      page->offset += bufSize;
      return TSDB_CODE_SUCCESS;
    }

    int32_t code = hJoinAddPageToBufs(pPages);
    if (code) {
      return code;
    }
  } while (true);
}

/*
 * Calculates the total buffer size needed to store a single row's value data.
 * Returns valBufSize (fixed portion) plus the actual lengths of any variable-length columns
 * in valVarCols[]. Null variable-length columns (offset == -1) contribute 0 bytes.
 * This per-row calculation is needed because variable-length columns have runtime-varying sizes.
 *
 * @param pTable  build-side table context
 * @param rowIdx  row index in the current block
 * @return total byte size required for the row's value data (excluding SBufRowInfo header)
 */
static FORCE_INLINE int32_t hJoinGetValBufSize(SHJoinTableCtx* pTable, int32_t rowIdx) {
  if (NULL == pTable->valVarCols) {
    return pTable->valBufSize;
  }

  int32_t* varColIdx = NULL;
  int32_t bufLen = pTable->valBufSize;
  int32_t varColNum = taosArrayGetSize(pTable->valVarCols);
  for (int32_t i = 0; i < varColNum; ++i) {
    varColIdx = taosArrayGet(pTable->valVarCols, i);
    if (-1 == pTable->valCols[*varColIdx].colData->varmeta.offset[rowIdx]) {
      continue;
    }
    char* pData = pTable->valCols[*varColIdx].data + pTable->valCols[*varColIdx].colData->varmeta.offset[rowIdx];
    bufLen += calcStrBytesByType(pTable->valCols[*varColIdx].colData->info.type, pData);
  }

  return bufLen;
}


/*
 * Core implementation for inserting a single build-side row into the hash table.
 * Allocates a SBufRowInfo node in the page pool and either creates a new hash group
 * or prepends the row to an existing group's linked list.
 *
 * grpSingleRow optimization: if the group already exists and grpSingleRow is true
 * (SEMI/ANTI without ON condition), the row is skipped — only the first row per key
 * is needed since SEMI/ANTI semantics only require existence, not all matches.
 *
 * Note: the new row is prepended (not appended) to the linked list, so the order of
 * rows within a group is reversed relative to the build-side input order.
 *
 * @param pJoin    hash join operator info
 * @param pGroup   existing group in the hash table, or NULL for a new group
 * @param pTable   build-side table context (provides keyData and valData for the row)
 * @param keyLen   length of the serialized key
 * @param rowIdx   row index in the build block (used for valBufSize calculation)
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
static int32_t hJoinAddRowToHashImpl(SHJoinOperatorInfo* pJoin, SGroupData* pGroup, SHJoinTableCtx* pTable, size_t keyLen, int32_t rowIdx) {
  SGroupData group = {0};
  SBufRowInfo* pRow = NULL;

  if (pGroup && pJoin->ctx.grpSingleRow) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = hJoinGetValBufFromPages(pJoin->pRowBufs, hJoinGetValBufSize(pTable, rowIdx) + sizeof(SBufRowInfo), &pTable->valData, &pRow);
  if (code) {
    return code;
  }

  if (NULL == pGroup) {
    pRow->next = NULL;
    group.rows = pRow;
    if (tSimpleHashPut(pJoin->pKeyHash, pTable->keyData, keyLen, &group, sizeof(group))) {
      taosMemoryFree(pRow);
      return terrno;
    }
  } else {
    pRow->next = pGroup->rows;
    pGroup->rows = pRow;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t hJoinAddRowToFullHashImpl(SHJoinOperatorInfo* pJoin, SFGroupData* pGroup, SHJoinTableCtx* pTable, size_t keyLen,
                                         int32_t rowIdx) {
  SFGroupData group = {0};
  SBufRowInfo* pRow = NULL;

  int32_t code = hJoinGetValBufFromPages(pJoin->pRowBufs, hJoinGetValBufSize(pTable, rowIdx) + sizeof(SBufRowInfo), &pTable->valData, &pRow);
  if (code) {
    return code;
  }

  if (NULL == pGroup) {
    pRow->grpRowIdx = 0;
    pRow->next = NULL;
    group.rows = pRow;
    group.bitmap = NULL;
    group.rowsNum = 1;
    group.rowsMatchNum = 0;
    group.keyOffsets = NULL;
    group.keyOffsetNum = 0;

    if (tSimpleHashPut(pJoin->pKeyHash, pTable->keyData, keyLen, &group, sizeof(group))) {
      return terrno;
    }
  } else {
    pRow->grpRowIdx = pGroup->rowsNum;
    pRow->next = pGroup->rows;
    pGroup->rows = pRow;
    pGroup->rowsNum += 1;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t hJoinAddRowToHash(SHJoinOperatorInfo* pJoin, SSDataBlock* pBlock, size_t keyLen, int32_t rowIdx) {
  SHJoinTableCtx* pBuild = pJoin->pBuild;
  int32_t code = hJoinSetValColsData(pBlock, pBuild);
  if (code) {
    return code;
  }

  SGroupData* pGroup = tSimpleHashGet(pJoin->pKeyHash, pBuild->keyData, keyLen);
  if (JOIN_TYPE_FULL == pJoin->joinType) {
    code = hJoinAddRowToFullHashImpl(pJoin, (SFGroupData*)pGroup, pBuild, keyLen, rowIdx);
  } else {
    code = hJoinAddRowToHashImpl(pJoin, pGroup, pBuild, keyLen, rowIdx);
  }
  if (code) {
    return code;
  }
  
  hJoinCopyValColsDataToBuf(pBuild, rowIdx);

  return TSDB_CODE_SUCCESS;
}

bool hJoinFilterTimeRange(SHJoinCtx* pCtx, SSDataBlock* pBlock, STimeWindow* pRange, int32_t primSlot, int32_t* startIdx, int32_t* endIdx) {
  SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, primSlot);
  if (NULL == pCol) {
    qError("hash join can't get prim col, slot:%d, slotNum:%d", primSlot, (int32_t)taosArrayGetSize(pBlock->pDataBlock));
    return false;
  }

  TSKEY skey = pCtx->ascTs ? (*(TSKEY*)colDataGetData(pCol, 0)) : (*(TSKEY*)colDataGetData(pCol, (pBlock->info.rows - 1)));
  TSKEY ekey = pCtx->ascTs ? (*(TSKEY*)colDataGetData(pCol, (pBlock->info.rows - 1))) : (*(TSKEY*)colDataGetData(pCol, 0));

  if (ekey < pRange->skey || skey > pRange->ekey) {
    return false;
  }

  if (skey >= pRange->skey && ekey <= pRange->ekey) {
    *startIdx = 0;
    *endIdx = pBlock->info.rows - 1;
    return true;
  }

  if (pCtx->ascTs) {
    if (skey < pRange->skey && ekey > pRange->ekey) {
      TSKEY *pStart = (TSKEY*)taosbsearch(&pRange->skey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_GE);
      TSKEY *pEnd = (TSKEY*)taosbsearch(&pRange->ekey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_LE);
      *startIdx = ((uint64_t)pStart - (uint64_t)pCol->pData) / sizeof(int64_t);
      *endIdx = ((uint64_t)pEnd - (uint64_t)pCol->pData) / sizeof(int64_t);
      return true;
    }

    if (skey >= pRange->skey) {
      TSKEY *pEnd = (TSKEY*)taosbsearch(&pRange->ekey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_LE);
      *startIdx = 0;
      *endIdx = ((uint64_t)pEnd - (uint64_t)pCol->pData) / sizeof(int64_t);
      return true;
    }

    TSKEY *pStart = (TSKEY*)taosbsearch(&pRange->skey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_GE);
    *startIdx = ((uint64_t)pStart - (uint64_t)pCol->pData) / sizeof(int64_t);
    *endIdx = pBlock->info.rows - 1;
    
    return true;
  }

  if (skey < pRange->skey && ekey > pRange->ekey) {
    TSKEY *pStart = (TSKEY*)taosrbsearch(&pRange->skey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_GE);
    TSKEY *pEnd = (TSKEY*)taosrbsearch(&pRange->ekey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_LE);
    *startIdx = ((uint64_t)pEnd - (uint64_t)pCol->pData) / sizeof(int64_t);
    *endIdx = ((uint64_t)pStart - (uint64_t)pCol->pData) / sizeof(int64_t);
    return true;
  }
  
  if (skey >= pRange->skey) {
    TSKEY *pEnd = (TSKEY*)taosrbsearch(&pRange->ekey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_LE);
    *startIdx = ((uint64_t)pEnd - (uint64_t)pCol->pData) / sizeof(int64_t);
    *endIdx = pBlock->info.rows - 1;
    return true;
  }
  
  TSKEY *pStart = (TSKEY*)taosrbsearch(&pRange->skey, pCol->pData, pBlock->info.rows, sizeof(TSKEY), compareInt64Val, TD_GE);
  *startIdx = 0;
  *endIdx = ((uint64_t)pStart - (uint64_t)pCol->pData) / sizeof(int64_t);
  
  return true;
}

/*
 * Processes all rows in a single build-side block and inserts them into the hash table.
 * Applies time range filtering via hJoinFilterTimeRange (if hasTimeRange is true),
 * evaluates key expressions via hJoinLaunchEqualExpr, sets up key column data pointers,
 * then iterates over the filtered row range and calls hJoinAddRowToHash for each row.
 * Rows with NULL keys are automatically skipped by hJoinCopyKeyColsDataToBuf.
 *
 * @param pBlock  build-side data block to process
 * @param pJoin   hash join operator info
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
static int32_t hJoinAddBlockRowsToHash(SSDataBlock* pBlock, SHJoinOperatorInfo* pJoin) {
  SHJoinTableCtx* pBuild = pJoin->pBuild;
  int32_t startIdx = 0, endIdx = pBlock->info.rows - 1;
  if (pBuild->hasTimeRange && !hJoinFilterTimeRange(&pJoin->ctx, pBlock, &pJoin->tblTimeRange, pBuild->primCol->srcSlot, &startIdx, &endIdx)) {
    return TSDB_CODE_SUCCESS;
  }

  HJ_ERR_RET(hJoinLaunchEqualExpr(pJoin->pOperator, pBlock, pBuild, startIdx, endIdx));

  int32_t code = hJoinSetKeyColsData(pBlock, pBuild);
  if (code) {
    return code;
  }

  size_t bufLen = 0;
  for (int32_t i = startIdx; i <= endIdx; ++i) {
    if (hJoinCopyKeyColsDataToBuf(pBuild, i, &bufLen)) {
      continue;
    }
    code = hJoinAddRowToHash(pJoin, pBlock, bufLen, i);
    if (code) {
      return code;
    }
  }

  return code;
}

int32_t hJoinBuildHash(struct SOperatorInfo* pOperator, bool* returnDirect) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SSDataBlock*        pBlock = NULL;
  int32_t             code = TSDB_CODE_SUCCESS;

  while (true) {
    pBlock = getNextBlockFromDownstream(pOperator, pJoin->pBuild->downStreamIdx);
    if (NULL == pBlock) {
      break;
    }

    pJoin->execInfo.buildBlkNum++;
    pJoin->execInfo.buildBlkRows += pBlock->info.rows;

    code = hJoinAddBlockRowsToHash(pBlock, pJoin);
    if (code) {
      return code;
    }
  }

  if (tSimpleHashGetSize(pJoin->pKeyHash) <= 0) {
    if (IS_INNER_NONE_JOIN(pJoin->joinType, pJoin->subType) || IS_SEMI_JOIN(pJoin->subType)) {
      hJoinSetDone(pOperator);
      *returnDirect = true;
    }
    
    tSimpleHashCleanup(pJoin->pKeyHash);
    pJoin->pKeyHash = NULL;
  }
  
#if 0  
  qTrace("build table rows:%" PRId64, hJoinGetRowsNumOfKeyHash(pJoin->pKeyHash));
#endif

  pJoin->keyHashBuilt = true;

  return TSDB_CODE_SUCCESS;
}

/*
 * Prepares the probe context for a new probe block and dispatches to joinFp.
 * Steps:
 *   1. Evaluates key expressions on the probe block.
 *   2. If the hash table is NULL (empty build side): sets probePhase=POST and dispatches
 *      so non-matching probe rows can still be emitted (for LEFT/ANTI/FULL joins).
 *   3. Applies time range filter to determine [startIdx, endIdx] for the CUR phase.
 *   4. Sets up key and value column data pointers for the probe block.
 *   5. Initializes probePreIdx, probeStartIdx, probeEndIdx, probePostIdx.
 *   6. If startIdx > 0 and join needs non-matching rows: sets probePhase=PRE.
 *      Otherwise: sets probePhase=CUR.
 *   7. Calls joinFp.
 *
 * @param pOperator  the hash join operator
 * @param pBlock     probe-side data block to process
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
static int32_t hJoinPrepareStart(struct SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SHJoinTableCtx* pProbe = pJoin->pProbe;
  int32_t startIdx = 0, endIdx = pBlock->info.rows - 1;

  HJ_ERR_RET(hJoinLaunchEqualExpr(pOperator, pBlock, pProbe, startIdx, endIdx));

  if (NULL == pJoin->pKeyHash) {
    pJoin->ctx.probeEndIdx = -1;
    pJoin->ctx.probePostIdx = 0;
    pJoin->ctx.pProbeData = pBlock;
    pJoin->ctx.rowRemains = true;
    pJoin->ctx.probePhase = E_JOIN_PHASE_POST;
    
    return (*pJoin->joinFp)(pOperator);
  }
  
  if (pProbe->hasTimeRange && !hJoinFilterTimeRange(&pJoin->ctx, pBlock, &pJoin->tblTimeRange, pProbe->primCol->srcSlot, &startIdx, &endIdx)) {
    if (IS_NEED_NMATCH_JOIN(pJoin->joinType, pJoin->subType)) {
      pJoin->ctx.probeEndIdx = -1;
      pJoin->ctx.probePostIdx = 0;
      pJoin->ctx.pProbeData = pBlock;
      pJoin->ctx.rowRemains = true;
      pJoin->ctx.probePhase = E_JOIN_PHASE_POST;
      
      HJ_ERR_RET((*pJoin->joinFp)(pOperator));
    }
    
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = hJoinSetKeyColsData(pBlock, pProbe);
  if (code) {
    return code;
  }
  code = hJoinSetValColsData(pBlock, pProbe);
  if (code) {
    return code;
  }

  pJoin->ctx.probeStartIdx = startIdx;
  pJoin->ctx.probeEndIdx = endIdx;
  pJoin->ctx.pBuildRow = NULL;
  pJoin->ctx.pProbeData = pBlock;
  pJoin->ctx.rowRemains = true;
  pJoin->ctx.probePreIdx = 0;
  pJoin->ctx.probePostIdx = endIdx + 1;

  if (IS_NEED_NMATCH_JOIN(pJoin->joinType, pJoin->subType) && startIdx > 0) {
    pJoin->ctx.probePhase = E_JOIN_PHASE_PRE;
  } else {
    pJoin->ctx.probePhase = E_JOIN_PHASE_CUR;
  }

  HJ_ERR_RET((*pJoin->joinFp)(pOperator));

  return TSDB_CODE_SUCCESS;
}

void hJoinSetDone(struct SOperatorInfo* pOperator) {
  setOperatorCompleted(pOperator);

  SHJoinOperatorInfo* pInfo = pOperator->info;
  if (JOIN_TYPE_FULL == pInfo->joinType) {
    hJoinFreeFullGroupBitmap(pInfo->pKeyHash, &pInfo->ctx);
  }
  //hJoinDestroyKeyHash(&pInfo->pKeyHash);
  tSimpleHashCleanup(pInfo->pKeyHash);
  pInfo->pKeyHash = NULL;

  //blockDataDestroy(pInfo->finBlk);
  //pInfo->finBlk = NULL;
  blockDataDestroy(pInfo->midBlk);
  pInfo->midBlk = NULL;

  taosArrayDestroyEx(pInfo->pRowBufs, hJoinFreeBufPage);
  pInfo->pRowBufs = NULL;

  qDebug("hash Join done");  
}

/*
 * Main entry point for hash join execution, called repeatedly by the upstream operator.
 * On each call:
 *   1. Build phase (first call only): calls buildFp to populate the hash table.
 *      If buildFp sets returnDirect=true (e.g. FULL JOIN emitted rows during build),
 *      returns the result block immediately.
 *   2. Main loop:
 *      a. If midRemains: swap midBlk/finBlk to drain leftover pPreFilter rows.
 *      b. If rowRemains: resume joinFp to continue processing the current probe block.
 *      c. Otherwise: fetch the next probe block and call hJoinPrepareStart.
 *   3. Applies pFinFilter (WHERE clause) to the output block before returning.
 *   4. When all probe blocks are exhausted: calls hJoinSetDone to release resources.
 *
 * Returns the result block via *pResBlock when it has rows. Returns NULL when done.
 *
 * @param pOperator   the hash join operator
 * @param pResBlock   output: pointer set to finBlk when rows are available
 * @return TSDB_CODE_SUCCESS on success, error code on failure (also longjmp on fatal error)
 */
static int32_t hJoinMainProcess(struct SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  SHJoinOperatorInfo* pJoin = pOperator->info;
  SExecTaskInfo*      pTaskInfo = pOperator->pTaskInfo;
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SSDataBlock*        pRes = pJoin->finBlk;

  QRY_PARAM_CHECK(pResBlock);

  if (pOperator->status == OP_EXEC_DONE) {
    pRes->info.rows = 0;
    goto _exit;
  }

  blockDataCleanup(pRes);

  if (!pJoin->keyHashBuilt) {
    bool returnDirect = false;
    TAOS_CHECK_EXIT((*pJoin->buildFp)(pOperator, &returnDirect));

    if (returnDirect) {
      goto _exit;
    }
  }
  
  while (true) {
    if (pJoin->ctx.midRemains) {
      TAOS_CHECK_EXIT(hJoinHandleMidRemains(pJoin));
      
      pRes = pJoin->finBlk;
      if (hJoinBlkReachThreshold(pJoin, pRes->info.rows)) {
        goto _exit;
      }
    }
    
    if (pJoin->ctx.rowRemains) {
      TAOS_CHECK_EXIT(hJoinHandleRowRemains(pOperator, pJoin));
      
      if (pRes->info.rows > 0) {
        goto _exit;
      }

      continue;
    }
    
    SSDataBlock* pBlock = NULL;
    if (!pJoin->ctx.probeDone) {
      pBlock = getNextBlockFromDownstream(pOperator, pJoin->pProbe->downStreamIdx);
      if (NULL == pBlock) {
        pJoin->ctx.probeDone = true;
      }
    }

    if (pJoin->ctx.probeDone) {
      if (pJoin->probeEndFp != NULL) {
        bool needBreak = false;
        bool needContinue = false;
        TAOS_CHECK_EXIT((*pJoin->probeEndFp)(pJoin, &needBreak, &needContinue));

        if (needBreak) {
          break;
        }

        if (needContinue) {
          continue;
        }
      }

      hJoinSetDone(pOperator);

      if (pRes->info.rows > 0 && pJoin->pFinFilter != NULL) {
        TAOS_CHECK_EXIT(doFilter(pRes, pJoin->pFinFilter, NULL, NULL));
      }

      break;
    }

    pJoin->execInfo.probeBlkNum++;
    pJoin->execInfo.probeBlkRows += pBlock->info.rows;

    TAOS_CHECK_EXIT(hJoinPrepareStart(pOperator, pBlock));

    if (!hJoinBlkReachThreshold(pJoin, pRes->info.rows)) {
      continue;
    }

    if (pRes->info.rows > 0 && pJoin->pFinFilter != NULL) {
      TAOS_CHECK_EXIT(doFilter(pRes, pJoin->pFinFilter, NULL, NULL));
    }

    if (pRes->info.rows > 0) {
      break;
    }
  }

_exit:
  
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  
  if (pRes->info.rows > 0) {
    *pResBlock = pRes;
    qDebug("%s %s output %" PRId64 " rows final res", GET_TASKID(pTaskInfo), __func__, pRes->info.rows);
    printDataBlock(*pResBlock, __func__, GET_TASKID(pTaskInfo), pTaskInfo->id.queryId);
  }

  return code;
}

/*
 * Destructor for the hash join operator. Logs execution statistics, then frees all
 * resources: hash table, table contexts, finBlk, midBlk, result column map, and page pool.
 * Called by the operator framework when the query completes or is cancelled.
 *
 * Note: pRowBufs holds page-pool pages that contain SBufRowInfo nodes; freeing pRowBufs
 * implicitly frees all row buffer memory without needing to traverse linked lists.
 *
 * @param param  pointer to SHJoinOperatorInfo (cast from void*)
 */
static void destroyHashJoinOperator(void* param) {
  SHJoinOperatorInfo* pJoinOperator = (SHJoinOperatorInfo*)param;
  qDebug("hashJoin exec info, buildBlk:%" PRId64 ", buildRows:%" PRId64 ", probeBlk:%" PRId64 ", probeRows:%" PRId64 ", resRows:%" PRId64, 
         pJoinOperator->execInfo.buildBlkNum, pJoinOperator->execInfo.buildBlkRows, pJoinOperator->execInfo.probeBlkNum, 
         pJoinOperator->execInfo.probeBlkRows, pJoinOperator->execInfo.resRows);

  if (JOIN_TYPE_FULL == pJoinOperator->joinType) {
    hJoinFreeFullGroupBitmap(pJoinOperator->pKeyHash, &pJoinOperator->ctx);
  }
  //hJoinDestroyKeyHash(&pJoinOperator->pKeyHash);
  tSimpleHashCleanup(pJoinOperator->pKeyHash);
  pJoinOperator->pKeyHash = NULL;

  hJoinFreeTableInfo(&pJoinOperator->tbs[0]);
  hJoinFreeTableInfo(&pJoinOperator->tbs[1]);
  
  blockDataDestroy(pJoinOperator->finBlk);
  pJoinOperator->finBlk = NULL;
  blockDataDestroy(pJoinOperator->midBlk);
  pJoinOperator->midBlk = NULL;
  
  taosMemoryFreeClear(pJoinOperator->pResColMap);

  taosArrayDestroyEx(pJoinOperator->pRowBufs, hJoinFreeBufPage);
  pJoinOperator->pRowBufs = NULL;

  taosMemoryFreeClear(param);
}

/*
 * Initializes pPreFilter and pFinFilter from the physical plan node's condition expressions.
 * The filter assignment differs by join type:
 *
 *   INNER: pFullOnCond (non-equi ON condition) and pConditions (WHERE) are merged into
 *          a single pFinFilter applied after result construction.
 *
 *   LEFT/RIGHT/FULL: pFullOnCond → pPreFilter (applied to midBlk before merge into finBlk,
 *          to determine whether a probe row has a matching build row after the ON condition).
 *          pConditions → pFinFilter (applied to finBlk before returning to upstream).
 *
 * Rationale: For outer joins, the ON condition must be tested at the per-row level so that
 * unmatched probe rows can still be emitted with NULL build columns. For INNER join, all
 * conditions can be combined into a single post-filter since there are no non-matching rows.
 *
 * @param pJoin      hash join operator info (pPreFilter and pFinFilter are set here)
 * @param pJoinNode  physical plan node with condition expressions
 * @param pTaskInfo  task info (for stream runtime info passed to filterInitFromNode)
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hJoinHandleConds(SHJoinOperatorInfo* pJoin, SHashJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo) {
  switch (pJoin->joinType) {
    case JOIN_TYPE_INNER: {
      SNode* pCond = NULL;
      if (pJoinNode->pFullOnCond != NULL) {
        if (pJoinNode->node.pConditions != NULL) {
          HJ_ERR_RET(mergeJoinConds(&pJoinNode->pFullOnCond, &pJoinNode->node.pConditions));
        }
        pCond = pJoinNode->pFullOnCond;
      } else if (pJoinNode->node.pConditions != NULL) {
        pCond = pJoinNode->node.pConditions;
      }

      HJ_ERR_RET(filterInitFromNode(pCond, &pJoin->pFinFilter, 0, pTaskInfo->pStreamRuntimeInfo));
      break;
    }
    case JOIN_TYPE_LEFT:
    case JOIN_TYPE_RIGHT:
    case JOIN_TYPE_FULL:
      if (pJoinNode->pFullOnCond != NULL) {
        HJ_ERR_RET(filterInitFromNode(pJoinNode->pFullOnCond, &pJoin->pPreFilter, 0,
                                      pTaskInfo->pStreamRuntimeInfo));
      }
      if (pJoinNode->node.pConditions != NULL) {
        HJ_ERR_RET(filterInitFromNode(pJoinNode->node.pConditions, &pJoin->pFinFilter, 0,
                                      pTaskInfo->pStreamRuntimeInfo));
      }
      break;
    default:
      break;
  }

  return TSDB_CODE_SUCCESS;
}

/*
 * Calculates the row capacity of the final output block (finBlk).
 * The capacity is capped at the smaller of:
 *   - HJOIN_DEFAULT_BLK_ROWS_NUM or HJOIN_BLK_SIZE_LIMIT / row_size (size-based limit)
 *   - limit / HJOIN_BLK_THRESHOLD_RATIO + 1 (LIMIT-based limit, only when no pFinFilter)
 *
 * The LIMIT-based cap avoids over-allocating finBlk when the query has a small LIMIT
 * and no post-filter (since the block is yielded at HJOIN_BLK_THRESHOLD_RATIO * capacity).
 *
 * @param pJoin      hash join operator info (ctx.limit and pFinFilter)
 * @param pJoinNode  physical plan node (output row size)
 * @return row capacity for finBlk
 */
static uint32_t hJoinGetFinBlkCapacity(SHJoinOperatorInfo* pJoin, SHashJoinPhysiNode* pJoinNode) {
  uint32_t maxRows = TMAX(HJOIN_DEFAULT_BLK_ROWS_NUM, HJOIN_BLK_SIZE_LIMIT/pJoinNode->node.pOutputDataBlockDesc->totalRowSize);
  if (INT64_MAX != pJoin->ctx.limit && NULL == pJoin->pFinFilter) {
    uint32_t limitMaxRows = pJoin->ctx.limit / HJOIN_BLK_THRESHOLD_RATIO + 1;
    return TMIN(maxRows, limitMaxRows);
  }

  return maxRows;
}


/*
 * Allocates and initializes the output blocks (finBlk and midBlk).
 * finBlk is always created. midBlk is created only when pPreFilter is present,
 * as the two-block strategy is needed only for non-equi ON condition processing.
 * Both blocks get the same capacity (computed by hJoinGetFinBlkCapacity).
 * blkThreshold is set to capacity * HJOIN_BLK_THRESHOLD_RATIO for yield decisions.
 *
 * @param pJoin      hash join operator info
 * @param pJoinNode  physical plan node (output schema and optional LIMIT)
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hJoinInitResBlocks(SHJoinOperatorInfo* pJoin, SHashJoinPhysiNode* pJoinNode) {
  pJoin->finBlk = createDataBlockFromDescNode(pJoinNode->node.pOutputDataBlockDesc);
  if (NULL == pJoin->finBlk) {
    QRY_ERR_RET(terrno);
  }

  int32_t code = blockDataEnsureCapacity(pJoin->finBlk, hJoinGetFinBlkCapacity(pJoin, pJoinNode));
  if (TSDB_CODE_SUCCESS != code) {
    QRY_ERR_RET(code);
  }
  
  if (NULL != pJoin->pPreFilter) {
    pJoin->midBlk = NULL;
    code = createOneDataBlock(pJoin->finBlk, false, &pJoin->midBlk);
    if (code) {
      QRY_ERR_RET(code);
    }

    code = blockDataEnsureCapacity(pJoin->midBlk, pJoin->finBlk->info.capacity);
    if (TSDB_CODE_SUCCESS != code) {
      QRY_ERR_RET(code);
    }
  }

  pJoin->blkThreshold = pJoin->finBlk->info.capacity * HJOIN_BLK_THRESHOLD_RATIO;
  return TSDB_CODE_SUCCESS;
}

/*
 * Initializes the per-block execution context (SHJoinCtx) from the physical plan node.
 * Sets:
 *   - limit: from the LIMIT clause (INT64_MAX if none)
 *   - ascTs: true if input timestamps are ascending (ORDER_ASC or default)
 *   - grpSingleRow: true for SEMI/ANTI without ON condition — enables the optimization
 *     that skips inserting duplicate keys during the build phase (at most one row per key)
 *
 * @param pCtx       join context to initialize
 * @param pJoinNode  physical plan node (limit, input order, subType, ON condition)
 * @return TSDB_CODE_SUCCESS
 */
int32_t hJoinInitJoinCtx(SHJoinCtx* pCtx, SHashJoinPhysiNode* pJoinNode) {
  pCtx->limit = (pJoinNode->node.pLimit && ((SLimitNode*)pJoinNode->node.pLimit)->limit) ? ((SLimitNode*)pJoinNode->node.pLimit)->limit->datum.i : INT64_MAX;
  if (pJoinNode->node.inputTsOrder != ORDER_DESC) {
    pCtx->ascTs = true;
  }

  pCtx->grpSingleRow = (NULL == pJoinNode->pFullOnCond) && (JOIN_STYPE_SEMI == pJoinNode->subType || JOIN_STYPE_ANTI == pJoinNode->subType);

  return TSDB_CODE_SUCCESS;
}

/*
 * Resets all hash join operator state for re-execution (e.g. for streaming or re-scan).
 * Clears keyHashBuilt, resets block data, clears execInfo statistics, destroys the old
 * hash table and row buffer pool, then reinitializes them for the next build phase.
 * Preserves ctx.limit (set once at creation) while resetting all other ctx fields.
 *
 * @param pOper  the hash join operator
 * @return TSDB_CODE_SUCCESS on success, error code if hash table or page pool re-init fails
 */
static int32_t resetHashJoinOperState(SOperatorInfo* pOper) {
  SHJoinOperatorInfo* pHjOper = pOper->info;
  pHjOper->keyHashBuilt = false;
  blockDataCleanup(pHjOper->midBlk);
  blockDataCleanup(pHjOper->finBlk);
  pOper->status = OP_NOT_OPENED;

  pHjOper->execInfo = (SHJoinExecInfo){0};

  void*   pIte = NULL;
  int32_t iter = 0;
  while ((pIte = tSimpleHashIterate(pHjOper->pKeyHash, pIte, &iter)) != NULL) {
    SGroupData* pGroup = pIte;
    SBufRowInfo* pRow = pGroup->rows;
    SBufRowInfo* pNext = NULL;
    while (pRow) {
      pNext = pRow->next;
      taosMemoryFree(pRow);
      pRow = pNext;
    }
  }
  if (JOIN_TYPE_FULL == pHjOper->joinType) {
    hJoinFreeFullGroupBitmap(pHjOper->pKeyHash, &pHjOper->ctx);
  }
  tSimpleHashCleanup(pHjOper->pKeyHash);
  size_t hashCap = pHjOper->pBuild->inputStat.inputRowNum > 0 ? (pHjOper->pBuild->inputStat.inputRowNum * 1.5) : 1024;
  pHjOper->pKeyHash = tSimpleHashInit(hashCap, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (pHjOper->pKeyHash == NULL) {
    return terrno; 
  }
  taosArrayDestroyEx(pHjOper->pRowBufs, hJoinFreeBufPage);
  int32_t code = hJoinInitBufPages(pHjOper);
  int64_t limit = pHjOper->ctx.limit;
  pHjOper->ctx = (SHJoinCtx){0};
  pHjOper->ctx.limit = limit;
  return code;
}

/*
 * Allocates and fully initializes a hash join operator. Initialization order:
 *   1. hJoinInitJoinCtx: LIMIT, timestamp order, grpSingleRow flag
 *   2. hJoinSetBuildAndProbeTable: assigns pBuild/pProbe based on join type
 *   3. hJoinInitTableInfo x2: key/value columns, expressions for each side
 *   4. hJoinBuildResColsMap: per-column build/probe routing array
 *   5. hJoinInitBufPages: page pool for row value storage
 *   6. tSimpleHashInit: hash table sized from build-side row count estimate
 *   7. hJoinHandleConds: pPreFilter and pFinFilter from plan conditions
 *   8. hJoinInitResBlocks: finBlk and midBlk output blocks
 *   9. hJoinSetImplFp: binds joinFp and buildFp to the correct join-type functions
 *   10. appendDownstream: registers downstream operators
 *   11. Sets fpSet with hJoinMainProcess as the main operator function
 *       and resetHashJoinOperState for re-execution support.
 *
 * On any initialization failure, calls destroyHashJoinOperator and destroyOperatorAndDownstreams
 * to clean up all partially initialized resources.
 *
 * @param pDownstream      array of downstream operator pointers
 * @param numOfDownstream  number of downstream operators (must be 2)
 * @param pJoinNode        physical plan node with all join configuration
 * @param pTaskInfo        task info for expression context and error reporting
 * @param pOptrInfo        output: initialized SOperatorInfo pointer
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t createHashJoinOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                           SHashJoinPhysiNode* pJoinNode, SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t             code = TSDB_CODE_SUCCESS;
  SHJoinOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SHJoinOperatorInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pOperator == NULL || pInfo == NULL) {
    code = terrno;
    goto _return;
  }
  initOperatorCostInfo(pOperator);

  pInfo->tblTimeRange.skey = pJoinNode->timeRange.skey;
  pInfo->tblTimeRange.ekey = pJoinNode->timeRange.ekey;
  pInfo->pOperator = pOperator;

  HJ_ERR_JRET(hJoinInitJoinCtx(&pInfo->ctx, pJoinNode));
  
  setOperatorInfo(pOperator, "HashJoinOperator", QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, false, OP_NOT_OPENED, pInfo, pTaskInfo);

  hJoinSetBuildAndProbeTable(pInfo, pJoinNode);

  HJ_ERR_JRET(hJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 0, &pJoinNode->inputStat[0]));
  HJ_ERR_JRET(hJoinInitTableInfo(pInfo, pJoinNode, pDownstream, 1, &pJoinNode->inputStat[1]));
  
  HJ_ERR_JRET(hJoinBuildResColsMap(pInfo, pJoinNode));

  HJ_ERR_JRET(hJoinInitBufPages(pInfo));

  size_t hashCap = pInfo->pBuild->inputStat.inputRowNum > 0 ? (pInfo->pBuild->inputStat.inputRowNum * 1.5) : 1024;
  pInfo->pKeyHash = tSimpleHashInit(hashCap, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (pInfo->pKeyHash == NULL) {
    code = terrno;
    goto _return;
  }

  HJ_ERR_JRET(hJoinHandleConds(pInfo, pJoinNode, pTaskInfo));

  HJ_ERR_JRET(hJoinInitResBlocks(pInfo, pJoinNode));

  HJ_ERR_JRET(hJoinSetImplFp(pInfo));

  HJ_ERR_JRET(appendDownstream(pOperator, pDownstream, numOfDownstream));

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, hJoinMainProcess, NULL, destroyHashJoinOperator, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetHashJoinOperState);

  qDebug("create hash Join operator done");

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_return:

  if (pInfo != NULL) {
    destroyHashJoinOperator(pInfo);
  }
  destroyOperatorAndDownstreams(pOperator, pDownstream, numOfDownstream);
  pTaskInfo->code = code;
  return code;
}


