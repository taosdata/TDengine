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

#include "executor.h"
#include "executorInt.h"
#include "filter.h"
#include "operator.h"
#include "plannodes.h"
#include "query.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tutil.h"

// ────────────────────────────────────────────────────────────────────────────────
// Internal state
// ────────────────────────────────────────────────────────────────────────────────

typedef struct SRowsetSourceInfo {
  SArray*   pBlocks;    // SArray<SSDataBlock*> — pre-deserialized blocks
  int32_t   curBlock;   // next block index to return
} SRowsetSourceInfo;

// ────────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────────

static void destroyRowsetSourceOperatorInfo(void* param) {
  if (param == NULL) return;
  SRowsetSourceInfo* pInfo = (SRowsetSourceInfo*)param;
  if (pInfo->pBlocks != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->pBlocks); ++i) {
      SSDataBlock** ppBlock = taosArrayGet(pInfo->pBlocks, i);
      if (ppBlock != NULL && *ppBlock != NULL) {
        blockDataDestroy(*ppBlock);
      }
    }
    taosArrayDestroy(pInfo->pBlocks);
  }
  taosMemoryFree(pInfo);
}

// ────────────────────────────────────────────────────────────────────────────────
// Operator callbacks
// ────────────────────────────────────────────────────────────────────────────────

static int32_t doRowsetSourceNext(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  SRowsetSourceInfo* pInfo = pOperator->info;

  while (pInfo->curBlock < (int32_t)taosArrayGetSize(pInfo->pBlocks)) {
    SSDataBlock** ppBlock = taosArrayGet(pInfo->pBlocks, pInfo->curBlock);
    pInfo->curBlock++;
    if (ppBlock == NULL || *ppBlock == NULL) continue;

    SSDataBlock* pBlock = *ppBlock;
    // Apply WHERE conditions if any
    if (pOperator->exprSupp.pFilterInfo != NULL) {
      int32_t code = doFilter(pBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL);
      if (code != TSDB_CODE_SUCCESS) return code;
      if (pBlock->info.rows == 0) continue;  // all rows filtered out, try next block
    }
    *pResBlock = pBlock;
    return TSDB_CODE_SUCCESS;
  }

  // Exhausted: signal end-of-stream
  setOperatorCompleted(pOperator);
  *pResBlock = NULL;
  return TSDB_CODE_SUCCESS;
}

// ────────────────────────────────────────────────────────────────────────────────
// Deserialization: length-prefix multi-block binary → SArray<SSDataBlock*>
// Format: [uint32_t blockSize][blockDataToBuf output] repeated numBlocks times
// ────────────────────────────────────────────────────────────────────────────────

static int32_t deserializeRowsetBlocks(const SRowsetSourcePhysiNode* pPhyNode,
                                       SArray** ppBlocks) {
  int32_t code      = TSDB_CODE_SUCCESS;
  int32_t numBlocks = pPhyNode->numBlocks;

  SArray* pArr = taosArrayInit(numBlocks, POINTER_BYTES);
  if (pArr == NULL) return terrno;

  if (pPhyNode->pBlockBuf == NULL || pPhyNode->blockBufLen <= 0 || numBlocks <= 0) {
    *ppBlocks = pArr;
    return TSDB_CODE_SUCCESS;
  }

  const uint8_t* pCur = pPhyNode->pBlockBuf;
  const uint8_t* pEnd = pPhyNode->pBlockBuf + pPhyNode->blockBufLen;

  SDataBlockDescNode* pDesc = pPhyNode->node.pOutputDataBlockDesc;

  for (int32_t i = 0; i < numBlocks; ++i) {
    if (pCur + sizeof(uint32_t) > pEnd) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _error;
    }
    uint32_t blockSize = 0;
    memcpy(&blockSize, pCur, sizeof(uint32_t));
    pCur += sizeof(uint32_t);

    if (pCur + blockSize > pEnd) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _error;
    }

    // Build an empty block with the declared schema from the physi node's output descriptor
    SSDataBlock* pBlock = createDataBlockFromDescNode((void*)pDesc);
    if (pBlock == NULL) {
      code = terrno;
      goto _error;
    }

    code = blockDataFromBuf(pBlock, (const char*)pCur);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
      goto _error;
    }
    pCur += blockSize;

    // blockDataFromBuf doesn't restore hasNull flag; do it now
    int32_t numCols = (int32_t)taosArrayGetSize(pBlock->pDataBlock);
    for (int32_t c = 0; c < numCols; ++c) {
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, c);
      if (pCol == NULL) continue;
      if (IS_VAR_DATA_TYPE(pCol->info.type)) {
        // VAR-length types encode NULL as varmeta.offset[r] == -1
        if (pCol->varmeta.offset) {
          for (int32_t r = 0; r < pBlock->info.rows; ++r) {
            if (pCol->varmeta.offset[r] == -1) { pCol->hasNull = true; break; }
          }
        }
      } else if (pCol->nullbitmap != NULL) {
        for (int32_t r = 0; r < pBlock->info.rows; ++r) {
          if (colDataIsNull_f(pCol, r)) { pCol->hasNull = true; break; }
        }
      }
    }

    if (taosArrayPush(pArr, &pBlock) == NULL) {
      blockDataDestroy(pBlock);
      code = terrno;
      goto _error;
    }
  }

  *ppBlocks = pArr;
  return TSDB_CODE_SUCCESS;

_error:
  for (int32_t j = 0; j < (int32_t)taosArrayGetSize(pArr); ++j) {
    SSDataBlock** ppB = taosArrayGet(pArr, j);
    if (ppB && *ppB) blockDataDestroy(*ppB);
  }
  taosArrayDestroy(pArr);
  return code;
}

// ────────────────────────────────────────────────────────────────────────────────
// Public: create operator
// ────────────────────────────────────────────────────────────────────────────────

int32_t createRowsetSourceOperatorInfo(SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                       SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t            code  = TSDB_CODE_SUCCESS;
  int32_t            lino  = 0;
  SRowsetSourceInfo* pInfo = taosMemoryCalloc(1, sizeof(SRowsetSourceInfo));
  SOperatorInfo*     pOptr = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOptr == NULL) {
    code = terrno;
    lino = __LINE__;
    goto _error;
  }

  code = deserializeRowsetBlocks((SRowsetSourcePhysiNode*)pPhyNode, &pInfo->pBlocks);
  QUERY_CHECK_CODE(code, lino, _error);

  code = filterInitFromNode((SNode*)((SRowsetSourcePhysiNode*)pPhyNode)->node.pConditions,
                            &pOptr->exprSupp.pFilterInfo, 0, pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->curBlock = 0;

  setOperatorInfo(pOptr, "RowsetSourceOperator", QUERY_NODE_PHYSICAL_PLAN_ROWSET_SOURCE, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  initResultSizeInfo(&pOptr->resultInfo, 4096);
  pOptr->fpSet = createOperatorFpSet(optrDummyOpenFn, doRowsetSourceNext, NULL, destroyRowsetSourceOperatorInfo,
                                     optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  *pOptrInfo = pOptr;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    destroyRowsetSourceOperatorInfo(pInfo);
  }
  taosMemoryFree(pOptr);
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}
