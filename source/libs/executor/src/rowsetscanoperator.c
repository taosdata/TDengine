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

#include "executor.h"
#include "executorInt.h"
#include "filter.h"
#include "operator.h"
#include "plannodes.h"
#include "query.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tutil.h"

// --------------------------------------------------------------------------------
// Internal state
// --------------------------------------------------------------------------------

typedef struct SRowsetSourceInfo {
  SArray*   pBlocks;    // SArray<SSDataBlock*>  -  pre-deserialized blocks
  int32_t   curBlock;   // next block index to return
} SRowsetSourceInfo;

// --------------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------------

static SSDataBlock* createRowsetDataBlock(const SRowsetSourcePhysiNode* pPhyNode) {
  SDataBlockDescNode* pDesc = pPhyNode->node.pOutputDataBlockDesc;

  if (NULL == pDesc || pPhyNode->colCount <= 0 || pPhyNode->colCount > LIST_LENGTH(pDesc->pSlots)) {
    qError("rowset source invalid data block desc, desc:%p colCount:%d slots:%d", pDesc, pPhyNode->colCount,
           NULL == pDesc ? 0 : LIST_LENGTH(pDesc->pSlots));
    terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return NULL;
  }

  return createDataBlockFromDescNode(pDesc);
}

static int32_t fillRowsetExtendedColumns(const SRowsetSourcePhysiNode* pPhyNode, SSDataBlock* pBlock) {
  SDataBlockDescNode* pDesc = pPhyNode->node.pOutputDataBlockDesc;
  int32_t             numOfSlots = LIST_LENGTH(pDesc->pSlots);

  for (int32_t dstIndex = pPhyNode->colCount; dstIndex < numOfSlots; ++dstIndex) {
    SSlotDescNode* pDstSlot = (SSlotDescNode*)nodesListGetNode(pDesc->pSlots, dstIndex);
    SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstIndex);
    if (NULL == pDstSlot || NULL == pDstCol) {
      qError("rowset source invalid extended slot:%d slot:%p col:%p", dstIndex, pDstSlot, pDstCol);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    if (pDstSlot->reserve) {
      colDataSetNNULL(pDstCol, 0, pBlock->info.rows);
      continue;
    }

    bool copied = false;
    for (int32_t srcIndex = 0; srcIndex < pPhyNode->colCount; ++srcIndex) {
      SSlotDescNode* pSrcSlot = (SSlotDescNode*)nodesListGetNode(pDesc->pSlots, srcIndex);
      SColumnInfoData* pSrcCol = taosArrayGet(pBlock->pDataBlock, srcIndex);
      if (NULL == pSrcSlot || NULL == pSrcCol) {
        qError("rowset source invalid source slot:%d slot:%p col:%p", srcIndex, pSrcSlot, pSrcCol);
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }
      if (pDstSlot->dataType.type != pSrcSlot->dataType.type || pDstSlot->dataType.bytes != pSrcSlot->dataType.bytes ||
          strcmp(pDstSlot->name, pSrcSlot->name) != 0) {
        continue;
      }

      int32_t code = colDataAssign(pDstCol, pSrcCol, pBlock->info.rows, &pBlock->info);
      if (TSDB_CODE_SUCCESS != code) {
        qError("rowset source failed to copy extended slot:%d from source slot:%d, code:%s", dstIndex, srcIndex,
               tstrerror(code));
        return code;
      }
      copied = true;
      break;
    }

    if (!copied) {
      qError("rowset source cannot populate extended slot:%d name:%s", dstIndex, pDstSlot->name);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
  }

  return TSDB_CODE_SUCCESS;
}

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

// --------------------------------------------------------------------------------
// Operator callbacks
// --------------------------------------------------------------------------------

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

// --------------------------------------------------------------------------------
// Deserialization: length-prefix multi-block binary  ->  SArray<SSDataBlock*>
// Format: [uint32_t blockSize][blockDataToBuf output] repeated numBlocks times
// --------------------------------------------------------------------------------

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

  for (int32_t i = 0; i < numBlocks; ++i) {
    if ((size_t)(pEnd - pCur) < sizeof(uint32_t)) {
      qError("rowset source block header exceeds buffer, block:%d remaining:%zu", i, (size_t)(pEnd - pCur));
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _error;
    }
    uint32_t blockSize = 0;
    memcpy(&blockSize, pCur, sizeof(uint32_t));
    pCur += sizeof(uint32_t);

    if ((size_t)blockSize > (size_t)(pEnd - pCur)) {
      qError("rowset source block exceeds buffer, block:%d blockSize:%u remaining:%zu", i, blockSize,
             (size_t)(pEnd - pCur));
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _error;
    }

    SSDataBlock* pBlock = createRowsetDataBlock(pPhyNode);
    if (pBlock == NULL) {
      code = terrno;
      qError("rowset source failed to create data block, code:%s", tstrerror(code));
      goto _error;
    }

    code = blockDataFromBufWithCapacity(pBlock, (const char*)pCur, blockSize, pPhyNode->colCount);
    if (code != TSDB_CODE_SUCCESS) {
      qError("rowset source failed to decode block:%d blockSize:%u colCount:%d, code:%s", i, blockSize,
             pPhyNode->colCount, tstrerror(code));
      blockDataDestroy(pBlock);
      goto _error;
    }
    code = fillRowsetExtendedColumns(pPhyNode, pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      qError("rowset source failed to populate extended columns for block:%d, code:%s", i, tstrerror(code));
      blockDataDestroy(pBlock);
      goto _error;
    }
    pCur += blockSize;

    if (taosArrayPush(pArr, &pBlock) == NULL) {
      blockDataDestroy(pBlock);
      code = terrno;
      goto _error;
    }
  }

  if (pCur != pEnd) {
    qError("rowset source buffer has trailing bytes:%zu", (size_t)(pEnd - pCur));
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _error;
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

// --------------------------------------------------------------------------------
// Public: create operator
// --------------------------------------------------------------------------------

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
