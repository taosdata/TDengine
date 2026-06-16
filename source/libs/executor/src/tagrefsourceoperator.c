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
#include "os.h"
#include "querynodes.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdef.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "ttypes.h"

#include "storageapi.h"

/*
 * ============================================================================
 * TagRefSource Operator
 *
 * This operator scans tag values from a source super table for tag references.
 * It is created by the planner when a virtual table references tags from source
 * tables using the REF syntax.
 *
 * Example SQL:
 *   CREATE VTABLE vtbl USING src_stb WITH TAGS (tag1 REF src_stb.region);
 *   SELECT tag1 FROM vtbl;
 *
 * ============================================================================
 */

/**
 * TagRefSource Operator execution information
 */

#define TAG_REF_SOURCE_DEFAULT_CAPACITY 4096
typedef struct STagRefSourceOperatorInfo {
  // Result data block (single-row output per getNext call)
  SSDataBlock* pRes;
  // Pre-scanned all rows (populated in open)
  SSDataBlock* pAllRows;

  // Source table information
  uint64_t      sourceSuid;       // Source super table UID
  SName         sourceTableName;  // Source table name (db.table)
  SNodeList*    pRefCols;         // Referenced tag columns (STagRefColumn)
  SNodeList*    pScanCols;        // Columns to scan from source table
  SVgroupsInfo* pVgroupList;      // Vgroup information (for distributed)

  // Scan state
  int32_t curPos;         // Current row position for getNext iteration
  int32_t totalRows;      // Total rows loaded in open
  bool    scanCompleted;  // Scan completion flag

  // Tag scan related (reuse TagScan logic)
  SReadHandle     readHandle;
  STableListInfo* pTableListInfo;
  SStorageAPI*    pStorageAPI;
  SNode*          pTagCond;       // Tag filter condition
  SNode*          pTagIndexCond;  // Tag index condition
  SColMatchInfo   matchInfo;
  SLimitInfo      limitInfo;

  // Data block capacity
  int32_t capacity;
} STagRefSourceOperatorInfo;

// Forward declaration
static int32_t tagRefSourceOpen(SOperatorInfo* pOperator);

/**
 * Scan one table's tags and add to result block
 * Similar to doTagScanOneTable in scanoperator.c
 */
static int32_t tagRefSourceScanOneTable(SOperatorInfo* pOperator, SSDataBlock* pRes, SMetaReader* mr,
                                        int32_t rowIndex) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  STagRefSourceOperatorInfo* pInfo = (STagRefSourceOperatorInfo*)pOperator->info;
  SStorageAPI*               pAPI = pInfo->pStorageAPI;

  // Get current table from table list
  STableKeyInfo* pItem = tableListGetInfo(pInfo->pTableListInfo, pInfo->curPos);
  QUERY_CHECK_NULL(pItem, code, lino, _end, TSDB_CODE_INVALID_PARA);

  // Get table entry from vnode
  code = pAPI->metaReaderFn.getTableEntryByUid(mr, pItem->uid);
  tDecoderClear(&(*mr).coder);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s: failed to get table meta for uid:0x%" PRIx64 ", code:%s", __func__, pItem->uid, tstrerror(code));
    goto _end;
  }
  // Iterate through each referenced column and extract tag value
  // Use slotIndex to map to the correct column in result block
  int32_t slotIndex = 0;
  if (pInfo->pRefCols != NULL) {
    SNode* pRefColNode = NULL;
    FOREACH(pRefColNode, pInfo->pRefCols) {
      STagRefColumn* pRefCol = (STagRefColumn*)pRefColNode;

      // Get column info by slot index (columns are in same order as pRefCols)
      SColumnInfoData* pColInfo = NULL;
      if (slotIndex < taosArrayGetSize(pRes->pDataBlock)) {
        pColInfo = taosArrayGet(pRes->pDataBlock, slotIndex);
      }

      if (pColInfo == NULL) {
        qError("%s: failed to get col info for slotIndex:%d", __func__, slotIndex);
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        goto _end;
      }

      // Extract tag value from table entry using source table's colId
      STagVal val = {0};
      val.cid = pRefCol->sourceColId;  // Use source table's tag column ID

      // Find the tag in the child table's tags
      const char* pTagVal = pAPI->metaFn.extractTagVal((*mr).me.ctbEntry.pTags, pColInfo->info.type, &val);
      char* data = NULL;
      bool  isNull = false;

      if (pColInfo->info.type != TSDB_DATA_TYPE_JSON && pTagVal != NULL) {
        data = tTagValToData((const STagVal*)pTagVal, false);
      } else {
        data = (char*)pTagVal;
      }

      if (data == NULL || (pColInfo->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data))) {
        isNull = true;
      }

      // Set the value in the result block
      code = colDataSetVal(pColInfo, rowIndex, data, isNull);
      if (code != TSDB_CODE_SUCCESS) {
        qWarn("%s: failed to set col value, slotIndex:%d, row:%d, code:%s", __func__, slotIndex, rowIndex,
               tstrerror(code));
        if ((pColInfo->info.type != TSDB_DATA_TYPE_JSON) && (pTagVal != NULL) &&
            IS_VAR_DATA_TYPE(((const STagVal*)pTagVal)->type) && (data != NULL)) {
          taosMemoryFree(data);
        }
        goto _end;
      }

      // Free allocated data for var types
      if ((pColInfo->info.type != TSDB_DATA_TYPE_JSON) && (pTagVal != NULL) &&
          IS_VAR_DATA_TYPE(((const STagVal*)pTagVal)->type) && (data != NULL)) {
        taosMemoryFree(data);
      }

      slotIndex++;
    }
  }

_end:
  if (code == TSDB_CODE_SUCCESS) {
    pRes->info.rows++;
  }

  return code;
}

/**
 * Get next row of tag values from pre-scanned data.
 * Returns exactly 1 row per call (caller expects rows==1).
 */
static int32_t tagRefSourceGetNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  STagRefSourceOperatorInfo* pInfo = NULL;

  QUERY_CHECK_NULL(pOperator, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pOperator->info, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pInfo = (STagRefSourceOperatorInfo*)pOperator->info;

  // Auto-open on first getNext call
  if (!OPTR_IS_OPENED(pOperator)) {
    code = tagRefSourceOpen(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pOperator->status == OP_EXEC_DONE || pInfo->curPos >= pInfo->totalRows) {
    *ppRes = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // Copy one row from pAllRows[curPos] into pRes
  SSDataBlock* pRes = pInfo->pRes;
  blockDataCleanup(pRes);
  pRes->info.rows = 0;

  int32_t numCols = taosArrayGetSize(pRes->pDataBlock);
  for (int32_t col = 0; col < numCols; col++) {
    SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, col);
    SColumnInfoData* pSrc = taosArrayGet(pInfo->pAllRows->pDataBlock, col);
    if (pDst == NULL || pSrc == NULL) continue;

    bool isNull = colDataIsNull_s(pSrc, pInfo->curPos);
    if (isNull) {
      colDataSetNULL(pDst, 0);
    } else {
      char* val = colDataGetData(pSrc, pInfo->curPos);
      code = colDataSetVal(pDst, 0, val, false);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  pRes->info.rows = 1;
  pInfo->curPos++;

  if (pInfo->curPos >= pInfo->totalRows) {
    setOperatorCompleted(pOperator);
  }

  *ppRes = pRes;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d: %s", __func__, lino, tstrerror(code));
    pOperator->pTaskInfo->code = code;
  }
  return code;
}

/**
 * Destroy TagRefSource operator info
 */
static void destroyTagRefSourceOperatorInfo(void* param) {
  if (NULL == param) {
    return;
  }

  STagRefSourceOperatorInfo* pInfo = (STagRefSourceOperatorInfo*)param;

  // Free result blocks
  blockDataDestroy(pInfo->pRes);
  blockDataDestroy(pInfo->pAllRows);

  // Free column lists
  nodesDestroyList(pInfo->pRefCols);
  nodesDestroyList(pInfo->pScanCols);

  // Free vgroup list
  taosMemoryFreeClear(pInfo->pVgroupList);

  // Free filter conditions
  nodesDestroyNode(pInfo->pTagCond);
  nodesDestroyNode(pInfo->pTagIndexCond);

  // Free match info
  taosArrayDestroy(pInfo->matchInfo.pList);

  tableListDestroy(pInfo->pTableListInfo);

  taosMemoryFreeClear(param);
}

/**
 * Open TagRefSource operator - batch process all child table tag refs into pAllRows
 */
static int32_t tagRefSourceOpen(SOperatorInfo* pOperator) {
  if (NULL == pOperator || NULL == pOperator->info) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  STagRefSourceOperatorInfo* pInfo = (STagRefSourceOperatorInfo*)pOperator->info;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*               pAPI = &pTaskInfo->storageAPI;
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  SMetaReader                mr = {0};

  pInfo->curPos = 0;
  pInfo->totalRows = 0;
  pInfo->scanCompleted = false;

  // Get table list size
  int32_t size = 0;
  code = tableListGetSize(pInfo->pTableListInfo, &size);
  QUERY_CHECK_CODE(code, lino, _end);

  if (size == 0) {
    qDebug("%s: empty table list, sourceSuid:%" PRIu64, __func__, pInfo->sourceSuid);
    OPTR_SET_OPENED(pOperator);
    return TSDB_CODE_SUCCESS;
  }

  // Ensure pAllRows has capacity for all tables
  if (size > pInfo->capacity) {
    code = blockDataEnsureCapacity(pInfo->pAllRows, size);
    QUERY_CHECK_CODE(code, lino, _end);
    pInfo->capacity = size;
  }

  blockDataCleanup(pInfo->pAllRows);
  pInfo->pAllRows->info.rows = 0;

  // Batch scan all child tables at once into pAllRows
  pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn, pInfo->readHandle.txnId);

  // Temporarily swap pRes to pAllRows so tagRefSourceScanOneTable writes there
  SSDataBlock* origRes = pInfo->pRes;
  pInfo->pRes = pInfo->pAllRows;

  for (int32_t i = 0; i < size; ++i) {
    pInfo->curPos = i;
    code = tagRefSourceScanOneTable(pOperator, pInfo->pAllRows, &mr, pInfo->pAllRows->info.rows);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pInfo->pRes = origRes;
  pAPI->metaReaderFn.clearReader(&mr);

  pInfo->totalRows = pInfo->pAllRows->info.rows;
  pInfo->curPos = 0;
  pInfo->scanCompleted = true;

  OPTR_SET_OPENED(pOperator);

  qDebug("%s: TagRefSource batch scan completed, sourceSuid:%" PRIu64 ", totalRows:%d",
         __func__, pInfo->sourceSuid, pInfo->totalRows);

  return code;

_end:
  pAPI->metaReaderFn.clearReader(&mr);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d: %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  return code;
}

/**
 * Close TagRefSource operator
 */
/**
 * Create TagRefSource Operator
 *
 * @param pTagRefSourceNode Physical plan node
 * @param pReadHandle Read handle for vnode access
 * @param pTableListInfo Table list info
 * @param pTaskInfo Execution task info
 * @param pOptrInfo Output operator info
 * @return Error code
 */
int32_t createTagRefSourceOperatorInfo(STagRefSourcePhysiNode* pTagRefSourceNode, SReadHandle* pReadHandle,
                                       STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo,
                                       SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  STagRefSourceOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STagRefSourceOperatorInfo));
  SOperatorInfo*             pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = pTagRefSourceNode;
  SDataBlockDescNode* pDescNode = pTagRefSourceNode->node.pOutputDataBlockDesc;

  // Copy source table information
  pInfo->sourceSuid = pTagRefSourceNode->sourceSuid;
  memcpy(&pInfo->sourceTableName, &pTagRefSourceNode->sourceTableName, sizeof(SName));

  // Clone referenced columns
  if (pTagRefSourceNode->pRefCols) {
    code = nodesCloneList(pTagRefSourceNode->pRefCols, &pInfo->pRefCols);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  // Clone scan columns
  if (pTagRefSourceNode->pScanCols) {
    code = nodesCloneList(pTagRefSourceNode->pScanCols, &pInfo->pScanCols);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  // Clone vgroup list if exists
  if (pTagRefSourceNode->pVgroupList) {
    pInfo->pVgroupList =
        taosMemoryMalloc(sizeof(SVgroupsInfo) + pTagRefSourceNode->pVgroupList->numOfVgroups * sizeof(SVgroupInfo));
    QUERY_CHECK_NULL(pInfo->pVgroupList, code, lino, _error, terrno);
    memcpy(pInfo->pVgroupList, pTagRefSourceNode->pVgroupList,
           sizeof(SVgroupsInfo) + pTagRefSourceNode->pVgroupList->numOfVgroups * sizeof(SVgroupInfo));
  }

  // Set storage API
  pInfo->pStorageAPI = &pTaskInfo->storageAPI;

  // Set read handle
  if (pReadHandle) {
    pInfo->readHandle = *pReadHandle;
  }

  // Set table list info
  pInfo->pTableListInfo = pTableListInfo;

  // Initialize scan state
  pInfo->curPos = 0;
  pInfo->scanCompleted = false;

  // Create result data block (1-row output buffer)
  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);
  code = blockDataEnsureCapacity(pInfo->pRes, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  // Create pAllRows block (bulk storage, populated in open)
  pInfo->pAllRows = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pAllRows, code, lino, _error, terrno);

  // Initialize limit info
  initLimitInfo(pTagRefSourceNode->node.pLimit, pTagRefSourceNode->node.pSlimit, &pInfo->limitInfo);

  // Set capacity (use fixed default since resultInfo is not yet initialized)
  pInfo->capacity = TAG_REF_SOURCE_DEFAULT_CAPACITY;
  code = blockDataEnsureCapacity(pInfo->pAllRows, pInfo->capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  // Initialize column match info
  if (pTagRefSourceNode->pScanCols) {
    int32_t numOfCols = 0;
    code = extractColMatchInfo(pTagRefSourceNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID,
                               &pInfo->matchInfo);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  // Set operator info
  setOperatorInfo(pOperator, "TagRefSourceOperator", QUERY_NODE_PHYSICAL_PLAN_TAG_REF_SOURCE, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);

  // Set result size
  initResultSizeInfo(&pOperator->resultInfo, pInfo->capacity);

  // Set function pointers
  pOperator->fpSet = createOperatorFpSet(tagRefSourceOpen, tagRefSourceGetNext, NULL, destroyTagRefSourceOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  qDebug("%s: TagRefSource operator created, sourceSuid:%" PRIu64 ", refCols:%d, scanCols:%d", __func__,
         pInfo->sourceSuid, pInfo->pRefCols ? LIST_LENGTH(pInfo->pRefCols) : 0,
         pInfo->pScanCols ? LIST_LENGTH(pInfo->pScanCols) : 0);

  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo != NULL) {
    pInfo->pTableListInfo = NULL;  // caller owns pTableListInfo on error
    destroyTagRefSourceOperatorInfo(pInfo);
  }
  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }

  qError("%s failed at line %d: %s", __func__, lino, tstrerror(code));
  return code;
}
