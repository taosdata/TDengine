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
#include "function.h"
#include "os.h"
#include "querynodes.h"
#include "systable.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdef.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"
#include "ttime.h"

#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "tcompare.h"
#include "thash.h"
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
typedef struct STagRefSourceOperatorInfo {
  // Result data block
  SSDataBlock* pRes;

  // Source table information
  uint64_t      sourceSuid;       // Source super table UID
  SName         sourceTableName;  // Source table name (db.table)
  SNodeList*    pRefCols;         // Referenced tag columns (STagRefColumn)
  SNodeList*    pScanCols;        // Columns to scan from source table
  SVgroupsInfo* pVgroupList;      // Vgroup information (for distributed)

  // Scan state
  int32_t curPos;         // Current position in table list
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

/**
 * Scan one table's tags and add to result block
 * Similar to doTagScanOneTable in scanoperator.c
 */
static int32_t tagRefSourceScanOneTable(SOperatorInfo* pOperator, SSDataBlock* pRes, SMetaReader* mr,
                                        int32_t rowIndex) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  STagRefSourceOperatorInfo* pInfo = (STagRefSourceOperatorInfo*)pOperator->info;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*               pAPI = pInfo->pStorageAPI;

  // Get current table from table list
  STableKeyInfo* pItem = tableListGetInfo(pInfo->pTableListInfo, pInfo->curPos);
  QUERY_CHECK_NULL(pItem, code, lino, _end, TSDB_CODE_INVALID_PARA);

  // Get table entry from vnode
  code = pAPI->metaReaderFn.getTableEntryByUid(mr, pItem->uid);
  if (code != TSDB_CODE_SUCCESS) {
    qDebug("%s: failed to get table meta for uid:0x%" PRIx64 ", code:%s", __func__, pItem->uid, tstrerror(code));
    // Continue to next table instead of failing completely
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
        qWarn("%s: failed to get col info for slotIndex:%d", __func__, slotIndex);
        slotIndex++;
        continue;
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
        qDebug("%s: failed to set col value, slotIndex:%d, row:%d, code:%s", __func__, slotIndex, rowIndex,
               tstrerror(code));
      } else {
        if (pColInfo->info.type == TSDB_DATA_TYPE_INT) {
          qDebug("tagref source fill: tag=%s sourceColId=%d slot=%d isNull=%d value=%d", pRefCol->colName,
                 pRefCol->sourceColId, slotIndex, isNull, isNull ? 0 : *(int32_t*)data);
        } else if (pColInfo->info.type == TSDB_DATA_TYPE_BOOL) {
          qDebug("tagref source fill: tag=%s sourceColId=%d slot=%d isNull=%d value=%d", pRefCol->colName,
                 pRefCol->sourceColId, slotIndex, isNull, isNull ? 0 : (int32_t)(*(bool*)data));
        } else {
          qDebug("tagref source fill: tag=%s sourceColId=%d slot=%d isNull=%d type=%d", pRefCol->colName,
                 pRefCol->sourceColId, slotIndex, isNull, pColInfo->info.type);
        }
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
 * Get next batch of tag values from source table
 * This is the main scanning function, similar to doTagScanFromMetaEntryNext
 */
static int32_t tagRefSourceGetNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  STagRefSourceOperatorInfo* pInfo = NULL;
  SSDataBlock*               pRes = NULL;
  SExecTaskInfo*             pTaskInfo = NULL;
  SStorageAPI*               pAPI = NULL;
  SMetaReader                mr = {0};

  // Check parameters
  QUERY_CHECK_NULL(pOperator, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pOperator->info, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pInfo = (STagRefSourceOperatorInfo*)pOperator->info;
  pTaskInfo = pOperator->pTaskInfo;
  pAPI = &pTaskInfo->storageAPI;
  pRes = pInfo->pRes;

  // Check if scan is completed
  if (pOperator->status == OP_EXEC_DONE) {
    *ppRes = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // Clean result block for new data
  blockDataCleanup(pRes);

  // Get table list size
  int32_t size = 0;
  code = tableListGetSize(pInfo->pTableListInfo, &size);
  QUERY_CHECK_CODE(code, lino, _end);

  if (size == 0) {
    qDebug("%s: empty table list, sourceSuid:%" PRIu64, __func__, pInfo->sourceSuid);
    setTaskStatus(pTaskInfo, TASK_COMPLETED);
    setOperatorCompleted(pOperator);
    *ppRes = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // Initialize meta reader
  pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
  pRes->info.rows = 0;

  // Scan tables until result block is full or no more tables
  while (pInfo->curPos < size && pRes->info.rows < pOperator->resultInfo.capacity) {
    code = tagRefSourceScanOneTable(pOperator, pRes, &mr, pRes->info.rows);

    // Ignore certain errors and continue to next table
    if (code != TSDB_CODE_OUT_OF_MEMORY && code != TSDB_CODE_QRY_REACH_QMEM_THRESHOLD &&
        code != TSDB_CODE_QRY_QUERY_MEM_EXHAUSTED) {
      if (code != TSDB_CODE_SUCCESS) {
        qWarn("%s: tagRefSourceScanOneTable failed, pos:%d, code:%s, skipping", __func__, pInfo->curPos,
               tstrerror(code));
      }
      code = TSDB_CODE_SUCCESS;
    }
    QUERY_CHECK_CODE(code, lino, _end);

    pInfo->curPos++;

    // Check if we've scanned all tables
    if (pInfo->curPos >= size) {
      setOperatorCompleted(pOperator);
      pInfo->scanCompleted = true;
      break;
    }
  }

  // Clear meta reader
  pAPI->metaReaderFn.clearReader(&mr);

  // Apply limit/offset if configured
  bool bLimitReached = applyLimitOffset(&pInfo->limitInfo, pRes, pTaskInfo);
  if (bLimitReached) {
    setOperatorCompleted(pOperator);
    pInfo->scanCompleted = true;
  }

  // Update statistics
  pOperator->resultInfo.totalRows += pRes->info.rows;

  // Log completion
  if (pOperator->status == OP_EXEC_DONE) {
    setTaskStatus(pTaskInfo, TASK_COMPLETED);
    qDebug("%s: TagRefSource scan completed, sourceSuid:%" PRIu64 ", totalRows:%" PRId64, __func__, pInfo->sourceSuid,
           pOperator->resultInfo.totalRows);
  } else {
    qDebug("%s: TagRefSource returning block, rows:%d, curPos:%d, size:%d", __func__, (int32_t)pRes->info.rows,
           (int32_t)pInfo->curPos, (int32_t)size);
  }

  // Return result (NULL if empty block)
  *ppRes = (pRes->info.rows == 0) ? NULL : pRes;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d: %s", __func__, lino, tstrerror(code));
    if (pTaskInfo != NULL) {
      pTaskInfo->code = code;
    }
    if (pAPI != NULL) {
      pAPI->metaReaderFn.clearReader(&mr);
    }
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

  // Free result block
  blockDataDestroy(pInfo->pRes);

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
 * Open TagRefSource operator - initialize scan
 */
static int32_t tagRefSourceOpen(SOperatorInfo* pOperator) {
  if (NULL == pOperator || NULL == pOperator->info) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  STagRefSourceOperatorInfo* pInfo = (STagRefSourceOperatorInfo*)pOperator->info;
  int32_t                    code = TSDB_CODE_SUCCESS;

  pInfo->curPos = 0;
  pInfo->scanCompleted = false;

  // Reset result block
  blockDataCleanup(pInfo->pRes);

  OPTR_SET_OPENED(pOperator);

  qDebug("%s: TagRefSource operator opened, sourceSuid:%" PRIu64 ", scanCols:%d", __func__, pInfo->sourceSuid,
         pInfo->pScanCols ? LIST_LENGTH(pInfo->pScanCols) : 0);

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

  // Create result data block
  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);

  // Initialize limit info
  initLimitInfo(pTagRefSourceNode->node.pLimit, pTagRefSourceNode->node.pSlimit, &pInfo->limitInfo);

  // Set capacity (use fixed default since resultInfo is not yet initialized)
#define TAG_REF_SOURCE_DEFAULT_CAPACITY 4096
  pInfo->capacity = TAG_REF_SOURCE_DEFAULT_CAPACITY;
  code = blockDataEnsureCapacity(pInfo->pRes, pInfo->capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  // Initialize column match info
  if (pTagRefSourceNode->pScanCols) {
    int32_t numOfCols = 0;
    code = extractColMatchInfo(pTagRefSourceNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID,
                               &pInfo->matchInfo);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  // Initialize filter from conditions
  code = filterInitFromNode((SNode*)pTagRefSourceNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

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
    destroyTagRefSourceOperatorInfo(pInfo);
  }
  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }

  qError("%s failed at line %d: %s", __func__, lino, tstrerror(code));
  return code;
}
