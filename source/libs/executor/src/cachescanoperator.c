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

#include "function.h"
#include "os.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "executorimpl.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"

typedef struct SCacheRowsScanInfo {
  SSDataBlock*    pRes;
  SReadHandle     readHandle;
  void*           pLastrowReader;
  SColMatchInfo   matchInfo;
  int32_t*        pSlotIds;
  SExprSupp       pseudoExprSup;
  int32_t         retrieveType;
  int32_t         currentGroupIndex;
  SSDataBlock*    pBufferredRes;
  SArray*         pUidList;
  int32_t         indexOfBufferedRes;
} SCacheRowsScanInfo;

static SSDataBlock* doScanCache(SOperatorInfo* pOperator);
static void         destroyCacheScanOperator(void* param);
static int32_t      extractCacheScanSlotId(const SArray* pColMatchInfo, SExecTaskInfo* pTaskInfo, int32_t** pSlotIds);
static int32_t      removeRedundantTsCol(SLastRowScanPhysiNode* pScanNode, SColMatchInfo* pColMatchInfo);

#define SCAN_ROW_TYPE(_t)  ((_t)? CACHESCAN_RETRIEVE_LAST : CACHESCAN_RETRIEVE_LAST_ROW)

SOperatorInfo* createCacherowsScanOperator(SLastRowScanPhysiNode* pScanNode, SReadHandle* readHandle,
                                           SExecTaskInfo* pTaskInfo) {
  int32_t           code = TSDB_CODE_SUCCESS;
  SCacheRowsScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SCacheRowsScanInfo));
  SOperatorInfo*    pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->readHandle = *readHandle;

  SDataBlockDescNode* pDescNode = pScanNode->scan.node.pOutputDataBlockDesc;
  pInfo->pRes = createDataBlockFromDescNode(pDescNode);

  int32_t numOfCols = 0;
  code =
      extractColMatchInfo(pScanNode->scan.pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  removeRedundantTsCol(pScanNode, &pInfo->matchInfo);

  code = extractCacheScanSlotId(pInfo->matchInfo.pList, pTaskInfo, &pInfo->pSlotIds);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  STableListInfo* pTableList = pTaskInfo->pTableInfoList;

  int32_t totalTables = tableListGetSize(pTableList);
  int32_t capacity = 0;

  pInfo->pUidList = taosArrayInit(4, sizeof(int64_t));

  // partition by tbname
  if (oneTableForEachGroup(pTableList) || (totalTables == 1)) {
    pInfo->retrieveType = CACHESCAN_RETRIEVE_TYPE_ALL | SCAN_ROW_TYPE(pScanNode->ignoreNull);

    STableKeyInfo* pList = tableListGetInfo(pTableList, 0);

    uint64_t suid = tableListGetSuid(pTableList);
    code = tsdbCacherowsReaderOpen(pInfo->readHandle.vnode, pInfo->retrieveType, pList, totalTables,
                                   taosArrayGetSize(pInfo->matchInfo.pList), suid, &pInfo->pLastrowReader, pTaskInfo->id.str);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    capacity = TMIN(totalTables, 4096);

    pInfo->pBufferredRes = createOneDataBlock(pInfo->pRes, false);
    blockDataEnsureCapacity(pInfo->pBufferredRes, capacity);
  } else {  // by tags
    pInfo->retrieveType = CACHESCAN_RETRIEVE_TYPE_SINGLE | SCAN_ROW_TYPE(pScanNode->ignoreNull);
    capacity = 1;  // only one row output
  }

  initResultSizeInfo(&pOperator->resultInfo, capacity);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  if (pScanNode->scan.pScanPseudoCols != NULL) {
    SExprSupp* p = &pInfo->pseudoExprSup;
    p->pExprInfo = createExprInfo(pScanNode->scan.pScanPseudoCols, NULL, &p->numOfExprs);
    p->pCtx = createSqlFunctionCtx(p->pExprInfo, p->numOfExprs, &p->rowEntryInfoOffset);
  }

  setOperatorInfo(pOperator, "CachedRowScanOperator", QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN, false, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);

  pOperator->fpSet =
      createOperatorFpSet(optrDummyOpenFn, doScanCache, NULL, destroyCacheScanOperator, optrDefaultBufFn, NULL);

  pOperator->cost.openCost = 0;
  return pOperator;

  _error:
  pTaskInfo->code = code;
  destroyCacheScanOperator(pInfo);
  taosMemoryFree(pOperator);
  return NULL;
}

SSDataBlock* doScanCache(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SCacheRowsScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*    pTaskInfo = pOperator->pTaskInfo;
  STableListInfo*   pTableList = pTaskInfo->pTableInfoList;

  uint64_t suid = tableListGetSuid(pTableList);
  int32_t  size = tableListGetSize(pTableList);
  if (size == 0) {
    setOperatorCompleted(pOperator);
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);

  // check if it is a group by tbname
  if ((pInfo->retrieveType & CACHESCAN_RETRIEVE_TYPE_ALL) == CACHESCAN_RETRIEVE_TYPE_ALL) {
    if (pInfo->indexOfBufferedRes >= pInfo->pBufferredRes->info.rows) {
      blockDataCleanup(pInfo->pBufferredRes);
      taosArrayClear(pInfo->pUidList);

      int32_t code =
          tsdbRetrieveCacheRows(pInfo->pLastrowReader, pInfo->pBufferredRes, pInfo->pSlotIds, pInfo->pUidList);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }

      // check for tag values
      int32_t resultRows = pInfo->pBufferredRes->info.rows;

      // the results may be null, if last values are all null
      ASSERT(resultRows == 0 || resultRows == taosArrayGetSize(pInfo->pUidList));
      pInfo->indexOfBufferedRes = 0;
    }

    SSDataBlock* pRes = pInfo->pRes;

    if (pInfo->indexOfBufferedRes < pInfo->pBufferredRes->info.rows) {
      for (int32_t i = 0; i < taosArrayGetSize(pInfo->matchInfo.pList); ++i) {
        SColMatchItem* pMatchInfo = taosArrayGet(pInfo->matchInfo.pList, i);
        int32_t        slotId = pMatchInfo->dstSlotId;

        SColumnInfoData* pSrc = taosArrayGet(pInfo->pBufferredRes->pDataBlock, slotId);
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, slotId);

        if (colDataIsNull_s(pSrc, pInfo->indexOfBufferedRes)) {
          colDataAppendNULL(pDst, 0);
        } else {
          char* p = colDataGetData(pSrc, pInfo->indexOfBufferedRes);
          colDataAppend(pDst, 0, p, false);
        }
      }

      pRes->info.id.uid = *(tb_uid_t*)taosArrayGet(pInfo->pUidList, pInfo->indexOfBufferedRes);
      pRes->info.rows = 1;

      SExprSupp* pSup = &pInfo->pseudoExprSup;
      int32_t code = addTagPseudoColumnData(&pInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pRes,
                                            pRes->info.rows, GET_TASKID(pTaskInfo), NULL);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        return NULL;
      }

      pRes->info.id.groupId = getTableGroupId(pTableList, pRes->info.id.uid);
      pInfo->indexOfBufferedRes += 1;
      return pRes;
    } else {
      setOperatorCompleted(pOperator);
      return NULL;
    }
  } else {
    size_t totalGroups = tableListGetOutputGroups(pTableList);

    while (pInfo->currentGroupIndex < totalGroups) {
      STableKeyInfo* pList = NULL;
      int32_t num = 0;

      int32_t code = tableListGetGroupList(pTableList, pInfo->currentGroupIndex, &pList, &num);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }

      code = tsdbCacherowsReaderOpen(pInfo->readHandle.vnode, pInfo->retrieveType, pList, num,
                                     taosArrayGetSize(pInfo->matchInfo.pList), suid, &pInfo->pLastrowReader,
                                     pTaskInfo->id.str);
      if (code != TSDB_CODE_SUCCESS) {
        pInfo->currentGroupIndex += 1;
        taosArrayClear(pInfo->pUidList);
        continue;
      }

      taosArrayClear(pInfo->pUidList);

      code = tsdbRetrieveCacheRows(pInfo->pLastrowReader, pInfo->pRes, pInfo->pSlotIds, pInfo->pUidList);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }

      pInfo->currentGroupIndex += 1;

      // check for tag values
      if (pInfo->pRes->info.rows > 0) {
        if (pInfo->pseudoExprSup.numOfExprs > 0) {
          SExprSupp* pSup = &pInfo->pseudoExprSup;

          STableKeyInfo* pKeyInfo = &((STableKeyInfo*)pList)[0];
          pInfo->pRes->info.id.groupId = pKeyInfo->groupId;

          if (taosArrayGetSize(pInfo->pUidList) > 0) {
            ASSERT((pInfo->retrieveType & CACHESCAN_RETRIEVE_LAST_ROW) == CACHESCAN_RETRIEVE_LAST_ROW);

            pInfo->pRes->info.id.uid = *(tb_uid_t*)taosArrayGet(pInfo->pUidList, 0);
            code = addTagPseudoColumnData(&pInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pInfo->pRes, pInfo->pRes->info.rows,
                                          GET_TASKID(pTaskInfo), NULL);
            if (code != TSDB_CODE_SUCCESS) {
              pTaskInfo->code = code;
              return NULL;
            }
          }
        }

        pInfo->pLastrowReader = tsdbCacherowsReaderClose(pInfo->pLastrowReader);
        return pInfo->pRes;
      } else {
        pInfo->pLastrowReader = tsdbCacherowsReaderClose(pInfo->pLastrowReader);
      }
    }

    setOperatorCompleted(pOperator);
    return NULL;
  }
}

void destroyCacheScanOperator(void* param) {
  SCacheRowsScanInfo* pInfo = (SCacheRowsScanInfo*)param;
  blockDataDestroy(pInfo->pRes);
  blockDataDestroy(pInfo->pBufferredRes);
  taosMemoryFree(pInfo->pSlotIds);
  taosArrayDestroy(pInfo->pUidList);
  taosArrayDestroy(pInfo->matchInfo.pList);

  if (pInfo->pLastrowReader != NULL) {
    pInfo->pLastrowReader = tsdbCacherowsReaderClose(pInfo->pLastrowReader);
  }

  cleanupExprSupp(&pInfo->pseudoExprSup);
  taosMemoryFreeClear(param);
}

int32_t extractCacheScanSlotId(const SArray* pColMatchInfo, SExecTaskInfo* pTaskInfo, int32_t** pSlotIds) {
  size_t numOfCols = taosArrayGetSize(pColMatchInfo);

  *pSlotIds = taosMemoryMalloc(numOfCols * sizeof(int32_t));
  if (*pSlotIds == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SSchemaWrapper* pWrapper = pTaskInfo->schemaInfo.sw;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColMatchItem* pColMatch = taosArrayGet(pColMatchInfo, i);
    for (int32_t j = 0; j < pWrapper->nCols; ++j) {
      if (pColMatch->colId == pWrapper->pSchema[j].colId && pColMatch->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        (*pSlotIds)[pColMatch->dstSlotId] = -1;
        break;
      }

      if (pColMatch->colId == pWrapper->pSchema[j].colId) {
        (*pSlotIds)[pColMatch->dstSlotId] = j;
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t removeRedundantTsCol(SLastRowScanPhysiNode* pScanNode, SColMatchInfo* pColMatchInfo) {
  if (!pScanNode->ignoreNull) {  // retrieve cached last value
    return TSDB_CODE_SUCCESS;
  }

  size_t size = taosArrayGetSize(pColMatchInfo->pList);
  SArray* pMatchInfo = taosArrayInit(size, sizeof(SColMatchItem));

  for (int32_t i = 0; i < size; ++i) {
    SColMatchItem* pColInfo = taosArrayGet(pColMatchInfo->pList, i);

    int32_t    slotId = pColInfo->dstSlotId;
    SNodeList* pList = pScanNode->scan.node.pOutputDataBlockDesc->pSlots;

    SSlotDescNode* pDesc = (SSlotDescNode*)nodesListGetNode(pList, slotId);
    if (pDesc->dataType.type != TSDB_DATA_TYPE_TIMESTAMP) {
      taosArrayPush(pMatchInfo, pColInfo);
    }
  }

  taosArrayDestroy(pColMatchInfo->pList);
  pColMatchInfo->pList = pMatchInfo;
  return TSDB_CODE_SUCCESS;
}
