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
#include "functionMgt.h"
#include "index.h"
#include "os.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "ttime.h"

#include "executil.h"
#include "executorimpl.h"
#include "tcompression.h"

// If the numOfOutputGroups is 1, the data blocks that belongs to different groups will be provided randomly
// The numOfOutputGroups is specified by physical plan. and will not be affect by numOfGroups
struct STableListInfo {
  bool      oneTableForEachGroup;
  int32_t   numOfOuputGroups;  // the data block will be generated one by one
  int32_t*  groupOffset;       // keep the offset value for each group in the tableList
  SArray*   pTableList;
  SHashObj* map;  // speedup acquire the tableQueryInfo by table uid
  uint64_t  suid;
};

typedef struct tagFilterAssist {
  SHashObj* colHash;
  int32_t   index;
  SArray*   cInfoList;
} tagFilterAssist;

static int32_t removeInvalidTable(SArray* uids, SHashObj* tags);
static int32_t optimizeTbnameInCond(void* metaHandle, int64_t suid, SArray* list, SNode* pTagCond, SHashObj* tags);
static int32_t optimizeTbnameInCondImpl(void* metaHandle, int64_t suid, SArray* list, SNode* pTagCond);
static int32_t getTableList(void* metaHandle, void* pVnode, SScanPhysiNode* pScanNode, SNode* pTagCond,
                            SNode* pTagIndexCond, STableListInfo* pListInfo);

static int64_t getLimit(const SNode* pLimit) { return NULL == pLimit ? -1 : ((SLimitNode*)pLimit)->limit; }
static int64_t getOffset(const SNode* pLimit) { return NULL == pLimit ? -1 : ((SLimitNode*)pLimit)->offset; }

void initResultRowInfo(SResultRowInfo* pResultRowInfo) {
  pResultRowInfo->size = 0;
  pResultRowInfo->cur.pageId = -1;
}

void closeResultRow(SResultRow* pResultRow) { pResultRow->closed = true; }

void resetResultRow(SResultRow* pResultRow, size_t entrySize) {
  pResultRow->numOfRows = 0;
  pResultRow->closed = false;
  pResultRow->endInterp = false;
  pResultRow->startInterp = false;

  if (entrySize > 0) {
    memset(pResultRow->pEntryInfo, 0, entrySize);
  }
}

// TODO refactor: use macro
SResultRowEntryInfo* getResultEntryInfo(const SResultRow* pRow, int32_t index, const int32_t* offset) {
  assert(index >= 0 && offset != NULL);
  return (SResultRowEntryInfo*)((char*)pRow->pEntryInfo + offset[index]);
}

size_t getResultRowSize(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  int32_t rowSize = (numOfOutput * sizeof(SResultRowEntryInfo)) + sizeof(SResultRow);

  for (int32_t i = 0; i < numOfOutput; ++i) {
    rowSize += pCtx[i].resDataInfo.interBufSize;
  }

  rowSize += (numOfOutput * sizeof(bool));
  // expand rowSize to mark if col is null for top/bottom result(saveTupleData)
  return rowSize;
}

void cleanupGroupResInfo(SGroupResInfo* pGroupResInfo) {
  assert(pGroupResInfo != NULL);

  for (int32_t i = 0; i < taosArrayGetSize(pGroupResInfo->pRows); ++i) {
    SResKeyPos* pRes = taosArrayGetP(pGroupResInfo->pRows, i);
    taosMemoryFree(pRes);
  }

  pGroupResInfo->pRows = taosArrayDestroy(pGroupResInfo->pRows);
  pGroupResInfo->index = 0;
}

int32_t resultrowComparAsc(const void* p1, const void* p2) {
  SResKeyPos* pp1 = *(SResKeyPos**)p1;
  SResKeyPos* pp2 = *(SResKeyPos**)p2;

  if (pp1->groupId == pp2->groupId) {
    int64_t pts1 = *(int64_t*)pp1->key;
    int64_t pts2 = *(int64_t*)pp2->key;

    if (pts1 == pts2) {
      return 0;
    } else {
      return pts1 < pts2 ? -1 : 1;
    }
  } else {
    return pp1->groupId < pp2->groupId ? -1 : 1;
  }
}

static int32_t resultrowComparDesc(const void* p1, const void* p2) { return resultrowComparAsc(p2, p1); }

void initGroupedResultInfo(SGroupResInfo* pGroupResInfo, SSHashObj* pHashmap, int32_t order) {
  if (pGroupResInfo->pRows != NULL) {
    taosArrayDestroy(pGroupResInfo->pRows);
  }

  // extract the result rows information from the hash map
  void* pData = NULL;
  pGroupResInfo->pRows = taosArrayInit(10, POINTER_BYTES);

  // todo avoid repeated malloc memory
  size_t  keyLen = 0;
  int32_t iter = 0;
  while ((pData = tSimpleHashIterate(pHashmap, pData, &iter)) != NULL) {
    void* key = tSimpleHashGetKey(pData, &keyLen);

    SResKeyPos* p = taosMemoryMalloc(keyLen + sizeof(SResultRowPosition));

    p->groupId = *(uint64_t*)key;
    p->pos = *(SResultRowPosition*)pData;
    memcpy(p->key, (char*)key + sizeof(uint64_t), keyLen - sizeof(uint64_t));
    taosArrayPush(pGroupResInfo->pRows, &p);
  }

  if (order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC) {
    __compar_fn_t fn = (order == TSDB_ORDER_ASC) ? resultrowComparAsc : resultrowComparDesc;
    int32_t       size = POINTER_BYTES;
    taosSort(pGroupResInfo->pRows->pData, taosArrayGetSize(pGroupResInfo->pRows), size, fn);
  }

  pGroupResInfo->index = 0;
  assert(pGroupResInfo->index <= getNumOfTotalRes(pGroupResInfo));
}

void initMultiResInfoFromArrayList(SGroupResInfo* pGroupResInfo, SArray* pArrayList) {
  if (pGroupResInfo->pRows != NULL) {
    taosArrayDestroyP(pGroupResInfo->pRows, taosMemoryFree);
  }

  pGroupResInfo->pRows = pArrayList;
  pGroupResInfo->index = 0;
  ASSERT(pGroupResInfo->index <= getNumOfTotalRes(pGroupResInfo));
}

bool hasRemainResults(SGroupResInfo* pGroupResInfo) {
  if (pGroupResInfo->pRows == NULL) {
    return false;
  }

  return pGroupResInfo->index < taosArrayGetSize(pGroupResInfo->pRows);
}

int32_t getNumOfTotalRes(SGroupResInfo* pGroupResInfo) {
  assert(pGroupResInfo != NULL);
  if (pGroupResInfo->pRows == 0) {
    return 0;
  }

  return (int32_t)taosArrayGetSize(pGroupResInfo->pRows);
}

SArray* createSortInfo(SNodeList* pNodeList) {
  size_t numOfCols = 0;

  if (pNodeList != NULL) {
    numOfCols = LIST_LENGTH(pNodeList);
  } else {
    numOfCols = 0;
  }

  SArray* pList = taosArrayInit(numOfCols, sizeof(SBlockOrderInfo));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return pList;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SOrderByExprNode* pSortKey = (SOrderByExprNode*)nodesListGetNode(pNodeList, i);
    SBlockOrderInfo   bi = {0};
    bi.order = (pSortKey->order == ORDER_ASC) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    bi.nullFirst = (pSortKey->nullOrder == NULL_ORDER_FIRST);

    SColumnNode* pColNode = (SColumnNode*)pSortKey->pExpr;
    bi.slotId = pColNode->slotId;
    taosArrayPush(pList, &bi);
  }

  return pList;
}

SSDataBlock* createDataBlockFromDescNode(SDataBlockDescNode* pNode) {
  int32_t numOfCols = LIST_LENGTH(pNode->pSlots);

  SSDataBlock* pBlock = createDataBlock();

  pBlock->info.id.blockId = pNode->dataBlockId;
  pBlock->info.type = STREAM_INVALID;
  pBlock->info.calWin = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
  pBlock->info.watermark = INT64_MIN;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SSlotDescNode*  pDescNode = (SSlotDescNode*)nodesListGetNode(pNode->pSlots, i);
    SColumnInfoData idata =
        createColumnInfoData(pDescNode->dataType.type, pDescNode->dataType.bytes, pDescNode->slotId);
    idata.info.scale = pDescNode->dataType.scale;
    idata.info.precision = pDescNode->dataType.precision;

    blockDataAppendColInfo(pBlock, &idata);
  }

  return pBlock;
}

EDealRes doTranslateTagExpr(SNode** pNode, void* pContext) {
  SMetaReader* mr = (SMetaReader*)pContext;
  if (nodeType(*pNode) == QUERY_NODE_COLUMN) {
    SColumnNode* pSColumnNode = *(SColumnNode**)pNode;

    SValueNode* res = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
    if (NULL == res) {
      return DEAL_RES_ERROR;
    }

    res->translate = true;
    res->node.resType = pSColumnNode->node.resType;

    STagVal tagVal = {0};
    tagVal.cid = pSColumnNode->colId;
    const char* p = metaGetTableTagVal(mr->me.ctbEntry.pTags, pSColumnNode->node.resType.type, &tagVal);
    if (p == NULL) {
      res->node.resType.type = TSDB_DATA_TYPE_NULL;
    } else if (pSColumnNode->node.resType.type == TSDB_DATA_TYPE_JSON) {
      int32_t len = ((const STag*)p)->len;
      res->datum.p = taosMemoryCalloc(len + 1, 1);
      memcpy(res->datum.p, p, len);
    } else if (IS_VAR_DATA_TYPE(pSColumnNode->node.resType.type)) {
      res->datum.p = taosMemoryCalloc(tagVal.nData + VARSTR_HEADER_SIZE + 1, 1);
      memcpy(varDataVal(res->datum.p), tagVal.pData, tagVal.nData);
      varDataSetLen(res->datum.p, tagVal.nData);
    } else {
      nodesSetValueNodeValue(res, &(tagVal.i64));
    }
    nodesDestroyNode(*pNode);
    *pNode = (SNode*)res;
  } else if (nodeType(*pNode) == QUERY_NODE_FUNCTION) {
    SFunctionNode* pFuncNode = *(SFunctionNode**)pNode;
    if (pFuncNode->funcType == FUNCTION_TYPE_TBNAME) {
      SValueNode* res = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
      if (NULL == res) {
        return DEAL_RES_ERROR;
      }

      res->translate = true;
      res->node.resType = pFuncNode->node.resType;

      int32_t len = strlen(mr->me.name);
      res->datum.p = taosMemoryCalloc(len + VARSTR_HEADER_SIZE + 1, 1);
      memcpy(varDataVal(res->datum.p), mr->me.name, len);
      varDataSetLen(res->datum.p, len);
      nodesDestroyNode(*pNode);
      *pNode = (SNode*)res;
    }
  }

  return DEAL_RES_CONTINUE;
}

int32_t isQualifiedTable(STableKeyInfo* info, SNode* pTagCond, void* metaHandle, bool* pQualified) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaReader mr = {0};

  metaReaderInit(&mr, metaHandle, 0);
  code = metaGetTableEntryByUidCache(&mr, info->uid);
  if (TSDB_CODE_SUCCESS != code) {
    metaReaderClear(&mr);
    *pQualified = false;

    return TSDB_CODE_SUCCESS;
  }

  SNode* pTagCondTmp = nodesCloneNode(pTagCond);

  nodesRewriteExprPostOrder(&pTagCondTmp, doTranslateTagExpr, &mr);
  metaReaderClear(&mr);

  SNode* pNew = NULL;
  code = scalarCalculateConstants(pTagCondTmp, &pNew);
  if (TSDB_CODE_SUCCESS != code) {
    terrno = code;
    nodesDestroyNode(pTagCondTmp);
    *pQualified = false;

    return code;
  }

  ASSERT(nodeType(pNew) == QUERY_NODE_VALUE);
  SValueNode* pValue = (SValueNode*)pNew;

  ASSERT(pValue->node.resType.type == TSDB_DATA_TYPE_BOOL);
  *pQualified = pValue->datum.b;

  nodesDestroyNode(pNew);
  return TSDB_CODE_SUCCESS;
}

static EDealRes getColumn(SNode** pNode, void* pContext) {
  SColumnNode* pSColumnNode = NULL;
  if (QUERY_NODE_COLUMN == nodeType((*pNode))) {
    pSColumnNode = *(SColumnNode**)pNode;
  } else if (QUERY_NODE_FUNCTION == nodeType((*pNode))) {
    SFunctionNode* pFuncNode = *(SFunctionNode**)(pNode);
    if (pFuncNode->funcType == FUNCTION_TYPE_TBNAME) {
      pSColumnNode = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      if (NULL == pSColumnNode) {
        return DEAL_RES_ERROR;
      }
      pSColumnNode->colId = -1;
      pSColumnNode->colType = COLUMN_TYPE_TBNAME;
      pSColumnNode->node.resType.type = TSDB_DATA_TYPE_VARCHAR;
      pSColumnNode->node.resType.bytes = TSDB_TABLE_FNAME_LEN - 1 + VARSTR_HEADER_SIZE;
      nodesDestroyNode(*pNode);
      *pNode = (SNode*)pSColumnNode;
    } else {
      return DEAL_RES_CONTINUE;
    }
  } else {
    return DEAL_RES_CONTINUE;
  }

  tagFilterAssist* pData = (tagFilterAssist*)pContext;
  void*            data = taosHashGet(pData->colHash, &pSColumnNode->colId, sizeof(pSColumnNode->colId));
  if (!data) {
    taosHashPut(pData->colHash, &pSColumnNode->colId, sizeof(pSColumnNode->colId), pNode, sizeof((*pNode)));
    pSColumnNode->slotId = pData->index++;
    SColumnInfo cInfo = {.colId = pSColumnNode->colId,
                         .type = pSColumnNode->node.resType.type,
                         .bytes = pSColumnNode->node.resType.bytes};
#if TAG_FILTER_DEBUG
    qDebug("tagfilter build column info, slotId:%d, colId:%d, type:%d", pSColumnNode->slotId, cInfo.colId, cInfo.type);
#endif
    taosArrayPush(pData->cInfoList, &cInfo);
  } else {
    SColumnNode* col = *(SColumnNode**)data;
    pSColumnNode->slotId = col->slotId;
  }

  return DEAL_RES_CONTINUE;
}

static int32_t createResultData(SDataType* pType, int32_t numOfRows, SScalarParam* pParam) {
  SColumnInfoData* pColumnData = taosMemoryCalloc(1, sizeof(SColumnInfoData));
  if (pColumnData == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  pColumnData->info.type = pType->type;
  pColumnData->info.bytes = pType->bytes;
  pColumnData->info.scale = pType->scale;
  pColumnData->info.precision = pType->precision;

  int32_t code = colInfoDataEnsureCapacity(pColumnData, numOfRows, true);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    taosMemoryFree(pColumnData);
    return terrno;
  }

  pParam->columnData = pColumnData;
  pParam->colAlloced = true;
  return TSDB_CODE_SUCCESS;
}

static SColumnInfoData* getColInfoResult(void* metaHandle, int64_t suid, SArray* uidList, SNode* pTagCond) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SArray*      pBlockList = NULL;
  SSDataBlock* pResBlock = NULL;
  SHashObj*    tags = NULL;
  SScalarParam output = {0};

  tagFilterAssist ctx = {0};

  ctx.colHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT), false, HASH_NO_LOCK);
  if (ctx.colHash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  ctx.cInfoList = taosArrayInit(4, sizeof(SColumnInfo));
  if (ctx.cInfoList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  nodesRewriteExprPostOrder(&pTagCond, getColumn, (void*)&ctx);

  pResBlock = createDataBlock();
  if (pResBlock == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  for (int32_t i = 0; i < taosArrayGetSize(ctx.cInfoList); ++i) {
    SColumnInfoData colInfo = {0};
    colInfo.info = *(SColumnInfo*)taosArrayGet(ctx.cInfoList, i);
    blockDataAppendColInfo(pResBlock, &colInfo);
  }

  //  int64_t stt = taosGetTimestampUs();
  tags = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);

  int32_t filter = optimizeTbnameInCond(metaHandle, suid, uidList, pTagCond, tags);
  if (filter == -1) {
    code = metaGetTableTags(metaHandle, suid, uidList, tags);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get table tags from meta, reason:%s, suid:%" PRIu64, tstrerror(code), suid);
      terrno = code;
      goto end;
    }
  }
  if (suid != 0) {
    removeInvalidTable(uidList, tags);
  }

  int32_t rows = taosArrayGetSize(uidList);
  if (rows == 0) {
    goto end;
  }

  code = blockDataEnsureCapacity(pResBlock, rows);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    goto end;
  }

  for (int32_t i = 0; i < rows; i++) {
    int64_t* uid = taosArrayGet(uidList, i);
    for (int32_t j = 0; j < taosArrayGetSize(pResBlock->pDataBlock); j++) {
      SColumnInfoData* pColInfo = (SColumnInfoData*)taosArrayGet(pResBlock->pDataBlock, j);

      if (pColInfo->info.colId == -1) {  // tbname
        char str[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
        metaGetTableNameByUid(metaHandle, *uid, str);
        colDataAppend(pColInfo, i, str, false);
#if TAG_FILTER_DEBUG
        qDebug("tagfilter uid:%ld, tbname:%s", *uid, str + 2);
#endif
      } else {
        void* tag = taosHashGet(tags, uid, sizeof(int64_t));
        if (tag == NULL) {
          continue;
        }
        STagVal tagVal = {0};
        tagVal.cid = pColInfo->info.colId;
        const char* p = metaGetTableTagVal(tag, pColInfo->info.type, &tagVal);

        if (p == NULL || (pColInfo->info.type == TSDB_DATA_TYPE_JSON && ((STag*)p)->nTag == 0)) {
          colDataAppend(pColInfo, i, p, true);
        } else if (pColInfo->info.type == TSDB_DATA_TYPE_JSON) {
          colDataAppend(pColInfo, i, p, false);
        } else if (IS_VAR_DATA_TYPE(pColInfo->info.type)) {
          char* tmp = taosMemoryCalloc(tagVal.nData + VARSTR_HEADER_SIZE + 1, 1);
          varDataSetLen(tmp, tagVal.nData);
          memcpy(tmp + VARSTR_HEADER_SIZE, tagVal.pData, tagVal.nData);
          colDataAppend(pColInfo, i, tmp, false);
#if TAG_FILTER_DEBUG
          qDebug("tagfilter varch:%s", tmp + 2);
#endif
          taosMemoryFree(tmp);
        } else {
          colDataAppend(pColInfo, i, (const char*)&tagVal.i64, false);
#if TAG_FILTER_DEBUG
          if (pColInfo->info.type == TSDB_DATA_TYPE_INT) {
            qDebug("tagfilter int:%d", *(int*)(&tagVal.i64));
          } else if (pColInfo->info.type == TSDB_DATA_TYPE_DOUBLE) {
            qDebug("tagfilter double:%f", *(double*)(&tagVal.i64));
          }
#endif
        }
      }
    }
  }

  pResBlock->info.rows = rows;

  //  int64_t st1 = taosGetTimestampUs();
  //  qDebug("generate tag block rows:%d, cost:%ld us", rows, st1-st);

  pBlockList = taosArrayInit(2, POINTER_BYTES);
  taosArrayPush(pBlockList, &pResBlock);

  SDataType type = {.type = TSDB_DATA_TYPE_BOOL, .bytes = sizeof(bool)};
  code = createResultData(&type, rows, &output);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    qError("failed to create result, reason:%s", tstrerror(code));
    goto end;
  }

  code = scalarCalculate(pTagCond, pBlockList, &output);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to calculate scalar, reason:%s", tstrerror(code));
    terrno = code;
    goto end;
  }
  //  int64_t st2 = taosGetTimestampUs();
  //  qDebug("calculate tag block rows:%d, cost:%ld us", rows, st2-st1);

end:
  taosHashCleanup(tags);
  taosHashCleanup(ctx.colHash);
  taosArrayDestroy(ctx.cInfoList);
  blockDataDestroy(pResBlock);
  taosArrayDestroy(pBlockList);
  return output.columnData;
}

static void releaseColInfoData(void* pCol) {
  if (pCol) {
    SColumnInfoData* col = (SColumnInfoData*)pCol;
    colDataDestroy(col);
    taosMemoryFree(col);
  }
}

int32_t getColInfoResultForGroupby(void* metaHandle, SNodeList* group, STableListInfo* pTableListInfo) {
  int32_t      code = TSDB_CODE_SUCCESS;
  SArray*      pBlockList = NULL;
  SSDataBlock* pResBlock = NULL;
  SHashObj*    tags = NULL;
  SArray*      uidList = NULL;
  void*        keyBuf = NULL;
  SArray*      groupData = NULL;

  int32_t rows = taosArrayGetSize(pTableListInfo->pTableList);
  if (rows == 0) {
    return TDB_CODE_SUCCESS;
  }

  tagFilterAssist ctx = {0};
  ctx.colHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT), false, HASH_NO_LOCK);
  if (ctx.colHash == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  ctx.index = 0;
  ctx.cInfoList = taosArrayInit(4, sizeof(SColumnInfo));
  if (ctx.cInfoList == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, group) {
    nodesRewriteExprPostOrder(&pNode, getColumn, (void*)&ctx);
    REPLACE_NODE(pNode);
  }

  pResBlock = createDataBlock();
  if (pResBlock == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  for (int32_t i = 0; i < taosArrayGetSize(ctx.cInfoList); ++i) {
    SColumnInfoData colInfo = {0};
    colInfo.info = *(SColumnInfo*)taosArrayGet(ctx.cInfoList, i);
    blockDataAppendColInfo(pResBlock, &colInfo);
  }

  uidList = taosArrayInit(rows, sizeof(uint64_t));
  for (int32_t i = 0; i < rows; ++i) {
    STableKeyInfo* pkeyInfo = taosArrayGet(pTableListInfo->pTableList, i);
    taosArrayPush(uidList, &pkeyInfo->uid);
  }

  //  int64_t stt = taosGetTimestampUs();
  tags = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  code = metaGetTableTags(metaHandle, pTableListInfo->suid, uidList, tags);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  //  int64_t stt1 = taosGetTimestampUs();
  //  qDebug("generate tag meta rows:%d, cost:%ld us", rows, stt1-stt);

  code = blockDataEnsureCapacity(pResBlock, rows);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  //  int64_t st = taosGetTimestampUs();
  for (int32_t i = 0; i < rows; i++) {
    int64_t* uid = taosArrayGet(uidList, i);
    for (int32_t j = 0; j < taosArrayGetSize(pResBlock->pDataBlock); j++) {
      SColumnInfoData* pColInfo = (SColumnInfoData*)taosArrayGet(pResBlock->pDataBlock, j);

      if (pColInfo->info.colId == -1) {  // tbname
        char str[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
        metaGetTableNameByUid(metaHandle, *uid, str);
        colDataAppend(pColInfo, i, str, false);
#if TAG_FILTER_DEBUG
        qDebug("tagfilter uid:%ld, tbname:%s", *uid, str + 2);
#endif
      } else {
        void* tag = taosHashGet(tags, uid, sizeof(int64_t));
        ASSERT(tag);

        STagVal tagVal = {0};
        tagVal.cid = pColInfo->info.colId;
        const char* p = metaGetTableTagVal(tag, pColInfo->info.type, &tagVal);

        if (p == NULL || (pColInfo->info.type == TSDB_DATA_TYPE_JSON && ((STag*)p)->nTag == 0)) {
          colDataAppend(pColInfo, i, p, true);
        } else if (pColInfo->info.type == TSDB_DATA_TYPE_JSON) {
          colDataAppend(pColInfo, i, p, false);
        } else if (IS_VAR_DATA_TYPE(pColInfo->info.type)) {
          char* tmp = taosMemoryCalloc(tagVal.nData + VARSTR_HEADER_SIZE + 1, 1);
          varDataSetLen(tmp, tagVal.nData);
          memcpy(tmp + VARSTR_HEADER_SIZE, tagVal.pData, tagVal.nData);
          colDataAppend(pColInfo, i, tmp, false);
#if TAG_FILTER_DEBUG
          qDebug("tagfilter varch:%s", tmp + 2);
#endif
          taosMemoryFree(tmp);
        } else {
          colDataAppend(pColInfo, i, (const char*)&tagVal.i64, false);
#if TAG_FILTER_DEBUG
          if (pColInfo->info.type == TSDB_DATA_TYPE_INT) {
            qDebug("tagfilter int:%d", *(int*)(&tagVal.i64));
          } else if (pColInfo->info.type == TSDB_DATA_TYPE_DOUBLE) {
            qDebug("tagfilter double:%f", *(double*)(&tagVal.i64));
          }
#endif
        }
      }
    }
  }

  pResBlock->info.rows = rows;

  //  int64_t st1 = taosGetTimestampUs();
  //  qDebug("generate tag block rows:%d, cost:%ld us", rows, st1-st);

  pBlockList = taosArrayInit(2, POINTER_BYTES);
  taosArrayPush(pBlockList, &pResBlock);

  groupData = taosArrayInit(2, POINTER_BYTES);
  FOREACH(pNode, group) {
    SScalarParam output = {0};

    switch (nodeType(pNode)) {
      case QUERY_NODE_VALUE:
        break;
      case QUERY_NODE_COLUMN:
      case QUERY_NODE_OPERATOR:
      case QUERY_NODE_FUNCTION: {
        SExprNode* expNode = (SExprNode*)pNode;
        code = createResultData(&expNode->resType, rows, &output);
        if (code != TSDB_CODE_SUCCESS) {
          goto end;
        }
        break;
      }

      default:
        code = TSDB_CODE_OPS_NOT_SUPPORT;
        goto end;
    }

    if (nodeType(pNode) == QUERY_NODE_COLUMN) {
      SColumnNode*     pSColumnNode = (SColumnNode*)pNode;
      SColumnInfoData* pColInfo = (SColumnInfoData*)taosArrayGet(pResBlock->pDataBlock, pSColumnNode->slotId);
      code = colDataAssign(output.columnData, pColInfo, rows, NULL);
    } else if (nodeType(pNode) == QUERY_NODE_VALUE) {
      continue;
    } else {
      code = scalarCalculate(pNode, pBlockList, &output);
    }

    if (code != TSDB_CODE_SUCCESS) {
      releaseColInfoData(output.columnData);
      goto end;
    }

    taosArrayPush(groupData, &output.columnData);
  }

  int32_t keyLen = 0;
  SNode*  node;
  FOREACH(node, group) {
    SExprNode* pExpr = (SExprNode*)node;
    keyLen += pExpr->resType.bytes;
  }

  int32_t nullFlagSize = sizeof(int8_t) * LIST_LENGTH(group);
  keyLen += nullFlagSize;

  keyBuf = taosMemoryCalloc(1, keyLen);
  if (keyBuf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  for (int i = 0; i < rows; i++) {
    STableKeyInfo* info = taosArrayGet(pTableListInfo->pTableList, i);

    char* isNull = (char*)keyBuf;
    char* pStart = (char*)keyBuf + sizeof(int8_t) * LIST_LENGTH(group);
    for (int j = 0; j < taosArrayGetSize(groupData); j++) {
      SColumnInfoData* pValue = (SColumnInfoData*)taosArrayGetP(groupData, j);

      if (colDataIsNull_s(pValue, i)) {
        isNull[j] = 1;
      } else {
        isNull[j] = 0;
        char* data = colDataGetData(pValue, i);
        if (pValue->info.type == TSDB_DATA_TYPE_JSON) {
          if (tTagIsJson(data)) {
            code = TSDB_CODE_QRY_JSON_IN_GROUP_ERROR;
            goto end;
          }
          if (tTagIsJsonNull(data)) {
            isNull[j] = 1;
            continue;
          }
          int32_t len = getJsonValueLen(data);
          memcpy(pStart, data, len);
          pStart += len;
        } else if (IS_VAR_DATA_TYPE(pValue->info.type)) {
          memcpy(pStart, data, varDataTLen(data));
          pStart += varDataTLen(data);
        } else {
          memcpy(pStart, data, pValue->info.bytes);
          pStart += pValue->info.bytes;
        }
      }
    }

    int32_t len = (int32_t)(pStart - (char*)keyBuf);
    info->groupId = calcGroupId(keyBuf, len);
  }

  //  int64_t st2 = taosGetTimestampUs();
  //  qDebug("calculate tag block rows:%d, cost:%ld us", rows, st2-st1);

end:
  taosMemoryFreeClear(keyBuf);
  taosHashCleanup(tags);
  taosHashCleanup(ctx.colHash);
  taosArrayDestroy(ctx.cInfoList);
  blockDataDestroy(pResBlock);
  taosArrayDestroy(pBlockList);
  taosArrayDestroy(uidList);
  taosArrayDestroyP(groupData, releaseColInfoData);
  return code;
}

static int32_t nameComparFn(const void* p1, const void* p2) {
  const char* pName1 = *(const char**)p1;
  const char* pName2 = *(const char**)p2;

  int32_t ret = strcmp(pName1, pName2);
  if (ret == 0) {
    return 0;
  } else {
    return (ret > 0) ? 1 : -1;
  }
}

static SArray* getTableNameList(const SNodeListNode* pList) {
  int32_t    len = LIST_LENGTH(pList->pNodeList);
  SListCell* cell = pList->pNodeList->pHead;

  SArray* pTbList = taosArrayInit(len, POINTER_BYTES);
  for (int i = 0; i < pList->pNodeList->length; i++) {
    SValueNode* valueNode = (SValueNode*)cell->pNode;
    if (!IS_VAR_DATA_TYPE(valueNode->node.resType.type)) {
      terrno = TSDB_CODE_INVALID_PARA;
      taosArrayDestroy(pTbList);
      return NULL;
    }

    char* name = varDataVal(valueNode->datum.p);
    taosArrayPush(pTbList, &name);
    cell = cell->pNext;
  }

  size_t numOfTables = taosArrayGetSize(pTbList);

  // order the name
  taosArraySort(pTbList, nameComparFn);

  // remove the duplicates
  SArray* pNewList = taosArrayInit(taosArrayGetSize(pTbList), sizeof(void*));
  taosArrayPush(pNewList, taosArrayGet(pTbList, 0));

  for (int32_t i = 1; i < numOfTables; ++i) {
    char** name = taosArrayGetLast(pNewList);
    char** nameInOldList = taosArrayGet(pTbList, i);
    if (strcmp(*name, *nameInOldList) == 0) {
      continue;
    }

    taosArrayPush(pNewList, nameInOldList);
  }

  taosArrayDestroy(pTbList);
  return pNewList;
}

static int tableUidCompare(const void* a, const void* b) {
  uint64_t u1 = *(uint64_t*)a;
  uint64_t u2 = *(uint64_t*)b;

  if (u1 == u2) {
    return 0;
  }

  return u1 < u2 ? -1 : 1;
}

static int32_t optimizeTbnameInCond(void* metaHandle, int64_t suid, SArray* list, SNode* cond, SHashObj* tags) {
  int32_t ret = -1;
  if (nodeType(cond) == QUERY_NODE_OPERATOR) {
    ret = optimizeTbnameInCondImpl(metaHandle, suid, list, cond);
    if (ret != -1) {
      metaGetTableTagsByUids(metaHandle, suid, list, tags);
      removeInvalidTable(list, tags);
    }
  }

  if (nodeType(cond) != QUERY_NODE_LOGIC_CONDITION || ((SLogicConditionNode*)cond)->condType != LOGIC_COND_TYPE_AND) {
    return ret;
  }

  bool                 hasTbnameCond = false;
  SLogicConditionNode* pNode = (SLogicConditionNode*)cond;
  SNodeList*           pList = (SNodeList*)pNode->pParameterList;

  int32_t len = LIST_LENGTH(pList);
  if (len <= 0) {
    return ret;
  }

  SListCell* cell = pList->pHead;
  for (int i = 0; i < len; i++) {
    if (cell == NULL) break;
    if (optimizeTbnameInCondImpl(metaHandle, suid, list, cell->pNode) == 0) {
      hasTbnameCond = true;
      break;
    }
    cell = cell->pNext;
  }

  taosArraySort(list, tableUidCompare);
  taosArrayRemoveDuplicate(list, tableUidCompare, NULL);

  if (hasTbnameCond) {
    ret = metaGetTableTagsByUids(metaHandle, suid, list, tags);
    removeInvalidTable(list, tags);
  }

  return ret;
}

/*
 * handle invalid uid
 */
static int32_t removeInvalidTable(SArray* uids, SHashObj* tags) {
  if (taosArrayGetSize(uids) <= 0) return 0;

  SArray* validUid = taosArrayInit(taosArrayGetSize(uids), sizeof(int64_t));

  for (int32_t i = 0; i < taosArrayGetSize(uids); i++) {
    int64_t* uid = taosArrayGet(uids, i);
    if (taosHashGet(tags, uid, sizeof(int64_t)) != NULL) {
      taosArrayPush(validUid, uid);
    }
  }

  taosArraySwap(uids, validUid);
  taosArrayDestroy(validUid);
  return 0;
}

static int32_t optimizeTbnameInCondImpl(void* metaHandle, int64_t suid, SArray* list, SNode* pTagCond) {
  if (nodeType(pTagCond) != QUERY_NODE_OPERATOR) {
    return -1;
  }

  SOperatorNode* pNode = (SOperatorNode*)pTagCond;
  if (pNode->opType != OP_TYPE_IN) {
    return -1;
  }

  if ((pNode->pLeft != NULL && nodeType(pNode->pLeft) == QUERY_NODE_COLUMN &&
       ((SColumnNode*)pNode->pLeft)->colType == COLUMN_TYPE_TBNAME) &&
      (pNode->pRight != NULL && nodeType(pNode->pRight) == QUERY_NODE_NODE_LIST)) {
    SNodeListNode* pList = (SNodeListNode*)pNode->pRight;

    int32_t len = LIST_LENGTH(pList->pNodeList);
    if (len <= 0) {
      return -1;
    }

    SArray*   pTbList = getTableNameList(pList);
    int32_t   numOfTables = taosArrayGetSize(pTbList);
    SHashObj* uHash = NULL;
    size_t    listlen = taosArrayGetSize(list);  // len > 0 means there already have uids
    if (listlen > 0) {
      uHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      for (int i = 0; i < listlen; i++) {
        int64_t* uid = taosArrayGet(list, i);
        taosHashPut(uHash, uid, sizeof(int64_t), &i, sizeof(i));
      }
    }

    for (int i = 0; i < numOfTables; i++) {
      char* name = taosArrayGetP(pTbList, i);

      uint64_t uid = 0;
      if (metaGetTableUidByName(metaHandle, name, &uid) == 0) {
        ETableType tbType = TSDB_TABLE_MAX;
        if (metaGetTableTypeByName(metaHandle, name, &tbType) == 0 && tbType == TSDB_CHILD_TABLE) {
          if (NULL == uHash || taosHashGet(uHash, &uid, sizeof(uid)) == NULL) {
            taosArrayPush(list, &uid);
          }
        } else {
          taosArrayDestroy(pTbList);
          taosHashCleanup(uHash);
          return -1;
        }
      } else {
//        qWarn("failed to get tableIds from by table name: %s, reason: %s", name, tstrerror(terrno));
        terrno = 0;
      }
    }

    taosHashCleanup(uHash);
    taosArrayDestroy(pTbList);
    return 0;
  }

  return -1;
}

static void genTagFilterDigest(const SNode* pTagCond, T_MD5_CTX* pContext) {
  if (pTagCond == NULL) {
    return;
  }

  char*   payload = NULL;
  int32_t len = 0;
  nodesNodeToMsg(pTagCond, &payload, &len);

  tMD5Init(pContext);
  tMD5Update(pContext, (uint8_t*)payload, (uint32_t)len);
  tMD5Final(pContext);

  taosMemoryFree(payload);
}

static int32_t doFilterByTagCond(STableListInfo* pListInfo, SArray* res, SNode* pTagCond, void* metaHandle) {
  if (pTagCond == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  terrno = TDB_CODE_SUCCESS;
  SColumnInfoData* pColInfoData = getColInfoResult(metaHandle, pListInfo->suid, res, pTagCond);
  if (terrno != TDB_CODE_SUCCESS) {
    colDataDestroy(pColInfoData);
    taosMemoryFreeClear(pColInfoData);
    taosArrayDestroy(res);
    qError("failed to getColInfoResult, code: %s", tstrerror(terrno));
    return terrno;
  }

  int32_t i = 0;
  int32_t len = taosArrayGetSize(res);

  if (pColInfoData != NULL) {
    bool*   pResult = (bool*)pColInfoData->pData;
    SArray* p = taosArrayInit(taosArrayGetSize(res), sizeof(uint64_t));

    while (i < len && pColInfoData) {
      int64_t* uid = taosArrayGet(res, i);
      qDebug("tagfilter get uid:%" PRId64 ", res:%d", *uid, pResult[i]);

      if (pResult[i]) {
        taosArrayPush(p, uid);
      }
      i += 1;
    }

    taosArraySwap(res, p);
    taosArrayDestroy(p);
  }

  colDataDestroy(pColInfoData);
  taosMemoryFreeClear(pColInfoData);

  return TSDB_CODE_SUCCESS;
}

int32_t getTableList(void* metaHandle, void* pVnode, SScanPhysiNode* pScanNode, SNode* pTagCond, SNode* pTagIndexCond,
                     STableListInfo* pListInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  size_t  numOfTables = 0;

  uint64_t tableUid = pScanNode->uid;
  pListInfo->suid = pScanNode->suid;
  SArray* res = taosArrayInit(8, sizeof(uint64_t));

  if (pScanNode->tableType != TSDB_SUPER_TABLE) {
    if (metaIsTableExist(metaHandle, tableUid)) {
      taosArrayPush(res, &tableUid);
    }

    code = doFilterByTagCond(pListInfo, res, pTagCond, metaHandle);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else {
    // try to retrieve the result from meta cache
    T_MD5_CTX context = {0};
    genTagFilterDigest(pTagCond, &context);

    bool acquired = false;
    metaGetCachedTableUidList(metaHandle, pScanNode->suid, context.digest, tListLen(context.digest), res, &acquired);
    if (acquired) {
      qDebug("retrieve table uid list from cache, numOfTables:%d", (int32_t)taosArrayGetSize(res));
      goto _end;
    }

    if (!pTagCond) {  // no tag condition exists, let's fetch all tables of this super table
      ASSERT(pTagIndexCond == NULL);
      vnodeGetCtbIdList(pVnode, pScanNode->suid, res);
    } else {
      // failed to find the result in the cache, let try to calculate the results
      if (pTagIndexCond) {
        SIndexMetaArg metaArg = {
            .metaEx = metaHandle, .idx = tsdbGetIdx(metaHandle), .ivtIdx = tsdbGetIvtIdx(metaHandle), .suid = tableUid};

        SIdxFltStatus status = SFLT_NOT_INDEX;
        code = doFilterTag(pTagIndexCond, &metaArg, res, &status);
        if (code != 0 || status == SFLT_NOT_INDEX) {
          qError("failed to get tableIds from index, reason:%s, suid:%" PRIu64, tstrerror(code), tableUid);
          code = TDB_CODE_SUCCESS;
        }
      }
    }

    code = doFilterByTagCond(pListInfo, res, pTagCond, metaHandle);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // let's add the filter results into meta-cache
    numOfTables = taosArrayGetSize(res);
    size_t size = numOfTables * sizeof(uint64_t) + sizeof(int32_t);
    char*  pPayload = taosMemoryMalloc(size);
    *(int32_t*)pPayload = numOfTables;

    if (numOfTables > 0) {
      memcpy(pPayload + sizeof(int32_t), taosArrayGet(res, 0), numOfTables * sizeof(uint64_t));
    }

    metaUidFilterCachePut(metaHandle, pScanNode->suid, context.digest, tListLen(context.digest), pPayload, size, 1);
  }

_end:
  numOfTables = taosArrayGetSize(res);
  for (int i = 0; i < numOfTables; i++) {
    STableKeyInfo info = {.uid = *(uint64_t*)taosArrayGet(res, i), .groupId = 0};

    void* p = taosArrayPush(pListInfo->pTableList, &info);
    if (p == NULL) {
      taosArrayDestroy(res);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    qTrace("tagfilter get uid:%" PRIu64 "", info.uid);
  }

  taosArrayDestroy(res);
  return code;
}

size_t getTableTagsBufLen(const SNodeList* pGroups) {
  size_t keyLen = 0;

  SNode* node;
  FOREACH(node, pGroups) {
    SExprNode* pExpr = (SExprNode*)node;
    keyLen += pExpr->resType.bytes;
  }

  keyLen += sizeof(int8_t) * LIST_LENGTH(pGroups);
  return keyLen;
}

int32_t getGroupIdFromTagsVal(void* pMeta, uint64_t uid, SNodeList* pGroupNode, char* keyBuf, uint64_t* pGroupId) {
  SMetaReader mr = {0};
  metaReaderInit(&mr, pMeta, 0);
  if (metaGetTableEntryByUidCache(&mr, uid) != 0) {  // table not exist
    metaReaderClear(&mr);
    return TSDB_CODE_PAR_TABLE_NOT_EXIST;
  }

  SNodeList* groupNew = nodesCloneList(pGroupNode);

  nodesRewriteExprsPostOrder(groupNew, doTranslateTagExpr, &mr);
  char* isNull = (char*)keyBuf;
  char* pStart = (char*)keyBuf + sizeof(int8_t) * LIST_LENGTH(pGroupNode);

  SNode*  pNode;
  int32_t index = 0;
  FOREACH(pNode, groupNew) {
    SNode*  pNew = NULL;
    int32_t code = scalarCalculateConstants(pNode, &pNew);
    if (TSDB_CODE_SUCCESS == code) {
      REPLACE_NODE(pNew);
    } else {
      taosMemoryFree(keyBuf);
      nodesDestroyList(groupNew);
      metaReaderClear(&mr);
      return code;
    }

    ASSERT(nodeType(pNew) == QUERY_NODE_VALUE);
    SValueNode* pValue = (SValueNode*)pNew;

    if (pValue->node.resType.type == TSDB_DATA_TYPE_NULL || pValue->isNull) {
      isNull[index++] = 1;
      continue;
    } else {
      isNull[index++] = 0;
      char* data = nodesGetValueFromNode(pValue);
      if (pValue->node.resType.type == TSDB_DATA_TYPE_JSON) {
        if (tTagIsJson(data)) {
          terrno = TSDB_CODE_QRY_JSON_IN_GROUP_ERROR;
          taosMemoryFree(keyBuf);
          nodesDestroyList(groupNew);
          metaReaderClear(&mr);
          return terrno;
        }
        int32_t len = getJsonValueLen(data);
        memcpy(pStart, data, len);
        pStart += len;
      } else if (IS_VAR_DATA_TYPE(pValue->node.resType.type)) {
        memcpy(pStart, data, varDataTLen(data));
        pStart += varDataTLen(data);
      } else {
        memcpy(pStart, data, pValue->node.resType.bytes);
        pStart += pValue->node.resType.bytes;
      }
    }
  }

  int32_t len = (int32_t)(pStart - (char*)keyBuf);
  *pGroupId = calcGroupId(keyBuf, len);

  nodesDestroyList(groupNew);
  metaReaderClear(&mr);
  return TSDB_CODE_SUCCESS;
}

SArray* extractPartitionColInfo(SNodeList* pNodeList) {
  if (!pNodeList) {
    return NULL;
  }

  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(SColumn));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnNode* pColNode = (SColumnNode*)nodesListGetNode(pNodeList, i);

    // todo extract method
    SColumn c = {0};
    c.slotId = pColNode->slotId;
    c.colId = pColNode->colId;
    c.type = pColNode->node.resType.type;
    c.bytes = pColNode->node.resType.bytes;
    c.precision = pColNode->node.resType.precision;
    c.scale = pColNode->node.resType.scale;

    taosArrayPush(pList, &c);
  }

  return pList;
}

int32_t extractColMatchInfo(SNodeList* pNodeList, SDataBlockDescNode* pOutputNodeList, int32_t* numOfOutputCols,
                            int32_t type, SColMatchInfo* pMatchInfo) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  int32_t code = 0;

  pMatchInfo->matchType = type;

  SArray* pList = taosArrayInit(numOfCols, sizeof(SColMatchItem));
  if (pList == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pNodeList, i);
    if (nodeType(pNode->pExpr) == QUERY_NODE_COLUMN) {
      SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

      SColMatchItem c = {.needOutput = true};
      c.colId = pColNode->colId;
      c.srcSlotId = pColNode->slotId;
      c.dstSlotId = pNode->slotId;
      taosArrayPush(pList, &c);
    }
  }

  // set the output flag for each column in SColMatchInfo, according to the
  *numOfOutputCols = 0;
  int32_t num = LIST_LENGTH(pOutputNodeList->pSlots);
  for (int32_t i = 0; i < num; ++i) {
    SSlotDescNode* pNode = (SSlotDescNode*)nodesListGetNode(pOutputNodeList->pSlots, i);

    // todo: add reserve flag check
    // it is a column reserved for the arithmetic expression calculation
    if (pNode->slotId >= numOfCols) {
      (*numOfOutputCols) += 1;
      continue;
    }

    SColMatchItem* info = NULL;
    for (int32_t j = 0; j < taosArrayGetSize(pList); ++j) {
      info = taosArrayGet(pList, j);
      if (info->dstSlotId == pNode->slotId) {
        break;
      }
    }

    if (pNode->output) {
      (*numOfOutputCols) += 1;
    } else if (info != NULL) {
      // select distinct tbname from stb where tbname='abc';
      info->needOutput = false;
    }
  }

  pMatchInfo->pList = pList;
  return code;
}

static SResSchema createResSchema(int32_t type, int32_t bytes, int32_t slotId, int32_t scale, int32_t precision,
                                  const char* name) {
  SResSchema s = {0};
  s.scale = scale;
  s.type = type;
  s.bytes = bytes;
  s.slotId = slotId;
  s.precision = precision;
  tstrncpy(s.name, name, tListLen(s.name));

  return s;
}

static SColumn* createColumn(int32_t blockId, int32_t slotId, int32_t colId, SDataType* pType, EColumnType colType) {
  SColumn* pCol = taosMemoryCalloc(1, sizeof(SColumn));
  if (pCol == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pCol->slotId = slotId;
  pCol->colId = colId;
  pCol->bytes = pType->bytes;
  pCol->type = pType->type;
  pCol->scale = pType->scale;
  pCol->precision = pType->precision;
  pCol->dataBlockId = blockId;
  pCol->colType = colType;
  return pCol;
}

void createExprFromOneNode(SExprInfo* pExp, SNode* pNode, int16_t slotId) {
  pExp->pExpr = taosMemoryCalloc(1, sizeof(tExprNode));
  pExp->pExpr->_function.num = 1;
  pExp->pExpr->_function.functionId = -1;

  int32_t type = nodeType(pNode);
  // it is a project query, or group by column
  if (type == QUERY_NODE_COLUMN) {
    pExp->pExpr->nodeType = QUERY_NODE_COLUMN;
    SColumnNode* pColNode = (SColumnNode*)pNode;

    pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
    pExp->base.numOfParams = 1;

    SDataType* pType = &pColNode->node.resType;
    pExp->base.resSchema =
        createResSchema(pType->type, pType->bytes, slotId, pType->scale, pType->precision, pColNode->colName);
    pExp->base.pParam[0].pCol =
        createColumn(pColNode->dataBlockId, pColNode->slotId, pColNode->colId, pType, pColNode->colType);
    pExp->base.pParam[0].type = FUNC_PARAM_TYPE_COLUMN;
  } else if (type == QUERY_NODE_VALUE) {
    pExp->pExpr->nodeType = QUERY_NODE_VALUE;
    SValueNode* pValNode = (SValueNode*)pNode;

    pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
    pExp->base.numOfParams = 1;

    SDataType* pType = &pValNode->node.resType;
    pExp->base.resSchema =
        createResSchema(pType->type, pType->bytes, slotId, pType->scale, pType->precision, pValNode->node.aliasName);
    pExp->base.pParam[0].type = FUNC_PARAM_TYPE_VALUE;
    nodesValueNodeToVariant(pValNode, &pExp->base.pParam[0].param);
  } else if (type == QUERY_NODE_FUNCTION) {
    pExp->pExpr->nodeType = QUERY_NODE_FUNCTION;
    SFunctionNode* pFuncNode = (SFunctionNode*)pNode;

    SDataType* pType = &pFuncNode->node.resType;
    pExp->base.resSchema =
        createResSchema(pType->type, pType->bytes, slotId, pType->scale, pType->precision, pFuncNode->node.aliasName);

    tExprNode* pExprNode = pExp->pExpr;

    pExprNode->_function.functionId = pFuncNode->funcId;
    pExprNode->_function.pFunctNode = pFuncNode;
    pExprNode->_function.functionType = pFuncNode->funcType;

    tstrncpy(pExprNode->_function.functionName, pFuncNode->functionName, tListLen(pExprNode->_function.functionName));

#if 1
    // todo refactor: add the parameter for tbname function
    const char* name = "tbname";
    int32_t     len = strlen(name);

    if (!pFuncNode->pParameterList && (memcmp(pExprNode->_function.functionName, name, len) == 0) &&
        pExprNode->_function.functionName[len] == 0) {
      pFuncNode->pParameterList = nodesMakeList();
      ASSERT(LIST_LENGTH(pFuncNode->pParameterList) == 0);
      SValueNode* res = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
      if (NULL == res) {  // todo handle error
      } else {
        res->node.resType = (SDataType){.bytes = sizeof(int64_t), .type = TSDB_DATA_TYPE_BIGINT};
        nodesListAppend(pFuncNode->pParameterList, (SNode*)res);
      }
    }
#endif

    int32_t numOfParam = LIST_LENGTH(pFuncNode->pParameterList);

    pExp->base.pParam = taosMemoryCalloc(numOfParam, sizeof(SFunctParam));
    pExp->base.numOfParams = numOfParam;

    for (int32_t j = 0; j < numOfParam; ++j) {
      SNode* p1 = nodesListGetNode(pFuncNode->pParameterList, j);
      if (p1->type == QUERY_NODE_COLUMN) {
        SColumnNode* pcn = (SColumnNode*)p1;

        pExp->base.pParam[j].type = FUNC_PARAM_TYPE_COLUMN;
        pExp->base.pParam[j].pCol =
            createColumn(pcn->dataBlockId, pcn->slotId, pcn->colId, &pcn->node.resType, pcn->colType);
      } else if (p1->type == QUERY_NODE_VALUE) {
        SValueNode* pvn = (SValueNode*)p1;
        pExp->base.pParam[j].type = FUNC_PARAM_TYPE_VALUE;
        nodesValueNodeToVariant(pvn, &pExp->base.pParam[j].param);
      }
    }
  } else if (type == QUERY_NODE_OPERATOR) {
    pExp->pExpr->nodeType = QUERY_NODE_OPERATOR;
    SOperatorNode* pOpNode = (SOperatorNode*)pNode;

    pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
    pExp->base.numOfParams = 1;

    SDataType* pType = &pOpNode->node.resType;
    pExp->base.resSchema =
        createResSchema(pType->type, pType->bytes, slotId, pType->scale, pType->precision, pOpNode->node.aliasName);
    pExp->pExpr->_optrRoot.pRootNode = pNode;
  } else if (type == QUERY_NODE_CASE_WHEN) {
    pExp->pExpr->nodeType = QUERY_NODE_OPERATOR;
    SCaseWhenNode* pCaseNode = (SCaseWhenNode*)pNode;

    pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
    pExp->base.numOfParams = 1;

    SDataType* pType = &pCaseNode->node.resType;
    pExp->base.resSchema =
        createResSchema(pType->type, pType->bytes, slotId, pType->scale, pType->precision, pCaseNode->node.aliasName);
    pExp->pExpr->_optrRoot.pRootNode = pNode;
  } else {
    ASSERT(0);
  }
}

void createExprFromTargetNode(SExprInfo* pExp, STargetNode* pTargetNode) {
  createExprFromOneNode(pExp, pTargetNode->pExpr, pTargetNode->slotId);
}

SExprInfo* createExprInfo(SNodeList* pNodeList, SNodeList* pGroupKeys, int32_t* numOfExprs) {
  int32_t numOfFuncs = LIST_LENGTH(pNodeList);
  int32_t numOfGroupKeys = 0;
  if (pGroupKeys != NULL) {
    numOfGroupKeys = LIST_LENGTH(pGroupKeys);
  }

  *numOfExprs = numOfFuncs + numOfGroupKeys;
  if (*numOfExprs == 0) {
    return NULL;
  }

  SExprInfo* pExprs = taosMemoryCalloc(*numOfExprs, sizeof(SExprInfo));

  for (int32_t i = 0; i < (*numOfExprs); ++i) {
    STargetNode* pTargetNode = NULL;
    if (i < numOfFuncs) {
      pTargetNode = (STargetNode*)nodesListGetNode(pNodeList, i);
    } else {
      pTargetNode = (STargetNode*)nodesListGetNode(pGroupKeys, i - numOfFuncs);
    }

    SExprInfo* pExp = &pExprs[i];
    createExprFromTargetNode(pExp, pTargetNode);
  }

  return pExprs;
}

// set the output buffer for the selectivity + tag query
static int32_t setSelectValueColumnInfo(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  int32_t num = 0;

  SqlFunctionCtx*  p = NULL;
  SqlFunctionCtx** pValCtx = taosMemoryCalloc(numOfOutput, POINTER_BYTES);
  if (pValCtx == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    const char* pName = pCtx[i].pExpr->pExpr->_function.functionName;
    if ((strcmp(pName, "_select_value") == 0) || (strcmp(pName, "_group_key") == 0)) {
      pValCtx[num++] = &pCtx[i];
    } else if (fmIsSelectFunc(pCtx[i].functionId)) {
      p = &pCtx[i];
    }
  }

  if (p != NULL) {
    p->subsidiaries.pCtx = pValCtx;
    p->subsidiaries.num = num;
  } else {
    taosMemoryFreeClear(pValCtx);
  }

  return TSDB_CODE_SUCCESS;
}

SqlFunctionCtx* createSqlFunctionCtx(SExprInfo* pExprInfo, int32_t numOfOutput, int32_t** rowEntryInfoOffset) {
  SqlFunctionCtx* pFuncCtx = (SqlFunctionCtx*)taosMemoryCalloc(numOfOutput, sizeof(SqlFunctionCtx));
  if (pFuncCtx == NULL) {
    return NULL;
  }

  *rowEntryInfoOffset = taosMemoryCalloc(numOfOutput, sizeof(int32_t));
  if (*rowEntryInfoOffset == 0) {
    taosMemoryFreeClear(pFuncCtx);
    return NULL;
  }

  for (int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExpr = &pExprInfo[i];

    SExprBasicInfo* pFunct = &pExpr->base;
    SqlFunctionCtx* pCtx = &pFuncCtx[i];

    pCtx->functionId = -1;
    pCtx->pExpr = pExpr;

    if (pExpr->pExpr->nodeType == QUERY_NODE_FUNCTION) {
      SFuncExecEnv env = {0};
      pCtx->functionId = pExpr->pExpr->_function.pFunctNode->funcId;

      if (fmIsAggFunc(pCtx->functionId) || fmIsIndefiniteRowsFunc(pCtx->functionId)) {
        bool isUdaf = fmIsUserDefinedFunc(pCtx->functionId);
        if (!isUdaf) {
          fmGetFuncExecFuncs(pCtx->functionId, &pCtx->fpSet);
        } else {
          char* udfName = pExpr->pExpr->_function.pFunctNode->functionName;
          tstrncpy(pCtx->udfName, udfName, TSDB_FUNC_NAME_LEN);
          fmGetUdafExecFuncs(pCtx->functionId, &pCtx->fpSet);
        }
        pCtx->fpSet.getEnv(pExpr->pExpr->_function.pFunctNode, &env);
      } else {
        fmGetScalarFuncExecFuncs(pCtx->functionId, &pCtx->sfp);
        if (pCtx->sfp.getEnv != NULL) {
          pCtx->sfp.getEnv(pExpr->pExpr->_function.pFunctNode, &env);
        }
      }
      pCtx->resDataInfo.interBufSize = env.calcMemSize;
    } else if (pExpr->pExpr->nodeType == QUERY_NODE_COLUMN || pExpr->pExpr->nodeType == QUERY_NODE_OPERATOR ||
               pExpr->pExpr->nodeType == QUERY_NODE_VALUE) {
      // for simple column, the result buffer needs to hold at least one element.
      pCtx->resDataInfo.interBufSize = pFunct->resSchema.bytes;
    }

    pCtx->input.numOfInputCols = pFunct->numOfParams;
    pCtx->input.pData = taosMemoryCalloc(pFunct->numOfParams, POINTER_BYTES);
    pCtx->input.pColumnDataAgg = taosMemoryCalloc(pFunct->numOfParams, POINTER_BYTES);

    pCtx->pTsOutput = NULL;
    pCtx->resDataInfo.bytes = pFunct->resSchema.bytes;
    pCtx->resDataInfo.type = pFunct->resSchema.type;
    pCtx->order = TSDB_ORDER_ASC;
    pCtx->start.key = INT64_MIN;
    pCtx->end.key = INT64_MIN;
    pCtx->numOfParams = pExpr->base.numOfParams;
    pCtx->param = pFunct->pParam;
    pCtx->saveHandle.currentPage = -1;
  }

  for (int32_t i = 1; i < numOfOutput; ++i) {
    (*rowEntryInfoOffset)[i] = (int32_t)((*rowEntryInfoOffset)[i - 1] + sizeof(SResultRowEntryInfo) +
                                         pFuncCtx[i - 1].resDataInfo.interBufSize);
  }

  setSelectValueColumnInfo(pFuncCtx, numOfOutput);
  return pFuncCtx;
}

// NOTE: sources columns are more than the destination SSDatablock columns.
// doFilter in table scan needs every column even its output is false
void relocateColumnData(SSDataBlock* pBlock, const SArray* pColMatchInfo, SArray* pCols, bool outputEveryColumn) {
  size_t numOfSrcCols = taosArrayGetSize(pCols);

  int32_t i = 0, j = 0;
  while (i < numOfSrcCols && j < taosArrayGetSize(pColMatchInfo)) {
    SColumnInfoData* p = taosArrayGet(pCols, i);
    SColMatchItem*   pmInfo = taosArrayGet(pColMatchInfo, j);

    if (p->info.colId == pmInfo->colId) {
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, pmInfo->dstSlotId);
      colDataAssign(pDst, p, pBlock->info.rows, &pBlock->info);
      i++;
      j++;
    } else if (p->info.colId < pmInfo->colId) {
      i++;
    } else {
      ASSERT(0);
    }
  }
}

SInterval extractIntervalInfo(const STableScanPhysiNode* pTableScanNode) {
  SInterval interval = {
      .interval = pTableScanNode->interval,
      .sliding = pTableScanNode->sliding,
      .intervalUnit = pTableScanNode->intervalUnit,
      .slidingUnit = pTableScanNode->slidingUnit,
      .offset = pTableScanNode->offset,
  };

  return interval;
}

SColumn extractColumnFromColumnNode(SColumnNode* pColNode) {
  SColumn c = {0};

  c.slotId = pColNode->slotId;
  c.colId = pColNode->colId;
  c.type = pColNode->node.resType.type;
  c.bytes = pColNode->node.resType.bytes;
  c.scale = pColNode->node.resType.scale;
  c.precision = pColNode->node.resType.precision;
  return c;
}

int32_t initQueryTableDataCond(SQueryTableDataCond* pCond, const STableScanPhysiNode* pTableScanNode) {
  pCond->order = pTableScanNode->scanSeq[0] > 0 ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
  pCond->numOfCols = LIST_LENGTH(pTableScanNode->scan.pScanCols);

  pCond->colList = taosMemoryCalloc(pCond->numOfCols, sizeof(SColumnInfo));
  pCond->pSlotList = taosMemoryMalloc(sizeof(int32_t) * pCond->numOfCols);
  if (pCond->colList == NULL || pCond->pSlotList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFreeClear(pCond->colList);
    taosMemoryFreeClear(pCond->pSlotList);
    return terrno;
  }

  // TODO: get it from stable scan node
  pCond->twindows = pTableScanNode->scanRange;
  pCond->suid = pTableScanNode->scan.suid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = -1;

  int32_t j = 0;
  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pTableScanNode->scan.pScanCols, i);
    SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;
    if (pColNode->colType == COLUMN_TYPE_TAG) {
      continue;
    }

    pCond->colList[j].type = pColNode->node.resType.type;
    pCond->colList[j].bytes = pColNode->node.resType.bytes;
    pCond->colList[j].colId = pColNode->colId;

    pCond->pSlotList[j] = pNode->slotId;
    j += 1;
  }

  pCond->numOfCols = j;
  return TSDB_CODE_SUCCESS;
}

void cleanupQueryTableDataCond(SQueryTableDataCond* pCond) {
  taosMemoryFreeClear(pCond->colList);
  taosMemoryFreeClear(pCond->pSlotList);
}

int32_t convertFillType(int32_t mode) {
  int32_t type = TSDB_FILL_NONE;
  switch (mode) {
    case FILL_MODE_PREV:
      type = TSDB_FILL_PREV;
      break;
    case FILL_MODE_NONE:
      type = TSDB_FILL_NONE;
      break;
    case FILL_MODE_NULL:
      type = TSDB_FILL_NULL;
      break;
    case FILL_MODE_NULL_F:
      type = TSDB_FILL_NULL_F;
      break;
    case FILL_MODE_NEXT:
      type = TSDB_FILL_NEXT;
      break;
    case FILL_MODE_VALUE:
      type = TSDB_FILL_SET_VALUE;
      break;
    case FILL_MODE_VALUE_F:
      type = TSDB_FILL_SET_VALUE_F;
      break;
    case FILL_MODE_LINEAR:
      type = TSDB_FILL_LINEAR;
      break;
    default:
      type = TSDB_FILL_NONE;
  }

  return type;
}

static void getInitialStartTimeWindow(SInterval* pInterval, TSKEY ts, STimeWindow* w, bool ascQuery) {
  if (ascQuery) {
    *w = getAlignQueryTimeWindow(pInterval, pInterval->precision, ts);
  } else {
    // the start position of the first time window in the endpoint that spreads beyond the queried last timestamp
    *w = getAlignQueryTimeWindow(pInterval, pInterval->precision, ts);

    int64_t key = w->skey;
    while (key < ts) {  // moving towards end
      key = taosTimeAdd(key, pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
      if (key >= ts) {
        break;
      }

      w->skey = key;
    }
  }
}

static STimeWindow doCalculateTimeWindow(int64_t ts, SInterval* pInterval) {
  STimeWindow w = {0};

  w.skey = taosTimeTruncate(ts, pInterval, pInterval->precision);
  w.ekey = taosTimeAdd(w.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;
  return w;
}

STimeWindow getFirstQualifiedTimeWindow(int64_t ts, STimeWindow* pWindow, SInterval* pInterval, int32_t order) {
  int32_t factor = (order == TSDB_ORDER_ASC) ? -1 : 1;

  STimeWindow win = *pWindow;
  STimeWindow save = win;
  while (win.skey <= ts && win.ekey >= ts) {
    save = win;
    win.skey = taosTimeAdd(win.skey, factor * pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
    win.ekey = taosTimeAdd(win.ekey, factor * pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
  }

  return save;
}

// get the correct time window according to the handled timestamp
// todo refactor
STimeWindow getActiveTimeWindow(SDiskbasedBuf* pBuf, SResultRowInfo* pResultRowInfo, int64_t ts, SInterval* pInterval,
                                int32_t order) {
  STimeWindow w = {0};
  if (pResultRowInfo->cur.pageId == -1) {  // the first window, from the previous stored value
    getInitialStartTimeWindow(pInterval, ts, &w, (order == TSDB_ORDER_ASC));
    w.ekey = taosTimeAdd(w.skey, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;
    return w;
  }

  SResultRow* pRow = getResultRowByPos(pBuf, &pResultRowInfo->cur, false);
  if (pRow) {
    w = pRow->win;
  }
  // in case of typical time window, we can calculate time window directly.
  if (w.skey > ts || w.ekey < ts) {
    w = doCalculateTimeWindow(ts, pInterval);
  }

  if (pInterval->interval != pInterval->sliding) {
    // it is an sliding window query, in which sliding value is not equalled to
    // interval value, and we need to find the first qualified time window.
    w = getFirstQualifiedTimeWindow(ts, &w, pInterval, order);
  }

  return w;
}

bool hasLimitOffsetInfo(SLimitInfo* pLimitInfo) {
  return (pLimitInfo->limit.limit != -1 || pLimitInfo->limit.offset != -1 || pLimitInfo->slimit.limit != -1 ||
          pLimitInfo->slimit.offset != -1);
}

bool hasSlimitOffsetInfo(SLimitInfo* pLimitInfo) {
  return (pLimitInfo->slimit.limit != -1 || pLimitInfo->slimit.offset != -1);
}

void initLimitInfo(const SNode* pLimit, const SNode* pSLimit, SLimitInfo* pLimitInfo) {
  SLimit limit = {.limit = getLimit(pLimit), .offset = getOffset(pLimit)};
  SLimit slimit = {.limit = getLimit(pSLimit), .offset = getOffset(pSLimit)};

  pLimitInfo->limit = limit;
  pLimitInfo->slimit = slimit;
  pLimitInfo->remainOffset = limit.offset;
  pLimitInfo->remainGroupOffset = slimit.offset;
}

void resetLimitInfoForNextGroup(SLimitInfo* pLimitInfo) {
  pLimitInfo->numOfOutputRows = 0;
  pLimitInfo->remainOffset = pLimitInfo->limit.offset;
}

uint64_t tableListGetSize(const STableListInfo* pTableList) {
  ASSERT(taosArrayGetSize(pTableList->pTableList) == taosHashGetSize(pTableList->map));
  return taosArrayGetSize(pTableList->pTableList);
}

uint64_t tableListGetSuid(const STableListInfo* pTableList) { return pTableList->suid; }

STableKeyInfo* tableListGetInfo(const STableListInfo* pTableList, int32_t index) {
  if (taosArrayGetSize(pTableList->pTableList) == 0) {
    return NULL;
  }

  return taosArrayGet(pTableList->pTableList, index);
}

uint64_t getTableGroupId(const STableListInfo* pTableList, uint64_t tableUid) {
  int32_t* slot = taosHashGet(pTableList->map, &tableUid, sizeof(tableUid));
  ASSERT(pTableList->map != NULL && slot != NULL);

  STableKeyInfo* pKeyInfo = taosArrayGet(pTableList->pTableList, *slot);
  ASSERT(pKeyInfo->uid == tableUid);

  return pKeyInfo->groupId;
}

// TODO handle the group offset info, fix it, the rule of group output will be broken by this function
int32_t tableListAddTableInfo(STableListInfo* pTableList, uint64_t uid, uint64_t gid) {
  if (pTableList->map == NULL) {
    ASSERT(taosArrayGetSize(pTableList->pTableList) == 0);
    pTableList->map = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  }

  STableKeyInfo keyInfo = {.uid = uid, .groupId = gid};
  taosArrayPush(pTableList->pTableList, &keyInfo);

  int32_t slot = (int32_t)taosArrayGetSize(pTableList->pTableList) - 1;
  taosHashPut(pTableList->map, &uid, sizeof(uid), &slot, sizeof(slot));

  qDebug("uid:%" PRIu64 ", groupId:%" PRIu64 " added into table list, slot:%d, total:%d", uid, gid, slot, slot + 1);
  return TSDB_CODE_SUCCESS;
}

int32_t tableListGetGroupList(const STableListInfo* pTableList, int32_t ordinalGroupIndex, STableKeyInfo** pKeyInfo,
                              int32_t* size) {
  int32_t totalGroups = tableListGetOutputGroups(pTableList);
  int32_t numOfTables =  tableListGetSize(pTableList);

  if (ordinalGroupIndex < 0 || ordinalGroupIndex >= totalGroups) {
    return TSDB_CODE_INVALID_PARA;
  }

  // here handle two special cases:
  // 1. only one group exists, and 2. one table exists for each group.
  if (totalGroups == 1) {
    *size = numOfTables;
    *pKeyInfo = (*size == 0) ? NULL : taosArrayGet(pTableList->pTableList, 0);
    return TSDB_CODE_SUCCESS;
  } else if (totalGroups == numOfTables) {
    *size = 1;
    *pKeyInfo = taosArrayGet(pTableList->pTableList, ordinalGroupIndex);
    return TSDB_CODE_SUCCESS;
  }

  int32_t offset = pTableList->groupOffset[ordinalGroupIndex];
  if (ordinalGroupIndex < totalGroups - 1) {
    *size = pTableList->groupOffset[ordinalGroupIndex + 1] - offset;
  } else {
    *size = numOfTables - offset;
  }

  *pKeyInfo = taosArrayGet(pTableList->pTableList, offset);
  return TSDB_CODE_SUCCESS;
}

int32_t tableListGetOutputGroups(const STableListInfo* pTableList) { return pTableList->numOfOuputGroups; }

bool oneTableForEachGroup(const STableListInfo* pTableList) { return pTableList->oneTableForEachGroup; }

STableListInfo* tableListCreate() {
  STableListInfo* pListInfo = taosMemoryCalloc(1, sizeof(STableListInfo));
  if (pListInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pListInfo->pTableList = taosArrayInit(4, sizeof(STableKeyInfo));
  if (pListInfo->pTableList == NULL) {
    goto _error;
  }

  pListInfo->map = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (pListInfo->map == NULL) {
    goto _error;
  }

  pListInfo->numOfOuputGroups = 1;
  return pListInfo;

_error:
  tableListDestroy(pListInfo);
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

void* tableListDestroy(STableListInfo* pTableListInfo) {
  if (pTableListInfo == NULL) {
    return NULL;
  }

  pTableListInfo->pTableList = taosArrayDestroy(pTableListInfo->pTableList);
  taosMemoryFreeClear(pTableListInfo->groupOffset);

  taosHashCleanup(pTableListInfo->map);

  pTableListInfo->pTableList = NULL;
  pTableListInfo->map = NULL;
  taosMemoryFree(pTableListInfo);
  return NULL;
}

void tableListClear(STableListInfo* pTableListInfo) {
  if (pTableListInfo == NULL) {
    return;
  }

  taosArrayClear(pTableListInfo->pTableList);
  taosHashClear(pTableListInfo->map);
  taosMemoryFree(pTableListInfo->groupOffset);
  pTableListInfo->numOfOuputGroups = 1;
  pTableListInfo->oneTableForEachGroup = false;
}

static int32_t orderbyGroupIdComparFn(const void* p1, const void* p2) {
  STableKeyInfo* pInfo1 = (STableKeyInfo*)p1;
  STableKeyInfo* pInfo2 = (STableKeyInfo*)p2;

  if (pInfo1->groupId == pInfo2->groupId) {
    return 0;
  } else {
    return pInfo1->groupId < pInfo2->groupId ? -1 : 1;
  }
}

static int32_t sortTableGroup(STableListInfo* pTableListInfo) {
  taosArraySort(pTableListInfo->pTableList, orderbyGroupIdComparFn);
  int32_t size = taosArrayGetSize(pTableListInfo->pTableList);

  SArray* pList = taosArrayInit(4, sizeof(int32_t));

  STableKeyInfo* pInfo = taosArrayGet(pTableListInfo->pTableList, 0);
  uint64_t       gid = pInfo->groupId;

  int32_t start = 0;
  taosArrayPush(pList, &start);

  for (int32_t i = 1; i < size; ++i) {
    pInfo = taosArrayGet(pTableListInfo->pTableList, i);
    if (pInfo->groupId != gid) {
      taosArrayPush(pList, &i);
      gid = pInfo->groupId;
    }
  }

  pTableListInfo->numOfOuputGroups = taosArrayGetSize(pList);
  pTableListInfo->groupOffset = taosMemoryMalloc(sizeof(int32_t) * pTableListInfo->numOfOuputGroups);
  if (pTableListInfo->groupOffset == NULL) {
    taosArrayDestroy(pList);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  memcpy(pTableListInfo->groupOffset, taosArrayGet(pList, 0), sizeof(int32_t) * pTableListInfo->numOfOuputGroups);
  taosArrayDestroy(pList);
  return TDB_CODE_SUCCESS;
}

int32_t buildGroupIdMapForAllTables(STableListInfo* pTableListInfo, SReadHandle* pHandle, SNodeList* group,
                                    bool groupSort) {
  int32_t code = TSDB_CODE_SUCCESS;
  ASSERT(pTableListInfo->map != NULL);

  bool   groupByTbname = groupbyTbname(group);
  size_t numOfTables = taosArrayGetSize(pTableListInfo->pTableList);
  if (group == NULL || groupByTbname) {
    for (int32_t i = 0; i < numOfTables; i++) {
      STableKeyInfo* info = taosArrayGet(pTableListInfo->pTableList, i);
      info->groupId = groupByTbname ? info->uid : 0;
    }

    pTableListInfo->oneTableForEachGroup = groupByTbname;

    if (groupSort && groupByTbname) {
      taosArraySort(pTableListInfo->pTableList, orderbyGroupIdComparFn);
      pTableListInfo->numOfOuputGroups = numOfTables;
    } else {
      pTableListInfo->numOfOuputGroups = 1;
    }
  } else {
    code = getColInfoResultForGroupby(pHandle->meta, group, pTableListInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (groupSort) {
      code = sortTableGroup(pTableListInfo);
    }
  }

  // add all table entry in the hash map
  size_t size = taosArrayGetSize(pTableListInfo->pTableList);
  for (int32_t i = 0; i < size; ++i) {
    STableKeyInfo* p = taosArrayGet(pTableListInfo->pTableList, i);
    taosHashPut(pTableListInfo->map, &p->uid, sizeof(uint64_t), &i, sizeof(int32_t));
  }

  return code;
}

int32_t createScanTableListInfo(SScanPhysiNode* pScanNode, SNodeList* pGroupTags, bool groupSort, SReadHandle* pHandle,
                                STableListInfo* pTableListInfo, SNode* pTagCond, SNode* pTagIndexCond,
                                SExecTaskInfo* pTaskInfo) {
  int64_t     st = taosGetTimestampUs();
  const char* idStr = GET_TASKID(pTaskInfo);

  if (pHandle == NULL) {
    qError("invalid handle, in creating operator tree, %s", idStr);
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = getTableList(pHandle->meta, pHandle->vnode, pScanNode, pTagCond, pTagIndexCond, pTableListInfo);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to getTableList, code: %s", tstrerror(code));
    return code;
  }

  int32_t numOfTables = taosArrayGetSize(pTableListInfo->pTableList);
  ASSERT(pTableListInfo->numOfOuputGroups == 1);

  int64_t st1 = taosGetTimestampUs();
  pTaskInfo->cost.extractListTime = (st1 - st) / 1000.0;
  qDebug("extract queried table list completed, %d tables, elapsed time:%.2f ms %s", numOfTables,
         pTaskInfo->cost.extractListTime, idStr);

  if (numOfTables == 0) {
    qDebug("no table qualified for query, %s" PRIx64, idStr);
    return TSDB_CODE_SUCCESS;
  }

  code = buildGroupIdMapForAllTables(pTableListInfo, pHandle, pGroupTags, groupSort);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pTaskInfo->cost.groupIdMapTime = (taosGetTimestampUs() - st1) / 1000.0;
  qDebug("generate group id map completed, elapsed time:%.2f ms %s", pTaskInfo->cost.groupIdMapTime, idStr);

  return TSDB_CODE_SUCCESS;
}

void printDataBlock(SSDataBlock* pBlock, const char* flag) {
  if (!pBlock || pBlock->info.rows == 0) {
    qDebug("===stream===printDataBlock: Block is Null or Empty");
    return;
  }
  char* pBuf = NULL;
  qDebug("%s", dumpBlockData(pBlock, flag, &pBuf));
  taosMemoryFree(pBuf);
}
