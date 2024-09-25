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
#include "query.h"
#include "tdatablock.h"
#include "thash.h"
#include "tmsg.h"
#include "ttime.h"

#include "executil.h"
#include "executorInt.h"
#include "querytask.h"
#include "storageapi.h"
#include "tcompression.h"

typedef struct tagFilterAssist {
  SHashObj* colHash;
  int32_t   index;
  SArray*   cInfoList;
  int32_t   code;
} tagFilterAssist;

typedef struct STransTagExprCtx {
  int32_t      code;
  SMetaReader* pReader;
} STransTagExprCtx;

typedef enum {
  FILTER_NO_LOGIC = 1,
  FILTER_AND,
  FILTER_OTHER,
} FilterCondType;

static FilterCondType checkTagCond(SNode* cond);
static int32_t optimizeTbnameInCond(void* metaHandle, int64_t suid, SArray* list, SNode* pTagCond, SStorageAPI* pAPI);
static int32_t optimizeTbnameInCondImpl(void* metaHandle, SArray* list, SNode* pTagCond, SStorageAPI* pStoreAPI);

static int32_t getTableList(void* pVnode, SScanPhysiNode* pScanNode, SNode* pTagCond, SNode* pTagIndexCond,
                            STableListInfo* pListInfo, uint8_t* digest, const char* idstr, SStorageAPI* pStorageAPI);

static int64_t getLimit(const SNode* pLimit) { return NULL == pLimit ? -1 : ((SLimitNode*)pLimit)->limit; }
static int64_t getOffset(const SNode* pLimit) { return NULL == pLimit ? -1 : ((SLimitNode*)pLimit)->offset; }
static void    releaseColInfoData(void* pCol);

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

// Convert buf read from rocksdb to result row
int32_t getResultRowFromBuf(SExprSupp *pSup, const char* inBuf, size_t inBufSize, char **outBuf, size_t *outBufSize) {
  SqlFunctionCtx *pCtx = pSup->pCtx;
  int32_t        *offset = pSup->rowEntryInfoOffset;
  SResultRow     *pResultRow = (SResultRow*)outBuf;
  size_t          processedSize = 0;
  int32_t         code = TSDB_CODE_SUCCESS;
  if (inBuf == NULL) {
    qError("invalid input buffer, inBuf:%p", inBuf);
    return TSDB_CODE_INVALID_PARA;
  }

  // calculate the size of output buffer
  *outBufSize = getResultRowSize(pCtx, pSup->numOfExprs);
  *outBuf = taosMemoryMalloc(*outBufSize);
  if (*outBuf == NULL) {
    qError("failed to allocate memory for output buffer, size:%zu", *outBufSize);
    return terrno;
  }
  (void)memcpy(pResultRow, inBuf, sizeof(SResultRow));
  inBuf += sizeof(SResultRow);
  processedSize += sizeof(SResultRow);
  for (int32_t i = 0; i < pSup->numOfExprs; ++i) {
    int32_t len = *(int32_t*)inBuf;
    inBuf += sizeof(int32_t);
    processedSize += sizeof(int32_t);
    if (pCtx->fpSet.decode) {
      code = pCtx->fpSet.decode(&pCtx[i], inBuf, getResultEntryInfo(pResultRow, i, offset), pResultRow->version);
      if (code != TSDB_CODE_SUCCESS) {
        qError("failed to decode result row, code:%d", code);
        return code;
      }
    } else {
      (void)memcpy(getResultEntryInfo(pResultRow, i, offset), inBuf, len);
    }
    inBuf += len;
    processedSize += len;
  }
  void *pos = getResultEntryInfo(pResultRow, pSup->numOfExprs - 1, offset) +
              sizeof(SResultRowEntryInfo) +
              pCtx[pSup->numOfExprs - 1].resDataInfo.interBufSize;
  (void)memcpy(pos, inBuf, pSup->numOfExprs * sizeof(bool));
  inBuf += pSup->numOfExprs * sizeof(bool);
  processedSize += pSup->numOfExprs * sizeof(bool);

  if (processedSize < inBufSize) {
    // stream stores extra data after result row
    size_t leftLen = inBufSize - processedSize;
    TAOS_MEMORY_REALLOC(*outBuf, *outBufSize + leftLen);
    if (*outBuf == NULL) {
      qError("failed to reallocate memory for output buffer, size:%zu", *outBufSize + leftLen);
      return terrno;
    }
    (void)memcpy(outBuf + processedSize, inBuf, leftLen);
    inBuf += leftLen;
    processedSize += leftLen;
    *outBufSize += leftLen;
  }
  return TSDB_CODE_SUCCESS;
}

// Convert result row to buf for rocksdb
int32_t putResultRowToBuf(SExprSupp *pSup, const char* inBuf, size_t inBufSize, char **outBuf, size_t *outBufSize) {
  SqlFunctionCtx *pCtx = pSup->pCtx;
  int32_t        *offset = pSup->rowEntryInfoOffset;
  SResultRow     *pResultRow = (SResultRow*)inBuf;
  size_t          rowSize = getResultRowSize(pCtx, pSup->numOfExprs);

  if (inBuf == NULL) {
    qError("invalid input buffer, inBuf:%p", inBuf);
    return TSDB_CODE_INVALID_PARA;
  }
  if (rowSize > inBufSize) {
    qError("invalid input buffer size, rowSize:%zu, inBufSize:%zu", rowSize, inBufSize);
    return TSDB_CODE_INVALID_PARA;
  }

  // calculate the size of output buffer
  *outBufSize = rowSize + sizeof(int32_t) * pSup->numOfExprs;
  if (rowSize < inBufSize) {
    *outBufSize += inBufSize - rowSize;
  }

  *outBuf = taosMemoryMalloc(*outBufSize);
  if (*outBuf == NULL) {
    qError("failed to allocate memory for output buffer, size:%zu", *outBufSize);
    return terrno;
  }

  pResultRow->version = FUNCTION_RESULT_INFO_VERSION;
  (void)memcpy(outBuf, pResultRow, sizeof(SResultRow));
  outBuf += sizeof(SResultRow);
  for (int32_t i = 0; i < pSup->numOfExprs; ++i) {
    *(int32_t *) outBuf = offset[i];
    outBuf += sizeof(int32_t);
    size_t len = sizeof(SResultRowEntryInfo) + pCtx[i].resDataInfo.interBufSize;
    (void)memcpy(outBuf, getResultEntryInfo(pResultRow, i, offset), len);
    outBuf += len;
  }

  // mark if col is null for top/bottom result(saveTupleData)
  void *pos = getResultEntryInfo(pResultRow, pSup->numOfExprs - 1, offset) +
              sizeof(SResultRowEntryInfo) +
              pCtx[pSup->numOfExprs - 1].resDataInfo.interBufSize;

  (void)memcpy(outBuf, pos, pSup->numOfExprs * sizeof(bool));

  if (rowSize < inBufSize) {
    // stream stores extra data after result row
    size_t leftLen = inBufSize - rowSize;
    (void)memcpy(outBuf, inBuf + rowSize, leftLen);
    outBuf += leftLen;
  }
  return TSDB_CODE_SUCCESS;
}

static void freeEx(void* p) { taosMemoryFree(*(void**)p); }

void cleanupGroupResInfo(SGroupResInfo* pGroupResInfo) {
  taosMemoryFreeClear(pGroupResInfo->pBuf);
  if (pGroupResInfo->freeItem) {
    //    taosArrayDestroy(pGroupResInfo->pRows);
    taosArrayDestroyEx(pGroupResInfo->pRows, freeEx);
    pGroupResInfo->freeItem = false;
    pGroupResInfo->pRows = NULL;
  } else {
    taosArrayDestroy(pGroupResInfo->pRows);
    pGroupResInfo->pRows = NULL;
  }
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

int32_t initGroupedResultInfo(SGroupResInfo* pGroupResInfo, SSHashObj* pHashmap, int32_t order) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pGroupResInfo->pRows != NULL) {
    taosArrayDestroy(pGroupResInfo->pRows);
  }
  if (pGroupResInfo->pBuf) {
    taosMemoryFree(pGroupResInfo->pBuf);
    pGroupResInfo->pBuf = NULL;
  }

  // extract the result rows information from the hash map
  int32_t size = tSimpleHashGetSize(pHashmap);

  void* pData = NULL;
  pGroupResInfo->pRows = taosArrayInit(size, POINTER_BYTES);
  QUERY_CHECK_NULL(pGroupResInfo->pRows, code, lino, _end, terrno);

  size_t  keyLen = 0;
  int32_t iter = 0;
  int64_t bufLen = 0, offset = 0;

  // todo move away and record this during create window
  while ((pData = tSimpleHashIterate(pHashmap, pData, &iter)) != NULL) {
    /*void* key = */ (void)tSimpleHashGetKey(pData, &keyLen);
    bufLen += keyLen + sizeof(SResultRowPosition);
  }

  pGroupResInfo->pBuf = taosMemoryMalloc(bufLen);
  QUERY_CHECK_NULL(pGroupResInfo->pBuf, code, lino, _end, terrno);

  iter = 0;
  while ((pData = tSimpleHashIterate(pHashmap, pData, &iter)) != NULL) {
    void* key = tSimpleHashGetKey(pData, &keyLen);

    SResKeyPos* p = (SResKeyPos*)(pGroupResInfo->pBuf + offset);

    p->groupId = *(uint64_t*)key;
    p->pos = *(SResultRowPosition*)pData;
    memcpy(p->key, (char*)key + sizeof(uint64_t), keyLen - sizeof(uint64_t));
    void* tmp = taosArrayPush(pGroupResInfo->pRows, &p);
    QUERY_CHECK_NULL(pGroupResInfo->pBuf, code, lino, _end, terrno);

    offset += keyLen + sizeof(struct SResultRowPosition);
  }

  if (order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC) {
    __compar_fn_t fn = (order == TSDB_ORDER_ASC) ? resultrowComparAsc : resultrowComparDesc;
    size = POINTER_BYTES;
    taosSort(pGroupResInfo->pRows->pData, taosArrayGetSize(pGroupResInfo->pRows), size, fn);
  }

  pGroupResInfo->index = 0;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void initMultiResInfoFromArrayList(SGroupResInfo* pGroupResInfo, SArray* pArrayList) {
  if (pGroupResInfo->pRows != NULL) {
    taosArrayDestroy(pGroupResInfo->pRows);
  }

  pGroupResInfo->freeItem = true;
  pGroupResInfo->pRows = pArrayList;
  pGroupResInfo->index = 0;
}

bool hasRemainResults(SGroupResInfo* pGroupResInfo) {
  if (pGroupResInfo->pRows == NULL) {
    return false;
  }

  return pGroupResInfo->index < taosArrayGetSize(pGroupResInfo->pRows);
}

int32_t getNumOfTotalRes(SGroupResInfo* pGroupResInfo) {
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
    if (!pSortKey) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      taosArrayDestroy(pList);
      pList = NULL;
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      break;
    }
    SBlockOrderInfo   bi = {0};
    bi.order = (pSortKey->order == ORDER_ASC) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
    bi.nullFirst = (pSortKey->nullOrder == NULL_ORDER_FIRST);

    SColumnNode* pColNode = (SColumnNode*)pSortKey->pExpr;
    bi.slotId = pColNode->slotId;
    void* tmp = taosArrayPush(pList, &bi);
    if (!tmp) {
      taosArrayDestroy(pList);
      pList = NULL;
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      break;
    }
  }

  return pList;
}

SSDataBlock* createDataBlockFromDescNode(SDataBlockDescNode* pNode) {
  int32_t      numOfCols = LIST_LENGTH(pNode->pSlots);
  SSDataBlock* pBlock = NULL;
  int32_t      code = createDataBlock(&pBlock);
  if (code) {
    terrno = code;
    return NULL;
  }

  pBlock->info.id.blockId = pNode->dataBlockId;
  pBlock->info.type = STREAM_INVALID;
  pBlock->info.calWin = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
  pBlock->info.watermark = INT64_MIN;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SSlotDescNode*  pDescNode = (SSlotDescNode*)nodesListGetNode(pNode->pSlots, i);
    if (!pDescNode) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      blockDataDestroy(pBlock);
      pBlock = NULL;
      terrno = TSDB_CODE_INVALID_PARA;
      break;
    }
    SColumnInfoData idata =
        createColumnInfoData(pDescNode->dataType.type, pDescNode->dataType.bytes, pDescNode->slotId);
    idata.info.scale = pDescNode->dataType.scale;
    idata.info.precision = pDescNode->dataType.precision;

    code = blockDataAppendColInfo(pBlock, &idata);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      blockDataDestroy(pBlock);
      pBlock = NULL;
      terrno = code;
      break;
    }
  }

  return pBlock;
}

int32_t prepareDataBlockBuf(SSDataBlock* pDataBlock, SColMatchInfo* pMatchInfo) {
  SDataBlockInfo* pBlockInfo = &pDataBlock->info;

  for (int32_t i = 0; i < taosArrayGetSize(pMatchInfo->pList); ++i) {
    SColMatchItem* pItem = taosArrayGet(pMatchInfo->pList, i);
    if (!pItem) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }

    if (pItem->isPk) {
      SColumnInfoData* pInfoData = taosArrayGet(pDataBlock->pDataBlock, pItem->dstSlotId);
      if (!pInfoData) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
        return terrno;
      }
      pBlockInfo->pks[0].type = pInfoData->info.type;
      pBlockInfo->pks[1].type = pInfoData->info.type;

      // allocate enough buffer size, which is pInfoData->info.bytes
      if (IS_VAR_DATA_TYPE(pItem->dataType.type)) {
        pBlockInfo->pks[0].pData = taosMemoryCalloc(1, pInfoData->info.bytes);
        if (pBlockInfo->pks[0].pData == NULL) {
          return terrno;
        }

        pBlockInfo->pks[1].pData = taosMemoryCalloc(1, pInfoData->info.bytes);
        if (pBlockInfo->pks[1].pData == NULL) {
          taosMemoryFreeClear(pBlockInfo->pks[0].pData);
          return terrno;
        }

        pBlockInfo->pks[0].nData = pInfoData->info.bytes;
        pBlockInfo->pks[1].nData = pInfoData->info.bytes;
      }

      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

EDealRes doTranslateTagExpr(SNode** pNode, void* pContext) {
  STransTagExprCtx* pCtx = pContext;
  SMetaReader* mr = pCtx->pReader;
  bool isTagCol = false, isTbname = false;
  if (nodeType(*pNode) == QUERY_NODE_COLUMN) {
    SColumnNode* pCol = (SColumnNode*)*pNode;
    if (pCol->colType == COLUMN_TYPE_TBNAME)
      isTbname = true;
    else
      isTagCol = true;
  } else if (nodeType(*pNode) == QUERY_NODE_FUNCTION) {
    SFunctionNode* pFunc = (SFunctionNode*)*pNode;
    if (pFunc->funcType == FUNCTION_TYPE_TBNAME) isTbname = true;
  }
  if (isTagCol) {
    SColumnNode* pSColumnNode = *(SColumnNode**)pNode;

    SValueNode* res = NULL;
    pCtx->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&res);
    if (NULL == res) {
      return DEAL_RES_ERROR;
    }

    res->translate = true;
    res->node.resType = pSColumnNode->node.resType;

    STagVal tagVal = {0};
    tagVal.cid = pSColumnNode->colId;
    const char* p = mr->pAPI->extractTagVal(mr->me.ctbEntry.pTags, pSColumnNode->node.resType.type, &tagVal);
    if (p == NULL) {
      res->node.resType.type = TSDB_DATA_TYPE_NULL;
    } else if (pSColumnNode->node.resType.type == TSDB_DATA_TYPE_JSON) {
      int32_t len = ((const STag*)p)->len;
      res->datum.p = taosMemoryCalloc(len + 1, 1);
      if (NULL == res->datum.p) {
        return DEAL_RES_ERROR;
      }
      memcpy(res->datum.p, p, len);
    } else if (IS_VAR_DATA_TYPE(pSColumnNode->node.resType.type)) {
      res->datum.p = taosMemoryCalloc(tagVal.nData + VARSTR_HEADER_SIZE + 1, 1);
      if (NULL == res->datum.p) {
        return DEAL_RES_ERROR;
      }
      memcpy(varDataVal(res->datum.p), tagVal.pData, tagVal.nData);
      varDataSetLen(res->datum.p, tagVal.nData);
    } else {
      int32_t code = nodesSetValueNodeValue(res, &(tagVal.i64));
      if (code != TSDB_CODE_SUCCESS) {
        return DEAL_RES_ERROR;
      }
    }
    nodesDestroyNode(*pNode);
    *pNode = (SNode*)res;
  } else if (isTbname) {
    SValueNode* res = NULL;
    pCtx->code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&res);
    if (NULL == res) {
      return DEAL_RES_ERROR;
    }

    res->translate = true;
    res->node.resType = ((SExprNode*)(*pNode))->resType;

    int32_t len = strlen(mr->me.name);
    res->datum.p = taosMemoryCalloc(len + VARSTR_HEADER_SIZE + 1, 1);
    if (NULL == res->datum.p) {
      return DEAL_RES_ERROR;
    }
    memcpy(varDataVal(res->datum.p), mr->me.name, len);
    varDataSetLen(res->datum.p, len);
    nodesDestroyNode(*pNode);
    *pNode = (SNode*)res;
  }

  return DEAL_RES_CONTINUE;
}

int32_t isQualifiedTable(STableKeyInfo* info, SNode* pTagCond, void* metaHandle, bool* pQualified, SStorageAPI* pAPI) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SMetaReader mr = {0};

  pAPI->metaReaderFn.initReader(&mr, metaHandle, META_READER_LOCK, &pAPI->metaFn);
  code = pAPI->metaReaderFn.getEntryGetUidCache(&mr, info->uid);
  if (TSDB_CODE_SUCCESS != code) {
    pAPI->metaReaderFn.clearReader(&mr);
    *pQualified = false;

    return TSDB_CODE_SUCCESS;
  }

  SNode* pTagCondTmp = NULL;
  code = nodesCloneNode(pTagCond, &pTagCondTmp);
  if (TSDB_CODE_SUCCESS != code) {
    *pQualified = false;
    pAPI->metaReaderFn.clearReader(&mr);
    return code;
  }
  STransTagExprCtx ctx = {.code = 0, .pReader = &mr};
  nodesRewriteExprPostOrder(&pTagCondTmp, doTranslateTagExpr, &ctx);
  pAPI->metaReaderFn.clearReader(&mr);
  if (TSDB_CODE_SUCCESS != ctx.code) {
    *pQualified = false;
    terrno = code;
    return code;
  }

  SNode* pNew = NULL;
  code = scalarCalculateConstants(pTagCondTmp, &pNew);
  if (TSDB_CODE_SUCCESS != code) {
    terrno = code;
    nodesDestroyNode(pTagCondTmp);
    *pQualified = false;

    return code;
  }

  SValueNode* pValue = (SValueNode*)pNew;
  *pQualified = pValue->datum.b;

  nodesDestroyNode(pNew);
  return TSDB_CODE_SUCCESS;
}

static EDealRes getColumn(SNode** pNode, void* pContext) {
  tagFilterAssist* pData = (tagFilterAssist*)pContext;
  SColumnNode* pSColumnNode = NULL;
  if (QUERY_NODE_COLUMN == nodeType((*pNode))) {
    pSColumnNode = *(SColumnNode**)pNode;
  } else if (QUERY_NODE_FUNCTION == nodeType((*pNode))) {
    SFunctionNode* pFuncNode = *(SFunctionNode**)(pNode);
    if (pFuncNode->funcType == FUNCTION_TYPE_TBNAME) {
      pData->code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pSColumnNode);
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

  void*            data = taosHashGet(pData->colHash, &pSColumnNode->colId, sizeof(pSColumnNode->colId));
  if (!data) {
    int32_t tempRes =
        taosHashPut(pData->colHash, &pSColumnNode->colId, sizeof(pSColumnNode->colId), pNode, sizeof((*pNode)));
    if (tempRes != TSDB_CODE_SUCCESS && tempRes != TSDB_CODE_DUP_KEY) {
      return DEAL_RES_ERROR;
    }
    pSColumnNode->slotId = pData->index++;
    SColumnInfo cInfo = {.colId = pSColumnNode->colId,
                         .type = pSColumnNode->node.resType.type,
                         .bytes = pSColumnNode->node.resType.bytes,
                         .pk = pSColumnNode->isPk};
#if TAG_FILTER_DEBUG
    qDebug("tagfilter build column info, slotId:%d, colId:%d, type:%d", pSColumnNode->slotId, cInfo.colId, cInfo.type);
#endif
    void* tmp = taosArrayPush(pData->cInfoList, &cInfo);
    if (!tmp) {
      return DEAL_RES_ERROR;
    }
  } else {
    SColumnNode* col = *(SColumnNode**)data;
    pSColumnNode->slotId = col->slotId;
  }

  return DEAL_RES_CONTINUE;
}

static int32_t createResultData(SDataType* pType, int32_t numOfRows, SScalarParam* pParam) {
  SColumnInfoData* pColumnData = taosMemoryCalloc(1, sizeof(SColumnInfoData));
  if (pColumnData == NULL) {
    return terrno;
  }

  pColumnData->info.type = pType->type;
  pColumnData->info.bytes = pType->bytes;
  pColumnData->info.scale = pType->scale;
  pColumnData->info.precision = pType->precision;

  int32_t code = colInfoDataEnsureCapacity(pColumnData, numOfRows, true);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    releaseColInfoData(pColumnData);
    return terrno;
  }

  pParam->columnData = pColumnData;
  pParam->colAlloced = true;
  return TSDB_CODE_SUCCESS;
}

static void releaseColInfoData(void* pCol) {
  if (pCol) {
    SColumnInfoData* col = (SColumnInfoData*)pCol;
    colDataDestroy(col);
    taosMemoryFree(col);
  }
}

void freeItem(void* p) {
  STUidTagInfo* pInfo = p;
  if (pInfo->pTagVal != NULL) {
    taosMemoryFree(pInfo->pTagVal);
  }
}

static int32_t genTagFilterDigest(const SNode* pTagCond, T_MD5_CTX* pContext) {
  if (pTagCond == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  char*   payload = NULL;
  int32_t len = 0;
  int32_t code = nodesNodeToMsg(pTagCond, &payload, &len);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

  tMD5Init(pContext);
  tMD5Update(pContext, (uint8_t*)payload, (uint32_t)len);
  tMD5Final(pContext);

  taosMemoryFree(payload);
  return TSDB_CODE_SUCCESS;
}

static int32_t genTbGroupDigest(const SNode* pGroup, uint8_t* filterDigest, T_MD5_CTX* pContext) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char*   payload = NULL;
  int32_t len = 0;
  code = nodesNodeToMsg(pGroup, &payload, &len);
  QUERY_CHECK_CODE(code, lino, _end);

  if (filterDigest[0]) {
    payload = taosMemoryRealloc(payload, len + tListLen(pContext->digest));
    QUERY_CHECK_NULL(payload, code, lino, _end, terrno);
    memcpy(payload + len, filterDigest + 1, tListLen(pContext->digest));
    len += tListLen(pContext->digest);
  }

  tMD5Init(pContext);
  tMD5Update(pContext, (uint8_t*)payload, (uint32_t)len);
  tMD5Final(pContext);

_end:
  taosMemoryFree(payload);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getColInfoResultForGroupby(void* pVnode, SNodeList* group, STableListInfo* pTableListInfo, uint8_t* digest,
                                   SStorageAPI* pAPI, bool initRemainGroups) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SArray*      pBlockList = NULL;
  SSDataBlock* pResBlock = NULL;
  void*        keyBuf = NULL;
  SArray*      groupData = NULL;
  SArray*      pUidTagList = NULL;
  SArray*      tableList = NULL;

  int32_t rows = taosArrayGetSize(pTableListInfo->pTableList);
  if (rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  tagFilterAssist ctx = {0};
  ctx.colHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT), false, HASH_NO_LOCK);
  if (ctx.colHash == NULL) {
    code = terrno;
    goto end;
  }

  ctx.index = 0;
  ctx.cInfoList = taosArrayInit(4, sizeof(SColumnInfo));
  if (ctx.cInfoList == NULL) {
    code = terrno;
    goto end;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, group) {
    nodesRewriteExprPostOrder(&pNode, getColumn, (void*)&ctx);
    if (TSDB_CODE_SUCCESS != ctx.code) {
      code = ctx.code;
      goto end;
    }
    REPLACE_NODE(pNode);
  }

  T_MD5_CTX context = {0};
  if (tsTagFilterCache) {
    SNodeListNode* listNode = NULL;
    code = nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&listNode);
    if (TSDB_CODE_SUCCESS != code) {
      goto end;
    }
    listNode->pNodeList = group;
    code = genTbGroupDigest((SNode*)listNode, digest, &context);
    QUERY_CHECK_CODE(code, lino, end);

    nodesFree(listNode);

    code = pAPI->metaFn.metaGetCachedTbGroup(pVnode, pTableListInfo->idInfo.suid, context.digest,
                                             tListLen(context.digest), &tableList);
    QUERY_CHECK_CODE(code, lino, end);

    if (tableList) {
      taosArrayDestroy(pTableListInfo->pTableList);
      pTableListInfo->pTableList = tableList;
      qDebug("retrieve tb group list from cache, numOfTables:%d",
             (int32_t)taosArrayGetSize(pTableListInfo->pTableList));
      goto end;
    }
  }

  pUidTagList = taosArrayInit(8, sizeof(STUidTagInfo));
  QUERY_CHECK_NULL(pUidTagList, code, lino, end, terrno);

  for (int32_t i = 0; i < rows; ++i) {
    STableKeyInfo* pkeyInfo = taosArrayGet(pTableListInfo->pTableList, i);
    QUERY_CHECK_NULL(pkeyInfo, code, lino, end, terrno);
    STUidTagInfo   info = {.uid = pkeyInfo->uid};
    void*          tmp = taosArrayPush(pUidTagList, &info);
    QUERY_CHECK_NULL(tmp, code, lino, end, terrno);
  }

  code = pAPI->metaFn.getTableTags(pVnode, pTableListInfo->idInfo.suid, pUidTagList);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }

  int32_t numOfTables = taosArrayGetSize(pUidTagList);
  pResBlock = createTagValBlockForFilter(ctx.cInfoList, numOfTables, pUidTagList, pVnode, pAPI);
  if (pResBlock == NULL) {
    code = terrno;
    goto end;
  }

  //  int64_t st1 = taosGetTimestampUs();
  //  qDebug("generate tag block rows:%d, cost:%ld us", rows, st1-st);

  pBlockList = taosArrayInit(2, POINTER_BYTES);
  QUERY_CHECK_NULL(pBlockList, code, lino, end, terrno);

  void* tmp = taosArrayPush(pBlockList, &pResBlock);
  QUERY_CHECK_NULL(tmp, code, lino, end, terrno);

  groupData = taosArrayInit(2, POINTER_BYTES);
  QUERY_CHECK_NULL(groupData, code, lino, end, terrno);

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
      QUERY_CHECK_NULL(pColInfo, code, lino, end, terrno);
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

    void* tmp = taosArrayPush(groupData, &output.columnData);
    QUERY_CHECK_NULL(tmp, code, lino, end, terrno);
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
    code = terrno;
    goto end;
  }

  if (initRemainGroups) {
    pTableListInfo->remainGroups =
        taosHashInit(rows, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    if (pTableListInfo->remainGroups == NULL) {
      code = terrno;
      goto end;
    }
  }

  for (int i = 0; i < rows; i++) {
    STableKeyInfo* info = taosArrayGet(pTableListInfo->pTableList, i);
    QUERY_CHECK_NULL(info, code, lino, end, terrno);

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
          if (varDataTLen(data) > pValue->info.bytes) {
            code = TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER;
            goto end;
          }
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
    if (initRemainGroups) {
      // groupId ~ table uid
      code = taosHashPut(pTableListInfo->remainGroups, &(info->groupId), sizeof(info->groupId), &(info->uid),
                         sizeof(info->uid));
      if (code == TSDB_CODE_DUP_KEY) {
        code = TSDB_CODE_SUCCESS;
      }
      QUERY_CHECK_CODE(code, lino, end);
    }
  }

  if (tsTagFilterCache) {
    tableList = taosArrayDup(pTableListInfo->pTableList, NULL);
    QUERY_CHECK_NULL(tableList, code, lino, end, terrno);

    code = pAPI->metaFn.metaPutTbGroupToCache(pVnode, pTableListInfo->idInfo.suid, context.digest,
                                              tListLen(context.digest), tableList,
                                              taosArrayGetSize(tableList) * sizeof(STableKeyInfo));
    QUERY_CHECK_CODE(code, lino, end);
  }

  //  int64_t st2 = taosGetTimestampUs();
  //  qDebug("calculate tag block rows:%d, cost:%ld us", rows, st2-st1);

end:
  taosMemoryFreeClear(keyBuf);
  taosHashCleanup(ctx.colHash);
  taosArrayDestroy(ctx.cInfoList);
  blockDataDestroy(pResBlock);
  taosArrayDestroy(pBlockList);
  taosArrayDestroyEx(pUidTagList, freeItem);
  taosArrayDestroyP(groupData, releaseColInfoData);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
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
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  int32_t    len = LIST_LENGTH(pList->pNodeList);
  SListCell* cell = pList->pNodeList->pHead;

  SArray* pTbList = taosArrayInit(len, POINTER_BYTES);
  QUERY_CHECK_NULL(pTbList, code, lino, _end, terrno);

  for (int i = 0; i < pList->pNodeList->length; i++) {
    SValueNode* valueNode = (SValueNode*)cell->pNode;
    if (!IS_VAR_DATA_TYPE(valueNode->node.resType.type)) {
      terrno = TSDB_CODE_INVALID_PARA;
      taosArrayDestroy(pTbList);
      return NULL;
    }

    char* name = varDataVal(valueNode->datum.p);
    void* tmp = taosArrayPush(pTbList, &name);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    cell = cell->pNext;
  }

  size_t numOfTables = taosArrayGetSize(pTbList);

  // order the name
  taosArraySort(pTbList, nameComparFn);

  // remove the duplicates
  SArray* pNewList = taosArrayInit(taosArrayGetSize(pTbList), sizeof(void*));
  QUERY_CHECK_NULL(pNewList, code, lino, _end, terrno);
  void*   tmpTbl = taosArrayGet(pTbList, 0);
  QUERY_CHECK_NULL(tmpTbl, code, lino, _end, terrno);
  void*   tmp = taosArrayPush(pNewList, tmpTbl);
  QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

  for (int32_t i = 1; i < numOfTables; ++i) {
    char** name = taosArrayGetLast(pNewList);
    char** nameInOldList = taosArrayGet(pTbList, i);
    QUERY_CHECK_NULL(nameInOldList, code, lino, _end, terrno);
    if (strcmp(*name, *nameInOldList) == 0) {
      continue;
    }

    tmp = taosArrayPush(pNewList, nameInOldList);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

_end:
  taosArrayDestroy(pTbList);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    return NULL;
  }
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

static int32_t filterTableInfoCompare(const void* a, const void* b) {
  STUidTagInfo* p1 = (STUidTagInfo*)a;
  STUidTagInfo* p2 = (STUidTagInfo*)b;

  if (p1->uid == p2->uid) {
    return 0;
  }

  return p1->uid < p2->uid ? -1 : 1;
}

static FilterCondType checkTagCond(SNode* cond) {
  if (nodeType(cond) == QUERY_NODE_OPERATOR) {
    return FILTER_NO_LOGIC;
  }
  if (nodeType(cond) != QUERY_NODE_LOGIC_CONDITION || ((SLogicConditionNode*)cond)->condType != LOGIC_COND_TYPE_AND) {
    return FILTER_AND;
  }
  return FILTER_OTHER;
}

static int32_t optimizeTbnameInCond(void* pVnode, int64_t suid, SArray* list, SNode* cond, SStorageAPI* pAPI) {
  int32_t ret = -1;
  int32_t ntype = nodeType(cond);

  if (ntype == QUERY_NODE_OPERATOR) {
    ret = optimizeTbnameInCondImpl(pVnode, list, cond, pAPI);
  }

  if (ntype != QUERY_NODE_LOGIC_CONDITION || ((SLogicConditionNode*)cond)->condType != LOGIC_COND_TYPE_AND) {
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
    if (optimizeTbnameInCondImpl(pVnode, list, cell->pNode, pAPI) == 0) {
      hasTbnameCond = true;
      break;
    }
    cell = cell->pNext;
  }

  taosArraySort(list, filterTableInfoCompare);
  taosArrayRemoveDuplicate(list, filterTableInfoCompare, NULL);

  if (hasTbnameCond) {
    ret = pAPI->metaFn.getTableTagsByUid(pVnode, suid, list);
  }

  return ret;
}

// only return uid that does not contained in pExistedUidList
static int32_t optimizeTbnameInCondImpl(void* pVnode, SArray* pExistedUidList, SNode* pTagCond,
                                        SStorageAPI* pStoreAPI) {
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

    size_t numOfExisted = taosArrayGetSize(pExistedUidList);  // len > 0 means there already have uids
    if (numOfExisted > 0) {
      uHash = taosHashInit(numOfExisted / 0.7, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      if (!uHash) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
        return terrno;
      }

      for (int i = 0; i < numOfExisted; i++) {
        STUidTagInfo* pTInfo = taosArrayGet(pExistedUidList, i);
        if (!pTInfo) {
          qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
          return terrno;
        }
        int32_t       tempRes = taosHashPut(uHash, &pTInfo->uid, sizeof(uint64_t), &i, sizeof(i));
        if (tempRes != TSDB_CODE_SUCCESS && tempRes != TSDB_CODE_DUP_KEY) {
          qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(tempRes));
          return tempRes;
        }
      }
    }

    for (int i = 0; i < numOfTables; i++) {
      char* name = taosArrayGetP(pTbList, i);

      uint64_t uid = 0;
      if (pStoreAPI->metaFn.getTableUidByName(pVnode, name, &uid) == 0) {
        ETableType tbType = TSDB_TABLE_MAX;
        if (pStoreAPI->metaFn.getTableTypeByName(pVnode, name, &tbType) == 0 && tbType == TSDB_CHILD_TABLE) {
          if (NULL == uHash || taosHashGet(uHash, &uid, sizeof(uid)) == NULL) {
            STUidTagInfo s = {.uid = uid, .name = name, .pTagVal = NULL};
            void*        tmp = taosArrayPush(pExistedUidList, &s);
            if (!tmp) {
              return terrno;
            }
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

SSDataBlock* createTagValBlockForFilter(SArray* pColList, int32_t numOfTables, SArray* pUidTagList, void* pVnode,
                                        SStorageAPI* pStorageAPI) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pResBlock = NULL;
  code = createDataBlock(&pResBlock);
  QUERY_CHECK_CODE(code, lino, _end);

  for (int32_t i = 0; i < taosArrayGetSize(pColList); ++i) {
    SColumnInfoData colInfo = {0};
    void* tmp = taosArrayGet(pColList, i);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    colInfo.info = *(SColumnInfo*)tmp;
    code = blockDataAppendColInfo(pResBlock, &colInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  code = blockDataEnsureCapacity(pResBlock, numOfTables);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    blockDataDestroy(pResBlock);
    return NULL;
  }

  pResBlock->info.rows = numOfTables;

  int32_t numOfCols = taosArrayGetSize(pResBlock->pDataBlock);

  for (int32_t i = 0; i < numOfTables; i++) {
    STUidTagInfo* p1 = taosArrayGet(pUidTagList, i);
    QUERY_CHECK_NULL(p1, code, lino, _end, terrno);

    for (int32_t j = 0; j < numOfCols; j++) {
      SColumnInfoData* pColInfo = (SColumnInfoData*)taosArrayGet(pResBlock->pDataBlock, j);
      QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);

      if (pColInfo->info.colId == -1) {  // tbname
        char str[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
        if (p1->name != NULL) {
          STR_TO_VARSTR(str, p1->name);
        } else {  // name is not retrieved during filter
          code = pStorageAPI->metaFn.getTableNameByUid(pVnode, p1->uid, str);
          QUERY_CHECK_CODE(code, lino, _end);
        }

        code = colDataSetVal(pColInfo, i, str, false);
        QUERY_CHECK_CODE(code, lino, _end);
#if TAG_FILTER_DEBUG
        qDebug("tagfilter uid:%ld, tbname:%s", *uid, str + 2);
#endif
      } else {
        STagVal tagVal = {0};
        tagVal.cid = pColInfo->info.colId;
        if (p1->pTagVal == NULL) {
          colDataSetNULL(pColInfo, i);
        } else {
          const char* p = pStorageAPI->metaFn.extractTagVal(p1->pTagVal, pColInfo->info.type, &tagVal);

          if (p == NULL || (pColInfo->info.type == TSDB_DATA_TYPE_JSON && ((STag*)p)->nTag == 0)) {
            colDataSetNULL(pColInfo, i);
          } else if (pColInfo->info.type == TSDB_DATA_TYPE_JSON) {
            code = colDataSetVal(pColInfo, i, p, false);
            QUERY_CHECK_CODE(code, lino, _end);
          } else if (IS_VAR_DATA_TYPE(pColInfo->info.type)) {
            char* tmp = taosMemoryMalloc(tagVal.nData + VARSTR_HEADER_SIZE + 1);
            QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
            varDataSetLen(tmp, tagVal.nData);
            memcpy(tmp + VARSTR_HEADER_SIZE, tagVal.pData, tagVal.nData);
            code = colDataSetVal(pColInfo, i, tmp, false);
#if TAG_FILTER_DEBUG
            qDebug("tagfilter varch:%s", tmp + 2);
#endif
            taosMemoryFree(tmp);
            QUERY_CHECK_CODE(code, lino, _end);
          } else {
            code = colDataSetVal(pColInfo, i, (const char*)&tagVal.i64, false);
            QUERY_CHECK_CODE(code, lino, _end);
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
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pResBlock);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    terrno = code;
    return NULL;
  }
  return pResBlock;
}

static int32_t doSetQualifiedUid(STableListInfo* pListInfo, SArray* pUidList, const SArray* pUidTagList,
                                 bool* pResultList, bool addUid) {
  taosArrayClear(pUidList);

  STableKeyInfo info = {.uid = 0, .groupId = 0};
  int32_t       numOfTables = taosArrayGetSize(pUidTagList);
  for (int32_t i = 0; i < numOfTables; ++i) {
    if (pResultList[i]) {
      STUidTagInfo* tmpTag = (STUidTagInfo*)taosArrayGet(pUidTagList, i);
      if (!tmpTag) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
        return terrno;
      }
      uint64_t uid = tmpTag->uid;
      qDebug("tagfilter get uid:%" PRId64 ", res:%d", uid, pResultList[i]);

      info.uid = uid;
      void* p = taosArrayPush(pListInfo->pTableList, &info);
      if (p == NULL) {
        return terrno;
      }

      if (addUid) {
        void* tmp = taosArrayPush(pUidList, &uid);
        if (tmp == NULL) {
          return terrno;
        }
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t copyExistedUids(SArray* pUidTagList, const SArray* pUidList) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t numOfExisted = taosArrayGetSize(pUidList);
  if (numOfExisted == 0) {
    return code;
  }

  for (int32_t i = 0; i < numOfExisted; ++i) {
    uint64_t*    uid = taosArrayGet(pUidList, i);
    if (!uid) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    STUidTagInfo info = {.uid = *uid};
    void*        tmp = taosArrayPush(pUidTagList, &info);
    if (!tmp) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      return code;
    }
  }
  return code;
}

static int32_t doFilterByTagCond(STableListInfo* pListInfo, SArray* pUidList, SNode* pTagCond, void* pVnode,
                                 SIdxFltStatus status, SStorageAPI* pAPI, bool addUid, bool* listAdded) {
  *listAdded = false;
  if (pTagCond == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  terrno = TSDB_CODE_SUCCESS;

  int32_t      lino = 0;
  int32_t      code = TSDB_CODE_SUCCESS;
  SArray*      pBlockList = NULL;
  SSDataBlock* pResBlock = NULL;
  SScalarParam output = {0};
  SArray*      pUidTagList = NULL;

  tagFilterAssist ctx = {0};
  ctx.colHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT), false, HASH_NO_LOCK);
  if (ctx.colHash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, end);
  }

  ctx.cInfoList = taosArrayInit(4, sizeof(SColumnInfo));
  if (ctx.cInfoList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = TSDB_CODE_OUT_OF_MEMORY;
    QUERY_CHECK_CODE(code, lino, end);
  }

  nodesRewriteExprPostOrder(&pTagCond, getColumn, (void*)&ctx);
  if (TSDB_CODE_SUCCESS != ctx.code) {
    terrno = code = ctx.code;
    goto end;
  }

  SDataType type = {.type = TSDB_DATA_TYPE_BOOL, .bytes = sizeof(bool)};

  //  int64_t stt = taosGetTimestampUs();
  pUidTagList = taosArrayInit(10, sizeof(STUidTagInfo));
  QUERY_CHECK_NULL(pUidTagList, code, lino, end, terrno);

  code = copyExistedUids(pUidTagList, pUidList);
  QUERY_CHECK_CODE(code, lino, end);

  FilterCondType condType = checkTagCond(pTagCond);

  int32_t filter = optimizeTbnameInCond(pVnode, pListInfo->idInfo.suid, pUidTagList, pTagCond, pAPI);
  if (filter == 0) {  // tbname in filter is activated, do nothing and return
    taosArrayClear(pUidList);

    int32_t numOfRows = taosArrayGetSize(pUidTagList);
    code = taosArrayEnsureCap(pUidList, numOfRows);
    QUERY_CHECK_CODE(code, lino, end);

    for (int32_t i = 0; i < numOfRows; ++i) {
      STUidTagInfo* pInfo = taosArrayGet(pUidTagList, i);
      QUERY_CHECK_NULL(pInfo, code, lino, end, terrno);
      void*         tmp = taosArrayPush(pUidList, &pInfo->uid);
      QUERY_CHECK_NULL(tmp, code, lino, end, terrno);
    }
    terrno = 0;
  } else {
    if ((condType == FILTER_NO_LOGIC || condType == FILTER_AND) && status != SFLT_NOT_INDEX) {
      code = pAPI->metaFn.getTableTagsByUid(pVnode, pListInfo->idInfo.suid, pUidTagList);
    } else {
      code = pAPI->metaFn.getTableTags(pVnode, pListInfo->idInfo.suid, pUidTagList);
    }
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get table tags from meta, reason:%s, suid:%" PRIu64, tstrerror(code), pListInfo->idInfo.suid);
      terrno = code;
      QUERY_CHECK_CODE(code, lino, end);
    }
  }

  int32_t numOfTables = taosArrayGetSize(pUidTagList);
  if (numOfTables == 0) {
    goto end;
  }

  pResBlock = createTagValBlockForFilter(ctx.cInfoList, numOfTables, pUidTagList, pVnode, pAPI);
  if (pResBlock == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, end);
  }

  //  int64_t st1 = taosGetTimestampUs();
  //  qDebug("generate tag block rows:%d, cost:%ld us", rows, st1-st);
  pBlockList = taosArrayInit(2, POINTER_BYTES);
  QUERY_CHECK_NULL(pBlockList, code, lino, end, terrno);

  void* tmp = taosArrayPush(pBlockList, &pResBlock);
  QUERY_CHECK_NULL(tmp, code, lino, end, terrno);

  code = createResultData(&type, numOfTables, &output);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    QUERY_CHECK_CODE(code, lino, end);
  }

  code = scalarCalculate(pTagCond, pBlockList, &output);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to calculate scalar, reason:%s", tstrerror(code));
    terrno = code;
    QUERY_CHECK_CODE(code, lino, end);
  }

  code = doSetQualifiedUid(pListInfo, pUidList, pUidTagList, (bool*)output.columnData->pData, addUid);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    QUERY_CHECK_CODE(code, lino, end);
  }
  *listAdded = true;

end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  taosHashCleanup(ctx.colHash);
  taosArrayDestroy(ctx.cInfoList);
  blockDataDestroy(pResBlock);
  taosArrayDestroy(pBlockList);
  taosArrayDestroyEx(pUidTagList, freeItem);

  colDataDestroy(output.columnData);
  taosMemoryFreeClear(output.columnData);
  return code;
}

int32_t getTableList(void* pVnode, SScanPhysiNode* pScanNode, SNode* pTagCond, SNode* pTagIndexCond,
                     STableListInfo* pListInfo, uint8_t* digest, const char* idstr, SStorageAPI* pStorageAPI) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  numOfTables = 0;
  bool    listAdded = false;

  pListInfo->idInfo.suid = pScanNode->suid;
  pListInfo->idInfo.tableType = pScanNode->tableType;

  SArray* pUidList = taosArrayInit(8, sizeof(uint64_t));
  QUERY_CHECK_NULL(pUidList, code, lino, _error, terrno);

  SIdxFltStatus status = SFLT_NOT_INDEX;
  if (pScanNode->tableType != TSDB_SUPER_TABLE) {
    pListInfo->idInfo.uid = pScanNode->uid;
    if (pStorageAPI->metaFn.isTableExisted(pVnode, pScanNode->uid)) {
      void* tmp = taosArrayPush(pUidList, &pScanNode->uid);
      QUERY_CHECK_NULL(tmp, code, lino, _error, terrno);
    }
    code = doFilterByTagCond(pListInfo, pUidList, pTagCond, pVnode, status, pStorageAPI, false, &listAdded);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    T_MD5_CTX context = {0};

    if (tsTagFilterCache) {
      // try to retrieve the result from meta cache
      code = genTagFilterDigest(pTagCond, &context);
      QUERY_CHECK_CODE(code, lino, _error);

      bool acquired = false;
      code = pStorageAPI->metaFn.getCachedTableList(pVnode, pScanNode->suid, context.digest, tListLen(context.digest),
                                                    pUidList, &acquired);
      QUERY_CHECK_CODE(code, lino, _error);

      if (acquired) {
        digest[0] = 1;
        memcpy(digest + 1, context.digest, tListLen(context.digest));
        qDebug("retrieve table uid list from cache, numOfTables:%d", (int32_t)taosArrayGetSize(pUidList));
        goto _end;
      }
    }

    if (!pTagCond) {  // no tag filter condition exists, let's fetch all tables of this super table
      code = pStorageAPI->metaFn.getChildTableList(pVnode, pScanNode->suid, pUidList);
      QUERY_CHECK_CODE(code, lino, _error);
    } else {
      // failed to find the result in the cache, let try to calculate the results
      if (pTagIndexCond) {
        void* pIndex = pStorageAPI->metaFn.getInvertIndex(pVnode);

        SIndexMetaArg metaArg = {.metaEx = pVnode,
                                 .idx = pStorageAPI->metaFn.storeGetIndexInfo(pVnode),
                                 .ivtIdx = pIndex,
                                 .suid = pScanNode->uid};

        status = SFLT_NOT_INDEX;
        code = doFilterTag(pTagIndexCond, &metaArg, pUidList, &status, &pStorageAPI->metaFilter);
        if (code != 0 || status == SFLT_NOT_INDEX) {  // temporarily disable it for performance sake
          qDebug("failed to get tableIds from index, suid:%" PRIu64, pScanNode->uid);
        } else {
          qDebug("succ to get filter result, table num: %d", (int)taosArrayGetSize(pUidList));
        }
      }
    }

    code = doFilterByTagCond(pListInfo, pUidList, pTagCond, pVnode, status, pStorageAPI, tsTagFilterCache, &listAdded);
    QUERY_CHECK_CODE(code, lino, _end);

    // let's add the filter results into meta-cache
    numOfTables = taosArrayGetSize(pUidList);

    if (tsTagFilterCache) {
      size_t size = numOfTables * sizeof(uint64_t) + sizeof(int32_t);
      char*  pPayload = taosMemoryMalloc(size);
      QUERY_CHECK_NULL(pPayload, code, lino, _end, terrno);

      *(int32_t*)pPayload = numOfTables;
      if (numOfTables > 0) {
        void* tmp = taosArrayGet(pUidList, 0);
        QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
        memcpy(pPayload + sizeof(int32_t), tmp, numOfTables * sizeof(uint64_t));
      }

      code = pStorageAPI->metaFn.putCachedTableList(pVnode, pScanNode->suid, context.digest, tListLen(context.digest),
                                                    pPayload, size, 1);
      QUERY_CHECK_CODE(code, lino, _error);

      digest[0] = 1;
      memcpy(digest + 1, context.digest, tListLen(context.digest));
    }
  }

_end:
  if (!listAdded) {
    numOfTables = taosArrayGetSize(pUidList);
    for (int i = 0; i < numOfTables; i++) {
      void* tmp = taosArrayGet(pUidList, i);
      QUERY_CHECK_NULL(tmp, code, lino, _error, terrno);
      STableKeyInfo info = {.uid = *(uint64_t*)tmp, .groupId = 0};

      void* p = taosArrayPush(pListInfo->pTableList, &info);
      if (p == NULL) {
        taosArrayDestroy(pUidList);
        return terrno;
      }

      qTrace("tagfilter get uid:%" PRIu64 ", %s", info.uid, idstr);
    }
  }

_error:
  taosArrayDestroy(pUidList);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t qGetTableList(int64_t suid, void* pVnode, void* node, SArray** tableList, void* pTaskInfo) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SSubplan*      pSubplan = (SSubplan*)node;
  SScanPhysiNode pNode = {0};
  pNode.suid = suid;
  pNode.uid = suid;
  pNode.tableType = TSDB_SUPER_TABLE;

  STableListInfo* pTableListInfo = tableListCreate();
  QUERY_CHECK_NULL(pTableListInfo, code, lino, _end, terrno);
  uint8_t         digest[17] = {0};
  code =
      getTableList(pVnode, &pNode, pSubplan ? pSubplan->pTagCond : NULL, pSubplan ? pSubplan->pTagIndexCond : NULL,
                   pTableListInfo, digest, "qGetTableList", &((SExecTaskInfo*)pTaskInfo)->storageAPI);
  QUERY_CHECK_CODE(code, lino, _end);
  *tableList = pTableListInfo->pTableList;
  pTableListInfo->pTableList = NULL;
  tableListDestroy(pTableListInfo);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
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

int32_t getGroupIdFromTagsVal(void* pVnode, uint64_t uid, SNodeList* pGroupNode, char* keyBuf, uint64_t* pGroupId,
                              SStorageAPI* pAPI) {
  SMetaReader mr = {0};

  pAPI->metaReaderFn.initReader(&mr, pVnode, META_READER_LOCK, &pAPI->metaFn);
  if (pAPI->metaReaderFn.getEntryGetUidCache(&mr, uid) != 0) {  // table not exist
    pAPI->metaReaderFn.clearReader(&mr);
    return TSDB_CODE_PAR_TABLE_NOT_EXIST;
  }

  SNodeList* groupNew = NULL;
  int32_t code = nodesCloneList(pGroupNode, &groupNew);
  if (TSDB_CODE_SUCCESS != code) {
    pAPI->metaReaderFn.clearReader(&mr);
    return code;
  }

  STransTagExprCtx ctx = {.code = 0, .pReader = &mr};
  nodesRewriteExprsPostOrder(groupNew, doTranslateTagExpr, &ctx);
  if (TSDB_CODE_SUCCESS != ctx.code) {
    nodesDestroyList(groupNew);
    pAPI->metaReaderFn.clearReader(&mr);
    return code;
  }
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
      nodesDestroyList(groupNew);
      pAPI->metaReaderFn.clearReader(&mr);
      return code;
    }

    if (nodeType(pNew) != QUERY_NODE_VALUE) {
      nodesDestroyList(groupNew);
      pAPI->metaReaderFn.clearReader(&mr);
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR));
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
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
          nodesDestroyList(groupNew);
          pAPI->metaReaderFn.clearReader(&mr);
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
  pAPI->metaReaderFn.clearReader(&mr);
  return TSDB_CODE_SUCCESS;
}

SArray* makeColumnArrayFromList(SNodeList* pNodeList) {
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
    if (!pColNode) {
      taosArrayDestroy(pList);
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      return NULL;
    }

    // todo extract method
    SColumn c = {0};
    c.slotId = pColNode->slotId;
    c.colId = pColNode->colId;
    c.type = pColNode->node.resType.type;
    c.bytes = pColNode->node.resType.bytes;
    c.precision = pColNode->node.resType.precision;
    c.scale = pColNode->node.resType.scale;

    void* tmp = taosArrayPush(pList, &c);
    if (!tmp) {
      taosArrayDestroy(pList);
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      return NULL;
    }
  }

  return pList;
}

int32_t extractColMatchInfo(SNodeList* pNodeList, SDataBlockDescNode* pOutputNodeList, int32_t* numOfOutputCols,
                            int32_t type, SColMatchInfo* pMatchInfo) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pMatchInfo->matchType = type;

  SArray* pList = taosArrayInit(numOfCols, sizeof(SColMatchItem));
  if (pList == NULL) {
    code = terrno;
    return code;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pNodeList, i);
    QUERY_CHECK_NULL(pNode, code, lino, _end, terrno);
    if (nodeType(pNode->pExpr) == QUERY_NODE_COLUMN) {
      SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

      SColMatchItem c = {.needOutput = true};
      c.colId = pColNode->colId;
      c.srcSlotId = pColNode->slotId;
      c.dstSlotId = pNode->slotId;
      c.isPk = pColNode->isPk;
      c.dataType = pColNode->node.resType;
      void* tmp = taosArrayPush(pList, &c);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    }
  }

  // set the output flag for each column in SColMatchInfo, according to the
  *numOfOutputCols = 0;
  int32_t num = LIST_LENGTH(pOutputNodeList->pSlots);
  for (int32_t i = 0; i < num; ++i) {
    SSlotDescNode* pNode = (SSlotDescNode*)nodesListGetNode(pOutputNodeList->pSlots, i);
    QUERY_CHECK_NULL(pNode, code, lino, _end, terrno);

    // todo: add reserve flag check
    // it is a column reserved for the arithmetic expression calculation
    if (pNode->slotId >= numOfCols) {
      (*numOfOutputCols) += 1;
      continue;
    }

    SColMatchItem* info = NULL;
    for (int32_t j = 0; j < taosArrayGetSize(pList); ++j) {
      info = taosArrayGet(pList, j);
      QUERY_CHECK_NULL(info, code, lino, _end, terrno);
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
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

int32_t createExprFromOneNode(SExprInfo* pExp, SNode* pNode, int16_t slotId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  pExp->base.numOfParams = 0;
  pExp->base.pParam = NULL;
  pExp->pExpr = taosMemoryCalloc(1, sizeof(tExprNode));
  QUERY_CHECK_NULL(pExp->pExpr, code, lino, _end, terrno);

  pExp->pExpr->_function.num = 1;
  pExp->pExpr->_function.functionId = -1;

  int32_t type = nodeType(pNode);
  // it is a project query, or group by column
  if (type == QUERY_NODE_COLUMN) {
    pExp->pExpr->nodeType = QUERY_NODE_COLUMN;
    SColumnNode* pColNode = (SColumnNode*)pNode;

    pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
    QUERY_CHECK_NULL(pExp->base.pParam, code, lino, _end, terrno);

    pExp->base.numOfParams = 1;

    SDataType* pType = &pColNode->node.resType;
    pExp->base.resSchema =
        createResSchema(pType->type, pType->bytes, slotId, pType->scale, pType->precision, pColNode->colName);

    pExp->base.pParam[0].pCol =
        createColumn(pColNode->dataBlockId, pColNode->slotId, pColNode->colId, pType, pColNode->colType);
    QUERY_CHECK_NULL(pExp->base.pParam[0].pCol, code, lino, _end, terrno);

    pExp->base.pParam[0].type = FUNC_PARAM_TYPE_COLUMN;
  } else if (type == QUERY_NODE_VALUE) {
    pExp->pExpr->nodeType = QUERY_NODE_VALUE;
    SValueNode* pValNode = (SValueNode*)pNode;

    pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
    QUERY_CHECK_NULL(pExp->base.pParam, code, lino, _end, terrno);

    pExp->base.numOfParams = 1;

    SDataType* pType = &pValNode->node.resType;
    pExp->base.resSchema =
        createResSchema(pType->type, pType->bytes, slotId, pType->scale, pType->precision, pValNode->node.aliasName);
    pExp->base.pParam[0].type = FUNC_PARAM_TYPE_VALUE;
    code = nodesValueNodeToVariant(pValNode, &pExp->base.pParam[0].param);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (type == QUERY_NODE_FUNCTION) {
    pExp->pExpr->nodeType = QUERY_NODE_FUNCTION;
    SFunctionNode* pFuncNode = (SFunctionNode*)pNode;

    SDataType*  pType = &pFuncNode->node.resType;
    const char* pName = pFuncNode->node.aliasName;
    if (pFuncNode->funcType == FUNCTION_TYPE_FORECAST_LOW || pFuncNode->funcType == FUNCTION_TYPE_FORECAST_HIGH ||
        pFuncNode->funcType == FUNCTION_TYPE_FORECAST_ROWTS) {
      pName = pFuncNode->functionName;
    }
    pExp->base.resSchema = createResSchema(pType->type, pType->bytes, slotId, pType->scale, pType->precision, pName);

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
      pFuncNode->pParameterList = NULL;
      int32_t code = nodesMakeList(&pFuncNode->pParameterList);
      SValueNode* res = NULL;
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&res);
      }
      QUERY_CHECK_CODE(code, lino, _end);
      res->node.resType = (SDataType){.bytes = sizeof(int64_t), .type = TSDB_DATA_TYPE_BIGINT};
      code = nodesListAppend(pFuncNode->pParameterList, (SNode*)res);
      if (code != TSDB_CODE_SUCCESS) {
        nodesDestroyNode((SNode*)res);
        res = NULL;
      }
      QUERY_CHECK_CODE(code, lino, _end);
    }
#endif

    int32_t numOfParam = LIST_LENGTH(pFuncNode->pParameterList);

    pExp->base.pParam = taosMemoryCalloc(numOfParam, sizeof(SFunctParam));
    QUERY_CHECK_NULL(pExp->base.pParam, code, lino, _end, terrno);
    pExp->base.numOfParams = numOfParam;

    for (int32_t j = 0; j < numOfParam && TSDB_CODE_SUCCESS == code; ++j) {
      SNode* p1 = nodesListGetNode(pFuncNode->pParameterList, j);
      QUERY_CHECK_NULL(p1, code, lino, _end, terrno);
      if (p1->type == QUERY_NODE_COLUMN) {
        SColumnNode* pcn = (SColumnNode*)p1;

        pExp->base.pParam[j].type = FUNC_PARAM_TYPE_COLUMN;
        pExp->base.pParam[j].pCol =
            createColumn(pcn->dataBlockId, pcn->slotId, pcn->colId, &pcn->node.resType, pcn->colType);
        QUERY_CHECK_NULL(pExp->base.pParam[j].pCol, code, lino, _end, terrno);
      } else if (p1->type == QUERY_NODE_VALUE) {
        SValueNode* pvn = (SValueNode*)p1;
        pExp->base.pParam[j].type = FUNC_PARAM_TYPE_VALUE;
        code = nodesValueNodeToVariant(pvn, &pExp->base.pParam[j].param);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  } else if (type == QUERY_NODE_OPERATOR) {
    pExp->pExpr->nodeType = QUERY_NODE_OPERATOR;
    SOperatorNode* pOpNode = (SOperatorNode*)pNode;

    pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
    QUERY_CHECK_NULL(pExp->base.pParam, code, lino, _end, terrno);
    pExp->base.numOfParams = 1;

    SDataType* pType = &pOpNode->node.resType;
    pExp->base.resSchema =
        createResSchema(pType->type, pType->bytes, slotId, pType->scale, pType->precision, pOpNode->node.aliasName);
    pExp->pExpr->_optrRoot.pRootNode = pNode;
  } else if (type == QUERY_NODE_CASE_WHEN) {
    pExp->pExpr->nodeType = QUERY_NODE_OPERATOR;
    SCaseWhenNode* pCaseNode = (SCaseWhenNode*)pNode;

    pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
    QUERY_CHECK_NULL(pExp->base.pParam, code, lino, _end, terrno);
    pExp->base.numOfParams = 1;

    SDataType* pType = &pCaseNode->node.resType;
    pExp->base.resSchema =
        createResSchema(pType->type, pType->bytes, slotId, pType->scale, pType->precision, pCaseNode->node.aliasName);
    pExp->pExpr->_optrRoot.pRootNode = pNode;
  } else if (type == QUERY_NODE_LOGIC_CONDITION) {
    pExp->pExpr->nodeType = QUERY_NODE_OPERATOR;
    SLogicConditionNode* pCond = (SLogicConditionNode*)pNode;
    pExp->base.pParam = taosMemoryCalloc(1, sizeof(SFunctParam));
    QUERY_CHECK_NULL(pExp->base.pParam, code, lino, _end, terrno);
    pExp->base.numOfParams = 1;
    SDataType* pType = &pCond->node.resType;
    pExp->base.resSchema = createResSchema(pType->type, pType->bytes, slotId, pType->scale, pType->precision, pCond->node.aliasName);
    pExp->pExpr->_optrRoot.pRootNode = pNode;
  } else {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t createExprFromTargetNode(SExprInfo* pExp, STargetNode* pTargetNode) {
  return createExprFromOneNode(pExp, pTargetNode->pExpr, pTargetNode->slotId);
}

SExprInfo* createExpr(SNodeList* pNodeList, int32_t* numOfExprs) {
  *numOfExprs = LIST_LENGTH(pNodeList);
  SExprInfo* pExprs = taosMemoryCalloc(*numOfExprs, sizeof(SExprInfo));
  if (!pExprs) {
    return NULL;
  }

  for (int32_t i = 0; i < (*numOfExprs); ++i) {
    SExprInfo* pExp = &pExprs[i];
    int32_t    code = createExprFromOneNode(pExp, nodesListGetNode(pNodeList, i), i + UD_TAG_COLUMN_INDEX);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFreeClear(pExprs);
      terrno = code;
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      return NULL;
    }
  }

  return pExprs;
}

int32_t createExprInfo(SNodeList* pNodeList, SNodeList* pGroupKeys, SExprInfo** pExprInfo, int32_t* numOfExprs) {
  QRY_PARAM_CHECK(pExprInfo);

  int32_t code = 0;
  int32_t numOfFuncs = LIST_LENGTH(pNodeList);
  int32_t numOfGroupKeys = 0;
  if (pGroupKeys != NULL) {
    numOfGroupKeys = LIST_LENGTH(pGroupKeys);
  }

  *numOfExprs = numOfFuncs + numOfGroupKeys;
  if (*numOfExprs == 0) {
    return code;
  }

  SExprInfo* pExprs = taosMemoryCalloc(*numOfExprs, sizeof(SExprInfo));
  if (pExprs == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < (*numOfExprs); ++i) {
    STargetNode* pTargetNode = NULL;
    if (i < numOfFuncs) {
      pTargetNode = (STargetNode*)nodesListGetNode(pNodeList, i);
    } else {
      pTargetNode = (STargetNode*)nodesListGetNode(pGroupKeys, i - numOfFuncs);
    }
    if (!pTargetNode) {
      destroyExprInfo(pExprs, *numOfExprs);
      taosMemoryFreeClear(pExprs);
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }

    SExprInfo* pExp = &pExprs[i];
    code = createExprFromTargetNode(pExp, pTargetNode);
    if (code != TSDB_CODE_SUCCESS) {
      destroyExprInfo(pExprs, *numOfExprs);
      taosMemoryFreeClear(pExprs);
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      return code;
    }
  }

  *pExprInfo = pExprs;
  return code;
}

// set the output buffer for the selectivity + tag query
static int32_t setSelectValueColumnInfo(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  int32_t num = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SqlFunctionCtx*  p = NULL;
  SqlFunctionCtx** pValCtx = taosMemoryCalloc(numOfOutput, POINTER_BYTES);
  if (pValCtx == NULL) {
    return terrno;
  }

  SHashObj* pSelectFuncs = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  QUERY_CHECK_NULL(pSelectFuncs, code, lino, _end, terrno);

  for (int32_t i = 0; i < numOfOutput; ++i) {
    const char* pName = pCtx[i].pExpr->pExpr->_function.functionName;
    if ((strcmp(pName, "_select_value") == 0) || (strcmp(pName, "_group_key") == 0) ||
        (strcmp(pName, "_group_const_value") == 0)) {
      pValCtx[num++] = &pCtx[i];
    } else if (fmIsSelectFunc(pCtx[i].functionId)) {
      void* data = taosHashGet(pSelectFuncs, pName, strlen(pName));
      if (taosHashGetSize(pSelectFuncs) != 0 && data == NULL) {
        p = NULL;
        break;
      } else {
        int32_t tempRes = taosHashPut(pSelectFuncs, pName, strlen(pName), &num, sizeof(num));
        if (tempRes != TSDB_CODE_SUCCESS && tempRes != TSDB_CODE_DUP_KEY) {
          code = tempRes;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        p = &pCtx[i];
      }
    }
  }
  taosHashCleanup(pSelectFuncs);

  if (p != NULL) {
    p->subsidiaries.pCtx = pValCtx;
    p->subsidiaries.num = num;
  } else {
    taosMemoryFreeClear(pValCtx);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFreeClear(pValCtx);
    taosHashCleanup(pSelectFuncs);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

SqlFunctionCtx* createSqlFunctionCtx(SExprInfo* pExprInfo, int32_t numOfOutput, int32_t** rowEntryInfoOffset,
                                     SFunctionStateStore* pStore) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
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
      pCtx->isPseudoFunc = fmIsWindowPseudoColumnFunc(pCtx->functionId);
      pCtx->isNotNullFunc = fmIsNotNullOutputFunc(pCtx->functionId);

      bool isUdaf = fmIsUserDefinedFunc(pCtx->functionId);
      if (fmIsAggFunc(pCtx->functionId) || fmIsIndefiniteRowsFunc(pCtx->functionId)) {
        if (!isUdaf) {
          code = fmGetFuncExecFuncs(pCtx->functionId, &pCtx->fpSet);
          QUERY_CHECK_CODE(code, lino, _end);
        } else {
          char* udfName = pExpr->pExpr->_function.pFunctNode->functionName;
          pCtx->udfName = taosStrdup(udfName);
          QUERY_CHECK_NULL(pCtx->udfName, code, lino, _end, terrno);

          code = fmGetUdafExecFuncs(pCtx->functionId, &pCtx->fpSet);
          QUERY_CHECK_CODE(code, lino, _end);
        }
        bool tmp = pCtx->fpSet.getEnv(pExpr->pExpr->_function.pFunctNode, &env);
        if (!tmp) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          QUERY_CHECK_CODE(code, lino, _end);
        }
      } else {
        code = fmGetScalarFuncExecFuncs(pCtx->functionId, &pCtx->sfp);
        if (code != TSDB_CODE_SUCCESS && isUdaf) {
          code = TSDB_CODE_SUCCESS;
        }
        QUERY_CHECK_CODE(code, lino, _end);

        if (pCtx->sfp.getEnv != NULL) {
          bool tmp = pCtx->sfp.getEnv(pExpr->pExpr->_function.pFunctNode, &env);
          if (!tmp) {
            code = TSDB_CODE_OUT_OF_MEMORY;
            QUERY_CHECK_CODE(code, lino, _end);
          }
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
    QUERY_CHECK_NULL(pCtx->input.pData, code, lino, _end, terrno);
    pCtx->input.pColumnDataAgg = taosMemoryCalloc(pFunct->numOfParams, POINTER_BYTES);
    QUERY_CHECK_NULL(pCtx->input.pColumnDataAgg, code, lino, _end, terrno);

    pCtx->pTsOutput = NULL;
    pCtx->resDataInfo.bytes = pFunct->resSchema.bytes;
    pCtx->resDataInfo.type = pFunct->resSchema.type;
    pCtx->order = TSDB_ORDER_ASC;
    pCtx->start.key = INT64_MIN;
    pCtx->end.key = INT64_MIN;
    pCtx->numOfParams = pExpr->base.numOfParams;
    pCtx->param = pFunct->pParam;
    pCtx->saveHandle.currentPage = -1;
    pCtx->pStore = pStore;
    pCtx->hasWindowOrGroup = false;
    pCtx->needCleanup = false;
  }

  for (int32_t i = 1; i < numOfOutput; ++i) {
    (*rowEntryInfoOffset)[i] = (int32_t)((*rowEntryInfoOffset)[i - 1] + sizeof(SResultRowEntryInfo) +
                                         pFuncCtx[i - 1].resDataInfo.interBufSize);
  }

  code = setSelectValueColumnInfo(pFuncCtx, numOfOutput);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    for (int32_t i = 0; i < numOfOutput; ++i) {
      taosMemoryFree(pFuncCtx[i].input.pData);
      taosMemoryFree(pFuncCtx[i].input.pColumnDataAgg);
    }
    taosMemoryFreeClear(*rowEntryInfoOffset);
    taosMemoryFreeClear(pFuncCtx);
    return NULL;
  }
  return pFuncCtx;
}

// NOTE: sources columns are more than the destination SSDatablock columns.
// doFilter in table scan needs every column even its output is false
int32_t relocateColumnData(SSDataBlock* pBlock, const SArray* pColMatchInfo, SArray* pCols, bool outputEveryColumn) {
  int32_t code = TSDB_CODE_SUCCESS;
  size_t  numOfSrcCols = taosArrayGetSize(pCols);

  int32_t i = 0, j = 0;
  while (i < numOfSrcCols && j < taosArrayGetSize(pColMatchInfo)) {
    SColumnInfoData* p = taosArrayGet(pCols, i);
    if (!p) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    SColMatchItem* pmInfo = taosArrayGet(pColMatchInfo, j);
    if (!pmInfo) {
      return terrno;
    }

    if (p->info.colId == pmInfo->colId) {
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, pmInfo->dstSlotId);
      if (!pDst) {
        return terrno;
      }
      code = colDataAssign(pDst, p, pBlock->info.rows, &pBlock->info);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        return code;
      }
      i++;
      j++;
    } else if (p->info.colId < pmInfo->colId) {
      i++;
    } else {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR));
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
  }
  return code;
}

SInterval extractIntervalInfo(const STableScanPhysiNode* pTableScanNode) {
  SInterval interval = {
      .interval = pTableScanNode->interval,
      .sliding = pTableScanNode->sliding,
      .intervalUnit = pTableScanNode->intervalUnit,
      .slidingUnit = pTableScanNode->slidingUnit,
      .offset = pTableScanNode->offset,
      .precision = pTableScanNode->scan.node.pOutputDataBlockDesc->precision,
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

int32_t initQueryTableDataCond(SQueryTableDataCond* pCond, const STableScanPhysiNode* pTableScanNode,
                               const SReadHandle* readHandle) {
  pCond->order = pTableScanNode->scanSeq[0] > 0 ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
  pCond->numOfCols = LIST_LENGTH(pTableScanNode->scan.pScanCols);

  pCond->colList = taosMemoryCalloc(pCond->numOfCols, sizeof(SColumnInfo));
  if (!pCond->colList) {
    return terrno;
  }
  pCond->pSlotList = taosMemoryMalloc(sizeof(int32_t) * pCond->numOfCols);
  if (pCond->pSlotList == NULL) {
    taosMemoryFreeClear(pCond->colList);
    return terrno;
  }

  // TODO: get it from stable scan node
  pCond->twindows = pTableScanNode->scanRange;
  pCond->suid = pTableScanNode->scan.suid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = -1;
  pCond->skipRollup = readHandle->skipRollup;

  // allowed read stt file optimization mode
  pCond->notLoadData = (pTableScanNode->dataRequired == FUNC_DATA_REQUIRED_NOT_LOAD) &&
                       (pTableScanNode->scan.node.pConditions == NULL) && (pTableScanNode->interval == 0);

  int32_t j = 0;
  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pTableScanNode->scan.pScanCols, i);
    if (!pNode) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;
    if (pColNode->colType == COLUMN_TYPE_TAG) {
      continue;
    }

    pCond->colList[j].type = pColNode->node.resType.type;
    pCond->colList[j].bytes = pColNode->node.resType.bytes;
    pCond->colList[j].colId = pColNode->colId;
    pCond->colList[j].pk = pColNode->isPk;

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

void getInitialStartTimeWindow(SInterval* pInterval, TSKEY ts, STimeWindow* w, bool ascQuery) {
  if (ascQuery) {
    *w = getAlignQueryTimeWindow(pInterval, ts);
  } else {
    // the start position of the first time window in the endpoint that spreads beyond the queried last timestamp
    *w = getAlignQueryTimeWindow(pInterval, ts);

    int64_t key = w->skey;
    while (key < ts) {  // moving towards end
      key = taosTimeAdd(key, pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
      if (key > ts) {
        break;
      }

      w->skey = key;
    }
  }
}

static STimeWindow doCalculateTimeWindow(int64_t ts, SInterval* pInterval) {
  STimeWindow w = {0};

  w.skey = taosTimeTruncate(ts, pInterval);
  w.ekey = taosTimeGetIntervalEnd(w.skey, pInterval);
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
    w.ekey = taosTimeGetIntervalEnd(w.skey, pInterval);
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

void getNextTimeWindow(const SInterval* pInterval, STimeWindow* tw, int32_t order) {
  int64_t slidingStart = 0;
  if (pInterval->offset > 0) {
    slidingStart = taosTimeAdd(tw->skey, -1 * pInterval->offset, pInterval->offsetUnit, pInterval->precision);
  } else {
    slidingStart = tw->skey;
  }
  int32_t factor = GET_FORWARD_DIRECTION_FACTOR(order);
  slidingStart = taosTimeAdd(slidingStart, factor * pInterval->sliding, pInterval->slidingUnit, pInterval->precision);
  tw->skey = taosTimeAdd(slidingStart, pInterval->offset, pInterval->offsetUnit, pInterval->precision);
  int64_t slidingEnd =
      taosTimeAdd(slidingStart, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;
  tw->ekey = taosTimeAdd(slidingEnd, pInterval->offset, pInterval->offsetUnit, pInterval->precision);
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

int32_t tableListGetSize(const STableListInfo* pTableList, int32_t* pRes) {
  if (taosArrayGetSize(pTableList->pTableList) != taosHashGetSize(pTableList->map)) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR));
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  (*pRes) = taosArrayGetSize(pTableList->pTableList);
  return TSDB_CODE_SUCCESS;
}

uint64_t tableListGetSuid(const STableListInfo* pTableList) { return pTableList->idInfo.suid; }

STableKeyInfo* tableListGetInfo(const STableListInfo* pTableList, int32_t index) {
  if (taosArrayGetSize(pTableList->pTableList) == 0) {
    return NULL;
  }

  return taosArrayGet(pTableList->pTableList, index);
}

int32_t tableListFind(const STableListInfo* pTableList, uint64_t uid, int32_t startIndex) {
  int32_t numOfTables = taosArrayGetSize(pTableList->pTableList);
  if (startIndex >= numOfTables) {
    return -1;
  }

  for (int32_t i = startIndex; i < numOfTables; ++i) {
    STableKeyInfo* p = taosArrayGet(pTableList->pTableList, i);
    if (!p) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return -1;
    }
    if (p->uid == uid) {
      return i;
    }
  }
  return -1;
}

void tableListGetSourceTableInfo(const STableListInfo* pTableList, uint64_t* psuid, uint64_t* uid, int32_t* type) {
  *psuid = pTableList->idInfo.suid;
  *uid = pTableList->idInfo.uid;
  *type = pTableList->idInfo.tableType;
}

uint64_t tableListGetTableGroupId(const STableListInfo* pTableList, uint64_t tableUid) {
  int32_t* slot = taosHashGet(pTableList->map, &tableUid, sizeof(tableUid));
  if (slot == NULL) {
    return -1;
  }

  STableKeyInfo* pKeyInfo = taosArrayGet(pTableList->pTableList, *slot);

  return pKeyInfo->groupId;
}

// TODO handle the group offset info, fix it, the rule of group output will be broken by this function
int32_t tableListAddTableInfo(STableListInfo* pTableList, uint64_t uid, uint64_t gid) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pTableList->map == NULL) {
    pTableList->map = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    QUERY_CHECK_NULL(pTableList->map, code, lino, _end, terrno);
  }

  STableKeyInfo keyInfo = {.uid = uid, .groupId = gid};
  void*         p = taosHashGet(pTableList->map, &uid, sizeof(uid));
  if (p != NULL) {
    qInfo("table:%" PRId64 " already in tableIdList, ignore it", uid);
    goto _end;
  }

  void* tmp = taosArrayPush(pTableList->pTableList, &keyInfo);
  QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

  int32_t slot = (int32_t)taosArrayGetSize(pTableList->pTableList) - 1;
  code = taosHashPut(pTableList->map, &uid, sizeof(uid), &slot, sizeof(slot));
  if (code != TSDB_CODE_SUCCESS) {
    // we have checked the existence of uid in hash map above
    QUERY_CHECK_CONDITION((code != TSDB_CODE_DUP_KEY), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    taosArrayPopTailBatch(pTableList->pTableList, 1);  // let's pop the last element in the array list
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  } else {
    qDebug("uid:%" PRIu64 ", groupId:%" PRIu64 " added into table list, slot:%d, total:%d", uid, gid, slot, slot + 1);
  }

  return code;
}

int32_t tableListGetGroupList(const STableListInfo* pTableList, int32_t ordinalGroupIndex, STableKeyInfo** pKeyInfo,
                              int32_t* size) {
  int32_t totalGroups = tableListGetOutputGroups(pTableList);
  int32_t numOfTables = 0;
  int32_t code = tableListGetSize(pTableList, &numOfTables);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

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
    return NULL;
  }

  pListInfo->remainGroups = NULL;
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
  return NULL;
}

void tableListDestroy(STableListInfo* pTableListInfo) {
  if (pTableListInfo == NULL) {
    return;
  }

  taosArrayDestroy(pTableListInfo->pTableList);
  taosMemoryFreeClear(pTableListInfo->groupOffset);

  taosHashCleanup(pTableListInfo->map);
  taosHashCleanup(pTableListInfo->remainGroups);
  pTableListInfo->pTableList = NULL;
  pTableListInfo->map = NULL;
  taosMemoryFree(pTableListInfo);
}

void tableListClear(STableListInfo* pTableListInfo) {
  if (pTableListInfo == NULL) {
    return;
  }

  taosArrayClear(pTableListInfo->pTableList);
  taosHashClear(pTableListInfo->map);
  taosHashClear(pTableListInfo->remainGroups);
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
  if (!pList) {
    return terrno;
  }

  STableKeyInfo* pInfo = taosArrayGet(pTableListInfo->pTableList, 0);
  if (!pInfo) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return terrno;
  }
  uint64_t       gid = pInfo->groupId;

  int32_t start = 0;
  void*   tmp = taosArrayPush(pList, &start);
  if (!tmp) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return terrno;
  }

  for (int32_t i = 1; i < size; ++i) {
    pInfo = taosArrayGet(pTableListInfo->pTableList, i);
    if (!pInfo) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    if (pInfo->groupId != gid) {
      tmp = taosArrayPush(pList, &i);
      if (!tmp) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
        return terrno;
      }
      gid = pInfo->groupId;
    }
  }

  pTableListInfo->numOfOuputGroups = taosArrayGetSize(pList);
  pTableListInfo->groupOffset = taosMemoryMalloc(sizeof(int32_t) * pTableListInfo->numOfOuputGroups);
  if (pTableListInfo->groupOffset == NULL) {
    taosArrayDestroy(pList);
    return terrno;
  }

  memcpy(pTableListInfo->groupOffset, taosArrayGet(pList, 0), sizeof(int32_t) * pTableListInfo->numOfOuputGroups);
  taosArrayDestroy(pList);
  return TSDB_CODE_SUCCESS;
}

int32_t buildGroupIdMapForAllTables(STableListInfo* pTableListInfo, SReadHandle* pHandle, SScanPhysiNode* pScanNode,
                                    SNodeList* group, bool groupSort, uint8_t* digest, SStorageAPI* pAPI) {
  int32_t code = TSDB_CODE_SUCCESS;

  bool   groupByTbname = groupbyTbname(group);
  size_t numOfTables = taosArrayGetSize(pTableListInfo->pTableList);
  if (!numOfTables) {
    return code;
  }
  if (group == NULL || groupByTbname) {
    if (tsCountAlwaysReturnValue && QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == nodeType(pScanNode) &&
        ((STableScanPhysiNode*)pScanNode)->needCountEmptyTable) {
      pTableListInfo->remainGroups =
          taosHashInit(numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      if (pTableListInfo->remainGroups == NULL) {
        return terrno;
      }

      for (int i = 0; i < numOfTables; i++) {
        STableKeyInfo* info = taosArrayGet(pTableListInfo->pTableList, i);
        if (!info) {
          qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
          return terrno;
        }
        info->groupId = groupByTbname ? info->uid : 0;

        int32_t tempRes = taosHashPut(pTableListInfo->remainGroups, &(info->groupId), sizeof(info->groupId),
                                      &(info->uid), sizeof(info->uid));
        if (tempRes != TSDB_CODE_SUCCESS && tempRes != TSDB_CODE_DUP_KEY) {
          qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(tempRes));
          return tempRes;
        }
      }
    } else {
      for (int32_t i = 0; i < numOfTables; i++) {
        STableKeyInfo* info = taosArrayGet(pTableListInfo->pTableList, i);
        if (!info) {
          qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
          return terrno;
        }
        info->groupId = groupByTbname ? info->uid : 0;
      }
    }

    pTableListInfo->oneTableForEachGroup = groupByTbname;
    if (numOfTables == 1 && pTableListInfo->idInfo.tableType == TSDB_CHILD_TABLE) {
      pTableListInfo->oneTableForEachGroup = true;
    }

    if (groupSort && groupByTbname) {
      taosArraySort(pTableListInfo->pTableList, orderbyGroupIdComparFn);
      pTableListInfo->numOfOuputGroups = numOfTables;
    } else if (groupByTbname && pScanNode->groupOrderScan) {
      pTableListInfo->numOfOuputGroups = numOfTables;
    } else {
      pTableListInfo->numOfOuputGroups = 1;
    }
  } else {
    bool initRemainGroups = false;
    if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == nodeType(pScanNode)) {
      STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pScanNode;
      if (tsCountAlwaysReturnValue && pTableScanNode->needCountEmptyTable &&
          !(groupSort || pScanNode->groupOrderScan)) {
        initRemainGroups = true;
      }
    }

    code = getColInfoResultForGroupby(pHandle->vnode, group, pTableListInfo, digest, pAPI, initRemainGroups);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pScanNode->groupOrderScan) pTableListInfo->numOfOuputGroups = taosArrayGetSize(pTableListInfo->pTableList);

    if (groupSort || pScanNode->groupOrderScan) {
      code = sortTableGroup(pTableListInfo);
    }
  }

  // add all table entry in the hash map
  size_t size = taosArrayGetSize(pTableListInfo->pTableList);
  for (int32_t i = 0; i < size; ++i) {
    STableKeyInfo* p = taosArrayGet(pTableListInfo->pTableList, i);
    if (!p) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    int32_t        tempRes = taosHashPut(pTableListInfo->map, &p->uid, sizeof(uint64_t), &i, sizeof(int32_t));
    if (tempRes != TSDB_CODE_SUCCESS && tempRes != TSDB_CODE_DUP_KEY) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(tempRes));
      return tempRes;
    }
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

  uint8_t digest[17] = {0};
  int32_t code = getTableList(pHandle->vnode, pScanNode, pTagCond, pTagIndexCond, pTableListInfo, digest, idStr,
                              &pTaskInfo->storageAPI);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to getTableList, code: %s", tstrerror(code));
    return code;
  }

  int32_t numOfTables = taosArrayGetSize(pTableListInfo->pTableList);

  int64_t st1 = taosGetTimestampUs();
  pTaskInfo->cost.extractListTime = (st1 - st) / 1000.0;
  qDebug("extract queried table list completed, %d tables, elapsed time:%.2f ms %s", numOfTables,
         pTaskInfo->cost.extractListTime, idStr);

  if (numOfTables == 0) {
    qDebug("no table qualified for query, %s" PRIx64, idStr);
    return TSDB_CODE_SUCCESS;
  }

  code = buildGroupIdMapForAllTables(pTableListInfo, pHandle, pScanNode, pGroupTags, groupSort, digest,
                                     &pTaskInfo->storageAPI);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pTaskInfo->cost.groupIdMapTime = (taosGetTimestampUs() - st1) / 1000.0;
  qDebug("generate group id map completed, elapsed time:%.2f ms %s", pTaskInfo->cost.groupIdMapTime, idStr);

  return TSDB_CODE_SUCCESS;
}

char* getStreamOpName(uint16_t opType) {
  switch (opType) {
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
      return "stream scan";
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      return "project";
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL:
      return "interval single";
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL:
      return "interval final";
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL:
      return "interval semi";
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL:
      return "interval mid";
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL:
      return "stream fill";
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION:
      return "session single";
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION:
      return "session semi";
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION:
      return "session final";
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE:
      return "state single";
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION:
      return "stream partitionby";
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT:
      return "stream event";
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT:
      return "stream count";
  }
  return "";
}

void printDataBlock(SSDataBlock* pBlock, const char* flag, const char* taskIdStr) {
  if (!pBlock) {
    qDebug("%s===stream===%s: Block is Null", taskIdStr, flag);
    return;
  } else if (pBlock->info.rows == 0) {
    qDebug("%s===stream===%s: Block is Empty. block type %d", taskIdStr, flag, pBlock->info.type);
    return;
  }
  if (qDebugFlag & DEBUG_DEBUG) {
    char*   pBuf = NULL;
    int32_t code = dumpBlockData(pBlock, flag, &pBuf, taskIdStr);
    if (code == 0) {
      qDebug("%s", pBuf);
      taosMemoryFree(pBuf);
    }
  }
}

void printSpecDataBlock(SSDataBlock* pBlock, const char* flag, const char* opStr, const char* taskIdStr) {
  if (!pBlock) {
    qDebug("%s===stream===%s %s: Block is Null", taskIdStr, flag, opStr);
    return;
  } else if (pBlock->info.rows == 0) {
    qDebug("%s===stream===%s %s: Block is Empty. block type %d", taskIdStr, flag, opStr, pBlock->info.type);
    return;
  }
  if (qDebugFlag & DEBUG_DEBUG) {
    char* pBuf = NULL;
    char  flagBuf[64];
    snprintf(flagBuf, sizeof(flagBuf), "%s %s", flag, opStr);
    int32_t code = dumpBlockData(pBlock, flagBuf, &pBuf, taskIdStr);
    if (code == 0) {
      qDebug("%s", pBuf);
      taosMemoryFree(pBuf);
    }
  }
}

TSKEY getStartTsKey(STimeWindow* win, const TSKEY* tsCols) { return tsCols == NULL ? win->skey : tsCols[0]; }

void updateTimeWindowInfo(SColumnInfoData* pColData, STimeWindow* pWin, int64_t delta) {
  int64_t* ts = (int64_t*)pColData->pData;

  int64_t duration = pWin->ekey - pWin->skey + delta;
  ts[2] = duration;            // set the duration
  ts[3] = pWin->skey;          // window start key
  ts[4] = pWin->ekey + delta;  // window end key
}

int32_t compKeys(const SArray* pSortGroupCols, const char* oldkeyBuf, int32_t oldKeysLen, const SSDataBlock* pBlock,
                 int32_t rowIndex) {
  SColumnDataAgg* pColAgg = NULL;
  const char*     isNull = oldkeyBuf;
  const char*     p = oldkeyBuf + sizeof(int8_t) * pSortGroupCols->size;

  for (int32_t i = 0; i < pSortGroupCols->size; ++i) {
    const SColumn*         pCol = (SColumn*)TARRAY_GET_ELEM(pSortGroupCols, i);
    const SColumnInfoData* pColInfoData = TARRAY_GET_ELEM(pBlock->pDataBlock, pCol->slotId);
    if (pBlock->pBlockAgg) pColAgg = &pBlock->pBlockAgg[pCol->slotId];

    if (colDataIsNull(pColInfoData, pBlock->info.rows, rowIndex, pColAgg)) {
      if (isNull[i] != 1) return 1;
    } else {
      if (isNull[i] != 0) return 1;
      const char* val = colDataGetData(pColInfoData, rowIndex);
      if (pCol->type == TSDB_DATA_TYPE_JSON) {
        int32_t len = getJsonValueLen(val);
        if (memcmp(p, val, len) != 0) return 1;
        p += len;
      } else if (IS_VAR_DATA_TYPE(pCol->type)) {
        if (memcmp(p, val, varDataTLen(val)) != 0) return 1;
        p += varDataTLen(val);
      } else {
        if (0 != memcmp(p, val, pCol->bytes)) return 1;
        p += pCol->bytes;
      }
    }
  }
  if ((int32_t)(p - oldkeyBuf) != oldKeysLen) return 1;
  return 0;
}

int32_t buildKeys(char* keyBuf, const SArray* pSortGroupCols, const SSDataBlock* pBlock, int32_t rowIndex) {
  uint32_t        colNum = pSortGroupCols->size;
  SColumnDataAgg* pColAgg = NULL;
  char*           isNull = keyBuf;
  char*           p = keyBuf + sizeof(int8_t) * colNum;

  for (int32_t i = 0; i < colNum; ++i) {
    const SColumn*         pCol = (SColumn*)TARRAY_GET_ELEM(pSortGroupCols, i);
    const SColumnInfoData* pColInfoData = TARRAY_GET_ELEM(pBlock->pDataBlock, pCol->slotId);
    if (pCol->slotId > pBlock->pDataBlock->size) continue;

    if (pBlock->pBlockAgg) pColAgg = &pBlock->pBlockAgg[pCol->slotId];

    if (colDataIsNull(pColInfoData, pBlock->info.rows, rowIndex, pColAgg)) {
      isNull[i] = 1;
    } else {
      isNull[i] = 0;
      const char* val = colDataGetData(pColInfoData, rowIndex);
      if (pCol->type == TSDB_DATA_TYPE_JSON) {
        int32_t len = getJsonValueLen(val);
        memcpy(p, val, len);
        p += len;
      } else if (IS_VAR_DATA_TYPE(pCol->type)) {
        varDataCopy(p, val);
        p += varDataTLen(val);
      } else {
        memcpy(p, val, pCol->bytes);
        p += pCol->bytes;
      }
    }
  }
  return (int32_t)(p - keyBuf);
}

uint64_t calcGroupId(char* pData, int32_t len) {
  T_MD5_CTX context;
  tMD5Init(&context);
  tMD5Update(&context, (uint8_t*)pData, len);
  tMD5Final(&context);

  // NOTE: only extract the initial 8 bytes of the final MD5 digest
  uint64_t id = 0;
  memcpy(&id, context.digest, sizeof(uint64_t));
  if (0 == id) memcpy(&id, context.digest + 8, sizeof(uint64_t));
  return id;
}

SNodeList* makeColsNodeArrFromSortKeys(SNodeList* pSortKeys) {
  SNode*     node;
  SNodeList* ret = NULL;
  FOREACH(node, pSortKeys) {
    SOrderByExprNode* pSortKey = (SOrderByExprNode*)node;
    int32_t           code = nodesListMakeAppend(&ret, pSortKey->pExpr);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      return NULL;
    }
  }
  return ret;
}

int32_t extractKeysLen(const SArray* keys, int32_t* pLen) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t len = 0;
  int32_t keyNum = taosArrayGetSize(keys);
  for (int32_t i = 0; i < keyNum; ++i) {
    SColumn* pCol = (SColumn*)taosArrayGet(keys, i);
    QUERY_CHECK_NULL(pCol, code, lino, _end, terrno);
    len += pCol->bytes;
  }
  len += sizeof(int8_t) * keyNum;  // null flag
  *pLen = len;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
