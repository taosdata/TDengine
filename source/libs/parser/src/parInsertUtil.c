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

#include "parInsertUtil.h"

#include "catalog.h"
#include "parInt.h"
#include "parUtil.h"
#include "querynodes.h"
#include "tRealloc.h"

#define IS_RAW_PAYLOAD(t) \
  (((int)(t)) == PAYLOAD_TYPE_RAW)  // 0: K-V payload for non-prepare insert, 1: rawPayload for prepare insert

typedef struct SBlockKeyTuple {
  TSKEY   skey;
  void*   payloadAddr;
  int16_t index;
} SBlockKeyTuple;

typedef struct SBlockKeyInfo {
  int32_t         maxBytesAlloc;
  SBlockKeyTuple* pKeyTuple;
} SBlockKeyInfo;

typedef struct {
  int32_t   index;
  SArray*   rowArray;  // array of merged rows(mem allocated by tRealloc/free by tFree)
  STSchema* pSchema;
  int64_t   tbUid;  // suid for child table, uid for normal table
} SBlockRowMerger;

static FORCE_INLINE void tdResetSBlockRowMerger(SBlockRowMerger* pMerger) {
  if (pMerger) {
    pMerger->index = -1;
  }
}

static void tdFreeSBlockRowMerger(SBlockRowMerger* pMerger) {
  if (pMerger) {
    int32_t size = taosArrayGetSize(pMerger->rowArray);
    for (int32_t i = 0; i < size; ++i) {
      tFree(*(void**)taosArrayGet(pMerger->rowArray, i));
    }
    taosArrayDestroy(pMerger->rowArray);

    taosMemoryFreeClear(pMerger->pSchema);
    taosMemoryFree(pMerger);
  }
}

static int32_t rowDataCompar(const void* lhs, const void* rhs) {
  TSKEY left = *(TSKEY*)lhs;
  TSKEY right = *(TSKEY*)rhs;
  if (left == right) {
    return 0;
  } else {
    return left > right ? 1 : -1;
  }
}

static int32_t rowDataComparStable(const void* lhs, const void* rhs) {
  TSKEY left = *(TSKEY*)lhs;
  TSKEY right = *(TSKEY*)rhs;
  if (left == right) {
    return ((SBlockKeyTuple*)lhs)->index - ((SBlockKeyTuple*)rhs)->index;
  } else {
    return left > right ? 1 : -1;
  }
}

int32_t insGetExtendedRowSize(STableDataBlocks* pBlock) {
  STableComInfo* pTableInfo = &pBlock->pTableMeta->tableInfo;
  ASSERT(pBlock->rowSize == pTableInfo->rowSize);
  return pBlock->rowSize + TD_ROW_HEAD_LEN - sizeof(TSKEY) + pBlock->boundColumnInfo.extendedVarLen +
         (int32_t)TD_BITMAP_BYTES(pTableInfo->numOfColumns - 1);
}

void insGetSTSRowAppendInfo(uint8_t rowType, SParsedDataColInfo* spd, col_id_t idx, int32_t* toffset,
                            col_id_t* colIdx) {
  col_id_t schemaIdx = 0;
  if (IS_DATA_COL_ORDERED(spd)) {
    schemaIdx = spd->boundColumns[idx];
    if (TD_IS_TP_ROW_T(rowType)) {
      *toffset = (spd->cols + schemaIdx)->toffset;  // the offset of firstPart
      *colIdx = schemaIdx;
    } else {
      *toffset = idx * sizeof(SKvRowIdx);  // the offset of SKvRowIdx
      *colIdx = idx;
    }
  } else {
    ASSERT(idx == (spd->colIdxInfo + idx)->boundIdx);
    schemaIdx = (spd->colIdxInfo + idx)->schemaColIdx;
    if (TD_IS_TP_ROW_T(rowType)) {
      *toffset = (spd->cols + schemaIdx)->toffset;
      *colIdx = schemaIdx;
    } else {
      *toffset = ((spd->colIdxInfo + idx)->finalIdx) * sizeof(SKvRowIdx);
      *colIdx = (spd->colIdxInfo + idx)->finalIdx;
    }
  }
}

int32_t insSetBlockInfo(SSubmitBlk* pBlocks, STableDataBlocks* dataBuf, int32_t numOfRows) {
  pBlocks->suid = (TSDB_NORMAL_TABLE == dataBuf->pTableMeta->tableType ? 0 : dataBuf->pTableMeta->suid);
  pBlocks->uid = dataBuf->pTableMeta->uid;
  pBlocks->sversion = dataBuf->pTableMeta->sversion;
  pBlocks->schemaLen = dataBuf->createTbReqLen;

  if (pBlocks->numOfRows + numOfRows >= INT32_MAX) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  } else {
    pBlocks->numOfRows += numOfRows;
    return TSDB_CODE_SUCCESS;
  }
}

void insSetBoundColumnInfo(SParsedDataColInfo* pColList, SSchema* pSchema, col_id_t numOfCols) {
  pColList->numOfCols = numOfCols;
  pColList->numOfBound = numOfCols;
  pColList->orderStatus = ORDER_STATUS_ORDERED;  // default is ORDERED for non-bound mode
  pColList->boundColumns = taosMemoryCalloc(pColList->numOfCols, sizeof(col_id_t));
  pColList->cols = taosMemoryCalloc(pColList->numOfCols, sizeof(SBoundColumn));
  pColList->colIdxInfo = NULL;
  pColList->flen = 0;
  pColList->allNullLen = 0;

  int32_t nVar = 0;
  for (int32_t i = 0; i < pColList->numOfCols; ++i) {
    uint8_t type = pSchema[i].type;
    if (i > 0) {
      pColList->cols[i].offset = pColList->cols[i - 1].offset + pSchema[i - 1].bytes;
      pColList->cols[i].toffset = pColList->flen;
    }
    pColList->flen += TYPE_BYTES[type];
    switch (type) {
      case TSDB_DATA_TYPE_BINARY:
        pColList->allNullLen += (VARSTR_HEADER_SIZE + CHAR_BYTES);
        ++nVar;
        break;
      case TSDB_DATA_TYPE_NCHAR:
        pColList->allNullLen += (VARSTR_HEADER_SIZE + TSDB_NCHAR_SIZE);
        ++nVar;
        break;
      default:
        break;
    }
    pColList->boundColumns[i] = i;
  }
  pColList->allNullLen += pColList->flen;
  pColList->boundNullLen = pColList->allNullLen;  // default set allNullLen
  pColList->extendedVarLen = (uint16_t)(nVar * sizeof(VarDataOffsetT));
}

int32_t insSchemaIdxCompar(const void* lhs, const void* rhs) {
  uint16_t left = *(uint16_t*)lhs;
  uint16_t right = *(uint16_t*)rhs;

  if (left == right) {
    return 0;
  } else {
    return left > right ? 1 : -1;
  }
}

int32_t insBoundIdxCompar(const void* lhs, const void* rhs) {
  uint16_t left = *(uint16_t*)POINTER_SHIFT(lhs, sizeof(uint16_t));
  uint16_t right = *(uint16_t*)POINTER_SHIFT(rhs, sizeof(uint16_t));

  if (left == right) {
    return 0;
  } else {
    return left > right ? 1 : -1;
  }
}

void destroyBoundColumnInfo(void* pBoundInfo) {
  if (NULL == pBoundInfo) {
    return;
  }

  SParsedDataColInfo* pColList = (SParsedDataColInfo*)pBoundInfo;

  taosMemoryFreeClear(pColList->boundColumns);
  taosMemoryFreeClear(pColList->cols);
  taosMemoryFreeClear(pColList->colIdxInfo);
}

static int32_t createDataBlock(size_t defaultSize, int32_t rowSize, int32_t startOffset, STableMeta* pTableMeta,
                               STableDataBlocks** dataBlocks) {
  STableDataBlocks* dataBuf = (STableDataBlocks*)taosMemoryCalloc(1, sizeof(STableDataBlocks));
  if (dataBuf == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  dataBuf->nAllocSize = (uint32_t)defaultSize;
  dataBuf->headerSize = startOffset;

  // the header size will always be the startOffset value, reserved for the subumit block header
  if (dataBuf->nAllocSize <= dataBuf->headerSize) {
    dataBuf->nAllocSize = dataBuf->headerSize * 2;
  }

  dataBuf->pData = taosMemoryMalloc(dataBuf->nAllocSize);
  if (dataBuf->pData == NULL) {
    taosMemoryFreeClear(dataBuf);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  memset(dataBuf->pData, 0, sizeof(SSubmitBlk));

  dataBuf->pTableMeta = tableMetaDup(pTableMeta);

  SParsedDataColInfo* pColInfo = &dataBuf->boundColumnInfo;
  SSchema*            pSchema = getTableColumnSchema(dataBuf->pTableMeta);
  insSetBoundColumnInfo(pColInfo, pSchema, dataBuf->pTableMeta->tableInfo.numOfColumns);

  dataBuf->ordered = true;
  dataBuf->prevTS = INT64_MIN;
  dataBuf->rowSize = rowSize;
  dataBuf->size = startOffset;
  dataBuf->vgId = dataBuf->pTableMeta->vgId;

  assert(defaultSize > 0 && pTableMeta != NULL && dataBuf->pTableMeta != NULL);

  *dataBlocks = dataBuf;
  return TSDB_CODE_SUCCESS;
}

int32_t insBuildCreateTbMsg(STableDataBlocks* pBlocks, SVCreateTbReq* pCreateTbReq) {
  SEncoder coder = {0};
  char*    pBuf;
  int32_t  len;

  int32_t ret = 0;
  tEncodeSize(tEncodeSVCreateTbReq, pCreateTbReq, len, ret);
  if (pBlocks->nAllocSize - pBlocks->size < len) {
    pBlocks->nAllocSize += len + pBlocks->rowSize;
    char* pTmp = taosMemoryRealloc(pBlocks->pData, pBlocks->nAllocSize);
    if (pTmp != NULL) {
      pBlocks->pData = pTmp;
      memset(pBlocks->pData + pBlocks->size, 0, pBlocks->nAllocSize - pBlocks->size);
    } else {
      pBlocks->nAllocSize -= len + pBlocks->rowSize;
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }

  pBuf = pBlocks->pData + pBlocks->size;

  tEncoderInit(&coder, pBuf, len);
  int32_t code = tEncodeSVCreateTbReq(&coder, pCreateTbReq);
  tEncoderClear(&coder);
  pBlocks->size += len;
  pBlocks->createTbReqLen = len;

  return code;
}

void insDestroyDataBlock(STableDataBlocks* pDataBlock) {
  if (pDataBlock == NULL) {
    return;
  }

  taosMemoryFreeClear(pDataBlock->pData);
  //  if (!pDataBlock->cloned) {
  // free the refcount for metermeta
  taosMemoryFreeClear(pDataBlock->pTableMeta);

  destroyBoundColumnInfo(&pDataBlock->boundColumnInfo);
  //  }
  taosMemoryFreeClear(pDataBlock);
}

int32_t insGetDataBlockFromList(SHashObj* pHashList, void* id, int32_t idLen, int32_t size, int32_t startOffset,
                                int32_t rowSize, STableMeta* pTableMeta, STableDataBlocks** dataBlocks,
                                SArray* pBlockList, SVCreateTbReq* pCreateTbReq) {
  *dataBlocks = NULL;
  STableDataBlocks** t1 = (STableDataBlocks**)taosHashGet(pHashList, (const char*)id, idLen);
  if (t1 != NULL) {
    *dataBlocks = *t1;
  }

  if (*dataBlocks == NULL) {
    int32_t ret = createDataBlock((size_t)size, rowSize, startOffset, pTableMeta, dataBlocks);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    if (NULL != pCreateTbReq && NULL != pCreateTbReq->ctb.pTag) {
      ret = insBuildCreateTbMsg(*dataBlocks, pCreateTbReq);
      if (ret != TSDB_CODE_SUCCESS) {
        insDestroyDataBlock(*dataBlocks);
        return ret;
      }
    }

    // converting to 'const char*' is to handle coverity scan errors
    taosHashPut(pHashList, (const char*)id, idLen, (const char*)dataBlocks, POINTER_BYTES);
    if (pBlockList) {
      taosArrayPush(pBlockList, dataBlocks);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t getRowExpandSize(STableMeta* pTableMeta) {
  int32_t  result = TD_ROW_HEAD_LEN - sizeof(TSKEY);
  int32_t  columns = getNumOfColumns(pTableMeta);
  SSchema* pSchema = getTableColumnSchema(pTableMeta);
  for (int32_t i = 0; i < columns; ++i) {
    if (IS_VAR_DATA_TYPE((pSchema + i)->type)) {
      result += TYPE_BYTES[TSDB_DATA_TYPE_BINARY];
    }
  }
  result += (int32_t)TD_BITMAP_BYTES(columns - 1);
  return result;
}

void insDestroyBlockArrayList(SArray* pDataBlockList) {
  if (pDataBlockList == NULL) {
    return;
  }

  size_t size = taosArrayGetSize(pDataBlockList);
  for (int32_t i = 0; i < size; i++) {
    void* p = taosArrayGetP(pDataBlockList, i);
    insDestroyDataBlock(p);
  }

  taosArrayDestroy(pDataBlockList);
}

void insDestroyBlockHashmap(SHashObj* pDataBlockHash) {
  if (pDataBlockHash == NULL) {
    return;
  }

  void** p1 = taosHashIterate(pDataBlockHash, NULL);
  while (p1) {
    STableDataBlocks* pBlocks = *p1;
    insDestroyDataBlock(pBlocks);

    p1 = taosHashIterate(pDataBlockHash, p1);
  }

  taosHashCleanup(pDataBlockHash);
}

// data block is disordered, sort it in ascending order
void sortRemoveDataBlockDupRowsRaw(STableDataBlocks* dataBuf) {
  SSubmitBlk* pBlocks = (SSubmitBlk*)dataBuf->pData;

  // size is less than the total size, since duplicated rows may be removed yet.
  assert(pBlocks->numOfRows * dataBuf->rowSize + sizeof(SSubmitBlk) == dataBuf->size);

  if (!dataBuf->ordered) {
    char* pBlockData = pBlocks->data;

    // todo. qsort is unstable, if timestamp is same, should get the last one
    taosSort(pBlockData, pBlocks->numOfRows, dataBuf->rowSize, rowDataCompar);

    int32_t i = 0;
    int32_t j = 1;

    // delete rows with timestamp conflicts
    while (j < pBlocks->numOfRows) {
      TSKEY ti = *(TSKEY*)(pBlockData + dataBuf->rowSize * i);
      TSKEY tj = *(TSKEY*)(pBlockData + dataBuf->rowSize * j);

      if (ti == tj) {
        ++j;
        continue;
      }

      int32_t nextPos = (++i);
      if (nextPos != j) {
        memmove(pBlockData + dataBuf->rowSize * nextPos, pBlockData + dataBuf->rowSize * j, dataBuf->rowSize);
      }

      ++j;
    }

    dataBuf->ordered = true;

    pBlocks->numOfRows = i + 1;
    dataBuf->size = sizeof(SSubmitBlk) + dataBuf->rowSize * pBlocks->numOfRows;
  }

  dataBuf->prevTS = INT64_MIN;
}

// data block is disordered, sort it in ascending order
static int sortRemoveDataBlockDupRows(STableDataBlocks* dataBuf, SBlockKeyInfo* pBlkKeyInfo) {
  SSubmitBlk* pBlocks = (SSubmitBlk*)dataBuf->pData;
  int16_t     nRows = pBlocks->numOfRows;

  // size is less than the total size, since duplicated rows may be removed yet.

  // allocate memory
  size_t nAlloc = nRows * sizeof(SBlockKeyTuple);
  if (pBlkKeyInfo->pKeyTuple == NULL || pBlkKeyInfo->maxBytesAlloc < nAlloc) {
    char* tmp = taosMemoryRealloc(pBlkKeyInfo->pKeyTuple, nAlloc);
    if (tmp == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    pBlkKeyInfo->pKeyTuple = (SBlockKeyTuple*)tmp;
    pBlkKeyInfo->maxBytesAlloc = (int32_t)nAlloc;
  }
  memset(pBlkKeyInfo->pKeyTuple, 0, nAlloc);

  int32_t         extendedRowSize = insGetExtendedRowSize(dataBuf);
  SBlockKeyTuple* pBlkKeyTuple = pBlkKeyInfo->pKeyTuple;
  char*           pBlockData = pBlocks->data + pBlocks->schemaLen;
  int             n = 0;
  while (n < nRows) {
    pBlkKeyTuple->skey = TD_ROW_KEY((STSRow*)pBlockData);
    pBlkKeyTuple->payloadAddr = pBlockData;
    pBlkKeyTuple->index = n;

    // next loop
    pBlockData += extendedRowSize;
    ++pBlkKeyTuple;
    ++n;
  }

  if (!dataBuf->ordered) {
    pBlkKeyTuple = pBlkKeyInfo->pKeyTuple;

    // todo. qsort is unstable, if timestamp is same, should get the last one
    taosSort(pBlkKeyTuple, nRows, sizeof(SBlockKeyTuple), rowDataComparStable);

    pBlkKeyTuple = pBlkKeyInfo->pKeyTuple;
    int32_t i = 0;
    int32_t j = 1;
    while (j < nRows) {
      TSKEY ti = (pBlkKeyTuple + i)->skey;
      TSKEY tj = (pBlkKeyTuple + j)->skey;

      if (ti == tj) {
        ++j;
        continue;
      }

      int32_t nextPos = (++i);
      if (nextPos != j) {
        memmove(pBlkKeyTuple + nextPos, pBlkKeyTuple + j, sizeof(SBlockKeyTuple));
      }
      ++j;
    }

    dataBuf->ordered = true;
    pBlocks->numOfRows = i + 1;
  }

  dataBuf->size = sizeof(SSubmitBlk) + pBlocks->numOfRows * extendedRowSize;
  dataBuf->prevTS = INT64_MIN;

  return 0;
}

static void* tdGetCurRowFromBlockMerger(SBlockRowMerger* pBlkRowMerger) {
  if (pBlkRowMerger && (pBlkRowMerger->index >= 0)) {
    ASSERT(pBlkRowMerger->index < taosArrayGetSize(pBlkRowMerger->rowArray));
    return *(void**)taosArrayGet(pBlkRowMerger->rowArray, pBlkRowMerger->index);
  }
  return NULL;
}

static int32_t tdBlockRowMerge(STableMeta* pTableMeta, SBlockKeyTuple* pEndKeyTp, int32_t nDupRows,
                               SBlockRowMerger** pBlkRowMerger, int32_t rowSize) {
  ASSERT(nDupRows > 1);
  SBlockKeyTuple* pStartKeyTp = pEndKeyTp - (nDupRows - 1);
  ASSERT(pStartKeyTp->skey == pEndKeyTp->skey);

  // TODO: optimization if end row is all normal
#if 0
  STSRow* pEndRow = (STSRow*)pEndKeyTp->payloadAddr;
  if(isNormal(pEndRow)) { // set the end row if it is normal and return directly
    pStartKeyTp->payloadAddr = pEndKeyTp->payloadAddr;
    return TSDB_CODE_SUCCESS;
  }
#endif

  if (!(*pBlkRowMerger)) {
    (*pBlkRowMerger) = taosMemoryCalloc(1, sizeof(**pBlkRowMerger));
    if (!(*pBlkRowMerger)) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_FAILED;
    }
    (*pBlkRowMerger)->index = -1;
    if (!(*pBlkRowMerger)->rowArray) {
      (*pBlkRowMerger)->rowArray = taosArrayInit(1, sizeof(void*));
      if (!(*pBlkRowMerger)->rowArray) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return TSDB_CODE_FAILED;
      }
    }
  }

  if ((*pBlkRowMerger)->pSchema) {
    if ((*pBlkRowMerger)->pSchema->version != pTableMeta->sversion) {
      taosMemoryFreeClear((*pBlkRowMerger)->pSchema);
    } else {
      if ((*pBlkRowMerger)->tbUid != (pTableMeta->suid > 0 ? pTableMeta->suid : pTableMeta->uid)) {
        taosMemoryFreeClear((*pBlkRowMerger)->pSchema);
      }
    }
  }

  if (!(*pBlkRowMerger)->pSchema) {
    (*pBlkRowMerger)->pSchema =
        tdGetSTSChemaFromSSChema(pTableMeta->schema, pTableMeta->tableInfo.numOfColumns, pTableMeta->sversion);

    if (!(*pBlkRowMerger)->pSchema) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_FAILED;
    }
    (*pBlkRowMerger)->tbUid = pTableMeta->suid > 0 ? pTableMeta->suid : pTableMeta->uid;
  }

  void* pDestRow = NULL;
  ++((*pBlkRowMerger)->index);
  if ((*pBlkRowMerger)->index < taosArrayGetSize((*pBlkRowMerger)->rowArray)) {
    void** pAlloc = (void**)taosArrayGet((*pBlkRowMerger)->rowArray, (*pBlkRowMerger)->index);
    if (tRealloc((uint8_t**)pAlloc, rowSize) != 0) {
      return TSDB_CODE_FAILED;
    }
    pDestRow = *pAlloc;
  } else {
    if (tRealloc((uint8_t**)&pDestRow, rowSize) != 0) {
      return TSDB_CODE_FAILED;
    }
    taosArrayPush((*pBlkRowMerger)->rowArray, &pDestRow);
  }

  // merge rows to pDestRow
  STSchema* pSchema = (*pBlkRowMerger)->pSchema;
  SArray*   pArray = taosArrayInit(pSchema->numOfCols, sizeof(SColVal));
  for (int32_t i = 0; i < pSchema->numOfCols; ++i) {
    SColVal colVal = {0};
    for (int32_t j = 0; j < nDupRows; ++j) {
      tTSRowGetVal((pEndKeyTp - j)->payloadAddr, pSchema, i, &colVal);
      if (!COL_VAL_IS_NONE(&colVal)) {
        break;
      }
    }
    taosArrayPush(pArray, &colVal);
  }
  if (tdSTSRowNew(pArray, pSchema, (STSRow**)&pDestRow) < 0) {
    taosArrayDestroy(pArray);
    return TSDB_CODE_FAILED;
  }

  taosArrayDestroy(pArray);
  return TSDB_CODE_SUCCESS;
}

// data block is disordered, sort it in ascending order, and merge dup rows if exists
static int sortMergeDataBlockDupRows(STableDataBlocks* dataBuf, SBlockKeyInfo* pBlkKeyInfo,
                                     SBlockRowMerger** ppBlkRowMerger) {
  SSubmitBlk* pBlocks = (SSubmitBlk*)dataBuf->pData;
  STableMeta* pTableMeta = dataBuf->pTableMeta;
  int32_t     nRows = pBlocks->numOfRows;

  // size is less than the total size, since duplicated rows may be removed.

  // allocate memory
  size_t nAlloc = nRows * sizeof(SBlockKeyTuple);
  if (pBlkKeyInfo->pKeyTuple == NULL || pBlkKeyInfo->maxBytesAlloc < nAlloc) {
    char* tmp = taosMemoryRealloc(pBlkKeyInfo->pKeyTuple, nAlloc);
    if (tmp == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    pBlkKeyInfo->pKeyTuple = (SBlockKeyTuple*)tmp;
    pBlkKeyInfo->maxBytesAlloc = (int32_t)nAlloc;
  }
  memset(pBlkKeyInfo->pKeyTuple, 0, nAlloc);

  tdResetSBlockRowMerger(*ppBlkRowMerger);

  int32_t         extendedRowSize = insGetExtendedRowSize(dataBuf);
  SBlockKeyTuple* pBlkKeyTuple = pBlkKeyInfo->pKeyTuple;
  char*           pBlockData = pBlocks->data + pBlocks->schemaLen;
  int32_t         n = 0;
  while (n < nRows) {
    pBlkKeyTuple->skey = TD_ROW_KEY((STSRow*)pBlockData);
    pBlkKeyTuple->payloadAddr = pBlockData;
    pBlkKeyTuple->index = n;

    // next loop
    pBlockData += extendedRowSize;
    ++pBlkKeyTuple;
    ++n;
  }

  if (!dataBuf->ordered) {
    pBlkKeyTuple = pBlkKeyInfo->pKeyTuple;

    taosSort(pBlkKeyTuple, nRows, sizeof(SBlockKeyTuple), rowDataComparStable);

    pBlkKeyTuple = pBlkKeyInfo->pKeyTuple;
    bool    hasDup = false;
    int32_t nextPos = 0;
    int32_t i = 0;
    int32_t j = 1;

    while (j < nRows) {
      TSKEY ti = (pBlkKeyTuple + i)->skey;
      TSKEY tj = (pBlkKeyTuple + j)->skey;

      if (ti == tj) {
        ++j;
        continue;
      }

      if ((j - i) > 1) {
        if (tdBlockRowMerge(pTableMeta, (pBlkKeyTuple + j - 1), j - i, ppBlkRowMerger, extendedRowSize) < 0) {
          return TSDB_CODE_FAILED;
        }
        (pBlkKeyTuple + nextPos)->payloadAddr = tdGetCurRowFromBlockMerger(*ppBlkRowMerger);
        if (!hasDup) {
          hasDup = true;
        }
        i = j;
      } else {
        if (hasDup) {
          memmove(pBlkKeyTuple + nextPos, pBlkKeyTuple + i, sizeof(SBlockKeyTuple));
        }
        ++i;
      }

      ++nextPos;
      ++j;
    }

    if ((j - i) > 1) {
      ASSERT((pBlkKeyTuple + i)->skey == (pBlkKeyTuple + j - 1)->skey);
      if (tdBlockRowMerge(pTableMeta, (pBlkKeyTuple + j - 1), j - i, ppBlkRowMerger, extendedRowSize) < 0) {
        return TSDB_CODE_FAILED;
      }
      (pBlkKeyTuple + nextPos)->payloadAddr = tdGetCurRowFromBlockMerger(*ppBlkRowMerger);
    } else if (hasDup) {
      memmove(pBlkKeyTuple + nextPos, pBlkKeyTuple + i, sizeof(SBlockKeyTuple));
    }

    dataBuf->ordered = true;
    pBlocks->numOfRows = nextPos + 1;
  }

  dataBuf->size = sizeof(SSubmitBlk) + pBlocks->numOfRows * extendedRowSize;
  dataBuf->prevTS = INT64_MIN;

  return TSDB_CODE_SUCCESS;
}

// Erase the empty space reserved for binary data
static int trimDataBlock(void* pDataBlock, STableDataBlocks* pTableDataBlock, SBlockKeyTuple* blkKeyTuple,
                         bool isRawPayload) {
  // TODO: optimize this function, handle the case while binary is not presented
  STableMeta*   pTableMeta = pTableDataBlock->pTableMeta;
  STableComInfo tinfo = getTableInfo(pTableMeta);
  SSchema*      pSchema = getTableColumnSchema(pTableMeta);

  int32_t     nonDataLen = sizeof(SSubmitBlk) + pTableDataBlock->createTbReqLen;
  SSubmitBlk* pBlock = pDataBlock;
  memcpy(pDataBlock, pTableDataBlock->pData, nonDataLen);
  pDataBlock = (char*)pDataBlock + nonDataLen;

  int32_t flen = 0;  // original total length of row
  if (isRawPayload) {
    for (int32_t j = 0; j < tinfo.numOfColumns; ++j) {
      flen += TYPE_BYTES[pSchema[j].type];
    }
  }
  pBlock->schemaLen = pTableDataBlock->createTbReqLen;

  char* p = pTableDataBlock->pData + nonDataLen;
  pBlock->dataLen = 0;
  int32_t numOfRows = pBlock->numOfRows;

  if (isRawPayload) {
    SRowBuilder builder = {0};

    tdSRowInit(&builder, pTableMeta->sversion);
    tdSRowSetInfo(&builder, getNumOfColumns(pTableMeta), -1, flen);

    for (int32_t i = 0; i < numOfRows; ++i) {
      tdSRowResetBuf(&builder, pDataBlock);
      int toffset = 0;
      for (int32_t j = 0; j < tinfo.numOfColumns; ++j) {
        int8_t  colType = pSchema[j].type;
        uint8_t valType = isNull(p, colType) ? TD_VTYPE_NULL : TD_VTYPE_NORM;
        tdAppendColValToRow(&builder, pSchema[j].colId, colType, valType, p, true, toffset, j);
        toffset += TYPE_BYTES[colType];
        p += pSchema[j].bytes;
      }
      tdSRowEnd(&builder);
      int32_t rowLen = TD_ROW_LEN((STSRow*)pDataBlock);
      pDataBlock = (char*)pDataBlock + rowLen;
      pBlock->dataLen += rowLen;
    }
  } else {
    for (int32_t i = 0; i < numOfRows; ++i) {
      void*     payload = (blkKeyTuple + i)->payloadAddr;
      TDRowLenT rowTLen = TD_ROW_LEN((STSRow*)payload);
      memcpy(pDataBlock, payload, rowTLen);
      pDataBlock = POINTER_SHIFT(pDataBlock, rowTLen);
      pBlock->dataLen += rowTLen;
    }
  }

  return pBlock->dataLen + pBlock->schemaLen;
}

int32_t insMergeTableDataBlocks(SHashObj* pHashObj, uint8_t payloadType, SArray** pVgDataBlocks) {
  const int INSERT_HEAD_SIZE = sizeof(SSubmitReq);
  int       code = 0;
  bool      isRawPayload = IS_RAW_PAYLOAD(payloadType);
  SHashObj* pVnodeDataBlockHashList = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, false);
  SArray*   pVnodeDataBlockList = taosArrayInit(8, POINTER_BYTES);

  STableDataBlocks** p = taosHashIterate(pHashObj, NULL);
  STableDataBlocks*  pOneTableBlock = *p;
  SBlockKeyInfo      blkKeyInfo = {0};  // share by pOneTableBlock
  SBlockRowMerger*   pBlkRowMerger = NULL;

  while (pOneTableBlock) {
    SSubmitBlk* pBlocks = (SSubmitBlk*)pOneTableBlock->pData;
    if (pBlocks->numOfRows > 0) {
      STableDataBlocks* dataBuf = NULL;
      pOneTableBlock->pTableMeta->vgId = pOneTableBlock->vgId;  // for schemaless, restore origin vgId
      int32_t ret = insGetDataBlockFromList(pVnodeDataBlockHashList, &pOneTableBlock->vgId,
                                            sizeof(pOneTableBlock->vgId), TSDB_PAYLOAD_SIZE, INSERT_HEAD_SIZE, 0,
                                            pOneTableBlock->pTableMeta, &dataBuf, pVnodeDataBlockList, NULL);
      if (ret != TSDB_CODE_SUCCESS) {
        tdFreeSBlockRowMerger(pBlkRowMerger);
        taosHashCleanup(pVnodeDataBlockHashList);
        insDestroyBlockArrayList(pVnodeDataBlockList);
        taosMemoryFreeClear(blkKeyInfo.pKeyTuple);
        return ret;
      }
      ASSERT(pOneTableBlock->pTableMeta->tableInfo.rowSize > 0);
      // the maximum expanded size in byte when a row-wise data is converted to SDataRow format
      int32_t expandSize = isRawPayload ? getRowExpandSize(pOneTableBlock->pTableMeta) : 0;
      int64_t destSize = dataBuf->size + pOneTableBlock->size + pBlocks->numOfRows * expandSize +
                         sizeof(STColumn) * getNumOfColumns(pOneTableBlock->pTableMeta) +
                         pOneTableBlock->createTbReqLen;

      if (dataBuf->nAllocSize < destSize) {
        dataBuf->nAllocSize = (uint32_t)(destSize * 1.5);
        char* tmp = taosMemoryRealloc(dataBuf->pData, dataBuf->nAllocSize);
        if (tmp != NULL) {
          dataBuf->pData = tmp;
        } else {  // failed to allocate memory, free already allocated memory and return error code
          tdFreeSBlockRowMerger(pBlkRowMerger);
          taosHashCleanup(pVnodeDataBlockHashList);
          insDestroyBlockArrayList(pVnodeDataBlockList);
          taosMemoryFreeClear(dataBuf->pData);
          taosMemoryFreeClear(blkKeyInfo.pKeyTuple);
          return TSDB_CODE_TSC_OUT_OF_MEMORY;
        }
      }

      if (isRawPayload) {
        sortRemoveDataBlockDupRowsRaw(pOneTableBlock);
      } else {
        if ((code = sortMergeDataBlockDupRows(pOneTableBlock, &blkKeyInfo, &pBlkRowMerger)) != 0) {
          tdFreeSBlockRowMerger(pBlkRowMerger);
          taosHashCleanup(pVnodeDataBlockHashList);
          insDestroyBlockArrayList(pVnodeDataBlockList);
          taosMemoryFreeClear(dataBuf->pData);
          taosMemoryFreeClear(blkKeyInfo.pKeyTuple);
          return code;
        }
        ASSERT(blkKeyInfo.pKeyTuple != NULL && pBlocks->numOfRows > 0);
      }

      // erase the empty space reserved for binary data
      int32_t finalLen =
          trimDataBlock(dataBuf->pData + dataBuf->size, pOneTableBlock, blkKeyInfo.pKeyTuple, isRawPayload);

      dataBuf->size += (finalLen + sizeof(SSubmitBlk));
      assert(dataBuf->size <= dataBuf->nAllocSize);
      dataBuf->numOfTables += 1;
    }

    p = taosHashIterate(pHashObj, p);
    if (p == NULL) {
      break;
    }

    pOneTableBlock = *p;
  }

  // free the table data blocks;
  tdFreeSBlockRowMerger(pBlkRowMerger);
  taosHashCleanup(pVnodeDataBlockHashList);
  taosMemoryFreeClear(blkKeyInfo.pKeyTuple);
  *pVgDataBlocks = pVnodeDataBlockList;
  return TSDB_CODE_SUCCESS;
}

int32_t insAllocateMemForSize(STableDataBlocks* pDataBlock, int32_t allSize) {
  size_t   remain = pDataBlock->nAllocSize - pDataBlock->size;
  uint32_t nAllocSizeOld = pDataBlock->nAllocSize;

  // expand the allocated size
  if (remain < allSize) {
    pDataBlock->nAllocSize = (pDataBlock->size + allSize) * 1.5;

    char* tmp = taosMemoryRealloc(pDataBlock->pData, (size_t)pDataBlock->nAllocSize);
    if (tmp != NULL) {
      pDataBlock->pData = tmp;
      memset(pDataBlock->pData + pDataBlock->size, 0, pDataBlock->nAllocSize - pDataBlock->size);
    } else {
      // do nothing, if allocate more memory failed
      pDataBlock->nAllocSize = nAllocSizeOld;
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t insInitRowBuilder(SRowBuilder* pBuilder, int16_t schemaVer, SParsedDataColInfo* pColInfo) {
  ASSERT(pColInfo->numOfCols > 0 && (pColInfo->numOfBound <= pColInfo->numOfCols));
  tdSRowInit(pBuilder, schemaVer);
  tdSRowSetExtendedInfo(pBuilder, pColInfo->numOfCols, pColInfo->numOfBound, pColInfo->flen, pColInfo->allNullLen,
                        pColInfo->boundNullLen);
  return TSDB_CODE_SUCCESS;
}

static char* tableNameGetPosition(SToken* pToken, char target) {
  bool inEscape = false;
  bool inQuote = false;
  char quotaStr = 0;

  for (uint32_t i = 0; i < pToken->n; ++i) {
    if (*(pToken->z + i) == target && (!inEscape) && (!inQuote)) {
      return pToken->z + i;
    }

    if (*(pToken->z + i) == TS_ESCAPE_CHAR) {
      if (!inQuote) {
        inEscape = !inEscape;
      }
    }

    if (*(pToken->z + i) == '\'' || *(pToken->z + i) == '"') {
      if (!inEscape) {
        if (!inQuote) {
          quotaStr = *(pToken->z + i);
          inQuote = !inQuote;
        } else if (quotaStr == *(pToken->z + i)) {
          inQuote = !inQuote;
        }
      }
    }
  }

  return NULL;
}

int32_t insCreateSName(SName* pName, SToken* pTableName, int32_t acctId, const char* dbName, SMsgBuf* pMsgBuf) {
  const char* msg1 = "name too long";
  const char* msg2 = "invalid database name";
  const char* msg3 = "db is not specified";
  const char* msg4 = "invalid table name";

  int32_t code = TSDB_CODE_SUCCESS;
  char*   p = tableNameGetPosition(pTableName, TS_PATH_DELIMITER[0]);

  if (p != NULL) {  // db has been specified in sql string so we ignore current db path
    assert(*p == TS_PATH_DELIMITER[0]);

    int32_t dbLen = p - pTableName->z;
    if (dbLen <= 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg2);
    }
    char name[TSDB_DB_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, dbLen);
    int32_t actualDbLen = strdequote(name);

    code = tNameSetDbName(pName, acctId, name, actualDbLen);
    if (code != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }

    int32_t tbLen = pTableName->n - dbLen - 1;
    if (tbLen <= 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg4);
    }

    char tbname[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(tbname, p + 1, tbLen);
    /*tbLen = */ strdequote(tbname);

    code = tNameFromString(pName, tbname, T_NAME_TABLE);
    if (code != 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  } else {  // get current DB name first, and then set it into path
    if (pTableName->n >= TSDB_TABLE_NAME_LEN) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }

    assert(pTableName->n < TSDB_TABLE_FNAME_LEN);

    char name[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, pTableName->n);
    strdequote(name);

    if (dbName == NULL) {
      return buildInvalidOperationMsg(pMsgBuf, msg3);
    }

    code = tNameSetDbName(pName, acctId, dbName, strlen(dbName));
    if (code != TSDB_CODE_SUCCESS) {
      code = buildInvalidOperationMsg(pMsgBuf, msg2);
      return code;
    }

    code = tNameFromString(pName, name, T_NAME_TABLE);
    if (code != 0) {
      code = buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  }

  return code;
}

int32_t insFindCol(SToken* pColname, int32_t start, int32_t end, SSchema* pSchema) {
  while (start < end) {
    if (strlen(pSchema[start].name) == pColname->n && strncmp(pColname->z, pSchema[start].name, pColname->n) == 0) {
      return start;
    }
    ++start;
  }
  return -1;
}

void insBuildCreateTbReq(SVCreateTbReq* pTbReq, const char* tname, STag* pTag, int64_t suid, const char* sname,
                         SArray* tagName, uint8_t tagNum) {
  pTbReq->type = TD_CHILD_TABLE;
  pTbReq->name = strdup(tname);
  pTbReq->ctb.suid = suid;
  pTbReq->ctb.tagNum = tagNum;
  if (sname) pTbReq->ctb.stbName = strdup(sname);
  pTbReq->ctb.pTag = (uint8_t*)pTag;
  pTbReq->ctb.tagName = taosArrayDup(tagName);
  pTbReq->ttl = TSDB_DEFAULT_TABLE_TTL;
  pTbReq->commentLen = -1;

  return;
}

int32_t insMemRowAppend(SMsgBuf* pMsgBuf, const void* value, int32_t len, void* param) {
  SMemParam*   pa = (SMemParam*)param;
  SRowBuilder* rb = pa->rb;

  if (value == NULL) {  // it is a null data
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NULL, value, false, pa->toffset, pa->colIdx);
    return TSDB_CODE_SUCCESS;
  }

  if (TSDB_DATA_TYPE_BINARY == pa->schema->type) {
    const char* rowEnd = tdRowEnd(rb->pBuf);
    STR_WITH_SIZE_TO_VARSTR(rowEnd, value, len);
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NORM, rowEnd, false, pa->toffset, pa->colIdx);
  } else if (TSDB_DATA_TYPE_NCHAR == pa->schema->type) {
    // if the converted output len is over than pColumnModel->bytes, return error: 'Argument list too long'
    int32_t     output = 0;
    const char* rowEnd = tdRowEnd(rb->pBuf);
    if (!taosMbsToUcs4(value, len, (TdUcs4*)varDataVal(rowEnd), pa->schema->bytes - VARSTR_HEADER_SIZE, &output)) {
      if (errno == E2BIG) {
        return generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pa->schema->name);
      }
      char buf[512] = {0};
      snprintf(buf, tListLen(buf), "%s", strerror(errno));
      return buildSyntaxErrMsg(pMsgBuf, buf, value);
    }
    varDataSetLen(rowEnd, output);
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NORM, rowEnd, false, pa->toffset, pa->colIdx);
  } else {
    tdAppendColValToRow(rb, pa->schema->colId, pa->schema->type, TD_VTYPE_NORM, value, false, pa->toffset, pa->colIdx);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t insCheckTimestamp(STableDataBlocks* pDataBlocks, const char* start) {
  // once the data block is disordered, we do NOT keep previous timestamp any more
  if (!pDataBlocks->ordered) {
    return TSDB_CODE_SUCCESS;
  }

  TSKEY k = *(TSKEY*)start;
  if (k <= pDataBlocks->prevTS) {
    pDataBlocks->ordered = false;
  }

  pDataBlocks->prevTS = k;
  return TSDB_CODE_SUCCESS;
}

static void buildMsgHeader(STableDataBlocks* src, SVgDataBlocks* blocks) {
  SSubmitReq* submit = (SSubmitReq*)blocks->pData;
  submit->header.vgId = htonl(blocks->vg.vgId);
  submit->header.contLen = htonl(blocks->size);
  submit->length = submit->header.contLen;
  submit->numOfBlocks = htonl(blocks->numOfTables);
  SSubmitBlk* blk = (SSubmitBlk*)(submit + 1);
  int32_t     numOfBlocks = blocks->numOfTables;
  while (numOfBlocks--) {
    int32_t dataLen = blk->dataLen;
    int32_t schemaLen = blk->schemaLen;
    blk->uid = htobe64(blk->uid);
    blk->suid = htobe64(blk->suid);
    blk->sversion = htonl(blk->sversion);
    blk->dataLen = htonl(blk->dataLen);
    blk->schemaLen = htonl(blk->schemaLen);
    blk->numOfRows = htonl(blk->numOfRows);
    blk = (SSubmitBlk*)(blk->data + schemaLen + dataLen);
  }
}

int32_t insBuildOutput(SInsertParseContext* pCxt) {
  size_t numOfVg = taosArrayGetSize(pCxt->pVgDataBlocks);
  pCxt->pOutput->pDataBlocks = taosArrayInit(numOfVg, POINTER_BYTES);
  if (NULL == pCxt->pOutput->pDataBlocks) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  for (size_t i = 0; i < numOfVg; ++i) {
    STableDataBlocks* src = taosArrayGetP(pCxt->pVgDataBlocks, i);
    SVgDataBlocks*    dst = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
    if (NULL == dst) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    taosHashGetDup(pCxt->pVgroupsHashObj, (const char*)&src->vgId, sizeof(src->vgId), &dst->vg);
    dst->numOfTables = src->numOfTables;
    dst->size = src->size;
    TSWAP(dst->pData, src->pData);
    buildMsgHeader(src, dst);
    taosArrayPush(pCxt->pOutput->pDataBlocks, &dst);
  }
  return TSDB_CODE_SUCCESS;
}
