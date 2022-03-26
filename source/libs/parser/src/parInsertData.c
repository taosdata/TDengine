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

#include "parInsertData.h"

#include "catalog.h"
#include "parUtil.h"
#include "querynodes.h"

#define IS_RAW_PAYLOAD(t) \
  (((int)(t)) == PAYLOAD_TYPE_RAW)  // 0: K-V payload for non-prepare insert, 1: rawPayload for prepare insert

typedef struct SBlockKeyTuple {
  TSKEY skey;
  void* payloadAddr;
} SBlockKeyTuple;

typedef struct SBlockKeyInfo {
  int32_t         maxBytesAlloc;
  SBlockKeyTuple* pKeyTuple;
} SBlockKeyInfo;

static int32_t rowDataCompar(const void *lhs, const void *rhs) {
  TSKEY left = *(TSKEY *)lhs;
  TSKEY right = *(TSKEY *)rhs;

  if (left == right) {
    return 0;
  } else {
    return left > right ? 1 : -1;
  }
}

void setBoundColumnInfo(SParsedDataColInfo* pColList, SSchema* pSchema, int32_t numOfCols) {
  pColList->numOfCols = numOfCols;
  pColList->numOfBound = numOfCols;
  pColList->orderStatus = ORDER_STATUS_ORDERED;  // default is ORDERED for non-bound mode
  pColList->boundedColumns = taosMemoryCalloc(pColList->numOfCols, sizeof(int32_t));
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
    pColList->boundedColumns[i] = pSchema[i].colId;
  }
  pColList->allNullLen += pColList->flen;
  pColList->boundNullLen = pColList->allNullLen;  // default set allNullLen
  pColList->extendedVarLen = (uint16_t)(nVar * sizeof(VarDataOffsetT));
}

int32_t schemaIdxCompar(const void *lhs, const void *rhs) {
  uint16_t left = *(uint16_t *)lhs;
  uint16_t right = *(uint16_t *)rhs;

  if (left == right) {
    return 0;
  } else {
    return left > right ? 1 : -1;
  }
}

int32_t boundIdxCompar(const void *lhs, const void *rhs) {
  uint16_t left = *(uint16_t *)POINTER_SHIFT(lhs, sizeof(uint16_t));
  uint16_t right = *(uint16_t *)POINTER_SHIFT(rhs, sizeof(uint16_t));

  if (left == right) {
    return 0;
  } else {
    return left > right ? 1 : -1;
  }
}

void destroyBoundColumnInfo(SParsedDataColInfo* pColList) {
  taosMemoryFreeClear(pColList->boundedColumns);
  taosMemoryFreeClear(pColList->cols);
  taosMemoryFreeClear(pColList->colIdxInfo);
}

static int32_t createDataBlock(size_t defaultSize, int32_t rowSize, int32_t startOffset,
                           const STableMeta* pTableMeta, STableDataBlocks** dataBlocks) {
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

  //Here we keep the tableMeta to avoid it to be remove by other threads.
  dataBuf->pTableMeta = tableMetaDup(pTableMeta);

  SParsedDataColInfo* pColInfo = &dataBuf->boundColumnInfo;
  SSchema* pSchema = getTableColumnSchema(dataBuf->pTableMeta);
  setBoundColumnInfo(pColInfo, pSchema, dataBuf->pTableMeta->tableInfo.numOfColumns);

  dataBuf->ordered  = true;
  dataBuf->prevTS   = INT64_MIN;
  dataBuf->rowSize  = rowSize;
  dataBuf->size     = startOffset;
  dataBuf->vgId     = dataBuf->pTableMeta->vgId;

  assert(defaultSize > 0 && pTableMeta != NULL && dataBuf->pTableMeta != NULL);

  *dataBlocks = dataBuf;
  return TSDB_CODE_SUCCESS;
}

int32_t getDataBlockFromList(SHashObj* pHashList, int64_t id, int32_t size, int32_t startOffset, int32_t rowSize,
    const STableMeta* pTableMeta, STableDataBlocks** dataBlocks, SArray* pBlockList) {
  *dataBlocks = NULL;
  STableDataBlocks** t1 = (STableDataBlocks**)taosHashGet(pHashList, (const char*)&id, sizeof(id));
  if (t1 != NULL) {
    *dataBlocks = *t1;
  }

  if (*dataBlocks == NULL) {
    int32_t ret = createDataBlock((size_t)size, rowSize, startOffset, pTableMeta, dataBlocks);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    taosHashPut(pHashList, (const char*)&id, sizeof(int64_t), (char*)dataBlocks, POINTER_BYTES);
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

static void destroyDataBlock(STableDataBlocks* pDataBlock) {
  if (pDataBlock == NULL) {
    return;
  }

  taosMemoryFreeClear(pDataBlock->pData);
  if (!pDataBlock->cloned) {
    // free the refcount for metermeta
    if (pDataBlock->pTableMeta != NULL) {
      taosMemoryFreeClear(pDataBlock->pTableMeta);
    }

    destroyBoundColumnInfo(&pDataBlock->boundColumnInfo);
  }
  taosMemoryFreeClear(pDataBlock);
}

void destroyBlockArrayList(SArray* pDataBlockList) {
  if (pDataBlockList == NULL) {
    return;
  }

  size_t size = taosArrayGetSize(pDataBlockList);
  for (int32_t i = 0; i < size; i++) {
    void* p = taosArrayGetP(pDataBlockList, i);
    destroyDataBlock(p);
  }

  taosArrayDestroy(pDataBlockList);
}

void destroyBlockHashmap(SHashObj* pDataBlockHash) {
  if (pDataBlockHash == NULL) {
    return;
  }

  void** p1 = taosHashIterate(pDataBlockHash, NULL);
  while (p1) {
    STableDataBlocks* pBlocks = *p1;
    destroyDataBlock(pBlocks);

    p1 = taosHashIterate(pDataBlockHash, p1);
  }

  taosHashCleanup(pDataBlockHash);
}

// data block is disordered, sort it in ascending order
void sortRemoveDataBlockDupRowsRaw(STableDataBlocks *dataBuf) {
  SSubmitBlk *pBlocks = (SSubmitBlk *)dataBuf->pData;

  // size is less than the total size, since duplicated rows may be removed yet.
  assert(pBlocks->numOfRows * dataBuf->rowSize + sizeof(SSubmitBlk) == dataBuf->size);

  if (!dataBuf->ordered) {
    char *pBlockData = pBlocks->data;
    qsort(pBlockData, pBlocks->numOfRows, dataBuf->rowSize, rowDataCompar);

    int32_t i = 0;
    int32_t j = 1;

    // delete rows with timestamp conflicts
    while (j < pBlocks->numOfRows) {
      TSKEY ti = *(TSKEY *)(pBlockData + dataBuf->rowSize * i);
      TSKEY tj = *(TSKEY *)(pBlockData + dataBuf->rowSize * j);

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
int sortRemoveDataBlockDupRows(STableDataBlocks *dataBuf, SBlockKeyInfo *pBlkKeyInfo) {
  SSubmitBlk *pBlocks = (SSubmitBlk *)dataBuf->pData;
  int16_t     nRows = pBlocks->numOfRows;

  // size is less than the total size, since duplicated rows may be removed yet.

  // allocate memory
  size_t nAlloc = nRows * sizeof(SBlockKeyTuple);
  if (pBlkKeyInfo->pKeyTuple == NULL || pBlkKeyInfo->maxBytesAlloc < nAlloc) {
    char *tmp = taosMemoryRealloc(pBlkKeyInfo->pKeyTuple, nAlloc);
    if (tmp == NULL) {
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
    pBlkKeyInfo->pKeyTuple = (SBlockKeyTuple *)tmp;
    pBlkKeyInfo->maxBytesAlloc = (int32_t)nAlloc;
  }
  memset(pBlkKeyInfo->pKeyTuple, 0, nAlloc);

  int32_t         extendedRowSize = getExtendedRowSize(dataBuf);
  SBlockKeyTuple *pBlkKeyTuple = pBlkKeyInfo->pKeyTuple;
  char *          pBlockData = pBlocks->data;
  int             n = 0;
  while (n < nRows) {
    pBlkKeyTuple->skey = TD_ROW_KEY((STSRow *)pBlockData);
    pBlkKeyTuple->payloadAddr = pBlockData;

    // next loop
    pBlockData += extendedRowSize;
    ++pBlkKeyTuple;
    ++n;
  }

  if (!dataBuf->ordered) {
    pBlkKeyTuple = pBlkKeyInfo->pKeyTuple;
    qsort(pBlkKeyTuple, nRows, sizeof(SBlockKeyTuple), rowDataCompar);

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

// Erase the empty space reserved for binary data
static int trimDataBlock(void* pDataBlock, STableDataBlocks* pTableDataBlock, SBlockKeyTuple* blkKeyTuple, int8_t schemaAttached, bool isRawPayload) {
  // TODO: optimize this function, handle the case while binary is not presented
  STableMeta*     pTableMeta = pTableDataBlock->pTableMeta;
  STableComInfo   tinfo = getTableInfo(pTableMeta);
  SSchema*        pSchema = getTableColumnSchema(pTableMeta);

  SSubmitBlk* pBlock = pDataBlock;
  memcpy(pDataBlock, pTableDataBlock->pData, sizeof(SSubmitBlk));
  pDataBlock = (char*)pDataBlock + sizeof(SSubmitBlk);

  int32_t flen = 0;  // original total length of row

  // schema needs to be included into the submit data block
  if (schemaAttached) {
    int32_t numOfCols = getNumOfColumns(pTableDataBlock->pTableMeta);
    for(int32_t j = 0; j < numOfCols; ++j) {
      STColumn* pCol = (STColumn*) pDataBlock;
      pCol->colId = htons(pSchema[j].colId);
      pCol->type  = pSchema[j].type;
      pCol->bytes = htons(pSchema[j].bytes);
      pCol->offset = 0;

      pDataBlock = (char*)pDataBlock + sizeof(STColumn);
      flen += TYPE_BYTES[pSchema[j].type];
    }

    int32_t schemaSize = sizeof(STColumn) * numOfCols;
    pBlock->schemaLen = schemaSize;
  } else {
    if (isRawPayload) {
      for (int32_t j = 0; j < tinfo.numOfColumns; ++j) {
        flen += TYPE_BYTES[pSchema[j].type];
      }
    }
    pBlock->schemaLen = 0;
  }

  char* p = pTableDataBlock->pData + sizeof(SSubmitBlk);
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
      int32_t rowLen = TD_ROW_LEN((STSRow*)pDataBlock);
      pDataBlock = (char*)pDataBlock + rowLen;
      pBlock->dataLen += rowLen;
    }
  } else {
    for (int32_t i = 0; i < numOfRows; ++i) {
      char*      payload = (blkKeyTuple + i)->payloadAddr;
      TDRowLenT  rowTLen = TD_ROW_LEN((STSRow*)payload);
      memcpy(pDataBlock, payload, rowTLen);
      pDataBlock = POINTER_SHIFT(pDataBlock, rowTLen);
      pBlock->dataLen += rowTLen;
    }
  }

  return pBlock->dataLen + pBlock->schemaLen;
}

int32_t mergeTableDataBlocks(SHashObj* pHashObj, int8_t schemaAttached, uint8_t payloadType, SArray** pVgDataBlocks) {
  const int INSERT_HEAD_SIZE = sizeof(SSubmitReq);
  int       code = 0;
  bool      isRawPayload = IS_RAW_PAYLOAD(payloadType);
  SHashObj* pVnodeDataBlockHashList = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  SArray*   pVnodeDataBlockList = taosArrayInit(8, POINTER_BYTES);

  STableDataBlocks** p = taosHashIterate(pHashObj, NULL);
  STableDataBlocks* pOneTableBlock = *p;
  SBlockKeyInfo blkKeyInfo = {0};  // share by pOneTableBlock
  while (pOneTableBlock) {
    SSubmitBlk* pBlocks = (SSubmitBlk*) pOneTableBlock->pData;
    if (pBlocks->numOfRows > 0) {
      STableDataBlocks* dataBuf = NULL;
      int32_t ret = getDataBlockFromList(pVnodeDataBlockHashList, pOneTableBlock->vgId, TSDB_PAYLOAD_SIZE,
          INSERT_HEAD_SIZE, 0, pOneTableBlock->pTableMeta, &dataBuf, pVnodeDataBlockList);
      if (ret != TSDB_CODE_SUCCESS) {
        taosHashCleanup(pVnodeDataBlockHashList);
        destroyBlockArrayList(pVnodeDataBlockList);
        taosMemoryFreeClear(blkKeyInfo.pKeyTuple);
        return ret;
      }

      // the maximum expanded size in byte when a row-wise data is converted to SDataRow format
      int32_t expandSize = isRawPayload ? getRowExpandSize(pOneTableBlock->pTableMeta) : 0;
      int64_t destSize = dataBuf->size + pOneTableBlock->size + pBlocks->numOfRows * expandSize +
                         sizeof(STColumn) * getNumOfColumns(pOneTableBlock->pTableMeta);

      if (dataBuf->nAllocSize < destSize) {
        dataBuf->nAllocSize = (uint32_t)(destSize * 1.5);
        char* tmp = taosMemoryRealloc(dataBuf->pData, dataBuf->nAllocSize);
        if (tmp != NULL) {
          dataBuf->pData = tmp;
        } else {  // failed to allocate memory, free already allocated memory and return error code
          taosHashCleanup(pVnodeDataBlockHashList);
          destroyBlockArrayList(pVnodeDataBlockList);
          taosMemoryFreeClear(dataBuf->pData);
          taosMemoryFreeClear(blkKeyInfo.pKeyTuple);
          return TSDB_CODE_TSC_OUT_OF_MEMORY;
        }
      }

      if (isRawPayload) {
        sortRemoveDataBlockDupRowsRaw(pOneTableBlock);
      } else {
        if ((code = sortRemoveDataBlockDupRows(pOneTableBlock, &blkKeyInfo)) != 0) {
          taosHashCleanup(pVnodeDataBlockHashList);
          destroyBlockArrayList(pVnodeDataBlockList);
          taosMemoryFreeClear(dataBuf->pData);
          taosMemoryFreeClear(blkKeyInfo.pKeyTuple);
          return code;
        }
        ASSERT(blkKeyInfo.pKeyTuple != NULL && pBlocks->numOfRows > 0);
      }

      int32_t len = pBlocks->numOfRows *
                        (isRawPayload ? (pOneTableBlock->rowSize + expandSize) : getExtendedRowSize(pOneTableBlock)) +
                    sizeof(STColumn) * getNumOfColumns(pOneTableBlock->pTableMeta);

      // erase the empty space reserved for binary data
      int32_t finalLen = trimDataBlock(dataBuf->pData + dataBuf->size, pOneTableBlock, blkKeyInfo.pKeyTuple, schemaAttached, isRawPayload);
      assert(finalLen <= len);

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
  taosHashCleanup(pVnodeDataBlockHashList);
  taosMemoryFreeClear(blkKeyInfo.pKeyTuple);
  *pVgDataBlocks = pVnodeDataBlockList;
  return TSDB_CODE_SUCCESS;
}

int32_t allocateMemIfNeed(STableDataBlocks *pDataBlock, int32_t rowSize, int32_t * numOfRows) {
  size_t    remain = pDataBlock->nAllocSize - pDataBlock->size;
  const int factor = 5;
  uint32_t nAllocSizeOld = pDataBlock->nAllocSize;
  
  // expand the allocated size
  if (remain < rowSize * factor) {
    while (remain < rowSize * factor) {
      pDataBlock->nAllocSize = (uint32_t)(pDataBlock->nAllocSize * 1.5);
      remain = pDataBlock->nAllocSize - pDataBlock->size;
    }

    char *tmp = taosMemoryRealloc(pDataBlock->pData, (size_t)pDataBlock->nAllocSize);
    if (tmp != NULL) {
      pDataBlock->pData = tmp;
      memset(pDataBlock->pData + pDataBlock->size, 0, pDataBlock->nAllocSize - pDataBlock->size);
    } else {
      // do nothing, if allocate more memory failed
      pDataBlock->nAllocSize = nAllocSizeOld;
      *numOfRows = (int32_t)(pDataBlock->nAllocSize - pDataBlock->headerSize) / rowSize;
      return TSDB_CODE_TSC_OUT_OF_MEMORY;
    }
  }

  *numOfRows = (int32_t)(pDataBlock->nAllocSize - pDataBlock->headerSize) / rowSize;
  return TSDB_CODE_SUCCESS;
}

int  initRowBuilder(SRowBuilder *pBuilder, int16_t schemaVer, SParsedDataColInfo *pColInfo) {
  ASSERT(pColInfo->numOfCols > 0 && (pColInfo->numOfBound <= pColInfo->numOfCols));
  tdSRowInit(pBuilder, schemaVer);
  tdSRowSetExtendedInfo(pBuilder, pColInfo->numOfCols, pColInfo->numOfBound, pColInfo->flen, pColInfo->allNullLen,
                        pColInfo->boundNullLen);
  return TSDB_CODE_SUCCESS;
}
