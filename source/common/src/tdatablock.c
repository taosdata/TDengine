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

#define _DEFAULT_SOURCE
#include "tdatablock.h"
#include "tcompare.h"
#include "tglobal.h"
#include "tlog.h"
#include "tname.h"

int32_t taosGetFqdnPortFromEp(const char* ep, SEp* pEp) {
  pEp->port = 0;
  strcpy(pEp->fqdn, ep);

  char* temp = strchr(pEp->fqdn, ':');
  if (temp) {
    *temp = 0;
    pEp->port = atoi(temp + 1);
  }

  if (pEp->port == 0) {
    pEp->port = tsServerPort;
  }

  return 0;
}

void addEpIntoEpSet(SEpSet* pEpSet, const char* fqdn, uint16_t port) {
  if (pEpSet == NULL || fqdn == NULL || strlen(fqdn) == 0) {
    return;
  }

  int32_t index = pEpSet->numOfEps;
  tstrncpy(pEpSet->eps[index].fqdn, fqdn, tListLen(pEpSet->eps[index].fqdn));
  pEpSet->eps[index].port = port;
  pEpSet->numOfEps += 1;
}

bool isEpsetEqual(const SEpSet* s1, const SEpSet* s2) {
  if (s1->numOfEps != s2->numOfEps || s1->inUse != s2->inUse) {
    return false;
  }

  for (int32_t i = 0; i < s1->numOfEps; i++) {
    if (s1->eps[i].port != s2->eps[i].port || strncmp(s1->eps[i].fqdn, s2->eps[i].fqdn, TSDB_FQDN_LEN) != 0)
      return false;
  }
  return true;
}

void updateEpSet_s(SCorEpSet* pEpSet, SEpSet* pNewEpSet) {
  taosCorBeginWrite(&pEpSet->version);
  pEpSet->epSet = *pNewEpSet;
  taosCorEndWrite(&pEpSet->version);
}

SEpSet getEpSet_s(SCorEpSet* pEpSet) {
  SEpSet ep = {0};
  taosCorBeginRead(&pEpSet->version);
  ep = pEpSet->epSet;
  taosCorEndRead(&pEpSet->version);

  return ep;
}

int32_t colDataGetLength(const SColumnInfoData* pColumnInfoData, int32_t numOfRows) {
  ASSERT(pColumnInfoData != NULL);
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    return pColumnInfoData->varmeta.length;
  } else {
    if (pColumnInfoData->info.type == TSDB_DATA_TYPE_NULL) {
      return 0;
    } else {
      return pColumnInfoData->info.bytes * numOfRows;
    }
  }
}

int32_t colDataGetFullLength(const SColumnInfoData* pColumnInfoData, int32_t numOfRows) {
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    return pColumnInfoData->varmeta.length + sizeof(int32_t) * numOfRows;
  } else {
    return pColumnInfoData->info.bytes * numOfRows + BitmapLen(numOfRows);
  }
}

void colDataTrim(SColumnInfoData* pColumnInfoData) {
  // TODO
}

int32_t getJsonValueLen(const char* data) {
  int32_t dataLen = 0;
  if (*data == TSDB_DATA_TYPE_NULL) {
    dataLen = CHAR_BYTES;
  } else if (*data == TSDB_DATA_TYPE_NCHAR) {
    dataLen = varDataTLen(data + CHAR_BYTES) + CHAR_BYTES;
  } else if (*data == TSDB_DATA_TYPE_DOUBLE) {
    dataLen = DOUBLE_BYTES + CHAR_BYTES;
  } else if (*data == TSDB_DATA_TYPE_BOOL) {
    dataLen = CHAR_BYTES + CHAR_BYTES;
  } else if (tTagIsJson(data)) {  // json string
    dataLen = ((STag*)(data))->len;
  } else {
    ASSERT(0);
  }
  return dataLen;
}

int32_t colDataAppend(SColumnInfoData* pColumnInfoData, uint32_t currentRow, const char* pData, bool isNull) {
  ASSERT(pColumnInfoData != NULL);

  if (isNull) {
    // There is a placehold for each NULL value of binary or nchar type.
    if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
      pColumnInfoData->varmeta.offset[currentRow] = -1;  // it is a null value of VAR type.
    } else {
      colDataSetNull_f(pColumnInfoData->nullbitmap, currentRow);
    }

    pColumnInfoData->hasNull = true;
    return 0;
  }

  int32_t type = pColumnInfoData->info.type;
  if (IS_VAR_DATA_TYPE(type)) {
    int32_t dataLen = 0;
    if (type == TSDB_DATA_TYPE_JSON) {
      dataLen = getJsonValueLen(pData);
    } else {
      dataLen = varDataTLen(pData);
    }

    SVarColAttr* pAttr = &pColumnInfoData->varmeta;
    if (pAttr->allocLen < pAttr->length + dataLen) {
      uint32_t newSize = pAttr->allocLen;
      if (newSize <= 1) {
        newSize = 8;
      }

      while (newSize < pAttr->length + dataLen) {
        newSize = newSize * 1.5;
      }

      char* buf = taosMemoryRealloc(pColumnInfoData->pData, newSize);
      if (buf == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pColumnInfoData->pData = buf;
      pAttr->allocLen = newSize;
    }

    uint32_t len = pColumnInfoData->varmeta.length;
    pColumnInfoData->varmeta.offset[currentRow] = len;

    memcpy(pColumnInfoData->pData + len, pData, dataLen);
    pColumnInfoData->varmeta.length += dataLen;
  } else {
    memcpy(pColumnInfoData->pData + pColumnInfoData->info.bytes * currentRow, pData, pColumnInfoData->info.bytes);
  }

  return 0;
}

static void doBitmapMerge(SColumnInfoData* pColumnInfoData, int32_t numOfRow1, const SColumnInfoData* pSource,
                          int32_t numOfRow2) {
  if (numOfRow2 <= 0) return;

  uint32_t total = numOfRow1 + numOfRow2;

  uint32_t remindBits = BitPos(numOfRow1);
  uint32_t shiftBits = 8 - remindBits;

  if (remindBits == 0) {  // no need to shift bits of bitmap
    memcpy(pColumnInfoData->nullbitmap + BitmapLen(numOfRow1), pSource->nullbitmap, BitmapLen(numOfRow2));
    return;
  }

  uint8_t* p = (uint8_t*)pSource->nullbitmap;
  pColumnInfoData->nullbitmap[BitmapLen(numOfRow1) - 1] |= (p[0] >> remindBits);  // copy remind bits

  if (BitmapLen(numOfRow1) == BitmapLen(total)) {
    return;
  }

  int32_t len = BitmapLen(numOfRow2);
  int32_t i = 0;

  uint8_t* start = (uint8_t*)&pColumnInfoData->nullbitmap[BitmapLen(numOfRow1)];
  int32_t  overCount = BitmapLen(total) - BitmapLen(numOfRow1);
  while (i < len) {  // size limit of pSource->nullbitmap
    if (i >= 1) {
      start[i - 1] |= (p[i] >> remindBits);  // copy remind bits
    }

    if (i >= overCount) {  // size limit of pColumnInfoData->nullbitmap
      return;
    }

    start[i] |= (p[i] << shiftBits);  // copy shift bits
    i += 1;
  }
}

int32_t colDataMergeCol(SColumnInfoData* pColumnInfoData, uint32_t numOfRow1, uint32_t* capacity,
                        const SColumnInfoData* pSource, uint32_t numOfRow2) {
  ASSERT(pColumnInfoData != NULL && pSource != NULL && pColumnInfoData->info.type == pSource->info.type);
  if (numOfRow2 == 0) {
    return numOfRow1;
  }

  if (pSource->hasNull) {
    pColumnInfoData->hasNull = pSource->hasNull;
  }

  uint32_t finalNumOfRows = numOfRow1 + numOfRow2;
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    // Handle the bitmap
    if (finalNumOfRows > *capacity || (numOfRow1 == 0 && pColumnInfoData->info.bytes != 0)) {
      char* p = taosMemoryRealloc(pColumnInfoData->varmeta.offset, sizeof(int32_t) * (numOfRow1 + numOfRow2));
      if (p == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      *capacity = finalNumOfRows;
      pColumnInfoData->varmeta.offset = (int32_t*)p;
    }

    for (int32_t i = 0; i < numOfRow2; ++i) {
      if (pSource->varmeta.offset[i] == -1) {
        pColumnInfoData->varmeta.offset[i + numOfRow1] = -1;
      } else {
        pColumnInfoData->varmeta.offset[i + numOfRow1] = pSource->varmeta.offset[i] + pColumnInfoData->varmeta.length;
      }
    }

    // copy data
    uint32_t len = pSource->varmeta.length;
    uint32_t oldLen = pColumnInfoData->varmeta.length;
    if (pColumnInfoData->varmeta.allocLen < len + oldLen) {
      char* tmp = taosMemoryRealloc(pColumnInfoData->pData, len + oldLen);
      if (tmp == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pColumnInfoData->pData = tmp;
      pColumnInfoData->varmeta.allocLen = len + oldLen;
    }

    memcpy(pColumnInfoData->pData + oldLen, pSource->pData, len);
    pColumnInfoData->varmeta.length = len + oldLen;
  } else {
    if (finalNumOfRows > *capacity || (numOfRow1 == 0 && pColumnInfoData->info.bytes != 0)) {
      ASSERT(finalNumOfRows * pColumnInfoData->info.bytes);
      char* tmp = taosMemoryRealloc(pColumnInfoData->pData, finalNumOfRows * pColumnInfoData->info.bytes);
      if (tmp == NULL) {
        return TSDB_CODE_VND_OUT_OF_MEMORY;
      }

      pColumnInfoData->pData = tmp;

      if (BitmapLen(numOfRow1) < BitmapLen(finalNumOfRows)) {
        char*    btmp = taosMemoryRealloc(pColumnInfoData->nullbitmap, BitmapLen(finalNumOfRows));
        uint32_t extend = BitmapLen(finalNumOfRows) - BitmapLen(numOfRow1);
        memset(btmp + BitmapLen(numOfRow1), 0, extend);

        pColumnInfoData->nullbitmap = btmp;
      }

      *capacity = finalNumOfRows;
    }

    doBitmapMerge(pColumnInfoData, numOfRow1, pSource, numOfRow2);

    if (pSource->pData) {
      int32_t offset = pColumnInfoData->info.bytes * numOfRow1;
      memcpy(pColumnInfoData->pData + offset, pSource->pData, pSource->info.bytes * numOfRow2);
    }
  }

  return numOfRow1 + numOfRow2;
}

int32_t colDataAssign(SColumnInfoData* pColumnInfoData, const SColumnInfoData* pSource, int32_t numOfRows,
                      const SDataBlockInfo* pBlockInfo) {
  ASSERT(pColumnInfoData != NULL && pSource != NULL && pColumnInfoData->info.type == pSource->info.type);
  if (numOfRows <= 0) {
    return numOfRows;
  }

  if (pBlockInfo != NULL) {
    ASSERT(pBlockInfo->capacity >= numOfRows);
  }

  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    memcpy(pColumnInfoData->varmeta.offset, pSource->varmeta.offset, sizeof(int32_t) * numOfRows);
    if (pColumnInfoData->varmeta.allocLen < pSource->varmeta.length) {
      char* tmp = taosMemoryRealloc(pColumnInfoData->pData, pSource->varmeta.length);
      if (tmp == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pColumnInfoData->pData = tmp;
      pColumnInfoData->varmeta.allocLen = pSource->varmeta.length;
    }

    pColumnInfoData->varmeta.length = pSource->varmeta.length;
    memcpy(pColumnInfoData->pData, pSource->pData, pSource->varmeta.length);
  } else {
    memcpy(pColumnInfoData->nullbitmap, pSource->nullbitmap, BitmapLen(numOfRows));
    if (pSource->pData) {
      memcpy(pColumnInfoData->pData, pSource->pData, pSource->info.bytes * numOfRows);
    }
  }

  pColumnInfoData->hasNull = pSource->hasNull;
  pColumnInfoData->info = pSource->info;
  return 0;
}

size_t blockDataGetNumOfCols(const SSDataBlock* pBlock) { return taosArrayGetSize(pBlock->pDataBlock); }

size_t blockDataGetNumOfRows(const SSDataBlock* pBlock) { return pBlock->info.rows; }

int32_t blockDataUpdateTsWindow(SSDataBlock* pDataBlock, int32_t tsColumnIndex) {
  if (pDataBlock == NULL || pDataBlock->info.rows <= 0) {
    return 0;
  }

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  if (numOfCols <= 0) {
    return -1;
  }

  int32_t index = (tsColumnIndex == -1) ? 0 : tsColumnIndex;

  SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, index);
  if (pColInfoData->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
    return 0;
  }

  TSKEY skey = *(TSKEY*)colDataGetData(pColInfoData, 0);
  TSKEY ekey = *(TSKEY*)colDataGetData(pColInfoData, (pDataBlock->info.rows - 1));

  pDataBlock->info.window.skey = TMIN(skey, ekey);
  pDataBlock->info.window.ekey = TMAX(skey, ekey);

  return 0;
}

int32_t blockDataMerge(SSDataBlock* pDest, const SSDataBlock* pSrc) {
  assert(pSrc != NULL && pDest != NULL);
  int32_t capacity = pDest->info.capacity;

  size_t numOfCols = taosArrayGetSize(pDest->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol2 = taosArrayGet(pDest->pDataBlock, i);
    SColumnInfoData* pCol1 = taosArrayGet(pSrc->pDataBlock, i);

    capacity = pDest->info.capacity;
    colDataMergeCol(pCol2, pDest->info.rows, &capacity, pCol1, pSrc->info.rows);
  }

  pDest->info.capacity = capacity;
  pDest->info.rows += pSrc->info.rows;
  return TSDB_CODE_SUCCESS;
}

size_t blockDataGetSize(const SSDataBlock* pBlock) {
  assert(pBlock != NULL);

  size_t total = 0;
  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    total += colDataGetFullLength(pColInfoData, pBlock->info.rows);
  }

  return total;
}

// the number of tuples can be fit in one page.
// Actual data rows pluses the corresponding meta data must fit in one memory buffer of the given page size.
int32_t blockDataSplitRows(SSDataBlock* pBlock, bool hasVarCol, int32_t startIndex, int32_t* stopIndex,
                           int32_t pageSize) {
  ASSERT(pBlock != NULL && stopIndex != NULL);

  size_t  numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  int32_t numOfRows = pBlock->info.rows;

  int32_t bitmapChar = 1;

  size_t headerSize = sizeof(int32_t);
  size_t colHeaderSize = sizeof(int32_t) * numOfCols;
  size_t payloadSize = pageSize - (headerSize + colHeaderSize);

  // TODO speedup by checking if the whole page can fit in firstly.
  if (!hasVarCol) {
    size_t  rowSize = blockDataGetRowSize(pBlock);
    int32_t capacity = payloadSize / (rowSize + numOfCols * bitmapChar / 8.0);
    ASSERT(capacity > 0);

    *stopIndex = startIndex + capacity - 1;
    if (*stopIndex >= numOfRows) {
      *stopIndex = numOfRows - 1;
    }

    return TSDB_CODE_SUCCESS;
  }
  // iterate the rows that can be fit in this buffer page
  int32_t size = (headerSize + colHeaderSize);
  for (int32_t j = startIndex; j < numOfRows; ++j) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfoData = TARRAY_GET_ELEM(pBlock->pDataBlock, i);
      if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
        if (pColInfoData->varmeta.offset[j] != -1) {
          char* p = colDataGetData(pColInfoData, j);
          size += varDataTLen(p);
        }

        size += sizeof(pColInfoData->varmeta.offset[0]);
      } else {
        size += pColInfoData->info.bytes;

        if (((j - startIndex) & 0x07) == 0) {
          size += 1;  // the space for null bitmap
        }
      }
    }

    if (size > pageSize) {  // pageSize must be able to hold one row
      *stopIndex = j - 1;
      ASSERT(*stopIndex >= startIndex);

      return TSDB_CODE_SUCCESS;
    }
  }

  // all fit in
  *stopIndex = numOfRows - 1;
  return TSDB_CODE_SUCCESS;
}

SSDataBlock* blockDataExtractBlock(SSDataBlock* pBlock, int32_t startIndex, int32_t rowCount) {
  if (pBlock == NULL || startIndex < 0 || rowCount > pBlock->info.rows || rowCount + startIndex > pBlock->info.rows) {
    return NULL;
  }

  SSDataBlock* pDst = createDataBlock();
  if (pDst == NULL) {
    return NULL;
  }

  pDst->info = pBlock->info;
  pDst->info.rows = 0;
  pDst->info.capacity = 0;
  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData  colInfo = {0};
    SColumnInfoData* pSrcCol = taosArrayGet(pBlock->pDataBlock, i);
    colInfo.info = pSrcCol->info;
    blockDataAppendColInfo(pDst, &colInfo);
  }

  blockDataEnsureCapacity(pDst, rowCount);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
    SColumnInfoData* pDstCol = taosArrayGet(pDst->pDataBlock, i);

    for (int32_t j = startIndex; j < (startIndex + rowCount); ++j) {
      bool isNull = false;
      if (pBlock->pBlockAgg == NULL) {
        isNull = colDataIsNull_s(pColData, j);
      } else {
        isNull = colDataIsNull(pColData, pBlock->info.rows, j, pBlock->pBlockAgg[i]);
      }

      char* p = colDataGetData(pColData, j);
      colDataAppend(pDstCol, j - startIndex, p, isNull);
    }
  }

  pDst->info.rows = rowCount;
  return pDst;
}

/**
 *
 * +------------------+---------------------------------------------+
 * |the number of rows|                    column #1                |
 * |    (4 bytes)     |------------+-----------------------+--------+
 * |                  | null bitmap| column length(4bytes) | values |
 * +------------------+------------+-----------------------+--------+
 * @param buf
 * @param pBlock
 * @return
 */
int32_t blockDataToBuf(char* buf, const SSDataBlock* pBlock) {
  ASSERT(pBlock != NULL);

  // write the number of rows
  *(uint32_t*)buf = pBlock->info.rows;

  size_t  numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  int32_t numOfRows = pBlock->info.rows;

  char* pStart = buf + sizeof(uint32_t);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      memcpy(pStart, pCol->varmeta.offset, numOfRows * sizeof(int32_t));
      pStart += numOfRows * sizeof(int32_t);
    } else {
      memcpy(pStart, pCol->nullbitmap, BitmapLen(numOfRows));
      pStart += BitmapLen(pBlock->info.rows);
    }

    uint32_t dataSize = colDataGetLength(pCol, numOfRows);

    *(int32_t*)pStart = dataSize;
    pStart += sizeof(int32_t);

    memcpy(pStart, pCol->pData, dataSize);
    pStart += dataSize;
  }

  return 0;
}

int32_t blockDataFromBuf(SSDataBlock* pBlock, const char* buf) {
  int32_t numOfRows = *(int32_t*) buf;
  blockDataEnsureCapacity(pBlock, numOfRows);

  pBlock->info.rows = numOfRows;
  size_t      numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  const char* pStart = buf + sizeof(uint32_t);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);

    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      size_t metaSize = pBlock->info.rows * sizeof(int32_t);
      char*  tmp = taosMemoryRealloc(pCol->varmeta.offset, metaSize);  // preview calloc is too small
      if (tmp == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      pCol->varmeta.offset = (int32_t*)tmp;
      memcpy(pCol->varmeta.offset, pStart, metaSize);
      pStart += metaSize;
    } else {
      memcpy(pCol->nullbitmap, pStart, BitmapLen(pBlock->info.rows));
      pStart += BitmapLen(pBlock->info.rows);
    }

    int32_t colLength = *(int32_t*)pStart;
    pStart += sizeof(int32_t);

    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      if (pCol->varmeta.allocLen < colLength) {
        char* tmp = taosMemoryRealloc(pCol->pData, colLength);
        if (tmp == NULL) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }

        pCol->pData = tmp;
        pCol->varmeta.allocLen = colLength;
      }

      pCol->varmeta.length = colLength;
      ASSERT(pCol->varmeta.length <= pCol->varmeta.allocLen);
    }

    memcpy(pCol->pData, pStart, colLength);
    pStart += colLength;
  }

  return TSDB_CODE_SUCCESS;
}

// todo remove this
int32_t blockDataFromBuf1(SSDataBlock* pBlock, const char* buf, size_t capacity) {
  pBlock->info.rows = *(int32_t*)buf;
  pBlock->info.groupId = *(uint64_t*)(buf + sizeof(int32_t));

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  const char* pStart = buf + sizeof(uint32_t) + sizeof(uint64_t);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    pCol->hasNull = true;

    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      size_t metaSize = capacity * sizeof(int32_t);
      memcpy(pCol->varmeta.offset, pStart, metaSize);
      pStart += metaSize;
    } else {
      memcpy(pCol->nullbitmap, pStart, BitmapLen(capacity));
      pStart += BitmapLen(capacity);
    }

    int32_t colLength = *(int32_t*)pStart;
    pStart += sizeof(int32_t);

    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      if (pCol->varmeta.allocLen < colLength) {
        char* tmp = taosMemoryRealloc(pCol->pData, colLength);
        if (tmp == NULL) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }

        pCol->pData = tmp;
        pCol->varmeta.allocLen = colLength;
      }

      pCol->varmeta.length = colLength;
      ASSERT(pCol->varmeta.length <= pCol->varmeta.allocLen);
    }

    memcpy(pCol->pData, pStart, colLength);
    pStart += pCol->info.bytes * capacity;
  }

  return TSDB_CODE_SUCCESS;
}

size_t blockDataGetRowSize(SSDataBlock* pBlock) {
  ASSERT(pBlock != NULL);
  if (pBlock->info.rowSize == 0) {
    size_t rowSize = 0;

    size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
      rowSize += pColInfo->info.bytes;
    }

    pBlock->info.rowSize = rowSize;
  }

  return pBlock->info.rowSize;
}

/**
 * @refitem blockDataToBuf for the meta size
 * @param pBlock
 * @return
 */
size_t blockDataGetSerialMetaSize(uint32_t numOfCols) {
  // | total rows/total length | block group id | column schema | each column length |
  return sizeof(int32_t) + sizeof(uint64_t) + numOfCols * (sizeof(int16_t) + sizeof(int32_t)) +
         numOfCols * sizeof(int32_t);
}

double blockDataGetSerialRowSize(const SSDataBlock* pBlock) {
  ASSERT(pBlock != NULL);
  double rowSize = 0;

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
    rowSize += pColInfo->info.bytes;

    if (IS_VAR_DATA_TYPE(pColInfo->info.type)) {
      rowSize += sizeof(int32_t);
    } else {
      rowSize += 1 / 8.0;  // one bit for each record
    }
  }

  return rowSize;
}

typedef struct SSDataBlockSortHelper {
  SArray*      orderInfo;  // SArray<SBlockOrderInfo>
  SSDataBlock* pDataBlock;
} SSDataBlockSortHelper;

int32_t dataBlockCompar(const void* p1, const void* p2, const void* param) {
  const SSDataBlockSortHelper* pHelper = (const SSDataBlockSortHelper*)param;

  SSDataBlock* pDataBlock = pHelper->pDataBlock;

  int32_t left = *(int32_t*)p1;
  int32_t right = *(int32_t*)p2;

  SArray* pInfo = pHelper->orderInfo;

  for (int32_t i = 0; i < pInfo->size; ++i) {
    SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(pInfo, i);
    SColumnInfoData* pColInfoData = pOrder->pColData;  // TARRAY_GET_ELEM(pDataBlock->pDataBlock, pOrder->colIndex);

    if (pColInfoData->hasNull) {
      bool leftNull = colDataIsNull(pColInfoData, pDataBlock->info.rows, left, NULL);
      bool rightNull = colDataIsNull(pColInfoData, pDataBlock->info.rows, right, NULL);
      if (leftNull && rightNull) {
        continue;  // continue to next slot
      }

      if (rightNull) {
        return pOrder->nullFirst ? 1 : -1;
      }

      if (leftNull) {
        return pOrder->nullFirst ? -1 : 1;
      }
    }

    void* left1 = colDataGetData(pColInfoData, left);
    void* right1 = colDataGetData(pColInfoData, right);
    if (pColInfoData->info.type == TSDB_DATA_TYPE_JSON) {
      if (tTagIsJson(left1) || tTagIsJson(right1)) {
        terrno = TSDB_CODE_QRY_JSON_NOT_SUPPORT_ERROR;
        return 0;
      }
    }
    __compar_fn_t fn = getKeyComparFunc(pColInfoData->info.type, pOrder->order);

    int ret = fn(left1, right1);
    if (ret == 0) {
      continue;
    } else {
      return ret;
    }
  }

  return 0;
}

static int32_t doAssignOneTuple(SColumnInfoData* pDstCols, int32_t numOfRows, const SSDataBlock* pSrcBlock,
                                int32_t tupleIndex) {
  int32_t code = 0;
  size_t  numOfCols = taosArrayGetSize(pSrcBlock->pDataBlock);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDst = &pDstCols[i];
    SColumnInfoData* pSrc = taosArrayGet(pSrcBlock->pDataBlock, i);

    if (pSrc->hasNull && colDataIsNull(pSrc, pSrcBlock->info.rows, tupleIndex, pSrcBlock->pBlockAgg[i])) {
      code = colDataAppend(pDst, numOfRows, NULL, true);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else {
      char* p = colDataGetData(pSrc, tupleIndex);
      code = colDataAppend(pDst, numOfRows, p, false);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t blockDataAssign(SColumnInfoData* pCols, const SSDataBlock* pDataBlock, const int32_t* index) {
#if 0
  for (int32_t i = 0; i < pDataBlock->info.rows; ++i) {
    int32_t code = doAssignOneTuple(pCols, i, pDataBlock, index[i]);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
#else
  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDst = &pCols[i];
    SColumnInfoData* pSrc = taosArrayGet(pDataBlock->pDataBlock, i);

    if (IS_VAR_DATA_TYPE(pSrc->info.type)) {
      memcpy(pDst->pData, pSrc->pData, pSrc->varmeta.length);
      pDst->varmeta.length = pSrc->varmeta.length;

      for (int32_t j = 0; j < pDataBlock->info.rows; ++j) {
        pDst->varmeta.offset[j] = pSrc->varmeta.offset[index[j]];
      }
    } else {
      for (int32_t j = 0; j < pDataBlock->info.rows; ++j) {
        if (colDataIsNull_f(pSrc->nullbitmap, index[j])) {
          colDataSetNull_f(pDst->nullbitmap, j);
          continue;
        }
        memcpy(pDst->pData + j * pDst->info.bytes, pSrc->pData + index[j] * pDst->info.bytes, pDst->info.bytes);
      }
    }
  }
#endif
  return TSDB_CODE_SUCCESS;
}

static SColumnInfoData* createHelpColInfoData(const SSDataBlock* pDataBlock) {
  int32_t rows = pDataBlock->info.rows;
  size_t  numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);

  SColumnInfoData* pCols = taosMemoryCalloc(numOfCols, sizeof(SColumnInfoData));
  if (pCols == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, i);
    pCols[i].info = pColInfoData->info;

    if (IS_VAR_DATA_TYPE(pCols[i].info.type)) {
      pCols[i].varmeta.offset = taosMemoryCalloc(rows, sizeof(int32_t));
      pCols[i].pData = taosMemoryCalloc(1, pColInfoData->varmeta.length);

      pCols[i].varmeta.length = pColInfoData->varmeta.length;
      pCols[i].varmeta.allocLen = pCols[i].varmeta.length;
    } else {
      pCols[i].nullbitmap = taosMemoryCalloc(1, BitmapLen(rows));
      pCols[i].pData = taosMemoryCalloc(rows, pCols[i].info.bytes);
    }
  }

  return pCols;
}

static void copyBackToBlock(SSDataBlock* pDataBlock, SColumnInfoData* pCols) {
  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, i);
    pColInfoData->info = pCols[i].info;

    if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
      taosMemoryFreeClear(pColInfoData->varmeta.offset);
      pColInfoData->varmeta = pCols[i].varmeta;
    } else {
      taosMemoryFreeClear(pColInfoData->nullbitmap);
      pColInfoData->nullbitmap = pCols[i].nullbitmap;
    }

    taosMemoryFreeClear(pColInfoData->pData);
    pColInfoData->pData = pCols[i].pData;
  }

  taosMemoryFreeClear(pCols);
}

static int32_t* createTupleIndex(size_t rows) {
  int32_t* index = taosMemoryCalloc(rows, sizeof(int32_t));
  if (index == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < rows; ++i) {
    index[i] = i;
  }

  return index;
}

static void destroyTupleIndex(int32_t* index) { taosMemoryFreeClear(index); }

int32_t blockDataSort(SSDataBlock* pDataBlock, SArray* pOrderInfo) {
  ASSERT(pDataBlock != NULL && pOrderInfo != NULL);
  if (pDataBlock->info.rows <= 1) {
    return TSDB_CODE_SUCCESS;
  }

  // Allocate the additional buffer.
  uint32_t rows = pDataBlock->info.rows;

  bool sortColumnHasNull = false;
  bool varTypeSort = false;

  for (int32_t i = 0; i < taosArrayGetSize(pOrderInfo); ++i) {
    SBlockOrderInfo* pInfo = taosArrayGet(pOrderInfo, i);

    SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, pInfo->slotId);
    if (pColInfoData->hasNull) {
      sortColumnHasNull = true;
    }

    if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
      varTypeSort = true;
    }
  }

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);

  if (taosArrayGetSize(pOrderInfo) == 1 && (!sortColumnHasNull)) {
    if (numOfCols == 1) {
      if (!varTypeSort) {
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, 0);
        SBlockOrderInfo* pOrder = taosArrayGet(pOrderInfo, 0);

        int64_t p0 = taosGetTimestampUs();

        __compar_fn_t fn = getKeyComparFunc(pColInfoData->info.type, pOrder->order);
        taosSort(pColInfoData->pData, pDataBlock->info.rows, pColInfoData->info.bytes, fn);

        int64_t p1 = taosGetTimestampUs();
        uDebug("blockDataSort easy cost:%" PRId64 ", rows:%d\n", p1 - p0, pDataBlock->info.rows);

        return TSDB_CODE_SUCCESS;
      } else {  // var data type
      }
    } else if (numOfCols == 2) {
    }
  }

  int32_t* index = createTupleIndex(rows);
  if (index == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  int64_t p0 = taosGetTimestampUs();

  SSDataBlockSortHelper helper = {.pDataBlock = pDataBlock, .orderInfo = pOrderInfo};
  for (int32_t i = 0; i < taosArrayGetSize(helper.orderInfo); ++i) {
    struct SBlockOrderInfo* pInfo = taosArrayGet(helper.orderInfo, i);
    pInfo->pColData = taosArrayGet(pDataBlock->pDataBlock, pInfo->slotId);
  }

  terrno = 0;
  taosqsort(index, rows, sizeof(int32_t), &helper, dataBlockCompar);
  if (terrno) return terrno;

  int64_t p1 = taosGetTimestampUs();

  SColumnInfoData* pCols = createHelpColInfoData(pDataBlock);
  if (pCols == NULL) {
    destroyTupleIndex(index);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  int64_t p2 = taosGetTimestampUs();

  blockDataAssign(pCols, pDataBlock, index);

  int64_t p3 = taosGetTimestampUs();

  copyBackToBlock(pDataBlock, pCols);
  int64_t p4 = taosGetTimestampUs();

  uDebug("blockDataSort complex sort:%" PRId64 ", create:%" PRId64 ", assign:%" PRId64 ", copyback:%" PRId64
         ", rows:%d\n",
         p1 - p0, p2 - p1, p3 - p2, p4 - p3, rows);
  destroyTupleIndex(index);

  return TSDB_CODE_SUCCESS;
}

typedef struct SHelper {
  int32_t index;
  union {
    char*   pData;
    int64_t i64;
    double  d64;
  };
} SHelper;

SHelper* createTupleIndex_rv(int32_t numOfRows, SArray* pOrderInfo, SSDataBlock* pBlock) {
  int32_t sortValLengthPerRow = 0;
  int32_t numOfCols = taosArrayGetSize(pOrderInfo);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SBlockOrderInfo* pInfo = taosArrayGet(pOrderInfo, i);
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, pInfo->slotId);
    pInfo->pColData = pColInfo;
    sortValLengthPerRow += pColInfo->info.bytes;
  }

  size_t len = sortValLengthPerRow * pBlock->info.rows;

  char*    buf = taosMemoryCalloc(1, len);
  SHelper* phelper = taosMemoryCalloc(numOfRows, sizeof(SHelper));
  for (int32_t i = 0; i < numOfRows; ++i) {
    phelper[i].index = i;
    phelper[i].pData = buf + sortValLengthPerRow * i;
  }

  int32_t offset = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SBlockOrderInfo* pInfo = taosArrayGet(pOrderInfo, i);
    for (int32_t j = 0; j < numOfRows; ++j) {
      phelper[j].i64 = *(int32_t*)pInfo->pColData->pData + pInfo->pColData->info.bytes * j;
      //      memcpy(phelper[j].pData + offset, pInfo->pColData->pData + pInfo->pColData->info.bytes * j,
      //      pInfo->pColData->info.bytes);
    }

    offset += pInfo->pColData->info.bytes;
  }

  return phelper;
}

int32_t dataBlockCompar_rv(const void* p1, const void* p2, const void* param) {
  const SSDataBlockSortHelper* pHelper = (const SSDataBlockSortHelper*)param;

  //  SSDataBlock* pDataBlock = pHelper->pDataBlock;

  SHelper* left = (SHelper*)p1;
  SHelper* right = (SHelper*)p2;

  SArray* pInfo = pHelper->orderInfo;

  int32_t offset = 0;
  //  for(int32_t i = 0; i < pInfo->size; ++i) {
  //    SBlockOrderInfo* pOrder = TARRAY_GET_ELEM(pInfo, 0);
  //    SColumnInfoData* pColInfoData = pOrder->pColData;//TARRAY_GET_ELEM(pDataBlock->pDataBlock, pOrder->colIndex);

  //    if (pColInfoData->hasNull) {
  //      bool leftNull  = colDataIsNull(pColInfoData, pDataBlock->info.rows, left, pDataBlock->pBlockAgg);
  //      bool rightNull = colDataIsNull(pColInfoData, pDataBlock->info.rows, right, pDataBlock->pBlockAgg);
  //      if (leftNull && rightNull) {
  //        continue; // continue to next slot
  //      }
  //
  //      if (rightNull) {
  //        return pHelper->nullFirst? 1:-1;
  //      }
  //
  //      if (leftNull) {
  //        return pHelper->nullFirst? -1:1;
  //      }
  //    }

  //    void* left1  = colDataGetData(pColInfoData, left);
  //    void* right1 = colDataGetData(pColInfoData, right);

  //    switch(pColInfoData->info.type) {
  //      case TSDB_DATA_TYPE_INT: {
  int32_t leftx = *(int32_t*)left->pData;    //*(int32_t*)(left->pData + offset);
  int32_t rightx = *(int32_t*)right->pData;  //*(int32_t*)(right->pData + offset);

  //        offset += pColInfoData->info.bytes;
  if (leftx == rightx) {
    //          break;
    return 0;
  } else {
    //          if (pOrder->order == TSDB_ORDER_ASC) {
    return (leftx < rightx) ? -1 : 1;
    //          } else {
    //            return (leftx < rightx)? 1:-1;
    //          }
  }
  //      }
  //      default:
  //        assert(0);
  //    }
  //  }

  return 0;
}

int32_t varColSort(SColumnInfoData* pColumnInfoData, SBlockOrderInfo* pOrder) { return 0; }

int32_t blockDataSort_rv(SSDataBlock* pDataBlock, SArray* pOrderInfo, bool nullFirst) {
  // Allocate the additional buffer.
  int64_t p0 = taosGetTimestampUs();

  SSDataBlockSortHelper helper = {.pDataBlock = pDataBlock, .orderInfo = pOrderInfo};

  uint32_t rows = pDataBlock->info.rows;
  SHelper* index = createTupleIndex_rv(rows, helper.orderInfo, pDataBlock);
  if (index == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  taosqsort(index, rows, sizeof(SHelper), &helper, dataBlockCompar_rv);

  int64_t          p1 = taosGetTimestampUs();
  SColumnInfoData* pCols = createHelpColInfoData(pDataBlock);
  if (pCols == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  int64_t p2 = taosGetTimestampUs();

  //  int32_t code = blockDataAssign(pCols, pDataBlock, index);
  //  if (code != TSDB_CODE_SUCCESS) {
  //    terrno = code;
  //    return code;
  //  }

  int64_t p3 = taosGetTimestampUs();

  copyBackToBlock(pDataBlock, pCols);
  int64_t p4 = taosGetTimestampUs();

  printf("sort:%" PRId64 ", create:%" PRId64 ", assign:%" PRId64 ", copyback:%" PRId64 ", rows:%d\n", p1 - p0, p2 - p1,
         p3 - p2, p4 - p3, rows);
  //  destroyTupleIndex(index);
  return 0;
}

void blockDataCleanup(SSDataBlock* pDataBlock) {
  pDataBlock->info.rows = 0;
  pDataBlock->info.groupId = 0;

  pDataBlock->info.window.ekey = 0;
  pDataBlock->info.window.skey = 0;

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    colInfoDataCleanup(p, pDataBlock->info.capacity);
  }
}

static int32_t doEnsureCapacity(SColumnInfoData* pColumn, const SDataBlockInfo* pBlockInfo, uint32_t numOfRows) {
  ASSERT(numOfRows > 0 && pBlockInfo->capacity >= pBlockInfo->rows);
  if (numOfRows < pBlockInfo->capacity) {
    return TSDB_CODE_SUCCESS;
  }

  // todo temp disable it
  //  ASSERT(pColumn->info.bytes != 0);

  int32_t existedRows = pBlockInfo->rows;

  if (IS_VAR_DATA_TYPE(pColumn->info.type)) {
    char* tmp = taosMemoryRealloc(pColumn->varmeta.offset, sizeof(int32_t) * numOfRows);
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    pColumn->varmeta.offset = (int32_t*)tmp;
    memset(&pColumn->varmeta.offset[existedRows], 0, sizeof(int32_t) * (numOfRows - existedRows));
  } else {
    char* tmp = taosMemoryRealloc(pColumn->nullbitmap, BitmapLen(numOfRows));
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    int32_t oldLen = BitmapLen(existedRows);
    pColumn->nullbitmap = tmp;
    memset(&pColumn->nullbitmap[oldLen], 0, BitmapLen(numOfRows) - oldLen);

    if (pColumn->info.type == TSDB_DATA_TYPE_NULL) {
      return TSDB_CODE_SUCCESS;
    }

    assert(pColumn->info.bytes);
    tmp = taosMemoryRealloc(pColumn->pData, numOfRows * pColumn->info.bytes);
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    memset(tmp + pColumn->info.bytes * existedRows, 0, pColumn->info.bytes * (numOfRows - existedRows));
    pColumn->pData = tmp;
  }

  return TSDB_CODE_SUCCESS;
}

void colInfoDataCleanup(SColumnInfoData* pColumn, uint32_t numOfRows) {
  if (IS_VAR_DATA_TYPE(pColumn->info.type)) {
    pColumn->varmeta.length = 0;
    if (pColumn->varmeta.offset > 0) {
      memset(pColumn->varmeta.offset, 0, sizeof(int32_t) * numOfRows);
    }
  } else {
    if (pColumn->nullbitmap != NULL) {
      memset(pColumn->nullbitmap, 0, BitmapLen(numOfRows));
      if (pColumn->pData != NULL) {
        memset(pColumn->pData, 0, pColumn->info.bytes * numOfRows);
      }
    }
  }
}

int32_t colInfoDataEnsureCapacity(SColumnInfoData* pColumn, uint32_t numOfRows) {
  SDataBlockInfo info = {0};
  return doEnsureCapacity(pColumn, &info, numOfRows);
}

int32_t blockDataEnsureCapacity(SSDataBlock* pDataBlock, uint32_t numOfRows) {
  int32_t code = 0;
  if (numOfRows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (pDataBlock->info.capacity < numOfRows) {
    pDataBlock->info.capacity = numOfRows;
  }

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    code = doEnsureCapacity(p, &pDataBlock->info, numOfRows);
    if (code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void blockDataFreeRes(SSDataBlock* pBlock) {
  int32_t numOfOutput = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData* pColInfoData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
    colDataDestroy(pColInfoData);
  }

  taosArrayDestroy(pBlock->pDataBlock);
  taosMemoryFreeClear(pBlock->pBlockAgg);
  memset(&pBlock->info, 0, sizeof(SDataBlockInfo));
}

void* blockDataDestroy(SSDataBlock* pBlock) {
  if (pBlock == NULL) {
    return NULL;
  }

  blockDataFreeRes(pBlock);
  taosMemoryFreeClear(pBlock);
  return NULL;
}

int32_t assignOneDataBlock(SSDataBlock* dst, const SSDataBlock* src) {
  ASSERT(src != NULL);

  dst->info = src->info;
  dst->info.rows = 0;
  dst->info.capacity = 0;

  size_t numOfCols = taosArrayGetSize(src->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(src->pDataBlock, i);
    SColumnInfoData  colInfo = {.hasNull = true, .info = p->info};
    blockDataAppendColInfo(dst, &colInfo);
  }

  int32_t code = blockDataEnsureCapacity(dst, src->info.rows);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return -1;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDst = taosArrayGet(dst->pDataBlock, i);
    SColumnInfoData* pSrc = taosArrayGet(src->pDataBlock, i);
    if (pSrc->pData == NULL && (!IS_VAR_DATA_TYPE(pSrc->info.type))) {
      continue;
    }

    colDataAssign(pDst, pSrc, src->info.rows, &src->info);
  }

  dst->info.rows = src->info.rows;
  dst->info.capacity = src->info.rows;
  return 0;
}

int32_t copyDataBlock(SSDataBlock* dst, const SSDataBlock* src) {
  ASSERT(src != NULL && dst != NULL);

  blockDataCleanup(dst);
  int32_t code = blockDataEnsureCapacity(dst, src->info.rows);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }

  size_t numOfCols = taosArrayGetSize(src->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDst = taosArrayGet(dst->pDataBlock, i);
    SColumnInfoData* pSrc = taosArrayGet(src->pDataBlock, i);
    if (pSrc->pData == NULL) {
      continue;
    }

    colDataAssign(pDst, pSrc, src->info.rows, &src->info);
  }

  dst->info.rows = src->info.rows;
  dst->info.window = src->info.window;
  dst->info.type = src->info.type;
  return TSDB_CODE_SUCCESS;
}

SSDataBlock* createOneDataBlock(const SSDataBlock* pDataBlock, bool copyData) {
  if (pDataBlock == NULL) {
    return NULL;
  }

  SSDataBlock* pBlock = createDataBlock();
  pBlock->info = pDataBlock->info;
  pBlock->info.rows = 0;
  pBlock->info.capacity = 0;

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    SColumnInfoData  colInfo = {.hasNull = true, .info = p->info};
    blockDataAppendColInfo(pBlock, &colInfo);
  }

  if (copyData) {
    int32_t code = blockDataEnsureCapacity(pBlock, pDataBlock->info.rows);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      blockDataDestroy(pBlock);
      return NULL;
    }

    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, i);
      SColumnInfoData* pSrc = taosArrayGet(pDataBlock->pDataBlock, i);
      if (pSrc->pData == NULL) {
        continue;
      }

      colDataAssign(pDst, pSrc, pDataBlock->info.rows, &pDataBlock->info);
    }

    pBlock->info.rows = pDataBlock->info.rows;
    pBlock->info.capacity = pDataBlock->info.rows;
  }

  return pBlock;
}

SSDataBlock* createDataBlock() {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }

  pBlock->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));
  if (pBlock->pDataBlock == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pBlock);
  }

  return pBlock;
}

int32_t blockDataAppendColInfo(SSDataBlock* pBlock, SColumnInfoData* pColInfoData) {
  ASSERT(pBlock != NULL && pColInfoData != NULL);
  if (pBlock->pDataBlock == NULL) {
    pBlock->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));
    if (pBlock->pDataBlock == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return terrno;
    }
  }

  void* p = taosArrayPush(pBlock->pDataBlock, pColInfoData);
  if (p == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  // todo disable it temporarily
  //  ASSERT(pColInfoData->info.type != 0);
  if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
    pBlock->info.hasVarCol = true;
  }

  pBlock->info.rowSize += pColInfoData->info.bytes;
  return TSDB_CODE_SUCCESS;
}

SColumnInfoData createColumnInfoData(int16_t type, int32_t bytes, int16_t colId) {
  SColumnInfoData col = {.hasNull = true};
  col.info.colId = colId;
  col.info.type = type;
  col.info.bytes = bytes;

  return col;
}

SColumnInfoData* bdGetColumnInfoData(const SSDataBlock* pBlock, int32_t index) {
  ASSERT(pBlock != NULL);
  if (index >= taosArrayGetSize(pBlock->pDataBlock)) {
    return NULL;
  }

  return taosArrayGet(pBlock->pDataBlock, index);
}

size_t blockDataGetCapacityInRow(const SSDataBlock* pBlock, size_t pageSize) {
  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  int32_t payloadSize = pageSize - blockDataGetSerialMetaSize(numOfCols);
  int32_t rowSize = pBlock->info.rowSize;
  int32_t nRows = payloadSize / rowSize;

  // the true value must be less than the value of nRows
  int32_t additional = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      additional += nRows * sizeof(int32_t);
    } else {
      additional += BitmapLen(nRows);
    }
  }

  int32_t newRows = (payloadSize - additional) / rowSize;
  ASSERT(newRows <= nRows && newRows >= 1);

  return newRows;
}

void colDataDestroy(SColumnInfoData* pColData) {
  if (IS_VAR_DATA_TYPE(pColData->info.type)) {
    taosMemoryFreeClear(pColData->varmeta.offset);
  } else {
    taosMemoryFreeClear(pColData->nullbitmap);
  }

  taosMemoryFreeClear(pColData->pData);
}

static void doShiftBitmap(char* nullBitmap, size_t n, size_t total) {
  int32_t len = BitmapLen(total);

  int32_t newLen = BitmapLen(total - n);
  if (n % 8 == 0) {
    memmove(nullBitmap, nullBitmap + n / 8, newLen);
  } else {
    int32_t  tail = n % 8;
    int32_t  i = 0;
    uint8_t* p = (uint8_t*)nullBitmap;

    if (n < 8) {
      while (i < len) {
        uint8_t v = p[i];  // source bitmap value
        p[i] = (v << tail);

        if (i < len - 1) {
          uint8_t next = p[i + 1];
          p[i] |= (next >> (8 - tail));
        }

        i += 1;
      }
    } else if (n > 8) {
      int32_t gap = len - newLen;
      while (i < newLen) {
        uint8_t v = p[i + gap];
        p[i] = (v << tail);

        if (i < newLen - 1) {
          uint8_t next = p[i + gap + 1];
          p[i] |= (next >> (8 - tail));
        }

        i += 1;
      }
    }
  }
}

static int32_t colDataMoveVarData(SColumnInfoData* pColInfoData, size_t start, size_t end) {
  int32_t dataOffset = -1;
  int32_t dataLen = 0;
  int32_t beigin = start;
  while (beigin < end) {
    int32_t offset = pColInfoData->varmeta.offset[beigin];
    if (offset == -1) {
      beigin++;
      continue;
    }
    if (start != 0) {
      pColInfoData->varmeta.offset[beigin] = dataLen;
    }
    char* data = pColInfoData->pData + offset;
    if (dataOffset == -1) dataOffset = offset;  // mark the begin of data
    int32_t type = pColInfoData->info.type;
    if (type == TSDB_DATA_TYPE_JSON) {
      dataLen += getJsonValueLen(data);
    } else {
      dataLen += varDataTLen(data);
    }
    beigin++;
  }

  if (dataOffset > 0) {
    memmove(pColInfoData->pData, pColInfoData->pData + dataOffset, dataLen);
  }

  memmove(pColInfoData->varmeta.offset, &pColInfoData->varmeta.offset[start], (end - start) * sizeof(int32_t));
  return dataLen;
}

static void colDataTrimFirstNRows(SColumnInfoData* pColInfoData, size_t n, size_t total) {
  if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
    pColInfoData->varmeta.length = colDataMoveVarData(pColInfoData, n, total);
    memset(&pColInfoData->varmeta.offset[total - n], 0, n);
  } else {
    int32_t bytes = pColInfoData->info.bytes;
    memmove(pColInfoData->pData, ((char*)pColInfoData->pData + n * bytes), (total - n) * bytes);
    doShiftBitmap(pColInfoData->nullbitmap, n, total);
  }
}

int32_t blockDataTrimFirstNRows(SSDataBlock* pBlock, size_t n) {
  if (n == 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (pBlock->info.rows <= n) {
    blockDataCleanup(pBlock);
  } else {
    size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
      colDataTrimFirstNRows(pColInfoData, n, pBlock->info.rows);
    }

    pBlock->info.rows -= n;
  }
  return TSDB_CODE_SUCCESS;
}

static void colDataKeepFirstNRows(SColumnInfoData* pColInfoData, size_t n, size_t total) {
  if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
    pColInfoData->varmeta.length = colDataMoveVarData(pColInfoData, 0, n);
    memset(&pColInfoData->varmeta.offset[n], 0, total - n);
  }
}

int32_t blockDataKeepFirstNRows(SSDataBlock* pBlock, size_t n) {
  if (n == 0) {
    blockDataCleanup(pBlock);
    return TSDB_CODE_SUCCESS;
  }

  if (pBlock->info.rows <= n) {
    return TSDB_CODE_SUCCESS;
  } else {
    size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
      colDataKeepFirstNRows(pColInfoData, n, pBlock->info.rows);
    }

    pBlock->info.rows = n;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tEncodeDataBlock(void** buf, const SSDataBlock* pBlock) {
  int64_t tbUid = pBlock->info.uid;
  int16_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  int16_t hasVarCol = pBlock->info.hasVarCol;
  int32_t rows = pBlock->info.rows;
  int32_t sz = taosArrayGetSize(pBlock->pDataBlock);

  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, tbUid);
  tlen += taosEncodeFixedI16(buf, numOfCols);
  tlen += taosEncodeFixedI16(buf, hasVarCol);
  tlen += taosEncodeFixedI32(buf, rows);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SColumnInfoData* pColData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
    tlen += taosEncodeFixedI16(buf, pColData->info.colId);
    tlen += taosEncodeFixedI16(buf, pColData->info.type);
    tlen += taosEncodeFixedI32(buf, pColData->info.bytes);
    tlen += taosEncodeFixedBool(buf, pColData->hasNull);

    if (IS_VAR_DATA_TYPE(pColData->info.type)) {
      tlen += taosEncodeBinary(buf, pColData->varmeta.offset, sizeof(int32_t) * rows);
    } else {
      tlen += taosEncodeBinary(buf, pColData->nullbitmap, BitmapLen(rows));
    }

    int32_t len = colDataGetLength(pColData, rows);
    tlen += taosEncodeFixedI32(buf, len);

    tlen += taosEncodeBinary(buf, pColData->pData, len);
  }
  return tlen;
}

void* tDecodeDataBlock(const void* buf, SSDataBlock* pBlock) {
  int32_t sz;

  int16_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  buf = taosDecodeFixedU64(buf, &pBlock->info.uid);
  buf = taosDecodeFixedI16(buf, &numOfCols);
  buf = taosDecodeFixedI16(buf, &pBlock->info.hasVarCol);
  buf = taosDecodeFixedI32(buf, &pBlock->info.rows);
  buf = taosDecodeFixedI32(buf, &sz);
  pBlock->pDataBlock = taosArrayInit(sz, sizeof(SColumnInfoData));
  for (int32_t i = 0; i < sz; i++) {
    SColumnInfoData data = {0};
    buf = taosDecodeFixedI16(buf, &data.info.colId);
    buf = taosDecodeFixedI16(buf, &data.info.type);
    buf = taosDecodeFixedI32(buf, &data.info.bytes);
    buf = taosDecodeFixedBool(buf, &data.hasNull);

    if (IS_VAR_DATA_TYPE(data.info.type)) {
      buf = taosDecodeBinary(buf, (void**)&data.varmeta.offset, pBlock->info.rows * sizeof(int32_t));
      data.varmeta.length = pBlock->info.rows * sizeof(int32_t);
      data.varmeta.allocLen = data.varmeta.length;
    } else {
      buf = taosDecodeBinary(buf, (void**)&data.nullbitmap, BitmapLen(pBlock->info.rows));
    }

    int32_t len = 0;
    buf = taosDecodeFixedI32(buf, &len);
    buf = taosDecodeBinary(buf, (void**)&data.pData, len);
    taosArrayPush(pBlock->pDataBlock, &data);
  }
  return (void*)buf;
}

int32_t tEncodeDataBlocks(void** buf, const SArray* blocks) {
  int32_t tlen = 0;
  int32_t sz = taosArrayGetSize(blocks);
  tlen += taosEncodeFixedI32(buf, sz);

  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pBlock = taosArrayGet(blocks, i);
    tlen += tEncodeDataBlock(buf, pBlock);
  }

  return tlen;
}

void* tDecodeDataBlocks(const void* buf, SArray** blocks) {
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);

  *blocks = taosArrayInit(sz, sizeof(SSDataBlock));
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock pBlock = {0};
    buf = tDecodeDataBlock(buf, &pBlock);
    taosArrayPush(*blocks, &pBlock);
  }
  return (void*)buf;
}

static char* formatTimestamp(char* buf, int64_t val, int precision) {
  time_t  tt;
  int32_t ms = 0;
  if (precision == TSDB_TIME_PRECISION_NANO) {
    tt = (time_t)(val / 1000000000);
    ms = val % 1000000000;
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    tt = (time_t)(val / 1000000);
    ms = val % 1000000;
  } else {
    tt = (time_t)(val / 1000);
    ms = val % 1000;
  }

  /* comment out as it make testcases like select_with_tags.sim fail.
    but in windows, this may cause the call to localtime crash if tt < 0,
    need to find a better solution.
    if (tt < 0) {
      tt = 0;
    }
    */

  if (tt <= 0 && ms < 0) {
    tt--;
    if (precision == TSDB_TIME_PRECISION_NANO) {
      ms += 1000000000;
    } else if (precision == TSDB_TIME_PRECISION_MICRO) {
      ms += 1000000;
    } else {
      ms += 1000;
    }
  }
  struct tm ptm = {0};
  taosLocalTime(&tt, &ptm);
  size_t     pos = strftime(buf, 35, "%Y-%m-%d %H:%M:%S", &ptm);

  if (precision == TSDB_TIME_PRECISION_NANO) {
    sprintf(buf + pos, ".%09d", ms);
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    sprintf(buf + pos, ".%06d", ms);
  } else {
    sprintf(buf + pos, ".%03d", ms);
  }

  return buf;
}

void blockDebugShowDataBlock(SSDataBlock* pBlock, const char* flag) {
  SArray* dataBlocks = taosArrayInit(1, sizeof(SSDataBlock));
  taosArrayPush(dataBlocks, pBlock);
  blockDebugShowDataBlocks(dataBlocks, flag);
  taosArrayDestroy(dataBlocks);
}

void blockDebugShowDataBlocks(const SArray* dataBlocks, const char* flag) {
  char    pBuf[128] = {0};
  int32_t sz = taosArrayGetSize(dataBlocks);
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGet(dataBlocks, i);
    size_t       numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);

    int32_t rows = pDataBlock->info.rows;
    printf("%s |block type %d |child id %d|group id %zX\n", flag, (int32_t)pDataBlock->info.type,
           pDataBlock->info.childId, pDataBlock->info.groupId);
    for (int32_t j = 0; j < rows; j++) {
      printf("%s |", flag);
      for (int32_t k = 0; k < numOfCols; k++) {
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
        void*            var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
        if (colDataIsNull(pColInfoData, rows, j, NULL)) {
          printf(" %15s |", "NULL");
          continue;
        }
        switch (pColInfoData->info.type) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            formatTimestamp(pBuf, *(uint64_t*)var, TSDB_TIME_PRECISION_MILLI);
            printf(" %25s |", pBuf);
            break;
          case TSDB_DATA_TYPE_INT:
            printf(" %15d |", *(int32_t*)var);
            break;
          case TSDB_DATA_TYPE_UINT:
            printf(" %15u |", *(uint32_t*)var);
            break;
          case TSDB_DATA_TYPE_BIGINT:
            printf(" %15ld |", *(int64_t*)var);
            break;
          case TSDB_DATA_TYPE_UBIGINT:
            printf(" %15lu |", *(uint64_t*)var);
            break;
          case TSDB_DATA_TYPE_FLOAT:
            printf(" %15f |", *(float*)var);
            break;
          case TSDB_DATA_TYPE_DOUBLE:
            printf(" %15lf |", *(double*)var);
            break;
        }
      }
      printf("\n");
    }
  }
}

// for debug
char* dumpBlockData(SSDataBlock* pDataBlock, const char* flag, char** pDataBuf) {
  int32_t size = 2048;
  *pDataBuf = taosMemoryCalloc(size, 1);
  char*   dumpBuf = *pDataBuf;
  char    pBuf[128] = {0};
  int32_t colNum = taosArrayGetSize(pDataBlock->pDataBlock);
  int32_t rows = pDataBlock->info.rows;
  int32_t len = 0;
  len += snprintf(dumpBuf + len, size - len, "===stream===%s |block type %d |child id %d|group id:%" PRIu64 "| uid:%ld|\n", flag,
                  (int32_t)pDataBlock->info.type, pDataBlock->info.childId, pDataBlock->info.groupId,
                  pDataBlock->info.uid);
  if (len >= size - 1) return dumpBuf;

  for (int32_t j = 0; j < rows; j++) {
    len += snprintf(dumpBuf + len, size - len, "%s |", flag);
    if (len >= size - 1) return dumpBuf;

    for (int32_t k = 0; k < colNum; k++) {
      SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
      void*            var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
      if (colDataIsNull(pColInfoData, rows, j, NULL) || !pColInfoData->pData) {
        len += snprintf(dumpBuf + len, size - len, " %15s |", "NULL");
        if (len >= size - 1) return dumpBuf;
        continue;
      }
      switch (pColInfoData->info.type) {
        case TSDB_DATA_TYPE_TIMESTAMP:
          formatTimestamp(pBuf, *(uint64_t*)var, TSDB_TIME_PRECISION_MILLI);
          len += snprintf(dumpBuf + len, size - len, " %25s |", pBuf);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_INT:
          len += snprintf(dumpBuf + len, size - len, " %15d |", *(int32_t*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_UINT:
          len += snprintf(dumpBuf + len, size - len, " %15u |", *(uint32_t*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_BIGINT:
          len += snprintf(dumpBuf + len, size - len, " %15ld |", *(int64_t*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_UBIGINT:
          len += snprintf(dumpBuf + len, size - len, " %15lu |", *(uint64_t*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_FLOAT:
          len += snprintf(dumpBuf + len, size - len, " %15f |", *(float*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          len += snprintf(dumpBuf + len, size - len, " %15lf |", *(double*)var);
          if (len >= size - 1) return dumpBuf;
          break;
      }
    }
    len += snprintf(dumpBuf + len, size - len, "\n");
    if (len >= size - 1) return dumpBuf;
  }
  len += snprintf(dumpBuf + len, size - len, "%s |end\n", flag);
  return dumpBuf;
}

/**
 * @brief TODO: Assume that the final generated result it less than 3M
 *
 * @param pReq
 * @param pDataBlocks
 * @param vgId
 * @param suid  // TODO: check with Liao whether suid response is reasonable
 *
 * TODO: colId should be set
 */
int32_t buildSubmitReqFromDataBlock(SSubmitReq** pReq, const SArray* pDataBlocks, STSchema* pTSchema, int32_t vgId,
                                    tb_uid_t suid) {
  int32_t sz = taosArrayGetSize(pDataBlocks);
  int32_t bufSize = sizeof(SSubmitReq);
  for (int32_t i = 0; i < sz; ++i) {
    SDataBlockInfo* pBlkInfo = &((SSDataBlock*)taosArrayGet(pDataBlocks, i))->info;

    int32_t numOfCols = taosArrayGetSize(pDataBlocks);
    bufSize += pBlkInfo->rows * (TD_ROW_HEAD_LEN + pBlkInfo->rowSize + BitmapLen(numOfCols));
    bufSize += sizeof(SSubmitBlk);
  }

  *pReq = taosMemoryCalloc(1, bufSize);
  if (!(*pReq)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }
  void* pDataBuf = *pReq;

  int32_t     msgLen = sizeof(SSubmitReq);
  int32_t     numOfBlks = 0;
  SRowBuilder rb = {0};
  tdSRowInit(&rb, pTSchema->version);

  for (int32_t i = 0; i < sz; ++i) {
    SSDataBlock* pDataBlock = taosArrayGet(pDataBlocks, i);
    int32_t      colNum = taosArrayGetSize(pDataBlock->pDataBlock);
    int32_t      rows = pDataBlock->info.rows;
    //    int32_t      rowSize = pDataBlock->info.rowSize;
    //    int64_t      groupId = pDataBlock->info.groupId;

    if (colNum <= 1) {
      // invalid if only with TS col
      continue;
    }

    if (rb.nCols != colNum) {
      tdSRowSetTpInfo(&rb, colNum, pTSchema->flen);
    }

    SSubmitBlk* pSubmitBlk = POINTER_SHIFT(pDataBuf, msgLen);
    pSubmitBlk->suid = suid;
    pSubmitBlk->uid = pDataBlock->info.groupId;
    pSubmitBlk->numOfRows = rows;
    pSubmitBlk->sversion = pTSchema->version;

    msgLen += sizeof(SSubmitBlk);
    int32_t dataLen = 0;
    for (int32_t j = 0; j < rows; ++j) {                     // iterate by row
      tdSRowResetBuf(&rb, POINTER_SHIFT(pDataBuf, msgLen));  // set row buf
      bool    isStartKey = false;
      int32_t offset = 0;
      for (int32_t k = 0; k < colNum; ++k) {  // iterate by column
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
        STColumn*        pCol = &pTSchema->columns[k];
        void*            var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
        switch (pColInfoData->info.type) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            if (!isStartKey) {
              isStartKey = true;
              tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID, TSDB_DATA_TYPE_TIMESTAMP, TD_VTYPE_NORM, var, true,
                                  offset, k);

            } else {
              tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, TSDB_DATA_TYPE_TIMESTAMP, TD_VTYPE_NORM, var,
                                  true, offset, k);
            }
            break;
          case TSDB_DATA_TYPE_NCHAR: {
            tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, TSDB_DATA_TYPE_NCHAR, TD_VTYPE_NORM, var, true,
                                offset, k);
            break;
          }
          case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
            tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, TSDB_DATA_TYPE_VARCHAR, TD_VTYPE_NORM, var, true,
                                offset, k);
            break;
          }
          case TSDB_DATA_TYPE_VARBINARY:
          case TSDB_DATA_TYPE_DECIMAL:
          case TSDB_DATA_TYPE_BLOB:
          case TSDB_DATA_TYPE_JSON:
          case TSDB_DATA_TYPE_MEDIUMBLOB:
            uError("the column type %" PRIi16 " is defined but not implemented yet", pColInfoData->info.type);
            TASSERT(0);
            break;
          default:
            if (pColInfoData->info.type < TSDB_DATA_TYPE_MAX && pColInfoData->info.type > TSDB_DATA_TYPE_NULL) {
              if (pCol->type == pColInfoData->info.type) {
                tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, pCol->type, TD_VTYPE_NORM, var, true, offset,
                                    k);
              } else {
                char tv[8] = {0};
                if (pColInfoData->info.type == TSDB_DATA_TYPE_FLOAT) {
                  float v = 0;
                  GET_TYPED_DATA(v, float, pColInfoData->info.type, var);
                  SET_TYPED_DATA(&tv, pCol->type, v);
                } else if (pColInfoData->info.type == TSDB_DATA_TYPE_DOUBLE) {
                  double v = 0;
                  GET_TYPED_DATA(v, double, pColInfoData->info.type, var);
                  SET_TYPED_DATA(&tv, pCol->type, v);
                } else if (IS_SIGNED_NUMERIC_TYPE(pColInfoData->info.type)) {
                  int64_t v = 0;
                  GET_TYPED_DATA(v, int64_t, pColInfoData->info.type, var);
                  SET_TYPED_DATA(&tv, pCol->type, v);
                } else {
                  uint64_t v = 0;
                  GET_TYPED_DATA(v, uint64_t, pColInfoData->info.type, var);
                  SET_TYPED_DATA(&tv, pCol->type, v);
                }
                tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, pCol->type, TD_VTYPE_NORM, tv, true, offset,
                                    k);
              }
            } else {
              uError("the column type %" PRIi16 " is undefined\n", pColInfoData->info.type);
              TASSERT(0);
            }
            break;
        }
        offset += TYPE_BYTES[pCol->type];  // sum/avg would convert to int64_t/uint64_t/double during aggregation
      }
      dataLen += TD_ROW_LEN(rb.pBuf);
#ifdef TD_DEBUG_PRINT_ROW
      tdSRowPrint(rb.pBuf, pTSchema, __func__);
#endif
    }

    ++numOfBlks;

    pSubmitBlk->dataLen = dataLen;
    msgLen += pSubmitBlk->dataLen;
  }

  if (numOfBlks > 0) {
    (*pReq)->length = msgLen;

    (*pReq)->header.vgId = htonl(vgId);
    (*pReq)->header.contLen = htonl(msgLen);
    (*pReq)->length = (*pReq)->header.contLen;
    (*pReq)->numOfBlocks = htonl(numOfBlks);
    SSubmitBlk* blk = (SSubmitBlk*)((*pReq) + 1);
    while (numOfBlks--) {
      int32_t dataLen = blk->dataLen;
      blk->uid = htobe64(blk->uid);
      blk->suid = htobe64(blk->suid);
      blk->padding = htonl(blk->padding);
      blk->sversion = htonl(blk->sversion);
      blk->dataLen = htonl(blk->dataLen);
      blk->schemaLen = htonl(blk->schemaLen);
      blk->numOfRows = htons(blk->numOfRows);
      blk = (SSubmitBlk*)(blk->data + dataLen);
    }
  } else {
    // no valid rows
    taosMemoryFreeClear(*pReq);
  }

  return TSDB_CODE_SUCCESS;
}

char* buildCtbNameByGroupId(const char* stbName, uint64_t groupId) {
  ASSERT(stbName[0] != 0);
  SArray* tags = taosArrayInit(0, sizeof(void*));
  SSmlKv* pTag = taosMemoryCalloc(1, sizeof(SSmlKv));
  pTag->key = "group_id";
  pTag->keyLen = strlen(pTag->key);
  pTag->type = TSDB_DATA_TYPE_UBIGINT;
  pTag->u = groupId;
  pTag->length = sizeof(uint64_t);
  taosArrayPush(tags, &pTag);

  void* cname = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN + 1);

  RandTableName rname = {
      .tags = tags,
      .sTableName = stbName,
      .sTableNameLen = strlen(stbName),
      .childTableName = cname,
  };

  buildChildTableName(&rname);

  taosMemoryFree(pTag);
  taosArrayDestroy(tags);

  ASSERT(rname.childTableName && rname.childTableName[0]);
  return rname.childTableName;
}

void blockEncode(const SSDataBlock* pBlock, char* data, int32_t* dataLen, int32_t numOfCols, int8_t needCompress) {
  // todo extract method
  int32_t* actualLen = (int32_t*)data;
  data += sizeof(int32_t);

  uint64_t* groupId = (uint64_t*)data;
  data += sizeof(uint64_t);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

    *((int16_t*)data) = pColInfoData->info.type;
    data += sizeof(int16_t);

    *((int32_t*)data) = pColInfoData->info.bytes;
    data += sizeof(int32_t);
  }

  int32_t* colSizes = (int32_t*)data;
  data += numOfCols * sizeof(int32_t);

  *dataLen = blockDataGetSerialMetaSize(numOfCols);

  int32_t numOfRows = pBlock->info.rows;
  for (int32_t col = 0; col < numOfCols; ++col) {
    SColumnInfoData* pColRes = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, col);

    // copy the null bitmap
    size_t metaSize = 0;
    if (IS_VAR_DATA_TYPE(pColRes->info.type)) {
      metaSize = numOfRows * sizeof(int32_t);
      memcpy(data, pColRes->varmeta.offset, metaSize);
    } else {
      metaSize = BitmapLen(numOfRows);
      memcpy(data, pColRes->nullbitmap, metaSize);
    }

    data += metaSize;
    (*dataLen) += metaSize;

    if (needCompress) {
      colSizes[col] = blockCompressColData(pColRes, numOfRows, data, needCompress);
      data += colSizes[col];
      (*dataLen) += colSizes[col];
    } else {
      colSizes[col] = colDataGetLength(pColRes, numOfRows);
      (*dataLen) += colSizes[col];
      memmove(data, pColRes->pData, colSizes[col]);
      data += colSizes[col];
    }

    colSizes[col] = htonl(colSizes[col]);
  }

  *actualLen = *dataLen;
  *groupId = pBlock->info.groupId;
}

const char* blockDecode(SSDataBlock* pBlock, int32_t numOfCols, int32_t numOfRows, const char* pData) {
  const char* pStart = pData;

  int32_t dataLen = *(int32_t*)pStart;
  pStart += sizeof(int32_t);

  pBlock->info.groupId = *(uint64_t*)pStart;
  pStart += sizeof(uint64_t);

  if (pBlock->pDataBlock == NULL) {
    pBlock->pDataBlock = taosArrayInit(numOfCols, sizeof(SColumnInfoData));
    taosArraySetSize(pBlock->pDataBlock, numOfCols);
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    pColInfoData->info.type = *(int16_t*)pStart;
    pStart += sizeof(int16_t);

    pColInfoData->info.bytes = *(int32_t*)pStart;
    pStart += sizeof(int32_t);

    if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
      pBlock->info.hasVarCol = true;
    }
  }

  blockDataEnsureCapacity(pBlock, numOfRows);

  int32_t* colLen = (int32_t*)pStart;
  pStart += sizeof(int32_t) * numOfCols;

  for (int32_t i = 0; i < numOfCols; ++i) {
    colLen[i] = htonl(colLen[i]);
    ASSERT(colLen[i] >= 0);

    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
      memcpy(pColInfoData->varmeta.offset, pStart, sizeof(int32_t) * numOfRows);
      pStart += sizeof(int32_t) * numOfRows;

      if (colLen[i] > 0 && pColInfoData->varmeta.allocLen < colLen[i]) {
        char* tmp = taosMemoryRealloc(pColInfoData->pData, colLen[i]);
        if (tmp == NULL) {
          return NULL;
        }

        pColInfoData->pData = tmp;
        pColInfoData->varmeta.allocLen = colLen[i];
      }

      pColInfoData->varmeta.length = colLen[i];
    } else {
      memcpy(pColInfoData->nullbitmap, pStart, BitmapLen(numOfRows));
      pStart += BitmapLen(numOfRows);
    }

    if (colLen[i] > 0) {
      memcpy(pColInfoData->pData, pStart, colLen[i]);
    }

    // TODO
    // setting this flag to true temporarily so aggregate function on stable will
    // examine NULL value for non-primary key column
    pColInfoData->hasNull = true;
    pStart += colLen[i];
  }

  pBlock->info.rows = numOfRows;
  ASSERT(pStart - pData == dataLen);
  return pStart;
}

