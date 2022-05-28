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
    int32_t dataLen = varDataTLen(pData);
    if (type == TSDB_DATA_TYPE_JSON) {
      if (*pData == TSDB_DATA_TYPE_NULL) {
        dataLen = 0;
      } else if (*pData == TSDB_DATA_TYPE_NCHAR) {
        dataLen = varDataTLen(pData + CHAR_BYTES);
      } else if (*pData == TSDB_DATA_TYPE_DOUBLE) {
        dataLen = DOUBLE_BYTES;
      } else if (*pData == TSDB_DATA_TYPE_BOOL) {
        dataLen = CHAR_BYTES;
      } else if (*pData == TSDB_DATA_TYPE_JSON) {
        dataLen = kvRowLen(pData + CHAR_BYTES);
      } else {
        ASSERT(0);
      }
      dataLen += CHAR_BYTES;
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

int32_t colDataMergeCol(SColumnInfoData* pColumnInfoData, uint32_t numOfRow1, int32_t* capacity,
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
    if (finalNumOfRows > *capacity) {
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
    if (finalNumOfRows > *capacity) {
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

int32_t colDataAssign(SColumnInfoData* pColumnInfoData, const SColumnInfoData* pSource, int32_t numOfRows) {
  ASSERT(pColumnInfoData != NULL && pSource != NULL && pColumnInfoData->info.type == pSource->info.type);
  if (numOfRows == 0) {
    return numOfRows;
  }

  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    // Handle the bitmap
    char* p = taosMemoryRealloc(pColumnInfoData->varmeta.offset, sizeof(int32_t) * numOfRows);
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    pColumnInfoData->varmeta.offset = (int32_t*)p;
    memcpy(pColumnInfoData->varmeta.offset, pSource->varmeta.offset, sizeof(int32_t) * numOfRows);

    if (pColumnInfoData->varmeta.allocLen < pSource->varmeta.length) {
      char* tmp = taosMemoryRealloc(pColumnInfoData->pData, pSource->varmeta.length);
      if (tmp == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pColumnInfoData->pData = tmp;
      pColumnInfoData->varmeta.allocLen = pSource->varmeta.length;
    }

    memcpy(pColumnInfoData->pData, pSource->pData, pSource->varmeta.length);
    pColumnInfoData->varmeta.length = pSource->varmeta.length;
  } else {
    char* tmp = taosMemoryRealloc(pColumnInfoData->nullbitmap, BitmapLen(numOfRows));
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    pColumnInfoData->nullbitmap = tmp;
    memcpy(pColumnInfoData->nullbitmap, pSource->nullbitmap, BitmapLen(numOfRows));

    if (pSource->pData) {
      int32_t newSize = numOfRows * pColumnInfoData->info.bytes;
      tmp = taosMemoryRealloc(pColumnInfoData->pData, newSize);
      if (tmp == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pColumnInfoData->pData = tmp;
      memcpy(pColumnInfoData->pData, pSource->pData, pSource->info.bytes * numOfRows);
    }
  }

  pColumnInfoData->hasNull = pSource->hasNull;
  pColumnInfoData->info = pSource->info;
  return 0;
}

size_t blockDataGetNumOfCols(const SSDataBlock* pBlock) {
  ASSERT(pBlock && pBlock->info.numOfCols == taosArrayGetSize(pBlock->pDataBlock));
  return pBlock->info.numOfCols;
}

size_t blockDataGetNumOfRows(const SSDataBlock* pBlock) { return pBlock->info.rows; }

int32_t blockDataUpdateTsWindow(SSDataBlock* pDataBlock, int32_t tsColumnIndex) {
  if (pDataBlock == NULL || pDataBlock->info.rows <= 0) {
    return 0;
  }

  if (pDataBlock->info.numOfCols <= 0) {
    return -1;
  }

  int32_t          index = (tsColumnIndex == -1) ? 0 : tsColumnIndex;
  SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, index);
  if (pColInfoData->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
    return 0;
  }

  pDataBlock->info.window.skey = *(TSKEY*)colDataGetData(pColInfoData, 0);
  pDataBlock->info.window.ekey = *(TSKEY*)colDataGetData(pColInfoData, (pDataBlock->info.rows - 1));
  return 0;
}

int32_t blockDataMerge(SSDataBlock* pDest, const SSDataBlock* pSrc) {
  assert(pSrc != NULL && pDest != NULL);
  int32_t capacity = pDest->info.capacity;

  for (int32_t i = 0; i < pDest->info.numOfCols; ++i) {
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

  size_t  total = 0;
  int32_t numOfCols = pBlock->info.numOfCols;

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

  int32_t numOfCols = pBlock->info.numOfCols;
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

  SSDataBlock* pDst = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pDst == NULL) {
    return NULL;
  }

  pDst->info = pBlock->info;

  pDst->info.rows = 0;
  pDst->pDataBlock = taosArrayInit(pBlock->info.numOfCols, sizeof(SColumnInfoData));

  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
    SColumnInfoData  colInfo = {0};
    SColumnInfoData* pSrcCol = taosArrayGet(pBlock->pDataBlock, i);
    colInfo.info = pSrcCol->info;

    if (IS_VAR_DATA_TYPE(pSrcCol->info.type)) {
      SVarColAttr* pAttr = &colInfo.varmeta;
      pAttr->offset = taosMemoryCalloc(rowCount, sizeof(int32_t));
    } else {
      colInfo.nullbitmap = taosMemoryCalloc(1, BitmapLen(rowCount));
      colInfo.pData = taosMemoryCalloc(rowCount, colInfo.info.bytes);
    }

    taosArrayPush(pDst->pDataBlock, &colInfo);
  }

  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
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

  int32_t numOfCols = pBlock->info.numOfCols;
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
  pBlock->info.rows = *(int32_t*)buf;

  int32_t     numOfCols = pBlock->info.numOfCols;
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

int32_t blockDataFromBuf1(SSDataBlock* pBlock, const char* buf, size_t capacity) {
  pBlock->info.rows = *(int32_t*)buf;
  pBlock->info.groupId = *(uint64_t*)(buf + sizeof(int32_t));

  int32_t     numOfCols = pBlock->info.numOfCols;
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

    size_t numOfCols = pBlock->info.numOfCols;
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
size_t blockDataGetSerialMetaSize(const SSDataBlock* pBlock) {
  // | total rows/total length | block group id | each column length |
  return sizeof(int32_t) + sizeof(uint64_t) + pBlock->info.numOfCols * sizeof(int32_t);
}

double blockDataGetSerialRowSize(const SSDataBlock* pBlock) {
  ASSERT(pBlock != NULL);
  double rowSize = 0;

  size_t numOfCols = pBlock->info.numOfCols;
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
  int32_t numOfCols = pSrcBlock->info.numOfCols;

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

static int32_t blockDataAssign(SColumnInfoData* pCols, const SSDataBlock* pDataBlock, int32_t* index) {
#if 0
  for (int32_t i = 0; i < pDataBlock->info.rows; ++i) {
    int32_t code = doAssignOneTuple(pCols, i, pDataBlock, index[i]);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
#else
  for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
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
  int32_t numOfCols = pDataBlock->info.numOfCols;

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
  int32_t numOfCols = pDataBlock->info.numOfCols;

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

  if (taosArrayGetSize(pOrderInfo) == 1 && (!sortColumnHasNull)) {
    if (pDataBlock->info.numOfCols == 1) {
      if (!varTypeSort) {
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, 0);
        SBlockOrderInfo* pOrder = taosArrayGet(pOrderInfo, 0);

        int64_t p0 = taosGetTimestampUs();

        __compar_fn_t fn = getKeyComparFunc(pColInfoData->info.type, pOrder->order);
        qsort(pColInfoData->pData, pDataBlock->info.rows, pColInfoData->info.bytes, fn);

        int64_t p1 = taosGetTimestampUs();
        uDebug("blockDataSort easy cost:%" PRId64 ", rows:%d\n", p1 - p0, pDataBlock->info.rows);

        return TSDB_CODE_SUCCESS;
      } else {  // var data type
      }
    } else if (pDataBlock->info.numOfCols == 2) {
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

  taosqsort(index, rows, sizeof(int32_t), &helper, dataBlockCompar);

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
  for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    colInfoDataCleanup(p, pDataBlock->info.capacity);
  }
}

int32_t colInfoDataEnsureCapacity(SColumnInfoData* pColumn, size_t existRows, uint32_t numOfRows) {
  if (0 == numOfRows || numOfRows <= existRows) {
    return TSDB_CODE_SUCCESS;
  }

  if (IS_VAR_DATA_TYPE(pColumn->info.type)) {
    char* tmp = taosMemoryRealloc(pColumn->varmeta.offset, sizeof(int32_t) * numOfRows);
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    pColumn->varmeta.offset = (int32_t*)tmp;
    memset(&pColumn->varmeta.offset[existRows], 0, sizeof(int32_t) * (numOfRows - existRows));
  } else {
    char* tmp = taosMemoryRealloc(pColumn->nullbitmap, BitmapLen(numOfRows));
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    int32_t oldLen = BitmapLen(existRows);
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

    pColumn->pData = tmp;
  }

  return TSDB_CODE_SUCCESS;
}

void colInfoDataCleanup(SColumnInfoData* pColumn, uint32_t numOfRows) {
  if (IS_VAR_DATA_TYPE(pColumn->info.type)) {
    pColumn->varmeta.length = 0;
  } else {
    if (pColumn->nullbitmap != NULL) {
      memset(pColumn->nullbitmap, 0, BitmapLen(numOfRows));
    }
  }
}

int32_t blockDataEnsureCapacity(SSDataBlock* pDataBlock, uint32_t numOfRows) {
  int32_t code = 0;
  if (numOfRows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  pDataBlock->info.capacity = numOfRows;

  for (int32_t i = 0; i < pDataBlock->info.numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    code = colInfoDataEnsureCapacity(p, pDataBlock->info.rows, numOfRows);
    if (code) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void* blockDataDestroy(SSDataBlock* pBlock) {
  if (pBlock == NULL) {
    return NULL;
  }

  blockDestroyInner(pBlock);
  taosMemoryFreeClear(pBlock);
  return NULL;
}

SSDataBlock* createOneDataBlock(const SSDataBlock* pDataBlock, bool copyData) {
  if (pDataBlock == NULL) {
    return NULL;
  }

  int32_t numOfCols = pDataBlock->info.numOfCols;

  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->pDataBlock = taosArrayInit(numOfCols, sizeof(SColumnInfoData));

  pBlock->info.numOfCols = numOfCols;
  pBlock->info.hasVarCol = pDataBlock->info.hasVarCol;
  pBlock->info.rowSize = pDataBlock->info.rows;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData  colInfo = {0};
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    colInfo.info = p->info;
    taosArrayPush(pBlock->pDataBlock, &colInfo);
  }

  if (copyData) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, i);
      SColumnInfoData* pSrc = taosArrayGet(pDataBlock->pDataBlock, i);

      int32_t code = colInfoDataEnsureCapacity(pDst, 0, pDataBlock->info.rows);
      if (code != TSDB_CODE_SUCCESS) {
        return NULL;
      }

      colDataAssign(pDst, pSrc, pDataBlock->info.rows);
    }

    pBlock->info.rows = pDataBlock->info.rows;
    pBlock->info.capacity = pDataBlock->info.rows;
  }

  return pBlock;
}

size_t blockDataGetCapacityInRow(const SSDataBlock* pBlock, size_t pageSize) {
  int32_t payloadSize = pageSize - blockDataGetSerialMetaSize(pBlock);

  int32_t rowSize = pBlock->info.rowSize;

  int32_t nRows = payloadSize / rowSize;

  // the true value must be less than the value of nRows
  int32_t additional = 0;
  for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
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
    taosMemoryFree(pColData->varmeta.offset);
  } else {
    taosMemoryFree(pColData->nullbitmap);
  }

  taosMemoryFree(pColData->pData);
}

static void doShiftBitmap(char* nullBitmap, size_t n, size_t total) {
  int32_t len = BitmapLen(total);

  int32_t newLen = BitmapLen(total - n);
  if (n % 8 == 0) {
    memmove(nullBitmap, nullBitmap + n / 8, newLen);
  } else {
    int32_t tail = n % 8;
    int32_t i = 0;

    uint8_t* p = (uint8_t*)nullBitmap;
    while (i < len) {
      uint8_t v = p[i];

      p[i] = 0;
      p[i] = (v << tail);

      if (i < len - 1) {
        uint8_t next = p[i + 1];
        p[i] |= (next >> (8 - tail));
      }

      i += 1;
    }
  }
}

static void colDataTrimFirstNRows(SColumnInfoData* pColInfoData, size_t n, size_t total) {
  if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
    memmove(pColInfoData->varmeta.offset, &pColInfoData->varmeta.offset[n], (total - n) * sizeof(int32_t));
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
    for (int32_t i = 0; i < pBlock->info.numOfCols; ++i) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
      colDataTrimFirstNRows(pColInfoData, n, pBlock->info.rows);
    }

    pBlock->info.rows -= n;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tEncodeDataBlock(void** buf, const SSDataBlock* pBlock) {
  int64_t tbUid = pBlock->info.uid;
  int16_t numOfCols = pBlock->info.numOfCols;
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

  buf = taosDecodeFixedI64(buf, &pBlock->info.uid);
  buf = taosDecodeFixedI16(buf, &pBlock->info.numOfCols);
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

#ifdef WINDOWS
  if (tt < 0) tt = 0;
#endif
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

  struct tm* ptm = taosLocalTime(&tt, NULL);
  size_t     pos = strftime(buf, 35, "%Y-%m-%d %H:%M:%S", ptm);

  if (precision == TSDB_TIME_PRECISION_NANO) {
    sprintf(buf + pos, ".%09d", ms);
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    sprintf(buf + pos, ".%06d", ms);
  } else {
    sprintf(buf + pos, ".%03d", ms);
  }

  return buf;
}

void blockDebugShowData(const SArray* dataBlocks) {
  char    pBuf[128];
  int32_t sz = taosArrayGetSize(dataBlocks);
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGet(dataBlocks, i);
    int32_t      colNum = pDataBlock->info.numOfCols;
    int32_t      rows = pDataBlock->info.rows;
    for (int32_t j = 0; j < rows; j++) {
      printf("|");
      for (int32_t k = 0; k < colNum; k++) {
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
        void*            var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
        if (pColInfoData->hasNull) {
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
          case TSDB_DATA_TYPE_DOUBLE:
            printf(" %15f |", *(double*)var);
            break;
        }
      }
      printf("\n");
    }
  }
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
int32_t buildSubmitReqFromDataBlock(SSubmitReq** pReq, const SArray* pDataBlocks, STSchema* pTSchema, int32_t vgId, tb_uid_t suid) {
  int32_t sz = taosArrayGetSize(pDataBlocks);
  int32_t bufSize = sizeof(SSubmitReq);
  for (int32_t i = 0; i < sz; ++i) {
    SDataBlockInfo* pBlkInfo = &((SSDataBlock*)taosArrayGet(pDataBlocks, i))->info;
    bufSize += pBlkInfo->rows * (TD_ROW_HEAD_LEN + pBlkInfo->rowSize + BitmapLen(pBlkInfo->numOfCols));
    bufSize += sizeof(SSubmitBlk);
  }

  ASSERT(bufSize < 3 * 1024 * 1024);

  *pReq = taosMemoryCalloc(1, bufSize);
  if (!(*pReq)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }
  void* pDataBuf = *pReq;

  int32_t     msgLen = sizeof(SSubmitReq);
  int32_t     numOfBlks = 0;
  SRowBuilder rb = {0};
  tdSRowInit(&rb, pTSchema->version);  // TODO: use the latest version

  for (int32_t i = 0; i < sz; ++i) {
    SSDataBlock* pDataBlock = taosArrayGet(pDataBlocks, i);
    int32_t      colNum = pDataBlock->info.numOfCols;
    int32_t      rows = pDataBlock->info.rows;
    int32_t      rowSize = pDataBlock->info.rowSize;
    int64_t      groupId = pDataBlock->info.groupId;

    if (rb.nCols != colNum) {
      tdSRowSetTpInfo(&rb, colNum, pTSchema->flen);
    }

    SSubmitBlk* pSubmitBlk = POINTER_SHIFT(pDataBuf, msgLen);
    pSubmitBlk->suid = suid;
    pSubmitBlk->uid = pDataBlock->info.groupId;
    pSubmitBlk->numOfRows = rows;

    ++numOfBlks;

    msgLen += sizeof(SSubmitBlk);
    int32_t dataLen = 0;
    for (int32_t j = 0; j < rows; ++j) {                     // iterate by row
      tdSRowResetBuf(&rb, POINTER_SHIFT(pDataBuf, msgLen));  // set row buf
      printf("|");
      bool isStartKey = false;
      int32_t offset = 0;
      for (int32_t k = 0; k < colNum; ++k) {  // iterate by column
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
        void*            var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
        switch (pColInfoData->info.type) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            if (!isStartKey) {
              isStartKey = true;
              tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID, TSDB_DATA_TYPE_TIMESTAMP, TD_VTYPE_NORM, var, true,
                                  offset, k);

            } else {
              tdAppendColValToRow(&rb, 2, TSDB_DATA_TYPE_TIMESTAMP, TD_VTYPE_NORM, var, true, offset, k);
            }
            break;
          case TSDB_DATA_TYPE_NCHAR: {
            tdAppendColValToRow(&rb, 2, TSDB_DATA_TYPE_NCHAR, TD_VTYPE_NORM, var, true, offset, k);
            break;
          }
          case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
            tdAppendColValToRow(&rb, 2, TSDB_DATA_TYPE_VARCHAR, TD_VTYPE_NORM, var, true, offset, k);
            break;
          }
          case TSDB_DATA_TYPE_VARBINARY:
          case TSDB_DATA_TYPE_DECIMAL:
          case TSDB_DATA_TYPE_BLOB:
          case TSDB_DATA_TYPE_MEDIUMBLOB:
            printf("the column type %" PRIi16 " is defined but not implemented yet\n", pColInfoData->info.type);
            TASSERT(0);
            break;
          default:
            if (pColInfoData->info.type < TSDB_DATA_TYPE_MAX && pColInfoData->info.type > TSDB_DATA_TYPE_NULL) {
              tdAppendColValToRow(&rb, 2, pColInfoData->info.type, TD_VTYPE_NORM, var, true, offset, k);
            } else {
              printf("the column type %" PRIi16 " is undefined\n", pColInfoData->info.type);
              TASSERT(0);
            }
            break;
        }
        offset += TYPE_BYTES[pColInfoData->info.type];
      }
      dataLen += TD_ROW_LEN(rb.pBuf);
    }
    pSubmitBlk->dataLen = dataLen;
    msgLen += pSubmitBlk->dataLen;
  }

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

  return TSDB_CODE_SUCCESS;
}

SSubmitReq* tdBlockToSubmit(const SArray* pBlocks, const STSchema* pTSchema, bool createTb, int64_t suid,
                            const char* stbFullName, int32_t vgId) {
  SSubmitReq* ret = NULL;

  // cal size
  int32_t cap = sizeof(SSubmitReq);
  int32_t sz = taosArrayGetSize(pBlocks);
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGet(pBlocks, i);
    int32_t      rows = pDataBlock->info.rows;
    // TODO min
    int32_t rowSize = pDataBlock->info.rowSize;
    int32_t maxLen = TD_ROW_MAX_BYTES_FROM_SCHEMA(pTSchema);
    int32_t schemaLen = 0;

    if (createTb) {
      SVCreateTbReq createTbReq = {0};
      char*         cname = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN);
      snprintf(cname, TSDB_TABLE_FNAME_LEN, "%s:%ld", stbFullName, pDataBlock->info.groupId);
      createTbReq.name = cname;
      createTbReq.flags = 0;
      createTbReq.type = TSDB_CHILD_TABLE;
      createTbReq.ctb.suid = suid;

      SKVRowBuilder kvRowBuilder = {0};
      if (tdInitKVRowBuilder(&kvRowBuilder) < 0) {
        ASSERT(0);
      }
      tdAddColToKVRow(&kvRowBuilder, 1, &pDataBlock->info.groupId, sizeof(uint64_t));
      createTbReq.ctb.pTag = tdGetKVRowFromBuilder(&kvRowBuilder);
      tdDestroyKVRowBuilder(&kvRowBuilder);

      int32_t code;
      tEncodeSize(tEncodeSVCreateTbReq, &createTbReq, schemaLen, code);
      if (code < 0) return NULL;
      taosMemoryFree(cname);
    }

    cap += sizeof(SSubmitBlk) + schemaLen + rows * maxLen;
  }

  // assign data
  ret = taosMemoryCalloc(1, cap + 46);
  ret = POINTER_SHIFT(ret, 46);
  ret->header.vgId = vgId;
  ret->version = htonl(1);
  ret->length = sizeof(SSubmitReq);
  ret->numOfBlocks = htonl(sz);

  void* submitBlk = POINTER_SHIFT(ret, sizeof(SSubmitReq));
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGet(pBlocks, i);

    SSubmitBlk* blkHead = submitBlk;
    blkHead->numOfRows = htons(pDataBlock->info.rows);
    blkHead->sversion = htonl(pTSchema->version);
    // TODO
    blkHead->suid = htobe64(suid);
    // uid is assigned by vnode
    blkHead->uid = 0;

    int32_t rows = pDataBlock->info.rows;
    /*int32_t maxLen = TD_ROW_MAX_BYTES_FROM_SCHEMA(pTSchema);*/
    /*blkHead->dataLen = htonl(rows * maxLen);*/
    blkHead->dataLen = 0;

    void* blockData = POINTER_SHIFT(submitBlk, sizeof(SSubmitBlk));

    int32_t schemaLen = 0;
    if (createTb) {
      SVCreateTbReq createTbReq = {0};
      char*         cname = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN);
      snprintf(cname, TSDB_TABLE_FNAME_LEN, "%s:%ld", stbFullName, pDataBlock->info.groupId);
      createTbReq.name = cname;
      createTbReq.flags = 0;
      createTbReq.type = TSDB_CHILD_TABLE;
      createTbReq.ctb.suid = suid;

      SKVRowBuilder kvRowBuilder = {0};
      if (tdInitKVRowBuilder(&kvRowBuilder) < 0) {
        ASSERT(0);
      }
      tdAddColToKVRow(&kvRowBuilder, 1, &pDataBlock->info.groupId, sizeof(uint64_t));
      createTbReq.ctb.pTag = tdGetKVRowFromBuilder(&kvRowBuilder);
      tdDestroyKVRowBuilder(&kvRowBuilder);

      int32_t code;
      tEncodeSize(tEncodeSVCreateTbReq, &createTbReq, schemaLen, code);
      if (code < 0) return NULL;

      SEncoder encoder = {0};
      tEncoderInit(&encoder, blockData, schemaLen);
      if (tEncodeSVCreateTbReq(&encoder, &createTbReq) < 0) return NULL;
      tEncoderClear(&encoder);
    }
    blkHead->schemaLen = htonl(schemaLen);

    STSRow* rowData = POINTER_SHIFT(blockData, schemaLen);

    for (int32_t j = 0; j < rows; j++) {
      SRowBuilder rb = {0};
      tdSRowInit(&rb, pTSchema->version);
      tdSRowSetTpInfo(&rb, pTSchema->numOfCols, pTSchema->flen);
      tdSRowResetBuf(&rb, rowData);

      for (int32_t k = 0; k < pTSchema->numOfCols; k++) {
        const STColumn*  pColumn = &pTSchema->columns[k];
        SColumnInfoData* pColData = taosArrayGet(pDataBlock->pDataBlock, k);
        if (colDataIsNull_s(pColData, j)) {
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NONE, NULL, false, pColumn->offset, k);
        } else {
          void* data = colDataGetData(pColData, j);
          tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, data, true, pColumn->offset, k);
        }
      }
      int32_t rowLen = TD_ROW_LEN(rowData);
      rowData = POINTER_SHIFT(rowData, rowLen);
      blkHead->dataLen += rowLen;
    }
    int32_t dataLen = blkHead->dataLen;
    blkHead->dataLen = htonl(dataLen);

    ret->length += sizeof(SSubmitBlk) + schemaLen + dataLen;
    blkHead = POINTER_SHIFT(blkHead, schemaLen + dataLen);
    /*submitBlk = blkHead;*/
  }

  ret->length = htonl(ret->length);
  return ret;
}
