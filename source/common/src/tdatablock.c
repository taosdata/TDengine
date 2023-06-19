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
#include "tlog.h"
#include "tname.h"

#define MALLOC_ALIGN_BYTES 32

int32_t colDataGetLength(const SColumnInfoData* pColumnInfoData, int32_t numOfRows) {
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    if (pColumnInfoData->reassigned) {
      int32_t totalSize = 0;
      for (int32_t row = 0; row < numOfRows; ++row) {
        char* pColData = pColumnInfoData->pData + pColumnInfoData->varmeta.offset[row];
        int32_t colSize = 0;
        if (pColumnInfoData->info.type == TSDB_DATA_TYPE_JSON) {
          colSize = getJsonValueLen(pColData);
        } else {
          colSize = varDataTLen(pColData);
        }
        totalSize += colSize;
      }
      return totalSize;
    }
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
    return ((pColumnInfoData->info.type == TSDB_DATA_TYPE_NULL) ? 0 : pColumnInfoData->info.bytes * numOfRows) +
           BitmapLen(numOfRows);
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

int32_t colDataAppend(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, const char* pData, bool isNull) {
  return colDataSetVal(pColumnInfoData, rowIndex, pData, isNull);
}

int32_t colDataSetVal(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, const char* pData, bool isNull) {
  if (isNull) {
    // There is a placehold for each NULL value of binary or nchar type.
    if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
      pColumnInfoData->varmeta.offset[rowIndex] = -1;  // it is a null value of VAR type.
    } else {
      colDataSetNull_f_s(pColumnInfoData, rowIndex);
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
        if (newSize > UINT32_MAX) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }

      char* buf = taosMemoryRealloc(pColumnInfoData->pData, newSize);
      if (buf == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pColumnInfoData->pData = buf;
      pAttr->allocLen = newSize;
    }

    uint32_t len = pColumnInfoData->varmeta.length;
    pColumnInfoData->varmeta.offset[rowIndex] = len;

    memmove(pColumnInfoData->pData + len, pData, dataLen);
    pColumnInfoData->varmeta.length += dataLen;
  } else {
    memcpy(pColumnInfoData->pData + pColumnInfoData->info.bytes * rowIndex, pData, pColumnInfoData->info.bytes);
    colDataClearNull_f(pColumnInfoData->nullbitmap, rowIndex);
  }

  return 0;
}

int32_t colDataReassignVal(SColumnInfoData* pColumnInfoData, uint32_t dstRowIdx, uint32_t srcRowIdx, const char* pData) {
  int32_t type = pColumnInfoData->info.type;
  if (IS_VAR_DATA_TYPE(type)) {
    int32_t dataLen = 0;
    if (type == TSDB_DATA_TYPE_JSON) {
      dataLen = getJsonValueLen(pData);
    } else {
      dataLen = varDataTLen(pData);
    }

    SVarColAttr* pAttr = &pColumnInfoData->varmeta;

    pColumnInfoData->varmeta.offset[dstRowIdx] = pColumnInfoData->varmeta.offset[srcRowIdx];
    pColumnInfoData->reassigned = true;
  } else {
    memcpy(pColumnInfoData->pData + pColumnInfoData->info.bytes * dstRowIdx, pData, pColumnInfoData->info.bytes);
    colDataClearNull_f(pColumnInfoData->nullbitmap, dstRowIdx);
  }

  return 0;
}


int32_t colDataReserve(SColumnInfoData* pColumnInfoData, size_t newSize) {
  if (!IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    return TSDB_CODE_SUCCESS;
  }

  if (pColumnInfoData->varmeta.allocLen >= newSize) {
    return TSDB_CODE_SUCCESS;
  }

  if (pColumnInfoData->varmeta.allocLen < newSize) {
    char* buf = taosMemoryRealloc(pColumnInfoData->pData, newSize);
    if (buf == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    pColumnInfoData->pData = buf;
    pColumnInfoData->varmeta.allocLen = newSize;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doCopyNItems(struct SColumnInfoData* pColumnInfoData, int32_t currentRow, const char* pData,
                         int32_t itemLen, int32_t numOfRows, bool trimValue) {
  if (pColumnInfoData->info.bytes < itemLen) {
    uWarn("column/tag actual data len %d is bigger than schema len %d, trim it:%d", itemLen, pColumnInfoData->info.bytes, trimValue);
    if (trimValue) {
      itemLen = pColumnInfoData->info.bytes;
    } else {
      return TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER;
    }
  }
  
  size_t start = 1;

  // the first item
  memcpy(pColumnInfoData->pData, pData, itemLen);

  int32_t t = 0;
  int32_t count = log(numOfRows) / log(2);
  while (t < count) {
    int32_t xlen = 1 << t;
    memcpy(pColumnInfoData->pData + start * itemLen + pColumnInfoData->varmeta.length, pColumnInfoData->pData,
           xlen * itemLen);
    t += 1;
    start += xlen;
  }

  // the tail part
  if (numOfRows > start) {
    memcpy(pColumnInfoData->pData + start * itemLen + currentRow * itemLen, pColumnInfoData->pData,
           (numOfRows - start) * itemLen);
  }

  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    for (int32_t i = 0; i < numOfRows; ++i) {
      pColumnInfoData->varmeta.offset[i + currentRow] = pColumnInfoData->varmeta.length + i * itemLen;
    }

    pColumnInfoData->varmeta.length += numOfRows * itemLen;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t colDataSetNItems(SColumnInfoData* pColumnInfoData, uint32_t currentRow, const char* pData,
                            uint32_t numOfRows, bool trimValue) {
  int32_t len = pColumnInfoData->info.bytes;
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    len = varDataTLen(pData);
    if (pColumnInfoData->varmeta.allocLen < (numOfRows + currentRow) * len) {
      int32_t code = colDataReserve(pColumnInfoData, (numOfRows + currentRow) * len);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  return doCopyNItems(pColumnInfoData, currentRow, pData, len, numOfRows, trimValue);
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
  pColumnInfoData->nullbitmap[BitmapLen(numOfRow1) - 1] &= (0B11111111 << shiftBits);  // clear remind bits
  pColumnInfoData->nullbitmap[BitmapLen(numOfRow1) - 1] |= (p[0] >> remindBits);  // copy remind bits

  if (BitmapLen(numOfRow1) == BitmapLen(total)) {
    return;
  }

  int32_t len = BitmapLen(numOfRow2);
  int32_t i = 0;

  uint8_t* start = (uint8_t*)&pColumnInfoData->nullbitmap[BitmapLen(numOfRow1)];
  int32_t  overCount = BitmapLen(total) - BitmapLen(numOfRow1);
  memset(start, 0, overCount);
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

int32_t colDataMergeCol(SColumnInfoData* pColumnInfoData, int32_t numOfRow1, int32_t* capacity,
                        const SColumnInfoData* pSource, int32_t numOfRow2) {
  if (pColumnInfoData->info.type != pSource->info.type) {
    return TSDB_CODE_FAILED;
  }

  if (numOfRow2 == 0) {
    return numOfRow1;
  }

  if (pSource->hasNull) {
    pColumnInfoData->hasNull = pSource->hasNull;
  }

  uint32_t finalNumOfRows = numOfRow1 + numOfRow2;
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    // Handle the bitmap
    if (finalNumOfRows > (*capacity)) {
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

    if (pColumnInfoData->pData && pSource->pData) {  // TD-20382
      memcpy(pColumnInfoData->pData + oldLen, pSource->pData, len);
    }
    pColumnInfoData->varmeta.length = len + oldLen;
  } else {
    if (finalNumOfRows > (*capacity)) {
      // all data may be null, when the pColumnInfoData->info.type == 0, bytes == 0;
      char* tmp = taosMemoryRealloc(pColumnInfoData->pData, finalNumOfRows * pColumnInfoData->info.bytes);
      if (tmp == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pColumnInfoData->pData = tmp;
      if (BitmapLen(numOfRow1) < BitmapLen(finalNumOfRows)) {
        char*    btmp = taosMemoryRealloc(pColumnInfoData->nullbitmap, BitmapLen(finalNumOfRows));
        if (btmp == NULL) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }
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
  if (pColumnInfoData->info.type != pSource->info.type || (pBlockInfo != NULL && pBlockInfo->capacity < numOfRows)) {
    return TSDB_CODE_FAILED;
  }

  if (numOfRows <= 0) {
    return numOfRows;
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
    if (pColumnInfoData->pData != NULL && pSource->pData != NULL) {
      memcpy(pColumnInfoData->pData, pSource->pData, pSource->varmeta.length);
    }
  } else {
    memcpy(pColumnInfoData->nullbitmap, pSource->nullbitmap, BitmapLen(numOfRows));
    if (pSource->pData != NULL) {
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
  if (pDataBlock == NULL || pDataBlock->info.rows <= 0 || pDataBlock->info.dataLoad == 0) {
    return 0;
  }

  if (pDataBlock->info.rows > 0) {
    //    ASSERT(pDataBlock->info.dataLoad == 1);
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
  size_t  numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  int32_t numOfRows = pBlock->info.rows;

  int32_t bitmapChar = 1;

  size_t headerSize = sizeof(int32_t);
  size_t colHeaderSize = sizeof(int32_t) * numOfCols;

  // TODO speedup by checking if the whole page can fit in firstly.
  if (!hasVarCol) {
    size_t  rowSize = blockDataGetRowSize(pBlock);
    int32_t capacity = blockDataGetCapacityInRow(pBlock, pageSize, headerSize + colHeaderSize);
    if (capacity <= 0) {
      return TSDB_CODE_FAILED;
    }

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
      if (*stopIndex < startIndex) {
        return TSDB_CODE_FAILED;
      }

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

      if (isNull) {
        colDataSetNULL(pDstCol, j - startIndex);
      } else {
        char* p = colDataGetData(pColData, j);
        colDataSetVal(pDstCol, j - startIndex, p, false);
      }
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

    if (pCol->reassigned && IS_VAR_DATA_TYPE(pCol->info.type)) {
      for (int32_t row = 0; row < numOfRows; ++row) {
        char* pColData = pCol->pData + pCol->varmeta.offset[row];
        int32_t colSize = 0;
        if (pCol->info.type == TSDB_DATA_TYPE_JSON) {
          colSize = getJsonValueLen(pColData);
        } else {
          colSize = varDataTLen(pColData);
        }
        memcpy(pStart, pColData, colSize);
        pStart += colSize;
      }
    } else {
      memcpy(pStart, pCol->pData, dataSize);
      pStart += dataSize;
    }    
  }

  return 0;
}

int32_t blockDataFromBuf(SSDataBlock* pBlock, const char* buf) {
  int32_t numOfRows = *(int32_t*)buf;
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
      if (pCol->varmeta.length > pCol->varmeta.allocLen) {
        return TSDB_CODE_FAILED;
      }
    }

    memcpy(pCol->pData, pStart, colLength);
    pStart += colLength;
  }

  return TSDB_CODE_SUCCESS;
}

static bool colDataIsNNull(const SColumnInfoData* pColumnInfoData, int32_t startIndex,
                                          uint32_t nRows) {
  if (!pColumnInfoData->hasNull) {
    return false;
  }

  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    for (int32_t i = startIndex; i < nRows; ++i) {
      if (!colDataIsNull_var(pColumnInfoData, i)) {
        return false;
      }
    }
  } else {
    if (pColumnInfoData->nullbitmap == NULL) {
      return false;
    }

    for (int32_t i = startIndex; i < nRows; ++i) {
      if (!colDataIsNull_f(pColumnInfoData->nullbitmap, i)) {
        return false;
      }
    }
  }

  return true;
}

// todo remove this
int32_t blockDataFromBuf1(SSDataBlock* pBlock, const char* buf, size_t capacity) {
  pBlock->info.rows = *(int32_t*)buf;
  pBlock->info.id.groupId = *(uint64_t*)(buf + sizeof(int32_t));

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
      if (pCol->varmeta.length > pCol->varmeta.allocLen) {
        return TSDB_CODE_FAILED;
      }
    }

    if (!colDataIsNNull(pCol, 0, pBlock->info.rows)) {
      memcpy(pCol->pData, pStart, colLength);
    }

    pStart += pCol->info.bytes * capacity;
  }

  return TSDB_CODE_SUCCESS;
}

size_t blockDataGetRowSize(SSDataBlock* pBlock) {
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
  // | version | total length | total rows | total columns | flag seg| block group id | column schema
  // | each column length |
  return sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) + sizeof(uint64_t) +
         numOfCols * (sizeof(int8_t) + sizeof(int32_t)) + numOfCols * sizeof(int32_t);
}

double blockDataGetSerialRowSize(const SSDataBlock* pBlock) {
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
      code = colDataSetVal(pDst, numOfRows, NULL, true);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else {
      char* p = colDataGetData(pSrc, tupleIndex);
      code = colDataSetVal(pDst, numOfRows, p, false);
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
      if (pSrc->varmeta.length != 0) {
        memcpy(pDst->pData, pSrc->pData, pSrc->varmeta.length);
      }
      pDst->varmeta.length = pSrc->varmeta.length;

      for (int32_t j = 0; j < pDataBlock->info.rows; ++j) {
        pDst->varmeta.offset[j] = pSrc->varmeta.offset[index[j]];
      }
    } else {
      for (int32_t j = 0; j < pDataBlock->info.rows; ++j) {
        if (colDataIsNull_f(pSrc->nullbitmap, index[j])) {
          colDataSetNull_f_s(pDst, j);
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
  int32_t rows = pDataBlock->info.capacity;
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
        uDebug("blockDataSort easy cost:%" PRId64 ", rows:%" PRId64 "\n", p1 - p0, pDataBlock->info.rows);

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

#if 0
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

  taosMemoryFree(buf);
  return phelper;
}

int32_t dataBlockCompar_rv(const void* p1, const void* p2, const void* param) {
  const SSDataBlockSortHelper* pHelper = (const SSDataBlockSortHelper*)param;

  SHelper* left = (SHelper*)p1;
  SHelper* right = (SHelper*)p2;

  SArray* pInfo = pHelper->orderInfo;

  int32_t offset = 0;
  int32_t leftx = *(int32_t*)left->pData;    //*(int32_t*)(left->pData + offset);
  int32_t rightx = *(int32_t*)right->pData;  //*(int32_t*)(right->pData + offset);

  if (leftx == rightx) {
    return 0;
  } else {
    return (leftx < rightx) ? -1 : 1;
  }
  return 0;
}

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
#endif

void blockDataCleanup(SSDataBlock* pDataBlock) {
  blockDataEmpty(pDataBlock);
  SDataBlockInfo* pInfo = &pDataBlock->info;
  pInfo->id.uid = 0;
  pInfo->id.groupId = 0;
}

void blockDataEmpty(SSDataBlock* pDataBlock) {
  SDataBlockInfo* pInfo = &pDataBlock->info;
  if (pInfo->capacity == 0) {
    return;
  }

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    colInfoDataCleanup(p, pInfo->capacity);
  }

  pInfo->rows = 0;
  pInfo->dataLoad = 0;
  pInfo->window.ekey = 0;
  pInfo->window.skey = 0;
}

/*
 * NOTE: the type of the input column may be TSDB_DATA_TYPE_NULL, which is used to denote
 * the all NULL value in this column. It is an internal representation of all NULL value column, and no visible to
 * any users. The length of TSDB_DATA_TYPE_NULL is 0, and it is an special case.
 */
static int32_t doEnsureCapacity(SColumnInfoData* pColumn, const SDataBlockInfo* pBlockInfo, uint32_t numOfRows,
                                bool clearPayload) {
  if (numOfRows <= 0 || numOfRows <= pBlockInfo->capacity) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t existedRows = pBlockInfo->rows;

  if (IS_VAR_DATA_TYPE(pColumn->info.type)) {
    char* tmp = taosMemoryRealloc(pColumn->varmeta.offset, sizeof(int32_t) * numOfRows);
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    pColumn->varmeta.offset = (int32_t*)tmp;
    memset(&pColumn->varmeta.offset[existedRows], 0, sizeof(int32_t) * (numOfRows - existedRows));
  } else {
    // prepare for the null bitmap
    char* tmp = taosMemoryRealloc(pColumn->nullbitmap, BitmapLen(numOfRows));
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    int32_t oldLen = BitmapLen(existedRows);
    pColumn->nullbitmap = tmp;
    memset(&pColumn->nullbitmap[oldLen], 0, BitmapLen(numOfRows) - oldLen);
    if (pColumn->info.bytes == 0) {
      return TSDB_CODE_FAILED;
    }

    // here we employ the aligned malloc function, to make sure that the address of allocated memory is aligned
    // to MALLOC_ALIGN_BYTES
    tmp = taosMemoryMallocAlign(MALLOC_ALIGN_BYTES, numOfRows * pColumn->info.bytes);
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    // copy back the existed data
    if (pColumn->pData != NULL) {
      memcpy(tmp, pColumn->pData, existedRows * pColumn->info.bytes);
      taosMemoryFreeClear(pColumn->pData);
    }

    pColumn->pData = tmp;

    // check if the allocated memory is aligned to the requried bytes.
#if defined LINUX
    if ((((uint64_t)pColumn->pData) & (MALLOC_ALIGN_BYTES - 1)) != 0x0) {
      return TSDB_CODE_FAILED;
    }
#endif

    if (clearPayload) {
      memset(tmp + pColumn->info.bytes * existedRows, 0, pColumn->info.bytes * (numOfRows - existedRows));
    }
  }

  return TSDB_CODE_SUCCESS;
}

void colInfoDataCleanup(SColumnInfoData* pColumn, uint32_t numOfRows) {
  pColumn->hasNull = false;

  if (IS_VAR_DATA_TYPE(pColumn->info.type)) {
    pColumn->varmeta.length = 0;
    if (pColumn->varmeta.offset != NULL) {
      memset(pColumn->varmeta.offset, 0, sizeof(int32_t) * numOfRows);
    }
  } else {
    if (pColumn->nullbitmap != NULL) {
      memset(pColumn->nullbitmap, 0, BitmapLen(numOfRows));
    }
  }
}

int32_t colInfoDataEnsureCapacity(SColumnInfoData* pColumn, uint32_t numOfRows, bool clearPayload) {
  SDataBlockInfo info = {0};
  return doEnsureCapacity(pColumn, &info, numOfRows, clearPayload);
}

int32_t blockDataEnsureCapacity(SSDataBlock* pDataBlock, uint32_t numOfRows) {
  int32_t code = 0;
  if (numOfRows == 0 || numOfRows <= pDataBlock->info.capacity) {
    return TSDB_CODE_SUCCESS;
  }

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    code = doEnsureCapacity(p, &pDataBlock->info, numOfRows, false);
    if (code) {
      return code;
    }
  }

  pDataBlock->info.capacity = numOfRows;
  return TSDB_CODE_SUCCESS;
}

void blockDataFreeRes(SSDataBlock* pBlock) {
  int32_t numOfOutput = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData* pColInfoData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
    colDataDestroy(pColInfoData);
  }

  taosArrayDestroy(pBlock->pDataBlock);
  pBlock->pDataBlock = NULL;
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

  uint32_t cap = dst->info.capacity;
  dst->info = src->info;
  dst->info.capacity = cap;
  return 0;
}

int32_t copyDataBlock(SSDataBlock* dst, const SSDataBlock* src) {
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
    colDataAssign(pDst, pSrc, src->info.rows, &src->info);
  }

  uint32_t cap = dst->info.capacity;
  dst->info = src->info;
  dst->info.capacity = cap;
  return TSDB_CODE_SUCCESS;
}

SSDataBlock* createSpecialDataBlock(EStreamType type) {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->info.hasVarCol = false;
  pBlock->info.id.groupId = 0;
  pBlock->info.rows = 0;
  pBlock->info.type = type;
  pBlock->info.rowSize = sizeof(TSKEY) + sizeof(TSKEY) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(TSKEY) +
                         sizeof(TSKEY) + VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN;
  pBlock->info.watermark = INT64_MIN;

  pBlock->pDataBlock = taosArrayInit(6, sizeof(SColumnInfoData));
  SColumnInfoData infoData = {0};
  infoData.info.type = TSDB_DATA_TYPE_TIMESTAMP;
  infoData.info.bytes = sizeof(TSKEY);
  // window start ts
  taosArrayPush(pBlock->pDataBlock, &infoData);
  // window end ts
  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_UBIGINT;
  infoData.info.bytes = sizeof(uint64_t);
  // uid
  taosArrayPush(pBlock->pDataBlock, &infoData);
  // group id
  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_TIMESTAMP;
  infoData.info.bytes = sizeof(TSKEY);
  // calculate start ts
  taosArrayPush(pBlock->pDataBlock, &infoData);
  // calculate end ts
  taosArrayPush(pBlock->pDataBlock, &infoData);

  // table name
  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  return pBlock;
}

SSDataBlock* blockCopyOneRow(const SSDataBlock* pDataBlock, int32_t rowIdx) {
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

  int32_t code = blockDataEnsureCapacity(pBlock, 1);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    blockDataDestroy(pBlock);
    return NULL;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, i);
    SColumnInfoData* pSrc = taosArrayGet(pDataBlock->pDataBlock, i);
    void*            pData = colDataGetData(pSrc, rowIdx);
    bool             isNull = colDataIsNull(pSrc, pDataBlock->info.rows, rowIdx, NULL);
    colDataSetVal(pDst, 0, pData, isNull);
  }

  pBlock->info.rows = 1;

  return pBlock;
}

SSDataBlock* createOneDataBlock(const SSDataBlock* pDataBlock, bool copyData) {
  if (pDataBlock == NULL) {
    return NULL;
  }

  SSDataBlock* pBlock = createDataBlock();
  pBlock->info = pDataBlock->info;
  pBlock->info.rows = 0;
  pBlock->info.capacity = 0;
  pBlock->info.rowSize = 0;
  pBlock->info.id = pDataBlock->info.id;

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
    return NULL;
  }

  pBlock->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));
  if (pBlock->pDataBlock == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pBlock);
    return NULL;
  }

  return pBlock;
}

int32_t blockDataAppendColInfo(SSDataBlock* pBlock, SColumnInfoData* pColInfoData) {
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
  if (index >= taosArrayGetSize(pBlock->pDataBlock)) {
    return NULL;
  }

  return taosArrayGet(pBlock->pDataBlock, index);
}

size_t blockDataGetCapacityInRow(const SSDataBlock* pBlock, size_t pageSize, int32_t extraSize) {
  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  int32_t payloadSize = pageSize - extraSize;
  int32_t rowSize = pBlock->info.rowSize;
  int32_t nRows = payloadSize / rowSize;
  ASSERT(nRows >= 1);

  int32_t numVarCols = 0;
  int32_t numFixCols = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      ++numVarCols;
    } else {
      ++numFixCols;
    }
  }

  // find the data payload whose size is greater than payloadSize
  int result = -1;
  int start = 1;
  int end = nRows;
  while (start <= end) {
    int mid = start + (end - start) / 2;
    //data size + var data type columns offset + fixed data type columns bitmap len 
    int midSize = rowSize * mid + numVarCols * sizeof(int32_t) * mid + numFixCols * BitmapLen(mid); 
    if (midSize > payloadSize) {
      result = mid;
      end = mid - 1;
    } else {
      start = mid + 1;
    }
  }

  int32_t newRows = (result != -1) ? result - 1 : nRows;
  // the true value must be less than the value of nRows
  ASSERT(newRows <= nRows && newRows >= 1);

  return newRows;
}

void colDataDestroy(SColumnInfoData* pColData) {
  if (!pColData) {
    return;
  }

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
      int32_t remain = (total % 8 != 0 && total % 8 <= tail) ? 1 : 0;
      int32_t gap = len - newLen - remain;
      while (i < newLen) {
        uint8_t v = p[i + gap];
        p[i] = (v << tail);

        if (i < newLen - 1 + remain) {
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

    // clear the offset value of the unused entries.
    memset(&pColInfoData->varmeta.offset[total - n], 0, n);
  } else {
    int32_t bytes = pColInfoData->info.bytes;
    memmove(pColInfoData->pData, ((char*)pColInfoData->pData + n * bytes), (total - n) * bytes);
    doShiftBitmap(pColInfoData->nullbitmap, n, total);
  }
}

int32_t blockDataTrimFirstRows(SSDataBlock* pBlock, size_t n) {
  if (n == 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (pBlock->info.rows <= n) {
    blockDataEmpty(pBlock);
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
  } else {  // reset the bitmap value
    /*int32_t stopIndex = BitmapLen(n) * 8;
    for(int32_t i = n; i < stopIndex; ++i) {
      colDataClearNull_f(pColInfoData->nullbitmap, i);
    }

    int32_t remain = BitmapLen(total) - BitmapLen(n);
    if (remain > 0) {
      memset(pColInfoData->nullbitmap+BitmapLen(n), 0, remain);
    }*/
  }
}

int32_t blockDataKeepFirstNRows(SSDataBlock* pBlock, size_t n) {
  if (n == 0) {
    blockDataEmpty(pBlock);
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
  int64_t tbUid = pBlock->info.id.uid;
  int16_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  int16_t hasVarCol = pBlock->info.hasVarCol;
  int64_t rows = pBlock->info.rows;
  int32_t sz = taosArrayGetSize(pBlock->pDataBlock);

  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, tbUid);
  tlen += taosEncodeFixedI16(buf, numOfCols);
  tlen += taosEncodeFixedI16(buf, hasVarCol);
  tlen += taosEncodeFixedI64(buf, rows);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SColumnInfoData* pColData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
    tlen += taosEncodeFixedI16(buf, pColData->info.colId);
    tlen += taosEncodeFixedI8(buf, pColData->info.type);
    tlen += taosEncodeFixedI32(buf, pColData->info.bytes);
    tlen += taosEncodeFixedBool(buf, pColData->hasNull);

    if (IS_VAR_DATA_TYPE(pColData->info.type)) {
      tlen += taosEncodeBinary(buf, pColData->varmeta.offset, sizeof(int32_t) * rows);
    } else {
      tlen += taosEncodeBinary(buf, pColData->nullbitmap, BitmapLen(rows));
    }

    int32_t len = colDataGetLength(pColData, rows);
    tlen += taosEncodeFixedI32(buf, len);

    if (pColData->reassigned && IS_VAR_DATA_TYPE(pColData->info.type)) {
      for (int32_t row = 0; row < rows; ++row) {
        char* pData = pColData->pData + pColData->varmeta.offset[row];
        int32_t colSize = 0;
        if (pColData->info.type == TSDB_DATA_TYPE_JSON) {
          colSize = getJsonValueLen(pData);
        } else {
          colSize = varDataTLen(pData);
        }
        tlen += taosEncodeBinary(buf, pData, colSize);
      }
    } else {
      tlen += taosEncodeBinary(buf, pColData->pData, len);
    }
  }
  return tlen;
}

void* tDecodeDataBlock(const void* buf, SSDataBlock* pBlock) {
  int32_t sz;

  int16_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  buf = taosDecodeFixedU64(buf, &pBlock->info.id.uid);
  buf = taosDecodeFixedI16(buf, &numOfCols);
  buf = taosDecodeFixedI16(buf, &pBlock->info.hasVarCol);
  buf = taosDecodeFixedI64(buf, &pBlock->info.rows);
  buf = taosDecodeFixedI32(buf, &sz);
  pBlock->pDataBlock = taosArrayInit(sz, sizeof(SColumnInfoData));
  for (int32_t i = 0; i < sz; i++) {
    SColumnInfoData data = {0};
    buf = taosDecodeFixedI16(buf, &data.info.colId);
    buf = taosDecodeFixedI8(buf, &data.info.type);
    buf = taosDecodeFixedI32(buf, &data.info.bytes);
    buf = taosDecodeFixedBool(buf, &data.hasNull);

    if (IS_VAR_DATA_TYPE(data.info.type)) {
      buf = taosDecodeBinary(buf, (void**)&data.varmeta.offset, pBlock->info.rows * sizeof(int32_t));
    } else {
      buf = taosDecodeBinary(buf, (void**)&data.nullbitmap, BitmapLen(pBlock->info.rows));
    }

    int32_t len = 0;
    buf = taosDecodeFixedI32(buf, &len);
    buf = taosDecodeBinary(buf, (void**)&data.pData, len);
    if (IS_VAR_DATA_TYPE(data.info.type)) {
      data.varmeta.length = len;
      data.varmeta.allocLen = len;
    }
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
  if (taosLocalTime(&tt, &ptm, buf) == NULL) {
    return buf;
  }
  size_t pos = strftime(buf, 35, "%Y-%m-%d %H:%M:%S", &ptm);

  if (precision == TSDB_TIME_PRECISION_NANO) {
    sprintf(buf + pos, ".%09d", ms);
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    sprintf(buf + pos, ".%06d", ms);
  } else {
    sprintf(buf + pos, ".%03d", ms);
  }

  return buf;
}

#if 0
void blockDebugShowDataBlock(SSDataBlock* pBlock, const char* flag) {
  SArray* dataBlocks = taosArrayInit(1, sizeof(SSDataBlock*));
  taosArrayPush(dataBlocks, &pBlock);
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
    printf("%s |block ver %" PRIi64 " |block type %d |child id %d|group id %" PRIu64 "\n", flag,
           pDataBlock->info.version, (int32_t)pDataBlock->info.type, pDataBlock->info.childId,
           pDataBlock->info.id.groupId);
    for (int32_t j = 0; j < rows; j++) {
      printf("%s |", flag);
      for (int32_t k = 0; k < numOfCols; k++) {
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
        void*            var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
        if (k == 0) {
          printf("cols:%d |", (int32_t)numOfCols);
        }
        if (colDataIsNull(pColInfoData, rows, j, NULL)) {
          printf(" %15s |", "NULL");
          continue;
        }

        switch (pColInfoData->info.type) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            formatTimestamp(pBuf, *(uint64_t*)var, TSDB_TIME_PRECISION_MILLI);
            printf(" %25s |", pBuf);
            break;
          case TSDB_DATA_TYPE_BOOL:
            printf(" %15" PRIi8 " |", *(int8_t*)var);
            break;
          case TSDB_DATA_TYPE_TINYINT:
            printf(" %15" PRIi8 " |", *(int8_t*)var);
            break;
          case TSDB_DATA_TYPE_SMALLINT:
            printf(" %15" PRIi16 " |", *(int16_t*)var);
            break;
          case TSDB_DATA_TYPE_INT:
            printf(" %15d |", *(int32_t*)var);
            break;
          case TSDB_DATA_TYPE_UTINYINT:
            printf(" %15" PRIu8 " |", *(uint8_t*)var);
            break;
          case TSDB_DATA_TYPE_USMALLINT:
            printf(" %15" PRIu16 " |", *(uint16_t*)var);
            break;
          case TSDB_DATA_TYPE_UINT:
            printf(" %15u |", *(uint32_t*)var);
            break;
          case TSDB_DATA_TYPE_BIGINT:
            printf(" %15" PRId64 " |", *(int64_t*)var);
            break;
          case TSDB_DATA_TYPE_UBIGINT:
            printf(" %15" PRIu64 " |", *(uint64_t*)var);
            break;
          case TSDB_DATA_TYPE_FLOAT:
            printf(" %15f |", *(float*)var);
            break;
          case TSDB_DATA_TYPE_DOUBLE:
            printf(" %15lf |", *(double*)var);
            break;
          case TSDB_DATA_TYPE_VARCHAR:
          case TSDB_DATA_TYPE_GEOMETRY: {
            char*   pData = colDataGetVarData(pColInfoData, j);
            int32_t dataSize = TMIN(sizeof(pBuf) - 1, varDataLen(pData));
            memset(pBuf, 0, dataSize + 1);
            strncpy(pBuf, varDataVal(pData), dataSize);
            printf(" %15s |", pBuf);
          } break;
          case TSDB_DATA_TYPE_NCHAR: {
            char*   pData = colDataGetVarData(pColInfoData, j);
            int32_t dataSize = TMIN(sizeof(pBuf), varDataLen(pData));
            memset(pBuf, 0, dataSize);
            (void)taosUcs4ToMbs((TdUcs4*)varDataVal(pData), dataSize, pBuf);
            printf(" %15s |", pBuf);
          } break;
          default:
            break;
        }
      }
      printf("\n");
    }
  }
}
#endif

// for debug
char* dumpBlockData(SSDataBlock* pDataBlock, const char* flag, char** pDataBuf) {
  int32_t size = 2048*1024;
  *pDataBuf = taosMemoryCalloc(size, 1);
  char*   dumpBuf = *pDataBuf;
  char    pBuf[128] = {0};
  int32_t colNum = taosArrayGetSize(pDataBlock->pDataBlock);
  int32_t rows = pDataBlock->info.rows;
  int32_t len = 0;
  len += snprintf(dumpBuf + len, size - len,
                  "===stream===%s|block type %d|child id %d|group id:%" PRIu64 "|uid:%" PRId64
                  "|rows:%" PRId64 "|version:%" PRIu64 "|cal start:%" PRIu64 "|cal end:%" PRIu64 "|tbl:%s\n",
                  flag, (int32_t)pDataBlock->info.type, pDataBlock->info.childId, pDataBlock->info.id.groupId,
                  pDataBlock->info.id.uid, pDataBlock->info.rows, pDataBlock->info.version,
                  pDataBlock->info.calWin.skey, pDataBlock->info.calWin.ekey, pDataBlock->info.parTbName);
  if (len >= size - 1) return dumpBuf;

  for (int32_t j = 0; j < rows; j++) {
    len += snprintf(dumpBuf + len, size - len, "%s|", flag);
    if (len >= size - 1) return dumpBuf;

    for (int32_t k = 0; k < colNum; k++) {
      SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
      if (colDataIsNull(pColInfoData, rows, j, NULL) || !pColInfoData->pData) {
        len += snprintf(dumpBuf + len, size - len, " %15s |", "NULL");
        if (len >= size - 1) return dumpBuf;
        continue;
      }

      void* var = colDataGetData(pColInfoData, j);
      switch (pColInfoData->info.type) {
        case TSDB_DATA_TYPE_TIMESTAMP:
          memset(pBuf, 0, sizeof(pBuf));
          formatTimestamp(pBuf, *(uint64_t*)var, pColInfoData->info.precision);
          len += snprintf(dumpBuf + len, size - len, " %25s |", pBuf);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_TINYINT:
          len += snprintf(dumpBuf + len, size - len, " %15d |", *(int8_t*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_UTINYINT:
          len += snprintf(dumpBuf + len, size - len, " %15d |", *(uint8_t*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          len += snprintf(dumpBuf + len, size - len, " %15d |", *(int16_t*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_USMALLINT:
          len += snprintf(dumpBuf + len, size - len, " %15d |", *(uint16_t*)var);
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
          len += snprintf(dumpBuf + len, size - len, " %15" PRId64 " |", *(int64_t*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_UBIGINT:
          len += snprintf(dumpBuf + len, size - len, " %15" PRIu64 " |", *(uint64_t*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_FLOAT:
          len += snprintf(dumpBuf + len, size - len, " %15f |", *(float*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          len += snprintf(dumpBuf + len, size - len, " %15f |", *(double*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_BOOL:
          len += snprintf(dumpBuf + len, size - len, " %15d |", *(bool*)var);
          if (len >= size - 1) return dumpBuf;
          break;
        case TSDB_DATA_TYPE_VARCHAR:
        case TSDB_DATA_TYPE_GEOMETRY: {
          memset(pBuf, 0, sizeof(pBuf));
          char*   pData = colDataGetVarData(pColInfoData, j);
          int32_t dataSize = TMIN(sizeof(pBuf), varDataLen(pData));
          dataSize = TMIN(dataSize, 50);
          memcpy(pBuf, varDataVal(pData), dataSize);
          len += snprintf(dumpBuf + len, size - len, " %15s |", pBuf);
          if (len >= size - 1) return dumpBuf;
        } break;
        case TSDB_DATA_TYPE_NCHAR: {
          char*   pData = colDataGetVarData(pColInfoData, j);
          int32_t dataSize = TMIN(sizeof(pBuf), varDataLen(pData));
          memset(pBuf, 0, sizeof(pBuf));
          (void)taosUcs4ToMbs((TdUcs4*)varDataVal(pData), dataSize, pBuf);
          len += snprintf(dumpBuf + len, size - len, " %15s |", pBuf);
          if (len >= size - 1) return dumpBuf;
        } break;
      }
    }
    len += snprintf(dumpBuf + len, size - len, "%d\n", j);
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
 * @param suid
 *
 */
#if 0
int32_t buildSubmitReqFromDataBlock(SSubmitReq** pReq, const SSDataBlock* pDataBlock, STSchema* pTSchema, int32_t vgId,
                                    tb_uid_t suid) {
  int32_t bufSize = sizeof(SSubmitReq);
  int32_t sz = 1;
  for (int32_t i = 0; i < sz; ++i) {
    const SDataBlockInfo* pBlkInfo = &pDataBlock->info;

    int32_t colNum = taosArrayGetSize(pDataBlock->pDataBlock);
    bufSize += pBlkInfo->rows * (TD_ROW_HEAD_LEN + pBlkInfo->rowSize + BitmapLen(colNum));
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
    int32_t colNum = taosArrayGetSize(pDataBlock->pDataBlock);
    int32_t rows = pDataBlock->info.rows;

    if (colNum <= 1) {
      // invalid if only with TS col
      continue;
    }

    if (rb.nCols != colNum) {
      tdSRowSetTpInfo(&rb, colNum, pTSchema->flen);
    }

    SSubmitBlk* pSubmitBlk = POINTER_SHIFT(pDataBuf, msgLen);
    pSubmitBlk->suid = suid;
    pSubmitBlk->uid = pDataBlock->info.id.groupId;
    pSubmitBlk->numOfRows = rows;
    pSubmitBlk->sversion = pTSchema->version;

    msgLen += sizeof(SSubmitBlk);
    int32_t dataLen = 0;
    for (int32_t j = 0; j < rows; ++j) {                               // iterate by row
      tdSRowResetBuf(&rb, POINTER_SHIFT(pDataBuf, msgLen + dataLen));  // set row buf
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
              continue; // offset should keep 0 for next column

            } else if (colDataIsNull_s(pColInfoData, j)) {
              tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, TSDB_DATA_TYPE_TIMESTAMP, TD_VTYPE_NULL, NULL,
                                  false, offset, k);
            } else {
              tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, TSDB_DATA_TYPE_TIMESTAMP, TD_VTYPE_NORM, var,
                                  true, offset, k);
            }
            break;
          case TSDB_DATA_TYPE_NCHAR:
          case TSDB_DATA_TYPE_VARCHAR:  // TSDB_DATA_TYPE_BINARY
          case TSDB_DATA_TYPE_GEOMETRY: {
            if (colDataIsNull_s(pColInfoData, j)) {
              tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, pColInfoData->info.type, TD_VTYPE_NULL, NULL,
                                  false, offset, k);
            } else {
              void* data = colDataGetData(pColInfoData, j);
              tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, pColInfoData->info.type, TD_VTYPE_NORM, data,
                                  true, offset, k);
            }
            break;
          }
          case TSDB_DATA_TYPE_VARBINARY:
          case TSDB_DATA_TYPE_DECIMAL:
          case TSDB_DATA_TYPE_BLOB:
          case TSDB_DATA_TYPE_JSON:
          case TSDB_DATA_TYPE_MEDIUMBLOB:
            uError("the column type %" PRIi16 " is defined but not implemented yet", pColInfoData->info.type);
            break;
          default:
            if (pColInfoData->info.type < TSDB_DATA_TYPE_MAX && pColInfoData->info.type > TSDB_DATA_TYPE_NULL) {
              if (colDataIsNull_s(pColInfoData, j)) {
                tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, pCol->type, TD_VTYPE_NULL, NULL, false,
                                    offset, k);
              } else if (pCol->type == pColInfoData->info.type) {
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
            }
            break;
        }
        offset += TYPE_BYTES[pCol->type];  // sum/avg would convert to int64_t/uint64_t/double during aggregation
      }
      tdSRowEnd(&rb);
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
      blk->sversion = htonl(blk->sversion);
      blk->dataLen = htonl(blk->dataLen);
      blk->schemaLen = htonl(blk->schemaLen);
      blk->numOfRows = htonl(blk->numOfRows);
      blk = (SSubmitBlk*)(blk->data + dataLen);
    }
  } else {
    // no valid rows
    taosMemoryFreeClear(*pReq);
  }

  return TSDB_CODE_SUCCESS;
}
#endif

int32_t buildSubmitReqFromDataBlock(SSubmitReq2** ppReq, const SSDataBlock* pDataBlock, const STSchema* pTSchema,
                                    int64_t uid, int32_t vgId, tb_uid_t suid) {
  SSubmitReq2* pReq = *ppReq;
  SArray*      pVals = NULL;
  int32_t      numOfBlks = 0;
  int32_t      sz = 1;

  terrno = TSDB_CODE_SUCCESS;

  if (NULL == pReq) {
    if (!(pReq = taosMemoryMalloc(sizeof(SSubmitReq2)))) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _end;
    }

    if (!(pReq->aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData)))) {
      goto _end;
    }
  }

  for (int32_t i = 0; i < sz; ++i) {
    int32_t colNum = taosArrayGetSize(pDataBlock->pDataBlock);
    int32_t rows = pDataBlock->info.rows;

    if (colNum <= 1) {  // invalid if only with TS col
      continue;
    }

    // the rsma result should has the same column number with schema.
    ASSERT(colNum == pTSchema->numOfCols);

    SSubmitTbData tbData = {0};

    if (!(tbData.aRowP = taosArrayInit(rows, sizeof(SRow*)))) {
      goto _end;
    }
    tbData.suid = suid;
    tbData.uid = uid;
    tbData.sver = pTSchema->version;

    if (!pVals && !(pVals = taosArrayInit(colNum, sizeof(SColVal)))) {
      taosArrayDestroy(tbData.aRowP);
      goto _end;
    }

    for (int32_t j = 0; j < rows; ++j) {  // iterate by row

      taosArrayClear(pVals);

      bool    isStartKey = false;
      int32_t offset = 0;
      for (int32_t k = 0; k < colNum; ++k) {  // iterate by column
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
        const STColumn*  pCol = &pTSchema->columns[k];
        void*            var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);

        switch (pColInfoData->info.type) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            ASSERT(pColInfoData->info.type == pCol->type);
            if (!isStartKey) {
              isStartKey = true;
              ASSERT(PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId);
              SColVal cv = COL_VAL_VALUE(pCol->colId, pCol->type, (SValue){.val = *(TSKEY*)var});
              taosArrayPush(pVals, &cv);
            } else if (colDataIsNull_s(pColInfoData, j)) {
              SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
              taosArrayPush(pVals, &cv);
            } else {
              SColVal cv = COL_VAL_VALUE(pCol->colId, pCol->type, (SValue){.val = *(int64_t*)var});
              taosArrayPush(pVals, &cv);
            }
            break;
          case TSDB_DATA_TYPE_NCHAR:
          case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
            ASSERT(pColInfoData->info.type == pCol->type);
            if (colDataIsNull_s(pColInfoData, j)) {
              SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
              taosArrayPush(pVals, &cv);
            } else {
              void*   data = colDataGetVarData(pColInfoData, j);
              SValue  sv = (SValue){.nData = varDataLen(data), .pData = varDataVal(data)};  // address copy, no value
              SColVal cv = COL_VAL_VALUE(pCol->colId, pCol->type, sv);
              taosArrayPush(pVals, &cv);
            }
            break;
          }
          case TSDB_DATA_TYPE_VARBINARY:
          case TSDB_DATA_TYPE_DECIMAL:
          case TSDB_DATA_TYPE_BLOB:
          case TSDB_DATA_TYPE_JSON:
          case TSDB_DATA_TYPE_MEDIUMBLOB:
            uError("the column type %" PRIi16 " is defined but not implemented yet", pColInfoData->info.type);
            ASSERT(0);
            break;
          default:
            if (pColInfoData->info.type < TSDB_DATA_TYPE_MAX && pColInfoData->info.type > TSDB_DATA_TYPE_NULL) {
              if (colDataIsNull_s(pColInfoData, j)) {
                SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);  // should use pCol->type
                taosArrayPush(pVals, &cv);
              } else {
                SValue sv;
                if (pCol->type == pColInfoData->info.type) {
                  memcpy(&sv.val, var, tDataTypes[pCol->type].bytes);
                } else {
                  /**
                   *  1. sum/avg would convert to int64_t/uint64_t/double during aggregation
                   *  2. below conversion may lead to overflow or loss, the app should select the right data type.
                   */
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
                  memcpy(&sv.val, tv, tDataTypes[pCol->type].bytes);
                }
                SColVal cv = COL_VAL_VALUE(pCol->colId, pColInfoData->info.type, sv);
                taosArrayPush(pVals, &cv);
              }
            } else {
              uError("the column type %" PRIi16 " is undefined\n", pColInfoData->info.type);
              ASSERT(0);
            }
            break;
        }
      }
      SRow* pRow = NULL;
      if ((terrno = tRowBuild(pVals, pTSchema, &pRow)) < 0) {
        tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }
      ASSERT(pRow);
      taosArrayPush(tbData.aRowP, &pRow);
    }

    taosArrayPush(pReq->aSubmitTbData, &tbData);
  }
_end:
  taosArrayDestroy(pVals);
  if (terrno != 0) {
    *ppReq = NULL;
    if (pReq) {
      tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
      taosMemoryFreeClear(pReq);
    }

    return TSDB_CODE_FAILED;
  }
  *ppReq = pReq;
  return TSDB_CODE_SUCCESS;
}

char* buildCtbNameByGroupId(const char* stbFullName, uint64_t groupId) {
  char* pBuf = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN + 1);
  if (!pBuf) {
    return NULL;
  }
  int32_t code = buildCtbNameByGroupIdImpl(stbFullName, groupId, pBuf);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pBuf);
    return NULL;
  }
  return pBuf;
}

int32_t buildCtbNameByGroupIdImpl(const char* stbFullName, uint64_t groupId, char* cname) {
  if (stbFullName[0] == 0) {
    return TSDB_CODE_FAILED;
  }

  SArray* tags = taosArrayInit(0, sizeof(SSmlKv));
  if (tags == NULL) {
    return TSDB_CODE_FAILED;
  }

  if (cname == NULL) {
    taosArrayDestroy(tags);
    return TSDB_CODE_FAILED;
  }

  SSmlKv pTag = {.key = "group_id",
                 .keyLen = sizeof("group_id") - 1,
                 .type = TSDB_DATA_TYPE_UBIGINT,
                 .u = groupId,
                 .length = sizeof(uint64_t)};
  taosArrayPush(tags, &pTag);

  RandTableName rname = {
      .tags = tags,
      .stbFullName = stbFullName,
      .stbFullNameLen = strlen(stbFullName),
      .ctbShortName = cname,
  };

  buildChildTableName(&rname);

  taosArrayDestroy(tags);

  if ((rname.ctbShortName && rname.ctbShortName[0]) == 0) {
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t blockEncode(const SSDataBlock* pBlock, char* data, int32_t numOfCols) {
  int32_t dataLen = 0;

  // todo extract method
  int32_t* version = (int32_t*)data;
  *version = 1;
  data += sizeof(int32_t);

  int32_t* actualLen = (int32_t*)data;
  data += sizeof(int32_t);

  int32_t* rows = (int32_t*)data;
  *rows = pBlock->info.rows;
  data += sizeof(int32_t);
  ASSERT(*rows > 0);

  int32_t* cols = (int32_t*)data;
  *cols = numOfCols;
  data += sizeof(int32_t);

  // flag segment.
  // the inital bit is for column info
  int32_t* flagSegment = (int32_t*)data;
  *flagSegment = (1 << 31);

  data += sizeof(int32_t);

  uint64_t* groupId = (uint64_t*)data;
  data += sizeof(uint64_t);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

    *((int8_t*)data) = pColInfoData->info.type;
    data += sizeof(int8_t);

    *((int32_t*)data) = pColInfoData->info.bytes;
    data += sizeof(int32_t);
  }

  int32_t* colSizes = (int32_t*)data;
  data += numOfCols * sizeof(int32_t);

  dataLen = blockDataGetSerialMetaSize(numOfCols);

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
    dataLen += metaSize;

     if (pColRes->reassigned && IS_VAR_DATA_TYPE(pColRes->info.type)) {
        colSizes[col] = 0;
        for (int32_t row = 0; row < numOfRows; ++row) {
          char* pColData = pColRes->pData + pColRes->varmeta.offset[row];
          int32_t colSize = 0;
          if (pColRes->info.type == TSDB_DATA_TYPE_JSON) {
            colSize = getJsonValueLen(pColData);
          } else {
            colSize = varDataTLen(pColData);
          }
          colSizes[col] += colSize;
          dataLen += colSize;
          memmove(data, pColData, colSize);
          data += colSize;
        }
    } else {
      colSizes[col] = colDataGetLength(pColRes, numOfRows);
      dataLen += colSizes[col];
      if (pColRes->pData != NULL) {
        memmove(data, pColRes->pData, colSizes[col]);
      }
      data += colSizes[col];
    }

    colSizes[col] = htonl(colSizes[col]);
//    uError("blockEncode col bytes:%d, type:%d, size:%d, htonl size:%d", pColRes->info.bytes, pColRes->info.type, htonl(colSizes[col]), colSizes[col]);
  }

  *actualLen = dataLen;
  *groupId = pBlock->info.id.groupId;
  ASSERT(dataLen > 0);
  return dataLen;
}

const char* blockDecode(SSDataBlock* pBlock, const char* pData) {
  const char* pStart = pData;

  int32_t version = *(int32_t*)pStart;
  pStart += sizeof(int32_t);
  ASSERT(version == 1);

  // total length sizeof(int32_t)
  int32_t dataLen = *(int32_t*)pStart;
  pStart += sizeof(int32_t);

  // total rows sizeof(int32_t)
  int32_t numOfRows = *(int32_t*)pStart;
  pStart += sizeof(int32_t);

  // total columns sizeof(int32_t)
  int32_t numOfCols = *(int32_t*)pStart;
  pStart += sizeof(int32_t);

  // has column info segment
  int32_t flagSeg = *(int32_t*)pStart;
  int32_t hasColumnInfo = (flagSeg >> 31);
  pStart += sizeof(int32_t);

  // group id sizeof(uint64_t)
  pBlock->info.id.groupId = *(uint64_t*)pStart;
  pStart += sizeof(uint64_t);

  if (pBlock->pDataBlock == NULL) {
    pBlock->pDataBlock = taosArrayInit_s(sizeof(SColumnInfoData), numOfCols);
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    pColInfoData->info.type = *(int8_t*)pStart;
    pStart += sizeof(int8_t);

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

  pBlock->info.dataLoad = 1;
  pBlock->info.rows = numOfRows;
  ASSERT(pStart - pData == dataLen);
  return pStart;
}
