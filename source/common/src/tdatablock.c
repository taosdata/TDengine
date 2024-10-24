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
#include "tglobal.h"

#define MALLOC_ALIGN_BYTES 32

int32_t colDataGetLength(const SColumnInfoData* pColumnInfoData, int32_t numOfRows) {
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    if (pColumnInfoData->reassigned) {
      int32_t totalSize = 0;
      for (int32_t row = 0; row < numOfRows; ++row) {
        char*   pColData = pColumnInfoData->pData + pColumnInfoData->varmeta.offset[row];
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

int32_t colDataGetRowLength(const SColumnInfoData* pColumnInfoData, int32_t rowIdx) {
  if (colDataIsNull_s(pColumnInfoData, rowIdx)) {
    return 0;
  }

  if (!IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) return pColumnInfoData->info.bytes;
  if (pColumnInfoData->info.type == TSDB_DATA_TYPE_JSON)
    return getJsonValueLen(colDataGetData(pColumnInfoData, rowIdx));
  else
    return varDataTLen(colDataGetData(pColumnInfoData, rowIdx));
}

int32_t colDataGetFullLength(const SColumnInfoData* pColumnInfoData, int32_t numOfRows) {
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    return pColumnInfoData->varmeta.length + sizeof(int32_t) * numOfRows;
  } else {
    return ((pColumnInfoData->info.type == TSDB_DATA_TYPE_NULL) ? 0 : pColumnInfoData->info.bytes * numOfRows) +
           BitmapLen(numOfRows);
  }
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
    uError("Invalid data type:%d in Json", *data);
  }
  return dataLen;
}

int32_t colDataSetVal(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, const char* pData, bool isNull) {
  if (isNull || pData == NULL) {
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
        return terrno;
      }

      pColumnInfoData->pData = buf;
      pAttr->allocLen = newSize;
    }

    uint32_t len = pColumnInfoData->varmeta.length;
    pColumnInfoData->varmeta.offset[rowIndex] = len;

    (void) memmove(pColumnInfoData->pData + len, pData, dataLen);
    pColumnInfoData->varmeta.length += dataLen;
  } else {
    memcpy(pColumnInfoData->pData + pColumnInfoData->info.bytes * rowIndex, pData, pColumnInfoData->info.bytes);
    colDataClearNull_f(pColumnInfoData->nullbitmap, rowIndex);
  }

  return 0;
}

int32_t colDataReassignVal(SColumnInfoData* pColumnInfoData, uint32_t dstRowIdx, uint32_t srcRowIdx,
                           const char* pData) {
  int32_t type = pColumnInfoData->info.type;
  if (IS_VAR_DATA_TYPE(type)) {
    pColumnInfoData->varmeta.offset[dstRowIdx] = pColumnInfoData->varmeta.offset[srcRowIdx];
    pColumnInfoData->reassigned = true;
  } else {
    memcpy(pColumnInfoData->pData + pColumnInfoData->info.bytes * dstRowIdx, pData, pColumnInfoData->info.bytes);
    colDataClearNull_f(pColumnInfoData->nullbitmap, dstRowIdx);
  }

  return 0;
}

static int32_t colDataReserve(SColumnInfoData* pColumnInfoData, size_t newSize) {
  if (!IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    return TSDB_CODE_SUCCESS;
  }

  if (pColumnInfoData->varmeta.allocLen >= newSize) {
    return TSDB_CODE_SUCCESS;
  }

  if (pColumnInfoData->varmeta.allocLen < newSize) {
    char* buf = taosMemoryRealloc(pColumnInfoData->pData, newSize);
    if (buf == NULL) {
      return terrno;
    }

    pColumnInfoData->pData = buf;
    pColumnInfoData->varmeta.allocLen = newSize;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doCopyNItems(struct SColumnInfoData* pColumnInfoData, int32_t currentRow, const char* pData,
                            int32_t itemLen, int32_t numOfRows, bool trimValue) {
  if (pColumnInfoData->info.bytes < itemLen) {
    uWarn("column/tag actual data len %d is bigger than schema len %d, trim it:%d", itemLen,
          pColumnInfoData->info.bytes, trimValue);
    if (trimValue) {
      itemLen = pColumnInfoData->info.bytes;
    } else {
      return TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER;
    }
  }

  size_t   start = 1;
  int32_t  t = 0;
  int32_t  count = log(numOfRows) / log(2);
  uint32_t startOffset =
      (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) ? pColumnInfoData->varmeta.length : (currentRow * itemLen);

  // the first item
  memcpy(pColumnInfoData->pData + startOffset, pData, itemLen);

  while (t < count) {
    int32_t xlen = 1 << t;
    memcpy(pColumnInfoData->pData + start * itemLen + startOffset, pColumnInfoData->pData + startOffset,
           xlen * itemLen);
    t += 1;
    start += xlen;
  }

  // the tail part
  if (numOfRows > start) {
    memcpy(pColumnInfoData->pData + start * itemLen + startOffset, pColumnInfoData->pData + startOffset,
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

int32_t colDataSetNItems(SColumnInfoData* pColumnInfoData, uint32_t currentRow, const char* pData, uint32_t numOfRows,
                         bool trimValue) {
  int32_t len = pColumnInfoData->info.bytes;
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    if (pColumnInfoData->info.type == TSDB_DATA_TYPE_JSON) {
      len = getJsonValueLen(pData);
    } else {
      len = varDataTLen(pData);
    }
    if (pColumnInfoData->varmeta.allocLen < (numOfRows * len + pColumnInfoData->varmeta.length)) {
      int32_t code = colDataReserve(pColumnInfoData, (numOfRows * len + pColumnInfoData->varmeta.length));
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  return doCopyNItems(pColumnInfoData, currentRow, pData, len, numOfRows, trimValue);
}

void colDataSetNItemsNull(SColumnInfoData* pColumnInfoData, uint32_t currentRow, uint32_t numOfRows) {
  pColumnInfoData->hasNull = true;

  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    memset(&pColumnInfoData->varmeta.offset[currentRow], -1, sizeof(int32_t) * numOfRows);
  } else {
    if (numOfRows < 16) {
      for (int32_t i = 0; i < numOfRows; ++i) {
        colDataSetNull_f(pColumnInfoData->nullbitmap, currentRow + i);
      }
    } else {
      int32_t i = 0;
      for (; i < numOfRows; ++i) {
        if (BitPos(currentRow + i)) {
          colDataSetNull_f(pColumnInfoData->nullbitmap, currentRow + i);
        } else {
          break;
        }
      }

      int32_t bytes = (numOfRows - i) / 8;
      memset(&BMCharPos(pColumnInfoData->nullbitmap, currentRow + i), 0xFF, bytes);
      i += bytes * 8;

      for (; i < numOfRows; ++i) {
        colDataSetNull_f(pColumnInfoData->nullbitmap, currentRow + i);
      }
    }
  }
}

int32_t colDataCopyAndReassign(SColumnInfoData* pColumnInfoData, uint32_t currentRow, const char* pData,
                               uint32_t numOfRows) {
  int32_t code = colDataSetVal(pColumnInfoData, currentRow, pData, false);
  if (code) {
    return code;
  }

  if (numOfRows > 1) {
    int32_t* pOffset = pColumnInfoData->varmeta.offset;
    memset(&pOffset[currentRow + 1], pOffset[currentRow], sizeof(pOffset[0]) * (numOfRows - 1));
    pColumnInfoData->reassigned = true;
  }

  return code;
}

int32_t colDataCopyNItems(SColumnInfoData* pColumnInfoData, uint32_t currentRow, const char* pData, uint32_t numOfRows,
                          bool isNull) {
  int32_t len = pColumnInfoData->info.bytes;
  if (isNull) {
    colDataSetNItemsNull(pColumnInfoData, currentRow, numOfRows);
    pColumnInfoData->hasNull = true;
    return 0;
  }

  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    return colDataCopyAndReassign(pColumnInfoData, currentRow, pData, numOfRows);
  } else {
    int32_t  colBytes = pColumnInfoData->info.bytes;
    int32_t  colOffset = currentRow * colBytes;
    uint32_t num = 1;

    void* pStart = pColumnInfoData->pData + colOffset;
    memcpy(pStart, pData, colBytes);
    colOffset += num * colBytes;

    while (num < numOfRows) {
      int32_t maxNum = num << 1;
      int32_t tnum = maxNum > numOfRows ? (numOfRows - num) : num;

      memcpy(pColumnInfoData->pData + colOffset, pStart, tnum * colBytes);
      colOffset += tnum * colBytes;
      num += tnum;
    }
  }

  return TSDB_CODE_SUCCESS;
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
  pColumnInfoData->nullbitmap[BitmapLen(numOfRow1) - 1] |= (p[0] >> remindBits);       // copy remind bits

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
    return TSDB_CODE_INVALID_PARA;
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
        return terrno;
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
        return terrno;
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
        return terrno;
      }

      pColumnInfoData->pData = tmp;
      if (BitmapLen(numOfRow1) < BitmapLen(finalNumOfRows)) {
        char* btmp = taosMemoryRealloc(pColumnInfoData->nullbitmap, BitmapLen(finalNumOfRows));
        if (btmp == NULL) {
          return terrno;
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
    return TSDB_CODE_INVALID_PARA;
  }

  if (numOfRows <= 0) {
    return numOfRows;
  }

  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    int32_t newLen = pSource->varmeta.length;
    memcpy(pColumnInfoData->varmeta.offset, pSource->varmeta.offset, sizeof(int32_t) * numOfRows);
    if (pColumnInfoData->varmeta.allocLen < newLen) {
      char* tmp = taosMemoryRealloc(pColumnInfoData->pData, newLen);
      if (tmp == NULL) {
        return terrno;
      }

      pColumnInfoData->pData = tmp;
      pColumnInfoData->varmeta.allocLen = newLen;
    }

    pColumnInfoData->varmeta.length = newLen;
    if (pColumnInfoData->pData != NULL && pSource->pData != NULL) {
      memcpy(pColumnInfoData->pData, pSource->pData, newLen);
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

int32_t colDataAssignNRows(SColumnInfoData* pDst, int32_t dstIdx, const SColumnInfoData* pSrc, int32_t srcIdx,
                           int32_t numOfRows) {
  if (pDst->info.type != pSrc->info.type || pDst->info.bytes != pSrc->info.bytes || pSrc->reassigned) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (numOfRows <= 0) {
    return numOfRows;
  }

  if (IS_VAR_DATA_TYPE(pDst->info.type)) {
    int32_t allLen = 0;
    void*   srcAddr = NULL;
    if (pSrc->hasNull) {
      for (int32_t i = 0; i < numOfRows; ++i) {
        if (colDataIsNull_var(pSrc, srcIdx + i)) {
          pDst->varmeta.offset[dstIdx + i] = -1;
          pDst->hasNull = true;
          continue;
        }

        char* pData = colDataGetVarData(pSrc, srcIdx + i);
        if (NULL == srcAddr) {
          srcAddr = pData;
        }
        int32_t dataLen = 0;
        if (pSrc->info.type == TSDB_DATA_TYPE_JSON) {
          dataLen = getJsonValueLen(pData);
        } else {
          dataLen = varDataTLen(pData);
        }
        pDst->varmeta.offset[dstIdx + i] = pDst->varmeta.length + allLen;
        allLen += dataLen;
      }
    } else {
      for (int32_t i = 0; i < numOfRows; ++i) {
        char*   pData = colDataGetVarData(pSrc, srcIdx + i);
        int32_t dataLen = 0;
        if (pSrc->info.type == TSDB_DATA_TYPE_JSON) {
          dataLen = getJsonValueLen(pData);
        } else {
          dataLen = varDataTLen(pData);
        }
        pDst->varmeta.offset[dstIdx + i] = pDst->varmeta.length + allLen;
        allLen += dataLen;
      }
    }

    if (allLen > 0) {
      // copy data
      if (pDst->varmeta.allocLen < pDst->varmeta.length + allLen) {
        char* tmp = taosMemoryRealloc(pDst->pData, pDst->varmeta.length + allLen);
        if (tmp == NULL) {
          return terrno;
        }

        pDst->pData = tmp;
        pDst->varmeta.allocLen = pDst->varmeta.length + allLen;
      }
      if (pSrc->hasNull) {
        memcpy(pDst->pData + pDst->varmeta.length, srcAddr, allLen);
      } else {
        memcpy(pDst->pData + pDst->varmeta.length, colDataGetVarData(pSrc, srcIdx), allLen);
      }
      pDst->varmeta.length = pDst->varmeta.length + allLen;
    }
  } else {
    if (pSrc->hasNull) {
      if (BitPos(dstIdx) == BitPos(srcIdx)) {
        for (int32_t i = 0; i < numOfRows; ++i) {
          if (0 == BitPos(dstIdx) && (i + (1 << NBIT) <= numOfRows)) {
            BMCharPos(pDst->nullbitmap, dstIdx + i) = BMCharPos(pSrc->nullbitmap, srcIdx + i);
            if (BMCharPos(pDst->nullbitmap, dstIdx + i)) {
              pDst->hasNull = true;
            }
            i += (1 << NBIT) - 1;
          } else {
            if (colDataIsNull_f(pSrc->nullbitmap, srcIdx + i)) {
              colDataSetNull_f(pDst->nullbitmap, dstIdx + i);
              pDst->hasNull = true;
            } else {
              colDataClearNull_f(pDst->nullbitmap, dstIdx + i);
            }
          }
        }
      } else {
        for (int32_t i = 0; i < numOfRows; ++i) {
          if (colDataIsNull_f(pSrc->nullbitmap, srcIdx + i)) {
            colDataSetNull_f(pDst->nullbitmap, dstIdx + i);
            pDst->hasNull = true;
          } else {
            colDataClearNull_f(pDst->nullbitmap, dstIdx + i);
          }
        }
      }
    }

    if (pSrc->pData != NULL) {
      memcpy(pDst->pData + pDst->info.bytes * dstIdx, pSrc->pData + pSrc->info.bytes * srcIdx,
             pDst->info.bytes * numOfRows);
    }
  }

  return 0;
}

size_t blockDataGetNumOfCols(const SSDataBlock* pBlock) { return taosArrayGetSize(pBlock->pDataBlock); }

size_t blockDataGetNumOfRows(const SSDataBlock* pBlock) { return pBlock->info.rows; }

int32_t blockDataUpdateTsWindow(SSDataBlock* pDataBlock, int32_t tsColumnIndex) {
  if (pDataBlock == NULL || pDataBlock->info.rows <= 0 || pDataBlock->info.dataLoad == 0) {
    return 0;
  }

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  if (numOfCols <= 0) {
    return -1;
  }

  int32_t index = (tsColumnIndex == -1) ? 0 : tsColumnIndex;

  SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, index);
  if (pColInfoData == NULL) {
    return 0;
  }

  if (pColInfoData->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
    return 0;
  }

  TSKEY skey = *(TSKEY*)colDataGetData(pColInfoData, 0);
  TSKEY ekey = *(TSKEY*)colDataGetData(pColInfoData, (pDataBlock->info.rows - 1));

  pDataBlock->info.window.skey = TMIN(skey, ekey);
  pDataBlock->info.window.ekey = TMAX(skey, ekey);

  return 0;
}

int32_t blockDataUpdatePkRange(SSDataBlock* pDataBlock, int32_t pkColumnIndex, bool asc) {
  if (pDataBlock == NULL || pDataBlock->info.rows <= 0 || pDataBlock->info.dataLoad == 0 || pkColumnIndex == -1) {
    return 0;
  }

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  if (numOfCols <= 0) {
    return -1;
  }

  SDataBlockInfo*  pInfo = &pDataBlock->info;
  SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, pkColumnIndex);
  if (pColInfoData == NULL) {
    return terrno;
  }

  if (!IS_NUMERIC_TYPE(pColInfoData->info.type) && (pColInfoData->info.type != TSDB_DATA_TYPE_VARCHAR)) {
    return 0;
  }

  void* skey = colDataGetData(pColInfoData, 0);
  void* ekey = colDataGetData(pColInfoData, (pInfo->rows - 1));

  if (asc) {
    if (IS_NUMERIC_TYPE(pColInfoData->info.type)) {
      GET_TYPED_DATA(pInfo->pks[0].val, int64_t, pColInfoData->info.type, skey);
      GET_TYPED_DATA(pInfo->pks[1].val, int64_t, pColInfoData->info.type, ekey);
    } else {  // todo refactor
      memcpy(pInfo->pks[0].pData, varDataVal(skey), varDataLen(skey));
      pInfo->pks[0].nData = varDataLen(skey);

      memcpy(pInfo->pks[1].pData, varDataVal(ekey), varDataLen(ekey));
      pInfo->pks[1].nData = varDataLen(ekey);
    }
  } else {
    if (IS_NUMERIC_TYPE(pColInfoData->info.type)) {
      GET_TYPED_DATA(pInfo->pks[0].val, int64_t, pColInfoData->info.type, ekey);
      GET_TYPED_DATA(pInfo->pks[1].val, int64_t, pColInfoData->info.type, skey);
    } else {  // todo refactor
      memcpy(pInfo->pks[0].pData, varDataVal(ekey), varDataLen(ekey));
      pInfo->pks[0].nData = varDataLen(ekey);

      memcpy(pInfo->pks[1].pData, varDataVal(skey), varDataLen(skey));
      pInfo->pks[1].nData = varDataLen(skey);
    }
  }

  return 0;
}

int32_t blockDataMerge(SSDataBlock* pDest, const SSDataBlock* pSrc) {
  int32_t code = 0;
  int32_t capacity = pDest->info.capacity;
  size_t  numOfCols = taosArrayGetSize(pDest->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol2 = taosArrayGet(pDest->pDataBlock, i);
    SColumnInfoData* pCol1 = taosArrayGet(pSrc->pDataBlock, i);
    if (pCol1 == NULL || pCol2 == NULL) {
      return terrno;
    }

    capacity = pDest->info.capacity;
    int32_t ret = colDataMergeCol(pCol2, pDest->info.rows, &capacity, pCol1, pSrc->info.rows);
    if (ret < 0) {  // error occurs
      code = ret;
      return code;
    }
  }

  pDest->info.capacity = capacity;
  pDest->info.rows += pSrc->info.rows;
  return code;
}

int32_t blockDataMergeNRows(SSDataBlock* pDest, const SSDataBlock* pSrc, int32_t srcIdx, int32_t numOfRows) {
  int32_t code = 0;
  if (pDest->info.rows + numOfRows > pDest->info.capacity) {
    return TSDB_CODE_INVALID_PARA;
  }

  size_t numOfCols = taosArrayGetSize(pDest->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol2 = taosArrayGet(pDest->pDataBlock, i);
    SColumnInfoData* pCol1 = taosArrayGet(pSrc->pDataBlock, i);
    if (pCol2 == NULL || pCol1 == NULL) {
      return terrno;
    }

    code = colDataAssignNRows(pCol2, pDest->info.rows, pCol1, srcIdx, numOfRows);
    if (code) {
      return code;
    }
  }

  pDest->info.rows += numOfRows;
  return code;
}

void blockDataShrinkNRows(SSDataBlock* pBlock, int32_t numOfRows) {
  if (numOfRows == 0) {
    return;
  }
  
  if (numOfRows >= pBlock->info.rows) {
    blockDataCleanup(pBlock);
    return;
  }

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (pCol == NULL) {
      continue;
    }

    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      pCol->varmeta.length = pCol->varmeta.offset[pBlock->info.rows - numOfRows];
      memset(pCol->varmeta.offset + pBlock->info.rows - numOfRows, 0, sizeof(*pCol->varmeta.offset) * numOfRows);
    } else {
      int32_t i = pBlock->info.rows - numOfRows;
      for (; i < pBlock->info.rows; ++i) {
        if (BitPos(i)) {
          colDataClearNull_f(pCol->nullbitmap, i);
        } else {
          break;
        }
      }

      int32_t bytes = (pBlock->info.rows - i) / 8;
      memset(&BMCharPos(pCol->nullbitmap, i), 0, bytes);
      i += bytes * 8;

      for (; i < pBlock->info.rows; ++i) {
        colDataClearNull_f(pCol->nullbitmap, i);
      }
    }
  }

  pBlock->info.rows -= numOfRows;
}

size_t blockDataGetSize(const SSDataBlock* pBlock) {
  size_t total = 0;
  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    if (pColInfoData == NULL) {
      continue;
    }

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
      return terrno;
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
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }

      return TSDB_CODE_SUCCESS;
    }
  }

  // all fit in
  *stopIndex = numOfRows - 1;
  return TSDB_CODE_SUCCESS;
}

int32_t blockDataExtractBlock(SSDataBlock* pBlock, int32_t startIndex, int32_t rowCount, SSDataBlock** pResBlock) {
  int32_t code = 0;
  QRY_PARAM_CHECK(pResBlock);

  if (pBlock == NULL || startIndex < 0 || rowCount > pBlock->info.rows || rowCount + startIndex > pBlock->info.rows) {
    return TSDB_CODE_INVALID_PARA;
  }

  SSDataBlock* pDst = NULL;
  code = createOneDataBlock(pBlock, false, &pDst);
  if (code) {
    return code;
  }

  code = blockDataEnsureCapacity(pDst, rowCount);
  if (code) {
    blockDataDestroy(pDst);
    return code;
  }

  /* may have disorder varchar data, TODO
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
      SColumnInfoData* pDstCol = taosArrayGet(pDst->pDataBlock, i);

      colDataAssignNRows(pDstCol, 0, pColData, startIndex, rowCount);
    }
  */

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
    SColumnInfoData* pDstCol = taosArrayGet(pDst->pDataBlock, i);
    if (pColData == NULL || pDstCol == NULL) {
      continue;
    }

    for (int32_t j = startIndex; j < (startIndex + rowCount); ++j) {
      bool isNull = false;
      if (pBlock->pBlockAgg == NULL) {
        isNull = colDataIsNull_s(pColData, j);
      } else {
        isNull = colDataIsNull(pColData, pBlock->info.rows, j, &pBlock->pBlockAgg[i]);
      }

      if (isNull) {
        colDataSetNULL(pDstCol, j - startIndex);
      } else {
        char* p = colDataGetData(pColData, j);
        code = colDataSetVal(pDstCol, j - startIndex, p, false);
        if (code) {
          break;
        }
      }
    }
  }

  pDst->info.rows = rowCount;
  *pResBlock = pDst;
  return code;
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
    if (pCol == NULL) {
      continue;
    }

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
        char*   pColData = pCol->pData + pCol->varmeta.offset[row];
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
      if (dataSize != 0) {
        // ubsan reports error if pCol->pData==NULL && dataSize==0
        memcpy(pStart, pCol->pData, dataSize);
      }
      pStart += dataSize;
    }
  }

  return 0;
}

int32_t blockDataFromBuf(SSDataBlock* pBlock, const char* buf) {
  int32_t numOfRows = *(int32_t*)buf;
  if (numOfRows == 0) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  int32_t code = blockDataEnsureCapacity(pBlock, numOfRows);
  if (code) {
    return code;
  }

  pBlock->info.rows = numOfRows;
  size_t      numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  const char* pStart = buf + sizeof(uint32_t);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (pCol == NULL) {
      continue;
    }

    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      size_t metaSize = pBlock->info.rows * sizeof(int32_t);
      char*  tmp = taosMemoryRealloc(pCol->varmeta.offset, metaSize);  // preview calloc is too small
      if (tmp == NULL) {
        return terrno;
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
          return terrno;
        }

        pCol->pData = tmp;
        pCol->varmeta.allocLen = colLength;
      }

      pCol->varmeta.length = colLength;
      if (pCol->varmeta.length > pCol->varmeta.allocLen) {
        return TSDB_CODE_FAILED;
      }
    }
    if (colLength != 0) {
      // ubsan reports error if colLength==0 && pCol->pData == 0
      memcpy(pCol->pData, pStart, colLength);
    }
    pStart += colLength;
  }

  return TSDB_CODE_SUCCESS;
}

static bool colDataIsNNull(const SColumnInfoData* pColumnInfoData, int32_t startIndex, uint32_t nRows) {
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
    if (pCol == NULL) {
      continue;
    }

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
          return terrno;
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
      if (pColInfo == NULL) {
        continue;
      }

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
  // | version | total length | total rows | blankFull | total columns | flag seg| block group id | column schema
  // | each column length |
  return sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) + sizeof(bool) + sizeof(int32_t) + sizeof(int32_t) +
         sizeof(uint64_t) + numOfCols * (sizeof(int8_t) + sizeof(int32_t)) + numOfCols * sizeof(int32_t);
}

double blockDataGetSerialRowSize(const SSDataBlock* pBlock) {
  double rowSize = 0;

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
    if (pColInfo == NULL) {
      continue;
    }

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

    __compar_fn_t fn;
    if (pOrder->compFn) {
      fn = pOrder->compFn;
    } else {
      fn = getKeyComparFunc(pColInfoData->info.type, pOrder->order);
    }

    int ret = fn(left1, right1);
    if (ret == 0) {
      continue;
    } else {
      return ret;
    }
  }

  return 0;
}

static void blockDataAssign(SColumnInfoData* pCols, const SSDataBlock* pDataBlock, const int32_t* index) {
  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDst = &pCols[i];
    SColumnInfoData* pSrc = taosArrayGet(pDataBlock->pDataBlock, i);
    if (pSrc == NULL) {
      continue;
    }

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
}

static int32_t createHelpColInfoData(const SSDataBlock* pDataBlock, SColumnInfoData** ppCols) {
  int32_t code = 0;
  int32_t rows = pDataBlock->info.capacity;
  size_t  numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  int32_t i = 0;

  *ppCols = NULL;

  SColumnInfoData* pCols = taosMemoryCalloc(numOfCols, sizeof(SColumnInfoData));
  if (pCols == NULL) {
    return terrno;
  }

  for (i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, i);
    if (pColInfoData == NULL) {
      continue;
    }

    pCols[i].info = pColInfoData->info;
    if (IS_VAR_DATA_TYPE(pCols[i].info.type)) {
      pCols[i].varmeta.offset = taosMemoryCalloc(rows, sizeof(int32_t));
      pCols[i].pData = taosMemoryCalloc(1, pColInfoData->varmeta.length);
      if (pCols[i].varmeta.offset == NULL || pCols[i].pData == NULL) {
        code = terrno;
        taosMemoryFree(pCols[i].varmeta.offset);
        taosMemoryFree(pCols[i].pData);
        goto _error;
      }

      pCols[i].varmeta.length = pColInfoData->varmeta.length;
      pCols[i].varmeta.allocLen = pCols[i].varmeta.length;
    } else {
      pCols[i].nullbitmap = taosMemoryCalloc(1, BitmapLen(rows));
      pCols[i].pData = taosMemoryCalloc(rows, pCols[i].info.bytes);
      if (pCols[i].nullbitmap == NULL || pCols[i].pData == NULL) {
        code = terrno;
        taosMemoryFree(pCols[i].nullbitmap);
        taosMemoryFree(pCols[i].pData);
        goto _error;
      }
    }
  }

  *ppCols = pCols;
  return code;

  _error:
  for(int32_t j = 0; j < i; ++j) {
    colDataDestroy(&pCols[j]);
  }

  taosMemoryFree(pCols);
  return code;
}

static void copyBackToBlock(SSDataBlock* pDataBlock, SColumnInfoData* pCols) {
  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, i);
    if (pColInfoData == NULL) {
      continue;
    }

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
    if (pInfo == NULL) {
      continue;
    }

    SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, pInfo->slotId);
    if (pColInfoData == NULL) {
      continue;
    }

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
        if (pColInfoData == NULL || pOrder == NULL) {
          return errno;
        }

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
    return terrno;
  }

  int64_t p0 = taosGetTimestampUs();

  SSDataBlockSortHelper helper = {.pDataBlock = pDataBlock, .orderInfo = pOrderInfo};
  for (int32_t i = 0; i < taosArrayGetSize(helper.orderInfo); ++i) {
    struct SBlockOrderInfo* pInfo = taosArrayGet(helper.orderInfo, i);
    if (pInfo == NULL) {
      continue;
    }

    pInfo->pColData = taosArrayGet(pDataBlock->pDataBlock, pInfo->slotId);
    if (pInfo->pColData == NULL) {
      continue;
    }
    pInfo->compFn = getKeyComparFunc(pInfo->pColData->info.type, pInfo->order);
  }

  terrno = 0;
  taosqsort_r(index, rows, sizeof(int32_t), &helper, dataBlockCompar);
  if (terrno) return terrno;

  int64_t p1 = taosGetTimestampUs();

  SColumnInfoData* pCols = NULL;
  int32_t code = createHelpColInfoData(pDataBlock, &pCols);
  if (code != 0) {
    destroyTupleIndex(index);
    return code;
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

  taosMemoryFreeClear(pDataBlock->pBlockAgg);

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    if (p == NULL) {
      continue;
    }

    colInfoDataCleanup(p, pInfo->capacity);
  }

  pInfo->rows = 0;
  pInfo->dataLoad = 0;
  pInfo->window.ekey = 0;
  pInfo->window.skey = 0;
}

void blockDataReset(SSDataBlock* pDataBlock) {
  SDataBlockInfo* pInfo = &pDataBlock->info;
  if (pInfo->capacity == 0) {
    return;
  }

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    if (p == NULL) {
      continue;
    }

    p->hasNull = false;
    p->reassigned = false;
    if (IS_VAR_DATA_TYPE(p->info.type)) {
      p->varmeta.length = 0;
    }
  }

  pInfo->rows = 0;
  pInfo->dataLoad = 0;
  pInfo->window.ekey = 0;
  pInfo->window.skey = 0;
  pInfo->id.uid = 0;
  pInfo->id.groupId = 0;
}

/*
 * NOTE: the type of the input column may be TSDB_DATA_TYPE_NULL, which is used to denote
 * the all NULL value in this column. It is an internal representation of all NULL value column, and no visible to
 * any users. The length of TSDB_DATA_TYPE_NULL is 0, and it is an special case.
 */
int32_t doEnsureCapacity(SColumnInfoData* pColumn, const SDataBlockInfo* pBlockInfo, uint32_t numOfRows,
                         bool clearPayload) {
  if ((numOfRows <= 0)|| (pBlockInfo && numOfRows <= pBlockInfo->capacity)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t existedRows = pBlockInfo ? pBlockInfo->rows : 0;

  if (IS_VAR_DATA_TYPE(pColumn->info.type)) {
    char* tmp = taosMemoryRealloc(pColumn->varmeta.offset, sizeof(int32_t) * numOfRows);
    if (tmp == NULL) {
      return terrno;
    }

    pColumn->varmeta.offset = (int32_t*)tmp;
    memset(&pColumn->varmeta.offset[existedRows], 0, sizeof(int32_t) * (numOfRows - existedRows));
  } else {
    // prepare for the null bitmap
    char* tmp = taosMemoryRealloc(pColumn->nullbitmap, BitmapLen(numOfRows));
    if (tmp == NULL) {
      return terrno;
    }

    int32_t oldLen = BitmapLen(existedRows);
    pColumn->nullbitmap = tmp;
    memset(&pColumn->nullbitmap[oldLen], 0, BitmapLen(numOfRows) - oldLen);
    if (pColumn->info.bytes == 0) {
      return TSDB_CODE_INVALID_PARA;
    }

    // here we employ the aligned malloc function, to make sure that the address of allocated memory is aligned
    // to MALLOC_ALIGN_BYTES
    tmp = taosMemoryMallocAlign(MALLOC_ALIGN_BYTES, numOfRows * pColumn->info.bytes);
    if (tmp == NULL) {
      return terrno;
    }
    // memset(tmp, 0, numOfRows * pColumn->info.bytes);

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
    if (p == NULL) {
      return terrno;
    }

    code = doEnsureCapacity(p, &pDataBlock->info, numOfRows, false);
    if (code) {
      return code;
    }
  }

  pDataBlock->info.capacity = numOfRows;
  return TSDB_CODE_SUCCESS;
}

void blockDataFreeRes(SSDataBlock* pBlock) {
  if (pBlock == NULL){
    return;
  }

  int32_t numOfOutput = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData* pColInfoData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
    if (pColInfoData == NULL) {
      continue;
    }

    colDataDestroy(pColInfoData);
  }

  taosArrayDestroy(pBlock->pDataBlock);
  pBlock->pDataBlock = NULL;

  taosMemoryFreeClear(pBlock->pBlockAgg);
  memset(&pBlock->info, 0, sizeof(SDataBlockInfo));
}

void blockDataDestroy(SSDataBlock* pBlock) {
  if (pBlock == NULL) {
    return;
  }

  if (IS_VAR_DATA_TYPE(pBlock->info.pks[0].type)) {
    taosMemoryFreeClear(pBlock->info.pks[0].pData);
    taosMemoryFreeClear(pBlock->info.pks[1].pData);
  }

  blockDataFreeRes(pBlock);
  taosMemoryFreeClear(pBlock);
}

// todo remove it
int32_t assignOneDataBlock(SSDataBlock* dst, const SSDataBlock* src) {
  int32_t code = 0;

  dst->info = src->info;
  dst->info.pks[0].pData = NULL;
  dst->info.pks[1].pData = NULL;
  dst->info.rows = 0;
  dst->info.capacity = 0;

  size_t numOfCols = taosArrayGetSize(src->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(src->pDataBlock, i);
    if (p == NULL) {
      return terrno;
    }

    SColumnInfoData  colInfo = {.hasNull = true, .info = p->info};
    code = blockDataAppendColInfo(dst, &colInfo);
    if (code) {
      return code;
    }
  }

  code = blockDataEnsureCapacity(dst, src->info.rows);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDst = taosArrayGet(dst->pDataBlock, i);
    SColumnInfoData* pSrc = taosArrayGet(src->pDataBlock, i);
    if (pSrc == NULL || pDst == NULL || (pSrc->pData == NULL && (!IS_VAR_DATA_TYPE(pSrc->info.type)))) {
      continue;
    }

    int32_t ret = colDataAssign(pDst, pSrc, src->info.rows, &src->info);
    if (ret < 0) {
      return ret;
    }
  }

  uint32_t cap = dst->info.capacity;
  dst->info = src->info;
  dst->info.pks[0].pData = NULL;
  dst->info.pks[1].pData = NULL;
  dst->info.capacity = cap;
  return code;
}

int32_t copyDataBlock(SSDataBlock* pDst, const SSDataBlock* pSrc) {
  blockDataCleanup(pDst);

  int32_t code = blockDataEnsureCapacity(pDst, pSrc->info.rows);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  size_t numOfCols = taosArrayGetSize(pSrc->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDstCol = taosArrayGet(pDst->pDataBlock, i);
    SColumnInfoData* pSrcCol = taosArrayGet(pSrc->pDataBlock, i);
    if (pDstCol == NULL || pSrcCol == NULL) {
      continue;
    }

    int32_t ret = colDataAssign(pDstCol, pSrcCol, pSrc->info.rows, &pSrc->info);
    if (ret < 0) {
      code = ret;
      return code;
    }
  }

  uint32_t cap = pDst->info.capacity;

  pDst->info = pSrc->info;
  pDst->info.pks[0].pData = NULL;
  pDst->info.pks[1].pData = NULL;
  code = copyPkVal(&pDst->info, &pSrc->info);
  if (code != TSDB_CODE_SUCCESS) {
    uError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

  pDst->info.capacity = cap;
  return code;
}

int32_t createSpecialDataBlock(EStreamType type, SSDataBlock** pBlock) {
  QRY_PARAM_CHECK(pBlock);

  int32_t      code = 0;
  SSDataBlock* p = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (p == NULL) {
    return terrno;
  }

  p->info.hasVarCol = false;
  p->info.id.groupId = 0;
  p->info.rows = 0;
  p->info.type = type;
  p->info.rowSize = sizeof(TSKEY) + sizeof(TSKEY) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(TSKEY) +
                    sizeof(TSKEY) + VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN;
  p->info.watermark = INT64_MIN;

  p->pDataBlock = taosArrayInit(6, sizeof(SColumnInfoData));
  if (p->pDataBlock == NULL) {
    taosMemoryFree(p);
    return terrno;
  }

  SColumnInfoData infoData = {0};
  infoData.info.type = TSDB_DATA_TYPE_TIMESTAMP;
  infoData.info.bytes = sizeof(TSKEY);

  // window start ts
  void* px = taosArrayPush(p->pDataBlock, &infoData);
  if (px == NULL) {
    code = errno;
    goto _err;
  }

  // window end ts
  px = taosArrayPush(p->pDataBlock, &infoData);
  if (px == NULL) {
    code = errno;
    goto _err;
  }

  infoData.info.type = TSDB_DATA_TYPE_UBIGINT;
  infoData.info.bytes = sizeof(uint64_t);

  // uid
  px = taosArrayPush(p->pDataBlock, &infoData);
  if (px == NULL) {
    code = errno;
    goto _err;
  }

  // group id
  px = taosArrayPush(p->pDataBlock, &infoData);
  if (px == NULL) {
    code = errno;
    goto _err;
  }

  infoData.info.type = TSDB_DATA_TYPE_TIMESTAMP;
  infoData.info.bytes = sizeof(TSKEY);

  // calculate start ts
  px = taosArrayPush(p->pDataBlock, &infoData);
  if (px == NULL) {
    code = errno;
    goto _err;
  }

  // calculate end ts
  px = taosArrayPush(p->pDataBlock, &infoData);
  if (px == NULL) {
    code = errno;
    goto _err;
  }

  // table name
  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN;
  px = taosArrayPush(p->pDataBlock, &infoData);
  if (px == NULL) {
    code = errno;
    goto _err;
  }

  *pBlock = p;
  return code;

_err:
  taosArrayDestroy(p->pDataBlock);
  taosMemoryFree(p);
  return code;
}

int32_t blockCopyOneRow(const SSDataBlock* pDataBlock, int32_t rowIdx, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);

  if (pDataBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SSDataBlock* pBlock = NULL;
  int32_t code = createDataBlock(&pBlock);
  if (code) {
    return code;
  }

  pBlock->info = pDataBlock->info;
  pBlock->info.pks[0].pData = NULL;
  pBlock->info.pks[1].pData = NULL;
  pBlock->info.rows = 0;
  pBlock->info.capacity = 0;

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    if (p == NULL) {
      blockDataDestroy(pBlock);
      return terrno;
    }

    SColumnInfoData  colInfo = {.hasNull = true, .info = p->info};
    code = blockDataAppendColInfo(pBlock, &colInfo);
    if (code) {
      blockDataDestroy(pBlock);
      return code;
    }
  }

  code = blockDataEnsureCapacity(pBlock, 1);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return code;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, i);
    SColumnInfoData* pSrc = taosArrayGet(pDataBlock->pDataBlock, i);
    if (pDst == NULL || pSrc == NULL) {
      blockDataDestroy(pBlock);
      return terrno;
    }

    bool  isNull = colDataIsNull(pSrc, pDataBlock->info.rows, rowIdx, NULL);
    void* pData = NULL;
    if (!isNull) {
      pData = colDataGetData(pSrc, rowIdx);
    }

    code = colDataSetVal(pDst, 0, pData, isNull);
    if (code) {
      blockDataDestroy(pBlock);
      return code;
    }
  }

  pBlock->info.rows = 1;

  *pResBlock = pBlock;
  return code;
}

int32_t copyPkVal(SDataBlockInfo* pDst, const SDataBlockInfo* pSrc) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (!IS_VAR_DATA_TYPE(pSrc->pks[0].type)) {
    return code;
  }

  // prepare the pk buffer if needed
  SValue* p = &pDst->pks[0];

  p->type = pSrc->pks[0].type;
  p->pData = taosMemoryCalloc(1, pSrc->pks[0].nData);
  QUERY_CHECK_NULL(p->pData, code, lino, _end, terrno);

  p->nData = pSrc->pks[0].nData;
  memcpy(p->pData, pSrc->pks[0].pData, p->nData);

  p = &pDst->pks[1];
  p->type = pSrc->pks[1].type;
  p->pData = taosMemoryCalloc(1, pSrc->pks[1].nData);
  QUERY_CHECK_NULL(p->pData, code, lino, _end, terrno);

  p->nData = pSrc->pks[1].nData;
  memcpy(p->pData, pSrc->pks[1].pData, p->nData);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t createOneDataBlock(const SSDataBlock* pDataBlock, bool copyData, SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);
  if (pDataBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SSDataBlock* pDstBlock = NULL;
  int32_t code = createDataBlock(&pDstBlock);
  if (code) {
    return code;
  }

  pDstBlock->info = pDataBlock->info;
  pDstBlock->info.pks[0].pData = NULL;
  pDstBlock->info.pks[1].pData = NULL;

  pDstBlock->info.rows = 0;
  pDstBlock->info.capacity = 0;
  pDstBlock->info.rowSize = 0;
  pDstBlock->info.id = pDataBlock->info.id;
  pDstBlock->info.blankFill = pDataBlock->info.blankFill;

  size_t numOfCols = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pDataBlock->pDataBlock, i);
    if (p == NULL) {
      blockDataDestroy(pDstBlock);
      return terrno;
    }

    SColumnInfoData  colInfo = {.hasNull = true, .info = p->info};
    code = blockDataAppendColInfo(pDstBlock, &colInfo);
    if (code) {
      blockDataDestroy(pDstBlock);
      return code;
    }
  }

  code = copyPkVal(&pDstBlock->info, &pDataBlock->info);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pDstBlock);
    uError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

  if (copyData) {
    code = blockDataEnsureCapacity(pDstBlock, pDataBlock->info.rows);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pDstBlock);
      return code;
    }

    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pDst = taosArrayGet(pDstBlock->pDataBlock, i);
      SColumnInfoData* pSrc = taosArrayGet(pDataBlock->pDataBlock, i);
      if (pDst == NULL) {
        blockDataDestroy(pDstBlock);
        uError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        return terrno;
      }

      if (pSrc == NULL) {
        blockDataDestroy(pDstBlock);
        uError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        return terrno;
      }

      int32_t ret = colDataAssign(pDst, pSrc, pDataBlock->info.rows, &pDataBlock->info);
      if (ret < 0) {
        code = ret;
        blockDataDestroy(pDstBlock);
        return code;
      }
    }

    pDstBlock->info.rows = pDataBlock->info.rows;
    pDstBlock->info.capacity = pDataBlock->info.rows;
  }

  *pResBlock = pDstBlock;
  return code;
}

int32_t createDataBlock(SSDataBlock** pResBlock) {
  QRY_PARAM_CHECK(pResBlock);
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) {
    return terrno;
  }

  pBlock->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));
  if (pBlock->pDataBlock == NULL) {
    int32_t code = terrno;
    taosMemoryFree(pBlock);
    return code;
  }

  *pResBlock = pBlock;
  return 0;
}

int32_t blockDataAppendColInfo(SSDataBlock* pBlock, SColumnInfoData* pColInfoData) {
  if (pBlock->pDataBlock == NULL) {
    pBlock->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));
    if (pBlock->pDataBlock == NULL) {
      return terrno;
    }
  }

  void* p = taosArrayPush(pBlock->pDataBlock, pColInfoData);
  if (p == NULL) {
    return terrno;
  }

  // todo disable it temporarily
  //  A S S E R T(pColInfoData->info.type != 0);
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

int32_t bdGetColumnInfoData(const SSDataBlock* pBlock, int32_t index, SColumnInfoData** pColInfoData) {
  int32_t code = 0;
  QRY_PARAM_CHECK(pColInfoData);

  if (index >= taosArrayGetSize(pBlock->pDataBlock)) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pColInfoData = taosArrayGet(pBlock->pDataBlock, index);
  if (*pColInfoData == NULL) {
    code = terrno;
  }

  return code;
}

size_t blockDataGetCapacityInRow(const SSDataBlock* pBlock, size_t pageSize, int32_t extraSize) {
  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  int32_t payloadSize = pageSize - extraSize;
  int32_t rowSize = pBlock->info.rowSize;
  int32_t nRows = payloadSize / rowSize;
  if (nRows < 1) {
    uError("rows %d in page is too small, payloadSize:%d, rowSize:%d", nRows, payloadSize, rowSize);
    terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return -1;
  }

  int32_t numVarCols = 0;
  int32_t numFixCols = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (pCol == NULL) {
      return -1;
    }

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
    // data size + var data type columns offset + fixed data type columns bitmap len
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
  if (newRows > nRows || newRows < 1) {
    uError("invalid newRows:%d, nRows:%d", newRows, nRows);
    terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return -1;
  }

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
    (void) memmove(nullBitmap, nullBitmap + n / 8, newLen);
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
    (void) memmove(pColInfoData->pData, pColInfoData->pData + dataOffset, dataLen);
  }

  (void) memmove(pColInfoData->varmeta.offset, &pColInfoData->varmeta.offset[start], (end - start) * sizeof(int32_t));
  return dataLen;
}

static void colDataTrimFirstNRows(SColumnInfoData* pColInfoData, size_t n, size_t total) {
  if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
    // pColInfoData->varmeta.length = colDataMoveVarData(pColInfoData, n, total);
    (void) memmove(pColInfoData->varmeta.offset, &pColInfoData->varmeta.offset[n], (total - n) * sizeof(int32_t));

    // clear the offset value of the unused entries.
    memset(&pColInfoData->varmeta.offset[total - n], 0, n);
  } else {
    int32_t bytes = pColInfoData->info.bytes;
    (void) memmove(pColInfoData->pData, ((char*)pColInfoData->pData + n * bytes), (total - n) * bytes);
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
      if (pColInfoData == NULL) {
        return terrno;
      }

      colDataTrimFirstNRows(pColInfoData, n, pBlock->info.rows);
    }

    pBlock->info.rows -= n;
  }
  return TSDB_CODE_SUCCESS;
}

static void colDataKeepFirstNRows(SColumnInfoData* pColInfoData, size_t n, size_t total) {
  if (n >= total || n == 0) return;
  if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
    if (pColInfoData->varmeta.length != 0) {
      int32_t newLen = pColInfoData->varmeta.offset[n];
      if (-1 == newLen) {
        for (int i = n - 1; i >= 0; --i) {
          newLen = pColInfoData->varmeta.offset[i];
          if (newLen != -1) {
            if (pColInfoData->info.type == TSDB_DATA_TYPE_JSON) {
              newLen += getJsonValueLen(pColInfoData->pData + newLen);
            } else {
              newLen += varDataTLen(pColInfoData->pData + newLen);
            }
            break;
          }
        }
      }
      if (newLen <= -1) {
        uFatal("colDataKeepFirstNRows: newLen:%d  old:%d", newLen, pColInfoData->varmeta.length);
      } else {
        pColInfoData->varmeta.length = newLen;
      }
    }
    // pColInfoData->varmeta.length = colDataMoveVarData(pColInfoData, 0, n);
    memset(&pColInfoData->varmeta.offset[n], 0, total - n);
  }
}

void blockDataKeepFirstNRows(SSDataBlock* pBlock, size_t n) {
  if (n == 0) {
    blockDataEmpty(pBlock);
    return ;
  }

  if (pBlock->info.rows <= n) {
    return ;
  } else {
    size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
      if (pColInfoData == NULL) {
        continue;
      }

      colDataKeepFirstNRows(pColInfoData, n, pBlock->info.rows);
    }

    pBlock->info.rows = n;
  }
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
    if (pColData == NULL) {
      return terrno;
    }

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
        char*   pData = pColData->pData + pColData->varmeta.offset[row];
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
  int32_t sz = 0;
  int16_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  buf = taosDecodeFixedU64(buf, &pBlock->info.id.uid);
  buf = taosDecodeFixedI16(buf, &numOfCols);
  buf = taosDecodeFixedI16(buf, &pBlock->info.hasVarCol);
  buf = taosDecodeFixedI64(buf, &pBlock->info.rows);
  buf = taosDecodeFixedI32(buf, &sz);

  pBlock->pDataBlock = taosArrayInit(sz, sizeof(SColumnInfoData));
  if (pBlock->pDataBlock == NULL) {
    return NULL;
  }

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
    if(buf == NULL) {
      uError("failed to decode null bitmap/offset, type:%d", data.info.type);
      goto _error;
    }

    int32_t len = 0;
    buf = taosDecodeFixedI32(buf, &len);
    buf = taosDecodeBinary(buf, (void**)&data.pData, len);
    if (buf == NULL) {
      uError("failed to decode data, type:%d", data.info.type);
      goto _error;
    }
    if (IS_VAR_DATA_TYPE(data.info.type)) {
      data.varmeta.length = len;
      data.varmeta.allocLen = len;
    }

    void* px = taosArrayPush(pBlock->pDataBlock, &data);
    if (px == NULL) {
      return NULL;
    }
  }

  return (void*)buf;
_error:
  for (int32_t i = 0; i < sz; ++i) {
    SColumnInfoData* pColInfoData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
    if (pColInfoData == NULL) {
      break;
    }
    colDataDestroy(pColInfoData);
  }
  return NULL;
}

static int32_t formatTimestamp(char* buf, size_t cap, int64_t val, int precision) {
  time_t  tt;
  int32_t ms = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
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
  if (taosLocalTime(&tt, &ptm, buf, cap) == NULL) {
    code =  TSDB_CODE_INTERNAL_ERROR;
    TSDB_CHECK_CODE(code, lino, _end);
  }

  size_t pos = strftime(buf, cap, "%Y-%m-%d %H:%M:%S", &ptm);
  if (pos == 0) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    TSDB_CHECK_CODE(code, lino, _end);
  }
  int32_t nwritten = 0;
  if (precision == TSDB_TIME_PRECISION_NANO) {
    nwritten = snprintf(buf + pos, cap - pos, ".%09d", ms);
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    nwritten = snprintf(buf + pos, cap - pos, ".%06d", ms);
  } else {
    nwritten = snprintf(buf + pos, cap - pos, ".%03d", ms);
  }

  if (nwritten >= cap - pos) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// for debug
int32_t dumpBlockData(SSDataBlock* pDataBlock, const char* flag, char** pDataBuf, const char* taskIdStr) {
  int32_t lino = 0;
  int32_t size = 2048 * 1024;
  int32_t code = 0;
  char*   dumpBuf = NULL;
  char    pBuf[TD_TIME_STR_LEN] = {0};
  int32_t rows = pDataBlock->info.rows;
  int32_t len = 0;

  dumpBuf = taosMemoryCalloc(size, 1);
  if (dumpBuf == NULL) {
    return terrno;
  }

  int32_t colNum = taosArrayGetSize(pDataBlock->pDataBlock);
  len += tsnprintf(dumpBuf + len, size - len,
                  "%s===stream===%s|block type %d|child id %d|group id:%" PRIu64 "|uid:%" PRId64 "|rows:%" PRId64
                  "|version:%" PRIu64 "|cal start:%" PRIu64 "|cal end:%" PRIu64 "|tbl:%s\n",
                  taskIdStr, flag, (int32_t)pDataBlock->info.type, pDataBlock->info.childId,
                  pDataBlock->info.id.groupId, pDataBlock->info.id.uid, pDataBlock->info.rows, pDataBlock->info.version,
                  pDataBlock->info.calWin.skey, pDataBlock->info.calWin.ekey, pDataBlock->info.parTbName);
  if (len >= size - 1) {
    goto _exit;
  }

  for (int32_t j = 0; j < rows; j++) {
    len += tsnprintf(dumpBuf + len, size - len, "%s|", flag);
    if (len >= size - 1) {
      goto _exit;
    }

    for (int32_t k = 0; k < colNum; k++) {
      SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
      if (pColInfoData == NULL) {
        code = terrno;
        lino = __LINE__;
        goto _exit;
      }

      if (colDataIsNull(pColInfoData, rows, j, NULL) || !pColInfoData->pData) {
        len += tsnprintf(dumpBuf + len, size - len, " %15s |", "NULL");
        if (len >= size - 1) goto _exit;
        continue;
      }

      void* var = colDataGetData(pColInfoData, j);
      switch (pColInfoData->info.type) {
        case TSDB_DATA_TYPE_TIMESTAMP:
          memset(pBuf, 0, sizeof(pBuf));
          code = formatTimestamp(pBuf, sizeof(pBuf), *(uint64_t*)var, pColInfoData->info.precision);
          if (code != TSDB_CODE_SUCCESS) {
            TAOS_UNUSED(tsnprintf(pBuf, sizeof(pBuf), "NaN"));
          }
          len += tsnprintf(dumpBuf + len, size - len, " %25s |", pBuf);
          if (len >= size - 1) goto _exit;
          break;
        case TSDB_DATA_TYPE_TINYINT:
          len += tsnprintf(dumpBuf + len, size - len, " %15d |", *(int8_t*)var);
          if (len >= size - 1) goto _exit;
          break;
        case TSDB_DATA_TYPE_UTINYINT:
          len += tsnprintf(dumpBuf + len, size - len, " %15d |", *(uint8_t*)var);
          if (len >= size - 1) goto _exit;
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          len += tsnprintf(dumpBuf + len, size - len, " %15d |", *(int16_t*)var);
          if (len >= size - 1) goto _exit;
          break;
        case TSDB_DATA_TYPE_USMALLINT:
          len += tsnprintf(dumpBuf + len, size - len, " %15d |", *(uint16_t*)var);
          if (len >= size - 1) goto _exit;
          break;
        case TSDB_DATA_TYPE_INT:
          len += tsnprintf(dumpBuf + len, size - len, " %15d |", *(int32_t*)var);
          if (len >= size - 1) goto _exit;
          break;
        case TSDB_DATA_TYPE_UINT:
          len += tsnprintf(dumpBuf + len, size - len, " %15u |", *(uint32_t*)var);
          if (len >= size - 1) goto _exit;
          break;
        case TSDB_DATA_TYPE_BIGINT:
          len += tsnprintf(dumpBuf + len, size - len, " %15" PRId64 " |", *(int64_t*)var);
          if (len >= size - 1) goto _exit;
          break;
        case TSDB_DATA_TYPE_UBIGINT:
          len += tsnprintf(dumpBuf + len, size - len, " %15" PRIu64 " |", *(uint64_t*)var);
          if (len >= size - 1) goto _exit;
          break;
        case TSDB_DATA_TYPE_FLOAT:
          len += tsnprintf(dumpBuf + len, size - len, " %15f |", *(float*)var);
          if (len >= size - 1) goto _exit;
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          len += tsnprintf(dumpBuf + len, size - len, " %15f |", *(double*)var);
          if (len >= size - 1) goto _exit;
          break;
        case TSDB_DATA_TYPE_BOOL:
          len += tsnprintf(dumpBuf + len, size - len, " %15d |", *(bool*)var);
          if (len >= size - 1) goto _exit;
          break;
        case TSDB_DATA_TYPE_VARCHAR:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_GEOMETRY: {
          memset(pBuf, 0, sizeof(pBuf));
          char*   pData = colDataGetVarData(pColInfoData, j);
          int32_t dataSize = TMIN(sizeof(pBuf), varDataLen(pData));
          dataSize = TMIN(dataSize, 50);
          memcpy(pBuf, varDataVal(pData), dataSize);
          len += tsnprintf(dumpBuf + len, size - len, " %15s |", pBuf);
          if (len >= size - 1) goto _exit;
        } break;
        case TSDB_DATA_TYPE_NCHAR: {
          char*   pData = colDataGetVarData(pColInfoData, j);
          int32_t dataSize = TMIN(sizeof(pBuf), varDataLen(pData));
          memset(pBuf, 0, sizeof(pBuf));
          code = taosUcs4ToMbs((TdUcs4*)varDataVal(pData), dataSize, pBuf);
          if (code < 0) {
            uError("func %s failed to convert to ucs charset since %s", __func__, tstrerror(code));
            lino = __LINE__;
            goto _exit;
          } else { // reset the length value
            code = TSDB_CODE_SUCCESS;
          }
          len += tsnprintf(dumpBuf + len, size - len, " %15s |", pBuf);
          if (len >= size - 1) goto _exit;
        } break;
      }
    }
    len += tsnprintf(dumpBuf + len, size - len, "%d\n", j);
    if (len >= size - 1) goto _exit;
  }
  len += tsnprintf(dumpBuf + len, size - len, "%s |end\n", flag);

_exit:
  if (code == TSDB_CODE_SUCCESS) {
    *pDataBuf = dumpBuf;
    dumpBuf = NULL;
  } else {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    if (dumpBuf) {
      taosMemoryFree(dumpBuf);
    }
  }
  return code;
}

int32_t buildSubmitReqFromDataBlock(SSubmitReq2** ppReq, const SSDataBlock* pDataBlock, const STSchema* pTSchema,
                                    int64_t uid, int32_t vgId, tb_uid_t suid) {
  SSubmitReq2* pReq = *ppReq;
  SArray*      pVals = NULL;
  int32_t      sz = 1;
  int32_t      code = 0;
  *ppReq = NULL;
  terrno = 0;

  if (NULL == pReq) {
    if (!(pReq = taosMemoryMalloc(sizeof(SSubmitReq2)))) {
      code = terrno;
      goto _end;
    }

    if (!(pReq->aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData)))) {
      code = terrno;
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
    if (colNum != pTSchema->numOfCols) {
      uError("colNum %d is not equal to numOfCols %d", colNum, pTSchema->numOfCols);
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _end;
    }

    SSubmitTbData tbData = {0};

    if (!(tbData.aRowP = taosArrayInit(rows, sizeof(SRow*)))) {
      code = terrno;
      goto _end;
    }

    tbData.suid = suid;
    tbData.uid = uid;
    tbData.sver = pTSchema->version;

    if (!pVals && !(pVals = taosArrayInit(colNum, sizeof(SColVal)))) {
      code = terrno;
      taosArrayDestroy(tbData.aRowP);
      goto _end;
    }

    for (int32_t j = 0; j < rows; ++j) {  // iterate by row

      taosArrayClear(pVals);

      bool    isStartKey = false;
      int32_t offset = 0;
      for (int32_t k = 0; k < colNum; ++k) {  // iterate by column
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
        if (pColInfoData == NULL) {
          return terrno;
        }

        const STColumn*  pCol = &pTSchema->columns[k];
        void*            var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);

        switch (pColInfoData->info.type) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            if (pColInfoData->info.type != pCol->type) {
              uError("colType:%d mismatch with sechma colType:%d", pColInfoData->info.type, pCol->type);
              terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
              return terrno;
            }
            if (!isStartKey) {
              isStartKey = true;
              if (PRIMARYKEY_TIMESTAMP_COL_ID != pCol->colId) {
                uError("the first timestamp colId %d is not primary colId", pCol->colId);
                terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
                return terrno;
              }
              SColVal cv = COL_VAL_VALUE(pCol->colId, ((SValue){.type = pCol->type, .val = *(TSKEY*)var}));
              void*   px = taosArrayPush(pVals, &cv);
              if (px == NULL) {
                return terrno;
              }

            } else if (colDataIsNull_s(pColInfoData, j)) {
              SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
              void*   px = taosArrayPush(pVals, &cv);
              if (px == NULL) {
                return terrno;
              }
            } else {
              SColVal cv = COL_VAL_VALUE(pCol->colId, ((SValue){.type = pCol->type, .val = *(int64_t*)var}));
              void*   px = taosArrayPush(pVals, &cv);
              if (px == NULL) {
                return terrno;
              }
            }
            break;
          case TSDB_DATA_TYPE_NCHAR:
          case TSDB_DATA_TYPE_VARBINARY:
          case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
            if (pColInfoData->info.type != pCol->type) {
              uError("colType:%d mismatch with sechma colType:%d", pColInfoData->info.type, pCol->type);
              terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
              return terrno;
            }
            if (colDataIsNull_s(pColInfoData, j)) {
              SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
              void* px = taosArrayPush(pVals, &cv);
              if (px == NULL) {
                goto _end;
              }
            } else {
              void*  data = colDataGetVarData(pColInfoData, j);
              SValue sv = (SValue){
                  .type = pCol->type, .nData = varDataLen(data), .pData = (uint8_t*) varDataVal(data)};  // address copy, no value
              SColVal cv = COL_VAL_VALUE(pCol->colId, sv);
              void* px = taosArrayPush(pVals, &cv);
              if (px == NULL) {
                code = terrno;
                goto _end;
              }
            }
            break;
          }
          case TSDB_DATA_TYPE_DECIMAL:
          case TSDB_DATA_TYPE_BLOB:
          case TSDB_DATA_TYPE_JSON:
          case TSDB_DATA_TYPE_MEDIUMBLOB:
            uError("the column type %" PRIi16 " is defined but not implemented yet", pColInfoData->info.type);
            terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
            return terrno;
            break;
          default:
            if (pColInfoData->info.type < TSDB_DATA_TYPE_MAX && pColInfoData->info.type > TSDB_DATA_TYPE_NULL) {
              if (colDataIsNull_s(pColInfoData, j)) {
                SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);  // should use pCol->type
                void* px = taosArrayPush(pVals, &cv);
                if (px == NULL) {
                  goto _end;
                }
              } else {
                SValue sv = {.type = pCol->type};
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
                SColVal cv = COL_VAL_VALUE(pCol->colId, sv);
                void* px = taosArrayPush(pVals, &cv);
                if (px == NULL) {
                  code = terrno;
                  goto _end;
                }
              }
            } else {
              uError("the column type %" PRIi16 " is undefined\n", pColInfoData->info.type);
              terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
              return terrno;
            }
            break;
        }
      }
      SRow* pRow = NULL;
      if ((code = tRowBuild(pVals, pTSchema, &pRow)) < 0) {
        tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }

      void* px = taosArrayPush(tbData.aRowP, &pRow);
      if (px == NULL) {
        code = terrno;
        goto _end;
      }
    }

    void* px = taosArrayPush(pReq->aSubmitTbData, &tbData);
    if (px == NULL) {
      code = terrno;
      goto _end;
    }
  }

_end:
  taosArrayDestroy(pVals);
  if (code != 0) {
    if (pReq) {
      tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
      taosMemoryFreeClear(pReq);
    }
  } else {
    *ppReq = pReq;
  }

  return code;
}

// Construct the child table name in the form of <ctbName>_<stbName>_<groupId> and store it in `ctbName`.
// If the name length exceeds TSDB_TABLE_NAME_LEN, first convert <stbName>_<groupId> to an MD5 value and then
// concatenate. If the length is still too long, convert <ctbName> to an MD5 value as well.
int32_t buildCtbNameAddGroupId(const char* stbName, char* ctbName, uint64_t groupId, size_t cap) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  char      tmp[TSDB_TABLE_NAME_LEN] = {0};
  char*     suffix = tmp;
  size_t    suffixCap = sizeof(tmp);
  size_t    suffixLen = 0;
  size_t    prefixLen = 0;
  T_MD5_CTX context;

  if (ctbName == NULL || cap < TSDB_TABLE_NAME_LEN) {
    code = TSDB_CODE_INTERNAL_ERROR;
    TSDB_CHECK_CODE(code, lino, _end);
  }

  prefixLen = strlen(ctbName);

  if (stbName == NULL) {
    suffixLen = snprintf(suffix, suffixCap, "%" PRIu64, groupId);
    if (suffixLen >= suffixCap) {
      code = TSDB_CODE_INTERNAL_ERROR;
      TSDB_CHECK_CODE(code, lino, _end);
    }
  } else {
    int32_t i = strlen(stbName) - 1;
    for (; i >= 0; i--) {
      if (stbName[i] == '.') {
        break;
      }
    }
    suffixLen = snprintf(suffix, suffixCap, "%s_%" PRIu64, stbName + i + 1, groupId);
    if (suffixLen >= suffixCap) {
      suffixCap = suffixLen + 1;
      suffix = taosMemoryMalloc(suffixCap);
      TSDB_CHECK_NULL(suffix, code, lino, _end, TSDB_CODE_OUT_OF_MEMORY);
      suffixLen = snprintf(suffix, suffixCap, "%s_%" PRIu64, stbName + i + 1, groupId);
      if (suffixLen >= suffixCap) {
        code = TSDB_CODE_INTERNAL_ERROR;
        TSDB_CHECK_CODE(code, lino, _end);
      }
    }
  }

  if (prefixLen + suffixLen + 1 >= TSDB_TABLE_NAME_LEN) {
    // If the name length exceeeds the limit, convert the suffix to MD5 value.
    tMD5Init(&context);
    tMD5Update(&context, (uint8_t*)suffix, suffixLen);
    tMD5Final(&context);
    suffixLen = snprintf(suffix, suffixCap, "%016" PRIx64 "%016" PRIx64, *(uint64_t*)context.digest,
                         *(uint64_t*)(context.digest + 8));
    if (suffixLen >= suffixCap) {
      code = TSDB_CODE_INTERNAL_ERROR;
      TSDB_CHECK_CODE(code, lino, _end);
    }
  }

  if (prefixLen + suffixLen + 1 >= TSDB_TABLE_NAME_LEN) {
    // If the name is still too long, convert the ctbName to MD5 value.
    tMD5Init(&context);
    tMD5Update(&context, (uint8_t*)ctbName, prefixLen);
    tMD5Final(&context);
    prefixLen = snprintf(ctbName, cap, "t_%016" PRIx64 "%016" PRIx64, *(uint64_t*)context.digest,
                         *(uint64_t*)(context.digest + 8));
    if (prefixLen >= cap) {
      code = TSDB_CODE_INTERNAL_ERROR;
      TSDB_CHECK_CODE(code, lino, _end);
    }
  }

  if (prefixLen + suffixLen + 1 >= TSDB_TABLE_NAME_LEN) {
    code = TSDB_CODE_INTERNAL_ERROR;
    TSDB_CHECK_CODE(code, lino, _end);
  }

  ctbName[prefixLen] = '_';
  tstrncpy(&ctbName[prefixLen + 1], suffix, cap - prefixLen - 1);

  for (char* p = ctbName; *p; ++p) {
    if (*p == '.') *p = '_';
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (suffix != tmp) {
    taosMemoryFree(suffix);
  }
  return code;
}

// auto stream subtable name starts with 't_', followed by the first segment of MD5 digest for group vals.
// the total length is fixed to be 34 bytes.
bool isAutoTableName(char* ctbName) { return (strlen(ctbName) == 34 && ctbName[0] == 't' && ctbName[1] == '_'); }

bool alreadyAddGroupId(char* ctbName, int64_t groupId) {
  char tmp[64] = {0};
  snprintf(tmp, sizeof(tmp), "%" PRIu64, groupId);
  size_t len1 = strlen(ctbName);
  size_t len2 = strlen(tmp);
  if (len1 < len2) return false;
  return memcmp(ctbName + len1 - len2, tmp, len2) == 0;
}

int32_t buildCtbNameByGroupId(const char* stbFullName, uint64_t groupId, char** pName) {
  QRY_PARAM_CHECK(pName);

  char* pBuf = taosMemoryCalloc(1, TSDB_TABLE_NAME_LEN + 1);
  if (!pBuf) {
    return terrno;
  }

  int32_t code = buildCtbNameByGroupIdImpl(stbFullName, groupId, pBuf);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pBuf);
  } else {
    *pName = pBuf;
  }

  return code;
}

int32_t buildCtbNameByGroupIdImpl(const char* stbFullName, uint64_t groupId, char* cname) {
  if (stbFullName[0] == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  SArray* tags = taosArrayInit(0, sizeof(SSmlKv));
  if (tags == NULL) {
    return terrno;
  }

  if (cname == NULL) {
    taosArrayDestroy(tags);
    return TSDB_CODE_INVALID_PARA;
  }

  int8_t      type = TSDB_DATA_TYPE_UBIGINT;
  const char* name = "group_id";
  int32_t     len = strlen(name);

  SSmlKv pTag = {.key = name, .keyLen = len, .type = type, .u = groupId, .length = sizeof(uint64_t)};
  void*  px = taosArrayPush(tags, &pTag);
  if (px == NULL) {
    return terrno;
  }

  RandTableName rname = {
      .tags = tags, .stbFullName = stbFullName, .stbFullNameLen = strlen(stbFullName), .ctbShortName = cname};

  int32_t code = buildChildTableName(&rname);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  taosArrayDestroy(tags);
  if ((rname.ctbShortName && rname.ctbShortName[0]) == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  return code;
}

// return length of encoded data, return -1 if failed
int32_t blockEncode(const SSDataBlock* pBlock, char* data, size_t dataBuflen, int32_t numOfCols) {
  int32_t code = blockDataCheck(pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return -1;
  }

  int32_t dataLen = 0;

  // todo extract method
  int32_t* version = (int32_t*)data;
  *version = BLOCK_VERSION_1;
  data += sizeof(int32_t);

  int32_t* actualLen = (int32_t*)data;
  data += sizeof(int32_t);

  int32_t* rows = (int32_t*)data;
  *rows = pBlock->info.rows;
  data += sizeof(int32_t);
  if (*rows <= 0) {
    uError("Invalid rows %d in block", *rows);
    terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return -1;
  }

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
    if (pColInfoData == NULL) {
      return -1;
    }

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
    if (pColRes == NULL) {
      return -1;
    }

    // copy the null bitmap
    size_t metaSize = 0;
    if (IS_VAR_DATA_TYPE(pColRes->info.type)) {
      metaSize = numOfRows * sizeof(int32_t);
      if(dataLen + metaSize > dataBuflen) goto _exit;
      memcpy(data, pColRes->varmeta.offset, metaSize);
    } else {
      metaSize = BitmapLen(numOfRows);
      if(dataLen + metaSize > dataBuflen) goto _exit;
      memcpy(data, pColRes->nullbitmap, metaSize);
    }

    data += metaSize;
    dataLen += metaSize;

    if (pColRes->reassigned && IS_VAR_DATA_TYPE(pColRes->info.type)) {
      colSizes[col] = 0;
      for (int32_t row = 0; row < numOfRows; ++row) {
        char*   pColData = pColRes->pData + pColRes->varmeta.offset[row];
        int32_t colSize = 0;
        if (pColRes->info.type == TSDB_DATA_TYPE_JSON) {
          colSize = getJsonValueLen(pColData);
        } else {
          colSize = varDataTLen(pColData);
        }
        colSizes[col] += colSize;
        dataLen += colSize;
        if(dataLen > dataBuflen) goto _exit;
        (void) memmove(data, pColData, colSize);
        data += colSize;
      }
    } else {
      colSizes[col] = colDataGetLength(pColRes, numOfRows);
      dataLen += colSizes[col];
      if(dataLen > dataBuflen) goto _exit;
      if (pColRes->pData != NULL) {
        (void) memmove(data, pColRes->pData, colSizes[col]);
      }
      data += colSizes[col];
    }

    if (colSizes[col] <= 0 && !colDataIsNull_s(pColRes, 0) && pColRes->info.type != TSDB_DATA_TYPE_NULL) {
      uError("Invalid colSize:%d colIdx:%d colType:%d while encoding block", colSizes[col], col, pColRes->info.type);
      terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      return -1;
    }
    
    colSizes[col] = htonl(colSizes[col]);
    //    uError("blockEncode col bytes:%d, type:%d, size:%d, htonl size:%d", pColRes->info.bytes, pColRes->info.type,
    //    htonl(colSizes[col]), colSizes[col]);
  }

  bool* blankFill = (bool*)data;
  *blankFill = pBlock->info.blankFill;
  data += sizeof(bool);

  *actualLen = dataLen;
  *groupId = pBlock->info.id.groupId;
  if (dataLen > dataBuflen) goto _exit;

  return dataLen;

_exit:
  uError("blockEncode dataLen:%d, dataBuflen:%zu", dataLen, dataBuflen);
  terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  return -1;
}

int32_t blockDecode(SSDataBlock* pBlock, const char* pData, const char** pEndPos) {
  const char* pStart = pData;

  int32_t version = *(int32_t*)pStart;
  pStart += sizeof(int32_t);

  // total length sizeof(int32_t)
  int32_t dataLen = *(int32_t*)pStart;
  pStart += sizeof(int32_t);

  // total rows sizeof(int32_t)
  int32_t numOfRows = *(int32_t*)pStart;
  pStart += sizeof(int32_t);
  if (numOfRows <= 0) {
    uError("block decode numOfRows:%d error", numOfRows);
    terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return terrno;
  }

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
    if (pBlock->pDataBlock == NULL) {
      return terrno;
    }
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    if (pColInfoData == NULL) {
      return terrno;
    }

    pColInfoData->info.type = *(int8_t*)pStart;
    pStart += sizeof(int8_t);

    pColInfoData->info.bytes = *(int32_t*)pStart;
    pStart += sizeof(int32_t);

    if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
      pBlock->info.hasVarCol = true;
    }
  }

  int32_t code = blockDataEnsureCapacity(pBlock, numOfRows);
  if (code) {
    return code;
  }

  int32_t* colLen = (int32_t*)pStart;
  pStart += sizeof(int32_t) * numOfCols;

  for (int32_t i = 0; i < numOfCols; ++i) {
    colLen[i] = htonl(colLen[i]);
    if (colLen[i] < 0) {
      uError("block decode colLen:%d error, colIdx:%d", colLen[i], i);
      terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      return terrno;
    }

    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    if (pColInfoData == NULL) {
      return terrno;
    }

    if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
      memcpy(pColInfoData->varmeta.offset, pStart, sizeof(int32_t) * numOfRows);
      pStart += sizeof(int32_t) * numOfRows;

      if (colLen[i] > 0 && pColInfoData->varmeta.allocLen < colLen[i]) {
        char* tmp = taosMemoryRealloc(pColInfoData->pData, colLen[i]);
        if (tmp == NULL) {
          return terrno;
        }

        pColInfoData->pData = tmp;
        pColInfoData->varmeta.allocLen = colLen[i];
      }

      pColInfoData->varmeta.length = colLen[i];
    } else {
      memcpy(pColInfoData->nullbitmap, pStart, BitmapLen(numOfRows));
      pStart += BitmapLen(numOfRows);
    }

    // TODO
    // setting this flag to true temporarily so aggregate function on stable will
    // examine NULL value for non-primary key column
    pColInfoData->hasNull = true;

    if (colLen[i] > 0) {
      memcpy(pColInfoData->pData, pStart, colLen[i]);
    } else if (!colDataIsNull_s(pColInfoData, 0) && pColInfoData->info.type != TSDB_DATA_TYPE_NULL) {
      uError("block decode colLen:%d error, colIdx:%d, type:%d", colLen[i], i, pColInfoData->info.type);
      terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      return terrno;
    }

    pStart += colLen[i];
  }

  bool blankFill = *(bool*)pStart;
  pStart += sizeof(bool);

  pBlock->info.dataLoad = 1;
  pBlock->info.rows = numOfRows;
  pBlock->info.blankFill = blankFill;
  if (pStart - pData != dataLen) {
    uError("block decode msg len error, pStart:%p, pData:%p, dataLen:%d", pStart, pData, dataLen);
    terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return terrno;
  }

  *pEndPos = pStart;

  code = blockDataCheck(pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t trimDataBlock(SSDataBlock* pBlock, int32_t totalRows, const bool* pBoolList) {
  //  int32_t totalRows = pBlock->info.rows;
  int32_t code = 0;
  int32_t bmLen = BitmapLen(totalRows);
  char*   pBitmap = NULL;
  int32_t maxRows = 0;

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  if (!pBoolList) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, i);
      // it is a reserved column for scalar function, and no data in this column yet.
      if (pDst->pData == NULL) {
        continue;
      }

      int32_t numOfRows = 0;
      if (IS_VAR_DATA_TYPE(pDst->info.type)) {
        pDst->varmeta.length = 0;
      } else {
        memset(pDst->nullbitmap, 0, bmLen);
      }
    }
    return code;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pDst = taosArrayGet(pBlock->pDataBlock, i);
    // it is a reserved column for scalar function, and no data in this column yet.
    if (pDst->pData == NULL || (IS_VAR_DATA_TYPE(pDst->info.type) && pDst->varmeta.length == 0)) {
      continue;
    }

    int32_t numOfRows = 0;
    if (IS_VAR_DATA_TYPE(pDst->info.type)) {
      int32_t j = 0;
      pDst->varmeta.length = 0;

      while (j < totalRows) {
        if (pBoolList[j] == 0) {
          j += 1;
          continue;
        }

        if (colDataIsNull_var(pDst, j)) {
          colDataSetNull_var(pDst, numOfRows);
        } else {
          // fix address sanitizer error. p1 may point to memory that will change during realloc of colDataSetVal, first
          // copy it to p2
          char*   p1 = colDataGetVarData(pDst, j);
          int32_t len = 0;
          if (pDst->info.type == TSDB_DATA_TYPE_JSON) {
            len = getJsonValueLen(p1);
          } else {
            len = varDataTLen(p1);
          }

          char* p2 = taosMemoryMalloc(len);
          if (p2 == NULL) {
            return terrno;
          }

          memcpy(p2, p1, len);
          code = colDataSetVal(pDst, numOfRows, p2, false);
          taosMemoryFree(p2);
          if (code) {
            return code;
          }
        }
        numOfRows += 1;
        j += 1;
      }

      if (maxRows < numOfRows) {
        maxRows = numOfRows;
      }
    } else {
      if (pBitmap == NULL) {
        pBitmap = taosMemoryCalloc(1, bmLen);
        if (pBitmap == NULL) {
          return terrno;
        }
      }

      memcpy(pBitmap, pDst->nullbitmap, bmLen);
      memset(pDst->nullbitmap, 0, bmLen);

      int32_t j = 0;

      switch (pDst->info.type) {
        case TSDB_DATA_TYPE_BIGINT:
        case TSDB_DATA_TYPE_UBIGINT:
        case TSDB_DATA_TYPE_DOUBLE:
        case TSDB_DATA_TYPE_TIMESTAMP:
          while (j < totalRows) {
            if (pBoolList[j] == 0) {
              j += 1;
              continue;
            }

            if (colDataIsNull_f(pBitmap, j)) {
              colDataSetNull_f(pDst->nullbitmap, numOfRows);
            } else {
              ((int64_t*)pDst->pData)[numOfRows] = ((int64_t*)pDst->pData)[j];
            }
            numOfRows += 1;
            j += 1;
          }
          break;
        case TSDB_DATA_TYPE_FLOAT:
        case TSDB_DATA_TYPE_INT:
        case TSDB_DATA_TYPE_UINT:
          while (j < totalRows) {
            if (pBoolList[j] == 0) {
              j += 1;
              continue;
            }
            if (colDataIsNull_f(pBitmap, j)) {
              colDataSetNull_f(pDst->nullbitmap, numOfRows);
            } else {
              ((int32_t*)pDst->pData)[numOfRows] = ((int32_t*)pDst->pData)[j];
            }
            numOfRows += 1;
            j += 1;
          }
          break;
        case TSDB_DATA_TYPE_SMALLINT:
        case TSDB_DATA_TYPE_USMALLINT:
          while (j < totalRows) {
            if (pBoolList[j] == 0) {
              j += 1;
              continue;
            }
            if (colDataIsNull_f(pBitmap, j)) {
              colDataSetNull_f(pDst->nullbitmap, numOfRows);
            } else {
              ((int16_t*)pDst->pData)[numOfRows] = ((int16_t*)pDst->pData)[j];
            }
            numOfRows += 1;
            j += 1;
          }
          break;
        case TSDB_DATA_TYPE_BOOL:
        case TSDB_DATA_TYPE_TINYINT:
        case TSDB_DATA_TYPE_UTINYINT:
          while (j < totalRows) {
            if (pBoolList[j] == 0) {
              j += 1;
              continue;
            }
            if (colDataIsNull_f(pBitmap, j)) {
              colDataSetNull_f(pDst->nullbitmap, numOfRows);
            } else {
              ((int8_t*)pDst->pData)[numOfRows] = ((int8_t*)pDst->pData)[j];
            }
            numOfRows += 1;
            j += 1;
          }
          break;
      }
    }

    if (maxRows < numOfRows) {
      maxRows = numOfRows;
    }
  }

  pBlock->info.rows = maxRows;
  if (pBitmap != NULL) {
    taosMemoryFree(pBitmap);
  }

  return code;
}

int32_t blockGetEncodeSize(const SSDataBlock* pBlock) {
  return blockDataGetSerialMetaSize(taosArrayGetSize(pBlock->pDataBlock)) + blockDataGetSize(pBlock);
}

int32_t blockDataGetSortedRows(SSDataBlock* pDataBlock, SArray* pOrderInfo) {
  if (!pDataBlock || !pOrderInfo) return 0;
  for (int32_t i = 0; i < taosArrayGetSize(pOrderInfo); ++i) {
    SBlockOrderInfo* pOrder = taosArrayGet(pOrderInfo, i);
    if (pOrder == NULL) {
      continue;
    }

    pOrder->pColData = taosArrayGet(pDataBlock->pDataBlock, pOrder->slotId);
    if (pOrder->pColData == NULL) {
      continue;
    }

    pOrder->compFn = getKeyComparFunc(pOrder->pColData->info.type, pOrder->order);
  }

  SSDataBlockSortHelper sortHelper = {.orderInfo = pOrderInfo, .pDataBlock = pDataBlock};

  int32_t rowIdx = 0, nextRowIdx = 1;
  for (; rowIdx < pDataBlock->info.rows && nextRowIdx < pDataBlock->info.rows; ++rowIdx, ++nextRowIdx) {
    if (dataBlockCompar(&nextRowIdx, &rowIdx, &sortHelper) < 0) {
      break;
    }
  }

  return nextRowIdx;
}

#define BLOCK_DATA_CHECK_TRESSA(o)                      \
  if (!(o)) {                                           \
    uError("blockDataCheck failed! line:%d", __LINE__); \
    return TSDB_CODE_INTERNAL_ERROR;                    \
  }
int32_t blockDataCheck(const SSDataBlock* pDataBlock) {
  if (tsSafetyCheckLevel == TSDB_SAFETY_CHECK_LEVELL_NEVER || NULL == pDataBlock || pDataBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }

  BLOCK_DATA_CHECK_TRESSA(pDataBlock->info.rows > 0);
  if (!pDataBlock->info.dataLoad) {
    return TSDB_CODE_SUCCESS;
  }

  bool isVarType = false;
  int32_t colLen = 0;
  int32_t nextPos = 0;
  int64_t checkRows = 0;
  int64_t typeValue = 0;
  int32_t colNum = taosArrayGetSize(pDataBlock->pDataBlock);
  for (int32_t i = 0; i < colNum; ++i) {
    SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet(pDataBlock->pDataBlock, i);
    isVarType = IS_VAR_DATA_TYPE(pCol->info.type);
    checkRows = pDataBlock->info.rows;
    if (pCol->info.reserve == true) continue;

    if (isVarType) {
      BLOCK_DATA_CHECK_TRESSA(pCol->varmeta.offset);
    } else {
      BLOCK_DATA_CHECK_TRESSA(pCol->nullbitmap);
    }

    nextPos = 0;
    for (int64_t r = 0; r < checkRows; ++r) {
      if (tsSafetyCheckLevel <= TSDB_SAFETY_CHECK_LEVELL_NORMAL) break;
      if (!colDataIsNull_s(pCol, r)) {
        BLOCK_DATA_CHECK_TRESSA(pCol->pData);
        BLOCK_DATA_CHECK_TRESSA(pCol->varmeta.length <= pCol->varmeta.allocLen);

        if (isVarType) {
          BLOCK_DATA_CHECK_TRESSA(pCol->varmeta.allocLen > 0);
          BLOCK_DATA_CHECK_TRESSA(pCol->varmeta.offset[r] <= pCol->varmeta.length);
          if (pCol->reassigned) {
            BLOCK_DATA_CHECK_TRESSA(pCol->varmeta.offset[r] >= 0);
          } else if (0 == r) {
            nextPos = pCol->varmeta.offset[r];
          } else {
            BLOCK_DATA_CHECK_TRESSA(pCol->varmeta.offset[r] == nextPos);
          }
          
          colLen = varDataTLen(pCol->pData + pCol->varmeta.offset[r]);
          BLOCK_DATA_CHECK_TRESSA(colLen >= VARSTR_HEADER_SIZE);
          BLOCK_DATA_CHECK_TRESSA(colLen <= pCol->info.bytes);
          
          if (pCol->reassigned) {
            BLOCK_DATA_CHECK_TRESSA((pCol->varmeta.offset[r] + colLen) <= pCol->varmeta.length);
          } else {
            nextPos += colLen;
            BLOCK_DATA_CHECK_TRESSA(nextPos <= pCol->varmeta.length);
          }

          typeValue = *(char*)(pCol->pData + pCol->varmeta.offset[r] + colLen - 1);
        } else {
          if (TSDB_DATA_TYPE_FLOAT == pCol->info.type) {
            float v = 0;
            GET_TYPED_DATA(v, float, pCol->info.type, colDataGetNumData(pCol, r));
          } else if (TSDB_DATA_TYPE_DOUBLE == pCol->info.type) {
            double v = 0;
            GET_TYPED_DATA(v, double, pCol->info.type, colDataGetNumData(pCol, r));
          } else {
            GET_TYPED_DATA(typeValue, int64_t, pCol->info.type, colDataGetNumData(pCol, r));
          }
        }
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}


