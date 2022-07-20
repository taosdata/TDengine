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
#include "trow.h"

const uint8_t tdVTypeByte[2][3] = {{
                                       // 2 bits
                                       TD_VTYPE_NORM_BYTE_II,
                                       TD_VTYPE_NONE_BYTE_II,
                                       TD_VTYPE_NULL_BYTE_II,
                                   },
                                   {
                                       // 1 bit
                                       TD_VTYPE_NORM_BYTE_I,  // normal
                                       TD_VTYPE_NULL_BYTE_I,
                                       TD_VTYPE_NULL_BYTE_I,  // padding
                                   }

};

// declaration
static uint8_t              tdGetBitmapByte(uint8_t byte);
static int32_t              tdCompareColId(const void *arg1, const void *arg2);
static FORCE_INLINE int32_t compareKvRowColId(const void *key1, const void *key2);

// implementation
/**
 * @brief Compress bitmap bytes comprised of 2-bits to counterpart of 1-bit.
 * e.g.
 * TD_VTYPE_NORM 0x00U(00000000) to 00000000 Normal
 * TD_VTYPE_NULL 0x01U(00000001) to 00000001 Null
 * TD_VTYPE_NONE 0x02U(00000010) to 00000001 Null
 *
 * 00000000 0x00 0x00
 * 01000000 0x40 0x08
 * 10000000 0x80 0x08
 *  ...
 * @param byte
 * @return uint8_t
 */
static uint8_t tdGetMergedBitmapByte(uint8_t byte) {
  switch (byte) {
    case 0x00:
      return 0x00;
    case 0x40:
      return 0x08;
    case 0x80:
      return 0x08;
    case 0x10:
      return 0x04;
    case 0x50:
      return 0x0c;
    case 0x90:
      return 0x0c;
    case 0x20:
      return 0x04;
    case 0x60:
      return 0x0c;
    case 0xa0:
      return 0x0c;
    case 0x04:
      return 0x02;
    case 0x44:
      return 0x0a;
    case 0x84:
      return 0x0a;
    case 0x14:
      return 0x06;
    case 0x54:
      return 0x0e;
    case 0x94:
      return 0x0e;
    case 0x24:
      return 0x06;
    case 0x64:
      return 0x0e;
    case 0xa4:
      return 0x0e;
    case 0x08:
      return 0x02;
    case 0x48:
      return 0x0a;
    case 0x88:
      return 0x0a;
    case 0x18:
      return 0x06;
    case 0x58:
      return 0x0e;
    case 0x98:
      return 0x0e;
    case 0x28:
      return 0x06;
    case 0x68:
      return 0x0e;
    case 0xa8:
      return 0x0e;
    case 0x01:
      return 0x01;
    case 0x41:
      return 0x09;
    case 0x81:
      return 0x09;
    case 0x11:
      return 0x05;
    case 0x51:
      return 0x0d;
    case 0x91:
      return 0x0d;
    case 0x21:
      return 0x05;
    case 0x61:
      return 0x0d;
    case 0xa1:
      return 0x0d;
    case 0x05:
      return 0x03;
    case 0x45:
      return 0x0b;
    case 0x85:
      return 0x0b;
    case 0x15:
      return 0x07;
    case 0x55:
      return 0x0f;
    case 0x95:
      return 0x0f;
    case 0x25:
      return 0x07;
    case 0x65:
      return 0x0f;
    case 0xa5:
      return 0x0f;
    case 0x09:
      return 0x03;
    case 0x49:
      return 0x0b;
    case 0x89:
      return 0x0b;
    case 0x19:
      return 0x07;
    case 0x59:
      return 0x0f;
    case 0x99:
      return 0x0f;
    case 0x29:
      return 0x07;
    case 0x69:
      return 0x0f;
    case 0xa9:
      return 0x0f;
    case 0x02:
      return 0x01;
    case 0x42:
      return 0x09;
    case 0x82:
      return 0x09;
    case 0x12:
      return 0x05;
    case 0x52:
      return 0x0d;
    case 0x92:
      return 0x0d;
    case 0x22:
      return 0x05;
    case 0x62:
      return 0x0d;
    case 0xa2:
      return 0x0d;
    case 0x06:
      return 0x03;
    case 0x46:
      return 0x0b;
    case 0x86:
      return 0x0b;
    case 0x16:
      return 0x07;
    case 0x56:
      return 0x0f;
    case 0x96:
      return 0x0f;
    case 0x26:
      return 0x07;
    case 0x66:
      return 0x0f;
    case 0xa6:
      return 0x0f;
    case 0x0a:
      return 0x03;
    case 0x4a:
      return 0x0b;
    case 0x8a:
      return 0x0b;
    case 0x1a:
      return 0x07;
    case 0x5a:
      return 0x0f;
    case 0x9a:
      return 0x0f;
    case 0x2a:
      return 0x07;
    case 0x6a:
      return 0x0f;
    case 0xaa:
      return 0x0f;
    default:
      // make sure the bitmap area is set to 0 firstly
      ASSERT(0);
      return 0x0f;  // return NULL bitmap for exception
  }
}

/**
 * @brief Merge bitmap from 2 bits to 1 bit, and the memory buffer should be guaranteed by the invoker.
 *
 * @param srcBitmap
 * @param nBits
 * @param dstBitmap
 */
void tdMergeBitmap(uint8_t *srcBitmap, int32_t nBits, uint8_t *dstBitmap) {
  int32_t i = 0, j = 0;
  int32_t nBytes = TD_BITMAP_BYTES(nBits);
  int32_t nRoundBytes = nBits / 4;
  int32_t nRemainderBits = nBits - nRoundBytes * 4;

  switch (nRemainderBits) {
    case 0:
      // NOTHING TODO
      break;
    case 1: {
      void *lastByte = POINTER_SHIFT(srcBitmap, nRoundBytes);
      *(uint8_t *)lastByte &= 0xC0;
    } break;
    case 2: {
      void *lastByte = POINTER_SHIFT(srcBitmap, nRoundBytes);
      *(uint8_t *)lastByte &= 0xF0;
    } break;
    case 3: {
      void *lastByte = POINTER_SHIFT(srcBitmap, nRoundBytes);
      *(uint8_t *)lastByte &= 0xFC;
    } break;
    default:
      ASSERT(0);
  }

  if (nBytes > 0) {
    dstBitmap[j] = (tdGetMergedBitmapByte(srcBitmap[i]) << 4);
  }

  while ((++i) < nBytes) {
    if ((i & 1) == 0) {
      dstBitmap[j] = (tdGetMergedBitmapByte(srcBitmap[i]) << 4);
    } else {
      dstBitmap[j] |= tdGetMergedBitmapByte(srcBitmap[i]);
      ++j;
    }
  }
}

/**
 * @brief Set bitmap area by byte preferentially and then by bit.
 *
 * @param pBitmap
 * @param nEle
 * @param valType
 * @param bitmapMode 0 for 2 bits, 1 for 1 bit
 * @return int32_t
 */
int32_t tdSetBitmapValTypeN(void *pBitmap, int16_t nEle, TDRowValT valType, int8_t bitmapMode) {
  TASSERT(valType < TD_VTYPE_MAX);
  int32_t nBytes = (bitmapMode == 0 ? nEle / TD_VTYPE_PARTS : nEle / TD_VTYPE_PARTS_I);
  uint8_t vTypeByte = tdVTypeByte[bitmapMode][valType];
  for (int i = 0; i < nBytes; ++i) {
    *(uint8_t *)pBitmap = vTypeByte;
    pBitmap = POINTER_SHIFT(pBitmap, 1);
  }

  int32_t nLeft = nEle - nBytes * (bitmapMode == 0 ? TD_VTYPE_BITS : TD_VTYPE_BITS_I);
  for (int j = 0; j < nLeft; ++j) {
    tdSetBitmapValType(pBitmap, j, valType, bitmapMode);
  }
  return TSDB_CODE_SUCCESS;
}

bool tdIsBitmapBlkNorm(const void *pBitmap, int32_t numOfBits, int8_t bitmapMode) {
  int32_t  nBytes = (bitmapMode == 0 ? numOfBits / TD_VTYPE_PARTS : numOfBits / TD_VTYPE_PARTS_I);
  uint8_t  vTypeByte = tdVTypeByte[bitmapMode][TD_VTYPE_NORM];
  uint8_t *qBitmap = (uint8_t *)pBitmap;
  for (int i = 0; i < nBytes; ++i) {
    if (*qBitmap != vTypeByte) {
      return false;
    }
    qBitmap = (uint8_t *)POINTER_SHIFT(pBitmap, i);
  }

  int32_t nLeft = numOfBits - nBytes * (bitmapMode == 0 ? TD_VTYPE_BITS : TD_VTYPE_BITS_I);

  for (int j = 0; j < nLeft; ++j) {
    uint8_t vType;
    tdGetBitmapValType(qBitmap, j, &vType, bitmapMode);
    if (vType != TD_VTYPE_NORM) {
      return false;
    }
  }
  return true;
}

STSRow *tdRowDup(STSRow *row) {
  STSRow *trow = taosMemoryMalloc(TD_ROW_LEN(row));
  if (trow == NULL) return NULL;

  tdRowCpy(trow, row);
  return trow;
}

void tdSRowPrint(STSRow *row, STSchema *pSchema, const char *tag) {
  STSRowIter iter = {0};
  tdSTSRowIterInit(&iter, pSchema);
  tdSTSRowIterReset(&iter, row);
  printf("%s >>>type:%d,sver:%d ", tag, (int32_t)TD_ROW_TYPE(row), (int32_t)TD_ROW_SVER(row));
  for (int i = 0; i < pSchema->numOfCols; ++i) {
    STColumn *stCol = pSchema->columns + i;
    SCellVal  sVal = {255, NULL};
    if (!tdSTSRowIterNext(&iter, stCol->colId, stCol->type, &sVal)) {
      break;
    }
    ASSERT(sVal.valType == 0 || sVal.valType == 1 || sVal.valType == 2);
    tdSCellValPrint(&sVal, stCol->type);
  }
  printf("\n");
}

void tdSCellValPrint(SCellVal *pVal, int8_t colType) {
  if (tdValTypeIsNull(pVal->valType)) {
    printf("NULL ");
    return;
  } else if (tdValTypeIsNone(pVal->valType)) {
    printf("NONE ");
    return;
  }
  switch (colType) {
    case TSDB_DATA_TYPE_BOOL:
      printf("%s ", (*(int8_t *)pVal->val) == 0 ? "false" : "true");
      break;
    case TSDB_DATA_TYPE_TINYINT:
      printf("%" PRIi8 " ", *(int8_t *)pVal->val);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      printf("%" PRIi16 " ", *(int16_t *)pVal->val);
      break;
    case TSDB_DATA_TYPE_INT:
      printf("%" PRIi32 " ", *(int32_t *)pVal->val);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      printf("%" PRIi64 " ", *(int64_t *)pVal->val);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      printf("%f ", *(float *)pVal->val);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      printf("%lf ", *(double *)pVal->val);
      break;
    case TSDB_DATA_TYPE_VARCHAR:
      printf("VARCHAR ");
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      printf("%" PRIi64 " ", *(int64_t *)pVal->val);
      break;
    case TSDB_DATA_TYPE_NCHAR:
      printf("NCHAR ");
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      printf("%" PRIu8 " ", *(uint8_t *)pVal->val);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      printf("%" PRIu16 " ", *(uint16_t *)pVal->val);
      break;
    case TSDB_DATA_TYPE_UINT:
      printf("%" PRIu32 " ", *(uint32_t *)pVal->val);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      printf("%" PRIu64 " ", *(uint64_t *)pVal->val);
      break;
    case TSDB_DATA_TYPE_JSON:
      printf("JSON ");
      break;
    case TSDB_DATA_TYPE_VARBINARY:
      printf("VARBIN ");
      break;
    case TSDB_DATA_TYPE_DECIMAL:
      printf("DECIMAL ");
      break;
    case TSDB_DATA_TYPE_BLOB:
      printf("BLOB ");
      break;
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      printf("MedBLOB ");
      break;
    // case TSDB_DATA_TYPE_BINARY:
    //   printf("BINARY ");
    //   break;
    case TSDB_DATA_TYPE_MAX:
      printf("UNDEF ");
      break;
    default:
      printf("UNDEF ");
      break;
  }
}

bool tdSKvRowGetVal(STSRow *pRow, col_id_t colId, col_id_t colIdx, SCellVal *pVal) {
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    tdRowSetVal(pVal, TD_VTYPE_NORM, TD_ROW_KEY_ADDR(pRow));
    return true;
  }
  int16_t nCols = tdRowGetNCols(pRow) - 1;
  if (nCols <= 0) {
    pVal->valType = TD_VTYPE_NONE;
    return true;
  }

  SKvRowIdx *pColIdx =
      (SKvRowIdx *)taosbsearch(&colId, TD_ROW_COL_IDX(pRow), nCols, sizeof(SKvRowIdx), compareKvRowColId, TD_EQ);

  if (!pColIdx) {
    pVal->valType = TD_VTYPE_NONE;
    return true;
  }

  void *pBitmap = tdGetBitmapAddrKv(pRow, tdRowGetNCols(pRow));
  tdGetKvRowValOfCol(pVal, pRow, pBitmap, pColIdx->offset,
                     POINTER_DISTANCE(pColIdx, TD_ROW_COL_IDX(pRow)) / sizeof(SKvRowIdx));
  return true;
}

bool tdSTpRowGetVal(STSRow *pRow, col_id_t colId, col_type_t colType, int32_t flen, uint32_t offset, col_id_t colIdx,
                    SCellVal *pVal) {
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    tdRowSetVal(pVal, TD_VTYPE_NORM, TD_ROW_KEY_ADDR(pRow));
    return true;
  }
  void *pBitmap = tdGetBitmapAddrTp(pRow, flen);
  tdGetTpRowValOfCol(pVal, pRow, pBitmap, colType, offset - sizeof(TSKEY), colIdx);
  return true;
}

bool tdSTSRowIterNext(STSRowIter *pIter, col_id_t colId, col_type_t colType, SCellVal *pVal) {
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    pVal->val = &pIter->pRow->ts;
    pVal->valType = TD_VTYPE_NORM;
    return true;
  }

  if (TD_IS_TP_ROW(pIter->pRow)) {
    STColumn *pCol = NULL;
    STSchema *pSchema = pIter->pSchema;
    while (pIter->colIdx < pSchema->numOfCols) {
      pCol = &pSchema->columns[pIter->colIdx];  // 1st column of schema is primary TS key
      if (colId == pCol->colId) {
        break;
      } else if (pCol->colId < colId) {
        ++pIter->colIdx;
        continue;
      } else {
        return false;
      }
    }
    tdGetTpRowDataOfCol(pIter, pCol->type, pCol->offset - sizeof(TSKEY), pVal);
    ++pIter->colIdx;
  } else if (TD_IS_KV_ROW(pIter->pRow)) {
    return tdGetKvRowValOfColEx(pIter, colId, colType, &pIter->kvIdx, pVal);
  } else {
    pVal->valType = TD_VTYPE_NONE;
    terrno = TSDB_CODE_INVALID_PARA;
    if (COL_REACH_END(colId, pIter->maxColId)) return false;
  }
  return true;
}

bool tdGetKvRowValOfColEx(STSRowIter *pIter, col_id_t colId, col_type_t colType, col_id_t *nIdx, SCellVal *pVal) {
  STSRow    *pRow = pIter->pRow;
  SKvRowIdx *pKvIdx = NULL;
  bool       colFound = false;
  col_id_t   kvNCols = tdRowGetNCols(pRow) - 1;
  while (*nIdx < kvNCols) {
    pKvIdx = (SKvRowIdx *)POINTER_SHIFT(TD_ROW_COL_IDX(pRow), *nIdx * sizeof(SKvRowIdx));
    if (pKvIdx->colId == colId) {
      ++(*nIdx);
      pVal->val = POINTER_SHIFT(pRow, pKvIdx->offset);
      colFound = true;
      break;
    } else if (pKvIdx->colId > colId) {
      pVal->valType = TD_VTYPE_NONE;
      return true;
    } else {
      ++(*nIdx);
    }
  }

  if (!colFound) {
    if (colId <= pIter->maxColId) {
      pVal->valType = TD_VTYPE_NONE;
      return true;
    } else {
      return false;
    }
  }

#ifdef TD_SUPPORT_BITMAP
  int16_t colIdx = -1;
  if (pKvIdx) colIdx = POINTER_DISTANCE(pKvIdx, TD_ROW_COL_IDX(pRow)) / sizeof(SKvRowIdx);
  if (tdGetBitmapValType(pIter->pBitmap, colIdx, &pVal->valType, 0) != TSDB_CODE_SUCCESS) {
    pVal->valType = TD_VTYPE_NONE;
  }
#else
  pVal->valType = isNull(pVal->val, colType) ? TD_VTYPE_NULL : TD_VTYPE_NORM;
#endif

  return true;
}

bool tdGetTpRowDataOfCol(STSRowIter *pIter, col_type_t colType, int32_t offset, SCellVal *pVal) {
  STSRow *pRow = pIter->pRow;
  if (IS_VAR_DATA_TYPE(colType)) {
    pVal->val = POINTER_SHIFT(pRow, *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(pRow), offset));
  } else {
    pVal->val = POINTER_SHIFT(TD_ROW_DATA(pRow), offset);
  }

#ifdef TD_SUPPORT_BITMAP
  if (tdGetBitmapValType(pIter->pBitmap, pIter->colIdx - 1, &pVal->valType, 0) != TSDB_CODE_SUCCESS) {
    pVal->valType = TD_VTYPE_NONE;
  }
#else
  pVal->valType = isNull(pVal->val, colType) ? TD_VTYPE_NULL : TD_VTYPE_NORM;
#endif

  return true;
}

static FORCE_INLINE int32_t compareKvRowColId(const void *key1, const void *key2) {
  if (*(col_id_t *)key1 > ((SKvRowIdx *)key2)->colId) {
    return 1;
  } else if (*(col_id_t *)key1 < ((SKvRowIdx *)key2)->colId) {
    return -1;
  } else {
    return 0;
  }
}

int32_t tdSTSRowNew(SArray *pArray, STSchema *pTSchema, STSRow **ppRow) {
  STColumn *pTColumn;
  SColVal  *pColVal;
  int32_t   nColVal = taosArrayGetSize(pArray);
  int32_t   varDataLen = 0;
  int32_t   maxVarDataLen = 0;
  int32_t   iColVal = 0;
  void     *varBuf = NULL;
  bool      isAlloc = false;

  ASSERT(nColVal > 1);

  for (int32_t iColumn = 0; iColumn < pTSchema->numOfCols; ++iColumn) {
    pTColumn = &pTSchema->columns[iColumn];
    if (iColVal < nColVal) {
      pColVal = (SColVal *)taosArrayGet(pArray, iColVal);
    } else {
      pColVal = NULL;
    }

    if (iColumn == 0) {
      ASSERT(pColVal->cid == pTColumn->colId);
      ASSERT(pTColumn->type == TSDB_DATA_TYPE_TIMESTAMP);
      ASSERT(pTColumn->colId == PRIMARYKEY_TIMESTAMP_COL_ID);
    } else {
      if (IS_VAR_DATA_TYPE(pTColumn->type)) {
        if (pColVal) {
          varDataLen += (pColVal->value.nData + sizeof(VarDataLenT));
          if (maxVarDataLen < (pColVal->value.nData + sizeof(VarDataLenT))) {
            maxVarDataLen = pColVal->value.nData + sizeof(VarDataLenT);
          }
        } else {
          varDataLen += sizeof(VarDataLenT);
          if (pTColumn->type == TSDB_DATA_TYPE_VARCHAR) {
            varDataLen += CHAR_BYTES;
            if (maxVarDataLen < CHAR_BYTES + sizeof(VarDataLenT)) {
              maxVarDataLen = CHAR_BYTES + sizeof(VarDataLenT);
            }
          } else {
            varDataLen += INT_BYTES;
            if (maxVarDataLen < INT_BYTES + sizeof(VarDataLenT)) {
              maxVarDataLen = INT_BYTES + sizeof(VarDataLenT);
            }
          }
        }
      }
    }

    ++iColVal;
  }

  if (!(*ppRow)) {
    *ppRow = (STSRow *)taosMemoryCalloc(
        1, sizeof(STSRow) + pTSchema->flen + varDataLen + TD_BITMAP_BYTES(pTSchema->numOfCols - 1));
    isAlloc = true;
  }

  if (!(*ppRow)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (maxVarDataLen > 0) {
    varBuf = taosMemoryMalloc(maxVarDataLen);
    if (!varBuf) {
      if(isAlloc) {
        taosMemoryFreeClear(*ppRow);
      }
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  SRowBuilder rb = {0};
  tdSRowInit(&rb, pTSchema->version);
  tdSRowSetInfo(&rb, pTSchema->numOfCols, pTSchema->numOfCols, pTSchema->flen);
  tdSRowResetBuf(&rb, *ppRow);

  iColVal = 0;
  for (int32_t iColumn = 0; iColumn < pTSchema->numOfCols; ++iColumn) {
    pTColumn = &pTSchema->columns[iColumn];

    TDRowValT   valType = TD_VTYPE_NORM;
    const void *val = NULL;
    if (iColVal < nColVal) {
      pColVal = (SColVal *)taosArrayGet(pArray, iColVal);
      if (pColVal->isNone) {
        valType = TD_VTYPE_NONE;
      } else if (pColVal->isNull) {
        valType = TD_VTYPE_NULL;
      } else if (IS_VAR_DATA_TYPE(pTColumn->type)) {
        varDataSetLen(varBuf, pColVal->value.nData);
        memcpy(varDataVal(varBuf), pColVal->value.pData, pColVal->value.nData);
        val = varBuf;
      } else {
        val = (const void *)&pColVal->value.i64;
      }
    } else {
      pColVal = NULL;
      valType = TD_VTYPE_NONE;
    }

    tdAppendColValToRow(&rb, pTColumn->colId, pTColumn->type, valType, val, true, pTColumn->offset, iColVal);

    ++iColVal;
  }

  taosMemoryFreeClear(varBuf);

  return 0;
}

bool tdSTSRowGetVal(STSRowIter *pIter, col_id_t colId, col_type_t colType, SCellVal *pVal) {
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    pVal->val = &pIter->pRow->ts;
    pVal->valType = TD_VTYPE_NORM;
    return true;
  }

  STSRow *pRow = pIter->pRow;
  int16_t colIdx = -1;
  if (TD_IS_TP_ROW(pRow)) {
    STSchema *pSchema = pIter->pSchema;
    STColumn *pCol =
        (STColumn *)taosbsearch(&colId, pSchema->columns, pSchema->numOfCols, sizeof(STColumn), tdCompareColId, TD_EQ);
    if (!pCol) {
      pVal->valType = TD_VTYPE_NONE;
      if (COL_REACH_END(colId, pIter->maxColId)) return false;
      return true;
    }
#ifdef TD_SUPPORT_BITMAP
    colIdx = POINTER_DISTANCE(pCol, pSchema->columns) / sizeof(STColumn);
#endif
    tdGetTpRowValOfCol(pVal, pRow, pIter->pBitmap, pCol->type, pCol->offset - sizeof(TSKEY), colIdx - 1);
  } else if (TD_IS_KV_ROW(pRow)) {
    SKvRowIdx *pIdx = (SKvRowIdx *)taosbsearch(&colId, TD_ROW_COL_IDX(pRow), tdRowGetNCols(pRow), sizeof(SKvRowIdx),
                                               compareKvRowColId, TD_EQ);
#ifdef TD_SUPPORT_BITMAP
    if (pIdx) {
      colIdx = POINTER_DISTANCE(pIdx, TD_ROW_COL_IDX(pRow)) / sizeof(SKvRowIdx);
    }
#endif
    tdGetKvRowValOfCol(pVal, pRow, pIter->pBitmap, pIdx ? pIdx->offset : -1, colIdx);
  } else {
    if (COL_REACH_END(colId, pIter->maxColId)) return false;
    pVal->valType = TD_VTYPE_NONE;
  }

  return true;
}

static int32_t tdCompareColId(const void *arg1, const void *arg2) {
  int32_t   colId = *(int32_t *)arg1;
  STColumn *pCol = (STColumn *)arg2;

  if (colId < pCol->colId) {
    return -1;
  } else if (colId == pCol->colId) {
    return 0;
  } else {
    return 1;
  }
}

int32_t tdGetBitmapValTypeII(const void *pBitmap, int16_t colIdx, TDRowValT *pValType) {
  if (!pBitmap || colIdx < 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  int16_t nBytes = colIdx / TD_VTYPE_PARTS;
  int16_t nOffset = colIdx & TD_VTYPE_OPTR;
  char   *pDestByte = (char *)POINTER_SHIFT(pBitmap, nBytes);
  // use literal value directly and not use formula to simplify the codes
  switch (nOffset) {
    case 0:
      *pValType = (((*pDestByte) & 0xC0) >> 6);
      break;
    case 1:
      *pValType = (((*pDestByte) & 0x30) >> 4);
      break;
    case 2:
      *pValType = (((*pDestByte) & 0x0C) >> 2);
      break;
    case 3:
      *pValType = ((*pDestByte) & 0x03);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tdGetBitmapValTypeI(const void *pBitmap, int16_t colIdx, TDRowValT *pValType) {
  if (!pBitmap || colIdx < 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  int16_t nBytes = colIdx / TD_VTYPE_PARTS_I;
  int16_t nOffset = colIdx & TD_VTYPE_OPTR_I;
  char   *pDestByte = (char *)POINTER_SHIFT(pBitmap, nBytes);
  // use literal value directly and not use formula to simplify the codes
  switch (nOffset) {
    case 0:
      *pValType = (((*pDestByte) & 0x80) >> 7);
      break;
    case 1:
      *pValType = (((*pDestByte) & 0x40) >> 6);
      break;
    case 2:
      *pValType = (((*pDestByte) & 0x20) >> 5);
      break;
    case 3:
      *pValType = (((*pDestByte) & 0x10) >> 4);
      break;
    case 4:
      *pValType = (((*pDestByte) & 0x08) >> 3);
      break;
    case 5:
      *pValType = (((*pDestByte) & 0x04) >> 2);
      break;
    case 6:
      *pValType = (((*pDestByte) & 0x02) >> 1);
      break;
    case 7:
      *pValType = ((*pDestByte) & 0x01);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tdSetBitmapValTypeI(void *pBitmap, int16_t colIdx, TDRowValT valType) {
  if (!pBitmap || colIdx < 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  int16_t nBytes = colIdx / TD_VTYPE_PARTS_I;
  int16_t nOffset = colIdx & TD_VTYPE_OPTR_I;
  char   *pDestByte = (char *)POINTER_SHIFT(pBitmap, nBytes);
  // use literal value directly and not use formula to simplify the codes
  switch (nOffset) {
    case 0:
      *pDestByte = ((*pDestByte) & 0x7F) | (valType << 7);
      // set the value and clear other partitions for offset 0
      // *pDestByte |= (valType << 7);
      break;
    case 1:
      *pDestByte = ((*pDestByte) & 0xBF) | (valType << 6);
      // *pDestByte |= (valType << 6);
      break;
    case 2:
      *pDestByte = ((*pDestByte) & 0xDF) | (valType << 5);
      // *pDestByte |= (valType << 5);
      break;
    case 3:
      *pDestByte = ((*pDestByte) & 0xEF) | (valType << 4);
      // *pDestByte |= (valType << 4);
      break;
    case 4:
      *pDestByte = ((*pDestByte) & 0xF7) | (valType << 3);
      // *pDestByte |= (valType << 3);
      break;
    case 5:
      *pDestByte = ((*pDestByte) & 0xFB) | (valType << 2);
      // *pDestByte |= (valType << 2);
      break;
    case 6:
      *pDestByte = ((*pDestByte) & 0xFD) | (valType << 1);
      // *pDestByte |= (valType << 1);
      break;
    case 7:
      *pDestByte = ((*pDestByte) & 0xFE) | valType;
      // *pDestByte |= (valType);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tdGetKvRowValOfCol(SCellVal *output, STSRow *pRow, void *pBitmap, int32_t offset, int16_t colIdx) {
#ifdef TD_SUPPORT_BITMAP
  TASSERT(colIdx < tdRowGetNCols(pRow) - 1);
  if (tdGetBitmapValType(pBitmap, colIdx, &output->valType, 0) != TSDB_CODE_SUCCESS) {
    output->valType = TD_VTYPE_NONE;
    return terrno;
  }
  if (tdValTypeIsNorm(output->valType)) {
    if (offset < 0) {
      terrno = TSDB_CODE_INVALID_PARA;
      output->valType = TD_VTYPE_NONE;
      return terrno;
    }
    output->val = POINTER_SHIFT(pRow, offset);
  }
#else
  TASSERT(0);
  if (offset < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    output->valType = TD_VTYPE_NONE;
    return terrno;
  }
  output->val = POINTER_SHIFT(pRow, offset);
  output->valType = isNull(output->val, colType) ? TD_VTYPE_NULL : TD_VTYPE_NORM;
#endif
  return TSDB_CODE_SUCCESS;
}

int32_t tdGetTpRowValOfCol(SCellVal *output, STSRow *pRow, void *pBitmap, int8_t colType, int32_t offset,
                           int16_t colIdx) {
#ifdef TD_SUPPORT_BITMAP
  if (tdGetBitmapValType(pBitmap, colIdx, &output->valType, 0) != TSDB_CODE_SUCCESS) {
    output->valType = TD_VTYPE_NONE;
    return terrno;
  }
  if (tdValTypeIsNorm(output->valType)) {
    if (IS_VAR_DATA_TYPE(colType)) {
      output->val = POINTER_SHIFT(pRow, *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(pRow), offset));
    } else {
      output->val = POINTER_SHIFT(TD_ROW_DATA(pRow), offset);
    }
  }
#else
  if (IS_VAR_DATA_TYPE(colType)) {
    output->val = POINTER_SHIFT(pRow, *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(pRow), offset));
  } else {
    output->val = POINTER_SHIFT(TD_ROW_DATA(pRow), offset);
  }
  output->valType = isNull(output->val, colType) ? TD_VTYPE_NULL : TD_VTYPE_NORM;
#endif
  return TSDB_CODE_SUCCESS;
}

int32_t tdAppendColValToRow(SRowBuilder *pBuilder, col_id_t colId, int8_t colType, TDRowValT valType, const void *val,
                            bool isCopyVarData, int32_t offset, col_id_t colIdx) {
  STSRow *pRow = pBuilder->pBuf;
  if (!val) {
#ifdef TD_SUPPORT_BITMAP
    if (tdValTypeIsNorm(valType)) {
      terrno = TSDB_CODE_INVALID_PTR;
      return terrno;
    }
#else
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
#endif
  }
  // TS KEY is stored in STSRow.ts and not included in STSRow.data field.
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    TD_ROW_KEY(pRow) = *(TSKEY *)val;
    // The primary TS key is Norm all the time, thus its valType is not stored in bitmap.
    return TSDB_CODE_SUCCESS;
  }
  // TODO:  We can avoid the type judegement by FP, but would prevent the inline scheme.
  if (TD_IS_TP_ROW(pRow)) {
    tdAppendColValToTpRow(pBuilder, valType, val, isCopyVarData, colType, colIdx, offset);
  } else {
    tdAppendColValToKvRow(pBuilder, valType, val, isCopyVarData, colType, colIdx, offset, colId);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tdAppendColValToKvRow(SRowBuilder *pBuilder, TDRowValT valType, const void *val, bool isCopyVarData,
                              int8_t colType, int16_t colIdx, int32_t offset, col_id_t colId) {
  if ((offset < (int32_t)sizeof(SKvRowIdx)) || (colIdx < 1)) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  offset -= sizeof(SKvRowIdx);
  --colIdx;

#ifdef TD_SUPPORT_BITMAP
  if (tdSetBitmapValType(pBuilder->pBitmap, colIdx, valType, 0) != TSDB_CODE_SUCCESS) {
    return terrno;
  }
#endif

  STSRow *row = pBuilder->pBuf;
  // No need to store None/Null values.
  if (tdValIsNorm(valType, val, colType)) {
    // ts key stored in STSRow.ts
    SKvRowIdx *pColIdx = (SKvRowIdx *)POINTER_SHIFT(TD_ROW_COL_IDX(row), offset);
    char      *ptr = (char *)POINTER_SHIFT(row, TD_ROW_LEN(row));
    pColIdx->colId = colId;
    pColIdx->offset = TD_ROW_LEN(row);  // the offset include the TD_ROW_HEAD_LEN

    if (IS_VAR_DATA_TYPE(colType)) {
      if (isCopyVarData) {
        memcpy(ptr, val, varDataTLen(val));
      }
      TD_ROW_LEN(row) += varDataTLen(val);
    } else {
      memcpy(ptr, val, TYPE_BYTES[colType]);
      TD_ROW_LEN(row) += TYPE_BYTES[colType];
    }
  }
#ifdef TD_SUPPORT_BACK2
  // NULL/None value
  else {
    SKvRowIdx *pColIdx = (SKvRowIdx *)POINTER_SHIFT(TD_ROW_COL_IDX(row), offset);
    char      *ptr = (char *)POINTER_SHIFT(row, TD_ROW_LEN(row));
    pColIdx->colId = colId;
    pColIdx->offset = TD_ROW_LEN(row);  // the offset include the TD_ROW_HEAD_LEN
    const void *nullVal = getNullValue(colType);

    if (IS_VAR_DATA_TYPE(colType)) {
      if (isCopyVarData) {
        memcpy(ptr, nullVal, varDataTLen(nullVal));
      }
      TD_ROW_LEN(row) += varDataTLen(nullVal);
    } else {
      memcpy(ptr, nullVal, TYPE_BYTES[colType]);
      TD_ROW_LEN(row) += TYPE_BYTES[colType];
    }
  }
#endif

  return 0;
}

int32_t tdAppendColValToTpRow(SRowBuilder *pBuilder, TDRowValT valType, const void *val, bool isCopyVarData,
                              int8_t colType, int16_t colIdx, int32_t offset) {
  if ((offset < (int32_t)sizeof(TSKEY)) || (colIdx < 1)) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  offset -= sizeof(TSKEY);
  --colIdx;

#ifdef TD_SUPPORT_BITMAP
  if (tdSetBitmapValType(pBuilder->pBitmap, colIdx, valType, 0) != TSDB_CODE_SUCCESS) {
    return terrno;
  }
#endif

  STSRow *row = pBuilder->pBuf;

  // 1. No need to set flen part for Null/None, just use bitmap. When upsert for the same primary TS key, the bitmap
  // should be updated simultaneously if Norm val overwrite Null/None cols.
  // 2. When consume STSRow in memory by taos client/tq, the output of Null/None cols should both be Null.
  if (tdValIsNorm(valType, val, colType)) {
    // TODO: The layout of new data types imported since 3.0 like blob/medium blob is the same with binary/nchar.
    if (IS_VAR_DATA_TYPE(colType)) {
      // ts key stored in STSRow.ts
      *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(row), offset) = TD_ROW_LEN(row);
      if (isCopyVarData) {
        memcpy(POINTER_SHIFT(row, TD_ROW_LEN(row)), val, varDataTLen(val));
      }
      TD_ROW_LEN(row) += varDataTLen(val);
    } else {
      memcpy(POINTER_SHIFT(TD_ROW_DATA(row), offset), val, TYPE_BYTES[colType]);
    }
  }
#ifdef TD_SUPPORT_BACK2
  // NULL/None value
  else {
    // TODO: Null value for new data types imported since 3.0 need to be defined.
    const void *nullVal = getNullValue(colType);
    if (IS_VAR_DATA_TYPE(colType)) {
      // ts key stored in STSRow.ts
      *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(row), offset) = TD_ROW_LEN(row);

      if (isCopyVarData) {
        memcpy(POINTER_SHIFT(row, TD_ROW_LEN(row)), nullVal, varDataTLen(nullVal));
      }
      TD_ROW_LEN(row) += varDataTLen(nullVal);
    } else {
      memcpy(POINTER_SHIFT(TD_ROW_DATA(row), offset), nullVal, TYPE_BYTES[colType]);
    }
  }
#endif

  return 0;
}

int32_t tdSRowSetExtendedInfo(SRowBuilder *pBuilder, int32_t nCols, int32_t nBoundCols, int32_t flen,
                              int32_t allNullLen, int32_t boundNullLen) {
  if ((boundNullLen > 0) && (allNullLen > 0) && (nBoundCols > 0)) {
    uint32_t tpLen = allNullLen;
    uint32_t kvLen = sizeof(col_id_t) + sizeof(SKvRowIdx) * nBoundCols + boundNullLen;
    if (isSelectKVRow(kvLen, tpLen)) {
      pBuilder->rowType = TD_ROW_KV;
    } else {
      pBuilder->rowType = TD_ROW_TP;
    }

  } else {
    pBuilder->rowType = TD_ROW_TP;
  }
  pBuilder->flen = flen;
  pBuilder->nCols = nCols;
  pBuilder->nBoundCols = nBoundCols;
  if (pBuilder->flen <= 0 || pBuilder->nCols <= 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifdef TD_SUPPORT_BITMAP
  // the primary TS key is stored separatedly
  pBuilder->nBitmaps = (col_id_t)TD_BITMAP_BYTES(pBuilder->nCols - 1);
  if (nBoundCols > 0) {
    pBuilder->nBoundBitmaps = (col_id_t)TD_BITMAP_BYTES(pBuilder->nBoundCols - 1);
  } else {
    pBuilder->nBoundBitmaps = 0;
  }
#else
  pBuilder->nBitmaps = 0;
  pBuilder->nBoundBitmaps = 0;
#endif
  return TSDB_CODE_SUCCESS;
}

int32_t tdSRowResetBuf(SRowBuilder *pBuilder, void *pBuf) {
  pBuilder->pBuf = (STSRow *)pBuf;
  if (!pBuilder->pBuf) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  TD_ROW_SET_INFO(pBuilder->pBuf, 0);
  TD_ROW_SET_TYPE(pBuilder->pBuf, pBuilder->rowType);

  TASSERT(pBuilder->nBitmaps > 0 && pBuilder->flen > 0);

  uint32_t len = 0;
  switch (pBuilder->rowType) {
    case TD_ROW_TP:
#ifdef TD_SUPPORT_BITMAP
      pBuilder->pBitmap = tdGetBitmapAddrTp(pBuilder->pBuf, pBuilder->flen);
      memset(pBuilder->pBitmap, TD_VTYPE_NONE_BYTE_II, pBuilder->nBitmaps);
#endif
      // the primary TS key is stored separatedly
      len = TD_ROW_HEAD_LEN + pBuilder->flen - sizeof(TSKEY) + pBuilder->nBitmaps;
      TD_ROW_SET_LEN(pBuilder->pBuf, len);
      TD_ROW_SET_SVER(pBuilder->pBuf, pBuilder->sver);
      break;
    case TD_ROW_KV:
#ifdef TD_SUPPORT_BITMAP
      pBuilder->pBitmap = tdGetBitmapAddrKv(pBuilder->pBuf, pBuilder->nBoundCols);
      memset(pBuilder->pBitmap, TD_VTYPE_NONE_BYTE_II, pBuilder->nBoundBitmaps);
#endif
      len = TD_ROW_HEAD_LEN + TD_ROW_NCOLS_LEN + (pBuilder->nBoundCols - 1) * sizeof(SKvRowIdx) +
            pBuilder->nBoundBitmaps;  // add
      TD_ROW_SET_LEN(pBuilder->pBuf, len);
      TD_ROW_SET_SVER(pBuilder->pBuf, pBuilder->sver);
      TD_ROW_SET_NCOLS(pBuilder->pBuf, pBuilder->nBoundCols);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tdSRowGetBuf(SRowBuilder *pBuilder, void *pBuf) {
  pBuilder->pBuf = (STSRow *)pBuf;
  if (!pBuilder->pBuf) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  TASSERT(pBuilder->nBitmaps > 0 && pBuilder->flen > 0);

  uint32_t len = 0;
  switch (pBuilder->rowType) {
    case TD_ROW_TP:
#ifdef TD_SUPPORT_BITMAP
      pBuilder->pBitmap = tdGetBitmapAddrTp(pBuilder->pBuf, pBuilder->flen);
#endif
      break;
    case TD_ROW_KV:
#ifdef TD_SUPPORT_BITMAP
      pBuilder->pBitmap = tdGetBitmapAddrKv(pBuilder->pBuf, pBuilder->nBoundCols);
#endif
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tdSRowInitEx(SRowBuilder *pBuilder, void *pBuf, uint32_t allNullLen, uint32_t boundNullLen, int32_t nCols,
                     int32_t nBoundCols, int32_t flen) {
  if (tdSRowSetExtendedInfo(pBuilder, allNullLen, boundNullLen, nCols, nBoundCols, flen) < 0) {
    return terrno;
  }
  return tdSRowResetBuf(pBuilder, pBuf);
}

void tdSRowReset(SRowBuilder *pBuilder) {
  pBuilder->rowType = TD_ROW_TP;
  pBuilder->pBuf = NULL;
  pBuilder->nBoundCols = -1;
  pBuilder->nCols = -1;
  pBuilder->flen = -1;
  pBuilder->pBitmap = NULL;
}

int32_t tdSRowSetTpInfo(SRowBuilder *pBuilder, int32_t nCols, int32_t flen) {
  pBuilder->flen = flen;
  pBuilder->nCols = nCols;
  if (pBuilder->flen <= 0 || pBuilder->nCols <= 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifdef TD_SUPPORT_BITMAP
  // the primary TS key is stored separatedly
  pBuilder->nBitmaps = (int16_t)TD_BITMAP_BYTES(pBuilder->nCols - 1);
#else
  pBuilder->nBitmaps = 0;
  pBuilder->nBoundBitmaps = 0;
#endif
  return TSDB_CODE_SUCCESS;
}

int32_t tdSRowSetInfo(SRowBuilder *pBuilder, int32_t nCols, int32_t nBoundCols, int32_t flen) {
  pBuilder->flen = flen;
  pBuilder->nCols = nCols;
  pBuilder->nBoundCols = nBoundCols;
  if (pBuilder->flen <= 0 || pBuilder->nCols <= 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifdef TD_SUPPORT_BITMAP
  // the primary TS key is stored separatedly
  pBuilder->nBitmaps = (int16_t)TD_BITMAP_BYTES(pBuilder->nCols - 1);
  if (nBoundCols > 0) {
    pBuilder->nBoundBitmaps = (int16_t)TD_BITMAP_BYTES(pBuilder->nBoundCols - 1);
  } else {
    pBuilder->nBoundBitmaps = 0;
  }
#else
  pBuilder->nBitmaps = 0;
  pBuilder->nBoundBitmaps = 0;
#endif
  return TSDB_CODE_SUCCESS;
}

int32_t tdGetBitmapValType(const void *pBitmap, int16_t colIdx, TDRowValT *pValType, int8_t bitmapMode) {
  switch (bitmapMode) {
    case 0:
      tdGetBitmapValTypeII(pBitmap, colIdx, pValType);
      break;
    case -1:
    case 1:
      tdGetBitmapValTypeI(pBitmap, colIdx, pValType);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

bool tdIsBitmapValTypeNorm(const void *pBitmap, int16_t idx, int8_t bitmapMode) {
  TDRowValT valType = 0;
  tdGetBitmapValType(pBitmap, idx, &valType, bitmapMode);
  if (tdValTypeIsNorm(valType)) {
    return true;
  }
  return false;
}

int32_t tdSetBitmapValTypeII(void *pBitmap, int16_t colIdx, TDRowValT valType) {
  if (!pBitmap || colIdx < 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  int16_t nBytes = colIdx / TD_VTYPE_PARTS;
  int16_t nOffset = colIdx & TD_VTYPE_OPTR;
  char   *pDestByte = (char *)POINTER_SHIFT(pBitmap, nBytes);
  // use literal value directly and not use formula to simplify the codes
  switch (nOffset) {
    case 0:
      *pDestByte = ((*pDestByte) & 0x3F) | (valType << 6);
      // set the value and clear other partitions for offset 0
      // *pDestByte |= (valType << 6);
      break;
    case 1:
      *pDestByte = ((*pDestByte) & 0xCF) | (valType << 4);
      // *pDestByte |= (valType << 4);
      break;
    case 2:
      *pDestByte = ((*pDestByte) & 0xF3) | (valType << 2);
      // *pDestByte |= (valType << 2);
      break;
    case 3:
      *pDestByte = ((*pDestByte) & 0xFC) | valType;
      // *pDestByte |= (valType);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tdSetBitmapValType(void *pBitmap, int16_t colIdx, TDRowValT valType, int8_t bitmapMode) {
  switch (bitmapMode) {
    case 0:
      tdSetBitmapValTypeII(pBitmap, colIdx, valType);
      break;
    case -1:
    case 1:
      tdSetBitmapValTypeI(pBitmap, colIdx, valType);
      break;
    default:
      TASSERT(0);
      terrno = TSDB_CODE_INVALID_PARA;
      return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

void *tdGetBitmapAddr(STSRow *pRow, uint8_t rowType, uint32_t flen, col_id_t nKvCols) {
#ifdef TD_SUPPORT_BITMAP
  switch (rowType) {
    case TD_ROW_TP:
      return tdGetBitmapAddrTp(pRow, flen);
    case TD_ROW_KV:
      return tdGetBitmapAddrKv(pRow, nKvCols);
    default:
      break;
  }
#endif
  return NULL;
}

void tdSTSRowIterReset(STSRowIter *pIter, STSRow *pRow) {
  pIter->pRow = pRow;
  pIter->pBitmap = tdGetBitmapAddr(pRow, pRow->type, pIter->pSchema->flen, tdRowGetNCols(pRow));
  pIter->offset = 0;
  pIter->colIdx = PRIMARYKEY_TIMESTAMP_COL_ID;
  pIter->kvIdx = 0;
}

void tdSTSRowIterInit(STSRowIter *pIter, STSchema *pSchema) {
  pIter->pSchema = pSchema;
  pIter->maxColId = pSchema->columns[pSchema->numOfCols - 1].colId;
}

void tTSRowGetVal(STSRow *pRow, STSchema *pTSchema, int16_t iCol, SColVal *pColVal) {
  STColumn *pTColumn = &pTSchema->columns[iCol];
  SCellVal  cv;
  SValue    value;

  // ASSERT(iCol > 0);

  if (TD_IS_TP_ROW(pRow)) {
    tdSTpRowGetVal(pRow, pTColumn->colId, pTColumn->type, pTSchema->flen, pTColumn->offset, iCol - 1, &cv);
  } else if (TD_IS_KV_ROW(pRow)) {
    ASSERT(iCol > 0);
    tdSKvRowGetVal(pRow, pTColumn->colId, iCol - 1, &cv);
  } else {
    ASSERT(0);
  }

  if (tdValTypeIsNone(cv.valType)) {
    *pColVal = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
  } else if (tdValTypeIsNull(cv.valType)) {
    *pColVal = COL_VAL_NULL(pTColumn->colId, pTColumn->type);
  } else {
    if (IS_VAR_DATA_TYPE(pTColumn->type)) {
      value.nData = varDataLen(cv.val);
      value.pData = varDataVal(cv.val);
    } else {
      tGetValue(cv.val, &value, pTColumn->type);
    }

    *pColVal = COL_VAL_VALUE(pTColumn->colId, pTColumn->type, value);
  }
}
