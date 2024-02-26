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
#include "tlog.h"

static bool  tdSTSRowIterGetTpVal(STSRowIter *pIter, col_type_t colType, int32_t offset, SCellVal *pVal);
static bool  tdSTSRowIterGetKvVal(STSRowIter *pIter, col_id_t colId, col_id_t *nIdx, SCellVal *pVal);

void tdSTSRowIterInit(STSRowIter *pIter, STSchema *pSchema) {
  pIter->pSchema = pSchema;
  pIter->maxColId = pSchema->columns[pSchema->numOfCols - 1].colId;
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
  pIter->colIdx = 0;  // PRIMARYKEY_TIMESTAMP_COL_ID;
  pIter->kvIdx = 0;
}

bool tdSTSRowIterFetch(STSRowIter *pIter, col_id_t colId, col_type_t colType, SCellVal *pVal) {
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
    tdSTSRowIterGetTpVal(pIter, pCol->type, pCol->offset, pVal);
    ++pIter->colIdx;
  } else if (TD_IS_KV_ROW(pIter->pRow)) {
    return tdSTSRowIterGetKvVal(pIter, colId, &pIter->kvIdx, pVal);
  } else {
    pVal->valType = TD_VTYPE_NONE;
    terrno = TSDB_CODE_INVALID_PARA;
    if (COL_REACH_END(colId, pIter->maxColId)) return false;
  }
  return true;
}

bool tdSTSRowIterGetTpVal(STSRowIter *pIter, col_type_t colType, int32_t offset, SCellVal *pVal) {
  STSRow *pRow = pIter->pRow;
  if (pRow->statis == 0) {
    pVal->valType = TD_VTYPE_NORM;
    if (IS_VAR_DATA_TYPE(colType)) {
      pVal->val = POINTER_SHIFT(pRow, *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(pRow), offset));
    } else {
      pVal->val = POINTER_SHIFT(TD_ROW_DATA(pRow), offset);
    }
    return true;
  }

  if (tdGetBitmapValType(pIter->pBitmap, pIter->colIdx - 1, &pVal->valType, 0) != TSDB_CODE_SUCCESS) {
    pVal->valType = TD_VTYPE_NONE;
    return true;
  }

  if (pVal->valType == TD_VTYPE_NORM) {
    if (IS_VAR_DATA_TYPE(colType)) {
      pVal->val = POINTER_SHIFT(pRow, *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(pRow), offset));
    } else {
      pVal->val = POINTER_SHIFT(TD_ROW_DATA(pRow), offset);
    }
  }

  return true;
}

int32_t tdGetBitmapValTypeII(const void *pBitmap, int16_t colIdx, TDRowValT *pValType) {
  if (!pBitmap || colIdx < 0) {
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
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

#if 0
int32_t tdGetBitmapValTypeI(const void *pBitmap, int16_t colIdx, TDRowValT *pValType) {
  if (!pBitmap || colIdx < 0) {
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
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}
#endif

int32_t tdGetBitmapValType(const void *pBitmap, int16_t colIdx, TDRowValT *pValType, int8_t bitmapMode) {
  switch (bitmapMode) {
    case 0:
      tdGetBitmapValTypeII(pBitmap, colIdx, pValType);
      break;
#if 0
    case -1:
    case 1:
      tdGetBitmapValTypeI(pBitmap, colIdx, pValType);
      break;
#endif
    default:
      terrno = TSDB_CODE_INVALID_PARA;
      return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

bool tdSTSRowIterGetKvVal(STSRowIter *pIter, col_id_t colId, col_id_t *nIdx, SCellVal *pVal) {
  STSRow    *pRow = pIter->pRow;
  SKvRowIdx *pKvIdx = NULL;
  bool       colFound = false;
  col_id_t   kvNCols = tdRowGetNCols(pRow) - 1;
  void      *pColIdx = TD_ROW_COL_IDX(pRow);
  while (*nIdx < kvNCols) {
    pKvIdx = (SKvRowIdx *)POINTER_SHIFT(pColIdx, *nIdx * sizeof(SKvRowIdx));
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

  if (tdGetBitmapValType(pIter->pBitmap, pIter->kvIdx - 1, &pVal->valType, 0) != TSDB_CODE_SUCCESS) {
    pVal->valType = TD_VTYPE_NONE;
  }

  return true;
}

// #ifdef BUILD_NO_CALL
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
static uint8_t tdGetBitmapByte(uint8_t byte);
static bool    tdSTpRowGetVal(STSRow *pRow, col_id_t colId, col_type_t colType, int32_t flen, uint32_t offset,
                              col_id_t colIdx, SCellVal *pVal);
static bool    tdSKvRowGetVal(STSRow *pRow, col_id_t colId, col_id_t colIdx, SCellVal *pVal);
static void    tdSCellValPrint(SCellVal *pVal, int8_t colType);

// implementation
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
  STColumn *cols = (STColumn *)&iter.pSchema->columns;
  while (true) {
    SCellVal sVal = {.valType = 255, NULL};
    if (!tdSTSRowIterNext(&iter, &sVal)) {
      break;
    }
    ASSERT(sVal.valType == 0 || sVal.valType == 1 || sVal.valType == 2);
    tdSCellValPrint(&sVal, cols[iter.colIdx - 1].type);
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
  if (!pVal->val) {
    printf("BadVal ");
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
    case TSDB_DATA_TYPE_GEOMETRY:
      printf("GEOMETRY ");
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

static FORCE_INLINE int32_t compareKvRowColId(const void *key1, const void *key2) {
  if (*(col_id_t *)key1 > ((SKvRowIdx *)key2)->colId) {
    return 1;
  } else if (*(col_id_t *)key1 < ((SKvRowIdx *)key2)->colId) {
    return -1;
  } else {
    return 0;
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
  tdGetTpRowValOfCol(pVal, pRow, pBitmap, colType, offset, colIdx);
  return true;
}


bool tdSTSRowIterNext(STSRowIter *pIter, SCellVal *pVal) {
  if (pIter->colIdx >= pIter->pSchema->numOfCols) {
    return false;
  }

  STColumn *pCol = &pIter->pSchema->columns[pIter->colIdx];

  if (pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    pVal->val = &pIter->pRow->ts;
    pVal->valType = TD_VTYPE_NORM;
    ++pIter->colIdx;
    return true;
  }

  if (TD_IS_TP_ROW(pIter->pRow)) {
    tdSTSRowIterGetTpVal(pIter, pCol->type, pCol->offset, pVal);
  } else if (TD_IS_KV_ROW(pIter->pRow)) {
    tdSTSRowIterGetKvVal(pIter, pCol->colId, &pIter->kvIdx, pVal);
  } else {
    ASSERT(0);
  }
  ++pIter->colIdx;

  return true;
}

int32_t tdSTSRowNew(SArray *pArray, STSchema *pTSchema, STSRow **ppRow, int8_t rowType) {
  STColumn *pTColumn;
  SColVal  *pColVal;
  int32_t   nColVal = taosArrayGetSize(pArray);
  int32_t   varDataLen = 0;
  int32_t   nonVarDataLen = 0;
  int32_t   maxVarDataLen = 0;
  int32_t   iColVal = 0;
  int32_t   nBound = 0;
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
    if (pColVal && !COL_VAL_IS_NONE(pColVal)) {
      ++nBound;
    }

    if (iColumn == 0) {
      ASSERT(pColVal && pColVal->cid == pTColumn->colId);
      ASSERT(pTColumn->type == TSDB_DATA_TYPE_TIMESTAMP);
      ASSERT(pTColumn->colId == PRIMARYKEY_TIMESTAMP_COL_ID);
    } else {
      if (IS_VAR_DATA_TYPE(pTColumn->type)) {
        if (pColVal && COL_VAL_IS_VALUE(pColVal)) {
          varDataLen += (pColVal->value.nData + sizeof(VarDataLenT));
          if (maxVarDataLen < (pColVal->value.nData + sizeof(VarDataLenT))) {
            maxVarDataLen = pColVal->value.nData + sizeof(VarDataLenT);
          }
        } else {
          varDataLen += sizeof(VarDataLenT);
          if (pTColumn->type == TSDB_DATA_TYPE_VARCHAR || pTColumn->type == TSDB_DATA_TYPE_VARBINARY || pTColumn->type == TSDB_DATA_TYPE_GEOMETRY) {
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
      } else {
        if(pColVal && COL_VAL_IS_VALUE(pColVal)) {
          nonVarDataLen += TYPE_BYTES[pTColumn->type];
        }
      }
    }

    ++iColVal;
  }

  int32_t rowTotalLen = 0;
  if (rowType == TD_ROW_TP) {
    rowTotalLen = sizeof(STSRow) + pTSchema->flen + varDataLen + TD_BITMAP_BYTES(pTSchema->numOfCols - 1);
  } else {
    rowTotalLen = sizeof(STSRow) + sizeof(col_id_t) + varDataLen + nonVarDataLen + (nBound - 1) * sizeof(SKvRowIdx) +
                  TD_BITMAP_BYTES(nBound - 1);
  }
    if (!(*ppRow)) {
      *ppRow = (STSRow *)taosMemoryCalloc(1, rowTotalLen);
      isAlloc = true;
  }

  if (!(*ppRow)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (maxVarDataLen > 0) {
    varBuf = taosMemoryMalloc(maxVarDataLen);
    if (!varBuf) {
      if (isAlloc) {
        taosMemoryFreeClear(*ppRow);
      }
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  SRowBuilder rb = {.rowType = rowType};
  tdSRowInit(&rb, pTSchema->version);
  tdSRowSetInfo(&rb, pTSchema->numOfCols, nBound, pTSchema->flen);
  tdSRowResetBuf(&rb, *ppRow);
  int32_t iBound = 0;

  iColVal = 0;
  for (int32_t iColumn = 0; iColumn < pTSchema->numOfCols; ++iColumn) {
    pTColumn = &pTSchema->columns[iColumn];

    TDRowValT   valType = TD_VTYPE_NORM;
    const void *val = NULL;
    if (iColVal < nColVal) {
      pColVal = (SColVal *)taosArrayGet(pArray, iColVal);
      if (COL_VAL_IS_NONE(pColVal)) {
        valType = TD_VTYPE_NONE;
      } else if (COL_VAL_IS_NULL(pColVal)) {
        valType = TD_VTYPE_NULL;
        ++iBound;
      } else if (IS_VAR_DATA_TYPE(pTColumn->type)) {
        varDataSetLen(varBuf, pColVal->value.nData);
        if (pColVal->value.nData != 0) {
          memcpy(varDataVal(varBuf), pColVal->value.pData, pColVal->value.nData);
        }
        val = varBuf;
        ++iBound;
      } else {
        val = (const void *)&pColVal->value.val;
        ++iBound;
      }
    } else {
      // pColVal = NULL;
      valType = TD_VTYPE_NONE;
    }

    if (TD_IS_TP_ROW(rb.pBuf)) {
      tdAppendColValToRow(&rb, pTColumn->colId, pTColumn->type, valType, val, true, pTColumn->offset, iColVal);
    } else {
      tdAppendColValToRow(&rb, pTColumn->colId, pTColumn->type, valType, val, true, rb.offset, iBound - 1);
    }

    ++iColVal;
  }
  tdSRowEnd(&rb);

  taosMemoryFreeClear(varBuf);

  return 0;
}

static FORCE_INLINE int32_t tdCompareColId(const void *arg1, const void *arg2) {
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
    tdGetTpRowValOfCol(pVal, pRow, pIter->pBitmap, pCol->type, pCol->offset, colIdx - 1);
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
#if 0
int32_t tdSetBitmapValTypeI(void *pBitmap, int16_t colIdx, TDRowValT valType) {
  if (!pBitmap || colIdx < 0) {
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
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
}
#endif

int32_t tdGetKvRowValOfCol(SCellVal *output, STSRow *pRow, void *pBitmap, int32_t offset, int16_t colIdx) {
#ifdef TD_SUPPORT_BITMAP
  ASSERT(colIdx < tdRowGetNCols(pRow) - 1);
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
  if (pRow->statis == 0) {
    output->valType = TD_VTYPE_NORM;
    if (IS_VAR_DATA_TYPE(colType)) {
      output->val = POINTER_SHIFT(pRow, *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(pRow), offset));
    } else {
      output->val = POINTER_SHIFT(TD_ROW_DATA(pRow), offset);
    }
    return TSDB_CODE_SUCCESS;
  }

  if (tdGetBitmapValType(pBitmap, colIdx, &output->valType, 0) != TSDB_CODE_SUCCESS) {
    output->valType = TD_VTYPE_NONE;
    return terrno;
  }

  if (output->valType == TD_VTYPE_NORM) {
    if (IS_VAR_DATA_TYPE(colType)) {
      output->val = POINTER_SHIFT(pRow, *(VarDataOffsetT *)POINTER_SHIFT(TD_ROW_DATA(pRow), offset));
    } else {
      output->val = POINTER_SHIFT(TD_ROW_DATA(pRow), offset);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tdAppendColValToRow(SRowBuilder *pBuilder, col_id_t colId, int8_t colType, TDRowValT valType, const void *val,
                            bool isCopyVarData, int32_t offset, col_id_t colIdx) {
  STSRow *pRow = pBuilder->pBuf;
  if (!val) {
#ifdef TD_SUPPORT_BITMAP
    if (valType == TD_VTYPE_NORM) {
      terrno = TSDB_CODE_INVALID_PTR;
      return terrno;
    }
#else
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
#endif
  }
  // TS KEY is stored in STSRow.ts and not included in STSRow.data field.
  if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    if (!val) {
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
    }
    TD_ROW_KEY(pRow) = *(TSKEY *)val;
    // The primary TS key is Norm all the time, thus its valType is not stored in bitmap.
    return TSDB_CODE_SUCCESS;
  }
  // TODO:  We can avoid the type judegement by FP, but would prevent the inline scheme.

  switch (valType) {
    case TD_VTYPE_NORM:
      break;
    case TD_VTYPE_NULL:
      if (!pBuilder->hasNull) pBuilder->hasNull = true;
      break;
    case TD_VTYPE_NONE:
      if (!pBuilder->hasNone) pBuilder->hasNone = true;
      return TSDB_CODE_SUCCESS;
    default:
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }

  if (TD_IS_TP_ROW(pRow)) {
    tdAppendColValToTpRow(pBuilder, valType, val, isCopyVarData, colType, colIdx, offset);
  } else {
    tdAppendColValToKvRow(pBuilder, valType, val, isCopyVarData, colType, colIdx, offset, colId);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tdAppendColValToKvRow(SRowBuilder *pBuilder, TDRowValT valType, const void *val, bool isCopyVarData,
                              int8_t colType, int16_t colIdx, int32_t offset, col_id_t colId) {
  if (colIdx < 1) {
    terrno = TSDB_CODE_INVALID_PARA;
    ASSERTS(0, "colIdx is %" PRIi64, colIdx);
    return terrno;
  }
  --colIdx;

#ifdef TD_SUPPORT_BITMAP
  if (tdSetBitmapValType(pBuilder->pBitmap, colIdx, valType, 0) != TSDB_CODE_SUCCESS) {
    return terrno;
  }
#endif

  STSRow *row = pBuilder->pBuf;
  // No need to store None/Null values.
  SKvRowIdx *pColIdx = (SKvRowIdx *)POINTER_SHIFT(TD_ROW_COL_IDX(row), offset);
  pColIdx->colId = colId;
  pColIdx->offset = TD_ROW_LEN(row);  // the offset include the TD_ROW_HEAD_LEN
  pBuilder->offset += sizeof(SKvRowIdx);
  if (valType == TD_VTYPE_NORM) {
    char *ptr = (char *)POINTER_SHIFT(row, TD_ROW_LEN(row));
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

  return 0;
}

int32_t tdAppendColValToTpRow(SRowBuilder *pBuilder, TDRowValT valType, const void *val, bool isCopyVarData,
                              int8_t colType, int16_t colIdx, int32_t offset) {
  if (colIdx < 1) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
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
  if (valType == TD_VTYPE_NORM) {
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

  return 0;
}
#if 0
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
#endif

int32_t tdSRowResetBuf(SRowBuilder *pBuilder, void *pBuf) {
  pBuilder->pBuf = (STSRow *)pBuf;
  if (!pBuilder->pBuf) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  if (pBuilder->hasNone) pBuilder->hasNone = false;
  if (pBuilder->hasNull) pBuilder->hasNull = false;

  TD_ROW_SET_INFO(pBuilder->pBuf, 0);
  TD_ROW_SET_TYPE(pBuilder->pBuf, pBuilder->rowType);

  ASSERT(pBuilder->nBitmaps > 0 && pBuilder->flen > 0);

  uint32_t len = 0;
  switch (pBuilder->rowType) {
    case TD_ROW_TP:
#ifdef TD_SUPPORT_BITMAP
      pBuilder->pBitmap = tdGetBitmapAddrTp(pBuilder->pBuf, pBuilder->flen);
      memset(pBuilder->pBitmap, TD_VTYPE_NONE_BYTE_II, pBuilder->nBitmaps);
#endif
      // the primary TS key is stored separatedly
      len = TD_ROW_HEAD_LEN + pBuilder->flen + pBuilder->nBitmaps;
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
      pBuilder->offset = 0;
      break;
    default:
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tdSRowGetBuf(SRowBuilder *pBuilder, void *pBuf) {
  pBuilder->pBuf = (STSRow *)pBuf;
  if (!pBuilder->pBuf) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  ASSERT(pBuilder->nBitmaps > 0 && pBuilder->flen > 0);

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
      terrno = TSDB_CODE_INVALID_PARA;
      return terrno;
  }
  return TSDB_CODE_SUCCESS;
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

#if 0
bool tdIsBitmapValTypeNorm(const void *pBitmap, int16_t idx, int8_t bitmapMode) {
  TDRowValT valType = 0;
  tdGetBitmapValType(pBitmap, idx, &valType, bitmapMode);
  if (tdValTypeIsNorm(valType)) {
    return true;
  }
  return false;
}
#endif

int32_t tdSetBitmapValTypeII(void *pBitmap, int16_t colIdx, TDRowValT valType) {
  if (!pBitmap || colIdx < 0) {
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
#if 0
    case -1:
    case 1:
      tdSetBitmapValTypeI(pBitmap, colIdx, valType);
      break;
#endif
    default:
      terrno = TSDB_CODE_INVALID_PARA;
      return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}



void tTSRowGetVal(STSRow *pRow, STSchema *pTSchema, int16_t iCol, SColVal *pColVal) {
  STColumn *pTColumn = &pTSchema->columns[iCol];
  SCellVal  cv = {0};

  ASSERT((pTColumn->colId == PRIMARYKEY_TIMESTAMP_COL_ID) || (iCol > 0));

  if (TD_IS_TP_ROW(pRow)) {
    tdSTpRowGetVal(pRow, pTColumn->colId, pTColumn->type, pTSchema->flen, pTColumn->offset, iCol - 1, &cv);
  } else if (TD_IS_KV_ROW(pRow)) {
    tdSKvRowGetVal(pRow, pTColumn->colId, iCol - 1, &cv);
  } else {
    ASSERT(0);
  }

  if (tdValTypeIsNone(cv.valType)) {
    *pColVal = COL_VAL_NONE(pTColumn->colId, pTColumn->type);
  } else if (tdValTypeIsNull(cv.valType)) {
    *pColVal = COL_VAL_NULL(pTColumn->colId, pTColumn->type);
  } else {
    pColVal->cid = pTColumn->colId;
    pColVal->type = pTColumn->type;
    pColVal->flag = CV_FLAG_VALUE;

    if (IS_VAR_DATA_TYPE(pTColumn->type)) {
      pColVal->value.nData = varDataLen(cv.val);
      pColVal->value.pData = varDataVal(cv.val);
    } else {
      memcpy(&pColVal->value.val, cv.val, tDataTypes[pTColumn->type].bytes);
    }
  }
}