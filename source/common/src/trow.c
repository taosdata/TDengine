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

const uint8_t tdVTypeByte[3] = {
    TD_VTYPE_NORM_BYTE,  // TD_VTYPE_NORM
    TD_VTYPE_NONE_BYTE,  // TD_VTYPE_NONE
    TD_VTYPE_NULL_BYTE,  // TD_VTYPE_NULL
};

// static void dataColSetNEleNull(SDataCol *pCol, int nEle);
static void tdMergeTwoDataCols(SDataCols *target, SDataCols *src1, int *iter1, int limit1, SDataCols *src2, int *iter2,
                               int limit2, int tRows, bool forceSetNull);

static FORCE_INLINE void dataColSetNullAt(SDataCol *pCol, int index, bool setBitmap) {
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    pCol->dataOff[index] = pCol->len;
    char *ptr = POINTER_SHIFT(pCol->pData, pCol->len);
    setVardataNull(ptr, pCol->type);
    pCol->len += varDataTLen(ptr);
  } else {
    setNull(POINTER_SHIFT(pCol->pData, TYPE_BYTES[pCol->type] * index), pCol->type, pCol->bytes);
    pCol->len += TYPE_BYTES[pCol->type];
  }
  if (setBitmap) {
    tdSetBitmapValType(pCol->pBitmap, index, TD_VTYPE_NONE);
  }
}

// static void dataColSetNEleNull(SDataCol *pCol, int nEle) {
//   if (IS_VAR_DATA_TYPE(pCol->type)) {
//     pCol->len = 0;
//     for (int i = 0; i < nEle; i++) {
//       dataColSetNullAt(pCol, i);
//     }
//   } else {
//     setNullN(pCol->pData, pCol->type, pCol->bytes, nEle);
//     pCol->len = TYPE_BYTES[pCol->type] * nEle;
//   }
// }

int32_t tdSetBitmapValTypeN(void *pBitmap, int16_t nEle, TDRowValT valType) {
  TASSERT(valType < TD_VTYPE_MAX);
  int16_t nBytes = nEle / TD_VTYPE_PARTS;
  for (int i = 0; i < nBytes; ++i) {
    *(uint8_t *)pBitmap = tdVTypeByte[valType];
    pBitmap = POINTER_SHIFT(pBitmap, 1);
  }
  int16_t nLeft = nEle - nBytes * TD_VTYPE_BITS;

  for (int j = 0; j < nLeft; ++j) {
    tdSetBitmapValType(pBitmap, j, valType);
  }
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void dataColSetNoneAt(SDataCol *pCol, int index, bool setBitmap) {
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    pCol->dataOff[index] = pCol->len;
    char *ptr = POINTER_SHIFT(pCol->pData, pCol->len);
    setVardataNull(ptr, pCol->type);
    pCol->len += varDataTLen(ptr);
  } else {
    setNull(POINTER_SHIFT(pCol->pData, TYPE_BYTES[pCol->type] * index), pCol->type, pCol->bytes);
    pCol->len += TYPE_BYTES[pCol->type];
  }
  if(setBitmap) {
    tdSetBitmapValType(pCol->pBitmap, index, TD_VTYPE_NONE);
  }
}

static void dataColSetNEleNone(SDataCol *pCol, int nEle) {
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    pCol->len = 0;
    for (int i = 0; i < nEle; ++i) {
      dataColSetNoneAt(pCol, i, false);
    }
  } else {
    setNullN(pCol->pData, pCol->type, pCol->bytes, nEle);
    pCol->len = TYPE_BYTES[pCol->type] * nEle;
  }
#ifdef TD_SUPPORT_BITMAP
  tdSetBitmapValTypeN(pCol->pBitmap, nEle, TD_VTYPE_NONE);
#endif
}

#if 0
void trbSetRowInfo(SRowBuilder *pRB, bool del, uint16_t sver) {
  // TODO
}

void trbSetRowVersion(SRowBuilder *pRB, uint64_t ver) {
  // TODO
}

void trbSetRowTS(SRowBuilder *pRB, TSKEY ts) {
  // TODO
}

int trbWriteCol(SRowBuilder *pRB, void *pData, col_id_t cid) {
  // TODO
  return 0;
}

#endif

STSRow* tdRowDup(STSRow *row) {
  STSRow* trow = malloc(TD_ROW_LEN(row));
  if (trow == NULL) return NULL;

  tdRowCpy(trow, row);
  return trow;
}

int tdAppendValToDataCol(SDataCol *pCol, TDRowValT valType, const void *val, int numOfRows, int maxPoints) {
  TASSERT(pCol != NULL);

  // Assume that the columns not specified during insert/upsert mean None.
  if (isAllRowsNone(pCol)) {
    if (tdValIsNone(valType)) {
      // all None value yet, just return
      return 0;
    }

    if (tdAllocMemForCol(pCol, maxPoints) < 0) return -1;
    if (numOfRows > 0) {
      // Find the first not None value, fill all previous values as None
      dataColSetNEleNone(pCol, numOfRows);
    }
  }
  if (!tdValTypeIsNorm(valType)) {
    // TODO:
    // 1. back compatibility and easy to debug with codes of 2.0 to save NULL values.
    // 2. later on, considering further optimization, don't save Null/None for VarType.
    val = getNullValue(pCol->type);
  }
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    // set offset
    pCol->dataOff[numOfRows] = pCol->len;
    // Copy data
    memcpy(POINTER_SHIFT(pCol->pData, pCol->len), val, varDataTLen(val));
    // Update the length
    pCol->len += varDataTLen(val);
  } else {
    ASSERT(pCol->len == TYPE_BYTES[pCol->type] * numOfRows);
    memcpy(POINTER_SHIFT(pCol->pData, pCol->len), val, pCol->bytes);
    pCol->len += pCol->bytes;
  }
#ifdef TD_SUPPORT_BITMAP
  tdSetBitmapValType(pCol->pBitmap, numOfRows, valType);
#endif
  return 0;
}

// internal
static int32_t tdAppendTpRowToDataCol(STSRow *pRow, STSchema *pSchema, SDataCols *pCols) {
  ASSERT(pCols->numOfRows == 0 || dataColsKeyLast(pCols) < TD_ROW_KEY(pRow));

  int   rcol = 1;
  int   dcol = 1;
  void *pBitmap = tdGetBitmapAddrTp(pRow, pSchema->flen);

  SDataCol *pDataCol = &(pCols->cols[0]);
  if (pDataCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    tdAppendValToDataCol(pDataCol, TD_VTYPE_NORM, &pRow->ts,  pCols->numOfRows, pCols->maxPoints);
  }

  while (dcol < pCols->numOfCols) {
    pDataCol = &(pCols->cols[dcol]);
    if (rcol >= schemaNCols(pSchema)) {
      tdAppendValToDataCol(pDataCol, TD_VTYPE_NULL, NULL, pCols->numOfRows, pCols->maxPoints);
      ++dcol;
      continue;
    }

    STColumn *pRowCol = schemaColAt(pSchema, rcol);
    SCellVal  sVal = {0};
    if (pRowCol->colId == pDataCol->colId) {
      if (tdGetTpRowValOfCol(&sVal, pRow, pBitmap, pRowCol->type, pRowCol->offset - sizeof(TSKEY), rcol - 1) < 0) {
        return terrno;
      }
      tdAppendValToDataCol(pDataCol, sVal.valType, sVal.val, pCols->numOfRows, pCols->maxPoints);
      ++dcol;
      ++rcol;
    } else if (pRowCol->colId < pDataCol->colId) {
      ++rcol;
    } else {
      tdAppendValToDataCol(pDataCol, TD_VTYPE_NULL, NULL, pCols->numOfRows, pCols->maxPoints);
      ++dcol;
    }
  }
  ++pCols->numOfRows;

  return TSDB_CODE_SUCCESS;
}
// internal
static int32_t tdAppendKvRowToDataCol(STSRow *pRow, STSchema *pSchema, SDataCols *pCols) {
  ASSERT(pCols->numOfRows == 0 || dataColsKeyLast(pCols) < TD_ROW_KEY(pRow));

  int   rcol = 0;
  int   dcol = 1;
  int   tRowCols = tdRowGetNCols(pRow) - 1;  // the primary TS key not included in kvRowColIdx part
  int   tSchemaCols = schemaNCols(pSchema) - 1;
  void *pBitmap = tdGetBitmapAddrKv(pRow, tdRowGetNCols(pRow));

  SDataCol *pDataCol = &(pCols->cols[0]);
  if (pDataCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    tdAppendValToDataCol(pDataCol, TD_VTYPE_NORM, &pRow->ts, pCols->numOfRows, pCols->maxPoints);
  }

  while (dcol < pCols->numOfCols) {
    pDataCol = &(pCols->cols[dcol]);
    if (rcol >= tRowCols || rcol >= tSchemaCols) {
      tdAppendValToDataCol(pDataCol, TD_VTYPE_NULL, NULL, pCols->numOfRows, pCols->maxPoints);
      ++dcol;
      continue;
    }

    SKvRowIdx *pIdx = tdKvRowColIdxAt(pRow, rcol);
    int16_t    colIdx = -1;
    if (pIdx) {
      colIdx = POINTER_DISTANCE(pRow->data, pIdx) / sizeof(SKvRowIdx);
    }
    SCellVal sVal = {0};
    if (pIdx->colId == pDataCol->colId) {
      if (tdGetKvRowValOfCol(&sVal, pRow, pBitmap, pIdx->offset, colIdx) < 0) {
        return terrno;
      }
      tdAppendValToDataCol(pDataCol, sVal.valType, sVal.val, pCols->numOfRows, pCols->maxPoints);
      ++dcol;
      ++rcol;
    } else if (pIdx->colId < pDataCol->colId) {
      ++rcol;
    } else {
      tdAppendValToDataCol(pDataCol, TD_VTYPE_NULL, NULL, pCols->numOfRows, pCols->maxPoints);
      ++dcol;
    }
  }
  ++pCols->numOfRows;

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief exposed
 *
 * @param pRow
 * @param pSchema
 * @param pCols
 * @param forceSetNull
 */
int32_t tdAppendSTSRowToDataCol(STSRow *pRow, STSchema *pSchema, SDataCols *pCols, bool forceSetNull) {
  if (TD_IS_TP_ROW(pRow)) {
    return tdAppendTpRowToDataCol(pRow, pSchema, pCols);
  } else if (TD_IS_KV_ROW(pRow)) {
    return tdAppendKvRowToDataCol(pRow, pSchema, pCols);
  } else {
    ASSERT(0);
  }
  return TSDB_CODE_SUCCESS;
}

int tdMergeDataCols(SDataCols *target, SDataCols *source, int rowsToMerge, int *pOffset, bool forceSetNull) {
  ASSERT(rowsToMerge > 0 && rowsToMerge <= source->numOfRows);
  ASSERT(target->numOfCols == source->numOfCols);
  int offset = 0;

  if (pOffset == NULL) {
    pOffset = &offset;
  }

  SDataCols *pTarget = NULL;

  if ((target->numOfRows == 0) || (dataColsKeyLast(target) < dataColsKeyAtRow(source, *pOffset))) {  // No overlap
    ASSERT(target->numOfRows + rowsToMerge <= target->maxPoints);
    for (int i = 0; i < rowsToMerge; i++) {
      for (int j = 0; j < source->numOfCols; j++) {
        if (source->cols[j].len > 0 || target->cols[j].len > 0) {
          SCellVal sVal = {0};
          if (tdGetColDataOfRow(&sVal, source->cols + j, i + (*pOffset)) < 0) {
            TASSERT(0);
          }
          tdAppendValToDataCol(target->cols + j, sVal.valType, sVal.val, target->numOfRows, target->maxPoints);
        }
      }
      ++target->numOfRows;
    }
    (*pOffset) += rowsToMerge;
  } else {
    pTarget = tdDupDataCols(target, true);
    if (pTarget == NULL) goto _err;

    int iter1 = 0;
    tdMergeTwoDataCols(target, pTarget, &iter1, pTarget->numOfRows, source, pOffset, source->numOfRows,
                       pTarget->numOfRows + rowsToMerge, forceSetNull);
  }

  tdFreeDataCols(pTarget);
  return 0;

_err:
  tdFreeDataCols(pTarget);
  return -1;
}

// src2 data has more priority than src1
static void tdMergeTwoDataCols(SDataCols *target, SDataCols *src1, int *iter1, int limit1, SDataCols *src2, int *iter2,
                               int limit2, int tRows, bool forceSetNull) {
  tdResetDataCols(target);
  ASSERT(limit1 <= src1->numOfRows && limit2 <= src2->numOfRows);

  while (target->numOfRows < tRows) {
    if (*iter1 >= limit1 && *iter2 >= limit2) break;

    TSKEY key1 = (*iter1 >= limit1) ? INT64_MAX : dataColsKeyAt(src1, *iter1);
    TKEY  tkey1 = (*iter1 >= limit1) ? TKEY_NULL : dataColsTKeyAt(src1, *iter1);
    TSKEY key2 = (*iter2 >= limit2) ? INT64_MAX : dataColsKeyAt(src2, *iter2);
    // TKEY  tkey2 = (*iter2 >= limit2) ? TKEY_NULL : dataColsTKeyAt(src2, *iter2);

    ASSERT(tkey1 == TKEY_NULL || (!TKEY_IS_DELETED(tkey1)));

    if (key1 < key2) {
      for (int i = 0; i < src1->numOfCols; i++) {
        ASSERT(target->cols[i].type == src1->cols[i].type);
        if (src1->cols[i].len > 0 || target->cols[i].len > 0) {
          SCellVal sVal = {0};
          if (tdGetColDataOfRow(&sVal, src1->cols + i, *iter1) < 0) {
            TASSERT(0);
          }
          tdAppendValToDataCol(&(target->cols[i]), sVal.valType, sVal.val, target->numOfRows, target->maxPoints);
        }
      }

      target->numOfRows++;
      (*iter1)++;
    } else if (key1 >= key2) {
      // if ((key1 > key2) || (key1 == key2 && !TKEY_IS_DELETED(tkey2))) {
      if ((key1 > key2) || (key1 == key2)) {
        for (int i = 0; i < src2->numOfCols; i++) {
          SCellVal sVal = {0};
          ASSERT(target->cols[i].type == src2->cols[i].type);
          if (tdGetColDataOfRow(&sVal, src2->cols + i, *iter2) < 0) {
            TASSERT(0);
          }
          if (src2->cols[i].len > 0 && !tdValTypeIsNull(sVal.valType)) {
            tdAppendValToDataCol(&(target->cols[i]), sVal.valType, sVal.val, target->numOfRows, target->maxPoints);
          } else if (!forceSetNull && key1 == key2 && src1->cols[i].len > 0) {
            if (tdGetColDataOfRow(&sVal, src1->cols + i, *iter1) < 0) {
              TASSERT(0);
            }
            tdAppendValToDataCol(&(target->cols[i]), sVal.valType, sVal.val, target->numOfRows, target->maxPoints);
          } else if (target->cols[i].len > 0) {
            dataColSetNullAt(&target->cols[i], target->numOfRows, true);
          }
        }
        target->numOfRows++;
      }

      (*iter2)++;
      if (key1 == key2) (*iter1)++;
    }

    ASSERT(target->numOfRows <= target->maxPoints);
  }
}



STSRow* mergeTwoRows(void *buffer, STSRow* row1, STSRow *row2, STSchema *pSchema1, STSchema *pSchema2) {
#if 0
  ASSERT(TD_ROW_KEY(row1) == TD_ROW_KEY(row2));
  ASSERT(schemaVersion(pSchema1) == TD_ROW_SVER(row1));
  ASSERT(schemaVersion(pSchema2) == TD_ROW_SVER(row2));
  ASSERT(schemaVersion(pSchema1) >= schemaVersion(pSchema2));
#endif

#if 0
  SArray *stashRow = taosArrayInit(pSchema1->numOfCols, sizeof(SColInfo));
  if (stashRow == NULL) {
    return NULL;
  }

  STSRow  pRow = buffer;
  STpRow dataRow = memRowDataBody(pRow);
  memRowSetType(pRow, SMEM_ROW_DATA);
  dataRowSetVersion(dataRow, schemaVersion(pSchema1));  // use latest schema version
  dataRowSetLen(dataRow, (TDRowLenT)(TD_DATA_ROW_HEAD_SIZE + pSchema1->flen));

  TDRowLenT dataLen = 0, kvLen = TD_MEM_ROW_KV_HEAD_SIZE;

  int32_t  i = 0;  // row1
  int32_t  j = 0;  // row2
  int32_t  nCols1 = schemaNCols(pSchema1);
  int32_t  nCols2 = schemaNCols(pSchema2);
  SColInfo colInfo = {0};
  int32_t  kvIdx1 = 0, kvIdx2 = 0;

  while (i < nCols1) {
    STColumn *pCol = schemaColAt(pSchema1, i);
    void *    val1 = tdGetMemRowDataOfColEx(row1, pCol->colId, pCol->type, TD_DATA_ROW_HEAD_SIZE + pCol->offset, &kvIdx1);
    // if val1 != NULL, use val1;
    if (val1 != NULL && !isNull(val1, pCol->type)) {
      tdAppendColVal(dataRow, val1, pCol->type, pCol->offset);
      kvLen += tdGetColAppendLen(SMEM_ROW_KV, val1, pCol->type);
      setSColInfo(&colInfo, pCol->colId, pCol->type, val1);
      taosArrayPush(stashRow, &colInfo);
      ++i;  // next col
      continue;
    }

    void *val2 = NULL;
    while (j < nCols2) {
      STColumn *tCol = schemaColAt(pSchema2, j);
      if (tCol->colId < pCol->colId) {
        ++j;
        continue;
      }
      if (tCol->colId == pCol->colId) {
        val2 = tdGetMemRowDataOfColEx(row2, tCol->colId, tCol->type, TD_DATA_ROW_HEAD_SIZE + tCol->offset, &kvIdx2);
      } else if (tCol->colId > pCol->colId) {
        // set NULL
      }
      break;
    }  // end of while(j<nCols2)
    if (val2 == NULL) {
      val2 = (void *)getNullValue(pCol->type);
    }
    tdAppendColVal(dataRow, val2, pCol->type, pCol->offset);
    if (!isNull(val2, pCol->type)) {
      kvLen += tdGetColAppendLen(SMEM_ROW_KV, val2, pCol->type);
      setSColInfo(&colInfo, pCol->colId, pCol->type, val2);
      taosArrayPush(stashRow, &colInfo);
    }

    ++i;  // next col
  }

  dataLen = TD_ROW_LEN(pRow);

  if (kvLen < dataLen) {
    // scan stashRow and generate SKVRow
    memset(buffer, 0, sizeof(dataLen));
    STSRow tRow = buffer;
    memRowSetType(tRow, SMEM_ROW_KV);
    SKVRow kvRow = (SKVRow)memRowKvBody(tRow);
    int16_t nKvNCols = (int16_t) taosArrayGetSize(stashRow);
    kvRowSetLen(kvRow, (TDRowLenT)(TD_KV_ROW_HEAD_SIZE + sizeof(SColIdx) * nKvNCols));
    kvRowSetNCols(kvRow, nKvNCols);
    memRowSetKvVersion(tRow, pSchema1->version);

    int32_t toffset = 0;
    int16_t k;
    for (k = 0; k < nKvNCols; ++k) {
      SColInfo *pColInfo = taosArrayGet(stashRow, k);
      tdAppendKvColVal(kvRow, pColInfo->colVal, true, pColInfo->colId, pColInfo->colType, toffset);
      toffset += sizeof(SColIdx);
    }
    ASSERT(kvLen == TD_ROW_LEN(tRow));
  }
  taosArrayDestroy(stashRow);
  return buffer;
  #endif
  return NULL;
}
