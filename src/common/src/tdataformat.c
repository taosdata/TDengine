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
#include "tdataformat.h"
#include "tulog.h"
#include "talgo.h"
#include "tcoding.h"
#include "wchar.h"
#include "tarray.h"

static void dataColSetNEleNull(SDataCol *pCol, int nEle);
static void tdMergeTwoDataCols(SDataCols *target, SDataCols *src1, int *iter1, int limit1, SDataCols *src2, int *iter2,
                               int limit2, int tRows, bool forceSetNull);

int tdAllocMemForCol(SDataCol *pCol, int maxPoints) {
  int spaceNeeded = pCol->bytes * maxPoints;
  if(IS_VAR_DATA_TYPE(pCol->type)) {
    spaceNeeded += sizeof(VarDataOffsetT) * maxPoints;
  }
  if(pCol->spaceSize < spaceNeeded) {
    void* ptr = realloc(pCol->pData, spaceNeeded);
    if(ptr == NULL) {
      uDebug("malloc failure, size:%" PRId64 " failed, reason:%s", (int64_t)spaceNeeded,
             strerror(errno));
      return -1;
    } else {
      pCol->pData = ptr;
      pCol->spaceSize = spaceNeeded;
    }
  }
  if(IS_VAR_DATA_TYPE(pCol->type)) {
    pCol->dataOff = POINTER_SHIFT(pCol->pData, pCol->bytes * maxPoints);
  }
  return 0;
}

/**
 * Duplicate the schema and return a new object
 */
STSchema *tdDupSchema(STSchema *pSchema) {

  int tlen = sizeof(STSchema) + sizeof(STColumn) * schemaNCols(pSchema);
  STSchema *tSchema = (STSchema *)malloc(tlen);
  if (tSchema == NULL) return NULL;

  memcpy((void *)tSchema, (void *)pSchema, tlen);

  return tSchema;
}

/**
 * Encode a schema to dst, and return the next pointer
 */
int tdEncodeSchema(void **buf, STSchema *pSchema) {
  int tlen = 0;
  tlen += taosEncodeFixedI32(buf, schemaVersion(pSchema));
  tlen += taosEncodeFixedI32(buf, schemaNCols(pSchema));

  for (int i = 0; i < schemaNCols(pSchema); i++) {
    STColumn *pCol = schemaColAt(pSchema, i);
    tlen += taosEncodeFixedI8(buf, colType(pCol));
    tlen += taosEncodeFixedI16(buf, colColId(pCol));
    tlen += taosEncodeFixedI16(buf, colBytes(pCol));
  }

  return tlen;
}

/**
 * Decode a schema from a binary.
 */
void *tdDecodeSchema(void *buf, STSchema **pRSchema) {
  int version = 0;
  int numOfCols = 0;
  STSchemaBuilder schemaBuilder;

  buf = taosDecodeFixedI32(buf, &version);
  buf = taosDecodeFixedI32(buf, &numOfCols);

  if (tdInitTSchemaBuilder(&schemaBuilder, version) < 0) return NULL;

  for (int i = 0; i < numOfCols; i++) {
    int8_t  type = 0;
    int16_t colId = 0;
    int16_t bytes = 0;
    buf = taosDecodeFixedI8(buf, &type);
    buf = taosDecodeFixedI16(buf, &colId);
    buf = taosDecodeFixedI16(buf, &bytes);
    if (tdAddColToSchema(&schemaBuilder, type, colId, bytes) < 0) {
      tdDestroyTSchemaBuilder(&schemaBuilder);
      return NULL;
    }
  }

  *pRSchema = tdGetSchemaFromBuilder(&schemaBuilder);
  tdDestroyTSchemaBuilder(&schemaBuilder);
  return buf;
}

int tdInitTSchemaBuilder(STSchemaBuilder *pBuilder, int32_t version) {
  if (pBuilder == NULL) return -1;

  pBuilder->tCols = 256;
  pBuilder->columns = (STColumn *)malloc(sizeof(STColumn) * pBuilder->tCols);
  if (pBuilder->columns == NULL) return -1;

  tdResetTSchemaBuilder(pBuilder, version);
  return 0;
}

void tdDestroyTSchemaBuilder(STSchemaBuilder *pBuilder) {
  if (pBuilder) {
    tfree(pBuilder->columns);
  }
}

void tdResetTSchemaBuilder(STSchemaBuilder *pBuilder, int32_t version) {
  pBuilder->nCols = 0;
  pBuilder->tlen = 0;
  pBuilder->flen = 0;
  pBuilder->vlen = 0;
  pBuilder->version = version;
}

int tdAddColToSchema(STSchemaBuilder *pBuilder, int8_t type, int16_t colId, int16_t bytes) {
  if (!isValidDataType(type)) return -1;

  if (pBuilder->nCols >= pBuilder->tCols) {
    pBuilder->tCols *= 2;
    pBuilder->columns = (STColumn *)realloc(pBuilder->columns, sizeof(STColumn) * pBuilder->tCols);
    if (pBuilder->columns == NULL) return -1;
  }

  STColumn *pCol = &(pBuilder->columns[pBuilder->nCols]);
  colSetType(pCol, type);
  colSetColId(pCol, colId);
  if (pBuilder->nCols == 0) {
    colSetOffset(pCol, 0);
  } else {
    STColumn *pTCol = &(pBuilder->columns[pBuilder->nCols-1]);
    colSetOffset(pCol, pTCol->offset + TYPE_BYTES[pTCol->type]);
  }

  if (IS_VAR_DATA_TYPE(type)) {
    colSetBytes(pCol, bytes);
    pBuilder->tlen += (TYPE_BYTES[type] + bytes);
    pBuilder->vlen += bytes - sizeof(VarDataLenT);
  } else {
    colSetBytes(pCol, TYPE_BYTES[type]);
    pBuilder->tlen += TYPE_BYTES[type];
    pBuilder->vlen += TYPE_BYTES[type];
  }

  pBuilder->nCols++;
  pBuilder->flen += TYPE_BYTES[type];

  ASSERT(pCol->offset < pBuilder->flen);

  return 0;
}

STSchema *tdGetSchemaFromBuilder(STSchemaBuilder *pBuilder) {
  if (pBuilder->nCols <= 0) return NULL;

  int tlen = sizeof(STSchema) + sizeof(STColumn) * pBuilder->nCols;

  STSchema *pSchema = (STSchema *)malloc(tlen);
  if (pSchema == NULL) return NULL;

  schemaVersion(pSchema) = pBuilder->version;
  schemaNCols(pSchema) = pBuilder->nCols;
  schemaTLen(pSchema) = pBuilder->tlen;
  schemaFLen(pSchema) = pBuilder->flen;
  schemaVLen(pSchema) = pBuilder->vlen;

  memcpy(schemaColAt(pSchema, 0), pBuilder->columns, sizeof(STColumn) * pBuilder->nCols);

  return pSchema;
}

/**
 * Initialize a data row
 */
void tdInitDataRow(SDataRow row, STSchema *pSchema) {
  dataRowSetLen(row, TD_DATA_ROW_HEAD_SIZE + schemaFLen(pSchema));
  dataRowSetVersion(row, schemaVersion(pSchema));
}

SDataRow tdNewDataRowFromSchema(STSchema *pSchema) {
  int32_t size = dataRowMaxBytesFromSchema(pSchema);

  SDataRow row = malloc(size);
  if (row == NULL) return NULL;

  tdInitDataRow(row, pSchema);
  return row;
}

/**
 * Free the SDataRow object
 */
void tdFreeDataRow(SDataRow row) {
  if (row) free(row);
}

SDataRow tdDataRowDup(SDataRow row) {
  SDataRow trow = malloc(dataRowLen(row));
  if (trow == NULL) return NULL;

  dataRowCpy(trow, row);
  return trow;
}

SMemRow tdMemRowDup(SMemRow row) {
  SMemRow trow = malloc(memRowTLen(row));
  if (trow == NULL) return NULL;

  memRowCpy(trow, row);
  return trow;
}

void dataColInit(SDataCol *pDataCol, STColumn *pCol, int maxPoints) {
  pDataCol->type = colType(pCol);
  pDataCol->colId = colColId(pCol);
  pDataCol->bytes = colBytes(pCol);
  pDataCol->offset = colOffset(pCol) + TD_DATA_ROW_HEAD_SIZE;

  pDataCol->len = 0;
}
// value from timestamp should be TKEY here instead of TSKEY
int dataColAppendVal(SDataCol *pCol, const void *value, int numOfRows, int maxPoints) {
  ASSERT(pCol != NULL && value != NULL);

  if (isAllRowsNull(pCol)) {
    if (isNull(value, pCol->type)) {
      // all null value yet, just return
      return 0;
    }

    if(tdAllocMemForCol(pCol, maxPoints) < 0) return -1;
    if (numOfRows > 0) {
      // Find the first not null value, fill all previouse values as NULL
      dataColSetNEleNull(pCol, numOfRows);
    }
  }

  if (IS_VAR_DATA_TYPE(pCol->type)) {
    // set offset
    pCol->dataOff[numOfRows] = pCol->len;
    // Copy data
    memcpy(POINTER_SHIFT(pCol->pData, pCol->len), value, varDataTLen(value));
    // Update the length
    pCol->len += varDataTLen(value);
  } else {
    ASSERT(pCol->len == TYPE_BYTES[pCol->type] * numOfRows);
    memcpy(POINTER_SHIFT(pCol->pData, pCol->len), value, pCol->bytes);
    pCol->len += pCol->bytes;
  }
  return 0;
}

static FORCE_INLINE const void *tdGetColDataOfRowUnsafe(SDataCol *pCol, int row) {
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    return POINTER_SHIFT(pCol->pData, pCol->dataOff[row]);
  } else {
    return POINTER_SHIFT(pCol->pData, TYPE_BYTES[pCol->type] * row);
  }
}

bool isNEleNull(SDataCol *pCol, int nEle) {
  if(isAllRowsNull(pCol)) return true;
  for (int i = 0; i < nEle; i++) {
    if (!isNull(tdGetColDataOfRowUnsafe(pCol, i), pCol->type)) return false;
  }
  return true;
}

static FORCE_INLINE void dataColSetNullAt(SDataCol *pCol, int index) {
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    pCol->dataOff[index] = pCol->len;
    char *ptr = POINTER_SHIFT(pCol->pData, pCol->len);
    setVardataNull(ptr, pCol->type);
    pCol->len += varDataTLen(ptr);
  } else {
    setNull(POINTER_SHIFT(pCol->pData, TYPE_BYTES[pCol->type] * index), pCol->type, pCol->bytes);
    pCol->len += TYPE_BYTES[pCol->type];
  }
}

static void dataColSetNEleNull(SDataCol *pCol, int nEle) {
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    pCol->len = 0;
    for (int i = 0; i < nEle; i++) {
      dataColSetNullAt(pCol, i);
    }
  } else {
    setNullN(pCol->pData, pCol->type, pCol->bytes, nEle);
    pCol->len = TYPE_BYTES[pCol->type] * nEle;
  }
}

void dataColSetOffset(SDataCol *pCol, int nEle) {
  ASSERT(((pCol->type == TSDB_DATA_TYPE_BINARY) || (pCol->type == TSDB_DATA_TYPE_NCHAR)));

  void *tptr = pCol->pData;
  // char *tptr = (char *)(pCol->pData);

  VarDataOffsetT offset = 0;
  for (int i = 0; i < nEle; i++) {
    pCol->dataOff[i] = offset;
    offset += varDataTLen(tptr);
    tptr = POINTER_SHIFT(tptr, varDataTLen(tptr));
  }
}

SDataCols *tdNewDataCols(int maxCols, int maxRows) {
  SDataCols *pCols = (SDataCols *)calloc(1, sizeof(SDataCols));
  if (pCols == NULL) {
    uDebug("malloc failure, size:%" PRId64 " failed, reason:%s", (int64_t)sizeof(SDataCols), strerror(errno));
    return NULL;
  }

  pCols->maxPoints = maxRows;
  pCols->maxCols = maxCols;
  pCols->numOfRows = 0;
  pCols->numOfCols = 0;

  if (maxCols > 0) {
    pCols->cols = (SDataCol *)calloc(maxCols, sizeof(SDataCol));
    if (pCols->cols == NULL) {
      uDebug("malloc failure, size:%" PRId64 " failed, reason:%s", (int64_t)sizeof(SDataCol) * maxCols,
             strerror(errno));
      tdFreeDataCols(pCols);
      return NULL;
    }
    int i;
    for(i = 0; i < maxCols; i++) {
      pCols->cols[i].spaceSize = 0;
      pCols->cols[i].len = 0;
      pCols->cols[i].pData = NULL;
      pCols->cols[i].dataOff = NULL;
    }
  }

  return pCols;
}

int tdInitDataCols(SDataCols *pCols, STSchema *pSchema) {
  int i;
  int oldMaxCols = pCols->maxCols;
  if (schemaNCols(pSchema) > oldMaxCols) {
    pCols->maxCols = schemaNCols(pSchema);
    void* ptr = (SDataCol *)realloc(pCols->cols, sizeof(SDataCol) * pCols->maxCols);
    if (ptr == NULL) return -1;
    pCols->cols = ptr;
    for(i = oldMaxCols; i < pCols->maxCols; i++) {
      pCols->cols[i].pData = NULL;
      pCols->cols[i].dataOff = NULL;
      pCols->cols[i].spaceSize = 0;
    }
  }

  tdResetDataCols(pCols);
  pCols->numOfCols = schemaNCols(pSchema);

  for (i = 0; i < schemaNCols(pSchema); i++) {
    dataColInit(pCols->cols + i, schemaColAt(pSchema, i), pCols->maxPoints);
  }
  
  return 0;
}

SDataCols *tdFreeDataCols(SDataCols *pCols) {
  int i;
  if (pCols) {
    if(pCols->cols) {
      int maxCols = pCols->maxCols;
      for(i = 0; i < maxCols; i++) {
        SDataCol *pCol = &pCols->cols[i];
        tfree(pCol->pData);
      }
      free(pCols->cols);
      pCols->cols = NULL;
    }
    free(pCols);
  }
  return NULL;
}

SDataCols *tdDupDataCols(SDataCols *pDataCols, bool keepData) {
  SDataCols *pRet = tdNewDataCols(pDataCols->maxCols, pDataCols->maxPoints);
  if (pRet == NULL) return NULL;

  pRet->numOfCols = pDataCols->numOfCols;
  pRet->sversion = pDataCols->sversion;
  if (keepData) pRet->numOfRows = pDataCols->numOfRows;

  for (int i = 0; i < pDataCols->numOfCols; i++) {
    pRet->cols[i].type = pDataCols->cols[i].type;
    pRet->cols[i].colId = pDataCols->cols[i].colId;
    pRet->cols[i].bytes = pDataCols->cols[i].bytes;
    pRet->cols[i].offset = pDataCols->cols[i].offset;

    if (keepData) {
      if (pDataCols->cols[i].len > 0) {
        if(tdAllocMemForCol(&pRet->cols[i], pRet->maxPoints) < 0) {
          tdFreeDataCols(pRet);
          return NULL;
        }
        pRet->cols[i].len = pDataCols->cols[i].len;
        memcpy(pRet->cols[i].pData, pDataCols->cols[i].pData, pDataCols->cols[i].len);
        if (IS_VAR_DATA_TYPE(pRet->cols[i].type)) {
          int dataOffSize = sizeof(VarDataOffsetT) * pDataCols->maxPoints;
          memcpy(pRet->cols[i].dataOff, pDataCols->cols[i].dataOff, dataOffSize);
        }
      }
    }
  }

  return pRet;
}

void tdResetDataCols(SDataCols *pCols) {
  if (pCols != NULL) {
    pCols->numOfRows = 0;
    for (int i = 0; i < pCols->maxCols; i++) {
      dataColReset(pCols->cols + i);
    }
  }
}

static void tdAppendDataRowToDataCol(SDataRow row, STSchema *pSchema, SDataCols *pCols, bool forceSetNull) {
  ASSERT(pCols->numOfRows == 0 || dataColsKeyLast(pCols) < dataRowKey(row));

  int rcol = 0;
  int dcol = 0;

  while (dcol < pCols->numOfCols) {
    SDataCol *pDataCol = &(pCols->cols[dcol]);
    if (rcol >= schemaNCols(pSchema)) {
      dataColAppendVal(pDataCol, getNullValue(pDataCol->type), pCols->numOfRows, pCols->maxPoints);
      dcol++;
      continue;
    }

    STColumn *pRowCol = schemaColAt(pSchema, rcol);
    if (pRowCol->colId == pDataCol->colId) {
      void *value = tdGetRowDataOfCol(row, pRowCol->type, pRowCol->offset + TD_DATA_ROW_HEAD_SIZE);
      dataColAppendVal(pDataCol, value, pCols->numOfRows, pCols->maxPoints);
      dcol++;
      rcol++;
    } else if (pRowCol->colId < pDataCol->colId) {
      rcol++;
    } else {
      if(forceSetNull) {
        dataColAppendVal(pDataCol, getNullValue(pDataCol->type), pCols->numOfRows, pCols->maxPoints);
      }
      dcol++;
    }
  }
  pCols->numOfRows++;
}

static void tdAppendKvRowToDataCol(SKVRow row, STSchema *pSchema, SDataCols *pCols, bool forceSetNull) {
  ASSERT(pCols->numOfRows == 0 || dataColsKeyLast(pCols) < kvRowKey(row));

  int rcol = 0;
  int dcol = 0;

  int nRowCols = kvRowNCols(row);

  while (dcol < pCols->numOfCols) {
    SDataCol *pDataCol = &(pCols->cols[dcol]);
    if (rcol >= nRowCols || rcol >= schemaNCols(pSchema)) {
      dataColAppendVal(pDataCol, getNullValue(pDataCol->type), pCols->numOfRows, pCols->maxPoints);
      ++dcol;
      continue;
    }

    SColIdx *colIdx = kvRowColIdxAt(row, rcol);

    if (colIdx->colId == pDataCol->colId) {
      void *value = tdGetKvRowDataOfCol(row, colIdx->offset);
      dataColAppendVal(pDataCol, value, pCols->numOfRows, pCols->maxPoints);
      ++dcol;
      ++rcol;
    } else if (colIdx->colId < pDataCol->colId) {
      ++rcol;
    } else {
      if (forceSetNull) {
        dataColAppendVal(pDataCol, getNullValue(pDataCol->type), pCols->numOfRows, pCols->maxPoints);
      }
      ++dcol;
    }
  }
  pCols->numOfRows++;
}

void tdAppendMemRowToDataCol(SMemRow row, STSchema *pSchema, SDataCols *pCols, bool forceSetNull) {
  if (isDataRow(row)) {
    tdAppendDataRowToDataCol(memRowDataBody(row), pSchema, pCols, forceSetNull);
  } else if (isKvRow(row)) {
    tdAppendKvRowToDataCol(memRowKvBody(row), pSchema, pCols, forceSetNull);
  } else {
    ASSERT(0);
  }
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
        if (source->cols[j].len > 0) {
          dataColAppendVal(target->cols + j, tdGetColDataOfRow(source->cols + j, i + (*pOffset)), target->numOfRows,
                           target->maxPoints);
        }
      }
      target->numOfRows++;
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
    TKEY  tkey2 = (*iter2 >= limit2) ? TKEY_NULL : dataColsTKeyAt(src2, *iter2);

    ASSERT(tkey1 == TKEY_NULL || (!TKEY_IS_DELETED(tkey1)));

    if (key1 < key2) {
      for (int i = 0; i < src1->numOfCols; i++) {
        ASSERT(target->cols[i].type == src1->cols[i].type);
        if (src1->cols[i].len > 0) {
          dataColAppendVal(&(target->cols[i]), tdGetColDataOfRow(src1->cols + i, *iter1), target->numOfRows,
                           target->maxPoints);
        }
      }

      target->numOfRows++;
      (*iter1)++;
    } else if (key1 >= key2) {
      if ((key1 > key2) || (key1 == key2 && !TKEY_IS_DELETED(tkey2))) {
        for (int i = 0; i < src2->numOfCols; i++) {
          ASSERT(target->cols[i].type == src2->cols[i].type);
          if (src2->cols[i].len > 0 && !isNull(src2->cols[i].pData, src2->cols[i].type)) {
            dataColAppendVal(&(target->cols[i]), tdGetColDataOfRow(src2->cols + i, *iter2), target->numOfRows,
                             target->maxPoints);
          } else if(!forceSetNull && key1 == key2 && src1->cols[i].len > 0) {
            dataColAppendVal(&(target->cols[i]), tdGetColDataOfRow(src1->cols + i, *iter1), target->numOfRows,
                             target->maxPoints);
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

SKVRow tdKVRowDup(SKVRow row) {
  SKVRow trow = malloc(kvRowLen(row));
  if (trow == NULL) return NULL;

  kvRowCpy(trow, row);
  return trow;
}

static int compareColIdx(const void* a, const void* b) {
  const SColIdx* x = (const SColIdx*)a;
  const SColIdx* y = (const SColIdx*)b;
  if (x->colId > y->colId) {
    return 1;
  }
  if (x->colId < y->colId) {
    return -1;
  }
  return 0;
}

void tdSortKVRowByColIdx(SKVRow row) {
  qsort(kvRowColIdx(row), kvRowNCols(row), sizeof(SColIdx), compareColIdx);
}

int tdSetKVRowDataOfCol(SKVRow *orow, int16_t colId, int8_t type, void *value) {
  SColIdx *pColIdx = NULL;
  SKVRow   row = *orow;
  SKVRow   nrow = NULL;
  void *   ptr = taosbsearch(&colId, kvRowColIdx(row), kvRowNCols(row), sizeof(SColIdx), comparTagId, TD_GE);

  if (ptr == NULL || ((SColIdx *)ptr)->colId > colId) {  // need to add a column value to the row
    int diff = IS_VAR_DATA_TYPE(type) ? varDataTLen(value) : TYPE_BYTES[type];
    int nRowLen = kvRowLen(row) + sizeof(SColIdx) + diff;
    int oRowCols = kvRowNCols(row);

    ASSERT(diff > 0);
    nrow = malloc(nRowLen);
    if (nrow == NULL) return -1;

    kvRowSetLen(nrow, nRowLen);
    kvRowSetNCols(nrow, oRowCols + 1);

    memcpy(kvRowColIdx(nrow), kvRowColIdx(row), sizeof(SColIdx) * oRowCols);
    memcpy(kvRowValues(nrow), kvRowValues(row), kvRowValLen(row));

    pColIdx = kvRowColIdxAt(nrow, oRowCols);
    pColIdx->colId = colId;
    pColIdx->offset = kvRowValLen(row);

    memcpy(kvRowColVal(nrow, pColIdx), value, diff);  // copy new value

    tdSortKVRowByColIdx(nrow);

    *orow = nrow;
    free(row);
  } else {
    ASSERT(((SColIdx *)ptr)->colId == colId);
    if (IS_VAR_DATA_TYPE(type)) {
      void *pOldVal = kvRowColVal(row, (SColIdx *)ptr);

      if (varDataTLen(value) == varDataTLen(pOldVal)) { // just update the column value in place
        memcpy(pOldVal, value, varDataTLen(value));
      } else {  // need to reallocate the memory
        int16_t nlen = kvRowLen(row) + (varDataTLen(value) - varDataTLen(pOldVal));
        ASSERT(nlen > 0);
        nrow = malloc(nlen);
        if (nrow == NULL) return -1;

        kvRowSetLen(nrow, nlen);
        kvRowSetNCols(nrow, kvRowNCols(row));

        int zsize = sizeof(SColIdx) * kvRowNCols(row) + ((SColIdx *)ptr)->offset;
        memcpy(kvRowColIdx(nrow), kvRowColIdx(row), zsize);
        memcpy(kvRowColVal(nrow, ((SColIdx *)ptr)), value, varDataTLen(value));
        // Copy left value part
        int lsize = kvRowLen(row) - TD_KV_ROW_HEAD_SIZE - zsize - varDataTLen(pOldVal);
        if (lsize > 0) {
          memcpy(POINTER_SHIFT(nrow, TD_KV_ROW_HEAD_SIZE + zsize + varDataTLen(value)),
                 POINTER_SHIFT(row, TD_KV_ROW_HEAD_SIZE + zsize + varDataTLen(pOldVal)), lsize);
        }

        for (int i = 0; i < kvRowNCols(nrow); i++) {
          pColIdx = kvRowColIdxAt(nrow, i);

          if (pColIdx->offset > ((SColIdx *)ptr)->offset) {
            pColIdx->offset = pColIdx->offset - varDataTLen(pOldVal) + varDataTLen(value);
          }
        }

        *orow = nrow;
        free(row);
      }
    } else {
      memcpy(kvRowColVal(row, (SColIdx *)ptr), value, TYPE_BYTES[type]);
    }
  }

  return 0;
}

int tdEncodeKVRow(void **buf, SKVRow row) {
  // May change the encode purpose
  if (buf != NULL) {
    kvRowCpy(*buf, row);
    *buf = POINTER_SHIFT(*buf, kvRowLen(row));
  }

  return kvRowLen(row);
}

void *tdDecodeKVRow(void *buf, SKVRow *row) {
  *row = tdKVRowDup(buf);
  if (*row == NULL) return NULL;
  return POINTER_SHIFT(buf, kvRowLen(*row));
}

int tdInitKVRowBuilder(SKVRowBuilder *pBuilder) {
  pBuilder->tCols = 128;
  pBuilder->nCols = 0;
  pBuilder->pColIdx = (SColIdx *)malloc(sizeof(SColIdx) * pBuilder->tCols);
  if (pBuilder->pColIdx == NULL) return -1;
  pBuilder->alloc = 1024;
  pBuilder->size = 0;
  pBuilder->buf = malloc(pBuilder->alloc);
  if (pBuilder->buf == NULL) {
    free(pBuilder->pColIdx);
    return -1;
  }
  return 0;
}

void tdDestroyKVRowBuilder(SKVRowBuilder *pBuilder) {
  tfree(pBuilder->pColIdx);
  tfree(pBuilder->buf);
}

void tdResetKVRowBuilder(SKVRowBuilder *pBuilder) {
  pBuilder->nCols = 0;
  pBuilder->size = 0;
}

SKVRow tdGetKVRowFromBuilder(SKVRowBuilder *pBuilder) {
  int tlen = sizeof(SColIdx) * pBuilder->nCols + pBuilder->size;
  if (tlen == 0) return NULL;

  tlen += TD_KV_ROW_HEAD_SIZE;

  SKVRow row = malloc(tlen);
  if (row == NULL) return NULL;

  kvRowSetNCols(row, pBuilder->nCols);
  kvRowSetLen(row, tlen);

  memcpy(kvRowColIdx(row), pBuilder->pColIdx, sizeof(SColIdx) * pBuilder->nCols);
  memcpy(kvRowValues(row), pBuilder->buf, pBuilder->size);

  return row;
}

SMemRow mergeTwoMemRows(void *buffer, SMemRow row1, SMemRow row2, STSchema *pSchema1, STSchema *pSchema2) {
#if 0
  ASSERT(memRowKey(row1) == memRowKey(row2));
  ASSERT(schemaVersion(pSchema1) == memRowVersion(row1));
  ASSERT(schemaVersion(pSchema2) == memRowVersion(row2));
  ASSERT(schemaVersion(pSchema1) >= schemaVersion(pSchema2));
#endif

  SArray *stashRow = taosArrayInit(pSchema1->numOfCols, sizeof(SColInfo));
  if (stashRow == NULL) {
    return NULL;
  }

  SMemRow  pRow = buffer;
  SDataRow dataRow = memRowDataBody(pRow);
  memRowSetType(pRow, SMEM_ROW_DATA);
  dataRowSetVersion(dataRow, schemaVersion(pSchema1));  // use latest schema version
  dataRowSetLen(dataRow, (TDRowLenT)(TD_DATA_ROW_HEAD_SIZE + pSchema1->flen));

  TDRowTLenT dataLen = 0, kvLen = TD_MEM_ROW_KV_HEAD_SIZE;

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

  dataLen = memRowTLen(pRow);

  if (kvLen < dataLen) {
    // scan stashRow and generate SKVRow
    memset(buffer, 0, sizeof(dataLen));
    SMemRow tRow = buffer;
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
    ASSERT(kvLen == memRowTLen(tRow));
  }
  taosArrayDestroy(stashRow);
  return buffer;
}
