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
#include "talgo.h"
#include "wchar.h"

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
void *tdEncodeSchema(void *dst, STSchema *pSchema) {

  T_APPEND_MEMBER(dst, pSchema, STSchema, version);
  T_APPEND_MEMBER(dst, pSchema, STSchema, numOfCols);
  for (int i = 0; i < schemaNCols(pSchema); i++) {
    STColumn *pCol = schemaColAt(pSchema, i);
    T_APPEND_MEMBER(dst, pCol, STColumn, type);
    T_APPEND_MEMBER(dst, pCol, STColumn, colId);
    T_APPEND_MEMBER(dst, pCol, STColumn, bytes);
  }

  return dst;
}

/**
 * Decode a schema from a binary.
 */
STSchema *tdDecodeSchema(void **psrc) {
  int totalCols = 0;
  int version = 0;
  STSchemaBuilder schemaBuilder = {0};

  T_READ_MEMBER(*psrc, int, version);
  T_READ_MEMBER(*psrc, int, totalCols);

  if (tdInitTSchemaBuilder(&schemaBuilder, version) < 0) return NULL;

  for (int i = 0; i < totalCols; i++) {
    int8_t  type = 0;
    int16_t colId = 0;
    int32_t bytes = 0;
    T_READ_MEMBER(*psrc, int8_t, type);
    T_READ_MEMBER(*psrc, int16_t, colId);
    T_READ_MEMBER(*psrc, int32_t, bytes);

    if (tdAddColToSchema(&schemaBuilder, type, colId, bytes) < 0) {
      tdDestroyTSchemaBuilder(&schemaBuilder);
      return NULL;
    }
  }

  STSchema *pSchema = tdGetSchemaFromBuilder(&schemaBuilder);
  tdDestroyTSchemaBuilder(&schemaBuilder);
  return pSchema;
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
  pBuilder->version = version;
}

int tdAddColToSchema(STSchemaBuilder *pBuilder, int8_t type, int16_t colId, int32_t bytes) {
  if (!isValidDataType(type, 0)) return -1;

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
    pBuilder->tlen += (TYPE_BYTES[type] + sizeof(VarDataLenT) + bytes);
  } else {
    colSetBytes(pCol, TYPE_BYTES[type]);
    pBuilder->tlen += TYPE_BYTES[type];
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

void dataColInit(SDataCol *pDataCol, STColumn *pCol, void **pBuf, int maxPoints) {
  pDataCol->type = colType(pCol);
  pDataCol->colId = colColId(pCol);
  pDataCol->bytes = colBytes(pCol);
  pDataCol->offset = colOffset(pCol) + TD_DATA_ROW_HEAD_SIZE;

  pDataCol->len = 0;
  if (pDataCol->type == TSDB_DATA_TYPE_BINARY || pDataCol->type == TSDB_DATA_TYPE_NCHAR) {
    pDataCol->spaceSize = (sizeof(VarDataLenT) + pDataCol->bytes) * maxPoints;
    pDataCol->dataOff = (VarDataOffsetT *)(*pBuf);
    pDataCol->pData = POINTER_SHIFT(*pBuf, TYPE_BYTES[pDataCol->type] * maxPoints);
    *pBuf = POINTER_SHIFT(*pBuf, pDataCol->spaceSize + TYPE_BYTES[pDataCol->type] * maxPoints);
  } else {
    pDataCol->spaceSize = pDataCol->bytes * maxPoints;
    pDataCol->dataOff = NULL;
    pDataCol->pData = *pBuf;
    *pBuf = POINTER_SHIFT(*pBuf, pDataCol->spaceSize);
  }
}

void dataColAppendVal(SDataCol *pCol, void *value, int numOfRows, int maxPoints) {
  ASSERT(pCol != NULL && value != NULL);

  switch (pCol->type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      // set offset
      pCol->dataOff[numOfRows] = pCol->len;
      // Copy data
      memcpy(POINTER_SHIFT(pCol->pData, pCol->len), value, varDataTLen(value));
      // Update the length
      pCol->len += varDataTLen(value);
      break;
    default:
      ASSERT(pCol->len == TYPE_BYTES[pCol->type] * numOfRows);
      memcpy(POINTER_SHIFT(pCol->pData, pCol->len), value, pCol->bytes);
      pCol->len += pCol->bytes;
      break;
  }
}

void dataColPopPoints(SDataCol *pCol, int pointsToPop, int numOfRows) {
  int pointsLeft = numOfRows - pointsToPop;

  ASSERT(pointsLeft > 0);

  if (pCol->type == TSDB_DATA_TYPE_BINARY || pCol->type == TSDB_DATA_TYPE_NCHAR) {
    ASSERT(pCol->len > 0);
    VarDataOffsetT toffset = pCol->dataOff[pointsToPop];
    pCol->len = pCol->len - toffset;
    ASSERT(pCol->len > 0);
    memmove(pCol->pData, POINTER_SHIFT(pCol->pData, toffset), pCol->len);
    dataColSetOffset(pCol, pointsLeft);
  } else {
    ASSERT(pCol->len == TYPE_BYTES[pCol->type] * numOfRows);
    pCol->len = TYPE_BYTES[pCol->type] * pointsLeft;
    memmove(pCol->pData, POINTER_SHIFT(pCol->pData, TYPE_BYTES[pCol->type] * pointsToPop), pCol->len);
  }
}

bool isNEleNull(SDataCol *pCol, int nEle) {
  switch (pCol->type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      for (int i = 0; i < nEle; i++) {
        if (!isNull(varDataVal(tdGetColDataOfRow(pCol, i)), pCol->type)) return false;
      }
      return true;
    default:
      for (int i = 0; i < nEle; i++) {
        if (!isNull(tdGetColDataOfRow(pCol, i), pCol->type)) return false;
      }
      return true;
  }
}

void dataColSetNullAt(SDataCol *pCol, int index) {
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    pCol->dataOff[index] = pCol->len;
    char *ptr = POINTER_SHIFT(pCol->pData, pCol->len);
    varDataLen(ptr) = (pCol->type == TSDB_DATA_TYPE_BINARY) ? sizeof(char) : TSDB_NCHAR_SIZE;
    setNull(varDataVal(ptr), pCol->type, pCol->bytes);
    pCol->len += varDataTLen(ptr);
  } else {
    setNull(POINTER_SHIFT(pCol->pData, TYPE_BYTES[pCol->type] * index), pCol->type, pCol->bytes);
    pCol->len += TYPE_BYTES[pCol->type];
  }
}

void dataColSetNEleNull(SDataCol *pCol, int nEle, int maxPoints) {

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

SDataCols *tdNewDataCols(int maxRowSize, int maxCols, int maxRows) {
  SDataCols *pCols = (SDataCols *)calloc(1, sizeof(SDataCols) + sizeof(SDataCol) * maxCols);
  if (pCols == NULL) return NULL;

  pCols->maxRowSize = maxRowSize;
  pCols->maxCols = maxCols;
  pCols->maxPoints = maxRows;
  pCols->bufSize = maxRowSize * maxRows;

  pCols->buf = malloc(pCols->bufSize);
  if (pCols->buf == NULL) {
    free(pCols);
    return NULL;
  }

  return pCols;
}

void tdInitDataCols(SDataCols *pCols, STSchema *pSchema) {
  // assert(schemaNCols(pSchema) <= pCols->numOfCols);
  tdResetDataCols(pCols);
  pCols->numOfCols = schemaNCols(pSchema);

  void *ptr = pCols->buf;
  for (int i = 0; i < schemaNCols(pSchema); i++) {
    dataColInit(pCols->cols + i, schemaColAt(pSchema, i), &ptr, pCols->maxPoints);
    ASSERT((char *)ptr - (char *)(pCols->buf) <= pCols->bufSize);
  }
}

void tdFreeDataCols(SDataCols *pCols) {
  if (pCols) {
    tfree(pCols->buf);
    free(pCols);
  }
}

SDataCols *tdDupDataCols(SDataCols *pDataCols, bool keepData) {
  SDataCols *pRet = tdNewDataCols(pDataCols->maxRowSize, pDataCols->maxCols, pDataCols->maxPoints);
  if (pRet == NULL) return NULL;

  pRet->numOfCols = pDataCols->numOfCols;
  pRet->sversion = pDataCols->sversion;
  if (keepData) pRet->numOfRows = pDataCols->numOfRows;

  for (int i = 0; i < pDataCols->numOfCols; i++) {
    pRet->cols[i].type = pDataCols->cols[i].type;
    pRet->cols[i].colId = pDataCols->cols[i].colId;
    pRet->cols[i].bytes = pDataCols->cols[i].bytes;
    pRet->cols[i].offset = pDataCols->cols[i].offset;

    pRet->cols[i].spaceSize = pDataCols->cols[i].spaceSize;
    pRet->cols[i].pData = (void *)((char *)pRet->buf + ((char *)(pDataCols->cols[i].pData) - (char *)(pDataCols->buf)));

    if (pRet->cols[i].type == TSDB_DATA_TYPE_BINARY || pRet->cols[i].type == TSDB_DATA_TYPE_NCHAR) {
      ASSERT(pDataCols->cols[i].dataOff != NULL);
      pRet->cols[i].dataOff =
          (int32_t *)((char *)pRet->buf + ((char *)(pDataCols->cols[i].dataOff) - (char *)(pDataCols->buf)));
    }

    if (keepData) {
      pRet->cols[i].len = pDataCols->cols[i].len;
      memcpy(pRet->cols[i].pData, pDataCols->cols[i].pData, pDataCols->cols[i].len);
      if (pRet->cols[i].type == TSDB_DATA_TYPE_BINARY || pRet->cols[i].type == TSDB_DATA_TYPE_NCHAR) {
        memcpy(pRet->cols[i].dataOff, pDataCols->cols[i].dataOff, sizeof(VarDataOffsetT) * pDataCols->maxPoints);
      }
    }
  }

  return pRet;
}

void tdResetDataCols(SDataCols *pCols) {
  pCols->numOfRows = 0;
  for (int i = 0; i < pCols->maxCols; i++) {
    dataColReset(pCols->cols + i);
  }
}

void tdAppendDataRowToDataCol(SDataRow row, STSchema *pSchema, SDataCols *pCols) {
  ASSERT(dataColsKeyLast(pCols) < dataRowKey(row));

  int rcol = 0;
  int dcol = 0;

  while (dcol < pCols->numOfCols) {
    SDataCol *pDataCol = &(pCols->cols[dcol]);
    if (rcol >= schemaNCols(pSchema)) {
      dataColSetNullAt(pDataCol, pCols->numOfRows);
      dcol++;
      continue;
    }

    STColumn *pRowCol = schemaColAt(pSchema, rcol);
    if (pRowCol->colId == pDataCol->colId) {
      void *value = tdGetRowDataOfCol(row, pRowCol->type, pRowCol->offset+TD_DATA_ROW_HEAD_SIZE);
      dataColAppendVal(pDataCol, value, pCols->numOfRows, pCols->maxPoints);
      dcol++;
      rcol++;
    } else if (pRowCol->colId < pDataCol->colId) {
      rcol++;
    } else {
      dataColSetNullAt(pDataCol, pCols->numOfRows);
      dcol++;
    }
  }
  pCols->numOfRows++;
}

// Pop pointsToPop points from the SDataCols
void tdPopDataColsPoints(SDataCols *pCols, int pointsToPop) {
  int pointsLeft = pCols->numOfRows - pointsToPop;
  if (pointsLeft <= 0) {
    tdResetDataCols(pCols);
    return;
  }

  for (int iCol = 0; iCol < pCols->numOfCols; iCol++) {
    SDataCol *pCol = pCols->cols + iCol;
    dataColPopPoints(pCol, pointsToPop, pCols->numOfRows);
  }
  pCols->numOfRows = pointsLeft;
}

int tdMergeDataCols(SDataCols *target, SDataCols *source, int rowsToMerge) {
  ASSERT(rowsToMerge > 0 && rowsToMerge <= source->numOfRows);
  ASSERT(target->numOfRows + rowsToMerge <= target->maxPoints);
  ASSERT(target->numOfCols == source->numOfCols);

  SDataCols *pTarget = NULL;

  if (dataColsKeyLast(target) < dataColsKeyFirst(source)) {  // No overlap
    for (int i = 0; i < rowsToMerge; i++) {
      for (int j = 0; j < source->numOfCols; j++) {
        dataColAppendVal(target->cols + j, tdGetColDataOfRow(source->cols + j, i), target->numOfRows,
                         target->maxPoints);
      }
      target->numOfRows++;
    }
  } else {
    pTarget = tdDupDataCols(target, true);
    if (pTarget == NULL) goto _err;

    int iter1 = 0;
    int iter2 = 0;
    tdMergeTwoDataCols(target, pTarget, &iter1, source, &iter2, pTarget->numOfRows + rowsToMerge);
  }

  tdFreeDataCols(pTarget);
  return 0;

_err:
  tdFreeDataCols(pTarget);
  return -1;
}

void tdMergeTwoDataCols(SDataCols *target, SDataCols *src1, int *iter1, SDataCols *src2, int *iter2, int tRows) {
  // TODO: add resolve duplicate key here
  tdResetDataCols(target);

  while (target->numOfRows < tRows) {
    if (*iter1 >= src1->numOfRows && *iter2 >= src2->numOfRows) break;

    TSKEY key1 = (*iter1 >= src1->numOfRows) ? INT64_MAX : ((TSKEY *)(src1->cols[0].pData))[*iter1];
    TSKEY key2 = (*iter2 >= src2->numOfRows) ? INT64_MAX : ((TSKEY *)(src2->cols[0].pData))[*iter2];

    if (key1 <= key2) {
      for (int i = 0; i < src1->numOfCols; i++) {
        ASSERT(target->cols[i].type == src1->cols[i].type);
        dataColAppendVal(&(target->cols[i]), tdGetColDataOfRow(src1->cols + i, *iter1), target->numOfRows,
                         target->maxPoints);
      }

      target->numOfRows++;
      (*iter1)++;
      if (key1 == key2) (*iter2)++;
    } else {
      for (int i = 0; i < src2->numOfCols; i++) {
        ASSERT(target->cols[i].type == src2->cols[i].type);
        dataColAppendVal(&(target->cols[i]), tdGetColDataOfRow(src2->cols + i, *iter2), target->numOfRows,
                         target->maxPoints);
      }

      target->numOfRows++;
      (*iter2)++;
    }
  }
}

SKVRow tdKVRowDup(SKVRow row) {
  SKVRow trow = malloc(kvRowLen(row));
  if (trow == NULL) return NULL;

  kvRowCpy(trow, row);
  return trow;
}

int tdSetKVRowDataOfCol(SKVRow *orow, int16_t colId, int8_t type, void *value) {
  SColIdx *pColIdx = NULL;
  SKVRow   row = *orow;
  SKVRow   nrow = NULL;
  void *   ptr = taosbsearch(&colId, kvRowColIdx(row), kvRowNCols(row), sizeof(SColIdx), comparTagId, TD_GE);

  if (ptr == NULL || ((SColIdx *)ptr)->colId < colId) { // need to add a column value to the row
    int diff = IS_VAR_DATA_TYPE(type) ? varDataTLen(value) : TYPE_BYTES[type];
    nrow = malloc(kvRowLen(row) + sizeof(SColIdx) + diff);
    if (nrow == NULL) return -1;

    kvRowSetLen(nrow, kvRowLen(row) + sizeof(SColIdx) + diff);
    kvRowSetNCols(nrow, kvRowNCols(row) + 1);

    if (ptr == NULL) {
      memcpy(kvRowColIdx(nrow), kvRowColIdx(row), sizeof(SColIdx) * kvRowNCols(row));
      memcpy(kvRowValues(nrow), kvRowValues(row), POINTER_DISTANCE(kvRowEnd(row), kvRowValues(row)));
      int colIdx = kvRowNCols(nrow) - 1;
      kvRowColIdxAt(nrow, colIdx)->colId = colId;
      kvRowColIdxAt(nrow, colIdx)->offset = POINTER_DISTANCE(kvRowEnd(row), kvRowValues(row));
      memcpy(kvRowColVal(nrow, kvRowColIdxAt(nrow, colIdx)), value, diff);
    } else {
      int16_t tlen = POINTER_DISTANCE(ptr, kvRowColIdx(row));
      if (tlen > 0) {
        memcpy(kvRowColIdx(nrow), kvRowColIdx(row), tlen);
        memcpy(kvRowValues(nrow), kvRowValues(row), ((SColIdx *)ptr)->offset);
      }

      int colIdx = tlen / sizeof(SColIdx);
      kvRowColIdxAt(nrow, colIdx)->colId = colId;
      kvRowColIdxAt(nrow, colIdx)->offset = ((SColIdx *)ptr)->offset;
      memcpy(kvRowColVal(nrow, kvRowColIdxAt(nrow, colIdx)), value, diff);

      for (int i = colIdx; i < kvRowNCols(row); i++) {
        kvRowColIdxAt(nrow, i + 1)->colId = kvRowColIdxAt(row, i)->colId;
        kvRowColIdxAt(nrow, i + 1)->offset = kvRowColIdxAt(row, i)->offset + diff;
      }
      memcpy(kvRowColVal(nrow, kvRowColIdxAt(nrow, colIdx + 1)), kvRowColVal(row, kvRowColIdxAt(row, colIdx)),
             POINTER_DISTANCE(kvRowEnd(row), kvRowColVal(row, kvRowColIdxAt(row, colIdx)))

      );
    }

    *orow = nrow;
    free(row);
  } else {
    ASSERT(((SColIdx *)ptr)->colId == colId);
    if (IS_VAR_DATA_TYPE(type)) {
      void *pOldVal = kvRowColVal(row, (SColIdx *)ptr);

      if (varDataTLen(value) == varDataTLen(pOldVal)) { // just update the column value in place
        memcpy(pOldVal, value, varDataTLen(value));
      } else { // need to reallocate the memory
        int16_t diff = varDataTLen(value) - varDataTLen(pOldVal);
        int16_t nlen = kvRowLen(row) + diff;
        ASSERT(nlen > 0);
        nrow = malloc(nlen);
        if (nrow == NULL) return -1;

        kvRowSetLen(nrow, nlen);
        kvRowSetNCols(nrow, kvRowNCols(row));

        // Copy part ahead
        nlen = POINTER_DISTANCE(ptr, kvRowColIdx(row));
        ASSERT(nlen % sizeof(SColIdx) == 0);
        if (nlen > 0) {
          ASSERT(((SColIdx *)ptr)->offset > 0);
          memcpy(kvRowColIdx(nrow), kvRowColIdx(row), nlen);
          memcpy(kvRowValues(nrow), kvRowValues(row), ((SColIdx *)ptr)->offset);
        }

        // Construct current column value
        int colIdx = nlen / sizeof(SColIdx);
        pColIdx = kvRowColIdxAt(nrow, colIdx);
        pColIdx->colId = ((SColIdx *)ptr)->colId;
        pColIdx->offset = ((SColIdx *)ptr)->offset;
        memcpy(kvRowColVal(nrow, pColIdx), value, varDataTLen(value));
 
        // Construct columns after
        if (kvRowNCols(nrow) - colIdx - 1 > 0) {
          for (int i = colIdx + 1; i < kvRowNCols(nrow); i++) {
            kvRowColIdxAt(nrow, i)->colId = kvRowColIdxAt(row, i)->colId;
            kvRowColIdxAt(nrow, i)->offset += diff;
          }
          memcpy(kvRowColVal(nrow, kvRowColIdxAt(nrow, colIdx + 1)), kvRowColVal(row, kvRowColIdxAt(row, colIdx + 1)),
                 POINTER_DISTANCE(kvRowEnd(row), kvRowColVal(row, kvRowColIdxAt(row, colIdx + 1))));
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

void *tdEncodeKVRow(void *buf, SKVRow row) {
  // May change the encode purpose
  kvRowCpy(buf, row);
  return POINTER_SHIFT(buf, kvRowLen(row));
}

void *tdDecodeKVRow(void *buf, SKVRow *row) {
  *row = tdKVRowDup(buf);
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