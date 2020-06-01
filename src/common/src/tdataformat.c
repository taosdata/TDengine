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
void tdInitDataRow(SDataRow row, STSchema *pSchema) { dataRowSetLen(row, TD_DATA_ROW_HEAD_SIZE + schemaFLen(pSchema)); }

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

void dataColSetNEleNull(SDataCol *pCol, int nEle, int maxPoints) {
  char *ptr = NULL;
  switch (pCol->type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      pCol->len = 0;
      for (int i = 0; i < nEle; i++) {
        pCol->dataOff[i] = pCol->len;
        ptr = (char *)pCol->pData + pCol->len;
        varDataLen(ptr) = (pCol->type == TSDB_DATA_TYPE_BINARY) ? sizeof(char) : TSDB_NCHAR_SIZE;
        setNull(ptr + sizeof(VarDataLenT), pCol->type, pCol->bytes);
        pCol->len += varDataTLen(ptr);
      }

      break;
    default:
      setNullN(pCol->pData, pCol->type, pCol->bytes, nEle);
      pCol->len = TYPE_BYTES[pCol->type] * nEle;
      break;
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

void tdAppendDataRowToDataCol(SDataRow row, SDataCols *pCols) {
  ASSERT(dataColsKeyLast(pCols) < dataRowKey(row));

  for (int i = 0; i < pCols->numOfCols; i++) {
    SDataCol *pCol = pCols->cols + i;
    void *    value = tdGetRowDataOfCol(row, pCol->type, pCol->offset);

    dataColAppendVal(pCol, value, pCols->numOfRows, pCols->maxPoints);
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

SKVRow tdSetKVRowDataOfCol(SKVRow row, int16_t colId, int8_t type, void *value) {
  // TODO
  return NULL;
  // SColIdx *pColIdx = NULL;
  // SKVRow rrow = row;
  // SKVRow nrow = NULL;
  // void *ptr = taosbsearch(&colId, kvDataRowColIdx(row), kvDataRowNCols(row), sizeof(SColIdx), comparTagId, TD_GE);

  // if (ptr == NULL || ((SColIdx *)ptr)->colId < colId) { // need to add a column value to the row
  //   int tlen = kvDataRowLen(row) + sizeof(SColIdx) + (IS_VAR_DATA_TYPE(type) ? varDataTLen(value) :
  //   TYPE_BYTES[type]); nrow = malloc(tlen); if (nrow == NULL) return NULL;

  //   kvDataRowSetNCols(nrow, kvDataRowNCols(row)+1);
  //   kvDataRowSetLen(nrow, tlen);

  //   if (ptr == NULL) ptr = kvDataRowValues(row);

  //   // Copy the columns before the col
  //   if (POINTER_DISTANCE(ptr, kvDataRowColIdx(row)) > 0) {
  //     memcpy(kvDataRowColIdx(nrow), kvDataRowColIdx(row), POINTER_DISTANCE(ptr, kvDataRowColIdx(row)));
  //     memcpy(kvDataRowValues(nrow), kvDataRowValues(row), ((SColIdx *)ptr)->offset); // TODO: here is not correct
  //   }

  //   // Set the new col value
  //   pColIdx = (SColIdx *)POINTER_SHIFT(nrow, POINTER_DISTANCE(ptr, row));
  //   pColIdx->colId = colId;
  //   pColIdx->offset = ((SColIdx *)ptr)->offset; // TODO: here is not correct

  //   if (IS_VAR_DATA_TYPE(type)) {
  //     memcpy(POINTER_SHIFT(kvDataRowValues(nrow), pColIdx->offset), value, varDataLen(value));
  //   } else {
  //     memcpy(POINTER_SHIFT(kvDataRowValues(nrow), pColIdx->offset), value, TYPE_BYTES[type]);
  //   }

  //   // Copy the columns after the col
  //   if (POINTER_DISTANCE(kvDataRowValues(row), ptr) > 0) {
  //     // TODO: memcpy();
  //   }
  // } else {
  //   // TODO
  //   ASSERT(((SColIdx *)ptr)->colId == colId);
  //   if (IS_VAR_DATA_TYPE(type)) {
  //     void *pOldVal = kvDataRowColVal(row, (SColIdx *)ptr);

  //     if (varDataTLen(value) == varDataTLen(pOldVal)) { // just update the column value in place
  //       memcpy(pOldVal, value, varDataTLen(value));
  //     } else { // enlarge the memory
  //       // rrow = realloc(rrow, kvDataRowLen(rrow) + varDataTLen(value) - varDataTLen(pOldVal));
  //       // if (rrow == NULL) return NULL;
  //       // memmove();
  //       // for () {
  //       //   ((SColIdx *)ptr)->offset += balabala;
  //       // }

  //       // kvDataRowSetLen();

  //     }
  //   } else {
  //     memcpy(kvDataRowColVal(row, (SColIdx *)ptr), value, TYPE_BYTES[type]);
  //   }
  // }

  // return rrow;
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