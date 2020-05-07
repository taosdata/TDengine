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
#include "wchar.h"

/**
 * Create a SSchema object with nCols columns
 * ASSUMPTIONS: VALID PARAMETERS
 *
 * @param nCols number of columns the schema has
 *
 * @return a STSchema object for success
 *         NULL for failure
 */
STSchema *tdNewSchema(int32_t nCols) {
  int32_t size = sizeof(STSchema) + sizeof(STColumn) * nCols;

  STSchema *pSchema = (STSchema *)calloc(1, size);
  if (pSchema == NULL) return NULL;

  pSchema->numOfCols = 0;
  pSchema->totalCols = nCols;
  pSchema->flen = 0;
  pSchema->tlen = 0;

  return pSchema;
}

/**
 * Append a column to the schema
 */
int tdSchemaAddCol(STSchema *pSchema, int8_t type, int16_t colId, int32_t bytes) {
  if (!isValidDataType(type, 0) || pSchema->numOfCols >= pSchema->totalCols) return -1;

  STColumn *pCol = schemaColAt(pSchema, schemaNCols(pSchema));
  colSetType(pCol, type);
  colSetColId(pCol, colId);
  if (schemaNCols(pSchema) == 0) {
    colSetOffset(pCol, 0);
  } else {
    STColumn *pTCol = schemaColAt(pSchema, schemaNCols(pSchema)-1);
    colSetOffset(pCol, pTCol->offset + TYPE_BYTES[pTCol->type]);
  }
  switch (type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      colSetBytes(pCol, bytes); // Set as maximum bytes
      pSchema->tlen += (TYPE_BYTES[type] + sizeof(VarDataLenT) + bytes);
      break;
    default:
      colSetBytes(pCol, TYPE_BYTES[type]);
      pSchema->tlen += TYPE_BYTES[type];
      break;
  }

  pSchema->numOfCols++;
  pSchema->flen += TYPE_BYTES[type];

  ASSERT(pCol->offset < pSchema->flen);

  return 0;
}

/**
 * Duplicate the schema and return a new object
 */
STSchema *tdDupSchema(STSchema *pSchema) {
  STSchema *tSchema = tdNewSchema(schemaNCols(pSchema));
  if (tSchema == NULL) return NULL;

  int32_t size = sizeof(STSchema) + sizeof(STColumn) * schemaNCols(pSchema);
  memcpy((void *)tSchema, (void *)pSchema, size);

  return tSchema;
}

/**
 * Return the size of encoded schema
 */
int tdGetSchemaEncodeSize(STSchema *pSchema) {
  return T_MEMBER_SIZE(STSchema, totalCols) +
         schemaNCols(pSchema) *
             (T_MEMBER_SIZE(STColumn, type) + T_MEMBER_SIZE(STColumn, colId) + T_MEMBER_SIZE(STColumn, bytes));
}

/**
 * Encode a schema to dst, and return the next pointer
 */
void *tdEncodeSchema(void *dst, STSchema *pSchema) {
  ASSERT(pSchema->numOfCols == pSchema->totalCols);

  T_APPEND_MEMBER(dst, pSchema, STSchema, totalCols);
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

  T_READ_MEMBER(*psrc, int, totalCols);

  STSchema *pSchema = tdNewSchema(totalCols);
  if (pSchema == NULL) return NULL;
  for (int i = 0; i < totalCols; i++) {
    int8_t  type = 0;
    int16_t colId = 0;
    int32_t bytes = 0;
    T_READ_MEMBER(*psrc, int8_t, type);
    T_READ_MEMBER(*psrc, int16_t, colId);
    T_READ_MEMBER(*psrc, int32_t, bytes);

    tdSchemaAddCol(pSchema, type, colId, bytes);
  }

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

/**
 * Append a column value to the data row
 * @param type: column type
 * @param bytes: column bytes
 * @param offset: offset in the data row tuple, not including the data row header
 */
int tdAppendColVal(SDataRow row, void *value, int8_t type, int32_t bytes, int32_t offset) {
  ASSERT(value != NULL);
  int32_t toffset = offset + TD_DATA_ROW_HEAD_SIZE;
  char *  ptr = POINTER_SHIFT(row, dataRowLen(row));

  switch (type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      *(VarDataOffsetT *)POINTER_SHIFT(row, toffset) = dataRowLen(row);
      memcpy(ptr, value, varDataTLen(value));
      dataRowLen(row) += varDataTLen(value);
      break;
    default:
      memcpy(POINTER_SHIFT(row, toffset), value, TYPE_BYTES[type]);
      break;
  }

  return 0;
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

void dataColAppendVal(SDataCol *pCol, void *value, int numOfPoints, int maxPoints) {
  ASSERT(pCol != NULL && value != NULL);

  switch (pCol->type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      // set offset
      pCol->dataOff[numOfPoints] = pCol->len;
      // Copy data
      memcpy(POINTER_SHIFT(pCol->pData, pCol->len), value, varDataTLen(value));
      // Update the length
      pCol->len += varDataTLen(value);
      break;
    default:
      ASSERT(pCol->len == TYPE_BYTES[pCol->type] * numOfPoints);
      memcpy(POINTER_SHIFT(pCol->pData, pCol->len), value, pCol->bytes);
      pCol->len += pCol->bytes;
      break;
  }
}

void dataColPopPoints(SDataCol *pCol, int pointsToPop, int numOfPoints) {
  int pointsLeft = numOfPoints - pointsToPop;

  ASSERT(pointsLeft > 0);

  if (pCol->type == TSDB_DATA_TYPE_BINARY || pCol->type == TSDB_DATA_TYPE_NCHAR) {
    ASSERT(pCol->len > 0);
    VarDataOffsetT toffset = pCol->dataOff[pointsToPop];
    pCol->len = pCol->len - toffset;
    ASSERT(pCol->len > 0);
    memmove(pCol->pData, POINTER_SHIFT(pCol->pData, toffset), pCol->len);
    dataColSetOffset(pCol, pointsLeft);
  } else {
    ASSERT(pCol->len == TYPE_BYTES[pCol->type] * numOfPoints);
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

  void * tptr = pCol->pData;
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
  if (keepData) pRet->numOfPoints = pDataCols->numOfPoints;

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
  pCols->numOfPoints = 0;
  for (int i = 0; i < pCols->maxCols; i++) {
    dataColReset(pCols->cols + i);
  }
}

void tdAppendDataRowToDataCol(SDataRow row, SDataCols *pCols) {
  ASSERT(dataColsKeyLast(pCols) < dataRowKey(row));

  for (int i = 0; i < pCols->numOfCols; i++) {
    SDataCol *pCol = pCols->cols + i;
    void *    value = tdGetRowDataOfCol(row, pCol->type, pCol->offset);

    dataColAppendVal(pCol, value, pCols->numOfPoints, pCols->maxPoints);
  }
  pCols->numOfPoints++;
}

// Pop pointsToPop points from the SDataCols
void tdPopDataColsPoints(SDataCols *pCols, int pointsToPop) {
  int pointsLeft = pCols->numOfPoints - pointsToPop;
  if (pointsLeft <= 0) {
    tdResetDataCols(pCols);
    return;
  }

  for (int iCol = 0; iCol < pCols->numOfCols; iCol++) {
    SDataCol *pCol = pCols->cols + iCol;
    dataColPopPoints(pCol, pointsToPop, pCols->numOfPoints);
  }
  pCols->numOfPoints = pointsLeft;
}

int tdMergeDataCols(SDataCols *target, SDataCols *source, int rowsToMerge) {
  ASSERT(rowsToMerge > 0 && rowsToMerge <= source->numOfPoints);
  ASSERT(target->numOfPoints + rowsToMerge <= target->maxPoints);
  ASSERT(target->numOfCols == source->numOfCols);

  SDataCols *pTarget = NULL;

  if (dataColsKeyLast(target) < dataColsKeyFirst(source)) {  // No overlap
    for (int i = 0; i < rowsToMerge; i++) {
      for (int j = 0; j < source->numOfCols; j++) {
        dataColAppendVal(target->cols + j, tdGetColDataOfRow(source->cols + j, i), target->numOfPoints,
                         target->maxPoints);
      }
      target->numOfPoints++;
    }
  } else {
    pTarget = tdDupDataCols(target, true);
    if (pTarget == NULL) goto _err;

    int iter1 = 0;
    int iter2 = 0;
    tdMergeTwoDataCols(target, pTarget, &iter1, source, &iter2, pTarget->numOfPoints + rowsToMerge);
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

  while (target->numOfPoints < tRows) {
    if (*iter1 >= src1->numOfPoints && *iter2 >= src2->numOfPoints) break;

    TSKEY key1 = (*iter1 >= src1->numOfPoints) ? INT64_MAX : ((TSKEY *)(src1->cols[0].pData))[*iter1];
    TSKEY key2 = (*iter2 >= src2->numOfPoints) ? INT64_MAX : ((TSKEY *)(src2->cols[0].pData))[*iter2];

    if (key1 < key2) {
      for (int i = 0; i < src1->numOfCols; i++) {
        ASSERT(target->cols[i].type == src1->cols[i].type);
        dataColAppendVal(target->cols[i].pData, tdGetColDataOfRow(src1->cols + i, *iter1), target->numOfPoints,
                         target->maxPoints);
      }

      target->numOfPoints++;
      (*iter1)++;
    } else if (key1 > key2) {
      for (int i = 0; i < src2->numOfCols; i++) {
        ASSERT(target->cols[i].type == src2->cols[i].type);
        dataColAppendVal(target->cols[i].pData, tdGetColDataOfRow(src2->cols + i, *iter2), target->numOfPoints,
                         target->maxPoints);
      }

      target->numOfPoints++;
      (*iter2)++;
    } else {
      // TODO: deal with duplicate keys
      ASSERT(false);
    }
  }
}