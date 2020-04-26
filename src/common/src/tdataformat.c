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
#include "tutil.h"

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
  if (pSchema->numOfCols == 0) {
    colSetOffset(pCol, 0);
  } else {
    STColumn *pTCol = pSchema->columns + pSchema->numOfCols - 1;
    colSetOffset(pCol, pTCol->offset + TYPE_BYTES[pTCol->type]);
  }
  switch (type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      colSetBytes(pCol, bytes);
      pSchema->tlen += (TYPE_BYTES[type] + sizeof(int16_t) + bytes);  // TODO: remove int16_t here
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
  int32_t toffset = offset + TD_DATA_ROW_HEAD_SIZE;
  char *  ptr = dataRowAt(row, dataRowLen(row));

  switch (type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      if (value == NULL) {
        *(int32_t *)dataRowAt(row, toffset) = -1;
      } else {
        int16_t slen = (type) ? strlen((char *)value) : wcslen((wchar_t *)value) * TSDB_NCHAR_SIZE;
        if (slen > bytes) return -1;

        *(int32_t *)dataRowAt(row, toffset) = dataRowLen(row);
        *(int16_t *)ptr = slen;
        ptr += sizeof(int16_t);
        memcpy((void *)ptr, value, slen);
        dataRowLen(row) += (sizeof(int16_t) + slen);
      }
      break;
    default:
      if (value == NULL) {
        setNull(dataRowAt(row, toffset), type, bytes);
      } else {
        memcpy(dataRowAt(row, toffset), value, TYPE_BYTES[type]);
      }
      break;
  }

  return 0;
}

void tdDataRowReset(SDataRow row, STSchema *pSchema) { tdInitDataRow(row, pSchema); }

SDataRow tdDataRowDup(SDataRow row) {
  SDataRow trow = malloc(dataRowLen(row));
  if (trow == NULL) return NULL;

  dataRowCpy(trow, row);
  return trow;
}

SDataCols *tdNewDataCols(int maxRowSize, int maxCols, int maxRows) {
  SDataCols *pCols = (SDataCols *)calloc(1, sizeof(SDataCols) + sizeof(SDataCol) * maxCols);
  if (pCols == NULL) return NULL;

  pCols->maxRowSize = maxRowSize;
  pCols->maxCols = maxCols;
  pCols->maxPoints = maxRows;

  pCols->buf = malloc(maxRowSize * maxRows);
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

  pCols->cols[0].pData = pCols->buf;
  int offset = TD_DATA_ROW_HEAD_SIZE;
  for (int i = 0; i < schemaNCols(pSchema); i++) {
    if (i > 0) {
      pCols->cols[i].pData = (char *)(pCols->cols[i - 1].pData) + schemaColAt(pSchema, i - 1)->bytes * pCols->maxPoints;
    }
    pCols->cols[i].type = colType(schemaColAt(pSchema, i));
    pCols->cols[i].bytes = colBytes(schemaColAt(pSchema, i));
    pCols->cols[i].offset = offset;
    pCols->cols[i].colId = colColId(schemaColAt(pSchema, i));

    offset += TYPE_BYTES[pCols->cols[i].type];
  }
}

void tdFreeDataCols(SDataCols *pCols) {
  if (pCols) {
    if (pCols->buf) free(pCols->buf);
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
    pRet->cols[i].len = pDataCols->cols[i].len;
    pRet->cols[i].offset = pDataCols->cols[i].offset;
    pRet->cols[i].pData = (void *)((char *)pRet->buf + ((char *)(pDataCols->cols[i].pData) - (char *)(pDataCols->buf)));

    if (keepData) memcpy(pRet->cols[i].pData, pDataCols->cols[i].pData, pRet->cols[i].bytes * pDataCols->numOfPoints);
  }

  return pRet;
}

void tdResetDataCols(SDataCols *pCols) {
  pCols->numOfPoints = 0;
  for (int i = 0; i < pCols->maxCols; i++) {
    pCols->cols[i].len = 0;
  }
}

void tdAppendDataRowToDataCol(SDataRow row, SDataCols *pCols) {
  for (int i = 0; i < pCols->numOfCols; i++) {
    SDataCol *pCol = pCols->cols + i;
    memcpy((void *)((char *)(pCol->pData) + pCol->len), dataRowAt(row, pCol->offset), pCol->bytes);
    pCol->len += pCol->bytes;
  }
  pCols->numOfPoints++;
}
// Pop pointsToPop points from the SDataCols
void tdPopDataColsPoints(SDataCols *pCols, int pointsToPop) {
  int pointsLeft = pCols->numOfPoints - pointsToPop;

  for (int iCol = 0; iCol < pCols->numOfCols; iCol++) {
    SDataCol *p_col = pCols->cols + iCol;
    if (p_col->len > 0) {
      p_col->len = TYPE_BYTES[p_col->type] * pointsLeft;
      if (pointsLeft > 0) {
        memmove((void *)(p_col->pData), (void *)((char *)(p_col->pData) + TYPE_BYTES[p_col->type] * pointsToPop), p_col->len);
      }
    }
  }
  pCols->numOfPoints = pointsLeft;
}

int tdMergeDataCols(SDataCols *target, SDataCols *source, int rowsToMerge) {
  ASSERT(rowsToMerge > 0 && rowsToMerge <= source->numOfPoints);

  SDataCols *pTarget = tdDupDataCols(target, true);
  if (pTarget == NULL) goto _err;
  // tdResetDataCols(target);

  int iter1 = 0;
  int iter2 = 0;
  tdMergeTwoDataCols(target,pTarget, &iter1, source, &iter2, pTarget->numOfPoints + rowsToMerge);

  tdFreeDataCols(pTarget);
  return 0;

_err:
  tdFreeDataCols(pTarget);
  return -1;
}

void tdMergeTwoDataCols(SDataCols *target, SDataCols *src1, int *iter1, SDataCols *src2, int *iter2, int tRows) {
  tdResetDataCols(target);

  while (target->numOfPoints < tRows) {
    if (*iter1 >= src1->numOfPoints && *iter2 >= src2->numOfPoints) break;

    TSKEY key1 = (*iter1 >= src1->numOfPoints) ? INT64_MAX : ((TSKEY *)(src1->cols[0].pData))[*iter1];
    TSKEY key2 = (*iter2 >= src2->numOfPoints) ? INT64_MAX : ((TSKEY *)(src2->cols[0].pData))[*iter2];

    if (key1 < key2) {
      for (int i = 0; i < src1->numOfCols; i++) {
        ASSERT(target->cols[i].type == src1->cols[i].type);
        memcpy((void *)((char *)(target->cols[i].pData) + TYPE_BYTES[target->cols[i].type] * target->numOfPoints),
               (void *)((char *)(src1->cols[i].pData) + TYPE_BYTES[target->cols[i].type] * (*iter1)),
               TYPE_BYTES[target->cols[i].type]);
        target->cols[i].len += TYPE_BYTES[target->cols[i].type];
      }

      target->numOfPoints++;
      (*iter1)++;
    } else if (key1 > key2) {
      for (int i = 0; i < src2->numOfCols; i++) {
        ASSERT(target->cols[i].type == src2->cols[i].type);
        memcpy((void *)((char *)(target->cols[i].pData) + TYPE_BYTES[target->cols[i].type] * target->numOfPoints),
               (void *)((char *)(src2->cols[i].pData) + TYPE_BYTES[src2->cols[i].type] * (*iter2)),
               TYPE_BYTES[target->cols[i].type]);
        target->cols[i].len += TYPE_BYTES[target->cols[i].type];
      }

      target->numOfPoints++;
      (*iter2)++;
    } else {
      ASSERT(false);
    }
  }
}