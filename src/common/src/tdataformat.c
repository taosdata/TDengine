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

static int tdFLenFromSchema(STSchema *pSchema);

/**
 * Create a new STColumn object
 * ASSUMPTIONS: VALID PARAMETERS
 * 
 * @param type column type
 * @param colId column ID
 * @param bytes maximum bytes the col taken
 * 
 * @return a STColumn object on success
 *         NULL for failure
 */
STColumn *tdNewCol(int8_t type, int16_t colId, int16_t bytes) {
  if (!isValidDataType(type, 0)) return NULL;

  STColumn *pCol = (STColumn *)calloc(1, sizeof(STColumn));
  if (pCol == NULL) return NULL;

  colSetType(pCol, type);
  colSetColId(pCol, colId);
  colSetOffset(pCol, -1);
  switch (type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      colSetBytes(pCol, bytes);
      break;
    default:
      colSetBytes(pCol, TYPE_BYTES[type]);
      break;
  }

  return pCol;
}

/**
 * Free a STColumn object CREATED with tdNewCol
 */
void tdFreeCol(STColumn *pCol) {
  if (pCol) free(pCol);
}

/**
 * Copy from source to destinition
 */
void tdColCpy(STColumn *dst, STColumn *src) { memcpy((void *)dst, (void *)src, sizeof(STColumn)); }

/**
 * Set the column
 */
void tdSetCol(STColumn *pCol, int8_t type, int16_t colId, int32_t bytes) {
  colSetType(pCol, type);
  colSetColId(pCol, colId);
  switch (type)
  {
  case TSDB_DATA_TYPE_BINARY:
  case TSDB_DATA_TYPE_NCHAR:
    colSetBytes(pCol, bytes);
    break;
  default:
    colSetBytes(pCol, TYPE_BYTES[type]);
    break;
  }
}

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
  int32_t  size = sizeof(STSchema) + sizeof(STColumn) * nCols;

  STSchema *pSchema = (STSchema *)calloc(1, size);
  if (pSchema == NULL) return NULL;
  pSchema->numOfCols = 0;

  return pSchema;
}

/**
 * Append a column to the schema
 */
int tdSchemaAppendCol(STSchema *pSchema, int8_t type, int16_t colId, int32_t bytes) {
  // if (pSchema->numOfCols >= pSchema->totalCols) return -1;
  if (!isValidDataType(type, 0)) return -1;

  STColumn *pCol = schemaColAt(pSchema, schemaNCols(pSchema));
  colSetType(pCol, type);
  colSetColId(pCol, colId);
  colSetOffset(pCol, -1);
  switch (type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      colSetBytes(pCol, bytes);
      break;
    default:
      colSetBytes(pCol, TYPE_BYTES[type]);
      break;
  }

  pSchema->numOfCols++;

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
 * Free the SSchema object created by tdNewSchema or tdDupSchema
 */
void tdFreeSchema(STSchema *pSchema) {
  if (pSchema != NULL) free(pSchema);
}

/**
 * Function to update each columns's offset field in the schema.
 * ASSUMPTIONS: VALID PARAMETERS
 */
void tdUpdateSchema(STSchema *pSchema) {
  STColumn *pCol = NULL;
  int32_t offset = TD_DATA_ROW_HEAD_SIZE;
  for (int i = 0; i < schemaNCols(pSchema); i++) {
    pCol = schemaColAt(pSchema, i);
    colSetOffset(pCol, offset);
    offset += TYPE_BYTES[pCol->type];
  }
}

/**
 * Return the size of encoded schema
 */
int tdGetSchemaEncodeSize(STSchema *pSchema) {
  return sizeof(STSchema) + schemaNCols(pSchema) * (T_MEMBER_SIZE(STColumn, type) + T_MEMBER_SIZE(STColumn, colId) +
                                                    T_MEMBER_SIZE(STColumn, bytes));
}

/**
 * Encode a schema to dst, and return the next pointer
 */
void *tdEncodeSchema(void *dst, STSchema *pSchema) {
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
  int numOfCols = 0;

  T_READ_MEMBER(*psrc, int, numOfCols);

  STSchema *pSchema = tdNewSchema(numOfCols);
  if (pSchema == NULL) return NULL;
  for (int i = 0; i < numOfCols; i++) {
    int8_t  type = 0;
    int16_t colId = 0;
    int32_t bytes = 0;
    T_READ_MEMBER(*psrc, int8_t, type);
    T_READ_MEMBER(*psrc, int16_t, colId);
    T_READ_MEMBER(*psrc, int32_t, bytes);

    tdSchemaAppendCol(pSchema, type, colId, bytes);
  }

  return pSchema;
}

/**
 * Initialize a data row
 */
void tdInitDataRow(SDataRow row, STSchema *pSchema) {
  dataRowSetFLen(row, TD_DATA_ROW_HEAD_SIZE);
  dataRowSetLen(row, TD_DATA_ROW_HEAD_SIZE + tdFLenFromSchema(pSchema));
}

/**
 * Create a data row with maximum row length bytes.
 *
 * NOTE: THE AAPLICATION SHOULD MAKE SURE BYTES IS LARGE ENOUGH TO
 * HOLD THE WHOE ROW.
 *
 * @param bytes max bytes a row can take
 * @return SDataRow object for success
 *         NULL for failure
 */
SDataRow tdNewDataRow(int32_t bytes, STSchema *pSchema) {
  int32_t size = sizeof(int32_t) + bytes;

  SDataRow row = malloc(size);
  if (row == NULL) return NULL;

  tdInitDataRow(row, pSchema);

  return row;
}

/**
 * Get maximum bytes a data row from a schema
 * ASSUMPTIONS: VALID PARAMETER
 */
int tdMaxRowBytesFromSchema(STSchema *pSchema) {
  // TODO
  int bytes = TD_DATA_ROW_HEAD_SIZE;
  for (int i = 0; i < schemaNCols(pSchema); i++) {
    STColumn *pCol = schemaColAt(pSchema, i);
    bytes += TYPE_BYTES[pCol->type];

    if (pCol->type == TSDB_DATA_TYPE_BINARY || pCol->type == TSDB_DATA_TYPE_NCHAR) {
      bytes += pCol->bytes;
    }
  }

  return bytes;
}

SDataRow tdNewDataRowFromSchema(STSchema *pSchema) { return tdNewDataRow(tdMaxRowBytesFromSchema(pSchema), pSchema); }

/**
 * Free the SDataRow object
 */
void tdFreeDataRow(SDataRow row) {
  if (row) free(row);
}

/**
 * Append a column value to the data row
 */
int tdAppendColVal(SDataRow row, void *value, STColumn *pCol) {
  switch (colType(pCol))
  {
  case TSDB_DATA_TYPE_BINARY:
  case TSDB_DATA_TYPE_NCHAR:
    *(int32_t *)dataRowAt(row, dataRowFLen(row)) = dataRowLen(row);
    dataRowFLen(row) += TYPE_BYTES[colType(pCol)];
    memcpy((void *)dataRowAt(row, dataRowLen(row)), value, strlen(value));
    dataRowLen(row) += strlen(value);
    break;
  default:
    memcpy(dataRowAt(row, dataRowFLen(row)), value, TYPE_BYTES[colType(pCol)]);
    dataRowFLen(row) += TYPE_BYTES[colType(pCol)];
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

/**
 * Return the first part length of a data row for a schema
 */
static int tdFLenFromSchema(STSchema *pSchema) {
  int ret = 0;
  for (int i = 0; i < schemaNCols(pSchema); i++) {
    STColumn *pCol = schemaColAt(pSchema, i);
    ret += TYPE_BYTES[pCol->type];
  }

  return ret;
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