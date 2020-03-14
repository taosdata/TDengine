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
#include "dataformat.h"

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

  STSchema *pSchema = (STSchema *)malloc(size);
  if (pSchema == NULL) return NULL;
  pSchema->numOfCols = 0;
  pSchema->totalCols = nCols;

  return pSchema;
}

/**
 * Append a column to the schema
 */
int tdSchemaAppendCol(STSchema *pSchema, int8_t type, int16_t colId, int16_t bytes) {
  if (pSchema->numOfCols >= pSchema->totalCols) return -1;
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
  if (pSchema == NULL) free(pSchema);
}

/**
 * Function to update each columns's offset field in the schema.
 * ASSUMPTIONS: VALID PARAMETERS
 */
void tdUpdateSchema(STSchema *pSchema) {
  STColumn *pCol = NULL;
  int32_t offset = 0;
  for (int i = 0; i < schemaNCols(pSchema); i++) {
    pCol = schemaColAt(pSchema, i);
    colSetOffset(pCol, offset);
    offset += TYPE_BYTES[pCol->type];
  }
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
}

void tdDataRowReset(SDataRow row, STSchema *pSchema) { tdInitDataRow(row, pSchema); }

SDataRow tdDataRowDup(SDataRow row) {
  SDataRow trow = malloc(dataRowLen(row));
  if (trow == NULL) return NULL;

  dataRowCpy(trow, row);
  return trow;
}

void tdDataRowsAppendRow(SDataRows rows, SDataRow row) {
  dataRowCpy((void *)((char *)rows + dataRowsLen(rows)), row);
  dataRowsSetLen(rows, dataRowsLen(rows) + dataRowLen(row));
}

// Initialize the iterator
void tdInitSDataRowsIter(SDataRows rows, SDataRowsIter *pIter) {
  if (pIter == NULL) return;
  pIter->totalLen = dataRowsLen(rows);

  if (pIter->totalLen == TD_DATA_ROWS_HEAD_LEN) {
    pIter->row = NULL;
    return;
  }

  pIter->row = (SDataRow)((char *)rows + TD_DATA_ROWS_HEAD_LEN);
  pIter->len = TD_DATA_ROWS_HEAD_LEN + dataRowLen(pIter->row);
}

// Get the next row in Rows
SDataRow tdDataRowsNext(SDataRowsIter *pIter) {
  SDataRow row = pIter->row;
  if (row == NULL) return NULL;

  if (pIter->len >= pIter->totalLen) {
    pIter->row = NULL;
  } else {
    pIter->row = (char *)row + dataRowLen(row);
    pIter->len += dataRowLen(row);
  }

  return row;
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