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
  pSchema->numOfCols = nCols;

  return pSchema;
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
 * Create a data row with maximum row length bytes.
 *
 * NOTE: THE AAPLICATION SHOULD MAKE SURE BYTES IS LARGE ENOUGH TO
 * HOLD THE WHOE ROW.
 *
 * @param bytes max bytes a row can take
 * @return SDataRow object for success
 *         NULL for failure
 */
SDataRow tdNewDataRow(int32_t bytes) {
  int32_t size = sizeof(int32_t) + bytes;

  SDataRow row = malloc(size);
  if (row == NULL) return NULL;

  dataRowSetLen(row, sizeof(int32_t));

  return row;
}

// SDataRow tdNewDdataFromSchema(SSchema *pSchema) {
//   int32_t bytes = tdMaxRowDataBytes(pSchema);
//   return tdNewDataRow(bytes);
// }

/**
 * Free the SDataRow object
 */
void tdFreeDataRow(SDataRow row) {
  if (row) free(row);
}

/**
 * Append a column value to a SDataRow object.
 * NOTE: THE APPLICATION SHOULD MAKE SURE VALID PARAMETERS. THE FUNCTION ASSUMES
 * THE ROW OBJECT HAS ENOUGH SPACE TO HOLD THE VALUE.
 *
 * @param row the row to append value to
 * @param value value pointer to append
 * @param pSchema schema
 * @param colIdx column index
 * 
 * @return 0 for success and -1 for failure
 */
// int32_t tdAppendColVal(SDataRow row, void *value, SColumn *pCol, int32_t suffixOffset) {
//   int32_t offset;

//   switch (pCol->type) {
//     case TD_DATATYPE_BOOL:
//     case TD_DATATYPE_TINYINT:
//     case TD_DATATYPE_SMALLINT:
//     case TD_DATATYPE_INT:
//     case TD_DATATYPE_BIGINT:
//     case TD_DATATYPE_FLOAT:
//     case TD_DATATYPE_DOUBLE:
//     case TD_DATATYPE_TIMESTAMP:
//       memcpy(dataRowIdx(row, pCol->offset + sizeof(int32_t)), value, rowDataLen[pCol->type]);
//       if (dataRowLen(row) < suffixOffset + sizeof(int32_t))
//         dataRowSetLen(row, dataRowLen(row) + rowDataLen[pCol->type]);
//       break;
//     case TD_DATATYPE_VARCHAR:
//       offset = dataRowLen(row) > suffixOffset ? dataRowLen(row) : suffixOffset;
//       memcpy(dataRowIdx(row, pCol->offset+sizeof(int32_t)), (void *)(&offset), sizeof(offset));
//     case TD_DATATYPE_NCHAR:
//     case TD_DATATYPE_BINARY:
//       break;
//     default:
//       return -1;
//   }

//   return 0;
// }

/**
 * Copy a data row to a destination
 * ASSUMPTIONS: dst has enough room for a copy of row
 */
void tdDataRowCpy(void *dst, SDataRow row) { memcpy(dst, row, dataRowLen(row)); }
void tdDataRowReset(SDataRow row) { dataRowSetLen(row, sizeof(int32_t)); }
SDataRow tdDataRowDup(SDataRow row) {
  SDataRow trow = tdNewDataRow(dataRowLen(row));
  if (trow == NULL) return NULL;

  dataRowCpy(trow, row);
  return row;
}

void tdDataRowsAppendRow(SDataRows rows, SDataRow row) {
  tdDataRowCpy((void *)((char *)rows + dataRowsLen(rows)), row);
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