#include "dataformat.h"

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

SDataRow tdNewDdataFromSchema(SSchema *pSchema) {
  int32_t bytes = tdMaxRowDataBytes(pSchema);
  return tdNewDataRow(bytes);
}

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
int32_t tdAppendColVal(SDataRow row, void *value, SColumn *pCol, int32_t suffixOffset) {
  int32_t offset;

  switch (pCol->type) {
    case TD_DATATYPE_BOOL:
    case TD_DATATYPE_TINYINT:
    case TD_DATATYPE_SMALLINT:
    case TD_DATATYPE_INT:
    case TD_DATATYPE_BIGINT:
    case TD_DATATYPE_FLOAT:
    case TD_DATATYPE_DOUBLE:
    case TD_DATATYPE_TIMESTAMP:
      memcpy(dataRowIdx(row, pCol->offset + sizeof(int32_t)), value, rowDataLen[pCol->type]);
      if (dataRowLen(row) < suffixOffset + sizeof(int32_t))
        dataRowSetLen(row, dataRowLen(row) + rowDataLen[pCol->type]);
      break;
    case TD_DATATYPE_VARCHAR:
      offset = dataRowLen(row) > suffixOffset ? dataRowLen(row) : suffixOffset;
      memcpy(dataRowIdx(row, pCol->offset+sizeof(int32_t)), (void *)(&offset), sizeof(offset));
    case TD_DATATYPE_NCHAR:
    case TD_DATATYPE_BINARY:
      break;
    default:
      return -1;
  }

  return 0;
}

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