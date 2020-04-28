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
#ifndef _TD_DATA_FORMAT_H_
#define _TD_DATA_FORMAT_H_

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "taosdef.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

// ----------------- TSDB COLUMN DEFINITION
typedef struct {
  int8_t  type;    // Column type
  int16_t colId;   // column ID
  int32_t bytes;   // column bytes
  int32_t offset;  // point offset in SDataRow after the header part
} STColumn;

#define colType(col) ((col)->type)
#define colColId(col) ((col)->colId)
#define colBytes(col) ((col)->bytes)
#define colOffset(col) ((col)->offset)

#define colSetType(col, t) (colType(col) = (t))
#define colSetColId(col, id) (colColId(col) = (id))
#define colSetBytes(col, b) (colBytes(col) = (b))
#define colSetOffset(col, o) (colOffset(col) = (o))

// ----------------- TSDB SCHEMA DEFINITION
typedef struct {
  int      totalCols;  // Total columns allocated
  int      numOfCols;  // Number of columns appended
  int      tlen;       // maximum length of a SDataRow without the header part
  int      flen;       // First part length in a SDataRow after the header part
  STColumn columns[];
} STSchema;

#define schemaNCols(s) ((s)->numOfCols)
#define schemaTotalCols(s) ((s)->totalCols)
#define schemaTLen(s) ((s)->tlen)
#define schemaFLen(s) ((s)->flen)
#define schemaColAt(s, i) ((s)->columns + i)

STSchema *tdNewSchema(int32_t nCols);
#define   tdFreeSchema(s) tfree((s))
int       tdSchemaAddCol(STSchema *pSchema, int8_t type, int16_t colId, int32_t bytes);
STSchema *tdDupSchema(STSchema *pSchema);
int       tdGetSchemaEncodeSize(STSchema *pSchema);
void *    tdEncodeSchema(void *dst, STSchema *pSchema);
STSchema *tdDecodeSchema(void **psrc);

// ----------------- Data row structure

/* A data row, the format is like below:
 * |<------------------------------------- len ---------------------------------->|
 * |<--Head ->|<---------   flen -------------->|                                 |
 * +----------+---------------------------------+---------------------------------+
 * | int32_t  |                                 |                                 |
 * +----------+---------------------------------+---------------------------------+
 * |   len    |           First part            |             Second part         |
 * +----------+---------------------------------+---------------------------------+
 */
typedef void *SDataRow;

#define TD_DATA_ROW_HEAD_SIZE sizeof(int32_t)

#define dataRowLen(r) (*(int32_t *)(r))
#define dataRowAt(r, idx) ((char *)(r) + (idx))
#define dataRowTuple(r) dataRowAt(r, TD_DATA_ROW_HEAD_SIZE)
#define dataRowKey(r) (*(TSKEY *)(dataRowTuple(r)))
#define dataRowSetLen(r, l) (dataRowLen(r) = (l))
#define dataRowCpy(dst, r) memcpy((dst), (r), dataRowLen(r))
#define dataRowMaxBytesFromSchema(s) ((s)->tlen + TD_DATA_ROW_HEAD_SIZE)

SDataRow tdNewDataRowFromSchema(STSchema *pSchema);
void     tdFreeDataRow(SDataRow row);
void     tdInitDataRow(SDataRow row, STSchema *pSchema);
int      tdAppendColVal(SDataRow row, void *value, int8_t type, int32_t bytes, int32_t offset);
void     tdDataRowReset(SDataRow row, STSchema *pSchema);
SDataRow tdDataRowDup(SDataRow row);

static FORCE_INLINE void *tdGetRowDataOfCol(SDataRow row, int8_t type, int32_t offset) {
  switch (type) {
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      return dataRowAt(row, *(int32_t *)dataRowAt(row, offset));
      break;
    default:
      return row + offset;
      break;
  }
}

// ----------------- Data column structure
typedef struct SDataCol {
  int8_t  type;
  int16_t colId;
  int     bytes;
  int     len;
  int     offset;
  void *  pData; // Original data
} SDataCol;

void dataColAppendVal(SDataCol *pCol, void *value, int numOfPoints, int maxPoints);

// Get the data pointer from a column-wised data
static FORCE_INLINE void *tdGetColDataOfRow(SDataCol *pCol, int row) {
  switch (pCol->type)
  {
  case TSDB_DATA_TYPE_BINARY:
  case TSDB_DATA_TYPE_NCHAR:
    return pCol->pData + ((int32_t *)(pCol->pData))[row];
    break;

  default:
    return pCol->pData + TYPE_BYTES[pCol->type] * row;
    break;
  }
}

typedef struct {
  int      maxRowSize;
  int      maxCols;    // max number of columns
  int      maxPoints;  // max number of points
  int      exColBytes; // extra column bytes to allocate for each column

  int      numOfPoints;
  int      numOfCols;  // Total number of cols
  int      sversion;   // TODO: set sversion
  void *   buf;
  SDataCol cols[];
} SDataCols;

#define keyCol(pCols) (&((pCols)->cols[0]))  // Key column
#define dataColsKeyAt(pCols, idx) ((int64_t *)(keyCol(pCols)->pData))[(idx)]
#define dataColsKeyFirst(pCols) dataColsKeyAt(pCols, 0)
#define dataColsKeyLast(pCols) dataColsKeyAt(pCols, (pCols)->numOfPoints - 1)

SDataCols *tdNewDataCols(int maxRowSize, int maxCols, int maxRows, int exColBytes);
void       tdResetDataCols(SDataCols *pCols);
void       tdInitDataCols(SDataCols *pCols, STSchema *pSchema);
SDataCols *tdDupDataCols(SDataCols *pCols, bool keepData);
void       tdFreeDataCols(SDataCols *pCols);
void       tdAppendDataRowToDataCol(SDataRow row, SDataCols *pCols);
void       tdPopDataColsPoints(SDataCols *pCols, int pointsToPop);
int        tdMergeDataCols(SDataCols *target, SDataCols *src, int rowsToMerge);
void       tdMergeTwoDataCols(SDataCols *target, SDataCols *src1, int *iter1, SDataCols *src2, int *iter2, int tRows);

#ifdef __cplusplus
}
#endif

#endif  // _TD_DATA_FORMAT_H_
