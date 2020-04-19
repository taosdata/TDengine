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

#ifdef __cplusplus
extern "C" {
#endif

// ----------------- TSDB COLUMN DEFINITION
typedef struct {
  int8_t  type;    // Column type
  int16_t colId;   // column ID
  int32_t bytes;   // column bytes
  int32_t offset;  // point offset in a row data
} STColumn;

#define colType(col) ((col)->type)
#define colColId(col) ((col)->colId)
#define colBytes(col) ((col)->bytes)
#define colOffset(col) ((col)->offset)

#define colSetType(col, t) (colType(col) = (t))
#define colSetColId(col, id) (colColId(col) = (id))
#define colSetBytes(col, b) (colBytes(col) = (b))
#define colSetOffset(col, o) (colOffset(col) = (o))

STColumn *tdNewCol(int8_t type, int16_t colId, int16_t bytes);
void      tdFreeCol(STColumn *pCol);
void      tdColCpy(STColumn *dst, STColumn *src);
void      tdSetCol(STColumn *pCol, int8_t type, int16_t colId, int32_t bytes);

// ----------------- TSDB SCHEMA DEFINITION
typedef struct {
  int      numOfCols;  // Number of columns appended
  int      padding;  // Total columns allocated
  STColumn columns[];
} STSchema;

#define schemaNCols(s) ((s)->numOfCols)
#define schemaColAt(s, i) ((s)->columns + i)

STSchema *tdNewSchema(int32_t nCols);
int       tdSchemaAppendCol(STSchema *pSchema, int8_t type, int16_t colId, int32_t bytes);
STSchema *tdDupSchema(STSchema *pSchema);
void      tdFreeSchema(STSchema *pSchema);
void      tdUpdateSchema(STSchema *pSchema);
int       tdGetSchemaEncodeSize(STSchema *pSchema);
void *    tdEncodeSchema(void *dst, STSchema *pSchema);
STSchema *tdDecodeSchema(void **psrc);

// ----------------- Data row structure

/* A data row, the format is like below:
 * +----------+---------+---------------------------------+---------------------------------+
 * | int32_t  | int32_t |                                 |                                 |
 * +----------+---------+---------------------------------+---------------------------------+
 * |   len    |  flen   |           First part            |             Second part         |
 * +----------+---------+---------------------------------+---------------------------------+
 * plen: first part length
 * len: the length including sizeof(row) + sizeof(len)
 * row: actual row data encoding
 */
typedef void *SDataRow;


#define TD_DATA_ROW_HEAD_SIZE (2 * sizeof(int32_t))

#define dataRowLen(r) (*(int32_t *)(r))
#define dataRowFLen(r) (*(int32_t *)((char *)(r) + sizeof(int32_t)))
#define dataRowTuple(r) ((char *)(r) + TD_DATA_ROW_HEAD_SIZE)
#define dataRowKey(r) (*(TSKEY *)(dataRowTuple(r)))
#define dataRowSetLen(r, l) (dataRowLen(r) = (l))
#define dataRowSetFLen(r, l) (dataRowFLen(r) = (l))
#define dataRowIdx(r, i) ((char *)(r) + i)
#define dataRowCpy(dst, r) memcpy((dst), (r), dataRowLen(r))
#define dataRowAt(r, idx) ((char *)(r) + (idx))

void     tdInitDataRow(SDataRow row, STSchema *pSchema);
int      tdMaxRowBytesFromSchema(STSchema *pSchema);
SDataRow tdNewDataRow(int32_t bytes, STSchema *pSchema);
SDataRow tdNewDataRowFromSchema(STSchema *pSchema);
void     tdFreeDataRow(SDataRow row);
int      tdAppendColVal(SDataRow row, void *value, STColumn *pCol);
void     tdDataRowReset(SDataRow row, STSchema *pSchema);
SDataRow tdDataRowDup(SDataRow row);

// ----------------- Data column structure
typedef struct SDataCol {
  int8_t  type;
  int16_t colId;
  int     bytes;
  int     len;
  int     offset;
  void *  pData; // Original data
} SDataCol;

typedef struct {
  int      maxRowSize;
  int      maxCols;    // max number of columns
  int      maxPoints;  // max number of points
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

SDataCols *tdNewDataCols(int maxRowSize, int maxCols, int maxRows);
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
