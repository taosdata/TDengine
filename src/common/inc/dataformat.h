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
  int      totalCols;  // Total columns allocated
  STColumn columns[];
} STSchema;

#define schemaNCols(s) ((s)->numOfCols)
#define schemaTCols(s) ((s)->totalCols)
#define schemaColAt(s, i) ((s)->columns + i)

STSchema *tdNewSchema(int32_t nCols);
int       tdSchemaAppendCol(STSchema *pSchema, int8_t type, int16_t colId, int16_t bytes);
STSchema *tdDupSchema(STSchema *pSchema);
void      tdFreeSchema(STSchema *pSchema);
void      tdUpdateSchema(STSchema *pSchema);

// ----------------- Data row structure

/* A data row, the format is like below:
 * +---------+---------------------------------+
 * | int32_t |                                 |
 * +---------+---------------------------------+
 * |   len   |                row              |
 * +---------+---------------------------------+
 * len: the length including sizeof(row) + sizeof(len)
 * row: actual row data encoding
 */
typedef void *SDataRow;

#define TD_DATA_ROW_HEAD_SIZE sizeof(int32_t)

#define dataRowLen(r) (*(int32_t *)(r))
#define dataRowTuple(r) ((char *)(r) + TD_DATA_ROW_HEAD_SIZE)
#define dataRowSetLen(r, l) (dataRowLen(r) = (l))
#define dataRowIdx(r, i) ((char *)(r) + i)
#define dataRowCpy(dst, r) memcpy((dst), (r), dataRowLen(r))

SDataRow tdNewDataRow(int32_t bytes);
int      tdMaxRowBytesFromSchema(STSchema *pSchema);
SDataRow tdNewDataRowFromSchema(STSchema *pSchema);
void     tdFreeDataRow(SDataRow row);
// int32_t  tdAppendColVal(SDataRow row, void *value, SColumn *pCol, int32_t suffixOffset);
void     tdDataRowCpy(void *dst, SDataRow row);
void     tdDataRowReset(SDataRow row);
SDataRow tdDataRowDup(SDataRow row);

/* Data rows definition, the format of it is like below:
 * +---------+-----------------------+--------+-----------------------+
 * | int32_t |                       |        |                       |
 * +---------+-----------------------+--------+-----------------------+
 * |   len   |        SDataRow       |  ....  |        SDataRow       |
 * +---------+-----------------------+--------+-----------------------+
 */
typedef void *SDataRows;

#define TD_DATA_ROWS_HEAD_LEN sizeof(int32_t)

#define dataRowsLen(rs) (*(int32_t *)(rs))
#define dataRowsSetLen(rs, l) (dataRowsLen(rs) = (l))
#define dataRowsInit(rs) dataRowsSetLen(rs, sizeof(int32_t))

void tdDataRowsAppendRow(SDataRows rows, SDataRow row);

// Data rows iterator
typedef struct {
  int32_t totalLen;
  int32_t len;
  SDataRow row;
} SDataRowsIter;

void tdInitSDataRowsIter(SDataRows rows, SDataRowsIter *pIter);
SDataRow tdDataRowsNext(SDataRowsIter *pIter);

/* Data column definition
 * +---------+---------+-----------------------+
 * | int32_t | int32_t |                       |
 * +---------+---------+-----------------------+
 * |   len   | npoints |          data         |
 * +---------+---------+-----------------------+
 */
typedef char *SDataCol;

/* Data columns definition
 * +---------+---------+-----------------------+--------+-----------------------+
 * | int32_t | int32_t |                       |        |                       |
 * +---------+---------+-----------------------+--------+-----------------------+
 * |   len   | npoints |        SDataCol       |  ....  |        SDataCol       |
 * +---------+---------+-----------------------+--------+-----------------------+
 */
typedef char *SDataCols;

#ifdef __cplusplus
}
#endif

#endif  // _TD_DATA_FORMAT_H_
