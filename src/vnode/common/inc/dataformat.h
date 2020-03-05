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
#if !defined(_TD_DATA_FORMAT_H_)
#define _TD_DATA_FORMAT_H_

#include <stdint.h>

#include "schema.h"

#ifdef __cplusplus
extern "C" {
#endif
// ----------------- Data row structure

/* A data row, the format of it is like below:
 * +---------+---------------------------------+
 * | int32_t |                                 |
 * +---------+---------------------------------+
 * |   len   |               data              |
 * +---------+---------------------------------+
 */
typedef char* SDataRow;

/* Data rows definition, the format of it is like below:
 * +---------+---------+-----------------------+--------+-----------------------+
 * | int32_t | int32_t |                       |        |                       |
 * +---------+---------+-----------------------+--------+-----------------------+
 * |   len   |  nrows  |        SDataRow       |  ....  |        SDataRow       |
 * +---------+---------+-----------------------+--------+-----------------------+
 */
typedef char * SDataRows;

/* Data column definition
 * +---------+---------+-----------------------+
 * | int32_t | int32_t |                       |
 * +---------+---------+-----------------------+
 * |   len   | npoints |          data         |
 * +---------+---------+-----------------------+
 */
typedef char * SDataCol;

/* Data columns definition
 * +---------+---------+-----------------------+--------+-----------------------+
 * | int32_t | int32_t |                       |        |                       |
 * +---------+---------+-----------------------+--------+-----------------------+
 * |   len   | npoints |        SDataCol       |  ....  |        SDataCol       |
 * +---------+---------+-----------------------+--------+-----------------------+
 */
typedef char * SDataCols;

typedef struct {
    int32_t rowCounter;
    int32_t totalRows;
    SDataRow row;
} SDataRowsIter;

// ----------------- Data column structure

// ---- operation on SDataRow;
#define TD_DATA_ROW_HEADER_SIZE  sizeof(int32_t)
#define TD_DATAROW_LEN(pDataRow) (*(int32_t *)(pDataRow))
#define TD_DATAROW_DATA(pDataRow) ((pDataRow) + sizeof(int32_t))

SDataRow tdSDataRowDup(SDataRow rdata);
void tdSDataRowCpy(SDataRow src, void *dst);
void tdFreeSDataRow(SDataRow rdata);

// ---- operation on SDataRows
#define TD_DATAROWS_LEN(pDataRows) (*(int32_t *)(pDataRows))
#define TD_DATAROWS_ROWS(pDataRows) (*(int32_t *)((pDataRows) + sizeof(int32_t)))
#define TD_DATAROWS_DATA(pDataRows) (SDataRow)((pDataRows) + 2 * sizeof(int32_t))

// ---- operation on SDataCol
#define TD_DATACOL_LEN(pDataCol) (*(int32_t *)(pDataCol))
#define TD_DATACOL_NPOINTS(pDataCol) (*(int32_t *)(pDataCol + sizeof(int32_t)))

// ---- operation on SDataCols
#define TD_DATACOLS_LEN(pDataCols) (*(int32_t *)(pDataCols))
#define TD_DATACOLS_NPOINTS(pDataCols) (*(int32_t *)(pDataCols + sizeof(int32_t)))

// ---- operation on SDataRowIter
int32_t tdInitSDataRowsIter(SDataRows rows, SDataRowsIter *pIter);
int32_t tdRdataIterEnd(SDataRowsIter *pIter);
void    tdRdataIterNext(SDataRowsIter *pIter);

// ----
/**
 * Get the maximum
 */
int32_t tdGetMaxDataRowSize(SSchema *pSchema);

#ifdef __cplusplus
}
#endif

#endif // _TD_DATA_FORMAT_H_
