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

#ifndef _TD_COMMON_ROW_H_
#define _TD_COMMON_ROW_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

// types
typedef void *                  SRow;
typedef struct SRowBatch        SRowBatch;
typedef struct SRowBuilder      SRowBuilder;
typedef struct SRowBatchIter    SRowBatchIter;
typedef struct SRowBatchBuilder SRowBatchBuilder;

// SRow
#define ROW_HEADER_SIZE (sizeof(uint8_t) + 2 * sizeof(uint16_t) + sizeof(uint64_t))
#define rowType(r) (*(uint8_t *)(r))                                // row type
#define rowLen(r) (*(uint16_t *)POINTER_SHIFT(r, sizeof(uint8_t)))  // row length
#define rowSVer(r) \
  (*(uint16_t *)POINTER_SHIFT(r, sizeof(uint8_t) + sizeof(uint16_t)))  // row schema version, only for SDataRow
#define rowNCols(r) rowSVer(r)                                         // only for SKVRow
#define rowVer(r) (*(uint64_t)POINTER_SHIFT(r, sizeof(uint8_t) + 2 * sizeof(uint16_t)))  // row version
#define rowCopy(dest, r) memcpy((dest), r, rowLen(r))

static FORCE_INLINE SRow rowDup(SRow row) {
  SRow r = malloc(rowLen(row));
  if (r == NULL) {
    return NULL;
  }

  rowCopy(r, row);

  return r;
}

// SRowBatch

// SRowBuilder
SRowBuilder *rowBuilderCreate();
void         rowBuilderDestroy(SRowBuilder *);

// SRowBatchIter
SRowBatchIter *rowBatchIterCreate(SRowBatch *);
void           rowBatchIterDestroy(SRowBatchIter *);
const SRow     rowBatchIterNext(SRowBatchIter *);

// SRowBatchBuilder
SRowBatchBuilder *rowBatchBuilderCreate();
void              rowBatchBuilderDestroy(SRowBatchBuilder *);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_ROW_H_*/