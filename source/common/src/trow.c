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

#include "trow.h"

/* ------------ Structures ---------- */
struct SRowBatch {
  int32_t compress : 1;  // if batch row is compressed
  int32_t nrows : 31;    // number of rows
  int32_t tlen;          // total length (including `nrows` and `tlen`)
  char    rows[];
};

struct SRowBuilder {
  // TODO
};

struct SRowBatchIter {
  int32_t    counter;  // row counter
  SRowBatch *rb;       // row batch to iter
  SRow       nrow;     // next row
};

struct SRowBatchBuilder {
  // TODO
};

/* ------------ Methods ---------- */

// SRowBuilder
SRowBuilder *rowBuilderCreate() {
  SRowBuilder *pRowBuilder = NULL;
  // TODO

  return pRowBuilder;
}

void rowBuilderDestroy(SRowBuilder *pRowBuilder) {
  if (pRowBuilder) {
    free(pRowBuilder);
  }
}

// SRowBatchIter
SRowBatchIter *rowBatchIterCreate(SRowBatch *pRowBatch) {
  SRowBatchIter *pRowBatchIter = (SRowBatchIter *)malloc(sizeof(*pRowBatchIter));
  if (pRowBatchIter == NULL) {
    return NULL;
  }

  pRowBatchIter->counter = 0;
  pRowBatchIter->rb = pRowBatch;
  pRowBatchIter->nrow = pRowBatch->rows;

  return pRowBatchIter;
};

void rowBatchIterDestroy(SRowBatchIter *pRowBatchIter) {
  if (pRowBatchIter) {
    free(pRowBatchIter);
  }
}

const SRow rowBatchIterNext(SRowBatchIter *pRowBatchIter) {
  SRow r = NULL;
  if (pRowBatchIter->counter < pRowBatchIter->rb->nrows) {
    r = pRowBatchIter->nrow;
    pRowBatchIter->counter += 1;
    pRowBatchIter->nrow = (SRow)POINTER_SHIFT(r, rowLen(r));
  }

  return r;
}

// SRowBatchBuilder
SRowBatchBuilder *rowBatchBuilderCreate();
void              rowBatchBuilderDestroy(SRowBatchBuilder *);