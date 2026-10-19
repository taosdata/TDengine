/*
 * Copyright (c) 2023 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "taosudf.h"

DLL_EXPORT int32_t udf2_dup_init() { return 0; }

DLL_EXPORT int32_t udf2_dup_destroy() { return 0; }

DLL_EXPORT int32_t udf2_dup_start(SUdfInterBuf* buf) {
  *(int64_t*)(buf->buf) = 0;
  buf->bufLen = sizeof(double);
  buf->numOfResult = 1;
  return 0;
}

DLL_EXPORT int32_t udf2_dup(SUdfDataBlock* block, SUdfInterBuf* interBuf, SUdfInterBuf* newInterBuf) {
  double sumSquares = 0;
  if (interBuf->numOfResult == 1) {
    sumSquares = *(double*)interBuf->buf;
  }
  int8_t numNotNull = 0;
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    SUdfColumn* col = block->udfCols[i];
    if (!(col->colMeta.type == TSDB_DATA_TYPE_INT || col->colMeta.type == TSDB_DATA_TYPE_DOUBLE)) {
      return TSDB_CODE_UDF_INVALID_INPUT;
    }
  }
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    for (int32_t j = 0; j < block->numOfRows; ++j) {
      SUdfColumn* col = block->udfCols[i];
      if (udfColDataIsNull(col, j)) {
        continue;
      }
      switch (col->colMeta.type) {
        case TSDB_DATA_TYPE_INT: {
          char*   cell = udfColDataGetData(col, j);
          int32_t num = *(int32_t*)cell;
          sumSquares += (double)num * num;
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          char*  cell = udfColDataGetData(col, j);
          double num = *(double*)cell;
          sumSquares += num * num;
          break;
        }
        default:
          break;
      }
      ++numNotNull;
    }
  }

  *(double*)(newInterBuf->buf) = sumSquares;
  newInterBuf->bufLen = sizeof(double);

  if (interBuf->numOfResult == 0 && numNotNull == 0) {
    newInterBuf->numOfResult = 0;
  } else {
    newInterBuf->numOfResult = 1;
  }
  return 0;
}

DLL_EXPORT int32_t udf2_dup_finish(SUdfInterBuf* buf, SUdfInterBuf* resultData) {
  if (buf->numOfResult == 0) {
    resultData->numOfResult = 0;
    return 0;
  }
  double sumSquares = *(double*)(buf->buf);
  *(double*)(resultData->buf) = sqrt(sumSquares) + 100;
  resultData->bufLen = sizeof(double);
  resultData->numOfResult = 1;
  return 0;
}
