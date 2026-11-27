/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taosudf.h"

DLL_EXPORT int32_t bit_and_init() { return 0; }

DLL_EXPORT int32_t bit_and_destroy() { return 0; }

DLL_EXPORT int32_t bit_and(SUdfDataBlock* block, SUdfColumn* resultCol) {
  udfTrace("block:%p, processing begins, rows:%d cols:%d", block, block->numOfRows, block->numOfCols);

  if (block->numOfCols < 2) {
    udfError("block:%p, cols:%d needs to be greater than 2", block, block->numOfCols);
    return TSDB_CODE_UDF_INVALID_INPUT;
  }

  for (int32_t i = 0; i < block->numOfCols; ++i) {
    SUdfColumn* col = block->udfCols[i];
    if (col->colMeta.type != TSDB_DATA_TYPE_INT) {
      udfError("block:%p, col:%d type:%d should be int(%d)", block, i, col->colMeta.type, TSDB_DATA_TYPE_INT);
      return TSDB_CODE_UDF_INVALID_INPUT;
    }
  }

  SUdfColumnData* resultData = &resultCol->colData;

  for (int32_t i = 0; i < block->numOfRows; ++i) {
    if (udfColDataIsNull(block->udfCols[0], i)) {
      udfColDataSetNull(resultCol, i);
      udfTrace("block:%p, row:%d result is null since col:0 is null", block, i);
      continue;
    }

    int32_t result = *(int32_t*)udfColDataGetData(block->udfCols[0], i);
    udfTrace("block:%p, row:%d col:0 data:%d", block, i, result);

    int32_t j = 1;
    for (; j < block->numOfCols; ++j) {
      if (udfColDataIsNull(block->udfCols[j], i)) {
        udfColDataSetNull(resultCol, i);
        udfTrace("block:%p, row:%d result is null since col:%d is null", block, i, j);
        break;
      }

      char* colData = udfColDataGetData(block->udfCols[j], i);
      result &= *(int32_t*)colData;
      udfTrace("block:%p, row:%d col:%d data:%d", block, i, j, *(int32_t*)colData);
    }

    if (j == block->numOfCols) {
      udfColDataSet(resultCol, i, (char*)&result, false);
      udfTrace("block:%p, row:%d result is %d", block, i, result);
    }
  }

  resultData->numOfRows = block->numOfRows;
  udfTrace("block:%p, processing completed", block);

  return TSDB_CODE_SUCCESS;
}
