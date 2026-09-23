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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef LINUX
#include <unistd.h>
#endif
#ifdef WINDOWS
#include <windows.h>
#endif
#include "taosudf.h"


DLL_EXPORT int32_t udf1_dup_init() { return 0; }

DLL_EXPORT int32_t udf1_dup_destroy() { return 0; }

DLL_EXPORT int32_t udf1_dup(SUdfDataBlock *block, SUdfColumn *resultCol) {
  int32_t code = 0;
  SUdfColumnData *resultData = &resultCol->colData;
  for (int32_t i = 0; i < block->numOfRows; ++i) {
    int j = 0;
    for (; j < block->numOfCols; ++j) {
      if (udfColDataIsNull(block->udfCols[j], i)) {
        code = udfColDataSetNull(resultCol, i);
        if (code != 0) {
          return code;
        }
        break;
      }
    }
    if (j == block->numOfCols) {
      int32_t luckyNum = 2;
      code = udfColDataSet(resultCol, i, (char *)&luckyNum, false);
      if (code != 0) {
        return code;
      }
    }
  }
  // to simulate actual processing delay by udf
#ifdef LINUX
  usleep(1 * 1000);  // usleep takes sleep time in us (1 millionth of a second)
#endif
#ifdef WINDOWS
  Sleep(1);
#endif
  resultData->numOfRows = block->numOfRows;
  return 0;
}

