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

#include "os.h"
#include "taosdef.h"
#include "thash.h"
#include "tmsg.h"
#include "ttypes.h"

#include "function.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tfunctionInt.h"
#include "thistogram.h"
#include "tpercentile.h"
#include "ttszip.h"
#include "tudf.h"

void cleanupResultRowEntry(struct SResultRowEntryInfo* pCell) { pCell->initialized = false; }

int32_t getNumOfResult(SqlFunctionCtx* pCtx, int32_t num, SSDataBlock* pResBlock) {
  int32_t maxRows = 0;

  for (int32_t j = 0; j < num; ++j) {
    SResultRowEntryInfo* pResInfo = GET_RES_INFO(&pCtx[j]);
    if (pResInfo != NULL && maxRows < pResInfo->numOfRes) {
      maxRows = pResInfo->numOfRes;
    }
  }

  blockDataEnsureCapacity(pResBlock, maxRows);
  for (int32_t i = 0; i < num; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pResBlock->pDataBlock, i);

    SResultRowEntryInfo* pResInfo = GET_RES_INFO(&pCtx[i]);
    if (pResInfo->numOfRes == 0) {
      for (int32_t j = 0; j < pResInfo->numOfRes; ++j) {
        colDataAppend(pCol, j, NULL, true);  // TODO add set null data api
      }
    } else {
      for (int32_t j = 0; j < pResInfo->numOfRes; ++j) {
        colDataAppend(pCol, j, GET_ROWCELL_INTERBUF(pResInfo), false);
      }
    }
  }

  pResBlock->info.rows = maxRows;
  return maxRows;
}

bool isRowEntryCompleted(struct SResultRowEntryInfo* pEntry) {
  return pEntry->complete;
}

bool isRowEntryInitialized(struct SResultRowEntryInfo* pEntry) { return pEntry->initialized; }
