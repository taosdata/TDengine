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

#include "taoserror.h"
#include "tarray.h"
#include "tcommon.h"
#include "tsdb.h"

// todo parse the stsrow and set the results
static void keepOneRow(const STSRow* pRow, SSDataBlock* pBlock) {
  int32_t rowIndex = pBlock->info.rows;
  size_t  numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

    // todo extract the value of specified column id from STSRow
    const char* p = NULL;
    colDataAppend(pColInfoData, rowIndex, p, false);
  }

  pBlock->info.rows += 1;
}

int32_t tsdbRetrieveLastRow(void* pVnode, const SArray* pTableIdList, int32_t type, SSDataBlock* pResBlock) {
  if (pVnode == NULL || pTableIdList == NULL || pResBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SVnode* pv = pVnode;
  STSRow* pRow = NULL;
  size_t numOfTables = taosArrayGetSize(pTableIdList);

  // retrieve the only one last row of all tables in the uid list.
  if (type == LASTROW_RETRIEVE_TYPE_SINGLE) {
    int64_t lastKey = INT64_MIN;
    bool    internalResult = false;
    for (int32_t i = 0; i < numOfTables; ++i) {
      STableKeyInfo* pKeyInfo = taosArrayGet(pTableIdList, i);

      int32_t code = tsdbCacheGetLastrow(pv->pTsdb->lruCache, pKeyInfo->uid, pv->pTsdb, &pRow);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      if (pRow == NULL) {
        continue;
      }

      if (pRow->ts > lastKey) {
        // Set result row into the same rowIndex repeatly, so we need to check if the internal result row has already
        // appended or not.
        if (internalResult) {
          pResBlock->info.rows -= 1;
        }

        keepOneRow(pRow, pResBlock);
        internalResult = true;
        lastKey = pRow->ts;
      }
    }
  } else if (type == LASTROW_RETRIEVE_TYPE_ALL) {
    for (int32_t i = 0; i < numOfTables; ++i) {
      STableKeyInfo* pKeyInfo = taosArrayGet(pTableIdList, i);

      int32_t code = tsdbCacheGetLastrow(pv->pTsdb->lruCache, pKeyInfo->uid, pv->pTsdb, &pRow);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      // no data in the table of Uid
      if (pRow == NULL) {
        continue;
      }

      keepOneRow(pRow, pResBlock);
    }
  } else {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}
