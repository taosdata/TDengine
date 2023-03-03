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

#define _DEFAULT_SOURCE
#include "sdb.h"

SSdbRow *sdbAllocRow(int32_t objSize) {
  SSdbRow *pRow = taosMemoryCalloc(1, objSize + sizeof(SSdbRow));
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

#if 1
  mTrace("row:%p, is created, len:%d", pRow->pObj, objSize);
#endif
  return pRow;
}

void *sdbGetRowObj(SSdbRow *pRow) {
  if (pRow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  return pRow->pObj;
}

void sdbFreeRow(SSdb *pSdb, SSdbRow *pRow, bool callFunc) {
  // remove attached object such as trans
  SdbDeleteFp deleteFp = pSdb->deleteFps[pRow->type];
  if (deleteFp != NULL) {
    (*deleteFp)(pSdb, pRow->pObj, callFunc);
  }

  sdbPrintOper(pSdb, pRow, "free");

#if 1
  mTrace("row:%p, is freed", pRow->pObj);
#endif
  taosMemoryFreeClear(pRow);
}
