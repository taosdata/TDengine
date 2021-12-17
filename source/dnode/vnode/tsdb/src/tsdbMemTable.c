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

#include "tsdbDef.h"

#if 1
typedef struct STbData {
  TD_SLIST_NODE(STbData);
  SSubmitMsg *pMsg;
} STbData;
#else
typedef struct STbData {
  TD_SLIST_NODE(STbData);
  uint64_t   uid;  // TODO: change here as tb_uid_t
  TSKEY      keyMin;
  TSKEY      keyMax;
  uint64_t   nRows;
  SSkipList *pData;  // Here need a container, may not use the SL
  T_REF_DECLARE()
} STbData;
#endif

struct STsdbMemTable {
  T_REF_DECLARE()
  SRWLatch       latch;
  TSKEY          keyMin;
  TSKEY          keyMax;
  uint64_t       nRow;
  SMemAllocator *pMA;
  // Container
  TD_SLIST(STbData) list;
};

STsdbMemTable *tsdbNewMemTable(SMemAllocatorFactory *pMAF) {
  STsdbMemTable *pMemTable;
  SMemAllocator *pMA;

  pMA = (*pMAF->create)(pMAF);
  ASSERT(pMA != NULL);

  pMemTable = (STsdbMemTable *)TD_MA_MALLOC(pMA, sizeof(*pMemTable));
  if (pMemTable == NULL) {
    (*pMAF->destroy)(pMAF, pMA);
    return NULL;
  }

  T_REF_INIT_VAL(pMemTable, 1);
  taosInitRWLatch(&(pMemTable->latch));
  pMemTable->keyMin = TSKEY_MAX;
  pMemTable->keyMax = TSKEY_MIN;
  pMemTable->nRow = 0;
  pMemTable->pMA = pMA;
  TD_SLIST_INIT(&(pMemTable->list));

  // TODO
  return pMemTable;
}

void tsdbFreeMemTable(SMemAllocatorFactory *pMAF, STsdbMemTable *pMemTable) {
  SMemAllocator *pMA = pMemTable->pMA;

  if (TD_MA_FREE_FUNC(pMA) != NULL) {
    // TODO
    ASSERT(0);
  }

  (*pMAF->destroy)(pMAF, pMA);
}

int tsdbInsertDataToMemTable(STsdbMemTable *pMemTable, SSubmitMsg *pMsg) {
  SMemAllocator *pMA = pMemTable->pMA;
  STbData *      pTbData = (STbData *)TD_MA_MALLOC(pMA, sizeof(*pTbData));
  if (pTbData == NULL) {
    // TODO
  }

  TD_SLIST_PUSH(&(pMemTable->list), pTbData);

  return 0;
}

/* ------------------------ STATIC METHODS ------------------------ */