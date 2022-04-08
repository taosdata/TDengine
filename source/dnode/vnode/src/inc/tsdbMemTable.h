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

#ifndef _TD_TSDB_MEM_TABLE_H_
#define _TD_TSDB_MEM_TABLE_H_

#include "vnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int   rowsInserted;
  int   rowsUpdated;
  int   rowsDeleteSucceed;
  int   rowsDeleteFailed;
  int   nOperations;
  TSKEY keyFirst;
  TSKEY keyLast;
} SMergeInfo;

typedef struct STbData {
  tb_uid_t   uid;
  TSKEY      keyMin;
  TSKEY      keyMax;
  int64_t    nrows;
  SSkipList *pData;
} STbData;

typedef struct STsdbMemTable {
  T_REF_DECLARE()
  SRWLatch       latch;
  TSKEY          keyMin;
  TSKEY          keyMax;
  uint64_t       nRow;
  SMemAllocator *pMA;
  // Container
  SSkipList *pSlIdx;  // SSkiplist<STbData>
  SHashObj * pHashIdx;
} STsdbMemTable;

STsdbMemTable *tsdbNewMemTable(STsdb *pTsdb);
void           tsdbFreeMemTable(STsdb *pTsdb, STsdbMemTable *pMemTable);
int            tsdbMemTableInsert(STsdb *pTsdb, STsdbMemTable *pMemTable, SSubmitReq *pMsg, SSubmitRsp *pRsp);
int tsdbLoadDataFromCache(STable *pTable, SSkipListIterator *pIter, TSKEY maxKey, int maxRowsToRead, SDataCols *pCols,
                          TKEY *filterKeys, int nFilterKeys, bool keepDup, SMergeInfo *pMergeInfo);

static FORCE_INLINE STSRow *tsdbNextIterRow(SSkipListIterator *pIter) {
  if (pIter == NULL) return NULL;

  SSkipListNode *node = tSkipListIterGet(pIter);
  if (node == NULL) return NULL;

  return (STSRow *)SL_GET_NODE_DATA(node);
}

static FORCE_INLINE TSKEY tsdbNextIterKey(SSkipListIterator *pIter) {
  STSRow *row = tsdbNextIterRow(pIter);
  if (row == NULL) return TSDB_DATA_TIMESTAMP_NULL;

  return TD_ROW_KEY(row);
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_TSDB_MEM_TABLE_H_*/