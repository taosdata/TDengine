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

#ifndef _TD_TSDB_MEMTABLE_H_
#define _TD_TSDB_MEMTABLE_H_

typedef struct {
  int   rowsInserted;
  int   rowsUpdated;
  int   rowsDeleteSucceed;
  int   rowsDeleteFailed;
  int   nOperations;
  TSKEY keyFirst;
  TSKEY keyLast;
} SMergeInfo;

typedef struct {
  STable *           pTable;
  SSkipListIterator *pIter;
} SCommitIter;

typedef struct {
  uint64_t   uid;
  TSKEY      keyFirst;
  TSKEY      keyLast;
  int64_t    numOfRows;
  SSkipList* pData;
} STableData;

typedef struct {
  T_REF_DECLARE()
  SRWLatch     latch;
  TSKEY        keyFirst;
  TSKEY        keyLast;
  int64_t      numOfRows;
  int32_t      maxTables;
  STableData** tData;
  SList*       actList;
  SList*       extraBuffList;
  SList*       bufBlockList;
  int64_t      pointsAdd;   // TODO
  int64_t      storageAdd;  // TODO
} SMemTable;

enum { TSDB_UPDATE_META, TSDB_DROP_META };

#ifdef WINDOWS
#pragma pack(push ,1) 
typedef struct {
#else
typedef struct __attribute__((packed)){
#endif
  char     act;
  uint64_t uid;
} SActObj;
#ifdef WINDOWS
#pragma pack(pop) 
#endif

typedef struct {
  int  len;
  char cont[];
} SActCont;

int   tsdbRefMemTable(STsdbRepo* pRepo, SMemTable* pMemTable);
int   tsdbUnRefMemTable(STsdbRepo* pRepo, SMemTable* pMemTable);
int   tsdbTakeMemSnapshot(STsdbRepo* pRepo, SMemTable** pMem, SMemTable** pIMem);
void  tsdbUnTakeMemSnapShot(STsdbRepo* pRepo, SMemTable* pMem, SMemTable* pIMem);
void* tsdbAllocBytes(STsdbRepo* pRepo, int bytes);
int   tsdbAsyncCommit(STsdbRepo* pRepo);
int   tsdbLoadDataFromCache(STable* pTable, SSkipListIterator* pIter, TSKEY maxKey, int maxRowsToRead, SDataCols* pCols,
                            TKEY* filterKeys, int nFilterKeys, bool keepDup, SMergeInfo* pMergeInfo);
void* tsdbCommitData(STsdbRepo* pRepo);

static FORCE_INLINE SDataRow tsdbNextIterRow(SSkipListIterator* pIter) {
  if (pIter == NULL) return NULL;

  SSkipListNode* node = tSkipListIterGet(pIter);
  if (node == NULL) return NULL;

  return (SDataRow)SL_GET_NODE_DATA(node);
}

static FORCE_INLINE TSKEY tsdbNextIterKey(SSkipListIterator* pIter) {
  SDataRow row = tsdbNextIterRow(pIter);
  if (row == NULL) return TSDB_DATA_TIMESTAMP_NULL;

  return dataRowKey(row);
}

static FORCE_INLINE TKEY tsdbNextIterTKey(SSkipListIterator* pIter) {
  SDataRow row = tsdbNextIterRow(pIter);
  if (row == NULL) return TKEY_NULL;

  return dataRowTKey(row);
}

#endif /* _TD_TSDB_MEMTABLE_H_ */