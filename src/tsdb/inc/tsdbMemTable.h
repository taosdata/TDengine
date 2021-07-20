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

struct STableData {
  uint64_t   uid;
  TSKEY      keyFirst;
  TSKEY      keyLast;
  int64_t    numOfRows;
  SSkipList* pData;
  T_REF_DECLARE()
};

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
int   tsdbTakeMemSnapshot(STsdbRepo* pRepo, SMemSnapshot* pSnapshot, SArray* pATable);
void  tsdbUnTakeMemSnapShot(STsdbRepo* pRepo, SMemSnapshot* pSnapshot);
void* tsdbAllocBytes(STsdbRepo* pRepo, int bytes);
int   tsdbAsyncCommit(STsdbRepo* pRepo);
int   tsdbSyncCommitConfig(STsdbRepo* pRepo);
int   tsdbLoadDataFromCache(STable* pTable, SSkipListIterator* pIter, TSKEY maxKey, int maxRowsToRead, SDataCols* pCols,
                            TKEY* filterKeys, int nFilterKeys, bool keepDup, SMergeInfo* pMergeInfo);
void* tsdbCommitData(STsdbRepo* pRepo);

static FORCE_INLINE SMemRow tsdbNextIterRow(SSkipListIterator* pIter) {
  if (pIter == NULL) return NULL;

  SSkipListNode* node = tSkipListIterGet(pIter);
  if (node == NULL) return NULL;

  return (SMemRow)SL_GET_NODE_DATA(node);
}

static FORCE_INLINE TSKEY tsdbNextIterKey(SSkipListIterator* pIter) {
  SMemRow row = tsdbNextIterRow(pIter);
  if (row == NULL) return TSDB_DATA_TIMESTAMP_NULL;

  return memRowKey(row);
}

static FORCE_INLINE TKEY tsdbNextIterTKey(SSkipListIterator* pIter) {
  SMemRow row = tsdbNextIterRow(pIter);
  if (row == NULL) return TKEY_NULL;

  return memRowTKey(row);
}

#endif /* _TD_TSDB_MEMTABLE_H_ */