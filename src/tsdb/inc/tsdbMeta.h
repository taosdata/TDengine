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

#ifndef _TD_TSDB_META_H_
#define _TD_TSDB_META_H_

#define TSDB_MAX_TABLE_SCHEMAS 16

typedef struct STable {
  STableId       tableId;
  ETableType     type;
  tstr*          name;  // NOTE: there a flexible string here
  uint64_t       suid;
  struct STable* pSuper;  // super table pointer
  SArray*        schema;
  STSchema*      tagSchema;
  SKVRow         tagVal;
  SSkipList*     pIndex;         // For TSDB_SUPER_TABLE, it is the skiplist index
  void*          eventHandler;   // TODO
  void*          streamHandler;  // TODO
  TSKEY          lastKey;
  SMemRow        lastRow;
  char*          sql;
  void*          cqhandle;
  SRWLatch       latch;  // TODO: implementa latch functions

  SDataCol      *lastCols;
  int16_t        maxColNum;
  int16_t        restoreColumnNum;
  bool           hasRestoreLastColumn;
  int            lastColSVersion;
  int16_t        cacheLastConfigVersion;
  T_REF_DECLARE()
} STable;

typedef struct {
  pthread_rwlock_t rwLock;

  int32_t   nTables;
  int32_t   maxTables;
  STable**  tables;
  SList*    superList;
  SHashObj* uidMap;
  int       maxRowBytes;
  int       maxCols;
} STsdbMeta;

#define TSDB_INIT_NTABLES 1024
#define TABLE_TYPE(t) (t)->type
#define TABLE_NAME(t) (t)->name
#define TABLE_CHAR_NAME(t) TABLE_NAME(t)->data
#define TABLE_UID(t) (t)->tableId.uid
#define TABLE_TID(t) (t)->tableId.tid
#define TABLE_SUID(t) (t)->suid
// #define TSDB_META_FILE_MAGIC(m) KVSTORE_MAGIC((m)->pStore)
#define TSDB_RLOCK_TABLE(t) taosRLockLatch(&((t)->latch))
#define TSDB_RUNLOCK_TABLE(t) taosRUnLockLatch(&((t)->latch))
#define TSDB_WLOCK_TABLE(t) taosWLockLatch(&((t)->latch))
#define TSDB_WUNLOCK_TABLE(t) taosWUnLockLatch(&((t)->latch))

STsdbMeta* tsdbNewMeta(STsdbCfg* pCfg);
void       tsdbFreeMeta(STsdbMeta* pMeta);
int        tsdbOpenMeta(STsdbRepo* pRepo);
int        tsdbCloseMeta(STsdbRepo* pRepo);
STable*    tsdbGetTableByUid(STsdbMeta* pMeta, uint64_t uid);
STSchema*  tsdbGetTableSchemaByVersion(STable* pTable, int16_t _version, int8_t rowType);
int        tsdbWLockRepoMeta(STsdbRepo* pRepo);
int        tsdbRLockRepoMeta(STsdbRepo* pRepo);
int        tsdbUnlockRepoMeta(STsdbRepo* pRepo);
void       tsdbRefTable(STable* pTable);
void       tsdbUnRefTable(STable* pTable);
void       tsdbUpdateTableSchema(STsdbRepo* pRepo, STable* pTable, STSchema* pSchema, bool insertAct);
int        tsdbRestoreTable(STsdbRepo* pRepo, void* cont, int contLen);
void       tsdbOrgMeta(STsdbRepo* pRepo);
int        tsdbInitColIdCacheWithSchema(STable* pTable, STSchema* pSchema);
int16_t    tsdbGetLastColumnsIndexByColId(STable* pTable, int16_t colId);
int        tsdbUpdateLastColSchema(STable *pTable, STSchema *pNewSchema);
STSchema*  tsdbGetTableLatestSchema(STable *pTable);
void       tsdbFreeLastColumns(STable* pTable);

static FORCE_INLINE int tsdbCompareSchemaVersion(const void *key1, const void *key2) {
  if (*(int16_t *)key1 < schemaVersion(*(STSchema **)key2)) {
    return -1;
  } else if (*(int16_t *)key1 > schemaVersion(*(STSchema **)key2)) {
    return 1;
  } else {
    return 0;
  }
}

// set rowType to -1 at default if have no relationship with row
static FORCE_INLINE STSchema* tsdbGetTableSchemaImpl(STable* pTable, bool lock, bool copy, int16_t _version,
                                                     int8_t rowType) {
  STable*   pDTable = (pTable->pSuper != NULL) ? pTable->pSuper : pTable;  // for performance purpose
  STSchema* pSchema = NULL;
  STSchema* pTSchema = NULL;

  if (lock) TSDB_RLOCK_TABLE(pDTable);
  if (_version < 0) {  // get the latest version of schema
    pTSchema = *(STSchema **)taosArrayGetLast(pDTable->schema);
  } else {  // get the schema with version
    void* ptr = taosArraySearch(pDTable->schema, &_version, tsdbCompareSchemaVersion, TD_EQ);
    if (ptr == NULL) {
      if (rowType == SMEM_ROW_KV) {
        ptr = taosArrayGetLast(pDTable->schema);
      } else {
        terrno = TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION;
        goto _exit;
      }
    }
    pTSchema = *(STSchema**)ptr;
  }

  ASSERT(pTSchema != NULL);

  if (copy) {
    if ((pSchema = tdDupSchema(pTSchema)) == NULL) terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
  } else {
    pSchema = pTSchema;
  }

_exit:
  if (lock) TSDB_RUNLOCK_TABLE(pDTable);
  return pSchema;
}

static FORCE_INLINE STSchema* tsdbGetTableSchema(STable* pTable) {
  return tsdbGetTableSchemaImpl(pTable, false, false, -1, -1);
}

static FORCE_INLINE STSchema *tsdbGetTableTagSchema(STable *pTable) {
  if (pTable->type == TSDB_CHILD_TABLE) {  // check child table first
    STable *pSuper = pTable->pSuper;
    if (pSuper == NULL) return NULL;
    return pSuper->tagSchema;
  } else if (pTable->type == TSDB_SUPER_TABLE) {
    return pTable->tagSchema;
  } else {
    return NULL;
  }
}

static FORCE_INLINE TSKEY tsdbGetTableLastKeyImpl(STable* pTable) {
  ASSERT((pTable->lastRow == NULL) || (pTable->lastKey == memRowKey(pTable->lastRow)));
  return pTable->lastKey;
}

#endif /* _TD_TSDB_META_H_ */