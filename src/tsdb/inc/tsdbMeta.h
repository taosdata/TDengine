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

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_MAX_TABLE_SCHEMAS 16

typedef struct STable {
  STableId       tableId;
  ETableType     type;
  tstr*          name;  // NOTE: there a flexible string here
  uint64_t       suid;
  struct STable* pSuper;  // super table pointer
  uint8_t        numOfSchemas;
  STSchema*      schema[TSDB_MAX_TABLE_SCHEMAS];
  STSchema*      tagSchema;
  SKVRow         tagVal;
  SSkipList*     pIndex;         // For TSDB_SUPER_TABLE, it is the skiplist index
  void*          eventHandler;   // TODO
  void*          streamHandler;  // TODO
  TSKEY          lastKey;
  SDataRow       lastRow;
  char*          sql;
  void*          cqhandle;
  SRWLatch       latch;  // TODO: implementa latch functions
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
STSchema*  tsdbGetTableSchemaByVersion(STable* pTable, int16_t version);
int        tsdbWLockRepoMeta(STsdbRepo* pRepo);
int        tsdbRLockRepoMeta(STsdbRepo* pRepo);
int        tsdbUnlockRepoMeta(STsdbRepo* pRepo);
void       tsdbRefTable(STable* pTable);
void       tsdbUnRefTable(STable* pTable);
void       tsdbUpdateTableSchema(STsdbRepo* pRepo, STable* pTable, STSchema* pSchema, bool insertAct);

static FORCE_INLINE int tsdbCompareSchemaVersion(const void *key1, const void *key2) {
  if (*(int16_t *)key1 < schemaVersion(*(STSchema **)key2)) {
    return -1;
  } else if (*(int16_t *)key1 > schemaVersion(*(STSchema **)key2)) {
    return 1;
  } else {
    return 0;
  }
}

static FORCE_INLINE STSchema* tsdbGetTableSchemaImpl(STable* pTable, bool lock, bool copy, int16_t version) {
  STable*   pDTable = (TABLE_TYPE(pTable) == TSDB_CHILD_TABLE) ? pTable->pSuper : pTable;
  STSchema* pSchema = NULL;
  STSchema* pTSchema = NULL;

  if (lock) TSDB_RLOCK_TABLE(pDTable);
  if (version < 0) {  // get the latest version of schema
    pTSchema = pDTable->schema[pDTable->numOfSchemas - 1];
  } else {  // get the schema with version
    void* ptr = taosbsearch(&version, pDTable->schema, pDTable->numOfSchemas, sizeof(STSchema*),
                            tsdbCompareSchemaVersion, TD_EQ);
    if (ptr == NULL) {
      terrno = TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION;
      goto _exit;
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
  return tsdbGetTableSchemaImpl(pTable, false, false, -1);
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
  ASSERT(pTable->lastRow == NULL || pTable->lastKey == dataRowKey(pTable->lastRow));
  return pTable->lastKey;
}

#ifdef __cplusplus
}
#endif

#endif /* _TD_TSDB_META_H_ */