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
#include "tsdbint.h"
#include "tcompare.h"
#include "tutil.h"

#define TSDB_SUPER_TABLE_SL_LEVEL 5
#define DEFAULT_TAG_INDEX_COLUMN 0

static char *  getTagIndexKey(const void *pData);
static STable *tsdbNewTable();
static STable *tsdbCreateTableFromCfg(STableCfg *pCfg, bool isSuper, STable *pSTable);
static void    tsdbFreeTable(STable *pTable);
static int     tsdbAddTableToMeta(STsdbRepo *pRepo, STable *pTable, bool addIdx, bool lock);
static void    tsdbRemoveTableFromMeta(STsdbRepo *pRepo, STable *pTable, bool rmFromIdx, bool lock);
static int     tsdbAddTableIntoIndex(STsdbMeta *pMeta, STable *pTable, bool refSuper);
static int     tsdbRemoveTableFromIndex(STsdbMeta *pMeta, STable *pTable);
static int     tsdbInitTableCfg(STableCfg *config, ETableType type, uint64_t uid, int32_t tid);
static int     tsdbTableSetSchema(STableCfg *config, STSchema *pSchema, bool dup);
static int     tsdbTableSetName(STableCfg *config, char *name, bool dup);
static int     tsdbTableSetTagSchema(STableCfg *config, STSchema *pSchema, bool dup);
static int     tsdbTableSetSName(STableCfg *config, char *sname, bool dup);
static int     tsdbTableSetSuperUid(STableCfg *config, uint64_t uid);
static int     tsdbTableSetTagValue(STableCfg *config, SKVRow row, bool dup);
static int     tsdbTableSetStreamSql(STableCfg *config, char *sql, bool dup);
static int     tsdbEncodeTableName(void **buf, tstr *name);
static void *  tsdbDecodeTableName(void *buf, tstr **name);
static int     tsdbEncodeTable(void **buf, STable *pTable);
static void *  tsdbDecodeTable(void *buf, STable **pRTable);
static int     tsdbGetTableEncodeSize(int8_t act, STable *pTable);
static void *  tsdbInsertTableAct(STsdbRepo *pRepo, int8_t act, void *buf, STable *pTable);
static int     tsdbRemoveTableFromStore(STsdbRepo *pRepo, STable *pTable);
static int     tsdbRmTableFromMeta(STsdbRepo *pRepo, STable *pTable);
static int     tsdbAdjustMetaTables(STsdbRepo *pRepo, int tid);
static int     tsdbCheckTableTagVal(SKVRow *pKVRow, STSchema *pSchema);
static int     tsdbInsertNewTableAction(STsdbRepo *pRepo, STable* pTable);
static int     tsdbAddSchema(STable *pTable, STSchema *pSchema);
static void    tsdbFreeTableSchema(STable *pTable);

// ------------------ OUTER FUNCTIONS ------------------
int tsdbCreateTable(STsdbRepo *repo, STableCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  STable *   super = NULL;
  STable *   table = NULL;
  bool       newSuper = false;
  bool       superChanged = false;
  int        tid = pCfg->tableId.tid;
  STable *   pTable = NULL;

  if (tid < 1 || tid > TSDB_MAX_TABLES) {
    tsdbError("vgId:%d failed to create table since invalid tid %d", REPO_ID(pRepo), tid);
    terrno = TSDB_CODE_TDB_IVD_CREATE_TABLE_INFO;
    goto _err;
  }

  if (tid < pMeta->maxTables && pMeta->tables[tid] != NULL) {
    if (TABLE_UID(pMeta->tables[tid]) == pCfg->tableId.uid) {
      tsdbError("vgId:%d table %s already exists, tid %d uid %" PRId64, REPO_ID(pRepo),
                TABLE_CHAR_NAME(pMeta->tables[tid]), TABLE_TID(pMeta->tables[tid]), TABLE_UID(pMeta->tables[tid]));
      return 0;
    } else {
      tsdbInfo("vgId:%d table %s at tid %d uid %" PRIu64
                " exists, replace it with new table, this can be not reasonable",
                REPO_ID(pRepo), TABLE_CHAR_NAME(pMeta->tables[tid]), TABLE_TID(pMeta->tables[tid]),
                TABLE_UID(pMeta->tables[tid]));
      tsdbDropTable(pRepo, pMeta->tables[tid]->tableId);
    }
  }

  pTable = tsdbGetTableByUid(pMeta, pCfg->tableId.uid);
  if (pTable != NULL) {
    tsdbError("vgId:%d table %s already exists, tid %d uid %" PRId64, REPO_ID(pRepo), TABLE_CHAR_NAME(pTable),
              TABLE_TID(pTable), TABLE_UID(pTable));
    terrno = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
    goto _err;
  }

  if (pCfg->type == TSDB_CHILD_TABLE) {
    super = tsdbGetTableByUid(pMeta, pCfg->superUid);
    if (super == NULL) {  // super table not exists, try to create it
      newSuper = true;
      super = tsdbCreateTableFromCfg(pCfg, true, NULL);
      if (super == NULL) goto _err;
    } else {
      if (TABLE_TYPE(super) != TSDB_SUPER_TABLE || TABLE_UID(super) != pCfg->superUid) {
        terrno = TSDB_CODE_TDB_IVD_CREATE_TABLE_INFO;
        goto _err;
      }

      if (schemaVersion(pCfg->tagSchema) > schemaVersion(super->tagSchema)) {
        // tag schema out of date, need to update super table tag version
        STSchema *pOldSchema = super->tagSchema;
        TSDB_WLOCK_TABLE(super);
        super->tagSchema = tdDupSchema(pCfg->tagSchema);
        TSDB_WUNLOCK_TABLE(super);
        tdFreeSchema(pOldSchema);

        superChanged = true;
      }
    }
  }

  table = tsdbCreateTableFromCfg(pCfg, false, super);
  if (table == NULL) goto _err;

  // Register to meta
  tsdbWLockRepoMeta(pRepo);
  if (newSuper) {
    if (tsdbAddTableToMeta(pRepo, super, true, false) < 0) {
      super = NULL;
      tsdbUnlockRepoMeta(pRepo);
      goto _err;
    }
  }
  if (tsdbAddTableToMeta(pRepo, table, true, false) < 0) {
    table = NULL;
    tsdbUnlockRepoMeta(pRepo);
    goto _err;
  }
  tsdbUnlockRepoMeta(pRepo);

  // Write to memtable action
  if (newSuper || superChanged) {
    // add insert new super table action
    if (tsdbInsertNewTableAction(pRepo, super) != 0) {
      goto _err;
    }
  }
  // add insert new table action
  if (tsdbInsertNewTableAction(pRepo, table) != 0) {
    goto _err;
  }

  if (tsdbCheckCommit(pRepo) < 0) return -1;

  return 0;

_err:
  if (newSuper) {
    tsdbFreeTable(super);
  }
  tsdbFreeTable(table);
  return -1;
}

int tsdbDropTable(STsdbRepo *repo, STableId tableId) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  uint64_t   uid = tableId.uid;
  int        tid = 0;
  char *     tbname = NULL;

  STable *pTable = tsdbGetTableByUid(pMeta, uid);
  if (pTable == NULL) {
    tsdbError("vgId:%d failed to drop table since table not exists! tid:%d uid %" PRIu64, REPO_ID(pRepo), tableId.tid,
              uid);
    terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
    return -1;
  }

  tsdbDebug("vgId:%d try to drop table %s type %d", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable), TABLE_TYPE(pTable));

  tid = TABLE_TID(pTable);
  tbname = strdup(TABLE_CHAR_NAME(pTable));
  if (tbname == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  // Write to KV store first
  if (tsdbRemoveTableFromStore(pRepo, pTable) < 0) {
    tsdbError("vgId:%d failed to drop table %s since %s", REPO_ID(pRepo), tbname, tstrerror(terrno));
    goto _err;
  }

  // Remove table from Meta
  if (tsdbRmTableFromMeta(pRepo, pTable) < 0) {
    tsdbError("vgId:%d failed to drop table %s since %s", REPO_ID(pRepo), tbname, tstrerror(terrno));
    goto _err;
  }

  tsdbDebug("vgId:%d, table %s is dropped! tid:%d, uid:%" PRId64, pRepo->config.tsdbId, tbname, tid, uid);
  free(tbname);

  if (tsdbCheckCommit(pRepo) < 0) goto _err;

  return 0;

_err:
  tfree(tbname);
  return -1;
}

void *tsdbGetTableTagVal(const void* pTable, int32_t colId, int16_t type) {
  // TODO: this function should be changed also

  STSchema *pSchema = tsdbGetTableTagSchema((STable*) pTable);
  STColumn *pCol = tdGetColOfID(pSchema, colId);
  if (pCol == NULL) {
    return NULL;  // No matched tag volumn
  }

  char *val = NULL;
  if (pCol->type == TSDB_DATA_TYPE_JSON){
    val = ((STable*)pTable)->tagVal;
  }else{
    val = tdGetKVRowValOfCol(((STable*)pTable)->tagVal, colId);
    assert(type == pCol->type);
  }

  return val;
}

char *tsdbGetTableName(void* pTable) {
  // TODO: need to change as thread-safe

  if (pTable == NULL) {
    return NULL;
  } else {
    return (char*) (((STable *)pTable)->name);
  }
}

STableCfg *tsdbCreateTableCfgFromMsg(SMDCreateTableMsg *pMsg) {
  if (pMsg == NULL) return NULL;

  SSchema *pSchema = (SSchema *)pMsg->data;
  int16_t  numOfCols = htons(pMsg->numOfColumns);
  int16_t  numOfTags = htons(pMsg->numOfTags);

  STSchemaBuilder schemaBuilder = {0};

  STableCfg *pCfg = (STableCfg *)calloc(1, sizeof(STableCfg));
  if (pCfg == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  if (tsdbInitTableCfg(pCfg, pMsg->tableType, htobe64(pMsg->uid), htonl(pMsg->tid)) < 0) goto _err;
  if (tdInitTSchemaBuilder(&schemaBuilder, htonl(pMsg->sversion)) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  for (int i = 0; i < numOfCols; i++) {
    if (tdAddColToSchema(&schemaBuilder, pSchema[i].type, htons(pSchema[i].colId), htons(pSchema[i].bytes)) < 0) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      goto _err;
    }
  }
  if (tsdbTableSetSchema(pCfg, tdGetSchemaFromBuilder(&schemaBuilder), false) < 0) goto _err;
  if (tsdbTableSetName(pCfg, pMsg->tableFname, true) < 0) goto _err;

  if (numOfTags > 0) {
    // Decode tag schema
    tdResetTSchemaBuilder(&schemaBuilder, htonl(pMsg->tversion));
    for (int i = numOfCols; i < numOfCols + numOfTags; i++) {
      if (tdAddColToSchema(&schemaBuilder, pSchema[i].type, htons(pSchema[i].colId), htons(pSchema[i].bytes)) < 0) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }
    }
    if (tsdbTableSetTagSchema(pCfg, tdGetSchemaFromBuilder(&schemaBuilder), false) < 0) goto _err;
    if (tsdbTableSetSName(pCfg, pMsg->stableFname, true) < 0) goto _err;
    if (tsdbTableSetSuperUid(pCfg, htobe64(pMsg->superTableUid)) < 0) goto _err;

    int32_t tagDataLen = htonl(pMsg->tagDataLen);
    if (tagDataLen) {
      char *pTagData = pMsg->data + (numOfCols + numOfTags) * sizeof(SSchema);
      tsdbTableSetTagValue(pCfg, pTagData, true);
    }
  }

  if (pMsg->tableType == TSDB_STREAM_TABLE) {
    char *sql = pMsg->data + (numOfCols + numOfTags) * sizeof(SSchema);
    tsdbTableSetStreamSql(pCfg, sql, true);
  }

  tdDestroyTSchemaBuilder(&schemaBuilder);

  return pCfg;

_err:
  tdDestroyTSchemaBuilder(&schemaBuilder);
  tsdbClearTableCfg(pCfg);
  return NULL;
}

static UNUSED_FUNC int32_t colIdCompar(const void* left, const void* right) {
  int16_t colId = *(int16_t*) left;
  STColumn* p2 = (STColumn*) right;

  if (colId == p2->colId) {
    return 0;
  }

  return (colId < p2->colId)? -1:1;
}

int tsdbUpdateTableTagValue(STsdbRepo *repo, SUpdateTableTagValMsg *pMsg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  STSchema * pNewSchema = NULL;

  pMsg->uid = htobe64(pMsg->uid);
  pMsg->tid = htonl(pMsg->tid);
  pMsg->tversion  = htons(pMsg->tversion);
  pMsg->colId     = htons(pMsg->colId);
  pMsg->bytes     = htons(pMsg->bytes);
  pMsg->tagValLen = htonl(pMsg->tagValLen);
  pMsg->numOfTags = htons(pMsg->numOfTags);
  pMsg->schemaLen = htonl(pMsg->schemaLen);
  for (int i = 0; i < pMsg->numOfTags; i++) {
    STColumn *pTCol = (STColumn *)pMsg->data + i;
    pTCol->bytes = htons(pTCol->bytes);
    pTCol->colId = htons(pTCol->colId);
  }

  STable *pTable = tsdbGetTableByUid(pMeta, pMsg->uid);
  if (pTable == NULL || TABLE_TID(pTable) != pMsg->tid) {
    tsdbError("vgId:%d failed to update table tag value since invalid table id %d uid %" PRIu64, REPO_ID(pRepo),
              pMsg->tid, pMsg->uid);
    terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
    return -1;
  }

  if (TABLE_TYPE(pTable) != TSDB_CHILD_TABLE) {
    tsdbError("vgId:%d try to update tag value of a non-child table, invalid action", REPO_ID(pRepo));
    terrno = TSDB_CODE_TDB_INVALID_ACTION;
    return -1;
  }

  if (schemaVersion(pTable->pSuper->tagSchema) > pMsg->tversion) {
    tsdbError(
        "vgId:%d failed to update tag value of table %s since version out of date, client tag version %d server tag "
        "version %d",
        REPO_ID(pRepo), TABLE_CHAR_NAME(pTable), pMsg->tversion, schemaVersion(pTable->pSuper->tagSchema));
    terrno = TSDB_CODE_TDB_TAG_VER_OUT_OF_DATE;
    return -1;
  }

  if (schemaVersion(pTable->pSuper->tagSchema) < pMsg->tversion) {  // tag schema out of data,
    tsdbDebug("vgId:%d need to update tag schema of table %s tid %d uid %" PRIu64
              " since out of date, current version %d new version %d",
              REPO_ID(pRepo), TABLE_CHAR_NAME(pTable), TABLE_TID(pTable), TABLE_UID(pTable),
              schemaVersion(pTable->pSuper->tagSchema), pMsg->tversion);

    STSchemaBuilder schemaBuilder = {0};

    STColumn *pTCol = (STColumn *)pMsg->data;
    ASSERT(pMsg->schemaLen % sizeof(STColumn) == 0 && pTCol[0].colId == colColId(schemaColAt(pTable->pSuper->tagSchema, 0)));
    if (tdInitTSchemaBuilder(&schemaBuilder, pMsg->tversion) < 0) {
      tsdbDebug("vgId:%d failed to update tag schema of table %s tid %d uid %" PRIu64 " since out of memory",
                REPO_ID(pRepo), TABLE_CHAR_NAME(pTable), TABLE_TID(pTable), TABLE_UID(pTable));
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
    for (int i = 0; i < (pMsg->schemaLen / sizeof(STColumn)); i++) {
      if (tdAddColToSchema(&schemaBuilder, pTCol[i].type, pTCol[i].colId, pTCol[i].bytes) < 0) {
        tdDestroyTSchemaBuilder(&schemaBuilder);
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
    }
    pNewSchema = tdGetSchemaFromBuilder(&schemaBuilder);
    if (pNewSchema == NULL) {
      tdDestroyTSchemaBuilder(&schemaBuilder);
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
    tdDestroyTSchemaBuilder(&schemaBuilder);
  }

  // Change in memory
  if (pNewSchema != NULL) { // change super table tag schema
    TSDB_WLOCK_TABLE(pTable->pSuper);
    STSchema *pOldSchema = pTable->pSuper->tagSchema;
    pTable->pSuper->tagSchema = pNewSchema;
    tdFreeSchema(pOldSchema);
    TSDB_WUNLOCK_TABLE(pTable->pSuper);
  }

  bool      isChangeIndexCol = (pMsg->colId == colColId(schemaColAt(pTable->pSuper->tagSchema, 0)))
      || pMsg->type == TSDB_DATA_TYPE_JSON;
  // STColumn *pCol = bsearch(&(pMsg->colId), pMsg->data, pMsg->numOfTags, sizeof(STColumn), colIdCompar);
  // ASSERT(pCol != NULL);

  if (isChangeIndexCol) {
    tsdbWLockRepoMeta(pRepo);
    tsdbRemoveTableFromIndex(pMeta, pTable);
  }
  TSDB_WLOCK_TABLE(pTable);
  if (pMsg->type == TSDB_DATA_TYPE_JSON){
    kvRowFree(pTable->tagVal);
    pTable->tagVal = tdKVRowDup(POINTER_SHIFT(pMsg->data, pMsg->schemaLen));
  }else{
    tdSetKVRowDataOfCol(&(pTable->tagVal), pMsg->colId, pMsg->type, POINTER_SHIFT(pMsg->data, pMsg->schemaLen));
  }
  TSDB_WUNLOCK_TABLE(pTable);
  if (isChangeIndexCol) {
    tsdbAddTableIntoIndex(pMeta, pTable, false);
    tsdbUnlockRepoMeta(pRepo);
  }

  // Update on file
  int tlen1 = (pNewSchema) ? tsdbGetTableEncodeSize(TSDB_UPDATE_META, pTable->pSuper) : 0;
  int tlen2 = tsdbGetTableEncodeSize(TSDB_UPDATE_META, pTable);
  void *buf = tsdbAllocBytes(pRepo, tlen1+tlen2);
  ASSERT(buf != NULL);
  if (pNewSchema) {
    void *pBuf = tsdbInsertTableAct(pRepo, TSDB_UPDATE_META, buf, pTable->pSuper);
    ASSERT(POINTER_DISTANCE(pBuf, buf) == tlen1);
    buf = pBuf;
  }
  tsdbInsertTableAct(pRepo, TSDB_UPDATE_META, buf, pTable);

  if (tsdbCheckCommit(pRepo) < 0) return -1;

  return 0;
}

// ------------------ INTERNAL FUNCTIONS ------------------
static int tsdbInsertNewTableAction(STsdbRepo *pRepo, STable* pTable) {
  int   tlen = 0;
  void *pBuf = NULL;

  tlen = tsdbGetTableEncodeSize(TSDB_UPDATE_META, pTable);
  pBuf = tsdbAllocBytes(pRepo, tlen);
  if (pBuf == NULL) {
    return -1;
  }
  void *tBuf = tsdbInsertTableAct(pRepo, TSDB_UPDATE_META, pBuf, pTable);
  ASSERT(POINTER_DISTANCE(tBuf, pBuf) == tlen);

  return 0;
}

STsdbMeta *tsdbNewMeta(STsdbCfg *pCfg) {
  STsdbMeta *pMeta = (STsdbMeta *)calloc(1, sizeof(*pMeta));
  if (pMeta == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  int code = pthread_rwlock_init(&pMeta->rwLock, NULL);
  if (code != 0) {
    tsdbError("vgId:%d failed to init TSDB meta r/w lock since %s", pCfg->tsdbId, strerror(code));
    terrno = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  pMeta->maxTables = TSDB_INIT_NTABLES + 1;
  pMeta->tables = (STable **)calloc(pMeta->maxTables, sizeof(STable *));
  if (pMeta->tables == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  pMeta->superList = tdListNew(sizeof(STable *));
  if (pMeta->superList == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  pMeta->uidMap = taosHashInit((size_t)(TSDB_INIT_NTABLES * 1.1), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  if (pMeta->uidMap == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  return pMeta;

_err:
  tsdbFreeMeta(pMeta);
  return NULL;
}

void tsdbFreeMeta(STsdbMeta *pMeta) {
  if (pMeta) {
    taosHashCleanup(pMeta->uidMap);
    tdListFree(pMeta->superList);
    tfree(pMeta->tables);
    pthread_rwlock_destroy(&pMeta->rwLock);
    free(pMeta);
  }
}

int tsdbOpenMeta(STsdbRepo *pRepo) {
  return 0;
#if 0
  char *     fname = NULL;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  ASSERT(pMeta != NULL);

  fname = tsdbGetMetaFileName(pRepo->rootDir);
  if (fname == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  // pMeta->pStore = tdOpenKVStore(fname, tsdbRestoreTable, tsdbOrgMeta, (void *)pRepo);
  // if (pMeta->pStore == NULL) {
  //   tsdbError("vgId:%d failed to open TSDB meta while open the kv store since %s", REPO_ID(pRepo), tstrerror(terrno));
  //   goto _err;
  // }

  tsdbDebug("vgId:%d open TSDB meta succeed", REPO_ID(pRepo));
  tfree(fname);
  return 0;

_err:
  tfree(fname);
  return -1;
#endif
}

int tsdbCloseMeta(STsdbRepo *pRepo) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  SListNode *pNode = NULL;
  STable *   pTable = NULL;

  if (pMeta == NULL) return 0;
  // tdCloseKVStore(pMeta->pStore);
  for (int i = 1; i < pMeta->maxTables; i++) {
    tsdbFreeTable(pMeta->tables[i]);
  }

  while ((pNode = tdListPopHead(pMeta->superList)) != NULL) {
    tdListNodeGetData(pMeta->superList, pNode, (void *)(&pTable));
    tsdbFreeTable(pTable);
    listNodeFree(pNode);
  }

  tsdbDebug("vgId:%d TSDB meta is closed", REPO_ID(pRepo));
  return 0;
}

STable *tsdbGetTableByUid(STsdbMeta *pMeta, uint64_t uid) {
  void *ptr = taosHashGet(pMeta->uidMap, (char *)(&uid), sizeof(uid));

  if (ptr == NULL) return NULL;

  return *(STable **)ptr;
}

STSchema *tsdbGetTableSchemaByVersion(STable *pTable, int16_t _version, int8_t rowType) {
  return tsdbGetTableSchemaImpl(pTable, true, false, _version, rowType);
}

int tsdbWLockRepoMeta(STsdbRepo *pRepo) {
  int code = pthread_rwlock_wrlock(&(pRepo->tsdbMeta->rwLock));
  if (code != 0) {
    tsdbError("vgId:%d failed to write lock TSDB meta since %s", REPO_ID(pRepo), strerror(code));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }

  return 0;
}

int tsdbRLockRepoMeta(STsdbRepo *pRepo) {
  int code = pthread_rwlock_rdlock(&(pRepo->tsdbMeta->rwLock));
  if (code != 0) {
    tsdbError("vgId:%d failed to read lock TSDB meta since %s", REPO_ID(pRepo), strerror(code));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }

  return 0;
}

int tsdbUnlockRepoMeta(STsdbRepo *pRepo) {
  int code = pthread_rwlock_unlock(&(pRepo->tsdbMeta->rwLock));
  if (code != 0) {
    tsdbError("vgId:%d failed to unlock TSDB meta since %s", REPO_ID(pRepo), strerror(code));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }

  return 0;
}

void tsdbRefTable(STable *pTable) {
  int32_t ref = T_REF_INC(pTable);
  UNUSED(ref);
  tsdbDebug("ref table %s uid %" PRIu64 " tid:%d, refCount:%d", TABLE_CHAR_NAME(pTable), TABLE_UID(pTable), TABLE_TID(pTable), ref);
}

void tsdbUnRefTable(STable *pTable) {
  uint64_t uid = TABLE_UID(pTable);
  int32_t  tid = TABLE_TID(pTable);
  int32_t  ref = T_REF_DEC(pTable);

  tsdbDebug("unref table, uid:%" PRIu64 " tid:%d, refCount:%d", uid, tid, ref);

  if (ref == 0) {
    if (TABLE_TYPE(pTable) == TSDB_CHILD_TABLE) {
      tsdbUnRefTable(pTable->pSuper);
    }
    tsdbFreeTable(pTable);
  }
}

void tsdbFreeLastColumns(STable* pTable) {
  if (pTable->lastCols == NULL) {
    return;
  }

  for (int i = 0; i < pTable->maxColNum; ++i) {
    if (pTable->lastCols[i].bytes == 0) {
      continue;
    }
    tfree(pTable->lastCols[i].pData);
    pTable->lastCols[i].bytes = 0;
    pTable->lastCols[i].pData = NULL;
  }
  tfree(pTable->lastCols);
  pTable->lastCols = NULL;
  pTable->maxColNum = 0;
  pTable->lastColSVersion = -1;
  pTable->restoreColumnNum = 0;
  pTable->hasRestoreLastColumn = false;
}

int16_t tsdbGetLastColumnsIndexByColId(STable* pTable, int16_t colId) {
  if (pTable->lastCols == NULL) {
    return -1;
  }
  // TODO: use binary search instead
  for (int16_t i = 0; i < pTable->maxColNum; ++i) {
    if (pTable->lastCols[i].colId == colId) {
      return i;
    }
  }

  return -1;
}

int tsdbInitColIdCacheWithSchema(STable* pTable, STSchema* pSchema) {
  TSDB_WLOCK_TABLE(pTable);
  if (pTable->lastCols == NULL) {
    int16_t numOfColumn = pSchema->numOfCols;

    pTable->lastCols = (SDataCol *)malloc(numOfColumn * sizeof(SDataCol));
    if (pTable->lastCols == NULL) {
      TSDB_WUNLOCK_TABLE(pTable);
      return -1;
    }

    for (int16_t i = 0; i < numOfColumn; ++i) {
      STColumn *pCol = schemaColAt(pSchema, i);
      SDataCol *pDataCol = &(pTable->lastCols[i]);
      pDataCol->bytes = 0;
      pDataCol->pData = NULL;
      pDataCol->colId = pCol->colId;
    }

    pTable->lastColSVersion = schemaVersion(pSchema);
    pTable->maxColNum = numOfColumn;
    pTable->restoreColumnNum = 0;
    pTable->hasRestoreLastColumn = false;
  }
  TSDB_WUNLOCK_TABLE(pTable);
  return 0;
}

STSchema* tsdbGetTableLatestSchema(STable *pTable) {
  return tsdbGetTableSchemaByVersion(pTable, -1, -1);
}

int tsdbUpdateLastColSchema(STable *pTable, STSchema *pNewSchema) {
  if (pTable->lastColSVersion == schemaVersion(pNewSchema)) {
    return 0;
  }
  
  tsdbDebug("tsdbUpdateLastColSchema:%s,%d->%d", pTable->name->data, pTable->lastColSVersion, schemaVersion(pNewSchema));
  
  int16_t numOfCols = pNewSchema->numOfCols;
  SDataCol *lastCols = (SDataCol*)malloc(numOfCols * sizeof(SDataCol));
  if (lastCols == NULL) {
    return -1;
  }

  TSDB_WLOCK_TABLE(pTable);

  for (int16_t i = 0; i < numOfCols; ++i) {
    STColumn *pCol = schemaColAt(pNewSchema, i);
    int16_t idx = tsdbGetLastColumnsIndexByColId(pTable, pCol->colId);

    SDataCol* pDataCol = &(lastCols[i]);
    if (idx != -1) {
      // move col data to new last column array
      SDataCol* pOldDataCol = &(pTable->lastCols[idx]);
      memcpy(pDataCol, pOldDataCol, sizeof(SDataCol));
    } else {
      // init new colid data
      pDataCol->colId = pCol->colId;
      pDataCol->bytes = 0;
      pDataCol->pData = NULL;
    }
  }

  SDataCol *oldLastCols = pTable->lastCols;
  int16_t oldLastColNum = pTable->maxColNum;

  pTable->lastColSVersion = schemaVersion(pNewSchema);
  pTable->lastCols = lastCols;
  pTable->maxColNum = numOfCols;

  if (oldLastCols == NULL) {
    TSDB_WUNLOCK_TABLE(pTable);
    return 0;
  }

  // free old schema last column datas
  for (int16_t i = 0; i < oldLastColNum; ++i) {
    SDataCol* pDataCol = &(oldLastCols[i]);
    if (pDataCol->bytes == 0) {
      continue;
    }
    int16_t idx = tsdbGetLastColumnsIndexByColId(pTable, pDataCol->colId);
    if (idx != -1) {
      continue;
    }

    // free not exist column data
    tfree(pDataCol->pData);
  }
  TSDB_WUNLOCK_TABLE(pTable);
  tfree(oldLastCols);

  return 0;
}

void tsdbUpdateTableSchema(STsdbRepo *pRepo, STable *pTable, STSchema *pSchema, bool insertAct) {
  ASSERT(TABLE_TYPE(pTable) != TSDB_STREAM_TABLE && TABLE_TYPE(pTable) != TSDB_SUPER_TABLE);
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  STable *pCTable = (TABLE_TYPE(pTable) == TSDB_CHILD_TABLE) ? pTable->pSuper : pTable;
  ASSERT(schemaVersion(pSchema) > schemaVersion(*(STSchema **)taosArrayGetLast(pCTable->schema)));

  TSDB_WLOCK_TABLE(pCTable);
  tsdbAddSchema(pCTable, pSchema);

  if (schemaNCols(pSchema) > pMeta->maxCols) pMeta->maxCols = schemaNCols(pSchema);
  if (schemaTLen(pSchema) > pMeta->maxRowBytes) pMeta->maxRowBytes = schemaTLen(pSchema);
  TSDB_WUNLOCK_TABLE(pCTable);

  if (insertAct) {
    if (tsdbInsertNewTableAction(pRepo, pCTable) != 0) {
      tsdbError("vgId:%d table %s tid %d uid %" PRIu64 " tsdbInsertNewTableAction fail", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable),
            TABLE_TID(pTable), TABLE_UID(pTable));
    }
  }
}

int tsdbRestoreTable(STsdbRepo *pRepo, void *cont, int contLen) {
  STable *pTable = NULL;

  if (!taosCheckChecksumWhole((uint8_t *)cont, contLen)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  tsdbDecodeTable(cont, &pTable);

  if (tsdbAddTableToMeta(pRepo, pTable, false, false) < 0) {
    tsdbFreeTable(pTable);
    return -1;
  }

  tsdbTrace("vgId:%d table %s tid %d uid %" PRIu64 " is restored from file", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable),
            TABLE_TID(pTable), TABLE_UID(pTable));
  return 0;
}

void tsdbOrgMeta(STsdbRepo *pRepo) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  for (int i = 1; i < pMeta->maxTables; i++) {
    STable *pTable = pMeta->tables[i];
    if (pTable != NULL && pTable->type == TSDB_CHILD_TABLE) {
      tsdbAddTableIntoIndex(pMeta, pTable, true);
    }
  }
}

// ------------------ LOCAL FUNCTIONS ------------------
static char *getTagIndexKey(const void *pData) {
  STable *pTable = (STable *)pData;

  STSchema *pSchema = tsdbGetTableTagSchema(pTable);
  STColumn *pCol = schemaColAt(pSchema, DEFAULT_TAG_INDEX_COLUMN);
  void *    res = tdGetKVRowValOfCol(pTable->tagVal, pCol->colId);
  if (res == NULL) {
    // treat the column as NULL if we cannot find it
    res = (char*)getNullValue(pCol->type);
  }
  return res;
}

static STable *tsdbNewTable() {
  STable *pTable = (STable *)calloc(1, sizeof(*pTable));
  if (pTable == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  pTable->lastKey = TSKEY_INITIAL_VAL;

  pTable->lastCols = NULL;
  pTable->restoreColumnNum = 0;
  pTable->cacheLastConfigVersion = 0;
  pTable->maxColNum = 0;
  pTable->hasRestoreLastColumn = false;
  pTable->lastColSVersion = -1;
  return pTable;
}

static STable *tsdbCreateTableFromCfg(STableCfg *pCfg, bool isSuper, STable *pSTable) {
  STable *pTable = NULL;
  size_t  tsize = 0;

  pTable = tsdbNewTable();
  if (pTable == NULL) goto _err;

  if (isSuper) {
    pTable->type = TSDB_SUPER_TABLE;
    tsize = strnlen(pCfg->sname, TSDB_TABLE_NAME_LEN - 1);
    pTable->name = calloc(1, tsize + VARSTR_HEADER_SIZE + 1);
    if (pTable->name == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      goto _err;
    }
    STR_WITH_SIZE_TO_VARSTR(pTable->name, pCfg->sname, (VarDataLenT)tsize);
    TABLE_UID(pTable) = pCfg->superUid;
    TABLE_TID(pTable) = -1;
    TABLE_SUID(pTable) = -1;
    pTable->pSuper = NULL;
    if (tsdbAddSchema(pTable, tdDupSchema(pCfg->schema)) < 0) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      goto _err;
    }
    pTable->tagSchema = tdDupSchema(pCfg->tagSchema);
    if (pTable->tagSchema == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      goto _err;
    }
    pTable->tagVal = NULL;
    STColumn *pCol = schemaColAt(pTable->tagSchema, DEFAULT_TAG_INDEX_COLUMN);
    if(pCol->type == TSDB_DATA_TYPE_JSON){
      assert(pTable->tagSchema->numOfCols == 1);
      pTable->jsonKeyMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
      if (pTable->jsonKeyMap == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        tsdbFreeTable(pTable);
        return NULL;
      }
      taosHashSetFreeFp(pTable->jsonKeyMap, taosArrayDestroyForHash);
    }else{
      pTable->pIndex = tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, colType(pCol), (uint8_t)(colBytes(pCol)), NULL,
                                       SL_ALLOW_DUP_KEY, getTagIndexKey);
      if (pTable->pIndex == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }
    }
  } else {
    pTable->type = pCfg->type;
    tsize = strnlen(pCfg->name, TSDB_TABLE_NAME_LEN - 1);
    pTable->name = calloc(1, tsize + VARSTR_HEADER_SIZE + 1);
    if (pTable->name == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      goto _err;
    }
    STR_WITH_SIZE_TO_VARSTR(pTable->name, pCfg->name, (VarDataLenT)tsize);
    TABLE_UID(pTable) = pCfg->tableId.uid;
    TABLE_TID(pTable) = pCfg->tableId.tid;

    if (pCfg->type == TSDB_CHILD_TABLE) {
      TABLE_SUID(pTable) = pCfg->superUid;
      if (tsdbCheckTableTagVal(pCfg->tagValues, pSTable->tagSchema) < 0) {
        goto _err;
      }
      pTable->tagVal = tdKVRowDup(pCfg->tagValues);
      if (pTable->tagVal == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }
    } else {
      TABLE_SUID(pTable) = -1;
      if (tsdbAddSchema(pTable, tdDupSchema(pCfg->schema)) < 0) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }

      if (TABLE_TYPE(pTable) == TSDB_STREAM_TABLE) {
        pTable->sql = strdup(pCfg->sql);
        if (pTable->sql == NULL) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          goto _err;
        }
      }
    }
  }

  T_REF_INC(pTable);

  tsdbDebug("table %s tid %d uid %" PRIu64 " is created", TABLE_CHAR_NAME(pTable), TABLE_TID(pTable),
            TABLE_UID(pTable));

  return pTable;

_err:
  tsdbFreeTable(pTable);
  return NULL;
}

static void tsdbFreeTable(STable *pTable) {
  if (pTable) {
    if (pTable->name != NULL)
      tsdbTrace("table %s tid %d uid %" PRIu64 " is freed", TABLE_CHAR_NAME(pTable), TABLE_TID(pTable),
                TABLE_UID(pTable));
    tfree(TABLE_NAME(pTable));
    if (TABLE_TYPE(pTable) != TSDB_CHILD_TABLE) {
      tsdbFreeTableSchema(pTable);

      if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
        tdFreeSchema(pTable->tagSchema);
      }
    }

    kvRowFree(pTable->tagVal);

    tSkipListDestroy(pTable->pIndex);
    taosHashCleanup(pTable->jsonKeyMap);
    taosTZfree(pTable->lastRow);    
    tfree(pTable->sql);

    tsdbFreeLastColumns(pTable);
    free(pTable);
  }
}

static int tsdbAddTableToMeta(STsdbRepo *pRepo, STable *pTable, bool addIdx, bool lock) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  if (lock && tsdbWLockRepoMeta(pRepo) < 0) {
    tsdbError("vgId:%d failed to add table %s to meta since %s", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable),
              tstrerror(terrno));
    return -1;
  }

  if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
    if (tdListAppend(pMeta->superList, (void *)(&pTable)) < 0) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      tsdbError("vgId:%d failed to add table %s to meta since %s", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable),
                tstrerror(terrno));
      goto _err;
    }
  } else {
    if (TABLE_TID(pTable) >= pMeta->maxTables) {
      if (tsdbAdjustMetaTables(pRepo, TABLE_TID(pTable)) < 0) goto _err;
    }
    if (TABLE_TYPE(pTable) == TSDB_CHILD_TABLE && addIdx) {  // add STABLE to the index
      if (tsdbAddTableIntoIndex(pMeta, pTable, true) < 0) {
        tsdbDebug("vgId:%d failed to add table %s to meta while add table to index since %s", REPO_ID(pRepo),
                  TABLE_CHAR_NAME(pTable), tstrerror(terrno));
        goto _err;
      }
    }
    ASSERT(TABLE_TID(pTable) < pMeta->maxTables);
    pMeta->tables[TABLE_TID(pTable)] = pTable;
    pMeta->nTables++;
  }

  if (taosHashPut(pMeta->uidMap, (char *)(&pTable->tableId.uid), sizeof(pTable->tableId.uid), (void *)(&pTable),
                  sizeof(pTable)) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbError("vgId:%d failed to add table %s to meta while put into uid map since %s", REPO_ID(pRepo),
              TABLE_CHAR_NAME(pTable), tstrerror(terrno));
    goto _err;
  }

  if (TABLE_TYPE(pTable) != TSDB_CHILD_TABLE) {
    STSchema *pSchema = tsdbGetTableSchemaImpl(pTable, false, false, -1, -1);
    if (schemaNCols(pSchema) > pMeta->maxCols) pMeta->maxCols = schemaNCols(pSchema);
    if (schemaTLen(pSchema) > pMeta->maxRowBytes) pMeta->maxRowBytes = schemaTLen(pSchema);
  }

  if (lock && tsdbUnlockRepoMeta(pRepo) < 0) return -1;
  if (TABLE_TYPE(pTable) == TSDB_STREAM_TABLE && addIdx) {
    pTable->cqhandle = (*pRepo->appH.cqCreateFunc)(pRepo->appH.cqH, TABLE_UID(pTable), TABLE_TID(pTable), TABLE_NAME(pTable)->data, pTable->sql,
                                                   tsdbGetTableSchemaImpl(pTable, false, false, -1, -1), 1);
  }

  tsdbDebug("vgId:%d table %s tid %d uid %" PRIu64 " is added to meta", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable),
            TABLE_TID(pTable), TABLE_UID(pTable));
  return 0;

_err:
  tsdbRemoveTableFromMeta(pRepo, pTable, false, false);
  if (lock) tsdbUnlockRepoMeta(pRepo);
  return -1;
}

static void tsdbRemoveTableFromMeta(STsdbRepo *pRepo, STable *pTable, bool rmFromIdx, bool lock) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  SListIter  lIter = {0};
  SListNode *pNode = NULL;
  STable *   tTable = NULL;

  STSchema *pSchema = tsdbGetTableSchemaImpl(pTable, false, false, -1, -1);
  int       maxCols = schemaNCols(pSchema);
  int       maxRowBytes = schemaTLen(pSchema);

  if (lock) tsdbWLockRepoMeta(pRepo);

  if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
    tdListInitIter(pMeta->superList, &lIter, TD_LIST_BACKWARD);

    while ((pNode = tdListNext(&lIter)) != NULL) {
      tdListNodeGetData(pMeta->superList, pNode, (void *)(&tTable));
      if (pTable == tTable) {
        tdListPopNode(pMeta->superList, pNode);
        free(pNode);
        break;
      }
    }
  } else {
    pMeta->tables[pTable->tableId.tid] = NULL;
    if (TABLE_TYPE(pTable) == TSDB_CHILD_TABLE && rmFromIdx) {
      tsdbRemoveTableFromIndex(pMeta, pTable);
    }

    pMeta->nTables--;
  }

  taosHashRemove(pMeta->uidMap, (char *)(&(TABLE_UID(pTable))), sizeof(TABLE_UID(pTable)));

  if (maxCols == pMeta->maxCols || maxRowBytes == pMeta->maxRowBytes) {
    maxCols = 0;
    maxRowBytes = 0;
    for (int i = 0; i < pMeta->maxTables; i++) {
      STable *_pTable = pMeta->tables[i];
      if (_pTable != NULL) {
        pSchema = tsdbGetTableSchemaImpl(_pTable, false, false, -1, -1);
        maxCols = MAX(maxCols, schemaNCols(pSchema));
        maxRowBytes = MAX(maxRowBytes, schemaTLen(pSchema));
      }
    }
  }
  pMeta->maxCols = maxCols;
  pMeta->maxRowBytes = maxRowBytes;

  if (lock) tsdbUnlockRepoMeta(pRepo);
  tsdbDebug("vgId:%d table %s uid %" PRIu64 " is removed from meta", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable), TABLE_UID(pTable));
  tsdbUnRefTable(pTable);
}

void* tsdbGetJsonTagValue(STable* pTable, char* key, int32_t keyLen, int16_t* retColId){
  assert(TABLE_TYPE(pTable) == TSDB_CHILD_TABLE);
  STable* superTable= pTable->pSuper;
  SArray** data = (SArray**)taosHashGet(superTable->jsonKeyMap, key, keyLen);
  if(data == NULL) return NULL;
  JsonMapValue jmvalue = {pTable, 0};
  JsonMapValue* p = taosArraySearch(*data, &jmvalue, tsdbCompareJsonMapValue, TD_EQ);
  if (p == NULL) return NULL;
  int16_t colId = p->colId + 1;
  if(retColId) *retColId = p->colId;
  return tdGetKVRowValOfCol(pTable->tagVal, colId);
}

int tsdbCompareJsonMapValue(const void* a, const void* b) {
  const JsonMapValue* x = (const JsonMapValue*)a;
  const JsonMapValue* y = (const JsonMapValue*)b;
  if (x->table > y->table) return 1;
  if (x->table < y->table) return -1;
  return 0;
}

static int tsdbAddTableIntoIndex(STsdbMeta *pMeta, STable *pTable, bool refSuper) {
  ASSERT(pTable->type == TSDB_CHILD_TABLE && pTable != NULL);
  STable *pSTable = tsdbGetTableByUid(pMeta, TABLE_SUID(pTable));
  ASSERT(pSTable != NULL);

  pTable->pSuper = pSTable;
  if (refSuper) T_REF_INC(pSTable);

  if(pSTable->tagSchema->columns[0].type == TSDB_DATA_TYPE_JSON){
    ASSERT(pSTable->tagSchema->numOfCols == 1);
    int16_t nCols = kvRowNCols(pTable->tagVal);
    ASSERT(nCols%2 == 1);
    // check first
    for (int j = 0; j < nCols; ++j) {
      if (j != 0 && j % 2 == 0) continue;  // jump value
      SColIdx *pColIdx = kvRowColIdxAt(pTable->tagVal, j);
      void    *val = (kvRowColVal(pTable->tagVal, pColIdx));
      if (j == 0) {  // json value is the first
        int8_t jsonPlaceHolder = *(int8_t *)val;
        ASSERT(jsonPlaceHolder == TSDB_DATA_JSON_PLACEHOLDER);
        continue;
      }
      if (j == 1) {
        uint32_t jsonNULL = *(uint32_t *)(varDataVal(val));
        ASSERT(jsonNULL == TSDB_DATA_JSON_NULL);
      }

      // then insert
      char keyMd5[TSDB_MAX_JSON_KEY_MD5_LEN] = {0};
      jsonKeyMd5(varDataVal(val), varDataLen(val), keyMd5);
      SArray  *tablistNew = NULL;
      SArray **tablist = (SArray **)taosHashGet(pSTable->jsonKeyMap, keyMd5, TSDB_MAX_JSON_KEY_MD5_LEN);
      if (tablist == NULL) {
        tablistNew = taosArrayInit(8, sizeof(JsonMapValue));
        if (tablistNew == NULL) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          tsdbError("out of memory when alloc json tag array");
          return -1;
        }
        if (taosHashPut(pSTable->jsonKeyMap, keyMd5, TSDB_MAX_JSON_KEY_MD5_LEN, &tablistNew, sizeof(void *)) < 0) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          tsdbError("out of memory when put json tag array");
          return -1;
        }
      } else {
        tablistNew = *tablist;
      }

      JsonMapValue jmvalue = {pTable, pColIdx->colId};
      void* p = taosArraySearch(tablistNew, &jmvalue, tsdbCompareJsonMapValue, TD_EQ);
      if (p == NULL) {
        p = taosArraySearch(tablistNew, &jmvalue, tsdbCompareJsonMapValue, TD_GE);
        if(p == NULL){
          taosArrayPush(tablistNew, &jmvalue);
        }else{
          taosArrayInsert(tablistNew, TARRAY_ELEM_IDX(tablistNew, p), &jmvalue);
        }
      }else{
        tsdbError("insert dumplicate");
      }
    }
  }else{
    tSkipListPut(pSTable->pIndex, (void *)pTable);
  }

  return 0;
}

static int tsdbRemoveTableFromIndex(STsdbMeta *pMeta, STable *pTable) {
  ASSERT(pTable->type == TSDB_CHILD_TABLE && pTable != NULL);

  STable *pSTable = pTable->pSuper;
  ASSERT(pSTable != NULL);

  if(pSTable->tagSchema->columns[0].type == TSDB_DATA_TYPE_JSON){
    ASSERT(pSTable->tagSchema->numOfCols == 1);
    int16_t nCols = kvRowNCols(pTable->tagVal);
    ASSERT(nCols%2 == 1);
    for (int j = 0; j < nCols; ++j) {
      if (j != 0 && j%2 == 0) continue; // jump value
      SColIdx * pColIdx = kvRowColIdxAt(pTable->tagVal, j);
      void* val = (kvRowColVal(pTable->tagVal, pColIdx));
      if (j == 0){        // json value is the first
        int8_t jsonPlaceHolder = *(int8_t*)val;
        ASSERT(jsonPlaceHolder == TSDB_DATA_JSON_PLACEHOLDER);
        continue;
      }
      if (j == 1){
        uint32_t jsonNULL = *(uint32_t*)(varDataVal(val));
        ASSERT(jsonNULL == TSDB_DATA_JSON_NULL);
      }

      char keyMd5[TSDB_MAX_JSON_KEY_MD5_LEN] = {0};
      jsonKeyMd5(varDataVal(val), varDataLen(val), keyMd5);
      SArray** tablist = (SArray **)taosHashGet(pSTable->jsonKeyMap, keyMd5, TSDB_MAX_JSON_KEY_MD5_LEN);
      if(tablist == NULL) {
        tsdbError("json tag no key error,%d", j);
        continue;
      }

      JsonMapValue jmvalue = {pTable, pColIdx->colId};
      void* p = taosArraySearch(*tablist, &jmvalue, tsdbCompareJsonMapValue, TD_EQ);
      if (p == NULL) {
        tsdbError("json tag no tableid error,%d", j);
        continue;
      }
      taosArrayRemove(*tablist, TARRAY_ELEM_IDX(*tablist, p));
    }
  }else {
    char *  key = getTagIndexKey(pTable);
    SArray *res = tSkipListGet(pSTable->pIndex, key);

    size_t size = taosArrayGetSize(res);
    ASSERT(size > 0);

    for (int32_t i = 0; i < size; ++i) {
      SSkipListNode *pNode = taosArrayGetP(res, i);

      // STableIndexElem* pElem = (STableIndexElem*) SL_GET_NODE_DATA(pNode);
      if ((STable *)SL_GET_NODE_DATA(pNode) == pTable) {  // this is the exact what we need
        tSkipListRemoveNode(pSTable->pIndex, pNode);
      }
    }

    taosArrayDestroy(&res);
  }
  return 0;
}

static int tsdbInitTableCfg(STableCfg *config, ETableType type, uint64_t uid, int32_t tid) {
  if (type != TSDB_CHILD_TABLE && type != TSDB_NORMAL_TABLE && type != TSDB_STREAM_TABLE) {
    terrno = TSDB_CODE_TDB_INVALID_TABLE_TYPE;
    return -1;
  }

  memset((void *)config, 0, sizeof(*config));

  config->type = type;
  config->superUid = TSDB_INVALID_SUPER_TABLE_ID;
  config->tableId.uid = uid;
  config->tableId.tid = tid;
  return 0;
}

static int tsdbTableSetSchema(STableCfg *config, STSchema *pSchema, bool dup) {
  if (dup) {
    config->schema = tdDupSchema(pSchema);
    if (config->schema == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  } else {
    config->schema = pSchema;
  }
  return 0;
}

static int tsdbTableSetName(STableCfg *config, char *name, bool dup) {
  if (dup) {
    config->name = strdup(name);
    if (config->name == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  } else {
    config->name = name;
  }

  return 0;
}

static int tsdbTableSetTagSchema(STableCfg *config, STSchema *pSchema, bool dup) {
  if (config->type != TSDB_CHILD_TABLE) {
    terrno = TSDB_CODE_TDB_INVALID_CREATE_TB_MSG;
    return -1;
  }

  if (dup) {
    config->tagSchema = tdDupSchema(pSchema);
    if (config->tagSchema == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  } else {
    config->tagSchema = pSchema;
  }
  return 0;
}

static int tsdbTableSetSName(STableCfg *config, char *sname, bool dup) {
  if (config->type != TSDB_CHILD_TABLE) {
    terrno = TSDB_CODE_TDB_INVALID_CREATE_TB_MSG;
    return -1;
  }

  if (dup) {
    config->sname = strdup(sname);
    if (config->sname == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  } else {
    config->sname = sname;
  }
  return 0;
}

static int tsdbTableSetSuperUid(STableCfg *config, uint64_t uid) {
  if (config->type != TSDB_CHILD_TABLE || uid == TSDB_INVALID_SUPER_TABLE_ID) {
    terrno = TSDB_CODE_TDB_INVALID_CREATE_TB_MSG;
    return -1;
  }

  config->superUid = uid;
  return 0;
}

static int tsdbTableSetTagValue(STableCfg *config, SKVRow row, bool dup) {
  if (config->type != TSDB_CHILD_TABLE) {
    terrno = TSDB_CODE_TDB_INVALID_CREATE_TB_MSG;
    return -1;
  }

  if (dup) {
    config->tagValues = tdKVRowDup(row);
    if (config->tagValues == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  } else {
    config->tagValues = row;
  }

  return 0;
}

static int tsdbTableSetStreamSql(STableCfg *config, char *sql, bool dup) {
  if (config->type != TSDB_STREAM_TABLE) {
    terrno = TSDB_CODE_TDB_INVALID_CREATE_TB_MSG;
    return -1;
  }

  if (dup) {
    config->sql = strdup(sql);
    if (config->sql == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  } else {
    config->sql = sql;
  }

  return 0;
}

void tsdbClearTableCfg(STableCfg *config) {
  if (config) {
    if (config->schema) tdFreeSchema(config->schema);
    if (config->tagSchema) tdFreeSchema(config->tagSchema);
    if (config->tagValues) kvRowFree(config->tagValues);
    tfree(config->name);
    tfree(config->sname);
    tfree(config->sql);
    free(config);
  }
}

static int tsdbEncodeTableName(void **buf, tstr *name) {
  int tlen = 0;

  tlen += taosEncodeFixedI16(buf, name->len);
  if (buf != NULL) {
    memcpy(*buf, name->data, name->len);
    *buf = POINTER_SHIFT(*buf, name->len);
  }
  tlen += name->len;

  return tlen;
}

static void *tsdbDecodeTableName(void *buf, tstr **name) {
  VarDataLenT len = 0;

  buf = taosDecodeFixedI16(buf, &len);
  *name = calloc(1, sizeof(tstr) + len + 1);
  if (*name == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }
  (*name)->len = len;
  memcpy((*name)->data, buf, len);

  buf = POINTER_SHIFT(buf, len);
  return buf;
}

static int tsdbEncodeTable(void **buf, STable *pTable) {
  ASSERT(pTable != NULL);
  int tlen = 0;

  tlen += taosEncodeFixedU8(buf, pTable->type);
  tlen += tsdbEncodeTableName(buf, pTable->name);
  tlen += taosEncodeFixedU64(buf, TABLE_UID(pTable));
  tlen += taosEncodeFixedI32(buf, TABLE_TID(pTable));

  if (TABLE_TYPE(pTable) == TSDB_CHILD_TABLE) {
    tlen += taosEncodeFixedU64(buf, TABLE_SUID(pTable));
    tlen += tdEncodeKVRow(buf, pTable->tagVal);
  } else {
    uint32_t arraySize = (uint32_t)taosArrayGetSize(pTable->schema); 
    if(arraySize > UINT8_MAX) {
      tlen += taosEncodeFixedU8(buf, 0);
      tlen += taosEncodeFixedU32(buf, arraySize);
    } else {
      tlen += taosEncodeFixedU8(buf, (uint8_t)arraySize);
    }
    for (uint32_t i = 0; i < arraySize; i++) {
      STSchema *pSchema = taosArrayGetP(pTable->schema, i);
      tlen += tdEncodeSchema(buf, pSchema);
    }

    if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
      tlen += tdEncodeSchema(buf, pTable->tagSchema);
    }

    if (TABLE_TYPE(pTable) == TSDB_STREAM_TABLE) {
      tlen += taosEncodeString(buf, pTable->sql);
    }
  }

  return tlen;
}

static void *tsdbDecodeTable(void *buf, STable **pRTable) {
  STable *pTable = tsdbNewTable();
  if (pTable == NULL) return NULL;

  uint8_t type = 0;

  buf = taosDecodeFixedU8(buf, &type);
  pTable->type = type;
  buf = tsdbDecodeTableName(buf, &(pTable->name));
  buf = taosDecodeFixedU64(buf, &TABLE_UID(pTable));
  buf = taosDecodeFixedI32(buf, &TABLE_TID(pTable));

  if (TABLE_TYPE(pTable) == TSDB_CHILD_TABLE) {
    buf = taosDecodeFixedU64(buf, &TABLE_SUID(pTable));
    buf = tdDecodeKVRow(buf, &(pTable->tagVal));
  } else {
    uint32_t nSchemas = 0;
    buf = taosDecodeFixedU8(buf, (uint8_t *)&nSchemas);
    if(nSchemas == 0) {
      buf = taosDecodeFixedU32(buf, &nSchemas);
    }
    for (int i = 0; i < nSchemas; i++) {
      STSchema *pSchema;
      buf = tdDecodeSchema(buf, &pSchema);
      tsdbAddSchema(pTable, pSchema);
    }

    if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
      buf = tdDecodeSchema(buf, &(pTable->tagSchema));
      STColumn *pCol = schemaColAt(pTable->tagSchema, DEFAULT_TAG_INDEX_COLUMN);
      if(pCol->type == TSDB_DATA_TYPE_JSON){
        assert(pTable->tagSchema->numOfCols == 1);
        pTable->jsonKeyMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
        if (pTable->jsonKeyMap == NULL) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          tsdbFreeTable(pTable);
          return NULL;
        }
	taosHashSetFreeFp(pTable->jsonKeyMap, taosArrayDestroyForHash);
      }else{
        pTable->pIndex = tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, colType(pCol), (uint8_t)(colBytes(pCol)), NULL,
                                       SL_ALLOW_DUP_KEY, getTagIndexKey);
        if (pTable->pIndex == NULL) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          tsdbFreeTable(pTable);
          return NULL;
        }
      }
    }

    if (TABLE_TYPE(pTable) == TSDB_STREAM_TABLE) {
      buf = taosDecodeString(buf, &(pTable->sql));
    }
  }

  T_REF_INC(pTable);

  *pRTable = pTable;

  return buf;
}

static SArray* getJsonTagTableList(STable *pTable){
  uint32_t key = TSDB_DATA_JSON_NULL;
  char keyMd5[TSDB_MAX_JSON_KEY_MD5_LEN] = {0};
  jsonKeyMd5(&key, INT_BYTES, keyMd5);
  SArray** tablist = (SArray**)taosHashGet(pTable->jsonKeyMap, keyMd5, TSDB_MAX_JSON_KEY_MD5_LEN);

  return *tablist;
}

static int tsdbGetTableEncodeSize(int8_t act, STable *pTable) {
  int tlen = 0;
  if (act == TSDB_UPDATE_META) {
    tlen = sizeof(SListNode) + sizeof(SActObj) + sizeof(SActCont) + tsdbEncodeTable(NULL, pTable) + sizeof(TSCKSUM);
  } else {
    if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
      size_t tableSize = 0;
      if(pTable->tagSchema->columns[0].type == TSDB_DATA_TYPE_JSON){
        SArray* tablist = getJsonTagTableList(pTable);
        tableSize = taosArrayGetSize(tablist);
      }else{
        tableSize = SL_SIZE(pTable->pIndex);
      }
      tlen = (int)((sizeof(SListNode) + sizeof(SActObj)) * (tableSize + 1));
    } else {
      tlen = sizeof(SListNode) + sizeof(SActObj);
    }
  }

  return tlen;
}

static void *tsdbInsertTableAct(STsdbRepo *pRepo, int8_t act, void *buf, STable *pTable) {
  SListNode *pNode = (SListNode *)buf;
  SActObj *  pAct = (SActObj *)(pNode->data);
  SActCont * pCont = (SActCont *)POINTER_SHIFT(pAct, sizeof(*pAct));
  void *     pBuf = (void *)pCont;

  pNode->prev = pNode->next = NULL;
  pAct->act = act;
  pAct->uid = TABLE_UID(pTable);

  if (act == TSDB_UPDATE_META) {
    pBuf = (void *)(pCont->cont);
    pCont->len = tsdbEncodeTable(&pBuf, pTable) + sizeof(TSCKSUM);
    taosCalcChecksumAppend(0, (uint8_t *)pCont->cont, pCont->len);
    pBuf = POINTER_SHIFT(pBuf, sizeof(TSCKSUM));
  }

  tdListAppendNode(pRepo->mem->actList, pNode);

  return pBuf;
}

static int tsdbRemoveTableFromStore(STsdbRepo *pRepo, STable *pTable) {
  int   tlen = tsdbGetTableEncodeSize(TSDB_DROP_META, pTable);
  void *buf = tsdbAllocBytes(pRepo, tlen);
  if (buf == NULL) {
    return -1;
  }

  void *pBuf = buf;
  if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
    if(pTable->tagSchema->columns[0].type == TSDB_DATA_TYPE_JSON){
      SArray* tablist = getJsonTagTableList(pTable);
      for (int i = 0; i < taosArrayGetSize(tablist); ++i) {
        JsonMapValue* p = taosArrayGet(tablist, i);
        ASSERT(TABLE_TYPE((STable *)(p->table)) == TSDB_CHILD_TABLE);
        pBuf = tsdbInsertTableAct(pRepo, TSDB_DROP_META, pBuf, p->table);
      }
    }else {
      SSkipListIterator *pIter = tSkipListCreateIter(pTable->pIndex);
      if (pIter == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }

      while (tSkipListIterNext(pIter)) {
        STable *tTable = (STable *)SL_GET_NODE_DATA(tSkipListIterGet(pIter));
        ASSERT(TABLE_TYPE(tTable) == TSDB_CHILD_TABLE);
        pBuf = tsdbInsertTableAct(pRepo, TSDB_DROP_META, pBuf, tTable);
      }

      tSkipListDestroyIter(pIter);
    }
  }
  pBuf = tsdbInsertTableAct(pRepo, TSDB_DROP_META, pBuf, pTable);

  ASSERT(POINTER_DISTANCE(pBuf, buf) == tlen);

  return 0;
}

static int tsdbRmTableFromMeta(STsdbRepo *pRepo, STable *pTable) {
  if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
    tsdbWLockRepoMeta(pRepo);
    if(pTable->tagSchema->columns[0].type == TSDB_DATA_TYPE_JSON){
      SArray* tablist = getJsonTagTableList(pTable);
      for (int i = 0; i < taosArrayGetSize(tablist); ++i) {
        JsonMapValue* p = taosArrayGet(tablist, i);
        tsdbRemoveTableFromMeta(pRepo, p->table, false, false);
      }
    }else{
      SSkipListIterator *pIter = tSkipListCreateIter(pTable->pIndex);
      if (pIter == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
      while (tSkipListIterNext(pIter)) {
        STable *tTable = (STable *)SL_GET_NODE_DATA(tSkipListIterGet(pIter));
        tsdbRemoveTableFromMeta(pRepo, tTable, false, false);
      }
      tSkipListDestroyIter(pIter);
    }
    tsdbRemoveTableFromMeta(pRepo, pTable, false, false);
    tsdbUnlockRepoMeta(pRepo);
  } else {
    if ((TABLE_TYPE(pTable) == TSDB_STREAM_TABLE) && pTable->cqhandle) pRepo->appH.cqDropFunc(pTable->cqhandle);
    tsdbRemoveTableFromMeta(pRepo, pTable, true, true);
  }

  return 0;
}

static int tsdbAdjustMetaTables(STsdbRepo *pRepo, int tid) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  ASSERT(tid >= pMeta->maxTables);

  int maxTables = tsdbGetNextMaxTables(tid);

  STable **tables = (STable **)calloc(maxTables, sizeof(STable *));
  if (tables == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  memcpy((void *)tables, (void *)pMeta->tables, sizeof(STable *) * pMeta->maxTables);
  pMeta->maxTables = maxTables;

  STable **tTables = pMeta->tables;
  pMeta->tables = tables;
  tfree(tTables);
  tsdbDebug("vgId:%d tsdb meta maxTables is adjusted as %d", REPO_ID(pRepo), maxTables);

  return 0;
}

static int tsdbCheckTableTagVal(SKVRow *pKVRow, STSchema *pSchema) {
  for (size_t i = 0; i < kvRowNCols(pKVRow); i++) {
    SColIdx * pColIdx = kvRowColIdxAt(pKVRow, i);
    STColumn *pCol = tdGetColOfID(pSchema, pColIdx->colId);

    if ((pCol == NULL) || (!IS_VAR_DATA_TYPE(pCol->type))) continue;

    void *pValue = tdGetKVRowValOfCol(pKVRow, pCol->colId);
    if (varDataTLen(pValue) > pCol->bytes) {
      terrno = TSDB_CODE_TDB_IVLD_TAG_VAL;
      return -1;
    }
  }

  return 0;
}

static int tsdbAddSchema(STable *pTable, STSchema *pSchema) {
  ASSERT(TABLE_TYPE(pTable) != TSDB_CHILD_TABLE);

  if (pTable->schema == NULL) {
    pTable->schema = taosArrayInit(TSDB_MAX_TABLE_SCHEMAS, sizeof(SSchema *));
    if (pTable->schema == NULL) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }

  ASSERT(taosArrayGetSize(pTable->schema) == 0 ||
         schemaVersion(pSchema) > schemaVersion(*(STSchema **)taosArrayGetLast(pTable->schema)));

  if (taosArrayPush(pTable->schema, &pSchema) == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static void tsdbFreeTableSchema(STable *pTable) {
  ASSERT(pTable != NULL);

  if (pTable->schema) {
    for (size_t i = 0; i < taosArrayGetSize(pTable->schema); i++) {
      STSchema *pSchema = taosArrayGetP(pTable->schema, i);
      tdFreeSchema(pSchema);
    }

    taosArrayDestroy(&pTable->schema);
  }
}

