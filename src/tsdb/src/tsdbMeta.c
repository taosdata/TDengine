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
#include <stdlib.h>
#include "hash.h"
#include "taosdef.h"
#include "tchecksum.h"
#include "tsdb.h"
#include "tsdbMain.h"
#include "tskiplist.h"

#define TSDB_SUPER_TABLE_SL_LEVEL 5
#define DEFAULT_TAG_INDEX_COLUMN 0

// ------------------ OUTER FUNCTIONS ------------------
int tsdbCreateTable(TSDB_REPO_T *repo, STableCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  STable *   super = NULL;
  STable *   table = NULL;
  int        newSuper = 0;

  STable *pTable = tsdbGetTableByUid(pMeta, pCfg->tableId.uid);
  if (pTable != NULL) {
    tsdbError("vgId:%d table %s already exists, tid %d uid %" PRId64, REPO_ID(pRepo), TABLE_CHAR_NAME(pTable),
              TABLE_TID(pTable), TALBE_UID(pTable));
    return TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
  }

  if (pCfg->type == TSDB_CHILD_TABLE) {
    super = tsdbGetTableByUid(pMeta, pCfg->superUid);
    if (super == NULL) {  // super table not exists, try to create it
      newSuper = 1;
      super = tsdbNewTable(pCfg, true);
      if (super == NULL) goto _err;
    } else {
      // TODO
      if (super->type != TSDB_SUPER_TABLE) return -1;
      if (super->tableId.uid != pCfg->superUid) return -1;
      tsdbUpdateTable(pMeta, super, pCfg);
    }
  }

  table = tsdbNewTable(pCfg, false);
  if (table == NULL) goto _err;

  // Register to meta
  if (newSuper) {
    if (tsdbAddTableToMeta(pRepo, super, true) < 0) goto _err;
  }
  if (tsdbAddTableToMeta(pRepo, table, true) < 0) goto _err;

  // // Write to meta file
  // int   bufLen = 0;
  // char *buf = malloc(1024 * 1024);
  // if (newSuper) {
  //   tsdbEncodeTable(super, buf, &bufLen);
  //   tsdbInsertMetaRecord(pMeta->mfh, super->tableId.uid, buf, bufLen);
  // }

  // tsdbEncodeTable(table, buf, &bufLen);
  // tsdbInsertMetaRecord(pMeta->mfh, table->tableId.uid, buf, bufLen);
  // tfree(buf);

  return 0;

_err:
  tsdbFreeTable(super);
  tsdbFreeTable(table);
  return -1;
}

int tsdbDropTable(TSDB_REPO_T *repo, STableId tableId) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  if (pRepo == NULL) return -1;

  STsdbMeta *pMeta = pRepo->tsdbMeta;
  if (pMeta == NULL) return -1;

  STable *pTable = tsdbGetTableByUid(pMeta, tableId.uid);
  if (pTable == NULL) {
    tsdbError("vgId:%d, failed to drop table since table not exists! tid:%d, uid:" PRId64, pRepo->config.tsdbId,
              tableId.tid, tableId.uid);
    return -1;
  }

  tsdbTrace("vgId:%d, table %s is dropped! tid:%d, uid:%" PRId64, pRepo->config.tsdbId, varDataVal(pTable->name),
            tableId.tid, tableId.uid);
  if (tsdbRemoveTableFromMeta(pMeta, pTable, true) < 0) return -1;

  return 0;
}

void *tsdbGetTableTagVal(TSDB_REPO_T *repo, const STableId *id, int32_t colId, int16_t type, int16_t bytes) {
  // TODO: this function should be changed also
  STsdbMeta *pMeta = tsdbGetMeta(repo);
  STable *   pTable = tsdbGetTableByUid(pMeta, id->uid);

  STSchema *pSchema = tsdbGetTableTagSchema(pMeta, pTable);
  STColumn *pCol = tdGetColOfID(pSchema, colId);
  if (pCol == NULL) {
    return NULL;  // No matched tag volumn
  }

  char *val = tdGetKVRowValOfCol(pTable->tagVal, colId);
  assert(type == pCol->type && bytes == pCol->bytes);

  if (val != NULL && IS_VAR_DATA_TYPE(type)) {
    assert(varDataLen(val) < pCol->bytes);
  }

  return val;
}

char *tsdbGetTableName(TSDB_REPO_T *repo, const STableId *id) {
  // TODO: need to change as thread-safe
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  STable *pTable = tsdbGetTableByUid(pMeta, id->uid);

  if (pTable == NULL) {
    return NULL;
  } else {
    return (char *)pTable->name;
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

  if (tsdbInitTableCfg(pCfg, pMsg->tableType, htobe64(pMsg->uid), htonl(pMsg->sid)) < 0) goto _err;
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
  if (tsdbTableSetName(pCfg, pMsg->tableId, true) < 0) goto _err;

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
    if (tsdbTableSetSName(pCfg, pMsg->superTableId, true) < 0) goto _err;
    if (tsdbTableSetSuperUid(pCfg, htobe64(pMsg->superTableUid)) < 0) goto _err;

    // Decode tag values
    if (pMsg->tagDataLen) {
      int   accBytes = 0;
      char *pTagData = pMsg->data + (numOfCols + numOfTags) * sizeof(SSchema);

      SKVRowBuilder kvRowBuilder = {0};
      if (tdInitKVRowBuilder(&kvRowBuilder) < 0) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }
      for (int i = numOfCols; i < numOfCols + numOfTags; i++) {
        if (tdAddColToKVRow(&kvRowBuilder, htons(pSchema[i].colId), pSchema[i].type, pTagData + accBytes) < 0) {
          terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
          goto _err;
        }
        accBytes += htons(pSchema[i].bytes);
      }

      tsdbTableSetTagValue(pCfg, tdGetKVRowFromBuilder(&kvRowBuilder), false);
      tdDestroyKVRowBuilder(&kvRowBuilder);
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
  tfree(pCfg);
  return NULL;
}

// ------------------ INTERNAL FUNCTIONS ------------------
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

  pMeta->tables = (STable **)calloc(pCfg->maxTables, sizeof(STable *));
  if (pMeta->tables == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  pMeta->superList = tdListNew(sizeof(STable *));
  if (pMeta->superList == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  pMeta->uidMap = taosHashInit(pCfg->maxTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false);
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
  char *     fname = NULL;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  ASSERT(pMeta != NULL);

  fname = tsdbGetMetaFileName(pRepo->rootDir);
  if (fname == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  pMeta->pStore = tdOpenKVStore(fname, tsdbRestoreTable, tsdbOrgMeta, (void *)pRepo);
  if (pMeta->pStore == NULL) {
    tsdbError("vgId:%d failed to open TSDB meta while open the kv store since %s", REPO_ID(pRepo), tstrerror(terrno));
    goto _err;
  }

  tsdbTrace("vgId:%d open TSDB meta succeed", REPO_ID(pRepo));
  tfree(fname);
  return 0;

_err:
  tfree(fname);
  return -1;
}

int tsdbCloseMeta(STsdbRepo *pRepo) {
  STsdbCfg * pCfg = &pRepo->config;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  SListNode *pNode = NULL;
  STable *   pTable = NULL;

  if (pMeta == NULL) return 0;
  tdCloseKVStore(pMeta->pStore);
  for (int i = 1; i < pCfg->maxTables; i++) {
    tsdbFreeTable(pMeta->tables[i]);
  }

  while ((pNode = tdListPopHead(pMeta->superList)) != NULL) {
    tdListNodeGetData(pMeta->superList, pNode, (void *)(&pTable));
    tsdbFreeTable(pTable);
  }

  tsdbTrace("vgId:%d TSDB meta is closed", REPO_ID(pRepo));
  return 0;
}

STSchema *tsdbGetTableSchema(STable *pTable) {
  if (pTable->type == TSDB_NORMAL_TABLE || pTable->type == TSDB_SUPER_TABLE || pTable->type == TSDB_STREAM_TABLE) {
    return pTable->schema[pTable->numOfSchemas - 1];
  } else if (pTable->type == TSDB_CHILD_TABLE) {
    STable *pSuper = pTable->pSuper;
    if (pSuper == NULL) return NULL;
    return pSuper->schema[pSuper->numOfSchemas - 1];
  } else {
    return NULL;
  }
}

STable *tsdbGetTableByUid(STsdbMeta *pMeta, uint64_t uid) {
  void *ptr = taosHashGet(pMeta->uidMap, (char *)(&uid), sizeof(uid));

  if (ptr == NULL) return NULL;

  return *(STable **)ptr;
}

STSchema *tsdbGetTableSchemaByVersion(STable *pTable, int16_t version) {
  STable *pSearchTable = (pTable->type == TSDB_CHILD_TABLE) ? pTable->pSuper : pTable;
  if (pSearchTable == NULL) return NULL;

  void *ptr = taosbsearch(&version, pSearchTable->schema, pSearchTable->numOfSchemas, sizeof(STSchema *),
                          tsdbCompareSchemaVersion, TD_EQ);
  if (ptr == NULL) return NULL;

  return *(STSchema **)ptr;
}

STSchema *tsdbGetTableTagSchema(STable *pTable) {
  if (pTable->type == TSDB_SUPER_TABLE) {
    return pTable->tagSchema;
  } else if (pTable->type == TSDB_CHILD_TABLE) {
    STable *pSuper = pTable->pSuper;
    if (pSuper == NULL) return NULL;
    return pSuper->tagSchema;
  } else {
    return NULL;
  }
}

int tsdbUpdateTable(STsdbMeta *pMeta, STable *pTable, STableCfg *pCfg) {
  ASSERT(pTable->type != TSDB_CHILD_TABLE);
  bool isChanged = false;

  if (pTable->type == TSDB_SUPER_TABLE) {
    if (schemaVersion(pTable->tagSchema) < schemaVersion(pCfg->tagSchema)) {
      int32_t code = tsdbUpdateTableTagSchema(pTable, pCfg->tagSchema);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    isChanged = true;
  }

  STSchema *pTSchema = tsdbGetTableSchema(pMeta, pTable);
  if (schemaVersion(pTSchema) < schemaVersion(pCfg->schema)) {
    if (pTable->numOfSchemas < TSDB_MAX_TABLE_SCHEMAS) {
      pTable->schema[pTable->numOfSchemas++] = tdDupSchema(pCfg->schema);
    } else {
      ASSERT(pTable->numOfSchemas == TSDB_MAX_TABLE_SCHEMAS);
      STSchema *tSchema = tdDupSchema(pCfg->schema);
      tdFreeSchema(pTable->schema[0]);
      memmove(pTable->schema, pTable->schema + 1, sizeof(STSchema *) * (TSDB_MAX_TABLE_SCHEMAS - 1));
      pTable->schema[pTable->numOfSchemas - 1] = tSchema;
    }

    isChanged = true;
  }

  if (isChanged) {
    char *buf = malloc(1024 * 1024);
    int   bufLen = 0;
    tsdbEncodeTable(pTable, buf, &bufLen);
    tsdbInsertMetaRecord(pMeta->mfh, pTable->tableId.uid, buf, bufLen);
    free(buf);
  }

  return TSDB_CODE_SUCCESS;
}

char *getTSTupleKey(const void *data) {
  SDataRow row = (SDataRow)data;
  return POINTER_SHIFT(row, TD_DATA_ROW_HEAD_SIZE);
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

// ------------------ LOCAL FUNCTIONS ------------------
static int tsdbCompareSchemaVersion(const void *key1, const void *key2) {
  if (*(int16_t *)key1 < schemaVersion(*(STSchema **)key2)) {
    return -1;
  } else if (*(int16_t *)key1 > schemaVersion(*(STSchema **)key2)) {
    return 1;
  } else {
    return 0;
  }
}

static int tsdbRestoreTable(void *pHandle, void *cont, int contLen) {
  STsdbRepo *pRepo = (STsdbRepo *)pHandle;
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  STable *   pTable = NULL;

  if (!taosCheckChecksumWhole((uint8_t *)cont, contLen)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  tsdbDecodeTable(cont, &pTable);

  if (tsdbAddTableToMeta(pMeta, pTable, false) < 0) {
    tsdbFreeTable(pTable);
    return -1;
  }

  tsdbTrace("vgId:%d table %s tid %d uid %" PRIu64 " is restored from file", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable),
            TABLE_TID(pTable), TALBE_UID(pTable));
  return 0;
}

static void tsdbOrgMeta(void *pHandle) {
  STsdbRepo *pRepo = (STsdbRepo *)pHandle;
  STsdbCfg * pCfg = &pRepo->config;

  for (int i = 1; i < pMeta->maxTables; i++) {
    STable *pTable = pMeta->tables[i];
    if (pTable != NULL && pTable->type == TSDB_CHILD_TABLE) {
      tsdbAddTableIntoIndex(pMeta, pTable);
    }
  }
}

static char *getTagIndexKey(const void *pData) {
  STable *pTable = *(STable **)pData;

  STSchema *pSchema = tsdbGetTableTagSchema(pTable);
  STColumn *pCol = schemaColAt(DEFAULT_TAG_INDEX_COLUMN);
  void *    res = tdGetKVRowValOfCol(pTable->tagVal, pCol->colId);
  return res;
}

static STable *tsdbNewTable(STableCfg *pCfg, bool isSuper) {
  STable *pTable = NULL;
  size_t  tsize = 0;

  pTable = (STable *)calloc(1, sizeof(STable));
  if (pTable == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  if (isSuper) {
    pTable->type = TSDB_SUPER_TABLE;
    tsize = strnlen(pCfg->sname, TSDB_TABLE_NAME_LEN - 1);
    pTable->name = calloc(1, tsize + VARSTR_HEADER_SIZE + 1);
    if (pTable->name == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      goto _err;
    }
    STR_WITH_SIZE_TO_VARSTR(pTable->name, pCfg->sname, tsize);
    TALBE_UID(pTable) = pCfg->superUid;
    TABLE_TID(pTable) = -1;
    TABLE_SUID(pTable) = -1;
    pTable->pSuper = NULL;
    pTable->numOfSchemas = 1;
    pTable->schema[0] = tdDupSchema(pCfg->schema);
    if (pTable->schema[0] == NULL) {
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
    pTable->pIndex = tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, colType(pCol), colBytes(pCol), 1, 0, 1, getTagIndexKey);
    if (pTable->pIndex == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      goto _err;
    }
  } else {
    pTable->type = pCfg->type;
    tsize = strnlen(pCfg->name, TSDB_TABLE_NAME_LEN - 1);
    pTable->name = calloc(1, tsize + VARSTR_HEADER_SIZE + 1);
    if (pTable->name == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      goto _err;
    }
    STR_WITH_SIZE_TO_VARSTR(pTable->name, pCfg->name, tsize);
    TALBE_UID(pTable) = pCfg->tableId.uid;
    TABLE_TID(pTable) = pCfg->tableId.tid;

    if (pCfg->type == TSDB_CHILD_TABLE) {
      TABLE_SUID(pTable) = pCfg->superUid;
      pTable->tagVal = tdKVRowDup(pCfg->tagValues);
      if (pTable->tagVal == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        goto _err;
      }
    } else {
      TABLE_SUID(pTable) = -1;
      pTable->numOfSchemas = 1;
      pTable->schema[0] = tdDupSchema(pCfg->schema);
      if (pTable->schema[0] == NULL) {
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

    pTable->lastKey = TSKEY_INITIAL_VAL;
  }

  T_REF_INC(pTable);

  return pTable;

_err:
  tsdbFreeTable(pTable);
  return NULL;
}

static void tsdbFreeTable(STable *pTable) {
  if (pTable) {
    tfree(TABLE_NAME(pTable));
    if (TABLE_TYPE(pTable) != TSDB_CHILD_TABLE) {
      for (int i = 0; i < TSDB_MAX_TABLE_SCHEMAS; i++) {
        tdFreeSchema(pTable->schema[i]);
      }

      if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
        tdFreeSchema(pTable->tagSchema);
      }
    }

    kvRowFree(pTable->tagVal);

    tSkipListDestroy(pTable->pIndex);
    tfree(pTable->sql);
  }
}

static int tsdbUpdateTableTagSchema(STable *pTable, STSchema *newSchema) {
  ASSERT(pTable->type == TSDB_SUPER_TABLE);
  ASSERT(schemaVersion(pTable->tagSchema) < schemaVersion(newSchema));
  STSchema *pOldSchema = pTable->tagSchema;
  STSchema *pNewSchema = tdDupSchema(newSchema);
  if (pNewSchema == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }
  pTable->tagSchema = pNewSchema;
  tdFreeSchema(pOldSchema);

  return 0;
}

static int tsdbAddTableToMeta(STsdbRepo *pRepo, STable *pTable, bool addIdx) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  if (addIdx && tsdbWLockRepoMeta(pRepo) < 0) {
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
    if (TABLE_TID(pTable) == TSDB_CHILD_TABLE && addIdx) {  // add STABLE to the index
      if (tsdbAddTableIntoIndex(pMeta, pTable) < 0) {
        tsdbTrace("vgId:%d failed to add table %s to meta while add table to index since %s", REPO_ID(pRepo),
                  TABLE_CHAR_NAME(pTable), tstrerror(terrno));
        goto _err;
      }
    }
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

  if (addIdx && tsdbUnlockRepoMeta(pRepo) < 0) return -1;

  tsdbTrace("vgId:%d table %s tid %d uid %" PRIu64 " is added to meta", REPO_ID(pRepo), TABLE_CHAR_NAME(pTable),
            TABLE_TID(pTable), TALBE_UID(pTable));
  return 0;

_err:
  tsdbRemoveTableFromMeta(pRepo, pTable, false);
  if (addIdx) tsdbUnlockRepoMeta(pRepo);
  return -1;
}

static void tsdbRemoveTableFromMeta(STsdbRepo *pRepo, STable *pTable, bool rmFromIdx) {
  STsdbMeta *pMeta = pRepo->tsdbMeta;
  SListIter  lIter = {0};
  SListNode *pNode = NULL;
  STable *   tTable = NULL;

  if (rmFromIdx) tsdbWLockRepoMeta(pRepo);

  if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
    tdListInitIter(pMeta->superList, &lIter, TD_LIST_BACKWARD);

    while ((pNode = tdListNext(&lIter)) != NULL) {
      tdListNodeGetData(pMeta->superList, pNode, (void *)(tTable));
      if (pTable == tTable) {
        break;
        tdListPopNode(pMeta->superList, pNode);
        free(pNode);
      }
    }
  } else {
    pMeta->tables[pTable->tableId.tid] = NULL;
    if (TABLE_TYPE(pTable) == TSDB_CHILD_TABLE && rmFromIdx) {
      tsdbRemoveTableFromIndex(pMeta, pTable);
    }

    pMeta->nTables--;
  }

  taosHashRemove(pMeta->uid, (char *)(&(TALBE_UID(pTable))), sizeof(TALBE_UID(pTable)));

  if (rmFromIdx) tsdbUnlockRepoMeta(pRepo);
}

static int tsdbAddTableIntoIndex(STsdbMeta *pMeta, STable *pTable) {
  ASSERT(pTable->type == TSDB_CHILD_TABLE && pTable != NULL);
  STable *pSTable = tsdbGetTableByUid(pMeta, TABLE_SUID(pTable));
  ASSERT(pSTable != NULL);

  pTable->pSuper = pSTable;

  int32_t level = 0;
  int32_t headSize = 0;

  tSkipListNewNodeInfo(pSTable->pIndex, &level, &headSize);

  // NOTE: do not allocate the space for key, since in each skip list node, only keep the pointer to pTable, not the
  // actual key value, and the key value will be retrieved during query through the pTable and getTagIndexKey function
  SSkipListNode *pNode = calloc(1, headSize + sizeof(STable *));
  if (pNode == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }
  pNode->level = level;

  SSkipList *list = pSTable->pIndex;
  (STable *)(SL_GET_NODE_DATA(pNode)) = pTable;

  tSkipListPut(pSTable->pIndex, pNode);
  return 0;
}

static int tsdbRemoveTableFromIndex(STsdbMeta *pMeta, STable *pTable) {
  ASSERT(pTable->type == TSDB_CHILD_TABLE && pTable != NULL);

  STable *pSTable = tsdbGetTableByUid(pMeta, pTable->superUid);
  ASSERT(pSTable != NULL);

  STSchema *pSchema = tsdbGetTableTagSchema(pTable);
  STColumn *pCol = schemaColAt(pSchema, DEFAULT_TAG_INDEX_COLUMN);

  char *  key = tdGetKVRowValOfCol(pTable->tagVal, pCol->colId);
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

  taosArrayDestroy(res);
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

static void *tsdbEncodeTableName(void *buf, tstr *name) {
  void *pBuf = buf;

  pBuf = taosEncodeFixedI16(pBuf, name->len);
  memcpy(pBuf, name->data, name->len);
  pBuf = POINTER_SHIFT(pBuf, name->len);

  return POINTER_DISTANCE(pBuf, buf);
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

static void *tsdbEncodeTable(void *buf, STable *pTable) {
  ASSERT(pTable != NULL);

  buf = taosEncodeFixedU8(buf, pTable->type);
  buf = tsdbEncodeTableName(buf, pTable->name);
  buf = taosEncodeFixedU64(buf, TALBE_UID(pTable));
  buf = taosEncodeFixedI32(buf, TABLE_TID(pTable));

  if (TABLE_TYPE(pTable) == TSDB_CHILD_TABLE) {
    buf = taosEncodeFixedU64(buf, TABLE_SUID(pTable));
    buf = tdEncodeKVRow(buf, pTable->tagVal);
  } else {
    buf = taosEncodeFixedU8(buf, pTable->numOfSchemas);
    for (int i = 0; i < pTable->numOfSchemas; i++) {
      buf = tdEncodeSchema(buf, pTable->schema[i]);
    }

    if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
      buf = tdEncodeSchema(buf, pTable->tagSchema);
    }

    if (TABLE_TYPE(pTable) == TSDB_STREAM_TABLE) {
      buf = taosEncodeString(buf, pTable->sql);
    }
  }

  return buf;
}

static void *tsdbDecodeTable(void *buf, STable **pRTable) {
  STable *pTable = (STable *)calloc(1, sizeof(STable));
  if (pTable == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  buf = taosDecodeFixedU8(buf, &(pTable->type));
  buf = tsdbDecodeTableName(buf, &(pTable->name));
  buf = taosDecodeFixedU64(buf, &TALBE_UID(pTable));
  buf = taosDecodeFixedI32(buf, &TABLE_TID(pTable));

  if (TABLE_TYPE(pTable) == TSDB_CHILD_TABLE) {
    buf = taosDecodeFixedU64(buf, &TABLE_SUID(pTable));
    buf = tdDecodeKVRow(buf, &(pTable->tagVal));
  } else {
    buf = taosDecodeFixedU8(buf, &(pTable->numOfSchemas));
    for (int i = 0; i < pTable->numOfSchemas; i++) {
      buf = tdDecodeSchema(buf, &(pTable->schema[i]));
    }

    if (TABLE_TYPE(pTable) == TSDB_SUPER_TABLE) {
      buf = tdDecodeSchema(buf, &(pTable->tagSchema));
      STColumn *pCol = schemaColAt(pTable->tagSchema, DEFAULT_TAG_INDEX_COLUMN);
      pTable->pIndex =
          tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, colType(pCol), colBytes(pCol), 1, 0, 1, getTagIndexKey);
      if (pTable->pIndex == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        tsdbFreeTable(pTable);
        return NULL;
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