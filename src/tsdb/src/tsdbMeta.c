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

// ------------------ OUTER FUNCTIONS ------------------
int tsdbCreateTable(TSDB_REPO_T *repo, STableCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  if (tsdbCheckTableCfg(pCfg) < 0) return -1;

  STable *pTable = tsdbGetTableByUid(pMeta, pCfg->tableId.uid);
  if (pTable != NULL) {
    tsdbError("vgId:%d table %s already exists, tid %d uid %" PRId64, pRepo->config.tsdbId, varDataVal(pTable->name),
              pTable->tableId.tid, pTable->tableId.uid);
    return TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
  }

  STable *super = NULL;
  int newSuper = 0;

  if (pCfg->type == TSDB_CHILD_TABLE) {
    super = tsdbGetTableByUid(pMeta, pCfg->superUid);
    if (super == NULL) {  // super table not exists, try to create it
      newSuper = 1;
      super = tsdbNewTable(pCfg, true);
      if (super == NULL) return -1;
    } else {
      if (super->type != TSDB_SUPER_TABLE) return -1;
      if (super->tableId.uid != pCfg->superUid) return -1;
      tsdbUpdateTable(pMeta, super, pCfg);
    }
  }

  STable *table = tsdbNewTable(pCfg, false);
  if (table == NULL) {
    if (newSuper) {
      tsdbFreeTable(super);
      return -1;
    }
  }
  
  table->lastKey = TSKEY_INITIAL_VAL;
  
  // Register to meta
  if (newSuper) {
    tsdbAddTableToMeta(pMeta, super, true);
    tsdbTrace("vgId:%d, super table %s is created! uid:%" PRId64, pRepo->config.tsdbId, varDataVal(super->name),
              super->tableId.uid);
  }
  tsdbAddTableToMeta(pMeta, table, true);
  tsdbTrace("vgId:%d, table %s is created! tid:%d, uid:%" PRId64, pRepo->config.tsdbId, varDataVal(table->name),
            table->tableId.tid, table->tableId.uid);

  // Write to meta file
  int bufLen = 0;
  char *buf = malloc(1024*1024);
  if (newSuper) {
    tsdbEncodeTable(super, buf, &bufLen);
    tsdbInsertMetaRecord(pMeta->mfh, super->tableId.uid, buf, bufLen);
  }

  tsdbEncodeTable(table, buf, &bufLen);
  tsdbInsertMetaRecord(pMeta->mfh, table->tableId.uid, buf, bufLen);
  tfree(buf);

  return 0;
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

void* tsdbGetTableTagVal(TSDB_REPO_T* repo, const STableId* id, int32_t colId, int16_t type, int16_t bytes) {
  STsdbMeta* pMeta = tsdbGetMeta(repo);
  STable* pTable = tsdbGetTableByUid(pMeta, id->uid);

  STSchema *pSchema = tsdbGetTableTagSchema(pMeta, pTable);
  STColumn *pCol = tdGetColOfID(pSchema, colId);
  if (pCol == NULL) {
    return NULL;  // No matched tag volumn
  }
  
  char* val = tdGetKVRowValOfCol(pTable->tagVal, colId);
  assert(type == pCol->type && bytes == pCol->bytes);
  
  if (val != NULL && IS_VAR_DATA_TYPE(type)) {
    assert(varDataLen(val) < pCol->bytes);
  }
  
  return val;
}

char *tsdbGetTableName(TSDB_REPO_T *repo, const STableId *id) {
  STsdbMeta *pMeta = tsdbGetMeta(repo);
  STable *   pTable = tsdbGetTableByUid(pMeta, id->uid);

  if (pTable == NULL) {
    return NULL;
  } else {
    return (char *)pTable->name;
  }
}

STableCfg *tsdbCreateTableCfgFromMsg(SMDCreateTableMsg *pMsg) {
  if (pMsg == NULL) return NULL;
  SSchema *       pSchema = (SSchema *)pMsg->data;
  int16_t         numOfCols = htons(pMsg->numOfColumns);
  int16_t         numOfTags = htons(pMsg->numOfTags);
  STSchemaBuilder schemaBuilder = {0};

  STableCfg *pCfg = (STableCfg *)calloc(1, sizeof(STableCfg));
  if (pCfg == NULL) return NULL;

  if (tsdbInitTableCfg(pCfg, pMsg->tableType, htobe64(pMsg->uid), htonl(pMsg->sid)) < 0) goto _err;
  if (tdInitTSchemaBuilder(&schemaBuilder, htonl(pMsg->sversion)) < 0) goto _err;

  for (int i = 0; i < numOfCols; i++) {
    tdAddColToSchema(&schemaBuilder, pSchema[i].type, htons(pSchema[i].colId), htons(pSchema[i].bytes));
  }
  if (tsdbTableSetSchema(pCfg, tdGetSchemaFromBuilder(&schemaBuilder), false) < 0) goto _err;
  if (tsdbTableSetName(pCfg, pMsg->tableId, true) < 0) goto _err;

  if (numOfTags > 0) {
    // Decode tag schema
    tdResetTSchemaBuilder(&schemaBuilder, htonl(pMsg->tversion));
    for (int i = numOfCols; i < numOfCols + numOfTags; i++) {
      tdAddColToSchema(&schemaBuilder, pSchema[i].type, htons(pSchema[i].colId), htons(pSchema[i].bytes));
    }
    if (tsdbTableSetTagSchema(pCfg, tdGetSchemaFromBuilder(&schemaBuilder), false) < 0) goto _err;
    if (tsdbTableSetSName(pCfg, pMsg->superTableId, true) < 0) goto _err;
    if (tsdbTableSetSuperUid(pCfg, htobe64(pMsg->superTableUid)) < 0) goto _err;

    // Decode tag values
    if (pMsg->tagDataLen) {
      int   accBytes = 0;
      char *pTagData = pMsg->data + (numOfCols + numOfTags) * sizeof(SSchema);

      SKVRowBuilder kvRowBuilder = {0};
      if (tdInitKVRowBuilder(&kvRowBuilder) < 0) goto _err;
      for (int i = numOfCols; i < numOfCols + numOfTags; i++) {
        tdAddColToKVRow(&kvRowBuilder, htons(pSchema[i].colId), pSchema[i].type, pTagData + accBytes);
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

STSchema *tsdbGetTableSchema(STsdbMeta *pMeta, STable *pTable) {
  if (pTable->type == TSDB_NORMAL_TABLE || pTable->type == TSDB_SUPER_TABLE || pTable->type == TSDB_STREAM_TABLE) {
    return pTable->schema[pTable->numOfSchemas - 1];
  } else if (pTable->type == TSDB_CHILD_TABLE) {
    STable *pSuper = tsdbGetTableByUid(pMeta, pTable->superUid);
    if (pSuper == NULL) return NULL;
    return pSuper->schema[pSuper->numOfSchemas-1];
  } else {
    return NULL;
  }
}

STable *tsdbGetTableByUid(STsdbMeta *pMeta, uint64_t uid) {
  void *ptr = taosHashGet(pMeta->uidMap, (char *)(&uid), sizeof(uid));

  if (ptr == NULL) return NULL;

  return *(STable **)ptr;
}

STSchema *tsdbGetTableSchemaByVersion(STsdbMeta *pMeta, STable *pTable, int16_t version) {
  STable *pSearchTable = NULL;
  if (pTable->type == TSDB_CHILD_TABLE) {
    pSearchTable = tsdbGetTableByUid(pMeta, pTable->superUid);
  } else {
    pSearchTable = pTable;
  }
  ASSERT(pSearchTable != NULL);

  void *ptr = taosbsearch(&version, pSearchTable->schema, pSearchTable->numOfSchemas, sizeof(STSchema *),
                          tsdbCompareSchemaVersion, TD_EQ);
  if (ptr == NULL) return NULL;

  return *(STSchema **)ptr;
}

STSchema * tsdbGetTableTagSchema(STsdbMeta *pMeta, STable *pTable) {
  if (pTable->type == TSDB_SUPER_TABLE) {
    return pTable->tagSchema;
  } else if (pTable->type == TSDB_CHILD_TABLE) {
    STable *pSuper = tsdbGetTableByUid(pMeta, pTable->superUid);
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

char *getTSTupleKey(const void * data) {
  SDataRow row = (SDataRow)data;
  return POINTER_SHIFT(row, TD_DATA_ROW_HEAD_SIZE);
}


// ------------------ LOCAL FUNCTIONS ------------------
static void tsdbEncodeTable(STable *pTable, char *buf, int *contLen) {
  if (pTable == NULL) return;

  void *ptr = buf;
  T_APPEND_MEMBER(ptr, pTable, STable, type);
  // Encode name, todo refactor
  *(int *)ptr = varDataLen(pTable->name);
  ptr = (char *)ptr + sizeof(int);
  memcpy(ptr, varDataVal(pTable->name), varDataLen(pTable->name));
  ptr = (char *)ptr + varDataLen(pTable->name);

  T_APPEND_MEMBER(ptr, &(pTable->tableId), STableId, uid);
  T_APPEND_MEMBER(ptr, &(pTable->tableId), STableId, tid);
  T_APPEND_MEMBER(ptr, pTable, STable, superUid);

  if (pTable->type == TSDB_SUPER_TABLE) {
    T_APPEND_MEMBER(ptr, pTable, STable, numOfSchemas);
    for (int i = 0; i < pTable->numOfSchemas; i++) {
      ptr = tdEncodeSchema(ptr, pTable->schema[i]);
    }
    ptr = tdEncodeSchema(ptr, pTable->tagSchema);
  } else if (pTable->type == TSDB_CHILD_TABLE) {
    ptr = tdEncodeKVRow(ptr, pTable->tagVal);
  } else {
    T_APPEND_MEMBER(ptr, pTable, STable, numOfSchemas);
    for (int i = 0; i < pTable->numOfSchemas; i++) {
      ptr = tdEncodeSchema(ptr, pTable->schema[i]);
    }
  }

  if (pTable->type == TSDB_STREAM_TABLE) {
    ptr = taosEncodeString(ptr, pTable->sql);
  }

  *contLen = (char *)ptr - buf;
}

static STable *tsdbDecodeTable(void *cont, int contLen) {
  // TODO
  STable *pTable = (STable *)calloc(1, sizeof(STable));
  if (pTable == NULL) return NULL;

  void *ptr = cont;
  T_READ_MEMBER(ptr, int8_t, pTable->type);
  if (pTable->type != TSDB_CHILD_TABLE) {
    pTable->schema = (STSchema **)malloc(sizeof(STSchema *) * TSDB_MAX_TABLE_SCHEMAS);
    if (pTable->schema == NULL) {
      free(pTable);
      return NULL;
    }
  }
  int len = *(int *)ptr;
  ptr = (char *)ptr + sizeof(int);
  pTable->name = calloc(1, len + VARSTR_HEADER_SIZE + 1);
  if (pTable->name == NULL) return NULL;

  varDataSetLen(pTable->name, len);
  memcpy(pTable->name->data, ptr, len);

  ptr = (char *)ptr + len;
  T_READ_MEMBER(ptr, uint64_t, pTable->tableId.uid);
  T_READ_MEMBER(ptr, int32_t, pTable->tableId.tid);
  T_READ_MEMBER(ptr, uint64_t, pTable->superUid);

  if (pTable->type == TSDB_SUPER_TABLE) {
    T_READ_MEMBER(ptr, int16_t, pTable->numOfSchemas);
    for (int i = 0; i < pTable->numOfSchemas; i++) {
      pTable->schema[i] = tdDecodeSchema(&ptr);
    }
    pTable->tagSchema = tdDecodeSchema(&ptr);
  } else if (pTable->type == TSDB_CHILD_TABLE) {
    ptr = tdDecodeKVRow(ptr, &pTable->tagVal);
  } else {
    T_READ_MEMBER(ptr, int16_t, pTable->numOfSchemas);
    for (int i = 0; i < pTable->numOfSchemas; i++) {
      pTable->schema[i] = tdDecodeSchema(&ptr);
    }
  }

  if (pTable->type == TSDB_STREAM_TABLE) {
    ptr = taosDecodeString(ptr, &(pTable->sql));
  }

  pTable->lastKey = TSKEY_INITIAL_VAL;

  if (pTable->type == TSDB_SUPER_TABLE) {
    STColumn *pColSchema = schemaColAt(pTable->tagSchema, 0);
    pTable->pIndex =
        tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, pColSchema->type, pColSchema->bytes, 1, 0, 1, getTagIndexKey);
  }

  return pTable;
}

static int tsdbCompareSchemaVersion(const void *key1, const void *key2) {
  if (*(int16_t *)key1 < (*(STSchema **)key2)->version) {
    return -1;
  } else if (*(int16_t *)key1 > (*(STSchema **)key2)->version) {
    return 1;
  } else {
    return 0;
  }
}

static int tsdbRestoreTable(void *pHandle, void *cont, int contLen) {
  STsdbRepo *pRepo = (STsdbRepo *)pHandle;
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  if (!taosCheckChecksumWhole((uint8_t *)cont, contLen)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  STable *pTable = tsdbDecodeTable(cont, contLen);
  if (pTable == NULL) return -1;

  if (tsdbAddTableToMeta(pMeta, pTable, false) < 0) return -1;

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
  STableIndexElem *elem = (STableIndexElem *)pData;

  STSchema *pSchema = tsdbGetTableTagSchema(elem->pMeta, elem->pTable);
  STColumn *pCol = &pSchema->columns[DEFAULT_TAG_INDEX_COLUMN];
  void *    res = tdGetKVRowValOfCol(elem->pTable->tagVal, pCol->colId);
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

  pTable->type = pCfg->type;
  pTable->numOfSchemas = 0;

  if (isSuper) {
    pTable->type = TSDB_SUPER_TABLE;
    pTable->tableId.uid = pCfg->superUid;
    pTable->tableId.tid = -1;
    pTable->superUid = TSDB_INVALID_SUPER_TABLE_ID;
    pTable->schema = (STSchema **)malloc(sizeof(STSchema *) * TSDB_MAX_TABLE_SCHEMAS);
    pTable->numOfSchemas = 1;
    pTable->schema[0] = tdDupSchema(pCfg->schema);
    pTable->tagSchema = tdDupSchema(pCfg->tagSchema);

    tsize = strnlen(pCfg->sname, TSDB_TABLE_NAME_LEN - 1);
    pTable->name = calloc(1, tsize + VARSTR_HEADER_SIZE + 1);
    if (pTable->name == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      goto _err;
    }
    STR_WITH_SIZE_TO_VARSTR(pTable->name, pCfg->sname, tsize);

    STColumn *pColSchema = schemaColAt(pTable->tagSchema, 0);
    pTable->pIndex = tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, pColSchema->type, pColSchema->bytes, 1, 0, 0,
                                     getTagIndexKey);  // Allow duplicate key, no lock
    if (pTable->pIndex == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      goto _err;
    }
  } else {
    pTable->type = pCfg->type;
    pTable->tableId.uid = pCfg->tableId.uid;
    pTable->tableId.tid = pCfg->tableId.tid;
    pTable->lastKey = TSKEY_INITIAL_VAL;

    tsize = strnlen(pCfg->name, TSDB_TABLE_NAME_LEN - 1);
    pTable->name = calloc(1, tsize + VARSTR_HEADER_SIZE + 1);
    if (pTable->name == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      goto _err;
    }
    STR_WITH_SIZE_TO_VARSTR(pTable->name, pCfg->name, tsize);

    if (pCfg->type == TSDB_CHILD_TABLE) {
      pTable->superUid = pCfg->superUid;
      pTable->tagVal = tdKVRowDup(pCfg->tagValues);
    } else {
      pTable->schema = (STSchema **)malloc(sizeof(STSchema *) * TSDB_MAX_TABLE_SCHEMAS);
      pTable->numOfSchemas = 1;
      pTable->schema[0] = tdDupSchema(pCfg->schema);

      if (pCfg->type == TSDB_NORMAL_TABLE) {
        pTable->superUid = -1;
      } else {
        ASSERT(pCfg->type == TSDB_STREAM_TABLE);
        pTable->superUid = -1;
        pTable->sql = strdup(pCfg->sql);
      }
    }
  }

  return pTable;

_err:
  tsdbFreeTable(pTable);
  return NULL;
}

static int tsdbFreeTable(STable *pTable) {
  if (pTable == NULL) return 0;

  if (pTable->type == TSDB_CHILD_TABLE) {
    kvRowFree(pTable->tagVal);
  } else {
    if (pTable->schema) {
      for (int i = 0; i < pTable->numOfSchemas; i++) tdFreeSchema(pTable->schema[i]);
      free(pTable->schema);
    }
  }

  if (pTable->type == TSDB_STREAM_TABLE) {
    tfree(pTable->sql);
  }

  // Free content
  if (TSDB_TABLE_IS_SUPER_TABLE(pTable)) {
    tdFreeSchema(pTable->tagSchema);
    tSkipListDestroy(pTable->pIndex);
  }

  tsdbFreeMemTable(pTable->mem);
  tsdbFreeMemTable(pTable->imem);

  tfree(pTable->name);
  free(pTable);
  return 0;
}

static int tsdbUpdateTableTagSchema(STable *pTable, STSchema *newSchema) {
  ASSERT(pTable->type == TSDB_SUPER_TABLE);
  ASSERT(schemaVersion(pTable->tagSchema) < schemaVersion(newSchema));
  STSchema *pOldSchema = pTable->tagSchema;
  STSchema *pNewSchema = tdDupSchema(newSchema);
  if (pNewSchema == NULL) return TSDB_CODE_TDB_OUT_OF_MEMORY;
  pTable->tagSchema = pNewSchema;
  tdFreeSchema(pOldSchema);

  return TSDB_CODE_SUCCESS;
}

static int tsdbAddTableToMeta(STsdbMeta *pMeta, STable *pTable, bool addIdx) {
  STsdbRepo *pRepo = (STsdbRepo *)pMeta->pRepo;
  if (pTable->type == TSDB_SUPER_TABLE) { 
    // add super table to the linked list
    if (pMeta->superList == NULL) {
      pMeta->superList = pTable;
      pTable->next = NULL;
      pTable->prev = NULL;
    } else {
      pTable->next = pMeta->superList;
      pTable->prev = NULL;
      pTable->next->prev = pTable;
      pMeta->superList = pTable;
    }
  } else {
    // add non-super table to the array
    pMeta->tables[pTable->tableId.tid] = pTable;
    if (pTable->type == TSDB_CHILD_TABLE && addIdx) { // add STABLE to the index
      tsdbAddTableIntoIndex(pMeta, pTable);
    }
    if (pTable->type == TSDB_STREAM_TABLE && addIdx) {
      pTable->cqhandle = (*pRepo->appH.cqCreateFunc)(pRepo->appH.cqH, pTable->tableId.uid, pTable->tableId.tid, pTable->sql, tsdbGetTableSchema(pMeta, pTable));
    }
    
    pMeta->nTables++;
  }

  // Update the pMeta->maxCols and pMeta->maxRowBytes
  if (pTable->type == TSDB_SUPER_TABLE || pTable->type == TSDB_NORMAL_TABLE || pTable->type == TSDB_STREAM_TABLE) {
    if (schemaNCols(pTable->schema[pTable->numOfSchemas - 1]) > pMeta->maxCols)
      pMeta->maxCols = schemaNCols(pTable->schema[pTable->numOfSchemas - 1]);
    int bytes = dataRowMaxBytesFromSchema(pTable->schema[pTable->numOfSchemas - 1]);
    if (bytes > pMeta->maxRowBytes) pMeta->maxRowBytes = bytes;
  }

  if (taosHashPut(pMeta->map, (char *)(&pTable->tableId.uid), sizeof(pTable->tableId.uid), (void *)(&pTable), sizeof(pTable)) < 0) {
    return -1;
  }
  return 0;
}

static int tsdbRemoveTableFromMeta(STsdbMeta *pMeta, STable *pTable, bool rmFromIdx) {
  if (pTable->type == TSDB_SUPER_TABLE) {
    SSkipListIterator  *pIter = tSkipListCreateIter(pTable->pIndex);
    while (tSkipListIterNext(pIter)) {
      STableIndexElem *pEle = (STableIndexElem *)SL_GET_NODE_DATA(tSkipListIterGet(pIter));
      STable *tTable = pEle->pTable;

      ASSERT(tTable != NULL && tTable->type == TSDB_CHILD_TABLE);

      tsdbRemoveTableFromMeta(pMeta, tTable, false);
    }

    tSkipListDestroyIter(pIter);

    if (pTable->prev != NULL) {
      pTable->prev->next = pTable->next;
      if (pTable->next != NULL) {
        pTable->next->prev = pTable->prev;
      }
    } else {
      pMeta->superList = pTable->next;
    }
  } else {
    pMeta->tables[pTable->tableId.tid] = NULL;
    if (pTable->type == TSDB_CHILD_TABLE && rmFromIdx) {
      tsdbRemoveTableFromIndex(pMeta, pTable);
    }
    if (pTable->type == TSDB_STREAM_TABLE && rmFromIdx) {
      // TODO
    }

    pMeta->nTables--;
  }

  taosHashRemove(pMeta->map, (char *)(&(pTable->tableId.uid)), sizeof(pTable->tableId.uid));
  tsdbFreeTable(pTable);
  return 0;
}

static int tsdbAddTableIntoIndex(STsdbMeta *pMeta, STable *pTable) {
  assert(pTable->type == TSDB_CHILD_TABLE && pTable != NULL);
  STable* pSTable = tsdbGetTableByUid(pMeta, pTable->superUid);
  assert(pSTable != NULL);
  
  int32_t level = 0;
  int32_t headSize = 0;
  
  tSkipListNewNodeInfo(pSTable->pIndex, &level, &headSize);
  
  // NOTE: do not allocate the space for key, since in each skip list node, only keep the pointer to pTable, not the
  // actual key value, and the key value will be retrieved during query through the pTable and getTagIndexKey function
  SSkipListNode* pNode = calloc(1, headSize + sizeof(STableIndexElem));
  pNode->level = level;
  
  SSkipList* list = pSTable->pIndex;
  STableIndexElem* elem = (STableIndexElem*) (SL_GET_NODE_DATA(pNode));
  
  elem->pTable = pTable;
  elem->pMeta = pMeta;
  
  tSkipListPut(list, pNode);
  return 0;
}

static int tsdbRemoveTableFromIndex(STsdbMeta *pMeta, STable *pTable) {
  assert(pTable->type == TSDB_CHILD_TABLE && pTable != NULL);
  
  STable* pSTable = tsdbGetTableByUid(pMeta, pTable->superUid);
  assert(pSTable != NULL);
  
  STSchema* pSchema = tsdbGetTableTagSchema(pMeta, pTable);
  STColumn* pCol = &pSchema->columns[DEFAULT_TAG_INDEX_COLUMN];
  
  char* key = tdGetKVRowValOfCol(pTable->tagVal, pCol->colId);
  SArray* res = tSkipListGet(pSTable->pIndex, key);
  
  size_t size = taosArrayGetSize(res);
  assert(size > 0);
  
  for(int32_t i = 0; i < size; ++i) {
    SSkipListNode* pNode = taosArrayGetP(res, i);
    
    STableIndexElem* pElem = (STableIndexElem*) SL_GET_NODE_DATA(pNode);
    if (pElem->pTable == pTable) {  // this is the exact what we need
      tSkipListRemoveNode(pSTable->pIndex, pNode);
    }
  }
  
  taosArrayDestroy(res);
  return 0;
}

#if 0
#define TSDB_SUPER_TABLE_SL_LEVEL 5 // TODO: may change here
// #define TSDB_META_FILE_NAME "META"





static int tsdbInitTableCfg(STableCfg *config, ETableType type, uint64_t uid, int32_t tid) {
  if (config == NULL) return -1;
  if (type != TSDB_CHILD_TABLE && type != TSDB_NORMAL_TABLE && type != TSDB_STREAM_TABLE) return -1;

  memset((void *)config, 0, sizeof(STableCfg));

  config->type = type;
  config->superUid = TSDB_INVALID_SUPER_TABLE_ID;
  config->tableId.uid = uid;
  config->tableId.tid = tid;
  config->name = NULL;
  config->sql = NULL;
  return 0;
}

/**
 * Set the super table UID of the created table
 */
static int tsdbTableSetSuperUid(STableCfg *config, uint64_t uid) {
  if (config->type != TSDB_CHILD_TABLE) return -1;
  if (uid == TSDB_INVALID_SUPER_TABLE_ID) return -1;

  config->superUid = uid;
  return 0;
}

/**
 * Set the table schema in the configuration
 * @param config the configuration to set
 * @param pSchema the schema to set
 * @param dup use the schema directly or duplicate one for use
 * 
 * @return 0 for success and -1 for failure
 */
static int tsdbTableSetSchema(STableCfg *config, STSchema *pSchema, bool dup) {
  if (dup) {
    config->schema = tdDupSchema(pSchema);
  } else {
    config->schema = pSchema;
  }
  return 0;
}

/**
 * Set the table schema in the configuration
 * @param config the configuration to set
 * @param pSchema the schema to set
 * @param dup use the schema directly or duplicate one for use
 * 
 * @return 0 for success and -1 for failure
 */
static int tsdbTableSetTagSchema(STableCfg *config, STSchema *pSchema, bool dup) {
  if (config->type != TSDB_CHILD_TABLE) return -1;

  if (dup) {
    config->tagSchema = tdDupSchema(pSchema);
  } else {
    config->tagSchema = pSchema;
  }
  return 0;
}

static int tsdbTableSetTagValue(STableCfg *config, SKVRow row, bool dup) {
  if (config->type != TSDB_CHILD_TABLE) return -1;

  if (dup) {
    config->tagValues = tdKVRowDup(row);
  } else {
    config->tagValues = row;
  }

  return 0;
}

static int tsdbTableSetName(STableCfg *config, char *name, bool dup) {
  if (dup) {
    config->name = strdup(name);
    if (config->name == NULL) return -1;
  } else {
    config->name = name;
  }

  return 0;
}

static int tsdbTableSetSName(STableCfg *config, char *sname, bool dup) {
  if (config->type != TSDB_CHILD_TABLE) return -1;

  if (dup) {
    config->sname = strdup(sname);
    if (config->sname == NULL) return -1;
  } else {
    config->sname = sname;
  }
  return 0;
}

static int tsdbTableSetStreamSql(STableCfg *config, char *sql, bool dup) {
  if (config->type != TSDB_STREAM_TABLE) return -1;
  
  if (dup) {
    config->sql = strdup(sql);
    if (config->sql == NULL) return -1;
  } else {
    config->sql = sql;
  }

  return 0;
}
#endif