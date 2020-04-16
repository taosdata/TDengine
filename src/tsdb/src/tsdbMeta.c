#include <stdlib.h>

// #include "taosdef.h"
#include "tskiplist.h"
#include "tsdb.h"
#include "taosdef.h"
#include "hash.h"
#include "tsdbMain.h"

#define TSDB_SUPER_TABLE_SL_LEVEL 5 // TODO: may change here
#define TSDB_META_FILE_NAME "META"

static int     tsdbFreeTable(STable *pTable);
static int32_t tsdbCheckTableCfg(STableCfg *pCfg);
static int     tsdbAddTableToMeta(STsdbMeta *pMeta, STable *pTable, bool addIdx);
static int     tsdbAddTableIntoMap(STsdbMeta *pMeta, STable *pTable);
static int     tsdbAddTableIntoIndex(STsdbMeta *pMeta, STable *pTable);
static int     tsdbRemoveTableFromIndex(STsdbMeta *pMeta, STable *pTable);
static int     tsdbEstimateTableEncodeSize(STable *pTable);

/**
 * Encode a TSDB table object as a binary content
 * ASSUMPTIONS: VALID PARAMETERS
 * 
 * @param pTable table object to encode
 * @param contLen the encoded binary content length
 * 
 * @return binary content for success
 *         NULL fro failure
 */
void *tsdbEncodeTable(STable *pTable, int *contLen) {
  if (pTable == NULL) return NULL;

  *contLen = tsdbEstimateTableEncodeSize(pTable);
  if (*contLen < 0) return NULL;

  void *ret = calloc(1, *contLen);
  if (ret == NULL) return NULL;

  void *ptr = ret;
  T_APPEND_MEMBER(ptr, pTable, STable, type);
  T_APPEND_MEMBER(ptr, &(pTable->tableId), STableId, uid);
  T_APPEND_MEMBER(ptr, &(pTable->tableId), STableId, tid);
  T_APPEND_MEMBER(ptr, pTable, STable, superUid);
  T_APPEND_MEMBER(ptr, pTable, STable, sversion);

  if (pTable->type == TSDB_SUPER_TABLE) {
    ptr = tdEncodeSchema(ptr, pTable->schema);
    ptr = tdEncodeSchema(ptr, pTable->tagSchema);
  } else if (pTable->type == TSDB_CHILD_TABLE) {
    dataRowCpy(ptr, pTable->tagVal);
  } else {
    ptr = tdEncodeSchema(ptr, pTable->schema);
  }

  return ret;
}

/**
 * Decode from an encoded binary
 * ASSUMPTIONS: valid parameters
 * 
 * @param cont binary object
 * @param contLen binary length
 * 
 * @return TSDB table object for success
 *         NULL for failure
 */
STable *tsdbDecodeTable(void *cont, int contLen) {
  STable *pTable = (STable *)calloc(1, sizeof(STable));
  if (pTable == NULL) return NULL;

  void *ptr = cont;
  T_READ_MEMBER(ptr, int8_t, pTable->type);
  T_READ_MEMBER(ptr, int64_t, pTable->tableId.uid);
  T_READ_MEMBER(ptr, int32_t, pTable->tableId.tid);
  T_READ_MEMBER(ptr, int64_t, pTable->superUid);
  T_READ_MEMBER(ptr, int32_t, pTable->sversion);

  if (pTable->type == TSDB_SUPER_TABLE) {
    pTable->schema = tdDecodeSchema(&ptr);
    pTable->tagSchema = tdDecodeSchema(&ptr);
  } else if (pTable->type == TSDB_CHILD_TABLE) {
    pTable->tagVal = tdDataRowDup(ptr);
  } else {
    pTable->schema = tdDecodeSchema(&ptr);
  }

  return pTable;
}

void tsdbFreeEncode(void *cont) {
  if (cont != NULL) free(cont);
}

static char* getTagIndexKey(const void* pData) {
  STable* table = *(STable**) pData;
  return getTupleKey(table->tagVal);
}

int tsdbRestoreTable(void *pHandle, void *cont, int contLen) {
  STsdbMeta *pMeta = (STsdbMeta *)pHandle;

  STable *pTable = tsdbDecodeTable(cont, contLen);
  if (pTable == NULL) return -1;
  
  if (pTable->type == TSDB_SUPER_TABLE) {
    STColumn* pColSchema = schemaColAt(pTable->tagSchema, 0);
    pTable->pIndex = tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, pColSchema->type, pColSchema->bytes,
                                    1, 0, 0, getTagIndexKey);
  }

  tsdbAddTableToMeta(pMeta, pTable, false);

  return 0;
}

void tsdbOrgMeta(void *pHandle) {
  STsdbMeta *pMeta = (STsdbMeta *)pHandle;

  for (int i = 0; i < pMeta->maxTables; i++) {
    STable *pTable = pMeta->tables[i];
    if (pTable != NULL && pTable->type == TSDB_CHILD_TABLE) {
      tsdbAddTableIntoIndex(pMeta, pTable);
    }
  }
}

/**
 * Initialize the meta handle
 * ASSUMPTIONS: VALID PARAMETER
 */
STsdbMeta *tsdbInitMeta(char *rootDir, int32_t maxTables) {
  STsdbMeta *pMeta = (STsdbMeta *)malloc(sizeof(STsdbMeta));
  if (pMeta == NULL) return NULL;

  pMeta->maxTables = maxTables;
  pMeta->nTables = 0;
  pMeta->superList = NULL;
  pMeta->tables = (STable **)calloc(maxTables, sizeof(STable *));
  pMeta->maxRowBytes = 0;
  pMeta->maxCols = 0;
  if (pMeta->tables == NULL) {
    free(pMeta);
    return NULL;
  }

  pMeta->map = taosHashInit(maxTables * TSDB_META_HASH_FRACTION, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false);
  if (pMeta->map == NULL) {
    free(pMeta->tables);
    free(pMeta);
    return NULL;
  }

  pMeta->mfh = tsdbInitMetaFile(rootDir, maxTables, tsdbRestoreTable, tsdbOrgMeta, pMeta);
  if (pMeta->mfh == NULL) {
    taosHashCleanup(pMeta->map);
    free(pMeta->tables);
    free(pMeta);
    return NULL;
  }

  return pMeta;
}

int32_t tsdbFreeMeta(STsdbMeta *pMeta) {
  if (pMeta == NULL) return 0;

  tsdbCloseMetaFile(pMeta->mfh);

  for (int i = 0; i < pMeta->maxTables; i++) {
    if (pMeta->tables[i] != NULL) {
      tsdbFreeTable(pMeta->tables[i]);
    }
  }

  free(pMeta->tables);

  STable *pTable = pMeta->superList;
  while (pTable != NULL) {
    STable *pTemp = pTable;
    pTable = pTemp->next;
    tsdbFreeTable(pTemp);
  }

  taosHashCleanup(pMeta->map);

  free(pMeta);

  return 0;
}

STSchema *tsdbGetTableSchema(STsdbMeta *pMeta, STable *pTable) {
  if (pTable->type == TSDB_NORMAL_TABLE || pTable->type == TSDB_SUPER_TABLE) {
    return pTable->schema;
  } else if (pTable->type == TSDB_CHILD_TABLE) {
    STable *pSuper = tsdbGetTableByUid(pMeta, pTable->superUid);
    if (pSuper == NULL) return NULL;
    return pSuper->schema;
  } else {
    return NULL;
  }
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

int32_t tsdbCreateTableImpl(STsdbMeta *pMeta, STableCfg *pCfg) {
  if (tsdbCheckTableCfg(pCfg) < 0) return -1;

  STable *super = NULL;
  int newSuper = 0;

  if (pCfg->type == TSDB_CHILD_TABLE) {
    super = tsdbGetTableByUid(pMeta, pCfg->superUid);
    if (super == NULL) {  // super table not exists, try to create it
      newSuper = 1;
      // TODO: use function to implement create table object
      super = (STable *)calloc(1, sizeof(STable));
      if (super == NULL) return -1;

      super->type = TSDB_SUPER_TABLE;
      super->tableId.uid = pCfg->superUid;
      super->tableId.tid = -1;
      super->superUid = TSDB_INVALID_SUPER_TABLE_ID;
      super->schema = tdDupSchema(pCfg->schema);
      super->tagSchema = tdDupSchema(pCfg->tagSchema);
      super->tagVal = tdDataRowDup(pCfg->tagValues);
  
      // index the first tag column
      STColumn* pColSchema = schemaColAt(super->tagSchema, 0);
      super->pIndex = tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, pColSchema->type, pColSchema->bytes,
          1, 0, 0, getTagIndexKey);  // Allow duplicate key, no lock

      if (super->pIndex == NULL) {
        tdFreeSchema(super->schema);
        tdFreeSchema(super->tagSchema);
        tdFreeDataRow(super->tagVal);
        free(super);
        return -1;
      }
    } else {
      if (super->type != TSDB_SUPER_TABLE) return -1;
    }
  }

  STable *table = (STable *)calloc(1, sizeof(STable));
  if (table == NULL) {
    if (newSuper) tsdbFreeTable(super);
    return -1;
  }

  table->tableId = pCfg->tableId;
  if (IS_CREATE_STABLE(pCfg)) { // TSDB_CHILD_TABLE
    table->type = TSDB_CHILD_TABLE;
    table->superUid = pCfg->superUid;
    table->tagVal = tdDataRowDup(pCfg->tagValues);
  } else { // TSDB_NORMAL_TABLE
    table->type = TSDB_NORMAL_TABLE;
    table->superUid = -1;
    table->schema = tdDupSchema(pCfg->schema);
  }

  // Register to meta
  if (newSuper) tsdbAddTableToMeta(pMeta, super, true);
  tsdbAddTableToMeta(pMeta, table, true);

  // Write to meta file
  int bufLen = 0;
  if (newSuper) {
    void *buf = tsdbEncodeTable(super, &bufLen);
    tsdbInsertMetaRecord(pMeta->mfh, super->tableId.uid, buf, bufLen);
    tsdbFreeEncode(buf);
  }

  void *buf = tsdbEncodeTable(table, &bufLen);
  tsdbInsertMetaRecord(pMeta->mfh, table->tableId.uid, buf, bufLen);
  tsdbFreeEncode(buf);

  return 0;
}

/**
 * Check if a table is valid to insert.
 * @return NULL for invalid and the pointer to the table if valid
 */
STable *tsdbIsValidTableToInsert(STsdbMeta *pMeta, STableId tableId) {
  STable *pTable = tsdbGetTableByUid(pMeta, tableId.uid);
  if (pTable == NULL) {
    return NULL;
  }

  if (TSDB_TABLE_IS_SUPER_TABLE(pTable)) return NULL;
  if (pTable->tableId.tid != tableId.tid) return NULL;

  return pTable;
}

int32_t tsdbDropTableImpl(STsdbMeta *pMeta, STableId tableId) {
  if (pMeta == NULL) return -1;

  STable *pTable = tsdbGetTableByUid(pMeta, tableId.uid);
  if (pTable == NULL) return -1;

  if (pTable->type == TSDB_SUPER_TABLE) {
    // TODO: implement drop super table
    return -1;
  } else {
    pMeta->tables[pTable->tableId.tid] = NULL;
    pMeta->nTables--;
    assert(pMeta->nTables >= 0);
    if (pTable->type == TSDB_CHILD_TABLE) {
      tsdbRemoveTableFromIndex(pMeta, pTable);
    }

    tsdbFreeTable(pTable);
  }

  return 0;
}

// int32_t tsdbInsertRowToTableImpl(SSkipListNode *pNode, STable *pTable) {
//   tSkipListPut(pTable->mem->pData, pNode);
//   return 0;
// }

static void tsdbFreeMemTable(SMemTable *pMemTable) {
  if (pMemTable) {
    tSkipListDestroy(pMemTable->pData);
  }

  free(pMemTable);
}

static int tsdbFreeTable(STable *pTable) {
  // TODO: finish this function
  if (pTable->type == TSDB_CHILD_TABLE) {
    tdFreeDataRow(pTable->tagVal);
  } else {
    tdFreeSchema(pTable->schema);
  }

  // Free content
  if (TSDB_TABLE_IS_SUPER_TABLE(pTable)) {
    tSkipListDestroy(pTable->pIndex);
  }

  tsdbFreeMemTable(pTable->mem);
  tsdbFreeMemTable(pTable->imem);

  free(pTable);
  return 0;
}

static int32_t tsdbCheckTableCfg(STableCfg *pCfg) {
  // TODO
  return 0;
}

STable *tsdbGetTableByUid(STsdbMeta *pMeta, int64_t uid) {
  void *ptr = taosHashGet(pMeta->map, (char *)(&uid), sizeof(uid));

  if (ptr == NULL) return NULL;

  return *(STable **)ptr;
}

static int tsdbAddTableToMeta(STsdbMeta *pMeta, STable *pTable, bool addIdx) {
  if (pTable->type == TSDB_SUPER_TABLE) { 
    // add super table to the linked list
    if (pMeta->superList == NULL) {
      pMeta->superList = pTable;
      pTable->next = NULL;
    } else {
      STable *pTemp = pMeta->superList;
      pMeta->superList = pTable;
      pTable->next = pTemp;
    }
  } else {
    // add non-super table to the array
    pMeta->tables[pTable->tableId.tid] = pTable;
    if (pTable->type == TSDB_CHILD_TABLE && addIdx) { // add STABLE to the index
      tsdbAddTableIntoIndex(pMeta, pTable);
    }
    
    pMeta->nTables++;
  }

  // Update the pMeta->maxCols and pMeta->maxRowBytes
  if (pTable->type == TSDB_SUPER_TABLE || pTable->type == TSDB_NORMAL_TABLE) {
    if (schemaNCols(pTable->schema) > pMeta->maxCols) pMeta->maxCols = schemaNCols(pTable->schema);
    int bytes = tdMaxRowBytesFromSchema(pTable->schema);
    if (bytes > pMeta->maxRowBytes) pMeta->maxRowBytes = bytes;
    tdUpdateSchema(pTable->schema);
  }

  return tsdbAddTableIntoMap(pMeta, pTable);
}

// static int tsdbRemoveTableFromMeta(STsdbMeta *pMeta, STable *pTable) {
//   // TODO
//   return 0;
// }

static int tsdbAddTableIntoMap(STsdbMeta *pMeta, STable *pTable) {
  // TODO: add the table to the map
  int64_t uid = pTable->tableId.uid;
  if (taosHashPut(pMeta->map, (char *)(&uid), sizeof(uid), (void *)(&pTable), sizeof(pTable)) < 0) {
    return -1;
  }
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
  SSkipListNode* pNode = calloc(1, headSize + POINTER_BYTES);
  pNode->level = level;
  
  SSkipList* list = pSTable->pIndex;
  memcpy(SL_GET_NODE_DATA(pNode), &pTable, POINTER_BYTES);
  
  tSkipListPut(list, pNode);

  return 0;
}

static int tsdbRemoveTableFromIndex(STsdbMeta *pMeta, STable *pTable) {
  assert(pTable->type == TSDB_CHILD_TABLE);
  // TODO
  return 0;
}

static int tsdbEstimateTableEncodeSize(STable *pTable) {
  int size = 0;
  size += T_MEMBER_SIZE(STable, type);
  size += T_MEMBER_SIZE(STable, tableId);
  size += T_MEMBER_SIZE(STable, superUid);
  size += T_MEMBER_SIZE(STable, sversion);

  if (pTable->type == TSDB_SUPER_TABLE) {
    size += tdGetSchemaEncodeSize(pTable->schema);
    size += tdGetSchemaEncodeSize(pTable->tagSchema);
  } else if (pTable->type == TSDB_CHILD_TABLE) {
    size += dataRowLen(pTable->tagVal);
  } else {
    size += tdGetSchemaEncodeSize(pTable->schema);
  }

  return size;
}

char *getTupleKey(const void * data) {
  SDataRow row = (SDataRow)data;

  return dataRowAt(row, TD_DATA_ROW_HEAD_SIZE);
}