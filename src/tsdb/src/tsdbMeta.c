#include <stdlib.h>
#include "tskiplist.h"
#include "tsdb.h"
#include "taosdef.h"
#include "hash.h"
#include "tsdbMain.h"

#define TSDB_SUPER_TABLE_SL_LEVEL 5 // TODO: may change here
// #define TSDB_META_FILE_NAME "META"

const int32_t DEFAULT_TAG_INDEX_COLUMN = 0;   // skip list built based on the first column of tags

static int     tsdbFreeTable(STable *pTable);
static int32_t tsdbCheckTableCfg(STableCfg *pCfg);
static int     tsdbAddTableToMeta(STsdbMeta *pMeta, STable *pTable, bool addIdx);
static int     tsdbAddTableIntoIndex(STsdbMeta *pMeta, STable *pTable);
static int     tsdbRemoveTableFromIndex(STsdbMeta *pMeta, STable *pTable);
static int     tsdbRemoveTableFromMeta(STsdbMeta *pMeta, STable *pTable, bool rmFromIdx);

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
void tsdbEncodeTable(STable *pTable, char *buf, int *contLen) {
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
  T_APPEND_MEMBER(ptr, pTable, STable, sversion);

  if (pTable->type == TSDB_SUPER_TABLE) {
    ptr = tdEncodeSchema(ptr, pTable->schema);
    ptr = tdEncodeSchema(ptr, pTable->tagSchema);
  } else if (pTable->type == TSDB_CHILD_TABLE) {
    tdTagRowCpy(ptr, pTable->tagVal);
    ptr = POINTER_SHIFT(ptr, dataRowLen(pTable->tagVal) + ((STagRow *)pTable->tagVal)->dataLen);
  } else {
    ptr = tdEncodeSchema(ptr, pTable->schema);
  }

  if (pTable->type == TSDB_STREAM_TABLE) {
    ptr = taosEncodeString(ptr, pTable->sql);
  }

  *contLen = (char *)ptr - buf;
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
  T_READ_MEMBER(ptr, int32_t, pTable->sversion);

  if (pTable->type == TSDB_SUPER_TABLE) {
    pTable->schema = tdDecodeSchema(&ptr);
    pTable->tagSchema = tdDecodeSchema(&ptr);
  } else if (pTable->type == TSDB_CHILD_TABLE) {
    pTable->tagVal = tdTagRowDecode(ptr);
    ptr = POINTER_SHIFT(ptr, dataRowLen(pTable->tagVal) + ((STagRow *)pTable->tagVal)->dataLen);
  } else {
    pTable->schema = tdDecodeSchema(&ptr);
  }

  if (pTable->type == TSDB_STREAM_TABLE) {
    ptr = taosDecodeString(ptr, &(pTable->sql));
  }

  return pTable;
}

void tsdbFreeEncode(void *cont) {
  if (cont != NULL) free(cont);
}

static char* getTagIndexKey(const void* pData) {
  STableIndexElem* elem = (STableIndexElem*) pData;
  
  SDataRow row = elem->pTable->tagVal;
  STSchema* pSchema = tsdbGetTableTagSchema(elem->pMeta, elem->pTable);
  STColumn* pCol = &pSchema->columns[DEFAULT_TAG_INDEX_COLUMN];
  int16_t type = 0;
  void * res = tdQueryTagByID(row, pCol->colId,&type);
  ASSERT(type == pCol->type);
  return res;
}

int tsdbRestoreTable(void *pHandle, void *cont, int contLen) {
  STsdbMeta *pMeta = (STsdbMeta *)pHandle;

  STable *pTable = tsdbDecodeTable(cont, contLen);
  if (pTable == NULL) return -1;
  
  if (pTable->type == TSDB_SUPER_TABLE) {
    STColumn* pColSchema = schemaColAt(pTable->tagSchema, 0);
    pTable->pIndex = tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, pColSchema->type, pColSchema->bytes,
                                    1, 0, 1, getTagIndexKey);
  }

  tsdbAddTableToMeta(pMeta, pTable, false);

  return 0;
}

void tsdbOrgMeta(void *pHandle) {
  STsdbMeta *pMeta = (STsdbMeta *)pHandle;
  STsdbRepo *pRepo = (STsdbRepo *)pMeta->pRepo;

  for (int i = 1; i < pMeta->maxTables; i++) {
    STable *pTable = pMeta->tables[i];
    if (pTable != NULL && pTable->type == TSDB_CHILD_TABLE) {
      tsdbAddTableIntoIndex(pMeta, pTable);
    }
  }

  for (int i = 0; i < pMeta->maxTables; i++) {
    STable *pTable = pMeta->tables[i];
    if (pTable && pTable->type == TSDB_STREAM_TABLE) {
      pTable->cqhandle = (*pRepo->appH.cqCreateFunc)(pRepo->appH.cqH, i, pTable->sql, tsdbGetTableSchema(pMeta, pTable));
    }
  }
}

/**
 * Initialize the meta handle
 * ASSUMPTIONS: VALID PARAMETER
 */
STsdbMeta *tsdbInitMeta(char *rootDir, int32_t maxTables, void *pRepo) {
  STsdbMeta *pMeta = (STsdbMeta *)malloc(sizeof(STsdbMeta));
  if (pMeta == NULL) return NULL;

  pMeta->maxTables = maxTables;
  pMeta->nTables = 0;
  pMeta->superList = NULL;
  pMeta->tables = (STable **)calloc(maxTables, sizeof(STable *));
  pMeta->maxRowBytes = 0;
  pMeta->maxCols = 0;
  pMeta->pRepo = pRepo;
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
  STsdbRepo *pRepo = (STsdbRepo *)pMeta->pRepo;
  if (pMeta == NULL) return 0;

  tsdbCloseMetaFile(pMeta->mfh);

  for (int i = 1; i < pMeta->maxTables; i++) {
    if (pMeta->tables[i] != NULL) {
      STable *pTable = pMeta->tables[i];
      if (pTable->type == TSDB_STREAM_TABLE) (*pRepo->appH.cqDropFunc)(pTable->cqhandle);
      tsdbFreeTable(pTable);
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
  if (pTable->type == TSDB_NORMAL_TABLE || pTable->type == TSDB_SUPER_TABLE || pTable->type == TSDB_STREAM_TABLE) {
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

int32_t tsdbGetTableTagVal(TsdbRepoT* repo, STableId* id, int32_t colId, int16_t* type, int16_t* bytes, char** val) {
  STsdbMeta* pMeta = tsdbGetMeta(repo);
  STable* pTable = tsdbGetTableByUid(pMeta, id->uid);
  
  STSchema* pSchema = tsdbGetTableTagSchema(pMeta, pTable);
  STColumn* pCol = NULL;
  
  // todo binary search
  for(int32_t col = 0; col < schemaNCols(pSchema); ++col) {
    STColumn* p = schemaColAt(pSchema, col);
    if (p->colId == colId) {
      pCol = p;
      break;
    }
  }
  
  if (pCol == NULL) {
    return -1;  // No matched tags. Maybe the modification of tags has not been done yet.
  }
  
  SDataRow row = (SDataRow)pTable->tagVal;
  int16_t tagtype = 0;
  char* d = tdQueryTagByID(row, pCol->colId, &tagtype);
  //ASSERT((int8_t)tagtype == pCol->type)
  *val = d;
  *type  = pCol->type;
  *bytes = pCol->bytes;
  
  return TSDB_CODE_SUCCESS;
}

char* tsdbGetTableName(TsdbRepoT *repo, const STableId* id, int16_t* bytes) {
  STsdbMeta* pMeta = tsdbGetMeta(repo);
  STable* pTable = tsdbGetTableByUid(pMeta, id->uid);
  
  if (pTable == NULL) {
    if (bytes != NULL) {
      *bytes = 0;
    }
    
    return NULL;
  } else {
    if (bytes != NULL) {
      *bytes = varDataLen(pTable->name);
    }
    
    return (char*) pTable->name;
  }
}

static STable *tsdbNewTable(STableCfg *pCfg, bool isSuper) {
  STable *pTable = NULL;
  size_t  tsize = 0;

  pTable = (STable *)calloc(1, sizeof(STable));
  if (pTable == NULL) {
    terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
    goto _err;
  }

  pTable->type = pCfg->type;

  if (isSuper) {
    pTable->type = TSDB_SUPER_TABLE;
    pTable->tableId.uid = pCfg->superUid;
    pTable->tableId.tid = -1;
    pTable->superUid = TSDB_INVALID_SUPER_TABLE_ID;
    pTable->schema = tdDupSchema(pCfg->schema);
    pTable->tagSchema = tdDupSchema(pCfg->tagSchema);

    tsize = strnlen(pCfg->sname, TSDB_TABLE_NAME_LEN);
    pTable->name = calloc(1, tsize + VARSTR_HEADER_SIZE + 1);
    if (pTable->name == NULL) {
      terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
      goto _err;
    }
    STR_WITH_SIZE_TO_VARSTR(pTable->name, pCfg->sname, tsize);

    STColumn *pColSchema = schemaColAt(pTable->tagSchema, 0);
    pTable->pIndex = tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, pColSchema->type, pColSchema->bytes, 1, 0, 0,
                                     getTagIndexKey);  // Allow duplicate key, no lock
    if (pTable->pIndex == NULL) {
      terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
      goto _err;
    }
  } else {
    pTable->type = pCfg->type;
    pTable->tableId.uid = pCfg->tableId.uid;
    pTable->tableId.tid = pCfg->tableId.tid;
    pTable->lastKey = TSKEY_INITIAL_VAL;

    tsize = strnlen(pCfg->name, TSDB_TABLE_NAME_LEN);
    pTable->name = calloc(1, tsize + VARSTR_HEADER_SIZE + 1);
    if (pTable->name == NULL) {
      terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
      goto _err;
    }
    STR_WITH_SIZE_TO_VARSTR(pTable->name, pCfg->name, tsize);

    if (pCfg->type == TSDB_CHILD_TABLE) {
      pTable->superUid = pCfg->superUid;
      pTable->tagVal = tdDataRowDup(pCfg->tagValues);
    } else if (pCfg->type == TSDB_NORMAL_TABLE) {
      pTable->superUid = -1;
      pTable->schema = tdDupSchema(pCfg->schema);
    } else {
      ASSERT(pCfg->type == TSDB_STREAM_TABLE);
      pTable->superUid = -1;
      pTable->schema = tdDupSchema(pCfg->schema);
      pTable->sql = strdup(pCfg->sql);
    }
  }

  return pTable;

_err:
  tsdbFreeTable(pTable);
  return NULL;
}

int tsdbCreateTable(TsdbRepoT *repo, STableCfg *pCfg) {
  STsdbRepo *pRepo = (STsdbRepo *)repo;
  STsdbMeta *pMeta = pRepo->tsdbMeta;

  if (tsdbCheckTableCfg(pCfg) < 0) return -1;

  STable *pTable = tsdbGetTableByUid(pMeta, pCfg->tableId.uid);
  if (pTable != NULL) {
    tsdbError("vgId:%d table %s already exists, tid %d uid %" PRId64, pRepo->config.tsdbId, varDataVal(pTable->name),
              pTable->tableId.tid, pTable->tableId.uid);
    return TSDB_CODE_TABLE_ALREADY_EXIST;
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
    }
  }

  STable *table = tsdbNewTable(pCfg, false);
  if (table == NULL) {
    if (newSuper) {
      tsdbFreeTable(super);
      return -1;
    }
  }

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

// int32_t tsdbDropTableImpl(STsdbMeta *pMeta, STableId tableId) {
int tsdbDropTable(TsdbRepoT *repo, STableId tableId) {
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
  if (pTable == NULL) return 0;

  if (pTable->type == TSDB_CHILD_TABLE) {
    tdFreeTagRow(pTable->tagVal);
  } else {
    tdFreeSchema(pTable->schema);
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

static int32_t tsdbCheckTableCfg(STableCfg *pCfg) {
  // TODO
  return 0;
}

STable *tsdbGetTableByUid(STsdbMeta *pMeta, uint64_t uid) {
  void *ptr = taosHashGet(pMeta->map, (char *)(&uid), sizeof(uid));

  if (ptr == NULL) return NULL;

  return *(STable **)ptr;
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
      pTable->cqhandle = (*pRepo->appH.cqCreateFunc)(pRepo->appH.cqH, pTable->tableId.tid, pTable->sql, tsdbGetTableSchema(pMeta, pTable));
    }
    
    pMeta->nTables++;
  }

  // Update the pMeta->maxCols and pMeta->maxRowBytes
  if (pTable->type == TSDB_SUPER_TABLE || pTable->type == TSDB_NORMAL_TABLE) {
    if (schemaNCols(pTable->schema) > pMeta->maxCols) pMeta->maxCols = schemaNCols(pTable->schema);
    int bytes = dataRowMaxBytesFromSchema(pTable->schema);
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
  
  int16_t tagtype = 0;
  char* key = tdQueryTagByID(pTable->tagVal, pCol->colId, &tagtype);
  ASSERT(pCol->type == tagtype);
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


char *getTSTupleKey(const void * data) {
  SDataRow row = (SDataRow)data;
  return POINTER_SHIFT(row, TD_DATA_ROW_HEAD_SIZE);
}