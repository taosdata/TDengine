#include <stdlib.h>

// #include "taosdef.h"
#include "tskiplist.h"
#include "tsdb.h"
#include "taosdef.h"
#include "tsdbMeta.h"
#include "hash.h"
#include "tsdbCache.h"

#define TSDB_SUPER_TABLE_SL_LEVEL 5 // TODO: may change here
#define TSDB_META_FILE_NAME "META"

static int     tsdbFreeTable(STable *pTable);
static int32_t tsdbCheckTableCfg(STableCfg *pCfg);
static int     tsdbAddTableToMeta(STsdbMeta *pMeta, STable *pTable);
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

  void *ret = malloc(*contLen);
  if (ret == NULL) return NULL;

  // TODO: encode the object to the memory
  {}

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

  {
    // TODO recover from the binary content
  }

  return pTable;
}

void *tsdbFreeEncode(void *cont) {
  if (cont != NULL) free(cont);
}

/**
 * Initialize the meta handle
 * ASSUMPTIONS: VALID PARAMETER
 */
STsdbMeta *tsdbInitMeta(const char *rootDir, int32_t maxTables) {
  STsdbMeta *pMeta = (STsdbMeta *)malloc(sizeof(STsdbMeta));
  if (pMeta == NULL) return NULL;

  pMeta->maxTables = maxTables;
  pMeta->nTables = 0;
  pMeta->superList = NULL;
  pMeta->tables = (STable **)calloc(maxTables, sizeof(STable *));
  if (pMeta->tables == NULL) {
    free(pMeta);
    return NULL;
  }

  pMeta->map = taosInitHashTable(maxTables * TSDB_META_HASH_FRACTION, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false);
  if (pMeta->map == NULL) {
    free(pMeta->tables);
    free(pMeta);
    return NULL;
  }

  pMeta->mfh = tsdbInitMetaFile(rootDir, maxTables);
  if (pMeta->mfh == NULL) {
    taosCleanUpHashTable(pMeta->map);
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

int32_t tsdbCreateTableImpl(STsdbMeta *pMeta, STableCfg *pCfg) {
  if (tsdbCheckTableCfg(pCfg) < 0) return -1;

  STable *super = NULL;
  int newSuper = 0;

  if (pCfg->type == TSDB_STABLE) { 
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
      super->content.pIndex = tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1,
                                                0, NULL);  // Allow duplicate key, no lock

      if (super->content.pIndex == NULL) {
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

  STable *table = (STable *)malloc(sizeof(STable));
  if (table == NULL) {
    if (newSuper) tsdbFreeTable(super);
    return -1;
  }

  table->tableId = pCfg->tableId;
  if (IS_CREATE_STABLE(pCfg)) { // TSDB_STABLE
    table->type = TSDB_STABLE;
    table->superUid = pCfg->superUid;
    table->tagVal = tdDataRowDup(pCfg->tagValues);
  } else { // TSDB_NTABLE
    table->type = TSDB_NTABLE;
    table->superUid = -1;
    table->schema = tdDupSchema(pCfg->schema);
  }
  table->content.pData = tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, 0, 8, 0, 0, NULL);

  if (newSuper) tsdbAddTableToMeta(pMeta, super);
  tsdbAddTableToMeta(pMeta, table);

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
    if (pTable->type == TSDB_STABLE) {
      tsdbRemoveTableFromIndex(pMeta, pTable);
    }

    tsdbFreeTable(pTable);
  }

  return 0;
}

int32_t tsdbInsertRowToTableImpl(SSkipListNode *pNode, STable *pTable) {
  tSkipListPut(pTable->content.pData, pNode);
  return 0;
}

static int tsdbFreeTable(STable *pTable) {
  // TODO: finish this function
  if (pTable->type == TSDB_STABLE) {
    tdFreeDataRow(pTable->tagVal);
  } else {
    tdFreeSchema(pTable->schema);
  }

  // Free content
  if (TSDB_TABLE_IS_SUPER_TABLE(pTable)) {
    tSkipListDestroy(pTable->content.pIndex);
  } else {
    tSkipListDestroy(pTable->content.pData);
  }

  free(pTable);
  return 0;
}

static int32_t tsdbCheckTableCfg(STableCfg *pCfg) {
  // TODO
  return 0;
}

STable *tsdbGetTableByUid(STsdbMeta *pMeta, int64_t uid) {
  void *ptr = taosHashGet(pMeta->tableMap, (char *)(&uid), sizeof(uid));

  if (ptr == NULL) return NULL;

  return *(STable **)ptr;
}

static int tsdbAddTableToMeta(STsdbMeta *pMeta, STable *pTable) {
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
    if (pTable->type == TSDB_STABLE) {
      // add STABLE to the index
      tsdbAddTableIntoIndex(pMeta, pTable);
    }
    pMeta->nTables++;
  }

  return tsdbAddTableIntoMap(pMeta, pTable);
}

static int tsdbRemoveTableFromMeta(STsdbMeta *pMeta, STable *pTable) {
  // TODO
  return 0;
}

static int tsdbAddTableIntoMap(STsdbMeta *pMeta, STable *pTable) {
  // TODO: add the table to the map
  int64_t uid = pTable->tableId.uid;
  if (taosHashPut(pMeta->map, (char *)(&uid), sizeof(uid), (void *)(&pTable), sizeof(pTable)) < 0) {
    return -1;
  }
  return 0;
}
static int tsdbAddTableIntoIndex(STsdbMeta *pMeta, STable *pTable) {
  assert(pTable->type == TSDB_STABLE);
  // TODO
  return 0;
}

static int tsdbRemoveTableFromIndex(STsdbMeta *pMeta, STable *pTable) {
  assert(pTable->type == TSDB_STABLE);
  // TODO
  return 0;
}

static int tsdbEstimateTableEncodeSize(STable *pTable) {
  // TODO
  return 0;
}