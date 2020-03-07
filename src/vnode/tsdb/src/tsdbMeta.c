#include <stdlib.h>

// #include "taosdef.h"
#include "tskiplist.h"
#include "tsdb.h"
#include "taosdef.h"
#include "tsdbMeta.h"
#include "hash.h"
#include "tsdbCache.h"

#define TSDB_SUPER_TABLE_SL_LEVEL 5 // TODO: may change here

static int     tsdbFreeTable(STable *pTable);
static int32_t tsdbCheckTableCfg(STableCfg *pCfg);
static int     tsdbAddTableToMeta(STsdbMeta *pMeta, STable *pTable);
static int     tsdbAddTableIntoMap(STsdbMeta *pMeta, STable *pTable);
static int     tsdbAddTableIntoIndex(STsdbMeta *pMeta, STable *pTable);
static int     tsdbRemoveTableFromIndex(STsdbMeta *pMeta, STable *pTable);

STsdbMeta *tsdbCreateMeta(int32_t maxTables) {
  STsdbMeta *pMeta = (STsdbMeta *)malloc(sizeof(STsdbMeta));
  if (pMeta == NULL) {
    return NULL;
  }

  pMeta->maxTables = maxTables;
  pMeta->nTables = 0;
  pMeta->stables = NULL;
  pMeta->tables = (STable **)calloc(maxTables, sizeof(STable *));
  if (pMeta->tables == NULL) {
    free(pMeta);
    return NULL;
  }

  pMeta->tableMap = taosInitHashTable(maxTables + maxTables / 10, taosGetDefaultHashFunction, false);
  if (pMeta->tableMap == NULL) {
    free(pMeta->tables);
    free(pMeta);
    return NULL;
  }

  return pMeta;
}

int32_t tsdbFreeMeta(STsdbMeta *pMeta) {
  if (pMeta == NULL) return 0;

  for (int i = 0; i < pMeta->maxTables; i++) {
    if (pMeta->tables[i] != NULL) {
      tsdbFreeTable(pMeta->tables[i]);
    }
  }

  free(pMeta->tables);

  STable *pTable = pMeta->stables;
  while (pTable != NULL) {
    STable *pTemp = pTable;
    pTable = pTemp->next;
    tsdbFreeTable(pTemp);
  }

  taosCleanUpHashTable(pMeta->tableMap);

  free(pMeta);

  return 0;
}

int32_t tsdbCreateTableImpl(STsdbMeta *pMeta, STableCfg *pCfg) {
  if (tsdbCheckTableCfg(pCfg) < 0) {
    return -1;
  }

  STable *pSTable = NULL;
  int newSuper = 0;

  if (IS_CREATE_STABLE(pCfg)) {  // to create a TSDB_STABLE, check if super table exists
    pSTable = tsdbGetTableByUid(pMeta, pCfg->stableUid);
    if (pSTable == NULL) {  // super table not exists, try to create it
      newSuper = 1;
      pSTable = (STable *)calloc(1, sizeof(STable));
      if (pSTable == NULL) return -1;

      pSTable->tableId.uid = pCfg->stableUid;
      pSTable->tableId.tid = -1;
      pSTable->type = TSDB_SUPER_TABLE;
      // pSTable->createdTime = pCfg->createdTime; // The created time is not required
      pSTable->stableUid = -1;
      pSTable->numOfCols = pCfg->numOfCols;
      pSTable->pSchema = tdDupSchema(pCfg->schema);
      pSTable->content.pIndex = tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1,
                                                0, NULL);  // Allow duplicate key, no lock
      if (pSTable->content.pIndex == NULL) {
        free(pSTable);
        return -1;
      }
    } else {
      if (pSTable->type != TSDB_SUPER_TABLE) return -1;
    }
  }

  STable *pTable = (STable *)malloc(sizeof(STable));
  if (pTable == NULL) {
    if (newSuper) tsdbFreeTable(pSTable);
    return -1;
  }

  pTable->tableId = pCfg->tableId;
  pTable->createdTime = pCfg->createdTime;
  if (IS_CREATE_STABLE(pCfg)) { // TSDB_STABLE
    pTable->type = TSDB_STABLE;
    pTable->stableUid = pCfg->stableUid;
    pTable->pTagVal = tdDataRowDup(pCfg->tagValues);
  } else { // TSDB_NTABLE
    pTable->type = TSDB_NTABLE;
    pTable->stableUid = -1;
    pTable->pSchema = tdDupSchema(pCfg->schema);
  }
  pTable->content.pData = tSkipListCreate(TSDB_SUPER_TABLE_SL_LEVEL, 0, 8, 0, 0, NULL);

  if (newSuper) tsdbAddTableToMeta(pMeta, pSTable);
  tsdbAddTableToMeta(pMeta, pTable);

  return 0;
}

STsdbMeta *tsdbOpenMeta(char *tsdbDir) {
  // TODO : Open meta file for reading

  STsdbMeta *pMeta = (STsdbMeta *)malloc(sizeof(STsdbMeta));
  if (pMeta == NULL) {
    return NULL;
  }

  return pMeta;
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
    tdFreeDataRow(pTable->pTagVal);
  } else {
    tdFreeSchema(pTable->pSchema);
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
  void *ptr = taosGetDataFromHashTable(pMeta->tableMap, (char *)(&uid), sizeof(uid));

  if (ptr == NULL) return NULL;

  return *(STable **)ptr;
}

static int tsdbAddTableToMeta(STsdbMeta *pMeta, STable *pTable) {
  if (pTable->type == TSDB_SUPER_TABLE) { 
    // add super table to the linked list
    if (pMeta->stables == NULL) {
      pMeta->stables = pTable;
      pTable->next = NULL;
    } else {
      STable *pTemp = pMeta->stables;
      pMeta->stables = pTable;
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
  if (taosAddToHashTable(pMeta->tableMap, (char *)(&uid), sizeof(uid), (void *)(&pTable), sizeof(pTable)) < 0) {
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