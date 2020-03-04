#include <stdlib.h>

// #include "taosdef.h"
#include "hash.h"
#include "tskiplist.h"
#include "tsdb.h"
#include "tsdbMeta.h"

#define TSDB_MIN_TABLES 10
#define TSDB_MAX_TABLES 100000
#define TSDB_DEFAULT_NSTABLES 10

#define IS_VALID_MAX_TABLES(maxTables) (((maxTables) >= TSDB_MIN_TABLES) && ((maxTables) <= TSDB_MAX_TABLES))

static int     tsdbFreeTable(STable *pTable);
static int32_t tsdbCheckTableCfg(STableCfg *pCfg);
static STable *tsdbGetTableByUid(int64_t uid);
static int     tsdbAddTable(STsdbMeta *pMeta, STable *pTable);
static int     tsdbAddTableIntoMap(STsdbMeta *pMeta, STable *pTable);
static int     tsdbAddTableIntoIndex(STsdbMeta *pMeta, STable *pTable);

STsdbMeta *tsdbCreateMeta(int32_t maxTables) {
  if (!IS_VALID_MAX_TABLES(maxTables)) return NULL;

  STsdbMeta *pMeta = (STsdbMeta *)malloc(sizeof(STsdbMeta));
  if (pMeta == NULL) {
    return NULL;
  }

  pMeta->maxTables = maxTables;
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

  if (pCfg->stableUid > 0) {  // to create a TSDB_STABLE
    pSTable = tsdbGetTableByUid(pCfg->stableUid);
    if (pSTable == NULL) {  // super table not exists, try to create it
      pSTable = (STable *)calloc(1, sizeof(STable));
      if (pSTable == NULL) return -1;

      pSTable->tableId.uid = pCfg->stableUid;
      pSTable->tableId.tid = -1;
      pSTable->type = TSDB_SUPER_TABLE;
      pSTable->createdTime = pCfg->createdTime; // The created time is not required
      pSTable->stableUid = -1;
      pSTable->numOfCols = pCfg->numOfCols;
      pSTable->pSchema = tdDupSchema(pCfg->schema);
      pSTable->content.pIndex = tSkipListCreate(5, 0, 10); // TODO: change here
      tsdbAddTable(pMeta, pSTable);
    } else {
      if (pSTable->type != TSDB_SUPER_TABLE) return NULL;
    }
  }

  STable *pTable = (STable *)malloc(sizeof(STable));
  if (pTable == NULL) {
    return -1;
  }

  pTable->tableId = pCfg->tableId;
  pTable->createdTime = pCfg->createdTime;
  if (1 /* */) { // TSDB_STABLE
    pTable->type = TSDB_STABLE;
    pTable->stableUid = pCfg->stableUid;
    pTable->pTagVal = tdSDataRowDup(pCfg->tagValues);
  } else { // TSDB_NTABLE
    pTable->type = TSDB_NTABLE;
    pTable->stableUid = -1;
    pTable->pSchema = tdDupSchema(pCfg->schema);
  }
  pTable->content.pData = tSkipListCreate(5, 0, 10); // TODO: change here

  tsdbAddTable(pMeta, pTable);

  return 0;
}

STsdbMeta *tsdbOpenMetaHandle(char *tsdbDir) {
  // Open meta file for reading

  STsdbMeta *pMeta = (STsdbMeta *)malloc(sizeof(STsdbMeta));
  if (pMeta == NULL) {
    return NULL;
  }

  return pMeta;
}

int32_t tsdbInsertDataImpl(STsdbMeta *pMeta, STableId tableId, char *pData) {
  STable *pTable = pMeta->tables[tableId.tid];
  if (pTable == NULL) {
    // TODO: deal with the error here
    return 0;
  }

  if (pTable->tableId.uid != tableId.uid) {
    // TODO: deal with the error here
    return 0;
  }

  return 0;
}

static int tsdbFreeTable(STable *pTable) { return 0; }

static int32_t tsdbCheckTableCfg(STableCfg *pCfg) { return 0; }

static STable *tsdbGetTableByUid(int64_t uid) { return NULL; }

static int tsdbAddTable(STsdbMeta *pMeta, STable *pTable) {
  if (pTable->type == TSDB_SUPER_TABLE) {
    if (pMeta->stables == NULL) {
      pMeta->stables = pTable;
      pTable->next = NULL;
    } else {
      STable *pTemp = pMeta->stables;
      pMeta->stables = pTable;
      pTable->next = pTemp;
    }
  } else {
    pMeta->tables[pTable->tableId.tid] = pTable;
    if (pTable->type == TSDB_STABLE) {
      tsdbAddTableIntoIndex(pMeta, pTable);
    }
  }

  return tsdbAddTableIntoMap(pMeta, pTable);
}

static int tsdbAddTableIntoMap(STsdbMeta *pMeta, STable *pTable) {
  // TODO: add the table to the map
  return 0;
}
static int tsdbAddTableIntoIndex(STsdbMeta *pMeta, STable *pTable) {
  // TODO
  return 0;
}