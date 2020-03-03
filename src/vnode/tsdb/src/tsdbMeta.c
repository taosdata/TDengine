#include <stdlib.h>

// #include "taosdef.h"
#include "tsdb.h"
#include "tsdbMeta.h"

#define TSDB_MIN_TABLES 10
#define TSDB_MAX_TABLES 100000
#define TSDB_DEFAULT_NSTABLES 10

#define IS_VALID_MAX_TABLES(maxTables) (((maxTables) >= TSDB_MIN_TABLES) && ((maxTables) >= TSDB_MAX_TABLES))

static int tsdbFreeTable(STable *pTable);

STsdbMeta *tsdbCreateMeta(int32_t maxTables) {
  if (!IS_VALID_MAX_TABLES(maxTables)) return NULL;

  STsdbMeta *pMeta = (STsdbMeta *)malloc(sizeof(STsdbMeta));
  if (pMeta == NULL) {
    return NULL;
  }

  pMeta->maxTables = maxTables;
  pMeta->numOfSuperTables = 0;
  pMeta->stables = NULL;
  pMeta->tables = (STable **)calloc(maxTables, sizeof(STable *));
  if (pMeta->tables == NULL) {
    free(pMeta);
    return NULL;
  }

  // TODO : initialize the map
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
  // TODO close the map

  free(pMeta);

  return 0;
}

static int32_t tsdbCheckTableCfg(STableCfg *pCfg) { return 0; }

int32_t tsdbCreateTableImpl(STsdbMeta *pMeta, STableCfg *pCfg) {
  if (tsdbCheckTableCfg(pCfg) < 0) {
    return -1;
  }

  // TODO:
  STable *pTable = (STable *)malloc(sizeof(STable));
  if (pTable == NULL) {
    return -1;
  }

  pMeta->tables[pCfg->tableId.tid] = pTable;

  // TODO: add name to it
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

static int tsdbFreeTable(STable *pTable) { return 0; }