#include <stdlib.h>

// #include "taosdef.h"
#include "tsdb.h"
#include "tsdbMeta.h"

STsdbMeta *tsdbCreateMeta(int32_t maxTables) {
  STsdbMeta *pMeta = (STsdbMeta *)malloc(sizeof(STsdbMeta));
  if (pMeta == NULL) {
    return NULL;
  }

  pMeta->numOfTables = 0;
  pMeta->numOfSuperTables = 0;
  pMeta->pTables = calloc(sizeof(STable *), maxTables);
  if (pMeta->pTables == NULL) {
    free(pMeta);
    return NULL;
  }

  // TODO : initialize the map
  // pMetahandle->pNameTableMap = ;
  if (pMeta->pNameTableMap == NULL) {
    free(pMeta->pTables);
    free(pMeta);
    return NULL;
  }

  return pMeta;
}

int32_t tsdbFreeMeta(STsdbMeta *pMeta) {
}

static int32_t tsdbCheckTableCfg(STableCfg *pCfg) { return 0; }

int32_t tsdbCreateTableImpl(STsdbMeta *pHandle, STableCfg *pCfg) {
  if (tsdbCheckTableCfg(pCfg) < 0) {
    return -1;
  }

  // TODO:
  STable *pTable = (STable *)malloc(sizeof(STable));
  if (pTable == NULL) {
    return -1;
  }

  pHandle->pTables[pCfg->tableId.tid] = pTable;

  // TODO: add name to it
  return 0;
}

STsdbMeta * tsdbOpenMetaHandle(char *tsdbDir) {
  // Open meta file for reading

  STsdbMeta *pHandle = (STsdbMeta *)malloc(sizeof(STsdbMeta));
  if (pHandle == NULL) {
    return NULL;
  }

  return pHandle;
}