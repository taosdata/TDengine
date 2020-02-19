#include <stdlib.h>

// #include "taosdef.h"
#include "tsdb.h"
#include "tsdbMeta.h"

SMetaHandle *tsdbCreateMetaHandle(int32_t numOfTables) {
  SMetaHandle *pMetahandle = (SMetaHandle *)malloc(sizeof(SMetaHandle));
  if (pMetahandle == NULL) {
    return NULL;
  }

  pMetahandle->numOfTables = 0;
  pMetahandle->numOfSuperTables = 0;
  pMetahandle->pTables = calloc(sizeof(STable *), numOfTables);
  if (pMetahandle->pTables == NULL) {
    free(pMetahandle);
    return NULL;
  }

  // TODO : initialize the map
  // pMetahandle->pNameTableMap = ;
  if (pMetahandle->pNameTableMap == NULL) {
    free(pMetahandle->pTables);
    free(pMetahandle);
    return NULL;
  }

  return pMetahandle;
}

int32_t tsdbFreeMetaHandle(SMetaHandle *pMetaHandle) {
  // TODO

}

static int32_t tsdbCheckTableCfg(STableCfg *pCfg) { return 0; }

int32_t tsdbCreateTableImpl(SMetaHandle *pHandle, STableCfg *pCfg) {
  if (tsdbCheckTableCfg(pCfg) < 0) {
    return -1;
  }

  // TODO:
  STable *pTable = (STable *)malloc(sizeof(STable));
  if (pTable == NULL) {
    return -1;
  }

  pHandle->pTables[pCfg->tableId] = pTable;

  // TODO: add name to it
  return 0;
}

SMetaHandle * tsdbOpenMetaHandle(char *tsdbDir) {
  // Open meta file for reading

  SMetaHandle *pHandle = (SMetaHandle *)malloc(sizeof(SMetaHandle));
  if (pHandle == NULL) {
    return NULL;
  }

  return pHandle;
}