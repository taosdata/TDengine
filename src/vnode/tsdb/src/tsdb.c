#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

#include "taosdef.h"
// #include "disk.h"
#include "tsdbCache.h"
#include "tsdbMeta.h"

typedef struct STSDBRepo {
  // TSDB configuration
  STSDBcfg *pCfg;

  // The meter meta handle of this TSDB repository
  SMetaHandle *pMetaHandle;

  // The cache Handle
  SCacheHandle *pCacheHandle;

  // Disk tier handle for multi-tier storage
  SDiskTier *pDiskTier;

  // File Store
  void *pFileStore;

  pthread_mutext_t tsdbMutex;

} STSDBRepo;

#define TSDB_GET_TABLE_BY_ID(pRepo, sid) (((STSDBRepo *)pRepo)->pTableList)[sid]
#define TSDB_GET_TABLE_BY_NAME(pRepo, name)

// Check the correctness of the TSDB configuration
static int32_t tsdbCheckCfg(STSDBCfg *pCfg) {
  // TODO
  return 0;
}

tsdb_repo_t *tsdbCreateRepo(STSDBCfg *pCfg, int32_t *error) {
  int32_t err = 0;
  err = tsdbCheckCfg(pCfg);
  if (err != 0) {
    // TODO: deal with the error here
  }

  STSDBRepo *pRepo = (STSDBRepo *)malloc(sizeof(STSDBRepo));
  if (pRepo == NULL) {
    // TODO: deal with error
    return NULL;
  }

  // TODO: Initailize pMetahandle
  pRepo->pMetaHandle = tsdbCreateMetaHandle(pCfg->maxTables);
  if (pRepo->pMetaHandle == NULL) {
    // TODO: deal with error
    free(pRepo);
    return NULL;
  }

  // TODO: Initialize cache handle
  pRepo->pCacheHandle = tsdbCreateCache(5);
  if (pRepo->pCacheHandle == NULL) {
    // TODO: free the object and return error
    return NULL;
  }

  return (tsdb_repo_t *)pRepo;
}

int32_t tsdbDropRepo(tsdb_repo_t *pRepo, int32_t *error) {
  STSDBRepo *pTRepo = (STSDBRepo *)pRepo;

  // TODO: Close the metaHandle

  // TODO: Close the cache

  return 0;
}

tsdb_repo_t *tsdbOpenRepo(char *tsdbDir, int32_t *error) {
  STSDBRepo *pRepo = (STSDBRepo *)malloc(sizeof(STSDBRepo));
  if (pRepo == NULL) {
    return NULL;
  }

  // TODO: Initialize configuration from the file

  {
    // TODO: Initialize the pMetaHandle
  }
  if (pRepo->pMetaHandle == NULL) {
    free(pRepo);
    return NULL;
  }

  {
    // TODO: initialize the pCacheHandle
  }
  if (pRepo->pCacheHandle == NULL) {
    // TODO: deal with error
    return NULL;
  }

  return (tsdb_repo_t *)pRepo;
}

int32_t tsdbCloseRepo(tsdb_repo_t *pRepo, int32_t *error) {
  STSDBRepo *pTRepo = (STSDBRepo *)pRepo;

  return 0;
}

int32_t tsdbConfigRepo(STSDBCfg *pCfg, int32_t *error) {
  // TODO
}

STSDBRepoInfo *tsdbGetStatus(tsdb_repo_t *pRepo, int32_t *error) {
  // TODO
}

int32_t tsdbCreateTable(tsdb_repo_t *pRepo, STableCfg *pCfg, int32_t *error) {
  // TODO
}

int32_t tsdbAlterTable(tsdb_repo_t *pRepo, STableCfg *pCfg, int32_t *error) {
  // TODO
}

STableInfo *tsdbGetTableInfo(tsdb_repo_t *pRepo, STableId tid, int32_t *error) {
  // TODO
}

int32_t tsdbInsertData(tsdb_repo_t *pRepo, STableId tid, char *pData, int32_t *error) {
  // TODO
}