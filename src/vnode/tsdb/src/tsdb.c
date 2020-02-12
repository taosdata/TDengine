#include <stdint.h>
#include <pthread.h>
#include <stdlib.h>

#include "tsdb.h"
// #include "disk.h"
#include "tsdbMeta.h"
#include "tsdbCache.h"

typedef struct STSDBRepo
{
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

    STSDBRepo *pRepo = (STSDBRepo *) malloc(sizeof(STSDBRepo));
    if (pRepo == NULL) {
        // TODO: deal with error
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