#include <stdlib.h>

#include "tsdbCache.h"

STsdbCache *tsdbCreateCache(int32_t numOfBlocks) {
  STsdbCache *pCacheHandle = (STsdbCache *)malloc(sizeof(STsdbCache));
  if (pCacheHandle == NULL) {
    // TODO : deal with the error
    return NULL;
  }

  return pCacheHandle;
}

int32_t tsdbFreeCache(STsdbCache *pHandle) { return 0; }

void *tsdbAllocFromCache(STsdbCache *pCache, int64_t bytes) {
  // TODO: implement here
  return NULL;
}