#include <stdlib.h>

#include "tsdbCache.h"


SCacheHandle *tsdbCreateCache(int32_t numOfBlocks) {
    SCacheHandle *pCacheHandle = (SCacheHandle *)malloc(sizeof(SCacheHandle));
    if (pCacheHandle == NULL) {
        // TODO : deal with the error
        return NULL;
    }

    return pCacheHandle;

}