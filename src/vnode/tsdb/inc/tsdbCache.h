#if !defined(_TD_TSDBCACHE_H_)
#define _TD_TSDBCACHE_H_

#include <stdint.h>

#include "cache.h"

typedef struct STSDBCache {
    int64_t blockId;  // A block ID counter
    SCacheBlock *blockList;
} STSDBCache;


#endif // _TD_TSDBCACHE_H_
