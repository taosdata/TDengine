#if !defined(_TD_TSDBCACHE_H_)
#define _TD_TSDBCACHE_H_

#include <stdint.h>

#include "cache.h"
#include "dlist.h"

typedef struct {
  int64_t      blockId;
  SCacheBlock *pBlock
} STSDBCacheBlock;

// Use a doublely linked list to implement this
typedef struct STSDBCache {
  int64_t blockId;  // A block ID counter
  SDList *cacheList;
} STSDBCache;

STSDBCache *tsdbCreateCache();

#endif  // _TD_TSDBCACHE_H_
