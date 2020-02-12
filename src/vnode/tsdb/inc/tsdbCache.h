#if !defined(_TD_TSDBCACHE_H_)
#define _TD_TSDBCACHE_H_

#include <stdint.h>

#include "cache.h"

#define TSDB_DEFAULT_CACHE_BLOCK_SIZE 16*1024*1024 /* 16M */

typedef struct {
  int64_t skey;     // start key
  int64_t ekey;     // end key
  int32_t numOfRows // numOfRows
} STableCacheInfo;

typedef struct {
  char *pData;
  STableCacheInfo *pTableInfo;
  SCacheBlock *prev;
  SCacheBlock *next;
} STSDBCacheBlock;

// Use a doublely linked list to implement this
typedef struct STSDBCache {
  // Number of blocks the cache is allocated
  int32_t numOfBlocks;
  STSDBCacheBlock *cacheList;
  void *  current;
} SCacheHandle;


// ---- Operation on STSDBCacheBlock
#define TSDB_CACHE_BLOCK_DATA(pBlock) ((pBlock)->pData)
#define TSDB_CACHE_AVAIL_SPACE(pBlock) ((char *)((pBlock)->pTableInfo) - ((pBlock)->pData))
#define TSDB_TABLE_INFO_OF_CACHE(pBlock, tableId) ((pBlock)->pTableInfo)[tableId]
#define TSDB_NEXT_CACHE_BLOCK(pBlock) ((pBlock)->next)
#define TSDB_PREV_CACHE_BLOCK(pBlock) ((pBlock)->prev)

SCacheHandle *tsdbCreateCache(int32_t numOfBlocks);

#endif  // _TD_TSDBCACHE_H_
