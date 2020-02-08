#if !defined(_TD_CACHE_H_)
#define _TD_CACHE_H_

#define TD_MIN_CACHE_BLOCK_SIZE 1024*1024    /* 1M */
#define TD_MAX_CACHE_BLOCK_SIZE 64*1024*1024 /* 64M */

typedef void cache_pool_t;

typedef struct SCacheBlock
{
    int32_t blockId;
    char data[];
} SCacheBlock;



#endif // _TD_CACHE_H_
