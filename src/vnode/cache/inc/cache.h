#if !defined(_TD_CACHE_H_)
#define _TD_CACHE_H_

typedef void cache_pool_t;

typedef struct SCacheBlock
{
    SCacheBlock *next;
    SCacheBlock *prev;
    char data[];
} SCacheBlock;



#endif // _TD_CACHE_H_
