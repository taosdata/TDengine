#if !defined(_TD_LIST_H_)
#define _TD_LIST_H_

#include <stdint.h>

typedef enum { TD_LIST_ORDERED, TD_LIST_UNORDERED } TLIST_TYPE;

typedef int32_t (* comparefn(void *key1, void *key2));

struct _list_type {
    TLIST_TYPE type;
};

typedef struct _list_node {
} SListNode;

typedef struct _list {
} SList;

#endif  // _TD_LIST_H_
