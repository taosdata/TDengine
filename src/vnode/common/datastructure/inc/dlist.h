// A doubly linked list
#if !defined(_TD_DLIST_H_)
#define _TD_DLIST_H_

#include <stdint.h>

typedef struct {
  SListNode *prev;
  SListNode *next;
  void *     data;
} SListNode;

// Doubly linked list
typedef struct {
    SListNode *head;
    SListNode *tail;
    int32_t length;
} SDList;

// ----- Set operation
#define TD_GET_DLIST_LENGTH(pDList) (((SDList *)pDList)->length)
#define TD_GET_DLIST_HEAD(pDList) (((SDList *)pDList)->head)
#define TD_GET_DLIST_TAIL(pDList) (((SDList *)pDList)->tail)

#define TD_GET_DLIST_NEXT_NODE(pDNode) (((SListNode *)pDNode)->next)
#define TD_GET_DLIST_PREV_NODE(pDNode) (((SListNode *)pDNode)->prev)
#define TD_GET_DLIST_NODE_DATA(pDNode) (((SListNode *)pDNode)->data)

#endif  // _TD_DLIST_H_
