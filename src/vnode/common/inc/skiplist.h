#if !defined(_TD_SKIPLIST_H)
#define _TD_SKIPLIST_H

#include <stdint.h>

typedef enum {
  SKIPLIST_ALLOW_DUPLICATE_KEY,
  SKIPLIST_REPLACE_DUPLICATE_KEY,
  SKIPLIST_DISCARD_DUPLICATE_KEY
} SKIPLIST_DUPLICATE_KEY_STATEGY;

typedef struct _skiplist_node {
  void *                  key;
  void *                  value;
  struct _skiplist_node **nexts;
  struct _skiplist_node **prevs;
} SSkipListNode;

// To implement a general skip list
typedef struct _skiplist {
  SKIPLIST_DUPLICATE_KEY_STATEGY strategy;
  SSkipListNode *                head;
  SSkipListNode *                tail;
  int32_t                        nMaxLevels;
  int32_t                        count;
} SSkipList;

// -- Operations on SSkipListNode
SSkipListNode *tdCreateSkiplistNode(int32_t nlevels);
int32_t tdFreeSkiplistNode(SSkipListNode *pNode);

// -- Operations on SSkipList
SSkipList *tdCreateSkiplist(int16_t nMaxLevels, SKIPLIST_DUPLICATE_KEY_STATEGY strategy); 
int32_t tdFreeSkipList(SSkipList *pSkipList);
//  int32_t    tdAddItemToSkiplist(SSkipList *slist, void *key, void *value);
// int32_t    tdAddNodeToSkiplist(SSkipList *slist, SSkipListNode *node);

#endif  // _TD_SKIPLIST_H
