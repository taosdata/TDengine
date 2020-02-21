#include <stdlib.h>

#include "skiplist.h"

#define IS_VALID_SKIPLIST_DUPLICATE_KEY_STRATEGY(strategy) \
  (((strategy) >= SKIPLIST_ALLOW_DUPLICATE_KEY) && ((strategy) <= SKIPLIST_DISCARD_DUPLICATE_KEY))

SSkipListNode *tdCreateSkiplistNode(int32_t nlevels) {
  SSkipListNode *pNode = (SSkipListNode *)malloc(sizeof(SSkipListNode));
  if (pNode == NULL) return NULL;

  pNode->nexts = (struct _skiplist_node **)cmalloc(nlevels, sizeof(struct _skiplist_node *));
  if (pNode->nexts == NULL) {
    free(pNode);
    return NULL;
  }

  pNode->prevs = (struct _skiplist_node **)cmalloc(nlevels, sizeof(struct _skiplist_node *));
  if (pNode->nexts == NULL) {
    free(pNode->nexts);
    free(pNode);
    return NULL;
  }

  return pNode;
}

int32_t tdFreeSkiplistNode(SSkipListNode *pNode) {
    if (pNode == NULL) return 0;
    // TODO: free key and free value

    // Free the skip list
    free(pNode->nexts);
    free(pNode->prevs);
    free(pNode);
    return 0;
}

SSkipList *tdCreateSkiplist(int16_t nMaxLevels, SKIPLIST_DUPLICATE_KEY_STATEGY strategy) {
  // Check parameters
  if (!IS_VALID_SKIPLIST_DUPLICATE_KEY_STRATEGY(strategy)) return NULL;

  SSkipList *pSkipList = (SSkipList *)malloc(sizeof(SSkipList));
  if (pSkipList == NULL) {
    return NULL;
  }

  pSkipList->strategy = strategy;
  pSkipList->nMaxLevels = nMaxLevels;

  pSkipList->head = tdCreateSkiplistNode(nMaxLevels);
  if (pSkipList->head == NULL) {
    free(pSkipList);
    return NULL;
  }

  return pSkipList;
}

int32_t tdFreeSkipList(SSkipList *pSkipList) {
  if (pSkipList == NULL) return 0;

  SSkipListNode *pNode = pSkipList->head->nexts[0];
  while (pNode) {
      SSkipListNode *pTemp = pNode->nexts[0];
      tdFreeSkiplistNode(pNode);
      pNode = pTemp;
  }

  free(pSkipList);

  return 0;
}