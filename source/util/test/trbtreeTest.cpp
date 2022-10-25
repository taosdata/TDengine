#include <gtest/gtest.h>

#include <stdio.h>
#include <stdlib.h>

#include "trbtree.h"

static int32_t tCmprInteger(const void *p1, const void *p2) {
  if (*(int *)p1 < *(int *)p2) {
    return -1;
  } else if (*(int *)p1 > *(int *)p2) {
    return 1;
  }
  return 0;
}

TEST(trbtreeTest, rbtree_test1) {
#if 0
  SRBTree rt;
  tRBTreeCreate(&rt, tCmprInteger);
  int a[] = {1, 3, 4, 2, 7, 5, 8};

  for (int i = 0; i < sizeof(a) / sizeof(a[0]); i++) {
    SRBTreeNode *pNode = (SRBTreeNode *)taosMemoryMalloc(sizeof(*pNode) + sizeof(int));
    *(int *)pNode->payload = a[i];

    tRBTreePut(&rt, pNode);
  }

  SRBTreeIter  rti = tRBTreeIterCreate(&rt, 1);
  SRBTreeNode *pNode = tRBTreeIterNext(&rti);
  int          la = 0;
  while (pNode) {
    GTEST_ASSERT_GT(*(int *)pNode->payload, la);
    la = *(int *)pNode->payload;
    // printf("%d\n", la);
    pNode = tRBTreeIterNext(&rti);
  }
#endif
}