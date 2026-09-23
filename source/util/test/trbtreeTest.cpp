/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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