/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef _TD_UTIL_RBTREE_H_
#define _TD_UTIL_RBTREE_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SRBTree     SRBTree;
typedef struct SRBTreeNode SRBTreeNode;
typedef struct SRBTreeIter SRBTreeIter;

typedef int32_t (*tRBTreeCmprFn)(const SRBTreeNode *, const SRBTreeNode *);

// SRBTree =============================================
#define tRBTreeMin(T) ((T)->min == ((T)->NIL) ? NULL : (T)->min)
#define tRBTreeMax(T) ((T)->max == ((T)->NIL) ? NULL : (T)->max)

void         tRBTreeCreate(SRBTree *pTree, tRBTreeCmprFn cmprFn);
void         tRBTreeClear(SRBTree *pTree);
SRBTreeNode *tRBTreePut(SRBTree *pTree, SRBTreeNode *z);
void         tRBTreeDrop(SRBTree *pTree, SRBTreeNode *z);
SRBTreeNode *tRBTreeDropByKey(SRBTree *pTree, void *pKey);
SRBTreeNode *tRBTreeDropMin(SRBTree *pTree);
SRBTreeNode *tRBTreeDropMax(SRBTree *pTree);
SRBTreeNode *tRBTreeGet(const SRBTree *pTree, const SRBTreeNode *pKeyNode);

// SRBTreeIter =============================================
#define tRBTreeIterCreate(tree, ascend) \
  (SRBTreeIter) { .asc = (ascend), .pTree = (tree), .pNode = (ascend) ? (tree)->min : (tree)->max }

SRBTreeNode *tRBTreeIterNext(SRBTreeIter *pIter);

// STRUCT =============================================
typedef enum { RED, BLACK } ECOLOR;
struct SRBTreeNode {
  ECOLOR       color;
  SRBTreeNode *parent;
  SRBTreeNode *left;
  SRBTreeNode *right;
};

struct SRBTree {
  tRBTreeCmprFn cmprFn;
  int64_t       n;
  SRBTreeNode  *root;
  SRBTreeNode  *min;
  SRBTreeNode  *max;
  SRBTreeNode  *NIL;
  SRBTreeNode   NILNODE;
};

struct SRBTreeIter {
  int8_t         asc;
  const SRBTree *pTree;
  SRBTreeNode   *pNode;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_RBTREE_H_*/