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

typedef int32_t (*tRBTreeCmprFn)(const void *, const void *);

// SRBTree =============================================
#define tRBTreeCreate(compare) \
  (SRBTree) { .cmprFn = (compare), .rootNode = NULL, .minNode = NULL, .maxNode = NULL }

SRBTreeNode *tRBTreePut(SRBTree *pTree, SRBTreeNode *pNew);
void         tRBTreeDrop(SRBTree *pTree, SRBTreeNode *pNode);
SRBTreeNode *tRBTreeDropByKey(SRBTree *pTree, void *pKey);
SRBTreeNode *tRBTreeGet(SRBTree *pTree, void *pKey);

// SRBTreeIter =============================================
#define tRBTreeIterCreate(tree) \
  (SRBTreeIter) { .pTree = (tree), .pNode = (tree)->minNode }

SRBTreeNode *tRBTreeIterNext(SRBTreeIter *pIter);

// STRUCT =============================================
struct SRBTreeNode {
  enum { RED, BLACK } color;
  SRBTreeNode *parent;
  SRBTreeNode *left;
  SRBTreeNode *right;
  uint8_t      payload[];
};

struct SRBTree {
  tRBTreeCmprFn cmprFn;
  SRBTreeNode  *rootNode;
  SRBTreeNode  *minNode;
  SRBTreeNode  *maxNode;
};

struct SRBTreeIter {
  SRBTree     *pTree;
  SRBTreeNode *pNode;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_RBTREE_H_*/