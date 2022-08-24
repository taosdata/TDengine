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

#include "os.h"

typedef struct SRBTree     SRBTree;
typedef struct SRBTreeNode SRBTreeNode;
typedef struct SRBTreeIter SRBTreeIter;

typedef int32_t (*tRBTreeCmprFn)(void *, void *);

// SRBTree
#define tRBTreeCreate(compare) \
  (SRBTree) { .cmprFn = (compare), .root = NULL, .minNode = NULL, .maxNode = NULL }

SRBTreeNode *tRBTreePut(SRBTree *pTree, SRBTreeNode *pNew);
void         tRBTreeDrop(SRBTree *pTree, SRBTreeNode *pNode);
SRBTreeNode *tRBTreeDropByKey(SRBTree *pTree, void *pKey);
SRBTreeNode *tRBTreeGet(SRBTree *pTree, void *pKey);

// SRBTreeIter
#define tRBTreeIterCreate(tree) \
  (SRBTreeIter) { .pTree = (tree) }

SRBTreeNode *tRBTreeIterNext(SRBTreeIter *pIter);

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
  SRBTree *pTree;
};
