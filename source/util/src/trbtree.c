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

typedef int32_t (*tRBTreeCmprFn)(void *, void *);

typedef struct SRBTree     SRBTree;
typedef struct SRBTreeNode SRBTreeNode;
typedef struct SRBTreeIter SRBTreeIter;

struct SRBTreeNode {
  enum { RED, BLACK } color;
  SRBTreeNode *parent;
  SRBTreeNode *left;
  SRBTreeNode *right;
  uint8_t      payload[];
};

struct SRBTree {
  tRBTreeCmprFn cmprFn;
  SRBTreeNode  *root;
};

struct SRBTreeIter {
  SRBTree *pTree;
};

#define RBTREE_NODE_COLOR(N) ((N) ? (N)->color : BLACK)

// APIs ================================================
static void tRBTreeRotateLeft(SRBTree *pTree, SRBTreeNode *pNode) {
  SRBTreeNode *right = pNode->right;

  pNode->right = right->left;
  if (pNode->right) {
    pNode->right->parent = pNode;
  }

  right->parent = pNode->parent;
  if (pNode->parent == NULL) {
    pTree->root = right;
  } else if (pNode == pNode->parent->left) {
    pNode->parent->left = right;
  } else {
    pNode->parent->right = right;
  }

  right->left = pNode;
  pNode->parent = right;
}

static void tRBTreeRotateRight(SRBTree *pTree, SRBTreeNode *pNode) {
  SRBTreeNode *left = pNode->left;

  pNode->left = left->right;
  if (pNode->left) {
    pNode->left->parent = pNode;
  }

  left->parent = pNode->parent;
  if (pNode->parent == NULL) {
    pTree->root = left;
  } else if (pNode == pNode->parent->left) {
    pNode->parent->left = left;
  } else {
    pNode->parent->right = left;
  }

  left->right = pNode;
  pNode->parent = left;
}

#define tRBTreeCreate(compare) \
  (SRBTree) { .cmprFn = (compare), .root = NULL }

SRBTreeNode *tRBTreePut(SRBTree *pTree, SRBTreeNode *pNew) {
  pNew->left = NULL;
  pNew->right = NULL;
  pNew->color = RED;

  // insert
  if (pTree->root == NULL) {
    pNew->parent = NULL;
    pTree->root = pNew;
  } else {
    SRBTreeNode *pNode = pTree->root;
    while (true) {
      ASSERT(pNode);

      int32_t c = pTree->cmprFn(pNew->payload, pNode->payload);
      if (c < 0) {
        if (pNode->left) {
          pNode = pNode->left;
        } else {
          pNew->parent = pNode;
          pNode->left = pNew;
          break;
        }
      } else if (c > 0) {
        if (pNode->right) {
          pNode = pNode->right;
        } else {
          pNew->parent = pNode;
          pNode->right = pNew;
          break;
        }
      } else {
        return NULL;
      }
    }
  }

  // fix
  SRBTreeNode *pNode = pNew;
  while (pNode->parent && pNode->parent->color == RED) {
    SRBTreeNode *p = pNode->parent;
    SRBTreeNode *g = p->parent;

    if (p == g->left) {
      SRBTreeNode *u = g->right;

      if (RBTREE_NODE_COLOR(u) == RED) {
        p->color = BLACK;
        u->color = BLACK;
        g->color = RED;
        pNode = g;
      } else {
        if (pNode == p->right) {
          pNode = p;
          tRBTreeRotateLeft(pTree, pNode);
        }
        pNode->parent->color = BLACK;
        pNode->parent->parent->color = RED;
        tRBTreeRotateRight(pTree, pNode->parent->parent);
      }
    } else {
      SRBTreeNode *u = g->left;

      if (RBTREE_NODE_COLOR(u) == RED) {
        p->color = BLACK;
        u->color = BLACK;
        g->color = RED;
      } else {
        if (pNode == p->left) {
          pNode = p;
          tRBTreeRotateRight(pTree, pNode);
        }
        pNode->parent->color = BLACK;
        pNode->parent->parent->color = RED;
        tRBTreeRotateLeft(pTree, pNode->parent->parent);
      }
    }
  }

  pTree->root->color = BLACK;
  return pNew;
}

SRBTreeNode *tRBTreeDrop(SRBTree *pTree, void *pKey) {
  SRBTreeNode *pNode = pTree->root;

  while (pNode) {
    int32_t c = pTree->cmprFn(pKey, pNode->payload);

    if (c < 0) {
      pNode = pNode->left;
    } else if (c > 0) {
      pNode = pNode->right;
    } else {
      break;
    }
  }

  if (pNode) {
    // TODO
  }

  return pNode;
}

SRBTreeNode *tRBTreeGet(SRBTree *pTree, void *pKey) {
  SRBTreeNode *pNode = pTree->root;

  while (pNode) {
    int32_t c = pTree->cmprFn(pKey, pNode->payload);

    if (c < 0) {
      pNode = pNode->left;
    } else if (c > 0) {
      pNode = pNode->right;
    } else {
      break;
    }
  }

  return pNode;
}
