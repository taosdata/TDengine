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

#include "trbtree.h"

#define RBTREE_NODE_COLOR(N) ((N) ? (N)->color : BLACK)

// SRBTree ================================================
static void tRBTreeRotateLeft(SRBTree *pTree, SRBTreeNode *pNode) {
  SRBTreeNode *right = pNode->right;

  pNode->right = right->left;
  if (pNode->right) {
    pNode->right->parent = pNode;
  }

  right->parent = pNode->parent;
  if (pNode->parent == NULL) {
    pTree->rootNode = right;
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
    pTree->rootNode = left;
  } else if (pNode == pNode->parent->left) {
    pNode->parent->left = left;
  } else {
    pNode->parent->right = left;
  }

  left->right = pNode;
  pNode->parent = left;
}

static SRBTreeNode *tRBTreeSuccessor(SRBTreeNode *pNode) {
  if (pNode->right) {
    pNode = pNode->right;
    while (pNode->left) {
      pNode = pNode->left;
    }
  } else {
    while (true) {
      if (pNode->parent) {
        if (pNode == pNode->parent->left) {
          pNode = pNode->parent;
          break;
        } else {
          pNode = pNode->parent;
        }
      } else {
        pNode = NULL;
        break;
      }
    }
  }

  return pNode;
}

static SRBTreeNode *tRBTreePredecessor(SRBTreeNode *pNode) {
  if (pNode->left) {
    pNode = pNode->left;
    while (pNode->right) {
      pNode = pNode->right;
    }
  } else {
    while (true) {
      if (pNode->parent) {
        if (pNode == pNode->parent->right) {
          pNode = pNode->parent;
          break;
        } else {
          pNode = pNode->parent;
        }
      } else {
        pNode = NULL;
        break;
      }
    }
  }
  return NULL;
}

SRBTreeNode *tRBTreePut(SRBTree *pTree, SRBTreeNode *pNew) {
  pNew->left = NULL;
  pNew->right = NULL;
  pNew->color = RED;

  // insert
  if (pTree->rootNode == NULL) {
    pNew->parent = NULL;
    pTree->rootNode = pNew;
  } else {
    SRBTreeNode *pNode = pTree->rootNode;
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
  pTree->rootNode->color = BLACK;

  // update min/max node
  if (pTree->minNode == NULL || pTree->cmprFn(pTree->minNode->payload, pNew->payload) > 0) {
    pTree->minNode = pNew;
  }
  if (pTree->maxNode == NULL || pTree->cmprFn(pTree->maxNode->payload, pNew->payload) < 0) {
    pTree->maxNode = pNew;
  }

  return pNew;
}

void tRBTreeDrop(SRBTree *pTree, SRBTreeNode *pNode) {
  // update min/max node
  if (pTree->minNode == pNode) {
    pTree->minNode = tRBTreeSuccessor(pTree->minNode);
  }
  if (pTree->maxNode == pNode) {
    pTree->maxNode = tRBTreePredecessor(pTree->maxNode);
  }

  // drop impl
  if (pNode->left == NULL) {
    // transplant right
    if (pNode->parent == NULL) {
      pTree->rootNode = pNode->right;
    } else if (pNode == pNode->parent->left) {
      pNode->parent->left = pNode->right;
    } else {
      pNode->parent->right = pNode->right;
    }

    if (pNode->right) {
      pNode->right->parent = pNode->parent;
    }
  } else if (pNode->right == NULL) {
    // transplant left
    if (pNode->parent == NULL) {
      pTree->rootNode = pNode->left;
    } else if (pNode == pNode->parent->left) {
      pNode->parent->left = pNode->left;
    } else {
      pNode->parent->right = pNode->left;
    }

    if (pNode->left) {
      pNode->left->parent = pNode->parent;
    }
  } else {
    SRBTreeNode *y = tRBTreeSuccessor(pNode);
    pNode->color = RBTREE_NODE_COLOR(y);
  }

  // fix
  if (pNode->color == BLACK) {
    // TODO
  }
}

SRBTreeNode *tRBTreeDropByKey(SRBTree *pTree, void *pKey) {
  SRBTreeNode *pNode = tRBTreeGet(pTree, pKey);

  if (pNode) {
    tRBTreeDrop(pTree, pNode);
  }

  return pNode;
}

SRBTreeNode *tRBTreeGet(SRBTree *pTree, void *pKey) {
  SRBTreeNode *pNode = pTree->rootNode;

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

// SRBTreeIter ================================================
SRBTreeNode *tRBTreeIterNext(SRBTreeIter *pIter) {
  SRBTreeNode *pNode = pIter->pNode;

  if (pIter->pNode) {
    if (pIter->asc) {
      // ascend
      pIter->pNode = tRBTreeSuccessor(pIter->pNode);
    } else {
      // descend
      pIter->pNode = tRBTreePredecessor(pIter->pNode);
    }
  }

_exit:
  return pNode;
}