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
static void tRBTreeRotateLeft(SRBTree *pTree, SRBTreeNode *x) {
  SRBTreeNode *y = x->right;
  x->right = y->left;
  if (y->left) {
    y->left->parent = x;
  }
  y->parent = x->parent;
  if (x->parent == NULL) {
    pTree->rootNode = y;
  } else if (x == x->parent->left) {
    x->parent->left = y;
  } else {
    x->parent->right = y;
  }
  y->left = x;
  x->parent = y;
}

static void tRBTreeRotateRight(SRBTree *pTree, SRBTreeNode *x) {
  SRBTreeNode *y = x->left;
  x->left = y->right;
  if (y->right) {
    y->right->parent = x;
  }
  y->parent = x->parent;
  if (x->parent == NULL) {
    pTree->rootNode = y;
  } else if (x == x->parent->left) {
    x->parent->left = y;
  } else {
    x->parent->right = y;
  }
  y->right = x;
  x->parent = y;
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

static void tRBTreeTransplant(SRBTree *pTree, SRBTreeNode *u, SRBTreeNode *v) {
  if (u->parent == NULL) {
    pTree->rootNode = v;
  } else if (u == u->parent->left) {
    u->parent->left = v;
  } else {
    u->parent->right = v;
  }
  if (v) {
    v->parent = u->parent;
  }
}

static void tRBTreeDropFixup(SRBTree *t, SRBTreeNode *x) {
  while (x != t->rootNode && x->color == BLACK) {
    if (x == x->parent->left) {
      SRBTreeNode *w = x->parent->right;
      if (RBTREE_NODE_COLOR(w) == RED) {
        w->color = BLACK;
        x->parent->color = RED;
        tRBTreeRotateLeft(t, x->parent);
        w = x->parent->right;
      }
      if (RBTREE_NODE_COLOR(w->left) == BLACK && RBTREE_NODE_COLOR(w->right) == BLACK) {
        w->color = RED;
        x = x->parent;
      } else {
        if (RBTREE_NODE_COLOR(w->right) == BLACK) {
          w->left->color = BLACK;
          w->color = RED;
          tRBTreeRotateRight(t, w);
          w = x->parent->right;
        }
        w->color = x->parent->color;
        x->parent->color = BLACK;
        w->right->color = BLACK;
        tRBTreeRotateLeft(t, x->parent);
        x = t->rootNode;
      }
    } else {
      SRBTreeNode *w = x->parent->left;
      if (RBTREE_NODE_COLOR(w) == RED) {
        w->color = BLACK;
        x->parent->color = RED;
        tRBTreeRotateRight(t, x->parent);
        w = x->parent->left;
      }
      if (RBTREE_NODE_COLOR(w->right) == BLACK && RBTREE_NODE_COLOR(w->left) == BLACK) {
        w->color = RED;
        x = x->parent;
      } else {
        if (RBTREE_NODE_COLOR(w->left) == BLACK) {
          w->right->color = BLACK;
          w->color = RED;
          tRBTreeRotateLeft(t, w);
          w = x->parent->left;
        }
        w->color = x->parent->color;
        x->parent->color = BLACK;
        w->left->color = BLACK;
        tRBTreeRotateRight(t, x->parent);
        x = t->rootNode;
      }
    }
  }
  x->color = BLACK;
}

void tRBTreeDrop(SRBTree *t, SRBTreeNode *z) {
  // update min/max node
  if (t->minNode == z) {
    t->minNode = tRBTreeSuccessor(t->minNode);
  }
  if (t->maxNode == z) {
    t->maxNode = tRBTreePredecessor(t->maxNode);
  }

  // drop impl
  SRBTreeNode *y = z;
  SRBTreeNode *x;
  ECOLOR       oColor = y->color;

  if (z->left == NULL) {
    x = z->right;
    tRBTreeTransplant(t, z, z->right);
  } else if (z->right == NULL) {
    x = z->left;
    tRBTreeTransplant(t, z, z->left);
  } else {
    y = tRBTreeSuccessor(z);
    oColor = y->color;
    x = y->right;
    if (y->parent == z) {
      x->parent = z;
    } else {
      tRBTreeTransplant(t, y, y->right);
      y->right = z->right;
      y->right->parent = y;
    }
    tRBTreeTransplant(t, z, y);
    y->left = z->left;
    y->left->parent = y;
    y->color = z->color;
  }

  // fix
  if (oColor == BLACK) {
    tRBTreeDropFixup(t, x);
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