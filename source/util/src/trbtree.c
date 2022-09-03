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

static void tRBTreeRotateLeft(SRBTree *pTree, SRBTreeNode *x) {
  SRBTreeNode *y = x->right;
  x->right = y->left;
  if (y->left != pTree->NIL) {
    y->left->parent = x;
  }
  y->parent = x->parent;
  if (x->parent == pTree->NIL) {
    pTree->root = y;
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
  if (y->right != pTree->NIL) {
    y->right->parent = x;
  }
  y->parent = x->parent;
  if (x->parent == pTree->NIL) {
    pTree->root = y;
  } else if (x == x->parent->right) {
    x->parent->right = y;
  } else {
    x->parent->left = y;
  }
  y->right = x;
  x->parent = y;
}

static void tRBTreePutFix(SRBTree *pTree, SRBTreeNode *z) {
  while (z->parent->color == RED) {
    if (z->parent == z->parent->parent->left) {  // z.parent is the left child

      SRBTreeNode *y = z->parent->parent->right;  // uncle of z

      if (y->color == RED) {  // case 1
        z->parent->color = BLACK;
        y->color = BLACK;
        z->parent->parent->color = RED;
        z = z->parent->parent;
      } else {                        // case2 or case3
        if (z == z->parent->right) {  // case2
          z = z->parent;              // marked z.parent as new z
          tRBTreeRotateLeft(pTree, z);
        }
        // case3
        z->parent->color = BLACK;        // made parent black
        z->parent->parent->color = RED;  // made parent red
        tRBTreeRotateRight(pTree, z->parent->parent);
      }
    } else {                                     // z.parent is the right child
      SRBTreeNode *y = z->parent->parent->left;  // uncle of z

      if (y->color == RED) {
        z->parent->color = BLACK;
        y->color = BLACK;
        z->parent->parent->color = RED;
        z = z->parent->parent;
      } else {
        if (z == z->parent->left) {
          z = z->parent;  // marked z.parent as new z
          tRBTreeRotateRight(pTree, z);
        }
        z->parent->color = BLACK;        // made parent black
        z->parent->parent->color = RED;  // made parent red
        tRBTreeRotateLeft(pTree, z->parent->parent);
      }
    }
  }
  pTree->root->color = BLACK;
}

static void tRBTreeTransplant(SRBTree *pTree, SRBTreeNode *u, SRBTreeNode *v) {
  if (u->parent == pTree->NIL)
    pTree->root = v;
  else if (u == u->parent->left)
    u->parent->left = v;
  else
    u->parent->right = v;
  v->parent = u->parent;
}

static void tRBTreeDropFix(SRBTree *pTree, SRBTreeNode *x) {
  while (x != pTree->root && x->color == BLACK) {
    if (x == x->parent->left) {
      SRBTreeNode *w = x->parent->right;
      if (w->color == RED) {
        w->color = BLACK;
        x->parent->color = RED;
        tRBTreeRotateLeft(pTree, x->parent);
        w = x->parent->right;
      }
      if (w->left->color == BLACK && w->right->color == BLACK) {
        w->color = RED;
        x = x->parent;
      } else {
        if (w->right->color == BLACK) {
          w->left->color = BLACK;
          w->color = RED;
          tRBTreeRotateRight(pTree, w);
          w = x->parent->right;
        }
        w->color = x->parent->color;
        x->parent->color = BLACK;
        w->right->color = BLACK;
        tRBTreeRotateLeft(pTree, x->parent);
        x = pTree->root;
      }
    } else {
      SRBTreeNode *w = x->parent->left;
      if (w->color == RED) {
        w->color = BLACK;
        x->parent->color = RED;
        tRBTreeRotateRight(pTree, x->parent);
        w = x->parent->left;
      }
      if (w->right->color == BLACK && w->left->color == BLACK) {
        w->color = RED;
        x = x->parent;
      } else {
        if (w->left->color == BLACK) {
          w->right->color = BLACK;
          w->color = RED;
          tRBTreeRotateLeft(pTree, w);
          w = x->parent->left;
        }
        w->color = x->parent->color;
        x->parent->color = BLACK;
        w->left->color = BLACK;
        tRBTreeRotateRight(pTree, x->parent);
        x = pTree->root;
      }
    }
  }
  x->color = BLACK;
}

static SRBTreeNode *tRBTreeSuccessor(SRBTree *pTree, SRBTreeNode *pNode) {
  if (pNode->right != pTree->NIL) {
    pNode = pNode->right;
    while (pNode->left != pTree->NIL) {
      pNode = pNode->left;
    }
  } else {
    while (true) {
      if (pNode->parent == pTree->NIL || pNode == pNode->parent->left) {
        pNode = pNode->parent;
        break;
      } else {
        pNode = pNode->parent;
      }
    }
  }

  return pNode;
}

static SRBTreeNode *tRBTreePredecessor(SRBTree *pTree, SRBTreeNode *pNode) {
  if (pNode->left != pTree->NIL) {
    pNode = pNode->left;
    while (pNode->right != pTree->NIL) {
      pNode = pNode->right;
    }
  } else {
    while (true) {
      if (pNode->parent == pTree->NIL || pNode == pNode->parent->right) {
        pNode = pNode->parent;
        break;
      } else {
        pNode = pNode->parent;
      }
    }
  }

  return pNode;
}

void tRBTreeCreate(SRBTree *pTree, tRBTreeCmprFn cmprFn) {
  pTree->cmprFn = cmprFn;
  pTree->n = 0;
  pTree->NIL = &pTree->NILNODE;
  pTree->NIL->color = BLACK;
  pTree->NIL->parent = NULL;
  pTree->NIL->left = NULL;
  pTree->NIL->right = NULL;
  pTree->root = pTree->NIL;
  pTree->min = pTree->NIL;
  pTree->max = pTree->NIL;
}

SRBTreeNode *tRBTreePut(SRBTree *pTree, SRBTreeNode *z) {
  SRBTreeNode *y = pTree->NIL;  // variable for the parent of the added node
  SRBTreeNode *temp = pTree->root;

  while (temp != pTree->NIL) {
    y = temp;

    int32_t c = pTree->cmprFn(RBTREE_NODE_PAYLOAD(z), RBTREE_NODE_PAYLOAD(temp));
    if (c < 0) {
      temp = temp->left;
    } else if (c > 0) {
      temp = temp->right;
    } else {
      return NULL;
    }
  }
  z->parent = y;

  if (y == pTree->NIL) {
    pTree->root = z;
  } else if (pTree->cmprFn(RBTREE_NODE_PAYLOAD(z), RBTREE_NODE_PAYLOAD(y)) < 0) {
    y->left = z;
  } else {
    y->right = z;
  }

  z->color = RED;
  z->left = pTree->NIL;
  z->right = pTree->NIL;

  tRBTreePutFix(pTree, z);

  // update min/max node
  if (pTree->min == pTree->NIL || pTree->cmprFn(RBTREE_NODE_PAYLOAD(pTree->min), RBTREE_NODE_PAYLOAD(z)) > 0) {
    pTree->min = z;
  }
  if (pTree->max == pTree->NIL || pTree->cmprFn(RBTREE_NODE_PAYLOAD(pTree->max), RBTREE_NODE_PAYLOAD(z)) < 0) {
    pTree->max = z;
  }
  pTree->n++;
  return z;
}

void tRBTreeDrop(SRBTree *pTree, SRBTreeNode *z) {
  SRBTreeNode *y = z;
  SRBTreeNode *x;
  ECOLOR       y_orignal_color = y->color;

  // update min/max node
  if (pTree->min == z) {
    pTree->min = tRBTreeSuccessor(pTree, pTree->min);
  }
  if (pTree->max == z) {
    pTree->max = tRBTreePredecessor(pTree, pTree->max);
  }

  // drop impl
  if (z->left == pTree->NIL) {
    x = z->right;
    tRBTreeTransplant(pTree, z, z->right);
  } else if (z->right == pTree->NIL) {
    x = z->left;
    tRBTreeTransplant(pTree, z, z->left);
  } else {
    y = tRBTreeSuccessor(pTree, z);
    y_orignal_color = y->color;
    x = y->right;
    if (y->parent == z) {
      x->parent = z;
    } else {
      tRBTreeTransplant(pTree, y, y->right);
      y->right = z->right;
      y->right->parent = y;
    }
    tRBTreeTransplant(pTree, z, y);
    y->left = z->left;
    y->left->parent = y;
    y->color = z->color;
  }

  // fix
  if (y_orignal_color == BLACK) {
    tRBTreeDropFix(pTree, x);
  }
  pTree->n--;
}

SRBTreeNode *tRBTreeDropByKey(SRBTree *pTree, void *pKey) {
  SRBTreeNode *pNode = tRBTreeGet(pTree, pKey);

  if (pNode) {
    tRBTreeDrop(pTree, pNode);
  }

  return pNode;
}

SRBTreeNode *tRBTreeGet(SRBTree *pTree, void *pKey) {
  SRBTreeNode *pNode = pTree->root;

  while (pNode != pTree->NIL) {
    int32_t c = pTree->cmprFn(pKey, RBTREE_NODE_PAYLOAD(pNode));

    if (c < 0) {
      pNode = pNode->left;
    } else if (c > 0) {
      pNode = pNode->right;
    } else {
      break;
    }
  }

  return (pNode == pTree->NIL) ? NULL : pNode;
}

// SRBTreeIter ================================================
SRBTreeNode *tRBTreeIterNext(SRBTreeIter *pIter) {
  SRBTreeNode *pNode = pIter->pNode;

  if (pIter->pNode != pIter->pTree->NIL) {
    if (pIter->asc) {
      // ascend
      pIter->pNode = tRBTreeSuccessor(pIter->pTree, pIter->pNode);
    } else {
      // descend
      pIter->pNode = tRBTreePredecessor(pIter->pTree, pIter->pNode);
    }
  }

_exit:
  return (pNode == pIter->pTree->NIL) ? NULL : pNode;
}