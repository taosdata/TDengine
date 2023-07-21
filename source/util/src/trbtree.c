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
#include "tlog.h"

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

static SRBTreeNode *tRBTreeSuccessor(const SRBTree *pTree, SRBTreeNode *pNode) {
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

static SRBTreeNode *tRBTreePredecessor(const SRBTree *pTree, SRBTreeNode *pNode) {
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
  tRBTreeClear(pTree);
}

void tRBTreeClear(SRBTree *pTree) {
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

    int32_t c = pTree->cmprFn(z, temp);
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
  } else if (pTree->cmprFn(z, y) < 0) {
    y->left = z;
  } else {
    y->right = z;
  }

  z->color = RED;
  z->left = pTree->NIL;
  z->right = pTree->NIL;

  tRBTreePutFix(pTree, z);

  // update min/max node
  if (pTree->min == pTree->NIL || pTree->cmprFn(pTree->min, z) > 0) {
    pTree->min = z;
  }
  if (pTree->max == pTree->NIL || pTree->cmprFn(pTree->max, z) < 0) {
    pTree->max = z;
  }
  pTree->n++;
  return z;
}

#define RBTREE_NULL         rbtree->NIL
#define rbtree_t            SRBTree
#define rbnode_t            SRBTreeNode
#define rbtree_rotate_left  tRBTreeRotateLeft
#define rbtree_rotate_right tRBTreeRotateRight

static void rbtree_delete_fixup(rbtree_t *rbtree, rbnode_t *child, rbnode_t *child_parent) {
  rbnode_t *sibling;
  int       go_up = 1;

  /* determine sibling to the node that is one-black short */
  if (child_parent->right == child)
    sibling = child_parent->left;
  else
    sibling = child_parent->right;

  while (go_up) {
    if (child_parent == RBTREE_NULL) {
      /* removed parent==black from root, every path, so ok */
      return;
    }

    if (sibling->color == RED) { /* rotate to get a black sibling */
      child_parent->color = RED;
      sibling->color = BLACK;
      if (child_parent->right == child)
        rbtree_rotate_right(rbtree, child_parent);
      else
        rbtree_rotate_left(rbtree, child_parent);
      /* new sibling after rotation */
      if (child_parent->right == child)
        sibling = child_parent->left;
      else
        sibling = child_parent->right;
    }

    if (child_parent->color == BLACK && sibling->color == BLACK && sibling->left->color == BLACK &&
        sibling->right->color == BLACK) { /* fixup local with recolor of sibling */
      if (sibling != RBTREE_NULL) sibling->color = RED;

      child = child_parent;
      child_parent = child_parent->parent;
      /* prepare to go up, new sibling */
      if (child_parent->right == child)
        sibling = child_parent->left;
      else
        sibling = child_parent->right;
    } else
      go_up = 0;
  }

  if (child_parent->color == RED && sibling->color == BLACK && sibling->left->color == BLACK &&
      sibling->right->color == BLACK) {
    /* move red to sibling to rebalance */
    if (sibling != RBTREE_NULL) sibling->color = RED;
    child_parent->color = BLACK;
    return;
  }
  ASSERTS(sibling != RBTREE_NULL, "sibling is NULL");

  /* get a new sibling, by rotating at sibling. See which child
     of sibling is red */
  if (child_parent->right == child && sibling->color == BLACK && sibling->right->color == RED &&
      sibling->left->color == BLACK) {
    sibling->color = RED;
    sibling->right->color = BLACK;
    rbtree_rotate_left(rbtree, sibling);
    /* new sibling after rotation */
    if (child_parent->right == child)
      sibling = child_parent->left;
    else
      sibling = child_parent->right;
  } else if (child_parent->left == child && sibling->color == BLACK && sibling->left->color == RED &&
             sibling->right->color == BLACK) {
    sibling->color = RED;
    sibling->left->color = BLACK;
    rbtree_rotate_right(rbtree, sibling);
    /* new sibling after rotation */
    if (child_parent->right == child)
      sibling = child_parent->left;
    else
      sibling = child_parent->right;
  }

  /* now we have a black sibling with a red child. rotate and exchange colors. */
  sibling->color = child_parent->color;
  child_parent->color = BLACK;
  if (child_parent->right == child) {
    ASSERTS(sibling->left->color == RED, "slibing->left->color=%d not equal RED", sibling->left->color);
    sibling->left->color = BLACK;
    rbtree_rotate_right(rbtree, child_parent);
  } else {
    ASSERTS(sibling->right->color == RED, "slibing->right->color=%d not equal RED", sibling->right->color);
    sibling->right->color = BLACK;
    rbtree_rotate_left(rbtree, child_parent);
  }
}

/** helpers for delete: swap node colours */
static void swap_int8(ECOLOR *x, ECOLOR *y) {
  ECOLOR t = *x;
  *x = *y;
  *y = t;
}

/** helpers for delete: swap node pointers */
static void swap_np(rbnode_t **x, rbnode_t **y) {
  rbnode_t *t = *x;
  *x = *y;
  *y = t;
}

/** Update parent pointers of child trees of 'parent' */
static void change_parent_ptr(rbtree_t *rbtree, rbnode_t *parent, rbnode_t *old, rbnode_t *new) {
  if (parent == RBTREE_NULL) {
    ASSERTS(rbtree->root == old, "root not equal old");
    if (rbtree->root == old) rbtree->root = new;
    return;
  }
  ASSERT(parent->left == old || parent->right == old || parent->left == new || parent->right == new);
  if (parent->left == old) parent->left = new;
  if (parent->right == old) parent->right = new;
}
/** Update parent pointer of a node 'child' */
static void change_child_ptr(rbtree_t *rbtree, rbnode_t *child, rbnode_t *old, rbnode_t *new) {
  if (child == RBTREE_NULL) return;
  ASSERT(child->parent == old || child->parent == new);
  if (child->parent == old) child->parent = new;
}

rbnode_t *rbtree_delete(rbtree_t *rbtree, void *key) {
  rbnode_t *to_delete = key;
  rbnode_t *child;

  /* make sure we have at most one non-leaf child */
  if (to_delete->left != RBTREE_NULL && to_delete->right != RBTREE_NULL) {
    /* swap with smallest from right subtree (or largest from left) */
    rbnode_t *smright = to_delete->right;
    while (smright->left != RBTREE_NULL) smright = smright->left;
    /* swap the smright and to_delete elements in the tree,
     * but the rbnode_t is first part of user data struct
     * so cannot just swap the keys and data pointers. Instead
     * readjust the pointers left,right,parent */

    /* swap colors - colors are tied to the position in the tree */
    swap_int8(&to_delete->color, &smright->color);

    /* swap child pointers in parents of smright/to_delete */
    change_parent_ptr(rbtree, to_delete->parent, to_delete, smright);
    if (to_delete->right != smright) change_parent_ptr(rbtree, smright->parent, smright, to_delete);

    /* swap parent pointers in children of smright/to_delete */
    change_child_ptr(rbtree, smright->left, smright, to_delete);
    change_child_ptr(rbtree, smright->left, smright, to_delete);
    change_child_ptr(rbtree, smright->right, smright, to_delete);
    change_child_ptr(rbtree, smright->right, smright, to_delete);
    change_child_ptr(rbtree, to_delete->left, to_delete, smright);
    if (to_delete->right != smright) change_child_ptr(rbtree, to_delete->right, to_delete, smright);
    if (to_delete->right == smright) {
      /* set up so after swap they work */
      to_delete->right = to_delete;
      smright->parent = smright;
    }

    /* swap pointers in to_delete/smright nodes */
    swap_np(&to_delete->parent, &smright->parent);
    swap_np(&to_delete->left, &smright->left);
    swap_np(&to_delete->right, &smright->right);

    /* now delete to_delete (which is at the location where the smright previously was) */
  }
  ASSERT(to_delete->left == RBTREE_NULL || to_delete->right == RBTREE_NULL);

  if (to_delete->left != RBTREE_NULL)
    child = to_delete->left;
  else
    child = to_delete->right;

  /* unlink to_delete from the tree, replace to_delete with child */
  change_parent_ptr(rbtree, to_delete->parent, to_delete, child);
  change_child_ptr(rbtree, child, to_delete, to_delete->parent);

  if (to_delete->color == RED) {
    /* if node is red then the child (black) can be swapped in */
  } else if (child->color == RED) {
    /* change child to BLACK, removing a RED node is no problem */
    if (child != RBTREE_NULL) child->color = BLACK;
  } else
    rbtree_delete_fixup(rbtree, child, to_delete->parent);

  /* unlink completely */
  to_delete->parent = RBTREE_NULL;
  to_delete->left = RBTREE_NULL;
  to_delete->right = RBTREE_NULL;
  to_delete->color = BLACK;
  return to_delete;
}

void tRBTreeDrop(SRBTree *pTree, SRBTreeNode *z) {
  // update min/max node
  if (pTree->min == z) {
    pTree->min = tRBTreeSuccessor(pTree, pTree->min);
  }
  if (pTree->max == z) {
    pTree->max = tRBTreePredecessor(pTree, pTree->max);
  }

  rbtree_delete(pTree, z);

  pTree->n--;
}

SRBTreeNode *tRBTreeDropByKey(SRBTree *pTree, void *pKey) {
  SRBTreeNode *pNode = tRBTreeGet(pTree, pKey);

  if (pNode) {
    tRBTreeDrop(pTree, pNode);
  }

  return pNode;
}

SRBTreeNode *tRBTreeDropMin(SRBTree *pTree) {
  SRBTreeNode *pNode = tRBTreeMin(pTree);
  if (pNode) {
    tRBTreeDrop(pTree, pNode);
  }
  return pNode;
}

SRBTreeNode *tRBTreeDropMax(SRBTree *pTree) {
  SRBTreeNode *pNode = tRBTreeMax(pTree);
  if (pNode) {
    tRBTreeDrop(pTree, pNode);
  }
  return pNode;
}

SRBTreeNode *tRBTreeGet(const SRBTree *pTree, const SRBTreeNode *pKeyNode) {
  SRBTreeNode *pNode = pTree->root;

  while (pNode != pTree->NIL) {
    int32_t c = pTree->cmprFn(pKeyNode, pNode);

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
