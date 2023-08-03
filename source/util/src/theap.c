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

#define _DEFAULT_SOURCE
#include "theap.h"

size_t heapSize(Heap* heap) { return heap->nelts; }

Heap* heapCreate(HeapCompareFn fn) {
  Heap* heap = taosMemoryCalloc(1, sizeof(Heap));
  if (heap == NULL) {
    return NULL;
  }

  heap->min = NULL;
  heap->nelts = 0;
  heap->compFn = fn;
  return heap;
}

void heapDestroy(Heap* heap) { taosMemoryFree(heap); }

HeapNode* heapMin(const Heap* heap) { return heap->min; }

/* Swap parent with child. Child moves closer to the root, parent moves away. */
static void heapNodeSwap(Heap* heap, HeapNode* parent, HeapNode* child) {
  HeapNode* sibling;
  HeapNode  t;

  t = *parent;
  *parent = *child;
  *child = t;

  parent->parent = child;
  if (child->left == child) {
    child->left = parent;
    sibling = child->right;
  } else {
    child->right = parent;
    sibling = child->left;
  }
  if (sibling != NULL) sibling->parent = child;

  if (parent->left != NULL) parent->left->parent = parent;
  if (parent->right != NULL) parent->right->parent = parent;

  if (child->parent == NULL)
    heap->min = child;
  else if (child->parent->left == parent)
    child->parent->left = child;
  else
    child->parent->right = child;
}

void heapInsert(Heap* heap, HeapNode* newnode) {
  HeapNode** parent;
  HeapNode** child;
  uint32_t   path;
  uint32_t   n;
  uint32_t   k;

  newnode->left = NULL;
  newnode->right = NULL;
  newnode->parent = NULL;

  /* Calculate the path from the root to the insertion point.  This is a min
   * heap so we always insert at the left-most free node of the bottom row.
   */
  path = 0;
  for (k = 0, n = 1 + heap->nelts; n >= 2; k += 1, n /= 2) path = (path << 1) | (n & 1);

  /* Now traverse the heap using the path we calculated in the previous step. */
  parent = child = &heap->min;
  while (k > 0) {
    parent = child;
    if (path & 1)
      child = &(*child)->right;
    else
      child = &(*child)->left;
    path >>= 1;
    k -= 1;
  }

  /* Insert the new node. */
  newnode->parent = *parent;
  *child = newnode;
  heap->nelts += 1;

  /* Walk up the tree and check at each node if the heap property holds.
   * It's a min heap so parent < child must be true.
   */
  while (newnode->parent != NULL && (heap->compFn)(newnode, newnode->parent))
    heapNodeSwap(heap, newnode->parent, newnode);
}

void heapRemove(Heap* heap, HeapNode* node) {
  HeapNode*  smallest;
  HeapNode** max;
  HeapNode*  child;
  uint32_t   path;
  uint32_t   k;
  uint32_t   n;

  if (heap->nelts == 0) return;

  /* Calculate the path from the min (the root) to the max, the left-most node
   * of the bottom row.
   */
  path = 0;
  for (k = 0, n = heap->nelts; n >= 2; k += 1, n /= 2) path = (path << 1) | (n & 1);

  /* Now traverse the heap using the path we calculated in the previous step. */
  max = &heap->min;
  while (k > 0) {
    if (path & 1)
      max = &(*max)->right;
    else
      max = &(*max)->left;
    path >>= 1;
    k -= 1;
  }

  heap->nelts -= 1;

  /* Unlink the max node. */
  child = *max;
  *max = NULL;

  if (child == node) {
    /* We're removing either the max or the last node in the tree. */
    if (child == heap->min) {
      heap->min = NULL;
    }
    return;
  }

  /* Replace the to be deleted node with the max node. */
  child->left = node->left;
  child->right = node->right;
  child->parent = node->parent;

  if (child->left != NULL) {
    child->left->parent = child;
  }

  if (child->right != NULL) {
    child->right->parent = child;
  }

  if (node->parent == NULL) {
    heap->min = child;
  } else if (node->parent->left == node) {
    node->parent->left = child;
  } else {
    node->parent->right = child;
  }

  /* Walk down the subtree and check at each node if the heap property holds.
   * It's a min heap so parent < child must be true.  If the parent is bigger,
   * swap it with the smallest child.
   */
  for (;;) {
    smallest = child;
    if (child->left != NULL && (heap->compFn)(child->left, smallest)) smallest = child->left;
    if (child->right != NULL && (heap->compFn)(child->right, smallest)) smallest = child->right;
    if (smallest == child) break;
    heapNodeSwap(heap, child, smallest);
  }

  /* Walk up the subtree and check that each parent is less than the node
   * this is required, because `max` node is not guaranteed to be the
   * actual maximum in tree
   */
  while (child->parent != NULL && (heap->compFn)(child, child->parent)) heapNodeSwap(heap, child->parent, child);
}

void heapDequeue(Heap* heap) { heapRemove(heap, heap->min); }


struct PriorityQueue {
  SArray*    container;
  pq_comp_fn fn;
  FDelete    deleteFn;
  void*      param;
};
PriorityQueue* createPriorityQueue(pq_comp_fn fn, FDelete deleteFn, void* param) {
  PriorityQueue* pq = (PriorityQueue*)taosMemoryCalloc(1, sizeof(PriorityQueue));
  pq->container = taosArrayInit(1, sizeof(PriorityQueueNode));
  pq->fn = fn;
  pq->deleteFn = deleteFn;
  pq->param = param;
  return pq;
}

void taosPQSetFn(PriorityQueue* pq, pq_comp_fn fn) {
  pq->fn = fn;
}

void destroyPriorityQueue(PriorityQueue* pq) {
  if (pq->deleteFn)
    taosArrayDestroyP(pq->container, pq->deleteFn);
  else
    taosArrayDestroy(pq->container);
  taosMemoryFree(pq);
}

static size_t pqParent(size_t i) { return (--i) >> 1; /* (i - 1) / 2 */ }
static size_t pqLeft(size_t i) { return (i << 1) | 1; /* i * 2 + 1 */ }
static size_t pqRight(size_t i) { return (++i) << 1; /* (i + 1) * 2 */}
static void pqSwapPQNode(PriorityQueueNode* a, PriorityQueueNode* b) {
  void * tmp = a->data;
  a->data = b->data;
  b->data = tmp;
}

#define pqContainerGetEle(pq, i) ((PriorityQueueNode*)taosArrayGet((pq)->container, (i)))
#define pqContainerSize(pq) (taosArrayGetSize((pq)->container))

size_t taosPQSize(PriorityQueue* pq) { return pqContainerSize(pq); }

static PriorityQueueNode* pqHeapify(PriorityQueue* pq, size_t from, size_t last) {
  size_t largest = from;
  do {
    from = largest;
    size_t l = pqLeft(from);
    size_t r = pqRight(from);
    if (l < last && pq->fn(pqContainerGetEle(pq, from)->data, pqContainerGetEle(pq, l)->data, pq->param)) {
      largest = l;
    }
    if (r < last && pq->fn(pqContainerGetEle(pq, largest)->data, pqContainerGetEle(pq, r)->data, pq->param)) {
      largest = r;
    }
    if (largest != from) {
      pqSwapPQNode(pqContainerGetEle(pq, from), pqContainerGetEle(pq, largest));
    }
  } while (largest != from);
  return pqContainerGetEle(pq, largest);
}

static void pqBuildHeap(PriorityQueue* pq) {
  if (pqContainerSize(pq) > 1) {
    for (size_t i = pqContainerSize(pq) - 1; i > 0; --i) {
      pqHeapify(pq, i, pqContainerSize(pq));
    }
    pqHeapify(pq, 0, pqContainerSize(pq));
  }
}

static PriorityQueueNode* pqReverseHeapify(PriorityQueue* pq, size_t i) {
  while (i > 0 && !pq->fn(pqContainerGetEle(pq, i)->data, pqContainerGetEle(pq, pqParent(i))->data, pq->param)) {
    size_t parentIdx = pqParent(i);
    pqSwapPQNode(pqContainerGetEle(pq, i), pqContainerGetEle(pq, parentIdx));
    i = parentIdx;
  }
  return pqContainerGetEle(pq, i);
}

static void pqUpdate(PriorityQueue* pq, size_t i) {
  if (i == 0 || pq->fn(pqContainerGetEle(pq, i)->data, pqContainerGetEle(pq, pqParent(i))->data, pq->param)) {
    // if value in pos i is smaller than parent, heapify down from i to the end
    pqHeapify(pq, i, pqContainerSize(pq));
  } else {
    // if value in pos i is big than parent, heapify up from i
    pqReverseHeapify(pq, i);
  }
}

static void pqRemove(PriorityQueue* pq, size_t i) {
  if (i == pqContainerSize(pq) - 1) {
    taosArrayPop(pq->container);
    return;
  }

  taosArraySet(pq->container, i, taosArrayGet(pq->container, pqContainerSize(pq) - 1));
  taosArrayPop(pq->container);
  pqUpdate(pq, i);
}

PriorityQueueNode* taosPQTop(PriorityQueue* pq) {
  return pqContainerGetEle(pq, 0);
}

PriorityQueueNode* taosPQPush(PriorityQueue* pq, const PriorityQueueNode* node) {
  taosArrayPush(pq->container, node);
  return pqReverseHeapify(pq, pqContainerSize(pq) - 1);
}

void taosPQPop(PriorityQueue* pq) {
  PriorityQueueNode* top = taosPQTop(pq);
  if (pq->deleteFn) pq->deleteFn(top->data);
  pqRemove(pq, 0);
}

struct BoundedQueue {
  PriorityQueue* queue;
  uint32_t       maxSize;
};

BoundedQueue* createBoundedQueue(uint32_t maxSize, pq_comp_fn fn, FDelete deleteFn, void* param) {
  BoundedQueue* q = (BoundedQueue*)taosMemoryCalloc(1, sizeof(BoundedQueue));
  q->queue = createPriorityQueue(fn, deleteFn, param);
  taosArrayEnsureCap(q->queue->container, maxSize + 1);
  q->maxSize = maxSize;
  return q;
}

void taosBQSetFn(BoundedQueue* q, pq_comp_fn fn) {
  taosPQSetFn(q->queue, fn);
}

void destroyBoundedQueue(BoundedQueue* q) {
  if (!q) return;
  destroyPriorityQueue(q->queue);
  taosMemoryFree(q);
}

PriorityQueueNode* taosBQPush(BoundedQueue* q, PriorityQueueNode* n) {
  if (pqContainerSize(q->queue) == q->maxSize + 1) {
    PriorityQueueNode* top = pqContainerGetEle(q->queue, 0);
    if (q->queue->fn(top->data, n->data, q->queue->param)) {
      return NULL;
    } else {
      void* p = top->data;
      top->data = n->data;
      n->data = p;
      if (q->queue->deleteFn) q->queue->deleteFn(n->data);
    }
    return pqHeapify(q->queue, 0, taosBQSize(q));
  } else {
    return taosPQPush(q->queue, n);
  }
}

PriorityQueueNode* taosBQTop(BoundedQueue* q) {
  return taosPQTop(q->queue);
}

void taosBQBuildHeap(BoundedQueue *q) {
  pqBuildHeap(q->queue);
}

size_t taosBQMaxSize(BoundedQueue* q) {
  return q->maxSize;
}

size_t taosBQSize(BoundedQueue* q) {
  return taosPQSize(q->queue);
}

void taosBQPop(BoundedQueue* q) {
  taosPQPop(q->queue);
}
