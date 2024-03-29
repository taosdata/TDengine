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

#ifndef _TD_UTIL_HEAP_H_
#define _TD_UTIL_HEAP_H_

#include "os.h"
#include "tarray.h"

#ifdef __cplusplus
extern "C" {
#endif

struct HeapNode;

/* Return non-zero if a < b. */
typedef int32_t (*HeapCompareFn)(const struct HeapNode* a, const struct HeapNode* b);

typedef struct HeapNode {
  struct HeapNode* left;
  struct HeapNode* right;
  struct HeapNode* parent;
} HeapNode;

/* A binary min heap.  The usual properties hold: the root is the lowest
 * element in the set, the height of the tree is at most log2(nodes) and
 * it's always a complete binary tree.
 *
 */
typedef struct {
  HeapNode*     min;
  size_t        nelts;
  HeapCompareFn compFn;
} Heap;

Heap* heapCreate(HeapCompareFn fn);

void heapDestroy(Heap* heap);

HeapNode* heapMin(const Heap* heap);

void heapInsert(Heap* heap, HeapNode* node);

void heapRemove(Heap* heap, struct HeapNode* node);

void heapDequeue(Heap* heap);

size_t heapSize(Heap* heap);

typedef bool (*pq_comp_fn)(void* l, void* r, void* param);

typedef struct PriorityQueueNode {
  void* data;
} PriorityQueueNode;

typedef struct PriorityQueue PriorityQueue;

PriorityQueue* createPriorityQueue(pq_comp_fn fn, FDelete deleteFn, void* param);

void taosPQSetFn(PriorityQueue* pq, pq_comp_fn fn);

void destroyPriorityQueue(PriorityQueue* pq);

PriorityQueueNode* taosPQTop(PriorityQueue* pq);

size_t taosPQSize(PriorityQueue* pq);

PriorityQueueNode* taosPQPush(PriorityQueue* pq, const PriorityQueueNode* node);

void taosPQPop(PriorityQueue* pq);

typedef struct BoundedQueue BoundedQueue;

BoundedQueue* createBoundedQueue(uint32_t maxSize, pq_comp_fn fn, FDelete deleteFn, void* param);

void taosBQSetFn(BoundedQueue* q, pq_comp_fn fn);

void destroyBoundedQueue(BoundedQueue* q);

/*
 * Push one node into BQ
 * @retval NULL if n is upper than top node in q, and n is not freed
 * @retval the pushed Node if pushing succeeded
 * @note if maxSize exceeded, the original highest node is popped and freed with deleteFn
 * */
PriorityQueueNode* taosBQPush(BoundedQueue* q, PriorityQueueNode* n);

PriorityQueueNode* taosBQTop(BoundedQueue* q);

size_t taosBQSize(BoundedQueue* q);

size_t taosBQMaxSize(BoundedQueue* q);

void taosBQBuildHeap(BoundedQueue* q);

void taosBQPop(BoundedQueue* q);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_HEAP_H_*/
