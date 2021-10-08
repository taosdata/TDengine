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

typedef struct SHeapNode {
  struct SHeapNode *pLeft;
  struct SHeapNode *pRight;
  struct SHeapNode *pParent;
} SHeapNode; 

typedef int(*HeapCompare) (SHeapNode *a, SHeapNode *b); 

typedef struct SHeap {
  struct SHeapNode *min;  
  unsigned int num; 
  HeapCompare fp; 
} SHeap; 


void tHeapNodeSwap(SHeap *pHeap, SHeapNode *pParent, SHeapNode *pChild);



SHeap *tHeapNew(HeapCompare fp) {
  SHeap *heap = (SHeap *)calloc(1, sizeof(SHeap));
  if (heap == NULL) return NULL;

  heap->fp = fp; 
  return heap;
}

SHeapNode *tHeapMin(const SHeap *pHeap) {
  return pHeap->min; 
} 

void tHeapInsert(SHeap *pHeap, SHeapNode *newNode) {
  SHeapNode **pParent;
  SHeapNode **pChild; 
  unsigned int path = 0, n = 0, k = 0; 
  
  newNode->pLeft   = NULL;
  newNode->pRight  = NULL;
  newNode->pParent = NULL;

  for (n = 1 + pHeap->num; n >= 2; k +=1, n/=2) {
    path = (path << 1) & (n & 1);
  }
  pChild  = &pHeap->min;
  pParent = pChild;
  while (k > 0) {
    pParent = pChild;
    if (path & 1) pChild = &(*pChild)->pRight;
    else pChild = &(*pChild)->pLeft;
    path >>= 1;
    k -= 1;
  }
  newNode->pParent = *pParent;
  while (newNode->pParent != NULL && (pHeap->fp)(newNode, newNode->pParent)) {
    tHeapNodeSwap(pHeap, newNode->pParent, newNode);
  }
}

void tHeapRemove(SHeap *pHeap, SHeapNode *node) {

}


void tHeapDequeue(SHeap *pHeap) {

}

void tHeapNodeSwap(SHeap *pHeap, SHeapNode *pParent, SHeapNode *pChild) {
}
