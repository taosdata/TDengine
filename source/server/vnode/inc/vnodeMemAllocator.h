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

#ifndef _TD_VNODE_MEM_ALLOCATOR_H_
#define _TD_VNODE_MEM_ALLOCATOR_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SVnodeMemAllocator SVnodeMemAllocator;

SVnodeMemAllocator *VMACreate(size_t size /* base size */, size_t ssize /* step size */,
                              size_t threshold /* threshold size when full*/);
void                VMADestroy(SVnodeMemAllocator *pvma);
void                VMAReset(SVnodeMemAllocator *pvma);
void *              VMAMalloc(SVnodeMemAllocator *pvma, size_t size);
void                VMAFree(SVnodeMemAllocator *pvma, void *ptr);
bool                VMAIsFull(SVnodeMemAllocator *pvma);

// ------------------ FOR TEST ONLY ------------------
typedef struct SVMANode {
  struct SVMANode *prev;
  size_t           tsize;
  size_t           used;
  char             data[];
} SVMANode;

struct SVnodeMemAllocator {
  bool      full;       // if allocator is full
  size_t    threshold;  // threshold;
  size_t    ssize;      // step size to allocate
  SVMANode *inuse;      // inuse node to allocate
  SVMANode  node;       // basic node to use
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_MEM_ALLOCATOR_H_*/