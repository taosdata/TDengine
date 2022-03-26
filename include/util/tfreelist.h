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

#ifndef _TD_UTIL_FREELIST_H_
#define _TD_UTIL_FREELIST_H_

#include "tlist.h"

#ifdef __cplusplus
extern "C" {
#endif

struct SFreeListNode {
  TD_SLIST_NODE(SFreeListNode);
  char payload[];
};

typedef TD_SLIST(SFreeListNode) SFreeList;

#define TFL_MALLOC(PTR, TYPE, SIZE, LIST)                      \
  do {                                                         \
    void *ptr = taosMemoryMalloc((SIZE) + sizeof(struct SFreeListNode)); \
    if (ptr) {                                                 \
      TD_SLIST_PUSH((LIST), (struct SFreeListNode *)ptr);      \
      ptr = ((struct SFreeListNode *)ptr)->payload;            \
      (PTR) = (TYPE)(ptr);                                     \
    }else{                                                     \
      (PTR) = NULL;                                            \
    }                                                          \
  }while(0);

#define tFreeListInit(pFL) TD_SLIST_INIT(pFL)

static FORCE_INLINE void tFreeListClear(SFreeList *pFL) {
  struct SFreeListNode *pNode;
  for (;;) {
    pNode = TD_SLIST_HEAD(pFL);
    if (pNode == NULL) break;
    TD_SLIST_POP(pFL);
    taosMemoryFree(pNode);
  }
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_FREELIST_H_*/