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

#ifndef _VNODE_HAS_H_
#define _VNODE_HAS_H_

#include "vnd.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SVHashTable SVHashTable;

struct SVHashTable {
  uint32_t (*hash)(const void*);
  int32_t (*compare)(const void*, const void*);
  int32_t              numEntries;
  uint32_t             numBuckets;
  struct SVHashEntry** buckets;
};

#define vHashNumEntries(ht) ((ht)->numEntries)
int32_t vHashInit(SVHashTable** ht, uint32_t (*hash)(const void*), int32_t (*compare)(const void*, const void*));
int32_t vHashDestroy(SVHashTable** ht);
int32_t vHashPut(SVHashTable* ht, void* obj);
int32_t vHashGet(SVHashTable* ht, const void* obj, void** retObj);
int32_t vHashDrop(SVHashTable* ht, const void* obj);

#ifdef __cplusplus
}
#endif

#endif /*_VNODE_HAS_H_*/