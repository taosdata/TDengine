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
#ifndef __FST_REGISTRY_H__
#define __FST_REGISTRY_H__

#ifdef __cplusplus
extern "C" {
#endif

#include "indexFstNode.h"
#include "indexFstUtil.h"
#include "indexInt.h"

typedef struct FstRegistryCell {
  CompiledAddr    addr;
  FstBuilderNode* node;
} FstRegistryCell;

#define FST_REGISTRY_CELL_IS_EMPTY(cell) (cell->addr == NONE_ADDRESS)
#define FST_REGISTRY_CELL_INSERT(cell, tAddr) \
  do {                                        \
    cell->addr = tAddr;                       \
  } while (0)

// typedef struct FstRegistryCache {
//  SArray *cells;
//  uint32_t start;
//  uint32_t end;
//} FstRegistryCache;

typedef enum { FOUND, NOTFOUND, REJECTED } FstRegistryEntryState;

typedef struct FstRegistryEntry {
  FstRegistryEntryState state;
  CompiledAddr          addr;
  FstRegistryCell*      cell;
} FstRegistryEntry;

// Registry relation function
typedef struct FstRegistry {
  SArray*  table;      //<FstRegistryCell>
  uint64_t tableSize;  // num of rows
  uint64_t mruSize;    // num of columns
} FstRegistry;

//
FstRegistry* fstRegistryCreate(uint64_t tableSize, uint64_t mruSize);
void         fstRegistryDestroy(FstRegistry* registry);

FstRegistryEntry* fstRegistryGetEntry(FstRegistry* registry, FstBuilderNode* bNode);
void              fstRegistryEntryDestroy(FstRegistryEntry* entry);

#ifdef __cplusplus
}
#endif

#endif
