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

#include "indexFstRegistry.h"
#include "os.h"

static FORCE_INLINE uint64_t fstRegistryHash(FstRegistry* registry, FstBuilderNode* bNode) {
  // TODO(yihaoDeng): refactor later
  const uint64_t FNV_PRIME = 1099511628211;
  uint64_t       h = 14695981039346656037u;

  h = (h ^ (uint64_t)bNode->isFinal) * FNV_PRIME;
  h = (h ^ (bNode)->finalOutput) * FNV_PRIME;

  uint32_t sz = (uint32_t)taosArrayGetSize(bNode->trans);
  for (uint32_t i = 0; i < sz; i++) {
    FstTransition* trn = taosArrayGet(bNode->trans, i);
    h = (h ^ (uint64_t)(trn->inp)) * FNV_PRIME;
    h = (h ^ (uint64_t)(trn->out)) * FNV_PRIME;
    h = (h ^ (uint64_t)(trn->addr)) * FNV_PRIME;
  }
  return h % (registry->tableSize);
}
static void fstRegistryCellSwap(SArray* arr, uint32_t a, uint32_t b) {
  size_t sz = taosArrayGetSize(arr);
  if (a >= sz || b >= sz) {
    return;
  }

  FstRegistryCell* cell1 = (FstRegistryCell*)taosArrayGet(arr, a);
  FstRegistryCell* cell2 = (FstRegistryCell*)taosArrayGet(arr, b);

  FstRegistryCell t = {.addr = cell1->addr, .node = cell1->node};

  cell1->addr = cell2->addr;
  cell1->node = cell2->node;

  cell2->addr = t.addr;
  cell2->node = t.node;
  return;
}

static void fstRegistryCellPromote(SArray* arr, uint32_t start, uint32_t end) {
  size_t sz = taosArrayGetSize(arr);
  if (start >= sz && end >= sz) {
    return;
  }
  ASSERTS(start >= end, "index-fst start lower than end");
  if (start < end) return;

  int32_t s = (int32_t)start;
  int32_t e = (int32_t)end;
  while (s > e) {
    fstRegistryCellSwap(arr, s - 1, s);
    s -= 1;
  }
}

FstRegistry* fstRegistryCreate(uint64_t tableSize, uint64_t mruSize) {
  FstRegistry* registry = taosMemoryMalloc(sizeof(FstRegistry));
  if (registry == NULL) {
    return NULL;
  }

  uint64_t nCells = tableSize * mruSize;
  SArray*  tb = (SArray*)taosArrayInit(nCells, sizeof(FstRegistryCell));
  if (NULL == tb) {
    taosMemoryFree(registry);
    return NULL;
  }

  for (uint64_t i = 0; i < nCells; i++) {
    FstRegistryCell cell = {.addr = NONE_ADDRESS, .node = fstBuilderNodeDefault()};
    taosArrayPush(tb, &cell);
  }

  registry->table = tb;
  registry->tableSize = tableSize;
  registry->mruSize = mruSize;
  return registry;
}

void fstRegistryDestroy(FstRegistry* registry) {
  if (registry == NULL) {
    return;
  }

  SArray* tb = registry->table;
  size_t  sz = taosArrayGetSize(tb);
  for (size_t i = 0; i < sz; i++) {
    FstRegistryCell* cell = taosArrayGet(tb, i);
    fstBuilderNodeDestroy(cell->node);
  }
  taosArrayDestroy(tb);
  taosMemoryFree(registry);
}

FstRegistryEntry* fstRegistryGetEntry(FstRegistry* registry, FstBuilderNode* bNode) {
  if (taosArrayGetSize(registry->table) <= 0) {
    return NULL;
  }
  uint64_t bucket = fstRegistryHash(registry, bNode);
  uint64_t start = registry->mruSize * bucket;
  uint64_t end = start + registry->mruSize;

  FstRegistryEntry* entry = taosMemoryMalloc(sizeof(FstRegistryEntry));
  if (end - start == 1) {
    FstRegistryCell* cell = taosArrayGet(registry->table, start);
    // cell->isNode &&
    if (cell->addr != NONE_ADDRESS && fstBuilderNodeEqual(cell->node, bNode)) {
      entry->state = FOUND;
      entry->addr = cell->addr;
      return entry;
    } else {
      fstBuilderNodeCloneFrom(cell->node, bNode);
      entry->state = NOTFOUND;
      entry->cell = cell;  // copy or not
    }
  } else if (end - start == 2) {
    FstRegistryCell* cell1 = taosArrayGet(registry->table, start);
    if (cell1->addr != NONE_ADDRESS && fstBuilderNodeEqual(cell1->node, bNode)) {
      entry->state = FOUND;
      entry->addr = cell1->addr;
      return entry;
    }
    FstRegistryCell* cell2 = taosArrayGet(registry->table, start + 1);
    if (cell2->addr != NONE_ADDRESS && fstBuilderNodeEqual(cell2->node, bNode)) {
      entry->state = FOUND;
      entry->addr = cell2->addr;
      // must swap here
      fstRegistryCellSwap(registry->table, start, start + 1);
      return entry;
    }
    // clone from bNode, refactor later
    fstBuilderNodeCloneFrom(cell2->node, bNode);

    fstRegistryCellSwap(registry->table, start, start + 1);
    FstRegistryCell* cCell = taosArrayGet(registry->table, start);
    entry->state = NOTFOUND;
    entry->cell = cCell;
  } else {
    uint32_t i = start;
    for (; i < end; i++) {
      FstRegistryCell* cell = (FstRegistryCell*)taosArrayGet(registry->table, i);
      if (cell->addr != NONE_ADDRESS && fstBuilderNodeEqual(cell->node, bNode)) {
        entry->state = FOUND;
        entry->addr = cell->addr;
        fstRegistryCellPromote(registry->table, i, start);
        break;
      }
    }
    if (i >= end) {
      uint64_t         last = end - 1;
      FstRegistryCell* cell = (FstRegistryCell*)taosArrayGet(registry->table, last);
      // clone from bNode, refactor later
      fstBuilderNodeCloneFrom(cell->node, bNode);

      fstRegistryCellPromote(registry->table, last, start);
      FstRegistryCell* cCell = taosArrayGet(registry->table, start);
      entry->state = NOTFOUND;
      entry->cell = cCell;
    }
  }
  return entry;
}
void fstRegistryEntryDestroy(FstRegistryEntry* entry) { taosMemoryFree(entry); }
