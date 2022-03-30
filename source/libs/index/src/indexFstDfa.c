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

#include "indexFstDfa.h"
#include "thash.h"

static int dfaInstsEqual(const void *a, const void *b, size_t size) {
  SArray *ar = (SArray *)a;
  SArray *br = (SArray *)b;
  size_t  al = ar != NULL ? taosArrayGetSize(ar) : 0;
  size_t  bl = br != NULL ? taosArrayGetSize(br) : 0;
  if (al != bl) {
    return -1;
  }
  for (int i = 0; i < al; i++) {
    uint32_t v1 = *(uint32_t *)taosArrayGet(ar, i);
    uint32_t v2 = *(uint32_t *)taosArrayGet(br, i);
    if (v1 != v2) {
      return -1;
    }
  }
  return 0;
}
FstDfaBuilder *dfaBuilderCreate(SArray *insts) {
  FstDfaBuilder *builder = taosMemoryCalloc(1, sizeof(FstDfaBuilder));
  if (builder == NULL) {
    return NULL;
  }

  SArray *states = taosArrayInit(4, sizeof(State));

  builder->dfa = dfaCreate(insts, states);
  builder->cache = taosHashInit(
      4, taosGetDefaultHashFunction(POINTER_BYTES == sizeof(int64_t) ? TSDB_DATA_TYPE_BIGINT : TSDB_DATA_TYPE_INT),
      false, HASH_NO_LOCK);
  taosHashSetEqualFp(builder->cache, dfaInstsEqual);
  return builder;
}

FstDfa *dfaBuilderBuild(FstDfaBuilder *builder) {
  uint32_t      sz = taosArrayGetSize(builder->dfa->insts);
  FstSparseSet *cur = sparSetCreate(sz);
  FstSparseSet *nxt = sparSetCreate(sz);

  dfaAdd(builder->dfa, cur, 0);
}

bool dfaBuilderRunState(FstDfaBuilder *builder, FstSparseSet *cur, FstSparseSet *next, uint32_t state, uint8_t bytes,
                        uint32_t *result) {
  // impl run state
  return true;
}

bool dfaBuilderCachedState(FstDfaBuilder *builder, FstSparseSet *set, uint32_t *result) {
  // impl cache state
  return true;
}

FstDfa *dfaCreate(SArray *insts, SArray *states) {
  FstDfa *dfa = taosMemoryCalloc(1, sizeof(FstDfa));
  if (dfa == NULL) {
    return NULL;
  }

  dfa->insts = insts;
  dfa->states = states;
  return dfa;
}
bool dfaIsMatch(FstDfa *dfa, uint32_t si) {
  // impl match
  return true;
}
bool dfaAccept(FstDfa *dfa, uint32_t si, uint8_t byte, uint32_t *result) {
  // impl accept
  return true;
}
void dfaAdd(FstDfa *dfa, FstSparseSet *set, uint32_t ip) {
  // impl add
  return;
}
bool dfaRun(FstDfa *dfa, FstSparseSet *from, FstSparseSet *to, uint8_t byte) {
  // impl run
  return true;
}
