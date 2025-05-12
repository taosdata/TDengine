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

#ifndef __INDEX_FST_DFA_H__
#define __INDEX_FST_DFA_H__

#include "indexFstRegex.h"
#include "indexFstSparse.h"
#include "tarray.h"
#include "thash.h"

#ifdef __cplusplus

extern "C" {
#endif

typedef struct FstDfa FstDfa;

typedef struct {
  SArray  *insts;
  uint32_t next[256];
  bool     isMatch;
} DfaState;

/*
 * dfa builder related func
 **/
typedef struct FstDfaBuilder {
  FstDfa   *dfa;
  SHashObj *cache;
} FstDfaBuilder;

FstDfaBuilder *dfaBuilderCreate(SArray *insts);

void dfaBuilderDestroy(FstDfaBuilder *builder);

FstDfa *dfaBuilderBuild(FstDfaBuilder *builder);

bool dfaBuilderRunState(FstDfaBuilder *builder, FstSparseSet *cur, FstSparseSet *next, uint32_t state, uint8_t bytes,
                        uint32_t *result);

bool dfaBuilderCacheState(FstDfaBuilder *builder, FstSparseSet *set, uint32_t *result);

/*
 * dfa related func
 **/
typedef struct FstDfa {
  SArray *insts;
  SArray *states;
} FstDfa;

FstDfa *dfaCreate(SArray *insts, SArray *states);
bool    dfaIsMatch(FstDfa *dfa, uint32_t si);
bool    dfaAccept(FstDfa *dfa, uint32_t si, uint8_t byte, uint32_t *result);
void    dfaAdd(FstDfa *dfa, FstSparseSet *set, uint32_t ip);
bool    dfaRun(FstDfa *dfa, FstSparseSet *from, FstSparseSet *to, uint8_t byte);

#ifdef __cplusplus
}
#endif

#endif
