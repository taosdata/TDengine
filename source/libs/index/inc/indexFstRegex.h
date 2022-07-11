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

#ifndef _TD_INDEX_FST_REGEX_H_
#define _TD_INDEX_FST_REGEX_H_

//#include "indexFstDfa.h"
#include "taos.h"
#include "tarray.h"
#include "tchecksum.h"
#include "thash.h"
#include "tlog.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { MATCH, JUMP, SPLIT, RANGE } InstType;

typedef struct MatchValue {
#ifdef WINDOWS
  size_t avoidCompilationErrors;
#endif
} MatchValue;
typedef struct JumpValue {
  uint32_t step;
} JumpValue;

typedef struct SplitValue {
  uint32_t len1;
  uint32_t len2;
} SplitValue;

typedef struct RangeValue {
  uint8_t start;
  uint8_t end;
} RangeValue;

typedef struct {
  InstType ty;
  union {
    MatchValue mv;
    JumpValue  jv;
    SplitValue sv;
    RangeValue rv;
  };
} Inst;

typedef struct {
  char *orig;
  void *dfa;
} FstRegex;

FstRegex *regexCreate(const char *str);
void      regexDestroy(FstRegex *regex);

uint32_t regexAutomStart(FstRegex *regex);
bool     regexAutomIsMatch(FstRegex *regex, uint32_t state);
bool     regexAutomCanMatch(FstRegex *regex, uint32_t state, bool null);
bool     regexAutomAccept(FstRegex *regex, uint32_t state, uint8_t byte, uint32_t *result);

#ifdef __cplusplus
}
#endif

#endif
