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
#ifndef __INDEX_UTIL_H__
#define __INDEX_UTIL_H__

#include "indexInt.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SERIALIZE_MEM_TO_BUF(buf, key, mem)                     \
  do {                                                          \
    memcpy((void *)buf, (void *)(&key->mem), sizeof(key->mem)); \
    buf += sizeof(key->mem);                                    \
  } while (0)

#define SERIALIZE_STR_MEM_TO_BUF(buf, key, mem, len) \
  do {                                               \
    memcpy((void *)buf, (void *)key->mem, len);      \
    buf += len;                                      \
  } while (0)

#define SERIALIZE_VAR_TO_BUF(buf, var, type)    \
  do {                                          \
    type c = var;                               \
    memcpy((void *)buf, (void *)&c, sizeof(c)); \
    buf += sizeof(c);                           \
  } while (0)

#define SERIALIZE_STR_VAR_TO_BUF(buf, var, len) \
  do {                                          \
    memcpy((void *)buf, (void *)var, len);      \
    buf += len;                                 \
  } while (0)

#define INDEX_MERGE_ADD_DEL(src, dst, tgt)            \
  {                                                   \
    bool f = false;                                   \
    for (int i = 0; i < taosArrayGetSize(src); i++) { \
      if (*(uint64_t *)taosArrayGet(src, i) == tgt) { \
        f = true;                                     \
      }                                               \
    }                                                 \
    if (f == false) {                                 \
      taosArrayPush(dst, &tgt);                       \
    }                                                 \
  }

/* multi sorted result intersection
 * input: [1, 2, 4, 5]
 *        [2, 3, 4, 5]
 *        [1, 4, 5]
 * output:[4, 5]
 */
void iIntersection(SArray *in, SArray *out);

/* multi sorted result union
 * input: [1, 2, 4, 5]
 *        [2, 3, 4, 5]
 *        [1, 4, 5]
 * output:[1, 2, 3, 4, 5]
 */
void iUnion(SArray *in, SArray *out);

/* see example
 * total:   [1, 2, 4, 5, 7, 8]
 * except:  [4, 5]
 * return:  [1, 2, 7, 8] saved in total
 */

void iExcept(SArray *total, SArray *except);

int uidCompare(const void *a, const void *b);

// data with ver
typedef struct {
  uint32_t ver;
  uint64_t data;
} SIdxVerdata;

/*
 * index temp result
 *
 */
typedef struct {
  SArray *total;
  SArray *add;
  SArray *del;
} SIdxTRslt;

SIdxTRslt *idxTRsltCreate();

void idxTRsltClear(SIdxTRslt *tr);

void idxTRsltDestroy(SIdxTRslt *tr);

void idxTRsltMergeTo(SIdxTRslt *tr, SArray *out);

#ifdef __cplusplus
}
#endif

#endif
