/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "tskiplist2.h"

typedef struct SSLNode SSLNode;
struct SSLNode {
  int8_t   level;
  SSLNode *forwards[];
};

struct SSkipList2 {
  int8_t        level;
  uint32_t      seed;
  int32_t       size;
  const SSLCfg *pCfg;
};

struct SSLCursor {
  SSkipList2 *pSl;
  SSLNode    *forwards[SL_MAX_LEVEL];
};

static void   *slMalloc(void *pPool, int32_t size);
static void    slFree(void *pPool, void *p);
static int32_t slCmprFn(const void *pKey, int32_t nKey, const void *pData, int32_t nData);

const SSLCfg slDefaultCfg = {.maxLevel = SL_MAX_LEVEL,
                             .nKey = -1,
                             .nData = -1,
                             .cmprFn = slCmprFn,
                             .pPool = NULL,
                             .xMalloc = slMalloc,
                             .xFree = slFree};

int32_t slOpen(const SSLCfg *pCfg, SSkipList2 **ppSl) {
  SSkipList2 *pSl = NULL;

  *ppSl = NULL;
  if (pCfg == NULL) pCfg = &slDefaultCfg;

  // check cfg (TODO)

  // create handle
  pSl = pCfg->xMalloc(pCfg->pPool, sizeof(*pSl));
  // (TODO)
  *ppSl = pSl;
  return 0;
}

int32_t slClose(SSkipList2 *pSl) {
  // TODO
  return 0;
}

int32_t slcOpen(SSkipList2 *pSl, SSLCursor *pSlc) {
  // TODO
  return 0;
}

int32_t slcClose(SSLCursor *pSlc) {
  // TODO
  return 0;
}

int32_t slcMoveTo(SSLCursor *pSlc, const void *pKey, int32_t nKey) {
  // TODO
  return 0;
}

int32_t slcMoveToNext(SSLCursor *pSlc) {
  // TODO
  return 0;
}

int32_t slcMoveToPrev(SSLCursor *pSlc) {
  // TODO
  return 0;
}

int32_t slcMoveToFirst(SSLCursor *pSlc) {
  // TODO
  return 0;
}

int32_t slcMoveToLast(SSLCursor *pSlc) {
  // TODO
  return 0;
}

int32_t slcPut(SSLCursor *pSlc, const void *pKey, int32_t nKey, const void *pData, int32_t nData) {
  // TODO
  return 0;
}

int32_t slcGet(SSLCursor *pSlc, const void **ppKey, int32_t *nKey, const void **ppData, int32_t *nData) {
  // TODO
  return 0;
}

int32_t slcDrop(SSLCursor *pSlc) {
  // TODO
  return 0;
}

static FORCE_INLINE void *slMalloc(void *pPool, int32_t size) { return taosMemoryMalloc(size); }

static FORCE_INLINE void slFree(void *pPool, void *p) { taosMemoryFree(p); }

static int32_t slCmprFn(const void *pKey1, int32_t nKey1, const void *pKey2, int32_t nKey2) {
  ASSERT(nKey1 >= 0 && nKey2 >= 0);

  int32_t nKey = nKey1 > nKey2 ? nKey2 : nKey1;
  int32_t c;

  c = memcmp(pKey1, pKey2, nKey);
  if (c == 0) {
    if (nKey1 > nKey2) {
      c = 1;
    } else if (nKey1 < nKey2) {
      c = -1;
    }
  }

  return c;
}