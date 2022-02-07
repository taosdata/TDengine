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
#include "tdbInt.h"

struct SPage {
  pgid_t pgid;  // page id
  // TODO
};

typedef TD_DLIST(SPage) SPgList;

struct SPgCache {
  SPage *pages;

  SPgList freeList;

  struct {
    int32_t nbucket;
    struct {
      SRWLatch latch;
      TD_DLIST(SPage) ht;
    } * buckets;
  } pght;  // page hash table
};

int pgCacheCreate(SPgCache **ppPgCache) {
  SPgCache *pPgCache;

  pPgCache = (SPgCache *)calloc(1, sizeof(*pPgCache));
  if (pPgCache == NULL) {
    return -1;
  }

  *ppPgCache = pPgCache;
  return 0;
}

int pgCacheDestroy(SPgCache *pPgCache) {
  if (pPgCache) {
    free(pPgCache);
  }

  return 0;
}

int pgCacheOpen(SPgCache *pPgCache) {
  // TODO
  return 0;
}

int pgCacheClose(SPgCache *pPgCache) {
  // TODO
  return 0;
}

SPage *pgCacheFetch(SPgCache *pPgCache) {
  // TODO
  return NULL;
}

int pgCacheRelease(SPage *pPage) {
  // TODO
  return 0;
}