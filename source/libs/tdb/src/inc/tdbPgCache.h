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

#ifndef _TD_PAGE_CACHE_H_
#define _TD_PAGE_CACHE_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SPgCache SPgCache;
typedef struct SPage    SPage;

// SPgCache
int pgCacheOpen(SPgCache **ppPgCache, TENV *pEnv);
int pgCacheClose(SPgCache *pPgCache);

SPage *pgCacheFetch(SPgCache *pPgCache, pgid_t pgid);
int    pgCacheRelease(SPage *pPage);

// SPage
typedef TD_DLIST_NODE(SPage) SPgListNode;
struct SPage {
  pgid_t      pgid;      // page id
  frame_id_t  frameid;   // frame id
  uint8_t *   pData;     // real data
  SPgListNode freeNode;  // for SPgCache.freeList
  SPgListNode pghtNode;  // for pght
  SPgListNode lruNode;   // for LRU
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_PAGE_CACHE_H_*/