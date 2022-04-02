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

#define TDB_PCACHE_PAGE \
  u8      isAnchor;     \
  u8      isLocal;      \
  u8      isDirty;      \
  i32     nRef;         \
  SPage  *pCacheNext;   \
  SPage  *pFreeNext;    \
  SPage  *pHashNext;    \
  SPage  *pLruNext;     \
  SPage  *pLruPrev;     \
  SPage  *pDirtyNext;   \
  SPager *pPager;       \
  SPgid   pgid;

// For page ref
#define TDB_INIT_PAGE_REF(pPage) ((pPage)->nRef = 0)
#if 0
#define TDB_REF_PAGE(pPage)     (++(pPage)->nRef)
#define TDB_UNREF_PAGE(pPage)   (--(pPage)->nRef)
#define TDB_GET_PAGE_REF(pPage) ((pPage)->nRef)
#else
#define TDB_REF_PAGE(pPage)     atomic_add_fetch_32(&((pPage)->nRef), 1)
#define TDB_UNREF_PAGE(pPage)   atomic_sub_fetch_32(&((pPage)->nRef), 1)
#define TDB_GET_PAGE_REF(pPage) atomic_load_32(&((pPage)->nRef))
#endif

int    tdbPCacheOpen(int pageSize, int cacheSize, SPCache **ppCache);
int    tdbPCacheClose(SPCache *pCache);
SPage *tdbPCacheFetch(SPCache *pCache, const SPgid *pPgid, TXN *pTxn);
void   tdbPCacheRelease(SPCache *pCache, SPage *pPage);
int    tdbPCacheGetPageSize(SPCache *pCache);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PAGE_CACHE_H_*/