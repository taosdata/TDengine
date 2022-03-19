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
  u8       isAnchor;    \
  u8       isLocalPage; \
  u8       isDirty;     \
  i32      nRef;        \
  SPCache *pCache;      \
  SPage   *pFreeNext;   \
  SPage   *pHashNext;   \
  SPage   *pLruNext;    \
  SPage   *pLruPrev;    \
  SPage   *pDirtyNext;  \
  SPager  *pPager;      \
  SPgid    pgid;

int    tdbPCacheOpen(int pageSize, int cacheSize, SPCache **ppCache);
int    tdbPCacheClose(SPCache *pCache);
SPage *tdbPCacheFetch(SPCache *pCache, const SPgid *pPgid, bool alcNewPage);
void   tdbPCacheRelease(SPage *pPage);
int    tdbPCacheGetPageSize(SPCache *pCache);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PAGE_CACHE_H_*/