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

typedef struct SPCache SPCache;
typedef struct SPgHdr  SPgHdr;

struct SPgHdr {
  void *   pData;
  void *   pExtra;
  SPgid    pgid;
  uint8_t  isAnchor;
  uint8_t  isLocalPage;
  SPCache *pCache;
  SPgHdr * pFreeNext;
  SPgHdr * pHashNext;
  SPgHdr * pLruNext;
  SPgHdr * pLruPrev;
};

int     tdbOpenPCache(int pageSize, int cacheSize, int extraSize, SPCache **ppCache);
int     tdbPCacheClose(SPCache *pCache);
SPgHdr *tdbPCacheFetch(SPCache *pCache, const SPgid *pPgid, bool alcNewPage);
void    tdbFetchFinish(SPCache *pCache, SPgHdr *pPage);
void    tdbPCacheRelease(SPgHdr *pHdr);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PAGE_CACHE_H_*/