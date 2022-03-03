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

#ifndef _TDB_PAGE_H_
#define _TDB_PAGE_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct __attribute__((__packed__)) {
  u16 flags;
  u16 nCells;
  u16 cellCont;
  u16 freeCell;
  u16 nFree;
} SPageHdr;

typedef struct SPage SPage;
struct SPage {
  // Fields below used by page cache
  void *   pData;
  SPgid    pgid;
  u8       isAnchor;
  u8       isLocalPage;
  u8       isDirty;
  i32      nRef;
  SPCache *pCache;
  SPage *  pFreeNext;
  SPage *  pHashNext;
  SPage *  pLruNext;
  SPage *  pLruPrev;
  SPage *  pDirtyNext;
  SPager * pPager;
  // Fields below used by pager and am
  SPageHdr *pPageHdr;
  u16 *     aCellIdx;
  int       kLen;
  int       vLen;
  int       maxLocal;
  int       minLocal;
};

#ifdef __cplusplus
}
#endif

#endif /*_TDB_PAGE_H_*/