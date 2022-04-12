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

#ifndef _TDB_PAGER_H_
#define _TDB_PAGER_H_

#ifdef __cplusplus
extern "C" {
#endif

struct SPager {
  char    *dbFileName;
  char    *jFileName;
  int      pageSize;
  uint8_t  fid[TDB_FILE_ID_LEN];
  tdb_fd_t fd;
  tdb_fd_t jfd;
  SPCache *pCache;
  SPgno    dbFileSize;
  SPgno    dbOrigSize;
  SPage   *pDirty;
  u8       inTran;
  SPager  *pNext;      // used by TENV
  SPager  *pHashNext;  // used by TENV
};

int  tdbPagerOpen(SPCache *pCache, const char *fileName, SPager **ppPager);
int  tdbPagerClose(SPager *pPager);
int  tdbPagerOpenDB(SPager *pPager, SPgno *ppgno, bool toCreate);
int  tdbPagerWrite(SPager *pPager, SPage *pPage);
int  tdbPagerBegin(SPager *pPager, TXN *pTxn);
int  tdbPagerCommit(SPager *pPager, TXN *pTxn);
int  tdbPagerFetchPage(SPager *pPager, SPgno pgno, SPage **ppPage, int (*initPage)(SPage *, void *), void *arg,
                       TXN *pTxn);
int  tdbPagerNewPage(SPager *pPager, SPgno *ppgno, SPage **ppPage, int (*initPage)(SPage *, void *), void *arg,
                     TXN *pTxn);
void tdbPagerReturnPage(SPager *pPager, SPage *pPage, TXN *pTxn);
int  tdbPagerAllocPage(SPager *pPager, SPgno *ppgno);

#ifdef __cplusplus
}
#endif

#endif /*_TDB_PAGER_H_*/