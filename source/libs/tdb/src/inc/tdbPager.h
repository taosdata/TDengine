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

int    tdbPagerOpen(SPCache *pCache, const char *fileName, SPager **ppPager);
int    tdbPagerClose(SPager *pPager);
int    tdbPagerOpenDB(SPager *pPager, SPgno *ppgno, bool toCreate);
SPage *tdbPagerGet(SPager *pPager, SPgno pgno, bool toLoad);
int    tdbPagerWrite(SPager *pPager, SPage *pPage);
int    tdbPagerAllocPage(SPager *pPager, SPage **ppPage, SPgno *ppgno);
int    tdbPagerBegin(SPager *pPager);
int    tdbPagerCommit(SPager *pPager);
int    tdbPagerGetPageSize(SPager *pPager);

#ifdef __cplusplus
}
#endif

#endif /*_TDB_PAGER_H_*/