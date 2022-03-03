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

int    tdbPagerOpen(SPCache *pCache, const char *fileName, SPager **ppFile);
int    tdbPagerClose(SPager *pFile);
int    tdbPagerOpenDB(SPager *pFile, SPgno *ppgno, bool toCreate);
SPage *tdbPagerGet(SPager *pPager, SPgno pgno, bool toLoad);
int    tdbPagerWrite(SPager *pFile, SPage *pPage);
int    tdbPagerAllocPage(SPager *pFile, SPage **ppPage, SPgno *ppgno);
int    tdbPagerBegin(SPager *pFile);
int    tdbPagerCommit(SPager *pFile);
int    tdbPagerGetPageSize(SPager *pPager);

#ifdef __cplusplus
}
#endif

#endif /*_TDB_PAGER_H_*/