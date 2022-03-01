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

#ifndef _TD_PAGE_FILE_H_
#define _TD_PAGE_FILE_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SPager SPager;

int    tdbPagerOpen(SPCache *pCache, const char *fileName, SPager **ppFile);
int    tdbPagerClose(SPager *pFile);
int    tdbPagerOpenDB(SPager *pFile, SPgno *ppgno, bool toCreate);
SPage *tdbPagerGet(SPager *pFile, SPgno pgno);
int    tdbPagerWrite(SPager *pFile, SPage *pPage);
int    tdbPagerAllocPage(SPager *pFile, SPage **ppPage, SPgno *ppgno);
int    tdbPagerBegin(SPager *pFile);
int    tdbPagerCommit(SPager *pFile);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PAGE_FILE_H_*/