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

typedef struct SPFile SPFile;

int   tdbPFileOpen(SPCache *pCache, const char *fileName, SPFile **ppFile);
int   tdbPFileClose(SPFile *pFile);
void *tdbPFileGet(SPFile *pFile, SPgno pgno);
int   tdbPFileBegin(SPFile *pFile);
int   tdbPFileCommit(SPFile *pFile);
int   tdbPFileRollback(SPFile *pFile);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PAGE_FILE_H_*/