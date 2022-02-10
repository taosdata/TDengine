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

struct SPgFile {
  char *    fname;                    // backend file name
  uint8_t   fileid[TDB_FILE_ID_LEN];  // file id
  SPgCache *pPgCache;                 // page cache underline
  pgsize_t  pgSize;
  int       fd;
  pgno_t    pgFileSize;
};

int pgFileOpen(SPgFile **ppPgFile, const char *fname, SPgCache *pPgCache);
int pgFileClose(SPgFile *pPgFile);

SPage *pgFileFetch(SPgFile *pPgFile, pgno_t pgno);
int    pgFileRelease(SPage *pPage);

int pgFileWrite(SPage *pPage);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PAGE_FILE_H_*/