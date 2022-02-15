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

typedef struct __attribute__((__packed__)) {
  char    hdrInfo[16];  // info string
  pgsz_t  szPage;       // page size of current file
  int32_t cno;          // commit number counter
  pgno_t  freePgno;     // freelist page number
  uint8_t resv[100];    // reserved space
} SPgFileHdr;

#define TDB_PG_FILE_HDR_SIZE 128

TDB_STATIC_ASSERT(sizeof(SPgFileHdr) == TDB_PG_FILE_HDR_SIZE, "Page file header size if not 128");

struct SPgFile {
  TENV *          pEnv;                     // env containing this page file
  char *          fname;                    // backend file name
  uint8_t         fileid[TDB_FILE_ID_LEN];  // file id
  int             fd;
  SPgFileListNode envHash;
  // TDB *   pDb;  // For a SPgFile for multiple databases, this is the <dbname, pgno> mapping DB.
};

int pgFileOpen(SPgFile **ppPgFile, const char *fname, TENV *pEnv);
int pgFileClose(SPgFile *pPgFile);

SPage *pgFileFetch(SPgFile *pPgFile, pgno_t pgno);
int    pgFileRelease(SPage *pPage);

int pgFileWrite(SPage *pPage);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PAGE_FILE_H_*/