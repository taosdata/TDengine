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

#include "tdbInt.h"

struct SPFile {
  char *   dbFileName;
  char *   jFileName;
  int      pageSize;
  uint8_t  fid[TDB_FILE_ID_LEN];
  int      fd;
  int      jfd;
  SPCache *pCache;
  SPgno    dbFileSize;
  SPgno    dbOrigSize;
};

static int tdbPFileReadPage(SPFile *pFile, SPage *pPage);

int tdbPFileOpen(SPCache *pCache, const char *fileName, SPFile **ppFile) {
  uint8_t *pPtr;
  SPFile * pFile;
  int      fsize;
  int      zsize;

  *ppFile = NULL;

  fsize = strlen(fileName);
  zsize = sizeof(*pFile)   /* SPFile */
          + fsize + 1      /* dbFileName */
          + fsize + 8 + 1; /* jFileName */
  pPtr = (uint8_t *)calloc(1, zsize);
  if (pPtr == NULL) {
    return -1;
  }

  pFile = (SPFile *)pPtr;
  pPtr += sizeof(*pFile);
  pFile->dbFileName = (char *)pPtr;
  memcpy(pFile->dbFileName, fileName, fsize);
  pFile->dbFileName[fsize] = '\0';
  pPtr += fsize + 1;
  pFile->jFileName = (char *)pPtr;
  memcpy(pFile->jFileName, fileName, fsize);
  memcpy(pFile->jFileName + fsize, "-journal", 8);
  pFile->jFileName[fsize + 8] = '\0';

  pFile->fd = open(pFile->dbFileName, O_RDWR | O_CREAT, 0755);
  if (pFile->fd < 0) {
    return -1;
  }

  pFile->jfd = -1;

  *ppFile = pFile;
  return 0;
}

int tdbPFileClose(SPFile *pFile) {
  // TODO
  return 0;
}

SPage *tdbPFileGet(SPFile *pFile, SPgno pgno) {
  SPgid  pgid;
  SPage *pPage;

  memcpy(pgid.fileid, pFile->fid, TDB_FILE_ID_LEN);
  pgid.pgno = pgno;

  pPage = tdbPCacheFetch(pFile->pCache, &pgid, 1);
  if (pPage == NULL) {
    // TODO
    ASSERT(0);
  }
  tdbPCacheFetchFinish(pFile->pCache, pPage);

  if (!(pPage->isLoad)) {
    if (pgno > pFile->dbFileSize /*TODO*/) {
      memset(pPage->pData, 0, pFile->pageSize);
    } else {
      if (tdbPFileReadPage(pFile, pPage) < 0) {
        // TODO: handle error
        return NULL;
      }
    }

    pPage->isLoad = 1;
  }

  ASSERT(pPage->isLoad);

  return pPage;
}

int tdbPFileWrite(SPFile *pFile, SPage *pPage) {
  // TODO: if the page is not in journal, write to journal
  // mark the page as dirty
  return 0;
}

int tdbPFileAllocPage(SPFile *pFile, SPage **ppPage) {
  // TODO
  *ppPage = NULL;
  return 0;
}

int tdbPFileBegin(SPFile *pFile) {
  // TODO
  return 0;
}

int tdbPFileCommit(SPFile *pFile) {
  // TODO
  return 0;
}

int tdbPFileRollback(SPFile *pFile) {
  // TODO
  return 0;
}

static int tdbPFileReadPage(SPFile *pFile, SPage *pPage) {
  i64 offset;
  int ret;

  ASSERT(memcmp(pFile->fid, pPage->pgid.fileid, TDB_FILE_ID_LEN) == 0);

  offset = (pPage->pgid.pgno - 1) * (i64)(pFile->pageSize);
  ret = tdbPRead(pFile->fd, pPage->pData, pFile->pageSize, offset);
  if (ret < 0) {
    // TODO: handle error
    return -1;
  }
  return 0;
}