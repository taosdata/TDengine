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

static int pgFileRead(SPgFile *pPgFile, pgno_t pgno, uint8_t *pData);

int pgFileOpen(SPgFile **ppPgFile, const char *fname, TENV *pEnv) {
  SPgFile * pPgFile;
  SPgCache *pPgCache;
  size_t    fnameLen;

  *ppPgFile = NULL;

  // create the handle
  fnameLen = strlen(fname);
  pPgFile = (SPgFile *)calloc(1, sizeof(*pPgFile) + fnameLen + 1);
  if (pPgFile == NULL) {
    return -1;
  }

  ASSERT(pEnv != NULL);

  // init the handle
  pPgFile->pEnv = pEnv;
  pPgFile->fname = (char *)(&(pPgFile[1]));
  memcpy(pPgFile->fname, fname, fnameLen);
  pPgFile->fname[fnameLen] = '\0';
  pPgFile->fd = -1;

  pPgFile->fd = open(fname, O_CREAT | O_RDWR, 0755);
  if (pPgFile->fd < 0) {
    // TODO: handle error
    return -1;
  }

  tdbGnrtFileID(fname, pPgFile->fileid, false);

  /* TODO */

  *ppPgFile = pPgFile;
  return 0;
}

int pgFileClose(SPgFile *pPgFile) {
  if (pPgFile) {
    if (pPgFile->fd >= 0) {
      close(pPgFile->fd);
    }

    tfree(pPgFile->fname);
    free(pPgFile);
  }

  return 0;
}

SPage *pgFileFetch(SPgFile *pPgFile, pgno_t pgno) {
  SPgCache *pPgCache;
  SPage *   pPage;
  pgid_t    pgid;

#if 0
  pPgCache = pPgFile->pPgCache;
  pPage = NULL;
  memcpy(pgid.fileid, pPgFile->fileid, TDB_FILE_ID_LEN);
  pgid.pgno = pgno;

  if (pgno > pPgFile->pgFileSize) {
    // TODO
  } else {
    pPage = pgCacheFetch(pPgCache, pgid);
    if (1 /*Page is cached, no need to load from file*/) {
      return pPage;
    } else {
      // TODO: handle error
      if (pgFileRead(pPgFile, pgno, (void *)pPage) < 0) {
        // todoerr
      }
      return pPage;
    }
  }
#endif

  return pPage;
}

int pgFileRelease(SPage *pPage) {
  pgCacheRelease(pPage);
  return 0;
}

int pgFileWrite(SPage *pPage) {
  // TODO
  return 0;
}

static int pgFileRead(SPgFile *pPgFile, pgno_t pgno, uint8_t *pData) {
  pgsz_t   pgSize;
  ssize_t  rsize;
  uint8_t *pTData;
  size_t   szToRead;

#if 0

  // pgSize = ; (TODO)
  pTData = pData;
  szToRead = pgSize;
  for (; szToRead > 0;) {
    rsize = pread(pPgFile->fd, pTData, szToRead, pgno * pgSize);
    if (rsize < 0) {
      if (errno == EINTR) {
        continue;
      } else {
        return -1;
      }
    } else if (rsize == 0) {
      return -1;
    }

    szToRead -= rsize;
    pTData += rsize;
  }
#endif

  return 0;
}