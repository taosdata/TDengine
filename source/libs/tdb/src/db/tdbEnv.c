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

struct STDbEnv {
  char *      rootDir;    // root directory of the environment
  pgsz_t      pgSize;     // page size
  cachesz_t   cacheSize;  // total cache size
  STDbList    dbList;     // TDB List
  SPgFileList pgfList;    // SPgFile List
  SPgCache *  pPgCache;   // page cache
  struct {
  } pgfht;  // page file hash table;
  SJournal *pJournal;
};

static int tdbEnvDestroy(TENV *pEnv);

int tdbEnvCreate(TENV **ppEnv, const char *rootDir) {
  TENV * pEnv;
  size_t slen;

  ASSERT(rootDir != NULL);

  *ppEnv = NULL;
  slen = strlen(rootDir);
  pEnv = (TENV *)calloc(1, sizeof(*pEnv) + slen + 1);
  if (pEnv == NULL) {
    return -1;
  }

  pEnv->rootDir = (char *)(&pEnv[1]);
  pEnv->pgSize = TDB_DEFAULT_PGSIZE;
  pEnv->cacheSize = TDB_DEFAULT_CACHE_SIZE;

  memcpy(pEnv->rootDir, rootDir, slen);
  pEnv->rootDir[slen] = '\0';

  TD_DLIST_INIT(&(pEnv->dbList));
  TD_DLIST_INIT(&(pEnv->pgfList));

  /* TODO */

  *ppEnv = pEnv;
  return 0;
}

int tdbEnvOpen(TENV *pEnv) {
  SPgCache *pPgCache;
  int       ret;

  ASSERT(pEnv != NULL);

  /* TODO: here we do not need to create the root directory, more
   * work should be done here
   */
  mkdir(pEnv->rootDir, 0755);

  ret = pgCacheOpen(&pPgCache, pEnv);
  if (ret != 0) {
    goto _err;
  }

  pEnv->pPgCache = pPgCache;
  return 0;

_err:
  return -1;
}

int tdbEnvClose(TENV *pEnv) {
  if (pEnv == NULL) return 0;
  pgCacheClose(pEnv->pPgCache);
  tdbEnvDestroy(pEnv);
  return 0;
}

int tdbEnvSetCache(TENV *pEnv, pgsz_t pgSize, cachesz_t cacheSize) {
  if (!TDB_IS_PGSIZE_VLD(pgSize) || cacheSize / pgSize < 10) {
    return -1;
  }

  /* TODO */

  pEnv->pgSize = pgSize;
  pEnv->cacheSize = cacheSize;

  return 0;
}

pgsz_t tdbEnvGetPageSize(TENV *pEnv) { return pEnv->pgSize; }

cachesz_t tdbEnvGetCacheSize(TENV *pEnv) { return pEnv->cacheSize; }

SPgFile *tdbEnvGetPageFile(TENV *pEnv, const uint8_t fileid[]) {
  // TODO
  return NULL;
}

SPgCache *tdbEnvGetPgCache(TENV *pEnv) { return pEnv->pPgCache; }

static int tdbEnvDestroy(TENV *pEnv) {
  // TODO
  return 0;
}

int tdbEnvBeginTxn(TENV *pEnv) {
  // TODO
  return 0;
}

int tdbEnvCommit(TENV *pEnv) {
  // TODO
  return 0;
}

const char *tdbEnvGetRootDir(TENV *pEnv) { return pEnv->rootDir; }
