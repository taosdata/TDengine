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
  pgsz_t      pgSize;     // Page size
  cachesz_t   cacheSize;  // Total cache size
  STDbList    dbList;     // TDB List
  SPgFileList pgfList;    // SPgFile List
  SPgCache *  pPgCache;   // page cache
  struct {
  } pgfht;  // page file hash table;
};

static int tdbEnvDestroy(TENV *pEnv);

int tdbEnvCreate(TENV **ppEnv) {
  TENV *pEnv;

  pEnv = (TENV *)calloc(1, sizeof(*pEnv));
  if (pEnv == NULL) {
    return -1;
  }

  pEnv->pgSize = TDB_DEFAULT_PGSIZE;
  pEnv->cacheSize = TDB_DEFAULT_CACHE_SIZE;

  TD_DLIST_INIT(&(pEnv->dbList));
  TD_DLIST_INIT(&(pEnv->pgfList));
  // TODO
  return 0;
}

int tdbEnvOpen(TENV **ppEnv) {
  TENV *    pEnv;
  SPgCache *pPgCache;
  int       ret;

  // Create the ENV with default setting
  if (ppEnv == NULL) {
    TERR_A(ret, tdbEnvCreate(&pEnv), _err);
  }

  pEnv = *ppEnv;

  TERR_A(ret, pgCacheCreate(&pPgCache, pEnv->pgSize, pEnv->cacheSize / pEnv->pgSize), _err);
  TERR_A(ret, pgCacheOpen(&pPgCache), _err);

  pEnv->pPgCache = pPgCache;

  return 0;

_err:
  return -1;
}

int tdbEnvClose(TENV *pEnv) {
  if (pEnv == NULL) return 0;
  /* TODO */
  tdbEnvDestroy(pEnv);
  return 0;
}

int tdbEnvSetPageSize(TENV *pEnv, pgsz_t szPage) {
  /* TODO */
  pEnv->pgSize = szPage;
  return 0;
}

int tdbEnvSetCacheSize(TENV *pEnv, cachesz_t szCache) {
  /* TODO */
  pEnv->cacheSize = szCache;
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