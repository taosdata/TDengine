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

int tdbEnvOpen(const char *rootDir, int pageSize, int cacheSize, TENV **ppEnv) {
  TENV *pEnv;
  int   dsize;
  int   zsize;
  int   tsize;
  u8   *pPtr;
  int   ret;

  *ppEnv = NULL;

  dsize = strlen(rootDir);
  zsize = sizeof(*pEnv) + dsize * 2 + strlen(TDB_JOURNAL_NAME) + 3;

  pPtr = (uint8_t *)tdbOsCalloc(1, zsize);
  if (pPtr == NULL) {
    return -1;
  }

  pEnv = (TENV *)pPtr;
  pPtr += sizeof(*pEnv);
  // pEnv->rootDir
  pEnv->rootDir = pPtr;
  memcpy(pEnv->rootDir, rootDir, dsize);
  pEnv->rootDir[dsize] = '\0';
  pPtr = pPtr + dsize + 1;
  // pEnv->jfname
  pEnv->jfname = pPtr;
  memcpy(pEnv->jfname, rootDir, dsize);
  pEnv->jfname[dsize] = '/';
  memcpy(pEnv->jfname + dsize + 1, TDB_JOURNAL_NAME, strlen(TDB_JOURNAL_NAME));
  pEnv->jfname[dsize + 1 + strlen(TDB_JOURNAL_NAME)] = '\0';

  pEnv->jfd = -1;

  ret = tdbPCacheOpen(pageSize, cacheSize, &(pEnv->pCache));
  if (ret < 0) {
    return -1;
  }

  pEnv->nHash = 8;
  tsize = sizeof(SPager *) * pEnv->nHash;
  pEnv->pagerHash = TDB_REALLOC(pEnv->pagerHash, tsize);
  if (pEnv->pagerHash == NULL) {
    return -1;
  }
  memset(pEnv->pagerHash, 0, tsize);

  mkdir(rootDir, 0755);

  *ppEnv = pEnv;
  return 0;
}

int tdbEnvClose(TENV *pEnv) {
  // TODO
  return 0;
}

int tdbBegin(TENV *pEnv) {
  ASSERT(0);
  return 0;
}

int tdbCommit(TENV *pEnv) {
  SPager *pPager;

  pPager = pEnv->pagerList;
  while (pPager) {
    tdbPagerCommit(pPager);
  }

  return 0;
}

int tdbRollback(TENV *pEnv) {
  ASSERT(0);
  return 0;
}

SPager *tdbEnvGetPager(TENV *pEnv, const char *fname) {
  // TODO
  return NULL;
}

static void tdbEnvAddPager(TENV *pEnv, SPager *pPager) {

}