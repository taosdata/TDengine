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
  char *      jname;      // journal file name
  TdFilePtr   jpFile;        // journal file fd
  pgsz_t      pgSize;     // page size
  cachesz_t   cacheSize;  // total cache size
  STDbList    dbList;     // TDB List
  SPgFileList pgfList;    // SPgFile List
  SPgCache *  pPgCache;   // page cache
  struct {
#define TDB_ENV_PGF_HASH_BUCKETS 17
    SPgFileList buckets[TDB_ENV_PGF_HASH_BUCKETS];
  } pgfht;  // page file hash table;
};

#define TDB_ENV_PGF_HASH(fileid) (((uint8_t *)(fileid))[0] + ((uint8_t *)(fileid))[1] + ((uint8_t *)(fileid))[2])

static int tdbEnvDestroy(TENV *pEnv);

int tdbEnvCreate(TENV **ppEnv, const char *rootDir) {
  TENV * pEnv;
  size_t slen;
  size_t jlen;

  ASSERT(rootDir != NULL);

  *ppEnv = NULL;
  slen = strlen(rootDir);
  jlen = slen + strlen(TDB_JOURNAL_NAME) + 1;
  pEnv = (TENV *)calloc(1, sizeof(*pEnv) + slen + 1 + jlen + 1);
  if (pEnv == NULL) {
    return -1;
  }

  pEnv->rootDir = (char *)(&pEnv[1]);
  pEnv->jname = pEnv->rootDir + slen + 1;
  pEnv->jpFile = NULL;
  pEnv->pgSize = TDB_DEFAULT_PGSIZE;
  pEnv->cacheSize = TDB_DEFAULT_CACHE_SIZE;

  memcpy(pEnv->rootDir, rootDir, slen);
  pEnv->rootDir[slen] = '\0';
  sprintf(pEnv->jname, "%s/%s", rootDir, TDB_JOURNAL_NAME);

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
  SPgFileList *pBucket;
  SPgFile *    pPgFile;

  pBucket = pEnv->pgfht.buckets + (TDB_ENV_PGF_HASH(fileid) % TDB_ENV_PGF_HASH_BUCKETS);  // TODO
  for (pPgFile = TD_DLIST_HEAD(pBucket); pPgFile != NULL; pPgFile = TD_DLIST_NODE_NEXT_WITH_FIELD(pPgFile, envHash)) {
    if (memcmp(fileid, pPgFile->fileid, TDB_FILE_ID_LEN) == 0) break;
  };

  return pPgFile;
}

SPgCache *tdbEnvGetPgCache(TENV *pEnv) { return pEnv->pPgCache; }

static int tdbEnvDestroy(TENV *pEnv) {
  // TODO
  return 0;
}

int tdbEnvBeginTxn(TENV *pEnv) {
  pEnv->jpFile = taosOpenFile(pEnv->jname, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_READ);
  if (pEnv->jpFile == NULL) {
    return -1;
  }

  return 0;
}

int tdbEnvCommit(TENV *pEnv) {
  /* TODO */
  taosCloseFile(&pEnv->jpFile);
  pEnv->jpFile = NULL;
  return 0;
}

const char *tdbEnvGetRootDir(TENV *pEnv) { return pEnv->rootDir; }

int tdbEnvRgstPageFile(TENV *pEnv, SPgFile *pPgFile) {
  SPgFileList *pBucket;

  TD_DLIST_APPEND_WITH_FIELD(&(pEnv->pgfList), pPgFile, envPgfList);

  pBucket = pEnv->pgfht.buckets + (TDB_ENV_PGF_HASH(pPgFile->fileid) % TDB_ENV_PGF_HASH_BUCKETS);  // TODO
  TD_DLIST_APPEND_WITH_FIELD(pBucket, pPgFile, envHash);

  return 0;
}

int tdbEnvRgstDB(TENV *pEnv, TDB *pDb) {
  // TODO
  return 0;
}