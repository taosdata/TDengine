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

struct STDb {
  char         dbname[TDB_MAX_DBNAME_LEN];
  SBTree *     pBt;      // current access method (may extend)
  SPgFile *    pPgFile;  // backend page file this DB is using
  TENV *       pEnv;     // TENV containing the DB
  int          klen;     // key length if know
  int          vlen;     // value length if know
  bool         dup;      // dup mode
  TdbKeyCmprFn cFn;      // compare function
};

struct STDbCurosr {
  SBtCursor *pBtCur;
};

static int tdbDefaultKeyCmprFn(int keyLen1, const void *pKey1, int keyLen2, const void *pKey2);

int tdbCreate(TDB **ppDb) {
  TDB *pDb;

  // create the handle
  pDb = (TDB *)calloc(1, sizeof(*pDb));
  if (pDb == NULL) {
    return -1;
  }

  pDb->klen = TDB_VARIANT_LEN;
  pDb->vlen = TDB_VARIANT_LEN;
  pDb->dup = false;
  pDb->cFn = tdbDefaultKeyCmprFn;

  *ppDb = pDb;
  return 0;
}

static int tdbDestroy(TDB *pDb) {
  if (pDb) {
    free(pDb);
  }
  return 0;
}

int tdbOpen(TDB *pDb, const char *fname, const char *dbname, TENV *pEnv) {
  int       ret;
  uint8_t   fileid[TDB_FILE_ID_LEN];
  SPgFile * pPgFile;
  SPgCache *pPgCache;
  SBTree *  pBt;
  bool      fileExist;
  size_t    dbNameLen;
  char      dbfname[128];  // TODO: make this as a macro or malloc on the heap

  ASSERT(pDb != NULL);
  ASSERT(fname != NULL);
  // TODO: Here we simply put an assert here. In the future, make `pEnv`
  // can be set as NULL.
  ASSERT(pEnv != NULL);

  // check the DB name
  dbNameLen = 0;
  if (dbname) {
    dbNameLen = strlen(dbname);
    if (dbNameLen >= TDB_MAX_DBNAME_LEN) {
      return -1;
    }

    memcpy(pDb->dbname, dbname, dbNameLen);
  }

  pDb->dbname[dbNameLen] = '\0';

  // open pPgFile or get from the env
  snprintf(dbfname, 128, "%s/%s", tdbEnvGetRootDir(pEnv), fname);
  fileExist = (tdbCheckFileAccess(fname, TDB_F_OK) == 0);
  if (fileExist) {
    // TODO
  } else {
    ret = pgFileOpen(&pPgFile, dbfname, pEnv);
    if (ret != 0) {
      // TODO: handle error
      return -1;
    }
    // Create and open the page file
  }

#if 0
  pDb->pEnv = pEnv;

  // register DB to ENV

  ASSERT(fname != NULL);

  // Check if file exists
  if (tdbCheckFileAccess(fname, TDB_F_OK) != 0) {
    if (1) {
      // create the file
    }
  }

  // Check if the SPgFile already opened
  tdbGnrtFileID(fname, fileid, false);
  pPgFile = tdbEnvGetPageFile(pEnv, fileid);
  if (pPgFile == NULL) {
    pPgCache = tdbEnvGetPgCache(pEnv);
    if ((ret = pgFileOpen(&pPgFile, fname, pPgCache)) != 0) {
      return -1;
    }
  }

  pDb->pPgFile = pPgFile;

  // open the access method (TODO)
  if (btreeOpen(&pBt, pPgFile) != 0) {
    return -1;
  }

  pDb->pBt = pBt;
#endif

  return 0;
}

int tdbClose(TDB *pDb) {
  if (pDb == NULL) return 0;
  return tdbDestroy(pDb);
}

int tdbSetKeyLen(TDB *pDb, int klen) {
  // TODO: check `klen`
  pDb->klen = klen;
  return 0;
}

int tdbSetValLen(TDB *pDb, int vlen) {
  // TODO: check `vlen`
  pDb->vlen = vlen;
  return 0;
}

int tdbSetDup(TDB *pDb, int dup) {
  if (dup) {
    pDb->dup = true;
  } else {
    pDb->dup = false;
  }
  return 0;
}

int tdbSetCmprFunc(TDB *pDb, TdbKeyCmprFn fn) {
  if (fn == NULL) {
    return -1;
  } else {
    pDb->cFn = fn;
  }
  return 0;
}

int tdbGetKeyLen(TDB *pDb) { return pDb->klen; }

int tdbGetValLen(TDB *pDb) { return pDb->vlen; }

int tdbGetDup(TDB *pDb) {
  if (pDb->dup) {
    return 1;
  } else {
    return 0;
  }
}

static int tdbDefaultKeyCmprFn(int keyLen1, const void *pKey1, int keyLen2, const void *pKey2) {
  int mlen;
  int cret;

  ASSERT(keyLen1 > 0 && keyLen2 > 0 && pKey1 != NULL && pKey2 != NULL);

  mlen = keyLen1 < keyLen2 ? keyLen1 : keyLen2;
  cret = memcmp(pKey1, pKey2, mlen);
  if (cret == 0) {
    if (keyLen1 < keyLen2) {
      cret = -1;
    } else if (keyLen1 > keyLen2) {
      cret = 1;
    } else {
      cret = 0;
    }
  }
  return cret;
}