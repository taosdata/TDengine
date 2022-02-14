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
  SBTree * pBt;      // current access method
  SPgFile *pPgFile;  // backend page file this DB is using
  TENV *   pEnv;     // TENV containing the DB
};

struct STDbCurosr {
  SBtCursor *pBtCur;
};

int tdbCreate(TDB **ppDb) {
  TDB *pDb;

  pDb = (TDB *)calloc(1, sizeof(*pDb));
  if (pDb == NULL) {
    return -1;
  }

  /* TODO */

  return 0;
}

static int tdbDestroy(TDB *pDb) {
  if (pDb) {
    free(pDb);
  }
  return 0;
}

int tdbOpen(TDB **ppDb, const char *fname, const char *dbname, TENV *pEnv) {
  TDB *     pDb;
  int       ret;
  uint8_t   fileid[TDB_FILE_ID_LEN];
  SPgFile * pPgFile;
  SPgCache *pPgCache;
  SBTree *  pBt;

  // Create DB if DB handle is not created yet
  if (ppDb == NULL) {
    if ((ret = tdbCreate(ppDb)) != 0) {
      return -1;
    }
  }

  pDb = *ppDb;

  // Create a default ENV if pEnv is not set
  if (pEnv == NULL) {
    if ((ret = tdbEnvOpen(&pEnv)) != 0) {
      return -1;
    }
  }

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

  return 0;
}

int tdbClose(TDB *pDb) {
  if (pDb == NULL) return 0;
  return tdbDestroy(pDb);
}