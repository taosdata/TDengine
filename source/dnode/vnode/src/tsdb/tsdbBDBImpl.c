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

#define ALLOW_FORBID_FUNC
#include "db.h"

#include "tcoding.h"
#include "thash.h"
#include "tsdbDBDef.h"

#define IMPL_WITH_LOCK 1

struct SDBFile {
  DB *  pDB;
  char *path;
};

static int   tsdbOpenBDBEnv(DB_ENV **ppEnv, const char *path);
static void  tsdbCloseBDBEnv(DB_ENV *pEnv);
static int   tsdbOpenBDBDb(DB **ppDB, DB_ENV *pEnv, const char *pFName, bool isDup);
static void  tsdbCloseBDBDb(DB *pDB);

#define BDB_PERR(info, code) fprintf(stderr, info " reason: %s", db_strerror(code))

int tsdbOpenDBF(TDBEnv pEnv, SDBFile *pDBF) {
  // TDBEnv is shared by a group of SDBFile
  ASSERT(pEnv != NULL);

  // Open DBF
  if (tsdbOpenBDBDb(&(pDBF->pDB), pEnv, pDBF->path, false) < 0) {
    tsdbCloseBDBDb(pDBF->pDB);
    return -1;
  }

  return 0;
}

static void *tsdbFreeDBF(SDBFile *pDBF) {
  if (pDBF) {
    free(pDBF);
  }
  return NULL;
}

void tsdbCloseDBF(SDBFile *pDBF) {
  if (pDBF->pDB) {
    tsdbCloseBDBDb(pDBF->pDB);
    pDBF->pDB = tsdbFreeDBF(pDBF);
  }
}

static int tsdbOpenBDBEnv(DB_ENV **ppEnv, const char *path) {
  int     ret = 0;
  DB_ENV *pEnv = NULL;

  if (path == NULL) return 0;

  ret = db_env_create(&pEnv, 0);
  if (ret != 0) {
    BDB_PERR("Failed to create tsdb env", ret);
    return -1;
  }

  ret = pEnv->open(pEnv, path, DB_CREATE | DB_INIT_CDB | DB_INIT_MPOOL, 0);
  if (ret != 0) {
    BDB_PERR("Failed to open tsdb env", ret);
    return -1;
  }

  *ppEnv = pEnv;

  return 0;
}

static void tsdbCloseBDBEnv(DB_ENV *pEnv) {
  if (pEnv) {
    pEnv->close(pEnv, 0);
  }
}

static int tsdbOpenBDBDb(DB **ppDB, DB_ENV *pEnv, const char *pFName, bool isDup) {
  int ret;
  DB *pDB;

  ret = db_create(&(pDB), pEnv, 0);
  if (ret != 0) {
    BDB_PERR("Failed to create DBP", ret);
    return -1;
  }

  if (isDup) {
    ret = pDB->set_flags(pDB, DB_DUPSORT);
    if (ret != 0) {
      BDB_PERR("Failed to set DB flags", ret);
      return -1;
    }
  }

  ret = pDB->open(pDB, NULL, pFName, NULL, DB_BTREE, DB_CREATE, 0);
  if (ret) {
    BDB_PERR("Failed to open DBF", ret);
    return -1;
  }

  *ppDB = pDB;

  return 0;
}

static void tsdbCloseBDBDb(DB *pDB) {
  if (pDB) {
    pDB->close(pDB, 0);
  }
}