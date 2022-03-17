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

#include "taoserror.h"
#include "tcoding.h"
#include "thash.h"
#include "tsdbDBDef.h"
#include "tsdbLog.h"

#define IMPL_WITH_LOCK 1

static int  tsdbOpenBDBDb(DB **ppDB, DB_ENV *pEnv, const char *pFName, bool isDup);
static void tsdbCloseBDBDb(DB *pDB);

#define BDB_PERR(info, code) fprintf(stderr, "%s:%d " info " reason: %s\n", __FILE__, __LINE__, db_strerror(code))

int32_t tsdbOpenDBF(TDBEnv pEnv, SDBFile *pDBF) {
  // TDBEnv is shared by a group of SDBFile
  if (!pEnv) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  // Open DBF
  if (tsdbOpenBDBDb(&(pDBF->pDB), pEnv, pDBF->path, false) < 0) {
    terrno = TSDB_CODE_TDB_INIT_FAILED;
    tsdbCloseBDBDb(pDBF->pDB);
    return -1;
  }

  return 0;
}

void tsdbCloseDBF(SDBFile *pDBF) {
  if (pDBF->pDB) {
    tsdbCloseBDBDb(pDBF->pDB);
    pDBF->pDB = NULL;
  }
  tfree(pDBF->path);
}

int32_t tsdbOpenBDBEnv(DB_ENV **ppEnv, const char *path) {
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
    // BDB_PERR("Failed to open tsdb env", ret);
    tsdbWarn("Failed to open tsdb env for path %s since %d", path ? path : "NULL", ret);
    return -1;
  }

  *ppEnv = pEnv;

  return 0;
}

void tsdbCloseBDBEnv(DB_ENV *pEnv) {
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

int32_t tsdbSaveSmaToDB(SDBFile *pDBF, void *key, uint32_t keySize, void *data, uint32_t dataSize) {
  int ret;
  DBT key1 = {0}, value1 = {0};

  key1.data = key;
  key1.size = keySize;

  value1.data = data;
  value1.size = dataSize;

  // TODO: lock
  ret = pDBF->pDB->put(pDBF->pDB, NULL, &key1, &value1, 0);
  if (ret) {
    BDB_PERR("Failed to put data to DBF", ret);
    // TODO: unlock
    return -1;
  }
  // TODO: unlock

  return 0;
}

void *tsdbGetSmaDataByKey(SDBFile *pDBF, void* key, uint32_t keySize, uint32_t *valueSize) {
  void *result = NULL;
  DBT   key1 = {0};
  DBT   value1 = {0};
  int   ret;

  // Set key/value
  key1.data = key;
  key1.size = keySize;

  // Query
  // TODO: lock
  ret = pDBF->pDB->get(pDBF->pDB, NULL, &key1, &value1, 0);
  // TODO: unlock
  if (ret != 0) {
    return NULL;
  }

  result = calloc(1, value1.size);

  if (result == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  *valueSize = value1.size;
  memcpy(result, value1.data, value1.size);

  return result;
}