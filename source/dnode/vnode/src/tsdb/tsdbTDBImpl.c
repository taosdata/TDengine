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

#include "tsdb.h"

int32_t tsdbOpenDBEnv(TENV **ppEnv, const char *path) {
  int ret = 0;

  if (path == NULL) return -1;

  ret = tdbEnvOpen(path, 4096, 256, ppEnv);  // use as param

  if (ret != 0) {
    tsdbError("Failed to create tsdb db env, ret = %d", ret);
    return -1;
  }

  return 0;
}

int32_t tsdbCloseDBEnv(TENV *pEnv) { return tdbEnvClose(pEnv); }

static inline int tsdbSmaKeyCmpr(const void *arg1, int len1, const void *arg2, int len2) {
  const SSmaKey *pKey1 = (const SSmaKey *)arg1;
  const SSmaKey *pKey2 = (const SSmaKey *)arg2;

  ASSERT(len1 == len2 && len1 == sizeof(SSmaKey));

  if (pKey1->skey < pKey2->skey) {
    return -1;
  } else if (pKey1->skey > pKey2->skey) {
    return 1;
  }
  if (pKey1->groupId < pKey2->groupId) {
    return -1;
  } else if (pKey1->groupId > pKey2->groupId) {
    return 1;
  }

  return 0;
}

static int32_t tsdbOpenDBDb(TDB **ppDB, TENV *pEnv, const char *pFName) {
  int           ret;
  tdb_cmpr_fn_t compFunc;

  // Create a database
  compFunc = tsdbSmaKeyCmpr;
  ret = tdbOpen(pFName, -1, -1, compFunc, pEnv, ppDB);

  return 0;
}

static int32_t tsdbCloseDBDb(TDB *pDB) { return tdbClose(pDB); }

int32_t tsdbOpenDBF(TENV *pEnv, SDBFile *pDBF) {
  // TEnv is shared by a group of SDBFile
  if (!pEnv || !pDBF) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  // Open DBF
  if (tsdbOpenDBDb(&(pDBF->pDB), pEnv, pDBF->path) < 0) {
    terrno = TSDB_CODE_TDB_INIT_FAILED;
    tsdbCloseDBDb(pDBF->pDB);
    return -1;
  }

  return 0;
}

int32_t tsdbCloseDBF(SDBFile *pDBF) {
  int32_t ret = 0;
  if (pDBF->pDB) {
    ret = tsdbCloseDBDb(pDBF->pDB);
    pDBF->pDB = NULL;
  }
  taosMemoryFreeClear(pDBF->path);
  return ret;
}

int32_t tsdbSaveSmaToDB(SDBFile *pDBF, void *pKey, int32_t keyLen, void *pVal, int32_t valLen, TXN *txn) {
  int32_t ret;

  ret = tdbInsert(pDBF->pDB, pKey, keyLen, pVal, valLen, txn);
  if (ret < 0) {
    tsdbError("Failed to create insert sma data into db, ret = %d", ret);
    return -1;
  }

  return 0;
}

void *tsdbGetSmaDataByKey(SDBFile *pDBF, const void *pKey, int32_t keyLen, int32_t *valLen) {
  void *pVal = NULL;
  int   ret;

  ret = tdbGet(pDBF->pDB, pKey, keyLen, &pVal, valLen);

  if (ret < 0) {
    tsdbError("Failed to get sma data from db, ret = %d", ret);
    return NULL;
  }

  ASSERT(*valLen >= 0);

  // TODO: lock?
  // TODO: Would the key/value be destoryed during return the data?
  // TODO: How about the key is updated while value length is changed? The original value buffer would be freed
  // automatically?

  return pVal;
}