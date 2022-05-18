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

#include "sma.h"

int32_t smaOpenDBEnv(TENV **ppEnv, const char *path) {
  int ret = 0;

  if (path == NULL) return -1;

  ret = tdbEnvOpen(path, 4096, 256, ppEnv);  // use as param

  if (ret != 0) {
    smaError("failed to create tsdb db env, ret = %d", ret);
    return -1;
  }

  return 0;
}

int32_t smaCloseDBEnv(TENV *pEnv) { return tdbEnvClose(pEnv); }

static inline int tdSmaKeyCmpr(const void *arg1, int len1, const void *arg2, int len2) {
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

static int32_t smaOpenDBDb(TDB **ppDB, TENV *pEnv, const char *pFName) {
  tdb_cmpr_fn_t compFunc;

  // Create a database
  compFunc = tdSmaKeyCmpr;
  if (tdbOpen(pFName, -1, -1, compFunc, pEnv, ppDB) < 0) {
    return -1;
  }

  return 0;
}

static int32_t smaCloseDBDb(TDB *pDB) { return tdbClose(pDB); }

int32_t smaOpenDBF(TENV *pEnv, SDBFile *pDBF) {
  // TEnv is shared by a group of SDBFile
  if (!pEnv || !pDBF) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  // Open DBF
  if (smaOpenDBDb(&(pDBF->pDB), pEnv, pDBF->path) < 0) {
    smaError("failed to open DBF: %s", pDBF->path);
    smaCloseDBDb(pDBF->pDB);
    return -1;
  }

  return 0;
}

int32_t smaCloseDBF(SDBFile *pDBF) {
  int32_t ret = 0;
  if (pDBF->pDB) {
    ret = smaCloseDBDb(pDBF->pDB);
    pDBF->pDB = NULL;
  }
  taosMemoryFreeClear(pDBF->path);
  return ret;
}

int32_t smaSaveSmaToDB(SDBFile *pDBF, void *pKey, int32_t keyLen, void *pVal, int32_t valLen, TXN *txn) {
  int32_t ret;

  printf("save tsma data into %s, keyLen:%d valLen:%d txn:%p\n", pDBF->path, keyLen, valLen, txn);
  ret = tdbUpsert(pDBF->pDB, pKey, keyLen, pVal, valLen, txn);
  if (ret < 0) {
    smaError("failed to upsert tsma data into db, ret = %d", ret);
    return -1;
  }

  return 0;
}

void *smaGetSmaDataByKey(SDBFile *pDBF, const void *pKey, int32_t keyLen, int32_t *valLen) {
  void *pVal = NULL;
  int   ret;

  ret = tdbGet(pDBF->pDB, pKey, keyLen, &pVal, valLen);

  if (ret < 0) {
    smaError("failed to get tsma data from db, ret = %d", ret);
    return NULL;
  }

  ASSERT(*valLen >= 0);

  // TODO: lock?
  // TODO: Would the key/value be destoryed during return the data?
  // TODO: How about the key is updated while value length is changed? The original value buffer would be freed
  // automatically?

  return pVal;
}