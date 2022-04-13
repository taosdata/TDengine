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

#ifndef _TD_VNODE_TSDB_SMA_H_
#define _TD_VNODE_TSDB_SMA_H_

#include "tdbInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSmaKey SSmaKey;

struct SSmaKey {
  TSKEY   skey;
  int64_t groupId;
};


typedef struct SDBFile SDBFile;

struct SDBFile {
  int32_t fid;
  TDB    *pDB;
  char   *path;
};

int32_t tsdbOpenDBEnv(TENV **ppEnv, const char *path);
int32_t tsdbCloseDBEnv(TENV *pEnv);
int32_t tsdbOpenDBF(TENV *pEnv, SDBFile *pDBF);
int32_t tsdbCloseDBF(SDBFile *pDBF);
int32_t tsdbSaveSmaToDB(SDBFile *pDBF, void *pKey, int32_t keyLen, void *pVal, int32_t valLen, TXN *txn);
void   *tsdbGetSmaDataByKey(SDBFile *pDBF, const void *pKey, int32_t keyLen, int32_t *valLen);

void  tsdbDestroySmaEnv(SSmaEnv *pSmaEnv);
void *tsdbFreeSmaEnv(SSmaEnv *pSmaEnv);
#if 0
int32_t tsdbGetTSmaStatus(STsdb *pTsdb, STSma *param, void *result);
int32_t tsdbRemoveTSmaData(STsdb *pTsdb, STSma *param, STimeWindow *pWin);
#endif

// internal func
static FORCE_INLINE int32_t tsdbEncodeTSmaKey(int64_t groupId, TSKEY tsKey, void **pData) {
  int32_t len = 0;
  len += taosEncodeFixedI64(pData, tsKey);
  len += taosEncodeFixedI64(pData, groupId);
  return len;
}


#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_TSDB_SMA_H_*/