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

#ifndef _TD_TSDB_SMA_H_
#define _TD_TSDB_SMA_H_

typedef struct SSmaStat SSmaStat;
typedef struct SSmaEnv  SSmaEnv;

struct SSmaEnv {
  pthread_rwlock_t lock;
  TDBEnv           dbEnv;
  char *           path;
  SSmaStat *       pStat;
};

#define SMA_ENV_LOCK(env)       ((env)->lock)
#define SMA_ENV_ENV(env)        ((env)->dbEnv)
#define SMA_ENV_PATH(env)       ((env)->path)
#define SMA_ENV_STAT(env)       ((env)->pStat)
#define SMA_ENV_STAT_ITEMS(env) ((env)->pStat->smaStatItems)

void  tsdbDestroySmaEnv(SSmaEnv *pSmaEnv);
void *tsdbFreeSmaEnv(SSmaEnv *pSmaEnv);
#if 0
int32_t tsdbGetTSmaStatus(STsdb *pTsdb, STSma *param, void *result);
int32_t tsdbRemoveTSmaData(STsdb *pTsdb, STSma *param, STimeWindow *pWin);
#endif

// internal func
static FORCE_INLINE int32_t tsdbEncodeTSmaKey(tb_uid_t tableUid, col_id_t colId, TSKEY tsKey, void **pData) {
  int32_t len = 0;
  len += taosEncodeFixedI64(pData, tableUid);
  len += taosEncodeFixedU16(pData, colId);
  len += taosEncodeFixedI64(pData, tsKey);
  return len;
}

static FORCE_INLINE int tsdbRLockSma(SSmaEnv *pEnv) {
  int code = pthread_rwlock_rdlock(&(pEnv->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int tsdbWLockSma(SSmaEnv *pEnv) {
  int code = pthread_rwlock_wrlock(&(pEnv->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int tsdbUnLockSma(SSmaEnv *pEnv) {
  int code = pthread_rwlock_unlock(&(pEnv->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

#endif /* _TD_TSDB_SMA_H_ */