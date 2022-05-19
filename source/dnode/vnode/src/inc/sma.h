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

#ifndef _TD_VNODE_SMA_H_
#define _TD_VNODE_SMA_H_

#include "vnodeInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// smaDebug ================
// clang-format off
#define smaFatal(...) do { if (smaDebugFlag & DEBUG_FATAL) { taosPrintLog("SMA FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define smaError(...) do { if (smaDebugFlag & DEBUG_ERROR) { taosPrintLog("SMA ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define smaWarn(...)  do { if (smaDebugFlag & DEBUG_WARN)  { taosPrintLog("SMA WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define smaInfo(...)  do { if (smaDebugFlag & DEBUG_INFO)  { taosPrintLog("SMA ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define smaDebug(...) do { if (smaDebugFlag & DEBUG_DEBUG) { taosPrintLog("SMA ", DEBUG_DEBUG, tsdbDebugFlag, __VA_ARGS__); }} while(0)
#define smaTrace(...) do { if (smaDebugFlag & DEBUG_TRACE) { taosPrintLog("SMA ", DEBUG_TRACE, tsdbDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

typedef struct SSmaEnv      SSmaEnv;
typedef struct SSmaStat     SSmaStat;
typedef struct SSmaStatItem SSmaStatItem;
typedef struct SSmaKey      SSmaKey;
typedef struct SRSmaInfo    SRSmaInfo;

#define SMA_IVLD_FID INT_MIN

struct SSmaEnv {
  TdThreadRwlock lock;
  int8_t         type;
  TXN            txn;
  void          *pPool;  // SPoolMem
  SDiskID        did;
  TDB           *dbEnv;  // TODO: If it's better to put it in smaIndex level?
  char          *path;   // relative path
  SSmaStat      *pStat;
};

#define SMA_ENV_LOCK(env)       ((env)->lock)
#define SMA_ENV_TYPE(env)       ((env)->type)
#define SMA_ENV_DID(env)        ((env)->did)
#define SMA_ENV_ENV(env)        ((env)->dbEnv)
#define SMA_ENV_PATH(env)       ((env)->path)
#define SMA_ENV_STAT(env)       ((env)->pStat)
#define SMA_ENV_STAT_ITEMS(env) ((env)->pStat->smaStatItems)

struct SSmaStatItem {
  /**
   * @brief The field 'state' is here to demonstrate if one smaIndex is ready to provide service.
   *    - TSDB_SMA_STAT_OK: 1) The sma calculation of history data is finished; 2) Or recevied information from
   * Streaming Module or TSDB local persistence.
   *    - TSDB_SMA_STAT_EXPIRED: 1) If sma calculation of history TS data is not finished; 2) Or if the TSDB is open,
   * without information about its previous state.
   *    - TSDB_SMA_STAT_DROPPED: 1)sma dropped
   * N.B. only applicable to tsma
   */
  int8_t    state;           // ETsdbSmaStat
  SHashObj *expiredWindows;  // key: skey of time window, value: version
  STSma    *pTSma;           // cache schema
};

struct SSmaStat {
  union {
    SHashObj *smaStatItems;  // key: indexUid, value: SSmaStatItem for tsma
    SHashObj *rsmaInfoHash;  // key: stbUid, value: SRSmaInfo;
  };
  T_REF_DECLARE()
};
#define SMA_STAT_ITEMS(s)     ((s)->smaStatItems)
#define SMA_STAT_INFO_HASH(s) ((s)->rsmaInfoHash)

struct SSmaKey {
  TSKEY   skey;
  int64_t groupId;
};

typedef struct SDBFile SDBFile;

struct SDBFile {
  int32_t fid;
  TTB    *pDB;
  char   *path;
};

int32_t tdSmaBeginCommit(SSmaEnv *pEnv);
int32_t tdSmaEndCommit(SSmaEnv *pEnv);

int32_t smaOpenDBEnv(TDB **ppEnv, const char *path);
int32_t smaCloseDBEnv(TDB *pEnv);
int32_t smaOpenDBF(TDB *pEnv, SDBFile *pDBF);
int32_t smaCloseDBF(SDBFile *pDBF);
int32_t smaSaveSmaToDB(SDBFile *pDBF, void *pKey, int32_t keyLen, void *pVal, int32_t valLen, TXN *txn);
void   *smaGetSmaDataByKey(SDBFile *pDBF, const void *pKey, int32_t keyLen, int32_t *valLen);

void  tdDestroySmaEnv(SSmaEnv *pSmaEnv);
void *tdFreeSmaEnv(SSmaEnv *pSmaEnv);
#if 0
int32_t tbGetTSmaStatus(SSma *pSma, STSma *param, void *result);
int32_t tbRemoveTSmaData(SSma *pSma, STSma *param, STimeWindow *pWin);
#endif

static FORCE_INLINE int32_t tdEncodeTSmaKey(int64_t groupId, TSKEY tsKey, void **pData) {
  int32_t len = 0;
  len += taosEncodeFixedI64(pData, tsKey);
  len += taosEncodeFixedI64(pData, groupId);
  return len;
}

int32_t tdInitSma(SSma *pSma);
int32_t tdDropTSma(SSma *pSma, char *pMsg);
int32_t tdDropTSmaData(SSma *pSma, int64_t indexUid);
int32_t tdInsertRSmaData(SSma *pSma, char *msg);

int32_t tdRefSmaStat(SSma *pSma, SSmaStat *pStat);
int32_t tdUnRefSmaStat(SSma *pSma, SSmaStat *pStat);
int32_t tdCheckAndInitSmaEnv(SSma *pSma, int8_t smaType);

int32_t tdLockSma(SSma *pSma);
int32_t tdUnLockSma(SSma *pSma);

int32_t tdProcessTSmaInsertImpl(SSma *pSma, int64_t indexUid, const char *msg);

static FORCE_INLINE int16_t tdTSmaAdd(SSma *pSma, int16_t n) { return atomic_add_fetch_16(&SMA_TSMA_NUM(pSma), n); }
static FORCE_INLINE int16_t tdTSmaSub(SSma *pSma, int16_t n) { return atomic_sub_fetch_16(&SMA_TSMA_NUM(pSma), n); }

static FORCE_INLINE int32_t tdRLockSmaEnv(SSmaEnv *pEnv) {
  int code = taosThreadRwlockRdlock(&(pEnv->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int32_t tdWLockSmaEnv(SSmaEnv *pEnv) {
  int code = taosThreadRwlockWrlock(&(pEnv->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int32_t tdUnLockSmaEnv(SSmaEnv *pEnv) {
  int code = taosThreadRwlockUnlock(&(pEnv->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int8_t tdSmaStat(SSmaStatItem *pStatItem) {
  if (pStatItem) {
    return atomic_load_8(&pStatItem->state);
  }
  return TSDB_SMA_STAT_UNKNOWN;
}

static FORCE_INLINE bool tdSmaStatIsOK(SSmaStatItem *pStatItem, int8_t *state) {
  if (!pStatItem) {
    return false;
  }

  if (state) {
    *state = atomic_load_8(&pStatItem->state);
    return *state == TSDB_SMA_STAT_OK;
  }
  return atomic_load_8(&pStatItem->state) == TSDB_SMA_STAT_OK;
}

static FORCE_INLINE bool tdSmaStatIsExpired(SSmaStatItem *pStatItem) {
  return pStatItem ? (atomic_load_8(&pStatItem->state) & TSDB_SMA_STAT_EXPIRED) : true;
}

static FORCE_INLINE bool tdSmaStatIsDropped(SSmaStatItem *pStatItem) {
  return pStatItem ? (atomic_load_8(&pStatItem->state) & TSDB_SMA_STAT_DROPPED) : true;
}

static FORCE_INLINE void tdSmaStatSetOK(SSmaStatItem *pStatItem) {
  if (pStatItem) {
    atomic_store_8(&pStatItem->state, TSDB_SMA_STAT_OK);
  }
}

static FORCE_INLINE void tdSmaStatSetExpired(SSmaStatItem *pStatItem) {
  if (pStatItem) {
    atomic_or_fetch_8(&pStatItem->state, TSDB_SMA_STAT_EXPIRED);
  }
}

static FORCE_INLINE void tdSmaStatSetDropped(SSmaStatItem *pStatItem) {
  if (pStatItem) {
    atomic_or_fetch_8(&pStatItem->state, TSDB_SMA_STAT_DROPPED);
  }
}

static int32_t  tdInitSmaStat(SSmaStat **pSmaStat, int8_t smaType);
void           *tdFreeSmaStatItem(SSmaStatItem *pSmaStatItem);
static int32_t  tdDestroySmaState(SSmaStat *pSmaStat, int8_t smaType);
static SSmaEnv *tdNewSmaEnv(const SSma *pSma, int8_t smaType, const char *path, SDiskID did);
static int32_t  tdInitSmaEnv(SSma *pSma, int8_t smaType, const char *path, SDiskID did, SSmaEnv **pEnv);

void *tdFreeRSmaInfo(SRSmaInfo *pInfo);

int32_t tdProcessTSmaCreateImpl(SSma *pSma, int64_t version, const char *pMsg);
int32_t tdUpdateExpiredWindowImpl(SSma *pSma, SSubmitReq *pMsg, int64_t version);
// TODO: This is the basic params, and should wrap the params to a queryHandle.
int32_t tdGetTSmaDataImpl(SSma *pSma, char *pData, int64_t indexUid, TSKEY querySKey, int32_t nMaxResult);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_SMA_H_*/