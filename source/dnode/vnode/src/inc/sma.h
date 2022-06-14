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
  SSmaStat      *pStat;
};

#define SMA_ENV_LOCK(env)       ((env)->lock)
#define SMA_ENV_TYPE(env)       ((env)->type)
#define SMA_ENV_STAT(env)       ((env)->pStat)
#define SMA_ENV_STAT_ITEMS(env) ((env)->pStat->smaStatItems)

struct SSmaStatItem {
  int8_t state;  // ETsdbSmaStat
  STSma *pTSma;  // cache schema
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

void  tdDestroySmaEnv(SSmaEnv *pSmaEnv);
void *tdFreeSmaEnv(SSmaEnv *pSmaEnv);
#if 0
int32_t tbGetTSmaStatus(SSma *pSma, STSma *param, void *result);
int32_t tbRemoveTSmaData(SSma *pSma, STSma *param, STimeWindow *pWin);
#endif

int32_t tdInitSma(SSma *pSma);
int32_t tdDropTSma(SSma *pSma, char *pMsg);
int32_t tdDropTSmaData(SSma *pSma, int64_t indexUid);
int32_t tdInsertRSmaData(SSma *pSma, char *msg);

int32_t tdRefSmaStat(SSma *pSma, SSmaStat *pStat);
int32_t tdUnRefSmaStat(SSma *pSma, SSmaStat *pStat);
int32_t tdCheckAndInitSmaEnv(SSma *pSma, int8_t smaType, bool onlyCheck);

int32_t tdLockSma(SSma *pSma);
int32_t tdUnLockSma(SSma *pSma);

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
int32_t tdProcessTSmaInsertImpl(SSma *pSma, int64_t indexUid, const char *msg);
int32_t tdProcessTSmaGetDaysImpl(SVnodeCfg *pCfg, void *pCont, uint32_t contLen, int32_t *days);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_SMA_H_*/
