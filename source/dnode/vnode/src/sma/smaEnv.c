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

#include "sma.h"

typedef struct SSmaStat SSmaStat;

#define RSMA_TASK_INFO_HASH_SLOT 8

// declaration of static functions

static int32_t  tdInitSmaStat(SSmaStat **pSmaStat, int8_t smaType);
static SSmaEnv *tdNewSmaEnv(const SSma *pSma, int8_t smaType, const char *path);
static int32_t  tdInitSmaEnv(SSma *pSma, int8_t smaType, const char *path, SSmaEnv **pEnv);

// implementation

static SSmaEnv *tdNewSmaEnv(const SSma *pSma, int8_t smaType, const char *path) {
  SSmaEnv *pEnv = NULL;

  pEnv = (SSmaEnv *)taosMemoryCalloc(1, sizeof(SSmaEnv));
  if (!pEnv) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  SMA_ENV_TYPE(pEnv) = smaType;

  int code = taosThreadRwlockInit(&(pEnv->lock), NULL);
  if (code) {
    terrno = TAOS_SYSTEM_ERROR(code);
    taosMemoryFree(pEnv);
    return NULL;
  }

  if (tdInitSmaStat(&SMA_ENV_STAT(pEnv), smaType) != TSDB_CODE_SUCCESS) {
    tdFreeSmaEnv(pEnv);
    return NULL;
  }

  return pEnv;
}

static int32_t tdInitSmaEnv(SSma *pSma, int8_t smaType, const char *path, SSmaEnv **pEnv) {
  if (!pEnv) {
    terrno = TSDB_CODE_INVALID_PTR;
    return TSDB_CODE_FAILED;
  }

  if (!(*pEnv)) {
    if (!(*pEnv = tdNewSmaEnv(pSma, smaType, path))) {
      return TSDB_CODE_FAILED;
    }
  }

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Release resources allocated for its member fields, not including itself.
 *
 * @param pSmaEnv
 * @return int32_t
 */
void tdDestroySmaEnv(SSmaEnv *pSmaEnv) {
  if (pSmaEnv) {
    pSmaEnv->pStat = tdFreeSmaState(pSmaEnv->pStat, SMA_ENV_TYPE(pSmaEnv));
    taosThreadRwlockDestroy(&(pSmaEnv->lock));
  }
}

void *tdFreeSmaEnv(SSmaEnv *pSmaEnv) {
  if (pSmaEnv) {
    tdDestroySmaEnv(pSmaEnv);
    taosMemoryFreeClear(pSmaEnv);
  }
  return NULL;
}

int32_t tdRefSmaStat(SSma *pSma, SSmaStat *pStat) {
  if (!pStat) return 0;

  int ref = T_REF_INC(pStat);
  smaDebug("vgId:%d, ref sma stat:%p, val:%d", SMA_VID(pSma), pStat, ref);
  return 0;
}

int32_t tdUnRefSmaStat(SSma *pSma, SSmaStat *pStat) {
  if (!pStat) return 0;

  int ref = T_REF_DEC(pStat);
  smaDebug("vgId:%d, unref sma stat:%p, val:%d", SMA_VID(pSma), pStat, ref);
  return 0;
}

static int32_t tdInitSmaStat(SSmaStat **pSmaStat, int8_t smaType) {
  ASSERT(pSmaStat != NULL);

  if (*pSmaStat) {  // no lock
    return TSDB_CODE_SUCCESS;
  }

  /**
   *  1. Lazy mode utilized when init SSmaStat to update expire window(or hungry mode when tdNew).
   *  2. Currently, there is mutex lock when init SSmaEnv, thus no need add lock on SSmaStat, and please add lock if
   * tdInitSmaStat invoked in other multithread environment later.
   */
  if (!(*pSmaStat)) {
    *pSmaStat = (SSmaStat *)taosMemoryCalloc(1, sizeof(SSmaStat));
    if (!(*pSmaStat)) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_FAILED;
    }

    if (smaType == TSDB_SMA_TYPE_ROLLUP) {
      SMA_STAT_INFO_HASH(*pSmaStat) = taosHashInit(
          RSMA_TASK_INFO_HASH_SLOT, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);

      if (!SMA_STAT_INFO_HASH(*pSmaStat)) {
        taosMemoryFreeClear(*pSmaStat);
        return TSDB_CODE_FAILED;
      }
    } else if (smaType == TSDB_SMA_TYPE_TIME_RANGE) {
      // TODO
    } else {
      ASSERT(0);
    }
  }
  return TSDB_CODE_SUCCESS;
}

void *tdFreeSmaStatItem(SSmaStatItem *pSmaStatItem) {
  if (pSmaStatItem) {
    tDestroyTSma(pSmaStatItem->pTSma);
    taosMemoryFreeClear(pSmaStatItem->pTSma);
    taosMemoryFreeClear(pSmaStatItem);
  }
  return NULL;
}

void* tdFreeSmaState(SSmaStat *pSmaStat, int8_t smaType) {
  tdDestroySmaState(pSmaStat, smaType);
  taosMemoryFreeClear(pSmaStat);
  return NULL;
}

/**
 * @brief Release resources allocated for its member fields, not including itself.
 *
 * @param pSmaStat
 * @return int32_t
 */
int32_t tdDestroySmaState(SSmaStat *pSmaStat, int8_t smaType) {
  if (pSmaStat) {
    if (smaType == TSDB_SMA_TYPE_TIME_RANGE) {
      tdFreeSmaStatItem(&pSmaStat->tsmaStatItem);
    } else if (smaType == TSDB_SMA_TYPE_ROLLUP) {
      // TODO: use taosHashSetFreeFp when taosHashSetFreeFp is ready.
      void *infoHash = taosHashIterate(SMA_STAT_INFO_HASH(pSmaStat), NULL);
      while (infoHash) {
        SRSmaInfo *pInfoHash = *(SRSmaInfo **)infoHash;
        tdFreeRSmaInfo(pInfoHash);
        infoHash = taosHashIterate(SMA_STAT_INFO_HASH(pSmaStat), infoHash);
      }
      taosHashCleanup(SMA_STAT_INFO_HASH(pSmaStat));
    } else {
      ASSERT(0);
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tdLockSma(SSma *pSma) {
  int code = taosThreadMutexLock(&pSma->mutex);
  if (code != 0) {
    smaError("vgId:%d, failed to lock td since %s", SMA_VID(pSma), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  pSma->locked = true;
  return 0;
}

int32_t tdUnLockSma(SSma *pSma) {
  ASSERT(SMA_LOCKED(pSma));
  pSma->locked = false;
  int code = taosThreadMutexUnlock(&pSma->mutex);
  if (code != 0) {
    smaError("vgId:%d, failed to unlock td since %s", SMA_VID(pSma), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

int32_t tdCheckAndInitSmaEnv(SSma *pSma, int8_t smaType) {
  SSmaEnv *pEnv = NULL;

  switch (smaType) {
    case TSDB_SMA_TYPE_TIME_RANGE:
      if ((pEnv = (SSmaEnv *)atomic_load_ptr(&SMA_TSMA_ENV(pSma)))) {
        return TSDB_CODE_SUCCESS;
      }
      break;
    case TSDB_SMA_TYPE_ROLLUP:
      if ((pEnv = (SSmaEnv *)atomic_load_ptr(&SMA_RSMA_ENV(pSma)))) {
        return TSDB_CODE_SUCCESS;
      }
      break;
    default:
      TASSERT(0);
      return TSDB_CODE_FAILED;
  }

  // init sma env
  tdLockSma(pSma);
  pEnv = (smaType == TSDB_SMA_TYPE_TIME_RANGE) ? atomic_load_ptr(&SMA_TSMA_ENV(pSma))
                                               : atomic_load_ptr(&SMA_RSMA_ENV(pSma));
  if (!pEnv) {
    char rname[TSDB_FILENAME_LEN] = {0};

    if (tdInitSmaEnv(pSma, smaType, rname, &pEnv) < 0) {
      tdUnLockSma(pSma);
      return TSDB_CODE_FAILED;
    }

    (smaType == TSDB_SMA_TYPE_TIME_RANGE) ? atomic_store_ptr(&SMA_TSMA_ENV(pSma), pEnv)
                                          : atomic_store_ptr(&SMA_RSMA_ENV(pSma), pEnv);
  }
  tdUnLockSma(pSma);

  return TSDB_CODE_SUCCESS;
};

int32_t smaTimerInit(void **timer, int8_t *initFlag, const char *label) {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(initFlag, 0, 2);
    if (old != 2) break;
  }

  if (old == 0) {
    *timer = taosTmrInit(10000, 100, 10000, label);
    if (!(*timer)) {
      atomic_store_8(initFlag, 0);
      return -1;
    }
    atomic_store_8(initFlag, 1);
  }
  return 0;
}

void smaTimerCleanUp(void *timer, int8_t *initFlag) {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(initFlag, 1, 2);
    if (old != 2) break;
  }

  if (old == 1) {
    taosTmrCleanUp(timer);
    atomic_store_8(initFlag, 0);
  }
}
