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

static const char *TSDB_SMA_DNAME[] = {
    "",      // TSDB_SMA_TYPE_BLOCK
    "tsma",  // TSDB_SMA_TYPE_TIME_RANGE
    "rsma",  // TSDB_SMA_TYPE_ROLLUP
};

#define SMA_TEST_INDEX_NAME "smaTestIndexName"  // TODO: just for test
#define SMA_TEST_INDEX_UID  2000000001          // TODO: just for test
#define SMA_STATE_HASH_SLOT 4

#define RSMA_TASK_INFO_HASH_SLOT 8

typedef struct SPoolMem {
  int64_t          size;
  struct SPoolMem *prev;
  struct SPoolMem *next;
} SPoolMem;

// declaration of static functions

// insert data

static void tdGetSmaDir(int32_t vgId, ETsdbSmaType smaType, char dirName[]);

// Pool Memory
static SPoolMem *openPool();
static void      clearPool(SPoolMem *pPool);
static void      closePool(SPoolMem *pPool);
static void     *poolMalloc(void *arg, size_t size);
static void      poolFree(void *arg, void *ptr);

// implementation

static SPoolMem *openPool() {
  SPoolMem *pPool = (SPoolMem *)taosMemoryMalloc(sizeof(*pPool));

  pPool->prev = pPool->next = pPool;
  pPool->size = 0;

  return pPool;
}

static void clearPool(SPoolMem *pPool) {
  if (!pPool) return;

  SPoolMem *pMem;

  do {
    pMem = pPool->next;

    if (pMem == pPool) break;

    pMem->next->prev = pMem->prev;
    pMem->prev->next = pMem->next;
    pPool->size -= pMem->size;

    taosMemoryFree(pMem);
  } while (1);

  assert(pPool->size == 0);
}

static void closePool(SPoolMem *pPool) {
  if (pPool) {
    clearPool(pPool);
    taosMemoryFree(pPool);
  }
}

static void *poolMalloc(void *arg, size_t size) {
  void     *ptr = NULL;
  SPoolMem *pPool = (SPoolMem *)arg;
  SPoolMem *pMem;

  pMem = (SPoolMem *)taosMemoryMalloc(sizeof(*pMem) + size);
  if (!pMem) {
    assert(0);
  }

  pMem->size = sizeof(*pMem) + size;
  pMem->next = pPool->next;
  pMem->prev = pPool;

  pPool->next->prev = pMem;
  pPool->next = pMem;
  pPool->size += pMem->size;

  ptr = (void *)(&pMem[1]);
  return ptr;
}

static void poolFree(void *arg, void *ptr) {
  SPoolMem *pPool = (SPoolMem *)arg;
  SPoolMem *pMem;

  pMem = &(((SPoolMem *)ptr)[-1]);

  pMem->next->prev = pMem->prev;
  pMem->prev->next = pMem->next;
  pPool->size -= pMem->size;

  taosMemoryFree(pMem);
}

int32_t tdInitSma(SSma *pSma) {
  // tSma
  int32_t numOfTSma = taosArrayGetSize(metaGetSmaTbUids(SMA_META(pSma), false));
  if (numOfTSma > 0) {
    atomic_store_16(&SMA_TSMA_NUM(pSma), (int16_t)numOfTSma);
  }
  // TODO: rSma
  return TSDB_CODE_SUCCESS;
}

static void tdGetSmaDir(int32_t vgId, ETsdbSmaType smaType, char dirName[]) {
  snprintf(dirName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s", TD_DIRSEP, vgId, TD_DIRSEP, TSDB_SMA_DNAME[smaType]);
}

static SSmaEnv *tdNewSmaEnv(const SSma *pSma, int8_t smaType, const char *path, SDiskID did) {
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

  ASSERT(path && (strlen(path) > 0));
  SMA_ENV_PATH(pEnv) = strdup(path);
  if (!SMA_ENV_PATH(pEnv)) {
    tdFreeSmaEnv(pEnv);
    return NULL;
  }

  SMA_ENV_DID(pEnv) = did;

  if (tdInitSmaStat(&SMA_ENV_STAT(pEnv), smaType) != TSDB_CODE_SUCCESS) {
    tdFreeSmaEnv(pEnv);
    return NULL;
  }

  char aname[TSDB_FILENAME_LEN] = {0};
  tfsAbsoluteName(SMA_TFS(pSma), did, path, aname);
  if (smaOpenDBEnv(&pEnv->dbEnv, aname) != TSDB_CODE_SUCCESS) {
    tdFreeSmaEnv(pEnv);
    return NULL;
  }

  if (!(pEnv->pPool = openPool())) {
    tdFreeSmaEnv(pEnv);
    return NULL;
  }

  return pEnv;
}

static int32_t tdInitSmaEnv(SSma *pSma, int8_t smaType, const char *path, SDiskID did, SSmaEnv **pEnv) {
  if (!pEnv) {
    terrno = TSDB_CODE_INVALID_PTR;
    return TSDB_CODE_FAILED;
  }

  if (!(*pEnv)) {
    if (!(*pEnv = tdNewSmaEnv(pSma, smaType, path, did))) {
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
    tdDestroySmaState(pSmaEnv->pStat, SMA_ENV_TYPE(pSmaEnv));
    taosMemoryFreeClear(pSmaEnv->pStat);
    taosMemoryFreeClear(pSmaEnv->path);
    taosThreadRwlockDestroy(&(pSmaEnv->lock));
    smaCloseDBEnv(pSmaEnv->dbEnv);
    closePool(pSmaEnv->pPool);
  }
}

void *tdFreeSmaEnv(SSmaEnv *pSmaEnv) {
  tdDestroySmaEnv(pSmaEnv);
  taosMemoryFreeClear(pSmaEnv);
  return NULL;
}

int32_t tdRefSmaStat(SSma *pSma, SSmaStat *pStat) {
  if (!pStat) return 0;

  int ref = T_REF_INC(pStat);
  smaDebug("vgId:%d ref sma stat:%p, val:%d", SMA_VID(pSma), pStat, ref);
  return 0;
}

int32_t tdUnRefSmaStat(SSma *pSma, SSmaStat *pStat) {
  if (!pStat) return 0;

  int ref = T_REF_DEC(pStat);
  smaDebug("vgId:%d unref sma stat:%p, val:%d", SMA_VID(pSma), pStat, ref);
  return 0;
}

static int32_t tdInitSmaStat(SSmaStat **pSmaStat, int8_t smaType) {
  ASSERT(pSmaStat != NULL);

  if (*pSmaStat) {  // no lock
    return TSDB_CODE_SUCCESS;
  }

  /**
   *  1. Lazy mode utilized when init SSmaStat to update expired window(or hungry mode when tdNew).
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
      SMA_STAT_ITEMS(*pSmaStat) =
          taosHashInit(SMA_STATE_HASH_SLOT, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);

      if (!SMA_STAT_ITEMS(*pSmaStat)) {
        taosMemoryFreeClear(*pSmaStat);
        return TSDB_CODE_FAILED;
      }
    } else {
      ASSERT(0);
    }
  }
  return TSDB_CODE_SUCCESS;
}

void *tdFreeSmaStatItem(SSmaStatItem *pSmaStatItem) {
  if (pSmaStatItem) {
    tdDestroyTSma(pSmaStatItem->pTSma);
    taosMemoryFreeClear(pSmaStatItem->pTSma);
    taosHashCleanup(pSmaStatItem->expiredWindows);
    taosMemoryFreeClear(pSmaStatItem);
  }
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
    // TODO: use taosHashSetFreeFp when taosHashSetFreeFp is ready.
    if (smaType == TSDB_SMA_TYPE_TIME_RANGE) {
      void *item = taosHashIterate(SMA_STAT_ITEMS(pSmaStat), NULL);
      while (item) {
        SSmaStatItem *pItem = *(SSmaStatItem **)item;
        tdFreeSmaStatItem(pItem);
        item = taosHashIterate(SMA_STAT_ITEMS(pSmaStat), item);
      }
      taosHashCleanup(SMA_STAT_ITEMS(pSmaStat));
    } else if (smaType == TSDB_SMA_TYPE_ROLLUP) {
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
    smaError("vgId:%d failed to lock td since %s", SMA_VID(pSma), strerror(errno));
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
    smaError("vgId:%d failed to unlock td since %s", SMA_VID(pSma), strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

int32_t tdCheckAndInitSmaEnv(SSma *pSma, int8_t smaType) {
  SSmaEnv *pEnv = NULL;

  // return if already init
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

    SDiskID did = {0};
    if (tfsAllocDisk(SMA_TFS(pSma), TFS_PRIMARY_LEVEL, &did) < 0) {
      tdUnLockSma(pSma);
      return TSDB_CODE_FAILED;
    }

    if (did.level < 0 || did.id < 0) {
      tdUnLockSma(pSma);
      smaError("vgId:%d init sma env failed since invalid did(%d,%d)", SMA_VID(pSma), did.level, did.id);
      return TSDB_CODE_FAILED;
    }

    tdGetSmaDir(SMA_VID(pSma), smaType, rname);

    if (tfsMkdirRecurAt(SMA_TFS(pSma), rname, did) < 0) {
      tdUnLockSma(pSma);
      return TSDB_CODE_FAILED;
    }

    if (tdInitSmaEnv(pSma, smaType, rname, did, &pEnv) < 0) {
      tdUnLockSma(pSma);
      return TSDB_CODE_FAILED;
    }

    (smaType == TSDB_SMA_TYPE_TIME_RANGE) ? atomic_store_ptr(&SMA_TSMA_ENV(pSma), pEnv)
                                          : atomic_store_ptr(&SMA_RSMA_ENV(pSma), pEnv);
  }
  tdUnLockSma(pSma);

  return TSDB_CODE_SUCCESS;
};

int32_t tdSmaBeginCommit(SSmaEnv *pEnv) {
  TXN *pTxn = &pEnv->txn;
  // start a new txn
  tdbTxnOpen(pTxn, 0, poolMalloc, poolFree, pEnv->pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  if (tdbBegin(pEnv->dbEnv, pTxn) != 0) {
    smaWarn("tdSma tdb begin commit fail");
    return -1;
  }
  return 0;
}

int32_t tdSmaEndCommit(SSmaEnv *pEnv) {
  TXN *pTxn = &pEnv->txn;

  // Commit current txn
  if (tdbCommit(pEnv->dbEnv, pTxn) != 0) {
    smaWarn("tdSma tdb end commit fail");
    return -1;
  }
  tdbTxnClose(pTxn);
  clearPool(pEnv->pPool);
  return 0;
}

#if 0
/**
 * @brief Get the start TS key of the last data block of one interval/sliding.
 *
 * @param pSma
 * @param param
 * @param result
 * @return int32_t
 *         1) Return 0 and fill the result if the check procedure is normal;
 *         2) Return -1 if error occurs during the check procedure.
 */
int32_t tdGetTSmaStatus(SSma *pSma, void *smaIndex, void *result) {
  const char *procedure = "";
  if (strncmp(procedure, "get the start TS key of the last data block", 100) != 0) {
    return -1;
  }
  // fill the result
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Remove the tSma data files related to param between pWin.
 *
 * @param pSma
 * @param param
 * @param pWin
 * @return int32_t
 */
int32_t tdRemoveTSmaData(SSma *pSma, void *smaIndex, STimeWindow *pWin) {
  // for ("tSmaFiles of param-interval-sliding between pWin") {
  //   // remove the tSmaFile
  // }
  return TSDB_CODE_SUCCESS;
}
#endif
