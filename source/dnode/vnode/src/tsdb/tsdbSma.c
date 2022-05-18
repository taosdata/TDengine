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

#include "tsdbSma.h"
#include "tsdb.h"

static const char *TSDB_SMA_DNAME[] = {
    "",      // TSDB_SMA_TYPE_BLOCK
    "tsma",  // TSDB_SMA_TYPE_TIME_RANGE
    "rsma",  // TSDB_SMA_TYPE_ROLLUP
};

#undef _TEST_SMA_PRINT_DEBUG_LOG_
#define SMA_STORAGE_TSDB_DAYS   30
#define SMA_STORAGE_TSDB_TIMES  10
#define SMA_STORAGE_SPLIT_HOURS 24
#define SMA_KEY_LEN             16  // TSKEY+groupId 8+8
#define SMA_DROP_EXPIRED_TIME   10  // default is 10 seconds

#define SMA_STATE_HASH_SLOT      4
#define SMA_STATE_ITEM_HASH_SLOT 32

#define SMA_TEST_INDEX_NAME "smaTestIndexName"  // TODO: just for test
#define SMA_TEST_INDEX_UID  2000000001          // TODO: just for test

typedef struct SRSmaInfo SRSmaInfo;
typedef enum {
  SMA_STORAGE_LEVEL_TSDB = 0,     // use days of self-defined  e.g. vnode${N}/tsdb/tsma/sma_index_uid/v2f200.tsma
  SMA_STORAGE_LEVEL_DFILESET = 1  // use days of TS data       e.g. vnode${N}/tsdb/tsma/sma_index_uid/v2f1906.tsma
} ESmaStorageLevel;

typedef struct SPoolMem {
  int64_t          size;
  struct SPoolMem *prev;
  struct SPoolMem *next;
} SPoolMem;

struct SSmaEnv {
  TdThreadRwlock lock;
  int8_t         type;
  TXN            txn;
  SPoolMem      *pPool;
  SDiskID        did;
  TENV          *dbEnv;  // TODO: If it's better to put it in smaIndex level?
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

typedef struct {
  STsdb        *pTsdb;
  SDBFile       dFile;
  const SArray *pDataBlocks;  // sma data
  int32_t       interval;     // interval with the precision of DB
} STSmaWriteH;

typedef struct {
  int32_t iter;
  int32_t fid;
} SmaFsIter;

typedef struct {
  STsdb    *pTsdb;
  SDBFile   dFile;
  int32_t   interval;   // interval with the precision of DB
  int32_t   blockSize;  // size of SMA block item
  int8_t    storageLevel;
  int8_t    days;
  SmaFsIter smaFsIter;
} STSmaReadH;

typedef struct {
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
  SHashObj *expiredWindows;  // key: skey of time window, value: N/A
  STSma    *pSma;            // cache schema
} SSmaStatItem;

#define RSMA_TASK_INFO_HASH_SLOT 8
struct SRSmaInfo {
  void *taskInfo[TSDB_RETENTION_L2];  // qTaskInfo_t
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

static FORCE_INLINE void tsdbFreeTaskHandle(qTaskInfo_t *taskHandle) {
  // Note: free/kill may in RC
  qTaskInfo_t otaskHandle = atomic_load_ptr(taskHandle);
  if (otaskHandle && atomic_val_compare_exchange_ptr(taskHandle, otaskHandle, NULL)) {
    qDestroyTask(otaskHandle);
  }
}

static FORCE_INLINE void *tsdbFreeRSmaInfo(SRSmaInfo *pInfo) {
  for (int32_t i = 0; i < TSDB_RETENTION_MAX; ++i) {
    if (pInfo->taskInfo[i]) {
      tsdbFreeTaskHandle(pInfo->taskInfo[i]);
    }
  }
  return NULL;
}

// declaration of static functions

// expired window
static int32_t  tsdbUpdateExpiredWindowImpl(STsdb *pTsdb, SSubmitReq *pMsg, int64_t version);
static int32_t  tsdbSetExpiredWindow(STsdb *pTsdb, SHashObj *pItemsHash, int64_t indexUid, int64_t winSKey,
                                     int64_t version);
static int32_t  tsdbInitSmaStat(SSmaStat **pSmaStat, int8_t smaType);
static void    *tsdbFreeSmaStatItem(SSmaStatItem *pSmaStatItem);
static int32_t  tsdbDestroySmaState(SSmaStat *pSmaStat, int8_t smaType);
static SSmaEnv *tsdbNewSmaEnv(const STsdb *pTsdb, int8_t smaType, const char *path, SDiskID did);
static int32_t  tsdbInitSmaEnv(STsdb *pTsdb, int8_t smaType, const char *path, SDiskID did, SSmaEnv **pEnv);
static int32_t  tsdbResetExpiredWindow(STsdb *pTsdb, SSmaStat *pStat, int64_t indexUid, TSKEY skey);
static int32_t  tsdbRefSmaStat(STsdb *pTsdb, SSmaStat *pStat);
static int32_t  tsdbUnRefSmaStat(STsdb *pTsdb, SSmaStat *pStat);

// read data
// TODO: This is the basic params, and should wrap the params to a queryHandle.
static int32_t tsdbGetTSmaDataImpl(STsdb *pTsdb, char *pData, int64_t indexUid, TSKEY querySKey, int32_t nMaxResult);

// insert data
static int32_t tsdbInitTSmaWriteH(STSmaWriteH *pSmaH, STsdb *pTsdb, const SArray *pDataBlocks, int64_t interval,
                                  int8_t intervalUnit);
static void    tsdbDestroyTSmaWriteH(STSmaWriteH *pSmaH);
static int32_t tsdbInitTSmaReadH(STSmaReadH *pSmaH, STsdb *pTsdb, int64_t interval, int8_t intervalUnit);
static int32_t tsdbGetSmaStorageLevel(int64_t interval, int8_t intervalUnit);
static int32_t tsdbSetRSmaDataFile(STSmaWriteH *pSmaH, int32_t fid);
static int32_t tsdbInsertTSmaBlocks(STSmaWriteH *pSmaH, void *smaKey, int32_t keyLen, void *pData, int32_t dataLen,
                                    TXN *txn);
static int64_t tsdbGetIntervalByPrecision(int64_t interval, uint8_t intervalUnit, int8_t precision, bool adjusted);
static int32_t tsdbGetTSmaDays(STsdb *pTsdb, int64_t interval, int32_t storageLevel);
static int32_t tsdbSetTSmaDataFile(STSmaWriteH *pSmaH, int64_t indexUid, int32_t fid);
static int32_t tsdbInitTSmaFile(STSmaReadH *pSmaH, int64_t indexUid, TSKEY skey);
static bool    tsdbSetAndOpenTSmaFile(STSmaReadH *pReadH, TSKEY *queryKey);
static void    tsdbGetSmaDir(int32_t vgId, ETsdbSmaType smaType, char dirName[]);
static int32_t tsdbInsertTSmaDataImpl(STsdb *pTsdb, int64_t indexUid, const char *msg);
static int32_t tsdbInsertRSmaDataImpl(STsdb *pTsdb, const char *msg);

static FORCE_INLINE int32_t tsdbUidStorePut(STbUidStore *pStore, tb_uid_t suid, tb_uid_t *uid);
static FORCE_INLINE int32_t tsdbUpdateTbUidListImpl(STsdb *pTsdb, tb_uid_t *suid, SArray *tbUids);
static FORCE_INLINE int32_t tsdbExecuteRSmaImpl(STsdb *pTsdb, const void *pMsg, int32_t inputType,
                                                qTaskInfo_t *taskInfo, STSchema *pTSchema, tb_uid_t suid, tb_uid_t uid,
                                                int8_t level);
// mgmt interface
static int32_t tsdbDropTSmaDataImpl(STsdb *pTsdb, int64_t indexUid);

// Pool Memory
static SPoolMem *openPool();
static void      clearPool(SPoolMem *pPool);
static void      closePool(SPoolMem *pPool);
static void     *poolMalloc(void *arg, size_t size);
static void      poolFree(void *arg, void *ptr);

static int tsdbSmaBeginCommit(SSmaEnv *pEnv);
static int tsdbSmaEndCommit(SSmaEnv *pEnv);

// implementation
static FORCE_INLINE int16_t tsdbTSmaAdd(STsdb *pTsdb, int16_t n) {
  return atomic_add_fetch_16(&REPO_TSMA_NUM(pTsdb), n);
}
static FORCE_INLINE int16_t tsdbTSmaSub(STsdb *pTsdb, int16_t n) {
  return atomic_sub_fetch_16(&REPO_TSMA_NUM(pTsdb), n);
}

static FORCE_INLINE int32_t tsdbRLockSma(SSmaEnv *pEnv) {
  int code = taosThreadRwlockRdlock(&(pEnv->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int32_t tsdbWLockSma(SSmaEnv *pEnv) {
  int code = taosThreadRwlockWrlock(&(pEnv->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int32_t tsdbUnLockSma(SSmaEnv *pEnv) {
  int code = taosThreadRwlockUnlock(&(pEnv->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

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

int32_t tsdbInitSma(STsdb *pTsdb) {
  // tSma
  int32_t numOfTSma = taosArrayGetSize(metaGetSmaTbUids(REPO_META(pTsdb), false));
  if (numOfTSma > 0) {
    atomic_store_16(&REPO_TSMA_NUM(pTsdb), (int16_t)numOfTSma);
  }
  // TODO: rSma
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int8_t tsdbSmaStat(SSmaStatItem *pStatItem) {
  if (pStatItem) {
    return atomic_load_8(&pStatItem->state);
  }
  return TSDB_SMA_STAT_UNKNOWN;
}

static FORCE_INLINE bool tsdbSmaStatIsOK(SSmaStatItem *pStatItem, int8_t *state) {
  if (!pStatItem) {
    return false;
  }

  if (state) {
    *state = atomic_load_8(&pStatItem->state);
    return *state == TSDB_SMA_STAT_OK;
  }
  return atomic_load_8(&pStatItem->state) == TSDB_SMA_STAT_OK;
}

static FORCE_INLINE bool tsdbSmaStatIsExpired(SSmaStatItem *pStatItem) {
  return pStatItem ? (atomic_load_8(&pStatItem->state) & TSDB_SMA_STAT_EXPIRED) : true;
}

static FORCE_INLINE bool tsdbSmaStatIsDropped(SSmaStatItem *pStatItem) {
  return pStatItem ? (atomic_load_8(&pStatItem->state) & TSDB_SMA_STAT_DROPPED) : true;
}

static FORCE_INLINE void tsdbSmaStatSetOK(SSmaStatItem *pStatItem) {
  if (pStatItem) {
    atomic_store_8(&pStatItem->state, TSDB_SMA_STAT_OK);
  }
}

static FORCE_INLINE void tsdbSmaStatSetExpired(SSmaStatItem *pStatItem) {
  if (pStatItem) {
    atomic_or_fetch_8(&pStatItem->state, TSDB_SMA_STAT_EXPIRED);
  }
}

static FORCE_INLINE void tsdbSmaStatSetDropped(SSmaStatItem *pStatItem) {
  if (pStatItem) {
    atomic_or_fetch_8(&pStatItem->state, TSDB_SMA_STAT_DROPPED);
  }
}

static void tsdbGetSmaDir(int32_t vgId, ETsdbSmaType smaType, char dirName[]) {
  snprintf(dirName, TSDB_FILENAME_LEN, "vnode%svnode%d%s%s", TD_DIRSEP, vgId, TD_DIRSEP, TSDB_SMA_DNAME[smaType]);
}

static SSmaEnv *tsdbNewSmaEnv(const STsdb *pTsdb, int8_t smaType, const char *path, SDiskID did) {
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
    tsdbFreeSmaEnv(pEnv);
    return NULL;
  }

  SMA_ENV_DID(pEnv) = did;

  if (tsdbInitSmaStat(&SMA_ENV_STAT(pEnv), smaType) != TSDB_CODE_SUCCESS) {
    tsdbFreeSmaEnv(pEnv);
    return NULL;
  }

  char aname[TSDB_FILENAME_LEN] = {0};
  tfsAbsoluteName(REPO_TFS(pTsdb), did, path, aname);
  if (tsdbOpenDBEnv(&pEnv->dbEnv, aname) != TSDB_CODE_SUCCESS) {
    tsdbFreeSmaEnv(pEnv);
    return NULL;
  }

  if (!(pEnv->pPool = openPool())) {
    tsdbFreeSmaEnv(pEnv);
    return NULL;
  }

  return pEnv;
}

static int32_t tsdbInitSmaEnv(STsdb *pTsdb, int8_t smaType, const char *path, SDiskID did, SSmaEnv **pEnv) {
  if (!pEnv) {
    terrno = TSDB_CODE_INVALID_PTR;
    return TSDB_CODE_FAILED;
  }

  if (!(*pEnv)) {
    if (!(*pEnv = tsdbNewSmaEnv(pTsdb, smaType, path, did))) {
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
void tsdbDestroySmaEnv(SSmaEnv *pSmaEnv) {
  if (pSmaEnv) {
    tsdbDestroySmaState(pSmaEnv->pStat, SMA_ENV_TYPE(pSmaEnv));
    taosMemoryFreeClear(pSmaEnv->pStat);
    taosMemoryFreeClear(pSmaEnv->path);
    taosThreadRwlockDestroy(&(pSmaEnv->lock));
    tsdbCloseDBEnv(pSmaEnv->dbEnv);
    closePool(pSmaEnv->pPool);
  }
}

void *tsdbFreeSmaEnv(SSmaEnv *pSmaEnv) {
  tsdbDestroySmaEnv(pSmaEnv);
  taosMemoryFreeClear(pSmaEnv);
  return NULL;
}

static int32_t tsdbRefSmaStat(STsdb *pTsdb, SSmaStat *pStat) {
  if (!pStat) return 0;

  int ref = T_REF_INC(pStat);
  tsdbDebug("vgId:%d ref sma stat:%p, val:%d", REPO_ID(pTsdb), pStat, ref);
  return 0;
}

static int32_t tsdbUnRefSmaStat(STsdb *pTsdb, SSmaStat *pStat) {
  if (!pStat) return 0;

  int ref = T_REF_DEC(pStat);
  tsdbDebug("vgId:%d unref sma stat:%p, val:%d", REPO_ID(pTsdb), pStat, ref);
  return 0;
}

static int32_t tsdbInitSmaStat(SSmaStat **pSmaStat, int8_t smaType) {
  ASSERT(pSmaStat != NULL);

  if (*pSmaStat) {  // no lock
    return TSDB_CODE_SUCCESS;
  }

  /**
   *  1. Lazy mode utilized when init SSmaStat to update expired window(or hungry mode when tsdbNew).
   *  2. Currently, there is mutex lock when init SSmaEnv, thus no need add lock on SSmaStat, and please add lock if
   * tsdbInitSmaStat invoked in other multithread environment later.
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

static SSmaStatItem *tsdbNewSmaStatItem(int8_t state) {
  SSmaStatItem *pItem = NULL;

  pItem = (SSmaStatItem *)taosMemoryCalloc(1, sizeof(SSmaStatItem));
  if (pItem) {
    pItem->state = state;
    pItem->expiredWindows = taosHashInit(SMA_STATE_ITEM_HASH_SLOT, taosGetDefaultHashFunction(TSDB_DATA_TYPE_TIMESTAMP),
                                         true, HASH_ENTRY_LOCK);
    if (!pItem->expiredWindows) {
      taosMemoryFreeClear(pItem);
    }
  }
  return pItem;
}

static void *tsdbFreeSmaStatItem(SSmaStatItem *pSmaStatItem) {
  if (pSmaStatItem) {
    tdDestroyTSma(pSmaStatItem->pSma);
    taosMemoryFreeClear(pSmaStatItem->pSma);
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
int32_t tsdbDestroySmaState(SSmaStat *pSmaStat, int8_t smaType) {
  if (pSmaStat) {
    // TODO: use taosHashSetFreeFp when taosHashSetFreeFp is ready.
    if (smaType == TSDB_SMA_TYPE_TIME_RANGE) {
      void *item = taosHashIterate(SMA_STAT_ITEMS(pSmaStat), NULL);
      while (item) {
        SSmaStatItem *pItem = *(SSmaStatItem **)item;
        tsdbFreeSmaStatItem(pItem);
        item = taosHashIterate(SMA_STAT_ITEMS(pSmaStat), item);
      }
      taosHashCleanup(SMA_STAT_ITEMS(pSmaStat));
    } else if (smaType == TSDB_SMA_TYPE_ROLLUP) {
      void *infoHash = taosHashIterate(SMA_STAT_INFO_HASH(pSmaStat), NULL);
      while (infoHash) {
        SRSmaInfo *pInfoHash = *(SRSmaInfo **)infoHash;
        tsdbFreeRSmaInfo(pInfoHash);
        infoHash = taosHashIterate(SMA_STAT_INFO_HASH(pSmaStat), infoHash);
      }
      taosHashCleanup(SMA_STAT_INFO_HASH(pSmaStat));
    } else {
      ASSERT(0);
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tsdbCheckAndInitSmaEnv(STsdb *pTsdb, int8_t smaType) {
  SSmaEnv *pEnv = NULL;

  // return if already init
  switch (smaType) {
    case TSDB_SMA_TYPE_TIME_RANGE:
      if ((pEnv = (SSmaEnv *)atomic_load_ptr(&REPO_TSMA_ENV(pTsdb)))) {
        return TSDB_CODE_SUCCESS;
      }
      break;
    case TSDB_SMA_TYPE_ROLLUP:
      if ((pEnv = (SSmaEnv *)atomic_load_ptr(&REPO_RSMA_ENV(pTsdb)))) {
        return TSDB_CODE_SUCCESS;
      }
      break;
    default:
      terrno = TSDB_CODE_INVALID_PARA;
      return TSDB_CODE_FAILED;
  }

  // init sma env
  tsdbLockRepo(pTsdb);
  pEnv = (smaType == TSDB_SMA_TYPE_TIME_RANGE) ? atomic_load_ptr(&REPO_TSMA_ENV(pTsdb))
                                               : atomic_load_ptr(&REPO_RSMA_ENV(pTsdb));
  if (!pEnv) {
    char rname[TSDB_FILENAME_LEN] = {0};

    SDiskID did = {0};
    tfsAllocDisk(REPO_TFS(pTsdb), TFS_PRIMARY_LEVEL, &did);
    if (did.level < 0 || did.id < 0) {
      tsdbUnlockRepo(pTsdb);
      return TSDB_CODE_FAILED;
    }
    tsdbGetSmaDir(REPO_ID(pTsdb), smaType, rname);

    if (tfsMkdirRecurAt(REPO_TFS(pTsdb), rname, did) != TSDB_CODE_SUCCESS) {
      tsdbUnlockRepo(pTsdb);
      return TSDB_CODE_FAILED;
    }

    if (tsdbInitSmaEnv(pTsdb, smaType, rname, did, &pEnv) != TSDB_CODE_SUCCESS) {
      tsdbUnlockRepo(pTsdb);
      return TSDB_CODE_FAILED;
    }

    (smaType == TSDB_SMA_TYPE_TIME_RANGE) ? atomic_store_ptr(&REPO_TSMA_ENV(pTsdb), pEnv)
                                          : atomic_store_ptr(&REPO_RSMA_ENV(pTsdb), pEnv);
  }
  tsdbUnlockRepo(pTsdb);

  return TSDB_CODE_SUCCESS;
};

static int32_t tsdbSetExpiredWindow(STsdb *pTsdb, SHashObj *pItemsHash, int64_t indexUid, int64_t winSKey,
                                    int64_t version) {
  SSmaStatItem *pItem = taosHashGet(pItemsHash, &indexUid, sizeof(indexUid));
  if (!pItem) {
    // TODO: use TSDB_SMA_STAT_EXPIRED and update by stream computing later
    pItem = tsdbNewSmaStatItem(TSDB_SMA_STAT_OK);  // TODO use the real state
    if (!pItem) {
      // Response to stream computing: OOM
      // For query, if the indexUid not found, the TSDB should tell query module to query raw TS data.
      return TSDB_CODE_FAILED;
    }

    // cache smaMeta
    STSma *pSma = metaGetSmaInfoByIndex(REPO_META(pTsdb), indexUid, true);
    if (!pSma) {
      terrno = TSDB_CODE_TDB_NO_SMA_INDEX_IN_META;
      taosHashCleanup(pItem->expiredWindows);
      taosMemoryFree(pItem);
      tsdbWarn("vgId:%d update expired window failed for smaIndex %" PRIi64 " since %s", REPO_ID(pTsdb), indexUid,
               tstrerror(terrno));
      return TSDB_CODE_FAILED;
    }
    pItem->pSma = pSma;

    if (taosHashPut(pItemsHash, &indexUid, sizeof(indexUid), &pItem, sizeof(pItem)) != 0) {
      // If error occurs during put smaStatItem, free the resources of pItem
      taosHashCleanup(pItem->expiredWindows);
      taosMemoryFree(pItem);
      return TSDB_CODE_FAILED;
    }
  } else if (!(pItem = *(SSmaStatItem **)pItem)) {
    terrno = TSDB_CODE_INVALID_PTR;
    return TSDB_CODE_FAILED;
  }

  if (taosHashPut(pItem->expiredWindows, &winSKey, sizeof(TSKEY), &version, sizeof(version)) != 0) {
    // If error occurs during taosHashPut expired windows, remove the smaIndex from pTsdb->pSmaStat, thus TSDB would
    // tell query module to query raw TS data.
    // N.B.
    //  1) It is assumed to be extemely little probability event of fail to taosHashPut.
    //  2) This would solve the inconsistency to some extent, but not completely, unless we record all expired
    // windows failed to put into hash table.
    taosHashCleanup(pItem->expiredWindows);
    taosMemoryFreeClear(pItem->pSma);
    taosHashRemove(pItemsHash, &indexUid, sizeof(indexUid));
    tsdbWarn("vgId:%d smaIndex %" PRIi64 ", put skey %" PRIi64 " to expire window fail", REPO_ID(pTsdb), indexUid,
             winSKey);
    return TSDB_CODE_FAILED;
  }

  tsdbDebug("vgId:%d smaIndex %" PRIi64 ", put skey %" PRIi64 " to expire window succeed", REPO_ID(pTsdb), indexUid,
            winSKey);
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Update expired window according to msg from stream computing module.
 *
 * @param pTsdb
 * @param msg SSubmitReq
 * @return int32_t
 */
int32_t tsdbUpdateExpiredWindowImpl(STsdb *pTsdb, SSubmitReq *pMsg, int64_t version) {
  // no time-range-sma, just return success
  if (atomic_load_16(&REPO_TSMA_NUM(pTsdb)) <= 0) {
    tsdbTrace("vgId:%d not update expire window since no tSma", REPO_ID(pTsdb));
    return TSDB_CODE_SUCCESS;
  }

  if (!REPO_META(pTsdb)) {
    terrno = TSDB_CODE_INVALID_PTR;
    return TSDB_CODE_FAILED;
  }

  if (tsdbCheckAndInitSmaEnv(pTsdb, TSDB_SMA_TYPE_TIME_RANGE) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TDB_INIT_FAILED;
    return TSDB_CODE_FAILED;
  }

  // Firstly, assume that tSma can only be created on super table/normal table.
  // getActiveTimeWindow

  SSmaEnv  *pEnv = REPO_TSMA_ENV(pTsdb);
  SSmaStat *pStat = SMA_ENV_STAT(pEnv);
  SHashObj *pItemsHash = SMA_ENV_STAT_ITEMS(pEnv);

  TASSERT(pEnv && pStat && pItemsHash);

  // basic procedure
  // TODO: optimization
  tsdbRefSmaStat(pTsdb, pStat);

  SSubmitMsgIter msgIter = {0};
  SSubmitBlk    *pBlock = NULL;
  SInterval      interval = {0};
  TSKEY          lastWinSKey = INT64_MIN;

  if (tInitSubmitMsgIter(pMsg, &msgIter) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  while (true) {
    tGetSubmitMsgNext(&msgIter, &pBlock);
    if (!pBlock) break;

    STSmaWrapper *pSW = NULL;
    STSma        *pTSma = NULL;

    SSubmitBlkIter blkIter = {0};
    if (tInitSubmitBlkIter(&msgIter, pBlock, &blkIter) != TSDB_CODE_SUCCESS) {
      pSW = tdFreeTSmaWrapper(pSW);
      break;
    }

    while (true) {
      STSRow *row = tGetSubmitBlkNext(&blkIter);
      if (!row) {
        tdFreeTSmaWrapper(pSW);
        break;
      }
      if (!pSW || (pTSma->tableUid != pBlock->suid)) {
        if (pSW) {
          pSW = tdFreeTSmaWrapper(pSW);
        }
        if (!(pSW = metaGetSmaInfoByTable(REPO_META(pTsdb), pBlock->suid))) {
          break;
        }
        if ((pSW->number) <= 0 || !pSW->tSma) {
          pSW = tdFreeTSmaWrapper(pSW);
          break;
        }

        pTSma = pSW->tSma;

        interval.interval = pTSma->interval;
        interval.intervalUnit = pTSma->intervalUnit;
        interval.offset = pTSma->offset;
        interval.precision = REPO_CFG(pTsdb)->precision;
        interval.sliding = pTSma->sliding;
        interval.slidingUnit = pTSma->slidingUnit;
      }

      TSKEY winSKey = taosTimeTruncate(TD_ROW_KEY(row), &interval, interval.precision);

      if (lastWinSKey != winSKey) {
        lastWinSKey = winSKey;
        tsdbSetExpiredWindow(pTsdb, pItemsHash, pTSma->indexUid, winSKey, version);
      } else {
        tsdbDebug("vgId:%d smaIndex %" PRIi64 ", put skey %" PRIi64 " to expire window ignore as duplicated",
                  REPO_ID(pTsdb), pTSma->indexUid, winSKey);
      }
    }
  }

  tsdbUnRefSmaStat(pTsdb, pStat);

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief When sma data received from stream computing, make the relative expired window valid.
 *
 * @param pTsdb
 * @param pStat
 * @param indexUid
 * @param skey
 * @return int32_t
 */
static int32_t tsdbResetExpiredWindow(STsdb *pTsdb, SSmaStat *pStat, int64_t indexUid, TSKEY skey) {
  SSmaStatItem *pItem = NULL;

  tsdbRefSmaStat(pTsdb, pStat);

  if (pStat && SMA_STAT_ITEMS(pStat)) {
    pItem = taosHashGet(SMA_STAT_ITEMS(pStat), &indexUid, sizeof(indexUid));
  }
  if ((pItem) && ((pItem = *(SSmaStatItem **)pItem))) {
    // pItem resides in hash buffer all the time unless drop sma index
    // TODO: multithread protect
    if (taosHashRemove(pItem->expiredWindows, &skey, sizeof(TSKEY)) != 0) {
      // error handling
      tsdbUnRefSmaStat(pTsdb, pStat);
      tsdbWarn("vgId:%d remove skey %" PRIi64 " from expired window for sma index %" PRIi64 " fail", REPO_ID(pTsdb),
               skey, indexUid);
      return TSDB_CODE_FAILED;
    }
    tsdbDebug("vgId:%d remove skey %" PRIi64 " from expired window for sma index %" PRIi64 " succeed", REPO_ID(pTsdb),
              skey, indexUid);
    // TODO: use a standalone interface to received state upate notification from stream computing module.
    /**
     * @brief state
     *  - When SMA env init in TSDB, its status is TSDB_SMA_STAT_OK.
     *  - In startup phase of stream computing module, it should notify the SMA env in TSDB to expired if needed(e.g.
     * when batch data caculation not finised)
     *  - When TSDB_SMA_STAT_OK, the stream computing module should also notify that to the SMA env in TSDB.
     */
    pItem->state = TSDB_SMA_STAT_OK;
  } else {
    // error handling
    tsdbUnRefSmaStat(pTsdb, pStat);
    tsdbWarn("vgId:%d expired window %" PRIi64 " not exists for sma index %" PRIi64, REPO_ID(pTsdb), skey, indexUid);
    return TSDB_CODE_FAILED;
  }

  tsdbUnRefSmaStat(pTsdb, pStat);
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Judge the tSma storage level
 *
 * @param interval
 * @param intervalUnit
 * @return int32_t
 */
static int32_t tsdbGetSmaStorageLevel(int64_t interval, int8_t intervalUnit) {
  // TODO: configurable for SMA_STORAGE_SPLIT_HOURS?
  switch (intervalUnit) {
    case TIME_UNIT_HOUR:
      if (interval < SMA_STORAGE_SPLIT_HOURS) {
        return SMA_STORAGE_LEVEL_DFILESET;
      }
      break;
    case TIME_UNIT_MINUTE:
      if (interval < 60 * SMA_STORAGE_SPLIT_HOURS) {
        return SMA_STORAGE_LEVEL_DFILESET;
      }
      break;
    case TIME_UNIT_SECOND:
      if (interval < 3600 * SMA_STORAGE_SPLIT_HOURS) {
        return SMA_STORAGE_LEVEL_DFILESET;
      }
      break;
    case TIME_UNIT_MILLISECOND:
      if (interval < 3600 * 1e3 * SMA_STORAGE_SPLIT_HOURS) {
        return SMA_STORAGE_LEVEL_DFILESET;
      }
      break;
    case TIME_UNIT_MICROSECOND:
      if (interval < 3600 * 1e6 * SMA_STORAGE_SPLIT_HOURS) {
        return SMA_STORAGE_LEVEL_DFILESET;
      }
      break;
    case TIME_UNIT_NANOSECOND:
      if (interval < 3600 * 1e9 * SMA_STORAGE_SPLIT_HOURS) {
        return SMA_STORAGE_LEVEL_DFILESET;
      }
      break;
    default:
      break;
  }
  return SMA_STORAGE_LEVEL_TSDB;
}

/**
 * @brief Insert TSma data blocks to DB File build by B+Tree
 *
 * @param pSmaH
 * @param smaKey  tableUid-colId-skeyOfWindow(8-2-8)
 * @param keyLen
 * @param pData
 * @param dataLen
 * @return int32_t
 */
static int32_t tsdbInsertTSmaBlocks(STSmaWriteH *pSmaH, void *smaKey, int32_t keyLen, void *pData, int32_t dataLen,
                                    TXN *txn) {
  SDBFile *pDBFile = &pSmaH->dFile;

  // TODO: insert tsma data blocks into B+Tree(TDB)
  if (tsdbSaveSmaToDB(pDBFile, smaKey, keyLen, pData, dataLen, txn) != 0) {
    tsdbWarn("vgId:%d insert tsma data blocks into %s: smaKey %" PRIx64 "-%" PRIx64 ", dataLen %" PRIu32 " fail",
             REPO_ID(pSmaH->pTsdb), pDBFile->path, *(int64_t *)smaKey, *(int64_t *)POINTER_SHIFT(smaKey, 8), dataLen);
    return TSDB_CODE_FAILED;
  }
  tsdbDebug("vgId:%d insert tsma data blocks into %s: smaKey %" PRIx64 "-%" PRIx64 ", dataLen %" PRIu32 " succeed",
            REPO_ID(pSmaH->pTsdb), pDBFile->path, *(int64_t *)smaKey, *(int64_t *)POINTER_SHIFT(smaKey, 8), dataLen);

#ifdef _TEST_SMA_PRINT_DEBUG_LOG_
  uint32_t valueSize = 0;
  void    *data = tsdbGetSmaDataByKey(pDBFile, smaKey, keyLen, &valueSize);
  ASSERT(data != NULL);
  for (uint32_t v = 0; v < valueSize; v += 8) {
    tsdbWarn("vgId:%d insert sma data val[%d] %" PRIi64, REPO_ID(pSmaH->pTsdb), v, *(int64_t *)POINTER_SHIFT(data, v));
  }
#endif
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Approximate value for week/month/year.
 *
 * @param interval
 * @param intervalUnit
 * @param precision
 * @param adjusted Interval already adjusted according to DB precision
 * @return int64_t
 */
static int64_t tsdbGetIntervalByPrecision(int64_t interval, uint8_t intervalUnit, int8_t precision, bool adjusted) {
  if (adjusted) {
    return interval;
  }

  switch (intervalUnit) {
    case TIME_UNIT_YEAR:  // approximate value
      interval *= 365 * 86400 * 1e3;
      break;
    case TIME_UNIT_MONTH:  // approximate value
      interval *= 30 * 86400 * 1e3;
      break;
    case TIME_UNIT_WEEK:  // approximate value
      interval *= 7 * 86400 * 1e3;
      break;
    case TIME_UNIT_DAY:  // the interval for tSma calculation must <= day
      interval *= 86400 * 1e3;
      break;
    case TIME_UNIT_HOUR:
      interval *= 3600 * 1e3;
      break;
    case TIME_UNIT_MINUTE:
      interval *= 60 * 1e3;
      break;
    case TIME_UNIT_SECOND:
      interval *= 1e3;
      break;
    default:
      break;
  }

  switch (precision) {
    case TSDB_TIME_PRECISION_MILLI:
      if (TIME_UNIT_MICROSECOND == intervalUnit) {  // us
        return interval / 1e3;
      } else if (TIME_UNIT_NANOSECOND == intervalUnit) {  //  nano second
        return interval / 1e6;
      } else {  // ms
        return interval;
      }
      break;
    case TSDB_TIME_PRECISION_MICRO:
      if (TIME_UNIT_MICROSECOND == intervalUnit) {  // us
        return interval;
      } else if (TIME_UNIT_NANOSECOND == intervalUnit) {  //  ns
        return interval / 1e3;
      } else {  // ms
        return interval * 1e3;
      }
      break;
    case TSDB_TIME_PRECISION_NANO:
      if (TIME_UNIT_MICROSECOND == intervalUnit) {  // us
        return interval * 1e3;
      } else if (TIME_UNIT_NANOSECOND == intervalUnit) {  // ns
        return interval;
      } else {  // ms
        return interval * 1e6;
      }
      break;
    default:                                        // ms
      if (TIME_UNIT_MICROSECOND == intervalUnit) {  // us
        return interval / 1e3;
      } else if (TIME_UNIT_NANOSECOND == intervalUnit) {  //  ns
        return interval / 1e6;
      } else {  // ms
        return interval;
      }
      break;
  }
  return interval;
}

static int32_t tsdbInitTSmaWriteH(STSmaWriteH *pSmaH, STsdb *pTsdb, const SArray *pDataBlocks, int64_t interval,
                                  int8_t intervalUnit) {
  pSmaH->pTsdb = pTsdb;
  pSmaH->interval = tsdbGetIntervalByPrecision(interval, intervalUnit, REPO_CFG(pTsdb)->precision, true);
  pSmaH->pDataBlocks = pDataBlocks;
  pSmaH->dFile.fid = TSDB_IVLD_FID;
  return TSDB_CODE_SUCCESS;
}

static void tsdbDestroyTSmaWriteH(STSmaWriteH *pSmaH) {
  if (pSmaH) {
    tsdbCloseDBF(&pSmaH->dFile);
  }
}

static int32_t tsdbSetTSmaDataFile(STSmaWriteH *pSmaH, int64_t indexUid, int32_t fid) {
  STsdb *pTsdb = pSmaH->pTsdb;
  ASSERT(!pSmaH->dFile.path && !pSmaH->dFile.pDB);

  pSmaH->dFile.fid = fid;
  char tSmaFile[TSDB_FILENAME_LEN] = {0};
  snprintf(tSmaFile, TSDB_FILENAME_LEN, "%" PRIi64 "%sv%df%d.tsma", indexUid, TD_DIRSEP, REPO_ID(pTsdb), fid);
  pSmaH->dFile.path = strdup(tSmaFile);

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief
 *
 * @param pTsdb
 * @param interval Interval calculated by DB's precision
 * @param storageLevel
 * @return int32_t
 */
static int32_t tsdbGetTSmaDays(STsdb *pTsdb, int64_t interval, int32_t storageLevel) {
  STsdbKeepCfg *pCfg = REPO_KEEP_CFG(pTsdb);
  int32_t       daysPerFile = pCfg->days;

  if (storageLevel == SMA_STORAGE_LEVEL_TSDB) {
    int32_t days = SMA_STORAGE_TSDB_TIMES * (interval / tsTickPerMin[pCfg->precision]);
    daysPerFile = days > SMA_STORAGE_TSDB_DAYS ? days : SMA_STORAGE_TSDB_DAYS;
  }

  return daysPerFile;
}

static int tsdbSmaBeginCommit(SSmaEnv *pEnv) {
  TXN *pTxn = &pEnv->txn;
  // start a new txn
  tdbTxnOpen(pTxn, 0, poolMalloc, poolFree, pEnv->pPool, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED);
  if (tdbBegin(pEnv->dbEnv, pTxn) != 0) {
    tsdbWarn("tsdbSma tdb begin commit fail");
    return -1;
  }
  return 0;
}

static int tsdbSmaEndCommit(SSmaEnv *pEnv) {
  TXN *pTxn = &pEnv->txn;

  // Commit current txn
  if (tdbCommit(pEnv->dbEnv, pTxn) != 0) {
    tsdbWarn("tsdbSma tdb end commit fail");
    return -1;
  }
  tdbTxnClose(pTxn);
  clearPool(pEnv->pPool);
  return 0;
}

/**
 * @brief Insert/Update Time-range-wise SMA data.
 *  - If interval < SMA_STORAGE_SPLIT_HOURS(e.g. 24), save the SMA data as a part of DFileSet to e.g.
 * v3f1900.tsma.${sma_index_name}. The days is the same with that for TS data files.
 *  - If interval >= SMA_STORAGE_SPLIT_HOURS, save the SMA data to e.g. vnode3/tsma/v3f632.tsma.${sma_index_name}. The
 * days is 30 times of the interval, and the minimum days is SMA_STORAGE_TSDB_DAYS(30d).
 *  - The destination file of one data block for some interval is determined by its start TS key.
 *
 * @param pTsdb
 * @param msg
 * @return int32_t
 */
static int32_t tsdbInsertTSmaDataImpl(STsdb *pTsdb, int64_t indexUid, const char *msg) {
  STsdbCfg     *pCfg = REPO_CFG(pTsdb);
  const SArray *pDataBlocks = (const SArray *)msg;

  // TODO: destroy SSDataBlocks(msg)

  // For super table aggregation, the sma data is stored in vgroup calculated from the hash value of stable name. Thus
  // the sma data would arrive ahead of the update-expired-window msg.
  if (tsdbCheckAndInitSmaEnv(pTsdb, TSDB_SMA_TYPE_TIME_RANGE) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TDB_INIT_FAILED;
    return TSDB_CODE_FAILED;
  }

  if (!pDataBlocks) {
    terrno = TSDB_CODE_INVALID_PTR;
    tsdbWarn("vgId:%d insert tSma data failed since pDataBlocks is NULL", REPO_ID(pTsdb));
    return terrno;
  }

  if (taosArrayGetSize(pDataBlocks) <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    tsdbWarn("vgId:%d insert tSma data failed since pDataBlocks is empty", REPO_ID(pTsdb));
    return TSDB_CODE_FAILED;
  }

  SSmaEnv      *pEnv = REPO_TSMA_ENV(pTsdb);
  SSmaStat     *pStat = SMA_ENV_STAT(pEnv);
  SSmaStatItem *pItem = NULL;

  tsdbRefSmaStat(pTsdb, pStat);

  if (pStat && SMA_STAT_ITEMS(pStat)) {
    pItem = taosHashGet(SMA_STAT_ITEMS(pStat), &indexUid, sizeof(indexUid));
  }

  if (!pItem || !(pItem = *(SSmaStatItem **)pItem) || tsdbSmaStatIsDropped(pItem)) {
    terrno = TSDB_CODE_TDB_INVALID_SMA_STAT;
    tsdbUnRefSmaStat(pTsdb, pStat);
    return TSDB_CODE_FAILED;
  }

  STSma      *pSma = pItem->pSma;
  STSmaWriteH tSmaH = {0};

  if (tsdbInitTSmaWriteH(&tSmaH, pTsdb, pDataBlocks, pSma->interval, pSma->intervalUnit) != 0) {
    return TSDB_CODE_FAILED;
  }

  char rPath[TSDB_FILENAME_LEN] = {0};
  char aPath[TSDB_FILENAME_LEN] = {0};
  snprintf(rPath, TSDB_FILENAME_LEN, "%s%s%" PRIi64, SMA_ENV_PATH(pEnv), TD_DIRSEP, indexUid);
  tfsAbsoluteName(REPO_TFS(pTsdb), SMA_ENV_DID(pEnv), rPath, aPath);
  if (!taosCheckExistFile(aPath)) {
    if (tfsMkdirRecurAt(REPO_TFS(pTsdb), rPath, SMA_ENV_DID(pEnv)) != TSDB_CODE_SUCCESS) {
      tsdbUnRefSmaStat(pTsdb, pStat);
      return TSDB_CODE_FAILED;
    }
  }

  // Step 1: Judge the storage level and days
  int32_t storageLevel = tsdbGetSmaStorageLevel(pSma->interval, pSma->intervalUnit);
  int32_t daysPerFile = tsdbGetTSmaDays(pTsdb, tSmaH.interval, storageLevel);

  char    smaKey[SMA_KEY_LEN] = {0};  // key: skey + groupId
  char    dataBuf[512] = {0};         // val: aggr data // TODO: handle 512 buffer?
  void   *pDataBuf = NULL;
  int32_t sz = taosArrayGetSize(pDataBlocks);
  for (int32_t i = 0; i < sz; ++i) {
    SSDataBlock *pDataBlock = taosArrayGet(pDataBlocks, i);
    int32_t      colNum = pDataBlock->info.numOfCols;
    int32_t      rows = pDataBlock->info.rows;
    int32_t      rowSize = pDataBlock->info.rowSize;
    int64_t      groupId = pDataBlock->info.groupId;
    for (int32_t j = 0; j < rows; ++j) {
      printf("|");
      TSKEY skey = TSKEY_INITIAL_VAL;  //  the start key of TS window by interval
      void *pSmaKey = &smaKey;
      bool  isStartKey = false;

      int32_t tlen = 0;     // reset the len
      pDataBuf = &dataBuf;  // reset the buf
      for (int32_t k = 0; k < colNum; ++k) {
        SColumnInfoData *pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
        void            *var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
        switch (pColInfoData->info.type) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            if (!isStartKey) {
              isStartKey = true;
              skey = *(TSKEY *)var;
              printf("= skey %" PRIi64 " groupId = %" PRIi64 "|", skey, groupId);
              tsdbEncodeTSmaKey(groupId, skey, &pSmaKey);
            } else {
              printf(" %" PRIi64 " |", *(int64_t *)var);
              tlen += taosEncodeFixedI64(&pDataBuf, *(int64_t *)var);
              break;
            }
            break;
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_UTINYINT:
            printf(" %15d |", *(uint8_t *)var);
            tlen += taosEncodeFixedU8(&pDataBuf, *(uint8_t *)var);
            break;
          case TSDB_DATA_TYPE_TINYINT:
            printf(" %15d |", *(int8_t *)var);
            tlen += taosEncodeFixedI8(&pDataBuf, *(int8_t *)var);
            break;
          case TSDB_DATA_TYPE_SMALLINT:
            printf(" %15d |", *(int16_t *)var);
            tlen += taosEncodeFixedI16(&pDataBuf, *(int16_t *)var);
            break;
          case TSDB_DATA_TYPE_USMALLINT:
            printf(" %15d |", *(uint16_t *)var);
            tlen += taosEncodeFixedU16(&pDataBuf, *(uint16_t *)var);
            break;
          case TSDB_DATA_TYPE_INT:
            printf(" %15d |", *(int32_t *)var);
            tlen += taosEncodeFixedI32(&pDataBuf, *(int32_t *)var);
            break;
          case TSDB_DATA_TYPE_FLOAT:
            printf(" %15f |", *(float *)var);
            tlen += taosEncodeBinary(&pDataBuf, var, sizeof(float));
            break;
          case TSDB_DATA_TYPE_UINT:
            printf(" %15u |", *(uint32_t *)var);
            tlen += taosEncodeFixedU32(&pDataBuf, *(uint32_t *)var);
            break;
          case TSDB_DATA_TYPE_BIGINT:
            printf(" %15ld |", *(int64_t *)var);
            tlen += taosEncodeFixedI64(&pDataBuf, *(int64_t *)var);
            break;
          case TSDB_DATA_TYPE_DOUBLE:
            printf(" %15lf |", *(double *)var);
            tlen += taosEncodeBinary(&pDataBuf, var, sizeof(double));
          case TSDB_DATA_TYPE_UBIGINT:
            printf(" %15lu |", *(uint64_t *)var);
            tlen += taosEncodeFixedU64(&pDataBuf, *(uint64_t *)var);
            break;
          case TSDB_DATA_TYPE_NCHAR: {
            char tmpChar[100] = {0};
            strncpy(tmpChar, varDataVal(var), varDataLen(var));
            printf(" %s |", tmpChar);
            tlen += taosEncodeBinary(&pDataBuf, varDataVal(var), varDataLen(var));
            break;
          }
          case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
            char tmpChar[100] = {0};
            strncpy(tmpChar, varDataVal(var), varDataLen(var));
            printf(" %s |", tmpChar);
            tlen += taosEncodeBinary(&pDataBuf, varDataVal(var), varDataLen(var));
            break;
          }
          case TSDB_DATA_TYPE_VARBINARY:
            // TODO: add binary/varbinary
            TASSERT(0);
          default:
            printf("the column type %" PRIi16 " is undefined\n", pColInfoData->info.type);
            TASSERT(0);
            break;
        }
      }
      // if ((tlen > 0) && (skey != TSKEY_INITIAL_VAL)) {
      if (tlen > 0) {
        int32_t fid = (int32_t)(TSDB_KEY_FID(skey, daysPerFile, pCfg->precision));

        // Step 2: Set the DFile for storage of SMA index, and iterate/split the TSma data and store to B+Tree index
        // file
        //         - Set and open the DFile or the B+Tree file
        // TODO: tsdbStartTSmaCommit();
        if (fid != tSmaH.dFile.fid) {
          if (tSmaH.dFile.fid != TSDB_IVLD_FID) {
            tsdbSmaEndCommit(pEnv);
            tsdbCloseDBF(&tSmaH.dFile);
          }
          tsdbSetTSmaDataFile(&tSmaH, indexUid, fid);
          if (tsdbOpenDBF(pEnv->dbEnv, &tSmaH.dFile) != 0) {
            tsdbWarn("vgId:%d open DB file %s failed since %s", REPO_ID(pTsdb),
                     tSmaH.dFile.path ? tSmaH.dFile.path : "path is NULL", tstrerror(terrno));
            tsdbDestroyTSmaWriteH(&tSmaH);
            tsdbUnRefSmaStat(pTsdb, pStat);
            return TSDB_CODE_FAILED;
          }
          tsdbSmaBeginCommit(pEnv);
        }

        if (tsdbInsertTSmaBlocks(&tSmaH, &smaKey, SMA_KEY_LEN, dataBuf, tlen, &pEnv->txn) != 0) {
          tsdbWarn("vgId:%d insert tsma data blocks fail for index %" PRIi64 ", skey %" PRIi64 ", groupId %" PRIi64
                   " since %s",
                   REPO_ID(pTsdb), indexUid, skey, groupId, tstrerror(terrno));
          tsdbSmaEndCommit(pEnv);
          tsdbDestroyTSmaWriteH(&tSmaH);
          tsdbUnRefSmaStat(pTsdb, pStat);
          return TSDB_CODE_FAILED;
        }
        tsdbDebug("vgId:%d insert tsma data blocks success for index %" PRIi64 ", skey %" PRIi64 ", groupId %" PRIi64,
                  REPO_ID(pTsdb), indexUid, skey, groupId);
        // TODO:tsdbEndTSmaCommit();

        // Step 3: reset the SSmaStat
        tsdbResetExpiredWindow(pTsdb, pStat, indexUid, skey);
      } else {
        tsdbWarn("vgId:%d invalid data skey:%" PRIi64 ", tlen %" PRIi32 " during insert tSma data for %" PRIi64,
                 REPO_ID(pTsdb), skey, tlen, indexUid);
      }

      printf("\n");
    }
  }
  tsdbSmaEndCommit(pEnv);  // TODO: not commit for every insert
  tsdbDestroyTSmaWriteH(&tSmaH);
  tsdbUnRefSmaStat(pTsdb, pStat);

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Drop tSma data and local cache
 *        - insert/query reference
 * @param pTsdb
 * @param msg
 * @return int32_t
 */
static int32_t tsdbDropTSmaDataImpl(STsdb *pTsdb, int64_t indexUid) {
  SSmaEnv *pEnv = atomic_load_ptr(&REPO_TSMA_ENV(pTsdb));

  // clear local cache
  if (pEnv) {
    tsdbDebug("vgId:%d drop tSma local cache for %" PRIi64, REPO_ID(pTsdb), indexUid);

    SSmaStatItem *pItem = taosHashGet(SMA_ENV_STAT_ITEMS(pEnv), &indexUid, sizeof(indexUid));
    if ((pItem) || ((pItem = *(SSmaStatItem **)pItem))) {
      if (tsdbSmaStatIsDropped(pItem)) {
        tsdbDebug("vgId:%d tSma stat is already dropped for %" PRIi64, REPO_ID(pTsdb), indexUid);
        return TSDB_CODE_TDB_INVALID_ACTION;  // TODO: duplicate drop msg would be intercepted by mnode
      }

      tsdbWLockSma(pEnv);
      if (tsdbSmaStatIsDropped(pItem)) {
        tsdbUnLockSma(pEnv);
        tsdbDebug("vgId:%d tSma stat is already dropped for %" PRIi64, REPO_ID(pTsdb), indexUid);
        return TSDB_CODE_TDB_INVALID_ACTION;  // TODO: duplicate drop msg would be intercepted by mnode
      }
      tsdbSmaStatSetDropped(pItem);
      tsdbUnLockSma(pEnv);

      int32_t nSleep = 0;
      int32_t refVal = INT32_MAX;
      while (true) {
        if ((refVal = T_REF_VAL_GET(SMA_ENV_STAT(pEnv))) <= 0) {
          tsdbDebug("vgId:%d drop index %" PRIi64 " since refVal=%d", REPO_ID(pTsdb), indexUid, refVal);
          break;
        }
        tsdbDebug("vgId:%d wait 1s to drop index %" PRIi64 " since refVal=%d", REPO_ID(pTsdb), indexUid, refVal);
        taosSsleep(1);
        if (++nSleep > SMA_DROP_EXPIRED_TIME) {
          tsdbDebug("vgId:%d drop index %" PRIi64 " after wait %d (refVal=%d)", REPO_ID(pTsdb), indexUid, nSleep,
                    refVal);
          break;
        };
      }

      tsdbFreeSmaStatItem(pItem);
      tsdbDebug("vgId:%d getTSmaDataImpl failed since no index %" PRIi64 " in local cache", REPO_ID(pTsdb), indexUid);
    }
  }
  // clear sma data files
  // TODO:
  return TSDB_CODE_SUCCESS;
}

static int32_t tsdbSetRSmaDataFile(STSmaWriteH *pSmaH, int32_t fid) {
  STsdb *pTsdb = pSmaH->pTsdb;

  char tSmaFile[TSDB_FILENAME_LEN] = {0};
  snprintf(tSmaFile, TSDB_FILENAME_LEN, "v%df%d.rsma", REPO_ID(pTsdb), fid);
  pSmaH->dFile.path = strdup(tSmaFile);

  return TSDB_CODE_SUCCESS;
}

static int32_t tsdbInsertRSmaDataImpl(STsdb *pTsdb, const char *msg) {
  STsdbCfg     *pCfg = REPO_CFG(pTsdb);
  const SArray *pDataBlocks = (const SArray *)msg;
  SSmaEnv      *pEnv = atomic_load_ptr(&REPO_RSMA_ENV(pTsdb));
  int64_t       indexUid = SMA_TEST_INDEX_UID;

  if (!pEnv) {
    terrno = TSDB_CODE_INVALID_PTR;
    tsdbWarn("vgId:%d insert rSma data failed since pTSmaEnv is NULL", REPO_ID(pTsdb));
    return terrno;
  }

  if (!pDataBlocks) {
    terrno = TSDB_CODE_INVALID_PTR;
    tsdbWarn("vgId:%d insert rSma data failed since pDataBlocks is NULL", REPO_ID(pTsdb));
    return terrno;
  }

  if (taosArrayGetSize(pDataBlocks) <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    tsdbWarn("vgId:%d insert rSma data failed since pDataBlocks is empty", REPO_ID(pTsdb));
    return TSDB_CODE_FAILED;
  }

  SSmaStat     *pStat = SMA_ENV_STAT(pEnv);
  SSmaStatItem *pItem = NULL;

  tsdbRefSmaStat(pTsdb, pStat);

  if (pStat && SMA_STAT_ITEMS(pStat)) {
    pItem = taosHashGet(SMA_STAT_ITEMS(pStat), &indexUid, sizeof(indexUid));
  }

  if (!pItem || !(pItem = *(SSmaStatItem **)pItem) || tsdbSmaStatIsDropped(pItem)) {
    terrno = TSDB_CODE_TDB_INVALID_SMA_STAT;
    tsdbUnRefSmaStat(pTsdb, pStat);
    return TSDB_CODE_FAILED;
  }

  STSma *pSma = pItem->pSma;

  STSmaWriteH tSmaH = {0};

  if (tsdbInitTSmaWriteH(&tSmaH, pTsdb, pDataBlocks, pSma->interval, pSma->intervalUnit) != 0) {
    return TSDB_CODE_FAILED;
  }

  char rPath[TSDB_FILENAME_LEN] = {0};
  char aPath[TSDB_FILENAME_LEN] = {0};
  snprintf(rPath, TSDB_FILENAME_LEN, "%s%s%" PRIi64, SMA_ENV_PATH(pEnv), TD_DIRSEP, indexUid);
  tfsAbsoluteName(REPO_TFS(pTsdb), SMA_ENV_DID(pEnv), rPath, aPath);
  if (!taosCheckExistFile(aPath)) {
    if (tfsMkdirRecurAt(REPO_TFS(pTsdb), rPath, SMA_ENV_DID(pEnv)) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_FAILED;
    }
  }

  // Step 1: Judge the storage level and days
  int32_t storageLevel = tsdbGetSmaStorageLevel(pSma->interval, pSma->intervalUnit);
  int32_t daysPerFile = tsdbGetTSmaDays(pTsdb, tSmaH.interval, storageLevel);
#if 0
  int32_t fid = (int32_t)(TSDB_KEY_FID(pData->skey, daysPerFile, pCfg->precision));

  // Step 2: Set the DFile for storage of SMA index, and iterate/split the TSma data and store to B+Tree index file
  //         - Set and open the DFile or the B+Tree file
  // TODO: tsdbStartTSmaCommit();
  tsdbSetTSmaDataFile(&tSmaH, pData, indexUid, fid);
  if (tsdbOpenDBF(pTsdb->pTSmaEnv->dbEnv, &tSmaH.dFile) != 0) {
    tsdbWarn("vgId:%d open DB file %s failed since %s", REPO_ID(pTsdb),
             tSmaH.dFile.path ? tSmaH.dFile.path : "path is NULL", tstrerror(terrno));
    tsdbDestroyTSmaWriteH(&tSmaH);
    return TSDB_CODE_FAILED;
  }

  if (tsdbInsertTSmaDataSection(&tSmaH, pData) != 0) {
    tsdbWarn("vgId:%d insert tSma data section failed since %s", REPO_ID(pTsdb), tstrerror(terrno));
    tsdbDestroyTSmaWriteH(&tSmaH);
    return TSDB_CODE_FAILED;
  }
  // TODO:tsdbEndTSmaCommit();

  // Step 3: reset the SSmaStat
  tsdbResetExpiredWindow(pTsdb, SMA_ENV_STAT(pTsdb->pTSmaEnv), pData->indexUid, pData->skey);
#endif

  tsdbDestroyTSmaWriteH(&tSmaH);
  tsdbUnRefSmaStat(pTsdb, pStat);
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief
 *
 * @param pSmaH
 * @param pTsdb
 * @param interval
 * @param intervalUnit
 * @return int32_t
 */
static int32_t tsdbInitTSmaReadH(STSmaReadH *pSmaH, STsdb *pTsdb, int64_t interval, int8_t intervalUnit) {
  pSmaH->pTsdb = pTsdb;
  pSmaH->interval = tsdbGetIntervalByPrecision(interval, intervalUnit, REPO_CFG(pTsdb)->precision, true);
  pSmaH->storageLevel = tsdbGetSmaStorageLevel(interval, intervalUnit);
  pSmaH->days = tsdbGetTSmaDays(pTsdb, pSmaH->interval, pSmaH->storageLevel);
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Init of tSma FS
 *
 * @param pReadH
 * @param indexUid
 * @param skey
 * @return int32_t
 */
static int32_t tsdbInitTSmaFile(STSmaReadH *pSmaH, int64_t indexUid, TSKEY skey) {
  STsdb *pTsdb = pSmaH->pTsdb;

  int32_t fid = (int32_t)(TSDB_KEY_FID(skey, pSmaH->days, REPO_CFG(pTsdb)->precision));
  char    tSmaFile[TSDB_FILENAME_LEN] = {0};
  snprintf(tSmaFile, TSDB_FILENAME_LEN, "%" PRIi64 "%sv%df%d.tsma", indexUid, TD_DIRSEP, REPO_ID(pTsdb), fid);
  pSmaH->dFile.path = strdup(tSmaFile);
  pSmaH->smaFsIter.iter = 0;
  pSmaH->smaFsIter.fid = fid;
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Set and open tSma file if it has key locates in queryWin.
 *
 * @param pReadH
 * @param param
 * @param queryWin
 * @return true
 * @return false
 */
static bool tsdbSetAndOpenTSmaFile(STSmaReadH *pReadH, TSKEY *queryKey) {
  SArray *smaFs = pReadH->pTsdb->fs->cstatus->sf;
  int32_t nSmaFs = taosArrayGetSize(smaFs);

  tsdbCloseDBF(&pReadH->dFile);

#if 0
  while (pReadH->smaFsIter.iter < nSmaFs) {
    void *pSmaFile = taosArrayGet(smaFs, pReadH->smaFsIter.iter);
    if (pSmaFile) {  // match(indexName, queryWindow)
      // TODO: select the file by index_name ...
      pReadH->dFile = pSmaFile;
      ++pReadH->smaFsIter.iter;
      break;
    }
    ++pReadH->smaFsIter.iter;
  }

  if (pReadH->pDFile) {
    tsdbDebug("vg%d: smaFile %s matched", REPO_ID(pReadH->pTsdb), "[pSmaFile dir]");
    return true;
  }
#endif

  return false;
}

/**
 * @brief
 *
 * @param pTsdb Return the data between queryWin and fill the pData.
 * @param pData
 * @param indexUid
 * @param pQuerySKey
 * @param nMaxResult The query invoker should control the nMaxResult need to return to avoid OOM.
 * @return int32_t
 */
static int32_t tsdbGetTSmaDataImpl(STsdb *pTsdb, char *pData, int64_t indexUid, TSKEY querySKey, int32_t nMaxResult) {
  SSmaEnv  *pEnv = atomic_load_ptr(&REPO_TSMA_ENV(pTsdb));
  SSmaStat *pStat = NULL;

  if (!pEnv) {
    terrno = TSDB_CODE_INVALID_PTR;
    tsdbWarn("vgId:%d getTSmaDataImpl failed since pTSmaEnv is NULL", REPO_ID(pTsdb));
    return TSDB_CODE_FAILED;
  }

  pStat = SMA_ENV_STAT(pEnv);

  tsdbRefSmaStat(pTsdb, pStat);
  SSmaStatItem *pItem = taosHashGet(SMA_ENV_STAT_ITEMS(pEnv), &indexUid, sizeof(indexUid));
  if (!pItem || !(pItem = *(SSmaStatItem **)pItem)) {
    // Normally pItem should not be NULL, mark all windows as expired and notify query module to fetch raw TS data if
    // it's NULL.
    tsdbUnRefSmaStat(pTsdb, pStat);
    terrno = TSDB_CODE_TDB_INVALID_ACTION;
    tsdbDebug("vgId:%d getTSmaDataImpl failed since no index %" PRIi64, REPO_ID(pTsdb), indexUid);
    return TSDB_CODE_FAILED;
  }

#if 0
  int32_t nQueryWin = taosArrayGetSize(pQuerySKey);
  for (int32_t n = 0; n < nQueryWin; ++n) {
    TSKEY skey = taosArrayGet(pQuerySKey, n);
    if (taosHashGet(pItem->expiredWindows, &skey, sizeof(TSKEY))) {
      // TODO: mark this window as expired.
    }
  }
#endif

#if 1
  int8_t smaStat = 0;
  if (!tsdbSmaStatIsOK(pItem, &smaStat)) {  // TODO: multiple check for large scale sma query
    tsdbUnRefSmaStat(pTsdb, pStat);
    terrno = TSDB_CODE_TDB_INVALID_SMA_STAT;
    tsdbWarn("vgId:%d getTSmaDataImpl failed from index %" PRIi64 " since %s %" PRIi8, REPO_ID(pTsdb), indexUid,
             tstrerror(terrno), smaStat);
    return TSDB_CODE_FAILED;
  }

  if (taosHashGet(pItem->expiredWindows, &querySKey, sizeof(TSKEY))) {
    // TODO: mark this window as expired.
    tsdbDebug("vgId:%d skey %" PRIi64 " of window exists in expired window for index %" PRIi64, REPO_ID(pTsdb),
              querySKey, indexUid);
  } else {
    tsdbDebug("vgId:%d skey %" PRIi64 " of window not in expired window for index %" PRIi64, REPO_ID(pTsdb), querySKey,
              indexUid);
  }

  STSma *pTSma = pItem->pSma;
#endif

  STSmaReadH tReadH = {0};
  tsdbInitTSmaReadH(&tReadH, pTsdb, pTSma->interval, pTSma->intervalUnit);
  tsdbCloseDBF(&tReadH.dFile);

  tsdbUnRefSmaStat(pTsdb, pStat);

  tsdbInitTSmaFile(&tReadH, indexUid, querySKey);
  if (tsdbOpenDBF(pEnv->dbEnv, &tReadH.dFile) != 0) {
    tsdbWarn("vgId:%d open DBF %s failed since %s", REPO_ID(pTsdb), tReadH.dFile.path, tstrerror(terrno));
    return TSDB_CODE_FAILED;
  }

  char    smaKey[SMA_KEY_LEN] = {0};
  void   *pSmaKey = &smaKey;
  int64_t queryGroupId = 1;
  tsdbEncodeTSmaKey(queryGroupId, querySKey, (void **)&pSmaKey);

  tsdbDebug("vgId:%d get sma data from %s: smaKey %" PRIx64 "-%" PRIx64 ", keyLen %d", REPO_ID(pTsdb),
            tReadH.dFile.path, *(int64_t *)smaKey, *(int64_t *)POINTER_SHIFT(smaKey, 8), SMA_KEY_LEN);

  void   *result = NULL;
  int32_t valueSize = 0;
  if (!(result = tsdbGetSmaDataByKey(&tReadH.dFile, smaKey, SMA_KEY_LEN, &valueSize))) {
    tsdbWarn("vgId:%d get sma data failed from smaIndex %" PRIi64 ", smaKey %" PRIx64 "-%" PRIx64 " since %s",
             REPO_ID(pTsdb), indexUid, *(int64_t *)smaKey, *(int64_t *)POINTER_SHIFT(smaKey, 8), tstrerror(terrno));
    tsdbCloseDBF(&tReadH.dFile);
    return TSDB_CODE_FAILED;
  }

#ifdef _TEST_SMA_PRINT_DEBUG_LOG_
  for (uint32_t v = 0; v < valueSize; v += 8) {
    tsdbWarn("vgId:%d get sma data v[%d]=%" PRIi64, REPO_ID(pTsdb), v, *(int64_t *)POINTER_SHIFT(result, v));
  }
#endif
  taosMemoryFreeClear(result);  // TODO: fill the result to output

#if 0
  int32_t nResult = 0;
  int64_t lastKey = 0;

  while (true) {
    if (nResult >= nMaxResult) {
      break;
    }

    // set and open the file according to the STSma param
    if (tsdbSetAndOpenTSmaFile(&tReadH, queryWin)) {
      char bTree[100] = "\0";
      while (strncmp(bTree, "has more nodes", 100) == 0) {
        if (nResult >= nMaxResult) {
          break;
        }
        // tsdbGetDataFromBTree(bTree, queryWin, lastKey)
        // fill the pData
        ++nResult;
      }
    }
  }
#endif
  // read data from file and fill the result
  tsdbCloseDBF(&tReadH.dFile);
  return TSDB_CODE_SUCCESS;
}

int32_t tsdbCreateTSma(STsdb *pTsdb, char *pMsg) {
  SSmaCfg vCreateSmaReq = {0};
  if (!tDeserializeSVCreateTSmaReq(pMsg, &vCreateSmaReq)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tsdbWarn("vgId:%d tsma create msg received but deserialize failed since %s", REPO_ID(pTsdb), terrstr(terrno));
    return -1;
  }

  tsdbDebug("vgId:%d tsma create msg %s:%" PRIi64 " for table %" PRIi64 " received", REPO_ID(pTsdb),
            vCreateSmaReq.tSma.indexName, vCreateSmaReq.tSma.indexUid, vCreateSmaReq.tSma.tableUid);

  // record current timezone of server side
  vCreateSmaReq.tSma.timezoneInt = tsTimezone;

  if (metaCreateTSma(REPO_META(pTsdb), &vCreateSmaReq) < 0) {
    // TODO: handle error
    tsdbWarn("vgId:%d tsma %s:%" PRIi64 " create failed for table %" PRIi64 " since %s", REPO_ID(pTsdb),
             vCreateSmaReq.tSma.indexName, vCreateSmaReq.tSma.indexUid, vCreateSmaReq.tSma.tableUid, terrstr(terrno));
    tdDestroyTSma(&vCreateSmaReq.tSma);
    return -1;
  }

  tsdbTSmaAdd(pTsdb, 1);

  tdDestroyTSma(&vCreateSmaReq.tSma);
  // TODO: return directly or go on follow steps?
  return TSDB_CODE_SUCCESS;
}

int32_t tsdbDropTSma(STsdb *pTsdb, char *pMsg) {
  SVDropTSmaReq vDropSmaReq = {0};
  if (!tDeserializeSVDropTSmaReq(pMsg, &vDropSmaReq)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  // TODO: send msg to stream computing to drop tSma
  // if ((send msg to stream computing) < 0) {
  //   tdDestroyTSma(&vCreateSmaReq);
  //   return -1;
  // }
  //

  if (metaDropTSma(REPO_META(pTsdb), vDropSmaReq.indexUid) < 0) {
    // TODO: handle error
    return -1;
  }

  if (tsdbDropTSmaData(pTsdb, vDropSmaReq.indexUid) < 0) {
    // TODO: handle error
    return -1;
  }

  tsdbTSmaSub(pTsdb, 1);

  // TODO: return directly or go on follow steps?
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Check and init qTaskInfo_t, only applicable to stable with SRSmaParam.
 *
 * @param pTsdb
 * @param pMeta
 * @param pReq
 * @return int32_t
 */
int32_t tsdbRegisterRSma(STsdb *pTsdb, SMeta *pMeta, SVCreateStbReq *pReq, SMsgCb *pMsgCb) {
  if (!pReq->rollup) {
    tsdbDebug("vgId:%d return directly since no rollup for stable %s %" PRIi64, REPO_ID(pTsdb), pReq->name, pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  SRSmaParam *param = &pReq->pRSmaParam;

  if ((param->qmsg1Len == 0) && (param->qmsg2Len == 0)) {
    tsdbWarn("vgId:%d no qmsg1/qmsg2 for rollup stable %s %" PRIi64, REPO_ID(pTsdb), pReq->name, pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  if (tsdbCheckAndInitSmaEnv(pTsdb, TSDB_SMA_TYPE_ROLLUP) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TDB_INIT_FAILED;
    return TSDB_CODE_FAILED;
  }

  SSmaEnv   *pEnv = REPO_RSMA_ENV(pTsdb);
  SSmaStat  *pStat = SMA_ENV_STAT(pEnv);
  SRSmaInfo *pRSmaInfo = NULL;

  pRSmaInfo = taosHashGet(SMA_STAT_INFO_HASH(pStat), &pReq->suid, sizeof(tb_uid_t));
  if (pRSmaInfo) {
    tsdbWarn("vgId:%d rsma info already exists for stb: %s, %" PRIi64, REPO_ID(pTsdb), pReq->name, pReq->suid);
    return TSDB_CODE_SUCCESS;
  }

  pRSmaInfo = (SRSmaInfo *)taosMemoryCalloc(1, sizeof(SRSmaInfo));
  if (!pRSmaInfo) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  STqReadHandle *pReadHandle = tqInitSubmitMsgScanner(pMeta);
  if (!pReadHandle) {
    taosMemoryFree(pRSmaInfo);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }

  SReadHandle handle = {
      .reader = pReadHandle,
      .meta = pMeta,
      .pMsgCb = pMsgCb,
  };

  if (param->qmsg1) {
    pRSmaInfo->taskInfo[0] = qCreateStreamExecTaskInfo(param->qmsg1, &handle);
    if (!pRSmaInfo->taskInfo[0]) {
      taosMemoryFree(pRSmaInfo);
      taosMemoryFree(pReadHandle);
      return TSDB_CODE_FAILED;
    }
  }

  if (param->qmsg2) {
    pRSmaInfo->taskInfo[1] = qCreateStreamExecTaskInfo(param->qmsg2, &handle);
    if (!pRSmaInfo->taskInfo[1]) {
      taosMemoryFree(pRSmaInfo);
      taosMemoryFree(pReadHandle);
      return TSDB_CODE_FAILED;
    }
  }

  if (taosHashPut(SMA_STAT_INFO_HASH(pStat), &pReq->suid, sizeof(tb_uid_t), &pRSmaInfo, sizeof(pRSmaInfo)) !=
      TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  } else {
    tsdbDebug("vgId:%d register rsma info succeed for suid:%" PRIi64, REPO_ID(pTsdb), pReq->suid);
  }

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief store suid/[uids], prefer to use array and then hash
 *
 * @param pStore
 * @param suid
 * @param uid
 * @return int32_t
 */
static int32_t tsdbUidStorePut(STbUidStore *pStore, tb_uid_t suid, tb_uid_t *uid) {
  // prefer to store suid/uids in array
  if ((suid == pStore->suid) || (pStore->suid == 0)) {
    if (pStore->suid == 0) {
      pStore->suid = suid;
    }
    if (uid) {
      if (!pStore->tbUids) {
        if (!(pStore->tbUids = taosArrayInit(1, sizeof(tb_uid_t)))) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return TSDB_CODE_FAILED;
        }
      }
      if (!taosArrayPush(pStore->tbUids, uid)) {
        return TSDB_CODE_FAILED;
      }
    }
  } else {
    // store other suid/uids in hash when multiple stable/table included in 1 batch of request
    if (!pStore->uidHash) {
      pStore->uidHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
      if (!pStore->uidHash) {
        return TSDB_CODE_FAILED;
      }
    }
    if (uid) {
      SArray *uidArray = taosHashGet(pStore->uidHash, &suid, sizeof(tb_uid_t));
      if (uidArray && ((uidArray = *(SArray **)uidArray))) {
        taosArrayPush(uidArray, uid);
      } else {
        SArray *pUidArray = taosArrayInit(1, sizeof(tb_uid_t));
        if (!pUidArray) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return TSDB_CODE_FAILED;
        }
        if (!taosArrayPush(pUidArray, uid)) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return TSDB_CODE_FAILED;
        }
        if (taosHashPut(pStore->uidHash, &suid, sizeof(suid), &pUidArray, sizeof(pUidArray)) != 0) {
          return TSDB_CODE_FAILED;
        }
      }
    } else {
      if (taosHashPut(pStore->uidHash, &suid, sizeof(suid), NULL, 0) != 0) {
        return TSDB_CODE_FAILED;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

void tsdbUidStoreDestory(STbUidStore *pStore) {
  if (pStore) {
    if (pStore->uidHash) {
      if (pStore->tbUids) {
        // When pStore->tbUids not NULL, the pStore->uidHash has k/v; otherwise pStore->uidHash only has keys.
        void *pIter = taosHashIterate(pStore->uidHash, NULL);
        while (pIter) {
          SArray *arr = *(SArray **)pIter;
          taosArrayDestroy(arr);
          pIter = taosHashIterate(pStore->uidHash, pIter);
        }
      }
      taosHashCleanup(pStore->uidHash);
    }
    taosArrayDestroy(pStore->tbUids);
  }
}

void *tsdbUidStoreFree(STbUidStore *pStore) {
  if (pStore) {
    tsdbUidStoreDestory(pStore);
    taosMemoryFree(pStore);
  }
  return NULL;
}

/**
 * @brief fetch suid/uids when create child tables of rollup SMA
 *
 * @param pTsdb
 * @param ppStore
 * @param suid
 * @param uid
 * @return int32_t
 */
int32_t tsdbFetchTbUidList(STsdb *pTsdb, STbUidStore **ppStore, tb_uid_t suid, tb_uid_t uid) {
  SSmaEnv *pEnv = REPO_RSMA_ENV((STsdb *)pTsdb);

  // only applicable to rollup SMA ctables
  if (!pEnv) {
    return TSDB_CODE_SUCCESS;
  }

  SSmaStat *pStat = SMA_ENV_STAT(pEnv);
  SHashObj *infoHash = NULL;
  if (!pStat || !(infoHash = SMA_STAT_INFO_HASH(pStat))) {
    terrno = TSDB_CODE_TDB_INVALID_SMA_STAT;
    return TSDB_CODE_FAILED;
  }

  // info cached when create rsma stable and return directly for non-rsma ctables
  if (!taosHashGet(infoHash, &suid, sizeof(tb_uid_t))) {
    return TSDB_CODE_SUCCESS;
  }

  ASSERT(ppStore != NULL);

  if (!(*ppStore)) {
    if (tsdbUidStoreInit(ppStore) != 0) {
      return TSDB_CODE_FAILED;
    }
  }

  if (tsdbUidStorePut(*ppStore, suid, &uid) != 0) {
    *ppStore = tsdbUidStoreFree(*ppStore);
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t tsdbUpdateTbUidListImpl(STsdb *pTsdb, tb_uid_t *suid, SArray *tbUids) {
  SSmaEnv   *pEnv = REPO_RSMA_ENV(pTsdb);
  SSmaStat  *pStat = SMA_ENV_STAT(pEnv);
  SRSmaInfo *pRSmaInfo = NULL;

  if (!suid || !tbUids) {
    terrno = TSDB_CODE_INVALID_PTR;
    tsdbError("vgId:%d failed to get rsma info for uid:%" PRIi64 " since %s", REPO_ID(pTsdb), *suid, terrstr(terrno));
    return TSDB_CODE_FAILED;
  }

  pRSmaInfo = taosHashGet(SMA_STAT_INFO_HASH(pStat), suid, sizeof(tb_uid_t));
  if (!pRSmaInfo || !(pRSmaInfo = *(SRSmaInfo **)pRSmaInfo)) {
    tsdbError("vgId:%d failed to get rsma info for uid:%" PRIi64, REPO_ID(pTsdb), *suid);
    terrno = TSDB_CODE_TDB_INVALID_SMA_STAT;
    return TSDB_CODE_FAILED;
  }

  if (pRSmaInfo->taskInfo[0] && (qUpdateQualifiedTableId(pRSmaInfo->taskInfo[0], tbUids, true) != 0)) {
    tsdbError("vgId:%d update tbUidList failed for uid:%" PRIi64 " since %s", REPO_ID(pTsdb), *suid, terrstr(terrno));
    return TSDB_CODE_FAILED;
  } else {
    tsdbDebug("vgId:%d update tbUidList succeed for qTaskInfo:%p with suid:%" PRIi64 ", uid:%" PRIi64, REPO_ID(pTsdb),
              pRSmaInfo->taskInfo[0], *suid, *(int64_t *)taosArrayGet(tbUids, 0));
  }

  if (pRSmaInfo->taskInfo[1] && (qUpdateQualifiedTableId(pRSmaInfo->taskInfo[1], tbUids, true) != 0)) {
    tsdbError("vgId:%d update tbUidList failed for uid:%" PRIi64 " since %s", REPO_ID(pTsdb), *suid, terrstr(terrno));
    return TSDB_CODE_FAILED;
  } else {
    tsdbDebug("vgId:%d update tbUidList succeed for qTaskInfo:%p with suid:%" PRIi64 ", uid:%" PRIi64, REPO_ID(pTsdb),
              pRSmaInfo->taskInfo[1], *suid, *(int64_t *)taosArrayGet(tbUids, 0));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tsdbUpdateTbUidList(STsdb *pTsdb, STbUidStore *pStore) {
  if (!pStore || (taosArrayGetSize(pStore->tbUids) == 0)) {
    return TSDB_CODE_SUCCESS;
  }

  if (tsdbUpdateTbUidListImpl(pTsdb, &pStore->suid, pStore->tbUids) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  void *pIter = taosHashIterate(pStore->uidHash, NULL);
  while (pIter) {
    tb_uid_t *pTbSuid = (tb_uid_t *)taosHashGetKey(pIter, NULL);
    SArray   *pTbUids = *(SArray **)pIter;

    if (tsdbUpdateTbUidListImpl(pTsdb, pTbSuid, pTbUids) != TSDB_CODE_SUCCESS) {
      taosHashCancelIterate(pStore->uidHash, pIter);
      return TSDB_CODE_FAILED;
    }

    pIter = taosHashIterate(pStore->uidHash, pIter);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tsdbProcessSubmitReq(STsdb *pTsdb, int64_t version, void *pReq) {
  if (!pReq) {
    terrno = TSDB_CODE_INVALID_PTR;
    return TSDB_CODE_FAILED;
  }

  SSubmitReq *pSubmitReq = (SSubmitReq *)pReq;

  if (tsdbInsertData(pTsdb, version, pSubmitReq, NULL) < 0) {
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tsdbFetchSubmitReqSuids(SSubmitReq *pMsg, STbUidStore *pStore) {
  ASSERT(pMsg != NULL);
  SSubmitMsgIter msgIter = {0};
  SSubmitBlk    *pBlock = NULL;
  SSubmitBlkIter blkIter = {0};
  STSRow        *row = NULL;

  terrno = TSDB_CODE_SUCCESS;

  if (tInitSubmitMsgIter(pMsg, &msgIter) < 0) return -1;
  while (true) {
    if (tGetSubmitMsgNext(&msgIter, &pBlock) < 0) return -1;

    if (!pBlock) break;
    tsdbUidStorePut(pStore, msgIter.suid, NULL);
    pStore->uid = msgIter.uid;  // TODO: remove, just for debugging
  }

  if (terrno != TSDB_CODE_SUCCESS) return -1;
  return 0;
}

static FORCE_INLINE int32_t tsdbExecuteRSmaImpl(STsdb *pTsdb, const void *pMsg, int32_t inputType,
                                                qTaskInfo_t *taskInfo, STSchema *pTSchema, tb_uid_t suid, tb_uid_t uid,
                                                int8_t level) {
  SArray *pResult = NULL;

  if (!taskInfo) {
    tsdbDebug("vgId:%d no qTaskInfo to execute rsma %" PRIi8 " task for suid:%" PRIu64, REPO_ID(pTsdb), level, suid);
    return TSDB_CODE_SUCCESS;
  }

  tsdbDebug("vgId:%d execute rsma %" PRIi8 " task for qTaskInfo:%p suid:%" PRIu64, REPO_ID(pTsdb), level, taskInfo,
            suid);

  qSetStreamInput(taskInfo, pMsg, inputType);
  while (1) {
    SSDataBlock *output = NULL;
    uint64_t     ts;
    if (qExecTask(taskInfo, &output, &ts) < 0) {
      ASSERT(false);
    }
    if (!output) {
      break;
    }
    if (!pResult) {
      pResult = taosArrayInit(0, sizeof(SSDataBlock));
      if (!pResult) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return TSDB_CODE_FAILED;
      }
    }

    taosArrayPush(pResult, output);
  }

  if (taosArrayGetSize(pResult) > 0) {
    blockDebugShowData(pResult);
    STsdb      *sinkTsdb = (level == TSDB_RETENTION_L1 ? pTsdb->pVnode->pRSma1 : pTsdb->pVnode->pRSma2);
    SSubmitReq *pReq = NULL;
    if (buildSubmitReqFromDataBlock(&pReq, pResult, pTSchema, TD_VID(pTsdb->pVnode), uid, suid) != 0) {
      taosArrayDestroy(pResult);
      return TSDB_CODE_FAILED;
    }
    if (tsdbProcessSubmitReq(sinkTsdb, INT64_MAX, pReq) != 0) {
      taosArrayDestroy(pResult);
      taosMemoryFreeClear(pReq);
      return TSDB_CODE_FAILED;
    }
    taosMemoryFreeClear(pReq);
  } else {
    tsdbWarn("vgId:%d no rsma % " PRIi8 " data generated since %s", REPO_ID(pTsdb), level, tstrerror(terrno));
  }

  taosArrayDestroy(pResult);

  return TSDB_CODE_SUCCESS;
}

static int32_t tsdbExecuteRSma(STsdb *pTsdb, const void *pMsg, int32_t inputType, tb_uid_t suid, tb_uid_t uid) {
  SSmaEnv *pEnv = REPO_RSMA_ENV(pTsdb);
  if (!pEnv) {
    // only applicable when rsma env exists
    return TSDB_CODE_SUCCESS;
  }

  ASSERT(uid != 0);  // TODO: remove later

  SSmaStat  *pStat = SMA_ENV_STAT(pEnv);
  SRSmaInfo *pRSmaInfo = NULL;

  pRSmaInfo = taosHashGet(SMA_STAT_INFO_HASH(pStat), &suid, sizeof(tb_uid_t));

  if (!pRSmaInfo || !(pRSmaInfo = *(SRSmaInfo **)pRSmaInfo)) {
    tsdbDebug("vgId:%d no rsma info for suid:%" PRIu64, REPO_ID(pTsdb), suid);
    return TSDB_CODE_SUCCESS;
  }
  if (!pRSmaInfo->taskInfo[0]) {
    tsdbDebug("vgId:%d no rsma qTaskInfo for suid:%" PRIu64, REPO_ID(pTsdb), suid);
    return TSDB_CODE_SUCCESS;
  }

  if (inputType == STREAM_DATA_TYPE_SUBMIT_BLOCK) {
    // TODO: use the proper schema instead of 0, and cache STSchema in cache
    STSchema *pTSchema = metaGetTbTSchema(pTsdb->pVnode->pMeta, suid, 1);
    if (!pTSchema) {
      terrno = TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION;
      return TSDB_CODE_FAILED;
    }
    tsdbExecuteRSmaImpl(pTsdb, pMsg, inputType, pRSmaInfo->taskInfo[0], pTSchema, suid, uid, TSDB_RETENTION_L1);
    tsdbExecuteRSmaImpl(pTsdb, pMsg, inputType, pRSmaInfo->taskInfo[1], pTSchema, suid, uid, TSDB_RETENTION_L2);
    taosMemoryFree(pTSchema);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tsdbTriggerRSma(STsdb *pTsdb, void *pMsg, int32_t inputType) {
  SSmaEnv *pEnv = REPO_RSMA_ENV(pTsdb);
  if (!pEnv) {
    // only applicable when rsma env exists
    return TSDB_CODE_SUCCESS;
  }

  if (inputType == STREAM_DATA_TYPE_SUBMIT_BLOCK) {
    STbUidStore uidStore = {0};
    tsdbFetchSubmitReqSuids(pMsg, &uidStore);

    if (uidStore.suid != 0) {
      tsdbExecuteRSma(pTsdb, pMsg, inputType, uidStore.suid, uidStore.uid);

      void *pIter = taosHashIterate(uidStore.uidHash, NULL);
      while (pIter) {
        tb_uid_t *pTbSuid = (tb_uid_t *)taosHashGetKey(pIter, NULL);
        tsdbExecuteRSma(pTsdb, pMsg, inputType, *pTbSuid, 0);
        pIter = taosHashIterate(uidStore.uidHash, pIter);
      }

      tsdbUidStoreDestory(&uidStore);
    }
  }
  return TSDB_CODE_SUCCESS;
}

#if 0
/**
 * @brief Get the start TS key of the last data block of one interval/sliding.
 *
 * @param pTsdb
 * @param param
 * @param result
 * @return int32_t
 *         1) Return 0 and fill the result if the check procedure is normal;
 *         2) Return -1 if error occurs during the check procedure.
 */
int32_t tsdbGetTSmaStatus(STsdb *pTsdb, void *smaIndex, void *result) {
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
 * @param pTsdb
 * @param param
 * @param pWin
 * @return int32_t
 */
int32_t tsdbRemoveTSmaData(STsdb *pTsdb, void *smaIndex, STimeWindow *pWin) {
  // for ("tSmaFiles of param-interval-sliding between pWin") {
  //   // remove the tSmaFile
  // }
  return TSDB_CODE_SUCCESS;
}
#endif

// TODO: Who is responsible for resource allocate and release?
int32_t tsdbInsertTSmaData(STsdb *pTsdb, int64_t indexUid, const char *msg) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tsdbInsertTSmaDataImpl(pTsdb, indexUid, msg)) < 0) {
    tsdbWarn("vgId:%d insert tSma data failed since %s", REPO_ID(pTsdb), tstrerror(terrno));
  }
  // TODO: destroy SSDataBlocks(msg)
  return code;
}

int32_t tsdbUpdateSmaWindow(STsdb *pTsdb, SSubmitReq *pMsg, int64_t version) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tsdbUpdateExpiredWindowImpl(pTsdb, pMsg, version)) < 0) {
    tsdbWarn("vgId:%d update expired sma window failed since %s", REPO_ID(pTsdb), tstrerror(terrno));
  }
  return code;
}

int32_t tsdbInsertRSmaData(STsdb *pTsdb, char *msg) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tsdbInsertRSmaDataImpl(pTsdb, msg)) < 0) {
    tsdbWarn("vgId:%d insert rSma data failed since %s", REPO_ID(pTsdb), tstrerror(terrno));
  }
  return code;
}

int32_t tsdbGetTSmaData(STsdb *pTsdb, char *pData, int64_t indexUid, TSKEY querySKey, int32_t nMaxResult) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tsdbGetTSmaDataImpl(pTsdb, pData, indexUid, querySKey, nMaxResult)) < 0) {
    tsdbWarn("vgId:%d get tSma data failed since %s", REPO_ID(pTsdb), tstrerror(terrno));
  }
  return code;
}

int32_t tsdbDropTSmaData(STsdb *pTsdb, int64_t indexUid) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tsdbDropTSmaDataImpl(pTsdb, indexUid)) < 0) {
    tsdbWarn("vgId:%d drop tSma data failed since %s", REPO_ID(pTsdb), tstrerror(terrno));
  }
  return code;
}