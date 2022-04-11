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

#include "vnodeInt.h"

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
typedef enum {
  SMA_STORAGE_LEVEL_TSDB = 0,     // use days of self-defined  e.g. vnode${N}/tsdb/tsma/sma_index_uid/v2f200.tsma
  SMA_STORAGE_LEVEL_DFILESET = 1  // use days of TS data       e.g. vnode${N}/tsdb/tsma/sma_index_uid/v2f1906.tsma
} ESmaStorageLevel;

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
   */
  int8_t    state;           // ETsdbSmaStat
  SHashObj *expiredWindows;  // key: skey of time window, value: N/A
  STSma    *pSma;            // cache schema
} SSmaStatItem;

struct SSmaStat {
  SHashObj *smaStatItems;  // key: indexUid, value: SSmaStatItem
  T_REF_DECLARE()
};

// declaration of static functions

// expired window
static int32_t  tsdbUpdateExpiredWindowImpl(STsdb *pTsdb, SSubmitReq *pMsg);
static int32_t  tsdbSetExpiredWindow(STsdb *pTsdb, SHashObj *pItemsHash, int64_t indexUid, int64_t winSKey);
static int32_t  tsdbInitSmaStat(SSmaStat **pSmaStat);
static void    *tsdbFreeSmaStatItem(SSmaStatItem *pSmaStatItem);
static int32_t  tsdbDestroySmaState(SSmaStat *pSmaStat);
static SSmaEnv *tsdbNewSmaEnv(const STsdb *pTsdb, const char *path, SDiskID did);
static int32_t  tsdbInitSmaEnv(STsdb *pTsdb, const char *path, SDiskID did, SSmaEnv **pEnv);
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
static int32_t tsdbInsertTSmaBlocks(STSmaWriteH *pSmaH, void *smaKey, uint32_t keyLen, void *pData, uint32_t dataLen);
static int64_t tsdbGetIntervalByPrecision(int64_t interval, uint8_t intervalUnit, int8_t precision, bool adjusted);
static int32_t tsdbGetTSmaDays(STsdb *pTsdb, int64_t interval, int32_t storageLevel);
static int32_t tsdbSetTSmaDataFile(STSmaWriteH *pSmaH, int64_t indexUid, int32_t fid);
static int32_t tsdbInitTSmaFile(STSmaReadH *pSmaH, int64_t indexUid, TSKEY skey);
static bool    tsdbSetAndOpenTSmaFile(STSmaReadH *pReadH, TSKEY *queryKey);
static void    tsdbGetSmaDir(int32_t vgId, ETsdbSmaType smaType, char dirName[]);
static int32_t tsdbInsertTSmaDataImpl(STsdb *pTsdb, int64_t indexUid, const char *msg);
static int32_t tsdbInsertRSmaDataImpl(STsdb *pTsdb, const char *msg);

// mgmt interface
static int32_t tsdbDropTSmaDataImpl(STsdb *pTsdb, int64_t indexUid);

// implementation
static FORCE_INLINE int16_t tsdbTSmaAdd(STsdb *pTsdb, int16_t n) { return atomic_add_fetch_16(&REPO_TSMA_NUM(pTsdb), n); }
static FORCE_INLINE int16_t tsdbTSmaSub(STsdb *pTsdb, int16_t n) { return atomic_sub_fetch_16(&REPO_TSMA_NUM(pTsdb), n); }

int32_t tsdbInitSma(STsdb *pTsdb) {
  // tSma
  int32_t numOfTSma = taosArrayGetSize(metaGetSmaTbUids(pTsdb->pMeta, false));
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
  snprintf(dirName, TSDB_FILENAME_LEN, "vnode%svnode%d%stsdb%s%s", TD_DIRSEP, vgId, TD_DIRSEP, TD_DIRSEP,
           TSDB_SMA_DNAME[smaType]);
}

static SSmaEnv *tsdbNewSmaEnv(const STsdb *pTsdb, const char *path, SDiskID did) {
  SSmaEnv *pEnv = NULL;

  pEnv = (SSmaEnv *)taosMemoryCalloc(1, sizeof(SSmaEnv));
  if (pEnv == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  int code = taosThreadRwlockInit(&(pEnv->lock), NULL);
  if (code) {
    terrno = TAOS_SYSTEM_ERROR(code);
    taosMemoryFree(pEnv);
    return NULL;
  }

  ASSERT(path && (strlen(path) > 0));
  pEnv->path = strdup(path);
  if (pEnv->path == NULL) {
    tsdbFreeSmaEnv(pEnv);
    return NULL;
  }

  pEnv->did = did;

  if (tsdbInitSmaStat(&pEnv->pStat) != TSDB_CODE_SUCCESS) {
    tsdbFreeSmaEnv(pEnv);
    return NULL;
  }

  char aname[TSDB_FILENAME_LEN] = {0};
  tfsAbsoluteName(pTsdb->pTfs, did, path, aname);
  if (tsdbOpenBDBEnv(&pEnv->dbEnv, aname) != TSDB_CODE_SUCCESS) {
    tsdbFreeSmaEnv(pEnv);
    return NULL;
  }

  return pEnv;
}

static int32_t tsdbInitSmaEnv(STsdb *pTsdb, const char *path, SDiskID did, SSmaEnv **pEnv) {
  if (!pEnv) {
    terrno = TSDB_CODE_INVALID_PTR;
    return TSDB_CODE_FAILED;
  }

  if (*pEnv == NULL) {
    if ((*pEnv = tsdbNewSmaEnv(pTsdb, path, did)) == NULL) {
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
    tsdbDestroySmaState(pSmaEnv->pStat);
    taosMemoryFreeClear(pSmaEnv->pStat);
    taosMemoryFreeClear(pSmaEnv->path);
    taosThreadRwlockDestroy(&(pSmaEnv->lock));
    tsdbCloseBDBEnv(pSmaEnv->dbEnv);
  }
}

void *tsdbFreeSmaEnv(SSmaEnv *pSmaEnv) {
  tsdbDestroySmaEnv(pSmaEnv);
  taosMemoryFreeClear(pSmaEnv);
  return NULL;
}

static int32_t tsdbRefSmaStat(STsdb *pTsdb, SSmaStat *pStat) {
  if (pStat == NULL) return 0;

  int ref = T_REF_INC(pStat);
  tsdbDebug("vgId:%d ref sma stat:%p, val:%d", REPO_ID(pTsdb), pStat, ref);
  return 0;
}

static int32_t tsdbUnRefSmaStat(STsdb *pTsdb, SSmaStat *pStat) {
  if (pStat == NULL) return 0;

  int ref = T_REF_DEC(pStat);
  tsdbDebug("vgId:%d unref sma stat:%p, val:%d", REPO_ID(pTsdb), pStat, ref);
  return 0;
}

static int32_t tsdbInitSmaStat(SSmaStat **pSmaStat) {
  ASSERT(pSmaStat != NULL);

  if (*pSmaStat != NULL) {  // no lock
    return TSDB_CODE_SUCCESS;
  }

  /**
   *  1. Lazy mode utilized when init SSmaStat to update expired window(or hungry mode when tsdbNew).
   *  2. Currently, there is mutex lock when init SSmaEnv, thus no need add lock on SSmaStat, and please add lock if
   * tsdbInitSmaStat invoked in other multithread environment later.
   */
  if (*pSmaStat == NULL) {
    *pSmaStat = (SSmaStat *)taosMemoryCalloc(1, sizeof(SSmaStat));
    if (*pSmaStat == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_FAILED;
    }

    (*pSmaStat)->smaStatItems =
        taosHashInit(SMA_STATE_HASH_SLOT, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);

    if ((*pSmaStat)->smaStatItems == NULL) {
      taosMemoryFreeClear(*pSmaStat);
      return TSDB_CODE_FAILED;
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
  if (pSmaStatItem != NULL) {
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
int32_t tsdbDestroySmaState(SSmaStat *pSmaStat) {
  if (pSmaStat) {
    // TODO: use taosHashSetFreeFp when taosHashSetFreeFp is ready.
    void *item = taosHashIterate(pSmaStat->smaStatItems, NULL);
    while (item != NULL) {
      SSmaStatItem *pItem = *(SSmaStatItem **)item;
      tsdbFreeSmaStatItem(pItem);
      item = taosHashIterate(pSmaStat->smaStatItems, item);
    }
    taosHashCleanup(pSmaStat->smaStatItems);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tsdbCheckAndInitSmaEnv(STsdb *pTsdb, int8_t smaType) {
  SSmaEnv *pEnv = NULL;

  // return if already init
  switch (smaType) {
    case TSDB_SMA_TYPE_TIME_RANGE:
      if ((pEnv = (SSmaEnv *)atomic_load_ptr(&REPO_TSMA_ENV(pTsdb))) != NULL) {
        return TSDB_CODE_SUCCESS;
      }
      break;
    case TSDB_SMA_TYPE_ROLLUP:
      if ((pEnv = (SSmaEnv *)atomic_load_ptr(&REPO_RSMA_ENV(pTsdb))) != NULL) {
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
  if (pEnv == NULL) {
    char rname[TSDB_FILENAME_LEN] = {0};

    SDiskID did = {0};
    tfsAllocDisk(pTsdb->pTfs, TFS_PRIMARY_LEVEL, &did);
    if (did.level < 0 || did.id < 0) {
      tsdbUnlockRepo(pTsdb);
      return TSDB_CODE_FAILED;
    }
    tsdbGetSmaDir(REPO_ID(pTsdb), smaType, rname);

    if (tfsMkdirRecurAt(pTsdb->pTfs, rname, did) != TSDB_CODE_SUCCESS) {
      tsdbUnlockRepo(pTsdb);
      return TSDB_CODE_FAILED;
    }

    if (tsdbInitSmaEnv(pTsdb, rname, did, &pEnv) != TSDB_CODE_SUCCESS) {
      tsdbUnlockRepo(pTsdb);
      return TSDB_CODE_FAILED;
    }

    (smaType == TSDB_SMA_TYPE_TIME_RANGE) ? atomic_store_ptr(&REPO_TSMA_ENV(pTsdb), pEnv)
                                          : atomic_store_ptr(&REPO_RSMA_ENV(pTsdb), pEnv);
  }
  tsdbUnlockRepo(pTsdb);

  return TSDB_CODE_SUCCESS;
};

static int32_t tsdbSetExpiredWindow(STsdb *pTsdb, SHashObj *pItemsHash, int64_t indexUid, int64_t winSKey) {
  SSmaStatItem *pItem = taosHashGet(pItemsHash, &indexUid, sizeof(indexUid));
  if (pItem == NULL) {
    // TODO: use TSDB_SMA_STAT_EXPIRED and update by stream computing later
    pItem = tsdbNewSmaStatItem(TSDB_SMA_STAT_OK);  // TODO use the real state
    if (pItem == NULL) {
      // Response to stream computing: OOM
      // For query, if the indexUid not found, the TSDB should tell query module to query raw TS data.
      return TSDB_CODE_FAILED;
    }

    // cache smaMeta
    STSma *pSma = metaGetSmaInfoByIndex(pTsdb->pMeta, indexUid);
    if (pSma == NULL) {
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
  } else if ((pItem = *(SSmaStatItem **)pItem) == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return TSDB_CODE_FAILED;
  }

  int8_t state = TSDB_SMA_STAT_EXPIRED;
  if (taosHashPut(pItem->expiredWindows, &winSKey, sizeof(TSKEY), &state, sizeof(state)) != 0) {
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
int32_t tsdbUpdateExpiredWindowImpl(STsdb *pTsdb, SSubmitReq *pMsg) {
  if (atomic_load_16(&REPO_TSMA_NUM(pTsdb)) <= 0) {
    tsdbTrace("vgId:%d not update expire window since no tSma", REPO_ID(pTsdb));
    return TSDB_CODE_SUCCESS;
  }

  if (!pTsdb->pMeta) {
    terrno = TSDB_CODE_INVALID_PTR;
    return TSDB_CODE_FAILED;
  }

  if (tdScanAndConvertSubmitMsg(pMsg) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

// TODO: decode the msg from Stream Computing module => start
#ifdef TSDB_SMA_TESTx
  int64_t       indexUid = SMA_TEST_INDEX_UID;
  const int32_t SMA_TEST_EXPIRED_WINDOW_SIZE = 10;
  TSKEY         expiredWindows[SMA_TEST_EXPIRED_WINDOW_SIZE];
  TSKEY         skey1 = 1646987196 * 1e3;
  for (int32_t i = 0; i < SMA_TEST_EXPIRED_WINDOW_SIZE; ++i) {
    expiredWindows[i] = skey1 + i;
  }
#else

#endif
  // TODO: decode the msg <= end

  if (tsdbCheckAndInitSmaEnv(pTsdb, TSDB_SMA_TYPE_TIME_RANGE) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TDB_INIT_FAILED;
    return TSDB_CODE_FAILED;
  }

#ifndef TSDB_SMA_TEST
  TSKEY expiredWindows[SMA_TEST_EXPIRED_WINDOW_SIZE];
#endif

  // Firstly, assume that tSma can only be created on super table/normal table.
  // getActiveTimeWindow

  SSmaEnv  *pEnv = REPO_TSMA_ENV(pTsdb);
  SSmaStat *pStat = SMA_ENV_STAT(pEnv);
  SHashObj *pItemsHash = SMA_ENV_STAT_ITEMS(pEnv);

  TASSERT(pEnv != NULL && pStat != NULL && pItemsHash != NULL);

  // basic procedure
  // TODO: optimization
  tsdbRefSmaStat(pTsdb, pStat);

  SSubmitMsgIter msgIter = {0};
  SSubmitBlk    *pBlock = NULL;
  SInterval      interval = {0};

  if (tInitSubmitMsgIter(pMsg, &msgIter) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  while (true) {
    tGetSubmitMsgNext(&msgIter, &pBlock);
    if (pBlock == NULL) break;

    STSmaWrapper *pSW = NULL;
    STSma        *pTSma = NULL;

    SSubmitBlkIter blkIter = {0};
    if (tInitSubmitBlkIter(pBlock, &blkIter) != TSDB_CODE_SUCCESS) {
      tdFreeTSmaWrapper(pSW);
      break;
    }

    while (true) {
      STSRow *row = tGetSubmitBlkNext(&blkIter);
      if (row == NULL) {
        tdFreeTSmaWrapper(pSW);
        break;
      }
      if (pSW == NULL) {
        if ((pSW = metaGetSmaInfoByTable(REPO_META(pTsdb), pBlock->suid)) == NULL) {
          break;
        }
        if ((pSW->number) <= 0 || (pSW->tSma == NULL)) {
          tdFreeTSmaWrapper(pSW);
          break;
        }
        pTSma = pSW->tSma;
      }

      interval.interval = pTSma->interval;
      interval.intervalUnit = pTSma->intervalUnit;
      interval.offset = pTSma->offset;
      interval.precision = REPO_CFG(pTsdb)->precision;
      interval.sliding = pTSma->sliding;
      interval.slidingUnit = pTSma->slidingUnit;

      TSKEY winSKey = taosTimeTruncate(TD_ROW_KEY(row), &interval, interval.precision);

      tsdbSetExpiredWindow(pTsdb, pItemsHash, pTSma->indexUid, winSKey);
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

  if (pStat && pStat->smaStatItems) {
    pItem = taosHashGet(pStat->smaStatItems, &indexUid, sizeof(indexUid));
  }
  if ((pItem != NULL) && ((pItem = *(SSmaStatItem **)pItem) != NULL)) {
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
static int32_t tsdbInsertTSmaBlocks(STSmaWriteH *pSmaH, void *smaKey, uint32_t keyLen, void *pData, uint32_t dataLen) {
  SDBFile *pDBFile = &pSmaH->dFile;
  // TODO: insert sma data blocks into B+Tree(TDB)
  if (tsdbSaveSmaToDB(pDBFile, smaKey, keyLen, pData, dataLen) != 0) {
    tsdbWarn("vgId:%d insert sma data blocks into %s: smaKey %" PRIx64 "-%" PRIx64 ", dataLen %" PRIu32 " fail",
             REPO_ID(pSmaH->pTsdb), pDBFile->path, *(int64_t *)smaKey, *(int64_t *)POINTER_SHIFT(smaKey, 8), dataLen);
    return TSDB_CODE_FAILED;
  }
  tsdbDebug("vgId:%d insert sma data blocks into %s: smaKey %" PRIx64 "-%" PRIx64 ", dataLen %" PRIu32 " succeed",
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
  ASSERT(pSmaH->dFile.path == NULL && pSmaH->dFile.pDB == NULL);

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
  STsdbCfg *pCfg = REPO_CFG(pTsdb);
  int32_t   daysPerFile = pCfg->daysPerFile;

  if (storageLevel == SMA_STORAGE_LEVEL_TSDB) {
    int32_t days = SMA_STORAGE_TSDB_TIMES * (interval / tsTickPerDay[pCfg->precision]);
    daysPerFile = days > SMA_STORAGE_TSDB_DAYS ? days : SMA_STORAGE_TSDB_DAYS;
  }

  return daysPerFile;
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
  SSmaEnv      *pEnv = atomic_load_ptr(&REPO_TSMA_ENV(pTsdb));

  if (pEnv == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    tsdbWarn("vgId:%d insert tSma data failed since pTSmaEnv is NULL", REPO_ID(pTsdb));
    return terrno;
  }

  if (pDataBlocks == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    tsdbWarn("vgId:%d insert tSma data failed since pDataBlocks is NULL", REPO_ID(pTsdb));
    return terrno;
  }

  if (taosArrayGetSize(pDataBlocks) <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    tsdbWarn("vgId:%d insert tSma data failed since pDataBlocks is empty", REPO_ID(pTsdb));
    return TSDB_CODE_FAILED;
  }

  SSmaStat     *pStat = SMA_ENV_STAT(pEnv);
  SSmaStatItem *pItem = NULL;

  tsdbRefSmaStat(pTsdb, pStat);

  if (pStat && pStat->smaStatItems) {
    pItem = taosHashGet(pStat->smaStatItems, &indexUid, sizeof(indexUid));
  }

  if ((pItem == NULL) || ((pItem = *(SSmaStatItem **)pItem) == NULL) || tsdbSmaStatIsDropped(pItem)) {
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

  // key: skey + groupId
  char    smaKey[SMA_KEY_LEN] = {0};
  char    dataBuf[512] = {0};
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
      TSKEY skey = 1649295200000;  // TSKEY_INITIAL_VAL;  // the start key of TS window by interval
      void *pSmaKey = &smaKey;
      bool  isStartKey = false;
      {
        // just for debugging
        isStartKey = true;
        tsdbEncodeTSmaKey(groupId, skey, &pSmaKey);
      }
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
              printf("==> skey = %" PRIi64 " groupId = %" PRIi64 "|", skey, groupId);
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
        }

        if (tsdbInsertTSmaBlocks(&tSmaH, &smaKey, SMA_KEY_LEN, dataBuf, tlen) != 0) {
          tsdbWarn("vgId:%d insert tSma data blocks fail for index %" PRIi64 ", skey %" PRIi64 ", groupId %" PRIi64
                   " since %s",
                   REPO_ID(pTsdb), indexUid, skey, groupId, tstrerror(terrno));
          tsdbDestroyTSmaWriteH(&tSmaH);
          tsdbUnRefSmaStat(pTsdb, pStat);
          return TSDB_CODE_FAILED;
        }
        tsdbDebug("vgId:%d insert tSma data blocks success for index %" PRIi64 ", skey %" PRIi64 ", groupId %" PRIi64,
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
    if ((pItem != NULL) || ((pItem = *(SSmaStatItem **)pItem) != NULL)) {
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

  if (pEnv == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    tsdbWarn("vgId:%d insert rSma data failed since pTSmaEnv is NULL", REPO_ID(pTsdb));
    return terrno;
  }

  if (pEnv == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    tsdbWarn("vgId:%d insert rSma data failed since pTSmaEnv is NULL", REPO_ID(pTsdb));
    return terrno;
  }

  if (pDataBlocks == NULL) {
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

  if (pStat && pStat->smaStatItems) {
    pItem = taosHashGet(pStat->smaStatItems, &indexUid, sizeof(indexUid));
  }

  if ((pItem == NULL) || ((pItem = *(SSmaStatItem **)pItem) == NULL) || tsdbSmaStatIsDropped(pItem)) {
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

  if (pReadH->pDFile != NULL) {
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
  if ((pItem == NULL) || ((pItem = *(SSmaStatItem **)pItem) == NULL)) {
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
    if (taosHashGet(pItem->expiredWindows, &skey, sizeof(TSKEY)) != NULL) {
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

  if (taosHashGet(pItem->expiredWindows, &querySKey, sizeof(TSKEY)) != NULL) {
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

  void    *result = NULL;
  uint32_t valueSize = 0;
  if ((result = tsdbGetSmaDataByKey(&tReadH.dFile, smaKey, SMA_KEY_LEN, &valueSize)) == NULL) {
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
  if (tDeserializeSVCreateTSmaReq(pMsg, &vCreateSmaReq) == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tsdbWarn("vgId:%d TDMT_VND_CREATE_SMA received but deserialize failed since %s", REPO_ID(pTsdb), terrstr(terrno));
    return -1;
  }
  tsdbDebug("vgId:%d TDMT_VND_CREATE_SMA msg received for %s:%" PRIi64, REPO_ID(pTsdb), vCreateSmaReq.tSma.indexName,
         vCreateSmaReq.tSma.indexUid);

  // record current timezone of server side
  vCreateSmaReq.tSma.timezoneInt = tsTimezone;

  if (metaCreateTSma(pTsdb->pMeta, &vCreateSmaReq) < 0) {
    // TODO: handle error
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
  if (tDeserializeSVDropTSmaReq(pMsg, &vDropSmaReq) == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  // TODO: send msg to stream computing to drop tSma
  // if ((send msg to stream computing) < 0) {
  //   tdDestroyTSma(&vCreateSmaReq);
  //   return -1;
  // }
  //

  if (metaDropTSma(pTsdb->pMeta, vDropSmaReq.indexUid) < 0) {
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
  return code;
}

int32_t tsdbUpdateSmaWindow(STsdb *pTsdb, SSubmitReq *pMsg) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tsdbUpdateExpiredWindowImpl(pTsdb, pMsg)) < 0) {
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