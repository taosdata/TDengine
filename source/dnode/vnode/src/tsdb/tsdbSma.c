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

#include "tsdbDef.h"

#define SMA_STORAGE_TSDB_DAYS   30
#define SMA_STORAGE_SPLIT_HOURS 24
#define SMA_KEY_LEN             18  // tableUid_colId_TSKEY 8+2+8

#define SMA_STATE_HASH_SLOT      4
#define SMA_STATE_ITEM_HASH_SLOT 32

#define SMA_TEST_INDEX_NAME "smaTestIndexName"  // TODO: just for test
#define SMA_TEST_INDEX_UID  123456              // TODO: just for test
typedef enum {
  SMA_STORAGE_LEVEL_TSDB = 0,     // use days of self-defined  e.g. vnode${N}/tsdb/tsma/sma_index_uid/v2t200.dat
  SMA_STORAGE_LEVEL_DFILESET = 1  // use days of TS data       e.g. vnode${N}/tsdb/rsma/sma_index_uid/v2r200.dat
} ESmaStorageLevel;

typedef struct {
  STsdb * pTsdb;
  char *  pDFile;     // TODO: use the real DFile type, not char*
  int32_t interval;   // interval with the precision of DB
  // TODO
} STSmaWriteH;

typedef struct {
  int32_t iter;
} SmaFsIter;
typedef struct {
  STsdb *   pTsdb;
  char *    pDFile;     // TODO: use the real DFile type, not char*
  int32_t   interval;   // interval with the precision of DB
  int32_t   blockSize;  // size of SMA block item
  int8_t    storageLevel;
  int8_t    days;
  SmaFsIter smaFsIter;
  // TODO
} STSmaReadH;

typedef struct {
  /**
   * @brief The field 'state' is here to demonstrate if one smaIndex is ready to provide service.
   *    - TSDB_SMA_STAT_EXPIRED: 1) If sma calculation of history TS data is not finished; 2) Or if the TSDB is open,
   * without information about its previous state.
   *    - TSDB_SMA_STAT_OK: 1) The sma calculation of history data is finished; 2) Or recevied information from
   * Streaming Module or TSDB local persistence.
   */
  int8_t    state;           // ETsdbSmaStat
  SHashObj *expiredWindows;  // key: skey of time window, value: N/A
  STSma *   pSma;
} SSmaStatItem;

struct SSmaStat {
  SHashObj *smaStatItems;  // key: indexName, value: SSmaStatItem
};

// declaration of static functions
static int32_t tsdbInitTSmaWriteH(STSmaWriteH *pSmaH, STsdb *pTsdb, STSmaDataWrapper *pData);
static int32_t tsdbInitTSmaReadH(STSmaReadH *pSmaH, STsdb *pTsdb, STSmaDataWrapper *pData);
static int32_t tsdbJudgeStorageLevel(int64_t interval, int8_t intervalUnit);
static int32_t tsdbInsertTSmaDataSection(STSmaWriteH *pSmaH, STSmaDataWrapper *pData);
static int32_t tsdbInsertTSmaBlocks(void *bTree, const char *smaKey, const char *pData, int32_t dataLen);

static int64_t tsdbGetIntervalByPrecision(int64_t interval, uint8_t intervalUnit, int8_t precision);
static int32_t tsdbSetTSmaDataFile(STSmaWriteH *pSmaH, STSmaDataWrapper *pData, int32_t storageLevel, int32_t fid);

static int32_t tsdbInitTSmaReadH(STSmaReadH *pSmaH, STsdb *pTsdb, STSmaDataWrapper *pData);
static int32_t tsdbInitTSmaFile(STSmaReadH *pReadH, STimeWindow *queryWin);
static bool    tsdbSetAndOpenTSmaFile(STSmaReadH *pReadH, STimeWindow *queryWin);

static int32_t tsdbInitSmaStat(SSmaStat **pSmaStat) {
  ASSERT(pSmaStat != NULL);

  if (*pSmaStat != NULL) {  // no lock
    return TSDB_CODE_SUCCESS;
  }

  // TODO: lock. lazy mode when update expired window, or hungry mode during tsdbNew.
  if (*pSmaStat == NULL) {
    *pSmaStat = (SSmaStat *)calloc(1, sizeof(SSmaStat));
    if (*pSmaStat == NULL) {
      // TODO: unlock
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return TSDB_CODE_FAILED;
    }

    (*pSmaStat)->smaStatItems =
        taosHashInit(SMA_STATE_HASH_SLOT, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);

    if ((*pSmaStat)->smaStatItems == NULL) {
      tfree(*pSmaStat);
      // TODO: unlock
      return TSDB_CODE_FAILED;
    }
  }
  // TODO: unlock
  return TSDB_CODE_SUCCESS;
}

static SSmaStatItem *tsdbNewSmaStatItem(int8_t state) {
  SSmaStatItem *pItem = NULL;

  pItem = (SSmaStatItem *)calloc(1, sizeof(SSmaStatItem));
  if (pItem) {
    pItem->state = state;
    pItem->expiredWindows = taosHashInit(SMA_STATE_ITEM_HASH_SLOT, taosGetDefaultHashFunction(TSDB_DATA_TYPE_TIMESTAMP),
                                         true, HASH_ENTRY_LOCK);
    if (!pItem->expiredWindows) {
      tfree(pItem);
    }
  }
  return pItem;
}

int32_t tsdbDestroySmaState(SSmaStat *pSmaStat) {
  if (pSmaStat) {
    // TODO: use taosHashSetFreeFp when taosHashSetFreeFp is ready.
    SSmaStatItem *item = taosHashIterate(pSmaStat->smaStatItems, NULL);
    while (item != NULL) {
      tfree(item->pSma);
      taosHashCleanup(item->expiredWindows);
      item = taosHashIterate(pSmaStat->smaStatItems, item);
    }
    taosHashCleanup(pSmaStat->smaStatItems);
    free(pSmaStat);
  }
  return 0; // to avoid compile error
}

/**
 * @brief Update expired window according to msg from stream computing module.
 *
 * @param pTsdb
 * @param msg
 * @return int32_t
 */
int32_t tsdbUpdateExpiredWindow(STsdb *pTsdb, char *msg) {
  if (msg == NULL) {
    return TSDB_CODE_FAILED;
  }

  // lazy mode
  if (tsdbInitSmaStat(&pTsdb->pSmaStat) != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_FAILED;
  }

  // TODO: decode the msg => start
  int64_t       indexUid = SMA_TEST_INDEX_UID;
  const char *  indexName = SMA_TEST_INDEX_NAME;
  const int32_t SMA_TEST_EXPIRED_WINDOW_SIZE = 10;
  TSKEY         expiredWindows[SMA_TEST_EXPIRED_WINDOW_SIZE];
  int64_t       now = taosGetTimestampMs();
  for (int32_t i = 0; i < SMA_TEST_EXPIRED_WINDOW_SIZE; ++i) {
    expiredWindows[i] = now + i;
  }

  // TODO: decode the msg <= end
  SHashObj *pItemsHash = pTsdb->pSmaStat->smaStatItems;

  SSmaStatItem *pItem = (SSmaStatItem *)taosHashGet(pItemsHash, indexName, strlen(indexName));
  if (pItem == NULL) {
    pItem = tsdbNewSmaStatItem(TSDB_SMA_STAT_EXPIRED);  // TODO use the real state
    if (pItem == NULL) {
      // Response to stream computing: OOM
      // For query, if the indexName not found, the TSDB should tell query module to query raw TS data.
      return TSDB_CODE_FAILED;
    }

    // cache smaMeta
    STSma *pSma = metaGetSmaInfoByIndex(pTsdb->pMeta, indexUid);
    if (pSma == NULL) {
      taosHashCleanup(pItem->expiredWindows);
      free(pItem);
      return TSDB_CODE_FAILED;
    }
    pItem->pSma = pSma;

    // TODO: change indexName to indexUid
    if (taosHashPut(pItemsHash, indexName, strnlen(indexName, TSDB_INDEX_NAME_LEN), &pItem, sizeof(pItem)) != 0) {
      // If error occurs during put smaStatItem, free the resources of pItem
      taosHashCleanup(pItem->expiredWindows);
      free(pItem);
      return TSDB_CODE_FAILED;
    }
  }

  int8_t state = TSDB_SMA_STAT_EXPIRED;
  for (int32_t i = 0; i < SMA_TEST_EXPIRED_WINDOW_SIZE; ++i) {
    if (taosHashPut(pItem->expiredWindows, &expiredWindows[i], sizeof(TSKEY), &state, sizeof(state)) != 0) {
      // If error occurs during taosHashPut expired windows, remove the smaIndex from pTsdb->pSmaStat, thus TSDB would
      // tell query module to query raw TS data.
      // N.B.
      //  1) It is assumed to be extemely little probability event of fail to taosHashPut.
      //  2) This would solve the inconsistency to some extent, but not completely, unless we record all expired
      // windows failed to put into hash table.
      taosHashCleanup(pItem->expiredWindows);
      tfree(pItem->pSma);
      taosHashRemove(pItemsHash, indexName, sizeof(indexName));
      return TSDB_CODE_FAILED;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tsdbResetExpiredWindow(STsdb *pTsdb, int64_t indexUid, TSKEY skey) {
  SSmaStatItem *pItem = NULL;

  if (pTsdb->pSmaStat && pTsdb->pSmaStat->smaStatItems) {
    pItem = (SSmaStatItem *)taosHashGet(pTsdb->pSmaStat->smaStatItems, &indexUid, sizeof(indexUid));
  }

  if (pItem != NULL) {
    // TODO: reset time window for the sma data blocks
    if (taosHashRemove(pItem->expiredWindows, &skey, sizeof(TSKEY)) != 0) {
      // error handling
    }

  } else {
    // error handling
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Judge the tSma storage level
 *
 * @param interval
 * @param intervalUnit
 * @return int32_t
 */
static int32_t tsdbJudgeStorageLevel(int64_t interval, int8_t intervalUnit) {
  // TODO: configurable for SMA_STORAGE_SPLIT_HOURS?
  switch (intervalUnit) {
    case TD_TIME_UNIT_HOUR:
      if (interval < SMA_STORAGE_SPLIT_HOURS) {
        return SMA_STORAGE_LEVEL_DFILESET;
      }
      break;
    case TD_TIME_UNIT_MINUTE:
      if (interval < 60 * SMA_STORAGE_SPLIT_HOURS) {
        return SMA_STORAGE_LEVEL_DFILESET;
      }
      break;
    case TD_TIME_UNIT_SEC:
      if (interval < 3600 * SMA_STORAGE_SPLIT_HOURS) {
        return SMA_STORAGE_LEVEL_DFILESET;
      }
      break;
    case TD_TIME_UNIT_MILLISEC:
      if (interval < 3600 * 1e3 * SMA_STORAGE_SPLIT_HOURS) {
        return SMA_STORAGE_LEVEL_DFILESET;
      }
      break;
    case TD_TIME_UNIT_MICROSEC:
      if (interval < 3600 * 1e6 * SMA_STORAGE_SPLIT_HOURS) {
        return SMA_STORAGE_LEVEL_DFILESET;
      }
      break;
    case TD_TIME_UNIT_NANOSEC:
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
 * @brief Insert TSma data blocks to B+Tree
 *
 * @param bTree
 * @param smaKey
 * @param pData
 * @param dataLen
 * @return int32_t
 */
static int32_t tsdbInsertTSmaBlocks(void *bTree, const char *smaKey, const char *pData, int32_t dataLen) {
  // TODO: insert sma data blocks into B+Tree
  tsdbDebug("insert sma data blocks into B+Tree: smaKey %" PRIx64 "-%" PRIu16 "-%" PRIx64 ", dataLen %d",
         *(uint64_t *)smaKey, *(uint16_t *)POINTER_SHIFT(smaKey, 8), *(int64_t *)POINTER_SHIFT(smaKey, 10), dataLen);
  return TSDB_CODE_SUCCESS;
}

static int64_t tsdbGetIntervalByPrecision(int64_t interval, uint8_t intervalUnit, int8_t precision) {
  if (intervalUnit < TD_TIME_UNIT_MILLISEC) {
    switch (intervalUnit) {
      case TD_TIME_UNIT_YEAR:
      case TD_TIME_UNIT_SEASON:
      case TD_TIME_UNIT_MONTH:
      case TD_TIME_UNIT_WEEK:
        // illegal time unit
        tsdbError("invalid interval unit: %d\n", intervalUnit);
        TASSERT(0);
        break;
      case TD_TIME_UNIT_DAY:  // the interval for tSma calculation must <= day
        interval *= 86400 * 1e3;
        break;
      case TD_TIME_UNIT_HOUR:
        interval *= 3600 * 1e3;
        break;
      case TD_TIME_UNIT_MINUTE:
        interval *= 60 * 1e3;
        break;
      case TD_TIME_UNIT_SEC:
        interval *= 1e3;
        break;
      default:
        break;
    }
  }

  switch (intervalUnit) {
    case TD_TIME_UNIT_MILLISEC:
      if (TSDB_TIME_PRECISION_MILLI == precision) {
        return interval;
      } else if (TSDB_TIME_PRECISION_MICRO == precision) {
        return interval * 1e3;
      } else {  //  nano second
        return interval * 1e6;
      }
      break;
    case TD_TIME_UNIT_MICROSEC:
      if (TSDB_TIME_PRECISION_MILLI == precision) {
        return interval / 1e3;
      } else if (TSDB_TIME_PRECISION_MICRO == precision) {
        return interval;
      } else {  //  nano second
        return interval * 1e3;
      }
      break;
    case TD_TIME_UNIT_NANOSEC:
      if (TSDB_TIME_PRECISION_MILLI == precision) {
        return interval / 1e6;
      } else if (TSDB_TIME_PRECISION_MICRO == precision) {
        return interval / 1e3;
      } else {  // nano second
        return interval;
      }
      break;
    default:
      if (TSDB_TIME_PRECISION_MILLI == precision) {
        return interval * 1e3;
      } else if (TSDB_TIME_PRECISION_MICRO == precision) {
        return interval * 1e6;
      } else {  // nano second
        return interval * 1e9;
      }
      break;
  }
  return interval;
}

/**
 * @brief Split the TSma data blocks into expected size and insert into B+Tree.
 *
 * @param pSmaH
 * @param pData
 * @param nOffset The nOffset of blocks since fid changes.
 * @param nBlocks The nBlocks with the same fid since nOffset.
 * @return int32_t
 */
static int32_t tsdbInsertTSmaDataSection(STSmaWriteH *pSmaH, STSmaDataWrapper *pData) {
  STsdb *pTsdb = pSmaH->pTsdb;

  tsdbDebug("tsdbInsertTSmaDataSection: index %" PRIi64 ", skey %" PRIi64, pData->indexUid, pData->skey);

  // TODO: check the data integrity

  void *bTree = pSmaH->pDFile;

  int32_t len = 0;
  while (true) {
    if (len >= pData->dataLen) {
      break;
    }
    assert(pData->dataLen > 0);
    STSmaTbData *pTbData = (STSmaTbData *)POINTER_SHIFT(pData->data, len);

    int32_t tbLen = 0;
    while (true) {
      if (tbLen >= pTbData->dataLen) {
        break;
      }
      assert(pTbData->dataLen > 0);
      STSmaColData *pColData = (STSmaColData *)POINTER_SHIFT(pTbData->data, tbLen);
      char          smaKey[SMA_KEY_LEN] = {0};
      void *        pSmaKey = &smaKey;
#if 0
      printf("tsdbInsertTSmaDataSection: index %" PRIi64 ", skey %" PRIi64 " table[%" PRIi64 "]col[%" PRIu16 "]\n",
             pData->indexUid, pData->skey, pTbData->tableUid, pColData->colId);
#endif
      tsdbEncodeTSmaKey(pTbData->tableUid, pColData->colId, pData->skey, (void **)&pSmaKey);
      if (tsdbInsertTSmaBlocks(bTree, smaKey, pColData->data, pColData->blockSize) < 0) {
        tsdbWarn("vgId:%d insert tSma blocks failed since %s", REPO_ID(pTsdb), tstrerror(terrno));
      }
      tbLen += (sizeof(STSmaColData) + pColData->blockSize);
    }
    len += (sizeof(STSmaTbData) + pTbData->dataLen);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tsdbInitTSmaWriteH(STSmaWriteH *pSmaH, STsdb *pTsdb, STSmaDataWrapper *pData) {
  pSmaH->pTsdb = pTsdb;

  pSmaH->interval = tsdbGetIntervalByPrecision(pData->interval, pData->intervalUnit, REPO_CFG(pTsdb)->precision);
  return 0;
}

static int32_t tsdbSetTSmaDataFile(STSmaWriteH *pSmaH, STSmaDataWrapper *pData, int32_t storageLevel, int32_t fid) {
  // TODO
  pSmaH->pDFile = "tSma_interval_file_name";

  return TSDB_CODE_SUCCESS;
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
int32_t tsdbInsertTSmaDataImpl(STsdb *pTsdb, char *msg) {
  STsdbCfg *  pCfg = REPO_CFG(pTsdb);
  STSmaDataWrapper *pData = (STSmaDataWrapper *)msg;
  STSmaWriteH tSmaH = {0};

  tsdbInitTSmaWriteH(&tSmaH, pTsdb, pData);

  if (pData->dataLen <= 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  // Step 1: Judge the storage level
  int32_t storageLevel = tsdbJudgeStorageLevel(pData->interval, pData->intervalUnit);
  int32_t daysPerFile = storageLevel == SMA_STORAGE_LEVEL_TSDB ? SMA_STORAGE_TSDB_DAYS : pCfg->daysPerFile;

  // Step 2: Set the DFile for storage of SMA index, and iterate/split the TSma data and store to B+Tree index file
  //         - Set and open the DFile or the B+Tree file

  int32_t fid = (int32_t)(TSDB_KEY_FID(pData->skey, daysPerFile, pCfg->precision));

  // Save all the TSma data to one file
  // TODO: tsdbStartTSmaCommit();
  tsdbSetTSmaDataFile(&tSmaH, pData, storageLevel, fid);
  tsdbInsertTSmaDataSection(&tSmaH, pData);
  // TODO:tsdbEndTSmaCommit();

  // reset the SSmaStat
  tsdbResetExpiredWindow(pTsdb, pData->indexUid, pData->skey);

  return TSDB_CODE_SUCCESS;
}

static int32_t tsdbSetRSmaDataFile(STSmaWriteH *pSmaH, STSmaDataWrapper *pData, int32_t fid) {
  // TODO
  pSmaH->pDFile = "rSma_interval_file_name";

  return TSDB_CODE_SUCCESS;
}

int32_t tsdbInsertRSmaDataImpl(STsdb *pTsdb, char *msg) {
  STsdbCfg *  pCfg = REPO_CFG(pTsdb);
  STSmaDataWrapper *pData = (STSmaDataWrapper *)msg;
  STSmaWriteH tSmaH = {0};

  tsdbInitTSmaWriteH(&tSmaH, pTsdb, pData);

  if (pData->dataLen <= 0) {
    TASSERT(0);
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  // Step 1: Judge the storage level
  int32_t storageLevel = tsdbJudgeStorageLevel(pData->interval, pData->intervalUnit);
  int32_t daysPerFile = storageLevel == SMA_STORAGE_LEVEL_TSDB ? SMA_STORAGE_TSDB_DAYS : pCfg->daysPerFile;

  // Step 2: Set the DFile for storage of SMA index, and iterate/split the TSma data and store to B+Tree index file
  //         - Set and open the DFile or the B+Tree file

  int32_t fid = (int32_t)(TSDB_KEY_FID(pData->skey, daysPerFile, pCfg->precision));

  // Save all the TSma data to one file
  // TODO: tsdbStartTSmaCommit();
  tsdbSetTSmaDataFile(&tSmaH, pData, storageLevel, fid);
  tsdbInsertTSmaDataSection(&tSmaH, pData);
  // TODO:tsdbEndTSmaCommit();

  // reset the SSmaStat
  tsdbResetExpiredWindow(pTsdb, pData->indexUid, pData->skey);

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Init of tSma ReadH
 *
 * @param pSmaH
 * @param pTsdb
 * @param param
 * @param pData
 * @return int32_t
 */
static int32_t tsdbInitTSmaReadH(STSmaReadH *pSmaH, STsdb *pTsdb, STSmaDataWrapper *pData) {
  pSmaH->pTsdb = pTsdb;
  pSmaH->interval = tsdbGetIntervalByPrecision(pData->interval, pData->intervalUnit, REPO_CFG(pTsdb)->precision);
  // pSmaH->blockSize = param->numOfFuncIds * sizeof(int64_t);
  return 0; // to avoid compile error
}

/**
 * @brief Init of tSma FS
 *
 * @param pReadH
 * @param param
 * @param queryWin
 * @return int32_t
 */
static int32_t tsdbInitTSmaFile(STSmaReadH *pReadH, STimeWindow *queryWin) {
  int32_t storageLevel = 0;   //tsdbJudgeStorageLevel(param->interval, param->intervalUnit);
  int32_t daysPerFile =
      storageLevel == SMA_STORAGE_LEVEL_TSDB ? SMA_STORAGE_TSDB_DAYS : REPO_CFG(pReadH->pTsdb)->daysPerFile;
  pReadH->storageLevel = storageLevel;
  pReadH->days = daysPerFile;
  pReadH->smaFsIter.iter = 0;

  return 0;
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
static bool tsdbSetAndOpenTSmaFile(STSmaReadH *pReadH, STimeWindow *queryWin) {
  SArray *smaFs = pReadH->pTsdb->fs->cstatus->sf;
  int32_t nSmaFs = taosArrayGetSize(smaFs);

  pReadH->pDFile = NULL;

  while (pReadH->smaFsIter.iter < nSmaFs) {
    void *pSmaFile = taosArrayGet(smaFs, pReadH->smaFsIter.iter);
    if (pSmaFile) {  // match(indexName, queryWindow)
      // TODO: select the file by index_name ...
      pReadH->pDFile = pSmaFile;
      ++pReadH->smaFsIter.iter;
      break;
    }
    ++pReadH->smaFsIter.iter;
  }

  if (pReadH->pDFile != NULL) {
    tsdbDebug("vg%d: smaFile %s matched", REPO_ID(pReadH->pTsdb), "[pSmaFile dir]");
    return true;
  }

  return false;
}

/**
 * @brief Return the data between queryWin and fill the pData.
 *
 * @param pTsdb
 * @param param
 * @param pData
 * @param queryWin
 * @param nMaxResult The query invoker should control the nMaxResult need to return to avoid OOM.
 * @return int32_t
 */
int32_t tsdbGetTSmaDataImpl(STsdb *pTsdb, STSmaDataWrapper *pData, STimeWindow *queryWin, int32_t nMaxResult) {
  SSmaStatItem *pItem =
      (SSmaStatItem *)taosHashGet(pTsdb->pSmaStat->smaStatItems, &pData->indexUid, sizeof(pData->indexUid));
  if (pItem == NULL) {
    // mark all window as expired and notify query module to query raw TS data.
    return TSDB_CODE_SUCCESS;
  }

  int32_t nQueryWin = 0;
  for (int32_t n = 0; n < nQueryWin; ++n) {
    TSKEY thisWindow = n;
    if (taosHashGet(pItem->expiredWindows, &thisWindow, sizeof(thisWindow)) != NULL) {
      // TODO: mark this window as expired.
    }
  }

  STSmaReadH tReadH = {0};
  tsdbInitTSmaReadH(&tReadH, pTsdb, pData);

  tsdbInitTSmaFile(&tReadH, queryWin);

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

  // read data from file and fill the result
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
