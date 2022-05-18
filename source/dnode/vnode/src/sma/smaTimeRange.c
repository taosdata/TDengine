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
#include "tsdb.h"

typedef STsdbCfg STSmaKeepCfg;

#undef _TEST_SMA_PRINT_DEBUG_LOG_
#define SMA_STORAGE_TSDB_MINUTES 86400
#define SMA_STORAGE_TSDB_TIMES   10
#define SMA_STORAGE_SPLIT_FACTOR 144  // least records in tsma file
#define SMA_KEY_LEN              16   // TSKEY+groupId 8+8
#define SMA_DROP_EXPIRED_TIME    10   // default is 10 seconds

#define SMA_STATE_ITEM_HASH_SLOT 32

typedef struct {
  SSma         *pSma;
  SDBFile       dFile;
  const SArray *pDataBlocks;  // sma data
  int64_t       interval;     // interval with the precision of DB
} STSmaWriteH;

typedef struct {
  int32_t iter;
  int32_t fid;
} SmaFsIter;

typedef struct {
  STsdb    *pTsdb;
  SSma     *pSma;
  SDBFile   dFile;
  int64_t   interval;   // interval with the precision of DB
  int32_t   blockSize;  // size of SMA block item
  int32_t   days;
  int8_t    storageLevel;
  SmaFsIter smaFsIter;
} STSmaReadH;

typedef enum {
  SMA_STORAGE_LEVEL_TSDB = 0,     // use days of self-defined  e.g. vnode${N}/tsdb/tsma/sma_index_uid/v2f200.tsma
  SMA_STORAGE_LEVEL_DFILESET = 1  // use days of TS data       e.g. vnode${N}/tsdb/tsma/sma_index_uid/v2f1906.tsma
} ESmaStorageLevel;

// static func

static int64_t tdGetIntervalByPrecision(int64_t interval, uint8_t intervalUnit, int8_t precision, bool adjusted);
static int32_t tdGetSmaStorageLevel(STSmaKeepCfg *pCfg, int64_t interval);
static int32_t tdInitTSmaWriteH(STSmaWriteH *pSmaH, SSma *pSma, const SArray *pDataBlocks, int64_t interval,
                                int8_t intervalUnit);
static int32_t tdInitTSmaReadH(STSmaReadH *pSmaH, SSma *pSma, int64_t interval, int8_t intervalUnit);
static void    tdDestroyTSmaWriteH(STSmaWriteH *pSmaH);
static int32_t tdGetTSmaDays(SSma *pSma, int64_t interval, int32_t storageLevel);
static int32_t tdSetTSmaDataFile(STSmaWriteH *pSmaH, int64_t indexUid, int32_t fid);
static int32_t tdInitTSmaFile(STSmaReadH *pSmaH, int64_t indexUid, TSKEY skey);
static bool    tdSetAndOpenTSmaFile(STSmaReadH *pReadH, TSKEY *queryKey);
static int32_t tdInsertTSmaBlocks(STSmaWriteH *pSmaH, void *smaKey, int32_t keyLen, void *pData, int32_t dataLen,
                                  TXN *txn);
// expired window

static int32_t tdSetExpiredWindow(SSma *pSma, SHashObj *pItemsHash, int64_t indexUid, int64_t winSKey, int64_t version);
static int32_t tdResetExpiredWindow(SSma *pSma, SSmaStat *pStat, int64_t indexUid, TSKEY skey);
static int32_t tdDropTSmaDataImpl(SSma *pSma, int64_t indexUid);

// read data

// implementation

/**
 * @brief
 *
 * @param pSmaH
 * @param pSma
 * @param interval
 * @param intervalUnit
 * @return int32_t
 */
static int32_t tdInitTSmaReadH(STSmaReadH *pSmaH, SSma *pSma, int64_t interval, int8_t intervalUnit) {
  STSmaKeepCfg *pCfg = SMA_TSDB_CFG(pSma);
  pSmaH->pSma = pSma;
  pSmaH->interval = tdGetIntervalByPrecision(interval, intervalUnit, SMA_TSDB_CFG(pSma)->precision, true);
  pSmaH->storageLevel = tdGetSmaStorageLevel(pCfg, interval);
  pSmaH->days = tdGetTSmaDays(pSma, pSmaH->interval, pSmaH->storageLevel);
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
static int32_t tdInitTSmaFile(STSmaReadH *pSmaH, int64_t indexUid, TSKEY skey) {
  SSma *pSma = pSmaH->pSma;

  int32_t fid = (int32_t)(TSDB_KEY_FID(skey, pSmaH->days, SMA_TSDB_CFG(pSma)->precision));
  char    tSmaFile[TSDB_FILENAME_LEN] = {0};
  snprintf(tSmaFile, TSDB_FILENAME_LEN, "%" PRIi64 "%sv%df%d.tsma", indexUid, TD_DIRSEP, SMA_VID(pSma), fid);
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
static bool tdSetAndOpenTSmaFile(STSmaReadH *pReadH, TSKEY *queryKey) {
  // SArray *smaFs = pReadH->pTsdb->fs->cstatus->sf;
  // int32_t nSmaFs = taosArrayGetSize(smaFs);

  smaCloseDBF(&pReadH->dFile);

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
    tdDebug("vg%d: smaFile %s matched", REPO_ID(pReadH->pTsdb), "[pSmaFile dir]");
    return true;
  }
#endif

  return false;
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
static int64_t tdGetIntervalByPrecision(int64_t interval, uint8_t intervalUnit, int8_t precision, bool adjusted) {
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

static int32_t tdInitTSmaWriteH(STSmaWriteH *pSmaH, SSma *pSma, const SArray *pDataBlocks, int64_t interval,
                                int8_t intervalUnit) {
  pSmaH->pSma = pSma;
  pSmaH->interval = tdGetIntervalByPrecision(interval, intervalUnit, SMA_TSDB_CFG(pSma)->precision, true);
  pSmaH->pDataBlocks = pDataBlocks;
  pSmaH->dFile.fid = SMA_IVLD_FID;
  return TSDB_CODE_SUCCESS;
}

static void tdDestroyTSmaWriteH(STSmaWriteH *pSmaH) {
  if (pSmaH) {
    smaCloseDBF(&pSmaH->dFile);
  }
}

static int32_t tdSetTSmaDataFile(STSmaWriteH *pSmaH, int64_t indexUid, int32_t fid) {
  SSma *pSma = pSmaH->pSma;
  ASSERT(!pSmaH->dFile.path && !pSmaH->dFile.pDB);

  pSmaH->dFile.fid = fid;
  char tSmaFile[TSDB_FILENAME_LEN] = {0};
  snprintf(tSmaFile, TSDB_FILENAME_LEN, "%" PRIi64 "%sv%df%d.tsma", indexUid, TD_DIRSEP, SMA_VID(pSma), fid);
  pSmaH->dFile.path = strdup(tSmaFile);

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief
 *
 * @param pSma
 * @param interval Interval calculated by DB's precision
 * @param storageLevel
 * @return int32_t
 */
static int32_t tdGetTSmaDays(SSma *pSma, int64_t interval, int32_t storageLevel) {
  STsdbCfg *pCfg = SMA_TSDB_CFG(pSma);
  int32_t   daysPerFile = pCfg->days;  // unit is minute

  if (storageLevel == SMA_STORAGE_LEVEL_TSDB) {
    int32_t minutes = SMA_STORAGE_TSDB_TIMES * (interval / tsTickPerMin[pCfg->precision]);
    if (minutes > SMA_STORAGE_TSDB_MINUTES) {
      daysPerFile = SMA_STORAGE_TSDB_MINUTES;
    }
  }

  return daysPerFile;
}

/**
 * @brief Judge the tSma storage level
 *
 * @param pCfg
 * @param interval
 * @return int32_t
 */
static int32_t tdGetSmaStorageLevel(STSmaKeepCfg *pCfg, int64_t interval) {
  int64_t mInterval = convertTimeFromPrecisionToUnit(interval, pCfg->precision, TIME_UNIT_MINUTE);
  if (pCfg->days / mInterval >= SMA_STORAGE_SPLIT_FACTOR) {
    return SMA_STORAGE_LEVEL_DFILESET;
  }
  return SMA_STORAGE_LEVEL_TSDB;
}

/**
 * @brief Insert/Update Time-range-wise SMA data.
 *  - If interval < SMA_STORAGE_SPLIT_HOURS(e.g. 24), save the SMA data as a part of DFileSet to e.g.
 * v3f1900.tsma.${sma_index_name}. The days is the same with that for TS data files.
 *  - If interval >= SMA_STORAGE_SPLIT_HOURS, save the SMA data to e.g. vnode3/tsma/v3f632.tsma.${sma_index_name}. The
 * days is 30 times of the interval, and the minimum days is SMA_STORAGE_TSDB_DAYS(30d).
 *  - The destination file of one data block for some interval is determined by its start TS key.
 *
 * @param pSma
 * @param msg
 * @return int32_t
 */
int32_t tdProcessTSmaInsertImpl(SSma *pSma, int64_t indexUid, const char *msg) {
  STsdbCfg     *pCfg = SMA_TSDB_CFG(pSma);
  const SArray *pDataBlocks = (const SArray *)msg;
  int64_t       testSkey = TSKEY_INITIAL_VAL;

  // TODO: destroy SSDataBlocks(msg)

  // For super table aggregation, the sma data is stored in vgroup calculated from the hash value of stable name. Thus
  // the sma data would arrive ahead of the update-expired-window msg.
  if (tdCheckAndInitSmaEnv(pSma, TSDB_SMA_TYPE_TIME_RANGE) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TDB_INIT_FAILED;
    return TSDB_CODE_FAILED;
  }

  if (!pDataBlocks) {
    terrno = TSDB_CODE_INVALID_PTR;
    smaWarn("vgId:%d insert tSma data failed since pDataBlocks is NULL", SMA_VID(pSma));
    return terrno;
  }

  if (taosArrayGetSize(pDataBlocks) <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    smaWarn("vgId:%d insert tSma data failed since pDataBlocks is empty", SMA_VID(pSma));
    return TSDB_CODE_FAILED;
  }

  SSmaEnv      *pEnv = SMA_TSMA_ENV(pSma);
  SSmaStat     *pStat = SMA_ENV_STAT(pEnv);
  SSmaStatItem *pItem = NULL;

  tdRefSmaStat(pSma, pStat);

  if (pStat && SMA_STAT_ITEMS(pStat)) {
    pItem = taosHashGet(SMA_STAT_ITEMS(pStat), &indexUid, sizeof(indexUid));
  }

  if (!pItem || !(pItem = *(SSmaStatItem **)pItem) || tdSmaStatIsDropped(pItem)) {
    terrno = TSDB_CODE_TDB_INVALID_SMA_STAT;
    tdUnRefSmaStat(pSma, pStat);
    return TSDB_CODE_FAILED;
  }

  STSma      *pTSma = pItem->pTSma;
  STSmaWriteH tSmaH = {0};

  if (tdInitTSmaWriteH(&tSmaH, pSma, pDataBlocks, pTSma->interval, pTSma->intervalUnit) != 0) {
    return TSDB_CODE_FAILED;
  }

  char rPath[TSDB_FILENAME_LEN] = {0};
  char aPath[TSDB_FILENAME_LEN] = {0};
  snprintf(rPath, TSDB_FILENAME_LEN, "%s%s%" PRIi64, SMA_ENV_PATH(pEnv), TD_DIRSEP, indexUid);
  tfsAbsoluteName(SMA_TFS(pSma), SMA_ENV_DID(pEnv), rPath, aPath);
  if (!taosCheckExistFile(aPath)) {
    if (tfsMkdirRecurAt(SMA_TFS(pSma), rPath, SMA_ENV_DID(pEnv)) != TSDB_CODE_SUCCESS) {
      tdUnRefSmaStat(pSma, pStat);
      return TSDB_CODE_FAILED;
    }
  }

  // Step 1: Judge the storage level and days
  int32_t storageLevel = tdGetSmaStorageLevel(pCfg, tSmaH.interval);
  int32_t minutePerFile = tdGetTSmaDays(pSma, tSmaH.interval, storageLevel);

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
              testSkey = skey;
              printf("= skey %" PRIi64 " groupId = %" PRIi64 "|", skey, groupId);
              tdEncodeTSmaKey(groupId, skey, &pSmaKey);
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
      printf("\n");
      // if ((tlen > 0) && (skey != TSKEY_INITIAL_VAL)) {
      if (tlen > 0) {
        int32_t fid = (int32_t)(TSDB_KEY_FID(skey, minutePerFile, pCfg->precision));

        // Step 2: Set the DFile for storage of SMA index, and iterate/split the TSma data and store to B+Tree index
        // file
        //         - Set and open the DFile or the B+Tree file
        // TODO: tsdbStartTSmaCommit();
        if (fid != tSmaH.dFile.fid) {
          if (tSmaH.dFile.fid != SMA_IVLD_FID) {
            tdSmaEndCommit(pEnv);
            smaCloseDBF(&tSmaH.dFile);
          }
          tdSetTSmaDataFile(&tSmaH, indexUid, fid);
          smaDebug("@@@ vgId:%d write to DBF %s, days:%d, interval:%" PRIi64 ", storageLevel:%" PRIi32
                   " queryKey:%" PRIi64,
                   SMA_VID(pSma), tSmaH.dFile.path, minutePerFile, tSmaH.interval, storageLevel, testSkey);
          if (smaOpenDBF(pEnv->dbEnv, &tSmaH.dFile) != 0) {
            smaWarn("vgId:%d open DB file %s failed since %s", SMA_VID(pSma),
                    tSmaH.dFile.path ? tSmaH.dFile.path : "path is NULL", tstrerror(terrno));
            tdDestroyTSmaWriteH(&tSmaH);
            tdUnRefSmaStat(pSma, pStat);
            return TSDB_CODE_FAILED;
          }
          tdSmaBeginCommit(pEnv);
        }

        if (tdInsertTSmaBlocks(&tSmaH, &smaKey, SMA_KEY_LEN, dataBuf, tlen, &pEnv->txn) != 0) {
          smaWarn("vgId:%d insert tsma data blocks fail for index %" PRIi64 ", skey %" PRIi64 ", groupId %" PRIi64
                  " since %s",
                  SMA_VID(pSma), indexUid, skey, groupId, tstrerror(terrno));
          tdSmaEndCommit(pEnv);
          tdDestroyTSmaWriteH(&tSmaH);
          tdUnRefSmaStat(pSma, pStat);
          return TSDB_CODE_FAILED;
        }

        smaDebug("vgId:%d insert tsma data blocks success for index %" PRIi64 ", skey %" PRIi64 ", groupId %" PRIi64,
                 SMA_VID(pSma), indexUid, skey, groupId);
        // TODO:tsdbEndTSmaCommit();

        // Step 3: reset the SSmaStat
        tdResetExpiredWindow(pSma, pStat, indexUid, skey);
      } else {
        smaWarn("vgId:%d invalid data skey:%" PRIi64 ", tlen %" PRIi32 " during insert tSma data for %" PRIi64,
                SMA_VID(pSma), skey, tlen, indexUid);
      }
    }
  }
  tdSmaEndCommit(pEnv);  // TODO: not commit for every insert
  tdDestroyTSmaWriteH(&tSmaH);
  tdUnRefSmaStat(pSma, pStat);

  return TSDB_CODE_SUCCESS;
}

int32_t tdDropTSmaData(SSma *pSma, int64_t indexUid) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tdDropTSmaDataImpl(pSma, indexUid)) < 0) {
    smaWarn("vgId:%d drop tSma data failed since %s", SMA_VID(pSma), tstrerror(terrno));
  }
  return code;
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
static int32_t tdInsertTSmaBlocks(STSmaWriteH *pSmaH, void *smaKey, int32_t keyLen, void *pData, int32_t dataLen,
                                  TXN *txn) {
  SDBFile *pDBFile = &pSmaH->dFile;

  // TODO: insert tsma data blocks into B+Tree(TTB)
  if (smaSaveSmaToDB(pDBFile, smaKey, keyLen, pData, dataLen, txn) != 0) {
    smaWarn("vgId:%d insert tsma data blocks into %s: smaKey %" PRIx64 "-%" PRIx64 ", dataLen %" PRIu32 " fail",
            SMA_VID(pSmaH->pSma), pDBFile->path, *(int64_t *)smaKey, *(int64_t *)POINTER_SHIFT(smaKey, 8), dataLen);
    return TSDB_CODE_FAILED;
  }
  smaDebug("vgId:%d insert tsma data blocks into %s: smaKey %" PRIx64 "-%" PRIx64 ", dataLen %" PRIu32 " succeed",
           SMA_VID(pSmaH->pSma), pDBFile->path, *(int64_t *)smaKey, *(int64_t *)POINTER_SHIFT(smaKey, 8), dataLen);

#ifdef _TEST_SMA_PRINT_DEBUG_LOG_
  uint32_t valueSize = 0;
  void    *data = tdGetSmaDataByKey(pDBFile, smaKey, keyLen, &valueSize);
  ASSERT(data != NULL);
  for (uint32_t v = 0; v < valueSize; v += 8) {
    smaWarn("vgId:%d insert sma data val[%d] %" PRIi64, REPO_ID(pSmaH->pTsdb), v, *(int64_t *)POINTER_SHIFT(data, v));
  }
#endif
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief When sma data received from stream computing, make the relative expired window valid.
 *
 * @param pSma
 * @param pStat
 * @param indexUid
 * @param skey
 * @return int32_t
 */
static int32_t tdResetExpiredWindow(SSma *pSma, SSmaStat *pStat, int64_t indexUid, TSKEY skey) {
  SSmaStatItem *pItem = NULL;

  tdRefSmaStat(pSma, pStat);

  if (pStat && SMA_STAT_ITEMS(pStat)) {
    pItem = taosHashGet(SMA_STAT_ITEMS(pStat), &indexUid, sizeof(indexUid));
  }
  if ((pItem) && ((pItem = *(SSmaStatItem **)pItem))) {
    // pItem resides in hash buffer all the time unless drop sma index
    // TODO: multithread protect
    if (taosHashRemove(pItem->expiredWindows, &skey, sizeof(TSKEY)) != 0) {
      // error handling
      tdUnRefSmaStat(pSma, pStat);
      smaWarn("vgId:%d remove skey %" PRIi64 " from expired window for sma index %" PRIi64 " fail", SMA_VID(pSma), skey,
              indexUid);
      return TSDB_CODE_FAILED;
    }
    smaDebug("vgId:%d remove skey %" PRIi64 " from expired window for sma index %" PRIi64 " succeed", SMA_VID(pSma),
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
    tdUnRefSmaStat(pSma, pStat);
    smaWarn("vgId:%d expired window %" PRIi64 " not exists for sma index %" PRIi64, SMA_VID(pSma), skey, indexUid);
    return TSDB_CODE_FAILED;
  }

  tdUnRefSmaStat(pSma, pStat);
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Drop tSma data and local cache
 *        - insert/query reference
 * @param pSma
 * @param msg
 * @return int32_t
 */
static int32_t tdDropTSmaDataImpl(SSma *pSma, int64_t indexUid) {
  SSmaEnv *pEnv = atomic_load_ptr(&SMA_TSMA_ENV(pSma));

  // clear local cache
  if (pEnv) {
    smaDebug("vgId:%d drop tSma local cache for %" PRIi64, SMA_VID(pSma), indexUid);

    SSmaStatItem *pItem = taosHashGet(SMA_ENV_STAT_ITEMS(pEnv), &indexUid, sizeof(indexUid));
    if ((pItem) || ((pItem = *(SSmaStatItem **)pItem))) {
      if (tdSmaStatIsDropped(pItem)) {
        smaDebug("vgId:%d tSma stat is already dropped for %" PRIi64, SMA_VID(pSma), indexUid);
        return TSDB_CODE_TDB_INVALID_ACTION;  // TODO: duplicate drop msg would be intercepted by mnode
      }

      tdWLockSmaEnv(pEnv);
      if (tdSmaStatIsDropped(pItem)) {
        tdUnLockSmaEnv(pEnv);
        smaDebug("vgId:%d tSma stat is already dropped for %" PRIi64, SMA_VID(pSma), indexUid);
        return TSDB_CODE_TDB_INVALID_ACTION;  // TODO: duplicate drop msg would be intercepted by mnode
      }
      tdSmaStatSetDropped(pItem);
      tdUnLockSmaEnv(pEnv);

      int32_t nSleep = 0;
      int32_t refVal = INT32_MAX;
      while (true) {
        if ((refVal = T_REF_VAL_GET(SMA_ENV_STAT(pEnv))) <= 0) {
          smaDebug("vgId:%d drop index %" PRIi64 " since refVal=%d", SMA_VID(pSma), indexUid, refVal);
          break;
        }
        smaDebug("vgId:%d wait 1s to drop index %" PRIi64 " since refVal=%d", SMA_VID(pSma), indexUid, refVal);
        taosSsleep(1);
        if (++nSleep > SMA_DROP_EXPIRED_TIME) {
          smaDebug("vgId:%d drop index %" PRIi64 " after wait %d (refVal=%d)", SMA_VID(pSma), indexUid, nSleep, refVal);
          break;
        };
      }

      tdFreeSmaStatItem(pItem);
      smaDebug("vgId:%d getTSmaDataImpl failed since no index %" PRIi64 " in local cache", SMA_VID(pSma), indexUid);
    }
  }
  // clear sma data files
  // TODO:
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief
 *
 * @param pSma Return the data between queryWin and fill the pData.
 * @param pData
 * @param indexUid
 * @param pQuerySKey
 * @param nMaxResult The query invoker should control the nMaxResult need to return to avoid OOM.
 * @return int32_t
 */
int32_t tdGetTSmaDataImpl(SSma *pSma, char *pData, int64_t indexUid, TSKEY querySKey, int32_t nMaxResult) {
  SSmaEnv  *pEnv = atomic_load_ptr(&SMA_TSMA_ENV(pSma));
  SSmaStat *pStat = NULL;

  if (!pEnv) {
    terrno = TSDB_CODE_INVALID_PTR;
    smaWarn("vgId:%d getTSmaDataImpl failed since pTSmaEnv is NULL", SMA_VID(pSma));
    return TSDB_CODE_FAILED;
  }

  pStat = SMA_ENV_STAT(pEnv);

  tdRefSmaStat(pSma, pStat);
  SSmaStatItem *pItem = taosHashGet(SMA_ENV_STAT_ITEMS(pEnv), &indexUid, sizeof(indexUid));
  if (!pItem || !(pItem = *(SSmaStatItem **)pItem)) {
    // Normally pItem should not be NULL, mark all windows as expired and notify query module to fetch raw TS data if
    // it's NULL.
    tdUnRefSmaStat(pSma, pStat);
    terrno = TSDB_CODE_TDB_INVALID_ACTION;
    smaDebug("vgId:%d getTSmaDataImpl failed since no index %" PRIi64, SMA_VID(pSma), indexUid);
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
  if (!tdSmaStatIsOK(pItem, &smaStat)) {  // TODO: multiple check for large scale sma query
    tdUnRefSmaStat(pSma, pStat);
    terrno = TSDB_CODE_TDB_INVALID_SMA_STAT;
    smaWarn("vgId:%d getTSmaDataImpl failed from index %" PRIi64 " since %s %" PRIi8, SMA_VID(pSma), indexUid,
            tstrerror(terrno), smaStat);
    return TSDB_CODE_FAILED;
  }

  if (taosHashGet(pItem->expiredWindows, &querySKey, sizeof(TSKEY))) {
    // TODO: mark this window as expired.
    smaDebug("vgId:%d skey %" PRIi64 " of window exists in expired window for index %" PRIi64, SMA_VID(pSma), querySKey,
             indexUid);
  } else {
    smaDebug("vgId:%d skey %" PRIi64 " of window not in expired window for index %" PRIi64, SMA_VID(pSma), querySKey,
             indexUid);
  }

  STSma *pTSma = pItem->pTSma;
#endif

#if 1
  STSmaReadH tReadH = {0};
  tdInitTSmaReadH(&tReadH, pSma, pTSma->interval, pTSma->intervalUnit);
  smaCloseDBF(&tReadH.dFile);

  tdUnRefSmaStat(pSma, pStat);

  tdInitTSmaFile(&tReadH, indexUid, querySKey);
  smaDebug("### vgId:%d read from DBF %s  days:%d, interval:%" PRIi64 ", storageLevel:%" PRIi8 " queryKey:%" PRIi64,
           SMA_VID(pSma), tReadH.dFile.path, tReadH.days, tReadH.interval, tReadH.storageLevel, querySKey);
  if (smaOpenDBF(pEnv->dbEnv, &tReadH.dFile) != 0) {
    smaWarn("vgId:%d open DBF %s failed since %s", SMA_VID(pSma), tReadH.dFile.path, tstrerror(terrno));
    return TSDB_CODE_FAILED;
  }

  char    smaKey[SMA_KEY_LEN] = {0};
  void   *pSmaKey = &smaKey;
  int64_t queryGroupId = 0;
  tdEncodeTSmaKey(queryGroupId, querySKey, (void **)&pSmaKey);

  smaDebug("vgId:%d get sma data from %s: smaKey %" PRIx64 "-%" PRIx64 ", keyLen %d", SMA_VID(pSma), tReadH.dFile.path,
           *(int64_t *)smaKey, *(int64_t *)POINTER_SHIFT(smaKey, 8), SMA_KEY_LEN);

  void   *result = NULL;
  int32_t valueSize = 0;
  if (!(result = smaGetSmaDataByKey(&tReadH.dFile, smaKey, SMA_KEY_LEN, &valueSize))) {
    smaWarn("vgId:%d get sma data failed from smaIndex %" PRIi64 ", smaKey %" PRIx64 "-%" PRIx64 " since %s",
            SMA_VID(pSma), indexUid, *(int64_t *)smaKey, *(int64_t *)POINTER_SHIFT(smaKey, 8), tstrerror(terrno));
    smaCloseDBF(&tReadH.dFile);
    return TSDB_CODE_FAILED;
  }
#endif

#ifdef _TEST_SMA_PRINT_DEBUG_LOG_
  for (uint32_t v = 0; v < valueSize; v += 8) {
    smaWarn("vgId:%d get sma data v[%d]=%" PRIi64, SMA_VID(pSma), v, *(int64_t *)POINTER_SHIFT(result, v));
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
    if (tdSetAndOpenTSmaFile(&tReadH, queryWin)) {
      char bTree[100] = "\0";
      while (strncmp(bTree, "has more nodes", 100) == 0) {
        if (nResult >= nMaxResult) {
          break;
        }
        // tdGetDataFromBTree(bTree, queryWin, lastKey)
        // fill the pData
        ++nResult;
      }
    }
  }
#endif
  // read data from file and fill the result
  smaCloseDBF(&tReadH.dFile);
  return TSDB_CODE_SUCCESS;
}

int32_t tdProcessTSmaCreateImpl(SSma *pSma, int64_t version, const char *pMsg) {
  SSmaCfg *pCfg = (SSmaCfg *)pMsg;

  if (metaCreateTSma(SMA_META(pSma), version, pCfg) < 0) {
    return -1;
  }

  tdTSmaAdd(pSma, 1);
  return 0;
}

int32_t tdDropTSma(SSma *pSma, char *pMsg) {
#if 0
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

  if (metaDropTSma(SMA_META(pSma), vDropSmaReq.indexUid) < 0) {
    // TODO: handle error
    return -1;
  }

  if (tdDropTSmaData(pSma, vDropSmaReq.indexUid) < 0) {
    // TODO: handle error
    return -1;
  }

  tdTSmaSub(pSma, 1);
#endif

  // TODO: return directly or go on follow steps?
  return TSDB_CODE_SUCCESS;
}

static SSmaStatItem *tdNewSmaStatItem(int8_t state) {
  SSmaStatItem *pItem = NULL;

  pItem = (SSmaStatItem *)taosMemoryCalloc(1, sizeof(SSmaStatItem));
  if (!pItem) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pItem->state = state;
  pItem->expiredWindows = taosHashInit(SMA_STATE_ITEM_HASH_SLOT, taosGetDefaultHashFunction(TSDB_DATA_TYPE_TIMESTAMP),
                                       true, HASH_ENTRY_LOCK);
  if (!pItem->expiredWindows) {
    taosMemoryFreeClear(pItem);
    return NULL;
  }

  return pItem;
}

static int32_t tdSetExpiredWindow(SSma *pSma, SHashObj *pItemsHash, int64_t indexUid, int64_t winSKey,
                                  int64_t version) {
  SSmaStatItem *pItem = taosHashGet(pItemsHash, &indexUid, sizeof(indexUid));
  if (!pItem) {
    // TODO: use TSDB_SMA_STAT_EXPIRED and update by stream computing later
    pItem = tdNewSmaStatItem(TSDB_SMA_STAT_OK);  // TODO use the real state
    if (!pItem) {
      // Response to stream computing: OOM
      // For query, if the indexUid not found, the TSDB should tell query module to query raw TS data.
      return TSDB_CODE_FAILED;
    }

    // cache smaMeta
    STSma *pTSma = metaGetSmaInfoByIndex(SMA_META(pSma), indexUid);
    if (!pTSma) {
      terrno = TSDB_CODE_TDB_NO_SMA_INDEX_IN_META;
      taosHashCleanup(pItem->expiredWindows);
      taosMemoryFree(pItem);
      smaWarn("vgId:%d set expire window, get tsma meta failed for smaIndex %" PRIi64 " since %s", SMA_VID(pSma),
              indexUid, tstrerror(terrno));
      return TSDB_CODE_FAILED;
    }
    pItem->pTSma = pTSma;

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
    // If error occurs during taosHashPut expired windows, remove the smaIndex from pSma->pSmaStat, thus TSDB would
    // tell query module to query raw TS data.
    // N.B.
    //  1) It is assumed to be extemely little probability event of fail to taosHashPut.
    //  2) This would solve the inconsistency to some extent, but not completely, unless we record all expired
    // windows failed to put into hash table.
    taosHashCleanup(pItem->expiredWindows);
    taosMemoryFreeClear(pItem->pTSma);
    taosHashRemove(pItemsHash, &indexUid, sizeof(indexUid));
    smaWarn("vgId:%d smaIndex %" PRIi64 ", put skey %" PRIi64 " to expire window fail", SMA_VID(pSma), indexUid,
            winSKey);
    return TSDB_CODE_FAILED;
  }

  smaDebug("vgId:%d smaIndex %" PRIi64 ", put skey %" PRIi64 " to expire window succeed", SMA_VID(pSma), indexUid,
           winSKey);
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Update expired window according to msg from stream computing module.
 *
 * @param pSma
 * @param msg SSubmitReq
 * @return int32_t
 */
int32_t tdUpdateExpiredWindowImpl(SSma *pSma, SSubmitReq *pMsg, int64_t version) {
  // no time-range-sma, just return success
  if (atomic_load_16(&SMA_TSMA_NUM(pSma)) <= 0) {
    smaTrace("vgId:%d not update expire window since no tSma", SMA_VID(pSma));
    return TSDB_CODE_SUCCESS;
  }

  if (!SMA_META(pSma)) {
    terrno = TSDB_CODE_INVALID_PTR;
    smaError("vgId:%d update expire window failed since no meta ptr", SMA_VID(pSma));
    return TSDB_CODE_FAILED;
  }

  if (tdCheckAndInitSmaEnv(pSma, TSDB_SMA_TYPE_TIME_RANGE) < 0) {
    smaError("vgId:%d init sma env failed since %s", SMA_VID(pSma), terrstr(terrno));
    terrno = TSDB_CODE_TDB_INIT_FAILED;
    return TSDB_CODE_FAILED;
  }

  // Firstly, assume that tSma can only be created on super table/normal table.
  // getActiveTimeWindow

  SSmaEnv  *pEnv = SMA_TSMA_ENV(pSma);
  SSmaStat *pStat = SMA_ENV_STAT(pEnv);
  SHashObj *pItemsHash = SMA_ENV_STAT_ITEMS(pEnv);

  TASSERT(pEnv && pStat && pItemsHash);

  // basic procedure
  // TODO: optimization
  tdRefSmaStat(pSma, pStat);

  SSubmitMsgIter msgIter = {0};
  SSubmitBlk    *pBlock = NULL;
  SInterval      interval = {0};
  TSKEY          lastWinSKey = INT64_MIN;

  if (tInitSubmitMsgIter(pMsg, &msgIter) < 0) {
    return TSDB_CODE_FAILED;
  }

  while (true) {
    tGetSubmitMsgNext(&msgIter, &pBlock);
    if (!pBlock) break;

    STSmaWrapper *pSW = NULL;
    STSma        *pTSma = NULL;

    SSubmitBlkIter blkIter = {0};
    if (tInitSubmitBlkIter(&msgIter, pBlock, &blkIter) < 0) {
      pSW = tdFreeTSmaWrapper(pSW, false);
      break;
    }

    while (true) {
      STSRow *row = tGetSubmitBlkNext(&blkIter);
      if (!row) {
        pSW = tdFreeTSmaWrapper(pSW, false);
        break;
      }
      if (!pSW || (pTSma && (pTSma->tableUid != msgIter.suid))) {
        if (pSW) {
          pSW = tdFreeTSmaWrapper(pSW, false);
        }
        if (!(pSW = metaGetSmaInfoByTable(SMA_META(pSma), msgIter.suid, false))) {
          break;
        }
        if ((pSW->number) <= 0 || !pSW->tSma) {
          pSW = tdFreeTSmaWrapper(pSW, false);
          break;
        }

        pTSma = pSW->tSma;

        interval.interval = pTSma->interval;
        interval.intervalUnit = pTSma->intervalUnit;
        interval.offset = pTSma->offset;
        interval.precision = SMA_TSDB_CFG(pSma)->precision;
        interval.sliding = pTSma->sliding;
        interval.slidingUnit = pTSma->slidingUnit;
      }

      // TODO: process multiple tsma for one table uid
      TSKEY winSKey = taosTimeTruncate(TD_ROW_KEY(row), &interval, interval.precision);

      if (lastWinSKey != winSKey) {
        lastWinSKey = winSKey;
        if (tdSetExpiredWindow(pSma, pItemsHash, pTSma->indexUid, winSKey, version) < 0) {
          pSW = tdFreeTSmaWrapper(pSW, false);
          tdUnRefSmaStat(pSma, pStat);
          return TSDB_CODE_FAILED;
        }
      } else {
        smaDebug("vgId:%d smaIndex %" PRIi64 ", put skey %" PRIi64 " to expire window ignore as duplicated",
                 SMA_VID(pSma), pTSma->indexUid, winSKey);
      }
    }
  }

  tdUnRefSmaStat(pSma, pStat);

  return TSDB_CODE_SUCCESS;
}
