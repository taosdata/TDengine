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
#define SMA_STORAGE_MINUTES_MAX  86400
#define SMA_STORAGE_MINUTES_DAY  1440
#define SMA_STORAGE_MINUTES_MIN  1440
#define SMA_STORAGE_TSDB_MINUTES 86400
#define SMA_STORAGE_TSDB_TIMES   10
#define SMA_STORAGE_SPLIT_FACTOR 14400  // least records in tsma file TODO: the feasible value?
#define SMA_KEY_LEN              16     // TSKEY+groupId 8+8
#define SMA_DROP_EXPIRED_TIME    10     // default is 10 seconds

#define SMA_STATE_ITEM_HASH_SLOT 32

// static func

/**
 * @brief Judge the tsma file split days
 *
 * @param pCfg
 * @param pCont
 * @param contLen
 * @param days unit is minute
 * @return int32_t
 */
int32_t tdProcessTSmaGetDaysImpl(SVnodeCfg *pCfg, void *pCont, uint32_t contLen, int32_t *days) {
  SDecoder coder = {0};
  tDecoderInit(&coder, pCont, contLen);

  STSma tsma = {0};
  if (tDecodeSVCreateTSmaReq(&coder, &tsma) < 0) {
    terrno = TSDB_CODE_MSG_DECODE_ERROR;
    goto _err;
  }
  STsdbCfg *pTsdbCfg = &pCfg->tsdbCfg;
  int64_t   sInterval = convertTimeFromPrecisionToUnit(tsma.interval, pTsdbCfg->precision, TIME_UNIT_SECOND);
  if (sInterval <= 0) {
    *days = pTsdbCfg->days;
    return 0;
  }
  int64_t records = pTsdbCfg->days * 60 / sInterval;
  if (records >= SMA_STORAGE_SPLIT_FACTOR) {
    *days = pTsdbCfg->days;
  } else {
    int64_t mInterval = convertTimeFromPrecisionToUnit(tsma.interval, pTsdbCfg->precision, TIME_UNIT_MINUTE);
    int64_t daysPerFile = mInterval * SMA_STORAGE_MINUTES_DAY * 2;

    if (daysPerFile > SMA_STORAGE_MINUTES_MAX) {
      *days = SMA_STORAGE_MINUTES_MAX;
    } else {
      *days = (int32_t)daysPerFile;
    }

    if (*days < pTsdbCfg->days) {
      *days = pTsdbCfg->days;
    }
  }
  tDecoderClear(&coder);
  return 0;
_err:
  tDecoderClear(&coder);
  return -1;
}

// read data

// implementation

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
  STsdbCfg *pCfg = SMA_TSDB_CFG(pSma);

  const SArray *pDataBlocks = (const SArray *)msg;

  // TODO: destroy SSDataBlocks(msg)

  // For super table aggregation, the sma data is stored in vgroup calculated from the hash value of stable name. Thus
  // the sma data would arrive ahead of the update-expired-window msg.
  if (tdCheckAndInitSmaEnv(pSma, TSDB_SMA_TYPE_TIME_RANGE, false) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TDB_INIT_FAILED;
    return TSDB_CODE_FAILED;
  }

  if (!pDataBlocks) {
    terrno = TSDB_CODE_INVALID_PTR;
    smaWarn("vgId:%d, insert tsma data failed since pDataBlocks is NULL", SMA_VID(pSma));
    return terrno;
  }

  if (taosArrayGetSize(pDataBlocks) <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    smaWarn("vgId:%d, insert tsma data failed since pDataBlocks is empty", SMA_VID(pSma));
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
    terrno = TSDB_CODE_TSMA_INVALID_STAT;
    tdUnRefSmaStat(pSma, pStat);
    return TSDB_CODE_FAILED;
  }

  STSma *pTSma = pItem->pTSma;

  tdUnRefSmaStat(pSma, pStat);

  return TSDB_CODE_SUCCESS;
}

int32_t tdProcessTSmaCreateImpl(SSma *pSma, int64_t version, const char *pMsg) {
  SSmaCfg *pCfg = (SSmaCfg *)pMsg;

  if (metaCreateTSma(SMA_META(pSma), version, pCfg) < 0) {
    return -1;
  }

  if (TD_VID(pSma->pVnode) == pCfg->dstVgId) {
    // create stable to save tsma result in dstVgId
    SVCreateStbReq pReq = {0};
    pReq.name = pCfg->dstTbName;
    pReq.suid = pCfg->dstTbUid;
    pReq.schemaRow = pCfg->schemaRow;
    pReq.schemaTag = pCfg->schemaTag;

    if (metaCreateSTable(SMA_META(pSma), version, &pReq) < 0) {
      return -1;
    }
  }

  tdTSmaAdd(pSma, 1);
  return 0;
}