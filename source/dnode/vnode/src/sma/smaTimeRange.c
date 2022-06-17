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

#define SMA_STORAGE_MINUTES_MAX  86400
#define SMA_STORAGE_MINUTES_DAY  1440
#define SMA_STORAGE_SPLIT_FACTOR 14400  // least records in tsma file

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

/**
 * @brief create tsma meta and result stable
 *
 * @param pSma
 * @param version
 * @param pMsg
 * @return int32_t
 */
int32_t tdProcessTSmaCreateImpl(SSma *pSma, int64_t version, const char *pMsg) {
  SSmaCfg *pCfg = (SSmaCfg *)pMsg;

  if (TD_VID(pSma->pVnode) == pCfg->dstVgId) {
    // create tsma meta in dstVgId
    if (metaCreateTSma(SMA_META(pSma), version, pCfg) < 0) {
      return -1;
    }

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

  return 0;
}

/**
 * @brief Insert/Update Time-range-wise SMA data.
 *
 * @param pSma
 * @param msg
 * @return int32_t
 */
int32_t tdProcessTSmaInsertImpl(SSma *pSma, int64_t indexUid, const char *msg) {
  const SArray *pDataBlocks = (const SArray *)msg;
  // TODO: destroy SSDataBlocks(msg)
  if (!pDataBlocks) {
    terrno = TSDB_CODE_TSMA_INVALID_PTR;
    smaWarn("vgId:%d, insert tsma data failed since pDataBlocks is NULL", SMA_VID(pSma));
    return terrno;
  }

  if (taosArrayGetSize(pDataBlocks) <= 0) {
    terrno = TSDB_CODE_TSMA_INVALID_PARA;
    smaWarn("vgId:%d, insert tsma data failed since pDataBlocks is empty", SMA_VID(pSma));
    return TSDB_CODE_FAILED;
  }

  if (tdCheckAndInitSmaEnv(pSma, TSDB_SMA_TYPE_TIME_RANGE) != 0) {
    terrno = TSDB_CODE_TSMA_INIT_FAILED;
    return TSDB_CODE_FAILED;
  }

  SSmaEnv      *pEnv = SMA_TSMA_ENV(pSma);
  SSmaStat     *pStat = NULL;
  SSmaStatItem *pItem = NULL;

  if (!pEnv || !(pStat = SMA_ENV_STAT(pEnv))) {
    terrno = TSDB_CODE_TSMA_INVALID_STAT;
    return TSDB_CODE_FAILED;
  }

  tdRefSmaStat(pSma, pStat);
  pItem = &pStat->tsmaStatItem;

  ASSERT(pItem);

  if (!pItem->pTSma) {
    STSma *pTSma = metaGetSmaInfoByIndex(SMA_META(pSma), indexUid);
    if (!pTSma) {
      terrno = TSDB_CODE_TSMA_NO_INDEX_IN_META;
      smaWarn("vgId:%d, tsma insert for smaIndex %" PRIi64 " since %s", SMA_VID(pSma), indexUid, tstrerror(terrno));
      return TSDB_CODE_FAILED;
    }
    pItem->pTSma = pTSma;
    pItem->pTSchema = metaGetTbTSchema(SMA_META(pSma), pTSma->dstTbUid, -1);
    ASSERT(pItem->pTSchema);  // TODO
  }

  ASSERT(pItem->pTSma->indexUid == indexUid);

  SSubmitReq *pSubmitReq = NULL;

  pSubmitReq = tdBlockToSubmit((const SArray *)msg, pItem->pTSchema, true, pItem->pTSma->dstTbUid,
                               pItem->pTSma->dstTbName, pItem->pTSma->dstVgId);

  ASSERT(pSubmitReq);  // TODO

  ASSERT(!strncasecmp("td.tsma.rst.tb", pItem->pTSma->dstTbName, 14));

  SRpcMsg submitReqMsg = {
      .msgType = TDMT_VND_SUBMIT,
      .pCont = pSubmitReq,
      .contLen = ntohl(pSubmitReq->length),
  };

  ASSERT(tmsgPutToQueue(&pSma->pVnode->msgCb, WRITE_QUEUE, &submitReqMsg) == 0);

  tdUnRefSmaStat(pSma, pStat);

  return TSDB_CODE_SUCCESS;
}