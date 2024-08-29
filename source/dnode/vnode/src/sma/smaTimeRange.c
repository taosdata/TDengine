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
#include "tq.h"
#include "tsdb.h"

#define SMA_STORAGE_MINUTES_MAX  86400
#define SMA_STORAGE_MINUTES_DAY  1440
#define SMA_STORAGE_SPLIT_FACTOR 14400  // least records in tsma file

static int32_t tdProcessTSmaCreateImpl(SSma *pSma, int64_t version, const char *pMsg);
static int32_t tdProcessTSmaInsertImpl(SSma *pSma, int64_t indexUid, const char *msg);
static int32_t tdProcessTSmaGetDaysImpl(SVnodeCfg *pCfg, void *pCont, uint32_t contLen, int32_t *days);

int32_t tdProcessTSmaInsert(SSma *pSma, int64_t indexUid, const char *msg) {
  int32_t code = TSDB_CODE_SUCCESS;

  if ((code = tdProcessTSmaInsertImpl(pSma, indexUid, msg)) < 0) {
    smaError("vgId:%d, insert tsma data failed since %s", SMA_VID(pSma), tstrerror(code));
  }

  TAOS_RETURN(code);
}

int32_t tdProcessTSmaCreate(SSma *pSma, int64_t ver, const char *msg) {
  int32_t code = tdProcessTSmaCreateImpl(pSma, ver, msg);

  TAOS_RETURN(code);
}

int32_t smaGetTSmaDays(SVnodeCfg *pCfg, void *pCont, uint32_t contLen, int32_t *days) {
  int32_t code = tdProcessTSmaGetDaysImpl(pCfg, pCont, contLen, days);

  TAOS_RETURN(code);
}

/**
 * @brief Judge the tsma file split days
 *
 * @param pCfg
 * @param pCont
 * @param contLen
 * @param days unit is minute
 * @return int32_t
 */
static int32_t tdProcessTSmaGetDaysImpl(SVnodeCfg *pCfg, void *pCont, uint32_t contLen, int32_t *days) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder coder = {0};
  tDecoderInit(&coder, pCont, contLen);

  STSma tsma = {0};
  if (tDecodeSVCreateTSmaReq(&coder, &tsma) < 0) {
    code = TSDB_CODE_MSG_DECODE_ERROR;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  STsdbCfg *pTsdbCfg = &pCfg->tsdbCfg;
  int64_t   sInterval = -1;
  TAOS_CHECK_EXIT(convertTimeFromPrecisionToUnit(tsma.interval, pTsdbCfg->precision, TIME_UNIT_SECOND, &sInterval));
  if (0 == sInterval) {
    *days = pTsdbCfg->days;
    goto _exit;
  }
  int64_t records = pTsdbCfg->days * 60 / sInterval;
  if (records >= SMA_STORAGE_SPLIT_FACTOR) {
    *days = pTsdbCfg->days;
  } else {
    int64_t mInterval = -1;
    TAOS_CHECK_EXIT(convertTimeFromPrecisionToUnit(tsma.interval, pTsdbCfg->precision, TIME_UNIT_MINUTE, &mInterval));
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
_exit:
  if (code) {
    smaWarn("vgId:%d, failed at line %d to get tsma days %d since %s", pCfg->vgId, lino, *days, tstrerror(code));
  } else {
    smaDebug("vgId:%d, succeed to get tsma days %d", pCfg->vgId, *days);
  }
  tDecoderClear(&coder);
  TAOS_RETURN(code);
}

/**
 * @brief create tsma meta and result stable
 *
 * @param pSma
 * @param version
 * @param pMsg
 * @return int32_t
 */
static int32_t tdProcessTSmaCreateImpl(SSma *pSma, int64_t ver, const char *pMsg) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SSmaCfg       *pCfg = (SSmaCfg *)pMsg;
  SName          stbFullName = {0};
  SVCreateStbReq pReq = {0};

  if (TD_VID(pSma->pVnode) == pCfg->dstVgId) {
    // create tsma meta in dstVgId
    TAOS_CHECK_EXIT(metaCreateTSma(SMA_META(pSma), ver, pCfg));

    // create stable to save tsma result in dstVgId
    (void)tNameFromString(&stbFullName, pCfg->dstTbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    pReq.name = (char *)tNameGetTableName(&stbFullName);
    pReq.suid = pCfg->dstTbUid;
    pReq.schemaRow = pCfg->schemaRow;
    pReq.schemaTag = pCfg->schemaTag;

    TAOS_CHECK_EXIT(metaCreateSTable(SMA_META(pSma), ver, &pReq));
  } else {
    TAOS_CHECK_EXIT(TSDB_CODE_TSMA_INVALID_STAT);
  }

_exit:
  if (code) {
    smaError("vgId:%d, failed at line %d to create sma index %s %" PRIi64 " on stb:%" PRIi64 ", dstSuid:%" PRIi64
             " dstTb:%s dstVg:%d since %s",
             SMA_VID(pSma), lino, pCfg->indexName, pCfg->indexUid, pCfg->tableUid, pCfg->dstTbUid, pReq.name,
             pCfg->dstVgId, tstrerror(code));
  } else {
    smaDebug("vgId:%d, success to create sma index %s %" PRIi64 " on stb:%" PRIi64 ", dstSuid:%" PRIi64
             " dstTb:%s dstVg:%d",
             SMA_VID(pSma), pCfg->indexName, pCfg->indexUid, pCfg->tableUid, pCfg->dstTbUid, pReq.name, pCfg->dstVgId);
  }

  TAOS_RETURN(code);
}

int32_t smaBlockToSubmit(SVnode *pVnode, const SArray *pBlocks, const STSchema *pTSchema, int64_t suid,
                         const char *stbFullName, SBatchDeleteReq *pDeleteReq, void **ppData, int32_t *pLen) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void        *pBuf = NULL;
  int32_t      len = 0;
  SSubmitReq2 *pReq = NULL;
  SArray      *tagArray = NULL;
  SHashObj    *pTableIndexMap = NULL;

  int32_t numOfBlocks = taosArrayGetSize(pBlocks);

  tagArray = taosArrayInit(1, sizeof(STagVal));
  pReq = taosMemoryCalloc(1, sizeof(SSubmitReq2));

  if (!tagArray || !pReq) {
    TAOS_CHECK_EXIT(terrno);
  }

  pReq->aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData));
  if (pReq->aSubmitTbData == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  pTableIndexMap = taosHashInit(numOfBlocks, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pTableIndexMap == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  // SSubmitTbData req
  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock *pDataBlock = taosArrayGet(pBlocks, i);
    if (pDataBlock->info.type == STREAM_DELETE_RESULT) {
      pDeleteReq->suid = suid;
      pDeleteReq->deleteReqs = taosArrayInit(0, sizeof(SSingleDeleteReq));
      TAOS_CHECK_EXIT(tqBuildDeleteReq(pVnode->pTq, stbFullName, pDataBlock, pDeleteReq, "", true));
      continue;
    }

    SSubmitTbData tbData = {.suid = suid, .uid = 0, .sver = pTSchema->version, .flags = SUBMIT_REQ_AUTO_CREATE_TABLE};

    int32_t cid = taosArrayGetSize(pDataBlock->pDataBlock) + 1;

    TAOS_CHECK_EXIT(buildAutoCreateTableReq(stbFullName, suid, cid, pDataBlock, tagArray, true, &tbData.pCreateTbReq));

    {
      uint64_t groupId = pDataBlock->info.id.groupId;

      int32_t *index = taosHashGet(pTableIndexMap, &groupId, sizeof(groupId));
      if (index == NULL) {  // no data yet, append it
        code = tqSetDstTableDataPayload(suid, pTSchema, i, pDataBlock, &tbData, INT64_MIN, "");
        if (code != TSDB_CODE_SUCCESS) {
          continue;
        }

        if( taosArrayPush(pReq->aSubmitTbData, &tbData) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          continue;
        }

        int32_t size = (int32_t)taosArrayGetSize(pReq->aSubmitTbData) - 1;
        TAOS_CHECK_EXIT(taosHashPut(pTableIndexMap, &groupId, sizeof(groupId), &size, sizeof(size)));
      } else {
        code = tqSetDstTableDataPayload(suid, pTSchema, i, pDataBlock, &tbData, INT64_MIN, "");
        if (code != TSDB_CODE_SUCCESS) {
          continue;
        }

        SSubmitTbData *pExisted = taosArrayGet(pReq->aSubmitTbData, *index);
        code = doMergeExistedRows(pExisted, &tbData, "id");
        if (code != TSDB_CODE_SUCCESS) {
          continue;
        }
      }
    }
  }

  // encode
  tEncodeSize(tEncodeSubmitReq, pReq, len, code);
  if (TSDB_CODE_SUCCESS == code) {
    SEncoder encoder;
    len += sizeof(SSubmitReq2Msg);
    if (!(pBuf = rpcMallocCont(len))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    ((SSubmitReq2Msg *)pBuf)->header.vgId = TD_VID(pVnode);
    ((SSubmitReq2Msg *)pBuf)->header.contLen = htonl(len);
    ((SSubmitReq2Msg *)pBuf)->version = htobe64(1);
    tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SSubmitReq2Msg)), len - sizeof(SSubmitReq2Msg));
    if ((code = tEncodeSubmitReq(&encoder, pReq)) < 0) {
      tEncoderClear(&encoder);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tEncoderClear(&encoder);
  }

_exit:
  taosArrayDestroy(tagArray);
  taosHashCleanup(pTableIndexMap);
  if (pReq != NULL) {
    tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
    taosMemoryFree(pReq);
  }

  if (code) {
    rpcFreeCont(pBuf);
    taosArrayDestroy(pDeleteReq->deleteReqs);
    smaWarn("vgId:%d, failed at line %d since %s", TD_VID(pVnode), lino, tstrerror(code));
  } else {
    if (ppData) *ppData = pBuf;
    if (pLen) *pLen = len;
  }
  TAOS_RETURN(code);
}

static int32_t tsmaProcessDelReq(SSma *pSma, int64_t indexUid, SBatchDeleteReq *pDelReq) {
  int32_t code = 0;
  int32_t lino = 0;

  if (taosArrayGetSize(pDelReq->deleteReqs) > 0) {
    int32_t len = 0;
    tEncodeSize(tEncodeSBatchDeleteReq, pDelReq, len, code);
    TSDB_CHECK_CODE(code, lino, _exit);

    void *pBuf = rpcMallocCont(len + sizeof(SMsgHead));
    if (!pBuf) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    SEncoder encoder;
    tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SMsgHead)), len);
    (void)tEncodeSBatchDeleteReq(&encoder, pDelReq);
    tEncoderClear(&encoder);

    ((SMsgHead *)pBuf)->vgId = TD_VID(pSma->pVnode);

    SRpcMsg delMsg = {.msgType = TDMT_VND_BATCH_DEL, .pCont = pBuf, .contLen = len + sizeof(SMsgHead)};
    code = tmsgPutToQueue(&pSma->pVnode->msgCb, WRITE_QUEUE, &delMsg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  taosArrayDestroy(pDelReq->deleteReqs);
  if (code) {
    smaError("vgId:%d, failed at line %d to process delete req for smaIndex %" PRIi64 " since %s", SMA_VID(pSma), lino,
             indexUid, tstrerror(code));
  }

  TAOS_RETURN(code);
}

/**
 * @brief Insert/Update Time-range-wise SMA data.
 *
 * @param pSma
 * @param msg
 * @return int32_t
 */
static int32_t tdProcessTSmaInsertImpl(SSma *pSma, int64_t indexUid, const char *msg) {
  int32_t       code = 0;
  int32_t       lino = 0;
  const SArray *pDataBlocks = (const SArray *)msg;

  if (taosArrayGetSize(pDataBlocks) <= 0) {
    code = TSDB_CODE_TSMA_INVALID_PARA;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (tdCheckAndInitSmaEnv(pSma, TSDB_SMA_TYPE_TIME_RANGE) != 0) {
    code = TSDB_CODE_TSMA_INIT_FAILED;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SSmaEnv   *pEnv = SMA_TSMA_ENV(pSma);
  SSmaStat  *pStat = NULL;
  STSmaStat *pTsmaStat = NULL;

  if (!pEnv || !(pStat = SMA_ENV_STAT(pEnv))) {
    code = TSDB_CODE_TSMA_INVALID_ENV;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pTsmaStat = SMA_STAT_TSMA(pStat);

  if (!pTsmaStat->pTSma) {
    STSma *pTSma = metaGetSmaInfoByIndex(SMA_META(pSma), indexUid);
    if (!pTSma) {
      code = TSDB_CODE_TSMA_INVALID_PTR;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    pTsmaStat->pTSma = pTSma;
    pTsmaStat->pTSchema = metaGetTbTSchema(SMA_META(pSma), pTSma->dstTbUid, -1, 1);
    if (!pTsmaStat->pTSchema) {
      code = TSDB_CODE_TSMA_INVALID_PTR;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (pTsmaStat->pTSma->indexUid != indexUid) {
    code = TSDB_CODE_APP_ERROR;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SBatchDeleteReq deleteReq = {0};
  void           *pSubmitReq = NULL;
  int32_t         contLen = 0;

  code = smaBlockToSubmit(pSma->pVnode, (const SArray *)msg, pTsmaStat->pTSchema, pTsmaStat->pTSma->dstTbUid,
                          pTsmaStat->pTSma->dstTbName, &deleteReq, &pSubmitReq, &contLen);
  TSDB_CHECK_CODE(code, lino, _exit);

  TAOS_CHECK_EXIT(tsmaProcessDelReq(pSma, indexUid, &deleteReq));

#if 0
  if (!strncasecmp("td.tsma.rst.tb", pTsmaStat->pTSma->dstTbName, 14)) {
    code = TSDB_CODE_APP_ERROR;
    smaError("vgId:%d, tsma insert for smaIndex %" PRIi64 " failed since %s, %s", SMA_VID(pSma), indexUid,
             pTsmaStat->pTSma->indexUid, tstrerror(code), pTsmaStat->pTSma->dstTbName);
    goto _err;
  }
#endif

  SRpcMsg submitReqMsg = {
      .msgType = TDMT_VND_SUBMIT,
      .pCont = pSubmitReq,
      .contLen = contLen,
  };

  code = tmsgPutToQueue(&pSma->pVnode->msgCb, WRITE_QUEUE, &submitReqMsg);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    smaError("vgId:%d, %s failed at line %d since %s, smaIndex:%" PRIi64, SMA_VID(pSma), __func__, lino,
             tstrerror(code), indexUid);
  }
  TAOS_RETURN(code);
}
