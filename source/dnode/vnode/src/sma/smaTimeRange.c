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

  return code;
}

int32_t tdProcessTSmaCreate(SSma *pSma, int64_t ver, const char *msg) {
  int32_t code = tdProcessTSmaCreateImpl(pSma, ver, msg);

  return code;
}

int32_t smaGetTSmaDays(SVnodeCfg *pCfg, void *pCont, uint32_t contLen, int32_t *days) {
  int32_t code = tdProcessTSmaGetDaysImpl(pCfg, pCont, contLen, days);

  return code;
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
  int64_t   sInterval = convertTimeFromPrecisionToUnit(tsma.interval, pTsdbCfg->precision, TIME_UNIT_SECOND);
  if (sInterval <= 0) {
    *days = pTsdbCfg->days;
    goto _exit;
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
_exit:
  if (code) {
    smaWarn("vgId:%d, failed at line %d to get tsma days %d since %s", pCfg->vgId, lino, *days, tstrerror(code));
  } else {
    smaDebug("vgId:%d, succeed to get tsma days %d", pCfg->vgId, *days);
  }
  tDecoderClear(&coder);
  return code;
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
    if (metaCreateTSma(SMA_META(pSma), ver, pCfg) < 0) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    // create stable to save tsma result in dstVgId
    tNameFromString(&stbFullName, pCfg->dstTbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    pReq.name = (char *)tNameGetTableName(&stbFullName);
    pReq.suid = pCfg->dstTbUid;
    pReq.schemaRow = pCfg->schemaRow;
    pReq.schemaTag = pCfg->schemaTag;

    if (metaCreateSTable(SMA_META(pSma), ver, &pReq) < 0) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  } else {
    code = terrno = TSDB_CODE_TSMA_INVALID_STAT;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    smaError("vgId:%d, failed at line %d to create sma index %s %" PRIi64 " on stb:%" PRIi64 ", dstSuid:%" PRIi64
             " dstTb:%s dstVg:%d",
             SMA_VID(pSma), lino, pCfg->indexName, pCfg->indexUid, pCfg->tableUid, pCfg->dstTbUid, pReq.name,
             pCfg->dstVgId);
  } else {
    smaDebug("vgId:%d, success to create sma index %s %" PRIi64 " on stb:%" PRIi64 ", dstSuid:%" PRIi64
             " dstTb:%s dstVg:%d",
             SMA_VID(pSma), pCfg->indexName, pCfg->indexUid, pCfg->tableUid, pCfg->dstTbUid, pReq.name, pCfg->dstVgId);
  }

  return code;
}

int32_t smaBlockToSubmit(SVnode *pVnode, const SArray *pBlocks, const STSchema *pTSchema, int64_t suid,
                         const char *stbFullName, SBatchDeleteReq *pDeleteReq, void **ppData, int32_t *pLen) {
  int32_t      code = 0;
  int32_t      lino = 0;
  void        *pBuf = NULL;
  int32_t      len = 0;
  SSubmitReq2 *pReq = NULL;
  SArray      *tagArray = NULL;

  int32_t numOfBlocks = taosArrayGetSize(pBlocks);

  tagArray = taosArrayInit(1, sizeof(STagVal));
  pReq = taosMemoryCalloc(1, sizeof(SSubmitReq2));

  if (!tagArray || !pReq) {
    code = terrno == TSDB_CODE_SUCCESS ? TSDB_CODE_OUT_OF_MEMORY : terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pReq->aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData));
  if (pReq->aSubmitTbData == NULL) {
    code = terrno == TSDB_CODE_SUCCESS ? TSDB_CODE_OUT_OF_MEMORY : terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SHashObj *pTableIndexMap =
      taosHashInit(numOfBlocks, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);

  // SSubmitTbData req
  for (int32_t i = 0; i < numOfBlocks; ++i) {
    SSDataBlock *pDataBlock = taosArrayGet(pBlocks, i);
    if (pDataBlock->info.type == STREAM_DELETE_RESULT) {
      pDeleteReq->suid = suid;
      pDeleteReq->deleteReqs = taosArrayInit(0, sizeof(SSingleDeleteReq));
      code = tqBuildDeleteReq(pVnode->pTq, stbFullName, pDataBlock, pDeleteReq, "", true);
      TSDB_CHECK_CODE(code, lino, _exit);
      continue;
    }

    SSubmitTbData tbData = {.suid = suid, .uid = 0, .sver = pTSchema->version, .flags = SUBMIT_REQ_AUTO_CREATE_TABLE};

    int32_t cid = taosArrayGetSize(pDataBlock->pDataBlock) + 1;
    tbData.pCreateTbReq = buildAutoCreateTableReq(stbFullName, suid, cid, pDataBlock, tagArray, true);

    {
      uint64_t groupId = pDataBlock->info.id.groupId;

      int32_t *index = taosHashGet(pTableIndexMap, &groupId, sizeof(groupId));
      if (index == NULL) {  // no data yet, append it
        code = tqSetDstTableDataPayload(suid, pTSchema, i, pDataBlock, &tbData, "");
        if (code != TSDB_CODE_SUCCESS) {
          continue;
        }

        taosArrayPush(pReq->aSubmitTbData, &tbData);

        int32_t size = (int32_t)taosArrayGetSize(pReq->aSubmitTbData) - 1;
        taosHashPut(pTableIndexMap, &groupId, sizeof(groupId), &size, sizeof(size));
      } else {
        code = tqSetDstTableDataPayload(suid, pTSchema, i, pDataBlock, &tbData, "");
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

  taosHashCleanup(pTableIndexMap);

  // encode
  tEncodeSize(tEncodeSubmitReq, pReq, len, code);
  if (TSDB_CODE_SUCCESS == code) {
    SEncoder encoder;
    len += sizeof(SSubmitReq2Msg);
    if (!(pBuf = rpcMallocCont(len))) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    ((SSubmitReq2Msg *)pBuf)->header.vgId = TD_VID(pVnode);
    ((SSubmitReq2Msg *)pBuf)->header.contLen = htonl(len);
    ((SSubmitReq2Msg *)pBuf)->version = htobe64(1);
    tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SSubmitReq2Msg)), len - sizeof(SSubmitReq2Msg));
    if (tEncodeSubmitReq(&encoder, pReq) < 0) {
      tEncoderClear(&encoder);
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tEncoderClear(&encoder);
  }

_exit:
  taosArrayDestroy(tagArray);
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
  return code;
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
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    SEncoder encoder;
    tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SMsgHead)), len);
    tEncodeSBatchDeleteReq(&encoder, pDelReq);
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

  return code;
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
    terrno = 0;
    STSma *pTSma = metaGetSmaInfoByIndex(SMA_META(pSma), indexUid);
    if (!pTSma) {
      code = terrno ? terrno : TSDB_CODE_TSMA_INVALID_PTR;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    pTsmaStat->pTSma = pTSma;
    pTsmaStat->pTSchema = metaGetTbTSchema(SMA_META(pSma), pTSma->dstTbUid, -1, 1);
    if (!pTsmaStat->pTSchema) {
      code = terrno ? terrno : TSDB_CODE_TSMA_INVALID_PTR;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (ASSERTS(pTsmaStat->pTSma->indexUid == indexUid, "indexUid:%" PRIi64 " != %" PRIi64, pTsmaStat->pTSma->indexUid,
              indexUid)) {
    code = TSDB_CODE_APP_ERROR;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SBatchDeleteReq deleteReq = {0};
  void           *pSubmitReq = NULL;
  int32_t         contLen = 0;

  code = smaBlockToSubmit(pSma->pVnode, (const SArray *)msg, pTsmaStat->pTSchema, pTsmaStat->pTSma->dstTbUid,
                          pTsmaStat->pTSma->dstTbName, &deleteReq, &pSubmitReq, &contLen);
  TSDB_CHECK_CODE(code, lino, _exit);

  if ((terrno = tsmaProcessDelReq(pSma, indexUid, &deleteReq)) != 0) {
    goto _exit;
  }

#if 0
  if (!strncasecmp("td.tsma.rst.tb", pTsmaStat->pTSma->dstTbName, 14)) {
    terrno = TSDB_CODE_APP_ERROR;
    smaError("vgId:%d, tsma insert for smaIndex %" PRIi64 " failed since %s, %s", SMA_VID(pSma), indexUid,
             pTsmaStat->pTSma->indexUid, tstrerror(terrno), pTsmaStat->pTSma->dstTbName);
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
  return code;
}
