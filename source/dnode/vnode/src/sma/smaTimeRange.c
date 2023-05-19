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
    smaWarn("vgId:%d, insert tsma data failed since %s", SMA_VID(pSma), tstrerror(terrno));
  }

  return code;
}

int32_t tdProcessTSmaCreate(SSma *pSma, int64_t version, const char *msg) {
  int32_t code = TSDB_CODE_SUCCESS;

  if ((code = tdProcessTSmaCreateImpl(pSma, version, msg)) < 0) {
    smaWarn("vgId:%d, create tsma failed since %s", SMA_VID(pSma), tstrerror(terrno));
  }
  return code;
}

int32_t smaGetTSmaDays(SVnodeCfg *pCfg, void *pCont, uint32_t contLen, int32_t *days) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tdProcessTSmaGetDaysImpl(pCfg, pCont, contLen, days)) < 0) {
    smaWarn("vgId:%d, get tsma days failed since %s", pCfg->vgId, tstrerror(terrno));
  }
  smaDebug("vgId:%d, get tsma days %d", pCfg->vgId, *days);
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
static int32_t tdProcessTSmaCreateImpl(SSma *pSma, int64_t version, const char *pMsg) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SSmaCfg       *pCfg = (SSmaCfg *)pMsg;
  SName          stbFullName = {0};
  SVCreateStbReq pReq = {0};

  if (TD_VID(pSma->pVnode) == pCfg->dstVgId) {
    // create tsma meta in dstVgId
    if (metaCreateTSma(SMA_META(pSma), version, pCfg) < 0) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    // create stable to save tsma result in dstVgId
    tNameFromString(&stbFullName, pCfg->dstTbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    pReq.name = (char *)tNameGetTableName(&stbFullName);
    pReq.suid = pCfg->dstTbUid;
    pReq.schemaRow = pCfg->schemaRow;
    pReq.schemaTag = pCfg->schemaTag;

    if (metaCreateSTable(SMA_META(pSma), version, &pReq) < 0) {
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

int32_t smaBlockToSubmit(SVnode *pVnode, const SArray *pBlocks, const STSchema *pTSchema,
                         SSchemaWrapper *pTagSchemaWrapper, bool createTb, int64_t suid, const char *stbFullName,
                         SBatchDeleteReq *pDeleteReq, void **ppData, int32_t *pLen) {
  void        *pBuf = NULL;
  int32_t      len = 0;
  SSubmitReq2 *pReq = NULL;
  SArray      *tagArray = NULL;
  SArray      *createTbArray = NULL;
  SArray      *pVals = NULL;

  int32_t sz = taosArrayGetSize(pBlocks);

  if (!(tagArray = taosArrayInit(1, sizeof(STagVal)))) {
    goto _end;
  }

  if (!(createTbArray = taosArrayInit(sz, POINTER_BYTES))) {
    goto _end;
  }

  if (!(pReq = taosMemoryCalloc(1, sizeof(SSubmitReq2)))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  if (!(pReq->aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData)))) {
    goto _end;
  }

  // create table req
  if (createTb) {
    for (int32_t i = 0; i < sz; ++i) {
      SSDataBlock   *pDataBlock = taosArrayGet(pBlocks, i);
      SVCreateTbReq *pCreateTbReq = NULL;
      if (pDataBlock->info.type == STREAM_DELETE_RESULT) {
        taosArrayPush(createTbArray, &pCreateTbReq);
        continue;
      }

      if (!(pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateStbReq)))) {
        goto _end;
      };

      // don't move to the end of loop as to destroy in the end of func when error occur
      taosArrayPush(createTbArray, &pCreateTbReq);

      // set const
      pCreateTbReq->flags = 0;
      pCreateTbReq->type = TSDB_CHILD_TABLE;
      pCreateTbReq->ctb.suid = suid;

      // set super table name
      SName name = {0};
      tNameFromString(&name, stbFullName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
      pCreateTbReq->ctb.stbName = taosStrdup((char *)tNameGetTableName(&name));  // taosStrdup(stbFullName);

      // set tag content
      taosArrayClear(tagArray);
      STagVal tagVal = {
          .cid = taosArrayGetSize(pDataBlock->pDataBlock) + 1,
          .type = TSDB_DATA_TYPE_UBIGINT,
          .i64 = (int64_t)pDataBlock->info.id.groupId,
      };
      taosArrayPush(tagArray, &tagVal);
      pCreateTbReq->ctb.tagNum = taosArrayGetSize(tagArray);

      STag *pTag = NULL;
      tTagNew(tagArray, 1, false, &pTag);
      if (pTag == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        goto _end;
      }
      pCreateTbReq->ctb.pTag = (uint8_t *)pTag;

      // set tag name
      SArray *tagName = taosArrayInit(1, TSDB_COL_NAME_LEN);
      char    tagNameStr[TSDB_COL_NAME_LEN] = {0};
      strcpy(tagNameStr, "group_id");
      taosArrayPush(tagName, tagNameStr);
      pCreateTbReq->ctb.tagName = tagName;

      // set table name
      if (pDataBlock->info.parTbName[0]) {
        pCreateTbReq->name = taosStrdup(pDataBlock->info.parTbName);
      } else {
        pCreateTbReq->name = buildCtbNameByGroupId(stbFullName, pDataBlock->info.id.groupId);
      }
    }
  }

  // SSubmitTbData req
  for (int32_t i = 0; i < sz; ++i) {
    SSDataBlock *pDataBlock = taosArrayGet(pBlocks, i);
    if (pDataBlock->info.type == STREAM_DELETE_RESULT) {
      pDeleteReq->suid = suid;
      pDeleteReq->deleteReqs = taosArrayInit(0, sizeof(SSingleDeleteReq));
      tqBuildDeleteReq(stbFullName, pDataBlock, pDeleteReq, "");
      continue;
    }

    int32_t rows = pDataBlock->info.rows;

    SSubmitTbData tbData = {0};

    if (!(tbData.aRowP = taosArrayInit(rows, sizeof(SRow *)))) {
      goto _end;
    }
    tbData.suid = suid;
    tbData.uid = 0;  // uid is assigned by vnode
    tbData.sver = pTSchema->version;

    if (createTb) {
      tbData.pCreateTbReq = taosArrayGetP(createTbArray, i);
      if (tbData.pCreateTbReq) tbData.flags = SUBMIT_REQ_AUTO_CREATE_TABLE;
    }

    if (!pVals && !(pVals = taosArrayInit(pTSchema->numOfCols, sizeof(SColVal)))) {
      taosArrayDestroy(tbData.aRowP);
      goto _end;
    }

    for (int32_t j = 0; j < rows; ++j) {
      taosArrayClear(pVals);
      for (int32_t k = 0; k < pTSchema->numOfCols; k++) {
        const STColumn  *pCol = &pTSchema->columns[k];
        SColumnInfoData *pColData = taosArrayGet(pDataBlock->pDataBlock, k);
        if (colDataIsNull_s(pColData, j)) {
          SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
          taosArrayPush(pVals, &cv);
        } else {
          void *data = colDataGetData(pColData, j);
          if (IS_STR_DATA_TYPE(pCol->type)) {
            SValue  sv = (SValue){.nData = varDataLen(data), .pData = varDataVal(data)};  // address copy, no value
            SColVal cv = COL_VAL_VALUE(pCol->colId, pCol->type, sv);
            taosArrayPush(pVals, &cv);
          } else {
            SValue sv;
            memcpy(&sv.val, data, tDataTypes[pCol->type].bytes);
            SColVal cv = COL_VAL_VALUE(pCol->colId, pCol->type, sv);
            taosArrayPush(pVals, &cv);
          }
        }
      }
      SRow *pRow = NULL;
      if ((terrno = tRowBuild(pVals, (STSchema *)pTSchema, &pRow)) < 0) {
        tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }
      taosArrayPush(tbData.aRowP, &pRow);
    }

    taosArrayPush(pReq->aSubmitTbData, &tbData);
  }

  // encode
  tEncodeSize(tEncodeSubmitReq, pReq, len, terrno);
  if (TSDB_CODE_SUCCESS == terrno) {
    SEncoder encoder;
    len += sizeof(SSubmitReq2Msg);
    pBuf = rpcMallocCont(len);
    if (NULL == pBuf) {
      goto _end;
    }
    ((SSubmitReq2Msg *)pBuf)->header.vgId = TD_VID(pVnode);
    ((SSubmitReq2Msg *)pBuf)->header.contLen = htonl(len);
    ((SSubmitReq2Msg *)pBuf)->version = htobe64(1);
    tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SSubmitReq2Msg)), len - sizeof(SSubmitReq2Msg));
    if (tEncodeSubmitReq(&encoder, pReq) < 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      /*vError("failed to encode submit req since %s", terrstr());*/
    }
    tEncoderClear(&encoder);
  }
_end:
  taosArrayDestroy(createTbArray);
  taosArrayDestroy(tagArray);
  taosArrayDestroy(pVals);
  if (pReq) {
    tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
    taosMemoryFree(pReq);
  }

  if (terrno != 0) {
    rpcFreeCont(pBuf);
    taosArrayDestroy(pDeleteReq->deleteReqs);
    return TSDB_CODE_FAILED;
  }
  if (ppData) *ppData = pBuf;
  if (pLen) *pLen = len;
  return TSDB_CODE_SUCCESS;
}

/**
 * @brief Insert/Update Time-range-wise SMA data.
 *
 * @param pSma
 * @param msg
 * @return int32_t
 */
static int32_t tdProcessTSmaInsertImpl(SSma *pSma, int64_t indexUid, const char *msg) {
  const SArray *pDataBlocks = (const SArray *)msg;
  // TODO: destroy SSDataBlocks(msg)
  if (!pDataBlocks) {
    terrno = TSDB_CODE_TSMA_INVALID_PTR;
    smaWarn("vgId:%d, insert tsma data failed since pDataBlocks is NULL", SMA_VID(pSma));
    return TSDB_CODE_FAILED;
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

  SSmaEnv   *pEnv = SMA_TSMA_ENV(pSma);
  SSmaStat  *pStat = NULL;
  STSmaStat *pTsmaStat = NULL;

  if (!pEnv || !(pStat = SMA_ENV_STAT(pEnv))) {
    terrno = TSDB_CODE_TSMA_INVALID_ENV;
    return TSDB_CODE_FAILED;
  }

  pTsmaStat = SMA_STAT_TSMA(pStat);

  if (!pTsmaStat->pTSma) {
    STSma *pTSma = metaGetSmaInfoByIndex(SMA_META(pSma), indexUid);
    if (!pTSma) {
      smaError("vgId:%d, failed to get STSma while tsma insert for smaIndex %" PRIi64 " since %s", SMA_VID(pSma),
               indexUid, tstrerror(terrno));
      goto _err;
    }
    pTsmaStat->pTSma = pTSma;
    pTsmaStat->pTSchema = metaGetTbTSchema(SMA_META(pSma), pTSma->dstTbUid, -1, 1);
    if (!pTsmaStat->pTSchema) {
      smaError("vgId:%d, failed to get STSchema while tsma insert for smaIndex %" PRIi64 " since %s", SMA_VID(pSma),
               indexUid, tstrerror(terrno));
      goto _err;
    }
  }

  if (pTsmaStat->pTSma->indexUid != indexUid) {
    terrno = TSDB_CODE_APP_ERROR;
    smaError("vgId:%d, tsma insert for smaIndex %" PRIi64 "(!=%" PRIi64 ") failed since %s", SMA_VID(pSma), indexUid,
             pTsmaStat->pTSma->indexUid, tstrerror(terrno));
    goto _err;
  }

  SBatchDeleteReq deleteReq = {0};
  void           *pSubmitReq = NULL;
  int32_t         contLen = 0;

  if (smaBlockToSubmit(pSma->pVnode, (const SArray *)msg, pTsmaStat->pTSchema, &pTsmaStat->pTSma->schemaTag, true,
                       pTsmaStat->pTSma->dstTbUid, pTsmaStat->pTSma->dstTbName, &deleteReq, &pSubmitReq,
                       &contLen) < 0) {
    smaError("vgId:%d, failed to gen submit msg while tsma insert for smaIndex %" PRIi64 " since %s", SMA_VID(pSma),
             indexUid, tstrerror(terrno));
    goto _err;
  }

  // TODO deleteReq
  taosArrayDestroy(deleteReq.deleteReqs);
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

  if (tmsgPutToQueue(&pSma->pVnode->msgCb, WRITE_QUEUE, &submitReqMsg) < 0) {
    smaError("vgId:%d, failed to put SubmitReq msg while tsma insert for smaIndex %" PRIi64 " since %s", SMA_VID(pSma),
             indexUid, tstrerror(terrno));
    goto _err;
  }

  return TSDB_CODE_SUCCESS;
_err:
  return TSDB_CODE_FAILED;
}
