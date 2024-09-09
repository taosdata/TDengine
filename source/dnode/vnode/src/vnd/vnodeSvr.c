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

#include "audit.h"
#include "cos.h"
#include "monitor.h"
#include "tencode.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tstrbuild.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"

extern taos_counter_t *tsInsertCounter;

static int32_t vnodeProcessCreateStbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterStbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropStbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessCreateTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                       SRpcMsg *pOriginRpc);
static int32_t vnodeProcessAlterTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                     SRpcMsg *pOriginRpc);
static int32_t vnodeProcessSubmitReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                     SRpcMsg *pOriginalMsg);
static int32_t vnodeProcessCreateTSmaReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterConfirmReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterConfigReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropTtlTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessTrimReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessS3MigrateReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDeleteReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                     SRpcMsg *pOriginalMsg);
static int32_t vnodeProcessBatchDeleteReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessCreateIndexReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropIndexReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessCompactVnodeReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessConfigChangeReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessArbCheckSyncReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp);

static int32_t vnodePreCheckAssignedLogSyncd(SVnode *pVnode, char *member0Token, char *member1Token);
static int32_t vnodeCheckAssignedLogSyncd(SVnode *pVnode, char *member0Token, char *member1Token);
static int32_t vnodeProcessFetchTtlExpiredTbs(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);

extern int32_t vnodeProcessKillCompactReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);
extern int32_t vnodeQueryCompactProgress(SVnode *pVnode, SRpcMsg *pMsg);

static int32_t vnodePreprocessCreateTableReq(SVnode *pVnode, SDecoder *pCoder, int64_t btime, int64_t *pUid) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // flags
  if (tDecodeI32v(pCoder, NULL) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // name
  char *name = NULL;
  if (tDecodeCStr(pCoder, &name) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // uid
  int64_t uid = metaGetTableEntryUidByName(pVnode->pMeta, name);
  if (uid == 0) {
    uid = tGenIdPI64();
  }
  *(int64_t *)(pCoder->data + pCoder->pos) = uid;

  // btime
  *(int64_t *)(pCoder->data + pCoder->pos + 8) = btime;

  tEndDecode(pCoder);

_exit:
  if (code) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    vTrace("vgId:%d %s done, table:%s uid generated:%" PRId64, TD_VID(pVnode), __func__, name, uid);
    if (pUid) *pUid = uid;
  }
  return code;
}
static int32_t vnodePreProcessCreateTableMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;

  int64_t  btime = taosGetTimestampMs();
  SDecoder dc = {0};
  int32_t  nReqs;

  tDecoderInit(&dc, (uint8_t *)pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead));
  if (tStartDecode(&dc) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    return code;
  }

  if (tDecodeI32v(&dc, &nReqs) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  for (int32_t iReq = 0; iReq < nReqs; iReq++) {
    code = vnodePreprocessCreateTableReq(pVnode, &dc, btime, NULL);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tEndDecode(&dc);

_exit:
  tDecoderClear(&dc);
  if (code) {
    vError("vgId:%d, %s:%d failed to preprocess submit request since %s, msg type:%s", TD_VID(pVnode), __func__, lino,
           tstrerror(code), TMSG_INFO(pMsg->msgType));
  }
  return code;
}

static int32_t vnodePreProcessAlterTableMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_INVALID_MSG;
  int32_t lino = 0;

  SDecoder dc = {0};
  tDecoderInit(&dc, (uint8_t *)pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead));

  SVAlterTbReq vAlterTbReq = {0};
  int64_t      ctimeMs = taosGetTimestampMs();
  if (tDecodeSVAlterTbReqSetCtime(&dc, &vAlterTbReq, ctimeMs) < 0) {
    goto _exit;
  }

  code = 0;

_exit:
  tDecoderClear(&dc);
  if (code) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    vTrace("vgId:%d %s done, table:%s ctimeMs generated:%" PRId64, TD_VID(pVnode), __func__, vAlterTbReq.tbName,
           ctimeMs);
  }
  return code;
}

static int32_t vnodePreProcessDropTtlMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = TSDB_CODE_INVALID_MSG;
  int32_t lino = 0;

  SMsgHead *pContOld = pMsg->pCont;
  int32_t   reqLenOld = pMsg->contLen - sizeof(SMsgHead);

  SArray *tbUids = NULL;
  int64_t timestampMs = 0;

  SVDropTtlTableReq ttlReq = {0};
  if (tDeserializeSVDropTtlTableReq((char *)pContOld + sizeof(SMsgHead), reqLenOld, &ttlReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  {  // find expired uids
    tbUids = taosArrayInit(8, sizeof(tb_uid_t));
    if (tbUids == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    timestampMs = (int64_t)ttlReq.timestampSec * 1000;
    code = metaTtlFindExpired(pVnode->pMeta, timestampMs, tbUids, ttlReq.ttlDropMaxCount);
    if (code != 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    ttlReq.nUids = taosArrayGetSize(tbUids);
    ttlReq.pTbUids = tbUids;
  }

  if (ttlReq.nUids == 0) {
    code = TSDB_CODE_MSG_PREPROCESSED;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  {  // prepare new content
    int32_t reqLenNew = tSerializeSVDropTtlTableReq(NULL, 0, &ttlReq);
    int32_t contLenNew = reqLenNew + sizeof(SMsgHead);

    SMsgHead *pContNew = rpcMallocCont(contLenNew);
    if (pContNew == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    (void)tSerializeSVDropTtlTableReq((char *)pContNew + sizeof(SMsgHead), reqLenNew, &ttlReq);
    pContNew->contLen = htonl(reqLenNew);
    pContNew->vgId = pContOld->vgId;

    rpcFreeCont(pContOld);
    pMsg->pCont = pContNew;
    pMsg->contLen = contLenNew;
  }

  code = 0;

_exit:
  taosArrayDestroy(tbUids);

  if (code && code != TSDB_CODE_MSG_PREPROCESSED) {
    vError("vgId:%d, %s:%d failed to preprocess drop ttl request since %s, msg type:%s", TD_VID(pVnode), __func__, lino,
           tstrerror(code), TMSG_INFO(pMsg->msgType));
  } else {
    vTrace("vgId:%d, %s done, timestampSec:%d, nUids:%d", TD_VID(pVnode), __func__, ttlReq.timestampSec, ttlReq.nUids);
  }

  return code;
}

extern int64_t tsMaxKeyByPrecision[];
static int32_t vnodePreProcessSubmitTbData(SVnode *pVnode, SDecoder *pCoder, int64_t btimeMs, int64_t ctimeMs) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SSubmitTbData submitTbData;
  uint8_t       version;
  if (tDecodeI32v(pCoder, &submitTbData.flags) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  version = (submitTbData.flags >> 8) & 0xff;
  submitTbData.flags = submitTbData.flags & 0xff;

  int64_t uid;
  if (submitTbData.flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    code = vnodePreprocessCreateTableReq(pVnode, pCoder, btimeMs, &uid);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // submit data
  if (tDecodeI64(pCoder, &submitTbData.suid) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (submitTbData.flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    *(int64_t *)(pCoder->data + pCoder->pos) = uid;
    pCoder->pos += sizeof(int64_t);
  } else {
    if (tDecodeI64(pCoder, &submitTbData.uid) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (tDecodeI32v(pCoder, &submitTbData.sver) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // scan and check
  TSKEY now = btimeMs;
  if (pVnode->config.tsdbCfg.precision == TSDB_TIME_PRECISION_MICRO) {
    now *= 1000;
  } else if (pVnode->config.tsdbCfg.precision == TSDB_TIME_PRECISION_NANO) {
    now *= 1000000;
  }

  int32_t keep = pVnode->config.tsdbCfg.keep2;
  /*
  int32_t nlevel = tfsGetLevel(pVnode->pTfs);
  if (nlevel > 1 && tsS3Enabled) {
    if (nlevel == 3) {
      keep = pVnode->config.tsdbCfg.keep1;
    } else if (nlevel == 2) {
      keep = pVnode->config.tsdbCfg.keep0;
    }
  }
  */

  TSKEY minKey = now - tsTickPerMin[pVnode->config.tsdbCfg.precision] * keep;
  TSKEY maxKey = tsMaxKeyByPrecision[pVnode->config.tsdbCfg.precision];
  if (submitTbData.flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    uint64_t nColData;
    if (tDecodeU64v(pCoder, &nColData) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }

    SColData colData = {0};
    pCoder->pos += tGetColData(version, pCoder->data + pCoder->pos, &colData);
    if (colData.flag != HAS_VALUE) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }

    for (int32_t iRow = 0; iRow < colData.nVal; iRow++) {
      if (((TSKEY *)colData.pData)[iRow] < minKey || ((TSKEY *)colData.pData)[iRow] > maxKey) {
        code = TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE;
        goto _exit;
      }
    }

    for (uint64_t i = 1; i < nColData; i++) {
      pCoder->pos += tGetColData(version, pCoder->data + pCoder->pos, &colData);
    }
  } else {
    uint64_t nRow;
    if (tDecodeU64v(pCoder, &nRow) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }

    for (int32_t iRow = 0; iRow < nRow; ++iRow) {
      SRow *pRow = (SRow *)(pCoder->data + pCoder->pos);
      pCoder->pos += pRow->len;

      if (pRow->ts < minKey || pRow->ts > maxKey) {
        code = TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE;
        goto _exit;
      }
    }
  }

  if (!tDecodeIsEnd(pCoder)) {
    *(int64_t *)(pCoder->data + pCoder->pos) = ctimeMs;
    pCoder->pos += sizeof(int64_t);
  }

  tEndDecode(pCoder);

_exit:
  return code;
}
static int32_t vnodePreProcessSubmitMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;

  SDecoder *pCoder = &(SDecoder){0};

  if (taosHton64(((SSubmitReq2Msg *)pMsg->pCont)->version) != 1) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tDecoderInit(pCoder, (uint8_t *)pMsg->pCont + sizeof(SSubmitReq2Msg), pMsg->contLen - sizeof(SSubmitReq2Msg));

  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  uint64_t nSubmitTbData;
  if (tDecodeU64v(pCoder, &nSubmitTbData) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  int64_t btimeMs = taosGetTimestampMs();
  int64_t ctimeMs = btimeMs;
  for (int32_t i = 0; i < nSubmitTbData; i++) {
    code = vnodePreProcessSubmitTbData(pVnode, pCoder, btimeMs, ctimeMs);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tEndDecode(pCoder);

_exit:
  tDecoderClear(pCoder);
  if (code) {
    vError("vgId:%d, %s:%d failed to preprocess submit request since %s, msg type:%s", TD_VID(pVnode), __func__, lino,
           tstrerror(code), TMSG_INFO(pMsg->msgType));
  }
  return code;
}

static int32_t vnodePreProcessDeleteMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  int32_t    size;
  int32_t    ret;
  uint8_t   *pCont;
  SEncoder  *pCoder = &(SEncoder){0};
  SDeleteRes res = {0};

  SReadHandle handle = {.vnode = pVnode, .pMsgCb = &pVnode->msgCb, .skipRollup = 1};
  initStorageAPI(&handle.api);

  code = qWorkerProcessDeleteMsg(&handle, pVnode->pQuery, pMsg, &res);
  if (code) goto _exit;

  res.ctimeMs = taosGetTimestampMs();
  // malloc and encode
  tEncodeSize(tEncodeDeleteRes, &res, size, ret);
  pCont = rpcMallocCont(size + sizeof(SMsgHead));

  ((SMsgHead *)pCont)->contLen = size + sizeof(SMsgHead);
  ((SMsgHead *)pCont)->vgId = TD_VID(pVnode);

  tEncoderInit(pCoder, pCont + sizeof(SMsgHead), size);
  (void)tEncodeDeleteRes(pCoder, &res);
  tEncoderClear(pCoder);

  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = pCont;
  pMsg->contLen = size + sizeof(SMsgHead);

  taosArrayDestroy(res.uidList);

_exit:
  return code;
}

static int32_t vnodePreProcessBatchDeleteMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;

  int64_t         ctimeMs = taosGetTimestampMs();
  SBatchDeleteReq pReq = {0};
  SDecoder       *pCoder = &(SDecoder){0};

  tDecoderInit(pCoder, (uint8_t *)pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead));

  if (tDecodeSBatchDeleteReqSetCtime(pCoder, &pReq, ctimeMs) < 0) {
    code = TSDB_CODE_INVALID_MSG;
  }

  tDecoderClear(pCoder);
  taosArrayDestroy(pReq.deleteReqs);

  if (code) {
    vError("vgId:%d %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    vTrace("vgId:%d %s done, ctimeMs generated:%" PRId64, TD_VID(pVnode), __func__, ctimeMs);
  }
  return code;
}

static int32_t vnodePreProcessArbCheckSyncMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  SVArbCheckSyncReq syncReq = {0};

  if (tDeserializeSVArbCheckSyncReq((char *)pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead),
                                    &syncReq) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  (void)vnodePreCheckAssignedLogSyncd(pVnode, syncReq.member0Token, syncReq.member1Token);
  int32_t code = terrno;
  tFreeSVArbCheckSyncReq(&syncReq);

  return code;
}

int32_t vnodePreProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  switch (pMsg->msgType) {
    case TDMT_VND_CREATE_TABLE: {
      code = vnodePreProcessCreateTableMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_ALTER_TABLE: {
      code = vnodePreProcessAlterTableMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_FETCH_TTL_EXPIRED_TBS:
    case TDMT_VND_DROP_TTL_TABLE: {
      code = vnodePreProcessDropTtlMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_SUBMIT: {
      code = vnodePreProcessSubmitMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_DELETE: {
      code = vnodePreProcessDeleteMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_BATCH_DEL: {
      code = vnodePreProcessBatchDeleteMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_ARB_CHECK_SYNC: {
      code = vnodePreProcessArbCheckSyncMsg(pVnode, pMsg);
    } break;
    default:
      break;
  }

  if (code && code != TSDB_CODE_MSG_PREPROCESSED) {
    vError("vgId:%d, failed to preprocess write request since %s, msg type:%s", TD_VID(pVnode), tstrerror(code),
           TMSG_INFO(pMsg->msgType));
  }
  return code;
}

int32_t vnodeProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg, int64_t ver, SRpcMsg *pRsp) {
  int32_t code = 0;
  void   *ptr = NULL;
  void   *pReq;
  int32_t len;

  (void)taosThreadMutexLock(&pVnode->mutex);
  if (pVnode->disableWrite) {
    (void)taosThreadMutexUnlock(&pVnode->mutex);
    vError("vgId:%d write is disabled for snapshot, version:%" PRId64, TD_VID(pVnode), ver);
    return TSDB_CODE_VND_WRITE_DISABLED;
  }
  (void)taosThreadMutexUnlock(&pVnode->mutex);

  if (ver <= pVnode->state.applied) {
    vError("vgId:%d, duplicate write request. ver: %" PRId64 ", applied: %" PRId64 "", TD_VID(pVnode), ver,
           pVnode->state.applied);
    return terrno = TSDB_CODE_VND_DUP_REQUEST;
  }

  vDebug("vgId:%d, start to process write request %s, index:%" PRId64 ", applied:%" PRId64 ", state.applyTerm:%" PRId64
         ", conn.applyTerm:%" PRId64,
         TD_VID(pVnode), TMSG_INFO(pMsg->msgType), ver, pVnode->state.applied, pVnode->state.applyTerm,
         pMsg->info.conn.applyTerm);

  if (!(pVnode->state.applyTerm <= pMsg->info.conn.applyTerm)) {
    return terrno = TSDB_CODE_INTERNAL_ERROR;
  }

  if (!(pVnode->state.applied + 1 == ver)) {
    return terrno = TSDB_CODE_INTERNAL_ERROR;
  }

  atomic_store_64(&pVnode->state.applied, ver);
  atomic_store_64(&pVnode->state.applyTerm, pMsg->info.conn.applyTerm);

  if (!syncUtilUserCommit(pMsg->msgType)) goto _exit;

  // skip header
  pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  len = pMsg->contLen - sizeof(SMsgHead);
  bool needCommit = false;

  switch (pMsg->msgType) {
    /* META */
    case TDMT_VND_CREATE_STB:
      if (vnodeProcessCreateStbReq(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_ALTER_STB:
      if (vnodeProcessAlterStbReq(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_DROP_STB:
      if (vnodeProcessDropStbReq(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_CREATE_TABLE:
      if (vnodeProcessCreateTbReq(pVnode, ver, pReq, len, pRsp, pMsg) < 0) goto _err;
      break;
    case TDMT_VND_ALTER_TABLE:
      if (vnodeProcessAlterTbReq(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_DROP_TABLE:
      if (vnodeProcessDropTbReq(pVnode, ver, pReq, len, pRsp, pMsg) < 0) goto _err;
      break;
    case TDMT_VND_DROP_TTL_TABLE:
      if (vnodeProcessDropTtlTbReq(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_FETCH_TTL_EXPIRED_TBS:
      if (vnodeProcessFetchTtlExpiredTbs(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_TRIM:
      if (vnodeProcessTrimReq(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_S3MIGRATE:
      if (vnodeProcessS3MigrateReq(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_CREATE_SMA:
      if (vnodeProcessCreateTSmaReq(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
    /* TSDB */
    case TDMT_VND_SUBMIT:
      if (vnodeProcessSubmitReq(pVnode, ver, pMsg->pCont, pMsg->contLen, pRsp, pMsg) < 0) goto _err;
      break;
    case TDMT_VND_DELETE:
      if (vnodeProcessDeleteReq(pVnode, ver, pReq, len, pRsp, pMsg) < 0) goto _err;
      break;
    case TDMT_VND_BATCH_DEL:
      if (vnodeProcessBatchDeleteReq(pVnode, ver, pReq, len, pRsp) < 0) goto _err;
      break;
    /* TQ */
    case TDMT_VND_TMQ_SUBSCRIBE:
      if (tqProcessSubscribeReq(pVnode->pTq, ver, pReq, len) < 0) {
        goto _err;
      }
      break;
    case TDMT_VND_TMQ_DELETE_SUB:
      if (tqProcessDeleteSubReq(pVnode->pTq, ver, pMsg->pCont, pMsg->contLen) < 0) {
        goto _err;
      }
      break;
    case TDMT_VND_TMQ_COMMIT_OFFSET:
      if (tqProcessOffsetCommitReq(pVnode->pTq, ver, pReq, len) < 0) {
        goto _err;
      }
      break;
    case TDMT_VND_TMQ_ADD_CHECKINFO:
      if (tqProcessAddCheckInfoReq(pVnode->pTq, ver, pReq, len) < 0) {
        goto _err;
      }
      break;
    case TDMT_VND_TMQ_DEL_CHECKINFO:
      if (tqProcessDelCheckInfoReq(pVnode->pTq, ver, pReq, len) < 0) {
        goto _err;
      }
      break;
    case TDMT_STREAM_TASK_DEPLOY: {
      int32_t code = tqProcessTaskDeployReq(pVnode->pTq, ver, pReq, len);
      if (code != TSDB_CODE_SUCCESS) {
        terrno = code;
        goto _err;
      }
    } break;
    case TDMT_STREAM_TASK_DROP: {
      if (tqProcessTaskDropReq(pVnode->pTq, pMsg->pCont, pMsg->contLen) < 0) {
        goto _err;
      }
    } break;
    case TDMT_STREAM_TASK_UPDATE_CHKPT: {
      if (tqProcessTaskUpdateCheckpointReq(pVnode->pTq, pMsg->pCont, pMsg->contLen) < 0) {
        goto _err;
      }
    } break;
    case TDMT_STREAM_CONSEN_CHKPT: {
      if (pVnode->restored) {
        (void)tqProcessTaskConsenChkptIdReq(pVnode->pTq, pMsg);
      }
    } break;
    case TDMT_STREAM_TASK_PAUSE: {
      if (pVnode->restored && vnodeIsLeader(pVnode) &&
          tqProcessTaskPauseReq(pVnode->pTq, ver, pMsg->pCont, pMsg->contLen) < 0) {
        goto _err;
      }
    } break;
    case TDMT_STREAM_TASK_RESUME: {
      if (pVnode->restored && vnodeIsLeader(pVnode) &&
          tqProcessTaskResumeReq(pVnode->pTq, ver, pMsg->pCont, pMsg->contLen) < 0) {
        goto _err;
      }
    } break;
    case TDMT_VND_STREAM_TASK_RESET: {
      if (pVnode->restored && vnodeIsLeader(pVnode)) {
        (void)tqProcessTaskResetReq(pVnode->pTq, pMsg);
      }
    } break;
    case TDMT_VND_ALTER_CONFIRM:
      needCommit = pVnode->config.hashChange;
      if (vnodeProcessAlterConfirmReq(pVnode, ver, pReq, len, pRsp) < 0) {
        goto _err;
      }
      break;
    case TDMT_VND_ALTER_CONFIG:
      vnodeProcessAlterConfigReq(pVnode, ver, pReq, len, pRsp);
      break;
    case TDMT_VND_COMMIT:
      needCommit = true;
      break;
    case TDMT_VND_CREATE_INDEX:
      vnodeProcessCreateIndexReq(pVnode, ver, pReq, len, pRsp);
      break;
    case TDMT_VND_DROP_INDEX:
      vnodeProcessDropIndexReq(pVnode, ver, pReq, len, pRsp);
      break;
    case TDMT_VND_STREAM_CHECK_POINT_SOURCE:
      tqProcessTaskCheckPointSourceReq(pVnode->pTq, pMsg, pRsp);
      break;
    case TDMT_VND_STREAM_TASK_UPDATE:
      tqProcessTaskUpdateReq(pVnode->pTq, pMsg);
      break;
    case TDMT_VND_COMPACT:
      vnodeProcessCompactVnodeReq(pVnode, ver, pReq, len, pRsp);
      goto _exit;
    case TDMT_SYNC_CONFIG_CHANGE:
      vnodeProcessConfigChangeReq(pVnode, ver, pReq, len, pRsp);
      break;
#ifdef TD_ENTERPRISE
    case TDMT_VND_KILL_COMPACT:
      vnodeProcessKillCompactReq(pVnode, ver, pReq, len, pRsp);
      break;
#endif
    /* ARB */
    case TDMT_VND_ARB_CHECK_SYNC:
      vnodeProcessArbCheckSyncReq(pVnode, pReq, len, pRsp);
      break;
    default:
      vError("vgId:%d, unprocessed msg, %d", TD_VID(pVnode), pMsg->msgType);
      return TSDB_CODE_INVALID_MSG;
  }

  vTrace("vgId:%d, process %s request, code:0x%x index:%" PRId64, TD_VID(pVnode), TMSG_INFO(pMsg->msgType), pRsp->code,
         ver);

  (void)walApplyVer(pVnode->pWal, ver);

  code = tqPushMsg(pVnode->pTq, pMsg->msgType);
  if (code) {
    vError("vgId:%d, failed to push msg to TQ since %s", TD_VID(pVnode), tstrerror(terrno));
    return code;
  }

  // commit if need
  if (needCommit) {
    vInfo("vgId:%d, commit at version %" PRId64, TD_VID(pVnode), ver);
    code = vnodeAsyncCommit(pVnode);
    if (code) {
      vError("vgId:%d, failed to vnode async commit since %s.", TD_VID(pVnode), tstrerror(terrno));
      goto _err;
    }

    // start a new one
    code = vnodeBegin(pVnode);
    if (code) {
      vError("vgId:%d, failed to begin vnode since %s.", TD_VID(pVnode), tstrerror(terrno));
      goto _err;
    }
  }

_exit:
  return 0;

_err:
  vError("vgId:%d, process %s request failed since %s, ver:%" PRId64, TD_VID(pVnode), TMSG_INFO(pMsg->msgType),
         tstrerror(terrno), ver);
  return code;
}

int32_t vnodePreprocessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  if (TDMT_SCH_QUERY != pMsg->msgType && TDMT_SCH_MERGE_QUERY != pMsg->msgType) {
    return 0;
  }

  return qWorkerPreprocessQueryMsg(pVnode->pQuery, pMsg, TDMT_SCH_QUERY == pMsg->msgType);
}

int32_t vnodeProcessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo) {
  vTrace("message in vnode query queue is processing");
  if ((pMsg->msgType == TDMT_SCH_QUERY || pMsg->msgType == TDMT_VND_TMQ_CONSUME) &&
      !syncIsReadyForRead(pVnode->sync)) {
    vnodeRedirectRpcMsg(pVnode, pMsg, terrno);
    return 0;
  }

  if (pMsg->msgType == TDMT_VND_TMQ_CONSUME && !pVnode->restored) {
    vnodeRedirectRpcMsg(pVnode, pMsg, TSDB_CODE_SYN_RESTORING);
    return 0;
  }

  SReadHandle handle = {.vnode = pVnode, .pMsgCb = &pVnode->msgCb, .pWorkerCb = pInfo->workerCb};
  initStorageAPI(&handle.api);

  switch (pMsg->msgType) {
    case TDMT_SCH_QUERY:
    case TDMT_SCH_MERGE_QUERY:
      return qWorkerProcessQueryMsg(&handle, pVnode->pQuery, pMsg, 0);
    case TDMT_SCH_QUERY_CONTINUE:
      return qWorkerProcessCQueryMsg(&handle, pVnode->pQuery, pMsg, 0);
    case TDMT_VND_TMQ_CONSUME:
      return tqProcessPollReq(pVnode->pTq, pMsg);
    case TDMT_VND_TMQ_CONSUME_PUSH:
      return tqProcessPollPush(pVnode->pTq, pMsg);
    default:
      vError("unknown msg type:%d in query queue", pMsg->msgType);
      return TSDB_CODE_APP_ERROR;
  }
}

int32_t vnodeProcessFetchMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo) {
  vTrace("vgId:%d, msg:%p in fetch queue is processing", pVnode->config.vgId, pMsg);
  if ((pMsg->msgType == TDMT_SCH_FETCH || pMsg->msgType == TDMT_VND_TABLE_META || pMsg->msgType == TDMT_VND_TABLE_CFG ||
       pMsg->msgType == TDMT_VND_BATCH_META) &&
      !syncIsReadyForRead(pVnode->sync)) {
    vnodeRedirectRpcMsg(pVnode, pMsg, terrno);
    return 0;
  }

  switch (pMsg->msgType) {
    case TDMT_SCH_FETCH:
    case TDMT_SCH_MERGE_FETCH:
      return qWorkerProcessFetchMsg(pVnode, pVnode->pQuery, pMsg, 0);
    case TDMT_SCH_FETCH_RSP:
      return qWorkerProcessRspMsg(pVnode, pVnode->pQuery, pMsg, 0);
    // case TDMT_SCH_CANCEL_TASK:
    //   return qWorkerProcessCancelMsg(pVnode, pVnode->pQuery, pMsg, 0);
    case TDMT_SCH_DROP_TASK:
      return qWorkerProcessDropMsg(pVnode, pVnode->pQuery, pMsg, 0);
    case TDMT_SCH_TASK_NOTIFY:
      return qWorkerProcessNotifyMsg(pVnode, pVnode->pQuery, pMsg, 0);
    case TDMT_SCH_QUERY_HEARTBEAT:
      return qWorkerProcessHbMsg(pVnode, pVnode->pQuery, pMsg, 0);
    case TDMT_VND_TABLE_META:
      return vnodeGetTableMeta(pVnode, pMsg, true);
    case TDMT_VND_TABLE_CFG:
      return vnodeGetTableCfg(pVnode, pMsg, true);
    case TDMT_VND_BATCH_META:
      return vnodeGetBatchMeta(pVnode, pMsg);
#ifdef TD_ENTERPRISE
    case TDMT_VND_QUERY_COMPACT_PROGRESS:
      return vnodeQueryCompactProgress(pVnode, pMsg);
#endif
      //    case TDMT_VND_TMQ_CONSUME:
      //      return tqProcessPollReq(pVnode->pTq, pMsg);
    case TDMT_VND_TMQ_VG_WALINFO:
      return tqProcessVgWalInfoReq(pVnode->pTq, pMsg);
    case TDMT_VND_TMQ_VG_COMMITTEDINFO:
      return tqProcessVgCommittedInfoReq(pVnode->pTq, pMsg);
    case TDMT_VND_TMQ_SEEK:
      return tqProcessSeekReq(pVnode->pTq, pMsg);

    default:
      vError("unknown msg type:%d in fetch queue", pMsg->msgType);
      return TSDB_CODE_APP_ERROR;
  }
}

int32_t vnodeProcessStreamMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo) {
  vTrace("vgId:%d, msg:%p in fetch queue is processing", pVnode->config.vgId, pMsg);
  if ((pMsg->msgType == TDMT_SCH_FETCH || pMsg->msgType == TDMT_VND_TABLE_META || pMsg->msgType == TDMT_VND_TABLE_CFG ||
       pMsg->msgType == TDMT_VND_BATCH_META) &&
      !syncIsReadyForRead(pVnode->sync)) {
    vnodeRedirectRpcMsg(pVnode, pMsg, terrno);
    return 0;
  }

  switch (pMsg->msgType) {
    case TDMT_STREAM_TASK_RUN:
      return tqProcessTaskRunReq(pVnode->pTq, pMsg);
    case TDMT_STREAM_TASK_DISPATCH:
      return tqProcessTaskDispatchReq(pVnode->pTq, pMsg);
    case TDMT_STREAM_TASK_DISPATCH_RSP:
      return tqProcessTaskDispatchRsp(pVnode->pTq, pMsg);
    case TDMT_VND_STREAM_TASK_CHECK:
      return tqProcessTaskCheckReq(pVnode->pTq, pMsg);
    case TDMT_VND_STREAM_TASK_CHECK_RSP:
      return tqProcessTaskCheckRsp(pVnode->pTq, pMsg);
    case TDMT_STREAM_RETRIEVE:
      return tqProcessTaskRetrieveReq(pVnode->pTq, pMsg);
    case TDMT_STREAM_RETRIEVE_RSP:
      return tqProcessTaskRetrieveRsp(pVnode->pTq, pMsg);
    case TDMT_VND_STREAM_SCAN_HISTORY:
      return tqProcessTaskScanHistory(pVnode->pTq, pMsg);
    case TDMT_STREAM_TASK_CHECKPOINT_READY:
      return tqProcessTaskCheckpointReadyMsg(pVnode->pTq, pMsg);
    case TDMT_STREAM_TASK_CHECKPOINT_READY_RSP:
      return tqProcessTaskCheckpointReadyRsp(pVnode->pTq, pMsg);
    case TDMT_STREAM_RETRIEVE_TRIGGER:
      return tqProcessTaskRetrieveTriggerReq(pVnode->pTq, pMsg);
    case TDMT_STREAM_RETRIEVE_TRIGGER_RSP:
      return tqProcessTaskRetrieveTriggerRsp(pVnode->pTq, pMsg);
    case TDMT_MND_STREAM_HEARTBEAT_RSP:
      return tqProcessStreamHbRsp(pVnode->pTq, pMsg);
    case TDMT_MND_STREAM_REQ_CHKPT_RSP:
      return tqProcessStreamReqCheckpointRsp(pVnode->pTq, pMsg);
    case TDMT_VND_GET_STREAM_PROGRESS:
      return tqStreamProgressRetrieveReq(pVnode->pTq, pMsg);
    case TDMT_MND_STREAM_CHKPT_REPORT_RSP:
      return tqProcessTaskChkptReportRsp(pVnode->pTq, pMsg);
    default:
      vError("unknown msg type:%d in stream queue", pMsg->msgType);
      return TSDB_CODE_APP_ERROR;
  }
}

void smaHandleRes(void *pVnode, int64_t smaId, const SArray *data) {
  (void)tdProcessTSmaInsert(((SVnode *)pVnode)->pSma, smaId, (const char *)data);
}

void vnodeUpdateMetaRsp(SVnode *pVnode, STableMetaRsp *pMetaRsp) {
  if (NULL == pMetaRsp) {
    return;
  }

  strcpy(pMetaRsp->dbFName, pVnode->config.dbname);
  pMetaRsp->dbId = pVnode->config.dbId;
  pMetaRsp->vgId = TD_VID(pVnode);
  pMetaRsp->precision = pVnode->config.tsdbCfg.precision;
}

extern int32_t vnodeAsyncRetention(SVnode *pVnode, int64_t now);

static int32_t vnodeProcessTrimReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t     code = 0;
  SVTrimDbReq trimReq = {0};

  // decode
  if (tDeserializeSVTrimDbReq(pReq, len, &trimReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  vInfo("vgId:%d, trim vnode request will be processed, time:%d", pVnode->config.vgId, trimReq.timestamp);

  code = vnodeAsyncRetention(pVnode, trimReq.timestamp);

_exit:
  return code;
}

extern int32_t vnodeAsyncS3Migrate(SVnode *pVnode, int64_t now);

static int32_t vnodeProcessS3MigrateReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t          code = 0;
  SVS3MigrateDbReq s3migrateReq = {0};

  // decode
  if (tDeserializeSVS3MigrateDbReq(pReq, len, &s3migrateReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  vInfo("vgId:%d, s3migrate vnode request will be processed, time:%d", pVnode->config.vgId, s3migrateReq.timestamp);

  code = vnodeAsyncS3Migrate(pVnode, s3migrateReq.timestamp);

_exit:
  return code;
}

static int32_t vnodeProcessDropTtlTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int               ret = 0;
  SVDropTtlTableReq ttlReq = {0};
  if (tDeserializeSVDropTtlTableReq(pReq, len, &ttlReq) != 0) {
    ret = TSDB_CODE_INVALID_MSG;
    goto end;
  }

  if (ttlReq.nUids != taosArrayGetSize(ttlReq.pTbUids)) {
    ret = TSDB_CODE_INVALID_MSG;
    goto end;
  }

  if (ttlReq.nUids != 0) {
    vInfo("vgId:%d, drop ttl table req will be processed, time:%d, ntbUids:%d", pVnode->config.vgId,
          ttlReq.timestampSec, ttlReq.nUids);
  }

  if (ttlReq.nUids > 0) {
    int32_t code = metaDropTables(pVnode->pMeta, ttlReq.pTbUids);
    if (code) return code;

    (void)tqUpdateTbUidList(pVnode->pTq, ttlReq.pTbUids, false);
  }

end:
  taosArrayDestroy(ttlReq.pTbUids);
  return ret;
}

static int32_t vnodeProcessFetchTtlExpiredTbs(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t                 code = -1;
  SMetaReader             mr = {0};
  SVDropTtlTableReq       ttlReq = {0};
  SVFetchTtlExpiredTbsRsp rsp = {0};
  SEncoder                encoder = {0};
  SArray                 *pNames = NULL;
  pRsp->msgType = TDMT_VND_FETCH_TTL_EXPIRED_TBS_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  if (tDeserializeSVDropTtlTableReq(pReq, len, &ttlReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _end;
  }

  if (!(ttlReq.nUids == taosArrayGetSize(ttlReq.pTbUids))) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _end;
  }

  tb_uid_t    suid;
  char        ctbName[TSDB_TABLE_NAME_LEN];
  SVDropTbReq expiredTb = {.igNotExists = true};
  metaReaderDoInit(&mr, pVnode->pMeta, 0);
  rsp.vgId = TD_VID(pVnode);
  rsp.pExpiredTbs = taosArrayInit(ttlReq.nUids, sizeof(SVDropTbReq));
  if (!rsp.pExpiredTbs) goto _end;

  pNames = taosArrayInit(ttlReq.nUids, TSDB_TABLE_NAME_LEN);
  if (!pNames) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }
  char buf[TSDB_TABLE_NAME_LEN];
  for (int32_t i = 0; i < ttlReq.nUids; ++i) {
    tb_uid_t *uid = taosArrayGet(ttlReq.pTbUids, i);
    expiredTb.suid = *uid;
    terrno = metaReaderGetTableEntryByUid(&mr, *uid);
    if (terrno < 0) goto _end;
    strncpy(buf, mr.me.name, TSDB_TABLE_NAME_LEN);
    void *p = taosArrayPush(pNames, buf);
    if (p == NULL) {
      goto _end;
    }

    expiredTb.name = p;
    if (mr.me.type == TSDB_CHILD_TABLE) {
      expiredTb.suid = mr.me.ctbEntry.suid;
    }

    if (taosArrayPush(rsp.pExpiredTbs, &expiredTb) == NULL) {
      goto _end;
    }
  }

  int32_t ret = 0;
  tEncodeSize(tEncodeVFetchTtlExpiredTbsRsp, &rsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  if (pRsp->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto _end;
  }
  tEncoderInit(&encoder, pRsp->pCont, pRsp->contLen);
  terrno = tEncodeVFetchTtlExpiredTbsRsp(&encoder, &rsp);
  tEncoderClear(&encoder);

  if (terrno == 0) code = 0;
_end:
  metaReaderClear(&mr);
  tFreeFetchTtlExpiredTbsRsp(&rsp);
  taosArrayDestroy(ttlReq.pTbUids);
  if (pNames) taosArrayDestroy(pNames);
  pRsp->code = terrno;
  return code;
}

static int32_t vnodeProcessCreateStbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t        code = 0;
  SVCreateStbReq req = {0};
  SDecoder       coder;

  pRsp->msgType = TDMT_VND_CREATE_STB_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  // decode and process req
  tDecoderInit(&coder, pReq, len);

  code = tDecodeSVCreateStbReq(&coder, &req);
  if (code) {
    pRsp->code = code;
    goto _err;
  }

  code = metaCreateSTable(pVnode->pMeta, ver, &req);
  if (code) {
    pRsp->code = code;
    goto _err;
  }

  if ((code = tdProcessRSmaCreate(pVnode->pSma, &req)) < 0) {
    pRsp->code = code;
    goto _err;
  }

  tDecoderClear(&coder);
  return 0;

_err:
  tDecoderClear(&coder);
  return code;
}

static int32_t vnodeProcessCreateTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                       SRpcMsg *pOriginRpc) {
  SDecoder           decoder = {0};
  SEncoder           encoder = {0};
  int32_t            rcode = 0;
  SVCreateTbBatchReq req = {0};
  SVCreateTbReq     *pCreateReq;
  SVCreateTbBatchRsp rsp = {0};
  SVCreateTbRsp      cRsp = {0};
  char               tbName[TSDB_TABLE_FNAME_LEN];
  STbUidStore       *pStore = NULL;
  SArray            *tbUids = NULL;
  SArray            *tbNames = NULL;

  pRsp->msgType = TDMT_VND_CREATE_TABLE_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  // decode
  tDecoderInit(&decoder, pReq, len);
  if (tDecodeSVCreateTbBatchReq(&decoder, &req) < 0) {
    rcode = -1;
    terrno = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  rsp.pArray = taosArrayInit(req.nReqs, sizeof(cRsp));
  tbUids = taosArrayInit(req.nReqs, sizeof(int64_t));
  tbNames = taosArrayInit(req.nReqs, sizeof(char *));
  if (rsp.pArray == NULL || tbUids == NULL || tbNames == NULL) {
    rcode = -1;
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // loop to create table
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    memset(&cRsp, 0, sizeof(cRsp));

    if (tsEnableAudit && tsEnableAuditCreateTable) {
      char *str = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN);
      if (str == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        rcode = -1;
        goto _exit;
      }
      strcpy(str, pCreateReq->name);
      if (taosArrayPush(tbNames, &str) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        rcode = -1;
        goto _exit;
      }
    }

    // validate hash
    sprintf(tbName, "%s.%s", pVnode->config.dbname, pCreateReq->name);
    if (vnodeValidateTableHash(pVnode, tbName) < 0) {
      cRsp.code = TSDB_CODE_VND_HASH_MISMATCH;
      if (taosArrayPush(rsp.pArray, &cRsp) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        rcode = -1;
        goto _exit;
      }
      vError("vgId:%d create-table:%s failed due to hash value mismatch", TD_VID(pVnode), tbName);
      continue;
    }

    // do create table
    if (metaCreateTable(pVnode->pMeta, ver, pCreateReq, &cRsp.pMeta) < 0) {
      if (pCreateReq->flags & TD_CREATE_IF_NOT_EXISTS && terrno == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
        cRsp.code = TSDB_CODE_SUCCESS;
      } else {
        cRsp.code = terrno;
      }
    } else {
      cRsp.code = TSDB_CODE_SUCCESS;
      (void)tdFetchTbUidList(pVnode->pSma, &pStore, pCreateReq->ctb.suid, pCreateReq->uid);
      if (taosArrayPush(tbUids, &pCreateReq->uid) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        rcode = -1;
        goto _exit;
      }
      vnodeUpdateMetaRsp(pVnode, cRsp.pMeta);
    }

    if (taosArrayPush(rsp.pArray, &cRsp) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      rcode = -1;
      goto _exit;
    }
  }

  vDebug("vgId:%d, add %d new created tables into query table list", TD_VID(pVnode), (int32_t)taosArrayGetSize(tbUids));
  (void)tqUpdateTbUidList(pVnode->pTq, tbUids, true);
  if (tdUpdateTbUidList(pVnode->pSma, pStore, true) < 0) {
    goto _exit;
  }
  (void)tdUidStoreFree(pStore);

  // prepare rsp
  int32_t ret = 0;
  tEncodeSize(tEncodeSVCreateTbBatchRsp, &rsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  if (pRsp->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    rcode = -1;
    goto _exit;
  }
  tEncoderInit(&encoder, pRsp->pCont, pRsp->contLen);
  (void)tEncodeSVCreateTbBatchRsp(&encoder, &rsp);

  if (tsEnableAudit && tsEnableAuditCreateTable) {
    int64_t clusterId = pVnode->config.syncCfg.nodeInfo[0].clusterId;

    SName name = {0};
    (void)tNameFromString(&name, pVnode->config.dbname, T_NAME_ACCT | T_NAME_DB);

    SStringBuilder sb = {0};
    for (int32_t i = 0; i < tbNames->size; i++) {
      char **key = (char **)taosArrayGet(tbNames, i);
      taosStringBuilderAppendStringLen(&sb, *key, strlen(*key));
      if (i < tbNames->size - 1) {
        taosStringBuilderAppendChar(&sb, ',');
      }
      // taosMemoryFreeClear(*key);
    }

    size_t len = 0;
    char  *keyJoined = taosStringBuilderGetResult(&sb, &len);

    if (pOriginRpc->info.conn.user != NULL && strlen(pOriginRpc->info.conn.user) > 0) {
      auditAddRecord(pOriginRpc, clusterId, "createTable", name.dbname, "", keyJoined, len);
    }

    taosStringBuilderDestroy(&sb);
  }

_exit:
  tDeleteSVCreateTbBatchReq(&req);
  taosArrayDestroyEx(rsp.pArray, tFreeSVCreateTbRsp);
  taosArrayDestroy(tbUids);
  tDecoderClear(&decoder);
  tEncoderClear(&encoder);
  taosArrayDestroyP(tbNames, taosMemoryFree);
  return rcode;
}

static int32_t vnodeProcessAlterStbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t        code = 0;
  SVCreateStbReq req = {0};
  SDecoder       dc = {0};

  pRsp->msgType = TDMT_VND_ALTER_STB_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  tDecoderInit(&dc, pReq, len);

  // decode req
  code = tDecodeSVCreateStbReq(&dc, &req);
  if (code) {
    tDecoderClear(&dc);
    return code;
  }

  code = metaAlterSTable(pVnode->pMeta, ver, &req);
  if (code) {
    pRsp->code = code;
    tDecoderClear(&dc);
    return code;
  }

  tDecoderClear(&dc);

  return 0;
}

static int32_t vnodeProcessDropStbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVDropStbReq req = {0};
  int32_t      rcode = TSDB_CODE_SUCCESS;
  SDecoder     decoder = {0};
  SArray      *tbUidList = NULL;

  pRsp->msgType = TDMT_VND_CREATE_STB_RSP;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  // decode request
  tDecoderInit(&decoder, pReq, len);
  if (tDecodeSVDropStbReq(&decoder, &req) < 0) {
    rcode = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  // process request
  tbUidList = taosArrayInit(8, sizeof(int64_t));
  if (tbUidList == NULL) goto _exit;
  if (metaDropSTable(pVnode->pMeta, ver, &req, tbUidList) < 0) {
    rcode = terrno;
    goto _exit;
  }

  if (tqUpdateTbUidList(pVnode->pTq, tbUidList, false) < 0) {
    rcode = terrno;
    goto _exit;
  }

  if (tdProcessRSmaDrop(pVnode->pSma, &req) < 0) {
    rcode = terrno;
    goto _exit;
  }

  // return rsp
_exit:
  if (tbUidList) taosArrayDestroy(tbUidList);
  pRsp->code = rcode;
  tDecoderClear(&decoder);
  return 0;
}

static int32_t vnodeProcessAlterTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVAlterTbReq  vAlterTbReq = {0};
  SVAlterTbRsp  vAlterTbRsp = {0};
  SDecoder      dc = {0};
  int32_t       rcode = 0;
  int32_t       ret;
  SEncoder      ec = {0};
  STableMetaRsp vMetaRsp = {0};

  pRsp->msgType = TDMT_VND_ALTER_TABLE_RSP;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;
  pRsp->code = TSDB_CODE_SUCCESS;

  tDecoderInit(&dc, pReq, len);

  // decode
  if (tDecodeSVAlterTbReq(&dc, &vAlterTbReq) < 0) {
    vAlterTbRsp.code = TSDB_CODE_INVALID_MSG;
    tDecoderClear(&dc);
    rcode = -1;
    goto _exit;
  }

  // process
  if (metaAlterTable(pVnode->pMeta, ver, &vAlterTbReq, &vMetaRsp) < 0) {
    vAlterTbRsp.code = terrno;
    tDecoderClear(&dc);
    rcode = -1;
    goto _exit;
  }
  tDecoderClear(&dc);

  if (NULL != vMetaRsp.pSchemas) {
    vnodeUpdateMetaRsp(pVnode, &vMetaRsp);
    vAlterTbRsp.pMeta = &vMetaRsp;
  }

_exit:
  tEncodeSize(tEncodeSVAlterTbRsp, &vAlterTbRsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  tEncoderInit(&ec, pRsp->pCont, pRsp->contLen);
  (void)tEncodeSVAlterTbRsp(&ec, &vAlterTbRsp);
  tEncoderClear(&ec);
  if (vMetaRsp.pSchemas) {
    taosMemoryFree(vMetaRsp.pSchemas);
    taosMemoryFree(vMetaRsp.pSchemaExt);
  }
  return 0;
}

static int32_t vnodeProcessDropTbReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                     SRpcMsg *pOriginRpc) {
  SVDropTbBatchReq req = {0};
  SVDropTbBatchRsp rsp = {0};
  SDecoder         decoder = {0};
  SEncoder         encoder = {0};
  int32_t          ret;
  SArray          *tbUids = NULL;
  STbUidStore     *pStore = NULL;
  SArray          *tbNames = NULL;

  pRsp->msgType = TDMT_VND_DROP_TABLE_RSP;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;
  pRsp->code = TSDB_CODE_SUCCESS;

  // decode req
  tDecoderInit(&decoder, pReq, len);
  ret = tDecodeSVDropTbBatchReq(&decoder, &req);
  if (ret < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    pRsp->code = terrno;
    goto _exit;
  }

  // process req
  tbUids = taosArrayInit(req.nReqs, sizeof(int64_t));
  rsp.pArray = taosArrayInit(req.nReqs, sizeof(SVDropTbRsp));
  tbNames = taosArrayInit(req.nReqs, sizeof(char *));
  if (tbUids == NULL || rsp.pArray == NULL || tbNames == NULL) goto _exit;

  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    SVDropTbReq *pDropTbReq = req.pReqs + iReq;
    SVDropTbRsp  dropTbRsp = {0};
    tb_uid_t     tbUid = 0;

    /* code */
    ret = metaDropTable(pVnode->pMeta, ver, pDropTbReq, tbUids, &tbUid);
    if (ret < 0) {
      if (pDropTbReq->igNotExists && terrno == TSDB_CODE_TDB_TABLE_NOT_EXIST) {
        dropTbRsp.code = TSDB_CODE_SUCCESS;
      } else {
        dropTbRsp.code = terrno;
      }
    } else {
      dropTbRsp.code = TSDB_CODE_SUCCESS;
      if (tbUid > 0) (void)tdFetchTbUidList(pVnode->pSma, &pStore, pDropTbReq->suid, tbUid);
    }

    if (taosArrayPush(rsp.pArray, &dropTbRsp) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      pRsp->code = terrno;
      goto _exit;
    }

    if (tsEnableAuditCreateTable) {
      char *str = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN);
      strcpy(str, pDropTbReq->name);
      if (taosArrayPush(tbNames, &str) == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        pRsp->code = terrno;
        goto _exit;
      }
    }
  }

  (void)tqUpdateTbUidList(pVnode->pTq, tbUids, false);
  (void)tdUpdateTbUidList(pVnode->pSma, pStore, false);

  if (tsEnableAuditCreateTable) {
    int64_t clusterId = pVnode->config.syncCfg.nodeInfo[0].clusterId;

    SName name = {0};
    (void)tNameFromString(&name, pVnode->config.dbname, T_NAME_ACCT | T_NAME_DB);

    SStringBuilder sb = {0};
    for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
      char **key = (char **)taosArrayGet(tbNames, iReq);
      taosStringBuilderAppendStringLen(&sb, *key, strlen(*key));
      if (iReq < req.nReqs - 1) {
        taosStringBuilderAppendChar(&sb, ',');
      }
      taosMemoryFreeClear(*key);
    }

    size_t len = 0;
    char  *keyJoined = taosStringBuilderGetResult(&sb, &len);

    if (pOriginRpc->info.conn.user != NULL && strlen(pOriginRpc->info.conn.user) > 0) {
      auditAddRecord(pOriginRpc, clusterId, "dropTable", name.dbname, "", keyJoined, len);
    }

    taosStringBuilderDestroy(&sb);
  }

_exit:
  taosArrayDestroy(tbUids);
  (void)tdUidStoreFree(pStore);
  tDecoderClear(&decoder);
  tEncodeSize(tEncodeSVDropTbBatchRsp, &rsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  tEncoderInit(&encoder, pRsp->pCont, pRsp->contLen);
  (void)tEncodeSVDropTbBatchRsp(&encoder, &rsp);
  tEncoderClear(&encoder);
  taosArrayDestroy(rsp.pArray);
  taosArrayDestroy(tbNames);
  return 0;
}

#ifdef BUILD_NO_CALL
static int32_t vnodeDebugPrintSingleSubmitMsg(SMeta *pMeta, SSubmitBlk *pBlock, SSubmitMsgIter *msgIter,
                                              const char *tags) {
  SSubmitBlkIter blkIter = {0};
  STSchema      *pSchema = NULL;
  tb_uid_t       suid = 0;
  STSRow        *row = NULL;
  int32_t        rv = -1;

  tInitSubmitBlkIter(msgIter, pBlock, &blkIter);
  if (blkIter.row == NULL) return 0;

  int32_t code = metaGetTbTSchemaNotNull(pMeta, msgIter->suid, TD_ROW_SVER(blkIter.row), 1, &pSchema);  // TODO: use the real schema
  if (TSDB_CODE_SUCCESS != code) {
    printf("%s:%d no valid schema\n", tags, __LINE__);
    return code;
  }

  suid = msgIter->suid;
  rv = TD_ROW_SVER(blkIter.row);

  char __tags[128] = {0};
  snprintf(__tags, 128, "%s: uid %" PRIi64 " ", tags, msgIter->uid);
  while ((row = tGetSubmitBlkNext(&blkIter))) {
    tdSRowPrint(row, pSchema, __tags);
  }

  taosMemoryFreeClear(pSchema);

  return TSDB_CODE_SUCCESS;
}
#endif
typedef struct SSubmitReqConvertCxt {
  SSubmitMsgIter msgIter;
  SSubmitBlk    *pBlock;
  SSubmitBlkIter blkIter;
  STSRow        *pRow;
  STSRowIter     rowIter;
  SSubmitTbData *pTbData;
  STSchema      *pTbSchema;
  SArray        *pColValues;
} SSubmitReqConvertCxt;

static int32_t vnodeResetTableCxt(SMeta *pMeta, SSubmitReqConvertCxt *pCxt) {
  taosMemoryFreeClear(pCxt->pTbSchema);
  int32_t code = metaGetTbTSchemaNotNull(pMeta, pCxt->msgIter.suid > 0 ? pCxt->msgIter.suid : pCxt->msgIter.uid,
                                     pCxt->msgIter.sversion, 1, &pCxt->pTbSchema);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  tdSTSRowIterInit(&pCxt->rowIter, pCxt->pTbSchema);

  tDestroySubmitTbData(pCxt->pTbData, TSDB_MSG_FLG_ENCODE);
  if (NULL == pCxt->pTbData) {
    pCxt->pTbData = taosMemoryCalloc(1, sizeof(SSubmitTbData));
    if (NULL == pCxt->pTbData) {
      return terrno;
    }
  }
  pCxt->pTbData->flags = 0;
  pCxt->pTbData->suid = pCxt->msgIter.suid;
  pCxt->pTbData->uid = pCxt->msgIter.uid;
  pCxt->pTbData->sver = pCxt->msgIter.sversion;
  pCxt->pTbData->pCreateTbReq = NULL;
  pCxt->pTbData->aRowP = taosArrayInit(128, POINTER_BYTES);
  if (NULL == pCxt->pTbData->aRowP) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  taosArrayDestroy(pCxt->pColValues);
  pCxt->pColValues = taosArrayInit(pCxt->pTbSchema->numOfCols, sizeof(SColVal));
  if (NULL == pCxt->pColValues) {
    return terrno;
  }
  for (int32_t i = 0; i < pCxt->pTbSchema->numOfCols; ++i) {
    SColVal val = COL_VAL_NONE(pCxt->pTbSchema->columns[i].colId, pCxt->pTbSchema->columns[i].type);
    if (taosArrayPush(pCxt->pColValues, &val) == NULL) {
      return terrno;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void vnodeDestroySubmitReqConvertCxt(SSubmitReqConvertCxt *pCxt) {
  taosMemoryFreeClear(pCxt->pTbSchema);
  tDestroySubmitTbData(pCxt->pTbData, TSDB_MSG_FLG_ENCODE);
  taosMemoryFreeClear(pCxt->pTbData);
  taosArrayDestroy(pCxt->pColValues);
}

static int32_t vnodeCellValConvertToColVal(STColumn *pCol, SCellVal *pCellVal, SColVal *pColVal) {
  if (tdValTypeIsNone(pCellVal->valType)) {
    pColVal->flag = CV_FLAG_NONE;
    return TSDB_CODE_SUCCESS;
  }

  if (tdValTypeIsNull(pCellVal->valType)) {
    pColVal->flag = CV_FLAG_NULL;
    return TSDB_CODE_SUCCESS;
  }

  if (IS_VAR_DATA_TYPE(pCol->type)) {
    pColVal->value.nData = varDataLen(pCellVal->val);
    pColVal->value.pData = (uint8_t *)varDataVal(pCellVal->val);
  } else if (TSDB_DATA_TYPE_FLOAT == pCol->type) {
    float f = GET_FLOAT_VAL(pCellVal->val);
    memcpy(&pColVal->value.val, &f, sizeof(f));
  } else if (TSDB_DATA_TYPE_DOUBLE == pCol->type) {
    pColVal->value.val = *(int64_t *)pCellVal->val;
  } else {
    GET_TYPED_DATA(pColVal->value.val, int64_t, pCol->type, pCellVal->val);
  }

  pColVal->flag = CV_FLAG_VALUE;
  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeTSRowConvertToColValArray(SSubmitReqConvertCxt *pCxt) {
  int32_t code = TSDB_CODE_SUCCESS;
  tdSTSRowIterReset(&pCxt->rowIter, pCxt->pRow);
  for (int32_t i = 0; TSDB_CODE_SUCCESS == code && i < pCxt->pTbSchema->numOfCols; ++i) {
    STColumn *pCol = pCxt->pTbSchema->columns + i;
    SCellVal  cellVal = {0};
    if (!tdSTSRowIterFetch(&pCxt->rowIter, pCol->colId, pCol->type, &cellVal)) {
      break;
    }
    code = vnodeCellValConvertToColVal(pCol, &cellVal, (SColVal *)taosArrayGet(pCxt->pColValues, i));
  }
  return code;
}

static int32_t vnodeDecodeCreateTbReq(SSubmitReqConvertCxt *pCxt) {
  if (pCxt->msgIter.schemaLen <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  pCxt->pTbData->pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
  if (NULL == pCxt->pTbData->pCreateTbReq) {
    return terrno;
  }

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (uint8_t *)pCxt->pBlock->data, pCxt->msgIter.schemaLen);
  int32_t code = tDecodeSVCreateTbReq(&decoder, pCxt->pTbData->pCreateTbReq);
  tDecoderClear(&decoder);

  return code;
}

static int32_t vnodeSubmitReqConvertToSubmitReq2(SVnode *pVnode, SSubmitReq *pReq, SSubmitReq2 *pReq2) {
  pReq2->aSubmitTbData = taosArrayInit(128, sizeof(SSubmitTbData));
  if (NULL == pReq2->aSubmitTbData) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SSubmitReqConvertCxt cxt = {0};

  int32_t code = tInitSubmitMsgIter(pReq, &cxt.msgIter);
  while (TSDB_CODE_SUCCESS == code) {
    code = tGetSubmitMsgNext(&cxt.msgIter, &cxt.pBlock);
    if (TSDB_CODE_SUCCESS == code) {
      if (NULL == cxt.pBlock) {
        break;
      }
      code = vnodeResetTableCxt(pVnode->pMeta, &cxt);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = tInitSubmitBlkIter(&cxt.msgIter, cxt.pBlock, &cxt.blkIter);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = vnodeDecodeCreateTbReq(&cxt);
    }
    while (TSDB_CODE_SUCCESS == code && (cxt.pRow = tGetSubmitBlkNext(&cxt.blkIter)) != NULL) {
      code = vnodeTSRowConvertToColValArray(&cxt);
      if (TSDB_CODE_SUCCESS == code) {
        SRow **pNewRow = taosArrayReserve(cxt.pTbData->aRowP, 1);
        code = tRowBuild(cxt.pColValues, cxt.pTbSchema, pNewRow);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = (NULL == taosArrayPush(pReq2->aSubmitTbData, cxt.pTbData) ? TSDB_CODE_OUT_OF_MEMORY : TSDB_CODE_SUCCESS);
    }
    if (TSDB_CODE_SUCCESS == code) {
      taosMemoryFreeClear(cxt.pTbData);
    }
  }

  vnodeDestroySubmitReqConvertCxt(&cxt);
  return code;
}

static int32_t vnodeRebuildSubmitReqMsg(SSubmitReq2 *pSubmitReq, void **ppMsg) {
  int32_t  code = TSDB_CODE_SUCCESS;
  char    *pMsg = NULL;
  uint32_t msglen = 0;
  tEncodeSize(tEncodeSubmitReq, pSubmitReq, msglen, code);
  if (TSDB_CODE_SUCCESS == code) {
    pMsg = taosMemoryMalloc(msglen);
    if (NULL == pMsg) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    SEncoder encoder;
    tEncoderInit(&encoder, (uint8_t *)pMsg, msglen);
    code = tEncodeSubmitReq(&encoder, pSubmitReq);
    tEncoderClear(&encoder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *ppMsg = pMsg;
  }
  return code;
}

static int32_t vnodeProcessSubmitReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                     SRpcMsg *pOriginalMsg) {
  int32_t code = 0;
  terrno = 0;

  SSubmitReq2 *pSubmitReq = &(SSubmitReq2){0};
  SSubmitRsp2 *pSubmitRsp = &(SSubmitRsp2){0};
  SArray      *newTbUids = NULL;
  int32_t      ret;
  SEncoder     ec = {0};

  pRsp->code = TSDB_CODE_SUCCESS;

  void           *pAllocMsg = NULL;
  SSubmitReq2Msg *pMsg = (SSubmitReq2Msg *)pReq;
  if (0 == pMsg->version) {
    code = vnodeSubmitReqConvertToSubmitReq2(pVnode, (SSubmitReq *)pMsg, pSubmitReq);
    if (TSDB_CODE_SUCCESS == code) {
      code = vnodeRebuildSubmitReqMsg(pSubmitReq, &pReq);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pAllocMsg = pReq;
    }
    if (TSDB_CODE_SUCCESS != code) {
      goto _exit;
    }
  } else {
    // decode
    pReq = POINTER_SHIFT(pReq, sizeof(SSubmitReq2Msg));
    len -= sizeof(SSubmitReq2Msg);
    SDecoder dc = {0};
    tDecoderInit(&dc, pReq, len);
    if (tDecodeSubmitReq(&dc, pSubmitReq) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
    tDecoderClear(&dc);
  }

  // scan
  TSKEY now = taosGetTimestamp(pVnode->config.tsdbCfg.precision);
  TSKEY minKey = now - tsTickPerMin[pVnode->config.tsdbCfg.precision] * pVnode->config.tsdbCfg.keep2;
  TSKEY maxKey = tsMaxKeyByPrecision[pVnode->config.tsdbCfg.precision];
  for (int32_t i = 0; i < TARRAY_SIZE(pSubmitReq->aSubmitTbData); ++i) {
    SSubmitTbData *pSubmitTbData = taosArrayGet(pSubmitReq->aSubmitTbData, i);

    if (pSubmitTbData->pCreateTbReq && pSubmitTbData->pCreateTbReq->uid == 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }

    if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      if (TARRAY_SIZE(pSubmitTbData->aCol) <= 0) {
        code = TSDB_CODE_INVALID_MSG;
        goto _exit;
      }

      SColData *colDataArr = TARRAY_DATA(pSubmitTbData->aCol);
      SRowKey   lastKey;
      tColDataArrGetRowKey(colDataArr, TARRAY_SIZE(pSubmitTbData->aCol), 0, &lastKey);
      for (int32_t iRow = 1; iRow < colDataArr[0].nVal; iRow++) {
        SRowKey key;
        tColDataArrGetRowKey(TARRAY_DATA(pSubmitTbData->aCol), TARRAY_SIZE(pSubmitTbData->aCol), iRow, &key);
        if (tRowKeyCompare(&lastKey, &key) >= 0) {
          code = TSDB_CODE_INVALID_MSG;
          vError("vgId:%d %s failed 1 since %s, version:%" PRId64, TD_VID(pVnode), __func__, tstrerror(terrno), ver);
          goto _exit;
        }
      }
    } else {
      int32_t nRow = TARRAY_SIZE(pSubmitTbData->aRowP);
      SRow  **aRow = (SRow **)TARRAY_DATA(pSubmitTbData->aRowP);
      SRowKey lastRowKey;
      for (int32_t iRow = 0; iRow < nRow; ++iRow) {
        if (aRow[iRow]->ts < minKey || aRow[iRow]->ts > maxKey) {
          code = TSDB_CODE_INVALID_MSG;
          vError("vgId:%d %s failed 2 since %s, version:%" PRId64, TD_VID(pVnode), __func__, tstrerror(code), ver);
          goto _exit;
        }
        if (iRow == 0) {
          tRowGetKey(aRow[iRow], &lastRowKey);
        } else {
          SRowKey rowKey;
          tRowGetKey(aRow[iRow], &rowKey);

          if (tRowKeyCompare(&lastRowKey, &rowKey) >= 0) {
            code = TSDB_CODE_INVALID_MSG;
            vError("vgId:%d %s failed 3 since %s, version:%" PRId64, TD_VID(pVnode), __func__, tstrerror(code), ver);
            goto _exit;
          }
          lastRowKey = rowKey;
        }
      }
    }
  }

  for (int32_t i = 0; i < TARRAY_SIZE(pSubmitReq->aSubmitTbData); ++i) {
    SSubmitTbData *pSubmitTbData = taosArrayGet(pSubmitReq->aSubmitTbData, i);

    if (pSubmitTbData->pCreateTbReq) {
      pSubmitTbData->uid = pSubmitTbData->pCreateTbReq->uid;
    } else {
      SMetaInfo info = {0};

      code = metaGetInfo(pVnode->pMeta, pSubmitTbData->uid, &info, NULL);
      if (code) {
        code = TSDB_CODE_TDB_TABLE_NOT_EXIST;
        vWarn("vgId:%d, table uid:%" PRId64 " not exists", TD_VID(pVnode), pSubmitTbData->uid);
        goto _exit;
      }

      if (info.suid != pSubmitTbData->suid) {
        code = TSDB_CODE_INVALID_MSG;
        goto _exit;
      }

      if (info.suid) {
        (void)metaGetInfo(pVnode->pMeta, info.suid, &info, NULL);
      }

      if (pSubmitTbData->sver != info.skmVer) {
        code = TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER;
        goto _exit;
      }
    }

    if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      int32_t   nColData = TARRAY_SIZE(pSubmitTbData->aCol);
      SColData *aColData = (SColData *)TARRAY_DATA(pSubmitTbData->aCol);

      if (nColData <= 0) {
        code = TSDB_CODE_INVALID_MSG;
        goto _exit;
      }

      if (aColData[0].cid != PRIMARYKEY_TIMESTAMP_COL_ID || aColData[0].type != TSDB_DATA_TYPE_TIMESTAMP ||
          aColData[0].nVal <= 0) {
        code = TSDB_CODE_INVALID_MSG;
        goto _exit;
      }

      for (int32_t j = 1; j < nColData; j++) {
        if (aColData[j].nVal != aColData[0].nVal) {
          code = TSDB_CODE_INVALID_MSG;
          goto _exit;
        }
      }
    }
  }

  vDebug("vgId:%d, submit block size %d", TD_VID(pVnode), (int32_t)taosArrayGetSize(pSubmitReq->aSubmitTbData));

  // loop to handle
  for (int32_t i = 0; i < TARRAY_SIZE(pSubmitReq->aSubmitTbData); ++i) {
    SSubmitTbData *pSubmitTbData = taosArrayGet(pSubmitReq->aSubmitTbData, i);

    // create table
    if (pSubmitTbData->pCreateTbReq) {
      // alloc if need
      if (pSubmitRsp->aCreateTbRsp == NULL &&
          (pSubmitRsp->aCreateTbRsp = taosArrayInit(TARRAY_SIZE(pSubmitReq->aSubmitTbData), sizeof(SVCreateTbRsp))) ==
              NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }

      SVCreateTbRsp *pCreateTbRsp = taosArrayReserve(pSubmitRsp->aCreateTbRsp, 1);

      // create table
      if (metaCreateTable(pVnode->pMeta, ver, pSubmitTbData->pCreateTbReq, &pCreateTbRsp->pMeta) == 0) {
        // create table success

        if (newTbUids == NULL &&
            (newTbUids = taosArrayInit(TARRAY_SIZE(pSubmitReq->aSubmitTbData), sizeof(int64_t))) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _exit;
        }

        if (taosArrayPush(newTbUids, &pSubmitTbData->uid) == NULL) {
          code = terrno;
          goto _exit;
        }

        if (pCreateTbRsp->pMeta) {
          vnodeUpdateMetaRsp(pVnode, pCreateTbRsp->pMeta);
        }
      } else {  // create table failed
        if (terrno != TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
          code = terrno;
          vError("vgId:%d failed to create table:%s, code:%s", TD_VID(pVnode), pSubmitTbData->pCreateTbReq->name,
                 tstrerror(terrno));
          goto _exit;
        }
        terrno = 0;
        pSubmitTbData->uid = pSubmitTbData->pCreateTbReq->uid;  // update uid if table exist for using below
      }
    }

    // insert data
    int32_t affectedRows;
    code = tsdbInsertTableData(pVnode->pTsdb, ver, pSubmitTbData, &affectedRows);
    if (code) goto _exit;

    code = metaUpdateChangeTimeWithLock(pVnode->pMeta, pSubmitTbData->uid, pSubmitTbData->ctimeMs);
    if (code) goto _exit;

    pSubmitRsp->affectedRows += affectedRows;
  }

  // update the affected table uid list
  if (taosArrayGetSize(newTbUids) > 0) {
    vDebug("vgId:%d, add %d table into query table list in handling submit", TD_VID(pVnode),
           (int32_t)taosArrayGetSize(newTbUids));
    (void)tqUpdateTbUidList(pVnode->pTq, newTbUids, true);
  }

_exit:
  // message
  pRsp->code = code;
  tEncodeSize(tEncodeSSubmitRsp2, pSubmitRsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  tEncoderInit(&ec, pRsp->pCont, pRsp->contLen);
  (void)tEncodeSSubmitRsp2(&ec, pSubmitRsp);
  tEncoderClear(&ec);

  // update statistics
  (void)atomic_add_fetch_64(&pVnode->statis.nInsert, pSubmitRsp->affectedRows);
  (void)atomic_add_fetch_64(&pVnode->statis.nInsertSuccess, pSubmitRsp->affectedRows);
  (void)atomic_add_fetch_64(&pVnode->statis.nBatchInsert, 1);

  if (tsEnableMonitor && tsMonitorFqdn[0] != 0 && tsMonitorPort != 0 && pSubmitRsp->affectedRows > 0 &&
      strlen(pOriginalMsg->info.conn.user) > 0 && tsInsertCounter != NULL) {
    const char *sample_labels[] = {VNODE_METRIC_TAG_VALUE_INSERT_AFFECTED_ROWS,
                                   pVnode->monitor.strClusterId,
                                   pVnode->monitor.strDnodeId,
                                   tsLocalEp,
                                   pVnode->monitor.strVgId,
                                   pOriginalMsg->info.conn.user,
                                   "Success"};
    (void)taos_counter_add(tsInsertCounter, pSubmitRsp->affectedRows, sample_labels);
  }

  if (code == 0) {
    (void)atomic_add_fetch_64(&pVnode->statis.nBatchInsertSuccess, 1);
    code = tdProcessRSmaSubmit(pVnode->pSma, ver, pSubmitReq, pReq, len);
  }
  /*
  if (code == 0) {
    atomic_add_fetch_64(&pVnode->statis.nBatchInsertSuccess, 1);
    code = tdProcessRSmaSubmit(pVnode->pSma, ver, pSubmitReq, pReq, len);

    const char *batch_sample_labels[] = {VNODE_METRIC_TAG_VALUE_INSERT, pVnode->monitor.strClusterId,
                                        pVnode->monitor.strDnodeId, tsLocalEp, pVnode->monitor.strVgId,
                                          pOriginalMsg->info.conn.user, "Success"};
    taos_counter_inc(pVnode->monitor.insertCounter, batch_sample_labels);
  }
  else{
    const char *batch_sample_labels[] = {VNODE_METRIC_TAG_VALUE_INSERT, pVnode->monitor.strClusterId,
                                        pVnode->monitor.strDnodeId, tsLocalEp, pVnode->monitor.strVgId,
                                        pOriginalMsg->info.conn.user, "Failed"};
    taos_counter_inc(pVnode->monitor.insertCounter, batch_sample_labels);
  }
  */

  // clear
  taosArrayDestroy(newTbUids);
  tDestroySubmitReq(pSubmitReq, 0 == pMsg->version ? TSDB_MSG_FLG_CMPT : TSDB_MSG_FLG_DECODE);
  tDestroySSubmitRsp2(pSubmitRsp, TSDB_MSG_FLG_ENCODE);

  if (code) terrno = code;

  taosMemoryFree(pAllocMsg);

  return code;
}

static int32_t vnodeProcessCreateTSmaReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVCreateTSmaReq req = {0};
  SDecoder        coder = {0};

  if (pRsp) {
    pRsp->msgType = TDMT_VND_CREATE_SMA_RSP;
    pRsp->code = TSDB_CODE_SUCCESS;
    pRsp->pCont = NULL;
    pRsp->contLen = 0;
  }

  // decode and process req
  tDecoderInit(&coder, pReq, len);

  if (tDecodeSVCreateTSmaReq(&coder, &req) < 0) {
    terrno = TSDB_CODE_MSG_DECODE_ERROR;
    if (pRsp) pRsp->code = terrno;
    goto _err;
  }

  if (tdProcessTSmaCreate(pVnode->pSma, ver, (const char *)&req) < 0) {
    if (pRsp) pRsp->code = terrno;
    goto _err;
  }

  tDecoderClear(&coder);
  vDebug("vgId:%d, success to create tsma %s:%" PRIi64 " version %" PRIi64 " for table %" PRIi64, TD_VID(pVnode),
         req.indexName, req.indexUid, ver, req.tableUid);
  return 0;

_err:
  tDecoderClear(&coder);
  vError("vgId:%d, failed to create tsma %s:%" PRIi64 " version %" PRIi64 "for table %" PRIi64 " since %s",
         TD_VID(pVnode), req.indexName, req.indexUid, ver, req.tableUid, terrstr());
  return terrno;
}

/**
 * @brief specific for smaDstVnode
 *
 * @param pVnode
 * @param pCont
 * @param contLen
 * @return int32_t
 */
int32_t vnodeProcessCreateTSma(SVnode *pVnode, void *pCont, uint32_t contLen) {
  return vnodeProcessCreateTSmaReq(pVnode, 1, pCont, contLen, NULL);
}

static int32_t vnodeConsolidateAlterHashRange(SVnode *pVnode, int64_t ver) {
  int32_t code = TSDB_CODE_SUCCESS;

  vInfo("vgId:%d, trim meta of tables per hash range [%" PRIu32 ", %" PRIu32 "]. apply-index:%" PRId64, TD_VID(pVnode),
        pVnode->config.hashBegin, pVnode->config.hashEnd, ver);

  // TODO: trim meta of tables from TDB per hash range [pVnode->config.hashBegin, pVnode->config.hashEnd]
  code = metaTrimTables(pVnode->pMeta);

  return code;
}

static int32_t vnodeProcessAlterConfirmReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  vInfo("vgId:%d, vnode handle msgType:alter-confirm, alter confirm msg is processed", TD_VID(pVnode));
  int32_t code = TSDB_CODE_SUCCESS;
  if (!pVnode->config.hashChange) {
    goto _exit;
  }

  code = vnodeConsolidateAlterHashRange(pVnode, ver);
  if (code < 0) {
    vError("vgId:%d, failed to consolidate alter hashrange since %s. version:%" PRId64, TD_VID(pVnode), terrstr(), ver);
    goto _exit;
  }
  pVnode->config.hashChange = false;

_exit:
  pRsp->msgType = TDMT_VND_ALTER_CONFIRM_RSP;
  pRsp->code = code;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  return code;
}

extern int32_t tsdbDisableAndCancelAllBgTask(STsdb *pTsdb);
extern int32_t tsdbEnableBgTask(STsdb *pTsdb);

static int32_t vnodeProcessAlterConfigReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  bool walChanged = false;
  bool tsdbChanged = false;

  SAlterVnodeConfigReq req = {0};
  if (tDeserializeSAlterVnodeConfigReq(pReq, len, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return TSDB_CODE_INVALID_MSG;
  }

  vInfo("vgId:%d, start to alter vnode config, page:%d pageSize:%d buffer:%d szPage:%d szBuf:%" PRIu64
        " cacheLast:%d cacheLastSize:%d days:%d keep0:%d keep1:%d keep2:%d keepTimeOffset:%d s3KeepLocal:%d "
        "s3Compact:%d fsync:%d level:%d "
        "walRetentionPeriod:%d walRetentionSize:%d",
        TD_VID(pVnode), req.pages, req.pageSize, req.buffer, req.pageSize * 1024, (uint64_t)req.buffer * 1024 * 1024,
        req.cacheLast, req.cacheLastSize, req.daysPerFile, req.daysToKeep0, req.daysToKeep1, req.daysToKeep2,
        req.keepTimeOffset, req.s3KeepLocal, req.s3Compact, req.walFsyncPeriod, req.walLevel, req.walRetentionPeriod,
        req.walRetentionSize);

  if (pVnode->config.cacheLastSize != req.cacheLastSize) {
    pVnode->config.cacheLastSize = req.cacheLastSize;
    tsdbCacheSetCapacity(pVnode, (size_t)pVnode->config.cacheLastSize * 1024 * 1024);
  }

  if (pVnode->config.szBuf != req.buffer * 1024LL * 1024LL) {
    vInfo("vgId:%d, vnode buffer is changed from %" PRId64 " to %" PRId64, TD_VID(pVnode), pVnode->config.szBuf,
          (uint64_t)(req.buffer * 1024LL * 1024LL));
    pVnode->config.szBuf = req.buffer * 1024LL * 1024LL;
  }

  if (pVnode->config.szCache != req.pages) {
    if (metaAlterCache(pVnode->pMeta, req.pages) < 0) {
      vError("vgId:%d, failed to change vnode pages from %d to %d failed since %s", TD_VID(pVnode),
             pVnode->config.szCache, req.pages, tstrerror(errno));
      return errno;
    } else {
      vInfo("vgId:%d, vnode pages is changed from %d to %d", TD_VID(pVnode), pVnode->config.szCache, req.pages);
      pVnode->config.szCache = req.pages;
    }
  }

  if (pVnode->config.cacheLast != req.cacheLast) {
    pVnode->config.cacheLast = req.cacheLast;
  }

  if (pVnode->config.walCfg.fsyncPeriod != req.walFsyncPeriod) {
    pVnode->config.walCfg.fsyncPeriod = req.walFsyncPeriod;
    walChanged = true;
  }

  if (pVnode->config.walCfg.level != req.walLevel) {
    if (pVnode->config.walCfg.level == 0) {
      pVnode->config.walCfg.clearFiles = 1;
    }
    pVnode->config.walCfg.level = req.walLevel;
    walChanged = true;
  }

  if (pVnode->config.walCfg.retentionPeriod != req.walRetentionPeriod) {
    pVnode->config.walCfg.retentionPeriod = req.walRetentionPeriod;
    walChanged = true;
  }

  if (pVnode->config.walCfg.retentionSize != req.walRetentionSize) {
    pVnode->config.walCfg.retentionSize = req.walRetentionSize;
    walChanged = true;
  }

  if (pVnode->config.tsdbCfg.keep0 != req.daysToKeep0) {
    pVnode->config.tsdbCfg.keep0 = req.daysToKeep0;
    if (!VND_IS_RSMA(pVnode)) {
      tsdbChanged = true;
    }
  }

  if (pVnode->config.tsdbCfg.keep1 != req.daysToKeep1) {
    pVnode->config.tsdbCfg.keep1 = req.daysToKeep1;
    if (!VND_IS_RSMA(pVnode)) {
      tsdbChanged = true;
    }
  }

  if (pVnode->config.tsdbCfg.keep2 != req.daysToKeep2) {
    pVnode->config.tsdbCfg.keep2 = req.daysToKeep2;
    if (!VND_IS_RSMA(pVnode)) {
      tsdbChanged = true;
    }
  }

  if (pVnode->config.tsdbCfg.keepTimeOffset != req.keepTimeOffset) {
    pVnode->config.tsdbCfg.keepTimeOffset = req.keepTimeOffset;
    if (!VND_IS_RSMA(pVnode)) {
      tsdbChanged = true;
    }
  }

  if (req.sttTrigger != -1 && req.sttTrigger != pVnode->config.sttTrigger) {
    if (req.sttTrigger > 1 && pVnode->config.sttTrigger > 1) {
      pVnode->config.sttTrigger = req.sttTrigger;
    } else {
      (void)vnodeAWait(&pVnode->commitTask);
      (void)tsdbDisableAndCancelAllBgTask(pVnode->pTsdb);
      pVnode->config.sttTrigger = req.sttTrigger;
      (void)tsdbEnableBgTask(pVnode->pTsdb);
    }
  }

  if (req.minRows != -1 && req.minRows != pVnode->config.tsdbCfg.minRows) {
    pVnode->config.tsdbCfg.minRows = req.minRows;
  }

  if (req.s3KeepLocal != -1 && req.s3KeepLocal != pVnode->config.s3KeepLocal) {
    pVnode->config.s3KeepLocal = req.s3KeepLocal;
  }
  if (req.s3Compact != -1 && req.s3Compact != pVnode->config.s3Compact) {
    pVnode->config.s3Compact = req.s3Compact;
  }

  if (walChanged) {
    (void)walAlter(pVnode->pWal, &pVnode->config.walCfg);
  }

  if (tsdbChanged) {
    (void)tsdbSetKeepCfg(pVnode->pTsdb, &pVnode->config.tsdbCfg);
  }

  return 0;
}

static int32_t vnodeProcessBatchDeleteReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SBatchDeleteReq deleteReq;
  SDecoder        decoder;
  tDecoderInit(&decoder, pReq, len);
  (void)tDecodeSBatchDeleteReq(&decoder, &deleteReq);

  SMetaReader mr = {0};
  metaReaderDoInit(&mr, pVnode->pMeta, META_READER_NOLOCK);
  STsdb *pTsdb = pVnode->pTsdb;

  if (deleteReq.level) {
    pTsdb = deleteReq.level == 1 ? VND_RSMA1(pVnode) : VND_RSMA2(pVnode);
  }

  int32_t sz = taosArrayGetSize(deleteReq.deleteReqs);
  for (int32_t i = 0; i < sz; i++) {
    SSingleDeleteReq *pOneReq = taosArrayGet(deleteReq.deleteReqs, i);
    char             *name = pOneReq->tbname;
    if (metaGetTableEntryByName(&mr, name) < 0) {
      vDebug("vgId:%d, stream delete msg, skip since no table: %s", pVnode->config.vgId, name);
      continue;
    }

    int64_t uid = mr.me.uid;

    int32_t code = tsdbDeleteTableData(pTsdb, ver, deleteReq.suid, uid, pOneReq->startTs, pOneReq->endTs);
    if (code < 0) {
      terrno = code;
      vError("vgId:%d, delete error since %s, suid:%" PRId64 ", uid:%" PRId64 ", start ts:%" PRId64 ", end ts:%" PRId64,
             TD_VID(pVnode), terrstr(), deleteReq.suid, uid, pOneReq->startTs, pOneReq->endTs);
    }

    if (deleteReq.level == 0) {
      code = metaUpdateChangeTimeWithLock(pVnode->pMeta, uid, deleteReq.ctimeMs);
      if (code < 0) {
        terrno = code;
        vError("vgId:%d, update change time error since %s, suid:%" PRId64 ", uid:%" PRId64 ", start ts:%" PRId64
               ", end ts:%" PRId64,
               TD_VID(pVnode), terrstr(), deleteReq.suid, uid, pOneReq->startTs, pOneReq->endTs);
      }
    }
    tDecoderClear(&mr.coder);
  }
  metaReaderClear(&mr);
  taosArrayDestroy(deleteReq.deleteReqs);
  return 0;
}

static int32_t vnodeProcessDeleteReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp,
                                     SRpcMsg *pOriginalMsg) {
  int32_t     code = 0;
  SDecoder   *pCoder = &(SDecoder){0};
  SDeleteRes *pRes = &(SDeleteRes){0};

  pRsp->msgType = TDMT_VND_DELETE_RSP;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;
  pRsp->code = TSDB_CODE_SUCCESS;

  pRes->uidList = taosArrayInit(0, sizeof(tb_uid_t));
  if (pRes->uidList == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  tDecoderInit(pCoder, pReq, len);
  code = tDecodeDeleteRes(pCoder, pRes);
  if (code) goto _err;

  if (pRes->affectedRows > 0) {
    for (int32_t iUid = 0; iUid < taosArrayGetSize(pRes->uidList); iUid++) {
      uint64_t uid = *(uint64_t *)taosArrayGet(pRes->uidList, iUid);
      code = tsdbDeleteTableData(pVnode->pTsdb, ver, pRes->suid, uid, pRes->skey, pRes->ekey);
      if (code) goto _err;
      code = metaUpdateChangeTimeWithLock(pVnode->pMeta, uid, pRes->ctimeMs);
      if (code) goto _err;
    }
  }

  code = tdProcessRSmaDelete(pVnode->pSma, ver, pRes, pReq, len);

  tDecoderClear(pCoder);
  taosArrayDestroy(pRes->uidList);

  SVDeleteRsp rsp = {.affectedRows = pRes->affectedRows};
  int32_t     ret = 0;
  tEncodeSize(tEncodeSVDeleteRsp, &rsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  SEncoder ec = {0};
  tEncoderInit(&ec, pRsp->pCont, pRsp->contLen);
  code = tEncodeSVDeleteRsp(&ec, &rsp);
  if (code) goto _err;
  tEncoderClear(&ec);
  return code;

_err:
  /*
  if(code == TSDB_CODE_SUCCESS){
    const char *batch_sample_labels[] = {VNODE_METRIC_TAG_VALUE_DELETE, pVnode->monitor.strClusterId,
                                        pVnode->monitor.strDnodeId, tsLocalEp, pVnode->monitor.strVgId,
                                        pOriginalMsg->info.conn.user, "Success"};
    taos_counter_inc(pVnode->monitor.insertCounter, batch_sample_labels);
  }
  else{
    const char *batch_sample_labels[] = {VNODE_METRIC_TAG_VALUE_DELETE, pVnode->monitor.strClusterId,
                                        pVnode->monitor.strDnodeId, tsLocalEp, pVnode->monitor.strVgId,
                                        pOriginalMsg->info.conn.user, "Failed"};
    taos_counter_inc(pVnode->monitor.insertCounter, batch_sample_labels);
  }
  */

  return code;
}
static int32_t vnodeProcessCreateIndexReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVCreateStbReq req = {0};
  SDecoder       dc = {0};
  int32_t        code = 0;

  pRsp->msgType = TDMT_VND_CREATE_INDEX_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  tDecoderInit(&dc, pReq, len);
  // decode req
  if (tDecodeSVCreateStbReq(&dc, &req) < 0) {
    tDecoderClear(&dc);
    return terrno = TSDB_CODE_INVALID_MSG;
  }

  code = metaAddIndexToSTable(pVnode->pMeta, ver, &req);
  if (code) {
    pRsp->code = code;
    goto _err;
  }
  tDecoderClear(&dc);
  return 0;

_err:
  tDecoderClear(&dc);
  return code;
}
static int32_t vnodeProcessDropIndexReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SDropIndexReq req = {0};
  int32_t       code = 0;
  pRsp->msgType = TDMT_VND_DROP_INDEX_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  if ((code = tDeserializeSDropIdxReq(pReq, len, &req))) {
    pRsp->code = code;
    return code;
  }

  code = metaDropIndexFromSTable(pVnode->pMeta, ver, &req);
  if (code) {
    pRsp->code = code;
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

extern int32_t vnodeAsyncCompact(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp);

static int32_t vnodeProcessCompactVnodeReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  if (!pVnode->restored) {
    vInfo("vgId:%d, ignore compact req during restoring. ver:%" PRId64, TD_VID(pVnode), ver);
    return 0;
  }
  return vnodeAsyncCompact(pVnode, ver, pReq, len, pRsp);
}

static int32_t vnodeProcessConfigChangeReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  (void)syncCheckMember(pVnode->sync);

  pRsp->msgType = TDMT_SYNC_CONFIG_CHANGE_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  return 0;
}

static int32_t vnodePreCheckAssignedLogSyncd(SVnode *pVnode, char *member0Token, char *member1Token) {
  SSyncState syncState = syncGetState(pVnode->sync);
  if (syncState.state != TAOS_SYNC_STATE_LEADER) {
    return terrno = TSDB_CODE_SYN_NOT_LEADER;
  }

  char token[TSDB_ARB_TOKEN_SIZE] = {0};
  if (vnodeGetArbToken(pVnode, token) != 0) {
    return terrno = TSDB_CODE_NOT_FOUND;
  }

  if (strncmp(token, member0Token, TSDB_ARB_TOKEN_SIZE) != 0 &&
      strncmp(token, member1Token, TSDB_ARB_TOKEN_SIZE) != 0) {
    return terrno = TSDB_CODE_MND_ARB_TOKEN_MISMATCH;
  }

  terrno = TSDB_CODE_SUCCESS;
  return 0;
}

static int32_t vnodeCheckAssignedLogSyncd(SVnode *pVnode, char *member0Token, char *member1Token) {
  int32_t code = vnodePreCheckAssignedLogSyncd(pVnode, member0Token, member1Token);
  if (code != 0) {
    return code;
  }

  return syncGetAssignedLogSynced(pVnode->sync);
}

static int32_t vnodeProcessArbCheckSyncReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t code = 0;

  SVArbCheckSyncReq syncReq = {0};

  code = tDeserializeSVArbCheckSyncReq(pReq, len, &syncReq);
  if (code) {
    return terrno = code;
  }

  pRsp->msgType = TDMT_VND_ARB_CHECK_SYNC_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  SVArbCheckSyncRsp syncRsp = {0};
  syncRsp.arbToken = syncReq.arbToken;
  syncRsp.member0Token = syncReq.member0Token;
  syncRsp.member1Token = syncReq.member1Token;
  syncRsp.vgId = TD_VID(pVnode);

  (void)vnodeCheckAssignedLogSyncd(pVnode, syncReq.member0Token, syncReq.member1Token);
  syncRsp.errCode = terrno;

  if (vnodeUpdateArbTerm(pVnode, syncReq.arbTerm) != 0) {
    vError("vgId:%d, failed to update arb term", TD_VID(pVnode));
    code = -1;
    goto _OVER;
  }

  int32_t contLen = tSerializeSVArbCheckSyncRsp(NULL, 0, &syncRsp);
  if (contLen <= 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto _OVER;
  }
  void *pHead = rpcMallocCont(contLen);
  if (!pHead) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto _OVER;
  }

  if (tSerializeSVArbCheckSyncRsp(pHead, contLen, &syncRsp) <= 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    rpcFreeCont(pHead);
    code = -1;
    goto _OVER;
  }

  pRsp->pCont = pHead;
  pRsp->contLen = contLen;

  terrno = TSDB_CODE_SUCCESS;

_OVER:
  tFreeSVArbCheckSyncReq(&syncReq);
  return code;
}

#ifndef TD_ENTERPRISE
int32_t vnodeAsyncCompact(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) { return 0; }
int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw, bool sync) { return 0; }
#endif
