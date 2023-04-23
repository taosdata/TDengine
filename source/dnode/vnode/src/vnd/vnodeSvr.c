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

#include "tencode.h"
#include "tmsg.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"

static int32_t vnodeProcessCreateStbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterStbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropStbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessCreateTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessSubmitReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessCreateTSmaReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterConfirmReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterConfigReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropTtlTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessTrimReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDeleteReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessBatchDeleteReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessCreateIndexReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropIndexReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessCompactVnodeReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);

static int32_t vnodePreprocessCreateTableReq(SVnode *pVnode, SDecoder *pCoder, int64_t ctime, int64_t *pUid) {
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

  // ctime
  *(int64_t *)(pCoder->data + pCoder->pos + 8) = ctime;

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

  int64_t  ctime = taosGetTimestampMs();
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
    code = vnodePreprocessCreateTableReq(pVnode, &dc, ctime, NULL);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tEndDecode(&dc);

_exit:
  tDecoderClear(&dc);
  return code;
}
extern int64_t tsMaxKeyByPrecision[];
static int32_t vnodePreProcessSubmitTbData(SVnode *pVnode, SDecoder *pCoder, int64_t ctime) {
  int32_t code = 0;
  int32_t lino = 0;

  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SSubmitTbData submitTbData;
  if (tDecodeI32v(pCoder, &submitTbData.flags) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  int64_t uid;
  if (submitTbData.flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    code = vnodePreprocessCreateTableReq(pVnode, pCoder, ctime, &uid);
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
  TSKEY now = ctime;
  if (pVnode->config.tsdbCfg.precision == TSDB_TIME_PRECISION_MICRO) {
    now *= 1000;
  } else if (pVnode->config.tsdbCfg.precision == TSDB_TIME_PRECISION_NANO) {
    now *= 1000000;
  }
  TSKEY minKey = now - tsTickPerMin[pVnode->config.tsdbCfg.precision] * pVnode->config.tsdbCfg.keep2;
  TSKEY maxKey = tsMaxKeyByPrecision[pVnode->config.tsdbCfg.precision];
  if (submitTbData.flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    uint64_t nColData;
    if (tDecodeU64v(pCoder, &nColData) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }

    SColData colData = {0};
    pCoder->pos += tGetColData(pCoder->data + pCoder->pos, &colData);

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

  int64_t ctime = taosGetTimestampMs();
  for (int32_t i = 0; i < nSubmitTbData; i++) {
    code = vnodePreProcessSubmitTbData(pVnode, pCoder, ctime);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tEndDecode(pCoder);

_exit:
  tDecoderClear(pCoder);
  return code;
}

static int32_t vnodePreProcessDeleteMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  int32_t     size;
  int32_t     ret;
  uint8_t    *pCont;
  SEncoder   *pCoder = &(SEncoder){0};
  SDeleteRes  res = {0};
  SReadHandle handle = {.meta = pVnode->pMeta, .config = &pVnode->config, .vnode = pVnode, .pMsgCb = &pVnode->msgCb};

  code = qWorkerProcessDeleteMsg(&handle, pVnode->pQuery, pMsg, &res);
  if (code) goto _exit;

  // malloc and encode
  tEncodeSize(tEncodeDeleteRes, &res, size, ret);
  pCont = rpcMallocCont(size + sizeof(SMsgHead));

  ((SMsgHead *)pCont)->contLen = size + sizeof(SMsgHead);
  ((SMsgHead *)pCont)->vgId = TD_VID(pVnode);

  tEncoderInit(pCoder, pCont + sizeof(SMsgHead), size);
  tEncodeDeleteRes(pCoder, &res);
  tEncoderClear(pCoder);

  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = pCont;
  pMsg->contLen = size + sizeof(SMsgHead);

  taosArrayDestroy(res.uidList);

_exit:
  return code;
}

int32_t vnodePreProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t code = 0;

  switch (pMsg->msgType) {
    case TDMT_VND_CREATE_TABLE: {
      code = vnodePreProcessCreateTableMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_SUBMIT: {
      code = vnodePreProcessSubmitMsg(pVnode, pMsg);
    } break;
    case TDMT_VND_DELETE: {
      code = vnodePreProcessDeleteMsg(pVnode, pMsg);
    } break;
    default:
      break;
  }

_exit:
  if (code) {
    vError("vgId%d failed to preprocess write request since %s, msg type:%d", TD_VID(pVnode), tstrerror(code),
           pMsg->msgType);
  }
  return code;
}

int32_t vnodeProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg, int64_t version, SRpcMsg *pRsp) {
  void   *ptr = NULL;
  void   *pReq;
  int32_t len;
  int32_t ret;

  if (version <= pVnode->state.applied) {
    vError("vgId:%d, duplicate write request. version: %" PRId64 ", applied: %" PRId64 "", TD_VID(pVnode), version,
           pVnode->state.applied);
    terrno = TSDB_CODE_VND_DUP_REQUEST;
    return -1;
  }

  vDebug("vgId:%d, start to process write request %s, index:%" PRId64, TD_VID(pVnode), TMSG_INFO(pMsg->msgType),
         version);

  ASSERT(pVnode->state.applyTerm <= pMsg->info.conn.applyTerm);
  ASSERT(pVnode->state.applied + 1 == version);

  atomic_store_64(&pVnode->state.applied, version);
  atomic_store_64(&pVnode->state.applyTerm, pMsg->info.conn.applyTerm);

  if (!syncUtilUserCommit(pMsg->msgType)) goto _exit;

  if (pMsg->msgType == TDMT_VND_STREAM_RECOVER_BLOCKING_STAGE || pMsg->msgType == TDMT_STREAM_TASK_CHECK_RSP) {
    if (tqCheckLogInWal(pVnode->pTq, version)) return 0;
  }

  // skip header
  pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  len = pMsg->contLen - sizeof(SMsgHead);
  bool needCommit = false;

  switch (pMsg->msgType) {
    /* META */
    case TDMT_VND_CREATE_STB:
      if (vnodeProcessCreateStbReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_ALTER_STB:
      if (vnodeProcessAlterStbReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_DROP_STB:
      if (vnodeProcessDropStbReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_CREATE_TABLE:
      if (vnodeProcessCreateTbReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_ALTER_TABLE:
      if (vnodeProcessAlterTbReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_DROP_TABLE:
      if (vnodeProcessDropTbReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_DROP_TTL_TABLE:
      if (vnodeProcessDropTtlTbReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_TRIM:
      if (vnodeProcessTrimReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_CREATE_SMA:
      if (vnodeProcessCreateTSmaReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    /* TSDB */
    case TDMT_VND_SUBMIT:
      if (vnodeProcessSubmitReq(pVnode, version, pMsg->pCont, pMsg->contLen, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_DELETE:
      if (vnodeProcessDeleteReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_BATCH_DEL:
      if (vnodeProcessBatchDeleteReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    /* TQ */
    case TDMT_VND_TMQ_SUBSCRIBE:
      if (tqProcessSubscribeReq(pVnode->pTq, version, pReq, len) < 0) {
        goto _err;
      }
      break;
    case TDMT_VND_TMQ_DELETE_SUB:
      if (tqProcessDeleteSubReq(pVnode->pTq, version, pMsg->pCont, pMsg->contLen) < 0) {
        goto _err;
      }
      break;
    case TDMT_VND_TMQ_COMMIT_OFFSET:
      if (tqProcessOffsetCommitReq(pVnode->pTq, version, pReq, pMsg->contLen - sizeof(SMsgHead)) < 0) {
        goto _err;
      }
      break;
    case TDMT_VND_TMQ_ADD_CHECKINFO:
      if (tqProcessAddCheckInfoReq(pVnode->pTq, version, pReq, len) < 0) {
        goto _err;
      }
      break;
    case TDMT_VND_TMQ_DEL_CHECKINFO:
      if (tqProcessDelCheckInfoReq(pVnode->pTq, version, pReq, len) < 0) {
        goto _err;
      }
      break;
    case TDMT_STREAM_TASK_DEPLOY: {
      if (pVnode->restored && tqProcessTaskDeployReq(pVnode->pTq, version, pReq, len) < 0) {
        goto _err;
      }
    } break;
    case TDMT_STREAM_TASK_DROP: {
      if (tqProcessTaskDropReq(pVnode->pTq, version, pMsg->pCont, pMsg->contLen) < 0) {
        goto _err;
      }
    } break;
    case TDMT_VND_STREAM_RECOVER_BLOCKING_STAGE: {
      if (tqProcessTaskRecover2Req(pVnode->pTq, version, pMsg->pCont, pMsg->contLen) < 0) {
        goto _err;
      }
    } break;
    case TDMT_STREAM_TASK_CHECK_RSP: {
      if (tqProcessStreamTaskCheckRsp(pVnode->pTq, version, pReq, len) < 0) {
        goto _err;
      }
    } break;
    case TDMT_VND_ALTER_CONFIRM:
      vnodeProcessAlterConfirmReq(pVnode, version, pReq, len, pRsp);
      break;
    case TDMT_VND_ALTER_CONFIG:
      vnodeProcessAlterConfigReq(pVnode, version, pReq, len, pRsp);
      break;
    case TDMT_VND_COMMIT:
      needCommit = true;
      break;
    case TDMT_VND_CREATE_INDEX:
      vnodeProcessCreateIndexReq(pVnode, version, pReq, len, pRsp);
      break;
    case TDMT_VND_DROP_INDEX:
      vnodeProcessDropIndexReq(pVnode, version, pReq, len, pRsp);
      break;
    case TDMT_VND_COMPACT:
      vnodeProcessCompactVnodeReq(pVnode, version, pReq, len, pRsp);
      goto _exit;
    default:
      vError("vgId:%d, unprocessed msg, %d", TD_VID(pVnode), pMsg->msgType);
      return -1;
  }

  vTrace("vgId:%d, process %s request, code:0x%x index:%" PRId64, TD_VID(pVnode), TMSG_INFO(pMsg->msgType), pRsp->code,
         version);

  walApplyVer(pVnode->pWal, version);

  if (tqPushMsg(pVnode->pTq, pMsg->pCont, pMsg->contLen, pMsg->msgType, version) < 0) {
    /*vInfo("vgId:%d, push msg end", pVnode->config.vgId);*/
    vError("vgId:%d, failed to push msg to TQ since %s", TD_VID(pVnode), tstrerror(terrno));
    return -1;
  }

  // commit if need
  if (needCommit) {
    vInfo("vgId:%d, commit at version %" PRId64, TD_VID(pVnode), version);
    if (vnodeAsyncCommit(pVnode) < 0) {
      vError("vgId:%d, failed to vnode async commit since %s.", TD_VID(pVnode), tstrerror(terrno));
      goto _err;
    }

    // start a new one
    if (vnodeBegin(pVnode) < 0) {
      vError("vgId:%d, failed to begin vnode since %s.", TD_VID(pVnode), tstrerror(terrno));
      goto _err;
    }
  }

_exit:
  return 0;

_err:
  vError("vgId:%d, process %s request failed since %s, version:%" PRId64, TD_VID(pVnode), TMSG_INFO(pMsg->msgType),
         tstrerror(terrno), version);
  return -1;
}

int32_t vnodePreprocessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  if (TDMT_SCH_QUERY != pMsg->msgType && TDMT_SCH_MERGE_QUERY != pMsg->msgType) {
    return 0;
  }

  return qWorkerPreprocessQueryMsg(pVnode->pQuery, pMsg, TDMT_SCH_QUERY == pMsg->msgType);
}

int32_t vnodeProcessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  vTrace("message in vnode query queue is processing");
  if ((pMsg->msgType == TDMT_SCH_QUERY) && !syncIsReadyForRead(pVnode->sync)) {
    vnodeRedirectRpcMsg(pVnode, pMsg, terrno);
    return 0;
  }

  SReadHandle handle = {.meta = pVnode->pMeta, .config = &pVnode->config, .vnode = pVnode, .pMsgCb = &pVnode->msgCb};
  switch (pMsg->msgType) {
    case TDMT_SCH_QUERY:
    case TDMT_SCH_MERGE_QUERY:
      return qWorkerProcessQueryMsg(&handle, pVnode->pQuery, pMsg, 0);
    case TDMT_SCH_QUERY_CONTINUE:
      return qWorkerProcessCQueryMsg(&handle, pVnode->pQuery, pMsg, 0);
    default:
      vError("unknown msg type:%d in query queue", pMsg->msgType);
      return TSDB_CODE_APP_ERROR;
  }
}

int32_t vnodeProcessFetchMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo) {
  vTrace("vgId:%d, msg:%p in fetch queue is processing", pVnode->config.vgId, pMsg);
  if ((pMsg->msgType == TDMT_SCH_FETCH || pMsg->msgType == TDMT_VND_TABLE_META || pMsg->msgType == TDMT_VND_TABLE_CFG ||
       pMsg->msgType == TDMT_VND_BATCH_META || pMsg->msgType == TDMT_VND_TMQ_CONSUME) &&
      !syncIsReadyForRead(pVnode->sync)) {
    vnodeRedirectRpcMsg(pVnode, pMsg, terrno);
    return 0;
  }

  if (pMsg->msgType == TDMT_VND_TMQ_CONSUME && !pVnode->restored) {
    vnodeRedirectRpcMsg(pVnode, pMsg, TSDB_CODE_SYN_RESTORING);
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
    case TDMT_SCH_QUERY_HEARTBEAT:
      return qWorkerProcessHbMsg(pVnode, pVnode->pQuery, pMsg, 0);
    case TDMT_VND_TABLE_META:
      return vnodeGetTableMeta(pVnode, pMsg, true);
    case TDMT_VND_TABLE_CFG:
      return vnodeGetTableCfg(pVnode, pMsg, true);
    case TDMT_VND_BATCH_META:
      return vnodeGetBatchMeta(pVnode, pMsg);
    case TDMT_VND_TMQ_CONSUME:
      return tqProcessPollReq(pVnode->pTq, pMsg);
    case TDMT_STREAM_TASK_RUN:
      return tqProcessTaskRunReq(pVnode->pTq, pMsg);
    case TDMT_STREAM_TASK_DISPATCH:
      return tqProcessTaskDispatchReq(pVnode->pTq, pMsg, true);
    case TDMT_STREAM_TASK_CHECK:
      return tqProcessStreamTaskCheckReq(pVnode->pTq, pMsg);
    case TDMT_STREAM_TASK_DISPATCH_RSP:
      return tqProcessTaskDispatchRsp(pVnode->pTq, pMsg);
    case TDMT_STREAM_RETRIEVE:
      return tqProcessTaskRetrieveReq(pVnode->pTq, pMsg);
    case TDMT_STREAM_RETRIEVE_RSP:
      return tqProcessTaskRetrieveRsp(pVnode->pTq, pMsg);
    case TDMT_VND_STREAM_RECOVER_NONBLOCKING_STAGE:
      return tqProcessTaskRecover1Req(pVnode->pTq, pMsg);
    case TDMT_STREAM_RECOVER_FINISH:
      return tqProcessTaskRecoverFinishReq(pVnode->pTq, pMsg);
    case TDMT_STREAM_RECOVER_FINISH_RSP:
      return tqProcessTaskRecoverFinishRsp(pVnode->pTq, pMsg);
    default:
      vError("unknown msg type:%d in fetch queue", pMsg->msgType);
      return TSDB_CODE_APP_ERROR;
  }
}

// TODO: remove the function
void smaHandleRes(void *pVnode, int64_t smaId, const SArray *data) {
  // TODO
  // blockDebugShowDataBlocks(data, __func__);
  tdProcessTSmaInsert(((SVnode *)pVnode)->pSma, smaId, (const char *)data);
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

extern int32_t vnodeAsyncRentention(SVnode *pVnode, int64_t now);
static int32_t vnodeProcessTrimReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t     code = 0;
  SVTrimDbReq trimReq = {0};

  // decode
  if (tDeserializeSVTrimDbReq(pReq, len, &trimReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  vInfo("vgId:%d, trim vnode request will be processed, time:%d", pVnode->config.vgId, trimReq.timestamp);

  // process
  vnodeAsyncRentention(pVnode, trimReq.timestamp);
  tsem_wait(&pVnode->canCommit);
  tsem_post(&pVnode->canCommit);

_exit:
  return code;
}

static int32_t vnodeProcessDropTtlTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SArray *tbUids = taosArrayInit(8, sizeof(int64_t));
  if (tbUids == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  SVDropTtlTableReq ttlReq = {0};
  if (tDeserializeSVDropTtlTableReq(pReq, len, &ttlReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto end;
  }

  vDebug("vgId:%d, drop ttl table req will be processed, time:%d", pVnode->config.vgId, ttlReq.timestamp);
  int32_t ret = metaTtlDropTable(pVnode->pMeta, ttlReq.timestamp, tbUids);
  if (ret != 0) {
    goto end;
  }
  if (taosArrayGetSize(tbUids) > 0) {
    tqUpdateTbUidList(pVnode->pTq, tbUids, false);
  }

  vnodeAsyncRentention(pVnode, ttlReq.timestamp);

end:
  taosArrayDestroy(tbUids);
  return ret;
}

static int32_t vnodeProcessCreateStbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVCreateStbReq req = {0};
  SDecoder       coder;

  pRsp->msgType = TDMT_VND_CREATE_STB_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  // decode and process req
  tDecoderInit(&coder, pReq, len);

  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    pRsp->code = terrno;
    goto _err;
  }

  if (metaCreateSTable(pVnode->pMeta, version, &req) < 0) {
    pRsp->code = terrno;
    goto _err;
  }

  if (tdProcessRSmaCreate(pVnode->pSma, &req) < 0) {
    pRsp->code = terrno;
    goto _err;
  }

  tDecoderClear(&coder);
  return 0;

_err:
  tDecoderClear(&coder);
  return -1;
}

static int32_t vnodeProcessCreateTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
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
  if (rsp.pArray == NULL || tbUids == NULL) {
    rcode = -1;
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // loop to create table
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    memset(&cRsp, 0, sizeof(cRsp));

    if ((terrno = grantCheck(TSDB_GRANT_TIMESERIES)) < 0) {
      rcode = -1;
      goto _exit;
    }

    if ((terrno = grantCheck(TSDB_GRANT_TABLE)) < 0) {
      rcode = -1;
      goto _exit;
    }

    // validate hash
    sprintf(tbName, "%s.%s", pVnode->config.dbname, pCreateReq->name);
    if (vnodeValidateTableHash(pVnode, tbName) < 0) {
      cRsp.code = TSDB_CODE_VND_HASH_MISMATCH;
      taosArrayPush(rsp.pArray, &cRsp);
      continue;
    }

    // do create table
    if (metaCreateTable(pVnode->pMeta, version, pCreateReq, &cRsp.pMeta) < 0) {
      if (pCreateReq->flags & TD_CREATE_IF_NOT_EXISTS && terrno == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
        cRsp.code = TSDB_CODE_SUCCESS;
      } else {
        cRsp.code = terrno;
      }
    } else {
      cRsp.code = TSDB_CODE_SUCCESS;
      tdFetchTbUidList(pVnode->pSma, &pStore, pCreateReq->ctb.suid, pCreateReq->uid);
      taosArrayPush(tbUids, &pCreateReq->uid);
      vnodeUpdateMetaRsp(pVnode, cRsp.pMeta);
    }

    taosArrayPush(rsp.pArray, &cRsp);
  }

  vDebug("vgId:%d, add %d new created tables into query table list", TD_VID(pVnode), (int32_t)taosArrayGetSize(tbUids));
  tqUpdateTbUidList(pVnode->pTq, tbUids, true);
  if (tdUpdateTbUidList(pVnode->pSma, pStore, true) < 0) {
    goto _exit;
  }
  tdUidStoreFree(pStore);

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
  tEncodeSVCreateTbBatchRsp(&encoder, &rsp);

_exit:
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    taosMemoryFree(pCreateReq->comment);
    taosArrayDestroy(pCreateReq->ctb.tagName);
  }
  taosArrayDestroyEx(rsp.pArray, tFreeSVCreateTbRsp);
  taosArrayDestroy(tbUids);
  tDecoderClear(&decoder);
  tEncoderClear(&encoder);
  return rcode;
}

static int32_t vnodeProcessAlterStbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVCreateStbReq req = {0};
  SDecoder       dc = {0};

  pRsp->msgType = TDMT_VND_ALTER_STB_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  tDecoderInit(&dc, pReq, len);

  // decode req
  if (tDecodeSVCreateStbReq(&dc, &req) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    tDecoderClear(&dc);
    return -1;
  }

  if (metaAlterSTable(pVnode->pMeta, version, &req) < 0) {
    pRsp->code = terrno;
    tDecoderClear(&dc);
    return -1;
  }

  tDecoderClear(&dc);

  return 0;
}

static int32_t vnodeProcessDropStbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
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
  if (metaDropSTable(pVnode->pMeta, version, &req, tbUidList) < 0) {
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

static int32_t vnodeProcessAlterTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
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
  if (metaAlterTable(pVnode->pMeta, version, &vAlterTbReq, &vMetaRsp) < 0) {
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
  tEncodeSVAlterTbRsp(&ec, &vAlterTbRsp);
  tEncoderClear(&ec);
  if (vMetaRsp.pSchemas) {
    taosMemoryFree(vMetaRsp.pSchemas);
  }
  return 0;
}

static int32_t vnodeProcessDropTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVDropTbBatchReq req = {0};
  SVDropTbBatchRsp rsp = {0};
  SDecoder         decoder = {0};
  SEncoder         encoder = {0};
  int32_t          ret;
  SArray          *tbUids = NULL;
  STbUidStore     *pStore = NULL;

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
  if (tbUids == NULL || rsp.pArray == NULL) goto _exit;

  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    SVDropTbReq *pDropTbReq = req.pReqs + iReq;
    SVDropTbRsp  dropTbRsp = {0};
    tb_uid_t     tbUid = 0;

    /* code */
    ret = metaDropTable(pVnode->pMeta, version, pDropTbReq, tbUids, &tbUid);
    if (ret < 0) {
      if (pDropTbReq->igNotExists && terrno == TSDB_CODE_TDB_TABLE_NOT_EXIST) {
        dropTbRsp.code = TSDB_CODE_SUCCESS;
      } else {
        dropTbRsp.code = terrno;
      }
    } else {
      dropTbRsp.code = TSDB_CODE_SUCCESS;
      if (tbUid > 0) tdFetchTbUidList(pVnode->pSma, &pStore, pDropTbReq->suid, tbUid);
    }

    taosArrayPush(rsp.pArray, &dropTbRsp);
  }

  tqUpdateTbUidList(pVnode->pTq, tbUids, false);
  tdUpdateTbUidList(pVnode->pSma, pStore, false);

_exit:
  taosArrayDestroy(tbUids);
  tdUidStoreFree(pStore);
  tDecoderClear(&decoder);
  tEncodeSize(tEncodeSVDropTbBatchRsp, &rsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  tEncoderInit(&encoder, pRsp->pCont, pRsp->contLen);
  tEncodeSVDropTbBatchRsp(&encoder, &rsp);
  tEncoderClear(&encoder);
  taosArrayDestroy(rsp.pArray);
  return 0;
}

static int32_t vnodeDebugPrintSingleSubmitMsg(SMeta *pMeta, SSubmitBlk *pBlock, SSubmitMsgIter *msgIter,
                                              const char *tags) {
  SSubmitBlkIter blkIter = {0};
  STSchema      *pSchema = NULL;
  tb_uid_t       suid = 0;
  STSRow        *row = NULL;
  int32_t        rv = -1;

  tInitSubmitBlkIter(msgIter, pBlock, &blkIter);
  if (blkIter.row == NULL) return 0;

  pSchema = metaGetTbTSchema(pMeta, msgIter->suid, TD_ROW_SVER(blkIter.row), 1);  // TODO: use the real schema
  if (pSchema) {
    suid = msgIter->suid;
    rv = TD_ROW_SVER(blkIter.row);
  }
  if (!pSchema) {
    printf("%s:%d no valid schema\n", tags, __LINE__);
    return -1;
  }
  char __tags[128] = {0};
  snprintf(__tags, 128, "%s: uid %" PRIi64 " ", tags, msgIter->uid);
  while ((row = tGetSubmitBlkNext(&blkIter))) {
    tdSRowPrint(row, pSchema, __tags);
  }

  taosMemoryFreeClear(pSchema);

  return TSDB_CODE_SUCCESS;
}

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
  pCxt->pTbSchema = metaGetTbTSchema(pMeta, pCxt->msgIter.suid > 0 ? pCxt->msgIter.suid : pCxt->msgIter.uid,
                                     pCxt->msgIter.sversion, 1);
  if (NULL == pCxt->pTbSchema) {
    return TSDB_CODE_INVALID_MSG;
  }
  tdSTSRowIterInit(&pCxt->rowIter, pCxt->pTbSchema);

  tDestroySSubmitTbData(pCxt->pTbData, TSDB_MSG_FLG_ENCODE);
  if (NULL == pCxt->pTbData) {
    pCxt->pTbData = taosMemoryCalloc(1, sizeof(SSubmitTbData));
    if (NULL == pCxt->pTbData) {
      return TSDB_CODE_OUT_OF_MEMORY;
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  for (int32_t i = 0; i < pCxt->pTbSchema->numOfCols; ++i) {
    SColVal val = COL_VAL_NONE(pCxt->pTbSchema->columns[i].colId, pCxt->pTbSchema->columns[i].type);
    taosArrayPush(pCxt->pColValues, &val);
  }

  return TSDB_CODE_SUCCESS;
}

static void vnodeDestroySubmitReqConvertCxt(SSubmitReqConvertCxt *pCxt) {
  taosMemoryFreeClear(pCxt->pTbSchema);
  tDestroySSubmitTbData(pCxt->pTbData, TSDB_MSG_FLG_ENCODE);
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
    pColVal->value.pData = varDataVal(pCellVal->val);
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SDecoder decoder = {0};
  tDecoderInit(&decoder, pCxt->pBlock->data, pCxt->msgIter.schemaLen);
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
  tEncodeSize(tEncodeSSubmitReq2, pSubmitReq, msglen, code);
  if (TSDB_CODE_SUCCESS == code) {
    pMsg = taosMemoryMalloc(msglen);
    if (NULL == pMsg) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    SEncoder encoder;
    tEncoderInit(&encoder, pMsg, msglen);
    code = tEncodeSSubmitReq2(&encoder, pSubmitReq);
    tEncoderClear(&encoder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    *ppMsg = pMsg;
  }
  return code;
}

static int32_t vnodeProcessSubmitReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
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
    if (tDecodeSSubmitReq2(&dc, pSubmitReq) < 0) {
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

      SColData *pColData = (SColData *)taosArrayGet(pSubmitTbData->aCol, 0);
      TSKEY    *aKey = (TSKEY *)(pColData->pData);

      for (int32_t iRow = 0; iRow < pColData->nVal; iRow++) {
        if (aKey[iRow] < minKey || aKey[iRow] > maxKey || (iRow > 0 && aKey[iRow] <= aKey[iRow - 1])) {
          code = TSDB_CODE_INVALID_MSG;
          vError("vgId:%d %s failed since %s, version:%" PRId64, TD_VID(pVnode), __func__, tstrerror(terrno), version);
          goto _exit;
        }
      }

    } else {
      int32_t nRow = TARRAY_SIZE(pSubmitTbData->aRowP);
      SRow  **aRow = (SRow **)TARRAY_DATA(pSubmitTbData->aRowP);

      for (int32_t iRow = 0; iRow < nRow; ++iRow) {
        if (aRow[iRow]->ts < minKey || aRow[iRow]->ts > maxKey || (iRow > 0 && aRow[iRow]->ts <= aRow[iRow - 1]->ts)) {
          code = TSDB_CODE_INVALID_MSG;
          vError("vgId:%d %s failed since %s, version:%" PRId64, TD_VID(pVnode), __func__, tstrerror(terrno), version);
          goto _exit;
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
        metaGetInfo(pVnode->pMeta, info.suid, &info, NULL);
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
      // check (TODO: move check to create table)
      code = grantCheck(TSDB_GRANT_TIMESERIES);
      if (code) goto _exit;

      code = grantCheck(TSDB_GRANT_TABLE);
      if (code) goto _exit;

      // alloc if need
      if (pSubmitRsp->aCreateTbRsp == NULL &&
          (pSubmitRsp->aCreateTbRsp = taosArrayInit(TARRAY_SIZE(pSubmitReq->aSubmitTbData), sizeof(SVCreateTbRsp))) ==
              NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _exit;
      }

      SVCreateTbRsp *pCreateTbRsp = taosArrayReserve(pSubmitRsp->aCreateTbRsp, 1);

      // create table
      if (metaCreateTable(pVnode->pMeta, version, pSubmitTbData->pCreateTbReq, &pCreateTbRsp->pMeta) == 0) {
        // create table success

        if (newTbUids == NULL &&
            (newTbUids = taosArrayInit(TARRAY_SIZE(pSubmitReq->aSubmitTbData), sizeof(int64_t))) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _exit;
        }

        taosArrayPush(newTbUids, &pSubmitTbData->uid);

        if (pCreateTbRsp->pMeta) {
          vnodeUpdateMetaRsp(pVnode, pCreateTbRsp->pMeta);
        }
      } else {  // create table failed
        if (terrno != TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
          code = terrno;
          goto _exit;
        }
        pSubmitTbData->uid = pSubmitTbData->pCreateTbReq->uid;  // update uid if table exist for using below
      }
    }

    // insert data
    int32_t affectedRows;
    code = tsdbInsertTableData(pVnode->pTsdb, version, pSubmitTbData, &affectedRows);
    if (code) goto _exit;

    pSubmitRsp->affectedRows += affectedRows;
  }

  // update the affected table uid list
  if (taosArrayGetSize(newTbUids) > 0) {
    vDebug("vgId:%d, add %d table into query table list in handling submit", TD_VID(pVnode),
           (int32_t)taosArrayGetSize(newTbUids));
    tqUpdateTbUidList(pVnode->pTq, newTbUids, true);
  }

_exit:
  // message
  pRsp->code = code;
  tEncodeSize(tEncodeSSubmitRsp2, pSubmitRsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  tEncoderInit(&ec, pRsp->pCont, pRsp->contLen);
  tEncodeSSubmitRsp2(&ec, pSubmitRsp);
  tEncoderClear(&ec);

  // update statistics
  atomic_add_fetch_64(&pVnode->statis.nInsert, pSubmitRsp->affectedRows);
  atomic_add_fetch_64(&pVnode->statis.nInsertSuccess, pSubmitRsp->affectedRows);
  atomic_add_fetch_64(&pVnode->statis.nBatchInsert, 1);
  if (code == 0) {
    atomic_add_fetch_64(&pVnode->statis.nBatchInsertSuccess, 1);
    tdProcessRSmaSubmit(pVnode->pSma, version, pSubmitReq, pReq, len, STREAM_INPUT__DATA_SUBMIT);
  }

  // clear
  taosArrayDestroy(newTbUids);
  tDestroySSubmitReq2(pSubmitReq, 0 == pMsg->version ? TSDB_MSG_FLG_CMPT : TSDB_MSG_FLG_DECODE);
  tDestroySSubmitRsp2(pSubmitRsp, TSDB_MSG_FLG_ENCODE);

  if (code) terrno = code;

  taosMemoryFree(pAllocMsg);

  return code;
}

static int32_t vnodeProcessCreateTSmaReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
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

  if (tdProcessTSmaCreate(pVnode->pSma, version, (const char *)&req) < 0) {
    if (pRsp) pRsp->code = terrno;
    goto _err;
  }

  tDecoderClear(&coder);
  vDebug("vgId:%d, success to create tsma %s:%" PRIi64 " version %" PRIi64 " for table %" PRIi64, TD_VID(pVnode),
         req.indexName, req.indexUid, version, req.tableUid);
  return 0;

_err:
  tDecoderClear(&coder);
  vError("vgId:%d, failed to create tsma %s:%" PRIi64 " version %" PRIi64 "for table %" PRIi64 " since %s",
         TD_VID(pVnode), req.indexName, req.indexUid, version, req.tableUid, terrstr());
  return -1;
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

static int32_t vnodeProcessAlterConfirmReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  vInfo("vgId:%d, vnode management handle msgType:alter-confirm, alter replica confim msg is processed", 
          TD_VID(pVnode));
  pRsp->msgType = TDMT_VND_ALTER_CONFIRM_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  return 0;
}

static int32_t vnodeProcessAlterConfigReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  bool walChanged = false;
  bool tsdbChanged = false;

  SAlterVnodeConfigReq req = {0};
  if (tDeserializeSAlterVnodeConfigReq(pReq, len, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return TSDB_CODE_INVALID_MSG;
  }

  vInfo("vgId:%d, start to alter vnode config, page:%d pageSize:%d buffer:%d szPage:%d szBuf:%" PRIu64
        " cacheLast:%d cacheLastSize:%d days:%d keep0:%d keep1:%d keep2:%d fsync:%d level:%d walRetentionPeriod:%d "
        "walRetentionSize:%d",
        TD_VID(pVnode), req.pages, req.pageSize, req.buffer, req.pageSize * 1024, (uint64_t)req.buffer * 1024 * 1024,
        req.cacheLast, req.cacheLastSize, req.daysPerFile, req.daysToKeep0, req.daysToKeep1, req.daysToKeep2,
        req.walFsyncPeriod, req.walLevel, req.walRetentionPeriod, req.walRetentionSize);

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

  if (req.sttTrigger != -1 && req.sttTrigger != pVnode->config.sttTrigger) {
    pVnode->config.sttTrigger = req.sttTrigger;
  }

  if (req.minRows != -1 && req.minRows != pVnode->config.tsdbCfg.minRows) {
    pVnode->config.tsdbCfg.minRows = req.minRows;
  }

  if (walChanged) {
    walAlter(pVnode->pWal, &pVnode->config.walCfg);
  }

  if (tsdbChanged) {
    tsdbSetKeepCfg(pVnode->pTsdb, &pVnode->config.tsdbCfg);
  }

  return 0;
}

static int32_t vnodeProcessBatchDeleteReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SBatchDeleteReq deleteReq;
  SDecoder        decoder;
  tDecoderInit(&decoder, pReq, len);
  tDecodeSBatchDeleteReq(&decoder, &deleteReq);

  SMetaReader mr = {0};
  metaReaderInit(&mr, pVnode->pMeta, META_READER_NOLOCK);

  int32_t sz = taosArrayGetSize(deleteReq.deleteReqs);
  for (int32_t i = 0; i < sz; i++) {
    SSingleDeleteReq *pOneReq = taosArrayGet(deleteReq.deleteReqs, i);
    char             *name = pOneReq->tbname;
    if (metaGetTableEntryByName(&mr, name) < 0) {
      vDebug("vgId:%d, stream delete msg, skip since no table: %s", pVnode->config.vgId, name);
      continue;
    }

    int64_t uid = mr.me.uid;

    int32_t code = tsdbDeleteTableData(pVnode->pTsdb, version, deleteReq.suid, uid, pOneReq->startTs, pOneReq->endTs);
    if (code < 0) {
      terrno = code;
      vError("vgId:%d, delete error since %s, suid:%" PRId64 ", uid:%" PRId64 ", start ts:%" PRId64 ", end ts:%" PRId64,
             TD_VID(pVnode), terrstr(), deleteReq.suid, uid, pOneReq->startTs, pOneReq->endTs);
    }

    tDecoderClear(&mr.coder);
  }
  metaReaderClear(&mr);
  taosArrayDestroy(deleteReq.deleteReqs);
  return 0;
}

static int32_t vnodeProcessDeleteReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
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
  tDecodeDeleteRes(pCoder, pRes);
  ASSERT(taosArrayGetSize(pRes->uidList) == 0 || (pRes->skey != 0 && pRes->ekey != 0));

  for (int32_t iUid = 0; iUid < taosArrayGetSize(pRes->uidList); iUid++) {
    code = tsdbDeleteTableData(pVnode->pTsdb, version, pRes->suid, *(uint64_t *)taosArrayGet(pRes->uidList, iUid),
                               pRes->skey, pRes->ekey);
    if (code) goto _err;
  }

  tDecoderClear(pCoder);
  taosArrayDestroy(pRes->uidList);

  SVDeleteRsp rsp = {.affectedRows = pRes->affectedRows};
  int32_t     ret = 0;
  tEncodeSize(tEncodeSVDeleteRsp, &rsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  SEncoder ec = {0};
  tEncoderInit(&ec, pRsp->pCont, pRsp->contLen);
  tEncodeSVDeleteRsp(&ec, &rsp);
  tEncoderClear(&ec);
  return code;

_err:
  return code;
}
static int32_t vnodeProcessCreateIndexReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVCreateStbReq req = {0};
  SDecoder       dc = {0};

  pRsp->msgType = TDMT_VND_CREATE_INDEX_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  tDecoderInit(&dc, pReq, len);
  // decode req
  if (tDecodeSVCreateStbReq(&dc, &req) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    tDecoderClear(&dc);
    return -1;
  }
  if (metaAddIndexToSTable(pVnode->pMeta, version, &req) < 0) {
    pRsp->code = terrno;
    goto _err;
  }
  tDecoderClear(&dc);
  return 0;
_err:
  tDecoderClear(&dc);
  return -1;
}
static int32_t vnodeProcessDropIndexReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SDropIndexReq req = {0};
  pRsp->msgType = TDMT_VND_DROP_INDEX_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  if (tDeserializeSDropIdxReq(pReq, len, &req)) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  if (metaDropIndexFromSTable(pVnode->pMeta, version, &req) < 0) {
    pRsp->code = terrno;
    return -1;
  }
  return TSDB_CODE_SUCCESS;
}

extern int32_t vnodeProcessCompactVnodeReqImpl(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);

static int32_t vnodeProcessCompactVnodeReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  return vnodeProcessCompactVnodeReqImpl(pVnode, version, pReq, len, pRsp);
}

#ifndef TD_ENTERPRISE
int32_t vnodeProcessCompactVnodeReqImpl(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  return 0;
}
#endif
