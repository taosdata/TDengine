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

#include "vnd.h"

static int32_t vnodeProcessCreateStbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterStbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropStbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessCreateTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessSubmitReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessCreateTSmaReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterConfirmReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterHashRangeReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessAlterConfigReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDropTtlTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessTrimReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessDeleteReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int32_t vnodeProcessBatchDeleteReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);

int32_t vnodePreProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  int32_t  code = 0;
  SDecoder dc = {0};

  switch (pMsg->msgType) {
    case TDMT_VND_CREATE_TABLE: {
      int64_t ctime = taosGetTimestampMs();
      int32_t nReqs;

      tDecoderInit(&dc, (uint8_t *)pMsg->pCont + sizeof(SMsgHead), pMsg->contLen - sizeof(SMsgHead));
      if (tStartDecode(&dc) < 0) {
        code = TSDB_CODE_INVALID_MSG;
        return code;
      }

      if (tDecodeI32v(&dc, &nReqs) < 0) {
        code = TSDB_CODE_INVALID_MSG;
        goto _err;
      }
      for (int32_t iReq = 0; iReq < nReqs; iReq++) {
        tb_uid_t uid = tGenIdPI64();
        char    *name = NULL;
        if (tStartDecode(&dc) < 0) {
          code = TSDB_CODE_INVALID_MSG;
          goto _err;
        }

        if (tDecodeI32v(&dc, NULL) < 0) {
          code = TSDB_CODE_INVALID_MSG;
          return code;
        }
        if (tDecodeCStr(&dc, &name) < 0) {
          code = TSDB_CODE_INVALID_MSG;
          return code;
        }
        *(int64_t *)(dc.data + dc.pos) = uid;
        *(int64_t *)(dc.data + dc.pos + 8) = ctime;

        vTrace("vgId:%d, table:%s uid:%" PRId64 " is generated", pVnode->config.vgId, name, uid);
        tEndDecode(&dc);
      }

      tEndDecode(&dc);
      tDecoderClear(&dc);
    } break;
    case TDMT_VND_SUBMIT: {
      SSubmitMsgIter msgIter = {0};
      SSubmitReq    *pSubmitReq = (SSubmitReq *)pMsg->pCont;
      SSubmitBlk    *pBlock = NULL;
      int64_t        ctime = taosGetTimestampMs();
      tb_uid_t       uid;

      if (tInitSubmitMsgIter(pSubmitReq, &msgIter) < 0) {
        code = terrno;
        goto _err;
      }

      for (;;) {
        tGetSubmitMsgNext(&msgIter, &pBlock);
        if (pBlock == NULL) break;

        if (msgIter.schemaLen > 0) {
          char *name = NULL;

          tDecoderInit(&dc, pBlock->data, msgIter.schemaLen);
          if (tStartDecode(&dc) < 0) {
            code = TSDB_CODE_INVALID_MSG;
            return code;
          }

          if (tDecodeI32v(&dc, NULL) < 0) {
            code = TSDB_CODE_INVALID_MSG;
            return code;
          }
          if (tDecodeCStr(&dc, &name) < 0) {
            code = TSDB_CODE_INVALID_MSG;
            return code;
          }

          uid = metaGetTableEntryUidByName(pVnode->pMeta, name);
          if (uid == 0) {
            uid = tGenIdPI64();
          }
          *(int64_t *)(dc.data + dc.pos) = uid;
          *(int64_t *)(dc.data + dc.pos + 8) = ctime;
          pBlock->uid = htobe64(uid);

          tEndDecode(&dc);
          tDecoderClear(&dc);
        }
      }

    } break;
    case TDMT_VND_DELETE: {
      int32_t     size;
      int32_t     ret;
      uint8_t    *pCont;
      SEncoder   *pCoder = &(SEncoder){0};
      SDeleteRes  res = {0};
      SReadHandle handle = {
          .meta = pVnode->pMeta, .config = &pVnode->config, .vnode = pVnode, .pMsgCb = &pVnode->msgCb};

      code = qWorkerProcessDeleteMsg(&handle, pVnode->pQuery, pMsg, &res);
      if (code) {
        goto _err;
      }

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
    } break;
    default:
      break;
  }

  return code;

_err:
  vError("vgId%d, preprocess request failed since %s", TD_VID(pVnode), tstrerror(code));
  return code;
}

int32_t vnodeProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg, int64_t version, SRpcMsg *pRsp) {
  void   *ptr = NULL;
  void   *pReq;
  int32_t len;
  int32_t ret;

  if (!pVnode->inUse) {
    terrno = TSDB_CODE_VND_NO_AVAIL_BUFPOOL;
    vError("vgId:%d, not ready to write since %s", TD_VID(pVnode), terrstr());
    return -1;
  }

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

  pVnode->state.applied = version;
  pVnode->state.applyTerm = pMsg->info.conn.applyTerm;

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
      if (tqProcessTaskDeployReq(pVnode->pTq, version, pReq, len) < 0) {
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
    case TDMT_VND_ALTER_HASHRANGE:
      vnodeProcessAlterHashRangeReq(pVnode, version, pReq, len, pRsp);
      break;
    case TDMT_VND_ALTER_CONFIG:
      vnodeProcessAlterConfigReq(pVnode, version, pReq, len, pRsp);
      break;
    case TDMT_VND_COMMIT:
      needCommit = true;
      break;
    default:
      vError("vgId:%d, unprocessed msg, %d", TD_VID(pVnode), pMsg->msgType);
      return -1;
  }

  vTrace("vgId:%d, process %s request, code:0x%x index:%" PRId64, TD_VID(pVnode), TMSG_INFO(pMsg->msgType), pRsp->code,
         version);

  walApplyVer(pVnode->pWal, version);

  if (tqPushMsg(pVnode->pTq, pMsg->pCont, pMsg->contLen, pMsg->msgType, version) < 0) {
    vError("vgId:%d, failed to push msg to TQ since %s", TD_VID(pVnode), tstrerror(terrno));
    return -1;
  }

  // commit if need
  if (needCommit) {
    vInfo("vgId:%d, commit at version %" PRId64, TD_VID(pVnode), version);
    vnodeAsyncCommit(pVnode);

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
  // if ((pMsg->msgType == TDMT_SCH_QUERY) && !vnodeIsLeader(pVnode)) {
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
       pMsg->msgType == TDMT_VND_BATCH_META) &&
      !syncIsReadyForRead(pVnode->sync)) {
    //      !vnodeIsLeader(pVnode)) {
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
#if 1
    case TDMT_STREAM_TASK_DISPATCH:
      return tqProcessTaskDispatchReq(pVnode->pTq, pMsg, true);
#endif
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
  code = tsdbDoRetention(pVnode->pTsdb, trimReq.timestamp);
  if (code) goto _exit;

  code = smaDoRetention(pVnode->pSma, trimReq.timestamp);
  if (code) goto _exit;

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

static int32_t vnodeDebugPrintSubmitMsg(SVnode *pVnode, SSubmitReq *pMsg, const char *tags) {
  ASSERT(pMsg != NULL);
  SSubmitMsgIter msgIter = {0};
  SMeta         *pMeta = pVnode->pMeta;
  SSubmitBlk    *pBlock = NULL;

  if (tInitSubmitMsgIter(pMsg, &msgIter) < 0) return -1;
  while (true) {
    if (tGetSubmitMsgNext(&msgIter, &pBlock) < 0) return -1;
    if (pBlock == NULL) break;

    vnodeDebugPrintSingleSubmitMsg(pMeta, pBlock, &msgIter, tags);
  }

  return 0;
}

static int32_t vnodeProcessSubmitReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SSubmitReq    *pSubmitReq = (SSubmitReq *)pReq;
  SSubmitRsp     submitRsp = {0};
  SSubmitMsgIter msgIter = {0};
  SSubmitBlk    *pBlock = NULL;
  SVCreateTbReq  createTbReq = {0};
  SDecoder       decoder = {0};
  int32_t        nRows = 0;
  int32_t        tsize, ret;
  SEncoder       encoder = {0};
  SArray        *newTbUids = NULL;
  SVStatis       statis = {0};
  bool           tbCreated = false;
  terrno = TSDB_CODE_SUCCESS;

  pRsp->code = 0;
  pSubmitReq->version = version;
  statis.nBatchInsert = 1;

#ifdef TD_DEBUG_PRINT_ROW
  vnodeDebugPrintSubmitMsg(pVnode, pReq, __func__);
#endif

  if (tsdbScanAndConvertSubmitMsg(pVnode->pTsdb, pSubmitReq) < 0) {
    pRsp->code = terrno;
    goto _exit;
  }

  // handle the request
  if (tInitSubmitMsgIter(pSubmitReq, &msgIter) < 0) {
    pRsp->code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  submitRsp.pArray = taosArrayInit(msgIter.numOfBlocks, sizeof(SSubmitBlkRsp));
  newTbUids = taosArrayInit(msgIter.numOfBlocks, sizeof(int64_t));
  if (!submitRsp.pArray || !newTbUids) {
    pRsp->code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  for (;;) {
    tGetSubmitMsgNext(&msgIter, &pBlock);
    if (pBlock == NULL) break;

    SSubmitBlkRsp submitBlkRsp = {0};
    tbCreated = false;

    // create table for auto create table mode
    if (msgIter.schemaLen > 0) {
      tDecoderInit(&decoder, pBlock->data, msgIter.schemaLen);
      if (tDecodeSVCreateTbReq(&decoder, &createTbReq) < 0) {
        pRsp->code = TSDB_CODE_INVALID_MSG;
        tDecoderClear(&decoder);
        taosArrayDestroy(createTbReq.ctb.tagName);
        goto _exit;
      }

      if ((terrno = grantCheck(TSDB_GRANT_TIMESERIES)) < 0) {
        pRsp->code = terrno;
        tDecoderClear(&decoder);
        taosArrayDestroy(createTbReq.ctb.tagName);
        goto _exit;
      }

      if ((terrno = grantCheck(TSDB_GRANT_TABLE)) < 0) {
        pRsp->code = terrno;
        tDecoderClear(&decoder);
        taosArrayDestroy(createTbReq.ctb.tagName);
        goto _exit;
      }

      if (metaCreateTable(pVnode->pMeta, version, &createTbReq, &submitBlkRsp.pMeta) < 0) {
        if (terrno != TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
          submitBlkRsp.code = terrno;
          pRsp->code = terrno;
          tDecoderClear(&decoder);
          taosArrayDestroy(createTbReq.ctb.tagName);
          goto _exit;
        }
      } else {
        if (NULL != submitBlkRsp.pMeta) {
          vnodeUpdateMetaRsp(pVnode, submitBlkRsp.pMeta);
        }

        taosArrayPush(newTbUids, &createTbReq.uid);

        submitBlkRsp.uid = createTbReq.uid;
        submitBlkRsp.tblFName = taosMemoryMalloc(strlen(pVnode->config.dbname) + strlen(createTbReq.name) + 2);
        sprintf(submitBlkRsp.tblFName, "%s.%s", pVnode->config.dbname, createTbReq.name);
        tbCreated = true;
      }

      msgIter.uid = createTbReq.uid;
      if (createTbReq.type == TSDB_CHILD_TABLE) {
        msgIter.suid = createTbReq.ctb.suid;
      } else {
        msgIter.suid = 0;
      }

#ifdef TD_DEBUG_PRINT_ROW
      vnodeDebugPrintSingleSubmitMsg(pVnode->pMeta, pBlock, &msgIter, "real uid");
#endif
      tDecoderClear(&decoder);
      taosArrayDestroy(createTbReq.ctb.tagName);
    }

    if (tsdbInsertTableData(pVnode->pTsdb, version, &msgIter, pBlock, &submitBlkRsp) < 0) {
      submitBlkRsp.code = terrno;
    }

    submitRsp.numOfRows += submitBlkRsp.numOfRows;
    submitRsp.affectedRows += submitBlkRsp.affectedRows;
    if (tbCreated || submitBlkRsp.code) {
      taosArrayPush(submitRsp.pArray, &submitBlkRsp);
    }
  }

  if (taosArrayGetSize(newTbUids) > 0) {
    vDebug("vgId:%d, add %d table into query table list in handling submit", TD_VID(pVnode),
           (int32_t)taosArrayGetSize(newTbUids));
  }

  tqUpdateTbUidList(pVnode->pTq, newTbUids, true);

_exit:
  taosArrayDestroy(newTbUids);
  tEncodeSize(tEncodeSSubmitRsp, &submitRsp, tsize, ret);
  pRsp->pCont = rpcMallocCont(tsize);
  pRsp->contLen = tsize;
  tEncoderInit(&encoder, pRsp->pCont, tsize);
  tEncodeSSubmitRsp(&encoder, &submitRsp);
  tEncoderClear(&encoder);

  taosArrayDestroyEx(submitRsp.pArray, tFreeSSubmitBlkRsp);

  // TODO: the partial success scenario and the error case
  // => If partial success, extract the success submitted rows and reconstruct a new submit msg, and push to level
  // 1/level 2.
  // TODO: refactor
  if ((terrno == TSDB_CODE_SUCCESS) && (pRsp->code == TSDB_CODE_SUCCESS)) {
    statis.nBatchInsertSuccess = 1;
    tdProcessRSmaSubmit(pVnode->pSma, pReq, STREAM_INPUT__DATA_SUBMIT);
  }

  // N.B. not strict as the following procedure is not atomic
  atomic_add_fetch_64(&pVnode->statis.nInsert, submitRsp.numOfRows);
  atomic_add_fetch_64(&pVnode->statis.nInsertSuccess, submitRsp.affectedRows);
  atomic_add_fetch_64(&pVnode->statis.nBatchInsert, statis.nBatchInsert);
  atomic_add_fetch_64(&pVnode->statis.nBatchInsertSuccess, statis.nBatchInsertSuccess);

  vDebug("vgId:%d, submit success, index:%" PRId64, pVnode->config.vgId, version);
  return 0;
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
  vInfo("vgId:%d, alter replica confim msg is processed", TD_VID(pVnode));
  pRsp->msgType = TDMT_VND_ALTER_CONFIRM_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  return 0;
}

static int32_t vnodeProcessAlterHashRangeReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  vInfo("vgId:%d, alter hashrange msg will be processed", TD_VID(pVnode));

  // todo
  // 1. stop work
  // 2. adjust hash range / compact / remove wals / rename vgroups
  // 3. reload sync
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
        " cacheLast:%d cacheLastSize:%d days:%d keep0:%d keep1:%d keep2:%d fsync:%d level:%d",
        TD_VID(pVnode), req.pages, req.pageSize, req.buffer, req.pageSize * 1024, (uint64_t)req.buffer * 1024 * 1024,
        req.cacheLast, req.cacheLastSize, req.daysPerFile, req.daysToKeep0, req.daysToKeep1, req.daysToKeep2,
        req.walFsyncPeriod, req.walLevel);

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
