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

#include "vnodeInt.h"

static int vnodeProcessCreateStbReq(SVnode *pVnode, int64_t version, void *pReq, int len, SRpcMsg *pRsp);
static int vnodeProcessAlterStbReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp);
static int vnodeProcessDropStbReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp);
static int vnodeProcessCreateTbReq(SVnode *pVnode, int64_t version, void *pReq, int len, SRpcMsg *pRsp);
static int vnodeProcessAlterTbReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp);
static int vnodeProcessDropTbReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp);
static int vnodeProcessSubmitReq(SVnode *pVnode, SSubmitReq *pSubmitReq, SRpcMsg *pRsp);

int vnodePreprocessWriteReqs(SVnode *pVnode, SArray *pMsgs, int64_t *version) {
  SNodeMsg *pMsg;
  SRpcMsg  *pRpc;

  *version = pVnode->state.processed;
  for (int i = 0; i < taosArrayGetSize(pMsgs); i++) {
    pMsg = *(SNodeMsg **)taosArrayGet(pMsgs, i);
    pRpc = &pMsg->rpcMsg;

    // set request version
    if (walWrite(pVnode->pWal, pVnode->state.processed++, pRpc->msgType, pRpc->pCont, pRpc->contLen) < 0) {
      vError("vnode:%d  write wal error since %s", TD_VID(pVnode), terrstr());
      return -1;
    }
  }

  walFsync(pVnode->pWal, false);

  return 0;
}

int vnodeProcessWriteReq(SVnode *pVnode, SRpcMsg *pMsg, int64_t version, SRpcMsg *pRsp) {
  void *ptr = NULL;
  void *pReq;
  int   len;
  int   ret;

  vTrace("vgId: %d start to process write request %s, version %" PRId64, TD_VID(pVnode), TMSG_INFO(pMsg->msgType),
         version);

  pVnode->state.applied = version;

  // skip header
  pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  len = pMsg->contLen - sizeof(SMsgHead);

  // todo: change the interface here
  if (tqPushMsg(pVnode->pTq, pMsg->pCont, pMsg->contLen, pMsg->msgType, version) < 0) {
    vError("vgId: %d failed to push msg to TQ since %s", TD_VID(pVnode), tstrerror(terrno));
    return -1;
  }

  switch (pMsg->msgType) {
    /* META */
    case TDMT_VND_CREATE_STB:
      if (vnodeProcessCreateStbReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_ALTER_STB:
      if (vnodeProcessAlterStbReq(pVnode, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_DROP_STB:
      if (vnodeProcessDropStbReq(pVnode, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_CREATE_TABLE:
      if (vnodeProcessCreateTbReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_ALTER_TABLE:
      if (vnodeProcessAlterTbReq(pVnode, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_DROP_TABLE:
      if (vnodeProcessDropTbReq(pVnode, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_CREATE_SMA: {  // timeRangeSMA
      if (tsdbCreateTSma(pVnode->pTsdb, POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead))) < 0) {
        // TODO
      }
    } break;
    /* TSDB */
    case TDMT_VND_SUBMIT:
      pRsp->msgType = TDMT_VND_SUBMIT_RSP;
      vnodeProcessSubmitReq(pVnode, ptr, pRsp);
      break;
    /* TQ */
    case TDMT_VND_MQ_VG_CHANGE:
      if (tqProcessVgChangeReq(pVnode->pTq, POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)),
                               pMsg->contLen - sizeof(SMsgHead)) < 0) {
        // TODO: handle error
      }
      break;
#if 0
    case TDMT_VND_MQ_SET_CONN: {
      if (tqProcessSetConnReq(pVnode->pTq, POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead))) < 0) {
        // TODO: handle error
      }
    } break;
    case TDMT_VND_MQ_REB: {
      if (tqProcessRebReq(pVnode->pTq, POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead))) < 0) {
      }
    } break;
    case TDMT_VND_MQ_CANCEL_CONN: {
      if (tqProcessCancelConnReq(pVnode->pTq, POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead))) < 0) {
      }
    } break;
#endif
    case TDMT_VND_TASK_DEPLOY: {
      if (tqProcessTaskDeploy(pVnode->pTq, POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)),
                              pMsg->contLen - sizeof(SMsgHead)) < 0) {
      }
    } break;
    case TDMT_VND_TASK_WRITE_EXEC: {
      if (tqProcessTaskExec(pVnode->pTq, POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)), pMsg->contLen - sizeof(SMsgHead),
                            0) < 0) {
      }
    } break;
    case TDMT_VND_ALTER_VNODE:
      break;
    default:
      ASSERT(0);
      break;
  }

  vDebug("vgId: %d process %s request success, version: %" PRId64, TD_VID(pVnode), TMSG_INFO(pMsg->msgType), version);

  // Check if it needs to commit
  if (vnodeShouldCommit(pVnode)) {
    // tsem_wait(&(pVnode->canCommit));
    if (vnodeAsyncCommit(pVnode) < 0) {
      // TODO: handle error
    }
  }

  return 0;

_err:
  vDebug("vgId: %d process %s request failed since %s, version: %" PRId64, TD_VID(pVnode), TMSG_INFO(pMsg->msgType),
         tstrerror(terrno), version);
  return -1;
}

int vnodeProcessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  vTrace("message in query queue is processing");
  SReadHandle handle = {.reader = pVnode->pTsdb, .meta = pVnode->pMeta, .config = &pVnode->config};

  switch (pMsg->msgType) {
    case TDMT_VND_QUERY:
      return qWorkerProcessQueryMsg(&handle, pVnode->pQuery, pMsg);
    case TDMT_VND_QUERY_CONTINUE:
      return qWorkerProcessCQueryMsg(&handle, pVnode->pQuery, pMsg);
    default:
      vError("unknown msg type:%d in query queue", pMsg->msgType);
      return TSDB_CODE_VND_APP_ERROR;
  }
}

int vnodeProcessFetchMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo) {
  vTrace("message in fetch queue is processing");
  char   *msgstr = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);
  switch (pMsg->msgType) {
    case TDMT_VND_FETCH:
      return qWorkerProcessFetchMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_FETCH_RSP:
      return qWorkerProcessFetchRsp(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_RES_READY:
      return qWorkerProcessReadyMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_TASKS_STATUS:
      return qWorkerProcessStatusMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_CANCEL_TASK:
      return qWorkerProcessCancelMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_DROP_TASK:
      return qWorkerProcessDropMsg(pVnode, pVnode->pQuery, pMsg);
    case TDMT_VND_TABLE_META:
      return vnodeGetTableMeta(pVnode, pMsg);
    case TDMT_VND_CONSUME:
      return tqProcessPollReq(pVnode->pTq, pMsg, pInfo->workerId);
    case TDMT_VND_TASK_PIPE_EXEC:
    case TDMT_VND_TASK_MERGE_EXEC:
      return tqProcessTaskExec(pVnode->pTq, msgstr, msgLen, 0);
    case TDMT_VND_STREAM_TRIGGER:
      return tqProcessStreamTrigger(pVnode->pTq, pMsg->pCont, pMsg->contLen, 0);
    case TDMT_VND_QUERY_HEARTBEAT:
      return qWorkerProcessHbMsg(pVnode, pVnode->pQuery, pMsg);
    default:
      vError("unknown msg type:%d in fetch queue", pMsg->msgType);
      return TSDB_CODE_VND_APP_ERROR;
  }
}

// TODO: remove the function
void smaHandleRes(void *pVnode, int64_t smaId, const SArray *data) {
  // TODO

  // blockDebugShowData(data);
  tsdbInsertTSmaData(((SVnode *)pVnode)->pTsdb, smaId, (const char *)data);
}

int vnodeProcessSyncReq(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp) {
  /*vInfo("sync message is processed");*/
  return 0;
}

static int vnodeProcessCreateStbReq(SVnode *pVnode, int64_t version, void *pReq, int len, SRpcMsg *pRsp) {
  SVCreateStbReq req = {0};
  SCoder         coder;

  pRsp->msgType = TDMT_VND_CREATE_STB_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  // decode and process req
  tCoderInit(&coder, TD_LITTLE_ENDIAN, pReq, len, TD_DECODER);

  if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
    pRsp->code = terrno;
    goto _err;
  }

  if (metaCreateSTable(pVnode->pMeta, version, &req) < 0) {
    pRsp->code = terrno;
    goto _err;
  }

  tCoderClear(&coder);
  return 0;

_err:
  tCoderClear(&coder);
  return -1;
}

static int vnodeProcessCreateTbReq(SVnode *pVnode, int64_t version, void *pReq, int len, SRpcMsg *pRsp) {
  SCoder             coder = {0};
  int                rcode = 0;
  SVCreateTbBatchReq req = {0};
  SVCreateTbReq     *pCreateReq;
  SVCreateTbBatchRsp rsp = {0};
  SVCreateTbRsp      cRsp = {0};
  char               tbName[TSDB_TABLE_FNAME_LEN];

  pRsp->msgType = TDMT_VND_CREATE_TABLE_RSP;
  pRsp->code = TSDB_CODE_SUCCESS;
  pRsp->pCont = NULL;
  pRsp->contLen = 0;

  // decode
  tCoderInit(&coder, TD_LITTLE_ENDIAN, pReq, len, TD_DECODER);
  if (tDecodeSVCreateTbBatchReq(&coder, &req) < 0) {
    rcode = -1;
    terrno = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  rsp.pArray = taosArrayInit(sizeof(cRsp), req.nReqs);
  if (rsp.pArray == NULL) {
    rcode = -1;
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // loop to create table
  for (int iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;

    // validate hash
    sprintf(tbName, "%s.%s", pVnode->config.dbname, pCreateReq->name);
    if (vnodeValidateTableHash(pVnode, tbName) < 0) {
      cRsp.code = TSDB_CODE_VND_HASH_MISMATCH;
      taosArrayPush(rsp.pArray, &cRsp);
      continue;
    }

    // do create table
    if (metaCreateTable(pVnode->pMeta, version, pCreateReq) < 0) {
      cRsp.code = terrno;
    } else {
      cRsp.code = TSDB_CODE_SUCCESS;
    }

    taosArrayPush(rsp.pArray, &cRsp);
  }

  tCoderClear(&coder);

  // prepare rsp
  tEncodeSize(tEncodeSVCreateTbBatchRsp, &rsp, pRsp->contLen);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  if (pRsp->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    rcode = -1;
    goto _exit;
  }
  tCoderInit(&coder, TD_LITTLE_ENDIAN, pRsp->pCont, pRsp->contLen, TD_ENCODER);
  tEncodeSVCreateTbBatchRsp(&coder, &rsp);

_exit:
  taosArrayClear(rsp.pArray);
  tCoderClear(&coder);
  return rcode;
}

static int vnodeProcessAlterStbReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp) {
  ASSERT(0);
#if 0
  SVCreateTbReq vAlterTbReq = {0};
  vTrace("vgId:%d, process alter stb req", TD_VID(pVnode));
  tDeserializeSVCreateTbReq(pReq, &vAlterTbReq);
  // TODO: to encapsule a free API
  // taosMemoryFree(vAlterTbReq.stbCfg.pSchema);
  // taosMemoryFree(vAlterTbReq.stbCfg.pTagSchema);
  // if (vAlterTbReq.stbCfg.pRSmaParam) {
  //   taosMemoryFree(vAlterTbReq.stbCfg.pRSmaParam->pFuncIds);
  //   taosMemoryFree(vAlterTbReq.stbCfg.pRSmaParam);
  // }
  taosMemoryFree(vAlterTbReq.name);
#endif
  return 0;
}

static int vnodeProcessDropStbReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp) {
  // TODO
  ASSERT(0);
  return 0;
}

static int vnodeProcessAlterTbReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp) {
  // TODO
  ASSERT(0);
  return 0;
}

static int vnodeProcessDropTbReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp) {
  // TODO
  ASSERT(0);
  return 0;
}

static int vnodeProcessSubmitReq(SVnode *pVnode, SSubmitReq *pSubmitReq, SRpcMsg *pRsp) {
  SSubmitRsp rsp = {0};

  pRsp->code = 0;

  // handle the request
  if (tsdbInsertData(pVnode->pTsdb, pSubmitReq, &rsp) < 0) {
    pRsp->code = terrno;
    return -1;
  }

  // encode the response (TODO)
  pRsp->pCont = rpcMallocCont(sizeof(SSubmitRsp));
  memcpy(pRsp->pCont, &rsp, sizeof(rsp));
  pRsp->contLen = sizeof(SSubmitRsp);

  return 0;
}
