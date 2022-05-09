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

static int vnodeProcessCreateStbReq(SVnode *pVnode, int64_t version, void *pReq, int len, SRpcMsg *pRsp);
static int vnodeProcessAlterStbReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp);
static int vnodeProcessDropStbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int vnodeProcessCreateTbReq(SVnode *pVnode, int64_t version, void *pReq, int len, SRpcMsg *pRsp);
static int vnodeProcessAlterTbReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp);
static int vnodeProcessDropTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);
static int vnodeProcessSubmitReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp);

int vnodePreprocessWriteReqs(SVnode *pVnode, SArray *pMsgs, int64_t *version) {
#if 0
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

#endif
  return 0;
}

int vnodeProcessWriteReq(SVnode *pVnode, SRpcMsg *pMsg, int64_t version, SRpcMsg *pRsp) {
  void *ptr = NULL;
  void *pReq;
  int   len;
  int   ret;

  vTrace("vgId:%d start to process write request %s, version %" PRId64, TD_VID(pVnode), TMSG_INFO(pMsg->msgType),
         version);

  pVnode->state.applied = version;

  // skip header
  pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  len = pMsg->contLen - sizeof(SMsgHead);

  if (tqPushMsg(pVnode->pTq, pMsg->pCont, pMsg->contLen, pMsg->msgType, version) < 0) {
    vError("vgId:%d failed to push msg to TQ since %s", TD_VID(pVnode), tstrerror(terrno));
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
      if (vnodeProcessDropStbReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_CREATE_TABLE:
      if (vnodeProcessCreateTbReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_ALTER_TABLE:
      if (vnodeProcessAlterTbReq(pVnode, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_DROP_TABLE:
      if (vnodeProcessDropTbReq(pVnode, version, pReq, len, pRsp) < 0) goto _err;
      break;
    case TDMT_VND_CREATE_SMA: {  // timeRangeSMA
      if (tsdbCreateTSma(pVnode->pTsdb, POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead))) < 0) {
        // TODO
      }
    } break;
    /* TSDB */
    case TDMT_VND_SUBMIT:
      if (vnodeProcessSubmitReq(pVnode, version, pMsg->pCont, pMsg->contLen, pRsp) < 0) goto _err;
      break;
    /* TQ */
    case TDMT_VND_MQ_VG_CHANGE:
      if (tqProcessVgChangeReq(pVnode->pTq, POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead)),
                               pMsg->contLen - sizeof(SMsgHead)) < 0) {
        // TODO: handle error
      }
      break;
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

  vDebug("vgId:%d process %s request success, version: %" PRId64, TD_VID(pVnode), TMSG_INFO(pMsg->msgType), version);

  // commit if need
  if (vnodeShouldCommit(pVnode)) {
    vInfo("vgId:%d commit at version %" PRId64, TD_VID(pVnode), version);
    // commit current change
    vnodeCommit(pVnode);

    // start a new one
    vnodeBegin(pVnode);
  }

  return 0;

_err:
  vDebug("vgId:%d process %s request failed since %s, version: %" PRId64, TD_VID(pVnode), TMSG_INFO(pMsg->msgType),
         tstrerror(terrno), version);
  return -1;
}

int vnodeProcessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg) {
  vTrace("message in vnode query queue is processing");
#if 0
  SReadHandle handle = {.reader = pVnode->pTsdb, .meta = pVnode->pMeta, .config = &pVnode->config, .vnode = pVnode, .pMsgCb = &pVnode->msgCb};
#endif
  SReadHandle handle = {.meta = pVnode->pMeta, .config = &pVnode->config, .vnode = pVnode, .pMsgCb = &pVnode->msgCb};
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
  if (syncEnvIsStart()) {
    SSyncNode *pSyncNode = syncNodeAcquire(pVnode->sync);
    assert(pSyncNode != NULL);

    ESyncState state = syncGetMyRole(pVnode->sync);
    SyncTerm   currentTerm = syncGetMyTerm(pVnode->sync);

    SMsgHead *pHead = pMsg->pCont;

    char  logBuf[512];
    char *syncNodeStr = sync2SimpleStr(pVnode->sync);
    snprintf(logBuf, sizeof(logBuf), "==vnodeProcessSyncReq== msgType:%d, syncNode: %s", pMsg->msgType, syncNodeStr);
    syncRpcMsgLog2(logBuf, pMsg);
    taosMemoryFree(syncNodeStr);

    SRpcMsg *pRpcMsg = pMsg;

    if (pRpcMsg->msgType == TDMT_VND_SYNC_TIMEOUT) {
      SyncTimeout *pSyncMsg = syncTimeoutFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      syncNodeOnTimeoutCb(pSyncNode, pSyncMsg);
      syncTimeoutDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_VND_SYNC_PING) {
      SyncPing *pSyncMsg = syncPingFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      syncNodeOnPingCb(pSyncNode, pSyncMsg);
      syncPingDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_VND_SYNC_PING_REPLY) {
      SyncPingReply *pSyncMsg = syncPingReplyFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      syncNodeOnPingReplyCb(pSyncNode, pSyncMsg);
      syncPingReplyDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_VND_SYNC_CLIENT_REQUEST) {
      SyncClientRequest *pSyncMsg = syncClientRequestFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      syncNodeOnClientRequestCb(pSyncNode, pSyncMsg);
      syncClientRequestDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_VND_SYNC_REQUEST_VOTE) {
      SyncRequestVote *pSyncMsg = syncRequestVoteFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      syncNodeOnRequestVoteCb(pSyncNode, pSyncMsg);
      syncRequestVoteDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_VND_SYNC_REQUEST_VOTE_REPLY) {
      SyncRequestVoteReply *pSyncMsg = syncRequestVoteReplyFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      syncNodeOnRequestVoteReplyCb(pSyncNode, pSyncMsg);
      syncRequestVoteReplyDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_VND_SYNC_APPEND_ENTRIES) {
      SyncAppendEntries *pSyncMsg = syncAppendEntriesFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      syncNodeOnAppendEntriesCb(pSyncNode, pSyncMsg);
      syncAppendEntriesDestroy(pSyncMsg);

    } else if (pRpcMsg->msgType == TDMT_VND_SYNC_APPEND_ENTRIES_REPLY) {
      SyncAppendEntriesReply *pSyncMsg = syncAppendEntriesReplyFromRpcMsg2(pRpcMsg);
      assert(pSyncMsg != NULL);

      syncNodeOnAppendEntriesReplyCb(pSyncNode, pSyncMsg);
      syncAppendEntriesReplyDestroy(pSyncMsg);

    } else {
      vError("==vnodeProcessSyncReq== error msg type:%d", pRpcMsg->msgType);
    }

    syncNodeRelease(pSyncNode);
  } else {
    vError("==vnodeProcessSyncReq== error syncEnv stop");
  }
  return 0;
}

static int vnodeProcessCreateStbReq(SVnode *pVnode, int64_t version, void *pReq, int len, SRpcMsg *pRsp) {
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

  tsdbRegisterRSma(pVnode->pTsdb, pVnode->pMeta, &req, &pVnode->msgCb);

  tDecoderClear(&coder);
  return 0;

_err:
  tDecoderClear(&coder);
  return -1;
}

static int vnodeProcessCreateTbReq(SVnode *pVnode, int64_t version, void *pReq, int len, SRpcMsg *pRsp) {
  SDecoder           decoder = {0};
  int                rcode = 0;
  SVCreateTbBatchReq req = {0};
  SVCreateTbReq     *pCreateReq;
  SVCreateTbBatchRsp rsp = {0};
  SVCreateTbRsp      cRsp = {0};
  char               tbName[TSDB_TABLE_FNAME_LEN];
  STbUidStore       *pStore = NULL;

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
      if (pCreateReq->flags & TD_CREATE_IF_NOT_EXISTS && terrno == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
        cRsp.code = TSDB_CODE_SUCCESS;
      } else {
        cRsp.code = terrno;
      }
    } else {
      cRsp.code = TSDB_CODE_SUCCESS;
      tsdbFetchTbUidList(pVnode->pTsdb, &pStore, pCreateReq->ctb.suid, pCreateReq->uid);
    }

    taosArrayPush(rsp.pArray, &cRsp);
  }

  tDecoderClear(&decoder);

  tsdbUpdateTbUidList(pVnode->pTsdb, pStore);
  tsdbUidStoreFree(pStore);

  // prepare rsp
  SEncoder encoder = {0};
  int32_t  ret = 0;
  tEncodeSize(tEncodeSVCreateTbBatchRsp, &rsp, pRsp->contLen, ret);
  pRsp->pCont = rpcMallocCont(pRsp->contLen);
  if (pRsp->pCont == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    rcode = -1;
    goto _exit;
  }
  tEncoderInit(&encoder, pRsp->pCont, pRsp->contLen);
  tEncodeSVCreateTbBatchRsp(&encoder, &rsp);
  tEncoderClear(&encoder);

_exit:
  taosArrayClear(rsp.pArray);
  tDecoderClear(&decoder);
  tEncoderClear(&encoder);
  return rcode;
}

static int vnodeProcessAlterStbReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp) {
  // ASSERT(0);
#if 0
  SVCreateTbReq vAlterTbReq = {0};
  vTrace("vgId:%d, process alter stb req", TD_VID(pVnode));
  tDeserializeSVCreateTbReq(pReq, &vAlterTbReq);
  // TODO: to encapsule a free API
  taosMemoryFree(vAlterTbReq.stbCfg.pSchema);
  taosMemoryFree(vAlterTbReq.stbCfg.pTagSchema);
  if (vAlterTbReq.stbCfg.pRSmaParam) {
    taosMemoryFree(vAlterTbReq.stbCfg.pRSmaParam);
  }
  taosMemoryFree(vAlterTbReq.name);
#endif
  return 0;
}

static int vnodeProcessDropStbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVDropStbReq req = {0};
  int          rcode = TSDB_CODE_SUCCESS;
  SDecoder     decoder = {0};

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
  // if (metaDropSTable(pVnode->pMeta, version, &req) < 0) {
  //   rcode = terrno;
  //   goto _exit;
  // }

  // return rsp
_exit:
  pRsp->code = rcode;
  tDecoderClear(&decoder);
  return 0;
}

static int vnodeProcessAlterTbReq(SVnode *pVnode, void *pReq, int32_t len, SRpcMsg *pRsp) {
  // TODO
  ASSERT(0);
  return 0;
}

static int vnodeProcessDropTbReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SVDropTbBatchReq req = {0};
  SVDropTbBatchRsp rsp = {0};
  SDecoder         decoder = {0};
  int              ret;

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
  rsp.pArray = taosArrayInit(sizeof(SVDropTbRsp), req.nReqs);
  for (int iReq = 0; iReq < req.nReqs; iReq++) {
    SVDropTbReq *pDropTbReq = req.pReqs + iReq;
    SVDropTbRsp  dropTbRsp = {0};

    /* code */
    ret = metaDropTable(pVnode->pMeta, version, pDropTbReq);
    if (ret < 0) {
      if (pDropTbReq->igNotExists && terrno == TSDB_CODE_VND_TABLE_NOT_EXIST) {
        dropTbRsp.code = TSDB_CODE_SUCCESS;
      } else {
        dropTbRsp.code = terrno;
      }
    } else {
      dropTbRsp.code = TSDB_CODE_SUCCESS;
    }

    taosArrayPush(rsp.pArray, &dropTbRsp);
  }

_exit:
  tDecoderClear(&decoder);
  // encode rsp (TODO)
  return 0;
}

static int vnodeProcessSubmitReq(SVnode *pVnode, int64_t version, void *pReq, int32_t len, SRpcMsg *pRsp) {
  SSubmitReq    *pSubmitReq = (SSubmitReq *)pReq;
  SSubmitMsgIter msgIter = {0};
  SSubmitBlk    *pBlock;
  SSubmitRsp     rsp = {0};
  SVCreateTbReq  createTbReq = {0};
  SDecoder       decoder = {0};
  int32_t        nRows;

  pRsp->code = 0;

  // handle the request
  if (tInitSubmitMsgIter(pSubmitReq, &msgIter) < 0) {
    pRsp->code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  for (;;) {
    tGetSubmitMsgNext(&msgIter, &pBlock);
    if (pBlock == NULL) break;

    // create table for auto create table mode
    if (msgIter.schemaLen > 0) {
      tDecoderInit(&decoder, pBlock->data, msgIter.schemaLen);
      if (tDecodeSVCreateTbReq(&decoder, &createTbReq) < 0) {
        pRsp->code = TSDB_CODE_INVALID_MSG;
        tDecoderClear(&decoder);
        goto _exit;
      }

      if (metaCreateTable(pVnode->pMeta, version, &createTbReq) < 0) {
        if (terrno != TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
          pRsp->code = terrno;
          tDecoderClear(&decoder);
          goto _exit;
        }
      }

      msgIter.uid = createTbReq.uid;
      if (createTbReq.type == TSDB_CHILD_TABLE) {
        msgIter.suid = createTbReq.ctb.suid;
      } else {
        msgIter.suid = 0;
      }

      tDecoderClear(&decoder);
    }

    if (tsdbInsertTableData(pVnode->pTsdb, &msgIter, pBlock, &nRows) < 0) {
      pRsp->code = terrno;
      goto _exit;
    }

    rsp.numOfRows += nRows;
  }

_exit:
  // encode the response (TODO)
  pRsp->pCont = rpcMallocCont(sizeof(SSubmitRsp));
  memcpy(pRsp->pCont, &rsp, sizeof(rsp));
  pRsp->contLen = sizeof(SSubmitRsp);

  tsdbTriggerRSma(pVnode->pTsdb, pReq, STREAM_DATA_TYPE_SUBMIT_BLOCK);

  return 0;
}
