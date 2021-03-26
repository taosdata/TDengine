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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taosmsg.h"
#include "tqueue.h"
#include "tglobal.h"
#include "query.h"
#include "vnodeStatus.h"

static int32_t (*vnodeProcessReadMsgFp[TSDB_MSG_TYPE_MAX])(SVnodeObj *pVnode, SVReadMsg *pRead);
static int32_t  vnodeProcessQueryMsg(SVnodeObj *pVnode, SVReadMsg *pRead);
static int32_t  vnodeProcessFetchMsg(SVnodeObj *pVnode, SVReadMsg *pRead);

static int32_t  vnodeNotifyCurrentQhandle(void* handle, uint64_t qId, void* qhandle, int32_t vgId);

int32_t vnodeInitRead(void) {
  vnodeProcessReadMsgFp[TSDB_MSG_TYPE_QUERY] = vnodeProcessQueryMsg;
  vnodeProcessReadMsgFp[TSDB_MSG_TYPE_FETCH] = vnodeProcessFetchMsg;
  return 0;
}

void vnodeCleanupRead() {}

//
// After the fetch request enters the vnode queue, if the vnode cannot provide services, the process function are
// still required, or there will be a deadlock, so we don’t do any check here, but put the check codes before the
// request enters the queue
//
int32_t vnodeProcessRead(void *vparam, SVReadMsg *pRead) {
  SVnodeObj *pVnode = vparam;
  int32_t    msgType = pRead->msgType;

  if (vnodeProcessReadMsgFp[msgType] == NULL) {
    vDebug("vgId:%d, msgType:%s not processed, no handle", pVnode->vgId, taosMsg[msgType]);
    return TSDB_CODE_VND_MSG_NOT_PROCESSED;
  }

  return (*vnodeProcessReadMsgFp[msgType])(pVnode, pRead);
}

static int32_t vnodeCheckRead(SVnodeObj *pVnode) {
  if (!vnodeInReadyStatus(pVnode)) {
    vDebug("vgId:%d, vnode status is %s, refCount:%d pVnode:%p", pVnode->vgId, vnodeStatus[pVnode->status],
           pVnode->refCount, pVnode);
    return TSDB_CODE_APP_NOT_READY;
  }

  // tsdb may be in reset state
  if (pVnode->tsdb == NULL) {
    vDebug("vgId:%d, tsdb is null, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);
    return TSDB_CODE_APP_NOT_READY;
  }

  if (pVnode->role == TAOS_SYNC_ROLE_MASTER) {
    return TSDB_CODE_SUCCESS;
  }

  if (tsEnableSlaveQuery && pVnode->role == TAOS_SYNC_ROLE_SLAVE) {
    return TSDB_CODE_SUCCESS;
  }

  vDebug("vgId:%d, replica:%d role:%s, refCount:%d pVnode:%p, cant provide query service", pVnode->vgId, pVnode->syncCfg.replica,
         syncRole[pVnode->role], pVnode->refCount, pVnode);
  return TSDB_CODE_APP_NOT_READY;
}

void vnodeFreeFromRQueue(void *vparam, SVReadMsg *pRead) {
  SVnodeObj *pVnode = vparam;

  atomic_sub_fetch_32(&pVnode->queuedRMsg, 1);
  vTrace("vgId:%d, free from vrqueue, refCount:%d queued:%d", pVnode->vgId, pVnode->refCount, pVnode->queuedRMsg);

  taosFreeQitem(pRead);
  vnodeRelease(pVnode);
}

static SVReadMsg *vnodeBuildVReadMsg(SVnodeObj *pVnode, void *pCont, int32_t contLen, int8_t qtype, SRpcMsg *pRpcMsg) {
  int32_t size = sizeof(SVReadMsg) + contLen;
  SVReadMsg *pRead = taosAllocateQitem(size);
  if (pRead == NULL) {
    terrno = TSDB_CODE_VND_OUT_OF_MEMORY;
    return NULL;
  }

  if (pRpcMsg != NULL) {
    pRead->rpcHandle = pRpcMsg->handle;
    pRead->rpcAhandle = pRpcMsg->ahandle;
    pRead->msgType = pRpcMsg->msgType;
    pRead->code = pRpcMsg->code;
  }

  if (contLen != 0) {
    pRead->contLen = contLen;
    memcpy(pRead->pCont, pCont, contLen);
  } else {
    pRead->qhandle = pCont;
  }

  pRead->qtype = qtype;
  atomic_add_fetch_32(&pVnode->refCount, 1);

  return pRead;
}

int32_t vnodeWriteToRQueue(void *vparam, void *pCont, int32_t contLen, int8_t qtype, void *rparam) {
  SVReadMsg *pRead = vnodeBuildVReadMsg(vparam, pCont, contLen, qtype, rparam);
  if (pRead == NULL) {
    assert(terrno != 0);
    return terrno;
  }

  SVnodeObj *pVnode = vparam;

  int32_t code = vnodeCheckRead(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    taosFreeQitem(pRead);
    vnodeRelease(pVnode);
    return code;
  }

  atomic_add_fetch_32(&pVnode->queuedRMsg, 1);

  if (pRead->code == TSDB_CODE_RPC_NETWORK_UNAVAIL || pRead->msgType == TSDB_MSG_TYPE_FETCH) {
    vTrace("vgId:%d, write into vfetch queue, refCount:%d queued:%d", pVnode->vgId, pVnode->refCount,
           pVnode->queuedRMsg);
    return taosWriteQitem(pVnode->fqueue, qtype, pRead);
  } else {
    vTrace("vgId:%d, write into vquery queue, refCount:%d queued:%d", pVnode->vgId, pVnode->refCount,
           pVnode->queuedRMsg);
    return taosWriteQitem(pVnode->qqueue, qtype, pRead);
  }
}

static int32_t vnodePutItemIntoReadQueue(SVnodeObj *pVnode, void **qhandle, void *ahandle) {
  SRpcMsg rpcMsg = {0};
  rpcMsg.msgType = TSDB_MSG_TYPE_QUERY;
  rpcMsg.ahandle = ahandle;

  int32_t code = vnodeWriteToRQueue(pVnode, qhandle, 0, TAOS_QTYPE_QUERY, &rpcMsg);
  if (code == TSDB_CODE_SUCCESS) {
    vTrace("QInfo:%p add to vread queue for exec query", *qhandle);
  }

  return code;
}

/**
 *
 * @param pRet         response message object
 * @param pVnode       the vnode object
 * @param handle       qhandle for executing query
 * @param freeHandle   free qhandle or not
 * @param ahandle      sqlObj address at client side
 * @return
 */
static int32_t vnodeDumpQueryResult(SRspRet *pRet, void *pVnode, uint64_t qId, void **handle, bool *freeHandle, void *ahandle) {
  bool continueExec = false;

  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = qDumpRetrieveResult(*handle, (SRetrieveTableRsp **)&pRet->rsp, &pRet->len, &continueExec)) == TSDB_CODE_SUCCESS) {
    if (continueExec) {
      *freeHandle = false;
      code = vnodePutItemIntoReadQueue(pVnode, handle, ahandle);
      if (code != TSDB_CODE_SUCCESS) {
        *freeHandle = true;
        return code;
      } else {
        pRet->qhandle = *handle;
      }
    } else {
      *freeHandle = true;
      vTrace("QInfo:%"PRIu64"-%p exec completed, free handle:%d", qId, *handle, *freeHandle);
    }
  } else {
    SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    memset(pRsp, 0, sizeof(SRetrieveTableRsp));
    pRsp->completed = true;

    pRet->rsp = pRsp;
    pRet->len = sizeof(SRetrieveTableRsp);
    *freeHandle = true;
  }

  return code;
}

static void vnodeBuildNoResultQueryRsp(SRspRet *pRet) {
  pRet->rsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
  pRet->len = sizeof(SRetrieveTableRsp);

  memset(pRet->rsp, 0, sizeof(SRetrieveTableRsp));
  SRetrieveTableRsp *pRsp = pRet->rsp;

  pRsp->completed = true;
}

static int32_t vnodeProcessQueryMsg(SVnodeObj *pVnode, SVReadMsg *pRead) {
  void *   pCont = pRead->pCont;
  int32_t  contLen = pRead->contLen;
  SRspRet *pRet = &pRead->rspRet;

  SQueryTableMsg *pQueryTableMsg = (SQueryTableMsg *)pCont;
  memset(pRet, 0, sizeof(SRspRet));

  // qHandle needs to be freed correctly
  if (pRead->code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
    vError("error rpc msg in query, %s", tstrerror(pRead->code));
  }

  int32_t code = TSDB_CODE_SUCCESS;
  void ** handle = NULL;

  if (contLen != 0) {
    qinfo_t pQInfo = NULL;
    uint64_t qId = 0;
    code = qCreateQueryInfo(pVnode->tsdb, pVnode->vgId, pQueryTableMsg, &pQInfo, &qId);

    SQueryTableRsp *pRsp = (SQueryTableRsp *)rpcMallocCont(sizeof(SQueryTableRsp));
    pRsp->code = code;
    pRsp->qhandle = 0;

    pRet->len = sizeof(SQueryTableRsp);
    pRet->rsp = pRsp;
    int32_t vgId = pVnode->vgId;

    // current connect is broken
    if (code == TSDB_CODE_SUCCESS) {
      handle = qRegisterQInfo(pVnode->qMgmt, qId, (uint64_t)pQInfo);
      if (handle == NULL) {  // failed to register qhandle
        pRsp->code = terrno;
        terrno = 0;
        vError("vgId:%d, QInfo:%"PRIu64 "-%p register qhandle failed, return to app, code:%s", pVnode->vgId, qId, (void *)pQInfo,
               tstrerror(pRsp->code));
        qDestroyQueryInfo(pQInfo);  // destroy it directly
        return pRsp->code;
      } else {
        assert(*handle == pQInfo);
        pRsp->qhandle = htobe64(qId);
      }

      if (handle != NULL &&
          vnodeNotifyCurrentQhandle(pRead->rpcHandle, qId, *handle, pVnode->vgId) != TSDB_CODE_SUCCESS) {
        vError("vgId:%d, QInfo:%"PRIu64 "-%p, query discarded since link is broken, %p", pVnode->vgId, qId, *handle,
               pRead->rpcHandle);
        pRsp->code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
        qReleaseQInfo(pVnode->qMgmt, (void **)&handle, true);
        return pRsp->code;
      }
    } else {
      assert(pQInfo == NULL);
    }

    if (handle != NULL) {
      vTrace("vgId:%d, QInfo:%"PRIu64 "-%p, dnode query msg disposed, create qhandle and returns to app", vgId, qId, *handle);
      code = vnodePutItemIntoReadQueue(pVnode, handle, pRead->rpcHandle);
      if (code != TSDB_CODE_SUCCESS) {
        pRsp->code = code;
        qReleaseQInfo(pVnode->qMgmt, (void **)&handle, true);
        return pRsp->code;
      }
    }
  } else {
    assert(pCont != NULL);
    void **qhandle = (void **)pRead->qhandle;
    uint64_t qId = 0;

    vTrace("vgId:%d, QInfo:%p, dnode continues to exec query", pVnode->vgId, *qhandle);

    // In the retrieve blocking model, only 50% CPU will be used in query processing
    if (tsRetrieveBlockingModel) {
      qTableQuery(*qhandle, &qId);  // do execute query
      qReleaseQInfo(pVnode->qMgmt, (void **)&qhandle, false);
    } else {
      bool freehandle = false;
      bool buildRes = qTableQuery(*qhandle, &qId);  // do execute query

      // build query rsp, the retrieve request has reached here already
      if (buildRes) {
        // update the connection info according to the retrieve connection
        pRead->rpcHandle = qGetResultRetrieveMsg(*qhandle);
        assert(pRead->rpcHandle != NULL);

        vTrace("vgId:%d, QInfo:%p, start to build retrieval rsp after query paused, %p", pVnode->vgId, *qhandle,
               pRead->rpcHandle);

        // set the real rsp error code
        pRead->code = vnodeDumpQueryResult(&pRead->rspRet, pVnode, qId, qhandle, &freehandle, pRead->rpcHandle);

        // NOTE: set return code to be TSDB_CODE_QRY_HAS_RSP to notify dnode to return msg to client
        code = TSDB_CODE_QRY_HAS_RSP;
      } else {
        //void *h1 = qGetResultRetrieveMsg(*qhandle);

        /* remove this assert, one possible case that will cause h1 not NULL: query thread unlock pQInfo->lock, and then FETCH thread execute twice before query thread reach here */
        //assert(h1 == NULL);

        freehandle = qQueryCompleted(*qhandle);
      }

      // NOTE: if the qhandle is not put into vread queue or query is completed, free the qhandle.
      // If the building of result is not required, simply free it. Otherwise, mandatorily free the qhandle
      if (freehandle || (!buildRes)) {
        qReleaseQInfo(pVnode->qMgmt, (void **)&qhandle, freehandle);
      }
    }
  }

  return code;
}

static int32_t vnodeProcessFetchMsg(SVnodeObj *pVnode, SVReadMsg *pRead) {
  void *   pCont = pRead->pCont;
  SRspRet *pRet = &pRead->rspRet;

  SRetrieveTableMsg *pRetrieve = pCont;
  pRetrieve->free = htons(pRetrieve->free);
  pRetrieve->qId = htobe64(pRetrieve->qId);

  vTrace("vgId:%d, qId:%" PRIu64 ", retrieve msg is disposed, free:%d, conn:%p", pVnode->vgId, pRetrieve->qId,
         pRetrieve->free, pRead->rpcHandle);

  memset(pRet, 0, sizeof(SRspRet));

  terrno = TSDB_CODE_SUCCESS;
  int32_t code = TSDB_CODE_SUCCESS;
  void ** handle = qAcquireQInfo(pVnode->qMgmt, pRetrieve->qId);
  if (handle == NULL) {
    code = terrno;
    terrno = TSDB_CODE_SUCCESS;
  } else if (!checkQIdEqual(*handle, pRetrieve->qId)) {
    code = TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, invalid qId in retrieving result, code:%s, QInfo:%" PRIu64, pVnode->vgId, tstrerror(code), pRetrieve->qId);
    vnodeBuildNoResultQueryRsp(pRet);
    return code;
  }

  // kill current query and free corresponding resources.
  if (pRetrieve->free == 1) {
    vWarn("vgId:%d, QInfo:%"PRIu64 "-%p, retrieve msg received to kill query and free qhandle", pVnode->vgId, pRetrieve->qId, *handle);
    qKillQuery(*handle);
    qReleaseQInfo(pVnode->qMgmt, (void **)&handle, true);

    vnodeBuildNoResultQueryRsp(pRet);
    code = TSDB_CODE_TSC_QUERY_CANCELLED;
    return code;
  }

  // register the qhandle to connect to quit query immediate if connection is broken
  if (vnodeNotifyCurrentQhandle(pRead->rpcHandle, pRetrieve->qId, *handle, pVnode->vgId) != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, QInfo:%"PRIu64 "-%p, retrieve discarded since link is broken, %p", pVnode->vgId, pRetrieve->qhandle, *handle, pRead->rpcHandle);
    code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
    qKillQuery(*handle);
    qReleaseQInfo(pVnode->qMgmt, (void **)&handle, true);
    return code;
  }

  bool freeHandle = true;
  bool buildRes = false;

  code = qRetrieveQueryResultInfo(*handle, &buildRes, pRead->rpcHandle);
  if (code != TSDB_CODE_SUCCESS) {
    // TODO handle malloc failure
    pRet->rsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    pRet->len = sizeof(SRetrieveTableRsp);
    memset(pRet->rsp, 0, sizeof(SRetrieveTableRsp));
    freeHandle = true;
  } else {  // result is not ready, return immediately
    // Only effects in the non-blocking model
    if (!tsRetrieveBlockingModel) {
      if (!buildRes) {
        assert(pRead->rpcHandle != NULL);

        qReleaseQInfo(pVnode->qMgmt, (void **)&handle, false);
        return TSDB_CODE_QRY_NOT_READY;
      }
    }

    // ahandle is the sqlObj pointer
    code = vnodeDumpQueryResult(pRet, pVnode, pRetrieve->qId, handle, &freeHandle, pRead->rpcHandle);
  }

  // If qhandle is not added into vread queue, the query should be completed already or paused with error.
  // Here free qhandle immediately
  if (freeHandle) {
    qReleaseQInfo(pVnode->qMgmt, (void **)&handle, true);
  }

  return code;
}

// notify connection(handle) that current qhandle is created, if current connection from
// client is broken, the query needs to be killed immediately.
int32_t vnodeNotifyCurrentQhandle(void *handle, uint64_t qId, void *qhandle, int32_t vgId) {
  SRetrieveTableMsg *pMsg = rpcMallocCont(sizeof(SRetrieveTableMsg));
  pMsg->qhandle = htobe64((uint64_t)qhandle);
  pMsg->header.vgId = htonl(vgId);
  pMsg->header.contLen = htonl(sizeof(SRetrieveTableMsg));

  vTrace("QInfo:%"PRIu64"-%p register qhandle to connect:%p", qId, qhandle, handle);
  return rpcReportProgress(handle, (char *)pMsg, sizeof(SRetrieveTableMsg));
}

void vnodeWaitReadCompleted(SVnodeObj *pVnode) {
  while (pVnode->queuedRMsg > 0) {
    vTrace("vgId:%d, queued rmsg num:%d", pVnode->vgId, pVnode->queuedRMsg);
    taosMsleep(10);
  }
}
