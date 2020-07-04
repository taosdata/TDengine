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
#include <dnode.h>
#include "os.h"

#include "tglobal.h"
#include "taoserror.h"
#include "taosmsg.h"
#include "tcache.h"
#include "query.h"
#include "trpc.h"
#include "tsdb.h"
#include "vnode.h"
#include "vnodeInt.h"

static int32_t (*vnodeProcessReadMsgFp[TSDB_MSG_TYPE_MAX])(SVnodeObj *pVnode, SReadMsg *pReadMsg);
static int32_t  vnodeProcessQueryMsg(SVnodeObj *pVnode, SReadMsg *pReadMsg);
static int32_t  vnodeProcessFetchMsg(SVnodeObj *pVnode, SReadMsg *pReadMsg);
static int32_t  vnodeNotifyCurrentQhandle(void* handle, void* qhandle, int32_t vgId);

void vnodeInitReadFp(void) {
  vnodeProcessReadMsgFp[TSDB_MSG_TYPE_QUERY] = vnodeProcessQueryMsg;
  vnodeProcessReadMsgFp[TSDB_MSG_TYPE_FETCH] = vnodeProcessFetchMsg;
}

int32_t vnodeProcessRead(void *param, SReadMsg *pReadMsg) {
  SVnodeObj *pVnode = (SVnodeObj *)param;
  int msgType = pReadMsg->rpcMsg.msgType;

  if (vnodeProcessReadMsgFp[msgType] == NULL) {
    vDebug("vgId:%d, msgType:%s not processed, no handle", pVnode->vgId, taosMsg[msgType]);
    return TSDB_CODE_VND_MSG_NOT_PROCESSED;
  }

  if (pVnode->status == TAOS_VN_STATUS_DELETING || pVnode->status == TAOS_VN_STATUS_CLOSING) {
    vDebug("vgId:%d, msgType:%s not processed, vnode status is %d", pVnode->vgId, taosMsg[msgType], pVnode->status);
    return TSDB_CODE_VND_INVALID_VGROUP_ID; 
  }

  // TODO: Later, let slave to support query
  if (pVnode->syncCfg.replica > 1 && pVnode->role != TAOS_SYNC_ROLE_MASTER) {
    vDebug("vgId:%d, msgType:%s not processed, replica:%d role:%d", pVnode->vgId, taosMsg[msgType], pVnode->syncCfg.replica, pVnode->role);
    return TSDB_CODE_RPC_NOT_READY;
  }

  return (*vnodeProcessReadMsgFp[msgType])(pVnode, pReadMsg);
}

static int32_t vnodeProcessQueryMsg(SVnodeObj *pVnode, SReadMsg *pReadMsg) {
  void *   pCont = pReadMsg->pCont;
  int32_t  contLen = pReadMsg->contLen;
  SRspRet *pRet = &pReadMsg->rspRet;

  SQueryTableMsg* pQueryTableMsg = (SQueryTableMsg*) pCont;
  memset(pRet, 0, sizeof(SRspRet));

  // qHandle needs to be freed correctly
  if (pReadMsg->rpcMsg.code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
    SRetrieveTableMsg* killQueryMsg = (SRetrieveTableMsg*) pReadMsg->pCont;
    killQueryMsg->free = htons(killQueryMsg->free);
    killQueryMsg->qhandle = htobe64(killQueryMsg->qhandle);

    void* handle = NULL;
    if ((void**) killQueryMsg->qhandle != NULL) {
      handle = *(void**) killQueryMsg->qhandle;
    }

    vWarn("QInfo:%p connection %p broken, kill query", handle, pReadMsg->rpcMsg.handle);
    assert(pReadMsg->rpcMsg.contLen > 0 && killQueryMsg->free == 1);

    void** qhandle = qAcquireQInfo(pVnode->qMgmt, (void**) killQueryMsg->qhandle);
    if (qhandle == NULL || *qhandle == NULL) {
      vWarn("QInfo:%p invalid qhandle, no matched query handle, conn:%p", (void*) killQueryMsg->qhandle, pReadMsg->rpcMsg.handle);
    } else {
      assert(qhandle == (void**) killQueryMsg->qhandle);
      qReleaseQInfo(pVnode->qMgmt, (void**) &qhandle, true);
    }

    return TSDB_CODE_TSC_QUERY_CANCELLED;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  qinfo_t pQInfo = NULL;
  void**  handle = NULL;

  if (contLen != 0) {
    code = qCreateQueryInfo(pVnode->tsdb, pVnode->vgId, pQueryTableMsg, pVnode, NULL, &pQInfo);

    SQueryTableRsp *pRsp = (SQueryTableRsp *) rpcMallocCont(sizeof(SQueryTableRsp));
    pRsp->code    = code;
    pRsp->qhandle = 0;

    pRet->len = sizeof(SQueryTableRsp);
    pRet->rsp = pRsp;

    // current connect is broken
    if (code == TSDB_CODE_SUCCESS) {
      // add lock here
      handle = qRegisterQInfo(pVnode->qMgmt, pQInfo);
      if (handle == NULL) {  // failed to register qhandle
        pRsp->code = TSDB_CODE_QRY_INVALID_QHANDLE;

        qKillQuery(pQInfo);
        qKillQuery(pQInfo);
      } else {
        assert(*handle == pQInfo);
        pRsp->qhandle = htobe64((uint64_t) (handle));
      }

      if (handle != NULL && vnodeNotifyCurrentQhandle(pReadMsg->rpcMsg.handle, handle, pVnode->vgId) != TSDB_CODE_SUCCESS) {
        vError("vgId:%d, QInfo:%p, query discarded since link is broken, %p", pVnode->vgId, pQInfo, pReadMsg->rpcMsg.handle);
        pRsp->code = TSDB_CODE_RPC_NETWORK_UNAVAIL;

        // NOTE: there two refcount, needs to kill twice
        // query has not been put into qhandle pool, kill it directly.
        qKillQuery(pQInfo);
        qReleaseQInfo(pVnode->qMgmt, (void**) &handle, true);
        return pRsp->code;
      }
    } else {
      assert(pQInfo == NULL);
    }

    vDebug("vgId:%d, QInfo:%p, dnode query msg disposed", pVnode->vgId, pQInfo);
  } else {
    assert(pCont != NULL);
    pQInfo = *(void**)(pCont);
    handle = pCont;
    code = TSDB_CODE_VND_ACTION_IN_PROGRESS;

    vDebug("vgId:%d, QInfo:%p, dnode query msg in progress", pVnode->vgId, pQInfo);
  }

  if (pQInfo != NULL) {
    qTableQuery(pQInfo); // do execute query
    assert(handle != NULL);
    qReleaseQInfo(pVnode->qMgmt, (void**) &handle, false);
  }

  return code;
}

static int32_t vnodeProcessFetchMsg(SVnodeObj *pVnode, SReadMsg *pReadMsg) {
  void *   pCont = pReadMsg->pCont;
  SRspRet *pRet = &pReadMsg->rspRet;

  SRetrieveTableMsg *pRetrieve = pCont;
  void **pQInfo = (void*) htobe64(pRetrieve->qhandle);
  pRetrieve->free = htons(pRetrieve->free);

  vDebug("vgId:%d, QInfo:%p, retrieve msg is disposed", pVnode->vgId, *pQInfo);

  memset(pRet, 0, sizeof(SRspRet));
  int32_t ret = 0;

  void** handle = qAcquireQInfo(pVnode->qMgmt, pQInfo);
  if (handle == NULL || handle != pQInfo) {
    ret = TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  if (pRetrieve->free == 1) {
    if (ret == TSDB_CODE_SUCCESS) {
      vDebug("vgId:%d, QInfo:%p, retrieve msg received to kill query and free qhandle", pVnode->vgId, pQInfo);
      qReleaseQInfo(pVnode->qMgmt, (void**) &handle, true);

      pRet->rsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
      pRet->len = sizeof(SRetrieveTableRsp);

      memset(pRet->rsp, 0, sizeof(SRetrieveTableRsp));
      SRetrieveTableRsp* pRsp = pRet->rsp;
      pRsp->numOfRows = 0;
      pRsp->completed = true;
      pRsp->useconds  = 0;
    } else { // todo handle error
      qReleaseQInfo(pVnode->qMgmt, (void**) &handle, true);
    }
    return ret;
  }

  int32_t code = qRetrieveQueryResultInfo(*pQInfo);
  if (code != TSDB_CODE_SUCCESS || ret != TSDB_CODE_SUCCESS) {
    //TODO
    pRet->rsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    memset(pRet->rsp, 0, sizeof(SRetrieveTableRsp));

  } else {
    // todo check code and handle error in build result set
    code = qDumpRetrieveResult(*pQInfo, (SRetrieveTableRsp **)&pRet->rsp, &pRet->len);

    if (qHasMoreResultsToRetrieve(*handle)) {
      dnodePutQhandleIntoReadQueue(pVnode, handle);
      pRet->qhandle = handle;
      code = TSDB_CODE_SUCCESS;
    } else { // no further execution invoked, release the ref to vnode
      qReleaseQInfo(pVnode->qMgmt, (void**) &handle, true);
    }
  }

  return code;
}

// notify connection(handle) that current qhandle is created, if current connection from
// client is broken, the query needs to be killed immediately.
int32_t vnodeNotifyCurrentQhandle(void* handle, void* qhandle, int32_t vgId) {
  SRetrieveTableMsg* killQueryMsg = rpcMallocCont(sizeof(SRetrieveTableMsg));
  killQueryMsg->qhandle = htobe64((uint64_t) qhandle);
  killQueryMsg->free = htons(1);
  killQueryMsg->header.vgId = htonl(vgId);
  killQueryMsg->header.contLen = htonl(sizeof(SRetrieveTableMsg));

  vDebug("QInfo:%p register qhandle to connect:%p", qhandle, handle);
  return rpcReportProgress(handle, (char*) killQueryMsg, sizeof(SRetrieveTableMsg));
}