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
#include "taoserror.h"
#include "tqueue.h"
#include "trpc.h"
#include "tsdb.h"
#include "twal.h"
#include "tdataformat.h"
#include "vnode.h"
#include "vnodeInt.h"
#include "vnodeLog.h"
#include "query.h"

static int32_t (*vnodeProcessReadMsgFp[TSDB_MSG_TYPE_MAX])(SVnodeObj *pVnode, SReadMsg *pReadMsg);
static int32_t  vnodeProcessQueryMsg(SVnodeObj *pVnode, SReadMsg *pReadMsg);
static int32_t  vnodeProcessFetchMsg(SVnodeObj *pVnode, SReadMsg *pReadMsg);

void vnodeInitReadFp(void) {
  vnodeProcessReadMsgFp[TSDB_MSG_TYPE_QUERY] = vnodeProcessQueryMsg;
  vnodeProcessReadMsgFp[TSDB_MSG_TYPE_FETCH] = vnodeProcessFetchMsg;
}

int32_t vnodeProcessRead(void *param, SReadMsg *pReadMsg) {
  SVnodeObj *pVnode = (SVnodeObj *)param;
  int msgType = pReadMsg->rpcMsg.msgType;

  if (vnodeProcessReadMsgFp[msgType] == NULL) {
    vTrace("vgId:%d, msgType:%s not processed, no handle", pVnode->vgId, taosMsg[msgType]);
    return TSDB_CODE_VND_MSG_NOT_PROCESSED;
  }

  if (pVnode->status == TAOS_VN_STATUS_DELETING || pVnode->status == TAOS_VN_STATUS_CLOSING) {
    vTrace("vgId:%d, msgType:%s not processed, vnode status is %d", pVnode->vgId, taosMsg[msgType], pVnode->status);
    return TSDB_CODE_VND_INVALID_VGROUP_ID; 
  }

  // TODO: Later, let slave to support query
  if (pVnode->syncCfg.replica > 1 && pVnode->role != TAOS_SYNC_ROLE_MASTER) {
    vTrace("vgId:%d, msgType:%s not processed, replica:%d role:%d", pVnode->vgId, taosMsg[msgType], pVnode->syncCfg.replica, pVnode->role);
    return TSDB_CODE_RPC_NOT_READY;
  }

  return (*vnodeProcessReadMsgFp[msgType])(pVnode, pReadMsg);
}

// notify connection(handle) that current qhandle is created, if current connection from
// client is broken, the query needs to be killed immediately.
static int32_t vnodeNotifyCurrentQhandle(void* handle, void* qhandle, int32_t vgId) {
  SRetrieveTableMsg* killQueryMsg = rpcMallocCont(sizeof(SRetrieveTableMsg));
  killQueryMsg->qhandle = htobe64((uint64_t) qhandle);
  killQueryMsg->free = htons(1);
  killQueryMsg->header.vgId = htonl(vgId);
  killQueryMsg->header.contLen = htonl(sizeof(SRetrieveTableMsg));

  vTrace("QInfo:%p register qhandle to connect:%p", qhandle, handle);
  return rpcReportProgress(handle, (char*) killQueryMsg, sizeof(SRetrieveTableMsg));
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

    vWarn("QInfo:%p connection %p broken, kill query", (void*)killQueryMsg->qhandle, pReadMsg->rpcMsg.handle);
    assert(pReadMsg->rpcMsg.contLen > 0 && killQueryMsg->free == 1);

    // this message arrived here by means of the query message, so release the vnode is necessary
    qKillQuery((qinfo_t) killQueryMsg->qhandle, vnodeRelease, pVnode);
    vnodeRelease(pVnode);

    return TSDB_CODE_TSC_QUERY_CANCELLED; // todo change the error code
  }

  int32_t code = TSDB_CODE_SUCCESS;
  qinfo_t pQInfo = NULL;

  if (contLen != 0) {
    code = qCreateQueryInfo(pVnode->tsdb, pVnode->vgId, pQueryTableMsg, &pQInfo);

    SQueryTableRsp *pRsp = (SQueryTableRsp *) rpcMallocCont(sizeof(SQueryTableRsp));
    pRsp->qhandle = htobe64((uint64_t) (pQInfo));
    pRsp->code = code;

    pRet->len = sizeof(SQueryTableRsp);
    pRet->rsp = pRsp;

    // current connect is broken
    if (vnodeNotifyCurrentQhandle(pReadMsg->rpcMsg.handle, pQInfo, pVnode->vgId) != TSDB_CODE_SUCCESS) {
      vError("vgId:%d, QInfo:%p, dnode query discarded since link is broken, %p", pVnode->vgId, pQInfo, pReadMsg->rpcMsg.handle);
      pRsp->code = TSDB_CODE_RPC_NETWORK_UNAVAIL;

      //NOTE: there two refcount, needs to kill twice, todo refactor
      qKillQuery(pQInfo, vnodeRelease, pVnode);
      qKillQuery(pQInfo, vnodeRelease, pVnode);

      return pRsp->code;
    }

    vTrace("vgId:%d, QInfo:%p, dnode query msg disposed", pVnode->vgId, pQInfo);
  } else {
    assert(pCont != NULL);
    pQInfo = pCont;
    code = TSDB_CODE_VND_ACTION_IN_PROGRESS;
    vTrace("vgId:%d, QInfo:%p, dnode query msg in progress", pVnode->vgId, pQInfo);
  }

  if (pQInfo != NULL) {
    vTrace("vgId:%d, QInfo:%p, do qTableQuery", pVnode->vgId, pQInfo);
    qTableQuery(pQInfo, vnodeRelease, pVnode); // do execute query
  }

  return code;
}

static int32_t vnodeProcessFetchMsg(SVnodeObj *pVnode, SReadMsg *pReadMsg) {
  void *   pCont = pReadMsg->pCont;
  SRspRet *pRet = &pReadMsg->rspRet;

  SRetrieveTableMsg *pRetrieve = pCont;
  void *pQInfo = (void*) htobe64(pRetrieve->qhandle);
  pRetrieve->free = htons(pRetrieve->free);

  memset(pRet, 0, sizeof(SRspRet));

  if (pRetrieve->free == 1) {
    vTrace("vgId:%d, QInfo:%p, retrieve msg received to kill query and free qhandle", pVnode->vgId, pQInfo);
    int32_t ret = qKillQuery(pQInfo, vnodeRelease, pVnode);

    pRet->rsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    pRet->len = sizeof(SRetrieveTableRsp);

    memset(pRet->rsp, 0, sizeof(SRetrieveTableRsp));
    SRetrieveTableRsp* pRsp = pRet->rsp;
    pRsp->numOfRows = 0;
    pRsp->completed = true;
    pRsp->useconds  = 0;

    return ret;
  }

  vTrace("vgId:%d, QInfo:%p, retrieve msg is received", pVnode->vgId, pQInfo);

  int32_t code = qRetrieveQueryResultInfo(pQInfo);
  if (code != TSDB_CODE_SUCCESS) {
    //TODO
    pRet->rsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    memset(pRet->rsp, 0, sizeof(SRetrieveTableRsp));
  } else {
    // todo check code and handle error in build result set
    code = qDumpRetrieveResult(pQInfo, (SRetrieveTableRsp **)&pRet->rsp, &pRet->len);

    if (qHasMoreResultsToRetrieve(pQInfo)) {
      pRet->qhandle = pQInfo;
      code = TSDB_CODE_VND_ACTION_NEED_REPROCESSED;
    } else { // no further execution invoked, release the ref to vnode
      qDestroyQueryInfo(pQInfo, vnodeRelease, pVnode);
    }
  }
  
  vTrace("vgId:%d, QInfo:%p, retrieve msg is disposed", pVnode->vgId, pQInfo);
  return code;
}
