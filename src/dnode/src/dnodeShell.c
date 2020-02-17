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
#include "taoserror.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tlog.h"
#include "tsocket.h"
#include "tschemautil.h"
#include "textbuffer.h"
#include "trpc.h"
#include "http.h"
#include "dnode.h"
#include "dnodeMgmt.h"
#include "dnodeRead.h"
#include "dnodeSystem.h"
#include "dnodeShell.h"
#include "dnodeUtil.h"
#include "dnodeWrite.h"

static void dnodeProcessRetrieveRequest(int8_t *pMsg, int32_t msgLen, SShellObj *pObj);
static void dnodeProcessQueryRequest(int8_t *pMsg, int32_t msgLen, SShellObj *pObj);
static void dnodeProcessShellSubmitRequest(int8_t *pMsg, int32_t msgLen, SShellObj *pObj);

static void      *tsDnodeShellServer = NULL;
static SShellObj *tsDnodeShellList  = NULL;
static int32_t   tsDnodeSelectReqNum = 0;
static int32_t   tsDnodeInsertReqNum = 0;
static int32_t   tsDnodeShellConns   = 0;

#define NUM_OF_SESSIONS_PER_VNODE (300)
#define NUM_OF_SESSIONS_PER_DNODE (NUM_OF_SESSIONS_PER_VNODE * TSDB_MAX_VNODES)

void *dnodeProcessMsgFromShell(char *msg, void *ahandle, void *thandle) {
  int        sid, vnode;
  SShellObj *pObj = (SShellObj *)ahandle;
  SIntMsg *  pMsg = (SIntMsg *)msg;
  uint32_t   peerId, peerIp;
  uint16_t   peerPort;
  char       ipstr[20];

  if (msg == NULL) {
    if (pObj) {
      pObj->thandle = NULL;
      dTrace("QInfo:%p %s free qhandle", pObj->qhandle, __FUNCTION__);
      dnodeFreeQInfoInQueue(pObj);
      pObj->qhandle = NULL;
      tsDnodeShellConns--;
      dTrace("shell connection:%d is gone, shellConns:%d", pObj->sid, tsDnodeShellConns);
    }
    return NULL;
  }

  taosGetRpcConnInfo(thandle, &peerId, &peerIp, &peerPort, &vnode, &sid);

  if (pObj == NULL) {
    pObj = tsDnodeShellList + sid;
    pObj->thandle = thandle;
    pObj->sid = sid;
    pObj->ip = peerIp;
    tinet_ntoa(ipstr, peerIp);
    tsDnodeShellConns--;
    dTrace("shell connection:%d from ip:%s is created, shellConns:%d", sid, ipstr, tsDnodeShellConns);
  } else {
    if (pObj != tsDnodeShellList + sid) {
      dError("shell connection:%d, pObj:%p is not matched with:%p", sid, pObj, tsDnodeShellList + sid);
      return NULL;
    }
  }

  dTrace("vid:%d sid:%d, msg:%s is received pConn:%p", vnode, sid, taosMsg[pMsg->msgType], thandle);

  if (dnodeGetRunStatus() != TSDB_DNODE_RUN_STATUS_RUNING) {
    taosSendSimpleRsp(thandle, pMsg->msgType + 1, TSDB_CODE_NOT_READY);
    dTrace("sid:%d, shell query msg is ignored since dnode not running", sid);
    return pObj;
  }

  if (pMsg->msgType == TSDB_MSG_TYPE_QUERY) {
    dnodeProcessQueryRequest(pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pObj);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_RETRIEVE) {
    dnodeProcessRetrieveRequest(pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pObj);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_SUBMIT) {
    dnodeProcessShellSubmitRequest(pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pObj);
  } else {
    dError("%s is not processed", taosMsg[pMsg->msgType]);
  }

  return pObj;
}

int32_t dnodeInitShell() {
  SRpcInit rpcInit;

  int numOfThreads = tsNumOfCores * tsNumOfThreadsPerCore;
  numOfThreads = (1.0 - tsRatioOfQueryThreads) * numOfThreads / 2.0;
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  memset(&rpcInit, 0, sizeof(rpcInit));

  rpcInit.localIp = tsAnyIp ? "0.0.0.0" : tsPrivateIp;

  rpcInit.localPort = tsVnodeShellPort;
  rpcInit.label = "DND-shell";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.fp = dnodeProcessMsgFromShell;
  rpcInit.bits = TSDB_SHELL_VNODE_BITS;
  rpcInit.numOfChanns = TSDB_MAX_VNODES;
  rpcInit.sessionsPerChann = 16;
  rpcInit.idMgmt = TAOS_ID_FREE;
  rpcInit.connType = TAOS_CONN_SOCKET_TYPE_S();
  rpcInit.idleTime = tsShellActivityTimer * 2000;
  rpcInit.qhandle = tsRpcQhandle[0];
  //rpcInit.efp = vnodeSendVpeerCfgMsg;

  tsDnodeShellServer = taosOpenRpc(&rpcInit);
  if (tsDnodeShellServer == NULL) {
    dError("failed to init connection to shell");
    return -1;
  }


  const int32_t size = NUM_OF_SESSIONS_PER_DNODE * sizeof(SShellObj);
  tsDnodeShellList = (SShellObj *)malloc(size);
  if (tsDnodeShellList == NULL) {
    dError("failed to allocate shellObj, sessions:%d", NUM_OF_SESSIONS_PER_DNODE);
    return -1;
  }
  memset(tsDnodeShellList, 0, size);

  // TODO re initialize tsRpcQhandle
  if(taosOpenRpcChannWithQ(tsDnodeShellServer, 0, NUM_OF_SESSIONS_PER_DNODE, tsRpcQhandle) != TSDB_CODE_SUCCESS) {
    dError("sessions:%d, failed to open shell", NUM_OF_SESSIONS_PER_DNODE);
    return -1;
  }

  dError("sessions:%d, shell is opened", NUM_OF_SESSIONS_PER_DNODE);
  return TSDB_CODE_SUCCESS;
}

void dnodeCleanupShell() {
  if (tsDnodeShellServer) {
    taosCloseRpc(tsDnodeShellServer);
  }

  for (int i = 0; i < NUM_OF_SESSIONS_PER_DNODE; ++i) {
    dnodeFreeQInfoInQueue(tsDnodeShellList+i);
  }

  //tfree(tsDnodeShellList);
}

int vnodeSendQueryRspMsg(SShellObj *pObj, int code, void *qhandle) {
  char *pMsg, *pStart;
  int   msgLen;

  pStart = taosBuildRspMsgWithSize(pObj->thandle, TSDB_MSG_TYPE_QUERY_RSP, 128);
  if (pStart == NULL) return -1;
  pMsg = pStart;

  *pMsg = code;
  pMsg++;

  *((uint64_t *)pMsg) = (uint64_t)qhandle;
  pMsg += 8;

  msgLen = pMsg - pStart;
  taosSendMsgToPeer(pObj->thandle, pStart, msgLen);

  return msgLen;
}

int32_t dnodeSendShellSubmitRspMsg(SShellObj *pObj, int32_t code, int32_t numOfPoints) {
  char *pMsg, *pStart;
  int   msgLen;

  dTrace("code:%d numOfTotalPoints:%d", code, numOfPoints);
  pStart = taosBuildRspMsgWithSize(pObj->thandle, TSDB_MSG_TYPE_SUBMIT_RSP, 128);
  if (pStart == NULL) return -1;
  pMsg = pStart;

  *pMsg = code;
  pMsg++;

  *(int32_t *)pMsg = numOfPoints;
  pMsg += sizeof(numOfPoints);

  msgLen = pMsg - pStart;
  taosSendMsgToPeer(pObj->thandle, pStart, msgLen);

  return msgLen;
}

int vnodeProcessQueryRequest(char *pMsg, int msgLen, SShellObj *pObj) {
  int                ret, code = 0;
  SQueryMeterMsg *   pQueryMsg;
  SMeterSidExtInfo **pSids = NULL;
  int32_t            incNumber = 0;
  SSqlFunctionExpr * pExprs = NULL;
  SSqlGroupbyExpr *  pGroupbyExpr = NULL;
  SMeterObj **       pMeterObjList = NULL;

  pQueryMsg = (SQueryMeterMsg *)pMsg;
  if ((code = vnodeConvertQueryMeterMsg(pQueryMsg)) != TSDB_CODE_SUCCESS) {
    goto _query_over;
  }

  if (pQueryMsg->numOfSids <= 0) {
    dError("Invalid number of meters to query, numOfSids:%d", pQueryMsg->numOfSids);
    code = TSDB_CODE_INVALID_QUERY_MSG;
    goto _query_over;
  }

  if (pQueryMsg->vnode >= TSDB_MAX_VNODES || pQueryMsg->vnode < 0) {
    dError("qmsg:%p,vid:%d is out of range", pQueryMsg, pQueryMsg->vnode);
    code = TSDB_CODE_INVALID_TABLE_ID;
    goto _query_over;
  }

  SVnodeObj *pVnode = &vnodeList[pQueryMsg->vnode];

  if (pVnode->cfg.maxSessions == 0) {
    dError("qmsg:%p,vid:%d is not activated yet", pQueryMsg, pQueryMsg->vnode);
    vnodeSendVpeerCfgMsg(pQueryMsg->vnode);
    code = TSDB_CODE_NOT_ACTIVE_VNODE;
    goto _query_over;
  }

  if (!(pVnode->accessState & TSDB_VN_READ_ACCCESS)) {
    dError("qmsg:%p,vid:%d access not allowed", pQueryMsg, pQueryMsg->vnode);
    code = TSDB_CODE_NO_READ_ACCESS;
    goto _query_over;
  }
  
  if (pVnode->meterList == NULL) {
    dError("qmsg:%p,vid:%d has been closed", pQueryMsg, pQueryMsg->vnode);
    code = TSDB_CODE_NOT_ACTIVE_VNODE;
    goto _query_over;
  }

  if (pQueryMsg->pSidExtInfo == 0) {
    dError("qmsg:%p,SQueryMeterMsg wrong format", pQueryMsg);
    code = TSDB_CODE_INVALID_QUERY_MSG;
    goto _query_over;
  }

  pSids = (SMeterSidExtInfo **)pQueryMsg->pSidExtInfo;
  for (int32_t i = 0; i < pQueryMsg->numOfSids; ++i) {
    if (pSids[i]->sid >= pVnode->cfg.maxSessions || pSids[i]->sid < 0) {
      dError("qmsg:%p sid:%d out of range, valid range:[%d,%d]", pQueryMsg, pSids[i]->sid, 0, pVnode->cfg.maxSessions);
      code = TSDB_CODE_INVALID_TABLE_ID;
      goto _query_over;
    }
  }

  // todo optimize for single table query process
  pMeterObjList = (SMeterObj **)calloc(pQueryMsg->numOfSids, sizeof(SMeterObj *));
  if (pMeterObjList == NULL) {
    code = TSDB_CODE_SERV_OUT_OF_MEMORY;
    goto _query_over;
  }

  //add query ref for all meters. if any meter failed to add ref, rollback whole operation and go to error
  pthread_mutex_lock(&pVnode->vmutex);
  code = vnodeIncQueryRefCount(pQueryMsg, pSids, pMeterObjList, &incNumber);
  assert(incNumber <= pQueryMsg->numOfSids);
  pthread_mutex_unlock(&pVnode->vmutex);

  if (code != TSDB_CODE_SUCCESS || pQueryMsg->numOfSids == 0) { // all the meters may have been dropped.
    goto _query_over;
  }

  pExprs = vnodeCreateSqlFunctionExpr(pQueryMsg, &code);
  if (pExprs == NULL) {
    assert(code != TSDB_CODE_SUCCESS);
    goto _query_over;
  }

  pGroupbyExpr = vnodeCreateGroupbyExpr(pQueryMsg, &code);
  if ((pGroupbyExpr == NULL && pQueryMsg->numOfGroupCols != 0) || code != TSDB_CODE_SUCCESS) {
    goto _query_over;
  }

  if (pObj->qhandle) {
    dTrace("QInfo:%p %s free qhandle", pObj->qhandle, __FUNCTION__);
    void* qHandle = pObj->qhandle;
    pObj->qhandle = NULL;
    
    vnodeDecRefCount(qHandle);
  }

  if (QUERY_IS_STABLE_QUERY(pQueryMsg->queryType)) {
    pObj->qhandle = vnodeQueryOnMultiMeters(pMeterObjList, pGroupbyExpr, pExprs, pQueryMsg, &code);
  } else {
    pObj->qhandle = vnodeQueryOnSingleTable(pMeterObjList, pGroupbyExpr, pExprs, pQueryMsg, &code);
  }

_query_over:
  // if failed to add ref for all meters in this query, abort current query
  if (code != TSDB_CODE_SUCCESS) {
    vnodeDecQueryRefCount(pQueryMsg, pMeterObjList, incNumber);
  }

  tfree(pQueryMsg->pSqlFuncExprs);
  tfree(pMeterObjList);
  ret = vnodeSendQueryRspMsg(pObj, code, pObj->qhandle);

  tfree(pQueryMsg->pSidExtInfo);
  for(int32_t i = 0; i < pQueryMsg->numOfCols; ++i) {
    vnodeFreeColumnInfo(&pQueryMsg->colList[i]);
  }

  atomic_fetch_add_32(&tsDnodeSelectReqNum, 1);
  return ret;
}

void vnodeExecuteRetrieveReq(SSchedMsg *pSched) {

}

void dnodeProcessRetrieveRequestCb(int code, SRetrieveMeterRsp *result, SShellObj *pObj) {
  if (pObj == NULL || result == NULL || code == TSDB_CODE_ACTION_IN_PROGRESS) {
    return;
  }
}

static void dnodeProcessRetrieveRequest(int8_t *pMsg, int32_t msgLen, SShellObj *pObj) {
  SRetrieveMeterMsg *pRetrieve = (SRetrieveMeterMsg *) pMsg;
  dnodeRetrieveData(pRetrieve, msgLen, pObj, dnodeProcessRetrieveRequestCb);
}

void dnodeProcessShellSubmitRequestCb(SShellSubmitRspMsg *result, void *pObj) {
  if (pObj == NULL || result == NULL || result->code == TSDB_CODE_ACTION_IN_PROGRESS) {
    return;
  }

  SShellObj *pShellObj = (SShellObj *) pObj;
  int32_t   msgLen     = sizeof(SShellSubmitRspMsg) + result->numOfFailedBlocks * sizeof(SShellSubmitRspBlock);
  SShellSubmitRspMsg *submitRsp = (SShellSubmitRspMsg *) taosBuildRspMsgWithSize(pShellObj->thandle,
                                                                                 TSDB_MSG_TYPE_SUBMIT_RSP, msgLen);
  if (submitRsp == NULL) {
    return;
  }

  dTrace("code:%d, numOfRows:%d affectedRows:%d", result->code, result->numOfRows, result->affectedRows);
  memcpy(submitRsp, result, msgLen);

  for (int i = 0; i < submitRsp->numOfFailedBlocks; ++i) {
    SShellSubmitRspBlock *block = &submitRsp->failedBlocks[i];
    if (block->code == TSDB_CODE_NOT_ACTIVE_VNODE || block->code == TSDB_CODE_INVALID_VNODE_ID) {
      dnodeSendVpeerCfgMsg(block->vnode);
    } else if (block->code == TSDB_CODE_INVALID_TABLE_ID || block->code == TSDB_CODE_NOT_ACTIVE_TABLE) {
      dnodeSendMeterCfgMsg(block->vnode, block->sid);
    }
    block->vnode = htonl(block->vnode);
    block->sid   = htonl(block->sid);
    block->code  = htonl(block->code);
  }
  submitRsp->code              = htonl(submitRsp->code);
  submitRsp->numOfRows         = htonl(submitRsp->numOfRows);
  submitRsp->affectedRows      = htonl(submitRsp->affectedRows);
  submitRsp->failedRows        = htonl(submitRsp->failedRows);
  submitRsp->numOfFailedBlocks = htonl(submitRsp->numOfFailedBlocks);

  taosSendMsgToPeer(pShellObj->thandle, (int8_t*)submitRsp, msgLen);
}

static void dnodeProcessShellSubmitRequest(int8_t *pMsg, int32_t msgLen, SShellObj *pObj) {
  SShellSubmitMsg *pSubmit = (SShellSubmitMsg *) pMsg;
  dnodeWriteData(pSubmit, pObj, dnodeProcessShellSubmitRequestCb);
  atomic_fetch_add_32(&tsDnodeInsertReqNum, 1);
}

SDnodeStatisInfo dnodeGetStatisInfo() {
  SDnodeStatisInfo info = {0};
  if (dnodeGetRunStatus() == TSDB_DNODE_RUN_STATUS_RUNING) {
    info.httpReqNum   = httpGetReqCount();
    info.selectReqNum = atomic_exchange_32(&tsDnodeSelectReqNum, 0);
    info.insertReqNum = atomic_exchange_32(&tsDnodeInsertReqNum, 0);
  }

  return info;
}