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

static void dnodeProcessRetrieveRequest(int8_t *pCont, int32_t contLen, void *pConn);
static void dnodeProcessQueryRequest(int8_t *pCont, int32_t contLen, void *pConn);
static void dnodeProcessShellSubmitRequest(int8_t *pCont, int32_t contLen, void *pConn);

static void    *tsDnodeShellServer = NULL;
static int32_t tsDnodeQueryReqNum  = 0;
static int32_t tsDnodeSubmitReqNum = 0;

void dnodeProcessMsgFromShell(int32_t msgType, void *pCont, int32_t contLen, void *handle, int32_t index) {
  assert(handle != NULL);

  if (pCont == NULL || contLen == 0) {
    dnodeFreeQInfo(handle);
    dTrace("conn:%p, free query info", handle);
    return;
  }

  if (dnodeGetRunStatus() != TSDB_DNODE_RUN_STATUS_RUNING) {
    rpcSendSimpleRsp(handle, TSDB_CODE_NOT_READY);
    dTrace("conn:%p, query msg is ignored since dnode not running", handle);
    return;
  }

  dTrace("conn:%p, msg:%s is received", handle, taosMsg[msgType]);

  if (msgType == TSDB_MSG_TYPE_DNODE_QUERY) {
    dnodeProcessQueryRequest(pCont, contLen, handle);
  } else if (msgType == TSDB_MSG_TYPE_DNODE_RETRIEVE) {
    dnodeProcessRetrieveRequest(pCont, contLen, handle);
  } else if (msgType == TSDB_MSG_TYPE_DNODE_SUBMIT) {
    dnodeProcessShellSubmitRequest(pCont, contLen, handle);
  } else {
    dError("conn:%p, msg:%s is not processed", handle, taosMsg[msgType]);
  }
}

int32_t dnodeInitShell() {
  int32_t numOfThreads = tsNumOfCores * tsNumOfThreadsPerCore;
  numOfThreads = (int32_t) ((1.0 - tsRatioOfQueryThreads) * numOfThreads / 2.0);
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = tsAnyIp ? "0.0.0.0" : tsPrivateIp;
  rpcInit.localPort    = tsVnodeShellPort;
  rpcInit.label        = "DND-shell";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.fp           = dnodeProcessMsgFromShell;
  rpcInit.sessions     = TSDB_SESSIONS_PER_DNODE;
  rpcInit.connType     = TAOS_CONN_SOCKET_TYPE_S();
  rpcInit.idleTime     = tsShellActivityTimer * 2000;

  tsDnodeShellServer = rpcOpen(&rpcInit);
  if (tsDnodeShellServer == NULL) {
    dError("failed to init connection from shell");
    return -1;
  }

  dPrint("shell is opened");
  return TSDB_CODE_SUCCESS;
}

void dnodeCleanupShell() {
  if (tsDnodeShellServer) {
    rpcClose(tsDnodeShellServer);
  }

  dnodeFreeQInfos();
}

void dnodeProcessQueryRequestCb(int code, void *pQInfo, void *pConn) {
  int32_t contLen = sizeof(SQueryMeterRsp);
  SQueryMeterRsp *queryRsp = (SQueryMeterRsp *) rpcMallocCont(contLen);
  if (queryRsp == NULL) {
    return;
  }

  dTrace("conn:%p, query data, code:%d pQInfo:%p", pConn, code, pQInfo);

  queryRsp->code    = htonl(code);
  queryRsp->qhandle = (uint64_t) (pQInfo);

  rpcSendResponse(pConn, queryRsp, contLen);
}

static void dnodeProcessQueryRequest(int8_t *pCont, int32_t contLen, void *pConn) {
  atomic_fetch_add_32(&tsDnodeQueryReqNum, 1);
  dTrace("conn:%p, start to query data", pConn);

  SQueryMeterMsg *pQuery = (SQueryMeterMsg *) pCont;
  dnodeQueryData(pQuery, pConn, dnodeProcessQueryRequestCb);
}

void dnodeProcessRetrieveRequestCb(int32_t code, void *pQInfo, void *pConn) {
  dTrace("conn:%p, retrieve data, code:%d", pConn, code);

  assert(pConn != NULL);
  if (code != TSDB_CODE_SUCCESS) {
    rpcSendSimpleRsp(pConn, code);
    return;
  }

  assert(pQInfo != NULL);
  int32_t contLen = dnodeGetRetrieveDataSize(pQInfo);
  SRetrieveMeterRsp *retrieveRsp = (SRetrieveMeterRsp *) rpcMallocCont(contLen);
  if (retrieveRsp == NULL) {
    rpcSendSimpleRsp(pConn, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  code = dnodeGetRetrieveData(pQInfo, retrieveRsp);
  if (code != TSDB_CODE_SUCCESS) {
    rpcSendSimpleRsp(pConn, TSDB_CODE_INVALID_QHANDLE);
  }

  retrieveRsp->numOfRows = htonl(retrieveRsp->numOfRows);
  retrieveRsp->precision = htons(retrieveRsp->precision);
  retrieveRsp->offset    = htobe64(retrieveRsp->offset);
  retrieveRsp->useconds  = htobe64(retrieveRsp->useconds);

  rpcSendResponse(pConn, retrieveRsp, contLen);
}

static void dnodeProcessRetrieveRequest(int8_t *pCont, int32_t contLen, void *pConn) {
  dTrace("conn:%p, start to retrieve data", pConn);

  SRetrieveMeterMsg *pRetrieve = (SRetrieveMeterMsg *) pCont;
  dnodeRetrieveData(pRetrieve, pConn, dnodeProcessRetrieveRequestCb);
}

void dnodeProcessShellSubmitRequestCb(SShellSubmitRspMsg *result, void *pConn) {
  assert(result != NULL);

  if (result->code != 0) {
    rpcSendSimpleRsp(pConn, result->code);
    return;
  }

  int32_t contLen = sizeof(SShellSubmitRspMsg) + result->numOfFailedBlocks * sizeof(SShellSubmitRspBlock);
  SShellSubmitRspMsg *submitRsp = (SShellSubmitRspMsg *) rpcMallocCont(contLen);
  if (submitRsp == NULL) {
    rpcSendSimpleRsp(pConn, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  dTrace("code:%d, numOfRows:%d affectedRows:%d", result->code, result->numOfRows, result->affectedRows);
  memcpy(submitRsp, result, contLen);

  for (int i = 0; i < submitRsp->numOfFailedBlocks; ++i) {
    SShellSubmitRspBlock *block = &submitRsp->failedBlocks[i];
    if (block->code == TSDB_CODE_NOT_ACTIVE_VNODE || block->code == TSDB_CODE_INVALID_VNODE_ID) {
      dnodeSendVpeerCfgMsg(block->vnode);
    } else if (block->code == TSDB_CODE_INVALID_TABLE_ID || block->code == TSDB_CODE_NOT_ACTIVE_TABLE) {
      dnodeSendMeterCfgMsg(block->vnode, block->sid);
    }
    block->index = htonl(block->index);
    block->vnode = htonl(block->vnode);
    block->sid   = htonl(block->sid);
    block->code  = htonl(block->code);
  }
  submitRsp->code              = htonl(submitRsp->code);
  submitRsp->numOfRows         = htonl(submitRsp->numOfRows);
  submitRsp->affectedRows      = htonl(submitRsp->affectedRows);
  submitRsp->failedRows        = htonl(submitRsp->failedRows);
  submitRsp->numOfFailedBlocks = htonl(submitRsp->numOfFailedBlocks);

  rpcSendResponse(pConn, submitRsp, contLen);
}

static void dnodeProcessShellSubmitRequest(int8_t *pCont, int32_t contLen, void *pConn) {
  SShellSubmitMsg *pSubmit = (SShellSubmitMsg *) pCont;
  dnodeWriteData(pSubmit, pConn, dnodeProcessShellSubmitRequestCb);
  atomic_fetch_add_32(&tsDnodeSubmitReqNum, 1);
}

SDnodeStatisInfo dnodeGetStatisInfo() {
  SDnodeStatisInfo info = {0};
  if (dnodeGetRunStatus() == TSDB_DNODE_RUN_STATUS_RUNING) {
    info.httpReqNum   = httpGetReqCount();
    info.queryReqNum = atomic_exchange_32(&tsDnodeQueryReqNum, 0);
    info.submitReqNum = atomic_exchange_32(&tsDnodeSubmitReqNum, 0);
  }

  return info;
}