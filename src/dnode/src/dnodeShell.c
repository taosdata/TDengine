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
#include "dnodeVnodeMgmt.h"
#include "dnodeWrite.h"

static void dnodeProcessRetrieveMsg(void *pCont, int32_t contLen, void *pConn);
static void dnodeProcessQueryMsg(void *pCont, int32_t contLen, void *pConn);
static void dnodeProcessSubmitMsg(void *pCont, int32_t contLen, void *pConn);
static void dnodeProcessMsgFromShell(char msgType, void *pCont, int contLen, void *handle, int32_t code);
static int  dnodeRetrieveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey);

static void    *tsDnodeShellServer = NULL;
static int32_t tsDnodeQueryReqNum  = 0;
static int32_t tsDnodeSubmitReqNum = 0;

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
  rpcInit.cfp          = dnodeProcessMsgFromShell;
  rpcInit.sessions     = TSDB_SESSIONS_PER_DNODE;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 2000;
  rpcInit.afp          = dnodeRetrieveUserAuthInfo;

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
}

SDnodeStatisInfo dnodeGetStatisInfo() {
  SDnodeStatisInfo info = {0};
  if (dnodeGetRunStatus() == TSDB_DNODE_RUN_STATUS_RUNING) {
    info.httpReqNum   = httpGetReqCount();
    info.queryReqNum  = atomic_exchange_32(&tsDnodeQueryReqNum, 0);
    info.submitReqNum = atomic_exchange_32(&tsDnodeSubmitReqNum, 0);
  }

  return info;
}

static void dnodeProcessMsgFromShell(char msgType, void *pCont, int contLen, void *handle, int32_t code) {
  if (dnodeGetRunStatus() != TSDB_DNODE_RUN_STATUS_RUNING) {
    rpcSendResponse(handle, TSDB_CODE_NOT_READY, 0, 0);
    dTrace("query msg is ignored since dnode not running");
    return;
  }

  dTrace("conn:%p, msg:%s is received", handle, taosMsg[(int8_t)msgType]);

  if (msgType == TSDB_MSG_TYPE_QUERY) {
    dnodeProcessQueryMsg(pCont, contLen, handle);
  } else if (msgType == TSDB_MSG_TYPE_RETRIEVE) {
    dnodeProcessRetrieveMsg(pCont, contLen, handle);
  } else if (msgType == TSDB_MSG_TYPE_SUBMIT) {
    dnodeProcessSubmitMsg(pCont, contLen, handle);
  } else {
    dError("conn:%p, msg:%s is not processed", handle, taosMsg[(int8_t)msgType]);
  }

  //TODO free may be cause segmentfault
  // rpcFreeCont(pCont);
}

static int dnodeRetrieveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  return TSDB_CODE_SUCCESS;
}

static void dnodeProcessQueryMsgCb(int32_t code, void *pQInfo, void *pConn) {
  dTrace("conn:%p, query is returned, code:%d", pConn, code);

  int32_t contLen = sizeof(SQueryTableRsp);
  SQueryTableRsp *queryRsp = (SQueryTableRsp *) rpcMallocCont(contLen);
  if (queryRsp == NULL) {
    rpcSendResponse(pConn, TSDB_CODE_SERV_OUT_OF_MEMORY, NULL, 0);
    return;
  }

  queryRsp->code    = htonl(code);
  queryRsp->qhandle = htobe64((uint64_t) (pQInfo));
  rpcSendResponse(pConn, TSDB_CODE_SUCCESS, queryRsp, contLen);
}

static void dnodeProcessQueryMsg(void *pCont, int32_t contLen, void *pConn) {
  atomic_fetch_add_32(&tsDnodeQueryReqNum, 1);
  SQueryTableMsg *pQuery = (SQueryTableMsg *) pCont;
  dnodeQueryData(pQuery, pConn, dnodeProcessQueryMsgCb);
}

void dnodeProcessRetrieveMsgCb(int32_t code, void *pQInfo, void *pConn) {
  dTrace("conn:%p, retrieve is returned, code:%d", pConn, code);

  assert(pConn != NULL);
  if (code != TSDB_CODE_SUCCESS) {
    rpcSendResponse(pConn, code, 0, 0);
    return;
  }

  assert(pQInfo != NULL);
  int32_t contLen = dnodeGetRetrieveDataSize(pQInfo);
  SRetrieveTableRsp *pRetrieve = (SRetrieveTableRsp *) rpcMallocCont(contLen);
  if (pRetrieve == NULL) {
    rpcSendResponse(pConn, TSDB_CODE_SERV_OUT_OF_MEMORY, 0, 0);
    return;
  }

  code = dnodeGetRetrieveData(pQInfo, pRetrieve);
  if (code != TSDB_CODE_SUCCESS) {
    rpcSendResponse(pConn, TSDB_CODE_INVALID_QHANDLE, 0, 0);
  }

  pRetrieve->numOfRows = htonl(pRetrieve->numOfRows);
  pRetrieve->precision = htons(pRetrieve->precision);
  pRetrieve->offset    = htobe64(pRetrieve->offset);
  pRetrieve->useconds  = htobe64(pRetrieve->useconds);

  rpcSendResponse(pConn, TSDB_CODE_SUCCESS, pRetrieve, contLen);
}

static void dnodeProcessRetrieveMsg(void *pCont, int32_t contLen, void *pConn) {
  SRetrieveTableMsg *pRetrieve = (SRetrieveTableMsg *) pCont;
  pRetrieve->qhandle = htobe64(pRetrieve->qhandle);
  pRetrieve->free    = htons(pRetrieve->free);

  dnodeRetrieveData(pRetrieve, pConn, dnodeProcessRetrieveMsgCb);
}

void dnodeProcessSubmitMsgCb(SShellSubmitRspMsg *result, void *pConn) {
  assert(result != NULL);
  dTrace("conn:%p, submit is returned, code:%d", pConn, result->code);

  if (result->code != 0) {
    rpcSendResponse(pConn, result->code, NULL, 0);
    return;
  }

  int32_t contLen = sizeof(SShellSubmitRspMsg) + result->numOfFailedBlocks * sizeof(SShellSubmitRspBlock);
  SShellSubmitRspMsg *submitRsp = (SShellSubmitRspMsg *) rpcMallocCont(contLen);
  if (submitRsp == NULL) {
    rpcSendResponse(pConn, TSDB_CODE_SERV_OUT_OF_MEMORY, NULL, 0);
    return;
  }

  memcpy(submitRsp, result, contLen);

  for (int i = 0; i < submitRsp->numOfFailedBlocks; ++i) {
    SShellSubmitRspBlock *block = &submitRsp->failedBlocks[i];
    if (block->code == TSDB_CODE_NOT_ACTIVE_VNODE || block->code == TSDB_CODE_INVALID_VNODE_ID) {
      dnodeSendVnodeCfgMsg(block->vnode);
    } else if (block->code == TSDB_CODE_INVALID_TABLE_ID || block->code == TSDB_CODE_NOT_ACTIVE_TABLE) {
      dnodeSendTableCfgMsg(block->vnode, block->sid);
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

  rpcSendResponse(pConn, TSDB_CODE_SUCCESS, submitRsp, contLen);
}

static void dnodeProcessSubmitMsg(void *pCont, int32_t contLen, void *pConn) {
  atomic_fetch_add_32(&tsDnodeSubmitReqNum, 1);

  SShellSubmitMsg *pSubmit = (SShellSubmitMsg *) pCont;
  dnodeWriteData(pSubmit, pConn, dnodeProcessSubmitMsgCb);
}
