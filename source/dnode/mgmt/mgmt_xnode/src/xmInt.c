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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "xmInt.h"
#include "libs/function/tudf.h"

static int32_t xmRequire(const SMgmtInputOpt *pInput, bool *required) {
  return dmReadFile(pInput->path, pInput->name, required);
}

static void xmInitOption(SXnodeMgmt *pMgmt, SXnodeOpt *pOption) { pOption->msgCb = pMgmt->msgCb; }

static void xmClose(SXnodeMgmt *pMgmt) {
  if (pMgmt->pXnode != NULL) {
    // xmStopWorker(pMgmt);
    xndClose(pMgmt->pXnode);
    pMgmt->pXnode = NULL;
  }

  taosMemoryFree(pMgmt);
}

static int32_t xndOpenWrapper(SXnodeOpt *pOption, SXnode **pXnode) {
  int32_t code = xndOpen(pOption, pXnode);
  return code;
}

int32_t xmPutMsgToQueue(SXnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc) {
  int32_t  code;
  SRpcMsg *pMsg;

  code = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, pRpc->contLen, (void **)&pMsg);
  if (code) {
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    return code = terrno;
  }

  SXnode *pXnode = pMgmt->pXnode;
  if (pXnode == NULL) {
    code = terrno;
    dError("msg:%p failed to put into xnode queue since %s, type:%s qtype:%d len:%d", pMsg, tstrerror(code),
           TMSG_INFO(pMsg->msgType), qtype, pRpc->contLen);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
    return code;
  }

  SMsgHead *pHead = pRpc->pCont;
  pHead->contLen = htonl(pHead->contLen);
  pHead->vgId = SNODE_HANDLE;
  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  pRpc->pCont = NULL;

  switch (qtype) {
    case WRITE_QUEUE:
      // code = xmPutNodeMsgToWriteQueue(pMgmt, pMsg);
      // break;
    default:
      code = TSDB_CODE_INVALID_PARA;
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      return code;
  }
  return code;
}

static int32_t xmOpen(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  int32_t     code = 0;
  SXnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SXnodeMgmt));
  if (pMgmt == NULL) {
    return terrno;
  }

  pMgmt->pData = pInput->pData;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.putToQueueFp = (PutToQueueFp)xmPutMsgToQueue;
  pMgmt->msgCb.mgmt = pMgmt;

  SXnodeOpt option = {0};
  xmInitOption(pMgmt, &option);

  code = xndOpenWrapper(&option, &pMgmt->pXnode);
  if (code != 0) {
    dError("failed to open xnode since %s", tstrerror(code));
    xmClose(pMgmt);
    return code;
  }
  tmsgReportStartup("xnode-impl", "initialized");

  if ((code = udfcOpen()) != 0) {
    dError("xnode can not open udfc");
    xmClose(pMgmt);
    return code;
  }
  /*
  if ((code = xmStartWorker(pMgmt)) != 0) {
    dError("failed to start xnode worker since %s", tstrerror(code));
    xmClose(pMgmt);
    return code;
  }
  tmsgReportStartup("xnode-worker", "initialized");
  */
  pOutput->pMgmt = pMgmt;
  return code;
}

int32_t xmProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  int32_t          code = 0;
  SDCreateXnodeReq createReq = {0};
  if (tDeserializeSMCreateXnodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    return code;
  }

  if (pInput->pData->dnodeId != 0 && createReq.dnodeId != pInput->pData->dnodeId) {
    code = TSDB_CODE_INVALID_OPTION;
    dError("failed to create xnode since %s", tstrerror(code));

    tFreeSMCreateXnodeReq(&createReq);
    return code;
  }

  bool deployed = true;
  if ((code = dmWriteFile(pInput->path, pInput->name, deployed)) != 0) {
    dError("failed to write xnode file since %s", tstrerror(code));

    tFreeSMCreateXnodeReq(&createReq);
    return code;
  }

  tFreeSMCreateXnodeReq(&createReq);

  return 0;
}

int32_t xmProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  int32_t        code = 0;
  SDDropXnodeReq dropReq = {0};
  if (tDeserializeSMDropXnodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;

    return code;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    code = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop xnode since %s", tstrerror(code));

    tFreeSMDropXnodeReq(&dropReq);
    return code;
  }

  bool deployed = false;
  if ((code = dmWriteFile(pInput->path, pInput->name, deployed)) != 0) {
    dError("failed to write xnode file since %s", tstrerror(code));

    tFreeSMDropXnodeReq(&dropReq);
    return code;
  }

  tFreeSMDropXnodeReq(&dropReq);

  return 0;
}

SArray *xmGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(4, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  code = 0;
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}

SMgmtFunc xmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = xmOpen;
  mgmtFunc.closeFp = (NodeCloseFp)xmClose;
  mgmtFunc.createFp = (NodeCreateFp)xmProcessCreateReq;
  mgmtFunc.dropFp = (NodeDropFp)xmProcessDropReq;
  mgmtFunc.requiredFp = xmRequire;
  mgmtFunc.getHandlesFp = xmGetMsgHandles;

  return mgmtFunc;
}
