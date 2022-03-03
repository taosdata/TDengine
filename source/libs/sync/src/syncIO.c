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

#include "syncIO.h"
#include <tep.h>
#include "syncOnMessage.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tutil.h"

SSyncIO *gSyncIO = NULL;

// local function ------------
static int32_t  syncIOStartInternal(SSyncIO *io);
static int32_t  syncIOStopInternal(SSyncIO *io);
static SSyncIO *syncIOCreate(char *host, uint16_t port);
static int32_t  syncIODestroy(SSyncIO *io);

static void *syncIOConsumerFunc(void *param);
static int   syncIOAuth(void *parent, char *meterId, char *spi, char *encrypt, char *secret, char *ckey);
static void  syncIOProcessRequest(void *pParent, SRpcMsg *pMsg, SEpSet *pEpSet);
static void  syncIOProcessReply(void *pParent, SRpcMsg *pMsg, SEpSet *pEpSet);

static int32_t syncIOTickQInternal(SSyncIO *io);
static void    syncIOTickQFunc(void *param, void *tmrId);
static int32_t syncIOTickPingInternal(SSyncIO *io);
static void    syncIOTickPingFunc(void *param, void *tmrId);
// ----------------------------

// public function ------------
int32_t syncIOSendMsg(void *clientRpc, const SEpSet *pEpSet, SRpcMsg *pMsg) {
  sTrace(
      "<--- syncIOSendMsg ---> clientRpc:%p, numOfEps:%d, inUse:%d, destAddr:%s-%u, pMsg->ahandle:%p, pMsg->handle:%p, "
      "pMsg->msgType:%d, pMsg->contLen:%d",
      clientRpc, pEpSet->numOfEps, pEpSet->inUse, pEpSet->eps[0].fqdn, pEpSet->eps[0].port, pMsg->ahandle, pMsg->handle,
      pMsg->msgType, pMsg->contLen);
  pMsg->handle = NULL;
  rpcSendRequest(clientRpc, pEpSet, pMsg, NULL);
  return 0;
}

int32_t syncIOStart(char *host, uint16_t port) {
  gSyncIO = syncIOCreate(host, port);
  assert(gSyncIO != NULL);

  int32_t ret = syncIOStartInternal(gSyncIO);
  assert(ret == 0);

  sTrace("syncIOStart ok, gSyncIO:%p gSyncIO->clientRpc:%p", gSyncIO, gSyncIO->clientRpc);
  return 0;
}

int32_t syncIOStop() {
  int32_t ret = syncIOStopInternal(gSyncIO);
  assert(ret == 0);

  ret = syncIODestroy(gSyncIO);
  assert(ret == 0);
  return ret;
}

int32_t syncIOTickQ() {
  int32_t ret = syncIOTickQInternal(gSyncIO);
  assert(ret == 0);
  return ret;
}

int32_t syncIOTickPing() {
  int32_t ret = syncIOTickPingInternal(gSyncIO);
  assert(ret == 0);
  return ret;
}

// local function ------------
static int32_t syncIOStartInternal(SSyncIO *io) {
  taosBlockSIGPIPE();

  rpcInit();
  tsRpcForceTcp = 1;

  // cient rpc init
  {
    SRpcInit rpcInit;
    memset(&rpcInit, 0, sizeof(rpcInit));
    rpcInit.localPort = 0;
    rpcInit.label = "SYNC-IO-CLIENT";
    rpcInit.numOfThreads = 1;
    rpcInit.cfp = syncIOProcessReply;
    rpcInit.sessions = 100;
    rpcInit.idleTime = 100;
    rpcInit.user = "sync-io";
    rpcInit.secret = "sync-io";
    rpcInit.ckey = "key";
    rpcInit.spi = 0;
    rpcInit.connType = TAOS_CONN_CLIENT;

    io->clientRpc = rpcOpen(&rpcInit);
    if (io->clientRpc == NULL) {
      sError("failed to initialize RPC");
      return -1;
    }
  }

  // server rpc init
  {
    SRpcInit rpcInit;
    memset(&rpcInit, 0, sizeof(rpcInit));
    rpcInit.localPort = io->myAddr.eps[0].port;
    rpcInit.label = "SYNC-IO-SERVER";
    rpcInit.numOfThreads = 1;
    rpcInit.cfp = syncIOProcessRequest;
    rpcInit.sessions = 1000;
    rpcInit.idleTime = 2 * 1500;
    rpcInit.afp = syncIOAuth;
    rpcInit.parent = io;
    rpcInit.connType = TAOS_CONN_SERVER;

    void *pRpc = rpcOpen(&rpcInit);
    if (pRpc == NULL) {
      sError("failed to start RPC server");
      return -1;
    }
  }

  // start consumer thread
  {
    if (pthread_create(&io->consumerTid, NULL, syncIOConsumerFunc, io) != 0) {
      sError("failed to create sync consumer thread since %s", strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }

  // start tmr thread
  io->ioTimerManager = taosTmrInit(1000, 50, 10000, "SYNC");

  return 0;
}

static int32_t syncIOStopInternal(SSyncIO *io) {
  atomic_store_8(&io->isStart, 0);
  pthread_join(io->consumerTid, NULL);
  return 0;
}

static SSyncIO *syncIOCreate(char *host, uint16_t port) {
  SSyncIO *io = (SSyncIO *)malloc(sizeof(SSyncIO));
  memset(io, 0, sizeof(*io));

  io->pMsgQ = taosOpenQueue();
  io->pQset = taosOpenQset();
  taosAddIntoQset(io->pQset, io->pMsgQ, NULL);

  io->myAddr.inUse = 0;
  addEpIntoEpSet(&io->myAddr, host, port);

  return io;
}

static int32_t syncIODestroy(SSyncIO *io) {
  int8_t start = atomic_load_8(&io->isStart);
  assert(start == 0);

  if (io->serverRpc != NULL) {
    free(io->serverRpc);
    io->serverRpc = NULL;
  }

  if (io->clientRpc != NULL) {
    free(io->clientRpc);
    io->clientRpc = NULL;
  }

  taosCloseQueue(io->pMsgQ);
  taosCloseQset(io->pQset);

  return 0;
}

static void *syncIOConsumerFunc(void *param) {
  SSyncIO *io = param;

  STaosQall *qall;
  SRpcMsg *  pRpcMsg, rpcMsg;
  int        type;

  qall = taosAllocateQall();

  while (1) {
    int numOfMsgs = taosReadAllQitemsFromQset(io->pQset, qall, NULL, NULL);
    sTrace("syncIOConsumerFunc %d msgs are received", numOfMsgs);
    if (numOfMsgs <= 0) break;

    for (int i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(qall, (void **)&pRpcMsg);
      sTrace("syncIOConsumerFunc get item from queue: msgType:%d contLen:%d msg:%s", pRpcMsg->msgType, pRpcMsg->contLen,
             (char *)(pRpcMsg->pCont));

      if (pRpcMsg->msgType == SYNC_PING) {
        if (io->FpOnSyncPing != NULL) {
          SyncPing *pSyncMsg;

          SRpcMsg tmpRpcMsg;
          memcpy(&tmpRpcMsg, pRpcMsg, sizeof(SRpcMsg));
          pSyncMsg = syncPingBuild(tmpRpcMsg.contLen);

          syncPingFromRpcMsg(pRpcMsg, pSyncMsg);

          // memcpy(pSyncMsg, tmpRpcMsg.pCont, tmpRpcMsg.contLen);

          io->FpOnSyncPing(io->pSyncNode, pSyncMsg);
        }

      } else if (pRpcMsg->msgType == SYNC_PING_REPLY) {
        SyncPingReply *pSyncMsg = syncPingReplyBuild(pRpcMsg->contLen);
        syncPingReplyFromRpcMsg(pRpcMsg, pSyncMsg);

        if (io->FpOnSyncPingReply != NULL) {
          io->FpOnSyncPingReply(io->pSyncNode, pSyncMsg);
        }
      } else {
        ;
      }
    }

    taosResetQitems(qall);
    for (int i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(qall, (void **)&pRpcMsg);
      rpcFreeCont(pRpcMsg->pCont);

      if (pRpcMsg->handle != NULL) {
        int msgSize = 128;
        memset(&rpcMsg, 0, sizeof(rpcMsg));
        rpcMsg.pCont = rpcMallocCont(msgSize);
        rpcMsg.contLen = msgSize;
        snprintf(rpcMsg.pCont, rpcMsg.contLen, "%s", "give a reply");
        rpcMsg.handle = pRpcMsg->handle;
        rpcMsg.code = 0;

        sTrace("syncIOConsumerFunc rpcSendResponse ... msgType:%d contLen:%d", pRpcMsg->msgType, rpcMsg.contLen);
        rpcSendResponse(&rpcMsg);
      }

      taosFreeQitem(pRpcMsg);
    }
  }

  taosFreeQall(qall);
  return NULL;
}

static int syncIOAuth(void *parent, char *meterId, char *spi, char *encrypt, char *secret, char *ckey) {
  // app shall retrieve the auth info based on meterID from DB or a data file
  // demo code here only for simple demo
  int ret = 0;
  return ret;
}

static void syncIOProcessRequest(void *pParent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  sTrace("<-- syncIOProcessRequest --> type:%d, contLen:%d, cont:%s", pMsg->msgType, pMsg->contLen,
         (char *)pMsg->pCont);

  SSyncIO *io = pParent;
  SRpcMsg *pTemp;

  pTemp = taosAllocateQitem(sizeof(SRpcMsg));
  memcpy(pTemp, pMsg, sizeof(SRpcMsg));

  taosWriteQitem(io->pMsgQ, pTemp);
}

static void syncIOProcessReply(void *pParent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  sTrace("syncIOProcessReply: type:%d, contLen:%d msg:%s", pMsg->msgType, pMsg->contLen, (char *)pMsg->pCont);
  rpcFreeCont(pMsg->pCont);
}

static int32_t syncIOTickQInternal(SSyncIO *io) {
  io->ioTimerTickQ = taosTmrStart(syncIOTickQFunc, 1000, io, io->ioTimerManager);
  return 0;
}

static void syncIOTickQFunc(void *param, void *tmrId) {
  SSyncIO *io = (SSyncIO *)param;
  sTrace("<-- syncIOTickQFunc -->");

  SRpcMsg rpcMsg;
  rpcMsg.contLen = 64;
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  snprintf(rpcMsg.pCont, rpcMsg.contLen, "%s", "syncIOTickQ");
  rpcMsg.handle = NULL;
  rpcMsg.msgType = 55;

  SRpcMsg *pTemp;
  pTemp = taosAllocateQitem(sizeof(SRpcMsg));
  memcpy(pTemp, &rpcMsg, sizeof(SRpcMsg));

  taosWriteQitem(io->pMsgQ, pTemp);
  taosTmrReset(syncIOTickQFunc, 1000, io, io->ioTimerManager, &io->ioTimerTickQ);
}

static int32_t syncIOTickPingInternal(SSyncIO *io) {
  io->ioTimerTickPing = taosTmrStart(syncIOTickPingFunc, 1000, io, io->ioTimerManager);
  return 0;
}

static void syncIOTickPingFunc(void *param, void *tmrId) {
  SSyncIO *io = (SSyncIO *)param;
  sTrace("<-- syncIOTickPingFunc -->");

  SRpcMsg rpcMsg;
  rpcMsg.contLen = 64;
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  snprintf(rpcMsg.pCont, rpcMsg.contLen, "%s", "syncIOTickPing");
  rpcMsg.handle = NULL;
  rpcMsg.msgType = 77;

  rpcSendRequest(io->clientRpc, &io->myAddr, &rpcMsg, NULL);
  taosTmrReset(syncIOTickPingFunc, 1000, io, io->ioTimerManager, &io->ioTimerTickPing);
}