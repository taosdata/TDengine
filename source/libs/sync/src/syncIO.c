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

int32_t syncIOSendMsg(void *handle, const SEpSet *pEpSet, SRpcMsg *pMsg) { return 0; }

int32_t syncIOStart() { return 0; }

int32_t syncIOStop() { return 0; }

static void syncTick(void *param, void *tmrId) {
  SSyncIO *io = (SSyncIO *)param;
  sDebug("syncTick ... ");

  SRpcMsg rpcMsg;
  rpcMsg.pCont = rpcMallocCont(10);
  snprintf(rpcMsg.pCont, 10, "TICK");
  rpcMsg.contLen = 10;
  rpcMsg.handle = NULL;
  rpcMsg.msgType = 2;

  SRpcMsg *pTemp;

  pTemp = taosAllocateQitem(sizeof(SRpcMsg));
  memcpy(pTemp, &rpcMsg, sizeof(SRpcMsg));

  taosWriteQitem(io->pMsgQ, pTemp);

  io->syncTimer = taosTmrStart(syncTick, 1000, io, io->syncTimerManager);
}

void *syncConsumer(void *param) {
  SSyncIO *io = param;

  STaosQall *qall;
  SRpcMsg *  pRpcMsg, rpcMsg;
  int        type;

  qall = taosAllocateQall();

  while (1) {
    int numOfMsgs = taosReadAllQitemsFromQset(io->pQset, qall, NULL, NULL);
    sDebug("%d sync-io msgs are received", numOfMsgs);
    if (numOfMsgs <= 0) break;

    for (int i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(qall, (void **)&pRpcMsg);
      sDebug("sync-io recv type:%d msg:%s", pRpcMsg->msgType, (char *)(pRpcMsg->pCont));
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
        rpcMsg.handle = pRpcMsg->handle;
        rpcMsg.code = 0;
        rpcSendResponse(&rpcMsg);
      }

      taosFreeQitem(pRpcMsg);
    }
  }

  taosFreeQall(qall);
  return NULL;
}

static int retrieveAuthInfo(void *parent, char *meterId, char *spi, char *encrypt, char *secret, char *ckey) {
  // app shall retrieve the auth info based on meterID from DB or a data file
  // demo code here only for simple demo
  int ret = 0;
  return ret;
}

static void processResponse(void *pParent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  sDebug("processResponse ... ");
  rpcFreeCont(pMsg->pCont);
}

static void processRequestMsg(void *pParent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SSyncIO *io = pParent;
  SRpcMsg *pTemp;

  pTemp = taosAllocateQitem(sizeof(SRpcMsg));
  memcpy(pTemp, pMsg, sizeof(SRpcMsg));

  sDebug("request is received, type:%d, contLen:%d, item:%p", pMsg->msgType, pMsg->contLen, pTemp);
  taosWriteQitem(io->pMsgQ, pTemp);
}

SSyncIO *syncIOCreate() {
  SSyncIO *io = (SSyncIO *)malloc(sizeof(SSyncIO));
  memset(io, 0, sizeof(*io));

  io->pMsgQ = taosOpenQueue();
  io->pQset = taosOpenQset();
  taosAddIntoQset(io->pQset, io->pMsgQ, NULL);

  io->start = doSyncIOStart;
  io->stop = doSyncIOStop;
  io->ping = doSyncIOPing;
  io->onMsg = doSyncIOOnMsg;
  io->destroy = doSyncIODestroy;

  return io;
}

static int32_t doSyncIOStart(SSyncIO *io) {
  taosBlockSIGPIPE();

  tsRpcForceTcp = 1;

  // cient rpc init
  {
    SRpcInit rpcInit;
    memset(&rpcInit, 0, sizeof(rpcInit));
    rpcInit.localPort = 0;
    rpcInit.label = "SYNC-IO-CLIENT";
    rpcInit.numOfThreads = 1;
    rpcInit.cfp = processResponse;
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
    rpcInit.localPort = 38000;
    rpcInit.label = "SYNC-IO-SERVER";
    rpcInit.numOfThreads = 1;
    rpcInit.cfp = processRequestMsg;
    rpcInit.sessions = 1000;
    rpcInit.idleTime = 2 * 1500;
    rpcInit.afp = retrieveAuthInfo;
    rpcInit.parent = io;
    rpcInit.connType = TAOS_CONN_SERVER;

    void *pRpc = rpcOpen(&rpcInit);
    if (pRpc == NULL) {
      sError("failed to start RPC server");
      return -1;
    }
  }

  io->epSet.inUse = 0;
  addEpIntoEpSet(&io->epSet, "127.0.0.1", 38000);

  // start consumer thread
  {
    if (pthread_create(&io->tid, NULL, syncConsumer, io) != 0) {
      sError("failed to create sync consumer thread since %s", strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }

  // start tmr thread
  io->syncTimerManager = taosTmrInit(1000, 50, 10000, "SYNC");
  io->syncTimer = taosTmrStart(syncTick, 1000, io, io->syncTimerManager);

  return 0;
}

static int32_t doSyncIOStop(SSyncIO *io) {
  atomic_store_8(&io->isStart, 0);
  pthread_join(io->tid, NULL);
  return 0;
}

static int32_t doSyncIOPing(SSyncIO *io) {
  SRpcMsg rpcMsg, rspMsg;

  rpcMsg.pCont = rpcMallocCont(10);
  snprintf(rpcMsg.pCont, 10, "ping");
  rpcMsg.contLen = 10;
  rpcMsg.handle = NULL;
  rpcMsg.msgType = 1;

  rpcSendRequest(io->clientRpc, &io->epSet, &rpcMsg, NULL);

  return 0;
}

static int32_t doSyncIOOnMsg(struct SSyncIO *io, void *pParent, SRpcMsg *pMsg, SEpSet *pEpSet) { return 0; }

static int32_t doSyncIODestroy(SSyncIO *io) {
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

  if (io->pMsgQ != NULL) {
    free(io->pMsgQ);
    io->pMsgQ = NULL;
  }

  if (io->pQset != NULL) {
    free(io->pQset);
    io->pQset = NULL;
  }

  return 0;
}
