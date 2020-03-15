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
#include "taosmsg.h"
#include "tlog.h"
#include "tqueue.h"
#include "trpc.h"
#include "dnodeRead.h"
#include "dnodeMgmt.h"

typedef struct {
  int32_t  code;
  int32_t  count;
  int32_t  numOfVnodes;
} SRpcContext;

typedef struct {
  void        *pCont;
  int32_t      contLen;
  SRpcMsg      rpcMsg;
  SRpcContext *pRpcContext;  // RPC message context
} SReadMsg;

static void *dnodeProcessReadQueue(void *param);
static void  dnodeProcessReadResult(void *pVnode, SReadMsg *pRead);
static void  dnodeHandleIdleReadWorker();
static void  dnodeProcessQueryMsg(void *pVnode, SReadMsg *pMsg);
static void  dnodeProcessRetrieveMsg(void *pVnode, SReadMsg *pMsg);
static void(*dnodeProcessReadMsgFp[TSDB_MSG_TYPE_MAX])(void *pVnode, SReadMsg *pNode);

// module global variable
static taos_qset readQset;
static int32_t   threads;    // number of query threads
static int32_t   maxThreads;
static int32_t   minThreads;

int32_t dnodeInitRead() {
  dnodeProcessReadMsgFp[TSDB_MSG_TYPE_QUERY]    = dnodeProcessQueryMsg;
  dnodeProcessReadMsgFp[TSDB_MSG_TYPE_RETRIEVE] = dnodeProcessRetrieveMsg;

  readQset = taosOpenQset();

  minThreads = 3;
  maxThreads = tsNumOfCores*tsNumOfThreadsPerCore;
  if (maxThreads <= minThreads*2) maxThreads = 2*minThreads;

  return 0;
}

void dnodeCleanupRead() {
  taosCloseQset(readQset);
}

void dnodeRead(void *rpcMsg) {
  SRpcMsg *pMsg = rpcMsg;

  int32_t     leftLen      = pMsg->contLen;
  char        *pCont       = (char *) pMsg->pCont;
  int32_t     contLen      = 0;
  int32_t     numOfVnodes  = 0;
  int32_t     vgId         = 0;
  SRpcContext *pRpcContext = NULL;

  // parse head, get number of vnodes;
  if ( numOfVnodes > 1) {
    pRpcContext = calloc(sizeof(SRpcContext), 1);
    pRpcContext->numOfVnodes = 1;
  }

  while (leftLen > 0) {
    // todo: parse head, get vgId, contLen

    // get pVnode from vgId
    void *pVnode = dnodeGetVnode(vgId);
    if (pVnode == NULL) {
      continue;
    }

    // put message into queue
    SReadMsg *pReadMsg = taosAllocateQitem(sizeof(SReadMsg));
    pReadMsg->rpcMsg      = *pMsg;
    pReadMsg->pCont       = pCont;
    pReadMsg->contLen     = contLen;
    pReadMsg->pRpcContext = pRpcContext;

    taos_queue queue = dnodeGetVnodeRworker(pVnode);
    taosWriteQitem(queue, 0, pReadMsg);

    // next vnode
    leftLen -= contLen;
    pCont -= contLen;

    dnodeReleaseVnode(pVnode);
  }
}

void *dnodeAllocateReadWorker(void *pVnode) {
  taos_queue *queue = taosOpenQueue(sizeof(SReadMsg));
  if (queue == NULL) return NULL;

  taosAddIntoQset(readQset, queue, pVnode);

  // spawn a thread to process queue
  if (threads < maxThreads) {
    pthread_t thread;
    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&thread, &thAttr, dnodeProcessReadQueue, readQset) != 0) {
      dError("failed to create thread to process read queue, reason:%s", strerror(errno));
    }
  }

  return queue;
}

void dnodeFreeReadWorker(void *rqueue) {
  taosCloseQueue(rqueue);

  // dynamically adjust the number of threads
}

static void *dnodeProcessReadQueue(void *param) {
  taos_qset  qset = (taos_qset)param;
  SReadMsg  *pReadMsg;
  int        type;
  void      *pVnode;

  while (1) {
    if (taosReadQitemFromQset(qset, &type, &pReadMsg, &pVnode) == 0) {
      dnodeHandleIdleReadWorker();
      continue;
    }

    terrno = 0;
    if (dnodeProcessReadMsgFp[pReadMsg->rpcMsg.msgType]) {
      (*dnodeProcessReadMsgFp[pReadMsg->rpcMsg.msgType]) (pVnode, pReadMsg);
    } else {
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    }

    dnodeProcessReadResult(pVnode, pReadMsg);
    taosFreeQitem(pReadMsg);
  }

  return NULL;
}

static void dnodeHandleIdleReadWorker() {
  int32_t num = taosGetQueueNumber(readQset);

  if (num == 0 || (num <= minThreads && threads > minThreads)) {
    threads--;
    pthread_exit(NULL);
  } else {
    usleep(100);
    sched_yield();
  }
}

static void dnodeProcessReadResult(void *pVnode, SReadMsg *pRead) {
  SRpcContext *pRpcContext = pRead->pRpcContext;
  int32_t      code = 0;

  dnodeReleaseVnode(pVnode);

  if (pRpcContext) {
    if (terrno) {
      if (pRpcContext->code == 0) pRpcContext->code = terrno;
    }

    int32_t count = atomic_add_fetch_32(&pRpcContext->count, 1);
    if (count < pRpcContext->numOfVnodes) {
      // not over yet, multiple vnodes
      return;
    }

    // over, result can be merged now
    code = pRpcContext->code;
  } else {
    code = terrno;
  }

  SRpcMsg rsp;
  rsp.handle = pRead->rpcMsg.handle;
  rsp.code   = code;
  rsp.pCont  = NULL;
  rpcSendResponse(&rsp);
  rpcFreeCont(pRead->rpcMsg.pCont);  // free the received message
}

static void dnodeProcessQueryMsg(void *pVnode, SReadMsg *pMsg) {

}

static void dnodeProcessRetrieveMsg(void *pVnode, SReadMsg *pMsg) {

}
