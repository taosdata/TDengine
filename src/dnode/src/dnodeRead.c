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

#include "dnodeMgmt.h"
#include "dnodeRead.h"
#include "queryExecutor.h"

typedef struct {
  int32_t  code;
  int32_t  count;
  int32_t  numOfVnodes;
} SRpcContext;

typedef struct {
  void        *pCont;
  int32_t      contLen;
  SRpcMsg      rpcMsg;
  void        *pVnode;
  SRpcContext *pRpcContext;  // RPC message context
} SReadMsg;

static void *dnodeProcessReadQueue(void *param);
static void  dnodeProcessReadResult(SReadMsg *pRead);
static void  dnodeHandleIdleReadWorker();
static void  dnodeProcessQueryMsg(SReadMsg *pMsg);
static void  dnodeProcessRetrieveMsg(SReadMsg *pMsg);
static void(*dnodeProcessReadMsgFp[TSDB_MSG_TYPE_MAX])(SReadMsg *pNode);

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

  dPrint("dnode read is opened");
  return 0;
}

void dnodeCleanupRead() {
  taosCloseQset(readQset);
  dPrint("dnode read is closed");
}

void dnodeRead(SRpcMsg *pMsg) {
  int32_t     queuedMsgNum = 0;
  int32_t     leftLen      = pMsg->contLen;
  char        *pCont       = (char *) pMsg->pCont;
  SRpcContext *pRpcContext = NULL;
  
  dTrace("dnode read msg disposal");
  
//  SMsgDesc *pDesc = pCont;
//  pDesc->numOfVnodes = htonl(pDesc->numOfVnodes);
//  pCont += sizeof(SMsgDesc);
//  if (pDesc->numOfVnodes > 1) {
//    pRpcContext = calloc(sizeof(SRpcContext), 1);
//    pRpcContext->numOfVnodes = pDesc->numOfVnodes;
//  }
  if (pMsg->msgType == TSDB_MSG_TYPE_RETRIEVE) {
    queuedMsgNum = 0;
  }

  while (leftLen > 0) {
    SMsgHead *pHead = (SMsgHead *) pCont;
    pHead->vgId    = 1;//htonl(pHead->vgId);
    pHead->contLen = pMsg->contLen; //htonl(pHead->contLen);

    void *pVnode = dnodeGetVnode(pHead->vgId);
    if (pVnode == NULL) {
      leftLen -= pHead->contLen;
      pCont -= pHead->contLen;
      continue;
    }

    // put message into queue
    SReadMsg readMsg;
    readMsg.rpcMsg      = *pMsg;
    readMsg.pCont       = pCont;
    readMsg.contLen     = pHead->contLen;
    readMsg.pRpcContext = pRpcContext;
    readMsg.pVnode      = pVnode;

    taos_queue queue = dnodeGetVnodeRworker(pVnode);
    taosWriteQitem(queue, &readMsg);

    // next vnode
    leftLen -= pHead->contLen;
    pCont -= pHead->contLen;
    queuedMsgNum++;

    dnodeReleaseVnode(pVnode);
  }

  if (queuedMsgNum == 0) {
    SRpcMsg rpcRsp = {
        .handle  = pMsg->handle,
        .pCont   = NULL,
        .contLen = 0,
        .code    = TSDB_CODE_INVALID_VGROUP_ID,
        .msgType = 0
    };
    rpcSendResponse(&rpcRsp);
  }
}

void *dnodeAllocateReadWorker() {
  taos_queue *queue = taosOpenQueue(sizeof(SReadMsg));
  if (queue == NULL) return NULL;

  taosAddIntoQset(readQset, queue);

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
  SReadMsg   readMsg;

  while (1) {
    if (taosReadQitemFromQset(qset, &readMsg) <= 0) {
      dnodeHandleIdleReadWorker();
      continue;
    }

    terrno = 0;
    if (dnodeProcessReadMsgFp[readMsg.rpcMsg.msgType]) {
      (*dnodeProcessReadMsgFp[readMsg.rpcMsg.msgType]) (&readMsg);
    } else {
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    }

    dnodeProcessReadResult(&readMsg);
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

static void dnodeProcessReadResult(SReadMsg *pRead) {
  SRpcContext *pRpcContext = pRead->pRpcContext;
  int32_t      code = 0;

  dnodeReleaseVnode(pRead->pVnode);

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

static void dnodeProcessQueryMsg(SReadMsg *pMsg) {
  SQueryTableMsg* pQueryTableMsg = (SQueryTableMsg*) pMsg->pCont;
  
  SQInfo* pQInfo = NULL;
  void* tsdb = dnodeGetVnodeTsdb(pMsg->pVnode);
  int32_t code = qCreateQueryInfo(tsdb, pQueryTableMsg, &pQInfo);
  
  SQueryTableRsp *pRsp = (SQueryTableRsp *) rpcMallocCont(sizeof(SQueryTableRsp));
  pRsp->code    = code;
  pRsp->qhandle = htobe64((uint64_t) (pQInfo));

  SRpcMsg rpcRsp = {
      .handle = pMsg->rpcMsg.handle,
      .pCont = pRsp,
      .contLen = sizeof(SQueryTableRsp),
      .code = code,
      .msgType = 0
  };
  
  rpcSendResponse(&rpcRsp);
  
  // do execute query
  qTableQuery(pQInfo);
}

static void dnodeProcessRetrieveMsg(SReadMsg *pMsg) {
  SRetrieveTableMsg *pRetrieve = pMsg->pCont;
  void *pQInfo = htobe64(pRetrieve->qhandle);

  dTrace("QInfo:%p vgId:%d, retrieve msg is received", pQInfo, pRetrieve->header.vgId);
  
  int32_t rowSize = 0;
  int32_t numOfRows = 0;
  int32_t contLen = 0;
  
  SRpcMsg rpcRsp = {0};
  
  int32_t code = qRetrieveQueryResultInfo(pQInfo, &numOfRows, &rowSize);
  if (code != TSDB_CODE_SUCCESS) {
    contLen = sizeof(SRetrieveTableRsp);
    
    SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)rpcMallocCont(contLen);
    pRsp->numOfRows = 0;
    pRsp->precision = 0;
    pRsp->offset = 0;
    pRsp->useconds = 0;
  
    rpcRsp = (SRpcMsg) {
        .handle = pMsg->rpcMsg.handle,
        .pCont = pRsp,
        .contLen = contLen,
        .code = code,
        .msgType = 0
    };
    
    //todo free qinfo
  } else {
    SRetrieveTableRsp* pRsp = NULL;
    
    int32_t code = qDumpRetrieveResult(pQInfo, &pRsp, &contLen);
    //todo check code
    
    rpcRsp = (SRpcMsg) {
        .handle = pMsg->rpcMsg.handle,
        .pCont = pRsp,
        .contLen = contLen,
        .code = code,
        .msgType = 0
    };
  }
  
  rpcSendResponse(&rpcRsp);
}
