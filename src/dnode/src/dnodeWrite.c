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
#include "tlog.h"
#include "tqueue.h"
#include "trpc.h"
#include "dnodeWrite.h"
#include "dnodeMgmt.h"

typedef struct {
  int32_t  code;
  int32_t  count;       // number of vnodes returned result
  int32_t  numOfVnodes; // number of vnodes involved
} SRpcContext;

typedef struct _write {
  void        *pCont;
  int32_t      contLen;
  SRpcMsg      rpcMsg;
  void        *pVnode;      // pointer to vnode
  SRpcContext *pRpcContext; // RPC message context
} SWriteMsg;

typedef struct {
  taos_qset  qset;      // queue set
  pthread_t  thread;    // thread 
  int32_t    workerId;  // worker ID
} SWriteWorker;  

typedef struct _thread_obj {
  int32_t        max;        // max number of workers
  int32_t        nextId;     // from 0 to max-1, cyclic
  SWriteWorker  *writeWorker;
} SWriteWorkerPool;

static void (*dnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MAX])(SWriteMsg *);
static void  *dnodeProcessWriteQueue(void *param);
static void   dnodeHandleIdleWorker(SWriteWorker *pWorker);
static void   dnodeProcessWriteResult(SWriteMsg *pWrite);
static void   dnodeProcessSubmitMsg(SWriteMsg *pMsg);
static void   dnodeProcessCreateTableMsg(SWriteMsg *pMsg);
static void   dnodeProcessDropTableMsg(SWriteMsg *pMsg);
static void   dnodeProcessAlterTableMsg(SWriteMsg *pMsg);
static void   dnodeProcessDropStableMsg(SWriteMsg *pMsg);

SWriteWorkerPool wWorkerPool;

int32_t dnodeInitWrite() {
  dnodeProcessWriteMsgFp[TSDB_MSG_TYPE_SUBMIT]          = dnodeProcessSubmitMsg;
  dnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE] = dnodeProcessCreateTableMsg;
  dnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MD_DROP_TABLE]   = dnodeProcessDropTableMsg;
  dnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE]  = dnodeProcessAlterTableMsg;
  dnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MD_DROP_STABLE]  = dnodeProcessDropStableMsg;

  wWorkerPool.max = tsNumOfCores;
  wWorkerPool.writeWorker = (SWriteWorker *)calloc(sizeof(SWriteWorker), wWorkerPool.max);
  if (wWorkerPool.writeWorker == NULL) return -1;

  for (int32_t i = 0; i < wWorkerPool.max; ++i) {
    wWorkerPool.writeWorker[i].workerId = i;
  }

  dPrint("dnode write is opened");
  return 0;
}

void dnodeCleanupWrite() {
  free(wWorkerPool.writeWorker);
  dPrint("dnode write is closed");
}

void dnodeWrite(SRpcMsg *pMsg) {
  int32_t     queuedMsgNum = 0;
  int32_t     leftLen      = pMsg->contLen;
  char        *pCont       = (char *) pMsg->pCont;
  SRpcContext *pRpcContext = NULL;

  int32_t numOfVnodes = 0;
  if (pMsg->msgType == TSDB_MSG_TYPE_SUBMIT) {
    // TODO parse head, get number of vnodes;
    numOfVnodes = 1;
  } else {
    numOfVnodes = 1;
  }

  if (numOfVnodes > 1) {
    pRpcContext = calloc(sizeof(SRpcContext), 1);
    pRpcContext->numOfVnodes = numOfVnodes;
  }

  while (leftLen > 0) {
    SWriteMsgHead *pHead = (SWriteMsgHead *) pCont;
    int32_t vgId    = htonl(pHead->vgId);
    int32_t contLen = htonl(pHead->contLen);

    void *pVnode = dnodeGetVnode(vgId);
    if (pVnode == NULL) {
      leftLen -= contLen;
      pCont -= contLen;
      continue;
    }
   
    // put message into queue
    SWriteMsg writeMsg;
    writeMsg.rpcMsg      = *pMsg;
    writeMsg.pCont       = pCont;
    writeMsg.contLen     = contLen;
    writeMsg.pRpcContext = pRpcContext;
    writeMsg.pVnode      = pVnode;  // pVnode shall be saved for usage later
 
    taos_queue queue = dnodeGetVnodeWworker(pVnode);
    taosWriteQitem(queue, &writeMsg);

    // next vnode 
    leftLen -= contLen;
    pCont -= contLen;
    queuedMsgNum++;
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

void *dnodeAllocateWriteWorker() {
  SWriteWorker *pWorker = wWorkerPool.writeWorker + wWorkerPool.nextId;
  taos_queue *queue = taosOpenQueue(sizeof(SWriteMsg));
  if (queue != NULL) return queue;

  if (pWorker->qset == NULL) {
    pWorker->qset = taosOpenQset();
    if (pWorker->qset == NULL) return NULL;

    taosAddIntoQset(pWorker->qset, queue);
    wWorkerPool.nextId = (wWorkerPool.nextId + 1) % wWorkerPool.max;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, dnodeProcessWriteQueue, pWorker) != 0) {
      dError("failed to create thread to process read queue, reason:%s", strerror(errno));
      taosCloseQset(pWorker->qset);
    }
  } else {
    taosAddIntoQset(pWorker->qset, queue);
    wWorkerPool.nextId = (wWorkerPool.nextId + 1) % wWorkerPool.max;
  }

  return queue;
}

void dnodeFreeWriteWorker(void *wqueue) {
  taosCloseQueue(wqueue);

  // dynamically adjust the number of threads
}

static void *dnodeProcessWriteQueue(void *param) {
  SWriteWorker *pWorker = (SWriteWorker *)param;
  taos_qall     qall;
  SWriteMsg     writeMsg;
  int32_t       numOfMsgs;

  while (1) {
    numOfMsgs = taosReadAllQitemsFromQset(pWorker->qset, &qall);
    if (numOfMsgs <=0) { 
      dnodeHandleIdleWorker(pWorker);  // thread exit if no queues anymore
      continue;
    }

    for (int32_t i=0; i<numOfMsgs; ++i) {
      // retrieve all items, and write them into WAL
      taosGetQitem(qall, &writeMsg);

      // walWrite(pVnode->whandle, writeMsg.rpcMsg.msgType, writeMsg.pCont, writeMsg.contLen);
    }
    
    // flush WAL file
    // walFsync(pVnode->whandle);

    // browse all items, and process them one by one
    taosResetQitems(qall);
    for (int32_t i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(qall, &writeMsg);

      terrno = 0;
      if (dnodeProcessWriteMsgFp[writeMsg.rpcMsg.msgType]) {
        (*dnodeProcessWriteMsgFp[writeMsg.rpcMsg.msgType]) (&writeMsg);
      } else {
        terrno = TSDB_CODE_MSG_NOT_PROCESSED;  
      }
     
      dnodeProcessWriteResult(&writeMsg);
    }

    // free the Qitems;
    taosFreeQitems(qall);
  }

  return NULL;
}

static void dnodeProcessWriteResult(SWriteMsg *pWrite) {
  SRpcContext *pRpcContext = pWrite->pRpcContext;
  int32_t      code = 0;

  dnodeReleaseVnode(pWrite->pVnode);

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
  rsp.handle = pWrite->rpcMsg.handle;
  rsp.code   = code;
  rsp.pCont  = NULL;
  rpcSendResponse(&rsp);
  rpcFreeCont(pWrite->rpcMsg.pCont);  // free the received message
}

static void dnodeHandleIdleWorker(SWriteWorker *pWorker) {
  int32_t num = taosGetQueueNumber(pWorker->qset);

  if (num > 0) {
     usleep(100);
     sched_yield(); 
  } else {
     taosCloseQset(pWorker->qset);
     pWorker->qset = NULL;
     pthread_exit(NULL);
  }
}

static void dnodeProcessSubmitMsg(SWriteMsg *pMsg) {
  dTrace("submit msg is disposed");

  SShellSubmitRspMsg *pRsp = rpcMallocCont(sizeof(SShellSubmitRspMsg));
  pRsp->code              = 0;
  pRsp->numOfRows         = htonl(1);
  pRsp->affectedRows      = htonl(1);
  pRsp->numOfFailedBlocks = 0;

  SRpcMsg rpcRsp = {
    .handle = pMsg->rpcMsg.handle,
    .pCont = pRsp,
    .contLen = sizeof(SShellSubmitRspMsg),
    .code = 0,
    .msgType = 0
  };
  rpcSendResponse(&rpcRsp);
}

static void dnodeProcessCreateTableMsg(SWriteMsg *pMsg) {
  SMDCreateTableMsg *pTable = pMsg->rpcMsg.pCont;
  if (pTable->tableType == TSDB_TABLE_TYPE_CHILD_TABLE) {
    dTrace("table:%s, start to create child table, stable:%s", pTable->tableId, pTable->superTableId);
  } else if (pTable->tableType == TSDB_TABLE_TYPE_NORMAL_TABLE){
    dTrace("table:%s, start to create normal table", pTable->tableId);
  } else if (pTable->tableType == TSDB_TABLE_TYPE_STREAM_TABLE){
    dTrace("table:%s, start to create stream table", pTable->tableId);
  } else {
    dError("table:%s, invalid table type:%d", pTable->tableType);
  }

//  pTable->numOfColumns  = htons(pTable->numOfColumns);
//  pTable->numOfTags     = htons(pTable->numOfTags);
//  pTable->sid           = htonl(pTable->sid);
//  pTable->sversion      = htonl(pTable->sversion);
//  pTable->tagDataLen    = htonl(pTable->tagDataLen);
//  pTable->sqlDataLen    = htonl(pTable->sqlDataLen);
//  pTable->contLen       = htonl(pTable->contLen);
//  pTable->numOfVPeers   = htonl(pTable->numOfVPeers);
//  pTable->uid           = htobe64(pTable->uid);
//  pTable->superTableUid = htobe64(pTable->superTableUid);
//  pTable->createdTime   = htobe64(pTable->createdTime);
//
//  for (int i = 0; i < pTable->numOfVPeers; ++i) {
//    pTable->vpeerDesc[i].ip    = htonl(pTable->vpeerDesc[i].ip);
//    pTable->vpeerDesc[i].vnode = htonl(pTable->vpeerDesc[i].vnode);
//  }
//
//  int32_t totalCols = pTable->numOfColumns + pTable->numOfTags;
//  SSchema *pSchema = (SSchema *) pTable->data;
//  for (int32_t col = 0; col < totalCols; ++col) {
//    pSchema->bytes = htons(pSchema->bytes);
//    pSchema->colId = htons(pSchema->colId);
//    pSchema++;
//  }
//
//  int32_t code = dnodeCreateTable(pTable);

  SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  rpcSendResponse(&rpcRsp);
}

static void dnodeProcessDropTableMsg(SWriteMsg *pMsg) {
  SMDDropTableMsg *pTable = pMsg->rpcMsg.pCont;
  dPrint("table:%s, sid:%d is dropped", pTable->tableId, pTable->sid);

//  pTable->sid         = htonl(pTable->sid);
//  pTable->numOfVPeers = htonl(pTable->numOfVPeers);
//  pTable->uid         = htobe64(pTable->uid);
//
//  for (int i = 0; i < pTable->numOfVPeers; ++i) {
//    pTable->vpeerDesc[i].ip    = htonl(pTable->vpeerDesc[i].ip);
//    pTable->vpeerDesc[i].vnode = htonl(pTable->vpeerDesc[i].vnode);
//  }
//
//  int32_t code = dnodeDropTable(pTable);
//
  SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  rpcSendResponse(&rpcRsp);
}

static void dnodeProcessAlterTableMsg(SWriteMsg *pMsg) {
  SMDCreateTableMsg *pTable = pMsg->rpcMsg.pCont;
  dPrint("table:%s, sid:%d is alterd", pTable->tableId, pTable->sid);

  SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  rpcSendResponse(&rpcRsp);
}

static void dnodeProcessDropStableMsg(SWriteMsg *pMsg) {
  SMDDropSTableMsg *pTable = pMsg->rpcMsg.pCont;
  dPrint("stable:%s, is dropped", pTable->tableId);

  SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  rpcSendResponse(&rpcRsp);
}
