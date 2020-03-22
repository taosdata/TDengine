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
#include "tsdb.h"
#include "dataformat.h"
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

static void (*dnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MAX])(void *, SWriteMsg *);
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

  if (pMsg->msgType == TSDB_MSG_TYPE_SUBMIT || pMsg->msgType == TSDB_MSG_TYPE_MD_DROP_STABLE) {
    SMsgDesc *pDesc = (SMsgDesc *)pCont;
    pDesc->numOfVnodes = htonl(pDesc->numOfVnodes);
    pCont += sizeof(SMsgDesc);
    if (pDesc->numOfVnodes > 1) {
      pRpcContext = calloc(sizeof(SRpcContext), 1);
      pRpcContext->numOfVnodes = pDesc->numOfVnodes;
    }
  }

  while (leftLen > 0) {
    SMsgHead *pHead = (SMsgHead *) pCont;
    pHead->vgId    = htonl(pHead->vgId);
    pHead->contLen = htonl(pHead->contLen);

    void *pVnode = dnodeGetVnode(pHead->vgId);
    if (pVnode == NULL) {
      leftLen -= pHead->contLen;
      pCont -= pHead->contLen;
      continue;
    }
   
    // put message into queue
    SWriteMsg writeMsg;
    writeMsg.rpcMsg      = *pMsg;
    writeMsg.pCont       = pCont;
    writeMsg.contLen     = pHead->contLen;
    writeMsg.pRpcContext = pRpcContext;
    writeMsg.pVnode      = pVnode;  // pVnode shall be saved for usage later
 
    taos_queue queue = dnodeGetVnodeWworker(pVnode);
    taosWriteQitem(queue, &writeMsg);

    // next vnode 
    leftLen -= pHead->contLen;
    pCont -= pHead->contLen;
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

void *dnodeAllocateWriteWorker(void *pVnode) {
  SWriteWorker *pWorker = wWorkerPool.writeWorker + wWorkerPool.nextId;
  taos_queue *queue = taosOpenQueue(sizeof(SWriteMsg));
  if (queue == NULL) return NULL;

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
  SWriteMsg    *pWriteMsg;
  int32_t       numOfMsgs;
  int           type;
  void         *pVnode;

  while (1) {
    numOfMsgs = taosReadAllQitemsFromQset(pWorker->qset, &qall, &pVnode);
    if (numOfMsgs <=0) { 
      dnodeHandleIdleWorker(pWorker);  // thread exit if no queues anymore
      continue;
    }

    for (int32_t i=0; i<numOfMsgs; ++i) {
      // retrieve all items, and write them into WAL
      taosGetQitem(qall, &type, &pWriteMsg);

      // walWrite(pVnode->whandle, writeMsg.rpcMsg.msgType, writeMsg.pCont, writeMsg.contLen);
    }
    
    // flush WAL file
    // walFsync(pVnode->whandle);

    // browse all items, and process them one by one
    taosResetQitems(qall);
    for (int32_t i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(qall, &type, &pWriteMsg);

      terrno = 0;
      if (dnodeProcessWriteMsgFp[pWriteMsg->rpcMsg.msgType]) {
        (*dnodeProcessWriteMsgFp[pWriteMsg->rpcMsg.msgType]) (pVnode, pWriteMsg);
      } else {
        terrno = TSDB_CODE_MSG_NOT_PROCESSED;  
      }
     
      dnodeProcessWriteResult(pVnode, pWriteMsg);
    }

    // free the Qitems;
    taosFreeQitems(qall);
  }

  return NULL;
}

static void dnodeProcessWriteResult(void *pVnode, SWriteMsg *pWrite) {
  SRpcContext *pRpcContext = pWrite->pRpcContext;
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
  
  // todo write to tsdb
  
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
  SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  dTrace("table:%s, start to create in dnode, vgroup:%d", pTable->tableId, pTable->vgId);
  pTable->numOfColumns  = htons(pTable->numOfColumns);
  pTable->numOfTags     = htons(pTable->numOfTags);
  pTable->sid           = htonl(pTable->sid);
  pTable->sversion      = htonl(pTable->sversion);
  pTable->tagDataLen    = htonl(pTable->tagDataLen);
  pTable->sqlDataLen    = htonl(pTable->sqlDataLen);
  pTable->uid           = htobe64(pTable->uid);
  pTable->superTableUid = htobe64(pTable->superTableUid);
  pTable->createdTime   = htobe64(pTable->createdTime);
  SSchema *pSchema = (SSchema *) pTable->data;

  int totalCols = pTable->numOfColumns + pTable->numOfTags;
  for (int i = 0; i < totalCols; i++) {
    pSchema[i].colId = htons(pSchema[i].colId);
    pSchema[i].bytes = htons(pSchema[i].bytes);
  }

  STableCfg tCfg;
  tsdbInitTableCfg(&tCfg, pTable->tableType, pTable->uid, pTable->sid);

  STSchema *pDestSchema = tdNewSchema(pTable->numOfColumns);
  for (int i = 0; i < pTable->numOfColumns; i++) {
    tdSchemaAppendCol(pDestSchema, pSchema[i].type, pSchema[i].colId, pSchema[i].bytes);
  }
  tsdbTableSetSchema(&tCfg, pDestSchema, false);

  if (pTable->numOfTags != 0) {
    STSchema *pDestTagSchema = tdNewSchema(pTable->numOfTags);
    for (int i = pTable->numOfColumns; i < totalCols; i++) {
      tdSchemaAppendCol(pDestTagSchema, pSchema[i].type, pSchema[i].colId, pSchema[i].bytes);
    }
    tsdbTableSetTagSchema(&tCfg, pDestTagSchema, false);

    char *pTagData = pTable->data + totalCols * sizeof(SSchema);
    int accumBytes = 0;
    SDataRow dataRow = tdNewDataRowFromSchema(pDestTagSchema);

    for (int i = 0; i < pTable->numOfTags; i++) {
      tdAppendColVal(dataRow, pTagData + accumBytes, pDestTagSchema->columns + i);
      accumBytes += pSchema[i + pTable->numOfColumns].bytes;
    }
    tsdbTableSetTagValue(&tCfg, dataRow, false);
  }

  void *pTsdb = dnodeGetVnodeTsdb(pMsg->pVnode);

  rpcRsp.code = tsdbCreateTable(pTsdb, &tCfg);
  dnodeReleaseVnode(pMsg->pVnode);

  dTrace("table:%s, create table result:%s", pTable->tableId, tstrerror(rpcRsp.code));
  rpcSendResponse(&rpcRsp);
}

static void dnodeProcessDropTableMsg(SWriteMsg *pMsg) {
  SMDDropTableMsg *pTable = pMsg->rpcMsg.pCont;
  SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  dTrace("table:%s, start to drop in dnode, vgroup:%d", pTable->tableId, pTable->vgId);
  STableId tableId = {
    .uid = htobe64(pTable->uid),
    .tid = htonl(pTable->sid)
  };

  void *pTsdb = dnodeGetVnodeTsdb(pMsg->pVnode);

  rpcRsp.code = tsdbDropTable(pTsdb, tableId);
  dnodeReleaseVnode(pMsg->pVnode);

  dTrace("table:%s, drop table result:%s", pTable->tableId, tstrerror(rpcRsp.code));
  rpcSendResponse(&rpcRsp);
}

static void dnodeProcessAlterTableMsg(SWriteMsg *pMsg) {
  SMDCreateTableMsg *pTable = pMsg->rpcMsg.pCont;
  SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  dTrace("table:%s, start to alter in dnode, vgroup:%d", pTable->tableId, pTable->vgId);
  pTable->numOfColumns  = htons(pTable->numOfColumns);
  pTable->numOfTags     = htons(pTable->numOfTags);
  pTable->sid           = htonl(pTable->sid);
  pTable->sversion      = htonl(pTable->sversion);
  pTable->tagDataLen    = htonl(pTable->tagDataLen);
  pTable->sqlDataLen    = htonl(pTable->sqlDataLen);
  pTable->uid           = htobe64(pTable->uid);
  pTable->superTableUid = htobe64(pTable->superTableUid);
  pTable->createdTime   = htobe64(pTable->createdTime);
  SSchema *pSchema = (SSchema *) pTable->data;

  int totalCols = pTable->numOfColumns + pTable->numOfTags;
  for (int i = 0; i < totalCols; i++) {
    pSchema[i].colId = htons(pSchema[i].colId);
    pSchema[i].bytes = htons(pSchema[i].bytes);
  }

  STableCfg tCfg;
  tsdbInitTableCfg(&tCfg, pTable->tableType, pTable->uid, pTable->sid);

  STSchema *pDestSchema = tdNewSchema(pTable->numOfColumns);
  for (int i = 0; i < pTable->numOfColumns; i++) {
    tdSchemaAppendCol(pDestSchema, pSchema[i].type, pSchema[i].colId, pSchema[i].bytes);
  }
  tsdbTableSetSchema(&tCfg, pDestSchema, false);

  if (pTable->numOfTags != 0) {
    STSchema *pDestTagSchema = tdNewSchema(pTable->numOfTags);
    for (int i = pTable->numOfColumns; i < totalCols; i++) {
      tdSchemaAppendCol(pDestTagSchema, pSchema[i].type, pSchema[i].colId, pSchema[i].bytes);
    }
    tsdbTableSetSchema(&tCfg, pDestTagSchema, false);

    char *pTagData = pTable->data + totalCols * sizeof(SSchema);
    int accumBytes = 0;
    SDataRow dataRow = tdNewDataRowFromSchema(pDestTagSchema);

    for (int i = 0; i < pTable->numOfTags; i++) {
      tdAppendColVal(dataRow, pTagData + accumBytes, pDestTagSchema->columns + i);
      accumBytes += pSchema[i + pTable->numOfColumns].bytes;
    }
    tsdbTableSetTagValue(&tCfg, dataRow, false);
  }

  void *pTsdb = dnodeGetVnodeTsdb(pMsg->pVnode);

  rpcRsp.code = tsdbAlterTable(pTsdb, &tCfg);
  dnodeReleaseVnode(pMsg->pVnode);

  dTrace("table:%s, alter table result:%s", pTable->tableId, tstrerror(rpcRsp.code));
  rpcSendResponse(&rpcRsp);
}

static void dnodeProcessDropStableMsg(SWriteMsg *pMsg) {
  SMDDropSTableMsg *pTable = pMsg->rpcMsg.pCont;
  SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  dTrace("stable:%s, start to it drop in dnode, vgroup:%d", pTable->tableId, pTable->vgId);
  pTable->uid = htobe64(pTable->uid);

  // TODO: drop stable in vvnode
  //void *pTsdb = dnodeGetVnodeTsdb(pMsg->pVnode);
  //rpcRsp.code = tsdbDropSTable(pTsdb, pTable->uid);

  rpcRsp.code = TSDB_CODE_SUCCESS;
  dnodeReleaseVnode(pMsg->pVnode);

  dTrace("stable:%s, drop stable result:%s", pTable->tableId, tstrerror(rpcRsp.code));
  rpcSendResponse(&rpcRsp);
}

