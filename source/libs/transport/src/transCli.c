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

#ifdef USE_UV

#include "transComm.h"

typedef struct SCliConn {
  uv_connect_t connReq;
  uv_stream_t* stream;
  void*        data;
  queue        conn;
} SCliConn;
typedef struct SCliMsg {
  SRpcReqContext* context;
  queue           q;
} SCliMsg;

typedef struct SCliThrdObj {
  pthread_t       thread;
  uv_loop_t*      loop;
  uv_async_t*     cliAsync;  //
  void*           cache;     // conn pool
  queue           msg;
  pthread_mutex_t msgMtx;
  void*           shandle;
} SCliThrdObj;

typedef struct SClientObj {
  char          label[TSDB_LABEL_LEN];
  int32_t       index;
  int           numOfThreads;
  SCliThrdObj** pThreadObj;
} SClientObj;

static void clientWriteCb(uv_write_t* req, int status);
static void clientReadCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf);
static void clientConnCb(struct uv_connect_s* req, int status);
static void clientAsyncCb(uv_async_t* handle);

static void* clientThread(void* arg);

static void clientWriteCb(uv_write_t* req, int status) {
  // impl later
}
static void clientReadCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf) {
  // impl later
}
static void clientConnCb(struct uv_connect_s* req, int status) {
  // impl later
}

static SCliConn* getConnFromCache(void* cache, char* ip, uint32_t port) {
  // impl later
  return NULL;
}
static void clientAsyncCb(uv_async_t* handle) {
  SCliThrdObj* pThrd = handle->data;
  SCliMsg*     pMsg = NULL;
  pthread_mutex_lock(&pThrd->msgMtx);
  if (!QUEUE_IS_EMPTY(&pThrd->msg)) {
    queue* head = QUEUE_HEAD(&pThrd->msg);
    pMsg = QUEUE_DATA(head, SCliMsg, q);
    QUEUE_REMOVE(head);
  }
  pthread_mutex_unlock(&pThrd->msgMtx);

  SEpSet*  pEpSet = &pMsg->context->epSet;
  char*    fqdn = pEpSet->fqdn[pEpSet->inUse];
  uint32_t port = pEpSet->port[pEpSet->inUse];

  SCliConn* conn = getConnFromCache(pThrd->cache, fqdn, port);
  if (conn != NULL) {
  } else {
    SCliConn* conn = malloc(sizeof(SCliConn));

    conn->stream = (uv_stream_t*)malloc(sizeof(uv_tcp_t));
    uv_tcp_init(pThrd->loop, (uv_tcp_t*)(conn->stream));

    conn->connReq.data = conn;
    conn->data = pMsg;

    struct sockaddr_in addr;
    uv_ip4_addr(fqdn, port, &addr);
    // handle error in callback if connect error
    uv_tcp_connect(&conn->connReq, (uv_tcp_t*)(conn->stream), (const struct sockaddr*)&addr, clientConnCb);
  }

  // SRpcReqContext* pCxt = pMsg->context;

  // SRpcHead* pHead = rpcHeadFromCont(pCtx->pCont);
  // char*     msg = (char*)pHead;
  // int       len = rpcMsgLenFromCont(pCtx->contLen);
  // tmsg_t    msgType = pCtx->msgType;

  // impl later
}

static void* clientThread(void* arg) {
  SCliThrdObj* pThrd = (SCliThrdObj*)arg;

  QUEUE_INIT(&pThrd->msg);
  pthread_mutex_init(&pThrd->msgMtx, NULL);

  // QUEUE_INIT(&pThrd->clientCache);

  pThrd->loop = (uv_loop_t*)malloc(sizeof(uv_loop_t));
  uv_loop_init(pThrd->loop);

  pThrd->cliAsync = malloc(sizeof(uv_async_t));
  uv_async_init(pThrd->loop, pThrd->cliAsync, clientAsyncCb);
  pThrd->cliAsync->data = pThrd;

  uv_run(pThrd->loop, UV_RUN_DEFAULT);
}

void* taosInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle) {
  SClientObj* cli = calloc(1, sizeof(SClientObj));
  memcpy(cli->label, label, strlen(label));
  cli->numOfThreads = numOfThreads;
  cli->pThreadObj = (SCliThrdObj**)calloc(cli->numOfThreads, sizeof(SCliThrdObj*));

  for (int i = 0; i < cli->numOfThreads; i++) {
    SCliThrdObj* thrd = (SCliThrdObj*)calloc(1, sizeof(SCliThrdObj));

    thrd->shandle = shandle;
    int err = pthread_create(&thrd->thread, NULL, clientThread, (void*)(thrd));
    if (err == 0) {
      tDebug("sucess to create tranport-client thread %d", i);
    }
    cli->pThreadObj[i] = thrd;
  }
  return cli;
}

void rpcSendRequest(void* shandle, const SEpSet* pEpSet, SRpcMsg* pMsg, int64_t* pRid) {
  // impl later
  SRpcInfo* pRpc = (SRpcInfo*)shandle;

  int len = rpcCompressRpcMsg(pMsg->pCont, pMsg->contLen);

  SRpcReqContext* pContext;
  pContext = (SRpcReqContext*)((char*)pMsg->pCont - sizeof(SRpcHead) - sizeof(SRpcReqContext));
  pContext->ahandle = pMsg->ahandle;
  pContext->pRpc = (SRpcInfo*)shandle;
  pContext->epSet = *pEpSet;
  pContext->contLen = len;
  pContext->pCont = pMsg->pCont;
  pContext->msgType = pMsg->msgType;
  pContext->oldInUse = pEpSet->inUse;

  assert(pRpc->connType == TAOS_CONN_CLIENT);
  // atomic or not
  int64_t index = pRpc->index;
  if (pRpc->index++ >= pRpc->numOfThreads) {
    pRpc->index = 0;
  }
  SCliMsg* msg = malloc(sizeof(SCliMsg));
  msg->context = pContext;

  SCliThrdObj* thrd = ((SClientObj*)pRpc->tcphandle)->pThreadObj[index % pRpc->numOfThreads];

  pthread_mutex_lock(&thrd->msgMtx);
  QUEUE_PUSH(&thrd->msg, &msg->q);
  pthread_mutex_unlock(&thrd->msgMtx);

  uv_async_send(thrd->cliAsync);
}
#endif
