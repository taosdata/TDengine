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
  uv_write_t*  writeReq;
  void*        data;
  queue        conn;
  char         spi;
  char         secured;
} SCliConn;
typedef struct SCliMsg {
  SRpcReqContext* context;
  queue           q;
  uint64_t        st;
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

// conn pool
static SCliConn* getConnFromCache(void* cache, char* ip, uint32_t port);
static void      addConnToCache(void* cache, char* ip, uint32_t port, SCliConn* conn);

static void clientAllocrReadBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
static void clientReadCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf);
static void clientWriteCb(uv_write_t* req, int status);
static void clientConnCb(uv_connect_t* req, int status);
static void clientAsyncCb(uv_async_t* handle);
static void clientDestroy(uv_handle_t* handle);
static void clientConnDestroy(SCliConn* pConn);

static void* clientThread(void* arg);

static void clientHandleReq(SCliMsg* pMsg, SCliThrdObj* pThrd);

static void clientAllocrReadBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  // impl later
}
static void clientReadCb(uv_stream_t* handle, ssize_t nread, const uv_buf_t* buf) {
  // impl later
  SCliConn* conn = handle->data;
  if (nread > 0) {
    return;
  }
  //
  uv_close((uv_handle_t*)handle, clientDestroy);
}

static void clientConnDestroy(SCliConn* conn) {
  // impl later
  //
}
static void clientDestroy(uv_handle_t* handle) {
  SCliConn* conn = handle->data;
  clientConnDestroy(conn);
}

static void clientWriteCb(uv_write_t* req, int status) {
  SCliConn* pConn = req->data;
  if (status == 0) {
    tDebug("data already was written on stream");
  } else {
    uv_close((uv_handle_t*)pConn->stream, clientDestroy);
    return;
  }

  uv_read_start((uv_stream_t*)pConn->stream, clientAllocrReadBufferCb, clientReadCb);
  // impl later
}

static void clientWrite(SCliConn* pConn) {
  SCliMsg*  pMsg = pConn->data;
  SRpcHead* pHead = rpcHeadFromCont(pMsg->context->pCont);
  int       msgLen = rpcMsgLenFromCont(pMsg->context->contLen);
  char*     msg = (char*)(pHead);

  uv_buf_t wb = uv_buf_init(msg, msgLen);
  uv_write(pConn->writeReq, (uv_stream_t*)pConn->stream, &wb, 1, clientWriteCb);
}
static void clientConnCb(uv_connect_t* req, int status) {
  // impl later
  SCliConn* pConn = req->data;
  if (status != 0) {
    tError("failed to connect %s", uv_err_name(status));
    clientConnDestroy(pConn);
    return;
  }

  SCliMsg* pMsg = pConn->data;
  SEpSet*  pEpSet = &pMsg->context->epSet;
  SRpcMsg  rpcMsg;
  // rpcMsg.ahandle = pMsg->context->ahandle;
  // rpcMsg.pCont = NULL;

  char*    fqdn = pEpSet->fqdn[pEpSet->inUse];
  uint32_t port = pEpSet->port[pEpSet->inUse];
  if (status != 0) {
    // call user fp later
    tError("failed to connect server(%s, %d), errmsg: %s", fqdn, port, uv_strerror(status));
    SRpcInfo* pRpc = pMsg->context->pRpc;
    (pRpc->cfp)(NULL, &rpcMsg, pEpSet);
    uv_close((uv_handle_t*)req->handle, clientDestroy);
    return;
  }
  assert(pConn->stream == req->handle);
}

static SCliConn* getConnFromCache(void* cache, char* ip, uint32_t port) {
  // impl later

  return NULL;
}
static void addConnToCache(void* cache, char* ip, uint32_t port, SCliConn* conn) {
  // impl later
}

static void clientHandleReq(SCliMsg* pMsg, SCliThrdObj* pThrd) {
  SEpSet* pEpSet = &pMsg->context->epSet;

  char*    fqdn = pEpSet->fqdn[pEpSet->inUse];
  uint32_t port = pEpSet->port[pEpSet->inUse];

  uint64_t el = taosGetTimestampUs() - pMsg->st;
  tDebug("msg tran time cost: %" PRIu64 "", el);

  SCliConn* conn = getConnFromCache(pThrd->cache, fqdn, port);
  if (conn != NULL) {
    // impl later
    conn->data = pMsg;
    conn->writeReq->data = conn;
    clientWrite(conn);
    // uv_buf_t wb;
    // uv_write(conn->writeReq, (uv_stream_t*)conn->stream, &wb, 1, clientWriteCb);
  } else {
    SCliConn* conn = malloc(sizeof(SCliConn));

    conn->stream = (uv_stream_t*)malloc(sizeof(uv_tcp_t));
    uv_tcp_init(pThrd->loop, (uv_tcp_t*)(conn->stream));
    conn->writeReq = malloc(sizeof(uv_write_t));

    conn->connReq.data = conn;
    conn->data = pMsg;
    struct sockaddr_in addr;
    uv_ip4_addr(fqdn, port, &addr);
    // handle error in callback if fail to connect
    uv_tcp_connect(&conn->connReq, (uv_tcp_t*)(conn->stream), (const struct sockaddr*)&addr, clientConnCb);

    // SRpcMsg   rpcMsg;
    // SEpSet*   pEpSet = &pMsg->context->epSet;
    // SRpcInfo* pRpc = pMsg->context->pRpc;
    //// rpcMsg.ahandle = pMsg->context->ahandle;
    // rpcMsg.pCont = NULL;
    // rpcMsg.ahandle = pMsg->context->ahandle;
    // uint64_t el1 = taosGetTimestampUs() - et;
    // tError("msg tran back first: time cost: %" PRIu64 "", el1);
    // et = taosGetTimestampUs();
    //(pRpc->cfp)(NULL, &rpcMsg, pEpSet);
    // uint64_t el2 = taosGetTimestampUs() - et;
    // tError("msg tran back second: time cost: %" PRIu64 "", el2);
  }
}
static void clientAsyncCb(uv_async_t* handle) {
  SCliThrdObj* pThrd = handle->data;
  SCliMsg*     pMsg = NULL;
  queue        wq;

  // batch process to avoid to lock/unlock frequently
  pthread_mutex_lock(&pThrd->msgMtx);
  QUEUE_MOVE(&pThrd->msg, &wq);
  pthread_mutex_unlock(&pThrd->msgMtx);

  int count = 0;
  while (!QUEUE_IS_EMPTY(&wq)) {
    queue* h = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(h);
    pMsg = QUEUE_DATA(h, SCliMsg, q);
    clientHandleReq(pMsg, pThrd);
    count++;
    if (count >= 2) {
      tError("send batch size: %d", count);
    }
  }
}

static void* clientThread(void* arg) {
  SCliThrdObj* pThrd = (SCliThrdObj*)arg;
  uv_run(pThrd->loop, UV_RUN_DEFAULT);
}

void* taosInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle) {
  SClientObj* cli = calloc(1, sizeof(SClientObj));
  memcpy(cli->label, label, strlen(label));
  cli->numOfThreads = numOfThreads;
  cli->pThreadObj = (SCliThrdObj**)calloc(cli->numOfThreads, sizeof(SCliThrdObj*));

  for (int i = 0; i < cli->numOfThreads; i++) {
    SCliThrdObj* pThrd = (SCliThrdObj*)calloc(1, sizeof(SCliThrdObj));
    QUEUE_INIT(&pThrd->msg);
    pthread_mutex_init(&pThrd->msgMtx, NULL);
    pThrd->loop = (uv_loop_t*)malloc(sizeof(uv_loop_t));
    uv_loop_init(pThrd->loop);

    pThrd->cliAsync = malloc(sizeof(uv_async_t));
    uv_async_init(pThrd->loop, pThrd->cliAsync, clientAsyncCb);
    pThrd->cliAsync->data = pThrd;

    pThrd->shandle = shandle;
    int err = pthread_create(&pThrd->thread, NULL, clientThread, (void*)(pThrd));
    if (err == 0) {
      tDebug("sucess to create tranport-client thread %d", i);
    }
    cli->pThreadObj[i] = pThrd;
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
  msg->st = taosGetTimestampUs();

  SCliThrdObj* thrd = ((SClientObj*)pRpc->tcphandle)->pThreadObj[index % pRpc->numOfThreads];

  pthread_mutex_lock(&thrd->msgMtx);
  QUEUE_PUSH(&thrd->msg, &msg->q);
  pthread_mutex_unlock(&thrd->msgMtx);

  uv_async_send(thrd->cliAsync);
}
#endif
