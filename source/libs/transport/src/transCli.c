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

#define CONN_PERSIST_TIME(para) (para * 1000 * 100)

typedef struct SCliConn {
  uv_connect_t connReq;
  uv_stream_t* stream;
  uv_write_t*  writeReq;
  void*        hostThrd;
  SConnBuffer  readBuf;
  void*        data;
  queue        conn;
  char         spi;
  char         secured;
  uint64_t     expireTime;
} SCliConn;

typedef struct SCliMsg {
  STransConnCtx* ctx;
  SRpcMsg        msg;
  queue          q;
  uint64_t       st;
} SCliMsg;

typedef struct SCliThrdObj {
  pthread_t       thread;
  uv_loop_t*      loop;
  uv_async_t*     cliAsync;  //
  uv_timer_t*     pTimer;
  void*           pool;  // conn pool
  queue           msg;
  pthread_mutex_t msgMtx;
  uint64_t        nextTimeout;  // next timeout
  void*           pTransInst;   //

} SCliThrdObj;

typedef struct SClientObj {
  char          label[TSDB_LABEL_LEN];
  int32_t       index;
  int           numOfThreads;
  SCliThrdObj** pThreadObj;
} SClientObj;

typedef struct SConnList {
  queue conn;
} SConnList;

// conn pool
// add expire timeout and capacity limit
static void*     connPoolCreate(int size);
static void*     connPoolDestroy(void* pool);
static SCliConn* getConnFromPool(void* pool, char* ip, uint32_t port);
static void      addConnToPool(void* pool, char* ip, uint32_t port, SCliConn* conn);

// register timer in each thread to clear expire conn
static void clientTimeoutCb(uv_timer_t* handle);
// process data read from server, auth/decompress etc later
static void clientProcessData(SCliConn* conn);
// check whether already read complete packet from server
static bool clientReadComplete(SConnBuffer* pBuf);
// alloc buf for read
static void clientAllocBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
// callback after read nbytes from socket
static void clientReadCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf);
// callback after write data to socket
static void clientWriteCb(uv_write_t* req, int status);
// callback after conn  to server
static void clientConnCb(uv_connect_t* req, int status);
static void clientAsyncCb(uv_async_t* handle);
static void clientDestroy(uv_handle_t* handle);
static void clientConnDestroy(SCliConn* pConn);

static void clientMsgDestroy(SCliMsg* pMsg);

// thread obj
static SCliThrdObj* createThrdObj();
static void         destroyThrdObj(SCliThrdObj* pThrd);
// thread
static void* clientThread(void* arg);

static void clientHandleReq(SCliMsg* pMsg, SCliThrdObj* pThrd);

static void clientProcessData(SCliConn* conn) {
  STransConnCtx* pCtx = ((SCliMsg*)conn->data)->ctx;
  SRpcInfo*      pRpc = pCtx->pRpc;
  SRpcMsg        rpcMsg;

  rpcMsg.pCont = conn->readBuf.buf;
  rpcMsg.contLen = conn->readBuf.len;
  rpcMsg.ahandle = pCtx->ahandle;
  (pRpc->cfp)(NULL, &rpcMsg, NULL);

  SCliThrdObj* pThrd = conn->hostThrd;
  addConnToPool(pThrd->pool, pCtx->ip, pCtx->port, conn);
  if (!uv_is_active((uv_handle_t*)pThrd->pTimer) && pRpc->idleTime > 0) {
    uv_timer_start((uv_timer_t*)pThrd->pTimer, clientTimeoutCb, CONN_PERSIST_TIME(pRpc->idleTime) / 2, 0);
  }
  free(pCtx->ip);
  free(pCtx);
  // impl
}

static void clientTimeoutCb(uv_timer_t* handle) {
  SCliThrdObj* pThrd = handle->data;
  SRpcInfo*    pRpc = pThrd->pTransInst;
  int64_t      currentTime = pThrd->nextTimeout;
  tDebug("timeout, try to remove expire conn from conn pool");

  SConnList* p = taosHashIterate((SHashObj*)pThrd->pool, NULL);
  while (p != NULL) {
    while (!QUEUE_IS_EMPTY(&p->conn)) {
      queue*    h = QUEUE_HEAD(&p->conn);
      SCliConn* c = QUEUE_DATA(h, SCliConn, conn);
      if (c->expireTime < currentTime) {
        QUEUE_REMOVE(h);
        clientConnDestroy(c);
      } else {
        break;
      }
    }
    p = taosHashIterate((SHashObj*)pThrd->pool, p);
  }

  pThrd->nextTimeout = taosGetTimestampMs() + CONN_PERSIST_TIME(pRpc->idleTime);
  uv_timer_start(handle, clientTimeoutCb, CONN_PERSIST_TIME(pRpc->idleTime) / 2, 0);
}
static void* connPoolCreate(int size) {
  SHashObj* pool = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  return pool;
}
static void* connPoolDestroy(void* pool) {
  SConnList* connList = taosHashIterate((SHashObj*)pool, NULL);
  while (connList != NULL) {
    while (!QUEUE_IS_EMPTY(&connList->conn)) {
      queue* h = QUEUE_HEAD(&connList->conn);
      QUEUE_REMOVE(h);
      SCliConn* c = QUEUE_DATA(h, SCliConn, conn);
      clientConnDestroy(c);
    }
    connList = taosHashIterate((SHashObj*)pool, connList);
  }
  taosHashClear(pool);
}

static SCliConn* getConnFromPool(void* pool, char* ip, uint32_t port) {
  char key[128] = {0};
  tstrncpy(key, ip, strlen(ip));
  tstrncpy(key + strlen(key), (char*)(&port), sizeof(port));

  SHashObj*  pPool = pool;
  SConnList* plist = taosHashGet(pPool, key, strlen(key));
  if (plist == NULL) {
    SConnList list;
    taosHashPut(pPool, key, strlen(key), (void*)&list, sizeof(list));
    plist = taosHashGet(pPool, key, strlen(key));
    QUEUE_INIT(&plist->conn);
  }

  if (QUEUE_IS_EMPTY(&plist->conn)) {
    return NULL;
  }
  queue* h = QUEUE_HEAD(&plist->conn);
  QUEUE_REMOVE(h);
  return QUEUE_DATA(h, SCliConn, conn);
}
static void addConnToPool(void* pool, char* ip, uint32_t port, SCliConn* conn) {
  char key[128] = {0};
  tstrncpy(key, ip, strlen(ip));
  tstrncpy(key + strlen(key), (char*)(&port), sizeof(port));

  SRpcInfo* pRpc = ((SCliThrdObj*)conn->hostThrd)->pTransInst;
  conn->expireTime = taosGetTimestampMs() + CONN_PERSIST_TIME(pRpc->idleTime);
  SConnList* plist = taosHashGet((SHashObj*)pool, key, strlen(key));
  // list already create before
  assert(plist != NULL);
  QUEUE_PUSH(&plist->conn, &conn->conn);
}
static bool clientReadComplete(SConnBuffer* data) {
  STransMsgHead head;
  int32_t       headLen = sizeof(head);
  if (data->len >= headLen) {
    memcpy((char*)&head, data->buf, headLen);
    int32_t msgLen = (int32_t)htonl((uint32_t)head.msgLen);
    if (msgLen > data->len) {
      data->left = msgLen - data->len;
      return false;
    } else {
      return true;
    }
  } else {
    return false;
  }
}
static void clientAllocReadBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  // impl later
  static const int CAPACITY = 512;

  SCliConn*    conn = handle->data;
  SConnBuffer* pBuf = &conn->readBuf;
  if (pBuf->cap == 0) {
    pBuf->buf = (char*)calloc(1, CAPACITY * sizeof(char));
    pBuf->len = 0;
    pBuf->cap = CAPACITY;
    pBuf->left = -1;

    buf->base = pBuf->buf;
    buf->len = CAPACITY;
  } else {
    if (pBuf->len >= pBuf->cap) {
      if (pBuf->left == -1) {
        pBuf->cap *= 2;
        pBuf->buf = realloc(pBuf->buf, pBuf->cap);
      } else if (pBuf->len + pBuf->left > pBuf->cap) {
        pBuf->cap = pBuf->len + pBuf->left;
        pBuf->buf = realloc(pBuf->buf, pBuf->cap);
      }
    }
    buf->base = pBuf->buf + pBuf->len;
    buf->len = pBuf->cap - pBuf->len;
  }
}
static void clientReadCb(uv_stream_t* handle, ssize_t nread, const uv_buf_t* buf) {
  // impl later
  SCliConn*    conn = handle->data;
  SConnBuffer* pBuf = &conn->readBuf;
  if (nread > 0) {
    pBuf->len += nread;
    if (clientReadComplete(pBuf)) {
      tDebug("alread read complete");
      clientProcessData(conn);
    } else {
      tDebug("read halp packet, continue to read");
    }
    return;
  }

  if (nread != UV_EOF) {
    tDebug("Read error %s\n", uv_err_name(nread));
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
  QUEUE_REMOVE(&conn->conn);
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
  SCliThrdObj* pThrd = pConn->hostThrd;
  if (pConn->stream == NULL) {
    pConn->stream = (uv_stream_t*)malloc(sizeof(uv_tcp_t));
    uv_tcp_init(pThrd->loop, (uv_tcp_t*)pConn->stream);
    pConn->stream->data = pConn;
  }
  uv_read_start((uv_stream_t*)pConn->stream, clientAllocReadBufferCb, clientReadCb);
  // impl later
}

static void clientWrite(SCliConn* pConn) {
  SCliMsg*       pCliMsg = pConn->data;
  SRpcMsg*       pMsg = (SRpcMsg*)(&pCliMsg->msg);
  STransMsgHead* pHead = transHeadFromCont(pMsg->pCont);

  int msgLen = transMsgLenFromCont(pMsg->contLen);

  pHead->msgType = pMsg->msgType;
  pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);

  uv_buf_t wb = uv_buf_init((char*)pHead, msgLen);
  tDebug("data write out, msgType : %d, len: %d", pHead->msgType, msgLen);
  uv_write(pConn->writeReq, (uv_stream_t*)pConn->stream, &wb, 1, clientWriteCb);
}
static void clientConnCb(uv_connect_t* req, int status) {
  // impl later
  SCliConn* pConn = req->data;
  SCliMsg*  pMsg = pConn->data;

  STransConnCtx* pCtx = pMsg->ctx;
  SRpcInfo*      pRpc = pCtx->pRpc;

  if (status != 0) {
    // tError("failed to connect server(%s, %d), errmsg: %s", pCtx->ip, pCtx->port, uv_strerror(status));
    tError("failed to connect server,  errmsg: %s", uv_strerror(status));
    // call user fp later
    SRpcMsg rpcMsg;
    rpcMsg.ahandle = pCtx->ahandle;
    // SRpcInfo* pRpc = pMsg->ctx->pRpc;
    (pRpc->cfp)(NULL, &rpcMsg, NULL);
    uv_close((uv_handle_t*)req->handle, clientDestroy);
    return;
  }

  assert(pConn->stream == req->handle);
  clientWrite(pConn);
}

static void clientHandleReq(SCliMsg* pMsg, SCliThrdObj* pThrd) {
  uint64_t et = taosGetTimestampUs();
  uint64_t el = et - pMsg->st;
  tDebug("msg tran time cost: %" PRIu64 "", el);
  et = taosGetTimestampUs();

  STransConnCtx* pCtx = pMsg->ctx;
  SCliConn*      conn = getConnFromPool(pThrd->pool, pCtx->ip, pCtx->port);
  if (conn != NULL) {
    // impl later
    conn->data = pMsg;
    conn->writeReq->data = conn;

    conn->readBuf.len = 0;
    memset(conn->readBuf.buf, 0, conn->readBuf.cap);
    conn->readBuf.left = -1;
    clientWrite(conn);
  } else {
    SCliConn* conn = calloc(1, sizeof(SCliConn));

    // read/write stream handle
    conn->stream = (uv_stream_t*)malloc(sizeof(uv_tcp_t));
    uv_tcp_init(pThrd->loop, (uv_tcp_t*)(conn->stream));
    conn->stream->data = conn;

    // write req handle
    conn->writeReq = malloc(sizeof(uv_write_t));
    conn->writeReq->data = conn;
    QUEUE_INIT(&conn->conn);

    conn->connReq.data = conn;
    conn->data = pMsg;
    conn->hostThrd = pThrd;

    struct sockaddr_in addr;
    uv_ip4_addr(pMsg->ctx->ip, pMsg->ctx->port, &addr);
    // handle error in callback if fail to connect
    uv_tcp_connect(&conn->connReq, (uv_tcp_t*)(conn->stream), (const struct sockaddr*)&addr, clientConnCb);
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

    SCliMsg* pMsg = QUEUE_DATA(h, SCliMsg, q);
    clientHandleReq(pMsg, pThrd);
    count++;
  }
  if (count >= 2) {
    tDebug("already process batch size: %d", count);
  }
}

static void* clientThread(void* arg) {
  SCliThrdObj* pThrd = (SCliThrdObj*)arg;
  uv_run(pThrd->loop, UV_RUN_DEFAULT);
}

void* taosInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle) {
  SClientObj* cli = calloc(1, sizeof(SClientObj));

  SRpcInfo* pRpc = shandle;
  memcpy(cli->label, label, strlen(label));
  cli->numOfThreads = numOfThreads;
  cli->pThreadObj = (SCliThrdObj**)calloc(cli->numOfThreads, sizeof(SCliThrdObj*));

  for (int i = 0; i < cli->numOfThreads; i++) {
    SCliThrdObj* pThrd = createThrdObj();
    pThrd->nextTimeout = taosGetTimestampMs() + CONN_PERSIST_TIME(pRpc->idleTime);
    pThrd->pTransInst = shandle;

    int err = pthread_create(&pThrd->thread, NULL, clientThread, (void*)(pThrd));
    if (err == 0) {
      tDebug("sucess to create tranport-client thread %d", i);
    }
    cli->pThreadObj[i] = pThrd;
  }
  return cli;
}
static void clientMsgDestroy(SCliMsg* pMsg) {
  // impl later
  free(pMsg);
}
static SCliThrdObj* createThrdObj() {
  SCliThrdObj* pThrd = (SCliThrdObj*)calloc(1, sizeof(SCliThrdObj));

  QUEUE_INIT(&pThrd->msg);
  pthread_mutex_init(&pThrd->msgMtx, NULL);

  pThrd->loop = (uv_loop_t*)malloc(sizeof(uv_loop_t));
  uv_loop_init(pThrd->loop);

  pThrd->cliAsync = malloc(sizeof(uv_async_t));
  uv_async_init(pThrd->loop, pThrd->cliAsync, clientAsyncCb);
  pThrd->cliAsync->data = pThrd;

  pThrd->pTimer = malloc(sizeof(uv_timer_t));
  uv_timer_init(pThrd->loop, pThrd->pTimer);
  pThrd->pTimer->data = pThrd;

  pThrd->pool = connPoolCreate(1);
  return pThrd;
}
static void destroyThrdObj(SCliThrdObj* pThrd) {
  if (pThrd == NULL) {
    return;
  }
  pthread_join(pThrd->thread, NULL);
  pthread_mutex_destroy(&pThrd->msgMtx);
  free(pThrd->cliAsync);
  free(pThrd->loop);
  free(pThrd);
}
//
void taosCloseClient(void* arg) {
  // impl later
  SClientObj* cli = arg;
  for (int i = 0; i < cli->numOfThreads; i++) {
    destroyThrdObj(cli->pThreadObj[i]);
  }
  free(cli->pThreadObj);
  free(cli);
}

void rpcSendRequest(void* shandle, const SEpSet* pEpSet, SRpcMsg* pMsg, int64_t* pRid) {
  // impl later
  char*    ip = (char*)(pEpSet->fqdn[pEpSet->inUse]);
  uint32_t port = pEpSet->port[pEpSet->inUse];

  SRpcInfo* pRpc = (SRpcInfo*)shandle;

  int32_t flen = 0;
  if (transCompressMsg(pMsg->pCont, pMsg->contLen, &flen)) {
    // imp later
  }

  STransConnCtx* pCtx = calloc(1, sizeof(STransConnCtx));

  pCtx->pRpc = (SRpcInfo*)shandle;
  pCtx->ahandle = pMsg->ahandle;
  pCtx->msgType = pMsg->msgType;
  pCtx->ip = strdup(ip);
  pCtx->port = port;

  assert(pRpc->connType == TAOS_CONN_CLIENT);
  // atomic or not
  int64_t index = pRpc->index;
  if (pRpc->index++ >= pRpc->numOfThreads) {
    pRpc->index = 0;
  }
  SCliMsg* cliMsg = malloc(sizeof(SCliMsg));
  cliMsg->ctx = pCtx;
  cliMsg->msg = *pMsg;
  cliMsg->st = taosGetTimestampUs();

  SCliThrdObj* thrd = ((SClientObj*)pRpc->tcphandle)->pThreadObj[index % pRpc->numOfThreads];

  pthread_mutex_lock(&thrd->msgMtx);
  QUEUE_PUSH(&thrd->msg, &cliMsg->q);
  pthread_mutex_unlock(&thrd->msgMtx);

  uv_async_send(thrd->cliAsync);
}
#endif
