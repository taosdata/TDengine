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

#define CONN_PERSIST_TIME(para) (para * 1000 * 10)

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
  int8_t       notifyCount;  // timers already notify to client
  int32_t      ref;
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
static void*     creatConnPool(int size);
static void*     destroyConnPool(void* pool);
static SCliConn* getConnFromPool(void* pool, char* ip, uint32_t port);
static void      addConnToPool(void* pool, char* ip, uint32_t port, SCliConn* conn);

// register timer in each thread to clear expire conn
static void clientTimeoutCb(uv_timer_t* handle);
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
static void clientConnDestroy(SCliConn* pConn, bool clear /*clear tcp handle or not*/);

// process data read from server, auth/decompress etc later
static void clientHandleResp(SCliConn* conn);
// handle except about conn
static void clientHandleExcept(SCliConn* conn);
// handle req from app
static void clientHandleReq(SCliMsg* pMsg, SCliThrdObj* pThrd);

static void destroyUserdata(SRpcMsg* userdata);

static void destroyCmsg(SCliMsg* cmsg);
static void transDestroyConnCtx(STransConnCtx* ctx);
// thread obj
static SCliThrdObj* createThrdObj();
static void         destroyThrdObj(SCliThrdObj* pThrd);
// thread
static void* clientThread(void* arg);

static void clientHandleResp(SCliConn* conn) {
  SCliMsg*       pMsg = conn->data;
  STransConnCtx* pCtx = pMsg->ctx;
  SRpcInfo*      pRpc = pCtx->pTransInst;

  STransMsgHead* pHead = (STransMsgHead*)(conn->readBuf.buf);
  pHead->code = htonl(pHead->code);
  pHead->msgLen = htonl(pHead->msgLen);

  SRpcMsg rpcMsg;
  rpcMsg.contLen = transContLenFromMsg(pHead->msgLen);
  rpcMsg.pCont = transContFromHead((char*)pHead);
  rpcMsg.code = pHead->code;
  rpcMsg.msgType = pHead->msgType;
  rpcMsg.ahandle = pCtx->ahandle;

  tDebug("conn %p handle resp", conn);
  (pRpc->cfp)(NULL, &rpcMsg, NULL);
  conn->notifyCount += 1;

  // buf's mem alread translated to rpcMsg.pCont
  transClearBuffer(&conn->readBuf);

  uv_read_start((uv_stream_t*)conn->stream, clientAllocBufferCb, clientReadCb);

  SCliThrdObj* pThrd = conn->hostThrd;
  addConnToPool(pThrd->pool, pCtx->ip, pCtx->port, conn);

  destroyCmsg(pMsg);
  conn->data = NULL;
  // start thread's timer of conn pool if not active
  if (!uv_is_active((uv_handle_t*)pThrd->pTimer) && pRpc->idleTime > 0) {
    uv_timer_start((uv_timer_t*)pThrd->pTimer, clientTimeoutCb, CONN_PERSIST_TIME(pRpc->idleTime) / 2, 0);
  }
}
static void clientHandleExcept(SCliConn* pConn) {
  if (pConn->data == NULL) {
    // handle conn except in conn pool
    clientConnDestroy(pConn, true);
    return;
  }
  tDebug("conn %p start to destroy", pConn);
  SCliMsg* pMsg = pConn->data;

  destroyUserdata(&pMsg->msg);

  STransConnCtx* pCtx = pMsg->ctx;

  SRpcMsg rpcMsg = {0};
  rpcMsg.ahandle = pCtx->ahandle;
  rpcMsg.code = -1;
  // SRpcInfo* pRpc = pMsg->ctx->pRpc;
  (pCtx->pTransInst->cfp)(NULL, &rpcMsg, NULL);
  pConn->notifyCount += 1;

  destroyCmsg(pMsg);
  pConn->data = NULL;
  // transDestroyConnCtx(pCtx);
  clientConnDestroy(pConn, true);
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
        // uv_stream_t stm = *(c->stream);
        // uv_close((uv_handle_t*)&stm, clientDestroy);
        clientConnDestroy(c, true);
      } else {
        break;
      }
    }
    p = taosHashIterate((SHashObj*)pThrd->pool, p);
  }

  pThrd->nextTimeout = taosGetTimestampMs() + CONN_PERSIST_TIME(pRpc->idleTime);
  uv_timer_start(handle, clientTimeoutCb, CONN_PERSIST_TIME(pRpc->idleTime) / 2, 0);
}
static void* creatConnPool(int size) {
  // thread local, no lock
  return taosHashInit(size, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
}
static void* destroyConnPool(void* pool) {
  SConnList* connList = taosHashIterate((SHashObj*)pool, NULL);
  while (connList != NULL) {
    while (!QUEUE_IS_EMPTY(&connList->conn)) {
      queue* h = QUEUE_HEAD(&connList->conn);
      QUEUE_REMOVE(h);
      SCliConn* c = QUEUE_DATA(h, SCliConn, conn);
      clientConnDestroy(c, true);
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

  SCliConn* conn = QUEUE_DATA(h, SCliConn, conn);
  QUEUE_INIT(&conn->conn);
  return conn;
}
static void addConnToPool(void* pool, char* ip, uint32_t port, SCliConn* conn) {
  char key[128] = {0};

  tstrncpy(key, ip, strlen(ip));
  tstrncpy(key + strlen(key), (char*)(&port), sizeof(port));
  tDebug("conn %p added to conn pool, read buf cap: %d", conn, conn->readBuf.cap);

  SRpcInfo* pRpc = ((SCliThrdObj*)conn->hostThrd)->pTransInst;

  conn->expireTime = taosGetTimestampMs() + CONN_PERSIST_TIME(pRpc->idleTime);
  SConnList* plist = taosHashGet((SHashObj*)pool, key, strlen(key));
  conn->notifyCount = 0;
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
    } else if (msgLen == data->len) {
      data->left = 0;
      return true;
    }
  } else {
    return false;
  }
}
static void clientAllocBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  SCliConn*    conn = handle->data;
  SConnBuffer* pBuf = &conn->readBuf;
  transAllocBuffer(pBuf, buf);
}
static void clientReadCb(uv_stream_t* handle, ssize_t nread, const uv_buf_t* buf) {
  // impl later
  SCliConn*    conn = handle->data;
  SConnBuffer* pBuf = &conn->readBuf;
  if (nread > 0) {
    pBuf->len += nread;
    if (clientReadComplete(pBuf)) {
      uv_read_stop((uv_stream_t*)conn->stream);
      tDebug("conn %p read complete", conn);
      clientHandleResp(conn);
    } else {
      tDebug("conn %p read partial packet, continue to read", conn);
    }
    return;
  }
  assert(nread <= 0);
  if (nread == 0) {
    // ref http://docs.libuv.org/en/v1.x/stream.html?highlight=uv_read_start#c.uv_read_cb
    // nread might be 0, which does not indicate an error or EOF. This is equivalent to EAGAIN or EWOULDBLOCK under
    // read(2).
    return;
  }
  if (nread < 0 || nread == UV_EOF) {
    tError("conn %p read error: %s", conn, uv_err_name(nread));
    clientHandleExcept(conn);
  }
  // tDebug("Read error %s\n", uv_err_name(nread));
  // uv_close((uv_handle_t*)handle, clientDestroy);
}

static void clientConnDestroy(SCliConn* conn, bool clear) {
  //
  conn->ref--;
  if (conn->ref == 0) {
    tDebug("conn %p remove from conn pool", conn);
    QUEUE_REMOVE(&conn->conn);
    tDebug("conn %p remove from conn pool successfully", conn);
    if (clear) {
      uv_close((uv_handle_t*)conn->stream, clientDestroy);
    }
  }
}
static void clientDestroy(uv_handle_t* handle) {
  SCliConn* conn = handle->data;
  // transDestroyBuffer(&conn->readBuf);

  free(conn->stream);
  free(conn->writeReq);
  tDebug("conn %p destroy successfully", conn);
  free(conn);

  // clientConnDestroy(conn, false);
}

static void clientWriteCb(uv_write_t* req, int status) {
  SCliConn* pConn = req->data;
  if (status == 0) {
    tDebug("conn %p data already was written out", pConn);
    SCliMsg* pMsg = pConn->data;
    if (pMsg == NULL) {
      destroy
          // handle
          return;
    }
    destroyUserdata(&pMsg->msg);
  } else {
    tError("conn %p failed to write: %s", pConn, uv_err_name(status));
    clientHandleExcept(pConn);
    return;
  }
  SCliThrdObj* pThrd = pConn->hostThrd;
  uv_read_start((uv_stream_t*)pConn->stream, clientAllocBufferCb, clientReadCb);
}

static void clientWrite(SCliConn* pConn) {
  SCliMsg*       pCliMsg = pConn->data;
  SRpcMsg*       pMsg = (SRpcMsg*)(&pCliMsg->msg);
  STransMsgHead* pHead = transHeadFromCont(pMsg->pCont);

  int msgLen = transMsgLenFromCont(pMsg->contLen);

  pHead->msgType = pMsg->msgType;
  pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);

  uv_buf_t wb = uv_buf_init((char*)pHead, msgLen);
  tDebug("conn %p data write out, msgType : %d, len: %d", pConn, pHead->msgType, msgLen);
  uv_write(pConn->writeReq, (uv_stream_t*)pConn->stream, &wb, 1, clientWriteCb);
}
static void clientConnCb(uv_connect_t* req, int status) {
  // impl later
  SCliConn* pConn = req->data;
  if (status != 0) {
    // tError("failed to connect server(%s, %d), errmsg: %s", pCtx->ip, pCtx->port, uv_strerror(status));
    tError("conn %p failed to connect server: %s", pConn, uv_strerror(status));
    clientHandleExcept(pConn);
    return;
  }
  tDebug("conn %p create", pConn);

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
    tDebug("conn %p get from conn pool", conn);
    conn->data = pMsg;
    conn->writeReq->data = conn;
    transDestroyBuffer(&conn->readBuf);
    clientWrite(conn);
  } else {
    SCliConn* conn = calloc(1, sizeof(SCliConn));
    conn->ref++;
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

static void destroyUserdata(SRpcMsg* userdata) {
  if (userdata->pCont == NULL) {
    return;
  }
  transFreeMsg(userdata->pCont);
  userdata->pCont = NULL;
}
static void destroyCmsg(SCliMsg* pMsg) {
  if (pMsg == NULL) {
    return;
  }
  transDestroyConnCtx(pMsg->ctx);
  destroyUserdata(&pMsg->msg);
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

  pThrd->pool = creatConnPool(1);
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

static void transDestroyConnCtx(STransConnCtx* ctx) {
  if (ctx != NULL) {
    free(ctx->ip);
  }
  free(ctx);
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

  pCtx->pTransInst = (SRpcInfo*)shandle;
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
