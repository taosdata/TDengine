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

#define CONN_HOST_THREAD_INDEX(conn) (conn ? ((SCliConn*)conn)->hThrdIdx : -1)
#define CONN_PERSIST_TIME(para) (para * 1000 * 10)

typedef struct SCliConn {
  uv_connect_t connReq;
  uv_stream_t* stream;
  uv_write_t*  writeReq;
  void*        hostThrd;
  SConnBuffer  readBuf;
  void*        data;
  queue        conn;
  uint64_t     expireTime;
  int8_t       ctnRdCnt;  // continue read count
  int          hThrdIdx;

  SRpcPush* push;
  int       persist;  //
  // spi configure
  char    spi;
  char    secured;
  int32_t ref;
  // debug and log info
  struct sockaddr_in addr;
  struct sockaddr_in locaddr;
} SCliConn;

typedef struct SCliMsg {
  STransConnCtx* ctx;
  SRpcMsg        msg;
  queue          q;
  uint64_t       st;
} SCliMsg;

typedef struct SCliThrdObj {
  pthread_t  thread;
  uv_loop_t* loop;
  // uv_async_t*     cliAsync;  //
  SAsyncPool*     asyncPool;
  uv_timer_t*     timer;
  void*           pool;  // conn pool
  queue           msg;
  pthread_mutex_t msgMtx;
  uint64_t        nextTimeout;  // next timeout
  void*           pTransInst;   //
  bool            quit;
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
static void clientHandleQuit(SCliMsg* pMsg, SCliThrdObj* pThrd);
static void clientSendQuit(SCliThrdObj* thrd);

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

  if (rpcMsg.msgType == TDMT_VND_QUERY_RSP || rpcMsg.msgType == TDMT_VND_FETCH_RSP) {
    rpcMsg.handle = conn;
    conn->persist = 1;
  }

  tDebug("client conn %p %s received from %s:%d, local info: %s:%d", conn, TMSG_INFO(pHead->msgType),
         inet_ntoa(conn->addr.sin_addr), ntohs(conn->addr.sin_port), inet_ntoa(conn->locaddr.sin_addr),
         ntohs(conn->locaddr.sin_port));

  if (conn->push != NULL && conn->ctnRdCnt != 0) {
    (*conn->push->callback)(conn->push->arg, &rpcMsg);
    conn->push = NULL;
  } else {
    if (pCtx->pSem == NULL) {
      tTrace("client conn %p handle resp", conn);
      (pRpc->cfp)(pRpc->parent, &rpcMsg, NULL);
    } else {
      tTrace("client conn(sync) %p handle resp", conn);
      memcpy((char*)pCtx->pRsp, (char*)&rpcMsg, sizeof(rpcMsg));
      tsem_post(pCtx->pSem);
    }
  }
  conn->ctnRdCnt += 1;
  conn->secured = pHead->secured;

  // buf's mem alread translated to rpcMsg.pCont
  transClearBuffer(&conn->readBuf);

  uv_read_start((uv_stream_t*)conn->stream, clientAllocBufferCb, clientReadCb);

  SCliThrdObj* pThrd = conn->hostThrd;

  // user owns conn->persist = 1
  if (conn->push == NULL || conn->persist == 0) {
    addConnToPool(pThrd->pool, pCtx->ip, pCtx->port, conn);

    destroyCmsg(conn->data);
    conn->data = NULL;
  }

  // start thread's timer of conn pool if not active
  if (!uv_is_active((uv_handle_t*)pThrd->timer) && pRpc->idleTime > 0) {
    uv_timer_start((uv_timer_t*)pThrd->timer, clientTimeoutCb, CONN_PERSIST_TIME(pRpc->idleTime) / 2, 0);
  }
}
static void clientHandleExcept(SCliConn* pConn) {
  if (pConn->data == NULL && pConn->push == NULL) {
    // handle conn except in conn pool
    clientConnDestroy(pConn, true);
    return;
  }
  tTrace("client conn %p start to destroy", pConn);
  SCliMsg* pMsg = pConn->data;

  tmsg_t msgType = TDMT_MND_CONNECT;
  if (pMsg != NULL) {
    msgType = pMsg->msg.msgType;
  }
  STransConnCtx* pCtx = pMsg->ctx;

  SRpcMsg rpcMsg = {0};
  rpcMsg.ahandle = pCtx->ahandle;
  rpcMsg.code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
  rpcMsg.msgType = msgType + 1;

  if (pConn->push != NULL && pConn->ctnRdCnt != 0) {
    (*pConn->push->callback)(pConn->push->arg, &rpcMsg);
    pConn->push = NULL;
  } else {
    if (pCtx->pSem == NULL) {
      (pCtx->pTransInst->cfp)(pCtx->pTransInst->parent, &rpcMsg, NULL);
    } else {
      memcpy((char*)(pCtx->pRsp), (char*)(&rpcMsg), sizeof(rpcMsg));
      tsem_post(pCtx->pSem);
    }
    if (pConn->push != NULL) {
      (*pConn->push->callback)(pConn->push->arg, &rpcMsg);
    }
    pConn->push = NULL;
  }
  if (pConn->push == NULL) {
    destroyCmsg(pConn->data);
    pConn->data = NULL;
  }
  // transDestroyConnCtx(pCtx);
  clientConnDestroy(pConn, true);
  pConn->ctnRdCnt += 1;
}

static void clientTimeoutCb(uv_timer_t* handle) {
  SCliThrdObj* pThrd = handle->data;
  SRpcInfo*    pRpc = pThrd->pTransInst;
  int64_t      currentTime = pThrd->nextTimeout;
  tTrace("client conn timeout, try to remove expire conn from conn pool");

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
  tTrace("client conn %p added to conn pool, read buf cap: %d", conn, conn->readBuf.cap);

  SRpcInfo* pRpc = ((SCliThrdObj*)conn->hostThrd)->pTransInst;

  conn->expireTime = taosGetTimestampMs() + CONN_PERSIST_TIME(pRpc->idleTime);
  SConnList* plist = taosHashGet((SHashObj*)pool, key, strlen(key));
  conn->ctnRdCnt = 0;
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
      tTrace("client conn %p read complete", conn);
      clientHandleResp(conn);
    } else {
      tTrace("client conn %p read partial packet, continue to read", conn);
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
    tError("client conn %p read error: %s", conn, uv_err_name(nread));
    clientHandleExcept(conn);
  }
  // tDebug("Read error %s\n", uv_err_name(nread));
  // uv_close((uv_handle_t*)handle, clientDestroy);
}

static void clientConnDestroy(SCliConn* conn, bool clear) {
  //
  conn->ref--;
  if (conn->ref == 0) {
    tTrace("client conn %p remove from conn pool", conn);
    QUEUE_REMOVE(&conn->conn);
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
  tTrace("client conn %p destroy successfully", conn);
  free(conn);

  // clientConnDestroy(conn, false);
}

static void clientWriteCb(uv_write_t* req, int status) {
  SCliConn* pConn = req->data;
  if (status == 0) {
    tTrace("client conn %p data already was written out", pConn);
    SCliMsg* pMsg = pConn->data;
    if (pMsg == NULL) {
      // handle
      return;
    }
    destroyUserdata(&pMsg->msg);
  } else {
    tError("client conn %p failed to write: %s", pConn, uv_err_name(status));
    clientHandleExcept(pConn);
    return;
  }
  SCliThrdObj* pThrd = pConn->hostThrd;
  uv_read_start((uv_stream_t*)pConn->stream, clientAllocBufferCb, clientReadCb);
}

static void clientWrite(SCliConn* pConn) {
  SCliMsg*       pCliMsg = pConn->data;
  STransConnCtx* pCtx = pCliMsg->ctx;
  SRpcInfo*      pTransInst = pCtx->pTransInst;

  SRpcMsg* pMsg = (SRpcMsg*)(&pCliMsg->msg);

  STransMsgHead* pHead = transHeadFromCont(pMsg->pCont);
  int            msgLen = transMsgLenFromCont(pMsg->contLen);

  if (!pConn->secured) {
    char* buf = calloc(1, msgLen + sizeof(STransUserMsg));
    memcpy(buf, (char*)pHead, msgLen);

    STransUserMsg* uMsg = (STransUserMsg*)(buf + msgLen);
    memcpy(uMsg->user, pTransInst->user, tListLen(uMsg->user));
    memcpy(uMsg->secret, pTransInst->secret, tListLen(uMsg->secret));

    // to avoid mem leak
    destroyUserdata(pMsg);

    pMsg->pCont = (char*)buf + sizeof(STransMsgHead);
    pMsg->contLen = msgLen + sizeof(STransUserMsg) - sizeof(STransMsgHead);

    pHead = (STransMsgHead*)buf;
    pHead->secured = 1;
    msgLen += sizeof(STransUserMsg);
  }

  pHead->msgType = pMsg->msgType;
  pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);

  // if (pHead->msgType == TDMT_VND_QUERY || pHead->msgType == TDMT_VND_)

  uv_buf_t wb = uv_buf_init((char*)pHead, msgLen);
  tDebug("client conn %p %s is send to %s:%d, local info %s:%d", pConn, TMSG_INFO(pHead->msgType),
         inet_ntoa(pConn->addr.sin_addr), ntohs(pConn->addr.sin_port), inet_ntoa(pConn->locaddr.sin_addr),
         ntohs(pConn->locaddr.sin_port));
  uv_write(pConn->writeReq, (uv_stream_t*)pConn->stream, &wb, 1, clientWriteCb);
}
static void clientConnCb(uv_connect_t* req, int status) {
  // impl later
  SCliConn* pConn = req->data;
  if (status != 0) {
    // tError("failed to connect server(%s, %d), errmsg: %s", pCtx->ip, pCtx->port, uv_strerror(status));
    tError("client conn %p failed to connect server: %s", pConn, uv_strerror(status));
    clientHandleExcept(pConn);
    return;
  }
  int addrlen = sizeof(pConn->addr);
  uv_tcp_getpeername((uv_tcp_t*)pConn->stream, (struct sockaddr*)&pConn->addr, &addrlen);

  addrlen = sizeof(pConn->locaddr);
  uv_tcp_getsockname((uv_tcp_t*)pConn->stream, (struct sockaddr*)&pConn->locaddr, &addrlen);

  tTrace("client conn %p create", pConn);

  assert(pConn->stream == req->handle);
  clientWrite(pConn);
}

static void clientHandleQuit(SCliMsg* pMsg, SCliThrdObj* pThrd) {
  tDebug("client work thread %p start to quit", pThrd);
  destroyCmsg(pMsg);
  // transDestroyAsyncPool(pThr) uv_close((uv_handle_t*)pThrd->cliAsync, NULL);
  uv_timer_stop(pThrd->timer);
  pThrd->quit = true;
  // uv__async_stop(pThrd->cliAsync);
  uv_stop(pThrd->loop);
}
static void clientHandleReq(SCliMsg* pMsg, SCliThrdObj* pThrd) {
  uint64_t et = taosGetTimestampUs();
  uint64_t el = et - pMsg->st;
  tTrace("client msg tran time cost: %" PRIu64 "us", el);
  et = taosGetTimestampUs();

  STransConnCtx* pCtx = pMsg->ctx;

  SCliConn* conn = NULL;
  if (pMsg->msg.handle == NULL) {
    conn = getConnFromPool(pThrd->pool, pCtx->ip, pCtx->port);
    if (conn != NULL) {
      tTrace("client conn %p get from conn pool", conn);
    }
  } else {
    conn = (SCliConn*)(pMsg->msg.handle);
    if (conn != NULL) {
      tTrace("client conn %p reused", conn);
    }
  }

  if (conn != NULL) {
    conn->data = pMsg;
    conn->writeReq->data = conn;
    transDestroyBuffer(&conn->readBuf);

    if (pThrd->quit) {
      clientHandleExcept(conn);
      return;
    }
    clientWrite(conn);

  } else {
    conn = calloc(1, sizeof(SCliConn));
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

    // conn->push = pMsg->msg.push;
    // conn->ctnRdCnt = 0;

    struct sockaddr_in addr;
    uv_ip4_addr(pMsg->ctx->ip, pMsg->ctx->port, &addr);
    // handle error in callback if fail to connect
    uv_tcp_connect(&conn->connReq, (uv_tcp_t*)(conn->stream), (const struct sockaddr*)&addr, clientConnCb);
  }

  conn->push = pMsg->msg.push;
  conn->ctnRdCnt = 0;
  conn->hThrdIdx = pCtx->hThrdIdx;
}
static void clientAsyncCb(uv_async_t* handle) {
  SAsyncItem*  item = handle->data;
  SCliThrdObj* pThrd = item->pThrd;
  SCliMsg*     pMsg = NULL;
  queue        wq;

  // batch process to avoid to lock/unlock frequently
  pthread_mutex_lock(&item->mtx);
  QUEUE_MOVE(&item->qmsg, &wq);
  pthread_mutex_unlock(&item->mtx);

  int count = 0;
  while (!QUEUE_IS_EMPTY(&wq)) {
    queue* h = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(h);

    SCliMsg* pMsg = QUEUE_DATA(h, SCliMsg, q);
    if (pMsg->ctx == NULL) {
      clientHandleQuit(pMsg, pThrd);
    } else {
      clientHandleReq(pMsg, pThrd);
    }
    // clientHandleReq(pMsg, pThrd);
    count++;
  }
  if (count >= 2) {
    tTrace("client process batch size: %d", count);
  }
}

static void* clientThread(void* arg) {
  SCliThrdObj* pThrd = (SCliThrdObj*)arg;
  setThreadName("trans-client-work");
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
      tDebug("success to create tranport-client thread %d", i);
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

  pThrd->asyncPool = transCreateAsyncPool(pThrd->loop, 5, pThrd, clientAsyncCb);

  pThrd->timer = malloc(sizeof(uv_timer_t));
  uv_timer_init(pThrd->loop, pThrd->timer);
  pThrd->timer->data = pThrd;

  pThrd->pool = creatConnPool(4);

  pThrd->quit = false;
  return pThrd;
}
static void destroyThrdObj(SCliThrdObj* pThrd) {
  if (pThrd == NULL) {
    return;
  }
  uv_stop(pThrd->loop);
  pthread_join(pThrd->thread, NULL);
  pthread_mutex_destroy(&pThrd->msgMtx);
  transDestroyAsyncPool(pThrd->asyncPool);
  // free(pThrd->cliAsync);
  free(pThrd->timer);
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
static void clientSendQuit(SCliThrdObj* thrd) {
  // cli can stop gracefully
  SCliMsg* msg = calloc(1, sizeof(SCliMsg));
  msg->ctx = NULL;  //

  transSendAsync(thrd->asyncPool, &msg->q);
}
void taosCloseClient(void* arg) {
  SClientObj* cli = arg;
  for (int i = 0; i < cli->numOfThreads; i++) {
    clientSendQuit(cli->pThreadObj[i]);
    destroyThrdObj(cli->pThreadObj[i]);
  }
  free(cli->pThreadObj);
  free(cli);
}
static int clientRBChoseIdx(SRpcInfo* pRpc) {
  int64_t index = pRpc->index;
  if (pRpc->index++ >= pRpc->numOfThreads) {
    pRpc->index = 0;
  }
  return index % pRpc->numOfThreads;
}
void rpcSendRequest(void* shandle, const SEpSet* pEpSet, SRpcMsg* pMsg, int64_t* pRid) {
  // impl later
  char*    ip = (char*)(pEpSet->eps[pEpSet->inUse].fqdn);
  uint32_t port = pEpSet->eps[pEpSet->inUse].port;

  SRpcInfo* pRpc = (SRpcInfo*)shandle;

  int index = CONN_HOST_THREAD_INDEX(pMsg->handle);
  if (index == -1) {
    index = clientRBChoseIdx(pRpc);
  }
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
  pCtx->hThrdIdx = index;

  assert(pRpc->connType == TAOS_CONN_CLIENT);
  // atomic or not

  SCliMsg* cliMsg = malloc(sizeof(SCliMsg));
  cliMsg->ctx = pCtx;
  cliMsg->msg = *pMsg;
  cliMsg->st = taosGetTimestampUs();

  SCliThrdObj* thrd = ((SClientObj*)pRpc->tcphandle)->pThreadObj[index];
  transSendAsync(thrd->asyncPool, &(cliMsg->q));
}

void rpcSendRecv(void* shandle, SEpSet* pEpSet, SRpcMsg* pReq, SRpcMsg* pRsp) {
  char*    ip = (char*)(pEpSet->eps[pEpSet->inUse].fqdn);
  uint32_t port = pEpSet->eps[pEpSet->inUse].port;

  SRpcInfo* pRpc = (SRpcInfo*)shandle;

  int index = CONN_HOST_THREAD_INDEX(pReq->handle);
  if (index == -1) {
    index = clientRBChoseIdx(pRpc);
  }

  STransConnCtx* pCtx = calloc(1, sizeof(STransConnCtx));
  pCtx->pTransInst = (SRpcInfo*)shandle;
  pCtx->ahandle = pReq->ahandle;
  pCtx->msgType = pReq->msgType;
  pCtx->ip = strdup(ip);
  pCtx->port = port;
  pCtx->hThrdIdx = index;
  pCtx->pSem = calloc(1, sizeof(tsem_t));
  pCtx->pRsp = pRsp;
  tsem_init(pCtx->pSem, 0, 0);

  SCliMsg* cliMsg = malloc(sizeof(SCliMsg));
  cliMsg->ctx = pCtx;
  cliMsg->msg = *pReq;
  cliMsg->st = taosGetTimestampUs();

  SCliThrdObj* thrd = ((SClientObj*)pRpc->tcphandle)->pThreadObj[index];
  transSendAsync(thrd->asyncPool, &(cliMsg->q));
  tsem_t* pSem = pCtx->pSem;
  tsem_wait(pSem);
  tsem_destroy(pSem);
  free(pSem);

  return;
}
#endif
