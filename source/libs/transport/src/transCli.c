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

// Normal(default):   send/recv msg
// Quit:     quit rpc inst
// Release:  release handle to rpc inst
typedef enum { Normal, Quit, Release } SCliMsgType;

typedef struct SCliConn {
  T_REF_DECLARE()
  uv_connect_t connReq;
  uv_stream_t* stream;
  uv_write_t   writeReq;
  void*        hostThrd;
  SConnBuffer  readBuf;
  void*        data;
  queue        conn;
  uint64_t     expireTime;
  int          hThrdIdx;
  bool         broken;  // link broken or not

  int persist;  //
  // spi configure
  char spi;
  char secured;

  char*    ip;
  uint32_t port;

  // debug and log info
  struct sockaddr_in addr;
  struct sockaddr_in locaddr;

} SCliConn;

typedef struct SCliMsg {
  STransConnCtx* ctx;
  STransMsg      msg;
  queue          q;
  uint64_t       st;
  SCliMsgType    type;
} SCliMsg;

typedef struct SCliThrdObj {
  pthread_t   thread;
  uv_loop_t*  loop;
  SAsyncPool* asyncPool;
  uv_timer_t  timer;
  void*       pool;  // conn pool

  // msg queue
  queue           msg;
  pthread_mutex_t msgMtx;

  uint64_t nextTimeout;  // next timeout
  void*    pTransInst;   //
  bool     quit;
} SCliThrdObj;

typedef struct SCliObj {
  char          label[TSDB_LABEL_LEN];
  int32_t       index;
  int           numOfThreads;
  SCliThrdObj** pThreadObj;
} SCliObj;

typedef struct SConnList {
  queue conn;
} SConnList;

// conn pool
// add expire timeout and capacity limit
static void*     createConnPool(int size);
static void*     destroyConnPool(void* pool);
static SCliConn* getConnFromPool(void* pool, char* ip, uint32_t port);
static void      addConnToPool(void* pool, SCliConn* conn);

// register timer in each thread to clear expire conn
static void cliTimeoutCb(uv_timer_t* handle);
// alloc buf for recv
static void cliAllocRecvBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
// callback after  read nbytes from socket
static void cliRecvCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf);
// callback after write data to socket
static void cliSendCb(uv_write_t* req, int status);
// callback after conn  to server
static void cliConnCb(uv_connect_t* req, int status);
static void cliAsyncCb(uv_async_t* handle);

static SCliConn* cliCreateConn(SCliThrdObj* thrd);
static void      cliDestroyConn(SCliConn* pConn, bool clear /*clear tcp handle or not*/);
static void      cliDestroy(uv_handle_t* handle);

// process data read from server, add decompress etc later
static void cliHandleResp(SCliConn* conn);
// handle except about conn
static void cliHandleExcept(SCliConn* conn);
// handle req from app
static void cliHandleReq(SCliMsg* pMsg, SCliThrdObj* pThrd);
static void cliHandleQuit(SCliMsg* pMsg, SCliThrdObj* pThrd);
static void cliHandleRelease(SCliMsg* pMsg, SCliThrdObj* pThrd);

static void cliSendQuit(SCliThrdObj* thrd);
static void destroyUserdata(STransMsg* userdata);

static int cliRBChoseIdx(STrans* pTransInst);

static void destroyCmsg(SCliMsg* cmsg);
static void transDestroyConnCtx(STransConnCtx* ctx);
// thread obj
static SCliThrdObj* createThrdObj();
static void         destroyThrdObj(SCliThrdObj* pThrd);

#define CONN_HOST_THREAD_INDEX(conn) (conn ? ((SCliConn*)conn)->hThrdIdx : -1)
#define CONN_PERSIST_TIME(para) (para * 1000 * 10)
#define CONN_GET_HOST_THREAD(conn) (conn ? ((SCliConn*)conn)->hostThrd : NULL)
#define CONN_GET_INST_LABEL(conn) (((STrans*)(((SCliThrdObj*)(conn)->hostThrd)->pTransInst))->label)
#define CONN_HANDLE_THREAD_QUIT(conn, thrd) \
  do {                                      \
    if (thrd->quit) {                       \
      cliHandleExcept(conn);                \
      goto _RETURE;                         \
    }                                       \
  } while (0)

#define CONN_HANDLE_BROKEN(conn) \
  do {                           \
    if (conn->broken) {          \
      cliHandleExcept(conn);     \
      goto _RETURE;              \
    }                            \
  } while (0);

#define CONN_SET_PERSIST_BY_APP(conn) \
  do {                                \
    if (conn->persist == false) {     \
      conn->persist = true;           \
      transRefCliHandle(conn);        \
    }                                 \
  } while (0)
#define CONN_NO_PERSIST_BY_APP(conn) ((conn)->persist == false)

static void* cliWorkThread(void* arg);

void cliHandleResp(SCliConn* conn) {
  SCliThrdObj* pThrd = conn->hostThrd;
  STrans*      pTransInst = pThrd->pTransInst;

  STransMsgHead* pHead = (STransMsgHead*)(conn->readBuf.buf);
  pHead->code = htonl(pHead->code);
  pHead->msgLen = htonl(pHead->msgLen);

  STransMsg transMsg = {0};
  transMsg.contLen = transContLenFromMsg(pHead->msgLen);
  transMsg.pCont = transContFromHead((char*)pHead);
  transMsg.code = pHead->code;
  transMsg.msgType = pHead->msgType;
  transMsg.ahandle = NULL;

  SCliMsg*       pMsg = conn->data;
  STransConnCtx* pCtx = pMsg ? pMsg->ctx : NULL;
  if (pMsg == NULL && !CONN_NO_PERSIST_BY_APP(conn)) {
    transMsg.ahandle = pTransInst->mfp ? (*pTransInst->mfp)(pTransInst->parent, transMsg.msgType) : NULL;
  } else {
    transMsg.ahandle = pCtx ? pCtx->ahandle : NULL;
  }
  // if (rpcMsg.ahandle == NULL) {
  //  tDebug("%s cli conn %p handle except", CONN_GET_INST_LABEL(conn), conn);
  //  return;
  //}

  // buf's mem alread translated to transMsg.pCont
  transClearBuffer(&conn->readBuf);

  if (pTransInst->pfp != NULL && (*pTransInst->pfp)(pTransInst->parent, transMsg.msgType)) {
    transMsg.handle = conn;
    CONN_SET_PERSIST_BY_APP(conn);
    tDebug("%s cli conn %p ref by app", CONN_GET_INST_LABEL(conn), conn);
  }

  tDebug("%s cli conn %p %s received from %s:%d, local info: %s:%d, msg size: %d", pTransInst->label, conn,
         TMSG_INFO(pHead->msgType), inet_ntoa(conn->addr.sin_addr), ntohs(conn->addr.sin_port),
         inet_ntoa(conn->locaddr.sin_addr), ntohs(conn->locaddr.sin_port), transMsg.contLen);

  conn->secured = pHead->secured;

  if (pCtx == NULL && CONN_NO_PERSIST_BY_APP(conn)) {
    tTrace("except, server continue send while cli ignore it");
    // transUnrefCliHandle(conn);
    return;
  }

  if (pCtx == NULL || pCtx->pSem == NULL) {
    tTrace("%s cli conn %p handle resp", pTransInst->label, conn);
    (pTransInst->cfp)(pTransInst->parent, &transMsg, NULL);
  } else {
    tTrace("%s cli conn(sync) %p handle resp", pTransInst->label, conn);
    memcpy((char*)pCtx->pRsp, (char*)&transMsg, sizeof(transMsg));
    tsem_post(pCtx->pSem);
  }

  if (CONN_NO_PERSIST_BY_APP(conn)) {
    addConnToPool(pThrd->pool, conn);
  }
  destroyCmsg(conn->data);
  conn->data = NULL;

  uv_read_start((uv_stream_t*)conn->stream, cliAllocRecvBufferCb, cliRecvCb);
  // start thread's timer of conn pool if not active
  if (!uv_is_active((uv_handle_t*)&pThrd->timer) && pTransInst->idleTime > 0) {
    // uv_timer_start((uv_timer_t*)&pThrd->timer, cliTimeoutCb, CONN_PERSIST_TIME(pRpc->idleTime) / 2, 0);
  }
}

void cliHandleExcept(SCliConn* pConn) {
  if (pConn->data == NULL) {
    if (pConn->broken == true || CONN_NO_PERSIST_BY_APP(pConn)) {
      transUnrefCliHandle(pConn);
      return;
    }
  }
  SCliThrdObj* pThrd = pConn->hostThrd;
  STrans*      pTransInst = pThrd->pTransInst;

  SCliMsg*       pMsg = pConn->data;
  STransConnCtx* pCtx = pMsg ? pMsg->ctx : NULL;

  STransMsg transMsg = {0};
  transMsg.code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
  transMsg.msgType = pMsg ? pMsg->msg.msgType + 1 : 0;
  transMsg.ahandle = NULL;

  if (pMsg == NULL && !CONN_NO_PERSIST_BY_APP(pConn)) {
    transMsg.ahandle = pTransInst->mfp ? (*pTransInst->mfp)(pTransInst->parent, transMsg.msgType) : NULL;
  } else {
    transMsg.ahandle = pCtx ? pCtx->ahandle : NULL;
  }

  if (pCtx == NULL || pCtx->pSem == NULL) {
    tTrace("%s cli conn %p handle resp", pTransInst->label, pConn);
    (pTransInst->cfp)(pTransInst->parent, &transMsg, NULL);
  } else {
    tTrace("%s cli conn(sync) %p handle resp", pTransInst->label, pConn);
    memcpy((char*)(pCtx->pRsp), (char*)(&transMsg), sizeof(transMsg));
    tsem_post(pCtx->pSem);
  }
  destroyCmsg(pConn->data);
  pConn->data = NULL;

  tTrace("%s cli conn %p start to destroy", CONN_GET_INST_LABEL(pConn), pConn);
  transUnrefCliHandle(pConn);
}

void cliTimeoutCb(uv_timer_t* handle) {
  SCliThrdObj* pThrd = handle->data;
  STrans*      pTransInst = pThrd->pTransInst;
  int64_t      currentTime = pThrd->nextTimeout;
  tTrace("%s, cli conn timeout, try to remove expire conn from conn pool", pTransInst->label);

  SConnList* p = taosHashIterate((SHashObj*)pThrd->pool, NULL);
  while (p != NULL) {
    while (!QUEUE_IS_EMPTY(&p->conn)) {
      queue*    h = QUEUE_HEAD(&p->conn);
      SCliConn* c = QUEUE_DATA(h, SCliConn, conn);
      if (c->expireTime < currentTime) {
        QUEUE_REMOVE(h);
        transUnrefCliHandle(c);
      } else {
        break;
      }
    }
    p = taosHashIterate((SHashObj*)pThrd->pool, p);
  }

  pThrd->nextTimeout = taosGetTimestampMs() + CONN_PERSIST_TIME(pTransInst->idleTime);
  uv_timer_start(handle, cliTimeoutCb, CONN_PERSIST_TIME(pTransInst->idleTime) / 2, 0);
}

void* createConnPool(int size) {
  // thread local, no lock
  return taosHashInit(size, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
}
void* destroyConnPool(void* pool) {
  SConnList* connList = taosHashIterate((SHashObj*)pool, NULL);
  while (connList != NULL) {
    while (!QUEUE_IS_EMPTY(&connList->conn)) {
      queue* h = QUEUE_HEAD(&connList->conn);
      QUEUE_REMOVE(h);
      SCliConn* c = QUEUE_DATA(h, SCliConn, conn);
      cliDestroyConn(c, true);
    }
    connList = taosHashIterate((SHashObj*)pool, connList);
  }
  taosHashCleanup(pool);
  return NULL;
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
static void addConnToPool(void* pool, SCliConn* conn) {
  char key[128] = {0};

  tstrncpy(key, conn->ip, strlen(conn->ip));
  tstrncpy(key + strlen(key), (char*)(&conn->port), sizeof(conn->port));
  tTrace("cli conn %p added to conn pool, read buf cap: %d", conn, conn->readBuf.cap);

  STrans* pTransInst = ((SCliThrdObj*)conn->hostThrd)->pTransInst;

  conn->expireTime = taosGetTimestampMs() + CONN_PERSIST_TIME(pTransInst->idleTime);
  SConnList* plist = taosHashGet((SHashObj*)pool, key, strlen(key));
  // list already create before
  assert(plist != NULL);
  QUEUE_PUSH(&plist->conn, &conn->conn);
}
static void cliAllocRecvBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  SCliConn*    conn = handle->data;
  SConnBuffer* pBuf = &conn->readBuf;
  // avoid conn
  QUEUE_REMOVE(&conn->conn);
  transAllocBuffer(pBuf, buf);
}
static void cliRecvCb(uv_stream_t* handle, ssize_t nread, const uv_buf_t* buf) {
  // impl later
  if (handle->data == NULL) {
    return;
  }
  SCliConn*    conn = handle->data;
  SConnBuffer* pBuf = &conn->readBuf;
  if (nread > 0) {
    pBuf->len += nread;
    if (transReadComplete(pBuf)) {
      tTrace("%s cli conn %p read complete", CONN_GET_INST_LABEL(conn), conn);
      cliHandleResp(conn);
    } else {
      tTrace("%s cli conn %p read partial packet, continue to read", CONN_GET_INST_LABEL(conn), conn);
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
  if (nread < 0) {
    tError("%s cli conn %p read error: %s", CONN_GET_INST_LABEL(conn), conn, uv_err_name(nread));
    conn->broken = true;
    cliHandleExcept(conn);
  }
}

static SCliConn* cliCreateConn(SCliThrdObj* pThrd) {
  SCliConn* conn = calloc(1, sizeof(SCliConn));
  // read/write stream handle
  conn->stream = (uv_stream_t*)malloc(sizeof(uv_tcp_t));
  uv_tcp_init(pThrd->loop, (uv_tcp_t*)(conn->stream));
  conn->stream->data = conn;

  conn->writeReq.data = conn;
  conn->connReq.data = conn;

  QUEUE_INIT(&conn->conn);
  conn->hostThrd = pThrd;
  conn->persist = false;
  conn->broken = false;
  transRefCliHandle(conn);
  return conn;
}
static void cliDestroyConn(SCliConn* conn, bool clear) {
  tTrace("%s cli conn %p remove from conn pool", CONN_GET_INST_LABEL(conn), conn);
  QUEUE_REMOVE(&conn->conn);
  if (clear) {
    uv_close((uv_handle_t*)conn->stream, cliDestroy);
  }
}
static void cliDestroy(uv_handle_t* handle) {
  SCliConn* conn = handle->data;
  free(conn->ip);
  free(conn->stream);
  tTrace("%s cli conn %p destroy successfully", CONN_GET_INST_LABEL(conn), conn);
  free(conn);
}

static void cliSendCb(uv_write_t* req, int status) {
  SCliConn* pConn = req->data;

  if (status == 0) {
    tTrace("%s cli conn %p data already was written out", CONN_GET_INST_LABEL(pConn), pConn);
    SCliMsg* pMsg = pConn->data;
    if (pMsg == NULL) {
      return;
    }
    destroyUserdata(&pMsg->msg);
  } else {
    tError("%s cli conn %p failed to write: %s", CONN_GET_INST_LABEL(pConn), pConn, uv_err_name(status));
    cliHandleExcept(pConn);
    return;
  }
  uv_read_start((uv_stream_t*)pConn->stream, cliAllocRecvBufferCb, cliRecvCb);
}

void cliSend(SCliConn* pConn) {
  CONN_HANDLE_BROKEN(pConn);

  SCliMsg*       pCliMsg = pConn->data;
  STransConnCtx* pCtx = pCliMsg->ctx;

  SCliThrdObj* pThrd = pConn->hostThrd;
  STrans*      pTransInst = pThrd->pTransInst;

  STransMsg* pMsg = (STransMsg*)(&pCliMsg->msg);

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

  uv_buf_t wb = uv_buf_init((char*)pHead, msgLen);
  tDebug("%s cli conn %p %s is send to %s:%d, local info %s:%d", CONN_GET_INST_LABEL(pConn), pConn,
         TMSG_INFO(pHead->msgType), inet_ntoa(pConn->addr.sin_addr), ntohs(pConn->addr.sin_port),
         inet_ntoa(pConn->locaddr.sin_addr), ntohs(pConn->locaddr.sin_port));

  uv_write(&pConn->writeReq, (uv_stream_t*)pConn->stream, &wb, 1, cliSendCb);

  return;
_RETURE:
  return;
}

void cliConnCb(uv_connect_t* req, int status) {
  // impl later
  SCliConn* pConn = req->data;
  if (status != 0) {
    tError("%s cli conn %p failed to connect server: %s", CONN_GET_INST_LABEL(pConn), pConn, uv_strerror(status));
    cliHandleExcept(pConn);
    return;
  }
  int addrlen = sizeof(pConn->addr);
  uv_tcp_getpeername((uv_tcp_t*)pConn->stream, (struct sockaddr*)&pConn->addr, &addrlen);

  addrlen = sizeof(pConn->locaddr);
  uv_tcp_getsockname((uv_tcp_t*)pConn->stream, (struct sockaddr*)&pConn->locaddr, &addrlen);

  tTrace("%s cli conn %p connect to server successfully", CONN_GET_INST_LABEL(pConn), pConn);

  assert(pConn->stream == req->handle);
  cliSend(pConn);
}

static void cliHandleQuit(SCliMsg* pMsg, SCliThrdObj* pThrd) {
  tDebug("cli work thread %p start to quit", pThrd);
  destroyCmsg(pMsg);
  destroyConnPool(pThrd->pool);

  uv_timer_stop(&pThrd->timer);

  pThrd->quit = true;
  uv_stop(pThrd->loop);
}
static void cliHandleRelease(SCliMsg* pMsg, SCliThrdObj* pThrd) {
  SCliConn* conn = pMsg->msg.handle;
  tDebug("%s cli conn %p release to inst", CONN_GET_INST_LABEL(conn), conn);

  destroyCmsg(pMsg);
  conn->data = NULL;

  transDestroyBuffer(&conn->readBuf);
  if (conn->persist && T_REF_VAL_GET(conn) >= 2) {
    conn->persist = false;
    transUnrefCliHandle(conn);
    addConnToPool(pThrd->pool, conn);
  } else {
    transUnrefCliHandle(conn);
  }
}

SCliConn* cliGetConn(SCliMsg* pMsg, SCliThrdObj* pThrd) {
  SCliConn* conn = NULL;
  if (pMsg->msg.handle != NULL) {
    conn = (SCliConn*)(pMsg->msg.handle);
    if (conn != NULL) {
      tTrace("%s cli conn %p reused", CONN_GET_INST_LABEL(conn), conn);
    }
  } else {
    STransConnCtx* pCtx = pMsg->ctx;
    conn = getConnFromPool(pThrd->pool, pCtx->ip, pCtx->port);
    if (conn != NULL) {
      tTrace("%s cli conn %p get from conn pool", CONN_GET_INST_LABEL(conn), conn);
    }
  }
  return conn;
}

void cliHandleReq(SCliMsg* pMsg, SCliThrdObj* pThrd) {
  uint64_t et = taosGetTimestampUs();
  uint64_t el = et - pMsg->st;
  tTrace("%s cli msg tran time cost: %" PRIu64 "us", ((STrans*)pThrd->pTransInst)->label, el);

  STransConnCtx* pCtx = pMsg->ctx;
  STrans*        pTransInst = pThrd->pTransInst;

  SCliConn* conn = cliGetConn(pMsg, pThrd);
  if (conn != NULL) {
    conn->data = pMsg;
    conn->hThrdIdx = pCtx->hThrdIdx;

    transDestroyBuffer(&conn->readBuf);
    cliSend(conn);
  } else {
    conn = cliCreateConn(pThrd);
    conn->data = pMsg;
    conn->hThrdIdx = pCtx->hThrdIdx;
    conn->ip = strdup(pMsg->ctx->ip);
    conn->port = pMsg->ctx->port;

    int ret = transSetConnOption((uv_tcp_t*)conn->stream);
    if (ret) {
      tError("%s cli conn %p failed to set conn option, errmsg %s", pTransInst->label, conn, uv_err_name(ret));
    }
    struct sockaddr_in addr;
    uv_ip4_addr(pMsg->ctx->ip, pMsg->ctx->port, &addr);
    // handle error in callback if fail to connect
    tTrace("%s cli conn %p try to connect to %s:%d", pTransInst->label, conn, pMsg->ctx->ip, pMsg->ctx->port);
    uv_tcp_connect(&conn->connReq, (uv_tcp_t*)(conn->stream), (const struct sockaddr*)&addr, cliConnCb);
  }
}
static void cliAsyncCb(uv_async_t* handle) {
  SAsyncItem*  item = handle->data;
  SCliThrdObj* pThrd = item->pThrd;
  SCliMsg*     pMsg = NULL;

  // batch process to avoid to lock/unlock frequently
  queue wq;
  pthread_mutex_lock(&item->mtx);
  QUEUE_MOVE(&item->qmsg, &wq);
  pthread_mutex_unlock(&item->mtx);

  int count = 0;
  while (!QUEUE_IS_EMPTY(&wq)) {
    queue* h = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(h);

    SCliMsg* pMsg = QUEUE_DATA(h, SCliMsg, q);

    if (pMsg->type == Normal) {
      cliHandleReq(pMsg, pThrd);
    } else if (pMsg->type == Quit) {
      cliHandleQuit(pMsg, pThrd);
    } else if (pMsg->type == Release) {
      cliHandleRelease(pMsg, pThrd);
    }
    count++;
  }
  if (count >= 2) {
    tTrace("cli process batch size: %d", count);
  }
}

static void* cliWorkThread(void* arg) {
  SCliThrdObj* pThrd = (SCliThrdObj*)arg;
  setThreadName("trans-cli-work");
  uv_run(pThrd->loop, UV_RUN_DEFAULT);

  return NULL;
}

void* transInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle) {
  SCliObj* cli = calloc(1, sizeof(SCliObj));

  STrans* pTransInst = shandle;
  memcpy(cli->label, label, strlen(label));
  cli->numOfThreads = numOfThreads;
  cli->pThreadObj = (SCliThrdObj**)calloc(cli->numOfThreads, sizeof(SCliThrdObj*));

  for (int i = 0; i < cli->numOfThreads; i++) {
    SCliThrdObj* pThrd = createThrdObj();
    pThrd->nextTimeout = taosGetTimestampMs() + CONN_PERSIST_TIME(pTransInst->idleTime);
    pThrd->pTransInst = shandle;

    int err = pthread_create(&pThrd->thread, NULL, cliWorkThread, (void*)(pThrd));
    if (err == 0) {
      tDebug("success to create tranport-cli thread %d", i);
    }
    cli->pThreadObj[i] = pThrd;
  }
  return cli;
}

static void destroyUserdata(STransMsg* userdata) {
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

  pThrd->asyncPool = transCreateAsyncPool(pThrd->loop, 5, pThrd, cliAsyncCb);

  uv_timer_init(pThrd->loop, &pThrd->timer);
  pThrd->timer.data = pThrd;

  pThrd->pool = createConnPool(4);

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

  uv_timer_stop(&pThrd->timer);
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
void cliSendQuit(SCliThrdObj* thrd) {
  // cli can stop gracefully
  SCliMsg* msg = calloc(1, sizeof(SCliMsg));
  msg->type = Quit;
  transSendAsync(thrd->asyncPool, &msg->q);
}

int cliRBChoseIdx(STrans* pTransInst) {
  int64_t index = pTransInst->index;
  if (pTransInst->index++ >= pTransInst->numOfThreads) {
    pTransInst->index = 0;
  }
  return index % pTransInst->numOfThreads;
}

void transCloseClient(void* arg) {
  SCliObj* cli = arg;
  for (int i = 0; i < cli->numOfThreads; i++) {
    cliSendQuit(cli->pThreadObj[i]);
    destroyThrdObj(cli->pThreadObj[i]);
  }
  free(cli->pThreadObj);
  free(cli);
}
void transRefCliHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  int ref = T_REF_INC((SCliConn*)handle);
  UNUSED(ref);
}
void transUnrefCliHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  int ref = T_REF_DEC((SCliConn*)handle);
  tDebug("%s cli conn %p ref %d", CONN_GET_INST_LABEL((SCliConn*)handle), handle, ref);
  if (ref == 0) {
    cliDestroyConn((SCliConn*)handle, true);
  }
}
void transReleaseCliHandle(void* handle) {
  SCliThrdObj* thrd = CONN_GET_HOST_THREAD(handle);
  if (thrd == NULL) {
    return;
  }

  STransMsg tmsg = {.handle = handle};
  SCliMsg*  cmsg = calloc(1, sizeof(SCliMsg));
  cmsg->type = Release;
  cmsg->msg = tmsg;

  transSendAsync(thrd->asyncPool, &cmsg->q);
}

void transSendRequest(void* shandle, const char* ip, uint32_t port, STransMsg* pMsg) {
  STrans* pTransInst = (STrans*)shandle;
  int     index = CONN_HOST_THREAD_INDEX((SCliConn*)pMsg->handle);
  if (index == -1) {
    index = cliRBChoseIdx(pTransInst);
  }
  int32_t flen = 0;
  if (transCompressMsg(pMsg->pCont, pMsg->contLen, &flen)) {
    // imp later
  }
  tDebug("send request at thread:%d %p", index, pMsg);
  STransConnCtx* pCtx = calloc(1, sizeof(STransConnCtx));
  pCtx->ahandle = pMsg->ahandle;
  pCtx->msgType = pMsg->msgType;
  pCtx->ip = strdup(ip);
  pCtx->port = port;
  pCtx->hThrdIdx = index;

  assert(pTransInst->connType == TAOS_CONN_CLIENT);
  // atomic or not

  SCliMsg* cliMsg = calloc(1, sizeof(SCliMsg));
  cliMsg->ctx = pCtx;
  cliMsg->msg = *pMsg;
  cliMsg->st = taosGetTimestampUs();

  SCliThrdObj* thrd = ((SCliObj*)pTransInst->tcphandle)->pThreadObj[index];
  transSendAsync(thrd->asyncPool, &(cliMsg->q));
}
void transSendRecv(void* shandle, const char* ip, uint32_t port, STransMsg* pReq, STransMsg* pRsp) {
  STrans* pTransInst = (STrans*)shandle;
  int     index = CONN_HOST_THREAD_INDEX(pReq->handle);
  if (index == -1) {
    index = cliRBChoseIdx(pTransInst);
  }

  STransConnCtx* pCtx = calloc(1, sizeof(STransConnCtx));
  pCtx->ahandle = pReq->ahandle;
  pCtx->msgType = pReq->msgType;
  pCtx->ip = strdup(ip);
  pCtx->port = port;
  pCtx->hThrdIdx = index;
  pCtx->pSem = calloc(1, sizeof(tsem_t));
  pCtx->pRsp = pRsp;
  tsem_init(pCtx->pSem, 0, 0);

  SCliMsg* cliMsg = calloc(1, sizeof(SCliMsg));
  cliMsg->ctx = pCtx;
  cliMsg->msg = *pReq;
  cliMsg->st = taosGetTimestampUs();

  SCliThrdObj* thrd = ((SCliObj*)pTransInst->tcphandle)->pThreadObj[index];
  transSendAsync(thrd->asyncPool, &(cliMsg->q));
  tsem_t* pSem = pCtx->pSem;
  tsem_wait(pSem);
  tsem_destroy(pSem);
  free(pSem);
}

#endif
