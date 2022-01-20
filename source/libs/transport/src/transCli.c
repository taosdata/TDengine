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
  SConnBuffer  readBuf;
  void*        data;
  queue        conn;
  char         spi;
  char         secured;
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

typedef struct SConnList {
  queue conn;
} SConnList;

// conn pool
// add expire timeout and capacity limit
static void*     connCacheCreate(int size);
static void*     connCacheDestroy(void* cache);
static SCliConn* getConnFromCache(void* cache, char* ip, uint32_t port);
static void      addConnToCache(void* cache, char* ip, uint32_t port, SCliConn* conn);

// process data read from server, auth/decompress etc
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

static void* clientThread(void* arg);

static void clientProcessData(SCliConn* conn) {
  // impl
}
static void clientHandleReq(SCliMsg* pMsg, SCliThrdObj* pThrd);

static void* connCacheCreate(int size) {
  SHashObj* cache = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  return false;
}
static void* connCacheDestroy(void* cache) {
  SConnList* connList = taosHashIterate((SHashObj*)cache, NULL);
  while (!QUEUE_IS_EMPTY(&connList->conn)) {
    queue* h = QUEUE_HEAD(&connList->conn);
    QUEUE_REMOVE(h);
    SCliConn* c = QUEUE_DATA(h, SCliConn, conn);
    clientConnDestroy(c);
  }
  taosHashClear(cache);
}

static SCliConn* getConnFromCache(void* cache, char* ip, uint32_t port) {
  char key[128] = {0};
  tstrncpy(key, ip, strlen(ip));
  tstrncpy(key + strlen(key), (char*)(&port), sizeof(port));

  SHashObj*  pCache = cache;
  SConnList* plist = taosHashGet(pCache, key, strlen(key));
  if (plist == NULL) {
    SConnList list;
    plist = &list;
    QUEUE_INIT(&plist->conn);
    taosHashPut(pCache, key, strlen(key), plist, sizeof(*plist));
  }

  if (QUEUE_IS_EMPTY(&plist->conn)) {
    return NULL;
  }
  queue* h = QUEUE_HEAD(&plist->conn);
  QUEUE_REMOVE(h);
  return QUEUE_DATA(h, SCliConn, conn);
}
static void addConnToCache(void* cache, char* ip, uint32_t port, SCliConn* conn) {
  char key[128] = {0};
  tstrncpy(key, ip, strlen(ip));
  tstrncpy(key + strlen(key), (char*)(&port), sizeof(port));

  SHashObj*  pCache = cache;
  SConnList* plist = taosHashGet(pCache, key, strlen(key));
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
    pBuf->buf = (char*)calloc(CAPACITY, sizeof(char));
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
        pBuf->buf = realloc(pBuf->buf, pBuf->len + pBuf->left);
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
      tDebug("alread read complete pack");
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

  uv_read_start((uv_stream_t*)pConn->stream, clientAllocReadBufferCb, clientReadCb);
  // impl later
}

static void clientWrite(SCliConn* pConn) {
  SCliMsg*       pCliMsg = pConn->data;
  SRpcMsg*       pMsg = (SRpcMsg*)(&pCliMsg->msg);
  STransMsgHead* pHead = transHeadFromCont(pMsg->pCont);

  int   msgLen = transMsgLenFromCont(pMsg->contLen);
  char* msg = (char*)(pHead);

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

  SCliMsg*       pMsg = pConn->data;
  STransConnCtx* pCtx = ((SCliMsg*)(pConn->data))->ctx;

  SRpcMsg rpcMsg;
  rpcMsg.ahandle = pCtx->ahandle;

  if (status != 0) {
    // call user fp later
    tError("failed to connect server(%s, %d), errmsg: %s", pCtx->ip, pCtx->port, uv_strerror(status));
    SRpcInfo* pRpc = pMsg->ctx->pRpc;
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
  SCliConn*      conn = getConnFromCache(pThrd->cache, pCtx->ip, pCtx->port);
  if (conn != NULL) {
    // impl later
    conn->data = pMsg;
    conn->writeReq->data = conn;
    clientWrite(conn);
  } else {
    SCliConn* conn = calloc(1, sizeof(SCliConn));

    conn->stream = (uv_stream_t*)malloc(sizeof(uv_tcp_t));
    uv_tcp_init(pThrd->loop, (uv_tcp_t*)(conn->stream));
    conn->writeReq = malloc(sizeof(uv_write_t));

    conn->connReq.data = conn;
    conn->data = pMsg;

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
static void clientMsgDestroy(SCliMsg* pMsg) {
  // impl later
  free(pMsg);
}
void taosCloseClient(void* arg) {
  // impl later
  SClientObj* cli = arg;
  for (int i = 0; i < cli->numOfThreads; i++) {
    SCliThrdObj* pThrd = cli->pThreadObj[i];
    pthread_join(pThrd->thread, NULL);
    pthread_mutex_destroy(&pThrd->msgMtx);
    free(pThrd->cliAsync);
    free(pThrd->loop);
    free(pThrd);
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
