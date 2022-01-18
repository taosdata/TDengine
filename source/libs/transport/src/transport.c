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

#include <uv.h>
#include "lz4.h"
#include "os.h"
#include "rpcCache.h"
#include "rpcHead.h"
#include "rpcLog.h"
#include "rpcTcp.h"
#include "rpcUdp.h"
#include "taoserror.h"
#include "tglobal.h"
#include "thash.h"
#include "tidpool.h"
#include "tmd5.h"
#include "tmempool.h"
#include "tmsg.h"
#include "transportInt.h"
#include "tref.h"
#include "trpc.h"
#include "ttimer.h"
#include "tutil.h"

#define container_of(ptr, type, member) ((type*)((char*)(ptr)-offsetof(type, member)))
#define RPC_RESERVE_SIZE (sizeof(SRpcReqContext))
static const char* notify = "a";

typedef struct {
  int      sessions;      // number of sessions allowed
  int      numOfThreads;  // number of threads to process incoming messages
  int      idleTime;      // milliseconds;
  uint16_t localPort;
  int8_t   connType;
  int      index;  // for UDP server only, round robin for multiple threads
  char     label[TSDB_LABEL_LEN];

  char user[TSDB_UNI_LEN];         // meter ID
  char spi;                        // security parameter index
  char encrypt;                    // encrypt algorithm
  char secret[TSDB_PASSWORD_LEN];  // secret for the link
  char ckey[TSDB_PASSWORD_LEN];    // ciphering key

  void (*cfp)(void* parent, SRpcMsg*, SEpSet*);
  int (*afp)(void* parent, char* user, char* spi, char* encrypt, char* secret, char* ckey);

  int32_t          refCount;
  void*            parent;
  void*            idPool;     // handle to ID pool
  void*            tmrCtrl;    // handle to timer
  SHashObj*        hash;       // handle returned by hash utility
  void*            tcphandle;  // returned handle from TCP initialization
  void*            udphandle;  // returned handle from UDP initialization
  void*            pCache;     // connection cache
  pthread_mutex_t  mutex;
  struct SRpcConn* connList;  // connection list
} SRpcInfo;

typedef struct {
  SRpcInfo*        pRpc;      // associated SRpcInfo
  SEpSet           epSet;     // ip list provided by app
  void*            ahandle;   // handle provided by app
  struct SRpcConn* pConn;     // pConn allocated
  tmsg_t           msgType;   // message type
  uint8_t*         pCont;     // content provided by app
  int32_t          contLen;   // content length
  int32_t          code;      // error code
  int16_t          numOfTry;  // number of try for different servers
  int8_t           oldInUse;  // server EP inUse passed by app
  int8_t           redirect;  // flag to indicate redirect
  int8_t           connType;  // connection type
  int64_t          rid;       // refId returned by taosAddRef
  SRpcMsg*         pRsp;      // for synchronous API
  tsem_t*          pSem;      // for synchronous API
  SEpSet*          pSet;      // for synchronous API
  char             msg[0];    // RpcHead starts from here
} SRpcReqContext;

typedef struct SThreadObj {
  pthread_t       thread;
  uv_pipe_t*      pipe;
  int             fd;
  uv_loop_t*      loop;
  uv_async_t*     workerAsync;  //
  queue           conn;
  pthread_mutex_t connMtx;
  void*           shandle;
} SThreadObj;

typedef struct SClientObj {
  char         label[TSDB_LABEL_LEN];
  int32_t      index;
  int          numOfThreads;
  SThreadObj** pThreadObj;
} SClientObj;

#define RPC_MSG_OVERHEAD (sizeof(SRpcReqContext) + sizeof(SRpcHead) + sizeof(SRpcDigest))
#define rpcHeadFromCont(cont) ((SRpcHead*)((char*)cont - sizeof(SRpcHead)))
#define rpcContFromHead(msg) (msg + sizeof(SRpcHead))
#define rpcMsgLenFromCont(contLen) (contLen + sizeof(SRpcHead))
#define rpcContLenFromMsg(msgLen) (msgLen - sizeof(SRpcHead))
#define rpcIsReq(type) (type & 1U)

typedef struct SServerObj {
  pthread_t    thread;
  uv_tcp_t     server;
  uv_loop_t*   loop;
  int          workerIdx;
  int          numOfThreads;
  SThreadObj** pThreadObj;
  uv_pipe_t**  pipe;
  uint32_t     ip;
  uint32_t     port;
} SServerObj;

typedef struct SConnBuffer {
  char* buf;
  int   len;
  int   cap;
  int   left;
} SConnBuffer;

typedef struct SRpcConn {
  uv_tcp_t*   pTcp;
  uv_write_t* pWriter;
  uv_timer_t* pTimer;

  uv_async_t* pWorkerAsync;
  queue       queue;
  int         ref;
  int         persist;   // persist connection or not
  SConnBuffer connBuf;   // read buf,
  SConnBuffer writeBuf;  // write buf
  int         count;
  void*       shandle;  // rpc init
  void*       ahandle;  //
  void*       hostThread;
  // del later
  char secured;
  int  spi;
  char info[64];
  char user[TSDB_UNI_LEN];  // user ID for the link
  char secret[TSDB_PASSWORD_LEN];
  char ckey[TSDB_PASSWORD_LEN];  // ciphering key
} SRpcConn;

// auth function
static int  uvAuthMsg(SRpcConn* pConn, char* msg, int msgLen);
static int  rpcAuthenticateMsg(void* pMsg, int msgLen, void* pAuth, void* pKey);
static void rpcBuildAuthHead(void* pMsg, int msgLen, void* pAuth, void* pKey);
static int  rpcAddAuthPart(SRpcConn* pConn, char* msg, int msgLen);
// compress data
static int32_t   rpcCompressRpcMsg(char* pCont, int32_t contLen);
static SRpcHead* rpcDecompressRpcMsg(SRpcHead* pHead);

static void uvAllocConnBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
static void uvAllocReadBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
static void uvOnReadCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf);
static void uvOnTimeoutCb(uv_timer_t* handle);
static void uvOnWriteCb(uv_write_t* req, int status);
static void uvOnAcceptCb(uv_stream_t* stream, int status);
static void uvOnConnectionCb(uv_stream_t* q, ssize_t nread, const uv_buf_t* buf);
static void uvWorkerAsyncCb(uv_async_t* handle);

static SRpcConn* connCreate();
static void      connDestroy(SRpcConn* conn);
static void      uvConnDestroy(uv_handle_t* handle);

static void* workerThread(void* arg);
static void* acceptThread(void* arg);

void* taosInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle);
void* taosInitServer(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle);

void* (*taosHandle[])(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle) = {taosInitServer, taosInitClient};

void* taosInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle) {
  SClientObj* cli = calloc(1, sizeof(SClientObj));
  memcpy(cli->label, label, strlen(label));
  cli->numOfThreads = numOfThreads;
  cli->pThreadObj = (SThreadObj**)calloc(cli->numOfThreads, sizeof(SThreadObj*));

  for (int i = 0; i < cli->numOfThreads; i++) {
    SThreadObj* thrd = (SThreadObj*)calloc(1, sizeof(SThreadObj));

    int err = pthread_create(&thrd->thread, NULL, workerThread, (void*)(thrd));
    if (err == 0) {
      tDebug("sucess to create tranport-client thread %d", i);
    }
  }
  return cli;
}

void* taosInitServer(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle) {
  SServerObj* srv = calloc(1, sizeof(SServerObj));
  srv->loop = (uv_loop_t*)malloc(sizeof(uv_loop_t));
  srv->numOfThreads = numOfThreads;
  srv->workerIdx = 0;
  srv->pThreadObj = (SThreadObj**)calloc(srv->numOfThreads, sizeof(SThreadObj*));
  srv->pipe = (uv_pipe_t**)calloc(srv->numOfThreads, sizeof(uv_pipe_t*));
  srv->ip = ip;
  srv->port = port;
  uv_loop_init(srv->loop);

  for (int i = 0; i < srv->numOfThreads; i++) {
    SThreadObj* thrd = (SThreadObj*)calloc(1, sizeof(SThreadObj));
    srv->pipe[i] = (uv_pipe_t*)calloc(2, sizeof(uv_pipe_t));
    int fds[2];
    if (uv_socketpair(AF_UNIX, SOCK_STREAM, fds, UV_NONBLOCK_PIPE, UV_NONBLOCK_PIPE) != 0) {
      return NULL;
    }
    uv_pipe_init(srv->loop, &(srv->pipe[i][0]), 1);
    uv_pipe_open(&(srv->pipe[i][0]), fds[1]);  // init write

    thrd->shandle = shandle;
    thrd->fd = fds[0];
    thrd->pipe = &(srv->pipe[i][1]);  // init read
    int err = pthread_create(&(thrd->thread), NULL, workerThread, (void*)(thrd));
    if (err == 0) {
      tDebug("sucess to create worker-thread %d", i);
      // printf("thread %d create\n", i);
    } else {
      // TODO: clear all other resource later
      tError("failed to create worker-thread %d", i);
    }
    srv->pThreadObj[i] = thrd;
  }

  int err = pthread_create(&srv->thread, NULL, acceptThread, (void*)srv);
  if (err == 0) {
    tDebug("success to create accept-thread");
  } else {
    // clear all resource later
  }

  return srv;
}
void uvAllocReadBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  /*
   * formate of data buffer:
   * |<-------SRpcReqContext------->|<------------data read from socket----------->|
   */
  static const int CAPACITY = 1024;

  SRpcConn*    ctx = handle->data;
  SConnBuffer* pBuf = &ctx->connBuf;
  if (pBuf->cap == 0) {
    pBuf->buf = (char*)calloc(CAPACITY + RPC_RESERVE_SIZE, sizeof(char));
    pBuf->len = 0;
    pBuf->cap = CAPACITY;
    pBuf->left = -1;

    buf->base = pBuf->buf + RPC_RESERVE_SIZE;
    buf->len = CAPACITY;
  } else {
    if (pBuf->len >= pBuf->cap) {
      if (pBuf->left == -1) {
        pBuf->cap *= 2;
        pBuf->buf = realloc(pBuf->buf, pBuf->cap + RPC_RESERVE_SIZE);
      } else if (pBuf->len + pBuf->left > pBuf->cap) {
        pBuf->cap = pBuf->len + pBuf->left;
        pBuf->buf = realloc(pBuf->buf, pBuf->len + pBuf->left + RPC_RESERVE_SIZE);
      }
    }
    buf->base = pBuf->buf + pBuf->len + RPC_RESERVE_SIZE;
    buf->len = pBuf->cap - pBuf->len;
  }
}
// check data read from socket completely or not
//
static bool isReadAll(SConnBuffer* data) {
  // TODO(yihao): handle pipeline later
  SRpcHead rpcHead;
  int32_t  headLen = sizeof(rpcHead);
  if (data->len >= headLen) {
    memcpy((char*)&rpcHead, data->buf + RPC_RESERVE_SIZE, headLen);
    int32_t msgLen = (int32_t)htonl((uint32_t)rpcHead.msgLen);
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
static void uvDoProcess(SRecvInfo* pRecv) {
  SRpcHead* pHead = (SRpcHead*)pRecv->msg;
  SRpcInfo* pRpc = (SRpcInfo*)pRecv->shandle;
  SRpcConn* pConn = pRecv->thandle;

  tDump(pRecv->msg, pRecv->msgLen);

  terrno = 0;
  SRpcReqContext* pContest;

  // do auth and check
}
static int uvAuthMsg(SRpcConn* pConn, char* msg, int len) {
  SRpcHead* pHead = (SRpcHead*)msg;
  int       code = 0;

  if ((pConn->secured && pHead->spi == 0) || (pHead->spi == 0 && pConn->spi == 0)) {
    // secured link, or no authentication
    pHead->msgLen = (int32_t)htonl((uint32_t)pHead->msgLen);
    // tTrace("%s, secured link, no auth is required", pConn->info);
    return 0;
  }

  if (!rpcIsReq(pHead->msgType)) {
    // for response, if code is auth failure, it shall bypass the auth process
    code = htonl(pHead->code);
    if (code == TSDB_CODE_RPC_INVALID_TIME_STAMP || code == TSDB_CODE_RPC_AUTH_FAILURE || code == TSDB_CODE_RPC_INVALID_VERSION || code == TSDB_CODE_RPC_AUTH_REQUIRED ||
        code == TSDB_CODE_MND_USER_NOT_EXIST || code == TSDB_CODE_RPC_NOT_READY) {
      pHead->msgLen = (int32_t)htonl((uint32_t)pHead->msgLen);
      // tTrace("%s, dont check authentication since code is:0x%x", pConn->info, code);
      return 0;
    }
  }

  code = 0;
  if (pHead->spi == pConn->spi) {
    // authentication
    SRpcDigest* pDigest = (SRpcDigest*)((char*)pHead + len - sizeof(SRpcDigest));

    int32_t delta;
    delta = (int32_t)htonl(pDigest->timeStamp);
    delta -= (int32_t)taosGetTimestampSec();
    if (abs(delta) > 900) {
      tWarn("%s, time diff:%d is too big, msg discarded", pConn->info, delta);
      code = TSDB_CODE_RPC_INVALID_TIME_STAMP;
    } else {
      if (rpcAuthenticateMsg(pHead, len - TSDB_AUTH_LEN, pDigest->auth, pConn->secret) < 0) {
        // tDebug("%s, authentication failed, msg discarded", pConn->info);
        code = TSDB_CODE_RPC_AUTH_FAILURE;
      } else {
        pHead->msgLen = (int32_t)htonl((uint32_t)pHead->msgLen) - sizeof(SRpcDigest);
        if (!rpcIsReq(pHead->msgType)) pConn->secured = 1;  // link is secured for client
        // tTrace("%s, message is authenticated", pConn->info);
      }
    }
  } else {
    tDebug("%s, auth spi:%d not matched with received:%d", pConn->info, pConn->spi, pHead->spi);
    code = pHead->spi ? TSDB_CODE_RPC_AUTH_FAILURE : TSDB_CODE_RPC_AUTH_REQUIRED;
  }

  return code;
}
// refers specifically to query or insert timeout
static void uvHandleActivityTimeout(uv_timer_t* handle) {
  // impl later
  SRpcConn* conn = handle->data;
}
static void uvProcessData(SRpcConn* pConn) {
  SRecvInfo    info;
  SRecvInfo*   p = &info;
  SConnBuffer* pBuf = &pConn->connBuf;
  p->msg = pBuf->buf + RPC_RESERVE_SIZE;
  p->msgLen = pBuf->len;
  p->ip = 0;
  p->port = 0;
  p->shandle = pConn->shandle;  //
  p->thandle = pConn;
  p->chandle = NULL;

  //
  SRpcHead* pHead = (SRpcHead*)p->msg;
  assert(rpcIsReq(pHead->msgType));

  SRpcInfo* pRpc = (SRpcInfo*)p->shandle;
  pConn->ahandle = (void*)pHead->ahandle;
  // auth here

  int8_t code = uvAuthMsg(pConn, (char*)pHead, p->msgLen);
  if (code != 0) {
    terrno = code;
    return;
  }
  pHead->code = htonl(pHead->code);

  SRpcMsg rpcMsg;

  pHead = rpcDecompressRpcMsg(pHead);
  rpcMsg.contLen = rpcContLenFromMsg(pHead->msgLen);
  rpcMsg.pCont = pHead->content;
  rpcMsg.msgType = pHead->msgType;
  rpcMsg.code = pHead->code;
  rpcMsg.ahandle = pConn->ahandle;
  rpcMsg.handle = pConn;

  (*(pRpc->cfp))(pRpc->parent, &rpcMsg, NULL);
  uv_timer_start(pConn->pTimer, uvHandleActivityTimeout, pRpc->idleTime, 0);
  // auth
  // validate msg type
}
void uvOnReadCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf) {
  // opt
  SRpcConn*    ctx = cli->data;
  SConnBuffer* pBuf = &ctx->connBuf;
  if (nread > 0) {
    pBuf->len += nread;
    if (isReadAll(pBuf)) {
      tDebug("alread read complete packet");
      uvProcessData(ctx);
    } else {
      tDebug("read half packet, continue to read");
    }
    return;
  }
  if (terrno != 0) {
    // handle err code
  }

  if (nread != UV_EOF) {
    tDebug("Read error %s\n", uv_err_name(nread));
  }
  uv_close((uv_handle_t*)cli, uvConnDestroy);
}
void uvAllocConnBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  buf->base = malloc(sizeof(char));
  buf->len = 2;
}

void uvOnTimeoutCb(uv_timer_t* handle) {
  // opt
  tDebug("time out");
}

void uvOnWriteCb(uv_write_t* req, int status) {
  SRpcConn* conn = req->data;
  if (status == 0) {
    tDebug("data already was written on stream");
  } else {
    connDestroy(conn);
  }
  // opt
}

void uvWorkerAsyncCb(uv_async_t* handle) {
  SThreadObj* pThrd = container_of(handle, SThreadObj, workerAsync);
  SRpcConn*   conn = NULL;

  // opt later
  pthread_mutex_lock(&pThrd->connMtx);
  if (!QUEUE_IS_EMPTY(&pThrd->conn)) {
    queue* head = QUEUE_HEAD(&pThrd->conn);
    conn = QUEUE_DATA(head, SRpcConn, queue);
    QUEUE_REMOVE(&conn->queue);
  }
  pthread_mutex_unlock(&pThrd->connMtx);
  if (conn == NULL) {
    tError("except occurred, do nothing");
    return;
  }
  uv_buf_t wb = uv_buf_init(conn->writeBuf.buf, conn->writeBuf.len);
  uv_write(conn->pWriter, (uv_stream_t*)conn->pTcp, &wb, 1, uvOnWriteCb);
}

void uvOnAcceptCb(uv_stream_t* stream, int status) {
  if (status == -1) {
    return;
  }
  SServerObj* pObj = container_of(stream, SServerObj, server);

  uv_tcp_t* cli = (uv_tcp_t*)malloc(sizeof(uv_tcp_t));
  uv_tcp_init(pObj->loop, cli);

  if (uv_accept(stream, (uv_stream_t*)cli) == 0) {
    uv_write_t* wr = (uv_write_t*)malloc(sizeof(uv_write_t));

    uv_buf_t buf = uv_buf_init((char*)notify, strlen(notify));

    pObj->workerIdx = (pObj->workerIdx + 1) % pObj->numOfThreads;
    tDebug("new conntion accepted by main server, dispatch to %dth worker-thread", pObj->workerIdx);
    uv_write2(wr, (uv_stream_t*)&(pObj->pipe[pObj->workerIdx][0]), &buf, 1, (uv_stream_t*)cli, uvOnWriteCb);
  } else {
    uv_close((uv_handle_t*)cli, NULL);
  }
}
void uvOnConnectionCb(uv_stream_t* q, ssize_t nread, const uv_buf_t* buf) {
  tDebug("connection coming");
  if (nread < 0) {
    if (nread != UV_EOF) {
      tError("read error %s", uv_err_name(nread));
    }
    // TODO(log other failure reason)
    uv_close((uv_handle_t*)q, NULL);
    return;
  }
  // free memory allocated by
  assert(nread == strlen(notify));
  assert(buf->base[0] == notify[0]);
  free(buf->base);

  SThreadObj* pThrd = q->data;

  uv_pipe_t* pipe = (uv_pipe_t*)q;
  if (!uv_pipe_pending_count(pipe)) {
    tError("No pending count");
    return;
  }

  uv_handle_type pending = uv_pipe_pending_type(pipe);
  assert(pending == UV_TCP);

  SRpcConn* pConn = connCreate();
  pConn->shandle = pThrd->shandle;
  /* init conn timer*/
  pConn->pTimer = malloc(sizeof(uv_timer_t));
  uv_timer_init(pThrd->loop, pConn->pTimer);
  pConn->pTimer->data = pConn;

  pConn->hostThread = pThrd;
  pConn->pWorkerAsync = pThrd->workerAsync;  // thread safty

  // init client handle
  pConn->pTcp = (uv_tcp_t*)malloc(sizeof(uv_tcp_t));
  uv_tcp_init(pThrd->loop, pConn->pTcp);
  pConn->pTcp->data = pConn;

  // init write request, just
  pConn->pWriter = calloc(1, sizeof(uv_write_t));
  pConn->pWriter->data = pConn;

  if (uv_accept(q, (uv_stream_t*)(pConn->pTcp)) == 0) {
    uv_os_fd_t fd;
    uv_fileno((const uv_handle_t*)pConn->pTcp, &fd);
    tDebug("new connection created: %d", fd);
    uv_read_start((uv_stream_t*)(pConn->pTcp), uvAllocReadBufferCb, uvOnReadCb);
  } else {
    connDestroy(pConn);
  }
}

void* acceptThread(void* arg) {
  // opt
  SServerObj* srv = (SServerObj*)arg;
  uv_tcp_init(srv->loop, &srv->server);

  struct sockaddr_in bind_addr;

  uv_ip4_addr("0.0.0.0", srv->port, &bind_addr);
  uv_tcp_bind(&srv->server, (const struct sockaddr*)&bind_addr, 0);
  int err = 0;
  if ((err = uv_listen((uv_stream_t*)&srv->server, 128, uvOnAcceptCb)) != 0) {
    tError("Listen error %s\n", uv_err_name(err));
    return NULL;
  }
  uv_run(srv->loop, UV_RUN_DEFAULT);
}
void* workerThread(void* arg) {
  SThreadObj* pThrd = (SThreadObj*)arg;

  pThrd->loop = (uv_loop_t*)malloc(sizeof(uv_loop_t));
  uv_loop_init(pThrd->loop);

  uv_pipe_init(pThrd->loop, pThrd->pipe, 1);
  uv_pipe_open(pThrd->pipe, pThrd->fd);

  pThrd->pipe->data = pThrd;

  QUEUE_INIT(&pThrd->conn);

  pThrd->workerAsync = malloc(sizeof(uv_async_t));
  uv_async_init(pThrd->loop, pThrd->workerAsync, uvWorkerAsyncCb);

  uv_read_start((uv_stream_t*)pThrd->pipe, uvAllocConnBufferCb, uvOnConnectionCb);
  uv_run(pThrd->loop, UV_RUN_DEFAULT);
}
static SRpcConn* connCreate() {
  SRpcConn* pConn = (SRpcConn*)calloc(1, sizeof(SRpcConn));
  return pConn;
}
static void connDestroy(SRpcConn* conn) {
  if (conn == NULL) {
    return;
  }
  uv_timer_stop(conn->pTimer);
  free(conn->pTimer);
  uv_close((uv_handle_t*)conn->pTcp, NULL);
  free(conn->connBuf.buf);
  free(conn->pTcp);
  free(conn->pWriter);
  free(conn);
  // handle
}
static void uvConnDestroy(uv_handle_t* handle) {
  SRpcConn* conn = handle->data;
  connDestroy(conn);
}
void* rpcOpen(const SRpcInit* pInit) {
  SRpcInfo* pRpc = calloc(1, sizeof(SRpcInfo));
  if (pRpc == NULL) {
    return NULL;
  }
  if (pInit->label) {
    tstrncpy(pRpc->label, pInit->label, strlen(pInit->label));
  }
  pRpc->numOfThreads = pInit->numOfThreads > TSDB_MAX_RPC_THREADS ? TSDB_MAX_RPC_THREADS : pInit->numOfThreads;
  pRpc->connType = pInit->connType;
  pRpc->tcphandle = (*taosHandle[pRpc->connType])(0, pInit->localPort, pRpc->label, pRpc->numOfThreads, NULL, pRpc);
  // pRpc->taosInitServer(0, pInit->localPort, pRpc->label, pRpc->numOfThreads, NULL, pRpc);
  return pRpc;
}
void  rpcClose(void* arg) { return; }
void* rpcMallocCont(int contLen) { return NULL; }
void  rpcFreeCont(void* cont) { return; }
void* rpcReallocCont(void* ptr, int contLen) { return NULL; }

void rpcSendRequest(void* thandle, const SEpSet* pEpSet, SRpcMsg* pMsg, int64_t* rid) {
  // impl later
  return;
}

void rpcSendResponse(const SRpcMsg* pMsg) {
  SRpcConn*   pConn = pMsg->handle;
  SThreadObj* pThrd = pConn->hostThread;

  // opt later
  pthread_mutex_lock(&pThrd->connMtx);
  QUEUE_PUSH(&pThrd->conn, &pConn->queue);
  pthread_mutex_unlock(&pThrd->connMtx);

  uv_async_send(pConn->pWorkerAsync);
}

void rpcSendRedirectRsp(void* pConn, const SEpSet* pEpSet) {}
int  rpcGetConnInfo(void* thandle, SRpcConnInfo* pInfo) { return -1; }
void rpcSendRecv(void* shandle, SEpSet* pEpSet, SRpcMsg* pReq, SRpcMsg* pRsp) { return; }
int  rpcReportProgress(void* pConn, char* pCont, int contLen) { return -1; }
void rpcCancelRequest(int64_t rid) { return; }

static int rpcAuthenticateMsg(void* pMsg, int msgLen, void* pAuth, void* pKey) {
  T_MD5_CTX context;
  int       ret = -1;

  tMD5Init(&context);
  tMD5Update(&context, (uint8_t*)pKey, TSDB_PASSWORD_LEN);
  tMD5Update(&context, (uint8_t*)pMsg, msgLen);
  tMD5Update(&context, (uint8_t*)pKey, TSDB_PASSWORD_LEN);
  tMD5Final(&context);

  if (memcmp(context.digest, pAuth, sizeof(context.digest)) == 0) ret = 0;

  return ret;
}
static void rpcBuildAuthHead(void* pMsg, int msgLen, void* pAuth, void* pKey) {
  T_MD5_CTX context;

  tMD5Init(&context);
  tMD5Update(&context, (uint8_t*)pKey, TSDB_PASSWORD_LEN);
  tMD5Update(&context, (uint8_t*)pMsg, msgLen);
  tMD5Update(&context, (uint8_t*)pKey, TSDB_PASSWORD_LEN);
  tMD5Final(&context);

  memcpy(pAuth, context.digest, sizeof(context.digest));
}

static int rpcAddAuthPart(SRpcConn* pConn, char* msg, int msgLen) {
  SRpcHead* pHead = (SRpcHead*)msg;

  if (pConn->spi && pConn->secured == 0) {
    // add auth part
    pHead->spi = pConn->spi;
    SRpcDigest* pDigest = (SRpcDigest*)(msg + msgLen);
    pDigest->timeStamp = htonl(taosGetTimestampSec());
    msgLen += sizeof(SRpcDigest);
    pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
    rpcBuildAuthHead(pHead, msgLen - TSDB_AUTH_LEN, pDigest->auth, pConn->secret);
  } else {
    pHead->spi = 0;
    pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
  }

  return msgLen;
}

static int32_t rpcCompressRpcMsg(char* pCont, int32_t contLen) {
  SRpcHead* pHead = rpcHeadFromCont(pCont);
  int32_t   finalLen = 0;
  int       overhead = sizeof(SRpcComp);

  if (!NEEDTO_COMPRESSS_MSG(contLen)) {
    return contLen;
  }

  char* buf = malloc(contLen + overhead + 8);  // 8 extra bytes
  if (buf == NULL) {
    tError("failed to allocate memory for rpc msg compression, contLen:%d", contLen);
    return contLen;
  }

  int32_t compLen = LZ4_compress_default(pCont, buf, contLen, contLen + overhead);
  tDebug("compress rpc msg, before:%d, after:%d, overhead:%d", contLen, compLen, overhead);

  /*
   * only the compressed size is less than the value of contLen - overhead, the compression is applied
   * The first four bytes is set to 0, the second four bytes are utilized to keep the original length of message
   */
  if (compLen > 0 && compLen < contLen - overhead) {
    SRpcComp* pComp = (SRpcComp*)pCont;
    pComp->reserved = 0;
    pComp->contLen = htonl(contLen);
    memcpy(pCont + overhead, buf, compLen);

    pHead->comp = 1;
    tDebug("compress rpc msg, before:%d, after:%d", contLen, compLen);
    finalLen = compLen + overhead;
  } else {
    finalLen = contLen;
  }

  free(buf);
  return finalLen;
}

static SRpcHead* rpcDecompressRpcMsg(SRpcHead* pHead) {
  int       overhead = sizeof(SRpcComp);
  SRpcHead* pNewHead = NULL;
  uint8_t*  pCont = pHead->content;
  SRpcComp* pComp = (SRpcComp*)pHead->content;

  if (pHead->comp) {
    // decompress the content
    assert(pComp->reserved == 0);
    int contLen = htonl(pComp->contLen);

    // prepare the temporary buffer to decompress message
    char* temp = (char*)malloc(contLen + RPC_MSG_OVERHEAD);
    pNewHead = (SRpcHead*)(temp + sizeof(SRpcReqContext));  // reserve SRpcReqContext

    if (pNewHead) {
      int compLen = rpcContLenFromMsg(pHead->msgLen) - overhead;
      int origLen = LZ4_decompress_safe((char*)(pCont + overhead), (char*)pNewHead->content, compLen, contLen);
      assert(origLen == contLen);

      memcpy(pNewHead, pHead, sizeof(SRpcHead));
      pNewHead->msgLen = rpcMsgLenFromCont(origLen);
      /// rpcFreeMsg(pHead);  // free the compressed message buffer
      pHead = pNewHead;
      tTrace("decomp malloc mem:%p", temp);
    } else {
      tError("failed to allocate memory to decompress msg, contLen:%d", contLen);
    }
  }

  return pHead;
}
int32_t rpcInit(void) {
  // impl later
  return -1;
}

void rpcCleanup(void) {
  // impl later
  return;
}
#endif
