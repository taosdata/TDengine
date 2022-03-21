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

typedef struct {
  int       notifyCount;  //
  int       init;         // init or not
  STransMsg msg;
} SSrvRegArg;

typedef struct SSrvConn {
  T_REF_DECLARE()
  uv_tcp_t*  pTcp;
  uv_write_t pWriter;
  uv_timer_t pTimer;

  queue       queue;
  int         ref;
  int         persist;  // persist connection or not
  SConnBuffer readBuf;  // read buf,
  int         inType;
  void*       pTransInst;  // rpc init
  void*       ahandle;     //
  void*       hostThrd;
  STransQueue srvMsgs;

  SSrvRegArg regArg;
  bool       broken;  // conn broken;

  ConnStatus         status;
  struct sockaddr_in addr;
  struct sockaddr_in locaddr;

  char secured;
  int  spi;
  char info[64];
  char user[TSDB_UNI_LEN];  // user ID for the link
  char secret[TSDB_PASSWORD_LEN];
  char ckey[TSDB_PASSWORD_LEN];  // ciphering key
} SSrvConn;

typedef struct SSrvMsg {
  SSrvConn*     pConn;
  STransMsg     msg;
  queue         q;
  STransMsgType type;
} SSrvMsg;

typedef struct SWorkThrdObj {
  TdThread      thread;
  uv_pipe_t*    pipe;
  uv_os_fd_t    fd;
  uv_loop_t*    loop;
  SAsyncPool*   asyncPool;
  queue         msg;
  TdThreadMutex msgMtx;

  queue conn;
  void* pTransInst;
  bool  quit;
} SWorkThrdObj;

typedef struct SServerObj {
  TdThread   thread;
  uv_tcp_t   server;
  uv_loop_t* loop;

  // work thread info
  int            workerIdx;
  int            numOfThreads;
  SWorkThrdObj** pThreadObj;

  uv_pipe_t** pipe;
  uint32_t    ip;
  uint32_t    port;
  uv_async_t* pAcceptAsync;  // just to quit from from accept thread
} SServerObj;

static const char* notify = "a";

#define CONN_SHOULD_RELEASE(conn, head)                            \
  do {                                                             \
    if ((head)->release == 1 && (head->msgLen) == sizeof(*head)) { \
      conn->status = ConnRelease;                                  \
      transClearBuffer(&conn->readBuf);                            \
      transFreeMsg(transContFromHead((char*)head));                \
      tTrace("server conn %p received release request", conn);     \
                                                                   \
      STransMsg tmsg = {.handle = (void*)conn, .code = 0};         \
      SSrvMsg*  srvMsg = calloc(1, sizeof(SSrvMsg));               \
      srvMsg->msg = tmsg;                                          \
      srvMsg->type = Release;                                      \
      srvMsg->pConn = conn;                                        \
      if (!transQueuePush(&conn->srvMsgs, srvMsg)) {               \
        return;                                                    \
      }                                                            \
      uvStartSendRespInternal(srvMsg);                             \
      return;                                                      \
    }                                                              \
  } while (0)

static void uvAllocConnBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
static void uvAllocRecvBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
static void uvOnRecvCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf);
static void uvOnTimeoutCb(uv_timer_t* handle);
static void uvOnSendCb(uv_write_t* req, int status);
static void uvOnPipeWriteCb(uv_write_t* req, int status);
static void uvOnAcceptCb(uv_stream_t* stream, int status);
static void uvOnConnectionCb(uv_stream_t* q, ssize_t nread, const uv_buf_t* buf);
static void uvWorkerAsyncCb(uv_async_t* handle);
static void uvAcceptAsyncCb(uv_async_t* handle);
static void uvShutDownCb(uv_shutdown_t* req, int status);

static void uvStartSendRespInternal(SSrvMsg* smsg);
static void uvPrepareSendData(SSrvMsg* msg, uv_buf_t* wb);
static void uvStartSendResp(SSrvMsg* msg);

static void uvNotifyLinkBrokenToApp(SSrvConn* conn);

static void destroySmsg(SSrvMsg* smsg);
// check whether already read complete packet
static SSrvConn* createConn(void* hThrd);
static void      destroyConn(SSrvConn* conn, bool clear /*clear handle or not*/);

static void uvHandleQuit(SSrvMsg* msg, SWorkThrdObj* thrd);
static void uvHandleRelease(SSrvMsg* msg, SWorkThrdObj* thrd);
static void uvHandleResp(SSrvMsg* msg, SWorkThrdObj* thrd);
static void uvHandleRegister(SSrvMsg* msg, SWorkThrdObj* thrd);
static void (*transAsyncHandle[])(SSrvMsg* msg, SWorkThrdObj* thrd) = {uvHandleResp, uvHandleQuit, uvHandleRelease,
                                                                       uvHandleRegister};

static void uvDestroyConn(uv_handle_t* handle);

// server and worker thread
static void* workerThread(void* arg);
static void* acceptThread(void* arg);

// add handle loop
static bool addHandleToWorkloop(void* arg);
static bool addHandleToAcceptloop(void* arg);

void uvAllocRecvBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  SSrvConn*    conn = handle->data;
  SConnBuffer* pBuf = &conn->readBuf;
  transAllocBuffer(pBuf, buf);
}

// refers specifically to query or insert timeout
static void uvHandleActivityTimeout(uv_timer_t* handle) {
  SSrvConn* conn = handle->data;
  tDebug("%p timeout since no activity", conn);
}

static void uvHandleReq(SSrvConn* pConn) {
  SConnBuffer* pBuf = &pConn->readBuf;
  char*        msg = pBuf->buf;
  uint32_t     msgLen = pBuf->len;

  STransMsgHead* pHead = (STransMsgHead*)msg;
  if (pHead->secured == 1) {
    STransUserMsg* uMsg = (STransUserMsg*)((char*)msg + msgLen - sizeof(STransUserMsg));
    memcpy(pConn->user, uMsg->user, tListLen(uMsg->user));
    memcpy(pConn->secret, uMsg->secret, tListLen(uMsg->secret));
  }
  pHead->code = htonl(pHead->code);
  pHead->msgLen = htonl(pHead->msgLen);
  if (pHead->secured == 1) {
    pHead->msgLen -= sizeof(STransUserMsg);
  }

  CONN_SHOULD_RELEASE(pConn, pHead);

  STransMsg transMsg;
  transMsg.contLen = transContLenFromMsg(pHead->msgLen);
  transMsg.pCont = pHead->content;
  transMsg.msgType = pHead->msgType;
  transMsg.code = pHead->code;
  transMsg.ahandle = NULL;
  transMsg.handle = NULL;

  transClearBuffer(&pConn->readBuf);
  pConn->inType = pHead->msgType;
  if (pConn->status == ConnNormal) {
    if (pHead->persist == 1) {
      pConn->status = ConnAcquire;
      transRefSrvHandle(pConn);
    }
  }
  if (pConn->status == ConnNormal && pHead->noResp == 0) {
    transRefSrvHandle(pConn);
    tDebug("server conn %p %s received from %s:%d, local info: %s:%d, msg size: %d", pConn, TMSG_INFO(transMsg.msgType),
           taosInetNtoa(pConn->addr.sin_addr), ntohs(pConn->addr.sin_port), taosInetNtoa(pConn->locaddr.sin_addr),
           ntohs(pConn->locaddr.sin_port), transMsg.contLen);
  } else {
    tDebug("server conn %p %s received from %s:%d, local info: %s:%d, msg size: %d, resp:%d ", pConn,
           TMSG_INFO(transMsg.msgType), taosInetNtoa(pConn->addr.sin_addr), ntohs(pConn->addr.sin_port),
           taosInetNtoa(pConn->locaddr.sin_addr), ntohs(pConn->locaddr.sin_port), transMsg.contLen, pHead->noResp);
    // no ref here
  }

  if (pHead->noResp == 0) {
    transMsg.handle = pConn;
  }

  STrans* pTransInst = pConn->pTransInst;
  (*pTransInst->cfp)(pTransInst->parent, &transMsg, NULL);
  // uv_timer_start(&pConn->pTimer, uvHandleActivityTimeout, pRpc->idleTime * 10000, 0);
}

void uvOnRecvCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf) {
  // opt
  SSrvConn*    conn = cli->data;
  SConnBuffer* pBuf = &conn->readBuf;
  if (nread > 0) {
    pBuf->len += nread;
    tTrace("server conn %p read summary, total read: %d, current read: %d", conn, pBuf->len, (int)nread);
    if (transReadComplete(pBuf)) {
      tTrace("server conn %p alread read complete packet", conn);
      uvHandleReq(conn);
    } else {
      tTrace("server %p read partial packet, continue to read", conn);
    }
    return;
  }
  if (nread == 0) {
    return;
  }

  tError("server conn %p read error: %s", conn, uv_err_name(nread));
  if (nread < 0) {
    conn->broken = true;
    if (conn->status == ConnAcquire) {
      if (conn->regArg.init) {
        STrans* pTransInst = conn->pTransInst;
        (*pTransInst->cfp)(pTransInst->parent, &(conn->regArg.msg), NULL);
        memset(&conn->regArg, 0, sizeof(conn->regArg));
      }
    }
    transUnrefSrvHandle(conn);
  }
}
void uvAllocConnBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  buf->len = 2;
  buf->base = calloc(1, sizeof(char) * buf->len);
}

void uvOnTimeoutCb(uv_timer_t* handle) {
  // opt
  SSrvConn* pConn = handle->data;
  tError("server conn %p time out", pConn);
}

void uvOnSendCb(uv_write_t* req, int status) {
  SSrvConn* conn = req->data;
  transClearBuffer(&conn->readBuf);
  if (status == 0) {
    tTrace("server conn %p data already was written on stream", conn);
    if (!transQueueEmpty(&conn->srvMsgs)) {
      SSrvMsg* msg = transQueuePop(&conn->srvMsgs);
      if (msg->type == Release && conn->status != ConnNormal) {
        conn->status = ConnNormal;
        transUnrefSrvHandle(conn);
      }
      destroySmsg(msg);
      // send second data, just use for push
      if (!transQueueEmpty(&conn->srvMsgs)) {
        msg = (SSrvMsg*)transQueueGet(&conn->srvMsgs);
        if (msg->type == Register && conn->status == ConnAcquire) {
          conn->regArg.notifyCount = 0;
          conn->regArg.init = 1;
          conn->regArg.msg = msg->msg;
          if (conn->broken) {
            STrans* pTransInst = conn->pTransInst;
            (pTransInst->cfp)(pTransInst->parent, &(conn->regArg.msg), NULL);
            memset(&conn->regArg, 0, sizeof(conn->regArg));
          }
          transQueuePop(&conn->srvMsgs);
          free(msg);
        } else {
          uvStartSendRespInternal(msg);
        }
      }
    }
  } else {
    tError("server conn %p failed to write data, %s", conn, uv_err_name(status));
    conn->broken = true;
    transUnrefSrvHandle(conn);
  }
}
static void uvOnPipeWriteCb(uv_write_t* req, int status) {
  if (status == 0) {
    tTrace("success to dispatch conn to work thread");
  } else {
    tError("fail to dispatch conn to work thread");
  }
  free(req);
}

static void uvPrepareSendData(SSrvMsg* smsg, uv_buf_t* wb) {
  tTrace("server conn %p prepare to send resp", smsg->pConn);

  SSrvConn*  pConn = smsg->pConn;
  STransMsg* pMsg = &smsg->msg;
  if (pMsg->pCont == 0) {
    pMsg->pCont = (void*)rpcMallocCont(0);
    pMsg->contLen = 0;
  }
  STransMsgHead* pHead = transHeadFromCont(pMsg->pCont);

  // pHead->secured = pMsg->code == 0 ? 1 : 0;  //
  if (!pConn->secured) {
    pConn->secured = pMsg->code == 0 ? 1 : 0;
  }
  pHead->secured = pConn->secured;

  if (pConn->status == ConnNormal) {
    pHead->msgType = pConn->inType + 1;
  } else {
    pHead->msgType = smsg->type == Release ? 0 : pMsg->msgType;
  }
  pHead->release = smsg->type == Release ? 1 : 0;
  pHead->code = htonl(pMsg->code);

  char*   msg = (char*)pHead;
  int32_t len = transMsgLenFromCont(pMsg->contLen);
  tDebug("server conn %p %s is sent to %s:%d, local info: %s:%d", pConn, TMSG_INFO(pHead->msgType),
         taosInetNtoa(pConn->addr.sin_addr), ntohs(pConn->addr.sin_port), taosInetNtoa(pConn->locaddr.sin_addr),
         ntohs(pConn->locaddr.sin_port));
  pHead->msgLen = htonl(len);

  wb->base = msg;
  wb->len = len;
}

static void uvStartSendRespInternal(SSrvMsg* smsg) {
  uv_buf_t wb;
  uvPrepareSendData(smsg, &wb);

  SSrvConn* pConn = smsg->pConn;
  uv_timer_stop(&pConn->pTimer);
  uv_write(&pConn->pWriter, (uv_stream_t*)pConn->pTcp, &wb, 1, uvOnSendCb);
}
static void uvStartSendResp(SSrvMsg* smsg) {
  // impl
  SSrvConn* pConn = smsg->pConn;

  if (pConn->broken == true) {
    // persist by
    transUnrefSrvHandle(pConn);
    return;
  }
  if (pConn->status == ConnNormal) {
    transUnrefSrvHandle(pConn);
  }

  if (!transQueuePush(&pConn->srvMsgs, smsg)) {
    return;
  }
  uvStartSendRespInternal(smsg);
  return;
}

static void destroySmsg(SSrvMsg* smsg) {
  if (smsg == NULL) {
    return;
  }
  transFreeMsg(smsg->msg.pCont);
  free(smsg);
}
static void destroyAllConn(SWorkThrdObj* pThrd) {
  while (!QUEUE_IS_EMPTY(&pThrd->conn)) {
    queue* h = QUEUE_HEAD(&pThrd->conn);
    QUEUE_REMOVE(h);
    QUEUE_INIT(h);

    SSrvConn* c = QUEUE_DATA(h, SSrvConn, queue);
    while (T_REF_VAL_GET(c) >= 2) {
      transUnrefSrvHandle(c);
    }
    transUnrefSrvHandle(c);
  }
}
void uvWorkerAsyncCb(uv_async_t* handle) {
  SAsyncItem*   item = handle->data;
  SWorkThrdObj* pThrd = item->pThrd;
  SSrvConn*     conn = NULL;
  queue         wq;

  // batch process to avoid to lock/unlock frequently
  taosThreadMutexLock(&item->mtx);
  QUEUE_MOVE(&item->qmsg, &wq);
  taosThreadMutexUnlock(&item->mtx);

  while (!QUEUE_IS_EMPTY(&wq)) {
    queue* head = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(head);

    SSrvMsg* msg = QUEUE_DATA(head, SSrvMsg, q);
    if (msg == NULL) {
      tError("unexcept occurred, continue");
      continue;
    }
    (*transAsyncHandle[msg->type])(msg, pThrd);
  }
}
static void uvAcceptAsyncCb(uv_async_t* async) {
  SServerObj* srv = async->data;
  tDebug("close server port %d", srv->port);
  uv_close((uv_handle_t*)&srv->server, NULL);
  uv_stop(srv->loop);
}

static void uvShutDownCb(uv_shutdown_t* req, int status) {
  if (status != 0) {
    tDebug("conn failed to shut down: %s", uv_err_name(status));
  }
  uv_close((uv_handle_t*)req->handle, uvDestroyConn);
  free(req);
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

    tTrace("new conntion accepted by main server, dispatch to %dth worker-thread", pObj->workerIdx);
    uv_write2(wr, (uv_stream_t*)&(pObj->pipe[pObj->workerIdx][0]), &buf, 1, (uv_stream_t*)cli, uvOnPipeWriteCb);
  } else {
    uv_close((uv_handle_t*)cli, NULL);
    free(cli);
  }
}
void uvOnConnectionCb(uv_stream_t* q, ssize_t nread, const uv_buf_t* buf) {
  tTrace("server connection coming");
  if (nread < 0) {
    if (nread != UV_EOF) {
      tError("read error %s", uv_err_name(nread));
    }
    // TODO(log other failure reason)
    // uv_close((uv_handle_t*)q, NULL);
    return;
  }
  // free memory allocated by
  assert(nread == strlen(notify));
  assert(buf->base[0] == notify[0]);
  free(buf->base);

  SWorkThrdObj* pThrd = q->data;

  uv_pipe_t* pipe = (uv_pipe_t*)q;
  if (!uv_pipe_pending_count(pipe)) {
    tError("No pending count");
    return;
  }

  uv_handle_type pending = uv_pipe_pending_type(pipe);
  assert(pending == UV_TCP);

  SSrvConn* pConn = createConn(pThrd);

  pConn->pTransInst = pThrd->pTransInst;
  /* init conn timer*/
  uv_timer_init(pThrd->loop, &pConn->pTimer);
  pConn->pTimer.data = pConn;

  pConn->hostThrd = pThrd;

  // init client handle
  pConn->pTcp = (uv_tcp_t*)malloc(sizeof(uv_tcp_t));
  uv_tcp_init(pThrd->loop, pConn->pTcp);
  pConn->pTcp->data = pConn;

  pConn->pWriter.data = pConn;

  transSetConnOption((uv_tcp_t*)pConn->pTcp);

  if (uv_accept(q, (uv_stream_t*)(pConn->pTcp)) == 0) {
    uv_os_fd_t fd;
    uv_fileno((const uv_handle_t*)pConn->pTcp, &fd);
    tTrace("server conn %p created, fd: %d", pConn, fd);

    int addrlen = sizeof(pConn->addr);
    if (0 != uv_tcp_getpeername(pConn->pTcp, (struct sockaddr*)&pConn->addr, &addrlen)) {
      tError("server conn %p failed to get peer info", pConn);
      transUnrefSrvHandle(pConn);
      return;
    }

    addrlen = sizeof(pConn->locaddr);
    if (0 != uv_tcp_getsockname(pConn->pTcp, (struct sockaddr*)&pConn->locaddr, &addrlen)) {
      tError("server conn %p failed to get local info", pConn);
      transUnrefSrvHandle(pConn);
      return;
    }

    uv_read_start((uv_stream_t*)(pConn->pTcp), uvAllocRecvBufferCb, uvOnRecvCb);

  } else {
    tDebug("failed to create new connection");
    transUnrefSrvHandle(pConn);
  }
}

void* acceptThread(void* arg) {
  // opt
  setThreadName("trans-accept");
  SServerObj* srv = (SServerObj*)arg;
  uv_run(srv->loop, UV_RUN_DEFAULT);

  return NULL;
}
static bool addHandleToWorkloop(void* arg) {
  SWorkThrdObj* pThrd = arg;
  pThrd->loop = (uv_loop_t*)malloc(sizeof(uv_loop_t));
  if (0 != uv_loop_init(pThrd->loop)) {
    return false;
  }

  uv_pipe_init(pThrd->loop, pThrd->pipe, 1);
  uv_pipe_open(pThrd->pipe, pThrd->fd);

  pThrd->pipe->data = pThrd;

  QUEUE_INIT(&pThrd->msg);
  taosThreadMutexInit(&pThrd->msgMtx, NULL);

  // conn set
  QUEUE_INIT(&pThrd->conn);

  pThrd->asyncPool = transCreateAsyncPool(pThrd->loop, 4, pThrd, uvWorkerAsyncCb);
  uv_read_start((uv_stream_t*)pThrd->pipe, uvAllocConnBufferCb, uvOnConnectionCb);
  return true;
}

static bool addHandleToAcceptloop(void* arg) {
  // impl later
  SServerObj* srv = arg;

  int err = 0;
  if ((err = uv_tcp_init(srv->loop, &srv->server)) != 0) {
    tError("failed to init accept server: %s", uv_err_name(err));
    return false;
  }

  // register an async here to quit server gracefully
  srv->pAcceptAsync = calloc(1, sizeof(uv_async_t));
  uv_async_init(srv->loop, srv->pAcceptAsync, uvAcceptAsyncCb);
  srv->pAcceptAsync->data = srv;

  struct sockaddr_in bind_addr;
  uv_ip4_addr("0.0.0.0", srv->port, &bind_addr);
  if ((err = uv_tcp_bind(&srv->server, (const struct sockaddr*)&bind_addr, 0)) != 0) {
    tError("failed to bind: %s", uv_err_name(err));
    return false;
  }
  if ((err = uv_listen((uv_stream_t*)&srv->server, 512, uvOnAcceptCb)) != 0) {
    tError("failed to listen: %s", uv_err_name(err));
    return false;
  }
  return true;
}
void* workerThread(void* arg) {
  setThreadName("trans-worker");
  SWorkThrdObj* pThrd = (SWorkThrdObj*)arg;
  uv_run(pThrd->loop, UV_RUN_DEFAULT);

  return NULL;
}

static SSrvConn* createConn(void* hThrd) {
  SWorkThrdObj* pThrd = hThrd;

  SSrvConn* pConn = (SSrvConn*)calloc(1, sizeof(SSrvConn));
  QUEUE_INIT(&pConn->queue);

  QUEUE_PUSH(&pThrd->conn, &pConn->queue);

  transQueueInit(&pConn->srvMsgs, NULL);

  memset(&pConn->regArg, 0, sizeof(pConn->regArg));
  pConn->broken = false;
  pConn->status = ConnNormal;

  transRefSrvHandle(pConn);
  tTrace("server conn %p created", pConn);
  return pConn;
}

static void destroyConn(SSrvConn* conn, bool clear) {
  if (conn == NULL) {
    return;
  }
  transDestroyBuffer(&conn->readBuf);

  transQueueDestroy(&conn->srvMsgs);
  if (clear) {
    tTrace("server conn %p to be destroyed", conn);
    uv_shutdown_t* req = malloc(sizeof(uv_shutdown_t));
    uv_shutdown(req, (uv_stream_t*)conn->pTcp, uvShutDownCb);
  }
}
static void uvDestroyConn(uv_handle_t* handle) {
  SSrvConn* conn = handle->data;
  if (conn == NULL) {
    return;
  }
  SWorkThrdObj* thrd = conn->hostThrd;

  tDebug("server conn %p destroy", conn);
  uv_timer_stop(&conn->pTimer);
  QUEUE_REMOVE(&conn->queue);
  free(conn->pTcp);
  // free(conn);

  if (thrd->quit && QUEUE_IS_EMPTY(&thrd->conn)) {
    uv_loop_close(thrd->loop);
    uv_stop(thrd->loop);
  }
}

void* transInitServer(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle) {
  SServerObj* srv = calloc(1, sizeof(SServerObj));
  srv->loop = (uv_loop_t*)malloc(sizeof(uv_loop_t));
  srv->numOfThreads = numOfThreads;
  srv->workerIdx = 0;
  srv->pThreadObj = (SWorkThrdObj**)calloc(srv->numOfThreads, sizeof(SWorkThrdObj*));
  srv->pipe = (uv_pipe_t**)calloc(srv->numOfThreads, sizeof(uv_pipe_t*));
  srv->ip = ip;
  srv->port = port;
  uv_loop_init(srv->loop);

  for (int i = 0; i < srv->numOfThreads; i++) {
    SWorkThrdObj* thrd = (SWorkThrdObj*)calloc(1, sizeof(SWorkThrdObj));
    thrd->quit = false;
    srv->pThreadObj[i] = thrd;

    srv->pipe[i] = (uv_pipe_t*)calloc(2, sizeof(uv_pipe_t));
    int fds[2];
    if (uv_socketpair(AF_UNIX, SOCK_STREAM, fds, UV_NONBLOCK_PIPE, UV_NONBLOCK_PIPE) != 0) {
      goto End;
    }
    uv_pipe_init(srv->loop, &(srv->pipe[i][0]), 1);
    uv_pipe_open(&(srv->pipe[i][0]), fds[1]);  // init write

    thrd->pTransInst = shandle;
    thrd->fd = fds[0];
    thrd->pipe = &(srv->pipe[i][1]);  // init read

    if (false == addHandleToWorkloop(thrd)) {
      goto End;
    }
    int err = taosThreadCreate(&(thrd->thread), NULL, workerThread, (void*)(thrd));
    if (err == 0) {
      tDebug("sucess to create worker-thread %d", i);
      // printf("thread %d create\n", i);
    } else {
      // TODO: clear all other resource later
      tError("failed to create worker-thread %d", i);
    }
  }
  if (false == addHandleToAcceptloop(srv)) {
    goto End;
  }
  int err = taosThreadCreate(&srv->thread, NULL, acceptThread, (void*)srv);
  if (err == 0) {
    tDebug("success to create accept-thread");
  } else {
    // clear all resource later
  }

  return srv;
End:
  transCloseServer(srv);
  return NULL;
}
void uvHandleQuit(SSrvMsg* msg, SWorkThrdObj* thrd) {
  if (QUEUE_IS_EMPTY(&thrd->conn)) {
    uv_loop_close(thrd->loop);
    uv_stop(thrd->loop);
  } else {
    destroyAllConn(thrd);
    thrd->quit = true;
  }
  free(msg);
}
void uvHandleRelease(SSrvMsg* msg, SWorkThrdObj* thrd) {
  // release handle to rpc init
  SSrvConn* conn = msg->pConn;
  if (conn->status == ConnAcquire) {
    if (!transQueuePush(&conn->srvMsgs, msg)) {
      return;
    }
    uvStartSendRespInternal(msg);
    return;
  } else if (conn->status == ConnRelease || conn->status == ConnNormal) {
    tDebug("server conn %p already released, ignore release-msg", conn);
  }
  destroySmsg(msg);
}
void uvHandleResp(SSrvMsg* msg, SWorkThrdObj* thrd) {
  // send msg to client
  tDebug("server conn %p start to send resp", msg->pConn);
  uvStartSendResp(msg);
}
void uvHandleRegister(SSrvMsg* msg, SWorkThrdObj* thrd) {
  SSrvConn* conn = msg->pConn;
  tDebug("server conn %p register brokenlink callback", conn);
  if (conn->status == ConnAcquire) {
    if (!transQueuePush(&conn->srvMsgs, msg)) {
      return;
    }
    conn->regArg.notifyCount = 0;
    conn->regArg.init = 1;
    conn->regArg.msg = msg->msg;

    if (conn->broken) {
      STrans* pTransInst = conn->pTransInst;
      (*pTransInst->cfp)(pTransInst->parent, &(conn->regArg.msg), NULL);
      memset(&conn->regArg, 0, sizeof(conn->regArg));
    }
    free(msg);
  }
}
void destroyWorkThrd(SWorkThrdObj* pThrd) {
  if (pThrd == NULL) {
    return;
  }
  taosThreadJoin(pThrd->thread, NULL);
  free(pThrd->loop);
  transDestroyAsyncPool(pThrd->asyncPool);
  free(pThrd);
}
void sendQuitToWorkThrd(SWorkThrdObj* pThrd) {
  SSrvMsg* msg = calloc(1, sizeof(SSrvMsg));
  msg->type = Quit;
  tDebug("server send quit msg to work thread");
  transSendAsync(pThrd->asyncPool, &msg->q);
}

void transCloseServer(void* arg) {
  // impl later
  SServerObj* srv = arg;
  for (int i = 0; i < srv->numOfThreads; i++) {
    sendQuitToWorkThrd(srv->pThreadObj[i]);
    destroyWorkThrd(srv->pThreadObj[i]);
  }

  tDebug("send quit msg to accept thread");
  uv_async_send(srv->pAcceptAsync);
  taosThreadJoin(srv->thread, NULL);

  free(srv->pThreadObj);
  free(srv->pAcceptAsync);
  free(srv->loop);

  for (int i = 0; i < srv->numOfThreads; i++) {
    free(srv->pipe[i]);
  }
  free(srv->pipe);

  free(srv);
}

void transRefSrvHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  SSrvConn* conn = handle;

  int ref = T_REF_INC((SSrvConn*)handle);
  UNUSED(ref);
}

void transUnrefSrvHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  int ref = T_REF_DEC((SSrvConn*)handle);
  tDebug("server conn %p ref count: %d", handle, ref);
  if (ref == 0) {
    destroyConn((SSrvConn*)handle, true);
  }
}

void transReleaseSrvHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  SSrvConn*     pConn = handle;
  SWorkThrdObj* pThrd = pConn->hostThrd;

  STransMsg tmsg = {.handle = handle, .code = 0};

  SSrvMsg* srvMsg = calloc(1, sizeof(SSrvMsg));
  srvMsg->msg = tmsg;
  srvMsg->type = Release;
  srvMsg->pConn = pConn;

  tTrace("server conn %p start to release", pConn);
  transSendAsync(pThrd->asyncPool, &srvMsg->q);
}
void transSendResponse(const STransMsg* pMsg) {
  if (pMsg->handle == NULL) {
    return;
  }
  SSrvConn*     pConn = pMsg->handle;
  SWorkThrdObj* pThrd = pConn->hostThrd;

  SSrvMsg* srvMsg = calloc(1, sizeof(SSrvMsg));
  srvMsg->pConn = pConn;
  srvMsg->msg = *pMsg;
  srvMsg->type = Normal;
  tTrace("server conn %p start to send resp", pConn);
  transSendAsync(pThrd->asyncPool, &srvMsg->q);
}
void transRegisterMsg(const STransMsg* msg) {
  if (msg->handle == NULL) {
    return;
  }
  SSrvConn*     pConn = msg->handle;
  SWorkThrdObj* pThrd = pConn->hostThrd;

  SSrvMsg* srvMsg = calloc(1, sizeof(SSrvMsg));
  srvMsg->pConn = pConn;
  srvMsg->msg = *msg;
  srvMsg->type = Register;
  tTrace("server conn %p start to register brokenlink callback", pConn);
  transSendAsync(pThrd->asyncPool, &srvMsg->q);
}
int transGetConnInfo(void* thandle, STransHandleInfo* pInfo) {
  SSrvConn*          pConn = thandle;
  struct sockaddr_in addr = pConn->addr;

  pInfo->clientIp = (uint32_t)(addr.sin_addr.s_addr);
  pInfo->clientPort = ntohs(addr.sin_port);
  tstrncpy(pInfo->user, pConn->user, sizeof(pInfo->user));
  return 0;
}

#endif
