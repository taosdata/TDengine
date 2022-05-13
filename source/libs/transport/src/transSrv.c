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

static TdThreadOnce transModuleInit = PTHREAD_ONCE_INIT;

static char* notify = "a";
static int   transSrvInst = 0;

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

  int64_t refId;
  int     spi;
  char    info[64];
  char    user[TSDB_UNI_LEN];  // user ID for the link
  char    secret[TSDB_PASSWORD_LEN];
  char    ckey[TSDB_PASSWORD_LEN];  // ciphering key
} SSrvConn;

typedef struct SSrvMsg {
  SSrvConn*     pConn;
  STransMsg     msg;
  queue         q;
  STransMsgType type;
} SSrvMsg;

typedef struct SWorkThrdObj {
  TdThread      thread;
  uv_connect_t  connect_req;
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
  int            numOfWorkerReady;
  SWorkThrdObj** pThreadObj;

  uv_pipe_t   pipeListen;
  uv_pipe_t** pipe;
  uint32_t    ip;
  uint32_t    port;
  uv_async_t* pAcceptAsync;  // just to quit from from accept thread

  bool inited;
} SServerObj;

// handle
typedef struct SExHandle {
  void*         handle;
  int64_t       refId;
  SWorkThrdObj* pThrd;
} SExHandle;

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

/*
 * time-consuming task throwed into BG work thread
 */
static void uvWorkDoTask(uv_work_t* req);
static void uvWorkAfterTask(uv_work_t* req, int status);

static void uvWalkCb(uv_handle_t* handle, void* arg);
static void uvFreeCb(uv_handle_t* handle);

static void uvStartSendRespInternal(SSrvMsg* smsg);
static void uvPrepareSendData(SSrvMsg* msg, uv_buf_t* wb);
static void uvStartSendResp(SSrvMsg* msg);

static void uvNotifyLinkBrokenToApp(SSrvConn* conn);

static void destroySmsg(SSrvMsg* smsg);
// check whether already read complete packet
static SSrvConn* createConn(void* hThrd);
static void      destroyConn(SSrvConn* conn, bool clear /*clear handle or not*/);
static int       reallocConnRefHandle(SSrvConn* conn);

static void uvHandleQuit(SSrvMsg* msg, SWorkThrdObj* thrd);
static void uvHandleRelease(SSrvMsg* msg, SWorkThrdObj* thrd);
static void uvHandleResp(SSrvMsg* msg, SWorkThrdObj* thrd);
static void uvHandleRegister(SSrvMsg* msg, SWorkThrdObj* thrd);
static void (*transAsyncHandle[])(SSrvMsg* msg, SWorkThrdObj* thrd) = {uvHandleResp, uvHandleQuit, uvHandleRelease,
                                                                       uvHandleRegister};

static int32_t exHandlesMgt;

void       uvInitEnv();
void       uvOpenExHandleMgt(int size);
void       uvCloseExHandleMgt();
int64_t    uvAddExHandle(void* p);
int32_t    uvRemoveExHandle(int64_t refId);
int32_t    uvReleaseExHandle(int64_t refId);
void       uvDestoryExHandle(void* handle);
SExHandle* uvAcquireExHandle(int64_t refId);

static void uvDestroyConn(uv_handle_t* handle);

// server and worker thread
static void* transWorkerThread(void* arg);
static void* transAcceptThread(void* arg);

// add handle loop
static bool addHandleToWorkloop(SWorkThrdObj* pThrd, char* pipeName);
static bool addHandleToAcceptloop(void* arg);

#define CONN_SHOULD_RELEASE(conn, head)                                     \
  do {                                                                      \
    if ((head)->release == 1 && (head->msgLen) == sizeof(*head)) {          \
      conn->status = ConnRelease;                                           \
      transClearBuffer(&conn->readBuf);                                     \
      transFreeMsg(transContFromHead((char*)head));                         \
      tTrace("server conn %p received release request", conn);              \
                                                                            \
      STransMsg tmsg = {.code = 0, .handle = (void*)conn, .ahandle = NULL}; \
      SSrvMsg*  srvMsg = taosMemoryCalloc(1, sizeof(SSrvMsg));              \
      srvMsg->msg = tmsg;                                                   \
      srvMsg->type = Release;                                               \
      srvMsg->pConn = conn;                                                 \
      reallocConnRefHandle(conn);                                           \
      if (!transQueuePush(&conn->srvMsgs, srvMsg)) {                        \
        return;                                                             \
      }                                                                     \
      uvStartSendRespInternal(srvMsg);                                      \
      return;                                                               \
    }                                                                       \
  } while (0)

#define SRV_RELEASE_UV(loop)       \
  do {                             \
    uv_walk(loop, uvWalkCb, NULL); \
    uv_run(loop, UV_RUN_DEFAULT);  \
    uv_loop_close(loop);           \
  } while (0);

#define ASYNC_ERR_JRET(thrd)                            \
  do {                                                  \
    if (thrd->quit) {                                   \
      tTrace("worker thread already quit, ignore msg"); \
      goto _return1;                                    \
    }                                                   \
  } while (0)

#define ASYNC_CHECK_HANDLE(exh1, refId)                                                                               \
  do {                                                                                                                \
    if (refId > 0) {                                                                                                  \
      tTrace("server handle step1");                                                                                  \
      SExHandle* exh2 = uvAcquireExHandle(refId);                                                                     \
      if (exh2 == NULL || refId != exh2->refId) {                                                                     \
        tTrace("server handle %p except, may already freed, ignore msg, ref1: %" PRIu64 ", ref2 : %" PRIu64 "", exh1, \
               exh2 ? exh2->refId : 0, refId);                                                                        \
        goto _return1;                                                                                                \
      }                                                                                                               \
    } else if (refId == 0) {                                                                                          \
      tTrace("server handle step2");                                                                                  \
      SExHandle* exh2 = uvAcquireExHandle(refId);                                                                     \
      if (exh2 == NULL || refId != exh2->refId) {                                                                     \
        tTrace("server handle %p except, may already freed, ignore msg, ref1: %" PRIu64 ", ref2 : %" PRIu64 "", exh1, \
               refId, exh2 ? exh2->refId : 0);                                                                        \
        goto _return1;                                                                                                \
      } else {                                                                                                        \
        refId = exh1->refId;                                                                                          \
      }                                                                                                               \
    } else if (refId == -1) {                                                                                         \
      tTrace("server handle step3");                                                                                  \
      goto _return2;                                                                                                  \
    }                                                                                                                 \
  } while (0)

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
  pHead->code = htonl(pHead->code);
  pHead->msgLen = htonl(pHead->msgLen);
  memcpy(pConn->user, pHead->user, strlen(pHead->user));

  // TODO(dengyihao): time-consuming task throwed into BG Thread
  //  uv_work_t* wreq = taosMemoryMalloc(sizeof(uv_work_t));
  //  wreq->data = pConn;
  //  uv_read_stop((uv_stream_t*)pConn->pTcp);
  //  transRefSrvHandle(pConn);
  //  uv_queue_work(((SWorkThrdObj*)pConn->hostThrd)->loop, wreq, uvWorkDoTask, uvWorkAfterTask);

  CONN_SHOULD_RELEASE(pConn, pHead);

  STransMsg transMsg;
  transMsg.contLen = transContLenFromMsg(pHead->msgLen);
  transMsg.pCont = pHead->content;
  transMsg.msgType = pHead->msgType;
  transMsg.code = pHead->code;
  transMsg.ahandle = (void*)pHead->ahandle;
  transMsg.handle = NULL;

  // transDestroyBuffer(&pConn->readBuf);
  transClearBuffer(&pConn->readBuf);
  pConn->inType = pHead->msgType;
  if (pConn->status == ConnNormal) {
    if (pHead->persist == 1) {
      pConn->status = ConnAcquire;
      transRefSrvHandle(pConn);
      tDebug("server conn %p acquired by server app", pConn);
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

  // if pHead->noResp = 1,
  // 1. server application should not send resp on handle
  // 2. once send out data, cli conn released to conn pool immediately
  // 3. not mixed with persist

  transMsg.handle = (void*)uvAcquireExHandle(pConn->refId);
  tTrace("server handle %p conn: %p translated to app, refId: %" PRIu64 "", transMsg.handle, pConn, pConn->refId);
  transMsg.refId = pConn->refId;
  assert(transMsg.handle != NULL);
  if (pHead->noResp == 1) {
    transMsg.refId = -1;
  }
  uvReleaseExHandle(pConn->refId);

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
        tTrace("server conn %p broken, notify server app", conn);
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
  buf->base = taosMemoryCalloc(1, sizeof(char) * buf->len);
}

void uvOnTimeoutCb(uv_timer_t* handle) {
  // opt
  SSrvConn* pConn = handle->data;
  tError("server conn %p time out", pConn);
}

void uvOnSendCb(uv_write_t* req, int status) {
  SSrvConn* conn = req->data;
  // transClearBuffer(&conn->readBuf);
  if (status == 0) {
    tTrace("server conn %p data already was written on stream", conn);
    if (!transQueueEmpty(&conn->srvMsgs)) {
      SSrvMsg* msg = transQueuePop(&conn->srvMsgs);
      // if (msg->type == Release && conn->status != ConnNormal) {
      //  conn->status = ConnNormal;
      //  transUnrefSrvHandle(conn);
      //  reallocConnRefHandle(conn);
      //  destroySmsg(msg);
      //  transQueueClear(&conn->srvMsgs);
      //  return;
      //}
      destroySmsg(msg);
      // send second data, just use for push
      if (!transQueueEmpty(&conn->srvMsgs)) {
        msg = (SSrvMsg*)transQueueGet(&conn->srvMsgs, 0);
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
          taosMemoryFree(msg);

          msg = (SSrvMsg*)transQueueGet(&conn->srvMsgs, 0);
          if (msg != NULL) {
            uvStartSendRespInternal(msg);
          }
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
  uv_close((uv_handle_t*)req->data, uvFreeCb);
  // taosMemoryFree(req->data);
  taosMemoryFree(req);
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
  pHead->ahandle = (uint64_t)pMsg->ahandle;

  if (pConn->status == ConnNormal) {
    pHead->msgType = pConn->inType + 1;
  } else {
    if (smsg->type == Release) {
      pHead->msgType = 0;
      pConn->status = ConnNormal;
      transUnrefSrvHandle(pConn);
    } else {
      pHead->msgType = pMsg->msgType;
    }
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
  // uv_timer_stop(&pConn->pTimer);
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
  taosMemoryFree(smsg);
}
static void destroyAllConn(SWorkThrdObj* pThrd) {
  tTrace("thread %p destroy all conn ", pThrd);
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
    // release handle to rpc init
    if (msg->type == Quit) {
      (*transAsyncHandle[msg->type])(msg, pThrd);
      continue;
    } else {
      STransMsg transMsg = msg->msg;

      SExHandle* exh1 = transMsg.handle;
      int64_t    refId = transMsg.refId;
      SExHandle* exh2 = uvAcquireExHandle(refId);
      if (exh2 == NULL || exh1 != exh2) {
        tTrace("server handle except msg %p, ignore it", exh1);
        uvReleaseExHandle(refId);
        destroySmsg(msg);
        continue;
      }
      msg->pConn = exh1->handle;
      uvReleaseExHandle(refId);
      (*transAsyncHandle[msg->type])(msg, pThrd);
    }
  }
}
static void uvWalkCb(uv_handle_t* handle, void* arg) {
  if (!uv_is_closing(handle)) {
    uv_close(handle, NULL);
  }
}
static void uvFreeCb(uv_handle_t* handle) {
  //
  taosMemoryFree(handle);
}

static void uvAcceptAsyncCb(uv_async_t* async) {
  SServerObj* srv = async->data;
  tDebug("close server port %d", srv->port);
  uv_walk(srv->loop, uvWalkCb, NULL);
}

static void uvShutDownCb(uv_shutdown_t* req, int status) {
  if (status != 0) {
    tDebug("conn failed to shut down: %s", uv_err_name(status));
  }
  uv_close((uv_handle_t*)req->handle, uvDestroyConn);
  taosMemoryFree(req);
}

static void uvWorkDoTask(uv_work_t* req) {
  // doing time-consumeing task
  // only auth conn currently, add more func later
  tTrace("server conn %p start to be processed in BG Thread", req->data);
  return;
}

static void uvWorkAfterTask(uv_work_t* req, int status) {
  if (status != 0) {
    tTrace("server conn %p failed to processed ", req->data);
  }
  // Done time-consumeing task
  // add more func later
  // this func called in main loop
  tTrace("server conn %p already processed ", req->data);
  taosMemoryFree(req);
}

void uvOnAcceptCb(uv_stream_t* stream, int status) {
  if (status == -1) {
    return;
  }
  SServerObj* pObj = container_of(stream, SServerObj, server);

  uv_tcp_t* cli = (uv_tcp_t*)taosMemoryMalloc(sizeof(uv_tcp_t));
  uv_tcp_init(pObj->loop, cli);

  if (uv_accept(stream, (uv_stream_t*)cli) == 0) {
    if (pObj->numOfWorkerReady < pObj->numOfThreads) {
      tError("worker-threads are not ready for all, need %d instead of %d.", pObj->numOfThreads,
             pObj->numOfWorkerReady);
      uv_close((uv_handle_t*)cli, NULL);
      return;
    }

    uv_write_t* wr = (uv_write_t*)taosMemoryMalloc(sizeof(uv_write_t));
    wr->data = cli;
    uv_buf_t buf = uv_buf_init((char*)notify, strlen(notify));

    pObj->workerIdx = (pObj->workerIdx + 1) % pObj->numOfThreads;

    tTrace("new conntion accepted by main server, dispatch to %dth worker-thread", pObj->workerIdx);

    uv_write2(wr, (uv_stream_t*)&(pObj->pipe[pObj->workerIdx][0]), &buf, 1, (uv_stream_t*)cli, uvOnPipeWriteCb);
  } else {
    uv_close((uv_handle_t*)cli, NULL);
  }
}
void uvOnConnectionCb(uv_stream_t* q, ssize_t nread, const uv_buf_t* buf) {
  tTrace("server connection coming");
  if (nread < 0) {
    if (nread != UV_EOF) {
      tError("read error %s", uv_err_name(nread));
    }
    // TODO(log other failure reason)
    tError("failed to create connect: %p", q);
    taosMemoryFree(buf->base);
    uv_close((uv_handle_t*)q, NULL);
    // taosMemoryFree(q);
    return;
  }
  // free memory allocated by
  assert(nread == strlen(notify));
  assert(buf->base[0] == notify[0]);
  taosMemoryFree(buf->base);

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
  // uv_timer_init(pThrd->loop, &pConn->pTimer);
  // pConn->pTimer.data = pConn;

  pConn->hostThrd = pThrd;

  // init client handle
  pConn->pTcp = (uv_tcp_t*)taosMemoryMalloc(sizeof(uv_tcp_t));
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

void* transAcceptThread(void* arg) {
  // opt
  setThreadName("trans-accept");
  SServerObj* srv = (SServerObj*)arg;
  uv_run(srv->loop, UV_RUN_DEFAULT);

  return NULL;
}
void uvOnPipeConnectionCb(uv_connect_t* connect, int status) {
  if (status != 0) {
    return;
  }
  SWorkThrdObj* pThrd = container_of(connect, SWorkThrdObj, connect_req);
  uv_read_start((uv_stream_t*)pThrd->pipe, uvAllocConnBufferCb, uvOnConnectionCb);
}
static bool addHandleToWorkloop(SWorkThrdObj* pThrd, char* pipeName) {
  pThrd->loop = (uv_loop_t*)taosMemoryMalloc(sizeof(uv_loop_t));
  if (0 != uv_loop_init(pThrd->loop)) {
    return false;
  }

  uv_pipe_init(pThrd->loop, pThrd->pipe, 1);
  // int r = uv_pipe_open(pThrd->pipe, pThrd->fd);

  pThrd->pipe->data = pThrd;

  QUEUE_INIT(&pThrd->msg);
  taosThreadMutexInit(&pThrd->msgMtx, NULL);

  // conn set
  QUEUE_INIT(&pThrd->conn);

  pThrd->asyncPool = transCreateAsyncPool(pThrd->loop, 5, pThrd, uvWorkerAsyncCb);
  uv_pipe_connect(&pThrd->connect_req, pThrd->pipe, pipeName, uvOnPipeConnectionCb);
  // uv_read_start((uv_stream_t*)pThrd->pipe, uvAllocConnBufferCb, uvOnConnectionCb);
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
  srv->pAcceptAsync = taosMemoryCalloc(1, sizeof(uv_async_t));
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
    terrno = TSDB_CODE_RPC_PORT_EADDRINUSE;
    return false;
  }
  return true;
}
void* transWorkerThread(void* arg) {
  setThreadName("trans-worker");
  SWorkThrdObj* pThrd = (SWorkThrdObj*)arg;
  uv_run(pThrd->loop, UV_RUN_DEFAULT);

  return NULL;
}

static SSrvConn* createConn(void* hThrd) {
  SWorkThrdObj* pThrd = hThrd;

  SSrvConn* pConn = (SSrvConn*)taosMemoryCalloc(1, sizeof(SSrvConn));
  QUEUE_INIT(&pConn->queue);

  QUEUE_PUSH(&pThrd->conn, &pConn->queue);

  transQueueInit(&pConn->srvMsgs, NULL);

  memset(&pConn->regArg, 0, sizeof(pConn->regArg));
  pConn->broken = false;
  pConn->status = ConnNormal;

  SExHandle* exh = taosMemoryMalloc(sizeof(SExHandle));
  exh->handle = pConn;
  exh->pThrd = pThrd;
  exh->refId = uvAddExHandle(exh);
  uvAcquireExHandle(exh->refId);

  pConn->refId = exh->refId;
  transRefSrvHandle(pConn);
  tTrace("server handle %p, conn %p created, refId: %" PRId64 "", exh, pConn, pConn->refId);
  return pConn;
}

static void destroyConn(SSrvConn* conn, bool clear) {
  if (conn == NULL) {
    return;
  }

  transDestroyBuffer(&conn->readBuf);
  if (clear) {
    tTrace("server conn %p to be destroyed", conn);
    // uv_shutdown_t* req = taosMemoryMalloc(sizeof(uv_shutdown_t));
    uv_close((uv_handle_t*)conn->pTcp, uvDestroyConn);
    // uv_close(conn->pTcp)
    // uv_shutdown(req, (uv_stream_t*)conn->pTcp, uvShutDownCb);
  }
}
static int reallocConnRefHandle(SSrvConn* conn) {
  uvReleaseExHandle(conn->refId);
  uvRemoveExHandle(conn->refId);
  // avoid app continue to send msg on invalid handle
  SExHandle* exh = taosMemoryMalloc(sizeof(SExHandle));
  exh->handle = conn;
  exh->pThrd = conn->hostThrd;
  exh->refId = uvAddExHandle(exh);
  uvAcquireExHandle(exh->refId);
  conn->refId = exh->refId;

  return 0;
}
static void uvDestroyConn(uv_handle_t* handle) {
  SSrvConn* conn = handle->data;
  if (conn == NULL) {
    return;
  }
  SWorkThrdObj* thrd = conn->hostThrd;

  uvReleaseExHandle(conn->refId);
  uvRemoveExHandle(conn->refId);

  tDebug("server conn %p destroy", conn);
  // uv_timer_stop(&conn->pTimer);
  transQueueDestroy(&conn->srvMsgs);

  if (conn->regArg.init == 1) {
    transFreeMsg(conn->regArg.msg.pCont);
    conn->regArg.init = 0;
  }
  QUEUE_REMOVE(&conn->queue);
  taosMemoryFree(conn->pTcp);
  if (conn->regArg.init == 1) {
    transFreeMsg(conn->regArg.msg.pCont);
    conn->regArg.init = 0;
  }
  taosMemoryFree(conn);

  if (thrd->quit && QUEUE_IS_EMPTY(&thrd->conn)) {
    tTrace("work thread quit");
    uv_walk(thrd->loop, uvWalkCb, NULL);
  }
}
static void uvPipeListenCb(uv_stream_t* handle, int status) {
  ASSERT(status == 0);

  SServerObj* srv = container_of(handle, SServerObj, pipeListen);
  uv_pipe_t*  pipe = &(srv->pipe[srv->numOfWorkerReady][0]);
  ASSERT(0 == uv_pipe_init(srv->loop, pipe, 1));
  ASSERT(0 == uv_accept((uv_stream_t*)&srv->pipeListen, (uv_stream_t*)pipe));

  ASSERT(1 == uv_is_readable((uv_stream_t*)pipe));
  ASSERT(1 == uv_is_writable((uv_stream_t*)pipe));
  ASSERT(0 == uv_is_closing((uv_handle_t*)pipe));

  srv->numOfWorkerReady++;

  // ASSERT(0 == uv_listen((uv_stream_t*)&ctx.send.tcp, 512, uvOnAcceptCb));

  // r = uv_read_start((uv_stream_t*)&ctx.channel, alloc_cb, read_cb);
  // ASSERT(r == 0);
}

void* transInitServer(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle) {
  SServerObj* srv = taosMemoryCalloc(1, sizeof(SServerObj));
  srv->loop = (uv_loop_t*)taosMemoryMalloc(sizeof(uv_loop_t));
  srv->numOfThreads = numOfThreads;
  srv->workerIdx = 0;
  srv->numOfWorkerReady = 0;
  srv->pThreadObj = (SWorkThrdObj**)taosMemoryCalloc(srv->numOfThreads, sizeof(SWorkThrdObj*));
  srv->pipe = (uv_pipe_t**)taosMemoryCalloc(srv->numOfThreads, sizeof(uv_pipe_t*));
  srv->ip = ip;
  srv->port = port;
  uv_loop_init(srv->loop);

  taosThreadOnce(&transModuleInit, uvInitEnv);
  transSrvInst++;

  assert(0 == uv_pipe_init(srv->loop, &srv->pipeListen, 0));
#ifdef WINDOWS
  char pipeName[64];
  snprintf(pipeName, sizeof(pipeName), "\\\\?\\pipe\\trans.rpc.%p-%lu", taosSafeRand(), GetCurrentProcessId());
#else
  char pipeName[PATH_MAX] = {0};
  snprintf(pipeName, sizeof(pipeName), "%s%spipe.trans.rpc.%08X-%lu", tsTempDir, TD_DIRSEP, taosSafeRand(),
           taosGetSelfPthreadId());
#endif
  assert(0 == uv_pipe_bind(&srv->pipeListen, pipeName));
  assert(0 == uv_listen((uv_stream_t*)&srv->pipeListen, SOMAXCONN, uvPipeListenCb));

  for (int i = 0; i < srv->numOfThreads; i++) {
    SWorkThrdObj* thrd = (SWorkThrdObj*)taosMemoryCalloc(1, sizeof(SWorkThrdObj));
    thrd->pTransInst = shandle;
    thrd->quit = false;
    srv->pThreadObj[i] = thrd;
    thrd->pTransInst = shandle;

    srv->pipe[i] = (uv_pipe_t*)taosMemoryCalloc(2, sizeof(uv_pipe_t));
    thrd->pipe = &(srv->pipe[i][1]);  // init read

    if (false == addHandleToWorkloop(thrd, pipeName)) {
      goto End;
    }
    int err = taosThreadCreate(&(thrd->thread), NULL, transWorkerThread, (void*)(thrd));
    if (err == 0) {
      tDebug("sucess to create worker-thread %d", i);
      // printf("thread %d create\n", i);
    } else {
      // TODO: clear all other resource later
      tError("failed to create worker-thread %d", i);
      goto End;
    }
  }
  if (false == taosValidIpAndPort(srv->ip, srv->port)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    tError("invalid ip/port, reason: %s", terrstr());
    goto End;
  }
  if (false == addHandleToAcceptloop(srv)) {
    goto End;
  }
  int err = taosThreadCreate(&srv->thread, NULL, transAcceptThread, (void*)srv);
  if (err == 0) {
    tDebug("success to create accept-thread");
  } else {
    tError("failed  to create accept-thread");
    goto End;
    // clear all resource later
  }
  srv->inited = true;
  return srv;
End:
  transCloseServer(srv);
  return NULL;
}

void uvInitEnv() {
  uv_os_setenv("UV_TCP_SINGLE_ACCEPT", "1");
  uvOpenExHandleMgt(10000);
}
void uvOpenExHandleMgt(int size) {
  // added into once later
  exHandlesMgt = taosOpenRef(size, uvDestoryExHandle);
}
void uvCloseExHandleMgt() {
  // close ref
  taosCloseRef(exHandlesMgt);
}
int64_t uvAddExHandle(void* p) {
  // acquire extern handle
  return taosAddRef(exHandlesMgt, p);
}
int32_t uvRemoveExHandle(int64_t refId) {
  // acquire extern handle
  return taosRemoveRef(exHandlesMgt, refId);
}

SExHandle* uvAcquireExHandle(int64_t refId) {
  // acquire extern handle
  return (SExHandle*)taosAcquireRef(exHandlesMgt, refId);
}

int32_t uvReleaseExHandle(int64_t refId) {
  // release extern handle
  return taosReleaseRef(exHandlesMgt, refId);
}
void uvDestoryExHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  taosMemoryFree(handle);
}

void uvHandleQuit(SSrvMsg* msg, SWorkThrdObj* thrd) {
  thrd->quit = true;
  if (QUEUE_IS_EMPTY(&thrd->conn)) {
    uv_walk(thrd->loop, uvWalkCb, NULL);
  } else {
    destroyAllConn(thrd);
  }
  taosMemoryFree(msg);
}
void uvHandleRelease(SSrvMsg* msg, SWorkThrdObj* thrd) {
  SSrvConn* conn = msg->pConn;
  if (conn->status == ConnAcquire) {
    reallocConnRefHandle(conn);
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
  tDebug("server conn %p start to send resp (2/2)", msg->pConn);
  uvStartSendResp(msg);
}
void uvHandleRegister(SSrvMsg* msg, SWorkThrdObj* thrd) {
  SSrvConn* conn = msg->pConn;
  tDebug("server conn %p register brokenlink callback", conn);
  if (conn->status == ConnAcquire) {
    if (!transQueuePush(&conn->srvMsgs, msg)) {
      return;
    }
    transQueuePop(&conn->srvMsgs);
    conn->regArg.notifyCount = 0;
    conn->regArg.init = 1;
    conn->regArg.msg = msg->msg;
    tDebug("server conn %p register brokenlink callback succ", conn);

    if (conn->broken) {
      STrans* pTransInst = conn->pTransInst;
      (*pTransInst->cfp)(pTransInst->parent, &(conn->regArg.msg), NULL);
      memset(&conn->regArg, 0, sizeof(conn->regArg));
    }
    taosMemoryFree(msg);
  }
}
void destroyWorkThrd(SWorkThrdObj* pThrd) {
  if (pThrd == NULL) {
    return;
  }
  taosThreadJoin(pThrd->thread, NULL);
  SRV_RELEASE_UV(pThrd->loop);
  transDestroyAsyncPool(pThrd->asyncPool);
  taosMemoryFree(pThrd->loop);
  taosMemoryFree(pThrd);
}
void sendQuitToWorkThrd(SWorkThrdObj* pThrd) {
  SSrvMsg* msg = taosMemoryCalloc(1, sizeof(SSrvMsg));
  msg->type = Quit;
  tDebug("server send quit msg to work thread");
  transSendAsync(pThrd->asyncPool, &msg->q);
}

void transCloseServer(void* arg) {
  // impl later
  SServerObj* srv = arg;

  tDebug("send quit msg to accept thread");
  if (srv->inited) {
    uv_async_send(srv->pAcceptAsync);
    taosThreadJoin(srv->thread, NULL);
  }
  SRV_RELEASE_UV(srv->loop);

  for (int i = 0; i < srv->numOfThreads; i++) {
    sendQuitToWorkThrd(srv->pThreadObj[i]);
    destroyWorkThrd(srv->pThreadObj[i]);
  }

  taosMemoryFree(srv->pThreadObj);
  taosMemoryFree(srv->pAcceptAsync);
  taosMemoryFree(srv->loop);

  for (int i = 0; i < srv->numOfThreads; i++) {
    taosMemoryFree(srv->pipe[i]);
  }
  taosMemoryFree(srv->pipe);

  taosMemoryFree(srv);

  transSrvInst--;
  if (transSrvInst == 0) {
    TdThreadOnce tmpInit = PTHREAD_ONCE_INIT;
    memcpy(&transModuleInit, &tmpInit, sizeof(TdThreadOnce));
    uvCloseExHandleMgt();
  }
}

void transRefSrvHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  int ref = T_REF_INC((SSrvConn*)handle);
  tDebug("server conn %p ref count: %d", handle, ref);
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
  SExHandle* exh = handle;
  int64_t    refId = exh->refId;

  ASYNC_CHECK_HANDLE(exh, refId);

  SWorkThrdObj* pThrd = exh->pThrd;
  ASYNC_ERR_JRET(pThrd);

  STransMsg tmsg = {.code = 0, .handle = exh, .ahandle = NULL, .refId = refId};

  SSrvMsg* srvMsg = taosMemoryCalloc(1, sizeof(SSrvMsg));
  srvMsg->msg = tmsg;
  srvMsg->type = Release;

  tTrace("server conn %p start to release", exh->handle);
  transSendAsync(pThrd->asyncPool, &srvMsg->q);
  uvReleaseExHandle(refId);
  return;
_return1:
  tTrace("server handle %p failed to send to release handle", exh);
  uvReleaseExHandle(refId);
  return;
_return2:
  tTrace("server handle %p failed to send to release handle", exh);
  return;
}
void transSendResponse(const STransMsg* msg) {
  SExHandle* exh = msg->handle;
  int64_t    refId = msg->refId;
  ASYNC_CHECK_HANDLE(exh, refId);
  assert(refId != 0);

  STransMsg tmsg = *msg;
  tmsg.refId = refId;

  SWorkThrdObj* pThrd = exh->pThrd;
  ASYNC_ERR_JRET(pThrd);

  SSrvMsg* srvMsg = taosMemoryCalloc(1, sizeof(SSrvMsg));
  srvMsg->msg = tmsg;
  srvMsg->type = Normal;
  tDebug("server conn %p start to send resp (1/2)", exh->handle);
  transSendAsync(pThrd->asyncPool, &srvMsg->q);
  uvReleaseExHandle(refId);
  return;
_return1:
  tTrace("server handle %p failed to send resp", exh);
  rpcFreeCont(msg->pCont);
  uvReleaseExHandle(refId);
  return;
_return2:
  tTrace("server handle %p failed to send resp", exh);
  rpcFreeCont(msg->pCont);
  return;
}
void transRegisterMsg(const STransMsg* msg) {
  SExHandle* exh = msg->handle;
  int64_t    refId = msg->refId;
  ASYNC_CHECK_HANDLE(exh, refId);

  STransMsg tmsg = *msg;
  tmsg.refId = refId;

  SWorkThrdObj* pThrd = exh->pThrd;
  ASYNC_ERR_JRET(pThrd);

  SSrvMsg* srvMsg = taosMemoryCalloc(1, sizeof(SSrvMsg));
  srvMsg->msg = tmsg;
  srvMsg->type = Register;
  tTrace("server conn %p start to register brokenlink callback", exh->handle);
  transSendAsync(pThrd->asyncPool, &srvMsg->q);
  uvReleaseExHandle(refId);
  return;

_return1:
  tTrace("server handle %p failed to send to register brokenlink", exh);
  rpcFreeCont(msg->pCont);
  uvReleaseExHandle(refId);
  return;
_return2:
  tTrace("server handle %p failed to send to register brokenlink", exh);
  rpcFreeCont(msg->pCont);
}

int transGetConnInfo(void* thandle, STransHandleInfo* pInfo) {
  if (thandle == NULL) {
    tTrace("invalid handle %p, failed to Get Conn info", thandle);
    return -1;
  }
  SExHandle* ex = thandle;
  SSrvConn*  pConn = ex->handle;

  struct sockaddr_in addr = pConn->addr;
  pInfo->clientIp = (uint32_t)(addr.sin_addr.s_addr);
  pInfo->clientPort = ntohs(addr.sin_port);
  tstrncpy(pInfo->user, pConn->user, sizeof(pInfo->user));
  return 0;
}

#endif
