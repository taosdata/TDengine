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

static char*   notify = "a";
static int32_t tranSSvrInst = 0;
static int32_t refMgt = 0;

typedef struct {
  int       notifyCount;  //
  int       init;         // init or not
  STransMsg msg;
} SSvrRegArg;

typedef struct SSvrConn {
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

  SSvrRegArg regArg;
  bool       broken;  // conn broken;

  ConnStatus         status;
  struct sockaddr_in addr;
  struct sockaddr_in localAddr;

  int64_t refId;
  int     spi;
  char    info[64];
  char    user[TSDB_UNI_LEN];  // user ID for the link
  char    secret[TSDB_PASSWORD_LEN];
  char    ckey[TSDB_PASSWORD_LEN];  // ciphering key
} SSvrConn;

typedef struct SSvrMsg {
  SSvrConn*     pConn;
  STransMsg     msg;
  queue         q;
  STransMsgType type;
} SSvrMsg;

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

static void uvStartSendRespInternal(SSvrMsg* smsg);
static void uvPrepareSendData(SSvrMsg* msg, uv_buf_t* wb);
static void uvStartSendResp(SSvrMsg* msg);

static void uvNotifyLinkBrokenToApp(SSvrConn* conn);

static void destroySmsg(SSvrMsg* smsg);
// check whether already read complete packet
static SSvrConn* createConn(void* hThrd);
static void      destroyConn(SSvrConn* conn, bool clear /*clear handle or not*/);
static void      destroyConnRegArg(SSvrConn* conn);

static int reallocConnRefHandle(SSvrConn* conn);

static void uvHandleQuit(SSvrMsg* msg, SWorkThrdObj* thrd);
static void uvHandleRelease(SSvrMsg* msg, SWorkThrdObj* thrd);
static void uvHandleResp(SSvrMsg* msg, SWorkThrdObj* thrd);
static void uvHandleRegister(SSvrMsg* msg, SWorkThrdObj* thrd);
static void (*transAsyncHandle[])(SSvrMsg* msg, SWorkThrdObj* thrd) = {uvHandleResp, uvHandleQuit, uvHandleRelease,
                                                                       uvHandleRegister, NULL};

static int32_t exHandlesMgt;

// void       uvInitEnv();
// void       uvOpenExHandleMgt(int size);
// void       uvCloseExHandleMgt();
// int64_t    uvAddExHandle(void* p);
// int32_t    uvRemoveExHandle(int64_t refId);
// int32_t    uvReleaseExHandle(int64_t refId);
// void       uvDestoryExHandle(void* handle);
// SExHandle* uvAcquireExHandle(int64_t refId);

static void uvDestroyConn(uv_handle_t* handle);

// server and worker thread
static void* transWorkerThread(void* arg);
static void* transAcceptThread(void* arg);

// add handle loop
static bool addHandleToWorkloop(SWorkThrdObj* pThrd, char* pipeName);
static bool addHandleToAcceptloop(void* arg);

#define CONN_SHOULD_RELEASE(conn, head)                                               \
  do {                                                                                \
    if ((head)->release == 1 && (head->msgLen) == sizeof(*head)) {                    \
      conn->status = ConnRelease;                                                     \
      transClearBuffer(&conn->readBuf);                                               \
      transFreeMsg(transContFromHead((char*)head));                                   \
      tTrace("conn %p received release request", conn);                               \
                                                                                      \
      STransMsg tmsg = {.code = 0, .info.handle = (void*)conn, .info.ahandle = NULL}; \
      SSvrMsg*  srvMsg = taosMemoryCalloc(1, sizeof(SSvrMsg));                        \
      srvMsg->msg = tmsg;                                                             \
      srvMsg->type = Release;                                                         \
      srvMsg->pConn = conn;                                                           \
      reallocConnRefHandle(conn);                                                     \
      if (!transQueuePush(&conn->srvMsgs, srvMsg)) {                                  \
        return;                                                                       \
      }                                                                               \
      if (conn->regArg.init) {                                                        \
        tTrace("conn %p release, notify server app", conn);                           \
        STrans* pTransInst = conn->pTransInst;                                        \
        (*pTransInst->cfp)(pTransInst->parent, &(conn->regArg.msg), NULL);            \
        memset(&conn->regArg, 0, sizeof(conn->regArg));                               \
      }                                                                               \
      uvStartSendRespInternal(srvMsg);                                                \
      return;                                                                         \
    }                                                                                 \
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
      tTrace("handle step1");                                                                                         \
      SExHandle* exh2 = transAcquireExHandle(refMgt, refId);                                                          \
      if (exh2 == NULL || refId != exh2->refId) {                                                                     \
        tTrace("handle %p except, may already freed, ignore msg, ref1: %" PRIu64 ", ref2 : %" PRIu64 "", exh1,        \
               exh2 ? exh2->refId : 0, refId);                                                                        \
        goto _return1;                                                                                                \
      }                                                                                                               \
    } else if (refId == 0) {                                                                                          \
      tTrace("handle step2");                                                                                         \
      SExHandle* exh2 = transAcquireExHandle(refMgt, refId);                                                          \
      if (exh2 == NULL || refId != exh2->refId) {                                                                     \
        tTrace("handle %p except, may already freed, ignore msg, ref1: %" PRIu64 ", ref2 : %" PRIu64 "", exh1, refId, \
               exh2 ? exh2->refId : 0);                                                                               \
        goto _return1;                                                                                                \
      } else {                                                                                                        \
        refId = exh1->refId;                                                                                          \
      }                                                                                                               \
    } else if (refId < 0) {                                                                                           \
      tTrace("handle step3");                                                                                         \
      goto _return2;                                                                                                  \
    }                                                                                                                 \
  } while (0)

void uvAllocRecvBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  SSvrConn*    conn = handle->data;
  SConnBuffer* pBuf = &conn->readBuf;
  transAllocBuffer(pBuf, buf);
}

// refers specifically to query or insert timeout
static void uvHandleActivityTimeout(uv_timer_t* handle) {
  SSvrConn* conn = handle->data;
  tDebug("%p timeout since no activity", conn);
}

static void uvHandleReq(SSvrConn* pConn) {
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
  memset(&transMsg, 0, sizeof(transMsg));
  transMsg.contLen = transContLenFromMsg(pHead->msgLen);
  transMsg.pCont = pHead->content;
  transMsg.msgType = pHead->msgType;
  transMsg.code = pHead->code;

  transClearBuffer(&pConn->readBuf);

  pConn->inType = pHead->msgType;
  if (pConn->status == ConnNormal) {
    if (pHead->persist == 1) {
      pConn->status = ConnAcquire;
      transRefSrvHandle(pConn);
      tDebug("conn %p acquired by server app", pConn);
    }
  }
  STraceId* trace = &pHead->traceId;
  if (pConn->status == ConnNormal && pHead->noResp == 0) {
    transRefSrvHandle(pConn);

    tGTrace("conn %p %s received from %s:%d, local info: %s:%d, msg size: %d", pConn, TMSG_INFO(transMsg.msgType),
            taosInetNtoa(pConn->addr.sin_addr), ntohs(pConn->addr.sin_port), taosInetNtoa(pConn->localAddr.sin_addr),
            ntohs(pConn->localAddr.sin_port), transMsg.contLen);
  } else {
    tGTrace("conn %p %s received from %s:%d, local info: %s:%d, msg size: %d, resp:%d, code: %d", pConn,
            TMSG_INFO(transMsg.msgType), taosInetNtoa(pConn->addr.sin_addr), ntohs(pConn->addr.sin_port),
            taosInetNtoa(pConn->localAddr.sin_addr), ntohs(pConn->localAddr.sin_port), transMsg.contLen, pHead->noResp,
            transMsg.code);
    // no ref here
  }

  // pHead->noResp = 1,
  // 1. server application should not send resp on handle
  // 2. once send out data, cli conn released to conn pool immediately
  // 3. not mixed with persist
  transMsg.info.ahandle = (void*)pHead->ahandle;
  transMsg.info.handle = (void*)transAcquireExHandle(refMgt, pConn->refId);
  transMsg.info.refId = pConn->refId;
  transMsg.info.traceId = pHead->traceId;

  tGTrace("handle %p conn: %p translated to app, refId: %" PRIu64 "", transMsg.info.handle, pConn, pConn->refId);
  assert(transMsg.info.handle != NULL);

  if (pHead->noResp == 1) {
    transMsg.info.refId = -1;
  }

  // set up conn info
  SRpcConnInfo* pConnInfo = &(transMsg.info.conn);
  pConnInfo->clientIp = (uint32_t)(pConn->addr.sin_addr.s_addr);
  pConnInfo->clientPort = ntohs(pConn->addr.sin_port);
  tstrncpy(pConnInfo->user, pConn->user, sizeof(pConnInfo->user));

  transReleaseExHandle(refMgt, pConn->refId);

  STrans* pTransInst = pConn->pTransInst;
  (*pTransInst->cfp)(pTransInst->parent, &transMsg, NULL);
  // uv_timer_start(&pConn->pTimer, uvHandleActivityTimeout, pRpc->idleTime * 10000, 0);
}

void uvOnRecvCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf) {
  // opt
  SSvrConn*    conn = cli->data;
  SConnBuffer* pBuf = &conn->readBuf;
  if (nread > 0) {
    pBuf->len += nread;
    tTrace("conn %p total read: %d, current read: %d", conn, pBuf->len, (int)nread);
    if (transReadComplete(pBuf)) {
      tTrace("conn %p alread read complete packet", conn);
      uvHandleReq(conn);
    } else {
      tTrace("conn %p read partial packet, continue to read", conn);
    }
    return;
  }
  if (nread == 0) {
    return;
  }

  tError("conn %p read error: %s", conn, uv_err_name(nread));
  if (nread < 0) {
    conn->broken = true;
    if (conn->status == ConnAcquire) {
      if (conn->regArg.init) {
        tTrace("conn %p broken, notify server app", conn);
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
  SSvrConn* pConn = handle->data;
  tError("conn %p time out", pConn);
}

void uvOnSendCb(uv_write_t* req, int status) {
  SSvrConn* conn = req->data;
  // transClearBuffer(&conn->readBuf);
  if (status == 0) {
    tTrace("conn %p data already was written on stream", conn);
    if (!transQueueEmpty(&conn->srvMsgs)) {
      SSvrMsg* msg = transQueuePop(&conn->srvMsgs);
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
        msg = (SSvrMsg*)transQueueGet(&conn->srvMsgs, 0);
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

          msg = (SSvrMsg*)transQueueGet(&conn->srvMsgs, 0);
          if (msg != NULL) {
            uvStartSendRespInternal(msg);
          }
        } else {
          uvStartSendRespInternal(msg);
        }
      }
    }
  } else {
    tError("conn %p failed to write data, %s", conn, uv_err_name(status));
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

static void uvPrepareSendData(SSvrMsg* smsg, uv_buf_t* wb) {
  SSvrConn*  pConn = smsg->pConn;
  STransMsg* pMsg = &smsg->msg;
  if (pMsg->pCont == 0) {
    pMsg->pCont = (void*)rpcMallocCont(0);
    pMsg->contLen = 0;
  }
  STransMsgHead* pHead = transHeadFromCont(pMsg->pCont);
  pHead->ahandle = (uint64_t)pMsg->info.ahandle;
  pHead->traceId = pMsg->info.traceId;

  if (pConn->status == ConnNormal) {
    pHead->msgType = pConn->inType + 1;
  } else {
    if (smsg->type == Release) {
      pHead->msgType = 0;
      pConn->status = ConnNormal;

      destroyConnRegArg(pConn);
      transUnrefSrvHandle(pConn);
    } else {
      pHead->msgType = pMsg->msgType;
    }
  }

  pHead->release = smsg->type == Release ? 1 : 0;
  pHead->code = htonl(pMsg->code);

  char*   msg = (char*)pHead;
  int32_t len = transMsgLenFromCont(pMsg->contLen);

  STraceId* trace = &pMsg->info.traceId;
  tGTrace("conn %p %s is sent to %s:%d, local info: %s:%d, msglen:%d", pConn, TMSG_INFO(pHead->msgType),
          taosInetNtoa(pConn->addr.sin_addr), ntohs(pConn->addr.sin_port), taosInetNtoa(pConn->localAddr.sin_addr),
          ntohs(pConn->localAddr.sin_port), len);
  pHead->msgLen = htonl(len);

  wb->base = msg;
  wb->len = len;
}

static void uvStartSendRespInternal(SSvrMsg* smsg) {
  uv_buf_t wb;
  uvPrepareSendData(smsg, &wb);

  SSvrConn* pConn = smsg->pConn;
  // uv_timer_stop(&pConn->pTimer);
  uv_write(&pConn->pWriter, (uv_stream_t*)pConn->pTcp, &wb, 1, uvOnSendCb);
}
static void uvStartSendResp(SSvrMsg* smsg) {
  // impl
  SSvrConn* pConn = smsg->pConn;

  if (pConn->broken == true) {
    // persist by
    transFreeMsg(smsg->msg.pCont);
    taosMemoryFree(smsg);
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

static void destroySmsg(SSvrMsg* smsg) {
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

    SSvrConn* c = QUEUE_DATA(h, SSvrConn, queue);
    while (T_REF_VAL_GET(c) >= 2) {
      transUnrefSrvHandle(c);
    }
    transUnrefSrvHandle(c);
  }
}
void uvWorkerAsyncCb(uv_async_t* handle) {
  SAsyncItem*   item = handle->data;
  SWorkThrdObj* pThrd = item->pThrd;
  SSvrConn*     conn = NULL;
  queue         wq;

  // batch process to avoid to lock/unlock frequently
  taosThreadMutexLock(&item->mtx);
  QUEUE_MOVE(&item->qmsg, &wq);
  taosThreadMutexUnlock(&item->mtx);

  while (!QUEUE_IS_EMPTY(&wq)) {
    queue* head = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(head);

    SSvrMsg* msg = QUEUE_DATA(head, SSvrMsg, q);
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

      SExHandle* exh1 = transMsg.info.handle;
      int64_t    refId = transMsg.info.refId;
      SExHandle* exh2 = transAcquireExHandle(refMgt, refId);
      if (exh2 == NULL || exh1 != exh2) {
        tTrace("handle except msg %p, ignore it", exh1);
        transReleaseExHandle(refMgt, refId);
        destroySmsg(msg);
        continue;
      }
      msg->pConn = exh1->handle;
      transReleaseExHandle(refMgt, refId);
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
  tTrace("conn %p start to be processed in BG Thread", req->data);
  return;
}

static void uvWorkAfterTask(uv_work_t* req, int status) {
  if (status != 0) {
    tTrace("conn %p failed to processed ", req->data);
  }
  // Done time-consumeing task
  // add more func later
  // this func called in main loop
  tTrace("conn %p already processed ", req->data);
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
  tTrace("connection coming");
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

  SSvrConn* pConn = createConn(pThrd);

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
    tTrace("conn %p created, fd: %d", pConn, fd);

    int addrlen = sizeof(pConn->addr);
    if (0 != uv_tcp_getpeername(pConn->pTcp, (struct sockaddr*)&pConn->addr, &addrlen)) {
      tError("conn %p failed to get peer info", pConn);
      transUnrefSrvHandle(pConn);
      return;
    }

    addrlen = sizeof(pConn->localAddr);
    if (0 != uv_tcp_getsockname(pConn->pTcp, (struct sockaddr*)&pConn->localAddr, &addrlen)) {
      tError("conn %p failed to get local info", pConn);
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

static SSvrConn* createConn(void* hThrd) {
  SWorkThrdObj* pThrd = hThrd;

  SSvrConn* pConn = (SSvrConn*)taosMemoryCalloc(1, sizeof(SSvrConn));
  QUEUE_INIT(&pConn->queue);

  QUEUE_PUSH(&pThrd->conn, &pConn->queue);

  transQueueInit(&pConn->srvMsgs, NULL);

  memset(&pConn->regArg, 0, sizeof(pConn->regArg));
  pConn->broken = false;
  pConn->status = ConnNormal;

  SExHandle* exh = taosMemoryMalloc(sizeof(SExHandle));
  exh->handle = pConn;
  exh->pThrd = pThrd;
  exh->refId = transAddExHandle(refMgt, exh);
  transAcquireExHandle(refMgt, exh->refId);

  pConn->refId = exh->refId;
  transRefSrvHandle(pConn);
  tTrace("handle %p, conn %p created, refId: %" PRId64 "", exh, pConn, pConn->refId);
  return pConn;
}

static void destroyConn(SSvrConn* conn, bool clear) {
  if (conn == NULL) {
    return;
  }

  transDestroyBuffer(&conn->readBuf);
  if (clear) {
    tTrace("conn %p to be destroyed", conn);
    // uv_shutdown_t* req = taosMemoryMalloc(sizeof(uv_shutdown_t));
    uv_close((uv_handle_t*)conn->pTcp, uvDestroyConn);
    // uv_close(conn->pTcp)
    // uv_shutdown(req, (uv_stream_t*)conn->pTcp, uvShutDownCb);
  }
}
static void destroyConnRegArg(SSvrConn* conn) {
  if (conn->regArg.init == 1) {
    transFreeMsg(conn->regArg.msg.pCont);
    conn->regArg.init = 0;
  }
}
static int reallocConnRefHandle(SSvrConn* conn) {
  transReleaseExHandle(refMgt, conn->refId);
  transRemoveExHandle(refMgt, conn->refId);
  // avoid app continue to send msg on invalid handle
  SExHandle* exh = taosMemoryMalloc(sizeof(SExHandle));
  exh->handle = conn;
  exh->pThrd = conn->hostThrd;
  exh->refId = transAddExHandle(refMgt, exh);
  transAcquireExHandle(refMgt, exh->refId);
  conn->refId = exh->refId;

  return 0;
}
static void uvDestroyConn(uv_handle_t* handle) {
  SSvrConn* conn = handle->data;
  if (conn == NULL) {
    return;
  }
  SWorkThrdObj* thrd = conn->hostThrd;

  transReleaseExHandle(refMgt, conn->refId);
  transRemoveExHandle(refMgt, conn->refId);

  tDebug("conn %p destroy", conn);
  // uv_timer_stop(&conn->pTimer);
  transQueueDestroy(&conn->srvMsgs);

  QUEUE_REMOVE(&conn->queue);
  taosMemoryFree(conn->pTcp);
  destroyConnRegArg(conn);
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

  // taosThreadOnce(&transModuleInit, uvInitEnv);
  int ref = atomic_add_fetch_32(&tranSSvrInst, 1);
  if (ref == 1) {
    refMgt = transOpenExHandleMgt(50000);
  }

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
    tError("invalid ip/port, %d:%d, reason: %s", srv->ip, srv->port, terrstr());
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

void uvHandleQuit(SSvrMsg* msg, SWorkThrdObj* thrd) {
  thrd->quit = true;
  if (QUEUE_IS_EMPTY(&thrd->conn)) {
    uv_walk(thrd->loop, uvWalkCb, NULL);
  } else {
    destroyAllConn(thrd);
  }
  taosMemoryFree(msg);
}
void uvHandleRelease(SSvrMsg* msg, SWorkThrdObj* thrd) {
  SSvrConn* conn = msg->pConn;
  if (conn->status == ConnAcquire) {
    reallocConnRefHandle(conn);
    if (!transQueuePush(&conn->srvMsgs, msg)) {
      return;
    }
    uvStartSendRespInternal(msg);
    return;
  } else if (conn->status == ConnRelease || conn->status == ConnNormal) {
    tDebug("conn %p already released, ignore release-msg", conn);
  }
  destroySmsg(msg);
}
void uvHandleResp(SSvrMsg* msg, SWorkThrdObj* thrd) {
  // send msg to client
  tDebug("conn %p start to send resp (2/2)", msg->pConn);
  uvStartSendResp(msg);
}
void uvHandleRegister(SSvrMsg* msg, SWorkThrdObj* thrd) {
  SSvrConn* conn = msg->pConn;
  tDebug("conn %p register brokenlink callback", conn);
  if (conn->status == ConnAcquire) {
    if (!transQueuePush(&conn->srvMsgs, msg)) {
      return;
    }
    transQueuePop(&conn->srvMsgs);
    conn->regArg.notifyCount = 0;
    conn->regArg.init = 1;
    conn->regArg.msg = msg->msg;
    tDebug("conn %p register brokenlink callback succ", conn);

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
  TRANS_DESTROY_ASYNC_POOL_MSG(pThrd->asyncPool, SSvrMsg, destroySmsg);
  transDestroyAsyncPool(pThrd->asyncPool);
  taosMemoryFree(pThrd->loop);
  taosMemoryFree(pThrd);
}
void sendQuitToWorkThrd(SWorkThrdObj* pThrd) {
  SSvrMsg* msg = taosMemoryCalloc(1, sizeof(SSvrMsg));
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

  int ref = atomic_sub_fetch_32(&tranSSvrInst, 1);
  if (ref == 0) {
    // TdThreadOnce tmpInit = PTHREAD_ONCE_INIT;
    // memcpy(&transModuleInit, &tmpInit, sizeof(TdThreadOnce));
    transCloseExHandleMgt(refMgt);
  }
}

void transRefSrvHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  int ref = T_REF_INC((SSvrConn*)handle);
  tDebug("conn %p ref count: %d", handle, ref);
}

void transUnrefSrvHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  int ref = T_REF_DEC((SSvrConn*)handle);
  tDebug("conn %p ref count: %d", handle, ref);
  if (ref == 0) {
    destroyConn((SSvrConn*)handle, true);
  }
}

void transReleaseSrvHandle(void* handle) {
  SExHandle* exh = handle;
  int64_t    refId = exh->refId;

  ASYNC_CHECK_HANDLE(exh, refId);

  SWorkThrdObj* pThrd = exh->pThrd;
  ASYNC_ERR_JRET(pThrd);

  STransMsg tmsg = {.code = 0, .info.handle = exh, .info.ahandle = NULL, .info.refId = refId};

  SSvrMsg* m = taosMemoryCalloc(1, sizeof(SSvrMsg));
  m->msg = tmsg;
  m->type = Release;

  tTrace("conn %p start to release", exh->handle);
  transSendAsync(pThrd->asyncPool, &m->q);
  transReleaseExHandle(refMgt, refId);
  return;
_return1:
  tTrace("handle %p failed to send to release handle", exh);
  transReleaseExHandle(refMgt, refId);
  return;
_return2:
  tTrace("handle %p failed to send to release handle", exh);
  return;
}
void transSendResponse(const STransMsg* msg) {
  SExHandle* exh = msg->info.handle;
  int64_t    refId = msg->info.refId;
  ASYNC_CHECK_HANDLE(exh, refId);
  assert(refId != 0);

  STransMsg tmsg = *msg;
  tmsg.info.refId = refId;

  SWorkThrdObj* pThrd = exh->pThrd;
  ASYNC_ERR_JRET(pThrd);

  SSvrMsg* m = taosMemoryCalloc(1, sizeof(SSvrMsg));
  m->msg = tmsg;
  m->type = Normal;

  STraceId* trace = (STraceId*)&msg->info.traceId;
  tGTrace("conn %p start to send resp (1/2)", exh->handle);
  transSendAsync(pThrd->asyncPool, &m->q);
  transReleaseExHandle(refMgt, refId);
  return;
_return1:
  tTrace("handle %p failed to send resp", exh);
  rpcFreeCont(msg->pCont);
  transReleaseExHandle(refMgt, refId);
  return;
_return2:
  tTrace("handle %p failed to send resp", exh);
  rpcFreeCont(msg->pCont);
  return;
}
void transRegisterMsg(const STransMsg* msg) {
  SExHandle* exh = msg->info.handle;
  int64_t    refId = msg->info.refId;
  ASYNC_CHECK_HANDLE(exh, refId);

  STransMsg tmsg = *msg;
  tmsg.info.refId = refId;

  SWorkThrdObj* pThrd = exh->pThrd;
  ASYNC_ERR_JRET(pThrd);

  SSvrMsg* m = taosMemoryCalloc(1, sizeof(SSvrMsg));
  m->msg = tmsg;
  m->type = Register;

  tTrace("conn %p start to register brokenlink callback", exh->handle);
  transSendAsync(pThrd->asyncPool, &m->q);
  transReleaseExHandle(refMgt, refId);
  return;

_return1:
  tTrace("handle %p failed to register brokenlink", exh);
  rpcFreeCont(msg->pCont);
  transReleaseExHandle(refMgt, refId);
  return;
_return2:
  tTrace("handle %p failed to register brokenlink", exh);
  rpcFreeCont(msg->pCont);
}

int transGetConnInfo(void* thandle, STransHandleInfo* pConnInfo) { return -1; }

#endif
