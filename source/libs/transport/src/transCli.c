/** Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

typedef struct SConnList {
  queue conn;
} SConnList;

typedef struct SCliConn {
  T_REF_DECLARE()
  uv_connect_t connReq;
  uv_stream_t* stream;
  queue        wreqQueue;
  uv_timer_t*  timer;

  void* hostThrd;

  SConnBuffer readBuf;
  STransQueue cliMsgs;

  queue      q;
  SConnList* list;

  STransCtx  ctx;
  bool       broken;  // link broken or not
  ConnStatus status;  //

  int64_t  refId;
  char*    ip;
  uint32_t port;

  SDelayTask* task;

  // debug and log info
  char src[32];
  char dst[32];

} SCliConn;

typedef struct SCliMsg {
  STransConnCtx* ctx;
  STransMsg      msg;
  queue          q;
  STransMsgType  type;

  int64_t  refId;
  uint64_t st;
  int      sent;  //(0: no send, 1: alread sent)
} SCliMsg;

typedef struct SCliThrd {
  TdThread      thread;  // tid
  int64_t       pid;     // pid
  uv_loop_t*    loop;
  SAsyncPool*   asyncPool;
  uv_prepare_t* prepare;
  void*         pool;  // conn pool

  SArray* timerList;

  // msg queue

  queue         msg;
  TdThreadMutex msgMtx;
  SDelayQueue*  delayQueue;
  SDelayQueue*  timeoutQueue;
  uint64_t      nextTimeout;  // next timeout
  void*         pTransInst;   //

  SCvtAddr cvtAddr;

  SCliMsg* stopMsg;

  bool quit;
} SCliThrd;

typedef struct SCliObj {
  char       label[TSDB_LABEL_LEN];
  int32_t    index;
  int        numOfThreads;
  SCliThrd** pThreadObj;
} SCliObj;

// conn pool
// add expire timeout and capacity limit
static void*     createConnPool(int size);
static void*     destroyConnPool(void* pool);
static SCliConn* getConnFromPool(void* pool, char* ip, uint32_t port);
static void      addConnToPool(void* pool, SCliConn* conn);
static void      doCloseIdleConn(void* param);

static int sockDebugInfo(struct sockaddr* sockname, char* dst) {
  struct sockaddr_in addr = *(struct sockaddr_in*)sockname;

  char buf[16] = {0};
  int  r = uv_ip4_name(&addr, (char*)buf, sizeof(buf));
  sprintf(dst, "%s:%d", buf, ntohs(addr.sin_port));
  return r;
}
// register timer for read
static void cliReadTimeoutCb(uv_timer_t* handle);
// register timer in each thread to clear expire conn
// static void cliTimeoutCb(uv_timer_t* handle);
// alloc buf for recv
static void cliAllocRecvBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
// callback after  read nbytes from socket
static void cliRecvCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf);
// callback after write data to socket
static void cliSendCb(uv_write_t* req, int status);
// callback after conn  to server
static void cliConnCb(uv_connect_t* req, int status);
static void cliAsyncCb(uv_async_t* handle);
static void cliIdleCb(uv_idle_t* handle);
static void cliPrepareCb(uv_prepare_t* handle);

static int32_t allocConnRef(SCliConn* conn, bool update);

static int cliAppCb(SCliConn* pConn, STransMsg* pResp, SCliMsg* pMsg);

static SCliConn* cliCreateConn(SCliThrd* thrd);
static void      cliDestroyConn(SCliConn* pConn, bool clear /*clear tcp handle or not*/);
static void      cliDestroy(uv_handle_t* handle);
static void      cliSend(SCliConn* pConn);

static bool cliIsEpsetUpdated(int32_t code, STransConnCtx* pCtx) {
  if (code != 0) return false;
  if (pCtx->retryCnt == 0) return false;
  if (transEpSetIsEqual(&pCtx->epSet, &pCtx->origEpSet)) return false;
  return true;
}

void cliMayCvtFqdnToIp(SEpSet* pEpSet, SCvtAddr* pCvtAddr);
/*
 * set TCP connection timeout per-socket level
 */
static int cliCreateSocket();
// process data read from server, add decompress etc later
static void cliHandleResp(SCliConn* conn);
// handle except about conn
static void cliHandleExcept(SCliConn* conn);

// handle req from app
static void cliHandleReq(SCliMsg* pMsg, SCliThrd* pThrd);
static void cliHandleQuit(SCliMsg* pMsg, SCliThrd* pThrd);
static void cliHandleRelease(SCliMsg* pMsg, SCliThrd* pThrd);
static void cliHandleUpdate(SCliMsg* pMsg, SCliThrd* pThrd);
static void (*cliAsyncHandle[])(SCliMsg* pMsg, SCliThrd* pThrd) = {cliHandleReq, cliHandleQuit, cliHandleRelease, NULL,
                                                                   cliHandleUpdate};

static void cliSendQuit(SCliThrd* thrd);
static void destroyUserdata(STransMsg* userdata);

static int cliRBChoseIdx(STrans* pTransInst);

static void destroyCmsg(void* cmsg);
static void transDestroyConnCtx(STransConnCtx* ctx);
// thread obj
static SCliThrd* createThrdObj();
static void      destroyThrdObj(SCliThrd* pThrd);

static void cliWalkCb(uv_handle_t* handle, void* arg);

static void cliReleaseUnfinishedMsg(SCliConn* conn) {
  SCliMsg* pMsg = NULL;
  for (int i = 0; i < transQueueSize(&conn->cliMsgs); i++) {
    pMsg = transQueueGet(&conn->cliMsgs, i);
    if (pMsg != NULL && pMsg->ctx != NULL) {
      if (conn->ctx.freeFunc != NULL) {
        conn->ctx.freeFunc(pMsg->ctx->ahandle);
      }
    }
    destroyCmsg(pMsg);
  }
}
#define CLI_RELEASE_UV(loop)        \
  do {                              \
    uv_walk(loop, cliWalkCb, NULL); \
    uv_run(loop, UV_RUN_DEFAULT);   \
    uv_loop_close(loop);            \
  } while (0);

// snprintf may cause performance problem
#define CONN_CONSTRUCT_HASH_KEY(key, ip, port)          \
  do {                                                  \
    snprintf(key, sizeof(key), "%s:%d", ip, (int)port); \
  } while (0)

#define CONN_HOST_THREAD_IDX1(idx, exh, refId, pThrd) \
  do {                                                \
    if (exh == NULL) {                                \
      idx = -1;                                       \
    } else {                                          \
      ASYNC_CHECK_HANDLE((exh), refId);               \
      pThrd = (SCliThrd*)(exh)->pThrd;                \
    }                                                 \
  } while (0)
#define CONN_PERSIST_TIME(para)    ((para) <= 90000 ? 90000 : (para))
#define CONN_GET_HOST_THREAD(conn) (conn ? ((SCliConn*)conn)->hostThrd : NULL)
#define CONN_GET_INST_LABEL(conn)  (((STrans*)(((SCliThrd*)(conn)->hostThrd)->pTransInst))->label)
#define CONN_SHOULD_RELEASE(conn, head)                                                                           \
  do {                                                                                                            \
    if ((head)->release == 1 && (head->msgLen) == sizeof(*head)) {                                                \
      uint64_t ahandle = head->ahandle;                                                                           \
      CONN_GET_MSGCTX_BY_AHANDLE(conn, ahandle);                                                                  \
      transClearBuffer(&conn->readBuf);                                                                           \
      transFreeMsg(transContFromHead((char*)head));                                                               \
      if (transQueueSize(&conn->cliMsgs) > 0 && ahandle == 0) {                                                   \
        SCliMsg* cliMsg = transQueueGet(&conn->cliMsgs, 0);                                                       \
        if (cliMsg->type == Release) return;                                                                      \
      }                                                                                                           \
      tDebug("%s conn %p receive release request, ref:%d", CONN_GET_INST_LABEL(conn), conn, T_REF_VAL_GET(conn)); \
      if (T_REF_VAL_GET(conn) > 1) {                                                                              \
        transUnrefCliHandle(conn);                                                                                \
      }                                                                                                           \
      destroyCmsg(pMsg);                                                                                          \
      cliReleaseUnfinishedMsg(conn);                                                                              \
      transQueueClear(&conn->cliMsgs);                                                                            \
      addConnToPool(((SCliThrd*)conn->hostThrd)->pool, conn);                                                     \
      return;                                                                                                     \
    }                                                                                                             \
  } while (0)

#define CONN_GET_MSGCTX_BY_AHANDLE(conn, ahandle)                                         \
  do {                                                                                    \
    int i = 0, sz = transQueueSize(&conn->cliMsgs);                                       \
    for (; i < sz; i++) {                                                                 \
      pMsg = transQueueGet(&conn->cliMsgs, i);                                            \
      if (pMsg != NULL && pMsg->ctx != NULL && (uint64_t)pMsg->ctx->ahandle == ahandle) { \
        break;                                                                            \
      }                                                                                   \
    }                                                                                     \
    if (i == sz) {                                                                        \
      pMsg = NULL;                                                                        \
    } else {                                                                              \
      pMsg = transQueueRm(&conn->cliMsgs, i);                                             \
    }                                                                                     \
  } while (0)
#define CONN_GET_NEXT_SENDMSG(conn)                 \
  do {                                              \
    int i = 0;                                      \
    do {                                            \
      pCliMsg = transQueueGet(&conn->cliMsgs, i++); \
      if (pCliMsg && 0 == pCliMsg->sent) {          \
        break;                                      \
      }                                             \
    } while (pCliMsg != NULL);                      \
    if (pCliMsg == NULL) {                          \
      goto _RETURN;                                 \
    }                                               \
  } while (0)

#define CONN_HANDLE_THREAD_QUIT(thrd) \
  do {                                \
    if (thrd->quit) {                 \
      return;                         \
    }                                 \
  } while (0)

#define CONN_HANDLE_BROKEN(conn) \
  do {                           \
    if (conn->broken) {          \
      cliHandleExcept(conn);     \
      return;                    \
    }                            \
  } while (0)

#define CONN_SET_PERSIST_BY_APP(conn) \
  do {                                \
    if (conn->status == ConnNormal) { \
      conn->status = ConnAcquire;     \
      transRefCliHandle(conn);        \
    }                                 \
  } while (0)

#define CONN_NO_PERSIST_BY_APP(conn) \
  (((conn)->status == ConnNormal || (conn)->status == ConnInPool) && T_REF_VAL_GET(conn) == 1)
#define CONN_RELEASE_BY_SERVER(conn) \
  (((conn)->status == ConnRelease || (conn)->status == ConnInPool) && T_REF_VAL_GET(conn) == 1)

#define REQUEST_NO_RESP(msg)         ((msg)->info.noResp == 1)
#define REQUEST_PERSIS_HANDLE(msg)   ((msg)->info.persistHandle == 1)
#define REQUEST_RELEASE_HANDLE(cmsg) ((cmsg)->type == Release)

#define EPSET_IS_VALID(epSet)       ((epSet) != NULL && (epSet)->numOfEps != 0)
#define EPSET_GET_SIZE(epSet)       (epSet)->numOfEps
#define EPSET_GET_INUSE_IP(epSet)   ((epSet)->eps[(epSet)->inUse].fqdn)
#define EPSET_GET_INUSE_PORT(epSet) ((epSet)->eps[(epSet)->inUse].port)
#define EPSET_FORWARD_INUSE(epSet)                                 \
  do {                                                             \
    if ((epSet)->numOfEps != 0) {                                  \
      (epSet)->inUse = (++((epSet)->inUse)) % ((epSet)->numOfEps); \
    }                                                              \
  } while (0)

#define EPSET_DEBUG_STR(epSet, tbuf)                                                                                   \
  do {                                                                                                                 \
    int len = snprintf(tbuf, sizeof(tbuf), "epset:{");                                                                 \
    for (int i = 0; i < (epSet)->numOfEps; i++) {                                                                      \
      if (i == (epSet)->numOfEps - 1) {                                                                                \
        len += snprintf(tbuf + len, sizeof(tbuf) - len, "%d. %s:%d", i, (epSet)->eps[i].fqdn, (epSet)->eps[i].port);   \
      } else {                                                                                                         \
        len += snprintf(tbuf + len, sizeof(tbuf) - len, "%d. %s:%d, ", i, (epSet)->eps[i].fqdn, (epSet)->eps[i].port); \
      }                                                                                                                \
    }                                                                                                                  \
    len += snprintf(tbuf + len, sizeof(tbuf) - len, "}, inUse:%d", (epSet)->inUse);                                    \
  } while (0);

static void* cliWorkThread(void* arg);

bool cliMaySendCachedMsg(SCliConn* conn) {
  if (!transQueueEmpty(&conn->cliMsgs)) {
    SCliMsg* pCliMsg = NULL;
    CONN_GET_NEXT_SENDMSG(conn);
    cliSend(conn);
  }
  return false;
_RETURN:
  return false;
}
void cliHandleResp(SCliConn* conn) {
  SCliThrd* pThrd = conn->hostThrd;
  STrans*   pTransInst = pThrd->pTransInst;

  if (conn->timer) {
    if (uv_is_active((uv_handle_t*)conn->timer)) {
      tDebug("%s conn %p stop timer", CONN_GET_INST_LABEL(conn), conn);
      uv_timer_stop(conn->timer);
    }
    conn->timer->data = NULL;
    taosArrayPush(pThrd->timerList, &conn->timer);
    conn->timer = NULL;
  }

  STransMsgHead* pHead = NULL;
  transDumpFromBuffer(&conn->readBuf, (char**)&pHead);
  pHead->code = htonl(pHead->code);
  pHead->msgLen = htonl(pHead->msgLen);

  STransMsg transMsg = {0};
  transMsg.contLen = transContLenFromMsg(pHead->msgLen);
  transMsg.pCont = transContFromHead((char*)pHead);
  transMsg.code = pHead->code;
  transMsg.msgType = pHead->msgType;
  transMsg.info.ahandle = NULL;
  transMsg.info.traceId = pHead->traceId;
  transMsg.info.hasEpSet = pHead->hasEpSet;

  SCliMsg*       pMsg = NULL;
  STransConnCtx* pCtx = NULL;
  CONN_SHOULD_RELEASE(conn, pHead);

  if (CONN_NO_PERSIST_BY_APP(conn)) {
    pMsg = transQueuePop(&conn->cliMsgs);

    pCtx = pMsg ? pMsg->ctx : NULL;
    transMsg.info.ahandle = pCtx ? pCtx->ahandle : NULL;
    tDebug("%s conn %p get ahandle %p, persist: 0", CONN_GET_INST_LABEL(conn), conn, transMsg.info.ahandle);
  } else {
    uint64_t ahandle = (uint64_t)pHead->ahandle;
    CONN_GET_MSGCTX_BY_AHANDLE(conn, ahandle);
    if (pMsg == NULL) {
      transMsg.info.ahandle = transCtxDumpVal(&conn->ctx, transMsg.msgType);
      tDebug("%s conn %p construct ahandle %p by %s, persist: 1", CONN_GET_INST_LABEL(conn), conn,
             transMsg.info.ahandle, TMSG_INFO(transMsg.msgType));
      if (!CONN_RELEASE_BY_SERVER(conn) && transMsg.info.ahandle == NULL) {
        transMsg.code = TSDB_CODE_RPC_BROKEN_LINK;
        transMsg.info.ahandle = transCtxDumpBrokenlinkVal(&conn->ctx, (int32_t*)&(transMsg.msgType));
        tDebug("%s conn %p construct ahandle %p due brokenlink, persist: 1", CONN_GET_INST_LABEL(conn), conn,
               transMsg.info.ahandle);
      }
    } else {
      pCtx = pMsg ? pMsg->ctx : NULL;
      transMsg.info.ahandle = pCtx ? pCtx->ahandle : NULL;
      tDebug("%s conn %p get ahandle %p, persist: 1", CONN_GET_INST_LABEL(conn), conn, transMsg.info.ahandle);
    }
  }
  // buf's mem alread translated to transMsg.pCont
  if (!CONN_NO_PERSIST_BY_APP(conn)) {
    transMsg.info.handle = (void*)conn->refId;
    tDebug("%s conn %p ref by app", CONN_GET_INST_LABEL(conn), conn);
  }

  STraceId* trace = &transMsg.info.traceId;

  tGTrace("%s conn %p %s received from %s, local info:%s, len:%d, code str:%s", CONN_GET_INST_LABEL(conn), conn,
          TMSG_INFO(pHead->msgType), conn->dst, conn->src, transMsg.contLen, tstrerror(transMsg.code));

  if (pCtx == NULL && CONN_NO_PERSIST_BY_APP(conn)) {
    tDebug("%s except, conn %p read while cli ignore it", CONN_GET_INST_LABEL(conn), conn);
    return;
  }
  if (CONN_RELEASE_BY_SERVER(conn) && transMsg.info.ahandle == NULL) {
    tDebug("%s except, conn %p read while cli ignore it", CONN_GET_INST_LABEL(conn), conn);
    return;
  }

  if (cliAppCb(conn, &transMsg, pMsg) != 0) {
    return;
  }
  destroyCmsg(pMsg);

  if (cliMaySendCachedMsg(conn) == true) {
    return;
  }

  if (CONN_NO_PERSIST_BY_APP(conn)) {
    addConnToPool(pThrd->pool, conn);
  }

  uv_read_start((uv_stream_t*)conn->stream, cliAllocRecvBufferCb, cliRecvCb);
}

void cliHandleExceptImpl(SCliConn* pConn, int32_t code) {
  if (transQueueEmpty(&pConn->cliMsgs)) {
    if (pConn->broken == true && CONN_NO_PERSIST_BY_APP(pConn)) {
      tTrace("%s conn %p handle except, persist:0", CONN_GET_INST_LABEL(pConn), pConn);
      transUnrefCliHandle(pConn);
      return;
    }
  }
  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pTransInst = pThrd->pTransInst;
  bool      once = false;
  do {
    SCliMsg* pMsg = transQueuePop(&pConn->cliMsgs);
    if (pMsg == NULL && once) {
      break;
    }
    STransConnCtx* pCtx = pMsg ? pMsg->ctx : NULL;

    STransMsg transMsg = {0};
    transMsg.code = code == -1 ? (pConn->broken ? TSDB_CODE_RPC_BROKEN_LINK : TSDB_CODE_RPC_NETWORK_UNAVAIL) : code;
    transMsg.msgType = pMsg ? pMsg->msg.msgType + 1 : 0;
    transMsg.info.ahandle = NULL;

    if (pMsg == NULL && !CONN_NO_PERSIST_BY_APP(pConn)) {
      transMsg.info.ahandle = transCtxDumpVal(&pConn->ctx, transMsg.msgType);
      tDebug("%s conn %p construct ahandle %p by %s", CONN_GET_INST_LABEL(pConn), pConn, transMsg.info.ahandle,
             TMSG_INFO(transMsg.msgType));
      if (transMsg.info.ahandle == NULL) {
        transMsg.info.ahandle = transCtxDumpBrokenlinkVal(&pConn->ctx, (int32_t*)&(transMsg.msgType));
        tDebug("%s conn %p construct ahandle %p due to brokenlink", CONN_GET_INST_LABEL(pConn), pConn,
               transMsg.info.ahandle);
      }
    } else {
      transMsg.info.ahandle = pCtx ? pCtx->ahandle : NULL;
    }

    if (pCtx == NULL || pCtx->pSem == NULL) {
      if (transMsg.info.ahandle == NULL) {
        once = true;
        continue;
      }
    }
    if (cliAppCb(pConn, &transMsg, pMsg) != 0) {
      return;
    }
    destroyCmsg(pMsg);
    tTrace("%s conn %p start to destroy, ref:%d", CONN_GET_INST_LABEL(pConn), pConn, T_REF_VAL_GET(pConn));
  } while (!transQueueEmpty(&pConn->cliMsgs));
  transUnrefCliHandle(pConn);
}
void cliHandleExcept(SCliConn* conn) {
  tTrace("%s conn %p except ref:%d", CONN_GET_INST_LABEL(conn), conn, T_REF_VAL_GET(conn));
  cliHandleExceptImpl(conn, -1);
}

void cliReadTimeoutCb(uv_timer_t* handle) {
  // set up timeout cb
  SCliConn* conn = handle->data;
  tTrace("%s conn %p timeout, ref:%d", CONN_GET_INST_LABEL(conn), conn, T_REF_VAL_GET(conn));
  uv_read_stop(conn->stream);
  cliHandleExceptImpl(conn, TSDB_CODE_RPC_TIMEOUT);
}

void* createConnPool(int size) {
  // thread local, no lock
  return taosHashInit(size, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
}
void* destroyConnPool(void* pool) {
  SConnList* connList = taosHashIterate((SHashObj*)pool, NULL);
  while (connList != NULL) {
    while (!QUEUE_IS_EMPTY(&connList->conn)) {
      queue*    h = QUEUE_HEAD(&connList->conn);
      SCliConn* c = QUEUE_DATA(h, SCliConn, q);
      cliDestroyConn(c, true);
    }
    connList = taosHashIterate((SHashObj*)pool, connList);
  }
  taosHashCleanup(pool);
  return NULL;
}

static SCliConn* getConnFromPool(void* pool, char* ip, uint32_t port) {
  char key[32] = {0};
  CONN_CONSTRUCT_HASH_KEY(key, ip, port);
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
  queue*    h = QUEUE_HEAD(&plist->conn);
  SCliConn* conn = QUEUE_DATA(h, SCliConn, q);
  conn->status = ConnNormal;
  QUEUE_REMOVE(&conn->q);
  QUEUE_INIT(&conn->q);

  transDQCancel(((SCliThrd*)conn->hostThrd)->timeoutQueue, conn->task);
  conn->task = NULL;

  return conn;
}
static void addConnToPool(void* pool, SCliConn* conn) {
  if (conn->status == ConnInPool) {
    return;
  }
  SCliThrd* thrd = conn->hostThrd;
  CONN_HANDLE_THREAD_QUIT(thrd);

  allocConnRef(conn, true);

  STrans* pTransInst = thrd->pTransInst;
  cliReleaseUnfinishedMsg(conn);
  transQueueClear(&conn->cliMsgs);
  transCtxCleanup(&conn->ctx);
  conn->status = ConnInPool;

  if (conn->list == NULL) {
    char key[32] = {0};
    CONN_CONSTRUCT_HASH_KEY(key, conn->ip, conn->port);
    tTrace("%s conn %p added to conn pool, read buf cap:%d", CONN_GET_INST_LABEL(conn), conn, conn->readBuf.cap);
    conn->list = taosHashGet((SHashObj*)pool, key, strlen(key));
  } else {
    tTrace("%s conn %p added to conn pool, read buf cap:%d", CONN_GET_INST_LABEL(conn), conn, conn->readBuf.cap);
  }
  assert(conn->list != NULL);
  QUEUE_INIT(&conn->q);
  QUEUE_PUSH(&conn->list->conn, &conn->q);

  assert(!QUEUE_IS_EMPTY(&conn->list->conn));

  STaskArg* arg = taosMemoryCalloc(1, sizeof(STaskArg));
  arg->param1 = conn;
  arg->param2 = thrd;
  conn->task = transDQSched(thrd->timeoutQueue, doCloseIdleConn, arg, CONN_PERSIST_TIME(pTransInst->idleTime));
}
static int32_t allocConnRef(SCliConn* conn, bool update) {
  if (update) {
    transRemoveExHandle(transGetRefMgt(), conn->refId);
    conn->refId = -1;
  }
  SExHandle* exh = taosMemoryCalloc(1, sizeof(SExHandle));
  exh->handle = conn;
  exh->pThrd = conn->hostThrd;
  exh->refId = transAddExHandle(transGetRefMgt(), exh);
  conn->refId = exh->refId;
  return 0;
}

static int32_t specifyConnRef(SCliConn* conn, bool update, int64_t handle) {
  if (update) {
    transRemoveExHandle(transGetRefMgt(), conn->refId);
    conn->refId = -1;
  }
  SExHandle* exh = transAcquireExHandle(transGetRefMgt(), handle);
  if (exh == NULL) {
    return -1;
  }
  exh->handle = conn;
  exh->pThrd = conn->hostThrd;
  conn->refId = exh->refId;

  transReleaseExHandle(transGetRefMgt(), handle);
  return 0;
}

static void cliAllocRecvBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  SCliConn*    conn = handle->data;
  SConnBuffer* pBuf = &conn->readBuf;
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
    while (transReadComplete(pBuf)) {
      tTrace("%s conn %p read complete", CONN_GET_INST_LABEL(conn), conn);
      cliHandleResp(conn);
    }
    return;
  }

  assert(nread <= 0);
  if (nread == 0) {
    // ref http://docs.libuv.org/en/v1.x/stream.html?highlight=uv_read_start#c.uv_read_cb
    // nread might be 0, which does not indicate an error or EOF. This is equivalent to EAGAIN or EWOULDBLOCK under
    // read(2).
    tTrace("%s conn %p read empty", CONN_GET_INST_LABEL(conn), conn);
    return;
  }
  if (nread < 0) {
    tWarn("%s conn %p read error:%s, ref:%d", CONN_GET_INST_LABEL(conn), conn, uv_err_name(nread), T_REF_VAL_GET(conn));
    conn->broken = true;
    cliHandleExcept(conn);
  }
}

static SCliConn* cliCreateConn(SCliThrd* pThrd) {
  SCliConn* conn = taosMemoryCalloc(1, sizeof(SCliConn));
  // read/write stream handle
  conn->stream = (uv_stream_t*)taosMemoryMalloc(sizeof(uv_tcp_t));
  uv_tcp_init(pThrd->loop, (uv_tcp_t*)(conn->stream));
  conn->stream->data = conn;

  conn->connReq.data = conn;

  transReqQueueInit(&conn->wreqQueue);

  transQueueInit(&conn->cliMsgs, NULL);

  transInitBuffer(&conn->readBuf);
  QUEUE_INIT(&conn->q);
  conn->hostThrd = pThrd;
  conn->status = ConnNormal;
  conn->broken = 0;
  transRefCliHandle(conn);

  allocConnRef(conn, false);

  return conn;
}
static void cliDestroyConn(SCliConn* conn, bool clear) {
  SCliThrd* pThrd = conn->hostThrd;
  tTrace("%s conn %p remove from conn pool", CONN_GET_INST_LABEL(conn), conn);
  QUEUE_REMOVE(&conn->q);
  QUEUE_INIT(&conn->q);
  transRemoveExHandle(transGetRefMgt(), conn->refId);
  conn->refId = -1;

  if (conn->task != NULL) {
    transDQCancel(pThrd->timeoutQueue, conn->task);
    conn->task = NULL;
  }
  if (conn->timer != NULL) {
    uv_timer_stop(conn->timer);
    taosArrayPush(pThrd->timerList, &conn->timer);
    conn->timer->data = NULL;
    conn->timer = NULL;
  }

  if (clear) {
    if (!uv_is_closing((uv_handle_t*)conn->stream)) {
      uv_read_stop(conn->stream);
      uv_close((uv_handle_t*)conn->stream, cliDestroy);
    }
  }
}
static void cliDestroy(uv_handle_t* handle) {
  if (uv_handle_get_type(handle) != UV_TCP || handle->data == NULL) {
    return;
  }
  SCliConn* conn = handle->data;
  SCliThrd* pThrd = conn->hostThrd;
  if (conn->timer != NULL) {
    uv_timer_stop(conn->timer);
    taosArrayPush(pThrd->timerList, &conn->timer);
    conn->timer->data = NULL;
    conn->timer = NULL;
  }

  transRemoveExHandle(transGetRefMgt(), conn->refId);
  taosMemoryFree(conn->ip);
  conn->stream->data = NULL;
  taosMemoryFree(conn->stream);
  transCtxCleanup(&conn->ctx);
  cliReleaseUnfinishedMsg(conn);
  transQueueDestroy(&conn->cliMsgs);
  tTrace("%s conn %p destroy successfully", CONN_GET_INST_LABEL(conn), conn);
  transReqQueueClear(&conn->wreqQueue);
  transDestroyBuffer(&conn->readBuf);
  taosMemoryFree(conn);
}
static bool cliHandleNoResp(SCliConn* conn) {
  bool res = false;
  if (!transQueueEmpty(&conn->cliMsgs)) {
    SCliMsg* pMsg = transQueueGet(&conn->cliMsgs, 0);
    if (REQUEST_NO_RESP(&pMsg->msg)) {
      transQueuePop(&conn->cliMsgs);
      destroyCmsg(pMsg);
      res = true;
    }
    if (res == true) {
      if (cliMaySendCachedMsg(conn) == false) {
        SCliThrd* thrd = conn->hostThrd;
        addConnToPool(thrd->pool, conn);
      }
    }
  }
  return res;
}
static void cliSendCb(uv_write_t* req, int status) {
  SCliConn* pConn = transReqQueueRemove(req);
  if (pConn == NULL) return;

  if (status == 0) {
    tTrace("%s conn %p data already was written out", CONN_GET_INST_LABEL(pConn), pConn);
  } else {
    tError("%s conn %p failed to write:%s", CONN_GET_INST_LABEL(pConn), pConn, uv_err_name(status));
    cliHandleExcept(pConn);
    return;
  }
  if (cliHandleNoResp(pConn) == true) {
    tTrace("%s conn %p no resp required", CONN_GET_INST_LABEL(pConn), pConn);
    return;
  }
  uv_read_start((uv_stream_t*)pConn->stream, cliAllocRecvBufferCb, cliRecvCb);
}

void cliSend(SCliConn* pConn) {
  CONN_HANDLE_BROKEN(pConn);

  assert(!transQueueEmpty(&pConn->cliMsgs));

  SCliMsg* pCliMsg = NULL;
  CONN_GET_NEXT_SENDMSG(pConn);
  pCliMsg->sent = 1;

  STransConnCtx* pCtx = pCliMsg->ctx;

  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pTransInst = pThrd->pTransInst;

  STransMsg* pMsg = (STransMsg*)(&pCliMsg->msg);
  if (pMsg->pCont == 0) {
    pMsg->pCont = (void*)rpcMallocCont(0);
    pMsg->contLen = 0;
  }
  int msgLen = transMsgLenFromCont(pMsg->contLen);

  STransMsgHead* pHead = transHeadFromCont(pMsg->pCont);
  pHead->ahandle = pCtx != NULL ? (uint64_t)pCtx->ahandle : 0;
  pHead->noResp = REQUEST_NO_RESP(pMsg) ? 1 : 0;
  pHead->persist = REQUEST_PERSIS_HANDLE(pMsg) ? 1 : 0;
  pHead->msgType = pMsg->msgType;
  pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
  pHead->release = REQUEST_RELEASE_HANDLE(pCliMsg) ? 1 : 0;
  memcpy(pHead->user, pTransInst->user, strlen(pTransInst->user));
  pHead->traceId = pMsg->info.traceId;

  uv_buf_t wb = uv_buf_init((char*)pHead, msgLen);

  STraceId* trace = &pMsg->info.traceId;
  tGTrace("%s conn %p %s is sent to %s, local info %s, len:%d", CONN_GET_INST_LABEL(pConn), pConn,
          TMSG_INFO(pHead->msgType), pConn->dst, pConn->src, pMsg->contLen);

  if (pHead->persist == 1) {
    CONN_SET_PERSIST_BY_APP(pConn);
  }

  if (pTransInst->startTimer != NULL && pTransInst->startTimer(0, pMsg->msgType)) {
    uv_timer_t* timer = taosArrayGetSize(pThrd->timerList) > 0 ? *(uv_timer_t**)taosArrayPop(pThrd->timerList) : NULL;
    if (timer == NULL) {
      tDebug("no avaiable timer, create");
      timer = taosMemoryCalloc(1, sizeof(uv_timer_t));
      uv_timer_init(pThrd->loop, timer);
    }
    timer->data = pConn;
    pConn->timer = timer;

    tGTrace("%s conn %p start timer for msg:%s", CONN_GET_INST_LABEL(pConn), pConn, TMSG_INFO(pMsg->msgType));
    uv_timer_start((uv_timer_t*)pConn->timer, cliReadTimeoutCb, TRANS_READ_TIMEOUT, 0);
  }
  uv_write_t* req = transReqQueuePush(&pConn->wreqQueue);
  uv_write(req, (uv_stream_t*)pConn->stream, &wb, 1, cliSendCb);
  return;
_RETURN:
  return;
}

void cliConnCb(uv_connect_t* req, int status) {
  // impl later
  SCliConn* pConn = req->data;
  if (status != 0) {
    tError("%s conn %p failed to connect server:%s", CONN_GET_INST_LABEL(pConn), pConn, uv_strerror(status));
    cliHandleExcept(pConn);
    return;
  }
  // int addrlen = sizeof(pConn->addr);
  struct sockaddr peername, sockname;

  int addrlen = sizeof(peername);
  uv_tcp_getpeername((uv_tcp_t*)pConn->stream, &peername, &addrlen);
  transGetSockDebugInfo(&peername, pConn->dst);

  addrlen = sizeof(sockname);
  uv_tcp_getsockname((uv_tcp_t*)pConn->stream, &sockname, &addrlen);
  transGetSockDebugInfo(&sockname, pConn->src);

  tTrace("%s conn %p connect to server successfully", CONN_GET_INST_LABEL(pConn), pConn);
  assert(pConn->stream == req->handle);

  cliSend(pConn);
}

static void cliHandleQuit(SCliMsg* pMsg, SCliThrd* pThrd) {
  if (!transAsyncPoolIsEmpty(pThrd->asyncPool)) {
    pThrd->stopMsg = pMsg;
    return;
  }
  pThrd->stopMsg = NULL;
  pThrd->quit = true;
  tDebug("cli work thread %p start to quit", pThrd);
  destroyCmsg(pMsg);
  destroyConnPool(pThrd->pool);
  uv_walk(pThrd->loop, cliWalkCb, NULL);
}
static void cliHandleRelease(SCliMsg* pMsg, SCliThrd* pThrd) {
  int64_t    refId = (int64_t)(pMsg->msg.info.handle);
  SExHandle* exh = transAcquireExHandle(transGetRefMgt(), refId);
  if (exh == NULL) {
    tDebug("%" PRId64 " already release", refId);
    destroyCmsg(pMsg);
    return;
  }

  SCliConn* conn = exh->handle;
  transReleaseExHandle(transGetRefMgt(), refId);

  tDebug("%s conn %p start to release to inst", CONN_GET_INST_LABEL(conn), conn);

  if (T_REF_VAL_GET(conn) == 2) {
    transUnrefCliHandle(conn);
    if (!transQueuePush(&conn->cliMsgs, pMsg)) {
      return;
    }
    cliSend(conn);
  }
}
static void cliHandleUpdate(SCliMsg* pMsg, SCliThrd* pThrd) {
  STransConnCtx* pCtx = pMsg->ctx;
  pThrd->cvtAddr = pCtx->cvtAddr;
  destroyCmsg(pMsg);
}

SCliConn* cliGetConn(SCliMsg* pMsg, SCliThrd* pThrd, bool* ignore) {
  STransConnCtx* pCtx = pMsg->ctx;
  SCliConn*      conn = NULL;

  int64_t refId = (int64_t)(pMsg->msg.info.handle);
  if (refId != 0) {
    SExHandle* exh = transAcquireExHandle(transGetRefMgt(), refId);
    if (exh == NULL) {
      *ignore = true;
      destroyCmsg(pMsg);
      return NULL;
    } else {
      conn = exh->handle;
      if (conn == NULL) {
        conn = getConnFromPool(pThrd->pool, EPSET_GET_INUSE_IP(&pCtx->epSet), EPSET_GET_INUSE_PORT(&pCtx->epSet));
        if (conn != NULL) specifyConnRef(conn, true, refId);
      }
      transReleaseExHandle(transGetRefMgt(), refId);
    }
    return conn;
  };

  conn = getConnFromPool(pThrd->pool, EPSET_GET_INUSE_IP(&pCtx->epSet), EPSET_GET_INUSE_PORT(&pCtx->epSet));
  if (conn != NULL) {
    tTrace("%s conn %p get from conn pool:%p", CONN_GET_INST_LABEL(conn), conn, pThrd->pool);
  } else {
    tTrace("%s not found conn in conn pool:%p", ((STrans*)pThrd->pTransInst)->label, pThrd->pool);
  }
  return conn;
}
void cliMayCvtFqdnToIp(SEpSet* pEpSet, SCvtAddr* pCvtAddr) {
  if (pCvtAddr->cvt == false) {
    return;
  }
  for (int i = 0; i < pEpSet->numOfEps && pEpSet->numOfEps == 1; i++) {
    if (strncmp(pEpSet->eps[i].fqdn, pCvtAddr->fqdn, TSDB_FQDN_LEN) == 0) {
      memset(pEpSet->eps[i].fqdn, 0, TSDB_FQDN_LEN);
      memcpy(pEpSet->eps[i].fqdn, pCvtAddr->ip, TSDB_FQDN_LEN);
    }
  }
}
void cliHandleReq(SCliMsg* pMsg, SCliThrd* pThrd) {
  STrans*        pTransInst = pThrd->pTransInst;
  STransConnCtx* pCtx = pMsg->ctx;

  cliMayCvtFqdnToIp(&pCtx->epSet, &pThrd->cvtAddr);
  if (!EPSET_IS_VALID(&pCtx->epSet)) {
    destroyCmsg(pMsg);
    tError("invalid epset");
    return;
  }

  bool      ignore = false;
  SCliConn* conn = cliGetConn(pMsg, pThrd, &ignore);
  if (ignore == true) {
    tError("ignore msg");
    return;
  }

  if (conn != NULL) {
    transCtxMerge(&conn->ctx, &pCtx->appCtx);
    transQueuePush(&conn->cliMsgs, pMsg);
    cliSend(conn);
  } else {
    conn = cliCreateConn(pThrd);

    int64_t refId = (int64_t)pMsg->msg.info.handle;
    if (refId != 0) specifyConnRef(conn, true, refId);

    transCtxMerge(&conn->ctx, &pCtx->appCtx);
    transQueuePush(&conn->cliMsgs, pMsg);

    conn->ip = strdup(EPSET_GET_INUSE_IP(&pCtx->epSet));
    conn->port = EPSET_GET_INUSE_PORT(&pCtx->epSet);

    int ret = transSetConnOption((uv_tcp_t*)conn->stream);
    if (ret) {
      tError("%s conn %p failed to set conn option, errmsg %s", transLabel(pTransInst), conn, uv_err_name(ret));
    }
    int32_t fd = taosCreateSocketWithTimeout(TRANS_CONN_TIMEOUT);
    if (fd == -1) {
      tTrace("%s conn %p failed to create socket", transLabel(pTransInst), conn);
      cliHandleExcept(conn);
      return;
    }
    uv_tcp_open((uv_tcp_t*)conn->stream, fd);

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = taosGetIpv4FromFqdn(conn->ip);
    addr.sin_port = (uint16_t)htons((uint16_t)conn->port);
    tTrace("%s conn %p try to connect to %s:%d", pTransInst->label, conn, conn->ip, conn->port);
    ret = uv_tcp_connect(&conn->connReq, (uv_tcp_t*)(conn->stream), (const struct sockaddr*)&addr, cliConnCb);
    if (ret != 0) {
      tTrace("%s conn %p failed to connect to %s:%d, reason:%s", pTransInst->label, conn, conn->ip, conn->port,
             uv_err_name(ret));
      cliHandleExcept(conn);
      return;
    }
  }
}
static void cliAsyncCb(uv_async_t* handle) {
  SAsyncItem* item = handle->data;
  SCliThrd*   pThrd = item->pThrd;
  SCliMsg*    pMsg = NULL;

  // batch process to avoid to lock/unlock frequently
  queue wq;
  taosThreadMutexLock(&item->mtx);
  QUEUE_MOVE(&item->qmsg, &wq);
  taosThreadMutexUnlock(&item->mtx);

  int count = 0;
  while (!QUEUE_IS_EMPTY(&wq)) {
    queue* h = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(h);

    SCliMsg* pMsg = QUEUE_DATA(h, SCliMsg, q);
    if (pMsg == NULL) {
      continue;
    }
    (*cliAsyncHandle[pMsg->type])(pMsg, pThrd);
    count++;
  }
  if (count >= 2) {
    tTrace("cli process batch size:%d", count);
  }
  // if (!uv_is_active((uv_handle_t*)pThrd->prepare)) uv_prepare_start(pThrd->prepare, cliPrepareCb);

  if (pThrd->stopMsg != NULL) cliHandleQuit(pThrd->stopMsg, pThrd);
}
static void cliPrepareCb(uv_prepare_t* handle) {
  SCliThrd* thrd = handle->data;
  tTrace("prepare work start");

  SAsyncPool* pool = thrd->asyncPool;
  for (int i = 0; i < pool->nAsync; i++) {
    uv_async_t* async = &(pool->asyncs[i]);
    SAsyncItem* item = async->data;

    queue wq;
    taosThreadMutexLock(&item->mtx);
    QUEUE_MOVE(&item->qmsg, &wq);
    taosThreadMutexUnlock(&item->mtx);

    int count = 0;
    while (!QUEUE_IS_EMPTY(&wq)) {
      queue* h = QUEUE_HEAD(&wq);
      QUEUE_REMOVE(h);

      SCliMsg* pMsg = QUEUE_DATA(h, SCliMsg, q);
      if (pMsg == NULL) {
        continue;
      }
      (*cliAsyncHandle[pMsg->type])(pMsg, thrd);
      count++;
    }
  }
  tTrace("prepare work end");
  if (thrd->stopMsg != NULL) cliHandleQuit(thrd->stopMsg, thrd);
}

static void* cliWorkThread(void* arg) {
  SCliThrd* pThrd = (SCliThrd*)arg;
  pThrd->pid = taosGetSelfPthreadId();
  setThreadName("trans-cli-work");
  uv_run(pThrd->loop, UV_RUN_DEFAULT);
  return NULL;
}

void* transInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle) {
  SCliObj* cli = taosMemoryCalloc(1, sizeof(SCliObj));

  STrans* pTransInst = shandle;
  memcpy(cli->label, label, strlen(label));
  cli->numOfThreads = numOfThreads;
  cli->pThreadObj = (SCliThrd**)taosMemoryCalloc(cli->numOfThreads, sizeof(SCliThrd*));

  for (int i = 0; i < cli->numOfThreads; i++) {
    SCliThrd* pThrd = createThrdObj();
    pThrd->nextTimeout = taosGetTimestampMs() + CONN_PERSIST_TIME(pTransInst->idleTime);
    pThrd->pTransInst = shandle;

    int err = taosThreadCreate(&pThrd->thread, NULL, cliWorkThread, (void*)(pThrd));
    if (err == 0) {
      tDebug("success to create tranport-cli thread:%d", i);
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
static void destroyCmsg(void* arg) {
  SCliMsg* pMsg = arg;
  if (pMsg == NULL) {
    return;
  }
  transDestroyConnCtx(pMsg->ctx);
  destroyUserdata(&pMsg->msg);
  taosMemoryFree(pMsg);
}

static SCliThrd* createThrdObj() {
  SCliThrd* pThrd = (SCliThrd*)taosMemoryCalloc(1, sizeof(SCliThrd));

  QUEUE_INIT(&pThrd->msg);
  taosThreadMutexInit(&pThrd->msgMtx, NULL);

  pThrd->loop = (uv_loop_t*)taosMemoryMalloc(sizeof(uv_loop_t));
  uv_loop_init(pThrd->loop);

  pThrd->asyncPool = transAsyncPoolCreate(pThrd->loop, 5, pThrd, cliAsyncCb);

  pThrd->prepare = taosMemoryCalloc(1, sizeof(uv_prepare_t));
  uv_prepare_init(pThrd->loop, pThrd->prepare);
  pThrd->prepare->data = pThrd;
  // uv_prepare_start(pThrd->prepare, cliPrepareCb);

  int32_t timerSize = 512;
  pThrd->timerList = taosArrayInit(timerSize, sizeof(void*));
  for (int i = 0; i < timerSize; i++) {
    uv_timer_t* timer = taosMemoryCalloc(1, sizeof(uv_timer_t));
    uv_timer_init(pThrd->loop, timer);
    taosArrayPush(pThrd->timerList, &timer);
  }

  pThrd->pool = createConnPool(4);
  transDQCreate(pThrd->loop, &pThrd->delayQueue);

  transDQCreate(pThrd->loop, &pThrd->timeoutQueue);

  pThrd->quit = false;
  return pThrd;
}
static void destroyThrdObj(SCliThrd* pThrd) {
  if (pThrd == NULL) {
    return;
  }

  taosThreadJoin(pThrd->thread, NULL);
  CLI_RELEASE_UV(pThrd->loop);
  taosThreadMutexDestroy(&pThrd->msgMtx);
  TRANS_DESTROY_ASYNC_POOL_MSG(pThrd->asyncPool, SCliMsg, destroyCmsg);
  transAsyncPoolDestroy(pThrd->asyncPool);

  transDQDestroy(pThrd->delayQueue, destroyCmsg);
  transDQDestroy(pThrd->timeoutQueue, NULL);

  for (int i = 0; i < taosArrayGetSize(pThrd->timerList); i++) {
    uv_timer_t* timer = taosArrayGetP(pThrd->timerList, i);
    taosMemoryFree(timer);
  }
  taosArrayDestroy(pThrd->timerList);
  taosMemoryFree(pThrd->prepare);
  taosMemoryFree(pThrd->loop);
  taosMemoryFree(pThrd);
}

static void transDestroyConnCtx(STransConnCtx* ctx) {
  //
  taosMemoryFree(ctx);
}

void cliSendQuit(SCliThrd* thrd) {
  // cli can stop gracefully
  SCliMsg* msg = taosMemoryCalloc(1, sizeof(SCliMsg));
  msg->type = Quit;
  transAsyncSend(thrd->asyncPool, &msg->q);
  atomic_store_8(&thrd->asyncPool->stop, 1);
}
void cliWalkCb(uv_handle_t* handle, void* arg) {
  if (!uv_is_closing(handle)) {
    uv_read_stop((uv_stream_t*)handle);
    uv_close(handle, cliDestroy);
  }
}

int cliRBChoseIdx(STrans* pTransInst) {
  int8_t index = pTransInst->index;
  if (pTransInst->numOfThreads == 0) {
    return -1;
  }
  if (pTransInst->index++ >= pTransInst->numOfThreads) {
    pTransInst->index = 0;
  }
  return index % pTransInst->numOfThreads;
}
static void doDelayTask(void* param) {
  STaskArg* arg = param;
  SCliMsg*  pMsg = arg->param1;
  SCliThrd* pThrd = arg->param2;
  taosMemoryFree(arg);

  cliHandleReq(pMsg, pThrd);
}

static void doCloseIdleConn(void* param) {
  STaskArg* arg = param;
  SCliConn* conn = arg->param1;
  SCliThrd* pThrd = arg->param2;
  tTrace("%s conn %p idle, close it", CONN_GET_INST_LABEL(conn), conn);
  conn->task = NULL;
  cliDestroyConn(conn, true);
  taosMemoryFree(arg);
}

static void cliSchedMsgToNextNode(SCliMsg* pMsg, SCliThrd* pThrd) {
  STransConnCtx* pCtx = pMsg->ctx;

  STraceId* trace = &pMsg->msg.info.traceId;
  char      tbuf[256] = {0};
  EPSET_DEBUG_STR(&pCtx->epSet, tbuf);
  tGDebug("%s retry on next node, use %s, retryCnt:%d, limit:%d", transLabel(pThrd->pTransInst), tbuf,
          pCtx->retryCnt + 1, pCtx->retryLimit);

  STaskArg* arg = taosMemoryMalloc(sizeof(STaskArg));
  arg->param1 = pMsg;
  arg->param2 = pThrd;
  transDQSched(pThrd->delayQueue, doDelayTask, arg, TRANS_RETRY_INTERVAL);
}

void cliCompareAndSwap(int8_t* val, int8_t exp, int8_t newVal) {
  if (*val != exp) {
    *val = newVal;
  }
}

bool cliTryExtractEpSet(STransMsg* pResp, SEpSet* dst) {
  if ((pResp == NULL || pResp->info.hasEpSet == 0)) {
    return false;
  }
  // rebuild resp msg
  SEpSet epset;
  if (tDeserializeSEpSet(pResp->pCont, pResp->contLen, &epset) < 0) {
    return false;
  }
  int32_t tlen = tSerializeSEpSet(NULL, 0, dst);

  char*   buf = NULL;
  int32_t len = pResp->contLen - tlen;
  if (len != 0) {
    buf = rpcMallocCont(len);
    memcpy(buf, (char*)pResp->pCont + tlen, len);
  }
  rpcFreeCont(pResp->pCont);

  pResp->pCont = buf;
  pResp->contLen = len;

  *dst = epset;
  return true;
}
int cliAppCb(SCliConn* pConn, STransMsg* pResp, SCliMsg* pMsg) {
  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pTransInst = pThrd->pTransInst;

  if (pMsg == NULL || pMsg->ctx == NULL) {
    tTrace("%s conn %p handle resp", pTransInst->label, pConn);
    pTransInst->cfp(pTransInst->parent, pResp, NULL);
    return 0;
  }
  /*
   *  no retry
   *  1. query conn
   *  2. rpc thread already receive quit msg
   */
  STransConnCtx* pCtx = pMsg->ctx;
  int32_t        code = pResp->code;

  bool retry = (pTransInst->retry != NULL && pTransInst->retry(code, pResp->msgType - 1)) ? true : false;
  if (retry) {
    pMsg->sent = 0;
    pCtx->retryCnt += 1;
    if (code == TSDB_CODE_RPC_NETWORK_UNAVAIL || code == TSDB_CODE_RPC_BROKEN_LINK) {
      cliCompareAndSwap(&pCtx->retryLimit, TRANS_RETRY_COUNT_LIMIT, EPSET_GET_SIZE(&pCtx->epSet) * 3);
      if (pCtx->retryCnt < pCtx->retryLimit) {
        transUnrefCliHandle(pConn);
        EPSET_FORWARD_INUSE(&pCtx->epSet);
        transFreeMsg(pResp->pCont);
        cliSchedMsgToNextNode(pMsg, pThrd);
        return -1;
      }
    } else {
      cliCompareAndSwap(&pCtx->retryLimit, TRANS_RETRY_COUNT_LIMIT, TRANS_RETRY_COUNT_LIMIT);
      if (pCtx->retryCnt < pCtx->retryLimit) {
        if (pResp->contLen == 0) {
          EPSET_FORWARD_INUSE(&pCtx->epSet);
        } else {
          if (tDeserializeSEpSet(pResp->pCont, pResp->contLen, &pCtx->epSet) < 0) {
            tError("%s conn %p failed to deserialize epset", CONN_GET_INST_LABEL(pConn));
          }
        }
        addConnToPool(pThrd->pool, pConn);
        transFreeMsg(pResp->pCont);
        cliSchedMsgToNextNode(pMsg, pThrd);
        return -1;
      }
    }
  }

  STraceId* trace = &pResp->info.traceId;

  bool hasEpSet = cliTryExtractEpSet(pResp, &pCtx->epSet);
  if (hasEpSet) {
    char tbuf[256] = {0};
    EPSET_DEBUG_STR(&pCtx->epSet, tbuf);
    tGDebug("%s conn %p extract epset from msg", CONN_GET_INST_LABEL(pConn), pConn);
  }

  if (pCtx->pSem != NULL) {
    tGDebug("%s conn %p(sync) handle resp", CONN_GET_INST_LABEL(pConn), pConn);
    if (pCtx->pRsp == NULL) {
      tGTrace("%s conn %p(sync) failed to resp, ignore", CONN_GET_INST_LABEL(pConn), pConn);
    } else {
      memcpy((char*)pCtx->pRsp, (char*)pResp, sizeof(*pResp));
    }
    tsem_post(pCtx->pSem);
    pCtx->pRsp = NULL;
  } else {
    tGDebug("%s conn %p handle resp", CONN_GET_INST_LABEL(pConn), pConn);
    if (retry == false && hasEpSet == true) {
      pTransInst->cfp(pTransInst->parent, pResp, &pCtx->epSet);
    } else {
      if (!cliIsEpsetUpdated(code, pCtx)) {
        pTransInst->cfp(pTransInst->parent, pResp, NULL);
      } else {
        pTransInst->cfp(pTransInst->parent, pResp, &pCtx->epSet);
      }
    }
  }
  return 0;
}

void transCloseClient(void* arg) {
  SCliObj* cli = arg;
  for (int i = 0; i < cli->numOfThreads; i++) {
    cliSendQuit(cli->pThreadObj[i]);
    destroyThrdObj(cli->pThreadObj[i]);
  }
  taosMemoryFree(cli->pThreadObj);
  taosMemoryFree(cli);
}
void transRefCliHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  int ref = T_REF_INC((SCliConn*)handle);
  tTrace("%s conn %p ref %d", CONN_GET_INST_LABEL((SCliConn*)handle), handle, ref);
  UNUSED(ref);
}
void transUnrefCliHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  int ref = T_REF_DEC((SCliConn*)handle);
  tTrace("%s conn %p ref:%d", CONN_GET_INST_LABEL((SCliConn*)handle), handle, ref);
  if (ref == 0) {
    cliDestroyConn((SCliConn*)handle, true);
  }
}
SCliThrd* transGetWorkThrdFromHandle(int64_t handle, bool* validHandle) {
  SCliThrd*  pThrd = NULL;
  SExHandle* exh = transAcquireExHandle(transGetRefMgt(), handle);
  if (exh == NULL) {
    return NULL;
  }

  *validHandle = true;
  pThrd = exh->pThrd;
  transReleaseExHandle(transGetRefMgt(), handle);
  return pThrd;
}
SCliThrd* transGetWorkThrd(STrans* trans, int64_t handle, bool* validHandle) {
  if (handle == 0) {
    int idx = cliRBChoseIdx(trans);
    if (idx < 0) return NULL;
    return ((SCliObj*)trans->tcphandle)->pThreadObj[idx];
  }
  SCliThrd* pThrd = transGetWorkThrdFromHandle(handle, validHandle);
  if (*validHandle == true && pThrd == NULL) {
    int idx = cliRBChoseIdx(trans);
    if (idx < 0) return NULL;
    pThrd = ((SCliObj*)trans->tcphandle)->pThreadObj[idx];
  }
  return pThrd;
}
int transReleaseCliHandle(void* handle) {
  int  idx = -1;
  bool valid = false;

  SCliThrd* pThrd = transGetWorkThrdFromHandle((int64_t)handle, &valid);
  if (pThrd == NULL) {
    return -1;
  }

  STransMsg tmsg = {.info.handle = handle};
  TRACE_SET_MSGID(&tmsg.info.traceId, tGenIdPI64());

  SCliMsg* cmsg = taosMemoryCalloc(1, sizeof(SCliMsg));
  cmsg->msg = tmsg;
  cmsg->type = Release;

  STraceId* trace = &tmsg.info.traceId;
  tGDebug("send release request at thread:%08" PRId64 "", pThrd->pid);

  if (0 != transAsyncSend(pThrd->asyncPool, &cmsg->q)) {
    taosMemoryFree(cmsg);
    return -1;
  }
  return 0;
}

int transSendRequest(void* shandle, const SEpSet* pEpSet, STransMsg* pReq, STransCtx* ctx) {
  STrans* pTransInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)shandle);
  if (pTransInst == NULL) {
    transFreeMsg(pReq->pCont);
    return -1;
  }

  bool      valid = false;
  SCliThrd* pThrd = transGetWorkThrd(pTransInst, (int64_t)pReq->info.handle, &valid);
  if (pThrd == NULL && valid == false) {
    transFreeMsg(pReq->pCont);
    transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
    return -1;
  }

  TRACE_SET_MSGID(&pReq->info.traceId, tGenIdPI64());

  STransConnCtx* pCtx = taosMemoryCalloc(1, sizeof(STransConnCtx));
  pCtx->epSet = *pEpSet;
  pCtx->ahandle = pReq->info.ahandle;
  pCtx->msgType = pReq->msgType;

  if (ctx != NULL) {
    pCtx->appCtx = *ctx;
  }
  assert(pTransInst->connType == TAOS_CONN_CLIENT);

  SCliMsg* cliMsg = taosMemoryCalloc(1, sizeof(SCliMsg));
  cliMsg->ctx = pCtx;
  cliMsg->msg = *pReq;
  cliMsg->st = taosGetTimestampUs();
  cliMsg->type = Normal;
  cliMsg->refId = (int64_t)shandle;

  STraceId* trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pTransInst), pThrd->pid,
          EPSET_GET_INUSE_IP(&pCtx->epSet), EPSET_GET_INUSE_PORT(&pCtx->epSet), pReq->info.ahandle);
  if (0 != transAsyncSend(pThrd->asyncPool, &(cliMsg->q))) {
    destroyCmsg(cliMsg);
    transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
    return -1;
  }
  transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  return 0;
}

int transSendRecv(void* shandle, const SEpSet* pEpSet, STransMsg* pReq, STransMsg* pRsp) {
  STrans* pTransInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)shandle);
  if (pTransInst == NULL) {
    transFreeMsg(pReq->pCont);
    return -1;
  }

  bool      valid = false;
  SCliThrd* pThrd = transGetWorkThrd(pTransInst, (int64_t)pReq->info.handle, &valid);
  if (pThrd == NULL && valid == false) {
    transFreeMsg(pReq->pCont);
    transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
    return -1;
  }

  tsem_t* sem = taosMemoryCalloc(1, sizeof(tsem_t));
  tsem_init(sem, 0, 0);

  TRACE_SET_MSGID(&pReq->info.traceId, tGenIdPI64());

  STransConnCtx* pCtx = taosMemoryCalloc(1, sizeof(STransConnCtx));
  pCtx->epSet = *pEpSet;
  pCtx->origEpSet = *pEpSet;
  pCtx->ahandle = pReq->info.ahandle;
  pCtx->msgType = pReq->msgType;
  pCtx->pSem = sem;
  pCtx->pRsp = pRsp;

  SCliMsg* cliMsg = taosMemoryCalloc(1, sizeof(SCliMsg));
  cliMsg->ctx = pCtx;
  cliMsg->msg = *pReq;
  cliMsg->st = taosGetTimestampUs();
  cliMsg->type = Normal;
  cliMsg->refId = (int64_t)shandle;

  STraceId* trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pTransInst), pThrd->pid,
          EPSET_GET_INUSE_IP(&pCtx->epSet), EPSET_GET_INUSE_PORT(&pCtx->epSet), pReq->info.ahandle);

  int ret = transAsyncSend(pThrd->asyncPool, &cliMsg->q);
  if (ret != 0) {
    destroyCmsg(cliMsg);
    goto _RETURN;
  }
  tsem_wait(sem);

_RETURN:
  tsem_destroy(sem);
  taosMemoryFree(sem);
  transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  return ret;
}
/*
 *
 **/
int transSetDefaultAddr(void* shandle, const char* ip, const char* fqdn) {
  STrans* pTransInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)shandle);
  if (pTransInst == NULL) {
    return -1;
  }

  SCvtAddr cvtAddr = {0};
  if (ip != NULL && fqdn != NULL) {
    memcpy(cvtAddr.ip, ip, strlen(ip));
    memcpy(cvtAddr.fqdn, fqdn, strlen(fqdn));
    cvtAddr.cvt = true;
  }
  for (int i = 0; i < pTransInst->numOfThreads; i++) {
    STransConnCtx* pCtx = taosMemoryCalloc(1, sizeof(STransConnCtx));
    pCtx->cvtAddr = cvtAddr;

    SCliMsg* cliMsg = taosMemoryCalloc(1, sizeof(SCliMsg));
    cliMsg->ctx = pCtx;
    cliMsg->type = Update;
    cliMsg->refId = (int64_t)shandle;

    SCliThrd* thrd = ((SCliObj*)pTransInst->tcphandle)->pThreadObj[i];
    tDebug("%s update epset at thread:%08" PRId64, pTransInst->label, thrd->pid);

    if (transAsyncSend(thrd->asyncPool, &(cliMsg->q)) != 0) {
      destroyCmsg(cliMsg);
      transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
      return -1;
    }
  }
  transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  return 0;
}

int64_t transAllocHandle() {
  SExHandle* exh = taosMemoryCalloc(1, sizeof(SExHandle));
  exh->refId = transAddExHandle(transGetRefMgt(), exh);
  tDebug("pre alloc refId %" PRId64 "", exh->refId);
  return exh->refId;
}
#endif
