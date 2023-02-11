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
  queue   conns;
  int32_t size;
} SConnList;

typedef struct SCliConn {
  T_REF_DECLARE()
  uv_connect_t connReq;
  uv_stream_t* stream;
  queue        wreqQueue;

  uv_timer_t* timer;  // read timer, forbidden

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
  // timer handles
  SArray* timerList;
  // msg queue
  queue         msg;
  TdThreadMutex msgMtx;
  SDelayQueue*  delayQueue;
  SDelayQueue*  timeoutQueue;
  uint64_t      nextTimeout;  // next timeout
  void*         pTransInst;   //

  void (*destroyAhandleFp)(void* ahandle);
  SHashObj* fqdn2ipCache;
  SCvtAddr  cvtAddr;

  SHashObj* failFastCache;

  SCliMsg* stopMsg;

  bool quit;
} SCliThrd;

typedef struct SCliObj {
  char       label[TSDB_LABEL_LEN];
  int32_t    index;
  int        numOfThreads;
  SCliThrd** pThreadObj;
} SCliObj;

typedef struct {
  int32_t reinit;
  int64_t timestamp;
  int32_t count;
  int32_t threshold;
  int64_t interval;
} SFailFastItem;
// conn pool
// add expire timeout and capacity limit
static void*     createConnPool(int size);
static void*     destroyConnPool(void* pool);
static SCliConn* getConnFromPool(void* pool, char* ip, uint32_t port);
static void      addConnToPool(void* pool, SCliConn* conn);
static void      doCloseIdleConn(void* param);

// register conn timer
static void cliConnTimeout(uv_timer_t* handle);
// register timer for read
static void cliReadTimeoutCb(uv_timer_t* handle);
// register timer in each thread to clear expire conn
// static void cliTimeoutCb(uv_timer_t* handle);
// alloc buffer for recv
static FORCE_INLINE void cliAllocRecvBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
// callback after recv nbytes from socket
static void cliRecvCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf);
// callback after send data to socket
static void cliSendCb(uv_write_t* req, int status);
// callback after conn to server
static void cliConnCb(uv_connect_t* req, int status);
static void cliAsyncCb(uv_async_t* handle);
static void cliIdleCb(uv_idle_t* handle);
static void cliPrepareCb(uv_prepare_t* handle);

static bool cliRecvReleaseReq(SCliConn* conn, STransMsgHead* pHead);

static int32_t allocConnRef(SCliConn* conn, bool update);

static int cliAppCb(SCliConn* pConn, STransMsg* pResp, SCliMsg* pMsg);

static SCliConn* cliCreateConn(SCliThrd* thrd);
static void      cliDestroyConn(SCliConn* pConn, bool clear /*clear tcp handle or not*/);
static void      cliDestroy(uv_handle_t* handle);
static void      cliSend(SCliConn* pConn);
static void      cliDestroyConnMsgs(SCliConn* conn, bool destroy);

// cli util func
static FORCE_INLINE bool cliIsEpsetUpdated(int32_t code, STransConnCtx* pCtx);
static FORCE_INLINE void cliMayCvtFqdnToIp(SEpSet* pEpSet, SCvtAddr* pCvtAddr);

static FORCE_INLINE int32_t cliBuildExceptResp(SCliMsg* pMsg, STransMsg* resp);

static FORCE_INLINE uint32_t cliGetIpFromFqdnCache(SHashObj* cache, char* fqdn);
static FORCE_INLINE void     cliUpdateFqdnCache(SHashObj* cache, char* fqdn);

// process data read from server, add decompress etc later
static void cliHandleResp(SCliConn* conn);
// handle except about conn
static void cliHandleExcept(SCliConn* conn);
static void cliReleaseUnfinishedMsg(SCliConn* conn);

// handle req from app
static void cliHandleReq(SCliMsg* pMsg, SCliThrd* pThrd);
static void cliHandleQuit(SCliMsg* pMsg, SCliThrd* pThrd);
static void cliHandleRelease(SCliMsg* pMsg, SCliThrd* pThrd);
static void cliHandleUpdate(SCliMsg* pMsg, SCliThrd* pThrd);
static void (*cliAsyncHandle[])(SCliMsg* pMsg, SCliThrd* pThrd) = {cliHandleReq, cliHandleQuit, cliHandleRelease, NULL,
                                                                   cliHandleUpdate};

static FORCE_INLINE void destroyUserdata(STransMsg* userdata);
static FORCE_INLINE void destroyCmsg(void* cmsg);
static FORCE_INLINE void destroyCmsgAndAhandle(void* cmsg);
static FORCE_INLINE int  cliRBChoseIdx(STrans* pTransInst);
static FORCE_INLINE void transDestroyConnCtx(STransConnCtx* ctx);

// thread obj
static SCliThrd* createThrdObj(void* trans);
static void      destroyThrdObj(SCliThrd* pThrd);

static void cliWalkCb(uv_handle_t* handle, void* arg);

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

#define CONN_PERSIST_TIME(para)   ((para) <= 90000 ? 90000 : (para))
#define CONN_GET_INST_LABEL(conn) (((STrans*)(((SCliThrd*)(conn)->hostThrd)->pTransInst))->label)

#define CONN_GET_MSGCTX_BY_AHANDLE(conn, ahandle)                         \
  do {                                                                    \
    int i = 0, sz = transQueueSize(&conn->cliMsgs);                       \
    for (; i < sz; i++) {                                                 \
      pMsg = transQueueGet(&conn->cliMsgs, i);                            \
      if (pMsg->ctx != NULL && (uint64_t)pMsg->ctx->ahandle == ahandle) { \
        break;                                                            \
      }                                                                   \
    }                                                                     \
    if (i == sz) {                                                        \
      pMsg = NULL;                                                        \
      tDebug("msg not found, %" PRIu64 "", ahandle);                      \
    } else {                                                              \
      pMsg = transQueueRm(&conn->cliMsgs, i);                             \
      tDebug("msg found, %" PRIu64 "", ahandle);                          \
    }                                                                     \
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

#define EPSET_IS_VALID(epSet)       ((epSet) != NULL && (epSet)->numOfEps >= 0 && (epSet)->inUse >= 0)
#define EPSET_GET_SIZE(epSet)       (epSet)->numOfEps
#define EPSET_GET_INUSE_IP(epSet)   ((epSet)->eps[(epSet)->inUse].fqdn)
#define EPSET_GET_INUSE_PORT(epSet) ((epSet)->eps[(epSet)->inUse].port)
#define EPSET_FORWARD_INUSE(epSet)                             \
  do {                                                         \
    if ((epSet)->numOfEps != 0) {                              \
      ++((epSet)->inUse);                                      \
      (epSet)->inUse = ((epSet)->inUse) % ((epSet)->numOfEps); \
    }                                                          \
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

static void cliReleaseUnfinishedMsg(SCliConn* conn) {
  SCliThrd* pThrd = conn->hostThrd;

  for (int i = 0; i < transQueueSize(&conn->cliMsgs); i++) {
    SCliMsg* msg = transQueueGet(&conn->cliMsgs, i);
    if (msg != NULL && msg->ctx != NULL && msg->ctx->ahandle != (void*)0x9527) {
      if (conn->ctx.freeFunc != NULL && msg->ctx->ahandle != NULL) {
        conn->ctx.freeFunc(msg->ctx->ahandle);
      } else if (msg->ctx->ahandle != NULL && pThrd->destroyAhandleFp != NULL) {
        tDebug("%s conn %p destroy unfinished ahandle %p", CONN_GET_INST_LABEL(conn), conn, msg->ctx->ahandle);
        pThrd->destroyAhandleFp(msg->ctx->ahandle);
      }
    }
    destroyCmsg(msg);
  }
  memset(&conn->ctx, 0, sizeof(conn->ctx));
}
bool cliMaySendCachedMsg(SCliConn* conn) {
  if (!transQueueEmpty(&conn->cliMsgs)) {
    SCliMsg* pCliMsg = NULL;
    CONN_GET_NEXT_SENDMSG(conn);
    if (pCliMsg == NULL)
      return false;
    else {
      cliSend(conn);
      return true;
    }
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
    taosArrayPush(pThrd->timerList, &conn->timer);
    conn->timer->data = NULL;
    conn->timer = NULL;
  }

  STransMsgHead* pHead = NULL;

  int32_t msgLen = transDumpFromBuffer(&conn->readBuf, (char**)&pHead);
  if (msgLen <= 0) {
    tDebug("%s conn %p recv invalid packet ", CONN_GET_INST_LABEL(conn), conn);
    return;
  }

  if (transDecompressMsg((char**)&pHead, msgLen) < 0) {
    tDebug("%s conn %p recv invalid packet, failed to decompress", CONN_GET_INST_LABEL(conn), conn);
  }
  pHead->code = htonl(pHead->code);
  pHead->msgLen = htonl(pHead->msgLen);
  if (cliRecvReleaseReq(conn, pHead)) {
    return;
  }

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
      pCtx = pMsg->ctx;
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
  tGDebug("%s conn %p %s received from %s, local info:%s, len:%d, code str:%s", CONN_GET_INST_LABEL(conn), conn,
          TMSG_INFO(pHead->msgType), conn->dst, conn->src, pHead->msgLen, tstrerror(transMsg.code));

  if (pCtx == NULL && CONN_NO_PERSIST_BY_APP(conn)) {
    tDebug("%s except, conn %p read while cli ignore it", CONN_GET_INST_LABEL(conn), conn);
    transFreeMsg(transMsg.pCont);
    return;
  }
  if (CONN_RELEASE_BY_SERVER(conn) && transMsg.info.ahandle == NULL) {
    tDebug("%s except, conn %p read while cli ignore it", CONN_GET_INST_LABEL(conn), conn);
    transFreeMsg(transMsg.pCont);
    return;
  }

  if (pMsg == NULL || (pMsg && pMsg->type != Release)) {
    if (cliAppCb(conn, &transMsg, pMsg) != 0) {
      return;
    }
  }
  destroyCmsg(pMsg);

  if (cliMaySendCachedMsg(conn) == true) {
    return;
  }

  if (CONN_NO_PERSIST_BY_APP(conn)) {
    return addConnToPool(pThrd->pool, conn);
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

    if (pMsg != NULL && REQUEST_NO_RESP(&pMsg->msg)) {
      destroyCmsg(pMsg);
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
        int32_t msgType = 0;
        transMsg.info.ahandle = transCtxDumpBrokenlinkVal(&pConn->ctx, &msgType);
        transMsg.msgType = msgType;
        tDebug("%s conn %p construct ahandle %p due to brokenlink", CONN_GET_INST_LABEL(pConn), pConn,
               transMsg.info.ahandle);
      }
    } else {
      transMsg.info.ahandle = (pMsg != NULL && pMsg->type != Release && pCtx) ? pCtx->ahandle : NULL;
    }

    if (pCtx == NULL || pCtx->pSem == NULL) {
      if (transMsg.info.ahandle == NULL) {
        if (pMsg == NULL || REQUEST_NO_RESP(&pMsg->msg) || pMsg->type == Release) {
          destroyCmsg(pMsg);
          once = true;
          continue;
        }
      }
    }

    if (pMsg == NULL || (pMsg && pMsg->type != Release)) {
      if (cliAppCb(pConn, &transMsg, pMsg) != 0) {
        return;
      }
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

void cliConnTimeout(uv_timer_t* handle) {
  SCliConn* conn = handle->data;
  SCliThrd* pThrd = conn->hostThrd;

  tTrace("%s conn %p conn timeout, ref:%d", CONN_GET_INST_LABEL(conn), conn, T_REF_VAL_GET(conn));

  uv_timer_stop(handle);
  handle->data = NULL;
  taosArrayPush(pThrd->timerList, &conn->timer);

  conn->timer = NULL;
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
    while (!QUEUE_IS_EMPTY(&connList->conns)) {
      queue*    h = QUEUE_HEAD(&connList->conns);
      SCliConn* c = QUEUE_DATA(h, SCliConn, q);
      cliDestroyConn(c, true);
    }
    connList = taosHashIterate((SHashObj*)pool, connList);
  }
  taosHashCleanup(pool);
  return NULL;
}

static SCliConn* getConnFromPool(void* pool, char* ip, uint32_t port) {
  char key[TSDB_FQDN_LEN + 64] = {0};
  CONN_CONSTRUCT_HASH_KEY(key, ip, port);

  SConnList* plist = taosHashGet((SHashObj*)pool, key, strlen(key));
  if (plist == NULL) {
    SConnList list = {0};
    taosHashPut((SHashObj*)pool, key, strlen(key), (void*)&list, sizeof(list));
    plist = taosHashGet((SHashObj*)pool, key, strlen(key));
    if (plist == NULL) return NULL;
    QUEUE_INIT(&plist->conns);
  }

  if (QUEUE_IS_EMPTY(&plist->conns)) {
    return NULL;
  }

  plist->size -= 1;
  queue*    h = QUEUE_HEAD(&plist->conns);
  SCliConn* conn = QUEUE_DATA(h, SCliConn, q);
  conn->status = ConnNormal;
  QUEUE_REMOVE(&conn->q);
  QUEUE_INIT(&conn->q);

  if (conn->task != NULL) {
    transDQCancel(((SCliThrd*)conn->hostThrd)->timeoutQueue, conn->task);
    conn->task = NULL;
  }
  return conn;
}
static void addConnToPool(void* pool, SCliConn* conn) {
  if (conn->status == ConnInPool) {
    return;
  }
  allocConnRef(conn, true);

  SCliThrd* thrd = conn->hostThrd;
  if (conn->timer != NULL) {
    uv_timer_stop(conn->timer);
    taosArrayPush(thrd->timerList, &conn->timer);
    conn->timer->data = NULL;
    conn->timer = NULL;
  }
  if (T_REF_VAL_GET(conn) > 1) {
    transUnrefCliHandle(conn);
  }

  cliDestroyConnMsgs(conn, false);

  conn->status = ConnInPool;

  if (conn->list == NULL) {
    char key[TSDB_FQDN_LEN + 64] = {0};
    CONN_CONSTRUCT_HASH_KEY(key, conn->ip, conn->port);
    tTrace("%s conn %p added to conn pool, read buf cap:%d", CONN_GET_INST_LABEL(conn), conn, conn->readBuf.cap);
    conn->list = taosHashGet((SHashObj*)pool, key, strlen(key));
  } else {
    tTrace("%s conn %p added to conn pool, read buf cap:%d", CONN_GET_INST_LABEL(conn), conn, conn->readBuf.cap);
  }
  QUEUE_PUSH(&conn->list->conns, &conn->q);
  conn->list->size += 1;

  if (conn->list->size >= 50) {
    STaskArg* arg = taosMemoryCalloc(1, sizeof(STaskArg));
    arg->param1 = conn;
    arg->param2 = thrd;

    STrans* pTransInst = thrd->pTransInst;
    conn->task = transDQSched(thrd->timeoutQueue, doCloseIdleConn, arg, CONN_PERSIST_TIME(pTransInst->idleTime));
  }
}
static int32_t allocConnRef(SCliConn* conn, bool update) {
  if (update) {
    transReleaseExHandle(transGetRefMgt(), conn->refId);
    transRemoveExHandle(transGetRefMgt(), conn->refId);
    conn->refId = -1;
  }
  SExHandle* exh = taosMemoryCalloc(1, sizeof(SExHandle));
  exh->handle = conn;
  exh->pThrd = conn->hostThrd;
  exh->refId = transAddExHandle(transGetRefMgt(), exh);
  conn->refId = exh->refId;

  if (conn->refId == -1) {
    taosMemoryFree(exh);
  }
  return 0;
}

static int32_t specifyConnRef(SCliConn* conn, bool update, int64_t handle) {
  if (update) {
    transReleaseExHandle(transGetRefMgt(), conn->refId);
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
  tDebug("%s conn %p alloc read buf", CONN_GET_INST_LABEL(conn), conn);
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
      tDebug("%s conn %p read complete", CONN_GET_INST_LABEL(conn), conn);
      if (pBuf->invalid) {
        cliHandleExcept(conn);
        break;
      } else {
        cliHandleResp(conn);
      }
    }
    return;
  }

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
  // transSetConnOption((uv_tcp_t*)conn->stream);

  uv_timer_t* timer = taosArrayGetSize(pThrd->timerList) > 0 ? *(uv_timer_t**)taosArrayPop(pThrd->timerList) : NULL;
  if (timer == NULL) {
    timer = taosMemoryCalloc(1, sizeof(uv_timer_t));
    tDebug("no available timer, create a timer %p", timer);
    uv_timer_init(pThrd->loop, timer);
  }
  timer->data = conn;
  conn->timer = timer;

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
  transReleaseExHandle(transGetRefMgt(), conn->refId);
  transRemoveExHandle(transGetRefMgt(), conn->refId);
  conn->refId = -1;

  if (conn->task != NULL) {
    transDQCancel(pThrd->timeoutQueue, conn->task);
    conn->task = NULL;
  }
  if (conn->timer != NULL) {
    uv_timer_stop(conn->timer);
    conn->timer->data = NULL;
    taosArrayPush(pThrd->timerList, &conn->timer);
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

  transReleaseExHandle(transGetRefMgt(), conn->refId);
  transRemoveExHandle(transGetRefMgt(), conn->refId);
  taosMemoryFree(conn->ip);
  taosMemoryFree(conn->stream);

  cliDestroyConnMsgs(conn, true);

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
        res = false;
      } else {
        res = true;
      }
    }
  }
  return res;
}
static void cliSendCb(uv_write_t* req, int status) {
  SCliConn* pConn = transReqQueueRemove(req);
  if (pConn == NULL) return;

  SCliMsg* pMsg = !transQueueEmpty(&pConn->cliMsgs) ? transQueueGet(&pConn->cliMsgs, 0) : NULL;
  if (pMsg != NULL) {
    int64_t cost = taosGetTimestampUs() - pMsg->st;
    if (cost > 1000 * 20) {
      tWarn("%s conn %p send cost:%dus, send exception", CONN_GET_INST_LABEL(pConn), pConn, (int)cost);
    }
  }

  if (status == 0) {
    tTrace("%s conn %p data already was written out", CONN_GET_INST_LABEL(pConn), pConn);
  } else {
    if (!uv_is_closing((uv_handle_t*)&pConn->stream)) {
      tError("%s conn %p failed to write:%s", CONN_GET_INST_LABEL(pConn), pConn, uv_err_name(status));
      cliHandleExcept(pConn);
    }
    return;
  }
  if (cliHandleNoResp(pConn) == true) {
    tTrace("%s conn %p no resp required", CONN_GET_INST_LABEL(pConn), pConn);
    return;
  }
  uv_read_start((uv_stream_t*)pConn->stream, cliAllocRecvBufferCb, cliRecvCb);
}

void cliSend(SCliConn* pConn) {
  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pTransInst = pThrd->pTransInst;

  if (transQueueEmpty(&pConn->cliMsgs)) {
    tError("%s conn %p not msg to send", pTransInst->label, pConn);
    cliHandleExcept(pConn);
    return;
  }

  SCliMsg* pCliMsg = NULL;
  CONN_GET_NEXT_SENDMSG(pConn);
  pCliMsg->sent = 1;

  STransConnCtx* pCtx = pCliMsg->ctx;

  STransMsg* pMsg = (STransMsg*)(&pCliMsg->msg);
  if (pMsg->pCont == 0) {
    pMsg->pCont = (void*)rpcMallocCont(0);
    pMsg->contLen = 0;
  }

  int            msgLen = transMsgLenFromCont(pMsg->contLen);
  STransMsgHead* pHead = transHeadFromCont(pMsg->pCont);

  if (pHead->comp == 0) {
    pHead->ahandle = pCtx != NULL ? (uint64_t)pCtx->ahandle : 0;
    pHead->noResp = REQUEST_NO_RESP(pMsg) ? 1 : 0;
    pHead->persist = REQUEST_PERSIS_HANDLE(pMsg) ? 1 : 0;
    pHead->msgType = pMsg->msgType;
    pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
    pHead->release = REQUEST_RELEASE_HANDLE(pCliMsg) ? 1 : 0;
    memcpy(pHead->user, pTransInst->user, strlen(pTransInst->user));
    pHead->traceId = pMsg->info.traceId;
    pHead->magicNum = htonl(TRANS_MAGIC_NUM);
  }
  pHead->timestamp = taosHton64(taosGetTimestampUs());

  if (pHead->persist == 1) {
    CONN_SET_PERSIST_BY_APP(pConn);
  }

  STraceId* trace = &pMsg->info.traceId;

  if (pTransInst->startTimer != NULL && pTransInst->startTimer(0, pMsg->msgType)) {
    uv_timer_t* timer = taosArrayGetSize(pThrd->timerList) > 0 ? *(uv_timer_t**)taosArrayPop(pThrd->timerList) : NULL;
    if (timer == NULL) {
      timer = taosMemoryCalloc(1, sizeof(uv_timer_t));
      tDebug("no available timer, create a timer %p", timer);
      uv_timer_init(pThrd->loop, timer);
    }
    timer->data = pConn;
    pConn->timer = timer;

    tGTrace("%s conn %p start timer for msg:%s", CONN_GET_INST_LABEL(pConn), pConn, TMSG_INFO(pMsg->msgType));
    uv_timer_start((uv_timer_t*)pConn->timer, cliReadTimeoutCb, TRANS_READ_TIMEOUT, 0);
  }

  if (pHead->comp == 0) {
    if (pTransInst->compressSize != -1 && pTransInst->compressSize < pMsg->contLen) {
      msgLen = transCompressMsg(pMsg->pCont, pMsg->contLen) + sizeof(STransMsgHead);
      pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
    }
  } else {
    msgLen = (int32_t)ntohl((uint32_t)(pHead->msgLen));
  }

  tGDebug("%s conn %p %s is sent to %s, local info %s, len:%d", CONN_GET_INST_LABEL(pConn), pConn,
          TMSG_INFO(pHead->msgType), pConn->dst, pConn->src, msgLen);

  uv_buf_t    wb = uv_buf_init((char*)pHead, msgLen);
  uv_write_t* req = transReqQueuePush(&pConn->wreqQueue);

  int status = uv_write(req, (uv_stream_t*)pConn->stream, &wb, 1, cliSendCb);
  if (status != 0) {
    tGError("%s conn %p failed to send msg:%s, errmsg:%s", CONN_GET_INST_LABEL(pConn), pConn, TMSG_INFO(pMsg->msgType),
            uv_err_name(status));
    cliHandleExcept(pConn);
  }
  return;
_RETURN:
  return;
}

void cliConnCb(uv_connect_t* req, int status) {
  SCliConn* pConn = req->data;
  SCliThrd* pThrd = pConn->hostThrd;

  if (pConn->timer != NULL) {
    uv_timer_stop(pConn->timer);
    pConn->timer->data = NULL;
    taosArrayPush(pThrd->timerList, &pConn->timer);
    pConn->timer = NULL;
  }

  if (status != 0) {
    SCliMsg* pMsg = transQueueGet(&pConn->cliMsgs, 0);
    STrans*  pTransInst = pThrd->pTransInst;

    tError("%s msg %s failed to send, conn %p failed to connect to %s:%d, reason: %s", CONN_GET_INST_LABEL(pConn),
           pMsg ? TMSG_INFO(pMsg->msg.msgType) : 0, pConn, pConn->ip, pConn->port, uv_strerror(status));
    if (pMsg != NULL && REQUEST_NO_RESP(&pMsg->msg) &&
        (pTransInst->failFastFp != NULL && pTransInst->failFastFp(pMsg->msg.msgType))) {
      char*    ip = pConn->ip;
      uint32_t port = pConn->port;
      char     key[TSDB_FQDN_LEN + 64] = {0};
      CONN_CONSTRUCT_HASH_KEY(key, ip, port);

      SFailFastItem* item = taosHashGet(pThrd->failFastCache, key, strlen(key));
      int64_t        cTimestamp = taosGetTimestampMs();
      if (item != NULL) {
        int32_t elapse = cTimestamp - item->timestamp;
        if (elapse >= 0 && elapse <= pTransInst->failFastInterval) {
          item->count++;
        } else {
          item->count = 1;
          item->timestamp = cTimestamp;
        }
      } else {
        SFailFastItem item = {.count = 1, .timestamp = cTimestamp};
        taosHashPut(pThrd->failFastCache, key, strlen(key), &item, sizeof(SFailFastItem));
      }
    }
    cliHandleExcept(pConn);
    return;
  }
  struct sockaddr peername, sockname;

  int addrlen = sizeof(peername);
  uv_tcp_getpeername((uv_tcp_t*)pConn->stream, &peername, &addrlen);
  transSockInfo2Str(&peername, pConn->dst);

  addrlen = sizeof(sockname);
  uv_tcp_getsockname((uv_tcp_t*)pConn->stream, &sockname, &addrlen);
  transSockInfo2Str(&sockname, pConn->src);

  tTrace("%s conn %p connect to server successfully", CONN_GET_INST_LABEL(pConn), pConn);

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
    tDebug("%" PRId64 " already released", refId);
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
  } else {
    tError("%s conn %p already released", CONN_GET_INST_LABEL(conn), conn);
    destroyCmsg(pMsg);
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
      tError("failed to get conn, refId: %" PRId64 "", refId);
      *ignore = true;
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
FORCE_INLINE void cliMayCvtFqdnToIp(SEpSet* pEpSet, SCvtAddr* pCvtAddr) {
  if (pCvtAddr->cvt == false) {
    return;
  }
  if (pEpSet->numOfEps == 1 && strncmp(pEpSet->eps[0].fqdn, pCvtAddr->fqdn, TSDB_FQDN_LEN) == 0) {
    memset(pEpSet->eps[0].fqdn, 0, TSDB_FQDN_LEN);
    memcpy(pEpSet->eps[0].fqdn, pCvtAddr->ip, TSDB_FQDN_LEN);
  }
}

FORCE_INLINE bool cliIsEpsetUpdated(int32_t code, STransConnCtx* pCtx) {
  if (code != 0) return false;
  // if (pCtx->retryCnt == 0) return false;
  if (transEpSetIsEqual(&pCtx->epSet, &pCtx->origEpSet)) return false;
  return true;
}
FORCE_INLINE int32_t cliBuildExceptResp(SCliMsg* pMsg, STransMsg* pResp) {
  if (pMsg == NULL) return -1;

  memset(pResp, 0, sizeof(STransMsg));

  pResp->code = TSDB_CODE_RPC_BROKEN_LINK;
  pResp->msgType = pMsg->msg.msgType + 1;
  pResp->info.ahandle = pMsg->ctx ? pMsg->ctx->ahandle : NULL;
  pResp->info.traceId = pMsg->msg.info.traceId;

  return 0;
}
static FORCE_INLINE uint32_t cliGetIpFromFqdnCache(SHashObj* cache, char* fqdn) {
  uint32_t  addr = 0;
  uint32_t* v = taosHashGet(cache, fqdn, strlen(fqdn));
  if (v == NULL) {
    addr = taosGetIpv4FromFqdn(fqdn);
    if (addr == 0xffffffff) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      tError("failed to get ip from fqdn:%s since %s", fqdn, terrstr());
      return addr;
    }

    taosHashPut(cache, fqdn, strlen(fqdn), &addr, sizeof(addr));
  } else {
    addr = *v;
  }
  return addr;
}
static FORCE_INLINE void cliUpdateFqdnCache(SHashObj* cache, char* fqdn) {
  // impl later
  return;
}

void cliHandleReq(SCliMsg* pMsg, SCliThrd* pThrd) {
  STrans*        pTransInst = pThrd->pTransInst;
  STransConnCtx* pCtx = pMsg->ctx;

  cliMayCvtFqdnToIp(&pCtx->epSet, &pThrd->cvtAddr);
  STraceId* trace = &pMsg->msg.info.traceId;

  if (!EPSET_IS_VALID(&pCtx->epSet)) {
    tGError("%s, msg %s sent with invalid epset", pTransInst->label, TMSG_INFO(pMsg->msg.msgType));
    destroyCmsg(pMsg);
    return;
  }

  if (REQUEST_NO_RESP(&pMsg->msg) && (pTransInst->failFastFp != NULL && pTransInst->failFastFp(pMsg->msg.msgType))) {
    char*    ip = EPSET_GET_INUSE_IP(&pCtx->epSet);
    uint32_t port = EPSET_GET_INUSE_PORT(&pCtx->epSet);
    char     key[TSDB_FQDN_LEN + 64] = {0};
    CONN_CONSTRUCT_HASH_KEY(key, ip, port);

    SFailFastItem* item = taosHashGet(pThrd->failFastCache, key, strlen(key));
    if (item != NULL) {
      int32_t elapse = (int32_t)(taosGetTimestampMs() - item->timestamp);
      if (item->count >= pTransInst->failFastThreshold && (elapse >= 0 && elapse <= pTransInst->failFastInterval)) {
        STraceId* trace = &(pMsg->msg.info.traceId);
        tGTrace("%s, msg %s cancel to send, reason: failed to connect %s:%d: count: %d, at %d", pTransInst->label,
                TMSG_INFO(pMsg->msg.msgType), ip, port, item->count, elapse);
        destroyCmsg(pMsg);
        return;
      }
    }
  }

  bool      ignore = false;
  SCliConn* conn = cliGetConn(pMsg, pThrd, &ignore);
  if (ignore == true) {
    // persist conn already release by server
    STransMsg resp;
    cliBuildExceptResp(pMsg, &resp);
    if (pMsg->type != Release) {
      pTransInst->cfp(pTransInst->parent, &resp, NULL);
    }
    destroyCmsg(pMsg);
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

    uint32_t ipaddr = cliGetIpFromFqdnCache(pThrd->fqdn2ipCache, conn->ip);
    if (ipaddr == 0xffffffff) {
      uv_timer_stop(conn->timer);
      conn->timer->data = NULL;
      taosArrayPush(pThrd->timerList, &conn->timer);
      conn->timer = NULL;

      cliHandleExcept(conn);
      return;
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ipaddr;
    addr.sin_port = (uint16_t)htons((uint16_t)conn->port);

    tGTrace("%s conn %p try to connect to %s:%d", pTransInst->label, conn, conn->ip, conn->port);
    int32_t fd = taosCreateSocketWithTimeout(TRANS_CONN_TIMEOUT * 4);
    if (fd == -1) {
      tGError("%s conn %p failed to create socket, reason:%s", transLabel(pTransInst), conn,
              tstrerror(TAOS_SYSTEM_ERROR(errno)));
      cliHandleExcept(conn);
      errno = 0;
      return;
    }
    int ret = uv_tcp_open((uv_tcp_t*)conn->stream, fd);
    if (ret != 0) {
      tGError("%s conn %p failed to set stream, reason:%s", transLabel(pTransInst), conn, uv_err_name(ret));
      cliHandleExcept(conn);
      return;
    }
    ret = transSetConnOption((uv_tcp_t*)conn->stream);
    if (ret != 0) {
      tGError("%s conn %p failed to set socket opt, reason:%s", transLabel(pTransInst), conn, uv_err_name(ret));
      cliHandleExcept(conn);
      return;
    }

    ret = uv_tcp_connect(&conn->connReq, (uv_tcp_t*)(conn->stream), (const struct sockaddr*)&addr, cliConnCb);
    if (ret != 0) {
      tGError("%s conn %p failed to connect to %s:%d, reason:%s", pTransInst->label, conn, conn->ip, conn->port,
              uv_err_name(ret));

      uv_timer_stop(conn->timer);
      conn->timer->data = NULL;
      taosArrayPush(pThrd->timerList, &conn->timer);
      conn->timer = NULL;

      cliHandleExcept(conn);
      return;
    }
    uv_timer_start(conn->timer, cliConnTimeout, TRANS_CONN_TIMEOUT, 0);
  }
  tGTrace("%s conn %p ready", pTransInst->label, conn);
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
      (*cliAsyncHandle[pMsg->type])(pMsg, thrd);
      count++;
    }
  }
  tTrace("prepare work end");
  if (thrd->stopMsg != NULL) cliHandleQuit(thrd->stopMsg, thrd);
}

void cliDestroyConnMsgs(SCliConn* conn, bool destroy) {
  transCtxCleanup(&conn->ctx);
  cliReleaseUnfinishedMsg(conn);
  if (destroy == 1) {
    transQueueDestroy(&conn->cliMsgs);
  } else {
    transQueueClear(&conn->cliMsgs);
  }
}

void cliIteraConnMsgs(SCliConn* conn) {
  SCliThrd* pThrd = conn->hostThrd;
  STrans*   pTransInst = pThrd->pTransInst;

  for (int i = 0; i < transQueueSize(&conn->cliMsgs); i++) {
    SCliMsg* cmsg = transQueueGet(&conn->cliMsgs, i);
    if (cmsg->type == Release || REQUEST_NO_RESP(&cmsg->msg) || cmsg->msg.msgType == TDMT_SCH_DROP_TASK) {
      continue;
    }

    STransMsg resp = {0};
    if (-1 == cliBuildExceptResp(cmsg, &resp)) {
      continue;
    }
    pTransInst->cfp(pTransInst->parent, &resp, NULL);

    cmsg->ctx->ahandle = NULL;
  }
}
bool cliRecvReleaseReq(SCliConn* conn, STransMsgHead* pHead) {
  if (pHead->release == 1 && (pHead->msgLen) == sizeof(*pHead)) {
    uint64_t ahandle = pHead->ahandle;
    tDebug("ahandle = %" PRIu64 "", ahandle);
    SCliMsg* pMsg = NULL;
    CONN_GET_MSGCTX_BY_AHANDLE(conn, ahandle);

    transClearBuffer(&conn->readBuf);
    transFreeMsg(transContFromHead((char*)pHead));

    for (int i = 0; ahandle == 0 && i < transQueueSize(&conn->cliMsgs); i++) {
      SCliMsg* cliMsg = transQueueGet(&conn->cliMsgs, i);
      if (cliMsg->type == Release) {
        ASSERTS(pMsg == NULL, "trans-cli recv invaid release-req");
        return true;
      }
    }

    cliIteraConnMsgs(conn);

    tDebug("%s conn %p receive release request, refId:%" PRId64 "", CONN_GET_INST_LABEL(conn), conn, conn->refId);
    destroyCmsg(pMsg);

    addConnToPool(((SCliThrd*)conn->hostThrd)->pool, conn);
    return true;
  }
  return false;
}

static void* cliWorkThread(void* arg) {
  SCliThrd* pThrd = (SCliThrd*)arg;
  pThrd->pid = taosGetSelfPthreadId();
  setThreadName("trans-cli-work");
  uv_run(pThrd->loop, UV_RUN_DEFAULT);

  tDebug("thread quit-thread:%08" PRId64, pThrd->pid);
  return NULL;
}

void* transInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle) {
  SCliObj* cli = taosMemoryCalloc(1, sizeof(SCliObj));

  STrans* pTransInst = shandle;
  memcpy(cli->label, label, TSDB_LABEL_LEN);
  cli->numOfThreads = numOfThreads;
  cli->pThreadObj = (SCliThrd**)taosMemoryCalloc(cli->numOfThreads, sizeof(SCliThrd*));

  for (int i = 0; i < cli->numOfThreads; i++) {
    SCliThrd* pThrd = createThrdObj(shandle);
    if (pThrd == NULL) {
      return NULL;
    }

    int err = taosThreadCreate(&pThrd->thread, NULL, cliWorkThread, (void*)(pThrd));
    if (err == 0) {
      tDebug("success to create tranport-cli thread:%d", i);
    }
    cli->pThreadObj[i] = pThrd;
  }

  return cli;
}

static FORCE_INLINE void destroyUserdata(STransMsg* userdata) {
  if (userdata->pCont == NULL) {
    return;
  }
  transFreeMsg(userdata->pCont);
  userdata->pCont = NULL;
}

static FORCE_INLINE void destroyCmsg(void* arg) {
  SCliMsg* pMsg = arg;
  if (pMsg == NULL) {
    return;
  }

  transDestroyConnCtx(pMsg->ctx);
  destroyUserdata(&pMsg->msg);
  taosMemoryFree(pMsg);
}

static FORCE_INLINE void destroyCmsgAndAhandle(void* param) {
  if (param == NULL) return;

  STaskArg* arg = param;
  SCliMsg*  pMsg = arg->param1;
  SCliThrd* pThrd = arg->param2;

  tDebug("destroy Ahandle A");
  if (pThrd != NULL && pThrd->destroyAhandleFp != NULL) {
    tDebug("destroy Ahandle B");
    pThrd->destroyAhandleFp(pMsg->ctx->ahandle);
  }
  tDebug("destroy Ahandle C");

  transDestroyConnCtx(pMsg->ctx);
  destroyUserdata(&pMsg->msg);
  taosMemoryFree(pMsg);
}

static SCliThrd* createThrdObj(void* trans) {
  STrans* pTransInst = trans;

  SCliThrd* pThrd = (SCliThrd*)taosMemoryCalloc(1, sizeof(SCliThrd));

  QUEUE_INIT(&pThrd->msg);
  taosThreadMutexInit(&pThrd->msgMtx, NULL);

  pThrd->loop = (uv_loop_t*)taosMemoryMalloc(sizeof(uv_loop_t));
  int err = uv_loop_init(pThrd->loop);
  if (err != 0) {
    tError("failed to init uv_loop, reason:%s", uv_err_name(err));
    taosMemoryFree(pThrd->loop);
    taosThreadMutexDestroy(&pThrd->msgMtx);
    taosMemoryFree(pThrd);
    return NULL;
  }
  pThrd->asyncPool = transAsyncPoolCreate(pThrd->loop, 8, pThrd, cliAsyncCb);
  if (pThrd->asyncPool == NULL) {
    tError("failed to init async pool");
    uv_loop_close(pThrd->loop);
    taosMemoryFree(pThrd->loop);
    taosThreadMutexDestroy(&pThrd->msgMtx);
    taosMemoryFree(pThrd);
    return NULL;
  }

  pThrd->prepare = taosMemoryCalloc(1, sizeof(uv_prepare_t));
  uv_prepare_init(pThrd->loop, pThrd->prepare);
  pThrd->prepare->data = pThrd;
  // uv_prepare_start(pThrd->prepare, cliPrepareCb);

  int32_t timerSize = 64;
  pThrd->timerList = taosArrayInit(timerSize, sizeof(void*));
  for (int i = 0; i < timerSize; i++) {
    uv_timer_t* timer = taosMemoryCalloc(1, sizeof(uv_timer_t));
    uv_timer_init(pThrd->loop, timer);
    taosArrayPush(pThrd->timerList, &timer);
  }

  pThrd->pool = createConnPool(4);
  transDQCreate(pThrd->loop, &pThrd->delayQueue);

  transDQCreate(pThrd->loop, &pThrd->timeoutQueue);

  pThrd->nextTimeout = taosGetTimestampMs() + CONN_PERSIST_TIME(pTransInst->idleTime);
  pThrd->pTransInst = trans;

  pThrd->destroyAhandleFp = pTransInst->destroyFp;
  pThrd->fqdn2ipCache = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  pThrd->failFastCache = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);

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

  transDQDestroy(pThrd->delayQueue, destroyCmsgAndAhandle);
  transDQDestroy(pThrd->timeoutQueue, NULL);

  tDebug("thread destroy %" PRId64, pThrd->pid);
  for (int i = 0; i < taosArrayGetSize(pThrd->timerList); i++) {
    uv_timer_t* timer = taosArrayGetP(pThrd->timerList, i);
    taosMemoryFree(timer);
  }
  taosArrayDestroy(pThrd->timerList);
  taosMemoryFree(pThrd->prepare);
  taosMemoryFree(pThrd->loop);
  taosHashCleanup(pThrd->fqdn2ipCache);
  taosHashCleanup(pThrd->failFastCache);
  taosMemoryFree(pThrd);
}

static FORCE_INLINE void transDestroyConnCtx(STransConnCtx* ctx) {
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
    if (uv_handle_get_type(handle) == UV_TIMER) {
      // SCliConn* pConn = handle->data;
      //  if (pConn != NULL && pConn->timer != NULL) {
      //    SCliThrd* pThrd = pConn->hostThrd;
      //    uv_timer_stop((uv_timer_t*)handle);
      //    handle->data = NULL;
      //    taosArrayPush(pThrd->timerList, &pConn->timer);
      //    pConn->timer = NULL;
      //  }
    } else {
      uv_read_stop((uv_stream_t*)handle);
    }
    uv_close(handle, cliDestroy);
  }
}

FORCE_INLINE int cliRBChoseIdx(STrans* pTransInst) {
  int32_t index = pTransInst->index;
  if (pTransInst->numOfThreads == 0) {
    return -1;
  }
  /*
   * no lock, and to avoid CPU load imbalance, set limit pTransInst->numOfThreads * 2000;
   */
  if (pTransInst->index++ >= pTransInst->numOfThreads * 2000) {
    pTransInst->index = 0;
  }
  return index % pTransInst->numOfThreads;
}
static FORCE_INLINE void doDelayTask(void* param) {
  STaskArg* arg = param;
  cliHandleReq((SCliMsg*)arg->param1, (SCliThrd*)arg->param2);
  taosMemoryFree(arg);
}

static void doCloseIdleConn(void* param) {
  STaskArg* arg = param;
  SCliConn* conn = arg->param1;
  tDebug("%s conn %p idle, close it", CONN_GET_INST_LABEL(conn), conn);
  conn->task = NULL;
  cliDestroyConn(conn, true);
  taosMemoryFree(arg);
}

static void cliSchedMsgToNextNode(SCliMsg* pMsg, SCliThrd* pThrd) {
  STrans*        pTransInst = pThrd->pTransInst;
  STransConnCtx* pCtx = pMsg->ctx;

  STraceId* trace = &pMsg->msg.info.traceId;
  char      tbuf[256] = {0};
  EPSET_DEBUG_STR(&pCtx->epSet, tbuf);
  tGDebug("%s retry on next node,use:%s, step: %d,timeout:%" PRId64 "", transLabel(pThrd->pTransInst), tbuf,
          pCtx->retryStep, pCtx->retryNextInterval);

  STaskArg* arg = taosMemoryMalloc(sizeof(STaskArg));
  arg->param1 = pMsg;
  arg->param2 = pThrd;

  transDQSched(pThrd->delayQueue, doDelayTask, arg, pCtx->retryNextInterval);
}

FORCE_INLINE void cliCompareAndSwap(int8_t* val, int8_t exp, int8_t newVal) {
  if (*val != exp) {
    *val = newVal;
  }
}

FORCE_INLINE bool cliTryExtractEpSet(STransMsg* pResp, SEpSet* dst) {
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
bool cliResetEpset(STransConnCtx* pCtx, STransMsg* pResp, bool hasEpSet) {
  bool noDelay = true;
  if (hasEpSet == false) {
    if (pResp->contLen == 0) {
      if (pCtx->epsetRetryCnt >= pCtx->epSet.numOfEps) {
        noDelay = false;
      } else {
        EPSET_FORWARD_INUSE(&pCtx->epSet);
      }
    } else if (pResp->contLen != 0) {
      SEpSet  epSet;
      int32_t valid = tDeserializeSEpSet(pResp->pCont, pResp->contLen, &epSet);
      if (valid < 0) {
        tDebug("get invalid epset, epset equal, continue");
        if (pCtx->epsetRetryCnt >= pCtx->epSet.numOfEps) {
          noDelay = false;
        } else {
          EPSET_FORWARD_INUSE(&pCtx->epSet);
        }
      } else {
        if (!transEpSetIsEqual(&pCtx->epSet, &epSet)) {
          tDebug("epset not equal, retry new epset");
          pCtx->epSet = epSet;
          noDelay = false;
        } else {
          if (pCtx->epsetRetryCnt >= pCtx->epSet.numOfEps) {
            noDelay = false;
          } else {
            tDebug("epset equal, continue");
            EPSET_FORWARD_INUSE(&pCtx->epSet);
          }
        }
      }
    }
  } else {
    SEpSet  epSet;
    int32_t valid = tDeserializeSEpSet(pResp->pCont, pResp->contLen, &epSet);
    if (valid < 0) {
      tDebug("get invalid epset, epset equal, continue");
      if (pCtx->epsetRetryCnt >= pCtx->epSet.numOfEps) {
        noDelay = false;
      } else {
        EPSET_FORWARD_INUSE(&pCtx->epSet);
      }
    } else {
      if (!transEpSetIsEqual(&pCtx->epSet, &epSet)) {
        tDebug("epset not equal, retry new epset");
        pCtx->epSet = epSet;
        noDelay = false;
      } else {
        if (pCtx->epsetRetryCnt >= pCtx->epSet.numOfEps) {
          noDelay = false;
        } else {
          tDebug("epset equal, continue");
          EPSET_FORWARD_INUSE(&pCtx->epSet);
        }
      }
    }
  }
  return noDelay;
}
bool cliGenRetryRule(SCliConn* pConn, STransMsg* pResp, SCliMsg* pMsg) {
  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pTransInst = pThrd->pTransInst;

  STransConnCtx* pCtx = pMsg->ctx;
  int32_t        code = pResp->code;

  bool retry = pTransInst->retry != NULL ? pTransInst->retry(code, pResp->msgType - 1) : false;
  if (retry == false) {
    return false;
  }

  if (!pCtx->retryInit) {
    pCtx->retryMinInterval = pTransInst->retryMinInterval;
    pCtx->retryMaxInterval = pTransInst->retryMaxInterval;
    pCtx->retryStepFactor = pTransInst->retryStepFactor;
    pCtx->retryMaxTimeout = pTransInst->retryMaxTimouet;
    pCtx->retryInitTimestamp = taosGetTimestampMs();
    pCtx->retryNextInterval = pCtx->retryMinInterval;
    pCtx->retryStep = 0;
    pCtx->retryInit = true;
    pCtx->retryCode = TSDB_CODE_SUCCESS;

    // already retry, not use handle specified by app;
    pMsg->msg.info.handle = 0;
  }

  if (-1 != pCtx->retryMaxTimeout && taosGetTimestampMs() - pCtx->retryInitTimestamp >= pCtx->retryMaxTimeout) {
    return false;
  }

  // code, msgType

  // A:  epset,   leader, not self
  // B:  epset,   not know leader
  // C:  no epset, leader but not serivce

  bool noDelay = false;
  if (code == TSDB_CODE_RPC_BROKEN_LINK || code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
    tTrace("code str %s, contlen:%d 0", tstrerror(code), pResp->contLen);
    noDelay = cliResetEpset(pCtx, pResp, false);
    transFreeMsg(pResp->pCont);
    transUnrefCliHandle(pConn);
  } else if (code == TSDB_CODE_SYN_NOT_LEADER || code == TSDB_CODE_SYN_INTERNAL_ERROR ||
             code == TSDB_CODE_SYN_PROPOSE_NOT_READY || code == TSDB_CODE_VND_STOPPED ||
             code == TSDB_CODE_MNODE_NOT_FOUND || code == TSDB_CODE_APP_IS_STARTING ||
             code == TSDB_CODE_APP_IS_STOPPING || code == TSDB_CODE_VND_STOPPED) {
    tTrace("code str %s, contlen:%d 1", tstrerror(code), pResp->contLen);
    noDelay = cliResetEpset(pCtx, pResp, true);
    transFreeMsg(pResp->pCont);
    addConnToPool(pThrd->pool, pConn);
  } else if (code == TSDB_CODE_SYN_RESTORING) {
    tTrace("code str %s, contlen:%d 0", tstrerror(code), pResp->contLen);
    noDelay = cliResetEpset(pCtx, pResp, true);
    addConnToPool(pThrd->pool, pConn);
    transFreeMsg(pResp->pCont);
  } else {
    tTrace("code str %s, contlen:%d 0", tstrerror(code), pResp->contLen);
    noDelay = cliResetEpset(pCtx, pResp, false);
    addConnToPool(pThrd->pool, pConn);
    transFreeMsg(pResp->pCont);
  }
  if (code != TSDB_CODE_RPC_BROKEN_LINK && code != TSDB_CODE_RPC_NETWORK_UNAVAIL && code != TSDB_CODE_SUCCESS) {
    // save one internal code
    pCtx->retryCode = code;
  }

  if (noDelay == false) {
    pCtx->epsetRetryCnt = 1;
    pCtx->retryStep++;

    int64_t factor = pow(pCtx->retryStepFactor, pCtx->retryStep - 1);
    pCtx->retryNextInterval = factor * pCtx->retryMinInterval;
    if (pCtx->retryNextInterval >= pCtx->retryMaxInterval) {
      pCtx->retryNextInterval = pCtx->retryMaxInterval;
    }

    // if (-1 != pCtx->retryMaxTimeout && taosGetTimestampMs() - pCtx->retryInitTimestamp >= pCtx->retryMaxTimeout) {
    //   return false;
    // }
  } else {
    pCtx->retryNextInterval = 0;
    pCtx->epsetRetryCnt++;
  }

  pMsg->sent = 0;
  cliSchedMsgToNextNode(pMsg, pThrd);
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

  STransConnCtx* pCtx = pMsg->ctx;

  bool retry = cliGenRetryRule(pConn, pResp, pMsg);
  if (retry == true) {
    return -1;
  }

  if (pCtx->retryCode != TSDB_CODE_SUCCESS) {
    int32_t code = pResp->code;
    // return internal code app
    if (code == TSDB_CODE_RPC_NETWORK_UNAVAIL || code == TSDB_CODE_RPC_BROKEN_LINK ||
        code == TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED) {
      pResp->code = pCtx->retryCode;
    }
  }

  // check whole vnodes is offline on this vgroup
  if (pCtx->epsetRetryCnt >= pCtx->epSet.numOfEps || pCtx->retryStep > 0) {
    if (pResp->code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
      pResp->code = TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED;
    } else if (pResp->code == TSDB_CODE_RPC_BROKEN_LINK) {
      pResp->code = TSDB_CODE_RPC_SOMENODE_BROKEN_LINK;
    }
  }

  STraceId* trace = &pResp->info.traceId;
  bool      hasEpSet = cliTryExtractEpSet(pResp, &pCtx->epSet);
  if (hasEpSet) {
    char tbuf[256] = {0};
    EPSET_DEBUG_STR(&pCtx->epSet, tbuf);
    tGTrace("%s conn %p extract epset from msg", CONN_GET_INST_LABEL(pConn), pConn);
  }

  if (pCtx->pSem != NULL) {
    tGTrace("%s conn %p(sync) handle resp", CONN_GET_INST_LABEL(pConn), pConn);
    if (pCtx->pRsp == NULL) {
      tGTrace("%s conn %p(sync) failed to resp, ignore", CONN_GET_INST_LABEL(pConn), pConn);
    } else {
      memcpy((char*)pCtx->pRsp, (char*)pResp, sizeof(*pResp));
    }
    tsem_post(pCtx->pSem);
    pCtx->pRsp = NULL;
  } else {
    tGTrace("%s conn %p handle resp", CONN_GET_INST_LABEL(pConn), pConn);
    if (retry == false && hasEpSet == true) {
      pTransInst->cfp(pTransInst->parent, pResp, &pCtx->epSet);
    } else {
      if (!cliIsEpsetUpdated(pResp->code, pCtx)) {
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
static FORCE_INLINE SCliThrd* transGetWorkThrdFromHandle(STrans* trans, int64_t handle) {
  SCliThrd*  pThrd = NULL;
  SExHandle* exh = transAcquireExHandle(transGetRefMgt(), handle);
  if (exh == NULL) {
    return NULL;
  }

  if (exh->pThrd == NULL && trans != NULL) {
    int idx = cliRBChoseIdx(trans);
    if (idx < 0) return NULL;
    exh->pThrd = ((SCliObj*)trans->tcphandle)->pThreadObj[idx];
  }

  pThrd = exh->pThrd;
  transReleaseExHandle(transGetRefMgt(), handle);
  return pThrd;
}
SCliThrd* transGetWorkThrd(STrans* trans, int64_t handle) {
  if (handle == 0) {
    int idx = cliRBChoseIdx(trans);
    if (idx < 0) return NULL;
    return ((SCliObj*)trans->tcphandle)->pThreadObj[idx];
  }
  SCliThrd* pThrd = transGetWorkThrdFromHandle(trans, handle);
  return pThrd;
}
int transReleaseCliHandle(void* handle) {
  int  idx = -1;
  bool valid = false;

  SCliThrd* pThrd = transGetWorkThrdFromHandle(NULL, (int64_t)handle);
  if (pThrd == NULL) {
    return -1;
  }

  STransMsg tmsg = {.info.handle = handle, .info.ahandle = (void*)0x9527};
  TRACE_SET_MSGID(&tmsg.info.traceId, tGenIdPI64());

  STransConnCtx* pCtx = taosMemoryCalloc(1, sizeof(STransConnCtx));
  pCtx->ahandle = tmsg.info.ahandle;

  SCliMsg* cmsg = taosMemoryCalloc(1, sizeof(SCliMsg));
  cmsg->msg = tmsg;
  cmsg->st = taosGetTimestampUs();
  cmsg->type = Release;
  cmsg->ctx = pCtx;

  STraceId* trace = &tmsg.info.traceId;
  tGDebug("send release request at thread:%08" PRId64 "", pThrd->pid);

  if (0 != transAsyncSend(pThrd->asyncPool, &cmsg->q)) {
    destroyCmsg(cmsg);
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

  SCliThrd* pThrd = transGetWorkThrd(pTransInst, (int64_t)pReq->info.handle);
  if (pThrd == NULL) {
    transFreeMsg(pReq->pCont);
    transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
    return TSDB_CODE_RPC_BROKEN_LINK;
  }

  TRACE_SET_MSGID(&pReq->info.traceId, tGenIdPI64());

  STransConnCtx* pCtx = taosMemoryCalloc(1, sizeof(STransConnCtx));
  pCtx->epSet = *pEpSet;
  pCtx->ahandle = pReq->info.ahandle;
  pCtx->msgType = pReq->msgType;

  if (ctx != NULL) pCtx->appCtx = *ctx;

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

  SCliThrd* pThrd = transGetWorkThrd(pTransInst, (int64_t)pReq->info.handle);
  if (pThrd == NULL) {
    transFreeMsg(pReq->pCont);
    transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
    return TSDB_CODE_RPC_BROKEN_LINK;
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
    tstrncpy(cvtAddr.ip, ip, sizeof(cvtAddr.ip));
    tstrncpy(cvtAddr.fqdn, fqdn, sizeof(cvtAddr.fqdn));
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
