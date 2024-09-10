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

// clang-format off
#include "transComm.h"
#include "tmisce.h"
// clang-format on

typedef struct {
  int32_t numOfConn;
  queue   msgQ;
} SMsgList;

typedef struct SConnList {
  queue     conns;
  int32_t   size;
  SMsgList* list;
} SConnList;

typedef struct {
  queue   wq;
  int32_t len;

  int connMax;
  int connCnt;
  int batchLenLimit;
  int sending;

  char*    dst;
  char*    ip;
  uint16_t port;

} SCliBatchList;

typedef struct {
  queue          wq;
  queue          listq;
  int32_t        wLen;
  int32_t        batchSize;  //
  int32_t        batch;
  SCliBatchList* pList;
} SCliBatch;

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

  SCliBatch* pBatch;

  SDelayTask* task;

  uint32_t clientIp;
  uint32_t serverIp;

  char* dstAddr;
  char  src[32];
  char  dst[32];

  int64_t refId;
} SCliConn;

typedef struct SCliMsg {
  STransConnCtx* ctx;
  STransMsg      msg;
  queue          q;
  STransMsgType  type;

  int64_t  refId;
  uint64_t st;
  int      sent;  //(0: no send, 1: alread sent)
  queue    seqq;
} SCliMsg;

typedef struct SCliThrd {
  TdThread    thread;  // tid
  int64_t     pid;     // pid
  uv_loop_t*  loop;
  SAsyncPool* asyncPool;
  void*       pool;  // conn pool
  // timer handles
  SArray* timerList;
  // msg queue
  queue         msg;
  TdThreadMutex msgMtx;
  SDelayQueue*  delayQueue;
  SDelayQueue*  timeoutQueue;
  SDelayQueue*  waitConnQueue;
  uint64_t      nextTimeout;  // next timeout
  STrans*       pTransInst;   //

  int connCount;
  void (*destroyAhandleFp)(void* ahandle);
  SHashObj* fqdn2ipCache;
  SCvtAddr  cvtAddr;

  SHashObj* failFastCache;
  SHashObj* batchCache;

  SCliMsg* stopMsg;
  bool     quit;
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
static void*     destroyConnPool(SCliThrd* thread);
static SCliConn* getConnFromPool(SCliThrd* thread, char* key, bool* exceed);
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

static void cliHandleBatchReq(SCliBatch* pBatch, SCliThrd* pThrd);
static void cliSendBatchCb(uv_write_t* req, int status);

SCliBatch* cliGetHeadFromList(SCliBatchList* pList);

static bool cliRecvReleaseReq(SCliConn* conn, STransMsgHead* pHead);

static int32_t allocConnRef(SCliConn* conn, bool update);

static int cliAppCb(SCliConn* pConn, STransMsg* pResp, SCliMsg* pMsg);

static int32_t cliCreateConn(SCliThrd* thrd, SCliConn** pCliConn);
static void    cliDestroyConn(SCliConn* pConn, bool clear /*clear tcp handle or not*/);
static void    cliDestroy(uv_handle_t* handle);
static void    cliSend(SCliConn* pConn);
static void    cliSendBatch(SCliConn* pConn);
static void    cliDestroyConnMsgs(SCliConn* conn, bool destroy);

static void    doFreeTimeoutMsg(void* param);
static int32_t cliPreCheckSessionLimitForMsg(SCliThrd* pThrd, char* addr, SCliMsg** pMsg);

static void cliDestroyBatch(SCliBatch* pBatch);
// cli util func
static FORCE_INLINE bool cliIsEpsetUpdated(int32_t code, STransConnCtx* pCtx);
static FORCE_INLINE void cliMayCvtFqdnToIp(SEpSet* pEpSet, SCvtAddr* pCvtAddr);

static FORCE_INLINE int32_t cliBuildExceptResp(SCliMsg* pMsg, STransMsg* resp);

static FORCE_INLINE int32_t cliGetIpFromFqdnCache(SHashObj* cache, char* fqdn, uint32_t* ipaddr);
static FORCE_INLINE int32_t cliUpdateFqdnCache(SHashObj* cache, char* fqdn);

static FORCE_INLINE void cliMayUpdateFqdnCache(SHashObj* cache, char* dst);
// process data read from server, add decompress etc later
static void cliHandleResp(SCliConn* conn);
// handle except about conn
static void cliHandleExcept(SCliConn* conn, int32_t code);
static void cliReleaseUnfinishedMsg(SCliConn* conn);
static void cliHandleFastFail(SCliConn* pConn, int status);

static void doNotifyApp(SCliMsg* pMsg, SCliThrd* pThrd, int32_t code);
// handle req from app
static void cliHandleReq(SCliMsg* pMsg, SCliThrd* pThrd);
static void cliHandleQuit(SCliMsg* pMsg, SCliThrd* pThrd);
static void cliHandleRelease(SCliMsg* pMsg, SCliThrd* pThrd);
static void cliHandleUpdate(SCliMsg* pMsg, SCliThrd* pThrd);
static void cliHandleFreeById(SCliMsg* pMsg, SCliThrd* pThrd);

static void (*cliAsyncHandle[])(SCliMsg* pMsg, SCliThrd* pThrd) = {cliHandleReq, cliHandleQuit,   cliHandleRelease,
                                                                   NULL,         cliHandleUpdate, cliHandleFreeById};
/// static void (*cliAsyncHandle[])(SCliMsg* pMsg, SCliThrd* pThrd) = {cliHandleReq, cliHandleQuit, cliHandleRelease,
/// NULL,cliHandleUpdate};

static FORCE_INLINE void destroyCmsg(void* cmsg);

static FORCE_INLINE void destroyCmsgWrapper(void* arg, void* param);
static FORCE_INLINE void destroyCmsgAndAhandle(void* cmsg);
static FORCE_INLINE int  cliRBChoseIdx(STrans* pTransInst);
static FORCE_INLINE void transDestroyConnCtx(STransConnCtx* ctx);

// thread obj
static int32_t createThrdObj(void* trans, SCliThrd** pThrd);
static void    destroyThrdObj(SCliThrd* pThrd);

int32_t     cliSendQuit(SCliThrd* thrd);
static void cliWalkCb(uv_handle_t* handle, void* arg);

#define CLI_RELEASE_UV(loop)              \
  do {                                    \
    (void)uv_walk(loop, cliWalkCb, NULL); \
    (void)uv_run(loop, UV_RUN_DEFAULT);   \
    (void)uv_loop_close(loop);            \
  } while (0);

// snprintf may cause performance problem
#define CONN_CONSTRUCT_HASH_KEY(key, ip, port) \
  do {                                         \
    char*   t = key;                           \
    int16_t len = strlen(ip);                  \
    if (ip != NULL) memcpy(t, ip, len);        \
    t[len] = ':';                              \
    (void)titoa(port, 10, &t[len + 1]);        \
  } while (0)

#define CONN_PERSIST_TIME(para)   ((para) <= 90000 ? 90000 : (para))
#define CONN_GET_INST_LABEL(conn) (((STrans*)(((SCliThrd*)(conn)->hostThrd)->pTransInst))->label)

#define CONN_GET_MSGCTX_BY_AHANDLE(conn, ahandle)                                                                    \
  do {                                                                                                               \
    int i = 0, sz = transQueueSize(&conn->cliMsgs);                                                                  \
    for (; i < sz; i++) {                                                                                            \
      pMsg = transQueueGet(&conn->cliMsgs, i);                                                                       \
      if (pMsg->msg.msgType != TDMT_SCH_DROP_TASK && pMsg->ctx != NULL && (uint64_t)pMsg->ctx->ahandle == ahandle) { \
        break;                                                                                                       \
      }                                                                                                              \
    }                                                                                                                \
    if (i == sz) {                                                                                                   \
      pMsg = NULL;                                                                                                   \
    } else {                                                                                                         \
      pMsg = transQueueRm(&conn->cliMsgs, i);                                                                        \
    }                                                                                                                \
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

static void* cliWorkThread(void* arg);

static void cliReleaseUnfinishedMsg(SCliConn* conn) {
  SCliThrd* pThrd = conn->hostThrd;

  for (int i = 0; i < transQueueSize(&conn->cliMsgs); i++) {
    SCliMsg* msg = transQueueGet(&conn->cliMsgs, i);
    if (msg != NULL && msg->ctx != NULL && msg->ctx->ahandle != (void*)0x9527) {
      if (conn->ctx.freeFunc != NULL && msg->ctx->ahandle != NULL) {
        conn->ctx.freeFunc(msg->ctx->ahandle);
      } else if (msg->msg.info.notFreeAhandle == 0 && msg->ctx->ahandle != NULL && pThrd->destroyAhandleFp != NULL) {
        tDebug("%s conn %p destroy unfinished ahandle %p", CONN_GET_INST_LABEL(conn), conn, msg->ctx->ahandle);
        pThrd->destroyAhandleFp(msg->ctx->ahandle);
      }
    }
    destroyCmsg(msg);
  }
  transQueueClear(&conn->cliMsgs);
  memset(&conn->ctx, 0, sizeof(conn->ctx));
}
void cliResetTimer(SCliThrd* pThrd, SCliConn* conn) {
  if (conn->timer) {
    if (uv_is_active((uv_handle_t*)conn->timer)) {
      tDebug("%s conn %p stop timer", CONN_GET_INST_LABEL(conn), conn);
      (void)uv_timer_stop(conn->timer);
    }
    if (taosArrayPush(pThrd->timerList, &conn->timer) == NULL) {
      tError("failed to push timer %p to list, reason:%s", conn->timer, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      conn->timer = NULL;
      return;
    }
    conn->timer->data = NULL;
    conn->timer = NULL;
  }
}
bool cliMaySendCachedMsg(SCliConn* conn) {
  if (!transQueueEmpty(&conn->cliMsgs)) {
    SCliMsg* pCliMsg = NULL;
    CONN_GET_NEXT_SENDMSG(conn);
    cliSend(conn);
    return true;
  }
  return false;
_RETURN:
  return false;
}
bool cliConnSendSeqMsg(int64_t refId, SCliConn* conn) {
  if (refId == 0) return false;
  SExHandle* exh = transAcquireExHandle(transGetRefMgt(), refId);
  if (exh == NULL) {
    tDebug("release conn %p, refId: %" PRId64 "", conn, refId);
    return false;
  }
  taosWLockLatch(&exh->latch);
  if (exh->handle == NULL) exh->handle = conn;
  exh->inited = 1;
  exh->pThrd = conn->hostThrd;
  if (!QUEUE_IS_EMPTY(&exh->q)) {
    queue* h = QUEUE_HEAD(&exh->q);
    QUEUE_REMOVE(h);
    taosWUnLockLatch(&exh->latch);
    SCliMsg* t = QUEUE_DATA(h, SCliMsg, seqq);
    transCtxMerge(&conn->ctx, &t->ctx->appCtx);
    (void)transQueuePush(&conn->cliMsgs, t);
    tDebug("pop from conn %p, refId: %" PRId64 "", conn, refId);
    (void)transReleaseExHandle(transGetRefMgt(), refId);
    cliSend(conn);
    return true;
  }
  taosWUnLockLatch(&exh->latch);
  tDebug("empty conn %p, refId: %" PRId64 "", conn, refId);
  (void)transReleaseExHandle(transGetRefMgt(), refId);
  return false;
}

void cliHandleResp(SCliConn* conn) {
  SCliThrd* pThrd = conn->hostThrd;
  STrans*   pTransInst = pThrd->pTransInst;

  cliResetTimer(pThrd, conn);

  STransMsgHead* pHead = NULL;

  int8_t  resetBuf = conn->status == ConnAcquire ? 0 : 1;
  int32_t msgLen = transDumpFromBuffer(&conn->readBuf, (char**)&pHead, resetBuf);
  if (msgLen <= 0) {
    taosMemoryFree(pHead);
    tDebug("%s conn %p recv invalid packet ", CONN_GET_INST_LABEL(conn), conn);
    return;
  }

  if (resetBuf == 0) {
    tTrace("%s conn %p not reset read buf", transLabel(pTransInst), conn);
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
  transMsg.info.cliVer = htonl(pHead->compatibilityVer);

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
    transMsg.info.refId = (int64_t)(void*)conn->refId;
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
  int64_t refId = (pMsg == NULL ? 0 : (int64_t)(pMsg->msg.info.handle));
  tDebug("conn %p msg refId: %" PRId64 "", conn, refId);
  destroyCmsg(pMsg);

  if (cliConnSendSeqMsg(refId, conn)) {
    return;
  }

  if (cliMaySendCachedMsg(conn) == true) {
    return;
  }

  if (CONN_NO_PERSIST_BY_APP(conn)) {
    return addConnToPool(pThrd->pool, conn);
  }

  (void)uv_read_start((uv_stream_t*)conn->stream, cliAllocRecvBufferCb, cliRecvCb);
}
static void cliDestroyMsgInExhandle(int64_t refId) {
  if (refId == 0) return;
  SExHandle* exh = transAcquireExHandle(transGetRefMgt(), refId);
  if (exh) {
    taosWLockLatch(&exh->latch);
    while (!QUEUE_IS_EMPTY(&exh->q)) {
      queue* h = QUEUE_HEAD(&exh->q);
      QUEUE_REMOVE(h);
      SCliMsg* t = QUEUE_DATA(h, SCliMsg, seqq);
      destroyCmsg(t);
    }
    taosWUnLockLatch(&exh->latch);
    (void)transReleaseExHandle(transGetRefMgt(), refId);
  }
}

void cliHandleExceptImpl(SCliConn* pConn, int32_t code) {
  if (transQueueEmpty(&pConn->cliMsgs)) {
    if (pConn->broken == true && CONN_NO_PERSIST_BY_APP(pConn)) {
      tTrace("%s conn %p handle except, persist:0", CONN_GET_INST_LABEL(pConn), pConn);
      if (T_REF_VAL_GET(pConn) > 1) transUnrefCliHandle(pConn);
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
    transMsg.info.cliVer = pTransInst->compatibilityVer;

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
      int64_t refId = (pMsg == NULL ? 0 : (int64_t)(pMsg->msg.info.handle));
      cliDestroyMsgInExhandle(refId);
      if (cliAppCb(pConn, &transMsg, pMsg) != 0) {
        return;
      }
    }
    destroyCmsg(pMsg);
    tTrace("%s conn %p start to destroy, ref:%d", CONN_GET_INST_LABEL(pConn), pConn, T_REF_VAL_GET(pConn));
  } while (!transQueueEmpty(&pConn->cliMsgs));
  if (T_REF_VAL_GET(pConn) > 1) transUnrefCliHandle(pConn);
  transUnrefCliHandle(pConn);
}
void cliHandleExcept(SCliConn* conn, int32_t code) {
  tTrace("%s conn %p except ref:%d", CONN_GET_INST_LABEL(conn), conn, T_REF_VAL_GET(conn));
  if (code != TSDB_CODE_RPC_FQDN_ERROR) {
    code = -1;
  }
  cliHandleExceptImpl(conn, -1);
}

void cliConnTimeout(uv_timer_t* handle) {
  SCliConn* conn = handle->data;
  SCliThrd* pThrd = conn->hostThrd;

  tTrace("%s conn %p conn timeout, ref:%d", CONN_GET_INST_LABEL(conn), conn, T_REF_VAL_GET(conn));

  (void)uv_timer_stop(handle);
  handle->data = NULL;

  cliResetTimer(pThrd, conn);

  cliMayUpdateFqdnCache(pThrd->fqdn2ipCache, conn->dstAddr);
  cliHandleFastFail(conn, UV_ECANCELED);
}
void cliReadTimeoutCb(uv_timer_t* handle) {
  // set up timeout cb
  SCliConn* conn = handle->data;
  tTrace("%s conn %p timeout, ref:%d", CONN_GET_INST_LABEL(conn), conn, T_REF_VAL_GET(conn));
  (void)uv_read_stop(conn->stream);
  cliHandleExceptImpl(conn, TSDB_CODE_RPC_TIMEOUT);
}

void* createConnPool(int size) {
  // thread local, no lock
  return taosHashInit(size, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
}
void* destroyConnPool(SCliThrd* pThrd) {
  void* pool = pThrd->pool;
  if (pool == NULL) {
    return NULL;
  }

  SConnList* connList = taosHashIterate((SHashObj*)pool, NULL);
  while (connList != NULL) {
    while (!QUEUE_IS_EMPTY(&connList->conns)) {
      queue*    h = QUEUE_HEAD(&connList->conns);
      SCliConn* c = QUEUE_DATA(h, SCliConn, q);
      cliDestroyConn(c, true);
    }

    SMsgList* msglist = connList->list;
    while (!QUEUE_IS_EMPTY(&msglist->msgQ)) {
      queue* h = QUEUE_HEAD(&msglist->msgQ);
      QUEUE_REMOVE(h);

      SCliMsg* pMsg = QUEUE_DATA(h, SCliMsg, q);

      transDQCancel(pThrd->waitConnQueue, pMsg->ctx->task);
      pMsg->ctx->task = NULL;

      doNotifyApp(pMsg, pThrd, TSDB_CODE_RPC_MAX_SESSIONS);
    }
    taosMemoryFree(msglist);

    connList = taosHashIterate((SHashObj*)pool, connList);
  }
  taosHashCleanup(pool);
  pThrd->pool = NULL;
  return NULL;
}

static SCliConn* getConnFromPool(SCliThrd* pThrd, char* key, bool* exceed) {
  int32_t    code = 0;
  void*      pool = pThrd->pool;
  STrans*    pTranInst = pThrd->pTransInst;
  size_t     klen = strlen(key);
  SConnList* plist = taosHashGet((SHashObj*)pool, key, klen);
  if (plist == NULL) {
    SConnList list = {0};
    if ((code = taosHashPut((SHashObj*)pool, key, klen, (void*)&list, sizeof(list))) != 0) {
      return NULL;
    }
    plist = taosHashGet(pool, key, klen);

    SMsgList* nList = taosMemoryCalloc(1, sizeof(SMsgList));
    QUEUE_INIT(&nList->msgQ);
    nList->numOfConn++;

    QUEUE_INIT(&plist->conns);
    plist->list = nList;
  }

  if (QUEUE_IS_EMPTY(&plist->conns)) {
    if (plist->list->numOfConn >= pTranInst->connLimitNum) {
      *exceed = true;
      return NULL;
    }
    plist->list->numOfConn++;
    return NULL;
  }

  queue* h = QUEUE_TAIL(&plist->conns);
  QUEUE_REMOVE(h);
  plist->size -= 1;

  SCliConn* conn = QUEUE_DATA(h, SCliConn, q);
  conn->status = ConnNormal;
  QUEUE_INIT(&conn->q);
  tDebug("conn %p get from pool, pool size: %d, dst: %s", conn, conn->list->size, conn->dstAddr);

  if (conn->task != NULL) {
    transDQCancel(((SCliThrd*)conn->hostThrd)->timeoutQueue, conn->task);
    conn->task = NULL;
  }
  return conn;
}

static SCliConn* getConnFromPool2(SCliThrd* pThrd, char* key, SCliMsg** pMsg) {
  int32_t    code = 0;
  void*      pool = pThrd->pool;
  STrans*    pTransInst = pThrd->pTransInst;
  size_t     klen = strlen(key);
  SConnList* plist = taosHashGet((SHashObj*)pool, key, klen);
  if (plist == NULL) {
    SConnList list = {0};
    if ((code = taosHashPut((SHashObj*)pool, key, klen, (void*)&list, sizeof(list))) != 0) {
      tError("failed to put key %s to pool, reason:%s", key, tstrerror(code));
      return NULL;
    }
    plist = taosHashGet(pool, key, klen);

    SMsgList* nList = taosMemoryCalloc(1, sizeof(SMsgList));
    QUEUE_INIT(&nList->msgQ);
    nList->numOfConn++;

    QUEUE_INIT(&plist->conns);
    plist->list = nList;
  }

  STraceId* trace = &(*pMsg)->msg.info.traceId;
  // no avaliable conn in pool
  if (QUEUE_IS_EMPTY(&plist->conns)) {
    SMsgList* list = plist->list;
    if ((list)->numOfConn >= pTransInst->connLimitNum) {
      STraceId* trace = &(*pMsg)->msg.info.traceId;
      if (pTransInst->notWaitAvaliableConn ||
          (pTransInst->noDelayFp != NULL && pTransInst->noDelayFp((*pMsg)->msg.msgType))) {
        tDebug("%s msg %s not to send, reason: %s", pTransInst->label, TMSG_INFO((*pMsg)->msg.msgType),
               tstrerror(TSDB_CODE_RPC_NETWORK_BUSY));
        doNotifyApp(*pMsg, pThrd, TSDB_CODE_RPC_NETWORK_BUSY);
        *pMsg = NULL;
        return NULL;
      }

      STaskArg* arg = taosMemoryMalloc(sizeof(STaskArg));
      if (arg == NULL) {
        doNotifyApp(*pMsg, pThrd, TSDB_CODE_OUT_OF_MEMORY);
        *pMsg = NULL;
        return NULL;
      }
      arg->param1 = *pMsg;
      arg->param2 = pThrd;

      SDelayTask* task = transDQSched(pThrd->waitConnQueue, doFreeTimeoutMsg, arg, pTransInst->timeToGetConn);
      if (task == NULL) {
        taosMemoryFree(arg);
        doNotifyApp(*pMsg, pThrd, TSDB_CODE_OUT_OF_MEMORY);
        *pMsg = NULL;
        return NULL;
      }
      (*pMsg)->ctx->task = task;
      tGTrace("%s msg %s delay to send, wait for avaiable connect", pTransInst->label, TMSG_INFO((*pMsg)->msg.msgType));
      QUEUE_PUSH(&(list)->msgQ, &(*pMsg)->q);
      *pMsg = NULL;
    } else {
      // send msg in delay queue
      if (!(QUEUE_IS_EMPTY(&(list)->msgQ))) {
        STaskArg* arg = taosMemoryMalloc(sizeof(STaskArg));
        if (arg == NULL) {
          doNotifyApp(*pMsg, pThrd, TSDB_CODE_OUT_OF_MEMORY);
          *pMsg = NULL;
          return NULL;
        }
        arg->param1 = *pMsg;
        arg->param2 = pThrd;

        SDelayTask* task = transDQSched(pThrd->waitConnQueue, doFreeTimeoutMsg, arg, pTransInst->timeToGetConn);
        if (task == NULL) {
          taosMemoryFree(arg);
          doNotifyApp(*pMsg, pThrd, TSDB_CODE_OUT_OF_MEMORY);
          *pMsg = NULL;
          return NULL;
        }

        (*pMsg)->ctx->task = task;
        tGTrace("%s msg %s delay to send, wait for avaiable connect", pTransInst->label,
                TMSG_INFO((*pMsg)->msg.msgType));

        QUEUE_PUSH(&(list)->msgQ, &(*pMsg)->q);
        queue* h = QUEUE_HEAD(&(list)->msgQ);
        QUEUE_REMOVE(h);
        SCliMsg* ans = QUEUE_DATA(h, SCliMsg, q);

        *pMsg = ans;

        trace = &(*pMsg)->msg.info.traceId;
        tGTrace("%s msg %s pop from delay queue, start to send", pTransInst->label, TMSG_INFO((*pMsg)->msg.msgType));
        transDQCancel(pThrd->waitConnQueue, ans->ctx->task);
      }
      list->numOfConn++;
    }
    tDebug("%s numOfConn: %d, limit: %d, dst:%s", pTransInst->label, list->numOfConn, pTransInst->connLimitNum, key);
    return NULL;
  }

  queue* h = QUEUE_TAIL(&plist->conns);
  plist->size -= 1;
  QUEUE_REMOVE(h);

  SCliConn* conn = QUEUE_DATA(h, SCliConn, q);
  conn->status = ConnNormal;
  QUEUE_INIT(&conn->q);
  tDebug("conn %p get from pool, pool size: %d, dst: %s", conn, conn->list->size, conn->dstAddr);

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
  int32_t code = allocConnRef(conn, true);
  if (code != 0) {
    cliDestroyConn(conn, true);
    return;
  }

  SCliThrd* thrd = conn->hostThrd;
  cliResetTimer(thrd, conn);

  if (T_REF_VAL_GET(conn) > 1) {
    transUnrefCliHandle(conn);
  }

  cliDestroyConnMsgs(conn, false);

  if (conn->list == NULL) {
    conn->list = taosHashGet((SHashObj*)pool, conn->dstAddr, strlen(conn->dstAddr));
  }

  SConnList* pList = conn->list;
  SMsgList*  msgList = pList->list;
  if (!QUEUE_IS_EMPTY(&msgList->msgQ)) {
    queue* h = QUEUE_HEAD(&(msgList)->msgQ);
    QUEUE_REMOVE(h);

    SCliMsg* pMsg = QUEUE_DATA(h, SCliMsg, q);

    transDQCancel(thrd->waitConnQueue, pMsg->ctx->task);
    pMsg->ctx->task = NULL;

    transCtxMerge(&conn->ctx, &pMsg->ctx->appCtx);
    (void)transQueuePush(&conn->cliMsgs, pMsg);

    conn->status = ConnNormal;
    cliSend(conn);
    return;
  }

  conn->status = ConnInPool;
  QUEUE_PUSH(&conn->list->conns, &conn->q);
  conn->list->size += 1;
  tDebug("conn %p added to pool, pool size: %d, dst: %s", conn, conn->list->size, conn->dstAddr);

  if (conn->list->size >= 10) {
    STaskArg* arg = taosMemoryCalloc(1, sizeof(STaskArg));
    if (arg == NULL) return;
    arg->param1 = conn;
    arg->param2 = thrd;

    STrans* pTransInst = thrd->pTransInst;
    conn->task = transDQSched(thrd->timeoutQueue, doCloseIdleConn, arg, 10 * CONN_PERSIST_TIME(pTransInst->idleTime));
  }
}
static int32_t allocConnRef(SCliConn* conn, bool update) {
  if (update) {
    (void)transReleaseExHandle(transGetRefMgt(), conn->refId);
    (void)transRemoveExHandle(transGetRefMgt(), conn->refId);
    conn->refId = -1;
  }

  SExHandle* exh = taosMemoryCalloc(1, sizeof(SExHandle));
  if (exh == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  exh->refId = transAddExHandle(transGetRefMgt(), exh);
  if (exh->refId < 0) {
    taosMemoryFree(exh);
    return TSDB_CODE_REF_INVALID_ID;
  }

  QUEUE_INIT(&exh->q);
  taosInitRWLatch(&exh->latch);
  exh->handle = conn;
  exh->pThrd = conn->hostThrd;

  SExHandle* self = transAcquireExHandle(transGetRefMgt(), exh->refId);
  if (self != exh) {
    taosMemoryFree(exh);
    return TSDB_CODE_REF_INVALID_ID;
  }

  conn->refId = exh->refId;
  if (conn->refId < 0) {
    taosMemoryFree(exh);
  }
  return 0;
}

static int32_t specifyConnRef(SCliConn* conn, bool update, int64_t handle) {
  if (update) {
    (void)transReleaseExHandle(transGetRefMgt(), conn->refId);
    (void)transRemoveExHandle(transGetRefMgt(), conn->refId);
    conn->refId = -1;
  }
  SExHandle* exh = transAcquireExHandle(transGetRefMgt(), handle);
  if (exh == NULL) {
    return -1;
  }
  taosWLockLatch(&exh->latch);
  exh->handle = conn;
  exh->pThrd = conn->hostThrd;
  taosWUnLockLatch(&exh->latch);

  conn->refId = exh->refId;

  tDebug("conn %p specified by %" PRId64 "", conn, handle);

  (void)transReleaseExHandle(transGetRefMgt(), handle);
  return 0;
}

static void cliAllocRecvBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  SCliConn*    conn = handle->data;
  SConnBuffer* pBuf = &conn->readBuf;
  int32_t      code = transAllocBuffer(pBuf, buf);
  if (code < 0) {
    tError("conn %p failed to alloc buffer, since %s", conn, tstrerror(code));
  }
}
static void cliRecvCb(uv_stream_t* handle, ssize_t nread, const uv_buf_t* buf) {
  STUB_RAND_NETWORK_ERR(nread);

  if (handle->data == NULL) {
    return;
  }

  SCliConn*    conn = handle->data;
  SConnBuffer* pBuf = &conn->readBuf;
  if (nread > 0) {
    pBuf->len += nread;
    while (transReadComplete(pBuf)) {
      tTrace("%s conn %p read complete", CONN_GET_INST_LABEL(conn), conn);
      if (pBuf->invalid) {
        cliHandleExcept(conn, -1);
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
    tDebug("%s conn %p read error:%s, ref:%d", CONN_GET_INST_LABEL(conn), conn, uv_err_name(nread),
           T_REF_VAL_GET(conn));
    conn->broken = true;
    cliHandleExcept(conn, -1);
  }
}

static int32_t cliCreateConn(SCliThrd* pThrd, SCliConn** pCliConn) {
  int32_t   code = 0;
  SCliConn* conn = taosMemoryCalloc(1, sizeof(SCliConn));
  if (conn == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // read/write stream handle
  conn->stream = (uv_stream_t*)taosMemoryMalloc(sizeof(uv_tcp_t));
  if (conn->stream == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TAOS_CHECK_GOTO(code, NULL, _failed);
  }

  code = uv_tcp_init(pThrd->loop, (uv_tcp_t*)(conn->stream));
  if (code != 0) {
    tError("failed to init tcp handle, code:%d, %s", code, uv_strerror(code));
    code = TSDB_CODE_THIRDPARTY_ERROR;
    TAOS_CHECK_GOTO(code, NULL, _failed);
  }
  conn->stream->data = conn;

  uv_timer_t* timer = taosArrayGetSize(pThrd->timerList) > 0 ? *(uv_timer_t**)taosArrayPop(pThrd->timerList) : NULL;
  if (timer == NULL) {
    timer = taosMemoryCalloc(1, sizeof(uv_timer_t));
    if (timer == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _failed);
    }

    tDebug("no available timer, create a timer %p", timer);
    (void)uv_timer_init(pThrd->loop, timer);
  }
  timer->data = conn;

  conn->timer = timer;
  conn->connReq.data = conn;
  transReqQueueInit(&conn->wreqQueue);

  TAOS_CHECK_GOTO(transQueueInit(&conn->cliMsgs, NULL), NULL, _failed);

  TAOS_CHECK_GOTO(transInitBuffer(&conn->readBuf), NULL, _failed);

  QUEUE_INIT(&conn->q);
  conn->hostThrd = pThrd;
  conn->status = ConnNormal;
  conn->broken = false;
  transRefCliHandle(conn);

  (void)atomic_add_fetch_32(&pThrd->connCount, 1);

  TAOS_CHECK_GOTO(allocConnRef(conn, false), NULL, _failed);

  *pCliConn = conn;
  return code;
_failed:
  if (conn) {
    taosMemoryFree(conn->stream);
    transReqQueueClear(&conn->wreqQueue);
    (void)transDestroyBuffer(&conn->readBuf);
    transQueueDestroy(&conn->cliMsgs);
  }
  taosMemoryFree(conn);
  return code;
}
static void cliDestroyConn(SCliConn* conn, bool clear) {
  SCliThrd* pThrd = conn->hostThrd;
  tTrace("%s conn %p remove from conn pool", CONN_GET_INST_LABEL(conn), conn);
  conn->broken = true;
  QUEUE_REMOVE(&conn->q);
  QUEUE_INIT(&conn->q);

  conn->broken = true;
  if (conn->list == NULL) {
    conn->list = taosHashGet((SHashObj*)pThrd->pool, conn->dstAddr, strlen(conn->dstAddr));
  }

  if (conn->list) {
    SConnList* list = conn->list;
    list->list->numOfConn--;
    if (conn->status == ConnInPool) {
      list->size--;
    }
  }
  conn->list = NULL;

  (void)transReleaseExHandle(transGetRefMgt(), conn->refId);
  (void)transRemoveExHandle(transGetRefMgt(), conn->refId);
  conn->refId = -1;

  if (conn->task != NULL) {
    transDQCancel(pThrd->timeoutQueue, conn->task);
    conn->task = NULL;
  }
  cliResetTimer(pThrd, conn);

  if (clear) {
    if (!uv_is_closing((uv_handle_t*)conn->stream)) {
      (void)uv_read_stop(conn->stream);
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
  cliResetTimer(pThrd, conn);

  (void)atomic_sub_fetch_32(&pThrd->connCount, 1);

  if (conn->refId > 0) {
    (void)transReleaseExHandle(transGetRefMgt(), conn->refId);
    (void)transRemoveExHandle(transGetRefMgt(), conn->refId);
  }
  taosMemoryFree(conn->dstAddr);
  taosMemoryFree(conn->stream);

  cliDestroyConnMsgs(conn, true);

  tTrace("%s conn %p destroy successfully", CONN_GET_INST_LABEL(conn), conn);
  transReqQueueClear(&conn->wreqQueue);
  (void)transDestroyBuffer(&conn->readBuf);

  taosMemoryFree(conn);
}
static bool cliHandleNoResp(SCliConn* conn) {
  bool res = false;
  if (!transQueueEmpty(&conn->cliMsgs)) {
    SCliMsg* pMsg = transQueueGet(&conn->cliMsgs, 0);
    if (REQUEST_NO_RESP(&pMsg->msg)) {
      (void)transQueuePop(&conn->cliMsgs);
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
  STUB_RAND_NETWORK_ERR(status);

  SCliConn* pConn = transReqQueueRemove(req);
  if (pConn == NULL) return;

  SCliMsg* pMsg = transQueueGet(&pConn->cliMsgs, 0);
  if (pMsg != NULL) {
    int64_t cost = taosGetTimestampUs() - pMsg->st;
    if (cost > 1000 * 50) {
      tTrace("%s conn %p send cost:%dus ", CONN_GET_INST_LABEL(pConn), pConn, (int)cost);
    }
  }
  if (pMsg != NULL && pMsg->msg.contLen == 0 && pMsg->msg.pCont != 0) {
    rpcFreeCont(pMsg->msg.pCont);
    pMsg->msg.pCont = 0;
  }

  if (status == 0) {
    tDebug("%s conn %p data already was written out", CONN_GET_INST_LABEL(pConn), pConn);
  } else {
    if (!uv_is_closing((uv_handle_t*)&pConn->stream)) {
      tError("%s conn %p failed to write:%s", CONN_GET_INST_LABEL(pConn), pConn, uv_err_name(status));
      cliHandleExcept(pConn, -1);
    }
    return;
  }
  if (cliHandleNoResp(pConn) == true) {
    tTrace("%s conn %p no resp required", CONN_GET_INST_LABEL(pConn), pConn);
    return;
  }
  (void)uv_read_start((uv_stream_t*)pConn->stream, cliAllocRecvBufferCb, cliRecvCb);
}
void cliSendBatch(SCliConn* pConn) {
  int32_t   code = 0;
  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pTransInst = pThrd->pTransInst;

  SCliBatch* pBatch = pConn->pBatch;
  int32_t    wLen = pBatch->wLen;

  pBatch->pList->connCnt += 1;

  uv_buf_t* wb = taosMemoryCalloc(wLen, sizeof(uv_buf_t));
  if (wb == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    tError("%s conn %p failed to send batch msg since:%s", CONN_GET_INST_LABEL(pConn), pConn, tstrerror(code));
    goto _exception;
  }

  int    i = 0;
  queue* h = NULL;
  QUEUE_FOREACH(h, &pBatch->wq) {
    SCliMsg* pCliMsg = QUEUE_DATA(h, SCliMsg, q);

    STransConnCtx* pCtx = pCliMsg->ctx;

    STransMsg* pMsg = (STransMsg*)(&pCliMsg->msg);
    if (pMsg->pCont == 0) {
      pMsg->pCont = (void*)rpcMallocCont(0);
      if (pMsg->pCont == NULL) {
        code = TSDB_CODE_OUT_OF_BUFFER;
        tError("%s conn %p failed to send batch msg since:%s", CONN_GET_INST_LABEL(pConn), pConn, tstrerror(code));
        goto _exception;
      }
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
      pHead->version = TRANS_VER;
      pHead->compatibilityVer = htonl(pTransInst->compatibilityVer);
    }
    pHead->timestamp = taosHton64(taosGetTimestampUs());

    if (pHead->comp == 0 && pMsg->info.compressed == 0 && pConn->clientIp != pConn->serverIp) {
      if (pTransInst->compressSize != -1 && pTransInst->compressSize < pMsg->contLen) {
        msgLen = transCompressMsg(pMsg->pCont, pMsg->contLen) + sizeof(STransMsgHead);
        pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
      }
    } else {
      msgLen = (int32_t)ntohl((uint32_t)(pHead->msgLen));
    }
    wb[i++] = uv_buf_init((char*)pHead, msgLen);
  }

  uv_write_t* req = taosMemoryCalloc(1, sizeof(uv_write_t));
  if (req == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    tError("%s conn %p failed to send batch msg since:%s", CONN_GET_INST_LABEL(pConn), pConn, tstrerror(code));
    goto _exception;
  }
  req->data = pConn;
  tDebug("%s conn %p start to send batch msg, batch size:%d, msgLen:%d", CONN_GET_INST_LABEL(pConn), pConn,
         pBatch->wLen, pBatch->batchSize);

  code = uv_write(req, (uv_stream_t*)pConn->stream, wb, wLen, cliSendBatchCb);
  if (code != 0) {
    tDebug("%s conn %p failed to to send batch msg since %s", CONN_GET_INST_LABEL(pConn), pConn, uv_err_name(code));
    goto _exception;
  }

  taosMemoryFree(wb);
  return;

_exception:
  cliDestroyBatch(pBatch);
  taosMemoryFree(wb);
  pConn->pBatch = NULL;
  return;
}
void cliSend(SCliConn* pConn) {
  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pTransInst = pThrd->pTransInst;

  if (transQueueEmpty(&pConn->cliMsgs)) {
    tError("%s conn %p not msg to send", pTransInst->label, pConn);
    cliHandleExcept(pConn, -1);
    return;
  }

  SCliMsg* pCliMsg = NULL;
  CONN_GET_NEXT_SENDMSG(pConn);
  pCliMsg->sent = 1;

  STransConnCtx* pCtx = pCliMsg->ctx;

  STransMsg* pMsg = (STransMsg*)(&pCliMsg->msg);
  if (pMsg->pCont == 0) {
    pMsg->pCont = (void*)rpcMallocCont(0);
    tDebug("malloc memory: %p", pMsg->pCont);
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
    pHead->version = TRANS_VER;
    pHead->compatibilityVer = htonl(pTransInst->compatibilityVer);
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
      (void)uv_timer_init(pThrd->loop, timer);
    }
    timer->data = pConn;
    pConn->timer = timer;

    tGTrace("%s conn %p start timer for msg:%s", CONN_GET_INST_LABEL(pConn), pConn, TMSG_INFO(pMsg->msgType));
    (void)uv_timer_start((uv_timer_t*)pConn->timer, cliReadTimeoutCb, TRANS_READ_TIMEOUT, 0);
  }

  if (pHead->comp == 0 && pMsg->info.compressed == 0 && pConn->clientIp != pConn->serverIp) {
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
    cliHandleExcept(pConn, -1);
  }
  return;
_RETURN:
  return;
}

static void cliDestroyBatch(SCliBatch* pBatch) {
  if (pBatch == NULL) return;
  while (!QUEUE_IS_EMPTY(&pBatch->wq)) {
    queue* h = QUEUE_HEAD(&pBatch->wq);
    QUEUE_REMOVE(h);

    SCliMsg* p = QUEUE_DATA(h, SCliMsg, q);
    destroyCmsg(p);
  }
  SCliBatchList* p = pBatch->pList;
  p->sending -= 1;
  taosMemoryFree(pBatch);
}
static void cliHandleBatchReq(SCliBatch* pBatch, SCliThrd* pThrd) {
  int32_t code = 0;
  if (pThrd->quit == true) {
    cliDestroyBatch(pBatch);
    return;
  }

  if (pBatch == NULL || pBatch->wLen == 0 || QUEUE_IS_EMPTY(&pBatch->wq)) {
    return;
  }
  STrans*        pTransInst = pThrd->pTransInst;
  SCliBatchList* pList = pBatch->pList;

  char key[TSDB_FQDN_LEN + 64] = {0};
  CONN_CONSTRUCT_HASH_KEY(key, pList->ip, pList->port);

  bool      exceed = false;
  SCliConn* conn = getConnFromPool(pThrd, key, &exceed);

  if (conn == NULL && exceed) {
    tError("%s failed to send batch msg, batch size:%d, msgLen: %d, conn limit:%d", pTransInst->label, pBatch->wLen,
           pBatch->batchSize, pTransInst->connLimitNum);
    cliDestroyBatch(pBatch);
    return;
  }
  if (conn == NULL) {
    code = cliCreateConn(pThrd, &conn);
    if (code != 0) {
      tError("%s failed to send batch msg, batch size:%d, msgLen: %d, conn limit:%d, reason:%s", pTransInst->label,
             pBatch->wLen, pBatch->batchSize, pTransInst->connLimitNum, tstrerror(code));
      cliDestroyBatch(pBatch);
      return;
    }

    conn->pBatch = pBatch;
    conn->dstAddr = taosStrdup(pList->dst);
    if (conn->dstAddr == NULL) {
      tError("%s conn %p failed to send batch msg, reason:%s", transLabel(pTransInst), conn,
             tstrerror(TSDB_CODE_OUT_OF_MEMORY));
      cliHandleFastFail(conn, -1);
      return;
    }

    uint32_t ipaddr = 0;
    if ((code = cliGetIpFromFqdnCache(pThrd->fqdn2ipCache, pList->ip, &ipaddr)) != 0) {
      cliResetTimer(pThrd, conn);
      cliHandleFastFail(conn, code);
      return;
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ipaddr;
    addr.sin_port = (uint16_t)htons(pList->port);

    tTrace("%s conn %p try to connect to %s", pTransInst->label, conn, pList->dst);
    int32_t fd = taosCreateSocketWithTimeout(TRANS_CONN_TIMEOUT * 10);
    if (fd == -1) {
      tError("%s conn %p failed to create socket, reason:%s", transLabel(pTransInst), conn,
             tstrerror(TAOS_SYSTEM_ERROR(errno)));
      cliHandleFastFail(conn, -1);
      return;
    }
    int ret = uv_tcp_open((uv_tcp_t*)conn->stream, fd);
    if (ret != 0) {
      tError("%s conn %p failed to set stream, reason:%s", transLabel(pTransInst), conn, uv_err_name(ret));
      cliHandleFastFail(conn, -1);
      return;
    }
    ret = transSetConnOption((uv_tcp_t*)conn->stream, 20);
    if (ret != 0) {
      tError("%s conn %p failed to set socket opt, reason:%s", transLabel(pTransInst), conn, uv_err_name(ret));
      cliHandleFastFail(conn, -1);
      return;
    }

    ret = uv_tcp_connect(&conn->connReq, (uv_tcp_t*)(conn->stream), (const struct sockaddr*)&addr, cliConnCb);
    if (ret != 0) {
      cliResetTimer(pThrd, conn);

      cliMayUpdateFqdnCache(pThrd->fqdn2ipCache, conn->dstAddr);
      cliHandleFastFail(conn, -1);
      return;
    }
    (void)uv_timer_start(conn->timer, cliConnTimeout, TRANS_CONN_TIMEOUT, 0);
    return;
  }

  conn->pBatch = pBatch;
  cliSendBatch(conn);
}
static void cliSendBatchCb(uv_write_t* req, int status) {
  STUB_RAND_NETWORK_ERR(status);
  SCliConn*  conn = req->data;
  SCliThrd*  thrd = conn->hostThrd;
  SCliBatch* p = conn->pBatch;

  SCliBatchList* pBatchList = p->pList;
  SCliBatch*     nxtBatch = cliGetHeadFromList(pBatchList);
  pBatchList->connCnt -= 1;

  conn->pBatch = NULL;

  if (status != 0) {
    tDebug("%s conn %p failed to send batch msg, batch size:%d, msgLen:%d, reason:%s", CONN_GET_INST_LABEL(conn), conn,
           p->wLen, p->batchSize, uv_err_name(status));

    if (!uv_is_closing((uv_handle_t*)&conn->stream)) cliHandleExcept(conn, -1);

    cliHandleBatchReq(nxtBatch, thrd);
  } else {
    tDebug("%s conn %p succ to send batch msg, batch size:%d, msgLen:%d", CONN_GET_INST_LABEL(conn), conn, p->wLen,
           p->batchSize);
    if (!uv_is_closing((uv_handle_t*)&conn->stream) && conn->broken == false) {
      if (nxtBatch != NULL) {
        conn->pBatch = nxtBatch;
        cliSendBatch(conn);
      } else {
        addConnToPool(thrd->pool, conn);
      }
    } else {
      cliDestroyBatch(nxtBatch);
      // conn release by other callback
    }
  }

  cliDestroyBatch(p);
  taosMemoryFree(req);
}
static void cliHandleFastFail(SCliConn* pConn, int status) {
  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pTransInst = pThrd->pTransInst;

  if (status == -1) status = UV_EADDRNOTAVAIL;

  if (pConn->pBatch == NULL) {
    SCliMsg* pMsg = transQueueGet(&pConn->cliMsgs, 0);

    STraceId* trace = &pMsg->msg.info.traceId;
    tGError("%s msg %s failed to send, conn %p failed to connect to %s, reason: %s", CONN_GET_INST_LABEL(pConn),
            TMSG_INFO(pMsg->msg.msgType), pConn, pConn->dstAddr, uv_strerror(status));

    if (pMsg != NULL && REQUEST_NO_RESP(&pMsg->msg) &&
        (pTransInst->failFastFp != NULL && pTransInst->failFastFp(pMsg->msg.msgType))) {
      SFailFastItem* item = taosHashGet(pThrd->failFastCache, pConn->dstAddr, strlen(pConn->dstAddr));
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
        int32_t       code =
            taosHashPut(pThrd->failFastCache, pConn->dstAddr, strlen(pConn->dstAddr), &item, sizeof(SFailFastItem));
        if (code != 0) {
          tError("failed to put fail-fast item to cache, reason:%s", tstrerror(code));
        }
      }
    }
  } else {
    tError("%s batch msg failed to send, conn %p failed to connect to %s, reason: %s", CONN_GET_INST_LABEL(pConn),
           pConn, pConn->dstAddr, uv_strerror(status));
    cliDestroyBatch(pConn->pBatch);
    pConn->pBatch = NULL;
  }
  cliHandleExcept(pConn, status);
}

void cliConnCb(uv_connect_t* req, int status) {
  SCliConn* pConn = req->data;
  SCliThrd* pThrd = pConn->hostThrd;
  bool      timeout = false;

  if (pConn->timer == NULL) {
    timeout = true;
  } else {
    cliResetTimer(pThrd, pConn);
  }

  STUB_RAND_NETWORK_ERR(status);

  if (status != 0) {
    cliMayUpdateFqdnCache(pThrd->fqdn2ipCache, pConn->dstAddr);
    if (timeout == false) {
      cliHandleFastFail(pConn, status);
    } else if (timeout == true) {
      // already deal by timeout
    }
    return;
  }

  struct sockaddr peername, sockname;
  int             addrlen = sizeof(peername);
  (void)uv_tcp_getpeername((uv_tcp_t*)pConn->stream, &peername, &addrlen);
  (void)transSockInfo2Str(&peername, pConn->dst);

  addrlen = sizeof(sockname);
  (void)uv_tcp_getsockname((uv_tcp_t*)pConn->stream, &sockname, &addrlen);
  (void)transSockInfo2Str(&sockname, pConn->src);

  struct sockaddr_in addr = *(struct sockaddr_in*)&sockname;
  struct sockaddr_in saddr = *(struct sockaddr_in*)&peername;

  pConn->clientIp = addr.sin_addr.s_addr;
  pConn->serverIp = saddr.sin_addr.s_addr;

  tTrace("%s conn %p connect to server successfully", CONN_GET_INST_LABEL(pConn), pConn);
  if (pConn->pBatch != NULL) {
    cliSendBatch(pConn);
  } else {
    cliSend(pConn);
  }
}

static void doNotifyApp(SCliMsg* pMsg, SCliThrd* pThrd, int32_t code) {
  STransConnCtx* pCtx = pMsg->ctx;
  STrans*        pTransInst = pThrd->pTransInst;

  STransMsg transMsg = {0};
  transMsg.contLen = 0;
  transMsg.pCont = NULL;
  transMsg.code = code;
  transMsg.msgType = pMsg->msg.msgType + 1;
  transMsg.info.ahandle = pMsg->ctx->ahandle;
  transMsg.info.traceId = pMsg->msg.info.traceId;
  transMsg.info.hasEpSet = false;
  transMsg.info.cliVer = pTransInst->compatibilityVer;
  if (pCtx->pSem != NULL) {
    if (pCtx->pRsp == NULL) {
    } else {
      memcpy((char*)pCtx->pRsp, (char*)&transMsg, sizeof(transMsg));
    }
  } else {
    pTransInst->cfp(pTransInst->parent, &transMsg, NULL);
  }

  destroyCmsg(pMsg);
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

  (void)destroyConnPool(pThrd);
  (void)uv_walk(pThrd->loop, cliWalkCb, NULL);
}
static void cliHandleRelease(SCliMsg* pMsg, SCliThrd* pThrd) {
  int64_t    refId = (int64_t)(pMsg->msg.info.handle);
  SExHandle* exh = transAcquireExHandle(transGetRefMgt(), refId);
  if (exh == NULL) {
    tDebug("%" PRId64 " already released", refId);
    destroyCmsg(pMsg);
    return;
  }

  taosRLockLatch(&exh->latch);
  SCliConn* conn = exh->handle;
  taosRUnLockLatch(&exh->latch);

  (void)transReleaseExHandle(transGetRefMgt(), refId);
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
static void cliHandleFreeById(SCliMsg* pMsg, SCliThrd* pThrd) {
  int32_t    code = 0;
  int64_t    refId = (int64_t)(pMsg->msg.info.handle);
  SExHandle* exh = transAcquireExHandle(transGetRefMgt(), refId);
  if (exh == NULL) {
    tDebug("id %" PRId64 " already released", refId);
    destroyCmsg(pMsg);
    return;
  }

  taosRLockLatch(&exh->latch);
  SCliConn* conn = exh->handle;
  taosRUnLockLatch(&exh->latch);

  if (conn == NULL || conn->refId != refId) {
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, NULL, _exception);
  }
  tDebug("do free conn %p by id %" PRId64 "", conn, refId);

  int32_t size = transQueueSize(&conn->cliMsgs);
  if (size == 0) {
    // already recv, and notify upper layer
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, NULL, _exception);
  } else {
    destroyCmsg(pMsg);
    (void)transReleaseExHandle(transGetRefMgt(), refId);

    while (T_REF_VAL_GET(conn) >= 1) {
      transUnrefCliHandle(conn);
    }
    return;
  }
_exception:
  tDebug("already free conn %p by id %" PRId64 "", conn, refId);

  (void)transReleaseExHandle(transGetRefMgt(), refId);
  (void)transReleaseExHandle(transGetRefMgt(), refId);
  (void)transRemoveExHandle(transGetRefMgt(), refId);
  destroyCmsg(pMsg);
}

SCliConn* cliGetConn(SCliMsg** pMsg, SCliThrd* pThrd, bool* ignore, char* addr) {
  STransConnCtx* pCtx = (*pMsg)->ctx;
  SCliConn*      conn = NULL;

  int64_t refId = (int64_t)((*pMsg)->msg.info.handle);
  if (refId != 0) {
    SExHandle* exh = transAcquireExHandle(transGetRefMgt(), refId);
    if (exh == NULL) {
      tError("failed to get conn, refId: %" PRId64 "", refId);
      *ignore = true;
      return NULL;
    } else {
      taosRLockLatch(&exh->latch);
      conn = exh->handle;
      taosRUnLockLatch(&exh->latch);
      if (conn == NULL) {
        conn = getConnFromPool2(pThrd, addr, pMsg);
        if (conn != NULL) specifyConnRef(conn, true, refId);
      }
      (void)transReleaseExHandle(transGetRefMgt(), refId);
    }
    return conn;
  };

  conn = getConnFromPool2(pThrd, addr, pMsg);
  if (conn != NULL) {
    tTrace("%s conn %p get from conn pool:%p", CONN_GET_INST_LABEL(conn), conn, pThrd->pool);
  } else {
    tTrace("%s not found conn in conn pool:%p, dst:%s", ((STrans*)pThrd->pTransInst)->label, pThrd->pool, addr);
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

  // memset(pResp, 0, sizeof(STransMsg));

  if (pResp->code == 0) {
    pResp->code = TSDB_CODE_RPC_BROKEN_LINK;
  }
  pResp->msgType = pMsg->msg.msgType + 1;
  pResp->info.ahandle = pMsg->ctx ? pMsg->ctx->ahandle : NULL;
  pResp->info.traceId = pMsg->msg.info.traceId;

  return 0;
}

static FORCE_INLINE int32_t cliGetIpFromFqdnCache(SHashObj* cache, char* fqdn, uint32_t* ip) {
  int32_t   code = 0;
  uint32_t  addr = 0;
  size_t    len = strlen(fqdn);
  uint32_t* v = taosHashGet(cache, fqdn, len);
  if (v == NULL) {
    code = taosGetIpv4FromFqdn(fqdn, &addr);
    if (code != 0) {
      code = TSDB_CODE_RPC_FQDN_ERROR;
      tError("failed to get ip from fqdn:%s since %s", fqdn, tstrerror(code));
      return code;
    }

    if ((code = taosHashPut(cache, fqdn, len, &addr, sizeof(addr)) != 0)) {
      return code;
    }
    *ip = addr;
  } else {
    *ip = *v;
  }
  return 0;
}
static FORCE_INLINE int32_t cliUpdateFqdnCache(SHashObj* cache, char* fqdn) {
  // impl later
  uint32_t addr = 0;
  int32_t  code = taosGetIpv4FromFqdn(fqdn, &addr);
  if (code == 0) {
    size_t    len = strlen(fqdn);
    uint32_t* v = taosHashGet(cache, fqdn, len);
    if (addr != *v) {
      char old[64] = {0}, new[64] = {0};
      tinet_ntoa(old, *v);
      tinet_ntoa(new, addr);
      tWarn("update ip of fqdn:%s, old: %s, new: %s", fqdn, old, new);
      code = taosHashPut(cache, fqdn, strlen(fqdn), &addr, sizeof(addr));
    }
  } else {
    code = TSDB_CODE_RPC_FQDN_ERROR;  // TSDB_CODE_RPC_INVALID_FQDN;
  }
  return code;
}

static void cliMayUpdateFqdnCache(SHashObj* cache, char* dst) {
  if (dst == NULL) return;

  int16_t i = 0, len = strlen(dst);
  for (i = len - 1; i >= 0; i--) {
    if (dst[i] == ':') break;
  }
  if (i > 0) {
    char fqdn[TSDB_FQDN_LEN + 1] = {0};
    memcpy(fqdn, dst, i);
    (void)cliUpdateFqdnCache(cache, fqdn);
  }
}

static void doFreeTimeoutMsg(void* param) {
  STaskArg* arg = param;
  SCliMsg*  pMsg = arg->param1;
  SCliThrd* pThrd = arg->param2;
  STrans*   pTransInst = pThrd->pTransInst;
  int32_t   code = TSDB_CODE_RPC_MAX_SESSIONS;
  QUEUE_REMOVE(&pMsg->q);
  STraceId* trace = &pMsg->msg.info.traceId;
  tGTrace("%s msg %s cannot get available conn after timeout", pTransInst->label, TMSG_INFO(pMsg->msg.msgType));
  doNotifyApp(pMsg, pThrd, code);
  taosMemoryFree(arg);
}

void cliHandleReq(SCliMsg* pMsg, SCliThrd* pThrd) {
  int32_t code = 0;
  STrans* pTransInst = pThrd->pTransInst;

  cliMayCvtFqdnToIp(&pMsg->ctx->epSet, &pThrd->cvtAddr);
  if (!EPSET_IS_VALID(&pMsg->ctx->epSet)) {
    destroyCmsg(pMsg);
    return;
  }

  char*    fqdn = EPSET_GET_INUSE_IP(&pMsg->ctx->epSet);
  uint16_t port = EPSET_GET_INUSE_PORT(&pMsg->ctx->epSet);
  char     addr[TSDB_FQDN_LEN + 64] = {0};
  CONN_CONSTRUCT_HASH_KEY(addr, fqdn, port);

  bool      ignore = false;
  SCliConn* conn = cliGetConn(&pMsg, pThrd, &ignore, addr);
  if (ignore == true) {
    // persist conn already release by server
    STransMsg resp = {0};
    (void)cliBuildExceptResp(pMsg, &resp);
    // refactorr later
    resp.info.cliVer = pTransInst->compatibilityVer;

    if (pMsg->type != Release) {
      pTransInst->cfp(pTransInst->parent, &resp, NULL);
    }
    destroyCmsg(pMsg);
    return;
  }
  if (conn == NULL && pMsg == NULL) {
    return;
  }
  STraceId* trace = &pMsg->msg.info.traceId;

  if (conn != NULL) {
    transCtxMerge(&conn->ctx, &pMsg->ctx->appCtx);
    (void)transQueuePush(&conn->cliMsgs, pMsg);
    cliSend(conn);
  } else {
    code = cliCreateConn(pThrd, &conn);
    if (code != 0) {
      tError("%s failed to create conn, reason:%s", pTransInst->label, tstrerror(code));
      STransMsg resp = {.code = code};
      (void)cliBuildExceptResp(pMsg, &resp);

      resp.info.cliVer = pTransInst->compatibilityVer;
      if (pMsg->type != Release) {
        pTransInst->cfp(pTransInst->parent, &resp, NULL);
      }
      destroyCmsg(pMsg);
      return;
    }

    int64_t refId = (int64_t)pMsg->msg.info.handle;
    if (refId != 0) specifyConnRef(conn, true, refId);

    transCtxMerge(&conn->ctx, &pMsg->ctx->appCtx);
    (void)transQueuePush(&conn->cliMsgs, pMsg);

    conn->dstAddr = taosStrdup(addr);

    uint32_t ipaddr;
    int32_t  code = cliGetIpFromFqdnCache(pThrd->fqdn2ipCache, fqdn, &ipaddr);
    if (code != 0) {
      cliResetTimer(pThrd, conn);
      cliHandleExcept(conn, code);
      return;
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ipaddr;
    addr.sin_port = (uint16_t)htons(port);

    tGTrace("%s conn %p try to connect to %s", pTransInst->label, conn, conn->dstAddr);
    int32_t fd = taosCreateSocketWithTimeout(TRANS_CONN_TIMEOUT * 10);
    if (fd == -1) {
      tGError("%s conn %p failed to create socket, reason:%s", transLabel(pTransInst), conn,
              tstrerror(TAOS_SYSTEM_ERROR(errno)));
      cliHandleExcept(conn, -1);
      errno = 0;
      return;
    }

    int ret = uv_tcp_open((uv_tcp_t*)conn->stream, fd);
    if (ret != 0) {
      tGError("%s conn %p failed to set stream, reason:%s", transLabel(pTransInst), conn, uv_err_name(ret));
      cliHandleExcept(conn, -1);
      return;
    }

    ret = transSetConnOption((uv_tcp_t*)conn->stream, tsKeepAliveIdle);
    if (ret != 0) {
      tGError("%s conn %p failed to set socket opt, reason:%s", transLabel(pTransInst), conn, uv_err_name(ret));
      cliHandleExcept(conn, -1);
      return;
    }

    ret = uv_tcp_connect(&conn->connReq, (uv_tcp_t*)(conn->stream), (const struct sockaddr*)&addr, cliConnCb);
    if (ret != 0) {
      cliResetTimer(pThrd, conn);

      cliMayUpdateFqdnCache(pThrd->fqdn2ipCache, conn->dstAddr);
      cliHandleFastFail(conn, ret);
      return;
    }
    (void)uv_timer_start(conn->timer, cliConnTimeout, TRANS_CONN_TIMEOUT, 0);
  }
  tGTrace("%s conn %p ready", pTransInst->label, conn);
}

static void cliNoBatchDealReq(queue* wq, SCliThrd* pThrd) {
  int count = 0;

  while (!QUEUE_IS_EMPTY(wq)) {
    queue* h = QUEUE_HEAD(wq);
    QUEUE_REMOVE(h);

    SCliMsg* pMsg = QUEUE_DATA(h, SCliMsg, q);

    if (pMsg->type == Quit) {
      pThrd->stopMsg = pMsg;
      continue;
    }
    (*cliAsyncHandle[pMsg->type])(pMsg, pThrd);

    count++;
  }
  if (count >= 2) {
    tTrace("cli process batch size:%d", count);
  }
}
SCliBatch* cliGetHeadFromList(SCliBatchList* pList) {
  if (QUEUE_IS_EMPTY(&pList->wq) || pList->connCnt > pList->connMax || pList->sending > pList->connMax) {
    return NULL;
  }
  queue* hr = QUEUE_HEAD(&pList->wq);
  QUEUE_REMOVE(hr);
  pList->sending += 1;

  pList->len -= 1;

  SCliBatch* batch = QUEUE_DATA(hr, SCliBatch, listq);
  return batch;
}

static int32_t createBatchList(SCliBatchList** ppBatchList, char* key, char* ip, uint32_t port) {
  SCliBatchList* pBatchList = taosMemoryCalloc(1, sizeof(SCliBatchList));
  if (pBatchList == NULL) {
    tError("failed to create batch list, reason:%s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  QUEUE_INIT(&pBatchList->wq);
  pBatchList->port = port;
  pBatchList->connMax = 1;
  pBatchList->connCnt = 0;
  pBatchList->batchLenLimit = 0;
  pBatchList->len += 1;

  pBatchList->ip = taosStrdup(ip);
  pBatchList->dst = taosStrdup(key);
  if (pBatchList->ip == NULL || pBatchList->dst == NULL) {
    taosMemoryFree(pBatchList->ip);
    taosMemoryFree(pBatchList->dst);
    taosMemoryFree(pBatchList);
    tError("failed to create batch list, reason:%s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *ppBatchList = pBatchList;
  return 0;
}
static void destroyBatchList(SCliBatchList* pList) {
  if (pList == NULL) {
    return;
  }
  while (!QUEUE_IS_EMPTY(&pList->wq)) {
    queue* h = QUEUE_HEAD(&pList->wq);
    QUEUE_REMOVE(h);

    SCliBatch* pBatch = QUEUE_DATA(h, SCliBatch, listq);
    cliDestroyBatch(pBatch);
  }
  taosMemoryFree(pList->ip);
  taosMemoryFree(pList->dst);
  taosMemoryFree(pList);
}
static int32_t createBatch(SCliBatch** ppBatch, SCliBatchList* pList, SCliMsg* pMsg) {
  SCliBatch* pBatch = taosMemoryCalloc(1, sizeof(SCliBatch));
  if (pBatch == NULL) {
    tError("failed to create batch, reason:%s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  QUEUE_INIT(&pBatch->wq);
  QUEUE_INIT(&pBatch->listq);

  QUEUE_PUSH(&pBatch->wq, &pMsg->q);
  pBatch->wLen += 1;
  pBatch->batchSize = pMsg->msg.contLen;
  pBatch->pList = pList;

  QUEUE_PUSH(&pList->wq, &pBatch->listq);
  pList->len += 1;

  *ppBatch = pBatch;
  return 0;
}
static void cliBatchDealReq(queue* wq, SCliThrd* pThrd) {
  STrans* pInst = pThrd->pTransInst;
  int32_t code = 0;

  int count = 0;
  while (!QUEUE_IS_EMPTY(wq)) {
    queue* h = QUEUE_HEAD(wq);
    QUEUE_REMOVE(h);

    SCliMsg* pMsg = QUEUE_DATA(h, SCliMsg, q);

    if (pMsg->type == Quit) {
      pThrd->stopMsg = pMsg;
      continue;
    }

    if (pMsg->type == Normal && REQUEST_NO_RESP(&pMsg->msg)) {
      STransConnCtx* pCtx = pMsg->ctx;

      char*    ip = EPSET_GET_INUSE_IP(&pCtx->epSet);
      uint32_t port = EPSET_GET_INUSE_PORT(&pCtx->epSet);
      char     key[TSDB_FQDN_LEN + 64] = {0};
      CONN_CONSTRUCT_HASH_KEY(key, ip, port);
      size_t          klen = strlen(key);
      SCliBatchList** ppBatchList = taosHashGet(pThrd->batchCache, key, klen);
      if (ppBatchList == NULL || *ppBatchList == NULL) {
        SCliBatchList* pBatchList = NULL;
        code = createBatchList(&pBatchList, key, ip, port);
        if (code != 0) {
          destroyCmsg(pMsg);
          continue;
        }
        pBatchList->batchLenLimit = pInst->batchSize;

        SCliBatch* pBatch = NULL;
        code = createBatch(&pBatch, pBatchList, pMsg);
        if (code != 0) {
          destroyBatchList(pBatchList);
          destroyCmsg(pMsg);
          continue;
        }

        code = taosHashPut(pThrd->batchCache, key, klen, &pBatchList, sizeof(void*));
        if (code != 0) {
          destroyBatchList(pBatchList);
        }
      } else {
        if (QUEUE_IS_EMPTY(&(*ppBatchList)->wq)) {
          SCliBatch* pBatch = NULL;
          code = createBatch(&pBatch, *ppBatchList, pMsg);
          if (code != 0) {
            destroyCmsg(pMsg);
            cliDestroyBatch(pBatch);
          }
        } else {
          queue*     hdr = QUEUE_TAIL(&((*ppBatchList)->wq));
          SCliBatch* pBatch = QUEUE_DATA(hdr, SCliBatch, listq);
          if ((pBatch->batchSize + pMsg->msg.contLen) < (*ppBatchList)->batchLenLimit) {
            QUEUE_PUSH(&pBatch->wq, h);
            pBatch->batchSize += pMsg->msg.contLen;
            pBatch->wLen += 1;
          } else {
            SCliBatch* tBatch = NULL;
            code = createBatch(&tBatch, *ppBatchList, pMsg);
            if (code != 0) {
              destroyCmsg(pMsg);
            }
          }
        }
      }
      continue;
    }
    (*cliAsyncHandle[pMsg->type])(pMsg, pThrd);
    count++;
  }

  void** pIter = taosHashIterate(pThrd->batchCache, NULL);
  while (pIter != NULL) {
    SCliBatchList* batchList = (SCliBatchList*)(*pIter);
    SCliBatch*     batch = cliGetHeadFromList(batchList);
    if (batch != NULL) {
      cliHandleBatchReq(batch, pThrd);
    }
    pIter = (void**)taosHashIterate(pThrd->batchCache, pIter);
  }

  if (count >= 2) {
    tTrace("cli process batch size:%d", count);
  }
}

static void cliAsyncCb(uv_async_t* handle) {
  SAsyncItem* item = handle->data;
  SCliThrd*   pThrd = item->pThrd;
  STrans*     pTransInst = pThrd->pTransInst;

  // batch process to avoid to lock/unlock frequently
  queue wq;
  (void)taosThreadMutexLock(&item->mtx);
  QUEUE_MOVE(&item->qmsg, &wq);
  (void)taosThreadMutexUnlock(&item->mtx);

  int8_t supportBatch = pTransInst->supportBatch;
  if (supportBatch == 0) {
    cliNoBatchDealReq(&wq, pThrd);
  } else if (supportBatch == 1) {
    cliBatchDealReq(&wq, pThrd);
  }

  if (pThrd->stopMsg != NULL) cliHandleQuit(pThrd->stopMsg, pThrd);
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

void cliConnFreeMsgs(SCliConn* conn) {
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
    resp.info.cliVer = pTransInst->compatibilityVer;
    pTransInst->cfp(pTransInst->parent, &resp, NULL);

    cmsg->ctx->ahandle = NULL;
  }
}
bool cliRecvReleaseReq(SCliConn* conn, STransMsgHead* pHead) {
  if (pHead->release == 1 && (pHead->msgLen) == sizeof(*pHead)) {
    uint64_t ahandle = pHead->ahandle;
    SCliMsg* pMsg = NULL;
    CONN_GET_MSGCTX_BY_AHANDLE(conn, ahandle);
    tDebug("%s conn %p receive release request, refId:%" PRId64 ", may ignore", CONN_GET_INST_LABEL(conn), conn,
           conn->refId);

    (void)transClearBuffer(&conn->readBuf);
    transFreeMsg(transContFromHead((char*)pHead));

    for (int i = 0; ahandle == 0 && i < transQueueSize(&conn->cliMsgs); i++) {
      SCliMsg* cliMsg = transQueueGet(&conn->cliMsgs, i);
      if (cliMsg->type == Release) {
        tDebug("%s conn %p receive release request, refId:%" PRId64 ", ignore msg", CONN_GET_INST_LABEL(conn), conn,
               conn->refId);
        cliDestroyConn(conn, true);
        return true;
      }
    }

    cliConnFreeMsgs(conn);

    tDebug("%s conn %p receive release request, refId:%" PRId64 "", CONN_GET_INST_LABEL(conn), conn, conn->refId);
    destroyCmsg(pMsg);

    addConnToPool(((SCliThrd*)conn->hostThrd)->pool, conn);
    return true;
  }
  return false;
}

static void* cliWorkThread(void* arg) {
  char threadName[TSDB_LABEL_LEN] = {0};

  SCliThrd* pThrd = (SCliThrd*)arg;
  pThrd->pid = taosGetSelfPthreadId();

  (void)strtolower(threadName, pThrd->pTransInst->label);
  setThreadName(threadName);

  (void)uv_run(pThrd->loop, UV_RUN_DEFAULT);

  tDebug("thread quit-thread:%08" PRId64, pThrd->pid);
  return NULL;
}

void* transInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle) {
  int32_t  code = 0;
  SCliObj* cli = taosMemoryCalloc(1, sizeof(SCliObj));
  if (cli == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _err);
  }

  STrans* pTransInst = shandle;
  memcpy(cli->label, label, TSDB_LABEL_LEN);
  cli->numOfThreads = numOfThreads;

  cli->pThreadObj = (SCliThrd**)taosMemoryCalloc(cli->numOfThreads, sizeof(SCliThrd*));
  if (cli->pThreadObj == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _err);
  }

  for (int i = 0; i < cli->numOfThreads; i++) {
    SCliThrd* pThrd = NULL;
    code = createThrdObj(shandle, &pThrd);
    if (code != 0) {
      goto _err;
    }

    int err = taosThreadCreate(&pThrd->thread, NULL, cliWorkThread, (void*)(pThrd));
    if (err != 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      TAOS_CHECK_GOTO(code, NULL, _err);
    } else {
      tDebug("success to create tranport-cli thread:%d", i);
    }
    cli->pThreadObj[i] = pThrd;
  }
  return cli;

_err:
  if (cli) {
    for (int i = 0; i < cli->numOfThreads; i++) {
      if (cli->pThreadObj[i]) {
        (void)cliSendQuit(cli->pThreadObj[i]);
        destroyThrdObj(cli->pThreadObj[i]);
      }
    }
    taosMemoryFree(cli->pThreadObj);
    taosMemoryFree(cli);
  }
  terrno = code;
  return NULL;
}

static FORCE_INLINE void destroyCmsg(void* arg) {
  SCliMsg* pMsg = arg;
  if (pMsg == NULL) {
    return;
  }
  tDebug("free memory:%p, free ctx: %p", pMsg, pMsg->ctx);

  transDestroyConnCtx(pMsg->ctx);
  transFreeMsg(pMsg->msg.pCont);
  taosMemoryFree(pMsg);
}
static FORCE_INLINE void destroyCmsgWrapper(void* arg, void* param) {
  if (arg == NULL) return;

  SCliMsg*  pMsg = arg;
  SCliThrd* pThrd = param;
  if (pMsg->msg.info.notFreeAhandle == 0 && pThrd != NULL) {
    if (pThrd->destroyAhandleFp) (*pThrd->destroyAhandleFp)(pMsg->msg.info.ahandle);
  }
  destroyCmsg(pMsg);
}
static FORCE_INLINE void destroyCmsgAndAhandle(void* param) {
  if (param == NULL) return;

  STaskArg* arg = param;
  SCliMsg*  pMsg = arg->param1;
  SCliThrd* pThrd = arg->param2;

  if (pMsg->msg.info.notFreeAhandle == 0 && pThrd != NULL && pThrd->destroyAhandleFp != NULL) {
    pThrd->destroyAhandleFp(pMsg->ctx->ahandle);
  }

  if (pMsg->msg.info.handle != 0) {
    (void)transReleaseExHandle(transGetRefMgt(), (int64_t)pMsg->msg.info.handle);
    (void)transRemoveExHandle(transGetRefMgt(), (int64_t)pMsg->msg.info.handle);
  }

  transDestroyConnCtx(pMsg->ctx);
  transFreeMsg(pMsg->msg.pCont);
  taosMemoryFree(pMsg);
}

static int32_t createThrdObj(void* trans, SCliThrd** ppThrd) {
  int32_t code = 0;
  STrans* pTransInst = trans;

  SCliThrd* pThrd = (SCliThrd*)taosMemoryCalloc(1, sizeof(SCliThrd));
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }

  QUEUE_INIT(&pThrd->msg);
  (void)taosThreadMutexInit(&pThrd->msgMtx, NULL);

  pThrd->loop = (uv_loop_t*)taosMemoryMalloc(sizeof(uv_loop_t));
  if (pThrd->loop == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }

  code = uv_loop_init(pThrd->loop);
  if (code != 0) {
    tError("failed to init uv_loop, reason:%s", uv_err_name(code));
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, NULL, _end);
  }

  int32_t nSync = pTransInst->supportBatch ? 4 : 8;
  code = transAsyncPoolCreate(pThrd->loop, nSync, pThrd, cliAsyncCb, &pThrd->asyncPool);
  if (code != 0) {
    tError("failed to init async pool since:%s", tstrerror(code));
    TAOS_CHECK_GOTO(code, NULL, _end);
  }

  pThrd->pool = createConnPool(4);
  if (pThrd->pool == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }
  if ((code = transDQCreate(pThrd->loop, &pThrd->delayQueue)) != 0) {
    TAOS_CHECK_GOTO(code, NULL, _end);
  }

  if ((code = transDQCreate(pThrd->loop, &pThrd->timeoutQueue)) != 0) {
    TAOS_CHECK_GOTO(code, NULL, _end);
  }

  if ((code = transDQCreate(pThrd->loop, &pThrd->waitConnQueue)) != 0) {
    TAOS_CHECK_GOTO(code, NULL, _end);
  }

  pThrd->destroyAhandleFp = pTransInst->destroyFp;
  pThrd->fqdn2ipCache = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pThrd->fqdn2ipCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }
  pThrd->failFastCache = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pThrd->failFastCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }

  pThrd->batchCache = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pThrd->batchCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }

  int32_t timerSize = 64;
  pThrd->timerList = taosArrayInit(timerSize, sizeof(void*));
  if (pThrd->timerList == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }

  for (int i = 0; i < timerSize; i++) {
    uv_timer_t* timer = taosMemoryCalloc(1, sizeof(uv_timer_t));
    if (timer == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
    }
    (void)uv_timer_init(pThrd->loop, timer);
    if (taosArrayPush(pThrd->timerList, &timer) == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
    }
  }
  pThrd->nextTimeout = taosGetTimestampMs() + CONN_PERSIST_TIME(pTransInst->idleTime);
  pThrd->pTransInst = trans;
  pThrd->quit = false;

  *ppThrd = pThrd;
  return code;

_end:
  if (pThrd) {
    (void)taosThreadMutexDestroy(&pThrd->msgMtx);

    (void)uv_loop_close(pThrd->loop);
    taosMemoryFree(pThrd->loop);
    transAsyncPoolDestroy(pThrd->asyncPool);
    for (int i = 0; i < taosArrayGetSize(pThrd->timerList); i++) {
      uv_timer_t* timer = taosArrayGetP(pThrd->timerList, i);
      (void)uv_timer_stop(timer);
      taosMemoryFree(timer);
    }
    taosArrayDestroy(pThrd->timerList);

    (void)destroyConnPool(pThrd);
    transDQDestroy(pThrd->delayQueue, NULL);
    transDQDestroy(pThrd->timeoutQueue, NULL);
    transDQDestroy(pThrd->waitConnQueue, NULL);
    taosHashCleanup(pThrd->fqdn2ipCache);
    taosHashCleanup(pThrd->failFastCache);
    taosHashCleanup(pThrd->batchCache);

    taosMemoryFree(pThrd);
  }
  return code;
}
static void destroyThrdObj(SCliThrd* pThrd) {
  if (pThrd == NULL) {
    return;
  }

  (void)taosThreadJoin(pThrd->thread, NULL);
  CLI_RELEASE_UV(pThrd->loop);
  (void)taosThreadMutexDestroy(&pThrd->msgMtx);
  TRANS_DESTROY_ASYNC_POOL_MSG(pThrd->asyncPool, SCliMsg, destroyCmsgWrapper, (void*)pThrd);
  transAsyncPoolDestroy(pThrd->asyncPool);

  transDQDestroy(pThrd->delayQueue, destroyCmsgAndAhandle);
  transDQDestroy(pThrd->timeoutQueue, NULL);
  transDQDestroy(pThrd->waitConnQueue, NULL);

  tDebug("thread destroy %" PRId64, pThrd->pid);
  for (int i = 0; i < taosArrayGetSize(pThrd->timerList); i++) {
    uv_timer_t* timer = taosArrayGetP(pThrd->timerList, i);
    (void)uv_timer_stop(timer);
    taosMemoryFree(timer);
  }

  taosArrayDestroy(pThrd->timerList);
  taosMemoryFree(pThrd->loop);
  taosHashCleanup(pThrd->fqdn2ipCache);
  taosHashCleanup(pThrd->failFastCache);

  void** pIter = taosHashIterate(pThrd->batchCache, NULL);
  while (pIter != NULL) {
    SCliBatchList* pBatchList = (SCliBatchList*)(*pIter);
    while (!QUEUE_IS_EMPTY(&pBatchList->wq)) {
      queue* h = QUEUE_HEAD(&pBatchList->wq);
      QUEUE_REMOVE(h);

      SCliBatch* pBatch = QUEUE_DATA(h, SCliBatch, listq);
      cliDestroyBatch(pBatch);
    }
    taosMemoryFree(pBatchList->ip);
    taosMemoryFree(pBatchList->dst);
    taosMemoryFree(pBatchList);

    pIter = (void**)taosHashIterate(pThrd->batchCache, pIter);
  }
  taosHashCleanup(pThrd->batchCache);
  taosMemoryFree(pThrd);
}

static FORCE_INLINE void transDestroyConnCtx(STransConnCtx* ctx) {
  //
  taosMemoryFree(ctx);
}

int32_t cliSendQuit(SCliThrd* thrd) {
  // cli can stop gracefully
  int32_t  code = 0;
  SCliMsg* msg = taosMemoryCalloc(1, sizeof(SCliMsg));
  if (msg == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  msg->type = Quit;
  if ((code = transAsyncSend(thrd->asyncPool, &msg->q)) != 0) {
    code = (code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code);
    taosMemoryFree(msg);
    return code;
  }

  atomic_store_8(&thrd->asyncPool->stop, 1);
  return 0;
}
void cliWalkCb(uv_handle_t* handle, void* arg) {
  if (!uv_is_closing(handle)) {
    if (uv_handle_get_type(handle) == UV_TIMER) {
      // do nothing
    } else {
      (void)uv_read_stop((uv_stream_t*)handle);
    }
    (void)uv_close(handle, cliDestroy);
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
static void cliSchedMsgToDebug(SCliMsg* pMsg, char* label) {
  if (!(rpcDebugFlag & DEBUG_DEBUG)) {
    return;
  }
  STransConnCtx* pCtx = pMsg->ctx;
  STraceId*      trace = &pMsg->msg.info.traceId;
  char           tbuf[512] = {0};
  (void)epsetToStr(&pCtx->epSet, tbuf, tListLen(tbuf));
  tGDebug("%s retry on next node,use:%s, step: %d,timeout:%" PRId64 "", label, tbuf, pCtx->retryStep,
          pCtx->retryNextInterval);
  return;
}

static void cliSchedMsgToNextNode(SCliMsg* pMsg, SCliThrd* pThrd) {
  STrans*        pTransInst = pThrd->pTransInst;
  STransConnCtx* pCtx = pMsg->ctx;
  cliSchedMsgToDebug(pMsg, transLabel(pThrd->pTransInst));

  STaskArg* arg = taosMemoryMalloc(sizeof(STaskArg));
  arg->param1 = pMsg;
  arg->param2 = pThrd;

  (void)transDQSched(pThrd->delayQueue, doDelayTask, arg, pCtx->retryNextInterval);
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

  epsetAssign(dst, &epset);
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
        if (!transEpSetIsEqual2(&pCtx->epSet, &epSet)) {
          tDebug("epset not equal, retry new epset1");
          transPrintEpSet(&pCtx->epSet);
          transPrintEpSet(&epSet);
          epsetAssign(&pCtx->epSet, &epSet);
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
      if (!transEpSetIsEqual2(&pCtx->epSet, &epSet)) {
        tDebug("epset not equal, retry new epset2");
        transPrintEpSet(&pCtx->epSet);
        transPrintEpSet(&epSet);
        epsetAssign(&pCtx->epSet, &epSet);
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
    pCtx->retryMaxTimeout = pTransInst->retryMaxTimeout;
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
    if (rpcDebugFlag & DEBUG_TRACE) {
      char tbuf[512] = {0};
      (void)epsetToStr(&pCtx->epSet, tbuf, tListLen(tbuf));
      tGTrace("%s conn %p extract epset from msg", CONN_GET_INST_LABEL(pConn), pConn);
    }
  }
  if (pCtx->pSem || pCtx->syncMsgRef != 0) {
    tGTrace("%s conn %p(sync) handle resp", CONN_GET_INST_LABEL(pConn), pConn);
    if (pCtx->pSem) {
      if (pCtx->pRsp == NULL) {
        tGTrace("%s conn %p(sync) failed to resp, ignore", CONN_GET_INST_LABEL(pConn), pConn);
      } else {
        memcpy((char*)pCtx->pRsp, (char*)pResp, sizeof(*pResp));
      }
      (void)tsem_post(pCtx->pSem);
      pCtx->pRsp = NULL;
    } else {
      STransSyncMsg* pSyncMsg = taosAcquireRef(transGetSyncMsgMgt(), pCtx->syncMsgRef);
      if (pSyncMsg != NULL) {
        memcpy(pSyncMsg->pRsp, (char*)pResp, sizeof(*pResp));
        if (cliIsEpsetUpdated(pResp->code, pCtx)) {
          pSyncMsg->hasEpSet = 1;
          epsetAssign(&pSyncMsg->epSet, &pCtx->epSet);
        }
        (void)tsem2_post(pSyncMsg->pSem);
        (void)taosReleaseRef(transGetSyncMsgMgt(), pCtx->syncMsgRef);
      } else {
        rpcFreeCont(pResp->pCont);
      }
    }
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
  int32_t  code = 0;
  SCliObj* cli = arg;
  for (int i = 0; i < cli->numOfThreads; i++) {
    code = cliSendQuit(cli->pThreadObj[i]);
    if (code != 0) {
      tError("failed to send quit to thread:%d, reason:%s", i, tstrerror(code));
    }

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
  } else {
    tDebug("conn %p got", exh->handle);
  }
  taosWLockLatch(&exh->latch);
  if (exh->pThrd == NULL && trans != NULL) {
    int idx = cliRBChoseIdx(trans);
    if (idx < 0) return NULL;
    exh->pThrd = ((SCliObj*)trans->tcphandle)->pThreadObj[idx];
  }

  pThrd = exh->pThrd;
  taosWUnLockLatch(&exh->latch);
  (void)transReleaseExHandle(transGetRefMgt(), handle);

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
int32_t transReleaseCliHandle(void* handle) {
  int32_t   code = 0;
  SCliThrd* pThrd = transGetWorkThrdFromHandle(NULL, (int64_t)handle);
  if (pThrd == NULL) {
    return TSDB_CODE_RPC_BROKEN_LINK;
  }

  STransMsg tmsg = {.info.handle = handle, .info.ahandle = (void*)0x9527};
  TRACE_SET_MSGID(&tmsg.info.traceId, tGenIdPI64());

  STransConnCtx* pCtx = taosMemoryCalloc(1, sizeof(STransConnCtx));
  if (pCtx == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pCtx->ahandle = tmsg.info.ahandle;

  SCliMsg* cmsg = taosMemoryCalloc(1, sizeof(SCliMsg));
  if (cmsg == NULL) {
    taosMemoryFree(pCtx);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  cmsg->msg = tmsg;
  cmsg->st = taosGetTimestampUs();
  cmsg->type = Release;
  cmsg->ctx = pCtx;

  STraceId* trace = &tmsg.info.traceId;
  tGDebug("send release request at thread:%08" PRId64 ", malloc memory:%p", pThrd->pid, cmsg);

  if ((code = transAsyncSend(pThrd->asyncPool, &cmsg->q)) != 0) {
    destroyCmsg(cmsg);
    return code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code;
  }
  return code;
}

static int32_t transInitMsg(void* shandle, const SEpSet* pEpSet, STransMsg* pReq, STransCtx* ctx, SCliMsg** pCliMsg) {
  if (pReq->info.traceId.msgId == 0) TRACE_SET_MSGID(&pReq->info.traceId, tGenIdPI64());
  STransConnCtx* pCtx = taosMemoryCalloc(1, sizeof(STransConnCtx));
  if (pCtx == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  epsetAssign(&pCtx->epSet, pEpSet);
  epsetAssign(&pCtx->origEpSet, pEpSet);

  pCtx->ahandle = pReq->info.ahandle;
  pCtx->msgType = pReq->msgType;

  if (ctx != NULL) pCtx->appCtx = *ctx;

  SCliMsg* cliMsg = taosMemoryCalloc(1, sizeof(SCliMsg));
  if (cliMsg == NULL) {
    taosMemoryFree(pCtx);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  cliMsg->ctx = pCtx;
  cliMsg->msg = *pReq;
  cliMsg->st = taosGetTimestampUs();
  cliMsg->type = Normal;
  cliMsg->refId = (int64_t)shandle;
  QUEUE_INIT(&cliMsg->seqq);

  *pCliMsg = cliMsg;

  return 0;
}

int32_t transSendRequest(void* shandle, const SEpSet* pEpSet, STransMsg* pReq, STransCtx* ctx) {
  STrans* pTransInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)shandle);
  if (pTransInst == NULL) {
    transFreeMsg(pReq->pCont);
    pReq->pCont = NULL;
    return TSDB_CODE_RPC_MODULE_QUIT;
  }
  int32_t   code = 0;
  int64_t   handle = (int64_t)pReq->info.handle;
  SCliThrd* pThrd = transGetWorkThrd(pTransInst, handle);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_BROKEN_LINK, NULL, _exception;);
  }

  if (handle != 0) {
    SExHandle* exh = transAcquireExHandle(transGetRefMgt(), handle);
    if (exh != NULL) {
      taosWLockLatch(&exh->latch);
      if (exh->handle == NULL && exh->inited != 0) {
        SCliMsg* pCliMsg = NULL;
        code = transInitMsg(shandle, pEpSet, pReq, ctx, &pCliMsg);
        if (code != 0) {
          taosWUnLockLatch(&exh->latch);
          (void)transReleaseExHandle(transGetRefMgt(), handle);
          TAOS_CHECK_GOTO(code, NULL, _exception);
        }

        QUEUE_PUSH(&exh->q, &pCliMsg->seqq);
        taosWUnLockLatch(&exh->latch);

        tDebug("msg refId: %" PRId64 "", handle);
        (void)transReleaseExHandle(transGetRefMgt(), handle);
        (void)transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
        return 0;
      } else {
        exh->inited = 1;
        taosWUnLockLatch(&exh->latch);
        (void)transReleaseExHandle(transGetRefMgt(), handle);
      }
    }
  }

  SCliMsg* pCliMsg = NULL;
  TAOS_CHECK_GOTO(transInitMsg(shandle, pEpSet, pReq, ctx, &pCliMsg), NULL, _exception);

  STraceId* trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pTransInst), pThrd->pid,
          EPSET_GET_INUSE_IP(pEpSet), EPSET_GET_INUSE_PORT(pEpSet), pReq->info.ahandle);
  if ((code = transAsyncSend(pThrd->asyncPool, &(pCliMsg->q))) != 0) {
    destroyCmsg(pCliMsg);
    (void)transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
    return (code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code);
  }
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  return 0;

_exception:
  transFreeMsg(pReq->pCont);
  pReq->pCont = NULL;
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  return code;
}
int32_t transSendRequestWithId(void* shandle, const SEpSet* pEpSet, STransMsg* pReq, int64_t* transpointId) {
  if (transpointId == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;

  STrans* pTransInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)shandle);
  if (pTransInst == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_MODULE_QUIT, NULL, _exception);
  }

  TAOS_CHECK_GOTO(transAllocHandle(transpointId), NULL, _exception);

  SCliThrd* pThrd = transGetWorkThrd(pTransInst, *transpointId);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_BROKEN_LINK, NULL, _exception);
  }

  SExHandle* exh = transAcquireExHandle(transGetRefMgt(), *transpointId);
  if (exh == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_MODULE_QUIT, NULL, _exception);
  }

  pReq->info.handle = (void*)(*transpointId);

  SCliMsg* pCliMsg = NULL;
  TAOS_CHECK_GOTO(transInitMsg(shandle, pEpSet, pReq, NULL, &pCliMsg), NULL, _exception);

  STraceId* trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pTransInst), pThrd->pid,
          EPSET_GET_INUSE_IP(pEpSet), EPSET_GET_INUSE_PORT(pEpSet), pReq->info.ahandle);
  if ((code = transAsyncSend(pThrd->asyncPool, &(pCliMsg->q))) != 0) {
    destroyCmsg(pCliMsg);
    (void)transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
    return (code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code);
  }
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  return 0;

_exception:
  transFreeMsg(pReq->pCont);
  pReq->pCont = NULL;
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  return code;
}

int32_t transSendRecv(void* shandle, const SEpSet* pEpSet, STransMsg* pReq, STransMsg* pRsp) {
  STrans* pTransInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)shandle);
  if (pTransInst == NULL) {
    transFreeMsg(pReq->pCont);
    pReq->pCont = NULL;
    return TSDB_CODE_RPC_MODULE_QUIT;
  }
  int32_t code = 0;

  STransMsg* pTransRsp = taosMemoryCalloc(1, sizeof(STransMsg));
  if (pTransRsp == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _RETURN1);
  }

  SCliThrd* pThrd = transGetWorkThrd(pTransInst, (int64_t)pReq->info.handle);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_BROKEN_LINK, NULL, _RETURN1);
  }

  tsem_t* sem = taosMemoryCalloc(1, sizeof(tsem_t));
  if (sem == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _RETURN1);
  }

  code = tsem_init(sem, 0, 0);
  if (code != 0) {
    taosMemoryFree(sem);
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), NULL, _RETURN1);
  }

  if (pReq->info.traceId.msgId == 0) TRACE_SET_MSGID(&pReq->info.traceId, tGenIdPI64());

  STransConnCtx* pCtx = taosMemoryCalloc(1, sizeof(STransConnCtx));
  if (pCtx == NULL) {
    (void)tsem_destroy(sem);
    taosMemoryFree(sem);
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _RETURN1);
  }

  epsetAssign(&pCtx->epSet, pEpSet);
  epsetAssign(&pCtx->origEpSet, pEpSet);
  pCtx->ahandle = pReq->info.ahandle;
  pCtx->msgType = pReq->msgType;
  pCtx->pSem = sem;
  pCtx->pRsp = pTransRsp;

  SCliMsg* cliMsg = taosMemoryCalloc(1, sizeof(SCliMsg));
  if (cliMsg == NULL) {
    (void)tsem_destroy(sem);
    taosMemoryFree(sem);
    taosMemoryFree(pCtx);
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _RETURN1);
  }

  cliMsg->ctx = pCtx;
  cliMsg->msg = *pReq;
  cliMsg->st = taosGetTimestampUs();
  cliMsg->type = Normal;
  cliMsg->refId = (int64_t)shandle;

  STraceId* trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pTransInst), pThrd->pid,
          EPSET_GET_INUSE_IP(&pCtx->epSet), EPSET_GET_INUSE_PORT(&pCtx->epSet), pReq->info.ahandle);

  code = transAsyncSend(pThrd->asyncPool, &cliMsg->q);
  if (code != 0) {
    destroyCmsg(cliMsg);
    TAOS_CHECK_GOTO((code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code), NULL, _RETURN);
  }
  (void)tsem_wait(sem);

  memcpy(pRsp, pTransRsp, sizeof(STransMsg));

_RETURN:
  tsem_destroy(sem);
  taosMemoryFree(sem);
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  taosMemoryFree(pTransRsp);
  return code;
_RETURN1:
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  taosMemoryFree(pTransRsp);
  taosMemoryFree(pReq->pCont);
  pReq->pCont = NULL;
  return code;
}
int32_t transCreateSyncMsg(STransMsg* pTransMsg, int64_t* refId) {
  int32_t  code = 0;
  tsem2_t* sem = taosMemoryCalloc(1, sizeof(tsem2_t));
  if (sem == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (tsem2_init(sem, 0, 0) != 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), NULL, _EXIT);
  }

  STransSyncMsg* pSyncMsg = taosMemoryCalloc(1, sizeof(STransSyncMsg));
  if (pSyncMsg == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _EXIT);
  }

  taosInitRWLatch(&pSyncMsg->latch);
  pSyncMsg->inited = 0;
  pSyncMsg->pRsp = pTransMsg;
  pSyncMsg->pSem = sem;
  pSyncMsg->hasEpSet = 0;

  int64_t id = taosAddRef(transGetSyncMsgMgt(), pSyncMsg);
  if (id < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, NULL, _EXIT);
  } else {
    *refId = id;
  }
  return 0;

_EXIT:
  (void)tsem2_destroy(sem);
  taosMemoryFree(sem);
  taosMemoryFree(pSyncMsg);
  return code;
}
int32_t transSendRecvWithTimeout(void* shandle, SEpSet* pEpSet, STransMsg* pReq, STransMsg* pRsp, int8_t* epUpdated,
                                 int32_t timeoutMs) {
  int32_t code = 0;
  STrans* pTransInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)shandle);
  if (pTransInst == NULL) {
    transFreeMsg(pReq->pCont);
    pReq->pCont = NULL;
    return TSDB_CODE_RPC_MODULE_QUIT;
  }

  STransMsg* pTransMsg = taosMemoryCalloc(1, sizeof(STransMsg));
  if (pTransMsg == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _RETURN2);
  }

  SCliThrd* pThrd = transGetWorkThrd(pTransInst, (int64_t)pReq->info.handle);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_BROKEN_LINK, NULL, _RETURN2);
  }

  if (pReq->info.traceId.msgId == 0) TRACE_SET_MSGID(&pReq->info.traceId, tGenIdPI64());

  STransConnCtx* pCtx = taosMemoryCalloc(1, sizeof(STransConnCtx));
  if (pCtx == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _RETURN2);
  }

  epsetAssign(&pCtx->epSet, pEpSet);
  epsetAssign(&pCtx->origEpSet, pEpSet);
  pCtx->ahandle = pReq->info.ahandle;
  pCtx->msgType = pReq->msgType;

  if ((code = transCreateSyncMsg(pTransMsg, &pCtx->syncMsgRef)) != 0) {
    taosMemoryFree(pCtx);
    TAOS_CHECK_GOTO(code, NULL, _RETURN2);
  }

  int64_t        ref = pCtx->syncMsgRef;
  STransSyncMsg* pSyncMsg = taosAcquireRef(transGetSyncMsgMgt(), ref);
  if (pSyncMsg == NULL) {
    taosMemoryFree(pCtx);
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, NULL, _RETURN2);
  }

  SCliMsg* cliMsg = taosMemoryCalloc(1, sizeof(SCliMsg));
  if (cliMsg == NULL) {
    taosMemoryFree(pCtx);
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _RETURN2);
  }

  cliMsg->ctx = pCtx;
  cliMsg->msg = *pReq;
  cliMsg->st = taosGetTimestampUs();
  cliMsg->type = Normal;
  cliMsg->refId = (int64_t)shandle;

  STraceId* trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pTransInst), pThrd->pid,
          EPSET_GET_INUSE_IP(&pCtx->epSet), EPSET_GET_INUSE_PORT(&pCtx->epSet), pReq->info.ahandle);

  code = transAsyncSend(pThrd->asyncPool, &cliMsg->q);
  if (code != 0) {
    destroyCmsg(cliMsg);
    TAOS_CHECK_GOTO(code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code, NULL, _RETURN);
    goto _RETURN;
  }

  code = tsem2_timewait(pSyncMsg->pSem, timeoutMs);
  if (code < 0) {
    pRsp->code = TSDB_CODE_TIMEOUT_ERROR;
    code = TSDB_CODE_TIMEOUT_ERROR;
  } else {
    memcpy(pRsp, pSyncMsg->pRsp, sizeof(STransMsg));
    pSyncMsg->pRsp->pCont = NULL;
    if (pSyncMsg->hasEpSet == 1) {
      epsetAssign(pEpSet, &pSyncMsg->epSet);
      *epUpdated = 1;
    }
    code = 0;
  }
_RETURN:
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  (void)taosReleaseRef(transGetSyncMsgMgt(), ref);
  (void)taosRemoveRef(transGetSyncMsgMgt(), ref);
  return code;
_RETURN2:
  transFreeMsg(pReq->pCont);
  pReq->pCont = NULL;
  taosMemoryFree(pTransMsg);
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  return code;
}
/*
 *
 **/
int32_t transSetDefaultAddr(void* shandle, const char* ip, const char* fqdn) {
  if (ip == NULL || fqdn == NULL) return TSDB_CODE_INVALID_PARA;

  STrans* pTransInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)shandle);
  if (pTransInst == NULL) {
    return TSDB_CODE_RPC_MODULE_QUIT;
  }

  SCvtAddr cvtAddr = {0};
  tstrncpy(cvtAddr.ip, ip, sizeof(cvtAddr.ip));
  tstrncpy(cvtAddr.fqdn, fqdn, sizeof(cvtAddr.fqdn));
  cvtAddr.cvt = true;

  int32_t code = 0;
  for (int8_t i = 0; i < pTransInst->numOfThreads; i++) {
    STransConnCtx* pCtx = taosMemoryCalloc(1, sizeof(STransConnCtx));
    if (pCtx == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      break;
    }

    pCtx->cvtAddr = cvtAddr;

    SCliMsg* cliMsg = taosMemoryCalloc(1, sizeof(SCliMsg));
    if (cliMsg == NULL) {
      taosMemoryFree(pCtx);
      code = TSDB_CODE_OUT_OF_MEMORY;
      break;
    }

    cliMsg->ctx = pCtx;
    cliMsg->type = Update;
    cliMsg->refId = (int64_t)shandle;

    SCliThrd* thrd = ((SCliObj*)pTransInst->tcphandle)->pThreadObj[i];
    tDebug("%s update epset at thread:%08" PRId64, pTransInst->label, thrd->pid);

    if ((code = transAsyncSend(thrd->asyncPool, &(cliMsg->q))) != 0) {
      destroyCmsg(cliMsg);
      if (code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT) {
        code = TSDB_CODE_RPC_MODULE_QUIT;
      }
      break;
    }
  }

  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  return code;
}

int32_t transAllocHandle(int64_t* refId) {
  SExHandle* exh = taosMemoryCalloc(1, sizeof(SExHandle));
  if (exh == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  exh->refId = transAddExHandle(transGetRefMgt(), exh);
  if (exh->refId < 0) {
    taosMemoryFree(exh);
    return TSDB_CODE_REF_INVALID_ID;
  }

  SExHandle* self = transAcquireExHandle(transGetRefMgt(), exh->refId);
  if (exh != self) {
    taosMemoryFree(exh);
    return TSDB_CODE_REF_INVALID_ID;
  }

  QUEUE_INIT(&exh->q);
  taosInitRWLatch(&exh->latch);
  tDebug("pre alloc refId %" PRId64 "", exh->refId);
  *refId = exh->refId;
  return 0;
}
int32_t transFreeConnById(void* shandle, int64_t transpointId) {
  int32_t code = 0;
  STrans* pTransInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)shandle);
  if (pTransInst == NULL) {
    return TSDB_CODE_RPC_MODULE_QUIT;
  }
  if (transpointId == 0) {
    tDebug("not free by refId:%" PRId64 "", transpointId);
    TAOS_CHECK_GOTO(0, NULL, _exception);
  }

  SCliThrd* pThrd = transGetWorkThrdFromHandle(pTransInst, transpointId);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, NULL, _exception);
  }

  SCliMsg* pCli = taosMemoryCalloc(1, sizeof(SCliMsg));
  if (pCli == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _exception);
  }
  pCli->type = FreeById;

  tDebug("release conn id %" PRId64 "", transpointId);

  STransMsg msg = {.info.handle = (void*)transpointId};
  pCli->msg = msg;

  code = transAsyncSend(pThrd->asyncPool, &pCli->q);
  if (code != 0) {
    taosMemoryFree(pCli);
    TAOS_CHECK_GOTO(code, NULL, _exception);
  }

_exception:
  transReleaseExHandle(transGetInstMgt(), (int64_t)shandle);
  return code;
}
