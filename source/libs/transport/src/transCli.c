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
#include "taoserror.h"
#include "transComm.h"
#include "tmisce.h"
#include "transLog.h"
// clang-format on

typedef struct {
  int32_t numOfConn;
  queue   msgQ;
} SMsgList;

typedef struct SConnList {
  queue   conns;
  int32_t size;
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
  int32_t      ref;
  uv_connect_t connReq;
  uv_stream_t* stream;

  uv_timer_t* timer;  // read timer, forbidden

  void* hostThrd;

  SConnBuffer readBuf;
  STransQueue reqsToSend;
  STransQueue reqsSentOut;
  SHashObj*   pQueryTable;

  queue      q;
  SConnList* list;

  STransCtx  ctx;
  bool       broken;  // link broken or not
  ConnStatus status;  //

  SCliBatch* pBatch;

  SDelayTask* task;

  HeapNode node;  // for heap
  int8_t   inHeap;
  int32_t  reqRefCnt;
  uint32_t clientIp;
  uint32_t serverIp;

  char* dstAddr;
  char  src[32];
  char  dst[32];

  char*   ipStr;
  int32_t port;

  int64_t refId;
  int64_t seq;

  int8_t    registered;
  int8_t    connnected;
  SHashObj* pQTable;
  int8_t    userInited;
  void*     pInitUserReq;

  void*   heap;  // point to req conn heap
  int32_t heapMissHit;

  uv_buf_t* buf;
  int32_t   bufSize;

  queue wq;  // uv_write_t queue
} SCliConn;

// #define TRANS_CONN_REF_INC(tconn) ((tconn) ? (tconn)->ref++ : 0)
// #define TRANS_CONN_REF_DEC(tconn) ((tconn) ? (tconn)->ref-- : 0)
// #define TRANS_CONN_REF_GET(tconn) ((tconn) ? (tconn)->ref : 0)

typedef struct {
  SCliConn* conn;
  void*     arg;
} SReqState;

typedef struct SCliReq {
  SReqCtx*      ctx;
  queue         q;
  STransMsgType type;
  uint64_t      st;
  int64_t       seq;
  int32_t       sent;  //(0: no send, 1: alread sent)
  STransMsg     msg;

} SCliReq;

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
  STrans*       pInst;        //

  void (*destroyAhandleFp)(void* ahandle);
  SHashObj* fqdn2ipCache;
  SCvtAddr* pCvtAddr;

  SHashObj* failFastCache;
  SHashObj* batchCache;
  SHashObj* connHeapCache;

  SCliReq* stopMsg;
  bool     quit;

  int32_t (*initCb)(void* arg, SCliReq* pReq, STransMsg* pResp);
  int32_t (*notifyCb)(void* arg, SCliReq* pReq, STransMsg* pResp);
  int32_t (*notifyExceptCb)(void* arg, SCliReq* pReq, STransMsg* pResp);

  SHashObj* pIdConnTable;  // <qid, conn>
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
static void*   createConnPool(int size);
static void*   destroyConnPool(SCliThrd* thread);
static void    addConnToPool(void* pool, SCliConn* conn);
static void    doCloseIdleConn(void* param);
static int32_t cliCreateConn2(SCliThrd* pThrd, SCliReq* pReq, SCliConn** pConn);
static int32_t cliCreateConn(SCliThrd* pThrd, SCliConn** pCliConn, char* ip, int port);
static int32_t cliDoConn(SCliThrd* pThrd, SCliConn* conn);
static void    cliBatchSendCb(uv_write_t* req, int status);
void           cliBatchSendImpl(SCliConn* pConn);
static int32_t cliBatchSend(SCliConn* conn);
void           cliConnCheckTimoutMsg(SCliConn* conn);
bool           cliConnRmReleaseReq(SCliConn* conn, STransMsgHead* pHead);
// register conn timer
static void cliConnTimeout(uv_timer_t* handle);

void cliConnTimeout__checkReq(uv_timer_t* handle);
// register timer for read
static void cliReadTimeoutCb(uv_timer_t* handle);
// register timer in each thread to clear expire conn
// static void cliTimeoutCb(uv_timer_t* handle);
// alloc buffer for recv
static FORCE_INLINE void cliAllocRecvBufferCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
// callback after recv nbytes from socket
static void cliRecvCb(uv_stream_t* cli, ssize_t nread, const uv_buf_t* buf);
// callback after send data to socket
// static void cliSendCb(uv_write_t* req, int status);
// callback after conn to server
static void cliConnCb(uv_connect_t* req, int status);
static void cliAsyncCb(uv_async_t* handle);

SCliBatch* cliGetHeadFromList(SCliBatchList* pList);

static void destroyCliConnQTable(SCliConn* conn);

static void    cliHandleException(SCliConn* conn);
static int32_t allocConnRef(SCliConn* conn, bool update);

static int cliNotifyCb(SCliConn* pConn, SCliReq* pReq, STransMsg* pResp);
void       cliResetConnTimer(SCliConn* conn);

static void cliDestroyConn(SCliConn* pConn, bool clear /*clear tcp handle or not*/);
static void cliDestroy(uv_handle_t* handle);

static void cliDestroyConnMsgs(SCliConn* conn, bool destroy);

static void doFreeTimeoutMsg(void* param);

static void cliDestroyBatch(SCliBatch* pBatch);
// cli util func
static FORCE_INLINE bool    cliIsEpsetUpdated(int32_t code, SReqCtx* pCtx);
static FORCE_INLINE int32_t cliMayCvtFqdnToIp(SEpSet* pEpSet, const SCvtAddr* pCvtAddr);

static FORCE_INLINE int32_t cliBuildExceptResp(SCliThrd* thrd, SCliReq* pReq, STransMsg* resp);

static FORCE_INLINE int32_t cliGetIpFromFqdnCache(SHashObj* cache, char* fqdn, uint32_t* ipaddr);
static FORCE_INLINE int32_t cliUpdateFqdnCache(SHashObj* cache, char* fqdn);

static FORCE_INLINE void cliMayUpdateFqdnCache(SHashObj* cache, char* dst);
// process data read from server, add decompress etc later
// handle except about conn

static void doNotifyCb(SCliReq* pReq, SCliThrd* pThrd, int32_t code);
// handle req from app
static void cliHandleReq(SCliThrd* pThrd, SCliReq* pReq);
static void cliHandleQuit(SCliThrd* pThrd, SCliReq* pReq);
static void cliHandleRelease(SCliThrd* pThrd, SCliReq* pReq);
static void cliHandleUpdate(SCliThrd* pThrd, SCliReq* pReq);
static void cliHandleFreeById(SCliThrd* pThrd, SCliReq* pReq) { return; }

static void cliDoReq(queue* h, SCliThrd* pThrd);
static void cliDoBatchReq(queue* h, SCliThrd* pThrd);
static void (*cliDealFunc[])(queue* h, SCliThrd* pThrd) = {cliDoReq, cliDoBatchReq};

static void (*cliAsyncHandle[])(SCliThrd* pThrd, SCliReq* pReq) = {cliHandleReq, cliHandleQuit,   cliHandleRelease,
                                                                   NULL,         cliHandleUpdate, cliHandleFreeById};

static FORCE_INLINE void destroyReq(void* cmsg);

static FORCE_INLINE void destroyReqWrapper(void* arg, void* param);
static FORCE_INLINE void destroyReqAndAhanlde(void* cmsg);
static FORCE_INLINE int  cliRBChoseIdx(STrans* pInst);
static FORCE_INLINE void destroyReqCtx(SReqCtx* ctx);

int32_t cliHandleState_mayUpdateState(SCliThrd* pThrd, SCliReq* pReq, SCliConn* pConn);
int32_t cliMayGetStateByQid(SCliThrd* pThrd, SCliReq* pReq, SCliConn** pConn);

static SCliConn* getConnFromHeapCache(SHashObj* pConnHeapCache, char* key);
static int32_t   addConnToHeapCache(SHashObj* pConnHeapCacahe, SCliConn* pConn);
static int32_t   delConnFromHeapCache(SHashObj* pConnHeapCache, SCliConn* pConn);

// thread obj
static int32_t createThrdObj(void* trans, SCliThrd** pThrd);
static void    destroyThrdObj(SCliThrd* pThrd);

int32_t     cliSendQuit(SCliThrd* thrd);
static void cliWalkCb(uv_handle_t* handle, void* arg);

static void cliWalkCb(uv_handle_t* handle, void* arg);

static FORCE_INLINE int32_t destroyAllReqs(SCliConn* SCliConn);
static FORCE_INLINE bool    filterAllReq(void* e, void* arg);
typedef struct {
  void*    p;
  HeapNode node;
} SHeapNode;
typedef struct {
  // void*    p;
  Heap* heap;
  int32_t (*cmpFunc)(const HeapNode* a, const HeapNode* b);
} SHeap;

int32_t compareHeapNode(const HeapNode* a, const HeapNode* b);
int32_t transHeapInit(SHeap* heap, int32_t (*cmpFunc)(const HeapNode* a, const HeapNode* b));
void    transHeapDestroy(SHeap* heap);
int32_t transHeapGet(SHeap* heap, SCliConn** p);
int32_t transHeapInsert(SHeap* heap, SCliConn* p);
int32_t transHeapDelete(SHeap* heap, SCliConn* p);

#define CLI_RELEASE_UV(loop)            \
  do {                                  \
    uv_walk(loop, cliWalkCb, NULL);     \
    (void)uv_run(loop, UV_RUN_DEFAULT); \
    (void)uv_loop_close(loop);          \
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
#define CONN_GET_INST_LABEL(conn) (((STrans*)(((SCliThrd*)(conn)->hostThrd)->pInst))->label)

#define REQUEST_NO_RESP(msg) ((msg)->info.noResp == 1)

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

int32_t cliGetConnTimer(SCliThrd* pThrd, SCliConn* pConn) {
  uv_timer_t* timer = taosArrayGetSize(pThrd->timerList) > 0 ? *(uv_timer_t**)taosArrayPop(pThrd->timerList) : NULL;
  if (timer == NULL) {
    timer = taosMemoryCalloc(1, sizeof(uv_timer_t));
    if (timer == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    tDebug("no available timer, create a timer %p", timer);
    (void)uv_timer_init(pThrd->loop, timer);
  }
  timer->data = pConn;
  pConn->timer = timer;
  return 0;
}
void cliResetConnTimer(SCliConn* conn) {
  SCliThrd* pThrd = conn->hostThrd;
  if (conn->timer) {
    if (uv_is_active((uv_handle_t*)conn->timer)) {
      tDebug("%s conn %p stop timer", CONN_GET_INST_LABEL(conn), conn);
      (void)uv_timer_stop(conn->timer);
    }
    (void)taosArrayPush(pThrd->timerList, &conn->timer);
    conn->timer->data = NULL;
    conn->timer = NULL;
  }
}

void cliConnMayUpdateTimer(SCliConn* conn, int timeout) {
  SCliThrd* pThrd = conn->hostThrd;
  STrans*   pInst = pThrd->pInst;
  if (pInst->startReadTimer == 0) {
    return;
  }
  // reset previous timer
  if (conn->timer != NULL) {
    // reset previous timer
    cliResetConnTimer(conn);
  }
  int32_t reqsSentNum = transQueueSize(&conn->reqsSentOut);
  if (reqsSentNum == 0) {
    // no need to set timer
    return;
  }

  // start a new timer
  if (cliGetConnTimer(conn->hostThrd, conn) != 0) {
    return;
  }
  (void)uv_timer_start(conn->timer, cliConnTimeout__checkReq, timeout, 0);
}

void cliHandleBatchResp(SCliConn* conn) { return; }

void destroyCliConnQTable(SCliConn* conn) {
  void* pIter = taosHashIterate(conn->pQTable, NULL);
  while (pIter != NULL) {
    int64_t*   qid = taosHashGetKey(pIter, NULL);
    STransCtx* ctx = pIter;
    transCtxCleanup(ctx);
    pIter = taosHashIterate(conn->pQTable, pIter);

    (void)transReleaseExHandle(transGetRefMgt(), *qid);
    (void)transRemoveExHandle(transGetRefMgt(), *qid);
  }
  taosHashCleanup(conn->pQTable);
  conn->pQTable = NULL;
}

typedef struct {
  int64_t seq;
  int32_t msgType;
} SFiterArg;

bool filteBySeq(void* key, void* arg) {
  SFiterArg* targ = arg;
  SCliReq*   pReq = QUEUE_DATA(key, SCliReq, q);
  if (pReq->seq == targ->seq && pReq->msg.msgType + 1 == targ->msgType) {
    return true;
  } else {
    return false;
  }
}
int32_t cliGetReqBySeq(SCliConn* conn, int64_t seq, int32_t msgType, SCliReq** pReq) {
  int32_t code = 0;
  queue   set;
  QUEUE_INIT(&set)

  SFiterArg arg = {.seq = seq, .msgType = msgType};
  transQueueRemoveByFilter(&conn->reqsSentOut, filteBySeq, &arg, &set, 1);

  if (QUEUE_IS_EMPTY(&set)) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  queue*   e = QUEUE_HEAD(&set);
  SCliReq* p = QUEUE_DATA(e, SCliReq, q);

  *pReq = p;
  return 0;
}

int8_t cliMayRecycleConn(SCliConn* conn) {
  SCliThrd* pThrd = conn->hostThrd;
  if (transQueueSize(&conn->reqsToSend) == 0 && transQueueSize(&conn->reqsSentOut) == 0 &&
      taosHashGetSize(conn->pQTable) == 0) {
    (void)delConnFromHeapCache(pThrd->connHeapCache, conn);
    addConnToPool(pThrd->pool, conn);
    return 1;
  }
  return 0;
}

bool filterByQid(void* key, void* arg) {
  int64_t* qid = arg;
  SCliReq* pReq = QUEUE_DATA(key, SCliReq, q);

  if (pReq->msg.info.qId == *qid) {
    return true;
  } else {
    return false;
  }
}
int32_t cliBuildRespFromCont(SCliReq* pReq, STransMsg* pResp, STransMsgHead* pHead) {
  pResp->contLen = transContLenFromMsg(pHead->msgLen);
  pResp->pCont = transContFromHead((char*)pHead);
  pResp->code = pHead->code;
  pResp->msgType = pHead->msgType;
  if (pResp->info.ahandle == 0) {
    pResp->info.ahandle = (pReq && pReq->ctx) ? pReq->ctx->ahandle : NULL;
  }
  pResp->info.traceId = pHead->traceId;
  pResp->info.hasEpSet = pHead->hasEpSet;
  pResp->info.cliVer = htonl(pHead->compatibilityVer);
  pResp->info.seqNum = taosHton64(pHead->seqNum);

  int64_t qid = taosHton64(pHead->qid);
  pResp->info.handle = (void*)qid;
  return 0;
}
int32_t cliHandleState_mayHandleReleaseResp(SCliConn* conn, STransMsgHead* pHead) {
  int32_t   code = 0;
  SCliThrd* pThrd = conn->hostThrd;
  if (pHead->msgType == TDMT_SCH_TASK_RELEASE || pHead->msgType == TDMT_SCH_TASK_RELEASE + 1) {
    int64_t   qId = taosHton64(pHead->qid);
    STraceId* trace = &pHead->traceId;
    int64_t   seqNum = taosHton64(pHead->seqNum);
    tGDebug("%s conn %p %s received from %s, local info:%s, len:%d, seqNum:%" PRId64 ", qid:%" PRId64 "",
            CONN_GET_INST_LABEL(conn), conn, TMSG_INFO(pHead->msgType), conn->dst, conn->src, pHead->msgLen, seqNum,
            qId);

    STransCtx* p = taosHashGet(conn->pQTable, &qId, sizeof(qId));
    transCtxCleanup(p);

    code = taosHashRemove(conn->pQTable, &qId, sizeof(qId));
    if (code != 0) {
      tDebug("%s conn %p failed to release req:%" PRId64 " from conn", CONN_GET_INST_LABEL(conn), conn, qId);
    }

    code = taosHashRemove(pThrd->pIdConnTable, &qId, sizeof(qId));
    if (code != 0) {
      tDebug("%s conn %p failed to release req:%" PRId64 " from thrd ", CONN_GET_INST_LABEL(conn), conn, qId);
    }

    tDebug("%s %p reqToSend:%d, sentOut:%d", CONN_GET_INST_LABEL(conn), conn, transQueueSize(&conn->reqsToSend),
           transQueueSize(&conn->reqsSentOut));

    queue set;
    QUEUE_INIT(&set);
    transQueueRemoveByFilter(&conn->reqsSentOut, filterByQid, &qId, &set, -1);
    transQueueRemoveByFilter(&conn->reqsToSend, filterByQid, &qId, &set, -1);

    (void)transReleaseExHandle(transGetRefMgt(), qId);
    (void)transRemoveExHandle(transGetRefMgt(), qId);

    while (!QUEUE_IS_EMPTY(&set)) {
      queue* el = QUEUE_HEAD(&set);
      QUEUE_REMOVE(el);
      SCliReq* pReq = QUEUE_DATA(el, SCliReq, q);

      STraceId* trace = &pReq->msg.info.traceId;
      tGDebug("start to free msg %p", pReq);
      destroyReqWrapper(pReq, pThrd);
    }
    taosMemoryFree(pHead);
    return 1;
  }
  return 0;
}
int32_t cliHandleState_mayCreateAhandle(SCliConn* conn, STransMsgHead* pHead, STransMsg* pResp) {
  int32_t code = 0;
  int64_t qId = taosHton64(pHead->qid);
  if (qId == 0) {
    return 0;
  }

  STransCtx* pCtx = taosHashGet(conn->pQTable, &qId, sizeof(qId));
  if (pCtx == 0) {
    return TSDB_CODE_RPC_NO_STATE;
  }
  STraceId* trace = &pHead->traceId;
  pResp->info.ahandle = transCtxDumpVal(pCtx, pHead->msgType);
  tGDebug("%s conn %p %s received from %s, local info:%s, qid:%" PRId64 ", create ahandle %p by %s",
          CONN_GET_INST_LABEL(conn), conn, TMSG_INFO(pHead->msgType), conn->dst, conn->src, qId, pResp->info.ahandle,
          TMSG_INFO(pHead->msgType));
  return 0;
}

static FORCE_INLINE void cliConnClearInitUserMsg(SCliConn* conn) {
  if (conn->pInitUserReq) {
    taosMemoryFree(conn->pInitUserReq);
    conn->pInitUserReq = NULL;
  }
}
void cliHandleResp(SCliConn* conn) {
  int32_t   code = 0;
  SCliThrd* pThrd = conn->hostThrd;
  STrans*   pInst = pThrd->pInst;

  cliConnClearInitUserMsg(conn);
  SCliReq* pReq = NULL;

  STransMsgHead* pHead = NULL;
  int32_t        msgLen = transDumpFromBuffer(&conn->readBuf, (char**)&pHead, 0);
  if (msgLen < 0) {
    taosMemoryFree(pHead);
    tDebug("%s conn %p recv invalid packet ", CONN_GET_INST_LABEL(conn), conn);
    // TODO: notify cb
    (void)pThrd->notifyExceptCb(pThrd, NULL, NULL);
    return;
  }

  if ((code = transDecompressMsg((char**)&pHead, &msgLen)) < 0) {
    tDebug("%s conn %p recv invalid packet, failed to decompress", CONN_GET_INST_LABEL(conn), conn);
    // TODO: notify cb
    return;
  }
  int64_t qId = taosHton64(pHead->qid);
  pHead->code = htonl(pHead->code);
  pHead->msgLen = htonl(pHead->msgLen);
  int64_t   seq = taosHton64(pHead->seqNum);
  STransMsg resp = {0};

  if (cliHandleState_mayHandleReleaseResp(conn, pHead)) {
    if (cliMayRecycleConn(conn)) {
      return;
    }
    return;
  }
  code = cliGetReqBySeq(conn, seq, pHead->msgType, &pReq);
  if (code == TSDB_CODE_OUT_OF_RANGE) {
    code = cliHandleState_mayCreateAhandle(conn, pHead, &resp);
    if (code == 0) {
      code = cliBuildRespFromCont(NULL, &resp, pHead);
      code = cliNotifyCb(conn, NULL, &resp);
      return;
    } else {
      tDebug("%s conn %p recv unexpected packet, seqNum:%" PRId64 ",qid:%" PRId64 " reason:%s",
             CONN_GET_INST_LABEL(conn), conn, seq, qId, tstrerror(code));
    }
    if (code != 0) {
      tWarn("%s conn %p recv unexpected packet, msgType:%s, seqNum:%" PRId64 ", qId:%" PRId64
            ", the sever may sends repeated response,reason:%s",
            CONN_GET_INST_LABEL(conn), conn, TMSG_INFO(pHead->msgType), seq, qId, tstrerror(code));
      // TODO: notify cb
      if (cliMayRecycleConn(conn)) {
        return;
      }
      return;
    }
  }

  code = cliBuildRespFromCont(pReq, &resp, pHead);
  STraceId* trace = &resp.info.traceId;
  tGDebug("%s conn %p %s received from %s, local info:%s, len:%d, seq:%" PRId64 ", qid:%" PRId64 "",
          CONN_GET_INST_LABEL(conn), conn, TMSG_INFO(resp.msgType), conn->dst, conn->src, pHead->msgLen, seq, qId);

  code = cliNotifyCb(conn, pReq, &resp);
  if (code == TSDB_CODE_RPC_ASYNC_IN_PROCESS) {
    tGWarn("%s msg need retry", CONN_GET_INST_LABEL(conn));
  } else {
    destroyReq(pReq);
  }
  if (cliMayRecycleConn(conn)) {
    return;
  }
  cliConnCheckTimoutMsg(conn);

  cliConnMayUpdateTimer(conn, READ_TIMEOUT);

  (void)uv_read_start((uv_stream_t*)conn->stream, cliAllocRecvBufferCb, cliRecvCb);
}

void cliConnTimeout(uv_timer_t* handle) {
  SCliConn* conn = handle->data;
  SCliThrd* pThrd = conn->hostThrd;
  int32_t   ref = transUnrefCliHandle(conn);
  if (ref <= 0) {
    cliResetConnTimer(conn);
    return;
  }

  tTrace("%s conn %p conn timeout", CONN_GET_INST_LABEL(conn), conn);
}

bool filterToRmTimoutReq(void* key, void* arg) {
  SCliReq* pReq = QUEUE_DATA(key, SCliReq, q);
  if (pReq->msg.info.qId == 0 && !REQUEST_NO_RESP(&pReq->msg) && pReq->ctx) {
    int64_t elapse = ((taosGetTimestampUs() - pReq->st) / 1000);
    if (elapse > READ_TIMEOUT) {
      return true;
    }
    return true;
  }
  return false;
}

void cliConnCheckTimoutMsg(SCliConn* conn) {
  int32_t code = 0;
  queue   set;
  QUEUE_INIT(&set);
  SCliThrd* pThrd = conn->hostThrd;
  STrans*   pInst = pThrd->pInst;

  if (pInst->startReadTimer == 0) {
    return;
  }

  if (transQueueSize(&conn->reqsSentOut) == 0) {
    return;
  }

  transQueueRemoveByFilter(&conn->reqsSentOut, filterToRmTimoutReq, NULL, &set, -1);

  while (!QUEUE_IS_EMPTY(&set)) {
    queue* el = QUEUE_HEAD(&set);
    QUEUE_REMOVE(el);
    SCliReq*  pReq = QUEUE_DATA(el, SCliReq, q);
    STraceId* trace = &pReq->msg.info.traceId;
    tDebug("%s conn %p req %s timeout, start to free", CONN_GET_INST_LABEL(conn), conn, TMSG_INFO(pReq->msg.msgType));

    SReqCtx*  pCtx = pReq ? pReq->ctx : NULL;
    STransMsg resp = {0};
    resp.code = TSDB_CODE_RPC_TIMEOUT;
    resp.msgType = pReq ? pReq->msg.msgType + 1 : 0;
    resp.info.cliVer = pInst->compatibilityVer;
    resp.info.ahandle = pCtx ? pCtx->ahandle : 0;
    resp.info.handle = pReq->msg.info.handle;
    if (pReq) {
      resp.info.traceId = pReq->msg.info.traceId;
    }

    pReq->seq = 0;
    code = cliNotifyCb(conn, pReq, &resp);
    if (code == TSDB_CODE_RPC_ASYNC_IN_PROCESS) {
      continue;
    } else {
      // already notify user
      destroyReq(pReq);
      // destroyReqWrapper(pReq, pThrd);
    }
  }

  return;
}
void cliConnTimeout__checkReq(uv_timer_t* handle) {
  SCliConn* conn = handle->data;
  cliConnCheckTimoutMsg(conn);
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
      queue* h = QUEUE_HEAD(&connList->conns);
      QUEUE_REMOVE(h);
      SCliConn* c = QUEUE_DATA(h, SCliConn, q);
      cliDestroyConn(c, true);
    }
    connList = taosHashIterate((SHashObj*)pool, connList);
  }
  taosHashCleanup(pool);
  pThrd->pool = NULL;
  return NULL;
}

static int32_t getOrCreateConnList(SCliThrd* pThrd, const char* key, SConnList** ppList) {
  int32_t    code = 0;
  void*      pool = pThrd->pool;
  size_t     klen = strlen(key);
  SConnList* plist = taosHashGet((SHashObj*)pool, key, klen);
  if (plist == NULL) {
    SConnList list = {0};
    QUEUE_INIT(&list.conns);
    code = taosHashPut((SHashObj*)pool, key, klen, (void*)&list, sizeof(list));
    if (code != 0) {
      return code;
    }

    plist = taosHashGet(pool, key, klen);
    if (plist == NULL) {
      return TSDB_CODE_INVALID_PTR;
    }
    QUEUE_INIT(&plist->conns);
    *ppList = plist;
  } else {
    *ppList = plist;
  }
  return 0;
}
static int32_t cliGetConnFromPool(SCliThrd* pThrd, const char* key, SCliConn** ppConn) {
  int32_t code = 0;
  void*   pool = pThrd->pool;
  STrans* pInst = pThrd->pInst;

  SConnList* plist = NULL;
  code = getOrCreateConnList(pThrd, key, &plist);
  if (code != 0) {
    return code;
  }

  if (QUEUE_IS_EMPTY(&plist->conns)) {
    if (plist->size >= pInst->connLimitNum) {
      return TSDB_CODE_RPC_MAX_SESSIONS;
    }
    return TSDB_CODE_RPC_NETWORK_BUSY;
  }

  queue* h = QUEUE_TAIL(&plist->conns);
  plist->size -= 1;
  QUEUE_REMOVE(h);

  SCliConn* conn = QUEUE_DATA(h, SCliConn, q);
  conn->status = ConnNormal;
  QUEUE_INIT(&conn->q);
  conn->list = plist;

  if (conn->task != NULL) {
    SDelayTask* task = conn->task;
    conn->task = NULL;
    transDQCancel(((SCliThrd*)conn->hostThrd)->timeoutQueue, task);
  }

  tDebug("conn %p get from pool, pool size:%d, dst:%s", conn, conn->list->size, conn->dstAddr);

  *ppConn = conn;
  return 0;
}

// code
static int32_t cliGetOrCreateConn(SCliThrd* pThrd, SCliReq* pReq, SCliConn** pConn) {
  // impl later
  char*    fqdn = EPSET_GET_INUSE_IP(&pReq->ctx->epSet);
  uint16_t port = EPSET_GET_INUSE_PORT(&pReq->ctx->epSet);
  char     addr[TSDB_FQDN_LEN + 64] = {0};
  CONN_CONSTRUCT_HASH_KEY(addr, fqdn, port);

  int32_t code = cliGetConnFromPool(pThrd, addr, pConn);
  if (code == TSDB_CODE_RPC_MAX_SESSIONS) {
    return code;
  } else if (code == TSDB_CODE_RPC_NETWORK_BUSY) {
    code = cliCreateConn2(pThrd, pReq, pConn);
  } else {
  }
  return code;
}
static void addConnToPool(void* pool, SCliConn* conn) {
  if (conn->status == ConnInPool) {
    return;
  }
  (void)uv_read_stop(conn->stream);

  SCliThrd* thrd = conn->hostThrd;
  cliResetConnTimer(conn);
  if (conn->list == NULL && conn->dstAddr != NULL) {
    conn->list = taosHashGet((SHashObj*)pool, conn->dstAddr, strlen(conn->dstAddr));
  }

  conn->status = ConnInPool;
  QUEUE_INIT(&conn->q);
  QUEUE_PUSH(&conn->list->conns, &conn->q);
  conn->list->size += 1;
  tDebug("conn %p added to pool, pool size: %d, dst: %s", conn, conn->list->size, conn->dstAddr);

  conn->heapMissHit = 0;

  if (conn->list->size >= 10) {
    STaskArg* arg = taosMemoryCalloc(1, sizeof(STaskArg));
    if (arg == NULL) return;
    arg->param1 = conn;
    arg->param2 = thrd;

    STrans* pInst = thrd->pInst;
    conn->task = transDQSched(thrd->timeoutQueue, doCloseIdleConn, arg, 10 * CONN_PERSIST_TIME(pInst->idleTime));
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
    return terrno;
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
  int32_t fd;
  (void)uv_fileno((uv_handle_t*)handle, &fd);
  (void)taosSetSockOpt2(fd);

  SCliConn* conn = handle->data;

  SConnBuffer* pBuf = &conn->readBuf;
  if (nread > 0) {
    pBuf->len += nread;
    while (transReadComplete(pBuf)) {
      tTrace("%s conn %p read complete", CONN_GET_INST_LABEL(conn), conn);
      if (pBuf->invalid) {
        conn->broken = true;
        (void)transUnrefCliHandle(conn);
        return;
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
           transGetRefCount(conn));
    conn->broken = true;
    (void)transUnrefCliHandle(conn);
  }
}

static int32_t cliCreateConn2(SCliThrd* pThrd, SCliReq* pReq, SCliConn** ppConn) {
  int32_t   code = 0;
  SCliConn* pConn = NULL;
  char*     ip = EPSET_GET_INUSE_IP(&pReq->ctx->epSet);
  int32_t   port = EPSET_GET_INUSE_PORT(&pReq->ctx->epSet);

  TAOS_CHECK_GOTO(cliCreateConn(pThrd, &pConn, ip, port), NULL, _exception);

  code = cliHandleState_mayUpdateState(pThrd, pReq, pConn);

  (void)addConnToHeapCache(pThrd->connHeapCache, pConn);
  (void)transQueuePush(&pConn->reqsToSend, &pReq->q);
  return cliDoConn(pThrd, pConn);
_exception:
  // free conn
  return code;
}
void cliDestroyMsg(void* arg) {
  queue*   e = arg;
  SCliReq* pReq = QUEUE_DATA(e, SCliReq, q);
  if (pReq->msg.info.notFreeAhandle == 0 && pReq->ctx->ahandle != 0) {
    // taosMemoryFree(pReq->ctx->ahandle);
  }
  destroyReq(pReq);
}
static int32_t cliCreateConn(SCliThrd* pThrd, SCliConn** pCliConn, char* ip, int32_t port) {
  int32_t code = 0;
  int32_t lino = 0;

  SCliConn* conn = taosMemoryCalloc(1, sizeof(SCliConn));
  if (conn == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _failed);
  }

  char addr[TSDB_FQDN_LEN + 64] = {0};
  CONN_CONSTRUCT_HASH_KEY(addr, ip, port);
  conn->dstAddr = taosStrdup(addr);
  conn->ipStr = taosStrdup(ip);
  conn->port = port;

  conn->hostThrd = pThrd;
  conn->status = ConnNormal;
  conn->broken = false;
  QUEUE_INIT(&conn->q);

  TAOS_CHECK_GOTO(transQueueInit(&conn->reqsToSend, cliDestroyMsg), NULL, _failed);
  TAOS_CHECK_GOTO(transQueueInit(&conn->reqsSentOut, cliDestroyMsg), NULL, _failed);

  TAOS_CHECK_GOTO(transInitBuffer(&conn->readBuf), NULL, _failed);

  conn->hostThrd = pThrd;
  conn->seq = 0;

  conn->pQTable = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (conn->pQTable == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _failed);
  }
  TAOS_CHECK_GOTO(allocConnRef(conn, false), NULL, _failed);

  TAOS_CHECK_GOTO(cliGetConnTimer(pThrd, conn), &lino, _failed);

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

  conn->bufSize = BUFFER_LIMIT;
  conn->buf = (uv_buf_t*)taosMemoryCalloc(1, BUFFER_LIMIT * sizeof(uv_buf_t));

  (void)initWQ(&conn->wq);
  conn->stream->data = conn;
  conn->connReq.data = conn;

  *pCliConn = conn;

  return code;
_failed:
  if (conn) {
    taosMemoryFree(conn->stream);

    destroyCliConnQTable(conn);
    taosHashCleanup(conn->pQTable);
    (void)transDestroyBuffer(&conn->readBuf);
    transQueueDestroy(&conn->reqsToSend);
    transQueueDestroy(&conn->reqsSentOut);
    taosMemoryFree(conn->dstAddr);

    (void)transReleaseExHandle(transGetRefMgt(), conn->refId);
    (void)transRemoveExHandle(transGetRefMgt(), conn->refId);
  }
  tError("failed to create conn, code:%d", code);
  taosMemoryFree(conn);
  return code;
}
static void cliDestroyConn(SCliConn* conn, bool clear) { cliHandleException(conn); }
static void cliDestroy(uv_handle_t* handle) {
  if (uv_handle_get_type(handle) != UV_TCP || handle->data == NULL) {
    return;
  }
  SCliConn* conn = handle->data;
  SCliThrd* pThrd = conn->hostThrd;
  cliResetConnTimer(conn);

  (void)destroyAllReqs(conn);

  if (conn->refId > 0) {
    (void)transReleaseExHandle(transGetRefMgt(), conn->refId);
    (void)transRemoveExHandle(transGetRefMgt(), conn->refId);
  }
  (void)delConnFromHeapCache(pThrd->connHeapCache, conn);
  taosMemoryFree(conn->dstAddr);
  taosMemoryFree(conn->stream);
  taosMemoryFree(conn->ipStr);

  void* pIter = taosHashIterate(conn->pQTable, NULL);
  while (pIter) {
    int64_t* qid = taosHashGetKey(pIter, NULL);
    (void)taosHashRemove(pThrd->pIdConnTable, qid, sizeof(*qid));
    pIter = taosHashIterate(conn->pQTable, pIter);
    tDebug("%s conn %p destroy state %" PRId64 "", CONN_GET_INST_LABEL(conn), conn, *qid);
  }

  destroyCliConnQTable(conn);

  if (conn->pInitUserReq) {
    taosMemoryFree(conn->pInitUserReq);
    conn->pInitUserReq = NULL;
  }

  taosMemoryFree(conn->buf);
  destroyWQ(&conn->wq);
  (void)transDestroyBuffer(&conn->readBuf);

  tTrace("%s conn %p destroy successfully", CONN_GET_INST_LABEL(conn), conn);

  taosMemoryFree(conn);
}

static FORCE_INLINE bool filterAllReq(void* e, void* arg) { return 1; }

static FORCE_INLINE int32_t destroyAllReqs(SCliConn* conn) {
  int32_t   code = 0;
  SCliThrd* pThrd = conn->hostThrd;
  STrans*   pInst = pThrd->pInst;
  queue     set;
  QUEUE_INIT(&set);
  // TODO
  // 1. from qId from thread table
  // 2. not itera to all reqs
  transQueueRemoveByFilter(&conn->reqsSentOut, filterAllReq, NULL, &set, -1);
  transQueueRemoveByFilter(&conn->reqsToSend, filterAllReq, NULL, &set, -1);

  while (!QUEUE_IS_EMPTY(&set)) {
    queue* el = QUEUE_HEAD(&set);
    QUEUE_REMOVE(el);

    SCliReq* pReq = QUEUE_DATA(el, SCliReq, q);

    SReqCtx*  pCtx = pReq ? pReq->ctx : NULL;
    STransMsg resp = {0};
    resp.code = (conn->connnected ? TSDB_CODE_RPC_BROKEN_LINK : TSDB_CODE_RPC_NETWORK_UNAVAIL);
    resp.msgType = pReq ? pReq->msg.msgType + 1 : 0;
    resp.info.cliVer = pInst->compatibilityVer;
    resp.info.ahandle = pCtx ? pCtx->ahandle : 0;
    resp.info.handle = pReq->msg.info.handle;
    if (pReq) {
      resp.info.traceId = pReq->msg.info.traceId;
    }

    // handle noresp and inter manage msg
    if (pCtx == NULL || REQUEST_NO_RESP(&pReq->msg)) {
      destroyReq(pReq);
      continue;
    }

    pReq->seq = 0;
    code = cliNotifyCb(conn, pReq, &resp);
    if (code == TSDB_CODE_RPC_ASYNC_IN_PROCESS) {
      continue;
    } else {
      // already notify user
      destroyReq(pReq);
    }
  }
  return 0;
}
static void cliHandleException(SCliConn* conn) {
  int32_t   code = 0;
  SCliThrd* pThrd = conn->hostThrd;
  STrans*   pInst = pThrd->pInst;

  cliResetConnTimer(conn);
  (void)destroyAllReqs(conn);

  QUEUE_REMOVE(&conn->q);
  if (conn->registered) {
    int8_t ref = transGetRefCount(conn);
    if (ref == 0 && !uv_is_closing((uv_handle_t*)conn->stream)) {
      uv_close((uv_handle_t*)conn->stream, cliDestroy);
    }
  }
}

bool filterToRmReq(void* h, void* arg) {
  queue*   el = h;
  SCliReq* pReq = QUEUE_DATA(el, SCliReq, q);
  if (pReq->sent == 1 && REQUEST_NO_RESP(&pReq->msg)) {
    return true;
  }
  return false;
}
static void cliConnRmReqs(SCliConn* conn) {
  queue set;
  QUEUE_INIT(&set);

  transQueueRemoveByFilter(&conn->reqsSentOut, filterToRmReq, NULL, &set, -1);
  while (!QUEUE_IS_EMPTY(&set)) {
    queue* el = QUEUE_HEAD(&set);
    QUEUE_REMOVE(el);
    SCliReq* pReq = QUEUE_DATA(el, SCliReq, q);
    destroyReq(pReq);
  }
  return;
}

static void cliBatchSendCb(uv_write_t* req, int status) {
  SWReqsWrapper* wrapper = (SWReqsWrapper*)req->data;
  SCliConn*      conn = wrapper->arg;

  SCliThrd* pThrd = conn->hostThrd;

  freeWReqToWQ(&conn->wq, wrapper);

  int32_t ref = transUnrefCliHandle(conn);
  if (ref <= 0) {
    return;
  }
  cliConnRmReqs(conn);
  if (status != 0) {
    tDebug("%s conn %p failed to send  msg, reason:%s", CONN_GET_INST_LABEL(conn), conn, uv_err_name(status));
    (void)transUnrefCliHandle(conn);
    return;
  }

  cliConnMayUpdateTimer(conn, READ_TIMEOUT);

  (void)uv_read_start((uv_stream_t*)conn->stream, cliAllocRecvBufferCb, cliRecvCb);

  if (!cliMayRecycleConn(conn)) {
    (void)cliBatchSend(conn);
  }
}
bool cliConnMayAddUserInfo(SCliConn* pConn, STransMsgHead** ppHead, int32_t* msgLen) {
  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pInst = pThrd->pInst;
  if (pConn->userInited == 1) {
    return false;
  }
  STransMsgHead* pHead = *ppHead;
  STransMsgHead* tHead = taosMemoryCalloc(1, *msgLen + sizeof(pInst->user));
  memcpy((char*)tHead, (char*)pHead, TRANS_MSG_OVERHEAD);
  memcpy((char*)tHead + TRANS_MSG_OVERHEAD, pInst->user, sizeof(pInst->user));

  memcpy((char*)tHead + TRANS_MSG_OVERHEAD + sizeof(pInst->user), (char*)pHead + TRANS_MSG_OVERHEAD,
         *msgLen - TRANS_MSG_OVERHEAD);

  tHead->withUserInfo = 1;
  *ppHead = tHead;
  *msgLen += sizeof(pInst->user);

  pConn->pInitUserReq = tHead;
  pConn->userInited = 1;
  return true;
}
int32_t cliBatchSend(SCliConn* pConn) {
  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pInst = pThrd->pInst;

  if (pConn->broken) {
    return 0;
  }
  int32_t size = transQueueSize(&pConn->reqsToSend);

  int32_t totalLen = 0;
  if (size == 0) {
    tDebug("%s conn %p not msg to send", pInst->label, pConn);
    return 0;
  }
  uv_buf_t* wb = NULL;
  if (pConn->bufSize < size) {
    pConn->buf = taosMemoryRealloc(pConn->buf, size * sizeof(uv_buf_t));
    pConn->bufSize = size;
    taosMemoryFree(wb);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  wb = pConn->buf;

  int j = 0;
  while (!transQueueEmpty(&pConn->reqsToSend)) {
    queue*   h = transQueuePop(&pConn->reqsToSend);
    SCliReq* pCliMsg = QUEUE_DATA(h, SCliReq, q);
    SReqCtx* pCtx = pCliMsg->ctx;
    pConn->seq++;

    STransMsg* pReq = (STransMsg*)(&pCliMsg->msg);
    if (pReq->pCont == 0) {
      pReq->pCont = (void*)rpcMallocCont(0);
      pReq->contLen = 0;
    }

    int32_t msgLen = transMsgLenFromCont(pReq->contLen);

    STransMsgHead* pHead = transHeadFromCont(pReq->pCont);

    char*   content = pReq->pCont;
    int32_t contLen = pReq->contLen;
    if (cliConnMayAddUserInfo(pConn, &pHead, &msgLen)) {
      content = transContFromHead(pHead);
      contLen = transContLenFromMsg(msgLen);
    }
    if (pHead->comp == 0) {
      pHead->noResp = REQUEST_NO_RESP(pReq) ? 1 : 0;
      pHead->msgType = pReq->msgType;
      pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
      pHead->traceId = pReq->info.traceId;
      pHead->magicNum = htonl(TRANS_MAGIC_NUM);
      pHead->version = TRANS_VER;
      pHead->compatibilityVer = htonl(pInst->compatibilityVer);
    }
    pHead->timestamp = taosHton64(pCliMsg->st);
    pHead->seqNum = taosHton64(pConn->seq);
    pHead->qid = taosHton64(pReq->info.qId);

    if (pHead->comp == 0) {
      if (pInst->compressSize != -1 && pInst->compressSize < contLen) {
        msgLen = transCompressMsg(content, contLen) + sizeof(STransMsgHead);
        pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
      }
    } else {
      msgLen = (int32_t)ntohl((uint32_t)(pHead->msgLen));
    }
    wb[j++] = uv_buf_init((char*)pHead, msgLen);
    totalLen += msgLen;

    pCliMsg->sent = 1;
    pCliMsg->seq = pConn->seq;

    STraceId* trace = &pCliMsg->msg.info.traceId;
    tGDebug("%s conn %p %s is sent to %s, local info:%s, seq:%" PRId64 ", qid:%" PRId64 "", CONN_GET_INST_LABEL(pConn),
            pConn, TMSG_INFO(pReq->msgType), pConn->dst, pConn->src, pConn->seq, pReq->info.qId);
    (void)transQueuePush(&pConn->reqsSentOut, &pCliMsg->q);
  }
  transRefCliHandle(pConn);
  uv_write_t* req = allocWReqFromWQ(&pConn->wq, pConn);
  tDebug("%s conn %p start to send msg, batch size:%d, len:%d", CONN_GET_INST_LABEL(pConn), pConn, size, totalLen);
  int32_t ret = uv_write(req, (uv_stream_t*)pConn->stream, wb, j, cliBatchSendCb);
  if (ret != 0) {
    tError("%s conn %p failed to send msg, reason:%s", CONN_GET_INST_LABEL(pConn), pConn, uv_err_name(ret));
    freeWReqToWQ(&pConn->wq, req->data);
    transUnrefCliHandle(pConn);
  }
  return 0;
}

int32_t cliSendReq(SCliConn* pConn, SCliReq* pCliMsg) {
  int32_t code = 0;
  (void)transQueuePush(&pConn->reqsToSend, &pCliMsg->q);
  code = cliBatchSend(pConn);
  return code;
}

static void cliDestroyBatch(SCliBatch* pBatch) {
  if (pBatch == NULL) return;
  while (!QUEUE_IS_EMPTY(&pBatch->wq)) {
    queue* h = QUEUE_HEAD(&pBatch->wq);
    QUEUE_REMOVE(h);

    SCliReq* p = QUEUE_DATA(h, SCliReq, q);
    destroyReq(p);
  }
  SCliBatchList* p = pBatch->pList;
  p->sending -= 1;
  taosMemoryFree(pBatch);
}

static int32_t cliDoConn(SCliThrd* pThrd, SCliConn* conn) {
  int32_t lino = 0;
  STrans* pInst = pThrd->pInst;

  uint32_t ipaddr;
  int32_t  code = cliGetIpFromFqdnCache(pThrd->fqdn2ipCache, conn->ipStr, &ipaddr);
  if (code != 0) {
    TAOS_CHECK_GOTO(code, &lino, _exception1);
  }

  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = ipaddr;
  addr.sin_port = (uint16_t)htons(conn->port);

  tTrace("%s conn %p try to connect to %s", pInst->label, conn, conn->dstAddr);

  int32_t fd = taosCreateSocketWithTimeout(TRANS_CONN_TIMEOUT * 10);
  if (fd < 0) {
    TAOS_CHECK_GOTO(terrno, &lino, _exception1);
  }

  int ret = uv_tcp_open((uv_tcp_t*)conn->stream, fd);
  if (ret != 0) {
    tError("%s conn %p failed to set stream, reason:%s", transLabel(pInst), conn, uv_err_name(ret));
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, &lino, _exception1);
  }
  ret = transSetConnOption((uv_tcp_t*)conn->stream, 20);
  if (ret != 0) {
    tError("%s conn %p failed to set socket opt, reason:%s", transLabel(pInst), conn, uv_err_name(ret));
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, &lino, _exception1);
    return code;
  }

  transRefCliHandle(conn);
  ret = uv_tcp_connect(&conn->connReq, (uv_tcp_t*)(conn->stream), (const struct sockaddr*)&addr, cliConnCb);
  if (ret != 0) {
    tError("failed connect to %s, reason:%s", conn->dstAddr, uv_err_name(ret));
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, &lino, _exception1);
  }

  transRefCliHandle(conn);
  ret = uv_timer_start(conn->timer, cliConnTimeout, TRANS_CONN_TIMEOUT, 0);
  if (ret != 0) {
    tError("%s conn %p failed to start timer, reason:%s", transLabel(pInst), conn, uv_err_name(ret));
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, &lino, _exception2);
  }
  conn->registered = 1;
  return TSDB_CODE_RPC_ASYNC_IN_PROCESS;

_exception1:
  tError("%s conn %p failed to do connect, reason:%s", transLabel(pInst), conn, tstrerror(code));
  cliDestroyConn(conn, true);
  return code;

_exception2:
  transUnrefCliHandle(conn);
  tError("%s conn %p failed to do connect, reason:%s", transLabel(pInst), conn, tstrerror(code));
  return code;
}

int32_t cliConnSetSockInfo(SCliConn* pConn) {
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

  return 0;
};

// static int32_t cliBuildExeceptMsg(SCliConn* pConn, SCliReq* pReq, STransMsg* pResp) {
//   SCliThrd* pThrd = pConn->hostThrd;
//   STrans*   pInst = pThrd->pInst;
//   memset(pResp, 0, sizeof(STransMsg));
//   STransMsg resp = {0};
//   resp.contLen = 0;
//   resp.pCont = NULL;
//   resp.msgType = pReq->msg.msgType + 1;
//   resp.info.ahandle = pReq->ctx->ahandle;
//   resp.info.traceId = pReq->msg.info.traceId;
//   resp.info.hasEpSet = false;
//   resp.info.cliVer = pInst->compatibilityVer;
//   return 0;
// }

bool filteGetAll(void* q, void* arg) { return true; }
void cliConnCb(uv_connect_t* req, int status) {
  SCliConn* pConn = req->data;
  SCliThrd* pThrd = pConn->hostThrd;
  bool      timeout = false;

  int32_t ref = transUnrefCliHandle(pConn);
  if (ref <= 0) {
    return;
  }
  if (pConn->timer == NULL) {
    timeout = true;
    return;
  } else {
    cliResetConnTimer(pConn);
  }

  STUB_RAND_NETWORK_ERR(status);

  if (status != 0) {
    tDebug("%s conn %p failed to connect to %s, reason:%s", CONN_GET_INST_LABEL(pConn), pConn, pConn->dstAddr,
           uv_strerror(status));
    (void)transUnrefCliHandle(pConn);
    return;
  }
  pConn->connnected = 1;
  (void)cliConnSetSockInfo(pConn);
  tTrace("%s conn %p connect to server successfully", CONN_GET_INST_LABEL(pConn), pConn);

  (void)cliBatchSend(pConn);
}

static void doNotifyCb(SCliReq* pReq, SCliThrd* pThrd, int32_t code) {
  SReqCtx* pCtx = pReq->ctx;
  STrans*  pInst = pThrd->pInst;

  STransMsg resp = {0};
  resp.contLen = 0;
  resp.pCont = NULL;
  resp.code = code;
  resp.msgType = pReq->msg.msgType + 1;
  resp.info.ahandle = pReq->ctx->ahandle;
  resp.info.traceId = pReq->msg.info.traceId;
  resp.info.hasEpSet = false;
  resp.info.cliVer = pInst->compatibilityVer;
  if (pCtx->pSem != NULL) {
    if (pCtx->pRsp == NULL) {
    } else {
      memcpy((char*)pCtx->pRsp, (char*)&resp, sizeof(resp));
    }
  } else {
    pInst->cfp(pInst->parent, &resp, NULL);
  }

  destroyReq(pReq);
}
static void cliHandleQuit(SCliThrd* pThrd, SCliReq* pReq) {
  if (!transAsyncPoolIsEmpty(pThrd->asyncPool)) {
    pThrd->stopMsg = pReq;
    return;
  }
  pThrd->stopMsg = NULL;
  pThrd->quit = true;

  tDebug("cli work thread %p start to quit", pThrd);
  destroyReq(pReq);

  (void)destroyConnPool(pThrd);
  (void)uv_walk(pThrd->loop, cliWalkCb, NULL);
}
static void cliHandleRelease(SCliThrd* pThrd, SCliReq* pReq) { return; }
static void cliHandleUpdate(SCliThrd* pThrd, SCliReq* pReq) {
  SReqCtx* pCtx = pReq->ctx;
  if (pThrd->pCvtAddr != NULL) {
    taosMemoryFreeClear(pThrd->pCvtAddr);
  }
  pThrd->pCvtAddr = pCtx->pCvtAddr;
  destroyReq(pReq);
  return;
}

FORCE_INLINE int32_t cliMayCvtFqdnToIp(SEpSet* pEpSet, const SCvtAddr* pCvtAddr) {
  if (pCvtAddr == NULL) {
    return 0;
  }
  if (pCvtAddr->cvt == false) {
    if (EPSET_IS_VALID(pEpSet)) {
      return 0;
    } else {
      return TSDB_CODE_RPC_FQDN_ERROR;
    }
  }
  if (pEpSet->numOfEps == 1 && strncmp(pEpSet->eps[0].fqdn, pCvtAddr->fqdn, TSDB_FQDN_LEN) == 0) {
    memset(pEpSet->eps[0].fqdn, 0, TSDB_FQDN_LEN);
    memcpy(pEpSet->eps[0].fqdn, pCvtAddr->ip, TSDB_FQDN_LEN);
  }
  if (EPSET_IS_VALID(pEpSet)) {
    return 0;
  }
  return TSDB_CODE_RPC_FQDN_ERROR;
}

FORCE_INLINE bool cliIsEpsetUpdated(int32_t code, SReqCtx* pCtx) {
  if (code != 0) return false;

  return transEpSetIsEqual(&pCtx->epSet, &pCtx->origEpSet) ? false : true;
}

FORCE_INLINE int32_t cliBuildExceptResp(SCliThrd* pThrd, SCliReq* pReq, STransMsg* pResp) {
  if (pReq == NULL) return -1;

  STrans* pInst = pThrd->pInst;

  SReqCtx*  pCtx = pReq ? pReq->ctx : NULL;
  STransMsg resp = {0};
  // resp.code = (conn->connnected ? TSDB_CODE_RPC_BROKEN_LINK : TSDB_CODE_RPC_NETWORK_UNAVAIL);
  pResp->msgType = pReq ? pReq->msg.msgType + 1 : 0;
  pResp->info.cliVer = pInst->compatibilityVer;
  pResp->info.ahandle = pCtx ? pCtx->ahandle : 0;
  if (pReq) {
    pResp->info.traceId = pReq->msg.info.traceId;
  }

  // handle noresp and inter manage msg
  if (pCtx == NULL || REQUEST_NO_RESP(&pReq->msg)) {
    return TSDB_CODE_RPC_NO_STATE;
  }
  if (pResp->code == 0) {
    pResp->code = TSDB_CODE_RPC_BROKEN_LINK;
  }

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
  SCliReq*  pReq = arg->param1;
  SCliThrd* pThrd = arg->param2;
  STrans*   pInst = pThrd->pInst;

  QUEUE_REMOVE(&pReq->q);
  STraceId* trace = &pReq->msg.info.traceId;

  tGTrace("%s msg %s cannot get available conn after timeout", pInst->label, TMSG_INFO(pReq->msg.msgType));
  doNotifyCb(pReq, pThrd, TSDB_CODE_RPC_MAX_SESSIONS);

  taosMemoryFree(arg);
}

int32_t cliHandleState_mayUpdateStateCtx(SCliConn* pConn, SCliReq* pReq) {
  int32_t   code = 0;
  int64_t   qid = pReq->msg.info.qId;
  SReqCtx*  pCtx = pReq->ctx;
  SCliThrd* pThrd = pConn->hostThrd;
  if (pCtx == NULL) {
    tDebug("%s conn %p not need to update statue ctx, qid:%" PRId64 "", transLabel(pThrd->pInst), pConn, qid);
    return 0;
  }

  STransCtx* pUserCtx = taosHashGet(pConn->pQTable, &qid, sizeof(qid));
  if (pUserCtx == NULL) {
    code = taosHashPut(pConn->pQTable, &qid, sizeof(qid), &pCtx->userCtx, sizeof(pCtx->userCtx));
    tDebug("%s conn %p succ to add statue ctx, qid:%" PRId64 "", transLabel(pThrd->pInst), pConn, qid);
  } else {
    transCtxMerge(pUserCtx, &pCtx->userCtx);
    tDebug("%s conn %p succ to update statue ctx, qid:%" PRId64 "", transLabel(pThrd->pInst), pConn, qid);
  }
  return 0;
}
int32_t cliMayGetStateByQid(SCliThrd* pThrd, SCliReq* pReq, SCliConn** pConn) {
  int32_t code = 0;
  int64_t qid = pReq->msg.info.qId;
  if (qid == 0) {
    return TSDB_CODE_RPC_NO_STATE;
  } else {
    SExHandle* exh = transAcquireExHandle(transGetRefMgt(), qid);
    if (exh == NULL) {
      return TSDB_CODE_RPC_STATE_DROPED;
    }

    SReqState* pState = taosHashGet(pThrd->pIdConnTable, &qid, sizeof(qid));

    if (pState == NULL) {
      if (pReq->ctx == NULL) {
        (void)transReleaseExHandle(transGetRefMgt(), qid);
        return TSDB_CODE_RPC_STATE_DROPED;
      }
      tDebug("%s conn %p failed to get statue, qid:%" PRId64 "", transLabel(pThrd->pInst), pConn, qid);
      (void)transReleaseExHandle(transGetRefMgt(), qid);
      return TSDB_CODE_RPC_ASYNC_IN_PROCESS;
    } else {
      *pConn = pState->conn;
      tDebug("%s conn %p succ to get conn of statue, qid:%" PRId64 "", transLabel(pThrd->pInst), pConn, qid);
    }
    (void)transReleaseExHandle(transGetRefMgt(), qid);
    return 0;
  }
}

int32_t cliHandleState_mayUpdateState(SCliThrd* pThrd, SCliReq* pReq, SCliConn* pConn) {
  int32_t code = 0;
  int64_t qid = pReq->msg.info.qId;
  if (qid == 0) {
    return TSDB_CODE_RPC_NO_STATE;
  }

  SReqState state = {.conn = pConn, .arg = NULL};
  code = taosHashPut(pThrd->pIdConnTable, &qid, sizeof(qid), &state, sizeof(state));
  if (code != 0) {
    tDebug("%s conn %p failed to statue, qid:%" PRId64 "", transLabel(pThrd->pInst), pConn, qid);
  } else {
    tDebug("%s conn %p succ to add statue, qid:%" PRId64 " (1)", transLabel(pThrd->pInst), pConn, qid);
  }

  (void)cliHandleState_mayUpdateStateCtx(pConn, pReq);
  return code;
}
void cliHandleBatchReq(SCliThrd* pThrd, SCliReq* pReq) {
  int32_t   lino = 0;
  STransMsg resp = {0};
  int32_t   code = (pThrd->initCb)(pThrd, pReq, NULL);
  TAOS_CHECK_GOTO(code, &lino, _exception);

  STrans*   pInst = pThrd->pInst;
  SCliConn* pConn = NULL;
  code = cliMayGetStateByQid(pThrd, pReq, &pConn);
  if (code == 0) {
    (void)cliHandleState_mayUpdateStateCtx(pConn, pReq);
  } else if (code == TSDB_CODE_RPC_STATE_DROPED) {
    TAOS_CHECK_GOTO(code, &lino, _exception);
    return;
  }

  if (code == TSDB_CODE_RPC_NO_STATE || code == TSDB_CODE_RPC_ASYNC_IN_PROCESS) {
    char    addr[TSDB_FQDN_LEN + 64] = {0};
    char*   ip = EPSET_GET_INUSE_IP(&pReq->ctx->epSet);
    int32_t port = EPSET_GET_INUSE_PORT(&pReq->ctx->epSet);
    CONN_CONSTRUCT_HASH_KEY(addr, ip, port);

    pConn = getConnFromHeapCache(pThrd->connHeapCache, addr);
    if (pConn == NULL) {
      code = cliGetOrCreateConn(pThrd, pReq, &pConn);
      if (code == TSDB_CODE_RPC_MAX_SESSIONS) {
        TAOS_CHECK_GOTO(code, &lino, _exception);
      } else if (code == TSDB_CODE_RPC_ASYNC_IN_PROCESS) {
        // do nothing, notiy
        return;
      } else if (code == 0) {
        (void)addConnToHeapCache(pThrd->connHeapCache, pConn);
      } else {
        // do nothing, notiy
        return;
      }
    }
    code = cliHandleState_mayUpdateState(pThrd, pReq, pConn);
  }
  code = cliSendReq(pConn, pReq);

  tTrace("%s conn %p ready", pInst->label, pConn);
  return;

_exception:
  resp.code = code;
  STraceId* trace = &pReq->msg.info.traceId;
  tGWarn("%s failed to process req, reason:%s", pInst->label, tstrerror(code));
  (void)(pThrd->notifyExceptCb)(pThrd, pReq, &resp);
  return;
}

void cliHandleReq(SCliThrd* pThrd, SCliReq* pReq) { return cliHandleBatchReq(pThrd, pReq); }

static void cliDoReq(queue* wq, SCliThrd* pThrd) {
  int count = 0;

  while (!QUEUE_IS_EMPTY(wq)) {
    queue* h = QUEUE_HEAD(wq);
    QUEUE_REMOVE(h);

    SCliReq* pReq = QUEUE_DATA(h, SCliReq, q);

    if (pReq->type == Quit) {
      pThrd->stopMsg = pReq;
      continue;
    }
    (*cliAsyncHandle[pReq->type])(pThrd, pReq);
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
static int32_t createBatch(SCliBatch** ppBatch, SCliBatchList* pList, SCliReq* pReq);

static int32_t createBatchList(SCliBatchList** ppBatchList, char* key, char* ip, uint32_t port);

static void destroyBatchList(SCliBatchList* pList);
static void cliBuildBatch(SCliReq* pReq, queue* h, SCliThrd* pThrd) {
  int32_t  code = 0;
  STrans*  pInst = pThrd->pInst;
  SReqCtx* pCtx = pReq->ctx;

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
      destroyReq(pReq);
      return;
    }

    pBatchList->batchLenLimit = pInst->batchSize;

    SCliBatch* pBatch = NULL;
    code = createBatch(&pBatch, pBatchList, pReq);
    if (code != 0) {
      destroyBatchList(pBatchList);
      destroyReq(pReq);
      return;
    }

    code = taosHashPut(pThrd->batchCache, key, klen, &pBatchList, sizeof(void*));
    if (code != 0) {
      destroyBatchList(pBatchList);
    }
  } else {
    if (QUEUE_IS_EMPTY(&(*ppBatchList)->wq)) {
      SCliBatch* pBatch = NULL;
      code = createBatch(&pBatch, *ppBatchList, pReq);
      if (code != 0) {
        destroyReq(pReq);
        cliDestroyBatch(pBatch);
      }
    } else {
      queue*     hdr = QUEUE_TAIL(&((*ppBatchList)->wq));
      SCliBatch* pBatch = QUEUE_DATA(hdr, SCliBatch, listq);
      if ((pBatch->batchSize + pReq->msg.contLen) < (*ppBatchList)->batchLenLimit) {
        QUEUE_PUSH(&pBatch->wq, h);
        pBatch->batchSize += pReq->msg.contLen;
        pBatch->wLen += 1;
      } else {
        SCliBatch* tBatch = NULL;
        code = createBatch(&tBatch, *ppBatchList, pReq);
        if (code != 0) {
          destroyReq(pReq);
        }
      }
    }
  }
  return;
}
static int32_t createBatchList(SCliBatchList** ppBatchList, char* key, char* ip, uint32_t port) {
  SCliBatchList* pBatchList = taosMemoryCalloc(1, sizeof(SCliBatchList));
  if (pBatchList == NULL) {
    tError("failed to create batch list, reason:%s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return terrno;
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
static int32_t createBatch(SCliBatch** ppBatch, SCliBatchList* pList, SCliReq* pReq) {
  SCliBatch* pBatch = taosMemoryCalloc(1, sizeof(SCliBatch));
  if (pBatch == NULL) {
    tError("failed to create batch, reason:%s", tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    return terrno;
  }

  QUEUE_INIT(&pBatch->wq);
  QUEUE_INIT(&pBatch->listq);

  QUEUE_PUSH(&pBatch->wq, &pReq->q);
  pBatch->wLen += 1;
  pBatch->batchSize = pReq->msg.contLen;
  pBatch->pList = pList;

  QUEUE_PUSH(&pList->wq, &pBatch->listq);
  pList->len += 1;

  *ppBatch = pBatch;
  return 0;
}
static void cliDoBatchReq(queue* wq, SCliThrd* pThrd) { return cliDoReq(wq, pThrd); }

static void cliAsyncCb(uv_async_t* handle) {
  SAsyncItem* item = handle->data;
  SCliThrd*   pThrd = item->pThrd;
  STrans*     pInst = pThrd->pInst;

  // batch process to avoid to lock/unlock frequently
  queue wq;
  (void)taosThreadMutexLock(&item->mtx);
  QUEUE_MOVE(&item->qmsg, &wq);
  (void)taosThreadMutexUnlock(&item->mtx);

  cliDealFunc[pInst->supportBatch](&wq, pThrd);

  if (pThrd->stopMsg != NULL) cliHandleQuit(pThrd, pThrd->stopMsg);
}

static FORCE_INLINE void destroyReq(void* arg) {
  SCliReq* pReq = arg;
  if (pReq == NULL) {
    return;
  }
  STraceId* trace = &pReq->msg.info.traceId;
  tGDebug("free memory:%p, free ctx: %p", pReq, pReq->ctx);

  if (pReq->ctx) destroyReqCtx(pReq->ctx);
  transFreeMsg(pReq->msg.pCont);
  taosMemoryFree(pReq);
}
static FORCE_INLINE void destroyReqWrapper(void* arg, void* param) {
  if (arg == NULL) return;

  SCliReq*  pReq = arg;
  SCliThrd* pThrd = param;

  if (pReq->ctx != NULL && pReq->ctx->ahandle != NULL) {
    if (pReq->msg.info.notFreeAhandle == 0 && pThrd != NULL && pThrd->destroyAhandleFp != NULL) {
      (*pThrd->destroyAhandleFp)(pReq->ctx->ahandle);
    }
  }
  destroyReq(pReq);
}
static FORCE_INLINE void destroyReqAndAhanlde(void* param) {
  if (param == NULL) return;

  STaskArg* arg = param;
  SCliReq*  pReq = arg->param1;
  SCliThrd* pThrd = arg->param2;
  destroyReqWrapper(pReq, pThrd);
}

static void* cliWorkThread(void* arg) {
  char threadName[TSDB_LABEL_LEN] = {0};

  SCliThrd* pThrd = (SCliThrd*)arg;
  pThrd->pid = taosGetSelfPthreadId();

  tsEnableRandErr = true;
  (void)strtolower(threadName, pThrd->pInst->label);
  setThreadName(threadName);

  (void)uv_run(pThrd->loop, UV_RUN_DEFAULT);

  tDebug("thread quit-thread:%08" PRId64 "", pThrd->pid);
  return NULL;
}

void* transInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* pInstRef) {
  int32_t  code = 0;
  SCliObj* cli = taosMemoryCalloc(1, sizeof(SCliObj));
  if (cli == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _err);
  }

  STrans* pInst = pInstRef;
  memcpy(cli->label, label, TSDB_LABEL_LEN);
  cli->numOfThreads = numOfThreads;

  cli->pThreadObj = (SCliThrd**)taosMemoryCalloc(cli->numOfThreads, sizeof(SCliThrd*));
  if (cli->pThreadObj == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _err);
  }

  for (int i = 0; i < cli->numOfThreads; i++) {
    SCliThrd* pThrd = NULL;
    code = createThrdObj(pInstRef, &pThrd);
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
int32_t initCb(void* thrd, SCliReq* pReq, STransMsg* pResp) {
  SCliThrd* pThrd = thrd;
  if (pReq->ctx == NULL) {
    return 0;
  }
  return cliMayCvtFqdnToIp(&pReq->ctx->epSet, pThrd->pCvtAddr);
}
int32_t notifyExceptCb(void* thrd, SCliReq* pReq, STransMsg* pResp) {
  SCliThrd* pThrd = thrd;
  STrans*   pInst = pThrd->pInst;
  int32_t   code = cliBuildExceptResp(pThrd, pReq, pResp);
  if (code != 0) {
    destroyReq(pReq);
    return code;
  }
  pInst->cfp(pInst->parent, pResp, NULL);
  destroyReq(pReq);
  return code;
}

int32_t notfiyCb(void* thrd, SCliReq* pReq, STransMsg* pResp) {
  // impl later
  SCliThrd* pThrd = thrd;
  STrans*   pInst = pThrd->pInst;

  return 0;
}

static int32_t createThrdObj(void* trans, SCliThrd** ppThrd) {
  int32_t code = 0;
  STrans* pInst = trans;

  SCliThrd* pThrd = (SCliThrd*)taosMemoryCalloc(1, sizeof(SCliThrd));
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _end);
  }

  QUEUE_INIT(&pThrd->msg);
  (void)taosThreadMutexInit(&pThrd->msgMtx, NULL);

  pThrd->loop = (uv_loop_t*)taosMemoryMalloc(sizeof(uv_loop_t));
  if (pThrd->loop == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _end);
  }

  code = uv_loop_init(pThrd->loop);
  if (code != 0) {
    tError("failed to init uv_loop, reason:%s", uv_err_name(code));
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, NULL, _end);
  }

  int32_t nSync = pInst->supportBatch ? 4 : 8;
  code = transAsyncPoolCreate(pThrd->loop, nSync, pThrd, cliAsyncCb, &pThrd->asyncPool);
  if (code != 0) {
    tError("failed to init async pool since:%s", tstrerror(code));
    TAOS_CHECK_GOTO(code, NULL, _end);
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
    (void)taosArrayPush(pThrd->timerList, &timer);
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

  pThrd->destroyAhandleFp = pInst->destroyFp;

  pThrd->fqdn2ipCache = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pThrd->fqdn2ipCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }

  pThrd->batchCache = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pThrd->batchCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }

  pThrd->connHeapCache = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pThrd->connHeapCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }

  pThrd->pIdConnTable = taosHashInit(512, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pThrd->connHeapCache == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _end);
  }

  pThrd->initCb = initCb;
  pThrd->notifyCb = notfiyCb;
  pThrd->notifyExceptCb = notifyExceptCb;

  pThrd->nextTimeout = taosGetTimestampMs() + CONN_PERSIST_TIME(pInst->idleTime);
  pThrd->pInst = trans;
  pThrd->quit = false;

  *ppThrd = pThrd;
  return code;

_end:
  if (pThrd) {
    (void)taosThreadMutexDestroy(&pThrd->msgMtx);

    (void)uv_loop_close(pThrd->loop);
    taosMemoryFree(pThrd->loop);
    (void)taosThreadMutexDestroy(&pThrd->msgMtx);
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
    taosHashCleanup(pThrd->pIdConnTable);

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
  TRANS_DESTROY_ASYNC_POOL_MSG(pThrd->asyncPool, SCliReq, destroyReqWrapper, (void*)pThrd);
  transAsyncPoolDestroy(pThrd->asyncPool);

  transDQDestroy(pThrd->delayQueue, destroyReqAndAhanlde);
  transDQDestroy(pThrd->timeoutQueue, NULL);
  transDQDestroy(pThrd->waitConnQueue, NULL);

  tDebug("thread destroy %" PRId64, pThrd->pid);
  for (int i = 0; i < taosArrayGetSize(pThrd->timerList); i++) {
    uv_timer_t* timer = taosArrayGetP(pThrd->timerList, i);
    taosMemoryFree(timer);
  }

  taosArrayDestroy(pThrd->timerList);
  taosMemoryFree(pThrd->loop);
  taosHashCleanup(pThrd->fqdn2ipCache);

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

  void* pIter2 = taosHashIterate(pThrd->connHeapCache, NULL);
  while (pIter2 != NULL) {
    SHeap* heap = (SHeap*)(pIter2);
    transHeapDestroy(heap);
    pIter2 = (void*)taosHashIterate(pThrd->connHeapCache, pIter2);
  }
  taosHashCleanup(pThrd->connHeapCache);

  taosHashCleanup(pThrd->pIdConnTable);

  taosMemoryFree(pThrd->pCvtAddr);

  taosMemoryFree(pThrd);
}

static FORCE_INLINE void destroyReqCtx(SReqCtx* ctx) { taosMemoryFree(ctx); }

int32_t cliSendQuit(SCliThrd* thrd) {
  // cli can stop gracefully
  int32_t  code = 0;
  SCliReq* msg = taosMemoryCalloc(1, sizeof(SCliReq));
  if (msg == NULL) {
    return terrno;
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

FORCE_INLINE int cliRBChoseIdx(STrans* pInst) {
  int32_t index = pInst->index;
  if (pInst->numOfThreads == 0) {
    return -1;
  }
  /*
   * no lock, and to avoid CPU load imbalance, set limit pInst->numOfThreads * 2000;
   */
  if (pInst->index++ >= pInst->numOfThreads * 2000) {
    pInst->index = 0;
  }
  return index % pInst->numOfThreads;
}
static FORCE_INLINE void doDelayTask(void* param) {
  STaskArg* arg = param;
  cliHandleReq((SCliThrd*)arg->param2, (SCliReq*)arg->param1);
  taosMemoryFree(arg);
}

static FORCE_INLINE void doCloseIdleConn(void* param) {
  STaskArg* arg = param;
  SCliConn* conn = arg->param1;
  tDebug("%s conn %p idle, close it", CONN_GET_INST_LABEL(conn), conn);

  int32_t ref = transUnrefCliHandle(conn);
  if (ref <= 0) {
    conn->task = NULL;
    taosMemoryFree(arg);
    return;
  }
  taosMemoryFree(arg);
}
static FORCE_INLINE void cliPerfLog_schedMsg(SCliReq* pReq, char* label) {
  if (!(rpcDebugFlag & DEBUG_DEBUG)) {
    return;
  }
  SReqCtx*  pCtx = pReq->ctx;
  STraceId* trace = &pReq->msg.info.traceId;
  char      tbuf[512] = {0};
  (void)epsetToStr(&pCtx->epSet, tbuf, tListLen(tbuf));

  tGDebug("%s retry on next node,use:%s, step: %d,timeout:%" PRId64 "", label, tbuf, pCtx->retryStep,
          pCtx->retryNextInterval);
  return;
}
static FORCE_INLINE void cliPerfLog_epset(SCliConn* pConn, SCliReq* pReq) {
  if (!(rpcDebugFlag & DEBUG_TRACE)) {
    return;
  }
  SReqCtx* pCtx = pReq->ctx;

  char tbuf[512] = {0};
  (void)epsetToStr(&pCtx->epSet, tbuf, tListLen(tbuf));
  tTrace("%s conn %p extract epset from msg", CONN_GET_INST_LABEL(pConn), pConn);
  return;
}

static FORCE_INLINE int32_t cliSchedMsgToNextNode(SCliReq* pReq, SCliThrd* pThrd) {
  STrans*  pInst = pThrd->pInst;
  SReqCtx* pCtx = pReq->ctx;
  cliPerfLog_schedMsg(pReq, transLabel(pThrd->pInst));

  STaskArg* arg = taosMemoryMalloc(sizeof(STaskArg));
  if (arg == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  arg->param1 = pReq;
  arg->param2 = pThrd;

  SDelayTask* pTask = transDQSched(pThrd->delayQueue, doDelayTask, arg, pCtx->retryNextInterval);
  if (pTask == NULL) {
    taosMemoryFree(arg);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return 0;
}

FORCE_INLINE bool cliTryUpdateEpset(SCliReq* pReq, STransMsg* pResp) {
  int32_t  code = 0;
  SReqCtx* ctx = pReq->ctx;
  SEpSet*  dst = &ctx->epSet;

  if ((pResp == NULL || pResp->info.hasEpSet == 0)) {
    return false;
  }
  // rebuild resp msg
  SEpSet epset;
  if ((code = tDeserializeSEpSet(pResp->pCont, pResp->contLen, &epset)) < 0) {
    tError("failed to deserialize epset, code:%d", code);
    return false;
  }
  int32_t tlen = tSerializeSEpSet(NULL, 0, dst);

  char*   buf = NULL;
  int32_t len = pResp->contLen - tlen;
  if (len != 0) {
    buf = rpcMallocCont(len);
    if (buf == NULL) {
      pResp->code = TSDB_CODE_OUT_OF_MEMORY;
      return false;
    }
    // TODO: check buf
    memcpy(buf, (char*)pResp->pCont + tlen, len);
  }
  rpcFreeCont(pResp->pCont);

  pResp->pCont = buf;
  pResp->contLen = len;

  pResp->info.hasEpSet = 1;

  epsetAssign(dst, &epset);
  return true;
}

bool cliResetEpset(SReqCtx* pCtx, STransMsg* pResp, bool hasEpSet) {
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

void cliRetryMayInitCtx(STrans* pInst, SCliReq* pReq) {
  SReqCtx* pCtx = pReq->ctx;
  if (!pCtx->retryInit) {
    pCtx->retryMinInterval = pInst->retryMinInterval;
    pCtx->retryMaxInterval = pInst->retryMaxInterval;
    pCtx->retryStepFactor = pInst->retryStepFactor;
    pCtx->retryMaxTimeout = pInst->retryMaxTimeout;
    pCtx->retryInitTimestamp = taosGetTimestampMs();
    pCtx->retryNextInterval = pCtx->retryMinInterval;
    pCtx->retryStep = 0;
    pCtx->retryInit = 1;
    pCtx->retryCode = TSDB_CODE_SUCCESS;
    pReq->msg.info.handle = 0;
  }
}

int32_t cliRetryIsTimeout(STrans* pInst, SCliReq* pReq) {
  SReqCtx* pCtx = pReq->ctx;
  if (pCtx->retryMaxTimeout != -1 && taosGetTimestampMs() - pCtx->retryInitTimestamp >= pCtx->retryMaxTimeout) {
    return 1;
  }
  return 0;
}

int8_t cliRetryShouldRetry(STrans* pInst, STransMsg* pResp) {
  bool retry = pInst->retry != NULL ? pInst->retry(pResp->code, pResp->msgType - 1) : false;
  return retry == false ? 0 : 1;
}

void cliRetryUpdateRule(SReqCtx* pCtx, int8_t noDelay) {
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
}

int32_t cliRetryDoSched(SCliReq* pReq, SCliThrd* pThrd) {
  int32_t code = cliSchedMsgToNextNode(pReq, pThrd);
  if (code != 0) {
    tError("failed to sched msg to next node, reason:%s", tstrerror(code));
    return code;
  }
  return 0;
}

bool cliMayRetry(SCliConn* pConn, SCliReq* pReq, STransMsg* pResp) {
  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pInst = pThrd->pInst;

  SReqCtx* pCtx = pReq->ctx;
  int32_t  code = pResp->code;

  cliRetryMayInitCtx(pInst, pReq);

  if (!cliRetryShouldRetry(pInst, pResp)) {
    return false;
  }

  if (cliRetryIsTimeout(pInst, pReq)) {
    return false;
  }

  // code, msgType
  // A:  epset,leader, not self
  // B:  epset,not know leader
  // C:  noepset,leader but not serivce

  bool noDelay = false;
  if (code == TSDB_CODE_RPC_BROKEN_LINK || code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
    tTrace("code str %s, contlen:%d 0", tstrerror(code), pResp->contLen);
    noDelay = cliResetEpset(pCtx, pResp, false);
    transFreeMsg(pResp->pCont);
  } else if (code == TSDB_CODE_SYN_NOT_LEADER || code == TSDB_CODE_SYN_INTERNAL_ERROR ||
             code == TSDB_CODE_SYN_PROPOSE_NOT_READY || code == TSDB_CODE_VND_STOPPED ||
             code == TSDB_CODE_MNODE_NOT_FOUND || code == TSDB_CODE_APP_IS_STARTING ||
             code == TSDB_CODE_APP_IS_STOPPING || code == TSDB_CODE_VND_STOPPED) {
    tTrace("code str %s, contlen:%d 1", tstrerror(code), pResp->contLen);
    noDelay = cliResetEpset(pCtx, pResp, true);
    transFreeMsg(pResp->pCont);
  } else if (code == TSDB_CODE_SYN_RESTORING) {
    tTrace("code str %s, contlen:%d 0", tstrerror(code), pResp->contLen);
    noDelay = cliResetEpset(pCtx, pResp, true);
    transFreeMsg(pResp->pCont);
  } else {
    tTrace("code str %s, contlen:%d 0", tstrerror(code), pResp->contLen);
    noDelay = cliResetEpset(pCtx, pResp, false);
    transFreeMsg(pResp->pCont);
  }
  if (code != TSDB_CODE_RPC_BROKEN_LINK && code != TSDB_CODE_RPC_NETWORK_UNAVAIL && code != TSDB_CODE_SUCCESS) {
    // save one internal code
    pCtx->retryCode = code;
  }

  cliRetryUpdateRule(pCtx, noDelay);

  pReq->sent = 0;
  pReq->seq = 0;

  code = cliRetryDoSched(pReq, pThrd);
  if (code != 0) {
    pResp->code = code;
    tError("failed to sched msg to next node, reason:%s", tstrerror(code));
    return false;
  }
  return true;
}

void cliMayResetRespCode(SCliReq* pReq, STransMsg* pResp) {
  SReqCtx* pCtx = pReq->ctx;
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
}

int32_t cliNotifyImplCb(SCliConn* pConn, SCliReq* pReq, STransMsg* pResp) {
  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pInst = pThrd->pInst;
  SReqCtx*  pCtx = pReq ? pReq->ctx : NULL;
  STraceId* trace = &pResp->info.traceId;

  if (pCtx == NULL) {
    pInst->cfp(pInst->parent, pResp, NULL);
    return 0;
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
    if (pResp->info.hasEpSet == 1) {
      pInst->cfp(pInst->parent, pResp, &pCtx->epSet);
    } else {
      if (!cliIsEpsetUpdated(pResp->code, pCtx)) {
        pInst->cfp(pInst->parent, pResp, NULL);
      } else {
        pInst->cfp(pInst->parent, pResp, &pCtx->epSet);
      }
    }
  }
  return 0;
}
int32_t cliNotifyCb(SCliConn* pConn, SCliReq* pReq, STransMsg* pResp) {
  SCliThrd* pThrd = pConn->hostThrd;
  STrans*   pInst = pThrd->pInst;

  if (pReq != NULL) {
    if (cliMayRetry(pConn, pReq, pResp)) {
      return TSDB_CODE_RPC_ASYNC_IN_PROCESS;
    }

    cliMayResetRespCode(pReq, pResp);

    if (cliTryUpdateEpset(pReq, pResp)) {
      cliPerfLog_epset(pConn, pReq);
    }
  }
  return cliNotifyImplCb(pConn, pReq, pResp);
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
  int32_t ref = 0;
  if (handle == NULL) {
    return;
  }
  SCliConn* conn = (SCliConn*)handle;
  conn->ref++;

  tTrace("%s conn %p ref %d", CONN_GET_INST_LABEL(conn), conn, conn->ref);
}
int32_t transUnrefCliHandle(void* handle) {
  if (handle == NULL) {
    return 0;
  }
  SCliConn* conn = (SCliConn*)handle;
  int32_t   ref = conn->ref--;

  tTrace("%s conn %p ref:%d", CONN_GET_INST_LABEL(conn), conn, conn->ref);
  if (conn->ref == 0) {
    cliDestroyConn(conn, true);
  }
  return ref;
}

int32_t transGetRefCount(void* handle) {
  if (handle == NULL) {
    return 0;
  }
  SCliConn* conn = (SCliConn*)handle;
  return conn->ref;
}
static FORCE_INLINE SCliThrd* transGetWorkThrdFromHandle(STrans* trans, int64_t handle) {
  SCliThrd*  pThrd = NULL;
  SExHandle* exh = transAcquireExHandle(transGetRefMgt(), handle);
  if (exh == NULL) {
    return NULL;
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

  STransMsg tmsg = {
      .msgType = TDMT_SCH_TASK_RELEASE, .info.handle = handle, .info.ahandle = (void*)0, .info.qId = (int64_t)handle};

  TRACE_SET_MSGID(&tmsg.info.traceId, tGenIdPI64());

  SReqCtx* pCtx = taosMemoryCalloc(1, sizeof(SReqCtx));
  if (pCtx == NULL) {
    return terrno;
  }
  pCtx->ahandle = tmsg.info.ahandle;
  SCliReq* cmsg = taosMemoryCalloc(1, sizeof(SCliReq));

  if (cmsg == NULL) {
    taosMemoryFree(pCtx);
    return terrno;
  }
  cmsg->msg = tmsg;
  cmsg->st = taosGetTimestampUs();
  cmsg->type = Normal;
  cmsg->ctx = pCtx;

  STraceId* trace = &tmsg.info.traceId;
  tGDebug("send release request at thread:%08" PRId64 ", malloc memory:%p", pThrd->pid, cmsg);

  if ((code = transAsyncSend(pThrd->asyncPool, &cmsg->q)) != 0) {
    destroyReq(cmsg);
    return code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code;
  }
  return code;
}

static int32_t transInitMsg(void* pInstRef, const SEpSet* pEpSet, STransMsg* pReq, STransCtx* ctx, SCliReq** pCliMsg) {
  TRACE_SET_MSGID(&pReq->info.traceId, tGenIdPI64());

  SReqCtx* pCtx = taosMemoryCalloc(1, sizeof(SReqCtx));
  if (pCtx == NULL) {
    return terrno;
  }

  epsetAssign(&pCtx->epSet, pEpSet);
  epsetAssign(&pCtx->origEpSet, pEpSet);

  pCtx->ahandle = pReq->info.ahandle;
  pCtx->msgType = pReq->msgType;

  if (ctx != NULL) pCtx->userCtx = *ctx;

  SCliReq* pCliReq = taosMemoryCalloc(1, sizeof(SCliReq));
  if (pReq == NULL) {
    taosMemoryFree(pCtx);
    return terrno;
  }

  pCliReq->ctx = pCtx;
  pCliReq->msg = *pReq;
  pCliReq->st = taosGetTimestampUs();
  pCliReq->type = Normal;

  *pCliMsg = pCliReq;

  return 0;
}

int32_t transSendRequest(void* pInstRef, const SEpSet* pEpSet, STransMsg* pReq, STransCtx* ctx) {
  STrans* pInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (pInst == NULL) {
    transFreeMsg(pReq->pCont);
    pReq->pCont = NULL;
    return TSDB_CODE_RPC_MODULE_QUIT;
  }
  int32_t   code = 0;
  int64_t   handle = (int64_t)pReq->info.handle;
  SCliThrd* pThrd = transGetWorkThrd(pInst, handle);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_BROKEN_LINK, NULL, _exception);
  }

  pReq->info.qId = handle;

  SCliReq* pCliMsg = NULL;
  TAOS_CHECK_GOTO(transInitMsg(pInstRef, pEpSet, pReq, ctx, &pCliMsg), NULL, _exception);

  STraceId* trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pInst), pThrd->pid,
          EPSET_GET_INUSE_IP(pEpSet), EPSET_GET_INUSE_PORT(pEpSet), pReq->info.ahandle);
  if ((code = transAsyncSend(pThrd->asyncPool, &(pCliMsg->q))) != 0) {
    destroyReq(pCliMsg);
    (void)transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
    return (code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code);
  }
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  return 0;

_exception:
  transFreeMsg(pReq->pCont);
  pReq->pCont = NULL;
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  return code;
}
int32_t transSendRequestWithId(void* pInstRef, const SEpSet* pEpSet, STransMsg* pReq, int64_t* transpointId) {
  if (transpointId == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = 0;

  STrans* pInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (pInst == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_MODULE_QUIT, NULL, _exception);
  }

  TAOS_CHECK_GOTO(transAllocHandle(transpointId), NULL, _exception);

  SCliThrd* pThrd = transGetWorkThrd(pInst, *transpointId);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_BROKEN_LINK, NULL, _exception);
  }

  SExHandle* exh = transAcquireExHandle(transGetRefMgt(), *transpointId);
  if (exh == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_MODULE_QUIT, NULL, _exception);
  }

  pReq->info.handle = (void*)(*transpointId);
  pReq->info.qId = *transpointId;

  SCliReq* pCliMsg = NULL;
  TAOS_CHECK_GOTO(transInitMsg(pInstRef, pEpSet, pReq, NULL, &pCliMsg), NULL, _exception);

  STraceId* trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pInst), pThrd->pid,
          EPSET_GET_INUSE_IP(pEpSet), EPSET_GET_INUSE_PORT(pEpSet), pReq->info.ahandle);
  if ((code = transAsyncSend(pThrd->asyncPool, &(pCliMsg->q))) != 0) {
    destroyReq(pCliMsg);
    (void)transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
    return (code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code);
  }
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  return 0;

_exception:
  transFreeMsg(pReq->pCont);
  pReq->pCont = NULL;
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  return code;
}

int32_t transSendRecv(void* pInstRef, const SEpSet* pEpSet, STransMsg* pReq, STransMsg* pRsp) {
  STrans* pInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (pInst == NULL) {
    transFreeMsg(pReq->pCont);
    pReq->pCont = NULL;
    return TSDB_CODE_RPC_MODULE_QUIT;
  }
  int32_t code = 0;

  STransMsg* pTransRsp = taosMemoryCalloc(1, sizeof(STransMsg));
  if (pTransRsp == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN1);
  }

  SCliThrd* pThrd = transGetWorkThrd(pInst, (int64_t)pReq->info.handle);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_BROKEN_LINK, NULL, _RETURN1);
  }

  tsem_t* sem = taosMemoryCalloc(1, sizeof(tsem_t));
  if (sem == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN1);
  }

  code = tsem_init(sem, 0, 0);
  if (code != 0) {
    taosMemoryFree(sem);
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN1);
  }

  if (pReq->info.traceId.msgId == 0) TRACE_SET_MSGID(&pReq->info.traceId, tGenIdPI64());

  SReqCtx* pCtx = taosMemoryCalloc(1, sizeof(SReqCtx));
  if (pCtx == NULL) {
    (void)tsem_destroy(sem);
    taosMemoryFree(sem);
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN1);
  }

  epsetAssign(&pCtx->epSet, pEpSet);
  epsetAssign(&pCtx->origEpSet, pEpSet);
  pCtx->ahandle = pReq->info.ahandle;
  pCtx->msgType = pReq->msgType;
  pCtx->pSem = sem;
  pCtx->pRsp = pTransRsp;

  SCliReq* pCliReq = taosMemoryCalloc(1, sizeof(SCliReq));
  if (pCliReq == NULL) {
    (void)tsem_destroy(sem);
    taosMemoryFree(sem);
    taosMemoryFree(pCtx);
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN1);
  }

  pCliReq->ctx = pCtx;
  pCliReq->msg = *pReq;
  pCliReq->st = taosGetTimestampUs();
  pCliReq->type = Normal;

  STraceId* trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pInst), pThrd->pid,
          EPSET_GET_INUSE_IP(&pCtx->epSet), EPSET_GET_INUSE_PORT(&pCtx->epSet), pReq->info.ahandle);

  code = transAsyncSend(pThrd->asyncPool, &pCliReq->q);
  if (code != 0) {
    destroyReq(pReq);
    TAOS_CHECK_GOTO((code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code), NULL, _RETURN);
  }
  (void)tsem_wait(sem);

  memcpy(pRsp, pTransRsp, sizeof(STransMsg));

_RETURN:
  tsem_destroy(sem);
  taosMemoryFree(sem);
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  taosMemoryFree(pTransRsp);
  return code;
_RETURN1:
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  taosMemoryFree(pTransRsp);
  taosMemoryFree(pReq->pCont);
  pReq->pCont = NULL;
  return code;
}

int32_t transCreateSyncMsg(STransMsg* pTransMsg, int64_t* refId) {
  int32_t  code = 0;
  tsem2_t* sem = taosMemoryCalloc(1, sizeof(tsem2_t));
  if (sem == NULL) {
    return terrno;
  }

  if (tsem2_init(sem, 0, 0) != 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), NULL, _EXIT);
  }

  STransSyncMsg* pSyncMsg = taosMemoryCalloc(1, sizeof(STransSyncMsg));
  if (pSyncMsg == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _EXIT);
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

int32_t transSendRecvWithTimeout(void* pInstRef, SEpSet* pEpSet, STransMsg* pReq, STransMsg* pRsp, int8_t* epUpdated,
                                 int32_t timeoutMs) {
  int32_t code = 0;
  STrans* pInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (pInst == NULL) {
    transFreeMsg(pReq->pCont);
    pReq->pCont = NULL;
    return TSDB_CODE_RPC_MODULE_QUIT;
  }

  STransMsg* pTransMsg = taosMemoryCalloc(1, sizeof(STransMsg));
  if (pTransMsg == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN2);
  }

  SCliThrd* pThrd = transGetWorkThrd(pInst, (int64_t)pReq->info.handle);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_RPC_BROKEN_LINK, NULL, _RETURN2);
  }

  if (pReq->info.traceId.msgId == 0) TRACE_SET_MSGID(&pReq->info.traceId, tGenIdPI64());

  SReqCtx* pCtx = taosMemoryCalloc(1, sizeof(SReqCtx));
  if (pCtx == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN2);
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

  SCliReq* pCliReq = taosMemoryCalloc(1, sizeof(SCliReq));
  if (pReq == NULL) {
    taosMemoryFree(pCtx);
    TAOS_CHECK_GOTO(terrno, NULL, _RETURN2);
  }

  pCliReq->ctx = pCtx;
  pCliReq->msg = *pReq;
  pCliReq->st = taosGetTimestampUs();
  pCliReq->type = Normal;

  STraceId* trace = &pReq->info.traceId;
  tGDebug("%s send request at thread:%08" PRId64 ", dst:%s:%d, app:%p", transLabel(pInst), pThrd->pid,
          EPSET_GET_INUSE_IP(&pCtx->epSet), EPSET_GET_INUSE_PORT(&pCtx->epSet), pReq->info.ahandle);

  code = transAsyncSend(pThrd->asyncPool, &pCliReq->q);
  if (code != 0) {
    destroyReq(pReq);
    TAOS_CHECK_GOTO(code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT ? TSDB_CODE_RPC_MODULE_QUIT : code, NULL, _RETURN);
    goto _RETURN;
  }

  code = tsem2_timewait(pSyncMsg->pSem, timeoutMs);
  if (code != 0) {
    pRsp->code = code;
  } else {
    memcpy(pRsp, pSyncMsg->pRsp, sizeof(STransMsg));
    pSyncMsg->pRsp->pCont = NULL;
    if (pSyncMsg->hasEpSet == 1) {
      epsetAssign(pEpSet, &pSyncMsg->epSet);
      *epUpdated = 1;
    }
  }
_RETURN:
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  (void)taosReleaseRef(transGetSyncMsgMgt(), ref);
  (void)taosRemoveRef(transGetSyncMsgMgt(), ref);
  return code;
_RETURN2:
  transFreeMsg(pReq->pCont);
  pReq->pCont = NULL;
  taosMemoryFree(pTransMsg);
  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  return code;
}
/*
 *
 **/
int32_t transSetDefaultAddr(void* pInstRef, const char* ip, const char* fqdn) {
  if (ip == NULL || fqdn == NULL) return TSDB_CODE_INVALID_PARA;

  STrans* pInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (pInst == NULL) {
    return TSDB_CODE_RPC_MODULE_QUIT;
  }

  SCvtAddr cvtAddr = {0};
  tstrncpy(cvtAddr.ip, ip, sizeof(cvtAddr.ip));
  tstrncpy(cvtAddr.fqdn, fqdn, sizeof(cvtAddr.fqdn));
  cvtAddr.cvt = true;

  int32_t code = 0;
  for (int8_t i = 0; i < pInst->numOfThreads; i++) {
    SReqCtx* pCtx = taosMemoryCalloc(1, sizeof(SReqCtx));
    if (pCtx == NULL) {
      code = terrno;
      break;
    }

    pCtx->pCvtAddr = (SCvtAddr*)taosMemoryCalloc(1, sizeof(SCvtAddr));
    memcpy(pCtx->pCvtAddr, &cvtAddr, sizeof(SCvtAddr));

    SCliReq* pReq = taosMemoryCalloc(1, sizeof(SCliReq));
    if (pReq == NULL) {
      taosMemoryFree(pCtx->pCvtAddr);
      taosMemoryFree(pCtx);
      code = terrno;
      break;
    }

    pReq->ctx = pCtx;
    pReq->type = Update;

    SCliThrd* thrd = ((SCliObj*)pInst->tcphandle)->pThreadObj[i];
    tDebug("%s update epset at thread:%08" PRId64, pInst->label, thrd->pid);

    if ((code = transAsyncSend(thrd->asyncPool, &(pReq->q))) != 0) {
      taosMemoryFree(pCtx->pCvtAddr);
      destroyReq(pReq);
      if (code == TSDB_CODE_RPC_ASYNC_MODULE_QUIT) {
        code = TSDB_CODE_RPC_MODULE_QUIT;
      }
      break;
    }
  }

  (void)transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  return code;
}

int32_t transAllocHandle(int64_t* refId) {
  SExHandle* exh = taosMemoryCalloc(1, sizeof(SExHandle));
  if (exh == NULL) {
    return terrno;
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
  tDebug("trans alloc qid:%" PRId64 ", malloc:%p", exh->refId, exh);
  *refId = exh->refId;
  return 0;
}
int32_t transFreeConnById(void* pInstRef, int64_t transpointId) {
  int32_t code = 0;
  STrans* pInst = (STrans*)transAcquireExHandle(transGetInstMgt(), (int64_t)pInstRef);
  if (pInst == NULL) {
    return TSDB_CODE_RPC_MODULE_QUIT;
  }
  if (transpointId == 0) {
    tDebug("not free by refId:%" PRId64 "", transpointId);
    TAOS_CHECK_GOTO(0, NULL, _exception);
  }

  SCliThrd* pThrd = transGetWorkThrdFromHandle(pInst, transpointId);
  if (pThrd == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_REF_INVALID_ID, NULL, _exception);
  }

  SCliReq* pCli = taosMemoryCalloc(1, sizeof(SCliReq));
  if (pCli == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _exception);
  }
  pCli->type = Normal;

  STransMsg msg = {.msgType = TDMT_SCH_TASK_RELEASE, .info.handle = (void*)transpointId};
  TRACE_SET_MSGID(&msg.info.traceId, tGenIdPI64());
  msg.info.qId = transpointId;
  pCli->msg = msg;

  STraceId* trace = &pCli->msg.info.traceId;
  tGDebug("%s start to free conn qid:%" PRId64 "", pInst->label, transpointId);

  code = transAsyncSend(pThrd->asyncPool, &pCli->q);
  if (code != 0) {
    taosMemoryFree(pCli);
    TAOS_CHECK_GOTO(code, NULL, _exception);
  }

_exception:
  transReleaseExHandle(transGetInstMgt(), (int64_t)pInstRef);
  return code;
}

static int32_t getOrCreateHeap(SHashObj* pConnHeapCache, char* key, SHeap** pHeap) {
  int32_t code = 0;
  size_t  klen = strlen(key);

  SHeap* p = taosHashGet(pConnHeapCache, key, klen);
  if (p == NULL) {
    SHeap heap = {0};
    code = transHeapInit(&heap, compareHeapNode);
    if (code != 0) {
      tError("failed to init heap cache for key:%s, reason: %s", key, tstrerror(code));
      return code;
    }

    code = taosHashPut(pConnHeapCache, key, klen, &heap, sizeof(heap));
    if (code != 0) {
      transHeapDestroy(&heap);
      tError("failed to put heap to cache for key:%s, reason: %s", key, tstrerror(code));
      return code;
    }
    p = taosHashGet(pConnHeapCache, key, klen);
    if (p == NULL) {
      code = TSDB_CODE_INVALID_PARA;
    }
  }
  *pHeap = p;
  return code;
}

static FORCE_INLINE int8_t shouldSWitchToOtherConn(int32_t reqNum, int32_t sentNum, int32_t stateNum) {
  int32_t total = reqNum + sentNum;
  if (total >= BUFFER_LIMIT) {
    return 1;
  }

  if (stateNum >= BUFFER_LIMIT * 2) {
    return 1;
  }

  return 0;
}

static FORCE_INLINE bool filterToDebug(void* e, void* arg) {
  SCliReq*  pReq = QUEUE_DATA(e, SCliReq, q);
  STraceId* trace = &pReq->msg.info.traceId;
  tGWarn("%s is sent to, and no resp from server", TMSG_INFO(pReq->msg.msgType));
  return false;
}
static FORCE_INLINE int32_t logConnMissHit(SCliConn* pConn) {
  // queue set;
  // QUEUE_INIT(&set);
  pConn->heapMissHit++;
  tDebug("conn %p has %d reqs, %d sentout and %d status in process, total limit:%d, switch to other conn", pConn,
         transQueueSize(&pConn->reqsToSend), transQueueSize(&pConn->reqsSentOut), taosHashGetSize(pConn->pQTable),
         BUFFER_LIMIT);
  // if (transQueueSize(&pConn->reqsSentOut) >= BUFFER_LIMIT) {
  //   transQueueRemoveByFilter(&pConn->reqsSentOut, filterToDebug, NULL, &set, 1);
  // }
  return 0;
}
static SCliConn* getConnFromHeapCache(SHashObj* pConnHeapCache, char* key) {
  int       code = 0;
  SHeap*    pHeap = NULL;
  SCliConn* pConn = NULL;
  code = getOrCreateHeap(pConnHeapCache, key, &pHeap);
  if (code != 0) {
    tDebug("failed to get conn heap from cache for key:%s", key);
    return NULL;
  }
  code = transHeapGet(pHeap, &pConn);
  if (code != 0) {
    tDebug("failed to get conn from heap cache for key:%s", key);
    return NULL;
  } else {
    tDebug("get conn %p from heap cache for key:%s, status:%d, refCnt:%d", pConn, key, pConn->inHeap, pConn->reqRefCnt);
    int32_t reqsNum = transQueueSize(&pConn->reqsToSend);
    int32_t reqsSentOut = transQueueSize(&pConn->reqsSentOut);
    int32_t stateNum = taosHashGetSize(pConn->pQTable);

    if (shouldSWitchToOtherConn(reqsNum, reqsSentOut, stateNum)) {
      (void)logConnMissHit(pConn);
      return NULL;
    }
  }

  return pConn;
}
static int32_t addConnToHeapCache(SHashObj* pConnHeapCacahe, SCliConn* pConn) {
  SHeap*  p = NULL;
  int32_t code = 0;

  if (pConn->heap != NULL) {
    p = pConn->heap;
    tDebug("conn %p add to heap cache for key:%s,status:%d, refCnt:%d, add direct", pConn, pConn->dstAddr,
           pConn->inHeap, pConn->reqRefCnt);
  } else {
    code = getOrCreateHeap(pConnHeapCacahe, pConn->dstAddr, &p);
    if (code != 0) {
      return code;
    }
  }

  code = transHeapInsert(p, pConn);
  tDebug("conn %p add to heap cache for key:%s,status:%d, refCnt:%d", pConn, pConn->dstAddr, pConn->inHeap,
         pConn->reqRefCnt);
  return code;
}

static int32_t delConnFromHeapCache(SHashObj* pConnHeapCache, SCliConn* pConn) {
  if (pConn->heap != NULL) {
    tDebug("conn %p delete from heap cache direct", pConn);
    return transHeapDelete(pConn->heap, pConn);
  }

  SHeap* p = taosHashGet(pConnHeapCache, pConn->dstAddr, strlen(pConn->dstAddr));
  if (p == NULL) {
    tDebug("failed to get heap cache for key:%s, no need to del", pConn->dstAddr);
    return 0;
  }
  int32_t code = transHeapDelete(p, pConn);
  if (code != 0) {
    tDebug("conn %p failed delete from heap cache since %s", pConn, tstrerror(code));
  }
  return code;
}
// conn heap
int32_t compareHeapNode(const HeapNode* a, const HeapNode* b) {
  SCliConn* args1 = container_of(a, SCliConn, node);
  SCliConn* args2 = container_of(b, SCliConn, node);
  if (transQueueSize(&args1->reqsToSend) > transQueueSize(&args2->reqsToSend)) {
    return 0;
  }
  return 1;
}
int32_t transHeapInit(SHeap* heap, int32_t (*cmpFunc)(const HeapNode* a, const HeapNode* b)) {
  heap->heap = heapCreate(cmpFunc);
  if (heap->heap == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  heap->cmpFunc = cmpFunc;
  return 0;
}
void transHeapDestroy(SHeap* heap) {
  if (heap != NULL) {
    heapDestroy(heap->heap);
  }
}
int32_t transHeapGet(SHeap* heap, SCliConn** p) {
  if (heapSize(heap->heap) == 0) {
    *p = NULL;
    return -1;
  }
  HeapNode* minNode = heapMin(heap->heap);
  if (minNode == NULL) {
    *p = NULL;
    return -1;
  }
  *p = container_of(minNode, SCliConn, node);
  return 0;
}
int32_t transHeapInsert(SHeap* heap, SCliConn* p) {
  // impl later
  p->reqRefCnt++;
  if (p->inHeap == 1) {
    tDebug("failed to insert conn %p since already in heap", p);
    return TSDB_CODE_DUP_KEY;
  }

  heapInsert(heap->heap, &p->node);
  p->inHeap = 1;
  p->heap = heap;
  return 0;
}
int32_t transHeapDelete(SHeap* heap, SCliConn* p) {
  // impl later
  if (p->inHeap == 0) {
    tDebug("failed to del conn %p since not in heap", p);
    return 0;
  }
  p->inHeap = 0;
  p->reqRefCnt--;
  if (p->reqRefCnt == 0) {
    heapRemove(heap->heap, &p->node);
    tDebug("conn %p delete from heap", p);
  } else if (p->reqRefCnt < 0) {
    tDebug("conn %p has %d reqs, not delete from heap,assert", p, p->reqRefCnt);
  } else {
    tDebug("conn %p has %d reqs, not delete from heap", p, p->reqRefCnt);
  }
  return 0;
}
