/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef _TD_TRANSPORT_COMM_H
#define _TD_TRANSPORT_COMM_H

#ifdef __cplusplus
extern "C" {
#endif

#include <uv.h>
#include "theap.h"
#include "tmsg.h"
#include "transLog.h"
#include "transportInt.h"
#include "trpc.h"
#include "ttrace.h"

typedef bool (*FilteFunc)(void* arg);

typedef void* queue[2];
/* Private macros. */
#define QUEUE_NEXT(q) (*(queue**)&((*(q))[0]))
#define QUEUE_PREV(q) (*(queue**)&((*(q))[1]))

#define QUEUE_PREV_NEXT(q) (QUEUE_NEXT(QUEUE_PREV(q)))
#define QUEUE_NEXT_PREV(q) (QUEUE_PREV(QUEUE_NEXT(q)))
/* Initialize an empty queue. */
#define QUEUE_INIT(q)    \
  {                      \
    QUEUE_NEXT(q) = (q); \
    QUEUE_PREV(q) = (q); \
  }

/* Return true if the queue has no element. */
#define QUEUE_IS_EMPTY(q) ((const queue*)(q) == (const queue*)QUEUE_NEXT(q))

/* Insert an element at the back of a queue. */
#define QUEUE_PUSH(q, e)           \
  {                                \
    QUEUE_NEXT(e) = (q);           \
    QUEUE_PREV(e) = QUEUE_PREV(q); \
    QUEUE_PREV_NEXT(e) = (e);      \
    QUEUE_PREV(q) = (e);           \
  }

/* Remove the given element from the queue. Any element can be removed at any *
 * time. */
#define QUEUE_REMOVE(e)                 \
  {                                     \
    QUEUE_PREV_NEXT(e) = QUEUE_NEXT(e); \
    QUEUE_NEXT_PREV(e) = QUEUE_PREV(e); \
  }
#define QUEUE_SPLIT(h, q, n)       \
  do {                             \
    QUEUE_PREV(n) = QUEUE_PREV(h); \
    QUEUE_PREV_NEXT(n) = (n);      \
    QUEUE_NEXT(n) = (q);           \
    QUEUE_PREV(h) = QUEUE_PREV(q); \
    QUEUE_PREV_NEXT(h) = (h);      \
    QUEUE_PREV(q) = (n);           \
  } while (0)

#define QUEUE_MOVE(h, n)        \
  do {                          \
    if (QUEUE_IS_EMPTY(h)) {    \
      QUEUE_INIT(n);            \
    } else {                    \
      queue* q = QUEUE_HEAD(h); \
      QUEUE_SPLIT(h, q, n);     \
    }                           \
  } while (0)

/* Return the element at the front of the queue. */
#define QUEUE_HEAD(q) (QUEUE_NEXT(q))

/* Return the element at the back of the queue. */
#define QUEUE_TAIL(q) (QUEUE_PREV(q))

/* Iterate over the element of a queue. * Mutating the queue while iterating
 * results in undefined behavior. */
#define QUEUE_FOREACH(q, e) for ((q) = QUEUE_NEXT(e); (q) != (e); (q) = QUEUE_NEXT(q))

/* Return the structure holding the given element. */
#define QUEUE_DATA(e, type, field) ((type*)((void*)((char*)(e)-offsetof(type, field))))

// #define TRANS_RETRY_COUNT_LIMIT 100   // retry count limit
// #define TRANS_RETRY_INTERVAL    15    // retry interval (ms)
#define TRANS_CONN_TIMEOUT 3000  // connect timeout (ms)
#define TRANS_READ_TIMEOUT 3000  // read timeout  (ms)
#define TRANS_PACKET_LIMIT 1024 * 1024 * 512

#define TRANS_MAGIC_NUM           0x5f375a86
#define TRANS_NOVALID_PACKET(src) ((src) != TRANS_MAGIC_NUM ? 1 : 0)

typedef struct SRpcMsg STransMsg;
typedef SRpcCtx        STransCtx;
typedef SRpcCtxVal     STransCtxVal;
typedef SRpcInfo       STrans;
typedef SRpcConnInfo   STransHandleInfo;

// ref mgt handle
typedef struct SExHandle {
  void*    handle;
  int64_t  refId;
  void*    pThrd;
  int8_t   pThrdIdx;
  queue    q;
  int8_t   inited;
  SRWLatch latch;
} SExHandle;

typedef struct {
  STransMsg* pRsp;
  SEpSet     epSet;
  int8_t     hasEpSet;
  tsem2_t*   pSem;
  int8_t     inited;
  SRWLatch   latch;
} STransSyncMsg;

/*convet from fqdn to ip */
typedef struct SCvtAddr {
  char ip[TSDB_FQDN_LEN];
  char fqdn[TSDB_FQDN_LEN];
  bool cvt;
} SCvtAddr;

#pragma pack(push, 1)

typedef struct {
  int8_t inUse;
  int8_t numOfEps;
  SEp    eps[];
} SReqEpSet;

#define TRANS_VER 2
typedef struct {
  char version : 4;       // RPC version
  char comp : 2;          // compression algorithm, 0:no compression 1:lz4
  char noResp : 2;        // noResp bits, 0: resp, 1: resp
  char withUserInfo : 2;  // 0: sent user info or not
  char secured : 2;
  char spi : 2;
  char hasEpSet : 2;  // contain epset or not, 0(default): no epset, 1: contain epset

  uint64_t timestamp;
  int32_t  compatibilityVer;
  uint32_t magicNum;
  STraceId traceId;
  int64_t  qid;
  uint32_t code;  // del later
  uint32_t msgType;
  int32_t  msgLen;
  int64_t  seqNum;
  uint8_t  content[0];  // message body starts from here
} STransMsgHead;

typedef struct {
  int32_t reserved;
  int32_t contLen;
} STransCompMsg;

typedef struct {
  uint32_t timeStamp;
  uint8_t  auth[TSDB_AUTH_LEN];
} STransDigestMsg;

typedef struct {
  uint8_t user[TSDB_UNI_LEN];
  uint8_t secret[TSDB_PASSWORD_LEN];
} STransUserMsg;

#pragma pack(pop)

int32_t transCreateReqEpsetFromUserEpset(const SEpSet* pEpset, SReqEpSet** pReqEpSet);
int32_t transCreateUserEpsetFromReqEpset(const SReqEpSet* pReqEpSet, SEpSet* pEpSet);

int32_t transValidReqEpset(SReqEpSet* pReqEpSet);

typedef struct {
  SReqEpSet* epSet;  // ip list provided by app
  SReqEpSet* origEpSet;
  void*      ahandle;  // handle provided by app
  tmsg_t     msgType;  // message type

  STransCtx      userCtx;   //
  STransMsg*     pRsp;      // for synchronous API
  tsem_t*        pSem;      // for synchronous API
  STransSyncMsg* pSyncMsg;  // for syncchronous with timeout API
  int64_t        syncMsgRef;
  SCvtAddr*      pCvtAddr;

  int64_t retryInitTimestamp;
  int64_t retryNextInterval;
  int64_t retryMaxTimeout;
  int32_t retryMinInterval;
  int32_t retryMaxInterval;
  int32_t retryStepFactor;
  int32_t retryStep;
  int32_t retryCode;
  int8_t  retryInit;
  int8_t  epsetRetryCnt;
} SReqCtx;
typedef enum { Normal, Quit, Release, Register, Update, FreeById } STransMsgType;
typedef enum { ConnNormal, ConnAcquire, ConnRelease, ConnBroken, ConnInPool } ConnStatus;

#define container_of(ptr, type, member) ((type*)((char*)(ptr)-offsetof(type, member)))
#define RPC_RESERVE_SIZE                (sizeof(STranConnCtx))

#define rpcIsReq(type) (type & 1U)

#define TRANS_RESERVE_SIZE (sizeof(STranConnCtx))

#define TRANS_MSG_OVERHEAD           (sizeof(STransMsgHead))
#define transHeadFromCont(cont)      ((STransMsgHead*)((char*)cont - sizeof(STransMsgHead)))
#define transContFromHead(msg)       (((char*)msg) + sizeof(STransMsgHead))
#define transMsgLenFromCont(contLen) (contLen + sizeof(STransMsgHead))
#define transContLenFromMsg(msgLen)  (msgLen - sizeof(STransMsgHead));
#define transIsReq(type)             (type & 1U)

#define transLabel(trans) ((STrans*)trans)->label

typedef struct SConnBuffer {
  char* buf;
  int   len;
  int   cap;
  int   left;
  int   total;
  int   invalid;
} SConnBuffer;

typedef void (*AsyncCB)(uv_async_t* handle);

typedef struct {
  void*         pThrd;
  queue         qmsg;
  TdThreadMutex mtx;  // protect qmsg;
  int64_t       num;
} SAsyncItem;

typedef struct {
  int         index;
  int         nAsync;
  uv_async_t* asyncs;
  int8_t      stop;
} SAsyncPool;

int32_t transAsyncPoolCreate(uv_loop_t* loop, int sz, void* arg, AsyncCB cb, SAsyncPool** pPool);
void    transAsyncPoolDestroy(SAsyncPool* pool);
int     transAsyncSend(SAsyncPool* pool, queue* mq);
bool    transAsyncPoolIsEmpty(SAsyncPool* pool);

#define TRANS_DESTROY_ASYNC_POOL_MSG(pool, msgType, freeFunc, param) \
  do {                                                               \
    for (int i = 0; i < pool->nAsync; i++) {                         \
      uv_async_t* async = &(pool->asyncs[i]);                        \
      SAsyncItem* item = async->data;                                \
      while (!QUEUE_IS_EMPTY(&item->qmsg)) {                         \
        tTrace("destroy msg in async pool ");                        \
        queue* h = QUEUE_HEAD(&item->qmsg);                          \
        QUEUE_REMOVE(h);                                             \
        msgType* msg = QUEUE_DATA(h, msgType, q);                    \
        if (msg != NULL) {                                           \
          freeFunc(msg, param);                                      \
        }                                                            \
      }                                                              \
    }                                                                \
  } while (0)

#define ASYNC_CHECK_HANDLE(idMgt, id, exh1)                                              \
  do {                                                                                   \
    if (id > 0) {                                                                        \
      SExHandle* exh2 = transAcquireExHandle(idMgt, id);                                 \
      if (exh2 == NULL || exh1 != exh2 || (exh2 != NULL && exh2->refId != id)) {         \
        tDebug("handle not match, exh1:%p, exh2:%p, refId:%" PRId64 "", exh1, exh2, id); \
        code = TSDB_CODE_INVALID_MSG;                                                    \
        goto _return1;                                                                   \
      }                                                                                  \
    } else {                                                                             \
      tDebug("invalid handle to release");                                               \
      goto _return2;                                                                     \
    }                                                                                    \
  } while (0)

int32_t transInitBuffer(SConnBuffer* buf);
int32_t transClearBuffer(SConnBuffer* buf);
void    transDestroyBuffer(SConnBuffer* buf);
int32_t transAllocBuffer(SConnBuffer* connBuf, uv_buf_t* uvBuf);
bool    transReadComplete(SConnBuffer* connBuf);
int32_t transResetBuffer(SConnBuffer* connBuf, int8_t resetBuf);
int32_t transDumpFromBuffer(SConnBuffer* connBuf, char** buf, int8_t resetBuf);

int32_t transSetConnOption(uv_tcp_t* stream, int keepalive);

void transRefSrvHandle(void* handle);
void transUnrefSrvHandle(void* handle);

void    transRefCliHandle(void* handle);
int32_t transUnrefCliHandle(void* handle);
int32_t transGetRefCount(void* handle);

int32_t transReleaseCliHandle(void* handle);
int32_t transReleaseSrvHandle(void* handle);

int32_t transSendRequest(void* pInit, const SEpSet* pEpSet, STransMsg* pReq, STransCtx* pCtx);
int32_t transSendRecv(void* pInit, const SEpSet* pEpSet, STransMsg* pReq, STransMsg* pRsp);
int32_t transSendRecvWithTimeout(void* pInit, SEpSet* pEpSet, STransMsg* pReq, STransMsg* pRsp, int8_t* epUpdated,
                                 int32_t timeoutMs);
int32_t transSendRequestWithId(void* pInit, const SEpSet* pEpSet, STransMsg* pReq, int64_t* transpointId);
int32_t transFreeConnById(void* pInit, int64_t transpointId);

int32_t transSendResponse(const STransMsg* msg);
int32_t transRegisterMsg(const STransMsg* msg);
int32_t transSetDefaultAddr(void* pInit, const char* ip, const char* fqdn);
int32_t transSetIpWhiteList(void* pInit, void* arg, FilteFunc* func);

void transSockInfo2Str(struct sockaddr* sockname, char* dst);

int32_t transAllocHandle(int64_t* refId);

void* transInitServer(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* pInit);
void* transInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* pInit);

void transCloseClient(void* arg);
void transCloseServer(void* arg);

void  transCtxInit(STransCtx* ctx);
void  transCtxCleanup(STransCtx* ctx);
void  transCtxClear(STransCtx* ctx);
void  transCtxMerge(STransCtx* dst, STransCtx* src);
void* transCtxDumpVal(STransCtx* ctx, int32_t key);
void* transCtxDumpBrokenlinkVal(STransCtx* ctx, int32_t* msgType);

// request list
typedef struct SWriteReq {
  queue node;  // req queue node
  void* conn;
} SWriteReq;

// queue sending msgs
typedef struct {
  queue node;
  void (*freeFunc)(void* arg);
  int32_t size;
  int8_t  inited;
} STransQueue;

/*
 * init queue
 * note: queue'size is small, default 1
 */
int32_t transQueueInit(STransQueue* queue, void (*freeFunc)(void* arg));

/*
 * put arg into queue
 * if queue'size > 1, return false; else return true
 */
void transQueuePush(STransQueue* queue, void* arg);
/*
 * the size of queue
 */
int32_t transQueueSize(STransQueue* queue);
/*
 * pop head from queue
 */
void* transQueuePop(STransQueue* queue);
/*
 * get ith from queue
 */
void* transQueueGet(STransQueue* queue, int i);
/*
 * head elm from queue
 */

void* tranQueueHead(STransQueue* q);
/*
 *  remove all match elm from queue
 */
void transQueueRemoveByFilter(STransQueue* q, bool (*filter)(void* e, void* arg), void* arg, void* dst, int32_t size);
/*
 * rm ith from queue
 */

void* transQueueRm(STransQueue* queue, int i);
/*
 * remove el from queue
 */

void transQueueRemove(STransQueue* q, void* e);
/*
 * queue empty or not
 */
bool transQueueEmpty(STransQueue* queue);
/*
 * clear queue
 */
void transQueueClear(STransQueue* queue);
/*
 * destroy queue
 */
void transQueueDestroy(STransQueue* queue);

/*
 * delay queue based on uv loop and uv timer, and only used in retry
 */
typedef struct STaskArg {
  void* param1;
  void* param2;
} STaskArg;

typedef struct SDelayTask {
  void (*func)(void* arg);
  void*    arg;
  uint64_t execTime;
  HeapNode node;
} SDelayTask;

typedef struct SDelayQueue {
  uv_timer_t* timer;
  Heap*       heap;
  uv_loop_t*  loop;
} SDelayQueue;

int32_t     transDQCreate(uv_loop_t* loop, SDelayQueue** queue);
void        transDQDestroy(SDelayQueue* queue, void (*freeFunc)(void* arg));
SDelayTask* transDQSched(SDelayQueue* queue, void (*func)(void* arg), void* arg, uint64_t timeoutMs);
void        transDQCancel(SDelayQueue* queue, SDelayTask* task);

bool transReqEpsetIsEqual(SReqEpSet* a, SReqEpSet* b);

bool transCompareReqAndUserEpset(SReqEpSet* a, SEpSet* b);
/*
 * init global func
 */
void transThreadOnce();

int32_t transInit();
void    transCleanup();
void    transPrintEpSet(SEpSet* pEpSet);

void    transFreeMsg(void* msg);
int32_t transCompressMsg(char* msg, int32_t len);
int32_t transDecompressMsg(char** msg, int32_t* len);

int32_t transOpenRefMgt(int size, void (*func)(void*));
void    transCloseRefMgt(int32_t refMgt);
int64_t transAddExHandle(int32_t refMgt, void* p);
void    transRemoveExHandle(int32_t refMgt, int64_t refId);
void*   transAcquireExHandle(int32_t refMgt, int64_t refId);
void    transReleaseExHandle(int32_t refMgt, int64_t refId);
void    transDestroyExHandle(void* handle);

int32_t transGetRefMgt();
int32_t transGetSvrRefMgt();
int32_t transGetInstMgt();
int32_t transGetSyncMsgMgt();

void transHttpEnvDestroy();

typedef struct {
  uint32_t netmask;
  uint32_t address;
  uint32_t network;
  uint32_t broadcast;
  char     info[32];
  int8_t   type;
} SubnetUtils;

int32_t subnetInit(SubnetUtils* pUtils, SIpV4Range* pRange);
int32_t subnetCheckIp(SubnetUtils* pUtils, uint32_t ip);
int32_t subnetDebugInfoToBuf(SubnetUtils* pUtils, char* buf);

int32_t transUtilSIpRangeToStr(SIpV4Range* pRange, char* buf);
int32_t transUtilSWhiteListToStr(SIpWhiteList* pWhiteList, char** ppBuf);

enum { REQ_STATUS_INIT = 0, REQ_STATUS_PROCESSING };

#if defined(WINDOWS) || defined(DARWIN)
#define BUFFER_LIMIT       1
#define STATE_BUFFER_LIMIT 1
#else
#define BUFFER_LIMIT       4
#define STATE_BUFFER_LIMIT 8
#endif

#define HEAP_MISS_HIT_LIMIT 100000
#define READ_TIMEOUT        100000

typedef struct {
  queue      node;  // queue for write
  queue      q;     // queue for reqs
  uv_write_t wreq;
  void*      arg;
} SWReqsWrapper;

int32_t     initWQ(queue* wq);
void        destroyWQ(queue* wq);
uv_write_t* allocWReqFromWQ(queue* wq, void* arg);

void freeWReqToWQ(queue* wq, SWReqsWrapper* w);

int32_t transSetReadOption(uv_handle_t* handle);
#ifdef __cplusplus
}
#endif

#endif  // _TD_TRANSPORT_COMM_H
