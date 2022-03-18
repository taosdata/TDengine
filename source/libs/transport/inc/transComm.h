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

typedef struct {
  SRpcInfo* pRpc;     // associated SRpcInfo
  SEpSet    epSet;    // ip list provided by app
  void*     ahandle;  // handle provided by app
  // struct SRpcConn* pConn;     // pConn allocated
  tmsg_t   msgType;  // message type
  uint8_t* pCont;    // content provided by app
  int32_t  contLen;  // content length
  // int32_t  code;     // error code
  // int16_t  numOfTry;  // number of try for different servers
  // int8_t   oldInUse;  // server EP inUse passed by app
  // int8_t   redirect;  // flag to indicate redirect
  int8_t   connType;  // connection type
  int64_t  rid;       // refId returned by taosAddRef
  SRpcMsg* pRsp;      // for synchronous API
  tsem_t*  pSem;      // for synchronous API
  char*    ip;
  uint32_t port;
  // SEpSet*          pSet;      // for synchronous API
} SRpcReqContext;

typedef SRpcMsg      STransMsg;
typedef SRpcInfo     STrans;
typedef SRpcConnInfo STransHandleInfo;

typedef struct {
  SEpSet   epSet;    // ip list provided by app
  void*    ahandle;  // handle provided by app
  tmsg_t   msgType;  // message type
  uint8_t* pCont;    // content provided by app
  int32_t  contLen;  // content length
  // int32_t  code;     // error code
  // int16_t  numOfTry;  // number of try for different servers
  // int8_t   oldInUse;  // server EP inUse passed by app
  // int8_t   redirect;  // flag to indicate redirect
  int8_t  connType;  // connection type cli/srv
  int64_t rid;       // refId returned by taosAddRef

  STransMsg* pRsp;  // for synchronous API
  tsem_t*    pSem;  // for synchronous API

  int      hThrdIdx;
  char*    ip;
  uint32_t port;
  // SEpSet*          pSet;      // for synchronous API
} STransConnCtx;

#pragma pack(push, 1)

typedef struct {
  char version : 4;  // RPC version
  char comp : 4;     // compression algorithm, 0:no compression 1:lz4
  char resflag : 2;  // reserved bits
  char spi : 1;      // security parameter index
  char secured : 2;
  char encrypt : 3;  // encrypt algorithm, 0: no encryption

  uint32_t code;  // del later
  uint32_t msgType;
  int32_t  msgLen;
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

#define container_of(ptr, type, member) ((type*)((char*)(ptr)-offsetof(type, member)))
#define RPC_RESERVE_SIZE (sizeof(STranConnCtx))

#define RPC_MSG_OVERHEAD (sizeof(SRpcHead) + sizeof(SRpcDigest))
#define rpcHeadFromCont(cont) ((SRpcHead*)((char*)cont - sizeof(SRpcHead)))
#define rpcContFromHead(msg) (msg + sizeof(SRpcHead))
#define rpcMsgLenFromCont(contLen) (contLen + sizeof(SRpcHead))
#define rpcContLenFromMsg(msgLen) (msgLen - sizeof(SRpcHead))
#define rpcIsReq(type) (type & 1U)

#define TRANS_RESERVE_SIZE (sizeof(STranConnCtx))

#define TRANS_MSG_OVERHEAD (sizeof(STransMsgHead))
#define transHeadFromCont(cont) ((STransMsgHead*)((char*)cont - sizeof(STransMsgHead)))
#define transContFromHead(msg) (msg + sizeof(STransMsgHead))
#define transMsgLenFromCont(contLen) (contLen + sizeof(STransMsgHead))
#define transContLenFromMsg(msgLen) (msgLen - sizeof(STransMsgHead));
#define transIsReq(type) (type & 1U)

int       rpcAuthenticateMsg(void* pMsg, int msgLen, void* pAuth, void* pKey);
void      rpcBuildAuthHead(void* pMsg, int msgLen, void* pAuth, void* pKey);
int32_t   rpcCompressRpcMsg(char* pCont, int32_t contLen);
SRpcHead* rpcDecompressRpcMsg(SRpcHead* pHead);

int  transAuthenticateMsg(void* pMsg, int msgLen, void* pAuth, void* pKey);
void transBuildAuthHead(void* pMsg, int msgLen, void* pAuth, void* pKey);
bool transCompressMsg(char* msg, int32_t len, int32_t* flen);
bool transDecompressMsg(char* msg, int32_t len, int32_t* flen);

void transConnCtxDestroy(STransConnCtx* ctx);

void transFreeMsg(void* msg);

//
typedef struct SConnBuffer {
  char* buf;
  int   len;
  int   cap;
  int   total;
} SConnBuffer;

typedef void (*AsyncCB)(uv_async_t* handle);

typedef struct {
  void*           pThrd;
  queue           qmsg;
  pthread_mutex_t mtx;  // protect qmsg;
} SAsyncItem;

typedef struct {
  int         index;
  int         nAsync;
  uv_async_t* asyncs;
} SAsyncPool;

SAsyncPool* transCreateAsyncPool(uv_loop_t* loop, int sz, void* arg, AsyncCB cb);
void        transDestroyAsyncPool(SAsyncPool* pool);
int         transSendAsync(SAsyncPool* pool, queue* mq);

int  transInitBuffer(SConnBuffer* buf);
int  transClearBuffer(SConnBuffer* buf);
int  transDestroyBuffer(SConnBuffer* buf);
int  transAllocBuffer(SConnBuffer* connBuf, uv_buf_t* uvBuf);
bool transReadComplete(SConnBuffer* connBuf);

int transSetConnOption(uv_tcp_t* stream);

void transRefSrvHandle(void* handle);
void transUnrefSrvHandle(void* handle);

void transRefCliHandle(void* handle);
void transUnrefCliHandle(void* handle);

void transReleaseCliHandle(void* handle);
void transReleaseSrvHandle(void* handle);

void transSendRequest(void* shandle, const char* ip, uint32_t port, STransMsg* pMsg);
void transSendRecv(void* shandle, const char* ip, uint32_t port, STransMsg* pMsg, STransMsg* pRsp);
void transSendResponse(const STransMsg* pMsg);
int  transGetConnInfo(void* thandle, STransHandleInfo* pInfo);

void* transInitServer(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle);
void* transInitClient(uint32_t ip, uint32_t port, char* label, int numOfThreads, void* fp, void* shandle);

void transCloseClient(void* arg);
void transCloseServer(void* arg);

#endif
