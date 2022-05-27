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

// static TdThreadOnce transModuleInit = PTHREAD_ONCE_INIT;

int transAuthenticateMsg(void* pMsg, int msgLen, void* pAuth, void* pKey) {
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

void transBuildAuthHead(void* pMsg, int msgLen, void* pAuth, void* pKey) {
  T_MD5_CTX context;

  tMD5Init(&context);
  tMD5Update(&context, (uint8_t*)pKey, TSDB_PASSWORD_LEN);
  tMD5Update(&context, (uint8_t*)pMsg, msgLen);
  tMD5Update(&context, (uint8_t*)pKey, TSDB_PASSWORD_LEN);
  tMD5Final(&context);

  memcpy(pAuth, context.digest, sizeof(context.digest));
}

bool transCompressMsg(char* msg, int32_t len, int32_t* flen) {
  return false;
  // SRpcHead* pHead = rpcHeadFromCont(pCont);
  bool succ = false;
  int  overhead = sizeof(STransCompMsg);
  if (!NEEDTO_COMPRESSS_MSG(len)) {
    return succ;
  }

  char* buf = taosMemoryMalloc(len + overhead + 8);  // 8 extra bytes
  if (buf == NULL) {
    tError("failed to allocate memory for rpc msg compression, contLen:%d", len);
    *flen = len;
    return succ;
  }

  int32_t clen = LZ4_compress_default(msg, buf, len, len + overhead);
  tDebug("compress rpc msg, before:%d, after:%d, overhead:%d", len, clen, overhead);
  /*
   * only the compressed size is less than the value of contLen - overhead, the compression is applied
   * The first four bytes is set to 0, the second four bytes are utilized to keep the original length of message
   */
  if (clen > 0 && clen < len - overhead) {
    STransCompMsg* pComp = (STransCompMsg*)msg;
    pComp->reserved = 0;
    pComp->contLen = htonl(len);
    memcpy(msg + overhead, buf, clen);

    tDebug("compress rpc msg, before:%d, after:%d", len, clen);
    *flen = clen + overhead;
    succ = true;
  } else {
    *flen = len;
    succ = false;
  }
  taosMemoryFree(buf);
  return succ;
}
bool transDecompressMsg(char* msg, int32_t len, int32_t* flen) {
  // impl later
  return false;
  STransCompMsg* pComp = (STransCompMsg*)msg;

  int overhead = sizeof(STransCompMsg);
  int clen = 0;
  return false;
}

void transFreeMsg(void* msg) {
  if (msg == NULL) {
    return;
  }
  taosMemoryFree((char*)msg - sizeof(STransMsgHead));
}

int transInitBuffer(SConnBuffer* buf) {
  transClearBuffer(buf);
  return 0;
}
int transClearBuffer(SConnBuffer* buf) {
  memset(buf, 0, sizeof(*buf));
  buf->total = -1;
  return 0;
}
int transAllocBuffer(SConnBuffer* connBuf, uv_buf_t* uvBuf) {
  /*
   * formate of data buffer:
   * |<--------------------------data from socket------------------------------->|
   * |<------STransMsgHead------->|<-------------------userdata--------------->|<-----auth data----->|<----user
   * info--->|
   */
  static const int CAPACITY = sizeof(STransMsgHead);

  SConnBuffer* p = connBuf;
  if (p->cap == 0) {
    p->buf = (char*)taosMemoryCalloc(CAPACITY, sizeof(char));
    p->len = 0;
    p->cap = CAPACITY;
    p->total = -1;

    uvBuf->base = p->buf;
    uvBuf->len = CAPACITY;
  } else if (p->total == -1 && p->len < CAPACITY) {
    uvBuf->base = p->buf + p->len;
    uvBuf->len = CAPACITY - p->len;
  } else {
    p->cap = p->total;
    p->buf = taosMemoryRealloc(p->buf, p->cap);
    tTrace("internal malloc mem: %p, size: %d", p->buf, p->cap);

    uvBuf->base = p->buf + p->len;
    uvBuf->len = p->cap - p->len;
  }
  return 0;
}
// check whether already read complete
bool transReadComplete(SConnBuffer* connBuf) {
  if (connBuf->total == -1 && connBuf->len >= sizeof(STransMsgHead)) {
    STransMsgHead head;
    memcpy((char*)&head, connBuf->buf, sizeof(head));
    int32_t msgLen = (int32_t)htonl(head.msgLen);
    connBuf->total = msgLen;
  }
  if (connBuf->len == connBuf->cap && connBuf->total == connBuf->cap) {
    return true;
  }
  return false;
}
int transPackMsg(STransMsgHead* msgHead, bool sercured, bool auth) { return 0; }

int transUnpackMsg(STransMsgHead* msgHead) { return 0; }
int transDestroyBuffer(SConnBuffer* buf) {
  if (buf->cap > 0) {
    taosMemoryFreeClear(buf->buf);
  }
  transClearBuffer(buf);

  return 0;
}

int transSetConnOption(uv_tcp_t* stream) {
  uv_tcp_nodelay(stream, 1);
  int ret = uv_tcp_keepalive(stream, 5, 5);
  return ret;
}

SAsyncPool* transCreateAsyncPool(uv_loop_t* loop, int sz, void* arg, AsyncCB cb) {
  SAsyncPool* pool = taosMemoryCalloc(1, sizeof(SAsyncPool));
  pool->index = 0;
  pool->nAsync = sz;
  pool->asyncs = taosMemoryCalloc(1, sizeof(uv_async_t) * pool->nAsync);

  for (int i = 0; i < pool->nAsync; i++) {
    uv_async_t* async = &(pool->asyncs[i]);
    uv_async_init(loop, async, cb);

    SAsyncItem* item = taosMemoryCalloc(1, sizeof(SAsyncItem));
    item->pThrd = arg;
    QUEUE_INIT(&item->qmsg);
    taosThreadMutexInit(&item->mtx, NULL);

    async->data = item;
  }
  return pool;
}

void transDestroyAsyncPool(SAsyncPool* pool) {
  for (int i = 0; i < pool->nAsync; i++) {
    uv_async_t* async = &(pool->asyncs[i]);
    // uv_close((uv_handle_t*)async, NULL);
    SAsyncItem* item = async->data;
    taosThreadMutexDestroy(&item->mtx);
    taosMemoryFree(item);
  }
  taosMemoryFree(pool->asyncs);
  taosMemoryFree(pool);
}
int transSendAsync(SAsyncPool* pool, queue* q) {
  int idx = pool->index;
  idx = idx % pool->nAsync;
  // no need mutex here
  if (pool->index++ > pool->nAsync) {
    pool->index = 0;
  }
  uv_async_t* async = &(pool->asyncs[idx]);
  SAsyncItem* item = async->data;

  int64_t st = taosGetTimestampUs();
  taosThreadMutexLock(&item->mtx);
  QUEUE_PUSH(&item->qmsg, q);
  taosThreadMutexUnlock(&item->mtx);
  int64_t el = taosGetTimestampUs() - st;
  if (el > 50) {
    // tInfo("lock and unlock cost: %d", (int)el);
  }
  return uv_async_send(async);
}

void transCtxInit(STransCtx* ctx) {
  // init transCtx
  ctx->args = taosHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UINT), true, HASH_NO_LOCK);
}
void transCtxCleanup(STransCtx* ctx) {
  if (ctx->args == NULL) {
    return;
  }

  STransCtxVal* iter = taosHashIterate(ctx->args, NULL);
  while (iter) {
    ctx->freeFunc(iter->val);
    iter = taosHashIterate(ctx->args, iter);
  }

  taosHashCleanup(ctx->args);
  ctx->args = NULL;
}

void transCtxMerge(STransCtx* dst, STransCtx* src) {
  if (dst->args == NULL) {
    dst->args = src->args;
    dst->brokenVal = src->brokenVal;
    dst->freeFunc = src->freeFunc;
    src->args = NULL;
    return;
  }
  void*  key = NULL;
  size_t klen = 0;
  void*  iter = taosHashIterate(src->args, NULL);
  while (iter) {
    STransCtxVal* sVal = (STransCtxVal*)iter;
    key = taosHashGetKey(sVal, &klen);

    STransCtxVal* dVal = taosHashGet(dst->args, key, klen);
    if (dVal) {
      dst->freeFunc(dVal->val);
    }
    taosHashPut(dst->args, key, klen, sVal, sizeof(*sVal));
    iter = taosHashIterate(src->args, iter);
  }
  taosHashCleanup(src->args);
}
void* transCtxDumpVal(STransCtx* ctx, int32_t key) {
  if (ctx->args == NULL) {
    return NULL;
  }
  STransCtxVal* cVal = taosHashGet(ctx->args, (const void*)&key, sizeof(key));
  if (cVal == NULL) {
    return NULL;
  }
  void* ret = NULL;
  (*cVal->clone)(cVal->val, &ret);
  return ret;
}
void* transCtxDumpBrokenlinkVal(STransCtx* ctx, int32_t* msgType) {
  void* ret = NULL;
  if (ctx->brokenVal.clone == NULL) {
    return ret;
  }
  (*ctx->brokenVal.clone)(ctx->brokenVal.val, &ret);

  *msgType = ctx->brokenVal.msgType;

  return ret;
}

void transQueueInit(STransQueue* queue, void (*freeFunc)(const void* arg)) {
  queue->q = taosArrayInit(2, sizeof(void*));
  queue->freeFunc = (void (*)(const void*))freeFunc;
}
bool transQueuePush(STransQueue* queue, void* arg) {
  if (queue->q == NULL) {
    return true;
  }
  taosArrayPush(queue->q, &arg);
  if (taosArrayGetSize(queue->q) > 1) {
    return false;
  }
  return true;
}
void* transQueuePop(STransQueue* queue) {
  if (queue->q == NULL || taosArrayGetSize(queue->q) == 0) {
    return NULL;
  }
  void* ptr = taosArrayGetP(queue->q, 0);
  taosArrayRemove(queue->q, 0);
  return ptr;
}
int32_t transQueueSize(STransQueue* queue) {
  if (queue->q == NULL) {
    return 0;
  }
  return taosArrayGetSize(queue->q);
}
void* transQueueGet(STransQueue* queue, int i) {
  if (queue->q == NULL || taosArrayGetSize(queue->q) == 0) {
    return NULL;
  }
  if (i >= taosArrayGetSize(queue->q)) {
    return NULL;
  }

  void* ptr = taosArrayGetP(queue->q, i);
  return ptr;
}

void* transQueueRm(STransQueue* queue, int i) {
  if (queue->q == NULL || taosArrayGetSize(queue->q) == 0) {
    return NULL;
  }
  if (i >= taosArrayGetSize(queue->q)) {
    return NULL;
  }
  void* ptr = taosArrayGetP(queue->q, i);
  taosArrayRemove(queue->q, i);
  return ptr;
}

bool transQueueEmpty(STransQueue* queue) {
  if (queue->q == NULL) {
    return true;
  }
  return taosArrayGetSize(queue->q) == 0;
}
void transQueueClear(STransQueue* queue) {
  if (queue->freeFunc != NULL) {
    for (int i = 0; i < taosArrayGetSize(queue->q); i++) {
      void* p = taosArrayGetP(queue->q, i);
      queue->freeFunc(p);
    }
  }
  taosArrayClear(queue->q);
}
void transQueueDestroy(STransQueue* queue) {
  transQueueClear(queue);
  taosArrayDestroy(queue->q);
}

static int32_t timeCompare(const HeapNode* a, const HeapNode* b) {
  SDelayTask* arg1 = container_of(a, SDelayTask, node);
  SDelayTask* arg2 = container_of(b, SDelayTask, node);
  if (arg1->execTime > arg2->execTime) {
    return 0;
  } else {
    return 1;
  }
}

static void transDQTimeout(uv_timer_t* timer) {
  SDelayQueue* queue = timer->data;
  tTrace("timer %p timeout", timer);
  uint64_t timeout = 0;
  do {
    HeapNode* minNode = heapMin(queue->heap);
    if (minNode == NULL) break;
    SDelayTask* task = container_of(minNode, SDelayTask, node);
    if (task->execTime <= taosGetTimestampMs()) {
      heapRemove(queue->heap, minNode);
      task->func(task->arg);
      taosMemoryFree(task);
      timeout = 0;
    } else {
      timeout = task->execTime - taosGetTimestampMs();
      break;
    }
  } while (1);
  if (timeout != 0) {
    uv_timer_start(queue->timer, transDQTimeout, timeout, 0);
  }
}
int transDQCreate(uv_loop_t* loop, SDelayQueue** queue) {
  uv_timer_t* timer = taosMemoryCalloc(1, sizeof(uv_timer_t));
  uv_timer_init(loop, timer);

  Heap* heap = heapCreate(timeCompare);

  SDelayQueue* q = taosMemoryCalloc(1, sizeof(SDelayQueue));
  q->heap = heap;
  q->timer = timer;
  q->loop = loop;
  q->timer->data = q;

  *queue = q;
  return 0;
}

void transDQDestroy(SDelayQueue* queue) {
  taosMemoryFree(queue->timer);

  while (heapSize(queue->heap) > 0) {
    HeapNode* minNode = heapMin(queue->heap);
    if (minNode == NULL) {
      return;
    }
    heapRemove(queue->heap, minNode);

    SDelayTask* task = container_of(minNode, SDelayTask, node);
    taosMemoryFree(task);
  }
  heapDestroy(queue->heap);
  taosMemoryFree(queue);
}

int transDQSched(SDelayQueue* queue, void (*func)(void* arg), void* arg, uint64_t timeoutMs) {
  uint64_t    now = taosGetTimestampMs();
  SDelayTask* task = taosMemoryCalloc(1, sizeof(SDelayTask));
  task->func = func;
  task->arg = arg;
  task->execTime = now + timeoutMs;

  HeapNode* minNode = heapMin(queue->heap);
  if (minNode) {
    SDelayTask* minTask = container_of(minNode, SDelayTask, node);
    if (minTask->execTime < task->execTime) {
      timeoutMs = minTask->execTime <= now ? 0 : minTask->execTime - now;
    }
  }

  tTrace("timer %p put task into queue, timeoutMs: %" PRIu64 "", queue->timer, timeoutMs);
  heapInsert(queue->heap, &task->node);
  uv_timer_start(queue->timer, transDQTimeout, timeoutMs, 0);
  return 0;
}

void transPrintEpSet(SEpSet* pEpSet) {
  if (pEpSet == NULL) {
    tTrace("NULL epset");
    return;
  }
  tTrace("epset begin  inUse: %d", pEpSet->inUse);
  for (int i = 0; i < pEpSet->numOfEps; i++) {
    tTrace("ip: %s, port: %d", pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
  }
  tTrace("epset end");
}
bool transEpSetIsEqual(SEpSet* a, SEpSet* b) {
  if (a->numOfEps != b->numOfEps || a->inUse != b->inUse) {
    return false;
  }
  for (int i = 0; i < a->numOfEps; i++) {
    if (strncmp(a->eps[i].fqdn, b->eps[i].fqdn, TSDB_FQDN_LEN) != 0 || a->eps[i].port != b->eps[i].port) {
      return false;
    }
  }
  return true;
}
#endif
