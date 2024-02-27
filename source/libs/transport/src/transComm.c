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

#include "transComm.h"

#define BUFFER_CAP 4096

static TdThreadOnce transModuleInit = PTHREAD_ONCE_INIT;

static int32_t refMgt;
static int32_t instMgt;
static int32_t transSyncMsgMgt;

void transDestroySyncMsg(void* msg);

int32_t transCompressMsg(char* msg, int32_t len) {
  int32_t        ret = 0;
  int            compHdr = sizeof(STransCompMsg);
  STransMsgHead* pHead = transHeadFromCont(msg);

  char* buf = taosMemoryMalloc(len + compHdr + 8);  // 8 extra bytes
  if (buf == NULL) {
    tError("failed to allocate memory for rpc msg compression, contLen:%d", len);
    ret = len;
    return ret;
  }

  int32_t clen = LZ4_compress_default(msg, buf, len, len + compHdr);
  /*
   * only the compressed size is less than the value of contLen - overhead, the compression is applied
   * The first four bytes is set to 0, the second four bytes are utilized to keep the original length of message
   */
  if (clen > 0 && clen < len - compHdr) {
    STransCompMsg* pComp = (STransCompMsg*)msg;
    pComp->reserved = 0;
    pComp->contLen = htonl(len);
    memcpy(msg + compHdr, buf, clen);

    tDebug("compress rpc msg, before:%d, after:%d", len, clen);
    ret = clen + compHdr;
    pHead->comp = 1;
  } else {
    ret = len;
    pHead->comp = 0;
  }
  taosMemoryFree(buf);
  return ret;
}
int32_t transDecompressMsg(char** msg, int32_t len) {
  STransMsgHead* pHead = (STransMsgHead*)(*msg);
  if (pHead->comp == 0) return 0;

  char* pCont = transContFromHead(pHead);

  STransCompMsg* pComp = (STransCompMsg*)pCont;
  int32_t        oriLen = htonl(pComp->contLen);

  char*          buf = taosMemoryCalloc(1, oriLen + sizeof(STransMsgHead));
  STransMsgHead* pNewHead = (STransMsgHead*)buf;
  int32_t        decompLen = LZ4_decompress_safe(pCont + sizeof(STransCompMsg), (char*)pNewHead->content,
                                          len - sizeof(STransMsgHead) - sizeof(STransCompMsg), oriLen);
  memcpy((char*)pNewHead, (char*)pHead, sizeof(STransMsgHead));

  pNewHead->msgLen = htonl(oriLen + sizeof(STransMsgHead));

  taosMemoryFree(pHead);
  *msg = buf;
  if (decompLen != oriLen) {
    return -1;
  }
  return 0;
}

void transFreeMsg(void* msg) {
  if (msg == NULL) {
    return;
  }
  tTrace("rpc free cont:%p", (char*)msg - TRANS_MSG_OVERHEAD);
  taosMemoryFree((char*)msg - sizeof(STransMsgHead));
}
int transSockInfo2Str(struct sockaddr* sockname, char* dst) {
  struct sockaddr_in addr = *(struct sockaddr_in*)sockname;

  char buf[20] = {0};
  int  r = uv_ip4_name(&addr, (char*)buf, sizeof(buf));
  sprintf(dst, "%s:%d", buf, ntohs(addr.sin_port));
  return r;
}
int transInitBuffer(SConnBuffer* buf) {
  buf->cap = BUFFER_CAP;
  buf->buf = taosMemoryCalloc(1, BUFFER_CAP);
  buf->left = -1;
  buf->len = 0;
  buf->total = 0;
  buf->invalid = 0;
  return 0;
}
int transDestroyBuffer(SConnBuffer* p) {
  taosMemoryFree(p->buf);
  p->buf = NULL;
  return 0;
}

int transClearBuffer(SConnBuffer* buf) {
  SConnBuffer* p = buf;
  if (p->cap > BUFFER_CAP) {
    p->cap = BUFFER_CAP;
    p->buf = taosMemoryRealloc(p->buf, BUFFER_CAP);
  }
  p->left = -1;
  p->len = 0;
  p->total = 0;
  p->invalid = 0;
  return 0;
}

int transDumpFromBuffer(SConnBuffer* connBuf, char** buf) {
  static const int HEADSIZE = sizeof(STransMsgHead);

  SConnBuffer* p = connBuf;
  if (p->left != 0 || p->total <= 0) {
    return -1;
  }
  int total = p->total;
  if (total >= HEADSIZE && !p->invalid) {
    *buf = taosMemoryCalloc(1, total);
    memcpy(*buf, p->buf, total);
    if (transResetBuffer(connBuf) < 0) {
      return -1;
    }
  } else {
    total = -1;
  }
  return total;
}

int transResetBuffer(SConnBuffer* connBuf) {
  SConnBuffer* p = connBuf;
  if (p->total < p->len) {
    int left = p->len - p->total;
    memmove(p->buf, p->buf + p->total, left);
    p->left = -1;
    p->total = 0;
    p->len = left;
  } else if (p->total == p->len) {
    p->left = -1;
    p->total = 0;
    p->len = 0;
  } else {
    ASSERTS(0, "invalid read from sock buf");
    return -1;
  }
  return 0;
}
int transAllocBuffer(SConnBuffer* connBuf, uv_buf_t* uvBuf) {
  /*
   * formate of data buffer:
   * |<--------------------------data from socket------------------------------->|
   * |<------STransMsgHead------->|<-------------------userdata--------------->|<-----auth data----->|<----user
   * info--->|
   */
  SConnBuffer* p = connBuf;
  uvBuf->base = p->buf + p->len;
  if (p->left == -1) {
    uvBuf->len = p->cap - p->len;
  } else {
    if (p->left < p->cap - p->len) {
      uvBuf->len = p->left;
    } else {
      p->cap = p->left + p->len;
      p->buf = taosMemoryRealloc(p->buf, p->cap);
      uvBuf->base = p->buf + p->len;
      uvBuf->len = p->left;
    }
  }
  return 0;
}
// check whether already read complete
bool transReadComplete(SConnBuffer* connBuf) {
  SConnBuffer* p = connBuf;
  if (p->len >= sizeof(STransMsgHead)) {
    if (p->left == -1) {
      STransMsgHead head;
      memcpy((char*)&head, connBuf->buf, sizeof(head));
      int32_t msgLen = (int32_t)htonl(head.msgLen);
      p->total = msgLen;
      p->invalid = TRANS_NOVALID_PACKET(htonl(head.magicNum)) || head.version != TRANS_VER;
    }
    if (p->total >= p->len) {
      p->left = p->total - p->len;
    } else {
      p->left = 0;
    }
  }
  return (p->left == 0 || p->invalid) ? true : false;
}

int transSetConnOption(uv_tcp_t* stream, int keepalive) {
#if defined(WINDOWS) || defined(DARWIN)
#else
  uv_tcp_keepalive(stream, 1, keepalive);
#endif
  return uv_tcp_nodelay(stream, 1);
  // int ret = uv_tcp_keepalive(stream, 5, 60);
}

SAsyncPool* transAsyncPoolCreate(uv_loop_t* loop, int sz, void* arg, AsyncCB cb) {
  SAsyncPool* pool = taosMemoryCalloc(1, sizeof(SAsyncPool));
  pool->nAsync = sz;
  pool->asyncs = taosMemoryCalloc(1, sizeof(uv_async_t) * pool->nAsync);

  int i = 0, err = 0;
  for (i = 0; i < pool->nAsync; i++) {
    uv_async_t* async = &(pool->asyncs[i]);

    SAsyncItem* item = taosMemoryCalloc(1, sizeof(SAsyncItem));
    item->pThrd = arg;
    QUEUE_INIT(&item->qmsg);
    taosThreadMutexInit(&item->mtx, NULL);

    async->data = item;
    err = uv_async_init(loop, async, cb);
    if (err != 0) {
      tError("failed to init async, reason:%s", uv_err_name(err));
      break;
    }
  }

  if (i != pool->nAsync) {
    transAsyncPoolDestroy(pool);
    pool = NULL;
  }

  return pool;
}

void transAsyncPoolDestroy(SAsyncPool* pool) {
  for (int i = 0; i < pool->nAsync; i++) {
    uv_async_t* async = &(pool->asyncs[i]);
    SAsyncItem* item = async->data;
    if (item == NULL) continue;

    taosThreadMutexDestroy(&item->mtx);
    taosMemoryFree(item);
  }
  taosMemoryFree(pool->asyncs);
  taosMemoryFree(pool);
}
bool transAsyncPoolIsEmpty(SAsyncPool* pool) {
  for (int i = 0; i < pool->nAsync; i++) {
    uv_async_t* async = &(pool->asyncs[i]);
    SAsyncItem* item = async->data;
    if (!QUEUE_IS_EMPTY(&item->qmsg)) return false;
  }
  return true;
}
int transAsyncSend(SAsyncPool* pool, queue* q) {
  if (atomic_load_8(&pool->stop) == 1) {
    return -1;
  }
  int idx = pool->index % pool->nAsync;

  // no need mutex here
  if (pool->index++ > pool->nAsync * 2000) {
    pool->index = 0;
  }
  uv_async_t* async = &(pool->asyncs[idx]);
  SAsyncItem* item = async->data;

  taosThreadMutexLock(&item->mtx);
  QUEUE_PUSH(&item->qmsg, q);
  taosThreadMutexUnlock(&item->mtx);
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
  if (ctx->freeFunc) ctx->freeFunc(ctx->brokenVal.val);
  taosHashCleanup(ctx->args);
  ctx->args = NULL;
}

void transCtxMerge(STransCtx* dst, STransCtx* src) {
  if (src->args == NULL || src->freeFunc == NULL) {
    return;
  }
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

    // STransCtxVal* dVal = taosHashGet(dst->args, key, klen);
    // if (dVal) {
    //   dst->freeFunc(dVal->val);
    // }
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

void transReqQueueInit(queue* q) {
  // init req queue
  QUEUE_INIT(q);
}
void* transReqQueuePush(queue* q) {
  STransReq* req = taosMemoryCalloc(1, sizeof(STransReq));
  req->wreq.data = req;
  QUEUE_PUSH(q, &req->q);
  return &req->wreq;
}
void* transReqQueueRemove(void* arg) {
  void*       ret = NULL;
  uv_write_t* wreq = arg;

  STransReq* req = wreq ? wreq->data : NULL;
  if (req == NULL) return NULL;
  QUEUE_REMOVE(&req->q);

  ret = wreq && wreq->handle ? wreq->handle->data : NULL;
  taosMemoryFree(req);

  return ret;
}
void transReqQueueClear(queue* q) {
  while (!QUEUE_IS_EMPTY(q)) {
    queue* h = QUEUE_HEAD(q);
    QUEUE_REMOVE(h);
    STransReq* req = QUEUE_DATA(h, STransReq, q);
    taosMemoryFree(req);
  }
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

static FORCE_INLINE int32_t timeCompare(const HeapNode* a, const HeapNode* b) {
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
  int64_t  current = taosGetTimestampMs();
  do {
    HeapNode* minNode = heapMin(queue->heap);
    if (minNode == NULL) break;
    SDelayTask* task = container_of(minNode, SDelayTask, node);
    if (task->execTime <= current) {
      heapRemove(queue->heap, minNode);
      task->func(task->arg);
      taosMemoryFree(task);
      timeout = 0;
    } else {
      timeout = task->execTime - current;
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

void transDQDestroy(SDelayQueue* queue, void (*freeFunc)(void* arg)) {
  taosMemoryFree(queue->timer);

  while (heapSize(queue->heap) > 0) {
    HeapNode* minNode = heapMin(queue->heap);
    if (minNode == NULL) {
      return;
    }
    heapRemove(queue->heap, minNode);

    SDelayTask* task = container_of(minNode, SDelayTask, node);

    STaskArg* arg = task->arg;
    if (freeFunc) freeFunc(arg);
    taosMemoryFree(arg);

    taosMemoryFree(task);
  }
  heapDestroy(queue->heap);
  taosMemoryFree(queue);
}
void transDQCancel(SDelayQueue* queue, SDelayTask* task) {
  uv_timer_stop(queue->timer);

  if (heapSize(queue->heap) <= 0) {
    taosMemoryFree(task->arg);
    taosMemoryFree(task);
    return;
  }
  heapRemove(queue->heap, &task->node);

  taosMemoryFree(task->arg);
  taosMemoryFree(task);

  if (heapSize(queue->heap) != 0) {
    HeapNode* minNode = heapMin(queue->heap);
    if (minNode == NULL) return;

    uint64_t    now = taosGetTimestampMs();
    SDelayTask* task = container_of(minNode, SDelayTask, node);
    uint64_t    timeout = now > task->execTime ? now - task->execTime : 0;

    uv_timer_start(queue->timer, transDQTimeout, timeout, 0);
  }
}

SDelayTask* transDQSched(SDelayQueue* queue, void (*func)(void* arg), void* arg, uint64_t timeoutMs) {
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

  tTrace("timer %p put task into delay queue, timeoutMs:%" PRIu64, queue->timer, timeoutMs);
  heapInsert(queue->heap, &task->node);
  uv_timer_start(queue->timer, transDQTimeout, timeoutMs, 0);
  return task;
}

void transPrintEpSet(SEpSet* pEpSet) {
  if (pEpSet == NULL) {
    tTrace("NULL epset");
    return;
  }
  char buf[512] = {0};
  int  len = snprintf(buf, sizeof(buf), "epset:{");
  for (int i = 0; i < pEpSet->numOfEps; i++) {
    if (i == pEpSet->numOfEps - 1) {
      len += snprintf(buf + len, sizeof(buf) - len, "%d. %s:%d", i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
    } else {
      len += snprintf(buf + len, sizeof(buf) - len, "%d. %s:%d, ", i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
    }
  }
  len += snprintf(buf + len, sizeof(buf) - len, "}");
  tTrace("%s, inUse:%d", buf, pEpSet->inUse);
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
bool transEpSetIsEqual2(SEpSet* a, SEpSet* b) {
  if (a->numOfEps != b->numOfEps) {
    return false;
  }
  for (int i = 0; i < a->numOfEps; i++) {
    if (strncmp(a->eps[i].fqdn, b->eps[i].fqdn, TSDB_FQDN_LEN) != 0 || a->eps[i].port != b->eps[i].port) {
      return false;
    }
  }
  return true;
}

static void transInitEnv() {
  refMgt = transOpenRefMgt(50000, transDestroyExHandle);
  instMgt = taosOpenRef(50, rpcCloseImpl);
  transSyncMsgMgt = taosOpenRef(50, transDestroySyncMsg);
  uv_os_setenv("UV_TCP_SINGLE_ACCEPT", "1");
}
static void transDestroyEnv() {
  transCloseRefMgt(refMgt);
  transCloseRefMgt(instMgt);
  transCloseRefMgt(transSyncMsgMgt);
}

void transInit() {
  // init env
  taosThreadOnce(&transModuleInit, transInitEnv);
}

int32_t transGetRefMgt() { return refMgt; }
int32_t transGetInstMgt() { return instMgt; }
int32_t transGetSyncMsgMgt() { return transSyncMsgMgt; }

void transCleanup() {
  // clean env
  transDestroyEnv();
}
int32_t transOpenRefMgt(int size, void (*func)(void*)) {
  // added into once later
  return taosOpenRef(size, func);
}
void transCloseRefMgt(int32_t mgt) {
  // close ref
  taosCloseRef(mgt);
}
int64_t transAddExHandle(int32_t refMgt, void* p) {
  // acquire extern handle
  return taosAddRef(refMgt, p);
}
int32_t transRemoveExHandle(int32_t refMgt, int64_t refId) {
  // acquire extern handle
  return taosRemoveRef(refMgt, refId);
}

void* transAcquireExHandle(int32_t refMgt, int64_t refId) {
  // acquire extern handle
  return (void*)taosAcquireRef(refMgt, refId);
}

int32_t transReleaseExHandle(int32_t refMgt, int64_t refId) {
  // release extern handle
  return taosReleaseRef(refMgt, refId);
}
void transDestroyExHandle(void* handle) {
  if (handle == NULL) {
    return;
  }
  SExHandle* eh = handle;
  if (!QUEUE_IS_EMPTY(&eh->q)) {
    tDebug("handle %p mem leak", handle);
  }
  taosMemoryFree(handle);
}

void transDestroySyncMsg(void* msg) {
  if (msg == NULL) return;

  STransSyncMsg* pSyncMsg = msg;
  tsem_destroy(pSyncMsg->pSem);
  taosMemoryFree(pSyncMsg->pSem);
  transFreeMsg(pSyncMsg->pRsp->pCont);
  taosMemoryFree(pSyncMsg->pRsp);
  taosMemoryFree(pSyncMsg);
}

// void subnetIp2int(const char* const ip_addr, uint8_t* dst) {
//   char ip_addr_cpy[20];
//   char ip[5];

//   tstrncpy(ip_addr_cpy, ip_addr, sizeof(ip_addr_cpy));

//   char *s_start, *s_end;
//   s_start = ip_addr_cpy;
//   s_end = ip_addr_cpy;

//   int32_t k = 0;

//   for (k = 0; *s_start != '\0'; s_start = s_end) {
//     for (s_end = s_start; *s_end != '.' && *s_end != '\0'; s_end++) {
//     }
//     if (*s_end == '.') {
//       *s_end = '\0';
//       s_end++;
//     }
//     dst[k++] = (char)atoi(s_start);
//   }
// }

uint32_t subnetIpRang2Int(SIpV4Range* pRange) {
  uint32_t ip = pRange->ip;
  return ((ip & 0xFF) << 24) | ((ip & 0xFF00) << 8) | ((ip & 0xFF0000) >> 8) | ((ip >> 24) & 0xFF);
}
int32_t subnetInit(SubnetUtils* pUtils, SIpV4Range* pRange) {
  if (pRange->mask == 32) {
    pUtils->type = 0;
    pUtils->address = pRange->ip;
    return 0;
  }
  pUtils->address = subnetIpRang2Int(pRange);

  for (int i = 0; i < pRange->mask; i++) {
    pUtils->netmask |= (1 << (31 - i));
  }

  pUtils->network = pUtils->address & pUtils->netmask;
  pUtils->broadcast = (pUtils->network) | (pUtils->netmask ^ 0xFFFFFFFF);
  pUtils->type = (pRange->mask == 32 ? 0 : 1);

  return 0;
}
int32_t subnetDebugInfoToBuf(SubnetUtils* pUtils, char* buf) {
  sprintf(buf, "raw: %s, address: %d,  netmask:%d, network:%d, broadcast:%d", pUtils->info, pUtils->address,
          pUtils->netmask, pUtils->network, pUtils->broadcast);
  return 0;
}
int32_t subnetCheckIp(SubnetUtils* pUtils, uint32_t ip) {
  // impl later
  if (pUtils == NULL) return false;
  if (pUtils->type == 0) {
    return pUtils->address == ip;
  } else {
    SIpV4Range range = {.ip = ip, .mask = 32};

    uint32_t t = subnetIpRang2Int(&range);
    return t >= pUtils->network && t <= pUtils->broadcast;
  }
}

int32_t transUtilSIpRangeToStr(SIpV4Range* pRange, char* buf) {
  int32_t len = 0;

  struct in_addr addr;
  addr.s_addr = pRange->ip;

  uv_inet_ntop(AF_INET, &addr, buf, 32);

  len = strlen(buf);

  if (pRange->mask != 32) {
    len += sprintf(buf + len, "/%d", pRange->mask);
  }
  buf[len] = 0;
  return len;
}

int32_t transUtilSWhiteListToStr(SIpWhiteList* pList, char** ppBuf) {
  if (pList->num == 0) {
    *ppBuf = NULL;
    return 0;
  }
  int32_t len = 0;
  char*   pBuf = taosMemoryCalloc(1, pList->num * 36);

  for (int i = 0; i < pList->num; i++) {
    SIpV4Range* pRange = &pList->pIpRange[i];

    char tbuf[32] = {0};
    int  tlen = transUtilSIpRangeToStr(pRange, tbuf);
    len += sprintf(pBuf + len, "%s,", tbuf);
  }
  if (len > 0) {
    pBuf[len - 1] = 0;
  }

  *ppBuf = pBuf;
  return len;
}
