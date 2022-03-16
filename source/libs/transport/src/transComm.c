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

  char* buf = malloc(len + overhead + 8);  // 8 extra bytes
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
  free(buf);
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

void transConnCtxDestroy(STransConnCtx* ctx) {
  free(ctx->ip);
  free(ctx);
}

void transFreeMsg(void* msg) {
  if (msg == NULL) {
    return;
  }
  free((char*)msg - sizeof(STransMsgHead));
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
    p->buf = (char*)calloc(CAPACITY, sizeof(char));
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
    p->buf = realloc(p->buf, p->cap);

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
int transPackMsg(STransMsgHead* msgHead, bool sercured, bool auth) {return 0;}

int transUnpackMsg(STransMsgHead* msgHead) {return 0;}
int transDestroyBuffer(SConnBuffer* buf) {
  if (buf->cap > 0) {
    tfree(buf->buf);
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
  SAsyncPool* pool = calloc(1, sizeof(SAsyncPool));
  pool->index = 0;
  pool->nAsync = sz;
  pool->asyncs = calloc(1, sizeof(uv_async_t) * pool->nAsync);

  for (int i = 0; i < pool->nAsync; i++) {
    uv_async_t* async = &(pool->asyncs[i]);
    uv_async_init(loop, async, cb);

    SAsyncItem* item = calloc(1, sizeof(SAsyncItem));
    item->pThrd = arg;
    QUEUE_INIT(&item->qmsg);
    pthread_mutex_init(&item->mtx, NULL);

    async->data = item;
  }
  return pool;
}
void transDestroyAsyncPool(SAsyncPool* pool) {
  for (int i = 0; i < pool->nAsync; i++) {
    uv_async_t* async = &(pool->asyncs[i]);

    SAsyncItem* item = async->data;
    pthread_mutex_destroy(&item->mtx);
    free(item);
  }
  free(pool->asyncs);
  free(pool);
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
  pthread_mutex_lock(&item->mtx);
  QUEUE_PUSH(&item->qmsg, q);
  pthread_mutex_unlock(&item->mtx);
  int64_t el = taosGetTimestampUs() - st;
  if (el > 50) {
    // tInfo("lock and unlock cost: %d", (int)el);
  }
  return uv_async_send(async);
}

#endif
