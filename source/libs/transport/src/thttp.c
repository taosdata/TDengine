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

#define _DEFAULT_SOURCE
// clang-format off
#include <uv.h>
#include "zlib.h"
#include "thttp.h"
#include "taoserror.h"
#include "transComm.h"

// clang-format on

#define HTTP_RECV_BUF_SIZE 1024

static int32_t httpRefMgt = 0;
static int32_t FAST_FAILURE_LIMIT = 1;

static int64_t httpDefaultChanId = -1;

static int64_t httpSeqNum = 0;

typedef struct SHttpModule {
  uv_loop_t*  loop;
  SAsyncPool* asyncPool;
  TdThread    thread;
  SHashObj*   connStatusTable;
  SHashObj*   connPool;
  int8_t      quit;
  int16_t     connNum;
} SHttpModule;

typedef struct SHttpMsg {
  queue         q;
  char*         server;
  char*         uri;
  int32_t       port;
  char*         cont;
  int32_t       len;
  EHttpCompFlag flag;
  int8_t        quit;
  int64_t       chanId;
  int64_t       seq;
  char*         qid;
} SHttpMsg;

typedef struct SHttpClient {
  uv_connect_t       conn;
  uv_tcp_t           tcp;
  uv_write_t         req;
  uv_buf_t*          wbuf;
  char*              rbuf;
  char*              addr;
  uint16_t           port;
  struct sockaddr_in dest;
  int64_t            chanId;
  int64_t            seq;
} SHttpClient;

typedef struct SHttpConnList {
  queue q;

} SHttpConnList;

static TdThreadOnce transHttpInit = PTHREAD_ONCE_INIT;
static void         transHttpEnvInit();

static void    httpHandleReq(SHttpMsg* msg);
static void    httpHandleQuit(SHttpMsg* msg);
static int32_t httpSendQuit(SHttpModule* http, int64_t chanId);

static int32_t httpCreateMsg(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                             EHttpCompFlag flag, int64_t chanId, const char* qid, SHttpMsg** httpMsg);
static void    httpDestroyMsg(SHttpMsg* msg);

static bool    httpFailFastShoudIgnoreMsg(SHashObj* pTable, char* server, int16_t port);
static void    httpFailFastMayUpdate(SHashObj* pTable, char* server, int16_t port, int8_t succ);
static int32_t taosSendHttpReportImpl(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                                      EHttpCompFlag flag);
static void    httpModuleDestroy(SHttpModule* http);

static int32_t taosSendHttpReportImplByChan(const char* server, const char* uri, uint16_t port, char* pCont,
                                            int32_t contLen, EHttpCompFlag flag, int64_t chanId, const char* qid);

static int32_t taosBuildHttpHeader(const char* server, const char* uri, int32_t contLen, const char* qid, char* pHead,
                                   int32_t headLen,

                                   EHttpCompFlag flag) {
  int32_t code = 0;
  int32_t len = 0;
  if (flag == HTTP_FLAT) {
    if (qid == NULL) {
      len = snprintf(pHead, headLen,
                     "POST %s HTTP/1.1\n"
                     "Host: %s\n"
                     "Content-Type: application/json\n"
                     "Content-Length: %d\n\n",
                     uri, server, contLen);
    } else {
      len = snprintf(pHead, headLen,
                     "POST %s HTTP/1.1\n"
                     "Host: %s\n"
                     "X-QID: %s\n"
                     "Content-Type: application/json\n"
                     "Content-Length: %d\n\n",
                     uri, server, qid, contLen);
    }
    if (len < 0 || len >= headLen) {
      code = TSDB_CODE_OUT_OF_RANGE;
    }
  } else if (flag == HTTP_GZIP) {
    if (qid == NULL) {
      len = snprintf(pHead, headLen,
                     "POST %s HTTP/1.1\n"
                     "Host: %s\n"
                     "Content-Type: application/json\n"
                     "Content-Encoding: gzip\n"
                     "Content-Length: %d\n\n",
                     uri, server, contLen);
    } else {
      len = snprintf(pHead, headLen,
                     "POST %s HTTP/1.1\n"
                     "Host: %s\n"
                     "X-QID: %s\n"
                     "Content-Type: application/json\n"
                     "Content-Encoding: gzip\n"
                     "Content-Length: %d\n\n",
                     uri, server, qid, contLen);
    }
    if (len < 0 || len >= headLen) {
      code = TSDB_CODE_OUT_OF_RANGE;
    }
  } else {
    code = TSDB_CODE_INVALID_PARA;
  }
  return code;
}

static int32_t taosCompressHttpRport(char* pSrc, int32_t srcLen) {
  int32_t code = 0;
  int32_t destLen = srcLen;
  void*   pDest = taosMemoryMalloc(destLen);

  if (pDest == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  z_stream gzipStream = {0};
  gzipStream.zalloc = (alloc_func)0;
  gzipStream.zfree = (free_func)0;
  gzipStream.opaque = (voidpf)0;
  if (deflateInit2(&gzipStream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  gzipStream.next_in = (Bytef*)pSrc;
  gzipStream.avail_in = (uLong)srcLen;
  gzipStream.next_out = (Bytef*)pDest;
  gzipStream.avail_out = (uLong)(destLen);

  while (gzipStream.avail_in != 0 && gzipStream.total_out < (uLong)(destLen)) {
    if (deflate(&gzipStream, Z_FULL_FLUSH) != Z_OK) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _OVER;
    }
  }

  if (gzipStream.avail_in != 0) {
    code = TSDB_CODE_COMPRESS_ERROR;
    goto _OVER;
  }

  int32_t err = 0;
  while (1) {
    if ((err = deflate(&gzipStream, Z_FINISH)) == Z_STREAM_END) {
      break;
    }
    if (err != Z_OK) {
      code = TSDB_CODE_COMPRESS_ERROR;
      goto _OVER;
    }
  }

  if (deflateEnd(&gzipStream) != Z_OK) {
    code = TSDB_CODE_COMPRESS_ERROR;
    goto _OVER;
  }

  if (gzipStream.total_out >= srcLen) {
    code = TSDB_CODE_COMPRESS_ERROR;
    goto _OVER;
  }

  code = 0;

_OVER:
  if (code == 0) {
    memcpy(pSrc, pDest, gzipStream.total_out);
    code = gzipStream.total_out;
  }
  taosMemoryFree(pDest);
  return code;
}

static FORCE_INLINE int32_t taosBuildDstAddr(const char* server, uint16_t port, struct sockaddr_in* dest) {
  uint32_t ip = 0;
  int32_t  code = taosGetIpv4FromFqdn(server, &ip);
  if (code) {
    tError("http-report failed to resolving domain names %s, reason: %s", server, tstrerror(code));
    return TSDB_CODE_RPC_FQDN_ERROR;
  }
  char buf[256] = {0};
  tinet_ntoa(buf, ip);
  int ret = uv_ip4_addr(buf, port, dest);
  if (ret != 0) {
    tError("http-report failed to get addr, reason:%s", uv_err_name(ret));
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  return 0;
}

static void* httpThread(void* arg) {
  SHttpModule* http = (SHttpModule*)arg;
  setThreadName("http-cli-send-thread");
  (void)uv_run(http->loop, UV_RUN_DEFAULT);
  return NULL;
}

static int32_t httpCreateMsg(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                             EHttpCompFlag flag, int64_t chanId, const char* qid, SHttpMsg** httpMsg) {
  int64_t seqNum = atomic_fetch_add_64(&httpSeqNum, 1);
  if (server == NULL || uri == NULL) {
    tError("http-report failed to report to invalid addr, chanId:%" PRId64 ", seq:%" PRId64 "", chanId, seqNum);
    *httpMsg = NULL;
    return TSDB_CODE_INVALID_PARA;
  }

  if (pCont == NULL || contLen == 0) {
    tError("http-report failed to report empty packet, chanId:%" PRId64 ", seq:%" PRId64 "", chanId, seqNum);
    *httpMsg = NULL;
    return TSDB_CODE_INVALID_PARA;
  }

  SHttpMsg* msg = taosMemoryMalloc(sizeof(SHttpMsg));
  if (msg == NULL) {
    *httpMsg = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  msg->seq = seqNum;
  msg->port = port;
  msg->server = taosStrdup(server);
  msg->uri = taosStrdup(uri);
  msg->cont = taosMemoryMalloc(contLen);
  if (qid != NULL)
    msg->qid = taosStrdup(qid);
  else
    msg->qid = NULL;
  if (msg->server == NULL || msg->uri == NULL || msg->cont == NULL) {
    httpDestroyMsg(msg);
    *httpMsg = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  memcpy(msg->cont, pCont, contLen);
  msg->len = contLen;
  msg->flag = flag;
  msg->quit = 0;
  msg->chanId = chanId;
  *httpMsg = msg;
  return 0;
}
static void httpDestroyMsg(SHttpMsg* msg) {
  if (msg == NULL) return;

  taosMemoryFree(msg->server);
  taosMemoryFree(msg->uri);
  taosMemoryFree(msg->cont);
  if (msg->qid != NULL) taosMemoryFree(msg->qid);
  taosMemoryFree(msg);
}
static void httpDestroyMsgWrapper(void* cont, void* param) {
  SHttpMsg* pMsg = cont;
  tWarn("http-report destroy msg, chanId:%" PRId64 ", seq:%" PRId64 "", pMsg->chanId, pMsg->seq);
  httpDestroyMsg(pMsg);
}

static void httpMayDiscardMsg(SHttpModule* http, SAsyncItem* item) {
  SHttpMsg *msg = NULL, *quitMsg = NULL;
  if (atomic_load_8(&http->quit) == 0) {
    return;
  }

  while (!QUEUE_IS_EMPTY(&item->qmsg)) {
    queue* h = QUEUE_HEAD(&item->qmsg);
    QUEUE_REMOVE(h);
    msg = QUEUE_DATA(h, SHttpMsg, q);
    if (!msg->quit) {
      tError("http-report failed to report chanId:%" PRId64 ",seq:%" PRId64 ", reason: %s", msg->chanId, msg->seq,
             tstrerror(TSDB_CODE_HTTP_MODULE_QUIT));
      httpDestroyMsg(msg);
    } else {
      quitMsg = msg;
    }
  }
  if (quitMsg != NULL) {
    QUEUE_PUSH(&item->qmsg, &quitMsg->q);
  }
}
static void httpTrace(queue* q) {
  if (!(rpcDebugFlag & DEBUG_DEBUG) || (QUEUE_IS_EMPTY(q))) {
    return;
  }

  int64_t   startSeq = 0, endSeq = 0;
  SHttpMsg* msg = NULL;

  queue* h = QUEUE_HEAD(q);
  msg = QUEUE_DATA(h, SHttpMsg, q);
  startSeq = msg->seq;

  h = QUEUE_TAIL(q);
  msg = QUEUE_DATA(h, SHttpMsg, q);
  endSeq = msg->seq;

  tDebug("http-report process msg, start_seq:%" PRId64 ", end_seq:%" PRId64 ", max_seq:%" PRId64 "", startSeq, endSeq,
         atomic_load_64(&httpSeqNum) - 1);
}

static void httpAsyncCb(uv_async_t* handle) {
  SAsyncItem*  item = handle->data;
  SHttpModule* http = item->pThrd;

  SHttpMsg *msg = NULL, *quitMsg = NULL;
  queue     wq;
  QUEUE_INIT(&wq);

  static int32_t BATCH_SIZE = 20;
  int32_t        count = 0;

  (void)taosThreadMutexLock(&item->mtx);
  httpMayDiscardMsg(http, item);

  while (!QUEUE_IS_EMPTY(&item->qmsg) && count++ < BATCH_SIZE) {
    queue* h = QUEUE_HEAD(&item->qmsg);
    QUEUE_REMOVE(h);
    QUEUE_PUSH(&wq, h);
  }
  (void)taosThreadMutexUnlock(&item->mtx);

  httpTrace(&wq);

  while (!QUEUE_IS_EMPTY(&wq)) {
    queue* h = QUEUE_HEAD(&wq);
    QUEUE_REMOVE(h);
    msg = QUEUE_DATA(h, SHttpMsg, q);
    if (msg->quit) {
      quitMsg = msg;
    } else {
      httpHandleReq(msg);
    }
  }
  if (quitMsg) httpHandleQuit(quitMsg);
}

static FORCE_INLINE void destroyHttpClient(SHttpClient* cli) {
  taosMemoryFree(cli->wbuf[0].base);
  taosMemoryFree(cli->wbuf[1].base);
  taosMemoryFree(cli->wbuf);
  taosMemoryFree(cli->rbuf);
  taosMemoryFree(cli->addr);
  taosMemoryFree(cli);
}

static FORCE_INLINE void clientCloseCb(uv_handle_t* handle) {
  SHttpClient* cli = handle->data;

  int64_t      chanId = cli->chanId;
  SHttpModule* http = taosAcquireRef(httpRefMgt, cli->chanId);
  if (http != NULL) {
    http->connNum -= 1;
    (void)taosReleaseRef(httpRefMgt, chanId);
  }

  destroyHttpClient(cli);
}

static FORCE_INLINE void clientAllocBuffCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  SHttpClient* cli = handle->data;
  buf->base = cli->rbuf;
  buf->len = HTTP_RECV_BUF_SIZE;
}

static FORCE_INLINE void clientRecvCb(uv_stream_t* handle, ssize_t nread, const uv_buf_t* buf) {
  STUB_RAND_NETWORK_ERR(nread);
  SHttpClient* cli = handle->data;
  if (nread < 0) {
    tError("http-report recv error:%s, seq:%" PRId64 "", uv_strerror(nread), cli->seq);
  } else {
    tTrace("http-report succ to recv %d bytes, seq:%" PRId64 "", (int32_t)nread, cli->seq);
  }
  if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
    uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
  }
}
static void clientSentCb(uv_write_t* req, int32_t status) {
  STUB_RAND_NETWORK_ERR(status);
  SHttpClient* cli = req->data;
  if (status != 0) {
    tError("http-report failed to send data, reason: %s, dst:%s:%d, chanId:%" PRId64 ", seq:%" PRId64 "",
           uv_strerror(status), cli->addr, cli->port, cli->chanId, cli->seq);
    if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
      uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
    }
    return;
  } else {
    tTrace("http-report succ to send data, chanId:%" PRId64 ", seq:%" PRId64 "", cli->chanId, cli->seq);
  }

  status = uv_read_start((uv_stream_t*)&cli->tcp, clientAllocBuffCb, clientRecvCb);
  if (status != 0) {
    tError("http-report failed to recv data,reason:%s, dst:%s:%d, chanId:%" PRId64 ", seq:%" PRId64 "",
           uv_strerror(status), cli->addr, cli->port, cli->chanId, cli->seq);
    if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
      uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
    }
  }
}
static void clientConnCb(uv_connect_t* req, int32_t status) {
  STUB_RAND_NETWORK_ERR(status);
  SHttpClient* cli = req->data;
  int64_t      chanId = cli->chanId;

  SHttpModule* http = taosAcquireRef(httpRefMgt, chanId);
  if (status != 0) {
    httpFailFastMayUpdate(http->connStatusTable, cli->addr, cli->port, 0);
    tError("http-report failed to conn to server, reason:%s, dst:%s:%d, chanId:%" PRId64 ", seq:%" PRId64 "",
           uv_strerror(status), cli->addr, cli->port, chanId, cli->seq);
    if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
      uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
    }
    (void)taosReleaseRef(httpRefMgt, chanId);
    return;
  }
  http->connNum += 1;

  httpFailFastMayUpdate(http->connStatusTable, cli->addr, cli->port, 1);

  status = uv_write(&cli->req, (uv_stream_t*)&cli->tcp, cli->wbuf, 2, clientSentCb);
  if (0 != status) {
    tError("http-report failed to send data,reason:%s, dst:%s:%d, chanId:%" PRId64 ", seq:%" PRId64 "",
           uv_strerror(status), cli->addr, cli->port, chanId, cli->seq);
    if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
      uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
    }
  }
  (void)taosReleaseRef(httpRefMgt, chanId);
}

int32_t httpSendQuit(SHttpModule* http, int64_t chanId) {
  SHttpMsg* msg = taosMemoryCalloc(1, sizeof(SHttpMsg));
  if (msg == NULL) {
    return terrno;
  }
  msg->seq = atomic_fetch_add_64(&httpSeqNum, 1);
  msg->quit = 1;
  msg->chanId = chanId;

  int ret = transAsyncSend(http->asyncPool, &(msg->q));
  if (ret != 0) {
    taosMemoryFree(msg);
    return TSDB_CODE_THIRDPARTY_ERROR;
  }

  return 0;
}

static void httpDestroyClientCb(uv_handle_t* handle) {
  SHttpClient* http = handle->data;
  destroyHttpClient(http);
}
static void httpWalkCb(uv_handle_t* handle, void* arg) {
  // impl later
  if (!uv_is_closing(handle)) {
    uv_handle_type type = uv_handle_get_type(handle);
    if (uv_handle_get_type(handle) == UV_TCP) {
      uv_close(handle, httpDestroyClientCb);
    } else {
      uv_close(handle, NULL);
    }
  }
  return;
}
static void httpHandleQuit(SHttpMsg* msg) {
  int64_t seq = msg->seq;
  int64_t chanId = msg->chanId;
  taosMemoryFree(msg);

  tDebug("http-report receive quit, chanId:%" PRId64 ", seq:%" PRId64 "", chanId, seq);
  SHttpModule* http = taosAcquireRef(httpRefMgt, chanId);
  if (http == NULL) return;
  uv_walk(http->loop, httpWalkCb, NULL);
  (void)taosReleaseRef(httpRefMgt, chanId);
}

static bool httpFailFastShoudIgnoreMsg(SHashObj* pTable, char* server, int16_t port) {
  char buf[256] = {0};
  sprintf(buf, "%s:%d", server, port);

  int32_t* failedTime = (int32_t*)taosHashGet(pTable, buf, strlen(buf));
  if (failedTime == NULL) {
    return false;
  }

  int32_t now = taosGetTimestampSec();
  if (*failedTime > now - FAST_FAILURE_LIMIT) {
    tDebug("http-report succ to ignore msg,reason:connection timed out, dst:%s", buf);
    return true;
  } else {
    return false;
  }
}
static void httpFailFastMayUpdate(SHashObj* pTable, char* server, int16_t port, int8_t succ) {
  int32_t code = 0;
  char buf[256] = {0};
  sprintf(buf, "%s:%d", server, port);

  if (succ) {
    (void)taosHashRemove(pTable, buf, strlen(buf));
  } else {
    int32_t st = taosGetTimestampSec();
    if ((code = taosHashPut(pTable, buf, strlen(buf), &st, sizeof(st))) != 0) {
      tError("http-report failed to update conn status, dst:%s, reason:%s", buf, tstrerror(code));   
    }
  }
  return;
}
static void httpHandleReq(SHttpMsg* msg) {
  int64_t chanId = msg->chanId;
  int32_t ignore = false;
  char*   header = NULL;
  int32_t code = 0;

  SHttpModule* http = taosAcquireRef(httpRefMgt, chanId);
  if (http == NULL) {
    code = terrno;
    goto END;
  }
  if (httpFailFastShoudIgnoreMsg(http->connStatusTable, msg->server, msg->port)) {
    ignore = true;
    goto END;
  }
  struct sockaddr_in dest = {0};
  if (taosBuildDstAddr(msg->server, msg->port, &dest) < 0) {
    goto END;
  }

  if (msg->flag == HTTP_GZIP) {
    int32_t dstLen = taosCompressHttpRport(msg->cont, msg->len);
    if (dstLen > 0) {
      msg->len = dstLen;
    } else {
      msg->flag = HTTP_FLAT;
    }
    if (dstLen < 0) {
      code = dstLen;
      goto END;
    }
  }

  int32_t cap = 2048;
  header = taosMemoryCalloc(1, cap);
  if (header == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }

  int32_t headLen = taosBuildHttpHeader(msg->server, msg->uri, msg->len, msg->qid, header, cap, msg->flag);
  if (headLen < 0) {
    code = headLen;
    goto END;
  }

  uv_buf_t* wb = taosMemoryCalloc(2, sizeof(uv_buf_t));
  if (wb == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }

  wb[0] = uv_buf_init((char*)header, strlen(header));  //  heap var
  wb[1] = uv_buf_init((char*)msg->cont, msg->len);     //  heap var

  SHttpClient* cli = taosMemoryCalloc(1, sizeof(SHttpClient));
  if (cli == NULL) {
    taosMemoryFree(wb);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }
  cli->seq = msg->seq;
  cli->conn.data = cli;
  cli->tcp.data = cli;
  cli->req.data = cli;
  cli->dest = dest;
  cli->chanId = chanId;
  cli->addr = msg->server;
  cli->port = msg->port;
  if (msg->qid != NULL) taosMemoryFree(msg->qid);
  taosMemoryFree(msg->uri);
  taosMemoryFree(msg);

  cli->wbuf = wb;
  cli->rbuf = taosMemoryCalloc(1, HTTP_RECV_BUF_SIZE);
  if (cli->rbuf == NULL) {
    tError("http-report failed to alloc read buf, dst:%s:%d, chanId:%" PRId64 ", seq:%" PRId64 ",reason:%s", cli->addr,
           cli->port, chanId, cli->seq, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    destroyHttpClient(cli);
    (void)taosReleaseRef(httpRefMgt, chanId);
    return;
  }

  int err = uv_tcp_init(http->loop, &cli->tcp);
  if (err != 0) {
    tError("http-report failed to init socket handle, dst:%s:%d, chanId:%" PRId64 ", seq:%" PRId64 ", reason:%s",
           cli->addr, cli->port, chanId, cli->seq, uv_strerror(err));
    destroyHttpClient(cli);
    (void)taosReleaseRef(httpRefMgt, chanId);
    return;
  }

  // set up timeout to avoid stuck;
  int32_t fd = taosCreateSocketWithTimeout(5000);
  if (fd < 0) {
    tError("http-report failed to open socket, dst:%s:%d, chanId:%" PRId64 ", seq:%" PRId64 ", reason:%s", cli->addr,
           cli->port, chanId, cli->seq, tstrerror(terrno));
    destroyHttpClient(cli);
    (void)taosReleaseRef(httpRefMgt, chanId);
    return;
  }

  int ret = uv_tcp_open((uv_tcp_t*)&cli->tcp, fd);
  if (ret != 0) {
    tError("http-report failed to open socket, dst:%s:%d, chanId:%" PRId64 ", seq:%" PRId64 ", reason:%s", cli->addr,
           cli->port, chanId, cli->seq, uv_strerror(ret));

    destroyHttpClient(cli);
    (void)taosReleaseRef(httpRefMgt, chanId);
    return;
  }

  ret = uv_tcp_connect(&cli->conn, &cli->tcp, (const struct sockaddr*)&cli->dest, clientConnCb);
  if (ret != 0) {
    tError("http-report failed to connect to http-server,dst:%s:%d, chanId:%" PRId64 ", seq:%" PRId64 ", reson:%s",
           cli->addr, cli->port, chanId, cli->seq, uv_strerror(ret));
    httpFailFastMayUpdate(http->connStatusTable, cli->addr, cli->port, 0);
    destroyHttpClient(cli);
  }
  (void)taosReleaseRef(httpRefMgt, chanId);
  return;

END:
  if (ignore == false) {
    tError("http-report failed to report to addr: %s:%d, chanId:%" PRId64 ",seq:%" PRId64 " reason:%s", msg->server,
           msg->port, chanId, msg->seq, tstrerror(code));
  }
  httpDestroyMsg(msg);
  taosMemoryFree(header);
  (void)taosReleaseRef(httpRefMgt, chanId);
}

static void httpModuleDestroy(SHttpModule* http) {
  if (http == NULL) return;

  if (http->asyncPool != NULL) {
    TRANS_DESTROY_ASYNC_POOL_MSG(http->asyncPool, SHttpMsg, httpDestroyMsgWrapper, NULL);
    transAsyncPoolDestroy(http->asyncPool);
  }
  if (http->loop) {
    (void)uv_loop_close(http->loop);
    taosMemoryFree(http->loop);
  }

  taosHashCleanup(http->connStatusTable);
  // not free http, http freeed by ref
}

void httpModuleDestroy2(SHttpModule* http) {
  if (http == NULL) return;
  httpModuleDestroy(http);
  taosMemoryFree(http);
}

static int32_t taosSendHttpReportImplByChan(const char* server, const char* uri, uint16_t port, char* pCont,
                                            int32_t contLen, EHttpCompFlag flag, int64_t chanId, const char* qid) {
  SHttpModule* load = NULL;
  SHttpMsg*    msg = NULL;
  int32_t      code = httpCreateMsg(server, uri, port, pCont, contLen, flag, chanId, qid, &msg);
  if (code != 0) {
    goto _ERROR;
  }

  load = taosAcquireRef(httpRefMgt, chanId);
  if (load == NULL) {
    code = terrno;
    goto _ERROR;
  }

  if (atomic_load_8(&load->quit)) {
    code = TSDB_CODE_HTTP_MODULE_QUIT;
    goto _ERROR;
  }
  tDebug("http-report start to report, chanId:%" PRId64 ", seq:%" PRId64 "", chanId, msg->seq);

  code = transAsyncSend(load->asyncPool, &(msg->q));
  if (code != 0) {
    code = TSDB_CODE_HTTP_MODULE_QUIT;
    goto _ERROR;
  }
  msg = NULL;

_ERROR:

  if (code != 0) {
    tError("http-report failed to report reason:%s, chanId:%" PRId64 ", seq:%" PRId64 "", tstrerror(code), chanId,
           msg->seq);
  }
  httpDestroyMsg(msg);
  if (load != NULL) taosReleaseRef(httpRefMgt, chanId);
  return code;
}

int32_t taosSendHttpReportByChan(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                                 EHttpCompFlag flag, int64_t chanId, const char* qid) {
  return taosSendHttpReportImplByChan(server, uri, port, pCont, contLen, flag, chanId, qid);
}

int32_t taosSendHttpReport(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                           EHttpCompFlag flag) {
  return taosSendHttpReportWithQID(server, uri, port, pCont, contLen, flag, NULL);
}

int32_t taosSendHttpReportWithQID(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                                  EHttpCompFlag flag, const char* qid) {
  (void)taosThreadOnce(&transHttpInit, transHttpEnvInit);
  return taosSendHttpReportImplByChan(server, uri, port, pCont, contLen, flag, httpDefaultChanId, qid);
}

static void transHttpDestroyHandle(void* handle) { taosMemoryFree(handle); }

int64_t transInitHttpChanImpl();

static void transHttpEnvInit() {
  httpRefMgt = taosOpenRef(64, transHttpDestroyHandle);
  httpDefaultChanId = transInitHttpChanImpl();
  httpSeqNum = 0;
}

void transHttpEnvDestroy() {
  // remove default chanId
  taosDestroyHttpChan(httpDefaultChanId);
  httpDefaultChanId = -1;
}

int64_t transInitHttpChanImpl() {
  int32_t      code = 0;
  SHttpModule* http = taosMemoryCalloc(1, sizeof(SHttpModule));
  if (http == NULL) {
    code = terrno;
    goto _ERROR;
  }

  http->connStatusTable = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (http->connStatusTable == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _ERROR;
  }

  http->loop = taosMemoryMalloc(sizeof(uv_loop_t));
  if (http->loop == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _ERROR;
  }

  int err = uv_loop_init(http->loop);
  if (err != 0) {
    tError("http-report failed init uv, reason:%s", uv_strerror(err));
    code = TSDB_CODE_THIRDPARTY_ERROR;
    goto _ERROR;
  }

  code = transAsyncPoolCreate(http->loop, 1, http, httpAsyncCb, &http->asyncPool);
  if (code != 0) {
    goto _ERROR;
  }

  http->quit = 0;

  err = taosThreadCreate(&http->thread, NULL, httpThread, (void*)http);
  if (err != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _ERROR;
  }

  int64_t ref = taosAddRef(httpRefMgt, http);
  if (ref < 0) {
    goto _ERROR;
  }
  return ref;

_ERROR:
  httpModuleDestroy2(http);
  return code;
}
int64_t taosInitHttpChan() {
  (void)taosThreadOnce(&transHttpInit, transHttpEnvInit);
  return transInitHttpChanImpl();
}

void taosDestroyHttpChan(int64_t chanId) {
  tDebug("http-report send quit, chanId: %" PRId64 "", chanId);

  int          ret = 0;
  SHttpModule* load = taosAcquireRef(httpRefMgt, chanId);
  if (load == NULL) {
    tError("http-report failed to destroy chanId %" PRId64 ", reason:%s", chanId, tstrerror(terrno));
    ret = terrno;
    return;
  }

  atomic_store_8(&load->quit, 1);
  ret = httpSendQuit(load, chanId);
  if (ret != 0) {
    tDebug("http-report already destroyed, chanId %" PRId64 ",reason:%s", chanId, tstrerror(ret));
    (void)taosReleaseRef(httpRefMgt, chanId);
    return;
  }

  (void)taosThreadJoin(load->thread, NULL);

  httpModuleDestroy(load);

  (void)taosReleaseRef(httpRefMgt, chanId);
  (void)taosRemoveRef(httpRefMgt, chanId);
}
