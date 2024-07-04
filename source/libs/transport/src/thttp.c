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

typedef struct SHttpModule {
  uv_loop_t*  loop;
  SAsyncPool* asyncPool;
  TdThread    thread;
  SHashObj*   connStatusTable;
  SHashObj*   connPool;
  int8_t      quit;
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
} SHttpClient;

typedef struct SHttpConnList {
  queue q;

} SHttpConnList;

static TdThreadOnce transHttpInit = PTHREAD_ONCE_INIT;
static void         transHttpEnvInit();

static void    httpHandleReq(SHttpMsg* msg);
static void    httpHandleQuit(SHttpMsg* msg);
static int32_t httpSendQuit(int64_t chanId);

static SHttpMsg* httpCreateMsg(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                               EHttpCompFlag flag, int64_t chanId);
static void      httpDestroyMsg(SHttpMsg* msg);

static bool    httpFailFastShoudIgnoreMsg(SHashObj* pTable, char* server, int16_t port);
static void    httpFailFastMayUpdate(SHashObj* pTable, char* server, int16_t port, int8_t succ);
static int32_t taosSendHttpReportImpl(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                                      EHttpCompFlag flag);
static void    httpModuleDestroy(SHttpModule* http);

static int32_t taosSendHttpReportImplByChan(const char* server, const char* uri, uint16_t port, char* pCont,
                                            int32_t contLen, EHttpCompFlag flag, int64_t chanId);

static int32_t taosBuildHttpHeader(const char* server, const char* uri, int32_t contLen, char* pHead, int32_t headLen,
                                   EHttpCompFlag flag) {
  if (flag == HTTP_FLAT) {
    return snprintf(pHead, headLen,
                    "POST %s HTTP/1.1\n"
                    "Host: %s\n"
                    "Content-Type: application/json\n"
                    "Content-Length: %d\n\n",
                    uri, server, contLen);
  } else if (flag == HTTP_GZIP) {
    return snprintf(pHead, headLen,
                    "POST %s HTTP/1.1\n"
                    "Host: %s\n"
                    "Content-Type: application/json\n"
                    "Content-Encoding: gzip\n"
                    "Content-Length: %d\n\n",
                    uri, server, contLen);
  } else {
    terrno = TSDB_CODE_INVALID_CFG;
    return terrno;
  }
}

static int32_t taosCompressHttpRport(char* pSrc, int32_t srcLen) {
  int32_t code = -1;
  int32_t destLen = srcLen;
  void*   pDest = taosMemoryMalloc(destLen);

  if (pDest == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  z_stream gzipStream = {0};
  gzipStream.zalloc = (alloc_func)0;
  gzipStream.zfree = (free_func)0;
  gzipStream.opaque = (voidpf)0;
  if (deflateInit2(&gzipStream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  gzipStream.next_in = (Bytef*)pSrc;
  gzipStream.avail_in = (uLong)srcLen;
  gzipStream.next_out = (Bytef*)pDest;
  gzipStream.avail_out = (uLong)(destLen);

  while (gzipStream.avail_in != 0 && gzipStream.total_out < (uLong)(destLen)) {
    if (deflate(&gzipStream, Z_FULL_FLUSH) != Z_OK) {
      terrno = TSDB_CODE_COMPRESS_ERROR;
      goto _OVER;
    }
  }

  if (gzipStream.avail_in != 0) {
    terrno = TSDB_CODE_COMPRESS_ERROR;
    goto _OVER;
  }

  int32_t err = 0;
  while (1) {
    if ((err = deflate(&gzipStream, Z_FINISH)) == Z_STREAM_END) {
      break;
    }
    if (err != Z_OK) {
      terrno = TSDB_CODE_COMPRESS_ERROR;
      goto _OVER;
    }
  }

  if (deflateEnd(&gzipStream) != Z_OK) {
    terrno = TSDB_CODE_COMPRESS_ERROR;
    goto _OVER;
  }

  if (gzipStream.total_out >= srcLen) {
    terrno = TSDB_CODE_COMPRESS_ERROR;
    goto _OVER;
  }

  code = 0;

_OVER:
  if (code == 0) {
    memcpy(pSrc, pDest, gzipStream.total_out);
    code = gzipStream.total_out;
  } else {
    code = terrno;
  }

  taosMemoryFree(pDest);
  return code;
}

static FORCE_INLINE int32_t taosBuildDstAddr(const char* server, uint16_t port, struct sockaddr_in* dest) {
  uint32_t ip = taosGetIpv4FromFqdn(server);
  if (ip == 0xffffffff) {
    tError("http-report failed to resolving domain names: %s", server);
    return TSDB_CODE_RPC_FQDN_ERROR;
  }
  char buf[256] = {0};
  tinet_ntoa(buf, ip);
  int ret = uv_ip4_addr(buf, port, dest);
  if (ret != 0) {
    tError("http-report failed to get addr %s", uv_err_name(ret));
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  return 0;
}

static void* httpThread(void* arg) {
  SHttpModule* http = (SHttpModule*)arg;
  setThreadName("http-cli-send-thread");
  uv_run(http->loop, UV_RUN_DEFAULT);
  return NULL;
}

static SHttpMsg* httpCreateMsg(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                               EHttpCompFlag flag, int64_t chanId) {
  if (server == NULL || uri == NULL) {
    tError("http-report failed to report to invalid addr, chanId:%" PRId64 "", chanId);
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  if (pCont == NULL || contLen == 0) {
    tError("http-report failed to report empty packet, chanId:%" PRId64 "", chanId);
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  SHttpMsg* msg = taosMemoryMalloc(sizeof(SHttpMsg));
  if (msg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  msg->port = port;
  msg->server = taosStrdup(server);
  msg->uri = taosStrdup(uri);
  msg->cont = taosMemoryMalloc(contLen);
  if (msg->server == NULL || msg->uri == NULL || msg->cont == NULL) {
    httpDestroyMsg(msg);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  memcpy(msg->cont, pCont, contLen);
  msg->len = contLen;
  msg->flag = flag;
  msg->quit = 0;
  msg->chanId = chanId;
  return msg;
}
static void httpDestroyMsg(SHttpMsg* msg) {
  if (msg == NULL) return;

  taosMemoryFree(msg->server);
  taosMemoryFree(msg->uri);
  taosMemoryFree(msg->cont);
  taosMemoryFree(msg);
}
static void httpDestroyMsgWrapper(void* cont, void* param) { httpDestroyMsg((SHttpMsg*)cont); }

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
      httpDestroyMsg(msg);
    } else {
      quitMsg = msg;
    }
  }
  if (quitMsg != NULL) {
    QUEUE_PUSH(&item->qmsg, &quitMsg->q);
  }
}
static void httpAsyncCb(uv_async_t* handle) {
  SAsyncItem*  item = handle->data;
  SHttpModule* http = item->pThrd;

  SHttpMsg *msg = NULL, *quitMsg = NULL;
  queue     wq;
  QUEUE_INIT(&wq);

  static int32_t BATCH_SIZE = 5;
  int32_t        count = 0;

  taosThreadMutexLock(&item->mtx);
  httpMayDiscardMsg(http, item);

  while (!QUEUE_IS_EMPTY(&item->qmsg) && count++ < BATCH_SIZE) {
    queue* h = QUEUE_HEAD(&item->qmsg);
    QUEUE_REMOVE(h);
    QUEUE_PUSH(&wq, h);
  }
  taosThreadMutexUnlock(&item->mtx);

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
  destroyHttpClient(cli);
}

static FORCE_INLINE void clientAllocBuffCb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  SHttpClient* cli = handle->data;
  buf->base = cli->rbuf;
  buf->len = HTTP_RECV_BUF_SIZE;
}

static FORCE_INLINE void clientRecvCb(uv_stream_t* handle, ssize_t nread, const uv_buf_t* buf) {
  SHttpClient* cli = handle->data;
  if (nread < 0) {
    tError("http-report recv error:%s", uv_strerror(nread));
  } else {
    tTrace("http-report succ to recv %d bytes", (int32_t)nread);
  }
  if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
    uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
  }
}
static void clientSentCb(uv_write_t* req, int32_t status) {
  SHttpClient* cli = req->data;
  if (status != 0) {
    tError("http-report failed to send data, reason: %s, dst:%s:%d, chanId:%" PRId64 "", uv_strerror(status), cli->addr,
           cli->port, cli->chanId);
    if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
      uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
    }
    return;
  } else {
    tTrace("http-report succ to send data, chanId:%" PRId64 "", cli->chanId);
  }
  status = uv_read_start((uv_stream_t*)&cli->tcp, clientAllocBuffCb, clientRecvCb);
  if (status != 0) {
    tError("http-report failed to recv data,reason:%s, dst:%s:%d, chanId:%" PRId64 "", uv_strerror(status), cli->addr,
           cli->port, cli->chanId);
    if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
      uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
    }
  }
}
static void clientConnCb(uv_connect_t* req, int32_t status) {
  SHttpClient* cli = req->data;
  int64_t      chanId = cli->chanId;

  SHttpModule* http = taosAcquireRef(httpRefMgt, chanId);
  if (status != 0) {
    httpFailFastMayUpdate(http->connStatusTable, cli->addr, cli->port, 0);

    tError("http-report failed to conn to server, reason:%s, dst:%s:%d, chanId:%" PRId64 "", uv_strerror(status),
           cli->addr, cli->port, chanId);
    if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
      uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
    }
    taosReleaseRef(httpRefMgt, chanId);
    return;
  }
  httpFailFastMayUpdate(http->connStatusTable, cli->addr, cli->port, 1);

  status = uv_write(&cli->req, (uv_stream_t*)&cli->tcp, cli->wbuf, 2, clientSentCb);
  if (0 != status) {
    tError("http-report failed to send data,reason:%s, dst:%s:%d, chanId:%" PRId64 "", uv_strerror(status), cli->addr,
           cli->port, chanId);
    if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
      uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
    }
  }
  taosReleaseRef(httpRefMgt, chanId);
}

int32_t httpSendQuit(int64_t chanId) {
  SHttpModule* http = taosAcquireRef(httpRefMgt, chanId);
  if (http == NULL) return terrno;

  SHttpMsg* msg = taosMemoryCalloc(1, sizeof(SHttpMsg));
  if (msg == NULL) {
    taosReleaseRef(httpRefMgt, chanId);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  msg->quit = 1;
  msg->chanId = chanId;

  int ret = transAsyncSend(http->asyncPool, &(msg->q));
  if (ret != 0) {
    taosMemoryFree(msg);
    taosReleaseRef(httpRefMgt, chanId);
    return ret;
  }

  taosReleaseRef(httpRefMgt, chanId);
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
  int64_t chanId = msg->chanId;
  taosMemoryFree(msg);

  tDebug("http-report receive quit, chanId:%" PRId64 "", chanId);
  SHttpModule* http = taosAcquireRef(httpRefMgt, chanId);
  if (http == NULL) return;
  uv_walk(http->loop, httpWalkCb, NULL);
  taosReleaseRef(httpRefMgt, chanId);
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
  char buf[256] = {0};
  sprintf(buf, "%s:%d", server, port);

  if (succ) {
    taosHashRemove(pTable, buf, strlen(buf));
  } else {
    int32_t st = taosGetTimestampSec();
    taosHashPut(pTable, buf, strlen(buf), &st, sizeof(st));
  }
  return;
}
static void httpHandleReq(SHttpMsg* msg) {
  int64_t chanId = msg->chanId;
  int32_t ignore = false;
  char*   header = NULL;
  terrno = 0;

  SHttpModule* http = taosAcquireRef(httpRefMgt, chanId);
  if (http == NULL) {
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
      goto END;
    }
  }

  int32_t cap = 2048;
  header = taosMemoryCalloc(1, cap);
  if (header == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }

  int32_t headLen = taosBuildHttpHeader(msg->server, msg->uri, msg->len, header, cap, msg->flag);
  if (headLen < 0 || headLen >= cap) {
    goto END;
  }

  uv_buf_t* wb = taosMemoryCalloc(2, sizeof(uv_buf_t));
  if (wb == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto END;
  }

  wb[0] = uv_buf_init((char*)header, strlen(header));  //  heap var
  wb[1] = uv_buf_init((char*)msg->cont, msg->len);     //  heap var

  SHttpClient* cli = taosMemoryCalloc(1, sizeof(SHttpClient));
  if (cli == NULL) {
    taosMemoryFree(wb);
    goto END;
  }

  cli->conn.data = cli;
  cli->tcp.data = cli;
  cli->req.data = cli;
  cli->wbuf = wb;
  cli->rbuf = taosMemoryCalloc(1, HTTP_RECV_BUF_SIZE);
  if (cli->rbuf == NULL) {
    taosMemoryFree(msg->uri);
    taosMemoryFree(msg);
    destroyHttpClient(cli);
    taosReleaseRef(httpRefMgt, chanId);
    return;
  }

  cli->addr = msg->server;
  cli->port = msg->port;
  cli->dest = dest;
  cli->chanId = chanId;

  taosMemoryFree(msg->uri);
  taosMemoryFree(msg);

  int err = uv_tcp_init(http->loop, &cli->tcp);
  if (err != 0) {
    tError("http-report failed to init socket handle, dst:%s:%d,reason:%s chanId:%" PRId64 "", cli->addr, cli->port,
           uv_strerror(err), chanId);
    destroyHttpClient(cli);
    taosReleaseRef(httpRefMgt, chanId);
  }

  // set up timeout to avoid stuck;
  int32_t fd = taosCreateSocketWithTimeout(5000);
  if (fd < 0) {
    tError("http-report failed to open socket, dst:%s:%d, chanId:%" PRId64 "", cli->addr, cli->port, chanId);
    destroyHttpClient(cli);
    taosReleaseRef(httpRefMgt, chanId);
    return;
  }
  int ret = uv_tcp_open((uv_tcp_t*)&cli->tcp, fd);
  if (ret != 0) {
    tError("http-report failed to open socket, reason:%s, dst:%s:%d, chanId:%" PRId64 "", uv_strerror(ret), cli->addr,
           cli->port, chanId);
    destroyHttpClient(cli);
    taosReleaseRef(httpRefMgt, chanId);
    return;
  }

  ret = uv_tcp_connect(&cli->conn, &cli->tcp, (const struct sockaddr*)&cli->dest, clientConnCb);
  if (ret != 0) {
    tError("http-report failed to connect to http-server, reason:%s, dst:%s:%d, chanId:%" PRId64 "", uv_strerror(ret),
           cli->addr, cli->port, chanId);
    httpFailFastMayUpdate(http->connStatusTable, cli->addr, cli->port, 0);
    destroyHttpClient(cli);
  }
  taosReleaseRef(httpRefMgt, chanId);
  return;

END:
  if (ignore == false) {
    tError("http-report failed to report, reason: %s, addr: %s:%d, chanId:%" PRId64 "", terrstr(), msg->server,
           msg->port, chanId);
  }
  httpDestroyMsg(msg);
  taosMemoryFree(header);
  taosReleaseRef(httpRefMgt, chanId);
}

static void httpModuleDestroy(SHttpModule* http) {
  if (http->asyncPool != NULL) {
    TRANS_DESTROY_ASYNC_POOL_MSG(http->asyncPool, SHttpMsg, httpDestroyMsgWrapper, NULL);
    transAsyncPoolDestroy(http->asyncPool);
  }
  if (http->loop) {
    uv_loop_close(http->loop);
    taosMemoryFree(http->loop);
  }
  taosHashCleanup(http->connStatusTable);
  // not free http, http freeed by ref
}

static int32_t taosSendHttpReportImplByChan(const char* server, const char* uri, uint16_t port, char* pCont,
                                            int32_t contLen, EHttpCompFlag flag, int64_t chanId) {
  int32_t ret = 0;
  terrno = 0;
  SHttpMsg* msg = httpCreateMsg(server, uri, port, pCont, contLen, flag, chanId);
  if (msg == NULL) {
    return terrno;
  }

  SHttpModule* load = taosAcquireRef(httpRefMgt, chanId);
  if (load == NULL) {
    httpDestroyMsg(msg);
    return terrno;
  }

  if (atomic_load_8(&load->quit)) {
    httpDestroyMsg(msg);
    taosReleaseRef(httpRefMgt, chanId);
    terrno = TSDB_CODE_HTTP_MODULE_QUIT;
    return terrno;
  }

  ret = transAsyncSend(load->asyncPool, &(msg->q));
  if (ret < 0) {
    httpDestroyMsg(msg);
    taosReleaseRef(httpRefMgt, chanId);
    terrno = TSDB_CODE_HTTP_MODULE_QUIT;
    return terrno;
  }

  taosReleaseRef(httpRefMgt, chanId);
  return ret;
}

int32_t taosSendHttpReportByChan(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                                 EHttpCompFlag flag, int64_t chanId) {
  return taosSendHttpReportImplByChan(server, uri, port, pCont, contLen, flag, chanId);
}

int32_t taosSendHttpReport(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                           EHttpCompFlag flag) {
  taosThreadOnce(&transHttpInit, transHttpEnvInit);
  return taosSendHttpReportImplByChan(server, uri, port, pCont, contLen, flag, httpDefaultChanId);
}

static void transHttpDestroyHandle(void* handle) { taosMemoryFree(handle); }

int64_t transInitHttpChanImpl();

static void transHttpEnvInit() {
  httpRefMgt = taosOpenRef(64, transHttpDestroyHandle);
  httpDefaultChanId = transInitHttpChanImpl();
}

void transHttpEnvDestroy() {
  // remove default chanId
  taosDestroyHttpChan(httpDefaultChanId);
  httpDefaultChanId = -1;
}

int64_t transInitHttpChanImpl() {
  SHttpModule* http = taosMemoryCalloc(1, sizeof(SHttpModule));
  if (http == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  http->connStatusTable = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (http->connStatusTable == NULL) {
    httpModuleDestroy(http);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  http->loop = taosMemoryMalloc(sizeof(uv_loop_t));
  if (http->loop == NULL) {
    httpModuleDestroy(http);
    taosMemoryFree(http);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  int err = uv_loop_init(http->loop);
  if (err != 0) {
    tError("http-report failed init uv, reason:%s", uv_strerror(err));
    httpModuleDestroy(http);
    taosMemoryFree(http);
    return TSDB_CODE_THIRDPARTY_ERROR;
  }

  http->asyncPool = transAsyncPoolCreate(http->loop, 1, http, httpAsyncCb);
  if (NULL == http->asyncPool) {
    httpModuleDestroy(http);
    taosMemoryFree(http);
    return terrno;
  }
  http->quit = 0;

  err = taosThreadCreate(&http->thread, NULL, httpThread, (void*)http);
  if (err != 0) {
    httpModuleDestroy(http);
    taosMemoryFree(http);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }
  int64_t ref = taosAddRef(httpRefMgt, http);
  if (ref < 0) {
    return terrno;
  }
  return ref;
}
int64_t taosInitHttpChan() {
  taosThreadOnce(&transHttpInit, transHttpEnvInit);
  return transInitHttpChanImpl();
}

void taosDestroyHttpChan(int64_t chanId) {
  tDebug("http-report send quit, chanId:%" PRId64 "", chanId);

  int          ret = 0;
  SHttpModule* load = taosAcquireRef(httpRefMgt, chanId);
  if (load == NULL) {
    tError("http-report failed destroy chanId %" PRId64 "", chanId);
    return;
  }
  atomic_store_8(&load->quit, 1);

  ret = httpSendQuit(chanId);
  if (ret != 0) {
    tDebug("http-report already destroyed, chanId %" PRId64 "", chanId);
    taosReleaseRef(httpRefMgt, chanId);
    return;
  }

  taosThreadJoin(load->thread, NULL);

  httpModuleDestroy(load);

  taosReleaseRef(httpRefMgt, chanId);
  taosReleaseRef(httpRefMgt, chanId);
}