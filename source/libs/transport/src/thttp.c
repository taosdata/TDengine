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
#include "tlog.h"
#include "transComm.h"

// clang-format on

#define HTTP_RECV_BUF_SIZE 1024

typedef struct SHttpModule {
  uv_loop_t*  loop;
  SAsyncPool* asyncPool;
  TdThread    thread;
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
  SHttpModule*  http;

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
} SHttpClient;

static TdThreadOnce transHttpInit = PTHREAD_ONCE_INIT;
static SHttpModule* thttp = NULL;
static void         transHttpEnvInit();

static void    httpHandleReq(SHttpMsg* msg);
static void    httpHandleQuit(SHttpMsg* msg);
static int32_t httpSendQuit();

static int32_t taosSendHttpReportImpl(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                                      EHttpCompFlag flag);

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
    return -1;
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
  }

  taosMemoryFree(pDest);
  return code;
}

static FORCE_INLINE int32_t taosBuildDstAddr(const char* server, uint16_t port, struct sockaddr_in* dest) {
  uint32_t ip = taosGetIpv4FromFqdn(server);
  if (ip == 0xffffffff) {
    tError("http-report failed to get http server:%s since %s", server, errno == 0 ? "invalid http server" : terrstr());
    return -1;
  }
  char buf[128] = {0};
  tinet_ntoa(buf, ip);
  uv_ip4_addr(buf, port, dest);
  return 0;
}

static void* httpThread(void* arg) {
  SHttpModule* http = (SHttpModule*)arg;
  setThreadName("http-cli-send-thread");
  uv_run(http->loop, UV_RUN_DEFAULT);
  return NULL;
}

static void httpDestroyMsg(SHttpMsg* msg) {
  if (msg == NULL) return;

  taosMemoryFree(msg->server);
  taosMemoryFree(msg->uri);
  taosMemoryFree(msg->cont);
  taosMemoryFree(msg);
}
static void httpAsyncCb(uv_async_t* handle) {
  SAsyncItem*  item = handle->data;
  SHttpModule* http = item->pThrd;

  SHttpMsg *msg = NULL, *quitMsg = NULL;

  queue wq;
  taosThreadMutexLock(&item->mtx);
  QUEUE_MOVE(&item->qmsg, &wq);
  taosThreadMutexUnlock(&item->mtx);

  int count = 0;
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
    tError("http-report recv error:%s", uv_err_name(nread));
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
    tError("http-report failed to send data, reason: %s, dst:%s:%d", uv_strerror(status), cli->addr, cli->port);
    if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
      uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
    }
    return;
  } else {
    tTrace("http-report succ to send data");
  }
  status = uv_read_start((uv_stream_t*)&cli->tcp, clientAllocBuffCb, clientRecvCb);
  if (status != 0) {
    tError("http-report failed to recv data,reason:%s, dst:%s:%d", uv_strerror(status), cli->addr, cli->port);
    if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
      uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
    }
  }
}
static void clientConnCb(uv_connect_t* req, int32_t status) {
  SHttpClient* cli = req->data;
  if (status != 0) {
    tError("http-report failed to conn to server, reason:%s, dst:%s:%d", uv_strerror(status), cli->addr, cli->port);
    if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
      uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
    }
    return;
  }
  status = uv_write(&cli->req, (uv_stream_t*)&cli->tcp, cli->wbuf, 2, clientSentCb);
  if (0 != status) {
    tError("http-report failed to send data,reason:%s, dst:%s:%d", uv_strerror(status), cli->addr, cli->port);
    if (!uv_is_closing((uv_handle_t*)&cli->tcp)) {
      uv_close((uv_handle_t*)&cli->tcp, clientCloseCb);
    }
  }
}

int32_t httpSendQuit() {
  SHttpMsg* msg = taosMemoryCalloc(1, sizeof(SHttpMsg));
  msg->quit = 1;

  SHttpModule* load = atomic_load_ptr(&thttp);
  if (load == NULL) {
    httpDestroyMsg(msg);
    tError("http-report already released");
    return -1;
  } else {
    msg->http = load;
  }
  transAsyncSend(load->asyncPool, &(msg->q));
  return 0;
}

static int32_t taosSendHttpReportImpl(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                                      EHttpCompFlag flag) {
  SHttpMsg* msg = taosMemoryMalloc(sizeof(SHttpMsg));
  msg->server = strdup(server);
  msg->uri  = strdup(uri);
  msg->port = port;
  msg->cont = taosMemoryMalloc(contLen);
  memcpy(msg->cont, pCont, contLen);
  msg->len = contLen;
  msg->flag = flag;
  msg->quit = 0;

  SHttpModule* load = atomic_load_ptr(&thttp);
  if (load == NULL) {
    httpDestroyMsg(msg);
    tError("http-report already released");
    return -1;
  }
  
  msg->http = load;
  return transAsyncSend(load->asyncPool, &(msg->q));
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
  SHttpModule* http = msg->http;
  taosMemoryFree(msg);

  uv_walk(http->loop, httpWalkCb, NULL);
}
static void httpHandleReq(SHttpMsg* msg) {
  SHttpModule* http = msg->http;

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

  int32_t len = 2048;
  char*   header = taosMemoryCalloc(1, len);
  int32_t headLen = taosBuildHttpHeader(msg->server, msg->uri, msg->len, header, len, msg->flag);
  if (headLen < 0) {
    taosMemoryFree(header);
    goto END;
  }

  uv_buf_t* wb = taosMemoryCalloc(2, sizeof(uv_buf_t));
  wb[0] = uv_buf_init((char*)header, strlen(header));  //  heap var
  wb[1] = uv_buf_init((char*)msg->cont, msg->len);     //  heap var

  SHttpClient* cli = taosMemoryCalloc(1, sizeof(SHttpClient));
  cli->conn.data = cli;
  cli->tcp.data = cli;
  cli->req.data = cli;
  cli->wbuf = wb;
  cli->rbuf = taosMemoryCalloc(1, HTTP_RECV_BUF_SIZE);
  cli->addr = msg->server;
  cli->port = msg->port;
  cli->dest = dest;

  taosMemoryFree(msg->uri);
  taosMemoryFree(msg);

  uv_tcp_init(http->loop, &cli->tcp);

  // set up timeout to avoid stuck;
  int32_t fd = taosCreateSocketWithTimeout(5);
  int     ret = uv_tcp_open((uv_tcp_t*)&cli->tcp, fd);
  if (ret != 0) {
    tError("http-report failed to open socket, reason:%s, dst:%s:%d", uv_strerror(ret), cli->addr, cli->port);
    destroyHttpClient(cli);
    return;
  }

  ret = uv_tcp_connect(&cli->conn, &cli->tcp, (const struct sockaddr*)&cli->dest, clientConnCb);
  if (ret != 0) {
    tError("http-report failed to connect to http-server, reason:%s, dst:%s:%d", uv_strerror(ret), cli->addr,
           cli->port);
    destroyHttpClient(cli);
  }
  return;

END:
  tError("http-report failed to report, reason: %s, addr: %s:%d", terrstr(), msg->server, msg->port);
  httpDestroyMsg(msg);
}

int32_t taosSendHttpReport(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen, EHttpCompFlag flag) {
  taosThreadOnce(&transHttpInit, transHttpEnvInit);
  return taosSendHttpReportImpl(server, uri, port, pCont, contLen, flag);
}

static void transHttpEnvInit() {
  SHttpModule* http = taosMemoryMalloc(sizeof(SHttpModule));

  http->loop = taosMemoryMalloc(sizeof(uv_loop_t));
  uv_loop_init(http->loop);

  http->asyncPool = transAsyncPoolCreate(http->loop, 1, http, httpAsyncCb);

  int err = taosThreadCreate(&http->thread, NULL, httpThread, (void*)http);
  if (err != 0) {
    taosMemoryFree(http->loop);
    taosMemoryFree(http);
    http = NULL;
  }
  atomic_store_ptr(&thttp, http);
}

void transHttpEnvDestroy() {
  SHttpModule* load = atomic_load_ptr(&thttp);
  if (load == NULL) {
    return;
  }
  httpSendQuit();
  taosThreadJoin(load->thread, NULL);

  TRANS_DESTROY_ASYNC_POOL_MSG(load->asyncPool, SHttpMsg, httpDestroyMsg);
  transAsyncPoolDestroy(load->asyncPool);
  uv_loop_close(load->loop);
  taosMemoryFree(load->loop);
  taosMemoryFree(load);

  atomic_store_ptr(&thttp, NULL);
}
