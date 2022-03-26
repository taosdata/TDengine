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
#ifdef USE_UV
#include <uv.h>
#endif
#include "zlib.h"
#include "thttp.h"
#include "taoserror.h"
#include "tlog.h"

static int32_t taosBuildHttpHeader(const char* server, int32_t contLen, char* pHead, int32_t headLen,
                                   EHttpCompFlag flag) {
  if (flag == HTTP_FLAT) {
    return snprintf(pHead, headLen,
                    "POST /report HTTP/1.1\n"
                    "Host: %s\n"
                    "Content-Type: application/json\n"
                    "Content-Length: %d\n\n",
                    server, contLen);
  } else if (flag == HTTP_GZIP) {
    return snprintf(pHead, headLen,
                    "POST /report HTTP/1.1\n"
                    "Host: %s\n"
                    "Content-Type: application/json\n"
                    "Content-Encoding: gzip\n"
                    "Content-Length: %d\n\n",
                    server, contLen);
  } else {
    return -1;
  }
}

int32_t taosCompressHttpRport(char* pSrc, int32_t srcLen) {
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

#ifdef USE_UV
static void clientConnCb(uv_connect_t* req, int32_t status) {
  if (status < 0) {
    terrno = TAOS_SYSTEM_ERROR(status);
    uError("Connection error %s\n", uv_strerror(status));
    uv_close((uv_handle_t*)req->handle, NULL);
    return;
  }
  uv_buf_t* wb = req->data;
  assert(wb != NULL);
  uv_write_t write_req;
  uv_write(&write_req, req->handle, wb, 2, NULL);
  uv_close((uv_handle_t*)req->handle, NULL);
}

int32_t taosSendHttpReport(const char* server, uint16_t port, char* pCont, int32_t contLen, EHttpCompFlag flag) {
  uint32_t ipv4 = taosGetIpv4FromFqdn(server);
  if (ipv4 == 0xffffffff) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to get http server:%s ip since %s", server, terrstr());
    return -1;
  }

  char ipv4Buf[128] = {0};
  tinet_ntoa(ipv4Buf, ipv4);

  struct sockaddr_in dest = {0};
  uv_ip4_addr(ipv4Buf, port, &dest);

  uv_tcp_t   socket_tcp = {0};
  uv_loop_t* loop = uv_default_loop();
  uv_tcp_init(loop, &socket_tcp);
  uv_connect_t* connect = (uv_connect_t*)taosMemoryMalloc(sizeof(uv_connect_t));

  if (flag == HTTP_GZIP) {
    int32_t dstLen = taosCompressHttpRport(pCont, contLen);
    if (dstLen > 0) {
      contLen = dstLen;
    } else {
      flag = HTTP_FLAT;
    }
  }

  char    header[1024] = {0};
  int32_t headLen = taosBuildHttpHeader(server, contLen, header, sizeof(header), flag);

  uv_buf_t wb[2];
  wb[0] = uv_buf_init((char*)header, headLen);
  wb[1] = uv_buf_init((char*)pCont, contLen);

  connect->data = wb;
  uv_tcp_connect(connect, &socket_tcp, (const struct sockaddr*)&dest, clientConnCb);
  terrno = 0;
  uv_run(loop, UV_RUN_DEFAULT);
  uv_loop_close(loop);
  taosMemoryFree(connect);
  return terrno;
}

#else
int32_t taosSendHttpReport(const char* server, uint16_t port, char* pCont, int32_t contLen, EHttpCompFlag flag) {
  int32_t code = -1;
  TdSocketPtr pSocket = NULL;

  uint32_t ip = taosGetIpv4FromFqdn(server);
  if (ip == 0xffffffff) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to get http server:%s ip since %s", server, terrstr());
    goto SEND_OVER;
  }

  pSocket = taosOpenTcpClientSocket(ip, port, 0);
  if (pSocket == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to create http socket to %s:%u since %s", server, port, terrstr());
    goto SEND_OVER;
  }

  if (flag == HTTP_GZIP) {
    int32_t dstLen = taosCompressHttpRport(pCont, contLen);
    if (dstLen > 0) {
      contLen = dstLen;
    } else {
      flag = HTTP_FLAT;
    }
  }

  char    header[1024] = {0};
  int32_t headLen = taosBuildHttpHeader(server, contLen, header, sizeof(header), flag);
  if (taosWriteMsg(pSocket, header, headLen) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to send http header to %s:%u since %s", server, port, terrstr());
    goto SEND_OVER;
  }

  if (taosWriteMsg(pSocket, (void*)pCont, contLen) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to send http content to %s:%u since %s", server, port, terrstr());
    goto SEND_OVER;
  }

  // read something to avoid nginx error 499
  if (taosWriteMsg(pSocket, header, 10) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to receive response from %s:%u since %s", server, port, terrstr());
    goto SEND_OVER;
  }

  code = 0;

SEND_OVER:
  if (pSocket != NULL) {
    taosCloseSocket(&pSocket);
  }

  return code;
}

#endif