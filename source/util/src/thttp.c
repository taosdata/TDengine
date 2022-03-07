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
#include "thttp.h"
#include "taoserror.h"
#include "tlog.h"

#ifdef USE_UV

#include <uv.h>

void clientConnCb(uv_connect_t* req, int status) {
  if(status < 0) {
    terrno = TAOS_SYSTEM_ERROR(status);
    uError("Connection error %s\n",uv_strerror(status));
    return;
  }

  // impl later
  uv_buf_t* wb = req->data;
  if (wb == NULL) {
    uv_close((uv_handle_t *)req->handle,NULL);
  }
  uv_write_t write_req;
  uv_write(&write_req, req->handle, wb, 2, NULL);
  uv_close((uv_handle_t *)req->handle,NULL);
}

int32_t taosSendHttpReport(const char* server, uint16_t port, const char* pCont, int32_t contLen) {
  uint32_t ipv4 = taosGetIpv4FromFqdn(server);
  if (ipv4 == 0xffffffff) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to get http server:%s ip since %s", server, terrstr());
    return -1;
    // goto SEND_OVER;
  }
  char ipv4Buf[128];
  tinet_ntoa(ipv4Buf, ipv4);

  struct sockaddr_in dest;
  uv_ip4_addr(ipv4Buf, port, &dest);

  uv_tcp_t socket_tcp;
  uv_loop_t *loop = uv_default_loop();
  uv_tcp_init(loop, &socket_tcp);
  uv_connect_t* connect = (uv_connect_t*)malloc(sizeof(uv_connect_t));

  char    header[4096] = {0};
  int32_t headLen = snprintf(header, sizeof(header),
                             "POST /report HTTP/1.1\n"
                             "Host: %s\n"
                             "Content-Type: application/json\n"
                             "Content-Length: %d\n\n",
                             server, contLen);
  uv_buf_t wb[2];
  wb[0] = uv_buf_init((char*)header, headLen);
  wb[1] = uv_buf_init((char*)pCont, contLen);

  connect->data = wb;
  uv_tcp_connect(connect, &socket_tcp, (const struct sockaddr*)&dest, clientConnCb);
  terrno = 0;
  uv_run(loop,UV_RUN_DEFAULT);
  uv_loop_close(loop);
  free(connect);
  return terrno;
}

#else
int32_t taosSendHttpReport(const char* server, uint16_t port, const char* pCont, int32_t contLen) {
  int32_t code = -1;
  SOCKET  fd = 0;

  uint32_t ip = taosGetIpv4FromFqdn(server);
  if (ip == 0xffffffff) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to get http server:%s ip since %s", server, terrstr());
    goto SEND_OVER;
  }

  fd = taosOpenTcpClientSocket(ip, port, 0);
  if (fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to create http socket to %s:%u since %s", server, port, terrstr());
    goto SEND_OVER;
  }

  char    header[4096] = {0};
  int32_t headLen = snprintf(header, sizeof(header),
                             "POST /report HTTP/1.1\n"
                             "Host: %s\n"
                             "Content-Type: application/json\n"
                             "Content-Length: %d\n\n",
                             server, contLen);

  if (taosWriteSocket(fd, (void*)header, headLen) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to send http header to %s:%u since %s", server, port, terrstr());
    goto SEND_OVER;
  }

  if (taosWriteSocket(fd, (void*)pCont, contLen) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to send http content to %s:%u since %s", server, port, terrstr());
    goto SEND_OVER;
  }

  // read something to avoid nginx error 499
  if (taosReadSocket(fd, header, 10) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to receive response from %s:%u since %s", server, port, terrstr());
    goto SEND_OVER;
  }

  uTrace("send http to %s:%u, len:%d content: %s", server, port, contLen, pCont);
  code = 0;

SEND_OVER:
  if (fd != 0) {
    taosCloseSocket(fd);
  }

  return code;
}

#endif