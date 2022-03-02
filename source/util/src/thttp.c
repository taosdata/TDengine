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
    uError("failed to create http socket since %s", terrstr());
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
    uError("failed to send http header since %s", terrstr());
    goto SEND_OVER;
  }

  if (taosWriteSocket(fd, (void*)pCont, contLen) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to send http content since %s", terrstr());
    goto SEND_OVER;
  }

  // read something to avoid nginx error 499
  if (taosReadSocket(fd, header, 10) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to receive response since %s", terrstr());
    goto SEND_OVER;
  }

  uInfo("send http to %s:%d, len:%d content: %s", server, port, contLen, pCont);
  code = 0;

SEND_OVER:
  if (fd != 0) {
    taosCloseSocket(fd);
  }

  return code;
}
