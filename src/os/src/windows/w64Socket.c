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

#include "os.h"
#include <WS2tcpip.h>  
#include <IPHlpApi.h>  
#include <winsock2.h>  
#include <stdio.h>
#include <string.h>
#include <ws2def.h>
#include <tchar.h>

void taosWinSocketInit() {
  static char flag = 0;
  if (flag == 0) {
    WORD    wVersionRequested;
    WSADATA wsaData;
    wVersionRequested = MAKEWORD(1, 1);
    if (WSAStartup(wVersionRequested, &wsaData) == 0) {
      flag = 1;
    }
  }
}

int taosSetNonblocking(int sock, int on) {
  u_long mode;
  if (on) {
    mode = 1;
    ioctlsocket(sock, FIONBIO, &mode);
  } else {
    mode = 0;
    ioctlsocket(sock, FIONBIO, &mode);
  }
  return 0;
}

void taosBlockSIGPIPE() {}

int taosSetSockOpt(int socketfd, int level, int optname, void *optval, int optlen) {
  if (level == SOL_SOCKET && optname == TCP_KEEPCNT) {
    return 0;
  }

  if (level == SOL_TCP && optname == TCP_KEEPIDLE) {
    return 0;
  }

  if (level == SOL_TCP && optname == TCP_KEEPINTVL) {
    return 0;
  }

  return setsockopt(socketfd, level, optname, optval, optlen);
}