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
#include "os.h"
#include "tulog.h"

#ifndef TAOS_OS_FUNC_SOCKET

int32_t taosSetNonblocking(SOCKET sock, int32_t on) {
  int32_t flags = 0;
  if ((flags = fcntl(sock, F_GETFL, 0)) < 0) {
    uError("fcntl(F_GETFL) error: %d (%s)\n", errno, strerror(errno));
    return 1;
  }

  if (on)
    flags |= O_NONBLOCK;
  else
    flags &= ~O_NONBLOCK;

  if ((flags = fcntl(sock, F_SETFL, flags)) < 0) {
    uError("fcntl(F_SETFL) error: %d (%s)\n", errno, strerror(errno));
    return 1;
  }

  return 0;
}

void taosIgnSIGPIPE() {
  signal(SIGPIPE, SIG_IGN);
}

void taosBlockSIGPIPE() {
  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  sigaddset(&signal_mask, SIGPIPE);
  int32_t rc = pthread_sigmask(SIG_BLOCK, &signal_mask, NULL);
  if (rc != 0) {
    uError("failed to block SIGPIPE");
  }
}

void taosSetMaskSIGPIPE() {
  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  sigaddset(&signal_mask, SIGPIPE);
  int32_t rc = pthread_sigmask(SIG_SETMASK, &signal_mask, NULL);
  if (rc != 0) {
    uError("failed to setmask SIGPIPE");
  }
}

#endif

#ifndef TAOS_OS_FUNC_SOCKET_SETSOCKETOPT

int32_t taosSetSockOpt(SOCKET socketfd, int32_t level, int32_t optname, void *optval, int32_t optlen) {
  return setsockopt(socketfd, level, optname, optval, (socklen_t)optlen);
}

#endif

#ifndef TAOS_OS_FUNC_SOCKET_INET

uint32_t taosInetAddr(char *ipAddr) {
  return inet_addr(ipAddr);
}

const char *taosInetNtoa(struct in_addr ipInt) {
  return inet_ntoa(ipInt);
}

#endif