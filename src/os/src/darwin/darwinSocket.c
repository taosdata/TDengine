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

int taosSetSockOpt(int32_t socketfd, int level, int optname, void *optval, int optlen) {
  if (level == SOL_SOCKET && optname == SO_SNDBUF) {
    return 0;
  }

  if (level == SOL_SOCKET && optname == SO_RCVBUF) {
    return 0;
  }

  return setsockopt(socketfd, level, optname, optval, (socklen_t)optlen);
}
