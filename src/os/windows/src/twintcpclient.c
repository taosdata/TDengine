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

#include "tlog.h"

void *taosInitTcpClient(char *ip, short port, char *label, int num, void *fp, void *shandle) {
  tError("taosInitTcpClient not support in windows");
  return 0;
}

void taosCloseTcpClientConnection(void *chandle) {
  tError("taosCloseTcpClientConnection not support in windows");
}

void *taosOpenTcpClientConnection(void *shandle, void *thandle, char *ip, short port) {
  tError("taosOpenTcpClientConnection not support in windows");
  return 0;
}

int taosSendTcpClientData(unsigned int ip, short port, char *data, int len, void *chandle) {
  tError("taosSendTcpClientData not support in windows");
  return 0;
}

void taosCleanUpTcpClient(void *chandle) {
  tError("taosSendTcpClientData not support in windows");
}
