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

#include "tulog.h"

void *taosInitTcpClient(char *ip, uint16_t port, char *label, int num, void *fp, void *shandle) {
  tError("InitTcpClient not support in windows");
  return 0;
}

void taosCloseTcpClientConnection(void *chandle) {
  tError("CloseTcpClientConnection not support in windows");
}

void *taosOpenTcpClientConnection(void *shandle, void *thandle, char *ip, uint16_t port) {
  tError("OpenTcpClientConnection not support in windows");
  return 0;
}

int taosSendTcpClientData(unsigned int ip, uint16_t port, char *data, int len, void *chandle) {
  tError("SendTcpClientData not support in windows");
  return 0;
}

void taosCleanUpTcpClient(void *chandle) {
  tError("SendTcpClientData not support in windows");
}
