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

void taosCloseTcpServerConnection(void *chandle) {
  tError("CloseTcpServerConnection not support in windows");
}

void taosCleanUpTcpServer(void *handle) {
  tError("CleanUpTcpServer not support in windows");
}

void *taosInitTcpServer(char *ip, uint16_t port, char *label, int numOfThreads, void *fp, void *shandle) {
  tError("InitTcpServer not support in windows");
  return 0;
}

int taosSendTcpServerData(unsigned int ip, uint16_t port, char *data, int len, void *chandle) {
  tError("SendTcpServerData not support in windows");
  return 0;
}
