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

//#define _DEFAULT_SOURCE
#include "os.h"
#include "tlog.h"
#include "trpc.h"
#include "taoserror.h"
#include <stdint.h>

void processMsg(char type, void *pCont, int contLen, void *ahandle, int32_t code) {
  dPrint("response is received, type:%d, contLen:%d code:%x:%s", type, contLen, code, tstrerror(code));
}

void processUpdate(void *handle, SRpcIpSet *pIpSet) {
  dPrint("ip set is changed, index:%d", pIpSet->index);
}

int32_t main(int32_t argc, char *argv[]) {

  taosInitLog("client.log", 100000, 10);
  dPrint("unit test for rpc module");

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = "0.0.0.0";
  rpcInit.localPort    = 0;
  rpcInit.label        = "APP";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = processMsg;
  rpcInit.ufp          = processUpdate;
  rpcInit.sessions     = 1000;
  rpcInit.connType     = TAOS_CONN_UDPC;
  rpcInit.idleTime     = 2000;
  rpcInit.meterId      = "jefftao";
  rpcInit.secret       = "password";
  rpcInit.ckey         = "key";

  void *pRpc = rpcOpen(&rpcInit);
  if (pRpc == NULL) {
    dError("failed to initialize rpc");
    return -1;
  }

  SRpcIpSet ipSet;
  ipSet.numOfIps = 2;
  ipSet.index = 0;
  ipSet.port = 7000;
  ipSet.ip[0] = inet_addr("192.168.0.1");
  ipSet.ip[1] = inet_addr("127.0.0.1");

  void *cont = rpcMallocCont(100); 
  rpcSendRequest(pRpc, &ipSet, 1, cont, 100, 1);

  getchar();

  return 0;
}


