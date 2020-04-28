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
#include "taosdef.h"
#include "tulog.h"
#include "tglobal.h"
#include "tsocket.h"
#include "tsync.h"

int main(int argc, char *argv[]) {

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-p")==0 && i < argc-1) {
      tsSyncPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
      ddebugFlag = atoi(argv[++i]);
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-p port]: server port number, default is:%d\n", tsSyncPort);
      printf("  [-d debugFlag]: debug flag, default:%d\n", ddebugFlag);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  }
 
  tsAsyncLog = 0;
  taosInitLog("arbitrator.log", 1000000, 10);

  SSyncInfo syncInfo;
  memset(&syncInfo, 0, sizeof(syncInfo));

  syncInfo.syncCfg.replica = 1;
  syncInfo.syncCfg.quorum = 1;
  syncInfo.vgId = 1;
  syncInfo.ahandle = &syncInfo;
  syncInfo.syncCfg.nodeInfo[0].nodeId = 1;
  taosGetFqdn(syncInfo.syncCfg.nodeInfo[0].nodeFqdn);
  syncInfo.syncCfg.nodeInfo[0].nodePort = tsSyncPort;

  void *syncHandle = syncStart(&syncInfo);
  if (syncHandle == NULL) {
    uError("failed to init arbitrator");
    return -1;
  }

  uPrint("TAOS arbitrator: %s:%d is running\n", syncInfo.syncCfg.nodeInfo[0].nodeFqdn, tsSyncPort);

  while (1) {
    sleep(1);
  }

  return 0;
}


