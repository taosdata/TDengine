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
#include <stdint.h>
#include "os.h"
#include "tlog.h"
#include "tsync.h"

int main(int argc, char *argv[]) {

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-p")==0 && i < argc-1) {
      tsSyncPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-i")==0 && i < argc-1) {
      strcpy(tsPrivateIp, argv[++i]); 
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
      ddebugFlag = atoi(argv[++i]);
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-i ip]: server IP address, default is:%s\n", tsPrivateIp);
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

  strcpy(syncInfo.label, "vid:0");
  syncInfo.replica = 1;
  syncInfo.quorum = 1;
  syncInfo.vgId = 1;
  syncInfo.ahandle = &syncInfo;
  syncInfo.nodeInfo[0].nodeId = 1;
  strcpy(syncInfo.nodeInfo[0].name, tsPrivateIp);
  syncInfo.nodeInfo[0].nodeIp = inet_addr(tsPrivateIp);

  void *syncHandle = syncStart(&syncInfo);
  if (syncHandle == NULL) {
    dError("failed to init arbitrator");
    return -1;
  }

  dPrint("TAOS arbitrator is running, ip:%s\n", tsPrivateIp);

  while (1) {
    sleep(1);
  }

  return 0;
}


