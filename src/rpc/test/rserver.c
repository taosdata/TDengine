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
#include <stdint.h>

int msgSize = 128;
int commit = 0;
int dataFd = -1;

void processRequestMsg(char type, void *pCont, int contLen, void *thandle, int32_t code) {
  static int num = 0;
  tTrace("request is received, type:%d, contLen:%d", type, contLen);

  if (dataFd >=0) 
    write(dataFd, pCont, contLen);

  if (commit >=2) {
    ++num;
    if ( fsync(dataFd) < 0 ) {
      tPrint("failed to flush data to file, reason:%s", strerror(errno));
    }

    if (num % 10000 == 0) {
      tPrint("%d request have been written into disk", num);
    }
  }

  void *rsp = rpcMallocCont(msgSize);

  rpcSendResponse(thandle, 1, rsp, msgSize);

/*
  SRpcIpSet ipSet;
  ipSet.numOfIps = 1;
  ipSet.index = 0;
  ipSet.port = 7000;
  ipSet.ip[0] = inet_addr("192.168.0.2");

  rpcSendRedirectRsp(ahandle, &ipSet);
*/

  rpcFreeCont(pCont);
}

int main(int argc, char *argv[]) {
  SRpcInit rpcInit;
  char     socketType[20] = "udp";
  char     dataName[20] = "server.data";

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = "0.0.0.0";
  rpcInit.localPort    = 7000;
  rpcInit.label        = "SER";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = processRequestMsg;
  rpcInit.sessions     = 1000;
  rpcInit.idleTime     = 2000;

  for (int i=1; i<argc; ++i) {
    if ( strcmp(argv[i], "-c")==0 && i < argc-1 ) {
      strcpy(socketType, argv[++i]);
    } else if (strcmp(argv[i], "-p")==0 && i < argc-1) {
      rpcInit.localPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-i")==0 && i < argc-1) {
      strcpy(rpcInit.localIp, argv[++i]); 
    } else if (strcmp(argv[i], "-n")==0 && i < argc-1) {
      rpcInit.numOfThreads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-m")==0 && i < argc-1) {
      msgSize = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-s")==0 && i < argc-1) {
      rpcInit.sessions = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-o")==0 && i < argc-1) {
      tsCompressMsgSize = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-w")==0 && i < argc-1) {
      commit = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
      rpcDebugFlag = atoi(argv[++i]);
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-c ctype]: connection type:udp or tcp, default is:%s\n", socketType);
      printf("  [-i ip]: server IP address, default is:%s\n", rpcInit.localIp);
      printf("  [-p port]: server port number, default is:%d\n", rpcInit.localPort);
      printf("  [-t threads]: number of threads, default is:%d\n", rpcInit.numOfThreads);
      printf("  [-s sessions]: number of sessions, default is:%d\n", rpcInit.sessions);
      printf("  [-m msgSize]: message body size, default is:%d\n", msgSize);
      printf("  [-o compSize]: compression message size, default is:%d\n", tsCompressMsgSize);
      printf("  [-w write]: write received data to file(0, 1, 2), default is:%d\n", commit);
      printf("  [-d debugFlag]: debug flag, default:%d\n", rpcDebugFlag);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  } 

  rpcInit.connType = strcasecmp(socketType, "udp") == 0 ? TAOS_CONN_UDPS : TAOS_CONN_TCPS;

  taosInitLog("server.log", 100000, 10);

  void *pRpc = rpcOpen(&rpcInit);
  if (pRpc == NULL) {
    tError("failed to start RPC server");
    return -1;
  }

  tPrint("RPC server is running, ctrl-c to exit");

  if (commit) {
    dataFd = open(dataName, O_APPEND | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);  
    if (dataFd<0) 
      tPrint("failed to open data file, reason:%s", strerror(errno));
  }

  // loop forever
  while(1) {
    sleep(1);
  }

  if (dataFd >= 0) {
    close(dataFd);
    remove(dataName);
  }

  return 0;
}


