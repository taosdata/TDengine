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
#include "tglobal.h"
#include "tqueue.h"
#include "transLog.h"
#include "trpc.h"

int         msgSize = 128;
int         commit = 0;
TdFilePtr   pDataFile = NULL;
STaosQueue *qhandle = NULL;
STaosQset * qset = NULL;

void processShellMsg() {
  static int num = 0;
  STaosQall *qall;
  SRpcMsg *  pRpcMsg, rpcMsg;
  int        type;
  void *     pvnode;

  qall = taosAllocateQall();

  while (1) {
    int numOfMsgs = taosReadAllQitemsFromQset(qset, qall, &pvnode, NULL);
    tDebug("%d shell msgs are received", numOfMsgs);
    if (numOfMsgs <= 0) break;

    for (int i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(qall, (void **)&pRpcMsg);

      if (pDataFile != NULL) {
        if (taosWriteFile(pDataFile, pRpcMsg->pCont, pRpcMsg->contLen) < 0) {
          tInfo("failed to write data file, reason:%s", strerror(errno));
        }
      }
    }

    if (commit >= 2) {
      num += numOfMsgs;
      // if (taosFsync(pDataFile) < 0) {
      //  tInfo("failed to flush data to file, reason:%s", strerror(errno));
      //}

      if (num % 10000 == 0) {
        tInfo("%d request have been written into disk", num);
      }
    }

    taosResetQitems(qall);
    for (int i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(qall, (void **)&pRpcMsg);
      rpcFreeCont(pRpcMsg->pCont);

      memset(&rpcMsg, 0, sizeof(rpcMsg));
      rpcMsg.pCont = rpcMallocCont(msgSize);
      rpcMsg.contLen = msgSize;
      rpcMsg.info = pRpcMsg->info;
      rpcMsg.code = 0;
      rpcSendResponse(&rpcMsg);

      taosFreeQitem(pRpcMsg);
    }
  }

  taosFreeQall(qall);
}

int retrieveAuthInfo(void *parent, char *meterId, char *spi, char *encrypt, char *secret, char *ckey) {
  // app shall retrieve the auth info based on meterID from DB or a data file
  // demo code here only for simple demo
  int ret = 0;

  if (strcmp(meterId, "michael") == 0) {
    *spi = 1;
    *encrypt = 0;
    strcpy(secret, "mypassword");
    strcpy(ckey, "key");
  } else if (strcmp(meterId, "jeff") == 0) {
    *spi = 0;
    *encrypt = 0;
  } else {
    ret = -1;  // user not there
  }

  return ret;
}

void processRequestMsg(void *pParent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SRpcMsg *pTemp;

  pTemp = taosAllocateQitem(sizeof(SRpcMsg), DEF_QITEM);
  memcpy(pTemp, pMsg, sizeof(SRpcMsg));

  tDebug("request is received, type:%d, contLen:%d, item:%p", pMsg->msgType, pMsg->contLen, pTemp);
  taosWriteQitem(qhandle, pTemp);
}

int main(int argc, char *argv[]) {
  SRpcInit rpcInit;
  char     dataName[20] = "server.data";

  taosBlockSIGPIPE();

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = 7000;
  rpcInit.label = "SER";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = processRequestMsg;
  rpcInit.sessions = 1000;
  rpcInit.idleTime = 2 * 1500;

  rpcDebugFlag = 131;

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-p") == 0 && i < argc - 1) {
      rpcInit.localPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-t") == 0 && i < argc - 1) {
      rpcInit.numOfThreads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-m") == 0 && i < argc - 1) {
      msgSize = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-s") == 0 && i < argc - 1) {
      rpcInit.sessions = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-o") == 0 && i < argc - 1) {
      tsCompressMsgSize = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-w") == 0 && i < argc - 1) {
      commit = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d") == 0 && i < argc - 1) {
      rpcDebugFlag = atoi(argv[++i]);
      dDebugFlag = rpcDebugFlag;
      uDebugFlag = rpcDebugFlag;
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-p port]: server port number, default is:%d\n", rpcInit.localPort);
      printf("  [-t threads]: number of rpc threads, default is:%d\n", rpcInit.numOfThreads);
      printf("  [-s sessions]: number of sessions, default is:%d\n", rpcInit.sessions);
      printf("  [-m msgSize]: message body size, default is:%d\n", msgSize);
      printf("  [-o compSize]: compression message size, default is:%d\n", tsCompressMsgSize);
      printf("  [-w write]: write received data to file(0, 1, 2), default is:%d\n", commit);
      printf("  [-d debugFlag]: debug flag, default:%d\n", rpcDebugFlag);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  }

  tsAsyncLog = 0;
  rpcInit.connType = TAOS_CONN_SERVER;

  const char *path = TD_TMP_DIR_PATH "transport/server";
  taosRemoveDir(path);
  taosMkDir(path);
  tstrncpy(tsLogDir, path, PATH_MAX);
  taosInitLog("server.log", 10);

  void *pRpc = rpcOpen(&rpcInit);
  if (pRpc == NULL) {
    tError("failed to start RPC server");
    return -1;
  }
  // sleep(5);

  tInfo("RPC server is running, ctrl-c to exit");

  if (commit) {
    pDataFile = taosOpenFile(dataName, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
    if (pDataFile == NULL) tInfo("failed to open data file, reason:%s", strerror(errno));
  }
  qhandle = taosOpenQueue();
  qset = taosOpenQset();
  taosAddIntoQset(qset, qhandle, NULL);

  processShellMsg();

  if (pDataFile != NULL) {
    taosCloseFile(&pDataFile);
    taosRemoveFile(dataName);
  }

  return 0;
}
