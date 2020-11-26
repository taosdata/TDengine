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
#include "tulog.h"
#include "tglobal.h"
#include "tsocket.h"
#include "trpc.h"
#include "tqueue.h"
#include "twal.h"
#include "tsync.h"

int       msgSize = 128;
int       commit = 0;
int       dataFd = -1;
void *    qhandle = NULL;
int       walNum = 0;
uint64_t  tversion = 0;
int64_t   syncHandle;
int       role;
int       nodeId;
char      path[256];
int       numOfWrites;
SSyncInfo syncInfo;
SSyncCfg *pCfg;

int writeIntoWal(SWalHead *pHead) {
  if (dataFd < 0) {
    char walName[280];
    snprintf(walName, sizeof(walName), "%s/wal/wal.%d", path, walNum);
    (void)remove(walName);
    dataFd = open(walName, O_CREAT | O_WRONLY, S_IRWXU | S_IRWXG | S_IRWXO);
    if (dataFd < 0) {
      uInfo("failed to open wal file:%s(%s)", walName, strerror(errno));
      return -1;
    } else {
      walNum++;
      uInfo("file:%s is opened to write, walNum:%d", walName, walNum);
    }
  }

  if (write(dataFd, pHead, sizeof(SWalHead) + pHead->len) < 0) {
    uError("ver:%" PRIu64 ", failed to write wal file(%s)", pHead->version, strerror(errno));
  } else {
    uDebug("ver:%" PRIu64 ", written to wal", pHead->version);
  }

  numOfWrites++;
  if (numOfWrites >= 10000) {
    uInfo("%d request have been written into disk", numOfWrites);
    close(dataFd);
    dataFd = -1;
    numOfWrites = 0;
  }

  return 0;
}

void confirmForward(int32_t vgId, void *mhandle, int32_t code) {
  SRpcMsg * pMsg = (SRpcMsg *)mhandle;
  SWalHead *pHead = (SWalHead *)(((char *)pMsg->pCont) - sizeof(SWalHead));

  uDebug("ver:%" PRIu64 ", confirm is received", pHead->version);

  rpcFreeCont(pMsg->pCont);

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = rpcMallocCont(msgSize);
  rpcMsg.contLen = msgSize;
  rpcMsg.handle = pMsg->handle;
  rpcMsg.code = code;
  rpcSendResponse(&rpcMsg);

  taosFreeQitem(mhandle);
}

int processRpcMsg(void *item) {
  SRpcMsg * pMsg = (SRpcMsg *)item;
  SWalHead *pHead = (SWalHead *)(((char *)pMsg->pCont) - sizeof(SWalHead));
  int       code = -1;

  if (role != TAOS_SYNC_ROLE_MASTER) {
    uError("not master, write failed, role:%s", syncRole[role]);
  } else {
    pHead->version = ++tversion;
    pHead->msgType = pMsg->msgType;
    pHead->len = pMsg->contLen;

    uDebug("ver:%" PRIu64 ", pkt from client processed", pHead->version);
    writeIntoWal(pHead);
    syncForwardToPeer(syncHandle, pHead, item, TAOS_QTYPE_RPC);

    code = 0;
  }

  if (pCfg->quorum <= 1) {
    rpcFreeCont(pMsg->pCont);

    SRpcMsg rpcMsg = {0};
    rpcMsg.pCont = rpcMallocCont(msgSize);
    rpcMsg.contLen = msgSize;
    rpcMsg.handle = pMsg->handle;
    rpcMsg.code = code;
    rpcSendResponse(&rpcMsg);
    taosFreeQitem(item);
  }

  return code;
}

int processFwdMsg(void *item) {
  SWalHead *pHead = (SWalHead *)item;

  if (pHead->version <= tversion) {
    uError("ver:%" PRIu64 ", forward is even lower than local:%" PRIu64, pHead->version, tversion);
    return -1;
  }

  uDebug("ver:%" PRIu64 ", forward from peer is received", pHead->version);
  writeIntoWal(pHead);
  tversion = pHead->version;

  if (pCfg->quorum > 1) syncConfirmForward(syncHandle, pHead->version, 0);

  // write into cache

  /*
    if (pHead->handle) {
      syncSendFwdAck(syncHandle, pHead->handle, 0);
    }
  */

  taosFreeQitem(item);

  return 0;
}

int processWalMsg(void *item) {
  SWalHead *pHead = (SWalHead *)item;

  if (pHead->version <= tversion) {
    uError("ver:%" PRIu64 ", wal is even lower than local:%" PRIu64, pHead->version, tversion);
    return -1;
  };

  uDebug("ver:%" PRIu64 ", wal from peer is received", pHead->version);
  writeIntoWal(pHead);
  tversion = pHead->version;

  // write into cache

  /*
    if (pHead->handle) {
      syncSendFwdAck(syncHandle, pHead->handle, 0);
    }
  */

  taosFreeQitem(item);

  return 0;
}

void *processWriteQueue(void *param) {
  int   type;
  void *item;

  while (1) {
    int ret = taosReadQitem(qhandle, &type, &item);
    if (ret <= 0) {
      usleep(1000);
      continue;
    }

    if (type == TAOS_QTYPE_RPC) {
      processRpcMsg(item);
    } else if (type == TAOS_QTYPE_WAL) {
      processWalMsg(item);
    } else if (type == TAOS_QTYPE_FWD) {
      processFwdMsg(item);
    }
  }

  return NULL;
}

int retrieveAuthInfo(char *meterId, char *spi, char *encrypt, char *secret, char *ckey) {
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

void processRequestMsg(SRpcMsg *pMsg, SRpcEpSet *pEpSet) {
  SRpcMsg *pTemp;

  pTemp = taosAllocateQitem(sizeof(SRpcMsg));
  memcpy(pTemp, pMsg, sizeof(SRpcMsg));

  uDebug("request is received, type:%d, len:%d", pMsg->msgType, pMsg->contLen);
  taosWriteQitem(qhandle, TAOS_QTYPE_RPC, pTemp);
}

uint32_t getFileInfo(int32_t vgId, char *name, uint32_t *index, uint32_t eindex, int64_t *size, uint64_t *fversion) {
  uint32_t    magic;
  struct stat fstat;
  char        aname[280];

  if (*index == 2) {
    uInfo("wait for a while .....");
    sleep(3);
  }

  if (name[0] == 0) {
    // find the file
    snprintf(aname, sizeof(aname), "%s/data/data.%d", path, *index);
    sprintf(name, "data/data.%d", *index);
  } else {
    snprintf(aname, sizeof(aname), "%s/%s", path, name);
  }

  uInfo("get file info:%s", aname);
  if (stat(aname, &fstat) < 0) return 0;

  *size = fstat.st_size;
  magic = fstat.st_size;

  return magic;
}

int getWalInfo(int32_t vgId, char *name, int64_t *index) {
  struct stat fstat;
  char        aname[280];

  name[0] = 0;
  if (*index + 1 > walNum) return 0;

  snprintf(aname, sizeof(aname), "%s/wal/wal.%d", path, *index);
  sprintf(name, "wal/wal.%d", *index);
  uInfo("get wal info:%s", aname);

  if (stat(aname, &fstat) < 0) return -1;

  if (*index >= walNum - 1) return 0;  // no more

  return 1;
}

int writeToCache(int32_t vgId, void *data, int type) {
  SWalHead *pHead = data;

  uDebug("pkt from peer is received, ver:%" PRIu64 " len:%d type:%d", pHead->version, pHead->len, type);

  int   msgSize = pHead->len + sizeof(SWalHead);
  void *pMsg = taosAllocateQitem(msgSize);
  memcpy(pMsg, pHead, msgSize);
  taosWriteQitem(qhandle, type, pMsg);

  return 0;
}

void confirmFwd(int32_t vgId, int64_t version) { return; }

void notifyRole(int32_t vgId, int8_t r) {
  role = r;
  printf("current role:%s\n", syncRole[role]);
}

void initSync() {
  pCfg->replica = 1;
  pCfg->quorum = 1;
  syncInfo.vgId = 1;
  syncInfo.getFileInfo = getFileInfo;
  syncInfo.getWalInfo = getWalInfo;
  syncInfo.writeToCache = writeToCache;
  syncInfo.confirmForward = confirmForward;
  syncInfo.notifyRole = notifyRole;

  pCfg->nodeInfo[0].nodeId = 1;
  pCfg->nodeInfo[0].nodePort = 7010;
  taosGetFqdn(pCfg->nodeInfo[0].nodeFqdn);

  pCfg->nodeInfo[1].nodeId = 2;
  pCfg->nodeInfo[1].nodePort = 7110;
  taosGetFqdn(pCfg->nodeInfo[1].nodeFqdn);

  pCfg->nodeInfo[2].nodeId = 3;
  pCfg->nodeInfo[2].nodePort = 7210;
  taosGetFqdn(pCfg->nodeInfo[2].nodeFqdn);

  pCfg->nodeInfo[3].nodeId = 4;
  pCfg->nodeInfo[3].nodePort = 7310;
  taosGetFqdn(pCfg->nodeInfo[3].nodeFqdn);

  pCfg->nodeInfo[4].nodeId = 5;
  pCfg->nodeInfo[4].nodePort = 7410;
  taosGetFqdn(pCfg->nodeInfo[4].nodeFqdn);
}

void doSync() {
  for (int i = 0; i < 5; ++i) {
    if (tsSyncPort == pCfg->nodeInfo[i].nodePort) nodeId = pCfg->nodeInfo[i].nodeId;
  }

  snprintf(path, sizeof(path), "/root/test/d%d", nodeId);
  tstrncpy(syncInfo.path, path, sizeof(syncInfo.path));

  if (syncHandle == NULL) {
    syncHandle = syncStart(&syncInfo);
  } else {
    if (syncReconfig(syncHandle, pCfg) < 0) syncHandle = NULL;
  }

  uInfo("nodeId:%d path:%s syncPort:%d", nodeId, path, tsSyncPort);
}

int main(int argc, char *argv[]) {
  SRpcInit rpcInit;
  char     dataName[20] = "server.data";
  pCfg = &syncInfo.syncCfg;

  initSync();

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort = 7000;
  rpcInit.label = "SER";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = processRequestMsg;
  rpcInit.sessions = 1000;
  rpcInit.idleTime = tsShellActivityTimer * 1500;
  rpcInit.afp = retrieveAuthInfo;

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
    } else if (strcmp(argv[i], "-v") == 0 && i < argc - 1) {
      syncInfo.version = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-r") == 0 && i < argc - 1) {
      pCfg->replica = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-q") == 0 && i < argc - 1) {
      pCfg->quorum = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d") == 0 && i < argc - 1) {
      rpcDebugFlag = atoi(argv[++i]);
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-p port]: server port number, default is:%d\n", rpcInit.localPort);
      printf("  [-t threads]: number of rpc threads, default is:%d\n", rpcInit.numOfThreads);
      printf("  [-s sessions]: number of sessions, default is:%d\n", rpcInit.sessions);
      printf("  [-m msgSize]: message body size, default is:%d\n", msgSize);
      printf("  [-o compSize]: compression message size, default is:%d\n", tsCompressMsgSize);
      printf("  [-w write]: write received data to file(0, 1, 2), default is:%d\n", commit);
      printf("  [-v version]: initial node version, default is:%" PRId64 "\n", syncInfo.version);
      printf("  [-r replica]: replicacation number, default is:%d\n", pCfg->replica);
      printf("  [-q quorum]: quorum, default is:%d\n", pCfg->quorum);
      printf("  [-d debugFlag]: debug flag, default:%d\n", rpcDebugFlag);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  }

  uDebugFlag = rpcDebugFlag;
  dDebugFlag = rpcDebugFlag;
  // tmrDebugFlag = rpcDebugFlag;
  tsAsyncLog = 0;
  taosInitLog("server.log", 1000000, 10);

  rpcInit.connType = TAOS_CONN_SERVER;
  void *pRpc = rpcOpen(&rpcInit);
  if (pRpc == NULL) {
    uError("failed to start RPC server");
    return -1;
  }

  tsSyncPort = rpcInit.localPort + 10;
  qhandle = taosOpenQueue();

  doSync();

  pthread_attr_t thattr;
  pthread_t      thread;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&thread, &thattr, processWriteQueue, NULL) != 0) {
    uError("failed to create thread, reason:%s", strerror(errno));
    return -1;
  }

  printf("server is running, localPort:%d\n", rpcInit.localPort);
  SNodesRole nroles;

  while (1) {
    int c = getchar();

    switch (c) {
      case '1':
        pCfg->replica = 1;
        doSync();
        break;
      case '2':
        pCfg->replica = 2;
        doSync();
        break;
      case '3':
        pCfg->replica = 3;
        doSync();
        break;
      case '4':
        pCfg->replica = 4;
        doSync();
        break;
      case '5':
        pCfg->replica = 5;
        doSync();
        break;
      case 's':
        syncGetNodesRole(syncHandle, &nroles);
        for (int i = 0; i < pCfg->replica; ++i)
          printf("=== nodeId:%d role:%s\n", nroles.nodeId[i], syncRole[nroles.role[i]]);
        break;
      default:
        break;
    }

    if (c == 'q') break;
  }

  syncStop(syncHandle);

  if (dataFd >= 0) {
    close(dataFd);
    remove(dataName);
  }

  return 0;
}
