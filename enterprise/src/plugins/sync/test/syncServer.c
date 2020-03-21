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
#include "trpc.h"
#include "tqueue.h"
#include "tsync.h"

int msgSize = 128;
int commit = 0;
int dataFd = -1;
void *qhandle = NULL;
int walNum = 0;
uint64_t  tversion = 0;
void *syncHandle;
int   role;
int  nodeId;
extern uint32_t  tsPrivateIpv4;
char localIp[40] = "0.0.0.0";
char path[256];
int  numOfWrites ;
SSyncInfo syncInfo;

int writeIntoWal(SWalHead *pHead)
{ 
  if (dataFd < 0) {
    char  walName[64];
    sprintf(walName, "%s/wal/wal.%d", path, walNum);
    remove(walName);
    dataFd = open(walName, O_CREAT | O_WRONLY, S_IRWXU | S_IRWXG | S_IRWXO);  
    if (dataFd < 0) { 
      dPrint("failed to open wal file:%s(%s)", walName, strerror(errno));
      return -1;
    } else {
      walNum++;
      dPrint("file:%s is opened to write, walNum:%d", walName, walNum);
    }
  }
  
  if ( write(dataFd, pHead, sizeof(SWalHead)+pHead->len) <0 ) {
    dError("ver:%d, failed to write wal file(%s)", pHead->version, strerror(errno));
  } else {
    dTrace("ver:%d, written to wal", pHead->version);
  }

  numOfWrites++;
  if (numOfWrites >= 10000) {
    tPrint("%d request have been written into disk", numOfWrites);
    close(dataFd);
    dataFd = -1;
    numOfWrites = 0;
  }
 
  return 0;
}

void confirmForward(void *ahandle, void *mhandle, int32_t code)
{
  SRpcMsg  *pMsg = (SRpcMsg *)mhandle;
  SWalHead *pHead = (SWalHead *)(((char *)pMsg->pCont) - sizeof(SWalHead));

  dTrace("ver:%d, confirm is received", pHead->version);

  rpcFreeCont(pMsg->pCont);

  SRpcMsg    rpcMsg;
  rpcMsg.pCont = rpcMallocCont(msgSize);
  rpcMsg.contLen = msgSize;
  rpcMsg.handle = pMsg->handle;
  rpcMsg.code = code;
  rpcSendResponse(&rpcMsg);

  taosFreeQitem(mhandle); 
}

int processRpcMsg(void *item) {
  SRpcMsg   *pMsg = (SRpcMsg *)item;
  SWalHead  *pHead = (SWalHead *)(((char *)pMsg->pCont) - sizeof(SWalHead));
  int        code = -1;

  if (role != TAOS_SYNC_ROLE_MASTER) {
    dError("not master, write failed", syncRole[role]);
  } else {

    pHead->version = ++tversion;
    pHead->msgType = pMsg->msgType;
    pHead->len = pMsg->contLen;
 
    dTrace("ver:%d, pkt from client processed", pHead->version);
    writeIntoWal(pHead); 
    syncForwardToPeer(syncHandle, pHead, item);

    code = 0;
  }

  if (syncInfo.quorum <= 1) { 
    taosFreeQitem(item); 
    rpcFreeCont(pMsg->pCont);

    SRpcMsg    rpcMsg;
    rpcMsg.pCont = rpcMallocCont(msgSize);
    rpcMsg.contLen = msgSize;
    rpcMsg.handle = pMsg->handle;
    rpcMsg.code = code;
    rpcSendResponse(&rpcMsg);
  }

  return code;
}

int processFwdMsg(void *item) {

  SWalHead *pHead = (SWalHead *)item;
   
  if (pHead->version <= tversion) {
    dError("ver:%d, forward is even lower than local:%d", pHead->version, tversion);
    return -1;
  };

  dTrace("ver:%d, forward from peer is received", pHead->version);
  writeIntoWal(pHead);
  tversion = pHead->version;

  if (syncInfo.quorum > 1) syncConfirmForward(syncHandle, pHead->version, 0);

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
    dError("ver:%d, wal is even lower than local:%d", pHead->version, tversion);
    return -1;
  };

  dTrace("ver:%d, wal from peer is received", pHead->version);
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
  int        type;
  void      *item;

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

void processRequestMsg(SRpcMsg *pMsg) {
  
  SRpcMsg *pTemp;

  pTemp = taosAllocateQitem(sizeof(SRpcMsg));
  memcpy(pTemp, pMsg, sizeof(SRpcMsg));
  
  tTrace("request is received, type:%d, len:%d", pMsg->msgType, pMsg->contLen);
  taosWriteQitem(qhandle, TAOS_QTYPE_RPC, pTemp); 
}

uint32_t getFileInfo(char *name, int *index, int *size) 
{
  uint32_t     magic;
  struct stat  fstat;
  char         aname[256];

  if (*index == 2) {
    dPrint("wait for a while .....");
    sleep(3);
  }

  if (name[0] == 0) {
    // find the file 
    sprintf(aname, "%s/data/data.%d", path, *index);
    sprintf(name, "data/data.%d", *index); 
  } else {
    sprintf(aname, "%s/%s", path, name);
  }

  dPrint("get file info:%s", aname);
  if ( stat(aname, &fstat) < 0 ) return 0; 

  *size = fstat.st_size;
  magic = fstat.st_size;

  return magic;
}

int  getWalInfo(char *name, int *index) {

  struct stat  fstat;
  char         aname[256];

  name[0] = 0;
  if (*index > walNum -1) return 0;

  sprintf(aname, "%s/wal/wal.%d", path, *index);
  sprintf(name, "wal/wal.%d", *index); 
  dPrint("get wal info:%s", aname);

  if ( stat(aname, &fstat) < 0 ) return -1;

  if (*index >= walNum-1) return 0;  // no more

  return 1;

}

int writeToCache(void *ahandle, SWalHead *pHead, int type) {

  dTrace("pkt from peer is received, ver:%d len:%d type:%d", pHead->version, pHead->len, type);

  int   msgSize = pHead->len + sizeof(SWalHead);
  void *pMsg = taosAllocateQitem(msgSize);
  memcpy(pMsg, pHead, msgSize);
  taosWriteQitem(qhandle, type, pMsg); 

  return 0;
}

void confirmFwd(void *ahandle, int64_t version) {

  return;
}

void notifyRole(void *ahandle, int8_t r) {
  role = r;
  printf("current role:%s\n", syncRole[role]);
}


void initSync() {

  strcpy(syncInfo.label, "vid:1");
  syncInfo.replica = 1;
  syncInfo.quorum = 1;
  syncInfo.vgId = 1;
  syncInfo.ahandle = &syncInfo;
  syncInfo.getFileInfo = getFileInfo;
  syncInfo.getWalInfo = getWalInfo;
  syncInfo.writeToCache = writeToCache;
  syncInfo.confirmForward = confirmForward;
  syncInfo.notifyRole = notifyRole;
  syncInfo.nodeInfo[0].nodeId = 1;
  strcpy(syncInfo.nodeInfo[0].name, "192.168.0.1");
  syncInfo.nodeInfo[1].nodeId = 2;
  strcpy(syncInfo.nodeInfo[1].name, "192.168.0.2");
  syncInfo.nodeInfo[2].nodeId = 3;
  strcpy(syncInfo.nodeInfo[2].name, "192.168.0.3");
  syncInfo.nodeInfo[3].nodeId = 4;
  strcpy(syncInfo.nodeInfo[3].name, "192.168.0.4");
  syncInfo.nodeInfo[4].nodeId = 5;
  strcpy(syncInfo.nodeInfo[4].name, "192.168.0.5");

}

void doSync()
{
  for (int i=0; i<5; ++i) {
    syncInfo.nodeInfo[i].nodeIp = inet_addr(syncInfo.nodeInfo[i].name);
    if ( strcmp(localIp, syncInfo.nodeInfo[i].name) == 0 ) 
      nodeId = syncInfo.nodeInfo[i].nodeId;
  }

  sprintf(path, "/home/jhtao/test/d%d", nodeId);
  strcpy(syncInfo.path, path);

  if ((syncInfo.replica & 1) == 0) {
    syncInfo.arbitratorIp = inet_addr("192.168.0.6");
  } else {
    syncInfo.arbitratorIp = 0;
  }

  if ( syncHandle == NULL) {
      syncHandle = syncStart(&syncInfo);
  } else {
      if (syncReconfig(syncHandle, &syncInfo) < 0) syncHandle = NULL;
  }

  dPrint("nodeId:%d path:%s localIp:%s", nodeId, path, localIp);
}

int main(int argc, char *argv[]) {
  SRpcInit rpcInit;
  char     dataName[20] = "server.data";

  initSync();

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = localIp;
  rpcInit.localPort    = 7000;
  rpcInit.label        = "SER";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = processRequestMsg;
  rpcInit.sessions     = 1000;
  rpcInit.idleTime     = tsShellActivityTimer*1500; 
  rpcInit.afp          = retrieveAuthInfo;

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-p")==0 && i < argc-1) {
      rpcInit.localPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-i")==0 && i < argc-1) {
      strcpy(localIp, argv[++i]); 
    } else if (strcmp(argv[i], "-t")==0 && i < argc-1) {
      rpcInit.numOfThreads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-m")==0 && i < argc-1) {
      msgSize = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-s")==0 && i < argc-1) {
      rpcInit.sessions = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-o")==0 && i < argc-1) {
      tsCompressMsgSize = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-w")==0 && i < argc-1) {
      commit = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-v")==0 && i < argc-1) {
      syncInfo.version = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-r")==0 && i < argc-1) {
      syncInfo.replica = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-q")==0 && i < argc-1) {
      syncInfo.quorum = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
      rpcDebugFlag = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
      rpcDebugFlag = atoi(argv[++i]);
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-i ip]: server IP address, default is:%s\n", rpcInit.localIp);
      printf("  [-p port]: server port number, default is:%d\n", rpcInit.localPort);
      printf("  [-t threads]: number of rpc threads, default is:%d\n", rpcInit.numOfThreads);
      printf("  [-s sessions]: number of sessions, default is:%d\n", rpcInit.sessions);
      printf("  [-m msgSize]: message body size, default is:%d\n", msgSize);
      printf("  [-o compSize]: compression message size, default is:%d\n", tsCompressMsgSize);
      printf("  [-w write]: write received data to file(0, 1, 2), default is:%d\n", commit);
      printf("  [-v version]: initial node version, default is:%ld\n", syncInfo.version);
      printf("  [-r replica]: replicacation number, default is:%d\n", syncInfo.replica);
      printf("  [-q quorum]: quorum, default is:%d\n", syncInfo.quorum);
      printf("  [-d debugFlag]: debug flag, default:%d\n", rpcDebugFlag);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  }
 
  uDebugFlag = rpcDebugFlag;
  ddebugFlag = rpcDebugFlag; 
  //tmrDebugFlag = rpcDebugFlag; 
  tsAsyncLog = 0;
  taosInitLog("server.log", 1000000, 10);

  rpcInit.connType = TAOS_CONN_SERVER;
  void *pRpc = rpcOpen(&rpcInit);
  if (pRpc == NULL) {
    tError("failed to start RPC server");
    return -1;
  }

  qhandle = taosOpenQueue();

  strcpy(tsPrivateIp, localIp);
  doSync();

  pthread_attr_t thattr;
  pthread_t      thread;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  if (pthread_create(&thread, &thattr, processWriteQueue, NULL) != 0) {
    tError("failed to create thread, reason:%s", strerror(errno));
    return -1;
  }

  printf("server is running, ip:%s\n", rpcInit.localIp);
  SNodesRole nroles;

  while (1) {
    char c = getchar();

    switch(c) {
      case '1':
        syncInfo.replica = 1; doSync();
        break;        
      case '2':
        syncInfo.replica = 2; doSync();
        break;
      case '3':
        syncInfo.replica = 3; doSync();
        break;
      case '4':
        syncInfo.replica = 4; doSync();
        break;
      case '5':
        syncInfo.replica = 5; doSync();
        break;
      case 's':
        syncGetNodesRole(syncHandle, &nroles);
        for (int i=0; i<syncInfo.replica; ++i) 
          dPrint("=== nodeId:%d role:%s", nroles.nodeId[i], syncRole[nroles.role[i]]);
        break;
      default:
        break;
    }

  }

  if (dataFd >= 0) {
    close(dataFd);
    remove(dataName);
  }

  return 0;
}


