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
int64_t  tversion = 0;
void *syncHandle;
int   role;
int  nodeId;
extern uint32_t  tsPrivateIpv4;
char localIp[40] = "0.0.0.0";
char path[256];

int writeIntoWal(SWalHead *pHead)
{
  if (dataFd < 0) {
    char  walName[64];
    sprintf(walName, "d%d/wal/wal.%d", nodeId, walNum);
    dataFd = open(walName, O_APPEND | O_CREAT | O_WRONLY, S_IRWXU | S_IRWXG | S_IRWXO);  
    if (dataFd<0) 
      tPrint("failed to open wal file, reason:%s", strerror(errno));
    return -1;
  }
  
  if ( write(dataFd, pHead, sizeof(SWalHead)+pHead->len) <0 ) {
    tPrint("failed to write wal file, reason:%s", strerror(errno));
  }

  return 0;
}

int processRpcMsg(void *item) {
  SRpcMsg   *pMsg = (SRpcMsg *)item;
  SWalHead  *pHead = (SWalHead *)(((char *)pMsg) - sizeof(SWalHead));
  SRpcMsg    rpcMsg;
  int        code = -1;

  if ( role != TAOS_SYNC_ROLE_MASTER) {
    tError("not master, write fialed", syncRole[role]);
  } else {

    pHead->version = ++tversion;
    pHead->msgType = pMsg->msgType;
    pHead->len = pMsg->contLen;
 
    writeIntoWal(pHead); 
    syncForwardToPeer(syncHandle, pHead, NULL);

    // write into cache

    code = 0;
  }

  rpcFreeCont(rpcMsg.pCont);
  rpcMsg.pCont = rpcMallocCont(msgSize);
  rpcMsg.contLen = msgSize;
  rpcMsg.handle = rpcMsg.handle;
  rpcMsg.code = code;
  rpcSendResponse(&rpcMsg);

  return code;
}

int processFwdMsg(void *item) {

  SWalHead *pHead = (SWalHead *)item;
   
  if (pHead->version <= tversion) {
    tError("version:%d from forward is even higher than local:%d", pHead->version, tversion);
    return -1;
  };

  writeIntoWal(pHead);

  // write into cache

/*
  if (pHead->handle) {
    syncSendFwdAck(syncHandle, pHead->handle, 0);  
  }
*/

  return 1;
}

int processWalMsg(void *item) {

  SWalHead *pHead = (SWalHead *)item;
   
  if (pHead->version <= tversion) {
    tError("version:%d from wal is even higher than local:%d", pHead->version, tversion);
    return -1;
  };

  writeIntoWal(pHead);

  // write into cache

/*
  if (pHead->handle) {
    syncSendFwdAck(syncHandle, pHead->handle, 0);  
  }
*/

  return 1;
}

void *processWriteQueue(void *param) {
  static int num = 0;
  taos_qall  qall;
  int        type;
  void      *item;

  while (1) {
    int numOfMsgs = taosReadAllQitems(qhandle, &qall);
    if (numOfMsgs <= 0) {
      usleep(1000);
      continue;
    }     

    tTrace("%d msgs are received", numOfMsgs);
    int numOfWrites = 0;

    for (int i=0; i<numOfMsgs; ++i) {
      taosGetQitem(qall, &type, &item);

      if (type == TAOS_QTYPE_RPC) {
        if (processRpcMsg(item) > 0) numOfWrites++;
      } else if (type == TAOS_QTYPE_WAL) {
        if ( processWalMsg(item) > 0) numOfWrites++;
      } else if (type == TAOS_QTYPE_FWD) {
        if ( processFwdMsg(item) >0) numOfWrites++;
      }
    } 

    num += numOfWrites;
    if (num > 100000) {
      tPrint("%d request have been written into disk", num);
      close(dataFd);
      dataFd = -1;
      walNum++;
      num = 0;
    }
 
    taosFreeQitems(qall);
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
  tTrace("request is received, type:%d, contLen:%d", pMsg->msgType, pMsg->contLen);
  
  SRpcMsg *pTemp;

  pTemp = taosAllocateQitem(sizeof(SRpcMsg));
  *pTemp = *pMsg;
  
  taosWriteQitem(qhandle, TAOS_QTYPE_RPC, pTemp); 
}

uint32_t getFileInfo(char *name, int *index, int *size) 
{
  uint32_t     magic;
  struct stat  fstat;
  char         aname[256];

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

  if (*index >= walNum -1) return 0;

  if (name[0] == 0) {
    // find the file 
    sprintf(aname, "%s/wal/wal.%d", path, *index);
    sprintf(name, "data/wal.%d", *index); 
  } else {
    sprintf(aname, "%s/%s", path, name);
  }

  dPrint("get wal info:%s", aname);
  if ( stat(aname, &fstat) < 0 ) return -1;

  if (*index >= walNum-1) return 0;  // no more

  return 1;

}

int writeToCache(void *ahandle, SWalHead *pHead, int type) {

  tTrace("forward is received, type:%d, contLen:%d", pHead->msgType, pHead->len);

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

SSyncInfo syncInfo;

void initSync() {

  strcpy(syncInfo.label, "vid:1");
  syncInfo.replica = 1;
  syncInfo.quorum = 0;
  syncInfo.vgId = 1;
  syncInfo.ahandle = &syncInfo;
  syncInfo.getFileInfo = getFileInfo;
  syncInfo.getWalInfo = getWalInfo;
  syncInfo.writeToCache = writeToCache;
  syncInfo.confirmFwd = NULL;
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

  sprintf(path, "~/test/d%d", nodeId);
  strcpy(syncInfo.path, path);

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
      printf("  [-r replica]: initial replica, default is:%d\n", syncInfo.replica);
      printf("  [-d debugFlag]: debug flag, default:%d\n", rpcDebugFlag);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  }
 
  uDebugFlag = rpcDebugFlag;
  ddebugFlag = rpcDebugFlag; 
  tmrDebugFlag = rpcDebugFlag; 
  tsAsyncLog = 0;
  taosInitLog("server.log", 100000, 10);

  rpcInit.connType = TAOS_CONN_SERVER;
  void *pRpc = rpcOpen(&rpcInit);
  if (pRpc == NULL) {
    tError("failed to start RPC server");
    return -1;
  }

  tPrint("RPC server is running, ctrl-c to exit");

  qhandle = taosOpenQueue();

  tsPrivateIpv4 = inet_addr(localIp);
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


