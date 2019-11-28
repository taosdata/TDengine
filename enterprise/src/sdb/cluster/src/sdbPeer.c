/*******************************************************************
 *           Copyright (c) 2017 by TAOS Technologies, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Technologies.
 *  No part of this file may be reproduced, stored, transmitted,
 *  disclosed or used in any form or by any means other than as
 *  expressly provided by the written permission from Jianhui Tao
 *
 * ****************************************************************/
#define _DEFAULT_SOURCE
#include <arpa/inet.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "sdb.h"
#include "sdbint.h"
#include "trpc.h"
#include "tsocket.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

#define MAX_TRY_WAIT_TIMES 2000
#define TRY_WAIT_TIME_IN_MS 1

#pragma GCC diagnostic ignored "-Wpointer-sign"

extern void *mgmtTranQhandle;
void *       pPeerConn = NULL;  // for mnode-mnode communication
void *       sdbTmr;
void *       mnodeSdb;
void *       sdbRoleTimer;
int          sdbNumOfPeers = 0;
SSdbPeer *   sdbPeer[SDB_MAX_PEERS];  // first slot for self
STranQueue   sdbQueue;
int          sdbCode = 0;
sem_t        sdbSem;
uint32_t     selfIp;
uint32_t     sdbPublicIp;
uint32_t     sdbMasterStartTime;
void (*sdbWorkAsMasterCallback)();
SIpList *       pSdbIpList = NULL;
SIpList *       pSdbPublicIpList = NULL;
pthread_mutex_t sdbMutex;
void *          sdbQhandle;
int             tsMnodeUpdateSize;

// configurable
int   sdbMaxNodes = 100;
char  sdbZone[24] = "sbdroot";
char  sdbMasterIp[24];
char  sdbPrivateIp[24];

#define pSelf (sdbPeer[0])
#define sdbStatus (sdbPeer[0]->status)
#define SDB_BUFFER_SIZE 1024000

void *sdbProcessMsgFromPeer(char *msg, void *ahandle, void *thandle);
int sdbProcessHeartBeatFromPeer(char *pMsg, int msgLen, SSdbPeer *pPeer);
int sdbProcessHeartBeatRspFromPeer(char *pMsg, int msgLen, SSdbPeer *pPeer);
void sdbCheckPeerStatus(void *param, void *tmrId);
void sdbCheckRoleStatus(void *param, void *tmrId);
void sdbConfigPeers(int numOfPeers, uint32_t peerIp[]);
int sdbProcessForwardMsg(char *cont, int contLen, SSdbPeer *pPeer);
int sdbProcessForwardRspMsg(char *cont, int contLen, SSdbPeer *pPeer);
int sdbProcessSyncRequest(char *pMsg, int msgLen, SSdbPeer *pPeer);
void *sdbRetrieveSyncData(void *param);
void sdbStartSyncProcess(SSdbPeer *pPeer);
void *sdbAcceptSyncTcpConnection(void *argv);
int sdbProcessBufferedForwards();
int sdbProcessDbReq(char *cont, int contLen);
int sdbProcessQueuedDbReq(char *cont, int contLen);
void sdbUpdateIpList();
void sdbCheckSelfRole();
int sdbProcessCfgMnodeMsg(char *cont, int contLen, SSdbPeer *pPeer);

const char *taosGetSdbRoleStr(int sdbRole) {
  switch (sdbRole) {
    case SDB_ROLE_UNAPPROVED: return "unapproved";
    case SDB_ROLE_UNDECIDED:  return "undecided";
    case SDB_ROLE_MASTER:     return "master";
    case SDB_ROLE_SLAVE:      return "slave";
    default:                  return "undefined";
  }
}

const char *taosGetSdbStatusStr(int status) {
  switch (status) {
    case SDB_STATUS_OFFLINE:  return "offline";
    case SDB_STATUS_UNSYNCED: return "unsynced";
    case SDB_STATUS_SYNCING:  return "syncing";
    case SDB_STATUS_SERVING:  return "serving";
    case SDB_STATUS_DELETED:  return "deleted";
    default:                  return "undefined";
  }
}

const char *taosGetSdbTableName(int dbId) {
  switch (dbId) {
    case 0:  return "account";
    case 1:  return "user";
    case 2:  return "dnodes";
    case 3:  return "db";
    case 4:  return "vgroups";
    case 5:  return "meters";
    case 6:  return "mnode";
    default: return "undefined";
  }
}

const char *taosGetSdbOperName(int oper) {
  switch (oper) {
    case SDB_TYPE_INSERT:              return "insert";
    case SDB_TYPE_DELETE:              return "delete";
    case SDB_TYPE_UPDATE:              return "update";
    case SDB_TYPE_DECODE:              return "decode";
    case SDB_TYPE_ENCODE:              return "encode";
    case SDB_TYPE_BEFORE_BATCH_UPDATE: return "before_batch_update";
    case SDB_TYPE_BATCH_UPDATE:        return "batch_update";
    case SDB_TYPE_AFTER_BATCH_UPDATE:  return "after_batch_update";
    case SDB_TYPE_RESET:               return "reset";
    case SDB_TYPE_DESTROY:             return "destroy";
    case SDB_MAX_ACTION_TYPES:         return "invalid";
    default:                           return "undefined";
  }
}

void sdbProcessForwardRequest(SSchedMsg *pSchedMsg) {
  SIntMsg * pMsg = (SIntMsg *)pSchedMsg->msg;
  SSdbPeer *pPeer = (SSdbPeer *)pSchedMsg->ahandle;

  sdbProcessForwardMsg(pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pPeer);

  if (pSchedMsg->msg) free(pSchedMsg->msg);
}

int64_t sdbGetTimeStamp() {
  struct timeval systemTime;

  gettimeofday(&systemTime, NULL);
  return (int64_t)systemTime.tv_sec * 1000 + systemTime.tv_usec / 1000;
}

void sdbWorkAsMaster() {
  sdbLPrint("dnode:%s start to work as master", tsPrivateIp);

  pSelf->role = SDB_ROLE_MASTER;
  pSelf->status = SDB_STATUS_SERVING;
  sdbMaster = 1;
  sdbMasterStartTime = taosGetTimestampSec();

  sdbUpdateIpList();
  (*sdbWorkAsMasterCallback)();
}

void sdbStopWorkingAsMaster() {
  sdbLPrint("dnode:%s stop working as Master", tsPrivateIp);

  pSelf->role = SDB_ROLE_UNDECIDED;
  taosTmrReset(sdbCheckRoleStatus, tsMgmtPeerHBTimer * 1000, NULL, sdbTmr, &sdbRoleTimer);
  sdbMaster = 0;

  sdbUpdateIpList();
}

SSdbPeer *sdbAddPeer(uint32_t ip, uint32_t publicIp, char role) {
  SSdbPeer *pPeer;

  pPeer = (SSdbPeer *)malloc(sizeof(SSdbPeer));
  memset(pPeer, 0, sizeof(SSdbPeer));
  pPeer->ip = ip;
  pPeer->publicIp = publicIp;
  tinet_ntoa(pPeer->ipstr, pPeer->ip);
  strcpy(pPeer->zone, sdbZone);
  pPeer->createdTime = sdbGetTimeStamp();
  pPeer->role = role;

  if (sdbInsertRow(mnodeSdb, pPeer, 0) > 0) {
    sdbTrace("sdb peer:%s is added", pPeer->ipstr);
  } else {
    //sdbError("failed to add sdb peer:%s", pPeer->ipstr);
    tfree(pPeer);
  }

  return pPeer;
}

int sdbRemovePeer(SSdbPeer *pPeer) {
  if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED || pPeer->ip == 0) return 0;

  if (pPeer->ip == selfIp) {
    sdbWarn("could not remove self IP");
    return 0;
  } else {
    sdbTrace("sdb peer:%s will be removed", pPeer->ipstr);
  }

  sdbDeleteRow(mnodeSdb, &(pPeer->ip));

  return 0;
}

int sdbRemovePeerByIp(uint32_t ip) {
  SSdbPeer *pPeer;

  pPeer = sdbGetRow(mnodeSdb, &ip);
  if (pPeer == NULL) {
    sdbError("sdb peer:%s not exist, can not remove", taosIpStr(ip));
    return TSDB_CODE_INVALID_VALUE;
  }

  sdbRemovePeer(pPeer);

  return 0;
}

void sdbNewPeerAdded(SSdbPeer *pPeer) {
  int i;

  pPeer->hbTimer = NULL;
  pPeer->syncFd = -1;
  pPeer->thandle = NULL;
  pPeer->pSync = NULL;
  pPeer->status = SDB_STATUS_OFFLINE;

  if (pPeer->ip == selfIp) {
    if (pSelf && pSelf->status != SDB_STATUS_DELETED) memcpy(pPeer, pSelf, sizeof(SSdbPeer));
    pSelf = pPeer;
  } else {
    for (i = 1; i < SDB_MAX_PEERS; ++i) {
      if (sdbPeer[i] == NULL || sdbPeer[i]->status == SDB_STATUS_DELETED) {
        sdbPeer[i] = pPeer;
        taosTmrReset(sdbCheckPeerStatus, tsMgmtPeerHBTimer * 1000, pPeer, sdbTmr, &pPeer->hbTimer);
        break;
      }
    }

    if (i >= SDB_MAX_PEERS) {
      sdbError("numOfPeers:%d larger than max number of peers:%d, ignore new one:%s", i, SDB_MAX_PEERS, pPeer->ipstr);
      return;
    }
  }

  sdbNumOfPeers++;
  sdbTrace("peer:%s is added into system, numOfPeers:%d", pPeer->ipstr, sdbNumOfPeers);
}

void sdbPeerRemoved(SSdbPeer *pPeer) {
  int i;

  if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED) return;

  for (i = 0; i < SDB_MAX_PEERS; ++i) {
    if (sdbPeer[i] == pPeer) break;
  }

  if (i >= SDB_MAX_PEERS) {
    sdbError("removed peer:%s not in the list", pPeer->ipstr);
    return;
  }

  sdbPeer[i]->status = SDB_STATUS_DELETED;
  taosTmrStopA(&(pPeer->hbTimer));
  if (pPeer->thandle) taosCloseRpcConn(pPeer->thandle);
  // if ( pPeer->syncFd > 0 ) close(pPeer->syncFd);

  sdbNumOfPeers--;
  sdbTrace("peer:%s is removed, numOfPeers:%d", pPeer->ipstr, sdbNumOfPeers);

  sdbCheckRoleStatus(NULL, NULL);
}

void *mgmtPeerTool(char action, void *row, char *str, int size, int *ssize) {
  SSdbPeer *pPeer = NULL;
  int       tsize = 0;

  switch (action) {
    case SDB_TYPE_INSERT:
      pPeer = (SSdbPeer *)row;
      sdbNewPeerAdded(pPeer);
      sdbUpdateIpList();
      break;
    case SDB_TYPE_DELETE:
      pPeer = (SSdbPeer *)row;
      sdbPeerRemoved(pPeer);
      sdbUpdateIpList();
      break;
    case SDB_TYPE_UPDATE:
      break;
    case SDB_TYPE_BATCH_UPDATE:
      break;
    case SDB_TYPE_ENCODE:
      pPeer = (SSdbPeer *)row;
      tsize = pPeer->updateEnd - (char *)pPeer;
      if (size < tsize) {
        *ssize = -1;
      } else {
        memcpy(str, pPeer, tsize);
        *ssize = tsize;
      }
      break;
    case SDB_TYPE_DECODE:
      pPeer = (SSdbPeer *)malloc(sizeof(SSdbPeer));
      if (pPeer == NULL) return NULL;
      memset(pPeer, 0, sizeof(SSdbPeer));

      tsize = pPeer->updateEnd - (char *)pPeer;

      memcpy(pPeer, str, tsize);
      return (void *)pPeer;
    case SDB_TYPE_RESET:
      pPeer = (SSdbPeer *)row;
      tsize = pPeer->updateEnd - (char *)pPeer;
      memcpy(pPeer, str, tsize);
      break;
    case SDB_TYPE_DESTROY:
      pPeer = (SSdbPeer *)row;
      if (pPeer->status != SDB_STATUS_DELETED) {
        taosTmrStopA(&pPeer->hbTimer);
        if (pPeer->thandle) taosCloseRpcConn(pPeer->thandle);
        if (pPeer->syncFd > 0) taosCloseTcpSocket(pPeer->syncFd);
      }
      pPeer->ip = 0;
      tfree(row);
      break;
  }

  return NULL;
}

/* void sdbPeerChanged(char type, char *row) */
/* { */
/*   SSdbPeer *pPeer = (SSdbPeer *)row; */
/*    */
/*   //if ( strcmp(pPeer->zone, sdbZone) != 0 ) return; */
/*  */
/*   if ( type == SDB_TYPE_DELETE ) { */
/*     sdbPeerRemoved(pPeer); */
/*   } else if ( type == SDB_TYPE_INSERT ) { */
/*     sdbNewPeerAdded(pPeer); */
/*   }  */
/*  */
/*   sdbUpdateIpList(); */
/* } */

int sdbInitPeers(char *directory) {
  SSdbPeer *pPeer;
  SRpcInit  rpcInit;
  void     *pNode = NULL;
  uint32_t  masterIp;
  int       size;

  SSdbPeer  sdbObj;
  tsMnodeUpdateSize = sdbObj.updateEnd - (char *)&sdbObj;

  selfIp = inet_addr(sdbPrivateIp);
  masterIp = inet_addr(sdbMasterIp);
  memset(sdbPeer, 0, sizeof(SSdbPeer *) * SDB_MAX_PEERS);

  size = sizeof(SIpList) + sizeof(uint32_t) * (SDB_MAX_PEERS + 2);
  if (pSdbIpList == NULL) pSdbIpList = (SIpList *)malloc(size);
  memset(pSdbIpList, 0, size);

  if (pSdbPublicIpList == NULL) pSdbPublicIpList = (SIpList *)malloc(size);
  memset(pSdbPublicIpList, 0, size);

  pthread_mutex_init(&sdbQueue.qmutex, NULL);
  pthread_mutex_init(&sdbMutex, NULL);
  sem_init(&sdbSem, 0, 0);
  sdbTmr = taosTmrInit(SDB_MAX_PEERS * 100, 200, 100000, "MND-SDB");

  sdbQhandle = taosInitScheduler(sdbMaxNodes, 2, "qsdb");
  mnodeSdb = sdbOpenTable(sdbMaxNodes, sizeof(SSdbPeer), "mnode", SDB_KEYTYPE_UINT32, directory, mgmtPeerTool);
  if (mnodeSdb == NULL) {
    sdbError("failed to init mnode data");
    return -1;
  }

  int pos = 1;  // the first slot is reserved for self

  while (1) {
    pNode = sdbFetchRow(mnodeSdb, pNode, (void **)&pPeer);
    if (pPeer == NULL) break;
    if (strcmp(pPeer->zone, sdbZone)) continue;

    pPeer->status = SDB_STATUS_OFFLINE;
    pPeer->role = SDB_ROLE_UNDECIDED;
    pPeer->syncFd = 0;
    pPeer->hbTimer = NULL;
    pPeer->thandle = NULL;
    pPeer->pSync = NULL;

    if (pPeer->ip != selfIp) {
      sdbPeer[pos] = pPeer;
      ++pos;
    } else {
      pSelf = pPeer;
    }

    sdbNumOfPeers++;
    if (sdbNumOfPeers >= SDB_MAX_PEERS) break;
  }

  int64_t oldSdbVersion = sdbVersion;
  int32_t oldTableId = ((SSdbTable*)mnodeSdb)->id;

  // add masterIP into peer
  uint32_t masterPublicIp = (masterIp == selfIp) ? sdbPublicIp : masterIp;
  sdbAddPeer(masterIp, masterPublicIp, SDB_ROLE_MASTER);

  sdbPrint("add peer:%s to mnodes, old sdbVersion:%ld new sdbVersion:%ld, old id:%d new id:%d",
          taosIpStr(masterIp), oldSdbVersion, sdbVersion, oldTableId, ((SSdbTable*)mnodeSdb)->id);

  if (pSelf == NULL || pSelf->status == SDB_STATUS_DELETED) {
    pSelf = (SSdbPeer *)malloc(sizeof(SSdbPeer));
    memset(pSelf, 0, sizeof(SSdbPeer));
    pSelf->ip = selfIp;
    pSelf->publicIp = sdbPublicIp;
    strcpy(pSelf->ipstr, sdbPrivateIp);
  }

  pSelf->status = SDB_STATUS_UNSYNCED;
  pSelf->role = SDB_ROLE_UNDECIDED;
  pSelf->numOfMnodes = 0;
  if ((sdbNumOfPeers == 1) && (masterIp == selfIp)) {
    sdbPrint("numOfPeers:%d, master:%s self:%s work as master, sdbVersion:%ld id:%d",
            sdbNumOfPeers, taosIpStr(masterIp), taosIpStr(selfIp), sdbVersion, ((SSdbTable*)mnodeSdb)->id);
    sdbWorkAsMaster();
  } else {
    /*
     * The first mnode created when the system just start, should not enter version management
     */
    sdbPrint("reset sdbVersion from %d to old %ld, id from %d to %d, for mnode changed",
            sdbVersion, oldSdbVersion, ((SSdbTable*)mnodeSdb)->id, oldTableId);
    sdbVersion = oldSdbVersion;
    ((SSdbTable*)mnodeSdb)->id = oldTableId;
  }

  sdbUpdateIpList();

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp = sdbPrivateIp;
  rpcInit.localPort = tsMgmtMgmtPort;
  rpcInit.label = "MND-SDB";
  rpcInit.numOfThreads = 1;
  rpcInit.fp = sdbProcessMsgFromPeer;
  rpcInit.bits = 8;
  rpcInit.numOfChanns = 1;
  rpcInit.sessionsPerChann = SDB_MAX_PEERS + 1;
  rpcInit.idMgmt = TAOS_ID_FREE;
  rpcInit.connType = TAOS_CONN_UDPC;
  rpcInit.qhandle = sdbQhandle;

  pPeerConn = taosOpenRpc(&rpcInit);
  if (pPeerConn == NULL) {
    sdbError("failed to init %s connection between mnodes", tsSocketType);
    return -1;
  }

  for (int i = 1; i < SDB_MAX_PEERS; ++i) {
    pPeer = sdbPeer[i];
    if (pPeer && pPeer->hbTimer == NULL)
      taosTmrReset(sdbCheckPeerStatus, tsMgmtPeerHBTimer * 1000, pPeer, sdbTmr, &pPeer->hbTimer);
  }

  if (pSelf->role != SDB_ROLE_MASTER) taosTmrReset(sdbCheckRoleStatus, tsMgmtPeerHBTimer * 3000, NULL, sdbTmr, &sdbRoleTimer);

  return 0;
}

void sdbCleanUpPeers() {
  SSdbPeer *pPeer;

  taosTmrStopA(&sdbRoleTimer);

  for (int i = 0; i < SDB_MAX_PEERS; ++i) {
    pPeer = sdbPeer[i];
    if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED) continue;
    taosTmrStopA(&pPeer->hbTimer);
    if (pPeer->thandle) taosCloseRpcConn(pPeer->thandle);
    if (pPeer->syncFd > 0) taosCloseTcpSocket(pPeer->syncFd);
    sdbPeer[i] = NULL;
  }

  if (pPeerConn) taosCloseRpc(pPeerConn);
  if (sdbTmr) taosTmrCleanUp(sdbTmr);

  pPeerConn = NULL;
  sdbTmr = NULL;
  sdbCloseTable(mnodeSdb);
  taosCleanUpScheduler(sdbQhandle);

  sem_destroy(&sdbSem);
  pthread_mutex_destroy(&sdbMutex);
  pthread_mutex_destroy(&sdbQueue.qmutex);

  sdbNumOfPeers = 0;
  sdbExtConns = 0;
  sdbCode = 0;
  sdbMaster = 0;
  sdbVersion = 0;
}

void *sdbProcessMsgFromPeer(char *msg, void *ahandle, void *thandle) {
  int       temp;
  uint32_t  peerIp, peerId;
  uint16_t  peerPort;
  SSdbPeer *pPeer = (SSdbPeer *)ahandle;
  SIntMsg  *pMsg = (SIntMsg *)msg;
  int       ret = -1;
  char      ipstr[20];

  if (msg == NULL) {
    if (pPeer && pPeer->ip && pPeer->status != SDB_STATUS_DELETED) {
      sdbTrace("ip:%s, role:%s status:%s connection is gone, self numOfMnodes:%d",
              pPeer->ipstr, taosGetSdbRoleStr(pPeer->role), taosGetSdbStatusStr(pPeer->status), pSelf->numOfMnodes);

      int outType = taosGetOutType(pPeer->thandle);
      if (outType == TSDB_MSG_TYPE_FORWARD) {
        sdbCode = TSDB_CODE_OTHERS;
        sem_post(&sdbSem);
      }

      if (pPeer->status != SDB_STATUS_OFFLINE) {
        pPeer->status = SDB_STATUS_OFFLINE;
        pPeer->lostTime = sdbGetTimeStamp();
        pSelf->numOfMnodes--;
        if (pSelf->role == SDB_ROLE_MASTER) sdbCheckSelfRole();

        if (pPeer->role == SDB_ROLE_MASTER) {
          taosTmrReset(sdbCheckRoleStatus, tsMgmtPeerHBTimer * 2500, NULL, sdbTmr, &sdbRoleTimer);
          sdbTrace("connection to master:%s is lost", pPeer->ipstr);
        }

        pPeer->role = SDB_ROLE_UNDECIDED;
      }

      pPeer->thandle = NULL;
      taosTmrReset(sdbCheckPeerStatus, tsMgmtPeerHBTimer * 10000, pPeer, sdbTmr, &pPeer->hbTimer);
    }

    return NULL;
  }

  if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED) {
    taosGetRpcConnInfo(thandle, &peerId, &peerIp, &peerPort, &temp, &temp);

    for (int i = 1; i < SDB_MAX_PEERS; ++i) {
      if (sdbPeer[i] && sdbPeer[i]->status != SDB_STATUS_DELETED && sdbPeer[i]->ip == peerIp) {
        pPeer = sdbPeer[i];
        break;
      }
    }

    if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED) {
      tinet_ntoa(ipstr, peerIp);
      sdbTrace("ip:%s, sdb peer is not configured!!!", ipstr);
      taosSendSimpleRsp(thandle, pMsg->msgType + 1, TSDB_CODE_NO_RIGHTS);
      return NULL;
    }

    if (pPeer->thandle) {
      sdbTrace("ip:%s, sdb peer connection is already there, it shall be closed", pPeer->ipstr);
      taosCloseRpcConn(pPeer->thandle);
    }

    pPeer->thandle = thandle;
    sdbTrace("ip:%s, sdb peer connection is created", pPeer->ipstr);
  }

  if (pPeer == NULL || pPeer->ip == 0) {
    sdbError("sdb peer:%p is alreay released", pPeer);
    return NULL;
  }

  taosTmrStopA(&pPeer->hbTimer);

  if (pMsg->msgType == TSDB_MSG_TYPE_SYNC) {
    ret = sdbProcessSyncRequest(pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pPeer);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_SYNC_RSP) {
    ret = 0;
  } else if (pMsg->msgType == TSDB_MSG_TYPE_HEARTBEAT) {
    ret = sdbProcessHeartBeatFromPeer(pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pPeer);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_HEARTBEAT_RSP) {
    ret = sdbProcessHeartBeatRspFromPeer(pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pPeer);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_FORWARD) {
    SSchedMsg schedMsg;
    schedMsg.msg = malloc(pMsg->msgLen);
    memcpy(schedMsg.msg, pMsg, pMsg->msgLen);
    schedMsg.fp = sdbProcessForwardRequest;
    schedMsg.tfp = NULL;
    schedMsg.ahandle = pPeer;
    schedMsg.thandle = NULL;

    taosScheduleTask(mgmtTranQhandle, &schedMsg);
    ret = 0;
  } else if (pMsg->msgType == TSDB_MSG_TYPE_FORWARD_RSP) {
    ret = sdbProcessForwardRspMsg(pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pPeer);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_CFG_MNODE) {
    ret = sdbProcessCfgMnodeMsg(pMsg->content, pMsg->msgLen - sizeof(SIntMsg), pPeer);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_CFG_MNODE_RSP) {
    ret = 0;  // do nothing right now
  } else {
    sdbError("%s from %s is not processed", taosMsg[pMsg->msgType], pPeer->ipstr);
  }

  if (pMsg->msgType == TSDB_MSG_TYPE_HEARTBEAT)
    taosTmrReset(sdbCheckPeerStatus, tsMgmtPeerHBTimer * 1500, pPeer, sdbTmr, &pPeer->hbTimer);
  else
    taosTmrReset(sdbCheckPeerStatus, tsMgmtPeerHBTimer * 1000, pPeer, sdbTmr, &pPeer->hbTimer);

  if (ret < 0) {
    taosCloseRpcConn(pPeer->thandle);
    return NULL;
  }

  return pPeer;
}

int sdbUpdatePeerStatus(SSdbPeer *pPeer, char *msg, int msgLen) {
  SMnodeStatus *pStatus = (SMnodeStatus *)msg;

  if (pPeer->status == SDB_STATUS_OFFLINE) {
    pSelf->numOfMnodes++;
    sdbTrace("ip:%s is online, role:%s status:%s numOfMnodes:%d", pPeer->ipstr, taosGetSdbRoleStr(pStatus->role),
            taosGetSdbStatusStr(pStatus->status), pSelf->numOfMnodes);
  }

  int oldRole = pPeer->role;
  pPeer->status = pStatus->status;
  pPeer->numOfDnodes = htonl(pStatus->numOfDnodes);
  pPeer->numOfMnodes = pStatus->numOfMnodes;
  pPeer->dbVersion = htobe64(pStatus->dbVersion);
  pPeer->role = pStatus->role;

  if (pPeer->publicIp != pStatus->publicIp) {
    pPeer->publicIp = pStatus->publicIp;
    sdbUpdateIpList();
  }

  if (pPeer->role == SDB_ROLE_MASTER) {
    if (oldRole != SDB_ROLE_MASTER) {
      sdbTrace("master is set to:%s", pPeer->ipstr);
      sdbUpdateIpList();
    }

    if (pSelf->role == SDB_ROLE_MASTER) {
      pError("besides myself, another master:%s", pPeer->ipstr);
      if (pSelf->ip < pPeer->ip) sdbStopWorkingAsMaster();
    } else {
      if (pPeer->dbVersion == sdbVersion) {
        pSelf->status = SDB_STATUS_SERVING;
      } else if (pPeer->dbVersion > sdbVersion) {
        if (pSelf->status != SDB_STATUS_SYNCING) {
          sdbTrace("peer:%s dbVersion:%d, sdbVersion:%ld, sync start", pPeer->ipstr, pPeer->dbVersion, sdbVersion);
          sdbStartSyncProcess(pPeer);
        }
      } else {
        sdbError("peer:%s is master, but version:%d is lower than self:%d", pPeer->ipstr, pPeer->dbVersion, sdbVersion);
        pPeer->role = SDB_ROLE_UNDECIDED;
        taosTmrReset(sdbCheckRoleStatus, 100, NULL, sdbTmr, &sdbRoleTimer);
      }
    }
  } else {
    if (oldRole == SDB_ROLE_MASTER) {
      sdbCheckRoleStatus(NULL, NULL);
    } else {
      if ((pSelf->role == SDB_ROLE_MASTER) && (pPeer->dbVersion > sdbVersion)) {
        sdbError("master, but version:%d is lower than peer:%s version:%d", sdbVersion, pPeer->ipstr, pPeer->dbVersion);
        sdbStopWorkingAsMaster();
      }
    }
  }

  return 0;
}

char *sdbEncodeSelfStatus(SSdbPeer *pPeer, char *pMsg) {
  SMnodeStatus *pStatus;

  pStatus = (SMnodeStatus *)pMsg;
  pStatus->role = pSelf->role;
  pStatus->status = pSelf->status;
  pStatus->numOfMnodes = pSelf->numOfMnodes;
  pStatus->numOfDnodes = htonl(sdbExtConns);
  pStatus->dbVersion = htobe64(sdbVersion);
  pStatus->publicIp = sdbPublicIp;
  pMsg += sizeof(SMnodeStatus);
  sdbTrace("encode self status, sdbVersion:%ld", sdbVersion);

  return pMsg;
}

int sdbProcessHeartBeatFromPeer(char *msg, int msgLen, SSdbPeer *pPeer) {
  char *pStart, *pMsg;

  if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED) return 0;

  sdbUpdatePeerStatus(pPeer, msg, msgLen);

  pStart = taosBuildRspMsg(pPeer->thandle, TSDB_MSG_TYPE_HEARTBEAT_RSP);
  if (pStart == NULL) return -1;
  pMsg = pStart;
  *pMsg = 0;  // code
  pMsg++;

  pMsg = sdbEncodeSelfStatus(pPeer, pMsg);

  msgLen = pMsg - pStart;
  taosSendMsgToPeer(pPeer->thandle, pStart, msgLen);

  return 0;
}

int sdbProcessHeartBeatRspFromPeer(char *msg, int msgLen, SSdbPeer *pPeer) {
  unsigned char code = *msg;
  if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED) return 0;

  if (code != 0) {
    sdbTrace("HB rsp code:%d !!!", code);
  } else {
    sdbUpdatePeerStatus(pPeer, msg + 1, msgLen - 1);
  }

  return 0;
}

int sdbProcessCfgMnodeMsg(char *cont, int contLen, SSdbPeer *pPeer) {
  char *   pStart, *pMsg;
  SCfgMsg *pCfg = (SCfgMsg *)cont;

  pStart = taosBuildRspMsg(pPeer->thandle, TSDB_MSG_TYPE_CFG_MNODE_RSP);
  if (pStart == NULL) return -1;
  pMsg = pStart;

  int code = tsCfgDynamicOptions(pCfg->config);
  *pMsg = code;  // code
  pMsg++;

  int msgLen = pMsg - pStart;
  taosSendMsgToPeer(pPeer->thandle, pStart, msgLen);

  return 0;
}

void sdbUpdateIpList() {
  int numOfIps = 0;
  // pSdbIpList->numOfIps = 0;
  // pSdbPublicIpList->numOfIps = 0;

  // 0 reserved for master
  for (int i = 0; i < SDB_MAX_PEERS; ++i) {
    if (sdbPeer[i] == NULL || sdbPeer[i]->ip == 0 || sdbPeer[i]->status == SDB_STATUS_DELETED) continue;

    if (sdbPeer[i]->role == SDB_ROLE_MASTER) {
      pSdbIpList->ip[numOfIps] = sdbPeer[i]->ip;
      pSdbPublicIpList->ip[numOfIps] = sdbPeer[i]->publicIp;
      sdbTrace("index:%d ip:%s publicIp:%s is master", numOfIps, taosIpStr(pSdbIpList->ip[numOfIps]), taosIpStr(pSdbPublicIpList->ip[numOfIps]));
      numOfIps++;

      break;
    }
  }

  for (int i = 0; i < SDB_MAX_PEERS; ++i) {
    if (sdbPeer[i] == NULL || sdbPeer[i]->ip == 0 || sdbPeer[i]->status == SDB_STATUS_DELETED) continue;

    pSdbIpList->ip[numOfIps] = sdbPeer[i]->ip;
    pSdbPublicIpList->ip[numOfIps] = sdbPeer[i]->publicIp;
    sdbTrace("index:%d ip:%s publicIp:%s", numOfIps, taosIpStr(pSdbIpList->ip[numOfIps]), taosIpStr(pSdbPublicIpList->ip[numOfIps]));
    numOfIps++;
  }

  pSdbIpList->numOfIps = numOfIps;
  pSdbPublicIpList->numOfIps = numOfIps;
}

void sdbCheckSelfRole() {
  if (pSelf == NULL || pSelf->status == SDB_STATUS_DELETED) return;

  if (pSelf->role == SDB_ROLE_UNDECIDED) {
    sdbMaster = 0;
    taosTmrReset(sdbCheckRoleStatus, tsMgmtPeerHBTimer * 1000, NULL, sdbTmr, &sdbRoleTimer);
    return;
  }

  if (pSelf->role == SDB_ROLE_MASTER) {
    if ((pSelf->numOfMnodes + 1.0) / sdbNumOfPeers < 0.5) {
      sdbTrace("self role:%s status:%s numOfMnodes:%d sdbNumOfPeers:%d, mnode ratio %f < 0.5, stop work as master",
               taosGetSdbRoleStr(pSelf->role), taosGetSdbStatusStr(pSelf->status),
               pSelf->numOfMnodes, sdbNumOfPeers, (pSelf->numOfMnodes + 1.0) / sdbNumOfPeers);
      sdbStopWorkingAsMaster();
    }
  }
}

void sdbCheckRoleStatus(void *param, void *tmrId) {
  int       i, pos = -1;
  SSdbPeer *pPeer;
  uint32_t  maxIp = 0;
  uint64_t  dbVersion = 0;

  if (pSelf == NULL || pSelf->status == SDB_STATUS_DELETED) {
    taosTmrReset(sdbCheckRoleStatus, tsMgmtPeerHBTimer * 1000, NULL, sdbTmr, &sdbRoleTimer);
    return;
  }

  sdbRoleTimer = NULL;
  sdbTrace("check role status, self status:%s self role:%s dbVersion:%d",
          taosGetSdbStatusStr(pSelf->status), taosGetSdbRoleStr(pSelf->role), pSelf->dbVersion);

  for (i = 0; i < SDB_MAX_PEERS; ++i) {
    pPeer = sdbPeer[i];
    if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED) continue;

    if (pPeer->role == SDB_ROLE_MASTER && pPeer->status == SDB_STATUS_SERVING) {
      sdbTrace("master:%s is there, self status:%s role:%s",
              pPeer->ipstr, taosGetSdbStatusStr(pSelf->status), taosGetSdbRoleStr(pSelf->role));
      if (pPeer != pSelf) {
        pSelf->role = SDB_ROLE_SLAVE;
        if (pSelf->status != SDB_STATUS_SYNCING) pSelf->status = SDB_STATUS_SERVING;
      }
      return;
    }
  }

  // pick up the master
  pSelf->dbVersion = sdbVersion;
  pSelf->numOfDnodes = sdbExtConns;
  pSelf->createdTime = sdbGetTimeStamp();

  for (i = 0; i < SDB_MAX_PEERS; ++i) {
    pPeer = sdbPeer[i];
    if (pPeer == NULL) continue;

    if (pPeer != NULL) {
      sdbTrace("id:%d, peer:%s, role:%s, dbVersion:%ld, status:%s, numOfMnodes:%d, numOfDnodes:%d",
               i, pPeer->ipstr, taosGetSdbRoleStr(pPeer->role), pPeer->dbVersion,
               taosGetSdbStatusStr(pPeer->status), pPeer->numOfMnodes, pPeer->numOfDnodes);
    }

    if (pPeer->status == SDB_STATUS_DELETED) {
      sdbTrace("peer:%s, is deleting, give up", pPeer->ipstr);
      continue;
    }

    if (pPeer->dbVersion < dbVersion) {
      sdbTrace("peer:%s, version:%d < current version:%d, give up", pPeer->ipstr, pPeer->dbVersion, dbVersion);
      continue;
    }

    if (pPeer->dbVersion > dbVersion) {
      sdbTrace("peer:%s, version:%d larger than current version:%d, set pos from %d to -1",
              pPeer->ipstr, pPeer->dbVersion, dbVersion, pos);
      dbVersion = pPeer->dbVersion;
      pos = -1;
      maxIp = 0;
    }

    if (pPeer->numOfDnodes <= 0) {
      sdbTrace("peer:%s, numOfDnodes:%d smaller than 0, give up", pPeer->ipstr, pPeer->numOfDnodes);
      continue;
    }

    if (pPeer->status == SDB_STATUS_SYNCING) {
      sdbTrace("peer:%s, status is syncing, give up", pPeer->ipstr);
      continue;
    }

    if (pPeer->status == SDB_STATUS_OFFLINE) {
      sdbTrace("peer:%s, status is offline, give up", pPeer->ipstr);
      continue;
    }
    if ((pPeer->numOfMnodes + 1.0) / sdbNumOfPeers < 0.5) {
      sdbTrace("peer:%s, mnode ratio %f smaller than 0.5, give up, numOfMnodes:%d sdbNumOfPeers:%d",
              pPeer->ipstr, (pPeer->numOfMnodes + 1.0) / sdbNumOfPeers, pPeer->numOfMnodes, sdbNumOfPeers);
      continue;
    }

    if ((pPeer->dbVersion == dbVersion) && (pPeer->ip > maxIp)) {
      sdbTrace("peer:%s, version:%d equal with current version:%d, maxIp:%d, set pos from %d to %d",
               pPeer->ipstr, pPeer->dbVersion, dbVersion, maxIp, pos, i);
      maxIp = pPeer->ip;
      pos = i;
    } else {
      sdbTrace("peer:%s, version:%d current version:%d, maxIp:%d pos:%d give up",
               pPeer->ipstr, pPeer->dbVersion, dbVersion, maxIp, pos);
    }
  }

  if (pos >= 0 && pos < SDB_MAX_PEERS) {
    sdbTrace("%s shall work as master", sdbPeer[pos]->ipstr);
  } else {
    sdbTrace("no master is elected, pos:%d", pos);
  }

  if (pos == 0) {
    if ((pSelf->status == SDB_STATUS_SERVING) || (pSelf->numOfMnodes >= sdbNumOfPeers - 1)) {
      sdbWorkAsMaster();
    } else {
      sdbTrace("%s self status:%s numOfMnodes:%d sdbNumOfPeers:%d, can not work as master",
              sdbPeer[pos]->ipstr, taosGetSdbStatusStr(pSelf->status), pSelf->numOfMnodes, sdbNumOfPeers);
    }
  }

  taosTmrReset(sdbCheckRoleStatus, tsMgmtPeerHBTimer * 1000, NULL, sdbTmr, &sdbRoleTimer);
}

void sdbCheckPeerStatus(void *param, void *tmrId) {
  SSdbPeer *   pPeer = (SSdbPeer *)param;
  char         meterId[TSDB_METER_ID_LEN];
  char *       pStart, *pMsg;
  int          msgLen;
  SRpcConnInit connInit;
  uint8_t      code;

  if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED) return;
  if (pPeer->ip == 0) return;
  if (pSelf == NULL || pSelf->status == SDB_STATUS_DELETED) return;

  if (pPeer->thandle == NULL) {
    memset(meterId, 0, sizeof(meterId));
    strcpy(meterId, sdbPrivateIp);
    memset(&connInit, 0, sizeof(connInit));
    connInit.cid = 0;
    connInit.sid = 0;
    connInit.spi = 0;
    connInit.encrypt = 0;
    connInit.meterId = meterId;
    connInit.peerId = 0;
    connInit.shandle = pPeerConn;
    connInit.ahandle = pPeer;
    connInit.peerIp = pPeer->ipstr;
    connInit.peerPort = tsMgmtMgmtPort;
    pPeer->thandle = taosOpenRpcConn(&connInit, &code);

    if (pPeer->thandle == NULL) {
      taosTmrReset(sdbCheckPeerStatus, tsMgmtPeerHBTimer * 30000, pPeer, sdbTmr, &pPeer->hbTimer);
      return;
    }
  }

  pStart = taosBuildReqMsg(pPeer->thandle, TSDB_MSG_TYPE_HEARTBEAT);
  if (pStart == NULL) return;
  pMsg = sdbEncodeSelfStatus(pPeer, pStart);
  msgLen = pMsg - pStart;

  sdbTrace("send heartbeat msg to peer:%s", pPeer->ipstr);
  taosSendMsgToPeer(pPeer->thandle, pStart, msgLen);

  return;
}

int sdbTransferWholeDataToPeer(int syncFd, SSdbTable *pTable) {
  struct stat fstat;
  uint64_t    size;
  int         sfd;
  SForwardMsg forward;

  stat(pTable->fn, &fstat);
  size = fstat.st_size;

  fdatasync(pTable->fd);
  sfd = open(pTable->fn, O_RDONLY);
  if (sfd < 0) {
    sdbError("failed to open file:%s", pTable->fn);
    return -1;
  }

  forward.dbId = -pTable->dbId;
  forward.type = 0;
  forward.version = htobe64(size);
  forward.dataLen = 0;
  write(syncFd, &forward, sizeof(forward));

  if (tsendfile(syncFd, sfd, NULL, size) < 0) {
    sdbError("failed to transfer file:%s to peer", pTable->fn);
    tclose(sfd);
    return -1;
  }

  sdbTrace("file:%s is sent to peer", pTable->fn);
  tclose(sfd);

  return 0;
}

int sdbRetrieveRows(int syncFd, SSdbTable *pTable, uint64_t version) {
  int         versionDelta;
  int         firstUpdate;
  int         updatePos;
  int64_t     firstVersion, curVersion;
  SForwardMsg forward;
  int         rowSize = 0;

  char *msg = (char *)malloc(pTable->maxRowSize);
  if (msg == NULL) return -1;
  memset(msg, 0, pTable->maxRowSize);

  if (pTable->numOfUpdates > 0) {
    pTable->numOfUpdates = pTable->numOfUpdates % pTable->maxRows;
    firstUpdate = (pTable->updatePos - pTable->numOfUpdates + pTable->maxRows + 1) % pTable->maxRows;
    firstVersion = pTable->id - pTable->numOfUpdates + 1;
    versionDelta = version - firstVersion;

    if (versionDelta >= -1) {
      // retrieve from memory
      updatePos = (firstUpdate + versionDelta + 1) % pTable->maxRows;
      curVersion = version + 1;

      int writeLen = 0;
      while (curVersion <= pTable->id) {
        SSdbUpdate *pUpdate = pTable->update + updatePos;
        (*(pTable->appTool))(SDB_TYPE_ENCODE, pUpdate->row, msg, pTable->maxRowSize, &rowSize);

        forward.dbId = pTable->dbId;
        forward.type = pUpdate->type;
        forward.version = htobe64(curVersion - 1);
        forward.dataLen = htons(rowSize);

        writeLen = write(syncFd, &forward, sizeof(forward));
        if (writeLen != sizeof(forward)) {
          sdbError("table:%s, failed to send forward msg:%d, writeLen:%d reason:%s fd:%d",
                  pTable->name, sizeof(forward), writeLen, strerror(errno), syncFd);
          writeLen = -1;
          break;
        }

        writeLen = write(syncFd, msg, rowSize);;
        if (writeLen != rowSize) {
          sdbError("table:%s, failed to send %d rows data, writeLen:%d reason:%s fd:%d",
                   pTable->name, curVersion - version - 1, writeLen, strerror(errno), syncFd);
          writeLen = -1;
          break;
        }

        updatePos = (updatePos + 1) % pTable->maxRows;
        curVersion++;
      }

      if (writeLen != -1) {
        sdbTrace("table:%s, %d rows data are sent, rowSize:%d", pTable->name, curVersion - version - 1, rowSize);
      }

      tfree(msg);
      return 0;
    }
  }

  tfree(msg);

  return sdbTransferWholeDataToPeer(syncFd, pTable);
}

void *sdbRetrieveSyncData(void *argv) {
  SSdbPeer *pPeer = (SSdbPeer *)argv;
  SSdbSync *pSync = (SSdbSync *)pPeer->pSync;

  sdbPrint("ip:%s:%d, start to send sdb retrieve data, fd:%d", pPeer->ipstr, tsMgmtSyncPort, pPeer->syncFd);

  // based on dbVersion, forward the data to peer
  for (int i = 0; i < pSync->numOfTables; ++i) {
    if (tableList[i]->id > pSync->version[i]) {
      sdbTrace("table:%s, peer version:%d, self version:%d, send data", tableList[i]->name, pSync->version[i], tableList[i]->id);
      sdbRetrieveRows(pPeer->syncFd, tableList[i], pSync->version[i]);
    } else {
      sdbTrace("table:%s, peer version:%d, self version:%d, not need to send data", tableList[i]->name, pSync->version[i], tableList[i]->id);
    }
  }

  close(pPeer->syncFd);
  pPeer->syncFd = 0;

  sdbPrint("ip:%s:%d, send sdb retrieve data finished", pPeer->ipstr, tsMgmtSyncPort);

  return NULL;
}

int sdbProcessSyncRequest(char *msg, int msgLen, SSdbPeer *pPeer) {
  int            code = 0;
  pthread_attr_t thattr;
  pthread_t      thread;
  SSdbSync *     pSync;

  /*
   * Multiple messages may trigger synchronization at the same time
   * Use syncFd > 0 as a condition to determine whether synchronization is in progress
   */
  if (pPeer->syncFd > 0) {
    sdbError("%s, a sync thread is already started, sfd:%d", pPeer->ipstr, pPeer->syncFd);
    return TSDB_CODE_APP_ERROR;
  }

  pSync = (SSdbSync *)msg;
  for (int i = 0; i < pSync->numOfTables; ++i) pSync->version[i] = htobe64(pSync->version[i]);

  tfree(pPeer->pSync);
  pPeer->pSync = malloc(msgLen);
  memcpy(pPeer->pSync, pSync, msgLen);

  // set up tcp socket
  pPeer->syncFd = taosOpenTcpClientSocket(pPeer->ipstr, tsMgmtSyncPort, NULL);
  if (pPeer->syncFd <= 0) {
    sdbError("ip:%s, faile to open sync TCP socket:%d", pPeer->ipstr, tsMgmtSyncPort);
    code = TSDB_CODE_APP_ERROR;
    goto _sync_req_over;
  }

  sdbTrace("ip:%s:%d, sync tcp socket is setup, fd:%d", pPeer->ipstr, tsMgmtSyncPort, pPeer->syncFd);

  // start a new thread to transfer the cache
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  if (pthread_create(&thread, &thattr, sdbRetrieveSyncData, pPeer) != 0) {
    tclose(pPeer->syncFd);
    pPeer->syncFd = 0;
    sdbError("%s, failed to create sync thread, reason:%s", pPeer->ipstr, strerror(errno));
    code = TSDB_CODE_APP_ERROR;
  }

_sync_req_over:
  taosSendSimpleRsp(pPeer->thandle, TSDB_MSG_TYPE_SYNC_RSP, code);

  return code;
}

int sdbStartFullSync(int tcpFd, SForwardMsg *pForward) {
  SSdbTable *pTable;
  int        dfd;
  int64_t    size;

  pTable = tableList[-pForward->dbId];
  size = htobe64(pForward->version);
  tclose(pTable->fd);

  dfd = open(pTable->fn, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  if (dfd < 0) {
    sdbError("failed to open file:%s", pTable->fn);
    return -1;
  }

  /*
    int ret;
    int leftSize = size;
    while ( leftSize > 0 ) {
      ret = sendfile(dfd, tcpFd, NULL, leftSize);
      if ( ret < 0 ) break;
      leftSize -= ret;
    }
  */
  int ret = taosCopyFds(tcpFd, dfd, size);
  close(dfd);

  if (ret < 0) {
    remove(pTable->fn);
    sdbError("failed to receive table file:%s", pTable->fn);
  } else {
    sdbPrint("%s is received from master, reset", pTable->fn);
    // kill(0, SIGTERM);
    sdbResetTable(pTable);
  }

  return ret;
}

void sdbRestoreDbReq(int tcpFd) {
  // if TSDB_MAX_VNODES set to 128, the size may be 3696
  /* char         cont[10240]; */
  char *       cont = malloc(65536);
  int          ret = 0, dataLen;
  SForwardMsg *pForward = (SForwardMsg *)cont;

  while (1) {
    ret = taosReadMsg(tcpFd, pForward, sizeof(SForwardMsg));
    if (ret <= 0) {
      sdbTrace("failed to read forward msg size from fd:%d, ret:%d reason:%s, restore finished", tcpFd, ret, strerror(errno));
      break;
    }

    dataLen = htons(pForward->dataLen);
    sdbTrace("forward msg size received, table:%s type:%s version:%ld dataLen:%d ret:%d",
            taosGetSdbTableName(pForward->dbId), taosGetSdbOperName(pForward->type), htobe64(pForward->version), dataLen, ret);

    if (dataLen > 0) {
      ret = taosReadMsg(tcpFd, pForward->data, dataLen);
      if (ret <= 0) {
        sdbError("failed to read forward msg, dataLen:%d ret:%d reason:%s", dataLen, ret, strerror(errno));
        break;
      } else {
        //sdbTrace("forward msg received from fd:%d, dataLen:%d ret:%d", tcpFd, dataLen, ret);
      }
    } else {
      sdbError("invalid forward msg dataLen:%d, ret:%d reason:%s", dataLen, ret, strerror(errno));
      break;
    }

    if (pForward->dbId < 0) {
      ret = sdbStartFullSync(tcpFd, pForward);
      if (ret < 0) {
        sdbError("failed to full sync, ret:%d", ret);
        break;
      } else {
        sdbPrint("full sync finished, ret:%d", ret);
      }
    } else {
      if (dataLen > 0) {
        ret = sdbProcessQueuedDbReq(cont, dataLen + sizeof(SForwardMsg));
        if (ret < 0) {
          sdbError("failed to process queue db req, ret:%d", ret);
          break;
        } else {
          sdbTrace("forward msg processed, ret:%d sdbVersion:%ld", ret, sdbVersion);
        }
      } else {
        sdbError("invalid forward msg dataLen:%d", dataLen);
      }
    }
  }

  tfree(cont);

  if (ret < 0) {
    sdbError("sync failed, sdbVersion:%ld reason:%s ", sdbVersion, strerror(errno));
    pSelf->status = SDB_STATUS_UNSYNCED;
  } else {
    sdbProcessBufferedForwards();
    // pSelf->status = SDB_STATUS_SERVING;
    sdbTrace("sync is finished, sdbVersion:%ld", sdbVersion);
  }
}

void *sdbAcceptSyncTcpConnection(void *argv) {
  SSdbPeer *         pPeer = (SSdbPeer *)argv;
  int                tcpFd;
  int64_t            connFd = -1;
  struct sockaddr_in clientAddr;

  tcpFd = taosOpenTcpServerSocket(sdbPrivateIp, tsMgmtSyncPort);
  if (tcpFd <= 0) {
    sdbError("failed to create sync TCP socket, reason:%s", strerror(errno));
    pPeer->status = SDB_STATUS_UNSYNCED;
    goto _sync_over;
  }

  sdbTrace("sync TCP server is created, ip:%s port:%hu", sdbPrivateIp, tsMgmtSyncPort);

  char *    pStart, *pMsg;
  SSdbSync *pSync;
  pStart = taosBuildReqMsg(pPeer->thandle, TSDB_MSG_TYPE_SYNC);
  if (pStart == NULL) goto _sync_over;

  pMsg = pStart;
  pSync = (SSdbSync *)pMsg;
  pSync->numOfTables = sdbNumOfTables;

  for (int i = 0; i < sdbNumOfTables; ++i) pSync->version[i] = htobe64(tableList[i]->id);
  pMsg += sizeof(SSdbSync) + sdbNumOfTables * sizeof(uint64_t);

  if (taosSendMsgToPeer(pPeer->thandle, pStart, pMsg - pStart) < 0) {
    sdbError("failed to send sync request to:%s", pPeer->ipstr);
    goto _sync_over;
  }

  socklen_t addrlen = sizeof(clientAddr);
  connFd = accept(tcpFd, (struct sockaddr *)&clientAddr, &addrlen);

  if (connFd < 0) {
    sdbError("sync TCP accept failure, reason:%s", strerror(errno));
    pPeer->status = SDB_STATUS_UNSYNCED;
    goto _sync_over;
  }

  sdbTrace("sync TCP connection from ip:%s port:%hu fd:%d", inet_ntoa(clientAddr.sin_addr), htons(clientAddr.sin_port), connFd);

  sdbRestoreDbReq(connFd);

_sync_over:
  if (tcpFd > 0) taosCloseTcpSocket(tcpFd);
  if (connFd > 0) taosCloseTcpSocket(connFd);

  return NULL;
}

void sdbStartSyncProcess(SSdbPeer *pPeer) {
  pthread_attr_t thattr;
  pthread_t      thread;

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  if (pthread_create(&(thread), &thattr, (void *)sdbAcceptSyncTcpConnection, pPeer) != 0) {
    sdbError("failed to create sync TCP accept thread, reason:%s", strerror(errno));
    return;
  }
  pthread_attr_destroy(&thattr);
}

int sdbProcessDbReq(char *cont, int contLen) {
  SForwardMsg *pForward;
  SSdbTable *  pTable;

  pForward = (SForwardMsg *)cont;
  pForward->version = htobe64(pForward->version);
  int dataLen = contLen - sizeof(SForwardMsg);

  pTable = (SSdbTable *)tableList[pForward->dbId];
  if (pTable->id < pForward->version) {
    sdbTrace("version, peer:%d, self:%d, sync shall start!", pForward->version, pTable->id);
    return -1;
  }

  sdbTrace("table:%s type:%d, db req is received from peer", pTable->name, pForward->type);

  if (pForward->type == SDB_TYPE_INSERT) {
    /* char *row = malloc(dataLen); */
    /* memcpy(row, pForward->data, dataLen); */
    if (sdbInsertRow(pTable, pForward->data, dataLen) < 0) {
      sdbError("failed to process db insert req, table:%s rows:%d id:%d", pTable->name, pTable->numOfRows, pTable->id);
    }
  } else if (pForward->type == SDB_TYPE_DELETE) {
    if (sdbDeleteRow(pTable, pForward->data) < 0) {
      sdbError("failed to process db delete req, table:%s rows:%d id:%d", pTable->name, pTable->numOfRows, pTable->id);
    }
  } else if (pForward->type == SDB_TYPE_UPDATE) {
    if (sdbUpdateRow(pTable, pForward->data, dataLen, 0) < 0) {
      sdbError("failed to process db update req, table:%s rows:%d id:%d", pTable->name, pTable->numOfRows, pTable->id);
    }
  } else if (pForward->type == SDB_TYPE_BATCH_UPDATE) {
    if (sdbBatchUpdateRow(pTable, pForward->data, dataLen) < 0) {
      sdbError("failed to process db batch update req, table:%s rows:%d id:%d", pTable->name, pTable->numOfRows, pTable->id);
    }
  } else {
    sdbError("sdb type:%d not processed", pForward->type);
  }

  return 0;
}

int sdbProcessQueuedDbReq(char *cont, int contLen) {
  SForwardMsg *pForward;
  SSdbTable *  pTable;

  pForward = (SForwardMsg *)cont;
  pForward->version = htobe64(pForward->version);
  int dataLen = contLen - sizeof(SForwardMsg);

  pTable = (SSdbTable *)tableList[pForward->dbId];
  if (pTable->id > pForward->version) return 0;

  if (pForward->type == SDB_TYPE_INSERT) {
    /* char *row = malloc(dataLen); */
    /* memcpy(row, pForward->data, dataLen); */
    if (sdbInsertRow(pTable, pForward->data, dataLen) < 0) {
      sdbError("failed to process queued db insert req, table:%s rows:%d id:%d", pTable->name, pTable->numOfRows, pTable->id);
    }
  } else if (pForward->type == SDB_TYPE_DELETE) {
    if (sdbDeleteRow(pTable, pForward->data) < 0) {
      sdbError("failed to process queued db delete req, table:%s rows:%d id:%d", pTable->name, pTable->numOfRows, pTable->id);
    }
  } else if (pForward->type == SDB_TYPE_UPDATE) {
    if (sdbUpdateRow(pTable, pForward->data, dataLen, 0) < 0) {
      sdbError("failed to process queued db update req, table:%s rows:%d id:%d", pTable->name, pTable->numOfRows, pTable->id);
    }
  } else if (pForward->type == SDB_TYPE_BATCH_UPDATE) {
    if (sdbBatchUpdateRow(pTable, pForward->data, dataLen) < 0) {
      sdbError("failed to process queued db batch update req, table:%s rows:%d id:%d", pTable->name, pTable->numOfRows, pTable->id);
    }
  } else {
    sdbError("sdb type:%d not processed", pForward->type);
  }

  return 0;
}

int sdbProcessBufferedForwards() {
  int         submits = 0;
  short       msgLen;
  char *      offset;
  STranQueue *pQueue = &sdbQueue;

  offset = pQueue->buffer;

  while (submits < pQueue->trans) {
    msgLen = *((uint16_t *)offset);
    offset += sizeof(msgLen);
    sdbProcessQueuedDbReq(offset, msgLen);
    offset += msgLen;
    submits++;
  }

  pthread_mutex_lock(&pQueue->qmutex);

  if (offset == NULL) offset = pQueue->buffer;
  while (submits < pQueue->trans) {
    msgLen = *((uint16_t *)offset);
    offset += sizeof(msgLen);
    sdbProcessQueuedDbReq(offset, msgLen);
    offset += msgLen;
    submits++;
  }

  if (pSelf) pSelf->status = SDB_STATUS_SERVING;
  tfree(pQueue->buffer);
  pQueue->trans = 0;

  pthread_mutex_unlock(&pQueue->qmutex);

  return 0;
}

int sdbForwardDbReqToPeer(SSdbTable *pTable, char type, char *data, int dataLen) {
  char *       pStart, *pMsg;
  int          msgLen;
  SSdbPeer *   pPeer;
  SForwardMsg *pForward;
  int          numOfSuccess = 0;

  if (sdbNumOfPeers <= 1) return 0;
  if (pSelf == NULL) return 0;
  if (pSelf->role != SDB_ROLE_MASTER) return 0;

  pthread_mutex_lock(&sdbMutex);

  for (int i = 1; i < SDB_MAX_PEERS; ++i) {
    pPeer = sdbPeer[i];
    if (pPeer && pPeer->status != SDB_STATUS_OFFLINE && pPeer->status != SDB_STATUS_DELETED) {
      pStart = taosBuildReqMsgWithSize(pPeer->thandle, TSDB_MSG_TYPE_FORWARD,
                                       dataLen + sizeof(SForwardMsg) + sizeof(STaosHeader) + 64);
      if (pStart == NULL) continue;
      pMsg = pStart;
      pForward = (SForwardMsg *)pMsg;
      pForward->dbId = pTable->dbId;
      pForward->type = type;
      pForward->dataLen = htons(dataLen);
      pForward->version = htobe64(pTable->id);
      memcpy(pForward->data, data, dataLen);

      pMsg += sizeof(SForwardMsg) + dataLen;
      msgLen = pMsg - pStart;

      taosTmrStopA(&pPeer->hbTimer);
      pPeer->hbTimer = NULL;

      sdbTrace("table:%s type:%d db req is forwarding to:%s", pTable->name, type, pPeer->ipstr);
      if (taosSendMsgToPeer(pPeer->thandle, pStart, msgLen) < 0) continue;

      int trywaitTimes = 0;
      while (true) {
        int ret = sem_trywait(&sdbSem);
        if (ret != 0) {
          if (trywaitTimes++ > MAX_TRY_WAIT_TIMES) {
            sdbError("table:%s type:%d db req forward failed, trywaitTimes:%d", pTable->name, type, trywaitTimes);
            sdbCode = ret;
            break;
          } else {
            taosMsleep(TRY_WAIT_TIME_IN_MS);
            continue;
          }
        } else {
          sdbTrace("table:%s type:%d db req forward success, sdbCode:%d", pTable->name, type, sdbCode);
          break;
        }
      }

      //sem_wait(&sdbSem);
      if (sdbCode == 0) numOfSuccess++;
      sdbCode = 0;
    }
  }

  pthread_mutex_unlock(&sdbMutex);

  if ((numOfSuccess + 1.0) / sdbNumOfPeers >= 0.5) return 0;

  sdbError("table:%s type:%d, failed to forward, numOfSuccess:%d numOfPeers:%d", pTable->name, type, numOfSuccess,
           sdbNumOfPeers);
  return -1;
}

int sdbProcessForwardRspMsg(char *cont, int contLen, SSdbPeer *pPeer) {
  sdbCode = *cont;
  sem_post(&sdbSem);

  return 0;
}

int sdbProcessForwardMsg(char *cont, int contLen, SSdbPeer *pPeer) {
  STranQueue *pQueue = &sdbQueue;

  if (pSelf == NULL || pSelf->status == SDB_STATUS_DELETED) return 0;

  if (pSelf->status == SDB_STATUS_SERVING) {
    if (sdbProcessDbReq(cont, contLen) < 0) sdbStartSyncProcess(pPeer);

  } else if (pSelf->status == SDB_STATUS_SYNCING) {
    pthread_mutex_lock(&pQueue->qmutex);

    if (pQueue->buffer == NULL) {
      pQueue->buffer = malloc(SDB_BUFFER_SIZE);
      pQueue->offset = pQueue->buffer;
      pQueue->bufferSize = SDB_BUFFER_SIZE;
      pQueue->trans = 0;
    }

    if (pQueue->bufferSize - (pQueue->offset - pQueue->buffer) < contLen + 100) {
      pQueue->bufferSize += SDB_BUFFER_SIZE;
      pQueue->buffer = realloc(pQueue->buffer, pQueue->bufferSize);
    }

    *((uint16_t *)pQueue->offset) = contLen;
    pQueue->offset += 2;
    memcpy(pQueue->offset, cont, contLen);
    pQueue->offset += contLen;
    pQueue->trans++;

    pthread_mutex_unlock(&pQueue->qmutex);
    sdbTrace("data from %s are saved into sync queue", pPeer->ipstr);

  } else {
    sdbTrace("data from %s are thrown away, self status:%d", pPeer->ipstr, pSelf->status);
  }

  taosSendSimpleRsp(pPeer->thandle, TSDB_MSG_TYPE_FORWARD_RSP, 0);

  return 0;
}

int sdbCfgNode(char *cont) {
  SCfgMsg * pCfg = (SCfgMsg *)cont;
  int       code = TSDB_CODE_NODE_OFFLINE;
  SSdbPeer *pPeer;
  char *    pMsg, *pStart;
  uint32_t  ip;

  ip = inet_addr(pCfg->ip);
  pPeer = (SSdbPeer *)sdbGetRow(mnodeSdb, &ip);
  if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED) return TSDB_CODE_NOT_CONFIGURED;

  if (pPeer == pSelf) {
    code = tsCfgDynamicOptions(pCfg->config);
  } else {
    pStart = taosBuildReqMsg(pPeer->thandle, TSDB_MSG_TYPE_CFG_MNODE);
    if (pStart) {
      pMsg = pStart;
      memcpy(pMsg, cont, sizeof(SCfgMsg));
      pMsg += sizeof(SCfgMsg);

      int msgLen = pMsg - pStart;
      code = taosSendMsgToPeer(pPeer->thandle, pStart, msgLen);
    }
  }

  return code;
}
