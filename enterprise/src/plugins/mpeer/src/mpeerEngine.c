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
#include "os.h"
#include "tlog.h"
#include "trpc.h"
#include "tsched.h"
#include "tsocket.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"
#include "sdb.h"
#include "sdbint.h"
#include "mpeerStr.h"
#include "mpeerEngine.h"

extern SSdbPeer *sdbPeer[SDB_MAX_PEERS];  // first slot for self

static void *tsMpeerSched     = NULL;
static void *tsMpeerServer    = NULL;
static void *tsMpeerClient    = NULL;
static void *tsMpeerTmr       = NULL;
static void *tsMpeerRoleTimer = NULL;
static void *tsMnodeSdb       = NULL;

static uint32_t tsMpeerSelfIp          = 0;
static uint32_t tsMpeerPublicIp        = 0;
static int32_t  tsMpeerCode            = 0;
static int32_t  tsMpeerNumOfPeers      = 0;
static int32_t  tsMnodeUpdateSize      = 0;
static uint32_t tsMpeerMasterStartTime = 0;

static sem_t           tsSdbSem;
static STranQueue      tsMpeerQueue;
static pthread_mutex_t tsMpeerMutex;
static SRpcIpSet       tsMpeerIpSet;

void (*sdbWorkAsMasterCallback)() = NULL;

#define pSelf (sdbPeer[0])
#define sdbStatus (sdbPeer[0]->status)
#define SDB_BUFFER_SIZE 1024000

static void mpeerProcessMsgFromPeer(char type, void *pCont, int contLen, void *handle, int32_t code);
static void mpeerProcessRspFromPeer(char type, void *pCont, int contLen, void *handle, int32_t code);
static int32_t mpeerRetriveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey);

static void mpeerProcessHeartBeatMsg(void *msg, int32_t msgLen, SSdbPeer *pPeer, void *thandle);
static void mpeerProcessSyncMsg(void *pMsg, int32_t msgLen, SSdbPeer *pPeer, void *thandle);
static void mpeerProcessForwardMsg(void *cont, int32_t contLen, SSdbPeer *pPeer, void *thandle);
static void mpeerProcessHeartBeatRsp(void *msg, int32_t msgLen, int32_t code, SSdbPeer *pPeer);
static void mpeerProcessForwardRsp(void *cont, int32_t contLen, int32_t code, SSdbPeer *pPeer);

static void    mpeerCheckPeerStatus(void *param, void *tmrId);
static void    mpeerCheckRoleStatus(void *param, void *tmrId);
static void    mpeerCheckSelfRole();
static void *  mpeerRetrieveSyncData(void *param);
static void    mpeerStartSyncProcess(SSdbPeer *pPeer);
static void *  mpeerAcceptSyncTcpConnection(void *argv);
static int32_t mpeerProcessBufferedForwards();
static int32_t mpeerProcessDbReq(char *cont, int32_t contLen);
static int32_t mpeerProcessQueuedDbReq(char *cont, int32_t contLen);
static void    mpeerUpdateIpList();

static void mpeerProcessForwardReqQueue(SSchedMsg *pSchedMsg) {
  SSchedFordwardMsg *pMsg  = pSchedMsg->msg;
  SSdbPeer          *pPeer = (SSdbPeer *) pSchedMsg->ahandle;

  mpeerProcessForwardMsg(pMsg->content, pMsg->msgLen, pPeer, 0);
  if (pSchedMsg->msg) free(pSchedMsg->msg);
}

static int64_t mpeerGetTimeStamp() {
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  return (int64_t)systemTime.tv_sec * 1000 + systemTime.tv_usec / 1000;
}

static void mpeerWorkAsMaster() {
  sdbLPrint("dnode:%s start to work as master", tsPrivateIp);

  pSelf->role = SDB_ROLE_MASTER;
  pSelf->status = SDB_STATUS_SERVING;
  sdbMaster = 1;
  tsMpeerMasterStartTime = taosGetTimestampSec();

  mpeerUpdateIpList();
  (*sdbWorkAsMasterCallback)();
}

void sdbStopWorkingAsMaster() {
  sdbLPrint("dnode:%s stop working as Master", tsPrivateIp);

  pSelf->role = SDB_ROLE_UNDECIDED;
  taosTmrReset(mpeerCheckRoleStatus, tsMgmtPeerHBTimer * 1000, NULL, tsMpeerTmr, &tsMpeerRoleTimer);
  sdbMaster = 0;

  mpeerUpdateIpList();
}

SSdbPeer *mpeerAddMnodeWithRole(uint32_t ip, uint32_t publicIp, char role) {
  SSdbPeer *pPeer;

  pPeer = (SSdbPeer *)malloc(sizeof(SSdbPeer));
  memset(pPeer, 0, sizeof(SSdbPeer));
  pPeer->ip = ip;
  pPeer->publicIp = publicIp;
  tinet_ntoa(pPeer->ipstr, pPeer->ip);
  strcpy(pPeer->zone, MPEER_DEFAULT_ZONE);
  pPeer->createdTime = mpeerGetTimeStamp();
  pPeer->role = role;

  if (sdbInsertRow(tsMnodeSdb, pPeer, 0) > 0) {
    sdbPrint("sdb peer:%s is added", pPeer->ipstr);
  } else {
    //sdbError("failed to add sdb peer:%s", pPeer->ipstr);
    tfree(pPeer);
  }

  return pPeer;
}

int32_t mpeerAddMnode(uint32_t ip, uint32_t publicIp) {
  if (mpeerAddMnodeWithRole(ip, publicIp, 0) != NULL) {
    return 0;
  }
  return -1;
}

int32_t mpeerRemoveMnodeImp(SSdbPeer *pPeer) {
  if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED || pPeer->ip == 0) return 0;

  if (pPeer->ip == tsMpeerSelfIp) {
    sdbWarn("could not remove self IP");
    return 0;
  } else {
    sdbPrint("sdb peer:%s will be removed", pPeer->ipstr);
  }

  sdbDeleteRow(tsMnodeSdb, &(pPeer->ip));

  return 0;
}

int32_t mpeerRemoveMnode(uint32_t ip) {
  SSdbPeer *pPeer;

  pPeer = sdbGetRow(tsMnodeSdb, &ip);
  if (pPeer == NULL) {
    sdbError("sdb peer:%s not exist, can not remove", taosIpStr(ip));
    return TSDB_CODE_INVALID_VALUE;
  }

  mpeerRemoveMnodeImp(pPeer);

  return 0;
}

void mpeerNewPeerAdded(SSdbPeer *pPeer) {
  int32_t i;

  pPeer->hbTimer = NULL;
  pPeer->syncFd = -1;
  pPeer->pSync = NULL;
  pPeer->status = SDB_STATUS_OFFLINE;

  if (pPeer->ip == tsMpeerSelfIp) {
    if (pSelf && pSelf->status != SDB_STATUS_DELETED) memcpy(pPeer, pSelf, sizeof(SSdbPeer));
    pSelf = pPeer;
  } else {
    for (i = 1; i < SDB_MAX_PEERS; ++i) {
      if (sdbPeer[i] == NULL || sdbPeer[i]->status == SDB_STATUS_DELETED) {
        sdbPeer[i] = pPeer;
        taosTmrReset(mpeerCheckPeerStatus, tsMgmtPeerHBTimer * 1000, pPeer, tsMpeerTmr, &pPeer->hbTimer);
        break;
      }
    }

    if (i >= SDB_MAX_PEERS) {
      sdbError("numOfPeers:%d larger than max number of peers:%d, ignore new one:%s", i, SDB_MAX_PEERS, pPeer->ipstr);
      return;
    }
  }

  tsMpeerNumOfPeers++;
  sdbPrint("peer:%s is added into system, numOfPeers:%d sdbVersion:%d mnodeId:%d",
      pPeer->ipstr, tsMpeerNumOfPeers, sdbVersion, ((SSdbTable*)tsMnodeSdb)->id);
}

void mpeerPeerRemoved(SSdbPeer *pPeer) {
  int32_t i;

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

  tsMpeerNumOfPeers--;
  sdbPrint("peer:%s is removed, numOfPeers:%d, sdbVersion:%d mnodeId:%d",
      pPeer->ipstr, tsMpeerNumOfPeers, sdbVersion, ((SSdbTable*)tsMnodeSdb)->id);

  mpeerCheckRoleStatus(NULL, NULL);
}

void *mgmtPeerTool(char action, void *row, char *str, int32_t size, int32_t *ssize) {
  SSdbPeer *pPeer = NULL;
  int32_t   tsize = 0;

  switch (action) {
    case SDB_TYPE_INSERT:
      pPeer = (SSdbPeer *)row;
      mpeerNewPeerAdded(pPeer);
      mpeerUpdateIpList();
      break;
    case SDB_TYPE_DELETE:
      pPeer = (SSdbPeer *)row;
      mpeerPeerRemoved(pPeer);
      mpeerUpdateIpList();
      break;
    case SDB_TYPE_UPDATE:
      pPeer = (SSdbPeer *)row;
      if (sdbGetRow(tsMnodeSdb, &pPeer->ip) == NULL) {
        mpeerNewPeerAdded(pPeer);
        mpeerUpdateIpList();
      }
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
        mpeerPeerRemoved(pPeer);
        mpeerUpdateIpList();
        taosTmrStopA(&pPeer->hbTimer);
        if (pPeer->syncFd > 0) taosCloseTcpSocket(pPeer->syncFd);
      }
      pPeer->ip = 0;
      tfree(row);
      break;
  }

  return NULL;
}

static int32_t mpeerReadMnodeSdb(char *directory) {
  tsMnodeSdb = sdbOpenTable(MPEER_MAX_MNODES, sizeof(SSdbPeer), "mnode", SDB_KEYTYPE_UINT32, directory, mgmtPeerTool);
  if (tsMnodeSdb == NULL) {
    mpeerError("failed to init mpeer data");
    return -1;
  }

  SSdbPeer *pPeer;
  void     *pNode = NULL;
  int32_t   pos   = 1;  // the first slot is reserved for self
  while (true) {
    pNode = sdbFetchRow(tsMnodeSdb, pNode, (void **)&pPeer);
    if (pPeer == NULL) break;
    if (strcmp(pPeer->zone, MPEER_DEFAULT_ZONE) != 0) continue;

    pPeer->status  = SDB_STATUS_OFFLINE;
    pPeer->role    = SDB_ROLE_UNDECIDED;
    pPeer->syncFd  = 0;
    pPeer->hbTimer = NULL;
    pPeer->pSync   = NULL;

    if (pPeer->ip != tsMpeerSelfIp) {
      sdbPeer[pos++] = pPeer;
    } else {
      pSelf = pPeer;
    }

    tsMpeerNumOfPeers++;
    if (tsMpeerNumOfPeers >= SDB_MAX_PEERS) break;
  }

  return 0;
}

static int32_t mpeerInitRpc() {
  SRpcInit rpcInit;

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = tsPrivateIp;
  rpcInit.localPort    = tsMgmtMgmtPort;
  rpcInit.label        = "MND-MPEER-s";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = mpeerProcessMsgFromPeer;
  rpcInit.sessions     = 100;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 2000;
  rpcInit.afp          = mpeerRetriveUserAuthInfo;

  tsMpeerServer = rpcOpen(&rpcInit);
  if (tsMpeerServer == NULL) {
    sdbError("failed to init %s mpeer server");
    return -1;
  }

  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = tsPrivateIp;
  rpcInit.localPort    = tsMgmtMgmtPort;
  rpcInit.label        = "MND-MPEER-c";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = mpeerProcessRspFromPeer;
  rpcInit.sessions     = 100;
  rpcInit.connType     = TAOS_CONN_CLIENT;
  rpcInit.idleTime     = tsShellActivityTimer * 2000;
  rpcInit.user         = "mpeer";
  rpcInit.ckey         = "key";
  rpcInit.secret       = "secret";

  tsMpeerClient = rpcOpen(&rpcInit);
  if (tsMpeerClient == NULL) {
    sdbError("failed to init %s mpeer client");
    return -1;
  }

  return 0;
}

int32_t mpeerInitMnodes(char *directory) {
  SSdbPeer sdbObj;
  tsMnodeUpdateSize = sdbObj.updateEnd - (char *) &sdbObj;
  tsMpeerSelfIp     = inet_addr(tsPrivateIp);
  tsMpeerPublicIp   = inet_addr(tsPublicIp);


  memset(sdbPeer, 0, sizeof(SSdbPeer *) * SDB_MAX_PEERS);

  if (pSdbIpList == NULL) pSdbIpList = calloc(1, sizeof(SRpcIpSet));
  if (pSdbPublicIpList == NULL) pSdbPublicIpList = calloc(1, sizeof(SRpcIpSet));

  pthread_mutex_init(&tsMpeerQueue.qmutex, NULL);
  pthread_mutex_init(&tsMpeerMutex, NULL);
  sem_init(&tsSdbSem, 0, 0);

  if (mpeerReadMnodeSdb(directory) != 0) {
    mpeerError("failed to open mnodes.db");
    return -1;
  }

  tsMpeerTmr = taosTmrInit(SDB_MAX_PEERS * 100, 200, 100000, "MND-MPEER");
  if (tsMpeerTmr == NULL) {
    mpeerError("failed to init mpeer timer");
    return -1;
  }

  tsMpeerSched = taosInitScheduler(MPEER_MAX_QUEUE_SIZE, 1, "mpeer");
  if (tsMpeerSched == NULL) {
    mpeerError("failed to init mpeer queue");
    return -1;
  }

  if (mpeerInitRpc() != 0) {
    mpeerError("failed to rpc for mpeer");
    return -1;
  }

  int64_t oldSdbVersion = sdbVersion;
  int64_t oldTableId = ((SSdbTable*)tsMnodeSdb)->id;

  // add masterIP into peer
  uint32_t masterIp = inet_addr(tsMasterIp);
  uint32_t masterPublicIp = (masterIp == tsMpeerSelfIp) ? tsMpeerPublicIp : masterIp;
  mpeerAddMnodeWithRole(masterIp, masterPublicIp, SDB_ROLE_MASTER);

  sdbPrint("add peer:%s to mnodes, old sdbVersion:%" PRId64 " new sdbVersion:%" PRId64 ", old id:%" PRId64 " new id:%" PRId64,
          taosIpStr(masterIp), oldSdbVersion, sdbVersion, oldTableId, ((SSdbTable*)tsMnodeSdb)->id);

  if (pSelf == NULL || pSelf->status == SDB_STATUS_DELETED) {
    pSelf = (SSdbPeer *)malloc(sizeof(SSdbPeer));
    memset(pSelf, 0, sizeof(SSdbPeer));
    pSelf->ip = tsMpeerSelfIp;
    pSelf->publicIp = tsMpeerPublicIp;
    strcpy(pSelf->ipstr, tsPrivateIp);
  }

  pSelf->status      = SDB_STATUS_UNSYNCED;
  pSelf->role        = SDB_ROLE_UNDECIDED;
  pSelf->numOfMnodes = 0;
  if ((tsMpeerNumOfPeers == 1) && (masterIp == tsMpeerSelfIp)) {
    sdbPrint("numOfPeers:%d, master:%s self:%s work as master, sdbVersion:%" PRId64 " id:%" PRId64,
            tsMpeerNumOfPeers, taosIpStr(masterIp), taosIpStr(tsMpeerSelfIp), sdbVersion, ((SSdbTable*)tsMnodeSdb)->id);
    mpeerWorkAsMaster();
  } else {
    /*
     * The first mnode created when the system just start, should not enter version management
     */
    sdbPrint("reset sdbVersion from %" PRId64 " to old %" PRId64 ", id from %" PRId64 " to %" PRId64 ", for mnode changed",
            sdbVersion, oldSdbVersion, ((SSdbTable*)tsMnodeSdb)->id, oldTableId);
    sdbVersion = oldSdbVersion;
    ((SSdbTable*)tsMnodeSdb)->id = oldTableId;
  }

  mpeerUpdateIpList();

  for (int32_t i = 1; i < SDB_MAX_PEERS; ++i) {
    SSdbPeer *pPeer = sdbPeer[i];
    if (pPeer && pPeer->hbTimer == NULL) {
      taosTmrReset(mpeerCheckPeerStatus, tsMgmtPeerHBTimer * 1000, pPeer, tsMpeerTmr, &pPeer->hbTimer);
    }
  }

  if (pSelf->role != SDB_ROLE_MASTER) {
    taosTmrReset(mpeerCheckRoleStatus, tsMgmtPeerHBTimer * 3000, NULL, tsMpeerTmr, &tsMpeerRoleTimer);
  }

  return 0;
}

void mpeerCleanUpMnodes() {
  if (tsMpeerServer) {
    rpcClose(tsMpeerServer);
    tsMpeerServer = NULL;
  }

  if (tsMpeerClient) {
    rpcClose(tsMpeerClient);
    tsMpeerClient = NULL;
  }

  if (tsMpeerSched != NULL) {
    taosCleanUpScheduler(tsMpeerSched);
    tsMpeerSched = NULL;
  }

  if (tsMpeerTmr) {
    taosTmrCleanUp(tsMpeerTmr);
    tsMpeerTmr = NULL;
  }

  taosTmrStopA(&tsMpeerRoleTimer);

  for (int32_t i = 0; i < SDB_MAX_PEERS; ++i) {
    SSdbPeer *pPeer = sdbPeer[i];
    if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED) continue;
    if (pPeer->hbTimer != NULL) {
      taosTmrStopA(&pPeer->hbTimer);
    }
    if (pPeer->syncFd > 0) {
      taosCloseTcpSocket(pPeer->syncFd);
    }
    sdbPeer[i] = NULL;
  }

  sdbCloseTable(tsMnodeSdb);

  sem_destroy(&tsSdbSem);
  pthread_mutex_destroy(&tsMpeerMutex);
  pthread_mutex_destroy(&tsMpeerQueue.qmutex);

  tsMpeerNumOfPeers = 0;
  tsMpeerCode       = 0;
  sdbMaster         = 0;
  sdbVersion        = 0;
}

static void mpeerProcessRspFromPeer(char msgType, void *pCont, int contLen, void *ahandle, int32_t code) {
  SSdbPeer *pPeer = ahandle;
  if (pPeer == NULL) {
    mpeerError("invalid rsp from peer, ahandle is NULL");
    return;
  }

  if (pCont == NULL || contLen == 0 || code != TSDB_CODE_SUCCESS) {
    sdbWarn("ip:%s, role:%s status:%s connection is gone, self numOfMnodes:%d",
            pPeer->ipstr, mpeerGetSdbRoleStr(pPeer->role), mpeerGetSdbStatusStr(pPeer->status), pSelf->numOfMnodes);

    if (msgType == TSDB_MSG_TYPE_SDB_FORWARD + 1) {
      tsMpeerCode = TSDB_CODE_OTHERS;
      sem_post(&tsSdbSem);
    }

    if (pPeer->status != SDB_STATUS_OFFLINE) {
      pPeer->status = SDB_STATUS_OFFLINE;
      pPeer->lostTime = mpeerGetTimeStamp();
      pSelf->numOfMnodes--;
      if (pSelf->role == SDB_ROLE_MASTER) mpeerCheckSelfRole();

      if (pPeer->role == SDB_ROLE_MASTER) {
        taosTmrReset(mpeerCheckRoleStatus, tsMgmtPeerHBTimer * 2500, NULL, tsMpeerTmr, &tsMpeerRoleTimer);
        sdbWarn("connection to master:%s is lost", pPeer->ipstr);
      }

      pPeer->role = SDB_ROLE_UNDECIDED;
    }
    taosTmrReset(mpeerCheckPeerStatus, tsMgmtPeerHBTimer * 10000, pPeer, tsMpeerTmr, &pPeer->hbTimer);
  }

  if (msgType == TSDB_MSG_TYPE_SDB_SYNC_RSP) {
  } if (msgType == TSDB_MSG_TYPE_HEARTBEAT_RSP) {
    mpeerProcessHeartBeatRsp(pCont, contLen, code, pPeer);
  } else if (msgType == TSDB_MSG_TYPE_SDB_FORWARD_RSP) {
    mpeerProcessForwardRsp(pCont, contLen, code, pPeer);
  } else {
    sdbError("%s from %s is not processed", taosMsg[(int8_t)msgType], pPeer->ipstr);
  }

  if (msgType == TSDB_MSG_TYPE_HEARTBEAT) {
    taosTmrReset(mpeerCheckPeerStatus, tsMgmtPeerHBTimer * 1500, pPeer, tsMpeerTmr, &pPeer->hbTimer);
  }
  else {
    taosTmrReset(mpeerCheckPeerStatus, tsMgmtPeerHBTimer * 1000, pPeer, tsMpeerTmr, &pPeer->hbTimer);
  }
}

static void mpeerProcessMsgFromPeer(char msgType, void *pCont, int contLen, void *thandle, int32_t code) {
  // TODO: redefinite the msg from peer, not from conninfo
  uint32_t peerIp = 0;
  SSdbPeer *pPeer = NULL;// = mpeerGetMnode(peerIp);

  if (pPeer == NULL) {
    mpeerError("peer:%s, peer not found or already released", taosIpStr(peerIp));
    return;
  }

  taosTmrStopA(&pPeer->hbTimer);

  if (msgType == TSDB_MSG_TYPE_SDB_SYNC) {
    mpeerProcessSyncMsg((char*)pCont, contLen, pPeer, thandle);
  } else if (msgType == TSDB_MSG_TYPE_HEARTBEAT) {
    mpeerProcessHeartBeatMsg(pCont, contLen, pPeer, thandle);
  } else if (msgType == TSDB_MSG_TYPE_SDB_FORWARD) {
    SSchedFordwardMsg *pMsg = calloc(1, contLen + sizeof(SSchedFordwardMsg));
    pMsg->msgType = TSDB_MSG_TYPE_SDB_FORWARD;
    pMsg->msgLen  = contLen;
    memcpy(pMsg->content + sizeof(SSchedFordwardMsg), pCont, contLen);

    SSchedMsg schedMsg;
    schedMsg.fp      = mpeerProcessForwardReqQueue;
    schedMsg.tfp     = NULL;
    schedMsg.ahandle = pPeer;
    schedMsg.thandle = NULL;
    taosScheduleTask(tsMpeerSched, &schedMsg);
  } else {
    sdbError("%s from %s is not processed", taosMsg[(int8_t)msgType], pPeer->ipstr);
  }

  if (msgType == TSDB_MSG_TYPE_HEARTBEAT) {
    taosTmrReset(mpeerCheckPeerStatus, tsMgmtPeerHBTimer * 1500, pPeer, tsMpeerTmr, &pPeer->hbTimer);
  }
  else {
    taosTmrReset(mpeerCheckPeerStatus, tsMgmtPeerHBTimer * 1000, pPeer, tsMpeerTmr, &pPeer->hbTimer);
  }
}

static int32_t mpeerUpdatePeerStatus(SSdbPeer *pPeer, char *msg, int32_t msgLen) {
  SMpeerStatusRsp *pStatus = (SMpeerStatusRsp *)msg;

  if (pPeer->status == SDB_STATUS_OFFLINE) {
    pSelf->numOfMnodes++;
    sdbPrint("ip:%s is online, role:%s status:%s numOfMnodes:%d", pPeer->ipstr, mpeerGetSdbRoleStr(pStatus->role),
            mpeerGetSdbStatusStr(pStatus->status), pSelf->numOfMnodes);
  }

  int32_t oldRole = pPeer->role;
  pPeer->status = pStatus->status;
  pPeer->numOfDnodes = htonl(pStatus->numOfDnodes);
  pPeer->numOfMnodes = pStatus->numOfMnodes;
  pPeer->dbVersion = htobe64(pStatus->dbVersion);
  pPeer->role = pStatus->role;

  if (pPeer->publicIp != pStatus->publicIp) {
    pPeer->publicIp = pStatus->publicIp;
    mpeerUpdateIpList();
  }

  if (pPeer->role == SDB_ROLE_MASTER) {
    if (oldRole != SDB_ROLE_MASTER) {
      sdbPrint("master is set to:%s", pPeer->ipstr);
      mpeerUpdateIpList();
    }

    if (pSelf->role == SDB_ROLE_MASTER) {
      sdbError("besides myself, another master:%s", pPeer->ipstr);
      if (pSelf->ip < pPeer->ip) sdbStopWorkingAsMaster();
    } else {
      if (pPeer->dbVersion == sdbVersion) {
        pSelf->status = SDB_STATUS_SERVING;
      } else if (pPeer->dbVersion > sdbVersion) {
        if (pSelf->status != SDB_STATUS_SYNCING) {
          sdbPrint("peer:%s dbVersion:%" PRIu64 ", sdbVersion:%" PRId64 ", sync start", pPeer->ipstr, pPeer->dbVersion, sdbVersion);
          mpeerStartSyncProcess(pPeer);
        }
      } else {
        sdbError("peer:%s is master, but version:%d is lower than self:%d", pPeer->ipstr, pPeer->dbVersion, sdbVersion);
        pPeer->role = SDB_ROLE_UNDECIDED;
        taosTmrReset(mpeerCheckRoleStatus, 100, NULL, tsMpeerTmr, &tsMpeerRoleTimer);
      }
    }
  } else {
    if (oldRole == SDB_ROLE_MASTER) {
      mpeerCheckRoleStatus(NULL, NULL);
    } else {
      if ((pSelf->role == SDB_ROLE_MASTER) && (pPeer->dbVersion > sdbVersion)) {
        sdbError("master, but version:%d is lower than peer:%s version:%d", sdbVersion, pPeer->ipstr, pPeer->dbVersion);
        sdbStopWorkingAsMaster();
      }
    }
  }

  return 0;
}

void mpeerEncodeSelfStatus(SSdbPeer *pPeer, SMpeerStatusRsp *pStatus) {
  pStatus->role        = pSelf->role;
  pStatus->status      = pSelf->status;
  pStatus->numOfMnodes = pSelf->numOfMnodes;
  pStatus->dbVersion   = htobe64(sdbVersion);
  pStatus->publicIp    = tsMpeerPublicIp;
  sdbTrace("encode self status, sdbVersion:%" PRId64, sdbVersion);
}

static void mpeerProcessHeartBeatMsg(void *msg, int32_t msgLen, SSdbPeer *pPeer, void *thandle) {
  mpeerUpdatePeerStatus(pPeer, msg, msgLen);

  SMpeerStatusRsp *pStatus = rpcMallocCont(sizeof(SMpeerStatusRsp));
  if (pStatus == NULL) {
    mpeerError("failed to alloc mpeer heart beat message");
    rpcSendResponse(thandle, TSDB_CODE_SERV_OUT_OF_MEMORY, NULL, 0);
  }

  mpeerEncodeSelfStatus(pPeer, pStatus);
  rpcSendResponse(thandle, TSDB_CODE_SUCCESS, pStatus, sizeof(SMpeerStatusRsp));
}

void mpeerProcessHeartBeatRsp(void *msg, int32_t msgLen, int32_t code, SSdbPeer *pPeer) {
  if (msg == NULL || msgLen == 0 || code != TSDB_CODE_SUCCESS) {
    sdbTrace("HB rsp code:%d !!!", code);
  } else {
    mpeerUpdatePeerStatus(pPeer, msg, msgLen);
  }
}

void mpeerUpdateIpList() {
  int32_t numOfIps = 0;
  // pSdbIpList->numOfIps = 0;
  // pSdbPublicIpList->numOfIps = 0;

  // 0 reserved for master
  for (int32_t i = 0; i < SDB_MAX_PEERS; ++i) {
    if (sdbPeer[i] == NULL || sdbPeer[i]->ip == 0 || sdbPeer[i]->status == SDB_STATUS_DELETED) continue;

    if (sdbPeer[i]->role == SDB_ROLE_MASTER) {
      pSdbIpList->ip[numOfIps] = sdbPeer[i]->ip;
      pSdbPublicIpList->ip[numOfIps] = sdbPeer[i]->publicIp;
      sdbTrace("index:%d ip:%s publicIp:%s is master", numOfIps, taosIpStr(pSdbIpList->ip[numOfIps]), taosIpStr(pSdbPublicIpList->ip[numOfIps]));
      numOfIps++;

      break;
    }
  }

  for (int32_t i = 0; i < SDB_MAX_PEERS; ++i) {
    if (sdbPeer[i] == NULL || sdbPeer[i]->ip == 0 || sdbPeer[i]->status == SDB_STATUS_DELETED) continue;

    pSdbIpList->ip[numOfIps] = sdbPeer[i]->ip;
    pSdbPublicIpList->ip[numOfIps] = sdbPeer[i]->publicIp;
    sdbTrace("index:%d ip:%s publicIp:%s", numOfIps, taosIpStr(pSdbIpList->ip[numOfIps]), taosIpStr(pSdbPublicIpList->ip[numOfIps]));
    numOfIps++;
  }

  pSdbIpList->numOfIps = numOfIps;
  pSdbPublicIpList->numOfIps = numOfIps;
}

void mpeerCheckSelfRole() {
  if (pSelf == NULL || pSelf->status == SDB_STATUS_DELETED) return;

  if (pSelf->role == SDB_ROLE_UNDECIDED) {
    sdbMaster = 0;
    taosTmrReset(mpeerCheckRoleStatus, tsMgmtPeerHBTimer * 1000, NULL, tsMpeerTmr, &tsMpeerRoleTimer);
    return;
  }

  if (pSelf->role == SDB_ROLE_MASTER) {
    if ((pSelf->numOfMnodes + 1.0) / tsMpeerNumOfPeers < 0.5) {
      sdbPrint("self role:%s status:%s numOfMnodes:%d tsMpeerNumOfPeers:%d, mnode ratio %f < 0.5, stop work as master",
               mpeerGetSdbRoleStr(pSelf->role), mpeerGetSdbStatusStr(pSelf->status),
               pSelf->numOfMnodes, tsMpeerNumOfPeers, (pSelf->numOfMnodes + 1.0) / tsMpeerNumOfPeers);
      sdbStopWorkingAsMaster();
    }
  }
}

void mpeerCheckRoleStatus(void *param, void *tmrId) {
  int32_t       i, pos = -1;
  SSdbPeer *pPeer;
  uint32_t  maxIp = 0;
  uint64_t  dbVersion = 0;

  if (pSelf == NULL || pSelf->status == SDB_STATUS_DELETED) {
    taosTmrReset(mpeerCheckRoleStatus, tsMgmtPeerHBTimer * 1000, NULL, tsMpeerTmr, &tsMpeerRoleTimer);
    return;
  }

  tsMpeerRoleTimer = NULL;
  sdbTrace("check role status, self status:%s self role:%s dbVersion:%d",
          mpeerGetSdbStatusStr(pSelf->status), mpeerGetSdbRoleStr(pSelf->role), pSelf->dbVersion);

  for (i = 0; i < SDB_MAX_PEERS; ++i) {
    pPeer = sdbPeer[i];
    if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED) continue;

    if (pPeer->role == SDB_ROLE_MASTER && pPeer->status == SDB_STATUS_SERVING) {
      sdbPrint("master:%s is there, self status:%s role:%s",
              pPeer->ipstr, mpeerGetSdbStatusStr(pSelf->status), mpeerGetSdbRoleStr(pSelf->role));
      if (pPeer != pSelf) {
        pSelf->role = SDB_ROLE_SLAVE;
        if (pSelf->status != SDB_STATUS_SYNCING) pSelf->status = SDB_STATUS_SERVING;
      }
      return;
    }
  }

  // pick up the master
  pSelf->dbVersion = sdbVersion;
  pSelf->createdTime = mpeerGetTimeStamp();

  for (i = 0; i < SDB_MAX_PEERS; ++i) {
    pPeer = sdbPeer[i];
    if (pPeer == NULL) continue;

    if (pPeer != NULL) {
      sdbPrint("id:%d, peer:%s, role:%s, dbVersion:%" PRIu64 ", status:%s, numOfMnodes:%d, numOfDnodes:%d",
               i, pPeer->ipstr, mpeerGetSdbRoleStr(pPeer->role), pPeer->dbVersion,
               mpeerGetSdbStatusStr(pPeer->status), pPeer->numOfMnodes, pPeer->numOfDnodes);
    }

    if (pPeer->status == SDB_STATUS_DELETED) {
      sdbPrint("peer:%s, is deleting, give up", pPeer->ipstr);
      continue;
    }

    if (pPeer->dbVersion < dbVersion) {
      sdbPrint("peer:%s, version:%d < current version:%d, give up", pPeer->ipstr, pPeer->dbVersion, dbVersion);
      continue;
    }

    if (pPeer->dbVersion > dbVersion) {
      sdbPrint("peer:%s, version:%d larger than current version:%d, set pos from %d to -1",
              pPeer->ipstr, pPeer->dbVersion, dbVersion, pos);
      dbVersion = pPeer->dbVersion;
      pos = -1;
      maxIp = 0;
    }

    if (pPeer->numOfDnodes <= 0) {
      sdbPrint("peer:%s, numOfDnodes:%d smaller than 0, give up", pPeer->ipstr, pPeer->numOfDnodes);
      continue;
    }

    if (pPeer->status == SDB_STATUS_SYNCING) {
      sdbPrint("peer:%s, status is syncing, give up", pPeer->ipstr);
      continue;
    }

    if (pPeer->status == SDB_STATUS_OFFLINE) {
      sdbPrint("peer:%s, status is offline, give up", pPeer->ipstr);
      continue;
    }
    if ((pPeer->numOfMnodes + 1.0) / tsMpeerNumOfPeers < 0.5) {
      sdbPrint("peer:%s, mnode ratio %f smaller than 0.5, give up, numOfMnodes:%d tsMpeerNumOfPeers:%d",
              pPeer->ipstr, (pPeer->numOfMnodes + 1.0) / tsMpeerNumOfPeers, pPeer->numOfMnodes, tsMpeerNumOfPeers);
      continue;
    }

    if ((pPeer->dbVersion == dbVersion) && (pPeer->ip > maxIp)) {
      sdbPrint("peer:%s, version:%d equal with current version:%d, maxIp:%d, set pos from %d to %d",
               pPeer->ipstr, pPeer->dbVersion, dbVersion, maxIp, pos, i);
      maxIp = pPeer->ip;
      pos = i;
    } else {
      sdbPrint("peer:%s, version:%d current version:%d, maxIp:%d pos:%d give up",
               pPeer->ipstr, pPeer->dbVersion, dbVersion, maxIp, pos);
    }
  }

  if (pos >= 0 && pos < SDB_MAX_PEERS) {
    sdbPrint("%s shall work as master", sdbPeer[pos]->ipstr);
  } else {
    sdbPrint("no master is elected, pos:%d", pos);
  }

  if (pos == 0) {
    if ((pSelf->status == SDB_STATUS_SERVING) || (pSelf->numOfMnodes >= tsMpeerNumOfPeers - 1)) {
      mpeerWorkAsMaster();
    } else {
      sdbPrint("%s self status:%s numOfMnodes:%d tsMpeerNumOfPeers:%d, can not work as master",
              sdbPeer[pos]->ipstr, mpeerGetSdbStatusStr(pSelf->status), pSelf->numOfMnodes, tsMpeerNumOfPeers);
    }
  }

  taosTmrReset(mpeerCheckRoleStatus, tsMgmtPeerHBTimer * 1000, NULL, tsMpeerTmr, &tsMpeerRoleTimer);
}

void mpeerCheckPeerStatus(void *param, void *tmrId) {
  SSdbPeer *pPeer = (SSdbPeer *) param;
  if (pPeer == NULL || pPeer->status == SDB_STATUS_DELETED) return;
  if (pPeer->ip == 0) return;
  if (pSelf == NULL || pSelf->status == SDB_STATUS_DELETED) return;

  SMpeerStatusMsg *pStatus = rpcMallocCont(sizeof(SMpeerStatusMsg));
  if (pStatus == NULL) {
    mpeerError("failed to alloc mpeer heart beat message");
    return;
  }

  mpeerEncodeSelfStatus(pPeer, pStatus);

  mpeerTrace("send heartbeat msg to peer:%s", pPeer->ipstr);
  rpcSendRequest(tsMpeerClient, &tsMpeerIpSet, TSDB_MSG_TYPE_HEARTBEAT, pStatus, sizeof(SMpeerStatusRsp), pPeer);
}

int32_t mpeerTransferWholeDataToPeer(int32_t syncFd, SSdbTable *pTable) {
  struct stat fstat;
  uint64_t    size;
  int32_t         sfd;
  SForwardMsg forward;

  stat(pTable->fn, &fstat);
  size = fstat.st_size;

  fdatasync(pTable->fd);
  sfd = open(pTable->fn, O_RDONLY);
  if (sfd < 0) {
    sdbError("table:%s fd:%d, failed to open file:%s, reason:%s", pTable->name, syncFd, pTable->fn, strerror(errno));
    return -1;
  }

  forward.dbId = -pTable->dbId;
  forward.type = 0;
  forward.version = htobe64(size);
  forward.dataLen = 0;

  int32_t writeLen = write(syncFd, &forward, sizeof(forward));
  if (writeLen != sizeof(forward)) {
    sdbError("table:%s fd:%d, failed to send forward msg:%d for whole sync, writeLen:%d reason:%s",
             pTable->name, syncFd, sizeof(forward), writeLen, strerror(errno));
    tclose(sfd);
    return -1;
  }


  if (tsendfile(syncFd, sfd, NULL, size) < 0) {
    sdbError("table:%s fd:%d, failed to transfer file:%s to peer, size:%" PRIu64 " reason:%s",
        pTable->name, syncFd, pTable->fn, size, strerror(errno));
    tclose(sfd);
    return -1;
  }

  sdbPrint("table:%s fd:%d, file:%s is sent to peer, size:%" PRIu64, pTable->name, syncFd, pTable->fn, size);
  tclose(sfd);

  return 0;
}

int32_t mpeerRetrieveRows(int32_t syncFd, SSdbTable *pTable, uint64_t version) {
  int32_t         versionDelta;
  int32_t         firstUpdate;
  int32_t         updatePos;
  int64_t     firstVersion, curVersion;
  SForwardMsg forward;
  int32_t         rowSize = 0;

//  if (strcmp(pTable->name, "mnode") == 0) {
//    sdbPrint("table:%s fd:%d, force full sync", pTable->name, syncFd);
//    return mpeerTransferWholeDataToPeer(syncFd, pTable);
//  }

  char *msg = (char *)malloc(pTable->maxRowSize);
  if (msg == NULL) return -1;
  memset(msg, 0, pTable->maxRowSize);

  if (pTable->numOfUpdates > 0) {
    pTable->numOfUpdates = pTable->numOfUpdates % pTable->maxRows;
    firstUpdate = (pTable->updatePos - pTable->numOfUpdates + pTable->maxRows + 1) % pTable->maxRows;
    firstVersion = pTable->id - pTable->numOfUpdates + 1;
    versionDelta = version - firstVersion;

    if (versionDelta >= -1) {
      sdbPrint("table:%s fd:%d, numOfUpdates:%d maxRows:%d updatePos:%d firstVersion:%d delta:%d, start sync",
               pTable->name, syncFd, pTable->numOfUpdates, pTable->maxRows, pTable->updatePos, firstVersion, versionDelta);

      // retrieve from memory
      updatePos = (firstUpdate + versionDelta + 1) % pTable->maxRows;
      curVersion = version + 1;

      int32_t writeLen = 0;
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
        sdbPrint("table:%s fd:%d, %d rows data are sent, rowSize:%d", pTable->name, syncFd, curVersion - version - 1, rowSize);
      }

      tfree(msg);
      return 0;
    } else {
      sdbPrint("table:%s fd:%d, numOfUpdates:%d maxRows:%d updatePos:%d firstVersion:%d delta:%d, start full sync",
               pTable->name, syncFd, pTable->numOfUpdates, pTable->maxRows, pTable->updatePos, firstVersion, versionDelta);
    }
  } else {
    sdbPrint("table:%s fd:%d, numOfUpdates:%d less than 0, start full sync", pTable->name, syncFd, pTable->numOfUpdates);
  }

  tfree(msg);

  return mpeerTransferWholeDataToPeer(syncFd, pTable);
}

void *mpeerRetrieveSyncData(void *argv) {
  SSdbPeer *pPeer = (SSdbPeer *)argv;
  SSdbSync *pSync = (SSdbSync *)pPeer->pSync;

  taosBlockSIGPIPE();
  sdbPrint("fd:%d, send sdb retrieve data to ip:%s:%d", pPeer->syncFd, pPeer->ipstr, tsMgmtSyncPort);

  // based on dbVersion, forward the data to peer
  for (int32_t i = 0; i < pSync->numOfTables; ++i) {
    if (tableList[i]->id > pSync->version[i]) {
      sdbPrint("table:%s fd:%d, peer version:%d self version:%d, send data",
          tableList[i]->name, pPeer->syncFd, pSync->version[i], tableList[i]->id);
      mpeerRetrieveRows(pPeer->syncFd, tableList[i], pSync->version[i]);
    } else {
      sdbTrace("table:%s fd:%d, peer version:%d self version:%d, no need to send data",
          tableList[i]->name, pPeer->syncFd, pSync->version[i], tableList[i]->id);
    }
  }

  close(pPeer->syncFd);
  sdbPrint("fd:%d, send sdb retrieve data finished, sdbVersion:%d", pPeer->syncFd, sdbVersion);
  pPeer->syncFd = 0;

  return NULL;
}

void mpeerProcessSyncMsg(void *msg, int32_t msgLen, SSdbPeer *pPeer, void *thandle) {
  int32_t        code = 0;
  pthread_attr_t thattr;
  pthread_t      thread;
  SSdbSync *     pSync;

  /*
   * Multiple messages may trigger synchronization at the same time
   * Use syncFd > 0 as a condition to determine whether synchronization is in progress
   */
  if (pPeer->syncFd > 0) {
    sdbError("%s, a sync thread is already started, fd:%d", pPeer->ipstr, pPeer->syncFd);
    goto _sync_req_over;
  }

  pSync = (SSdbSync *)msg;
  for (int32_t i = 0; i < pSync->numOfTables; ++i) {
    pSync->version[i] = htobe64(pSync->version[i]);
  }

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

  sdbPrint("fd:%d, ip:%s:%d sync tcp socket is setup", pPeer->syncFd, pPeer->ipstr, tsMgmtSyncPort);

  // start a new thread to transfer the cache
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  if (pthread_create(&thread, &thattr, mpeerRetrieveSyncData, pPeer) != 0) {
    tclose(pPeer->syncFd);
    pPeer->syncFd = 0;
    sdbError("fd:%d, ip:%s:%d failed to create sync thread, reason:%s", pPeer->syncFd, pPeer->ipstr, tsMgmtSyncPort, strerror(errno));
    code = TSDB_CODE_APP_ERROR;
  }

_sync_req_over:
  rpcSendResponse(thandle, code, NULL, 0);
}

int32_t mpeerStartFullSync(int32_t tcpFd, SForwardMsg *pForward) {
  SSdbTable *pTable;
  int32_t        dfd;
  int64_t    size;

  pTable = tableList[-pForward->dbId];
  size = htobe64(pForward->version);
  tclose(pTable->fd);

  dfd = open(pTable->fn, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  if (dfd < 0) {
    sdbError("fd:%d table:%s, failed to open file:%s for full sync", tcpFd, pTable->name, pTable->fn);
    return -1;
  }

  /*
    int32_t ret;
    int32_t leftSize = size;
    while ( leftSize > 0 ) {
      ret = sendfile(dfd, tcpFd, NULL, leftSize);
      if ( ret < 0 ) break;
      leftSize -= ret;
    }
  */
  int32_t ret = taosCopyFds(tcpFd, dfd, size);
  close(dfd);

  if (ret < 0) {
    remove(pTable->fn);
    sdbError("fd:%d table:%s, failed to receive table file:%s for full sync, ret:%d size:%" PRId64 ", reason:%s",
        tcpFd, pTable->name, pTable->fn, ret, size, strerror(errno));
  } else {
    sdbPrint("fd:%d table:%s, %s is received from master for full sync, ret:%d size:%" PRId64 " reset table",
        tcpFd, pTable->name, pTable->fn, ret, size);
    sdbResetTable(pTable);
  }

  return ret;
}

void mpeerRestoreDbReq(int32_t tcpFd) {
  // if TSDB_MAX_VNODES set to 128, the size may be 3696
  /* char         cont[10240]; */
  char *       cont = malloc(65536);
  int32_t      ret = 0, dataLen;
  SForwardMsg *pForward = (SForwardMsg *)cont;

  while (1) {
    ret = taosReadMsg(tcpFd, pForward, sizeof(SForwardMsg));
    if (ret <= 0) {
      sdbPrint("fd:%d, failed to read forward msg size, ret:%d reason:%s, restore finished", tcpFd, ret, strerror(errno));
      break;
    }

    dataLen = htons(pForward->dataLen);
    sdbTrace("fd:%d, forward msg size received, table:%s type:%s version:%" PRIu64 " dataLen:%d ret:%d",
            tcpFd, mpeerGetSdbTableName(pForward->dbId), mpeerGetSdbOperName(pForward->type), htobe64(pForward->version), dataLen, ret);

    if (dataLen > 0) {
      ret = taosReadMsg(tcpFd, pForward->data, dataLen);
      if (ret <= 0) {
        sdbError("fd:%d, failed to read forward msg, dataLen:%d ret:%d reason:%s", tcpFd, dataLen, ret, strerror(errno));
        break;
      }
    }
    /*
     * datalen is less than 0 while full synchronization
     */
    //else {
    // sdbError("fd:%d, invalid forward msg, dataLen:%d ret:%d reason:%s", tcpFd, dataLen, ret, strerror(errno));
    // break;
    //}

    if (pForward->dbId < 0) {
      ret = mpeerStartFullSync(tcpFd, pForward);
      if (ret < 0) {
        sdbError("fd:%d, failed to full sync, ret:%d", tcpFd, ret);
        break;
      } else {
        sdbPrint("fd:%d, full sync finished, ret:%d", tcpFd, ret);
      }
    } else {
      if (dataLen > 0) {
        ret = mpeerProcessQueuedDbReq(cont, dataLen + sizeof(SForwardMsg));
        if (ret < 0) {
          sdbError("fd:%d, failed to process queue db:%s req, ret:%d", tcpFd, mpeerGetSdbTableName(pForward->dbId), ret);
          break;
        } else {
          sdbTrace("fd:%d, forward msg processed, db:%s ret:%d sdbVersion:%" PRId64 "", tcpFd, mpeerGetSdbTableName(pForward->dbId), ret, sdbVersion);
        }
      } else {
        sdbError("fd:%d, invalid forward msg dataLen:%d db:%s", tcpFd, dataLen, mpeerGetSdbTableName(pForward->dbId));
      }
    }
  }

  tfree(cont);

  if (ret < 0) {
    sdbError("fd:%d, sync failed, sdbVersion:%" PRId64 " reason:%s ", tcpFd, sdbVersion, strerror(errno));
    pSelf->status = SDB_STATUS_UNSYNCED;
  } else {
    mpeerProcessBufferedForwards();
    // pSelf->status = SDB_STATUS_SERVING;
    sdbPrint("fd:%d, sync is finished, sdbVersion:%" PRId64 "", tcpFd, sdbVersion);
  }
}

void *mpeerAcceptSyncTcpConnection(void *argv) {
  SSdbPeer *         pPeer = (SSdbPeer *)argv;
  int32_t            tcpFd;
  int64_t            connFd = -1;
  struct sockaddr_in clientAddr;

  tcpFd = taosOpenTcpServerSocket(tsPrivateIp, tsMgmtSyncPort);
  if (tcpFd <= 0) {
    sdbError("failed to create sync TCP socket, reason:%s", strerror(errno));
    pPeer->status = SDB_STATUS_UNSYNCED;
    goto _sync_over;
  }

  taosBlockSIGPIPE();
  sdbPrint("sync TCP server is created, ip:%s port:%hu", tsMpeerSelfIp, tsMgmtSyncPort);

  int32_t msgLen = sizeof(SSdbSync) + sdbNumOfTables * sizeof(uint64_t);
  SSdbSync *pSync = rpcMallocCont(msgLen);
  if (pSync == NULL) goto _sync_over;

  pSync->numOfTables = sdbNumOfTables;

  for (int32_t i = 0; i < sdbNumOfTables; ++i) {
    pSync->version[i] = htobe64(tableList[i]->id);
  }

  rpcSendRequest(tsMpeerClient, &tsMpeerIpSet, TSDB_MSG_TYPE_SDB_SYNC, pSync, msgLen, 0);

  socklen_t addrlen = sizeof(clientAddr);
  connFd = accept(tcpFd, (struct sockaddr *)&clientAddr, &addrlen);

  if (connFd < 0) {
    sdbError("sync TCP accept failure, reason:%s", strerror(errno));
    pPeer->status = SDB_STATUS_UNSYNCED;
    goto _sync_over;
  }

  sdbPrint("sync TCP connection from ip:%s port:%hu fd:%d", inet_ntoa(clientAddr.sin_addr), htons(clientAddr.sin_port), connFd);

  mpeerRestoreDbReq(connFd);

_sync_over:
  if (tcpFd > 0) taosCloseTcpSocket(tcpFd);
  if (connFd > 0) taosCloseTcpSocket(connFd);

  return NULL;
}

void mpeerStartSyncProcess(SSdbPeer *pPeer) {
  pthread_attr_t thattr;
  pthread_t      thread;

  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_DETACHED);
  if (pthread_create(&(thread), &thattr, (void *)mpeerAcceptSyncTcpConnection, pPeer) != 0) {
    sdbError("failed to create sync TCP accept thread, reason:%s", strerror(errno));
    return;
  }
  pthread_attr_destroy(&thattr);
}

int32_t mpeerProcessDbReq(char *cont, int32_t contLen) {
  SForwardMsg *pForward;
  SSdbTable *  pTable;

  pForward = (SForwardMsg *)cont;
  pForward->version = htobe64(pForward->version);
  int32_t dataLen = contLen - sizeof(SForwardMsg);

  pTable = (SSdbTable *)tableList[pForward->dbId];
  if (pTable->id < pForward->version) {
    sdbPrint("version, peer:%d, self:%d, sync shall start!", pForward->version, pTable->id);
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

int32_t mpeerProcessQueuedDbReq(char *cont, int32_t contLen) {
  SForwardMsg *pForward;
  SSdbTable *  pTable;

  pForward = (SForwardMsg *)cont;
  pForward->version = htobe64(pForward->version);
  int32_t dataLen = contLen - sizeof(SForwardMsg);

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

int32_t mpeerProcessBufferedForwards() {
  int32_t         submits = 0;
  short       msgLen;
  char *      offset;
  STranQueue *pQueue = &tsMpeerQueue;

  offset = pQueue->buffer;

  while (submits < pQueue->trans) {
    msgLen = *((uint16_t *)offset);
    offset += sizeof(msgLen);
    mpeerProcessQueuedDbReq(offset, msgLen);
    offset += msgLen;
    submits++;
  }

  pthread_mutex_lock(&pQueue->qmutex);

  if (offset == NULL) offset = pQueue->buffer;
  while (submits < pQueue->trans) {
    msgLen = *((uint16_t *)offset);
    offset += sizeof(msgLen);
    mpeerProcessQueuedDbReq(offset, msgLen);
    offset += msgLen;
    submits++;
  }

  if (pSelf) pSelf->status = SDB_STATUS_SERVING;
  tfree(pQueue->buffer);
  pQueue->trans = 0;

  pthread_mutex_unlock(&pQueue->qmutex);

  return 0;
}

int32_t mpeerForwardDbReqToPeer(SSdbTable *pTable, char type, char *data, int32_t dataLen) {
  SSdbPeer *   pPeer;
  int32_t      numOfSuccess = 0;

  if (tsMpeerNumOfPeers <= 1) return 0;
  if (pSelf == NULL) return 0;
  if (pSelf->role != SDB_ROLE_MASTER) return 0;

  pthread_mutex_lock(&tsMpeerMutex);

  for (int32_t i = 1; i < SDB_MAX_PEERS; ++i) {
    pPeer = sdbPeer[i];
    if (pPeer && pPeer->status != SDB_STATUS_OFFLINE && pPeer->status != SDB_STATUS_DELETED) {
      SForwardMsg *pForward = rpcMallocCont(dataLen + sizeof(SForwardMsg));
      pForward->dbId    = pTable->dbId;
      pForward->type    = type;
      pForward->dataLen = htons(dataLen);
      pForward->version = htobe64(pTable->id);
      memcpy(pForward->data, data, dataLen);

      taosTmrStopA(&pPeer->hbTimer);
      pPeer->hbTimer = NULL;

      sdbTrace("table:%s type:%d db req is forwarding to:%s", pTable->name, type, pPeer->ipstr);
      rpcSendRequest(tsMpeerClient, &tsMpeerIpSet, TSDB_MSG_TYPE_SDB_FORWARD, pForward, sizeof(SFreeVnodeMsg) + dataLen, 0);

      int32_t trywaitTimes = 0;
      while (true) {
        int32_t ret = sem_trywait(&tsSdbSem);
        if (ret != 0) {
          if (trywaitTimes++ > MPEER_MAX_TRY_WAIT_TIMES) {
            sdbError("table:%s type:%d db req forward failed, trywaitTimes:%d", pTable->name, type, trywaitTimes);
            tsMpeerCode = ret;
            break;
          } else {
            taosMsleep(MPEER_TRY_WAIT_TIME_IN_MS);
            continue;
          }
        } else {
          sdbTrace("table:%s type:%d db req forward success, tsMpeerCode:%d", pTable->name, type, tsMpeerCode);
          break;
        }
      }

      //sem_wait(&tsSdbSem);
      if (tsMpeerCode == 0) numOfSuccess++;
      tsMpeerCode = 0;
    }
  }

  pthread_mutex_unlock(&tsMpeerMutex);

  if ((numOfSuccess + 1.0) / tsMpeerNumOfPeers >= 0.5) return 0;

  sdbError("table:%s type:%d, failed to forward, numOfSuccess:%d numOfPeers:%d", pTable->name, type, numOfSuccess,
           tsMpeerNumOfPeers);
  return -1;
}

static void mpeerProcessForwardRsp(void *cont, int32_t contLen, int32_t code, SSdbPeer *pPeer) {
  tsMpeerCode = code;
  sem_post(&tsSdbSem);
}

static void mpeerProcessForwardMsg(void *cont, int32_t contLen, SSdbPeer *pPeer, void *thandle) {
  STranQueue *pQueue = &tsMpeerQueue;

  if (pSelf == NULL || pSelf->status == SDB_STATUS_DELETED) {
    rpcSendResponse(thandle, TSDB_CODE_OTHERS, NULL, 0);
  }

  if (pSelf->status == SDB_STATUS_SERVING) {
    if (mpeerProcessDbReq(cont, contLen) < 0) mpeerStartSyncProcess(pPeer);

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
    sdbError("data from %s are thrown away, self status:%d", pPeer->ipstr, pSelf->status);
  }

  rpcSendResponse(thandle, TSDB_MSG_TYPE_SDB_FORWARD_RSP, NULL, 0);
}

static int32_t mpeerRetriveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  return TSDB_CODE_SUCCESS;
}
