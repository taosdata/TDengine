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

#define _DEFAULT_SOURCE
#include "os.h"
#include "tglobalcfg.h"
#include "tmodule.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tlog.h"
#include "mnode.h"
#include "tbalance.h"
#include "tcluster.h"
#include "tsync.h"
#include "tgrant.h"
#include "vnode.h"
#include "mpeer.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"
#include "dnodeMClient.h"


static void   *tsMnodeSdb = NULL;
static int32_t tsMnodeUpdateSize = 0;
static void   *tsMpeerSync = NULL;
static void   *tsMpeerWal = NULL;
static SSyncCfg tsMpeerSyncCfg = { .quorum = 1 };
static SWalCfg  tsMpeerWalCfg = { .commitLog = 2, .wals = 2 };
static int8_t   tsMpeerRole = TAOS_SYNC_ROLE_OFFLINE;
static int8_t   tsMpeerStatus = TAOS_MN_STATUS_OFFLINE;

static int32_t  mpeerCreateMnode(uint32_t ip);
static int32_t  mpeerDropMnode(uint32_t ip);
static int      mpeerWalCallback(void *arg);
static uint32_t mpeerGetFileInfo(void *ahandle, char *name, uint32_t *index, int32_t *size);
static int      mpeerGetWalInfo(void *ahandle, char *name, uint32_t *index);
static void     mpeerNotifyRole(void *ahandle, int8_t role);
static void     mpeerConfirmForward(void *pVnode, void *param, int32_t code);

static int32_t mpeerActionDestroy(SSdbOperDesc *pOper) {
  tfree(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionInsert(SSdbOperDesc *pOper) {
  SMnodeObj *pMnode = pOper->pObj;
  SDnodeObj *pDnode = clusterGetDnode(pMnode->dnodeId);
  if (pDnode != NULL) {
    pMnode->privateIp = pDnode->privateIp;
    pDnode->publicIp = pDnode->publicIp;
    strcpy(pMnode->mnodeName, pDnode->dnodeName);
    pMnode->role = TAOS_SYNC_ROLE_OFFLINE;
    pMnode->status = TAOS_MN_STATUS_OFFLINE;
  }
  
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionDelete(SSdbOperDesc *pOper) {
  SMnodeObj *pMnode = pOper->pObj;
  mTrace("mnode:%d, is dropped from sdb", pMnode->dnodeId);
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionUpdate(SSdbOperDesc *pOper) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionEncode(SSdbOperDesc *pOper) {
  SMnodeObj *pMnode = pOper->pObj;

  if (pOper->maxRowSize < tsMnodeUpdateSize) {
    return -1;
  } else {
    memcpy(pOper->rowData, pMnode, tsMnodeUpdateSize);
    pOper->rowSize = tsMnodeUpdateSize;
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t mpeerActionDecode(SSdbOperDesc *pOper) {
  SMnodeObj *pMnode = calloc(1, sizeof(SMnodeObj));
  if (pMnode == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  memcpy(pMnode, pOper->rowData, tsMnodeUpdateSize);
  pOper->pObj = pMnode;
  return TSDB_CODE_SUCCESS;
}

int32_t mpeerInit() {
  SMnodeObj tObj;
  tsMnodeUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableName    = "mnodes",
    .hashSessions = TSDB_MAX_MNODES,
    .maxRowSize   = tsMnodeUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_TYPE_AUTO,
    .insertFp     = mpeerActionInsert,
    .deleteFp     = mpeerActionDelete,
    .updateFp     = mpeerActionUpdate,
    .encodeFp     = mpeerActionEncode,
    .decodeFp     = mpeerActionDecode,
    .destroyFp    = mpeerActionDestroy,
  };

  tsMnodeSdb = sdbOpenTable(&tableDesc);
  if (tsMnodeSdb == NULL) {
    mError("failed to init mnodes data");
    return -1;
  }

  SMnodeObj *pMnode = NULL;
  void *     pDnode = NULL;
  int32_t    index  = 0;
  while (1) {
    pNode = mpeerGetNextMnode(pNode, &pMnode);
    if (pMnode == NULL) break;
    tsMpeerSyncCfg.nodeInfo[index].nodeId = pMnode->dnodeId;
    tsMpeerSyncCfg.nodeInfo[index].nodeIp = pMnode->privateIp;
    strcpy(tsMpeerSyncCfg.nodeInfo[index].name, pMnode->mnodeName);
    mpeerReleaseMnode(pMnode);
  }
  tsMpeerSyncCfg.replica = index;
  
  // first init by module status
  if (tsMpeerSyncCfg.replica == 0) {
    SDMNodeInfos mpeers = dnodeGetMpeerInfos();
    for (int32_t i = 0; i < mpeers.nodeNum; ++i) {
      SDMNodeInfo *node = &mpeers.nodeInfos[i];
      tsMpeerSyncCfg.nodeInfo[i].nodeId = node->nodeId;
      tsMpeerSyncCfg.nodeInfo[i].nodeIp = node->nodeIp;
      strcpy(tsMpeerSyncCfg.nodeInfo[i].name, node->nodeName);   
    }
    tsMpeerSyncCfg.replica = mpeers.nodeNum;
  }

  tsMpeerSyncCfg.arbitratorIp = syncCfg.nodeInfo[0].nodeIp;

  mPrint("start to work as mpeer, replica:%d arbitratorIp:%s", tsMpeerSyncCfg.nodeNum,
         taosIpStr(tsMpeerSyncCfg.arbitratorIp));
  for (int32_t i = 0; i < mpeers.nodeNum; ++i) {
    mPrint("mpeer:%d, ip:%s name:%s", tsMpeerSyncCfg.nodeInfo[i].nodeId, taosIpStr(tsMpeerSyncCfg.nodeInfo[i].nodeIp),
           tsMpeerSyncCfg.nodeInfo[i].name);
  }

  sprintf(temp, "%s/wal", tsMnodeDir);
  tsMpeerWal = walOpen(temp, &tsMpeerWalCfg);

  SSyncInfo syncInfo;
  syncInfo.vgId = 1;
  syncInfo.version = pVnode->version;
  syncInfo.syncCfg = tsMpeerSyncCfg;
  sprintf(syncInfo.path, "%s/", tsMnodeDir);
  syncInfo.ahandle = NULL;
  syncInfo.getWalInfo = mpeerGetWalInfo;
  syncInfo.getFileInfo = mpeerGetFileInfo;
  syncInfo.writeToCache = mpeerWriteToQueue;
  syncInfo.confirmForward = mpeerConfirmForward; 
  syncInfo.notifyRole = mpeerNotifyRole;

  tsMpeerSync = syncStart(&syncInfo);

  mTrace("mnodes is initialized");
  return 0;
}

void mpeerCleanup() {
  sdbCloseTable(tsMnodeSdb);
}

bool mpeerInServerStatus() {
  return tsMpeerStatus == TAOS_MN_STATUS_READY;
}

bool mpeerIsMaster() {
  return tsMpeerRole == TAOS_SYNC_ROLE_MASTER;
}

bool mgmtCheckRedirect(void *handle) {
  return mpeerIsMaster();
}

void mpeerGetPrivateIpList(SRpcIpSet *ipSet) {

}

void mpeerGetPublicIpList(SRpcIpSet *ipSet) {

}


static int mpeerWalCallback(void *arg) {
  mPrint("mpeerWalCallback");
}

static uint32_t mpeerGetFileInfo(void *ahandle, char *name, uint32_t *index, int32_t *size) {
  mPrint("mpeerGetFileInfo");
  return 0;
}

static int mpeerGetWalInfo(void *ahandle, char *name, uint32_t *index) {
  mPrint("mpeerGetWalInfo");
}

static void mpeerNotifyRole(void *ahandle, int8_t role) {
  mPrint("mnode role changed from %s to %s");  
  tsMpeerRole = role;
  
}

static void mpeerConfirmForward(void *ahandle, void *param, int32_t code) {
  mPrint("mpeerConfirmForward");
}

// static void mpeerWorkAsMaster() {
//   sdbLPrint("dnode:%s start to work as master", tsPrivateIp);

//   pSelf->role = SDB_ROLE_MASTER;
//   pSelf->status = SDB_STATUS_SERVING;
//   sdbMaster = 1;
//   tsMpeerMasterStartTime = taosGetTimestampSec();

//   mpeerUpdateIpList();
//   (*sdbWorkAsMasterCallback)();
// }

// void sdbStopWorkingAsMaster() {
//   sdbLPrint("dnode:%s stop working as Master", tsPrivateIp);

//   pSelf->role = SDB_ROLE_UNDECIDED;
//   taosTmrReset(mpeerCheckRoleStatus, tsMgmtPeerHBTimer * 1000, NULL, tsMpeerTmr, &tsMpeerRoleTimer);
//   sdbMaster = 0;

//   mpeerUpdateIpList();
// }
