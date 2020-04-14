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
#include "twal.h"
#include "tgrant.h"
#include "vnode.h"
#include "tbalance.h"
#include "mpeer.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"
#include "dnodeMClient.h"
#include "dnodeMgmt.h"

typedef struct {
  void *      sync;
  sem_t       sem;
  int32_t     code;
  int8_t      inUse;
  int8_t      role;
  SSyncCfg    cfg;
  SSdbObject *sdb;
} SSdbSyncObject;

static SSdbSyncObject tsSdbSync = {0};
static void *   tsMnodeSdb = NULL;
static int32_t  tsMnodeUpdateSize = 0;
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
  SDnodeObj *pDnode = clusterGetDnode(pMnode->mnodeId);
  if (pDnode != NULL) {
    pMnode->privateIp = pDnode->privateIp;
    pMnode->publicIp = pDnode->publicIp;
    strcpy(pMnode->mnodeName, pDnode->dnodeName);
    pMnode->role = TAOS_SYNC_ROLE_OFFLINE;
  } else {
    pMnode->privateIp = inet_addr(tsPrivateIp);
    pMnode->publicIp = inet_addr(tsPublicIp);
    sprintf(pMnode->mnodeName, "n%d", 1);
    pMnode->role = TAOS_SYNC_ROLE_OFFLINE;
  }
  
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionDelete(SSdbOperDesc *pOper) {
  SMnodeObj *pMnode = pOper->pObj;
  mTrace("mnode:%d, is dropped from sdb", pMnode->mnodeId);
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionUpdate(SSdbOperDesc *pOper) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionEncode(SSdbOperDesc *pOper) {
  SMnodeObj *pMnode = pOper->pObj;
  memcpy(pOper->rowData, pMnode, tsMnodeUpdateSize);
  pOper->rowSize = tsMnodeUpdateSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionDecode(SSdbOperDesc *pOper) {
  SMnodeObj *pMnode = calloc(1, sizeof(SMnodeObj));
  if (pMnode == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  memcpy(pMnode, pOper->rowData, tsMnodeUpdateSize);
  pOper->pObj = pMnode;
  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerUpdateSync() {
  SSyncCfg syncCfg = {0};

  void *  pNode = NULL;
  int32_t index = 0;
  while (1) {
    SMnodeObj *pMnode = NULL;
    pNode = mpeerGetNextMnode(pNode, &pMnode);
    if (pMnode == NULL) break;

    syncCfg.nodeInfo[index].nodeId = pMnode->mnodeId;
    syncCfg.nodeInfo[index].nodeIp = pMnode->privateIp;
    strcpy(syncCfg.nodeInfo[index].name, pMnode->mnodeName);
    index++;

    mpeerReleaseMnode(pMnode);
  }

  // first init by module status
  if (index == 0) {
    SDMNodeInfos *mpeers = dnodeGetMpeerInfos();
    for (int32_t i = 0; i < mpeers->nodeNum; ++i) {
      SDMNodeInfo *node = &mpeers->nodeInfos[i];
      syncCfg.nodeInfo[i].nodeId = node->nodeId;
      syncCfg.nodeInfo[i].nodeIp = node->nodeIp;
      strcpy(syncCfg.nodeInfo[i].name, node->nodeName);  
      index++; 
    }
  }

  syncCfg.replica = index;
  syncCfg.quorum = 1;
  syncCfg.arbitratorIp = syncCfg.nodeInfo[0].nodeIp;

  mPrint("work as mpeer, replica:%d arbitratorIp:%s", syncCfg.replica, taosIpStr(syncCfg.arbitratorIp));
  for (int32_t i = 0; i < syncCfg.replica; ++i) {
    mPrint("mpeer:%d, ip:%s name:%s", syncCfg.nodeInfo[i].nodeId, taosIpStr(syncCfg.nodeInfo[i].nodeIp),
           syncCfg.nodeInfo[i].name);
  }

  SSyncInfo syncInfo;
  syncInfo.vgId = 1;
  syncInfo.version = tsSdbSync.sdb->version;
  syncInfo.syncCfg = syncCfg;
  sprintf(syncInfo.path, "%s/", tsMnodeDir);
  syncInfo.ahandle = NULL;
  syncInfo.getWalInfo = mpeerGetWalInfo;
  syncInfo.getFileInfo = mpeerGetFileInfo;
  syncInfo.writeToCache = sdbProcessWrite;
  syncInfo.confirmForward = mpeerConfirmForward; 
  syncInfo.notifyRole = mpeerNotifyRole;
  tsSdbSync.cfg = syncCfg;

  if (tsSdbSync.sync) {
    syncReconfig(tsSdbSync.sync, &syncCfg);
  } else {
    tsSdbSync.sync = syncStart(&syncInfo);
  }
  
  SNodesRole roles = {0};
  syncGetNodesRole(tsSdbSync.sync, &roles);

  for (int32_t i = 0; i < tsSdbSync.cfg.replica; ++i) {
    SMnodeObj *pMnode = mpeerGetMnode(roles.nodeId[i]);
    if (pMnode != NULL) {
      pMnode->role = roles.role[i];
      if (pMnode->role == TAOS_SYNC_ROLE_MASTER) {
        tsSdbSync.inUse = i;
      }
      mpeerReleaseMnode(pMnode);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mpeerActionRestored() {
  int32_t numOfRows = sdbGetNumOfRows(tsMnodeSdb);
  if (numOfRows <= 0) {
    if (strcmp(tsMasterIp, tsPrivateIp) == 0) {
      mpeerAddMnode(1);
    }
  }

  tsSdbSync.sdb = sdbGetObj();
  sem_init(&tsSdbSync.sem, 0, 0);

  return mpeerUpdateSync();
}

int32_t mpeerInitMnodes() {
  SMnodeObj tObj;
  tsMnodeUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableId      = SDB_TABLE_MNODE,
    .tableName    = "mnodes",
    .hashSessions = TSDB_MAX_MNODES,
    .maxRowSize   = tsMnodeUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_INT,
    .insertFp     = mpeerActionInsert,
    .deleteFp     = mpeerActionDelete,
    .updateFp     = mpeerActionUpdate,
    .encodeFp     = mpeerActionEncode,
    .decodeFp     = mpeerActionDecode,
    .destroyFp    = mpeerActionDestroy,
    .restoredFp   = mpeerActionRestored
  };

  tsMnodeSdb = sdbOpenTable(&tableDesc);
  if (tsMnodeSdb == NULL) {
    mError("failed to init mnodes data");
    return -1;
  }

  mTrace("mnodes is initialized");
  return 0;
}

void mpeerCleanupMnodes() {
  sdbCloseTable(tsMnodeSdb);
  if (tsSdbSync.sync) {
    syncStop(tsSdbSync.sync);
    free(tsSdbSync.sync);
    sem_destroy(&tsSdbSync.sem);
    memset(&tsSdbSync, 0, sizeof(tsSdbSync));
  }
}

int32_t mpeerGetMnodesNum() { 
  return sdbGetNumOfRows(tsMnodeSdb); 
}

void *mpeerGetMnode(int32_t mnodeId) {
  return sdbGetRow(tsMnodeSdb, &mnodeId);
}

void *mpeerGetNextMnode(void *pNode, SMnodeObj **pMnode) { 
  return sdbFetchRow(tsMnodeSdb, pNode, (void **)pMnode); 
}

void mpeerReleaseMnode(struct _mnode_obj *pMnode) {
  sdbDecRef(tsMnodeSdb, pMnode);
}

bool mpeerIsMaster() {
  return tsSdbSync.role == TAOS_SYNC_ROLE_MASTER;
}

void mpeerGetPrivateIpList(SRpcIpSet *ipSet) {
  ipSet->numOfIps = tsSdbSync.cfg.replica;
  ipSet->inUse = 0;
  ipSet->port = htons(tsMnodeDnodePort);
  for (int32_t i = 0; i < tsSdbSync.cfg.replica; ++i) {
    ipSet->ip[i] = htonl(tsSdbSync.cfg.nodeInfo[i].nodeIp);
  }
}

void mpeerGetPublicIpList(SRpcIpSet *ipSet) {
  ipSet->numOfIps = tsSdbSync.cfg.replica;
  ipSet->inUse = tsSdbSync.inUse;
  ipSet->port = htons(tsMnodeDnodePort);
  for (int32_t i = 0; i < tsSdbSync.cfg.replica; ++i) {
    ipSet->ip[i] = htonl(tsSdbSync.cfg.nodeInfo[i].nodeIp);
  }
}

void mpeerGetMpeerInfos(void *param) {
  int32_t dnodeId = dnodeGetDnodeId();
  SDMNodeInfos *mpeers = param;
  mpeers->nodeNum = tsSdbSync.cfg.replica;
  mpeers->inUse = 0;
  for (int32_t i = 0; i < tsSdbSync.cfg.replica; ++i) {
    mpeers->nodeInfos[0].nodeId = htonl(tsSdbSync.cfg.nodeInfo[i].nodeId);
    mpeers->nodeInfos[0].nodeIp = htonl(tsSdbSync.cfg.nodeInfo[i].nodeIp);
    mpeers->nodeInfos[0].nodePort = htons(tsMnodeDnodePort);
    strcpy(mpeers->nodeInfos[0].nodeName, tsSdbSync.cfg.nodeInfo[i].name);
    if (tsSdbSync.cfg.nodeInfo[i].nodeId == dnodeId) {
      mpeers->inUse = i;
    }
  }
}

static uint32_t mpeerGetFileInfo(void *ahandle, char *name, uint32_t *index, int32_t *size) {
  mPrint("mpeerGetFileInfo");
  return 0;
}

static int mpeerGetWalInfo(void *ahandle, char *name, uint32_t *index) {
  mPrint("mpeerGetWalInfo");
  return 0;
}

static void mpeerNotifyRole(void *ahandle, int8_t role) {
  mPrint("mnode role changed from %s to %s", syncRole[tsSdbSync.role], syncRole[role]);  

  tsSdbSync.role = role;
  if (role == TAOS_SYNC_ROLE_MASTER) {
    balanceReset();
  }
  
  if (tsSdbSync.sync != NULL) {
    SNodesRole roles = {0};
    syncGetNodesRole(tsSdbSync.sync, &roles);

    for (int32_t i = 0; i < tsSdbSync.cfg.replica; ++i) {
      SMnodeObj *pMnode = mpeerGetMnode(roles.nodeId[i]);
      if (pMnode != NULL) {
        pMnode->role = roles.role[i];
        if (pMnode->role == TAOS_SYNC_ROLE_MASTER) {
          tsSdbSync.inUse = i;
        }
        mpeerReleaseMnode(pMnode);
      }
    }
  }  
}

static void mpeerConfirmForward(void *ahandle, void *param, int32_t code) {
  sem_post(&tsSdbSync.sem);
  tsSdbSync.code = code;
  mPrint("mpeerConfirmForward");
}

int32_t mpeerForwardReqToPeer(void *pHead) {
  if (tsSdbSync.sync == NULL) return TSDB_CODE_SUCCESS;
  if (tsSdbSync.cfg.replica <= 1) return TSDB_CODE_SUCCESS;

  int32_t code = syncForwardToPeer(NULL, pHead, NULL);
  if (code < 0) {
    return code;
  }
  
  sem_wait(&tsSdbSync.sem);
  return tsSdbSync.code;
}

int32_t mpeerAddMnode(int32_t dnodeId) {
  SMnodeObj *pMnode = calloc(1, sizeof(SMnodeObj));
  pMnode->mnodeId = dnodeId;
  pMnode->createdTime = taosGetTimestampMs();

  SSdbOperDesc oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsMnodeSdb,
    .pObj = pMnode,
    .rowSize = tsMnodeUpdateSize
  };

  int32_t code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(pMnode);
    code = TSDB_CODE_SDB_ERROR;
  }

  return code;
}

int32_t mpeerRemoveMnode(int32_t dnodeId) {
  SMnodeObj *pMnode = sdbGetRow(tsMnodeSdb, &dnodeId);
  if (pMnode == NULL) {
    return TSDB_CODE_DNODE_NOT_EXIST;
  }
  
  SSdbOperDesc oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsMnodeSdb,
    .pObj = pMnode
  };

  int32_t code = sdbDeleteRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_SDB_ERROR;
  }

  sdbDecRef(tsMnodeSdb, pMnode);
  return code;
}
