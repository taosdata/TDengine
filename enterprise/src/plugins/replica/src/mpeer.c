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
#include "taosdef.h"
#include "taosmsg.h"
#include "tutil.h"
#include "tsync.h"
#include "twal.h"
#include "tgrant.h"
#include "balance.h"
#include "mpeer.h"
#include "vnode.h"
#include "mgmtDef.h"
#include "mgmtLog.h"
#include "mgmtMnode.h"
#include "mgmtDnode.h"
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
  int8_t      role;
  SSyncCfg    cfg;
} SSdbSyncObject;

extern int32_t tsMnodeIsMaster;
static SSdbSyncObject tsSdbSync = {0};
static uint32_t mpeerGetFileInfo(void *ahandle, char *name, uint32_t *index, int32_t *size);
static int      mpeerGetWalInfo(void *ahandle, char *name, uint32_t *index);
static void     mpeerNotifyRole(void *ahandle, int8_t role);
static void     mpeerConfirmForward(void *pVnode, void *param, int32_t code);

void mpeerNotify() {
  SSyncCfg syncCfg = {0};

  int32_t index = 0;
  SDMNodeInfos *mnodes = dnodeGetMnodeList();
  for (int32_t i = 0; i < mnodes->nodeNum; ++i) {
    SDMNodeInfo *node = &mnodes->nodeInfos[i];
    syncCfg.nodeInfo[i].nodeId = node->nodeId;
    syncCfg.nodeInfo[i].nodeIp = node->nodeIp;
    strcpy(syncCfg.nodeInfo[i].name, node->nodeName);
    index++;
  }

  if (index == 0) {
    void *pNode = NULL;
    while (1) {
      SMnodeObj *pMnode = NULL;
      pNode = mgmtGetNextMnode(pNode, &pMnode);
      if (pMnode == NULL) break;

      syncCfg.nodeInfo[index].nodeId = pMnode->mnodeId;
      syncCfg.nodeInfo[index].nodeIp = pMnode->pDnode->privateIp;
      strcpy(syncCfg.nodeInfo[index].name, pMnode->pDnode->dnodeName);
      index++;

      mgmtReleaseMnode(pMnode);
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
  syncInfo.version = sdbGetVersion();
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
    sem_init(&tsSdbSync.sem, 0, 0);
  }
}

int32_t mpeerInit() {
  mpeerNotify();
  return 0;
}

void mpeerCleanUp() {
  if (tsSdbSync.sync) {
    syncStop(tsSdbSync.sync);
    free(tsSdbSync.sync);
    sem_destroy(&tsSdbSync.sem);
    memset(&tsSdbSync, 0, sizeof(tsSdbSync));
  }
}

static uint32_t mpeerGetFileInfo(void *ahandle, char *name, uint32_t *index, int32_t *size) {
  mPrint("mpeerGetFileInfo");
  return 0;
}

static int mpeerGetWalInfo(void *ahandle, char *name, uint32_t *index) {
  strcpy(name, "wal0");
  return 0;
}

static void mpeerNotifyRole(void *ahandle, int8_t role) {
  mPrint("mnode role changed from %s to %s", syncRole[tsSdbSync.role], syncRole[role]);  

  tsSdbSync.role = role;
  if (role == TAOS_SYNC_ROLE_MASTER) {
    tsMnodeIsMaster = true;
    balanceReset();
  } else {
    tsMnodeIsMaster = false;
  }

  if (tsSdbSync.sync != NULL) {
    SNodesRole roles = {0};
    syncGetNodesRole(tsSdbSync.sync, &roles);

    for (int32_t i = 0; i < tsSdbSync.cfg.replica; ++i) {
      SMnodeObj *pMnode = mgmtGetMnode(roles.nodeId[i]);
      if (pMnode != NULL) {
        pMnode->role = roles.role[i];
        mgmtReleaseMnode(pMnode);
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

  int32_t code = syncForwardToPeer(tsSdbSync.sync, pHead, NULL);
  if (code < 0) {
    return code;
  }
  
  sem_wait(&tsSdbSync.sem);
  return tsSdbSync.code;
}
