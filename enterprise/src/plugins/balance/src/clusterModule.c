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
#include "tmodule.h"
#include "mgmtBalance.h"
#include "mgmtDnode.h"
#include "mgmtMnode.h"
#include "dnodeModule.h"

#define LB_MODULE_UNLIMIT -1

extern void *tsVgroupSdb;
extern void *tsDnodeSdb;
extern void *tstsMnodeSdb;
extern int32_t tsVgUpdateSize;
extern int32_t tsMnodeUpdateSize;
extern int32_t tsDnodeUpdateSize;

void clusterSetModuleInDnode(SDnodeObj *pDnode, int32_t moduleType) {
  pDnode->moduleStatus |= (1 << moduleType);
  sdbUpdateRow(tsDnodeSdb, pDnode, tsDnodeUpdateSize, 1);

  if (moduleType == TSDB_MOD_MGMT) {
    mgmtAddMnode(pDnode->privateIp, pDnode->publicIp);
    mPrint("dnode:%s, add mnode done", taosIpStr(pDnode->privateIp));
  }
}

int32_t clusterUnSetModuleInDnode(SDnodeObj *pDnode, int32_t moduleType) {
  pDnode->moduleStatus &= ~(1 << moduleType);
  sdbUpdateRow(tsDnodeSdb, pDnode, tsDnodeUpdateSize, 1);

  if (moduleType == TSDB_MOD_MGMT) {
    int32_t code = sdbRemovePeerByIp(pDnode->privateIp);
    mPrint("dnode:%s, drop mnode done, code:%d", taosIpStr(pDnode->privateIp), code);
    return code;
  }
  return 0;
}

void mgmtStopRemoveStateModule(SDnodeObj *pDnode) {
  for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
    if (!mgmtCheckModuleInDnode(pDnode, moduleType)) {
      continue;
    }

    mPrint("dnode:%s, stop %s module for its offline or remove", taosIpStr(pDnode->privateIp), tsModule[moduleType].name);
    clusterUnSetModuleInDnode(pDnode, moduleType);
  }
}

void mgmtStartModuleInAllDnodes(int32_t moduleType) {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;

  while (1) {
    pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;

    if (mgmtCheckModuleInDnode(pDnode, moduleType)) {
      continue;
    }

    if (mgmtCheckDnodeInOfflineState(pDnode)) {
      continue;
    }

    if (mgmtCheckDnodeInRemoveState(pDnode)) {
      continue;
    }

    mPrint("dnode:%s, add %s module for schedule:%d", taosIpStr(pDnode->privateIp), tsModule[moduleType].name, -1);
    clusterSetModuleInDnode(pDnode, moduleType);
  }
}

void mgmtStartModuleInDnode(int32_t moduleType) {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;

  while (1) {
    pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;

    if (mgmtCheckDnodeInOfflineState(pDnode)) {
      continue;
    }

    if (mgmtCheckDnodeInRemoveState(pDnode)) {
      continue;
    }

    if (mgmtCheckModuleInDnode(pDnode, moduleType)) {
      continue;
    }

    if (moduleType == TSDB_MOD_MGMT && pDnode->alternativeRole == TSDB_DNODE_ROLE_VNODE) {
      continue;
    }

    mPrint("dnode:%s, add %s module for schedule", taosIpStr(pDnode->privateIp), tsModule[moduleType].name);
    clusterSetModuleInDnode(pDnode, moduleType);

    break;
  }
}

void mgmtStopModuleInDnode(int32_t moduleType) {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;

  while (1) {
    pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;

    if (!mgmtCheckModuleInDnode(pDnode, moduleType)) {
      continue;
    }

    mPrint("dnode:%s, stop %s module for schedule", taosIpStr(pDnode->privateIp), tsModule[moduleType].name);
    clusterUnSetModuleInDnode(pDnode, moduleType);
    break;
  }
}

// TODO
void clusterMonitorDnodeModule() {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;
  int32_t        onlineDnodes = 0;

  for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
    tsModule[moduleType].curNum = 0;
  }

  // dnode loop
  while (1) {
    pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;

    if (mgmtCheckDnodeInRemoveState(pDnode)) {
      mPrint("dnode:%s, status:%d, remove all modules for removing", taosIpStr(pDnode->privateIp), pDnode->status);
      mgmtStopRemoveStateModule(pDnode);
      continue;
    }

    for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
      if (mgmtCheckModuleInDnode(pDnode, moduleType)) {
        tsModule[moduleType].curNum += mgmtCheckModuleInDnode(pDnode, moduleType);
      }
    }

    if (!mgmtCheckDnodeInOfflineState(pDnode)) {
      onlineDnodes++;
    }
  }

  for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
    if (tsModule[moduleType].num == LB_MODULE_UNLIMIT) {
      mgmtStartModuleInAllDnodes(moduleType);
      continue;
    }
    if (tsModule[moduleType].curNum < tsModule[moduleType].num) {
      if (onlineDnodes <= tsModule[moduleType].curNum) {
        continue;
      }
      mTrace("need add %s module, curNum:%d, expectNum:%d", tsModule[moduleType].name, tsModule[moduleType].curNum,
             tsModule[moduleType].num);
      for (int32_t i = tsModule[moduleType].curNum; i < tsModule[moduleType].num; ++i) {
        mgmtStartModuleInDnode(moduleType);
      }
    } else if (tsModule[moduleType].curNum > tsModule[moduleType].num) {
      mTrace("need drop %s module, curNum:%d, expectNum:%d", tsModule[moduleType].name, tsModule[moduleType].curNum,
             tsModule[moduleType].num);
      for (int32_t i = tsModule[moduleType].num; i < tsModule[moduleType].curNum; ++i) {
        mgmtStopModuleInDnode(moduleType);
      }
    } else {
    }
  }
}

void dnodeProcessModuleStatus(uint32_t status) {
  int news = status;
  int olds = tsModuleStatus;

  for (int moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
    int newStatus = news & (1 << moduleType);
    int oldStatus = olds & (1 << moduleType);

    if (oldStatus > 0) {
      if (newStatus == 0) {
        if (tsModule[moduleType].stopFp) {
          dPrint("module:%s is stopped on this node", tsModule[moduleType].name);
          (*tsModule[moduleType].stopFp)();
        }
      }
    } else if (oldStatus == 0) {
      if (newStatus > 0) {
        if (tsModule[moduleType].startFp) {
          dPrint("module:%s is started on this node", tsModule[moduleType].name);
          (*tsModule[moduleType].startFp)();
        }
      }
    } else {
    }
  }
  tsModuleStatus = status;
}

void mgmtUpdateModules(uint32_t status) {
  if (status != tsModuleStatus) {
    dPrint("module status is received, old:%d, new:%d", tsModuleStatus, status);
    dnodeProcessModuleStatus(status);
  }
}

