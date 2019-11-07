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
#include "mgmtBalance.h"

#define LB_MODULE_UNLIMIT -1

void mgmtSetModuleInDnode(SDnodeObj *pDnode, int moduleType) {
  pDnode->moduleStatus |= (1 << moduleType);
  sdbUpdateRow(dnodeSdb, pDnode, tsDnodeUpdateSize, 1);

  if (moduleType == TSDB_MOD_MGMT) {
    sdbAddPeer(pDnode->privateIp, pDnode->publicIp, 0);
    mPrint("dnode:%s, add mnode done", taosIpStr(pDnode->privateIp));
  }
}

int mgmtUnSetModuleInDnode(SDnodeObj *pDnode, int moduleType) {
  pDnode->moduleStatus &= ~(1 << moduleType);
  sdbUpdateRow(dnodeSdb, pDnode, tsDnodeUpdateSize, 1);

  if (moduleType == TSDB_MOD_MGMT) {
    int code = sdbRemovePeerByIp(pDnode->privateIp);
    mPrint("dnode:%s, drop mnode done, code:%d", taosIpStr(pDnode->privateIp), code);
    return code;
  }
  return 0;
}

bool mgmtCheckModuleInDnode(SDnodeObj *pDnode, int moduleType) {
  uint32_t status = pDnode->moduleStatus & (1 << moduleType);
  return status > 0;
}

void mgmtStopRemoveStateModule(SDnodeObj *pDnode) {
  for (int moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
    if (!mgmtCheckModuleInDnode(pDnode, moduleType)) {
      continue;
    }

    mPrint("dnode:%s, stop %s module for its offline or remove", taosIpStr(pDnode->privateIp), tsModule[moduleType].name);
    mgmtUnSetModuleInDnode(pDnode, moduleType);
  }
}

void mgmtStartModuleInAllDnodes(int moduleType) {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;

  while (1) {
    pNode = sdbFetchRow(dnodeSdb, pNode, (void **)&pDnode);
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
    mgmtSetModuleInDnode(pDnode, moduleType);
  }
}

void mgmtStartModuleInDnode(int moduleType) {
  mgmtMakeDnodeOrderList();

  for (int i = mgmtOrderedDnodesSize - 1; i >= 0; --i) {
    SDnodeObj *pDnode = mgmtOrderedDnodes[i];
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
    mgmtSetModuleInDnode(pDnode, moduleType);

    break;
  }
}

void mgmtStopModuleInDnode(int moduleType) {
  mgmtMakeDnodeOrderList();

  for (int i = 0; i < mgmtOrderedDnodesSize; ++i) {
    SDnodeObj *pDnode = mgmtOrderedDnodes[i];

    if (!mgmtCheckModuleInDnode(pDnode, moduleType)) {
      continue;
    }

    mPrint("dnode:%s, stop %s module for schedule", taosIpStr(pDnode->privateIp), tsModule[moduleType].name);
    mgmtUnSetModuleInDnode(pDnode, moduleType);
    break;
  }
}

void mgmtMonitorDnodeModule() {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;
  int        onlineDnodes = 0;

  for (int moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
    tsModule[moduleType].curNum = 0;
  }

  // dnode loop
  while (1) {
    pNode = sdbFetchRow(dnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;

    if (mgmtCheckDnodeInRemoveState(pDnode)) {
      mPrint("dnode:%s, status:%d, lbState:%d, remove all modules for it in remove state",
          taosIpStr(pDnode->privateIp), pDnode->status, pDnode->lbState);
      mgmtStopRemoveStateModule(pDnode);
      continue;
    }

    for (int moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
      if (mgmtCheckModuleInDnode(pDnode, moduleType)) {
        tsModule[moduleType].curNum += mgmtCheckModuleInDnode(pDnode, moduleType);
      }
    }

    if (!mgmtCheckDnodeInOfflineState(pDnode)) {
      onlineDnodes++;
    }
  }

  for (int moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
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
      for (int i = tsModule[moduleType].curNum; i < tsModule[moduleType].num; ++i) {
        mgmtStartModuleInDnode(moduleType);
      }
    } else if (tsModule[moduleType].curNum > tsModule[moduleType].num) {
      mTrace("need drop %s module, curNum:%d, expectNum:%d", tsModule[moduleType].name, tsModule[moduleType].curNum,
             tsModule[moduleType].num);
      for (int i = tsModule[moduleType].num; i < tsModule[moduleType].curNum; ++i) {
        mgmtStopModuleInDnode(moduleType);
      }
    } else {
    }
  }
}
