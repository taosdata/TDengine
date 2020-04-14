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
#include "tbalance.h"
#include "tcluster.h"
#include "mnode.h"
#include "mpeer.h"
#include "dnodeModule.h"

static void clusterSetModuleInDnode(SDnodeObj *pDnode, int32_t moduleType) {
  pDnode->moduleStatus |= (1 << moduleType);
  clusterUpdateDnode(pDnode);

  if (moduleType == TSDB_MOD_MGMT) {
    mpeerAddMnode(pDnode->dnodeId);
    mPrint("dnode:%d, add it into mnode list", pDnode->dnodeId);
  }
}

static void clusterUnSetModuleInDnode(SDnodeObj *pDnode, int32_t moduleType) {
  pDnode->moduleStatus &= ~(1 << moduleType);
  clusterUpdateDnode(pDnode);

  if (moduleType == TSDB_MOD_MGMT) {
    mpeerRemoveMnode(pDnode->dnodeId);
    mPrint("dnode:%d, remove it from mnode list", pDnode->dnodeId);
  }
}

static void clusterStopAllModuleInDnode(SDnodeObj *pDnode) {
  for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
    if (!clusterCheckModuleInDnode(pDnode, moduleType)) {
      continue;
    }

    mPrint("dnode:%d, stop %s module for its offline or remove", pDnode->dnodeId, tsModule[moduleType].name);
    clusterUnSetModuleInDnode(pDnode, moduleType);
  }
}

static void clusterStartModuleInAllDnodes(int32_t moduleType) {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;

  while (1) {
    pNode = clusterGetNextDnode(pNode, &pDnode);
    if (pDnode == NULL) break;

    if (!clusterCheckModuleInDnode(pDnode, moduleType) 
        && pDnode->status != TAOS_DN_STATUS_OFFLINE 
        && pDnode->status != TAOS_DN_STATUS_DROPPING) {
      mPrint("dnode:%d, add %s module for schedule", pDnode->dnodeId, tsModule[moduleType].name);
      clusterSetModuleInDnode(pDnode, moduleType);
    }

    clusterReleaseDnode(pNode);
  }
}

static void clusterStartModuleInOneDnode(int32_t moduleType) {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;

  while (1) {
    pNode = clusterGetNextDnode(pNode, &pDnode);
    if (pDnode == NULL) break;

    if (!clusterCheckModuleInDnode(pDnode, moduleType) 
        && pDnode->status != TAOS_DN_STATUS_OFFLINE 
        && pDnode->status != TAOS_DN_STATUS_DROPPING
        && !(moduleType == TSDB_MOD_MGMT && pDnode->alternativeRole == TSDB_DNODE_ROLE_VNODE)) {
      mPrint("dnode:%d, add %s module for schedule", pDnode->dnodeId, tsModule[moduleType].name);
      clusterSetModuleInDnode(pDnode, moduleType);
      clusterReleaseDnode(pNode);
      break;
    }

    clusterReleaseDnode(pNode);
  }
}

static void clusterStopModuleInOneDnode(int32_t moduleType) {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;

  while (1) {
    pNode = clusterGetNextDnode(pNode, &pDnode);
    if (pDnode == NULL) break;

    if (clusterCheckModuleInDnode(pDnode, moduleType)) {
      mPrint("dnode:%d, stop %s module for schedule", pDnode->dnodeId, tsModule[moduleType].name);
      clusterUnSetModuleInDnode(pDnode, moduleType);
      clusterReleaseDnode(pNode);
      break;
    }

    clusterReleaseDnode(pNode);
  }
}

void clusterMonitorDnodeModule() {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;
  int32_t        onlineDnodes = 0;

  for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
    tsModule[moduleType].curNum = 0;
  }

  // dnode loop
  while (1) {
    pNode = clusterGetNextDnode(pNode, &pDnode);
    if (pDnode == NULL) break;

    if (pDnode->status == TAOS_DN_STATUS_DROPPING) {
      mPrint("dnode:%d, status:%d, remove all modules for removing", pDnode->dnodeId, pDnode->status);
      clusterStopAllModuleInDnode(pDnode);
      clusterReleaseDnode(pDnode);
      continue;
    }

    for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
      if (clusterCheckModuleInDnode(pDnode, moduleType)) {
        tsModule[moduleType].curNum ++;
      }
    }

    if (pDnode->status != TAOS_DN_STATUS_OFFLINE) {
      onlineDnodes++;
    }

    clusterReleaseDnode(pDnode);
  }

  for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
    if (tsModule[moduleType].num == -1) {
      clusterStartModuleInAllDnodes(moduleType);
      continue;
    }
    if (tsModule[moduleType].curNum < tsModule[moduleType].num) {
      if (onlineDnodes <= tsModule[moduleType].curNum) {
        continue;
      }
      mTrace("need add %s module, curNum:%d, expectNum:%d", tsModule[moduleType].name, tsModule[moduleType].curNum,
             tsModule[moduleType].num);
      for (int32_t i = tsModule[moduleType].curNum; i < tsModule[moduleType].num; ++i) {
        clusterStartModuleInOneDnode(moduleType);
      }
    } else if (tsModule[moduleType].curNum > tsModule[moduleType].num) {
      mTrace("need drop %s module, curNum:%d, expectNum:%d", tsModule[moduleType].name, tsModule[moduleType].curNum,
             tsModule[moduleType].num);
      for (int32_t i = tsModule[moduleType].num; i < tsModule[moduleType].curNum; ++i) {
        clusterStopModuleInOneDnode(moduleType);
      }
    } else {
    }
  }
}
