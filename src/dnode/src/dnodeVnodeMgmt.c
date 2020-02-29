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
#include "tlog.h"
#include "taoserror.h"
#include "dnodeVnodeMgmt.h"

int32_t dnodeOpenVnodes() {
  dPrint("open all vnodes");
  return TSDB_CODE_SUCCESS;
}

int32_t dnodeCleanupVnodes() {
  dPrint("clean all vnodes");
  return TSDB_CODE_SUCCESS;
}

bool dnodeCheckVnodeExist(int32_t vnode) {
  dPrint("vnode:%d, check vnode exist", vnode);
  return true;
}

int32_t dnodeCreateVnode(SCreateVnodeMsg *pVnode) {
  dPrint("vnode:%d, is created", htonl(pVnode->vnode));
  return TSDB_CODE_SUCCESS;
}

int32_t dnodeDropVnode(int32_t vnode) {
  dPrint("vnode:%d, is dropped", vnode);
  return TSDB_CODE_SUCCESS;
}

void* dnodeGetVnode(int32_t vnode) {
  dPrint("vnode:%d, get vnode");
  return NULL;
}

EVnodeStatus dnodeGetVnodeStatus(int32_t vnode) {
  dPrint("vnode:%d, get vnode status");
  return TSDB_VN_STATUS_MASTER;
}

bool dnodeCheckTableExist(int32_t vnode, int32_t sid, int64_t uid) {
  dPrint("vnode:%d, sid:%d, check table exist");
  return true;
}

