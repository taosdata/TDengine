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

#include "vnodeInt.h"

struct Vnode *vnodeCreateInstance(SVnodePara para) {
  return NULL;
}

void vnodeDropInstance(struct Vnode *vnode) {}

int32_t vnodeGetStatistics(struct Vnode *vnode, SVnodeStat *stat) { return 0; }

void vnodeGetStatus(struct Vnode *vnode, struct SStatusMsg *status) {}

void vnodeSetAccess(struct Vnode *vnode, struct SVgroupAccess *access, int32_t numOfVnodes) {}

void vnodeProcessMsg(struct Vnode *vnode, SRpcMsg *msg) {}
